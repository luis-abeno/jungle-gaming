//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/uuid"

	"wallet/internal/app"
	"wallet/internal/messaging"
	"wallet/internal/observability"
	"wallet/internal/postgres"
)

var quietLog = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

type countingPublisher struct {
	next  app.EventPublisher
	fails atomic.Int32
	mu    sync.Mutex
	sent  map[uuid.UUID][]string
	total atomic.Int32
	delay time.Duration
}

func newCountingPublisher(next app.EventPublisher) *countingPublisher {
	return &countingPublisher{next: next, sent: map[uuid.UUID][]string{}}
}

func (p *countingPublisher) PublishBatch(ctx context.Context, events []app.PendingEvent) (map[uuid.UUID]error, error) {
	if p.fails.Load() > 0 {
		p.fails.Add(-1)
		return nil, errors.New("broker unavailable")
	}
	time.Sleep(p.delay)
	failed, err := p.next.PublishBatch(ctx, events)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range events {
		if _, ok := failed[e.ID]; !ok {
			p.sent[e.ID] = append(p.sent[e.ID], e.Payload)
			p.total.Add(1)
		}
	}
	return failed, nil
}

type forgetfulStore struct {
	app.OutboxStore
	dropped atomic.Bool
}

func (s *forgetfulStore) MarkPublished(ctx context.Context, ids ...uuid.UUID) error {
	if s.dropped.CompareAndSwap(false, true) {
		return errors.New("connection lost")
	}
	return s.OutboxStore.MarkPublished(ctx, ids...)
}

func (e *testEnv) outboxStore(t *testing.T) *postgres.OutboxRepo {
	t.Helper()
	pool, err := postgres.NewPool(context.Background(), postgres.Config{URL: e.DBURL, MaxConns: 4, LockTimeout: 5 * time.Second, StatementTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return postgres.NewOutboxRepo(postgres.NewTxManager(pool))
}

func relay(store app.OutboxStore, pub app.EventPublisher, batch int, lease time.Duration) *app.OutboxRelay {
	return app.NewOutboxRelay(store, pub, observability.NewMetrics(), quietLog,
		app.RelayConfig{Batch: batch, Lease: lease, MaxBackoff: time.Second}, nil)
}

func (e *testEnv) pendingEvents(t *testing.T) int64 {
	return queryInt(t, e.Owner, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`)
}

func eventIDs(t *testing.T, queueURL string, wait time.Duration) map[string]int {
	t.Helper()
	ids := map[string]int{}
	for _, m := range receiveAll(t, queueURL, wait) {
		var env struct {
			EventID string `json:"eventId"`
		}
		json.Unmarshal([]byte(aws.ToString(m.Body)), &env)
		ids[env.EventID]++
	}
	return ids
}

func TestCompetingPublishers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := e.start(t, map[string]string{"OUTBOX_ENABLED": "false", "CONSUMER_ENABLED": "false"})
	for i := 0; i < 20; i++ {
		openWallet(t, svc.URL, "10.00")
	}
	total := e.pendingEvents(t)
	if total != 40 {
		t.Fatalf("pending events %d", total)
	}

	sqsPub := messaging.NewEventPublisher(sqsClient, e.Queues.URLs)
	pubs := []*countingPublisher{newCountingPublisher(sqsPub), newCountingPublisher(sqsPub)}
	var wg sync.WaitGroup
	for _, p := range pubs {
		p.delay = 5 * time.Millisecond
		r := relay(e.outboxStore(t), p, 5, 30*time.Second)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n, err := r.RunOnce(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	if e.pendingEvents(t) != 0 {
		t.Fatalf("%d events left", e.pendingEvents(t))
	}
	a, b := pubs[0].total.Load(), pubs[1].total.Load()
	if int64(a+b) != total || a == 0 || b == 0 {
		t.Fatalf("publisher counts %d and %d for %d events", a, b, total)
	}
	ids := eventIDs(t, e.Queues.URLs.Events, 10*time.Second)
	if int64(len(ids)) != total {
		t.Fatalf("queue has %d distinct events", len(ids))
	}
	for id, n := range ids {
		if n != 1 {
			t.Fatalf("event %s delivered %d times", id, n)
		}
	}
}

func TestRepublishAfterCrashBeforeConfirmation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := e.start(t, map[string]string{"OUTBOX_ENABLED": "false", "CONSUMER_ENABLED": "false"})
	openWallet(t, svc.URL, "10.00")

	sqsPub := messaging.NewEventPublisher(sqsClient, e.Queues.URLs)
	pub := newCountingPublisher(sqsPub)
	crashing := relay(&forgetfulStore{OutboxStore: e.outboxStore(t)}, pub, 1, time.Second)
	if _, err := crashing.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	var lost uuid.UUID
	for id := range pub.sent {
		lost = id
	}
	if e.pendingEvents(t) != 2 {
		t.Fatal("the unconfirmed event must still be pending")
	}

	other := relay(e.outboxStore(t), pub, 10, time.Second)
	eventually(t, 10*time.Second, "all events confirmed", func() bool {
		other.RunOnce(context.Background())
		return e.pendingEvents(t) == 0
	})
	sent := pub.sent[lost]
	if len(sent) != 2 || sent[0] != sent[1] {
		t.Fatalf("lost event sent %d times, same payload: %v", len(sent), len(sent) == 2 && sent[0] == sent[1])
	}
	if a := queryInt(t, e.Owner, `SELECT attempts FROM outbox_events WHERE id = $1`, lost); a != 2 {
		t.Fatalf("attempts %d", a)
	}
	ids := eventIDs(t, e.Queues.URLs.Events, 10*time.Second)
	if ids[lost.String()] != 1 || len(ids) != 2 {
		t.Fatalf("queue content %v", ids)
	}
}

func TestEventsCommittedBeforeACrashArePublishedLater(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	first := e.start(t, map[string]string{"OUTBOX_ENABLED": "false"})
	walletID, playerID := openWallet(t, first.URL, "100.00")
	submit(t, first.URL, bet(walletID, playerID, "late-1", "10.00"))
	first.Stop()
	if e.pendingEvents(t) != 4 {
		t.Fatalf("pending %d", e.pendingEvents(t))
	}

	e.start(t, nil)
	eventually(t, 15*time.Second, "events published by the new instance", func() bool { return e.pendingEvents(t) == 0 })
	if ids := eventIDs(t, e.Queues.URLs.Events, 10*time.Second); len(ids) != 4 {
		t.Fatalf("queue has %d events", len(ids))
	}
}

func TestPublishRetriesWithBackoff(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := e.start(t, map[string]string{"OUTBOX_ENABLED": "false", "CONSUMER_ENABLED": "false"})
	openWallet(t, svc.URL, "10.00")

	pub := newCountingPublisher(messaging.NewEventPublisher(sqsClient, e.Queues.URLs))
	pub.fails.Store(4)
	r := relay(e.outboxStore(t), pub, 10, 30*time.Second)
	var before time.Time
	e.Owner.QueryRow(context.Background(), `SELECT now()`).Scan(&before)
	r.RunOnce(context.Background())

	var lastError *string
	var delayed bool
	e.Owner.QueryRow(context.Background(), `
		SELECT max(last_error), bool_and(next_attempt_at >= $1::timestamptz + interval '250 milliseconds')
		FROM outbox_events`, before).Scan(&lastError, &delayed)
	if lastError == nil || !delayed {
		t.Fatalf("failed publish must be recorded and delayed: %v %v", lastError, delayed)
	}
	eventually(t, 15*time.Second, "published after retries", func() bool {
		r.RunOnce(context.Background())
		return e.pendingEvents(t) == 0
	})
	if n := queryInt(t, e.Owner, `SELECT min(attempts) FROM outbox_events`); n < 2 {
		t.Fatalf("attempts %d", n)
	}
}
