package app

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"

	"wallet/internal/observability/logctx"
)

type RelayConfig struct {
	Batch          int
	Lease          time.Duration
	MaxBackoff     time.Duration
	PublishTimeout time.Duration
}

type OutboxRelay struct {
	store     OutboxStore
	publisher EventPublisher
	metrics   Metrics
	log       *slog.Logger
	cfg       RelayConfig
	faults    FaultHook
	lastStats time.Time
}

func NewOutboxRelay(store OutboxStore, publisher EventPublisher, metrics Metrics, log *slog.Logger, cfg RelayConfig, faults FaultHook) *OutboxRelay {
	if faults == nil {
		faults = NoFaults
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = 5 * time.Second
	}
	return &OutboxRelay{store: store, publisher: publisher, metrics: metrics, log: log, cfg: cfg, faults: faults}
}

func (r *OutboxRelay) RunOnce(ctx context.Context) (int, error) {
	r.refreshStats(ctx)
	events, err := r.store.Claim(ctx, r.cfg.Batch, r.cfg.Lease)
	if err != nil {
		return 0, err
	}
	for start := 0; start < len(events); start += sendBatch {
		if ctx.Err() != nil {
			r.release(events[start:])
			return len(events), ctx.Err()
		}
		chunk := events[start:min(start+sendBatch, len(events))]
		failed := r.publish(ctx, chunk)
		sent := make([]uuid.UUID, 0, len(chunk))
		for _, e := range chunk {
			if err, ok := failed[e.ID]; ok {
				r.metrics.OutboxPublishFailed()
				delay := r.backoff(e.Attempts)
				ectx := logctx.With(ctx, "eventId", e.ID.String(), "eventType", e.EventType, "walletId", e.PartitionKey)
				r.log.WarnContext(ectx, "event publish failed", "error", err, "attempt", e.Attempts, "retryIn", delay.String())
				if err := r.store.Reschedule(ctx, e.ID, delay, err.Error()); err != nil {
					r.log.ErrorContext(ectx, "could not reschedule event", "error", err)
				}
				continue
			}
			sent = append(sent, e.ID)
			r.metrics.OutboxPublished()
		}
		if len(sent) == 0 {
			continue
		}
		r.faults("outbox.after_publish")
		if err := r.store.MarkPublished(ctx, sent...); err != nil {
			r.log.WarnContext(ctx, "events published but not marked", "error", err, "count", len(sent))
		}
	}
	return len(events), nil
}

const sendBatch = 10

func (r *OutboxRelay) publish(ctx context.Context, events []PendingEvent) map[uuid.UUID]error {
	ctx, cancel := context.WithTimeout(ctx, r.cfg.PublishTimeout)
	defer cancel()
	failed, err := r.publisher.PublishBatch(ctx, events)
	if err != nil {
		failed = make(map[uuid.UUID]error, len(events))
		for _, e := range events {
			failed[e.ID] = err
		}
	}
	return failed
}

func (r *OutboxRelay) release(events []PendingEvent) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, e := range events {
		if err := r.store.Reschedule(ctx, e.ID, 0, "released on shutdown"); err != nil {
			r.log.WarnContext(ctx, "could not release event", "eventId", e.ID.String(), "error", err)
		}
	}
}

func (r *OutboxRelay) backoff(attempt int) time.Duration {
	d := 500 * time.Millisecond
	for i := 1; i < attempt && d < r.cfg.MaxBackoff; i++ {
		d *= 2
	}
	d = min(d, r.cfg.MaxBackoff)
	return d/2 + rand.N(d/2+1)
}

func (r *OutboxRelay) refreshStats(ctx context.Context) {
	if time.Since(r.lastStats) < 5*time.Second {
		return
	}
	r.lastStats = time.Now()
	s, err := r.store.Stats(ctx)
	if err != nil {
		return
	}
	var lag time.Duration
	if s.Pending > 0 && !s.Oldest.IsZero() {
		lag = time.Since(s.Oldest)
	}
	r.metrics.OutboxBacklog(s.Pending, lag)
}
