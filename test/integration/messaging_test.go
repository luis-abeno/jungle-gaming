//go:build integration

package integration

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"wallet/internal/app"
	"wallet/internal/messaging"
	"wallet/internal/observability"
)

func TestQueuesAreProvisioned(t *testing.T) {
	t.Parallel()
	q := newQueues(t, 5, 30)
	out, err := sqsClient.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(q.URLs.Wager), AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		t.Fatal(err)
	}
	a := out.Attributes
	if a["FifoQueue"] != "true" || a["VisibilityTimeout"] != "30" || a["Policy"] == "" ||
		!strings.Contains(a["RedrivePolicy"], "wager-dlq.fifo") || !strings.Contains(a["RedrivePolicy"], `"maxReceiveCount":"5"`) {
		t.Fatalf("attributes %v", a)
	}
}

func TestSQSMessageIsProcessed(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "100.00")

	o := bet(walletID, playerID, "sqs-1", "40.00")
	sendMessage(t, e.Queues.URLs.Wager, o.message("msg-sqs-1"), walletID, "msg-sqs-1")
	eventually(t, 15*time.Second, "message processed", func() bool {
		s, _ := txStatus(t, e.Owner, "provider-a", "sqs-1")
		return s == "PROCESSED"
	})
	if e.balance(t, walletID) != 6000 {
		t.Fatalf("balance %d", e.balance(t, walletID))
	}
	if n := queryInt(t, e.Owner, `SELECT count(*) FROM inbox_messages WHERE message_id = 'msg-sqs-1' AND completed_at IS NOT NULL AND transaction_id IS NOT NULL`); n != 1 {
		t.Fatalf("inbox rows %d", n)
	}
	eventually(t, 10*time.Second, "queue drained", func() bool { return queueDepth(t, e.Queues.URLs.Wager) == 0 })
}

func TestSQSRedeliveryIsDeduplicated(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "100.00")

	body := bet(walletID, playerID, "sqs-dup", "10.00").message("msg-dup")
	for i := 0; i < 3; i++ {
		sendMessage(t, e.Queues.URLs.Wager, body, walletID, fmt.Sprintf("dedup-%d", i))
	}
	eventually(t, 15*time.Second, "all copies consumed", func() bool {
		return testutil.ToFloat64(app.Metrics.Messages.WithLabelValues("processed"))+
			testutil.ToFloat64(app.Metrics.Messages.WithLabelValues("duplicate")) == 3
	})
	if d := testutil.ToFloat64(app.Metrics.Messages.WithLabelValues("duplicate")); d != 2 {
		t.Fatalf("duplicates %v", d)
	}
	if e.ledgerEntries(t, walletID) != 2 || e.balance(t, walletID) != 9000 {
		t.Fatal("duplicate message moved money")
	}
	if n := queryInt(t, e.Owner, `SELECT count(*) FROM inbox_messages`); n != 1 {
		t.Fatalf("inbox rows %d", n)
	}
}

func TestHTTPAndSQSShareIdempotency(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "100.00")

	first := bet(walletID, playerID, "cross-1", "10.00")
	r := submit(t, app.URL, first)
	if r.Status != http.StatusOK {
		t.Fatal(r.Raw)
	}
	sendMessage(t, e.Queues.URLs.Wager, first.message("msg-cross-1"), walletID, "msg-cross-1")
	eventually(t, 15*time.Second, "sqs copy handled", func() bool {
		return queryInt(t, e.Owner, `SELECT count(*) FROM inbox_messages WHERE message_id = 'msg-cross-1' AND completed_at IS NOT NULL`) == 1
	})
	var inboxTx string
	e.Owner.QueryRow(context.Background(), `SELECT transaction_id::text FROM inbox_messages WHERE message_id = 'msg-cross-1'`).Scan(&inboxTx)
	if inboxTx != r.str("transactionId") {
		t.Fatalf("inbox points to %s, http created %s", inboxTx, r.str("transactionId"))
	}

	second := bet(walletID, playerID, "cross-2", "10.00")
	sendMessage(t, e.Queues.URLs.Wager, second.message("msg-cross-2"), walletID, "msg-cross-2")
	parallel(5, func(int) {
		if r := submit(t, app.URL, second); r.Status != http.StatusOK {
			t.Errorf("%d %s", r.Status, r.Raw)
		}
	})
	eventually(t, 15*time.Second, "sqs copy handled", func() bool {
		return queryInt(t, e.Owner, `SELECT count(*) FROM inbox_messages WHERE message_id = 'msg-cross-2' AND completed_at IS NOT NULL`) == 1
	})
	if e.ledgerEntries(t, walletID) != 3 || e.balance(t, walletID) != 8000 {
		t.Fatalf("entries %d balance %d", e.ledgerEntries(t, walletID), e.balance(t, walletID))
	}

	conflicting := first
	conflicting.Key = "other-key"
	sendMessage(t, e.Queues.URLs.Wager, conflicting.message("msg-cross-3"), walletID, "msg-cross-3")
	dlq := receiveAll(t, e.Queues.URLs.DLQ, 15*time.Second)
	if len(dlq) != 1 || aws.ToString(dlq[0].MessageAttributes["failureReason"].StringValue) != "EXTERNAL_TRANSACTION_CONFLICT" {
		t.Fatalf("dlq: %+v", dlq)
	}
	e.assertConsistent(t)
}

func TestInvalidMessagesGoToDLQ(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "100.00")
	good := bet(walletID, playerID, "dlq-ok", "1.00")

	opening := good.with("OPENING", "dlq-opening", "1.00", "")
	noWallet := good.with("BET", "dlq-nowallet", "1.00", "")
	noWallet.Wallet = uuid.NewString()

	cases := map[string]string{
		"MALFORMED_MESSAGE": `{"messageId":`,
		"UNSUPPORTED_TYPE":  `{"messageId":"m-type","type":"Other","data":{}}`,
		"VALIDATION_ERROR":  opening.message("m-opening"),
		"WALLET_NOT_FOUND":  noWallet.message("m-nowallet"),
	}
	for _, body := range cases {
		sendMessage(t, e.Queues.URLs.Wager, body, walletID, uuid.NewString())
	}
	sendMessage(t, e.Queues.URLs.Wager, good.message("m-reused"), walletID, uuid.NewString())
	eventually(t, 15*time.Second, "first copy", func() bool {
		s, _ := txStatus(t, e.Owner, "provider-a", "dlq-ok")
		return s == "PROCESSED"
	})
	sendMessage(t, e.Queues.URLs.Wager, good.with("BET", "dlq-ok-2", "2.00", "").message("m-reused"), walletID, uuid.NewString())
	cases["MESSAGE_ID_CONFLICT"] = ""

	got := map[string]bool{}
	eventually(t, 20*time.Second, "dead letters", func() bool {
		for _, m := range receiveAll(t, e.Queues.URLs.DLQ, 2*time.Second) {
			got[aws.ToString(m.MessageAttributes["failureReason"].StringValue)] = true
		}
		return len(got) == len(cases)
	})
	for reason := range cases {
		if !got[reason] {
			t.Errorf("missing %s in DLQ, got %v", reason, got)
		}
	}
	if e.ledgerEntries(t, walletID) != 2 {
		t.Fatalf("entries %d", e.ledgerEntries(t, walletID))
	}
}

func TestTransientFailuresEndInDLQ(t *testing.T) {
	t.Parallel()
	q := newQueues(t, 2, 1)
	failing := &flakyHandler{}
	c := messaging.NewConsumer(sqsClient, q.URLs, failing, messaging.ConsumerConfig{
		Name: "flaky", Workers: 1, WaitSeconds: 1, VisibilityTimeout: 1, HandleTimeout: 900 * time.Millisecond,
		AllowedSenders: map[string][]string{"*": {"*"}},
	}, slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})), observability.NewMetrics(), nil)
	if err := c.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Stop(context.Background())

	o := bet(uuid.NewString(), uuid.NewString(), "flaky-1", "1.00")
	sendMessage(t, q.URLs.Wager, o.message("m-flaky"), o.Wallet, "m-flaky")

	dlq := receiveAll(t, q.URLs.DLQ, 30*time.Second)
	if len(dlq) != 1 {
		t.Fatalf("dlq has %d messages", len(dlq))
	}
	if failing.calls.Load() < 2 {
		t.Fatalf("handler called %d times", failing.calls.Load())
	}
}

type flakyHandler struct{ calls atomic.Int32 }

func (f *flakyHandler) HandleMessage(context.Context, app.Message) (app.MessageOutcome, error) {
	f.calls.Add(1)
	return app.MessageOutcome{}, fmt.Errorf("%w: database down", app.ErrUnavailable)
}

func TestSenderAndProviderAllowList(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, map[string]string{"SQS_ALLOWED_SENDERS": "000000000000=provider-a"})
	walletID, playerID := openWallet(t, app.URL, "100.00")

	fromB := bet(walletID, playerID, "allow-b", "1.00")
	fromB.Provider = "provider-b"
	sendMessage(t, e.Queues.URLs.Wager, fromB.message("m-allow-b"), walletID, "m-allow-b")
	dlq := receiveAll(t, e.Queues.URLs.DLQ, 15*time.Second)
	if len(dlq) != 1 || aws.ToString(dlq[0].MessageAttributes["failureReason"].StringValue) != "FORBIDDEN_PROVIDER" {
		t.Fatalf("dlq: %v", dlq)
	}

	other := newEnv(t)
	app2 := other.start(t, map[string]string{"SQS_ALLOWED_SENDERS": "123456789012=*"})
	w2, p2 := openWallet(t, app2.URL, "100.00")
	sendMessage(t, other.Queues.URLs.Wager, bet(w2, p2, "allow-x", "1.00").message("m-allow-x"), w2, "m-allow-x")
	dlq = receiveAll(t, other.Queues.URLs.DLQ, 15*time.Second)
	if len(dlq) != 1 || aws.ToString(dlq[0].MessageAttributes["failureReason"].StringValue) != "UNAUTHORIZED_SENDER" {
		t.Fatalf("dlq: %v", dlq)
	}
	if e.ledgerEntries(t, walletID) != 1 || other.ledgerEntries(t, w2) != 1 {
		t.Fatal("refused messages moved money")
	}
}
