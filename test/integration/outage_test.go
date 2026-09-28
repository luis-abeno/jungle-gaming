//go:build integration

package integration

import (
	"net/http"
	"os/exec"
	"testing"
	"time"
)

var (
	pgContainer  = env("IT_PG_CONTAINER", "wallet-postgres-1")
	sqsContainer = env("IT_SQS_CONTAINER", "wallet-ministack-1")
)

func pause(t *testing.T, container string) (resume func()) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}
	if out, err := exec.Command("docker", "pause", container).CombinedOutput(); err != nil {
		t.Skipf("cannot pause %s: %v %s", container, err, out)
	}
	done := false
	resume = func() {
		if !done {
			done = true
			exec.Command("docker", "unpause", container).Run()
		}
	}
	t.Cleanup(resume)
	return resume
}

func TestPostgresOutage(t *testing.T) {
	e := newEnv(t)
	app := e.start(t, map[string]string{"REQUEST_TIMEOUT": "2s", "SQS_VISIBILITY_TIMEOUT": "5"})
	walletID, playerID := openWallet(t, app.URL, "100.00")
	o := bet(walletID, playerID, "outage-http", "10.00")
	m := bet(walletID, playerID, "outage-sqs", "5.00")

	resume := pause(t, pgContainer)
	r := submit(t, app.URL, o)
	if r.Status != http.StatusServiceUnavailable || r.str("error", "code") != "TEMPORARILY_UNAVAILABLE" {
		t.Fatalf("during outage: %d %s", r.Status, r.Raw)
	}
	if ready := call(t, "GET", app.URL+"/health/ready", "", nil, nil); ready.Status != http.StatusServiceUnavailable || ready.str("postgres") != "down" {
		t.Fatalf("readiness during outage: %d %s", ready.Status, ready.Raw)
	}
	if live := call(t, "GET", app.URL+"/health/live", "", nil, nil); live.Status != http.StatusOK {
		t.Fatalf("liveness: %d", live.Status)
	}
	sendMessage(t, e.Queues.URLs.Wager, m.message("m-outage"), walletID, "m-outage")
	time.Sleep(3 * time.Second)
	resume()

	eventually(t, 30*time.Second, "ready again", func() bool {
		return call(t, "GET", app.URL+"/health/ready", "", nil, nil).Status == http.StatusOK
	})
	if r := submit(t, app.URL, o); r.Status != http.StatusOK || r.Body["idempotentReplay"] != false {
		t.Fatalf("after outage: %d %s", r.Status, r.Raw)
	}
	eventually(t, 30*time.Second, "message retried", func() bool {
		s, _ := txStatus(t, e.Owner, "provider-a", "outage-sqs")
		return s == "PROCESSED"
	})
	if e.balance(t, walletID) != 8500 {
		t.Fatalf("balance %d", e.balance(t, walletID))
	}
	e.assertConsistent(t)
}

func TestSQSOutage(t *testing.T) {
	e := newEnv(t)
	app := e.start(t, map[string]string{"OUTBOX_MAX_BACKOFF": "2s"})
	walletID, playerID := openWallet(t, app.URL, "100.00")
	eventually(t, 10*time.Second, "opening events published", func() bool {
		return queryInt(t, e.Owner, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})

	resume := pause(t, sqsContainer)
	if r := submit(t, app.URL, bet(walletID, playerID, "sqs-down", "10.00")); r.Status != http.StatusOK {
		t.Fatalf("bet during outage: %d %s", r.Status, r.Raw)
	}
	eventually(t, 30*time.Second, "failed publish recorded", func() bool {
		return queryInt(t, e.Owner, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL AND last_error IS NOT NULL`) == 2
	})
	if ready := call(t, "GET", app.URL+"/health/ready", "", nil, nil); ready.str("sqs") != "down" {
		t.Fatalf("readiness during outage: %s", ready.Raw)
	}
	resume()

	eventually(t, 30*time.Second, "events published after recovery", func() bool {
		return queryInt(t, e.Owner, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
	if ids := eventIDs(t, e.Queues.URLs.Events, 10*time.Second); len(ids) != 4 {
		t.Fatalf("events in queue: %d", len(ids))
	}
}
