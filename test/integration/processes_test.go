//go:build integration

package integration

import (
	"bufio"
	"fmt"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"wallet/internal/fault"
)

func metric(t *testing.T, baseURL, series string) float64 {
	t.Helper()
	resp, err := http.Get(baseURL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		if strings.HasPrefix(sc.Text(), series+" ") {
			v, _ := strconv.ParseFloat(strings.TrimPrefix(sc.Text(), series+" "), 64)
			return v
		}
	}
	return 0
}

func TestThreeInstances(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	procs := []*process{e.spawn(t, nil), e.spawn(t, nil), e.spawn(t, nil)}
	url := func(i int) string { return procs[i%len(procs)].URL }

	t.Run("two bets of 80 over 100", func(t *testing.T) {
		walletID, playerID := openWallet(t, url(0), "100.00")
		bets := []op{bet(walletID, playerID, "p-race-1", "80.00"), bet(walletID, playerID, "p-race-2", "80.00")}
		results := make([]response, 2)
		parallel(2, func(i int) { results[i] = submit(t, url(i), bets[i]) })
		checkRace(t, e, walletID, results)
		parallel(6, func(i int) {
			r := submit(t, url(i), bets[i%2])
			if r.str("transactionId") != results[i%2].str("transactionId") || r.Body["idempotentReplay"] != true {
				t.Errorf("resend: %s", r.Raw)
			}
		})
		checkRace(t, e, walletID, results)
	})

	t.Run("same bet 50 times across instances", func(t *testing.T) {
		walletID, playerID := openWallet(t, url(1), "100.00")
		o := bet(walletID, playerID, "p-dup", "30.00")
		var replays atomic.Int32
		parallel(50, func(i int) {
			r := submit(t, url(i), o)
			if r.Status != http.StatusOK {
				t.Errorf("%d %s", r.Status, r.Raw)
			}
			if r.Body["idempotentReplay"] == true {
				replays.Add(1)
			}
		})
		if replays.Load() != 49 || e.ledgerEntries(t, walletID) != 2 || e.balance(t, walletID) != 7000 {
			t.Fatalf("replays %d entries %d balance %d", replays.Load(), e.ledgerEntries(t, walletID), e.balance(t, walletID))
		}
	})

	t.Run("wallets in parallel over HTTP and SQS", func(t *testing.T) {
		type w struct{ id, player string }
		ws := make([]w, 12)
		for i := range ws {
			ws[i].id, ws[i].player = openWallet(t, url(i), "100.00")
		}
		var ops []op
		for i := 0; i < 60; i++ {
			target := ws[i%len(ws)]
			ops = append(ops, bet(target.id, target.player, fmt.Sprintf("p-mix-%d", i), "3.00"))
		}
		for i, o := range ops {
			sendMessage(t, e.Queues.URLs.Wager, o.message(fmt.Sprintf("m-mix-%d", i)), o.Wallet, fmt.Sprintf("m-mix-%d", i))
		}
		parallel(len(ops), func(i int) {
			if r := submit(t, url(i), ops[i]); r.Status != http.StatusOK {
				t.Errorf("%d %s", r.Status, r.Raw)
			}
		})
		eventually(t, 30*time.Second, "all messages consumed", func() bool {
			return queryInt(t, e.Owner, `SELECT count(*) FROM inbox_messages WHERE message_id LIKE 'm-mix-%' AND completed_at IS NOT NULL`) == int64(len(ops))
		})
		for _, target := range ws {
			if e.ledgerEntries(t, target.id) != 6 || e.balance(t, target.id) != 8500 {
				t.Errorf("wallet %s: entries %d balance %d", target.id, e.ledgerEntries(t, target.id), e.balance(t, target.id))
			}
		}
		consumed := 0.0
		for _, p := range procs {
			consumed += metric(t, p.URL, `sqs_messages_total{outcome="processed"}`)
		}
		if consumed < float64(len(ops)) {
			t.Errorf("consumers handled %v messages", consumed)
		}
	})

	e.assertConsistent(t)
}

func TestConsumerCrashAfterCommit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	crashing := e.spawn(t, map[string]string{"FAULT_POINTS": "consumer.after_commit"})
	walletID, playerID := openWallet(t, crashing.URL, "100.00")

	sendMessage(t, e.Queues.URLs.Wager, bet(walletID, playerID, "crash-commit", "10.00").message("m-crash-commit"), walletID, "m-crash-commit")
	code, exited := crashing.ExitCode(20 * time.Second)
	if !exited || code != fault.ExitCode {
		t.Fatalf("expected injected crash, exited=%v code=%d", exited, code)
	}
	if s, _ := txStatus(t, e.Owner, "provider-a", "crash-commit"); s != "PROCESSED" {
		t.Fatalf("commit before crash: %s", s)
	}

	survivor := e.spawn(t, nil)
	eventually(t, 30*time.Second, "redelivery handled", func() bool {
		return metric(t, survivor.URL, `sqs_messages_total{outcome="duplicate"}`) == 1
	})
	eventually(t, 10*time.Second, "queue drained", func() bool { return queueDepth(t, e.Queues.URLs.Wager) == 0 })
	if e.ledgerEntries(t, walletID) != 2 || e.balance(t, walletID) != 9000 {
		t.Fatalf("entries %d balance %d", e.ledgerEntries(t, walletID), e.balance(t, walletID))
	}
	e.assertConsistent(t)
}

func TestCrashBeforeCommit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	crashing := e.spawn(t, map[string]string{"FAULT_POINTS": "wager.before_commit"})
	walletID, playerID := openWallet(t, crashing.URL, "100.00")
	o := bet(walletID, playerID, "crash-http", "10.00")

	if _, err := tryCall("POST", crashing.URL+"/wagering/transactions", token(t, "provider-a"), map[string]string{"Idempotency-Key": o.key()}, o.body()); err == nil {
		t.Fatal("expected the connection to drop")
	}
	if code, _ := crashing.ExitCode(10 * time.Second); code != fault.ExitCode {
		t.Fatalf("exit code %d", code)
	}
	if s, _ := txStatus(t, e.Owner, "provider-a", "crash-http"); s != "" {
		t.Fatalf("uncommitted work visible: %s", s)
	}

	crashing2 := e.spawn(t, map[string]string{"FAULT_POINTS": "wager.before_commit"})
	m := bet(walletID, playerID, "crash-sqs", "5.00")
	sendMessage(t, e.Queues.URLs.Wager, m.message("m-crash-sqs"), walletID, "m-crash-sqs")
	if code, _ := crashing2.ExitCode(20 * time.Second); code != fault.ExitCode {
		t.Fatalf("exit code %d", code)
	}
	if n := queryInt(t, e.Owner, `SELECT count(*) FROM inbox_messages`); n != 0 {
		t.Fatalf("inbox rows %d", n)
	}

	healthy := e.spawn(t, nil)
	r := submit(t, healthy.URL, o)
	if r.Status != http.StatusOK || r.Body["idempotentReplay"] != false {
		t.Fatalf("retry: %d %s", r.Status, r.Raw)
	}
	eventually(t, 30*time.Second, "message redelivered", func() bool {
		s, _ := txStatus(t, e.Owner, "provider-a", "crash-sqs")
		return s == "PROCESSED"
	})
	if e.balance(t, walletID) != 8500 || e.ledgerEntries(t, walletID) != 3 {
		t.Fatalf("balance %d entries %d", e.balance(t, walletID), e.ledgerEntries(t, walletID))
	}
	e.assertConsistent(t)
}

func TestPublisherCrashAfterPublish(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	crashing := e.spawnNoWait(t, map[string]string{"FAULT_POINTS": "outbox.after_publish", "OUTBOX_LEASE": "2s"})
	if err := crashing.waitReady(60 * time.Second); err != nil {
		t.Fatal(err)
	}
	player := "0192f28f-5dc0-7d58-bdb2-" + fmt.Sprintf("%012d", time.Now().UnixNano()%1e12)
	call(t, "POST", crashing.URL+"/wallets", token(t, "wallet-internal"), nil, map[string]any{
		"playerId": player, "initialBalance": map[string]string{"amount": "10.00", "currency": "BRL"},
	})
	if code, _ := crashing.ExitCode(20 * time.Second); code != fault.ExitCode {
		t.Fatalf("exit code %d", code)
	}
	if n := queryInt(t, e.Owner, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`); n != 2 {
		t.Fatalf("pending %d", n)
	}

	e.spawn(t, nil)
	eventually(t, 20*time.Second, "events published", func() bool {
		return queryInt(t, e.Owner, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})
	if n := queryInt(t, e.Owner, `SELECT max(attempts) FROM outbox_events`); n != 2 {
		t.Fatalf("max attempts %d", n)
	}
	ids := eventIDs(t, e.Queues.URLs.Events, 10*time.Second)
	if len(ids) != 2 {
		t.Fatalf("events in queue %v", ids)
	}
	for id, n := range ids {
		if n != 1 {
			t.Fatalf("event %s delivered %d times", id, n)
		}
	}
}

func TestRestartKeepsState(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	first := e.spawn(t, nil)
	walletID, playerID := openWallet(t, first.URL, "100.00")
	b := bet(walletID, playerID, "restart-bet", "20.00")
	original := submit(t, first.URL, b)
	rb := b.with("ROLLBACK", "restart-rb", "5.00", "restart-late")
	if r := submit(t, first.URL, rb); r.Status != http.StatusAccepted {
		t.Fatalf("pending: %d %s", r.Status, r.Raw)
	}
	submit(t, first.URL, b.with("BET", "restart-bet-2", "10.00", ""))
	first.Kill()

	second := e.spawn(t, nil)
	r := submit(t, second.URL, b)
	if r.Body["idempotentReplay"] != true || r.str("transactionId") != original.str("transactionId") || r.str("balance", "amount") != "80.00" {
		t.Fatalf("replay after restart: %s", r.Raw)
	}
	if r := submit(t, second.URL, b.with("BET", "restart-late", "5.00", "")); r.Status != http.StatusOK {
		t.Fatal(r.Raw)
	}
	eventually(t, 15*time.Second, "pending rollback resumed by the new process", func() bool {
		s, _ := txStatus(t, e.Owner, "provider-a", "restart-rb")
		return s == "PROCESSED"
	})
	rec := call(t, "POST", second.URL+"/wallets/"+walletID+"/reconciliation", token(t, "wallet-internal"), nil, nil)
	if rec.Body["consistent"] != true || rec.str("storedBalance", "amount") != "70.00" {
		t.Fatalf("reconciliation: %s", rec.Raw)
	}
	e.assertConsistent(t)
}

func TestGracefulShutdownOnSIGTERM(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM cannot be sent to a process on Windows; covered by TestFxStartAndStopReleaseEverything")
	}
	t.Parallel()
	e := newEnv(t)
	p := e.spawn(t, nil)
	walletID, playerID := openWallet(t, p.URL, "100.00")
	submit(t, p.URL, bet(walletID, playerID, "term-1", "1.00"))

	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	code, exited := p.ExitCode(30 * time.Second)
	if !exited || code != 0 {
		t.Fatalf("exit %v code %d\n%s", exited, code, p.logs.String())
	}
	logs := p.logs.String()
	last := -1
	for _, msg := range []string{"http server stopped", "consumer stopped", "worker stopped", "database pool closed"} {
		i := strings.Index(logs, `"msg":"`+msg+`"`)
		if i < last || i < 0 {
			t.Fatalf("shutdown order broken at %q:\n%s", msg, logs)
		}
		last = i
	}
}
