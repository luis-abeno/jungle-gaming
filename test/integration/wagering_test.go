//go:build integration

package integration

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestReplayReturnsOriginalResult(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "1000.00")

	first := submit(t, app.URL, bet(walletID, playerID, "replay-1", "25.00"))
	if first.Status != http.StatusOK || first.str("balance", "amount") != "975.00" || first.Body["idempotentReplay"] != false {
		t.Fatalf("first: %d %s", first.Status, first.Raw)
	}
	if r := submit(t, app.URL, bet(walletID, playerID, "replay-2", "100.00")); r.str("balance", "amount") != "875.00" {
		t.Fatalf("second: %s", r.Raw)
	}
	again := submit(t, app.URL, bet(walletID, playerID, "replay-1", "25.00"))
	if again.Status != http.StatusOK || again.Body["idempotentReplay"] != true ||
		again.str("transactionId") != first.str("transactionId") || again.str("balance", "amount") != "975.00" {
		t.Fatalf("replay: %d %s", again.Status, again.Raw)
	}
	if e.ledgerEntries(t, walletID) != 3 || e.balance(t, walletID) != 87500 {
		t.Fatal("replay moved money")
	}

	big := bet(walletID, playerID, "replay-3", "5000.00")
	r1, r2 := submit(t, app.URL, big), submit(t, app.URL, big)
	if r1.Status != http.StatusUnprocessableEntity || r2.Status != http.StatusUnprocessableEntity ||
		r2.str("failureCode") != "INSUFFICIENT_FUNDS" || r2.Body["idempotentReplay"] != true {
		t.Fatalf("rejection replay: %s / %s", r1.Raw, r2.Raw)
	}
}

func TestIdempotencyConflicts(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "100.00")

	o := bet(walletID, playerID, "conflict-1", "10.00")
	if r := submit(t, app.URL, o); r.Status != http.StatusOK {
		t.Fatal(r.Raw)
	}

	changed := o
	changed.Amount = "11.00"
	if r := submit(t, app.URL, changed); r.Status != http.StatusConflict || r.str("error", "code") != "IDEMPOTENCY_KEY_CONFLICT" {
		t.Fatalf("same key, other payload: %d %s", r.Status, r.Raw)
	}

	otherKey := o
	otherKey.Key = "another-key"
	if r := submit(t, app.URL, otherKey); r.Status != http.StatusConflict || r.str("error", "code") != "EXTERNAL_TRANSACTION_CONFLICT" {
		t.Fatalf("same external id, other key: %d %s", r.Status, r.Raw)
	}

	custom := bet(walletID, playerID, "conflict-2", "1.00")
	custom.Key = "client-chosen-key-1"
	if r := submit(t, app.URL, custom); r.Status != http.StatusOK {
		t.Fatal(r.Raw)
	}
	var key string
	e.Owner.QueryRow(context.Background(), `SELECT idempotency_key FROM wager_transactions WHERE external_transaction_id = 'conflict-2'`).Scan(&key)
	if key != "client-chosen-key-1" {
		t.Fatalf("stored key %q", key)
	}
	if e.ledgerEntries(t, walletID) != 3 {
		t.Fatalf("ledger entries: %d", e.ledgerEntries(t, walletID))
	}
}

func TestSameBetFiftyTimesInParallel(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "100.00")
	o := bet(walletID, playerID, "dup-50", "30.00")

	var replays atomic.Int32
	ids := sync.Map{}
	parallel(50, func(int) {
		r := submit(t, app.URL, o)
		if r.Status != http.StatusOK || r.str("balance", "amount") != "70.00" {
			t.Errorf("status %d %s", r.Status, r.Raw)
			return
		}
		ids.Store(r.str("transactionId"), true)
		if r.Body["idempotentReplay"] == true {
			replays.Add(1)
		}
	})
	n := 0
	ids.Range(func(any, any) bool { n++; return true })
	if n != 1 || replays.Load() != 49 {
		t.Fatalf("distinct ids %d, replays %d", n, replays.Load())
	}
	if debits := queryInt(t, e.Owner, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID); debits != 1 {
		t.Fatalf("debits: %d", debits)
	}
	if e.balance(t, walletID) != 7000 {
		t.Fatalf("balance %d", e.balance(t, walletID))
	}
	e.assertConsistent(t)
}

func TestTwoBetsRaceForTheSameBalance(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "100.00")
	bets := []op{bet(walletID, playerID, "race-1", "80.00"), bet(walletID, playerID, "race-2", "80.00")}

	results := make([]response, 2)
	parallel(2, func(i int) { results[i] = submit(t, app.URL, bets[i]) })
	checkRace(t, e, walletID, results)

	parallel(2, func(i int) {
		r := submit(t, app.URL, bets[i])
		if r.Status != results[i].Status || r.str("transactionId") != results[i].str("transactionId") || r.Body["idempotentReplay"] != true {
			t.Errorf("resend %d: %s vs %s", i, r.Raw, results[i].Raw)
		}
	})
	checkRace(t, e, walletID, results)
}

func checkRace(t *testing.T, e *testEnv, walletID string, results []response) {
	t.Helper()
	processed, rejected := 0, 0
	for _, r := range results {
		switch {
		case r.Status == http.StatusOK && r.str("status") == "PROCESSED" && r.str("balance", "amount") == "20.00":
			processed++
		case r.Status == http.StatusUnprocessableEntity && r.str("failureCode") == "INSUFFICIENT_FUNDS":
			rejected++
		default:
			t.Errorf("unexpected result %d %s", r.Status, r.Raw)
		}
	}
	if processed != 1 || rejected != 1 {
		t.Fatalf("processed %d rejected %d", processed, rejected)
	}
	if e.balance(t, walletID) != 2000 {
		t.Fatalf("balance %d", e.balance(t, walletID))
	}
	if n := queryInt(t, e.Owner, `SELECT count(*) FROM wallet_ledger_entries WHERE wallet_id = $1 AND direction = 'DEBIT'`, walletID); n != 1 {
		t.Fatalf("debits %d", n)
	}
	e.assertConsistent(t)
}

func TestNoLostUpdates(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "100.00")

	var ok, rejected atomic.Int32
	parallel(120, func(i int) {
		r := submit(t, app.URL, bet(walletID, playerID, fmt.Sprintf("lost-%d", i), "1.00"))
		switch r.Status {
		case http.StatusOK:
			ok.Add(1)
		case http.StatusUnprocessableEntity:
			rejected.Add(1)
		default:
			t.Errorf("status %d %s", r.Status, r.Raw)
		}
	})
	if ok.Load() != 100 || rejected.Load() != 20 {
		t.Fatalf("processed %d rejected %d", ok.Load(), rejected.Load())
	}
	if e.balance(t, walletID) != 0 {
		t.Fatalf("balance %d", e.balance(t, walletID))
	}
	if v := queryInt(t, e.Owner, `SELECT version FROM wallets WHERE id = $1`, walletID); v != 101 {
		t.Fatalf("version %d", v)
	}
	e.assertConsistent(t)
}

func TestWalletsProgressIndependently(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, map[string]string{"DB_LOCK_TIMEOUT": "10s", "REQUEST_TIMEOUT": "15s"})
	blockedID, blockedPlayer := openWallet(t, app.URL, "100.00")
	freeID, freePlayer := openWallet(t, app.URL, "100.00")

	ctx := context.Background()
	tx, err := e.Owner.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `SELECT 1 FROM wallets WHERE id = $1 FOR UPDATE`, blockedID); err != nil {
		t.Fatal(err)
	}

	blockedDone := make(chan response, 1)
	go func() { blockedDone <- submit(t, app.URL, bet(blockedID, blockedPlayer, "blocked-1", "10.00")) }()

	start := time.Now()
	for i := 0; i < 10; i++ {
		if r := submit(t, app.URL, bet(freeID, freePlayer, fmt.Sprintf("free-%d", i), "1.00")); r.Status != http.StatusOK {
			t.Fatalf("free wallet: %d %s", r.Status, r.Raw)
		}
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("free wallet waited %s", elapsed)
	}
	select {
	case r := <-blockedDone:
		t.Fatalf("blocked wallet finished while locked: %s", r.Raw)
	default:
	}

	tx.Rollback(ctx)
	r := <-blockedDone
	if r.Status != http.StatusOK || r.str("balance", "amount") != "90.00" {
		t.Fatalf("blocked wallet after release: %d %s", r.Status, r.Raw)
	}
}

func TestManyWalletsInParallel(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	const wallets, ops = 20, 10
	type w struct{ id, player string }
	ws := make([]w, wallets)
	for i := range ws {
		ws[i].id, ws[i].player = openWallet(t, app.URL, "50.00")
	}
	parallel(wallets*ops, func(i int) {
		target := ws[i%wallets]
		o := bet(target.id, target.player, fmt.Sprintf("many-%d", i), "5.00")
		if i%3 == 0 {
			o = o.with("WIN", o.External, "2.50", "")
		}
		if r := submit(t, app.URL, o); r.Status != http.StatusOK {
			t.Errorf("%d %s", r.Status, r.Raw)
		}
	})
	e.assertConsistent(t)
	for _, target := range ws {
		if e.ledgerEntries(t, target.id) != ops+1 {
			t.Errorf("wallet %s: %d entries", target.id, e.ledgerEntries(t, target.id))
		}
	}
}

func TestValidationErrors(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "100.00")
	o := bet(walletID, playerID, "val-1", "10.00")
	tok := token(t, "provider-a")

	send := func(body any, key string) response {
		return call(t, "POST", app.URL+"/wagering/transactions", tok, map[string]string{"Idempotency-Key": key}, body)
	}
	cases := []struct {
		name string
		body any
		key  string
		code string
	}{
		{"missing key", o.body(), "", "INVALID_IDEMPOTENCY_KEY"},
		{"float amount", `{"providerId":"provider-a","externalTransactionId":"v","playerId":"` + playerID + `","walletId":"` + walletID + `","roundId":"r","gameId":"g","kind":"BET","money":{"amount":25.00,"currency":"BRL"}}`, "k1", "MALFORMED_REQUEST"},
		{"unknown field", `{"providerId":"provider-a","foo":1}`, "k2", "MALFORMED_REQUEST"},
		{"scientific", o.with("BET", "v3", "1e2", "").body(), "k3", "VALIDATION_ERROR"},
		{"negative", o.with("BET", "v4", "-1.00", "").body(), "k4", "VALIDATION_ERROR"},
		{"excess scale", o.with("BET", "v5", "1.001", "").body(), "k5", "VALIDATION_ERROR"},
		{"opening kind", o.with("OPENING", "v6", "1.00", "").body(), "k6", "VALIDATION_ERROR"},
		{"zero bet", o.with("BET", "v7", "0.00", "").body(), "k7", "VALIDATION_ERROR"},
		{"loss with amount", o.with("LOSS", "v8", "1.00", "").body(), "k8", "VALIDATION_ERROR"},
		{"refund without ref", o.with("REFUND", "v9", "1.00", "").body(), "k9", "VALIDATION_ERROR"},
	}
	for _, c := range cases {
		r := send(c.body, c.key)
		if r.Status != http.StatusBadRequest || r.str("error", "code") != c.code {
			t.Errorf("%s: %d %s", c.name, r.Status, r.Raw)
		}
	}

	usd := o.with("BET", "v10", "1.00", "")
	usd.Currency = "USD"
	if r := submit(t, app.URL, usd); r.Status != http.StatusBadRequest || r.str("error", "code") != "CURRENCY_MISMATCH" {
		t.Errorf("currency: %d %s", r.Status, r.Raw)
	}
	other := o.with("BET", "v11", "1.00", "")
	other.Player = "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1"
	if r := submit(t, app.URL, other); r.Status != http.StatusBadRequest || r.str("error", "code") != "WALLET_PLAYER_MISMATCH" {
		t.Errorf("player: %d %s", r.Status, r.Raw)
	}
	missing := o.with("BET", "v12", "1.00", "")
	missing.Wallet = "0192f291-27dd-7d3f-8071-5f8685deef37"
	if r := submit(t, app.URL, missing); r.Status != http.StatusNotFound || r.str("error", "code") != "WALLET_NOT_FOUND" {
		t.Errorf("wallet: %d %s", r.Status, r.Raw)
	}
	if n := queryInt(t, e.Owner, `SELECT count(*) FROM wager_transactions WHERE origin = 'EXTERNAL'`); n != 0 {
		t.Fatalf("stored %d rows", n)
	}
	fixed := o.with("BET", "v10", "1.00", "")
	if r := submit(t, app.URL, fixed); r.Status != http.StatusOK {
		t.Fatalf("fixed request: %d %s", r.Status, r.Raw)
	}
}
