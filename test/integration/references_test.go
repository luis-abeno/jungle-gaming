//go:build integration

package integration

import (
	"net/http"
	"testing"
	"time"
)

func TestRollbackBeforeBetIsResolved(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "100.00")
	b := bet(walletID, playerID, "early-bet", "30.00")
	rb := b.with("ROLLBACK", "early-rollback", "30.00", "early-bet")

	r := submit(t, app.URL, rb)
	if r.Status != http.StatusAccepted || r.str("status") != "PENDING_REFERENCE" || r.str("retryDeadline") == "" {
		t.Fatalf("rollback first: %d %s", r.Status, r.Raw)
	}
	if again := submit(t, app.URL, rb); again.Status != http.StatusAccepted || again.Body["idempotentReplay"] != true {
		t.Fatalf("pending replay: %d %s", again.Status, again.Raw)
	}
	if r := submit(t, app.URL, b); r.Status != http.StatusOK {
		t.Fatalf("bet: %d %s", r.Status, r.Raw)
	}
	eventually(t, 10*time.Second, "rollback settled", func() bool {
		s, _ := txStatus(t, e.Owner, "provider-a", "early-rollback")
		return s == "PROCESSED"
	})
	got := call(t, "GET", app.URL+"/providers/provider-a/wagering/transactions/early-rollback", token(t, "provider-a"), nil, nil)
	if got.str("balance", "amount") != "100.00" || got.str("referenceTransactionId") == "" {
		t.Fatalf("rollback view: %s", got.Raw)
	}
	if e.balance(t, walletID) != 10000 || e.ledgerEntries(t, walletID) != 3 {
		t.Fatalf("balance %d entries %d", e.balance(t, walletID), e.ledgerEntries(t, walletID))
	}
	events := queryInt(t, e.Owner, `SELECT count(*) FROM outbox_events WHERE event_type = 'WagerTransactionPendingReference'`)
	if events != 1 {
		t.Fatalf("pending events %d", events)
	}
	e.assertConsistent(t)
}

func TestMissingReferenceExpires(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, map[string]string{"REFERENCE_TTL": "2s"})
	walletID, playerID := openWallet(t, app.URL, "100.00")

	refund := bet(walletID, playerID, "", "").with("REFUND", "orphan-refund", "10.00", "never-sent")
	if r := submit(t, app.URL, refund); r.Status != http.StatusAccepted {
		t.Fatalf("refund: %d %s", r.Status, r.Raw)
	}
	eventually(t, 10*time.Second, "refund rejected", func() bool {
		s, _ := txStatus(t, e.Owner, "provider-a", "orphan-refund")
		return s == "REJECTED"
	})
	if _, code := txStatus(t, e.Owner, "provider-a", "orphan-refund"); code != "REFERENCE_NOT_FOUND" {
		t.Fatalf("failure code %s", code)
	}
	r := submit(t, app.URL, refund)
	if r.Status != http.StatusUnprocessableEntity || r.str("failureCode") != "REFERENCE_NOT_FOUND" {
		t.Fatalf("replay after expiry: %d %s", r.Status, r.Raw)
	}
	if n := queryInt(t, e.Owner, `SELECT count(*) FROM outbox_events WHERE event_type = 'WagerTransactionRejected'`); n != 1 {
		t.Fatalf("rejected events %d", n)
	}
	if r := submit(t, app.URL, bet(walletID, playerID, "never-sent", "10.00")); r.Status != http.StatusOK {
		t.Fatal(r.Raw)
	}
	if e.balance(t, walletID) != 9000 {
		t.Fatalf("balance %d", e.balance(t, walletID))
	}
}

func TestRefundAndRollbackCombinations(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "100.00")
	b := bet(walletID, playerID, "combo-bet", "40.00")
	expect := func(o op, status int, failure string) response {
		t.Helper()
		r := submit(t, app.URL, o)
		if r.Status != status || r.str("failureCode") != failure {
			t.Fatalf("%s %s: %d %s", o.Kind, o.External, r.Status, r.Raw)
		}
		return r
	}

	expect(b, http.StatusOK, "")
	expect(b.with("REFUND", "combo-refund", "40.00", "combo-bet"), http.StatusOK, "")
	expect(b.with("ROLLBACK", "combo-rb-bet", "40.00", "combo-bet"), http.StatusUnprocessableEntity, "ALREADY_REVERSED")
	expect(b.with("REFUND", "combo-refund-2", "40.00", "combo-bet"), http.StatusUnprocessableEntity, "ALREADY_REVERSED")
	expect(b.with("ROLLBACK", "combo-rb-refund", "40.00", "combo-refund"), http.StatusOK, "")
	expect(b.with("ROLLBACK", "combo-rb-refund-2", "40.00", "combo-refund"), http.StatusUnprocessableEntity, "ALREADY_REVERSED")
	expect(b.with("ROLLBACK", "combo-rb-rb", "40.00", "combo-rb-refund"), http.StatusUnprocessableEntity, "INVALID_REFERENCE_KIND")
	win := b.with("WIN", "combo-win", "15.00", "combo-bet")
	expect(win, http.StatusOK, "")
	expect(b.with("ROLLBACK", "combo-rb-win", "10.00", "combo-win"), http.StatusUnprocessableEntity, "REFERENCE_AMOUNT_MISMATCH")
	otherRound := b.with("REFUND", "combo-round", "40.00", "combo-bet")
	otherRound.Round = "round-2"
	expect(otherRound, http.StatusUnprocessableEntity, "REFERENCE_MISMATCH")

	if e.balance(t, walletID) != 7500 {
		t.Fatalf("balance %d", e.balance(t, walletID))
	}
	e.assertConsistent(t)
}

func TestReversalWithoutFundsIsRejected(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "0.00")
	b := bet(walletID, playerID, "", "")

	if r := submit(t, app.URL, b.with("WIN", "nf-win", "50.00", "")); r.Status != http.StatusOK {
		t.Fatal(r.Raw)
	}
	if r := submit(t, app.URL, b.with("BET", "nf-bet", "45.00", "")); r.Status != http.StatusOK {
		t.Fatal(r.Raw)
	}
	r := submit(t, app.URL, b.with("ROLLBACK", "nf-rb", "50.00", "nf-win"))
	if r.Status != http.StatusUnprocessableEntity || r.str("failureCode") != "INSUFFICIENT_FUNDS_FOR_REVERSAL" {
		t.Fatalf("rollback: %d %s", r.Status, r.Raw)
	}
	r = submit(t, app.URL, b.with("BET", "nf-bet-2", "50.00", ""))
	if r.str("failureCode") != "INSUFFICIENT_FUNDS" {
		t.Fatalf("bet: %s", r.Raw)
	}
	var view = call(t, "GET", app.URL+"/providers/provider-a/wagering/transactions/nf-rb", token(t, "wallet-internal"), nil, nil)
	if view.str("status") != "REJECTED" || view.str("failureCode") != "INSUFFICIENT_FUNDS_FOR_REVERSAL" {
		t.Fatalf("audit view: %s", view.Raw)
	}
	e.assertConsistent(t)
}

func TestLossAndZeroOpening(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "0.00")
	if n := queryInt(t, e.Owner, `SELECT count(*) FROM wager_transactions`); n != 0 {
		t.Fatal("zero opening created a transaction")
	}
	if n := queryInt(t, e.Owner, `SELECT count(*) FROM outbox_events`); n != 0 {
		t.Fatal("zero opening created events")
	}

	r := submit(t, app.URL, bet(walletID, playerID, "", "").with("LOSS", "loss-1", "0.00", ""))
	if r.Status != http.StatusOK || r.str("balance", "amount") != "0.00" {
		t.Fatalf("loss: %d %s", r.Status, r.Raw)
	}
	if v := queryInt(t, e.Owner, `SELECT version FROM wallets WHERE id = $1`, walletID); v != 1 {
		t.Fatalf("version %d", v)
	}
	if e.ledgerEntries(t, walletID) != 0 {
		t.Fatal("LOSS created a ledger entry")
	}
	types := queryInt(t, e.Owner, `SELECT count(*) FROM outbox_events WHERE event_type = 'WalletBalanceChanged'`)
	processed := queryInt(t, e.Owner, `SELECT count(*) FROM outbox_events WHERE event_type = 'WagerTransactionProcessed'`)
	if types != 0 || processed != 1 {
		t.Fatalf("events: balance %d processed %d", types, processed)
	}

	again := call(t, "POST", app.URL+"/wallets", token(t, "wallet-internal"), nil, map[string]any{
		"playerId": playerID, "initialBalance": map[string]string{"amount": "5.00", "currency": "BRL"},
	})
	if again.Status != http.StatusConflict || again.str("error", "code") != "WALLET_ALREADY_EXISTS" {
		t.Fatalf("second opening: %d %s", again.Status, again.Raw)
	}
}

func TestLedgerPaginationAndReconciliation(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	app := e.start(t, nil)
	walletID, playerID := openWallet(t, app.URL, "1000.00")
	for i := 0; i < 7; i++ {
		submit(t, app.URL, bet(walletID, playerID, "page-"+string(rune('a'+i)), "25.00"))
	}
	internal := token(t, "wallet-internal")
	var versions []float64
	cursor := ""
	for pages := 0; ; pages++ {
		r := call(t, "GET", app.URL+"/wallets/"+walletID+"/ledger?limit=3&cursor="+cursor, internal, nil, nil)
		if r.Status != http.StatusOK {
			t.Fatal(r.Raw)
		}
		for _, it := range r.Body["items"].([]any) {
			versions = append(versions, it.(map[string]any)["walletVersion"].(float64))
		}
		next, _ := r.Body["nextCursor"].(string)
		if next == "" {
			break
		}
		cursor = next
		if pages > 5 {
			t.Fatal("pagination does not end")
		}
	}
	if len(versions) != 8 {
		t.Fatalf("entries %v", versions)
	}
	for i, v := range versions {
		if int(v) != i+1 {
			t.Fatalf("order %v", versions)
		}
	}
	if r := call(t, "GET", app.URL+"/wallets/"+walletID+"/ledger?cursor=bogus", internal, nil, nil); r.Status != http.StatusBadRequest {
		t.Fatalf("bad cursor: %d", r.Status)
	}

	rec := call(t, "POST", app.URL+"/wallets/"+walletID+"/reconciliation", internal, nil, nil)
	if rec.Body["consistent"] != true || rec.str("storedBalance", "amount") != "825.00" ||
		rec.str("difference", "amount") != "0.00" || rec.Body["checkedEntries"].(float64) != 8 {
		t.Fatalf("reconciliation: %s", rec.Raw)
	}
}
