package app

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"wallet/internal/domain"
	"wallet/internal/domain/money"
)

type fixture struct {
	t       *testing.T
	mem     *memory
	metrics *countingMetrics
	wager   *Wagering
	wallets *WalletService
	wallet  *domain.Wallet
}

func setup(t *testing.T, balance string) *fixture {
	t.Helper()
	f := &fixture{t: t, mem: newMemory(), metrics: &countingMetrics{}}
	f.wager, f.wallets = newServices(f.mem, f.metrics)
	initial, _ := money.Parse(balance, "BRL")
	w, err := f.wallets.Open(context.Background(), uuid.New(), initial, "corr-open")
	if err != nil {
		t.Fatal(err)
	}
	f.wallet = w
	return f
}

func (f *fixture) request(ext, kind, amount, ref string) domain.WagerRequest {
	f.t.Helper()
	r, err := domain.NewWagerRequest(domain.WagerInput{
		ProviderID: "provider-a", ExternalTransactionID: ext, IdempotencyKey: "provider-a:" + ext,
		PlayerID: f.wallet.PlayerID().String(), WalletID: f.wallet.ID().String(),
		RoundID: "round-1", GameID: "game-1", Kind: kind, Amount: amount, Currency: "BRL",
		ReferenceExternalTransactionID: ref,
	})
	if err != nil {
		f.t.Fatal(err)
	}
	return r
}

func (f *fixture) submit(r domain.WagerRequest) (Outcome, error) {
	return f.wager.Submit(context.Background(), Command{Request: r, CorrelationID: "corr-1", Source: "test"})
}

func TestOpenWalletStoresOpeningAtomically(t *testing.T) {
	f := setup(t, "1000.00")
	if len(f.mem.txs) != 1 || len(f.mem.ledger) != 1 || len(f.mem.outbox) != 2 {
		t.Fatalf("txs %d ledger %d outbox %d", len(f.mem.txs), len(f.mem.ledger), len(f.mem.outbox))
	}
	if f.mem.outbox[0].EventType != "WagerTransactionProcessed" || f.mem.outbox[1].EventType != "WalletBalanceChanged" {
		t.Fatalf("events %s %s", f.mem.outbox[0].EventType, f.mem.outbox[1].EventType)
	}
	if f.mem.outbox[0].CorrelationID != "corr-open" {
		t.Fatal("correlation id not propagated")
	}
	_, err := f.wallets.Open(context.Background(), f.wallet.PlayerID(), f.wallet.Balance(), "")
	if !errors.Is(err, ErrWalletExists) {
		t.Fatalf("second wallet: %v", err)
	}
}

func TestSubmitAndReplay(t *testing.T) {
	f := setup(t, "100.00")
	req := f.request("bet-1", "BET", "25.00", "")
	out, err := f.submit(req)
	if err != nil || out.Replay || out.Transaction.Status() != domain.StatusProcessed {
		t.Fatalf("first: %v %+v", err, out)
	}
	if _, err := f.submit(f.request("bet-2", "BET", "10.00", "")); err != nil {
		t.Fatal(err)
	}
	events := len(f.mem.outbox)

	again, err := f.submit(req)
	if err != nil || !again.Replay || again.Transaction.ID() != out.Transaction.ID() {
		t.Fatalf("replay: %v %+v", err, again)
	}
	if again.Transaction.BalanceAfter().Amount() != "75.00" {
		t.Fatalf("replay must return the original balance, got %s", again.Transaction.BalanceAfter())
	}
	if len(f.mem.ledger) != 3 || len(f.mem.outbox) != events {
		t.Fatal("replay wrote something")
	}
	if f.metrics.get("duplicate:idempotency_key") != 1 {
		t.Fatal("replay not counted")
	}
}

func TestPayloadConflicts(t *testing.T) {
	f := setup(t, "100.00")
	req := f.request("bet-1", "BET", "25.00", "")
	if _, err := f.submit(req); err != nil {
		t.Fatal(err)
	}

	changed := req
	changed.Money, _ = money.Parse("26.00", "BRL")
	if _, err := f.submit(changed); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("same key, other payload: %v", err)
	}
	otherKey := req
	otherKey.IdempotencyKey = "another"
	if _, err := f.submit(otherKey); !errors.Is(err, ErrExternalIDConflict) {
		t.Fatalf("same external id, other key: %v", err)
	}
	if len(f.mem.ledger) != 2 {
		t.Fatal("conflict moved money")
	}
}

func TestCorrectableErrorsStoreNothing(t *testing.T) {
	f := setup(t, "100.00")
	missing := f.request("bet-1", "BET", "1.00", "")
	missing.WalletID = uuid.New()
	if _, err := f.submit(missing); !errors.Is(err, ErrWalletNotFound) {
		t.Fatalf("missing wallet: %v", err)
	}
	other := f.request("bet-2", "BET", "1.00", "")
	other.PlayerID = uuid.New()
	if _, err := f.submit(other); !errors.Is(err, domain.ErrWalletPlayerMismatch) {
		t.Fatalf("player: %v", err)
	}
	if len(f.mem.txs) != 1 {
		t.Fatalf("stored %d transactions", len(f.mem.txs))
	}
}

func TestLostRaceIsRetried(t *testing.T) {
	f := setup(t, "100.00")
	f.mem.failInsert = []error{ErrConcurrentWrite}
	out, err := f.submit(f.request("bet-1", "BET", "10.00", ""))
	if err != nil || out.Transaction.Status() != domain.StatusProcessed {
		t.Fatalf("%v %+v", err, out)
	}
	if f.metrics.get("retry:transaction") != 1 || f.metrics.get("conflict:concurrent_write") != 1 {
		t.Fatalf("metrics %v", f.metrics.counts)
	}

	f.mem.failInsert = []error{ErrConcurrentWrite, ErrConcurrentWrite, ErrConcurrentWrite}
	if _, err := f.submit(f.request("bet-2", "BET", "10.00", "")); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("after max attempts: %v", err)
	}
}

func TestPermanentFailureIsRecorded(t *testing.T) {
	f := setup(t, "100.00")
	f.mem.failInsert = []error{errors.New("check constraint violated")}
	req := f.request("bet-1", "BET", "10.00", "")
	out, err := f.submit(req)
	if err != nil || out.Transaction.Status() != domain.StatusFailed || out.Transaction.FailureCode() != domain.FailureProcessingFailed {
		t.Fatalf("%v %+v", err, out)
	}
	again, err := f.submit(req)
	if err != nil || !again.Replay || again.Transaction.Status() != domain.StatusFailed {
		t.Fatalf("replay of failure: %v %+v", err, again)
	}
}

func TestMessageInbox(t *testing.T) {
	f := setup(t, "100.00")
	req := f.request("bet-1", "BET", "10.00", "")
	msg := Message{Consumer: "c", MessageID: "m-1", Hash: "h1", Command: Command{Request: req, Source: "sqs"}}

	first, err := f.wager.HandleMessage(context.Background(), msg)
	if err != nil || first.Duplicate {
		t.Fatalf("first: %v %+v", err, first)
	}
	second, err := f.wager.HandleMessage(context.Background(), msg)
	if err != nil || !second.Duplicate || second.Transaction.ID() != first.Transaction.ID() {
		t.Fatalf("redelivery: %v %+v", err, second)
	}
	msg.Hash = "h2"
	if _, err := f.wager.HandleMessage(context.Background(), msg); !errors.Is(err, ErrInboxConflict) {
		t.Fatalf("reused id: %v", err)
	}
	msg.MessageID, msg.Hash = "m-2", "h3"
	third, err := f.wager.HandleMessage(context.Background(), msg)
	if err != nil || third.Duplicate || !third.Replay {
		t.Fatalf("cross transport: %v %+v", err, third)
	}
	if len(f.mem.ledger) != 2 {
		t.Fatalf("ledger %d", len(f.mem.ledger))
	}
}

func TestPendingReferenceIsResolvedByWorker(t *testing.T) {
	f := setup(t, "100.00")
	out, err := f.submit(f.request("rb-1", "ROLLBACK", "30.00", "bet-1"))
	if err != nil || out.Transaction.Status() != domain.StatusPendingReference {
		t.Fatalf("%v %+v", err, out)
	}
	if n, _ := f.wager.ResolveDue(context.Background()); n != 1 {
		t.Fatalf("claimed %d", n)
	}
	if s := f.mem.txs[out.Transaction.ID()].Status; s != domain.StatusPendingReference {
		t.Fatalf("still missing reference: %s", s)
	}
	if _, err := f.submit(f.request("bet-1", "BET", "30.00", "")); err != nil {
		t.Fatal(err)
	}
	if _, err := f.wager.ResolveDue(context.Background()); err != nil {
		t.Fatal(err)
	}
	s := f.mem.txs[out.Transaction.ID()]
	if s.Status != domain.StatusProcessed || s.BalanceAfter.Amount() != "100.00" {
		t.Fatalf("resolved: %s %s", s.Status, s.BalanceAfter)
	}
}

func TestReconcile(t *testing.T) {
	f := setup(t, "100.00")
	f.submit(f.request("bet-1", "BET", "30.00", ""))
	r, err := f.wallets.Reconcile(context.Background(), f.wallet.ID())
	if err != nil || !r.Consistent || r.CheckedEntries != 2 || r.CalculatedBalance.Amount() != "70.00" {
		t.Fatalf("%v %+v", err, r)
	}

	s := f.mem.wallets[f.wallet.ID()]
	s.Balance, _ = money.Parse("71.00", "BRL")
	f.mem.wallets[f.wallet.ID()] = s
	r, _ = f.wallets.Reconcile(context.Background(), f.wallet.ID())
	if r.Consistent || r.Difference.Amount() != "1.00" || f.metrics.get("divergence") != 1 {
		t.Fatalf("divergence: %+v", r)
	}
}

func TestCursor(t *testing.T) {
	for _, v := range []int64{0, 1, 99} {
		got, err := decodeCursor(encodeCursor(v))
		if err != nil || got != v {
			t.Fatalf("%d: %d %v", v, got, err)
		}
	}
	for _, bad := range []string{"x", "djI6MQ", encodeCursor(-1)} {
		if _, err := decodeCursor(bad); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("%q: %v", bad, err)
		}
	}
}
