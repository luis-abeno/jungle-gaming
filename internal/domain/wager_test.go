package domain

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestNewTransactionStartsPending(t *testing.T) {
	w := openWallet(t, "100.00")
	tx := newTx(t, input(w, "bet-1", KindBet, "10.00", ""))
	if tx.Status() != StatusPending || tx.Origin() != OriginExternal || tx.PayloadHash() == "" {
		t.Fatalf("new tx: %s %s", tx.Status(), tx.Origin())
	}
	if len(tx.PullEvents()) != 0 {
		t.Fatal("creation must not record events")
	}
}

func TestTransitions(t *testing.T) {
	w := openWallet(t, "100.00")
	bal := brl(t, "90.00")

	tx := newTx(t, input(w, "bet-1", KindBet, "10.00", ""))
	if err := tx.MarkProcessed(bal, t0); err != nil {
		t.Fatal(err)
	}
	for _, fn := range []func() error{
		func() error { return tx.MarkProcessed(bal, t0) },
		func() error { return tx.Reject(FailureInsufficientFunds, t0) },
		func() error { return tx.Fail(t0) },
		func() error { return tx.AwaitReference(t0.Add(time.Second), t0.Add(time.Minute), t0) },
		func() error { return tx.RetryReferenceLater(t0.Add(time.Second), t0) },
	} {
		if err := fn(); !errors.Is(err, ErrInvalidTransition) {
			t.Errorf("terminal tx accepted a transition: %v", err)
		}
	}

	rej := newTx(t, input(w, "bet-2", KindBet, "10.00", ""))
	if err := rej.Reject(FailureProcessingFailed, t0); !errors.Is(err, ErrInvariant) {
		t.Fatalf("PROCESSING_FAILED is not a rejection code: %v", err)
	}
	if err := rej.Reject(FailureInsufficientFunds, t0); err != nil || rej.Status() != StatusRejected {
		t.Fatalf("reject: %v", err)
	}
	if err := rej.MarkProcessed(bal, t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("rejected -> processed: %v", err)
	}

	failed := newTx(t, input(w, "bet-3", KindBet, "10.00", ""))
	if err := failed.Fail(t0); err != nil || failed.Status() != StatusFailed || failed.FailureCode() != FailureProcessingFailed {
		t.Fatalf("fail: %v", err)
	}
	if len(failed.PullEvents()) != 0 {
		t.Fatal("FAILED does not publish events")
	}
}

func TestPendingReferenceTransitions(t *testing.T) {
	w := openWallet(t, "100.00")
	bet := newTx(t, input(w, "bet-1", KindBet, "10.00", ""))
	if err := bet.AwaitReference(t0.Add(time.Second), t0.Add(time.Minute), t0); !errors.Is(err, ErrInvariant) {
		t.Fatalf("a BET has no reference to wait for: %v", err)
	}

	tx := newTx(t, input(w, "rb-1", KindRollback, "10.00", "bet-1"))
	if err := tx.RetryReferenceLater(t0.Add(time.Second), t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("retry before waiting: %v", err)
	}
	if err := tx.AwaitReference(t0.Add(time.Second), t0.Add(time.Minute), t0); err != nil {
		t.Fatal(err)
	}
	if tx.Status() != StatusPendingReference || tx.Attempts() != 1 {
		t.Fatalf("status %s attempts %d", tx.Status(), tx.Attempts())
	}
	if got := eventTypes(tx.PullEvents()); len(got) != 1 || got[0] != "WagerTransactionPendingReference" {
		t.Fatalf("events: %v", got)
	}
	if err := tx.AwaitReference(t0.Add(time.Second), t0.Add(time.Minute), t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("second await: %v", err)
	}
	if err := tx.RetryReferenceLater(t0.Add(2*time.Second), t0.Add(time.Second)); err != nil || tx.Attempts() != 2 {
		t.Fatalf("retry: %v attempts %d", err, tx.Attempts())
	}
	if len(tx.PullEvents()) != 0 {
		t.Fatal("retries do not publish events")
	}
	if err := tx.Reject(FailureReferenceNotFound, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if !tx.NextAttemptAt().IsZero() {
		t.Fatal("terminal tx must not keep a schedule")
	}
}

func TestRehydrateTransaction(t *testing.T) {
	w := openWallet(t, "100.00")
	tx := newTx(t, input(w, "bet-1", KindBet, "10.00", ""))
	if err := tx.MarkProcessed(brl(t, "90.00"), t0); err != nil {
		t.Fatal(err)
	}
	back, err := RehydrateWagerTransaction(tx.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if back.Status() != StatusProcessed || back.BalanceAfter().Amount() != "90.00" || len(back.PullEvents()) != 0 {
		t.Fatal("rehydration must restore state without events")
	}

	bad := []func(*WagerTransactionSnapshot){
		func(s *WagerTransactionSnapshot) { s.ID = uuid.Nil },
		func(s *WagerTransactionSnapshot) { s.Status = "DONE" },
		func(s *WagerTransactionSnapshot) { s.Kind = KindOpening },
		func(s *WagerTransactionSnapshot) { s.PayloadHash = "" },
		func(s *WagerTransactionSnapshot) { s.Status = StatusRejected; s.FailureCode = "" },
	}
	for i, mutate := range bad {
		s := tx.Snapshot()
		mutate(&s)
		if _, err := RehydrateWagerTransaction(s); !errors.Is(err, ErrInvariant) {
			t.Errorf("case %d: %v", i, err)
		}
	}
}
