package domain

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"

	"wallet/internal/domain/money"
)

func settle(t *testing.T, tx *WagerTransaction, w *Wallet, ref *Reference, now time.Time) *LedgerEntry {
	t.Helper()
	e, err := Settle(tx, w, ref, uuid.New(), policy, now)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func TestBet(t *testing.T) {
	w := openWallet(t, "100.00")
	tx := newTx(t, input(w, "bet-1", KindBet, "25.00", ""))
	e := settle(t, tx, w, nil, t0)
	if tx.Status() != StatusProcessed || tx.BalanceAfter().Amount() != "75.00" {
		t.Fatalf("bet: %s %s", tx.Status(), tx.BalanceAfter())
	}
	if e == nil || e.Direction() != Debit || e.Amount().Amount() != "25.00" || e.TransactionID() != tx.ID() {
		t.Fatalf("entry: %+v", e)
	}
	got := append(eventTypes(tx.PullEvents()), eventTypes(w.PullEvents())...)
	if !slices.Equal(got, []string{"WagerTransactionProcessed", "WalletBalanceChanged"}) {
		t.Fatalf("events: %v", got)
	}
}

func TestBetInsufficientFunds(t *testing.T) {
	w := openWallet(t, "100.00")
	settled(t, w, input(w, "bet-1", KindBet, "80.00", ""), nil)

	tx := newTx(t, input(w, "bet-2", KindBet, "80.00", ""))
	if e := settle(t, tx, w, nil, t0); e != nil {
		t.Fatal("rejected bet must not produce ledger")
	}
	if tx.Status() != StatusRejected || tx.FailureCode() != FailureInsufficientFunds {
		t.Fatalf("got %s %s", tx.Status(), tx.FailureCode())
	}
	if w.Balance().Amount() != "20.00" || w.Version() != 2 {
		t.Fatalf("wallet: %s v%d", w.Balance(), w.Version())
	}
	if got := eventTypes(tx.PullEvents()); !slices.Equal(got, []string{"WagerTransactionRejected"}) {
		t.Fatalf("events: %v", got)
	}
	if len(w.PullEvents()) != 0 {
		t.Fatal("no balance event on rejection")
	}
}

func TestWin(t *testing.T) {
	w := openWallet(t, "100.00")
	tx := newTx(t, input(w, "win-1", KindWin, "40.00", ""))
	e := settle(t, tx, w, nil, t0)
	if e.Direction() != Credit || tx.BalanceAfter().Amount() != "140.00" {
		t.Fatalf("win: %s %s", e.Direction(), tx.BalanceAfter())
	}
}

func TestWinWithBetReference(t *testing.T) {
	w := openWallet(t, "100.00")
	bet := settled(t, w, input(w, "bet-1", KindBet, "10.00", ""), nil)
	tx := newTx(t, input(w, "win-1", KindWin, "30.00", "bet-1"))
	settle(t, tx, w, &Reference{Transaction: bet}, t0)
	if tx.Status() != StatusProcessed || tx.ReferenceID() != bet.ID() {
		t.Fatalf("win: %s ref %s", tx.Status(), tx.ReferenceID())
	}
}

func TestLoss(t *testing.T) {
	w := openWallet(t, "100.00")
	tx := newTx(t, input(w, "loss-1", KindLoss, "0.00", ""))
	if e := settle(t, tx, w, nil, t0); e != nil {
		t.Fatal("LOSS must not create ledger")
	}
	if tx.Status() != StatusProcessed || tx.BalanceAfter().Amount() != "100.00" || w.Version() != 1 {
		t.Fatalf("loss: %s %s v%d", tx.Status(), tx.BalanceAfter(), w.Version())
	}
	if got := eventTypes(tx.PullEvents()); !slices.Equal(got, []string{"WagerTransactionProcessed"}) {
		t.Fatalf("events: %v", got)
	}
	if len(w.PullEvents()) != 0 {
		t.Fatal("LOSS must not emit WalletBalanceChanged")
	}
}

func TestRefund(t *testing.T) {
	w := openWallet(t, "100.00")
	bet := settled(t, w, input(w, "bet-1", KindBet, "30.00", ""), nil)
	tx := newTx(t, input(w, "refund-1", KindRefund, "30.00", "bet-1"))
	e := settle(t, tx, w, &Reference{Transaction: bet}, t0)
	if e.Direction() != Credit || w.Balance().Amount() != "100.00" || tx.ReferenceID() != bet.ID() {
		t.Fatalf("refund: %s %s", e.Direction(), w.Balance())
	}
}

func TestRollbackDirections(t *testing.T) {
	w := openWallet(t, "100.00")
	bet := settled(t, w, input(w, "bet-1", KindBet, "30.00", ""), nil)
	win := settled(t, w, input(w, "win-1", KindWin, "50.00", ""), nil)

	rbBet := newTx(t, input(w, "rb-bet", KindRollback, "30.00", "bet-1"))
	if e := settle(t, rbBet, w, &Reference{Transaction: bet}, t0); e.Direction() != Credit {
		t.Fatal("rollback of BET credits")
	}
	rbWin := newTx(t, input(w, "rb-win", KindRollback, "50.00", "win-1"))
	if e := settle(t, rbWin, w, &Reference{Transaction: win}, t0); e.Direction() != Debit {
		t.Fatal("rollback of WIN debits")
	}
	if w.Balance().Amount() != "100.00" {
		t.Fatalf("balance: %s", w.Balance())
	}
}

func TestRollbackOfRefund(t *testing.T) {
	w := openWallet(t, "100.00")
	bet := settled(t, w, input(w, "bet-1", KindBet, "30.00", ""), nil)
	refund := settled(t, w, input(w, "refund-1", KindRefund, "30.00", "bet-1"), &Reference{Transaction: bet})
	tx := newTx(t, input(w, "rb-refund", KindRollback, "30.00", "refund-1"))
	e := settle(t, tx, w, &Reference{Transaction: refund}, t0)
	if e.Direction() != Debit || w.Balance().Amount() != "70.00" {
		t.Fatalf("rollback refund: %s %s", e.Direction(), w.Balance())
	}
}

func TestReversalWithoutFunds(t *testing.T) {
	w := openWallet(t, "0.00")
	win := settled(t, w, input(w, "win-1", KindWin, "50.00", ""), nil)
	settled(t, w, input(w, "bet-1", KindBet, "40.00", ""), nil)

	tx := newTx(t, input(w, "rb-win", KindRollback, "50.00", "win-1"))
	if e := settle(t, tx, w, &Reference{Transaction: win}, t0); e != nil {
		t.Fatal("must not move money")
	}
	if tx.FailureCode() != FailureInsufficientFundsForReversal {
		t.Fatalf("got %s", tx.FailureCode())
	}
	if FailureInsufficientFundsForReversal == FailureInsufficientFunds {
		t.Fatal("codes must differ")
	}
}

func TestReferenceRules(t *testing.T) {
	w := openWallet(t, "100.00")
	bet := settled(t, w, input(w, "bet-1", KindBet, "30.00", ""), nil)
	win := settled(t, w, input(w, "win-1", KindWin, "10.00", ""), nil)
	loss := settled(t, w, input(w, "loss-1", KindLoss, "0.00", ""), nil)

	rejectedBet := newTx(t, input(w, "bet-x", KindBet, "1000.00", ""))
	settle(t, rejectedBet, w, nil, t0)

	otherRound := input(w, "bet-r2", KindBet, "5.00", "")
	otherRound.RoundID = "round-2"
	betRound2 := settled(t, w, otherRound, nil)

	cases := []struct {
		name string
		in   WagerInput
		ref  *Reference
		want FailureCode
	}{
		{"refund of WIN", input(w, "r1", KindRefund, "10.00", "win-1"), &Reference{Transaction: win}, FailureInvalidReferenceKind},
		{"rollback of LOSS", input(w, "r2", KindRollback, "0.01", "loss-1"), &Reference{Transaction: loss}, FailureInvalidReferenceKind},
		{"win referencing WIN", input(w, "r3", KindWin, "10.00", "win-1"), &Reference{Transaction: win}, FailureInvalidReferenceKind},
		{"partial refund", input(w, "r4", KindRefund, "10.00", "bet-1"), &Reference{Transaction: bet}, FailureReferenceAmountMismatch},
		{"other round", input(w, "r5", KindRefund, "5.00", "bet-r2"), &Reference{Transaction: betRound2}, FailureReferenceMismatch},
		{"rejected reference", input(w, "r6", KindRefund, "1000.00", "bet-x"), &Reference{Transaction: rejectedBet}, FailureReferenceNotProcessed},
		{"already reversed", input(w, "r7", KindRollback, "30.00", "bet-1"), &Reference{Transaction: bet, Reversed: true}, FailureAlreadyReversed},
	}
	for _, c := range cases {
		tx := newTx(t, c.in)
		before := w.Balance()
		settle(t, tx, w, c.ref, t0)
		if tx.Status() != StatusRejected || tx.FailureCode() != c.want {
			t.Errorf("%s: got %s %s, want %s", c.name, tx.Status(), tx.FailureCode(), c.want)
		}
		if !w.Balance().Equal(before) {
			t.Errorf("%s: balance changed", c.name)
		}
	}
}

func TestReferenceMismatchOnPlayerOrCurrency(t *testing.T) {
	w := openWallet(t, "100.00")
	bet := settled(t, w, input(w, "bet-1", KindBet, "30.00", ""), nil)

	other, err := OpenWallet(OpeningIDs{uuid.New(), uuid.New(), uuid.New()}, uuid.New(), brl(t, "100.00"), t0)
	if err != nil {
		t.Fatal(err)
	}
	tx := newTx(t, input(other.Wallet, "refund-1", KindRefund, "30.00", "bet-1"))
	settle(t, tx, other.Wallet, &Reference{Transaction: bet}, t0)
	if tx.FailureCode() != FailureReferenceMismatch {
		t.Fatalf("got %s", tx.FailureCode())
	}
}

func TestPendingReferenceLifecycle(t *testing.T) {
	w := openWallet(t, "100.00")
	tx := newTx(t, input(w, "refund-1", KindRefund, "30.00", "bet-1"))

	settle(t, tx, w, nil, t0)
	if tx.Status() != StatusPendingReference || !tx.NextAttemptAt().Equal(t0.Add(time.Second)) || !tx.ReferenceDeadline().Equal(t0.Add(time.Minute)) {
		t.Fatalf("pending: %s next %s deadline %s", tx.Status(), tx.NextAttemptAt(), tx.ReferenceDeadline())
	}

	now := t0.Add(time.Second)
	settle(t, tx, w, nil, now)
	if tx.Attempts() != 2 || !tx.NextAttemptAt().Equal(now.Add(2*time.Second)) {
		t.Fatalf("attempt 2: %d %s", tx.Attempts(), tx.NextAttemptAt())
	}
	now = now.Add(2 * time.Second)
	settle(t, tx, w, nil, now)
	if !tx.NextAttemptAt().Equal(now.Add(4 * time.Second)) {
		t.Fatalf("attempt 3: %s", tx.NextAttemptAt())
	}

	bet := settled(t, w, input(w, "bet-1", KindBet, "30.00", ""), nil)
	settle(t, tx, w, &Reference{Transaction: bet}, now.Add(4*time.Second))
	if tx.Status() != StatusProcessed || w.Balance().Amount() != "100.00" {
		t.Fatalf("resolved: %s %s", tx.Status(), w.Balance())
	}
}

func TestPendingReferenceExpires(t *testing.T) {
	w := openWallet(t, "100.00")
	tx := newTx(t, input(w, "rb-1", KindRollback, "30.00", "missing"))
	settle(t, tx, w, nil, t0)
	tx.PullEvents()

	settle(t, tx, w, nil, t0.Add(time.Minute))
	if tx.Status() != StatusRejected || tx.FailureCode() != FailureReferenceNotFound {
		t.Fatalf("got %s %s", tx.Status(), tx.FailureCode())
	}
	if got := eventTypes(tx.PullEvents()); !slices.Equal(got, []string{"WagerTransactionRejected"}) {
		t.Fatalf("events: %v", got)
	}
}

func TestPendingReferenceMaxAttempts(t *testing.T) {
	w := openWallet(t, "100.00")
	tx := newTx(t, input(w, "rb-1", KindRollback, "30.00", "missing"))
	now := t0
	for i := 0; tx.Status() != StatusRejected; i++ {
		if i > policy.MaxAttempts {
			t.Fatal("attempt limit not enforced")
		}
		settle(t, tx, w, nil, now)
		now = now.Add(time.Millisecond)
	}
	if tx.Attempts() != policy.MaxAttempts {
		t.Fatalf("attempts: %d", tx.Attempts())
	}
}

func TestReferenceStillPending(t *testing.T) {
	w := openWallet(t, "100.00")
	refund := newTx(t, input(w, "refund-1", KindRefund, "30.00", "bet-1"))
	settle(t, refund, w, nil, t0)

	rb := newTx(t, input(w, "rb-1", KindRollback, "30.00", "refund-1"))
	settle(t, rb, w, &Reference{Transaction: refund}, t0)
	if rb.Status() != StatusPendingReference {
		t.Fatalf("got %s", rb.Status())
	}
	settle(t, rb, w, &Reference{Transaction: refund}, t0.Add(time.Minute))
	if rb.FailureCode() != FailureReferenceStillPending {
		t.Fatalf("got %s", rb.FailureCode())
	}
}

func TestSettleGuards(t *testing.T) {
	w := openWallet(t, "100.00")
	other := openWallet(t, "100.00")
	tx := newTx(t, input(w, "bet-1", KindBet, "10.00", ""))
	if _, err := Settle(tx, other, nil, uuid.New(), policy, t0); !errors.Is(err, ErrInvariant) {
		t.Fatalf("other wallet: %v", err)
	}

	in := input(w, "bet-2", KindBet, "10.00", "")
	in.Currency = "USD"
	usdTx := newTx(t, in)
	if _, err := Settle(usdTx, w, nil, uuid.New(), policy, t0); !errors.Is(err, ErrWalletCurrencyMismatch) {
		t.Fatalf("currency: %v", err)
	}

	done := settled(t, w, input(w, "bet-3", KindBet, "10.00", ""), nil)
	if _, err := Settle(done, w, nil, uuid.New(), policy, t0); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("terminal: %v", err)
	}
}

func TestCheckWallet(t *testing.T) {
	w := openWallet(t, "100.00")
	req, _ := NewWagerRequest(input(w, "bet-1", KindBet, "10.00", ""))
	if err := CheckWallet(w, req); err != nil {
		t.Fatal(err)
	}
	req.PlayerID = uuid.New()
	if err := CheckWallet(w, req); !errors.Is(err, ErrWalletPlayerMismatch) || !errors.Is(err, ErrInvalid) {
		t.Fatalf("player: %v", err)
	}
	req.PlayerID = w.PlayerID()
	req.Money, _ = money.Parse("10.00", "EUR")
	if err := CheckWallet(w, req); !errors.Is(err, ErrWalletCurrencyMismatch) {
		t.Fatalf("currency: %v", err)
	}
}
