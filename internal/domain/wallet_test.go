package domain

import (
	"errors"
	"slices"
	"testing"

	"github.com/google/uuid"

	"wallet/internal/domain/money"
)

func TestOpenWalletWithBalance(t *testing.T) {
	ids := OpeningIDs{uuid.New(), uuid.New(), uuid.New()}
	o, err := OpenWallet(ids, playerID, brl(t, "1000.00"), t0)
	if err != nil {
		t.Fatal(err)
	}
	w := o.Wallet
	if w.Version() != 1 || w.Balance().Amount() != "1000.00" {
		t.Fatalf("wallet: version %d balance %s", w.Version(), w.Balance())
	}
	tx := o.Transaction
	if tx.Kind() != KindOpening || tx.Origin() != OriginInternal || tx.Status() != StatusProcessed {
		t.Fatalf("opening tx: %s %s %s", tx.Kind(), tx.Origin(), tx.Status())
	}
	if tx.ProviderID() != "" || tx.ExternalTransactionID() != "" || tx.IdempotencyKey() != "" || tx.PayloadHash() != "" || tx.RoundID() != "" {
		t.Fatal("opening must not carry external metadata")
	}
	e := o.Entry
	if e.Direction() != Credit || e.WalletVersion() != 1 || !e.BalanceBefore().IsZero() || e.BalanceAfter().Amount() != "1000.00" {
		t.Fatalf("opening entry: %+v", e.Snapshot())
	}
	if e.TransactionID() != tx.ID() {
		t.Fatal("entry must point to the opening transaction")
	}
	got := append(eventTypes(tx.PullEvents()), eventTypes(w.PullEvents())...)
	if !slices.Equal(got, []string{"WagerTransactionProcessed", "WalletBalanceChanged"}) {
		t.Fatalf("events: %v", got)
	}
}

func TestOpenWalletZeroBalance(t *testing.T) {
	o, err := OpenWallet(OpeningIDs{uuid.New(), uuid.New(), uuid.New()}, playerID, brl(t, "0.00"), t0)
	if err != nil {
		t.Fatal(err)
	}
	if o.Transaction != nil || o.Entry != nil || len(o.Wallet.PullEvents()) != 0 {
		t.Fatal("zero opening must not create transaction, ledger or events")
	}
	if o.Wallet.Version() != 1 {
		t.Fatalf("version: %d", o.Wallet.Version())
	}
}

func TestOpenWalletRejects(t *testing.T) {
	ids := OpeningIDs{uuid.New(), uuid.New(), uuid.New()}
	neg, _ := money.FromMinor(-1, "BRL")
	cases := map[string]struct {
		player uuid.UUID
		amount money.Money
	}{
		"nil player":    {uuid.Nil, brl(t, "1.00")},
		"negative":      {playerID, neg},
		"uninitialized": {playerID, money.Money{}},
	}
	for name, c := range cases {
		if _, err := OpenWallet(ids, c.player, c.amount, t0); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got %v", name, err)
		}
	}
}

func TestDebitAndCredit(t *testing.T) {
	w := openWallet(t, "100.00")
	txID := uuid.New()

	e, err := w.Debit(uuid.New(), txID, brl(t, "30.00"), t0)
	if err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "70.00" || w.Version() != 2 {
		t.Fatalf("after debit: %s v%d", w.Balance(), w.Version())
	}
	if e.BalanceBefore().Amount() != "100.00" || e.BalanceAfter().Amount() != "70.00" || e.WalletVersion() != 2 {
		t.Fatalf("entry: %+v", e.Snapshot())
	}

	if _, err := w.Credit(uuid.New(), uuid.New(), brl(t, "5.50"), t0); err != nil {
		t.Fatal(err)
	}
	if w.Balance().Amount() != "75.50" || w.Version() != 3 {
		t.Fatalf("after credit: %s v%d", w.Balance(), w.Version())
	}
	evs := w.PullEvents()
	if len(evs) != 2 {
		t.Fatalf("events: %d", len(evs))
	}
	bc := evs[0].(WalletBalanceChanged)
	if bc.WalletVersion != 2 || bc.Direction != Debit || bc.BalanceBefore.Amount() != "100.00" || bc.BalanceAfter.Amount() != "70.00" {
		t.Fatalf("event: %+v", bc)
	}
}

func TestDebitKeepsBalanceNonNegative(t *testing.T) {
	w := openWallet(t, "10.00")
	if _, err := w.Debit(uuid.New(), uuid.New(), brl(t, "10.01"), t0); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("got %v", err)
	}
	if w.Balance().Amount() != "10.00" || w.Version() != 1 || len(w.PullEvents()) != 0 {
		t.Fatal("failed debit must not change the wallet")
	}
	if _, err := w.Debit(uuid.New(), uuid.New(), brl(t, "10.00"), t0); err != nil {
		t.Fatalf("debit to zero: %v", err)
	}
}

func TestWalletRejectsOtherCurrencyAndZero(t *testing.T) {
	w := openWallet(t, "10.00")
	usd, _ := money.Parse("1.00", "USD")
	if _, err := w.Credit(uuid.New(), uuid.New(), usd, t0); !errors.Is(err, ErrWalletCurrencyMismatch) {
		t.Fatalf("currency: %v", err)
	}
	if _, err := w.Credit(uuid.New(), uuid.New(), brl(t, "0.00"), t0); !errors.Is(err, ErrInvariant) {
		t.Fatalf("zero: %v", err)
	}
	if w.Version() != 1 {
		t.Fatal("version changed")
	}
}

func TestRehydrateWallet(t *testing.T) {
	good := WalletSnapshot{ID: uuid.New(), PlayerID: playerID, Balance: brl(t, "5.00"), Version: 7, CreatedAt: t0, UpdatedAt: t0}
	w, err := RehydrateWallet(good)
	if err != nil {
		t.Fatal(err)
	}
	if w.Version() != 7 || len(w.PullEvents()) != 0 {
		t.Fatal("rehydration must keep state and record nothing")
	}
	neg, _ := money.FromMinor(-1, "BRL")
	bad := []WalletSnapshot{
		{ID: uuid.Nil, PlayerID: playerID, Balance: brl(t, "5.00"), Version: 1, CreatedAt: t0, UpdatedAt: t0},
		{ID: uuid.New(), PlayerID: playerID, Balance: neg, Version: 1, CreatedAt: t0, UpdatedAt: t0},
		{ID: uuid.New(), PlayerID: playerID, Balance: brl(t, "5.00"), Version: 0, CreatedAt: t0, UpdatedAt: t0},
		{ID: uuid.New(), PlayerID: playerID, Version: 1, CreatedAt: t0, UpdatedAt: t0},
	}
	for i, s := range bad {
		if _, err := RehydrateWallet(s); !errors.Is(err, ErrInvariant) {
			t.Errorf("case %d: %v", i, err)
		}
	}
}

func TestLedgerEntryValidation(t *testing.T) {
	base := LedgerEntrySnapshot{
		ID: uuid.New(), WalletID: uuid.New(), TransactionID: uuid.New(), WalletVersion: 2,
		Direction: Debit, Amount: brl(t, "10.00"), BalanceBefore: brl(t, "30.00"), BalanceAfter: brl(t, "20.00"), CreatedAt: t0,
	}
	if _, err := NewLedgerEntry(base); err != nil {
		t.Fatal(err)
	}
	wrong := base
	wrong.BalanceAfter = brl(t, "21.00")
	if _, err := NewLedgerEntry(wrong); !errors.Is(err, ErrInvariant) {
		t.Fatalf("wrong after: %v", err)
	}
	wrong = base
	wrong.Direction = Credit
	if _, err := NewLedgerEntry(wrong); !errors.Is(err, ErrInvariant) {
		t.Fatalf("wrong direction: %v", err)
	}
	wrong = base
	wrong.BalanceBefore = brl(t, "5.00")
	wrong.BalanceAfter, _ = money.FromMinor(-500, "BRL")
	if _, err := NewLedgerEntry(wrong); !errors.Is(err, ErrInvariant) {
		t.Fatalf("negative after: %v", err)
	}
	wrong = base
	wrong.Amount = brl(t, "0.00")
	wrong.BalanceAfter = base.BalanceBefore
	if _, err := NewLedgerEntry(wrong); !errors.Is(err, ErrInvariant) {
		t.Fatalf("zero amount: %v", err)
	}
}
