package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"wallet/internal/domain/money"
)

type LedgerEntry struct {
	id            uuid.UUID
	walletID      uuid.UUID
	transactionID uuid.UUID
	walletVersion int64
	direction     Direction
	amount        money.Money
	balanceBefore money.Money
	balanceAfter  money.Money
	createdAt     time.Time
}

type LedgerEntrySnapshot struct {
	ID            uuid.UUID
	WalletID      uuid.UUID
	TransactionID uuid.UUID
	WalletVersion int64
	Direction     Direction
	Amount        money.Money
	BalanceBefore money.Money
	BalanceAfter  money.Money
	CreatedAt     time.Time
}

func NewLedgerEntry(s LedgerEntrySnapshot) (LedgerEntry, error) {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.TransactionID == uuid.Nil {
		return LedgerEntry{}, fmt.Errorf("%w: ledger entry ids are required", ErrInvariant)
	}
	if s.WalletVersion < 1 || s.CreatedAt.IsZero() {
		return LedgerEntry{}, fmt.Errorf("%w: ledger entry version and time are required", ErrInvariant)
	}
	if !s.Amount.IsPositive() {
		return LedgerEntry{}, fmt.Errorf("%w: ledger amount must be positive", ErrInvariant)
	}
	var expected money.Money
	var err error
	switch s.Direction {
	case Credit:
		expected, err = s.BalanceBefore.Add(s.Amount)
	case Debit:
		expected, err = s.BalanceBefore.Sub(s.Amount)
	default:
		return LedgerEntry{}, fmt.Errorf("%w: invalid direction %q", ErrInvariant, s.Direction)
	}
	if err != nil {
		return LedgerEntry{}, fmt.Errorf("%w: %v", ErrInvariant, err)
	}
	if !expected.Equal(s.BalanceAfter) {
		return LedgerEntry{}, fmt.Errorf("%w: balance after %s does not match %s", ErrInvariant, s.BalanceAfter, expected)
	}
	if s.BalanceBefore.IsNegative() || s.BalanceAfter.IsNegative() {
		return LedgerEntry{}, fmt.Errorf("%w: negative balance in ledger", ErrInvariant)
	}
	return LedgerEntry{
		id:            s.ID,
		walletID:      s.WalletID,
		transactionID: s.TransactionID,
		walletVersion: s.WalletVersion,
		direction:     s.Direction,
		amount:        s.Amount,
		balanceBefore: s.BalanceBefore,
		balanceAfter:  s.BalanceAfter,
		createdAt:     s.CreatedAt,
	}, nil
}

func (e LedgerEntry) Snapshot() LedgerEntrySnapshot {
	return LedgerEntrySnapshot{
		ID:            e.id,
		WalletID:      e.walletID,
		TransactionID: e.transactionID,
		WalletVersion: e.walletVersion,
		Direction:     e.direction,
		Amount:        e.amount,
		BalanceBefore: e.balanceBefore,
		BalanceAfter:  e.balanceAfter,
		CreatedAt:     e.createdAt,
	}
}

func (e LedgerEntry) ID() uuid.UUID              { return e.id }
func (e LedgerEntry) WalletID() uuid.UUID        { return e.walletID }
func (e LedgerEntry) TransactionID() uuid.UUID   { return e.transactionID }
func (e LedgerEntry) WalletVersion() int64       { return e.walletVersion }
func (e LedgerEntry) Direction() Direction       { return e.direction }
func (e LedgerEntry) Amount() money.Money        { return e.amount }
func (e LedgerEntry) BalanceBefore() money.Money { return e.balanceBefore }
func (e LedgerEntry) BalanceAfter() money.Money  { return e.balanceAfter }
func (e LedgerEntry) CreatedAt() time.Time       { return e.createdAt }
