package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"wallet/internal/domain/money"
)

type Wallet struct {
	id        uuid.UUID
	playerID  uuid.UUID
	balance   money.Money
	version   int64
	createdAt time.Time
	updatedAt time.Time
	events    []Event
}

type WalletSnapshot struct {
	ID        uuid.UUID
	PlayerID  uuid.UUID
	Balance   money.Money
	Version   int64
	CreatedAt time.Time
	UpdatedAt time.Time
}

type Opening struct {
	Wallet      *Wallet
	Transaction *WagerTransaction
	Entry       *LedgerEntry
}

type OpeningIDs struct {
	Wallet      uuid.UUID
	Transaction uuid.UUID
	Entry       uuid.UUID
}

func OpenWallet(ids OpeningIDs, playerID uuid.UUID, initial money.Money, now time.Time) (Opening, error) {
	verr := &ValidationError{}
	if playerID == uuid.Nil {
		verr.add("playerId", "required")
	}
	if !initial.Valid() {
		verr.add("initialBalance", "required")
	} else if initial.IsNegative() {
		verr.add("initialBalance", "must not be negative")
	}
	if err := verr.orNil(); err != nil {
		return Opening{}, err
	}
	if ids.Wallet == uuid.Nil || now.IsZero() {
		return Opening{}, fmt.Errorf("%w: wallet id and time are required", ErrInvariant)
	}

	zero, _ := money.Zero(initial.Currency().String())
	w := &Wallet{id: ids.Wallet, playerID: playerID, balance: zero, version: 1, createdAt: now, updatedAt: now}
	if initial.IsZero() {
		return Opening{Wallet: w}, nil
	}

	tx, err := newOpeningTransaction(ids.Transaction, w, initial, now)
	if err != nil {
		return Opening{}, err
	}
	entry, err := w.move(ids.Entry, tx.id, Credit, initial, now, false)
	if err != nil {
		return Opening{}, err
	}
	if err := tx.MarkProcessed(w.balance, now); err != nil {
		return Opening{}, err
	}
	return Opening{Wallet: w, Transaction: tx, Entry: &entry}, nil
}

func RehydrateWallet(s WalletSnapshot) (*Wallet, error) {
	switch {
	case s.ID == uuid.Nil || s.PlayerID == uuid.Nil:
		return nil, fmt.Errorf("%w: wallet ids are required", ErrInvariant)
	case !s.Balance.Valid() || s.Balance.IsNegative():
		return nil, fmt.Errorf("%w: wallet balance %v is invalid", ErrInvariant, s.Balance)
	case s.Version < 1:
		return nil, fmt.Errorf("%w: wallet version must be at least 1", ErrInvariant)
	case s.CreatedAt.IsZero() || s.UpdatedAt.IsZero():
		return nil, fmt.Errorf("%w: wallet timestamps are required", ErrInvariant)
	}
	return &Wallet{
		id:        s.ID,
		playerID:  s.PlayerID,
		balance:   s.Balance,
		version:   s.Version,
		createdAt: s.CreatedAt,
		updatedAt: s.UpdatedAt,
	}, nil
}

func (w *Wallet) ID() uuid.UUID              { return w.id }
func (w *Wallet) PlayerID() uuid.UUID        { return w.playerID }
func (w *Wallet) Currency() money.Currency   { return w.balance.Currency() }
func (w *Wallet) Balance() money.Money       { return w.balance }
func (w *Wallet) Version() int64             { return w.version }
func (w *Wallet) CreatedAt() time.Time       { return w.createdAt }
func (w *Wallet) UpdatedAt() time.Time       { return w.updatedAt }
func (w *Wallet) Accepts(m money.Money) bool { return m.Valid() && m.Currency() == w.Currency() }

func (w *Wallet) Snapshot() WalletSnapshot {
	return WalletSnapshot{
		ID:        w.id,
		PlayerID:  w.playerID,
		Balance:   w.balance,
		Version:   w.version,
		CreatedAt: w.createdAt,
		UpdatedAt: w.updatedAt,
	}
}

func (w *Wallet) PullEvents() []Event {
	e := w.events
	w.events = nil
	return e
}

func (w *Wallet) Debit(entryID, transactionID uuid.UUID, amount money.Money, now time.Time) (LedgerEntry, error) {
	return w.move(entryID, transactionID, Debit, amount, now, true)
}

func (w *Wallet) Credit(entryID, transactionID uuid.UUID, amount money.Money, now time.Time) (LedgerEntry, error) {
	return w.move(entryID, transactionID, Credit, amount, now, true)
}

func (w *Wallet) move(entryID, transactionID uuid.UUID, dir Direction, amount money.Money, now time.Time, bump bool) (LedgerEntry, error) {
	if !w.Accepts(amount) {
		return LedgerEntry{}, ErrWalletCurrencyMismatch
	}
	if !amount.IsPositive() {
		return LedgerEntry{}, fmt.Errorf("%w: movement amount must be positive", ErrInvariant)
	}
	var after money.Money
	var err error
	if dir == Debit {
		after, err = w.balance.Sub(amount)
	} else {
		after, err = w.balance.Add(amount)
	}
	if err != nil {
		return LedgerEntry{}, fmt.Errorf("%w: %v", ErrInvariant, err)
	}
	if after.IsNegative() {
		return LedgerEntry{}, ErrInsufficientFunds
	}
	version := w.version
	if bump {
		version++
	}
	entry, err := NewLedgerEntry(LedgerEntrySnapshot{
		ID:            entryID,
		WalletID:      w.id,
		TransactionID: transactionID,
		WalletVersion: version,
		Direction:     dir,
		Amount:        amount,
		BalanceBefore: w.balance,
		BalanceAfter:  after,
		CreatedAt:     now,
	})
	if err != nil {
		return LedgerEntry{}, err
	}
	w.events = append(w.events, WalletBalanceChanged{
		WalletID:      w.id,
		TransactionID: transactionID,
		Direction:     dir,
		Money:         amount,
		BalanceBefore: w.balance,
		BalanceAfter:  after,
		WalletVersion: version,
		ChangedAt:     Timestamp(now),
	})
	w.balance = after
	w.version = version
	w.updatedAt = now
	return entry, nil
}
