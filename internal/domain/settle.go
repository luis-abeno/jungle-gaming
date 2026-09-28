package domain

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type ReferencePolicy struct {
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	TTL            time.Duration
	MaxAttempts    int
}

func (p ReferencePolicy) backoff(attempt int) time.Duration {
	d := p.InitialBackoff
	for i := 1; i < attempt && d < p.MaxBackoff; i++ {
		d *= 2
	}
	return min(d, p.MaxBackoff)
}

type Reference struct {
	Transaction *WagerTransaction
	Reversed    bool
}

func CheckWallet(w *Wallet, req WagerRequest) error {
	if w.playerID != req.PlayerID {
		return ErrWalletPlayerMismatch
	}
	if !w.Accepts(req.Money) {
		return ErrWalletCurrencyMismatch
	}
	return nil
}

func Settle(t *WagerTransaction, w *Wallet, ref *Reference, entryID uuid.UUID, p ReferencePolicy, now time.Time) (*LedgerEntry, error) {
	if t.status != StatusPending && t.status != StatusPendingReference {
		return nil, fmt.Errorf("%w: cannot settle a %s transaction", ErrInvalidTransition, t.status)
	}
	if t.walletID != w.id {
		return nil, fmt.Errorf("%w: transaction targets another wallet", ErrInvariant)
	}
	if t.playerID != w.playerID {
		return nil, ErrWalletPlayerMismatch
	}
	if !w.Accepts(t.money) {
		return nil, ErrWalletCurrencyMismatch
	}

	var target *WagerTransaction
	if t.NeedsReference() {
		if ref == nil || ref.Transaction == nil {
			return nil, t.waitForReference(FailureReferenceNotFound, p, now)
		}
		target = ref.Transaction
		switch target.status {
		case StatusPending, StatusPendingReference:
			return nil, t.waitForReference(FailureReferenceStillPending, p, now)
		case StatusProcessed:
		default:
			return nil, t.Reject(FailureReferenceNotProcessed, now)
		}
		if code := t.checkReference(target, ref.Reversed); code != "" {
			return nil, t.Reject(code, now)
		}
		t.referenceID = target.id
	}

	var entry LedgerEntry
	var err error
	switch t.kind {
	case KindLoss:
		return nil, t.MarkProcessed(w.balance, now)
	case KindBet:
		entry, err = w.Debit(entryID, t.id, t.money, now)
		if errors.Is(err, ErrInsufficientFunds) {
			return nil, t.Reject(FailureInsufficientFunds, now)
		}
	case KindWin, KindRefund:
		entry, err = w.Credit(entryID, t.id, t.money, now)
	case KindRollback:
		if target.kind == KindBet {
			entry, err = w.Credit(entryID, t.id, t.money, now)
			break
		}
		entry, err = w.Debit(entryID, t.id, t.money, now)
		if errors.Is(err, ErrInsufficientFunds) {
			return nil, t.Reject(FailureInsufficientFundsForReversal, now)
		}
	default:
		return nil, fmt.Errorf("%w: cannot settle kind %s", ErrInvariant, t.kind)
	}
	if err != nil {
		return nil, err
	}
	if err := t.MarkProcessed(w.balance, now); err != nil {
		return nil, err
	}
	return &entry, nil
}

func (t *WagerTransaction) waitForReference(expired FailureCode, p ReferencePolicy, now time.Time) error {
	if t.status == StatusPending {
		return t.AwaitReference(now.Add(p.backoff(1)), now.Add(p.TTL), now)
	}
	if !now.Before(t.referenceDeadline) || t.attempts >= p.MaxAttempts {
		return t.Reject(expired, now)
	}
	return t.RetryReferenceLater(now.Add(p.backoff(t.attempts+1)), now)
}

func (t *WagerTransaction) checkReference(target *WagerTransaction, reversed bool) FailureCode {
	switch t.kind {
	case KindWin, KindRefund:
		if target.kind != KindBet {
			return FailureInvalidReferenceKind
		}
	case KindRollback:
		if target.kind != KindBet && target.kind != KindWin && target.kind != KindRefund {
			return FailureInvalidReferenceKind
		}
	}
	if target.providerID != t.providerID || target.playerID != t.playerID || target.walletID != t.walletID ||
		target.money.Currency() != t.money.Currency() || target.roundID != t.roundID {
		return FailureReferenceMismatch
	}
	if t.kind.Reversal() {
		if !target.money.Equal(t.money) {
			return FailureReferenceAmountMismatch
		}
		if reversed {
			return FailureAlreadyReversed
		}
	}
	return ""
}
