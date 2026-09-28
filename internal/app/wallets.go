package app

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"

	"github.com/google/uuid"

	"wallet/internal/domain"
	"wallet/internal/domain/money"
)

var ErrInvalidCursor = fmt.Errorf("%w: invalid cursor", domain.ErrInvalid)

type WalletDeps struct {
	Tx           TxRunner
	Wallets      WalletRepository
	Transactions TransactionRepository
	Ledger       LedgerRepository
	Outbox       OutboxRepository
	Metrics      Metrics
	Log          *slog.Logger
	Clock        Clock
	IDs          IDGenerator
}

type WalletService struct {
	WalletDeps
}

func NewWalletService(deps WalletDeps) *WalletService {
	if deps.Clock == nil {
		deps.Clock = SystemClock
	}
	if deps.IDs == nil {
		deps.IDs = NewV7
	}
	return &WalletService{deps}
}

func (s *WalletService) Open(ctx context.Context, playerID uuid.UUID, initial money.Money, correlationID string) (*domain.Wallet, error) {
	var w *domain.Wallet
	err := s.Tx.WithinTx(ctx, func(ctx context.Context) error {
		ids := domain.OpeningIDs{Wallet: s.IDs(), Transaction: s.IDs(), Entry: s.IDs()}
		o, err := domain.OpenWallet(ids, playerID, initial, s.Clock())
		if err != nil {
			return err
		}
		w = o.Wallet
		if err := s.Wallets.Insert(ctx, w); err != nil {
			return err
		}
		if o.Transaction == nil {
			return nil
		}
		if err := s.Transactions.Insert(ctx, o.Transaction); err != nil {
			return err
		}
		if err := s.Ledger.Append(ctx, *o.Entry); err != nil {
			return err
		}
		events := append(o.Transaction.PullEvents(), w.PullEvents()...)
		return s.Outbox.Append(ctx, envelopes(s.IDs, events, correlationID, ""))
	})
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (s *WalletService) Get(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	w, err := s.Wallets.Get(ctx, id)
	if errors.Is(err, ErrNotFound) {
		return nil, ErrWalletNotFound
	}
	return w, err
}

type LedgerPage struct {
	Entries    []domain.LedgerEntry
	NextCursor string
}

const cursorPrefix = "v1:"

func encodeCursor(version int64) string {
	return base64.RawURLEncoding.EncodeToString([]byte(cursorPrefix + strconv.FormatInt(version, 10)))
}

func decodeCursor(c string) (int64, error) {
	if c == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil || !strings.HasPrefix(string(raw), cursorPrefix) {
		return 0, ErrInvalidCursor
	}
	v, err := strconv.ParseInt(strings.TrimPrefix(string(raw), cursorPrefix), 10, 64)
	if err != nil || v < 0 {
		return 0, ErrInvalidCursor
	}
	return v, nil
}

func (s *WalletService) LedgerPage(ctx context.Context, walletID uuid.UUID, cursor string, limit int) (LedgerPage, error) {
	after, err := decodeCursor(cursor)
	if err != nil {
		return LedgerPage{}, err
	}
	var page LedgerPage
	err = s.Tx.WithinSnapshot(ctx, func(ctx context.Context) error {
		if _, err := s.Get(ctx, walletID); err != nil {
			return err
		}
		entries, err := s.Ledger.Page(ctx, walletID, after, limit+1)
		if err != nil {
			return err
		}
		if len(entries) > limit {
			entries = entries[:limit]
			page.NextCursor = encodeCursor(entries[limit-1].WalletVersion())
		}
		page.Entries = entries
		return nil
	})
	return page, err
}

type Reconciliation struct {
	WalletID          uuid.UUID
	StoredBalance     money.Money
	CalculatedBalance money.Money
	Difference        money.Money
	Consistent        bool
	CheckedEntries    int64
}

func (s *WalletService) Reconcile(ctx context.Context, walletID uuid.UUID) (Reconciliation, error) {
	var r Reconciliation
	err := s.Tx.WithinSnapshot(ctx, func(ctx context.Context) error {
		w, err := s.Get(ctx, walletID)
		if err != nil {
			return err
		}
		totals, err := s.Ledger.Totals(ctx, walletID)
		if err != nil {
			return err
		}
		calculated, err := money.FromMinor(totals.NetMinor, w.Currency().String())
		if err != nil {
			return err
		}
		diff, err := w.Balance().Sub(calculated)
		if err != nil {
			return err
		}
		r = Reconciliation{
			WalletID:          walletID,
			StoredBalance:     w.Balance(),
			CalculatedBalance: calculated,
			Difference:        diff,
			Consistent:        diff.IsZero(),
			CheckedEntries:    totals.Entries,
		}
		return nil
	})
	if err != nil {
		return Reconciliation{}, err
	}
	if !r.Consistent {
		s.Metrics.ReconciliationDivergence()
		s.Log.ErrorContext(ctx, "wallet balance diverges from ledger",
			"walletId", walletID.String(), "difference", r.Difference.Amount(), "checkedEntries", r.CheckedEntries)
	}
	return r, nil
}
