package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"

	"wallet/internal/app"
	"wallet/internal/domain"
	"wallet/internal/domain/money"
)

type LedgerRepo struct{ tm *TxManager }

func NewLedgerRepo(tm *TxManager) *LedgerRepo { return &LedgerRepo{tm} }

func (r *LedgerRepo) Append(ctx context.Context, e domain.LedgerEntry) error {
	s := e.Snapshot()
	_, err := r.tm.q(ctx).Exec(ctx, `
		INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, wallet_version, direction, currency,
			amount_minor, balance_before_minor, balance_after_minor, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		s.ID, s.WalletID, s.TransactionID, s.WalletVersion, s.Direction, s.Amount.Currency().String(),
		s.Amount.Minor(), s.BalanceBefore.Minor(), s.BalanceAfter.Minor(), s.CreatedAt)
	if _, ok := uniqueViolation(err); ok {
		return app.ErrConcurrentWrite
	}
	return classify(err)
}

func (r *LedgerRepo) Page(ctx context.Context, walletID uuid.UUID, afterVersion int64, limit int) ([]domain.LedgerEntry, error) {
	rows, err := r.tm.q(ctx).Query(ctx, `
		SELECT id, wallet_id, transaction_id, wallet_version, direction, currency,
			amount_minor, balance_before_minor, balance_after_minor, created_at
		FROM wallet_ledger_entries
		WHERE wallet_id = $1 AND wallet_version > $2
		ORDER BY wallet_version
		LIMIT $3`, walletID, afterVersion, limit)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []domain.LedgerEntry
	for rows.Next() {
		var (
			s                     domain.LedgerEntrySnapshot
			currency              string
			amount, before, after int64
			createdAt             time.Time
		)
		if err := rows.Scan(&s.ID, &s.WalletID, &s.TransactionID, &s.WalletVersion, &s.Direction, &currency,
			&amount, &before, &after, &createdAt); err != nil {
			return nil, classify(err)
		}
		if s.Amount, err = money.FromMinor(amount, currency); err != nil {
			return nil, err
		}
		s.BalanceBefore, _ = money.FromMinor(before, currency)
		s.BalanceAfter, _ = money.FromMinor(after, currency)
		s.CreatedAt = createdAt.UTC()
		e, err := domain.NewLedgerEntry(s)
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, classify(rows.Err())
}

func (r *LedgerRepo) Totals(ctx context.Context, walletID uuid.UUID) (app.LedgerTotals, error) {
	var t app.LedgerTotals
	err := r.tm.q(ctx).QueryRow(ctx, `
		SELECT COALESCE(SUM(CASE direction WHEN 'CREDIT' THEN amount_minor ELSE -amount_minor END), 0)::bigint,
		       count(*)
		FROM wallet_ledger_entries WHERE wallet_id = $1`, walletID).Scan(&t.NetMinor, &t.Entries)
	return t, classify(err)
}
