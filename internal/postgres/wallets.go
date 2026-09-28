package postgres

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"wallet/internal/app"
	"wallet/internal/domain"
	"wallet/internal/domain/money"
)

type WalletRepo struct{ tm *TxManager }

func NewWalletRepo(tm *TxManager) *WalletRepo { return &WalletRepo{tm} }

const walletColumns = `id, player_id, currency, balance_minor, version, created_at, updated_at`

func (r *WalletRepo) Insert(ctx context.Context, w *domain.Wallet) error {
	s := w.Snapshot()
	_, err := r.tm.q(ctx).Exec(ctx, `
		INSERT INTO wallets (`+walletColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		s.ID, s.PlayerID, s.Balance.Currency().String(), s.Balance.Minor(), s.Version, s.CreatedAt, s.UpdatedAt)
	if c, ok := uniqueViolation(err); ok && c == "wallets_player_currency_key" {
		return app.ErrWalletExists
	}
	return classify(err)
}

func (r *WalletRepo) Get(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return scanWallet(r.tm.q(ctx).QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1`, id))
}

func (r *WalletRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return scanWallet(r.tm.q(ctx).QueryRow(ctx, `SELECT `+walletColumns+` FROM wallets WHERE id = $1 FOR UPDATE`, id))
}

func (r *WalletRepo) Update(ctx context.Context, w *domain.Wallet, previousVersion int64) error {
	s := w.Snapshot()
	tag, err := r.tm.q(ctx).Exec(ctx, `
		UPDATE wallets SET balance_minor = $2, version = $3, updated_at = $4
		WHERE id = $1 AND version = $5`,
		s.ID, s.Balance.Minor(), s.Version, s.UpdatedAt, previousVersion)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrConcurrentWrite
	}
	return nil
}

func scanWallet(row pgx.Row) (*domain.Wallet, error) {
	var (
		s        domain.WalletSnapshot
		currency string
		balance  int64
	)
	if err := row.Scan(&s.ID, &s.PlayerID, &currency, &balance, &s.Version, &s.CreatedAt, &s.UpdatedAt); err != nil {
		return nil, classify(err)
	}
	var err error
	if s.Balance, err = money.FromMinor(balance, currency); err != nil {
		return nil, err
	}
	s.CreatedAt, s.UpdatedAt = s.CreatedAt.UTC(), s.UpdatedAt.UTC()
	return domain.RehydrateWallet(s)
}

func utc(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return t.UTC()
}

func nullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func nullString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func str(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}
