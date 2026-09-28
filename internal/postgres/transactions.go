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

type TransactionRepo struct{ tm *TxManager }

func NewTransactionRepo(tm *TxManager) *TransactionRepo { return &TransactionRepo{tm} }

const txColumns = `id, origin, kind, status, wallet_id, player_id, currency, amount_minor,
	provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
	reference_external_transaction_id, reference_transaction_id, failure_code, balance_after_minor,
	correlation_id, attempts, next_attempt_at, reference_deadline, created_at, updated_at, completed_at`

func (r *TransactionRepo) Insert(ctx context.Context, t *domain.WagerTransaction) error {
	s := t.Snapshot()
	_, err := r.tm.q(ctx).Exec(ctx, `
		INSERT INTO wager_transactions (`+txColumns+`)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18,
		        $19, $20, $21, $22, $23, $24, $25)`,
		s.ID, s.Origin, s.Kind, s.Status, s.WalletID, s.PlayerID, s.Money.Currency().String(), s.Money.Minor(),
		nullString(s.ProviderID), nullString(s.ExternalTransactionID), nullString(s.IdempotencyKey),
		nullString(s.PayloadHash), nullString(s.RoundID), nullString(s.GameID),
		nullString(s.ReferenceExternalTransactionID), nullUUID(s.ReferenceTransactionID),
		nullString(string(s.FailureCode)), balanceMinor(s.BalanceAfter), nullString(s.CorrelationID),
		s.Attempts, nullTime(s.NextAttemptAt), nullTime(s.ReferenceDeadline),
		s.CreatedAt, s.UpdatedAt, nullTime(s.CompletedAt))
	return txWriteError(err)
}

func (r *TransactionRepo) Update(ctx context.Context, t *domain.WagerTransaction) error {
	s := t.Snapshot()
	tag, err := r.tm.q(ctx).Exec(ctx, `
		UPDATE wager_transactions SET
			status = $2, reference_transaction_id = $3, failure_code = $4, balance_after_minor = $5,
			attempts = $6, next_attempt_at = $7, reference_deadline = $8, updated_at = $9, completed_at = $10
		WHERE id = $1`,
		s.ID, s.Status, nullUUID(s.ReferenceTransactionID), nullString(string(s.FailureCode)),
		balanceMinor(s.BalanceAfter), s.Attempts, nullTime(s.NextAttemptAt), nullTime(s.ReferenceDeadline),
		s.UpdatedAt, nullTime(s.CompletedAt))
	if err != nil {
		return txWriteError(err)
	}
	if tag.RowsAffected() != 1 {
		return app.ErrNotFound
	}
	return nil
}

func txWriteError(err error) error {
	if c, ok := uniqueViolation(err); ok {
		switch c {
		case "wager_transactions_idempotency_key", "wager_transactions_external_id",
			"wager_transactions_single_reversal", "wager_transactions_single_opening":
			return app.ErrConcurrentWrite
		}
	}
	return classify(err)
}

func (r *TransactionRepo) Get(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	return scanTransaction(r.tm.q(ctx).QueryRow(ctx, `SELECT `+txColumns+` FROM wager_transactions WHERE id = $1`, id))
}

func (r *TransactionRepo) GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	return scanTransaction(r.tm.q(ctx).QueryRow(ctx, `SELECT `+txColumns+` FROM wager_transactions WHERE id = $1 FOR UPDATE`, id))
}

func (r *TransactionRepo) ByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	return scanTransaction(r.tm.q(ctx).QueryRow(ctx, `
		SELECT `+txColumns+` FROM wager_transactions
		WHERE provider_id = $1 AND external_transaction_id = $2`, providerID, externalID))
}

func (r *TransactionRepo) Existing(ctx context.Context, providerID, key, externalID string) ([]*domain.WagerTransaction, error) {
	rows, err := r.tm.q(ctx).Query(ctx, `
		SELECT `+txColumns+` FROM wager_transactions
		WHERE provider_id = $1 AND (idempotency_key = $2 OR external_transaction_id = $3)`,
		providerID, key, externalID)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []*domain.WagerTransaction
	for rows.Next() {
		t, err := scanTransaction(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, classify(rows.Err())
}

func (r *TransactionRepo) HasProcessedReversal(ctx context.Context, id uuid.UUID) (bool, error) {
	var exists bool
	err := r.tm.q(ctx).QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM wager_transactions
			WHERE reference_transaction_id = $1 AND status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK'))`,
		id).Scan(&exists)
	return exists, classify(err)
}

func (r *TransactionRepo) ClaimDue(ctx context.Context, limit int, lease time.Duration) ([]app.DueTransaction, error) {
	rows, err := r.tm.q(ctx).Query(ctx, `
		UPDATE wager_transactions t
		SET next_attempt_at = now() + $2 * interval '1 millisecond'
		FROM (
			SELECT id FROM wager_transactions
			WHERE status = 'PENDING_REFERENCE' AND next_attempt_at <= now()
			ORDER BY next_attempt_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		) due
		WHERE t.id = due.id
		RETURNING t.id, t.wallet_id`, limit, lease.Milliseconds())
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []app.DueTransaction
	for rows.Next() {
		var d app.DueTransaction
		if err := rows.Scan(&d.ID, &d.WalletID); err != nil {
			return nil, classify(err)
		}
		out = append(out, d)
	}
	return out, classify(rows.Err())
}

func (r *TransactionRepo) WakeWaiting(ctx context.Context, providerID, externalID string) error {
	_, err := r.tm.q(ctx).Exec(ctx, `
		UPDATE wager_transactions SET next_attempt_at = now()
		WHERE id IN (
			SELECT id FROM wager_transactions
			WHERE provider_id = $1 AND reference_external_transaction_id = $2 AND status = 'PENDING_REFERENCE'
			FOR UPDATE SKIP LOCKED)`, providerID, externalID)
	return classify(err)
}

func scanTransaction(row pgx.Row) (*domain.WagerTransaction, error) {
	var (
		s                                       domain.WagerTransactionSnapshot
		currency                                string
		amount                                  int64
		provider, external, key, hash, round    *string
		game, refExternal, failure, correlation *string
		refID                                   uuid.NullUUID
		balanceAfter                            *int64
		nextAttempt, deadline, completed        *time.Time
	)
	err := row.Scan(&s.ID, &s.Origin, &s.Kind, &s.Status, &s.WalletID, &s.PlayerID, &currency, &amount,
		&provider, &external, &key, &hash, &round, &game, &refExternal, &refID, &failure, &balanceAfter,
		&correlation, &s.Attempts, &nextAttempt, &deadline, &s.CreatedAt, &s.UpdatedAt, &completed)
	if err != nil {
		return nil, classify(err)
	}
	if s.Money, err = money.FromMinor(amount, currency); err != nil {
		return nil, err
	}
	if balanceAfter != nil {
		if s.BalanceAfter, err = money.FromMinor(*balanceAfter, currency); err != nil {
			return nil, err
		}
	}
	s.ProviderID, s.ExternalTransactionID, s.IdempotencyKey = str(provider), str(external), str(key)
	s.PayloadHash, s.RoundID, s.GameID = str(hash), str(round), str(game)
	s.ReferenceExternalTransactionID, s.CorrelationID = str(refExternal), str(correlation)
	s.FailureCode = domain.FailureCode(str(failure))
	if refID.Valid {
		s.ReferenceTransactionID = refID.UUID
	}
	s.NextAttemptAt, s.ReferenceDeadline, s.CompletedAt = utc(nextAttempt), utc(deadline), utc(completed)
	s.CreatedAt, s.UpdatedAt = s.CreatedAt.UTC(), s.UpdatedAt.UTC()
	return domain.RehydrateWagerTransaction(s)
}

func nullUUID(id uuid.UUID) uuid.NullUUID { return uuid.NullUUID{UUID: id, Valid: id != uuid.Nil} }

func balanceMinor(m money.Money) *int64 {
	if !m.Valid() {
		return nil
	}
	v := m.Minor()
	return &v
}
