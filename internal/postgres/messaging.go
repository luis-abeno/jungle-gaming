package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"wallet/internal/app"
	"wallet/internal/domain"
)

type OutboxRepo struct{ tm *TxManager }

func NewOutboxRepo(tm *TxManager) *OutboxRepo { return &OutboxRepo{tm} }

func (r *OutboxRepo) Append(ctx context.Context, envs []domain.Envelope) error {
	if len(envs) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, e := range envs {
		payload, err := json.Marshal(e)
		if err != nil {
			return fmt.Errorf("encode event %s: %w", e.EventType, err)
		}
		batch.Queue(`
			INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, event_version,
				partition_key, payload, occurred_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7::json, $8)`,
			e.EventID, e.Data.AggregateType(), e.AggregateID, e.EventType, e.Version,
			e.Data.PartitionKey().String(), string(payload), e.Data.OccurredAt())
	}
	return classify(r.tm.q(ctx).SendBatch(ctx, batch).Close())
}

func (r *OutboxRepo) Claim(ctx context.Context, limit int, lease time.Duration) ([]app.PendingEvent, error) {
	rows, err := r.tm.q(ctx).Query(ctx, `
		UPDATE outbox_events o
		SET attempts = o.attempts + 1, next_attempt_at = now() + $2 * interval '1 millisecond'
		FROM (
			SELECT id FROM outbox_events
			WHERE published_at IS NULL AND next_attempt_at <= now()
			ORDER BY next_attempt_at, occurred_at
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		) due
		WHERE o.id = due.id
		RETURNING o.id, o.event_type, o.partition_key, o.payload::text, o.attempts, o.occurred_at`,
		limit, lease.Milliseconds())
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var out []app.PendingEvent
	for rows.Next() {
		var e app.PendingEvent
		if err := rows.Scan(&e.ID, &e.EventType, &e.PartitionKey, &e.Payload, &e.Attempts, &e.OccurredAt); err != nil {
			return nil, classify(err)
		}
		out = append(out, e)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].OccurredAt.Before(out[j].OccurredAt) })
	return out, classify(rows.Err())
}

func (r *OutboxRepo) MarkPublished(ctx context.Context, ids ...uuid.UUID) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := r.tm.q(ctx).Exec(ctx, `
		UPDATE outbox_events SET published_at = now(), last_error = NULL
		WHERE id = ANY($1) AND published_at IS NULL`, ids)
	return classify(err)
}

func (r *OutboxRepo) Reschedule(ctx context.Context, id uuid.UUID, delay time.Duration, reason string) error {
	_, err := r.tm.q(ctx).Exec(ctx, `
		UPDATE outbox_events SET next_attempt_at = now() + $2 * interval '1 millisecond', last_error = $3
		WHERE id = $1 AND published_at IS NULL`, id, delay.Milliseconds(), reason)
	return classify(err)
}

func (r *OutboxRepo) Stats(ctx context.Context) (app.OutboxStats, error) {
	var s app.OutboxStats
	var oldest *time.Time
	err := r.tm.q(ctx).QueryRow(ctx, `
		SELECT count(*), min(occurred_at) FROM outbox_events WHERE published_at IS NULL`).Scan(&s.Pending, &oldest)
	s.Oldest = utc(oldest)
	return s, classify(err)
}

type InboxRepo struct{ tm *TxManager }

func NewInboxRepo(tm *TxManager) *InboxRepo { return &InboxRepo{tm} }

func (r *InboxRepo) Begin(ctx context.Context, consumer, messageID, hash string, now time.Time) (app.InboxEntry, bool, error) {
	q := r.tm.q(ctx)
	tag, err := q.Exec(ctx, `
		INSERT INTO inbox_messages (consumer_name, message_id, payload_hash, received_at)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (consumer_name, message_id) DO NOTHING`, consumer, messageID, hash, now)
	if err != nil {
		return app.InboxEntry{}, false, classify(err)
	}
	if tag.RowsAffected() == 1 {
		return app.InboxEntry{}, true, nil
	}
	var e app.InboxEntry
	var txID uuid.NullUUID
	err = q.QueryRow(ctx, `
		SELECT payload_hash, transaction_id FROM inbox_messages
		WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID).Scan(&e.PayloadHash, &txID)
	if txID.Valid {
		e.TransactionID = txID.UUID
	}
	return e, false, classify(err)
}

func (r *InboxRepo) Complete(ctx context.Context, consumer, messageID string, transactionID uuid.UUID, now time.Time) error {
	_, err := r.tm.q(ctx).Exec(ctx, `
		UPDATE inbox_messages SET transaction_id = $3, completed_at = $4
		WHERE consumer_name = $1 AND message_id = $2`, consumer, messageID, nullUUID(transactionID), now)
	return classify(err)
}
