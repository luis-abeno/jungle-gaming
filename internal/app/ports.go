package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"wallet/internal/domain"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrWalletNotFound      = fmt.Errorf("wallet %w", ErrNotFound)
	ErrWalletExists        = errors.New("wallet already exists for player and currency")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different payload")
	ErrExternalIDConflict  = errors.New("external transaction already registered under another idempotency key")
	ErrInboxConflict       = errors.New("message id reused with a different payload")

	ErrUnavailable     = errors.New("temporarily unavailable")
	ErrLockTimeout     = fmt.Errorf("%w: lock timeout", ErrUnavailable)
	ErrConcurrentWrite = errors.New("concurrent write")
)

func Transient(err error) bool {
	return errors.Is(err, ErrUnavailable) || errors.Is(err, ErrConcurrentWrite) ||
		errors.Is(err, context.DeadlineExceeded)
}

type TxRunner interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
	WithinSnapshot(ctx context.Context, fn func(ctx context.Context) error) error
}

type WalletRepository interface {
	Insert(ctx context.Context, w *domain.Wallet) error
	Get(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error)
	Update(ctx context.Context, w *domain.Wallet, previousVersion int64) error
}

type TransactionRepository interface {
	Insert(ctx context.Context, t *domain.WagerTransaction) error
	Update(ctx context.Context, t *domain.WagerTransaction) error
	Get(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error)
	GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error)
	ByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error)
	Existing(ctx context.Context, providerID, idempotencyKey, externalID string) ([]*domain.WagerTransaction, error)
	HasProcessedReversal(ctx context.Context, id uuid.UUID) (bool, error)
	ClaimDue(ctx context.Context, limit int, lease time.Duration) ([]DueTransaction, error)
	WakeWaiting(ctx context.Context, providerID, externalID string) error
}

type DueTransaction struct {
	ID       uuid.UUID
	WalletID uuid.UUID
}

type LedgerRepository interface {
	Append(ctx context.Context, e domain.LedgerEntry) error
	Page(ctx context.Context, walletID uuid.UUID, afterVersion int64, limit int) ([]domain.LedgerEntry, error)
	Totals(ctx context.Context, walletID uuid.UUID) (LedgerTotals, error)
}

type LedgerTotals struct {
	NetMinor int64
	Entries  int64
}

type OutboxRepository interface {
	Append(ctx context.Context, envs []domain.Envelope) error
}

type OutboxStore interface {
	Claim(ctx context.Context, limit int, lease time.Duration) ([]PendingEvent, error)
	MarkPublished(ctx context.Context, ids ...uuid.UUID) error
	Reschedule(ctx context.Context, id uuid.UUID, delay time.Duration, reason string) error
	Stats(ctx context.Context) (OutboxStats, error)
}

type PendingEvent struct {
	ID           uuid.UUID
	EventType    string
	PartitionKey string
	Payload      string
	Attempts     int
	OccurredAt   time.Time
}

type OutboxStats struct {
	Pending int64
	Oldest  time.Time
}

type EventPublisher interface {
	PublishBatch(ctx context.Context, events []PendingEvent) (failed map[uuid.UUID]error, err error)
}

type InboxRepository interface {
	Begin(ctx context.Context, consumer, messageID, hash string, now time.Time) (entry InboxEntry, fresh bool, err error)
	Complete(ctx context.Context, consumer, messageID string, transactionID uuid.UUID, now time.Time) error
}

type InboxEntry struct {
	PayloadHash   string
	TransactionID uuid.UUID
}

type Metrics interface {
	TransactionResult(source string, kind domain.Kind, status domain.Status, replay bool)
	Duplicate(source, reason string)
	Retry(component string)
	ConcurrencyConflict(reason string)
	ProcessingDuration(source string, d time.Duration)
	ReconciliationDivergence()
	OutboxPublished()
	OutboxPublishFailed()
	OutboxBacklog(pending int64, lag time.Duration)
}

type Clock func() time.Time

func SystemClock() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

type IDGenerator func() uuid.UUID

func NewV7() uuid.UUID { return uuid.Must(uuid.NewV7()) }

type FaultHook func(point string)

func NoFaults(string) {}
