package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"wallet/internal/domain"
	"wallet/internal/observability/logctx"
)

const maxTxAttempts = 3

type WageringConfig struct {
	References domain.ReferencePolicy
	ClaimBatch int
	ClaimLease time.Duration
}

type WageringDeps struct {
	Tx           TxRunner
	Wallets      WalletRepository
	Transactions TransactionRepository
	Ledger       LedgerRepository
	Outbox       OutboxRepository
	Inbox        InboxRepository
	Metrics      Metrics
	Log          *slog.Logger
	Clock        Clock
	IDs          IDGenerator
	Faults       FaultHook
}

type Wagering struct {
	WageringDeps
	cfg WageringConfig
}

func NewWagering(deps WageringDeps, cfg WageringConfig) *Wagering {
	if deps.Clock == nil {
		deps.Clock = SystemClock
	}
	if deps.IDs == nil {
		deps.IDs = NewV7
	}
	if deps.Faults == nil {
		deps.Faults = NoFaults
	}
	return &Wagering{WageringDeps: deps, cfg: cfg}
}

type Command struct {
	Request       domain.WagerRequest
	CorrelationID string
	CausationID   string
	Source        string
}

type Outcome struct {
	Transaction *domain.WagerTransaction
	Replay      bool
}

type Message struct {
	Consumer  string
	MessageID string
	Hash      string
	Command   Command
}

type MessageOutcome struct {
	Outcome
	Duplicate bool
}

func (s *Wagering) Submit(ctx context.Context, cmd Command) (Outcome, error) {
	start := time.Now()
	var out Outcome
	err := s.retry(ctx, func(ctx context.Context) error {
		return s.Tx.WithinTx(ctx, func(ctx context.Context) error {
			var err error
			out, err = s.submit(ctx, cmd)
			if err == nil {
				s.Faults("wager.before_commit")
			}
			return err
		})
	})
	if err != nil {
		out = Outcome{}
		if permanentFailure(err) {
			out, err = s.recordFailure(ctx, cmd, err)
		}
	}
	s.observe(cmd, out, err, start)
	return out, err
}

func (s *Wagering) HandleMessage(ctx context.Context, msg Message) (MessageOutcome, error) {
	start := time.Now()
	var out MessageOutcome
	err := s.retry(ctx, func(ctx context.Context) error {
		return s.Tx.WithinTx(ctx, func(ctx context.Context) error {
			out = MessageOutcome{}
			entry, fresh, err := s.Inbox.Begin(ctx, msg.Consumer, msg.MessageID, msg.Hash, s.Clock())
			if err != nil {
				return err
			}
			if !fresh {
				if entry.PayloadHash != msg.Hash {
					return ErrInboxConflict
				}
				out.Duplicate = true
				if entry.TransactionID != uuid.Nil {
					out.Transaction, err = s.Transactions.Get(ctx, entry.TransactionID)
				}
				return err
			}
			o, err := s.submit(ctx, msg.Command)
			if err != nil {
				return err
			}
			out.Outcome = o
			if err := s.Inbox.Complete(ctx, msg.Consumer, msg.MessageID, o.Transaction.ID(), s.Clock()); err != nil {
				return err
			}
			s.Faults("wager.before_commit")
			return nil
		})
	})
	if err == nil && out.Duplicate {
		s.Metrics.Duplicate(msg.Command.Source, "inbox")
		return out, nil
	}
	if err != nil {
		out = MessageOutcome{}
		if permanentFailure(err) {
			out.Outcome, err = s.recordFailure(ctx, msg.Command, err)
		}
	}
	s.observe(msg.Command, out.Outcome, err, start)
	return out, err
}

func (s *Wagering) submit(ctx context.Context, cmd Command) (Outcome, error) {
	req := cmd.Request
	if out, found, err := s.existing(ctx, req); err != nil || found {
		return out, err
	}

	w, err := s.Wallets.GetForUpdate(ctx, req.WalletID)
	if errors.Is(err, ErrNotFound) {
		return Outcome{}, ErrWalletNotFound
	}
	if err != nil {
		return Outcome{}, err
	}
	if err := domain.CheckWallet(w, req); err != nil {
		return Outcome{}, err
	}
	if out, found, err := s.existing(ctx, req); err != nil || found {
		return out, err
	}

	now := s.Clock()
	t, err := domain.NewExternalTransaction(s.IDs(), req, cmd.CorrelationID, now)
	if err != nil {
		return Outcome{}, err
	}
	ref, err := s.reference(ctx, t)
	if err != nil {
		return Outcome{}, err
	}
	version := w.Version()
	entry, err := domain.Settle(t, w, ref, s.IDs(), s.cfg.References, now)
	if err != nil {
		return Outcome{}, err
	}
	if err := s.Transactions.Insert(ctx, t); err != nil {
		return Outcome{}, err
	}
	if err := s.persistMovement(ctx, w, version, entry); err != nil {
		return Outcome{}, err
	}
	if err := s.afterSettle(ctx, t, w, cmd.CorrelationID, cmd.CausationID); err != nil {
		return Outcome{}, err
	}
	return Outcome{Transaction: t}, nil
}

func (s *Wagering) existing(ctx context.Context, req domain.WagerRequest) (Outcome, bool, error) {
	rows, err := s.Transactions.Existing(ctx, req.ProviderID, req.IdempotencyKey, req.ExternalTransactionID)
	if err != nil || len(rows) == 0 {
		return Outcome{}, false, err
	}
	for _, t := range rows {
		if t.IdempotencyKey() == req.IdempotencyKey {
			if t.PayloadHash() != req.PayloadHash() {
				return Outcome{}, true, ErrIdempotencyConflict
			}
			return Outcome{Transaction: t, Replay: true}, true, nil
		}
	}
	return Outcome{}, true, ErrExternalIDConflict
}

func (s *Wagering) reference(ctx context.Context, t *domain.WagerTransaction) (*domain.Reference, error) {
	if !t.NeedsReference() {
		return nil, nil
	}
	target, err := s.Transactions.ByExternalID(ctx, t.ProviderID(), t.ReferenceExternalID())
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	reversed, err := s.Transactions.HasProcessedReversal(ctx, target.ID())
	if err != nil {
		return nil, err
	}
	return &domain.Reference{Transaction: target, Reversed: reversed}, nil
}

func (s *Wagering) persistMovement(ctx context.Context, w *domain.Wallet, previousVersion int64, entry *domain.LedgerEntry) error {
	if entry == nil {
		return nil
	}
	if err := s.Wallets.Update(ctx, w, previousVersion); err != nil {
		return err
	}
	return s.Ledger.Append(ctx, *entry)
}

func (s *Wagering) afterSettle(ctx context.Context, t *domain.WagerTransaction, w *domain.Wallet, correlationID, causationID string) error {
	events := append(t.PullEvents(), w.PullEvents()...)
	if err := s.Outbox.Append(ctx, envelopes(s.IDs, events, correlationID, causationID)); err != nil {
		return err
	}
	if t.Status() == domain.StatusProcessed {
		return s.Transactions.WakeWaiting(ctx, t.ProviderID(), t.ExternalTransactionID())
	}
	return nil
}

func (s *Wagering) ResolveDue(ctx context.Context) (int, error) {
	due, err := s.Transactions.ClaimDue(ctx, s.cfg.ClaimBatch, s.cfg.ClaimLease)
	if err != nil {
		return 0, err
	}
	for _, d := range due {
		if ctx.Err() != nil {
			return len(due), ctx.Err()
		}
		ctx := logctx.With(ctx, "transactionId", d.ID.String(), "walletId", d.WalletID.String())
		err := s.retry(ctx, func(ctx context.Context) error { return s.resolve(ctx, d) })
		switch {
		case err == nil:
		case Transient(err) || errors.Is(err, context.Canceled):
			s.Metrics.Retry("reference")
			s.Log.WarnContext(ctx, "reference resolution will be retried", "error", err)
		default:
			s.Log.ErrorContext(ctx, "reference resolution failed permanently", "error", err)
			if ferr := s.markFailed(ctx, d.ID); ferr != nil {
				s.Log.ErrorContext(ctx, "could not record failure", "error", ferr)
			}
		}
	}
	return len(due), nil
}

func (s *Wagering) resolve(ctx context.Context, d DueTransaction) error {
	start := time.Now()
	var t *domain.WagerTransaction
	err := s.Tx.WithinTx(ctx, func(ctx context.Context) error {
		w, err := s.Wallets.GetForUpdate(ctx, d.WalletID)
		if err != nil {
			return err
		}
		t, err = s.Transactions.GetForUpdate(ctx, d.ID)
		if err != nil {
			return err
		}
		if t.Status() != domain.StatusPendingReference {
			t = nil
			return nil
		}
		ref, err := s.reference(ctx, t)
		if err != nil {
			return err
		}
		version := w.Version()
		entry, err := domain.Settle(t, w, ref, s.IDs(), s.cfg.References, s.Clock())
		if err != nil {
			return err
		}
		if err := s.Transactions.Update(ctx, t); err != nil {
			return err
		}
		if err := s.persistMovement(ctx, w, version, entry); err != nil {
			return err
		}
		return s.afterSettle(ctx, t, w, t.CorrelationID(), t.ID().String())
	})
	if err == nil && t != nil && t.Status().Terminal() {
		s.Metrics.TransactionResult("worker", t.Kind(), t.Status(), false)
		s.Metrics.ProcessingDuration("worker", time.Since(start))
		s.Log.InfoContext(ctx, "pending reference settled", "status", t.Status(), "failureCode", t.FailureCode())
	}
	return err
}

func (s *Wagering) markFailed(ctx context.Context, id uuid.UUID) error {
	return s.Tx.WithinTx(ctx, func(ctx context.Context) error {
		t, err := s.Transactions.GetForUpdate(ctx, id)
		if err != nil {
			return err
		}
		if t.Status().Terminal() {
			return nil
		}
		if err := t.Fail(s.Clock()); err != nil {
			return err
		}
		return s.Transactions.Update(ctx, t)
	})
}

func (s *Wagering) recordFailure(ctx context.Context, cmd Command, cause error) (Outcome, error) {
	s.Log.ErrorContext(ctx, "operation failed permanently", "error", cause)
	var out Outcome
	err := s.Tx.WithinTx(ctx, func(ctx context.Context) error {
		if o, found, err := s.existing(ctx, cmd.Request); err != nil || found {
			out = o
			return err
		}
		t, err := domain.NewExternalTransaction(s.IDs(), cmd.Request, cmd.CorrelationID, s.Clock())
		if err != nil {
			return err
		}
		if err := t.Fail(s.Clock()); err != nil {
			return err
		}
		out = Outcome{Transaction: t}
		return s.Transactions.Insert(ctx, t)
	})
	if err != nil {
		return Outcome{}, fmt.Errorf("%w (recording the failure also failed: %v)", cause, err)
	}
	return out, nil
}

func (s *Wagering) GetTransaction(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	return s.Transactions.Get(ctx, id)
}

func (s *Wagering) GetByExternalID(ctx context.Context, providerID, externalID string) (*domain.WagerTransaction, error) {
	return s.Transactions.ByExternalID(ctx, providerID, externalID)
}

func (s *Wagering) retry(ctx context.Context, fn func(context.Context) error) error {
	for attempt := 1; ; attempt++ {
		err := fn(ctx)
		if !errors.Is(err, ErrConcurrentWrite) {
			return err
		}
		s.Metrics.ConcurrencyConflict("concurrent_write")
		if attempt == maxTxAttempts || ctx.Err() != nil {
			return fmt.Errorf("%w: %w", ErrUnavailable, err)
		}
		s.Metrics.Retry("transaction")
	}
}

func (s *Wagering) observe(cmd Command, out Outcome, err error, start time.Time) {
	s.Metrics.ProcessingDuration(cmd.Source, time.Since(start))
	if errors.Is(err, ErrLockTimeout) {
		s.Metrics.ConcurrencyConflict("lock_timeout")
	}
	if out.Transaction == nil {
		return
	}
	t := out.Transaction
	s.Metrics.TransactionResult(cmd.Source, t.Kind(), t.Status(), out.Replay)
	if out.Replay {
		s.Metrics.Duplicate(cmd.Source, "idempotency_key")
	}
}

func permanentFailure(err error) bool {
	switch {
	case Transient(err), errors.Is(err, context.Canceled),
		errors.Is(err, domain.ErrInvalid), errors.Is(err, ErrNotFound),
		errors.Is(err, ErrIdempotencyConflict), errors.Is(err, ErrExternalIDConflict),
		errors.Is(err, ErrInboxConflict):
		return false
	}
	return true
}

func envelopes(ids IDGenerator, events []domain.Event, correlationID, causationID string) []domain.Envelope {
	out := make([]domain.Envelope, len(events))
	for i, e := range events {
		out[i] = domain.NewEnvelope(ids(), e, correlationID, causationID)
	}
	return out
}
