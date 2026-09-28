package domain

import (
	"fmt"
	"time"

	"github.com/google/uuid"

	"wallet/internal/domain/money"
)

type WagerTransaction struct {
	id                  uuid.UUID
	origin              Origin
	kind                Kind
	status              Status
	walletID            uuid.UUID
	playerID            uuid.UUID
	money               money.Money
	providerID          string
	externalID          string
	idempotencyKey      string
	payloadHash         string
	roundID             string
	gameID              string
	referenceExternalID string
	referenceID         uuid.UUID
	failureCode         FailureCode
	balanceAfter        money.Money
	correlationID       string
	attempts            int
	nextAttemptAt       time.Time
	referenceDeadline   time.Time
	createdAt           time.Time
	updatedAt           time.Time
	completedAt         time.Time
	events              []Event
}

type WagerTransactionSnapshot struct {
	ID                             uuid.UUID
	Origin                         Origin
	Kind                           Kind
	Status                         Status
	WalletID                       uuid.UUID
	PlayerID                       uuid.UUID
	Money                          money.Money
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PayloadHash                    string
	RoundID                        string
	GameID                         string
	ReferenceExternalTransactionID string
	ReferenceTransactionID         uuid.UUID
	FailureCode                    FailureCode
	BalanceAfter                   money.Money
	CorrelationID                  string
	Attempts                       int
	NextAttemptAt                  time.Time
	ReferenceDeadline              time.Time
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
	CompletedAt                    time.Time
}

func NewExternalTransaction(id uuid.UUID, req WagerRequest, correlationID string, now time.Time) (*WagerTransaction, error) {
	if id == uuid.Nil || now.IsZero() {
		return nil, fmt.Errorf("%w: transaction id and time are required", ErrInvariant)
	}
	if !req.Kind.External() || !req.Money.Valid() || req.ProviderID == "" || req.IdempotencyKey == "" {
		return nil, fmt.Errorf("%w: request was not validated", ErrInvariant)
	}
	return &WagerTransaction{
		id:                  id,
		origin:              OriginExternal,
		kind:                req.Kind,
		status:              StatusPending,
		walletID:            req.WalletID,
		playerID:            req.PlayerID,
		money:               req.Money,
		providerID:          req.ProviderID,
		externalID:          req.ExternalTransactionID,
		idempotencyKey:      req.IdempotencyKey,
		payloadHash:         req.PayloadHash(),
		roundID:             req.RoundID,
		gameID:              req.GameID,
		referenceExternalID: req.ReferenceExternalTransactionID,
		correlationID:       correlationID,
		createdAt:           now,
		updatedAt:           now,
	}, nil
}

func newOpeningTransaction(id uuid.UUID, w *Wallet, amount money.Money, now time.Time) (*WagerTransaction, error) {
	if id == uuid.Nil {
		return nil, fmt.Errorf("%w: opening transaction id is required", ErrInvariant)
	}
	if !amount.IsPositive() {
		return nil, fmt.Errorf("%w: opening amount must be positive", ErrInvariant)
	}
	return &WagerTransaction{
		id:        id,
		origin:    OriginInternal,
		kind:      KindOpening,
		status:    StatusPending,
		walletID:  w.id,
		playerID:  w.playerID,
		money:     amount,
		createdAt: now,
		updatedAt: now,
	}, nil
}

func RehydrateWagerTransaction(s WagerTransactionSnapshot) (*WagerTransaction, error) {
	if s.ID == uuid.Nil || s.WalletID == uuid.Nil || s.PlayerID == uuid.Nil {
		return nil, fmt.Errorf("%w: transaction ids are required", ErrInvariant)
	}
	if _, err := ParseKind(string(s.Kind)); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvariant, err)
	}
	if _, err := ParseStatus(string(s.Status)); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvariant, err)
	}
	if !s.Money.Valid() || s.Money.IsNegative() {
		return nil, fmt.Errorf("%w: transaction money is invalid", ErrInvariant)
	}
	if (s.Origin == OriginInternal) != (s.Kind == KindOpening) {
		return nil, fmt.Errorf("%w: origin %s does not match kind %s", ErrInvariant, s.Origin, s.Kind)
	}
	if s.Origin == OriginExternal && (s.ProviderID == "" || s.ExternalTransactionID == "" || s.IdempotencyKey == "" || s.PayloadHash == "") {
		return nil, fmt.Errorf("%w: external transaction without external identity", ErrInvariant)
	}
	if s.Status == StatusProcessed && !s.BalanceAfter.Valid() {
		return nil, fmt.Errorf("%w: processed transaction without result balance", ErrInvariant)
	}
	if (s.Status == StatusRejected || s.Status == StatusFailed) && s.FailureCode == "" {
		return nil, fmt.Errorf("%w: %s transaction without failure code", ErrInvariant, s.Status)
	}
	return &WagerTransaction{
		id:                  s.ID,
		origin:              s.Origin,
		kind:                s.Kind,
		status:              s.Status,
		walletID:            s.WalletID,
		playerID:            s.PlayerID,
		money:               s.Money,
		providerID:          s.ProviderID,
		externalID:          s.ExternalTransactionID,
		idempotencyKey:      s.IdempotencyKey,
		payloadHash:         s.PayloadHash,
		roundID:             s.RoundID,
		gameID:              s.GameID,
		referenceExternalID: s.ReferenceExternalTransactionID,
		referenceID:         s.ReferenceTransactionID,
		failureCode:         s.FailureCode,
		balanceAfter:        s.BalanceAfter,
		correlationID:       s.CorrelationID,
		attempts:            s.Attempts,
		nextAttemptAt:       s.NextAttemptAt,
		referenceDeadline:   s.ReferenceDeadline,
		createdAt:           s.CreatedAt,
		updatedAt:           s.UpdatedAt,
		completedAt:         s.CompletedAt,
	}, nil
}

func (t *WagerTransaction) Snapshot() WagerTransactionSnapshot {
	return WagerTransactionSnapshot{
		ID:                             t.id,
		Origin:                         t.origin,
		Kind:                           t.kind,
		Status:                         t.status,
		WalletID:                       t.walletID,
		PlayerID:                       t.playerID,
		Money:                          t.money,
		ProviderID:                     t.providerID,
		ExternalTransactionID:          t.externalID,
		IdempotencyKey:                 t.idempotencyKey,
		PayloadHash:                    t.payloadHash,
		RoundID:                        t.roundID,
		GameID:                         t.gameID,
		ReferenceExternalTransactionID: t.referenceExternalID,
		ReferenceTransactionID:         t.referenceID,
		FailureCode:                    t.failureCode,
		BalanceAfter:                   t.balanceAfter,
		CorrelationID:                  t.correlationID,
		Attempts:                       t.attempts,
		NextAttemptAt:                  t.nextAttemptAt,
		ReferenceDeadline:              t.referenceDeadline,
		CreatedAt:                      t.createdAt,
		UpdatedAt:                      t.updatedAt,
		CompletedAt:                    t.completedAt,
	}
}

func (t *WagerTransaction) ID() uuid.UUID                 { return t.id }
func (t *WagerTransaction) Origin() Origin                { return t.origin }
func (t *WagerTransaction) Kind() Kind                    { return t.kind }
func (t *WagerTransaction) Status() Status                { return t.status }
func (t *WagerTransaction) WalletID() uuid.UUID           { return t.walletID }
func (t *WagerTransaction) PlayerID() uuid.UUID           { return t.playerID }
func (t *WagerTransaction) Money() money.Money            { return t.money }
func (t *WagerTransaction) ProviderID() string            { return t.providerID }
func (t *WagerTransaction) ExternalTransactionID() string { return t.externalID }
func (t *WagerTransaction) IdempotencyKey() string        { return t.idempotencyKey }
func (t *WagerTransaction) PayloadHash() string           { return t.payloadHash }
func (t *WagerTransaction) RoundID() string               { return t.roundID }
func (t *WagerTransaction) ReferenceExternalID() string   { return t.referenceExternalID }
func (t *WagerTransaction) ReferenceID() uuid.UUID        { return t.referenceID }
func (t *WagerTransaction) FailureCode() FailureCode      { return t.failureCode }
func (t *WagerTransaction) BalanceAfter() money.Money     { return t.balanceAfter }
func (t *WagerTransaction) CorrelationID() string         { return t.correlationID }
func (t *WagerTransaction) Attempts() int                 { return t.attempts }
func (t *WagerTransaction) NextAttemptAt() time.Time      { return t.nextAttemptAt }
func (t *WagerTransaction) ReferenceDeadline() time.Time  { return t.referenceDeadline }
func (t *WagerTransaction) NeedsReference() bool          { return t.referenceExternalID != "" }

func (t *WagerTransaction) PullEvents() []Event {
	e := t.events
	t.events = nil
	return e
}

func (t *WagerTransaction) record(e Event)      { t.events = append(t.events, e) }
func (t *WagerTransaction) touch(now time.Time) { t.updatedAt = now }

func (t *WagerTransaction) complete(to Status, now time.Time) {
	t.status = to
	t.completedAt = now
	t.nextAttemptAt = time.Time{}
	t.updatedAt = now
}

func (t *WagerTransaction) transition(to Status) error {
	allowed := false
	switch t.status {
	case StatusPending:
		allowed = to == StatusProcessed || to == StatusRejected || to == StatusFailed || to == StatusPendingReference
	case StatusPendingReference:
		allowed = to == StatusProcessed || to == StatusRejected || to == StatusFailed || to == StatusPendingReference
	}
	if !allowed {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.status, to)
	}
	return nil
}

func (t *WagerTransaction) MarkProcessed(balanceAfter money.Money, now time.Time) error {
	if err := t.transition(StatusProcessed); err != nil {
		return err
	}
	if !balanceAfter.Valid() || balanceAfter.IsNegative() || balanceAfter.Currency() != t.money.Currency() {
		return fmt.Errorf("%w: invalid result balance %v", ErrInvariant, balanceAfter)
	}
	t.balanceAfter = balanceAfter
	t.complete(StatusProcessed, now)
	ev := WagerTransactionProcessed{
		TransactionID:         t.id,
		Origin:                t.origin,
		Kind:                  t.kind,
		WalletID:              t.walletID,
		PlayerID:              t.playerID,
		ProviderID:            t.providerID,
		ExternalTransactionID: t.externalID,
		RoundID:               t.roundID,
		GameID:                t.gameID,
		Money:                 t.money,
		BalanceAfter:          balanceAfter,
		ProcessedAt:           Timestamp(now),
	}
	if t.referenceID != uuid.Nil {
		ref := t.referenceID
		ev.ReferenceTransactionID = &ref
	}
	t.record(ev)
	return nil
}

func (t *WagerTransaction) Reject(code FailureCode, now time.Time) error {
	if err := t.transition(StatusRejected); err != nil {
		return err
	}
	if !code.rejection() {
		return fmt.Errorf("%w: %q is not a rejection code", ErrInvariant, code)
	}
	t.failureCode = code
	t.complete(StatusRejected, now)
	t.record(WagerTransactionRejected{
		TransactionID:         t.id,
		Kind:                  t.kind,
		WalletID:              t.walletID,
		PlayerID:              t.playerID,
		ProviderID:            t.providerID,
		ExternalTransactionID: t.externalID,
		RoundID:               t.roundID,
		GameID:                t.gameID,
		Money:                 t.money,
		FailureCode:           code,
		RejectedAt:            Timestamp(now),
	})
	return nil
}

func (t *WagerTransaction) Fail(now time.Time) error {
	if err := t.transition(StatusFailed); err != nil {
		return err
	}
	t.failureCode = FailureProcessingFailed
	t.complete(StatusFailed, now)
	return nil
}

func (t *WagerTransaction) AwaitReference(nextAttempt, deadline, now time.Time) error {
	if t.status != StatusPending {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, t.status, StatusPendingReference)
	}
	if !t.NeedsReference() || !nextAttempt.After(now) || !deadline.After(now) {
		return fmt.Errorf("%w: invalid reference wait", ErrInvariant)
	}
	t.status = StatusPendingReference
	t.attempts = 1
	t.nextAttemptAt = nextAttempt
	t.referenceDeadline = deadline
	t.touch(now)
	t.record(WagerTransactionPendingReference{
		TransactionID:                  t.id,
		Kind:                           t.kind,
		WalletID:                       t.walletID,
		PlayerID:                       t.playerID,
		ProviderID:                     t.providerID,
		ExternalTransactionID:          t.externalID,
		ReferenceExternalTransactionID: t.referenceExternalID,
		Money:                          t.money,
		RetryDeadline:                  Timestamp(deadline),
		RegisteredAt:                   Timestamp(now),
	})
	return nil
}

func (t *WagerTransaction) RetryReferenceLater(nextAttempt, now time.Time) error {
	if t.status != StatusPendingReference {
		return fmt.Errorf("%w: retry from %s", ErrInvalidTransition, t.status)
	}
	if !nextAttempt.After(now) {
		return fmt.Errorf("%w: next attempt must be in the future", ErrInvariant)
	}
	t.attempts++
	t.nextAttemptAt = nextAttempt
	t.touch(now)
	return nil
}
