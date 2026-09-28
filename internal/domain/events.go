package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"wallet/internal/domain/money"
)

type Event interface {
	EventType() string
	EventVersion() int
	AggregateType() string
	AggregateID() uuid.UUID
	PartitionKey() uuid.UUID
	OccurredAt() time.Time
}

type Timestamp time.Time

func (t Timestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time(t).UTC().Format("2006-01-02T15:04:05.000Z07:00"))
}

func (t *Timestamp) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return err
	}
	v, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return err
	}
	*t = Timestamp(v.UTC())
	return nil
}

type WagerTransactionProcessed struct {
	TransactionID          uuid.UUID   `json:"transactionId"`
	Origin                 Origin      `json:"origin"`
	Kind                   Kind        `json:"kind"`
	WalletID               uuid.UUID   `json:"walletId"`
	PlayerID               uuid.UUID   `json:"playerId"`
	ProviderID             string      `json:"providerId,omitempty"`
	ExternalTransactionID  string      `json:"externalTransactionId,omitempty"`
	RoundID                string      `json:"roundId,omitempty"`
	GameID                 string      `json:"gameId,omitempty"`
	ReferenceTransactionID *uuid.UUID  `json:"referenceTransactionId,omitempty"`
	Money                  money.Money `json:"money"`
	BalanceAfter           money.Money `json:"balanceAfter"`
	ProcessedAt            Timestamp   `json:"processedAt"`
}

func (WagerTransactionProcessed) EventType() string         { return "WagerTransactionProcessed" }
func (WagerTransactionProcessed) EventVersion() int         { return 1 }
func (WagerTransactionProcessed) AggregateType() string     { return "WagerTransaction" }
func (e WagerTransactionProcessed) AggregateID() uuid.UUID  { return e.TransactionID }
func (e WagerTransactionProcessed) PartitionKey() uuid.UUID { return e.WalletID }
func (e WagerTransactionProcessed) OccurredAt() time.Time   { return time.Time(e.ProcessedAt) }

type WagerTransactionRejected struct {
	TransactionID         uuid.UUID   `json:"transactionId"`
	Kind                  Kind        `json:"kind"`
	WalletID              uuid.UUID   `json:"walletId"`
	PlayerID              uuid.UUID   `json:"playerId"`
	ProviderID            string      `json:"providerId"`
	ExternalTransactionID string      `json:"externalTransactionId"`
	RoundID               string      `json:"roundId"`
	GameID                string      `json:"gameId"`
	Money                 money.Money `json:"money"`
	FailureCode           FailureCode `json:"failureCode"`
	RejectedAt            Timestamp   `json:"rejectedAt"`
}

func (WagerTransactionRejected) EventType() string         { return "WagerTransactionRejected" }
func (WagerTransactionRejected) EventVersion() int         { return 1 }
func (WagerTransactionRejected) AggregateType() string     { return "WagerTransaction" }
func (e WagerTransactionRejected) AggregateID() uuid.UUID  { return e.TransactionID }
func (e WagerTransactionRejected) PartitionKey() uuid.UUID { return e.WalletID }
func (e WagerTransactionRejected) OccurredAt() time.Time   { return time.Time(e.RejectedAt) }

type WagerTransactionPendingReference struct {
	TransactionID                  uuid.UUID   `json:"transactionId"`
	Kind                           Kind        `json:"kind"`
	WalletID                       uuid.UUID   `json:"walletId"`
	PlayerID                       uuid.UUID   `json:"playerId"`
	ProviderID                     string      `json:"providerId"`
	ExternalTransactionID          string      `json:"externalTransactionId"`
	ReferenceExternalTransactionID string      `json:"referenceExternalTransactionId"`
	Money                          money.Money `json:"money"`
	RetryDeadline                  Timestamp   `json:"retryDeadline"`
	RegisteredAt                   Timestamp   `json:"registeredAt"`
}

func (WagerTransactionPendingReference) EventType() string         { return "WagerTransactionPendingReference" }
func (WagerTransactionPendingReference) EventVersion() int         { return 1 }
func (WagerTransactionPendingReference) AggregateType() string     { return "WagerTransaction" }
func (e WagerTransactionPendingReference) AggregateID() uuid.UUID  { return e.TransactionID }
func (e WagerTransactionPendingReference) PartitionKey() uuid.UUID { return e.WalletID }
func (e WagerTransactionPendingReference) OccurredAt() time.Time   { return time.Time(e.RegisteredAt) }

type WalletBalanceChanged struct {
	WalletID      uuid.UUID   `json:"walletId"`
	TransactionID uuid.UUID   `json:"transactionId"`
	Direction     Direction   `json:"direction"`
	Money         money.Money `json:"money"`
	BalanceBefore money.Money `json:"balanceBefore"`
	BalanceAfter  money.Money `json:"balanceAfter"`
	WalletVersion int64       `json:"walletVersion"`
	ChangedAt     Timestamp   `json:"changedAt"`
}

func (WalletBalanceChanged) EventType() string         { return "WalletBalanceChanged" }
func (WalletBalanceChanged) EventVersion() int         { return 1 }
func (WalletBalanceChanged) AggregateType() string     { return "Wallet" }
func (e WalletBalanceChanged) AggregateID() uuid.UUID  { return e.WalletID }
func (e WalletBalanceChanged) PartitionKey() uuid.UUID { return e.WalletID }
func (e WalletBalanceChanged) OccurredAt() time.Time   { return time.Time(e.ChangedAt) }

type Envelope struct {
	EventID       uuid.UUID `json:"eventId"`
	EventType     string    `json:"eventType"`
	AggregateID   uuid.UUID `json:"aggregateId"`
	CorrelationID string    `json:"correlationId"`
	CausationID   string    `json:"causationId,omitempty"`
	OccurredAt    Timestamp `json:"occurredAt"`
	Version       int       `json:"version"`
	Data          Event     `json:"data"`
}

func NewEnvelope(id uuid.UUID, e Event, correlationID, causationID string) Envelope {
	return Envelope{
		EventID:       id,
		EventType:     e.EventType(),
		AggregateID:   e.AggregateID(),
		CorrelationID: correlationID,
		CausationID:   causationID,
		OccurredAt:    Timestamp(e.OccurredAt()),
		Version:       e.EventVersion(),
		Data:          e,
	}
}
