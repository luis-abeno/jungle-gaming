package app

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"

	"wallet/internal/domain"
)

type memory struct {
	mu      sync.Mutex
	wallets map[uuid.UUID]domain.WalletSnapshot
	txs     map[uuid.UUID]domain.WagerTransactionSnapshot
	ledger  []domain.LedgerEntry
	outbox  []domain.Envelope
	inbox   map[string]InboxEntry

	failInsert []error
}

func newMemory() *memory {
	return &memory{
		wallets: map[uuid.UUID]domain.WalletSnapshot{},
		txs:     map[uuid.UUID]domain.WagerTransactionSnapshot{},
		inbox:   map[string]InboxEntry{},
	}
}

func (m *memory) WithinTx(ctx context.Context, fn func(context.Context) error) error { return fn(ctx) }
func (m *memory) WithinSnapshot(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}

type memWallets struct{ *memory }

func (r memWallets) Insert(_ context.Context, w *domain.Wallet) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.wallets {
		if s.PlayerID == w.PlayerID() && s.Balance.Currency() == w.Currency() {
			return ErrWalletExists
		}
	}
	r.wallets[w.ID()] = w.Snapshot()
	return nil
}

func (r memWallets) Get(_ context.Context, id uuid.UUID) (*domain.Wallet, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.wallets[id]
	if !ok {
		return nil, ErrNotFound
	}
	return domain.RehydrateWallet(s)
}

func (r memWallets) GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.Wallet, error) {
	return r.Get(ctx, id)
}

func (r memWallets) Update(_ context.Context, w *domain.Wallet, previous int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.wallets[w.ID()].Version != previous {
		return ErrConcurrentWrite
	}
	r.wallets[w.ID()] = w.Snapshot()
	return nil
}

type memTxs struct{ *memory }

func (r memTxs) Insert(_ context.Context, t *domain.WagerTransaction) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.failInsert) > 0 {
		err := r.failInsert[0]
		r.failInsert = r.failInsert[1:]
		return err
	}
	for _, s := range r.txs {
		if s.ProviderID != "" && s.ProviderID == t.ProviderID() &&
			(s.IdempotencyKey == t.IdempotencyKey() || s.ExternalTransactionID == t.ExternalTransactionID()) {
			return ErrConcurrentWrite
		}
	}
	r.txs[t.ID()] = t.Snapshot()
	return nil
}

func (r memTxs) Update(_ context.Context, t *domain.WagerTransaction) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.txs[t.ID()] = t.Snapshot()
	return nil
}

func (r memTxs) Get(_ context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.txs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return domain.RehydrateWagerTransaction(s)
}

func (r memTxs) GetForUpdate(ctx context.Context, id uuid.UUID) (*domain.WagerTransaction, error) {
	return r.Get(ctx, id)
}

func (r memTxs) ByExternalID(_ context.Context, provider, external string) (*domain.WagerTransaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.txs {
		if s.ProviderID == provider && s.ExternalTransactionID == external {
			return domain.RehydrateWagerTransaction(s)
		}
	}
	return nil, ErrNotFound
}

func (r memTxs) Existing(_ context.Context, provider, key, external string) ([]*domain.WagerTransaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*domain.WagerTransaction
	for _, s := range r.txs {
		if s.ProviderID == provider && (s.IdempotencyKey == key || s.ExternalTransactionID == external) {
			t, _ := domain.RehydrateWagerTransaction(s)
			out = append(out, t)
		}
	}
	return out, nil
}

func (r memTxs) HasProcessedReversal(_ context.Context, id uuid.UUID) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, s := range r.txs {
		if s.ReferenceTransactionID == id && s.Status == domain.StatusProcessed && s.Kind.Reversal() {
			return true, nil
		}
	}
	return false, nil
}

func (r memTxs) ClaimDue(_ context.Context, limit int, lease time.Duration) ([]DueTransaction, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []DueTransaction
	for id, s := range r.txs {
		if s.Status == domain.StatusPendingReference && len(out) < limit {
			out = append(out, DueTransaction{ID: id, WalletID: s.WalletID})
		}
	}
	return out, nil
}

func (r memTxs) WakeWaiting(context.Context, string, string) error { return nil }

type memLedger struct{ *memory }

func (r memLedger) Append(_ context.Context, e domain.LedgerEntry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ledger = append(r.ledger, e)
	return nil
}

func (r memLedger) Page(context.Context, uuid.UUID, int64, int) ([]domain.LedgerEntry, error) {
	return nil, nil
}

func (r memLedger) Totals(_ context.Context, walletID uuid.UUID) (LedgerTotals, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var t LedgerTotals
	for _, e := range r.ledger {
		if e.WalletID() != walletID {
			continue
		}
		t.Entries++
		if e.Direction() == domain.Credit {
			t.NetMinor += e.Amount().Minor()
		} else {
			t.NetMinor -= e.Amount().Minor()
		}
	}
	return t, nil
}

type memOutbox struct{ *memory }

func (r memOutbox) Append(_ context.Context, envs []domain.Envelope) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outbox = append(r.outbox, envs...)
	return nil
}

type memInbox struct{ *memory }

func (r memInbox) Begin(_ context.Context, consumer, id, hash string, _ time.Time) (InboxEntry, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.inbox[consumer+"/"+id]; ok {
		return e, false, nil
	}
	r.inbox[consumer+"/"+id] = InboxEntry{PayloadHash: hash}
	return InboxEntry{}, true, nil
}

func (r memInbox) Complete(_ context.Context, consumer, id string, txID uuid.UUID, _ time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e := r.inbox[consumer+"/"+id]
	e.TransactionID = txID
	r.inbox[consumer+"/"+id] = e
	return nil
}

type countingMetrics struct {
	mu     sync.Mutex
	counts map[string]int
}

func (m *countingMetrics) inc(k string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counts == nil {
		m.counts = map[string]int{}
	}
	m.counts[k]++
}

func (m *countingMetrics) get(k string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[k]
}

func (m *countingMetrics) TransactionResult(_ string, _ domain.Kind, s domain.Status, _ bool) {
	m.inc("result:" + string(s))
}
func (m *countingMetrics) Duplicate(_, reason string)               { m.inc("duplicate:" + reason) }
func (m *countingMetrics) Retry(c string)                           { m.inc("retry:" + c) }
func (m *countingMetrics) ConcurrencyConflict(r string)             { m.inc("conflict:" + r) }
func (m *countingMetrics) ProcessingDuration(string, time.Duration) {}
func (m *countingMetrics) ReconciliationDivergence()                { m.inc("divergence") }
func (m *countingMetrics) OutboxPublished()                         { m.inc("published") }
func (m *countingMetrics) OutboxPublishFailed()                     { m.inc("publish_failed") }
func (m *countingMetrics) OutboxBacklog(int64, time.Duration)       {}

var discard = slog.New(slog.NewTextHandler(io.Discard, nil))

func newServices(m *memory, metrics *countingMetrics) (*Wagering, *WalletService) {
	w := NewWagering(WageringDeps{
		Tx: m, Wallets: memWallets{m}, Transactions: memTxs{m}, Ledger: memLedger{m},
		Outbox: memOutbox{m}, Inbox: memInbox{m}, Metrics: metrics, Log: discard,
	}, WageringConfig{
		References: domain.ReferencePolicy{InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, TTL: time.Hour, MaxAttempts: 100},
		ClaimBatch: 10, ClaimLease: time.Second,
	})
	ws := NewWalletService(WalletDeps{
		Tx: m, Wallets: memWallets{m}, Transactions: memTxs{m}, Ledger: memLedger{m},
		Outbox: memOutbox{m}, Metrics: metrics, Log: discard,
	})
	return w, ws
}
