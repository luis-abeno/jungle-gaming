package domain

import (
	"testing"
	"time"

	"github.com/google/uuid"

	"wallet/internal/domain/money"
)

var (
	t0       = time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	playerID = uuid.MustParse("0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1")
	policy   = ReferencePolicy{InitialBackoff: time.Second, MaxBackoff: 8 * time.Second, TTL: time.Minute, MaxAttempts: 5}
)

func brl(t *testing.T, amount string) money.Money {
	t.Helper()
	m, err := money.Parse(amount, "BRL")
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func openWallet(t *testing.T, amount string) *Wallet {
	t.Helper()
	o, err := OpenWallet(OpeningIDs{uuid.New(), uuid.New(), uuid.New()}, playerID, brl(t, amount), t0)
	if err != nil {
		t.Fatal(err)
	}
	o.Wallet.PullEvents()
	return o.Wallet
}

func input(w *Wallet, ext string, kind Kind, amount, ref string) WagerInput {
	return WagerInput{
		ProviderID:                     "provider-a",
		ExternalTransactionID:          ext,
		IdempotencyKey:                 "provider-a:" + ext,
		PlayerID:                       w.PlayerID().String(),
		WalletID:                       w.ID().String(),
		RoundID:                        "round-1",
		GameID:                         "fortune-chimp",
		Kind:                           string(kind),
		Amount:                         amount,
		Currency:                       "BRL",
		ReferenceExternalTransactionID: ref,
	}
}

func newTx(t *testing.T, in WagerInput) *WagerTransaction {
	t.Helper()
	req, err := NewWagerRequest(in)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := NewExternalTransaction(uuid.New(), req, "corr-1", t0)
	if err != nil {
		t.Fatal(err)
	}
	return tx
}

func settled(t *testing.T, w *Wallet, in WagerInput, ref *Reference) *WagerTransaction {
	t.Helper()
	tx := newTx(t, in)
	if _, err := Settle(tx, w, ref, uuid.New(), policy, t0); err != nil {
		t.Fatal(err)
	}
	if tx.Status() != StatusProcessed {
		t.Fatalf("%s: expected PROCESSED, got %s %s", in.Kind, tx.Status(), tx.FailureCode())
	}
	tx.PullEvents()
	w.PullEvents()
	return tx
}

func eventTypes(events []Event) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.EventType()
	}
	return out
}
