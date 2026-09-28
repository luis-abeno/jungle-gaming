package domain

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestEnvelopeFormat(t *testing.T) {
	w := openWallet(t, "100.00")
	_, err := w.Debit(uuid.New(), uuid.New(), brl(t, "25.00"), time.Date(2026, 9, 8, 9, 0, 0, 123456000, time.FixedZone("BRT", -3*3600)))
	if err != nil {
		t.Fatal(err)
	}
	ev := w.PullEvents()[0]
	env := NewEnvelope(uuid.New(), ev, "corr-1", "msg-1")
	if env.EventType != "WalletBalanceChanged" || env.Version != 1 || env.AggregateID != w.ID() {
		t.Fatalf("envelope: %+v", env)
	}

	b, err := json.Marshal(env)
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"eventId", "eventType", "aggregateId", "correlationId", "causationId", "occurredAt", "version", "data"} {
		if _, ok := raw[k]; !ok {
			t.Errorf("missing %s", k)
		}
	}
	if raw["occurredAt"] != "2026-09-08T12:00:00.123Z" {
		t.Errorf("occurredAt must be UTC RFC 3339: %v", raw["occurredAt"])
	}
	data := raw["data"].(map[string]any)
	for _, k := range []string{"walletId", "transactionId", "direction", "money", "balanceBefore", "balanceAfter", "walletVersion"} {
		if _, ok := data[k]; !ok {
			t.Errorf("data missing %s", k)
		}
	}
	if !strings.Contains(string(b), `"balanceAfter":{"amount":"75.00","currency":"BRL"}`) {
		t.Errorf("money must be a decimal string: %s", b)
	}
}

func TestOpeningEventOmitsExternalFields(t *testing.T) {
	o, err := OpenWallet(OpeningIDs{uuid.New(), uuid.New(), uuid.New()}, playerID, brl(t, "10.00"), t0)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(NewEnvelope(uuid.New(), o.Transaction.PullEvents()[0], "corr", ""))
	for _, k := range []string{"providerId", "externalTransactionId", "roundId", "gameId", "causationId"} {
		if strings.Contains(string(b), `"`+k+`"`) {
			t.Errorf("opening event should not carry %s: %s", k, b)
		}
	}
}
