package domain

import (
	"errors"
	"strings"
	"testing"
)

func validInput() WagerInput {
	return WagerInput{
		ProviderID:            "provider-a",
		ExternalTransactionID: "transaction-123",
		IdempotencyKey:        "provider-a:transaction-123",
		PlayerID:              "0192f28f-5dc0-7d58-bdb2-814ad6a0f4a1",
		WalletID:              "0192f291-27dd-7d3f-8071-5f8685deef37",
		RoundID:               "round-987",
		GameID:                "fortune-chimp",
		Kind:                  "BET",
		Amount:                "25.00",
		Currency:              "BRL",
	}
}

func TestZeroAmountPolicy(t *testing.T) {
	cases := []struct {
		kind   Kind
		amount string
		ref    string
		ok     bool
	}{
		{KindBet, "25.00", "", true},
		{KindBet, "0.00", "", false},
		{KindWin, "25.00", "", true},
		{KindWin, "0.00", "", false},
		{KindWin, "25.00", "bet-1", true},
		{KindLoss, "0.00", "", true},
		{KindLoss, "0.01", "", false},
		{KindRefund, "25.00", "bet-1", true},
		{KindRefund, "0.00", "bet-1", false},
		{KindRollback, "25.00", "bet-1", true},
		{KindRollback, "0.00", "bet-1", false},
	}
	for _, c := range cases {
		in := validInput()
		in.Kind, in.Amount, in.ReferenceExternalTransactionID = string(c.kind), c.amount, c.ref
		_, err := NewWagerRequest(in)
		if c.ok && err != nil {
			t.Errorf("%s %s: unexpected %v", c.kind, c.amount, err)
		}
		if !c.ok && !errors.Is(err, ErrInvalid) {
			t.Errorf("%s %s: expected validation error, got %v", c.kind, c.amount, err)
		}
	}
}

func TestRequestValidation(t *testing.T) {
	cases := map[string]func(*WagerInput){
		"opening kind":           func(in *WagerInput) { in.Kind = "OPENING" },
		"unknown kind":           func(in *WagerInput) { in.Kind = "bet" },
		"empty provider":         func(in *WagerInput) { in.ProviderID = "" },
		"bad provider":           func(in *WagerInput) { in.ProviderID = "provider a" },
		"missing external id":    func(in *WagerInput) { in.ExternalTransactionID = "" },
		"missing round":          func(in *WagerInput) { in.RoundID = "" },
		"missing game":           func(in *WagerInput) { in.GameID = "" },
		"missing key":            func(in *WagerInput) { in.IdempotencyKey = "" },
		"key with space":         func(in *WagerInput) { in.IdempotencyKey = "a b" },
		"bad player":             func(in *WagerInput) { in.PlayerID = "not-a-uuid" },
		"nil wallet":             func(in *WagerInput) { in.WalletID = "00000000-0000-0000-0000-000000000000" },
		"braced uuid":            func(in *WagerInput) { in.WalletID = "{0192f291-27dd-7d3f-8071-5f8685deef37}" },
		"negative amount":        func(in *WagerInput) { in.Amount = "-25.00" },
		"scientific":             func(in *WagerInput) { in.Amount = "2.5e1" },
		"excess scale":           func(in *WagerInput) { in.Amount = "25.001" },
		"empty amount":           func(in *WagerInput) { in.Amount = "" },
		"bad currency":           func(in *WagerInput) { in.Currency = "brl" },
		"refund without ref":     func(in *WagerInput) { in.Kind = "REFUND" },
		"rollback without ref":   func(in *WagerInput) { in.Kind = "ROLLBACK" },
		"bet with ref":           func(in *WagerInput) { in.ReferenceExternalTransactionID = "x" },
		"self reference":         func(in *WagerInput) { in.Kind = "REFUND"; in.ReferenceExternalTransactionID = in.ExternalTransactionID },
		"external id too long":   func(in *WagerInput) { in.ExternalTransactionID = strings.Repeat("x", 129) },
		"external id whitespace": func(in *WagerInput) { in.ExternalTransactionID = " tx" },
	}
	for name, mutate := range cases {
		in := validInput()
		mutate(&in)
		if _, err := NewWagerRequest(in); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: expected validation error, got %v", name, err)
		}
	}
}

func TestValidationCollectsFields(t *testing.T) {
	in := validInput()
	in.ProviderID = ""
	in.Amount = "x"
	_, err := NewWagerRequest(in)
	var verr *ValidationError
	if !errors.As(err, &verr) || len(verr.Fields) != 2 {
		t.Fatalf("got %v", err)
	}
}

func TestPayloadHash(t *testing.T) {
	req, err := NewWagerRequest(validInput())
	if err != nil {
		t.Fatal(err)
	}
	h := req.PayloadHash()
	if len(h) != 64 {
		t.Fatalf("hash: %s", h)
	}

	in := validInput()
	in.IdempotencyKey = "another-key"
	other, _ := NewWagerRequest(in)
	if other.PayloadHash() != h {
		t.Fatal("idempotency key must not affect the hash")
	}

	in = validInput()
	in.PlayerID = strings.ToUpper(in.PlayerID)
	upper, _ := NewWagerRequest(in)
	if upper.PayloadHash() != h {
		t.Fatal("uuid case must not affect the hash")
	}

	changes := []func(*WagerInput){
		func(in *WagerInput) { in.Amount = "25.01" },
		func(in *WagerInput) { in.RoundID = "round-988" },
		func(in *WagerInput) { in.Kind = "WIN" },
		func(in *WagerInput) { in.Currency = "USD" },
		func(in *WagerInput) { in.Kind = "WIN"; in.ReferenceExternalTransactionID = "bet-1" },
	}
	for i, change := range changes {
		in := validInput()
		change(&in)
		r, err := NewWagerRequest(in)
		if err != nil {
			t.Fatal(err)
		}
		if r.PayloadHash() == h {
			t.Errorf("change %d kept the hash", i)
		}
	}
}

func TestCanonicalEncodingIsStable(t *testing.T) {
	got := CanonicalHash(map[string]any{"b": "2", "a": map[string]string{"y": "<", "x": "1"}})
	want := CanonicalHash(map[string]any{"a": map[string]string{"x": "1", "y": "<"}, "b": "2"})
	if got != want {
		t.Fatal("key order must not matter")
	}
}
