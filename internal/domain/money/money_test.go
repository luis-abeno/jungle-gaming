package money

import (
	"encoding/json"
	"errors"
	"math"
	"testing"
)

func mustParse(t *testing.T, amount, cur string) Money {
	t.Helper()
	m, err := Parse(amount, cur)
	if err != nil {
		t.Fatalf("parse %q: %v", amount, err)
	}
	return m
}

func TestParseValid(t *testing.T) {
	cases := map[string]int64{
		"0.00":                 0,
		"0.01":                 1,
		"25.00":                2500,
		"1000.50":              100050,
		"92233720368547758.07": math.MaxInt64,
	}
	for in, want := range cases {
		m := mustParse(t, in, "BRL")
		if m.Minor() != want {
			t.Errorf("%s: got %d want %d", in, m.Minor(), want)
		}
		if m.Amount() != in {
			t.Errorf("%s: round trip gave %s", in, m.Amount())
		}
	}
}

func TestParseRejects(t *testing.T) {
	cases := []struct {
		in  string
		err error
	}{
		{"", ErrInvalidAmount},
		{"25", ErrInvalidAmount},
		{"25.0", ErrInvalidAmount},
		{"25.000", ErrInvalidAmount},
		{"-1.00", ErrInvalidAmount},
		{"+1.00", ErrInvalidAmount},
		{"1e2", ErrInvalidAmount},
		{"1.5e2", ErrInvalidAmount},
		{"1.00e0", ErrInvalidAmount},
		{"NaN", ErrInvalidAmount},
		{"Infinity", ErrInvalidAmount},
		{"-Infinity", ErrInvalidAmount},
		{".50", ErrInvalidAmount},
		{"01.00", ErrInvalidAmount},
		{" 1.00", ErrInvalidAmount},
		{"1.00 ", ErrInvalidAmount},
		{"1,00", ErrInvalidAmount},
		{"0x10.00", ErrInvalidAmount},
		{"92233720368547758.08", ErrOverflow},
		{"99999999999999999999.99", ErrOverflow},
	}
	for _, c := range cases {
		if _, err := Parse(c.in, "BRL"); !errors.Is(err, c.err) {
			t.Errorf("%q: got %v want %v", c.in, err, c.err)
		}
	}
}

func TestParseCurrency(t *testing.T) {
	for _, c := range []string{"", "brl", "BR", "BRLL", "XXX", "JPY"} {
		if _, err := Parse("1.00", c); !errors.Is(err, ErrInvalidCurrency) {
			t.Errorf("%q: expected invalid currency, got %v", c, err)
		}
	}
}

func TestArithmetic(t *testing.T) {
	a := mustParse(t, "100.00", "BRL")
	b := mustParse(t, "80.00", "BRL")

	sum, err := a.Add(b)
	if err != nil || sum.Amount() != "180.00" {
		t.Fatalf("add: %v %v", sum, err)
	}
	diff, err := b.Sub(a)
	if err != nil || diff.Amount() != "-20.00" || !diff.IsNegative() {
		t.Fatalf("sub: %v %v", diff, err)
	}
	neg, err := a.Neg()
	if err != nil || neg.Amount() != "-100.00" {
		t.Fatalf("neg: %v %v", neg, err)
	}
	if c, _ := a.Cmp(b); c != 1 {
		t.Fatalf("cmp: %d", c)
	}
	if c, _ := b.Cmp(a); c != -1 {
		t.Fatalf("cmp: %d", c)
	}
	if c, _ := a.Cmp(a); c != 0 {
		t.Fatalf("cmp: %d", c)
	}
	if a.Minor() != 10000 {
		t.Fatal("operands must stay unchanged")
	}
}

func TestOverflow(t *testing.T) {
	max, _ := FromMinor(math.MaxInt64, "BRL")
	min, _ := FromMinor(math.MinInt64, "BRL")
	one, _ := FromMinor(1, "BRL")

	if _, err := max.Add(one); !errors.Is(err, ErrOverflow) {
		t.Errorf("add overflow: %v", err)
	}
	if _, err := min.Sub(one); !errors.Is(err, ErrOverflow) {
		t.Errorf("sub overflow: %v", err)
	}
	if _, err := one.Sub(min); !errors.Is(err, ErrOverflow) {
		t.Errorf("sub of min overflow: %v", err)
	}
	if _, err := min.Neg(); !errors.Is(err, ErrOverflow) {
		t.Errorf("neg overflow: %v", err)
	}
	if min.Amount() != "-92233720368547758.08" {
		t.Errorf("min amount: %s", min.Amount())
	}
}

func TestCurrencyMismatch(t *testing.T) {
	brl := mustParse(t, "1.00", "BRL")
	usd := mustParse(t, "1.00", "USD")
	if _, err := brl.Add(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("add: %v", err)
	}
	if _, err := brl.Sub(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("sub: %v", err)
	}
	if _, err := brl.Cmp(usd); !errors.Is(err, ErrCurrencyMismatch) {
		t.Errorf("cmp: %v", err)
	}
	if brl.Equal(usd) {
		t.Error("different currencies must not be equal")
	}
}

func TestUninitialized(t *testing.T) {
	var zero Money
	one := mustParse(t, "1.00", "BRL")
	if _, err := zero.Add(one); !errors.Is(err, ErrUninitialized) {
		t.Errorf("add: %v", err)
	}
	if _, err := one.Sub(zero); !errors.Is(err, ErrUninitialized) {
		t.Errorf("sub: %v", err)
	}
	if _, err := zero.Neg(); !errors.Is(err, ErrUninitialized) {
		t.Errorf("neg: %v", err)
	}
	if _, err := json.Marshal(zero); err == nil {
		t.Error("marshal of zero value must fail")
	}
}

func TestZero(t *testing.T) {
	z, err := Zero("BRL")
	if err != nil || !z.IsZero() || z.Amount() != "0.00" || z.Currency() != "BRL" {
		t.Fatalf("zero: %v %v", z, err)
	}
}

func TestJSON(t *testing.T) {
	m := mustParse(t, "25.00", "BRL")
	b, err := json.Marshal(m)
	if err != nil || string(b) != `{"amount":"25.00","currency":"BRL"}` {
		t.Fatalf("marshal: %s %v", b, err)
	}
	var back Money
	if err := json.Unmarshal(b, &back); err != nil || !back.Equal(m) {
		t.Fatalf("unmarshal: %v %v", back, err)
	}

	bad := []string{
		`{"amount":25.00,"currency":"BRL"}`,
		`{"amount":"25","currency":"BRL"}`,
		`{"amount":"25.00"}`,
		`{"amount":"25.00","currency":"BRL","extra":1}`,
		`{"amount":"-1.00","currency":"BRL"}`,
		`"25.00"`,
	}
	for _, in := range bad {
		var v Money
		if err := json.Unmarshal([]byte(in), &v); err == nil {
			t.Errorf("%s: expected error", in)
		}
	}
}
