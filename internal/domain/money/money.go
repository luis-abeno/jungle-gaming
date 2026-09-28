package money

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

const Scale = 2

var (
	ErrInvalidAmount    = errors.New("money: invalid amount")
	ErrInvalidCurrency  = errors.New("money: invalid currency")
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	ErrOverflow         = errors.New("money: overflow")
	ErrUninitialized    = errors.New("money: uninitialized value")
)

type Currency string

var supported = map[Currency]struct{}{
	"BRL": {}, "USD": {}, "EUR": {}, "GBP": {}, "ARS": {}, "MXN": {}, "CAD": {}, "AUD": {},
}

func ParseCurrency(s string) (Currency, error) {
	c := Currency(s)
	if _, ok := supported[c]; !ok {
		return "", fmt.Errorf("%w: %q", ErrInvalidCurrency, s)
	}
	return c, nil
}

func (c Currency) String() string { return string(c) }

type Money struct {
	minor    int64
	currency Currency
}

func Parse(amount, currency string) (Money, error) {
	cur, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	intPart, frac, ok := strings.Cut(amount, ".")
	if !ok || len(frac) != Scale || intPart == "" || !digits(intPart) || !digits(frac) {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidAmount, amount)
	}
	if len(intPart) > 1 && intPart[0] == '0' {
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidAmount, amount)
	}
	minor, err := strconv.ParseInt(intPart+frac, 10, 64)
	if err != nil {
		if errors.Is(err, strconv.ErrRange) {
			return Money{}, fmt.Errorf("%w: %q", ErrOverflow, amount)
		}
		return Money{}, fmt.Errorf("%w: %q", ErrInvalidAmount, amount)
	}
	return Money{minor: minor, currency: cur}, nil
}

func FromMinor(minor int64, currency string) (Money, error) {
	cur, err := ParseCurrency(currency)
	if err != nil {
		return Money{}, err
	}
	return Money{minor: minor, currency: cur}, nil
}

func Zero(currency string) (Money, error) { return FromMinor(0, currency) }

func digits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}

func (m Money) Minor() int64       { return m.minor }
func (m Money) Currency() Currency { return m.currency }
func (m Money) Valid() bool        { return m.currency != "" }
func (m Money) IsZero() bool       { return m.minor == 0 }
func (m Money) IsPositive() bool   { return m.minor > 0 }
func (m Money) IsNegative() bool   { return m.minor < 0 }

func (m Money) compatible(o Money) error {
	if !m.Valid() || !o.Valid() {
		return ErrUninitialized
	}
	if m.currency != o.currency {
		return fmt.Errorf("%w: %s and %s", ErrCurrencyMismatch, m.currency, o.currency)
	}
	return nil
}

func (m Money) Add(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor > 0 && m.minor > math.MaxInt64-o.minor) || (o.minor < 0 && m.minor < math.MinInt64-o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor + o.minor, currency: m.currency}, nil
}

func (m Money) Sub(o Money) (Money, error) {
	if err := m.compatible(o); err != nil {
		return Money{}, err
	}
	if (o.minor < 0 && m.minor > math.MaxInt64+o.minor) || (o.minor > 0 && m.minor < math.MinInt64+o.minor) {
		return Money{}, ErrOverflow
	}
	return Money{minor: m.minor - o.minor, currency: m.currency}, nil
}

func (m Money) Neg() (Money, error) {
	if !m.Valid() {
		return Money{}, ErrUninitialized
	}
	if m.minor == math.MinInt64 {
		return Money{}, ErrOverflow
	}
	return Money{minor: -m.minor, currency: m.currency}, nil
}

func (m Money) Cmp(o Money) (int, error) {
	if err := m.compatible(o); err != nil {
		return 0, err
	}
	switch {
	case m.minor < o.minor:
		return -1, nil
	case m.minor > o.minor:
		return 1, nil
	}
	return 0, nil
}

func (m Money) Equal(o Money) bool { return m == o }

func (m Money) Amount() string {
	sign := ""
	u := uint64(m.minor)
	if m.minor < 0 {
		sign = "-"
		u = uint64(-(m.minor + 1)) + 1
	}
	return fmt.Sprintf("%s%d.%02d", sign, u/100, u%100)
}

func (m Money) String() string { return m.Amount() + " " + string(m.currency) }

type wire struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

func (m Money) MarshalJSON() ([]byte, error) {
	if !m.Valid() {
		return nil, ErrUninitialized
	}
	return json.Marshal(wire{Amount: m.Amount(), Currency: string(m.currency)})
}

func (m *Money) UnmarshalJSON(b []byte) error {
	var w wire
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&w); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidAmount, err)
	}
	v, err := Parse(w.Amount, w.Currency)
	if err != nil {
		return err
	}
	*m = v
	return nil
}
