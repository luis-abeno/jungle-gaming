package domain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"

	"github.com/google/uuid"

	"wallet/internal/domain/money"
)

type WagerRequest struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       uuid.UUID
	WalletID                       uuid.UUID
	RoundID                        string
	GameID                         string
	Kind                           Kind
	Money                          money.Money
	ReferenceExternalTransactionID string
}

type WagerInput struct {
	ProviderID                     string
	ExternalTransactionID          string
	IdempotencyKey                 string
	PlayerID                       string
	WalletID                       string
	RoundID                        string
	GameID                         string
	Kind                           string
	Amount                         string
	Currency                       string
	ReferenceExternalTransactionID string
}

var providerPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func NewWagerRequest(in WagerInput) (WagerRequest, error) {
	verr := &ValidationError{}
	req := WagerRequest{
		ProviderID:                     in.ProviderID,
		ExternalTransactionID:          in.ExternalTransactionID,
		IdempotencyKey:                 in.IdempotencyKey,
		RoundID:                        in.RoundID,
		GameID:                         in.GameID,
		ReferenceExternalTransactionID: in.ReferenceExternalTransactionID,
	}

	if !providerPattern.MatchString(in.ProviderID) {
		verr.add("providerId", "must match "+providerPattern.String())
	}
	checkIdentifier(verr, "externalTransactionId", in.ExternalTransactionID, 128)
	checkIdentifier(verr, "roundId", in.RoundID, 128)
	checkIdentifier(verr, "gameId", in.GameID, 128)
	if reason := IdempotencyKeyProblem(in.IdempotencyKey); reason != "" {
		verr.add("idempotencyKey", reason)
	}
	req.PlayerID = parseUUID(verr, "playerId", in.PlayerID)
	req.WalletID = parseUUID(verr, "walletId", in.WalletID)

	kind, err := ParseKind(in.Kind)
	switch {
	case err != nil:
		verr.add("kind", "must be one of BET, WIN, LOSS, REFUND, ROLLBACK")
	case !kind.External():
		verr.add("kind", "OPENING is reserved for internal use")
	default:
		req.Kind = kind
	}

	m, err := money.Parse(in.Amount, in.Currency)
	switch {
	case errors.Is(err, money.ErrInvalidCurrency):
		verr.add("money.currency", "must be a supported ISO 4217 code")
	case errors.Is(err, money.ErrOverflow):
		verr.add("money.amount", "out of range")
	case err != nil:
		verr.add("money.amount", `must be a non negative decimal string with two places, like "25.00"`)
	default:
		req.Money = m
		switch {
		case req.Kind == KindLoss && !m.IsZero():
			verr.add("money.amount", `must be "0.00" for LOSS`)
		case req.Kind != "" && req.Kind != KindLoss && !m.IsPositive():
			verr.add("money.amount", "must be greater than zero")
		}
	}

	ref := in.ReferenceExternalTransactionID
	switch req.Kind {
	case KindRefund, KindRollback:
		if ref == "" {
			verr.add("referenceExternalTransactionId", "required for "+string(req.Kind))
		}
	case KindBet, KindLoss:
		if ref != "" {
			verr.add("referenceExternalTransactionId", "not allowed for "+string(req.Kind))
		}
	}
	if ref != "" {
		checkIdentifier(verr, "referenceExternalTransactionId", ref, 128)
		if ref == in.ExternalTransactionID {
			verr.add("referenceExternalTransactionId", "must differ from externalTransactionId")
		}
	}

	if err := verr.orNil(); err != nil {
		return WagerRequest{}, err
	}
	return req, nil
}

func checkIdentifier(verr *ValidationError, field, v string, max int) {
	switch {
	case v == "":
		verr.add(field, "required")
	case len(v) > max:
		verr.add(field, "too long")
	case strings.TrimSpace(v) != v || strings.ContainsFunc(v, func(r rune) bool { return r < 0x20 || r == 0x7f }):
		verr.add(field, "contains whitespace or control characters")
	}
}

func IdempotencyKeyProblem(k string) string {
	if k == "" {
		return "required"
	}
	if len(k) > 255 {
		return "too long"
	}
	for i := 0; i < len(k); i++ {
		if k[i] < 0x21 || k[i] > 0x7e {
			return "must be printable ASCII without spaces"
		}
	}
	return ""
}

func parseUUID(verr *ValidationError, field, v string) uuid.UUID {
	if len(v) != 36 {
		verr.add(field, "must be a UUID")
		return uuid.Nil
	}
	id, err := uuid.Parse(v)
	if err != nil || id == uuid.Nil {
		verr.add(field, "must be a UUID")
		return uuid.Nil
	}
	return id
}

func (r WagerRequest) PayloadHash() string {
	fields := map[string]any{
		"providerId":            r.ProviderID,
		"externalTransactionId": r.ExternalTransactionID,
		"playerId":              r.PlayerID.String(),
		"walletId":              r.WalletID.String(),
		"roundId":               r.RoundID,
		"gameId":                r.GameID,
		"kind":                  string(r.Kind),
		"money": map[string]string{
			"amount":   r.Money.Amount(),
			"currency": r.Money.Currency().String(),
		},
	}
	if r.ReferenceExternalTransactionID != "" {
		fields["referenceExternalTransactionId"] = r.ReferenceExternalTransactionID
	}
	return hashCanonical(fields)
}

func hashCanonical(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	sum := sha256.Sum256(bytes.TrimRight(buf.Bytes(), "\n"))
	return hex.EncodeToString(sum[:])
}

func CanonicalHash(v map[string]any) string { return hashCanonical(v) }
