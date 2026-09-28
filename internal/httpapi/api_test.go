package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"wallet/internal/app"
	"wallet/internal/domain"
)

func TestErrorContract(t *testing.T) {
	a := &API{log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	_, verr := domain.NewWagerRequest(domain.WagerInput{})
	cases := []struct {
		err    error
		status int
		code   string
	}{
		{verr, 400, "VALIDATION_ERROR"},
		{domain.ErrWalletPlayerMismatch, 400, "WALLET_PLAYER_MISMATCH"},
		{domain.ErrWalletCurrencyMismatch, 400, "CURRENCY_MISMATCH"},
		{app.ErrInvalidCursor, 400, "INVALID_CURSOR"},
		{app.ErrWalletNotFound, 404, "WALLET_NOT_FOUND"},
		{app.ErrNotFound, 404, "NOT_FOUND"},
		{app.ErrWalletExists, 409, "WALLET_ALREADY_EXISTS"},
		{app.ErrIdempotencyConflict, 409, "IDEMPOTENCY_KEY_CONFLICT"},
		{app.ErrExternalIDConflict, 409, "EXTERNAL_TRANSACTION_CONFLICT"},
		{fmt.Errorf("%w: db down", app.ErrUnavailable), 503, "TEMPORARILY_UNAVAILABLE"},
		{app.ErrLockTimeout, 503, "TEMPORARILY_UNAVAILABLE"},
		{errors.New("boom"), 500, "INTERNAL_ERROR"},
	}
	for _, c := range cases {
		rec := httptest.NewRecorder()
		a.fail(rec, httptest.NewRequest("GET", "/", nil), c.err)
		var body errorBody
		json.Unmarshal(rec.Body.Bytes(), &body)
		if rec.Code != c.status || body.Error.Code != c.code {
			t.Errorf("%v: got %d %s", c.err, rec.Code, body.Error.Code)
		}
	}
	rec := httptest.NewRecorder()
	a.fail(rec, httptest.NewRequest("GET", "/", nil), app.ErrLockTimeout)
	if rec.Header().Get("Retry-After") == "" {
		t.Error("503 must carry Retry-After")
	}
}

func TestSubmitStatus(t *testing.T) {
	want := map[domain.Status]int{
		domain.StatusProcessed:        http.StatusOK,
		domain.StatusRejected:         http.StatusUnprocessableEntity,
		domain.StatusPendingReference: http.StatusAccepted,
		domain.StatusFailed:           http.StatusInternalServerError,
	}
	for s, code := range want {
		if submitStatus(s) != code {
			t.Errorf("%s: %d", s, submitStatus(s))
		}
	}
}

func TestDecodeIsStrict(t *testing.T) {
	bad := []string{
		`{"money":{"amount":25.00,"currency":"BRL"}}`,
		`{"unknown":1}`,
		`{"kind":"BET"} {"kind":"BET"}`,
		`[]`,
		strings.Repeat(" ", maxBody+1),
	}
	for _, b := range bad {
		var v submitRequest
		if err := decode(httptest.NewRequest("POST", "/", strings.NewReader(b)), &v); err == nil {
			t.Errorf("%.40q accepted", b)
		}
	}
	var v submitRequest
	if err := decode(httptest.NewRequest("POST", "/", strings.NewReader(`{"kind":"BET","money":{"amount":"1.00","currency":"BRL"}}`)), &v); err != nil {
		t.Fatal(err)
	}
}
