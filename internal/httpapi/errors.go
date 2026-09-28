package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"wallet/internal/app"
	"wallet/internal/domain"
)

type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string              `json:"code"`
	Message string              `json:"message"`
	Fields  []domain.FieldError `json:"fields,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, errorBody{Error: errorDetail{Code: code, Message: msg}})
}

func (a *API) fail(w http.ResponseWriter, r *http.Request, err error) {
	var verr *domain.ValidationError
	switch {
	case errors.As(err, &verr):
		writeJSON(w, http.StatusBadRequest, errorBody{Error: errorDetail{
			Code: "VALIDATION_ERROR", Message: "request has invalid fields", Fields: verr.Fields,
		}})
	case errors.Is(err, domain.ErrWalletPlayerMismatch):
		writeError(w, http.StatusBadRequest, "WALLET_PLAYER_MISMATCH", "wallet belongs to another player")
	case errors.Is(err, domain.ErrWalletCurrencyMismatch):
		writeError(w, http.StatusBadRequest, "CURRENCY_MISMATCH", "currency differs from wallet currency")
	case errors.Is(err, app.ErrInvalidCursor):
		writeError(w, http.StatusBadRequest, "INVALID_CURSOR", "cursor is not valid")
	case errors.Is(err, domain.ErrInvalid):
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", err.Error())
	case errors.Is(err, app.ErrWalletNotFound):
		writeError(w, http.StatusNotFound, "WALLET_NOT_FOUND", "wallet not found")
	case errors.Is(err, app.ErrNotFound):
		writeError(w, http.StatusNotFound, "NOT_FOUND", "resource not found")
	case errors.Is(err, app.ErrWalletExists):
		writeError(w, http.StatusConflict, "WALLET_ALREADY_EXISTS", "player already has a wallet in this currency")
	case errors.Is(err, app.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "IDEMPOTENCY_KEY_CONFLICT", "idempotency key was used with a different payload")
	case errors.Is(err, app.ErrExternalIDConflict):
		writeError(w, http.StatusConflict, "EXTERNAL_TRANSACTION_CONFLICT",
			"externalTransactionId was already submitted with another idempotency key")
	case app.Transient(err) || errors.Is(err, context.Canceled):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusServiceUnavailable, "TEMPORARILY_UNAVAILABLE", "try again later with the same idempotency key")
	default:
		a.log.ErrorContext(r.Context(), "unexpected error", "error", err)
		writeError(w, http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected error")
	}
	if app.Transient(err) {
		a.log.WarnContext(r.Context(), "transient failure", "error", err)
	}
}
