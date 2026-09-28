package httpapi

import (
	"net/http"

	"github.com/google/uuid"

	"wallet/internal/app"
	"wallet/internal/auth"
	"wallet/internal/domain"
	"wallet/internal/domain/money"
	"wallet/internal/observability/logctx"
)

type submitRequest struct {
	ProviderID                     string   `json:"providerId"`
	ExternalTransactionID          string   `json:"externalTransactionId"`
	PlayerID                       string   `json:"playerId"`
	WalletID                       string   `json:"walletId"`
	RoundID                        string   `json:"roundId"`
	GameID                         string   `json:"gameId"`
	Kind                           string   `json:"kind"`
	Money                          moneyDTO `json:"money"`
	ReferenceExternalTransactionID string   `json:"referenceExternalTransactionId,omitempty"`
}

type submitResponse struct {
	TransactionID    uuid.UUID          `json:"transactionId"`
	Status           domain.Status      `json:"status"`
	Balance          *money.Money       `json:"balance,omitempty"`
	FailureCode      domain.FailureCode `json:"failureCode,omitempty"`
	NextAttemptAt    *domain.Timestamp  `json:"nextAttemptAt,omitempty"`
	RetryDeadline    *domain.Timestamp  `json:"retryDeadline,omitempty"`
	IdempotentReplay bool               `json:"idempotentReplay"`
}

func submitStatus(s domain.Status) int {
	switch s {
	case domain.StatusProcessed:
		return http.StatusOK
	case domain.StatusRejected:
		return http.StatusUnprocessableEntity
	case domain.StatusFailed:
		return http.StatusInternalServerError
	}
	return http.StatusAccepted
}

func (a *API) submit(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	key := r.Header.Get("Idempotency-Key")
	if reason := domain.IdempotencyKeyProblem(key); reason != "" {
		writeError(w, http.StatusBadRequest, "INVALID_IDEMPOTENCY_KEY", "Idempotency-Key header "+reason)
		return
	}
	var body submitRequest
	if err := decode(r, &body); err != nil {
		badJSON(w, err)
		return
	}
	if body.ProviderID != p.ProviderID {
		writeError(w, http.StatusForbidden, "PROVIDER_MISMATCH", "providerId does not match the authenticated provider")
		return
	}
	req, err := domain.NewWagerRequest(domain.WagerInput{
		ProviderID:                     p.ProviderID,
		ExternalTransactionID:          body.ExternalTransactionID,
		IdempotencyKey:                 key,
		PlayerID:                       body.PlayerID,
		WalletID:                       body.WalletID,
		RoundID:                        body.RoundID,
		GameID:                         body.GameID,
		Kind:                           body.Kind,
		Amount:                         body.Money.Amount,
		Currency:                       body.Money.Currency,
		ReferenceExternalTransactionID: body.ReferenceExternalTransactionID,
	})
	if err != nil {
		a.fail(w, r, err)
		return
	}
	ctx := logctx.With(r.Context(), "walletId", req.WalletID.String(), "externalTransactionId", req.ExternalTransactionID)
	corr := w.Header().Get("X-Correlation-Id")
	out, err := a.wagering.Submit(ctx, app.Command{Request: req, CorrelationID: corr, CausationID: corr, Source: "http"})
	if err != nil {
		a.fail(w, r.WithContext(ctx), err)
		return
	}
	t := out.Transaction
	a.log.InfoContext(logctx.With(ctx, "transactionId", t.ID().String()), "operation handled",
		"kind", t.Kind(), "status", t.Status(), "failureCode", t.FailureCode(), "idempotentReplay", out.Replay)
	writeJSON(w, submitStatus(t.Status()), submitView(out))
}

func submitView(out app.Outcome) submitResponse {
	t := out.Transaction
	resp := submitResponse{
		TransactionID:    t.ID(),
		Status:           t.Status(),
		FailureCode:      t.FailureCode(),
		IdempotentReplay: out.Replay,
	}
	if b := t.BalanceAfter(); b.Valid() {
		resp.Balance = &b
	}
	if t.Status() == domain.StatusPendingReference {
		next, deadline := domain.Timestamp(t.NextAttemptAt()), domain.Timestamp(t.ReferenceDeadline())
		resp.NextAttemptAt, resp.RetryDeadline = &next, &deadline
	}
	return resp
}

type transactionResponse struct {
	TransactionID                  uuid.UUID          `json:"transactionId"`
	Origin                         domain.Origin      `json:"origin"`
	Kind                           domain.Kind        `json:"kind"`
	Status                         domain.Status      `json:"status"`
	WalletID                       uuid.UUID          `json:"walletId"`
	PlayerID                       uuid.UUID          `json:"playerId"`
	ProviderID                     string             `json:"providerId,omitempty"`
	ExternalTransactionID          string             `json:"externalTransactionId,omitempty"`
	RoundID                        string             `json:"roundId,omitempty"`
	GameID                         string             `json:"gameId,omitempty"`
	Money                          money.Money        `json:"money"`
	ReferenceExternalTransactionID string             `json:"referenceExternalTransactionId,omitempty"`
	ReferenceTransactionID         *uuid.UUID         `json:"referenceTransactionId,omitempty"`
	FailureCode                    domain.FailureCode `json:"failureCode,omitempty"`
	Balance                        *money.Money       `json:"balance,omitempty"`
	Attempts                       int                `json:"attempts"`
	NextAttemptAt                  *domain.Timestamp  `json:"nextAttemptAt,omitempty"`
	RetryDeadline                  *domain.Timestamp  `json:"retryDeadline,omitempty"`
	CreatedAt                      domain.Timestamp   `json:"createdAt"`
	UpdatedAt                      domain.Timestamp   `json:"updatedAt"`
	CompletedAt                    *domain.Timestamp  `json:"completedAt,omitempty"`
}

func transactionView(t *domain.WagerTransaction) transactionResponse {
	s := t.Snapshot()
	resp := transactionResponse{
		TransactionID:                  s.ID,
		Origin:                         s.Origin,
		Kind:                           s.Kind,
		Status:                         s.Status,
		WalletID:                       s.WalletID,
		PlayerID:                       s.PlayerID,
		ProviderID:                     s.ProviderID,
		ExternalTransactionID:          s.ExternalTransactionID,
		RoundID:                        s.RoundID,
		GameID:                         s.GameID,
		Money:                          s.Money,
		ReferenceExternalTransactionID: s.ReferenceExternalTransactionID,
		FailureCode:                    s.FailureCode,
		Attempts:                       s.Attempts,
		CreatedAt:                      domain.Timestamp(s.CreatedAt),
		UpdatedAt:                      domain.Timestamp(s.UpdatedAt),
	}
	if s.ReferenceTransactionID != uuid.Nil {
		resp.ReferenceTransactionID = &s.ReferenceTransactionID
	}
	if s.BalanceAfter.Valid() {
		resp.Balance = &s.BalanceAfter
	}
	if !s.NextAttemptAt.IsZero() {
		v := domain.Timestamp(s.NextAttemptAt)
		resp.NextAttemptAt = &v
	}
	if !s.ReferenceDeadline.IsZero() {
		v := domain.Timestamp(s.ReferenceDeadline)
		resp.RetryDeadline = &v
	}
	if !s.CompletedAt.IsZero() {
		v := domain.Timestamp(s.CompletedAt)
		resp.CompletedAt = &v
	}
	return resp
}

func (a *API) getTransaction(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "transactionId")
	if !ok {
		return
	}
	p, _ := auth.FromContext(r.Context())
	t, err := a.wagering.GetTransaction(r.Context(), id)
	if err == nil && !p.Has(auth.RoleInternal) && t.ProviderID() != p.ProviderID {
		err = app.ErrNotFound
	}
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, transactionView(t))
}

func (a *API) getByExternalID(w http.ResponseWriter, r *http.Request) {
	p, _ := auth.FromContext(r.Context())
	providerID := r.PathValue("providerId")
	if !p.Has(auth.RoleInternal) && providerID != p.ProviderID {
		writeError(w, http.StatusForbidden, "PROVIDER_MISMATCH", "providers can only read their own transactions")
		return
	}
	t, err := a.wagering.GetByExternalID(r.Context(), providerID, r.PathValue("externalTransactionId"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, transactionView(t))
}
