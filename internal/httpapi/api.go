package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"wallet/internal/app"
	"wallet/internal/auth"
	"wallet/internal/domain"
	"wallet/internal/domain/money"
	"wallet/internal/observability"
	"wallet/internal/observability/logctx"
)

type ReadinessCheck struct {
	Name  string
	Check func(context.Context) error
}

type API struct {
	wallets  *app.WalletService
	wagering *app.Wagering
	verifier *auth.Verifier
	metrics  *observability.Metrics
	log      *slog.Logger
	checks   []ReadinessCheck
	timeout  time.Duration
	draining atomic.Bool
}

func NewAPI(wallets *app.WalletService, wagering *app.Wagering, verifier *auth.Verifier,
	metrics *observability.Metrics, log *slog.Logger, checks []ReadinessCheck, timeout time.Duration) *API {
	return &API{wallets: wallets, wagering: wagering, verifier: verifier, metrics: metrics, log: log,
		checks: checks, timeout: timeout}
}

func (a *API) Drain() { a.draining.Store(true) }

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", a.live)
	mux.HandleFunc("GET /health/ready", a.ready)
	mux.Handle("GET /metrics", promhttp.HandlerFor(a.metrics.Registry, promhttp.HandlerOpts{}))

	mux.Handle("POST /wallets", a.require(auth.RoleInternal, a.openWallet))
	mux.Handle("GET /wallets/{walletId}", a.require(auth.RoleInternal, a.getWallet))
	mux.Handle("GET /wallets/{walletId}/ledger", a.require(auth.RoleInternal, a.ledger))
	mux.Handle("POST /wallets/{walletId}/reconciliation", a.require(auth.RoleInternal, a.reconcile))

	mux.Handle("POST /wagering/transactions", a.require(auth.RoleProvider, a.submit))
	mux.Handle("GET /wagering/transactions/{transactionId}", a.require("", a.getTransaction))
	mux.Handle("GET /providers/{providerId}/wagering/transactions/{externalTransactionId}", a.require("", a.getByExternalID))

	return a.observe(mux)
}

var correlationPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

func (a *API) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		corr := r.Header.Get("X-Correlation-Id")
		if !correlationPattern.MatchString(corr) {
			corr = app.NewV7().String()
		}
		w.Header().Set("X-Correlation-Id", corr)
		ctx, cancel := context.WithTimeout(r.Context(), a.timeout)
		defer cancel()
		ctx = logctx.With(ctx, "correlationId", corr)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}

		defer func() {
			if p := recover(); p != nil {
				a.log.ErrorContext(ctx, "panic in handler", "panic", p)
				writeError(rec, http.StatusInternalServerError, "INTERNAL_ERROR", "unexpected error")
			}
			route := r.Pattern
			if route == "" {
				route = "unmatched"
			}
			a.metrics.HTTPRequests.WithLabelValues(r.Method, route, strconv.Itoa(rec.status)).Inc()
			a.metrics.HTTPDuration.WithLabelValues(r.Method, route).Observe(time.Since(start).Seconds())
			if !strings.HasPrefix(r.URL.Path, "/health") && r.URL.Path != "/metrics" {
				a.log.InfoContext(ctx, "http request", "method", r.Method, "route", route,
					"status", rec.status, "durationMs", time.Since(start).Milliseconds())
			}
		}()
		next.ServeHTTP(rec, r.WithContext(ctx))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (a *API) require(role string, h http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || raw == "" {
			w.Header().Set("WWW-Authenticate", `Bearer realm="wallet"`)
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "missing bearer token")
			return
		}
		p, err := a.verifier.Verify(r.Context(), raw)
		if err != nil {
			a.log.InfoContext(r.Context(), "token rejected", "reason", err.Error())
			w.Header().Set("WWW-Authenticate", `Bearer realm="wallet", error="invalid_token"`)
			writeError(w, http.StatusUnauthorized, "INVALID_TOKEN", "token is invalid or expired")
			return
		}
		if role != "" && !p.Has(role) {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "caller lacks the "+role+" role")
			return
		}
		if role == "" && !p.Has(auth.RoleInternal) && !p.Has(auth.RoleProvider) {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "caller has no wallet role")
			return
		}
		if p.Has(auth.RoleProvider) && p.ProviderID == "" {
			writeError(w, http.StatusForbidden, "FORBIDDEN", "provider token without provider_id")
			return
		}
		ctx := logctx.With(r.Context(), "clientId", p.ClientID, "providerId", p.ProviderID)
		h(w, r.WithContext(auth.WithPrincipal(ctx, p)))
	})
}

func (a *API) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	if a.draining.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "draining"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	status, code := map[string]string{}, http.StatusOK
	for _, c := range a.checks {
		if err := c.Check(ctx); err != nil {
			status[c.Name] = "down"
			code = http.StatusServiceUnavailable
			a.log.WarnContext(ctx, "readiness check failed", "check", c.Name, "error", err)
			continue
		}
		status[c.Name] = "up"
	}
	writeJSON(w, code, status)
}

const maxBody = 64 << 10

func decode(r *http.Request, v any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		return err
	}
	if len(body) > maxBody {
		return errors.New("body too large")
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("unexpected data after JSON object")
	}
	return nil
}

func badJSON(w http.ResponseWriter, err error) {
	writeError(w, http.StatusBadRequest, "MALFORMED_REQUEST", "invalid JSON body: "+err.Error())
}

func pathUUID(w http.ResponseWriter, r *http.Request, name string) (uuid.UUID, bool) {
	v := r.PathValue(name)
	id, err := uuid.Parse(v)
	if err != nil || len(v) != 36 {
		writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", name+" must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

type moneyDTO struct {
	Amount   string `json:"amount"`
	Currency string `json:"currency"`
}

type openWalletRequest struct {
	PlayerID       string   `json:"playerId"`
	InitialBalance moneyDTO `json:"initialBalance"`
}

type walletResponse struct {
	ID        uuid.UUID        `json:"id"`
	PlayerID  uuid.UUID        `json:"playerId"`
	Balance   money.Money      `json:"balance"`
	Version   int64            `json:"version"`
	CreatedAt domain.Timestamp `json:"createdAt"`
	UpdatedAt domain.Timestamp `json:"updatedAt"`
}

func walletView(w *domain.Wallet) walletResponse {
	return walletResponse{w.ID(), w.PlayerID(), w.Balance(), w.Version(),
		domain.Timestamp(w.CreatedAt()), domain.Timestamp(w.UpdatedAt())}
}

func (a *API) openWallet(w http.ResponseWriter, r *http.Request) {
	var req openWalletRequest
	if err := decode(r, &req); err != nil {
		badJSON(w, err)
		return
	}
	verr := &domain.ValidationError{}
	playerID, err := uuid.Parse(req.PlayerID)
	if err != nil || len(req.PlayerID) != 36 || playerID == uuid.Nil {
		verr.Fields = append(verr.Fields, domain.FieldError{Field: "playerId", Reason: "must be a UUID"})
	}
	initial, err := money.Parse(req.InitialBalance.Amount, req.InitialBalance.Currency)
	if err != nil {
		verr.Fields = append(verr.Fields, domain.FieldError{Field: "initialBalance", Reason: err.Error()})
	}
	if len(verr.Fields) > 0 {
		a.fail(w, r, verr)
		return
	}
	wallet, err := a.wallets.Open(r.Context(), playerID, initial, w.Header().Get("X-Correlation-Id"))
	if err != nil {
		a.fail(w, r, err)
		return
	}
	a.log.InfoContext(logctx.With(r.Context(), "walletId", wallet.ID().String()), "wallet opened")
	writeJSON(w, http.StatusCreated, walletView(wallet))
}

func (a *API) getWallet(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	wallet, err := a.wallets.Get(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, walletView(wallet))
}

type ledgerEntryResponse struct {
	ID            uuid.UUID        `json:"id"`
	WalletID      uuid.UUID        `json:"walletId"`
	TransactionID uuid.UUID        `json:"transactionId"`
	WalletVersion int64            `json:"walletVersion"`
	Direction     domain.Direction `json:"direction"`
	Money         money.Money      `json:"money"`
	BalanceBefore money.Money      `json:"balanceBefore"`
	BalanceAfter  money.Money      `json:"balanceAfter"`
	CreatedAt     domain.Timestamp `json:"createdAt"`
}

type ledgerResponse struct {
	Items      []ledgerEntryResponse `json:"items"`
	NextCursor *string               `json:"nextCursor"`
}

func (a *API) ledger(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	limit := 50
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 200 {
			writeError(w, http.StatusBadRequest, "VALIDATION_ERROR", "limit must be between 1 and 200")
			return
		}
		limit = n
	}
	page, err := a.wallets.LedgerPage(r.Context(), id, r.URL.Query().Get("cursor"), limit)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	resp := ledgerResponse{Items: make([]ledgerEntryResponse, 0, len(page.Entries))}
	for _, e := range page.Entries {
		resp.Items = append(resp.Items, ledgerEntryResponse{e.ID(), e.WalletID(), e.TransactionID(), e.WalletVersion(),
			e.Direction(), e.Amount(), e.BalanceBefore(), e.BalanceAfter(), domain.Timestamp(e.CreatedAt())})
	}
	if page.NextCursor != "" {
		resp.NextCursor = &page.NextCursor
	}
	writeJSON(w, http.StatusOK, resp)
}

type reconciliationResponse struct {
	WalletID          uuid.UUID   `json:"walletId"`
	StoredBalance     money.Money `json:"storedBalance"`
	CalculatedBalance money.Money `json:"calculatedBalance"`
	Difference        money.Money `json:"difference"`
	Consistent        bool        `json:"consistent"`
	CheckedEntries    int64       `json:"checkedEntries"`
}

func (a *API) reconcile(w http.ResponseWriter, r *http.Request) {
	id, ok := pathUUID(w, r, "walletId")
	if !ok {
		return
	}
	rec, err := a.wallets.Reconcile(r.Context(), id)
	if err != nil {
		a.fail(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, reconciliationResponse{rec.WalletID, rec.StoredBalance, rec.CalculatedBalance,
		rec.Difference, rec.Consistent, rec.CheckedEntries})
}
