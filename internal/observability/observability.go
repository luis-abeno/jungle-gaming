package observability

import (
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"wallet/internal/domain"
	"wallet/internal/observability/logctx"
)

func NewLogger(level string, w io.Writer) *slog.Logger {
	var l slog.Level
	if err := l.UnmarshalText([]byte(strings.ToUpper(level))); err != nil {
		l = slog.LevelInfo
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: l})
	return slog.New(logctx.Handler{Handler: h})
}

type Metrics struct {
	Registry *prometheus.Registry

	transactions *prometheus.CounterVec
	duplicates   *prometheus.CounterVec
	retries      *prometheus.CounterVec
	conflicts    *prometheus.CounterVec
	duration     *prometheus.HistogramVec
	divergences  prometheus.Counter

	Messages     *prometheus.CounterVec
	DeadLettered *prometheus.CounterVec

	outboxPublished prometheus.Counter
	outboxFailures  prometheus.Counter
	outboxPending   prometheus.Gauge
	outboxLag       prometheus.Gauge

	HTTPRequests *prometheus.CounterVec
	HTTPDuration *prometheus.HistogramVec
}

func NewMetrics() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		transactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_transactions_total", Help: "Operation results by source, kind and status.",
		}, []string{"source", "kind", "status", "replay"}),
		duplicates: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wager_duplicates_total", Help: "Repeated deliveries detected by idempotency key or inbox.",
		}, []string{"source", "reason"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_retries_total", Help: "Retries by component.",
		}, []string{"component"}),
		conflicts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "wallet_concurrency_conflicts_total", Help: "Lost races and lock timeouts.",
		}, []string{"reason"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "wager_processing_seconds", Help: "Time to process an operation.",
			Buckets: []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
		}, []string{"source"}),
		divergences: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "wallet_reconciliation_divergences_total", Help: "Reconciliations where stored balance differs from the ledger.",
		}),
		Messages: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sqs_messages_total", Help: "Consumed messages by outcome.",
		}, []string{"outcome"}),
		DeadLettered: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sqs_dead_lettered_total", Help: "Messages moved to the DLQ by the consumer.",
		}, []string{"reason"}),
		outboxPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_published_total", Help: "Events sent to the broker.",
		}),
		outboxFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "outbox_publish_failures_total", Help: "Failed publish attempts.",
		}),
		outboxPending: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_pending_events", Help: "Events not yet published.",
		}),
		outboxLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "outbox_lag_seconds", Help: "Age of the oldest unpublished event.",
		}),
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total", Help: "HTTP requests by route and status.",
		}, []string{"method", "route", "code"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "http_request_seconds", Help: "HTTP latency by route.", Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
	}
	m.Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.transactions, m.duplicates, m.retries, m.conflicts, m.duration, m.divergences,
		m.Messages, m.DeadLettered,
		m.outboxPublished, m.outboxFailures, m.outboxPending, m.outboxLag,
		m.HTTPRequests, m.HTTPDuration,
	)
	return m
}

func (m *Metrics) TransactionResult(source string, kind domain.Kind, status domain.Status, replay bool) {
	m.transactions.WithLabelValues(source, string(kind), string(status), strconv.FormatBool(replay)).Inc()
}

func (m *Metrics) Duplicate(source, reason string) {
	m.duplicates.WithLabelValues(source, reason).Inc()
}
func (m *Metrics) Retry(component string) { m.retries.WithLabelValues(component).Inc() }
func (m *Metrics) ConcurrencyConflict(reason string) {
	m.conflicts.WithLabelValues(reason).Inc()
}
func (m *Metrics) ProcessingDuration(source string, d time.Duration) {
	m.duration.WithLabelValues(source).Observe(d.Seconds())
}
func (m *Metrics) ReconciliationDivergence() { m.divergences.Inc() }

func (m *Metrics) OutboxPublished()     { m.outboxPublished.Inc() }
func (m *Metrics) OutboxPublishFailed() { m.outboxFailures.Inc() }

func (m *Metrics) OutboxBacklog(pending int64, lag time.Duration) {
	m.outboxPending.Set(float64(pending))
	m.outboxLag.Set(lag.Seconds())
}
