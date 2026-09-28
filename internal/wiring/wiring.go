package wiring

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	"wallet/internal/app"
	"wallet/internal/auth"
	"wallet/internal/config"
	"wallet/internal/domain"
	"wallet/internal/fault"
	"wallet/internal/httpapi"
	"wallet/internal/messaging"
	"wallet/internal/observability"
	"wallet/internal/postgres"
	"wallet/internal/worker"
)

func Options(cfg config.Config) fx.Option {
	return fx.Options(
		fx.Supply(cfg),
		fx.StartTimeout(90*time.Second),
		fx.StopTimeout(cfg.ShutdownTimeout+5*time.Second),
		fx.WithLogger(func(log *slog.Logger) fxevent.Logger {
			l := &fxevent.SlogLogger{Logger: log.With("component", "fx")}
			l.UseLogLevel(slog.LevelDebug)
			return l
		}),
		ObservabilityModule,
		DatabaseModule,
		MessagingModule,
		AuthModule,
		ServicesModule,
		WorkersModule,
		ConsumerModule,
		HTTPModule,
	)
}

var ObservabilityModule = fx.Module("observability",
	fx.Provide(
		func(cfg config.Config) *slog.Logger {
			return observability.NewLogger(cfg.LogLevel, os.Stdout).With("service", "wallet")
		},
		observability.NewMetrics,
		fx.Annotate(func(m *observability.Metrics) *observability.Metrics { return m }, fx.As(new(app.Metrics))),
		func(cfg config.Config, log *slog.Logger) app.FaultHook { return fault.Hook(cfg.FaultPoints, log) },
	),
)

var DatabaseModule = fx.Module("database",
	fx.Provide(
		newPool,
		fx.Annotate(postgres.NewTxManager, fx.As(fx.Self()), fx.As(new(app.TxRunner))),
		fx.Annotate(postgres.NewWalletRepo, fx.As(new(app.WalletRepository))),
		fx.Annotate(postgres.NewTransactionRepo, fx.As(new(app.TransactionRepository))),
		fx.Annotate(postgres.NewLedgerRepo, fx.As(new(app.LedgerRepository))),
		fx.Annotate(postgres.NewInboxRepo, fx.As(new(app.InboxRepository))),
		postgres.NewOutboxRepo,
		func(r *postgres.OutboxRepo) app.OutboxRepository { return r },
		func(r *postgres.OutboxRepo) app.OutboxStore { return r },
	),
)

func newPool(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) (*pgxpool.Pool, error) {
	pool, err := postgres.NewPool(context.Background(), postgres.Config{
		URL:              cfg.DatabaseURL,
		MaxConns:         cfg.DBMaxConns,
		LockTimeout:      cfg.DBLockTimeout,
		StatementTimeout: cfg.DBStatementTimeout,
	})
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			return waitFor(ctx, log, "postgres", pool.Ping)
		},
		OnStop: func(context.Context) error {
			pool.Close()
			log.Info("database pool closed")
			return nil
		},
	})
	return pool, nil
}

var MessagingModule = fx.Module("messaging",
	fx.Provide(
		func(cfg config.Config) (*sqs.Client, error) {
			return messaging.NewClient(context.Background(), messaging.ClientConfig{Region: cfg.AWSRegion, Endpoint: cfg.AWSEndpoint})
		},
		newQueues,
		fx.Annotate(messaging.NewEventPublisher, fx.As(new(app.EventPublisher))),
	),
)

func newQueues(cfg config.Config, client *sqs.Client, log *slog.Logger) (messaging.Queues, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var q messaging.Queues
	err := waitFor(ctx, log, "sqs", func(ctx context.Context) error {
		var err error
		q, err = messaging.ResolveQueues(ctx, client, messaging.QueueNames{
			Wager: cfg.WagerQueue, DLQ: cfg.WagerDLQ, Events: cfg.EventsQueue,
		})
		return err
	})
	return q, err
}

var AuthModule = fx.Module("auth",
	fx.Provide(func(lc fx.Lifecycle, cfg config.Config, log *slog.Logger) *auth.Verifier {
		v := auth.NewVerifier(cfg.OIDCIssuer, cfg.OIDCJWKSURL, cfg.OIDCAudience)
		lc.Append(fx.Hook{OnStart: func(ctx context.Context) error {
			return waitFor(ctx, log, "identity provider", v.Ready)
		}})
		return v
	}),
)

var ServicesModule = fx.Module("services",
	fx.Provide(
		func(cfg config.Config, d app.WageringDeps) *app.Wagering {
			return app.NewWagering(d, app.WageringConfig{
				References: domain.ReferencePolicy{
					InitialBackoff: cfg.ReferenceInitialBackoff,
					MaxBackoff:     cfg.ReferenceMaxBackoff,
					TTL:            cfg.ReferenceTTL,
					MaxAttempts:    cfg.ReferenceMaxAttempts,
				},
				ClaimBatch: cfg.ReferenceBatch,
				ClaimLease: cfg.ReferenceLease,
			})
		},
		func(d app.WalletDeps) *app.WalletService { return app.NewWalletService(d) },
		func(cfg config.Config, store app.OutboxStore, pub app.EventPublisher, m app.Metrics, log *slog.Logger, faults app.FaultHook) *app.OutboxRelay {
			return app.NewOutboxRelay(store, pub, m, log.With("component", "outbox"), app.RelayConfig{
				Batch: cfg.OutboxBatch, Lease: cfg.OutboxLease, MaxBackoff: cfg.OutboxMaxBackoff, PublishTimeout: 5 * time.Second,
			}, faults)
		},
		wageringDeps,
		walletDeps,
	),
)

type repos struct {
	fx.In
	Tx           app.TxRunner
	Wallets      app.WalletRepository
	Transactions app.TransactionRepository
	Ledger       app.LedgerRepository
	Outbox       app.OutboxRepository
	Inbox        app.InboxRepository
	Metrics      app.Metrics
	Log          *slog.Logger
	Faults       app.FaultHook
}

func wageringDeps(r repos) app.WageringDeps {
	return app.WageringDeps{Tx: r.Tx, Wallets: r.Wallets, Transactions: r.Transactions, Ledger: r.Ledger,
		Outbox: r.Outbox, Inbox: r.Inbox, Metrics: r.Metrics, Log: r.Log, Faults: r.Faults}
}

func walletDeps(r repos) app.WalletDeps {
	return app.WalletDeps{Tx: r.Tx, Wallets: r.Wallets, Transactions: r.Transactions, Ledger: r.Ledger,
		Outbox: r.Outbox, Metrics: r.Metrics, Log: r.Log}
}

var WorkersModule = fx.Module("workers",
	fx.Invoke(func(lc fx.Lifecycle, cfg config.Config, w *app.Wagering, relay *app.OutboxRelay, log *slog.Logger) {
		if cfg.ReferencesEnabled {
			loop := worker.NewLoop("pending-references", cfg.ReferencePollInterval, time.Minute, w.ResolveDue, log)
			lc.Append(fx.StartStopHook(loop.Start, loop.Stop))
		}
		if cfg.OutboxEnabled {
			loop := worker.NewLoop("outbox-relay", cfg.OutboxPollInterval, time.Minute, relay.RunOnce, log)
			lc.Append(fx.StartStopHook(loop.Start, loop.Stop))
		}
	}),
)

var ConsumerModule = fx.Module("consumer",
	fx.Provide(func(cfg config.Config, client *sqs.Client, q messaging.Queues, w *app.Wagering,
		log *slog.Logger, m *observability.Metrics, faults app.FaultHook) *messaging.Consumer {
		return messaging.NewConsumer(client, q, w, messaging.ConsumerConfig{
			Name:              cfg.ConsumerName,
			Workers:           cfg.ConsumerWorkers,
			WaitSeconds:       cfg.ReceiveWait,
			VisibilityTimeout: cfg.VisibilityTimeout,
			HandleTimeout:     cfg.HandleTimeout,
			AllowedSenders:    cfg.AllowedSenders,
		}, log, m, faults)
	}),
	fx.Invoke(func(lc fx.Lifecycle, cfg config.Config, c *messaging.Consumer) {
		if cfg.ConsumerEnabled {
			lc.Append(fx.StartStopHook(c.Start, c.Stop))
		}
	}),
)

var HTTPModule = fx.Module("http",
	fx.Provide(
		func(pool *pgxpool.Pool, client *sqs.Client, q messaging.Queues) []httpapi.ReadinessCheck {
			return []httpapi.ReadinessCheck{
				{Name: "postgres", Check: pool.Ping},
				{Name: "sqs", Check: func(ctx context.Context) error { return messaging.Ping(ctx, client, q.Wager) }},
			}
		},
		func(cfg config.Config, w *app.WalletService, wg *app.Wagering, v *auth.Verifier, m *observability.Metrics,
			log *slog.Logger, checks []httpapi.ReadinessCheck) *httpapi.API {
			return httpapi.NewAPI(w, wg, v, m, log, checks, cfg.RequestTimeout)
		},
	),
	fx.Invoke(registerServer),
)

func registerServer(lc fx.Lifecycle, cfg config.Config, api *httpapi.API, log *slog.Logger) {
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	lc.Append(fx.Hook{
		OnStart: func(context.Context) error {
			ln, err := net.Listen("tcp", cfg.HTTPAddr)
			if err != nil {
				return err
			}
			go func() {
				if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
					log.Error("http server stopped", "error", err)
				}
			}()
			log.Info("http server listening", "addr", ln.Addr().String())
			return nil
		},
		OnStop: func(ctx context.Context) error {
			api.Drain()
			err := srv.Shutdown(ctx)
			log.Info("http server stopped")
			return err
		},
	})
}

func waitFor(ctx context.Context, log *slog.Logger, name string, check func(context.Context) error) error {
	delay := 250 * time.Millisecond
	for {
		cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := check(cctx)
		cancel()
		if err == nil {
			return nil
		}
		log.Warn("dependency not ready", "dependency", name, "error", err)
		select {
		case <-ctx.Done():
			return fmt.Errorf("%s not ready: %w", name, err)
		case <-time.After(delay):
		}
		delay = min(delay*2, 5*time.Second)
	}
}
