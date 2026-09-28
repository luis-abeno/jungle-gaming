//go:build integration

package integration

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"
	"go.uber.org/goleak"

	"wallet/internal/observability/logctx"
	"wallet/internal/wiring"
)

func TestFxStartAndStopReleaseEverything(t *testing.T) {
	e := newEnv(t)
	httpClient.CloseIdleConnections()
	baseline := goleak.IgnoreCurrent()

	port := freePort(t)
	cfg := e.config(t, port, nil)
	logs := &lockedBuffer{}
	var pool *pgxpool.Pool
	a := fx.New(
		wiring.Options(cfg),
		fx.Decorate(func(*slog.Logger) *slog.Logger {
			return slog.New(logctx.Handler{Handler: slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelInfo})})
		}),
		fx.Populate(&pool),
		fx.NopLogger,
	)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := a.Start(ctx); err != nil {
		t.Fatal(err)
	}
	url := "http://127.0.0.1:" + strconv.Itoa(port)

	walletID, playerID := openWallet(t, url, "100.00")
	o := bet(walletID, playerID, "fx-1", "10.00")
	sendMessage(t, e.Queues.URLs.Wager, o.message("m-fx-1"), walletID, "m-fx-1")
	eventually(t, 15*time.Second, "work done", func() bool {
		s, _ := txStatus(t, e.Owner, "provider-a", "fx-1")
		return s == "PROCESSED" && queryInt(t, e.Owner, `SELECT count(*) FROM outbox_events WHERE published_at IS NULL`) == 0
	})

	if err := a.Stop(ctx); err != nil {
		t.Fatalf("stop: %v", err)
	}

	if err := pool.Ping(context.Background()); err == nil {
		t.Error("pool still usable after stop")
	}
	if _, err := http.Get(url + "/health/live"); err == nil {
		t.Error("server still accepting connections")
	}

	order := []string{"http server stopped", "consumer stopped", "worker stopped", "database pool closed"}
	out := logs.String()
	last := -1
	for _, msg := range order {
		i := strings.Index(out, `"msg":"`+msg+`"`)
		if i < 0 || i < last {
			t.Fatalf("shutdown order broken at %q:\n%s", msg, out)
		}
		last = i
	}

	httpClient.CloseIdleConnections()
	http.DefaultClient.CloseIdleConnections()
	goleak.VerifyNone(t, baseline,
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
	)
}
