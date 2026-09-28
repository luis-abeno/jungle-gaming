package postgres

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"wallet/internal/app"
)

type Config struct {
	URL              string
	MaxConns         int32
	LockTimeout      time.Duration
	StatementTimeout time.Duration
}

func NewPool(ctx context.Context, cfg Config) (*pgxpool.Pool, error) {
	pc, err := pgxpool.ParseConfig(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if cfg.MaxConns > 0 {
		pc.MaxConns = cfg.MaxConns
	}
	if pc.ConnConfig.ConnectTimeout == 0 {
		pc.ConnConfig.ConnectTimeout = 5 * time.Second
	}
	rp := pc.ConnConfig.RuntimeParams
	rp["application_name"] = "wallet"
	rp["lock_timeout"] = fmt.Sprintf("%dms", cfg.LockTimeout.Milliseconds())
	rp["statement_timeout"] = fmt.Sprintf("%dms", cfg.StatementTimeout.Milliseconds())
	rp["idle_in_transaction_session_timeout"] = "60000"
	rp["timezone"] = "UTC"
	return pgxpool.NewWithConfig(ctx, pc)
}

type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

type txKey struct{}

type TxManager struct {
	pool *pgxpool.Pool
}

func NewTxManager(pool *pgxpool.Pool) *TxManager { return &TxManager{pool: pool} }

func (m *TxManager) WithinTx(ctx context.Context, fn func(context.Context) error) error {
	return m.run(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted}, fn)
}

func (m *TxManager) WithinSnapshot(ctx context.Context, fn func(context.Context) error) error {
	return m.run(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly}, fn)
}

func (m *TxManager) run(ctx context.Context, opts pgx.TxOptions, fn func(context.Context) error) error {
	if _, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return fn(ctx)
	}
	tx, err := m.pool.BeginTx(ctx, opts)
	if err != nil {
		return classify(err)
	}
	defer func() {
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rctx)
	}()
	if err := fn(context.WithValue(ctx, txKey{}, tx)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return classify(err)
	}
	return nil
}

func (m *TxManager) q(ctx context.Context) querier {
	if tx, ok := ctx.Value(txKey{}).(pgx.Tx); ok {
		return tx
	}
	return m.pool
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return app.ErrNotFound
	}
	if errors.Is(err, context.Canceled) {
		return err
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch {
		case pgErr.Code == "55P03":
			return fmt.Errorf("%w: %s", app.ErrLockTimeout, pgErr.Message)
		case pgErr.Code == "40001", pgErr.Code == "40P01":
			return fmt.Errorf("%w: %s", app.ErrConcurrentWrite, pgErr.Message)
		case pgErr.Code == "57014", pgErr.Code == "53300", pgErr.Code == "53400",
			pgErr.Code[:2] == "08", pgErr.Code[:2] == "57":
			return fmt.Errorf("%w: %s (%s)", app.ErrUnavailable, pgErr.Message, pgErr.Code)
		}
		return err
	}
	var netErr net.Error
	var connErr *pgconn.ConnectError
	if errors.As(err, &netErr) || errors.As(err, &connErr) || errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, context.DeadlineExceeded) ||
		pgconn.SafeToRetry(err) || pgconn.Timeout(err) {
		return fmt.Errorf("%w: %v", app.ErrUnavailable, err)
	}
	if errors.Is(err, net.ErrClosed) {
		return fmt.Errorf("%w: %v", app.ErrUnavailable, err)
	}
	return err
}

func uniqueViolation(err error) (constraint string, ok bool) {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return pgErr.ConstraintName, true
	}
	return "", false
}
