//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"wallet/internal/postgres"
)

func TestMigrationsApplyAndRevert(t *testing.T) {
	t.Parallel()
	name := fmt.Sprintf("wallet_it_%s_migrations", runID)
	ctx := context.Background()
	if _, err := adminPool.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { adminPool.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)") })
	url := pgURL(pgOwner, name)

	tables := func() int64 {
		pool, err := pgxpool.New(ctx, url)
		if err != nil {
			t.Fatal(err)
		}
		defer pool.Close()
		return queryInt(t, pool, `SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name <> 'schema_migrations'`)
	}

	if v, dirty, err := postgres.Migrate(url, "up", 0); err != nil || v != 5 || dirty {
		t.Fatalf("up: %d %v %v", v, dirty, err)
	}
	if n := tables(); n != 5 {
		t.Fatalf("tables after up: %d", n)
	}
	if v, _, err := postgres.Migrate(url, "down", 1); err != nil || v != 4 {
		t.Fatalf("down one: %d %v", v, err)
	}
	if _, _, err := postgres.Migrate(url, "down", -1); err != nil {
		t.Fatalf("down all: %v", err)
	}
	if n := tables(); n != 0 {
		t.Fatalf("tables after down: %d", n)
	}
	if v, _, err := postgres.Migrate(url, "up", 0); err != nil || v != 5 {
		t.Fatalf("up again: %d %v", v, err)
	}
}

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

func execTx(pool *pgxpool.Pool, stmts ...string) error {
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, s := range stmts {
		if _, err := tx.Exec(ctx, s); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func TestLedgerIsAppendOnly(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := e.start(t, nil)
	walletID, _ := openWallet(t, svc.URL, "100.00")

	appPool, err := pgxpool.New(context.Background(), e.DBURL)
	if err != nil {
		t.Fatal(err)
	}
	defer appPool.Close()

	where := fmt.Sprintf(" wallet_ledger_entries WHERE wallet_id = '%s'", walletID)
	for _, stmt := range []string{
		"UPDATE wallet_ledger_entries SET amount_minor = 1 WHERE wallet_id = '" + walletID + "'",
		"DELETE FROM" + where,
		"TRUNCATE wallet_ledger_entries",
	} {
		if err := execTx(appPool, stmt); pgCode(err) != "42501" {
			t.Errorf("app role %q: %v", stmt, err)
		}
		if err := execTx(e.Owner, stmt); pgCode(err) != "P0001" {
			t.Errorf("owner %q: %v", stmt, err)
		}
	}
	if err := execTx(appPool, "DELETE FROM wallets WHERE id = '"+walletID+"'"); pgCode(err) != "42501" {
		t.Errorf("app role delete wallet: %v", err)
	}
	if e.ledgerEntries(t, walletID) != 1 {
		t.Fatal("ledger changed")
	}
}

func TestSchemaEnforcesInvariants(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	svc := e.start(t, nil)
	walletID, playerID := openWallet(t, svc.URL, "100.00")
	b := bet(walletID, playerID, "schema-bet", "10.00")
	betTx := submit(t, svc.URL, b).str("transactionId")
	refundTx := submit(t, svc.URL, b.with("REFUND", "schema-refund", "10.00", "schema-bet")).str("transactionId")
	if betTx == "" || refundTx == "" {
		t.Fatal("setup failed")
	}
	insertTx := func(id, kind, status, ext, key string, amount int, ref string) string {
		refCol := "NULL"
		refExt := "NULL"
		if ref != "" {
			refCol, refExt = "'"+ref+"'", "'x'"
		}
		balance, failure, completed := "NULL", "NULL", "NULL"
		switch status {
		case "PROCESSED":
			balance, completed = "10000", "now()"
		case "REJECTED":
			failure, completed = "'INSUFFICIENT_FUNDS'", "now()"
		}
		return fmt.Sprintf(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, currency,
			amount_minor, provider_id, external_transaction_id, idempotency_key, payload_hash, round_id, game_id,
			reference_external_transaction_id, reference_transaction_id, failure_code, balance_after_minor,
			created_at, updated_at, completed_at)
			VALUES ('%s', 'EXTERNAL', '%s', '%s', '%s', '%s', 'BRL', %d, 'provider-a', '%s', '%s', 'h', 'round-1', 'g',
			%s, %s, %s, %s, now(), now(), %s)`,
			id, kind, status, walletID, playerID, amount, ext, key, refExt, refCol, failure, balance, completed)
	}
	ledger := func(txID string, version, before, after int, dir string) string {
		return fmt.Sprintf(`INSERT INTO wallet_ledger_entries (id, wallet_id, transaction_id, wallet_version, direction,
			currency, amount_minor, balance_before_minor, balance_after_minor, created_at)
			VALUES ('%s', '%s', '%s', %d, '%s', 'BRL', %d, %d, %d, now())`,
			uuid.NewString(), walletID, txID, version, dir, abs(after-before), before, after)
	}
	lossID, rejID, rev2ID := uuid.NewString(), uuid.NewString(), uuid.NewString()

	cases := []struct {
		name  string
		code  string
		stmts []string
	}{
		{"negative balance", "23514", []string{`UPDATE wallets SET balance_minor = -1, version = version + 1 WHERE id = '` + walletID + `'`}},
		{"balance without ledger", "P0001", []string{`UPDATE wallets SET balance_minor = balance_minor + 1, version = version + 1 WHERE id = '` + walletID + `'`}},
		{"version jump", "P0001", []string{`UPDATE wallets SET balance_minor = balance_minor + 1, version = version + 5 WHERE id = '` + walletID + `'`}},
		{"version without balance change", "P0001", []string{`UPDATE wallets SET version = version + 1 WHERE id = '` + walletID + `'`}},
		{"second wallet for player and currency", "23505", []string{fmt.Sprintf(`INSERT INTO wallets VALUES ('%s', '%s', 'BRL', 0, 1, now(), now())`, uuid.NewString(), playerID)}},
		{"second opening", "23505", []string{fmt.Sprintf(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, currency, amount_minor, balance_after_minor, created_at, updated_at, completed_at) VALUES ('%s', 'INTERNAL', 'OPENING', 'PROCESSED', '%s', '%s', 'BRL', 1, 1, now(), now(), now())`, uuid.NewString(), walletID, playerID)}},
		{"opening with provider", "23514", []string{fmt.Sprintf(`INSERT INTO wager_transactions (id, origin, kind, status, wallet_id, player_id, currency, amount_minor, provider_id, balance_after_minor, created_at, updated_at, completed_at) VALUES ('%s', 'INTERNAL', 'OPENING', 'PROCESSED', '%s', '%s', 'BRL', 1, 'provider-a', 1, now(), now(), now())`, uuid.NewString(), walletID, playerID)}},
		{"wrong currency for wallet", "23503", []string{strings.Replace(insertTx(uuid.NewString(), "BET", "REJECTED", "usd-1", "k-usd-1", 100, ""), "'BRL'", "'USD'", 1)}},
		{"terminal transaction update", "P0001", []string{`UPDATE wager_transactions SET status = 'REJECTED', failure_code = 'INSUFFICIENT_FUNDS' WHERE id = '` + betTx + `'`}},
		{"transaction delete", "P0001", []string{`DELETE FROM wager_transactions WHERE id = '` + betTx + `'`}},
		{"duplicate idempotency key", "23505", []string{insertTx(uuid.NewString(), "BET", "REJECTED", "other-ext", "provider-a:schema-bet", 100, "")}},
		{"duplicate external id", "23505", []string{insertTx(uuid.NewString(), "BET", "REJECTED", "schema-bet", "other-key", 100, "")}},
		{"loss with amount", "23514", []string{insertTx(uuid.NewString(), "LOSS", "PROCESSED", "l-1", "k-l-1", 100, "")}},
		{"second reversal of the same bet", "23505", []string{insertTx(rev2ID, "ROLLBACK", "PROCESSED", "rb-2", "k-rb-2", 1000, betTx)}},
		{"duplicate ledger entry for a transaction", "23505", []string{ledger(betTx, 4, 10000, 9000, "DEBIT")}},
		{"broken ledger chain", "P0001", []string{
			insertTx(rejID, "WIN", "PROCESSED", "w-chain", "k-w-chain", 500, ""),
			ledger(rejID, 4, 5, 505, "CREDIT"),
		}},
		{"ledger for a LOSS", "P0001", []string{
			insertTx(lossID, "LOSS", "PROCESSED", "l-2", "k-l-2", 0, ""),
			fmt.Sprintf(`INSERT INTO wallet_ledger_entries VALUES ('%s', '%s', '%s', 4, 'CREDIT', 'BRL', 1, 10000, 10001, now())`, uuid.NewString(), walletID, lossID),
		}},
		{"ledger arithmetic", "23514", []string{
			insertTx(uuid.NewString(), "WIN", "PROCESSED", "w-arith", "k-w-arith", 500, ""),
			fmt.Sprintf(`INSERT INTO wallet_ledger_entries VALUES ('%s', '%s', '%s', 4, 'CREDIT', 'BRL', 500, 10000, 10499, now())`, uuid.NewString(), walletID, betTx),
		}},
	}
	for _, c := range cases {
		if err := execTx(e.Owner, c.stmts...); pgCode(err) != c.code {
			t.Errorf("%s: want %s, got %v", c.name, c.code, err)
		}
	}
	if e.balance(t, walletID) != 10000 || e.ledgerEntries(t, walletID) != 3 {
		t.Fatalf("state changed: balance %d entries %d", e.balance(t, walletID), e.ledgerEntries(t, walletID))
	}
	e.assertConsistent(t)

	if err := execTx(e.Owner,
		"SET LOCAL session_replication_role = replica",
		`UPDATE wallets SET balance_minor = balance_minor + 1 WHERE id = '`+walletID+`'`); err != nil {
		t.Fatal(err)
	}
	rec := call(t, "POST", svc.URL+"/wallets/"+walletID+"/reconciliation", token(t, "wallet-internal"), nil, nil)
	if rec.Status != http.StatusOK || rec.Body["consistent"] != false || rec.str("difference", "amount") != "0.01" {
		t.Fatalf("reconciliation: %s", rec.Raw)
	}
	if e.balance(t, walletID) != 10001 {
		t.Fatal("reconciliation must not change the balance")
	}
}

func abs(n int) int {
	if n < 0 {
		return -n
	}
	return n
}
