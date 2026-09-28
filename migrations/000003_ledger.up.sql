CREATE TABLE wallet_ledger_entries (
    id                   uuid        PRIMARY KEY,
    wallet_id            uuid        NOT NULL,
    transaction_id       uuid        NOT NULL,
    wallet_version       bigint      NOT NULL CHECK (wallet_version >= 1),
    direction            text        NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    currency             char(3)     NOT NULL,
    amount_minor         bigint      NOT NULL CHECK (amount_minor > 0),
    balance_before_minor bigint      NOT NULL CHECK (balance_before_minor >= 0),
    balance_after_minor  bigint      NOT NULL CHECK (balance_after_minor >= 0),
    created_at           timestamptz NOT NULL,

    CONSTRAINT wallet_ledger_entries_wallet_fk FOREIGN KEY (wallet_id, currency) REFERENCES wallets (id, currency),
    CONSTRAINT wallet_ledger_entries_transaction_fk FOREIGN KEY (transaction_id, wallet_id)
        REFERENCES wager_transactions (id, wallet_id),
    CONSTRAINT wallet_ledger_entries_wallet_transaction_key UNIQUE (wallet_id, transaction_id),
    CONSTRAINT wallet_ledger_entries_wallet_version_key UNIQUE (wallet_id, wallet_version),
    CONSTRAINT wallet_ledger_entries_arithmetic CHECK (
        (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
        OR (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor))
);

CREATE FUNCTION wallet_ledger_entries_append_only() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append only (% refused)', TG_OP;
END;
$$;

CREATE TRIGGER wallet_ledger_entries_no_update_delete
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_entries_append_only();

CREATE TRIGGER wallet_ledger_entries_no_truncate
    BEFORE TRUNCATE ON wallet_ledger_entries
    FOR EACH STATEMENT EXECUTE FUNCTION wallet_ledger_entries_append_only();

CREATE FUNCTION wallet_ledger_entries_chain() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    previous_after bigint;
BEGIN
    SELECT balance_after_minor INTO previous_after
    FROM wallet_ledger_entries
    WHERE wallet_id = NEW.wallet_id AND wallet_version = NEW.wallet_version - 1;

    IF FOUND THEN
        IF previous_after <> NEW.balance_before_minor THEN
            RAISE EXCEPTION 'ledger chain broken on wallet % at version %', NEW.wallet_id, NEW.wallet_version;
        END IF;
    ELSIF NEW.balance_before_minor <> 0 OR NEW.wallet_version > 2 THEN
        RAISE EXCEPTION 'ledger entry for wallet % at version % has no predecessor', NEW.wallet_id, NEW.wallet_version;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wallet_ledger_entries_chain
    BEFORE INSERT ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_entries_chain();

CREATE FUNCTION wallet_ledger_entries_match_transaction() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    tx wager_transactions%ROWTYPE;
BEGIN
    SELECT * INTO tx FROM wager_transactions WHERE id = NEW.transaction_id;
    IF tx.status <> 'PROCESSED' THEN
        RAISE EXCEPTION 'ledger entry % points to a % transaction', NEW.id, tx.status;
    END IF;
    IF tx.amount_minor <> NEW.amount_minor OR tx.currency <> NEW.currency THEN
        RAISE EXCEPTION 'ledger entry % amount differs from transaction %', NEW.id, tx.id;
    END IF;
    IF tx.kind = 'LOSS'
        OR (tx.kind = 'BET' AND NEW.direction <> 'DEBIT')
        OR (tx.kind IN ('OPENING', 'WIN', 'REFUND') AND NEW.direction <> 'CREDIT') THEN
        RAISE EXCEPTION 'ledger entry % has direction % for a % transaction', NEW.id, NEW.direction, tx.kind;
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER wallet_ledger_entries_match_transaction
    AFTER INSERT ON wallet_ledger_entries
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wallet_ledger_entries_match_transaction();

CREATE FUNCTION wallets_require_ledger() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
    before_minor bigint;
BEGIN
    IF TG_OP = 'INSERT' THEN
        before_minor := 0;
    ELSE
        before_minor := OLD.balance_minor;
    END IF;

    IF NEW.balance_minor = before_minor THEN
        RETURN NULL;
    END IF;

    PERFORM 1 FROM wallet_ledger_entries
    WHERE wallet_id = NEW.id
      AND wallet_version = NEW.version
      AND balance_before_minor = before_minor
      AND balance_after_minor = NEW.balance_minor;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'wallet % changed to version % without a ledger entry', NEW.id, NEW.version;
    END IF;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER wallets_require_ledger
    AFTER INSERT OR UPDATE ON wallets
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION wallets_require_ledger();

CREATE INDEX wallet_ledger_entries_transaction ON wallet_ledger_entries (transaction_id);
