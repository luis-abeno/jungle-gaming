CREATE TABLE wager_transactions (
    id                                uuid        PRIMARY KEY,
    origin                            text        NOT NULL CHECK (origin IN ('INTERNAL', 'EXTERNAL')),
    kind                              text        NOT NULL CHECK (kind IN ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK')),
    status                            text        NOT NULL CHECK (status IN ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED')),
    wallet_id                         uuid        NOT NULL,
    player_id                         uuid        NOT NULL,
    currency                          char(3)     NOT NULL,
    amount_minor                      bigint      NOT NULL,
    provider_id                       text,
    external_transaction_id           text,
    idempotency_key                   text,
    payload_hash                      text,
    round_id                          text,
    game_id                           text,
    reference_external_transaction_id text,
    reference_transaction_id          uuid        REFERENCES wager_transactions (id),
    failure_code                      text,
    balance_after_minor               bigint      CHECK (balance_after_minor >= 0),
    correlation_id                    text,
    attempts                          integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at                   timestamptz,
    reference_deadline                timestamptz,
    created_at                        timestamptz NOT NULL,
    updated_at                        timestamptz NOT NULL,
    completed_at                      timestamptz,

    CONSTRAINT wager_transactions_wallet_fk FOREIGN KEY (wallet_id, player_id, currency)
        REFERENCES wallets (id, player_id, currency),
    CONSTRAINT wager_transactions_id_wallet_key UNIQUE (id, wallet_id),

    CONSTRAINT wager_transactions_origin CHECK ((origin = 'INTERNAL') = (kind = 'OPENING')),
    CONSTRAINT wager_transactions_internal_fields CHECK (
        origin = 'EXTERNAL' OR (
            provider_id IS NULL AND external_transaction_id IS NULL AND idempotency_key IS NULL
            AND payload_hash IS NULL AND round_id IS NULL AND game_id IS NULL
            AND reference_external_transaction_id IS NULL AND reference_transaction_id IS NULL)),
    CONSTRAINT wager_transactions_external_fields CHECK (
        origin = 'INTERNAL' OR (
            provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL
            AND payload_hash IS NOT NULL AND round_id IS NOT NULL AND game_id IS NOT NULL)),
    CONSTRAINT wager_transactions_amount CHECK (
        CASE kind WHEN 'LOSS' THEN amount_minor = 0 ELSE amount_minor > 0 END),
    CONSTRAINT wager_transactions_reference_required CHECK (
        kind NOT IN ('REFUND', 'ROLLBACK') OR reference_external_transaction_id IS NOT NULL),
    CONSTRAINT wager_transactions_reference_forbidden CHECK (
        kind NOT IN ('OPENING', 'BET', 'LOSS') OR reference_external_transaction_id IS NULL),
    CONSTRAINT wager_transactions_processed CHECK (
        status <> 'PROCESSED' OR (balance_after_minor IS NOT NULL AND completed_at IS NOT NULL)),
    CONSTRAINT wager_transactions_failure CHECK ((status IN ('REJECTED', 'FAILED')) = (failure_code IS NOT NULL)),
    CONSTRAINT wager_transactions_waiting CHECK (
        status <> 'PENDING_REFERENCE' OR (next_attempt_at IS NOT NULL AND reference_deadline IS NOT NULL)),
    CONSTRAINT wager_transactions_opening_processed CHECK (kind <> 'OPENING' OR status = 'PROCESSED')
);

CREATE UNIQUE INDEX wager_transactions_idempotency_key ON wager_transactions (provider_id, idempotency_key);
CREATE UNIQUE INDEX wager_transactions_external_id ON wager_transactions (provider_id, external_transaction_id);

CREATE UNIQUE INDEX wager_transactions_single_opening ON wager_transactions (wallet_id) WHERE kind = 'OPENING';

CREATE UNIQUE INDEX wager_transactions_single_reversal ON wager_transactions (reference_transaction_id)
    WHERE status = 'PROCESSED' AND kind IN ('REFUND', 'ROLLBACK');

CREATE INDEX wager_transactions_due ON wager_transactions (next_attempt_at) WHERE status = 'PENDING_REFERENCE';
CREATE INDEX wager_transactions_waiting_on ON wager_transactions (provider_id, reference_external_transaction_id)
    WHERE status = 'PENDING_REFERENCE';
CREATE INDEX wager_transactions_wallet ON wager_transactions (wallet_id, created_at);

CREATE FUNCTION wager_transactions_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'transaction % cannot be deleted', OLD.id;
    END IF;
    IF OLD.status IN ('PROCESSED', 'REJECTED', 'FAILED') THEN
        RAISE EXCEPTION 'transaction % is terminal (%)', OLD.id, OLD.status;
    END IF;
    IF NEW.id <> OLD.id OR NEW.origin <> OLD.origin OR NEW.kind <> OLD.kind
        OR NEW.wallet_id <> OLD.wallet_id OR NEW.player_id <> OLD.player_id
        OR NEW.currency <> OLD.currency OR NEW.amount_minor <> OLD.amount_minor
        OR NEW.provider_id IS DISTINCT FROM OLD.provider_id
        OR NEW.external_transaction_id IS DISTINCT FROM OLD.external_transaction_id
        OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
        OR NEW.payload_hash IS DISTINCT FROM OLD.payload_hash
        OR NEW.round_id IS DISTINCT FROM OLD.round_id
        OR NEW.game_id IS DISTINCT FROM OLD.game_id
        OR NEW.reference_external_transaction_id IS DISTINCT FROM OLD.reference_external_transaction_id
        OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'business fields of transaction % are immutable', OLD.id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wager_transactions_guard
    BEFORE UPDATE OR DELETE ON wager_transactions
    FOR EACH ROW EXECUTE FUNCTION wager_transactions_guard();
