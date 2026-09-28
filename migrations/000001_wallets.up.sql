CREATE TABLE wallets (
    id            uuid        PRIMARY KEY,
    player_id     uuid        NOT NULL,
    currency      char(3)     NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_minor bigint      NOT NULL CHECK (balance_minor >= 0),
    version       bigint      NOT NULL CHECK (version >= 1),
    created_at    timestamptz NOT NULL,
    updated_at    timestamptz NOT NULL,
    CONSTRAINT wallets_player_currency_key UNIQUE (player_id, currency),
    CONSTRAINT wallets_id_currency_key UNIQUE (id, currency),
    CONSTRAINT wallets_id_player_currency_key UNIQUE (id, player_id, currency)
);

CREATE FUNCTION wallets_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'wallet % cannot be deleted', OLD.id;
    END IF;
    IF NEW.id <> OLD.id OR NEW.player_id <> OLD.player_id OR NEW.currency <> OLD.currency OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'identity of wallet % is immutable', OLD.id;
    END IF;
    IF NEW.balance_minor <> OLD.balance_minor AND NEW.version <> OLD.version + 1 THEN
        RAISE EXCEPTION 'balance change on wallet % must bump version from % to %', OLD.id, OLD.version, OLD.version + 1;
    END IF;
    IF NEW.balance_minor = OLD.balance_minor AND NEW.version <> OLD.version THEN
        RAISE EXCEPTION 'version of wallet % only changes with its balance', OLD.id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER wallets_guard
    BEFORE UPDATE OR DELETE ON wallets
    FOR EACH ROW EXECUTE FUNCTION wallets_guard();
