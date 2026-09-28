CREATE TABLE inbox_messages (
    consumer_name  text        NOT NULL,
    message_id     text        NOT NULL,
    payload_hash   text        NOT NULL,
    transaction_id uuid        REFERENCES wager_transactions (id),
    received_at    timestamptz NOT NULL,
    completed_at   timestamptz,
    PRIMARY KEY (consumer_name, message_id)
);

CREATE TABLE outbox_events (
    id              uuid        PRIMARY KEY,
    aggregate_type  text        NOT NULL,
    aggregate_id    uuid        NOT NULL,
    event_type      text        NOT NULL,
    event_version   integer     NOT NULL CHECK (event_version >= 1),
    partition_key   text        NOT NULL,
    payload         json        NOT NULL,
    occurred_at     timestamptz NOT NULL,
    created_at      timestamptz NOT NULL DEFAULT now(),
    attempts        integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    published_at    timestamptz,
    last_error      text
);

CREATE INDEX outbox_events_due ON outbox_events (next_attempt_at) WHERE published_at IS NULL;

CREATE FUNCTION outbox_events_guard() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        IF OLD.published_at IS NULL THEN
            RAISE EXCEPTION 'outbox event % was not published yet', OLD.id;
        END IF;
        RETURN OLD;
    END IF;
    IF NEW.id <> OLD.id OR NEW.aggregate_type <> OLD.aggregate_type OR NEW.aggregate_id <> OLD.aggregate_id
        OR NEW.event_type <> OLD.event_type OR NEW.event_version <> OLD.event_version
        OR NEW.partition_key <> OLD.partition_key OR NEW.payload::text <> OLD.payload::text
        OR NEW.occurred_at <> OLD.occurred_at THEN
        RAISE EXCEPTION 'outbox event % is immutable', OLD.id;
    END IF;
    IF OLD.published_at IS NOT NULL AND NEW.published_at IS DISTINCT FROM OLD.published_at THEN
        RAISE EXCEPTION 'outbox event % was already published', OLD.id;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER outbox_events_guard
    BEFORE UPDATE OR DELETE ON outbox_events
    FOR EACH ROW EXECUTE FUNCTION outbox_events_guard();
