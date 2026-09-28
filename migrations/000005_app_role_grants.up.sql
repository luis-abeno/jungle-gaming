DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wallet_app') THEN
        GRANT SELECT, INSERT, UPDATE ON wallets, wager_transactions, inbox_messages, outbox_events TO wallet_app;
        GRANT SELECT, INSERT ON wallet_ledger_entries TO wallet_app;
    END IF;
END;
$$;

REVOKE ALL ON wallets, wager_transactions, wallet_ledger_entries, inbox_messages, outbox_events FROM PUBLIC;
