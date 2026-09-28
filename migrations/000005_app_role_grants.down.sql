DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'wallet_app') THEN
        REVOKE ALL ON wallets, wager_transactions, wallet_ledger_entries, inbox_messages, outbox_events FROM wallet_app;
    END IF;
END;
$$;
