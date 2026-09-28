DROP TRIGGER IF EXISTS wallets_require_ledger ON wallets;
DROP FUNCTION IF EXISTS wallets_require_ledger();
DROP TABLE IF EXISTS wallet_ledger_entries;
DROP FUNCTION IF EXISTS wallet_ledger_entries_match_transaction();
DROP FUNCTION IF EXISTS wallet_ledger_entries_chain();
DROP FUNCTION IF EXISTS wallet_ledger_entries_append_only();
