-- =============================================================================
-- migrate_v4.sql — add 'inferred_call' to calls.message_type CHECK constraint
-- =============================================================================
-- Run with:
--   psql -h 127.0.0.1 -U postgres -d solana_signals -v ON_ERROR_STOP=1 -f database/migrate_v4.sql
--
-- Safe to re-run — DROP CONSTRAINT IF EXISTS guards against missing constraint.
-- =============================================================================


-- PostgreSQL names inline CHECK constraints as {table}_{column}_check.
-- We drop and recreate to add the new 'inferred_call' value.

ALTER TABLE calls
    DROP CONSTRAINT IF EXISTS calls_message_type_check;

ALTER TABLE calls
    ADD CONSTRAINT calls_message_type_check
    CHECK (message_type IN ('lagging_call', 'initial_call', 'update', 'inferred_call'));
