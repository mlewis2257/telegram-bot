-- =============================================================================
-- migrate_v6.sql — dev history + fake vol + mint resolution columns on tokens
-- =============================================================================
-- Run with:
--   psql -h 127.0.0.1 -U postgres -d solana_signals -v ON_ERROR_STOP=1 -f database/migrate_v6.sql
--
-- Safe to re-run — uses ADD COLUMN IF NOT EXISTS throughout.
-- =============================================================================

ALTER TABLE tokens
    ADD COLUMN IF NOT EXISTS dev_tokens_made  INTEGER,
    ADD COLUMN IF NOT EXISTS dev_bonds        INTEGER,
    ADD COLUMN IF NOT EXISTS dev_best_mcap    NUMERIC,
    ADD COLUMN IF NOT EXISTS fake_vol_usd     NUMERIC,
    ADD COLUMN IF NOT EXISTS fake_vol_pct     NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS dev_sold_pct     NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS mint_resolved    BOOLEAN NOT NULL DEFAULT TRUE;
