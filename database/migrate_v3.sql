-- =============================================================================
-- migrate_v3.sql — whale_alerts table + token signal columns
-- =============================================================================
-- Run with:
--   psql -h 127.0.0.1 -U postgres -d solana_signals -v ON_ERROR_STOP=1 -f database/migrate_v3.sql
--
-- Safe to re-run — uses IF NOT EXISTS / ADD COLUMN IF NOT EXISTS throughout.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. New columns on tokens
-- -----------------------------------------------------------------------------
ALTER TABLE tokens
    ADD COLUMN IF NOT EXISTS dexscreener_paid     BOOLEAN     NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS dexscreener_paid_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS boost_count          INTEGER     NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_boost_at        TIMESTAMPTZ;


-- -----------------------------------------------------------------------------
-- 2. New whale_alerts table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS whale_alerts (
    id               SERIAL PRIMARY KEY,
    call_id          INTEGER     REFERENCES calls(id)  ON DELETE SET NULL,
    token_id         INTEGER     REFERENCES tokens(id) ON DELETE SET NULL,
    symbol           TEXT,
    sol_amount       NUMERIC,
    wallet_size_sol  NUMERIC,
    mcap_at_whale    NUMERIC,
    signal_count     INTEGER,
    raw_message      TEXT,
    detected_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_whale_alerts_token_id
    ON whale_alerts (token_id);

CREATE INDEX IF NOT EXISTS idx_whale_alerts_call_id
    ON whale_alerts (call_id);

CREATE INDEX IF NOT EXISTS idx_whale_alerts_detected_at
    ON whale_alerts (detected_at DESC);

CREATE INDEX IF NOT EXISTS idx_whale_alerts_unlinked
    ON whale_alerts (token_id)
    WHERE call_id IS NULL;
