-- =============================================================================
-- migrate_v5.sql — realtime channel token metadata + channel_daily_stats
-- =============================================================================
-- Run with:
--   psql -h 127.0.0.1 -U postgres -d solana_signals -v ON_ERROR_STOP=1 -f database/migrate_v5.sql
--
-- Safe to re-run — uses IF NOT EXISTS / ADD COLUMN IF NOT EXISTS throughout.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. New realtime signal columns on tokens
-- -----------------------------------------------------------------------------
ALTER TABLE tokens
    ADD COLUMN IF NOT EXISTS token_age_minutes        INTEGER,
    ADD COLUMN IF NOT EXISTS security_flag            TEXT,
    ADD COLUMN IF NOT EXISTS is_cto                   BOOLEAN     NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS bundle_count             INTEGER,
    ADD COLUMN IF NOT EXISTS bundle_pct_initial       NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS bundle_pct_remaining     NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS sniper_count             INTEGER,
    ADD COLUMN IF NOT EXISTS sniper_pct_initial       NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS sniper_pct_remaining     NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS first_20_pct             NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS dev_sol_held             NUMERIC,
    ADD COLUMN IF NOT EXISTS dev_pct_held             NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS dev_sold                 BOOLEAN,
    ADD COLUMN IF NOT EXISTS bundled_pct              NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS bundled_sold_pct         NUMERIC(5,2),
    ADD COLUMN IF NOT EXISTS hodl_count               INTEGER,
    ADD COLUMN IF NOT EXISTS liq_at_detection         NUMERIC,
    ADD COLUMN IF NOT EXISTS vol_1h_at_detection      NUMERIC,
    ADD COLUMN IF NOT EXISTS detecting_wallet_sol     NUMERIC,
    ADD COLUMN IF NOT EXISTS detecting_sol_spent      NUMERIC;


-- -----------------------------------------------------------------------------
-- 2. channel_daily_stats table
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS channel_daily_stats (
    id                  SERIAL PRIMARY KEY,
    channel_id          INTEGER     NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    stat_date           DATE        NOT NULL,

    -- TYPE 4: daily aggregate stats
    entry_signals       INTEGER,
    win_rate_50pct      NUMERIC(5,2),
    wins_avg_profit     NUMERIC(5,2),
    best_token          TEXT,
    best_multiplier     NUMERIC(8,2),
    alerts_2x           INTEGER,
    alerts_5x           INTEGER,
    alerts_10x          INTEGER,
    alerts_15x_plus     INTEGER,

    -- TYPE 5: top performers leaderboard [{symbol, multiplier}, ...]
    top_performers      JSONB,

    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- One row per channel per day — TYPE 4 and TYPE 5 messages merge into it
CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_daily_stats_channel_date
    ON channel_daily_stats (channel_id, stat_date);

CREATE INDEX IF NOT EXISTS idx_channel_daily_stats_date
    ON channel_daily_stats (stat_date DESC);


-- -----------------------------------------------------------------------------
-- 3. New realtime channel seeds
-- -----------------------------------------------------------------------------
INSERT INTO channels (handle, platform, channel_type, weight) VALUES
    ('solwhaletrending', 'telegram', 'realtime', 1.0),
    ('solearlytrending',  'telegram', 'realtime', 1.0)
ON CONFLICT (platform, handle) DO NOTHING;
