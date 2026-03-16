-- =============================================================================
-- migrate_v2.sql — Apply schema v2 changes to existing solana_signals database
-- =============================================================================
-- Run with:
--   psql -h 127.0.0.1 -U postgres -d solana_signals -v ON_ERROR_STOP=1 -f database/migrate_v2.sql
--
-- Safe to re-run — all statements use IF NOT EXISTS or ON CONFLICT DO NOTHING.
-- Does NOT drop or modify existing data.
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. Create channels table (new)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS channels (
    id           SERIAL PRIMARY KEY,
    handle       TEXT        NOT NULL,
    platform     TEXT        NOT NULL CHECK (platform IN ('telegram', 'twitter', 'discord')),
    channel_type TEXT        NOT NULL CHECK (channel_type IN ('lagging', 'realtime')),
    weight       NUMERIC(3,2) NOT NULL DEFAULT 1.0
                              CHECK (weight >= 0.0 AND weight <= 1.0),
    is_active    BOOLEAN     NOT NULL DEFAULT TRUE,
    added_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_channels_platform_handle
    ON channels (platform, handle);

-- Seed the initial lagging channel
INSERT INTO channels (handle, platform, channel_type, weight) VALUES
    ('solhousesignal', 'telegram', 'lagging', 0.6)
ON CONFLICT (platform, handle) DO NOTHING;


-- -----------------------------------------------------------------------------
-- 2. Add new columns to calls
-- -----------------------------------------------------------------------------
ALTER TABLE calls
    ADD COLUMN IF NOT EXISTS message_type TEXT
        CHECK (message_type IN ('lagging_call', 'initial_call', 'update')),
    ADD COLUMN IF NOT EXISTS channel_id   INTEGER
        REFERENCES channels(id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_calls_channel_id
    ON calls (channel_id);

CREATE INDEX IF NOT EXISTS idx_calls_message_type
    ON calls (message_type);


-- -----------------------------------------------------------------------------
-- 3. Add new columns to outcomes
-- -----------------------------------------------------------------------------
ALTER TABLE outcomes
    ADD COLUMN IF NOT EXISTS mcap_at_result     NUMERIC,
    ADD COLUMN IF NOT EXISTS result_reported_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS stated_multiplier  NUMERIC(8,2);


-- Migration complete. Verify with:
--   psql -h 127.0.0.1 -U postgres -d solana_signals -c "\d calls"
--   psql -h 127.0.0.1 -U postgres -d solana_signals -c "\d outcomes"
--   psql -h 127.0.0.1 -U postgres -d solana_signals -c "SELECT * FROM channels;"
