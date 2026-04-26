-- migrate_v27.sql
-- Track per-position observed peaks for clean Strategy A/B analysis.

ALTER TABLE trading_positions
    ADD COLUMN IF NOT EXISTS peak_mcap NUMERIC,
    ADD COLUMN IF NOT EXISTS peak_multiplier NUMERIC(10,4),
    ADD COLUMN IF NOT EXISTS peak_at TIMESTAMPTZ;
