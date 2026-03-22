-- migrate_v9.sql
-- Add live-trading columns to trading_positions.
-- NULL for all existing paper rows; populated only for live trades.

ALTER TABLE trading_positions
    ADD COLUMN IF NOT EXISTS tokens_held   BIGINT,   -- raw token units received on buy
    ADD COLUMN IF NOT EXISTS tx_signature  TEXT,     -- Solana tx signature for the buy
    ADD COLUMN IF NOT EXISTS router        TEXT;     -- Jupiter router that won the order
