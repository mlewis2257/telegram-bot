-- migrate_v22.sql
-- Add vip_tier column to trading_positions so VIP gamble tier classification
-- persists across restarts instead of relying on in-memory dict.

ALTER TABLE trading_positions
    ADD COLUMN IF NOT EXISTS vip_tier VARCHAR(20);
