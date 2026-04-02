-- migrate_v15.sql
-- Store entry volume_h1 in trading_positions so monitor process can access it

ALTER TABLE trading_positions
ADD COLUMN IF NOT EXISTS entry_volume_h1 NUMERIC;
