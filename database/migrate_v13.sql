-- migrate_v13.sql
-- Add VIP channel fields to calls and tokens tables.

ALTER TABLE calls
ADD COLUMN IF NOT EXISTS vip_tier TEXT
CHECK (vip_tier IN (
    'safe', 'gamble', 'gamble_risk',
    'high_risk', 'whale'
));

ALTER TABLE calls
ADD COLUMN IF NOT EXISTS vip_message_type TEXT
CHECK (vip_message_type IN (
    'gem_alert', 'whale_alert', 'volume_alert'
));

ALTER TABLE tokens
ADD COLUMN IF NOT EXISTS whale_wallet_sol NUMERIC;

ALTER TABLE tokens
ADD COLUMN IF NOT EXISTS whale_sol_spent NUMERIC;

ALTER TABLE tokens
ADD COLUMN IF NOT EXISTS whale_pct_spent NUMERIC;
