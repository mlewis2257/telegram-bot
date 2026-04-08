-- migrate_v20.sql
-- 1. Add 'vip_paused' to skip_reason CHECK constraint.
-- 2. Add peak_multiplier_from_entry column to outcomes table.

ALTER TABLE calls DROP CONSTRAINT IF EXISTS calls_skip_reason_check;
ALTER TABLE calls ADD CONSTRAINT calls_skip_reason_check
CHECK (skip_reason IN (
    'slippage', 'quiet_hours', 'low_score', 'duplicate',
    'balance', 'allowed_hours', 'security_warning',
    'mcap_too_high', 'no_data', 'dex_circuit_open', 'vip_mcap_gate',
    'momentum_dump', 'mcap_too_low', 'unconfirmed', 'vip_paused'
));

ALTER TABLE outcomes
    ADD COLUMN IF NOT EXISTS peak_multiplier_from_entry NUMERIC;
