-- migrate_v24.sql
-- Align calls.skip_reason CHECK constraint with all current code paths.

ALTER TABLE calls
    ADD COLUMN IF NOT EXISTS skip_reason TEXT;

ALTER TABLE calls DROP CONSTRAINT IF EXISTS calls_skip_reason_check;
ALTER TABLE calls ADD CONSTRAINT calls_skip_reason_check
CHECK (skip_reason IN (
    'slippage', 'quiet_hours', 'low_score', 'duplicate',
    'balance', 'allowed_hours', 'security_warning',
    'mcap_too_high', 'no_data', 'dex_circuit_open', 'vip_mcap_gate',
    'momentum_dump', 'mcap_too_low', 'unconfirmed', 'vip_paused',
    'high_bundle', 'serial_rugger', 'low_quality_bucket',
    'vip_low_score', 'no_entry_mcap'
));
