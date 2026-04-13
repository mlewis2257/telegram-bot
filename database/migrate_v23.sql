-- migrate_v23.sql
-- Add 'high_bundle' and 'serial_rugger' to skip_reason CHECK constraint
-- for gamble_risk on-chain data filtering.

ALTER TABLE calls DROP CONSTRAINT IF EXISTS calls_skip_reason_check;
ALTER TABLE calls ADD CONSTRAINT calls_skip_reason_check
CHECK (skip_reason IN (
    'slippage', 'quiet_hours', 'low_score', 'duplicate',
    'balance', 'allowed_hours', 'security_warning',
    'mcap_too_high', 'no_data', 'dex_circuit_open', 'vip_mcap_gate',
    'momentum_dump', 'mcap_too_low', 'unconfirmed', 'vip_paused',
    'high_bundle', 'serial_rugger'
));
