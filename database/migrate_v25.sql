-- migrate_v25.sql
-- Add vip_mcap_too_low and high_fake_vol skip reasons for VIP safe tier filters.

ALTER TABLE calls DROP CONSTRAINT IF EXISTS calls_skip_reason_check;
ALTER TABLE calls ADD CONSTRAINT calls_skip_reason_check
CHECK (skip_reason IN (
    'slippage', 'quiet_hours', 'low_score', 'duplicate',
    'balance', 'allowed_hours', 'security_warning',
    'mcap_too_high', 'no_data', 'dex_circuit_open', 'vip_mcap_gate',
    'momentum_dump', 'mcap_too_low', 'unconfirmed', 'vip_paused',
    'high_bundle', 'serial_rugger', 'low_quality_bucket',
    'vip_low_score', 'no_entry_mcap', 'vip_mcap_too_low', 'high_fake_vol'
));
