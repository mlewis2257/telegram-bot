-- migrate_v16.sql
-- Add 'dex_circuit_open' to skip_reason constraint so circuit breaker
-- skips are auditable in the calls table.

ALTER TABLE calls DROP CONSTRAINT IF EXISTS calls_skip_reason_check;
ALTER TABLE calls ADD CONSTRAINT calls_skip_reason_check
CHECK (skip_reason IN (
    'slippage', 'quiet_hours', 'low_score', 'duplicate',
    'balance', 'allowed_hours', 'security_warning',
    'mcap_too_high', 'no_data', 'dex_circuit_open'
));
