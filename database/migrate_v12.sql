-- migrate_v12.sql
-- Extend calls.skip_reason CHECK constraint to include new entry gate values.

ALTER TABLE calls
    DROP CONSTRAINT IF EXISTS calls_skip_reason_check;

ALTER TABLE calls
    ADD CONSTRAINT calls_skip_reason_check
    CHECK (skip_reason IN (
        'slippage',
        'quiet_hours',
        'low_score',
        'duplicate',
        'balance',
        'allowed_hours',
        'security_warning',
        'mcap_too_high'
    ));
