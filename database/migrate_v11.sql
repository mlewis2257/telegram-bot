-- migrate_v11.sql
-- Add skip_reason column to calls for tracking filtered-out signals.

ALTER TABLE calls
ADD COLUMN IF NOT EXISTS skip_reason TEXT
CHECK (skip_reason IN (
    'slippage',
    'quiet_hours',
    'low_score',
    'duplicate',
    'balance',
    'allowed_hours'
));

CREATE INDEX IF NOT EXISTS idx_calls_skip_reason
    ON calls (skip_reason)
    WHERE skip_reason IS NOT NULL;
