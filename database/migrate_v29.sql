-- migrate_v29.sql
-- Align live-trading guards and current skip_reason values with code paths.

ALTER TABLE calls DROP CONSTRAINT IF EXISTS calls_skip_reason_check;
ALTER TABLE calls ADD CONSTRAINT calls_skip_reason_check
CHECK (skip_reason IN (
    'slippage', 'quiet_hours', 'low_score', 'duplicate',
    'balance', 'allowed_hours', 'security_warning',
    'mcap_too_high', 'no_data', 'dex_circuit_open', 'vip_mcap_gate',
    'momentum_dump', 'mcap_too_low', 'unconfirmed', 'vip_paused',
    'high_bundle', 'serial_rugger', 'low_quality_bucket',
    'vip_low_score', 'no_entry_mcap', 'vip_mcap_too_low',
    'high_fake_vol', 'no_base_position', 'pending_duplicate',
    'paper_open_failed', 'blocked_channel', 'reentry_cooldown',
    'shadow_only', 'vip_missing_tier', 'vip_unhandled_tier',
    'vip_route_fallthrough', 'vip_gamble_allowed_hours',
    'vip_safe_allowed_hours', 'vip_gamble_weak_pocket',
    'free_allowed_bucket', 'free_blocked_hour', 'free_weak_pocket',
    'high_holders', 'unsupported_channel', 'paper_dispatch_fallthrough'
));

ALTER TABLE trading_positions
    ADD COLUMN IF NOT EXISTS entry_price_fill NUMERIC,
    ADD COLUMN IF NOT EXISTS exit_price_fill NUMERIC,
    ADD COLUMN IF NOT EXISTS real_peak_mcap DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS tokens_held BIGINT,
    ADD COLUMN IF NOT EXISTS tx_signature TEXT,
    ADD COLUMN IF NOT EXISTS router TEXT;

ALTER TABLE trading_positions DROP CONSTRAINT IF EXISTS trading_positions_status_check;
ALTER TABLE trading_positions ADD CONSTRAINT trading_positions_status_check
CHECK (status IN ('open', 'closing', 'closed', 'cancelled'));
