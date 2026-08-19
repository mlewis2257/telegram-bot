-- migrate_v29.sql
-- Two reproducibility/correctness fixes surfaced by the 2026-08-18 code audit.
-- Run as the table OWNER (sudo -u postgres psql -d solana_signals -f migrate_v29.sql)
-- — the app user cannot ALTER these existing tables.

-- (1) Real-fill / real-peak columns. These are RELIED ON by live_trader for real-fill
--     tracking and quote-based exits, but were added to production out-of-band and never
--     captured in a committed migration. A fresh deploy therefore lacks them and live
--     SILENTLY falls back to feed-based (inflated) exits. IF NOT EXISTS = no-op where they
--     already exist (i.e. current production).
ALTER TABLE trading_positions ADD COLUMN IF NOT EXISTS entry_price_fill NUMERIC;
ALTER TABLE trading_positions ADD COLUMN IF NOT EXISTS exit_price_fill  NUMERIC;
ALTER TABLE trading_positions ADD COLUMN IF NOT EXISTS real_peak_mcap   DOUBLE PRECISION;

-- (2) skip_reason drift. The code emits 9 reasons the v26 CHECK rejects, so those UPDATEs
--     fail and the calls stay UNLABELED — corrupting lane research AND silently disabling
--     the high_holders rug gate on the trending lane (it reclassifies to 'high_holders',
--     the write is rejected, the call keeps its tradeable lane). Extend the allowlist to
--     every reason the code actually writes.
ALTER TABLE calls DROP CONSTRAINT IF EXISTS calls_skip_reason_check;
ALTER TABLE calls ADD CONSTRAINT calls_skip_reason_check
CHECK (skip_reason IN (
    'slippage', 'quiet_hours', 'low_score', 'duplicate',
    'balance', 'allowed_hours', 'security_warning',
    'mcap_too_high', 'no_data', 'dex_circuit_open', 'vip_mcap_gate',
    'momentum_dump', 'mcap_too_low', 'unconfirmed', 'vip_paused',
    'high_bundle', 'serial_rugger', 'low_quality_bucket',
    'vip_low_score', 'no_entry_mcap', 'vip_mcap_too_low', 'high_fake_vol',
    'no_base_position',
    -- added v29: reasons the code emits that v26 rejected
    'high_holders', 'blocked_channel', 'reentry_cooldown', 'shadow_only',
    'vip_missing_tier', 'paper_open_failed', 'pending_duplicate',
    'vip_route_fallthrough', 'vip_unhandled_tier'
));
