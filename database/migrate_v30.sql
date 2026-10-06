-- migrate_v30.sql
-- Capture TWO changes that were applied to production by hand on 2026-10-02/03 and never
-- committed. Without this file, a rebuild from the repo lands on migrate_v28's constraint,
-- which contains NO bank reasons at all — so live would sell on-chain, fail the DB close,
-- and strand every bank exit at status='closing'. That exact failure happened once (ZPAD,
-- call_id 296124) and the constraint rejecting the write is the ONLY reason the underlying
-- decimals=0 bug was ever noticed.
--
-- Keep the enum EXPLICIT rather than a regex. A regex would have silently admitted the
-- fabricated value that surfaced the bug.

-- ── 1. exit_reason: add every LIVE_EXIT_OVERLAYS key ─────────────────────────────────
-- Sourced from live_trader.LIVE_EXIT_OVERLAYS plus the base reasons from v28. 'rug' is
-- also written by the collapse guard (live_trader check_live_exits, basis=feed_protective).
-- NULL is allowed explicitly: v28's IN (...) form rejected NULL, which blocked the row
-- from ever being opened with exit_reason unset.

ALTER TABLE trading_positions
DROP CONSTRAINT IF EXISTS trading_positions_exit_reason_check;

ALTER TABLE trading_positions
ADD CONSTRAINT trading_positions_exit_reason_check
CHECK (exit_reason IS NULL OR exit_reason = ANY (ARRAY[
    -- base (migrate_v28)
    'take_profit','stop_loss','timeout','3x_tp','5x_tp','10x_tp','profit_floor',
    'trail_stop','hard_stop','time_stop','manual','rug','data_error',
    -- bank overlays
    'bank_1p2x','bank_1p3x','bank_1p4x','bank_1p5x','bank_1p75x','bank_2x',
    -- confirm-bank overlays
    'confirm_bank_1p2x','confirm_bank_1p3x','confirm_bank_1p4x','confirm_bank_1p5x',
    'confirm_bank_1p75x','confirm_bank_2x',
    -- lock-trail overlays
    'lock_trail_a1p75_f1p35_tr30','lock_trail_a1p75_f1p35_tr40',
    'lock_trail_a1p5_f1p2_tr30','lock_trail_a2x_f1p55_tr30',
    -- lock-or-bank overlays
    'lock_or_bank_1p3x_1p1x','lock_or_bank_1p4x_1p15x','lock_or_bank_1p5x_1p2x',
    'lock_or_bank_1p75x_1p35x','lock_or_bank_2x_1p55x',
    -- no-bounce stop (live_trader LIVE_NO_BOUNCE_STOP_ENABLED)
    'no_bounce_stop'
]::text[]));

-- ── 2. calls.live_skip_reason ────────────────────────────────────────────────────────
-- skip_reason is the LANE KEY (lane_policy reads it as `category`:
-- `(skip_reason or "") == "low_score"`), and set_call_skip_reason only writes it when it
-- is NULL. The router labels every tradeable call first, so that function was a NO-OP for
-- every call live could possibly trade — which is why `WHERE skip_reason='security_warning'`
-- returned zero rows while the log plainly showed live skipping on it, and why auditing
-- live's skips meant grepping pm2 logs. live_skip_reason always takes the latest value.
--
-- KNOWN LIMITATION: the router calls the same function, so this column holds whoever wrote
-- last, not live specifically. Live's own values are entry_filter, entry_quote_429,
-- entry_roundtrip_429, entry_quote_no_route, security_warning, mcap_too_high, duplicate.
-- Deliberately UNCONSTRAINED so a new live skip reason can never fail the write.

ALTER TABLE calls ADD COLUMN IF NOT EXISTS live_skip_reason text;

-- ── 3. calls_skip_reason_check: add live's entry-path values ─────────────────────────
-- migrate_v29's list omits the reasons live_trader writes on its entry path, so whenever
-- skip_reason happened to be NULL the write violated the constraint and was swallowed by
-- set_call_skip_reason's try/except. Harmless but it silently lost data.

ALTER TABLE calls DROP CONSTRAINT IF EXISTS calls_skip_reason_check;
ALTER TABLE calls ADD CONSTRAINT calls_skip_reason_check
CHECK (skip_reason IS NULL OR skip_reason = ANY (ARRAY[
    -- migrate_v29
    'slippage','quiet_hours','low_score','duplicate',
    'balance','allowed_hours','security_warning',
    'mcap_too_high','no_data','dex_circuit_open','vip_mcap_gate',
    'momentum_dump','mcap_too_low','unconfirmed','vip_paused',
    'high_bundle','serial_rugger','low_quality_bucket',
    'vip_low_score','no_entry_mcap','vip_mcap_too_low',
    'high_fake_vol','no_base_position','pending_duplicate',
    'paper_open_failed','blocked_channel','reentry_cooldown',
    'shadow_only','vip_missing_tier','vip_unhandled_tier',
    'vip_route_fallthrough','vip_gamble_allowed_hours',
    'vip_safe_allowed_hours','vip_gamble_weak_pocket',
    'free_allowed_bucket','free_blocked_hour','free_weak_pocket',
    'high_holders','unsupported_channel','paper_dispatch_fallthrough',
    -- live entry path (live_trader), missing from v29
    'entry_filter','entry_quote_429','entry_roundtrip_429','entry_quote_no_route',
    -- entry_quality gate reasons
    'entry_exec_ratio','entry_roundtrip'
]::text[]));

-- ── Verify ───────────────────────────────────────────────────────────────────────────
-- SELECT pg_get_constraintdef(oid) FROM pg_constraint
--  WHERE conname IN ('trading_positions_exit_reason_check','calls_skip_reason_check');
-- SELECT column_name FROM information_schema.columns
--  WHERE table_name='calls' AND column_name='live_skip_reason';
--
-- On the VPS, TablePlus connects as the app user and cannot ALTER. Run as:
--   sudo -u postgres psql -d solana_signals -v ON_ERROR_STOP=1 -f migrate_v30.sql
-- Production already has items 1 and 2 applied by hand; all three statements are
-- idempotent, so re-running is safe and only item 3 should actually change anything.
