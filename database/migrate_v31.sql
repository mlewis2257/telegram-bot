-- migrate_v31.sql — steep hold (live_trader LIVE_STEEP_HOLD)
--
-- 1. trading_positions.steep_hold_at: when a live position skipped its 2x bank to hold.
--    Shared state, because two processes (sol-monitor, sol-ws-monitor) evaluate exits.
-- 2. Three new exit reasons in the exit_reason CHECK constraint. Without them, the close
--    after a real on-chain sell would fail to record. live_trader checks for both at
--    startup and keeps steep hold OFF until this has been applied.
--
-- Same list as migrate_v30 plus the three steep_hold_* reasons. Safe to re-run.
-- RUN (VPS):  sudo -u postgres psql -d solana_signals -f migrate_v31.sql

BEGIN;

ALTER TABLE trading_positions ADD COLUMN IF NOT EXISTS steep_hold_at timestamptz;

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
    'no_bounce_stop',
    -- runner window floor (live_trader LIVE_RUNNER_WINDOW_ENABLED, off by default)
    'runner_floor_stop',
    -- steep hold (live_trader LIVE_STEEP_HOLD, off by default) — v31
    'steep_hold_target','steep_hold_floor','steep_hold_time'
]::text[]));

COMMIT;

-- Check:
--   SELECT column_name FROM information_schema.columns
--    WHERE table_name = 'trading_positions' AND column_name = 'steep_hold_at';
--   SELECT pg_get_constraintdef(oid) LIKE '%steep_hold_time%'
--     FROM pg_constraint WHERE conname = 'trading_positions_exit_reason_check';
