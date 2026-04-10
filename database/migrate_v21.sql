-- migrate_v21.sql
-- 1. Add is_strategy_b column to trading_positions (AB test strategy flag).
-- 2. Add 2x_tp to exit_reason CHECK constraint.

ALTER TABLE trading_positions
    ADD COLUMN IF NOT EXISTS is_strategy_b BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE trading_positions DROP CONSTRAINT IF EXISTS trading_positions_exit_reason_check;
ALTER TABLE trading_positions ADD CONSTRAINT trading_positions_exit_reason_check
CHECK (exit_reason IN (
    'take_profit', 'stop_loss', 'manual', 'timeout', 'rug',
    'hard_stop', 'trail_stop', 'time_stop',
    '3x_tp', '5x_tp', '10x_tp', '2x_tp'
));
