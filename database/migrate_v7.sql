-- migrate_v7.sql
-- Expand trading_positions.exit_reason CHECK constraint to granular values.
--
-- The original constraint allowed only generic labels ('take_profit',
-- 'stop_loss', 'timeout'). Expanding to distinguish 5x vs 3x take-profit,
-- trailing vs hard stop-loss, and the 24h time stop for accurate reporting.
-- The three original values are retained for backward compatibility with any
-- existing rows that were written before this migration.

ALTER TABLE trading_positions
DROP CONSTRAINT IF EXISTS trading_positions_exit_reason_check;

ALTER TABLE trading_positions
ADD CONSTRAINT trading_positions_exit_reason_check
CHECK (exit_reason IN (
    'take_profit',  -- kept for backward compat
    'stop_loss',    -- kept for backward compat
    'timeout',      -- kept for backward compat
    '5x_tp',        -- 5x take profit
    '3x_tp',        -- 3x take profit
    'trail_stop',   -- trailing stop (peak >= 2x, then 40% drawdown from peak)
    'hard_stop',    -- hard stop loss (down 60% from entry)
    'time_stop'     -- 24-hour time stop
));
