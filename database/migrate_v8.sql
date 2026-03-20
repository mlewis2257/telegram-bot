-- migrate_v8.sql
-- Add '10x_tp' to the trading_positions exit_reason CHECK constraint.

ALTER TABLE trading_positions
DROP CONSTRAINT IF EXISTS trading_positions_exit_reason_check;

ALTER TABLE trading_positions
ADD CONSTRAINT trading_positions_exit_reason_check
CHECK (exit_reason IN (
    'take_profit',  -- kept for backward compat
    'stop_loss',    -- kept for backward compat
    'timeout',      -- kept for backward compat
    '3x_tp',        -- 3x take profit
    '5x_tp',        -- 5x take profit
    '10x_tp',       -- 10x take profit
    'trail_stop',   -- trailing stop (tiered: 15% / 20% / 25% from peak)
    'hard_stop',    -- hard stop loss (down 50% from entry)
    'time_stop'     -- 24-hour time stop
));
