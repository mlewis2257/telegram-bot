-- migrate_v28.sql
-- Add profit_floor exit reason for Strategy A protected runner exits.

ALTER TABLE trading_positions
DROP CONSTRAINT IF EXISTS trading_positions_exit_reason_check;

ALTER TABLE trading_positions
ADD CONSTRAINT trading_positions_exit_reason_check
CHECK (exit_reason IN (
    'take_profit',
    'stop_loss',
    'timeout',
    '3x_tp',
    '5x_tp',
    '10x_tp',
    'profit_floor',
    'trail_stop',
    'hard_stop',
    'time_stop',
    'data_error',
    'manual',
    'rug'
));
