"""
peak_timing_report.py — Compare eventual signal peak vs observed post-entry peak.

Usage:
    python3 peak_timing_report.py
    python3 peak_timing_report.py --days 7
    python3 peak_timing_report.py --strategy a
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))

import db


def _build_since(days: int | None) -> datetime | None:
    if days is None:
        return None
    return datetime.now(timezone.utc) - timedelta(days=days)


def _strategy_sql(strategy: str) -> tuple[str, str]:
    if strategy == "a":
        return "AND tp.is_strategy_b = FALSE", "A"
    if strategy == "b":
        return "AND tp.is_strategy_b = TRUE", "B"
    return "", "A+B"


def _fmt_dt(dt) -> str:
    if dt is None:
        return "n/a"
    return str(dt)


def _fmt_num(v, places: int = 2) -> str:
    if v is None:
        return "n/a"
    return f"{float(v):.{places}f}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare eventual call peak vs post-entry observed peak")
    parser.add_argument("--days", type=int, default=3, help="Limit to the last N days by entry_time")
    parser.add_argument("--strategy", choices=["a", "b", "both"], default="both", help="Strategy filter")
    parser.add_argument("--limit", type=int, default=50, help="Row limit")
    args = parser.parse_args()

    since = _build_since(args.days)
    strategy_sql, strategy_label = _strategy_sql(args.strategy)

    conn = db.get_conn()
    with conn.cursor() as cur:
        sql = f"""
            SELECT
                tp.entry_time,
                t.symbol,
                ch.handle,
                tp.is_strategy_b,
                tp.exit_reason,
                o.peak_multiplier,
                o.result_reported_at,
                tp.peak_multiplier,
                tp.peak_at,
                tp.entry_price,
                tp.exit_price,
                tp.pnl_sol
            FROM trading_positions tp
            JOIN calls c ON c.id = tp.call_id
            JOIN tokens t ON t.id = c.token_id
            JOIN channels ch ON ch.id = c.channel_id
            LEFT JOIN outcomes o ON o.call_id = c.id
            WHERE tp.is_simulation = TRUE
              AND tp.status = 'closed'
              {strategy_sql}
              {"AND tp.entry_time >= %s" if since else ""}
            ORDER BY tp.entry_time DESC
            LIMIT %s
        """
        params = [since, args.limit] if since else [args.limit]
        cur.execute(sql, params)
        rows = cur.fetchall()

    print(f"Peak Timing Report — strategy {strategy_label}")
    if since:
        print(f"Since: {since.isoformat()}")
    print()
    print(
        "entry_time | symbol | strat_b | channel | exit_reason | "
        "signal_peak | signal_reported_at | observed_peak | observed_at | exit_mult | pnl_sol"
    )
    for row in rows:
        entry_time, symbol, handle, is_b, exit_reason, signal_peak, signal_reported_at, observed_peak, observed_at, entry_price, exit_price, pnl_sol = row
        exit_mult = (float(exit_price) / float(entry_price)) if entry_price and exit_price else None
        print(
            f"{_fmt_dt(entry_time)} | {symbol} | {is_b} | {handle} | {exit_reason} | "
            f"{_fmt_num(signal_peak)} | {_fmt_dt(signal_reported_at)} | "
            f"{_fmt_num(observed_peak, 4)} | {_fmt_dt(observed_at)} | "
            f"{_fmt_num(exit_mult, 2)} | {_fmt_num(pnl_sol, 4)}"
        )


if __name__ == "__main__":
    main()
