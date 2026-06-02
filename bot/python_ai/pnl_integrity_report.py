"""
pnl_integrity_report.py — sanity-check paper PnL and websocket-backed exits.

This is a read-only audit report for days where paper PnL looks unusually good
or bad. It checks the arithmetic and highlights the rows most likely to explain
the move.

Usage:
    python3 pnl_integrity_report.py --today
    python3 pnl_integrity_report.py --days 2
    python3 pnl_integrity_report.py --today --tz America/Los_Angeles
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db


def _fmt_sol(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):+.4f}"


def _fmt_pct(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):+.1f}%"


def _fmt_mcap(value) -> str:
    if value is None:
        return "n/a"
    value = float(value)
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}m"
    return f"${value / 1000:.1f}k"


def _since_from_args(args) -> tuple[datetime | None, str]:
    if args.today:
        tz = ZoneInfo(args.tz)
        local_now = datetime.now(tz)
        local_midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        return local_midnight.astimezone(timezone.utc), f"today ({args.tz})"
    if args.days:
        return datetime.now(timezone.utc) - timedelta(days=args.days), f"last {args.days} day(s)"
    return None, "all time"


def _date_filter(alias: str, since: datetime | None, params: list) -> str:
    if since is None:
        return ""
    params.append(since)
    return f"AND {alias}.exit_time >= %s"


def _load_summary(since: datetime | None) -> list[dict]:
    conn = db.get_conn()
    params: list = []
    date_sql = _date_filter("tp", since, params)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                CASE WHEN tp.is_strategy_b THEN 'B' ELSE 'A' END AS strategy,
                COUNT(*) AS closed,
                COUNT(*) FILTER (WHERE tp.pnl_sol > 0) AS winners,
                COUNT(*) FILTER (WHERE tp.pnl_sol <= 0) AS losers,
                COALESCE(SUM(tp.pnl_sol), 0) AS pnl_sol,
                COALESCE(AVG(tp.pnl_pct), 0) AS avg_pnl_pct,
                MAX(tp.pnl_pct) AS best_pct,
                MIN(tp.pnl_pct) AS worst_pct,
                COUNT(*) FILTER (
                    WHERE tp.entry_price <= 0
                       OR tp.exit_price IS NULL
                       OR tp.sol_in <= 0
                       OR tp.sol_out IS NULL
                ) AS invalid_math_rows,
                COUNT(*) FILTER (
                    WHERE tp.entry_price > 0
                      AND tp.exit_price IS NOT NULL
                      AND tp.sol_in > 0
                      AND tp.sol_out IS NOT NULL
                      AND ABS(tp.sol_out - (tp.sol_in * (tp.exit_price / tp.entry_price))) > 0.000001
                ) AS sol_out_mismatch_rows
            FROM trading_positions tp
            WHERE tp.is_simulation = TRUE
              AND tp.status = 'closed'
              {date_sql}
            GROUP BY tp.is_strategy_b
            ORDER BY strategy
            """,
            params,
        )
        return cur.fetchall()


def _load_duplicates(since: datetime | None) -> list[dict]:
    conn = db.get_conn()
    params: list = []
    date_sql = _date_filter("tp", since, params)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                CASE WHEN tp.is_strategy_b THEN 'B' ELSE 'A' END AS strategy,
                tp.call_id,
                COUNT(*) AS rows,
                COUNT(*) FILTER (WHERE tp.status = 'closed') AS closed_rows,
                COUNT(*) FILTER (WHERE tp.status = 'open') AS open_rows
            FROM trading_positions tp
            WHERE tp.is_simulation = TRUE
              {date_sql}
            GROUP BY tp.is_strategy_b, tp.call_id
            HAVING COUNT(*) > 1
            ORDER BY rows DESC, tp.call_id DESC
            LIMIT 20
            """,
            params,
        )
        return cur.fetchall()


def _load_extremes(since: datetime | None, limit: int, direction: str) -> list[dict]:
    conn = db.get_conn()
    params: list = []
    date_sql = _date_filter("tp", since, params)
    order = "DESC" if direction == "best" else "ASC"
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                CASE WHEN tp.is_strategy_b THEN 'B' ELSE 'A' END AS strategy,
                tp.call_id,
                COALESCE(t.symbol, '?') AS symbol,
                COALESCE(ch.handle, '?') AS channel,
                COALESCE(c.conviction_score, 0) AS score,
                tp.exit_reason,
                tp.sol_in,
                tp.sol_out,
                tp.pnl_sol,
                tp.pnl_pct,
                tp.entry_price,
                tp.exit_price,
                tp.peak_multiplier,
                tp.entry_time,
                tp.exit_time,
                COALESCE(ws.obs_count, 0) AS ws_obs,
                COALESCE(ws.helius_obs, 0) AS ws_helius_obs,
                ws.min_mcap AS ws_min_mcap,
                ws.max_mcap AS ws_max_mcap,
                ws.last_mcap AS ws_last_mcap,
                near_ws.mcap AS nearest_ws_mcap,
                near_ws.source AS nearest_ws_source,
                ABS(EXTRACT(EPOCH FROM (near_ws.observed_at - tp.exit_time))) AS nearest_ws_abs_sec
            FROM trading_positions tp
            JOIN calls c ON c.id = tp.call_id
            LEFT JOIN tokens t ON t.id = c.token_id
            LEFT JOIN channels ch ON ch.id = c.channel_id
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*) AS obs_count,
                    COUNT(*) FILTER (WHERE source = 'helius_tx') AS helius_obs,
                    MIN(mcap) AS min_mcap,
                    MAX(mcap) AS max_mcap,
                    (ARRAY_AGG(mcap ORDER BY observed_at DESC))[1] AS last_mcap
                FROM ws_market_observations w
                WHERE w.call_id = tp.call_id
                  AND w.mcap IS NOT NULL
                  AND w.observed_at >= tp.entry_time
                  AND w.observed_at <= tp.exit_time + INTERVAL '30 seconds'
            ) ws ON TRUE
            LEFT JOIN LATERAL (
                SELECT w.mcap, w.source, w.observed_at
                FROM ws_market_observations w
                WHERE w.call_id = tp.call_id
                  AND w.mcap IS NOT NULL
                ORDER BY ABS(EXTRACT(EPOCH FROM (w.observed_at - tp.exit_time))) ASC
                LIMIT 1
            ) near_ws ON TRUE
            WHERE tp.is_simulation = TRUE
              AND tp.status = 'closed'
              {date_sql}
            ORDER BY tp.pnl_pct {order} NULLS LAST
            LIMIT %s
            """,
            params + [limit],
        )
        return cur.fetchall()


def _load_exit_breakdown(since: datetime | None) -> list[dict]:
    conn = db.get_conn()
    params: list = []
    date_sql = _date_filter("tp", since, params)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                CASE WHEN tp.is_strategy_b THEN 'B' ELSE 'A' END AS strategy,
                COALESCE(tp.exit_reason, 'unknown') AS exit_reason,
                COUNT(*) AS trades,
                COALESCE(SUM(tp.pnl_sol), 0) AS pnl_sol,
                COALESCE(AVG(tp.pnl_pct), 0) AS avg_pnl_pct
            FROM trading_positions tp
            WHERE tp.is_simulation = TRUE
              AND tp.status = 'closed'
              {date_sql}
            GROUP BY tp.is_strategy_b, tp.exit_reason
            ORDER BY strategy, pnl_sol DESC
            """,
            params,
        )
        return cur.fetchall()


def _print_summary(rows: list[dict]) -> None:
    print("Summary")
    print("-" * 100)
    print(
        f"{'strat':<6} {'closed':>7} {'wins':>6} {'losses':>7} {'pnl':>10} "
        f"{'avg%':>8} {'best%':>9} {'worst%':>9} {'bad_math':>9} {'mismatch':>9}"
    )
    for row in rows:
        print(
            f"{row['strategy']:<6} {int(row['closed']):>7} {int(row['winners']):>6} "
            f"{int(row['losers']):>7} {_fmt_sol(row['pnl_sol']):>10} "
            f"{_fmt_pct(row['avg_pnl_pct']):>8} {_fmt_pct(row['best_pct']):>9} "
            f"{_fmt_pct(row['worst_pct']):>9} {int(row['invalid_math_rows']):>9} "
            f"{int(row['sol_out_mismatch_rows']):>9}"
        )


def _print_exit_breakdown(rows: list[dict]) -> None:
    print("\nExit Breakdown PnL")
    print("-" * 100)
    print(f"{'strat':<6} {'reason':<14} {'trades':>7} {'pnl':>10} {'avg%':>8}")
    for row in rows:
        print(
            f"{row['strategy']:<6} {row['exit_reason']:<14} {int(row['trades']):>7} "
            f"{_fmt_sol(row['pnl_sol']):>10} {_fmt_pct(row['avg_pnl_pct']):>8}"
        )


def _print_duplicates(rows: list[dict]) -> None:
    print("\nDuplicate Position Keys")
    print("-" * 100)
    if not rows:
        print("No duplicate call_id/strategy position groups found.")
        return
    print(f"{'strat':<6} {'call_id':>8} {'rows':>5} {'closed':>7} {'open':>5}")
    for row in rows:
        print(
            f"{row['strategy']:<6} {int(row['call_id']):>8} {int(row['rows']):>5} "
            f"{int(row['closed_rows']):>7} {int(row['open_rows']):>5}"
        )


def _exit_vs_ws_pct(exit_price, nearest_ws_mcap) -> str:
    """Percentage difference between stored exit_price and nearest WS observation."""
    try:
        e = float(exit_price)
        w = float(nearest_ws_mcap)
        if w <= 0:
            return "n/a"
        return f"{(e / w - 1.0) * 100:+.1f}%"
    except (TypeError, ValueError):
        return "n/a"


def _print_extremes(title: str, rows: list[dict]) -> None:
    print(f"\n{title}")
    print("-" * 155)
    print(
        f"{'strat':<5} {'call':>7} {'symbol':<12} {'channel':<18} {'score':>6} "
        f"{'reason':<13} {'pnl':>9} {'pnl%':>9} {'entry':>9} {'exit':>9} "
        f"{'peakx':>7} {'ws':>4} {'helius':>6} {'near_s':>7} {'near_src':<12} {'exit_vs_ws':>10}"
    )
    for row in rows:
        print(
            f"{row['strategy']:<5} {int(row['call_id']):>7} "
            f"{(row['symbol'] or '?')[:12]:<12} {(row['channel'] or '?')[:18]:<18} "
            f"{float(row['score'] or 0):>6.1f} {(row['exit_reason'] or '?')[:13]:<13} "
            f"{_fmt_sol(row['pnl_sol']):>9} {_fmt_pct(row['pnl_pct']):>9} "
            f"{_fmt_mcap(row['entry_price']):>9} {_fmt_mcap(row['exit_price']):>9} "
            f"{float(row['peak_multiplier'] or 0):>7.2f} {int(row['ws_obs'] or 0):>4} "
            f"{int(row['ws_helius_obs'] or 0):>6} "
            f"{float(row['nearest_ws_abs_sec'] or 0):>7.1f} "
            f"{(row['nearest_ws_source'] or '-')[:12]:<12} "
            f"{_exit_vs_ws_pct(row['exit_price'], row['nearest_ws_mcap']):>10}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit paper PnL integrity and extreme rows")
    parser.add_argument("--today", action="store_true", help="From local midnight in --tz")
    parser.add_argument("--days", type=int, default=None, help="Trailing UTC day window")
    parser.add_argument("--tz", default="UTC", help="Timezone for --today, default UTC")
    parser.add_argument("--limit", type=int, default=12, help="Top/bottom row limit")
    args = parser.parse_args()

    since, label = _since_from_args(args)

    print("=" * 100)
    print(f"Paper PnL Integrity Report — {label}")
    if since:
        print(f"Window start UTC: {since.isoformat()}")
    print("=" * 100)

    _print_summary(_load_summary(since))
    _print_exit_breakdown(_load_exit_breakdown(since))
    _print_duplicates(_load_duplicates(since))
    _print_extremes("Best Trades", _load_extremes(since, args.limit, "best"))
    _print_extremes("Worst Trades", _load_extremes(since, args.limit, "worst"))


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
