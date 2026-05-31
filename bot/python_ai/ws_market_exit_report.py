"""
ws_market_exit_report.py — Join websocket market observations to paper exits.

This report answers the first audit question after adding Helius tx-derived
market snapshots:

    Did websocket observations see materially different lows/highs than the
    stored paper exit path?

Usage:
    python3 ws_market_exit_report.py --hours 24
    python3 ws_market_exit_report.py --hours 6 --limit 25
"""

from __future__ import annotations

import argparse
import os
import sys

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db


def _fmt_sol(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):+.4f}"


def _fmt_num(value, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.{digits}f}"


def _fmt_mcap(value) -> str:
    if value is None:
        return "n/a"
    return f"${float(value)/1000:.1f}k"


def _load_rows(hours: int) -> list[dict]:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            WITH obs AS (
                SELECT
                    call_id,
                    COUNT(*) AS obs_count,
                    COUNT(*) FILTER (WHERE source = 'helius_tx') AS helius_obs,
                    COUNT(*) FILTER (WHERE source IS DISTINCT FROM 'helius_tx') AS fallback_obs,
                    MIN(mcap) AS min_obs_mcap,
                    MAX(mcap) AS max_obs_mcap,
                    (ARRAY_AGG(mcap ORDER BY observed_at ASC))[1] AS first_obs_mcap,
                    (ARRAY_AGG(mcap ORDER BY observed_at DESC))[1] AS last_obs_mcap,
                    (ARRAY_AGG(observed_at ORDER BY observed_at ASC))[1] AS first_obs_at,
                    (ARRAY_AGG(observed_at ORDER BY observed_at DESC))[1] AS last_obs_at,
                    (ARRAY_AGG(observed_at ORDER BY mcap ASC NULLS LAST, observed_at ASC))[1] AS min_obs_at
                FROM ws_market_observations
                WHERE observed_at >= NOW() - (%s * INTERVAL '1 hour')
                  AND mcap IS NOT NULL
                GROUP BY call_id
            )
            SELECT
                CASE WHEN tp.is_strategy_b THEN 'B' ELSE 'A' END AS strategy,
                tp.call_id,
                t.symbol,
                ch.handle AS channel_handle,
                COALESCE(tp.vip_tier, c.vip_tier, '') AS vip_tier,
                COALESCE(c.conviction_score, 0) AS conviction_score,
                tp.status,
                tp.entry_time,
                tp.exit_time,
                tp.exit_reason,
                tp.entry_price,
                tp.exit_price,
                tp.pnl_sol,
                tp.pnl_pct,
                tp.peak_mcap,
                tp.peak_multiplier,
                obs.obs_count,
                obs.helius_obs,
                obs.fallback_obs,
                obs.first_obs_mcap,
                obs.last_obs_mcap,
                obs.min_obs_mcap,
                obs.max_obs_mcap,
                obs.first_obs_at,
                obs.last_obs_at,
                obs.min_obs_at,
                CASE
                    WHEN tp.entry_price > 0 THEN obs.first_obs_mcap / tp.entry_price
                    ELSE NULL
                END AS first_obs_mult,
                CASE
                    WHEN tp.entry_price > 0 THEN obs.last_obs_mcap / tp.entry_price
                    ELSE NULL
                END AS last_obs_mult,
                CASE
                    WHEN tp.entry_price > 0 THEN obs.min_obs_mcap / tp.entry_price
                    ELSE NULL
                END AS min_obs_mult,
                CASE
                    WHEN tp.entry_price > 0 THEN obs.max_obs_mcap / tp.entry_price
                    ELSE NULL
                END AS max_obs_mult,
                CASE
                    WHEN tp.entry_price > 0 AND tp.exit_price IS NOT NULL THEN tp.exit_price / tp.entry_price
                    ELSE NULL
                END AS exit_mult,
                CASE
                    WHEN tp.exit_time IS NOT NULL THEN
                        EXTRACT(EPOCH FROM (obs.first_obs_at - tp.exit_time))
                    ELSE NULL
                END AS first_obs_vs_exit_sec,
                CASE
                    WHEN tp.exit_time IS NOT NULL THEN
                        EXTRACT(EPOCH FROM (obs.last_obs_at - tp.exit_time))
                    ELSE NULL
                END AS last_obs_vs_exit_sec,
                CASE
                    WHEN tp.exit_time IS NOT NULL THEN
                        EXTRACT(EPOCH FROM (obs.min_obs_at - tp.exit_time))
                    ELSE NULL
                END AS min_obs_vs_exit_sec,
                CASE
                    WHEN COALESCE(tp.vip_tier, c.vip_tier, '') IN ('gamble', 'gamble_risk') THEN 0.70
                    ELSE 0.65
                END AS hard_stop_mult,
                CASE
                    WHEN tp.entry_price > 0 THEN
                        (obs.min_obs_mcap / tp.entry_price) <=
                        CASE
                            WHEN COALESCE(tp.vip_tier, c.vip_tier, '') IN ('gamble', 'gamble_risk') THEN 0.70
                            ELSE 0.65
                        END
                    ELSE FALSE
                END AS ws_touched_hard_stop
            FROM obs
            JOIN trading_positions tp
              ON tp.call_id = obs.call_id
             AND tp.is_simulation = TRUE
            JOIN calls c ON c.id = tp.call_id
            JOIN tokens t ON t.id = c.token_id
            LEFT JOIN channels ch ON ch.id = c.channel_id
            WHERE obs.last_obs_at >= tp.entry_time
              AND obs.first_obs_at <= COALESCE(tp.exit_time, NOW())
            ORDER BY obs.last_obs_at DESC, tp.call_id DESC, strategy ASC
            """,
            (hours,),
        )
        return cur.fetchall()


def _print_summary(rows: list[dict]) -> None:
    by_strategy: dict[str, dict[str, float]] = {}
    for row in rows:
        strategy = row["strategy"]
        stats = by_strategy.setdefault(
            strategy,
            {
                "positions": 0,
                "closed": 0,
                "obs": 0,
                "helius": 0,
                "fallback": 0,
                "obs_before_exit": 0,
                "min_before_exit": 0,
                "hard_touch": 0,
                "hard_touch_not_hard_exit": 0,
            },
        )
        stats["positions"] += 1
        stats["closed"] += 1 if row["status"] == "closed" else 0
        stats["obs"] += int(row["obs_count"] or 0)
        stats["helius"] += int(row["helius_obs"] or 0)
        stats["fallback"] += int(row["fallback_obs"] or 0)
        if row["first_obs_vs_exit_sec"] is not None and float(row["first_obs_vs_exit_sec"]) <= 0:
            stats["obs_before_exit"] += 1
        if row["min_obs_vs_exit_sec"] is not None and float(row["min_obs_vs_exit_sec"]) <= 0:
            stats["min_before_exit"] += 1
        if row["ws_touched_hard_stop"]:
            stats["hard_touch"] += 1
            if row["exit_reason"] != "hard_stop":
                stats["hard_touch_not_hard_exit"] += 1

    print("Summary")
    print("-" * 100)
    print(
        f"{'strategy':<8} {'positions':>9} {'closed':>7} {'obs':>7} "
        f"{'helius':>7} {'fallback':>9} {'obs_pre':>8} {'min_pre':>8} "
        f"{'ws_hard':>8} {'hard_not_exit':>13}"
    )
    for strategy in sorted(by_strategy):
        stats = by_strategy[strategy]
        print(
            f"{strategy:<8} {int(stats['positions']):>9} {int(stats['closed']):>7} "
            f"{int(stats['obs']):>7} {int(stats['helius']):>7} "
            f"{int(stats['fallback']):>9} {int(stats['obs_before_exit']):>8} "
            f"{int(stats['min_before_exit']):>8} {int(stats['hard_touch']):>8} "
            f"{int(stats['hard_touch_not_hard_exit']):>13}"
        )


def _print_rows(rows: list[dict], limit: int) -> None:
    print("\nObserved Positions")
    print("-" * 160)
    print(
        f"{'strat':<5} {'call':>7} {'symbol':<14} {'channel':<20} {'score':>6} "
        f"{'obs':>4} {'src':>9} {'min':>9} {'max':>9} {'last':>9} "
        f"{'minx':>6} {'maxx':>6} {'exitx':>6} {'min_vs_exit':>11} "
        f"{'last_vs_exit':>12} {'reason':<13} {'pnl':>9} {'ws_hard':>7}"
    )
    for row in rows[:limit]:
        source_mix = f"{int(row['helius_obs'] or 0)}/{int(row['fallback_obs'] or 0)}"
        print(
            f"{row['strategy']:<5} {int(row['call_id']):>7} "
            f"{(row['symbol'] or '?')[:14]:<14} {(row['channel_handle'] or '?')[:20]:<20} "
            f"{float(row['conviction_score'] or 0):>6.1f} "
            f"{int(row['obs_count'] or 0):>4} {source_mix:>9} "
            f"{_fmt_mcap(row['min_obs_mcap']):>9} {_fmt_mcap(row['max_obs_mcap']):>9} "
            f"{_fmt_mcap(row['last_obs_mcap']):>9} "
            f"{_fmt_num(row['min_obs_mult']):>6} {_fmt_num(row['max_obs_mult']):>6} "
            f"{_fmt_num(row['exit_mult']):>6} "
            f"{_fmt_num(row['min_obs_vs_exit_sec'], 1):>11} "
            f"{_fmt_num(row['last_obs_vs_exit_sec'], 1):>12} "
            f"{(row['exit_reason'] or row['status'] or '?')[:13]:<13} "
            f"{_fmt_sol(row['pnl_sol']):>9} {str(bool(row['ws_touched_hard_stop'])):>7}"
        )


def _print_timing_notes(rows: list[dict]) -> None:
    closed_rows = [row for row in rows if row["exit_time"] is not None]
    if not closed_rows:
        return
    before_exit = [
        row for row in closed_rows
        if row["first_obs_vs_exit_sec"] is not None and float(row["first_obs_vs_exit_sec"]) <= 0
    ]
    min_before_exit = [
        row for row in closed_rows
        if row["min_obs_vs_exit_sec"] is not None and float(row["min_obs_vs_exit_sec"]) <= 0
    ]
    print("\nTiming Notes")
    print("-" * 100)
    print(
        f"closed_with_observations={len(closed_rows)}  "
        f"first_observation_before_or_at_exit={len(before_exit)}  "
        f"min_observation_before_or_at_exit={len(min_before_exit)}"
    )
    print("Negative timing values mean the websocket observation arrived before the stored paper exit.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Join websocket market observations to paper exits")
    parser.add_argument("--hours", type=int, default=24, help="Lookback window")
    parser.add_argument("--limit", type=int, default=30, help="Detail row limit")
    args = parser.parse_args()

    db.ensure_ws_market_observations_table()
    rows = _load_rows(args.hours)

    print("=" * 100)
    print(f"Websocket Market Exit Report — last {args.hours} hour(s)")
    print("=" * 100)
    if not rows:
        print("No observed paper positions found.")
        return

    _print_summary(rows)
    _print_rows(rows, args.limit)
    _print_timing_notes(rows)


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
