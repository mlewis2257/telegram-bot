"""
solhousesignal_score_report.py — Focused Strategy A report for free solhousesignal.

Usage:
    python3 solhousesignal_score_report.py --days 3
    python3 solhousesignal_score_report.py --date-from 2026-05-26 --date-to 2026-05-29
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))

import db


def _parse_dt(value: str) -> datetime:
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise ValueError(f"Unsupported datetime format: {value}")


def _score_bucket(score: float) -> str:
    if score >= 85:
        return "85+"
    if score >= 70:
        return "70-84"
    if score >= 63:
        return "63-69"
    return "<63"


def _fmt_sol(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):+,.4f}"


def _fmt_pct(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):+.1f}%"


def _fmt_ratio(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.2f}x"


def _get_peak_column() -> str | None:
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'outcomes'
              AND column_name IN ('peak_multiplier_from_entry', 'peak_multiplier')
            ORDER BY CASE column_name
                WHEN 'peak_multiplier_from_entry' THEN 1
                WHEN 'peak_multiplier' THEN 2
                ELSE 99
            END
            LIMIT 1
            """
        )
        row = cur.fetchone()
        return row[0] if row else None


def main() -> None:
    parser = argparse.ArgumentParser(description="Strategy A free solhousesignal score-bucket report")
    parser.add_argument("--days", type=int, default=None, help="Last N days by entry_time")
    parser.add_argument("--date-from", dest="date_from", default=None, help="UTC start date/time")
    parser.add_argument("--date-to", dest="date_to", default=None, help="UTC end date/time (exclusive)")
    args = parser.parse_args()

    since = None
    until = None
    label = "All Time"
    if args.days:
        since = datetime.now(timezone.utc) - timedelta(days=args.days)
        label = f"Last {args.days} day(s)"
    if args.date_from:
        since = _parse_dt(args.date_from)
        label = f"Since {args.date_from}"
    if args.date_to:
        until = _parse_dt(args.date_to)
        label = f"{label} until {args.date_to}" if since else f"Until {args.date_to}"

    peak_col = _get_peak_column()
    signal_peak_expr = f"o.{peak_col}" if peak_col else "NULL"
    params: list[object] = []
    time_clauses: list[str] = []
    if since:
        time_clauses.append("tp.entry_time >= %s")
        params.append(since)
    if until:
        time_clauses.append("tp.entry_time < %s")
        params.append(until)
    time_sql = ""
    if time_clauses:
        time_sql = "AND " + " AND ".join(time_clauses)

    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH tagged AS (
                SELECT
                    CASE
                        WHEN c.conviction_score >= 85 THEN '85+'
                        WHEN c.conviction_score >= 70 THEN '70-84'
                        WHEN c.conviction_score >= 63 THEN '63-69'
                        ELSE '<63'
                    END AS score_bucket,
                    tp.pnl_sol,
                    tp.pnl_pct,
                    tp.exit_reason,
                    tp.peak_multiplier AS observed_peak_mult,
                    {signal_peak_expr} AS signal_peak_mult,
                    CASE
                        WHEN tp.entry_price IS NOT NULL AND tp.exit_price IS NOT NULL AND tp.entry_price > 0
                        THEN tp.exit_price / tp.entry_price
                    END AS exit_mult
                FROM trading_positions tp
                JOIN calls c ON c.id = tp.call_id
                JOIN channels ch ON ch.id = c.channel_id
                LEFT JOIN outcomes o ON o.call_id = c.id
                WHERE tp.is_simulation = TRUE
                  AND tp.is_strategy_b = FALSE
                  AND tp.status = 'closed'
                  AND ch.handle = 'solhousesignal'
                  {time_sql}
            )
            SELECT
                score_bucket,
                COUNT(*) AS trades,
                COUNT(*) FILTER (WHERE pnl_sol > 0) AS winners,
                ROUND(
                    COUNT(*) FILTER (WHERE pnl_sol > 0) * 100.0 / NULLIF(COUNT(*), 0),
                    1
                ) AS win_rate,
                ROUND(SUM(pnl_sol)::numeric, 4) AS total_pnl_sol,
                ROUND(AVG(pnl_sol)::numeric, 4) AS avg_pnl_sol,
                ROUND(AVG(pnl_pct)::numeric, 1) AS avg_pnl_pct,
                COUNT(*) FILTER (WHERE exit_reason = 'hard_stop') AS hard_stops,
                ROUND(
                    COUNT(*) FILTER (WHERE exit_reason = 'hard_stop') * 100.0 / NULLIF(COUNT(*), 0),
                    1
                ) AS hard_stop_rate,
                COUNT(*) FILTER (WHERE observed_peak_mult >= 2) AS observed_2x_plus,
                COUNT(*) FILTER (WHERE observed_peak_mult >= 5) AS observed_5x_plus,
                COUNT(*) FILTER (WHERE observed_peak_mult < 1 OR observed_peak_mult IS NULL) AS observed_lt_1x,
                ROUND(AVG(observed_peak_mult)::numeric, 2) AS avg_observed_peak,
                ROUND(AVG(signal_peak_mult)::numeric, 2) AS avg_signal_peak,
                ROUND(AVG(exit_mult)::numeric, 2) AS avg_exit_mult
            FROM tagged
            GROUP BY score_bucket
            ORDER BY CASE score_bucket
                WHEN '85+' THEN 1
                WHEN '70-84' THEN 2
                WHEN '63-69' THEN 3
                ELSE 4
            END
            """,
            params,
        )
        score_rows = cur.fetchall()

        cur.execute(
            f"""
            WITH tagged AS (
                SELECT
                    CASE
                        WHEN c.conviction_score >= 85 THEN '85+'
                        WHEN c.conviction_score >= 70 THEN '70-84'
                        WHEN c.conviction_score >= 63 THEN '63-69'
                        ELSE '<63'
                    END AS score_bucket,
                    COALESCE(tp.exit_reason, 'unknown') AS exit_reason,
                    tp.pnl_sol
                FROM trading_positions tp
                JOIN calls c ON c.id = tp.call_id
                JOIN channels ch ON ch.id = c.channel_id
                WHERE tp.is_simulation = TRUE
                  AND tp.is_strategy_b = FALSE
                  AND tp.status = 'closed'
                  AND ch.handle = 'solhousesignal'
                  {time_sql}
            )
            SELECT
                score_bucket,
                exit_reason,
                COUNT(*) AS trades,
                ROUND(SUM(pnl_sol)::numeric, 4) AS pnl_sol
            FROM tagged
            GROUP BY score_bucket, exit_reason
            ORDER BY CASE score_bucket
                WHEN '85+' THEN 1
                WHEN '70-84' THEN 2
                WHEN '63-69' THEN 3
                ELSE 4
            END, trades DESC, pnl_sol DESC
            """,
            params,
        )
        exit_rows = cur.fetchall()

    print("=" * 96)
    print(f"Strategy A Free Solhousesignal Score Report — {label}")
    print("=" * 96)
    print()
    print(
        f"{'bucket':<10} {'trades':>7} {'win%':>8} {'pnl_sol':>11} {'avg_sol':>11} "
        f"{'avg_%':>8} {'hard%':>8} {'2x+':>6} {'5x+':>6} {'<1x':>6} "
        f"{'obs_peak':>10} {'exit':>8}"
    )
    print("-" * 96)
    for row in score_rows:
        (
            bucket,
            trades,
            winners,
            win_rate,
            total_pnl_sol,
            avg_pnl_sol,
            avg_pnl_pct,
            hard_stops,
            hard_stop_rate,
            observed_2x_plus,
            observed_5x_plus,
            observed_lt_1x,
            avg_observed_peak,
            avg_signal_peak,
            avg_exit_mult,
        ) = row
        print(
            f"{bucket:<10} {int(trades):>7} {float(win_rate or 0):>8.1f} "
            f"{_fmt_sol(total_pnl_sol):>11} {_fmt_sol(avg_pnl_sol):>11} {_fmt_pct(avg_pnl_pct):>8} "
            f"{float(hard_stop_rate or 0):>8.1f} {int(observed_2x_plus or 0):>6} "
            f"{int(observed_5x_plus or 0):>6} {int(observed_lt_1x or 0):>6} "
            f"{_fmt_ratio(avg_observed_peak):>10} {_fmt_ratio(avg_exit_mult):>8}"
        )
    print()

    print("Exit Breakdown by Score Bucket")
    print("-" * 96)
    if not exit_rows:
        print("No rows.")
    else:
        current_bucket = None
        for bucket, exit_reason, trades, pnl_sol in exit_rows:
            if bucket != current_bucket:
                current_bucket = bucket
                print(f"{bucket}:")
            print(f"  {exit_reason:<16} trades={int(trades):<4} pnl={_fmt_sol(pnl_sol)}")


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
