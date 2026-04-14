"""
strategy_diff_report.py — Compare Strategy A vs B closed paper trades by call_id.

Usage:
    python3 strategy_diff_report.py
    python3 strategy_diff_report.py --today
    python3 strategy_diff_report.py --days 3
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))

import db


def _build_since(args) -> tuple[datetime | None, str]:
    if args.today:
        return (
            datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
            "Today (UTC)",
        )
    if args.days:
        return (datetime.now(timezone.utc) - timedelta(days=args.days), f"Last {args.days} day(s)")
    return (None, "All Time")


def _query_diff(since: datetime | None) -> tuple[dict, list[tuple]]:
    conn = db.get_conn()
    with conn.cursor() as cur:
        date_clause = "AND tp.exit_time >= %s" if since else ""
        params = [since] if since else []

        cur.execute(
            f"""
            WITH a AS (
                SELECT tp.call_id, tp.exit_reason, tp.pnl_sol
                FROM trading_positions tp
                WHERE tp.is_simulation = TRUE
                  AND tp.is_strategy_b = FALSE
                  AND tp.status = 'closed'
                  {date_clause}
            ),
            b AS (
                SELECT tp.call_id, tp.exit_reason, tp.pnl_sol
                FROM trading_positions tp
                WHERE tp.is_simulation = TRUE
                  AND tp.is_strategy_b = TRUE
                  AND tp.status = 'closed'
                  {date_clause}
            )
            SELECT
                COUNT(*) FILTER (WHERE a.call_id IS NOT NULL) AS a_closed,
                COUNT(*) FILTER (WHERE b.call_id IS NOT NULL) AS b_closed,
                COUNT(*) FILTER (WHERE a.call_id IS NOT NULL AND b.call_id IS NOT NULL) AS both_closed,
                COUNT(*) FILTER (
                    WHERE a.call_id IS NOT NULL AND b.call_id IS NOT NULL
                      AND COALESCE(a.exit_reason, '') != COALESCE(b.exit_reason, '')
                ) AS diff_exit_reason,
                COUNT(*) FILTER (
                    WHERE a.call_id IS NOT NULL AND b.call_id IS NOT NULL
                      AND COALESCE(a.pnl_sol, 0) != COALESCE(b.pnl_sol, 0)
                ) AS diff_pnl
            FROM a
            FULL OUTER JOIN b ON a.call_id = b.call_id
            """,
            params + params,
        )
        row = cur.fetchone()
        summary = {
            "a_closed": int(row[0] or 0),
            "b_closed": int(row[1] or 0),
            "both_closed": int(row[2] or 0),
            "diff_exit_reason": int(row[3] or 0),
            "diff_pnl": int(row[4] or 0),
        }

        cur.execute(
            f"""
            WITH a AS (
                SELECT tp.call_id, tp.exit_reason, tp.pnl_sol
                FROM trading_positions tp
                WHERE tp.is_simulation = TRUE
                  AND tp.is_strategy_b = FALSE
                  AND tp.status = 'closed'
                  {date_clause}
            ),
            b AS (
                SELECT tp.call_id, tp.exit_reason, tp.pnl_sol
                FROM trading_positions tp
                WHERE tp.is_simulation = TRUE
                  AND tp.is_strategy_b = TRUE
                  AND tp.status = 'closed'
                  {date_clause}
            )
            SELECT
                COALESCE(a.call_id, b.call_id) AS call_id,
                a.exit_reason AS a_reason,
                b.exit_reason AS b_reason,
                a.pnl_sol AS a_pnl,
                b.pnl_sol AS b_pnl
            FROM a
            FULL OUTER JOIN b ON a.call_id = b.call_id
            WHERE a.call_id IS NULL
               OR b.call_id IS NULL
               OR COALESCE(a.exit_reason, '') != COALESCE(b.exit_reason, '')
               OR COALESCE(a.pnl_sol, 0) != COALESCE(b.pnl_sol, 0)
            ORDER BY 1
            LIMIT 30
            """,
            params + params,
        )
        diffs = cur.fetchall()

    return summary, diffs


def main() -> None:
    parser = argparse.ArgumentParser(description="Strategy A/B overlap and divergence report")
    parser.add_argument("--today", action="store_true", help="UTC day only")
    parser.add_argument("--days", type=int, default=None, help="Last N days")
    args = parser.parse_args()

    since, label = _build_since(args)
    summary, diffs = _query_diff(since)

    print("=" * 64)
    print(f"Strategy A vs B Diff Report — {label}")
    print("=" * 64)
    print(f"A closed:         {summary['a_closed']}")
    print(f"B closed:         {summary['b_closed']}")
    print(f"Both closed:      {summary['both_closed']}")
    print(f"Diff exit reason: {summary['diff_exit_reason']}")
    print(f"Diff pnl:         {summary['diff_pnl']}")
    print()

    if not diffs:
        print("No differences found in sampled rows.")
    else:
        print("Sample differences (up to 30 rows):")
        print("call_id | a_reason -> b_reason | a_pnl -> b_pnl")
        for call_id, a_reason, b_reason, a_pnl, b_pnl in diffs:
            print(f"{call_id} | {a_reason} -> {b_reason} | {a_pnl} -> {b_pnl}")


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
