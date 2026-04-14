"""
compare_report_b.py — Strategy B paper vs Strategy A paper comparison.

Usage:
    python3 compare_report_b.py              # all-time
    python3 compare_report_b.py --today      # today only
    python3 compare_report_b.py --days 7     # last 7 days
"""

import db
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import os
import sys
import argparse

sys.path.insert(0, os.path.dirname(__file__))


load_dotenv()


def get_stats(is_strategy_b: bool, since=None) -> dict:
    """Get trading stats for one paper strategy, optionally filtered by date."""
    conn = db.get_conn()
    with conn.cursor() as cur:
        date_filter = ""
        params = [is_strategy_b]
        if since:
            date_filter = "AND exit_time >= %s"
            params.append(since)

        cur.execute(
            f"""
            SELECT
                COUNT(*)                                   AS total,
                COUNT(CASE WHEN pnl_sol > 0  THEN 1 END)  AS winners,
                COUNT(CASE WHEN pnl_sol <= 0 THEN 1 END)  AS losers,
                COALESCE(SUM(pnl_sol), 0)                 AS total_pnl,
                COALESCE(AVG(pnl_pct), 0)                 AS avg_pnl_pct,
                MAX(pnl_pct)                               AS best_pct,
                MIN(pnl_pct)                               AS worst_pct
            FROM trading_positions
            WHERE is_simulation = TRUE
              AND is_strategy_b = %s
              AND status = 'closed'
              {date_filter}
            """,
            params,
        )
        row = cur.fetchone()

        open_params = [is_strategy_b]
        cur.execute(
            f"""
            SELECT COUNT(*) FROM trading_positions
            WHERE is_simulation = TRUE
              AND is_strategy_b = %s
              AND status = 'open'
            """,
            open_params,
        )
        open_count = cur.fetchone()[0]

        cur.execute(
            f"""
            SELECT exit_reason, COUNT(*)
            FROM trading_positions
            WHERE is_simulation = TRUE
              AND is_strategy_b = %s
              AND status = 'closed'
              {date_filter}
            GROUP BY exit_reason
            ORDER BY COUNT(*) DESC
            """,
            params,
        )
        breakdown = dict(cur.fetchall())

    total = int(row[0])
    return {
        "total":       total,
        "winners":     int(row[1]),
        "losers":      int(row[2]),
        "win_rate":    round(int(row[1]) / total * 100, 1) if total > 0 else 0,
        "total_pnl":   float(row[3]),
        "avg_pnl_pct": float(row[4]),
        "best_pct":    float(row[5]) if row[5] is not None else 0,
        "worst_pct":   float(row[6]) if row[6] is not None else 0,
        "open":        int(open_count),
        "breakdown":   breakdown,
    }


def get_strategy_overlap(since=None) -> dict:
    """
    Compare Strategy A vs B by call_id for the selected window.
    Helps verify whether both strategies are actually exiting identically.
    """
    conn = db.get_conn()
    with conn.cursor() as cur:
        date_filter = "AND tp.exit_time >= %s" if since else ""
        params = [since] if since else []
        cur.execute(
            f"""
            WITH a AS (
                SELECT tp.call_id, tp.exit_reason, tp.pnl_sol
                FROM trading_positions tp
                WHERE tp.is_simulation = TRUE
                  AND tp.is_strategy_b = FALSE
                  AND tp.status = 'closed'
                  {date_filter}
            ),
            b AS (
                SELECT tp.call_id, tp.exit_reason, tp.pnl_sol
                FROM trading_positions tp
                WHERE tp.is_simulation = TRUE
                  AND tp.is_strategy_b = TRUE
                  AND tp.status = 'closed'
                  {date_filter}
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
        return {
            "a_closed":         int(row[0] or 0),
            "b_closed":         int(row[1] or 0),
            "both_closed":      int(row[2] or 0),
            "diff_exit_reason": int(row[3] or 0),
            "diff_pnl":         int(row[4] or 0),
        }


def print_comparison_b(strategy_b: dict, strategy_a: dict, overlap: dict, label: str) -> None:
    """Print Strategy B and Strategy A paper stats side by side."""
    print(f"{'=' * 60}")
    print(f"  STRATEGY B (PAPER) vs STRATEGY A (PAPER) — {label}")
    print(f"{'=' * 60}")
    print()
    print(f"{'Metric':<20} {'Strategy B':>15} {'Strategy A':>15}")
    print(f"{'-' * 50}")
    print(f"{'Closed trades':<20} {strategy_b['total']:>15} {strategy_a['total']:>15}")
    print(f"{'Open positions':<20} {strategy_b['open']:>15} {strategy_a['open']:>15}")
    print(f"{'Winners':<20} {strategy_b['winners']:>15} {strategy_a['winners']:>15}")
    print(f"{'Losers':<20} {strategy_b['losers']:>15} {strategy_a['losers']:>15}")
    print(
        f"{'Win rate':<20} {strategy_b['win_rate']:>14.1f}% {strategy_a['win_rate']:>14.1f}%")
    print(
        f"{'Total P&L (SOL)':<20} {strategy_b['total_pnl']:>+14.4f} {strategy_a['total_pnl']:>+14.4f}")
    print(
        f"{'Avg P&L %':<20} {strategy_b['avg_pnl_pct']:>+14.1f}% {strategy_a['avg_pnl_pct']:>+14.1f}%")
    print(
        f"{'Best trade %':<20} {strategy_b['best_pct']:>+14.1f}% {strategy_a['best_pct']:>+14.1f}%")
    print(
        f"{'Worst trade %':<20} {strategy_b['worst_pct']:>+14.1f}% {strategy_a['worst_pct']:>+14.1f}%")
    print()

    all_reasons = sorted(
        set(list(strategy_b["breakdown"].keys()) + list(strategy_a["breakdown"].keys()))
    )
    if all_reasons:
        print(f"{'Exit Breakdown':<20} {'Strategy B':>15} {'Strategy A':>15}")
        print(f"{'-' * 50}")
        for reason in all_reasons:
            b = strategy_b["breakdown"].get(reason, 0)
            a = strategy_a["breakdown"].get(reason, 0)
            print(f"  {reason:<18} {b:>15} {a:>15}")

    print()
    print("Overlap check (closed paper trades by call_id):")
    print(f"  both_closed:      {overlap['both_closed']}")
    print(f"  diff_exit_reason: {overlap['diff_exit_reason']}")
    print(f"  diff_pnl:         {overlap['diff_pnl']}")
    if overlap["both_closed"] > 0 and overlap["diff_exit_reason"] == 0 and overlap["diff_pnl"] == 0:
        print("  note: all overlapping closed trades are identical between A and B in this window")

    print(f"{'=' * 60}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Strategy B paper vs Strategy A paper performance comparison")
    parser.add_argument("--today", action="store_true", help="Today only")
    parser.add_argument("--days", type=int, default=None, help="Last N days")
    args = parser.parse_args()

    if args.today:
        since = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        label = "Today"
    elif args.days:
        since = datetime.now(timezone.utc) - timedelta(days=args.days)
        label = f"Last {args.days} days"
    else:
        since = None
        label = "All Time"

    strategy_b = get_stats(is_strategy_b=True, since=since)
    strategy_a = get_stats(is_strategy_b=False, since=since)
    overlap = get_strategy_overlap(since=since)
    print_comparison_b(strategy_b, strategy_a, overlap, label)


if __name__ == "__main__":
    main()
