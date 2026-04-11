"""
compare_report_b.py — Strategy B paper vs live performance comparison.

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


def get_stats_b(is_simulation: bool, since=None) -> dict:
    """Get trading stats for Strategy B paper or live, optionally filtered by date."""
    conn = db.get_conn()
    with conn.cursor() as cur:
        date_filter = ""
        strat_filter = "AND is_strategy_b = TRUE" if is_simulation else ""
        params = [is_simulation]
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
            WHERE is_simulation = %s
              {strat_filter}
              AND status = 'closed'
              {date_filter}
            """,
            params,
        )
        row = cur.fetchone()

        open_params = [is_simulation]
        open_strat_filter = "AND is_strategy_b = TRUE" if is_simulation else ""
        cur.execute(
            f"""
            SELECT COUNT(*) FROM trading_positions
            WHERE is_simulation = %s {open_strat_filter} AND status = 'open'
            """,
            open_params,
        )
        open_count = cur.fetchone()[0]

        cur.execute(
            f"""
            SELECT exit_reason, COUNT(*)
            FROM trading_positions
            WHERE is_simulation = %s AND is_strategy_b = TRUE
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


def print_comparison_b(paper: dict, live: dict, label: str) -> None:
    """Print Strategy B paper and live stats side by side."""
    print(f"{'=' * 60}")
    print(f"  STRATEGY B (PAPER) vs STRATEGY A (PAPER) — {label}")
    print(f"{'=' * 60}")
    print()
    print(f"{'Metric':<20} {'Strategy B':>15} {'Live':>15}")
    print(f"{'-' * 50}")
    print(f"{'Closed trades':<20} {paper['total']:>15} {live['total']:>15}")
    print(f"{'Open positions':<20} {paper['open']:>15} {live['open']:>15}")
    print(f"{'Winners':<20} {paper['winners']:>15} {live['winners']:>15}")
    print(f"{'Losers':<20} {paper['losers']:>15} {live['losers']:>15}")
    print(
        f"{'Win rate':<20} {paper['win_rate']:>14.1f}% {live['win_rate']:>14.1f}%")
    print(
        f"{'Total P&L (SOL)':<20} {paper['total_pnl']:>+14.4f} {live['total_pnl']:>+14.4f}")
    print(
        f"{'Avg P&L %':<20} {paper['avg_pnl_pct']:>+14.1f}% {live['avg_pnl_pct']:>+14.1f}%")
    print(
        f"{'Best trade %':<20} {paper['best_pct']:>+14.1f}% {live['best_pct']:>+14.1f}%")
    print(
        f"{'Worst trade %':<20} {paper['worst_pct']:>+14.1f}% {live['worst_pct']:>+14.1f}%")
    print()

    all_reasons = sorted(
        set(list(paper["breakdown"].keys()) + list(live["breakdown"].keys()))
    )
    if all_reasons:
        print(f"{'Exit Breakdown':<20} {'Strategy B':>15} {'Live':>15}")
        print(f"{'-' * 50}")
        for reason in all_reasons:
            p = paper["breakdown"].get(reason, 0)
            lv = live["breakdown"].get(reason, 0)
            print(f"  {reason:<18} {p:>15} {lv:>15}")

    print(f"{'=' * 60}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Strategy B paper vs live performance comparison")
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

    paper = get_stats_b(is_simulation=True,  since=since)
    live = get_stats_b(is_simulation=False, since=since)
    print_comparison_b(paper, live, label)


if __name__ == "__main__":
    main()
