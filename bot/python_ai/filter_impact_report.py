"""
filter_impact_report.py — Track impact of entry filters and key buckets.

Usage:
    python3 filter_impact_report.py
    python3 filter_impact_report.py --today
    python3 filter_impact_report.py --days 3
    python3 filter_impact_report.py --strategy a
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))

import db


def _build_since(args):
    if args.today:
        return (
            datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0),
            "Today (UTC)",
        )
    if args.days:
        return (datetime.now(timezone.utc) - timedelta(days=args.days), f"Last {args.days} day(s)")
    return (None, "All Time")


def _parse_split(split_raw):
    if not split_raw:
        return None
    value = split_raw.strip()
    fmts = ("%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M", "%Y-%m-%d")
    for fmt in fmts:
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise ValueError(
        "Invalid --split format. Use 'YYYY-MM-DD HH:MM', 'YYYY-MM-DDTHH:MM', or 'YYYY-MM-DD' (UTC)."
    )


def _strategy_sql(strategy):
    if strategy == "a":
        return "AND tp.is_strategy_b = FALSE", "Strategy A"
    if strategy == "b":
        return "AND tp.is_strategy_b = TRUE", "Strategy B"
    return "", "A + B"


def _get_peak_column():
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


def _build_window_clause(alias, time_col, since=None, until=None):
    clauses = []
    params = []
    if since:
        clauses.append(f"{alias}.{time_col} >= %s")
        params.append(since)
    if until:
        clauses.append(f"{alias}.{time_col} < %s")
        params.append(until)
    if not clauses:
        return "", []
    return "AND " + " AND ".join(clauses), params


def _print_blocked_summary(since=None, until=None):
    conn = db.get_conn()
    with conn.cursor() as cur:
        date_clause, params = _build_window_clause("c", "created_at", since=since, until=until)
        cur.execute(
            f"""
            SELECT
                c.skip_reason,
                COUNT(*) AS calls,
                ROUND(AVG(c.conviction_score)::numeric, 1) AS avg_score
            FROM calls c
            WHERE c.skip_reason IN ('vip_low_score', 'low_quality_bucket')
              {date_clause}
            GROUP BY c.skip_reason
            ORDER BY calls DESC
            """,
            params,
        )
        rows = cur.fetchall()

    print("Blocked Calls (new filters)")
    print("-" * 64)
    if not rows:
        print("No blocked calls found in window.")
        return
    print(f"{'skip_reason':<22} {'calls':>8} {'avg_score':>10}")
    for reason, calls, avg_score in rows:
        print(f"{reason:<22} {int(calls):>8} {float(avg_score or 0):>10.1f}")
    print()


def _print_blocked_runner_rates(since=None, until=None):
    peak_col = _get_peak_column()
    if not peak_col:
        print("Blocked Calls Runner Rates")
        print("-" * 64)
        print("outcomes table has no peak multiplier column.")
        print()
        return

    conn = db.get_conn()
    with conn.cursor() as cur:
        date_clause, params = _build_window_clause("c", "created_at", since=since, until=until)
        cur.execute(
            f"""
            SELECT
                c.skip_reason,
                COUNT(*) AS calls,
                COUNT(CASE WHEN o.{peak_col} >= 2 THEN 1 END) AS runner_2x,
                COUNT(CASE WHEN o.{peak_col} >= 3 THEN 1 END) AS runner_3x,
                ROUND(
                    COUNT(CASE WHEN o.{peak_col} >= 2 THEN 1 END) * 100.0
                    / NULLIF(COUNT(*), 0), 1
                ) AS runner_2x_rate,
                ROUND(
                    COUNT(CASE WHEN o.{peak_col} >= 3 THEN 1 END) * 100.0
                    / NULLIF(COUNT(*), 0), 1
                ) AS runner_3x_rate
            FROM calls c
            LEFT JOIN outcomes o ON o.call_id = c.id
            WHERE c.skip_reason IN ('vip_low_score', 'low_quality_bucket')
              {date_clause}
            GROUP BY c.skip_reason
            ORDER BY calls DESC
            """,
            params,
        )
        rows = cur.fetchall()

    print("Blocked Calls Runner Rates")
    print("-" * 64)
    if not rows:
        print("No blocked calls with outcomes found in window.")
        print()
        return
    print(f"{'skip_reason':<22} {'calls':>8} {'2x%':>8} {'3x%':>8}")
    for reason, calls, _, _, r2, r3 in rows:
        print(f"{reason:<22} {int(calls):>8} {float(r2 or 0):>8.1f} {float(r3 or 0):>8.1f}")
    print()


def _print_bucket_performance(since=None, until=None, strategy="all"):
    strat_clause, strat_label = _strategy_sql(strategy)
    conn = db.get_conn()
    with conn.cursor() as cur:
        date_clause, params = _build_window_clause("tp", "exit_time", since=since, until=until)
        cur.execute(
            f"""
            WITH tagged AS (
                SELECT
                    tp.is_strategy_b,
                    tp.pnl_sol,
                    tp.exit_reason,
                    CASE
                        WHEN ch.handle = 'solhousesignal_vip'
                             AND COALESCE(c.conviction_score, 0) < 63
                            THEN 'vip_lt63'
                        WHEN ch.handle LIKE 'solhousesignal%%'
                             AND (t.bundle_pct_remaining IS NULL OR t.bundle_pct_remaining >= 10)
                             AND (t.fake_vol_pct IS NULL OR t.fake_vol_pct >= 5)
                            THEN 'bad_combo'
                        WHEN ch.handle = 'solhousesignal'
                             AND c.conviction_score >= 70
                             AND c.conviction_score < 75
                             AND t.bundle_pct_remaining < 10
                             AND t.fake_vol_pct < 5
                            THEN 'priority_70_74_clean'
                        ELSE 'other'
                    END AS bucket
                FROM trading_positions tp
                JOIN calls c    ON c.id = tp.call_id
                JOIN channels ch ON ch.id = c.channel_id
                JOIN tokens t   ON t.id = c.token_id
                WHERE tp.is_simulation = TRUE
                  {strat_clause}
                  AND tp.status = 'closed'
                  AND COALESCE(tp.exit_reason, '') <> 'data_error'
                  {date_clause}
            )
            SELECT
                CASE WHEN is_strategy_b THEN 'B' ELSE 'A' END AS strategy,
                bucket,
                COUNT(*) AS trades,
                ROUND(SUM(pnl_sol)::numeric, 4) AS pnl,
                ROUND(AVG(pnl_sol)::numeric, 4) AS avg_pnl,
                ROUND(
                    COUNT(CASE WHEN pnl_sol > 0 THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1
                ) AS win_rate,
                ROUND(
                    COUNT(CASE WHEN exit_reason = 'hard_stop' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1
                ) AS hard_stop_rate
            FROM tagged
            GROUP BY strategy, bucket
            ORDER BY strategy, CASE bucket
                WHEN 'bad_combo' THEN 1
                WHEN 'vip_lt63' THEN 2
                WHEN 'priority_70_74_clean' THEN 3
                ELSE 4
            END
            """,
            params,
        )
        rows = cur.fetchall()

    print(f"Closed Trade Performance by Bucket ({strat_label})")
    print("-" * 64)
    if not rows:
        print("No closed paper trades found in window.")
        print()
        return
    print(
        f"{'strat':<6} {'bucket':<22} {'trades':>7} {'pnl':>10} {'avg':>10} {'win%':>8} {'hard%':>8}"
    )
    for strat, bucket, trades, pnl, avg_pnl, win_rate, hard_rate in rows:
        print(
            f"{strat:<6} {bucket:<22} {int(trades):>7} {float(pnl or 0):>10.4f} "
            f"{float(avg_pnl or 0):>10.4f} {float(win_rate or 0):>8.1f} {float(hard_rate or 0):>8.1f}"
        )
    print()


def _print_window(title, since=None, until=None, strategy="all"):
    print("=" * 64)
    print(title)
    print("=" * 64)
    print()
    _print_blocked_summary(since=since, until=until)
    _print_blocked_runner_rates(since=since, until=until)
    _print_bucket_performance(since=since, until=until, strategy=strategy)


def main():
    parser = argparse.ArgumentParser(description="Filter impact report for strategy tuning")
    parser.add_argument("--today", action="store_true", help="UTC day only")
    parser.add_argument("--days", type=int, default=None, help="Last N days")
    parser.add_argument(
        "--strategy",
        choices=["all", "a", "b"],
        default="all",
        help="Closed-trade performance scope",
    )
    parser.add_argument(
        "--split",
        type=str,
        default=None,
        help="UTC split point for pre/post comparison (e.g. '2026-04-15 01:30')",
    )
    args = parser.parse_args()

    since, label = _build_since(args)
    split_dt = _parse_split(args.split) if args.split else None

    if not split_dt:
        _print_window(f"Filter Impact Report — {label}", since=since, until=None, strategy=args.strategy)
        return

    split_label = split_dt.strftime("%Y-%m-%d %H:%M UTC")
    _print_window(
        f"Filter Impact Report (Pre-Split) — {label} to {split_label}",
        since=since,
        until=split_dt,
        strategy=args.strategy,
    )
    _print_window(
        f"Filter Impact Report (Post-Split) — {split_label} onward",
        since=split_dt,
        until=None,
        strategy=args.strategy,
    )


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
