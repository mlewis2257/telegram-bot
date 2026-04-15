"""
today_calls_audit.py — Audit all calls in a local-day window and show missed runners.

Usage:
    python3 today_calls_audit.py
    python3 today_calls_audit.py --tz America/Los_Angeles
    python3 today_calls_audit.py --date 2026-04-15
"""

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(__file__))

import db


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


def _resolve_window(day_str: str | None, tz_name: str) -> tuple[datetime, datetime, str]:
    tz = ZoneInfo(tz_name)
    if day_str:
        local_day = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=tz)
    else:
        now_local = datetime.now(tz)
        local_day = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    local_end = local_day + timedelta(days=1)
    start_utc = local_day.astimezone(timezone.utc)
    end_utc = local_end.astimezone(timezone.utc)
    label = f"{local_day.strftime('%Y-%m-%d')} ({tz_name})"
    return start_utc, end_utc, label


def _print_table(title: str, headers: list[str], rows: list[tuple], limit: int | None = None) -> None:
    print(title)
    print("-" * 96)
    if not rows:
        print("No rows.")
        print()
        return
    data = rows[:limit] if limit else rows
    widths = [len(h) for h in headers]
    for r in data:
        for i, v in enumerate(r):
            widths[i] = max(widths[i], len(str(v)))
    fmt = "  ".join("{:<" + str(w) + "}" for w in widths)
    print(fmt.format(*headers))
    for r in data:
        print(fmt.format(*[str(v) for v in r]))
    if limit and len(rows) > limit:
        print(f"... ({len(rows) - limit} more rows)")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit today's calls vs traded outcomes")
    parser.add_argument("--tz", default="America/Los_Angeles", help="Local timezone name")
    parser.add_argument("--date", default=None, help="Local date YYYY-MM-DD (defaults to today)")
    parser.add_argument("--limit", type=int, default=50, help="Row limit for detailed sections")
    args = parser.parse_args()

    start_utc, end_utc, label = _resolve_window(args.date, args.tz)
    peak_col = _get_peak_column()

    print("=" * 96)
    print(f"Today Calls Audit — {label}")
    print(f"Window UTC: {start_utc.isoformat()}  ->  {end_utc.isoformat()}")
    print("=" * 96)
    print()

    conn = db.get_conn()
    with conn.cursor() as cur:
        # 1) High-level pipeline counts
        cur.execute(
            """
            WITH calls_w AS (
                SELECT c.id
                FROM calls c
                WHERE c.created_at >= %s AND c.created_at < %s
            ),
            traded AS (
                SELECT DISTINCT tp.call_id
                FROM trading_positions tp
                WHERE tp.is_simulation = TRUE
            )
            SELECT
                (SELECT COUNT(*) FROM calls_w) AS calls_total,
                (SELECT COUNT(*) FROM calls c
                 WHERE c.created_at >= %s AND c.created_at < %s
                   AND c.skip_reason IS NOT NULL) AS calls_skipped,
                (SELECT COUNT(*) FROM calls c
                 WHERE c.created_at >= %s AND c.created_at < %s
                   AND c.skip_reason IS NULL) AS calls_not_skipped,
                (SELECT COUNT(*) FROM calls_w cw JOIN traded t ON t.call_id = cw.id) AS calls_traded
            """,
            (start_utc, end_utc, start_utc, end_utc, start_utc, end_utc),
        )
        total, skipped, not_skipped, traded = cur.fetchone()
        print(f"calls_total={int(total)}  skipped={int(skipped)}  not_skipped={int(not_skipped)}  traded={int(traded)}")
        print()

        # 2) Skip reasons
        cur.execute(
            """
            SELECT COALESCE(c.skip_reason, 'none') AS skip_reason, COUNT(*) AS calls
            FROM calls c
            WHERE c.created_at >= %s AND c.created_at < %s
            GROUP BY COALESCE(c.skip_reason, 'none')
            ORDER BY calls DESC
            """,
            (start_utc, end_utc),
        )
        _print_table("Skip Reason Breakdown", ["skip_reason", "calls"], cur.fetchall())

        # 3) Channel x score band x skip
        cur.execute(
            """
            SELECT
                ch.handle,
                CASE
                    WHEN c.conviction_score >= 85 THEN '85+'
                    WHEN c.conviction_score >= 75 THEN '75-84'
                    WHEN c.conviction_score >= 70 THEN '70-74'
                    WHEN c.conviction_score >= 63 THEN '63-69'
                    ELSE '<63'
                END AS score_band,
                COALESCE(c.skip_reason, 'none') AS skip_reason,
                COUNT(*) AS calls
            FROM calls c
            JOIN channels ch ON ch.id = c.channel_id
            WHERE c.created_at >= %s AND c.created_at < %s
            GROUP BY 1,2,3
            ORDER BY calls DESC
            """,
            (start_utc, end_utc),
        )
        _print_table(
            "Channel / Score / Skip Buckets",
            ["channel", "score_band", "skip_reason", "calls"],
            cur.fetchall(),
            limit=args.limit,
        )

        # 4) Missed runners (calls not traded OR skipped) with peak >= 2x
        if peak_col:
            cur.execute(
                f"""
                WITH traded AS (
                    SELECT DISTINCT tp.call_id
                    FROM trading_positions tp
                    WHERE tp.is_simulation = TRUE
                )
                SELECT
                    c.id AS call_id,
                    c.created_at,
                    ch.handle,
                    COALESCE(c.vip_tier, 'none') AS vip_tier,
                    ROUND(COALESCE(c.conviction_score, 0)::numeric, 1) AS score,
                    COALESCE(c.skip_reason, 'none') AS skip_reason,
                    ROUND(COALESCE(t.bundle_pct_remaining, -1)::numeric, 2) AS bundle_pct,
                    ROUND(COALESCE(t.fake_vol_pct, -1)::numeric, 2) AS fake_pct,
                    ROUND(o.{peak_col}::numeric, 2) AS peak_mult
                FROM calls c
                JOIN channels ch ON ch.id = c.channel_id
                JOIN tokens t ON t.id = c.token_id
                JOIN outcomes o ON o.call_id = c.id
                LEFT JOIN traded tr ON tr.call_id = c.id
                WHERE c.created_at >= %s AND c.created_at < %s
                  AND o.{peak_col} >= 2
                  AND (tr.call_id IS NULL OR c.skip_reason IS NOT NULL)
                ORDER BY o.{peak_col} DESC, c.created_at DESC
                """,
                (start_utc, end_utc),
            )
            _print_table(
                "Missed Runners (peak>=2x, skipped or not traded)",
                ["call_id", "created_at", "channel", "vip_tier", "score", "skip_reason", "bundle_pct", "fake_pct", "peak_mult"],
                cur.fetchall(),
                limit=args.limit,
            )

            # 5) Traded runners for context
            cur.execute(
                f"""
                WITH traded AS (
                    SELECT DISTINCT tp.call_id
                    FROM trading_positions tp
                    WHERE tp.is_simulation = TRUE
                )
                SELECT
                    c.id AS call_id,
                    c.created_at,
                    ch.handle,
                    ROUND(COALESCE(c.conviction_score, 0)::numeric, 1) AS score,
                    ROUND(o.{peak_col}::numeric, 2) AS peak_mult
                FROM calls c
                JOIN channels ch ON ch.id = c.channel_id
                JOIN outcomes o ON o.call_id = c.id
                JOIN traded tr ON tr.call_id = c.id
                WHERE c.created_at >= %s AND c.created_at < %s
                  AND o.{peak_col} >= 2
                ORDER BY o.{peak_col} DESC, c.created_at DESC
                """,
                (start_utc, end_utc),
            )
            _print_table(
                "Traded Runners (peak>=2x)",
                ["call_id", "created_at", "channel", "score", "peak_mult"],
                cur.fetchall(),
                limit=args.limit,
            )
        else:
            print("outcomes peak multiplier column not found; skipping runner sections.")


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
