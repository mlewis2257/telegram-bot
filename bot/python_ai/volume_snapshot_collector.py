"""
volume_snapshot_collector.py — Capture time-based market snapshots for calls.

Designed to be cron-friendly. Run it every few minutes and it will backfill
the configured snapshot offsets once per call:

    python3 volume_snapshot_collector.py
    python3 volume_snapshot_collector.py --minutes 0,5,15,30,60
    python3 volume_snapshot_collector.py --summary
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import data_fetcher
import db


DEFAULT_SNAPSHOT_MINUTES = (0, 5, 15, 30, 60)


def _parse_minutes(raw: str | None) -> list[int]:
    if not raw:
        return list(DEFAULT_SNAPSHOT_MINUTES)
    values: list[int] = []
    for part in raw.split(","):
        value = int(part.strip())
        if value < 0:
            raise ValueError("snapshot minutes must be non-negative")
        values.append(value)
    return sorted(set(values))


def _collect_snapshot_minute(
    snapshot_minutes: int,
    *,
    since_hours: int,
    limit: int,
    dry_run: bool,
) -> tuple[int, int]:
    due_rows = db.get_due_volume_snapshot_calls(
        snapshot_minutes,
        since_hours=since_hours,
        limit=limit,
    )
    snapshot_label = f"t_plus_{snapshot_minutes:02d}m"
    inserted = 0

    for row in due_rows:
        mint = (row.get("mint_address") or "").strip()
        if not mint or mint.startswith(("INFERRED:", "UNKNOWN:")):
            continue

        market = data_fetcher.fetch_token_price(mint)
        if not market:
            continue

        if dry_run:
            print(
                f"[dry-run] call_id={row['call_id']} label={snapshot_label} "
                f"symbol={row.get('symbol') or '?'} mcap={market.get('mcap')} "
                f"liq={market.get('liquidity_usd')} vol_h1={market.get('volume_h1')}"
            )
            inserted += 1
            continue

        if db.insert_token_volume_snapshot(
            call_id=int(row["call_id"]),
            token_id=int(row["token_id"]),
            mint_address=mint,
            snapshot_label=snapshot_label,
            minutes_since_call=snapshot_minutes,
            created_at=row["created_at"],
            age_minutes=float(row["age_minutes"]) if row.get("age_minutes") is not None else None,
            channel_handle=row.get("channel_handle"),
            vip_tier=row.get("vip_tier"),
            conviction_score=float(row["conviction_score"]) if row.get("conviction_score") is not None else None,
            market=market,
        ):
            inserted += 1

    return len(due_rows), inserted


def _print_summary(days: int) -> None:
    rows = db.get_recent_volume_snapshot_counts(days=days)
    print("=" * 72)
    print(f"Volume Snapshot Coverage — last {days} day(s)")
    print("=" * 72)
    if not rows:
        print("No snapshots found.")
        return
    print(f"{'label':<16} {'snapshots':>10} {'avg_age_min':>12}")
    print("-" * 40)
    for row in rows:
        print(
            f"{row['snapshot_label']:<16} {int(row['snapshots']):>10} "
            f"{float(row['avg_age_minutes'] or 0):>12.1f}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Capture scheduled volume snapshots for recent calls")
    parser.add_argument("--minutes", default=None, help="Comma-separated snapshot offsets, e.g. 0,5,15,30,60")
    parser.add_argument("--since-hours", type=int, default=48, help="Only consider calls newer than this")
    parser.add_argument("--limit", type=int, default=500, help="Per-snapshot fetch limit")
    parser.add_argument("--dry-run", action="store_true", help="Print what would be inserted without writing")
    parser.add_argument("--summary", action="store_true", help="Print recent snapshot coverage and exit")
    parser.add_argument("--summary-days", type=int, default=3, help="Window for --summary")
    args = parser.parse_args()

    db.ensure_token_volume_snapshots_table()

    if args.summary:
        _print_summary(args.summary_days)
        return

    snapshot_minutes = _parse_minutes(args.minutes)
    print("=" * 72)
    print("Volume Snapshot Collector")
    print("=" * 72)
    total_due = 0
    total_inserted = 0
    for minute in snapshot_minutes:
        due, inserted = _collect_snapshot_minute(
            minute,
            since_hours=args.since_hours,
            limit=args.limit,
            dry_run=args.dry_run,
        )
        total_due += due
        total_inserted += inserted
        print(f"t+{minute:02d}m  due={due}  inserted={inserted}")
    print("-" * 72)
    print(f"total_due={total_due}  total_inserted={total_inserted}")


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
