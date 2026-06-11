"""
holder_collector.py — snapshot each coin's top holders (smart-money bootstrap).

For recent calls with no holder snapshot yet, pulls the current top holders via
Helius RPC and stores them in coin_holders. Run it on a cron / loop. Combined with
coin outcomes (peak_multiplier), this is the raw material to retroactively score
which wallets keep holding winners — i.e. build a smart-money signal from our own
data. The scoring step comes later, once a couple weeks of snapshots accumulate.

REQUIRES a real RPC (Helius) in SOLANA_RPC_URL — the public endpoint 429s these calls.

    python3 holder_collector.py                 # one pass
    python3 holder_collector.py --loop          # run continuously
    python3 holder_collector.py --summary

Env:
    HOLDER_TOP_N=20            # holders to capture per coin
    HOLDER_PASS_INTERVAL=60    # seconds between passes (--loop)
    HOLDER_FETCH_DELAY_MS=120  # delay between coins (rate-limit friendliness)
"""

from __future__ import annotations

import argparse
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

import db
import data_fetcher

TOP_N = int(os.getenv("HOLDER_TOP_N", "20"))
PASS_INTERVAL = float(os.getenv("HOLDER_PASS_INTERVAL", "60"))
FETCH_DELAY_MS = int(os.getenv("HOLDER_FETCH_DELAY_MS", "120"))


def run_pass(since_hours: int, limit: int, selection: str, dry_run: bool) -> tuple[int, int, int]:
    due = db.get_calls_needing_holders(since_hours=since_hours, limit=limit, selection=selection)
    captured = skipped = 0
    for row in due:
        mint = (row.get("mint_address") or "").strip()
        if not mint or mint.startswith(("INFERRED:", "UNKNOWN:")):
            skipped += 1
            continue
        holders = data_fetcher.fetch_top_holders_helius(mint, top_n=TOP_N)
        if FETCH_DELAY_MS > 0:
            time.sleep(FETCH_DELAY_MS / 1000.0)
        if not holders:
            skipped += 1
            continue
        if dry_run:
            print(f"[dry-run] {row.get('symbol','?')} call_id={row['call_id']} "
                  f"-> {len(holders)} holders (top owner {holders[0]['wallet'][:6]}…)")
            captured += 1
            continue
        n = db.insert_coin_holders(int(row["call_id"]), mint, holders)
        if n:
            captured += 1
        else:
            skipped += 1
    return len(due), captured, skipped


def _summary() -> None:
    conn = db.get_conn()
    db.safe_rollback()
    from psycopg2.extras import RealDictCursor
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT count(DISTINCT call_id) AS coins,
                   count(*)                AS holder_rows,
                   count(DISTINCT wallet)  AS unique_wallets,
                   max(captured_at)        AS last_capture
            FROM coin_holders
            WHERE captured_at >= now() - interval '7 days'
            """
        )
        r = cur.fetchone()
    print("=" * 60)
    print("Coin holders coverage — last 7d")
    print("=" * 60)
    print(f"  coins snapshotted : {r['coins']}")
    print(f"  holder rows       : {r['holder_rows']}")
    print(f"  unique wallets    : {r['unique_wallets']}")
    print(f"  last capture      : {r['last_capture']}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-hours", type=int, default=48)
    ap.add_argument("--limit", type=int, default=50)
    ap.add_argument("--selection", choices=["traded", "not_skipped", "all"], default="traded")
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--summary", action="store_true")
    args = ap.parse_args()

    db.ensure_coin_holders_table()

    if args.summary:
        _summary()
        return

    def one():
        due, cap, skip = run_pass(args.since_hours, args.limit, args.selection, args.dry_run)
        print(f"[holders] due={due} captured={cap} skipped={skip}")

    try:
        if args.loop:
            print(f"[holder_collector] loop — interval={PASS_INTERVAL}s top_n={TOP_N}")
            while True:
                try:
                    one()
                except Exception as e:
                    print(f"[holder_collector] pass error: {e}")
                time.sleep(PASS_INTERVAL)
        else:
            one()
    finally:
        db.close_conn()


if __name__ == "__main__":
    main()
