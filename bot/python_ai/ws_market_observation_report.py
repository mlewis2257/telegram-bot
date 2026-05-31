"""
ws_market_observation_report.py — Summarize websocket market observation sources.

Usage:
    python3 ws_market_observation_report.py
    python3 ws_market_observation_report.py --hours 6
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import db


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize websocket market observations")
    parser.add_argument("--hours", type=int, default=24, help="Lookback window")
    args = parser.parse_args()

    db.ensure_ws_market_observations_table()
    rows = db.get_recent_ws_market_observation_counts(hours=args.hours)

    print("=" * 72)
    print(f"Websocket Market Observations — last {args.hours} hour(s)")
    print("=" * 72)
    if not rows:
        print("No observations found.")
        return

    print(f"{'source':<18} {'observations':>12} {'calls':>8} {'avg_mcap':>14} {'last_seen'}")
    print("-" * 72)
    for row in rows:
        avg_mcap = float(row["avg_mcap"] or 0)
        print(
            f"{row['source']:<18} {int(row['observations']):>12} "
            f"{int(row['calls']):>8} {avg_mcap:>14.2f} {row['last_seen']}"
        )


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
