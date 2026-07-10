"""
exit_monitor.py — dedicated, isolated exit driver for OPEN paper positions.

WHY THIS EXISTS
    Exit-price sampling for open positions used to ride *behind* monitor.py's
    watchlist loop (which stretches as the trending/milestone firehose grows) and
    lean on ws_monitor for density (which dies when the Helius quota maxes). On
    heavy days BOTH starve at exactly the moment the fat-tail runners happen, so
    the paper trader under-observed peaks the shadow caught — e.g. DOG recorded a
    paper peak of 5.0x vs a shadow peak of 17.7x (2026-06-30), and the whole
    solwhale/low_score edge lives in those runners. See memory:
    phantom_peak_root_cause (2026-07-02 note) + shadow_lane_edges.

    shadow_monitor.py proves the cure: a lean, dedicated process that does NOTHING
    but batch-price its open positions every 15s captures those runs. This process
    gives the REAL paper positions the same treatment — a tight, dedicated,
    single-batch exit loop, decoupled from the watchlist AND from ws_monitor health.

DESIGN
    It reuses monitor.run_exit_sweep() so the exit logic (peak ratchet, zero-mark
    guard, guard_trough, lane exit + order_flow, force-close) stays a SINGLE source
    of truth shared with sol-monitor — no duplicated/divergent exit code here.

    include_live=False: this process owns PAPER exit density only. Live on-chain
    SELLS stay owned by sol-monitor + ws_monitor so two processes never race to
    submit the same real sell. Paper closes are idempotent (close only WHERE
    status='open'), and peak writes are a monotonic ratchet, so running this sweep
    concurrently with sol-monitor's own sweep is safe by construction — it can only
    ADD sampling density, never corrupt state.

Usage:
    python3 exit_monitor.py            # runs forever
    python3 exit_monitor.py --once     # single pass (cron/debug)

Env:
    EXIT_MONITOR_INTERVAL=8   # seconds between exit sweeps (dense by design)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

sys.path.insert(0, os.path.dirname(__file__))

import db
import monitor

# Dense by design — this loop does one cheap batched Jupiter call per pass on only
# the open positions, so a tight interval is affordable and is the whole point.
INTERVAL = float(os.getenv("EXIT_MONITOR_INTERVAL", "8"))


async def main_loop() -> None:
    print(f"[exit_monitor] started — interval={INTERVAL}s, paper-only "
          f"(dedicated open-position exit driver)")
    while True:
        try:
            t0 = time.monotonic()
            n = await monitor.run_exit_sweep(include_live=False)
            dur = time.monotonic() - t0
            # Sweep duration is the whole point of the fix: if this creeps toward/past
            # INTERVAL on busy days, the effective exit cadence is stretching and paper
            # falls behind shadow on fast coins. Flag the slow passes loudly.
            tag = "  SLOW ⚠" if dur > INTERVAL else ""
            print(f"[exit_monitor] pass {dur:.1f}s closed={n}{tag}")
        except Exception as e:
            db.safe_rollback()
            print(f"[exit_monitor] pass error: {e}")
        await asyncio.sleep(INTERVAL)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="single pass then exit")
    args = ap.parse_args()
    try:
        if args.once:
            asyncio.run(monitor.run_exit_sweep(include_live=False))
        else:
            asyncio.run(main_loop())
    finally:
        db.close_conn()


if __name__ == "__main__":
    main()
