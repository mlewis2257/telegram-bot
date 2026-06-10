"""
shadow_monitor.py — standalone exit manager for shadow paper positions.

Runs as its own process, completely separate from monitor.py / ws_monitor.py, so it
CANNOT affect the main strategy. It polls open shadow positions, tracks each one's
peak (corroboration-guarded), and applies the real EXIT_A_PAPER exit logic from the
position's own entry price — exactly like the main paper path, but isolated.

    python3 shadow_monitor.py
    python3 shadow_monitor.py --once     # single pass (for cron)

Env:
    SHADOW_PASS_INTERVAL=15   # seconds between sweeps (loop mode)
    SHADOW_MAX_HOURS=24       # force-close a shadow position with no data after N hours
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

import db
import data_fetcher
import peak_guard
from exit_config import EXIT_A_PAPER, EXIT_RIDE, apply_exit_config

PASS_INTERVAL = float(os.getenv("SHADOW_PASS_INTERVAL", "15"))
MAX_HOURS = float(os.getenv("SHADOW_MAX_HOURS", "24"))
# ride_vol momentum gate. RVOL = (last-5-min volume) / (hourly-average 5-min pace)
#   = volume_m5 / (volume_h1 / 12). >1 = accelerating, <1 = decelerating.
# Keep RIDING while RVOL >= RVOL_HOLD (move still alive on a 5-min view); once the
# 5-min pace fades below it, switch to EARLY exits and bank. Checked every pass.
RVOL_HOLD = float(os.getenv("SHADOW_RVOL_HOLD", "0.8"))

# Static variant -> exit config. 'ride_vol' is dynamic (see _pick_config).
_VARIANT_CONFIGS = {
    "early": EXIT_A_PAPER,
    "ride": EXIT_RIDE,
}


def _aware(ts):
    if ts is not None and ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


async def run_pass() -> int:
    positions = db.get_open_shadow_positions()
    if not positions:
        return 0

    mints = [
        p["mint_address"] for p in positions
        if p.get("mint_address") and not p["mint_address"].startswith(("INFERRED:", "UNKNOWN:"))
    ]
    prices = data_fetcher.fetch_prices_batch_jupiter(mints) if mints else {}

    # Cache mcap per mint within a pass so all variants of the same coin agree.
    mcap_cache: dict[str, float | None] = {}
    vol_cache: dict[str, float | None] = {}

    def _mcap(mint: str):
        if mint in mcap_cache:
            return mcap_cache[mint]
        val = None
        jp = prices.get(mint)
        if jp:
            val = data_fetcher.get_mcap_blended(mint, jp)
        if not val:
            m = data_fetcher.fetch_token_price_fast(mint)
            val = float(m["mcap"]) if m and m.get("mcap") else None
        mcap_cache[mint] = val
        return val

    def _rvol(mint: str):
        # Relative volume = 5-min volume / hourly-average 5-min pace. From DexScreener
        # (Jupiter lacks volume). Cached per pass. None if no usable reading.
        if mint in vol_cache:
            return vol_cache[mint]
        val = None
        try:
            m = data_fetcher.fetch_token_price(mint)
            if m:
                m5 = m.get("volume_m5")
                h1 = m.get("volume_h1")
                if m5 is not None and h1 and float(h1) > 0:
                    val = float(m5) / (float(h1) / 12.0)
        except Exception:
            val = None
        vol_cache[mint] = val
        return val

    def _pick_config(variant: str, mint: str):
        """ride_vol: RIDE while 5-min volume is accelerating, else EARLY. Others: static."""
        if variant != "ride_vol":
            return _VARIANT_CONFIGS.get(variant, EXIT_A_PAPER)
        rv = _rvol(mint)
        if rv is None:
            return EXIT_RIDE          # no reading → keep riding
        return EXIT_RIDE if rv >= RVOL_HOLD else EXIT_A_PAPER

    closed = 0
    for pos in positions:
        call_id = pos["call_id"]
        variant = pos.get("exit_variant") or "early"
        mint = pos.get("mint_address")
        entry = float(pos.get("entry_price") or 0)
        if entry <= 0 or not mint:
            continue
        cfg = _pick_config(variant, mint)
        gkey = f"shadow:{variant}:{call_id}"
        try:
            mcap = _mcap(mint)

            if not mcap or mcap <= 0:
                # No data — force-close as delisted after MAX_HOURS (mirrors paper sweep).
                et = _aware(pos.get("entry_time"))
                if et and (datetime.now(timezone.utc) - et).total_seconds() / 3600 > MAX_HOURS:
                    db.close_shadow_position(call_id, variant, 0, 0, "hard_stop")
                    peak_guard.clear(gkey)
                    closed += 1
                    print(f"[shadow] {pos.get('symbol','?')} [{variant}] delisted — force closed (-100%)")
                continue

            db_peak = float(pos.get("peak_mcap") or 0)
            peak = peak_guard.guard_peak(gkey, mcap, db_peak)
            if peak > db_peak and entry > 0:
                db.update_shadow_position_peak(call_id, variant, peak, peak / entry)

            res = apply_exit_config(
                cfg,
                current_mcap=mcap,
                peak_mcap=max(peak, db_peak),
                entry_mcap=entry,
                is_vip_gamble=(pos.get("vip_tier") in ("gamble", "gamble_risk")),
                channel_handle=(pos.get("channel_handle") or "").lstrip("@"),
                entry_time=_aware(pos.get("entry_time")),
            )
            if res.should_exit:
                exit_mcap = res.exit_mcap or mcap
                sol_out = float(pos["sol_in"]) * (exit_mcap / entry)
                db.close_shadow_position(call_id, variant, exit_mcap, sol_out, res.reason)
                peak_guard.clear(gkey)
                closed += 1
                print(
                    f"[shadow] closed {pos.get('symbol','?')} [{variant}] call_id={call_id}"
                    f" tier={pos.get('vip_tier')} {res.reason} @ {exit_mcap/entry:.2f}x"
                )
        except Exception as e:
            db.safe_rollback()
            print(f"[shadow] exit error call_id={call_id} [{variant}]: {e}")

    return closed


async def main_loop() -> None:
    print(f"[shadow_monitor] started — interval={PASS_INTERVAL}s, max_hours={MAX_HOURS}")
    while True:
        try:
            n = await run_pass()
            if n:
                print(f"[shadow_monitor] closed {n} this pass")
        except Exception as e:
            print(f"[shadow_monitor] pass error: {e}")
        await asyncio.sleep(PASS_INTERVAL)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="single pass then exit (cron mode)")
    args = ap.parse_args()
    db.ensure_shadow_positions_table()
    try:
        if args.once:
            asyncio.run(run_pass())
        else:
            asyncio.run(main_loop())
    finally:
        db.close_conn()


if __name__ == "__main__":
    main()
