"""
replay_exit_fix.py — Validate the exit-baseline fix against already-logged data.

The bug: monitor.py judged paper exits against mcap_at_call (the call price) instead
of the trade's own entry_price, so late entries trailed out on the pre-entry pump —
firing trail_stop with the trade-relative peak well below the 2.0x arming threshold.

This replays the NEW exit logic (apply_exit_config with the trade's entry_price +
trade-relative peak, corroboration-guarded) tick-by-tick over the ws_market_observations
already recorded during each closed paper-A position's hold. It reports, per position:

  recorded  — how the trade actually exited (old logic, from the DB)
  replayed  — when/why the NEW logic would exit over the same observed ticks

If the fix works, positions that recorded `trail_stop` at peak < 2.0x should NOT
trail-stop under replay (they hold, or exit via hard_stop on a real drawdown).

No live trades needed; uses only data already in the DB.

Usage:
    python3 replay_exit_fix.py --hours 24
    python3 replay_exit_fix.py --hours 48 --reason trail_stop
"""

from __future__ import annotations

import argparse
import os
import sys

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db
import peak_guard
from exit_config import EXIT_A_PAPER, apply_exit_config


POS_QUERY = """
SELECT
  tp.id, tp.call_id, tok.symbol, tok.mint_address,
  tp.entry_price, tp.entry_time, tp.exit_time,
  tp.peak_multiplier, tp.exit_reason, tp.pnl_pct,
  c.mcap_at_call,
  COALESCE(ch.handle, '') AS channel_handle,
  tp.vip_tier
FROM trading_positions tp
JOIN tokens   tok ON tok.id = tp.token_id
JOIN calls    c   ON c.id   = tp.call_id
LEFT JOIN channels ch ON ch.id = c.channel_id
WHERE tp.is_simulation = TRUE
  AND tp.is_strategy_b = FALSE
  AND tp.status = 'closed'
  AND tp.exit_time IS NOT NULL
  AND tp.entry_time >= now() - (%s || ' hours')::interval
  AND (%s = 'any' OR tp.exit_reason = %s)
ORDER BY tp.entry_time DESC
"""

OBS_QUERY = """
SELECT observed_at, mcap
FROM ws_market_observations
WHERE mint_address = %s
  AND observed_at BETWEEN %s AND %s
  AND mcap IS NOT NULL
ORDER BY observed_at
"""


def replay_one(pos: dict, obs: list[dict]) -> dict:
    """Walk the observed ticks under the NEW (entry-relative, guarded) exit logic."""
    entry = float(pos["entry_price"] or 0)
    if entry <= 0 or not obs:
        return {"replay_reason": "no_data", "replay_tick": None, "replay_mult": None,
                "ticks": len(obs), "max_obs_mult": None}

    key = f"replay:{pos['id']}"
    peak_guard.clear(key)
    is_vip_gamble = pos.get("vip_tier") in ("gamble", "gamble_risk")
    channel = (pos.get("channel_handle") or "").lstrip("@")

    db_peak = 0.0
    max_obs_mult = 0.0
    for i, o in enumerate(obs):
        cur = float(o["mcap"])
        max_obs_mult = max(max_obs_mult, cur / entry)
        # Mirror monitor.py: guard the trade-relative peak.
        peak = peak_guard.guard_peak(key, cur, db_peak)
        db_peak = max(db_peak, peak)
        res = apply_exit_config(
            EXIT_A_PAPER,
            current_mcap=cur,
            peak_mcap=db_peak,
            entry_mcap=entry,
            is_vip_gamble=is_vip_gamble,
            channel_handle=channel,
            entry_time=pos["entry_time"],
        )
        if res.should_exit:
            peak_guard.clear(key)
            return {"replay_reason": res.reason, "replay_tick": i + 1,
                    "replay_mult": round(db_peak / entry, 2),
                    "ticks": len(obs), "max_obs_mult": round(max_obs_mult, 2)}
    peak_guard.clear(key)
    return {"replay_reason": "HELD (no exit)", "replay_tick": None,
            "replay_mult": round(db_peak / entry, 2),
            "ticks": len(obs), "max_obs_mult": round(max_obs_mult, 2)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=24)
    ap.add_argument("--reason", default="trail_stop",
                    help="filter recorded exit_reason, or 'any'")
    args = ap.parse_args()

    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(POS_QUERY, (args.hours, args.reason, args.reason))
        positions = cur.fetchall()

    if not positions:
        print("No matching closed paper-A positions in window.")
        return

    print(f"\nExit-fix replay — last {args.hours}h, recorded reason='{args.reason}'\n")
    hdr = (f"{'sym':<10} {'entry':>8} {'rec_reason':<12} {'rec_pk':>6}  "
           f"-> {'new_reason':<14} {'new_pk':>6} {'tick':>5} {'obs_pk':>6}")
    print(hdr)
    print("-" * len(hdr))

    fixed = same = 0
    for pos in positions:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(OBS_QUERY, (pos["mint_address"], pos["entry_time"], pos["exit_time"]))
            obs = cur.fetchall()
        r = replay_one(pos, obs)
        rec_pk = float(pos["peak_multiplier"] or 0)

        # The bug fingerprint: recorded trail_stop with trade peak < 2.0x.
        # "fixed" = replay does NOT reproduce that premature trail_stop.
        was_premature = pos["exit_reason"] == "trail_stop" and rec_pk < 2.0
        new_is_trail = r["replay_reason"] == "trail_stop"
        if was_premature and not new_is_trail:
            fixed += 1
            mark = "  ✅"
        elif was_premature:
            same += 1
            mark = "  ⚠ still trails"
        else:
            mark = ""

        print(f"{(pos['symbol'] or '?')[:10]:<10} "
              f"{float(pos['entry_price'] or 0):>8.0f} "
              f"{(pos['exit_reason'] or ''):<12} {rec_pk:>6.2f}  -> "
              f"{r['replay_reason']:<14} "
              f"{(r['replay_mult'] if r['replay_mult'] is not None else 0):>6.2f} "
              f"{str(r['replay_tick'] or '-'):>5} "
              f"{(r['max_obs_mult'] if r['max_obs_mult'] is not None else 0):>6.2f}{mark}")

    print("\nSummary")
    print(f"  positions replayed:                 {len(positions)}")
    print(f"  premature trail_stops fixed:        {fixed}")
    print(f"  premature trail_stops still firing: {same}")
    if same == 0 and fixed > 0:
        print("\n  ✅ No recorded sub-2x trail_stop reproduces under the entry-relative logic.")
    elif same > 0:
        print("\n  ⚠️  Some still trail below 2x under replay — investigate those rows.")
    print()


if __name__ == "__main__":
    main()
