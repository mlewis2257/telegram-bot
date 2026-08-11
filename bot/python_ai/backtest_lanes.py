"""
backtest_lanes.py — coarse early backtest of 'early' vs 'ride' exits, on the lanes
we already have tick data for.

We only have dense price ticks (ws_market_observations) for calls that were monitored —
which, for gated lanes, means the data_only watchlist lanes (notably gamble_risk /
vip_paused, plus mcap_too_high and safe with no skip) since the observation-logging
fix (~2026-06-08). For every such call this:

  1. enters at the first observed mcap (the price the bot would have bought),
  2. replays BOTH EXIT_A_PAPER ('early') and EXIT_RIDE ('ride') over the tick path,
  3. marks any position still open at the end of data to the last observed mcap,

then aggregates PnL by lane (vip_tier × skip_reason) × variant.

HONEST LIMITS: short history, only the lanes we happened to track, ~24h of ticks per
call (so a 48h 'ride' is capped at the data we have), and peak/exit are only as good as
the logged observations. This is a directional peek, not a verdict — the shadow
experiment is the real, all-lanes answer.

    python3 backtest_lanes.py --days 3
    python3 backtest_lanes.py --days 3 --min 5     # only lanes with >=5 calls
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db
import peak_guard
from exit_config import EXIT_A_PAPER, EXIT_RIDE, EXIT_A_FLOOR, apply_exit_config

NOTIONAL_SOL = 0.5
VARIANTS = {"early": EXIT_A_PAPER, "e_flr": EXIT_A_FLOOR, "ride": EXIT_RIDE}

CALLS_QUERY = """
SELECT DISTINCT
  c.id AS call_id,
  COALESCE(ch.handle, '')        AS channel,
  COALESCE(c.vip_tier, 'none')   AS vip_tier,
  COALESCE(c.skip_reason, 'none') AS skip_reason
FROM ws_market_observations o
JOIN calls c    ON c.id = o.call_id
LEFT JOIN channels ch ON ch.id = c.channel_id
WHERE o.observed_at >= now() - (%s || ' days')::interval
  AND o.call_id IS NOT NULL
"""

OBS_QUERY = """
SELECT mcap
FROM ws_market_observations
WHERE call_id = %s AND mcap IS NOT NULL AND mcap > 0
ORDER BY observed_at
"""


def simulate(variant: str, cfg, entry: float, mcaps: list[float], channel: str, is_vip_gamble: bool, cid: int):
    """Replay one exit config over the tick path. Returns (exit_mult, peak_mult, reason)."""
    key = f"bt:{variant}:{cid}"
    peak_guard.clear(key)
    peak = 0.0
    for cur in mcaps:
        peak = peak_guard.guard_peak(key, cur, peak)
        res = apply_exit_config(
            cfg,
            current_mcap=cur,
            peak_mcap=peak,
            entry_mcap=entry,
            is_vip_gamble=is_vip_gamble,
            channel_handle=channel,
            entry_time=None,          # disable time_stop in backtest
        )
        if res.should_exit:
            peak_guard.clear(key)
            return (res.exit_mcap or cur) / entry, peak / entry, res.reason
    peak_guard.clear(key)
    # never exited within the data we have — mark to last observed price
    return mcaps[-1] / entry, peak / entry, "open_end"


# ── scale-into-strength (the "filter during the trade, not at entry" test) ──────
# You can't tell the monster from the rug at second zero (AUC ~0.60), but monsters
# take a median ~18 min to run, so information accrues AFTER entry. This tests:
# enter a small base on every call, then ADD size to the coins that show early
# strength (up >= threshold within a window). If concentrating capital into
# developing strength lifts edge/SOL over flat entry, the filter works.
OBS_QUERY_T = """
SELECT mcap, observed_at
FROM ws_market_observations
WHERE call_id = %s AND mcap IS NOT NULL AND mcap > 0
ORDER BY observed_at
"""


def _find_exit(cfg, entry, ticks, channel, is_gamble, cid):
    """Walk the tick path with the real exit logic. -> (exit_mcap, reason, t_exit_secs)."""
    key = f"si:{cid}"
    peak_guard.clear(key)
    peak = 0.0
    for cur, t in ticks:
        peak = peak_guard.guard_peak(key, cur, peak)
        res = apply_exit_config(
            cfg, current_mcap=cur, peak_mcap=peak, entry_mcap=entry,
            is_vip_gamble=is_gamble, channel_handle=channel, entry_time=None,
        )
        if res.should_exit:
            peak_guard.clear(key)
            return (res.exit_mcap or cur), res.reason, t
    peak_guard.clear(key)
    return ticks[-1][0], "open_end", ticks[-1][1]


def _find_add(entry, ticks, threshold, window_s, t_exit):
    """First tick within the window (and before exit) where the coin is up >= threshold.
    Returns the mcap you'd add at, or None if strength never showed in time."""
    for cur, t in ticks:
        if t > window_s or t >= t_exit:
            break
        if cur / entry - 1.0 >= threshold:
            return cur
    return None


def run_scalein(conn, args) -> None:
    thresholds = [float(x) for x in args.thresholds.split(",")]
    cfg = EXIT_A_FLOOR if args.exit == "e_flr" else EXIT_A_PAPER
    base_n = args.base_frac * NOTIONAL_SOL   # SOL deployed at entry, always
    add_n = args.add_frac * NOTIONAL_SOL     # SOL added when strength triggers

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(CALLS_QUERY, (args.days,))
        calls = cur.fetchall()

    # Compute each call's exit ONCE (exit is threshold-independent); keep the tick
    # path so we can locate the add point per threshold without re-walking exits.
    lane_calls: dict[tuple, list] = defaultdict(list)
    n_used = 0
    for c in calls:
        cid = c["call_id"]
        with conn.cursor() as cur:
            cur.execute(OBS_QUERY_T, (cid,))
            rows = cur.fetchall()
        if len(rows) < 3:
            continue
        t0 = rows[0][1]
        ticks = [(float(m), (ts - t0).total_seconds()) for m, ts in rows]
        entry = ticks[0][0]
        if entry <= 0:
            continue
        n_used += 1
        chan = (c["channel"] or "").lstrip("@")
        is_gamble = c["vip_tier"] in ("gamble", "gamble_risk")
        exit_mcap, _reason, t_exit = _find_exit(cfg, entry, ticks, chan, is_gamble, cid)
        lane_calls[(chan, c["skip_reason"])].append(
            {"entry": entry, "exit_mcap": exit_mcap, "t_exit": t_exit, "ticks": ticks}
        )

    trig = args.base_frac + args.add_frac
    print(f"\nScale-into-strength backtest — last {args.days}d  ({n_used} calls with usable ticks)")
    print(f"exit={args.exit}  add-window={args.add_window:g}s  base={args.base_frac:g}x  add={args.add_frac:g}x")
    print(f"  → a strong trade deploys {trig:g}x, a weak trade {args.base_frac:g}x (1x = {NOTIONAL_SOL:g} SOL)\n")

    hdr = (f"{'channel':<14} {'lane':<11} {'mode':<9} {'n':>4} {'%add':>5} "
           f"{'tot_sol':>9} {'edge/SOL':>9} {'win%':>5}")
    print(hdr)
    print("-" * len(hdr))

    for (chan_, skip) in sorted(lane_calls):
        rows = lane_calls[(chan_, skip)]
        n = len(rows)
        if n < args.min:
            continue
        # flat baseline: full notional at entry on every call
        f_pnl = sum(NOTIONAL_SOL * (r["exit_mcap"] / r["entry"] - 1.0) for r in rows)
        f_inv = NOTIONAL_SOL * n
        f_win = sum(1 for r in rows if r["exit_mcap"] > r["entry"])
        print(f"{chan_[:14]:<14} {skip[:11]:<11} {'flat':<9} {n:>4} {'--':>5} "
              f"{round(f_pnl,3):>9} {round(f_pnl/f_inv,4):>9} {round(100*f_win/n):>5}")
        for thr in thresholds:
            s_pnl = s_inv = 0.0
            s_win = n_add = 0
            for r in rows:
                mult = r["exit_mcap"] / r["entry"]
                pnl = base_n * (mult - 1.0)
                inv = base_n
                add_mcap = _find_add(r["entry"], r["ticks"], thr, args.add_window, r["t_exit"])
                if add_mcap:
                    n_add += 1
                    pnl += add_n * (r["exit_mcap"] / add_mcap - 1.0)
                    inv += add_n
                s_pnl += pnl
                s_inv += inv
                if pnl > 0:
                    s_win += 1
            tag = f"+{int(thr*100)}%"
            print(f"{'':<14} {'':<11} {tag:<9} {n:>4} {round(100*n_add/n):>5} "
                  f"{round(s_pnl,3):>9} {round(s_pnl/s_inv,4):>9} {round(100*s_win/n):>5}")
        print()

    print("Read: does any '+X%' row beat 'flat' on edge/SOL (return per SOL deployed)?")
    print("edge/SOL is the fair metric — it normalizes for the extra capital scale-in commits.")
    print("If scale-in's edge/SOL <= flat, adding into strength does NOT help: 'early strength'")
    print("is still a coin flip and you paid more to learn it. %add = how often the trigger fired.")
    print("Caveat: exit anchored on original entry; ticks capped at what we logged — directional.\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--min", type=int, default=5, help="min calls per lane to display")
    ap.add_argument("--scalein", action="store_true",
                    help="run the scale-into-strength comparison instead of the exit-variant table")
    ap.add_argument("--exit", choices=["early", "e_flr"], default="early",
                    help="exit config for the scale-in test (default: early = EXIT_A_PAPER)")
    ap.add_argument("--add-window", type=float, default=300.0, dest="add_window",
                    help="seconds: only add if strength shows within this window (default 300)")
    ap.add_argument("--base-frac", type=float, default=0.5, dest="base_frac",
                    help="base position deployed at entry, in units of full notional (default 0.5)")
    ap.add_argument("--add-frac", type=float, default=1.0, dest="add_frac",
                    help="size added when strength triggers, in units of full notional (default 1.0)")
    ap.add_argument("--thresholds", default="0.10,0.20,0.35,0.50",
                    help="comma list of add triggers as fractional gains (default 0.10,0.20,0.35,0.50)")
    args = ap.parse_args()

    conn = db.get_conn()
    db.safe_rollback()

    if args.scalein:
        run_scalein(conn, args)
        return

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(CALLS_QUERY, (args.days,))
        calls = cur.fetchall()

    # agg[(vip_tier, skip_reason, variant)] = list of (exit_mult, peak_mult)
    agg: dict[tuple, list] = defaultdict(list)
    n_used = 0
    for c in calls:
        cid = c["call_id"]
        with conn.cursor() as cur:
            cur.execute(OBS_QUERY, (cid,))
            mcaps = [float(r[0]) for r in cur.fetchall()]
        if len(mcaps) < 3:
            continue
        entry = mcaps[0]
        if entry <= 0:
            continue
        n_used += 1
        is_gamble = c["vip_tier"] in ("gamble", "gamble_risk")
        chan = (c["channel"] or "").lstrip("@")
        for variant, cfg in VARIANTS.items():
            ex, pk, _ = simulate(variant, cfg, entry, mcaps, chan, is_gamble, cid)
            agg[(chan, c["skip_reason"], variant)].append((ex, pk))

    print(f"\nLane backtest — last {args.days}d  ({n_used} calls with usable ticks)\n")
    if not agg:
        print("  No lanes with tick data in window.\n")
        return

    hdr = (f"{'channel':<14} {'lane':<11} {'var':<6} {'n':>4} {'win%':>6} "
           f"{'avg%':>7} {'total_sol':>10} {'avg_pk':>7} {'2x%':>5}")
    print(hdr)
    print("-" * len(hdr))

    # group by channel×lane so early / e_flr / ride sit adjacent
    lanes = sorted({(ch, s) for (ch, s, _) in agg})
    for (chan_, skip) in lanes:
        rows = [(v, agg.get((chan_, skip, v), [])) for v in ("early", "e_flr", "ride")]
        if max(len(r) for _, r in rows) < args.min:
            continue
        for variant, data in rows:
            if not data:
                continue
            n = len(data)
            wins = sum(1 for ex, _ in data if ex > 1.0)
            avg_pct = sum((ex - 1.0) for ex, _ in data) / n * 100
            total_sol = sum(NOTIONAL_SOL * (ex - 1.0) for ex, _ in data)
            avg_pk = sum(pk for _, pk in data) / n
            pct_2x = sum(1 for _, pk in data if pk >= 2.0) / n * 100
            print(f"{chan_[:14]:<14} {skip[:11]:<11} {variant:<6} {n:>4} "
                  f"{round(100*wins/n):>6} {round(avg_pct,1):>7} {round(total_sol,3):>10} "
                  f"{round(avg_pk,2):>7} {round(pct_2x):>5}")
        print()

    print("Read per channel×lane: does 'e_flr' beat 'early' on total_sol / avg%? e_flr = same exit")
    print("but with the profit-floor turned ON for solwhaletrending. If it lifts total_sol without")
    print("gutting avg_pk (i.e. it protects faders without choking runners), enabling the floor helps.")
    print("Caveat: short history, ticks capped at what we logged — directional only.\n")


if __name__ == "__main__":
    main()
