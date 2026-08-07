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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--min", type=int, default=5, help="min calls per lane to display")
    args = ap.parse_args()

    conn = db.get_conn()
    db.safe_rollback()
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
