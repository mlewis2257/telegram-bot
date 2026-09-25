"""
qsim_cadence_sensitivity.py — how much does qsim's exit depend on how often it looks?

THE QUESTION
------------
qsim quotes a position every QSIM_TICK_SECS (30s), stretched by adaptive cadence
to 60s "mid" and 90s "far" — and "far" means far from a trigger, i.e. a coin
sitting at 1.0x. That is exactly the coin that can run to 2.4x and fall back
before anyone looks.

If qsim misses banks it would otherwise take, its P&L reads LOW, and every
decision made against it ("qsim is not profitable, so do not go live") is being
made against a ruler that is built to read low. The qsim->live calibration that
would settle this is n=4.

NOT A ONE-WAY BIAS — this was the first design's mistake
--------------------------------------------------------
db.py says the bias is "one-way (missed banks, never missed losses)", and that
is true for a coin that goes down and STAYS down: you see it on the next quote
whenever that comes. It is NOT true for a transient dip. Looking more often
also catches dips that would have recovered, and the stop then fires on a wick
(see the trail_stop wick finding: ~13% of trail_stops are wick-triggered).

So denser observation buys banks AND costs stops. The net is empirical, which
is the whole reason for this file.

METHOD — PAIRED, SO CADENCE IS THE ONLY VARIABLE
------------------------------------------------
For each position, take its real in-life quote series and SUBSAMPLE it: keep the
first quote, then the next one at least C seconds after the last kept, and so on.
Run the same bank+stop policy over each thinned series. The same coin is priced
at every cadence, so nothing but the sampling rate differs.

    m >= bank -> sell    m <= stop -> sell    else -> last kept quote

WHAT IT CAN AND CANNOT SEE
--------------------------
It can only go COARSER. Quotes that were never taken cannot be invented, so this
measures the slope of P&L against cadence, not the value of a 5s tick. If P&L
degrades steadily as C rises, finer is worth paying for and the current 30-90s
adaptive range is leaving money in the sampler; the size of the gain from going
finer is an EXTRAPOLATION of that slope, not a measurement.

The policy here is a simplified bank+stop, not qsim's full exit stack (no profit
floor, stall, or runner leg). That is deliberate: the same simplified policy runs
at every cadence, so the DIFFERENCE is attributable to sampling. The absolute
P&L column is not a forecast of anything.

NO OUTCOME FILTER. Every position with at least one in-life quote is included.
Positions with a single quote are unaffected by thinning and contribute exactly
zero to every delta — they dilute the average without distorting the comparison.
Requiring N observations would select on survival, which is gotcha #1.

    python3 qsim_cadence_sensitivity.py --days 17
    python3 qsim_cadence_sensitivity.py --days 17 --bank 2.0 --stop 0.80
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))

import db  # noqa: E402

CADENCES = (0, 45, 60, 90, 120, 180)  # 0 = the series qsim actually took


def _positions(days: float) -> dict[int, dict]:
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT call_id, sol_in, exit_time
            FROM qsim_positions
            WHERE status = 'closed'
              AND entry_time >= now() - (%s || ' days')::interval
              AND sol_in > 0 AND exit_time IS NOT NULL
        """, (days,))
        return {int(r["call_id"]): dict(r) for r in cur.fetchall()}


def _in_life_quotes(days: float) -> dict[int, list[tuple[float, float]]]:
    """call_id -> [(seconds_since_first_quote, real_mult)], in-life only.

    `real_mult` is a verified PRICE multiple (normalized by the bag still held),
    so it is comparable across a partial bank — see qsim_floor_cost._held_sol_out
    for the arithmetic that established that.
    """
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    out: dict[int, list[tuple[float, float]]] = defaultdict(list)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT o.call_id,
                   EXTRACT(epoch FROM o.observed_at) AS ts,
                   o.real_mult
            FROM qsim_quote_observations o
            JOIN qsim_positions qp ON qp.call_id = o.call_id
            WHERE qp.status = 'closed'
              AND qp.entry_time >= now() - (%s || ' days')::interval
              AND o.observed_at <= qp.exit_time
              AND o.real_mult IS NOT NULL
              AND NOT coalesce(o.no_route, false)
            ORDER BY o.call_id, o.observed_at
        """, (days,))
        for r in cur.fetchall():
            out[int(r["call_id"])].append((float(r["ts"]), float(r["real_mult"])))
    return out


def _thin(series: list[tuple[float, float]], min_gap: float
          ) -> list[tuple[float, float]]:
    """Keep the first quote, then each quote >= min_gap after the last kept,
    and ALWAYS the last quote of the position's life.

    min_gap 0 returns the series untouched, which is qsim's real sampling.

    THE FINAL QUOTE IS NOT OPTIONAL. Without it, thinning drops whatever falls
    within min_gap of the previous kept quote — and the tail of a position's
    series is disproportionately its death. The first version of this function
    omitted it and produced a +20 SOL improvement from looking LESS often,
    because coarse sampling was truncating positions before they died and
    valuing them at a mid-life price. A position's life ends when it ends; how
    often you looked changes what you SAW, not when it was over.
    """
    if min_gap <= 0 or not series:
        return series
    kept = [series[0]]
    for ts, m in series[1:]:
        if ts - kept[-1][0] >= min_gap:
            kept.append((ts, m))
    if kept[-1][0] != series[-1][0]:
        kept.append(series[-1])
    return kept


def _simulate(series: list[tuple[float, float]], bank: float, stop: float
              ) -> tuple[float, str]:
    for _, m in series:
        if bank > 0 and m >= bank:
            return m, "bank"
        if stop > 0 and m <= stop:
            return m, "stop"
    return series[-1][1], "terminal"


def report(days: float, bank: float, stop: float) -> None:
    pos = _positions(days)
    quotes = _in_life_quotes(days)
    have = [c for c in pos if quotes.get(c)]
    if not have:
        print("no in-life quotes in window")
        return

    dep = sum(float(pos[c]["sol_in"]) for c in have)
    multi = sum(1 for c in have if len(quotes[c]) > 1)

    print(f"CADENCE SENSITIVITY  last {days:g}d   bank={bank:g}x  stop={stop:g}x")
    print(f"  closed positions: {len(pos)}   with in-life quotes: {len(have)}   "
          f"with >1 quote (thinnable): {multi}")
    print(f"  deployed: {dep:.2f} SOL")
    print()
    print(f"  {'cadence':>10}{'banks':>8}{'stops':>8}{'terminal':>10}"
          f"{'med_gap':>9}{'pnl_sol':>10}{'vs actual':>11}")
    print("  " + "-" * 64)

    base = None
    for c in CADENCES:
        pnl = 0.0
        counts = {"bank": 0, "stop": 0, "terminal": 0}
        gaps: list[float] = []
        for cid in have:
            s = _thin(quotes[cid], c)
            mult, why = _simulate(s, bank, stop)
            counts[why] += 1
            pnl += float(pos[cid]["sol_in"]) * (mult - 1.0)
            if len(s) > 1:
                gaps.extend(s[i][0] - s[i - 1][0] for i in range(1, len(s)))
        gaps.sort()
        med = gaps[len(gaps) // 2] if gaps else 0.0
        if base is None:
            base = pnl
        label = "actual" if c == 0 else f">={c}s"
        print(f"  {label:>10}{counts['bank']:>8}{counts['stop']:>8}"
              f"{counts['terminal']:>10}{med:>9.0f}{pnl:>10.3f}{pnl - base:>11.3f}")

    print()
    print("  Read the BANKS and STOPS columns together. Looking more often buys")
    print("  banks and also costs stops, because a transient dip that would have")
    print("  recovered gets seen and sold. If pnl falls steadily as cadence")
    print("  coarsens, a FINER tick is worth paying for — but that direction is an")
    print("  extrapolation of this slope, not a measurement: quotes qsim never")
    print("  took cannot be simulated.")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=float, default=17.0)
    ap.add_argument("--bank", type=float, default=2.0)
    ap.add_argument("--stop", type=float, default=0.80)
    args = ap.parse_args()
    try:
        report(args.days, args.bank, args.stop)
    finally:
        db.close_conn()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
