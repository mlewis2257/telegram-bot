"""
qsim_ratchet_trail.py — a trail that TIGHTENS as the coin proves itself.

THE DESIGN BEING TESTED
-----------------------
Not a ladder, not partial sells. One position, sold once:

  * a 20% hard stop (exit at 0.80x) that never goes away
  * a trailing stop below the running peak
  * the trail gets TIGHTER each time the coin clears a checkpoint

        peak < 3x    ->  base trail   (loose: let it breathe)
        peak >= 3x   ->  tighter
        peak >= 5x   ->  tighter still
        peak >= 10x  ->  tightest (protect a monster)

The exit level each tick is max(hard_stop, peak * (1 - trail)), so the stop
governs early and the trail takes over once the coin has run. This is what
`lock_or_bank` and `lock_trail` do NOT do: they lock ONE floor and hold ONE
trail, so a coin at 12x is still protected by a level set when it hit 1.75x.

WHY THIS TEST IS BIASED AGAINST THE POLICY — READ THIS FIRST
------------------------------------------------------------
A trail needs to OBSERVE a retracement to fire. Once qsim sells, it drops the
coin to a low-priority post-exit probe: roughly one quote every 30 minutes
against the 15-90s adaptive cadence a HELD position gets. So this replay makes
the trail exit at whatever sparse quote it happens to see, when a live position
would have seen the path down and trailed out far higher.

Concretely (2026-09-25): MEOWLIN banked 2.526 and the next post-exit quote was
0.191. A live trail would likely have seen intermediate prices and exited well
above that. Here it eats the whole drop.

So the number this prints is a FLOOR on what the policy is worth, not an
estimate. If the ratchet wins here, it wins — the handicap only runs one way.
If it loses narrowly, that is genuinely inconclusive and should be said so.

INTEGRITY
---------
Every row takes its TERMINAL value if neither stop nor trail fires — the last
observed quote, win or lose. No row falls back to qsim's actual stop-protected
result after reading the series. That pairing of post-exit upside with
stop-protected downside is the bor_ free option, which read +13.27 until it was
bounded and became -112.75.

No outcome filter. Positions with no quotes are reported separately, not
absorbed into the comparison.

    python3 qsim_ratchet_trail.py --days 17
    python3 qsim_ratchet_trail.py --days 17 --base 0.50 --t3 0.40 --t5 0.30 --t10 0.20
    python3 qsim_ratchet_trail.py --days 17 --detail
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))

import db  # noqa: E402


def _positions(days: float) -> dict[int, dict]:
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT qp.call_id, qp.sol_in, qp.sol_out, qp.exit_reason, t.symbol
            FROM qsim_positions qp
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.status = 'closed'
              AND qp.entry_time >= now() - (%s || ' days')::interval
              AND qp.sol_in > 0
        """, (days,))
        return {int(r["call_id"]): dict(r) for r in cur.fetchall()}


def _series(days: float) -> dict[int, list[float]]:
    """Every observed price multiple per position, in time order — in-life AND
    post-exit, because this policy deliberately holds longer than qsim did.

    `real_mult` is a verified PRICE multiple (normalized by the bag still held),
    so it stays comparable across a partial bank.
    """
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    out: dict[int, list[float]] = defaultdict(list)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT o.call_id, o.real_mult
            FROM qsim_quote_observations o
            JOIN qsim_positions qp ON qp.call_id = o.call_id
            WHERE qp.status = 'closed'
              AND qp.entry_time >= now() - (%s || ' days')::interval
              AND o.real_mult IS NOT NULL
              AND NOT coalesce(o.no_route, false)
            ORDER BY o.call_id, o.observed_at
        """, (days,))
        for r in cur.fetchall():
            out[int(r["call_id"])].append(float(r["real_mult"]))
    return out


def _trail_for(peak: float, base: float, t3: float, t5: float, t10: float) -> float:
    if peak >= 10.0:
        return t10
    if peak >= 5.0:
        return t5
    if peak >= 3.0:
        return t3
    return base


def _simulate(mults: list[float], stop: float, base: float,
              t3: float, t5: float, t10: float) -> tuple[float, str]:
    """Walk the series; sell on the stop or the ratcheting trail, else terminal.

    The exit level is max(stop, peak * (1 - trail)): the hard stop governs while
    the coin is near entry (a 50% trail off a 1.0 peak sits at 0.50, below the
    0.80 stop), and the trail takes over once the peak has climbed.
    """
    peak = mults[0]
    for m in mults:
        if m > peak:
            peak = m
        trail = _trail_for(peak, base, t3, t5, t10)
        level = max(stop, peak * (1.0 - trail))
        if m <= level:
            return m, ("stop" if level == stop else "trail")
    return mults[-1], "terminal"


def report(days: float, stop: float, base: float, t3: float, t5: float,
           t10: float, detail: bool) -> None:
    pos = _positions(days)
    ser = _series(days)
    resolved, unresolved = [], []
    for cid, p in pos.items():
        sol_in = float(p["sol_in"])
        actual = float(p["sol_out"] or 0.0) - sol_in
        mults = ser.get(cid) or []
        if not mults:
            unresolved.append({**p, "actual": actual})
            continue
        mult, why = _simulate(mults, stop, base, t3, t5, t10)
        resolved.append({**p, "actual": actual, "held": sol_in * mult - sol_in,
                         "mult": mult, "why": why, "n_obs": len(mults),
                         "peak": max(mults)})

    print(f"RATCHET TRAIL  last {days:g}d   stop={stop:g}x   "
          f"trail: base {base:.0%} / 3x {t3:.0%} / 5x {t5:.0%} / 10x {t10:.0%}")
    print(f"  positions: {len(pos)}   resolved: {len(resolved)}   "
          f"no quotes: {len(unresolved)}")
    if not resolved:
        print("  nothing resolved")
        return

    act = sum(x["actual"] for x in resolved)
    held = sum(x["held"] for x in resolved)
    dep = sum(float(x["sol_in"]) for x in resolved)
    print()
    print(f"  deployed                      {dep:.2f} SOL")
    print(f"  actual  (qsim as configured)  {act:+.4f} SOL   {100*act/dep:+.2f}%/SOL")
    print(f"  ratchet trail                 {held:+.4f} SOL   {100*held/dep:+.2f}%/SOL")
    print(f"  DIFFERENCE                    {held - act:+.4f} SOL")
    print()
    for why in ("trail", "stop", "terminal"):
        sub = [x for x in resolved if x["why"] == why]
        if not sub:
            continue
        a = sum(x["actual"] for x in sub)
        h = sum(x["held"] for x in sub)
        print(f"  {why:<9} n={len(sub):<5} actual {a:+9.4f}  ratchet {h:+9.4f}  "
              f"delta {h - a:+9.4f}")

    tiers = (("reached 10x+", 10.0), ("reached 5-10x", 5.0),
             ("reached 3-5x", 3.0), ("reached 2-3x", 2.0), ("never 2x", 0.0))
    print()
    print("  by how far the coin actually ran (peak of all observed quotes):")
    lo_prev = float("inf")
    for label, lo in tiers:
        sub = [x for x in resolved if lo <= x["peak"] < lo_prev]
        lo_prev = lo
        if not sub:
            continue
        a = sum(x["actual"] for x in sub)
        h = sum(x["held"] for x in sub)
        print(f"  {label:<15} n={len(sub):<5} actual {a:+9.4f}  "
              f"ratchet {h:+9.4f}  delta {h - a:+9.4f}")

    if detail:
        print()
        print(f"  {'symbol':<14}{'was':<16}{'peak':>8}{'ratchet_x':>11}"
              f"{'why':>10}{'delta':>10}{'n_obs':>7}")
        for x in sorted(resolved, key=lambda r: r["held"] - r["actual"],
                        reverse=True)[:40]:
            print(f"  {(x['symbol'] or '?')[:13]:<14}{(x['exit_reason'] or '')[:15]:<16}"
                  f"{x['peak']:>8.2f}{x['mult']:>11.3f}{x['why']:>10}"
                  f"{x['held'] - x['actual']:>10.4f}{x['n_obs']:>7}")

    print()
    print("  BIASED AGAINST THE TRAIL: post-exit quotes are ~30 min apart against")
    print("  the 15-90s a held position gets, so the trail eats drops it would")
    print("  really have caught. A win here is a real win; a narrow loss is not")
    print("  a verdict.")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=float, default=17.0)
    ap.add_argument("--stop", type=float, default=0.80)
    ap.add_argument("--base", type=float, default=0.50, help="trail below 3x")
    ap.add_argument("--t3", type=float, default=0.40, help="trail at 3x+")
    ap.add_argument("--t5", type=float, default=0.30, help="trail at 5x+")
    ap.add_argument("--t10", type=float, default=0.20, help="trail at 10x+")
    ap.add_argument("--detail", action="store_true")
    a = ap.parse_args()
    try:
        report(a.days, a.stop, a.base, a.t3, a.t5, a.t10, a.detail)
    finally:
        db.close_conn()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
