"""
qsim_floor_checkpoint.py — at the 2x, arm a FLOOR and ride to a checkpoint. What
does the floor actually realize?

THE QUESTION (user's, 2026-10-01)
---------------------------------
bank_2x sells every 2x at 2x. The user's point: watching a coin do 8x, you would
take it at 5x, not hold forever and not sell at 2x. So: pass on the 2x, arm a
floor so a fade still keeps most of the gain, and exit at the next checkpoint.

The arithmetic already says this wins IF the floor holds. With band 2x-reach
rates (n=104: 40.4% to 3x, 23.1% to 5x, 8.7% to 10x) and EV = r*T + (1-r)*F:

    F=0.80 (a plain stop)   3x 1.689   5x 1.770   10x 1.600   <- all LOSE to 2.0
    F=1.55 (a floor)        3x 2.136   5x 2.347   10x 2.285   <- all WIN

That is why qsim_partial_runner found runners negative at all 16 keep x target
combinations: it swept with --stop 0.80. It tested the UNFLOORED version.

Solving for the break-even floor at a 5x target: 0.231*5 + 0.769*F = 2.0 gives
F = 1.098. So the only open question is whether a floor armed at ~1.55 realizes
above ~1.10, or whether coins gap straight through it. That is a question about
DESCENT SPEED, and it is measurable from data already stored.

WHY THIS CAN BE MEASURED AT ALL, AND ONLY HERE
----------------------------------------------
A floor has to observe the way DOWN to fire, and post-exit quotes are 30-60
minutes apart — which is exactly why every trail test in this project was
inconclusive. But qsim has one era, everything before 2026-09-23, with a 0.00%
bank rate: nothing banked at 2x, so every 2x-reacher was HELD PAST ITS 2x with
dense 30s IN-LIFE coverage of the whole descent.

So this file uses IN-LIFE QUOTES ONLY. The sparse-post-exit problem is not
avoided by cleverness, it is avoided by picking the population that does not
have it. Positions whose series ends at the 2x are counted and reported, never
treated as though the floor held.

INTEGRITY
---------
  * The floor realizes the ACTUAL QUOTE that breached it, never the nominal
    level. A series going 1.60 -> 1.20 realizes 1.20. Measuring the gap is the
    entire point; crediting 1.55 there would assume the answer.
  * The target likewise realizes the quote that crossed it.
  * Neither hit -> the LAST OBSERVED in-life quote, win or lose. It never falls
    back to qsim's own result after reading the series. That pairing of upside
    with a protected downside chosen per row is the bor_ free option, +13.27
    until bounded and -112.75 after.
  * Baseline is bank-at-2x realizing the CROSSING quote (>= 2.0, and higher on a
    gap up), because that is what bank_2x actually does.
  * Paired: identical rows in both arms, so no selection is possible.
  * No outcome filters.

    python3 qsim_floor_checkpoint.py --days 21 --sweep
    python3 qsim_floor_checkpoint.py --days 21 --floor 1.55 --target 5
    python3 qsim_floor_checkpoint.py --days 21 --floor 1.55 --target 5 --halves
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))

import db  # noqa: E402


def _positions(days: float, band: bool) -> dict[int, dict]:
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    sql = """
        SELECT qp.call_id, qp.entry_time, qp.exit_time, qp.sol_in, qp.sol_out,
               qp.exit_reason, t.symbol, c.mcap_at_call
        FROM qsim_positions qp
        JOIN tokens t ON t.id = qp.token_id
        JOIN calls  c ON c.id = qp.call_id
        WHERE qp.status = 'closed'
          AND qp.entry_time >= now() - (%s || ' days')::interval
          AND qp.exit_time IS NOT NULL
          AND qp.sol_in > 0
    """
    if band:
        sql += " AND c.mcap_at_call BETWEEN 80000 AND 120000"
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (days,))
        return {int(r["call_id"]): dict(r) for r in cur.fetchall()}


def _inlife(days: float) -> dict[int, list[float]]:
    """IN-LIFE quote multiples per position, time-ordered.

    In-life ONLY (observed_at <= exit_time). The whole design rests on this: the
    population is positions qsim HELD past their 2x, so their descent is covered
    at the dense 30s in-life cadence rather than the 30-60 minute post-exit probe.
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
              AND qp.exit_time IS NOT NULL
              AND o.observed_at <= qp.exit_time
              AND o.real_mult IS NOT NULL
              AND NOT coalesce(o.no_route, false)
            ORDER BY o.call_id, o.observed_at
        """, (days,))
        for r in cur.fetchall():
            out[int(r["call_id"])].append(float(r["real_mult"]))
    return out


def _simulate(series: list[float], floor: float, target: float):
    """(baseline_x, policy_x, why) or None if the position never engages.

    None means either no 2x crossing in-life, or a crossing with no quote after
    it — in both cases the policy had no decision to make and the row is reported
    rather than scored.
    """
    idx = next((i for i, m in enumerate(series) if m >= 2.0), None)
    if idx is None:
        return None
    baseline = series[idx]              # what bank_2x realizes: the crossing quote
    rest = series[idx + 1:]
    if not rest:
        return None                     # nothing observed after the 2x
    for m in rest:
        if target > 0 and m >= target:
            return baseline, m, "target"
        if m <= floor:
            return baseline, m, "floor"  # the ACTUAL breaching quote, not `floor`
    return baseline, rest[-1], "terminal"


def _run(pos: dict, ser: dict, floor: float, target: float) -> dict:
    base_sol = pol_sol = dep = 0.0
    counts: dict[str, int] = defaultdict(int)
    floor_hits: list[float] = []
    rows: list[tuple] = []
    no_series = no_2x = no_room = 0
    for cid, p in pos.items():
        s = ser.get(cid) or []
        if not s:
            no_series += 1
            continue
        res = _simulate(s, floor, target)
        if res is None:
            if any(m >= 2.0 for m in s):
                no_room += 1          # crossed 2x but nothing observed after
            else:
                no_2x += 1
            continue
        b, pl, why = res
        sol_in = float(p["sol_in"])
        dep += sol_in
        base_sol += sol_in * b
        pol_sol += sol_in * pl
        counts[why] += 1
        if why == "floor":
            floor_hits.append(pl)
        rows.append((sol_in * (pl - b), p.get("symbol") or "?", b, pl, why,
                     p.get("entry_time")))
    return {"baseline": base_sol, "policy": pol_sol, "deployed": dep,
            "counts": dict(counts), "floor_hits": floor_hits, "rows": rows,
            "no_series": no_series, "no_2x": no_2x, "no_room": no_room,
            "engaged": len(rows)}


def _med(xs):
    xs = sorted(xs)
    n = len(xs)
    if n == 0:
        return float("nan")
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0


def report(days: float, band: bool, floor: float, target: float,
           sweep: bool, halves: bool, detail: bool) -> None:
    pos = _positions(days, band)
    ser = _inlife(days)
    if not pos:
        print("no closed positions in window")
        return

    label = "mcap band 80-120k" if band else "all mcaps"
    print(f"FLOOR + CHECKPOINT  last {days:g}d   {label}   "
          f"{len(pos)} closed positions")
    print()
    print("  Baseline = bank_2x realizing the crossing quote.")
    print("  Policy   = pass the 2x, arm a floor, exit at the checkpoint, the")
    print("             floor, or the last in-life quote. IN-LIFE QUOTES ONLY.")
    print()

    if sweep:
        hdr = (f"  {'floor':>6}{'target':>8}{'n':>6}{'baseline':>11}{'policy':>10}"
               f"{'delta':>10}{'tgt':>6}{'flr':>6}{'term':>6}{'med flr real':>14}")
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for f in (1.20, 1.35, 1.55, 1.75):
            for t in (3.0, 5.0, 10.0, 0.0):
                r = _run(pos, ser, f, t)
                c = r["counts"]
                fh = _med(r["floor_hits"]) if r["floor_hits"] else float("nan")
                print(f"  {f:>6.2f}{(f'{t:g}x' if t else 'none'):>8}"
                      f"{r['engaged']:>6}{r['baseline']:>11.4f}{r['policy']:>10.4f}"
                      f"{r['policy'] - r['baseline']:>10.4f}"
                      f"{c.get('target', 0):>6}{c.get('floor', 0):>6}"
                      f"{c.get('terminal', 0):>6}{fh:>14.3f}")
        print()
        print("  'med flr real' is the diagnostic: the median multiple the floor")
        print("  ACTUALLY realized. Compare it to the nominal floor column. If it")
        print("  sits far below, coins gap through the floor and the whole idea")
        print("  fails regardless of hit rates. Break-even for a 5x target is")
        print("  ~1.10 (0.231*5 + 0.769*F = 2.0).")
        print()
        print("  target 'none' = floor only, ride until the floor or the last quote.")
        return

    r = _run(pos, ser, floor, target)
    print(f"  floor {floor:g}x   checkpoint "
          f"{(f'{target:g}x' if target else 'none')}")
    print()
    print(f"  closed positions                 {len(pos):>6}")
    print(f"    no in-life quote series        {r['no_series']:>6}")
    print(f"    never reached 2x in-life       {r['no_2x']:>6}")
    print(f"    reached 2x, nothing after it   {r['no_room']:>6}   "
          f"(no decision to make; NOT scored as a floor hold)")
    print(f"    ENGAGED (paired both arms)     {r['engaged']:>6}")
    tot = r["no_series"] + r["no_2x"] + r["no_room"] + r["engaged"]
    assert tot == len(pos), f"stage counts must sum: {tot} != {len(pos)}"
    print(f"  stages sum to total: OK ({tot} = {len(pos)})")
    if not r["engaged"]:
        print("\n  nothing engaged — no question to answer")
        return

    print()
    print(f"  deployed (engaged rows)          {r['deployed']:.3f} SOL")
    print(f"  bank_2x baseline                 {r['baseline']:+.4f} SOL")
    print(f"  floor + checkpoint               {r['policy']:+.4f} SOL")
    print(f"  DIFFERENCE                       {r['policy'] - r['baseline']:+.4f} SOL")
    print(f"  per engaged position             "
          f"{(r['policy'] - r['baseline']) / r['engaged']:+.5f} SOL")
    print()
    print("  how the policy closed:")
    for why, n in sorted(r["counts"].items(), key=lambda kv: -kv[1]):
        sub = [d for d in r["rows"] if d[4] == why]
        print(f"    {why:<10} n={n:<5} delta {sum(d[0] for d in sub):+8.4f} SOL"
              f"   median realized {_med([d[3] for d in sub]):.3f}x")
    if r["floor_hits"]:
        fh = r["floor_hits"]
        print()
        print(f"  FLOOR REALIZATION — nominal {floor:g}x, actual:")
        print(f"    median {_med(fh):.3f}x   worst {min(fh):.3f}x   "
              f"best {max(fh):.3f}x   n={len(fh)}")
        below = sum(1 for x in fh if x < 1.10)
        print(f"    below the 1.10 break-even: {below}/{len(fh)} "
              f"({100.0*below/len(fh):.1f}%)")

    if halves:
        rows = [d for d in r["rows"] if d[5] is not None]
        rows.sort(key=lambda d: d[5])
        mid = len(rows) // 2
        print()
        print("  REPLICATION — split by entry_time. A result that only appears in")
        print("  one half is an era artifact, which is how the 'coarse beats dense'")
        print("  finding died (90% an already-fixed bug, not cadence).")
        for name, chunk in (("first", rows[:mid]), ("second", rows[mid:])):
            if not chunk:
                continue
            d = sum(c[0] for c in chunk)
            print(f"    {name:<7} n={len(chunk):<5} delta {d:+8.4f} SOL   "
                  f"per position {d/len(chunk):+.5f}")

    if detail:
        print()
        print(f"  {'symbol':<14}{'banked_at':>11}{'policy_at':>11}{'why':>10}"
              f"{'delta_sol':>11}")
        for d in sorted(r["rows"], reverse=True)[:20]:
            print(f"  {d[1][:13]:<14}{d[2]:>11.3f}{d[3]:>11.3f}{d[4]:>10}"
                  f"{d[0]:>11.5f}")
        print("  ...")
        for d in sorted(r["rows"])[:10]:
            print(f"  {d[1][:13]:<14}{d[2]:>11.3f}{d[3]:>11.3f}{d[4]:>10}"
                  f"{d[0]:>11.5f}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=float, default=21.0)
    ap.add_argument("--band", action="store_true")
    ap.add_argument("--floor", type=float, default=1.55)
    ap.add_argument("--target", type=float, default=5.0,
                    help="checkpoint multiple (0 = floor only)")
    ap.add_argument("--sweep", action="store_true")
    ap.add_argument("--halves", action="store_true")
    ap.add_argument("--detail", action="store_true")
    a = ap.parse_args()
    try:
        report(a.days, a.band, a.floor, a.target, a.sweep, a.halves, a.detail)
    finally:
        db.close_conn()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
