"""
qsim_partial_runner.py — bank most of it at 2x, let a slice run to a TARGET.

THE POLICY
----------
    first quote >= bank      -> sell (1 - keep) of the position there
    the remaining `keep`     -> run on, and exit at whichever comes first:
                                  a quote >= target      (the checkpoint)
                                  a quote <= stop        (still protected)
                                  the last quote observed (terminal)

Positions that never reach `bank` are untouched and score exactly what qsim
booked, so they enter both arms identically and contribute nothing to the delta.

WHY THIS IS MEASURABLE WHERE THE TRAIL WAS NOT
-----------------------------------------------
qsim_ratchet_trail could not settle its question because a TRAIL has to observe
the path DOWN to fire, and post-exit quotes are 30-60 minutes apart. A trail on
that data exits at whatever sparse print it happens to see, which is why the
same dataset priced the profit floor at -9.05 when the live config books +4.13
on identical positions.

A TARGET has no such problem. "This coin printed 5x at some point" is a single
observation. Sparse sampling can only make you MISS a target that really
happened; it can never invent one. So every number here is a FLOOR on what the
policy is worth, and the handicap runs one way only.

The runner's stop and its terminal fallback are the two legs that a sparse
series can still distort, and both distort AGAINST the policy: a missed dip
means you exit lower later, and a terminal is whatever the last sparse quote
says. So the bias is consistent.

INTEGRITY
---------
A runner that hits neither target nor stop takes the LAST OBSERVED QUOTE, win or
lose. It never falls back to qsim's own result after reading the series — that
pairing of post-exit upside with stop-protected downside is the bor_ free
option, +13.27 until bounded and -112.75 after.

No outcome filter. Positions with no observations are reported, not absorbed.

    python3 qsim_partial_runner.py --days 17
    python3 qsim_partial_runner.py --days 17 --keep 0.20 --target 5.0
    python3 qsim_partial_runner.py --days 17 --sweep
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
    """Every observed price multiple per position in time order, in-life AND
    post-exit. `real_mult` is a verified PRICE multiple, normalized by the bag
    still held, so it stays comparable across a partial bank."""
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


def _simulate(mults: list[float], bank: float, keep: float,
              target: float, stop: float) -> tuple[float, str] | None:
    """Blended multiple for the whole position, and what closed the runner.

    None means the coin never reached `bank`, so the policy did nothing and the
    row keeps qsim's actual result.
    """
    idx = next((i for i, m in enumerate(mults) if m >= bank), None)
    if idx is None:
        return None
    bank_mult = mults[idx]
    rest = mults[idx + 1:]
    if not rest:
        # Banked on the last quote there is; the runner has nowhere to go and
        # is marked at the same price rather than assumed to survive.
        return bank_mult, "no_runner_data"
    for m in rest:
        if target > 0 and m >= target:
            return (1 - keep) * bank_mult + keep * m, "target"
        if stop > 0 and m <= stop:
            return (1 - keep) * bank_mult + keep * m, "stop"
    return (1 - keep) * bank_mult + keep * rest[-1], "terminal"


def _run(pos: dict, ser: dict, bank: float, keep: float,
         target: float, stop: float) -> dict:
    act = sh = dep = 0.0
    counts: dict[str, int] = defaultdict(int)
    deltas: list[tuple] = []
    engaged = 0
    for cid, p in pos.items():
        sol_in = float(p["sol_in"])
        a = float(p["sol_out"] or 0.0) - sol_in
        act += a
        dep += sol_in
        mults = ser.get(cid) or []
        res = _simulate(mults, bank, keep, target, stop) if mults else None
        if res is None:
            sh += a                      # never reached the bank: identical arms
            continue
        mult, why = res
        h = sol_in * mult - sol_in
        sh += h
        engaged += 1
        counts[why] += 1
        deltas.append((h - a, p.get("symbol") or "?", mult, why, max(mults)))
    return {"actual": act, "policy": sh, "deployed": dep, "engaged": engaged,
            "counts": dict(counts), "deltas": deltas}


def report(days: float, bank: float, keep: float, target: float,
           stop: float, detail: bool, sweep: bool) -> None:
    pos = _positions(days)
    ser = _series(days)
    if not pos:
        print("no closed positions in window")
        return
    missing = sum(1 for cid in pos if not ser.get(cid))

    if sweep:
        print(f"SWEEP  last {days:g}d   bank={bank:g}x  stop={stop:g}x   "
              f"({len(pos)} positions, {missing} with no quotes)")
        print(f"  {'keep':>6}{'target':>8}{'engaged':>9}{'actual':>10}"
              f"{'policy':>10}{'delta':>10}")
        for k in (0.10, 0.20, 0.30, 0.50):
            for t in (3.0, 5.0, 10.0, 0.0):
                r = _run(pos, ser, bank, k, t, stop)
                label = f"{t:g}x" if t > 0 else "none"
                print(f"  {k:>6.0%}{label:>8}{r['engaged']:>9}"
                      f"{r['actual']:>10.3f}{r['policy']:>10.3f}"
                      f"{r['policy'] - r['actual']:>10.3f}")
        print()
        print("  target 'none' = the runner only ever exits on the stop or on")
        print("  the last quote, which is the pure hold-longer case.")
        return

    r = _run(pos, ser, bank, keep, target, stop)
    print(f"PARTIAL RUNNER  last {days:g}d   bank {bank:g}x, keep {keep:.0%} "
          f"running to {target:g}x   stop={stop:g}x")
    print(f"  positions: {len(pos)}   reached the bank: {r['engaged']}   "
          f"no quotes: {missing}")
    print()
    print(f"  deployed                 {r['deployed']:.2f} SOL")
    print(f"  actual (qsim booked)     {r['actual']:+.4f} SOL   "
          f"{100*r['actual']/r['deployed']:+.2f}%/SOL")
    print(f"  partial runner           {r['policy']:+.4f} SOL   "
          f"{100*r['policy']/r['deployed']:+.2f}%/SOL")
    print(f"  DIFFERENCE               {r['policy'] - r['actual']:+.4f} SOL")
    print()
    print("  how the runner closed:")
    for why, n in sorted(r["counts"].items(), key=lambda kv: -kv[1]):
        sub = [d for d in r["deltas"] if d[3] == why]
        print(f"    {why:<16} n={n:<5} delta {sum(d[0] for d in sub):+8.4f}")

    if detail:
        print()
        print(f"  {'symbol':<14}{'blended_x':>11}{'why':>16}{'peak':>9}{'delta':>10}")
        for d in sorted(r["deltas"], reverse=True)[:25]:
            print(f"  {d[1][:13]:<14}{d[2]:>11.3f}{d[3]:>16}{d[4]:>9.2f}{d[0]:>10.4f}")
        print("  ...")
        for d in sorted(r["deltas"])[:10]:
            print(f"  {d[1][:13]:<14}{d[2]:>11.3f}{d[3]:>16}{d[4]:>9.2f}{d[0]:>10.4f}")

    print()
    print("  A TARGET is robust to sparse quoting in a way a trail is not: a")
    print("  missed quote can only hide a target that happened, never invent")
    print("  one. So this is a FLOOR on the policy, not an estimate.")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=float, default=17.0)
    ap.add_argument("--bank", type=float, default=2.0)
    ap.add_argument("--keep", type=float, default=0.20,
                    help="fraction left running after the bank")
    ap.add_argument("--target", type=float, default=3.0,
                    help="runner's exit target (0 = no target)")
    ap.add_argument("--stop", type=float, default=0.80)
    ap.add_argument("--detail", action="store_true")
    ap.add_argument("--sweep", action="store_true",
                    help="grid over keep x target")
    a = ap.parse_args()
    try:
        report(a.days, a.bank, a.keep, a.target, a.stop, a.detail, a.sweep)
    finally:
        db.close_conn()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
