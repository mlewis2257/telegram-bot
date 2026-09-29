"""
qsim_early_abort.py — if the first few ticks are bad, does bailing beat holding?

THE IDEA
--------
70% of this book exits on the hard stop, and a stop realises ~0.65 because coins
gap. If a position that is going to die is already sagging on its first two or
three quotes, selling there at ~0.95 instead of riding to a 0.65 fill saves
about thirty points a trade. That is worth more than any exit-side change tested
so far, IF the early ticks actually carry the signal.

The cost is coins that dip on tick 2, get sold, and then run. That is what this
measures.

    trigger: the position has at least N in-life quotes, and
             mode 'nth' -> quote N is below `threshold`
             mode 'all' -> quotes 1..N are ALL below `threshold`
    action:  sell at quote N
    else:    the position keeps exactly what qsim booked

WHY THIS IS A CLEAN TEST, unlike the trail work
------------------------------------------------
The trigger depends ONLY on quotes 1..N, which are observed before any decision
is made. So the split between "acted on" and "left alone" is fixed by
pre-decision data and cannot be influenced by how the coin turned out.

That is the difference from the bor_ free option, where the fallback to
current_return was chosen AFTER seeing whether a recovery happened. Here, a row
that does not trigger genuinely had no decision to make, so keeping its actual
result is correct rather than convenient.

It is also immune to the sparse-quote problem that sank the trail tests: the
first N quotes of a HELD position come at the 10-30s live cadence, not the
30-minute post-exit probe. This policy only ever reads early in-life data.

WHAT TO READ
------------
`gave_up_2x` is the cost column: triggered positions that went on to touch 2x
anyway. If that number is large the rule is killing runners, and the headline
delta is borrowing from the tail to pay for the duds.

    python3 qsim_early_abort.py --days 17
    python3 qsim_early_abort.py --days 17 --ticks 3 --threshold 0.95 --mode all
    python3 qsim_early_abort.py --days 17 --sweep
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
            SELECT qp.call_id, qp.sol_in, qp.sol_out, qp.exit_reason, t.symbol,
                   GREATEST(qp.peak_multiplier,
                            qp.sol_out / NULLIF(qp.sol_in, 0),
                            COALESCE(qp.runner_peak_mult, 0)) AS peak_x
            FROM qsim_positions qp
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.status = 'closed'
              AND qp.entry_time >= now() - (%s || ' days')::interval
              AND qp.sol_in > 0
        """, (days,))
        return {int(r["call_id"]): dict(r) for r in cur.fetchall()}


def _early(days: float, max_ticks: int) -> dict[int, list[float]]:
    """The first `max_ticks` IN-LIFE quotes per position, in time order.

    In-life only: this policy decides early and never needs to see past qsim's
    exit, which is why it dodges the sparse post-exit problem entirely.
    """
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    out: dict[int, list[float]] = defaultdict(list)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT call_id, real_mult FROM (
                SELECT o.call_id, o.real_mult,
                       row_number() OVER (PARTITION BY o.call_id
                                          ORDER BY o.observed_at) AS rn
                FROM qsim_quote_observations o
                JOIN qsim_positions qp ON qp.call_id = o.call_id
                WHERE qp.status = 'closed'
                  AND qp.entry_time >= now() - (%s || ' days')::interval
                  AND o.observed_at <= qp.exit_time
                  AND o.real_mult IS NOT NULL
                  AND NOT coalesce(o.no_route, false)
            ) s
            WHERE rn <= %s
            ORDER BY call_id, rn
        """, (days, max_ticks))
        for r in cur.fetchall():
            out[int(r["call_id"])].append(float(r["real_mult"]))
    return out


def _triggers(ticks: list[float], n: int, threshold: float, mode: str) -> float | None:
    """Exit multiple if the rule fires, else None (position left alone)."""
    if len(ticks) < n:
        return None                      # never got N looks; no decision to make
    window = ticks[:n]
    hit = all(m < threshold for m in window) if mode == "all" else window[-1] < threshold
    return window[-1] if hit else None


def _run(pos: dict, early: dict, n: int, threshold: float, mode: str) -> dict:
    act = pol = dep = 0.0
    fired = gave_up_2x = too_short = 0
    rows = []
    for cid, p in pos.items():
        sol_in = float(p["sol_in"])
        a = float(p["sol_out"] or 0.0) - sol_in
        act += a
        dep += sol_in
        ticks = early.get(cid) or []
        if len(ticks) < n:
            too_short += 1
        m = _triggers(ticks, n, threshold, mode)
        if m is None:
            pol += a
            continue
        h = sol_in * m - sol_in
        pol += h
        fired += 1
        if float(p["peak_x"] or 0) >= 2.0:
            gave_up_2x += 1
        rows.append((h - a, p.get("symbol") or "?", m, float(p["peak_x"] or 0), a))
    return {"actual": act, "policy": pol, "deployed": dep, "fired": fired,
            "gave_up_2x": gave_up_2x, "too_short": too_short, "rows": rows}


def report(days: float, n: int, threshold: float, mode: str,
           detail: bool, sweep: bool) -> None:
    pos = _positions(days)
    if not pos:
        print("no closed positions in window")
        return
    early = _early(days, 8)

    if sweep:
        print(f"SWEEP  last {days:g}d   {len(pos)} positions")
        print(f"  {'ticks':>6}{'thresh':>8}{'mode':>6}{'fired':>7}{'gave_up_2x':>12}"
              f"{'actual':>10}{'policy':>10}{'delta':>10}")
        for nn in (1, 2, 3, 5):
            for th in (1.00, 0.95, 0.90):
                for md in ("nth", "all"):
                    r = _run(pos, early, nn, th, md)
                    print(f"  {nn:>6}{th:>8.2f}{md:>6}{r['fired']:>7}"
                          f"{r['gave_up_2x']:>12}{r['actual']:>10.3f}"
                          f"{r['policy']:>10.3f}{r['policy'] - r['actual']:>10.3f}")
        print()
        print("  gave_up_2x = triggered positions that went on to touch 2x anyway.")
        print("  A big delta with a big gave_up_2x is the rule paying for the duds")
        print("  by selling the tail; that trade has lost every time it was tested.")
        return

    r = _run(pos, early, n, threshold, mode)
    print(f"EARLY ABORT  last {days:g}d   sell at tick {n} if "
          f"{'all of ticks 1..%d' % n if mode == 'all' else 'tick %d' % n} "
          f"< {threshold:g}x")
    print(f"  positions: {len(pos)}   fired: {r['fired']}   "
          f"never reached {n} quotes: {r['too_short']}")
    print()
    print(f"  deployed                 {r['deployed']:.2f} SOL")
    print(f"  actual (qsim booked)     {r['actual']:+.4f} SOL   "
          f"{100*r['actual']/r['deployed']:+.2f}%/SOL")
    print(f"  early abort              {r['policy']:+.4f} SOL   "
          f"{100*r['policy']/r['deployed']:+.2f}%/SOL")
    print(f"  DIFFERENCE               {r['policy'] - r['actual']:+.4f} SOL")
    print()
    print(f"  of the {r['fired']} it sold early, {r['gave_up_2x']} went on to touch 2x")

    if detail and r["rows"]:
        print()
        print(f"  {'symbol':<14}{'sold_at':>9}{'peak':>9}{'was':>10}{'delta':>10}")
        for d in sorted(r["rows"], reverse=True)[:15]:
            print(f"  {d[1][:13]:<14}{d[2]:>9.3f}{d[3]:>9.2f}{d[4]:>10.4f}{d[0]:>10.4f}")
        print("  ...")
        for d in sorted(r["rows"])[:15]:
            print(f"  {d[1][:13]:<14}{d[2]:>9.3f}{d[3]:>9.2f}{d[4]:>10.4f}{d[0]:>10.4f}")

    print()
    print("  Clean test: the trigger reads only quotes 1..N, so which rows are")
    print("  acted on is fixed before any outcome is known. And those quotes come")
    print("  at the live 10-30s cadence, not the 30-min post-exit probe.")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=float, default=17.0)
    ap.add_argument("--ticks", type=int, default=3)
    ap.add_argument("--threshold", type=float, default=1.0)
    ap.add_argument("--mode", choices=("nth", "all"), default="nth")
    ap.add_argument("--detail", action="store_true")
    ap.add_argument("--sweep", action="store_true")
    a = ap.parse_args()
    try:
        report(a.days, a.ticks, a.threshold, a.mode, a.detail, a.sweep)
    finally:
        db.close_conn()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
