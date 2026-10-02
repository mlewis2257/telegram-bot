"""
qsim_time_to_2x.py — of the coins that reach 2x, does HOW FAST they got there
predict whether they keep going?

THE QUESTION (user's, 2026-10-01)
---------------------------------
bank_2x sells every 2x at 2x. If the coins that double in 90 seconds behave
differently from the ones that grind there over an hour, then "bank or hold" is
answerable at the 2x moment from a quantity already known at that moment — the
time it took. That is a decision rule, not a prediction about the future.

Ansemmas is what prompted it: stopped out at -20%, then ran 8.4x from the entry.
It never reached 2x at all, so bank_2x never saw it. The related question of
whether the first N minutes deserve a different stop is a SEPARATE test.

WHY THIS IS NOT THE ML DEAD END
-------------------------------
ml_entry_filter tried to predict outcomes from CALL-TIME metadata and got
AUC ~0.60, because nothing at call time knows anything. Here the coin has
already told us something: it doubled, and it doubled at a particular speed.
Post-entry behaviour is a far richer signal than channel identity.

Start with ONE feature on purpose. If time-to-2x has no relationship to
continuation, no chart model will find one either and the effort is saved. If it
does, this is the baseline any model has to beat.

THE THREE TRAPS THIS FILE AVOIDS, each of which has cost a wrong answer before
-----------------------------------------------------------------------------
1. POST-EXIT LOOK-AHEAD ON THE TRIGGER. The 2x crossing must land on a quote
   taken WHILE HELD (observed_at <= exit_time). A post-exit 2x was never
   available to anyone. Reading one as actionable is what made me claim live
   "missed" a 2.28x bank on P when that print came after live was out.

2. BUT CONTINUATION MAY USE POST-EXIT QUOTES, and legitimately so: a policy that
   holds past 2x genuinely keeps holding, so quotes after qsim's own exit are
   what that policy would have seen. The illegitimate version — which this file
   does NOT do — is pairing post-exit upside with a stop-protected downside and
   picking per row after the fact. That is the bor_ free option, +13.27 until
   bounded and -112.75 after.

3. NO OUTCOME FILTERS. Conditioning on "reached 2x in-life" is the population the
   policy would act on, which is legitimate. Dropping rows for having too few
   observations is not — it selects on outcome, because dying coins stop being
   quoted. Positions with no usable series are COUNTED AND REPORTED, never
   silently absorbed.

A target is observation-robust in a way a trail is not: sparse sampling can only
MISS a 3x that happened, never invent one. So every continuation rate here is a
FLOOR, and the bias runs one way only.

    python3 qsim_time_to_2x.py --days 21
    python3 qsim_time_to_2x.py --days 21 --band          # mcap 80-120k only
    python3 qsim_time_to_2x.py --days 21 --detail
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))

import db  # noqa: E402

# Upper edge in MINUTES for each time-to-2x bucket. Last is open-ended.
BUCKETS = [2.0, 10.0, 30.0, 120.0, float("inf")]


def _bucket_label(mins: float) -> str:
    lo = 0.0
    for hi in BUCKETS:
        if mins <= hi:
            if hi == float("inf"):
                return f">{lo:g}m"
            return f"{lo:g}-{hi:g}m"
        lo = hi
    return "?"


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
          AND qp.sol_in > 0
          AND qp.exit_time IS NOT NULL
    """
    if band:
        sql += " AND c.mcap_at_call BETWEEN 80000 AND 120000"
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (days,))
        return {int(r["call_id"]): dict(r) for r in cur.fetchall()}


def _series(days: float) -> dict[int, list[tuple]]:
    """(observed_at, real_mult, in_life) per observation, time-ordered.

    in_life is observed_at <= the position's own exit_time. Load-bearing: the 2x
    TRIGGER may only use in_life rows (trap 1), while CONTINUATION may use all of
    them (trap 2).
    """
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    out: dict[int, list[tuple]] = defaultdict(list)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT o.call_id, o.observed_at, o.real_mult,
                   (o.observed_at <= qp.exit_time) AS in_life
            FROM qsim_quote_observations o
            JOIN qsim_positions qp ON qp.call_id = o.call_id
            WHERE qp.status = 'closed'
              AND qp.entry_time >= now() - (%s || ' days')::interval
              AND qp.exit_time IS NOT NULL
              AND o.real_mult IS NOT NULL
              AND NOT coalesce(o.no_route, false)
            ORDER BY o.call_id, o.observed_at
        """, (days,))
        for r in cur.fetchall():
            out[int(r["call_id"])].append(
                (r["observed_at"], float(r["real_mult"]), bool(r["in_life"])))
    return out


def analyse(days: float, band: bool, detail: bool) -> None:
    pos = _positions(days, band)
    ser = _series(days)
    if not pos:
        print("no closed positions in window")
        return

    # ── Stage counts. Every position lands in exactly one stage, and the stages
    # ── are printed so nothing can vanish into a filter unnoticed (trap 3).
    n_total = len(pos)
    n_noobs = 0
    n_no2x = 0
    reached: list[dict] = []

    for cid, p in pos.items():
        s = ser.get(cid) or []
        if not s:
            n_noobs += 1
            continue
        # TRIGGER: first in-life quote at or above 2x. in_life only.
        hit = next(((at, m) for at, m, live in s if live and m >= 2.0), None)
        if hit is None:
            n_no2x += 1
            continue
        hit_at, hit_mult = hit
        mins = (hit_at - p["entry_time"]).total_seconds() / 60.0
        # CONTINUATION: every quote strictly after the crossing, in-life or not.
        after = [m for at, m, _ in s if at > hit_at]
        reached.append({
            "cid": cid, "symbol": p.get("symbol") or "?",
            "mins": mins, "hit_mult": hit_mult,
            "max_after": max(after) if after else None,
            "terminal": after[-1] if after else None,
            "n_after": len(after),
            "actual_x": float(p["sol_out"] or 0) / float(p["sol_in"]),
            "exit_reason": p.get("exit_reason"),
        })

    print(f"TIME TO 2x  last {days:g}d"
          f"{'   mcap band 80-120k only' if band else '   all mcaps'}")
    print()
    print(f"  closed positions in window        {n_total:>6}")
    print(f"    no usable quote series          {n_noobs:>6}   "
          f"(reported, NOT dropped — see trap 3)")
    print(f"    never reached 2x while held     {n_no2x:>6}")
    print(f"    REACHED 2x while held           {len(reached):>6}   "
          f"{100.0*len(reached)/n_total:>5.1f}% of all closed")
    assert n_noobs + n_no2x + len(reached) == n_total, "stage counts must sum"
    print(f"  stages sum to total: OK ({n_noobs} + {n_no2x} + {len(reached)}"
          f" = {n_total})")
    if not reached:
        print("\n  nothing reached 2x in-life — no question to answer")
        return

    # Rows with no post-crossing quote cannot speak to continuation. Counted
    # separately rather than treated as "did not continue", which would be a
    # silent outcome filter in the other direction.
    speak = [r for r in reached if r["max_after"] is not None]
    mute = len(reached) - len(speak)
    print(f"    of those, no quote after the 2x {mute:>6}   "
          f"(excluded from continuation rates, counted here)")
    print()

    print("  Does time-to-2x predict continuation? Rates are of the rows that")
    print("  have at least one quote after the crossing.")
    print()
    hdr = (f"  {'time to 2x':<12}{'n':>5}{'>=2.5x':>8}{'>=3x':>7}{'>=5x':>7}"
           f"{'>=10x':>7}{'med max':>9}{'med term':>10}{'med obs':>9}")
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))

    def _med(xs):
        xs = sorted(xs)
        n = len(xs)
        if n == 0:
            return float("nan")
        return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0

    buckets: dict[str, list[dict]] = defaultdict(list)
    for r in speak:
        buckets[_bucket_label(r["mins"])].append(r)

    order = []
    lo = 0.0
    for hi in BUCKETS:
        order.append(f">{lo:g}m" if hi == float("inf") else f"{lo:g}-{hi:g}m")
        lo = hi

    checked = 0
    for label in order:
        rows = buckets.get(label) or []
        if not rows:
            print(f"  {label:<12}{0:>5}{'—':>8}{'—':>7}{'—':>7}{'—':>7}"
                  f"{'—':>9}{'—':>10}{'—':>9}")
            continue
        checked += len(rows)
        n = len(rows)
        mx = [r["max_after"] for r in rows]
        tm = [r["terminal"] for r in rows]
        print(f"  {label:<12}{n:>5}"
              f"{100.0*sum(m >= 2.5 for m in mx)/n:>7.1f}%"
              f"{100.0*sum(m >= 3.0 for m in mx)/n:>6.1f}%"
              f"{100.0*sum(m >= 5.0 for m in mx)/n:>6.1f}%"
              f"{100.0*sum(m >= 10.0 for m in mx)/n:>6.1f}%"
              f"{_med(mx):>9.2f}{_med(tm):>10.2f}"
              f"{_med([r['n_after'] for r in rows]):>9.0f}")
    assert checked == len(speak), "bucket counts must sum to the population"
    print(f"\n  bucket counts sum to population: OK ({checked} = {len(speak)})")

    # Whole-population baseline, so each bucket can be read against it rather
    # than against intuition.
    mx_all = [r["max_after"] for r in speak]
    n = len(speak)
    print()
    print(f"  {'ALL 2x-reachers':<12}{n:>5}"
          f"{100.0*sum(m >= 2.5 for m in mx_all)/n:>7.1f}%"
          f"{100.0*sum(m >= 3.0 for m in mx_all)/n:>6.1f}%"
          f"{100.0*sum(m >= 5.0 for m in mx_all)/n:>6.1f}%"
          f"{100.0*sum(m >= 10.0 for m in mx_all)/n:>6.1f}%"
          f"{_med(mx_all):>9.2f}{_med([r['terminal'] for r in speak]):>10.2f}")

    print()
    print("  HOW TO READ IT. 'med term' is the median LAST observed multiple —")
    print("  what a hold-forever policy would be marked at. If it sits well below")
    print("  2.0 in every bucket, holding past 2x gives the gain back regardless")
    print("  of speed, and bank_2x is right for all of them. A bucket where")
    print("  med term > 2.0 AND the >=3x rate beats the ALL row is the only shape")
    print("  that would justify holding, and only for that bucket.")
    print()
    print("  Continuation rates are FLOORS: a 30s cadence can miss a 3x that")
    print("  happened but can never invent one.")

    if detail:
        print()
        print(f"  {'symbol':<14}{'mins':>7}{'hit':>7}{'max_after':>11}"
              f"{'terminal':>10}{'n_after':>9}{'qsim_got':>10}  exit")
        for r in sorted(reached, key=lambda r: r["mins"]):
            ma = f"{r['max_after']:.2f}" if r["max_after"] is not None else "—"
            tm = f"{r['terminal']:.2f}" if r["terminal"] is not None else "—"
            print(f"  {r['symbol'][:13]:<14}{r['mins']:>7.1f}{r['hit_mult']:>7.2f}"
                  f"{ma:>11}{tm:>10}{r['n_after']:>9}"
                  f"{r['actual_x']:>10.3f}  {r['exit_reason']}")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=float, default=21.0)
    ap.add_argument("--band", action="store_true",
                    help="restrict to mcap_at_call 80-120k (what live trades)")
    ap.add_argument("--detail", action="store_true")
    a = ap.parse_args()
    try:
        analyse(a.days, a.band, a.detail)
    finally:
        db.close_conn()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
