"""
qsim_stop_grace.py — suppress the hard stop for the first N minutes. Does holding
through the early dip save more runners than it costs in further decline?

THE CASE FOR ASKING (Ansemmas, 2026-10-01)
------------------------------------------
Live bought at a 119k fill, dipped to ~95k, the 20% stop fired correctly, the sell
filled at 106k — and the coin went to 999k. 8.4x from entry, sold three minutes
in. No bug anywhere: the dip was real, the stop did what it is configured to do.

A coin that runs 31k -> 999k in minutes does not get there without dipping 20%
somewhere. So the stop and the runners may be structurally incompatible on this
book, and bank_2x only pays if the position survives long enough to reach 2x.

THE POPULATION, AND WHY ONLY IT CHANGES
---------------------------------------
Only hard stops that fired INSIDE the grace window behave differently. A stop at
minute 40 with a 15-minute grace is unaffected; so is every non-stop exit. Those
rows are identical in both arms and contribute nothing to the delta, which is what
makes this a paired test rather than a comparison of two populations.

Measured over 21d, solwhaletrending, in-band: 361 stops fired within 15 minutes,
average hold 2.5 minutes, average realisation 0.6040. Of those, 25 (6.9%) later
touched 2x and 10 (2.8%) touched 5x.

So the arithmetic is already most of the way decided:
    25 * 2.0 + 336 * F = 361 * 0.6040  ->  F = 0.500
The non-recoverers must still be worth more than 0.50 after the extra wait. They
are worth 0.604 at minute 2.5 today. The whole question is how far they fall.

THE DATA EXISTS, AND IT IS ON LOAN
----------------------------------
349 of 361 (97%) have a quote within 15 minutes of their stop, which is why this
is a replay and not a forward shadow. That density comes from ratchet_shadow's
60-second post-exit fast lane -- DISABLED 2026-10-02 to free its quote budget. So
coverage for positions closed after that date is the 1-hour probe cadence only,
and re-running this on newer data will NOT have the same resolution. Treat the
window as fixed.

INTEGRITY
---------
  * Post-exit quotes are used, and legitimately: a policy that suppresses the stop
    genuinely keeps holding, so quotes after qsim's exit are what it would have
    seen. The illegitimate version is pairing that upside with a stop-protected
    downside chosen per row after the fact -- the bor_ free option, +13.27 until
    bounded and -112.75 after. Here the downside is whatever the series shows.
  * Baseline is the position's ACTUAL realisation, never a nominal stop level.
  * The policy realises the ACTUAL quote that triggers it, never the threshold.
  * No fallback to qsim's result after reading the series. Rows with no post-stop
    quote are reported, not absorbed.
  * During grace the hard stop is the ONLY rule disabled; a 2x bank still fires,
    which is the entire point.

    python3 qsim_stop_grace.py --days 21 --band --sweep
    python3 qsim_stop_grace.py --days 21 --band --grace 15 --detail
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))

import db  # noqa: E402


def _stops(days: float, band: bool, grace_max: float) -> dict[int, dict]:
    """Hard stops that fired within grace_max minutes of entry."""
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    sql = """
        SELECT qp.call_id, qp.entry_time, qp.exit_time, qp.sol_in, qp.sol_out,
               qp.exit_reason, t.symbol, c.mcap_at_call,
               EXTRACT(epoch FROM qp.exit_time - qp.entry_time)/60.0 AS held_min
        FROM qsim_positions qp
        JOIN tokens t ON t.id = qp.token_id
        JOIN calls  c ON c.id = qp.call_id
        WHERE qp.status = 'closed'
          AND qp.exit_reason LIKE %s
          AND qp.entry_time >= now() - (%s || ' days')::interval
          AND qp.exit_time IS NOT NULL
          AND qp.sol_in > 0
          AND qp.exit_time <= qp.entry_time + (%s || ' minutes')::interval
    """
    params: list = ["%hard_stop", days, grace_max]
    if band:
        sql += " AND c.mcap_at_call BETWEEN 80000 AND 120000"
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, tuple(params))
        return {int(r["call_id"]): dict(r) for r in cur.fetchall()}


def _series(call_ids: list[int]) -> dict[int, list[tuple[float, float]]]:
    """(minutes_from_entry, real_mult) per observation, time-ordered.

    Spans in-life AND post-exit. Post-exit is legitimate here — see INTEGRITY.
    """
    from psycopg2.extras import RealDictCursor
    if not call_ids:
        return {}
    conn = db.get_conn()
    db.safe_rollback()
    out: dict[int, list[tuple[float, float]]] = defaultdict(list)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT o.call_id, o.real_mult,
                   EXTRACT(epoch FROM o.observed_at - qp.entry_time)/60.0 AS min_in
            FROM qsim_quote_observations o
            JOIN qsim_positions qp ON qp.call_id = o.call_id
            WHERE o.call_id = ANY(%s)
              AND o.real_mult IS NOT NULL
              AND NOT coalesce(o.no_route, false)
            ORDER BY o.call_id, o.observed_at
        """, (call_ids,))
        for r in cur.fetchall():
            out[int(r["call_id"])].append((float(r["min_in"]), float(r["real_mult"])))
    return out


def _simulate(series: list[tuple[float, float]], held_min: float,
              grace: float, bank: float, stop_mult: float):
    """(policy_mult, why) or None when the row cannot be scored.

    Walks quotes strictly AFTER the stop that actually happened. During grace the
    hard stop is suppressed but the bank still fires; once past grace the stop
    arms and takes the first quote at or below it.
    """
    rest = [(m, x) for m, x in series if m > held_min]
    if not rest:
        return None                       # nothing observed after the stop
    for m, x in rest:
        if x >= bank:
            return x, "bank"              # the recovery this policy exists to catch
        if m >= grace and x <= stop_mult:
            return x, "stop_armed"        # the ACTUAL breaching quote
    return rest[-1][1], "terminal"        # last quote, win or lose


def _run(stops: dict, ser: dict, grace: float, bank: float,
         stop_mult: float) -> dict:
    base = pol = dep = 0.0
    counts: dict[str, int] = defaultdict(int)
    rows: list[tuple] = []
    unscorable = 0
    for cid, p in stops.items():
        if float(p["held_min"]) > grace:
            continue                      # stop fired outside grace: arms identical
        s = ser.get(cid) or []
        res = _simulate(s, float(p["held_min"]), grace, bank, stop_mult)
        if res is None:
            unscorable += 1
            continue
        pm, why = res
        sol_in = float(p["sol_in"])
        bx = float(p["sol_out"] or 0.0) / sol_in
        dep += sol_in
        base += sol_in * bx
        pol += sol_in * pm
        counts[why] += 1
        rows.append((sol_in * (pm - bx), p.get("symbol") or "?", bx, pm, why,
                     float(p["held_min"])))
    return {"baseline": base, "policy": pol, "deployed": dep, "counts": dict(counts),
            "rows": rows, "engaged": len(rows), "unscorable": unscorable}


def _med(xs):
    xs = sorted(xs)
    n = len(xs)
    return float("nan") if n == 0 else (
        xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2.0)


def report(days: float, band: bool, grace: float, bank: float, stop_mult: float,
           sweep: bool, detail: bool) -> None:
    grace_max = 120.0 if sweep else grace
    stops = _stops(days, band, grace_max)
    if not stops:
        print("no early hard stops in window")
        return
    ser = _series(sorted(stops))

    print(f"STOP GRACE  last {days:g}d   "
          f"{'mcap band 80-120k' if band else 'all mcaps'}   "
          f"stop={stop_mult:g}x bank={bank:g}x")
    print()
    print("  Baseline = the stop this position ACTUALLY realised.")
    print("  Policy   = hard stop suppressed for the first N minutes; the bank still")
    print("             fires. Only stops INSIDE the window differ; all else is paired.")
    print()

    if sweep:
        hdr = (f"  {'grace':>7}{'n':>6}{'baseline':>11}{'policy':>10}{'delta':>10}"
               f"{'bank':>6}{'armed':>7}{'term':>6}{'med armed':>11}{'unscor':>8}")
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for g in (2.0, 5.0, 10.0, 15.0, 30.0, 60.0):
            r = _run(stops, ser, g, bank, stop_mult)
            if not r["engaged"]:
                continue
            c = r["counts"]
            armed = [d[3] for d in r["rows"] if d[4] == "stop_armed"]
            print(f"  {g:>7.0f}{r['engaged']:>6}{r['baseline']:>11.3f}"
                  f"{r['policy']:>10.3f}{r['policy'] - r['baseline']:>10.3f}"
                  f"{c.get('bank', 0):>6}{c.get('stop_armed', 0):>7}"
                  f"{c.get('terminal', 0):>6}"
                  f"{(_med(armed) if armed else float('nan')):>11.3f}"
                  f"{r['unscorable']:>8}")
        print()
        print("  'med armed' is the number that decides it: what the stop realises")
        print("  AFTER the wait. Break-even on the 15m cell needs it above ~0.50")
        print("  against the 0.604 these positions realise today at minute 2.5.")
        print()
        print("  A rising delta with grace is the runners being saved; a falling one")
        print("  is the non-recoverers bleeding further. Both are in the same column.")
        return

    r = _run(stops, ser, grace, bank, stop_mult)
    print(f"  grace {grace:g} minutes")
    print(f"  early stops in window      {len(stops):>6}")
    print(f"    inside grace, scorable   {r['engaged']:>6}")
    print(f"    inside grace, no quote   {r['unscorable']:>6}   (reported, NOT absorbed)")
    if not r["engaged"]:
        print("\n  nothing to score")
        return
    print()
    print(f"  deployed                   {r['deployed']:.3f} SOL")
    print(f"  actual stops (baseline)    {r['baseline']:+.4f} SOL")
    print(f"  with grace                 {r['policy']:+.4f} SOL")
    print(f"  DIFFERENCE                 {r['policy'] - r['baseline']:+.4f} SOL")
    print(f"  per position               "
          f"{(r['policy'] - r['baseline']) / r['engaged']:+.5f} SOL")
    print()
    print("  how the policy closed:")
    for why, n in sorted(r["counts"].items(), key=lambda kv: -kv[1]):
        sub = [d for d in r["rows"] if d[4] == why]
        print(f"    {why:<12} n={n:<5} delta {sum(d[0] for d in sub):+8.4f} SOL"
              f"   median realised {_med([d[3] for d in sub]):.3f}x")

    if detail:
        print()
        print(f"  {'symbol':<14}{'held_m':>8}{'stopped_at':>12}{'policy_at':>11}"
              f"{'why':>12}{'delta':>10}")
        for d in sorted(r["rows"], reverse=True)[:15]:
            print(f"  {d[1][:13]:<14}{d[5]:>8.1f}{d[2]:>12.3f}{d[3]:>11.3f}"
                  f"{d[4]:>12}{d[0]:>10.5f}")
        print("  ...")
        for d in sorted(r["rows"])[:10]:
            print(f"  {d[1][:13]:<14}{d[5]:>8.1f}{d[2]:>12.3f}{d[3]:>11.3f}"
                  f"{d[4]:>12}{d[0]:>10.5f}")

    print()
    print("  CAVEAT ON THE DATA, not the method: 97% of these rows have a quote")
    print("  within 15 minutes of their stop only because ratchet_shadow's 60s")
    print("  post-exit fast lane was running. That was disabled 2026-10-02, so newer")
    print("  positions have 1-hour resolution and this window cannot be extended.")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=float, default=21.0)
    ap.add_argument("--band", action="store_true")
    ap.add_argument("--grace", type=float, default=15.0,
                    help="minutes the hard stop is suppressed")
    ap.add_argument("--bank", type=float, default=2.0)
    ap.add_argument("--stop", type=float, default=0.80, dest="stop_mult")
    ap.add_argument("--sweep", action="store_true")
    ap.add_argument("--detail", action="store_true")
    a = ap.parse_args()
    try:
        report(a.days, a.band, a.grace, a.bank, a.stop_mult, a.sweep, a.detail)
    finally:
        db.close_conn()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
