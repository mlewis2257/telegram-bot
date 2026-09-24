"""
qsim_floor_cost.py — what does the profit floor COST on the coins it sells?

WHY THIS EXISTS, AND WHY THE MAIN REPLAY CANNOT ANSWER IT
---------------------------------------------------------
qsim_quote_capture_replay.py evaluates bank policies as OVERLAYS:

    def _bank_return(mults, level, current_return):
        first = _first_cross(mults, level)
        return first - 1.0 if first is not None else current_return

and `mults` is `_held_mults(points, exit_time)` — truncated at the real exit.
That truncation is what makes every bare-mults policy post-exit safe, and it is
load-bearing (gotcha #2). It also makes the floor's opportunity cost INVISIBLE.

2026-09-24, PELON: profit_floor sold at 1.059. Its held mults top out at 1.398,
so `_first_cross(mults, 2.0)` is None and `bank_2x` scores it as current_return,
+5.85%. PELON went on to 4.179. MCNAP the same (held peak 1.366, ran to 3.242),
PUPTOBER (1.886 -> 2.070).

The replay cannot tell "never reached 2x" from "the floor sold it before it
could". Both look identical: no crossing, take current_return. So every exit
comparison has been scoring the floor's victims as if the floor were free.

WHAT THIS MEASURES
------------------
Only rows the floor actually closed. For each one, ignore that exit and keep
holding on POST-EXIT quotes, with the hard stop and a bank target still active:

    walk post-exit quotes in time order
      m >= bank  -> sell at m           (the bank the floor pre-empted)
      m <= stop  -> sell at m           (the stop still protects)
      neither    -> sell at the LAST observed quote

THE LAST LINE IS THE WHOLE INTEGRITY OF THIS TOOL. A row that never banks and
never stops takes its TERMINAL value, whatever it is. It does NOT fall back to
current_return. Falling back after reading the series is the free option that
made bor_ read +13.27 until it was bounded and became -112.75 — post-exit upside
paired with qsim's stop-protected downside, chosen per row after the fact.
Here the downside comes from the same world as the upside, on every row.

UNRESOLVED ROWS ARE REPORTED, NOT ABSORBED
------------------------------------------
~14% of positions have no post-exit observations at all. Those cannot be
evaluated, so they keep their actual result — and are counted separately,
because a coin that rugs into no-route may stop producing quotes. That
correlation would bias this test OPTIMISTIC. Read the unresolved count before
the headline; if it is large and lopsided, the headline is soft.

Coverage was checked before building this (17d): losers averaged 80.3 post-exit
observations, winners 79.3, zero-coverage 13.8% vs 15.5%. No winner bias.

    python3 qsim_floor_cost.py --days 17
    python3 qsim_floor_cost.py --days 17 --bank 3.0
    python3 qsim_floor_cost.py --days 17 --detail
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import db  # noqa: E402

# Exit reasons that are the FLOOR giving up a live position. `runner_floor` is
# queried but every partial-banked row is then EXCLUDED from the comparison and
# counted separately — see the guard in report(). It is listed here so the
# excluded count is visible rather than silently absent.
FLOOR_REASONS = ("profit_floor", "stale_profit_floor", "runner_floor")


def _rows(days: float, reasons: tuple[str, ...]) -> list[dict]:
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT qp.call_id, qp.exit_reason, qp.exit_time,
                   qp.sol_in, qp.sol_out,
                   qp.partial_fraction, qp.partial_sol_out,
                   t.symbol
            FROM qsim_positions qp
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.status = 'closed'
              AND qp.exit_reason = ANY(%s)
              AND qp.entry_time >= now() - (%s || ' days')::interval
              AND qp.sol_in > 0
            ORDER BY qp.exit_time
        """, (list(reasons), days))
        return [dict(r) for r in cur.fetchall()]


def _post_quotes(call_id: int, exit_time) -> list[float]:
    """Observed multiples-vs-entry AFTER the exit, in time order.

    `real_mult` is validated against the stored peak: max(real_mult) in life
    reproduces GREATEST(peak_multiplier, sol_out/sol_in, runner_peak_mult) on
    58 of 60 rows checked, the two exceptions being the documented
    peak_multiplier ratchet failure (the column under-reports, not this).
    """
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT real_mult
            FROM qsim_quote_observations
            WHERE call_id = %s
              AND observed_at > %s
              AND real_mult IS NOT NULL
              AND NOT coalesce(no_route, false)
            ORDER BY observed_at
        """, (call_id, exit_time))
        return [float(r["real_mult"]) for r in cur.fetchall()]


def _simulate(mults: list[float], bank: float, stop: float) -> tuple[float, str]:
    """Exit multiple and why, holding forward through `mults`.

    Terminal fallback is the LAST observed quote — never current_return. See
    the module docstring: that single choice is what keeps this from being a
    free option.
    """
    for m in mults:
        if bank > 0 and m >= bank:
            return m, "bank"
        if stop > 0 and m <= stop:
            return m, "stop"
    return mults[-1], "terminal"


def report(days: float, bank: float, stop: float, detail: bool) -> None:
    rows = _rows(days, FLOOR_REASONS)
    if not rows:
        print("no floor exits in window")
        return

    resolved, unresolved, partial = [], [], []
    for r in rows:
        sol_in = float(r["sol_in"])
        actual_pnl = float(r["sol_out"] or 0.0) - sol_in
        # A partial-banked row is EXCLUDED, not modelled. `real_mult` may be the
        # runner's value rather than the price multiple, and if it is, crediting
        # sol_in * mult applies the size reduction twice. That is exactly the
        # confounded-column mistake this project has made four times before, so
        # the rows are dropped and counted rather than guessed at. Confirm the
        # semantics on a partial-banked position and they can come back in.
        if r["partial_sol_out"] is not None:
            partial.append({**r, "actual_pnl": actual_pnl})
            continue
        mults = _post_quotes(int(r["call_id"]), r["exit_time"])
        if not mults:
            unresolved.append({**r, "actual_pnl": actual_pnl})
            continue
        mult, why = _simulate(mults, bank, stop)
        held_pnl = sol_in * mult - sol_in
        resolved.append({**r, "actual_pnl": actual_pnl, "held_pnl": held_pnl,
                         "mult": mult, "why": why, "n_post": len(mults)})

    print(f"FLOOR COST  last {days:g}d   bank={bank:g}x  stop={stop:g}x")
    print(f"  floor exits: {len(rows)}   resolved: {len(resolved)}   "
          f"unresolved (no post-exit quotes): {len(unresolved)}   "
          f"excluded (partial bank): {len(partial)}")
    if not resolved:
        print("  nothing resolved — no comparison to make")
        return

    act = sum(x["actual_pnl"] for x in resolved)
    held = sum(x["held_pnl"] for x in resolved)
    dep = sum(float(x["sol_in"]) for x in resolved)
    if unresolved:
        u_act = sum(x["actual_pnl"] for x in unresolved)
        print(f"  unresolved keep their actual result, {u_act:+.4f} SOL — "
              f"NOT included in the comparison below")
    print()
    print(f"  deployed on resolved rows      {dep:.3f} SOL")
    print(f"  actual  (floor sold)           {act:+.4f} SOL   {100*act/dep:+.2f}%/SOL")
    print(f"  held    (floor ignored)        {held:+.4f} SOL   {100*held/dep:+.2f}%/SOL")
    print(f"  DIFFERENCE                     {held - act:+.4f} SOL")
    print()

    for why in ("bank", "stop", "terminal"):
        sub = [x for x in resolved if x["why"] == why]
        if not sub:
            continue
        a = sum(x["actual_pnl"] for x in sub)
        h = sum(x["held_pnl"] for x in sub)
        print(f"  {why:<9} n={len(sub):<4} actual {a:+8.4f}  held {h:+8.4f}  "
              f"delta {h - a:+8.4f}")

    if detail:
        print()
        print(f"  {'symbol':<14}{'reason':<20}{'exit_x':>8}{'held_x':>9}"
              f"{'why':>10}{'delta':>10}{'n_post':>8}")
        for x in sorted(resolved, key=lambda r: r["held_pnl"] - r["actual_pnl"],
                        reverse=True):
            exit_x = float(x["sol_out"] or 0.0) / float(x["sol_in"])
            print(f"  {(x['symbol'] or '?')[:13]:<14}{x['exit_reason']:<20}"
                  f"{exit_x:>8.3f}{x['mult']:>9.3f}{x['why']:>10}"
                  f"{x['held_pnl'] - x['actual_pnl']:>10.4f}{x['n_post']:>8}")

    print()
    print("  Terminal rows took the LAST observed quote, win or lose — no row")
    print("  falls back to qsim's stop-protected result after reading the series.")
    print("  That is the bor_ free option and it is what this tool exists to avoid.")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=float, default=17.0)
    ap.add_argument("--bank", type=float, default=2.0,
                    help="bank target while holding (0 = no bank)")
    ap.add_argument("--stop", type=float, default=0.80,
                    help="hard stop while holding, as a multiple (0 = no stop)")
    ap.add_argument("--detail", action="store_true")
    args = ap.parse_args()
    try:
        report(args.days, args.bank, args.stop, args.detail)
    finally:
        db.close_conn()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
