"""
qsim_path_shape.py — how do winners die, and could a trail ever have caught them?

THE QUESTION GTF RAISED
-----------------------
GTF was entered at 1.041, climbed steadily for two hours to a sustained 61.5x,
and then printed 0.024x — the very next observation, 91 seconds later. There is
no quote in between. The 15% trail was aiming to exit near 54x and there was
never a price there to exit at.

That is not a mis-tuned trail. A trail is a rule that says "sell once the price
comes back down through a level", and it is only ever as good as the assumption
that the price comes back down THROUGH the level rather than jumping past it. On
GTF that assumption was false, and no trail percentage — 5%, 15%, 50% — would
have changed the outcome by a single lamport.

If GTF is a one-off, it is an anecdote and the exit policy is fine. If most of
the book's winners die that way, then the upside machinery is structurally
mismatched to the instrument, and a fixed TARGET (which fires on the way up,
where prices demonstrably exist) is the only thing that can collect.

That is a question about the shape of price paths, and it is answerable without
tuning anything, which is why this tool exists.

WHAT IT MEASURES
----------------
Per closed position, it walks the real quote series forward and simulates:

  trail    arm at ARM x, then exit at the first quote at or below
           running_peak * (1 - DD). Records what it AIMED for
           (running_peak * (1 - DD)) and what it actually REALISED.
  target   exit at the first quote at or above T x, else the stop, else the
           last quote. Fires on the way UP, so it never needs a price to exist
           on the way down.

and classifies each path that ran:

  gapped     the trail fired but realised less than GAP_RATIO of its nominal —
             the price jumped past the level instead of through it
  retraced   the trail fired near its nominal; it worked as designed
  no_exit    still above the trail when observation ended

The headline is the SOL the book left on the table: for every position, the
difference between what a flat target would have realised and what actually
happened, times the size actually deployed. That number either closes the gap to
breakeven or it does not, and it is not a matter of opinion.

EVERY CLOSED POSITION COUNTS, INCLUDING THE ONES THAT CANNOT BE REPLAYED
------------------------------------------------------------------------
A position quoted fewer than twice cannot have a policy simulated against it.
The tempting move is to drop it as a data-quality problem. That is wrong, and
it is wrong in a specific, well-documented way: the coins that die fastest are
exactly the coins that produce the fewest quotes, so the drop removes losers
and nothing else. --min-obs did this once already and turned a -9.94%/SOL book
into -0.71%.

The first version of THIS tool did it too, and the tell was that `actual` PnL
changed between an in-life run and a --post-exit run. Realised PnL cannot
depend on which quotes you look at; only the denominator can. So those
positions stay in `deployed` and in the baseline, and every counterfactual
policy is charged their ACTUAL result — no exit rule could have done anything
different with a single price. `actual` is now identical across both runs,
which is the invariant to check if this tool is ever edited again.

SUSTAINED PRICES ONLY
---------------------
Every multiple is min(m[i], m[i+1]) — a price is only credited if the NEXT quote
confirms it. A single-quote spike is not a price you could have sold into, and
counting one would manufacture exactly the kind of tail capture this tool exists
to measure honestly. The final observation has no successor, so it is dropped.

NO LOOK-AHEAD, AND WHY POST-EXIT PROBES ARE OFF BY DEFAULT
----------------------------------------------------------
Every policy here is decidable at the moment its quote arrives. "Exit at the
first quote at or above 5x" needs no knowledge of the future.

In-life quotes only, by default. Post-exit probes are sparse and opportunistic
rather than a monitoring cadence, so letting a counterfactual policy transact
against them credits it with fills at prices it would never have been sampling
at. --post-exit includes them, and the result should be read as a CEILING, not
an estimate. Note the direction of the default's bias: a position the bot banked
at 1.3x has no in-life quotes above 1.3x, so the target policies are being
scored BELOW what they would really have made. The conservative number is the
one that counts.

    python3 qsim_path_shape.py --days 30
    python3 qsim_path_shape.py --days 30 --channel solhousesignal
    python3 qsim_path_shape.py --days 30 --post-exit      # ceiling, not estimate

Read-only. Executes nothing, writes nothing.
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))

# Shared with the rest of the qsim tooling: a quote above this is a data error,
# not a price. Applied BEFORE the sustained-pair reduction.
MAX_MULT = float(os.getenv("QSIM_REPLAY_MAX_QOBS_MULT", "1000"))

SQL = """
SELECT qp.call_id,
       qp.sol_in,
       qp.pnl_sol,
       qp.exit_reason,
       qp.entry_time,
       qp.exit_time,
       coalesce(qp.channel_handle, '?') AS channel,
       coalesce(t.symbol, '?')          AS symbol,
       (
         SELECT coalesce(
           jsonb_agg(x.real_mult ORDER BY x.observed_at), '[]'::jsonb)
         FROM qsim_quote_observations x
         WHERE x.call_id = qp.call_id
           AND x.real_mult IS NOT NULL
           AND x.real_mult > 0
           AND x.observed_at >= qp.entry_time
           AND (%(post_exit)s OR qp.exit_time IS NULL
                OR x.observed_at <= qp.exit_time)
       ) AS mults
FROM qsim_positions qp
LEFT JOIN tokens t ON t.id = qp.token_id
WHERE qp.status = 'closed'
  AND qp.entry_time >= now() - (%(days)s || ' days')::interval
  AND qp.sol_in > 0
  AND (%(channel)s = 'any' OR coalesce(qp.channel_handle, '?') = %(channel)s)
  AND left(coalesce(qp.exit_reason, ''), 6) <> 'stale_'
ORDER BY qp.entry_time
"""


def _rows(days: int, channel: str, post_exit: bool) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor
    import db
    db.ensure_qsim_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SQL, {"days": days, "channel": channel, "post_exit": post_exit})
        return [dict(r) for r in cur.fetchall()]


def sustained(mults: list[float]) -> list[float]:
    """
    min(m[i], m[i+1]) — a price only counts if the next quote confirms it.

    This is the same reduction used to establish that the big peaks are real
    rather than single-quote artifacts, and it is what makes a simulated fill
    here a price the bag could actually have been sold into.
    """
    clean = [m for m in mults if m is not None and 0 < m <= MAX_MULT]
    if len(clean) < 2:
        return []
    return [min(clean[i], clean[i + 1]) for i in range(len(clean) - 1)]


# ── Policies ──────────────────────────────────────────────────────────────────

def sim_trail(s: list[float], arm: float, dd: float, stop: float
              ) -> tuple[float, float | None, float | None]:
    """
    Returns (realised_mult, nominal_mult, peak_at_exit).

    nominal is running_peak * (1 - dd): the price the trail was aiming for.
    realised is the quote it actually transacted at. The two differ exactly when
    the price gapped past the level, and their ratio is the whole question.
    nominal is None when the trail never armed or never fired.
    """
    peak = 0.0
    armed = False
    for m in s:
        if m <= stop and not armed:
            return m, None, None          # hard stop before the trail ever armed
        if m > peak:
            peak = m
        if not armed and peak >= arm:
            armed = True
            continue                      # arming tick cannot also trigger
        if armed:
            level = peak * (1.0 - dd)
            if m <= level:
                return m, level, peak
    return s[-1], None, (peak if armed else None)


def sim_target(s: list[float], target: float, stop: float) -> float:
    """Exit at the first quote at or above `target`, else the stop, else the last."""
    for m in s:
        if m >= target:
            return m
        if m <= stop:
            return m
    return s[-1]


# ── Report ────────────────────────────────────────────────────────────────────

def _pct(n: int, d: int) -> str:
    return f"{100.0 * n / d:.1f}%" if d else "  -  "


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--channel", default="any")
    ap.add_argument("--arm", type=float, default=2.0,
                    help="peak multiple at which the trail arms (default 2.0)")
    ap.add_argument("--dd", type=float, default=0.15,
                    help="trail drawdown from peak (default 0.15 = 15%%)")
    ap.add_argument("--stop", type=float, default=0.90,
                    help="hard stop as a MULTIPLE, not a loss pct: 0.90 = -10%%")
    ap.add_argument("--gap-ratio", type=float, default=0.50,
                    help="realised/nominal below this is a gap, not a retrace")
    ap.add_argument("--targets", default="2,3,5,10",
                    help="flat targets to price, comma separated")
    ap.add_argument("--post-exit", action="store_true",
                    help="include post-exit probes: a CEILING, not an estimate")
    args = ap.parse_args()

    targets = [float(t) for t in args.targets.split(",") if t.strip()]
    rows = _rows(args.days, args.channel, args.post_exit)

    # A position with fewer than two sustained quotes cannot have a policy
    # replayed against it. It must NOT be dropped: the coins that die fastest
    # produce the fewest quotes, so excluding them is an outcome filter wearing
    # a data-quality hat — the same mistake --min-obs made, which turned a
    # -9.94%/SOL book into -0.71% by discarding 97% of the loss.
    #
    # They stay in the baseline and in `deployed`, and every counterfactual
    # policy is charged their ACTUAL result, since no exit rule could have done
    # anything different with a single price.
    usable, unsimulatable = [], []
    for r in rows:
        s = sustained(list(r.get("mults") or []))
        if len(s) < 2:
            unsimulatable.append(r)
        else:
            r["s"] = s
            usable.append(r)

    if not usable:
        print("no positions with two or more priced quotes in this window")
        return 1

    n = len(usable)
    # Denominators span EVERY closed position, simulatable or not.
    deployed = sum(float(r["sol_in"]) for r in rows)
    actual_pnl = sum(float(r["pnl_sol"] or 0) for r in rows)
    # Carried unchanged into every policy total below.
    fixed_pnl = sum(float(r["pnl_sol"] or 0) for r in unsimulatable)

    print(f"window      {args.days}d"
          + (f"   channel={args.channel}" if args.channel != "any" else ""))
    print(f"positions   {len(rows)} closed  ({n} replayable, "
          f"{len(unsimulatable)} carried at their actual result: under two quotes)")
    print(f"deployed    {deployed:.2f} SOL      actual PnL {actual_pnl:+.3f} SOL "
          f"({100 * actual_pnl / deployed:+.2f}%/SOL)")
    print(f"quotes      {'in-life + POST-EXIT PROBES (ceiling)' if args.post_exit else 'in-life only (conservative)'}")
    print(f"trail       arms {args.arm:g}x, exits {args.dd:.0%} off peak, "
          f"stop {args.stop:g}x")
    print()

    # ── Path shapes ───────────────────────────────────────────────────────────
    shapes: dict[str, list[dict]] = {"gapped": [], "retraced": [],
                                     "no_exit": [], "never_ran": []}
    for r in usable:
        realised, nominal, peak = sim_trail(r["s"], args.arm, args.dd, args.stop)
        r["trail_realised"] = realised
        r["trail_nominal"] = nominal
        r["trail_peak"] = peak
        if peak is None:
            shapes["never_ran"].append(r)
        elif nominal is None:
            shapes["no_exit"].append(r)
        elif realised / nominal < args.gap_ratio:
            r["slip"] = realised / nominal
            shapes["gapped"].append(r)
        else:
            r["slip"] = realised / nominal
            shapes["retraced"].append(r)

    ran = len(shapes["gapped"]) + len(shapes["retraced"]) + len(shapes["no_exit"])
    hdr = f"{'path shape':<12}{'n':>6}{'% of ran':>10}{'med peak':>11}{'med aimed':>11}{'med got':>10}{'realised/aimed':>16}"
    print("HOW THE COINS THAT REACHED " + f"{args.arm:g}x".ljust(4) + "ACTUALLY ENDED")
    print(hdr)
    print("-" * len(hdr))
    for key in ("gapped", "retraced", "no_exit"):
        g = shapes[key]
        if not g:
            print(f"{key:<12}{0:>6}{'  -  ':>10}")
            continue
        peaks = [x["trail_peak"] for x in g if x["trail_peak"]]
        aimed = [x["trail_nominal"] for x in g if x["trail_nominal"]]
        got = [x["trail_realised"] for x in g]
        slips = [x["slip"] for x in g if "slip" in x]
        print(f"{key:<12}{len(g):>6}{_pct(len(g), ran):>10}"
              f"{statistics.median(peaks) if peaks else 0:>11.2f}"
              f"{statistics.median(aimed) if aimed else 0:>11.2f}"
              f"{statistics.median(got):>10.2f}"
              f"{(statistics.median(slips) if slips else float('nan')):>16.2f}")
    print(f"{'never_ran':<12}{len(shapes['never_ran']):>6}"
          f"{'  (below the arm level — the trail was never in play)':>10}")
    print()

    # ── What a flat target would have collected ───────────────────────────────
    print("WHAT A FLAT TARGET WOULD HAVE COLLECTED INSTEAD")
    print("  Same positions, same sizes, same stop. Only the UPSIDE exit changes.")
    print("  A target fires on the way up, so it never needs a price to exist on")
    print("  the way down — which is exactly what the gapped rows did not have.")
    print()
    thdr = f"{'policy':<14}{'PnL SOL':>11}{'%/SOL':>10}{'vs actual':>12}{'hit target':>12}{'med exit':>10}"
    print(thdr)
    print("-" * len(thdr))

    base_pnl = actual_pnl
    print(f"{'actual':<14}{base_pnl:>11.3f}{100 * base_pnl / deployed:>9.2f}%"
          f"{'—':>12}{'—':>12}{'—':>10}")

    # The trail as simulated here, so target-vs-trail is a like-for-like
    # comparison rather than a comparison against whatever mix of overlays,
    # floors and time stops the live config happened to be running.
    tr_pnl = fixed_pnl + sum(float(r["sol_in"]) * (r["trail_realised"] - 1.0) for r in usable)
    print(f"{'trail (sim)':<14}{tr_pnl:>11.3f}{100 * tr_pnl / deployed:>9.2f}%"
          f"{tr_pnl - base_pnl:>+12.3f}{'—':>12}"
          f"{statistics.median([r['trail_realised'] for r in usable]):>10.2f}")

    for t in targets:
        exits = [sim_target(r["s"], t, args.stop) for r in usable]
        pnl = fixed_pnl + sum(float(r["sol_in"]) * (e - 1.0) for r, e in zip(usable, exits))
        hits = sum(1 for e in exits if e >= t)
        print(f"{'target ' + f'{t:g}x':<14}{pnl:>11.3f}{100 * pnl / deployed:>9.2f}%"
              f"{pnl - base_pnl:>+12.3f}{_pct(hits, n):>12}"
              f"{statistics.median(exits):>10.2f}")
    print()

    # ── The gapped rows specifically ──────────────────────────────────────────
    g = shapes["gapped"]
    if g:
        print(f"THE {len(g)} GAPPED POSITIONS — what the trail could not reach")
        ghdr = f"{'symbol':<14}{'peak':>9}{'aimed':>9}{'got':>9}{'target 5x':>11}{'SOL missed':>12}"
        print(ghdr)
        print("-" * len(ghdr))
        missed_total = 0.0
        for r in sorted(g, key=lambda x: -(x["trail_peak"] or 0))[:15]:
            t5 = sim_target(r["s"], 5.0, args.stop)
            missed = float(r["sol_in"]) * (t5 - r["trail_realised"])
            print(f"{str(r['symbol'])[:13]:<14}{r['trail_peak']:>9.2f}"
                  f"{r['trail_nominal']:>9.2f}{r['trail_realised']:>9.3f}"
                  f"{t5:>11.2f}{missed:>+12.3f}")
        for r in g:
            t5 = sim_target(r["s"], 5.0, args.stop)
            missed_total += float(r["sol_in"]) * (t5 - r["trail_realised"])
        print("-" * len(ghdr))
        print(f"{'ALL ' + str(len(g)) + ' gapped':<14}{'':>9}{'':>9}{'':>9}{'':>11}"
              f"{missed_total:>+12.3f}")
        print()

    # ── The verdict, stated as arithmetic ─────────────────────────────────────
    def _target_pnl(t: float) -> float:
        return fixed_pnl + sum(
            float(r["sol_in"]) * (sim_target(r["s"], t, args.stop) - 1.0) for r in usable)

    best = max(targets, key=_target_pnl)
    best_pnl = _target_pnl(best)
    gap_to_be = -base_pnl

    print("=" * len(thdr))
    print("DOES FIXING THE EXIT CLOSE THE GAP?")
    print("=" * len(thdr))
    print(f"  gap to breakeven            {gap_to_be:+.3f} SOL")
    print(f"  best flat target ({best:g}x)       {best_pnl - base_pnl:+.3f} SOL over actual")
    if gap_to_be > 0:
        print(f"  closes                      {100 * (best_pnl - base_pnl) / gap_to_be:.0f}% of it")
    print()
    share = len(g) / ran if ran else 0.0
    print(f"  {len(g)} of {ran} coins that reached {args.arm:g}x gapped past the trail "
          f"({share:.0%}).")
    if share >= 0.5:
        print("  The trail is structurally mismatched to this instrument: most winners")
        print("  do not retrace through a level, they jump past it. A target is not a")
        print("  tuning preference here, it is the only rule that can transact.")
    elif share >= 0.2:
        print("  A substantial minority gap. The trail works on the rest, so the")
        print("  question is whether the gapped rows carry enough size to matter —")
        print("  read the SOL missed column, not the count.")
    else:
        print("  Most winners DO retrace through the trail's level. GTF is then the")
        print("  exception rather than the rule, and the exit policy is not the")
        print("  binding constraint on this book.")
    print()
    print("  Note the sign of the actual PnL before reading any of this as a plan.")
    print("  A better exit applied to a book with no entry edge makes the book less")
    print("  negative; it does not make it positive. Check whether best_pnl is above")
    print("  zero, not merely above actual.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
