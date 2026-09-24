"""
qsim_reentry_strength.py — re-enter only the coins that prove strength, not every bounce.

THE IDEA, AND WHY IT IS NOT THE OLD RE-ENTRY TEST
-------------------------------------------------
More than half of the biggest movers in the book did most of their move AFTER
the bot was out. PAID exited at 1.683 off a 2.699 in-life peak and then reached
168x. ELON hard-stopped at 0.690 and reached 45.8x. HUHCAT 0.658 -> 35.5x.

`qsim_reentry_backtest.py` asked "what if we bought back after an exit" and came
back negative, but it re-entered on a price LEVEL — which buys every corpse that
twitches. This asks a different and much more selective question:

    re-enter only when the coin prints a NEW HIGH above its own in-life peak.

That bar is not a bounce. A coin that exceeds the best price it ever made while
you held it is doing something a dying one cannot do, and every large post-exit
mover in the book clears it. The whole question is how many coins clear it and
die anyway, which is exactly what the trigger rate and the loser tail below
measure.

WHAT IS AND IS NOT LOOK-AHEAD
-----------------------------
The trigger is decidable the moment its quote arrives: prior peak is known, the
new quote is on the tape, the comparison needs no future. The re-entry exit is
likewise driven only by quotes at or after the re-entry.

What this CANNOT fully model is fill quality. Post-exit probes are sparse and
opportunistic rather than a monitoring cadence, so a re-entry priced at a probe
may be filling at a print that was not continuously available. Every number here
is therefore an UPPER BOUND, and the honest reading is "is this big enough to be
worth building", not "this is the PnL".

The round trip IS charged (--roundtrip, default 0.976 measured), because a
re-entry pays the spread again and a model that forgets this manufactures edge
out of churn.

    python3 qsim_reentry_strength.py --days 30
    python3 qsim_reentry_strength.py --days 30 --trigger 1.2 --ceiling 20
    python3 qsim_reentry_strength.py --days 30 --by-reason

Read-only. Executes nothing, writes nothing.
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))

MAX_MULT = float(os.getenv("QSIM_REPLAY_MAX_QOBS_MULT", "1000"))

SQL = """
SELECT qp.call_id,
       qp.sol_in,
       qp.pnl_sol,
       coalesce(qp.exit_reason, '?')    AS exit_reason,
       coalesce(t.symbol, '?')          AS symbol,
       qp.exit_time,
       (
         SELECT coalesce(jsonb_agg(jsonb_build_object(
                   'at', x.observed_at, 'm', x.real_mult) ORDER BY x.observed_at), '[]'::jsonb)
         FROM qsim_quote_observations x
         WHERE x.call_id = qp.call_id
           AND x.real_mult IS NOT NULL AND x.real_mult > 0 AND x.real_mult <= %(maxmult)s
           AND x.observed_at >= qp.entry_time
       ) AS obs
FROM qsim_positions qp
LEFT JOIN tokens t ON t.id = qp.token_id
WHERE qp.status = 'closed'
  AND qp.entry_time >= now() - (%(days)s || ' days')::interval
  AND qp.exit_time IS NOT NULL
  AND qp.sol_in > 0
  AND left(coalesce(qp.exit_reason, ''), 6) <> 'stale_'
ORDER BY qp.entry_time
"""


def _rows(days: int) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor
    import db
    db.ensure_qsim_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SQL, {"days": days, "maxmult": MAX_MULT})
        return [dict(r) for r in cur.fetchall()]


def _parse(v: Any):
    from datetime import datetime
    if hasattr(v, "year"):
        return v
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None


def simulate(row: dict, trigger: float, ceiling: float, stop: float,
             trail: float, roundtrip: float) -> dict | None:
    """
    Re-enter at the first post-exit quote above prior_peak * trigger, then exit on
    ceiling / trail-from-new-peak / stop, else the last quote.

    Returns None when the position never triggered — which is the common case and
    is the whole point: a rule that fires rarely costs nothing when it does not.
    """
    obs = row.get("obs") or []
    exit_time = _parse(row.get("exit_time"))
    if not obs or exit_time is None:
        return None

    held, after = [], []
    for o in obs:
        at, m = _parse(o.get("at")), o.get("m")
        if at is None or m is None:
            continue
        (held if at <= exit_time else after).append((at, float(m)))
    if not held or not after:
        return None

    prior_peak = max(m for _, m in held)
    bar = prior_peak * trigger

    entry = None
    for at, m in after:
        if m >= bar:
            entry = m
            after = [(a, x) for a, x in after if a > at]
            break
    if entry is None or entry <= 0:
        return None

    # Everything from here is expressed as a multiple OF THE RE-ENTRY PRICE.
    peak = 1.0
    exit_mult, reason = None, "last"
    for _, m in after:
        r = m / entry
        peak = max(peak, r)
        if ceiling > 0 and r >= ceiling:
            exit_mult, reason = r, "ceiling"
            break
        if stop > 0 and r <= stop:
            exit_mult, reason = r, "stop"
            break
        if trail > 0 and peak > 1.0 and r <= peak * (1.0 - trail):
            exit_mult, reason = r, "trail"
            break
    if exit_mult is None:
        exit_mult = (after[-1][1] / entry) if after else 1.0

    return {
        "symbol": row.get("symbol"), "call_id": row["call_id"],
        "exit_reason": row.get("exit_reason"),
        "prior_peak": prior_peak, "reentry_at": entry,
        "exit_mult": exit_mult, "reason": reason,
        # The re-entry pays the spread AGAIN. Forgetting this is how churn
        # turns into fake edge.
        "ret": exit_mult * roundtrip - 1.0,
        "sol_in": float(row["sol_in"]),
    }


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--trigger", type=float, default=1.0,
                    help="re-enter at prior_peak * this. 1.0 = any new high, "
                         "1.2 = must clear the old peak by 20%%")
    ap.add_argument("--ceiling", type=float, default=20.0,
                    help="take it at this multiple OF THE RE-ENTRY price (0 = none)")
    ap.add_argument("--stop", type=float, default=0.80,
                    help="stop as a multiple of the re-entry price")
    ap.add_argument("--trail", type=float, default=0.30)
    ap.add_argument("--roundtrip", type=float, default=0.976,
                    help="measured round trip charged to the re-entry")
    ap.add_argument("--by-reason", action="store_true",
                    help="split by how the ORIGINAL position exited")
    ap.add_argument("--detail", type=int, default=15)
    args = ap.parse_args()

    rows = _rows(args.days)
    if not rows:
        print("no closed positions with an exit time in this window")
        return 1

    sims = [s for s in (simulate(r, args.trigger, args.ceiling, args.stop,
                                 args.trail, args.roundtrip) for r in rows) if s]

    n, k = len(rows), len(sims)
    print(f"window        {args.days}d   {n} closed positions")
    print(f"trigger       new high >= prior peak x {args.trigger:g}")
    print(f"re-entry exit ceiling {args.ceiling:g}x  stop {args.stop:g}x  "
          f"trail {args.trail:.0%}  round trip {args.roundtrip:g}")
    print()
    if not k:
        print("NOTHING TRIGGERED. No position printed a new high above its own")
        print("in-life peak after exiting, so there is no rule to evaluate.")
        return 0

    rets = [s["ret"] for s in sims]
    wins = sum(1 for r in rets if r > 0)
    # Sizing mirrors the original position, so this is directly comparable to the
    # book rather than being an unconstrained new strategy.
    pnl = sum(s["sol_in"] * s["ret"] for s in sims)
    deployed = sum(s["sol_in"] for s in sims)

    print(f"triggered     {k} of {n}  ({100.0*k/n:.1f}%)")
    print(f"win rate      {100.0*wins/k:.1f}%   ({wins} up, {k-wins} down)")
    print(f"median ret    {100*statistics.median(rets):+.1f}%")
    print(f"mean ret      {100*sum(rets)/k:+.1f}%")
    print(f"PnL           {pnl:+.3f} SOL on {deployed:.2f} SOL redeployed "
          f"({100*pnl/deployed:+.2f}%/SOL)")
    print()

    # A fat tail is the point, so say out loud how much of it is one coin.
    top = sorted(sims, key=lambda s: -s["sol_in"] * s["ret"])
    carried = sum(s["sol_in"] * s["ret"] for s in top[:3])
    if pnl > 0:
        print(f"  top 3 of {k} carry {carried:+.3f} SOL of the {pnl:+.3f} "
              f"({100*carried/pnl:.0f}%)")
        print("  Concentration is expected in a power-law market — it is the")
        print("  mechanism, not a defect. But the mean is noisy at this n, so")
        print("  read the TRIGGER RATE and win rate, which converge faster.")
        print()

    hdr = (f"{'symbol':<14}{'orig exit':<16}{'prior pk':>9}{'re-in':>8}"
           f"{'exit':>8}{'why':>9}{'ret':>9}")
    print(hdr)
    print("-" * len(hdr))
    for s in top[:args.detail]:
        print(f"{str(s['symbol'])[:13]:<14}{str(s['exit_reason'])[:15]:<16}"
              f"{s['prior_peak']:>9.2f}{s['reentry_at']:>8.2f}"
              f"{s['exit_mult']:>8.2f}{s['reason']:>9}{100*s['ret']:>+8.0f}%")
    if len(top) > args.detail:
        worst = top[-1]
        print(f"{'...':<14}{'':<16}{'':>9}{'':>8}{'':>8}{'':>9}")
        print(f"{str(worst['symbol'])[:13]:<14}{str(worst['exit_reason'])[:15]:<16}"
              f"{worst['prior_peak']:>9.2f}{worst['reentry_at']:>8.2f}"
              f"{worst['exit_mult']:>8.2f}{worst['reason']:>9}"
              f"{100*worst['ret']:>+8.0f}%   <- worst")
    print()

    if args.by_reason:
        print("BY HOW THE ORIGINAL POSITION EXITED")
        bh = f"{'orig exit':<20}{'trig':>7}{'win%':>8}{'mean':>9}{'pnl_sol':>10}"
        print(bh)
        print("-" * len(bh))
        groups: dict[str, list] = {}
        for s in sims:
            groups.setdefault(str(s["exit_reason"]), []).append(s)
        for reason, g in sorted(groups.items(), key=lambda kv: -len(kv[1])):
            gr = [x["ret"] for x in g]
            gp = sum(x["sol_in"] * x["ret"] for x in g)
            print(f"{reason[:19]:<20}{len(g):>7}{100*sum(1 for r in gr if r>0)/len(gr):>7.0f}%"
                  f"{100*sum(gr)/len(gr):>8.0f}%{gp:>+10.3f}")
        print()
        print("  A hard_stop that then makes a new high is a different animal from")
        print("  a trail exit that does — the first never went up while held, the")
        print("  second already had and pulled back. They may not pay alike.")
        print()

    print("  UPPER BOUND. Post-exit probes are sparse and opportunistic, so a")
    print("  re-entry priced at one may be filling at a print that was not")
    print("  continuously available. Read this as 'is it worth building', not as")
    print("  the PnL. If it is, the build is a live watcher on exited positions.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
