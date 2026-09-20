"""
qsim_stop_sweep.py — does a TIGHTER stop actually lose less, or does it gap anyway?

THE QUESTION
------------
Breakeven win rate is L / (W + L). With W = +39.3%/SOL and L = -38.5%, you need
56.2% wins at a 1.3x bank and you have 39.3% — and that deficit is roughly
constant at every bank level, which is why ~150 exit variants all came back
negative. Moving the bank slides you along the curve, never off it.

The other term has never been swept. Hold the win rate where it is and solve
for L: at L = -20% the system breaks even with no change in hit rate at all.

We know stops realize -38.5% against a -20% nominal. We established that this
is GAPPING, not sampling -- drop magnitude is invariant to the OBSERVATION
WINDOW, so polling faster does not help. That is NOT the same as invariant to
the STOP LEVEL, which nobody has measured.

    If gapping is PROPORTIONAL, a -10% stop realizes about -20%, and tightening
    is the single highest-leverage change available.

    If gapping is ABSOLUTE -- roughly -35% lands on you whatever you set --
    tightening only stops you out of more eventual winners for the same loss,
    and the whole idea is dead.

The confirm-entry run hinted at the first: s0.8 beat s0.7 beat s0.6 (-12.25,
-13.53, -14.68), tighter better at every step, and 0.8 is the tightest anyone
has tried. This measures it directly.

THE COLUMN THAT ANSWERS IT is slip_x: (1 - realized) / (1 - nominal).
  ~1.0  -> stops fill where you put them; tighten freely
  ~2.0  -> you always lose twice what you asked for, but tightening still helps
   rising as the stop tightens -> gapping is absolute, tightening is futile

CENSORING (why looser stops are not trustworthy here)
-----------------------------------------------------
qsim really stopped at 0.80, so its quote series ENDS around there. Levels at or
above 0.80 are fully observed. Below it we see the stop fire but can never see
the recovery that holding might have caught, so those rows are pessimistically
censored and flagged. Read the tight end; treat the loose end as a lower bound.

Post-exit probes are excluded entirely -- this is about the held window, and
mixing in a 30-minute cadence is what produced three artifacts in the confirm
backtest.

Read-only. Executes nothing, writes nothing.

    python3 qsim_stop_sweep.py --days 30
    python3 qsim_stop_sweep.py --days 30 --bank 1.3 --bank 1.5
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))

MAX_MULT = float(os.getenv("QSIM_STOP_SWEEP_MAX_MULT", "50"))

SQL = """
WITH pos AS (
    SELECT qp.call_id, qp.token_id, qp.entry_time, qp.exit_time, qp.sol_in, qp.pnl_sol
    FROM qsim_positions qp
    WHERE qp.status = 'closed' AND qp.exit_time IS NOT NULL AND qp.sol_in > 0
      AND qp.entry_time >= now() - (%(days)s || ' days')::interval
),
obs AS (
    -- HELD WINDOW ONLY. Post-exit probes are ~30 minutes apart and would let a
    -- stop "fill" at a price no live bot could reach.
    SELECT q.call_id,
           json_agg(json_build_object('t', q.observed_at, 'm', q.real_mult)
                    ORDER BY q.observed_at) AS series,
           count(*) AS n_obs
    FROM qsim_quote_observations q
    JOIN pos p ON p.call_id = q.call_id
    WHERE q.real_mult IS NOT NULL AND q.real_mult > 0 AND q.real_mult <= %(maxmult)s
      AND (q.note IS NULL OR q.note NOT LIKE 'post_exit_probe%%')
      AND q.observed_at <= p.exit_time + interval '5 seconds'
    GROUP BY q.call_id
)
SELECT p.call_id, p.token_id, p.sol_in, p.pnl_sol, o.series, coalesce(o.n_obs, 0) AS n_obs
FROM pos p LEFT JOIN obs o ON o.call_id = p.call_id
WHERE coalesce(o.n_obs, 0) >= %(minobs)s
"""

# What the sweep cannot model, so the omission is never silent.
EXCLUDED_SQL = """
WITH pos AS (
    SELECT qp.call_id, qp.exit_time, qp.sol_in, qp.pnl_sol
    FROM qsim_positions qp
    WHERE qp.status = 'closed' AND qp.exit_time IS NOT NULL AND qp.sol_in > 0
      AND qp.entry_time >= now() - (%(days)s || ' days')::interval
),
obs AS (
    SELECT q.call_id, count(*) AS n_obs
    FROM qsim_quote_observations q
    JOIN pos p ON p.call_id = q.call_id
    WHERE q.real_mult IS NOT NULL AND q.real_mult > 0
      AND (q.note IS NULL OR q.note NOT LIKE 'post_exit_probe%%')
      AND q.observed_at <= p.exit_time + interval '5 seconds'
    GROUP BY q.call_id
)
SELECT count(*) AS n, coalesce(sum(p.pnl_sol), 0) AS pnl,
       coalesce(sum(p.sol_in), 0) AS sol
FROM pos p LEFT JOIN obs o ON o.call_id = p.call_id
WHERE coalesce(o.n_obs, 0) < %(minobs)s
"""


def _rows(days: int, min_obs: int) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor
    import db
    db.ensure_qsim_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SQL, {"days": days, "maxmult": MAX_MULT, "minobs": min_obs})
        rows = [dict(r) for r in cur.fetchall()]
        cur.execute(EXCLUDED_SQL, {"days": days, "minobs": min_obs})
        return rows, dict(cur.fetchone())


def _series(raw: Any) -> list[float]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    out = []
    for it in raw or []:
        try:
            m = float(it.get("m"))
        except (TypeError, ValueError):
            continue
        if 0 < m <= MAX_MULT:
            out.append(m)
    return out


def run(rows, stop: float, bank: float):
    """First event wins. Returns per-position outcomes for one (stop, bank)."""
    stopped, banked, neither = [], [], []
    for r in rows:
        ms = _series(r.get("series"))
        if not ms:
            continue
        size = float(r["sol_in"])
        hit = None
        for m in ms:
            if m >= bank:
                hit = ("bank", m)
                break
            if m <= stop:
                hit = ("stop", m)
                break
        if hit is None:
            neither.append((size, ms[-1]))
        elif hit[0] == "bank":
            banked.append((size, hit[1]))
        else:
            stopped.append((size, hit[1]))
    return stopped, banked, neither


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--bank", type=float, action="append", default=None,
                    help="bank level(s) to pair each stop with (default 1.3)")
    ap.add_argument("--min-obs", type=int, default=1,
                    help="DEFAULT 1 ON PURPOSE. A coin that dies before it can "
                         "be quoted three times is not a data-quality problem, "
                         "it is the worst trade in the book. Requiring 3 dropped "
                         "828 of 3,768 positions carrying ~87%% of the loss and "
                         "made the book look like -1.3%%/SOL instead of -13.3%%. "
                         "Raise it only to test sensitivity, never to clean data.")
    ap.add_argument("--qsim-stop", type=float, default=0.80,
                    help="qsim's REAL stop. Levels below it are censored, since "
                         "the series ends there and a recovery cannot be seen.")
    ap.add_argument("--clean-devs", type=int, default=0,
                    help="restrict to tokens whose deployer had >= N prior tokens "
                         "and NO prior rug. The stop's benefit was measured on the "
                         "WHOLE book, most of which is rugs; on a population that "
                         "already rugs at 8%% instead of 17%% it may buy far less, "
                         "so the two effects must be measured together, not added.")
    ap.add_argument("--factory-min", type=int, default=40)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    banks = args.bank or [1.3]
    rows, excl = _rows(args.days, args.min_obs)
    if args.clean_devs:
        from dev_history_edge import clean_token_ids
        allowed = clean_token_ids(args.clean_devs, args.factory_min)
        before = len(rows)
        rows = [r for r in rows if int(r.get("token_id") or -1) in allowed]
        print(f"clean-dev filter: {len(rows)} of {before} positions kept "
              f"(deployer had >= {args.clean_devs} prior tokens, none rugged)")
    if not rows:
        print("no usable positions")
        return 1

    firsts = [_series(r.get("series"))[0] for r in rows if _series(r.get("series"))]
    print(f"window     {args.days}d   {len(rows)} positions with >= {args.min_obs} "
          f"held-window quotes")
    if int(excl.get("n") or 0):
        e_n, e_pnl = int(excl["n"]), float(excl["pnl"])
        e_sol = float(excl["sol"]) or 1.0
        print(f"EXCLUDED   {e_n} positions had fewer, and qsim REALLY booked "
              f"{e_pnl:+.4f} SOL on them ({100 * e_pnl / e_sol:+.1f}%/SOL).")
        print(f"           They are not in any row below. If that number is large, "
              f"every %/SOL here is optimistic by roughly that much.")
    print(f"entry      first observable quote is p50 {statistics.median(firsts):.4f} "
          f"of the call price — that is the round trip, and every stop below is "
          f"measured from the CALL, not from there")
    print()

    levels = [0.95, 0.90, 0.85, 0.80, 0.75, 0.70]
    out = []
    for bank in banks:
        print(f"bank at {bank:g}x")
        hdr = (f"  {'stop':>6}{'n_stop':>8}{'n_bank':>8}{'other':>7}{'win%':>7}"
               f"{'p50_real':>10}{'mean_real':>11}{'slip_x':>8}{'%/SOL':>9}  note")
        print(hdr)
        print("  " + "-" * (len(hdr) - 2))
        for s in levels:
            stopped, banked, neither = run(rows, s, bank)
            n_s, n_b, n_n = len(stopped), len(banked), len(neither)
            if n_s + n_b == 0:
                continue
            reals = [m for _, m in stopped]
            p50 = statistics.median(reals) if reals else float("nan")
            mean = statistics.fmean(reals) if reals else float("nan")
            # THE answer column: realized loss as a multiple of the loss you asked for
            slip = ((1.0 - mean) / (1.0 - s)) if s < 1.0 and reals else float("nan")
            pnl = (sum(sz * (m - 1.0) for sz, m in stopped)
                   + sum(sz * (m - 1.0) for sz, m in banked)
                   + sum(sz * (m - 1.0) for sz, m in neither))
            sol = sum(sz for sz, _ in stopped + banked + neither)
            note = "CENSORED" if s < args.qsim_stop - 1e-9 else ""
            print(f"  {s:>6.2f}{n_s:>8}{n_b:>8}{n_n:>7}"
                  f"{100.0 * n_b / (n_b + n_s):>7.1f}{p50:>10.4f}{mean:>11.4f}"
                  f"{slip:>8.2f}{100.0 * pnl / sol if sol else 0:>9.2f}  {note}")
            out.append({"bank": bank, "stop": s, "n_stop": n_s, "n_bank": n_b,
                        "p50_real": p50, "mean_real": mean, "slip_x": slip,
                        "pct_per_sol": 100.0 * pnl / sol if sol else 0.0,
                        "censored": bool(note)})
        print()

    if args.json:
        print(json.dumps(out, indent=2))
        return 0

    print("  slip_x = (1 - realized) / (1 - nominal): how much of the loss you")
    print("  asked for you ACTUALLY take.")
    print("    flat across the rows      -> gapping is PROPORTIONAL; tightening works")
    print("    rising as the stop tightens -> gapping is ABSOLUTE; tightening only")
    print("                                   buys you more stop-outs for the same loss")
    print("  Target: a row whose mean_real is at or above 0.80 (an L of -20%),")
    print("  because that is where breakeven arrives at today's win rate.")
    print("  CENSORED rows sit below qsim's real stop: the quote series ends there,")
    print("  so a recovery that holding would have caught is invisible. Lower bound.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
