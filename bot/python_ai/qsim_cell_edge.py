"""
qsim_cell_edge.py — is ANY channel/lane/day cell profitable on the honest book?

THE QUESTION
------------
Everything downstream of entry is now settled. Exits are good (the median coin
qsim sells is worth 0.18x of our exit price 36h later). The runner leg is
+39%/SOL. The whole loss is 635 hard stops at -38.5%, which is 62% of every
trade we take — an ENTRY selection problem.

Historically the only confirmed edge in this system was lane x day: vip_mcap_gate
early Saturday/Monday at +16.6%/SOL. That was measured on the OLD paper book,
whose entries were inflated by the lying price feed (see live_execution_haircut).
It has never been re-tested against quote-priced qsim. This does that.

WHY THIS SCRIPT IS MOSTLY STATISTICS
------------------------------------
Slicing ~1,200 trades into 100+ cells and reporting the best one is the single
easiest way to manufacture a fake edge, and this project has already been burned
by exactly this shape of error three times (--min-obs selecting on the outcome,
--include-post-exit look-ahead, a "positive" re-entry policy that was one coin).
A cell that looks like +20%/SOL on 25 trades is entirely ordinary noise.

So the headline number here is NOT the best cell. It is the PERMUTATION p-value:

    Shuffle the PnL across trades so that, by construction, no cell has any edge.
    Recompute every cell. Take the best one. Repeat 2,000 times.
    That distribution is what "the best of N cells" looks like when nothing is
    real. If the observed best cell does not clear it, there is no edge here —
    no matter how good the cell looks in isolation.

This is a family-wise test: it prices in the entire search automatically, so it
cannot be gamed by adding more cells.

Also reported per cell:
  - bootstrap 95% CI on %/SOL (resampled, because per-trade PnL is heavy-tailed
    and a normal approximation would be far too narrow)
  - h1/h2 split by entry date — a cell positive in only one half is noise
  - bank% (reached the 1.3x bank) and rug% (booked <= -80%), the two mechanical
    drivers, so a surviving cell can be checked for a plausible REASON

WARNING ABOUT RUNNING THIS REPEATEDLY
-------------------------------------
The permutation test prices in the cells within ONE grouping. It does not know
about other groupings you tried. Running --by four different ways and keeping the
best is itself a 4x search that no p-value here corrects for. Pick the grouping
first, on a prior reason, then look.

Examples:
    python3 qsim_cell_edge.py --days 14 --by lane,dow
    python3 qsim_cell_edge.py --days 14 --by channel,lane --min-n 30
    python3 qsim_cell_edge.py --days 30 --by channel,lane,dow --min-n 25
"""

from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))

SQL = """
SELECT qp.call_id, qp.channel_handle, qp.lane, qp.variant, qp.vip_tier,
       qp.entry_time, qp.sol_in, qp.pnl_sol, qp.pnl_pct, qp.exit_reason,
       coalesce(qp.partial_fraction, 0) AS partial_fraction
FROM qsim_positions qp
WHERE qp.status = 'closed'
  AND qp.entry_time >= now() - (%(days)s || ' days')::interval
  AND qp.sol_in > 0
ORDER BY qp.entry_time
"""

DIMS = ("channel", "lane", "variant", "vip_tier", "dow", "hour6")


def _rows(days: int) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor
    import db

    db.ensure_qsim_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SQL, {"days": days})
        return [dict(r) for r in cur.fetchall()]


def _f(v: Any, d: float = 0.0) -> float:
    if v is None:
        return d
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def _dim(row: dict[str, Any], dim: str) -> str:
    t = row.get("entry_time")
    if isinstance(t, datetime):
        t = t.astimezone(timezone.utc)
    if dim == "channel":
        return (row.get("channel_handle") or "?")[:18]
    if dim == "lane":
        return row.get("lane") or "none"
    if dim == "variant":
        return row.get("variant") or "?"
    if dim == "vip_tier":
        return row.get("vip_tier") or "-"
    if dim == "dow":
        return t.strftime("%a") if isinstance(t, datetime) else "?"
    if dim == "hour6":
        return f"h{(t.hour // 6) * 6:02d}" if isinstance(t, datetime) else "?"
    raise SystemExit(f"unknown dimension '{dim}' (choose from {', '.join(DIMS)})")


def _pct_per_sol(pnl: list[float], sol: list[float]) -> float:
    s = sum(sol)
    return 100.0 * sum(pnl) / s if s else 0.0


def _bootstrap_ci(pnl: list[float], sol: list[float], iters: int,
                  rng: random.Random) -> tuple[float, float]:
    """Resampled CI. A normal approximation on per-trade PnL would be far too
    narrow here — one 168x coin dominates any cell it lands in."""
    n = len(pnl)
    if n < 2:
        return (float("nan"), float("nan"))
    vals = []
    idx = list(range(n))
    for _ in range(iters):
        pick = rng.choices(idx, k=n)
        sp = ss = 0.0
        for i in pick:
            sp += pnl[i]
            ss += sol[i]
        vals.append(100.0 * sp / ss if ss else 0.0)
    vals.sort()
    lo = vals[int(0.025 * len(vals))]
    hi = vals[min(len(vals) - 1, int(0.975 * len(vals)))]
    return (lo, hi)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--by", default="lane,dow",
                    help=f"comma list from: {', '.join(DIMS)} (default lane,dow)")
    ap.add_argument("--min-n", type=int, default=20,
                    help="cells smaller than this are reported but EXCLUDED from "
                         "the permutation test, because a 3-trade cell can show "
                         "any number at all (default 20)")
    ap.add_argument("--perms", type=int, default=2000)
    ap.add_argument("--boot", type=int, default=1000)
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--seed", type=int, default=20260918)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    dims = [d.strip() for d in args.by.split(",") if d.strip()]
    for d in dims:
        if d not in DIMS:
            raise SystemExit(f"unknown dimension '{d}' (choose from {', '.join(DIMS)})")

    rows = _rows(args.days)
    if not rows:
        print("no closed qsim positions in window")
        return 1

    rng = random.Random(args.seed)
    entries = sorted(r["entry_time"] for r in rows if r.get("entry_time"))
    mid = entries[len(entries) // 2] if entries else None

    cells: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        cells[" / ".join(_dim(r, d) for d in dims)].append(r)

    book_pnl = sum(_f(r["pnl_sol"]) for r in rows)
    book_sol = sum(_f(r["sol_in"]) for r in rows)

    stats: list[dict[str, Any]] = []
    for name, rs in cells.items():
        pnl = [_f(r["pnl_sol"]) for r in rs]
        sol = [_f(r["sol_in"]) for r in rs]
        h1 = [(_f(r["pnl_sol"]), _f(r["sol_in"])) for r in rs
              if mid and r["entry_time"] <= mid]
        h2 = [(_f(r["pnl_sol"]), _f(r["sol_in"])) for r in rs
              if mid and r["entry_time"] > mid]
        eligible = len(rs) >= args.min_n
        lo, hi = _bootstrap_ci(pnl, sol, args.boot, rng) if eligible else (float("nan"),) * 2
        stats.append({
            "cell": name,
            "n": len(rs),
            "sol_in": sum(sol),
            "pnl_sol": sum(pnl),
            "pct_per_sol": _pct_per_sol(pnl, sol),
            "ci_lo": lo, "ci_hi": hi,
            "bank_pct": 100.0 * sum(1 for r in rs if _f(r["partial_fraction"]) > 0) / len(rs),
            "rug_pct": 100.0 * sum(1 for r in rs if _f(r["pnl_pct"], 0) <= -80) / len(rs),
            "h1": _pct_per_sol([a for a, _ in h1], [b for _, b in h1]) if h1 else float("nan"),
            "h2": _pct_per_sol([a for a, _ in h2], [b for _, b in h2]) if h2 else float("nan"),
            "eligible": eligible,
        })

    tested = [s for s in stats if s["eligible"]]
    if not tested:
        print(f"no cell reaches --min-n {args.min_n}; grouping is too fine for {len(rows)} trades")
        return 1

    # ── permutation test: what does the BEST cell look like under the null? ──
    sizes = [len(cells[s["cell"]]) for s in stats]
    order = [s["cell"] for s in stats]
    elig = {s["cell"] for s in tested}
    pairs = [(_f(r["pnl_sol"]), _f(r["sol_in"])) for r in rows]
    null_best: list[float] = []
    for _ in range(args.perms):
        rng.shuffle(pairs)
        pos, best = 0, float("-inf")
        for name, size in zip(order, sizes):
            end = pos + size
            if name in elig:
                sp = ss = 0.0
                for a, b in pairs[pos:end]:
                    sp += a
                    ss += b
                if ss:
                    v = 100.0 * sp / ss
                    if v > best:
                        best = v
            pos = end
        null_best.append(best)
    null_best.sort()

    observed_best = max(s["pct_per_sol"] for s in tested)
    p_fw = sum(1 for v in null_best if v >= observed_best) / len(null_best)
    null_p50 = statistics.median(null_best)
    null_p95 = null_best[int(0.95 * len(null_best))]

    if args.json:
        print(json.dumps({"cells": stats, "observed_best": observed_best,
                          "p_familywise": p_fw, "null_p50": null_p50,
                          "null_p95": null_p95}, indent=2, default=str))
        return 0

    print(f"window        {args.days}d   {len(rows)} closed trades   "
          f"book {book_pnl:+.4f} SOL ({100 * book_pnl / book_sol if book_sol else 0:+.2f}%/SOL)")
    print(f"grouping      {' / '.join(dims)}   {len(stats)} cells, "
          f"{len(tested)} with n >= {args.min_n}")
    print()

    hdr = (f"{'cell':<30}{'n':>5}{'pnl_sol':>10}{'%/SOL':>9}{'ci_lo':>9}{'ci_hi':>9}"
           f"{'bank%':>8}{'rug%':>7}{'h1':>9}{'h2':>9}")
    print(hdr)
    print("-" * len(hdr))
    for s in sorted(stats, key=lambda x: -x["pct_per_sol"])[:args.top]:
        mark = "" if s["eligible"] else "  (n<min)"
        print(f"{s['cell'][:29]:<30}{s['n']:>5}{s['pnl_sol']:>10.4f}{s['pct_per_sol']:>9.2f}"
              f"{s['ci_lo']:>9.1f}{s['ci_hi']:>9.1f}{s['bank_pct']:>8.1f}{s['rug_pct']:>7.1f}"
              f"{s['h1']:>9.1f}{s['h2']:>9.1f}{mark}")

    print()
    print("═" * len(hdr))
    print("PERMUTATION TEST — the only line that matters")
    print("═" * len(hdr))
    print(f"  best observed cell        {observed_best:+.2f} %/SOL")
    print(f"  best cell under the null  {null_p50:+.2f} %/SOL typical, "
          f"{null_p95:+.2f} at the 95th pct")
    print(f"  family-wise p             {p_fw:.4f}   "
          f"({args.perms} shuffles over {len(tested)} eligible cells)")
    print()
    if p_fw <= 0.05:
        print("  -> The best cell BEATS what the search alone would produce.")
        print("     Next: confirm it is positive in BOTH halves (h1/h2 above), that its")
        print("     bootstrap CI excludes zero, and that bank%/rug% give it a mechanical")
        print("     reason. A cell that passes all four is worth trading alone.")
    else:
        print("  -> The best cell is INDISTINGUISHABLE from the best cell you would get")
        print("     by shuffling PnL at random. Slicing this book by these dimensions")
        print("     finds nothing. Do not size up on any row in the table above.")
    print()
    print("  Note: this prices in the cells of THIS grouping only. Trying several")
    print("  --by groupings and keeping the best is a further search no p corrects for.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
