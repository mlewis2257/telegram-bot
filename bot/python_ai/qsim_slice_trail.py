"""
qsim_slice_trail.py — bank most of it at 2x, TRAIL a slice. Pure DB replay, no API calls.

THE POLICY
----------
    first in-life quote >= bank   -> sell (1 - keep) of the position there
    the remaining `keep`          -> ride, and sell at whichever comes first:
                                       a quote >= target              (if a target is set)
                                       a quote <= floor               (multiple of ENTRY)
                                       a quote <= high * (1 - trail)  (the trail)
                                       the last quote inside the horizon

Positions that never reach `bank` are identical in both arms and are left out entirely;
every number here is about the banked coins only, against "sell the whole bag at the bank".

WHY THIS FILE EXISTS WHEN qsim_partial_runner.py ALREADY DOES
-------------------------------------------------------------
That one replays a TARGET, on purpose: its own header says a trail could not be measured
because post-exit quotes were 30-60 minutes apart, and a trail has to SEE the path down.
Since 36fd7dd (2026-10-07) banked exits are probed every 15s for 45 minutes, so the path
exists. This is the replay that data was collected for.

THREE WAYS THIS CAN LIE, AND WHAT IS DONE ABOUT EACH
----------------------------------------------------
1. SPARSE PATHS. A trail replayed on hourly quotes is fiction: a 40% pullback and its
   recovery fit between two looks, so the trail "survives" things it never would have.
   Only coins with a dense post-bank path are scored (--min-dense-quotes inside the first
   --dense-mins). The rest are COUNTED AND LISTED, never silently dropped — and note the
   density depends on when the coin closed (after the bank-window deploy or not), not on
   how it performed.

2. DEAD COINS VANISHING. A rugged coin returns no-route, which carries no multiple. The
   target replay excludes those rows, so its "terminal" is the last quote BEFORE the rug.
   Here a no-route look is a multiple of 0: if the slice is still held, it is worth
   nothing. Without this the coins that die hardest would be the ones priced kindest.

3. SELLING AT THE LINE. The slice is sold at the QUOTE THAT BREACHED the trail, not at the
   trail level. Coins gap: Pumpcord went 3.88x -> 2.07x between two 15s looks. Booking the
   trail level would credit a fill nobody could have had. A target, by contrast, is
   credited at the target (a bot watching every few seconds sells near it; the first 15s
   look past it can read far higher).

Even so this is a replay of a QUOTE path, 15s apart, for a 0.05 SOL bag. Live would look
more often (better) and would have to actually land a sell into a falling book (worse).
It ranks rules; it does not forecast P&L. Live is the ruler for that.

    python3 qsim_slice_trail.py --since 2026-10-08 --sweep
    python3 qsim_slice_trail.py --since 2026-10-08 --keep 0.2 --trail 0.45 --detail
    python3 qsim_slice_trail.py --since 2026-10-08 --keep 0.2 --trail 0.45 \\
        --tighten 5:0.35,10:0.25 --floor 1.5 --detail
    python3 qsim_slice_trail.py --since 2026-10-08 --all-lanes --mcap-max 0 --sweep
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))

import db  # noqa: E402
from qsim_partial_runner import _live_channels, _scope_sql  # noqa: E402


def _positions(where: str, params: list) -> dict[int, dict]:
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT qp.call_id, qp.sol_in, qp.sol_out, qp.exit_reason, t.symbol,
                   coalesce(nullif(lower(trim(t.security_flag)), ''), 'null') AS flag
            FROM qsim_positions qp
            JOIN tokens t ON t.id = qp.token_id
            JOIN calls  c ON c.id = qp.call_id
            WHERE {where}
        """, tuple(params))
        return {int(r["call_id"]): dict(r) for r in cur.fetchall()}


def _series(where: str, params: list) -> dict[int, list[tuple[float, bool, float]]]:
    """(multiple, in_life, minutes_after_exit) per look, in time order.

    A no-route look is kept as multiple 0.0 — see the module docstring, point 2.
    Rate-limited looks saw nothing and are dropped.
    """
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    out: dict[int, list] = defaultdict(list)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT o.call_id,
                   CASE WHEN coalesce(o.no_route, false) THEN 0.0 ELSE o.real_mult END AS mult,
                   (o.observed_at <= qp.exit_time) AS in_life,
                   extract(epoch FROM o.observed_at - qp.exit_time) / 60.0 AS mins_after
            FROM qsim_quote_observations o
            JOIN qsim_positions qp ON qp.call_id = o.call_id
            JOIN tokens t ON t.id = qp.token_id
            JOIN calls  c ON c.id = qp.call_id
            WHERE {where}
              AND NOT coalesce(o.rate_limited, false)
              AND (o.real_mult IS NOT NULL OR coalesce(o.no_route, false))
            ORDER BY o.call_id, o.observed_at
        """, tuple(params))
        for r in cur.fetchall():
            out[int(r["call_id"])].append(
                (float(r["mult"]), bool(r["in_life"]), float(r["mins_after"] or 0.0)))
    return out


def parse_tighten(spec: str | None) -> list[tuple[float, float]]:
    """'5:0.35,10:0.25' -> [(10.0, 0.25), (5.0, 0.35)], highest level first."""
    tiers: list[tuple[float, float]] = []
    for part in (spec or "").split(","):
        part = part.strip()
        if not part:
            continue
        level, pct = part.split(":")
        level_f, pct_f = float(level), float(pct)
        if level_f <= 0 or not 0 < pct_f < 1:
            raise ValueError(f"bad --tighten tier {part!r}")
        tiers.append((level_f, pct_f))
    return sorted(tiers, reverse=True)


def _trail_for(high: float, trail: float, tighten: list[tuple[float, float]]) -> float:
    for level, pct in tighten:
        if high >= level:
            return pct
    return trail


def bank_index(series: list, bank: float) -> int | None:
    """First IN-LIFE look at or above the bank. Post-exit looks never bank: a coin that
    stopped at 0.78x and later printed 2.5x gave nobody a 2.5x fill."""
    return next((i for i, row in enumerate(series) if row[1] and row[0] >= bank), None)


def dense_quotes(series: list, idx: int, dense_mins: float) -> int:
    return sum(1 for row in series[idx + 1:] if 0 <= row[2] <= dense_mins)


def simulate(series: list, bank: float, trail: float, *, floor: float = 0.0,
             target: float = 0.0, max_hours: float = 0.0,
             tighten: list[tuple[float, float]] | None = None) -> dict | None:
    """Where the slice gets sold. None = the coin never banked.

    Returns {bank_x, slice_x, why, high, mins}. slice_x is a multiple of ENTRY, the same
    unit as bank_x, so the blended position is (1-keep)*bank_x + keep*slice_x.
    """
    idx = bank_index(series, bank)
    if idx is None:
        return None
    bank_x = series[idx][0]
    rest = [row for row in series[idx + 1:]
            if max_hours <= 0 or row[2] <= max_hours * 60.0]
    if not rest:
        return {"bank_x": bank_x, "slice_x": bank_x, "why": "no_runner_data",
                "high": bank_x, "mins": 0.0}
    tighten = tighten or []
    high = bank_x
    for m, _live, mins in rest:
        if target > 0 and m >= target:
            return {"bank_x": bank_x, "slice_x": target, "why": "target",
                    "high": max(high, m), "mins": mins}
        if floor > 0 and m <= floor:
            return {"bank_x": bank_x, "slice_x": m, "why": "floor", "high": high, "mins": mins}
        if trail > 0 and m <= high * (1.0 - _trail_for(high, trail, tighten)):
            return {"bank_x": bank_x, "slice_x": m, "why": "trail", "high": high, "mins": mins}
        high = max(high, m)
    last_m, _l, last_mins = rest[-1]
    return {"bank_x": bank_x, "slice_x": last_m,
            "why": "horizon" if max_hours > 0 else "still_held_at_last_look",
            "high": high, "mins": last_mins}


def _score(banked: list[dict], keep: float, trail: float, **kw) -> dict:
    base = pol = dep = 0.0
    rows = []
    why_n: dict[str, int] = defaultdict(int)
    for b in banked:
        r = simulate(b["series"], b["bank"], trail, **kw)
        sol_in = b["sol_in"]
        blended = (1 - keep) * r["bank_x"] + keep * r["slice_x"]
        base += sol_in * (r["bank_x"] - 1)
        pol += sol_in * (blended - 1)
        dep += sol_in
        why_n[r["why"]] += 1
        rows.append({**r, "symbol": b["symbol"], "flag": b["flag"], "blended": blended,
                     "delta": sol_in * (blended - r["bank_x"]), "quotes": b["quotes"]})
    return {"base": base, "policy": pol, "deployed": dep, "rows": rows, "why": dict(why_n)}


def report(a) -> None:
    channels = [] if a.all_lanes else _live_channels()
    where, params = _scope_sql(a.days, a.since, channels, a.mcap_min, a.mcap_max, a.flag)
    pos = _positions(where, params)
    ser = _series(where, params)
    tighten = parse_tighten(a.tighten)

    print(f"SCOPE  {', '.join(channels) if channels else 'ALL lanes'}   "
          + (f"mcap {a.mcap_min/1000:g}k-{a.mcap_max/1000:g}k" if a.mcap_max > 0 else "all mcaps")
          + (f"   flag={a.flag}" if a.flag else "   all flags")
          + (f"   since {a.since}" if a.since else f"   last {a.days:g}d")
          + f"   bank {a.bank:g}x"
          + (f"   horizon {a.max_hours:g}h" if a.max_hours > 0 else "   horizon: last look"))

    banked, sparse = [], []
    for cid, p in pos.items():
        s = ser.get(cid) or []
        idx = bank_index(s, a.bank)
        if idx is None:
            continue
        q = dense_quotes(s, idx, a.dense_mins)
        row = {"series": s, "bank": a.bank, "sol_in": float(p["sol_in"]),
               "symbol": p.get("symbol") or "?", "flag": p.get("flag"), "quotes": q}
        (banked if q >= a.min_dense_quotes else sparse).append(row)

    print(f"  closed positions: {len(pos)}   reached the bank: {len(banked) + len(sparse)}   "
          f"with a DENSE path: {len(banked)}   too sparse to trail: {len(sparse)}")
    if sparse:
        names = ", ".join(f"{r['symbol']}({r['quotes']})" for r in sparse[:12])
        print(f"  not scored (looks in first {a.dense_mins:g}min < {a.min_dense_quotes}): {names}"
              + (" ..." if len(sparse) > 12 else ""))
    if not banked:
        print("\n  No banked coin has a dense post-bank path in this window. The 15s bank")
        print("  probes started 2026-10-07 (36fd7dd); use --since on or after that date.")
        return

    kw = dict(floor=a.floor, target=a.target, max_hours=a.max_hours, tighten=tighten)

    if a.sweep:
        print()
        print(f"  SWEEP over {len(banked)} banked coins"
              + (f"   floor {a.floor:g}x" if a.floor > 0 else "   no floor")
              + (f"   target {a.target:g}x" if a.target > 0 else "   no target")
              + (f"   tighten {a.tighten}" if tighten else ""))
        print(f"  baseline = sell the whole bag at the bank: "
              f"{_score(banked, 0.0, 0.0, **kw)['base']:+.4f} SOL")
        print(f"  {'trail':>7}" + "".join(f"{'keep ' + format(k, '.0%'):>12}" for k in a.keeps)
              + f"{'median slice_x':>16}{'sold by trail':>15}")
        for t in a.trails:
            cells, med, n_trail = [], 0.0, 0
            for k in a.keeps:
                r = _score(banked, k, t, **kw)
                cells.append(r["policy"] - r["base"])
                med = statistics.median(x["slice_x"] for x in r["rows"])
                n_trail = r["why"].get("trail", 0)
            print(f"  {t:>7.0%}" + "".join(f"{c:>+12.4f}" for c in cells)
                  + f"{med:>16.2f}{n_trail:>15}")
        print()
        print("  Cells are SOL gained or lost against the baseline, over these coins only.")
        print("  'median slice_x' is the TYPICAL slice; if it sits below the bank while the")
        print("  cell is positive, one or two coins are carrying the row — run --detail.")
        return

    r = _score(banked, a.keep, a.trail, **kw)
    print()
    print(f"  keep {a.keep:.0%}, trail {a.trail:.0%}"
          + (f", floor {a.floor:g}x" if a.floor > 0 else "")
          + (f", target {a.target:g}x" if a.target > 0 else "")
          + (f", tighten {a.tighten}" if tighten else ""))
    print(f"  sell everything at the bank   {r['base']:+.4f} SOL")
    print(f"  bank + trailed slice          {r['policy']:+.4f} SOL")
    print(f"  DIFFERENCE                    {r['policy'] - r['base']:+.4f} SOL "
          f"over {len(banked)} banked coins")
    rows = sorted(r["rows"], key=lambda x: -x["delta"])
    if rows:
        top = rows[0]
        rest = r["policy"] - r["base"] - top["delta"]
        print(f"  without the best coin ({top['symbol']})   {rest:+.4f} SOL")
    print("  how the slice was sold: "
          + "  ".join(f"{k}={v}" for k, v in sorted(r["why"].items(), key=lambda kv: -kv[1])))
    if a.detail:
        print()
        print(f"  {'symbol':<13}{'flag':<9}{'bank_x':>8}{'high':>8}{'slice_x':>9}"
              f"{'why':>26}{'mins':>7}{'looks':>7}{'delta SOL':>11}")
        for x in rows:
            print(f"  {x['symbol'][:12]:<13}{(x['flag'] or '')[:8]:<9}{x['bank_x']:>8.2f}"
                  f"{x['high']:>8.2f}{x['slice_x']:>9.2f}{x['why']:>26}{x['mins']:>7.0f}"
                  f"{x['quotes']:>7}{x['delta']:>+11.4f}")
    print()
    print("  The slice is sold at the quote that breached the trail, 15s resolution, for a")
    print("  qsim-sized bag. This ranks rules against each other; it is not a P&L forecast.")


def _floats(s: str) -> list[float]:
    return [float(x) for x in s.split(",") if x.strip()]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=float, default=7.0)
    ap.add_argument("--since", metavar="YYYY-MM-DD",
                    help="start date. Dense bank paths exist from 2026-10-07 evening UTC.")
    ap.add_argument("--bank", type=float, default=2.0)
    ap.add_argument("--keep", type=float, default=0.20, help="fraction kept after the bank")
    ap.add_argument("--trail", type=float, default=0.45,
                    help="sell the slice this far below its running high (0 = no trail)")
    ap.add_argument("--tighten", metavar="LEVEL:PCT,...",
                    help="tighter trail once the high passes a level, e.g. 5:0.35,10:0.25")
    ap.add_argument("--floor", type=float, default=0.0,
                    help="sell the slice at or below this multiple of ENTRY (0 = none)")
    ap.add_argument("--target", type=float, default=0.0, help="sell the slice here (0 = none)")
    ap.add_argument("--max-hours", type=float, default=0.0,
                    help="sell the slice at the last look within this many hours of the exit")
    ap.add_argument("--dense-mins", type=float, default=45.0)
    ap.add_argument("--min-dense-quotes", type=int, default=60,
                    help="looks required in the first --dense-mins after the bank for a coin "
                         "to be scored (45 min at 15s is 180; 60 tolerates a busy hour)")
    ap.add_argument("--flag", help="only this security_flag")
    ap.add_argument("--mcap-min", type=float, default=80000.0)
    ap.add_argument("--mcap-max", type=float, default=120000.0, help="0 = no mcap band")
    ap.add_argument("--all-lanes", action="store_true")
    ap.add_argument("--detail", action="store_true")
    ap.add_argument("--sweep", action="store_true", help="grid over trail x keep")
    ap.add_argument("--trails", type=_floats, default=[0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.60])
    ap.add_argument("--keeps", type=_floats, default=[0.10, 0.20, 0.30, 0.50])
    a = ap.parse_args()
    try:
        report(a)
    finally:
        db.close_conn()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
