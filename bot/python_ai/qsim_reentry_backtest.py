"""
qsim_reentry_backtest.py — would re-buying a coin AFTER qsim exited have paid?

THE QUESTION
------------
Post-exit probing showed that the coins which eventually run 10x-168x almost
never do it while we hold them. PAID peaked at 2.70x in-hold, got trailed out,
and hit 168x THIRTY-SIX HOURS LATER. At the moment the trail fired it was
indistinguishable from the 63 other 2-5x coins that trailed out and died, so no
exit-side parameter can separate them (same shape as profit_floor_validated).

What IS different is time. The move arrives on a clock, a median ~10h after we
sold, and the post-exit probes already SEE it -- that is how we know it happened.
So the testable mechanism is not a looser leash, it is a second entry.

This script replays re-entry rules against the probe series we already have. It
executes nothing and changes no state.

HONESTY MACHINERY (read before trusting any number this prints)
---------------------------------------------------------------
1. NO LOOK-AHEAD. The trigger may only read probes at or before the moment it
   fires; the exit may only read probes strictly after the re-entry. Both are
   enforced by a single forward pass per position -- there is no place to peek.

2. THE BUY IS HAIRCUT BY THE MEASURED ROUND-TRIP. A probe's real_mult is a SELL
   quote (sol_out / sol_in on the original bag). Re-entering AT that number would
   assume we buy on the bid, which is free money. Instead we buy at
   `m / roundtrip`, where `roundtrip` is the MEASURED median of each position's
   first in-life observation -- i.e. what a real buy-then-immediately-sell round
   trip actually returned. Override with --roundtrip; --roundtrip 1.0 prints the
   free-money version for comparison and is clearly labelled as such.

3. PROBE CADENCE IS ~23 MINUTES, vs ~32s while qsim holds. Exits therefore
   realize LATE and gap harder than anything in the live book. The realized gap
   is reported so the number is not read as achievable.

4. COVERAGE IS A SELECTION. Positions with no probes cannot show a re-entry and
   are NOT counted as "no trade" -- they are excluded, and the excluded count is
   printed. Never scale a per-position figure to the whole book without it.

5. BEST-OF-N IS INFLATED. This grid tests many policies against one sample; the
   best one is biased upward by construction. Every policy is therefore also
   reported split into two halves by exit date (h1/h2). A policy that only works
   in one half is noise. This is the --min-obs lesson, pre-empted.

Examples:
    python3 qsim_reentry_backtest.py --days 14
    python3 qsim_reentry_backtest.py --days 14 --by-path --detail
    python3 qsim_reentry_backtest.py --days 14 --roundtrip 1.0   # free-money check
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))

# Same sanity ceiling the replay tooling uses, so a single bad quote cannot
# invent a 10,000x. Env-tunable because the real tail genuinely reaches 168x.
MAX_QOBS_MULT = float(os.getenv("QSIM_REPLAY_MAX_QOBS_MULT", "1000"))


# ── data ────────────────────────────────────────────────────────────────────
SQL = """
WITH pos AS (
    SELECT qp.call_id, qp.token_id, qp.entry_time, qp.exit_time, qp.exit_reason,
           qp.sol_in, qp.pnl_sol,
           coalesce(qp.partial_fraction, 0) AS partial_fraction,
           coalesce(qp.runner_peak_mult, 0) AS runner_peak_mult
    FROM qsim_positions qp
    WHERE qp.status = 'closed'
      AND qp.exit_time IS NOT NULL
      AND qp.entry_time >= now() - (%(days)s || ' days')::interval
),
life AS (
    -- The HELD window only. Post-exit probes are excluded here or the "peak we
    -- saw while holding" would silently include price action after the sale.
    SELECT q.call_id,
           max(q.real_mult)                                          AS held_peak,
           (array_agg(q.real_mult ORDER BY q.observed_at))[1]        AS first_mult,
           (array_agg(q.real_mult ORDER BY q.observed_at DESC))[1]   AS last_mult,
           count(*)                                                  AS life_obs
    FROM qsim_quote_observations q
    JOIN pos p ON p.call_id = q.call_id
    WHERE q.real_mult IS NOT NULL
      AND q.real_mult > 0
      AND q.real_mult <= %(maxmult)s
      AND (q.note IS NULL OR q.note NOT LIKE 'post_exit_probe%%')
      AND q.observed_at <= p.exit_time + interval '5 seconds'
    GROUP BY q.call_id
),
probes AS (
    SELECT q.call_id,
           json_agg(json_build_object('t', q.observed_at, 'm', q.real_mult)
                    ORDER BY q.observed_at) AS series,
           count(*) AS n_probes
    FROM qsim_quote_observations q
    JOIN pos p ON p.call_id = q.call_id
    WHERE q.real_mult IS NOT NULL
      AND q.real_mult > 0
      AND q.real_mult <= %(maxmult)s
      AND q.note LIKE 'post_exit_probe%%'
      AND q.observed_at > p.exit_time
    GROUP BY q.call_id
)
SELECT p.call_id, t.symbol, p.entry_time, p.exit_time, p.exit_reason,
       p.sol_in, p.pnl_sol, p.partial_fraction, p.runner_peak_mult,
       l.held_peak, l.first_mult, l.last_mult, l.life_obs,
       pr.series, coalesce(pr.n_probes, 0) AS n_probes
FROM pos p
JOIN tokens t ON t.id = p.token_id
LEFT JOIN life   l  ON l.call_id  = p.call_id
LEFT JOIN probes pr ON pr.call_id = p.call_id
ORDER BY p.exit_time
"""


def _rows(days: int) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor
    import db

    db.ensure_qsim_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SQL, {"days": days, "maxmult": MAX_QOBS_MULT})
        return [dict(r) for r in cur.fetchall()]


def _f(v: Any, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _dt(v: Any) -> datetime | None:
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if not v:
        return None
    try:
        p = datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except ValueError:
        return None
    return p if p.tzinfo else p.replace(tzinfo=timezone.utc)


def _series(raw: Any) -> list[tuple[datetime, float]]:
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    out: list[tuple[datetime, float]] = []
    for item in raw or []:
        t, m = _dt(item.get("t")), _f(item.get("m"))
        if t and 0 < m <= MAX_QOBS_MULT:
            out.append((t, m))
    out.sort(key=lambda x: x[0])
    return out


def _path(row: dict[str, Any]) -> str:
    """Which exit path the ORIGINAL position took. 3 of the 20x coins were hard
    stops, so re-entry must be tested on those too, not just on runners."""
    if _f(row.get("partial_fraction")) > 0:
        return "banked+ran"
    if "hard_stop" in (row.get("exit_reason") or ""):
        return "hard_stop"
    return "other"


# ── policies ────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class Policy:
    name: str
    # trigger
    ref: str            # 'held_peak' | 'exit_mult' — what the trigger measures against
    trig_k: float       # fire when probe >= ref * trig_k
    abs_min: float      # ...and probe >= this absolute multiple of ORIGINAL entry
    delay_min: float    # ...and at least this many minutes have passed since exit
    # management of the re-entered position (all relative to the re-entry price)
    trail_pct: float    # 0 disables
    floor: float        # 0 disables
    target: float       # 0 disables
    horizon_h: float    # 0 disables


def default_policies() -> list[Policy]:
    """A curated set, not an exhaustive grid. Each line answers a question.

    reclaim_*  : "it got back above the high it made while we held it"
    break_*    : "it is making a NEW high, well past anything we saw"
    naive_hold : the null hypothesis — buy the first probe and sit. If a real
                 policy cannot beat this, the trigger is adding nothing.
    """
    return [
        # the null
        Policy("naive_hold",      "exit_mult", 0.0, 0.00,  0,  0.0, 0.0, 0.0, 48),
        # reclaim the in-hold high
        Policy("reclaim_1p0",     "held_peak", 1.00, 1.00, 15, 0.5, 0.5, 0.0, 48),
        Policy("reclaim_1p2",     "held_peak", 1.20, 1.00, 15, 0.5, 0.5, 0.0, 48),
        Policy("reclaim_1p5",     "held_peak", 1.50, 1.00, 15, 0.5, 0.5, 0.0, 48),
        # reclaim, looser leash on the re-entry
        Policy("reclaim_1p2_t70", "held_peak", 1.20, 1.00, 15, 0.7, 0.4, 0.0, 48),
        Policy("reclaim_1p2_t30", "held_peak", 1.20, 1.00, 15, 0.3, 0.7, 0.0, 48),
        # reclaim with a hard profit target instead of a trail
        Policy("reclaim_1p2_x3",  "held_peak", 1.20, 1.00, 15, 0.0, 0.5, 3.0, 48),
        # new-high breakouts measured off where we sold
        Policy("break_2x_exit",   "exit_mult", 2.00, 1.00, 15, 0.5, 0.5, 0.0, 48),
        Policy("break_3x_exit",   "exit_mult", 3.00, 1.00, 15, 0.5, 0.5, 0.0, 48),
        # only re-enter coins already well above the ORIGINAL entry
        Policy("reclaim_above2x", "held_peak", 1.00, 2.00, 15, 0.5, 0.5, 0.0, 48),
        Policy("reclaim_above3x", "held_peak", 1.00, 3.00, 15, 0.5, 0.5, 0.0, 48),
        # patience: ignore the first two hours entirely
        Policy("reclaim_1p2_2h",  "held_peak", 1.20, 1.00, 120, 0.5, 0.5, 0.0, 48),
    ]


@dataclass
class Trade:
    call_id: int
    symbol: str
    path: str
    exit_time: datetime
    entry_at: datetime
    entry_mult: float      # ORIGINAL-entry multiple at which we re-bought (pre-haircut)
    basis: float           # haircut buy basis
    exit_mult: float
    ret: float             # exit_mult / basis
    pnl_sol: float
    hours_held: float
    reason: str
    gap_min: float         # probe gap immediately before the closing observation
    peak_ret: float        # best return seen while re-holding


def simulate(row: dict[str, Any], pol: Policy, roundtrip: float,
             stake: float | None) -> Trade | None:
    """One forward pass. Trigger reads only past probes; exit only later ones."""
    series = _series(row.get("series"))
    if not series:
        return None
    exit_time = _dt(row.get("exit_time"))
    if exit_time is None:
        return None

    held_peak = _f(row.get("held_peak"))
    exit_mult = _f(row.get("last_mult"))
    ref = held_peak if pol.ref == "held_peak" else exit_mult
    if ref <= 0 and pol.trig_k > 0:
        # A policy that triggers off a reference cannot run without one. The null
        # policy (trig_k == 0) has no reference by design, so it must NOT be
        # silently given a smaller candidate set than the policies it benchmarks.
        return None

    size = stake if stake is not None else _f(row.get("sol_in"))
    if size <= 0:
        return None

    entered = False
    basis = 0.0
    entry_at: datetime | None = None
    entry_mult = 0.0
    peak_m = 0.0
    prev_t: datetime | None = None

    for t, m in series:
        if not entered:
            if (t - exit_time).total_seconds() < pol.delay_min * 60:
                prev_t = t
                continue
            if m >= ref * pol.trig_k and m >= pol.abs_min:
                # We BUY here, so we pay the ask: the sell-quote multiple m is
                # divided by the measured round-trip, never used raw.
                entry_mult = m
                basis = m / roundtrip
                entry_at = t
                peak_m = m
                entered = True
            prev_t = t
            continue

        # ── holding the re-entry ────────────────────────────────────────────
        peak_m = max(peak_m, m)
        ret = m / basis
        reason: str | None = None
        if pol.target > 0 and ret >= pol.target:
            reason = "target"
        elif pol.floor > 0 and ret <= pol.floor:
            reason = "floor"
        elif pol.trail_pct > 0 and peak_m > 0 and m <= peak_m * (1.0 - pol.trail_pct):
            reason = "trail"
        elif pol.horizon_h > 0 and entry_at is not None \
                and (t - entry_at).total_seconds() >= pol.horizon_h * 3600:
            reason = "horizon"

        if reason:
            gap = (t - prev_t).total_seconds() / 60.0 if prev_t else 0.0
            return Trade(
                call_id=int(row["call_id"]), symbol=row.get("symbol") or "?",
                path=_path(row), exit_time=exit_time, entry_at=entry_at, # type: ignore[arg-type]
                entry_mult=entry_mult, basis=basis, exit_mult=m, ret=ret,
                pnl_sol=size * (ret - 1.0),
                hours_held=(t - entry_at).total_seconds() / 3600.0,  # type: ignore[union-attr]
                reason=reason, gap_min=gap, peak_ret=peak_m / basis,
            )
        prev_t = t

    if entered and entry_at is not None:
        # Ran out of probes still holding. This is NOT a result — the position is
        # unresolved. Marked to the last quote and counted separately so it can
        # never be mistaken for a realized win.
        t, m = series[-1]
        ret = m / basis
        return Trade(
            call_id=int(row["call_id"]), symbol=row.get("symbol") or "?",
            path=_path(row), exit_time=exit_time, entry_at=entry_at,
            entry_mult=entry_mult, basis=basis, exit_mult=m, ret=ret,
            pnl_sol=size * (ret - 1.0),
            hours_held=(t - entry_at).total_seconds() / 3600.0,
            reason="unresolved", gap_min=0.0, peak_ret=peak_m / basis,
        )
    return None


# ── reporting ───────────────────────────────────────────────────────────────
def _pct(n: float, d: float) -> float:
    return 100.0 * n / d if d else 0.0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=14)
    ap.add_argument("--min-probes", type=int, default=5,
                    help="a coin watched once cannot show a recovery; below this "
                         "the row proves nothing and is excluded (default 5)")
    ap.add_argument("--roundtrip", type=float, default=None,
                    help="buy-then-sell round-trip return. Default: MEASURED median "
                         "of each position's first in-life quote. 1.0 = free money.")
    ap.add_argument("--stake", type=float, default=None,
                    help="SOL per re-entry (default: the position's own sol_in)")
    ap.add_argument("--by-path", action="store_true",
                    help="split results by the ORIGINAL exit path")
    ap.add_argument("--detail", type=int, nargs="?", const=15, default=0,
                    help="list the N biggest re-entry trades of the best policy")
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    rows = _rows(args.days)
    if not rows:
        print("no closed qsim positions in window")
        return 1

    total = len(rows)
    usable = [r for r in rows if int(r.get("n_probes") or 0) >= args.min_probes]
    excluded = total - len(usable)

    # ── measured round trip ────────────────────────────────────────────────
    firsts = [_f(r.get("first_mult")) for r in rows if _f(r.get("first_mult")) > 0]
    measured = statistics.median(firsts) if firsts else 1.0
    roundtrip = args.roundtrip if args.roundtrip is not None else measured

    # ── probe cadence, so nobody reads these exits as live-achievable ──────
    cadences: list[float] = []
    for r in usable:
        s = _series(r.get("series"))
        if len(s) > 1:
            span = (s[-1][0] - s[0][0]).total_seconds() / 60.0
            cadences.append(span / (len(s) - 1))
    p50_cadence = statistics.median(cadences) if cadences else 0.0

    print(f"window            {args.days}d   closed positions {total}")
    print(f"probe coverage    {len(usable)} usable (>= {args.min_probes} probes), "
          f"{excluded} excluded — per-position figures apply to the usable subset ONLY")
    print(f"round trip        {roundtrip:.4f}"
          + ("  (MEASURED median first in-life quote)" if args.roundtrip is None
             else "  (OVERRIDE — 1.0 means buys are free, which they are not)"))
    print(f"probe cadence     p50 {p50_cadence:.1f} min between probes "
          f"(qsim holds at ~0.5 min; exits below realize LATE)")
    print()

    # median exit date splits the sample for the stability check
    exits = sorted(_dt(r["exit_time"]) for r in usable if r.get("exit_time"))
    mid = exits[len(exits) // 2] if exits else None

    results: list[dict[str, Any]] = []
    trades_by_policy: dict[str, list[Trade]] = {}

    for pol in default_policies():
        trades = [t for t in (simulate(r, pol, roundtrip, args.stake) for r in usable)
                  if t is not None]
        trades_by_policy[pol.name] = trades
        resolved = [t for t in trades if t.reason != "unresolved"]
        unresolved = [t for t in trades if t.reason == "unresolved"]
        pnl = sum(t.pnl_sol for t in trades)
        h1 = sum(t.pnl_sol for t in trades if mid and t.exit_time <= mid)
        h2 = sum(t.pnl_sol for t in trades if mid and t.exit_time > mid)
        rets = sorted(t.ret for t in trades)
        results.append({
            "policy": pol.name,
            "re": len(trades),
            "rate": _pct(len(trades), len(usable)),
            "win": _pct(sum(1 for t in trades if t.ret > 1.0), len(trades)),
            "pnl_sol": pnl,
            "per_re": pnl / len(trades) if trades else 0.0,
            "per_cand": pnl / len(usable) if usable else 0.0,
            "p50_ret": statistics.median(rets) if rets else 0.0,
            "max_ret": max(rets) if rets else 0.0,
            "unres": len(unresolved),
            "h1": h1,
            "h2": h2,
            "p50_gap": statistics.median([t.gap_min for t in resolved]) if resolved else 0.0,
        })

    if args.json:
        print(json.dumps(results, indent=2, default=str))
        return 0

    hdr = (f"{'policy':<18}{'re':>5}{'rate%':>7}{'win%':>7}{'pnl_sol':>10}"
           f"{'per_re':>9}{'per_cand':>10}{'p50_ret':>9}{'max_ret':>9}"
           f"{'unres':>7}{'h1':>9}{'h2':>9}{'gap_m':>7}")
    print(hdr)
    print("-" * len(hdr))
    for r in sorted(results, key=lambda x: -x["pnl_sol"]):
        print(f"{r['policy']:<18}{r['re']:>5}{r['rate']:>7.1f}{r['win']:>7.1f}"
              f"{r['pnl_sol']:>10.4f}{r['per_re']:>9.4f}{r['per_cand']:>10.4f}"
              f"{r['p50_ret']:>9.2f}{r['max_ret']:>9.2f}{r['unres']:>7}"
              f"{r['h1']:>9.4f}{r['h2']:>9.4f}{r['p50_gap']:>7.1f}")

    print()
    print(f"{len(results)} policies tested against ONE sample. The top row is biased "
          f"upward by that search alone —")
    print("h1/h2 are the two halves by exit date: a policy positive in only one half is noise.")
    print("'unres' still held when the probes ran out — marked to last quote, NOT a realized win.")

    base = sum(_f(r.get("pnl_sol")) for r in usable)
    print(f"\noriginal book over the {len(usable)} usable positions: {base:+.4f} SOL")

    if args.by_path:
        best = max(results, key=lambda x: x["pnl_sol"])["policy"]
        print(f"\nby original exit path — policy '{best}'")
        print(f"{'path':<14}{'cand':>6}{'re':>5}{'pnl_sol':>10}{'per_re':>9}{'max_ret':>9}")
        counts: dict[str, int] = {}
        for r in usable:
            counts[_path(r)] = counts.get(_path(r), 0) + 1
        for path in sorted(counts):
            ts = [t for t in trades_by_policy[best] if t.path == path]
            pnl = sum(t.pnl_sol for t in ts)
            print(f"{path:<14}{counts[path]:>6}{len(ts):>5}{pnl:>10.4f}"
                  f"{(pnl / len(ts) if ts else 0):>9.4f}"
                  f"{(max((t.ret for t in ts), default=0)):>9.2f}")

    if args.detail:
        best = max(results, key=lambda x: x["pnl_sol"])["policy"]
        ts = sorted(trades_by_policy[best], key=lambda t: -t.pnl_sol)[:args.detail]
        print(f"\ntop {len(ts)} re-entry trades — policy '{best}'")
        print(f"{'symbol':<12}{'path':<12}{'buy@':>7}{'basis':>7}{'sell@':>8}"
              f"{'ret':>7}{'pnl':>9}{'hrs':>7}{'peak':>7}  reason")
        for t in ts:
            print(f"{t.symbol[:11]:<12}{t.path:<12}{t.entry_mult:>7.2f}{t.basis:>7.2f}"
                  f"{t.exit_mult:>8.2f}{t.ret:>7.2f}{t.pnl_sol:>9.4f}"
                  f"{t.hours_held:>7.1f}{t.peak_ret:>7.2f}  {t.reason}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
