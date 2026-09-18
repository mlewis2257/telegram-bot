"""
qsim_confirm_entry_backtest.py — don't buy the call. Buy the CONFIRMATION.

THE IDEA
--------
Coins that reach 1.3x are materially more likely to keep going: conditional on
banking, P(reach 10x) is 3.3% against 1.47% unconditional. So rather than trying
to predict at call time which coins will run -- every static entry feature we
collect has been tested and is null -- wait for the coin to prove it, and use the
1.3x crossing itself as the entry filter. Skip the ~70% that never confirm.

That is a momentum entry, and it is different in kind from everything tried so
far, because it conditions on price action instead of on metadata.

WHAT COULD KILL IT (both are measured here, not assumed)
--------------------------------------------------------
1. YOU PAY 1.3x FOR THE ENTRY. A coin reaching 2x from the call is only 1.54x
   from a confirmed entry; a 10x is 7.7x. The improved hit rate has to cover a
   30% worse basis. If continuation from 1.3x to 1.69x is no better than the
   base rate of reaching 1.3x at all, the filter is pure cost.

2. YOUR STOP SITS UNDER A COIN THAT JUST RAN. A -20% stop from a confirmed
   entry is 1.04x of the call price, and the trough data says most fizzlers
   revisit that level. Confirmation buys a better population and a worse stop
   placement at the same time.

HONESTY MACHINERY
-----------------
- ENTRY IS ON THE NEXT QUOTE AFTER THE CROSSING, not the crossing itself
  (--enter-on cross to see the optimistic version). Live, you cannot fill at the
  tick that triggers you: detection, quote and swap all take time, and this is
  exactly where a confirmation strategy bleeds. The default is the honest one.

- THE BUY IS HAIRCUT by the measured round trip (median first in-life quote),
  because quote multiples are SELL quotes and filling at one is free money.

- THE BASELINE IS PAIRED AND CORRECT. For each position the comparison is
  (this policy's PnL, or ZERO if it never confirmed) against what qsim actually
  booked on that same position. Skipping a trade books nothing -- it does not
  book the loss you avoided -- and that avoided loss is where the edge would
  come from. CI is a paired bootstrap over positions on the difference.

- POSITIONS WHOSE QUOTES ARE TOO SPARSE to see a crossing are reported, not
  silently treated as "never confirmed."

- Policies are also split h1/h2 by entry date, and the policy count is printed,
  because a grid against one sample flatters its own best row.

Read-only. Executes nothing, writes nothing.

    python3 qsim_confirm_entry_backtest.py --days 30
    python3 qsim_confirm_entry_backtest.py --days 30 --enter-on cross   # optimistic
"""

from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))

MAX_QOBS_MULT = float(os.getenv("QSIM_REPLAY_MAX_QOBS_MULT", "1000"))

SQL = """
WITH pos AS (
    SELECT qp.call_id, qp.token_id, qp.entry_time, qp.exit_time,
           qp.sol_in, qp.pnl_sol
    FROM qsim_positions qp
    WHERE qp.status = 'closed'
      AND qp.exit_time IS NOT NULL
      AND qp.sol_in > 0
      AND qp.entry_time >= now() - (%(days)s || ' days')::interval
),
obs AS (
    -- In-life quotes AND post-exit probes, merged. Both are entry-relative
    -- multiples on the original bag (runner_mult divides by the reduced basis,
    -- which restores the full-bag multiple -- see qsim.py:786), so the merged
    -- series is one continuous price path from entry.
    SELECT q.call_id,
           json_agg(json_build_object('t', q.observed_at, 'm', q.real_mult)
                    ORDER BY q.observed_at) AS series,
           count(*) AS n_obs,
           min(q.real_mult) FILTER (WHERE q.observed_at <= p.exit_time) AS dummy_min
    FROM qsim_quote_observations q
    JOIN pos p ON p.call_id = q.call_id
    WHERE q.real_mult IS NOT NULL AND q.real_mult > 0 AND q.real_mult <= %(maxmult)s
    GROUP BY q.call_id
),
firstq AS (
    SELECT DISTINCT ON (q.call_id) q.call_id, q.real_mult AS first_mult
    FROM qsim_quote_observations q
    JOIN pos p ON p.call_id = q.call_id
    WHERE q.real_mult IS NOT NULL AND q.real_mult > 0
      AND (q.note IS NULL OR q.note NOT LIKE 'post_exit_probe%%')
    ORDER BY q.call_id, q.observed_at
)
SELECT p.call_id, t.symbol, p.entry_time, p.sol_in, p.pnl_sol,
       o.series, coalesce(o.n_obs, 0) AS n_obs, f.first_mult
FROM pos p
JOIN tokens t ON t.id = p.token_id
LEFT JOIN obs    o ON o.call_id = p.call_id
LEFT JOIN firstq f ON f.call_id = p.call_id
ORDER BY p.entry_time
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


def _f(v: Any, d: float = 0.0) -> float:
    if v is None:
        return d
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


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
    out = []
    for it in raw or []:
        t, m = _dt(it.get("t")), _f(it.get("m"))
        if t and 0 < m <= MAX_QOBS_MULT:
            out.append((t, m))
    out.sort(key=lambda x: x[0])
    return out


@dataclass(frozen=True)
class Policy:
    name: str
    confirm: float     # enter once the coin trades at/above this multiple of the call
    stop: float        # exit at this fraction of the CONFIRMED entry (0 disables)
    bank: float        # exit at this multiple of the confirmed entry (0 disables)
    trail: float       # trail this fraction off the confirmed-entry peak (0 disables)
    horizon_h: float


def policies() -> list[Policy]:
    return [
        # mirror the current live shape, but entered on confirmation
        Policy("c1.3_b1.3_s0.8",  1.30, 0.80, 1.30, 0.00, 48),
        Policy("c1.3_b1.5_s0.8",  1.30, 0.80, 1.50, 0.00, 48),
        Policy("c1.3_b2.0_s0.8",  1.30, 0.80, 2.00, 0.00, 48),
        # wider stop: the whole risk is that 0.8 sits under a coin that just ran
        Policy("c1.3_b1.5_s0.7",  1.30, 0.70, 1.50, 0.00, 48),
        Policy("c1.3_b1.5_s0.6",  1.30, 0.60, 1.50, 0.00, 48),
        # let it run instead of banking
        Policy("c1.3_trail40",    1.30, 0.70, 0.00, 0.40, 48),
        Policy("c1.3_trail50",    1.30, 0.70, 0.00, 0.50, 48),
        # demand more confirmation
        Policy("c1.5_b1.5_s0.7",  1.50, 0.70, 1.50, 0.00, 48),
        Policy("c2.0_b1.5_s0.7",  2.00, 0.70, 1.50, 0.00, 48),
        Policy("c2.0_trail50",    2.00, 0.70, 0.00, 0.50, 48),
        # less confirmation, to see which direction the gradient runs
        Policy("c1.2_b1.5_s0.7",  1.20, 0.70, 1.50, 0.00, 48),
    ]


def simulate(row: dict[str, Any], pol: Policy, roundtrip: float,
             enter_on: str) -> tuple[float, str] | None:
    """Forward pass. Returns (pnl_sol, reason), or None if never confirmed."""
    series = _series(row.get("series"))
    if not series:
        return None
    size = _f(row.get("sol_in"))
    if size <= 0:
        return None

    basis = 0.0
    entry_at: datetime | None = None
    peak = 0.0
    armed = False

    for i, (t, m) in enumerate(series):
        if basis == 0.0:
            if not armed:
                if m >= pol.confirm:
                    armed = True
                    if enter_on == "cross":
                        basis, entry_at, peak = m / roundtrip, t, m
                continue
            # armed: fill on the NEXT observation, which is what live can do
            basis, entry_at, peak = m / roundtrip, t, m
            continue

        peak = max(peak, m)
        r = m / basis
        reason = None
        if pol.bank > 0 and r >= pol.bank:
            reason = "bank"
        elif pol.stop > 0 and r <= pol.stop:
            reason = "stop"
        elif pol.trail > 0 and m <= peak * (1.0 - pol.trail):
            reason = "trail"
        elif pol.horizon_h > 0 and entry_at is not None \
                and (t - entry_at).total_seconds() >= pol.horizon_h * 3600:
            reason = "horizon"
        if reason:
            return (size * (r - 1.0), reason)

    if basis > 0.0:
        # still holding when quotes ran out — mark to last, flagged as unresolved
        return (size * (series[-1][1] / basis - 1.0), "unresolved")
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--enter-on", choices=("next", "cross"), default="next",
                    help="'next' fills on the quote AFTER the crossing (honest: "
                         "live cannot fill on the tick that triggers it). "
                         "'cross' is the optimistic version, for comparison.")
    ap.add_argument("--roundtrip", type=float, default=None,
                    help="default: MEASURED median first in-life quote")
    ap.add_argument("--min-obs", type=int, default=3,
                    help="positions with fewer quotes cannot be judged to have "
                         "missed a crossing; they are excluded and counted")
    ap.add_argument("--boot", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=20260918)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    rows = _rows(args.days)
    if not rows:
        print("no closed qsim positions in window")
        return 1

    firsts = [_f(r.get("first_mult")) for r in rows if _f(r.get("first_mult")) > 0]
    measured = statistics.median(firsts) if firsts else 1.0
    roundtrip = args.roundtrip if args.roundtrip is not None else measured

    usable = [r for r in rows if int(r.get("n_obs") or 0) >= args.min_obs]
    excluded = len(rows) - len(usable)
    base_pnl = sum(_f(r["pnl_sol"]) for r in usable)
    base_sol = sum(_f(r["sol_in"]) for r in usable)

    ents = sorted(_dt(r["entry_time"]) for r in usable if r.get("entry_time"))
    mid = ents[len(ents) // 2] if ents else None

    print(f"window        {args.days}d   {len(rows)} closed positions, "
          f"{len(usable)} with >= {args.min_obs} quotes ({excluded} excluded)")
    print(f"baseline      qsim booked {base_pnl:+.4f} SOL on those "
          f"({100 * base_pnl / base_sol if base_sol else 0:+.2f}%/SOL)")
    print(f"round trip    {roundtrip:.4f}   fill on: {args.enter_on.upper()}"
          + ("  (honest — live cannot fill the trigger tick)" if args.enter_on == "next"
             else "  (OPTIMISTIC — fills the trigger tick itself)"))
    print()

    rng = random.Random(args.seed)
    out = []
    for pol in policies():
        per: list[float] = []      # policy pnl per position (0 when not taken)
        diffs: list[float] = []    # paired difference vs what qsim booked
        taken = wins = unres = 0
        h1 = h2 = 0.0
        for r in usable:
            res = simulate(r, pol, roundtrip, args.enter_on)
            p = 0.0
            if res is not None:
                p, reason = res
                taken += 1
                if p > 0:
                    wins += 1
                if reason == "unresolved":
                    unres += 1
            per.append(p)
            d = p - _f(r["pnl_sol"])
            diffs.append(d)
            if mid and _dt(r["entry_time"]) and _dt(r["entry_time"]) <= mid:
                h1 += d
            else:
                h2 += d
        # paired bootstrap on the difference
        n = len(diffs)
        idx = list(range(n))
        sums = []
        for _ in range(args.boot):
            pick = rng.choices(idx, k=n)
            sums.append(sum(diffs[i] for i in pick))
        sums.sort()
        lo = sums[int(0.025 * len(sums))]
        hi = sums[min(len(sums) - 1, int(0.975 * len(sums)))]
        out.append({
            "policy": pol.name, "taken": taken,
            "rate": 100.0 * taken / len(usable) if usable else 0.0,
            "pnl": sum(per), "per_trade": sum(per) / taken if taken else 0.0,
            "vs_base": sum(diffs), "ci_lo": lo, "ci_hi": hi,
            "win": 100.0 * wins / taken if taken else 0.0,
            "unres": unres, "h1": h1, "h2": h2,
        })

    if args.json:
        print(json.dumps(out, indent=2, default=str))
        return 0

    hdr = (f"{'policy':<18}{'taken':>7}{'take%':>7}{'pnl_sol':>10}{'per_trade':>11}"
           f"{'vs_base':>10}{'ci_lo':>9}{'ci_hi':>9}{'win%':>7}{'unres':>7}"
           f"{'h1':>9}{'h2':>9}")
    print(hdr)
    print("-" * len(hdr))
    for r in sorted(out, key=lambda x: -x["vs_base"]):
        print(f"{r['policy']:<18}{r['taken']:>7}{r['rate']:>7.1f}{r['pnl']:>10.4f}"
              f"{r['per_trade']:>11.4f}{r['vs_base']:>10.4f}{r['ci_lo']:>9.3f}"
              f"{r['ci_hi']:>9.3f}{r['win']:>7.1f}{r['unres']:>7}"
              f"{r['h1']:>9.3f}{r['h2']:>9.3f}")

    print()
    print("  vs_base = this policy's PnL minus what qsim actually booked, position")
    print("  by position. A skipped trade books ZERO, not the loss it avoided —")
    print("  that avoided loss IS the edge, and it is already inside vs_base.")
    print("  ci_lo/ci_hi are a PAIRED bootstrap on that difference: if ci_lo is")
    print("  below zero the policy has not been shown to beat doing nothing.")
    print(f"  {len(out)} policies against one sample — the top row flatters itself;")
    print("  h1/h2 must agree in sign before any of this is worth trading.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
