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
             enter_on: str, max_mult: float = 50.0, max_gap_min: float = 5.0,
             confirm_ticks: bool = True, max_fill_slip: float = 0.5
             ) -> tuple[float, str, float, float, float] | None:
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

    prev_t = None
    prev_m = 0.0
    trig_t = None
    trig_m = 0.0
    entry_mult = 0.0
    for i, (t, m) in enumerate(series):
        if m > max_mult:
            # Beyond the cap we do not believe the print. It must NOT become
            # prev_m either: a rejected price is not a price we could have sold
            # at, and using it as the blind-close fallback booked +121 SOL on a
            # single position (WOFI) and drove an entire bogus result.
            continue
        if basis == 0.0:
            if not armed:
                if m >= pol.confirm:
                    armed = True
                    trig_t, trig_m = t, m
                    if enter_on == "cross":
                        basis, entry_at, peak = m / roundtrip, t, m
                        entry_mult = m
                prev_t, prev_m = t, m
                continue
            # armed: fill on the NEXT observation — but only if that fill could
            # REALLY have happened. Two ways it could not, both of which
            # manufactured enormous fake wins (WOFI booked +121 SOL alone):
            #
            #   * the next quote is 30 MINUTES later (post-exit probe cadence).
            #     Nobody sends an order off half-hour-old data.
            #   * the price has moved wildly from the trigger. A high confirm
            #     level fires on a SPIKE and the next print is often the revert
            #     or an outright collapse; filling there sets a microscopic
            #     basis and every later quote becomes a vast multiple. That is
            #     why %/SOL ROSE with the confirm level (c2.0 +382, c1.5 +203,
            #     c1.2 +0.8) instead of falling, and why --enter-on cross, which
            #     never fills post-spike, was clean.
            if trig_t is not None and (t - trig_t).total_seconds() > max_gap_min * 60:
                return None
            if trig_m > 0 and not (1.0 - max_fill_slip <= m / trig_m <= 1.0 + max_fill_slip):
                return None
            basis, entry_at, peak = m / roundtrip, t, m
            entry_mult = m
            prev_t, prev_m = t, m
            continue

        # Coverage guard. In-life quotes are ~30s apart; post-exit probes are
        # ~30 MINUTES. An exit priced off a 30-minute-stale series is fiction:
        # the sim can take a spike no live bot could reach, and can sit through
        # a drawdown it never saw. Past max_gap we simply do not know, so close
        # at the last price we could actually have acted on and flag it.
        if max_gap_min > 0 and prev_t is not None \
                and (t - prev_t).total_seconds() > max_gap_min * 60:
            return (size * (prev_m / basis - 1.0), "blind", entry_mult, basis, prev_m)

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
            # A one-tick print is not a fill. Require the level to survive into
            # the next observation, and settle at the WORSE of the two.
            if reason in ("bank", "trail") and confirm_ticks:
                nxt = series[i + 1][1] if i + 1 < len(series) else None
                if nxt is None:
                    return (size * (r - 1.0), reason + "_unconfirmed", entry_mult, basis, m)
                r = min(r, nxt / basis)
            return (size * (r - 1.0), reason, entry_mult, basis, m)
        prev_t, prev_m = t, m

    if basis > 0.0:
        # still holding when quotes ran out — mark to last, flagged as unresolved
        return (size * (series[-1][1] / basis - 1.0), "unresolved", entry_mult, basis, series[-1][1])
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
    ap.add_argument("--max-mult", type=float, default=50.0,
                    help="ignore quotes above this multiple. The replay default "
                         "of 1000 let a single bad print book +34 SOL on one "
                         "trade and drove an entire bogus result (default 50)")
    ap.add_argument("--max-gap-min", type=float, default=5.0,
                    help="if quotes go quiet longer than this while holding, the "
                         "exit cannot be modelled honestly — in-life quotes are "
                         "~30s apart but post-exit probes are ~30 MINUTES. Close "
                         "at the last actionable price and flag it (default 5)")
    ap.add_argument("--max-fill-slip", type=float, default=0.5,
                    help="reject the trade if the fill quote differs from the "
                         "trigger quote by more than this fraction — a spike "
                         "that reverts before you fill is not a trade you took")
    ap.add_argument("--no-confirm-ticks", action="store_true",
                    help="allow single-tick prints to fill an exit (unsafe)")
    ap.add_argument("--detail", type=int, nargs="?", const=15, default=0,
                    help="list the N biggest contributing trades of the best policy")
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
        taken = wins = unres = blind = 0
        pol_sol = xb_pol = xb_base = 0.0
        h1 = h2 = 0.0
        contrib: list[tuple[float, str, str]] = []
        for r in usable:
            res = simulate(r, pol, roundtrip, args.enter_on, args.max_mult,
                           args.max_gap_min, not args.no_confirm_ticks,
                           args.max_fill_slip)
            p = 0.0
            if res is not None:
                p, reason, e_m, bas, x_m = res
                taken += 1
                if p > 0:
                    wins += 1
                if reason == "unresolved":
                    unres += 1
                if reason == "blind":
                    blind += 1
                contrib.append((p, r.get("symbol") or "?", reason, e_m, bas, x_m))
            per.append(p)
            if res is not None:
                pol_sol += _f(r["sol_in"])
                if reason != "blind":
                    xb_pol += p
                    xb_base += _f(r["pnl_sol"])
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
            "unres": unres, "blind": blind, "h1": h1, "h2": h2,
            "per_sol": 100.0 * sum(per) / pol_sol if pol_sol else 0.0,
            "vs_base_xb": xb_pol - xb_base,
            "contrib": sorted(contrib, key=lambda x: -x[0]),
        })

    if args.json:
        print(json.dumps(out, indent=2, default=str))
        return 0

    def top5(r) -> float:
        """Share of gross profit from the 5 best trades. Near 100 means the
        'edge' IS those trades, not a strategy."""
        pos = [c[0] for c in r["contrib"] if c[0] > 0]
        return 100.0 * sum(sorted(pos, reverse=True)[:5]) / sum(pos) if pos else 0.0

    hdr = (f"{'policy':<18}{'taken':>7}{'take%':>7}{'pnl_sol':>10}{'per_trade':>11}"
           f"{'%/SOL':>8}{'vs_base':>10}{'xblind':>9}{'ci_lo':>9}{'ci_hi':>9}{'win%':>7}{'unres':>7}"
           f"{'blind':>7}{'top5%':>7}{'h1':>9}{'h2':>9}")
    print(hdr)
    print("-" * len(hdr))
    for r in sorted(out, key=lambda x: -x["vs_base"]):
        print(f"{r['policy']:<18}{r['taken']:>7}{r['rate']:>7.1f}{r['pnl']:>10.4f}"
              f"{r['per_trade']:>11.4f}{r['per_sol']:>8.2f}{r['vs_base']:>10.4f}{r['vs_base_xb']:>9.3f}{r['ci_lo']:>9.3f}"
              f"{r['ci_hi']:>9.3f}{r['win']:>7.1f}{r['unres']:>7}{r['blind']:>7}"
              f"{top5(r):>7.0f}{r['h1']:>9.3f}{r['h2']:>9.3f}")

    print()
    print("  vs_base = this policy's PnL minus what qsim actually booked, position")
    print("  by position. A skipped trade books ZERO, not the loss it avoided —")
    print("  that avoided loss IS the edge, and it is already inside vs_base.")
    print("  ci_lo/ci_hi are a PAIRED bootstrap on that difference: if ci_lo is")
    print("  below zero the policy has not been shown to beat doing nothing.")
    if args.detail:
        best = max(out, key=lambda x: x["vs_base"])
        print(f"\ntop {args.detail} contributing trades — policy '{best['policy']}'")
        print(f"{'symbol':<14}{'pnl_sol':>10}{'entry@':>9}{'basis':>9}{'exit@':>10}"
              f"{'ret':>9}  reason")
        for c, sym, reason, e_m, bas, x_m in best["contrib"][:args.detail]:
            print(f"{sym[:13]:<14}{c:>10.4f}{e_m:>9.3f}{bas:>9.3f}{x_m:>10.3f}"
                  f"{(x_m / bas if bas else 0):>9.2f}  {reason}")
        print()
    print(f"  %/SOL is THIS POLICY's return on the capital it actually deployed.")
    print(f"  Compare it to the baseline %/SOL in the header: if they match, the")
    print("  filter is not picking better trades, it is only taking FEWER of them —")
    print("  which you could match by trading at random, and is not an edge.")
    print("  xblind = vs_base with unmodellable 'blind' exits removed from both sides.")
    print("  top5% is the share of gross profit from the 5 best trades — near 100")
    print("  means the 'edge' IS those trades. 'blind' closed because quote coverage")
    print("  went stale and the exit could not be modelled honestly.")
    print(f"  {len(out)} policies against one sample — the top row flatters itself;")
    print("  h1/h2 must agree in sign before any of this is worth trading.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
