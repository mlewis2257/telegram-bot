"""
dev_history_edge.py — is the clean-deployer filter real, with the look-ahead removed?

THE FINDING THIS VALIDATES
--------------------------
Bucketing qsim trades by the deployer's prior track record produced the first
signal in this project that improves %/SOL rather than merely cutting exposure:

    any prior rug   -26.75 %/SOL      (last 30d)
    0 prior         -17.34
    1-2 clean       -11.69
    3+ clean         -0.18   <- breakeven, 293 trades

It orders correctly in BOTH directions, replicates across eras at a uniformly
worse level in the older one, and its profit is not concentrated (top 5 tokens
are 33.7% of gross wins). Those are the cheap checks and it passed them all.

THE EXPENSIVE CHECK: LOOK-AHEAD
-------------------------------
The SQL version counted a deployer's prior tokens by first_call ORDER ONLY. A
token called yesterday whose qsim position is still open, or closed only this
morning, was still counted as known history. You cannot trade on a rug verdict
that had not happened yet, and this is the fourth time in this investigation
that a result has hinged on accidentally reading the future.

Here a prior token counts ONLY if its qsim position CLOSED strictly before the
current call was made. That is the information a live bot would actually have.
--loose reproduces the old ordering-only behaviour so the two can be compared:
if the edge collapses under strict timing, it was never tradeable.

THE OTHER TWO CAVEATS, BOTH PARAMETERISED
-----------------------------------------
* 95% of creators resolved via the first_tx fee payer, not DAS. Launchpads pay
  deploy fees for their users, so some "deployers" are relayers. --source
  restricts to das_creators to see whether the edge survives on clean identity.
* The factory cutoff matters: top creators cluster at 38-39 tokens, right under
  the default 40. --factory-min sweeps it, because an address that "deploys"
  dozens of tokens is infrastructure and fakes a long clean history.

Every bucket gets a bootstrap CI, because -0.18%/SOL on 281 tokens with a
heavy-tailed PnL distribution is not distinguishable from a wide range of
values, and the point estimate alone would mislead.

Read-only. Executes nothing, writes nothing.

    python3 dev_history_edge.py --days 30
    python3 dev_history_edge.py --days 30 --loose          # old behaviour
    python3 dev_history_edge.py --days 30 --factory-min 20
    python3 dev_history_edge.py --days 30 --source das_creators
"""

from __future__ import annotations

import argparse
import bisect
import os
import random
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))

SQL = """
SELECT t.id                                   AS token_id,
       t.symbol                               AS symbol,
       tc.creator_address                     AS creator,
       tc.creator_source                      AS source,
       min(c.created_at)                      AS first_call,
       max(qp.exit_time)                      AS last_exit,
       bool_or(qp.pnl_pct <= -80)             AS rugged,
       coalesce(sum(qp.pnl_sol), 0)           AS pnl_sol,
       coalesce(sum(qp.sol_in), 0)            AS sol_in,
       count(qp.call_id)                      AS trades
FROM tokens t
JOIN token_creators tc ON tc.token_id = t.id
JOIN calls c           ON c.token_id  = t.id
LEFT JOIN qsim_positions qp ON qp.call_id = c.id AND qp.status = 'closed'
WHERE tc.creator_address IS NOT NULL
GROUP BY t.id, t.symbol, tc.creator_address, tc.creator_source
"""


def _rows() -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor
    import db
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SQL)
        return [dict(r) for r in cur.fetchall()]


def _dt(v: Any) -> datetime | None:
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    return None


def _f(v: Any) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def bucket_of(prior_n: int, prior_rugs: int) -> str:
    if prior_rugs > 0:
        return "z: any prior rug"
    if prior_n == 0:
        return "a: 0 prior"
    if prior_n <= 2:
        return "b: 1-2 clean"
    return "c: 3+ clean"


def boot_ci(pnl: list[float], sol: list[float], iters: int,
            rng: random.Random) -> tuple[float, float]:
    n = len(pnl)
    if n < 2:
        return (float("nan"), float("nan"))
    idx = list(range(n))
    vals = []
    for _ in range(iters):
        pick = rng.choices(idx, k=n)
        sp = ss = 0.0
        for i in pick:
            sp += pnl[i]
            ss += sol[i]
        vals.append(100.0 * sp / ss if ss else 0.0)
    vals.sort()
    return (vals[int(0.025 * len(vals))], vals[min(len(vals) - 1, int(0.975 * len(vals)))])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--loose", action="store_true",
                    help="count prior tokens by call ORDER only, ignoring whether "
                         "their outcome was known yet — the old, look-ahead version")
    ap.add_argument("--factory-min", type=int, default=40,
                    help="creators holding this many tokens are infrastructure "
                         "and are dropped (default 40)")
    ap.add_argument("--source", default=None,
                    help="restrict to one creator_source, e.g. das_creators")
    ap.add_argument("--boot", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=20260919)
    args = ap.parse_args()

    rows = _rows()
    if args.source:
        rows = [r for r in rows if (r.get("source") or "") == args.source]
    if not rows:
        print("no rows — has the backfill run?")
        return 1

    per_creator: dict[str, int] = defaultdict(int)
    for r in rows:
        per_creator[r["creator"]] += 1
    factories = {c for c, n in per_creator.items() if n >= args.factory_min}
    rows = [r for r in rows if r["creator"] not in factories]

    by_creator: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        if _dt(r["first_call"]):
            by_creator[r["creator"]].append(r)

    cutoff = datetime.now(timezone.utc).timestamp() - args.days * 86400
    buckets: dict[str, list[dict]] = defaultdict(list)
    unknown_at_call = 0

    for toks in by_creator.values():
        toks.sort(key=lambda r: _dt(r["first_call"]))
        resolved: list[float] = []        # last_exit timestamps of prior tokens
        resolved_rug: list[float] = []    # ...of prior tokens that rugged
        order_n = order_rugs = 0
        for r in toks:
            call_ts = _dt(r["first_call"]).timestamp()
            if args.loose:
                p_n, p_rugs = order_n, order_rugs
            else:
                # STRICT: only priors whose outcome was already settled.
                p_n = bisect.bisect_left(resolved, call_ts)
                p_rugs = bisect.bisect_left(resolved_rug, call_ts)
                unknown_at_call += order_n - p_n
            if int(r["trades"] or 0) > 0 and call_ts >= cutoff:
                buckets[bucket_of(p_n, p_rugs)].append(r)
            order_n += 1
            order_rugs += 1 if r["rugged"] else 0
            le = _dt(r["last_exit"])
            if le:
                bisect.insort(resolved, le.timestamp())
                if r["rugged"]:
                    bisect.insort(resolved_rug, le.timestamp())

    rng = random.Random(args.seed)
    mode = "LOOSE (call order only — look-ahead)" if args.loose \
        else "STRICT (prior outcome must have settled before the call)"
    print(f"window        last {args.days}d   prior-history mode: {mode}")
    print(f"factories     {len(factories)} creators dropped at >= {args.factory_min} tokens"
          + (f"   source={args.source}" if args.source else ""))
    if not args.loose:
        print(f"look-ahead    {unknown_at_call} prior-token outcomes were NOT yet known "
              f"at call time and are excluded here but counted by --loose")
    print()

    hdr = (f"{'bucket':<20}{'tokens':>8}{'trades':>8}{'pnl_sol':>10}{'%/SOL':>9}"
           f"{'ci_lo':>9}{'ci_hi':>9}{'rug%':>8}{'top5%':>8}")
    print(hdr)
    print("-" * len(hdr))
    for name in sorted(buckets):
        rs = buckets[name]
        pnl = [_f(r["pnl_sol"]) for r in rs]
        sol = [_f(r["sol_in"]) for r in rs]
        tot_sol = sum(sol)
        pct = 100.0 * sum(pnl) / tot_sol if tot_sol else 0.0
        lo, hi = boot_ci(pnl, sol, args.boot, rng)
        wins = sorted([p for p in pnl if p > 0], reverse=True)
        top5 = 100.0 * sum(wins[:5]) / sum(wins) if wins else 0.0
        rug = 100.0 * sum(1 for r in rs if r["rugged"]) / len(rs)
        print(f"{name:<20}{len(rs):>8}{sum(int(r['trades']) for r in rs):>8}"
              f"{sum(pnl):>10.4f}{pct:>9.2f}{lo:>9.2f}{hi:>9.2f}{rug:>8.1f}{top5:>8.1f}")

    print()
    print("  The claim is NOT that '3+ clean' is profitable — it is that it beats")
    print("  '0 prior'. Read whether their CIs overlap. A bucket whose own CI")
    print("  straddles zero is breakeven-or-unknown, not an edge.")
    print("  top5% is the share of GROSS WINS from the 5 best tokens; near 100")
    print("  would mean the bucket is a few lucky coins wearing a filter's clothes.")
    print()
    print("  Then run --loose. If the edge is much larger there, the difference is")
    print("  exactly the rug verdicts that had not happened yet at call time, and")
    print("  only the STRICT number is tradeable.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
