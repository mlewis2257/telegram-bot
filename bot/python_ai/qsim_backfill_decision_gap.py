"""
qsim_backfill_decision_gap.py — stamp `decision_gap_secs` on historical qsim rows and
relabel closes that were decided on a stale quote.

WHY
---
qsim's exit thresholds can only fire when a sell quote comes back. On 2026-09-05 the quote
budget was mis-allocated (three flat positions ate 89% of it while 19 others got 21 quotes
between them), so positions ran 1-4 HOURS unobserved and were then closed on the first quote
that finally arrived — at -95% to -100%. Those rows were written as `hard_stop`, but the -20%
stop never got to evaluate anything: they are rugs discovered late, not stops taken.

qsim.py now records this at write time. This script applies the same rule BACKWARDS, so an
already-collected window can be salvaged instead of thrown away.

THE RULE (identical to qsim._decision_gap_secs)
    decision_gap_secs = how long the position went UNOBSERVED immediately before the quote
    that closed it:
      >= 2 quotes  -> closing quote  minus  the one before it
      == 1 quote   -> that quote     minus  entry_time   (blind since entry)
      == 0 quotes  -> exit_time      minus  entry_time   (never observed at all)

    Rate-limited rows are NOT observations (a 429 saw no price). No-route rows ARE — we
    looked, and the answer was "unsellable".

    gap > threshold  ->  exit_reason gets a `stale_` prefix.

The prefix matters more than the column: every report that does GROUP BY exit_reason
separates these automatically, including ad-hoc SQL that has never heard of
decision_gap_secs. A column alone can be silently ignored; a different label cannot.

Idempotent — already-prefixed rows are left alone. Dry-run unless --apply.

Examples:
    python3 qsim_backfill_decision_gap.py --since '2026-09-03 23:00 UTC'
    python3 qsim_backfill_decision_gap.py --since '2026-09-03 23:00 UTC' --apply
    python3 qsim_backfill_decision_gap.py --days 30 --threshold 120 --apply
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))

import db


SQL = """
WITH obs AS (
    SELECT
        qo.call_id,
        qo.observed_at,
        row_number() OVER (PARTITION BY qo.call_id ORDER BY qo.observed_at DESC) AS rn
    FROM qsim_quote_observations qo
    JOIN qsim_positions qp ON qp.call_id = qo.call_id
    WHERE qo.rate_limited = false
      AND qo.note IS DISTINCT FROM 'post_exit_probe'
      AND qp.exit_time IS NOT NULL
      AND qo.observed_at <= qp.exit_time
)
SELECT
    qp.call_id,
    t.symbol,
    qp.channel_handle,
    qp.exit_reason,
    qp.sol_in,
    qp.sol_out,
    qp.pnl_sol,
    qp.decision_gap_secs,
    EXTRACT(epoch FROM (
        COALESCE(last_obs.observed_at, qp.exit_time)
        - COALESCE(prev_obs.observed_at, qp.entry_time)
    )) AS gap_secs,
    (SELECT count(*) FROM obs o WHERE o.call_id = qp.call_id) AS obs_count
FROM qsim_positions qp
JOIN tokens t ON t.id = qp.token_id
LEFT JOIN obs last_obs ON last_obs.call_id = qp.call_id AND last_obs.rn = 1
LEFT JOIN obs prev_obs ON prev_obs.call_id = qp.call_id AND prev_obs.rn = 2
WHERE qp.status = 'closed'
  AND qp.exit_reason IS NOT NULL
  {since_clause}
ORDER BY qp.entry_time
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default=None, help="only rows entered at/after this timestamp")
    ap.add_argument("--days", type=int, default=None, help="alternative to --since")
    ap.add_argument("--threshold", type=float,
                    default=float(os.getenv("QSIM_STALE_DECISION_SECS", "180")),
                    help="seconds unobserved above which a close is stale (default: env/180)")
    ap.add_argument("--apply", action="store_true", help="write changes (default: dry run)")
    args = ap.parse_args()

    if args.since:
        since_clause = "AND qp.entry_time >= %(since)s::timestamptz"
        params = {"since": args.since}
    elif args.days:
        since_clause = "AND qp.entry_time >= now() - (%(days)s || ' days')::interval"
        params = {"days": args.days}
    else:
        since_clause, params = "", {}

    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(SQL.format(since_clause=since_clause), params)
        cols = [d.name for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    if not rows:
        print("no closed qsim rows in range")
        return

    stale, clean, already = [], [], []
    for r in rows:
        gap = float(r["gap_secs"] or 0.0)
        r["gap_secs"] = gap
        if (r["exit_reason"] or "").startswith("stale_"):
            already.append(r)
        elif gap > args.threshold:
            stale.append(r)
        else:
            clean.append(r)

    def pnl(rs):
        return sum(float(x["pnl_sol"] or 0) for x in rs)

    def deployed(rs):
        return sum(float(x["sol_in"] or 0) for x in rs)

    print(f"threshold: {args.threshold:g}s unobserved before the closing quote")
    print(f"rows: {len(rows)}  clean={len(clean)}  stale={len(stale)}  already-labelled={len(already)}\n")

    for name, rs in (("CLEAN", clean), ("STALE", stale)):
        if not rs:
            continue
        dep = deployed(rs)
        wins = sum(1 for x in rs if float(x["pnl_sol"] or 0) > 0)
        print(f"{name:<6} n={len(rs):<4} pnl={pnl(rs):+.4f} SOL on {dep:.2f} deployed "
              f"= {(100*pnl(rs)/dep if dep else 0):+.2f}%/SOL   win={100*wins/len(rs):.0f}%")

    by_reason: dict[str, list] = defaultdict(list)
    for r in stale:
        by_reason[r["exit_reason"]].append(r)
    if by_reason:
        print("\nwould relabel:")
        for reason, rs in sorted(by_reason.items(), key=lambda kv: -len(kv[1])):
            print(f"  {reason:<16} -> stale_{reason:<16} n={len(rs):<4} pnl={pnl(rs):+.4f}")

        print("\nworst offenders (longest blind before close):")
        for r in sorted(stale, key=lambda x: -x["gap_secs"])[:15]:
            mult = (float(r["sol_out"] or 0) / float(r["sol_in"] or 1))
            print(f"  {(r['symbol'] or '?'):<12} {r['gap_secs']/60:>6.1f} min blind  "
                  f"obs={r['obs_count']:<4} exit={mult:.3f}x  {float(r['pnl_sol'] or 0):+.4f}  "
                  f"{r['exit_reason']}")

    if not args.apply:
        print("\nDRY RUN — re-run with --apply to write. Reverting later is a prefix strip:")
        print("  UPDATE qsim_positions SET exit_reason = replace(exit_reason, 'stale_', '')")
        print(r"   WHERE exit_reason LIKE 'stale\_%';")
        return

    with conn.cursor() as cur:
        # Stamp the measured gap on EVERY row in range, stale or not — the column is the
        # evidence, and a clean row proving it was clean is worth as much as a flagged one.
        for r in rows:
            cur.execute(
                "UPDATE qsim_positions SET decision_gap_secs = %s, updated_at = NOW() "
                "WHERE call_id = %s",
                (r["gap_secs"], r["call_id"]),
            )
        for r in stale:
            cur.execute(
                "UPDATE qsim_positions SET exit_reason = %s, updated_at = NOW() "
                "WHERE call_id = %s AND exit_reason NOT LIKE 'stale\\_%%'",
                (f"stale_{r['exit_reason']}", r["call_id"]),
            )
        conn.commit()
    print(f"\napplied: {len(rows)} gaps stamped, {len(stale)} rows relabelled stale_*")


if __name__ == "__main__":
    main()
