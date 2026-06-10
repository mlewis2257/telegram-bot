"""
shadow_report.py — tradeable PnL of shadow lanes, segmented by lane.

Shows the REAL (entry-relative, real-exit) outcome of the gated lanes we're
shadow-trading — the honest answer to "which VIP lanes are worth trading."

    python3 shadow_report.py            # all time
    python3 shadow_report.py --days 7
"""

from __future__ import annotations

import argparse
import os
import sys

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db

QUERY = """
SELECT
  COALESCE(sp.vip_tier, 'none')                              AS vip_tier,
  COALESCE(c.skip_reason, 'none')                            AS skip_reason,
  count(*) FILTER (WHERE sp.status='closed')                 AS closed,
  count(*) FILTER (WHERE sp.status='open')                   AS still_open,
  count(*) FILTER (WHERE sp.pnl_sol > 0)                     AS wins,
  round(100.0 * count(*) FILTER (WHERE sp.pnl_sol > 0)
        / NULLIF(count(*) FILTER (WHERE sp.status='closed'),0), 1) AS win_rate,
  round(avg(sp.pnl_pct) FILTER (WHERE sp.status='closed'), 1) AS avg_pnl_pct,
  round(sum(sp.pnl_sol) FILTER (WHERE sp.status='closed'), 3) AS total_sol,
  round(avg(sp.peak_multiplier) FILTER (WHERE sp.status='closed'), 2) AS avg_peak,
  count(*) FILTER (WHERE sp.peak_multiplier >= 2)            AS hit_2x
FROM shadow_positions sp
JOIN calls c ON c.id = sp.call_id
WHERE ( %s = 0 OR sp.entry_time >= now() - (%s || ' days')::interval )
GROUP BY 1, 2
ORDER BY total_sol DESC NULLS LAST
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=0, help="0 = all time")
    args = ap.parse_args()

    db.ensure_shadow_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(QUERY, (args.days, args.days))
        rows = cur.fetchall()

    label = f"last {args.days}d" if args.days else "all time"
    print(f"\nShadow lane PnL — {label} (real entry, real exits, isolated from main)\n")
    if not rows:
        print("  No shadow positions yet. Set SHADOW_LANES (e.g. gamble,gamble_risk),")
        print("  restart the listener, and run shadow_monitor.py.\n")
        return

    hdr = (f"{'tier':<12} {'skip_reason':<16} {'closed':>6} {'open':>5} {'win%':>6} "
           f"{'avg%':>7} {'total_sol':>10} {'avg_pk':>7} {'2x':>4}")
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print(f"{(r['vip_tier'] or '')[:12]:<12} {(r['skip_reason'] or '')[:16]:<16} "
              f"{r['closed']:>6} {r['still_open']:>5} "
              f"{(r['win_rate'] if r['win_rate'] is not None else 0):>6} "
              f"{(r['avg_pnl_pct'] if r['avg_pnl_pct'] is not None else 0):>7} "
              f"{(r['total_sol'] if r['total_sol'] is not None else 0):>10} "
              f"{(r['avg_peak'] if r['avg_peak'] is not None else 0):>7} "
              f"{r['hit_2x']:>4}")
    print()


if __name__ == "__main__":
    main()
