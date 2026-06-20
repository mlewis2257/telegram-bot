"""
shadow_report.py — tradeable PnL of shadow lanes, segmented by lane.

Shows the REAL (entry-relative, real-exit) outcome of the gated lanes we're
shadow-trading — the honest answer to "which VIP lanes are worth trading."

    python3 shadow_report.py            # all time
    python3 shadow_report.py --days 7
    python3 shadow_report.py --raw      # don't drop phantom-price rows
    python3 shadow_report.py --by-day --days 7   # per-day total_sol per lane×variant

PHANTOM GUARD: cross-source / stale-supply pricing still leaks the occasional
impossible exit into the shadow set (e.g. a +6716% avg on a lane whose avg peak
is only 1.7x, or an avg_peak in the thousands). Those single rows dominate a
sum(pnl_sol) and fake out the early-vs-ride comparison. By default we exclude any
closed position whose peak or pnl is outside sane memecoin bounds and list them
in an EXCLUDED block so the bad price source can be chased down. --raw disables it.
"""

from __future__ import annotations

import argparse
import os
import sys

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db

# Sane memecoin bounds. Real runners reach ~45x ($willo2 +4412%), so 50x leaves
# headroom; anything past it — or a loss worse than -100% (impossible) — is a
# cross-source/stale-supply pricing artifact, not a trade.
MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0   # +5000% == 51x
MIN_SANE_PNL_PCT = -100.5   # can't lose more than the position

# Reused by both the aggregate (negated) and the EXCLUDED listing.
PHANTOM_PRED = """(
       sp.peak_multiplier > %(max_peak)s
    OR sp.pnl_pct > %(max_pnl)s
    OR sp.pnl_pct < %(min_pnl)s
)"""

QUERY = """
SELECT
  COALESCE(ch.handle, '?')                                   AS channel,
  COALESCE(sp.vip_tier, 'none')                              AS vip_tier,
  COALESCE(c.skip_reason, 'none')                            AS skip_reason,
  sp.exit_variant                                            AS variant,
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
LEFT JOIN channels ch ON ch.id = c.channel_id
WHERE ( %(days)s = 0 OR sp.entry_time >= now() - (%(days)s || ' days')::interval )
  {phantom_clause}
GROUP BY 1, 2, 3, 4
ORDER BY channel, vip_tier, skip_reason, variant
"""

# Individual positions dropped by the phantom guard — so the bad price can be traced.
EXCLUDED_QUERY = """
SELECT
  COALESCE(ch.handle, '?')  AS channel,
  sp.exit_variant           AS variant,
  tok.symbol                AS symbol,
  sp.peak_multiplier        AS peak,
  sp.pnl_pct                AS pnl_pct,
  sp.pnl_sol                AS pnl_sol,
  sp.exit_reason            AS exit_reason
FROM shadow_positions sp
JOIN calls c ON c.id = sp.call_id
LEFT JOIN channels ch ON ch.id = c.channel_id
LEFT JOIN tokens tok ON tok.id = sp.token_id
WHERE sp.status = 'closed'
  AND ( %(days)s = 0 OR sp.entry_time >= now() - (%(days)s || ' days')::interval )
  AND """ + PHANTOM_PRED + """
ORDER BY sp.peak_multiplier DESC NULLS LAST, sp.pnl_pct DESC
"""


# Per-day pivot: total_sol per (lane, variant) per UTC entry-day. Same phantom guard.
BY_DAY_QUERY = """
SELECT
  COALESCE(ch.handle, '?')                       AS channel,
  COALESCE(sp.vip_tier, 'none')                  AS vip_tier,
  COALESCE(c.skip_reason, 'none')                AS skip_reason,
  sp.exit_variant                                AS variant,
  (sp.entry_time AT TIME ZONE 'UTC')::date        AS day,
  round(sum(sp.pnl_sol), 2)                       AS day_sol,
  count(*)                                        AS n
FROM shadow_positions sp
JOIN calls c ON c.id = sp.call_id
LEFT JOIN channels ch ON ch.id = c.channel_id
WHERE sp.status = 'closed'
  {phantom_clause}
  AND ( %(days)s = 0 OR sp.entry_time >= now() - (%(days)s || ' days')::interval )
GROUP BY 1, 2, 3, 4, 5
"""


def _run_by_day(conn, params: dict, phantom_clause: str, args) -> None:
    """Pivot closed-trade PnL into one row per (lane, variant) with a column per day,
    so you can see whether a lane is consistently positive vs one lucky day."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(BY_DAY_QUERY.format(phantom_clause=phantom_clause), params)
        rows = cur.fetchall()

    label = f"last {args.days}d" if args.days else "all time"
    print(f"\nShadow PnL by day — {label}  (total_sol per UTC entry-day, phantom-excluded,"
          f" lanes with >= {args.min_trades} trades)\n")
    if not rows:
        print("  No closed shadow positions in window.\n")
        return

    days = sorted({r["day"] for r in rows})
    lanes: dict[tuple, dict] = {}
    for r in rows:
        key = (r["channel"], r["vip_tier"], r["skip_reason"], r["variant"])
        e = lanes.setdefault(key, {"days": {}, "n": 0, "total": 0.0})
        sol = float(r["day_sol"] or 0)
        e["days"][r["day"]] = sol
        e["n"] += int(r["n"])
        e["total"] += sol

    items = [(k, v) for k, v in lanes.items() if v["n"] >= args.min_trades]
    items.sort(key=lambda kv: kv[1]["total"], reverse=True)
    if not items:
        print(f"  No lanes with >= {args.min_trades} trades. Lower --min-trades.\n")
        return

    daycols = "".join(f"{d.strftime('%m/%d'):>8}" for d in days)
    hdr = f"{'lane':<32}{'var':<9}{daycols}{'TOTAL':>9}{'n':>6}"
    print(hdr)
    print("-" * len(hdr))
    for (ch, tier, skip, var), v in items:
        lane = f"{(ch or '')[:11]:<11} {(tier or '')[:4]:<4} {(skip or '')[:13]:<13}"
        cells = "".join(f"{v['days'].get(d, 0.0):>8.2f}" for d in days)
        print(f"{lane:<32}{(var or '')[:8]:<9}{cells}{v['total']:>9.2f}{v['n']:>6}")
    print("\n  Read each row L→R: a real edge is positive across MOST days, not one spike.\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=0, help="0 = all time")
    ap.add_argument("--raw", action="store_true",
                    help="include phantom-price rows (peak>50x or pnl outside [-100%%,+5000%%])")
    ap.add_argument("--by-day", action="store_true",
                    help="per-day total_sol per lane×variant (spot consistency vs one lucky day)")
    ap.add_argument("--min-trades", type=int, default=10,
                    help="--by-day: hide lanes with fewer than this many closed trades")
    args = ap.parse_args()

    params = {
        "days": args.days,
        "max_peak": MAX_SANE_PEAK,
        "max_pnl": MAX_SANE_PNL_PCT,
        "min_pnl": MIN_SANE_PNL_PCT,
    }
    # Exclude only CLOSED phantom rows; open rows have NULL pnl/peak and must stay
    # counted, so guard with status + IS TRUE (NULL predicate → keep the row).
    phantom_clause = "" if args.raw else (
        f"AND NOT (sp.status = 'closed' AND {PHANTOM_PRED} IS TRUE)"
    )

    db.ensure_shadow_positions_table()
    conn = db.get_conn()
    db.safe_rollback()

    if args.by_day:
        _run_by_day(conn, params, phantom_clause, args)
        return

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(QUERY.format(phantom_clause=phantom_clause), params)
        rows = cur.fetchall()
        excluded = []
        if not args.raw:
            cur.execute(EXCLUDED_QUERY, params)
            excluded = cur.fetchall()

    label = f"last {args.days}d" if args.days else "all time"
    print(f"\nShadow lane PnL — {label} (real entry, real exits, isolated from main)\n")
    if not rows:
        print("  No shadow positions yet. Set SHADOW_LANES (e.g. gamble,gamble_risk),")
        print("  restart the listener, and run shadow_monitor.py.\n")
        return

    hdr = (f"{'channel':<14} {'tier':<7} {'skip_reason':<14} {'var':<8} {'closed':>6} {'open':>5} {'win%':>6} "
           f"{'avg%':>7} {'total_sol':>10} {'avg_pk':>7} {'2x':>4}")
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        print(f"{(r['channel'] or '')[:14]:<14} {(r['vip_tier'] or '')[:7]:<7} "
              f"{(r['skip_reason'] or '')[:14]:<14} "
              f"{(r['variant'] or '')[:8]:<8} "
              f"{r['closed']:>6} {r['still_open']:>5} "
              f"{(r['win_rate'] if r['win_rate'] is not None else 0):>6} "
              f"{(r['avg_pnl_pct'] if r['avg_pnl_pct'] is not None else 0):>7} "
              f"{(r['total_sol'] if r['total_sol'] is not None else 0):>10} "
              f"{(r['avg_peak'] if r['avg_peak'] is not None else 0):>7} "
              f"{r['hit_2x']:>4}")
    print("\n  Compare 'early' vs 'ride' within each lane: ride should show bigger avg_peak")
    print("  and (if your thesis holds) higher total_sol despite a lower win rate.")

    if not args.raw:
        if excluded:
            print(f"\n  EXCLUDED {len(excluded)} phantom-price row(s) from the totals above")
            print("  (peak>50x or pnl outside [-100%,+5000%] — a cross-source/stale-supply")
            print("   artifact, not a real trade). Chase these price sources down:\n")
            ehdr = (f"    {'channel':<14} {'var':<8} {'symbol':<14} "
                    f"{'peak':>9} {'pnl%':>12} {'pnl_sol':>10} {'exit':<12}")
            print(ehdr)
            print("    " + "-" * (len(ehdr) - 4))
            for e in excluded:
                print(f"    {(e['channel'] or '')[:14]:<14} {(e['variant'] or '')[:8]:<8} "
                      f"{(e['symbol'] or '?')[:14]:<14} "
                      f"{(float(e['peak']) if e['peak'] is not None else 0):>9.1f} "
                      f"{(float(e['pnl_pct']) if e['pnl_pct'] is not None else 0):>12.1f} "
                      f"{(float(e['pnl_sol']) if e['pnl_sol'] is not None else 0):>10.3f} "
                      f"{(e['exit_reason'] or '')[:12]:<12}")
            print("\n  Run with --raw to include them. Re-run after fixing the price source.")
        else:
            print("\n  No phantom-price rows in window — totals are clean.")
    print()


if __name__ == "__main__":
    main()
