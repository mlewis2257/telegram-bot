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


def _project(args) -> float | None:
    """SOL size to project each trade to, or None for raw pnl_sol. `--size X` projects
    to X SOL/trade; `--normalize` is shorthand for `--size 1` (per-1-SOL, scale-invariant)."""
    if getattr(args, "size", None) is not None:
        return float(args.size)
    return 1.0 if args.normalize else None


def _q(query: str, project: float | None, **fmt) -> str:
    """Format a report query, then (if projecting) rewrite the PnL sum to a per-SOL
    return scaled to `project` SOL/trade — pnl_sol / sol_in * project. This is invariant
    to SHADOW_SOL_IN, so totals stay comparable across a size change AND show the PnL you'd
    have at any size. Targets only `sum(sp.pnl_sol)`, not win counts."""
    q = query.format(**fmt)
    if project is not None:
        q = q.replace("sum(sp.pnl_sol)",
                      f"sum(sp.pnl_sol / NULLIF(sp.sol_in, 0) * {float(project)})")
    return q


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
        cur.execute(_q(BY_DAY_QUERY, _project(args), phantom_clause=phantom_clause), params)
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


# Per-day-of-week pivot: which lanes earn on which weekdays + overall volume by day.
BY_DOW_QUERY = """
SELECT
  COALESCE(ch.handle, '?')                       AS channel,
  COALESCE(sp.vip_tier, 'none')                  AS vip_tier,
  COALESCE(c.skip_reason, 'none')                AS skip_reason,
  sp.exit_variant                                AS variant,
  EXTRACT(ISODOW FROM (sp.entry_time AT TIME ZONE 'UTC'))::int AS dow,
  round(sum(sp.pnl_sol), 2)                       AS dow_sol,
  count(*)                                        AS n
FROM shadow_positions sp
JOIN calls c ON c.id = sp.call_id
LEFT JOIN channels ch ON ch.id = c.channel_id
WHERE sp.status = 'closed'
  {phantom_clause}
  AND ( %(days)s = 0 OR sp.entry_time >= now() - (%(days)s || ' days')::interval )
GROUP BY 1, 2, 3, 4, 5
"""

_DOW_NAMES = {1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat", 7: "Sun"}


def _run_by_dow(conn, params: dict, phantom_clause: str, args) -> None:
    """Pivot PnL by day-of-WEEK (Mon..Sun) + show overall trade volume per weekday,
    so you can see which days are busiest and which lanes earn on which days."""
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(_q(BY_DOW_QUERY, _project(args), phantom_clause=phantom_clause), params)
        rows = cur.fetchall()

    label = f"last {args.days}d" if args.days else "all time"
    print(f"\nShadow PnL by day-of-week — {label}  (phantom-excluded, lanes >= {args.min_trades} trades)\n")
    if not rows:
        print("  No closed shadow positions in window.\n")
        return

    dows = list(range(1, 8))
    vol = {d: 0 for d in dows}
    lanes: dict[tuple, dict] = {}
    for r in rows:
        d = int(r["dow"])
        vol[d] += int(r["n"])
        key = (r["channel"], r["vip_tier"], r["skip_reason"], r["variant"])
        e = lanes.setdefault(key, {"dows": {}, "n": 0, "total": 0.0})
        sol = float(r["dow_sol"] or 0)
        e["dows"][d] = e["dows"].get(d, 0.0) + sol
        e["n"] += int(r["n"])
        e["total"] += sol

    print("  Volume (trades) by weekday:  " + "   ".join(f"{_DOW_NAMES[d]} {vol[d]}" for d in dows) + "\n")

    items = [(k, v) for k, v in lanes.items() if v["n"] >= args.min_trades]
    items.sort(key=lambda kv: kv[1]["total"], reverse=True)
    if not items:
        print(f"  No lanes with >= {args.min_trades} trades. Lower --min-trades.\n")
        return

    daycols = "".join(f"{_DOW_NAMES[d]:>8}" for d in dows)
    hdr = f"{'lane':<32}{'var':<9}{daycols}{'TOTAL':>9}{'n':>6}"
    print(hdr)
    print("-" * len(hdr))
    for (ch, tier, skip, var), v in items:
        lane = f"{(ch or '')[:11]:<11} {(tier or '')[:4]:<4} {(skip or '')[:13]:<13}"
        cells = "".join(f"{v['dows'].get(d, 0.0):>8.2f}" for d in dows)
        print(f"{lane:<32}{(var or '')[:8]:<9}{cells}{v['total']:>9.2f}{v['n']:>6}")
    print("\n  NEEDS SEVERAL WEEKS to trust — each weekday column is only ~(days/7) samples,")
    print("  so a single Saturday can dominate 'Sat' until you have many of them.\n")


# Hour-of-week pivot: total_sol per (weekday, hour-of-day) so you can see the intra-day
# SHAPE of a day's PnL — whether a bad weekday is bad all day or just a few hours. Hours
# are UTC (matches --by-dow). Optional channel/skip_reason filters isolate one lane
# (e.g. an anchor), since summing every lane lets the big losers swamp the cells.
BY_HOUR_QUERY = """
SELECT
  EXTRACT(ISODOW FROM (sp.entry_time AT TIME ZONE 'UTC'))::int AS dow,
  EXTRACT(HOUR   FROM (sp.entry_time AT TIME ZONE 'UTC'))::int AS hour,
  round(sum(sp.pnl_sol), 3)                                    AS sol,
  count(*)                                                     AS n
FROM shadow_positions sp
JOIN calls c ON c.id = sp.call_id
LEFT JOIN channels ch ON ch.id = c.channel_id
WHERE sp.status = 'closed'
  {phantom_clause}
  AND ( %(days)s = 0 OR sp.entry_time >= now() - (%(days)s || ' days')::interval )
  {lane_filter}
GROUP BY 1, 2
"""


def _run_by_hour(conn, params: dict, phantom_clause: str, args) -> None:
    """Grid of total_sol by weekday (cols) × hour-of-day (rows, UTC) — the intra-day
    shape of PnL, to see whether a bad day is bad all day or just a tight window."""
    lane_filter, scope = _lane_filter(args, params)
    q = _q(BY_HOUR_QUERY, _project(args), phantom_clause=phantom_clause, lane_filter=lane_filter)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q, params)
        rows = cur.fetchall()

    label = f"last {args.days}d" if args.days else "all time"
    print(f"\nShadow PnL by hour-of-week — {label}  (UTC hours, phantom-excluded, {scope})\n")
    if not rows:
        print("  No closed shadow positions in window.\n")
        return

    dows = list(range(1, 8))
    grid = {(d, h): 0.0 for d in dows for h in range(24)}
    voln = {(d, h): 0 for d in dows for h in range(24)}
    for r in rows:
        d, h = int(r["dow"]), int(r["hour"])
        grid[(d, h)] += float(r["sol"] or 0)
        voln[(d, h)] += int(r["n"])

    daycols = "".join(f"{_DOW_NAMES[d]:>8}" for d in dows)
    hdr = f"{'UTC':>3}  {daycols}{'TOTAL':>9}"
    print(hdr)
    print("-" * len(hdr))
    dow_tot = {d: 0.0 for d in dows}
    for h in range(24):
        cells, rowtot = "", 0.0
        for d in dows:
            v = grid[(d, h)]
            dow_tot[d] += v
            rowtot += v
            cells += f"{v:>8.2f}"
        print(f"{h:>3}  {cells}{rowtot:>9.2f}")
    print("-" * len(hdr))
    tot_cells = "".join(f"{dow_tot[d]:>8.2f}" for d in dows)
    print(f"{'TOT':>3}  {tot_cells}{sum(dow_tot.values()):>9.2f}")
    vol_cells = "".join(f"{sum(voln[(d, h)] for h in range(24)):>8d}" for d in dows)
    print(f"{'n':>3}  {vol_cells}")
    print("\n  Read DOWN a column for a day's intra-day shape; a real time-of-day effect")
    print("  is a band of same-hour cells with the same sign across MULTIPLE day columns.")
    print("  One cell = one hour of one day = tiny sample. UTC hours; PDT = UTC-7.\n")


def _lane_filter(args, params: dict):
    """Build the optional channel/vip_tier/skip_reason WHERE fragment + a scope label,
    shared by the lane-filtered breakdowns. Mutates `params` with the bind values."""
    frag, parts = "", []
    if args.channel:
        frag += " AND ch.handle ILIKE %(channel)s"
        params["channel"] = f"%{args.channel}%"
        parts.append(f"channel~{args.channel}")
    if args.vip_tier:
        frag += " AND COALESCE(sp.vip_tier, 'none') = %(tier)s"
        params["tier"] = args.vip_tier
        parts.append(f"tier={args.vip_tier}")
    if args.skip_reason:
        frag += " AND COALESCE(c.skip_reason, 'none') = %(skip)s"
        params["skip"] = args.skip_reason
        parts.append(f"skip_reason={args.skip_reason}")
    return frag, (", ".join(parts) if parts else "all lanes")


# Weekday × WEEK pivot: the same weekday split across calendar weeks, so you can see
# whether a lane's "good day" REPEATS (a real day-of-week edge) or was one lucky week.
# This is the persistence test for the day patterns --by-dow only hints at. Needs
# several weeks of history to say anything (use a wide --days, e.g. 28+).
DOW_WEEKS_QUERY = """
SELECT
  EXTRACT(ISODOW FROM (sp.entry_time AT TIME ZONE 'UTC'))::int AS dow,
  date_trunc('week', (sp.entry_time AT TIME ZONE 'UTC'))::date AS wk,
  round(sum(sp.pnl_sol), 2)                                    AS sol,
  count(*)                                                     AS n
FROM shadow_positions sp
JOIN calls c ON c.id = sp.call_id
LEFT JOIN channels ch ON ch.id = c.channel_id
WHERE sp.status = 'closed'
  {phantom_clause}
  AND ( %(days)s = 0 OR sp.entry_time >= now() - (%(days)s || ' days')::interval )
  {lane_filter}
GROUP BY 1, 2
"""


def _run_dow_weeks(conn, params: dict, phantom_clause: str, args) -> None:
    """For a (filtered) lane, show each weekday's PnL split BY WEEK + a '+wk' consistency
    count — turning '<lane> had one good Monday' into 'is Monday RELIABLY good?'."""
    lane_filter, scope = _lane_filter(args, params)
    q = _q(DOW_WEEKS_QUERY, _project(args), phantom_clause=phantom_clause, lane_filter=lane_filter)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(q, params)
        rows = cur.fetchall()

    label = f"last {args.days}d" if args.days else "all time"
    print(f"\nWeekday PnL across weeks — {label}  (UTC, phantom-excluded, {scope})\n")
    if not rows:
        print("  No closed shadow positions in window.\n")
        return

    weeks = sorted({r["wk"] for r in rows})
    sol = {(int(r["dow"]), r["wk"]): float(r["sol"] or 0) for r in rows}
    have = {(int(r["dow"]), r["wk"]) for r in rows if int(r["n"]) > 0}

    wkcols = "".join(f"{w.strftime('%m/%d'):>9}" for w in weeks)
    hdr = f"{'wkday':<6}{wkcols}{'mean':>9}{'+wk':>8}"
    print(hdr)
    print("-" * len(hdr))
    for d in range(1, 8):
        cells, vals, pos = "", [], 0
        for w in weeks:
            if (d, w) in have:
                v = sol.get((d, w), 0.0)
                cells += f"{v:>9.2f}"
                vals.append(v)
                pos += 1 if v > 0 else 0
            else:
                cells += f"{'·':>9}"
        mean = sum(vals) / len(vals) if vals else 0.0
        print(f"{_DOW_NAMES[d]:<6}{cells}{mean:>9.2f}{(f'{pos}/{len(vals)}'):>8}")
    print("\n  Each column is one ISO week (Mon-start). '+wk' = weeks that weekday was")
    print("  POSITIVE / weeks it traded. A real day-of-week edge is a HIGH, repeating +wk")
    print("  ratio — NOT one green week. '·' = lane didn't trade that weekday that week.\n")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=0, help="0 = all time")
    ap.add_argument("--raw", action="store_true",
                    help="include phantom-price rows (peak>50x or pnl outside [-100%%,+5000%%])")
    ap.add_argument("--by-day", action="store_true",
                    help="per-day total_sol per lane×variant (spot consistency vs one lucky day)")
    ap.add_argument("--by-dow", action="store_true",
                    help="per-day-of-week pivot + volume by weekday (needs weeks of data)")
    ap.add_argument("--by-hour", action="store_true",
                    help="weekday × hour-of-day grid of total_sol (intra-day shape; UTC)")
    ap.add_argument("--dow-weeks", action="store_true",
                    help="a lane's weekday PnL split BY WEEK + consistency count (use --days 28+)")
    ap.add_argument("--channel", default=None,
                    help="--by-hour: filter to channels whose handle matches (ILIKE)")
    ap.add_argument("--vip-tier", default=None,
                    help="--by-hour: filter to one vip tier (none|safe|gamble|gamble_risk)")
    ap.add_argument("--skip-reason", default=None,
                    help="--by-hour: filter to one lane category (e.g. low_score)")
    ap.add_argument("--min-trades", type=int, default=10,
                    help="--by-day/--by-dow: hide lanes with fewer than this many closed trades")
    ap.add_argument("--normalize", action="store_true",
                    help="report PnL per 1 SOL deployed (pnl_sol/sol_in) — comparable across SHADOW_SOL_IN changes")
    ap.add_argument("--size", type=float, default=None,
                    help="project PnL to this SOL/trade (e.g. --size 2). Implies --normalize at that size.")
    args = ap.parse_args()

    _proj = _project(args)
    if _proj is not None:
        if _proj == 1.0:
            print("\n[NORMALIZED] values are PnL per 1 SOL deployed (pnl_sol / sol_in) — "
                  "scale-invariant; multiply by your intended size to project.")
        else:
            print(f"\n[PROJECTED → {_proj:g} SOL/trade] values are PnL as if every position "
                  f"were {_proj:g} SOL (size-invariant projection from pnl_sol / sol_in).")

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
    if args.by_dow:
        _run_by_dow(conn, params, phantom_clause, args)
        return
    if args.by_hour:
        _run_by_hour(conn, params, phantom_clause, args)
        return
    if args.dow_weeks:
        _run_dow_weeks(conn, params, phantom_clause, args)
        return

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(_q(QUERY, _project(args), phantom_clause=phantom_clause), params)
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
