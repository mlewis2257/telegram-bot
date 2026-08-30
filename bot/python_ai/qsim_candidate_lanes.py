"""
qsim_candidate_lanes.py — pick shadow-screened lanes worth quote-testing.

Shadow is a cheap radar, not the trading ruler. This report finds lanes whose
shadow_report performance is positive or close to breakeven, then emits a small
QSIM_EXTRA_LANES_JSON block so qsim can interrogate only the best suspects.

Example:
    python3 qsim_candidate_lanes.py --days 14 --min-closed 50 --near-sol -5 --top 5
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))


MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5
ALL_DAYS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


SQL = """
WITH closed AS (
    SELECT
        COALESCE(ch.handle, '?') AS channel,
        COALESCE(sp.vip_tier, 'none') AS vip_tier,
        COALESCE(c.skip_reason, 'none') AS lane,
        sp.exit_variant AS shadow_variant,
        (sp.entry_time AT TIME ZONE 'UTC')::date AS day,
        sp.pnl_sol / NULLIF(sp.sol_in, 0) AS pnl_per_sol,
        sp.peak_multiplier
    FROM shadow_positions sp
    JOIN calls c ON c.id = sp.call_id
    LEFT JOIN channels ch ON ch.id = c.channel_id
    WHERE sp.status = 'closed'
      AND (%(days)s = 0 OR sp.entry_time >= now() - (%(days)s || ' days')::interval)
      AND (%(since)s IS NULL OR sp.entry_time >= %(since)s::timestamptz)
      AND (%(channel)s = 'any' OR COALESCE(ch.handle, '?') = %(channel)s)
      AND (%(lane)s = 'any' OR COALESCE(c.skip_reason, 'none') = %(lane)s)
      AND (%(variant)s = 'any' OR sp.exit_variant = %(variant)s)
      AND NOT (
          COALESCE(sp.peak_multiplier, 0) > %(max_peak)s
       OR COALESCE(sp.pnl_pct, 0) > %(max_pnl)s
       OR COALESCE(sp.pnl_pct, 0) < %(min_pnl)s
      )
),
lane_day AS (
    SELECT
        channel,
        vip_tier,
        lane,
        shadow_variant,
        day,
        SUM(pnl_per_sol) AS day_pnl,
        COUNT(*) AS day_n
    FROM closed
    GROUP BY 1, 2, 3, 4, 5
),
lane_totals AS (
    SELECT
        channel,
        vip_tier,
        lane,
        shadow_variant,
        COUNT(*) AS closed,
        SUM(pnl_per_sol) AS total_per_sol,
        AVG(pnl_per_sol) AS avg_per_sol,
        AVG(peak_multiplier) AS avg_peak,
        COUNT(*) FILTER (WHERE pnl_per_sol > 0) AS wins,
        COUNT(*) FILTER (WHERE peak_multiplier >= 2) AS hit_2x,
        MAX(pnl_per_sol) AS best_trade,
        MIN(pnl_per_sol) AS worst_trade
    FROM closed
    GROUP BY 1, 2, 3, 4
),
day_totals AS (
    SELECT
        channel,
        vip_tier,
        lane,
        shadow_variant,
        COUNT(*) AS days_seen,
        COUNT(*) FILTER (WHERE day_pnl > 0) AS green_days,
        MAX(day_pnl) AS best_day,
        MIN(day_pnl) AS worst_day
    FROM lane_day
    GROUP BY 1, 2, 3, 4
)
SELECT
    lt.*,
    dt.days_seen,
    dt.green_days,
    dt.best_day,
    dt.worst_day
FROM lane_totals lt
JOIN day_totals dt USING (channel, vip_tier, lane, shadow_variant)
WHERE lt.closed >= %(min_closed)s
  AND lt.total_per_sol >= %(near_sol)s
ORDER BY lt.total_per_sol DESC, lt.closed DESC
"""


def _rows(params: dict[str, Any]) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor
    import db

    db.ensure_shadow_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SQL, params)
        return [dict(row) for row in cur.fetchall()]


def _f(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _score(row: dict[str, Any]) -> float:
    total = _f(row.get("total_per_sol"))
    closed = max(1, int(row.get("closed") or 0))
    green_days = int(row.get("green_days") or 0)
    days_seen = max(1, int(row.get("days_seen") or 0))
    best_share = max(0.0, _f(row.get("best_trade")) / total) if total > 0 else 1.0
    consistency = green_days / days_seen
    volume_bonus = min(closed, 300) / 300.0
    outlier_penalty = max(0.0, best_share - 0.35) * 2.0
    return total * (0.65 + 0.35 * consistency) * (0.75 + 0.25 * volume_bonus) - outlier_penalty


def _risk_note(row: dict[str, Any]) -> str:
    total = _f(row.get("total_per_sol"))
    best = _f(row.get("best_trade"))
    best_share = best / total if total > 0 else 0.0
    green_days = int(row.get("green_days") or 0)
    days_seen = int(row.get("days_seen") or 0)
    notes = []
    if total <= 0:
        notes.append("near")
    if best_share >= 0.50:
        notes.append("outlier")
    elif best_share >= 0.35:
        notes.append("spiky")
    if days_seen and green_days / days_seen < 0.45:
        notes.append("choppy")
    if _f(row.get("avg_peak")) > 3.0:
        notes.append("peaky")
    return ",".join(notes) or "clean"


def _dedupe_best_variant(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    best_by_lane: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in rows:
        key = (row["channel"], row["vip_tier"], row["lane"])
        current = best_by_lane.get(key)
        if current is None or row["_score"] > current["_score"]:
            best_by_lane[key] = row
    return sorted(best_by_lane.values(), key=lambda r: r["_score"], reverse=True)


def _candidate_json(rows: list[dict[str, Any]], args: argparse.Namespace) -> str:
    payload = []
    for row in rows[: args.top]:
        if args.exclude_existing and (
            row["channel"],
            row["vip_tier"],
            row["lane"],
        ) in {
            ("solwhaletrending", "none", "none"),
            ("solhousesignal", "none", "low_score"),
            ("solwhaletrending", "none", "low_score"),
        }:
            continue
        payload.append(
            {
                "channel": row["channel"],
                "vip_tier": row["vip_tier"],
                "lane": row["lane"],
                "variant": args.qsim_variant,
                "size": args.size,
                "days": ALL_DAYS,
            }
        )
    return json.dumps(payload, separators=(",", ":"))


def _print(rows: list[dict[str, Any]], args: argparse.Namespace) -> None:
    print(
        f"\nQSIM CANDIDATE LANES — days={args.days} since={args.since} channel={args.channel} "
        f"lane={args.lane} variant={args.variant} min_closed={args.min_closed} near_sol={args.near_sol}"
    )
    print("Shadow is only the screener. Promote candidates to qsim, then trust quote-backed qsim.")
    if not rows:
        print("\nNo candidate lanes matched. Lower --min-closed or --near-sol.")
        return

    hdr = (
        f"{'channel':<16} {'tier':<7} {'lane':<14} {'shadow_var':<10} "
        f"{'n':>5} {'win%':>6} {'avg':>8} {'total':>9} {'avg_pk':>7} "
        f"{'2x':>5} {'green':>7} {'best':>8} {'share':>7} {'risk':<14}"
    )
    print("\nCandidates")
    print(hdr)
    print("-" * len(hdr))
    for row in rows:
        closed = int(row.get("closed") or 0)
        wins = int(row.get("wins") or 0)
        total = _f(row.get("total_per_sol"))
        best = _f(row.get("best_trade"))
        share = best / total if total > 0 else 0.0
        print(
            f"{row['channel'][:16]:<16} {row['vip_tier'][:7]:<7} {row['lane'][:14]:<14} "
            f"{row['shadow_variant'][:10]:<10} {closed:>5} "
            f"{(wins / closed * 100 if closed else 0):>5.1f}% "
            f"{_f(row.get('avg_per_sol')):>+8.2f} {total:>+9.2f} "
            f"{_f(row.get('avg_peak')):>7.2f} {int(row.get('hit_2x') or 0):>5} "
            f"{int(row.get('green_days') or 0):>2}/{int(row.get('days_seen') or 0):<2} "
            f"{best:>+8.2f} {share:>6.0%} {_risk_note(row):<14}"
        )

    qsim_json = _candidate_json(rows, args)
    print("\nQSIM_EXTRA_LANES_JSON")
    print(qsim_json if qsim_json != "[]" else "[]  # no non-existing lanes selected")
    print("\nNext: add at most 2-3 candidates, restart sol-listener + sol-qsim, then run qsim_lane_scan.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Find shadow-positive lanes worth adding to qsim.")
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--since", default=None, help="only include shadow entries at/after this timestamp")
    parser.add_argument("--channel", default="any")
    parser.add_argument("--lane", default="any")
    parser.add_argument("--variant", default="any", help="shadow exit variant filter")
    parser.add_argument("--min-closed", type=int, default=50)
    parser.add_argument("--near-sol", type=float, default=-5.0, help="include lanes at/above this per-1-SOL total")
    parser.add_argument("--top", type=int, default=5)
    parser.add_argument("--size", type=float, default=0.05)
    parser.add_argument("--qsim-variant", default="early", choices=("early", "ride", "ride_vol"))
    parser.add_argument("--all-variants", action="store_true", help="show every shadow variant instead of best per lane")
    parser.add_argument("--exclude-existing", action="store_true", help="omit hardcoded qsim lanes from JSON output")
    args = parser.parse_args()

    params = {
        "days": args.days,
        "since": args.since,
        "channel": args.channel,
        "lane": args.lane,
        "variant": args.variant,
        "min_closed": args.min_closed,
        "near_sol": args.near_sol,
        "max_peak": MAX_SANE_PEAK,
        "max_pnl": MAX_SANE_PNL_PCT,
        "min_pnl": MIN_SANE_PNL_PCT,
    }
    rows = _rows(params)
    for row in rows:
        row["_score"] = _score(row)
    rows = sorted(rows, key=lambda r: r["_score"], reverse=True)
    if not args.all_variants:
        rows = _dedupe_best_variant(rows)
    _print(rows[: args.top], args)


if __name__ == "__main__":
    main()
