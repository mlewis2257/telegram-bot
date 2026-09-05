"""
qsim_lane_scan.py — scan all qsim lanes with real quote-path outcomes.

This is the broad version of qsim_quote_capture_replay.py. It groups closed qsim
positions by channel × skip_reason(lane) × variant, restricted to rows with
qsim_quote_observations, then compares:

    current qsim PnL
    best raw observed quote upper bound
    best simple exit-policy replay from qsim_quote_capture_replay
    shadow comparison, when matched

Use it to decide which lanes deserve deeper entry-filter work.

Example:
    python3 qsim_lane_scan.py --days 7 --min-n 20 --min-entry-ratio 0.5 --max-entry-ratio 2
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))

import qsim_quote_capture_replay as replay


MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5


SQL = """
WITH base AS (
    SELECT
        q.call_id,
        tok.symbol,
        COALESCE(ch.handle, '?') AS channel,
        COALESCE(c.skip_reason, 'none') AS lane,
        q.variant,
        q.vip_tier,
        q.entry_time,
        q.exit_time,
        q.entry_price AS qsim_entry,
        q.exit_price AS qsim_exit,
        q.peak_multiplier AS qsim_peak,
        q.sol_in AS qsim_sol_in,
        q.pnl_sol AS qsim_pnl,
        q.pnl_pct AS qsim_pnl_pct,
        q.exit_reason AS qsim_reason,
        sp.entry_price AS shadow_entry,
        sp.exit_price AS shadow_exit,
        sp.peak_multiplier AS shadow_peak,
        sp.sol_in AS shadow_sol_in,
        sp.pnl_sol AS shadow_pnl,
        sp.pnl_pct AS shadow_pnl_pct,
        sp.exit_reason AS shadow_reason
    FROM qsim_positions q
    JOIN calls c ON c.id = q.call_id
    JOIN tokens tok ON tok.id = q.token_id
    LEFT JOIN channels ch ON ch.id = c.channel_id
    LEFT JOIN shadow_positions sp
      ON sp.call_id = q.call_id
     AND sp.exit_variant = q.variant
     AND sp.status = 'closed'
    WHERE q.status = 'closed'
      AND q.entry_time >= now() - (%(days)s || ' days')::interval
      AND (%(since)s IS NULL OR q.entry_time >= %(since)s::timestamptz)
      AND (%(channel)s = 'any' OR COALESCE(ch.handle, '?') = %(channel)s)
      AND (%(lane)s = 'any' OR COALESCE(c.skip_reason, 'none') = %(lane)s)
      AND (%(variant)s = 'any' OR q.variant = %(variant)s)
      -- DATA QUALITY — see qsim_quote_capture_replay.py. Starved rows measure the quote
      -- outage, not the strategy; excluded by default.
      AND (%(include_stale)s OR left(COALESCE(q.exit_reason, ''), 6) <> 'stale_')
      AND (%(min_obs)s <= 0 OR COALESCE(q.obs_count, %(min_obs)s) >= %(min_obs)s)
      AND (%(max_gap_secs)s <= 0 OR COALESCE(q.max_gap_secs, 0) <= %(max_gap_secs)s)
      AND (%(min_entry_ratio)s IS NULL OR q.entry_price / NULLIF(sp.entry_price, 0) >= %(min_entry_ratio)s)
      AND (%(max_entry_ratio)s IS NULL OR q.entry_price / NULLIF(sp.entry_price, 0) <= %(max_entry_ratio)s)
      AND (%(raw)s OR NOT (
          COALESCE(sp.peak_multiplier, 0) > %(max_peak)s
       OR COALESCE(sp.pnl_pct, 0) > %(max_pnl)s
       OR COALESCE(sp.pnl_pct, 0) < %(min_pnl)s
       OR COALESCE(q.peak_multiplier, 0) > %(max_peak)s
       OR COALESCE(q.pnl_pct, 0) > %(max_pnl)s
       OR COALESCE(q.pnl_pct, 0) < %(min_pnl)s
      ))
)
SELECT
    b.*,
    qobs.qobs_count,
    qobs.max_qobs_mult,
    qobs.observations
FROM base b
JOIN LATERAL (
    SELECT
        -- Only PRICED observations count as coverage. qobs_count is used solely as a
        -- `> 0` gate (replay --require-qobs, lane_scan join, forward_referee's
        -- referee-grade test), and counting 429/no-route rows made a row with zero
        -- usable quotes read as covered — which is how the 2026-09-05 starved window
        -- passed the referee. (qsim_positions.obs_count is a different question —
        -- 'did we look' — so it does count no-route.)
        COUNT(*) FILTER (WHERE qo.real_mult IS NOT NULL) AS qobs_count,
        MAX(qo.real_mult) AS max_qobs_mult,
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'observed_at', qo.observed_at,
                    'real_mult', qo.real_mult,
                    'exit_reason', qo.exit_reason,
                    'should_exit', qo.should_exit,
                    'no_route', qo.no_route,
                    'rate_limited', qo.rate_limited
                )
                ORDER BY qo.observed_at
            ) FILTER (WHERE qo.id IS NOT NULL),
            '[]'::jsonb
        ) AS observations
    FROM qsim_quote_observations qo
    WHERE qo.call_id = b.call_id
      AND qo.observed_at BETWEEN b.entry_time AND COALESCE(b.exit_time, now())
) qobs ON qobs.qobs_count > 0
ORDER BY b.entry_time DESC
"""


def _rows(params: dict[str, Any]) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor
    import db

    db.ensure_qsim_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SQL, params)
        return [dict(row) for row in cur.fetchall()]


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _normalize_observations(row: dict[str, Any]) -> None:
    raw = row.get("observations")
    if raw is None or isinstance(raw, list):
        return
    if isinstance(raw, str):
        row["observations"] = json.loads(raw)
        return
    row["observations"] = json.loads(json.dumps(raw, default=_json_default))


def _policy_names() -> list[str]:
    names = ["raw_config"]
    for level in replay.BANK_LEVELS:
        suffix = replay._level_suffix(level)
        names.extend([f"bank_{suffix}", f"confirm_bank_{suffix}"])
    for level in (1.30, 1.40, 1.50):
        suffix = replay._level_suffix(level)
        for fraction in replay.BANK_FRACTIONS:
            frac_suffix = replay._fraction_suffix(fraction)
            for stop in replay.BANK_REMAINDER_STOPS:
                stop_suffix = replay._level_suffix(stop)
                names.append(f"{frac_suffix}_bank_{suffix}_stop_{stop_suffix}")
    for bounce in replay.NO_BOUNCE_THRESHOLDS:
        bounce_suffix = replay._level_suffix(bounce)
        for stop in replay.NO_BOUNCE_STOPS:
            stop_suffix = replay._level_suffix(stop)
            names.append(f"no_{bounce_suffix}_stop_{stop_suffix}")
    return names


def _fmt(value: float) -> str:
    return f"{value:+.2f}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan all qsim lanes by quote-path PnL.")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--since", default=None, help="only include qsim entries at/after this timestamp")
    parser.add_argument("--channel", default="any")
    parser.add_argument("--lane", default="any")
    parser.add_argument("--variant", default="any")
    parser.add_argument("--include-stale", action="store_true",
                        help="include rows closed on a stale quote (exit_reason stale_*)")
    parser.add_argument("--min-obs", type=int, default=0,
                        help="minimum priced observations over the position's life (0 = off)")
    parser.add_argument("--max-gap-secs", type=float, default=0.0,
                        help="reject rows with a blind hole longer than this (0 = off)")
    parser.add_argument("--min-n", type=int, default=15)
    parser.add_argument("--min-entry-ratio", type=float, default=None)
    parser.add_argument("--max-entry-ratio", type=float, default=None)
    parser.add_argument("--raw", action="store_true")
    parser.add_argument(
        "--max-qmax",
        type=float,
        default=replay.MAX_QOBS_MULT,
        help="ignore quote observations above this multiple as quote artifacts",
    )
    parser.add_argument("--sort", choices=("current", "best", "improve", "shadow"), default="best")
    args = parser.parse_args()
    replay.MAX_QOBS_MULT = args.max_qmax

    params = {
        "days": args.days,
        "since": args.since,
        "channel": args.channel,
        "lane": args.lane,
        "variant": args.variant,
        "min_entry_ratio": args.min_entry_ratio,
        "max_entry_ratio": args.max_entry_ratio,
        "raw": args.raw,
        "max_peak": MAX_SANE_PEAK,
        "max_pnl": MAX_SANE_PNL_PCT,
        "min_pnl": MIN_SANE_PNL_PCT,
        "include_stale": args.include_stale,
        "min_obs": args.min_obs,
        "max_gap_secs": args.max_gap_secs,
    }

    groups: dict[tuple[str, str, str], list[replay.ReplayRow]] = defaultdict(list)
    for row in _rows(params):
        _normalize_observations(row)
        key = (row.get("channel") or "?", row.get("lane") or "none", row.get("variant") or "?")
        groups[key].append(replay._view(row))

    policy_names = _policy_names()
    summaries = []
    for key, views in groups.items():
        if len(views) < args.min_n:
            continue
        current = sum(view.returns["current"] for view in views)
        shadow_views = [view for view in views if view.shadow_return is not None]
        shadow = sum(view.shadow_return or 0.0 for view in shadow_views)
        best_raw = sum(view.returns["best_raw"] for view in views)
        entry_times = [
            view.row.get("entry_time")
            for view in views
            if view.row.get("entry_time") is not None
        ]
        first_entry = min(entry_times) if entry_times else None
        last_entry = max(entry_times) if entry_times else None
        best_policy = max(
            policy_names,
            key=lambda policy: sum(view.returns[policy] for view in views),
        )
        best_total = sum(view.returns[best_policy] for view in views)
        runner_15 = sum(1 for view in views if (view.max_quote_mult or 0.0) >= 1.5)
        dead_11 = sum(1 for view in views if (view.max_quote_mult or 0.0) < 1.1)
        summaries.append({
            "key": key,
            "n": len(views),
            "current": current,
            "avg": current / len(views),
            "shadow": shadow,
            "shadow_n": len(shadow_views),
            "first_entry": first_entry,
            "last_entry": last_entry,
            "best_raw": best_raw,
            "best_policy": best_policy,
            "best_total": best_total,
            "improve": best_total - current,
            "runner_15": runner_15,
            "dead_11": dead_11,
        })

    sort_key = {
        "current": lambda row: row["current"],
        "best": lambda row: row["best_total"],
        "improve": lambda row: row["improve"],
        "shadow": lambda row: row["shadow"],
    }[args.sort]
    summaries.sort(key=sort_key, reverse=True)

    print(
        f"\nQSIM LANE SCAN — days={args.days} since={args.since} channel={args.channel} lane={args.lane} "
        f"variant={args.variant} min_n={args.min_n} max_qmax={args.max_qmax}"
    )
    print(
        "PnL normalized per 1 SOL deployed; only rows with qsim quote observations.\n"
        "`shadow` is the matched shadow PnL for these same qsim rows, not the full shadow_report lane.\n"
    )
    hdr = (
        f"{'channel':<16} {'lane':<14} {'var':<8} {'first':<10} {'last':<10} {'n':>5} {'cur':>9} {'avg':>7} "
        f"{'shadow':>9} {'best':>9} {'impr':>8} {'r1.5':>5} {'d1.1':>5} best_policy"
    )
    print(hdr)
    print("-" * len(hdr))
    for row in summaries:
        channel, lane, variant = row["key"]
        first = _date(row["first_entry"])
        last = _date(row["last_entry"])
        print(
            f"{channel[:16]:<16} {lane[:14]:<14} {variant[:8]:<8} "
            f"{first:<10} {last:<10} "
            f"{row['n']:>5} {row['current']:>+9.2f} {row['avg']:>+7.2f} "
            f"{row['shadow']:>+9.2f} {row['best_total']:>+9.2f} "
            f"{row['improve']:>+8.2f} {row['runner_15']:>5} {row['dead_11']:>5} "
            f"{row['best_policy']}"
        )

    if not summaries:
        print("No lanes met that min_n/filter yet.")
    else:
        print("\nRead: compare this to shadow_report only when the date/sample sizes line up.")


def _date(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, datetime):
        return value.strftime("%m/%d")
    return str(value)[:10]


if __name__ == "__main__":
    main()
