"""
qsim_recovery_classifier.py — compare recoverable hard stops vs true dead rugs.

This is a diagnostic, not a trading strategy. It answers:

    "When qsim hard-stops, which losers later recover, and what did their quote
     path look like before exit?"

Use it to find deterministic features for a future soft-stop rule. Recovery is
based only on qsim_quote_observations / Jupiter quote multiples, never shadow.

Example:
    python3 qsim_recovery_classifier.py --since '2026-08-28 00:00 UTC' --channel solwhaletrending --lane low_score --variant early
    python3 qsim_recovery_classifier.py --since '2026-09-01 00:00 UTC' --recovery-mult 1.2 --detail
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


RECOVERY_LEVELS = (0.90, 1.0, 1.20, 1.30, 1.40, 2.0)

SQL = """
WITH base AS (
    SELECT
        q.call_id,
        t.symbol,
        t.mint_address,
        COALESCE(ch.handle, q.channel_handle, '?') AS channel,
        COALESCE(q.vip_tier, 'none') AS tier,
        COALESCE(c.skip_reason, q.lane, 'none') AS lane,
        q.variant,
        q.entry_time,
        q.exit_time,
        q.exit_reason,
        q.entry_price,
        q.exit_price,
        q.sol_in,
        q.sol_out,
        q.pnl_sol,
        q.pnl_sol / NULLIF(q.sol_in, 0) AS qsim_ret,
        q.sol_out / NULLIF(q.sol_in, 0) AS exit_mult,
        q.entry_price / NULLIF(c.mcap_at_call, 0) AS entry_ref_ratio,
        c.mcap_at_call AS ref_mcap,
        c.conviction_score AS score,
        t.liq_at_detection AS liquidity,
        t.vol_1h_at_detection AS vol_1h,
        t.vol_1h_at_detection / NULLIF(q.entry_price, 0) AS vol_mcap,
        t.token_age_minutes AS age_min,
        t.holder_count,
        t.hodl_count,
        t.top_10_holder_pct AS top10_pct,
        t.first_20_pct AS first20_pct,
        t.detecting_wallet_sol AS detector_sol,
        t.detecting_sol_spent,
        t.bundle_count,
        t.bundle_pct_remaining AS bundle_pct,
        t.sniper_count,
        t.sniper_pct_remaining AS sniper_pct,
        t.fake_vol_pct
    FROM qsim_positions q
    JOIN calls c ON c.id = q.call_id
    JOIN tokens t ON t.id = q.token_id
    LEFT JOIN channels ch ON ch.id = c.channel_id
    WHERE q.status = 'closed'
      AND q.exit_time IS NOT NULL
      AND q.entry_time >= now() - (%(days)s || ' days')::interval
      AND (%(since)s IS NULL OR q.entry_time >= %(since)s::timestamptz)
      AND (%(channel)s = 'any' OR COALESCE(ch.handle, q.channel_handle, '?') = %(channel)s)
      AND (%(lane)s = 'any' OR COALESCE(c.skip_reason, q.lane, 'none') = %(lane)s)
      AND (%(variant)s = 'any' OR q.variant = %(variant)s)
      AND (%(exit_reason)s = 'any' OR q.exit_reason = %(exit_reason)s)
      AND q.pnl_sol <= 0
      AND (%(min_entry_ratio)s IS NULL OR q.entry_price / NULLIF(c.mcap_at_call, 0) >= %(min_entry_ratio)s)
      AND (%(max_entry_ratio)s IS NULL OR q.entry_price / NULLIF(c.mcap_at_call, 0) <= %(max_entry_ratio)s)
)
SELECT
    b.*,
    EXTRACT(EPOCH FROM (b.exit_time - b.entry_time)) / 60.0 AS mins_to_exit,
    pre.pre_ticks,
    pre.pre_no_route_count,
    pre.pre_min_mult,
    pre.pre_max_mult,
    pre.pre_last_mult,
    pre.pre_observations,
    post.post_ticks,
    post.post_max_mult,
    post.first_post_0p9,
    post.first_post_1x,
    post.first_post_1p2,
    post.first_post_1p3,
    post.first_post_1p4,
    post.first_post_2x
FROM base b
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE qo.sol_out IS NOT NULL AND qo.real_mult > 0 AND qo.real_mult <= %(max_qmax)s) AS pre_ticks,
        COUNT(*) FILTER (WHERE qo.no_route) AS pre_no_route_count,
        MIN(qo.real_mult) FILTER (WHERE qo.sol_out IS NOT NULL AND qo.real_mult > 0 AND qo.real_mult <= %(max_qmax)s) AS pre_min_mult,
        MAX(qo.real_mult) FILTER (WHERE qo.sol_out IS NOT NULL AND qo.real_mult > 0 AND qo.real_mult <= %(max_qmax)s) AS pre_max_mult,
        (ARRAY_AGG(qo.real_mult ORDER BY qo.observed_at DESC)
            FILTER (WHERE qo.sol_out IS NOT NULL AND qo.real_mult > 0 AND qo.real_mult <= %(max_qmax)s))[1] AS pre_last_mult,
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'observed_at', qo.observed_at,
                    'real_mult', qo.real_mult,
                    'no_route', qo.no_route
                )
                ORDER BY qo.observed_at
            ) FILTER (WHERE qo.sol_out IS NOT NULL AND qo.real_mult > 0 AND qo.real_mult <= %(max_qmax)s),
            '[]'::jsonb
        ) AS pre_observations
    FROM qsim_quote_observations qo
    WHERE qo.call_id = b.call_id
      AND qo.observed_at BETWEEN b.entry_time AND b.exit_time
) pre ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) FILTER (WHERE qo.sol_out IS NOT NULL AND qo.real_mult > 0 AND qo.real_mult <= %(max_qmax)s) AS post_ticks,
        MAX(qo.real_mult) FILTER (WHERE qo.sol_out IS NOT NULL AND qo.real_mult > 0 AND qo.real_mult <= %(max_qmax)s) AS post_max_mult,
        MIN(qo.observed_at) FILTER (WHERE qo.sol_out IS NOT NULL AND qo.real_mult >= 0.90 AND qo.real_mult <= %(max_qmax)s) AS first_post_0p9,
        MIN(qo.observed_at) FILTER (WHERE qo.sol_out IS NOT NULL AND qo.real_mult >= 1.0 AND qo.real_mult <= %(max_qmax)s) AS first_post_1x,
        MIN(qo.observed_at) FILTER (WHERE qo.sol_out IS NOT NULL AND qo.real_mult >= 1.20 AND qo.real_mult <= %(max_qmax)s) AS first_post_1p2,
        MIN(qo.observed_at) FILTER (WHERE qo.sol_out IS NOT NULL AND qo.real_mult >= 1.30 AND qo.real_mult <= %(max_qmax)s) AS first_post_1p3,
        MIN(qo.observed_at) FILTER (WHERE qo.sol_out IS NOT NULL AND qo.real_mult >= 1.40 AND qo.real_mult <= %(max_qmax)s) AS first_post_1p4,
        MIN(qo.observed_at) FILTER (WHERE qo.sol_out IS NOT NULL AND qo.real_mult >= 2.0 AND qo.real_mult <= %(max_qmax)s) AS first_post_2x
    FROM qsim_quote_observations qo
    WHERE qo.call_id = b.call_id
      AND qo.observed_at > b.exit_time
      AND qo.observed_at <= b.exit_time + (%(post_mins)s || ' minutes')::interval
) post ON TRUE
ORDER BY b.exit_time DESC
"""


FEATURES = (
    ("exit_mult", "exit"),
    ("pre_max_mult", "pre_qmax"),
    ("pre_min_mult", "pre_qmin"),
    ("pre_last_mult", "pre_last"),
    ("pre_drawdown", "pre_dd"),
    ("last3_slope", "last3"),
    ("mins_to_exit", "min_exit"),
    ("pre_ticks", "ticks"),
    ("pre_no_route_count", "no_route"),
    ("entry_ref_ratio", "entry/ref"),
    ("score", "score"),
    ("liquidity", "liq"),
    ("vol_1h", "vol1h"),
    ("vol_mcap", "vol/mcap"),
    ("age_min", "age"),
    ("holder_count", "holders"),
    ("hodl_count", "hodlers"),
    ("first20_pct", "first20"),
    ("bundle_count", "bundles"),
    ("bundle_pct", "bundle%"),
    ("sniper_count", "snipers"),
    ("sniper_pct", "sniper%"),
    ("fake_vol_pct", "fakevol%"),
)


def _rows(params: dict[str, Any]) -> list[dict[str, Any]]:
    import db
    from psycopg2.extras import RealDictCursor

    db.ensure_qsim_positions_table()
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


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _observations(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return loaded if isinstance(loaded, list) else []
    if isinstance(raw, list):
        return raw
    try:
        loaded = json.loads(json.dumps(raw, default=_json_default))
    except (TypeError, ValueError):
        return []
    return loaded if isinstance(loaded, list) else []


def _prepare(row: dict[str, Any]) -> None:
    pre_mults = [
        _f(obs.get("real_mult"))
        for obs in _observations(row.get("pre_observations"))
        if 0 < _f(obs.get("real_mult"))
    ]
    row["pre_drawdown"] = (
        _f(row.get("pre_last_mult")) / _f(row.get("pre_max_mult")) if _f(row.get("pre_max_mult")) > 0 else None
    )
    row["last3_slope"] = pre_mults[-1] - pre_mults[-3] if len(pre_mults) >= 3 else None


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    values = sorted(values)
    mid = len(values) // 2
    if len(values) % 2:
        return values[mid]
    return (values[mid - 1] + values[mid]) / 2.0


def _fmt(value: Any, width: int = 9, decimals: int = 3) -> str:
    if value is None:
        return f"{'n/a':>{width}}"
    return f"{_f(value):>{width}.{decimals}f}"


def _mins_after(row: dict[str, Any], key: str) -> float | None:
    value = row.get(key)
    exit_time = row.get("exit_time")
    if not value or not exit_time:
        return None
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if isinstance(exit_time, str):
        exit_time = datetime.fromisoformat(exit_time.replace("Z", "+00:00"))
    return (value - exit_time).total_seconds() / 60.0


def _print_threshold_summary(rows: list[dict[str, Any]]) -> None:
    losers = len(rows)
    print("\nPost-Exit Recovery Thresholds")
    print(f"{'level':>8} {'count':>7} {'loser%':>8} {'med_min':>9}")
    print("-" * 36)
    key_by_level = {
        0.90: "first_post_0p9",
        1.0: "first_post_1x",
        1.20: "first_post_1p2",
        1.30: "first_post_1p3",
        1.40: "first_post_1p4",
        2.0: "first_post_2x",
    }
    for level in RECOVERY_LEVELS:
        recovered = [row for row in rows if _f(row.get("post_max_mult")) >= level]
        mins = [
            value
            for row in recovered
            if (value := _mins_after(row, key_by_level[level])) is not None
        ]
        pct = len(recovered) / losers if losers else 0.0
        print(f"{level:>8.2f} {len(recovered):>7} {pct:>7.1%} {_fmt(_median(mins), width=9, decimals=1)}")


def _print_feature_split(rows: list[dict[str, Any]], recovery_mult: float) -> None:
    recovered = [row for row in rows if _f(row.get("post_max_mult")) >= recovery_mult]
    dead = [row for row in rows if _f(row.get("post_max_mult")) < recovery_mult]

    print(f"\nRecovery Classifier — recovered >= {recovery_mult:g}x")
    print(f"losers={len(rows)} recovered={len(recovered)} dead={len(dead)}")
    print(f"{'feature':<12} {'rec_med':>10} {'dead_med':>10} {'gap':>10} {'rec_p75':>10} {'dead_p75':>10}")
    print("-" * 66)
    for feature, label in FEATURES:
        rec_values = [_f(row.get(feature)) for row in recovered if row.get(feature) is not None]
        dead_values = [_f(row.get(feature)) for row in dead if row.get(feature) is not None]
        rec_med = _median(rec_values)
        dead_med = _median(dead_values)
        rec_p75 = sorted(rec_values)[int(len(rec_values) * 0.75)] if rec_values else None
        dead_p75 = sorted(dead_values)[int(len(dead_values) * 0.75)] if dead_values else None
        gap = rec_med - dead_med if rec_med is not None and dead_med is not None else None
        print(
            f"{label:<12} {_fmt(rec_med, width=10)} {_fmt(dead_med, width=10)} "
            f"{_fmt(gap, width=10)} {_fmt(rec_p75, width=10)} {_fmt(dead_p75, width=10)}"
        )


def _print_day_summary(rows: list[dict[str, Any]], recovery_mult: float) -> None:
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        day = str(row.get("entry_time"))[:10]
        by_day[day].append(row)

    print("\nBy Day")
    print(f"{'day':<10} {'losers':>7} {'rec':>6} {'rec%':>7} {'med_exit':>9} {'med_post':>9}")
    print("-" * 55)
    for day in sorted(by_day):
        day_rows = by_day[day]
        recovered = [row for row in day_rows if _f(row.get("post_max_mult")) >= recovery_mult]
        print(
            f"{day:<10} {len(day_rows):>7} {len(recovered):>6} "
            f"{(len(recovered) / len(day_rows) if day_rows else 0):>6.1%} "
            f"{_fmt(_median([_f(row.get('exit_mult')) for row in day_rows]), width=9)} "
            f"{_fmt(_median([_f(row.get('post_max_mult')) for row in day_rows]), width=9)}"
        )


def _print_detail(rows: list[dict[str, Any]], recovery_mult: float, limit: int) -> None:
    rows = sorted(
        rows,
        key=lambda row: (
            _f(row.get("post_max_mult")) - _f(row.get("exit_mult")),
            _f(row.get("post_max_mult")),
        ),
        reverse=True,
    )[:limit]
    print("\nDetail — biggest post-exit recovery first")
    hdr = (
        f"{'call':>7} {'symbol':<12} {'exit':>6} {'ret':>7} {'preMax':>7} "
        f"{'preLast':>7} {'dd':>6} {'slope':>7} {'mins':>6} {'post':>7} "
        f"{'rec':>4} {'m1x':>6} {'m13':>6} {'ticks':>5}"
    )
    print(hdr)
    print("-" * len(hdr))
    for row in rows:
        recovered = "yes" if _f(row.get("post_max_mult")) >= recovery_mult else "no"
        print(
            f"{int(row['call_id']):>7} "
            f"{(row.get('symbol') or '?')[:12]:<12} "
            f"{_fmt(row.get('exit_mult'), width=6)} "
            f"{_fmt(row.get('qsim_ret'), width=7)} "
            f"{_fmt(row.get('pre_max_mult'), width=7)} "
            f"{_fmt(row.get('pre_last_mult'), width=7)} "
            f"{_fmt(row.get('pre_drawdown'), width=6)} "
            f"{_fmt(row.get('last3_slope'), width=7)} "
            f"{_fmt(row.get('mins_to_exit'), width=6, decimals=1)} "
            f"{_fmt(row.get('post_max_mult'), width=7)} "
            f"{recovered:>4} "
            f"{_fmt(_mins_after(row, 'first_post_1x'), width=6, decimals=1)} "
            f"{_fmt(_mins_after(row, 'first_post_1p3'), width=6, decimals=1)} "
            f"{int(row.get('pre_ticks') or 0):>5}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Classify qsim hard-stop losers that later recover."
    )
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--since", default=None)
    parser.add_argument("--channel", default="solwhaletrending")
    parser.add_argument("--lane", default="low_score")
    parser.add_argument("--variant", default="early")
    parser.add_argument("--exit-reason", default="hard_stop")
    parser.add_argument("--post-mins", type=float, default=90.0)
    parser.add_argument("--max-qmax", type=float, default=50.0)
    parser.add_argument("--recovery-mult", type=float, default=1.0)
    parser.add_argument("--min-entry-ratio", type=float, default=None)
    parser.add_argument("--max-entry-ratio", type=float, default=None)
    parser.add_argument("--limit", type=int, default=60)
    parser.add_argument("--detail", action="store_true")
    args = parser.parse_args()

    params = {
        "days": args.days,
        "since": args.since,
        "channel": args.channel,
        "lane": args.lane,
        "variant": args.variant,
        "exit_reason": args.exit_reason,
        "post_mins": args.post_mins,
        "max_qmax": args.max_qmax,
        "min_entry_ratio": args.min_entry_ratio,
        "max_entry_ratio": args.max_entry_ratio,
    }
    rows = _rows(params)
    for row in rows:
        _prepare(row)

    print(
        "QSIM RECOVERY CLASSIFIER — "
        f"days={args.days} since={args.since} channel={args.channel} "
        f"lane={args.lane} variant={args.variant} exit_reason={args.exit_reason} "
        f"post_mins={args.post_mins:g} recovery_mult={args.recovery_mult:g}"
    )
    print("Rows are closed losing qsim positions; recovery uses post-exit Jupiter quote observations only.")
    print(f"rows={len(rows)}")

    if not rows:
        return

    _print_threshold_summary(rows)
    _print_feature_split(rows, args.recovery_mult)
    _print_day_summary(rows, args.recovery_mult)
    if args.detail:
        _print_detail(rows, args.recovery_mult, args.limit)
    else:
        print("\nRun with --detail for per-trade rows.\n")


if __name__ == "__main__":
    main()
