"""
shadow_peak_timing_report.py — audit when shadow's positive peaks happen.

Shadow can look great for three very different reasons:

    1. tradable move: shadow peak is early and qsim quote max corroborates it
    2. late/feed move: shadow peak happens late or near/after exit
    3. feed-only move: shadow peak is high but qsim never saw a comparable sell quote

This read-only report joins closed shadow rows to matching qsim rows/quote
observations and buckets the peak timing + quote corroboration. Use it before
trusting shadow_report as a live-trading target.

Example:
    python3 shadow_peak_timing_report.py --days 14 --channel solwhaletrending --lane low_score --variant early
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db


MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5
MAX_QOBS_MULT = 50.0


SQL = """
WITH shadow_base AS (
    SELECT
        sp.call_id,
        tok.symbol,
        COALESCE(ch.handle, '?') AS channel,
        COALESCE(c.skip_reason, 'none') AS lane,
        sp.exit_variant AS variant,
        sp.entry_time,
        sp.peak_at,
        sp.exit_time,
        sp.entry_price AS shadow_entry,
        sp.peak_multiplier AS shadow_peak,
        sp.sol_in AS shadow_sol_in,
        sp.pnl_sol AS shadow_pnl,
        sp.pnl_pct AS shadow_pnl_pct,
        sp.exit_reason AS shadow_reason,
        q.id AS qsim_id,
        q.entry_time AS qsim_entry_time,
        q.exit_time AS qsim_exit_time,
        q.peak_multiplier AS qsim_peak,
        q.sol_in AS qsim_sol_in,
        q.pnl_sol AS qsim_pnl,
        q.exit_reason AS qsim_reason
    FROM shadow_positions sp
    JOIN calls c ON c.id = sp.call_id
    JOIN tokens tok ON tok.id = sp.token_id
    LEFT JOIN channels ch ON ch.id = c.channel_id
    LEFT JOIN qsim_positions q
      ON q.call_id = sp.call_id
     AND q.variant = sp.exit_variant
     AND q.status = 'closed'
    WHERE sp.status = 'closed'
      AND (%(days)s = 0 OR sp.entry_time >= now() - (%(days)s || ' days')::interval)
      AND (%(channel)s = 'any' OR COALESCE(ch.handle, '?') = %(channel)s)
      AND (%(lane)s = 'any' OR COALESCE(c.skip_reason, 'none') = %(lane)s)
      AND (%(variant)s = 'any' OR sp.exit_variant = %(variant)s)
      AND (%(raw)s OR NOT (
          COALESCE(sp.peak_multiplier, 0) > %(max_peak)s
       OR COALESCE(sp.pnl_pct, 0) > %(max_pnl)s
       OR COALESCE(sp.pnl_pct, 0) < %(min_pnl)s
      ))
)
SELECT
    sb.*,
    qobs.qobs_count,
    qobs.max_qobs_mult,
    qobs.max_qobs_at,
    EXTRACT(EPOCH FROM (sb.peak_at - sb.entry_time)) / 60.0 AS shadow_peak_min,
    EXTRACT(EPOCH FROM (sb.exit_time - sb.entry_time)) / 60.0 AS shadow_exit_min,
    EXTRACT(EPOCH FROM (qobs.max_qobs_at - sb.entry_time)) / 60.0 AS qmax_min,
    CASE
        WHEN sb.shadow_peak IS NULL THEN 'no_shadow_peak'
        WHEN qobs.max_qobs_mult IS NULL THEN 'no_qsim_quotes'
        WHEN sb.shadow_peak <= 1.2 THEN 'no_big_shadow_move'
        WHEN qobs.max_qobs_mult >= LEAST(sb.shadow_peak * %(qsim_corroboration)s, sb.shadow_peak - %(qsim_gap)s)
            THEN 'quote_corroborated'
        WHEN qobs.max_qobs_mult >= 1.4 THEN 'quote_smaller_but_tradable'
        ELSE 'feed_only_peak'
    END AS corroboration
FROM shadow_base sb
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) AS qobs_count,
        MAX(qo.real_mult) AS max_qobs_mult,
        (ARRAY_AGG(qo.observed_at ORDER BY qo.real_mult DESC, qo.observed_at ASC))[1] AS max_qobs_at
    FROM qsim_quote_observations qo
    WHERE qo.call_id = sb.call_id
      AND sb.qsim_id IS NOT NULL
      AND qo.real_mult > 0
      AND qo.real_mult <= %(max_qmax)s
      AND qo.observed_at BETWEEN sb.qsim_entry_time AND COALESCE(sb.qsim_exit_time, now())
) qobs ON TRUE
ORDER BY sb.entry_time DESC
"""


def _rows(params: dict[str, Any]) -> list[dict[str, Any]]:
    db.ensure_shadow_positions_table()
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


def _ret(row: dict[str, Any], prefix: str) -> float:
    pnl = _f(row.get(f"{prefix}_pnl"))
    sol_in = _f(row.get(f"{prefix}_sol_in"))
    return pnl / sol_in if sol_in > 0 else 0.0


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    values = sorted(values)
    return values[len(values) // 2]


def _fmt(value: float | None, width: int = 8) -> str:
    if value is None:
        return f"{'n/a':>{width}}"
    return f"{value:>{width}.2f}"


def _bucket_peak_min(minutes: float | None) -> str:
    if minutes is None:
        return "unknown"
    if minutes <= 5:
        return "<=5m"
    if minutes <= 15:
        return "5-15m"
    if minutes <= 60:
        return "15-60m"
    if minutes <= 240:
        return "1-4h"
    if minutes <= 720:
        return "4-12h"
    return "12h+"


def _print_summary(rows: list[dict[str, Any]]) -> None:
    print("\nCorroboration Buckets")
    hdr = (
        f"{'bucket':<24} {'n':>6} {'shadow':>10} {'qsim':>10} "
        f"{'avg_spk':>8} {'avg_qmax':>8} {'med_pk_m':>9} {'med_q_m':>8}"
    )
    print(hdr)
    print("-" * len(hdr))
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[row.get("corroboration") or "unknown"].append(row)
    for key, bucket in sorted(buckets.items(), key=lambda item: len(item[1]), reverse=True):
        shadow = sum(_ret(row, "shadow") for row in bucket)
        qsim = sum(_ret(row, "qsim") for row in bucket)
        avg_spk = sum(_f(row.get("shadow_peak")) for row in bucket) / len(bucket)
        qmaxes = [_f(row.get("max_qobs_mult")) for row in bucket if row.get("max_qobs_mult") is not None]
        avg_qmax = sum(qmaxes) / len(qmaxes) if qmaxes else 0.0
        peak_mins = [_f(row.get("shadow_peak_min")) for row in bucket if row.get("shadow_peak_min") is not None]
        qmax_mins = [_f(row.get("qmax_min")) for row in bucket if row.get("qmax_min") is not None]
        print(
            f"{key:<24} {len(bucket):>6} {shadow:>+10.2f} {qsim:>+10.2f} "
            f"{avg_spk:>8.2f} {avg_qmax:>8.2f} {_fmt(_median(peak_mins), 9)} {_fmt(_median(qmax_mins), 8)}"
        )


def _print_timing(rows: list[dict[str, Any]]) -> None:
    print("\nShadow Peak Timing")
    hdr = f"{'peak_time':<10} {'n':>6} {'shadow':>10} {'qsim':>10} {'avg_spk':>8} {'avg_qmax':>8}"
    print(hdr)
    print("-" * len(hdr))
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[_bucket_peak_min(row.get("shadow_peak_min"))].append(row)
    order = ["<=5m", "5-15m", "15-60m", "1-4h", "4-12h", "12h+", "unknown"]
    for key in order:
        bucket = buckets.get(key, [])
        if not bucket:
            continue
        shadow = sum(_ret(row, "shadow") for row in bucket)
        qsim = sum(_ret(row, "qsim") for row in bucket)
        avg_spk = sum(_f(row.get("shadow_peak")) for row in bucket) / len(bucket)
        qmaxes = [_f(row.get("max_qobs_mult")) for row in bucket if row.get("max_qobs_mult") is not None]
        avg_qmax = sum(qmaxes) / len(qmaxes) if qmaxes else 0.0
        print(f"{key:<10} {len(bucket):>6} {shadow:>+10.2f} {qsim:>+10.2f} {avg_spk:>8.2f} {avg_qmax:>8.2f}")


def _date(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, datetime):
        return value.strftime("%m/%d %H:%M")
    return str(value)[:16]


def _print_examples(rows: list[dict[str, Any]], limit: int) -> None:
    print("\nLargest Shadow Winners")
    hdr = (
        f"{'call':>7} {'symbol':<12} {'entry':<11} {'pk_at':<11} "
        f"{'exit_m':>7} {'spk':>6} {'qmax':>6} {'q_m':>7} {'shadow':>8} {'qsim':>8} {'bucket':<24}"
    )
    print(hdr)
    print("-" * len(hdr))
    winners = sorted(rows, key=lambda row: _ret(row, "shadow"), reverse=True)[:limit]
    for row in winners:
        print(
            f"{int(row['call_id']):>7} "
            f"{(row.get('symbol') or '?')[:12]:<12} "
            f"{_date(row.get('entry_time')):<11} "
            f"{_date(row.get('peak_at')):<11} "
            f"{_fmt(row.get('shadow_exit_min'), 7)} "
            f"{_f(row.get('shadow_peak')):>6.2f} "
            f"{_f(row.get('max_qobs_mult')):>6.2f} "
            f"{_fmt(row.get('qmax_min'), 7)} "
            f"{_ret(row, 'shadow'):>+8.2f} "
            f"{_ret(row, 'qsim'):>+8.2f} "
            f"{(row.get('corroboration') or '?')[:24]:<24}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit shadow peak timing vs qsim quotes.")
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--channel", default="any")
    parser.add_argument("--lane", default="any")
    parser.add_argument("--variant", default="early")
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--raw", action="store_true")
    parser.add_argument("--max-qmax", type=float, default=MAX_QOBS_MULT)
    parser.add_argument(
        "--qsim-corroboration",
        type=float,
        default=0.80,
        help="qmax must be at least this fraction of shadow peak to count as corroborated",
    )
    parser.add_argument(
        "--qsim-gap",
        type=float,
        default=0.30,
        help="or qmax may be within this absolute multiple of shadow peak",
    )
    args = parser.parse_args()

    params = {
        "days": args.days,
        "channel": args.channel,
        "lane": args.lane,
        "variant": args.variant,
        "raw": args.raw,
        "max_peak": MAX_SANE_PEAK,
        "max_pnl": MAX_SANE_PNL_PCT,
        "min_pnl": MIN_SANE_PNL_PCT,
        "max_qmax": args.max_qmax,
        "qsim_corroboration": args.qsim_corroboration,
        "qsim_gap": args.qsim_gap,
    }
    rows = _rows(params)
    print(
        f"\nSHADOW PEAK TIMING — days={args.days} channel={args.channel} "
        f"lane={args.lane} variant={args.variant} max_qmax={args.max_qmax}"
    )
    print("PnL is normalized per 1 SOL deployed. qmax is max real Jupiter sell quote.\n")
    print(f"closed shadow rows: {len(rows)}")
    if not rows:
        return
    _print_summary(rows)
    _print_timing(rows)
    _print_examples(rows, args.limit)


if __name__ == "__main__":
    main()
