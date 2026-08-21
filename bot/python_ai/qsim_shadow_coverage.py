"""
qsim_shadow_coverage.py — explain qsim vs shadow sample mismatch.

Shadow opens/feed-monitors many more rows than qsim. Qsim is intentionally scoped,
quote-budgeted, and first-open-per-mint deduped, so a positive shadow_report lane
can disagree wildly with qsim_lane_scan simply because qsim only sampled a subset.

This read-only report starts from shadow_positions and buckets each shadow trade:

    has_qsim_quotes     same call+variant has qsim position and quote observations
    qsim_no_quotes      same call+variant has qsim position but no quote observations
    no_qsim_position    shadow row has no corresponding qsim row

Use it before concluding qsim is “wrong” or shadow is “fake.”

Example:
    python3 qsim_shadow_coverage.py --days 7 --channel solwhaletrending --lane low_score --variant early
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))


MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5


SQL = """
WITH shadow_base AS (
    SELECT
        sp.call_id,
        sp.exit_variant AS variant,
        sp.entry_time,
        sp.sol_in AS shadow_sol_in,
        sp.pnl_sol,
        sp.pnl_pct,
        sp.peak_multiplier,
        COALESCE(ch.handle, '?') AS channel,
        COALESCE(c.skip_reason, 'none') AS lane,
        t.mint_address,
        t.symbol,
        c.call_type,
        c.message_type
    FROM shadow_positions sp
    JOIN calls c ON c.id = sp.call_id
    JOIN tokens t ON t.id = c.token_id
    LEFT JOIN channels ch ON ch.id = c.channel_id
    WHERE sp.status = 'closed'
      AND sp.entry_time >= now() - (%(days)s || ' days')::interval
      AND (%(channel)s = 'any' OR COALESCE(ch.handle, '?') = %(channel)s)
      AND (%(lane)s = 'any' OR COALESCE(c.skip_reason, 'none') = %(lane)s)
      AND (%(variant)s = 'any' OR sp.exit_variant = %(variant)s)
      AND (%(raw)s OR NOT (
          COALESCE(sp.peak_multiplier, 0) > %(max_peak)s
       OR COALESCE(sp.pnl_pct, 0) > %(max_pnl)s
       OR COALESCE(sp.pnl_pct, 0) < %(min_pnl)s
      ))
),
annotated AS (
    SELECT
        sb.*,
        q.id AS qsim_id,
        q.status AS qsim_status,
        q.pnl_sol AS qsim_pnl,
        q.sol_in AS qsim_sol_in,
        q.pnl_pct AS qsim_pnl_pct,
        q.peak_multiplier AS qsim_peak,
        q.exit_reason AS qsim_reason,
        q.entry_time AS qsim_entry_time,
        q.exit_time AS qsim_exit_time,
        COALESCE(qobs.qobs_count, 0) AS qobs_count,
        qobs.max_qobs_mult,
        CASE
            WHEN q.id IS NULL THEN 'no_qsim_position'
            WHEN COALESCE(qobs.qobs_count, 0) = 0 THEN 'qsim_no_quotes'
            ELSE 'has_qsim_quotes'
        END AS coverage
    FROM shadow_base sb
    LEFT JOIN qsim_positions q
      ON q.call_id = sb.call_id
     AND q.variant = sb.variant
    LEFT JOIN LATERAL (
        SELECT COUNT(*) AS qobs_count, MAX(qo.real_mult) AS max_qobs_mult
        FROM qsim_quote_observations qo
        WHERE qo.call_id = sb.call_id
          AND q.id IS NOT NULL
          AND qo.observed_at BETWEEN q.entry_time AND COALESCE(q.exit_time, now())
    ) qobs ON TRUE
)
SELECT * FROM annotated
ORDER BY entry_time DESC
"""


def _rows(params: dict[str, Any]) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor
    import db

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


def _ret(pnl: Any, sol_in: Any) -> float:
    sol = _f(sol_in)
    if sol <= 0:
        return 0.0
    return _f(pnl) / sol


def _date(value: Any) -> str:
    if value is None:
        return "n/a"
    if hasattr(value, "strftime"):
        return value.strftime("%m/%d")
    return str(value)[:10]


def _print_bucket_summary(rows: list[dict[str, Any]]) -> None:
    print("\nCoverage Buckets")
    print("PnL columns are normalized per 1 SOL deployed.")
    print(f"{'coverage':<18} {'n':>6} {'shadow':>10} {'qsim':>10} {'avg_spk':>8} {'avg_qpk':>8} {'avg_qmax':>8}")
    print("-" * 78)
    for coverage in ("has_qsim_quotes", "qsim_no_quotes", "no_qsim_position"):
        bucket = [row for row in rows if row["coverage"] == coverage]
        if not bucket:
            continue
        shadow = sum(_ret(row.get("pnl_sol"), row.get("shadow_sol_in")) for row in bucket)
        qsim = sum(
            _ret(row.get("qsim_pnl"), row.get("qsim_sol_in"))
            for row in bucket
            if row.get("qsim_pnl") is not None
        )
        peaks = [_f(row.get("peak_multiplier")) for row in bucket if row.get("peak_multiplier") is not None]
        qpeaks = [_f(row.get("qsim_peak")) for row in bucket if row.get("qsim_peak") is not None]
        qmaxes = [_f(row.get("max_qobs_mult")) for row in bucket if row.get("max_qobs_mult") is not None]
        avg_peak = sum(peaks) / len(peaks) if peaks else 0.0
        avg_qpeak = sum(qpeaks) / len(qpeaks) if qpeaks else 0.0
        avg_qmax = sum(qmaxes) / len(qmaxes) if qmaxes else 0.0
        print(
            f"{coverage:<18} {len(bucket):>6} {shadow:>+10.2f} {qsim:>+10.2f} "
            f"{avg_peak:>8.2f} {avg_qpeak:>8.2f} {avg_qmax:>8.2f}"
        )


def _print_day_summary(rows: list[dict[str, Any]]) -> None:
    days = sorted({_date(row["entry_time"]) for row in rows})
    print("\nBy Day")
    print(f"{'day':<8} {'coverage':<18} {'n':>6} {'shadow':>10} {'qsim':>10}")
    print("-" * 58)
    for day in days:
        day_rows = [row for row in rows if _date(row["entry_time"]) == day]
        for coverage in ("has_qsim_quotes", "qsim_no_quotes", "no_qsim_position"):
            bucket = [row for row in day_rows if row["coverage"] == coverage]
            if not bucket:
                continue
            shadow = sum(_ret(row.get("pnl_sol"), row.get("shadow_sol_in")) for row in bucket)
            qsim = sum(
                _ret(row.get("qsim_pnl"), row.get("qsim_sol_in"))
                for row in bucket
                if row.get("qsim_pnl") is not None
            )
            print(f"{day:<8} {coverage:<18} {len(bucket):>6} {shadow:>+10.2f} {qsim:>+10.2f}")


def _print_missing_examples(rows: list[dict[str, Any]], limit: int) -> None:
    missing = [row for row in rows if row["coverage"] == "no_qsim_position"]
    missing.sort(key=lambda row: abs(_f(row.get("pnl_sol"))), reverse=True)
    if not missing:
        return
    print("\nLargest shadow rows with no qsim position")
    hdr = f"{'call':>7} {'day':<8} {'symbol':<12} {'shadow':>9} {'spk':>6} {'call_type':<12} {'msg_type':<14}"
    print(hdr)
    print("-" * len(hdr))
    for row in missing[:limit]:
        print(
            f"{int(row['call_id']):>7} {_date(row['entry_time']):<8} "
            f"{(row.get('symbol') or '?')[:12]:<12} {_f(row.get('pnl_sol')):>+9.2f} "
            f"{_f(row.get('peak_multiplier')):>6.2f} "
            f"{(row.get('call_type') or '?')[:12]:<12} {(row.get('message_type') or '?')[:14]:<14}"
        )


def _print_no_path_disagreements(rows: list[dict[str, Any]], limit: int) -> None:
    no_path = [row for row in rows if row["coverage"] == "qsim_no_quotes"]
    no_path.sort(
        key=lambda row: abs(
            _ret(row.get("pnl_sol"), row.get("shadow_sol_in"))
            - _ret(row.get("qsim_pnl"), row.get("qsim_sol_in"))
        ),
        reverse=True,
    )
    if not no_path:
        return
    print("\nLargest qsim positions with no quote-path log")
    hdr = (
        f"{'call':>7} {'day':<8} {'symbol':<12} {'s_ret':>9} {'q_ret':>9} "
        f"{'edge':>9} {'spk':>6} {'qpk':>6} {'q_reason':<14}"
    )
    print(hdr)
    print("-" * len(hdr))
    for row in no_path[:limit]:
        shadow = _ret(row.get("pnl_sol"), row.get("shadow_sol_in"))
        qsim = _ret(row.get("qsim_pnl"), row.get("qsim_sol_in"))
        print(
            f"{int(row['call_id']):>7} {_date(row['entry_time']):<8} "
            f"{(row.get('symbol') or '?')[:12]:<12} {shadow:>+9.2f} {qsim:>+9.2f} "
            f"{shadow - qsim:>+9.2f} {_f(row.get('peak_multiplier')):>6.2f} "
            f"{_f(row.get('qsim_peak')):>6.2f} {(row.get('qsim_reason') or '?')[:14]:<14}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit shadow rows missing qsim coverage.")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--channel", default="any")
    parser.add_argument("--lane", default="any")
    parser.add_argument("--variant", default="early")
    parser.add_argument("--limit", type=int, default=30)
    parser.add_argument("--raw", action="store_true")
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
    }
    rows = _rows(params)
    print(
        f"\nQSIM/SHADOW COVERAGE — days={args.days} channel={args.channel} "
        f"lane={args.lane} variant={args.variant}"
    )
    print(f"shadow rows: {len(rows)}")
    if not rows:
        return
    _print_bucket_summary(rows)
    _print_day_summary(rows)
    _print_no_path_disagreements(rows, args.limit)
    _print_missing_examples(rows, args.limit)


if __name__ == "__main__":
    main()
