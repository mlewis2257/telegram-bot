"""
qsim_forward_referee.py — one-table qmax-covered referee for live decisions.

This report exists to stop comparing incompatible slices:

    shadow_report             = all feed-shadow rows
    qsim_lane_scan            = qsim rows with quote observations
    qsim_shadow_coverage      = coverage audit, not a final decision table

The referee starts from shadow rows for a lane/day, joins the same call+variant
qsim row, and splits each day into:

    shadow_n      all shadow rows
    qsim_n        same rows that have a qsim position
    qmax_n        same rows that have qsim quote observations
    coverage      qmax_n / qsim_n

PnL columns are normalized per 1 SOL deployed. A row/day is only "referee-grade"
when qmax coverage is high enough; otherwise it prints WAIT.

Example:
    python3 qsim_forward_referee.py --days 7 --channel solwhaletrending --lane low_score --variant early
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
WITH shadow_base AS (
    SELECT
        sp.call_id,
        sp.exit_variant AS variant,
        sp.entry_time,
        sp.sol_in AS shadow_sol_in,
        sp.pnl_sol AS shadow_pnl,
        sp.pnl_pct AS shadow_pnl_pct,
        sp.peak_multiplier AS shadow_peak,
        sp.exit_reason AS shadow_reason,
        COALESCE(ch.handle, '?') AS channel,
        COALESCE(c.skip_reason, 'none') AS lane,
        tok.symbol
    FROM shadow_positions sp
    JOIN calls c ON c.id = sp.call_id
    JOIN tokens tok ON tok.id = c.token_id
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
)
SELECT
    sb.*,
    q.id AS qsim_id,
    q.status AS qsim_status,
    q.entry_time AS qsim_entry_time,
    q.exit_time AS qsim_exit_time,
    q.entry_price AS qsim_entry,
    q.exit_price AS qsim_exit,
    q.peak_multiplier AS qsim_peak,
    q.sol_in AS qsim_sol_in,
    q.pnl_sol AS qsim_pnl,
    q.pnl_pct AS qsim_pnl_pct,
    q.exit_reason AS qsim_reason,
    q.variant AS qsim_variant,
    q.vip_tier,
    qobs.qobs_count,
    qobs.max_qobs_mult,
    qobs.observations
FROM shadow_base sb
LEFT JOIN qsim_positions q
  ON q.call_id = sb.call_id
 AND q.variant = sb.variant
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) AS qobs_count,
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
    WHERE q.id IS NOT NULL
      AND qo.call_id = sb.call_id
      AND qo.observed_at BETWEEN q.entry_time AND COALESCE(q.exit_time, now())
) qobs ON TRUE
ORDER BY sb.entry_time
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


def _day(value: Any) -> str:
    if value is None:
        return "n/a"
    if hasattr(value, "strftime"):
        return value.strftime("%m/%d")
    return str(value)[:10]


def _as_replay_row(row: dict[str, Any]) -> replay.ReplayRow:
    qrow = {
        "call_id": row["call_id"],
        "symbol": row.get("symbol"),
        "channel": row.get("channel"),
        "lane": row.get("lane"),
        "variant": row.get("variant"),
        "vip_tier": row.get("vip_tier"),
        "entry_time": row.get("qsim_entry_time"),
        "exit_time": row.get("qsim_exit_time"),
        "qsim_entry": row.get("qsim_entry"),
        "qsim_exit": row.get("qsim_exit"),
        "qsim_peak": row.get("qsim_peak"),
        "qsim_sol_in": row.get("qsim_sol_in"),
        "qsim_pnl": row.get("qsim_pnl"),
        "qsim_pnl_pct": row.get("qsim_pnl_pct"),
        "qsim_reason": row.get("qsim_reason"),
        "shadow_entry": None,
        "shadow_exit": None,
        "shadow_peak": row.get("shadow_peak"),
        "shadow_sol_in": row.get("shadow_sol_in"),
        "shadow_pnl": row.get("shadow_pnl"),
        "shadow_pnl_pct": row.get("shadow_pnl_pct"),
        "shadow_reason": row.get("shadow_reason"),
        "qobs_count": row.get("qobs_count") or 0,
        "max_qobs_mult": row.get("max_qobs_mult"),
        "observations": row.get("observations") or [],
    }
    return replay._view(qrow)


def _verdict(qsim_n: int, qmax_n: int, coverage: float, current: float,
             best: float, min_coverage: float, min_qmax_n: int) -> str:
    if qsim_n <= 0:
        return "NO_QSIM"
    if qmax_n < min_qmax_n or coverage < min_coverage:
        return "WAIT"
    if current > 0:
        return "LIVE_CANDIDATE"
    if best >= 0:
        return "EXIT_WORK"
    return "NO_EDGE"


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily qmax-covered referee table.")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--channel", default="solwhaletrending")
    parser.add_argument("--lane", default="low_score")
    parser.add_argument("--variant", default="early")
    parser.add_argument("--min-coverage", type=float, default=0.80)
    parser.add_argument("--min-qmax-n", type=int, default=25)
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
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        _normalize_observations(row)
        by_day[_day(row.get("entry_time"))].append(row)

    print(
        f"\nQSIM FORWARD REFEREE — days={args.days} channel={args.channel} "
        f"lane={args.lane} variant={args.variant}"
    )
    print("PnL is normalized per 1 SOL deployed. Verdict only trusts qmax-covered rows.\n")
    hdr = (
        f"{'day':<8} {'sh_n':>5} {'qs_n':>5} {'qmax_n':>6} {'cov':>5} "
        f"{'shadow':>9} {'qsim':>9} {'qmax_q':>9} {'best_raw':>9} "
        f"{'bank13':>9} {'bank14':>9} {'r1.5':>5} {'d1.1':>5} verdict"
    )
    print(hdr)
    print("-" * len(hdr))

    totals = {
        "shadow_n": 0, "qsim_n": 0, "qmax_n": 0, "shadow": 0.0, "qsim": 0.0,
        "qmax_qsim": 0.0, "best_raw": 0.0, "bank13": 0.0, "bank14": 0.0,
        "runners": 0, "dead": 0,
    }

    for day in sorted(by_day):
        day_rows = by_day[day]
        qsim_rows = [row for row in day_rows if row.get("qsim_id") is not None]
        qmax_rows = [row for row in qsim_rows if int(row.get("qobs_count") or 0) > 0]
        qmax_views = [_as_replay_row(row) for row in qmax_rows]
        shadow = sum(_ret(row.get("shadow_pnl"), row.get("shadow_sol_in")) for row in day_rows)
        qsim = sum(_ret(row.get("qsim_pnl"), row.get("qsim_sol_in")) for row in qsim_rows)
        qmax_qsim = sum(view.returns["current"] for view in qmax_views)
        best_raw = sum(view.returns["best_raw"] for view in qmax_views)
        bank13 = sum(view.returns["bank_1p3x"] for view in qmax_views)
        bank14 = sum(view.returns["bank_1p4x"] for view in qmax_views)
        runners = sum(1 for view in qmax_views if (view.max_quote_mult or 0.0) >= 1.5)
        dead = sum(1 for view in qmax_views if (view.max_quote_mult or 0.0) < 1.1)
        coverage = (len(qmax_rows) / len(qsim_rows)) if qsim_rows else 0.0
        best = max(qmax_qsim, best_raw, bank13, bank14)
        verdict = _verdict(
            len(qsim_rows), len(qmax_rows), coverage, qmax_qsim, best,
            args.min_coverage, args.min_qmax_n,
        )

        print(
            f"{day:<8} {len(day_rows):>5} {len(qsim_rows):>5} {len(qmax_rows):>6} "
            f"{coverage:>4.0%} {shadow:>+9.2f} {qsim:>+9.2f} {qmax_qsim:>+9.2f} "
            f"{best_raw:>+9.2f} {bank13:>+9.2f} {bank14:>+9.2f} "
            f"{runners:>5} {dead:>5} {verdict}"
        )

        totals["shadow_n"] += len(day_rows)
        totals["qsim_n"] += len(qsim_rows)
        totals["qmax_n"] += len(qmax_rows)
        totals["shadow"] += shadow
        totals["qsim"] += qsim
        totals["qmax_qsim"] += qmax_qsim
        totals["best_raw"] += best_raw
        totals["bank13"] += bank13
        totals["bank14"] += bank14
        totals["runners"] += runners
        totals["dead"] += dead

    if rows:
        coverage = totals["qmax_n"] / totals["qsim_n"] if totals["qsim_n"] else 0.0
        best = max(totals["qmax_qsim"], totals["best_raw"], totals["bank13"], totals["bank14"])
        verdict = _verdict(
            totals["qsim_n"], totals["qmax_n"], coverage, totals["qmax_qsim"], best,
            args.min_coverage, args.min_qmax_n,
        )
        print("-" * len(hdr))
        print(
            f"{'TOTAL':<8} {totals['shadow_n']:>5} {totals['qsim_n']:>5} {totals['qmax_n']:>6} "
            f"{coverage:>4.0%} {totals['shadow']:>+9.2f} {totals['qsim']:>+9.2f} "
            f"{totals['qmax_qsim']:>+9.2f} {totals['best_raw']:>+9.2f} "
            f"{totals['bank13']:>+9.2f} {totals['bank14']:>+9.2f} "
            f"{totals['runners']:>5} {totals['dead']:>5} {verdict}"
        )
    else:
        print("No shadow rows matched the filter.")

    print("\nVerdicts: WAIT = insufficient qmax coverage; EXIT_WORK = qmax sample can be rescued by an exit replay; NO_EDGE = qmax sample stays negative.")


if __name__ == "__main__":
    main()
