"""
qsim_quote_capture_replay.py — replay qsim rows using raw Jupiter quote spikes.

This is a read-only diagnostic for the live/shadow/qsim gap:

    "If Jupiter actually showed a temporary sell quote spike, how much did qsim
     leave on the table by not banking it?"

It does NOT use shadow feed prices to invent exits. Shadow is included only as a
comparison column. The replay variants are intentionally simple:

    floor_2x / floor_3x / floor_5x
        If any observed Jupiter quote reaches the threshold, exit at exactly the
        threshold. This is conservative and ignores upside beyond the threshold.

    obs_2x / obs_3x / obs_5x
        If any observed Jupiter quote reaches the threshold, exit at the first
        observed quote multiple that crossed it.

    confirm_2x / confirm_3x / confirm_5x
        Same as obs_*, but requires two consecutive quote observations above the
        threshold. Useful for measuring whether a rule would be too slow.

Examples:
    python3 qsim_quote_capture_replay.py --days 1 --channel solwhaletrending --lane low_score --variant early --detail
    python3 qsim_quote_capture_replay.py --days 7 --channel solwhaletrending --lane low_score --variant early --max-entry-ratio 2
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))


MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5
THRESHOLDS = (2.0, 3.0, 5.0)


SQL = """
WITH base AS (
    SELECT
        q.call_id,
        t.symbol,
        COALESCE(ch.handle, '?') AS channel,
        COALESCE(c.skip_reason, 'none') AS lane,
        q.variant,
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
    JOIN tokens t ON t.id = q.token_id
    LEFT JOIN channels ch ON ch.id = c.channel_id
    LEFT JOIN shadow_positions sp
      ON sp.call_id = q.call_id
     AND sp.exit_variant = q.variant
     AND sp.status = 'closed'
    WHERE q.status = 'closed'
      AND q.entry_time >= now() - (%(days)s || ' days')::interval
      AND (%(channel)s = 'any' OR COALESCE(ch.handle, '?') = %(channel)s)
      AND (%(lane)s = 'any' OR COALESCE(c.skip_reason, 'none') = %(lane)s)
      AND (%(variant)s = 'any' OR q.variant = %(variant)s)
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
    qobs.exit_signal_count,
    qobs.no_route_count,
    qobs.max_qobs_mult,
    qobs.observations
FROM base b
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) AS qobs_count,
        COUNT(*) FILTER (WHERE qo.should_exit) AS exit_signal_count,
        COUNT(*) FILTER (WHERE qo.no_route) AS no_route_count,
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
) qobs ON TRUE
ORDER BY b.entry_time DESC
"""


@dataclass
class ReplayRow:
    row: dict[str, Any]
    qsim_return: float
    shadow_return: float | None
    max_quote_mult: float | None
    returns: dict[str, float]
    first_hits: dict[str, float | None]


def _rows(params: dict[str, Any]) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor
    import db

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


def _ratio(num: Any, den: Any) -> float | None:
    den_f = _f(den)
    if den_f <= 0:
        return None
    return _f(num) / den_f


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


def _quote_mults(row: dict[str, Any]) -> list[float]:
    mults: list[float] = []
    for obs in _observations(row.get("observations")):
        mult = _f(obs.get("real_mult"))
        if mult > 0:
            mults.append(mult)
    return mults


def _first_cross(mults: list[float], threshold: float) -> float | None:
    for mult in mults:
        if mult >= threshold:
            return mult
    return None


def _confirmed_cross(mults: list[float], threshold: float) -> float | None:
    prev_hit = False
    for mult in mults:
        hit = mult >= threshold
        if hit and prev_hit:
            return mult
        prev_hit = hit
    return None


def _view(row: dict[str, Any]) -> ReplayRow:
    qsim_return = _ratio(row.get("qsim_pnl"), row.get("qsim_sol_in")) or 0.0
    shadow_return = _ratio(row.get("shadow_pnl"), row.get("shadow_sol_in"))
    mults = _quote_mults(row)
    max_quote_mult = max(mults) if mults else None
    returns = {"current": qsim_return}
    first_hits: dict[str, float | None] = {}

    if max_quote_mult is not None:
        returns["best_raw"] = max_quote_mult - 1.0
    else:
        returns["best_raw"] = qsim_return

    for threshold in THRESHOLDS:
        suffix = f"{int(threshold)}x"
        first = _first_cross(mults, threshold)
        confirmed = _confirmed_cross(mults, threshold)
        first_hits[suffix] = first
        returns[f"floor_{suffix}"] = threshold - 1.0 if first is not None else qsim_return
        returns[f"obs_{suffix}"] = first - 1.0 if first is not None else qsim_return
        returns[f"confirm_{suffix}"] = confirmed - 1.0 if confirmed is not None else qsim_return

    return ReplayRow(
        row=row,
        qsim_return=qsim_return,
        shadow_return=shadow_return,
        max_quote_mult=max_quote_mult,
        returns=returns,
        first_hits=first_hits,
    )


def _pct(value: float | None) -> str:
    if value is None:
        return "   n/a "
    return f"{value * 100:+7.1f}%"


def _mult(value: float | None) -> str:
    if value is None:
        return "  n/a"
    return f"{value:>5.2f}"


def _entry_ratio(view: ReplayRow) -> float | None:
    return _ratio(view.row.get("qsim_entry"), view.row.get("shadow_entry"))


def _print_summary(views: list[ReplayRow]) -> None:
    if not views:
        print("No closed qsim rows found for that filter.\n")
        return

    n = len(views)
    with_qobs = [view for view in views if int(view.row.get("qobs_count") or 0) > 0]
    with_shadow = [view for view in views if view.shadow_return is not None]
    current = sum(view.returns["current"] for view in views)

    print("\nQSIM QUOTE CAPTURE REPLAY")
    print("PnL is normalized per 1 SOL deployed; replay exits use raw Jupiter quote observations only.\n")
    print(f"closed qsim rows:   {n}")
    print(f"rows with qobs:     {len(with_qobs)}/{n}")
    print(f"rows with shadow:   {len(with_shadow)}/{n}")
    print(f"current qsim sum:   {current:+.2f}")
    if with_shadow:
        shadow_sum = sum(view.shadow_return or 0.0 for view in with_shadow)
        print(f"shadow compare sum: {shadow_sum:+.2f} ({len(with_shadow)} matched)")

    print("\nReplay Totals")
    print(f"{'policy':<14} {'sum':>10} {'delta':>10} {'hits':>7} {'avg_hit':>9}")
    print("-" * 56)
    policies = [
        "best_raw",
        "floor_2x", "obs_2x", "confirm_2x",
        "floor_3x", "obs_3x", "confirm_3x",
        "floor_5x", "obs_5x", "confirm_5x",
    ]
    for policy in policies:
        total = sum(view.returns[policy] for view in views)
        if policy == "best_raw":
            hit_values = [view.max_quote_mult for view in views if view.max_quote_mult is not None]
        elif policy.startswith("confirm_"):
            threshold = float(policy.split("_")[1].replace("x", ""))
            hit_values = [_confirmed_cross(_quote_mults(view.row), threshold) for view in views]
            hit_values = [value for value in hit_values if value is not None]
        else:
            threshold = float(policy.split("_")[1].replace("x", ""))
            hit_values = [view.first_hits[f"{int(threshold)}x"] for view in views]
            hit_values = [value for value in hit_values if value is not None]
        avg_hit = sum(hit_values) / len(hit_values) if hit_values else None
        print(f"{policy:<14} {total:>+10.2f} {total - current:>+10.2f} {len(hit_values):>7} {_mult(avg_hit):>9}")


def _print_detail(views: list[ReplayRow], limit: int) -> None:
    rows = sorted(
        views,
        key=lambda view: view.returns["obs_2x"] - view.returns["current"],
        reverse=True,
    )[:limit]

    print("\nDetail — biggest obs_2x improvement first")
    hdr = (
        f"{'call':>7} {'symbol':<12} {'ent':>5} {'qpk':>5} {'qmax':>5} {'spk':>5} "
        f"{'cur':>8} {'obs2':>8} {'floor2':>8} {'conf2':>8} {'shadow':>8} "
        f"{'q_reason/shadow_reason':<28}"
    )
    print(hdr)
    print("-" * len(hdr))
    for view in rows:
        row = view.row
        reason_pair = f"{row.get('qsim_reason')}/{row.get('shadow_reason')}"
        print(
            f"{int(row['call_id']):>7} "
            f"{(row.get('symbol') or '?')[:12]:<12} "
            f"{_mult(_entry_ratio(view)):>5} "
            f"{_mult(_f(row.get('qsim_peak'))):>5} "
            f"{_mult(view.max_quote_mult):>5} "
            f"{_mult(_f(row.get('shadow_peak'))):>5} "
            f"{_pct(view.returns['current']):>8} "
            f"{_pct(view.returns['obs_2x']):>8} "
            f"{_pct(view.returns['floor_2x']):>8} "
            f"{_pct(view.returns['confirm_2x']):>8} "
            f"{_pct(view.shadow_return):>8} "
            f"{reason_pair[:28]:<28}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Replay qsim using raw Jupiter quote observations."
    )
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--channel", default="any")
    parser.add_argument("--lane", default="any")
    parser.add_argument("--variant", default="early")
    parser.add_argument("--min-entry-ratio", type=float, default=None)
    parser.add_argument("--max-entry-ratio", type=float, default=None)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--raw", action="store_true", help="include phantom-price rows")
    parser.add_argument("--detail", action="store_true", help="print per-trade rows")
    args = parser.parse_args()

    params = {
        "days": args.days,
        "channel": args.channel,
        "lane": args.lane,
        "variant": args.variant,
        "min_entry_ratio": args.min_entry_ratio,
        "max_entry_ratio": args.max_entry_ratio,
        "raw": args.raw,
        "max_peak": MAX_SANE_PEAK,
        "max_pnl": MAX_SANE_PNL_PCT,
        "min_pnl": MIN_SANE_PNL_PCT,
    }

    rows = _rows(params)
    views = [_view(row) for row in rows]
    print(
        f"filters: days={args.days} channel={args.channel} lane={args.lane} "
        f"variant={args.variant} min_entry_ratio={args.min_entry_ratio} "
        f"max_entry_ratio={args.max_entry_ratio}"
    )
    _print_summary(views)
    if views and args.detail:
        _print_detail(views, args.limit)
    elif views:
        print("\nRun with --detail for per-trade rows.\n")


if __name__ == "__main__":
    main()
