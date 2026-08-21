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

    bank_1.2x ... bank_2x
        Exit at the first observed quote multiple at/above that level.

    p50_bank_1.3x
        Sell 50% at the first observed quote >= 1.3x, then let the remaining
        50% use the original qsim outcome. Tests runner-preserving de-risking.

    p50_bank_1.3x_stop_1x
        Sell 50% at 1.3x, then exit the remaining 50% if the quote falls back
        to 1x. Tests de-risking plus "do not let the moonbag round-trip."

    lock_1.5x_1.2x
        Arm after quote reaches 1.5x, then exit if it falls back to 1.2x.

    lock_or_bank_1.5x_1.2x
        Same as lock_*, but if the floor never fires before qsim closes, bank at
        the final observed quote. This tests "never let an armed winner become a
        full loser" and is intentionally less conservative.

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

from exit_config import EXIT_A_PAPER, EXIT_RIDE, apply_exit_config


MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5
THRESHOLDS = (2.0, 3.0, 5.0)
BANK_LEVELS = (1.20, 1.30, 1.40, 1.50, 1.75, 2.0)
BANK_FRACTIONS = (0.25, 0.50, 0.75)
BANK_REMAINDER_STOPS = (0.85, 1.0, 1.10)
LOCK_FLOORS = (
    (1.30, 1.10),
    (1.40, 1.15),
    (1.50, 1.20),
    (1.75, 1.35),
    (2.00, 1.55),
)


SQL = """
WITH base AS (
    SELECT
        q.call_id,
        t.symbol,
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


def _bank_return(mults: list[float], level: float, current_return: float) -> float:
    """Exit at the first observed quote >= level; fallback to current qsim result."""
    first = _first_cross(mults, level)
    return first - 1.0 if first is not None else current_return


def _confirm_bank_return(mults: list[float], level: float, current_return: float) -> float:
    """Exit at the second consecutive observed quote >= level."""
    confirmed = _confirmed_cross(mults, level)
    return confirmed - 1.0 if confirmed is not None else current_return


def _partial_bank_return(
    mults: list[float],
    level: float,
    fraction: float,
    current_return: float,
) -> float:
    """
    Sell fraction at first quote >= level; remainder follows current qsim result.

    This keeps tail upside from existing winners while reducing the round-trip
    damage on trades that briefly touch 1.2x-1.7x before collapsing.
    """
    first = _first_cross(mults, level)
    if first is None:
        return current_return
    banked_return = first - 1.0
    return fraction * banked_return + (1.0 - fraction) * current_return


def _confirm_partial_bank_return(
    mults: list[float],
    level: float,
    fraction: float,
    current_return: float,
) -> float:
    confirmed = _confirmed_cross(mults, level)
    if confirmed is None:
        return current_return
    banked_return = confirmed - 1.0
    return fraction * banked_return + (1.0 - fraction) * current_return


def _partial_bank_with_stop_return(
    mults: list[float],
    level: float,
    fraction: float,
    stop: float,
    current_return: float,
) -> float:
    """
    Sell fraction at first quote >= level; remainder exits if it later falls to stop.

    If the remainder stop never fires, the remainder follows the original qsim
    result. This is the practical candidate live shape: take money when the
    quote proves strength, then stop the remaining bag from becoming a disaster.
    """
    banked_mult = None
    armed = False
    for mult in mults:
        if not armed and mult >= level:
            banked_mult = mult
            armed = True
            continue
        if armed and mult <= stop:
            banked_return = (banked_mult or level) - 1.0
            stop_return = mult - 1.0
            return fraction * banked_return + (1.0 - fraction) * stop_return

    if banked_mult is None:
        return current_return
    banked_return = banked_mult - 1.0
    return fraction * banked_return + (1.0 - fraction) * current_return


def _lock_floor_return(
    mults: list[float],
    trigger: float,
    floor: float,
    current_return: float,
) -> float:
    """
    Arm after quote peak reaches trigger, then exit if quote falls to floor.

    This models "bank the move before it round-trips" without pretending the bot
    could sell at the peak. If the floor never fires, fallback to current qsim.
    """
    armed = False
    for mult in mults:
        if not armed and mult >= trigger:
            armed = True
            continue
        if armed and mult <= floor:
            return mult - 1.0
    return current_return


def _lock_floor_or_bank_return(
    mults: list[float],
    trigger: float,
    floor: float,
    current_return: float,
) -> float:
    """
    Arm at trigger; if floor never fires, bank at the final observed quote.

    This is an optimistic-but-plausible "don't let an armed trade become a full
    loser" replay. It is less conservative than lock_floor_*.
    """
    armed = False
    last_mult = None
    for mult in mults:
        last_mult = mult
        if not armed and mult >= trigger:
            armed = True
            continue
        if armed and mult <= floor:
            return mult - 1.0
    if armed and last_mult is not None:
        return last_mult - 1.0
    return current_return


def _config_for_variant(variant: str | None):
    if variant in {"ride", "ride_vol"}:
        return EXIT_RIDE
    return EXIT_A_PAPER


def _raw_config_exit(row: dict[str, Any], mults: list[float]) -> tuple[float, str]:
    """
    Replay the real exit_config over raw Jupiter quote multiples.

    This intentionally bypasses peak_guard/trough_guard and treats a Jupiter
    sell quote as the executable current price. Time-stop is not simulated here
    because qobs contains quote samples, not a complete wall-clock runner path
    for older partially instrumented positions.
    """
    if not mults:
        return (_ratio(row.get("qsim_pnl"), row.get("qsim_sol_in")) or 0.0, "current")

    cfg = _config_for_variant(row.get("variant"))
    channel_handle = (row.get("channel") or "").lstrip("@")
    is_vip_gamble = row.get("vip_tier") in {"gamble", "gamble_risk"}
    peak_mult = 0.0
    for mult in mults:
        peak_mult = max(peak_mult, mult)
        result = apply_exit_config(
            cfg,
            current_mcap=mult,
            peak_mcap=peak_mult,
            entry_mcap=1.0,
            is_vip_gamble=is_vip_gamble,
            channel_handle=channel_handle,
            entry_time=None,
        )
        if result.should_exit:
            return mult - 1.0, result.reason or "raw_config"

    return (_ratio(row.get("qsim_pnl"), row.get("qsim_sol_in")) or 0.0, "held_to_current")


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

    raw_config_return, raw_config_reason = _raw_config_exit(row, mults)
    returns["raw_config"] = raw_config_return
    row["raw_config_reason"] = raw_config_reason

    for level in BANK_LEVELS:
        suffix = _level_suffix(level)
        returns[f"bank_{suffix}"] = _bank_return(mults, level, qsim_return)
        returns[f"confirm_bank_{suffix}"] = _confirm_bank_return(mults, level, qsim_return)
        for fraction in BANK_FRACTIONS:
            frac_suffix = _fraction_suffix(fraction)
            returns[f"{frac_suffix}_bank_{suffix}"] = _partial_bank_return(
                mults, level, fraction, qsim_return
            )
            returns[f"confirm_{frac_suffix}_bank_{suffix}"] = _confirm_partial_bank_return(
                mults, level, fraction, qsim_return
            )
            for stop in BANK_REMAINDER_STOPS:
                stop_suffix = _level_suffix(stop)
                returns[f"{frac_suffix}_bank_{suffix}_stop_{stop_suffix}"] = (
                    _partial_bank_with_stop_return(
                        mults, level, fraction, stop, qsim_return
                    )
                )

    for trigger, floor in LOCK_FLOORS:
        suffix = f"{_level_suffix(trigger)}_{_level_suffix(floor)}"
        returns[f"lock_{suffix}"] = _lock_floor_return(mults, trigger, floor, qsim_return)
        returns[f"lock_or_bank_{suffix}"] = _lock_floor_or_bank_return(
            mults, trigger, floor, qsim_return
        )

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


def _level_suffix(level: float) -> str:
    return f"{level:g}x".replace(".", "p")


def _fraction_suffix(fraction: float) -> str:
    return f"p{int(round(fraction * 100))}"


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
        "best_raw", "raw_config",
        "floor_2x", "obs_2x", "confirm_2x",
        "floor_3x", "obs_3x", "confirm_3x",
        "floor_5x", "obs_5x", "confirm_5x",
    ]
    for policy in policies:
        _print_policy_row(policy, views, current)

    print("\nEarly Bank Totals")
    print(f"{'policy':<18} {'sum':>10} {'delta':>10} {'hits':>7} {'avg_hit':>9}")
    print("-" * 60)
    for level in BANK_LEVELS:
        suffix = _level_suffix(level)
        _print_policy_row(f"bank_{suffix}", views, current, width=18)
        _print_policy_row(f"confirm_bank_{suffix}", views, current, width=18)

    print("\nPartial Bank Totals")
    print(f"{'policy':<24} {'sum':>10} {'delta':>10} {'hits':>7} {'avg_hit':>9}")
    print("-" * 66)
    for level in BANK_LEVELS:
        suffix = _level_suffix(level)
        for fraction in BANK_FRACTIONS:
            frac_suffix = _fraction_suffix(fraction)
            _print_policy_row(f"{frac_suffix}_bank_{suffix}", views, current, width=24)
            _print_policy_row(
                f"confirm_{frac_suffix}_bank_{suffix}", views, current, width=24
            )

    print("\nPartial Bank + Remainder Stop Totals")
    print(f"{'policy':<32} {'sum':>10} {'delta':>10} {'hits':>7} {'avg_hit':>9}")
    print("-" * 74)
    for level in (1.30, 1.40, 1.50):
        suffix = _level_suffix(level)
        for fraction in BANK_FRACTIONS:
            frac_suffix = _fraction_suffix(fraction)
            for stop in BANK_REMAINDER_STOPS:
                stop_suffix = _level_suffix(stop)
                _print_policy_row(
                    f"{frac_suffix}_bank_{suffix}_stop_{stop_suffix}",
                    views,
                    current,
                    width=32,
                )

    print("\nPeak Lock Totals")
    print(f"{'policy':<22} {'sum':>10} {'delta':>10} {'hits':>7} {'avg_hit':>9}")
    print("-" * 64)
    for trigger, floor in LOCK_FLOORS:
        suffix = f"{_level_suffix(trigger)}_{_level_suffix(floor)}"
        _print_policy_row(f"lock_{suffix}", views, current, width=22)
        _print_policy_row(f"lock_or_bank_{suffix}", views, current, width=22)


def _policy_hit_mults(policy: str, views: list[ReplayRow]) -> list[float]:
    if policy == "best_raw":
        return [view.max_quote_mult for view in views if view.max_quote_mult is not None]
    if policy == "raw_config":
        return [
            view.returns[policy] + 1.0
            for view in views
            if view.row.get("raw_config_reason") not in {"current", "held_to_current"}
        ]
    if policy.startswith("confirm_"):
        level = _level_from_policy(policy)
        return [
            value
            for view in views
            if (value := _confirmed_cross(_quote_mults(view.row), level)) is not None
        ]
    if policy.startswith("lock_") or policy.startswith("lock_or_bank_"):
        return [
            view.returns[policy] + 1.0
            for view in views
            if view.returns[policy] != view.returns["current"]
        ]
    if (
        policy.startswith("bank_")
        or policy.startswith("floor_")
        or policy.startswith("obs_")
        or "_bank_" in policy
    ):
        level = _bank_level_from_policy(policy) if "_bank_" in policy else _level_from_policy(policy)
        return [
            value
            for view in views
            if (value := _first_cross(_quote_mults(view.row), level)) is not None
        ]
    return []


def _level_from_policy(policy: str) -> float:
    parts = policy.split("_")
    token = next(part for part in reversed(parts) if part.endswith("x"))
    return float(token.removesuffix("x").replace("p", "."))


def _bank_level_from_policy(policy: str) -> float:
    parts = policy.split("_")
    bank_idx = parts.index("bank")
    token = parts[bank_idx + 1]
    return float(token.removesuffix("x").replace("p", "."))


def _print_policy_row(
    policy: str,
    views: list[ReplayRow],
    current: float,
    *,
    width: int = 14,
) -> None:
    total = sum(view.returns[policy] for view in views)
    hit_values = _policy_hit_mults(policy, views)
    avg_hit = sum(hit_values) / len(hit_values) if hit_values else None
    print(
        f"{policy:<{width}} {total:>+10.2f} {total - current:>+10.2f} "
        f"{len(hit_values):>7} {_mult(avg_hit):>9}"
    )


def _print_detail(views: list[ReplayRow], limit: int) -> None:
    rows = sorted(
        views,
        key=lambda view: max(
            view.returns["bank_1p3x"],
            view.returns["p50_bank_1p3x"],
            view.returns["lock_or_bank_1p5x_1p2x"],
            view.returns["obs_2x"],
        ) - view.returns["current"],
        reverse=True,
    )[:limit]

    print("\nDetail — biggest bank/lock improvement first")
    hdr = (
        f"{'call':>7} {'symbol':<12} {'ent':>5} {'qpk':>5} {'qmax':>5} {'spk':>5} "
        f"{'cur':>8} {'bank13':>8} {'p50b13':>8} {'bank15':>8} {'obs2':>8} {'shadow':>8} "
        f"{'raw_reason':<12} {'q_reason/shadow_reason':<28}"
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
            f"{_pct(view.returns['bank_1p3x']):>8} "
            f"{_pct(view.returns['p50_bank_1p3x']):>8} "
            f"{_pct(view.returns['bank_1p5x']):>8} "
            f"{_pct(view.returns['obs_2x']):>8} "
            f"{_pct(view.shadow_return):>8} "
            f"{(row.get('raw_config_reason') or '?')[:12]:<12} "
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
    parser.add_argument("--require-qobs", action="store_true",
                        help="only include rows with quote observations")
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
    if args.require_qobs:
        views = [view for view in views if int(view.row.get("qobs_count") or 0) > 0]
    print(
        f"filters: days={args.days} channel={args.channel} lane={args.lane} "
        f"variant={args.variant} min_entry_ratio={args.min_entry_ratio} "
        f"max_entry_ratio={args.max_entry_ratio} require_qobs={args.require_qobs}"
    )
    _print_summary(views)
    if views and args.detail:
        _print_detail(views, args.limit)
    elif views:
        print("\nRun with --detail for per-trade rows.\n")


if __name__ == "__main__":
    main()
