"""
ws_market_exit_report.py — Join websocket market observations to paper exits.

This report answers the first audit question after adding Helius tx-derived
market snapshots:

    Did websocket observations see materially different lows/highs than the
    stored paper exit path?

Usage:
    python3 ws_market_exit_report.py --hours 24
    python3 ws_market_exit_report.py --hours 6 --limit 25
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db


def _as_float(value, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _fmt_sol(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):+.4f}"


def _fmt_num(value, digits: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.{digits}f}"


def _fmt_mcap(value) -> str:
    if value is None:
        return "n/a"
    return f"${float(value)/1000:.1f}k"


def _load_rows(hours: int) -> list[dict]:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            WITH obs AS (
                SELECT
                    call_id,
                    COUNT(*) AS obs_count,
                    COUNT(*) FILTER (WHERE source = 'helius_tx') AS helius_obs,
                    COUNT(*) FILTER (WHERE source IS DISTINCT FROM 'helius_tx') AS fallback_obs,
                    MIN(mcap) AS min_obs_mcap,
                    MAX(mcap) AS max_obs_mcap,
                    (ARRAY_AGG(mcap ORDER BY observed_at ASC))[1] AS first_obs_mcap,
                    (ARRAY_AGG(mcap ORDER BY observed_at DESC))[1] AS last_obs_mcap,
                    (ARRAY_AGG(observed_at ORDER BY observed_at ASC))[1] AS first_obs_at,
                    (ARRAY_AGG(observed_at ORDER BY observed_at DESC))[1] AS last_obs_at,
                    (ARRAY_AGG(observed_at ORDER BY mcap ASC NULLS LAST, observed_at ASC))[1] AS min_obs_at,
                    JSONB_AGG(
                        JSONB_BUILD_OBJECT(
                            'observed_at', observed_at,
                            'mcap', mcap,
                            'source', source
                        )
                        ORDER BY observed_at ASC
                    ) AS observations_json
                FROM ws_market_observations
                WHERE observed_at >= NOW() - (%s * INTERVAL '1 hour')
                  AND mcap IS NOT NULL
                GROUP BY call_id
            )
            SELECT
                CASE WHEN tp.is_strategy_b THEN 'B' ELSE 'A' END AS strategy,
                tp.call_id,
                t.symbol,
                ch.handle AS channel_handle,
                COALESCE(tp.vip_tier, c.vip_tier, '') AS vip_tier,
                COALESCE(c.conviction_score, 0) AS conviction_score,
                tp.status,
                tp.entry_time,
                tp.exit_time,
                tp.exit_reason,
                tp.entry_price,
                tp.exit_price,
                tp.sol_in,
                tp.pnl_sol,
                tp.pnl_pct,
                tp.peak_mcap,
                tp.peak_multiplier,
                obs.obs_count,
                obs.helius_obs,
                obs.fallback_obs,
                obs.first_obs_mcap,
                obs.last_obs_mcap,
                obs.min_obs_mcap,
                obs.max_obs_mcap,
                obs.first_obs_at,
                obs.last_obs_at,
                obs.min_obs_at,
                obs.observations_json,
                CASE
                    WHEN tp.entry_price > 0 THEN obs.first_obs_mcap / tp.entry_price
                    ELSE NULL
                END AS first_obs_mult,
                CASE
                    WHEN tp.entry_price > 0 THEN obs.last_obs_mcap / tp.entry_price
                    ELSE NULL
                END AS last_obs_mult,
                CASE
                    WHEN tp.entry_price > 0 THEN obs.min_obs_mcap / tp.entry_price
                    ELSE NULL
                END AS min_obs_mult,
                CASE
                    WHEN tp.entry_price > 0 THEN obs.max_obs_mcap / tp.entry_price
                    ELSE NULL
                END AS max_obs_mult,
                CASE
                    WHEN tp.entry_price > 0 AND tp.exit_price IS NOT NULL THEN tp.exit_price / tp.entry_price
                    ELSE NULL
                END AS exit_mult,
                CASE
                    WHEN tp.exit_time IS NOT NULL THEN
                        EXTRACT(EPOCH FROM (obs.first_obs_at - tp.exit_time))
                    ELSE NULL
                END AS first_obs_vs_exit_sec,
                CASE
                    WHEN tp.exit_time IS NOT NULL THEN
                        EXTRACT(EPOCH FROM (obs.last_obs_at - tp.exit_time))
                    ELSE NULL
                END AS last_obs_vs_exit_sec,
                CASE
                    WHEN tp.exit_time IS NOT NULL THEN
                        EXTRACT(EPOCH FROM (obs.min_obs_at - tp.exit_time))
                    ELSE NULL
                END AS min_obs_vs_exit_sec,
                CASE
                    WHEN COALESCE(tp.vip_tier, c.vip_tier, '') IN ('gamble', 'gamble_risk') THEN 0.70
                    ELSE 0.65
                END AS hard_stop_mult,
                CASE
                    WHEN tp.entry_price > 0 THEN
                        (obs.min_obs_mcap / tp.entry_price) <=
                        CASE
                            WHEN COALESCE(tp.vip_tier, c.vip_tier, '') IN ('gamble', 'gamble_risk') THEN 0.70
                            ELSE 0.65
                        END
                    ELSE FALSE
                END AS ws_touched_hard_stop
            FROM obs
            JOIN trading_positions tp
              ON tp.call_id = obs.call_id
             AND tp.is_simulation = TRUE
            JOIN calls c ON c.id = tp.call_id
            JOIN tokens t ON t.id = c.token_id
            LEFT JOIN channels ch ON ch.id = c.channel_id
            WHERE obs.last_obs_at >= tp.entry_time
              AND obs.first_obs_at <= COALESCE(tp.exit_time, NOW())
            ORDER BY obs.last_obs_at DESC, tp.call_id DESC, strategy ASC
            """,
            (hours,),
        )
        rows = cur.fetchall()
        for row in rows:
            row.update(_simulate_shadow_exit(row))
            row.update(_simulate_pre_exit_opportunity(row))
        return rows


def _parse_observed_at(value):
    if value is None or isinstance(value, datetime):
        return value
    try:
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _strategy_a_shadow_reason(
    *,
    current_mcap: float,
    peak_mcap: float,
    entry_mcap: float,
    channel_handle: str,
    vip_tier: str,
) -> tuple[str | None, float | None]:
    if entry_mcap <= 0:
        return None, None

    current_mult = current_mcap / entry_mcap
    peak_mult = peak_mcap / entry_mcap if peak_mcap > 0 else 0.0
    is_vip_gamble = vip_tier in ("gamble", "gamble_risk")
    is_free_solhouse = channel_handle == "solhousesignal"

    if current_mult >= 10.0:
        return "10x_tp", current_mcap
    if current_mult >= 5.0:
        return "5x_tp", current_mcap

    if is_free_solhouse and peak_mcap > 0:
        if peak_mult >= 10.0 and current_mult <= 4.0:
            return "profit_floor", entry_mcap * 4.0
        if peak_mult >= 5.0 and current_mult <= 2.5:
            return "profit_floor", entry_mcap * 2.5
        if peak_mult >= 3.0 and current_mult <= 1.75:
            return "profit_floor", entry_mcap * 1.75

    if peak_mcap > 0:
        if peak_mult >= 10.0:
            trail_pct = 0.12
        elif peak_mult >= 5.0:
            trail_pct = 0.15
        elif peak_mult >= 3.0:
            trail_pct = 0.22
        elif peak_mult >= 2.0:
            trail_pct = 0.25
        else:
            trail_pct = None
        if trail_pct is not None and (peak_mcap - current_mcap) / peak_mcap >= trail_pct:
            return "trail_stop", peak_mcap * (1.0 - trail_pct)

    hard_stop_pct = 0.30 if is_vip_gamble else 0.35
    if current_mult <= (1.0 - hard_stop_pct):
        return "hard_stop", current_mcap

    return None, None


def _strategy_b_shadow_reason(
    *,
    current_mcap: float,
    peak_mcap: float,
    entry_mcap: float,
    vip_tier: str,
) -> tuple[str | None, float | None]:
    if entry_mcap <= 0:
        return None, None

    current_mult = current_mcap / entry_mcap
    peak_mult = peak_mcap / entry_mcap if peak_mcap > 0 else 0.0
    is_vip_gamble = vip_tier in ("gamble", "gamble_risk")

    if not is_vip_gamble and current_mult >= 3.0:
        return "3x_tp", current_mcap

    if peak_mult >= 2.0 and (peak_mcap - current_mcap) / peak_mcap >= 0.25:
        return "trail_stop", peak_mcap * 0.75

    hard_stop_pct = 0.30 if is_vip_gamble else 0.35
    if current_mult <= (1.0 - hard_stop_pct):
        return "hard_stop", current_mcap

    return None, None


def _simulate_shadow_exit(row: dict) -> dict:
    entry_mcap = _as_float(row.get("entry_price"))
    sol_in = _as_float(row.get("sol_in"))
    stored_pnl = row.get("pnl_sol")
    exit_time = row.get("exit_time")
    strategy = row.get("strategy")
    observations = row.get("observations_json") or []

    result = {
        "shadow_exit_reason": None,
        "shadow_exit_mcap": None,
        "shadow_exit_at": None,
        "shadow_exit_mult": None,
        "shadow_pnl_sol": None,
        "shadow_delta_pnl_sol": None,
        "shadow_seconds_before_exit": None,
    }
    if entry_mcap <= 0 or sol_in <= 0:
        return result

    peak_mcap = entry_mcap
    for obs in observations:
        observed_at = _parse_observed_at(obs.get("observed_at"))
        current_mcap = _as_float(obs.get("mcap"))
        if current_mcap <= 0:
            continue
        if exit_time is not None and observed_at is not None and observed_at > exit_time:
            continue

        peak_mcap = max(peak_mcap, current_mcap)
        if strategy == "B":
            reason, exit_mcap = _strategy_b_shadow_reason(
                current_mcap=current_mcap,
                peak_mcap=peak_mcap,
                entry_mcap=entry_mcap,
                vip_tier=(row.get("vip_tier") or ""),
            )
        else:
            reason, exit_mcap = _strategy_a_shadow_reason(
                current_mcap=current_mcap,
                peak_mcap=peak_mcap,
                entry_mcap=entry_mcap,
                channel_handle=(row.get("channel_handle") or ""),
                vip_tier=(row.get("vip_tier") or ""),
            )

        if not reason:
            continue

        exit_mcap = exit_mcap or current_mcap
        shadow_mult = exit_mcap / entry_mcap
        shadow_pnl = sol_in * (shadow_mult - 1.0)
        result.update(
            {
                "shadow_exit_reason": reason,
                "shadow_exit_mcap": exit_mcap,
                "shadow_exit_at": observed_at,
                "shadow_exit_mult": shadow_mult,
                "shadow_pnl_sol": shadow_pnl,
                "shadow_delta_pnl_sol": (
                    shadow_pnl - float(stored_pnl) if stored_pnl is not None else None
                ),
                "shadow_seconds_before_exit": (
                    (exit_time - observed_at).total_seconds()
                    if exit_time is not None and observed_at is not None
                    else None
                ),
            }
        )
        return result

    return result


def _simulate_pre_exit_opportunity(row: dict) -> dict:
    entry_mcap = _as_float(row.get("entry_price"))
    sol_in = _as_float(row.get("sol_in"))
    stored_pnl = row.get("pnl_sol")
    exit_time = row.get("exit_time")
    observations = row.get("observations_json") or []
    result = {
        "pre_obs_count": 0,
        "pre_min_mcap": None,
        "pre_max_mcap": None,
        "pre_last_mcap": None,
        "pre_min_delta_pnl_sol": None,
        "pre_max_delta_pnl_sol": None,
        "pre_last_delta_pnl_sol": None,
    }
    if entry_mcap <= 0 or sol_in <= 0 or stored_pnl is None:
        return result

    pre_mcaps: list[float] = []
    pre_last_mcap = None
    for obs in observations:
        observed_at = _parse_observed_at(obs.get("observed_at"))
        current_mcap = _as_float(obs.get("mcap"))
        if current_mcap <= 0:
            continue
        if exit_time is not None and observed_at is not None and observed_at > exit_time:
            continue
        pre_mcaps.append(current_mcap)
        pre_last_mcap = current_mcap

    if not pre_mcaps:
        return result

    def delta_for(mcap: float) -> float:
        return sol_in * ((mcap / entry_mcap) - 1.0) - float(stored_pnl)

    pre_min = min(pre_mcaps)
    pre_max = max(pre_mcaps)
    result.update(
        {
            "pre_obs_count": len(pre_mcaps),
            "pre_min_mcap": pre_min,
            "pre_max_mcap": pre_max,
            "pre_last_mcap": pre_last_mcap,
            "pre_min_delta_pnl_sol": delta_for(pre_min),
            "pre_max_delta_pnl_sol": delta_for(pre_max),
            "pre_last_delta_pnl_sol": delta_for(pre_last_mcap),
        }
    )
    return result


def _print_summary(rows: list[dict]) -> None:
    by_strategy: dict[str, dict[str, float]] = {}
    for row in rows:
        strategy = row["strategy"]
        stats = by_strategy.setdefault(
            strategy,
            {
                "positions": 0,
                "closed": 0,
                "obs": 0,
                "helius": 0,
                "fallback": 0,
                "obs_before_exit": 0,
                "min_before_exit": 0,
                "hard_touch": 0,
                "hard_touch_not_hard_exit": 0,
                "shadow_exits": 0,
                "shadow_delta": 0.0,
                "pre_last_delta": 0.0,
                "pre_min_delta": 0.0,
                "pre_max_delta": 0.0,
            },
        )
        stats["positions"] += 1
        stats["closed"] += 1 if row["status"] == "closed" else 0
        stats["obs"] += int(row["obs_count"] or 0)
        stats["helius"] += int(row["helius_obs"] or 0)
        stats["fallback"] += int(row["fallback_obs"] or 0)
        if row["first_obs_vs_exit_sec"] is not None and float(row["first_obs_vs_exit_sec"]) <= 0:
            stats["obs_before_exit"] += 1
        if row["min_obs_vs_exit_sec"] is not None and float(row["min_obs_vs_exit_sec"]) <= 0:
            stats["min_before_exit"] += 1
        if row["ws_touched_hard_stop"]:
            stats["hard_touch"] += 1
            if row["exit_reason"] != "hard_stop":
                stats["hard_touch_not_hard_exit"] += 1
        if row["shadow_exit_reason"]:
            stats["shadow_exits"] += 1
            stats["shadow_delta"] += float(row["shadow_delta_pnl_sol"] or 0.0)
        stats["pre_last_delta"] += float(row["pre_last_delta_pnl_sol"] or 0.0)
        stats["pre_min_delta"] += float(row["pre_min_delta_pnl_sol"] or 0.0)
        stats["pre_max_delta"] += float(row["pre_max_delta_pnl_sol"] or 0.0)

    print("Summary")
    print("-" * 100)
    print(
        f"{'strategy':<8} {'positions':>9} {'closed':>7} {'obs':>7} "
        f"{'helius':>7} {'fallback':>9} {'obs_pre':>8} {'min_pre':>8} "
        f"{'ws_hard':>8} {'hard_not_exit':>13} {'shadow':>8} {'sh_delta':>10} "
        f"{'last_pre':>10} {'min_preΔ':>10} {'max_preΔ':>10}"
    )
    for strategy in sorted(by_strategy):
        stats = by_strategy[strategy]
        print(
            f"{strategy:<8} {int(stats['positions']):>9} {int(stats['closed']):>7} "
            f"{int(stats['obs']):>7} {int(stats['helius']):>7} "
            f"{int(stats['fallback']):>9} {int(stats['obs_before_exit']):>8} "
            f"{int(stats['min_before_exit']):>8} {int(stats['hard_touch']):>8} "
            f"{int(stats['hard_touch_not_hard_exit']):>13} "
            f"{int(stats['shadow_exits']):>8} {_fmt_sol(stats['shadow_delta']):>10} "
            f"{_fmt_sol(stats['pre_last_delta']):>10} "
            f"{_fmt_sol(stats['pre_min_delta']):>10} "
            f"{_fmt_sol(stats['pre_max_delta']):>10}"
        )


def _print_rows(rows: list[dict], limit: int) -> None:
    print("\nObserved Positions")
    print("-" * 160)
    print(
        f"{'strat':<5} {'call':>7} {'symbol':<14} {'channel':<20} {'score':>6} "
        f"{'obs':>4} {'src':>9} {'min':>9} {'max':>9} {'last':>9} "
        f"{'minx':>6} {'maxx':>6} {'exitx':>6} {'min_vs_exit':>11} "
        f"{'last_vs_exit':>12} {'reason':<13} {'pnl':>9} {'shadow':<11} "
        f"{'sh_pnl':>9} {'delta':>9} {'lead_s':>7} {'last_pre':>9} "
        f"{'min_preΔ':>9} {'max_preΔ':>9} {'ws_hard':>7}"
    )
    for row in rows[:limit]:
        source_mix = f"{int(row['helius_obs'] or 0)}/{int(row['fallback_obs'] or 0)}"
        print(
            f"{row['strategy']:<5} {int(row['call_id']):>7} "
            f"{(row['symbol'] or '?')[:14]:<14} {(row['channel_handle'] or '?')[:20]:<20} "
            f"{float(row['conviction_score'] or 0):>6.1f} "
            f"{int(row['obs_count'] or 0):>4} {source_mix:>9} "
            f"{_fmt_mcap(row['min_obs_mcap']):>9} {_fmt_mcap(row['max_obs_mcap']):>9} "
            f"{_fmt_mcap(row['last_obs_mcap']):>9} "
            f"{_fmt_num(row['min_obs_mult']):>6} {_fmt_num(row['max_obs_mult']):>6} "
            f"{_fmt_num(row['exit_mult']):>6} "
            f"{_fmt_num(row['min_obs_vs_exit_sec'], 1):>11} "
            f"{_fmt_num(row['last_obs_vs_exit_sec'], 1):>12} "
            f"{(row['exit_reason'] or row['status'] or '?')[:13]:<13} "
            f"{_fmt_sol(row['pnl_sol']):>9} "
            f"{(row['shadow_exit_reason'] or '-')[:11]:<11} "
            f"{_fmt_sol(row['shadow_pnl_sol']):>9} "
            f"{_fmt_sol(row['shadow_delta_pnl_sol']):>9} "
            f"{_fmt_num(row['shadow_seconds_before_exit'], 1):>7} "
            f"{_fmt_sol(row['pre_last_delta_pnl_sol']):>9} "
            f"{_fmt_sol(row['pre_min_delta_pnl_sol']):>9} "
            f"{_fmt_sol(row['pre_max_delta_pnl_sol']):>9} "
            f"{str(bool(row['ws_touched_hard_stop'])):>7}"
        )


def _print_timing_notes(rows: list[dict]) -> None:
    closed_rows = [row for row in rows if row["exit_time"] is not None]
    if not closed_rows:
        return
    before_exit = [
        row for row in closed_rows
        if row["first_obs_vs_exit_sec"] is not None and float(row["first_obs_vs_exit_sec"]) <= 0
    ]
    min_before_exit = [
        row for row in closed_rows
        if row["min_obs_vs_exit_sec"] is not None and float(row["min_obs_vs_exit_sec"]) <= 0
    ]
    print("\nTiming Notes")
    print("-" * 100)
    print(
        f"closed_with_observations={len(closed_rows)}  "
        f"first_observation_before_or_at_exit={len(before_exit)}  "
        f"min_observation_before_or_at_exit={len(min_before_exit)}"
    )
    print("Negative timing values mean the websocket observation arrived before the stored paper exit.")


def _print_shadow_notes(rows: list[dict]) -> None:
    shadow_rows = [row for row in rows if row["shadow_exit_reason"]]
    if not shadow_rows:
        print("\nShadow Exit Notes")
        print("-" * 100)
        print("No websocket observations would have triggered the current paper exit rules before stored exit.")
        return

    print("\nShadow Exit Notes")
    print("-" * 100)
    by_reason: dict[str, dict[str, float]] = {}
    for row in shadow_rows:
        reason = row["shadow_exit_reason"] or "unknown"
        stats = by_reason.setdefault(reason, {"count": 0, "delta": 0.0})
        stats["count"] += 1
        stats["delta"] += float(row["shadow_delta_pnl_sol"] or 0.0)
    print(
        "Shadow exits replay the current paper exit rules over websocket observations only, "
        "using observations before the stored exit time."
    )
    for reason in sorted(by_reason):
        stats = by_reason[reason]
        print(f"{reason:<13} count={int(stats['count']):>3}  pnl_delta={_fmt_sol(stats['delta'])}")


def _print_opportunity_notes(rows: list[dict]) -> None:
    rows_with_pre = [row for row in rows if int(row.get("pre_obs_count") or 0) > 0]
    if not rows_with_pre:
        return

    last_delta = sum(float(row["pre_last_delta_pnl_sol"] or 0.0) for row in rows_with_pre)
    min_delta = sum(float(row["pre_min_delta_pnl_sol"] or 0.0) for row in rows_with_pre)
    max_delta = sum(float(row["pre_max_delta_pnl_sol"] or 0.0) for row in rows_with_pre)
    print("\nPre-Exit Opportunity Notes")
    print("-" * 100)
    print(
        "last_pre is the PnL delta if exiting at the last websocket observation before stored exit. "
        "min/max pre deltas are lower/upper bounds from observed pre-exit prices."
    )
    print(
        f"rows_with_pre_exit_observations={len(rows_with_pre)}  "
        f"last_pre_delta={_fmt_sol(last_delta)}  "
        f"min_pre_delta={_fmt_sol(min_delta)}  "
        f"max_pre_delta={_fmt_sol(max_delta)}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Join websocket market observations to paper exits")
    parser.add_argument("--hours", type=int, default=24, help="Lookback window")
    parser.add_argument("--limit", type=int, default=30, help="Detail row limit")
    args = parser.parse_args()

    db.ensure_ws_market_observations_table()
    rows = _load_rows(args.hours)

    print("=" * 100)
    print(f"Websocket Market Exit Report — last {args.hours} hour(s)")
    print("=" * 100)
    if not rows:
        print("No observed paper positions found.")
        return

    _print_summary(rows)
    _print_rows(rows, args.limit)
    _print_timing_notes(rows)
    _print_shadow_notes(rows)
    _print_opportunity_notes(rows)


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
