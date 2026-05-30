"""
ml_dataset_export.py — Export a model-friendly dataset from calls, paper trades,
outcomes, and volume snapshots.

This is the first AI/ML pipeline step: build one row per call with
entry-time features plus later labels. The output is CSV on purpose so we can
inspect and iterate on feature shape before adding heavier ML dependencies.

Examples:
    python3 ml_dataset_export.py --days 30
    python3 ml_dataset_export.py --date-from 2026-05-01 --date-to 2026-05-30
    python3 ml_dataset_export.py --days 14 --output exports/ml_dataset_last14d.csv
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from datetime import datetime, timedelta, timezone

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db


def _parse_dt(value: str) -> datetime:
    fmts = ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M")
    for fmt in fmts:
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise ValueError(f"Unsupported datetime format: {value}")


def _default_output_path() -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    base_dir = os.path.join(os.path.dirname(__file__), "exports")
    os.makedirs(base_dir, exist_ok=True)
    return os.path.join(base_dir, f"ml_dataset_{stamp}.csv")


def _load_rows(since: datetime | None, until: datetime | None) -> list[dict]:
    conn = db.get_conn()
    params: list[object] = []
    clauses: list[str] = []
    if since:
        clauses.append("c.created_at >= %s")
        params.append(since)
    if until:
        clauses.append("c.created_at < %s")
        params.append(until)
    where_sql = ""
    if clauses:
        where_sql = "WHERE " + " AND ".join(clauses)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                c.id AS call_id,
                c.created_at,
                c.source_platform,
                c.source_message_id,
                c.message_type,
                c.vip_message_type,
                ch.handle AS channel_handle,
                ch.channel_type,
                COALESCE(c.vip_tier, '') AS vip_tier,
                COALESCE(c.conviction_score, 0) AS conviction_score,
                COALESCE(c.skip_reason, '') AS skip_reason,
                COALESCE(c.mcap_at_call, 0) AS mcap_at_call,
                EXTRACT(HOUR FROM c.created_at AT TIME ZONE 'America/Los_Angeles')::int AS local_hour_pst,
                EXTRACT(DOW FROM c.created_at AT TIME ZONE 'America/Los_Angeles')::int AS local_dow_pst,
                t.id AS token_id,
                COALESCE(t.symbol, '') AS symbol,
                COALESCE(t.name, '') AS token_name,
                COALESCE(t.mint_address, '') AS mint_address,
                t.mint_resolved,
                t.is_cto,
                t.security_flag,
                t.token_age_minutes,
                t.bundle_count,
                t.bundle_pct_initial,
                t.bundle_pct_remaining,
                t.sniper_count,
                t.sniper_pct_initial,
                t.sniper_pct_remaining,
                t.first_20_pct,
                t.dev_sol_held,
                t.dev_pct_held,
                t.dev_sold,
                t.dev_tokens_made,
                t.dev_bonds,
                t.dev_best_mcap,
                t.bundled_pct,
                t.bundled_sold_pct,
                t.hodl_count,
                t.liq_at_detection,
                t.vol_1h_at_detection,
                t.detecting_wallet_sol,
                t.detecting_sol_spent,
                t.fake_vol_usd,
                t.fake_vol_pct,
                o.stated_multiplier,
                o.mcap_at_result,
                o.mcap_1h,
                o.mcap_4h,
                o.mcap_24h,
                o.peak_multiplier AS outcome_peak_multiplier,
                o.peak_multiplier_from_entry AS outcome_peak_multiplier_from_entry,
                (
                    (
                        SELECT COUNT(DISTINCT ch2.handle)
                        FROM calls c2
                        JOIN channels ch2 ON ch2.id = c2.channel_id
                        WHERE c2.token_id = t.id
                          AND ch2.handle IN ('solwhaletrending', 'solearlytrending')
                          AND ABS(EXTRACT(EPOCH FROM (c2.created_at - c.created_at))) <= 3600
                    ) >= 2
                    OR
                    (
                        EXISTS (
                            SELECT 1
                            FROM calls c2
                            JOIN channels ch2 ON ch2.id = c2.channel_id
                            WHERE c2.token_id = t.id
                              AND ch2.handle = 'solhousesignal_vip'
                              AND ABS(EXTRACT(EPOCH FROM (c2.created_at - c.created_at))) <= 14400
                        )
                        AND EXISTS (
                            SELECT 1
                            FROM calls c2
                            JOIN channels ch2 ON ch2.id = c2.channel_id
                            WHERE c2.token_id = t.id
                              AND ch2.handle = 'solhousesignal'
                              AND ABS(EXTRACT(EPOCH FROM (c2.created_at - c.created_at))) <= 14400
                        )
                    )
                ) AS cross_channel_confirmed,
                a_tp.call_id IS NOT NULL AS a_traded,
                a_tp.status AS a_status,
                a_tp.vip_tier AS a_position_vip_tier,
                a_tp.sol_in AS a_sol_in,
                a_tp.entry_price AS a_entry_price,
                a_tp.entry_time AS a_entry_time,
                a_tp.entry_volume_h1 AS a_entry_volume_h1,
                a_tp.exit_price AS a_exit_price,
                a_tp.exit_time AS a_exit_time,
                a_tp.exit_reason AS a_exit_reason,
                a_tp.pnl_sol AS a_pnl_sol,
                a_tp.pnl_pct AS a_pnl_pct,
                a_tp.peak_mcap AS a_peak_mcap,
                a_tp.peak_multiplier AS a_peak_multiplier,
                b_tp.call_id IS NOT NULL AS b_traded,
                b_tp.status AS b_status,
                b_tp.vip_tier AS b_position_vip_tier,
                b_tp.sol_in AS b_sol_in,
                b_tp.entry_price AS b_entry_price,
                b_tp.entry_time AS b_entry_time,
                b_tp.entry_volume_h1 AS b_entry_volume_h1,
                b_tp.exit_price AS b_exit_price,
                b_tp.exit_time AS b_exit_time,
                b_tp.exit_reason AS b_exit_reason,
                b_tp.pnl_sol AS b_pnl_sol,
                b_tp.pnl_pct AS b_pnl_pct,
                b_tp.peak_mcap AS b_peak_mcap,
                b_tp.peak_multiplier AS b_peak_multiplier,
                t0.observed_at AS snap_t0_observed_at,
                t0.age_minutes AS snap_t0_age_minutes,
                t0.mcap AS snap_t0_mcap,
                t0.liquidity_usd AS snap_t0_liquidity_usd,
                t0.volume_h1 AS snap_t0_volume_h1,
                t0.price_usd AS snap_t0_price_usd,
                t30.observed_at AS snap_t30_observed_at,
                t30.age_minutes AS snap_t30_age_minutes,
                t30.mcap AS snap_t30_mcap,
                t30.liquidity_usd AS snap_t30_liquidity_usd,
                t30.volume_h1 AS snap_t30_volume_h1,
                t30.price_usd AS snap_t30_price_usd,
                t60.observed_at AS snap_t60_observed_at,
                t60.age_minutes AS snap_t60_age_minutes,
                t60.mcap AS snap_t60_mcap,
                t60.liquidity_usd AS snap_t60_liquidity_usd,
                t60.volume_h1 AS snap_t60_volume_h1,
                t60.price_usd AS snap_t60_price_usd
            FROM calls c
            JOIN tokens t ON t.id = c.token_id
            LEFT JOIN channels ch ON ch.id = c.channel_id
            LEFT JOIN outcomes o ON o.call_id = c.id
            LEFT JOIN LATERAL (
                SELECT tp.*
                FROM trading_positions tp
                WHERE tp.call_id = c.id
                  AND tp.is_simulation = TRUE
                  AND tp.is_strategy_b = FALSE
                ORDER BY tp.entry_time DESC NULLS LAST, tp.id DESC
                LIMIT 1
            ) a_tp ON TRUE
            LEFT JOIN LATERAL (
                SELECT tp.*
                FROM trading_positions tp
                WHERE tp.call_id = c.id
                  AND tp.is_simulation = TRUE
                  AND tp.is_strategy_b = TRUE
                ORDER BY tp.entry_time DESC NULLS LAST, tp.id DESC
                LIMIT 1
            ) b_tp ON TRUE
            LEFT JOIN token_volume_snapshots t0
                ON t0.call_id = c.id
               AND t0.snapshot_label = 't_plus_00m'
            LEFT JOIN token_volume_snapshots t30
                ON t30.call_id = c.id
               AND t30.snapshot_label = 't_plus_30m'
            LEFT JOIN token_volume_snapshots t60
                ON t60.call_id = c.id
               AND t60.snapshot_label = 't_plus_60m'
            {where_sql}
            ORDER BY c.created_at ASC, c.id ASC
            """,
            params,
        )
        return cur.fetchall()


def _safe_float(value):
    if value is None or value == "":
        return None
    return float(value)


def _ratio(numerator, denominator):
    num = _safe_float(numerator)
    den = _safe_float(denominator)
    if num is None or den in (None, 0.0):
        return None
    return num / den


def _augment_row(row: dict) -> dict:
    out = dict(row)

    outcome_peak = max(
        [
            v
            for v in (
                _safe_float(row.get("outcome_peak_multiplier")),
                _safe_float(row.get("outcome_peak_multiplier_from_entry")),
                _ratio(row.get("mcap_at_result"), row.get("mcap_at_call")),
                _ratio(row.get("mcap_1h"), row.get("mcap_at_call")),
                _ratio(row.get("mcap_4h"), row.get("mcap_at_call")),
                _ratio(row.get("mcap_24h"), row.get("mcap_at_call")),
            )
            if v is not None
        ],
        default=None,
    )
    out["label_signal_peak_mult"] = outcome_peak
    out["label_signal_peak_ge_2x"] = int(outcome_peak is not None and outcome_peak >= 2.0)
    out["label_signal_peak_ge_5x"] = int(outcome_peak is not None and outcome_peak >= 5.0)
    out["label_signal_peak_lt_1x"] = int(outcome_peak is not None and outcome_peak < 1.0)

    a_peak = _safe_float(row.get("a_peak_multiplier"))
    out["label_a_peak_ge_2x"] = int(a_peak is not None and a_peak >= 2.0)
    out["label_a_peak_ge_5x"] = int(a_peak is not None and a_peak >= 5.0)
    out["label_a_pnl_positive"] = int(_safe_float(row.get("a_pnl_sol")) is not None and float(row["a_pnl_sol"]) > 0)
    out["label_a_hard_stop"] = int((row.get("a_exit_reason") or "") == "hard_stop")
    out["label_a_traded"] = int(bool(row.get("a_traded")))

    b_peak = _safe_float(row.get("b_peak_multiplier"))
    out["label_b_peak_ge_2x"] = int(b_peak is not None and b_peak >= 2.0)
    out["label_b_peak_ge_5x"] = int(b_peak is not None and b_peak >= 5.0)
    out["label_b_pnl_positive"] = int(_safe_float(row.get("b_pnl_sol")) is not None and float(row["b_pnl_sol"]) > 0)
    out["label_b_hard_stop"] = int((row.get("b_exit_reason") or "") == "hard_stop")
    out["label_b_traded"] = int(bool(row.get("b_traded")))

    out["snap_t30_volume_over_t0"] = _ratio(row.get("snap_t30_volume_h1"), row.get("snap_t0_volume_h1"))
    out["snap_t60_volume_over_t0"] = _ratio(row.get("snap_t60_volume_h1"), row.get("snap_t0_volume_h1"))
    out["snap_t30_mcap_over_t0"] = _ratio(row.get("snap_t30_mcap"), row.get("snap_t0_mcap"))
    out["snap_t60_mcap_over_t0"] = _ratio(row.get("snap_t60_mcap"), row.get("snap_t0_mcap"))
    out["snap_t30_liquidity_over_t0"] = _ratio(row.get("snap_t30_liquidity_usd"), row.get("snap_t0_liquidity_usd"))
    out["snap_t60_liquidity_over_t0"] = _ratio(row.get("snap_t60_liquidity_usd"), row.get("snap_t0_liquidity_usd"))
    out["snap_t0_volume_to_mcap"] = _ratio(row.get("snap_t0_volume_h1"), row.get("snap_t0_mcap"))
    out["snap_t30_volume_to_mcap"] = _ratio(row.get("snap_t30_volume_h1"), row.get("snap_t30_mcap"))
    out["snap_t60_volume_to_mcap"] = _ratio(row.get("snap_t60_volume_h1"), row.get("snap_t60_mcap"))
    out["snap_t0_liquidity_to_mcap"] = _ratio(row.get("snap_t0_liquidity_usd"), row.get("snap_t0_mcap"))
    out["snap_t30_liquidity_to_mcap"] = _ratio(row.get("snap_t30_liquidity_usd"), row.get("snap_t30_mcap"))
    out["snap_t60_liquidity_to_mcap"] = _ratio(row.get("snap_t60_liquidity_usd"), row.get("snap_t60_mcap"))
    out["feature_has_t0_snapshot"] = int(row.get("snap_t0_mcap") is not None or row.get("snap_t0_volume_h1") is not None)
    out["feature_has_t30_snapshot"] = int(row.get("snap_t30_mcap") is not None or row.get("snap_t30_volume_h1") is not None)
    out["feature_has_t60_snapshot"] = int(row.get("snap_t60_mcap") is not None or row.get("snap_t60_volume_h1") is not None)

    return out


def _write_csv(rows: list[dict], output_path: str) -> None:
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(key)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export a model-friendly call dataset to CSV")
    parser.add_argument("--days", type=int, default=None, help="Trailing day window")
    parser.add_argument("--date-from", dest="date_from", default=None, help="UTC date/datetime start")
    parser.add_argument("--date-to", dest="date_to", default=None, help="UTC date/datetime end (exclusive)")
    parser.add_argument("--output", default=None, help="CSV output path")
    args = parser.parse_args()

    since = None
    until = None
    if args.days:
        since = datetime.now(timezone.utc) - timedelta(days=args.days)
    if args.date_from:
        since = _parse_dt(args.date_from)
    if args.date_to:
        until = _parse_dt(args.date_to)

    output_path = args.output or _default_output_path()
    raw_rows = _load_rows(since, until)
    rows = [_augment_row(row) for row in raw_rows]
    _write_csv(rows, output_path)

    print("=" * 72)
    print("ML Dataset Export")
    print("=" * 72)
    print(f"rows={len(rows)}")
    print(f"output={output_path}")
    if rows:
        a_traded = sum(int(row.get("label_a_traded", 0)) for row in rows)
        signal_2x = sum(int(row.get("label_signal_peak_ge_2x", 0)) for row in rows)
        t0_count = sum(int(row.get("feature_has_t0_snapshot", 0)) for row in rows)
        t30_count = sum(int(row.get("feature_has_t30_snapshot", 0)) for row in rows)
        t60_count = sum(int(row.get("feature_has_t60_snapshot", 0)) for row in rows)
        print(f"a_traded={a_traded}")
        print(f"signal_2x_plus={signal_2x}")
        print(f"snapshots_t0={t0_count}  t30={t30_count}  t60={t60_count}")


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
