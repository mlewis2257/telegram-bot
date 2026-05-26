"""
strategy_a_window_report.py — Compare Strategy A paper performance across two date windows.

Usage:
    python3 strategy_a_window_report.py \
        --window-a-from 2026-05-01 --window-a-to 2026-05-11 \
        --window-b-from 2026-05-15 --window-b-to 2026-05-25 \
        --tz America/Los_Angeles

    python3 strategy_a_window_report.py \
        --window-a-from 2026-05-01 --window-a-to 2026-05-11 \
        --window-b-from 2026-05-15 --window-b-to 2026-05-25 \
        --tz America/Los_Angeles \
        --replay-strategy a \
        --replay-compare-against a_vip_tight_b
"""

import argparse
import os
import sys
from collections import Counter
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(__file__))

import db
from strategy_config import get_strategy_config
from strategy_engine import StrategyCallContext, evaluate_strategy_a_entry


def _parse_local_date(date_raw: str, tz_name: str) -> datetime:
    tz = ZoneInfo(tz_name)
    dt = datetime.strptime(date_raw.strip(), "%Y-%m-%d")
    return dt.replace(tzinfo=tz).astimezone(timezone.utc)


def _fmt_sol(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):+,.4f}"


def _fmt_pct(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):+.1f}%"


def _fmt_num(value: float | None, places: int = 2) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.{places}f}"


def _fmt_ratio(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.2f}x"


def _fmt_peak_bucket_counts(summary: dict) -> str:
    return (
        f"5x+={summary['trade_5x_plus']}, "
        f"2x-5x={summary['trade_2x_to_5x']}, "
        f"1x-2x={summary['trade_1x_to_2x']}, "
        f"<1x={summary['trade_lt_1x']}"
    )


def _print_metric_row(label: str, a_value: str, b_value: str) -> None:
    print(f"{label:<28} {a_value:>18} {b_value:>18}")


def _get_peak_column() -> str | None:
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = 'outcomes'
              AND column_name IN ('peak_multiplier_from_entry', 'peak_multiplier')
            ORDER BY CASE column_name
                WHEN 'peak_multiplier_from_entry' THEN 1
                WHEN 'peak_multiplier' THEN 2
                ELSE 99
            END
            LIMIT 1
            """
        )
        row = cur.fetchone()
        return row[0] if row else None


def _query_summary(since: datetime, until: datetime, peak_col: str | None) -> dict:
    conn = db.get_conn()
    signal_peak_expr = f"o.{peak_col}" if peak_col else "NULL"

    with conn.cursor() as cur:
        cur.execute(
            f"""
            WITH closed AS (
                SELECT
                    tp.call_id,
                    tp.pnl_sol,
                    tp.pnl_pct,
                    tp.exit_reason,
                    tp.entry_price,
                    tp.exit_price,
                    tp.peak_multiplier AS observed_peak_mult,
                    c.conviction_score,
                    ch.handle AS channel_handle,
                    {signal_peak_expr} AS signal_peak_mult,
                    CASE
                        WHEN c.conviction_score >= 85 THEN '85+'
                        WHEN c.conviction_score >= 70 THEN '70-84'
                        WHEN c.conviction_score >= 63 THEN '63-69'
                        ELSE '<63'
                    END AS score_bucket
                FROM trading_positions tp
                JOIN calls c      ON c.id = tp.call_id
                JOIN channels ch  ON ch.id = c.channel_id
                LEFT JOIN outcomes o ON o.call_id = c.id
                WHERE tp.is_simulation = TRUE
                  AND tp.is_strategy_b = FALSE
                  AND tp.status = 'closed'
                  AND tp.entry_time >= %s
                  AND tp.entry_time < %s
            )
            SELECT
                COUNT(*) AS trades,
                COUNT(*) FILTER (WHERE pnl_sol > 0) AS winners,
                COUNT(*) FILTER (WHERE pnl_sol <= 0) AS losers,
                COALESCE(SUM(pnl_sol), 0) AS total_pnl_sol,
                COALESCE(AVG(pnl_sol), 0) AS avg_pnl_sol,
                COALESCE(AVG(pnl_pct), 0) AS avg_pnl_pct,
                COUNT(*) FILTER (WHERE exit_reason = 'hard_stop') AS hard_stop_count,
                COUNT(*) FILTER (WHERE observed_peak_mult >= 2) AS observed_2x_plus,
                COUNT(*) FILTER (WHERE observed_peak_mult >= 3) AS observed_3x_plus,
                COUNT(*) FILTER (
                    WHERE observed_peak_mult >= 2 AND pnl_sol <= 0
                ) AS observed_2x_plus_closed_red,
                COALESCE(AVG(
                    CASE
                        WHEN entry_price IS NOT NULL AND exit_price IS NOT NULL AND entry_price > 0
                        THEN exit_price / entry_price
                    END
                ), 0) AS avg_exit_mult,
                COALESCE(AVG(observed_peak_mult), 0) AS avg_observed_peak_mult,
                COALESCE(AVG(signal_peak_mult), 0) AS avg_signal_peak_mult,
                COALESCE(AVG(
                    CASE
                        WHEN observed_peak_mult IS NOT NULL AND observed_peak_mult > 0
                         AND entry_price IS NOT NULL AND exit_price IS NOT NULL AND entry_price > 0
                        THEN (exit_price / entry_price) / observed_peak_mult
                    END
                ), 0) AS avg_peak_capture_ratio
            FROM closed
            """,
            (since, until),
        )
        row = cur.fetchone()

        cur.execute(
            """
            SELECT
                ch.handle,
                COUNT(*) AS trades,
                ROUND(SUM(tp.pnl_sol)::numeric, 4) AS pnl_sol,
                ROUND(AVG(tp.pnl_sol)::numeric, 4) AS avg_pnl_sol,
                ROUND(AVG(tp.pnl_pct)::numeric, 1) AS avg_pnl_pct
            FROM trading_positions tp
            JOIN calls c     ON c.id = tp.call_id
            JOIN channels ch ON ch.id = c.channel_id
            WHERE tp.is_simulation = TRUE
              AND tp.is_strategy_b = FALSE
              AND tp.status = 'closed'
              AND tp.entry_time >= %s
              AND tp.entry_time < %s
            GROUP BY ch.handle
            ORDER BY trades DESC, pnl_sol DESC
            """,
            (since, until),
        )
        by_channel = cur.fetchall()

        cur.execute(
            """
            WITH tagged AS (
                SELECT
                    CASE
                        WHEN c.conviction_score >= 85 THEN '85+'
                        WHEN c.conviction_score >= 70 THEN '70-84'
                        WHEN c.conviction_score >= 63 THEN '63-69'
                        ELSE '<63'
                    END AS score_bucket,
                    tp.pnl_sol
                FROM trading_positions tp
                JOIN calls c ON c.id = tp.call_id
                WHERE tp.is_simulation = TRUE
                  AND tp.is_strategy_b = FALSE
                  AND tp.status = 'closed'
                  AND tp.entry_time >= %s
                  AND tp.entry_time < %s
            )
            SELECT
                score_bucket,
                COUNT(*) AS trades,
                ROUND(SUM(pnl_sol)::numeric, 4) AS pnl_sol,
                ROUND(AVG(pnl_sol)::numeric, 4) AS avg_pnl_sol
            FROM tagged
            GROUP BY score_bucket
            ORDER BY CASE score_bucket
                WHEN '85+' THEN 1
                WHEN '70-84' THEN 2
                WHEN '63-69' THEN 3
                ELSE 4
            END
            """,
            (since, until),
        )
        by_score = cur.fetchall()

        cur.execute(
            """
            SELECT
                COALESCE(tp.exit_reason, 'unknown') AS exit_reason,
                COUNT(*) AS trades,
                ROUND(SUM(tp.pnl_sol)::numeric, 4) AS pnl_sol
            FROM trading_positions tp
            WHERE tp.is_simulation = TRUE
              AND tp.is_strategy_b = FALSE
              AND tp.status = 'closed'
              AND tp.entry_time >= %s
              AND tp.entry_time < %s
            GROUP BY COALESCE(tp.exit_reason, 'unknown')
            ORDER BY trades DESC, pnl_sol DESC
            """,
            (since, until),
        )
        by_exit = cur.fetchall()

    return {
        "summary": row,
        "by_channel": by_channel,
        "by_score": by_score,
        "by_exit": by_exit,
    }


def _load_call_rows(since: datetime, until: datetime, peak_col: str | None) -> list[dict]:
    conn = db.get_conn()
    signal_peak_expr = f"o.{peak_col}" if peak_col else "NULL"
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                c.id AS call_id,
                c.created_at,
                ch.handle AS channel_handle,
                COALESCE(c.vip_tier, '') AS vip_tier,
                COALESCE(c.conviction_score, 0) AS conviction_score,
                COALESCE(c.mcap_at_call, 0) AS entry_mcap,
                COALESCE(t.bundle_pct_remaining, NULL) AS bundle_pct_remaining,
                COALESCE(t.fake_vol_pct, NULL) AS fake_vol_pct,
                COALESCE(t.security_flag, '') AS security_flag,
                COALESCE(t.dev_tokens_made, NULL) AS dev_tokens_made,
                COALESCE(t.symbol, '') AS symbol,
                EXTRACT(HOUR FROM c.created_at AT TIME ZONE 'America/Los_Angeles')::int AS local_hour_pst,
                CASE
                    WHEN COALESCE(c.mcap_at_call, 0) <= 0 THEN NULL
                    ELSE GREATEST(
                        COALESCE(o.peak_multiplier, 0),
                        COALESCE(o.mcap_at_result / NULLIF(c.mcap_at_call, 0), 0),
                        COALESCE(o.mcap_1h / NULLIF(c.mcap_at_call, 0), 0),
                        COALESCE(o.mcap_4h / NULLIF(c.mcap_at_call, 0), 0),
                        COALESCE(o.mcap_24h / NULLIF(c.mcap_at_call, 0), 0)
                    )
                END AS derived_peak_mult,
                {signal_peak_expr} AS signal_peak_mult
            FROM calls c
            JOIN channels ch ON ch.id = c.channel_id
            JOIN tokens t ON t.id = c.token_id
            LEFT JOIN outcomes o ON o.call_id = c.id
            WHERE c.created_at >= %s
              AND c.created_at < %s
            ORDER BY c.created_at ASC
            """,
            (since, until),
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def _score_bucket(score: float) -> str:
    if score >= 85:
        return "85+"
    if score >= 70:
        return "70-84"
    if score >= 63:
        return "63-69"
    return "<63"


def _evaluate_replay_window(config, rows: list[dict]) -> dict:
    trade_count = 0
    trade_5x_plus = 0
    trade_2x_to_5x = 0
    trade_1x_to_2x = 0
    trade_lt_1x = 0
    trade_peak_sum = 0.0
    trade_peak_n = 0
    signal_peak_sum = 0.0
    signal_peak_n = 0
    reason_counts: Counter[str] = Counter()
    by_channel: Counter[str] = Counter()
    by_score: Counter[str] = Counter()

    for row in rows:
        ctx = StrategyCallContext(
            call_id=row["call_id"],
            strategy_name=config.strategy_name,
            channel_handle=row["channel_handle"],
            vip_tier=row["vip_tier"] or None,
            score=float(row["conviction_score"] or 0),
            local_hour_pst=int(row["local_hour_pst"]),
            entry_mcap=float(row["entry_mcap"] or 0),
            bundle_pct=float(row["bundle_pct_remaining"]) if row["bundle_pct_remaining"] is not None else None,
            fake_pct=float(row["fake_vol_pct"]) if row["fake_vol_pct"] is not None else None,
            security_flag=row["security_flag"] or None,
            dev_tokens_made=int(row["dev_tokens_made"]) if row["dev_tokens_made"] is not None else None,
            symbol=row["symbol"] or None,
        )
        decision = evaluate_strategy_a_entry(ctx, config)
        reason_counts[decision.reason] += 1
        if not decision.should_trade:
            continue

        trade_count += 1
        by_channel[row["channel_handle"]] += 1
        by_score[_score_bucket(float(row["conviction_score"] or 0))] += 1
        peak_mult = float(row["derived_peak_mult"]) if row["derived_peak_mult"] is not None else None
        sig_peak = float(row["signal_peak_mult"]) if row["signal_peak_mult"] is not None else None
        if sig_peak is not None and sig_peak > 0:
            signal_peak_sum += sig_peak
            signal_peak_n += 1
        if peak_mult is None:
            continue
        trade_peak_sum += peak_mult
        trade_peak_n += 1
        if peak_mult >= 5:
            trade_5x_plus += 1
        elif peak_mult >= 2:
            trade_2x_to_5x += 1
        elif peak_mult >= 1:
            trade_1x_to_2x += 1
        else:
            trade_lt_1x += 1

    return {
        "trade_count": trade_count,
        "trade_5x_plus": trade_5x_plus,
        "trade_2x_to_5x": trade_2x_to_5x,
        "trade_1x_to_2x": trade_1x_to_2x,
        "trade_lt_1x": trade_lt_1x,
        "avg_peak": (trade_peak_sum / trade_peak_n) if trade_peak_n else None,
        "avg_signal_peak": (signal_peak_sum / signal_peak_n) if signal_peak_n else None,
        "reason_counts": reason_counts,
        "by_channel": by_channel,
        "by_score": by_score,
    }


def _to_map(rows: list[tuple], key_index: int = 0) -> dict:
    return {row[key_index]: row[1:] for row in rows}


def _print_side_by_side_table(
    title: str,
    left_rows: list[tuple],
    right_rows: list[tuple],
    value_headers: list[str],
) -> None:
    print(title)
    print("-" * 84)

    left_map = _to_map(left_rows)
    right_map = _to_map(right_rows)
    labels = sorted(set(left_map.keys()) | set(right_map.keys()))
    if not labels:
        print("No rows.")
        print()
        return

    left_header = "A"
    right_header = "B"
    print(f"{'bucket':<24} {left_header:>28} {right_header:>28}")
    for label in labels:
        left_vals = left_map.get(label)
        right_vals = right_map.get(label)
        left_text = ", ".join(
            f"{hdr}={_fmt_num(val, 4 if 'pnl' in hdr else 1) if isinstance(val, float) else val}"
            for hdr, val in zip(value_headers, left_vals or ())
        ) or "n/a"
        right_text = ", ".join(
            f"{hdr}={_fmt_num(val, 4 if 'pnl' in hdr else 1) if isinstance(val, float) else val}"
            for hdr, val in zip(value_headers, right_vals or ())
        ) or "n/a"
        print(f"{str(label):<24} {left_text:>28} {right_text:>28}")
    print()


def _print_replay_compare_table(
    title: str,
    window_a_label: str,
    window_b_label: str,
    primary_a: dict,
    compare_a: dict | None,
    primary_b: dict,
    compare_b: dict | None,
) -> None:
    print("=" * 100)
    print(title)
    print("=" * 100)
    headers = [window_a_label, window_b_label]
    print(f"{'metric':<24} {headers[0]:>34} {headers[1]:>34}")
    print("-" * 96)

    def _cell(primary: dict, compare: dict | None) -> str:
        if compare is None:
            return (
                f"trades={primary['trade_count']}, "
                f"avg_peak={_fmt_ratio(primary['avg_peak'])}, "
                f"{_fmt_peak_bucket_counts(primary)}"
            )
        return (
            f"primary trades={primary['trade_count']}, compare={compare['trade_count']} | "
            f"primary avg_peak={_fmt_ratio(primary['avg_peak'])}, compare={_fmt_ratio(compare['avg_peak'])}"
        )

    print(f"{'topline':<24} {_cell(primary_a, compare_a):>34} {_cell(primary_b, compare_b):>34}")

    def _detail(primary: dict, compare: dict | None, key: str) -> str:
        p = primary[key]
        if compare is None:
            return str(p)
        return f"{p} -> {compare[key]}"

    for label, key in [
        ("5x+ captures", "trade_5x_plus"),
        ("2x-5x captures", "trade_2x_to_5x"),
        ("1x-2x captures", "trade_1x_to_2x"),
        ("<1x trades", "trade_lt_1x"),
    ]:
        print(
            f"{label:<24} "
            f"{_detail(primary_a, compare_a, key):>34} "
            f"{_detail(primary_b, compare_b, key):>34}"
        )
    print()

    print("Trade Counts by Channel")
    print("-" * 96)
    channel_labels = sorted(
        set(primary_a["by_channel"].keys())
        | set(primary_b["by_channel"].keys())
        | (set(compare_a["by_channel"].keys()) if compare_a else set())
        | (set(compare_b["by_channel"].keys()) if compare_b else set())
    )
    for label in channel_labels:
        left = primary_a["by_channel"].get(label, 0)
        right = primary_b["by_channel"].get(label, 0)
        left_text = str(left) if compare_a is None else f"{left} -> {compare_a['by_channel'].get(label, 0)}"
        right_text = str(right) if compare_b is None else f"{right} -> {compare_b['by_channel'].get(label, 0)}"
        print(f"{label:<24} {left_text:>34} {right_text:>34}")
    print()

    print("Trade Counts by Score Bucket")
    print("-" * 96)
    for label in ["85+", "70-84", "63-69", "<63"]:
        left = primary_a["by_score"].get(label, 0)
        right = primary_b["by_score"].get(label, 0)
        left_text = str(left) if compare_a is None else f"{left} -> {compare_a['by_score'].get(label, 0)}"
        right_text = str(right) if compare_b is None else f"{right} -> {compare_b['by_score'].get(label, 0)}"
        print(f"{label:<24} {left_text:>34} {right_text:>34}")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare Strategy A paper performance across two windows")
    parser.add_argument("--window-a-from", required=True, help="Window A start date in YYYY-MM-DD")
    parser.add_argument("--window-a-to", required=True, help="Window A end date in YYYY-MM-DD (exclusive)")
    parser.add_argument("--window-b-from", required=True, help="Window B start date in YYYY-MM-DD")
    parser.add_argument("--window-b-to", required=True, help="Window B end date in YYYY-MM-DD (exclusive)")
    parser.add_argument("--tz", default="America/Los_Angeles", help="Local timezone for date parsing")
    parser.add_argument("--replay-strategy", default=None, help="Replay a strategy/config key across both windows")
    parser.add_argument("--replay-compare-against", default=None, help="Optional compare config for replay mode")
    args = parser.parse_args()

    a_since = _parse_local_date(args.window_a_from, args.tz)
    a_until = _parse_local_date(args.window_a_to, args.tz)
    b_since = _parse_local_date(args.window_b_from, args.tz)
    b_until = _parse_local_date(args.window_b_to, args.tz)
    peak_col = _get_peak_column()

    if args.replay_strategy:
        primary_config = get_strategy_config(args.replay_strategy)
        compare_config = get_strategy_config(args.replay_compare_against) if args.replay_compare_against else None
        rows_a = _load_call_rows(a_since, a_until, peak_col)
        rows_b = _load_call_rows(b_since, b_until, peak_col)
        primary_a = _evaluate_replay_window(primary_config, rows_a)
        primary_b = _evaluate_replay_window(primary_config, rows_b)
        compare_a = _evaluate_replay_window(compare_config, rows_a) if compare_config else None
        compare_b = _evaluate_replay_window(compare_config, rows_b) if compare_config else None

        title = (
            f"Strategy A Replay Window Report — {primary_config.version}"
            if not compare_config
            else f"Strategy A Replay Window Report — {primary_config.version} vs {compare_config.version}"
        )
        _print_replay_compare_table(
            title,
            f"A {args.window_a_from}->{args.window_a_to}",
            f"B {args.window_b_from}->{args.window_b_to}",
            primary_a,
            compare_a,
            primary_b,
            compare_b,
        )
        return

    a = _query_summary(a_since, a_until, peak_col)
    b = _query_summary(b_since, b_until, peak_col)

    (
        a_trades,
        a_winners,
        a_losers,
        a_total_pnl,
        a_avg_pnl_sol,
        a_avg_pnl_pct,
        a_hard_stop_count,
        a_obs_2x,
        a_obs_3x,
        a_obs_2x_red,
        a_avg_exit_mult,
        a_avg_obs_peak,
        a_avg_sig_peak,
        a_peak_capture,
    ) = a["summary"]
    (
        b_trades,
        b_winners,
        b_losers,
        b_total_pnl,
        b_avg_pnl_sol,
        b_avg_pnl_pct,
        b_hard_stop_count,
        b_obs_2x,
        b_obs_3x,
        b_obs_2x_red,
        b_avg_exit_mult,
        b_avg_obs_peak,
        b_avg_sig_peak,
        b_peak_capture,
    ) = b["summary"]

    a_win_rate = (a_winners / a_trades * 100) if a_trades else 0.0
    b_win_rate = (b_winners / b_trades * 100) if b_trades else 0.0
    a_hard_stop_rate = (a_hard_stop_count / a_trades * 100) if a_trades else 0.0
    b_hard_stop_rate = (b_hard_stop_count / b_trades * 100) if b_trades else 0.0
    a_obs_2x_red_rate = (a_obs_2x_red / a_obs_2x * 100) if a_obs_2x else 0.0
    b_obs_2x_red_rate = (b_obs_2x_red / b_obs_2x * 100) if b_obs_2x else 0.0

    print("=" * 84)
    print("Strategy A Window Report")
    print("=" * 84)
    print(f"Window A: {args.window_a_from} -> {args.window_a_to} ({args.tz})")
    print(f"Window B: {args.window_b_from} -> {args.window_b_to} ({args.tz})")
    print()
    print(f"{'metric':<28} {'Window A':>18} {'Window B':>18}")
    print("-" * 68)
    _print_metric_row("closed trades", str(int(a_trades)), str(int(b_trades)))
    _print_metric_row("winners", str(int(a_winners)), str(int(b_winners)))
    _print_metric_row("losers", str(int(a_losers)), str(int(b_losers)))
    _print_metric_row("win rate", _fmt_pct(a_win_rate), _fmt_pct(b_win_rate))
    _print_metric_row("total pnl (SOL)", _fmt_sol(a_total_pnl), _fmt_sol(b_total_pnl))
    _print_metric_row("avg pnl / trade (SOL)", _fmt_sol(a_avg_pnl_sol), _fmt_sol(b_avg_pnl_sol))
    _print_metric_row("avg pnl / trade (%)", _fmt_pct(a_avg_pnl_pct), _fmt_pct(b_avg_pnl_pct))
    _print_metric_row("hard stop rate", _fmt_pct(a_hard_stop_rate), _fmt_pct(b_hard_stop_rate))
    _print_metric_row("observed 2x+ trades", str(int(a_obs_2x)), str(int(b_obs_2x)))
    _print_metric_row("observed 3x+ trades", str(int(a_obs_3x)), str(int(b_obs_3x)))
    _print_metric_row("2x+ closed red", str(int(a_obs_2x_red)), str(int(b_obs_2x_red)))
    _print_metric_row("2x+ closed red rate", _fmt_pct(a_obs_2x_red_rate), _fmt_pct(b_obs_2x_red_rate))
    _print_metric_row("avg exit multiple", _fmt_ratio(a_avg_exit_mult), _fmt_ratio(b_avg_exit_mult))
    _print_metric_row("avg observed peak", _fmt_ratio(a_avg_obs_peak), _fmt_ratio(b_avg_obs_peak))
    _print_metric_row("avg signal peak", _fmt_ratio(a_avg_sig_peak), _fmt_ratio(b_avg_sig_peak))
    _print_metric_row("avg peak capture ratio", _fmt_num(a_peak_capture, 2), _fmt_num(b_peak_capture, 2))
    print()

    _print_side_by_side_table(
        "Closed Trades by Channel",
        a["by_channel"],
        b["by_channel"],
        ["trades", "pnl_sol", "avg_pnl_sol", "avg_pnl_pct"],
    )
    _print_side_by_side_table(
        "Closed Trades by Score Bucket",
        a["by_score"],
        b["by_score"],
        ["trades", "pnl_sol", "avg_pnl_sol"],
    )
    _print_side_by_side_table(
        "Exit Breakdown",
        a["by_exit"],
        b["by_exit"],
        ["trades", "pnl_sol"],
    )


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
