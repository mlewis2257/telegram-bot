"""
replay_strategy.py — Replay stored calls against a versioned strategy config.

Initial goal:
  - evaluate deterministic entry decisions without touching live code
  - summarize trade/skip counts by reason
  - compare replayed decision to stored skip_reason on historical calls

Usage:
    python3 replay_strategy.py --strategy a --days 7
    python3 replay_strategy.py --strategy a --date-from 2026-05-15 --date-to 2026-05-22
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))

import db
from strategy_config import get_strategy_config
from strategy_engine import (
    StrategyCallContext,
    classify_mcap_bucket,
    evaluate_strategy_a_entry,
    evaluate_strategy_b_entry,
)


def _parse_dt(value: str) -> datetime:
    fmts = ("%Y-%m-%d", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M")
    for fmt in fmts:
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    raise ValueError(f"Unsupported datetime format: {value}")


def _load_calls(strategy: str, since: datetime | None, until: datetime | None) -> list[dict]:
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

    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                c.id AS call_id,
                c.created_at,
                ch.handle AS channel_handle,
                COALESCE(c.vip_tier, '') AS vip_tier,
                COALESCE(c.conviction_score, 0) AS conviction_score,
                COALESCE(c.skip_reason, '') AS skip_reason,
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
                tp.status AS paper_status,
                tp.exit_reason AS paper_exit_reason,
                tp.pnl_sol AS paper_pnl_sol
            FROM calls c
            JOIN channels ch ON ch.id = c.channel_id
            JOIN tokens t ON t.id = c.token_id
            LEFT JOIN outcomes o ON o.call_id = c.id
            LEFT JOIN LATERAL (
                SELECT
                    tp1.status,
                    tp1.exit_reason,
                    tp1.pnl_sol
                FROM trading_positions tp1
                WHERE tp1.call_id = c.id
                  AND tp1.is_simulation = TRUE
                  AND tp1.is_strategy_b = %s
                ORDER BY tp1.entry_time DESC NULLS LAST, tp1.id DESC
                LIMIT 1
            ) tp ON TRUE
            {where_sql}
            ORDER BY c.created_at ASC
            """,
            [strategy.strip().lower().startswith("b"), *params],
        )
        rows = cur.fetchall()
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in rows]


def _peak_bucket(peak_mult: float | None) -> str:
    if peak_mult is None or peak_mult <= 0:
        return "unknown"
    if peak_mult >= 5:
        return "5x+"
    if peak_mult >= 2:
        return "2x-5x"
    if peak_mult >= 1:
        return "1x-2x"
    return "<1x"


def _evaluate(strategy: str, config, ctx: StrategyCallContext):
    if strategy.startswith("a"):
        return evaluate_strategy_a_entry(ctx, config)
    if strategy.startswith("b"):
        return evaluate_strategy_b_entry(ctx, config)
    raise NotImplementedError(f"Strategy {strategy!r} replay is not implemented yet")


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay calls against versioned strategy config")
    parser.add_argument(
        "--strategy",
        default="a",
        help="Strategy/config key: a, a_relaxed_free, a_matrix, a_no_h14, a_no_weak, a_no_h14_no_weak, a_vip_tight_b, a_vip_soft, b",
    )
    parser.add_argument("--days", type=int, default=None)
    parser.add_argument("--date-from", dest="date_from", default=None)
    parser.add_argument("--date-to", dest="date_to", default=None)
    parser.add_argument("--compare-against", dest="compare_against", default=None)
    parser.add_argument("--limit", type=int, default=20, help="Show top N mismatches")
    args = parser.parse_args()

    since = None
    until = None
    if args.days:
        since = datetime.now(timezone.utc) - timedelta(days=args.days)
    if args.date_from:
        since = _parse_dt(args.date_from)
    if args.date_to:
        until = _parse_dt(args.date_to)

    config = get_strategy_config(args.strategy)
    compare_config = get_strategy_config(args.compare_against) if args.compare_against else None
    calls = _load_calls(args.strategy, since, until)

    replay_counts: Counter[str] = Counter()
    stored_counts: Counter[str] = Counter()
    replay_by_channel: dict[str, Counter[str]] = defaultdict(Counter)
    replay_by_hour: dict[int, Counter[str]] = defaultdict(Counter)
    replay_by_hour_bucket: dict[tuple[int, str], Counter[str]] = defaultdict(Counter)
    replay_outcome_counts: Counter[str] = Counter()
    replay_trade_outcome_counts: Counter[str] = Counter()
    replay_skip_outcome_counts: Counter[str] = Counter()
    replay_trade_peak_sum = 0.0
    replay_trade_peak_n = 0
    replay_skip_peak_sum = 0.0
    replay_skip_peak_n = 0
    replay_live_trade_count = 0
    replay_live_closed_count = 0
    replay_live_pnl_sum = 0.0
    mismatch_pairs: Counter[tuple[str, str]] = Counter()
    mismatches: list[dict] = []
    compare_counts: Counter[str] = Counter()
    compare_changed_pairs: Counter[tuple[str, str]] = Counter()
    compare_changed_rows: list[dict] = []
    compare_trade_outcome_counts: Counter[str] = Counter()
    compare_trade_peak_sum = 0.0
    compare_trade_peak_n = 0
    compare_trade_count = 0
    compare_changed_by_channel: dict[str, Counter[tuple[str, str]]] = defaultdict(Counter)
    compare_changed_by_hour: dict[int, Counter[tuple[str, str]]] = defaultdict(Counter)
    compare_changed_by_hour_bucket: dict[tuple[int, str], Counter[tuple[str, str]]] = defaultdict(Counter)

    for row in calls:
        stored_reason = row["skip_reason"] or "none"
        stored_counts[stored_reason] += 1

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

        decision = _evaluate(args.strategy, config, ctx)
        compare_decision = _evaluate(args.compare_against, compare_config, ctx) if compare_config else None

        replay_counts[decision.reason] += 1
        replay_by_channel[row["channel_handle"]][decision.reason] += 1
        replay_by_hour[int(row["local_hour_pst"])][decision.reason] += 1
        bucket = classify_mcap_bucket(float(row["entry_mcap"] or 0)) or "unknown"
        peak_mult = float(row["derived_peak_mult"]) if row["derived_peak_mult"] is not None else None
        outcome_bucket = _peak_bucket(peak_mult)
        replay_outcome_counts[outcome_bucket] += 1
        replay_by_hour_bucket[(int(row["local_hour_pst"]), bucket)][decision.reason] += 1
        replay_trade_state = "trade" if decision.should_trade else decision.reason
        stored_trade_state = "trade" if stored_reason in ("", "none") else stored_reason
        if decision.should_trade:
            replay_trade_outcome_counts[outcome_bucket] += 1
            if peak_mult is not None:
                replay_trade_peak_sum += peak_mult
                replay_trade_peak_n += 1
            if row["paper_status"]:
                replay_live_trade_count += 1
                if row["paper_status"] == "closed":
                    replay_live_closed_count += 1
                    if row["paper_pnl_sol"] is not None:
                        replay_live_pnl_sum += float(row["paper_pnl_sol"])
        else:
            replay_skip_outcome_counts[outcome_bucket] += 1
            if peak_mult is not None:
                replay_skip_peak_sum += peak_mult
                replay_skip_peak_n += 1
        if replay_trade_state != stored_trade_state:
            mismatch_pairs[(stored_trade_state, replay_trade_state)] += 1
            mismatches.append(
                {
                    "call_id": row["call_id"],
                    "symbol": row["symbol"],
                    "channel": row["channel_handle"],
                    "hour": row["local_hour_pst"],
                    "entry_mcap": row["entry_mcap"],
                    "bucket": bucket,
                    "peak_mult": peak_mult,
                    "score": row["conviction_score"],
                    "stored": stored_trade_state,
                    "replay": replay_trade_state,
                }
            )
        if compare_decision:
            compare_state = "trade" if compare_decision.should_trade else compare_decision.reason
            compare_counts[compare_decision.reason] += 1
            if compare_decision.should_trade:
                compare_trade_count += 1
                compare_trade_outcome_counts[outcome_bucket] += 1
                if peak_mult is not None:
                    compare_trade_peak_sum += peak_mult
                    compare_trade_peak_n += 1
            if compare_state != replay_trade_state:
                compare_changed_pairs[(replay_trade_state, compare_state)] += 1
                compare_changed_by_channel[row["channel_handle"]][(replay_trade_state, compare_state)] += 1
                compare_changed_by_hour[int(row["local_hour_pst"])][(replay_trade_state, compare_state)] += 1
                compare_changed_by_hour_bucket[(int(row["local_hour_pst"]), bucket)][(replay_trade_state, compare_state)] += 1
                compare_changed_rows.append(
                    {
                        "call_id": row["call_id"],
                        "symbol": row["symbol"],
                        "channel": row["channel_handle"],
                        "hour": row["local_hour_pst"],
                        "entry_mcap": row["entry_mcap"],
                        "bucket": bucket,
                        "peak_mult": peak_mult,
                        "score": row["conviction_score"],
                        "primary": replay_trade_state,
                        "compare": compare_state,
                    }
                )

    print("=" * 72)
    print(f"Replay Strategy {config.strategy_name}  version={config.version}")
    print(f"calls={len(calls)}")
    print("=" * 72)
    print()
    print("Replay Decision Counts")
    print("-" * 72)
    for reason, count in replay_counts.most_common():
        print(f"{reason:<28} {count:>8}")
    if compare_config:
        primary_2x = replay_trade_outcome_counts.get("2x-5x", 0) + replay_trade_outcome_counts.get("5x+", 0)
        compare_2x = compare_trade_outcome_counts.get("2x-5x", 0) + compare_trade_outcome_counts.get("5x+", 0)
        primary_5x = replay_trade_outcome_counts.get("5x+", 0)
        compare_5x = compare_trade_outcome_counts.get("5x+", 0)
        primary_avg_peak = (replay_trade_peak_sum / replay_trade_peak_n) if replay_trade_peak_n else 0.0
        compare_avg_peak = (compare_trade_peak_sum / compare_trade_peak_n) if compare_trade_peak_n else 0.0
        print()
        print(f"Compare Scoreboard ({config.version} vs {compare_config.version})")
        print("-" * 72)
        print(f"{'metric':<24} {'primary':>10} {'compare':>10}")
        print(f"{'trades':<24} {sum(replay_trade_outcome_counts.values()):>10} {compare_trade_count:>10}")
        print(f"{'2x+ captures':<24} {primary_2x:>10} {compare_2x:>10}")
        print(f"{'5x+ captures':<24} {primary_5x:>10} {compare_5x:>10}")
        print(f"{'avg peak on trades':<24} {primary_avg_peak:>10.2f} {compare_avg_peak:>10.2f}")
        print()
        print(f"Compare Decision Counts ({compare_config.version})")
        print("-" * 72)
        for reason, count in compare_counts.most_common():
            print(f"{reason:<28} {count:>8}")
        print()
        print(f"Compare Trade Outcome Summary ({compare_config.version})")
        print("-" * 72)
        print(f"trades{'':<24} {sum(compare_trade_outcome_counts.values()):>8}")
        for bucket_name in ("5x+", "2x-5x", "1x-2x", "<1x", "unknown"):
            count = compare_trade_outcome_counts.get(bucket_name, 0)
            if count:
                print(f"trade_{bucket_name:<22} {count:>8}")
        if compare_trade_peak_n:
            print(f"trade_avg_peak{'':<15} {compare_trade_peak_sum / compare_trade_peak_n:>8.2f}")
    print()
    print("Stored Skip Counts")
    print("-" * 72)
    for reason, count in stored_counts.most_common():
        print(f"{reason:<28} {count:>8}")
    print()
    print("Replay Outcome Coverage")
    print("-" * 72)
    for bucket_name in ("5x+", "2x-5x", "1x-2x", "<1x", "unknown"):
        count = replay_outcome_counts.get(bucket_name, 0)
        if count:
            print(f"{bucket_name:<28} {count:>8}")
    print()
    print("Replay Trade Outcome Summary")
    print("-" * 72)
    print(f"trades{'':<24} {sum(replay_trade_outcome_counts.values()):>8}")
    for bucket_name in ("5x+", "2x-5x", "1x-2x", "<1x", "unknown"):
        count = replay_trade_outcome_counts.get(bucket_name, 0)
        if count:
            print(f"trade_{bucket_name:<22} {count:>8}")
    if replay_trade_peak_n:
        print(f"trade_avg_peak{'':<15} {replay_trade_peak_sum / replay_trade_peak_n:>8.2f}")
    print()
    print("Replay Skip Outcome Summary")
    print("-" * 72)
    print(f"skips{'':<25} {sum(replay_skip_outcome_counts.values()):>8}")
    for bucket_name in ("5x+", "2x-5x", "1x-2x", "<1x", "unknown"):
        count = replay_skip_outcome_counts.get(bucket_name, 0)
        if count:
            print(f"skip_{bucket_name:<23} {count:>8}")
    if replay_skip_peak_n:
        print(f"skip_avg_peak{'':<16} {replay_skip_peak_sum / replay_skip_peak_n:>8.2f}")
    print()
    print("Replay vs Stored Paper Outcome")
    print("-" * 72)
    print(f"replay_trade_calls{'':<11} {sum(replay_trade_outcome_counts.values()):>8}")
    print(f"stored_paper_rows{'':<12} {replay_live_trade_count:>8}")
    print(f"stored_closed_rows{'':<11} {replay_live_closed_count:>8}")
    print(f"stored_closed_pnl_sol{'':<7} {replay_live_pnl_sum:>8.4f}")
    print()
    print("Replay by Channel")
    print("-" * 72)
    for channel, counts in sorted(replay_by_channel.items()):
        parts = ", ".join(f"{reason}={count}" for reason, count in counts.most_common())
        print(f"{channel:<24} {parts}")
    print()
    print("Replay by Hour")
    print("-" * 72)
    for hour in sorted(replay_by_hour):
        counts = replay_by_hour[hour]
        trades = counts.get("trade", 0)
        total = sum(counts.values())
        parts = ", ".join(f"{reason}={count}" for reason, count in counts.most_common())
        print(f"{hour:02d}:00  trades={trades}/{total}  {parts}")
    print()
    print("Replay by Hour + Mcap Bucket")
    print("-" * 72)
    for (hour, bucket), counts in sorted(replay_by_hour_bucket.items()):
        total = sum(counts.values())
        if total < 3:
            continue
        trades = counts.get("trade", 0)
        parts = ", ".join(f"{reason}={count}" for reason, count in counts.most_common())
        print(f"{hour:02d}:00  {bucket:<10} trades={trades}/{total}  {parts}")
    print()
    print("Mismatch Pairs")
    print("-" * 72)
    if mismatch_pairs:
        for (stored, replay), count in mismatch_pairs.most_common():
            print(f"stored={stored:<24} replay={replay:<24} count={count}")
    else:
        print("No mismatches.")
    print()
    print(f"Mismatches (showing up to {args.limit})")
    print("-" * 72)
    for row in mismatches[: args.limit]:
        print(
            f"call_id={row['call_id']} symbol={row['symbol'] or '?'} channel={row['channel']} "
            f"hour={row['hour']:02d} bucket={row['bucket']} mcap={float(row['entry_mcap'] or 0):.0f} score={float(row['score'] or 0):.1f} "
            f"peak={float(row['peak_mult'] or 0):.2f} stored={row['stored']} replay={row['replay']}"
        )
    if not mismatches:
        print("No mismatches.")
    if compare_config:
        print()
        print(f"Primary vs Compare Changes ({config.version} -> {compare_config.version})")
        print("-" * 72)
        if compare_changed_pairs:
            for (primary, compare), count in compare_changed_pairs.most_common():
                print(f"primary={primary:<23} compare={compare:<23} count={count}")
        else:
            print("No primary/compare changes.")
        print()
        print("Primary vs Compare Changes by Channel")
        print("-" * 72)
        if compare_changed_by_channel:
            for channel, counts in sorted(compare_changed_by_channel.items()):
                parts = ", ".join(
                    f"{primary}->{compare}={count}"
                    for (primary, compare), count in counts.most_common()
                )
                print(f"{channel:<24} {parts}")
        else:
            print("No channel-level changes.")
        print()
        print("Primary vs Compare Changes by Hour")
        print("-" * 72)
        if compare_changed_by_hour:
            for hour in sorted(compare_changed_by_hour):
                counts = compare_changed_by_hour[hour]
                parts = ", ".join(
                    f"{primary}->{compare}={count}"
                    for (primary, compare), count in counts.most_common()
                )
                print(f"{hour:02d}:00  {parts}")
        else:
            print("No hour-level changes.")
        print()
        print("Primary vs Compare Changes by Hour + Mcap Bucket")
        print("-" * 72)
        if compare_changed_by_hour_bucket:
            for (hour, bucket), counts in sorted(compare_changed_by_hour_bucket.items()):
                total = sum(counts.values())
                if total < 2:
                    continue
                parts = ", ".join(
                    f"{primary}->{compare}={count}"
                    for (primary, compare), count in counts.most_common()
                )
                print(f"{hour:02d}:00  {bucket:<10} {parts}")
        else:
            print("No hour+bucket-level changes.")
        print()
        print(f"Primary vs Compare Changed Calls (showing up to {args.limit})")
        print("-" * 72)
        for row in compare_changed_rows[: args.limit]:
            print(
                f"call_id={row['call_id']} symbol={row['symbol'] or '?'} channel={row['channel']} "
                f"hour={row['hour']:02d} bucket={row['bucket']} mcap={float(row['entry_mcap'] or 0):.0f} score={float(row['score'] or 0):.1f} "
                f"peak={float(row['peak_mult'] or 0):.2f} primary={row['primary']} compare={row['compare']}"
            )
        if not compare_changed_rows:
            print("No primary/compare changes.")


if __name__ == "__main__":
    main()
