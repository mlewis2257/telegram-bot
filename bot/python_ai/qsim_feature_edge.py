"""
qsim_feature_edge.py — entry filters against real qsim quote outcomes.

This is the upstream counterpart to qsim_quote_capture_replay.py. It asks:

    "Which entry-time token/call features separate real quote runners from
     dead-on-arrival qsim trades in this exact lane?"

Labels use max_qobs_mult from Jupiter sell quotes, not shadow feed peaks:
    runner: max_qobs_mult >= --runner
    loser:  max_qobs_mult <  --loser

The report is intentionally simple: for each numeric feature it finds the best
single-threshold filter and shows runner keep-rate, loser drop-rate, and kept
PnL under current qsim. Use this to discover candidate entry filters, not to
ship a rule blindly.

Example:
    python3 qsim_feature_edge.py --days 7 --channel solwhaletrending --lane low_score --variant early --min-entry-ratio 0.5 --max-entry-ratio 2
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from dataclasses import dataclass
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))


MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5
MAX_QOBS_MULT = 50.0

FEATURES = [
    ("score", "score", None),
    ("entry_mcap", "entry_mcap", None),
    ("entry_ratio", "q/feed_entry", None),
    ("liquidity", "liquidity", None),
    ("vol_1h", "vol_1h", None),
    ("turnover", "vol/liq", None),
    ("vol_to_mcap", "vol/mcap", None),
    ("age_min", "age_min", None),
    ("holder_count", "holders", None),
    ("hodl_count", "hodlers", None),
    ("top10_pct", "top10_pct", None),
    ("first20_pct", "first20_pct", None),
    ("detector_sol", "detector_sol", None),
    ("detecting_sol_spent", "spent_sol", None),
    ("dev_best_mcap", "dev_best_mc", None),
    ("dev_pct_held", "dev_held_pct", None),
    ("dev_sold_pct", "dev_sold_pct", None),
    ("dev_tokens_made", "dev_tokens", None),
    ("bundle_pct", "bundle_pct", -1),
    ("fake_vol_pct", "fake_vol_pct", -1),
    ("sniper_pct", "sniper_pct", -1),
    ("bundle_count", "bundle_cnt", None),
    ("sniper_count", "sniper_cnt", None),
]

FEATURE_ALIASES = {
    feature: feature
    for feature, _, _ in FEATURES
}
FEATURE_ALIASES.update({
    label.replace("/", "_"): feature
    for feature, label, _ in FEATURES
})
FEATURE_ALIASES.update({
    "vol_mcap": "vol_to_mcap",
    "vol_to_mcap": "vol_to_mcap",
    "vol_liq": "turnover",
    "turnover": "turnover",
    "holders": "holder_count",
    "hodlers": "hodl_count",
})


SQL = """
WITH base AS (
    SELECT
        q.call_id,
        tok.symbol,
        COALESCE(ch.handle, '?') AS channel,
        COALESCE(c.skip_reason, 'none') AS lane,
        q.variant,
        q.entry_time,
        q.entry_price AS entry_mcap,
        q.sol_in AS qsim_sol_in,
        q.pnl_sol AS qsim_pnl,
        q.pnl_pct AS qsim_pnl_pct,
        q.peak_multiplier AS qsim_peak,
        q.exit_reason AS qsim_reason,
        c.mcap_at_call AS feed_entry_mcap,
        c.conviction_score AS score,
        tok.liq_at_detection AS liquidity,
        tok.vol_1h_at_detection AS vol_1h,
        tok.vol_1h_at_detection / NULLIF(tok.liq_at_detection, 0) AS turnover,
        tok.vol_1h_at_detection / NULLIF(q.entry_price, 0) AS vol_to_mcap,
        tok.token_age_minutes AS age_min,
        tok.holder_count,
        tok.hodl_count,
        tok.top_10_holder_pct AS top10_pct,
        tok.first_20_pct AS first20_pct,
        tok.detecting_wallet_sol AS detector_sol,
        tok.detecting_sol_spent,
        tok.dev_best_mcap,
        tok.dev_pct_held,
        tok.dev_sold_pct,
        tok.dev_tokens_made,
        tok.bundle_pct_remaining AS bundle_pct,
        tok.fake_vol_pct,
        tok.sniper_pct_remaining AS sniper_pct,
        tok.bundle_count,
        tok.sniper_count,
        q.entry_price / NULLIF(c.mcap_at_call, 0) AS entry_ratio,
        qobs.qobs_count,
        qobs.max_qobs_mult
    FROM qsim_positions q
    JOIN calls c ON c.id = q.call_id
    JOIN tokens tok ON tok.id = q.token_id
    LEFT JOIN channels ch ON ch.id = c.channel_id
    LEFT JOIN LATERAL (
        SELECT COUNT(*) AS qobs_count, MAX(qo.real_mult) AS max_qobs_mult
        FROM qsim_quote_observations qo
        WHERE qo.call_id = q.call_id
          AND qo.real_mult > 0
          AND qo.real_mult <= %(max_qmax)s
          AND qo.observed_at BETWEEN q.entry_time AND COALESCE(q.exit_time, now())
    ) qobs ON TRUE
    WHERE q.status = 'closed'
      AND q.entry_time >= now() - (%(days)s || ' days')::interval
      AND (%(since)s IS NULL OR q.entry_time >= %(since)s::timestamptz)
      AND (%(channel)s = 'any' OR COALESCE(ch.handle, '?') = %(channel)s)
      AND (%(lane)s = 'any' OR COALESCE(c.skip_reason, 'none') = %(lane)s)
      AND (%(variant)s = 'any' OR q.variant = %(variant)s)
      AND qobs.qobs_count > 0
      AND (%(min_entry_ratio)s IS NULL OR q.entry_price / NULLIF(c.mcap_at_call, 0) >= %(min_entry_ratio)s)
      AND (%(max_entry_ratio)s IS NULL OR q.entry_price / NULLIF(c.mcap_at_call, 0) <= %(max_entry_ratio)s)
      AND (%(raw)s OR NOT (
          COALESCE(q.peak_multiplier, 0) > %(max_peak)s
       OR COALESCE(q.pnl_pct, 0) > %(max_pnl)s
       OR COALESCE(q.pnl_pct, 0) < %(min_pnl)s
      ))
)
SELECT * FROM base
"""


@dataclass
class Stump:
    feature: str
    label: str
    direction: str
    threshold: float
    runner_keep: float
    loser_drop: float
    sep: float
    precision: float
    kept: int
    kept_pnl: float
    kept_avg: float
    dropped_pnl: float
    runner_median: float | None
    loser_median: float | None


def _f(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _ratio(num: Any, den: Any) -> float:
    den_f = _f(den)
    if den_f <= 0:
        return 0.0
    return _f(num) / den_f


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    values = sorted(values)
    return values[len(values) // 2]


def _keep(value: float, direction: str, threshold: float) -> bool:
    return value >= threshold if direction == ">=" else value <= threshold


def _parse_where(where: str | None) -> tuple[str, str, float] | None:
    if not where:
        return None
    match = re.fullmatch(r"\s*([A-Za-z0-9_/-]+)\s*(<=|>=|<|>)\s*(-?\d+(?:\.\d+)?)\s*", where)
    if not match:
        raise SystemExit("Invalid --where. Use a known numeric feature like vol_mcap>=1.331")
    raw_feature, op, raw_threshold = match.groups()
    feature_key = raw_feature.replace("/", "_")
    feature = FEATURE_ALIASES.get(feature_key)
    if feature is None:
        known = ", ".join(sorted(FEATURE_ALIASES))
        raise SystemExit(f"Unknown --where feature '{raw_feature}'. Known: {known}")
    return feature, op, float(raw_threshold)


def _passes_where(row: dict[str, Any], where: tuple[str, str, float] | None) -> bool:
    if where is None:
        return True
    feature, op, threshold = where
    value = row.get(feature)
    if value is None:
        return False
    value_f = _f(value)
    if op == ">=":
        return value_f >= threshold
    if op == ">":
        return value_f > threshold
    if op == "<=":
        return value_f <= threshold
    return value_f < threshold


def _best_stump(rows: list[dict[str, Any]], feature: str, label: str,
                sentinel: float | None) -> Stump | None:
    pairs: list[tuple[float, bool, float]] = []
    runner_values: list[float] = []
    loser_values: list[float] = []
    for row in rows:
        value = row.get(feature)
        if value is None:
            continue
        value_f = _f(value)
        if sentinel is not None and value_f == sentinel:
            continue
        is_runner = bool(row["_is_runner"])
        pnl = row["_return"]
        pairs.append((value_f, is_runner, pnl))
        (runner_values if is_runner else loser_values).append(value_f)

    runners = [p for p in pairs if p[1]]
    losers = [p for p in pairs if not p[1]]
    if len(runners) < 3 or len(losers) < 10:
        return None

    best: Stump | None = None
    for threshold in sorted({p[0] for p in pairs}):
        for direction in (">=", "<="):
            kept_rows = [p for p in pairs if _keep(p[0], direction, threshold)]
            if len(kept_rows) < 5:
                continue
            kept_runners = sum(1 for p in kept_rows if p[1])
            kept_losers = sum(1 for p in kept_rows if not p[1])
            runner_keep = kept_runners / len(runners)
            loser_keep = kept_losers / len(losers)
            loser_drop = 1.0 - loser_keep
            sep = runner_keep + loser_drop - 1.0
            precision = kept_runners / len(kept_rows)
            kept_pnl = sum(p[2] for p in kept_rows)
            dropped_pnl = sum(p[2] for p in pairs if not _keep(p[0], direction, threshold))
            stump = Stump(
                feature=feature,
                label=label,
                direction=direction,
                threshold=threshold,
                runner_keep=runner_keep,
                loser_drop=loser_drop,
                sep=sep,
                precision=precision,
                kept=len(kept_rows),
                kept_pnl=kept_pnl,
                kept_avg=kept_pnl / len(kept_rows),
                dropped_pnl=dropped_pnl,
                runner_median=_median(runner_values),
                loser_median=_median(loser_values),
            )
            if best is None or (stump.kept_avg, stump.sep, stump.kept_pnl) > (
                best.kept_avg, best.sep, best.kept_pnl
            ):
                best = stump
    return best


def _rows(params: dict[str, Any]) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor
    import db

    db.ensure_qsim_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SQL, params)
        return [dict(row) for row in cur.fetchall()]


def _fmt(value: float | None) -> str:
    if value is None:
        return "     n/a"
    return f"{value:>8.3g}"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Find qsim entry features separating quote runners from dead trades."
    )
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--since", default=None, help="only include qsim entries at/after this timestamp")
    parser.add_argument("--channel", default="any")
    parser.add_argument("--lane", default="any")
    parser.add_argument("--variant", default="early")
    parser.add_argument("--runner", type=float, default=1.5)
    parser.add_argument("--loser", type=float, default=1.1)
    parser.add_argument("--min-entry-ratio", type=float, default=None)
    parser.add_argument("--max-entry-ratio", type=float, default=None)
    parser.add_argument(
        "--where",
        default=None,
        help="pre-filter rows with one numeric condition, e.g. vol_mcap>=1.331",
    )
    parser.add_argument(
        "--max-qmax",
        type=float,
        default=MAX_QOBS_MULT,
        help="ignore quote observations above this multiple as quote artifacts",
    )
    parser.add_argument("--raw", action="store_true")
    args = parser.parse_args()
    where = _parse_where(args.where)

    params = {
        "days": args.days,
        "since": args.since,
        "channel": args.channel,
        "lane": args.lane,
        "variant": args.variant,
        "min_entry_ratio": args.min_entry_ratio,
        "max_entry_ratio": args.max_entry_ratio,
        "raw": args.raw,
        "max_peak": MAX_SANE_PEAK,
        "max_pnl": MAX_SANE_PNL_PCT,
        "min_pnl": MIN_SANE_PNL_PCT,
        "max_qmax": args.max_qmax,
    }
    rows = _rows(params)
    labeled = []
    for row in rows:
        if not _passes_where(row, where):
            continue
        qmax = _f(row.get("max_qobs_mult"))
        if qmax >= args.runner:
            row["_is_runner"] = True
        elif qmax < args.loser:
            row["_is_runner"] = False
        else:
            continue
        row["_return"] = _ratio(row.get("qsim_pnl"), row.get("qsim_sol_in"))
        labeled.append(row)

    runners = [row for row in labeled if row["_is_runner"]]
    losers = [row for row in labeled if not row["_is_runner"]]
    total_return = sum(row["_return"] for row in labeled)
    print(
        f"\nQSIM FEATURE EDGE — days={args.days} since={args.since} channel={args.channel} "
        f"lane={args.lane} variant={args.variant} max_qmax={args.max_qmax}"
    )
    print(
        f"labels: runner=qmax>={args.runner}x loser=qmax<{args.loser}x "
        f"rows={len(labeled)} runners={len(runners)} losers={len(losers)} "
        f"baseline_pnl={total_return:+.2f} where={args.where or 'none'}\n"
    )
    if len(runners) < 3 or len(losers) < 10:
        print("Not enough labeled rows yet. Let qsim quote-path accumulate more.")
        return

    stumps = []
    for feature, label, sentinel in FEATURES:
        stump = _best_stump(labeled, feature, label, sentinel)
        if stump is not None:
            stumps.append(stump)

    hdr = (
        f"{'feature':<14} {'run_med':>8} {'lose_med':>8} {'filter':<14} "
        f"{'keepR':>6} {'dropL':>6} {'prec':>6} {'kept':>5} "
        f"{'kept_pnl':>9} {'avg':>7} {'drop_pnl':>9}"
    )
    print(hdr)
    print("-" * len(hdr))
    for stump in sorted(stumps, key=lambda s: (s.kept_avg, s.kept_pnl), reverse=True):
        filt = f"{stump.direction} {stump.threshold:.4g}"
        print(
            f"{stump.label:<14} {_fmt(stump.runner_median)} {_fmt(stump.loser_median)} "
            f"{filt:<14} {100 * stump.runner_keep:>5.0f}% {100 * stump.loser_drop:>5.0f}% "
            f"{100 * stump.precision:>5.0f}% {stump.kept:>5} "
            f"{stump.kept_pnl:>+9.2f} {stump.kept_avg:>+7.2f} {stump.dropped_pnl:>+9.2f}"
        )

    print("\nRead: prioritize positive kept_pnl/avg with decent kept count; then validate forward.")


if __name__ == "__main__":
    main()
