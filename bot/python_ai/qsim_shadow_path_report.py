"""
qsim_shadow_path_report.py — explain same-call qsim vs shadow disagreements.

This is a read-only diagnostic for the exact uncomfortable question:

    "Did qsim miss a real opportunity, did shadow price a fake one, or are we
     comparing two different entry/exit rulers?"

It matches closed qsim rows to the same call's closed shadow row for the same exit
variant, normalizes PnL per 1 SOL deployed, and joins any websocket market
observations recorded during the shared hold window. The report is intentionally
not a trade recommender; it is a disagreement classifier for building smarter live
in-trade rules.

Examples:
    python3 qsim_shadow_path_report.py --days 30 --channel solwhaletrending --lane low_score --variant early
    python3 qsim_shadow_path_report.py --days 30 --channel solwhaletrending --lane none --variant early --detail
    python3 qsim_shadow_path_report.py --days 30 --reason-pair hard_stop/profit_floor --limit 100
    python3 qsim_shadow_path_report.py --days 30 --require-ws --detail
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass

sys.path.insert(0, os.path.dirname(__file__))


MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5


SQL = """
WITH matched AS (
    SELECT
        q.call_id,
        t.symbol,
        COALESCE(ch.handle, '?') AS channel,
        COALESCE(c.skip_reason, 'none') AS lane,
        COALESCE(q.vip_tier, sp.vip_tier, 'none') AS vip_tier,
        q.variant,
        c.mcap_at_call,
        sp.entry_time AS shadow_entry_time,
        sp.exit_time AS shadow_exit_time,
        sp.entry_price AS shadow_entry,
        sp.exit_price AS shadow_exit,
        sp.peak_multiplier AS shadow_peak,
        sp.sol_in AS shadow_sol_in,
        sp.pnl_sol AS shadow_pnl,
        sp.pnl_pct AS shadow_pnl_pct,
        sp.exit_reason AS shadow_reason,
        q.entry_time AS qsim_entry_time,
        q.exit_time AS qsim_exit_time,
        q.entry_price AS qsim_entry,
        q.exit_price AS qsim_exit,
        q.peak_multiplier AS qsim_peak,
        q.sol_in AS qsim_sol_in,
        q.pnl_sol AS qsim_pnl,
        q.pnl_pct AS qsim_pnl_pct,
        q.exit_reason AS qsim_reason
    FROM qsim_positions q
    JOIN shadow_positions sp
      ON sp.call_id = q.call_id
     AND sp.exit_variant = q.variant
    JOIN calls c ON c.id = q.call_id
    JOIN tokens t ON t.id = q.token_id
    LEFT JOIN channels ch ON ch.id = c.channel_id
    WHERE q.status = 'closed'
      AND sp.status = 'closed'
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
    m.*,
    obs.obs_count,
    obs.first_obs_at,
    obs.last_obs_at,
    obs.min_ws_mcap,
    obs.max_ws_mcap,
    obs.max_ws_before_qsim_exit,
    obs.max_ws_after_qsim_before_shadow,
    CASE WHEN m.shadow_entry > 0 THEN obs.max_ws_mcap / m.shadow_entry ELSE NULL END AS max_ws_mult,
    CASE WHEN m.shadow_entry > 0 THEN obs.max_ws_before_qsim_exit / m.shadow_entry ELSE NULL END AS max_ws_before_qsim_mult,
    CASE WHEN m.shadow_entry > 0 THEN obs.max_ws_after_qsim_before_shadow / m.shadow_entry ELSE NULL END AS max_ws_after_qsim_mult,
    qobs.qobs_count,
    qobs.qobs_exit_signals,
    qobs.qobs_no_routes,
    qobs.max_qobs_mult,
    qobs.last_qobs_mult,
    qobs.max_qobs_before_shadow_exit
FROM matched m
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) AS obs_count,
        MIN(o.observed_at) AS first_obs_at,
        MAX(o.observed_at) AS last_obs_at,
        MIN(o.mcap) AS min_ws_mcap,
        MAX(o.mcap) AS max_ws_mcap,
        MAX(o.mcap) FILTER (
            WHERE m.qsim_exit_time IS NOT NULL
              AND o.observed_at <= m.qsim_exit_time
        ) AS max_ws_before_qsim_exit,
        MAX(o.mcap) FILTER (
            WHERE m.qsim_exit_time IS NOT NULL
              AND m.shadow_exit_time IS NOT NULL
              AND m.shadow_exit_time > m.qsim_exit_time
              AND o.observed_at > m.qsim_exit_time
              AND o.observed_at <= m.shadow_exit_time
        ) AS max_ws_after_qsim_before_shadow
    FROM ws_market_observations o
    WHERE o.call_id = m.call_id
      AND o.mcap IS NOT NULL
      AND o.observed_at BETWEEN LEAST(m.shadow_entry_time, m.qsim_entry_time)
                            AND GREATEST(m.shadow_exit_time, m.qsim_exit_time)
) obs ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) AS qobs_count,
        COUNT(*) FILTER (WHERE qo.should_exit) AS qobs_exit_signals,
        COUNT(*) FILTER (WHERE qo.no_route) AS qobs_no_routes,
        MAX(qo.real_mult) AS max_qobs_mult,
        (ARRAY_AGG(qo.real_mult ORDER BY qo.observed_at DESC))[1] AS last_qobs_mult,
        MAX(qo.real_mult) FILTER (
            WHERE m.shadow_exit_time IS NOT NULL
              AND qo.observed_at <= m.shadow_exit_time
        ) AS max_qobs_before_shadow_exit
    FROM qsim_quote_observations qo
    WHERE qo.call_id = m.call_id
      AND qo.observed_at BETWEEN m.qsim_entry_time AND COALESCE(m.qsim_exit_time, m.shadow_exit_time)
) qobs ON TRUE
ORDER BY m.qsim_entry_time DESC
"""


@dataclass
class RowView:
    row: dict
    shadow_return: float
    qsim_return: float
    edge: float
    entry_ratio: float | None
    peak_gap: float | None
    exit_gap: float | None
    reason_pair: str
    bucket: str


def _rows(sql: str, params: dict) -> list[dict]:
    from psycopg2.extras import RealDictCursor
    import db

    db.ensure_qsim_positions_table()
    db.ensure_ws_market_observations_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def _f(value, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _ratio(num, den) -> float | None:
    den = _f(den)
    if den <= 0:
        return None
    return _f(num) / den


def _reason_is_bank(reason: str | None) -> bool:
    return reason in {"profit_floor", "trail_stop", "5x_tp", "10x_tp", "50x_tp"}


def _bucket(row: dict, edge: float, entry_ratio: float | None,
            peak_gap: float | None, reason_pair: str) -> str:
    q_reason = row.get("qsim_reason")
    s_reason = row.get("shadow_reason")
    obs_count = int(row.get("obs_count") or 0)
    max_after = _f(row.get("max_ws_after_qsim_mult"))
    max_ws = _f(row.get("max_ws_mult"))
    max_qobs = _f(row.get("max_qobs_mult"))
    q_peak = _f(row.get("qsim_peak"))
    s_peak = _f(row.get("shadow_peak"))

    if entry_ratio is not None and (entry_ratio >= 10.0 or entry_ratio <= 0.10):
        return "extreme_entry_ratio_check_source"
    if q_reason == s_reason and abs(edge) < 0.10:
        return "aligned"
    if q_reason == "hard_stop" and _reason_is_bank(s_reason):
        if max_qobs >= 2.0 and (q_peak < 1.5 or q_peak < max_qobs * 0.5):
            return "qsim_raw_quote_spike_not_banked"
        if obs_count > 0 and max_after >= 1.0:
            return "qsim_hard_stop_before_feed_recovery"
        if obs_count > 0 and max_ws < 1.2:
            return "shadow_bank_not_ws_confirmed"
        if s_peak >= 1.5 and (q_peak < 1.2 or (peak_gap and peak_gap >= 1.5)):
            return "shadow_peak_not_seen_by_qsim_no_path" if obs_count == 0 else "shadow_peak_not_seen_by_qsim"
        return "hard_stop_vs_shadow_bank"
    if _reason_is_bank(q_reason) and s_reason == "hard_stop":
        return "qsim_bank_shadow_hard_stop"
    if entry_ratio is not None and entry_ratio >= 1.25:
        return "executable_entry_haircut"
    if entry_ratio is not None and entry_ratio <= 0.75:
        return "qsim_entry_cheaper_than_feed"
    if obs_count > 0 and peak_gap is not None and peak_gap >= 1.75 and max_ws >= s_peak * 0.8:
        return "feed_path_outran_qsim_quotes"
    if obs_count == 0 and peak_gap is not None and peak_gap >= 1.75:
        return "shadow_peak_gap_no_path"
    if reason_pair.split("/")[:1] != reason_pair.split("/")[1:]:
        return "reason_mismatch"
    if edge >= 0.20:
        return "shadow_materially_better"
    if edge <= -0.20:
        return "qsim_materially_better"
    return "small_gap"


def _view(row: dict) -> RowView:
    shadow_return = _ratio(row.get("shadow_pnl"), row.get("shadow_sol_in")) or 0.0
    qsim_return = _ratio(row.get("qsim_pnl"), row.get("qsim_sol_in")) or 0.0
    edge = shadow_return - qsim_return
    entry_ratio = _ratio(row.get("qsim_entry"), row.get("shadow_entry"))
    peak_gap = _ratio(row.get("shadow_peak"), row.get("qsim_peak"))
    shadow_exit_mult = _ratio(row.get("shadow_exit"), row.get("shadow_entry"))
    qsim_exit_mult = _ratio(row.get("qsim_exit"), row.get("qsim_entry"))
    exit_gap = None
    if shadow_exit_mult is not None and qsim_exit_mult is not None:
        exit_gap = shadow_exit_mult - qsim_exit_mult
    reason_pair = f"{row.get('qsim_reason')}/{row.get('shadow_reason')}"
    return RowView(
        row=row,
        shadow_return=shadow_return,
        qsim_return=qsim_return,
        edge=edge,
        entry_ratio=entry_ratio,
        peak_gap=peak_gap,
        exit_gap=exit_gap,
        reason_pair=reason_pair,
        bucket=_bucket(row, edge, entry_ratio, peak_gap, reason_pair),
    )


def _fmt(value: float | None, digits: int = 2) -> str:
    if value is None:
        return "  n/a"
    return f"{value:>{digits + 4}.{digits}f}"


def _pct(value: float) -> str:
    return f"{value * 100:+6.1f}%"


def _print_summary(views: list[RowView]) -> None:
    if not views:
        print("No matched qsim/shadow rows found for that filter.\n")
        return

    n = len(views)
    q_sum = sum(v.qsim_return for v in views)
    s_sum = sum(v.shadow_return for v in views)
    entry_ratios = [v.entry_ratio for v in views if v.entry_ratio is not None]
    peak_gaps = [v.peak_gap for v in views if v.peak_gap is not None]
    exit_gaps = [v.exit_gap for v in views if v.exit_gap is not None]
    obs_rows = [v for v in views if int(v.row.get("obs_count") or 0) > 0]
    qobs_rows = [v for v in views if int(v.row.get("qobs_count") or 0) > 0]
    same_reason = sum(1 for v in views if v.row.get("qsim_reason") == v.row.get("shadow_reason"))
    shadow_better = sum(1 for v in views if v.edge > 0.05)
    qsim_better = sum(1 for v in views if v.edge < -0.05)

    print("\nQSIM vs SHADOW PATH REPORT")
    print("PnL is normalized per 1 SOL deployed; edge = shadow_return - qsim_return.\n")
    print(f"matched rows:        {n}")
    print(f"reason agreement:   {same_reason}/{n} ({same_reason / n:.0%})")
    print(f"shadow return sum:  {s_sum:+.2f} SOL per-1-SOL units")
    print(f"qsim return sum:    {q_sum:+.2f} SOL per-1-SOL units")
    print(f"shadow - qsim edge: {s_sum - q_sum:+.2f}")
    print(f"shadow better rows: {shadow_better}   qsim better rows: {qsim_better}")
    if entry_ratios:
        print(f"avg qsim/feed entry:{sum(entry_ratios) / len(entry_ratios):.2f}x")
    if peak_gaps:
        print(f"avg shadow/q peak:  {sum(peak_gaps) / len(peak_gaps):.2f}x")
    if exit_gaps:
        print(f"avg exit-mult gap:  {sum(exit_gaps) / len(exit_gaps):+.2f}x")
    print(f"rows with ws path:  {len(obs_rows)}/{n}")
    print(f"rows with qsim path:{len(qobs_rows)}/{n}")


def _print_buckets(views: list[RowView]) -> None:
    buckets: dict[str, list[RowView]] = defaultdict(list)
    for view in views:
        buckets[view.bucket].append(view)

    print("\nBuckets")
    print(f"{'bucket':<38} {'n':>5} {'edge':>9} {'q_ret':>9} {'s_ret':>9} {'agree':>7}")
    print("-" * 84)
    for bucket, rows in sorted(buckets.items(), key=lambda item: sum(v.edge for v in item[1])):
        n = len(rows)
        edge = sum(v.edge for v in rows)
        q_ret = sum(v.qsim_return for v in rows)
        s_ret = sum(v.shadow_return for v in rows)
        agree = sum(1 for v in rows if v.row.get("qsim_reason") == v.row.get("shadow_reason"))
        print(f"{bucket:<38} {n:>5} {edge:>+9.2f} {q_ret:>+9.2f} {s_ret:>+9.2f} {agree:>3}/{n:<3}")


def _print_reason_pairs(views: list[RowView], limit: int) -> None:
    pair_counts = Counter(v.reason_pair for v in views)
    print("\nReason Pairs")
    print(f"{'pair':<32} {'n':>5} {'edge':>9}")
    print("-" * 50)
    for pair, n in pair_counts.most_common(limit):
        edge = sum(v.edge for v in views if v.reason_pair == pair)
        print(f"{pair:<32} {n:>5} {edge:>+9.2f}")


def _print_detail(views: list[RowView], limit: int) -> None:
    print("\nDetail — largest absolute normalized edge first")
    hdr = (
        f"{'call':>7} {'symbol':<12} {'q/s ent':>7} {'qpk':>5} {'spk':>5} "
        f"{'qret':>7} {'sret':>7} {'edge':>7} {'wsmax':>6} {'qmax':>6} "
        f"{'q_reason/shadow_reason':<28} bucket"
    )
    print(hdr)
    print("-" * len(hdr))
    rows = sorted(views, key=lambda view: abs(view.edge), reverse=True)[:limit]
    for view in rows:
        row = view.row
        print(
            f"{int(row['call_id']):>7} "
            f"{(row.get('symbol') or '?')[:12]:<12} "
            f"{_fmt(view.entry_ratio, 2):>7} "
            f"{_fmt(_f(row.get('qsim_peak')), 2):>5} "
            f"{_fmt(_f(row.get('shadow_peak')), 2):>5} "
            f"{_pct(view.qsim_return):>7} "
            f"{_pct(view.shadow_return):>7} "
            f"{_pct(view.edge):>7} "
            f"{_fmt(_f(row.get('max_ws_mult')), 2):>6} "
            f"{_fmt(_f(row.get('max_qobs_mult')), 2):>6} "
            f"{view.reason_pair[:28]:<28} "
            f"{view.bucket}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Explain same-call qsim vs shadow path disagreements."
    )
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--channel", default="any")
    parser.add_argument("--lane", default="any")
    parser.add_argument("--variant", default="early")
    parser.add_argument("--reason-pair", default="any",
                        help="Filter to qsim_reason/shadow_reason, e.g. hard_stop/profit_floor")
    parser.add_argument("--bucket", default="any", help="Filter detail rows to one bucket")
    parser.add_argument("--require-ws", action="store_true",
                        help="only include rows with websocket market observations")
    parser.add_argument("--min-entry-ratio", type=float, default=None,
                        help="only include qsim_entry / shadow_entry >= this value")
    parser.add_argument("--max-entry-ratio", type=float, default=None,
                        help="only include qsim_entry / shadow_entry <= this value")
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
    rows = _rows(SQL, params)
    views = [_view(row) for row in rows]
    if args.require_ws:
        views = [view for view in views if int(view.row.get("obs_count") or 0) > 0]
    if args.reason_pair != "any":
        views = [view for view in views if view.reason_pair == args.reason_pair]
    if args.bucket != "any":
        views = [view for view in views if view.bucket == args.bucket]

    print(
        f"filters: days={args.days} channel={args.channel} lane={args.lane} "
        f"variant={args.variant} reason_pair={args.reason_pair} bucket={args.bucket} "
        f"require_ws={args.require_ws}"
    )
    _print_summary(views)
    if views:
        _print_buckets(views)
        _print_reason_pairs(views, limit=20)
        if args.detail:
            _print_detail(views, args.limit)
        else:
            print("\nRun with --detail for per-trade rows, or --bucket/--reason-pair to zoom in.\n")


if __name__ == "__main__":
    main()
