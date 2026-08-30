"""
qsim_shadow_trade_reconcile.py — side-by-side qsim vs shadow trade accounting.

This is the boring ledger report:

    entry price, exit price, peak, PnL, exit reason, timing, qmax

for every qsim/shadow row in a window. It exists to answer whether qsim and
shadow disagree because of entry price, exit price, peak capture, missing quote
coverage, or missing positions.

Example:
    python3 qsim_shadow_trade_reconcile.py --days 5 --channel solwhaletrending --lane low_score --variant early
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db


MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5
MAX_QOBS_MULT = 50.0


SQL = """
WITH shadow_base AS (
    SELECT
        sp.call_id,
        sp.exit_variant AS variant,
        sp.entry_time,
        sp.exit_time,
        sp.peak_at,
        sp.entry_price,
        sp.entry_source,
        sp.entry_ref_mcap,
        sp.exit_price,
        sp.peak_multiplier,
        sp.sol_in,
        sp.sol_out,
        sp.pnl_sol,
        sp.pnl_pct,
        sp.exit_reason,
        sp.status,
        sp.vip_tier
    FROM shadow_positions sp
    WHERE (%(days)s = 0 OR sp.entry_time >= now() - (%(days)s || ' days')::interval)
      AND (%(since)s IS NULL OR sp.entry_time >= %(since)s::timestamptz)
      AND (%(variant)s = 'any' OR sp.exit_variant = %(variant)s)
      AND (%(raw)s OR NOT (
          COALESCE(sp.peak_multiplier, 0) > %(max_peak)s
       OR COALESCE(sp.pnl_pct, 0) > %(max_pnl)s
       OR COALESCE(sp.pnl_pct, 0) < %(min_pnl)s
      ))
),
qsim_base AS (
    SELECT
        q.call_id,
        q.variant,
        q.entry_time,
        q.exit_time,
        q.peak_at,
        q.entry_price,
        q.exit_price,
        q.peak_multiplier,
        q.sol_in,
        q.entry_tokens,
        q.entry_decimals,
        q.sol_out,
        q.pnl_sol,
        q.pnl_pct,
        q.exit_reason,
        q.status,
        q.vip_tier
    FROM qsim_positions q
    WHERE (%(days)s = 0 OR q.entry_time >= now() - (%(days)s || ' days')::interval)
      AND (%(since)s IS NULL OR q.entry_time >= %(since)s::timestamptz)
      AND (%(variant)s = 'any' OR q.variant = %(variant)s)
      AND (%(raw)s OR NOT (
          COALESCE(q.peak_multiplier, 0) > %(max_peak)s
       OR COALESCE(q.pnl_pct, 0) > %(max_pnl)s
       OR COALESCE(q.pnl_pct, 0) < %(min_pnl)s
      ))
),
matched AS (
    SELECT
        COALESCE(s.call_id, q.call_id) AS call_id,
        COALESCE(s.variant, q.variant) AS variant,
        s.entry_time AS shadow_entry_time,
        s.exit_time AS shadow_exit_time,
        s.peak_at AS shadow_peak_at,
        s.entry_price AS shadow_entry,
        s.entry_source AS shadow_entry_source,
        s.entry_ref_mcap AS shadow_entry_ref_mcap,
        s.exit_price AS shadow_exit,
        s.peak_multiplier AS shadow_peak,
        s.sol_in AS shadow_sol_in,
        s.sol_out AS shadow_sol_out,
        s.pnl_sol AS shadow_pnl,
        s.pnl_pct AS shadow_pnl_pct,
        s.exit_reason AS shadow_reason,
        s.status AS shadow_status,
        q.entry_time AS qsim_entry_time,
        q.exit_time AS qsim_exit_time,
        q.peak_at AS qsim_peak_at,
        q.entry_price AS qsim_entry,
        q.exit_price AS qsim_exit,
        q.peak_multiplier AS qsim_peak,
        q.sol_in AS qsim_sol_in,
        q.entry_tokens AS qsim_entry_tokens,
        q.entry_decimals AS qsim_entry_decimals,
        q.sol_out AS qsim_sol_out,
        q.pnl_sol AS qsim_pnl,
        q.pnl_pct AS qsim_pnl_pct,
        q.exit_reason AS qsim_reason,
        q.status AS qsim_status,
        COALESCE(s.vip_tier, q.vip_tier) AS vip_tier
    FROM shadow_base s
    FULL OUTER JOIN qsim_base q
      ON q.call_id = s.call_id
     AND q.variant = s.variant
)
SELECT
    m.*,
    tok.symbol,
    tok.total_supply,
    tok.decimals AS token_decimals,
    COALESCE(ch.handle, '?') AS channel,
    COALESCE(c.skip_reason, 'none') AS lane,
    c.message_type,
    c.call_type,
    c.mcap_at_call AS feed_entry,
    qobs.qobs_count,
    qobs.max_qobs_mult,
    qobs.max_qobs_at,
    sh_any.shadow_any_count,
    sh_any.shadow_variant_statuses,
    EXTRACT(EPOCH FROM (m.qsim_entry_time - m.shadow_entry_time)) AS entry_delay_sec,
    EXTRACT(EPOCH FROM (m.qsim_exit_time - m.shadow_exit_time)) AS exit_delay_sec,
    EXTRACT(EPOCH FROM (m.shadow_peak_at - m.shadow_entry_time)) / 60.0 AS shadow_peak_min,
    EXTRACT(EPOCH FROM (m.qsim_peak_at - m.qsim_entry_time)) / 60.0 AS qsim_peak_min,
    EXTRACT(EPOCH FROM (qobs.max_qobs_at - m.qsim_entry_time)) / 60.0 AS qmax_min
FROM matched m
JOIN calls c ON c.id = m.call_id
JOIN tokens tok ON tok.id = c.token_id
LEFT JOIN channels ch ON ch.id = c.channel_id
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) AS qobs_count,
        MAX(qo.real_mult) AS max_qobs_mult,
        (ARRAY_AGG(qo.observed_at ORDER BY qo.real_mult DESC, qo.observed_at ASC))[1] AS max_qobs_at
    FROM qsim_quote_observations qo
    WHERE qo.call_id = m.call_id
      AND m.qsim_entry_time IS NOT NULL
      AND qo.real_mult > 0
      AND qo.real_mult <= %(max_qmax)s
      AND qo.observed_at BETWEEN m.qsim_entry_time AND COALESCE(m.qsim_exit_time, now())
) qobs ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) AS shadow_any_count,
        STRING_AGG(sp2.exit_variant || ':' || sp2.status, ', ' ORDER BY sp2.exit_variant, sp2.status) AS shadow_variant_statuses
    FROM shadow_positions sp2
    WHERE sp2.call_id = m.call_id
) sh_any ON TRUE
WHERE (%(channel)s = 'any' OR COALESCE(ch.handle, '?') = %(channel)s)
  AND (%(lane)s = 'any' OR COALESCE(c.skip_reason, 'none') = %(lane)s)
ORDER BY COALESCE(m.shadow_entry_time, m.qsim_entry_time) DESC
"""


def _rows(params: dict[str, Any]) -> list[dict[str, Any]]:
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


def _ratio(num: Any, den: Any) -> float | None:
    den_f = _f(den)
    if den_f <= 0:
        return None
    return _f(num) / den_f


def _ret(row: dict[str, Any], prefix: str) -> float:
    return _ratio(row.get(f"{prefix}_pnl"), row.get(f"{prefix}_sol_in")) or 0.0


def _bucket(row: dict[str, Any], edge: float) -> str:
    has_shadow = row.get("shadow_entry") is not None
    has_qsim = row.get("qsim_entry") is not None
    if has_shadow and not has_qsim:
        return "shadow_only"
    if has_qsim and not has_shadow:
        return "qsim_only"
    if row.get("qsim_status") != "closed":
        return "qsim_open"

    entry_ratio = _ratio(row.get("qsim_entry"), row.get("shadow_entry"))
    shadow_peak = _f(row.get("shadow_peak"))
    qsim_peak = _f(row.get("qsim_peak"))
    qmax = _f(row.get("max_qobs_mult"))
    shadow_exit_mult = _ratio(row.get("shadow_exit"), row.get("shadow_entry"))
    qsim_exit_mult = _ratio(row.get("qsim_exit"), row.get("qsim_entry"))

    if entry_ratio is not None and (entry_ratio >= 2.0 or entry_ratio <= 0.5):
        return "entry_price_gap"
    if shadow_peak >= 1.5 and qmax < 1.2:
        return "shadow_peak_not_quoted"
    if shadow_peak >= 1.5 and qmax >= 1.2 and qmax < shadow_peak * 0.8:
        return "quote_smaller_than_shadow"
    if qsim_peak and shadow_peak and qsim_peak < shadow_peak * 0.6:
        return "qsim_peak_lower"
    if shadow_exit_mult is not None and qsim_exit_mult is not None:
        if shadow_exit_mult - qsim_exit_mult >= 0.5:
            return "shadow_exit_better"
        if qsim_exit_mult - shadow_exit_mult >= 0.5:
            return "qsim_exit_better"
    if edge >= 0.5:
        return "shadow_pnl_better"
    if edge <= -0.5:
        return "qsim_pnl_better"
    return "close"


def _fmt(value: Any, width: int = 7, decimals: int = 2) -> str:
    if value is None:
        return f"{'n/a':>{width}}"
    return f"{_f(value):>{width}.{decimals}f}"


def _date(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, datetime):
        return value.strftime("%m/%d %H:%M")
    return str(value)[:11]


def _decimals(row: dict[str, Any]) -> int:
    value = row.get("qsim_entry_decimals")
    if value is None:
        value = row.get("token_decimals")
    try:
        return int(value) if value is not None else 6
    except (TypeError, ValueError):
        return 6


def _supply_whole(row: dict[str, Any]) -> float | None:
    supply_raw = _f(row.get("total_supply"))
    if supply_raw <= 0:
        return None
    decimals = _decimals(row)
    supply_whole = supply_raw / (10 ** decimals)
    return supply_whole if supply_whole > 0 else None


def _qsim_tokens_whole(row: dict[str, Any]) -> float | None:
    tokens_raw = _f(row.get("qsim_entry_tokens"))
    if tokens_raw <= 0:
        return None
    tokens_whole = tokens_raw / (10 ** _decimals(row))
    return tokens_whole if tokens_whole > 0 else None


def _entry_diag(row: dict[str, Any]) -> dict[str, float | None]:
    entry_ratio = row.get("_entry_ratio")
    q_tokens = _qsim_tokens_whole(row)
    supply = _supply_whole(row)
    qsim_entry = _f(row.get("qsim_entry"))
    qsim_sol = _f(row.get("qsim_sol_in"))

    expected_tokens = None
    token_fill_ratio = None
    if q_tokens is not None and entry_ratio is not None and entry_ratio > 0:
        expected_tokens = q_tokens * entry_ratio
        token_fill_ratio = q_tokens / expected_tokens if expected_tokens > 0 else None

    implied_sol_usd = None
    if q_tokens is not None and supply and qsim_entry > 0 and qsim_sol > 0:
        implied_sol_usd = qsim_entry * q_tokens / (qsim_sol * supply)

    return {
        "q_tokens": q_tokens,
        "expected_tokens": expected_tokens,
        "token_fill_ratio": token_fill_ratio,
        "supply": supply,
        "implied_sol_usd": implied_sol_usd,
    }


def _annotate(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        shadow_return = _ret(row, "shadow")
        qsim_return = _ret(row, "qsim")
        edge = shadow_return - qsim_return
        row["_shadow_return"] = shadow_return
        row["_qsim_return"] = qsim_return
        row["_edge"] = edge
        row["_entry_ratio"] = _ratio(row.get("qsim_entry"), row.get("shadow_entry"))
        row["_shadow_exit_mult"] = _ratio(row.get("shadow_exit"), row.get("shadow_entry"))
        row["_qsim_exit_mult"] = _ratio(row.get("qsim_exit"), row.get("qsim_entry"))
        row["_bucket"] = _bucket(row, edge)
        out.append(row)
    return out


def _filtered(rows: list[dict[str, Any]], args: argparse.Namespace) -> list[dict[str, Any]]:
    out = []
    for row in rows:
        if args.closed_only and (
            row.get("shadow_status") != "closed" or row.get("qsim_status") != "closed"
        ):
            continue
        if args.require_qobs and int(row.get("qobs_count") or 0) <= 0:
            continue
        if args.min_entry_ratio is not None:
            ratio = row.get("_entry_ratio")
            if ratio is None or ratio < args.min_entry_ratio:
                continue
        if args.max_entry_ratio is not None:
            ratio = row.get("_entry_ratio")
            if ratio is None or ratio > args.max_entry_ratio:
                continue
        out.append(row)
    return out


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    values = sorted(values)
    return values[len(values) // 2]


def _print_summary(rows: list[dict[str, Any]]) -> None:
    matched = [row for row in rows if row.get("shadow_entry") is not None and row.get("qsim_entry") is not None]
    shadow_only = [row for row in rows if row.get("shadow_entry") is not None and row.get("qsim_entry") is None]
    qsim_only = [row for row in rows if row.get("qsim_entry") is not None and row.get("shadow_entry") is None]
    shadow_sum = sum(row["_shadow_return"] for row in rows)
    qsim_sum = sum(row["_qsim_return"] for row in rows)
    entry_ratios = [row["_entry_ratio"] for row in matched if row["_entry_ratio"] is not None]
    entry_delays = [_f(row.get("entry_delay_sec")) for row in matched if row.get("entry_delay_sec") is not None]
    print("\nSummary")
    print(f"rows={len(rows)} matched={len(matched)} shadow_only={len(shadow_only)} qsim_only={len(qsim_only)}")
    print(f"shadow_sum={shadow_sum:+.2f} qsim_sum={qsim_sum:+.2f} edge={shadow_sum - qsim_sum:+.2f}")
    if entry_ratios:
        print(f"avg_qsim/shadow_entry={sum(entry_ratios) / len(entry_ratios):.2f}x")
    if entry_delays:
        print(
            "entry_delay_sec="
            f"avg:{sum(entry_delays) / len(entry_delays):.1f} "
            f"med:{_median(entry_delays):.1f} "
            f"min:{min(entry_delays):.1f} max:{max(entry_delays):.1f}"
        )


def _print_shadow_overlap(rows: list[dict[str, Any]]) -> None:
    qsim_only = [row for row in rows if row.get("qsim_entry") is not None and row.get("shadow_entry") is None]
    if not qsim_only:
        return

    counts: dict[str, int] = defaultdict(int)
    for row in qsim_only:
        variants = row.get("shadow_variant_statuses") or "no_shadow_row"
        counts[str(variants)] += 1

    print("\nQsim-Only Shadow Overlap")
    print("Shows whether qsim-only rows have shadow rows under another variant/status.")
    hdr = f"{'shadow_rows_for_call':<46} {'n':>6}"
    print(hdr)
    print("-" * len(hdr))
    for label, count in sorted(counts.items(), key=lambda item: item[1], reverse=True):
        print(f"{label[:46]:<46} {count:>6}")


def _print_buckets(rows: list[dict[str, Any]]) -> None:
    print("\nDiscrepancy Buckets")
    hdr = (
        f"{'bucket':<26} {'n':>6} {'shadow':>10} {'qsim':>10} {'edge':>10} "
        f"{'avg_ent':>8} {'avg_spk':>8} {'avg_qpk':>8} {'avg_qmax':>9}"
    )
    print(hdr)
    print("-" * len(hdr))
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[row["_bucket"]].append(row)
    for bucket, group in sorted(buckets.items(), key=lambda item: abs(sum(r["_edge"] for r in item[1])), reverse=True):
        shadow = sum(row["_shadow_return"] for row in group)
        qsim = sum(row["_qsim_return"] for row in group)
        entry_ratios = [row["_entry_ratio"] for row in group if row["_entry_ratio"] is not None]
        avg_entry = sum(entry_ratios) / len(entry_ratios) if entry_ratios else 0.0
        avg_spk = sum(_f(row.get("shadow_peak")) for row in group) / len(group)
        avg_qpk = sum(_f(row.get("qsim_peak")) for row in group) / len(group)
        qmaxes = [_f(row.get("max_qobs_mult")) for row in group if row.get("max_qobs_mult") is not None]
        avg_qmax = sum(qmaxes) / len(qmaxes) if qmaxes else 0.0
        print(
            f"{bucket:<26} {len(group):>6} {shadow:>+10.2f} {qsim:>+10.2f} {shadow - qsim:>+10.2f} "
            f"{avg_entry:>8.2f} {avg_spk:>8.2f} {avg_qpk:>8.2f} {avg_qmax:>9.2f}"
        )


def _print_details(rows: list[dict[str, Any]], limit: int) -> None:
    print("\nLargest Absolute Gaps")
    hdr = (
        f"{'call':>7} {'symbol':<12} {'bucket':<24} {'ent':>6} {'delay':>7} "
        f"{'s_ex':>6} {'q_ex':>6} {'s_pk':>6} {'q_pk':>6} {'qmax':>6} "
        f"{'s_ret':>8} {'q_ret':>8} {'edge':>8} {'s_reason/q_reason':<28}"
    )
    print(hdr)
    print("-" * len(hdr))
    for row in sorted(rows, key=lambda r: abs(r["_edge"]), reverse=True)[:limit]:
        reason = f"{row.get('shadow_reason')}/{row.get('qsim_reason')}"
        print(
            f"{int(row['call_id']):>7} "
            f"{(row.get('symbol') or '?')[:12]:<12} "
            f"{row['_bucket'][:24]:<24} "
            f"{_fmt(row.get('_entry_ratio'), 6, 2)} "
            f"{_fmt(row.get('entry_delay_sec'), 7, 1)} "
            f"{_fmt(row.get('_shadow_exit_mult'), 6, 2)} "
            f"{_fmt(row.get('_qsim_exit_mult'), 6, 2)} "
            f"{_fmt(row.get('shadow_peak'), 6, 2)} "
            f"{_fmt(row.get('qsim_peak'), 6, 2)} "
            f"{_fmt(row.get('max_qobs_mult'), 6, 2)} "
            f"{row['_shadow_return']:>+8.2f} "
            f"{row['_qsim_return']:>+8.2f} "
            f"{row['_edge']:>+8.2f} "
            f"{reason[:28]:<28}"
        )


def _print_timing(rows: list[dict[str, Any]], limit: int) -> None:
    print("\nLargest Shadow Winners Timing")
    hdr = (
        f"{'call':>7} {'symbol':<12} {'entry':<11} {'s_pk_m':>7} {'q_pk_m':>7} "
        f"{'qmax_m':>7} {'x_delay':>8} {'s_pk':>6} {'qmax':>6} {'s_ret':>8} {'q_ret':>8}"
    )
    print(hdr)
    print("-" * len(hdr))
    winners = sorted(rows, key=lambda r: r["_shadow_return"], reverse=True)[:limit]
    for row in winners:
        print(
            f"{int(row['call_id']):>7} "
            f"{(row.get('symbol') or '?')[:12]:<12} "
            f"{_date(row.get('shadow_entry_time') or row.get('qsim_entry_time')):<11} "
            f"{_fmt(row.get('shadow_peak_min'), 7, 2)} "
            f"{_fmt(row.get('qsim_peak_min'), 7, 2)} "
            f"{_fmt(row.get('qmax_min'), 7, 2)} "
            f"{_fmt(row.get('entry_delay_sec'), 8, 1)} "
            f"{_fmt(row.get('shadow_peak'), 6, 2)} "
            f"{_fmt(row.get('max_qobs_mult'), 6, 2)} "
            f"{row['_shadow_return']:>+8.2f} "
            f"{row['_qsim_return']:>+8.2f}"
        )


def _print_entry_debug(rows: list[dict[str, Any]], limit: int) -> None:
    print("\nEntry Price Forensics")
    print(
        "expected_tok = tokens qsim would have received if qsim fill matched shadow entry "
        "using the same supply/SOL assumptions."
    )
    hdr = (
        f"{'call':>7} {'symbol':<12} {'bucket':<20} {'ent':>6} {'delay':>6} "
        f"{'shadow_k':>9} {'qsim_k':>9} {'feed_k':>9} "
        f"{'q_tok_m':>9} {'exp_tok_m':>10} {'fill%':>7} {'sh/feed':>7} {'q/feed':>7} "
        f"{'sol$':>7} {'dec':>4} {'supply_b':>9} {'sh_src':<18}"
    )
    print(hdr)
    print("-" * len(hdr))
    candidates = [
        row for row in rows
        if row.get("_entry_ratio") is not None and row.get("qsim_entry") and row.get("shadow_entry")
    ]
    candidates = sorted(candidates, key=lambda r: abs((_f(r.get("_entry_ratio")) or 1.0) - 1.0), reverse=True)
    for row in candidates[:limit]:
        diag = _entry_diag(row)
        fill_ratio = diag["token_fill_ratio"]
        supply = diag["supply"]
        print(
            f"{int(row['call_id']):>7} "
            f"{(row.get('symbol') or '?')[:12]:<12} "
            f"{row['_bucket'][:20]:<20} "
            f"{_fmt(row.get('_entry_ratio'), 6, 2)} "
            f"{_fmt(row.get('entry_delay_sec'), 6, 1)} "
            f"{_f(row.get('shadow_entry')) / 1000:>9.1f} "
            f"{_f(row.get('qsim_entry')) / 1000:>9.1f} "
            f"{_f(row.get('feed_entry')) / 1000:>9.1f} "
            f"{((diag['q_tokens'] or 0.0) / 1_000_000):>9.2f} "
            f"{((diag['expected_tokens'] or 0.0) / 1_000_000):>10.2f} "
            f"{((fill_ratio or 0.0) * 100):>6.1f}% "
            f"{_fmt(_ratio(row.get('shadow_entry'), row.get('feed_entry')), 7, 2)} "
            f"{_fmt(_ratio(row.get('qsim_entry'), row.get('feed_entry')), 7, 2)} "
            f"{_fmt(diag.get('implied_sol_usd'), 7, 0)} "
            f"{_decimals(row):>4} "
            f"{((supply or 0.0) / 1_000_000_000):>9.2f} "
            f"{(row.get('shadow_entry_source') or '?')[:18]:<18}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Reconcile qsim vs shadow trade accounting.")
    parser.add_argument("--days", type=int, default=5)
    parser.add_argument("--since", default=None, help="only include entries at/after this timestamp")
    parser.add_argument("--channel", default="any")
    parser.add_argument("--lane", default="any")
    parser.add_argument("--variant", default="early")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--closed-only", action="store_true", help="only include closed qsim + closed shadow matches")
    parser.add_argument("--require-qobs", action="store_true", help="only include rows with qsim quote observations")
    parser.add_argument("--min-entry-ratio", type=float, default=None)
    parser.add_argument("--max-entry-ratio", type=float, default=None)
    parser.add_argument("--raw", action="store_true")
    parser.add_argument("--max-qmax", type=float, default=MAX_QOBS_MULT)
    parser.add_argument("--entry-debug", action="store_true", help="print qsim-vs-shadow entry token forensics")
    args = parser.parse_args()

    params = {
        "days": args.days,
        "since": args.since,
        "channel": args.channel,
        "lane": args.lane,
        "variant": args.variant,
        "raw": args.raw,
        "max_peak": MAX_SANE_PEAK,
        "max_pnl": MAX_SANE_PNL_PCT,
        "min_pnl": MIN_SANE_PNL_PCT,
        "max_qmax": args.max_qmax,
    }
    rows = _filtered(_annotate(_rows(params)), args)
    print(
        f"\nQSIM/SHADOW TRADE RECONCILE — days={args.days} channel={args.channel} "
        f"lane={args.lane} variant={args.variant} since={args.since} max_qmax={args.max_qmax}"
    )
    print(
        "PnL is normalized per 1 SOL deployed. entry ratio = qsim_entry / shadow_entry. "
        "delay = qsim_entry_time - shadow_entry_time seconds."
    )
    if not rows:
        print("No rows matched.")
        return
    _print_summary(rows)
    _print_shadow_overlap(rows)
    _print_buckets(rows)
    _print_details(rows, args.limit)
    _print_timing(rows, min(args.limit, 30))
    if args.entry_debug:
        _print_entry_debug(rows, args.limit)


if __name__ == "__main__":
    main()
