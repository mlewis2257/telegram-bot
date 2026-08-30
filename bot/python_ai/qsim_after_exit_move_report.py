"""
qsim_after_exit_move_report.py — audit what happened after qsim exited.

This answers the DOGGYSTYLE question:

    "Did qsim exit before the real move, or was the later shadow move not
     corroborated by anything we could have sold?"

Historical mode uses ws_market_observations/feed after qsim exit through the
matching shadow exit. Forward mode also reads qsim post-exit quote probes
written by qsim.py when QSIM_POST_EXIT_OBS_ENABLED=true.

Important: historical ws/feed observations may be sparse after qsim closes.
For pre-probe history, the report also uses shadow_peak_at to identify cases
where shadow's larger peak happened after qsim had already exited.

Examples:
    python3 qsim_after_exit_move_report.py --days 7 --channel solwhaletrending --lane low_score --variant early
    python3 qsim_after_exit_move_report.py --days 7 --variant early --shadow-variant ride_vol
    python3 qsim_after_exit_move_report.py --days 1 --symbol DOGGYSTYLE --detail
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from typing import Any

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
        q.variant,
        q.entry_time AS qsim_entry_time,
        q.exit_time AS qsim_exit_time,
        q.entry_price AS qsim_entry,
        q.exit_price AS qsim_exit,
        q.peak_multiplier AS qsim_peak,
        q.pnl_sol AS qsim_pnl,
        q.pnl_pct AS qsim_pnl_pct,
        q.exit_reason AS qsim_reason,
        q.sol_in AS qsim_sol_in,
        sp.exit_variant AS shadow_variant,
        sp.entry_time AS shadow_entry_time,
        sp.exit_time AS shadow_exit_time,
        sp.peak_at AS shadow_peak_at,
        sp.entry_price AS shadow_entry,
        sp.exit_price AS shadow_exit,
        sp.peak_multiplier AS shadow_peak,
        sp.sol_in AS shadow_sol_in,
        sp.pnl_sol AS shadow_pnl,
        sp.pnl_pct AS shadow_pnl_pct,
        sp.exit_reason AS shadow_reason,
        c.mcap_at_call AS feed_entry,
        c.conviction_score AS score,
        t.liq_at_detection AS liquidity,
        t.vol_1h_at_detection AS vol_1h,
        t.vol_1h_at_detection / NULLIF(q.entry_price, 0) AS vol_mcap,
        t.bundle_count,
        t.bundle_pct_remaining AS bundle_pct,
        t.sniper_count,
        t.sniper_pct_remaining AS sniper_pct,
        t.fake_vol_pct,
        t.holder_count,
        t.first_20_pct,
        t.token_age_minutes AS age_min
    FROM qsim_positions q
    JOIN shadow_positions sp
      ON sp.call_id = q.call_id
     AND (
          (%(shadow_variant)s = 'same' AND sp.exit_variant = q.variant)
       OR (%(shadow_variant)s != 'same' AND sp.exit_variant = %(shadow_variant)s)
     )
    JOIN calls c ON c.id = q.call_id
    JOIN tokens t ON t.id = q.token_id
    LEFT JOIN channels ch ON ch.id = c.channel_id
    WHERE q.status = 'closed'
      AND sp.status = 'closed'
      AND q.exit_time IS NOT NULL
      AND q.entry_time >= now() - (%(days)s || ' days')::interval
      AND (%(since)s IS NULL OR q.entry_time >= %(since)s::timestamptz)
      AND (%(channel)s = 'any' OR COALESCE(ch.handle, '?') = %(channel)s)
      AND (%(lane)s = 'any' OR COALESCE(c.skip_reason, 'none') = %(lane)s)
      AND (%(variant)s = 'any' OR q.variant = %(variant)s)
      AND (%(symbol)s = 'any' OR lower(t.symbol) = lower(%(symbol)s))
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
    qpre.max_qobs_mult,
    qpost.post_qobs_count,
    qpost.max_post_qobs_mult,
    qpost.max_post_qobs_at,
    ws.obs_after_count,
    ws.max_ws_after_mcap,
    ws.max_ws_after_at,
    CASE WHEN m.qsim_entry > 0 THEN ws.max_ws_after_mcap / m.qsim_entry ELSE NULL END AS max_ws_after_qsim_mult,
    CASE WHEN m.shadow_entry > 0 THEN ws.max_ws_after_mcap / m.shadow_entry ELSE NULL END AS max_ws_after_shadow_mult,
    CASE
        WHEN m.shadow_peak_at > m.qsim_exit_time THEN m.shadow_peak
        ELSE NULL
    END AS shadow_peak_after_qsim,
    EXTRACT(EPOCH FROM (m.shadow_exit_time - m.qsim_exit_time)) / 60.0 AS shadow_held_after_qsim_min,
    EXTRACT(EPOCH FROM (m.shadow_peak_at - m.qsim_exit_time)) / 60.0 AS shadow_peak_after_qsim_min,
    EXTRACT(EPOCH FROM (ws.max_ws_after_at - m.qsim_exit_time)) / 60.0 AS ws_peak_after_qsim_min,
    EXTRACT(EPOCH FROM (qpost.max_post_qobs_at - m.qsim_exit_time)) / 60.0 AS qobs_peak_after_qsim_min
FROM matched m
LEFT JOIN LATERAL (
    SELECT MAX(qo.real_mult) AS max_qobs_mult
    FROM qsim_quote_observations qo
    WHERE qo.call_id = m.call_id
      AND qo.real_mult > 0
      AND qo.real_mult <= %(max_qmax)s
      AND qo.observed_at BETWEEN m.qsim_entry_time AND m.qsim_exit_time
) qpre ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) AS post_qobs_count,
        MAX(qo.real_mult) AS max_post_qobs_mult,
        (ARRAY_AGG(qo.observed_at ORDER BY qo.real_mult DESC, qo.observed_at ASC))[1] AS max_post_qobs_at
    FROM qsim_quote_observations qo
    WHERE qo.call_id = m.call_id
      AND qo.real_mult > 0
      AND qo.real_mult <= %(max_qmax)s
      AND qo.observed_at > m.qsim_exit_time
      AND qo.observed_at <= m.qsim_exit_time + (%(post_mins)s || ' minutes')::interval
) qpost ON TRUE
LEFT JOIN LATERAL (
    SELECT
        COUNT(*) AS obs_after_count,
        MAX(o.mcap) AS max_ws_after_mcap,
        (ARRAY_AGG(o.observed_at ORDER BY o.mcap DESC, o.observed_at ASC))[1] AS max_ws_after_at
    FROM ws_market_observations o
    WHERE o.call_id = m.call_id
      AND o.mcap IS NOT NULL
      AND o.observed_at > m.qsim_exit_time
      AND o.observed_at <= LEAST(
          m.qsim_exit_time + (%(post_mins)s || ' minutes')::interval,
          GREATEST(m.shadow_exit_time, m.qsim_exit_time)
      )
) ws ON TRUE
ORDER BY
    (COALESCE(ws.max_ws_after_mcap / NULLIF(m.qsim_entry, 0), 0) - COALESCE(m.qsim_exit / NULLIF(m.qsim_entry, 0), 0)) DESC,
    m.qsim_exit_time DESC
"""


def _rows(params: dict[str, Any]) -> list[dict[str, Any]]:
    import db
    from psycopg2.extras import RealDictCursor

    db.ensure_qsim_positions_table()
    db.ensure_shadow_positions_table()
    db.ensure_ws_market_observations_table()
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


def _bucket(row: dict[str, Any]) -> str:
    q_exit = _ratio(row.get("qsim_exit"), row.get("qsim_entry")) or 0.0
    q_peak = _f(row.get("qsim_peak"))
    shadow_peak = _f(row.get("shadow_peak"))
    shadow_after = _f(row.get("shadow_peak_after_qsim"))
    ws_after = _f(row.get("max_ws_after_qsim_mult"))
    qpost = _f(row.get("max_post_qobs_mult"))
    held_min = _f(row.get("shadow_held_after_qsim_min"))

    if held_min <= 0:
        return "shadow_not_longer"
    if qpost >= max(q_exit + 0.5, q_peak * 1.25, 2.0):
        return "quote_confirmed_after_exit"
    if ws_after >= max(q_exit + 0.5, q_peak * 1.25, 2.0):
        return "feed_after_exit_runner"
    if shadow_after >= max(q_exit + 0.5, q_peak * 1.25, 2.0):
        return "shadow_peak_after_qsim"
    if shadow_peak >= q_peak * 1.5 and ws_after < q_peak * 1.1:
        return "shadow_peak_not_ws_after_exit"
    if ws_after > q_exit + 0.25:
        return "modest_after_exit_lift"
    return "no_after_exit_edge"


def _fmt(value: Any, width: int = 7, decimals: int = 2) -> str:
    if value is None:
        return f"{'n/a':>{width}}"
    return f"{_f(value):>{width}.{decimals}f}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--since", default=None, help="only include qsim entries at/after this timestamp")
    parser.add_argument("--channel", default="any")
    parser.add_argument("--lane", default="any")
    parser.add_argument("--variant", default="any")
    parser.add_argument("--shadow-variant", default="same", choices=("same", "early", "ride", "ride_vol"))
    parser.add_argument("--symbol", default="any")
    parser.add_argument("--post-mins", type=float, default=90.0)
    parser.add_argument("--max-qmax", type=float, default=50.0)
    parser.add_argument("--min-entry-ratio", type=float, default=None)
    parser.add_argument("--max-entry-ratio", type=float, default=None)
    parser.add_argument("--limit", type=int, default=40)
    parser.add_argument("--held-longer-only", action="store_true")
    parser.add_argument("--detail", action="store_true")
    parser.add_argument("--raw", action="store_true")
    args = parser.parse_args()

    params = {
        "days": args.days,
        "since": args.since,
        "channel": args.channel,
        "lane": args.lane,
        "variant": args.variant,
        "shadow_variant": args.shadow_variant,
        "symbol": args.symbol,
        "post_mins": args.post_mins,
        "max_qmax": args.max_qmax,
        "min_entry_ratio": args.min_entry_ratio,
        "max_entry_ratio": args.max_entry_ratio,
        "raw": args.raw,
        "max_peak": MAX_SANE_PEAK,
        "max_pnl": MAX_SANE_PNL_PCT,
        "min_pnl": MIN_SANE_PNL_PCT,
    }
    rows = _rows(params)
    if args.held_longer_only:
        rows = [row for row in rows if _f(row.get("shadow_held_after_qsim_min")) > 0]

    print(
        f"\nQSIM AFTER-EXIT MOVE REPORT — days={args.days} since={args.since} channel={args.channel} "
        f"lane={args.lane} variant={args.variant} shadow_variant={args.shadow_variant} "
        f"post_mins={args.post_mins:g} max_qmax={args.max_qmax:g}"
    )
    print("PnL normalized per 1 SOL. Historical after-exit uses ws/feed; forward after-exit uses qsim post-exit quote probes.\n")
    print(f"rows={len(rows)}")

    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        buckets[_bucket(row)].append(row)

    print("\nBuckets")
    print(f"{'bucket':<32} {'n':>5} {'qsim':>9} {'shadow':>9} {'avg_qpk':>8} {'avg_sh_after':>12} {'avg_ws_after':>12} {'avg_qpost':>10}")
    print("-" * 106)
    for bucket, group in sorted(buckets.items(), key=lambda item: sum(_ret(r, "shadow") - _ret(r, "qsim") for r in item[1]), reverse=True):
        qsim_sum = sum(_ret(r, "qsim") for r in group)
        shadow_sum = sum(_ret(r, "shadow") for r in group)
        qpeaks = [_f(r.get("qsim_peak")) for r in group if r.get("qsim_peak") is not None]
        sh_after = [_f(r.get("shadow_peak_after_qsim")) for r in group if r.get("shadow_peak_after_qsim") is not None]
        ws_after = [_f(r.get("max_ws_after_qsim_mult")) for r in group if r.get("max_ws_after_qsim_mult") is not None]
        qpost = [_f(r.get("max_post_qobs_mult")) for r in group if r.get("max_post_qobs_mult") is not None]
        print(
            f"{bucket:<32} {len(group):>5} {qsim_sum:>+9.2f} {shadow_sum:>+9.2f} "
            f"{(sum(qpeaks)/len(qpeaks) if qpeaks else 0):>8.2f} "
            f"{(sum(sh_after)/len(sh_after) if sh_after else 0):>12.2f} "
            f"{(sum(ws_after)/len(ws_after) if ws_after else 0):>12.2f} "
            f"{(sum(qpost)/len(qpost) if qpost else 0):>10.2f}"
        )

    print("\nLargest After-Exit Moves")
    print(
        f"{'call':>7} {'symbol':<12} {'bucket':<28} {'ch':<15} {'lane':<10} "
        f"{'q_ex':>6} {'q_pk':>6} {'sh_a':>6} {'ws_a':>6} {'qpost':>6} {'hold+':>7} {'shpk+':>7} "
        f"{'q_ret':>8} {'s_ret':>8} {'reason':<23}"
    )
    print("-" * 153)
    ranked = sorted(
        rows,
        key=lambda r: (
            max(
                _f(r.get("shadow_peak_after_qsim")),
                _f(r.get("max_ws_after_qsim_mult")),
                _f(r.get("max_post_qobs_mult")),
            )
            - (_ratio(r.get("qsim_exit"), r.get("qsim_entry")) or 0.0)
        ),
        reverse=True,
    )
    for row in ranked[: args.limit]:
        q_exit_mult = _ratio(row.get("qsim_exit"), row.get("qsim_entry"))
        q_ret = _ret(row, "qsim")
        s_ret = _ret(row, "shadow")
        reason = f"{row.get('qsim_reason')}/{row.get('shadow_reason')}"
        print(
            f"{int(row['call_id']):>7} {str(row.get('symbol') or '?')[:12]:<12} {_bucket(row)[:28]:<28} "
            f"{str(row.get('channel') or '?')[:15]:<15} {str(row.get('lane') or '?')[:10]:<10} "
            f"{_fmt(q_exit_mult, 6, 2)} {_fmt(row.get('qsim_peak'), 6, 2)} "
            f"{_fmt(row.get('shadow_peak_after_qsim'), 6, 2)} {_fmt(row.get('max_ws_after_qsim_mult'), 6, 2)} "
            f"{_fmt(row.get('max_post_qobs_mult'), 6, 2)} {_fmt(row.get('shadow_held_after_qsim_min'), 7, 1)} "
            f"{_fmt(row.get('shadow_peak_after_qsim_min'), 7, 1)} "
            f"{q_ret:>+8.2f} {s_ret:>+8.2f} {reason[:23]:<23}"
        )

    if args.detail:
        print("\nDetail Feature Columns")
        print(f"{'call':>7} {'symbol':<12} {'score':>5} {'liq':>8} {'vol/mcap':>8} {'bund':>5} {'b%':>6} {'snip':>5} {'s%':>6} {'fake':>6} {'age':>6}")
        print("-" * 92)
        for row in ranked[: args.limit]:
            print(
                f"{int(row['call_id']):>7} {str(row.get('symbol') or '?')[:12]:<12} "
                f"{_fmt(row.get('score'), 5, 0)} {_fmt(row.get('liquidity'), 8, 0)} "
                f"{_fmt(row.get('vol_mcap'), 8, 2)} {_fmt(row.get('bundle_count'), 5, 0)} "
                f"{_fmt(row.get('bundle_pct'), 6, 1)} {_fmt(row.get('sniper_count'), 5, 0)} "
                f"{_fmt(row.get('sniper_pct'), 6, 1)} {_fmt(row.get('fake_vol_pct'), 6, 1)} "
                f"{_fmt(row.get('age_min'), 6, 0)}"
            )


if __name__ == "__main__":
    main()
