"""
compare_report_live.py — Live trading vs paper Strategy A and B comparison.

Shows closed-trade metrics for Live, Paper A, and Paper B side by side.
The paper-vs-live section compares matched call_ids to measure how much the
live execution compresses vs paper (DexScreener marginal price vs Jupiter swap).

Usage:
    python3 compare_report_live.py             # all time
    python3 compare_report_live.py --today     # since UTC midnight
    python3 compare_report_live.py --days 7    # last 7 days
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta, timezone

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db


# ── Helpers ───────────────────────────────────────────────────────────────────

def _pct(n: float, d: float) -> str:
    if not d:
        return "n/a"
    return f"{n / d * 100:.1f}%"


def _sol(v) -> str:
    if v is None:
        return "n/a"
    return f"{float(v):+.4f}"


def _fmt(v, digits: int = 1) -> str:
    if v is None:
        return "n/a"
    return f"{float(v):+.{digits}f}%"


# ── Queries ───────────────────────────────────────────────────────────────────

def _load_paper(since: datetime | None, is_strategy_b: bool) -> dict:
    conn = db.get_conn()
    db.safe_rollback()
    params: list = []
    date_sql = ""
    if since:
        date_sql = "AND (tp.exit_time >= %s OR tp.exit_time IS NULL)"
        params.append(since)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE tp.status = 'closed') AS closed,
                COUNT(*) FILTER (WHERE tp.status = 'open')   AS open_pos,
                COUNT(*) FILTER (WHERE tp.status = 'closed' AND tp.pnl_sol > 0) AS winners,
                COUNT(*) FILTER (WHERE tp.status = 'closed' AND tp.pnl_sol <= 0) AS losers,
                COALESCE(SUM(tp.pnl_sol) FILTER (WHERE tp.status = 'closed'), 0) AS total_pnl,
                COALESCE(AVG(tp.pnl_pct) FILTER (WHERE tp.status = 'closed'), 0) AS avg_pct,
                MAX(tp.pnl_pct) FILTER (WHERE tp.status = 'closed') AS best_pct,
                MIN(tp.pnl_pct) FILTER (WHERE tp.status = 'closed') AS worst_pct
            FROM trading_positions tp
            WHERE tp.is_simulation = TRUE
              AND tp.is_strategy_b = %s
              {date_sql}
            """,
            [is_strategy_b] + params,
        )
        row = dict(cur.fetchone())

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                COALESCE(tp.exit_reason, 'unknown') AS reason,
                COUNT(*) AS cnt,
                COALESCE(SUM(tp.pnl_sol), 0) AS pnl
            FROM trading_positions tp
            WHERE tp.is_simulation = TRUE
              AND tp.is_strategy_b = %s
              AND tp.status = 'closed'
              {date_sql}
            GROUP BY tp.exit_reason
            ORDER BY pnl DESC
            """,
            [is_strategy_b] + params,
        )
        row["exits"] = cur.fetchall()

    return row


def _load_live(since: datetime | None) -> dict:
    conn = db.get_conn()
    db.safe_rollback()
    params: list = []
    date_sql = ""
    if since:
        date_sql = "AND (tp.exit_time >= %s OR tp.exit_time IS NULL)"
        params.append(since)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                COUNT(*) FILTER (WHERE tp.status = 'closed') AS closed,
                COUNT(*) FILTER (WHERE tp.status = 'open')   AS open_pos,
                COUNT(*) FILTER (WHERE tp.status = 'closed' AND tp.pnl_sol > 0) AS winners,
                COUNT(*) FILTER (WHERE tp.status = 'closed' AND tp.pnl_sol <= 0) AS losers,
                COALESCE(SUM(tp.pnl_sol) FILTER (WHERE tp.status = 'closed'), 0) AS total_pnl,
                COALESCE(AVG(tp.pnl_pct) FILTER (WHERE tp.status = 'closed'), 0) AS avg_pct,
                MAX(tp.pnl_pct) FILTER (WHERE tp.status = 'closed') AS best_pct,
                MIN(tp.pnl_pct) FILTER (WHERE tp.status = 'closed') AS worst_pct,
                COALESCE(SUM(tp.sol_in)  FILTER (WHERE tp.status = 'closed'), 0) AS total_sol_in,
                COALESCE(SUM(tp.sol_out) FILTER (WHERE tp.status = 'closed'), 0) AS total_sol_out
            FROM trading_positions tp
            WHERE tp.is_simulation = FALSE
              {date_sql}
            """,
            params,
        )
        row = dict(cur.fetchone())

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                COALESCE(tp.exit_reason, 'unknown') AS reason,
                COUNT(*) AS cnt,
                COALESCE(SUM(tp.pnl_sol), 0) AS pnl,
                COALESCE(AVG(tp.pnl_pct), 0) AS avg_pct
            FROM trading_positions tp
            WHERE tp.is_simulation = FALSE
              AND tp.status = 'closed'
              {date_sql}
            GROUP BY tp.exit_reason
            ORDER BY pnl DESC
            """,
            params,
        )
        row["exits"] = cur.fetchall()

    return row


def _load_paper_vs_live(since: datetime | None) -> dict:
    """
    For call_ids where both a live position and a paper A position were closed,
    compute the PnL difference to measure paper-to-live compression.
    """
    conn = db.get_conn()
    db.safe_rollback()
    params: list = []
    date_sql = ""
    if since:
        date_sql = "AND live.exit_time >= %s AND paper.exit_time >= %s"
        params.extend([since, since])

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                COUNT(*)                              AS both_traded,
                COALESCE(SUM(live.pnl_sol),  0)      AS live_pnl,
                COALESCE(SUM(paper.pnl_sol), 0)      AS paper_a_pnl,
                COALESCE(SUM(live.pnl_sol - paper.pnl_sol), 0) AS raw_delta,
                COALESCE(AVG(live.pnl_pct),  0)      AS avg_live_pct,
                COALESCE(AVG(paper.pnl_pct), 0)      AS avg_paper_pct,
                COUNT(*) FILTER (WHERE live.exit_reason != paper.exit_reason) AS diff_exit_reason,
                COUNT(*) FILTER (WHERE live.pnl_sol > paper.pnl_sol)          AS live_beats_paper,
                COUNT(*) FILTER (WHERE live.pnl_sol < paper.pnl_sol)          AS paper_beats_live
            FROM trading_positions live
            JOIN trading_positions paper
              ON paper.call_id       = live.call_id
             AND paper.is_simulation = TRUE
             AND paper.is_strategy_b = FALSE
             AND paper.status        = 'closed'
            WHERE live.is_simulation = FALSE
              AND live.status        = 'closed'
              {date_sql}
            """,
            params,
        )
        return dict(cur.fetchone())


def _load_live_channel(since: datetime | None) -> list[dict]:
    conn = db.get_conn()
    db.safe_rollback()
    params: list = []
    date_sql = ""
    if since:
        date_sql = "AND tp.exit_time >= %s"
        params.append(since)

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                COALESCE(ch.handle, 'unknown') AS channel,
                COUNT(*) AS trades,
                COUNT(*) FILTER (WHERE tp.pnl_sol > 0) AS winners,
                ROUND(SUM(tp.pnl_sol)::numeric, 4)  AS net_pnl,
                ROUND(AVG(tp.pnl_pct)::numeric, 1)  AS avg_pct,
                COUNT(*) FILTER (WHERE tp.exit_reason = 'hard_stop') AS hard_stops
            FROM trading_positions tp
            JOIN calls c ON c.id = tp.call_id
            LEFT JOIN channels ch ON ch.id = c.channel_id
            WHERE tp.is_simulation = FALSE
              AND tp.status = 'closed'
              {date_sql}
            GROUP BY ch.handle
            ORDER BY net_pnl DESC
            """,
            params,
        )
        return cur.fetchall()


# ── Printing ──────────────────────────────────────────────────────────────────

def _print_summary(live: dict, paper_a: dict, paper_b: dict, label: str) -> None:
    w = 14
    print("=" * 68)
    print(f"  LIVE vs STRATEGY A (PAPER) vs STRATEGY B (PAPER) — {label}")
    print("=" * 68)

    def row(metric: str, lv, pa, pb) -> None:
        print(f"  {metric:<26} {str(lv):>{w}} {str(pa):>{w}} {str(pb):>{w}}")

    print(f"\n  {'Metric':<26} {'Live':>{w}} {'Strategy A':>{w}} {'Strategy B':>{w}}")
    print("  " + "-" * 62)
    row("Closed trades",
        int(live["closed"]), int(paper_a["closed"]), int(paper_b["closed"]))
    row("Open positions",
        int(live["open_pos"]), int(paper_a["open_pos"]), int(paper_b["open_pos"]))
    row("Winners",
        int(live["winners"]), int(paper_a["winners"]), int(paper_b["winners"]))
    row("Losers",
        int(live["losers"]), int(paper_a["losers"]), int(paper_b["losers"]))
    row("Win rate",
        _pct(live["winners"], live["closed"]),
        _pct(paper_a["winners"], paper_a["closed"]),
        _pct(paper_b["winners"], paper_b["closed"]))
    row("Total P&L (SOL)",
        _sol(live["total_pnl"]), _sol(paper_a["total_pnl"]), _sol(paper_b["total_pnl"]))
    row("Avg P&L %",
        _fmt(live["avg_pct"]), _fmt(paper_a["avg_pct"]), _fmt(paper_b["avg_pct"]))
    row("Best trade %",
        _fmt(live["best_pct"]), _fmt(paper_a["best_pct"]), _fmt(paper_b["best_pct"]))
    row("Worst trade %",
        _fmt(live["worst_pct"]), _fmt(paper_a["worst_pct"]), _fmt(paper_b["worst_pct"]))


def _print_exit_breakdown(live: dict, paper_a: dict, paper_b: dict) -> None:
    print(f"\n  {'Exit Breakdown':<26} {'Live':>8} {'Paper A':>9} {'Paper B':>9}")
    print("  " + "-" * 54)

    reasons: set[str] = set()
    for r in live["exits"]:
        reasons.add(r["reason"])
    for r in paper_a["exits"]:
        reasons.add(r["reason"])
    for r in paper_b["exits"]:
        reasons.add(r["reason"])

    live_map    = {r["reason"]: r for r in live["exits"]}
    paper_a_map = {r["reason"]: r for r in paper_a["exits"]}
    paper_b_map = {r["reason"]: r for r in paper_b["exits"]}

    for reason in sorted(reasons):
        lv = live_map.get(reason, {}).get("cnt", 0)
        pa = paper_a_map.get(reason, {}).get("cnt", 0)
        pb = paper_b_map.get(reason, {}).get("cnt", 0)
        print(f"    {reason:<24} {int(lv):>6} {int(pa):>9} {int(pb):>9}")


def _print_live_exits_pnl(live: dict) -> None:
    print(f"\n  Live Exit PnL")
    print("  " + "-" * 54)
    print(f"    {'reason':<20} {'trades':>7} {'net_pnl':>10} {'avg%':>8}")
    for r in live["exits"]:
        print(
            f"    {r['reason']:<20} {int(r['cnt']):>7} "
            f"{_sol(r['pnl']):>10} {_fmt(r['avg_pct']):>8}"
        )


def _print_paper_vs_live(pvl: dict) -> None:
    print(f"\n  Paper A vs Live (matched call_ids)")
    print("  " + "-" * 54)
    if not pvl["both_traded"]:
        print("    No matched closed positions yet.")
        return
    n = int(pvl["both_traded"])
    print(f"    both_traded:          {n:>6}")
    print(f"    live_pnl:             {_sol(pvl['live_pnl']):>10}")
    print(f"    paper_a_pnl:          {_sol(pvl['paper_a_pnl']):>10}")
    print(f"    pnl_delta (live-A):   {_sol(pvl['raw_delta']):>10}")
    if pvl["paper_a_pnl"] and float(pvl["paper_a_pnl"]) != 0:
        compression = float(pvl["live_pnl"]) / float(pvl["paper_a_pnl"])
        print(f"    live/paper ratio:     {compression:>10.3f}x")
    print(f"    avg_live_pct:         {_fmt(pvl['avg_live_pct']):>10}")
    print(f"    avg_paper_a_pct:      {_fmt(pvl['avg_paper_pct']):>10}")
    print(f"    diff_exit_reason:     {int(pvl['diff_exit_reason']):>6}")
    print(f"    live_beats_paper:     {int(pvl['live_beats_paper']):>6}")
    print(f"    paper_beats_live:     {int(pvl['paper_beats_live']):>6}")


def _print_live_by_channel(rows: list[dict]) -> None:
    if not rows:
        return
    print(f"\n  Live by Channel")
    print("  " + "-" * 68)
    print(f"    {'channel':<22} {'trades':>7} {'winners':>8} {'win%':>6} {'net_pnl':>10} {'avg%':>8} {'hs':>4}")
    for r in rows:
        wr = _pct(r["winners"], r["trades"])
        print(
            f"    {r['channel']:<22} {int(r['trades']):>7} {int(r['winners']):>8} "
            f"{wr:>6} {_sol(r['net_pnl']):>10} {_fmt(r['avg_pct']):>8} "
            f"{int(r['hard_stops']):>4}"
        )


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Live trading vs paper Strategy A and B comparison"
    )
    parser.add_argument("--today", action="store_true", help="Since UTC midnight today")
    parser.add_argument("--days",  type=int, default=None, help="Last N days (rolling UTC)")
    args = parser.parse_args()

    if args.today:
        since = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        label = "Today"
    elif args.days:
        since = datetime.now(timezone.utc) - timedelta(days=args.days)
        label = f"Last {args.days} days"
    else:
        since = None
        label = "All time"

    live    = _load_live(since)
    paper_a = _load_paper(since, is_strategy_b=False)
    paper_b = _load_paper(since, is_strategy_b=True)
    pvl     = _load_paper_vs_live(since)
    channels = _load_live_channel(since)

    _print_summary(live, paper_a, paper_b, label)
    _print_exit_breakdown(live, paper_a, paper_b)
    _print_live_exits_pnl(live)
    _print_paper_vs_live(pvl)
    _print_live_by_channel(channels)
    print()


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
