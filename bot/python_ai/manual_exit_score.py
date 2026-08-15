#!/usr/bin/env python3
"""
manual_exit_score.py — score the operator's logged exit calls against the rules.

For each row in manual_exit_calls, find the matching trade, backfill the coin's REAL
swap-price path from Helius, and compare three exits off the same entry:
    YOUR exit (at your logged time)  vs  the RULE's exit  vs  the coin's PEAK.

Answers the whole experiment: do your eyes beat the rules, by how much, and where?

  python3 manual_exit_score.py             # every logged call
  python3 manual_exit_score.py --days 14
"""
from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import sys
from collections import defaultdict
from datetime import timezone

import httpx

sys.path.insert(0, os.path.dirname(__file__))
import db
from flow_backfill_probe import _signatures, _all_swaps, _seed_sig, _mint_col


def _ts(dt):
    return (dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)).timestamp()


def _price_series(swaps):
    """(ts, price) sorted; price = SOL/token, dust filtered to keep it real."""
    pts = [(s["ts"], s["sol_size"] / s["token_amount"])
           for s in swaps
           if s.get("token_amount") and s["token_amount"] > 0 and (s.get("sol_size") or 0) >= 0.001]
    pts.sort()
    return pts


def _price_at(pts, t, win=25):
    """Robust price near time t: median within ±win s, else nearest within 120s."""
    near = [p for (ts, p) in pts if abs(ts - t) <= win]
    if near:
        return statistics.median(near)
    if not pts:
        return None
    ts0, p0 = min(pts, key=lambda x: abs(x[0] - t))
    return p0 if abs(ts0 - t) <= 120 else None


def _peak(pts, t0, t1, buk=10):
    """Robust peak in [t0,t1]: max of per-bucket median prices (ignores outlier ticks)."""
    b = defaultdict(list)
    for ts, p in pts:
        if t0 <= ts <= t1:
            b[int(ts // buk)].append(p)
    meds = [statistics.median(v) for v in b.values() if v]
    return max(meds) if meds else None


def _match_trade(conn, mint_col, call_id, symbol, called_at):
    """Prefer the exact call_id logged at post time; else the symbol's position that was
    open when the call was made."""
    with conn.cursor() as cur:
        if call_id:
            cur.execute(f"""
                SELECT tp.call_id, tk.symbol, tk.{mint_col}, tp.entry_time, tp.exit_time,
                       tp.pnl_pct, tp.exit_reason
                FROM trading_positions tp JOIN tokens tk ON tk.id = tp.token_id
                WHERE tp.call_id = %s ORDER BY tp.entry_time DESC LIMIT 1
            """, (call_id,))
            row = cur.fetchone()
            if row:
                return row
        cur.execute(f"""
            SELECT tp.call_id, tk.symbol, tk.{mint_col}, tp.entry_time, tp.exit_time,
                   tp.pnl_pct, tp.exit_reason
            FROM trading_positions tp JOIN tokens tk ON tk.id = tp.token_id
            WHERE tk.symbol ILIKE %s AND tp.entry_time <= %s
            ORDER BY tp.entry_time DESC LIMIT 1
        """, (symbol, called_at))
        return cur.fetchone()


async def _run(days):
    conn = db.get_conn(); db.safe_rollback()
    mint_col = _mint_col(conn)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT id, symbol, note, called_at, call_id
            FROM manual_exit_calls
            WHERE (%s IS NULL OR called_at >= now() - (%s || ' days')::interval)
            ORDER BY called_at
        """, (days, days))
        calls = cur.fetchall()

    if not calls:
        print("\nNo logged exit calls yet. Post a ticker to your exit-log channel first.\n")
        return

    rows = []
    async with httpx.AsyncClient(timeout=30) as client:
        for _id, sym, note, called_at, call_id in calls:
            trade = _match_trade(conn, mint_col, call_id, sym, called_at)
            if not trade:
                rows.append((sym, None, None, None, note, "no matching trade")); continue
            cid, tsym, mint, entry_t, exit_t, pnl, reason = trade
            e0 = _ts(entry_t)
            call_ts = _ts(called_at)
            ex = _ts(exit_t) if exit_t else call_ts
            end = max(ex, call_ts) + 120
            seed = _seed_sig(conn, cid, (exit_t or called_at))
            sigs = await _signatures(client, mint, int(e0 - 60), int(end), 6000, before=seed)
            pts = _price_series(await _all_swaps(client, mint, sigs))
            entry_px = _price_at(pts, e0)
            if not entry_px:
                rows.append((sym, None, None, None, note, "no entry price")); continue
            ym = (_price_at(pts, call_ts) or 0) / entry_px or None
            rm = (_price_at(pts, ex) or 0) / entry_px or None
            pm = (_peak(pts, e0, max(ex, call_ts)) or 0) / entry_px or None
            rows.append((sym, ym, rm, pm, note, reason or "-"))

    print(f"\nManual-exit scorecard — {len(rows)} calls.  mult = exit ÷ entry (real prices)\n")
    print(f"  {'you':>6} {'rule':>6} {'peak':>6} {'edge':>6}  symbol   rule / note")
    print("  " + "-" * 58)
    wins = tot = 0
    edges = []
    for sym, ym, rm, pm, note, reason in rows:
        ys = f"{ym:.2f}" if ym else "-"
        rs = f"{rm:.2f}" if rm else "-"
        pms = f"{pm:.2f}" if pm else "-"
        edge = (ym - rm) if (ym and rm) else None
        es = f"{edge:+.2f}" if edge is not None else "-"
        if edge is not None:
            tot += 1; edges.append(edge)
            if edge > 0:
                wins += 1
        print(f"  {ys:>6} {rs:>6} {pms:>6} {es:>6}  {sym[:8]:<8} {reason[:14]} {('· '+note) if note else ''}")
    if tot:
        print(f"\n  You beat the rule on {wins}/{tot} calls.  "
              f"avg edge = {statistics.mean(edges):+.3f}x  (median {statistics.median(edges):+.3f}x)")
        print("  edge > 0 = you exited higher than the rules did. Positive and consistent")
        print("  across 20-30 calls = your read is a real, encodable edge.\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=None, help="only score calls from the last N days")
    args = ap.parse_args()
    asyncio.run(_run(args.days))


if __name__ == "__main__":
    main()
