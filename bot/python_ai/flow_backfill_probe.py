#!/usr/bin/env python3
"""
flow_backfill_probe.py — reconstruct DENSE order flow for past live trades.

The live bot samples ~1 swap per few seconds, so its order_flow reads 1-2 swaps per
tick — too sparse to see structure. This probe pulls EVERY swap for a coin's trade
window from Helius history, parses each with the SAME order_flow.parse_swap the bot
uses, and prints a dense per-bucket flow timeline around the exit.

The question it answers: on the runners the -20% stop cut (Shadow, Bros) vs the rugs,
does dense flow show a "keep running" vs "dying" signal at the moment of the stop —
i.e. is giving the bot "eyes" a real project or a ghost?

READ-ONLY. Touches nothing live. Uses the Helius RPC already in SOLANA_RPC_URL.

  python3 flow_backfill_probe.py Shadow Bros Cupsey INSTAGZAM
  python3 flow_backfill_probe.py --bucket 10 --pre 180 --post 60 Shadow
"""
from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import sys
from collections import defaultdict
from datetime import timezone, timedelta

import httpx

sys.path.insert(0, os.path.dirname(__file__))
import db
import order_flow

RPC_URL = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
_SEM = asyncio.Semaphore(8)          # cap concurrent RPC calls (Helius-friendly)


async def _rpc(client: httpx.AsyncClient, method: str, params: list, tries: int = 4):
    """One JSON-RPC call with 429/5xx backoff. Returns the 'result' or None."""
    for attempt in range(tries):
        async with _SEM:
            try:
                r = await client.post(RPC_URL, json={
                    "jsonrpc": "2.0", "id": 1, "method": method, "params": params,
                })
            except Exception:
                await asyncio.sleep(0.4 * (attempt + 1))
                continue
        if r.status_code == 429 or r.status_code >= 500:
            await asyncio.sleep(0.5 * (attempt + 1))
            continue
        if r.status_code != 200:
            return None
        return r.json().get("result")
    return None


async def _signatures(client, mint: str, start_ts: int, end_ts: int, max_sigs: int,
                      before: str | None = None) -> list[str]:
    """Page getSignaturesForAddress(mint) from `before` (or newest) back past start_ts.
    Seeding `before` at a signature near the trade window avoids paging through the
    coin's entire recent history, which throttles out on still-active coins and leaves
    us with only post-exit swaps."""
    sigs: list[str] = []
    while len(sigs) < max_sigs:
        params = [mint, {"limit": 1000, **({"before": before} if before else {})}]
        rows = await _rpc(client, "getSignaturesForAddress", params)
        if not rows:
            break
        for row in rows:
            bt = row.get("blockTime")
            if bt is None:
                continue
            if bt < start_ts:
                return sigs                       # walked past the window
            if bt <= end_ts:
                sigs.append(row["signature"])
        before = rows[-1]["signature"]
        if len(rows) < 1000:
            break
    return sigs


async def _one_swap(client, sig: str, mint: str):
    tx = await _rpc(client, "getTransaction", [
        sig, {"maxSupportedTransactionVersion": 0, "encoding": "jsonParsed"},
    ])
    if not tx:
        return None
    swap = order_flow.parse_swap(tx, mint)                 # the bot's own parser
    if swap and swap.get("ts") is None:
        swap["ts"] = tx.get("blockTime")
    return swap


async def _all_swaps(client, mint: str, sigs: list[str]) -> list[dict]:
    out = await asyncio.gather(*[_one_swap(client, s, mint) for s in sigs])
    return [s for s in out if s and s.get("ts")]


def _mint_col(conn) -> str:
    """Find the mint/address column on tokens (schema varies)."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'tokens'
              AND column_name IN ('address','mint','mint_address','token_address')
            ORDER BY array_position(
                ARRAY['address','mint','mint_address','token_address'], column_name)
            LIMIT 1
        """)
        row = cur.fetchone()
    if not row:
        raise SystemExit("Could not find a mint column on tokens (address/mint/...).")
    return row[0]


def _lookup_trade(conn, symbol: str, mint_col: str):
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT tp.call_id, tk.{mint_col}, tp.entry_time, tp.exit_time,
                   tp.entry_price_fill, tp.exit_price_fill, tp.pnl_pct, tp.exit_reason
            FROM trading_positions tp
            JOIN tokens tk ON tk.id = tp.token_id
            WHERE tp.is_simulation = FALSE AND tk.symbol ILIKE %s
            ORDER BY tp.exit_time DESC LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
    if not row:
        return None
    keys = ["call_id", "mint", "entry_time", "exit_time",
            "entry_fill", "exit_fill", "pnl", "reason"]
    return dict(zip(keys, row))


def _seed_sig(conn, call_id, end_dt):
    """Latest logged swap signature at//before the trade window — used to anchor the
    Helius pagination so we don't page from 'now' through a still-active coin."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT signature FROM ws_market_observations
            WHERE call_id = %s AND signature IS NOT NULL AND observed_at <= %s
            ORDER BY observed_at DESC LIMIT 1
        """, (call_id, end_dt))
        row = cur.fetchone()
    return row[0] if row else None


def _bucketize(swaps: list[dict], bucket: int) -> dict[int, dict]:
    """Aggregate swaps into fixed-width time buckets (unix-sec // bucket)."""
    b: dict[int, dict] = defaultdict(lambda: {
        "nbuy": 0, "nsell": 0, "bvol": 0.0, "svol": 0.0,
        "btok": 0.0, "stok": 0.0, "wallets": set()})
    for s in swaps:
        k = int(s["ts"] // bucket)
        d = b[k]
        if s["side"] == "buy":
            d["nbuy"] += 1; d["bvol"] += s["sol_size"]; d["btok"] += s["token_amount"]
        else:
            d["nsell"] += 1; d["svol"] += s["sol_size"]; d["stok"] += s["token_amount"]
        if s.get("wallet"):
            d["wallets"].add(s["wallet"])
    return b


def _report(symbol, trade, swaps, bucket):
    exit_t = trade["exit_time"]
    exit_ts = exit_t.replace(tzinfo=timezone.utc).timestamp() if exit_t.tzinfo is None \
        else exit_t.timestamp()
    print(f"\n=== {symbol}  [{trade['reason']} {trade['pnl']:+.1f}%]  "
          f"mint={trade['mint'][:6]}…  swaps={len(swaps)} ===")
    if not swaps:
        print("  no swaps parsed in window — mint may not be the swap-bearing account, "
              "or Helius history is thin here.")
        return

    b = _bucketize(swaps, bucket)
    # dense price from the swaps themselves (real VWAP, bypasses the lying feed)
    base_px = None
    print(f"  {'t_rel':>7} {'px_x':>6} {'nbuy':>4} {'nsell':>5} {'netP':>6} "
          f"{'buySOL':>7} {'sellSOL':>7} {'uniqW':>5}")
    for k in sorted(b):
        d = b[k]
        tok = d["btok"] + d["stok"]
        vwap = (d["bvol"] + d["svol"]) / tok if tok else None
        if vwap and base_px is None:
            base_px = vwap
        px_x = (vwap / base_px) if (vwap and base_px) else None
        tot = d["bvol"] + d["svol"]
        netp = (d["bvol"] - d["svol"]) / tot if tot else 0.0
        t_rel = int(k * bucket - exit_ts)                 # secs relative to exit
        mark = "  <-EXIT" if -bucket <= t_rel < bucket else ""
        print(f"  {t_rel:>6}s {('%.2f'%px_x) if px_x else '   -':>6} "
              f"{d['nbuy']:>4} {d['nsell']:>5} {netp:>+6.2f} "
              f"{d['bvol']:>7.2f} {d['svol']:>7.2f} {len(d['wallets']):>5}{mark}")

    # summary: the 60s window ending at the exit
    pre = [b[k] for k in b if -60 <= (k*bucket - exit_ts) < 0]
    if pre:
        bv = sum(x["bvol"] for x in pre); sv = sum(x["svol"] for x in pre)
        nb = sum(x["nbuy"] for x in pre); ns = sum(x["nsell"] for x in pre)
        ratio = (bv / sv) if sv else float("inf")
        print(f"  --- 60s pre-exit: buys {nb}/{bv:.2f}◎  sells {ns}/{sv:.2f}◎  "
              f"buy:sell vol = {ratio:.2f}  net {'BUY-heavy' if bv>sv else 'SELL-heavy'} ---")


async def _run(symbols, bucket, pre, post, max_sigs):
    conn = db.get_conn(); db.safe_rollback()
    mint_col = _mint_col(conn)
    async with httpx.AsyncClient(timeout=30) as client:
        for sym in symbols:
            trade = _lookup_trade(conn, sym, mint_col)
            if not trade:
                print(f"\n=== {sym} === no live trade found for that symbol.")
                continue
            entry_t, exit_t = trade["entry_time"], trade["exit_time"]
            e0 = (entry_t.replace(tzinfo=timezone.utc) if entry_t.tzinfo is None else entry_t).timestamp()
            e1 = (exit_t.replace(tzinfo=timezone.utc) if exit_t.tzinfo is None else exit_t).timestamp()
            end_dt = (exit_t if exit_t.tzinfo else exit_t.replace(tzinfo=timezone.utc)) + timedelta(seconds=post)
            seed = _seed_sig(conn, trade["call_id"], end_dt)   # anchor pagination at the window
            sigs = await _signatures(client, trade["mint"], int(e0 - pre), int(e1 + post),
                                     max_sigs, before=seed)
            swaps = await _all_swaps(client, trade["mint"], sigs)
            _report(sym, trade, swaps, bucket)


async def _run_all(days: int, window: int, max_sigs: int):
    """Batch: for every live closed trade, compute two features and print sorted by the
    sell-wall spike — testing whether a SUDDEN concentrated dump (max single-bucket sell
    vol vs the coin's own baseline) isolates the rugs, where the 60s buy:sell AVERAGE
    (kept as 'r60') failed. BASE window pulls enough history to set a baseline."""
    conn = db.get_conn(); db.safe_rollback()
    mint_col = _mint_col(conn)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT tp.call_id, tk.symbol, tk.{mint_col}, tp.exit_time,
                   tp.real_peak_mcap, tp.entry_price_fill, tp.pnl_pct, tp.exit_reason
            FROM trading_positions tp JOIN tokens tk ON tk.id = tp.token_id
            WHERE tp.is_simulation = FALSE AND tp.status = 'closed'
              AND tp.exit_time IS NOT NULL
              AND tp.entry_time >= now() - (%s || ' days')::interval
            ORDER BY tp.exit_time
        """, (days,))
        rows = cur.fetchall()

    BASE, BUK = max(window, 300), 10          # 5min history for baseline, 10s buckets
    results = []
    async with httpx.AsyncClient(timeout=30) as client:
        for call_id, sym, mint, exit_t, rpeak, efill, pnl, reason in rows:
            exit_dt = exit_t if exit_t.tzinfo else exit_t.replace(tzinfo=timezone.utc)
            end = int(exit_dt.timestamp()); start = end - BASE
            seed = _seed_sig(conn, call_id, exit_dt)
            sigs = await _signatures(client, mint, start - 30, end, max_sigs, before=seed)
            swaps = [s for s in await _all_swaps(client, mint, sigs) if start <= s["ts"] <= end]
            buks: dict[int, dict] = defaultdict(lambda: {"b": 0.0, "s": 0.0})
            for s in swaps:
                buks[int(s["ts"] // BUK)]["b" if s["side"] == "buy" else "s"] += s["sol_size"]
            # 60s buy:sell average (the feature that failed)
            bv = sum(s["sol_size"] for s in swaps if s["side"] == "buy"  and s["ts"] >= end - 60)
            sv = sum(s["sol_size"] for s in swaps if s["side"] == "sell" and s["ts"] >= end - 60)
            r60 = (bv / sv) if sv > 0 else (float("inf") if bv > 0 else None)
            # sell-wall spike: biggest single-bucket SELL vol in last 120s vs median bucket vol
            totals = [v["b"] + v["s"] for v in buks.values() if (v["b"] + v["s"]) > 0]
            med = statistics.median(totals) if totals else 0.0
            peak_sell = max((v["s"] for k, v in buks.items() if k * BUK >= end - 120), default=0.0)
            spike = peak_sell / max(med, 0.1)
            peak_x = (float(rpeak) / float(efill)) if (rpeak and efill) else None
            results.append((sym, spike, peak_sell, r60, peak_x,
                            float(pnl) if pnl is not None else None, reason))

    print(f"\nSell-wall probe — {len(results)} live trades, last {days}d.")
    print("spike = biggest single 10s-bucket SELL vol in the last 120s / median bucket vol"
          " (a sudden dump).\n")
    print(f"  {'spike':>6} {'peakSell':>8} {'r60':>6} {'peak_x':>6} {'pnl%':>6}  symbol / reason")
    print("  " + "-" * 60)
    for sym, spike, peak_sell, r60, peak_x, pnl, reason in sorted(results, key=lambda r: -r[1]):
        rs = "inf" if r60 == float("inf") else ("-" if r60 is None else f"{r60:.2f}")
        px = f"{peak_x:.2f}" if peak_x else "-"
        ps = f"{pnl:+.0f}" if pnl is not None else "-"
        print(f"  {spike:>6.1f} {peak_sell:>8.1f} {rs:>6} {px:>6} {ps:>6}  {sym[:16]} / {reason}")
    print("\nRead: do the RUGS (big -pnl, peak_x that round-tripped) own the biggest spikes,")
    print("cleanly above the runners? If a spike cutoff isolates rugs without catching a")
    print("runner, it's a real (narrow) dump-exit. If duds/runners spike too, it isn't.\n")


async def _run_ath(days: int, max_sigs: int):
    """Entry-quality test: for every live trade, reconstruct the coin's swap price for
    30 min BEFORE we bought, and measure how close to its pre-entry high we entered.
    ath_frac ~1.0 = bought at the local top; pre_run = how far it had already pumped.
    The question: do near-top entries (high ath_frac after a big pre_run) dump?"""
    PRE = 1800
    conn = db.get_conn(); db.safe_rollback()
    mint_col = _mint_col(conn)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT tp.call_id, tk.symbol, tk.{mint_col}, tp.entry_time, tp.exit_time,
                   tp.real_peak_mcap, tp.entry_price_fill, tp.pnl_pct, tp.exit_reason
            FROM trading_positions tp JOIN tokens tk ON tk.id = tp.token_id
            WHERE tp.is_simulation = FALSE AND tp.status = 'closed'
              AND tp.exit_time IS NOT NULL
              AND tp.entry_time >= now() - (%s || ' days')::interval
            ORDER BY tp.exit_time
        """, (days,))
        rows = cur.fetchall()

    def _px(s):
        return (s["sol_size"] / s["token_amount"]) if s.get("token_amount") else None

    results = []
    async with httpx.AsyncClient(timeout=30) as client:
        for call_id, sym, mint, entry_t, exit_t, rpeak, efill, pnl, reason in rows:
            entry_dt = entry_t if entry_t.tzinfo else entry_t.replace(tzinfo=timezone.utc)
            exit_dt = exit_t if exit_t.tzinfo else exit_t.replace(tzinfo=timezone.utc)
            ent = int(entry_dt.timestamp())
            seed = _seed_sig(conn, call_id, exit_dt)
            sigs = await _signatures(client, mint, ent - PRE - 30, int(exit_dt.timestamp()),
                                     max_sigs, before=seed)
            swaps = await _all_swaps(client, mint, sigs)
            pre = [p for p in (_px(s) for s in swaps if ent - PRE <= s["ts"] < ent) if p]
            at_entry = [p for p in (_px(s) for s in swaps if abs(s["ts"] - ent) <= 30) if p]
            pnl_f = float(pnl) if pnl is not None else None
            peak_x = (float(rpeak) / float(efill)) if (rpeak and efill) else None
            if not pre or not at_entry:
                results.append((sym, None, None, len(pre), peak_x, pnl_f, reason)); continue
            e = statistics.median(at_entry)
            ath_frac = e / max(pre) if max(pre) else None
            pre_run = (max(pre) / min(pre)) if min(pre) else None
            results.append((sym, ath_frac, pre_run, len(pre), peak_x, pnl_f, reason))

    print(f"\nEntry-vs-ATH probe — {len(results)} live trades, last {days}d.")
    print("ath_frac = entry price / pre-entry high (~1.0 = bought the local top).")
    print("pre_run  = how far it pumped in the 30min before we bought.  (bought-top at top)\n")
    print(f"  {'ath_frac':>8} {'pre_run':>7} {'npre':>5} {'peak_x':>6} {'pnl%':>6}  symbol / reason")
    print("  " + "-" * 62)
    for sym, af, pr, npre, peak_x, pnl, reason in sorted(results, key=lambda r: -(r[1] or -1)):
        afs = f"{af:.2f}" if af else "-"
        prs = f"{pr:.1f}" if pr else "-"
        px = f"{peak_x:.2f}" if peak_x else "-"
        ps = f"{pnl:+.0f}" if pnl is not None else "-"
        print(f"  {afs:>8} {prs:>7} {npre:>5} {px:>6} {ps:>6}  {sym[:16]} / {reason}")
    print("\nRead: do the BOUGHT-THE-TOP trades (ath_frac near 1.0, big pre_run) cluster as")
    print("losers (low peak_x, big -pnl)? If entering near the pre-entry high reliably dumps,")
    print("an 'ATH proximity' entry gate is real. If winners are up there too, it isn't.\n")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("symbols", nargs="*", help="token symbols, e.g. Shadow Bros Cupsey")
    ap.add_argument("--bucket", type=int, default=10, help="seconds per flow bucket (default 10)")
    ap.add_argument("--pre", type=int, default=180, help="secs before entry to pull (default 180)")
    ap.add_argument("--post", type=int, default=90, help="secs after exit to pull (default 90)")
    ap.add_argument("--max-sigs", type=int, default=6000, dest="max_sigs",
                    help="safety cap on signatures fetched per coin (default 6000)")
    ap.add_argument("--all", type=int, default=None, metavar="DAYS",
                    help="batch mode: buy:sell ratio for EVERY live trade in the last DAYS")
    ap.add_argument("--window", type=int, default=60,
                    help="pre-exit window (secs) for the batch ratio (default 60)")
    ap.add_argument("--ath", type=int, default=None, metavar="DAYS",
                    help="entry-vs-ATH probe: how close to the pre-entry high each trade bought")
    args = ap.parse_args()
    if args.ath is not None:
        asyncio.run(_run_ath(args.ath, args.max_sigs))
        return
    if args.all is not None:
        asyncio.run(_run_all(args.all, args.window, args.max_sigs))
        return
    if not args.symbols:
        ap.error("give token symbols, or use --all DAYS for batch mode")
    asyncio.run(_run(args.symbols, args.bucket, args.pre, args.post, args.max_sigs))


if __name__ == "__main__":
    main()
