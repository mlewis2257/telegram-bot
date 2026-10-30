"""
new_launch_scan.py — do brand-new devs' pump.fun launches get bought in the first seconds?

WHY (2026-10-30). The serial devs we audited earn ~$20/launch selling their dev buy into
sniper bots 3 s after launch; their snipers turned out to follow those specific devs. The
user's point: there are thousands of snipers, many may buy ANY new launch. The fair test
is to look at launches themselves, including fresh devs'.

HOW. Recent launches = recent transactions of pump.fun's mint authority (every creation
goes through it). For each launch (at least --min-age-s old):
  dev            the create transaction's signer; FRESH if its whole history is < --fresh-txs
  dev buy        SOL the dev spent in the create transaction (native + wrapped)
  first N s      the coin's transactions in the first --window-s seconds: OTHER wallets that
                 bought (token balance up) and the SOL they spent; whether the dev sold, and
                 the dev's net SOL on those transactions if it did
Split by fresh vs established dev and by dev-buy size, since snipers may key on either.

Read-only; hard RPC ceiling. Run:
  python3 bot/launcher/new_launch_scan.py [--launches 60] [--window-s 10] [--max-calls 900]
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import dev_launch_audit as d  # noqa: E402

MINT_AUTHORITY = "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM"
WSOL = "So11111111111111111111111111111111111111112"
LAMPORTS = 1_000_000_000


def sol_delta(tx: dict, wallet: str) -> float:
    meta = tx["meta"]
    keys = [k["pubkey"] if isinstance(k, dict) else k for k in tx["transaction"]["message"]["accountKeys"]]
    if wallet not in keys:
        return 0.0
    i = keys.index(wallet)
    v = (meta["postBalances"][i] - meta["preBalances"][i]) / LAMPORTS
    for side, sign in (("postTokenBalances", 1), ("preTokenBalances", -1)):
        v += sign * sum(float(b["uiTokenAmount"]["uiAmount"] or 0) for b in meta.get(side) or []
                        if b.get("owner") == wallet and b["mint"] == WSOL)
    return v


def token_delta(tx: dict, wallet: str, mint: str) -> float:
    meta = tx["meta"]
    pre = sum(float(b["uiTokenAmount"]["uiAmount"] or 0) for b in meta.get("preTokenBalances") or []
              if b.get("owner") == wallet and b["mint"] == mint)
    post = sum(float(b["uiTokenAmount"]["uiAmount"] or 0) for b in meta.get("postTokenBalances") or []
               if b.get("owner") == wallet and b["mint"] == mint)
    return post - pre


def signer(tx: dict) -> str | None:
    return next((k["pubkey"] for k in tx["transaction"]["message"]["accountKeys"]
                 if isinstance(k, dict) and k.get("signer")), None)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--launches", type=int, default=60)
    ap.add_argument("--window-s", type=int, default=10)
    ap.add_argument("--min-age-s", type=int, default=60)
    ap.add_argument("--fresh-txs", type=int, default=20,
                    help="a dev wallet with fewer total transactions than this is FRESH")
    ap.add_argument("--per-coin-txs", type=int, default=12, help="coin txs read inside the window")
    ap.add_argument("--max-calls", type=int, default=900)
    ap.add_argument("--fresh-only", action="store_true",
                    help="read the first seconds only for FRESH devs' launches (cheap way to sample many)")
    a = ap.parse_args()
    d._max_calls = a.max_calls
    now = time.time()
    results = []
    try:
        sigs = d.rpc("getSignaturesForAddress", [MINT_AUTHORITY, {"limit": 1000}]) or []
        sigs = [s for s in sigs if s.get("err") is None and s.get("blockTime")
                and now - s["blockTime"] >= a.min_age_s]
        # spread the sample over the window read instead of taking one burst
        step = max(1, len(sigs) // a.launches)
        for s in sigs[::step][:a.launches]:
            tx = d.get_tx(s["signature"])
            if not tx:
                continue
            logs = " ".join(tx["meta"].get("logMessages") or [])
            if "Instruction: Create" not in logs:
                continue
            dev = signer(tx)
            mints = [b["mint"] for b in tx["meta"].get("postTokenBalances") or []
                     if b["mint"] != WSOL and b["mint"].endswith("pump")]
            mint = mints[0] if mints else None
            if not dev or not mint:
                continue
            t0 = s["blockTime"]
            dev_buy = -min(0.0, sol_delta(tx, dev))
            hist = d.rpc("getSignaturesForAddress", [dev, {"limit": 100}]) or []
            fresh = len(hist) < a.fresh_txs
            if a.fresh_only and not fresh:
                results.append({"dev": dev, "fresh": False, "dev_txs": len(hist), "dev_buy": dev_buy,
                                "buyers": None, "buyer_sol": None, "dev_sold": False, "dev_net": None,
                                "n_window": None})
                continue
            csigs = d.rpc("getSignaturesForAddress", [mint, {"limit": 100}]) or []
            window = sorted((x for x in csigs if x.get("err") is None and x.get("blockTime")
                             and 0 <= x["blockTime"] - t0 <= a.window_s
                             and x["signature"] != s["signature"]), key=lambda x: x["blockTime"])
            buyers, buyer_sol, dev_sold, dev_net = set(), 0.0, False, None
            for x in window[:a.per_coin_txs]:
                ctx = d.get_tx(x["signature"])
                if not ctx:
                    continue
                who = signer(ctx)
                if who == dev:
                    if token_delta(ctx, dev, mint) < 0:
                        dev_sold = True
                        dev_net = (dev_net or 0.0) + sol_delta(ctx, dev)
                    continue
                if who and token_delta(ctx, who, mint) > 0:
                    buyers.add(who)
                    buyer_sol += max(0.0, -sol_delta(ctx, who))
            results.append({"dev": dev, "fresh": fresh, "dev_txs": len(hist), "dev_buy": dev_buy,
                            "buyers": len(buyers), "buyer_sol": buyer_sol, "dev_sold": dev_sold,
                            "dev_net": (dev_net - dev_buy) if dev_sold and dev_net is not None else None,
                            "n_window": len(window)})
    except d.Ceiling:
        print("[scan] RPC ceiling reached — partial sample")

    if not results:
        print("no launches analysed")
        return
    print(f"{len(results)} pump.fun launches analysed (first {a.window_s}s after creation); "
          f"RPC calls used {d._calls}/{d._max_calls}\n")

    print(f"launches classified: {len(results)}; fresh devs: {sum(1 for r in results if r['fresh'])} "
          f"({sum(1 for r in results if r['fresh']) / len(results) * 100:.0f}%)\n")

    def summary(title, g):
        g = [r for r in g if r["buyers"] is not None]
        if not g:
            return
        any_b = sum(1 for r in g if r["buyers"] > 0)
        sold = [r for r in g if r["dev_sold"] and r["dev_net"] is not None]
        print(f"  {title:34s} launches {len(g):3d} | bought by others in {a.window_s}s: {any_b:3d} "
              f"({any_b / len(g) * 100:3.0f}%) | median outside SOL {statistics.median(r['buyer_sol'] for r in g):6.3f} "
              f"| avg buyers {statistics.mean(r['buyers'] for r in g):4.1f}"
              + (f" | dev sold in window: {len(sold)}, median net {statistics.median(r['dev_net'] for r in sold):+.3f} SOL"
                 if sold else ""))

    print("BY DEV:")
    summary("fresh devs (< %d txs ever)" % a.fresh_txs, [r for r in results if r["fresh"]])
    summary("established devs", [r for r in results if not r["fresh"]])
    print("\nBY DEV BUY AT CREATION:")
    for lo, hi, label in ((0, 0.05, "under 0.05 SOL"), (0.05, 0.5, "0.05-0.5 SOL"), (0.5, 1.5, "0.5-1.5 SOL"),
                          (1.5, 1e9, "1.5 SOL+")):
        summary(label, [r for r in results if lo <= r["dev_buy"] < hi])
    print("\nFRESH DEVS WITH A REAL DEV BUY (0.5 SOL+):")
    summary("fresh, dev buy 0.5+ SOL", [r for r in results if r["fresh"] and r["dev_buy"] >= 0.5])
    fresh_rows = [r for r in results if r["fresh"] and r["buyers"] is not None]
    if fresh_rows:
        got = [r for r in fresh_rows if r["buyers"] > 0]
        print(f"\nFRESH-DEV LAUNCHES one by one ({len(fresh_rows)}; {len(got)} got outside buyers):")
        print(f"  {'dev buy':>8s} {'buyers':>6s} {'outside SOL':>11s} {'dev sold?':>9s} {'dev net':>8s}")
        for r in sorted(fresh_rows, key=lambda r: -r["buyer_sol"]):
            print(f"  {r['dev_buy']:8.3f} {r['buyers']:6d} {r['buyer_sol']:11.3f} "
                  f"{('yes' if r['dev_sold'] else 'no'):>9s} {('%+.3f' % r['dev_net']) if r['dev_net'] is not None else '':>8s}")
        if got:
            print(f"  among launches that got buyers: median outside SOL {statistics.median(r['buyer_sol'] for r in got):.3f}, "
                  f"mean {statistics.mean(r['buyer_sol'] for r in got):.3f}, max {max(r['buyer_sol'] for r in got):.3f}")
    print("\n'outside SOL' = SOL other wallets spent buying in the window (the money a dev could sell into).")


if __name__ == "__main__":
    main()
