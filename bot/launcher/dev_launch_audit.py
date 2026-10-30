"""
dev_launch_audit.py — what does a serial pump.fun deployer REALLY net per launch?

WHY (2026-10-30). Axiom showed a dev making ~+$23 on most launches (sell the dev buy into
sniper bots seconds after launch) plus the occasional hit. Axiom's PnL is buys minus sells;
it leaves out creation fees, priority fees / tips and rent. Before building anything, this
measures the real number from the chain.

HOW. Reads the dev wallet's recent transactions (getSignaturesForAddress + getTransaction).
For every transaction the dev SIGNED, the dev's SOL balance change is the true cash effect —
fees, tips and rent included. Each transaction is attributed to the pump.fun coin whose
balance the dev's token account changed in it (classic or Token-2022). Per launch:
  created      the transaction that created the coin (pump.fun 'Create' instruction)
  dev in       SOL that left the wallet in the create/buy transactions (incl. all costs)
  dev out      SOL that came back in its sell transactions
  net          out - in (the real per-launch result)
  sold after   seconds from creation to the first sell
Creator-fee collections are counted separately (not tied to a launch).
Optional --buyers: for each launch, reads the coin's own transactions up to the dev's first
sell and counts OTHER wallets' buys and their SOL (the sniper flow being sold into).

Read-only. RPC cost is printed; --max-calls is a hard ceiling (default 400).

Run:  python3 bot/launcher/dev_launch_audit.py --dev <WALLET> [--launches 30] [--buyers]
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "python_ai"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(HERE.parent / "python_ai" / ".env")

import data_fetcher  # noqa: E402

LAMPORTS = 1_000_000_000
_calls = 0
_max_calls = 400


class Ceiling(Exception):
    pass


def rpc(method: str, params: list):
    global _calls
    if _calls >= _max_calls:
        raise Ceiling()
    _calls += 1
    r = data_fetcher._rpc_post({"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
                               timeout=25)
    if r is None or r.status_code != 200:
        return None
    j = r.json()
    if j.get("error"):
        return None
    return j.get("result")


def get_tx(sig: str):
    for ver in (1, 0):
        tx = rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": ver,
                                          "commitment": "confirmed"}])
        if tx is not None:
            return tx
    return None


def classify(tx: dict, dev: str) -> dict | None:
    """Dev's SOL change, the pump coin whose balance the dev changed, and the action."""
    meta = tx.get("meta") or {}
    if meta.get("err") is not None:
        return None
    keys = tx["transaction"]["message"]["accountKeys"]
    pubkeys = [k["pubkey"] if isinstance(k, dict) else k for k in keys]
    if dev not in pubkeys:
        return None
    i = pubkeys.index(dev)
    signer = isinstance(keys[i], dict) and keys[i].get("signer")
    sol = (meta["postBalances"][i] - meta["preBalances"][i]) / LAMPORTS
    # Wrapped SOL is SOL too: a buy paid from the dev's WSOL account barely moves its native
    # balance (the jeff dev showed 0.016 SOL in, 3.7 SOL out without this).
    wsol = "So11111111111111111111111111111111111111112"
    wpre = sum(float(b["uiTokenAmount"]["uiAmount"] or 0) for b in meta.get("preTokenBalances") or []
               if b.get("owner") == dev and b["mint"] == wsol)
    wpost = sum(float(b["uiTokenAmount"]["uiAmount"] or 0) for b in meta.get("postTokenBalances") or []
                if b.get("owner") == dev and b["mint"] == wsol)
    sol += wpost - wpre
    pre = {(b["mint"]): float(b["uiTokenAmount"]["uiAmount"] or 0)
           for b in meta.get("preTokenBalances") or [] if b.get("owner") == dev}
    post = {(b["mint"]): float(b["uiTokenAmount"]["uiAmount"] or 0)
            for b in meta.get("postTokenBalances") or [] if b.get("owner") == dev}
    mint, dtok = None, 0.0
    for m in (set(pre) | set(post)) - {"So11111111111111111111111111111111111111112"}:
        d = post.get(m, 0.0) - pre.get(m, 0.0)
        if abs(d) > abs(dtok):
            mint, dtok = m, d
    other_mint = False
    if mint is None and signer:
        # The dev signed (and paid) but its OWN token balance did not move: buys into other
        # wallets, transfers, bundles. Attribute to the coin whose balance moved most for
        # anyone, so the cost is not lost (it was: the jeff dev showed 0.01 SOL in, 5 SOL out).
        allpre = defaultdict(float)
        allpost = defaultdict(float)
        for b in meta.get("preTokenBalances") or []:
            allpre[b["mint"]] += float(b["uiTokenAmount"]["uiAmount"] or 0)
        for b in meta.get("postTokenBalances") or []:
            allpost[b["mint"]] += float(b["uiTokenAmount"]["uiAmount"] or 0)
        moved = {m: abs(allpost.get(m, 0.0) - allpre.get(m, 0.0)) for m in set(allpre) | set(allpost)}
        moved = {m: v for m, v in moved.items()
                 if v > 0 and m != "So11111111111111111111111111111111111111112"}
        if moved:
            mint = max(moved, key=moved.get)
            other_mint = True
    logs = " ".join(meta.get("logMessages") or [])
    if "DistributeFeeToHolders" in logs:
        action = "fee_payout"          # the dev sharing creator fees with holders (costs SOL)
    elif "Instruction: Create" in logs:
        action = "create"
    elif "CollectCreatorFee" in logs or "CollectCoinCreatorFee" in logs:
        action = "fee_claim"
    elif dtok > 0:
        action = "buy"
    elif dtok < 0:
        action = "sell"
    elif other_mint:
        action = "for_others"          # dev paid; tokens moved in other wallets (bundles/transfers)
    else:
        action = "other"
    toks = {m: post.get(m, 0.0) - pre.get(m, 0.0) for m in (set(pre) | set(post))
            if m != "So11111111111111111111111111111111111111112" and abs(post.get(m, 0.0) - pre.get(m, 0.0)) > 0}
    return {"sig": tx["transaction"]["signatures"][0], "t": tx.get("blockTime"), "sol": sol,
            "mint": mint, "dtok": dtok, "action": action, "signer": bool(signer), "toks": toks}


def coin_buyers(mint: str, t_create: int, t_until: int, dev: str, limit_txs: int) -> tuple[int, int, float]:
    """(wallets that bought, buy txs, SOL they spent) between creation and t_until."""
    # Newest first, and these coins get many transactions (and failed bot spam) after the dev
    # sells: page back until the creation is reached, keep only the window that matters.
    sigs, before = [], None
    for _ in range(4):
        opts = {"limit": 1000}
        if before:
            opts["before"] = before
        page = rpc("getSignaturesForAddress", [mint, opts]) or []
        sigs += page
        if not page or len(page) < 1000 or (page[-1].get("blockTime") or 0) <= t_create:
            break
        before = page[-1]["signature"]
    sigs = sorted((s for s in sigs if s.get("blockTime") and t_create <= s["blockTime"] <= t_until
                   and s.get("err") is None), key=lambda s: s["blockTime"])
    wallets, n, spent = set(), 0, 0.0
    for s in sigs[:limit_txs]:
        tx = get_tx(s["signature"])
        if not tx:
            continue
        meta = tx.get("meta") or {}
        keys = tx["transaction"]["message"]["accountKeys"]
        signer = next((k["pubkey"] for k in keys if isinstance(k, dict) and k.get("signer")), None)
        if not signer or signer == dev:
            continue
        pre = {b["mint"]: float(b["uiTokenAmount"]["uiAmount"] or 0)
               for b in meta.get("preTokenBalances") or [] if b.get("owner") == signer}
        post = {b["mint"]: float(b["uiTokenAmount"]["uiAmount"] or 0)
                for b in meta.get("postTokenBalances") or [] if b.get("owner") == signer}
        if post.get(mint, 0.0) - pre.get(mint, 0.0) > 0:
            pk = [k["pubkey"] if isinstance(k, dict) else k for k in keys]
            j = pk.index(signer)
            spent += max(0.0, (meta["preBalances"][j] - meta["postBalances"][j]) / LAMPORTS)
            wallets.add(signer)
            n += 1
    return len(wallets), n, spent, sorted(wallets)


def main() -> None:
    global _max_calls
    ap = argparse.ArgumentParser()
    ap.add_argument("--dev", required=True)
    ap.add_argument("--launches", type=int, default=30)
    ap.add_argument("--max-calls", type=int, default=400)
    ap.add_argument("--buyers", action="store_true", help="also count other wallets' buys before the dev sold")
    ap.add_argument("--buyer-txs", type=int, default=15, help="per launch, at most this many coin txs read")
    a = ap.parse_args()
    _max_calls = a.max_calls
    dev = a.dev.strip()
    sol_usd = data_fetcher.get_sol_price_usd() or 0.0

    rows, launches, before = [], [], None
    extra = 15
    try:
        while len(launches) < a.launches or extra >= 0:
            opts = {"limit": 200}
            if before:
                opts["before"] = before
            sigs = rpc("getSignaturesForAddress", [dev, opts])
            if not sigs:
                break
            for s in sigs:
                if s.get("err") is not None:
                    continue
                tx = get_tx(s["signature"])
                if not tx:
                    continue
                c = classify(tx, dev)
                if c:
                    rows.append(c)
                    if c["action"] == "create" and c["mint"] and c["mint"] not in launches \
                            and len(launches) < a.launches:
                        launches.append(c["mint"])
                        if len(launches) == a.launches:
                            extra = 15          # the oldest launch's quote-token purchase can
                                                # sit just past it (same second, listed later)
                if len(launches) >= a.launches:
                    extra -= 1
                    if extra < 0:
                        break
            before = sigs[-1]["signature"]
            if len(sigs) < 200:
                break
    except Ceiling:
        print(f"[audit] stopped at the {_max_calls}-call ceiling")

    # Quote tokens. Some devs launch coins paired with another token (the jeff dev: HYPE):
    # SOL -> HYPE, HYPE buys the launch, the coin is sold back for SOL. Every non-launch token
    # the dev acquires with SOL gets a running SOL cost basis; when a launch transaction spends
    # (or returns) it, that SOL value is charged to (or credited to) the launch.
    launched = set(launches)
    basis = defaultdict(lambda: [0.0, 0.0])          # quote mint -> [sol spent, units]
    unpriced = 0
    # Pass 1: average SOL cost of each quote token over everything read (a purchase and the
    # launch that spends it often land in the same second, so order cannot be relied on).
    for r in rows:
        if not any(m in launched for m in r["toks"]):
            for q, v in r["toks"].items():
                if v > 0 and r["sol"] < 0:
                    basis[q][0] += -r["sol"]
                    basis[q][1] += v
    # Pass 2: charge / credit launch transactions at that cost.
    for r in rows:
        coin_toks = {m: v for m, v in r["toks"].items() if m in launched}
        quote_toks = {m: v for m, v in r["toks"].items() if m not in launched}
        r["sol_adj"] = r["sol"]
        if not coin_toks:
            continue
        for q, v in quote_toks.items():               # a launch tx paid or paid out in a quote token
            spent, units = basis[q]
            if units <= 0:
                unpriced += 1
                continue
            r["sol_adj"] += v * (spent / units)       # v<0 spent: cost; v>0 received: value
        if r["mint"] not in launched:
            r["mint"] = max(coin_toks, key=lambda m: abs(coin_toks[m]))
    if unpriced:
        print(f"[audit] {unpriced} launch transaction(s) used a quote token with no SOL purchase seen "
              f"in the history read — those launches are UNDER-costed; read further back (--max-calls)")

    by_mint = defaultdict(list)
    fee_claims, fee_payouts = [], []
    for r in rows:
        if r["action"] == "fee_claim":
            fee_claims.append(r)
        elif r["action"] == "fee_payout":
            fee_payouts.append(r)
        elif r["mint"]:
            by_mint[r["mint"]].append(r)
    results = []
    for mint in launches:
        txs = sorted(by_mint.get(mint, []), key=lambda r: r["t"] or 0)
        created = next((r for r in txs if r["action"] == "create"), None)
        if not created:
            continue
        sells = [r for r in txs if r["action"] == "sell"]
        dev_in = -sum(r["sol_adj"] for r in txs if r["sol_adj"] < 0)
        dev_out = sum(r["sol_adj"] for r in txs if r["sol_adj"] > 0)
        n_others = sum(1 for r in txs if r["action"] == "for_others")
        res = {"mint": mint, "t": created["t"], "in": dev_in, "out": dev_out, "net": dev_out - dev_in,
               "for_others": n_others,
               "n_tx": len(txs), "sold_after": (sells[0]["t"] - created["t"]) if sells else None,
               "open": not sells, "buyers": None}
        if a.buyers and sells:
            try:
                res["buyers"] = coin_buyers(mint, created["t"], sells[0]["t"], dev, a.buyer_txs)
            except Ceiling:
                print(f"[audit] ceiling reached while counting buyers")
                a.buyers = False
        results.append(res)
    if not results:
        kinds = defaultdict(int)
        for r in rows:
            kinds[r["action"]] += 1
        print(f"no launches found for {dev} in {len(rows)} transactions read (RPC calls used: {_calls}); "
              f"types: {dict(kinds)}. Raise --max-calls to read further back.")
        return

    print(f"dev {dev}: {len(results)} launches read (RPC calls used: {_calls}/{_max_calls}); "
          f"SOL ${sol_usd:.2f}\n")
    hdr = f"  {'created (UTC)':16s} {'coin':10s} {'dev in':>9s} {'dev out':>9s} {'net SOL':>9s} {'net $':>8s} {'sold after':>10s}"
    if a.buyers:
        hdr += f" {'buyers':>7s} {'their SOL':>9s}"
    print(hdr)
    for r in sorted(results, key=lambda r: r["t"]):
        when = datetime.fromtimestamp(r["t"], timezone.utc).strftime("%m-%d %H:%M:%S")
        sold = "still held" if r["open"] else f"{r['sold_after']:.0f}s"
        line = (f"  {when:16s} {r['mint'][:8] + '..':10s} {r['in']:9.4f} {r['out']:9.4f} {r['net']:+9.4f} "
                f"{r['net'] * sol_usd:+8.2f} {sold:>10s}" + (f"  [{r['for_others']} tx for other wallets]"
                                                            if r.get('for_others') else ""))
        if a.buyers and r["buyers"]:
            line += f" {r['buyers'][0]:7d} {r['buyers'][2]:9.3f}"
        print(line)
    closed = [r for r in results if not r["open"]]
    nets = [r["net"] for r in closed]
    if nets:
        span_h = (max(r["t"] for r in results) - min(r["t"] for r in results)) / 3600 or 1
        print(f"\n  closed launches {len(closed)}: profitable {sum(1 for n in nets if n > 0)} "
              f"({sum(1 for n in nets if n > 0) / len(nets) * 100:.0f}%), median net {statistics.median(nets):+.4f} SOL "
              f"(${statistics.median(nets) * sol_usd:+.2f}), mean {statistics.mean(nets):+.4f} SOL, "
              f"total {sum(nets):+.4f} SOL (${sum(nets) * sol_usd:+.2f})")
        print(f"  dev in per launch (cost of a launch incl. its buy): median {statistics.median(r['in'] for r in closed):.4f} SOL; "
              f"sold after: median {statistics.median(r['sold_after'] for r in closed):.0f}s")
        print(f"  pace: {len(results) / span_h:.1f} launches/hour over {span_h:.1f}h -> "
              f"~{sum(nets) / span_h:+.3f} SOL/hour (${sum(nets) / span_h * sol_usd:+.2f}/h) from launches")
    t_all = [r["t"] for r in rows if r["t"]]
    if t_all:
        span = (max(t_all) - min(t_all)) / 3600
        print(f"\n  wallet transactions read: {len(rows)} covering {span:.1f}h; by type: "
              + ", ".join(f"{k}={v}" for k, v in sorted(defaultdict(int, {a: sum(1 for r in rows if r['action'] == a)
                                                                         for a in {r['action'] for r in rows}}).items())))
    if fee_claims:
        print(f"  creator-fee COLLECTIONS (income): {len(fee_claims)}, {sum(r['sol'] for r in fee_claims):+.4f} SOL")
    if fee_payouts:
        print(f"  fee PAYOUTS to holders (cost): {len(fee_payouts)}, {sum(r['sol'] for r in fee_payouts):+.4f} SOL")
    seen = defaultdict(int)
    with_buyers = [r for r in results if r.get("buyers")]
    for r in with_buyers:
        for w in r["buyers"][3]:
            seen[w] += 1
    if seen:
        print(f"\n  wallets that bought before the dev sold, and on how many of {len(with_buyers)} launches:")
        for w, k in sorted(seen.items(), key=lambda kv: -kv[1])[:10]:
            print(f"    {w}  {k}")
    print("\nnet = the dev wallet's real SOL change on that coin's transactions (all fees, tips, rent "
          "included). 'still held' = no sell seen yet in what was read.")


if __name__ == "__main__":
    main()
