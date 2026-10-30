"""
sniper_targets.py — which launches do a set of sniper wallets buy, and from which devs?

WHY (2026-10-30). The SWAN dev's launches are bought within seconds by the same 3 wallets.
Open question: do those bots snipe launches from ANY dev (then a new launcher could attract
them by copying the pattern) or only from a few established devs (then the dev's ~$20/launch
floor depends on a following a new wallet would not have)?

HOW. For each sniper wallet, its recent transactions; every coin it BOUGHT (its token balance
went up). For each coin: its creation transaction (oldest signature, paging back up to
--pages); seconds from creation to the sniper's first buy; who created it (the create tx's
signer); and how established that creator is (its transaction count up to 1000 and the
oldest time in that window — a wallet with < 1000 txs shows its true age).
A buy within --snipe-secs of creation is a LAUNCH SNIPE; later buys are ordinary trades.

Read-only, hard RPC ceiling (--max-calls). Run:
  python3 bot/launcher/sniper_targets.py --wallets A,B,C [--txs 120] [--max-calls 700]
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import dev_launch_audit as d  # noqa: E402  (shares the RPC helpers and ceiling)


def oldest_signature(mint: str, pages: int):
    before, oldest = None, None
    for _ in range(pages):
        opts = {"limit": 1000}
        if before:
            opts["before"] = before
        page = d.rpc("getSignaturesForAddress", [mint, opts]) or []
        if not page:
            break
        oldest = page[-1]
        before = page[-1]["signature"]
        if len(page) < 1000:
            return oldest, True
    return oldest, False


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--wallets", required=True)
    ap.add_argument("--txs", type=int, default=120, help="recent transactions read per sniper wallet")
    ap.add_argument("--pages", type=int, default=2, help="signature pages to page back per coin")
    ap.add_argument("--snipe-secs", type=int, default=10)
    ap.add_argument("--max-calls", type=int, default=700)
    a = ap.parse_args()
    d._max_calls = a.max_calls
    wallets = [w.strip() for w in a.wallets.split(",") if w.strip()]

    buys: dict[str, dict] = {}          # mint -> {first buy time, wallets}
    try:
        for w in wallets:
            sigs = d.rpc("getSignaturesForAddress", [w, {"limit": a.txs}]) or []
            for s in sigs:
                if s.get("err") is not None:
                    continue
                tx = d.get_tx(s["signature"])
                if not tx:
                    continue
                meta = tx["meta"]
                pre = {b["mint"]: float(b["uiTokenAmount"]["uiAmount"] or 0)
                       for b in meta.get("preTokenBalances") or [] if b.get("owner") == w}
                post = {b["mint"]: float(b["uiTokenAmount"]["uiAmount"] or 0)
                        for b in meta.get("postTokenBalances") or [] if b.get("owner") == w}
                for m in post:
                    if m == "So11111111111111111111111111111111111111112":
                        continue
                    if post[m] - pre.get(m, 0.0) > 0:
                        e = buys.setdefault(m, {"t": s["blockTime"], "wallets": set()})
                        e["t"] = min(e["t"], s["blockTime"])
                        e["wallets"].add(w[:6])
    except d.Ceiling:
        print("[sniper] RPC ceiling reached while reading sniper transactions")

    print(f"{len(wallets)} sniper wallets, {len(buys)} distinct coins bought in their last {a.txs} txs each\n")
    creators: dict[str, dict] = {}
    rows = []
    try:
        for mint, e in sorted(buys.items(), key=lambda kv: -kv[1]["t"]):
            oldest, complete = oldest_signature(mint, a.pages)
            if not oldest or not complete:
                rows.append((mint, e, None, None, "busy coin (history longer than read): not a launch snipe"))
                continue
            secs = e["t"] - oldest["blockTime"]
            if secs > a.snipe_secs:
                rows.append((mint, e, secs, None, "bought later: ordinary trade"))
                continue
            tx = d.get_tx(oldest["signature"])
            if not tx:
                continue
            creator = next((k["pubkey"] for k in tx["transaction"]["message"]["accountKeys"]
                            if isinstance(k, dict) and k.get("signer")), None)
            if creator and creator not in creators:
                cs = d.rpc("getSignaturesForAddress", [creator, {"limit": 1000}]) or []
                creators[creator] = {"n": len(cs),
                                     "oldest": cs[-1]["blockTime"] if cs else None,
                                     "launches_sniped": 0}
            if creator:
                creators[creator]["launches_sniped"] += 1
            rows.append((mint, e, secs, creator, "LAUNCH SNIPE"))
    except d.Ceiling:
        print("[sniper] RPC ceiling reached while checking coins — partial results")

    snipes = [r for r in rows if r[4] == "LAUNCH SNIPE"]
    later = sum(1 for r in rows if r[4] != "LAUNCH SNIPE")
    print(f"launch snipes (bought within {a.snipe_secs}s of creation): {len(snipes)}; "
          f"ordinary trades / busy coins: {later}; coins not checked (ceiling): {len(buys) - len(rows)}\n")
    if creators:
        now = datetime.now(timezone.utc).timestamp()
        print(f"  {'creator':46s} {'sniped':>6s} {'creator txs':>11s} {'creator history spans':>22s}")
        for c, info in sorted(creators.items(), key=lambda kv: -kv[1]["launches_sniped"]):
            n = info["n"]
            age = f"{(now - info['oldest']) / 3600:.0f}h" if info["oldest"] else "?"
            span = ("whole life: " + age) if n < 1000 else ("last 1000 txs: " + age)
            print(f"  {c:46s} {info['launches_sniped']:6d} {('1000+' if n >= 1000 else n):>11} {span:>22s}")
        fresh = [c for c, i in creators.items() if i["n"] < 50]
        print(f"\n  distinct creators sniped: {len(creators)}; FRESH creators (< 50 txs ever): {len(fresh)}, "
              f"getting {sum(creators[c]['launches_sniped'] for c in fresh)} of {len(snipes)} snipes")
    print(f"\nRPC calls used: {d._calls}/{d._max_calls}")
    print("Read: if many snipes go to FRESH creators, these bots buy new devs' launches and a new launcher "
          "could attract them. If nearly all go to a few creators with long histories, they follow devs.")


if __name__ == "__main__":
    main()
