"""
launch_features.py — what do pump.fun launches that DRAW money have in common?

WHY (2026-10-30). Fresh devs' launches are often bought within seconds, but the money is
concentrated: the top 3 of 19 drew 3-5 SOL in 10 s and the rest little or nothing, and the
dev buy size was not what attracted it. Copying "dev buy, sell into the first seconds" is
breakeven unless a launch draws one of those big inflows. This looks for what they share.

HOW. Recent launches (pump.fun mint authority), aged --min-age-min to --max-age-min:
  inflow     SOL that came in from others = the bonding curve's real SOL reserve minus the
             dev's own buy (one getAccountInfo); a coin that already graduated counts as
             'graduated' (the top outcome)
  metadata   name, ticker, description and social links from the coin's metadata JSON
             (getAsset for the URI, then the JSON itself over HTTP — no RPC)
Features compared between launches that drew 1+ SOL and those that drew ~nothing: has an X
link (and whether it points at a TWEET, a profile or a community), a website, a Telegram,
a description, name length / all-caps ticker, the dev's freshness, the dev buy.

Read-only; RPC ceiling. Run:
  python3 bot/launcher/launch_features.py [--launches 200] [--max-calls 800] [--list 25]
"""

from __future__ import annotations

import argparse
import base64
import re
import sys
import time
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
import dev_launch_audit as d  # noqa: E402
from new_launch_scan import MINT_AUTHORITY, WSOL, signer, sol_delta  # noqa: E402

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
LAMPORTS = 1_000_000_000
IPFS_GATEWAYS = ("https://ipfs.io/ipfs/", "https://cloudflare-ipfs.com/ipfs/", "https://gateway.pinata.cloud/ipfs/")


def curve_state(mint: str):
    """(real SOL reserve in SOL, graduated) from the coin's bonding-curve account."""
    from solders.pubkey import Pubkey
    curve, _ = Pubkey.find_program_address([b"bonding-curve", bytes(Pubkey.from_string(mint))],
                                           Pubkey.from_string(PUMP_PROGRAM))
    res = d.rpc("getAccountInfo", [str(curve), {"encoding": "base64"}])
    v = (res or {}).get("value")
    if not v:
        return None, None
    raw = base64.b64decode(v["data"][0])
    real_sol = int.from_bytes(raw[32:40], "little") / LAMPORTS
    complete = bool(raw[48]) if len(raw) > 48 else False
    return real_sol, complete


def metadata(mint: str) -> dict:
    a = d.rpc("getAsset", {"id": mint}) or {}
    content = a.get("content") or {}
    md = content.get("metadata") or {}
    out = {"name": md.get("name") or "", "symbol": md.get("symbol") or "", "description": "",
           "twitter": "", "telegram": "", "website": ""}
    uri = content.get("json_uri") or ""
    if uri:
        urls = [uri]
        m = re.search(r"/ipfs/([A-Za-z0-9]+)", uri)
        if m:
            urls += [g + m.group(1) for g in IPFS_GATEWAYS]
        for u in urls:
            try:
                r = requests.get(u, timeout=6)
                if r.status_code == 200:
                    j = r.json()
                    for k in ("description", "twitter", "telegram", "website"):
                        out[k] = str(j.get(k) or "")
                    break
            except Exception:
                continue
    return out


def x_kind(link: str) -> str:
    l = link.lower()
    if not l:
        return "none"
    if "/status/" in l:
        return "tweet"
    if "/i/communities/" in l or "/communities/" in l:
        return "community"
    if "x.com" in l or "twitter.com" in l:
        return "profile"
    return "other"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--launches", type=int, default=200)
    ap.add_argument("--min-age-min", type=float, default=5)
    ap.add_argument("--max-age-min", type=float, default=60)
    ap.add_argument("--max-calls", type=int, default=800)
    ap.add_argument("--drew", type=float, default=1.0, help="SOL from others that counts as 'drew money'")
    ap.add_argument("--list", type=int, default=25)
    a = ap.parse_args()
    d._max_calls = a.max_calls
    now = time.time()
    rows = []
    try:
        sigs, before = [], None
        for _ in range(3):
            opts = {"limit": 1000}
            if before:
                opts["before"] = before
            page = d.rpc("getSignaturesForAddress", [MINT_AUTHORITY, opts]) or []
            sigs += page
            if not page or now - page[-1]["blockTime"] > a.max_age_min * 60:
                break
            before = page[-1]["signature"]
        sigs = [s for s in sigs if s.get("err") is None and s.get("blockTime")
                and a.min_age_min * 60 <= now - s["blockTime"] <= a.max_age_min * 60]
        step = max(1, len(sigs) // a.launches)
        for s in sigs[::step][:a.launches]:
            tx = d.get_tx(s["signature"])
            if not tx or "Instruction: Create" not in " ".join(tx["meta"].get("logMessages") or []):
                continue
            dev = signer(tx)
            mints = [b["mint"] for b in tx["meta"].get("postTokenBalances") or []
                     if b["mint"] != WSOL and b["mint"].endswith("pump")]
            if not dev or not mints:
                continue
            mint = mints[0]
            dev_buy = -min(0.0, sol_delta(tx, dev))
            real_sol, graduated = curve_state(mint)
            if real_sol is None:
                continue
            md = metadata(mint)
            inflow = max(0.0, real_sol - dev_buy) if not graduated else 85.0
            rows.append({"mint": mint, "age_min": (now - s["blockTime"]) / 60, "dev_buy": dev_buy,
                         "inflow": inflow, "graduated": graduated, **md})
    except d.Ceiling:
        print("[features] RPC ceiling reached — partial sample")
    if not rows:
        print("no launches analysed")
        return
    drew = [r for r in rows if r["inflow"] >= a.drew]
    flat = [r for r in rows if r["inflow"] < 0.1]
    print(f"{len(rows)} launches aged {a.min_age_min:g}-{a.max_age_min:g} min (RPC calls {d._calls}/{d._max_calls}); "
          f"drew {a.drew:g}+ SOL from others: {len(drew)} ({len(drew) / len(rows) * 100:.0f}%); "
          f"drew ~nothing (<0.1): {len(flat)} ({len(flat) / len(rows) * 100:.0f}%); "
          f"graduated: {sum(r['graduated'] for r in rows)}\n")

    feats = {
        "X link: tweet": lambda r: x_kind(r["twitter"]) == "tweet",
        "X link: profile": lambda r: x_kind(r["twitter"]) == "profile",
        "X link: community": lambda r: x_kind(r["twitter"]) == "community",
        "no X link": lambda r: x_kind(r["twitter"]) == "none",
        "website": lambda r: bool(r["website"]),
        "telegram": lambda r: bool(r["telegram"]),
        "description 20+ chars": lambda r: len(r["description"]) >= 20,
        "ticker ALL CAPS": lambda r: r["symbol"].isupper(),
        "name 1-2 words": lambda r: 1 <= len(r["name"].split()) <= 2,
        "dev buy 0.5+ SOL": lambda r: r["dev_buy"] >= 0.5,
    }
    print(f"  {'feature':24s} {'share of DREW':>13s} {'share of NOTHING':>16s}   drew-rate with / without")
    for name, f in feats.items():
        with_ = [r for r in rows if f(r)]
        without = [r for r in rows if not f(r)]
        rate = lambda g: (sum(1 for r in g if r["inflow"] >= a.drew) / len(g) * 100) if g else 0.0
        print(f"  {name:24s} {sum(1 for r in drew if f(r)) / max(len(drew), 1) * 100:12.0f}% "
              f"{sum(1 for r in flat if f(r)) / max(len(flat), 1) * 100:15.0f}%   "
              f"{rate(with_):5.1f}% ({len(with_)}) / {rate(without):5.1f}% ({len(without)})")
    if a.list:
        print(f"\n  top {min(a.list, len(rows))} by inflow:")
        for r in sorted(rows, key=lambda r: -r["inflow"])[:a.list]:
            amount = "GRAD" if r["graduated"] else f"{r['inflow']:5.2f}"
            print(f"    {amount:>5s} SOL  {r['symbol'][:12]:12s} "
                  f"{r['name'][:28]:28s} X:{x_kind(r['twitter']):9s} {r['twitter'][:45]}")
    print("\nA feature matters if 'drew-rate with' is far above 'without' on a decent count.")


if __name__ == "__main__":
    main()
