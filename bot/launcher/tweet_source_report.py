"""
tweet_source_report.py — do coins built on a TWEET run more, and which X accounts make runners?

WHY (2026-11-03). Runners on the Axiom feed keep linking a tweet (jimcramer, mamesukeshow, ...).
The feed only shows the ones that ran, so this measures the base rate first. Every pump.fun coin's
metadata (new_tokens.uri, an IPFS JSON) carries a `twitter` field; coins built on a tweet usually
link the tweet itself (x.com/<account>/status/<id>). No X access is needed.

Coins measured (created since --since, older than --min-age-h so they had time to be called/graduate):
  every graduated coin, every coin called by our channels, and a random --sample of the rest.
  Rates are WEIGHTED back to the full population (each sampled "rest" coin counts rest/sampled).
Link classes: tweet (status link), community (x.com/i/communities), profile (x.com/<account>), none.
From the tweet id (a snowflake) we also get the tweet's own time -> minutes from tweet to coin,
and whether a coin was the FIRST coin on that tweet among those measured.

Cost: one HTTP GET per coin to the coin's own metadata host / public IPFS gateways (no Helius, no
X). Hard cap --max-fetch per run, printed. Results cached in coin_metadata, so re-runs only fetch
new coins. Read-only apart from that cache table.

Run:  python3 bot/launcher/tweet_source_report.py [--since 2026-11-01] [--sample 800] [--max-fetch 2500]
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

import requests

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "python_ai"))
import db  # noqa: E402

GATEWAYS = ("https://ipfs.io/ipfs/", "https://dweb.link/ipfs/")
_CID_RE = re.compile(r"/ipfs/([A-Za-z0-9]+)")
_X_RE = re.compile(r"(?:twitter\.com|x\.com)/([A-Za-z0-9_]{1,15})(?:/(status|communities)/(\d+))?", re.I)
_TWITTER_EPOCH_MS = 1288834974657


def ensure_cache() -> None:
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS coin_metadata (
                mint        text PRIMARY KEY,
                ok          boolean NOT NULL,
                twitter     text,
                website     text,
                fetched_at  timestamptz NOT NULL DEFAULT now()
            )""")
    conn.commit()


def fetch_meta(uri: str | None) -> dict | None:
    """The coin's metadata JSON, or None. Tries the uri itself, then public gateways for its CID."""
    if not uri:
        return None
    urls = [uri]
    m = _CID_RE.search(uri)
    if m:
        urls += [g + m.group(1) for g in GATEWAYS if not uri.startswith(g)]
    for u in urls:
        try:
            r = requests.get(u, timeout=8)
            if r.status_code == 200:
                return r.json()
        except (requests.RequestException, ValueError):
            continue
    return None


def classify(twitter: str | None, website: str | None) -> tuple[str, str | None, int | None]:
    """(class, account, tweet_id). A status link anywhere wins; then community; then profile."""
    found = [m for s in (twitter, website) if s for m in _X_RE.finditer(s)]
    for acc, kind, num in ((m.group(1), m.group(2), m.group(3)) for m in found):
        if kind and kind.lower() == "status" and num:
            return "tweet", acc.lower(), int(num)
    for m in found:
        if m.group(1).lower() == "i" and (m.group(2) or "").lower() == "communities":
            return "community", None, None
    for m in found:
        if m.group(1).lower() not in ("i", "home", "search", "intent", "share"):
            return "profile", m.group(1).lower(), None
    return "none", None, None


def tweet_time(tweet_id: int) -> datetime:
    return datetime.fromtimestamp(((tweet_id >> 22) + _TWITTER_EPOCH_MS) / 1000, tz=timezone.utc)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-11-01")
    ap.add_argument("--min-age-h", type=float, default=6)
    ap.add_argument("--sample", type=int, default=800, help="random non-called, non-graduated coins")
    ap.add_argument("--max-fetch", type=int, default=2500, help="hard cap on metadata GETs this run")
    ap.add_argument("--workers", type=int, default=6)
    ap.add_argument("--accounts", type=int, default=30, help="rows in the account table")
    ap.add_argument("--seed", type=int, default=7)
    a = ap.parse_args()

    ensure_cache()
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT nt.mint, nt.symbol, nt.uri, nt.created_at, nt.migrated_at IS NOT NULL,
                   EXISTS (SELECT 1 FROM tokens t JOIN calls c ON c.token_id = t.id
                           WHERE t.mint_address = nt.mint),
                   cm.ok, cm.twitter, cm.website
            FROM new_tokens nt LEFT JOIN coin_metadata cm ON cm.mint = nt.mint
            WHERE nt.created_at >= %s AND nt.created_at < now() - (%s || ' hours')::interval
            ORDER BY nt.created_at""", (a.since, a.min_age_h))
        rows = [dict(zip(("mint", "symbol", "uri", "t", "grad", "called", "ok", "twitter", "website"), r))
                for r in cur.fetchall()]
    if not rows:
        print("no new_tokens rows in the window — is news_recorder.py running?")
        return
    hits = [r for r in rows if r["grad"] or r["called"]]
    rest = [r for r in rows if not (r["grad"] or r["called"])]
    random.Random(a.seed).shuffle(rest)
    sample = rest[:a.sample]
    weight = len(rest) / len(sample) if sample else 0.0
    for r in hits:
        r["w"] = 1.0
    for r in sample:
        r["w"] = weight
    measured = hits + sample

    todo = [r for r in measured if r["ok"] is None]
    if len(todo) > a.max_fetch:
        print(f"[tweets] FETCH CEILING: {len(todo)} uncached coins, fetching {a.max_fetch} "
              f"(re-run to continue — cached rows are not fetched again)")
        todo = todo[:a.max_fetch]
    print(f"[tweets] window {a.since} -> now-{a.min_age_h:g}h: {len(rows)} coins, {len(hits)} graduated/called "
          f"(all measured), {len(sample)} of {len(rest)} others sampled (weight {weight:.1f}); "
          f"fetching {len(todo)} uncached metadata files (cap {a.max_fetch})")
    if todo:
        with ThreadPoolExecutor(a.workers) as ex:
            metas = list(ex.map(lambda r: fetch_meta(r["uri"]), todo))
        with conn.cursor() as cur:
            for r, m in zip(todo, metas):
                r["ok"] = m is not None
                r["twitter"] = str(m.get("twitter") or "")[:300] if isinstance(m, dict) else None
                r["website"] = str(m.get("website") or "")[:300] if isinstance(m, dict) else None
                cur.execute("INSERT INTO coin_metadata (mint, ok, twitter, website) VALUES (%s,%s,%s,%s) "
                            "ON CONFLICT (mint) DO UPDATE SET ok=EXCLUDED.ok, twitter=EXCLUDED.twitter, "
                            "website=EXCLUDED.website, fetched_at=now()",
                            (r["mint"], r["ok"], r["twitter"], r["website"]))
        conn.commit()

    got = sorted((r for r in measured if r["ok"]), key=lambda r: r["t"])
    failed = sum(1 for r in measured if r["ok"] is False)
    print(f"[tweets] metadata read for {len(got)} coins ({failed} unreadable, skipped)\n")
    if not got:
        return
    for r in got:
        r["cls"], r["acc"], r["tid"] = classify(r["twitter"], r["website"])
        r["lag"] = ((r["t"] - tweet_time(r["tid"])).total_seconds() / 60) if r["tid"] else None

    def rates(g):
        w = sum(r["w"] for r in g)
        return (w, sum(r["w"] for r in g if r["grad"]) / w * 100 if w else 0.0,
                sum(r["w"] for r in g if r["called"]) / w * 100 if w else 0.0)

    def hit(g):
        w = sum(r["w"] for r in g)
        return sum(r["w"] for r in g if r["grad"] or r["called"]) / w * 100 if w else 0.0

    mid = got[len(got) // 2]["t"] if len(got) > 1 else None
    w_all, g_all, c_all = rates(got)
    print(f"base rate: est. {w_all:.0f} coins, graduated {g_all:.2f}%, called {c_all:.2f}%, hit (either) {hit(got):.2f}%\n")
    print(f"  {'link':10s} {'est. coins':>10s} {'share':>6s} {'grad':>7s} {'called':>7s}   {'hit 1st/2nd half':>19s}")
    for cls in ("tweet", "community", "profile", "none"):
        g = [r for r in got if r["cls"] == cls]
        if not g:
            continue
        w, gr, ca = rates(g)
        h1 = [r for r in g if mid and r["t"] < mid]
        h2 = [r for r in g if mid and r["t"] >= mid]
        hh = " / ".join(f"{hit(h):.2f}%" if h else "-" for h in (h1, h2))
        print(f"  {cls:10s} {w:10.0f} {w / w_all * 100:5.1f}% {gr:6.2f}% {ca:6.2f}%   {hh:>19s}")

    tw = [r for r in got if r["cls"] == "tweet"]
    if tw:
        print("\nTWEET COINS — minutes from the tweet to the coin:")
        bands = [(-1e9, 0, "before tweet?"), (0, 5, "0-5"), (5, 30, "5-30"), (30, 120, "30-120"),
                 (120, 1440, "2h-1d"), (1440, 1e12, "older than 1d")]
        for lo, hi, lab in bands:
            g = [r for r in tw if lo <= r["lag"] < hi]
            if g:
                w, gr, ca = rates(g)
                print(f"  {lab:14s} est. {w:7.0f}  grad {gr:6.2f}%  called {ca:6.2f}%")
        by_tweet = defaultdict(list)
        for r in tw:
            by_tweet[r["tid"]].append(r)
        multi = [sorted(v, key=lambda r: r["t"]) for v in by_tweet.values() if len(v) >= 2]
        if multi:
            firsts = [v[0] for v in multi]
            laters = [r for v in multi for r in v[1:]]
            print(f"\n  tweets with 2+ measured coins: {len(multi)}. FIRST coin called "
                  f"{rates(firsts)[2]:.2f}% / graduated {rates(firsts)[1]:.2f}%; later coins called "
                  f"{rates(laters)[2]:.2f}% / graduated {rates(laters)[1]:.2f}% (measured coins only)")

        acc = defaultdict(list)
        for r in tw:
            acc[r["acc"]].append(r)
        print(f"\nACCOUNTS whose tweets coins were built on (top {a.accounts} by graduated+called; "
              f"est. = weighted coin count, duds included):")
        print(f"  {'account':16s} {'est. coins':>10s} {'tweets':>6s} {'grad':>5s} {'called':>6s} {'hit %':>6s}  "
              f"examples")
        ranked = sorted(acc.items(), key=lambda kv: (-sum(1 for r in kv[1] if r["grad"] or r["called"]),
                                                     -sum(r["w"] for r in kv[1])))
        for name, g in ranked[:a.accounts]:
            w = sum(r["w"] for r in g)
            ng = sum(1 for r in g if r["grad"])
            nc = sum(1 for r in g if r["called"])
            nh = sum(1 for r in g if r["grad"] or r["called"])
            ex = ", ".join(sorted({r["symbol"] or "?" for r in g if r["grad"] or r["called"]})[:4])
            print(f"  {name[:16]:16s} {w:10.0f} {len({r['tid'] for r in g}):6d} {ng:5d} {nc:6d} "
                  f"{nh / w * 100:5.1f}%  {ex}")
    print("\nA link class or account matters only if its rate is well above the base rate AND holds in both "
          "halves. Small accounts (est. < 20) are anecdotes. called = any of our channels called it; hit = graduated "
          "or called. first/later coin uses measured coins only (sampled duds can be missing).")


if __name__ == "__main__":
    main()
