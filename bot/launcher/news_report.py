"""
news_report.py — how fast do coins follow a story, and does the FIRST coin do best?

Reads news_posts (Telegram news channels + RSS, from the listener and news_recorder.py) and
new_tokens (every pump.fun creation). For each story keyword:
  keyword       a proper-noun-like word (or two-word name) from the headline, 4+ letters,
                skipped if it is a common word or if it already appeared in more than
                --bg-max new coin names in the 24 h BEFORE the post (background, not news)
  first coin    minutes from the EARLIEST post using the keyword to the first coin whose
                name or symbol contains it (coins up to 30 min BEFORE the post are shown as
                negative: the story was out before our sources had it)
  coins         how many such coins in 1 h and in --hours
  drew          SOL other buyers put in (bonding-curve real reserve minus the dev buy; one
                RPC call per coin, the first --per-story coins only; capped by --max-calls)
Summary: median minutes to the first coin, and how often the first coin drew the most.

Read-only. Run:  python3 bot/launcher/news_report.py [--since '2026-11-01'] [--hours 6] [--max-calls 300]
"""

from __future__ import annotations

import argparse
import re
import statistics
import sys
from collections import defaultdict
from datetime import timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "python_ai"))
import db  # noqa: E402
import dev_launch_audit as d  # noqa: E402
from launch_features import curve_state  # noqa: E402

STOP = set("""about above after again against also amid among another around back based because been
before being below between both breaking brings came cannot could crypto daily data despite does
during each early either even every first following from further gets give given good great have
having here high highs into just last later latest least less like likely little live long lower made
make many market markets more most move moves much must near need never news next nothing only other
over plans price prices quickly rally rate rates really record report reported reports said says
second since some still such take than that their them then there these they this those though
three through today top under until update users very wants week weekly were what when where which
while whose will with within without would year years your token tokens coin coins trading trader
traders launch launches million billion percent analysts investors exchange exchanges group groups
company companies people world global official officials government says amid after following
hack hacks hacked breaking alert update""".split())


def _alnum(s) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def keywords(title: str) -> set[str]:
    """Proper-noun-like words (capitalised or ALL CAPS) and adjacent capitalised pairs."""
    words = re.findall(r"[A-Za-z][A-Za-z0-9']+", title or "")
    out, prev_cap = set(), None
    for w in words:
        lw = _alnum(w)
        cap = w[0].isupper()
        if cap and len(lw) >= 4 and lw not in STOP:
            out.add(lw)
        if cap and prev_cap and len(prev_cap + lw) >= 6:
            out.add(prev_cap + lw)
        prev_cap = lw if cap else None
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-11-01")
    ap.add_argument("--hours", type=float, default=6)
    ap.add_argument("--bg-max", type=int, default=8)
    ap.add_argument("--per-story", type=int, default=4)
    ap.add_argument("--max-calls", type=int, default=300)
    ap.add_argument("--list", type=int, default=30)
    a = ap.parse_args()
    d._max_calls = a.max_calls
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("SELECT seen_at, source, channel, title FROM news_posts WHERE seen_at >= %s ORDER BY seen_at",
                    (a.since,))
        posts = cur.fetchall()
        cur.execute("SELECT mint, name, symbol, dev_buy_sol, created_at FROM new_tokens "
                    "WHERE created_at >= %s::timestamptz - interval '1 day' ORDER BY created_at", (a.since,))
        coins = [{"mint": m, "name": n or "", "symbol": s or "", "dev_buy": float(b or 0), "at": t,
                  "text": _alnum(n) + " " + _alnum(s)} for m, n, s, b, t in cur.fetchall()]
    if not posts or not coins:
        print(f"news posts: {len(posts)}, new coins recorded: {len(coins)} — need both (are the listener's "
              f"NEWS_CHANNELS and news_recorder.py running?)")
        return
    earliest: dict[str, tuple] = {}
    for at, src, ch, title in posts:
        for kw in keywords(title):
            if kw not in earliest:
                earliest[kw] = (at, src, ch, title)
    stories = []
    for kw, (at, src, ch, title) in earliest.items():
        bg = sum(1 for c in coins if at - timedelta(hours=24) <= c["at"] < at - timedelta(minutes=30) and kw in c["text"])
        if bg > a.bg_max:
            continue
        matched = [c for c in coins if at - timedelta(minutes=30) <= c["at"] <= at + timedelta(hours=a.hours)
                   and kw in c["text"]]
        if not matched:
            continue
        stories.append({"kw": kw, "at": at, "src": f"{src}:{ch}", "title": title or "", "coins": matched,
                        "first_min": (matched[0]["at"] - at).total_seconds() / 60,
                        "n1h": sum(1 for c in matched if c["at"] <= at + timedelta(hours=1))})
    # One headline can yield several keywords matching the same coins ('lazarus', 'lazarusgroup'):
    # keep, per headline, the keyword with the most coins and drop those whose coins it covers.
    kept, by_title = [], defaultdict(list)
    for st in stories:
        by_title[st["title"]].append(st)
    for group in by_title.values():
        group.sort(key=lambda s: -len(s["coins"]))
        covered: set[str] = set()
        for st in group:
            mints = {c["mint"] for c in st["coins"]}
            if mints <= covered:
                continue
            covered |= mints
            kept.append(st)
    stories = kept
    print(f"{len(posts)} news posts, {len(coins)} new coins recorded; {len(stories)} story keywords had coins "
          f"named after them within {a.hours:g}h (background words skipped)\n")
    if not stories:
        return
    try:
        for st in sorted(stories, key=lambda s: -len(s["coins"])):
            st["drew"] = []
            for c in st["coins"][:a.per_story]:
                real, grad = curve_state(c["mint"])
                st["drew"].append(None if real is None else (85.0 if grad else max(0.0, real - c["dev_buy"])))
    except d.Ceiling:
        print(f"[news] RPC ceiling {a.max_calls} reached — inflow shown for part of the stories\n")
    firsts = sorted(s["first_min"] for s in stories)
    after = [x for x in firsts if x >= 0]
    print(f"minutes from the story to the first coin: median {statistics.median(firsts):+.1f} "
          f"(after-story only: median {statistics.median(after) if after else float('nan'):.1f}, "
          f"under 5 min: {sum(1 for x in after if x < 5)}/{len(after)})")
    judged = [s for s in stories if len([x for x in s.get("drew", []) if x is not None]) >= 2]
    if judged:
        first_best = sum(1 for s in judged if s["drew"][0] is not None
                         and s["drew"][0] >= max(x for x in s["drew"] if x is not None))
        print(f"stories with 2+ coins measured: {len(judged)}; the FIRST coin drew the most in {first_best} "
              f"({first_best / len(judged) * 100:.0f}%)")
    print(f"\n  {'story time':11s} {'keyword':14s} {'first':>6s} {'1h':>3s} {'all':>4s}  drew (first..)        headline")
    for s in sorted(stories, key=lambda s: s["at"])[-a.list:]:
        drew = " ".join("?" if x is None else f"{x:.1f}" for x in s.get("drew", [])) or "-"
        print(f"  {s['at']:%m-%d %H:%M} {s['kw'][:14]:14s} {s['first_min']:+6.0f} {s['n1h']:3d} {len(s['coins']):4d}  "
              f"{drew:20s} {s['title'][:70]}")
    print("\nfirst = minutes from the earliest post with the keyword to the first coin named after it "
          "(negative = the coin came first). drew = SOL from other buyers, in launch order.")


if __name__ == "__main__":
    main()
