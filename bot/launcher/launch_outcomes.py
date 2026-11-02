"""
launch_outcomes.py — which launches get traction? The data side of "don't launch blind".

Every pump.fun creation is in new_tokens (news_recorder.py, PumpPortal free feed). Two outcome
labels, both free (no RPC):
  called     a channel we listen to (solearlytrending / solwhaletrending / solhousesignal ...)
             posted the coin — it reached roughly $30K+ and got attention. Available for every
             coin the recorder has seen.
  graduated  the coin migrated off the bonding curve (subscribeMigration, recorded from
             2026-11-02). Only coins created after the first recorded graduation are scored.

Each launch-time feature is shown as: launches, called %, graduated %, and the called % in the
first and second half of the window — a feature only matters if it holds in both halves.
Features (all knowable BEFORE launching):
  news       name/symbol contains a keyword from a news headline posted in the 6 h before
  first      first coin with this name in the window vs a copy of an earlier one
  stock      name looks like a stock / big company (the tokenised-stock meme trend)
  dev_buy    SOL the dev put in at creation
  dev_prior  launches by the same dev earlier in the window
  hour       UTC hour of creation
  ticker     all-caps ticker, ticker length

Read-only, no RPC. Run on the VPS:
  python3 bot/launcher/launch_outcomes.py [--since 2026-10-31] [--min-age-hours 6]
"""

from __future__ import annotations

import argparse
import bisect
import re
import sys
from collections import Counter, defaultdict
from datetime import timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "python_ai"))
import db  # noqa: E402
from news_report import keywords  # noqa: E402

STOCKS = """nvidia nvda tesla tsla apple aapl amazon amzn google googl alphabet microsoft msft meta
facebook netflix nflx spacex openai anthropic palantir pltr coinbase coin robinhood hood
microstrategy mstr gamestop gme amc cocacola coca coke pepsi mcdonalds walmart disney nike
boeing intel amd broadcom oracle salesforce uber airbnb paypal visa mastercard jpmorgan
blackrock berkshire ford toyota samsung sony nintendo starbucks costco""".split()
_STOCK_RE = re.compile("|".join(sorted(STOCKS, key=len, reverse=True)))


def _alnum(s) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def load(since: str, min_age_h: float):
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT min(migrated_at) FROM new_tokens")
        grad_from = cur.fetchone()[0]
        cur.execute(
            """
            SELECT nt.mint, nt.name, nt.symbol, nt.dev, nt.dev_buy_sol, nt.created_at, nt.migrated_at,
                   EXISTS (SELECT 1 FROM tokens t JOIN calls c ON c.token_id = t.id
                            WHERE t.mint_address = nt.mint) AS called
            FROM new_tokens nt
            WHERE nt.created_at >= %s AND nt.name IS NOT NULL
              AND nt.created_at < now() - (%s || ' hours')::interval
            ORDER BY nt.created_at
            """, (since, min_age_h))
        coins = [dict(zip(("mint", "name", "symbol", "dev", "dev_buy", "t", "migrated_at", "called"), r))
                 for r in cur.fetchall()]
        # seen_at = when WE had it (an RSS published_at can predate our copy)
        cur.execute("SELECT seen_at, coalesce(title, left(body, 300)) FROM news_posts "
                    "WHERE seen_at >= %s::timestamptz - interval '6 hours' ORDER BY 1", (since,))
        posts = cur.fetchall()
    return coins, posts, grad_from


def news_flags(coins, posts) -> None:
    """coin['news'] = a headline keyword from the prior 6 h appears in its name/symbol."""
    kw_events = sorted((t, kw) for t, title in posts if t for kw in keywords(title) if len(kw) >= 4)
    times = [t for t, _ in kw_events]
    for c in coins:
        lo = bisect.bisect_left(times, c["t"] - timedelta(hours=6))
        hi = bisect.bisect_right(times, c["t"])
        text = _alnum(c["name"]) + " " + _alnum(c["symbol"])
        c["news"] = any(kw in text for _, kw in kw_events[lo:hi])


def features(coins) -> None:
    seen_name: Counter = Counter()
    dev_count: Counter = Counter()
    for c in coins:
        key = _alnum(c["symbol"]) or _alnum(c["name"])
        c["first"] = "first of its name" if seen_name[key] == 0 else "copy of an earlier name"
        seen_name[key] += 1
        n = dev_count[c["dev"]]
        c["dev_prior"] = "0" if n == 0 else "1-4" if n < 5 else "5-49" if n < 50 else "50+"
        dev_count[c["dev"]] += 1
        b = float(c["dev_buy"] or 0)
        c["dev_buy_b"] = ("0" if b <= 0 else "<0.5" if b < 0.5 else "0.5-1" if b < 1
                          else "1-3" if b < 3 else "3+")
        c["stock"] = bool(_STOCK_RE.search(_alnum(c["name"]) + " " + _alnum(c["symbol"])))
        h = c["t"].hour
        c["hour"] = f"{h // 6 * 6:02d}-{h // 6 * 6 + 5:02d} UTC"
        sym = c["symbol"] or ""
        c["ticker"] = ("ALL CAPS" if sym.isupper() else "mixed/lower") + (
            ", 1-4 chars" if len(sym) <= 4 else ", 5-8 chars" if len(sym) <= 8 else ", 9+ chars")


def table(coins, key, title, grad_ok) -> None:
    mid = coins[len(coins) // 2]["t"] if coins else None
    groups = defaultdict(list)
    for c in coins:
        groups[c[key]].append(c)
    print(f"\n{title}")
    print(f"  {'value':26s} {'launches':>8s} {'called':>7s} {'grad':>6s}   {'called 1st/2nd half':>19s}")
    for val, g in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        n = len(g)
        called = sum(c["called"] for c in g) / n * 100
        gg = [c for c in g if grad_ok(c)]
        grad = f"{sum(c['migrated_at'] is not None for c in gg) / len(gg) * 100:5.2f}%" if gg else "    -"
        h1 = [c for c in g if c["t"] < mid]
        h2 = [c for c in g if c["t"] >= mid]
        p = lambda xs: f"{sum(c['called'] for c in xs) / len(xs) * 100:.2f}%" if xs else "-"
        print(f"  {str(val):26s} {n:8d} {called:6.2f}% {grad:>6s}   {p(h1):>9s} / {p(h2):<8s}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-10-31")
    ap.add_argument("--min-age-hours", type=float, default=6.0,
                    help="skip coins younger than this (not had time to be called/graduate)")
    a = ap.parse_args()
    coins, posts, grad_from = load(a.since, a.min_age_hours)
    if not coins:
        print("no launches in the window")
        return
    news_flags(coins, posts)
    features(coins)
    grad_ok = (lambda c: grad_from is not None and c["t"] >= grad_from)
    n_called = sum(c["called"] for c in coins)
    gg = [c for c in coins if grad_ok(c)]
    print(f"{len(coins):,} launches {coins[0]['t']:%m-%d %H:%M} -> {coins[-1]['t']:%m-%d %H:%M} UTC; "
          f"called by our channels: {n_called} ({n_called / len(coins) * 100:.2f}%); "
          + (f"graduated: {sum(c['migrated_at'] is not None for c in gg)} of {len(gg)} scored "
             f"({sum(c['migrated_at'] is not None for c in gg) / len(gg) * 100:.2f}%)" if gg
             else "graduation not recorded yet (news-recorder restart with subscribeMigration)"))
    print(f"news headlines in window: {len(posts)}")
    for key, title in (("news", "NEWS: name matches a headline keyword from the prior 6 h"),
                       ("first", "FIRST vs COPY of the name (within the window)"),
                       ("stock", "STOCK / big-company name"),
                       ("dev_buy_b", "DEV BUY at creation, SOL"),
                       ("dev_prior", "DEV's earlier launches in the window"),
                       ("hour", "HOUR created"),
                       ("ticker", "TICKER style")):
        table(coins, key, title, grad_ok)
    print("\nA feature matters only if its called/graduated rate is well above the base rate on a decent "
          "number of launches AND in both halves. Called = reached a channel (~$30K+); it is not profit.")


if __name__ == "__main__":
    main()
