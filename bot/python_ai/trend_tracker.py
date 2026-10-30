"""
trend_tracker.py — which themes are pulling coins and volume RIGHT NOW?

WHY (2026-10-29). Two uses. Trading: know which metas are live (the kimchi wave of Oct 21-24
was invisible to the bot until we looked back). Launching: a transparent launch only earns
real creator fees if it lands on a theme people are actively buying.

WHAT. Every call on every channel the bot listens to (calls + tokens), grouped by THEME: a
word of 4+ letters that appears inside coins' symbols or names. Substring matching, so
'kimchi' groups KIMCHI, ogkimchi, GKIMCHI and KIMCHICITY. For each theme in the last --hours:
  coins       distinct coins called with it            momentum   coins now vs its average
  channels    how many channels called them                       per window over --baseline h
  ran 2x      coins whose shadow peak reached 2x        best      highest peak mcap reached
  first seen  when the theme first appeared in the last 72h ('NEW' = inside this window)
Outcomes come from shadow (follows every call on every channel; feed-priced, so it
OVERSTATES; used for ranking only). Peaks above $1B or 200x are treated as bad data.

EVALUATE (--evaluate). Is a hot theme still worth anything a few hours later? Walking back
over --days in steps of --hours: take the top themes at each step, then look at coins with
those themes called in the NEXT window: how many, and how often they ran 2x, against all
other coins called in that window. If hot themes keep beating the rest, the tracker is
useful for picking launch topics and for trading; if not, trends burn out too fast to use.

Read-only. Run on the VPS:  python3 trend_tracker.py [--hours 6] [--baseline 24] [--top 25]
                            python3 trend_tracker.py --evaluate [--days 7] [--hours 6]
"""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import db

STOP = {
    "coin", "token", "the", "official", "solana", "meme", "memes", "pump", "pumpfun", "with",
    "this", "that", "from", "fun", "just", "real", "community", "dollar", "money", "capital",
    "project", "final", "first", "world", "time", "life", "super", "mega", "based", "only",
    "your", "their", "have", "will", "what", "when", "best", "good", "make", "like", "more",
    "inu", "coins", "tokens", "launch", "chain", "network", "protocol", "finance", "labs",
}
BAD_PEAK_MCAP = 1e9
# A theme is TRENDING only if it is well above its own normal level: words that are always
# around (baby, pepe, trump, doge, or any word common in names) otherwise top every list. The
# first test of this tool ranked such a background word first at every step.
HOT_MOMENTUM = 2.0
BAD_PEAK_MULT = 200.0


def _alnum(s) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def candidate_words(symbol, name) -> set[str]:
    out = set()
    for part in [_alnum(symbol)] + re.split(r"[^a-z0-9]+", f"{symbol or ''} {name or ''}".lower()):
        if 4 <= len(part) <= 20 and part not in STOP and not part.isdigit():
            out.add(part)
    return out


def load(since: datetime, until: datetime) -> list[dict]:
    """One row per (coin, first call in range): coin text, channel, call time, outcome."""
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (t.id)
                   t.id, t.mint_address, t.symbol, t.name, c.created_at, ltrim(ch.handle, '@'),
                   c.mcap_at_call,
                   (SELECT max(sp.peak_multiplier) FROM shadow_positions sp
                     WHERE sp.token_id = t.id AND sp.exit_variant = 'early'),
                   (SELECT max(sp.peak_mcap) FROM shadow_positions sp
                     WHERE sp.token_id = t.id AND sp.exit_variant = 'early')
            FROM calls c
            JOIN tokens t ON t.id = c.token_id
            LEFT JOIN channels ch ON ch.id = c.channel_id
            WHERE c.created_at >= %s AND c.created_at < %s
              AND t.mint_address NOT LIKE 'UNKNOWN:%%' AND t.mint_address NOT LIKE 'INFERRED:%%'
            ORDER BY t.id, c.created_at
            """, (since, until))
        rows = cur.fetchall()
    out = []
    for tid, mint, sym, name, at, ch, mcap, pmult, pmcap in rows:
        pmult = None if pmult is None else float(pmult)
        pmcap = None if pmcap is None else float(pmcap)
        if (pmcap is not None and pmcap > BAD_PEAK_MCAP) or (pmult is not None and pmult > BAD_PEAK_MULT):
            pmult = pmcap = None
        out.append({"id": tid, "mint": mint, "sym": sym or "", "name": name or "", "at": at,
                    "channel": ch or "?", "mcap": None if mcap is None else float(mcap),
                    "peak_mult": pmult, "peak_mcap": pmcap,
                    "s": _alnum(sym), "n": _alnum(name)})
    out.sort(key=lambda r: r["at"])
    return out


def themes(coins: list[dict], min_coins: int) -> list[tuple[str, list[dict]]]:
    """[(theme, coins)] for themes found in >= min_coins coins, most coins first. A theme
    whose coins are mostly (80%+) already covered by a stronger related theme (one contains
    the other) is dropped, so 'kimchi' wins over 'gkimchi' and 'kimchicity'."""
    cands = set()
    for c in coins:
        cands |= candidate_words(c["sym"], c["name"])
    groups = []
    for w in cands:
        g = [c for c in coins if w in c["s"] or w in c["n"]]
        if len(g) >= min_coins:
            groups.append((w, g))
    groups.sort(key=lambda wg: (-len(wg[1]), len(wg[0])))
    kept: list[tuple[str, list[dict]]] = []
    for w, g in groups:
        ids = {c["id"] for c in g}
        redundant = False
        for kw, kg in kept:
            if kw in w or w in kw:
                if len(ids & {c["id"] for c in kg}) >= 0.8 * len(ids):
                    redundant = True
                    break
        if not redundant:
            kept.append((w, g))
    return kept


def theme_stats(window: list[dict], base: list[dict], hours: float, baseline: float,
                min_coins: int) -> list[dict]:
    """Themes in `window` with momentum against `base` (the baseline hours before), hottest
    first: trending themes (momentum >= HOT_MOMENTUM or new) ahead of background ones."""
    out = []
    for w, g in themes(window, min_coins):
        b = sum(1 for c in base if w in c["s"] or w in c["n"])
        per_window = b / (baseline / hours) if baseline else 0.0
        momentum = (len(g) / per_window) if per_window > 0 else float("inf")
        out.append({"theme": w, "coins": g, "momentum": momentum, "base": b,
                    "hot": momentum >= HOT_MOMENTUM})
    out.sort(key=lambda t: (not t["hot"], -len(t["coins"]) * min(t["momentum"], 10.0)))
    return out


def _fmt_money(v):
    if v is None:
        return "?"
    return f"${v / 1e6:.1f}M" if v >= 1e6 else f"${v / 1e3:.0f}K"


def snapshot(now: datetime, hours: float, baseline: float, top: int, min_coins: int) -> None:
    w_start = now - timedelta(hours=hours)
    b_start = w_start - timedelta(hours=baseline)
    hist_start = now - timedelta(hours=72)
    hist = load(hist_start, now)
    window = [c for c in hist if c["at"] >= w_start]
    base = [c for c in hist if b_start <= c["at"] < w_start]
    if not window:
        print("no calls in the window")
        return
    all_ran = sum(1 for c in window if (c["peak_mult"] or 0) >= 2)
    print(f"trend tracker — {now:%Y-%m-%d %H:%M} UTC, last {hours:g}h: {len(window)} coins called "
          f"(all channels); {all_ran} ({all_ran / len(window) * 100:.0f}%) ran 2x+ on shadow. "
          f"Baseline: the {baseline:g}h before.")
    rows = []
    stats = theme_stats(window, base, hours, baseline, min_coins)
    for st in stats:
        w, g, momentum = st["theme"], st["coins"], st["momentum"]
        first = min((c["at"] for c in hist if w in c["s"] or w in c["n"]), default=None)
        ran = sum(1 for c in g if (c["peak_mult"] or 0) >= 2)
        best = max(g, key=lambda c: c["peak_mcap"] or 0)
        rows.append((w, g, momentum, first, ran, best, len({c["channel"] for c in g}), st["hot"]))
    hot_n = sum(1 for r in rows if r[7])
    print(f"\n  TRENDING ({hot_n}: momentum {HOT_MOMENTUM:g}x+ or new), then BACKGROUND themes:")
    print(f"  {'theme':14s} {'coins':>5s} {'momentum':>9s} {'ch':>3s} {'ran 2x':>7s} {'best':>8s}  "
          f"{'first seen':12s} examples")
    for i, (w, g, mom, first, ran, best, nch, hot) in enumerate(rows[:top]):
        if i == hot_n:
            print("  " + "-" * 40 + " background (always around)")
        mom_s = "NEW" if mom == float("inf") else f"{mom:.1f}x"
        first_s = "NEW" if first and first >= w_start else (f"{(now - first).total_seconds() / 3600:.0f}h ago"
                                                           if first else "?")
        ex = ", ".join(sorted({c["sym"][:10] for c in g}, key=str.lower)[:5])
        print(f"  {w:14s} {len(g):5d} {mom_s:>9s} {nch:3d} {ran:4d}/{len(g):<2d} "
              f"{_fmt_money(best['peak_mcap']):>8s}  {first_s:12s} {ex}")
    hot = sorted((c for c in window if c["peak_mcap"]), key=lambda c: -c["peak_mcap"])[:10]
    if hot:
        print(f"\n  biggest runs among coins called in the last {hours:g}h (shadow peak):")
        for c in hot:
            print(f"    {c['sym'][:14]:14s} {c['name'][:24]:24s} called {c['at']:%m-%d %H:%M} on "
                  f"{c['channel'][:20]:20s} at {_fmt_money(c['mcap']):>7s} -> peak {_fmt_money(c['peak_mcap']):>7s} "
                  f"({(c['peak_mult'] or 0):.1f}x)")
    print("\nmomentum = coins in this window / the theme's average per window over the baseline "
          "(NEW = not seen in the baseline). Shadow overstates runs; compare themes, not absolutes.")


def evaluate(days: float, hours: float, top: int, min_coins: int) -> None:
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=days)
    allc = load(start - timedelta(hours=hours + 24), now)
    print(f"evaluation over {days:g} days, steps of {hours:g}h: at each step take up to {top} "
          f"TRENDING themes (>= {min_coins} coins, momentum {HOT_MOMENTUM:g}x+ vs the 24h before), then score coins with those themes called in the NEXT "
          f"{hours:g}h against every other coin called then (shadow 2x+ rate).")
    hot_n = hot_ran = other_n = other_ran = 0
    steps = 0
    per_step = []
    t = start
    earliest = allc[0]["at"] if allc else now
    while t + timedelta(hours=hours) <= now:
        if t - timedelta(hours=hours + 24) < earliest:      # no full baseline: everything looks new
            t += timedelta(hours=hours)
            continue
        window = [c for c in allc if t - timedelta(hours=hours) <= c["at"] < t]
        base = [c for c in allc if t - timedelta(hours=hours + 24) <= c["at"] < t - timedelta(hours=hours)]
        nxt = [c for c in allc if t <= c["at"] < t + timedelta(hours=hours) and c["peak_mult"] is not None]
        tops = [st["theme"] for st in theme_stats(window, base, hours, 24, min_coins) if st["hot"]][:top]
        if tops and nxt:
            steps += 1
            h = [c for c in nxt if any(w in c["s"] or w in c["n"] for w in tops)]
            o = [c for c in nxt if c not in h]
            hr = sum(1 for c in h if c["peak_mult"] >= 2)
            orr = sum(1 for c in o if c["peak_mult"] >= 2)
            hot_n += len(h); hot_ran += hr; other_n += len(o); other_ran += orr
            per_step.append((t, len(h), hr, len(o), orr, tops[:4]))
        t += timedelta(hours=hours)
    if not steps:
        print("not enough data")
        return
    print(f"\n  {'step (UTC)':12s} {'hot-theme coins':>15s} {'ran 2x':>7s} {'other coins':>11s} {'ran 2x':>7s}  top themes")
    for t, hn, hr, on, orr, tops in per_step:
        print(f"  {t:%m-%d %H:%M}  {hn:15d} {(hr / hn * 100 if hn else 0):6.0f}% {on:11d} "
              f"{(orr / on * 100 if on else 0):6.0f}%  {', '.join(tops)}")
    half = len(per_step) // 2

    def rate(rows):
        hn = sum(r[1] for r in rows); hr = sum(r[2] for r in rows)
        on = sum(r[3] for r in rows); orr = sum(r[4] for r in rows)
        return hn, (hr / hn * 100 if hn else 0), on, (orr / on * 100 if on else 0)
    for label, rows in (("ALL", per_step), ("1st half", per_step[:half]), ("2nd half", per_step[half:])):
        hn, hp, on, op = rate(rows)
        print(f"  {label:9s} hot-theme coins {hn:5d} ran 2x {hp:5.1f}%   other coins {on:5d} ran 2x "
              f"{op:5.1f}%   difference {hp - op:+5.1f} pts")
    print("\nUseful if hot-theme coins beat the rest in BOTH halves. Shadow overstates 2x rates for "
          "everyone, so read the difference, not the level.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=6)
    ap.add_argument("--baseline", type=float, default=24)
    ap.add_argument("--top", type=int, default=25)
    ap.add_argument("--min-coins", type=int, default=3)
    ap.add_argument("--evaluate", action="store_true")
    ap.add_argument("--days", type=float, default=7)
    a = ap.parse_args()
    if a.evaluate:
        evaluate(a.days, a.hours, min(a.top, 10), a.min_coins)
    else:
        snapshot(datetime.now(timezone.utc), a.hours, a.baseline, a.top, a.min_coins)


if __name__ == "__main__":
    main()
