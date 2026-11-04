"""
entry_filter_scan.py — which ENTRY-TIME features separate winners from losers, per channel?

WHY (2026-11-03). solearlytrending and solwhaletrending both had many runners with no
followed wallets in them. This looks for a second filter, using only what is known at the
moment of entry, scored on qsim's real-quote outcomes.

FEATURES (all known at entry): security flag, score, call mcap, entry price vs call price,
token age, bundle % remaining, sniper count, followed wallets, holder snapshot at entry
(largest wallet %, top-10 combined %, wallets holding 2%+), UTC hour, big-name/stock ticker.

OUTCOMES per group: trades, bank % (qsim 2x bank), rug % (hard stop filled under 0.30x),
runner % (peak or after-exit high >= 5x — what a different exit could have caught),
P&L per SOL staked, and the same P&L in the first and second half of the window.

A group matters only if it is (a) a decent size, (b) better than its channel in BOTH halves.
With ~60 groups per channel some will look good by luck — the halves are the guard, and a
pair found here is a hypothesis for forward testing, not a result.

Read-only, no RPC. Duplicate entries (same channel + mint within 2 min) are counted once.
Run:  python3 entry_filter_scan.py [--channel solearlytrending] [--since 2026-10-31] [--pairs]
      python3 entry_filter_scan.py --frozen --since 2026-11-04     (forward test of the frozen rules)
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import db  # noqa: E402

BIG_NAMES = re.compile(
    r"nvidia|nvda|tesla|apple|amazon|google|gemini|microsoft|openai|claude|grok|spacex|robinhood|"
    r"coinbase|cocacola|coca|starbucks|youtube|mrbeast|beast|elon|musk|trump|barron|bitcoin|"
    r"xtwitter|twitter|meta|netflix|disney|nike|gta|rockstar|lego|fomo|ishowspeed")

SQL = """
SELECT qp.call_id, qp.channel_handle, qp.entry_time, t.mint_address, t.symbol,
       qp.sol_in, qp.sol_out, qp.status, qp.exit_reason, qp.peak_multiplier,
       t.security_flag, c.conviction_score, c.mcap_at_call, qp.entry_price,
       t.token_age_minutes, t.bundle_pct_remaining, t.sniper_count, f.n_holding,
       hs.supply_ui, top.top1, top.top10, top.n2,
       nt.mint IS NOT NULL AS in_feed, nt.dev_buy_sol,
       (SELECT max(o.real_mult) FROM qsim_quote_observations o
         WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time
           AND NOT o.rate_limited AND o.real_mult IS NOT NULL) AS post_max
FROM qsim_positions qp
JOIN tokens t ON t.id = qp.token_id
JOIN calls  c ON c.id = qp.call_id
LEFT JOIN followed_at_call f ON f.call_id = qp.call_id
LEFT JOIN new_tokens nt ON nt.mint = t.mint_address
LEFT JOIN holder_snapshots hs ON hs.call_id = qp.call_id AND hs.phase = 'entry' AND hs.ok
LEFT JOIN LATERAL (
    SELECT max(x.amt) AS top1,
           sum(x.amt) FILTER (WHERE x.rn <= 10) AS top10,
           count(*) FILTER (WHERE x.amt >= 0.02 * hs.supply_ui) AS n2
    FROM (SELECT (h->>'ui_amount')::numeric AS amt,
                 row_number() OVER (ORDER BY (h->>'ui_amount')::numeric DESC) AS rn
          FROM jsonb_array_elements(hs.holders) h
          WHERE COALESCE((h->>'on_curve')::boolean, true)) x
) top ON true
WHERE qp.status = 'closed' AND qp.sol_in > 0
  AND qp.entry_time >= %s
  AND (%s = '' OR qp.channel_handle = %s)
ORDER BY qp.entry_time
"""


def bucket(v, edges, labels, none="n/a"):
    if v is None:
        return none
    v = float(v)
    for e, lab in zip(edges, labels):
        if v < e:
            return lab
    return labels[-1]


def features(r: dict) -> dict:
    sup = float(r["supply_ui"] or 0)
    pct = (lambda x: float(x) / sup * 100 if (x is not None and sup > 0) else None)
    ratio = (float(r["entry_price"]) / float(r["mcap_at_call"])
             if r["entry_price"] and r["mcap_at_call"] else None)
    return {
        "flag": r["security_flag"] or "none",
        "score": bucket(r["conviction_score"], [20, 40, 63, 1e9], ["<20", "20-39", "40-62", "63+"]),
        "call mcap": bucket((r["mcap_at_call"] or 0) / 1000 if r["mcap_at_call"] else None,
                            [50, 80, 120, 200, 1e9], ["<50k", "50-80k", "80-120k", "120-200k", "200k+"]),
        "entry/call": bucket(ratio, [0.8, 0.95, 1.05, 1.2, 1e9],
                             ["<0.80", "0.80-0.95", "0.95-1.05", "1.05-1.20", "1.20+"]),
        "age min": bucket(r["token_age_minutes"], [3, 10, 30, 1e9], ["<3", "3-9", "10-29", "30+"]),
        "bundle %": bucket(r["bundle_pct_remaining"], [5, 20, 50, 75, 1e9],
                           ["<5", "5-20", "20-50", "50-75", "75+"]),
        "snipers": bucket(r["sniper_count"], [6, 16, 1e9], ["0-5", "6-15", "16+"]),
        "wallets": bucket(r["n_holding"], [1, 3, 1e9], ["0", "1-2", "3+"], none="not checked"),
        "top1 %": bucket(pct(r["top1"]), [3, 5, 10, 1e9], ["<3", "3-5", "5-10", "10+"]),
        "top10 %": bucket(pct(r["top10"]), [10, 20, 30, 1e9], ["<10", "10-20", "20-30", "30+"]),
        "2%+ wallets": bucket(r["n2"] if r["supply_ui"] else None, [1, 4, 8, 1e9], ["0", "1-3", "4-7", "8+"]),
        "hour UTC": f"{r['entry_time'].hour // 6 * 6:02d}-{r['entry_time'].hour // 6 * 6 + 5:02d}",
        "big name": "yes" if BIG_NAMES.search((r["symbol"] or "").lower()) else "no",
        # creation seen by news_recorder's free feed (only meaningful while the recorder was up)
        "in feed": "yes" if r["in_feed"] else "no",
        "dev buy SOL": bucket(r["dev_buy_sol"], [0.05, 0.5, 3, 1e9], ["<0.05", "0.05-0.5", "0.5-3", "3+"],
                              none="no record"),
    }


def _pct(r, key):
    sup = float(r["supply_ui"] or 0)
    return float(r[key]) / sup * 100 if (r[key] is not None and sup > 0) else None


# Rules FROZEN before their forward window (2026-11-03). Each returns True (pass), False (fail) or
# None (data missing). Scored with --frozen on trades since --since; never re-tuned on that data.
FROZEN = [
    ("solearlytrending", "2%+ wallets >= 4 & top10 >= 20% & bundle < 75%",
     lambda r: None if (r["supply_ui"] is None or r["bundle_pct_remaining"] is None)
     else (r["n2"] or 0) >= 4 and (_pct(r, "top10") or 0) >= 20 and float(r["bundle_pct_remaining"]) < 75),
    ("solwhaletrending", "top10 10-20%",
     lambda r: None if _pct(r, "top10") is None else 10 <= _pct(r, "top10") < 20),
    ("solwhaletrending", "security flag = safe",
     lambda r: None if not r["security_flag"] else r["security_flag"] == "safe"),
    ("solwhaletrending", "followed wallets 3+ (the live lane)",
     lambda r: None if r["n_holding"] is None else r["n_holding"] >= 3),
]


def frozen_report(clean) -> None:
    print("\nFROZEN RULES — forward test (rules fixed 2026-11-03; pass vs fail on this window):")
    print(f"  {'rule':60s} {'group':5s} {'n':>4s} {'bank%':>6s} {'rug%':>5s} {'P&L/SOL':>8s}  days")
    for ch, name, rule in FROZEN:
        rs = [r for r in clean if r["channel_handle"] == ch]
        verdicts = [(rule(r), r) for r in rs]
        for lab, want in (("pass", True), ("fail", False)):
            g = [r for v, r in verdicts if v is want]
            if not g:
                print(f"  {ch.replace('trending', '') + ': ' + name:60s} {lab:5s}    0")
                continue
            n, bank, rug, _, pnl = stats([outcome(r) for r in g])
            days = len({r["entry_time"].date() for r in g})
            print(f"  {ch.replace('trending', '') + ': ' + name:60s} {lab:5s} {n:4d} {bank:6.1f} {rug:5.1f} {pnl:+8.1f}  {days}")
        miss = sum(1 for v, _ in verdicts if v is None)
        if miss:
            print(f"  {'':60s} {'n/a':5s} {miss:4d}  (data missing — not scored)")


def outcome(r: dict) -> dict:
    x = float(r["sol_out"] or 0) / float(r["sol_in"])
    bank = "bank" in (r["exit_reason"] or "") or r["exit_reason"] == "take_profit"
    hi = max(float(r["peak_multiplier"] or 0), float(r["post_max"] or 0))
    return {"pnl": float(r["sol_out"] or 0) - float(r["sol_in"]), "sol_in": float(r["sol_in"]),
            "bank": bank, "rug": r["exit_reason"] == "hard_stop" and x < 0.30, "runner": hi >= 5.0}


def stats(g):
    n = len(g)
    s_in = sum(o["sol_in"] for o in g) or 1e-9
    return (n, sum(o["bank"] for o in g) / n * 100, sum(o["rug"] for o in g) / n * 100,
            sum(o["runner"] for o in g) / n * 100, sum(o["pnl"] for o in g) / s_in * 100)


def per_sol(g):
    s = sum(o["sol_in"] for o in g)
    return sum(o["pnl"] for o in g) / s * 100 if s else None


def combined_score_test(items, min_n: int, margin: float) -> None:
    """Walk-forward test of a COMBINED filter: learn which feature values were good (+1) or bad
    (-1) on one half, score every trade of the OTHER half by adding them up, and show how the
    other half did per score. Done both ways. A real combined signal shows higher scores doing
    better in BOTH directions; a pattern that only exists in-sample falls flat here."""
    halves = ([x for x in items if x[2]], [x for x in items if not x[2]])
    for name, train, test in (("learn 1st half -> test 2nd", halves[0], halves[1]),
                              ("learn 2nd half -> test 1st", halves[1], halves[0])):
        base = per_sol([o for _, o, _ in train]) or 0.0
        weights = {}
        for feat in train[0][0]:
            groups = defaultdict(list)
            for f, o, _ in train:
                groups[f[feat]].append(o)
            for val, g in groups.items():
                if val in ("n/a", "not checked") or len(g) < min_n:
                    continue
                p = per_sol(g)
                if p is not None and p > base + margin:
                    weights[(feat, val)] = 1
                elif p is not None and p < base - margin:
                    weights[(feat, val)] = -1
        by = defaultdict(list)
        for f, o, _ in test:
            sc = sum(weights.get((k, v), 0) for k, v in f.items())
            by[max(-3, min(3, sc))].append(o)
        good = ", ".join(f"{k}={v}" for (k, v), w in sorted(weights.items()) if w > 0)
        bad = ", ".join(f"{k}={v}" for (k, v), w in sorted(weights.items()) if w < 0)
        print(f"\n  COMBINED SCORE, {name} (learned: +1 for [{good}]; -1 for [{bad}])")
        print(f"    {'score':>6s} {'n':>4s} {'bank%':>6s} {'rug%':>5s} {'run%':>5s} {'P&L/SOL':>8s}")
        for sc in sorted(by):
            n, bank, rug, run, pnl = stats(by[sc])
            lab = ("<=-3" if sc == -3 else ">=+3" if sc == 3 else f"{sc:+d}")
            print(f"    {lab:>6s} {n:4d} {bank:6.1f} {rug:5.1f} {run:5.1f} {pnl:+8.1f}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", default="", help="one channel (default: each of the three in turn)")
    ap.add_argument("--since", default="2026-10-31")
    ap.add_argument("--min-n", type=int, default=15, help="smallest group shown")
    ap.add_argument("--pairs", action="store_true", help="also scan two-feature combinations")
    ap.add_argument("--combined", action="store_true",
                    help="walk-forward test of an added-up score over all features (learn one half, test the other)")
    ap.add_argument("--frozen", action="store_true", help="forward test of the rules frozen on 2026-11-03")
    ap.add_argument("--margin", type=float, default=10.0,
                    help="combined: a value counts as good/bad if its P&L/SOL beats/trails the half by this")
    a = ap.parse_args()

    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(SQL, (a.since, a.channel.lstrip("@"), a.channel.lstrip("@")))
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    # one row per (channel, mint) within 2 min — duplicate opens (fixed b1d6fc9) count once
    seen, clean = {}, []
    for r in rows:
        k = (r["channel_handle"], r["mint_address"])
        if k in seen and (r["entry_time"] - seen[k]).total_seconds() < 120:
            continue
        seen[k] = r["entry_time"]
        clean.append(r)
    print(f"{len(clean)} closed qsim trades since {a.since} ({len(rows) - len(clean)} duplicate opens dropped). "
          f"bank = qsim 2x bank; rug = stop filled < 0.30x; runner = peak or after-exit high >= 5x.")

    if a.frozen:
        frozen_report(clean)
        return

    channels = [a.channel.lstrip("@")] if a.channel else \
        ["solwhaletrending", "solearlytrending", "solhousesignal"]
    for ch in channels:
        rs = [r for r in clean if r["channel_handle"] == ch]
        if not rs:
            continue
        mid = rs[len(rs) // 2]["entry_time"]
        items = [(features(r), outcome(r), r["entry_time"] < mid) for r in rs]
        base = stats([o for _, o, _ in items])
        b1 = per_sol([o for _, o, h in items if h])
        b2 = per_sol([o for _, o, h in items if not h])
        print(f"\n=== {ch}: {base[0]} trades  bank {base[1]:.1f}%  rug {base[2]:.1f}%  runner {base[3]:.1f}%  "
              f"P&L {base[4]:+.1f}%/SOL  (halves {b1:+.1f} / {b2:+.1f}) ===")
        print(f"  {'feature':12s} {'value':12s} {'n':>4s} {'bank%':>6s} {'rug%':>5s} {'run%':>5s} "
              f"{'P&L/SOL':>8s} {'1st':>7s} {'2nd':>7s}")
        keep = []
        for feat in items[0][0]:
            groups = defaultdict(list)
            for f, o, h in items:
                groups[f[feat]].append((o, h))
            for val, g in sorted(groups.items(), key=lambda kv: str(kv[0])):
                if len(g) < a.min_n:
                    continue
                n, bank, rug, run, pnl = stats([o for o, _ in g])
                h1 = per_sol([o for o, h in g if h])
                h2 = per_sol([o for o, h in g if not h])
                both = h1 is not None and h2 is not None and h1 > b1 and h2 > b2
                mark = " <" if both and pnl > base[4] + 5 else ""
                print(f"  {feat:12s} {str(val):12s} {n:4d} {bank:6.1f} {rug:5.1f} {run:5.1f} {pnl:+8.1f} "
                      f"{(h1 if h1 is not None else 0):+7.1f} {(h2 if h2 is not None else 0):+7.1f}{mark}")
                if mark:
                    keep.append((feat, val))
        if a.combined:
            combined_score_test(items, max(8, a.min_n // 2), a.margin)
        if a.pairs and keep:
            print(f"\n  PAIRS of the marked groups (n >= {a.min_n}, both halves above the channel):")
            found = []
            for i in range(len(keep)):
                for j in range(i + 1, len(keep)):
                    (f1, v1), (f2, v2) = keep[i], keep[j]
                    if f1 == f2:
                        continue
                    g = [(o, h) for f, o, h in items if f[f1] == v1 and f[f2] == v2]
                    if len(g) < a.min_n:
                        continue
                    h1 = per_sol([o for o, h in g if h])
                    h2 = per_sol([o for o, h in g if not h])
                    if h1 is None or h2 is None or h1 <= b1 or h2 <= b2:
                        continue
                    found.append((stats([o for o, _ in g]), h1, h2, f"{f1}={v1} & {f2}={v2}"))
            for (n, bank, rug, run, pnl), h1, h2, name in sorted(found, key=lambda x: -x[0][4])[:12]:
                print(f"  {name:44s} n={n:3d} bank {bank:5.1f}% rug {rug:5.1f}% run {run:5.1f}% "
                      f"P&L {pnl:+6.1f}%/SOL ({h1:+.1f} / {h2:+.1f})")
            if not found:
                print("  none")
    print("\n'<' = better than the channel by 5+ points AND above it in both halves. Still a hypothesis: "
          "~60 groups per channel means a few pass by luck. Re-run on later days before trading one.")


if __name__ == "__main__":
    main()
