"""
feature_edge.py — which token features actually separate runners from losers?

For traded calls, labels each as a RUNNER (peak >= --runner, default 2.0x) or a
LOSER (peak < --loser, default 1.2x), then for every feature finds the single
threshold that best separates the two — i.e. the best one-rule filter — and shows
how many runners you'd keep vs losers you'd drop.

Use it to build strict, DATA-DERIVED entry filters instead of guessing. Read-only.

    python3 feature_edge.py --days 14
    python3 feature_edge.py --days 14 --runner 3 --loser 1.0

NOTE on labels: this uses the trade's entry-relative peak_multiplier (the honest
"did it run from where we'd buy"), restricted to coins we actually traded — so it
tightens the CURRENT filter. The broader, all-lanes version comes from re-running
this on the shadow dataset once it accumulates.
"""

from __future__ import annotations

import argparse
import os
import sys

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db

# (column, label, higher_is_better_hint, sentinel) — sentinel values treated as missing.
# Ordered roughly by the on-chain "quality discovery" thesis: volume spike, holders,
# holder quality/concentration, dev quality, then the structural gates.
FEATURES = [
    # --- volume / spike (the "it's moving" signal) ---
    ("turnover",             "vol/liq",        None,  None),   # vol_1h / liquidity — the spike intensity
    ("vol_to_mcap",          "vol/mcap",       None,  None),
    ("vol_1h_at_detection",  "vol_1h_usd",     None,  None),
    ("liq_at_detection",     "liquidity_usd",  None,  None),
    # --- holders / quality ---
    ("holder_count",         "holders",        None,  None),
    ("hodl_count",           "hodlers",        None,  None),
    ("top_10_holder_pct",    "top10_pct",      None,  None),
    ("first_20_pct",         "first20_pct",    None,  None),
    ("detecting_wallet_sol", "detector_sol",   None,  None),   # SOL of the wallet that bought — smart-money proxy
    # --- dev quality ---
    ("dev_best_mcap",        "dev_best_mc",    None,  None),    # dev's prior best — track record
    ("dev_pct_held",         "dev_held_pct",   None,  None),
    ("dev_tokens_made",      "dev_tokens",     None,  None),
    # --- structural / risk ---
    ("bundle_pct_remaining", "bundle_pct",     None,  -1),
    ("fake_vol_pct",         "fake_vol_pct",   None,  -1),
    ("sniper_pct_remaining", "sniper_pct",     None,  -1),
    ("bundle_count",         "bundle_cnt",     None,  None),
    ("sniper_count",         "sniper_cnt",     None,  None),
    ("token_age_minutes",    "age_min",        None,  None),
    ("entry_mcap",           "entry_mcap",     None,  None),
    ("score",                "score",          None,  None),
]

QUERY = """
SELECT
  tp.peak_multiplier                                          AS peak,
  tp.entry_price                                              AS entry_mcap,
  c.conviction_score                                          AS score,
  tok.vol_1h_at_detection / NULLIF(tok.liq_at_detection, 0)   AS turnover,
  tok.vol_1h_at_detection / NULLIF(tp.entry_price, 0)         AS vol_to_mcap,
  tok.vol_1h_at_detection, tok.liq_at_detection,
  tok.holder_count, tok.hodl_count,
  tok.top_10_holder_pct, tok.first_20_pct,
  tok.detecting_wallet_sol,
  tok.dev_best_mcap, tok.dev_pct_held, tok.dev_tokens_made,
  tok.bundle_pct_remaining, tok.fake_vol_pct, tok.sniper_pct_remaining,
  tok.bundle_count, tok.sniper_count,
  tok.token_age_minutes
FROM trading_positions tp
JOIN calls  c   ON c.id  = tp.call_id
JOIN tokens tok ON tok.id = tp.token_id
WHERE tp.is_simulation = TRUE AND NOT tp.is_strategy_b AND tp.status = 'closed'
  AND tp.peak_multiplier IS NOT NULL
  AND ( %s = 0 OR tp.entry_time >= now() - (%s || ' days')::interval )
"""


def best_stump(pairs):
    """pairs = [(value, is_runner)]. Find threshold+direction maximizing
    (runner_keep_rate - loser_keep_rate). Returns dict or None."""
    runners = [v for v, r in pairs if r]
    losers = [v for v, r in pairs if not r]
    if len(runners) < 5 or len(losers) < 5:
        return None
    nR, nL = len(runners), len(losers)
    cand = sorted({v for v, _ in pairs})
    best = None
    for t in cand:
        for direction in (">=", "<="):
            if direction == ">=":
                rk = sum(1 for v in runners if v >= t)
                lk = sum(1 for v in losers if v >= t)
            else:
                rk = sum(1 for v in runners if v <= t)
                lk = sum(1 for v in losers if v <= t)
            r_keep = rk / nR
            l_keep = lk / nL
            sep = r_keep - l_keep
            kept = rk + lk
            precision = rk / kept if kept else 0.0
            if best is None or sep > best["sep"]:
                best = {"t": t, "dir": direction, "r_keep": r_keep,
                        "l_keep": l_keep, "sep": sep, "precision": precision,
                        "kept": kept, "nR": nR, "nL": nL}
    return best


def median(xs):
    s = sorted(xs)
    return s[len(s) // 2] if s else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=0, help="0 = all time")
    ap.add_argument("--runner", type=float, default=2.0, help="peak >= this = runner")
    ap.add_argument("--loser", type=float, default=1.2, help="peak < this = loser")
    args = ap.parse_args()

    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(QUERY, (args.days, args.days))
        rows = cur.fetchall()

    labeled = []
    for r in rows:
        pk = float(r["peak"])
        if pk >= args.runner:
            labeled.append((r, True))
        elif pk < args.loser:
            labeled.append((r, False))
    nR = sum(1 for _, x in labeled if x)
    nL = sum(1 for _, x in labeled if not x)

    label = f"last {args.days}d" if args.days else "all time"
    print(f"\nFeature edge — {label}  (runner = peak>={args.runner}x, loser = peak<{args.loser}x)")
    print(f"  runners={nR}  losers={nL}  baseline runner rate={100*nR/(nR+nL):.0f}%\n"
          if (nR + nL) else "  no labeled trades in window\n")
    if not (nR and nL):
        return

    hdr = (f"{'feature':<14} {'run_med':>10} {'lose_med':>10}  {'best filter':<16} "
           f"{'keep_run':>8} {'drop_lose':>9} {'sep':>5} {'precis':>6}")
    print(hdr)
    print("-" * len(hdr))

    results = []
    for col, name, _, sentinel in FEATURES:
        pairs = []
        rv, lv = [], []
        for r, is_run in labeled:
            v = r.get(col)
            if v is None:
                continue
            v = float(v)
            if sentinel is not None and v == sentinel:
                continue
            pairs.append((v, is_run))
            (rv if is_run else lv).append(v)
        stump = best_stump(pairs)
        if not stump:
            continue
        results.append((name, median(rv), median(lv), stump))

    # show strongest separators first
    for name, rmed, lmed, s in sorted(results, key=lambda x: x[3]["sep"], reverse=True):
        filt = f"{s['dir']} {s['t']:.4g}"
        print(f"{name:<14} {(rmed if rmed is not None else 0):>10.4g} "
              f"{(lmed if lmed is not None else 0):>10.4g}  {filt:<16} "
              f"{100*s['r_keep']:>7.0f}% {100*(1-s['l_keep']):>8.0f}% "
              f"{s['sep']:>5.2f} {100*s['precision']:>5.0f}%")

    print("\n  Read: a high 'sep' with high 'keep_run' + 'drop_lose' = a real filter.")
    print("  'precis' = runner rate among kept (vs baseline above — higher is lift).")
    print("  Stack the top 2-3 independent filters; then VALIDATE forward on shadow data")
    print("  before trusting — single-feature stumps overfit on small samples.\n")


if __name__ == "__main__":
    main()
