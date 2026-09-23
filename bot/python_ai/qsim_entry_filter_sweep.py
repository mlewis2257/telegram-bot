"""
qsim_entry_filter_sweep.py — can ANY entry feature produce a profitable subset?

THE QUESTION
------------
The loss is 635 hard stops at -38.5%/SOL against a runner leg at +39.3%. That is
an entry-selection problem, and the one thing that ever worked in this project
was a STRUCTURAL entry filter (the mcap ceiling: rug rate 19% -> 6.5%), not a
predictive model (ML, wallet identity, holder counts -- all null).

So: sweep every entry-time feature we already collect, at every threshold, in
both directions, and ask whether any of them carves out a subset of trades that
is actually PROFITABLE on the quote-priced book.

THE TRAP THIS SCRIPT IS BUILT AROUND
------------------------------------
The book is NEGATIVE. That means throwing away trades AT RANDOM "improves" it,
and the fewer you keep the better it looks. A sweep over ~15 features x ~1000
split points x 2 directions is 30,000 chances to find a subset that looks great
by luck alone. Reporting the best one is how you manufacture an edge.

Two defences, both load-bearing:

  1. --min-keep. A filter that keeps 40 trades is not a strategy. The retained
     population must stay large enough to trade.

  2. THE PERMUTATION TEST IS THE HEADLINE. Shuffle outcomes against feature
     values so that no feature can carry information, then re-run the ENTIRE
     sweep -- every feature, every split, both directions -- and record the best
     subset it finds. 500 times. That distribution is what this search returns
     when nothing is real. The observed best must beat it.

     This is family-wise over the whole search, so adding features or splits
     raises the null's bar too. It cannot be gamed by looking harder.

NULL HANDLING
-------------
A filter cannot act on a value it does not have, so each feature is evaluated
only on rows where it is populated, and that subpopulation's own baseline is
shown next to the result. Coverage is printed for every feature: a feature that
is 85% NULL cannot help the live book no matter how good it looks here.

Read-only. Executes nothing, writes nothing.

    python3 qsim_entry_filter_sweep.py --days 30
    python3 qsim_entry_filter_sweep.py --days 30 --min-keep 300 --perms 1000
"""

from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import sys
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))

# Candidate entry-time features. Presence is checked against information_schema
# at runtime, so a column that does not exist is skipped rather than crashing.
CANDIDATES: list[tuple[str, str, str]] = [
    # (source table alias, column, display name)
    ("c",   "mcap_at_call",          "mcap_at_call"),
    ("c",   "liquidity_at_call",     "liq_at_call"),
    ("c",   "conviction_score",      "score"),
    ("tok", "dev_tokens_made",       "dev_tokens_made"),
    ("tok", "dev_best_mcap",         "dev_best_mcap"),
    ("tok", "dev_pct_held",          "dev_pct_held"),
    ("tok", "bundle_count",          "bundle_count"),
    ("tok", "bundle_pct_remaining",  "bundle_pct_rem"),
    ("tok", "sniper_count",          "sniper_count"),
    ("tok", "sniper_pct_remaining",  "sniper_pct_rem"),
    ("tok", "fake_vol_pct",          "fake_vol_pct"),
    ("tok", "first_20_pct",          "first_20_pct"),
    ("tok", "hodl_count",            "hodl_count"),
    ("tok", "holder_count",          "holder_count"),
    ("tok", "top_10_holder_pct",     "top10_pct"),
    ("tok", "token_age_minutes",     "token_age_min"),
    ("tok", "liq_at_detection",      "liq_at_detect"),
    ("tok", "vol_1h_at_detection",   "vol_1h"),
    ("tok", "detecting_wallet_sol",  "detector_sol"),
]

# Order-flow features. These live in ws_market_observations, not on calls/tokens,
# so they are joined rather than looked up in information_schema.
#
# THE JOIN CONDITION IS THE WHOLE POINT. feature_edge.py reads the first snapshot
# in [entry_time, entry_time + 2 min] — i.e. AFTER the entry. A filter cannot use
# data that does not exist when it has to decide, and post-entry flow is partly a
# function of the outcome. That leak is what made the earlier ML attempt look
# predictive (see memory/ml_entry_filter_dead_end.md). Here it is the LATEST
# snapshot at or BEFORE entry_time, strictly.
OF_FEATURES = [
    ("of_net_pressure", "(ofq.of->>'net_pressure')::numeric"),
    ("of_buy_vol",      "(ofq.of->>'buy_vol_sol')::numeric"),
    ("of_uniq_buyers",  "(ofq.of->>'unique_buyers')::numeric"),
    ("of_n_buys",       "(ofq.of->>'n_buys')::numeric"),
]

OF_JOIN = """
LEFT JOIN LATERAL (
    SELECT o.market_json->'order_flow' AS of
    FROM ws_market_observations o
    WHERE o.mint_address = tok.mint_address
      AND o.observed_at <= qp.entry_time          -- STRICTLY at or before entry
      AND o.market_json ? 'order_flow'
      AND o.market_json->'order_flow' <> 'null'::jsonb
    ORDER BY o.observed_at DESC
    LIMIT 1
) ofq ON true
"""

DERIVED = [
    # name, numerator, denominator — ratios are often the real tell (thin
    # liquidity against a big mcap is the classic drain setup)
    ("liq_to_mcap", "liquidity_at_call", "mcap_at_call"),
    ("vol_to_liq",  "vol_1h_at_detection", "liq_at_detection"),
]


def _present(cols: list[tuple[str, str, str]]) -> list[tuple[str, str, str]]:
    import db
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name, column_name FROM information_schema.columns
            WHERE table_name IN ('calls','tokens')
        """)
        have = {(t, c) for t, c in cur.fetchall()}
    tbl = {"c": "calls", "tok": "tokens"}
    return [x for x in cols if (tbl[x[0]], x[1]) in have]


def _rows(days: int, feats: list[tuple[str, str, str]]) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor
    import db

    cols = [f"{a}.{col} AS {name}" for a, col, name in feats]
    cols += [f"{expr} AS {name}" for name, expr in OF_FEATURES]
    sel = ",\n       ".join(cols)
    sql = f"""
    SELECT qp.call_id, qp.sol_in, qp.pnl_sol, qp.pnl_pct,
           coalesce(qp.partial_fraction, 0) AS partial_fraction,
           {sel}
    FROM qsim_positions qp
    JOIN calls  c   ON c.id   = qp.call_id
    JOIN tokens tok ON tok.id = qp.token_id
    {OF_JOIN}
    WHERE qp.status = 'closed'
      AND qp.entry_time >= now() - (%(days)s || ' days')::interval
      AND qp.sol_in > 0
    """
    db.ensure_qsim_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, {"days": days})
        return [dict(r) for r in cur.fetchall()]


def _f(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def best_split(order: list[int], pnl: list[float], sol: list[float],
               min_keep: int) -> tuple[float, int, str]:
    """Best retained %/SOL over every split of one feature, both directions.

    `order` is row indices sorted ascending by the feature. Suffix sums give
    every 'keep above' threshold in one pass; prefix sums give every 'keep
    below'. O(n) per feature, which is what makes the permutation test tractable.
    """
    n = len(order)
    if n < min_keep:
        return (float("-inf"), 0, "")

    # keep-above: rows order[k:]
    suf_p = [0.0] * (n + 1)
    suf_s = [0.0] * (n + 1)
    for i in range(n - 1, -1, -1):
        suf_p[i] = suf_p[i + 1] + pnl[order[i]]
        suf_s[i] = suf_s[i + 1] + sol[order[i]]
    best, at, direction = float("-inf"), 0, ""
    for k in range(0, n - min_keep + 1):
        if suf_s[k] > 0:
            v = 100.0 * suf_p[k] / suf_s[k]
            if v > best:
                best, at, direction = v, k, "above"

    # keep-below: rows order[:k]
    pre_p = pre_s = 0.0
    for k in range(0, n):
        pre_p += pnl[order[k]]
        pre_s += sol[order[k]]
        if k + 1 >= min_keep and pre_s > 0:
            v = 100.0 * pre_p / pre_s
            if v > best:
                best, at, direction = v, k + 1, "below"
    return (best, at, direction)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--min-keep", type=int, default=200,
                    help="a filter that keeps fewer trades than this is not a "
                         "strategy; also stops the sweep chasing tiny lucky "
                         "subsets (default 200)")
    ap.add_argument("--min-cov", type=float, default=0.30,
                    help="skip features populated on less than this fraction of "
                         "rows — they cannot move the live book (default 0.30)")
    ap.add_argument("--perms", type=int, default=500)
    ap.add_argument("--seed", type=int, default=20260918)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    feats = _present(CANDIDATES)
    rows = _rows(args.days, feats)
    if not rows:
        print("no closed qsim positions in window")
        return 1

    n = len(rows)
    pnl = [float(r["pnl_sol"] or 0) for r in rows]
    sol = [float(r["sol_in"] or 0) for r in rows]
    banked = [1 if float(r["partial_fraction"] or 0) > 0 else 0 for r in rows]
    rugged = [1 if float(r["pnl_pct"] or 0) <= -80 else 0 for r in rows]

    book_pnl, book_sol = sum(pnl), sum(sol)
    base = 100.0 * book_pnl / book_sol if book_sol else 0.0

    # feature vectors, including derived ratios
    vectors: dict[str, list[float | None]] = {}
    for _, _, name in feats:
        vectors[name] = [_f(r.get(name)) for r in rows]
    for name, _ in OF_FEATURES:
        vectors[name] = [_f(r.get(name)) for r in rows]
    colname = {name: name for _, _, name in feats}
    for dname, num, den in DERIVED:
        nk = next((nm for _, c, nm in feats if c == num), None)
        dk = next((nm for _, c, nm in feats if c == den), None)
        if nk and dk:
            vectors[dname] = [
                (a / b) if (a is not None and b not in (None, 0)) else None
                for a, b in zip(vectors[nk], vectors[dk])
            ]
            colname[dname] = f"{nk}/{dk}"

    usable: dict[str, list[int]] = {}
    coverage: dict[str, float] = {}
    for name, vec in vectors.items():
        idx = [i for i, v in enumerate(vec) if v is not None]
        coverage[name] = len(idx) / n
        if coverage[name] >= args.min_cov and len(idx) >= args.min_keep:
            idx.sort(key=lambda i: vec[i])  # type: ignore[arg-type,return-value]
            usable[name] = idx

    of_cov = sum(1 for r in rows if r.get("of_net_pressure") is not None) / n
    print(f"window        {args.days}d   {n} closed trades   "
          f"book {book_pnl:+.4f} SOL ({base:+.2f}%/SOL)")
    print(f"order flow    {100 * of_cov:.1f}% of trades have a snapshot at or "
          f"BEFORE entry")
    if of_cov < 0.10:
        print("              ^ ws_* observations mostly START at entry, so this "
              "data does not exist")
        print("                when the decision has to be made. Order flow "
              "cannot be an entry")
        print("                filter here regardless of what the sweep below "
              "says.")
    print(f"features      {len(vectors)} candidates, {len(usable)} testable "
          f"(coverage >= {args.min_cov:.0%}, n >= {args.min_keep})")
    print()

    print(f"{'feature':<18}{'cov%':>7}{'n':>7}{'sub_base':>10}{'best':>9}"
          f"{'dir':>7}{'thresh':>14}{'keep':>7}{'bank%':>8}{'rug%':>7}")
    print("-" * 94)

    results = []
    skipped = []
    for name, vec in vectors.items():
        if name not in usable:
            skipped.append((name, coverage[name]))
            continue
        idx = usable[name]
        sp = sum(pnl[i] for i in idx)
        ss = sum(sol[i] for i in idx)
        sub_base = 100.0 * sp / ss if ss else 0.0
        val, at, direction = best_split(idx, pnl, sol, args.min_keep)
        kept = idx[at:] if direction == "above" else idx[:at]
        thr = vec[idx[at]] if at < len(idx) else None
        bank = 100.0 * sum(banked[i] for i in kept) / len(kept) if kept else 0.0
        rug = 100.0 * sum(rugged[i] for i in kept) / len(kept) if kept else 0.0
        results.append({"feature": name, "col": colname[name], "cov": coverage[name],
                        "n": len(idx), "sub_base": sub_base, "best": val,
                        "dir": direction, "thresh": thr, "keep": len(kept),
                        "bank": bank, "rug": rug})

    for r in sorted(results, key=lambda x: -x["best"]):
        t = f"{r['thresh']:.4g}" if r["thresh"] is not None else "-"
        print(f"{r['feature']:<18}{100 * r['cov']:>7.0f}{r['n']:>7}{r['sub_base']:>10.2f}"
              f"{r['best']:>9.2f}{r['dir']:>7}{t:>14}{r['keep']:>7}"
              f"{r['bank']:>8.1f}{r['rug']:>7.1f}")
    if skipped:
        print(f"\nskipped (coverage below {args.min_cov:.0%} or too few rows): "
              + ", ".join(f"{nm} {100 * cv:.0f}%" for nm, cv in sorted(skipped, key=lambda x: -x[1])))

    if not results:
        print("\nnothing testable — no feature is populated widely enough.")
        return 1

    # ── permutation: re-run the WHOLE sweep on shuffled outcomes ────────────
    rng = random.Random(args.seed)
    observed = max(r["best"] for r in results)
    order_of = [usable[r["feature"]] for r in results]
    null_best = []
    perm = list(range(n))
    for _ in range(args.perms):
        rng.shuffle(perm)
        sp = [pnl[perm[i]] for i in range(n)]
        ss = [sol[perm[i]] for i in range(n)]
        b = float("-inf")
        for idx in order_of:
            v, _, _ = best_split(idx, sp, ss, args.min_keep)
            if v > b:
                b = v
        null_best.append(b)
    null_best.sort()
    p_fw = sum(1 for v in null_best if v >= observed) / len(null_best)
    null_p50 = statistics.median(null_best)
    null_p95 = null_best[int(0.95 * len(null_best))]

    if args.json:
        print(json.dumps({"results": results, "observed": observed,
                          "p_familywise": p_fw, "null_p50": null_p50,
                          "null_p95": null_p95}, indent=2, default=str))
        return 0

    print()
    print("=" * 94)
    print("PERMUTATION TEST — the only line that matters")
    print("=" * 94)
    print(f"  whole-book baseline        {base:+.2f} %/SOL")
    print(f"  best filter found          {observed:+.2f} %/SOL")
    print(f"  best under the null        {null_p50:+.2f} %/SOL typical, "
          f"{null_p95:+.2f} at the 95th pct")
    print(f"  family-wise p              {p_fw:.4f}   ({args.perms} shuffles, "
          f"{len(results)} features x every split x both directions)")
    print()
    if p_fw <= 0.05:
        print("  -> A real entry filter exists in this data. Confirm before trusting it:")
        print("     (a) it must be POSITIVE, not merely less negative — a filter that")
        print("         turns -15%/SOL into -4%/SOL still loses money on every trade;")
        print("     (b) re-run on a later window it was not fitted to;")
        print("     (c) the bank%/rug% columns must give it a mechanical reason.")
    else:
        print("  -> No entry filter here beats the search itself. Every row in the")
        print("     table above is the sweep finding shapes in noise.")
    print()
    print(f"  Reminder: breakeven needs the retained population to bank ~50% of the")
    print(f"  time. Check the bank% column, not just the %/SOL.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
