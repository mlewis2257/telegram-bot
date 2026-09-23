"""
qsim_winner_profile.py — do winners and losers differ AT ALL, on anything?

A DIFFERENT QUESTION FROM THE SWEEP
-----------------------------------
`qsim_entry_filter_sweep.py` asks "can a threshold on this feature carve out a
PROFITABLE subset". That is a high bar: a feature can separate winners from
losers clearly and still not produce a profitable subset, because the whole book
is -13%/SOL and a filter has to overcome that.

This asks the prior question: is there any measurable difference between the two
groups, anywhere, regardless of whether it is tradeable. If the answer is no on
every feature, the sweep's null is explained and there is nothing left to model.
If some feature separates, a multivariate model becomes worth trying.

THE MEASURE
-----------
Per feature, the AUC of that feature as a ranker of winners vs losers:

    0.50   the feature is noise
    0.55   weak but real if n is large
    0.60+  worth modelling
    <0.50  separates in the INVERSE direction, which is equally interesting

AUC is used instead of a threshold search because it needs no cut point, so it
cannot be tuned, and it reads the same in both directions.

THE LABEL IS EXIT-INDEPENDENT BY DEFAULT
----------------------------------------
--label peak2x marks a coin a winner if it EVER reached 2x, using post-exit
probes as well as in-life quotes. That asks "can we tell in advance which coins
run", which is the underlying question and does not depend on how the bot
exited. --label pnl uses realised PnL instead, which is contaminated by exit
policy: a coin that ran 5x and was badly exited counts as a loser.

MULTIPLE COMPARISONS
--------------------
~20 features, so the largest |AUC - 0.5| is biased upward by the search alone.
The headline is a permutation test: shuffle the labels, recompute every AUC,
take the largest deviation, 2000 times. That is what the best feature looks like
when nothing is real.

Read-only. Executes nothing, writes nothing.

    python3 qsim_winner_profile.py --days 60
    python3 qsim_winner_profile.py --days 60 --label pnl
"""

from __future__ import annotations

import argparse
import os
import random
import statistics
import sys
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))

MAX_MULT = float(os.getenv("QSIM_REPLAY_MAX_QOBS_MULT", "1000"))

FEATURES: list[tuple[str, str]] = [
    ("mcap_at_call",     "c.mcap_at_call"),
    ("liq_at_call",      "c.liquidity_at_call"),
    ("score",            "c.conviction_score"),
    ("liq_to_mcap",      "c.liquidity_at_call / NULLIF(c.mcap_at_call, 0)"),
    ("token_age_min",    "tok.token_age_minutes"),
    ("hodl_count",       "tok.hodl_count"),
    ("first_20_pct",     "tok.first_20_pct"),
    ("bundle_count",     "tok.bundle_count"),
    ("bundle_pct_rem",   "tok.bundle_pct_remaining"),
    ("sniper_count",     "tok.sniper_count"),
    ("sniper_pct_rem",   "tok.sniper_pct_remaining"),
    ("fake_vol_pct",     "tok.fake_vol_pct"),
    ("dev_pct_held",     "tok.dev_pct_held"),
    ("liq_at_detect",    "tok.liq_at_detection"),
    ("vol_1h",           "tok.vol_1h_at_detection"),
    ("vol_to_liq",       "tok.vol_1h_at_detection / NULLIF(tok.liq_at_detection, 0)"),
    ("vol_to_mcap",      "tok.vol_1h_at_detection / NULLIF(c.mcap_at_call, 0)"),
    ("detector_sol",     "tok.detecting_wallet_sol"),
    ("entry_roundtrip",  "NULL::numeric"),   # filled in below from quotes
]


def _sql(days: int) -> str:
    sel = ",\n           ".join(f"{expr} AS {name}" for name, expr in FEATURES
                                if expr != "NULL::numeric")
    return f"""
    WITH pk AS (
        SELECT q.call_id,
               max(q.real_mult) AS ever_peak,
               (array_agg(q.real_mult ORDER BY q.observed_at))[1] AS first_mult
        FROM qsim_quote_observations q
        WHERE q.real_mult IS NOT NULL AND q.real_mult > 0
          AND q.real_mult <= {MAX_MULT}
        GROUP BY q.call_id
    )
    SELECT qp.call_id, qp.pnl_sol, qp.pnl_pct, qp.sol_in,
           coalesce(qp.channel_handle, '?') AS channel,
           pk.ever_peak, pk.first_mult,
           {sel}
    FROM qsim_positions qp
    JOIN calls  c   ON c.id   = qp.call_id
    JOIN tokens tok ON tok.id = qp.token_id
    LEFT JOIN pk ON pk.call_id = qp.call_id
    WHERE qp.status = 'closed'
      AND qp.entry_time >= now() - (%(days)s || ' days')::interval
      AND qp.sol_in > 0
    """


def _rows(days: int) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor
    import db
    db.ensure_qsim_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(_sql(days), {"days": days})
        return [dict(r) for r in cur.fetchall()]


def _f(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def auc(vals: list[float], labels: list[int]) -> float:
    """P(random winner ranks above random loser). Ties counted as half."""
    pairs = sorted(zip(vals, labels))
    n1 = sum(labels)
    n0 = len(labels) - n1
    if n1 == 0 or n0 == 0:
        return float("nan")
    # rank sum with tie-averaged ranks
    ranks = [0.0] * len(pairs)
    i = 0
    while i < len(pairs):
        j = i
        while j + 1 < len(pairs) and pairs[j + 1][0] == pairs[i][0]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            ranks[k] = avg
        i = j + 1
    r1 = sum(r for r, (_, lab) in zip(ranks, pairs) if lab == 1)
    return (r1 - n1 * (n1 + 1) / 2.0) / (n1 * n0)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--label", choices=("peak2x", "peak5x", "pnl"), default="peak2x",
                    help="peak2x (default): winner = ever reached 2x, which is "
                         "independent of how the bot exited. pnl: winner = "
                         "positive realised PnL, which is contaminated by exit "
                         "policy.")
    ap.add_argument("--channel", default=None)
    ap.add_argument("--min-cov", type=float, default=0.30)
    ap.add_argument("--perms", type=int, default=2000)
    ap.add_argument("--seed", type=int, default=20260923)
    args = ap.parse_args()

    rows = _rows(args.days)
    if args.channel:
        want = args.channel.lstrip("@").lower()
        rows = [r for r in rows if (r.get("channel") or "").lstrip("@").lower() == want]
    if not rows:
        print("no rows")
        return 1

    # entry_roundtrip: the first observable quote, i.e. what the spread cost you.
    for r in rows:
        r["entry_roundtrip"] = r.get("first_mult")

    def is_winner(r) -> int | None:
        if args.label == "pnl":
            v = _f(r.get("pnl_sol"))
            return None if v is None else int(v > 0)
        peak = _f(r.get("ever_peak"))
        if peak is None:
            return None
        return int(peak >= (2.0 if args.label == "peak2x" else 5.0))

    labelled = [(r, is_winner(r)) for r in rows]
    labelled = [(r, w) for r, w in labelled if w is not None]
    n = len(labelled)
    wins = sum(w for _, w in labelled)
    print(f"window     {args.days}d   {n} closed trades"
          + (f"   channel={args.channel}" if args.channel else ""))
    print(f"label      {args.label}   winners {wins} ({100 * wins / n:.1f}%), "
          f"losers {n - wins}")
    print()

    results = []
    for name, _ in FEATURES:
        pv = [(_f(r.get(name)), w) for r, w in labelled]
        pv = [(v, w) for v, w in pv if v is not None]
        cov = len(pv) / n
        if cov < args.min_cov or len(pv) < 100:
            continue
        vals = [v for v, _ in pv]
        labs = [w for _, w in pv]
        if sum(labs) < 10 or len(labs) - sum(labs) < 10:
            continue
        a = auc(vals, labs)
        w_med = statistics.median([v for v, l in pv if l == 1])
        l_med = statistics.median([v for v, l in pv if l == 0])
        results.append({"feature": name, "cov": cov, "n": len(pv), "auc": a,
                        "w_med": w_med, "l_med": l_med,
                        "vals": vals, "labs": labs})

    if not results:
        print("no feature has enough coverage to test")
        return 1

    hdr = (f"{'feature':<18}{'cov%':>7}{'n':>7}{'winner_med':>13}{'loser_med':>13}"
           f"{'AUC':>8}{'|AUC-.5|':>10}")
    print(hdr)
    print("-" * len(hdr))
    for r in sorted(results, key=lambda x: -abs(x["auc"] - 0.5)):
        print(f"{r['feature']:<18}{100 * r['cov']:>7.0f}{r['n']:>7}"
              f"{r['w_med']:>13.4g}{r['l_med']:>13.4g}"
              f"{r['auc']:>8.3f}{abs(r['auc'] - 0.5):>10.3f}")

    # ── family-wise: what does the best feature look like under the null? ────
    rng = random.Random(args.seed)
    observed = max(abs(r["auc"] - 0.5) for r in results)
    null_best = []
    for _ in range(args.perms):
        best = 0.0
        for r in results:
            labs = r["labs"][:]
            rng.shuffle(labs)
            best = max(best, abs(auc(r["vals"], labs) - 0.5))
        null_best.append(best)
    null_best.sort()
    p_fw = sum(1 for v in null_best if v >= observed) / len(null_best)

    print()
    print("=" * len(hdr))
    print("PERMUTATION TEST")
    print("=" * len(hdr))
    print(f"  best |AUC-0.5| observed   {observed:.3f}")
    print(f"  under the null            {statistics.median(null_best):.3f} typical, "
          f"{null_best[int(0.95 * len(null_best))]:.3f} at the 95th pct")
    print(f"  family-wise p             {p_fw:.4f}   ({args.perms} shuffles, "
          f"{len(results)} features)")
    print()
    if p_fw <= 0.05:
        print("  -> Something separates winners from losers. It may still be too")
        print("     weak to trade, but a multivariate model is now justified:")
        print("     interactions can exist where single features are flat.")
    else:
        print("  -> Nothing separates winners from losers on anything measured.")
        print("     That explains the sweep's null and closes the modelling")
        print("     question: there is no signal for a model to find.")
    print()
    print("  AUC 0.50 = noise. 0.55 = weak but real at large n. 0.60+ = worth")
    print("  modelling. Below 0.50 means the feature separates INVERSELY, which")
    print("  is equally usable — direction does not matter, distance from 0.5 does.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
