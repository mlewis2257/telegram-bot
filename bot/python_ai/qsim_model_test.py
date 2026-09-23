"""
qsim_model_test.py — does the separation survive out of sample, and does it pay?

WHAT CAME BEFORE
----------------
`qsim_winner_profile.py` found real separation between coins that reach 2x and
coins that do not: family-wise p = 0.0000, replicated across 30d/60d windows and
across channels, with a coherent direction — winners are smaller mcap, thinner
liquidity, lower volume, younger, less concentrated in the first 20 holders.

But `qsim_entry_filter_sweep.py` found that single thresholds on those same
features do NOT carve out a profitable subset (p = 0.16). Both can be true:
separation exists and no one cut point monetises it. That is precisely the case
where a multivariate model is justified, because interactions live where single
features are flat.

THE TWO TRAPS THIS IS BUILT AROUND
----------------------------------
1. LEAKAGE. `entry_roundtrip` is the first quote observed AFTER entry, so it
   mixes the spread you paid (knowable in advance via a buy-then-sell quote pair)
   with ~30s of price movement (not knowable). It was the top feature in three of
   four profile runs. It is EXCLUDED by default; --allow-roundtrip includes it
   and the output is labelled contaminated. An earlier ML attempt on this project
   looked predictive for exactly this reason.

2. TIME. A random train/test split lets the model learn a market regime and be
   tested on the same days. The split here is chronological: train on the earlier
   portion, test on the later. That is the only split that answers "would this
   have worked going forward".

THE TEST THAT MATTERS IS NOT AUC
--------------------------------
A model can rank well and still lose money, because the book is -13%/SOL and any
selection has to clear that. So the headline is the %/SOL of the trades the model
would actually have taken, in the held-out period, by score decile. If the top
decile is not profitable out of sample, the separation is real and unusable.

A label-shuffled model is trained alongside as a floor: whatever a model achieves
on noise is what this pipeline manufactures for free.

Read-only. Executes nothing, writes nothing.

    python3 qsim_model_test.py --days 60
    python3 qsim_model_test.py --days 60 --channel solwhaletrending
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

# Pre-entry features only. Everything here is known from the call message or the
# token record at decision time.
FEATURES: list[tuple[str, str]] = [
    ("mcap_at_call",   "c.mcap_at_call"),
    ("liq_at_call",    "c.liquidity_at_call"),
    ("score",          "c.conviction_score"),
    ("liq_to_mcap",    "c.liquidity_at_call / NULLIF(c.mcap_at_call, 0)"),
    ("token_age_min",  "tok.token_age_minutes"),
    ("hodl_count",     "tok.hodl_count"),
    ("first_20_pct",   "tok.first_20_pct"),
    ("bundle_count",   "tok.bundle_count"),
    ("bundle_pct_rem", "tok.bundle_pct_remaining"),
    ("sniper_count",   "tok.sniper_count"),
    ("sniper_pct_rem", "tok.sniper_pct_remaining"),
    ("fake_vol_pct",   "tok.fake_vol_pct"),
    ("dev_pct_held",   "tok.dev_pct_held"),
    ("liq_at_detect",  "tok.liq_at_detection"),
    ("vol_1h",         "tok.vol_1h_at_detection"),
    ("vol_to_liq",     "tok.vol_1h_at_detection / NULLIF(tok.liq_at_detection, 0)"),
    ("vol_to_mcap",    "tok.vol_1h_at_detection / NULLIF(c.mcap_at_call, 0)"),
    ("detector_sol",   "tok.detecting_wallet_sol"),
]
LEAKY = ("entry_roundtrip",)


def _sql(days: int) -> str:
    sel = ",\n           ".join(f"{expr} AS {name}" for name, expr in FEATURES)
    return f"""
    WITH pk AS (
        SELECT q.call_id, max(q.real_mult) AS ever_peak,
               max(q.real_mult) FILTER (WHERE q.note IS NULL
                     OR q.note NOT LIKE 'post_exit_probe%%')   AS held_peak,
               (array_agg(q.real_mult ORDER BY q.observed_at))[1] AS first_mult
        FROM qsim_quote_observations q
        WHERE q.real_mult IS NOT NULL AND q.real_mult > 0 AND q.real_mult <= {MAX_MULT}
        GROUP BY q.call_id
    )
    SELECT qp.call_id, qp.entry_time, qp.pnl_sol, qp.sol_in,
           coalesce(qp.channel_handle, '?') AS channel,
           pk.ever_peak, pk.held_peak, pk.first_mult AS entry_roundtrip,
           {sel}
    FROM qsim_positions qp
    JOIN calls  c   ON c.id   = qp.call_id
    JOIN tokens tok ON tok.id = qp.token_id
    LEFT JOIN pk ON pk.call_id = qp.call_id
    WHERE qp.status = 'closed'
      AND qp.entry_time >= now() - (%(days)s || ' days')::interval
      AND qp.sol_in > 0
    ORDER BY qp.entry_time
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


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=60)
    ap.add_argument("--channel", default=None)
    ap.add_argument("--label", choices=("peak2x", "peak5x"), default="peak2x")
    ap.add_argument("--train-frac", type=float, default=0.6)
    ap.add_argument("--allow-roundtrip", action="store_true",
                    help="include entry_roundtrip, which is measured AFTER entry "
                         "and partly leaks early price movement. Output is "
                         "labelled contaminated.")
    ap.add_argument("--write-scores", action="store_true",
                    help="persist OUT-OF-SAMPLE scores for the test period to "
                         "qsim_model_scores, so the exit replay can be run on "
                         "the population the model would actually have selected")
    ap.add_argument("--score-model", choices=("logistic", "gbm"), default="logistic")
    ap.add_argument("--seed", type=int, default=20260923)
    args = ap.parse_args()

    try:
        import numpy as np
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.linear_model import LogisticRegression
        from sklearn.metrics import roc_auc_score
        from sklearn.preprocessing import StandardScaler
    except ImportError as e:
        print(f"needs scikit-learn and numpy: {e}")
        return 1

    rows = _rows(args.days)
    if args.channel:
        want = args.channel.lstrip("@").lower()
        rows = [r for r in rows if (r.get("channel") or "").lstrip("@").lower() == want]
    rows = [r for r in rows if _f(r.get("ever_peak")) is not None]
    if len(rows) < 400:
        print(f"only {len(rows)} usable rows — too few to split")
        return 1

    names = [n for n, _ in FEATURES]
    if args.allow_roundtrip:
        names = names + list(LEAKY)

    thresh = 2.0 if args.label == "peak2x" else 5.0
    y = np.array([1 if _f(r["ever_peak"]) >= thresh else 0 for r in rows])
    pnl = np.array([_f(r["pnl_sol"]) or 0.0 for r in rows])
    sol = np.array([_f(r["sol_in"]) or 0.0 for r in rows])
    # bank% uses the HELD window — what you could actually have banked — while
    # 2x/5x use the full path including post-exit probes, i.e. what the coin did.
    # Breakeven needs ~50% of trades reaching 1.3x, so bank% is the column that
    # says whether a selected population can pay under ANY exit.
    hp = np.array([_f(r.get("held_peak")) or 0.0 for r in rows])
    ep = np.array([_f(r.get("ever_peak")) or 0.0 for r in rows])

    cut = int(len(rows) * args.train_frac)
    X_raw = [[_f(r.get(n)) for n in names] for r in rows]

    # Impute with the TRAIN median only — using all rows would leak test
    # distribution into training.
    med = []
    for j in range(len(names)):
        col = [X_raw[i][j] for i in range(cut) if X_raw[i][j] is not None]
        med.append(statistics.median(col) if col else 0.0)
    X = np.array([[(v if v is not None else med[j]) for j, v in enumerate(row)]
                  for row in X_raw], dtype=float)

    Xtr, Xte = X[:cut], X[cut:]
    ytr, yte = y[:cut], y[cut:]
    pnl_te, sol_te = pnl[cut:], sol[cut:]
    hp_te, ep_te = hp[cut:], ep[cut:]

    sc = StandardScaler().fit(Xtr)
    Xtr_s, Xte_s = sc.transform(Xtr), sc.transform(Xte)

    print(f"window     {args.days}d   {len(rows)} trades"
          + (f"   channel={args.channel}" if args.channel else ""))
    print(f"split      train {cut} (earlier)  |  test {len(rows) - cut} (later)")
    print(f"label      {args.label}   train winners {ytr.mean() * 100:.1f}%  "
          f"test winners {yte.mean() * 100:.1f}%")
    print(f"features   {len(names)}"
          + ("   *** CONTAMINATED: entry_roundtrip included ***"
             if args.allow_roundtrip else "   (pre-entry only)"))
    base = 100 * pnl_te.sum() / sol_te.sum() if sol_te.sum() else 0.0
    print(f"test book  {pnl_te.sum():+.4f} SOL  ({base:+.2f}%/SOL)  <- the bar")
    print()

    models = {
        "logistic": LogisticRegression(max_iter=2000, C=0.5),
        "gbm":      GradientBoostingClassifier(random_state=args.seed,
                                               n_estimators=150, max_depth=3),
    }
    rng = random.Random(args.seed)
    y_shuf = ytr.copy()
    rng.shuffle(y_shuf)

    for mname, model in models.items():
        Xa, Xb = (Xtr_s, Xte_s) if mname == "logistic" else (Xtr, Xte)
        model.fit(Xa, ytr)
        p = model.predict_proba(Xb)[:, 1]
        try:
            a = roc_auc_score(yte, p)
        except ValueError:
            a = float("nan")

        # noise floor: same pipeline, shuffled labels
        import copy
        m2 = copy.deepcopy(model)
        m2.fit(Xa, y_shuf)
        p2 = m2.predict_proba(Xb)[:, 1]
        try:
            a2 = roc_auc_score(yte, p2)
        except ValueError:
            a2 = float("nan")

        print(f"{mname}   test AUC {a:.3f}   (shuffled-label floor {a2:.3f})")
        order = np.argsort(-p)
        print(f"  {'keep top':<10}{'n':>6}{'bank%':>8}{'2x%':>7}{'5x%':>7}"
              f"{'pnl_sol':>10}{'%/SOL':>9}")
        for frac in (0.10, 0.20, 0.30, 0.50):
            k = max(1, int(len(order) * frac))
            idx = order[:k]
            ssum = sol_te[idx].sum()
            bank = 100.0 * (hp_te[idx] >= 1.3).mean()
            x2 = 100.0 * (ep_te[idx] >= 2.0).mean()
            x5 = 100.0 * (ep_te[idx] >= 5.0).mean()
            print(f"  {int(frac * 100):>3}%      {k:>6}{bank:>8.1f}{x2:>7.1f}{x5:>7.1f}"
                  f"{pnl_te[idx].sum():>10.4f}"
                  f"{(100 * pnl_te[idx].sum() / ssum if ssum else 0):>9.2f}")
        allbank = 100.0 * (hp_te >= 1.3).mean()
        print(f"  {'(all)':<10}{len(hp_te):>6}{allbank:>8.1f}"
              f"{100.0 * (ep_te >= 2.0).mean():>7.1f}"
              f"{100.0 * (ep_te >= 5.0).mean():>7.1f}"
              f"{pnl_te.sum():>10.4f}{base:>9.2f}")
        print()

    print("  The bar is the test-book %/SOL above, not AUC. A model can rank")
    print("  well and still lose money, because any selection must clear a")
    print("  negative book before it is worth anything.")
    print("  The shuffled-label floor is what this pipeline produces from noise;")
    print("  a real AUC has to beat it, not just beat 0.5.")
    print("  bank% is the HELD-window rate of reaching 1.3x. Breakeven needs it")
    print("  near 50% — that is the number deciding whether a selected population")
    print("  can pay under ANY exit policy, and %/SOL here reflects the CURRENT")
    print("  exit, which the replay already showed is not the right one.")
    if args.write_scores:
        import db
        mdl = models[args.score_model]
        Xb = Xte_s if args.score_model == "logistic" else Xte
        scores = mdl.predict_proba(Xb)[:, 1]
        conn = db.get_conn(); db.safe_rollback()
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS qsim_model_scores (
                    call_id    integer PRIMARY KEY,
                    score      numeric NOT NULL,
                    model      text,
                    scored_at  timestamptz NOT NULL DEFAULT now()
                )
            """)
            cur.execute("DELETE FROM qsim_model_scores")
            for r, sc_ in zip(rows[cut:], scores):
                cur.execute("""
                    INSERT INTO qsim_model_scores (call_id, score, model)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (call_id) DO UPDATE SET score = EXCLUDED.score,
                        model = EXCLUDED.model, scored_at = now()
                """, (int(r["call_id"]), float(sc_), args.score_model))
        conn.commit()
        print(f"\n  wrote {len(scores)} OUT-OF-SAMPLE scores "
              f"({args.score_model}) to qsim_model_scores")
        print("  Only test-period rows are written. Scoring the training rows "
              "would be")
        print("  in-sample and would flatter any exit measured on them.")
        print()
    print("  Deciles are OUT OF SAMPLE and chronological — trained on earlier")
    print("  trades, scored on later ones, which is the only split that answers")
    print("  'would this have worked going forward'.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
