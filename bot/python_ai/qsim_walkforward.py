"""
qsim_walkforward.py — does model-selection + a 2x target survive walk-forward?

WHAT IS BEING TESTED
--------------------
Two things were found separately and have only been combined once, in-sample:

  1. A model trained on pre-entry features ranks coins that RUN. Out of sample
     it lifts the 2x rate 19.6% -> 33.1% in the top decile, with AUC beating a
     shuffled-label floor (0.610 vs 0.500).
  2. On that selected population, exit target and return are monotone:
     bank 1.2x -0.046, 1.3x -0.015, 1.4x +0.022, 1.5x +0.040, 1.75x +0.063,
     2x +0.108. Six levels, strictly increasing, replicated at top-10% and
     top-20%.

The mechanism is coherent — qsim partial-banks 70% at 1.3x, so a coin that runs
to 2x delivers only 30% of its move, and the model selects precisely for coins
that run. But the exit was chosen after seeing that table, the two runs were
NESTED rather than independent, and it rests on 29-56 hits.

WHAT THIS ADDS
--------------
WALK-FORWARD. The data is cut into folds by time. For each fold the model is
trained ONLY on trades that closed before it, then scores that fold, and the
exits are applied to the selection. Every fold is out of sample by construction,
and the folds are disjoint rather than nested.

A RANDOM-SELECTION CONTROL. For each fold a random subset of the SAME SIZE gets
the same exits, repeated many times. Selecting fewer trades cannot flatter a
per-SOL figure the way it flatters a total, but the control makes the comparison
explicit rather than argued: the model has to beat picking at random.

A BOOTSTRAP CI on the per-trade return, because +10.6%/SOL on 282 rows with 56
hits is a point estimate and the interval is what decides anything.

Exit semantics match qsim_quote_capture_replay: bank_X exits at the first HELD
quote at or above X; otherwise the position falls back to what qsim actually
booked. Post-exit probes are excluded — a bank cannot fire on a price seen after
the position closed.

Read-only. Executes nothing, writes nothing.

    python3 qsim_walkforward.py --days 30 --folds 4 --top 20
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

MAX_MULT = float(os.getenv("QSIM_REPLAY_MAX_QOBS_MULT", "1000"))
BANKS = (1.2, 1.3, 1.4, 1.5, 1.75, 2.0, 2.5, 3.0)

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


def _sql(days: int) -> str:
    sel = ",\n           ".join(f"{expr} AS {name}" for name, expr in FEATURES)
    return f"""
    WITH held AS (
        SELECT q.call_id,
               json_agg(q.real_mult ORDER BY q.observed_at) AS mults,
               max(q.real_mult) AS held_peak
        FROM qsim_quote_observations q
        JOIN qsim_positions p ON p.call_id = q.call_id
        WHERE q.real_mult IS NOT NULL AND q.real_mult > 0 AND q.real_mult <= {MAX_MULT}
          AND (q.note IS NULL OR q.note NOT LIKE 'post_exit_probe%%')
          AND q.observed_at <= p.exit_time + interval '5 seconds'
        GROUP BY q.call_id
    ),
    ever AS (
        SELECT q.call_id, max(q.real_mult) AS ever_peak
        FROM qsim_quote_observations q
        WHERE q.real_mult IS NOT NULL AND q.real_mult > 0 AND q.real_mult <= {MAX_MULT}
        GROUP BY q.call_id
    )
    SELECT qp.call_id, qp.entry_time, qp.pnl_pct,
           coalesce(qp.channel_handle, '?') AS channel,
           h.mults, h.held_peak, e.ever_peak,
           {sel}
    FROM qsim_positions qp
    JOIN calls  c   ON c.id   = qp.call_id
    JOIN tokens tok ON tok.id = qp.token_id
    LEFT JOIN held h ON h.call_id = qp.call_id
    LEFT JOIN ever e ON e.call_id = qp.call_id
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


def _mults(raw: Any) -> list[float]:
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    out = []
    for v in raw or []:
        f = _f(v)
        if f is not None and 0 < f <= MAX_MULT:
            out.append(f)
    return out


def bank_return(row: dict, level: float) -> float:
    """Return per 1 SOL deployed. Exit at the first HELD quote >= level,
    otherwise fall back to what qsim actually booked — same semantics as the
    replay's bank_* with fallback=current."""
    for m in _mults(row.get("mults")):
        if m >= level:
            return m - 1.0
    return (_f(row.get("pnl_pct")) or 0.0) / 100.0


def boot_ci(vals: list[float], iters: int, rng: random.Random) -> tuple[float, float]:
    if len(vals) < 5:
        return (float("nan"), float("nan"))
    idx = list(range(len(vals)))
    out = []
    for _ in range(iters):
        pick = rng.choices(idx, k=len(idx))
        out.append(sum(vals[i] for i in pick) / len(pick))
    out.sort()
    return (out[int(0.025 * len(out))], out[min(len(out) - 1, int(0.975 * len(out)))])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--folds", type=int, default=4)
    ap.add_argument("--top", type=float, default=20.0, help="percent to keep")
    ap.add_argument("--min-train", type=int, default=600)
    ap.add_argument("--channel", default=None)
    ap.add_argument("--boot", type=int, default=3000)
    ap.add_argument("--control-reps", type=int, default=200)
    ap.add_argument("--seed", type=int, default=20260923)
    args = ap.parse_args()

    try:
        import numpy as np
        from sklearn.linear_model import LogisticRegression
        from sklearn.preprocessing import StandardScaler
    except ImportError as e:
        print(f"needs scikit-learn and numpy: {e}")
        return 1

    rows = _rows(args.days)
    if args.channel:
        want = args.channel.lstrip("@").lower()
        rows = [r for r in rows if (r.get("channel") or "").lstrip("@").lower() == want]
    rows = [r for r in rows if _f(r.get("ever_peak")) is not None and _mults(r.get("mults"))]
    n = len(rows)
    if n < args.min_train + 200:
        print(f"only {n} usable rows")
        return 1

    names = [nm for nm, _ in FEATURES]
    rng = random.Random(args.seed)

    # fold boundaries after the minimum training block
    start = args.min_train
    edges = [start + round(i * (n - start) / args.folds) for i in range(args.folds + 1)]

    sel_ret: dict[float, list[float]] = {b: [] for b in BANKS}
    ctl_ret: dict[float, list[float]] = {b: [] for b in BANKS}
    sel_cur: list[float] = []
    all_cur: list[float] = []
    fold_lines = []

    for fi in range(args.folds):
        lo, hi = edges[fi], edges[fi + 1]
        tr, te = rows[:lo], rows[lo:hi]
        if len(te) < 30:
            continue
        ytr = np.array([1 if _f(r["ever_peak"]) >= 2.0 else 0 for r in tr])
        if ytr.sum() < 20 or (1 - ytr).sum() < 20:
            continue

        med = []
        for j, nm in enumerate(names):
            col = [_f(r.get(nm)) for r in tr]
            col = [v for v in col if v is not None]
            med.append(statistics.median(col) if col else 0.0)

        def mat(rs):
            return np.array([[(_f(r.get(nm)) if _f(r.get(nm)) is not None else med[j])
                              for j, nm in enumerate(names)] for r in rs], dtype=float)

        Xtr, Xte = mat(tr), mat(te)
        sc = StandardScaler().fit(Xtr)
        mdl = LogisticRegression(max_iter=2000, C=0.5).fit(sc.transform(Xtr), ytr)
        p = mdl.predict_proba(sc.transform(Xte))[:, 1]

        k = max(5, int(len(te) * args.top / 100.0))
        order = sorted(range(len(te)), key=lambda i: -p[i])[:k]
        chosen = [te[i] for i in order]

        for b in BANKS:
            sel_ret[b].extend(bank_return(r, b) for r in chosen)
        sel_cur.extend((_f(r.get("pnl_pct")) or 0.0) / 100.0 for r in chosen)
        all_cur.extend((_f(r.get("pnl_pct")) or 0.0) / 100.0 for r in te)

        # random-selection control, same size, same exits
        for _ in range(args.control_reps):
            pick = rng.sample(range(len(te)), k)
            for b in BANKS:
                ctl_ret[b].extend(bank_return(te[i], b) for i in pick)

        m2 = statistics.fmean([bank_return(r, 2.0) for r in chosen])
        fold_lines.append(f"  fold {fi + 1}: train {len(tr):>5}  test {len(te):>5}  "
                          f"selected {k:>4}  bank_2x {m2:+.4f}")

    if not sel_ret[2.0]:
        print("no usable folds")
        return 1

    print(f"window     {args.days}d   {n} trades"
          + (f"   channel={args.channel}" if args.channel else ""))
    print(f"walk-fwd   {args.folds} folds, min train {args.min_train}, "
          f"keep top {args.top:g}%  (every fold OUT OF SAMPLE, folds DISJOINT)")
    print()
    for ln in fold_lines:
        print(ln)
    print()

    base_all = statistics.fmean(all_cur)
    base_sel = statistics.fmean(sel_cur)
    print(f"  {'policy':<14}{'n':>7}{'mean/SOL':>11}{'ci_lo':>9}{'ci_hi':>9}"
          f"{'random ctl':>12}{'edge':>9}")
    print("  " + "-" * 69)
    print(f"  {'current(all)':<14}{len(all_cur):>7}{100 * base_all:>11.2f}"
          f"{'':>9}{'':>9}{'':>12}{'':>9}")
    print(f"  {'current(sel)':<14}{len(sel_cur):>7}{100 * base_sel:>11.2f}"
          f"{'':>9}{'':>9}{'':>12}{'':>9}")
    for b in BANKS:
        v = sel_ret[b]
        lo, hi = boot_ci(v, args.boot, rng)
        ctl = statistics.fmean(ctl_ret[b]) if ctl_ret[b] else float("nan")
        m = statistics.fmean(v)
        print(f"  {'bank_' + format(b, 'g') + 'x':<14}{len(v):>7}{100 * m:>11.2f}"
              f"{100 * lo:>9.2f}{100 * hi:>9.2f}{100 * ctl:>12.2f}"
              f"{100 * (m - ctl):>9.2f}")

    print()
    print("  mean/SOL is per 1 SOL deployed; ci is a bootstrap over the selected")
    print("  trades. 'random ctl' applies the SAME exit to a random subset of the")
    print("  same size, so 'edge' is what the MODEL contributes beyond the exit.")
    print("  A bank level can look good purely because the exit suits the whole")
    print("  book — the control is what separates those.")
    print()
    print("  Every fold trains only on trades that closed before it, and the folds")
    print("  do not overlap. That is stricter than the earlier single split, where")
    print("  top-10% and top-20% were nested and the exit was chosen after seeing")
    print("  the table.")
    print()
    print("  What would confirm: ci_lo above zero AND a positive edge over the")
    print("  random control AND the gradient still rising across bank levels.")
    print("  Any one of those alone is not enough.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
