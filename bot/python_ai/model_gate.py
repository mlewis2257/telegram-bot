"""
model_gate.py — score a call at ENTRY time and skip the ones the model ranks low.

WHY THIS EXISTS
---------------
`qsim_model_test.py` found that a GBM over 18 pre-entry features ranks coins that
will reach 2x above coins that will not, out of sample and chronologically split:
AUC 0.627 against a shuffled-label floor of 0.519, with the top decile lifting
the 2x rate from 18.7% to 35.5% and the 5x rate from 4.0% to 16.1%.

That result lived only in the analysis tools. `qsim_model_scores` was read by
`qsim_model_test.py` and the replay and by nothing else, so the selection could
be measured after the fact and never acted on. This is the missing half: the
same model, loaded once and asked a question at the moment a call arrives.

The exit half is already wired (`lock_or_bank_1p75x_1p35x`), and it is WORSE than
the old config on the full book and better only on the model's top decile —
because it wins by not capping a tail, and the untouched book has too few tails
to pay for the trades where the floor gives back. The two halves only work
together, which is why this one has to exist for either to mean anything.

SHADOW BY DEFAULT, AND WHY THAT IS NOT TIMIDITY
-----------------------------------------------
Default mode is `shadow`: the gate scores, logs, and allows. That keeps qsim
trading the coins it would block, which is the CONTROL ARM. Without it a
filtered book can only be compared against a different month, and that confound
already made one month look ten points better than another on a filter that did
nothing. Enforce in live, shadow in qsim — the same split `dev_gate` uses and
for the same reason.

FAIL OPEN, ALWAYS
-----------------
Any failure — no model file, a missing feature, a DB hiccup, a slow query —
allows the trade. A gate that blocks on error silently stops the bot, and a
scoring model is not worth that risk. Every exception path returns allowed=True.

THE THRESHOLD IS CALIBRATED OUT OF SAMPLE
-----------------------------------------
`--train` splits chronologically, fits on the earlier portion, and takes the
score cut from the HELD-OUT portion's distribution. Taking the cut from training
scores would put the threshold where the model is overconfident and let far more
than the intended percentage through.

    python3 model_gate.py --train --days 30          # fit, calibrate, persist
    python3 model_gate.py --report                   # blocked vs allowed outcomes
    python3 model_gate.py --check 279013             # score one call

Environment
-----------
MODEL_GATE_MODE        shadow | enforce   (default shadow)
MODEL_GATE_MODE_QSIM   overrides for qsim   (default: MODEL_GATE_MODE)
MODEL_GATE_MODE_LIVE   overrides for live   (default: MODEL_GATE_MODE)
MODEL_GATE_TOP_PCT     keep the top N% by score (default 10)
MODEL_GATE_CHANNELS    comma list; empty = all channels
MODEL_GATE_TIMEOUT_MS  budget for the feature query (default 800)
MODEL_GATE_MAX_AGE_H   refuse to use a model older than this, in hours (default 168)
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))

import db

# The feature list is duplicated from qsim_model_test.py ON PURPOSE. If that file
# changes its features, this gate must be retrained rather than silently scoring
# with a different vector than it was fitted on — a shared import would hide that.
# _FEATURE_NAMES is persisted with the model and checked on load.
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
FEATURE_NAMES = [n for n, _ in FEATURES]

MAX_MULT   = float(os.getenv("QSIM_REPLAY_MAX_QOBS_MULT", "1000"))
MODE       = os.getenv("MODEL_GATE_MODE", "shadow").strip().lower()
MODE_QSIM  = (os.getenv("MODEL_GATE_MODE_QSIM", "").strip().lower() or MODE)
MODE_LIVE  = (os.getenv("MODEL_GATE_MODE_LIVE", "").strip().lower() or MODE)
TOP_PCT    = float(os.getenv("MODEL_GATE_TOP_PCT", "10"))
TIMEOUT_MS = float(os.getenv("MODEL_GATE_TIMEOUT_MS", "800"))
MAX_AGE_H  = float(os.getenv("MODEL_GATE_MAX_AGE_H", "168"))
CHANNELS   = {c.strip().lstrip("@").lower()
              for c in os.getenv("MODEL_GATE_CHANNELS", "").split(",") if c.strip()}

_MODEL_DIR  = os.path.join(os.path.dirname(__file__), ".models")
_MODEL_FILE = os.path.join(_MODEL_DIR, "model_gate.joblib")

_bundle: dict | None = None
_bundle_mtime: float = 0.0
_TABLE_READY = False
_LOG_FAILED_ONCE = False


def mode_for(context: str) -> str:
    return MODE_LIVE if context == "live" else MODE_QSIM


@dataclass(frozen=True)
class Decision:
    allowed: bool
    reason: str
    score: float | None
    threshold: float | None
    latency_ms: float


# ── Feature access ────────────────────────────────────────────────────────────

_FEATURE_SQL_TRAIN = """
WITH pk AS (
    SELECT q.call_id,
           max(q.real_mult) FILTER (
               WHERE q.note IS NULL OR q.note NOT LIKE 'post_exit_probe%%'
           ) AS held_peak
    FROM qsim_quote_observations q
    WHERE q.real_mult IS NOT NULL AND q.real_mult > 0 AND q.real_mult <= {maxmult}
    GROUP BY q.call_id
)
SELECT qp.call_id, qp.entry_time,
       coalesce(qp.channel_handle, '?') AS channel,
       pk.held_peak,
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

# At decision time there is no position row yet, so tokens is reached through
# calls.token_id rather than qsim_positions.token_id.
_FEATURE_SQL_ONE = """
SELECT {sel}
FROM calls c
JOIN tokens tok ON tok.id = c.token_id
WHERE c.id = %(call_id)s
"""


def _sel() -> str:
    return ",\n       ".join(f"{expr} AS {name}" for name, expr in FEATURES)


def _f(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f          # drop NaN


def _vector(row: dict) -> list[float]:
    """Feature vector with missing values as 0.0.

    GBM handles a zero-filled missing value as just another split point, and the
    training rows are filled the same way, so the model learns what a missing
    field means for this data rather than having it imputed to something the
    caller invented.
    """
    return [(_f(row.get(n)) or 0.0) for n in FEATURE_NAMES]


# ── Model load / persist ──────────────────────────────────────────────────────

def _load() -> dict | None:
    """Load the persisted bundle, re-reading it when the file changes on disk.

    Re-reading on mtime means a retrain lands in a running qsim without a
    restart, which matters because the alternative is a model quietly going
    stale for as long as the process happens to live.
    """
    global _bundle, _bundle_mtime
    try:
        mtime = os.path.getmtime(_MODEL_FILE)
    except OSError:
        return None
    if _bundle is not None and mtime == _bundle_mtime:
        return _bundle
    try:
        import joblib
        b = joblib.load(_MODEL_FILE)
    except Exception as e:
        print(f"[model_gate] could not load model (allowing everything): {e}")
        return None
    if list(b.get("features") or []) != FEATURE_NAMES:
        print("[model_gate] model was fitted on a DIFFERENT feature list — "
              "refusing to use it. Retrain with --train.")
        return None
    _bundle, _bundle_mtime = b, mtime
    return b


def model_age_hours(b: dict) -> float:
    ts = b.get("trained_at")
    if not isinstance(ts, datetime):
        return 1e9
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - ts).total_seconds() / 3600.0


# ── Decision log ──────────────────────────────────────────────────────────────

def ensure_table() -> None:
    global _TABLE_READY
    if _TABLE_READY:
        return
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS model_gate_log (
                id          BIGSERIAL PRIMARY KEY,
                call_id     BIGINT,
                channel     TEXT,
                context     TEXT,
                mode        TEXT,
                allowed     BOOLEAN,
                reason      TEXT,
                score       DOUBLE PRECISION,
                threshold   DOUBLE PRECISION,
                latency_ms  DOUBLE PRECISION,
                created_at  TIMESTAMPTZ DEFAULT now()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS model_gate_log_call_idx "
                    "ON model_gate_log (call_id)")
    conn.commit()
    _TABLE_READY = True


def _log(call_id, channel, d: Decision, context: str, mode: str) -> None:
    ensure_table()
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO model_gate_log (call_id, channel, context, mode, allowed,"
            " reason, score, threshold, latency_ms) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (call_id, channel, context, mode, d.allowed, d.reason,
             d.score, d.threshold, d.latency_ms),
        )
    conn.commit()


# ── The gate ──────────────────────────────────────────────────────────────────

async def check(call_id: int | None, channel: str | None,
                context: str = "qsim") -> Decision:
    """Score this call and decide. NEVER raises; every failure allows the trade.

    `context` picks the mode so qsim can stay in shadow — keeping both arms of
    the forward test — while live enforces.
    """
    t0 = time.monotonic()
    mode = mode_for(context)

    def done(allowed: bool, reason: str, score=None, thr=None) -> Decision:
        ms = (time.monotonic() - t0) * 1000.0
        d = Decision(allowed, reason, score, thr, ms)
        try:
            _log(call_id, channel, d, context, mode)
        except Exception as e:
            global _LOG_FAILED_ONCE
            if not _LOG_FAILED_ONCE:
                _LOG_FAILED_ONCE = True
                print(f"[model_gate] log failed (continuing): {e}")
        return d

    try:
        if mode not in ("shadow", "enforce"):
            return done(True, f"mode_off:{mode}")
        if call_id is None:
            return done(True, "no_call_id")
        handle = (channel or "").lstrip("@").lower()
        if CHANNELS and handle not in CHANNELS:
            return done(True, "channel_not_gated")

        b = _load()
        if b is None:
            return done(True, "no_model")
        age = model_age_hours(b)
        if MAX_AGE_H > 0 and age > MAX_AGE_H:
            # A stale model is a model fitted on a market that may no longer
            # exist. Allowing is the safe direction; blocking on stale data is
            # how a gate silently turns the bot off.
            return done(True, f"model_stale:{age:.0f}h")

        row = _features_for_call(call_id)
        if row is None:
            return done(True, "no_features")

        score = float(b["model"].predict_proba([_vector(row)])[0][1])
        thr = float(b["threshold"])
        if score >= thr:
            return done(True, "pass", score, thr)
        if mode == "shadow":
            return done(True, "shadow_would_block", score, thr)
        return done(False, "below_threshold", score, thr)
    except Exception as e:
        ms = (time.monotonic() - t0) * 1000.0
        print(f"[model_gate] error (allowing): {type(e).__name__} {e}")
        return Decision(True, f"error:{type(e).__name__}", None, None, ms)


def _recent_call_ids(n: int) -> list[int]:
    """Most recent calls that actually have a token row — the population the
    gate will be asked about. Used by --check so proving the decision-time
    join does not require hunting for a call_id by hand."""
    try:
        conn = db.get_conn()
        db.safe_rollback()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT c.id FROM calls c JOIN tokens tok ON tok.id = c.token_id "
                "ORDER BY c.id DESC LIMIT %s", (n,))
            return [int(r[0]) for r in cur.fetchall()]
    except Exception as e:
        print(f"[model_gate] could not list recent calls: {e}")
        return []


def _features_for_call(call_id: int) -> dict | None:
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"SET LOCAL statement_timeout = {int(TIMEOUT_MS)}")
        cur.execute(_FEATURE_SQL_ONE.format(sel=_sel()), {"call_id": call_id})
        r = cur.fetchone()
    return dict(r) if r else None


# ── Training ──────────────────────────────────────────────────────────────────

def train(days: int, top_pct: float, test_frac: float = 0.35) -> dict:
    from psycopg2.extras import RealDictCursor
    from sklearn.ensemble import GradientBoostingClassifier
    import joblib
    import numpy as np

    db.ensure_qsim_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(_FEATURE_SQL_TRAIN.format(sel=_sel(), maxmult=MAX_MULT),
                    {"days": days})
        rows = [dict(r) for r in cur.fetchall()]

    rows = [r for r in rows if _f(r.get("held_peak")) is not None]
    if len(rows) < 200:
        raise SystemExit(f"only {len(rows)} usable rows — too few to fit a gate")

    rows.sort(key=lambda r: r["entry_time"])          # chronological, never random
    cut = int(len(rows) * (1.0 - test_frac))
    tr, te = rows[:cut], rows[cut:]

    X_tr = np.array([_vector(r) for r in tr], dtype=float)
    y_tr = np.array([1 if _f(r["held_peak"]) >= 2.0 else 0 for r in tr])
    X_te = np.array([_vector(r) for r in te], dtype=float)
    y_te = np.array([1 if _f(r["held_peak"]) >= 2.0 else 0 for r in te])

    if y_tr.sum() < 20 or y_te.sum() < 10:
        raise SystemExit(f"too few winners to fit (train {y_tr.sum()}, test {y_te.sum()})")

    # Deliberately IDENTICAL to qsim_model_test.py's gbm (n_estimators=150,
    # max_depth=3, default learning rate). The gate exists to act on that
    # tool's result; fitting a differently-tuned model here would make the
    # measured AUC and decile lift describe something this never runs.
    model = GradientBoostingClassifier(random_state=20260924,
                                       n_estimators=150, max_depth=3)
    model.fit(X_tr, y_tr)

    # The cut comes from HELD-OUT scores. Taking it from training scores puts it
    # where the model is overconfident, and far more than top_pct then passes.
    s_te = model.predict_proba(X_te)[:, 1]
    threshold = float(np.percentile(s_te, 100.0 - top_pct))

    try:
        from sklearn.metrics import roc_auc_score
        auc = float(roc_auc_score(y_te, s_te))
    except Exception:
        auc = float("nan")

    sel = s_te >= threshold
    bundle = {
        "model": model,
        "features": FEATURE_NAMES,
        "threshold": threshold,
        "top_pct": top_pct,
        "trained_at": datetime.now(timezone.utc),
        "n_train": len(tr),
        "n_test": len(te),
        "test_auc": auc,
        "base_2x_rate": float(y_te.mean()),
        "selected_2x_rate": float(y_te[sel].mean()) if sel.sum() else float("nan"),
        "n_selected": int(sel.sum()),
    }
    os.makedirs(_MODEL_DIR, exist_ok=True)
    joblib.dump(bundle, _MODEL_FILE)
    return bundle


# ── CLI ───────────────────────────────────────────────────────────────────────

def _report() -> int:
    from psycopg2.extras import RealDictCursor
    ensure_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT g.context, g.mode, g.reason,
                   count(*) AS n,
                   count(qp.call_id) AS closed,
                   avg(qp.pnl_sol / NULLIF(qp.sol_in,0)) AS avg_ret,
                   sum(qp.pnl_sol) AS pnl_sol
            FROM model_gate_log g
            LEFT JOIN qsim_positions qp
                   ON qp.call_id = g.call_id AND qp.status = 'closed'
            GROUP BY g.context, g.mode, g.reason
            ORDER BY g.context, g.reason
        """)
        rows = [dict(r) for r in cur.fetchall()]
    if not rows:
        print("no gate decisions logged yet")
        return 0
    hdr = f"{'context':<8}{'mode':<9}{'reason':<22}{'n':>7}{'closed':>8}{'avg_ret':>10}{'pnl_sol':>10}"
    print(hdr)
    print("-" * len(hdr))
    for r in rows:
        ar = f"{float(r['avg_ret']):+.3f}" if r["avg_ret"] is not None else "   -  "
        pn = f"{float(r['pnl_sol']):+.3f}" if r["pnl_sol"] is not None else "   -  "
        print(f"{r['context']:<8}{r['mode']:<9}{str(r['reason'])[:21]:<22}"
              f"{r['n']:>7}{r['closed']:>8}{ar:>10}{pn:>10}")
    print()
    print("  In SHADOW the blocked rows still traded, so `shadow_would_block` vs")
    print("  `pass` is the forward test: pass should beat would_block on avg_ret.")
    print("  Until both have closed trades this table proves nothing.")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--train", action="store_true")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--check", nargs="?", type=int, const=0, default=None,
                    metavar="CALL_ID",
                    help="score one call; with no id, scores the 5 most recent "
                         "calls that have a token row — which is what proves the "
                         "decision-time join works")
    # 30, not 60: qsim's record past ~30 days predates fixes and is not
    # comparable. Training on 60d measurably degrades the model — AUC 0.586
    # and a 13.0% base 2x rate, against 0.627 and 18.7% on the recent window.
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--top-pct", type=float, default=TOP_PCT)
    args = ap.parse_args()

    if args.train:
        b = train(args.days, args.top_pct)
        print(f"trained on {args.days}d  train={b['n_train']} test={b['n_test']}")
        print(f"  test AUC           {b['test_auc']:.3f}")
        print(f"  threshold          {b['threshold']:.4f}  (top {b['top_pct']:g}%)")
        print(f"  2x rate  base      {100*b['base_2x_rate']:.1f}%")
        print(f"  2x rate  selected  {100*b['selected_2x_rate']:.1f}%  "
              f"(n={b['n_selected']})")
        print(f"  saved              {_MODEL_FILE}")
        print()
        print("  The selected 2x rate is measured on the SAME held-out rows the")
        print("  threshold came from, so it is optimistic. The honest number is")
        print("  the forward one in --report once shadow decisions have closed.")
        return 0

    if args.check is not None:
        import asyncio
        b = _load()
        if b is None:
            print("no usable model — run --train first")
            return 1
        ids = [args.check] if args.check else _recent_call_ids(5)
        if not ids:
            print("no recent calls with a token row — nothing to score")
            return 1
        print(f"model age {model_age_hours(b):.1f}h  test AUC "
              f"{b.get('test_auc', float('nan')):.3f}  threshold {b['threshold']:.4f}")
        print(f"{'call_id':>10}{'allowed':>9}{'reason':>22}{'score':>9}{'ms':>6}")
        print("-" * 56)
        ok = 0
        for cid in ids:
            d = asyncio.run(check(cid, None, context="qsim"))
            sc = "   -  " if d.score is None else f"{d.score:.4f}"
            if d.reason not in ("no_features", "no_model"):
                ok += 1
            print(f"{cid:>10}{str(d.allowed):>9}{d.reason[:21]:>22}{sc:>9}{d.latency_ms:>6.0f}")
        print()
        if ok:
            print(f"  {ok}/{len(ids)} scored — the decision-time join works.")
        else:
            print("  NOTHING SCORED. Every row came back no_features, which means the")
            print("  calls -> tokens join at decision time is not resolving. The gate")
            print("  fails open, so it would allow everything and silently never gate.")
            return 1
        return 0

    if args.report:
        return _report()

    ap.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
