"""
reprice_analysis.py — is the paper/shadow backtest lying about entries?

THE PROBLEM (see memory: live_execution_haircut / live_quote_driven_exits_phase2)
--------------------------------------------------------------------------------
paper_trader books the ENTRY at the live price-feed mcap (paper_trader.py:184
`entry_price = actual_entry or msg_mcap`). Live wallet fills proved that feed is a
systematic LIAR at entry — it reads ~0.68x of the real fill on fast/fresh coins
(range 0.55-0.96), so paper buys a fake-CHEAP entry and books an inflated multiple.
The call-time estimate `mcap_at_call`, by contrast, tracks the real fill within
~+-15% (fill/call ~1.0). So the honest cost basis is mcap_at_call, not the feed.

WHAT THIS SCRIPT DOES (it changes NOTHING — read-only, like lane_policy_review)
------------------------------------------------------------------------------
1. VALIDATE on LIVE trades (the only rows with ground-truth realized wallet P&L):
   compare three ways of estimating each trade's P&L against what the wallet ACTUALLY
   made, so we KNOW which repricing predicts live before trusting it on paper:
       A feed/feed  = sol_in*(exit_price      / entry_price)      - sol_in   [what paper does now]
       B call/feed  = sol_in*(exit_price      / mcap_at_call)     - sol_in   [proposed reprice]
       C fill/fill  = sol_in*(exit_price_fill / entry_price_fill) - sol_in   [real, live-only]
   Plus an entry-estimator check: how close is mcap_at_call vs feed to the real fill.

2. APPLY the reprice to PAPER (Strategy A or B), per lane: booked P&L vs repriced
   (entry -> mcap_at_call), so you see the honest haircut per lane before it drives
   any sizing decision.

    python3 reprice_analysis.py                 # 14d, Strategy A
    python3 reprice_analysis.py --days 30
    python3 reprice_analysis.py --strategy b
    python3 reprice_analysis.py --live-days 60  # widen the live validation window
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from psycopg2.extras import RealDictCursor

import db

# Phantom guard — identical to lane_policy_review / shadow_report so numbers tie out.
MAX_SANE_PEAK    = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5


def _rows(sql: str, params: dict) -> list[dict]:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def _pnl(sol_in: float, exit_mcap: float, entry_mcap: float) -> float | None:
    """Paper's own P&L model: sol_out = sol_in * (exit/entry); pnl = sol_out - sol_in."""
    if not sol_in or not entry_mcap or entry_mcap <= 0 or exit_mcap is None:
        return None
    return sol_in * (exit_mcap / entry_mcap) - sol_in


# ─────────────────────────────────────────────────────────────────────────────
# 1. LIVE VALIDATION — which repricing scheme predicts realized wallet P&L?
# ─────────────────────────────────────────────────────────────────────────────

LIVE_SQL = """
SELECT t.symbol,
       c.mcap_at_call,
       tp.entry_price, tp.exit_price,
       tp.entry_price_fill, tp.exit_price_fill,
       tp.sol_in, tp.sol_out, tp.pnl_sol,
       tp.exit_reason, tp.entry_time
FROM trading_positions tp
JOIN calls  c ON c.id = tp.call_id
JOIN tokens t ON t.id = c.token_id
WHERE tp.is_simulation = FALSE
  AND tp.status = 'closed'
  AND tp.entry_time >= now() - (%(days)s || ' days')::interval
ORDER BY tp.entry_time
"""


def _mae_bias(ests: list[float], truth: list[float]) -> tuple[float, float]:
    """Mean absolute error and mean signed error (bias) of ests vs truth."""
    pairs = [(e, t) for e, t in zip(ests, truth) if e is not None and t is not None]
    if not pairs:
        return 0.0, 0.0
    mae  = sum(abs(e - t) for e, t in pairs) / len(pairs)
    bias = sum(e - t for e, t in pairs) / len(pairs)
    return mae, bias


def live_validation(days: int) -> None:
    rows = _rows(LIVE_SQL, {"days": days})
    print("=" * 78)
    print(f"  LIVE VALIDATION  (ground truth = realized wallet P&L, sol_out - sol_in)")
    print(f"  window: last {days}d   n = {len(rows)} closed live trades")
    print("=" * 78)
    if not rows:
        print("  no closed live trades in window — widen with --live-days\n")
        return

    truth, est_A, est_B, est_C = [], [], [], []
    ent_call_ratio, ent_feed_ratio = [], []

    hdr = (f"  {'symbol':<10} {'realized':>9} {'A f/f':>8} {'B c/f':>8} {'C fill':>8}"
           f"   {'fill/feed':>9} {'fill/call':>9}  reason")
    print(hdr)
    print("  " + "-" * (len(hdr) - 2))
    for r in rows:
        si   = float(r["sol_in"] or 0)
        real = float(r["pnl_sol"]) if r["pnl_sol"] is not None else (
            (float(r["sol_out"]) - si) if r["sol_out"] is not None else None)
        a = _pnl(si, r["exit_price"],      r["entry_price"])
        b = _pnl(si, r["exit_price"],      r["mcap_at_call"])
        c = _pnl(si, r["exit_price_fill"], r["entry_price_fill"])
        truth.append(real); est_A.append(a); est_B.append(b); est_C.append(c)

        fill = r["entry_price_fill"]
        fr = cr = None
        if fill and r["entry_price"] and r["entry_price"] > 0:
            fr = float(fill) / float(r["entry_price"]); ent_feed_ratio.append(fr)
        if fill and r["mcap_at_call"] and r["mcap_at_call"] > 0:
            cr = float(fill) / float(r["mcap_at_call"]); ent_call_ratio.append(cr)

        def f(x, w=8): return f"{x:>{w}.4f}" if x is not None else f"{'—':>{w}}"
        print(f"  {r['symbol'][:10]:<10} {f(real,9)} {f(a)} {f(b)} {f(c)}"
              f"   {(f'{fr:.2f}x' if fr else '—'):>9} {(f'{cr:.2f}x' if cr else '—'):>9}"
              f"  {r['exit_reason']}")

    def total(xs): return sum(x for x in xs if x is not None)
    print("  " + "-" * (len(hdr) - 2))
    print(f"  {'TOTAL':<10} {total(truth):>9.4f} {total(est_A):>8.4f} "
          f"{total(est_B):>8.4f} {total(est_C):>8.4f}")

    print("\n  Accuracy vs realized (lower MAE = better predictor of live):")
    for name, ests, tag in (("A feed/feed", est_A, "what paper books now"),
                            ("B call/feed", est_B, "proposed reprice"),
                            ("C fill/fill", est_C, "real, live-only")):
        mae, bias = _mae_bias(ests, truth)
        print(f"    {name:<12} MAE={mae:.4f}  bias={bias:+.4f} SOL/trade   ({tag})")

    if ent_feed_ratio:
        def stats(v): return sum(v) / len(v), min(v), max(v)
        fm, flo, fhi = stats(ent_feed_ratio)
        print(f"\n  Entry estimator vs REAL fill  (want mean~1.0, tight range):")
        print(f"    feed entry_price : real/feed mean={fm:.2f}x  range {flo:.2f}-{fhi:.2f}  <- the liar")
        if ent_call_ratio:
            cm, clo, chi = stats(ent_call_ratio)
            print(f"    mcap_at_call     : real/call mean={cm:.2f}x  range {clo:.2f}-{chi:.2f}  <- honest anchor")
    print()


# ─────────────────────────────────────────────────────────────────────────────
# 2. PAPER REPRICING — booked P&L vs entry->mcap_at_call, per lane
# ─────────────────────────────────────────────────────────────────────────────

PAPER_SQL = """
SELECT COALESCE(ch.handle, '?')          AS channel,
       COALESCE(c.skip_reason, 'none')   AS lane,
       c.mcap_at_call,
       tp.entry_price, tp.exit_price,
       tp.sol_in, tp.pnl_sol,
       tp.peak_multiplier, tp.pnl_pct
FROM trading_positions tp
JOIN calls  c  ON c.id = tp.call_id
LEFT JOIN channels ch ON ch.id = c.channel_id
WHERE tp.is_simulation = TRUE
  AND tp.is_strategy_b = %(is_b)s
  AND tp.status = 'closed'
  AND tp.entry_time >= now() - (%(days)s || ' days')::interval
  AND NOT (
      COALESCE(tp.peak_multiplier, 0) > %(max_peak)s
   OR COALESCE(tp.pnl_pct, 0)         > %(max_pnl)s
   OR COALESCE(tp.pnl_pct, 0)         < %(min_pnl)s
  )
"""


def paper_reprice(days: int, is_b: bool) -> None:
    rows = _rows(PAPER_SQL, {"days": days, "is_b": is_b,
                             "max_peak": MAX_SANE_PEAK,
                             "max_pnl": MAX_SANE_PNL_PCT, "min_pnl": MIN_SANE_PNL_PCT})
    strat = "B" if is_b else "A"
    print("=" * 78)
    print(f"  PAPER REPRICING — Strategy {strat}   (entry: feed price -> mcap_at_call)")
    print(f"  window: last {days}d   n = {len(rows)} closed paper trades (phantom-excluded)")
    print("=" * 78)
    if not rows:
        print("  no closed paper trades in window\n")
        return

    # lane -> [n, orig_sum, repriced_sum, n_repriceable]
    lanes: dict[str, list[float]] = {}
    tot = [0, 0.0, 0.0, 0]
    for r in rows:
        lane = f"{r['channel']}/{r['lane']}"
        acc = lanes.setdefault(lane, [0, 0.0, 0.0, 0])
        orig = float(r["pnl_sol"]) if r["pnl_sol"] is not None else 0.0
        rep  = _pnl(float(r["sol_in"] or 0), r["exit_price"], r["mcap_at_call"])
        acc[0] += 1; tot[0] += 1
        acc[1] += orig; tot[1] += orig
        if rep is not None:
            acc[2] += rep;  tot[2] += rep
            acc[3] += 1;    tot[3] += 1
        else:
            acc[2] += orig  # not repriceable (no call mcap) — keep booked so totals stay comparable

    print(f"  {'lane':<34} {'n':>4} {'booked':>9} {'repriced':>9} {'haircut':>9}")
    print("  " + "-" * 68)
    for lane, (n, orig, rep, nre) in sorted(lanes.items(), key=lambda kv: kv[1][1], reverse=True):
        cut = (rep - orig)
        flag = "  (no call-mcap on some)" if nre < n else ""
        print(f"  {lane[:34]:<34} {n:>4} {orig:>9.3f} {rep:>9.3f} {cut:>+9.3f}{flag}")
    print("  " + "-" * 68)
    cut = tot[2] - tot[1]
    cutpct = (cut / abs(tot[1]) * 100) if tot[1] else 0.0
    print(f"  {'TOTAL':<34} {tot[0]:>4} {tot[1]:>9.3f} {tot[2]:>9.3f} {cut:>+9.3f}  ({cutpct:+.0f}%)")
    print(f"\n  {tot[3]}/{tot[0]} trades had a usable mcap_at_call to reprice against.")
    print("  'repriced' books the honest entry; a negative haircut = the feed was inflating this lane.\n")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=int, default=14, help="paper reprice window (default 14)")
    ap.add_argument("--live-days", type=int, default=30, help="live validation window (default 30)")
    ap.add_argument("--strategy", choices=["a", "b"], default="a", help="paper strategy (default a)")
    args = ap.parse_args()

    live_validation(args.live_days)
    paper_reprice(args.days, is_b=(args.strategy == "b"))


if __name__ == "__main__":
    main()
