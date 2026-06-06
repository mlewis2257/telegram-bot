"""
verify_phantom_fix.py — Detect phantom peak/exit prices on live trades.

Cross-checks each live trade's recorded peak_mcap / exit_price against two
independent ground-truth witnesses:

  1. observed_peak / observed_at_exit — the max (and last) mcap actually logged
     to ws_market_observations during the hold window.
  2. wallet_implied_mcap — entry_mcap * (sol_out / sol_in), i.e. what the real
     on-chain fill implies you sold at.

If the phantom-peak bug is present, db_peak / db_exit float ~2x above BOTH
witnesses (peak_inflation >> 1.0). After the get_mcap_blended fix, peak_inflation
should collapse to ~1.0 and the two witnesses should agree.

See memory: phantom_peak_root_cause.

Usage:
    python3 verify_phantom_fix.py             # last 24h
    python3 verify_phantom_fix.py --hours 12
    python3 verify_phantom_fix.py --days 7
"""

from __future__ import annotations

import argparse
import os
import sys

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db

# A trade is flagged phantom when its recorded peak exceeds the highest mcap
# actually observed during the hold by more than this factor.
PHANTOM_THRESHOLD = 1.20


QUERY = """
SELECT
  tp.id,
  tok.symbol,
  tp.entry_price                                              AS entry_mcap,
  tp.peak_mcap                                                AS db_peak,
  obs.max_obs_mcap                                            AS observed_peak,
  tp.peak_mcap / NULLIF(obs.max_obs_mcap, 0)                  AS peak_inflation,
  tp.exit_price                                               AS db_exit,
  obs.exit_mcap                                               AS observed_at_exit,
  tp.entry_price * (tp.sol_out / NULLIF(tp.sol_in, 0))        AS wallet_implied_mcap,
  obs.obs_count,
  tp.pnl_pct,
  tp.exit_reason
FROM trading_positions tp
JOIN tokens tok ON tok.id = tp.token_id
LEFT JOIN LATERAL (
  SELECT
    max(o.mcap) AS max_obs_mcap,
    count(*)    AS obs_count,
    (SELECT o2.mcap FROM ws_market_observations o2
       WHERE o2.mint_address = tok.mint_address
         AND o2.observed_at <= tp.exit_time
       ORDER BY o2.observed_at DESC LIMIT 1) AS exit_mcap
  FROM ws_market_observations o
  WHERE o.mint_address = tok.mint_address
    AND o.observed_at BETWEEN tp.entry_time AND tp.exit_time
) obs ON true
WHERE tp.is_simulation = FALSE
  AND tp.status = 'closed'
  AND tp.entry_time >= now() - (%s || ' hours')::interval
ORDER BY (tp.peak_mcap / NULLIF(obs.max_obs_mcap, 0)) DESC NULLS LAST
"""

SOURCE_QUERY = """
SELECT COALESCE(source, 'null') AS source, count(*) AS n
FROM ws_market_observations
WHERE observed_at >= now() - (%s || ' hours')::interval
GROUP BY 1
ORDER BY n DESC
"""


def _fmt(v, nd=0):
    if v is None:
        return "—"
    return f"{float(v):,.{nd}f}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=int, default=None, help="look-back window in hours")
    ap.add_argument("--days", type=int, default=None, help="look-back window in days")
    args = ap.parse_args()

    if args.days is not None:
        hours = args.days * 24
        window_label = f"last {args.days}d"
    elif args.hours is not None:
        hours = args.hours
        window_label = f"last {args.hours}h"
    else:
        hours = 24
        window_label = "last 24h"

    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(QUERY, (hours,))
        rows = cur.fetchall()
        cur.execute(SOURCE_QUERY, (hours,))
        sources = cur.fetchall()

    print(f"\nPhantom-peak verification — {window_label}\n")

    if not rows:
        print("  No closed live trades in window.\n")
        return

    header = (
        f"{'sym':<10} {'entry':>8} {'db_peak':>9} {'obs_peak':>9} "
        f"{'infl':>5} {'db_exit':>9} {'obs_exit':>9} {'wallet':>9} "
        f"{'obs#':>4} {'pnl%':>7}  reason"
    )
    print(header)
    print("-" * len(header))

    phantom_count = 0
    inflations = []
    no_obs = 0
    for r in rows:
        infl = r["peak_inflation"]
        if r["observed_peak"] is None or not r["obs_count"]:
            no_obs += 1
        if infl is not None:
            inflations.append(float(infl))
            if float(infl) >= PHANTOM_THRESHOLD:
                phantom_count += 1
        flag = " ⚠" if (infl is not None and float(infl) >= PHANTOM_THRESHOLD) else ""
        print(
            f"{(r['symbol'] or '?')[:10]:<10} "
            f"{_fmt(r['entry_mcap']):>8} "
            f"{_fmt(r['db_peak']):>9} "
            f"{_fmt(r['observed_peak']):>9} "
            f"{(_fmt(infl, 2) if infl is not None else '—'):>5} "
            f"{_fmt(r['db_exit']):>9} "
            f"{_fmt(r['observed_at_exit']):>9} "
            f"{_fmt(r['wallet_implied_mcap']):>9} "
            f"{(r['obs_count'] or 0):>4} "
            f"{_fmt(r['pnl_pct'], 1):>7}  "
            f"{r['exit_reason'] or ''}{flag}"
        )

    n = len(rows)
    med = sorted(inflations)[len(inflations) // 2] if inflations else None
    avg = (sum(inflations) / len(inflations)) if inflations else None

    print("\nSummary")
    print(f"  trades:                 {n}")
    print(f"  phantom (infl ≥ {PHANTOM_THRESHOLD:.2f}):  {phantom_count}  ({phantom_count / n * 100:.0f}%)")
    print(f"  median peak_inflation:  {_fmt(med, 2)}")
    print(f"  avg peak_inflation:     {_fmt(avg, 2)}")
    if no_obs:
        print(f"  trades w/ no observations in window: {no_obs}  (can't verify — check feed coverage)")

    print("\n  ws_market_observations sources in window:")
    for s in sources:
        print(f"    {s['source']:<16} {s['n']:>8,}")
    if not any(s["source"] == "jupiter_batch" for s in sources):
        print("    (no 'jupiter_batch' rows yet — monitor.py logging fix not deployed/active)")

    print()
    if med is not None and med < PHANTOM_THRESHOLD:
        print("  ✅ median inflation below threshold — peaks track observed reality.")
    else:
        print("  ⚠️  peaks still inflated above observed mcap — phantom not resolved.")
    print()


if __name__ == "__main__":
    main()
