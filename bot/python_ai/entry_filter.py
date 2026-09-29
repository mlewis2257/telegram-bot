"""
entry_filter.py — the mcap band + dev_sold gate, for LIVE money only.

WHAT IT GATES ON, AND WHY THOSE TWO
-----------------------------------
Measured 2026-09-28 over 21d of closed qsim positions:

  mcap_at_call 80-120k   -5.31%/SOL vs -11.50% book, on 40% of volume.
      Replicated in BOTH halves of the window (three metrics, magnitudes within
      a point) and confirmed OUT OF SAMPLE on ~3,300 older trades at 5.3 sigma
      on rug rate (13.0% in-band vs 19.8% out, within dev_sold=true).
      Mechanism: median liq/mcap FALLS with mcap (0.243 -> 0.138) while rug rate
      RISES (5.4% -> 14.9%). Bigger coins are backed by proportionally less
      liquidity and collapse more often.

  dev_sold = false       2.8% rug rate vs 8.8% for true, 3.7 sigma.
      Moves RUGS ONLY — win rate 36.2% vs 33.9%, i.e. unchanged. It is a
      downside filter, not an upside one. Genuine pre-entry data: if it were
      written after the fact, `true` would show a rug rate near 100%, not 8.8%.

Together, n=70: +8.05%/SOL, win 40.0%, rug 2.9%, 2x 17.1%. THAT CELL IS NOT
PROVEN — each ingredient is negative alone, and a positive interaction between
two negative marginals at n=70 is the textbook overfitting signature. The MAIN
EFFECTS are what replicated. Treat the combination as the reason to run a small
live test, not as an established edge.

IT GATES ON mcap_at_call, NOT THE LIVE PRICE
--------------------------------------------
live_trader has two mcaps in scope: `msg_mcap` (= calls.mcap_at_call, the feed's
number at call time) and `actual_entry` (a fresh DexScreener quote). The entire
analysis above used mcap_at_call, and the two diverge badly on fast risers — the
feed under-records entries by ~2.3x there. Gating on `actual_entry` would select
a different population than the one that was measured. Pass msg_mcap.

IT FAILS CLOSED, unlike dev_gate
--------------------------------
dev_gate fails open because a broken gate must not silently reject everything.
This one does the opposite and skips the trade when it cannot verify, for a
reason that is in the data rather than in caution: positions with missing token
metadata ran -17.60%/SOL at a 24.9% win rate against -8.70% and 34.2% for those
with it. "Unknown" is not neutral here, it is the worst bucket. Skipping on
missing data is correct on the merits.

    LIVE_ENTRY_FILTER_ENABLED=true     # off by default
    LIVE_ENTRY_MCAP_MIN=80000
    LIVE_ENTRY_MCAP_MAX=120000
    LIVE_ENTRY_REQUIRE_DEV_NOT_SOLD=true

    python3 entry_filter.py --backtest 21    # what it would have kept
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import db  # noqa: E402

ENABLED = os.getenv("LIVE_ENTRY_FILTER_ENABLED", "false").strip().lower() == "true"
MCAP_MIN = float(os.getenv("LIVE_ENTRY_MCAP_MIN", "80000"))
MCAP_MAX = float(os.getenv("LIVE_ENTRY_MCAP_MAX", "120000"))
REQUIRE_DEV_NOT_SOLD = (
    os.getenv("LIVE_ENTRY_REQUIRE_DEV_NOT_SOLD", "true").strip().lower() == "true"
)


def describe() -> str:
    if not ENABLED:
        return "[entry_filter] DISABLED"
    return (f"[entry_filter] ENABLED — mcap_at_call {MCAP_MIN/1000:g}k-{MCAP_MAX/1000:g}k"
            f"{', dev_sold must be false' if REQUIRE_DEV_NOT_SOLD else ''}"
            f" (fails CLOSED on missing data)")


def _dev_sold(mint: str) -> bool | None:
    """tokens.dev_sold for this mint. None means unknown OR the lookup failed —
    the caller treats both the same way, which is to skip."""
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT dev_sold FROM tokens WHERE mint_address = %s LIMIT 1", (mint,))
        row = cur.fetchone()
    return None if row is None else row[0]


def check(mint: str | None, msg_mcap: float | None) -> tuple[bool, str]:
    """(allowed, reason). Pass msg_mcap (= mcap_at_call), NOT the live price.

    Returns (True, "filter_off") when disabled so the caller needs no branch.
    """
    if not ENABLED:
        return True, "filter_off"
    try:
        if not msg_mcap or msg_mcap <= 0:
            return False, "no_mcap_at_call"
        if msg_mcap < MCAP_MIN:
            return False, f"mcap {msg_mcap/1000:.0f}k below {MCAP_MIN/1000:g}k"
        if msg_mcap > MCAP_MAX:
            return False, f"mcap {msg_mcap/1000:.0f}k above {MCAP_MAX/1000:g}k"
        if REQUIRE_DEV_NOT_SOLD:
            if not mint or mint.startswith(("INFERRED:", "UNKNOWN:")):
                return False, "no_mint_for_dev_sold"
            ds = _dev_sold(mint)
            if ds is None:
                return False, "dev_sold unknown"
            if ds:
                return False, "dev_sold=true"
        return True, "pass"
    except Exception as e:
        # Fails CLOSED. See the module docstring: missing metadata is the WORST
        # bucket in the data, so declining to trade what we cannot verify is the
        # measured-correct action, not just the careful one.
        return False, f"filter_error:{type(e).__name__}"


def backtest(days: float) -> None:
    """What the filter would have kept, on closed qsim positions."""
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT CASE WHEN c.mcap_at_call BETWEEN %s AND %s
                         AND t.dev_sold = false THEN 'KEPT' ELSE 'skipped' END AS side,
                   count(*)                                              AS n,
                   round(sum(qp.sol_in)::numeric, 2)                     AS deployed,
                   round(sum(qp.sol_out - qp.sol_in)::numeric, 4)        AS pnl_sol,
                   round((100.0 * sum(qp.sol_out - qp.sol_in)
                          / NULLIF(sum(qp.sol_in), 0))::numeric, 2)      AS pct_per_sol,
                   round(100.0 * avg((qp.pnl_pct > 0)::int)::numeric, 1) AS win_pct,
                   round(100.0 * avg((qp.pnl_pct <= -90)::int)::numeric, 1) AS rug_pct
            FROM qsim_positions qp
            JOIN calls  c ON c.id = qp.call_id
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.status = 'closed'
              AND qp.entry_time >= now() - (%s || ' days')::interval
            GROUP BY 1 ORDER BY 1
        """, (MCAP_MIN, MCAP_MAX, days))
        rows = [dict(r) for r in cur.fetchall()]
    print(describe())
    print(f"\n  what it would have done over {days:g}d:\n")
    print(f"  {'side':<9}{'n':>6}{'deployed':>10}{'pnl_sol':>10}"
          f"{'%/SOL':>9}{'win%':>7}{'rug%':>7}")
    for r in rows:
        print(f"  {r['side']:<9}{r['n']:>6}{float(r['deployed']):>10.2f}"
              f"{float(r['pnl_sol']):>10.4f}{float(r['pct_per_sol']):>9.2f}"
              f"{float(r['win_pct']):>7.1f}{float(r['rug_pct']):>7.1f}")
    print()
    print("  KEPT is ~4% of volume. Do not read its pnl as an edge — the cell is")
    print("  n=70 over 21d and its two ingredients are each NEGATIVE alone. The")
    print("  replicated part is the mcap band; the combination is why a small")
    print("  live test is worth running, not a reason to size up.")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--backtest", type=float, metavar="DAYS")
    a = ap.parse_args()
    try:
        if a.backtest:
            backtest(a.backtest)
        else:
            print(describe())
    finally:
        db.close_conn()
