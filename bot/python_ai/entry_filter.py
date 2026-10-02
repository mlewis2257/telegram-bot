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
# Block security_flag = 'unknown'. Measured 2026-10-02 on live's exact lane and band
# (solwhaletrending, mcap 80-120k, 21d of closed qsim positions, which have NO security
# gate so the counterfactual exists):
#
#   unknown   245   -0.9797 SOL   -8.00%/SOL   rug 10.6%   bank 6.12%
#   safe      212   +0.0655 SOL   +0.62%/SOL   rug  0.5%   bank 3.77%
#   warning   160   +0.3619 SOL   +4.52%/SOL   rug  2.5%   bank 6.88%
#
# unknown rugs 10.6% against 1.34% for flagged coins, z = 4.51. live_trader blocks
# 'warning' and lets 'unknown' straight through, because the gate reads
# `security_flag == "warning"` and 'unknown' is a non-matching string. So live has been
# rejecting its best segment and trading the one losing 8%/SOL.
#
# It is a genuine PRE-ENTRY value: db.upsert_token_realtime_metadata writes
# `security_flag = COALESCE(security_flag, %s)`, so the first detection wins and nothing
# can overwrite it later. The reverse-causation worry (rugged -> unreadable -> 'unknown')
# cannot apply, because the flag predates the outcome.
#
# 'warning' is deliberately left to live_trader's existing gate. Its apparent edge over
# 'safe' is only z = 1.30 on bank rate, so unblocking it is not established; this change
# rests on the 4.51 sigma rug finding alone.
# Comma-separated security_flag values to BLOCK. One list, one place: previously
# live_trader blocked 'warning' and entry_filter blocked 'unknown', which is exactly how
# the worst group ended up traded and the best group blocked. entry_filter is now the
# single authority. Use the literal token "null" to block rows with no flag; empty string
# blocks nothing.
#
# DEFAULT 'safe', which looks backwards and is not. Measured on live's lane and band
# (solwhaletrending, mcap 80-120k) over the bank_2x era only, since the pre-09-23 window
# has ZERO banks in every group and contaminated the first version of this analysis:
#
#   IN   warning  67   +4.80%/SOL   bank 16.4%   win 31.3%
#   IN   unknown  76   +0.85%       bank 22.4%   win 36.8%
#   IN   safe     78   -8.44%       bank 11.5%   win 26.9%   rug 0.0%
#
# `safe` has the LOWEST bank rate and the WORST pnl while never rugging once. This book is
# carried entirely by banks, so rug avoidance is not what it needs — the security scanner
# is doing its job perfectly and its job is the wrong one.
#
# It is not an age proxy, which was the obvious confound and was checked: within the <15m
# bucket that holds 80% of in-band flow, average ages are 3.5 / 4.9 / 3.4 minutes across
# safe / unknown / warning, and the ordering STILL separates (-13.02 / -1.02 / +3.93).
# Within that controlled stratum safe banks 8.8% against 18.2% for the other two, z = 1.83.
#
# The ordering has replicated across six slices: pooled, IN band, out band, 7d, 10d, and
# a single day where all three were positive and safe was still last.
#
# NOT blocked, deliberately: 'safe' positions older than 15m run +3.97%/SOL at a 19.0%
# bank rate, so the real effect may be safe-AND-young. That cell is n=21, and a two-
# variable conditional rule fitted to 221 trades is the same overfitting signature as the
# mcap+dev_sold cell at n=70. One variable, the one that replicated.
_raw_block = os.getenv("LIVE_ENTRY_BLOCK_SECURITY_FLAGS", "safe")
BLOCK_SECURITY_FLAGS = {s.strip().lower() for s in _raw_block.split(",") if s.strip()}


def describe() -> str:
    if not ENABLED:
        return "[entry_filter] DISABLED"
    return (f"[entry_filter] ENABLED — mcap_at_call {MCAP_MIN/1000:g}k-{MCAP_MAX/1000:g}k"
            f"{', dev_sold must be false' if REQUIRE_DEV_NOT_SOLD else ''}"
            f"{', security_flag blocked: ' + ','.join(sorted(BLOCK_SECURITY_FLAGS)) if BLOCK_SECURITY_FLAGS else ', all security_flags traded'}")


def _token_flags(mint: str) -> tuple[bool | None, str | None, bool]:
    """(dev_sold, security_flag, row_found) for this mint, in ONE lookup.

    row_found distinguishes "no tokens row at all" from "row exists, column NULL".
    The old _dev_sold collapsed those into None; both still skip, but the reason
    printed is now accurate, which matters because 'no row' and 'dev_sold is NULL'
    want different follow-up.
    """
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT dev_sold, security_flag FROM tokens WHERE mint_address = %s LIMIT 1",
            (mint,))
        row = cur.fetchone()
    if row is None:
        return None, None, False
    return row[0], row[1], True


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
        if REQUIRE_DEV_NOT_SOLD or BLOCK_SECURITY_FLAGS:
            if not mint or mint.startswith(("INFERRED:", "UNKNOWN:")):
                # dev_sold fails CLOSED here (its own docstring argues why). The security
                # list does NOT: the rule is "block safe", and a coin we cannot identify
                # is not known to be safe. Blocking it would smuggle an unmeasured gate in
                # under cover of a measured one, and coverage is the binding constraint.
                if REQUIRE_DEV_NOT_SOLD:
                    return False, "no_mint_for_dev_sold"
                return True, "pass_no_mint"
            ds, sec, found = _token_flags(mint)
            if not found:
                if REQUIRE_DEV_NOT_SOLD:
                    return False, "no_tokens_row"
                return True, "pass_no_tokens_row"
            if BLOCK_SECURITY_FLAGS:
                # 'null' is the token for a row that exists with no flag written.
                _sec = (sec or "").strip().lower() or "null"
                if _sec in BLOCK_SECURITY_FLAGS:
                    return False, f"security_flag={_sec}"
            if REQUIRE_DEV_NOT_SOLD:
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
    # Mirror the ACTUAL config rather than a hardcoded rule: this previously always
    # required dev_sold = false even with REQUIRE_DEV_NOT_SOLD off, so --backtest
    # described a gate live was not running.
    conds = ["c.mcap_at_call BETWEEN %s AND %s"]
    params: list = [MCAP_MIN, MCAP_MAX]
    if REQUIRE_DEV_NOT_SOLD:
        conds.append("t.dev_sold = false")
    if BLOCK_SECURITY_FLAGS:
        # Parameterised, not interpolated: these values come from the environment.
        # NULLIF(...,'') then COALESCE mirrors check()'s "blank or missing -> 'null'".
        conds.append("coalesce(nullif(lower(trim(t.security_flag)), ''), 'null') "
                     "<> ALL(%s)")
        params.append(sorted(BLOCK_SECURITY_FLAGS))
    pred = " AND ".join(conds)
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT CASE WHEN {pred} THEN 'KEPT' ELSE 'skipped' END AS side,
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
        """, tuple(params + [days]))
        rows = [dict(r) for r in cur.fetchall()]
    print(describe())
    print(f"\n  what it would have done over {days:g}d:\n")
    print(f"  {'side':<9}{'n':>6}{'deployed':>10}{'pnl_sol':>10}"
          f"{'%/SOL':>9}{'win%':>7}{'rug%':>7}")
    for r in rows:
        print(f"  {r['side']:<9}{r['n']:>6}{float(r['deployed']):>10.2f}"
              f"{float(r['pnl_sol']):>10.4f}{float(r['pct_per_sol']):>9.2f}"
              f"{float(r['win_pct']):>7.1f}{float(r['rug_pct']):>7.1f}")
    # Describe the gate that actually ran. The old footer was hardcoded for a
    # mcap+dev_sold config and read "~4% of volume, n=70" under numbers that had
    # since become 387 and 23% — a stale caveat under correct figures is worse
    # than none, because it looks like it was checked.
    kept = next((r for r in rows if r["side"] == "KEPT"), None)
    tot_n = sum(int(r["n"]) for r in rows) or 1
    print()
    if kept:
        print(f"  KEPT is {100.0*int(kept['n'])/tot_n:.0f}% of positions "
              f"({int(kept['n'])} of {tot_n}) under the gate ACTUALLY configured:")
    print(f"    mcap_at_call {MCAP_MIN/1000:g}k-{MCAP_MAX/1000:g}k"
          f"{'  +  dev_sold = false' if REQUIRE_DEV_NOT_SOLD else ''}"
          + (f"  +  security_flag NOT IN ({','.join(sorted(BLOCK_SECURITY_FLAGS))})"
             if BLOCK_SECURITY_FLAGS else ""))
    print()
    print("  Evidence behind each, which is NOT the same strength:")
    print("    mcap band        replicated in both halves on three metrics, and")
    print("                     out of sample at 5.3 sigma on rug rate (n~3300).")
    if BLOCK_SECURITY_FLAGS:
        print("    security list    'safe' has the LOWEST bank rate and WORST pnl while")
        print("                     never rugging; ordering replicated across six slices")
        print("                     and survives the age control (z = 1.83 within <15m).")
    if REQUIRE_DEV_NOT_SOLD:
        print("    dev_sold=false   3.7 sigma on rug rate, but NEGATIVE alone on pnl;")
        print("                     the mcap+dev_sold cell was n=70 and unproven.")
    print()
    print("  This spans ALL lanes. LIVE_LANES holds solwhaletrending only, so the")
    print("  slice live actually trades is smaller than KEPT shown here.")


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
