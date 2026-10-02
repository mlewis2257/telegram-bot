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

# Age exemption for an in-band blocked flag. 'safe' underperforms specifically when YOUNG:
#   <15m   safe 57 trades  -13.02%/SOL  bank  8.8%
#   >=15m  safe 21 trades   +3.97%/SOL  bank 19.0%
# 0 disables the exemption (block the flag at any age). NULL age never qualifies — if we
# cannot establish the coin is old, the measured-bad young case is the default.
SAFE_MIN_AGE_MIN = float(os.getenv("LIVE_ENTRY_SAFE_MIN_AGE_MIN", "15"))

# Flags tradeable OUTSIDE the mcap band. Empty = out-of-band blocked entirely (the band
# alone decides), which is what this did before.
#   out  warning  11 trades  +33.13%/SOL  bank 27.3%
#   out  unknown 122         -12.81%      bank 21.3%
#   out  safe     20         -15.59%      bank  5.0%
_raw_out = os.getenv("LIVE_ENTRY_OUT_BAND_ALLOW_FLAGS", "warning")
OUT_BAND_ALLOW_FLAGS = {s.strip().lower() for s in _raw_out.split(",") if s.strip()}

# SIZE OF THE EVIDENCE, stated because it is small and the rule is not:
# the band and the in-band 'safe' block rest on n=78-221 and replicated orderings. The
# two refinements above do NOT. out-band warning is ELEVEN trades carried by three banks,
# and the safe age exemption is twenty-one. A band x flag x age rule fitted to ~375
# positions with cells at 11 and 21 is the overfitting signature flagged on the
# mcap+dev_sold cell at n=70 -- each cell was selected because of the sign it happened to
# show, and two or three of six such cells flip on noise alone. Raised twice, and the
# operator's call; these are the knobs to revert first if forward results disagree.

# Date the bank_2x overlay went live. Positions before it took NO bank exit, so any
# window reaching back past it is comparing two different strategies. --backtest warns
# when the window starts earlier; see the note at that check for why the first version
# of the warning (zero-bank rows) missed the case that matters.
from datetime import date as _date  # noqa: E402
OVERLAY_START = _date.fromisoformat(
    os.getenv("QSIM_BANK_OVERLAY_START", "2026-09-23"))


def describe() -> str:
    if not ENABLED:
        return "[entry_filter] DISABLED"
    _blk = (f"blocked in-band: {','.join(sorted(BLOCK_SECURITY_FLAGS))}"
            + (f" unless age>={SAFE_MIN_AGE_MIN:g}m" if SAFE_MIN_AGE_MIN > 0 else "")
            ) if BLOCK_SECURITY_FLAGS else "all flags traded in-band"
    _out = (f"out-of-band allowed: {','.join(sorted(OUT_BAND_ALLOW_FLAGS))}"
            if OUT_BAND_ALLOW_FLAGS else "out-of-band blocked")
    return (f"[entry_filter] ENABLED — mcap {MCAP_MIN/1000:g}k-{MCAP_MAX/1000:g}k"
            f"{', dev_sold must be false' if REQUIRE_DEV_NOT_SOLD else ''}"
            f" | {_blk} | {_out}")


def _token_flags(mint: str) -> tuple[bool | None, str | None, float | None, bool]:
    """(dev_sold, security_flag, token_age_minutes, row_found) in ONE lookup.

    row_found distinguishes "no tokens row at all" from "row exists, column NULL".
    The old _dev_sold collapsed those into None; both still skip, but the reason
    printed is now accurate, which matters because 'no row' and 'dev_sold is NULL'
    want different follow-up.

    token_age_minutes shares security_flag's provenance guarantee — both are written
    `COALESCE(col, %s)` by upsert_token_realtime_metadata, so first detection wins and
    nothing overwrites them. Genuine pre-entry values, not hindsight.
    """
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT dev_sold, security_flag, token_age_minutes "
            "FROM tokens WHERE mint_address = %s LIMIT 1", (mint,))
        row = cur.fetchone()
    if row is None:
        return None, None, None, False
    age = float(row[2]) if row[2] is not None else None
    return row[0], row[1], age, True


def check(mint: str | None, msg_mcap: float | None) -> tuple[bool, str]:
    """(allowed, reason). Pass msg_mcap (= mcap_at_call), NOT the live price.

    Returns (True, "filter_off") when disabled so the caller needs no branch.
    """
    if not ENABLED:
        return True, "filter_off"
    try:
        if not msg_mcap or msg_mcap <= 0:
            return False, "no_mcap_at_call"
        in_band = MCAP_MIN <= msg_mcap <= MCAP_MAX
        mc = f"{msg_mcap/1000:.0f}k"

        # Out-of-band is no longer an unconditional reject: OUT_BAND_ALLOW_FLAGS names
        # the flags tradeable outside the band. Empty set restores the old behaviour.
        if not in_band and not OUT_BAND_ALLOW_FLAGS:
            return False, (f"mcap {mc} below {MCAP_MIN/1000:g}k" if msg_mcap < MCAP_MIN
                           else f"mcap {mc} above {MCAP_MAX/1000:g}k")

        need_flags = (REQUIRE_DEV_NOT_SOLD or BLOCK_SECURITY_FLAGS
                      or not in_band)
        if not need_flags:
            return True, "pass"

        if not mint or mint.startswith(("INFERRED:", "UNKNOWN:")):
            # dev_sold fails CLOSED (its own docstring argues why), and so does
            # out-of-band, which is only reachable by positively identifying an allowed
            # flag. The in-band security list does NOT: the rule is "block safe", and a
            # coin we cannot identify is not known to be safe — blocking it would smuggle
            # an unmeasured gate in under cover of a measured one, and coverage is the
            # binding constraint.
            if REQUIRE_DEV_NOT_SOLD:
                return False, "no_mint_for_dev_sold"
            if not in_band:
                return False, f"mcap {mc} out of band, no mint to check flag"
            return True, "pass_no_mint"
        ds, sec, age, found = _token_flags(mint)
        if not found:
            if REQUIRE_DEV_NOT_SOLD:
                return False, "no_tokens_row"
            if not in_band:
                return False, f"mcap {mc} out of band, no tokens row"
            return True, "pass_no_tokens_row"

        # 'null' is the token for a row that exists with no flag written.
        _sec = (sec or "").strip().lower() or "null"

        if not in_band:
            if _sec not in OUT_BAND_ALLOW_FLAGS:
                return False, f"mcap {mc} out of band, security_flag={_sec}"
        elif _sec in BLOCK_SECURITY_FLAGS:
            # Age exemption: the block is measured on YOUNG coins of this flag.
            if SAFE_MIN_AGE_MIN > 0 and age is not None and age >= SAFE_MIN_AGE_MIN:
                pass
            else:
                _why = "age unknown" if age is None else f"age {age:.0f}m"
                return False, f"security_flag={_sec} ({_why})"

        if REQUIRE_DEV_NOT_SOLD:
            if ds is None:
                return False, "dev_sold unknown"
            if ds:
                return False, "dev_sold=true"
        return True, "pass" if in_band else f"pass_out_band_{_sec}"
    except Exception as e:
        # Fails CLOSED. See the module docstring: missing metadata is the WORST
        # bucket in the data, so declining to trade what we cannot verify is the
        # measured-correct action, not just the careful one.
        return False, f"filter_error:{type(e).__name__}"


def _live_channels() -> list[str]:
    """Channel handles in LIVE_LANES — the only ones live can trade."""
    try:
        import lane_policy
        return sorted({k[0] for k in lane_policy.LIVE_LANES})
    except Exception:
        return []


def backtest(days: float, since: str | None = None,
             all_lanes: bool = False) -> None:
    """What the filter would have kept, on closed qsim positions.

    DEFAULTS TO LIVE'S OWN LANES. Without that this measured both lanes including
    solhousesignal, which LIVE_LANES does not hold, and reported it as what the
    gate would do for live — 443 positions at -3.36%/SOL where live's actual slice
    over the bank era was 143 at +2.70%. A verification tool describing a
    population the thing being verified cannot trade is worse than no tool.

    --since matters just as much. qsim ran NO bank overlay before 2026-09-23 (zero
    bank exits in every group), so a 21-day window is two-thirds a different
    strategy. Any comparison that straddles it is measuring the exit config.
    """
    from psycopg2.extras import RealDictCursor
    # Mirror the ACTUAL config rather than a hardcoded rule: this previously always
    # required dev_sold = false even with REQUIRE_DEV_NOT_SOLD off, so --backtest
    # described a gate live was not running.
    # Mirrors check()'s three branches. Parameterised, never interpolated — these values
    # come from the environment. SEC matches check()'s "blank or missing -> 'null'".
    SEC = "coalesce(nullif(lower(trim(t.security_flag)), ''), 'null')"
    BAND = "c.mcap_at_call BETWEEN %s AND %s"
    params: list = []

    # in-band leg
    in_leg = [BAND]
    params += [MCAP_MIN, MCAP_MAX]
    if BLOCK_SECURITY_FLAGS:
        if SAFE_MIN_AGE_MIN > 0:
            in_leg.append(f"({SEC} <> ALL(%s) OR t.token_age_minutes >= %s)")
            params += [sorted(BLOCK_SECURITY_FLAGS), SAFE_MIN_AGE_MIN]
        else:
            in_leg.append(f"{SEC} <> ALL(%s)")
            params.append(sorted(BLOCK_SECURITY_FLAGS))
    legs = ["(" + " AND ".join(in_leg) + ")"]

    # out-of-band leg
    if OUT_BAND_ALLOW_FLAGS:
        legs.append(f"(NOT ({BAND}) AND {SEC} = ANY(%s))")
        params += [MCAP_MIN, MCAP_MAX, sorted(OUT_BAND_ALLOW_FLAGS)]

    conds = ["(" + " OR ".join(legs) + ")"]
    if REQUIRE_DEV_NOT_SOLD:
        conds.append("t.dev_sold = false")
    pred = " AND ".join(conds)

    where = ["qp.status = 'closed'"]
    channels = [] if all_lanes else _live_channels()
    if channels:
        where.append("qp.channel_handle = ANY(%s)")
        params.append(channels)
    if since:
        where.append("qp.entry_time >= %s::date")
        params.append(since)
        window = f"since {since}"
    else:
        where.append("qp.entry_time >= now() - (%s || ' days')::interval")
        params.append(days)
        window = f"over {days:g}d"
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
                   round(100.0 * avg((qp.pnl_pct <= -90)::int)::numeric, 1) AS rug_pct,
                   count(*) FILTER (WHERE qp.exit_reason LIKE '%%bank%%')  AS banks
            FROM qsim_positions qp
            JOIN calls  c ON c.id = qp.call_id
            JOIN tokens t ON t.id = qp.token_id
            WHERE {' AND '.join(where)}
            GROUP BY 1 ORDER BY 1
        """, tuple(params))
        rows = [dict(r) for r in cur.fetchall()]
    _scope = ("ALL lanes" if all_lanes
              else (", ".join(channels) if channels else "ALL lanes (no LIVE_LANES)"))
    print(describe())
    print(f"\n  scope: {_scope}   {window}\n")
    print(f"  {'side':<9}{'n':>6}{'deployed':>10}{'pnl_sol':>10}"
          f"{'%/SOL':>9}{'win%':>7}{'rug%':>7}{'banks':>7}")
    for r in rows:
        print(f"  {r['side']:<9}{r['n']:>6}{float(r['deployed']):>10.2f}"
              f"{float(r['pnl_sol']):>10.4f}{float(r['pct_per_sol']):>9.2f}"
              f"{float(r['win_pct']):>7.1f}{float(r['rug_pct']):>7.1f}"
              f"{int(r['banks']):>7}")
    # Warn on the window's START DATE, not on a zero-bank row. The first version only
    # caught windows entirely before the overlay — but a STRADDLING window still shows
    # banks in aggregate and passed silently, which is the case that actually matters and
    # the exact confound that made my first security_flag conclusion wrong (68% of that
    # evidence came from pre-overlay rows while the totals looked populated).
    from datetime import date, timedelta
    _start = (date.fromisoformat(since) if since
              else date.today() - timedelta(days=days))
    if _start < OVERLAY_START:
        _pre = (OVERLAY_START - _start).days
        print()
        print(f"  WARNING: this window starts {_start} — {_pre} day(s) before the")
        print(f"  bank_2x overlay ({OVERLAY_START}). Those rows ran NO bank exit at all,")
        print("  so they describe a different strategy and will drag any comparison")
        print(f"  toward it. Re-run with --since {OVERLAY_START} for the live regime.")
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
    print()
    print("  Scope is LIVE_LANES by default — the slice live can actually trade.")
    print("  --all-lanes widens it (useful for research, NOT for verifying live).")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--backtest", type=float, metavar="DAYS", default=21.0,
                    nargs="?", const=21.0)
    ap.add_argument("--since", metavar="YYYY-MM-DD",
                    help="start date instead of --backtest DAYS. Use 2026-09-23 to "
                         "stay inside the bank_2x era; earlier windows ran no bank "
                         "overlay at all and measure a different strategy.")
    ap.add_argument("--all-lanes", action="store_true",
                    help="all channels, not just LIVE_LANES. Research only — it "
                         "reports a population live cannot trade.")
    a = ap.parse_args()
    try:
        if a.backtest or a.since:
            backtest(a.backtest, since=a.since, all_lanes=a.all_lanes)
        else:
            print(describe())
    finally:
        db.close_conn()
