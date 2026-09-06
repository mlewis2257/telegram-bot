"""
qsim_clean_window.py — the ONE report for the post-fix clean window.

WHY THIS EXISTS
---------------
Every other qsim report answers many questions at once. `qsim_quote_capture_replay.py` sweeps
~150 exit policies; picking the best one on a small sample finds noise essentially every time.
That is how a 28-trade window produced a "best policy" that was inside fee noise.

This report asks exactly two questions, decided BEFORE the data existed, and refuses to answer
either one until the sample can actually support it:

    1. Is the current stack profitable?         (absolute — needs a large n)
    2. Does ONE named alternative beat it?      (paired — needs a much smaller n)

Question 2 is paired: both policies are replayed over the SAME rows, so the per-row difference
cancels most of the volatility that makes question 1 so expensive. That is why an A/B can be
called in days while "is it profitable" can take weeks.

Every number is reported with the smallest effect the current sample could detect. If the
observed effect is inside that band the verdict is INCONCLUSIVE — not a weak signal, not a
hint, not something to act on. That distinction is the whole point of this file.

DATA QUALITY is reported alongside, because a clean window is a claim that has to be checked:
starved rows are excluded (see qsim._stale_reason / decision_gap_secs) and the report shows how
many were dropped and why. If that count is not ~0 after the 2026-09-05 scheduler fixes, the
plumbing has regressed and the numbers below mean nothing.

Example:
    python3 qsim_clean_window.py --since '2026-09-06 00:00:00 UTC'
    python3 qsim_clean_window.py --since '2026-09-06 00:00:00 UTC' --fee-pct 0.02
"""

from __future__ import annotations

import argparse
import math
import os
import sys
from collections import Counter
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

import qsim_quote_capture_replay as replay


Z95 = 1.959964            # normal approximation; fine at the n we care about
DEFAULT_ALT = "no_1p3x_stop_0p85x"


def _mean_sd(xs: list[float]) -> tuple[float, float]:
    n = len(xs)
    if n == 0:
        return 0.0, 0.0
    mean = sum(xs) / n
    if n < 2:
        return mean, 0.0
    var = sum((x - mean) ** 2 for x in xs) / (n - 1)
    return mean, math.sqrt(var)


def _ci_half(sd: float, n: int) -> float:
    """95% CI half-width for a mean. This is the smallest effect this n can resolve."""
    if n < 2:
        return float("inf")
    if sd <= 0:
        return 0.0          # zero variance = perfectly precise, NOT unknowable
    return Z95 * sd / math.sqrt(n)


def _n_needed(sd: float, effect: float) -> int:
    """Trades required to resolve an effect of this size at 95%."""
    if effect <= 0 or sd <= 0:
        return 0
    return int(math.ceil((Z95 * sd / effect) ** 2))


def _verdict(mean: float, half: float) -> tuple[str, str]:
    if half == float("inf"):
        return "NO ANSWER", "not enough trades to compute an interval"
    lo, hi = mean - half, mean + half
    if lo > 0:
        return "POSITIVE", f"95% CI [{lo:+.1%}, {hi:+.1%}] excludes zero"
    if hi < 0:
        return "NEGATIVE", f"95% CI [{lo:+.1%}, {hi:+.1%}] excludes zero"
    return "INCONCLUSIVE", f"95% CI [{lo:+.1%}, {hi:+.1%}] contains zero"


def _params(args, *, clean: bool) -> dict:
    return {
        "days": 3650,                     # --since is the real bound
        "since": args.since,
        "channel": args.channel,
        "lane": args.lane,
        "variant": args.variant,
        "min_entry_ratio": None,
        "max_entry_ratio": None,
        "raw": False,
        "max_peak": replay.MAX_SANE_PEAK,
        "max_pnl": replay.MAX_SANE_PNL_PCT,
        "min_pnl": replay.MIN_SANE_PNL_PCT,
        "max_qmax": args.max_qmax,
        "include_post_exit": False,
        "post_exit_mins": 90,
        "include_stale": not clean,
        "min_obs": args.min_obs if clean else 0,
        "max_gap_secs": args.max_gap_secs if clean else 0.0,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=True, help="clean-window start, e.g. '2026-09-06 00:00:00 UTC'")
    ap.add_argument("--channel", default="solwhaletrending")
    ap.add_argument("--lane", default="low_score")
    ap.add_argument("--variant", default="early")
    ap.add_argument("--alt", default=DEFAULT_ALT,
                    help=f"the ONE alternative policy to test (default: {DEFAULT_ALT})")
    ap.add_argument("--fee-pct", type=float, default=0.0,
                    help="round-trip cost per trade as a fraction, subtracted from every "
                         "return. qsim quotes already include slippage but NOT network + "
                         "priority fees; ~0.02 is a reasonable guess at 0.05 SOL sizing. "
                         "Left at 0 it is NOT modelled and the report says so.")
    ap.add_argument("--min-obs", type=int, default=5)
    ap.add_argument("--max-gap-secs", type=float, default=180.0)
    ap.add_argument("--max-qmax", type=float, default=50.0)
    args = ap.parse_args()

    clean_rows = replay._rows(_params(args, clean=True))
    all_rows = replay._rows(_params(args, clean=False))

    # ── data quality: prove the window is actually clean ──────────────────────
    clean_ids = {r.get("call_id") for r in clean_rows}
    dropped = [r for r in all_rows if r.get("call_id") not in clean_ids]
    drop_reason: Counter = Counter()
    for r in dropped:
        reason = str(r.get("qsim_reason") or "")
        if reason.startswith("stale_"):
            drop_reason["starved close (stale_*)"] += 1
        elif int(r.get("obs_count") or 0) < args.min_obs:
            drop_reason[f"under {args.min_obs} quotes"] += 1
        else:
            drop_reason[f"blind hole > {args.max_gap_secs:g}s"] += 1

    print("=" * 72)
    print("QSIM CLEAN WINDOW")
    print("=" * 72)
    print(f"window start : {args.since}")
    print(f"lane         : {args.channel} / {args.lane} / {args.variant}")
    print(f"filters      : min_obs={args.min_obs} max_gap_secs={args.max_gap_secs:g} "
          f"stale excluded, fallback=current")
    print(f"fees         : " + (f"{args.fee_pct:.2%} per trade, subtracted"
                                if args.fee_pct > 0 else "NOT MODELLED (pass --fee-pct)"))

    print("\nDATA QUALITY")
    print(f"  rows in window     : {len(all_rows)}")
    print(f"  usable (clean)     : {len(clean_rows)}")
    print(f"  dropped            : {len(dropped)}")
    for reason, n in drop_reason.most_common():
        print(f"      {reason:<28} {n}")
    if all_rows and len(dropped) / len(all_rows) > 0.15:
        print("  ** WARNING: >15% dropped. The scheduler fixes are not holding —")
        print("  ** treat every number below as suspect and check the qsim logs.")

    if not clean_rows:
        print("\nNo usable rows yet. Let it run.")
        return

    views = [replay._view(r, fallback_mode="current") for r in clean_rows]

    available = set()
    for v in views:
        available.update(v.returns.keys())
    if args.alt not in available:
        print(f"\nunknown --alt {args.alt!r}. Some available policies:")
        for name in sorted(x for x in available if x != "current")[:40]:
            print(f"  {name}")
        sys.exit(2)

    fee = args.fee_pct
    cur = [v.returns["current"] - fee for v in views]
    alt = [v.returns[args.alt] - fee for v in views]
    n = len(cur)

    # trades/day, so the wait can be projected rather than guessed
    try:
        start = datetime.fromisoformat(args.since.replace(" UTC", "+00:00"))
        days = max((datetime.now(timezone.utc) - start).total_seconds() / 86400.0, 1e-9)
        per_day = n / days
    except Exception:
        days, per_day = 0.0, 0.0

    print("\nSAMPLE")
    print(f"  closed trades      : {n}")
    if per_day:
        print(f"  elapsed            : {days:.2f} days  ({per_day:.1f} trades/day)")

    # ── Q1: absolute profitability (unpaired — expensive) ────────────────────
    m_cur, sd_cur = _mean_sd(cur)
    half_cur = _ci_half(sd_cur, n)
    verdict, why = _verdict(m_cur, half_cur)
    print("\n" + "-" * 72)
    print("Q1 — IS THE CURRENT STACK PROFITABLE?")
    print("-" * 72)
    print(f"  mean return/trade  : {m_cur:+.2%}")
    print(f"  volatility (sd)    : {sd_cur:.2%}")
    print(f"  smallest detectable: +/-{half_cur:.2%} at n={n}")
    print(f"  VERDICT            : {verdict}  ({why})")
    if verdict == "INCONCLUSIVE" and abs(m_cur) > 1e-9:
        need = _n_needed(sd_cur, abs(m_cur))
        more = max(0, need - n)
        eta = (more / per_day) if per_day else 0
        print(f"  to resolve an effect this size ({m_cur:+.2%}) you would need ~{need} trades "
              f"({more} more" + (f", ~{eta:.1f} days)" if eta else ")"))

    # ── Q2: the one A/B (paired — cheap) ─────────────────────────────────────
    diff = [a - c for a, c in zip(alt, cur)]
    m_d, sd_d = _mean_sd(diff)
    half_d = _ci_half(sd_d, n)
    verdict_d, why_d = _verdict(m_d, half_d)
    changed = sum(1 for d in diff if abs(d) > 1e-12)
    print("\n" + "-" * 72)
    print(f"Q2 — DOES {args.alt} BEAT CURRENT?   (paired, same rows)")
    print("-" * 72)
    m_alt, _ = _mean_sd(alt)
    print(f"  current  mean/trade: {m_cur:+.2%}")
    print(f"  {args.alt:<9} mean/trade: {m_alt:+.2%}")
    print(f"  paired difference  : {m_d:+.2%}")
    print(f"  rows it changed    : {changed}/{n}")
    print(f"  smallest detectable: +/-{half_d:.2%} at n={n}")
    print(f"  VERDICT            : {verdict_d}  ({why_d})")
    if verdict_d == "INCONCLUSIVE" and abs(m_d) > 1e-9:
        need = _n_needed(sd_d, abs(m_d))
        more = max(0, need - n)
        eta = (more / per_day) if per_day else 0
        print(f"  to resolve a difference this size you would need ~{need} trades "
              f"({more} more" + (f", ~{eta:.1f} days)" if eta else ")"))

    print("\n" + "-" * 72)
    print("HOW TO READ THIS")
    print("-" * 72)
    print("  INCONCLUSIVE means the sample cannot tell this result from zero. It is not a")
    print("  weak positive and not a hint — there is nothing to act on. Do not compare it")
    print("  against other policies to find something better-looking; that is the sweep")
    print("  that manufactured the last false winner. Two questions, one alternative.")
    print()


if __name__ == "__main__":
    main()
