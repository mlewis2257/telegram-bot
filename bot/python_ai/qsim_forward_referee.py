"""
qsim_forward_referee.py — one-table qmax-covered referee for live decisions.

This report exists to stop comparing incompatible slices:

    shadow_report             = all feed-shadow rows
    qsim_lane_scan            = qsim rows with quote observations
    qsim_shadow_coverage      = coverage audit, not a final decision table

The referee starts from qsim rows for a lane/day, using the same row loader and
quote-observation window as qsim_quote_capture_replay.py, and splits each day into:

    qsim_n        closed qsim rows
    qmax_n        same rows that have qsim quote observations
    coverage      qmax_n / qsim_n

PnL columns are normalized per 1 SOL deployed. A row/day is only "referee-grade"
when qmax coverage is high enough; otherwise it prints WAIT.

Example:
    python3 qsim_forward_referee.py --days 7 --channel solwhaletrending --lane low_score --variant early
    python3 qsim_forward_referee.py --days 7 --channel solwhaletrending --lane low_score --variant early --include-post-exit
    python3 qsim_forward_referee.py --days 30 --since '2026-08-28 00:00 UTC' --channel solwhaletrending --lane low_score --variant early
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))

import qsim_quote_capture_replay as replay


MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5


def _f(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _ret(pnl: Any, sol_in: Any) -> float:
    sol = _f(sol_in)
    if sol <= 0:
        return 0.0
    return _f(pnl) / sol


def _day(value: Any) -> str:
    if value is None:
        return "n/a"
    if hasattr(value, "strftime"):
        return value.strftime("%m/%d")
    return str(value)[:10]


def _verdict(qsim_n: int, qmax_n: int, coverage: float, current: float,
             best: float, min_coverage: float, min_qmax_n: int) -> str:
    if qsim_n <= 0:
        return "NO_QSIM"
    if qmax_n < min_qmax_n or coverage < min_coverage:
        return "WAIT"
    if current > 0:
        return "LIVE_CANDIDATE"
    if best >= 0:
        return "EXIT_WORK"
    return "NO_EDGE"


def main() -> None:
    parser = argparse.ArgumentParser(description="Daily qmax-covered referee table.")
    parser.add_argument("--include-stale", action="store_true",
                        help="include rows closed on a stale quote (exit_reason stale_*)")
    parser.add_argument("--min-obs", type=int, default=0,
                        help="minimum priced observations over the position's life (0 = off)")
    parser.add_argument("--max-gap-secs", type=float, default=0.0,
                        help="reject rows with a blind hole longer than this (0 = off)")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--since", default=None, help="only include qsim entries at/after this timestamp")
    parser.add_argument("--channel", default="solwhaletrending")
    parser.add_argument("--lane", default="low_score")
    parser.add_argument("--variant", default="early")
    parser.add_argument("--min-entry-ratio", type=float, default=None)
    parser.add_argument("--max-entry-ratio", type=float, default=None)
    parser.add_argument("--min-coverage", type=float, default=0.80)
    parser.add_argument("--min-qmax-n", type=int, default=25)
    parser.add_argument("--raw", action="store_true")
    parser.add_argument(
        "--max-qmax",
        type=float,
        default=replay.MAX_QOBS_MULT,
        help="ignore quote observations above this multiple as quote artifacts",
    )
    parser.add_argument(
        "--include-post-exit",
        action="store_true",
        help="include qsim post-exit quote probes in replay observations",
    )
    parser.add_argument(
        "--post-exit-mins",
        type=float,
        default=90.0,
        help="minutes of post-exit qsim quote probes to include with --include-post-exit",
    )
    args = parser.parse_args()
    replay.MAX_QOBS_MULT = args.max_qmax

    params = {
        "days": args.days,
        "since": args.since,
        "channel": args.channel,
        "lane": args.lane,
        "variant": args.variant,
        "min_entry_ratio": args.min_entry_ratio,
        "max_entry_ratio": args.max_entry_ratio,
        "raw": args.raw,
        "max_peak": MAX_SANE_PEAK,
        "max_pnl": MAX_SANE_PNL_PCT,
        "min_pnl": MIN_SANE_PNL_PCT,
        "max_qmax": args.max_qmax,
        "include_post_exit": args.include_post_exit,
        "post_exit_mins": args.post_exit_mins,
        "include_stale": args.include_stale,
        "min_obs": args.min_obs,
        "max_gap_secs": args.max_gap_secs,
    }
    rows = replay._rows(params)
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_day[_day(row.get("entry_time"))].append(row)

    print(
        f"\nQSIM FORWARD REFEREE — days={args.days} channel={args.channel} "
        f"lane={args.lane} variant={args.variant} since={args.since} max_qmax={args.max_qmax} "
        f"include_post_exit={args.include_post_exit} post_exit_mins={args.post_exit_mins:g}"
    )
    print(
        "PnL is normalized per 1 SOL deployed. Rows come from qsim_quote_capture_replay's "
        "same qsim universe/window."
    )
    print("`sh_n`/`shadow` are matched shadow rows inside that qsim universe, not full shadow_report.\n")
    hdr = (
        f"{'day':<8} {'sh_n':>5} {'qs_n':>5} {'qmax_n':>6} {'cov':>5} "
        f"{'shadow':>9} {'qsim':>9} {'qmax_q':>9} {'best_raw':>9} "
        f"{'bank13':>9} {'bank14':>9} {'r1.5':>5} {'d1.1':>5} verdict"
    )
    print(hdr)
    print("-" * len(hdr))

    totals = {
        "shadow_n": 0, "qsim_n": 0, "qmax_n": 0, "shadow": 0.0, "qsim": 0.0,
        "qmax_qsim": 0.0, "best_raw": 0.0, "bank13": 0.0, "bank14": 0.0,
        "runners": 0, "dead": 0,
    }

    for day in sorted(by_day):
        day_rows = by_day[day]
        qsim_rows = day_rows
        shadow_rows = [row for row in qsim_rows if row.get("shadow_pnl") is not None]
        qmax_rows = [row for row in qsim_rows if int(row.get("qobs_count") or 0) > 0]
        qmax_views = [replay._view(row) for row in qmax_rows]
        shadow = sum(_ret(row.get("shadow_pnl"), row.get("shadow_sol_in")) for row in shadow_rows)
        qsim = sum(_ret(row.get("qsim_pnl"), row.get("qsim_sol_in")) for row in qsim_rows)
        qmax_qsim = sum(view.returns["current"] for view in qmax_views)
        best_raw = sum(view.returns["best_raw"] for view in qmax_views)
        bank13 = sum(view.returns["bank_1p3x"] for view in qmax_views)
        bank14 = sum(view.returns["bank_1p4x"] for view in qmax_views)
        runners = sum(1 for view in qmax_views if (view.max_quote_mult or 0.0) >= 1.5)
        dead = sum(1 for view in qmax_views if (view.max_quote_mult or 0.0) < 1.1)
        coverage = (len(qmax_rows) / len(qsim_rows)) if qsim_rows else 0.0
        best = max(qmax_qsim, best_raw, bank13, bank14)
        verdict = _verdict(
            len(qsim_rows), len(qmax_rows), coverage, qmax_qsim, best,
            args.min_coverage, args.min_qmax_n,
        )

        print(
            f"{day:<8} {len(shadow_rows):>5} {len(qsim_rows):>5} {len(qmax_rows):>6} "
            f"{coverage:>4.0%} {shadow:>+9.2f} {qsim:>+9.2f} {qmax_qsim:>+9.2f} "
            f"{best_raw:>+9.2f} {bank13:>+9.2f} {bank14:>+9.2f} "
            f"{runners:>5} {dead:>5} {verdict}"
        )

        totals["shadow_n"] += len(shadow_rows)
        totals["qsim_n"] += len(qsim_rows)
        totals["qmax_n"] += len(qmax_rows)
        totals["shadow"] += shadow
        totals["qsim"] += qsim
        totals["qmax_qsim"] += qmax_qsim
        totals["best_raw"] += best_raw
        totals["bank13"] += bank13
        totals["bank14"] += bank14
        totals["runners"] += runners
        totals["dead"] += dead

    if rows:
        coverage = totals["qmax_n"] / totals["qsim_n"] if totals["qsim_n"] else 0.0
        best = max(totals["qmax_qsim"], totals["best_raw"], totals["bank13"], totals["bank14"])
        verdict = _verdict(
            totals["qsim_n"], totals["qmax_n"], coverage, totals["qmax_qsim"], best,
            args.min_coverage, args.min_qmax_n,
        )
        print("-" * len(hdr))
        print(
            f"{'TOTAL':<8} {totals['shadow_n']:>5} {totals['qsim_n']:>5} {totals['qmax_n']:>6} "
            f"{coverage:>4.0%} {totals['shadow']:>+9.2f} {totals['qsim']:>+9.2f} "
            f"{totals['qmax_qsim']:>+9.2f} {totals['best_raw']:>+9.2f} "
            f"{totals['bank13']:>+9.2f} {totals['bank14']:>+9.2f} "
            f"{totals['runners']:>5} {totals['dead']:>5} {verdict}"
        )
    else:
        print("No qsim rows matched the filter.")

    print("\nVerdicts: WAIT = insufficient qmax coverage; EXIT_WORK = qmax sample can be rescued by an exit replay; NO_EDGE = qmax sample stays negative.")


if __name__ == "__main__":
    main()
