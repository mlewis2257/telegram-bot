"""
compare_shadow.py — paper trades vs their OWN idealized shadow, matched per-lane.

The honest "is paper leaking to the feed, or is shadow just optimistic?" check.
For every CLOSED paper position it looks up the shadow row for the exit variant
that lane ACTUALLY runs (via lane_policy.lane_exit, honoring per-day overrides like
vip_mcap_gate Fri->ride and solwhale/none Sat->ride_vol) — so the PnL comparison is
apples-to-apples for EVERY lane, not just the `early` ones a hand-written join catches.

    python3 compare_shadow.py                 # Strategy A, all time
    python3 compare_shadow.py --today         # just today (UTC calendar day)
    python3 compare_shadow.py --days 14        # last 14 days
    python3 compare_shadow.py --strategy b     # Strategy B (anchors + pockets)
    python3 compare_shadow.py --today --detail # per-trade breakdown, worst gap first

Two numbers per lane:
  EDGE = paper_sol - shadow_sol. NEGATIVE = paper made less than its idealized shadow
         (left on the table). POSITIVE = paper BEAT shadow (dense polling caught a higher
         peak / rode further than shadow's coarser sampling).
  CAP  = avg(paper_peak / shadow_peak). ~1.0 = feed is seeing the same highs shadow sees;
         well under 1.0 across many trades = real feed under-capture.

IMPORTANT — shadow is an OPTIMISTIC upper bound, not "money owed." It polls coarser
(15s vs paper's 8s), so it under-samples drawdowns and dodges dips that legitimately
fire paper's stops (see the profit_floor CASHSEY case). A small negative EDGE with
CAP~1.0 is NOT a leak — it's shadow's survivorship. A large negative EDGE with CAP<<1
IS a capturable feed problem. Read them together.

Sizing: compares raw pnl_sol. Under LANE_UNIFORM_SIZE=0.5 paper and shadow are both
0.5 so this is directly comparable; for windows spanning the pre-uniform (0.1) era pass
--normalize to compare PnL per 1 SOL deployed instead.
"""

from __future__ import annotations

import argparse
import os
import sys

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db
import lane_policy

# Phantom guard — identical bounds to shadow_report so numbers tie out to the reports.
MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5


def _is_phantom(peak, pnl_pct) -> bool:
    """A cross-source/stale-supply pricing artifact, not a real trade."""
    if peak is not None and float(peak) > MAX_SANE_PEAK:
        return True
    if pnl_pct is not None and (float(pnl_pct) > MAX_SANE_PNL_PCT
                                or float(pnl_pct) < MIN_SANE_PNL_PCT):
        return True
    return False


PAPER_Q = """
SELECT tp.call_id,
       COALESCE(ch.handle, '?')        AS channel,
       COALESCE(tp.vip_tier, 'none')   AS vip_tier,
       COALESCE(c.skip_reason, 'none') AS skip_reason,
       tp.entry_time,
       tp.exit_reason,
       tp.pnl_sol,
       tp.pnl_pct,
       tp.peak_multiplier,
       tp.sol_in,
       t.symbol
FROM trading_positions tp
JOIN calls  c ON c.id = tp.call_id
JOIN tokens t ON t.id = c.token_id
LEFT JOIN channels ch ON ch.id = c.channel_id
WHERE tp.is_simulation = TRUE
  AND tp.status = 'closed'
  AND tp.is_strategy_b = %(is_b)s
  {date_filter}
"""

# All shadow variants for the matched calls; we pick the lane's variant per trade in Python.
SHADOW_Q = """
SELECT sp.call_id, sp.exit_variant, sp.exit_reason,
       sp.pnl_sol, sp.pnl_pct, sp.peak_multiplier, sp.sol_in
FROM shadow_positions sp
WHERE sp.status = 'closed'
  AND sp.call_id = ANY(%(ids)s)
"""


def _fmt_lane(ch, tier, skip) -> str:
    return f"{ch[:15]:<15} {tier[:6]:<6} {skip[:13]:<13}"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=0, help="lookback in days (0 = all time)")
    ap.add_argument("--today", action="store_true",
                    help="only today (UTC calendar day); overrides --days")
    ap.add_argument("--strategy", choices=["a", "b"], default="a",
                    help="which paper trader: a=anchors, b=anchors+pockets (default a)")
    ap.add_argument("--detail", action="store_true",
                    help="per-trade breakdown, biggest shadow-beats-paper gap first")
    ap.add_argument("--min-trades", type=int, default=1,
                    help="hide lanes with fewer than this many matched trades")
    ap.add_argument("--normalize", action="store_true",
                    help="compare PnL per 1 SOL deployed (use for windows spanning the 0.1 era)")
    ap.add_argument("--raw", action="store_true",
                    help="include phantom-price rows instead of excluding them")
    args = ap.parse_args()

    if args.today:
        date_filter = "AND tp.exit_time >= CURRENT_DATE"
    elif args.days > 0:
        date_filter = "AND tp.exit_time >= now() - (%(days)s || ' days')::interval"
    else:
        date_filter = ""

    is_b = args.strategy == "b"
    db.ensure_shadow_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    params = {"is_b": is_b, "days": args.days}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(PAPER_Q.format(date_filter=date_filter), params)
        paper_rows = cur.fetchall()
        ids = [r["call_id"] for r in paper_rows]
        shadow_map: dict = {}
        if ids:
            cur.execute(SHADOW_Q, {"ids": ids})
            for r in cur.fetchall():
                shadow_map[(r["call_id"], r["exit_variant"])] = r
    db.close_conn()

    # Match each paper trade to the shadow row for the variant its lane runs.
    cells: dict = {}          # (ch,tier,skip,variant) -> accumulator
    details: list = []
    n_paper = len(paper_rows)
    n_matched = n_no_variant = n_phantom = 0
    tot_paper = tot_shadow = 0.0

    for p in paper_rows:
        variant = lane_policy.lane_exit(p["channel"], p["vip_tier"],
                                        p["skip_reason"], p["entry_time"])
        s = shadow_map.get((p["call_id"], variant))
        if s is None:
            n_no_variant += 1
            continue
        if not args.raw and (_is_phantom(s["peak_multiplier"], s["pnl_pct"])
                             or _is_phantom(p["peak_multiplier"], p["pnl_pct"])):
            n_phantom += 1
            continue
        n_matched += 1

        p_sol = float(p["pnl_sol"] or 0)
        s_sol = float(s["pnl_sol"] or 0)
        if args.normalize:
            p_sol /= float(p["sol_in"] or 1) or 1
            s_sol /= float(s["sol_in"] or 1) or 1
        tot_paper += p_sol
        tot_shadow += s_sol

        pk_p, pk_s = p["peak_multiplier"], s["peak_multiplier"]
        cap = (float(pk_p) / float(pk_s)) if (pk_p and pk_s and float(pk_s) > 0) else None

        key = (p["channel"], p["vip_tier"], p["skip_reason"], variant)
        c = cells.setdefault(key, {"n": 0, "p": 0.0, "s": 0.0, "cap_sum": 0.0, "cap_n": 0})
        c["n"] += 1
        c["p"] += p_sol
        c["s"] += s_sol
        if cap is not None:
            c["cap_sum"] += cap
            c["cap_n"] += 1

        if args.detail:
            details.append({
                "sym": p["symbol"], "lane": _fmt_lane(p["channel"], p["vip_tier"], p["skip_reason"]),
                "var": variant, "p_exit": p["exit_reason"], "p_pct": p["pnl_pct"], "p_sol": p_sol,
                "s_exit": s["exit_reason"], "s_pct": s["pnl_pct"], "s_sol": s_sol,
                "cap": cap, "edge": p_sol - s_sol,
            })

    # ── Report ──
    win = ("today" if args.today else f"last {args.days}d" if args.days else "all time")
    norm = "  (per-1-SOL normalized)" if args.normalize else ""
    print(f"\nPAPER vs SHADOW — Strategy {args.strategy.upper()} — {win}{norm}")
    print(f"matched {n_matched}/{n_paper} closed paper trades"
          + (f"  (skipped: {n_no_variant} no-shadow-variant, {n_phantom} phantom)"
             if (n_no_variant or n_phantom) else "") + "\n")

    hdr = f"{'LANE':<37} {'VAR':<8} {'N':>4} {'PAPER':>9} {'SHADOW':>9} {'EDGE':>9} {'CAP':>6}"
    print(hdr)
    print("─" * len(hdr))
    # Worst edge (paper most behind shadow) first — that's where any leak hides.
    for key, c in sorted(cells.items(), key=lambda kv: kv[1]["p"] - kv[1]["s"]):
        if c["n"] < args.min_trades:
            continue
        ch, tier, skip, variant = key
        edge = c["p"] - c["s"]
        cap = c["cap_sum"] / c["cap_n"] if c["cap_n"] else float("nan")
        print(f"{_fmt_lane(ch, tier, skip):<37} {variant:<8} {c['n']:>4} "
              f"{c['p']:>+9.3f} {c['s']:>+9.3f} {edge:>+9.3f} {cap:>6.2f}")

    print("─" * len(hdr))
    tot_edge = tot_paper - tot_shadow
    tot_cap = "" if not n_matched else ""
    print(f"{'TOTAL':<37} {'':<8} {n_matched:>4} "
          f"{tot_paper:>+9.3f} {tot_shadow:>+9.3f} {tot_edge:>+9.3f}")

    if args.detail and details:
        print("\nPER-TRADE (biggest shadow-beats-paper gap first):")
        dh = (f"{'SYMBOL':<16} {'VAR':<8} {'PAPER_EXIT':<12} {'P%':>7} {'P_SOL':>8}  "
              f"{'SHADOW_EXIT':<12} {'S%':>7} {'S_SOL':>8} {'CAP':>6} {'EDGE':>8}")
        print(dh)
        print("─" * len(dh))
        for d in sorted(details, key=lambda x: x["edge"]):
            cap = f"{d['cap']:.2f}" if d["cap"] is not None else "  -"
            print(f"{(d['sym'] or '?')[:16]:<16} {d['var']:<8} "
                  f"{(d['p_exit'] or '?'):<12} {float(d['p_pct'] or 0):>+7.1f} {d['p_sol']:>+8.3f}  "
                  f"{(d['s_exit'] or '?'):<12} {float(d['s_pct'] or 0):>+7.1f} {d['s_sol']:>+8.3f} "
                  f"{cap:>6} {d['edge']:>+8.3f}")

    print("\n  EDGE = paper - shadow. Negative = paper made less than idealized shadow, but")
    print("  shadow is an OPTIMISTIC upper bound (coarser polling under-samples dips). A small")
    print("  negative EDGE with CAP~1.0 = shadow survivorship, NOT a leak. Large negative EDGE")
    print("  with CAP<<1.0 = real, capturable feed under-capture.\n")


if __name__ == "__main__":
    main()
