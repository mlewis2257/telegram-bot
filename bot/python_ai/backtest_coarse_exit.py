"""
backtest_coarse_exit.py — would COARSER exit polling capture more of shadow's edge?

The question behind "shadow keeps beating paper on the runners": paper reacts to a
noisy per-swap feed (ws_monitor, ~3s helius_tx ticks) and exits on transient dips;
shadow polls ~15s and rides through them. This replays each closed paper position's
exit at a COARSE cadence using the SAME production exit logic (exit_config.apply_exit_config,
called exactly like shadow_monitor), fed a subsampled tick series from ws_market_observations.

    python3 backtest_coarse_exit.py --days 7 --strategy b
    python3 backtest_coarse_exit.py --days 7 --strategy b --interval 12 --detail

Three columns per lane, all at the PAPER position's own size:
  PAPER  — what paper actually realized (trading_positions.pnl_sol).
  COARSE — replay of the SAME exit config at --interval cadence on the ws ticks.
  SHADOW — the live 15s shadow's realized result (shadow_positions), for reference.

HONEST DATA CAVEAT (read this): ws_market_observations only logs ticks WHILE paper held
the position — the feed stops the instant paper exits. So when the coarse replay rides
THROUGH to the end of available ticks (never triggers within the observed window), we
cannot see what happened next from ticks. For those, we borrow SHADOW's realized outcome
as the continuation (shadow = the live coarse-held version of the same coin). Rows resolved
that way are flagged `->shadow` and counted separately — they lean on shadow being a fair
proxy for "what a coarse-held paper position would have done."

Approximations (kept small, all noted): ride_vol -> EXIT_RIDE (no historical RVOL replay);
peak = running max of SAMPLED ticks (no corroboration guard_peak); time_stop evaluated off
tick timestamps. These bias toward FAITHFUL cadence comparison, not perfect PnL reproduction.
Run with --validate to see how often the DENSE (every-tick) replay reproduces paper's real
exit_reason — that's the trust check on the harness.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import timezone

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db
import lane_policy
from exit_config import EXIT_A_PAPER, EXIT_RIDE, apply_exit_config

# Phantom guard — identical bounds to compare_shadow / shadow_report.
MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5

# variant -> exit config. ride_vol approximated as RIDE (its riding branch; no live RVOL here).
_VARIANT_CONFIGS = {"early": EXIT_A_PAPER, "ride": EXIT_RIDE, "ride_vol": EXIT_RIDE}


def _is_phantom(peak, pnl_pct) -> bool:
    if peak is not None and float(peak) > MAX_SANE_PEAK:
        return True
    if pnl_pct is not None and (float(pnl_pct) > MAX_SANE_PNL_PCT
                                or float(pnl_pct) < MIN_SANE_PNL_PCT):
        return True
    return False


def _aware(ts):
    if ts is not None and ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts


PAPER_Q = """
SELECT tp.call_id,
       COALESCE(ch.handle, '?')        AS channel,
       COALESCE(tp.vip_tier, 'none')   AS vip_tier,
       COALESCE(c.skip_reason, 'none') AS skip_reason,
       tp.entry_time, tp.entry_price, tp.exit_reason,
       tp.pnl_sol, tp.pnl_pct, tp.peak_multiplier, tp.sol_in,
       t.symbol
FROM trading_positions tp
JOIN calls  c ON c.id = tp.call_id
JOIN tokens t ON t.id = c.token_id
LEFT JOIN channels ch ON ch.id = c.channel_id
WHERE tp.is_simulation = TRUE
  AND tp.status = 'closed'
  AND tp.is_strategy_b = %(is_b)s
  AND tp.entry_price > 0
  {date_filter}
"""

SHADOW_Q = """
SELECT sp.call_id, sp.exit_variant, sp.exit_reason,
       sp.pnl_pct, sp.peak_multiplier
FROM shadow_positions sp
WHERE sp.status = 'closed' AND sp.call_id = ANY(%(ids)s)
"""

TICKS_Q = """
SELECT call_id, mcap, observed_at
FROM ws_market_observations
WHERE call_id = ANY(%(ids)s) AND mcap > 0
ORDER BY call_id, observed_at
"""


def replay(ticks, entry, cfg, is_gamble, handle, entry_time, interval):
    """Replay the exit config over `ticks` [(observed_at, mcap)] at `interval` seconds
    cadence (interval=0 => every tick / dense). Returns (reason, exit_mcap) or (None, None)
    if it rode through to the end of the ticks."""
    peak = 0.0
    last_sample = None
    for obs_at, m in ticks:
        if interval > 0 and last_sample is not None \
                and (obs_at - last_sample).total_seconds() < interval:
            continue
        last_sample = obs_at
        if m <= 0:
            continue
        peak = max(peak, m)
        if entry_time is not None:
            age_h = (obs_at - entry_time).total_seconds() / 3600.0
            if age_h > cfg.max_hours:
                return ("time_stop", m)
        res = apply_exit_config(
            cfg, current_mcap=m, peak_mcap=peak, entry_mcap=entry,
            is_vip_gamble=is_gamble, channel_handle=handle, entry_time=None,
        )
        if res.should_exit:
            return (res.reason, res.exit_mcap or m)
    return (None, None)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7, help="lookback in days (0 = all time)")
    ap.add_argument("--today", action="store_true", help="only today (UTC); overrides --days")
    ap.add_argument("--strategy", choices=["a", "b"], default="b")
    ap.add_argument("--interval", type=float, default=15.0,
                    help="coarse sample cadence in seconds (default 15 = shadow-like)")
    ap.add_argument("--detail", action="store_true", help="per-coin breakdown, biggest coarse-vs-paper gain first")
    ap.add_argument("--min-trades", type=int, default=1)
    ap.add_argument("--validate", action="store_true",
                    help="also report how often a DENSE replay reproduces paper's real exit_reason")
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
        shadow_map, tick_map = {}, {}
        if ids:
            cur.execute(SHADOW_Q, {"ids": ids})
            for r in cur.fetchall():
                shadow_map[(r["call_id"], r["exit_variant"])] = r
            cur.execute(TICKS_Q, {"ids": ids})
            for r in cur.fetchall():
                tick_map.setdefault(r["call_id"], []).append(
                    (_aware(r["observed_at"]), float(r["mcap"])))
    db.close_conn()

    cells: dict = {}
    details: list = []
    n_match = n_noticks = n_phantom = n_proxied = n_replayed = 0
    dense_ok = dense_n = 0
    tot_p = tot_c = tot_s = 0.0

    for p in paper_rows:
        variant = lane_policy.lane_exit(p["channel"], p["vip_tier"], p["skip_reason"], p["entry_time"])
        variant = variant or "early"
        s = shadow_map.get((p["call_id"], variant))
        if s and _is_phantom(s["peak_multiplier"], s["pnl_pct"]):
            n_phantom += 1
            continue
        ticks = tick_map.get(p["call_id"], [])
        if not ticks:
            n_noticks += 1
            continue

        entry = float(p["entry_price"])
        sol_in = float(p["sol_in"] or 0)
        cfg = _VARIANT_CONFIGS.get(variant, EXIT_A_PAPER)
        is_gamble = p["vip_tier"] in ("gamble", "gamble_risk")
        handle = (p["channel"] or "").lstrip("@")
        etime = _aware(p["entry_time"])

        reason, exit_mcap = replay(ticks, entry, cfg, is_gamble, handle, etime, args.interval)
        if reason is not None:
            c_pct = (exit_mcap / entry - 1.0) * 100.0
            c_reason = reason
            n_replayed += 1
        else:
            # Rode through the observed window — borrow shadow's realized outcome.
            if s is None:
                # No continuation available; mark closed at last observed tick (conservative).
                last_m = ticks[-1][1]
                c_pct = (last_m / entry - 1.0) * 100.0
                c_reason = "->lasttick"
            else:
                c_pct = float(s["pnl_pct"] or 0)
                c_reason = "->shadow:" + (s["exit_reason"] or "?")
                n_proxied += 1

        if args.validate:
            d_reason, _ = replay(ticks, entry, cfg, is_gamble, handle, etime, 0.0)
            dense_n += 1
            if d_reason == p["exit_reason"]:
                dense_ok += 1

        p_sol = float(p["pnl_sol"] or 0)
        c_sol = sol_in * c_pct / 100.0
        s_sol = sol_in * float(s["pnl_pct"] or 0) / 100.0 if s else None
        tot_p += p_sol
        tot_c += c_sol
        if s_sol is not None:
            tot_s += s_sol
        n_match += 1

        key = (p["channel"], p["vip_tier"], p["skip_reason"], variant)
        cc = cells.setdefault(key, {"n": 0, "p": 0.0, "c": 0.0, "s": 0.0})
        cc["n"] += 1
        cc["p"] += p_sol
        cc["c"] += c_sol
        cc["s"] += s_sol if s_sol is not None else 0.0

        if args.detail:
            details.append({
                "sym": p["symbol"], "var": variant,
                "p_exit": p["exit_reason"], "p_pct": float(p["pnl_pct"] or 0),
                "c_exit": c_reason, "c_pct": c_pct,
                "s_pct": float(s["pnl_pct"]) if s else None,
                "gain": c_sol - p_sol,
            })

    win = "today" if args.today else (f"last {args.days}d" if args.days else "all time")
    print(f"\nCOARSE-EXIT BACKTEST — Strategy {args.strategy.upper()} — {win} — "
          f"coarse cadence = {args.interval:g}s")
    print(f"matched {n_match} positions  (replayed-exit {n_replayed}, rode-through->shadow {n_proxied}; "
          f"skipped {n_noticks} no-ticks, {n_phantom} phantom)")
    if args.validate and dense_n:
        print(f"validation: dense (every-tick) replay reproduced paper's exit_reason "
              f"on {dense_ok}/{dense_n} ({100*dense_ok/dense_n:.0f}%)")
    print()

    hdr = f"{'LANE':<37} {'VAR':<8} {'N':>4} {'PAPER':>9} {'COARSE':>9} {'SHADOW':>9} {'Δc-p':>8}"
    print(hdr)
    print("─" * len(hdr))
    for key, c in sorted(cells.items(), key=lambda kv: -(kv[1]["c"] - kv[1]["p"])):
        if c["n"] < args.min_trades:
            continue
        ch, tier, skip, variant = key
        lane = f"{ch[:15]:<15} {tier[:6]:<6} {skip[:13]:<13}"
        print(f"{lane:<37} {variant:<8} {c['n']:>4} "
              f"{c['p']:>+9.3f} {c['c']:>+9.3f} {c['s']:>+9.3f} {c['c']-c['p']:>+8.3f}")
    print("─" * len(hdr))
    print(f"{'TOTAL':<37} {'':<8} {n_match:>4} {tot_p:>+9.3f} {tot_c:>+9.3f} {tot_s:>+9.3f} {tot_c-tot_p:>+8.3f}")

    if args.detail and details:
        print("\nPER-COIN (biggest coarse-beats-paper gain first):")
        dh = (f"{'SYMBOL':<16} {'VAR':<8} {'PAPER_EXIT':<12} {'P%':>7}  "
              f"{'COARSE_EXIT':<18} {'C%':>7}  {'SHADOW%':>8} {'Δc-p SOL':>9}")
        print(dh)
        print("─" * len(dh))
        for d in sorted(details, key=lambda x: -x["gain"]):
            spct = f"{d['s_pct']:+.1f}" if d["s_pct"] is not None else "   -"
            print(f"{(d['sym'] or '?')[:16]:<16} {d['var']:<8} "
                  f"{(d['p_exit'] or '?'):<12} {d['p_pct']:>+7.1f}  "
                  f"{d['c_exit'][:18]:<18} {d['c_pct']:>+7.1f}  {spct:>8} {d['gain']:>+9.3f}")

    print("\n  PAPER=actual realized. COARSE=same exit config replayed at the coarse cadence")
    print("  (rows shown ->shadow rode through the observed ticks and use shadow's realized")
    print("  outcome as continuation). Δc-p>0 = coarsening would have HELPED that lane.")
    print("  If COARSE lands near SHADOW and above PAPER, shadow's edge is cadence-capturable.\n")


if __name__ == "__main__":
    main()
