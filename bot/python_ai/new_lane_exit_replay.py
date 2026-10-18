"""
new_lane_exit_replay.py — which exit rules fit the unlabelled solwhaletrending lane?

WHY (2026-10-18). Since Oct 15 live trades the unlabelled lane (score 63+, younger and
less bundled coins). On Oct 18 live made 17 trades and no banks while 5 of 16 coins ran
to 2x+ after we sold. The exits were designed and tested on the old low_score lane; this
asks, on the new lane only, which exit rules would have done better.

HOW. Every real-quote look qsim recorded for each coin, from entry onward (in-life looks
and the post-exit looks), is replayed under each rule set from scratch:
  - hard stop        sell at the first look at or below the stop line
  - bank             sell at the first look at or above 2x (steep hold may override)
  - profit floor     tiers (peak arm -> floor), as live's EXIT_A_PAPER
  - steep hold       at the bank, if the coin was under 1.20x 3 min earlier, hold for a
                     target or a floor (multiples of entry) or N hours instead
  - buy-back         after a stop, buy again at the first look back at 1.2x of the
                     original entry; 20% stop / 2x bank from that price, 6h horizon.
                     Counted as an extra trade.
  - otherwise        sold at the last look within 24h (live's time stop)
Every sell is booked at the look that triggered it (no credit at the line itself).

LIMITS. After qsim's own exit, looks are once a minute for 30 min, once a minute while the
coin is back above entry, every 15 s around banks, otherwise hourly. A rule that holds
longer than qsim did can therefore see a fall late and book it lower than live would —
harsh on the patient rules, never generous. No fees. A no-route look is skipped unless
it is the last look (then the position is worth 0).

64 rule combinations are tried on a few dozen trades, so the best one will look better
than it is. Read the CONSISTENCY columns: a rule worth building improves both halves of
the window and does not depend on one coin.

Run on the VPS:  python3 new_lane_exit_replay.py
    --since '2026-10-15 06:50'   (when qsim started trading the unlabelled lane)
    --lane none|low_score         (default none = the new lane)
"""

from __future__ import annotations

import argparse
import itertools
from collections import defaultdict
from dataclasses import dataclass, field

import db

HORIZON_S = 24 * 3600
STEEP_LOOKBACK_S = 180


@dataclass(frozen=True)
class Rules:
    stop: float = 0.80
    bank: float | None = 2.0
    floors: tuple = ((1.6, 1.30), (1.35, 1.15))
    steep: tuple | None = None          # (x3_max, target, floor, hours)
    buyback: tuple | None = None        # (trigger, stop_frac, bank_mult, hours)
    label: str = ""


@dataclass
class Result:
    x: float
    reason: str
    t: float
    extra: list = field(default_factory=list)   # buy-back trade results (x of its own entry)


def _x3(path: list, t: float):
    """Latest valid multiple at or before t - lookback, or None."""
    best = None
    for ti, m in path:
        if ti > t - STEEP_LOOKBACK_S:
            break
        if m is not None:
            best = m
    return best


def _last(path: list, t_end: float):
    last = None
    for ti, m in path:
        if ti > t_end:
            break
        last = (ti, m)
    if last is None:
        return None
    return 0.0 if last[1] is None else last[1]


def simulate(path: list, r: Rules) -> Result:
    """path: [(seconds since entry, multiple or None for no-route)], ascending."""
    peak = 0.0
    hold_t = None
    for t, m in path:
        if t > HORIZON_S:
            break
        if m is None:
            continue
        if hold_t is not None:
            x3max, target, sfloor, hours = r.steep
            if m >= target:
                return _with_buyback(path, r, Result(m, "steep_target", t))
            if m <= sfloor:
                return _with_buyback(path, r, Result(m, "steep_floor", t))
            if t - hold_t > hours * 3600:
                return _with_buyback(path, r, Result(m, "steep_time", t))
            continue
        peak = max(peak, m)
        if m <= r.stop:
            return _with_buyback(path, r, Result(m, "stop", t))
        if r.bank is not None and m >= r.bank:
            if r.steep is not None:
                x3 = _x3(path, t)
                if x3 is not None and x3 < r.steep[0] and m < r.steep[1]:
                    hold_t = t
                    continue
            return Result(m, "bank", t)
        for arm, fl in sorted(r.floors, key=lambda z: -z[0]):
            if peak >= arm and m <= fl:
                return _with_buyback(path, r, Result(m, "floor", t))
            if peak >= arm:
                break
    end = _last(path, HORIZON_S)
    return _with_buyback(path, r, Result(1.0 if end is None else end, "time", HORIZON_S))


def _with_buyback(path: list, r: Rules, res: Result) -> Result:
    if r.buyback is None or res.reason != "stop":
        return res
    trig, stop_frac, bank_mult, hours = r.buyback
    t0 = m0 = None
    for t, m in path:
        if t <= res.t or m is None:
            continue
        if t > res.t + HORIZON_S:
            break
        if m >= trig:
            t0, m0 = t, m
            break
    if t0 is None:
        return res
    out = None
    for t, m in path:
        if t <= t0:
            continue
        if t > t0 + hours * 3600:
            break
        if m is None:
            continue
        if m <= stop_frac * m0 or m >= bank_mult * m0:
            out = m / m0
            break
    if out is None:
        end = _last(path, t0 + hours * 3600)
        out = (end if end is not None else m0) / m0
    res.extra.append(out)
    return res


def rule_grid() -> list[Rules]:
    floors = {
        "floor now (1.35>1.15, 1.6>1.30)": ((1.6, 1.30), (1.35, 1.15)),
        "floor off": (),
        "floor late (1.6>1.15)": ((1.6, 1.15),),
        "floor at entry (1.35>1.0)": ((1.35, 1.0),),
    }
    stops = {"stop 0.80": 0.80, "stop 0.75": 0.75, "stop 0.70": 0.70, "stop 0.65": 0.65}
    steeps = {"": None, "steep 3x": (1.20, 3.0, 1.2, 6)}
    bbs = {"": None, "buyback 1.2x": (1.2, 0.8, 2.0, 6)}
    out = []
    for (fn, fv), (sn, sv), (tn, tv), (bn, bv) in itertools.product(
            floors.items(), stops.items(), steeps.items(), bbs.items()):
        label = " | ".join(p for p in (sn, fn, tn, bn) if p)
        out.append(Rules(stop=sv, floors=fv, steep=tv, buyback=bv, label=label))
    return out


def load(since: str, lane: str):
    conn = db.get_conn()
    db.safe_rollback()
    lane_sql = "c.skip_reason IS NULL" if lane == "none" else "c.skip_reason = %s"
    params = [since] + ([] if lane == "none" else [lane])
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT qp.call_id, left(t.symbol, 12), COALESCE(t.security_flag, '-'),
                   c.mcap_at_call, qp.entry_time, qp.exit_reason, qp.sol_out / qp.sol_in
            FROM qsim_positions qp
            JOIN calls c ON c.id = qp.call_id
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.status = 'closed' AND qp.channel_handle = 'solwhaletrending'
              AND qp.entry_time >= %s AND qp.entry_time < now() - interval '6 hours'
              AND {lane_sql}
            ORDER BY qp.entry_time
            """, params)
        pos = cur.fetchall()
        ids = [p[0] for p in pos]
        if not ids:
            return [], {}
        cur.execute(
            """
            SELECT call_id, observed_at, real_mult, no_route
            FROM qsim_quote_observations
            WHERE call_id = ANY(%s) AND NOT rate_limited
              AND (real_mult IS NOT NULL OR no_route)
            ORDER BY call_id, observed_at
            """, (ids,))
        obs = defaultdict(list)
        for cid, at, m, nr in cur.fetchall():
            obs[cid].append((at, None if nr else float(m)))
    positions = []
    for cid, sym, flag, mcap, entry, q_reason, q_x in pos:
        path = [((at - entry).total_seconds(), m) for at, m in obs.get(cid, []) if at >= entry]
        if not path:
            continue
        positions.append({
            "call_id": cid, "sym": sym, "flag": flag,
            "in_band": mcap is not None and 80000 <= float(mcap) <= 120000,
            "entry": entry, "q_reason": q_reason, "q_x": float(q_x or 0), "path": path,
        })
    return positions, obs


def score(positions: list, r: Rules) -> dict:
    nets, extras, reasons = [], [], defaultdict(int)
    per_pos = []
    for p in positions:
        res = simulate(p["path"], r)
        reasons[res.reason] += 1
        net = res.x - 1.0 + sum(e - 1.0 for e in res.extra)
        nets.append(net)
        extras.extend(res.extra)
        per_pos.append((p, net, res))
    n = len(positions)
    half = n // 2
    best = max(nets) if nets else 0.0
    return {
        "net": sum(nets), "n": n, "reasons": dict(reasons),
        "bb_trades": len(extras), "bb_net": sum(e - 1 for e in extras),
        "first_half": sum(nets[:half]), "second_half": sum(nets[half:]),
        "without_best": sum(nets) - best, "per_pos": per_pos,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-10-15 06:50")
    ap.add_argument("--lane", default="none", choices=["none", "low_score"])
    ap.add_argument("--top", type=int, default=12)
    a = ap.parse_args()

    allpos, _ = load(a.since, a.lane)
    groups = {
        "safe, 80-120k (live's filter)": [p for p in allpos if p["flag"] == "safe" and p["in_band"]],
        "all flags and sizes (cross-check)": allpos,
    }
    grid = rule_grid()
    base = grid[0]
    for gname, ps in groups.items():
        print(f"\n=== {a.lane} lane since {a.since} — {gname}: {len(ps)} trades ===")
        if not ps:
            continue
        actual = sum(p["q_x"] - 1 for p in ps)
        b = score(ps, base)
        print(f"calibration: qsim's actual result {actual:+.2f} positions; this replay of the "
              f"current rules {b['net']:+.2f}  (should be close)")
        rows = sorted(((r, score(ps, r)) for r in grid), key=lambda rs: -rs[1]["net"])
        print(f"\n{'rules':58s} {'net':>7s} {'vs now':>7s} {'1st half':>8s} {'2nd half':>8s} "
              f"{'w/o best':>8s} {'buybacks':>9s}  exits")
        shown = rows[:a.top]
        if all(r is not base for r, _ in shown):
            shown.append(next(rs for rs in rows if rs[0] is base))
        for r, s in shown:
            bb = f"{s['bb_trades']}:{s['bb_net']:+.1f}" if r.buyback else ""
            ex = " ".join(f"{k}={v}" for k, v in sorted(s["reasons"].items()))
            tag = "  <- now" if r is base else ""
            print(f"{(r.label or 'current'):58s} {s['net']:+7.2f} {s['net'] - b['net']:+7.2f} "
                  f"{s['first_half']:+8.2f} {s['second_half']:+8.2f} {s['without_best']:+8.2f} "
                  f"{bb:>9s}  {ex}{tag}")
        print(f"\nnow: 1st half {b['first_half']:+.2f}, 2nd half {b['second_half']:+.2f}, "
              f"without best coin {b['without_best']:+.2f}.  A rule is believable only if it beats "
              f"'now' in BOTH halves and still beats it without its best coin.")
        best_r, best_s = rows[0]
        print(f"\nper coin, best rule ({best_r.label}) vs now:")
        bmap = {p["call_id"]: (net, res) for p, net, res in b["per_pos"]}
        diffs = []
        for p, net, res in best_s["per_pos"]:
            bnet, bres = bmap[p["call_id"]]
            if abs(net - bnet) > 1e-9:
                diffs.append((net - bnet, p, net, res, bnet, bres))
        for d, p, net, res, bnet, bres in sorted(diffs, key=lambda z: -abs(z[0]))[:15]:
            bb = (" + buy-back " + ", ".join(f"{e:.2f}x" for e in res.extra)) if res.extra else ""
            print(f"  {p['entry']:%m-%d %H:%M} {p['sym']:12s} now {bres.reason:6s} {bres.x:5.2f}x  ->  "
                  f"{res.reason:12s} {res.x:5.2f}x{bb}   ({d:+.2f} positions)")


if __name__ == "__main__":
    main()
