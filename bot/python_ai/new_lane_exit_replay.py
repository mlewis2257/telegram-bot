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
# qsim OVERSTATES upside exits: it looks every 15-90 s, so by the time it sees a coin past
# 2x the coin is often at 2.3x and it books that. Live sells at the line (paired new-lane
# banks 2026-10-19: live 2.09x vs qsim 2.34x). With FILL_CAP set, any exit triggered by
# REACHING a level (bank, target, buy-back bank) is credited at no more than
# level * (1 + FILL_CAP). Stops and floors are not capped: they already sell at the look.
FILL_CAP: float | None = 0.05


def _cap(m: float, level: float) -> float:
    return m if FILL_CAP is None else min(m, level * (1.0 + FILL_CAP))


@dataclass(frozen=True)
class Rules:
    stop: float = 0.80
    bank: float | None = 2.0
    floors: tuple = ((1.6, 1.30), (1.35, 1.15))
    steep: tuple | None = None          # (x3_max, target, floor, hours)
    buyback: tuple | None = None        # (trigger, stop_frac, bank_mult, hours)
    target: float | None = None         # sell everything at the first look >= target
    trail: tuple | None = None          # (arm, pct): once peak >= arm, sell at peak*(1-pct)
    trail_tiers: tuple = ()             # ((arm, pct), ...): the highest arm reached sets the trail
    split: float | None = None          # sell this fraction at the 2x bank; the rest runs on
                                        # these rules (no bank). Result is the blend.
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
                return _with_buyback(path, r, Result(_cap(m, target), "steep_target", t))
            if m <= sfloor:
                return _with_buyback(path, r, Result(m, "steep_floor", t))
            if t - hold_t > hours * 3600:
                return _with_buyback(path, r, Result(m, "steep_time", t))
            continue
        peak = max(peak, m)
        if m <= r.stop:
            return _with_buyback(path, r, Result(m, "stop", t))
        if r.target is not None and m >= r.target:
            return Result(_cap(m, r.target), "target", t)
        if r.bank is not None and m >= r.bank:
            if r.steep is not None:
                x3 = _x3(path, t)
                if x3 is not None and x3 < r.steep[0] and m < r.steep[1]:
                    hold_t = t
                    continue
            return Result(_cap(m, r.bank), "bank", t)
        for arm, fl in sorted(r.floors, key=lambda z: -z[0]):
            if peak >= arm and m <= fl:
                return _with_buyback(path, r, Result(m, "floor", t))
            if peak >= arm:
                break
        if r.trail is not None and peak >= r.trail[0] and m <= peak * (1.0 - r.trail[1]):
            return Result(m, "trail", t)
        if r.trail_tiers:
            pct = None
            for arm, p in sorted(r.trail_tiers, key=lambda z: -z[0]):
                if peak >= arm:
                    pct = p
                    break
            if pct is not None and m <= peak * (1.0 - pct):
                return Result(m, "trail", t)
    end = _last(path, HORIZON_S)
    return _with_buyback(path, r, Result(1.0 if end is None else end, "time", HORIZON_S))


def simulate_split(path: list, r: Rules) -> Result:
    """With r.split: that fraction is sold at the 2x bank (today's rules otherwise: same
    stop and pre-2x floor), the rest runs on r. Without it, plain simulate()."""
    if not r.split:
        return simulate(path, r)
    pre = tuple(f for f in r.floors if f[0] < 2.0)
    banked = simulate(path, Rules(stop=r.stop, bank=2.0, floors=pre))
    runner = simulate(path, Rules(stop=r.stop, bank=None, floors=r.floors, target=r.target,
                                  trail=r.trail, trail_tiers=r.trail_tiers))
    x = r.split * banked.x + (1.0 - r.split) * runner.x
    reason = banked.reason if banked.reason != "bank" else f"half-bank+{runner.reason}"
    return Result(x, reason, max(banked.t, runner.t))


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
        if m <= stop_frac * m0:
            out = m / m0
            break
        if m >= bank_mult * m0:
            out = _cap(m / m0, bank_mult)
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


def runner_grid() -> list[Rules]:
    """No 2x bank: let winners run, protected only once they have shown 2x+.

    'floor after 2x' = the profit floor arms only once the coin has reached 2x, and
    then sells if it falls back to FLOOR. Below 2x the pre-2x floor is either off (the
    new-lane finding) or today's tiers. Targets sell everything at 3x / 5x. Trails sell
    at a fixed fraction below the running peak once the coin has passed the arm level.
    The first row is today's rules, for reference.
    """
    now_floors = ((1.6, 1.30), (1.35, 1.15))
    pre = {"pre-2x floor off": (), "pre-2x floor now": now_floors}
    post = {
        "bank 2x": dict(bank=2.0),
        "floor after 2x @1.5": dict(bank=None, post_floor=(2.0, 1.5)),
        "floor after 2x @1.5 + target 3x": dict(bank=None, post_floor=(2.0, 1.5), target=3.0),
        "floor after 2x @1.5 + target 5x": dict(bank=None, post_floor=(2.0, 1.5), target=5.0),
        "floor after 2x @1.3 + target 3x": dict(bank=None, post_floor=(2.0, 1.3), target=3.0),
        "trail 30% after 2x": dict(bank=None, trail=(2.0, 0.30)),
        "trail 40% after 2x": dict(bank=None, trail=(2.0, 0.40)),
        "floor after 2x @1.5 + trail 25% after 3x": dict(bank=None, post_floor=(2.0, 1.5), trail=(3.0, 0.25)),
        "target 3x, nothing else": dict(bank=None, target=3.0),
    }
    bbs = {"": None, "buyback 1.2x": (1.2, 0.8, 2.0, 6)}
    out = [Rules(label="NOW: stop 0.80 | bank 2x | floor now")]
    for (pn, pv), (qn, qv), (bn, bv) in itertools.product(pre.items(), post.items(), bbs.items()):
        floors = tuple(pv) + ((qv["post_floor"],) if "post_floor" in qv else ())
        out.append(Rules(stop=0.80, bank=qv.get("bank"), floors=floors, buyback=bv,
                         target=qv.get("target"), trail=qv.get("trail"),
                         label=" | ".join(x for x in (pn, qn, bn) if x)))
    return out


def trail_grid() -> list[Rules]:
    """Trailing stops instead of (or after part of) the 2x bank.

    'always' trails are armed from entry: sell when the coin falls PCT below its highest
    look so far (the 20% hard stop still applies). Tiered trails arm at 2x and tighten at
    3x and 5x: 'trails 35/30/25' = 35% below the peak once past 2x, 30% past 3x, 25% past 5x.
    'half at 2x' sells half the position at the 2x bank and trails the rest.
    """
    now_floors = ((1.6, 1.30), (1.35, 1.15))
    pre = {"pre-2x floor now": now_floors, "pre-2x floor off": ()}
    after2x = ((2.0, 1.5),)
    tiers = {
        "trails 25/20/15": ((2.0, 0.25), (3.0, 0.20), (5.0, 0.15)),
        "trails 30/25/20": ((2.0, 0.30), (3.0, 0.25), (5.0, 0.20)),
        "trails 35/30/25": ((2.0, 0.35), (3.0, 0.30), (5.0, 0.25)),
        "trails 40/35/30": ((2.0, 0.40), (3.0, 0.35), (5.0, 0.30)),
    }
    out = [Rules(label="NOW: stop 0.80 | bank 2x | floor now")]
    for pn, pv in pre.items():
        for pct in (0.25, 0.35, 0.50):
            out.append(Rules(bank=None, floors=pv, trail_tiers=((0.0, pct),),
                             label=f"{pn} | no bank | trail {int(pct * 100)}% always"))
        for tn, tv in tiers.items():
            out.append(Rules(bank=None, floors=pv, trail_tiers=tv, label=f"{pn} | no bank | {tn}"))
            out.append(Rules(bank=None, floors=pv + after2x, trail_tiers=tv,
                             label=f"{pn} | no bank | {tn} + floor after 2x @1.5"))
        for tn in ("trails 35/30/25", "trails 40/35/30"):
            out.append(Rules(bank=None, floors=pv + after2x, trail_tiers=tiers[tn], split=0.5,
                             label=f"{pn} | half at 2x, rest {tn} + floor after 2x @1.5"))
    return out


def slice_grid() -> list[Rules]:
    """Bank most at 2x, keep a slice with a TIGHT floor (2026-10-19).

    Banked new-lane coins barely dip after the bank (median low 1.95x) and peak fast
    (median 11 min); 5 of 11 filtered banks went to 3x+. Earlier hold rules used a 1.5x
    floor and gave a lot back on the banks that did not run. Here only a slice is kept,
    and its floor sits just under 2x. 'keep 30%' = sell 70% at the 2x bank.
    """
    now_floors = ((1.6, 1.30), (1.35, 1.15))
    out = [Rules(label="NOW: stop 0.80 | bank 2x | floor now")]
    for keep in (0.30, 0.50):
        for fl in (1.8, 1.6):
            for tn, tgt, tr in (("target 4x", 4.0, ()), ("target 5x", 5.0, ()),
                                ("trail 25% after 3x", None, ((3.0, 0.25),))):
                out.append(Rules(bank=None, floors=now_floors + ((2.0, fl),), target=tgt,
                                 trail_tiers=tr, split=1.0 - keep,
                                 label=f"keep {int(keep * 100)}% after 2x | slice floor {fl}x | {tn}"))
    return out


# ── Entry timing: buy the shakeout on purpose ──────────────────────────────────
# 2026-10-21: the lane's coins still double after the call about as often as before, but
# far fewer reach 2x from our entry before a 20%+ dip stops us out (bank rate 19-29% early
# October -> 7-13%). These rules enter the dip deliberately instead of being shaken out by it.

def _rebase(path: list, t0: float, m0: float) -> list:
    return [(t - t0, None if m is None else m / m0) for t, m in path if t >= t0]


def _first_dip(path: list, level: float, within_s: float):
    for t, m in path:
        if t > within_s:
            return None
        if m is not None and m <= level:
            return t, m
    return None


def sim_pullback(path: list, dip: float, within_min: float, base: Rules) -> float | None:
    """Buy only at the first look at or below `dip` (x of call-time price) within the window;
    then today's rules measured from THAT price. None = never dipped = no trade."""
    hit = _first_dip(path, dip, within_min * 60)
    if hit is None:
        return None
    t0, m0 = hit
    return simulate(_rebase(path, t0, m0), base).x


def sim_scale_in(path: list, dip: float, within_min: float, stop_frac: float, base: Rules) -> float:
    """Half at the call; the other half at the first look <= `dip` within the window.
    Before the dip fills, the first half has no stop above the dip (the dip IS the add).
    Filled: the whole position is managed from its average price — bank at 2x of it, sell
    if it falls `stop_frac` below it, today's profit floor measured from it. Not filled: the
    first half runs on today's rules from the call. Returned as x of the full position."""
    hit = _first_dip(path, dip, within_min * 60)
    if hit is None:
        return 0.5 * simulate(path, base).x + 0.5 * 1.0       # half never deployed: cash
    t1, m1 = hit
    avg = (1.0 + m1) / 2.0
    rel = [(t, m) for t, m in _rebase(path, 0.0, avg) if t >= t1]
    r = simulate(rel, Rules(stop=1.0 - stop_frac, bank=base.bank, floors=base.floors))
    return r.x


def entry_rows(positions: list) -> list[tuple[str, list]]:
    base = Rules()
    out = [("NOW: buy at the call, today's exits", [simulate(p["path"], base).x for p in positions])]
    for dip, win in ((0.85, 15), (0.80, 15), (0.75, 15), (0.70, 30)):
        out.append((f"wait for a dip to {dip:.2f}x within {win} min",
                    [sim_pullback(p["path"], dip, win, base) for p in positions]))
    for dip, win, sf in ((0.80, 15, 0.25), (0.75, 15, 0.25), (0.75, 15, 0.30), (0.70, 30, 0.25)):
        out.append((f"scale in: half at call, half at {dip:.2f}x within {win} min, stop {int(sf * 100)}% below avg",
                    [sim_scale_in(p["path"], dip, win, sf, base) for p in positions]))
    return out


def print_entry_grid(positions: list) -> None:
    n = len(positions)
    half = n // 2
    rows = entry_rows(positions)
    now = rows[0][1]
    now_net = sum(x - 1 for x in now)
    print(f"\n{'entry rule':76s} {'trades':>6s} {'net':>7s} {'vs now':>7s} {'1st half':>8s} "
          f"{'2nd half':>8s} {'w/o best':>8s}")
    for label, xs in rows:
        nets = [0.0 if x is None else x - 1 for x in xs]
        traded = sum(1 for x in xs if x is not None)
        net = sum(nets)
        print(f"{label:76s} {traded:6d} {net:+7.2f} {net - now_net:+7.2f} {sum(nets[:half]):+8.2f} "
              f"{sum(nets[half:]):+8.2f} {net - max(nets):+8.2f}")
    print("\nnow: 1st half {:+.2f}, 2nd half {:+.2f}. Units: full positions. 'wait for a dip' trades "
          "only the coins that dipped; the rest count as no trade (0).".format(
              sum(x - 1 for x in now[:half]), sum(x - 1 for x in now[half:])))


def load(since: str, lane: str):
    conn = db.get_conn()
    db.safe_rollback()
    lane_sql = "c.skip_reason IS NULL" if lane == "none" else "c.skip_reason = %s"
    params = [since] + ([] if lane == "none" else [lane])
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT qp.call_id, left(t.symbol, 12), COALESCE(t.security_flag, '-'),
                   c.mcap_at_call, qp.entry_time, qp.exit_reason, qp.sol_out / qp.sol_in,
                   c.conviction_score, t.hodl_count, t.token_age_minutes
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
    for cid, sym, flag, mcap, entry, q_reason, q_x, score, holders, age in pos:
        path = [((at - entry).total_seconds(), m) for at, m in obs.get(cid, []) if at >= entry]
        if not path:
            continue
        positions.append({
            "call_id": cid, "sym": sym, "flag": flag,
            "in_band": mcap is not None and 80000 <= float(mcap) <= 120000,
            "entry": entry, "q_reason": q_reason, "q_x": float(q_x or 0), "path": path,
            "score": None if score is None else float(score),
            "holders": None if holders is None else int(holders),
            "age": None if age is None else float(age),
        })
    return positions, obs


def score(positions: list, r: Rules) -> dict:
    nets, extras, reasons = [], [], defaultdict(int)
    per_pos = []
    for p in positions:
        res = simulate_split(p["path"], r)
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
    global FILL_CAP
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-10-15 06:50")
    ap.add_argument("--lane", default="none", choices=["none", "low_score"])
    ap.add_argument("--top", type=int, default=12)
    ap.add_argument("--grid", default="exits", choices=["exits", "runner", "trail", "slice", "entry"],
                    help="exits = stop/floor/steep/buy-back; runner = no-bank targets and floors; "
                         "trail = trailing stops (always, or tiered at 2x/3x/5x, or after half at 2x)")
    ap.add_argument("--max-holders", type=int, default=0,
                    help="skip coins with this many holders or more at the call (0 = no filter)")
    ap.add_argument("--min-score", type=float, default=0,
                    help="skip calls scoring below this (0 = no filter)")
    ap.add_argument("--skip-age", default="",
                    help="skip tokens whose age in minutes is in LO-HI, e.g. 5-30")
    ap.add_argument("--no-fill-cap", action="store_true",
                    help="book upside exits at qsim's look price (overstates banks)")
    a = ap.parse_args()
    if a.no_fill_cap:
        FILL_CAP = None

    allpos, _ = load(a.since, a.lane)
    age_lo = age_hi = None
    if a.skip_age:
        age_lo, age_hi = (float(v) for v in a.skip_age.split("-"))

    def keep(p) -> bool:
        if a.max_holders and p["holders"] is not None and p["holders"] >= a.max_holders:
            return False
        if a.min_score and p["score"] is not None and p["score"] < a.min_score:
            return False
        if age_lo is not None and p["age"] is not None and age_lo <= p["age"] < age_hi:
            return False
        return True

    filt = [f"holders < {a.max_holders}"] if a.max_holders else []
    filt += [f"score >= {a.min_score:g}"] if a.min_score else []
    filt += [f"age not in {a.skip_age} min"] if a.skip_age else []
    if filt:
        before = len(allpos)
        allpos = [p for p in allpos if keep(p)]
        print(f"entry filters: {', '.join(filt)} — {len(allpos)} of {before} trades kept")
    print("upside fills: " + ("capped at the line + 5% (live-like)" if FILL_CAP is not None
                              else "qsim's look price (overstates banks)"))
    groups = {
        "safe, 80-120k (live's filter)": [p for p in allpos if p["flag"] == "safe" and p["in_band"]],
        "all flags and sizes (cross-check)": allpos,
    }
    if a.grid == "entry":
        for gname, ps in groups.items():
            print(f"\n=== {a.lane} lane since {a.since} — {gname}: {len(ps)} trades ===")
            if ps:
                print_entry_grid(ps)
        return
    grid = {"exits": rule_grid, "runner": runner_grid, "trail": trail_grid,
            "slice": slice_grid}[a.grid]()
    base = grid[0]
    for gname, ps in groups.items():
        print(f"\n=== {a.lane} lane since {a.since} — {gname}: {len(ps)} trades ===")
        if not ps:
            continue
        actual = sum(p["q_x"] - 1 for p in ps)
        saved, FILL_CAP = FILL_CAP, None
        raw = score(ps, base)
        FILL_CAP = saved
        b = score(ps, base)
        print(f"calibration: qsim's actual result {actual:+.2f} positions; this replay of the "
              f"current rules, uncapped, {raw['net']:+.2f}  (should be close)")
        if FILL_CAP is not None:
            print(f"current rules with live-like upside fills: {b['net']:+.2f}  "
                  f"(all comparisons below use these fills)")
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
