"""
stop_recovery_scan.py — which hard-stopped coins come back, and can we tell at the stop?

WHY (2026-10-21). About 1 in 4 solwhaletrending coins that stop us out reach 2x of our
entry within 24 h, and on the new lane 8 of 17 such score-80+ coins never fell below
0.75x first — shaken out, not rugged. Earlier work (profit_floor_validated,
stop_grace_closed_rugs_are_fast) found recoverers look like dying coins ON PRICE. This
looks under the surface: what was true AT THE MOMENT OF THE STOP — the market cap the coin
fell to, how it fell (fast gap or slow bleed, from what peak, how long after entry), what
the token is made of (liquidity vs size, holders, bundles, snipers, age, score), and, where
wallet_holdings was running, whether a followed wallet bought in the 30 min after.

WHAT IS SCORED. For every qsim hard stop on solwhaletrending, the alternative "do not sell
at the stop; hold, sell at the first look at or below HOLD_FLOOR of entry, or at the first
look at or above 2x (credited at most 2.1x), else at the last look within 24 h". The
'hold gain' column is that result minus what the stop actually got, in positions. A
feature value is interesting only if its hold gain is positive in BOTH halves of the window
while the stops overall lose by holding.

LIMITS. After a stop below 1.0x qsim looks once a minute for 30 min, then hourly: a fall
through HOLD_FLOOR between hourly looks is booked late (lower), a short spike above 2x can
be missed. Both make holding look worse than it would be, never better. With ~15 features
x ~5 values, a few will pass both halves by chance: believe one only if its neighbours
point the same way and it holds on stops after the day it was found.

Read-only. Run on the VPS:  python3 stop_recovery_scan.py
    [--since 2026-10-07] [--hold-floor 0.5] [--lane all|none|low_score] [--min-n 15]
"""

from __future__ import annotations

import argparse
from collections import defaultdict

import db

HORIZON_S = 24 * 3600
BANK = 2.0
BANK_CAP = 2.1


def bucket(v, edges):
    if v is None:
        return None
    v = float(v)
    for hi, label in edges:
        if v < hi:
            return label
    return edges[-1][1]


FEATURES = {
    "mcap at stop": ("mcap_at_stop", [(40e3, "under 40K"), (60e3, "40-60K"), (80e3, "60-80K"),
                                      (100e3, "80-100K"), (150e3, "100-150K"), (1e18, "150K+")]),
    "entry mcap": ("entry_mcap", [(80e3, "under 80K"), (100e3, "80-100K"), (130e3, "100-130K"),
                                  (1e18, "130K+")]),
    "peak before stop": ("peak_x", [(1.05, "under 1.05x"), (1.2, "1.05-1.20x"), (1.5, "1.20-1.50x"),
                                    (1e9, "1.50x+")]),
    "mins to stop": ("mins_to_stop", [(2, "under 2"), (5, "2-5"), (15, "5-15"), (60, "15-60"),
                                      (1e9, "60+")]),
    "mins peak->stop": ("mins_fall", [(1, "under 1"), (3, "1-3"), (10, "3-10"), (1e9, "10+")]),
    "stop fill": ("stop_x", [(0.65, "under 0.65 (gap)"), (0.75, "0.65-0.75"), (1e9, "0.75+")]),
    "liq / call mcap": ("liq_ratio", [(0.15, "under 0.15"), (0.20, "0.15-0.20"), (0.25, "0.20-0.25"),
                                      (1e9, "0.25+")]),
    "liquidity": ("liq", [(20e3, "under 20K"), (25e3, "20-25K"), (30e3, "25-30K"), (1e18, "30K+")]),
    "holders": ("holders", [(150, "under 150"), (300, "150-300"), (500, "300-500"), (700, "500-700"),
                            (1e18, "700+")]),
    "bundles left %": ("bundles", [(1, "under 1%"), (5, "1-5%"), (15, "5-15%"), (1e9, "15%+")]),
    "snipers": ("snipers", [(1, "0"), (3, "1-2"), (6, "3-5"), (1e9, "6+")]),
    "token age": ("age", [(5, "under 5 min"), (10, "5-10 min"), (30, "10-30 min"), (120, "30 min-2 h"),
                          (1e18, "2 h+")]),
    "score": ("score", [(63, "under 63"), (70, "63-69"), (80, "70-79"), (1e9, "80+")]),
    "hour UTC": ("hour", [(6, "00-05"), (12, "06-11"), (18, "12-17"), (24, "18-23")]),
}
CATEGORICAL = {"lane": "lane", "security flag": "flag", "followed wallet buy <30m": "wallet_buy"}


def load(since: str, lane: str) -> list[dict]:
    conn = db.get_conn()
    db.safe_rollback()
    lane_sql = {"all": "", "none": "AND c.skip_reason IS NULL",
                "low_score": "AND c.skip_reason = 'low_score'"}[lane]
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT qp.call_id, left(t.symbol, 12), qp.entry_time, qp.exit_time,
                   qp.sol_out / qp.sol_in, qp.entry_price, c.mcap_at_call,
                   COALESCE(c.skip_reason, 'none'), COALESCE(t.security_flag, '-'),
                   c.conviction_score, t.hodl_count, t.liq_at_detection,
                   t.bundle_pct_remaining, t.sniper_count, t.token_age_minutes
            FROM qsim_positions qp
            JOIN calls c  ON c.id = qp.call_id
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.status = 'closed' AND qp.channel_handle = 'solwhaletrending'
              AND qp.exit_reason = 'hard_stop' AND qp.sol_in > 0
              AND qp.entry_time >= %s AND qp.exit_time < now() - interval '24 hours'
              {lane_sql}
            ORDER BY qp.entry_time
            """, (since,))
        pos = cur.fetchall()
        ids = [p[0] for p in pos]
        if not ids:
            return []
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
        buys = _wallet_buys(cur, ids)
    out = []
    for (cid, sym, ent, ext, stop_x, entry_mcap, call_mcap, lane_lbl, flag, score, holders,
         liq, bundles, snipers, age) in pos:
        path = obs.get(cid, [])
        before = [(at, m) for at, m in path if ent <= at <= ext and m is not None]
        after = [(at, m) for at, m in path if ext < at <= ext + _td(HORIZON_S)]
        if not before or not after:
            continue
        peak_at, peak_x = max(before, key=lambda z: z[1])
        stop_x = float(stop_x or 0)
        wb = None
        if buys is not None:
            first_seen, per_call = buys
            if first_seen is not None and ext >= first_seen:
                wb = "yes" if any(ext <= at <= ext + _td(1800) for at in per_call.get(cid, [])) else "no"
        out.append({
            "call_id": cid, "sym": sym, "entry": ent, "exit": ext, "stop_x": stop_x,
            "after": after,
            "mcap_at_stop": None if entry_mcap is None else float(entry_mcap) * stop_x,
            "entry_mcap": entry_mcap, "peak_x": peak_x,
            "mins_to_stop": (ext - ent).total_seconds() / 60,
            "mins_fall": (ext - peak_at).total_seconds() / 60,
            "liq_ratio": (float(liq) / float(call_mcap)) if liq and call_mcap else None,
            "liq": liq, "holders": holders, "bundles": bundles, "snipers": snipers, "age": age,
            "score": score, "hour": ent.hour, "lane": lane_lbl, "flag": flag, "wallet_buy": wb,
        })
    return out


def _td(secs):
    from datetime import timedelta
    return timedelta(seconds=secs)


def _wallet_buys(cur, ids):
    """(first observation time in tracked_wallet_holdings, {call_id: [buy times]}) or None
    if the table does not exist. A buy = a balance that went up between two looks."""
    try:
        cur.execute("SELECT min(observed_at) FROM tracked_wallet_holdings")
        first = cur.fetchone()[0]
        cur.execute(
            """
            SELECT call_id, observed_at FROM tracked_wallet_holdings
            WHERE call_id = ANY(%s) AND prev_balance IS NOT NULL AND balance > prev_balance
            """, (ids,))
        per = defaultdict(list)
        for cid, at in cur.fetchall():
            per[cid].append(at)
        return first, per
    except Exception:
        db.safe_rollback()
        return None


def hold_result(s: dict, floor: float) -> tuple[float, bool]:
    """(x of entry if we had held from the stop, reached 2x within 24h)."""
    reached = any(m is not None and m >= BANK for _, m in s["after"])
    for _, m in s["after"]:
        if m is None:
            continue
        if m <= floor:
            return m, reached
        if m >= BANK:
            return min(m, BANK_CAP), reached
    last = s["after"][-1][1]
    return (0.0 if last is None else last), reached


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-10-07")
    ap.add_argument("--hold-floor", type=float, default=0.5)
    ap.add_argument("--lane", default="all", choices=["all", "none", "low_score"])
    ap.add_argument("--min-n", type=int, default=15)
    a = ap.parse_args()
    stops = load(a.since, a.lane)
    if not stops:
        print("no hard stops found")
        return
    for s in stops:
        s["hold_x"], s["rec"] = hold_result(s, a.hold_floor)
        s["gain"] = s["hold_x"] - s["stop_x"]
    mid = stops[len(stops) // 2]["entry"]

    def summ(g):
        n = len(g)
        rec = sum(1 for s in g if s["rec"])
        gain = sum(s["gain"] for s in g)
        g1 = sum(s["gain"] for s in g if s["entry"] < mid)
        g2 = sum(s["gain"] for s in g if s["entry"] >= mid)
        n1 = sum(1 for s in g if s["entry"] < mid)
        return n, rec, gain, g1, g2, n1, n - n1

    n, rec, gain, g1, g2, n1, n2 = summ(stops)
    r1, r2 = g1 / max(n1, 1), g2 / max(n2, 1)
    print(f"solwhaletrending qsim hard stops since {a.since} (lane {a.lane}): {n}")
    print(f"reached 2x of entry within 24h after the stop: {rec} ({rec / n * 100:.0f}%)")
    print(f"holding instead (sell <= {a.hold_floor:g}x, bank at 2x, else 24h): gain {gain:+.2f} positions "
          f"({gain / n * 100:+.1f}% per stop)  1st half {g1:+.2f}  2nd half {g2:+.2f}")
    print(f"halves split at {mid:%m-%d %H:%M}\n")
    print(f"  {'feature':24s} {'value':18s} {'stops':>5s} {'came back':>9s} {'hold gain':>9s} "
          f"{'per stop':>8s} {'1st half':>8s} {'2nd half':>8s}")
    rows = []
    feats = [(name, key, edges) for name, (key, edges) in FEATURES.items()]
    feats += [(name, key, None) for name, key in CATEGORICAL.items()]
    for name, key, edges in feats:
        groups = defaultdict(list)
        for s in stops:
            v = bucket(s[key], edges) if edges else s[key]
            if v is not None:
                groups[v].append(s)
        order = [lbl for _, lbl in edges] if edges else sorted(groups)
        for val in order:
            g = groups.get(val, [])
            if len(g) < a.min_n:
                continue
            gn, grec, ggain, gg1, gg2, gn1, gn2 = summ(g)
            if not gn1 or not gn2:
                continue
            tag = ""
            if gg1 > 0 and gg2 > 0 and gg1 / gn1 > r1 and gg2 / gn2 > r2:
                tag = "  <- holding wins both halves"
            elif gg1 / gn1 < min(r1, 0) and gg2 / gn2 < min(r2, 0):
                tag = "  <- sell fast here"
            rows.append((name, val, gn, grec, ggain, gg1, gg2, tag))
            print(f"  {name:24s} {str(val):18s} {gn:5d} {grec / gn * 100:8.0f}% {ggain:+9.2f} "
                  f"{ggain / gn * 100:+7.1f}% {gg1:+8.2f} {gg2:+8.2f}{tag}")
    print("\n'came back' = reached 2x of our entry within 24h after the stop. 'hold gain' = what holding "
          "(sell at the hold floor, bank at 2x) would have added vs the stop, in positions.\n"
          "A row is tagged 'holding wins' when holding is positive AND beats the all-stops rate in BOTH\n"
          "halves; 'sell fast' when it is below both the all-stops rate and zero in both halves.\n"
          "Believe a tag only if neighbouring values lean the same way.")
    good = [r for r in rows if "both halves" in r[7]]
    if good:
        print("\ncoins behind each 'holding wins both halves' row (largest gains first):")
        for name, val, *_ in good:
            key, edges = (FEATURES[name] if name in FEATURES else (CATEGORICAL[name], None))
            g = [s for s in stops if (bucket(s[key], edges) if edges else s[key]) == val]
            top = sorted(g, key=lambda s: -s["gain"])[:4]
            print(f"  {name} {val}: " + ", ".join(f"{s['sym']} {s['stop_x']:.2f}->{s['hold_x']:.2f}" for s in top))


if __name__ == "__main__":
    main()
