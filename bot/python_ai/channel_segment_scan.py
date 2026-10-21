"""
channel_segment_scan.py — which kinds of call are better or worse than their channel?

WHY (2026-10-20). Four channels feed the bot (solhousesignal, solhousesignal_vip,
solwhaletrending, solearlytrending). Only solwhaletrending is traded; the filter that
made its new lane work (holders < 700, score >= 70) was found by splitting its calls by
what the bot records about them. This does the same for every channel, to find segments
worth trading (and segments worth cutting).

RULER. Shadow (shadow_positions, 'early' exit variant) is the only record that covers
every call on every channel. It is priced off the feed and OVERSTATES results — by about
40 points on solwhaletrending (vs qsim, 78 paired coins, 2026-10-12), more on volatile
lanes such as the VIP channel. So nothing here is compared in absolute terms: each
segment is compared with ITS OWN CHANNEL'S average, and only segments that beat (or trail)
that average in BOTH halves of the window are listed. A second, feed-independent column
is "lost half": the share of trades that sold below 0.5x, which reads the same on any ruler.

Shadow nominates; qsim confirms. A segment found here is a candidate for a qsim lane, not
a trade.

Run on the VPS:  python3 channel_segment_scan.py   [--days 30] [--min-trades 40]
"""

from __future__ import annotations

import argparse
from collections import defaultdict

import db


def bucket(name: str, v):
    if v is None:
        return None
    v = float(v)
    edges = {
        "call mcap": [(5e3, "under 5K"), (10e3, "5-10K"), (20e3, "10-20K"), (40e3, "20-40K"),
                      (80e3, "40-80K"), (120e3, "80-120K"), (250e3, "120-250K"), (1e18, "250K+")],
        "score": [(40, "under 40"), (55, "40-54"), (63, "55-62"), (70, "63-69"),
                  (85, "70-84"), (1e9, "85+")],
        "token age": [(5, "under 5 min"), (10, "5-10 min"), (30, "10-30 min"), (120, "30 min-2 h"),
                      (1440, "2-24 h"), (1e18, "over a day")],
        "holders": [(100, "under 100"), (300, "100-300"), (500, "300-500"), (700, "500-700"),
                    (1500, "700-1500"), (1e18, "1500+")],
        "liquidity": [(10e3, "under 10K"), (20e3, "10-20K"), (30e3, "20-30K"), (60e3, "30-60K"),
                      (1e18, "60K+")],
        "bundles left %": [(5, "under 5%"), (15, "5-15%"), (30, "15-30%"), (1e9, "30%+")],
        "snipers": [(1, "0"), (5, "1-4"), (15, "5-14"), (1e9, "15+")],
        "hour UTC": [(6, "00-05"), (12, "06-11"), (18, "12-17"), (24, "18-23")],
    }[name]
    for hi, label in edges:
        if v < hi:
            return label
    return edges[-1][1]


# Each shadow trade is capped at CAP (default 2.1x): live sells at the 2x bank, so a trade
# cannot really make more, and on tiny coins the feed reports freak spikes (2026-10-20 scan:
# "under 5K" +2,554% per trade on the VIP channel) that swamp every average. --no-cap turns
# it off.
CAP: float | None = 2.1

FEATURES = ["security flag", "message type", "vip tier", "lane label", "call mcap", "score",
            "token age", "holders", "liquidity", "bundles left %", "snipers", "hour UTC"]


def load(days: float) -> list[dict]:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ltrim(ch.handle, '@'), sp.entry_time, sp.sol_out / NULLIF(sp.sol_in, 0),
                   COALESCE(t.security_flag, '(none)'), COALESCE(c.message_type, '(none)'),
                   COALESCE(c.vip_tier, '(none)'), COALESCE(c.skip_reason, '(unlabelled)'),
                   c.mcap_at_call, c.conviction_score, t.token_age_minutes, t.hodl_count,
                   t.liq_at_detection, t.bundle_pct_remaining, t.sniper_count
            FROM shadow_positions sp
            JOIN calls c ON c.id = sp.call_id
            JOIN tokens t ON t.id = sp.token_id
            JOIN channels ch ON ch.id = c.channel_id
            WHERE sp.status = 'closed' AND sp.exit_variant = 'early'
              AND sp.entry_time >= now() - (%s || ' days')::interval
              AND sp.sol_in > 0 AND sp.sol_out IS NOT NULL
            ORDER BY sp.entry_time
            """, (days,))
        rows = cur.fetchall()
    out = []
    for (ch, ent, x, flag, mtype, tier, lane, mcap, score, age, hodl, liq, bpr, snip) in rows:
        x = float(x or 0)
        if CAP is not None:
            x = min(x, CAP)
        out.append({
            "channel": ch, "entry": ent, "x": x,
            "security flag": flag, "message type": mtype, "vip tier": tier, "lane label": lane,
            "call mcap": bucket("call mcap", mcap), "score": bucket("score", score),
            "token age": bucket("token age", age), "holders": bucket("holders", hodl),
            "liquidity": bucket("liquidity", liq), "bundles left %": bucket("bundles left %", bpr),
            "snipers": bucket("snipers", snip), "hour UTC": bucket("hour UTC", ent.hour),
        })
    return out


def stats(rows: list[dict]) -> tuple[int, float, float]:
    n = len(rows)
    if not n:
        return 0, 0.0, 0.0
    return n, sum(r["x"] - 1 for r in rows) / n * 100, sum(1 for r in rows if r["x"] < 0.5) / n * 100


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=float, default=30)
    ap.add_argument("--min-trades", type=int, default=40)
    ap.add_argument("--min-gap", type=float, default=5.0,
                    help="points per trade a segment must beat (or trail) its channel by in BOTH halves")
    ap.add_argument("--no-cap", action="store_true", help="use shadow's raw result (freak spikes included)")
    a = ap.parse_args()
    global CAP
    if a.no_cap:
        CAP = None
    print("shadow results " + (f"capped at {CAP:g}x per trade (like the 2x bank)" if CAP else "UNCAPPED"))
    rows = load(a.days)
    by_ch = defaultdict(list)
    for r in rows:
        by_ch[r["channel"]].append(r)
    for ch, rs in sorted(by_ch.items(), key=lambda kv: -len(kv[1])):
        rs.sort(key=lambda r: r["entry"])
        mid = rs[len(rs) // 2]["entry"]
        h1 = [r for r in rs if r["entry"] < mid]
        h2 = [r for r in rs if r["entry"] >= mid]
        n, avg, lost = stats(rs)
        _, a1, _ = stats(h1)
        _, a2, _ = stats(h2)
        print(f"\n=== {ch}: {n} shadow trades in {a.days:g} days — channel average {avg:+.1f}% per trade "
              f"(1st half {a1:+.1f}%, 2nd half {a2:+.1f}%), lost half {lost:.0f}% ===")
        good, bad = [], []
        for feat in FEATURES:
            groups = defaultdict(list)
            for r in rs:
                if r[feat] is not None:
                    groups[r[feat]].append(r)
            for val, g in groups.items():
                if len(g) < a.min_trades or len(g) > 0.9 * n:
                    continue
                g1 = [r for r in g if r["entry"] < mid]
                g2 = [r for r in g if r["entry"] >= mid]
                if len(g1) < a.min_trades // 4 or len(g2) < a.min_trades // 4:
                    continue
                gn, gavg, glost = stats(g)
                _, ga1, _ = stats(g1)
                _, ga2, _ = stats(g2)
                d1, d2 = ga1 - a1, ga2 - a2
                row = (feat, val, gn, gavg, gavg - avg, d1, d2, glost, glost - lost)
                if d1 >= a.min_gap and d2 >= a.min_gap:
                    good.append(row)
                elif d1 <= -a.min_gap and d2 <= -a.min_gap:
                    bad.append(row)
        hdr = (f"  {'feature':15s} {'value':16s} {'trades':>6s} {'per trade':>9s} {'vs channel':>10s} "
               f"{'1st half':>9s} {'2nd half':>9s} {'lost half':>9s} {'vs channel':>10s}")
        for title, lst, key in (("BETTER than the channel in both halves (candidates to trade):", good, -1),
                                ("WORSE than the channel in both halves (candidates to cut):", bad, 1)):
            print(f"\n {title}")
            if not lst:
                print("  (none)")
                continue
            print(hdr)
            for f, v, gn, gavg, d, d1, d2, gl, dl in sorted(lst, key=lambda z: key * z[4]):
                print(f"  {f:15s} {str(v):16s} {gn:6d} {gavg:+8.1f}% {d:+9.1f} {d1:+9.1f} {d2:+9.1f} "
                      f"{gl:8.0f}% {dl:+9.0f}")
    print("\nShadow overstates every result, so read 'vs channel', not 'per trade'. A segment is a "
          "candidate only; it needs qsim (real quotes) before money goes near it.")


if __name__ == "__main__":
    main()
