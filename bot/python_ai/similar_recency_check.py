"""
similar_recency_check.py — does the similar-coins evidence go stale? Pure DB, no brain, no API calls.

THE QUESTION
------------
v7 is shown the 8 closed coins with the nearest setup from the channel's last 7 days and a verdict
("better" / "about normal" / "worse") against the channel's bank and rug rates. On solearly the
"better" coins made +30% on Nov 1-10, then -31% on Nov 11-13. The proposed fix is to also show how
the lookalikes did in the LAST 3 DAYS. Before spending brain credit on that, check the idea on the data:

    for every coin, recompute — as of its entry — the 7-day lookalikes (what v7 sees today) and the
    3-day lookalikes (the proposed extra). Does the 3-day view predict the coin's outcome better, and
    does it split the "better" coins into ones that still bank and ones that don't?

Judged per period, so one episode can't carry it: the idea passes only if it helps in MOST periods,
on both channels, not just Nov 11-15 (the week that suggested it).

NO LOOKAHEAD
------------
A lookalike counts only if it entered in the window before the coin AND had closed before the coin
entered — the same rule as trader_brain._candidates(as_of). The distance is the same as
trader_brain.similar_coins (same stats, same scales, K = SIMILAR_K).

USAGE
-----
    python3 similar_recency_check.py                                  # both channels, Oct 1 -> now
    python3 similar_recency_check.py --channel solearlytrending --since 2026-10-15
    python3 similar_recency_check.py --periods 2026-10-01,2026-10-16,2026-11-01,2026-11-11
"""
import argparse
import math
import sys
from bisect import bisect_left
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import db
from trader_brain import SIMILAR_K

SCALE = {"bundle_launch": 30.0, "bundle_left": 8.0, "dev_bundled": 15.0, "dev_bundle_sold": 15.0,
         "snipers": 15.0, "fake_vol": 5.0}
LONG, SHORT = timedelta(days=7), timedelta(days=3)
MIN_SHORT = 5          # fewer 3-day lookalikes than this -> no 3-day view for the coin


def pac(day: str) -> datetime:
    """Pacific midnight of a YYYY-MM-DD, in UTC."""
    return datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=ZoneInfo("America/Los_Angeles")).astimezone(timezone.utc)


def load(channel: str, since: datetime) -> list[dict]:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT qp.call_id, t.mint_address, qp.entry_time, qp.exit_time, qp.exit_reason,
                   qp.sol_out / nullif(qp.sol_in, 0),
                   t.bundle_pct_initial, t.bundle_pct_remaining, t.bundled_pct, t.bundled_sold_pct,
                   t.sniper_count, t.liq_at_detection, t.fake_vol_pct
            FROM qsim_positions qp JOIN tokens t ON t.id = qp.token_id
            WHERE qp.channel_handle = %s AND qp.status = 'closed' AND qp.sol_in > 0
              AND qp.entry_time >= %s AND qp.exit_time IS NOT NULL
            ORDER BY qp.entry_time""", (channel, since - LONG))
        rows = cur.fetchall()
    f = lambda v: float(v) if v is not None else None
    out = []
    for cid, mint, et, xt, reason, x, b0, b1, devb, devs, sn, liq, fake in rows:
        x = float(x or 0)
        out.append({"call_id": cid, "mint": mint, "entry": et, "exit": xt, "x": x,
                    "bank": "bank" in (reason or ""), "rug": x < 0.30,
                    "bundle_launch": f(b0), "bundle_left": f(b1), "dev_bundled": f(devb),
                    "dev_bundle_sold": f(devs), "snipers": f(sn), "liq": f(liq), "fake_vol": f(fake)})
    return out


def dist(me: dict, c: dict) -> float | None:
    d, n = 0.0, 0
    for k, sc in SCALE.items():
        if me[k] is not None and c[k] is not None:
            d += abs(me[k] - c[k]) / sc
            n += 1
    if me["liq"] and c["liq"] and me["liq"] > 0 and c["liq"] > 0:
        d += abs(math.log(me["liq"] / c["liq"])) / 0.5
        n += 1
    return d / n if n >= 3 else None


def verdict(bank_pct: float, rug_pct: float, cb: float, cr: float) -> str:
    """Same rule as trader_brain.add_similar."""
    if rug_pct >= cr + 25 or (bank_pct <= cb - 15 and rug_pct >= cr + 10):
        return "worse"
    if bank_pct >= cb + 20 and rug_pct <= cr + 5:
        return "better"
    return "about normal"


def score(rows: list[dict], since: datetime) -> list[dict]:
    """For each coin entered since `since`: its 7-day and 3-day lookalike stats as of its entry."""
    entries = [r["entry"] for r in rows]
    out = []
    for i, me in enumerate(rows):
        if me["entry"] < since:
            continue
        if sum(me[k] is not None for k in ("bundle_launch", "bundle_left", "dev_bundled", "dev_bundle_sold",
                                           "snipers", "liq", "fake_vol")) < 3:
            continue
        t = me["entry"]
        lo = bisect_left(entries, t - LONG)
        pool = [c for c in rows[lo:i] if c["exit"] < t and c["mint"] != me["mint"]]
        if len(pool) < 30:
            continue
        short_t = t - SHORT
        cb_l = 100 * sum(c["bank"] for c in pool) / len(pool)
        cr_l = 100 * sum(c["rug"] for c in pool) / len(pool)
        spool = [c for c in pool if c["entry"] >= short_t]
        scored = [(d, c) for c in pool if (d := dist(me, c)) is not None]
        if len(scored) < SIMILAR_K:
            continue
        scored.sort(key=lambda z: z[0])
        near = [c for _, c in scored[:SIMILAR_K]]
        bl = 100 * sum(c["bank"] for c in near) / len(near)
        rl = 100 * sum(c["rug"] for c in near) / len(near)
        rec = {"coin": me, "cb_long": cb_l, "cr_long": cr_l, "sim_bank_long": bl, "sim_rug_long": rl,
               "v_long": verdict(bl, rl, cb_l, cr_l), "sim_bank_short": None, "v_short": None}
        snear = [c for d, c in scored if c["entry"] >= short_t][:SIMILAR_K]
        if len(snear) >= MIN_SHORT and len(spool) >= 15:
            cb_s = 100 * sum(c["bank"] for c in spool) / len(spool)
            cr_s = 100 * sum(c["rug"] for c in spool) / len(spool)
            bs = 100 * sum(c["bank"] for c in snear) / len(snear)
            rs = 100 * sum(c["rug"] for c in snear) / len(snear)
            rec.update(sim_bank_short=bs, sim_rug_short=rs, cb_short=cb_s, v_short=verdict(bs, rs, cb_s, cr_s))
        out.append(rec)
    return out


def auc(xs: list[float], ys: list[bool]) -> float | None:
    """P(score of a random bank > score of a random non-bank), ties half. 0.5 = no signal."""
    pos = [x for x, y in zip(xs, ys) if y]
    neg = [x for x, y in zip(xs, ys) if not y]
    if len(pos) < 5 or len(neg) < 5:
        return None
    allv = sorted((x, y) for x, y in zip(xs, ys))
    # rank-sum with tie averaging
    ranks, i = {}, 0
    vals = [v for v, _ in allv]
    while i < len(vals):
        j = i
        while j < len(vals) and vals[j] == vals[i]:
            j += 1
        ranks[vals[i]] = (i + j + 1) / 2
        i = j
    rs = sum(ranks[x] for x in pos)
    return (rs - len(pos) * (len(pos) + 1) / 2) / (len(pos) * len(neg))


def line(label: str, recs: list[dict], width: int = 34) -> str:
    n = len(recs)
    if not n:
        return f"    {label:<{width}} {0:>4}"
    b = sum(r["coin"]["bank"] for r in recs)
    rg = sum(r["coin"]["rug"] for r in recs)
    pnl = 100 * (sum(r["coin"]["x"] for r in recs) / n - 1)
    return f"    {label:<{width}} {n:>4}  banked {b:>3} ({100 * b / n:>3.0f}%)  rugs {rg:>3}  P&L {pnl:>+6.1f}%"


def report(channel: str, recs: list[dict], periods: list[tuple[str, datetime, datetime]]) -> None:
    print(f"\n=== {channel}: {len(recs)} coins scored "
          f"({sum(r['v_short'] is not None for r in recs)} with a 3-day view) ===")
    wins = {"auc": 0, "split": 0, "periods": 0}
    for name, a, b in periods + [("ALL", datetime.min.replace(tzinfo=timezone.utc), datetime.max.replace(tzinfo=timezone.utc))]:
        P = [r for r in recs if a <= r["coin"]["entry"] < b]
        if len(P) < 40:
            print(f"\n  {name}: {len(P)} coins — too few, skipped")
            continue
        both = [r for r in P if r["v_short"] is not None]
        y = [r["coin"]["bank"] for r in both]
        a_l = auc([r["sim_bank_long"] for r in both], y)
        a_s = auc([r["sim_bank_short"] for r in both], y)
        print(f"\n  {name}")
        print(line("all coins", P))
        fmt = lambda v: "  n/a" if v is None else f"{v:.3f}"
        print(f"    predicts the bank (AUC, 0.5 = nothing), same {len(both)} coins:  "
              f"7-day lookalikes {fmt(a_l)}   3-day lookalikes {fmt(a_s)}")
        for v in ("better", "about normal", "worse"):
            print(line(f"7-day verdict {v}", [r for r in P if r["v_long"] == v]))
        better = [r for r in both if r["v_long"] == "better"]
        still = [r for r in better if r["sim_bank_short"] >= r["cb_long"]]
        faded = [r for r in better if r["sim_bank_short"] < r["cb_long"]]
        print(line("  'better', 3-day lookalikes still bank", still, 34))
        print(line("  'better', 3-day lookalikes faded", faded, 34))
        if name != "ALL":
            wins["periods"] += 1
            if a_l is not None and a_s is not None and a_s > a_l:
                wins["auc"] += 1
            if len(still) >= 5 and len(faded) >= 5:
                ps = sum(r["coin"]["x"] for r in still) / len(still)
                pf = sum(r["coin"]["x"] for r in faded) / len(faded)
                if ps > pf:
                    wins["split"] += 1
    print(f"\n  SCORECARD {channel}: 3-day view out-predicts 7-day in {wins['auc']}/{wins['periods']} periods; "
          f"'still bank' beats 'faded' in {wins['split']}/{wins['periods']} periods (needs >=5 coins each side)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--channel", action="append", help="default: solearlytrending and solwhaletrending")
    ap.add_argument("--since", default="2026-10-01", help="Pacific day")
    ap.add_argument("--periods", default="2026-10-01,2026-10-16,2026-11-01,2026-11-11",
                    help="Pacific day boundaries; the last runs to now")
    args = ap.parse_args()
    chans = args.channel or ["solearlytrending", "solwhaletrending"]
    since = pac(args.since)
    bounds = [pac(d) for d in args.periods.split(",")]
    periods = []
    for k, a in enumerate(bounds):
        b = bounds[k + 1] if k + 1 < len(bounds) else datetime.max.replace(tzinfo=timezone.utc)
        end = args.periods.split(",")[k + 1] if k + 1 < len(bounds) else "now"
        periods.append((f"{args.periods.split(',')[k]} .. {end}", a, b))
    print(f"similar_recency_check: lookalikes K={SIMILAR_K}, long window 7 days, short 3 days "
          f"(>= {MIN_SHORT} short lookalikes needed). No API calls.")
    for ch in chans:
        rows = load(ch, since)
        if not rows:
            print(f"\n=== {ch}: no closed qsim coins ===")
            continue
        report(ch, score(rows, since), periods)
    print("\nRead: the 3-day idea is worth a brain replay only if it out-predicts the 7-day view, and/or"
          "\n'still bank' beats 'faded', in MOST periods on BOTH channels — not just Nov 11+ (the week that"
          "\nsuggested it). If not, the cheap fix is to drop the similar-coins block on solearly.")


if __name__ == "__main__":
    sys.exit(main())
