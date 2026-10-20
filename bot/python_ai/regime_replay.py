"""
regime_replay.py — trade only when solwhaletrending is "hot"?

WHY (2026-10-20). From Oct 17 the share of solwhaletrending coins reaching the 2x bank fell
from ~20% to 5-10% across every flag, size and lane, and the new-lane strategy went from
+1.4..+3.4 positions a day to flat or negative. Over five days the PREVIOUS day's channel
bank rate lined up with the strategy's result. That is a different kind of rule from the
entry filters and exit rules: stand aside (or trade small) when the whole channel is cold.

HOW. qsim trades every solwhaletrending call, so its closed positions are a real-time
thermometer. For each strategy trade, the channel's temperature is measured from qsim
positions that CLOSED in the WINDOW hours before that trade's ENTRY (never after, so no
look-ahead): the share that banked. A rule "skip when the last W hours banked under T%"
is then scored on the strategy's trades, with results at live-like prices (banks capped
at 2.1x). Fewer than MIN_SAMPLE closes in the window = temperature unknown = trade.

Two strategies are scored separately:
  new lane   unlabelled calls, safe, 80-120k, under 700 holders, score 70+ (live's plan)
  old lane   low_score calls, safe, 80-120k (the lane live traded until 2026-10-15)
The old lane has data back to Sep 23, so a temperature rule can be checked on weeks, not
days, there. Data before Oct 7 had rate-limited qsim exits; the bank FLAG is still sound.

Read-only. Run on the VPS:  python3 regime_replay.py   [--since 2026-09-23]
"""

from __future__ import annotations

import argparse
import bisect
from datetime import timedelta

import db

BANK_CAP = 2.1
MIN_SAMPLE = 10


def load(since: str) -> list[dict]:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT qp.call_id, qp.entry_time, qp.exit_time, qp.exit_reason,
                   qp.sol_out / qp.sol_in, c.skip_reason, COALESCE(t.security_flag, '-'),
                   c.mcap_at_call, t.hodl_count, c.conviction_score
            FROM qsim_positions qp
            JOIN calls c ON c.id = qp.call_id
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.status = 'closed' AND qp.channel_handle = 'solwhaletrending'
              AND qp.entry_time >= %s::timestamptz - interval '2 days'
              AND qp.exit_time IS NOT NULL AND qp.sol_in > 0
            ORDER BY qp.entry_time
            """, (since,))
        rows = cur.fetchall()
    out = []
    for cid, ent, ext, reason, x, lane, flag, mcap, holders, score in rows:
        bank = "bank" in (reason or "")
        x = float(x or 0)
        out.append({
            "call_id": cid, "entry": ent, "exit": ext, "bank": bank,
            "x": min(x, BANK_CAP) if bank else x,
            "lane": lane, "flag": flag,
            "in_band": mcap is not None and 80000 <= float(mcap) <= 120000,
            "holders": holders, "score": None if score is None else float(score),
        })
    return out


def strategies(rows: list[dict], since) -> dict[str, list[dict]]:
    base = [r for r in rows if r["entry"] >= since and r["flag"] == "safe" and r["in_band"]]
    new = [r for r in base if r["lane"] is None
           and (r["holders"] is None or r["holders"] < 700)
           and (r["score"] is None or r["score"] >= 70)]
    old = [r for r in base if r["lane"] == "low_score"]
    return {"new lane (filtered, live's plan)": new, "old lane (low_score)": old}


class Thermometer:
    """Bank share of qsim closes in a window before a moment, from sorted exit times."""

    def __init__(self, rows: list[dict]):
        s = sorted(rows, key=lambda r: r["exit"])
        self.times = [r["exit"] for r in s]
        self.cum = [0]
        for r in s:
            self.cum.append(self.cum[-1] + (1 if r["bank"] else 0))

    def at(self, moment, hours: float):
        lo = bisect.bisect_left(self.times, moment - timedelta(hours=hours))
        hi = bisect.bisect_left(self.times, moment)          # strictly before entry
        n = hi - lo
        if n < MIN_SAMPLE:
            return None, n
        return (self.cum[hi] - self.cum[lo]) / n, n


def score(trades: list[dict], keep) -> dict:
    kept = [t for t in trades if keep(t)]
    skipped = [t for t in trades if not keep(t)]
    half = len(trades) // 2
    first = {id(t) for t in trades[:half]}
    net = sum(t["x"] - 1 for t in kept)
    return {
        "kept": len(kept), "net": net,
        "per": (net / len(kept) * 100) if kept else 0.0,
        "skipped": len(skipped), "skipped_net": sum(t["x"] - 1 for t in skipped),
        "h1": sum(t["x"] - 1 for t in kept if id(t) in first),
        "h2": sum(t["x"] - 1 for t in kept if id(t) not in first),
        "banks": sum(1 for t in kept if t["bank"]),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-09-23")
    a = ap.parse_args()
    rows = load(a.since)
    if not rows:
        print("no qsim positions found")
        return
    since = next((r["entry"] for r in rows if r["entry"].strftime("%Y-%m-%d") >= a.since), rows[-1]["entry"])
    thermo = Thermometer(rows)
    for name, trades in strategies(rows, since).items():
        trades.sort(key=lambda t: t["entry"])
        print(f"\n=== {name}: {len(trades)} trades since {a.since} "
              f"(temperature = share of ALL solwhaletrending qsim closes that banked) ===")
        if not trades:
            continue
        for hours in (12, 24):
            for t in trades:
                t[f"temp{hours}"], t[f"n{hours}"] = thermo.at(t["entry"], hours)
        base = score(trades, lambda t: True)
        print(f"{'rule':44s} {'trades':>6s} {'banks':>5s} {'net':>7s} {'per tr':>7s} "
              f"{'vs all':>7s} {'1st half':>8s} {'2nd half':>8s} {'skipped':>7s} {'skip net':>8s}")
        print(f"{'trade every call (now)':44s} {base['kept']:6d} {base['banks']:5d} {base['net']:+7.2f} "
              f"{base['per']:+6.1f}% {0:+7.2f} {base['h1']:+8.2f} {base['h2']:+8.2f} {0:7d} {0:+8.2f}")
        for hours in (12, 24):
            for th in (0.08, 0.10, 0.12, 0.15, 0.20):
                key = f"temp{hours}"
                s = score(trades, lambda t, k=key, th=th: t[k] is None or t[k] >= th)
                print(f"{f'skip if last {hours}h banked under {int(th * 100)}%':44s} {s['kept']:6d} "
                      f"{s['banks']:5d} {s['net']:+7.2f} {s['per']:+6.1f}% {s['net'] - base['net']:+7.2f} "
                      f"{s['h1']:+8.2f} {s['h2']:+8.2f} {s['skipped']:7d} {s['skipped_net']:+8.2f}")
        known = [t for t in trades if t["temp24"] is not None]
        if known:
            print("\nby temperature (last 24h), trades and result:")
            for lo, hi in ((0, 0.08), (0.08, 0.12), (0.12, 0.16), (0.16, 0.20), (0.20, 1.01)):
                g = [t for t in known if lo <= t["temp24"] < hi]
                if g:
                    net = sum(t["x"] - 1 for t in g)
                    print(f"  {int(lo * 100):2d}-{int(min(hi, 1) * 100):3d}%   {len(g):4d} trades   "
                          f"{sum(1 for t in g if t['bank']):3d} banks   net {net:+6.2f}   "
                          f"per trade {net / len(g) * 100:+6.1f}%")
        print("\nA rule is worth considering only if it beats 'trade every call' in BOTH halves "
              "and the trades it skips lost money ('skip net' negative).")


if __name__ == "__main__":
    main()
