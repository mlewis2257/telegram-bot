"""
wallet_reentry_scan.py — buy back in when followed wallets buy after we sold?

WHY (2026-10-27). Among solwhaletrending coins where a followed wallet was first seen
buying only AFTER qsim's exit, about half went on to 2x+ of the original entry (CARTI
11x, Marvin 8x, GME 6x, SCS 6x, ELON 5x) against ~14% of stopped coins in general. Every
earlier re-entry test used price alone (buy back at 1.0/1.2/1.5x) and lost. The open
question is order: did the wallets buy BEFORE the run (a signal) or partway up it
(chasing)? The only fair test is to trade it: re-enter at the real quote at the moment the
buy is seen, then run the exits from there.

WHAT. For every closed solwhaletrending qsim position since --since:
  trigger   the first moment, within --window minutes after qsim's exit, at which the
            number of DISTINCT followed wallets seen buying reaches N (1 and 2 scored).
            A buy = a balance that went UP between two wallet_holdings looks
            (prev_balance not null). First readings (prev_balance null) are NOT buys:
            wallet_holdings re-reads every holder as "first" when a coin returns to its
            watch list, so they are counted separately as a data check only.
  entry     the first qsim real quote at or after the trigger (within 5 min), as a
            multiple of the ORIGINAL entry; the re-entry is measured from there.
  exits     replayed on the following quotes (24h): 'stop 0.90 | bank 2.5x' (the wallet
            lane) and 'stop 0.80 | bank 2x' (today's regular), with the regular profit
            floor below the bank, upside fills capped at the line + 5% (live-like).
LIMITS. After qsim's exit it looks once a minute for 30 min (longer around banks and
recoveries), then hourly: a fall is booked late (harsh), a short spike can be missed.
wallet_holdings looks every 2 min and stops 60 min after exit, so --window above 60 finds
nothing more. No fees.

Read-only. Run on the VPS:  python3 wallet_reentry_scan.py  [--since '2026-10-21 07:30']
    [--window 60] [--list 40]
"""

from __future__ import annotations

import argparse
from collections import defaultdict

import db
import new_lane_exit_replay as rp

RULES = {
    "stop 0.90 | bank 2.5x": rp.Rules(stop=0.90, bank=2.5, floors=((1.6, 1.30), (1.35, 1.15))),
    "stop 0.80 | bank 2x":   rp.Rules(stop=0.80, bank=2.0, floors=((1.6, 1.30), (1.35, 1.15))),
}
TRIGGERS = (1, 2)
ENTRY_SLACK_S = 300


def load(since: str, window_min: float):
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT qp.call_id, left(t.symbol, 12), t.mint_address, qp.entry_time, qp.exit_time,
                   qp.exit_reason, COALESCE(c.skip_reason, 'none')
            FROM qsim_positions qp
            JOIN calls c  ON c.id = qp.call_id
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.status = 'closed' AND qp.channel_handle = 'solwhaletrending'
              AND qp.entry_time >= %s AND qp.exit_time IS NOT NULL
              AND qp.exit_time < now() - interval '2 hours'
            ORDER BY qp.entry_time
            """, (since,))
        pos = [dict(zip(("call_id", "sym", "mint", "entry", "exit", "reason", "lane"), r))
               for r in cur.fetchall()]
        if not pos:
            return [], {}, {}
        mints = list({p["mint"] for p in pos})
        cur.execute(
            """
            SELECT mint, wallet, observed_at, balance, prev_balance
            FROM tracked_wallet_holdings
            WHERE mint = ANY(%s)
            ORDER BY observed_at
            """, (mints,))
        hold = defaultdict(list)
        for mint, wallet, at, bal, prev in cur.fetchall():
            hold[mint].append((at, wallet, float(bal), None if prev is None else float(prev)))
        cur.execute(
            """
            SELECT call_id, observed_at, real_mult, no_route
            FROM qsim_quote_observations
            WHERE call_id = ANY(%s) AND NOT rate_limited
              AND (real_mult IS NOT NULL OR no_route)
            ORDER BY call_id, observed_at
            """, ([p["call_id"] for p in pos],))
        obs = defaultdict(list)
        for cid, at, m, nr in cur.fetchall():
            obs[cid].append((at, None if nr else float(m)))
    return pos, hold, obs


def trigger_time(p: dict, rows: list, n: int, window_min: float):
    """(time the n-th distinct wallet was seen buying after exit, wallets) or (None, ...)."""
    lo, hi = p["exit"], p["exit"] + rp_timedelta(window_min * 60)
    seen = []
    for at, wallet, bal, prev in rows:
        if at <= lo or at > hi:
            continue
        if prev is not None and bal > prev and wallet not in seen:
            seen.append(wallet)
            if len(seen) >= n:
                return at, seen
    return None, seen


def first_reads_after_exit(p: dict, rows: list, window_min: float) -> int:
    lo, hi = p["exit"], p["exit"] + rp_timedelta(window_min * 60)
    return len({w for at, w, bal, prev in rows if lo < at <= hi and prev is None and bal > 0})


def rp_timedelta(secs: float):
    from datetime import timedelta
    return timedelta(seconds=secs)


def reentry(p: dict, path_obs: list, t_trig):
    """(m0 = multiple of original entry at the re-entry quote, rebased path) or None."""
    m0_at = m0 = None
    for at, m in path_obs:
        if at < t_trig:
            continue
        if (at - t_trig).total_seconds() > ENTRY_SLACK_S:
            return None
        if m is not None and m > 0:
            m0_at, m0 = at, m
            break
    if m0 is None:
        return None
    path = [((at - m0_at).total_seconds(), None if m is None else m / m0)
            for at, m in path_obs if at > m0_at]
    if not path:
        return None
    return m0, path


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-10-21 07:30")
    ap.add_argument("--window", type=float, default=60, help="minutes after qsim's exit to watch for buys")
    ap.add_argument("--list", type=int, default=40)
    a = ap.parse_args()
    pos, hold, obs = load(a.since, a.window)
    if not pos:
        print("no qsim positions found")
        return
    first_reads = sum(1 for p in pos if first_reads_after_exit(p, hold.get(p["mint"], []), a.window) > 0)
    print(f"solwhaletrending qsim positions since {a.since}: {len(pos)} closed. Followed-wallet BUYS "
          f"(balance up between two looks) within {a.window:g} min after qsim's exit are the trigger.")
    print(f"data check: {first_reads} positions had a followed wallet appear as a FIRST reading after "
          f"exit (coin re-added to the watch list) — not counted as buys here.")

    for n in TRIGGERS:
        trades = []
        for p in pos:
            rows = hold.get(p["mint"], [])
            t_trig, wallets = trigger_time(p, rows, n, a.window)
            if t_trig is None:
                continue
            path_obs = [(at, m) for at, m in obs.get(p["call_id"], []) if at > p["exit"]]
            r = reentry(p, path_obs, t_trig)
            if r is None:
                continue
            m0, path = r
            later = [m for at, m in path_obs if at >= t_trig and m is not None]
            res = {name: rp.simulate(path, rules) for name, rules in RULES.items()}
            trades.append({**p, "t_trig": t_trig, "wallets": len(wallets), "m0": m0,
                           "peak_after": max(later) if later else None, "res": res,
                           "mins_after_exit": (t_trig - p["exit"]).total_seconds() / 60})
        print(f"\n=== RE-ENTER when {n}+ followed wallet{'s' if n > 1 else ''} seen buying after our exit: "
              f"{len(trades)} trades ===")
        if not trades:
            continue
        trades.sort(key=lambda t: t["t_trig"])
        half = len(trades) // 2
        print(f"  {'exits':24s} {'trades':>6s} {'banks':>5s} {'bank %':>6s} {'net':>7s} {'per tr':>7s} "
              f"{'1st half':>8s} {'2nd half':>8s} {'w/o best':>8s}")
        for name in RULES:
            nets = [t["res"][name].x - 1 for t in trades]
            banks = sum(1 for t in trades if t["res"][name].reason == "bank")
            net = sum(nets)
            print(f"  {name:24s} {len(trades):6d} {banks:5d} {banks / len(trades) * 100:5.1f}% {net:+7.2f} "
                  f"{net / len(trades) * 100:+6.1f}% {sum(nets[:half]):+8.2f} {sum(nets[half:]):+8.2f} "
                  f"{net - max(nets):+8.2f}")
        by_reason = defaultdict(list)
        for t in trades:
            key = "after a bank" if "bank" in (t["reason"] or "") else (
                "after a stop" if t["reason"] == "hard_stop" else f"after {t['reason']}")
            by_reason[key].append(t)
        name0 = next(iter(RULES))
        print(f"  by what we sold on ({name0}):")
        for key, g in sorted(by_reason.items()):
            net = sum(t["res"][name0].x - 1 for t in g)
            banks = sum(1 for t in g if t["res"][name0].reason == "bank")
            print(f"    {key:22s} {len(g):4d} trades  {banks:3d} banks  net {net:+6.2f}  "
                  f"per trade {net / len(g) * 100:+6.1f}%")
        if a.list:
            print(f"  trades (re-entry price and later peak are multiples of the ORIGINAL entry):")
            for t in trades[-a.list:]:
                r0 = t["res"][name0]
                print(f"    {t['entry']:%m-%d %H:%M} {str(t['sym']):12s} sold {t['reason'] or '?':12s} "
                      f"+{t['mins_after_exit']:4.0f}m  {t['wallets']}w  re-enter at {t['m0']:5.2f}x  "
                      f"later peak {('%.2f' % t['peak_after']) if t['peak_after'] else '  ?  '}x  ->  "
                      f"{r0.reason:6s} {r0.x:5.2f}x of re-entry")
    print("\nWorth building only if a rule is positive in BOTH halves and without its best trade. "
          "'re-enter at' below 1.0x with a high 'later peak' = the wallets bought before the run; "
          "re-entry near the peak = they were chasing it.")


if __name__ == "__main__":
    main()
