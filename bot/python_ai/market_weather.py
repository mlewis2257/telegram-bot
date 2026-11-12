"""
market_weather.py — do bloodbath days show early enough to stop trading? Pure DB replay, no API calls.

WHY (2026-11-12). Go-live night: 11 live brain-lane trades, 0 banks, -0.198 SOL. qsim on both channels
banked 6 of 100 coins that night (normal is ~15-25%), and v7's BUYs did worse than its passes. A trader
would have felt the market and traded less. The question is whether the bot can see it in time.

WEATHER at a moment = of all qsim trades on solwhaletrending + solearlytrending that CLOSED in the
--hours before it, the share that banked. Only trades already closed count (no lookahead: a trade that
is still open has no result yet). Needs --min-recent closes, else "unknown". It is the channels' own
recent results — what the coins are doing today — not BTC or SOL.

For every trade in the population (--pop v7: the coins v7 scored 6+, live answers else the November
replay's; --pop all: every qsim trade) it reads the weather at the trade's ENTRY, then shows:
  1. results by weather bucket — does a bad recent stretch predict bad next trades?
  2. the gate "don't trade when weather < X%" for several X: trades kept / skipped, and the SOL it
     saves (the skipped trades' losses) or costs (their wins);
  3. the same split by first / second half of the window — a gate must work in both.
A same coin called on both channels counts twice (as qsim trades it twice); small effect, noted.

    python3 market_weather.py --pop v7
    python3 market_weather.py --pop all --since 2026-10-08 --hours 6

--signal sol / btc (2026-11-12): the weather is instead SOL's (BTC's) % change in USD over the --hours before
entry, from market_prices (price_recorder.py; its --backfill fills 90 days of hourly history). The price at
a moment = the latest recorded at or before it, if no more than 2 h old; else unknown. The gate becomes
"don't trade when SOL is down more than X% over the last N hours".
    python3 market_weather.py --signal sol --pop all --since 2026-10-08
"""

from __future__ import annotations

import argparse
import bisect
import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))
import db  # noqa: E402

CHANNELS = ["solwhaletrending", "solearlytrending"]
BUCKETS = [(0, 8), (8, 12), (12, 16), (16, 20), (20, 25), (25, 101)]
GATES = [6, 8, 10, 12, 15]
PRICE_BUCKETS = [(-100, -5), (-5, -2), (-2, 0), (0, 2), (2, 5), (5, 1000)]
PRICE_GATES = [1, 2, 3, 5, 8]               # pause when down more than X%
PRICE_STALE_H = 2.0


def load(since: str) -> list[dict]:
    """Every closed qsim trade on the two channels from a day before `since` (the weather needs history)."""
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute("""SELECT call_id, channel_handle, entry_time, exit_time, exit_reason, sol_in, sol_out
                       FROM qsim_positions
                       WHERE status = 'closed' AND sol_in > 0 AND exit_time IS NOT NULL
                         AND channel_handle = ANY(%s) AND entry_time >= %s::timestamptz - interval '1 day'
                       ORDER BY exit_time""", (CHANNELS, since))
        return [{"cid": r[0], "ch": r[1], "entry": r[2], "exit": r[3], "bank": "bank" in (r[4] or ""),
                 "s_in": float(r[5]), "s_out": float(r[6] or 0)} for r in cur.fetchall()]


def v7_buys() -> set[int]:
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute("SELECT call_id FROM trader_brain_entry WHERE prompt_v = 7 AND vs_typical >= 6")
        ids = {r[0] for r in cur.fetchall()}
        cur.execute("SELECT to_regclass('brain_replay_verdict') IS NOT NULL")
        if cur.fetchone()[0]:
            cur.execute("""SELECT r.call_id FROM brain_replay_verdict r
                           WHERE r.run = 'nov' AND r.arm = 'v7' AND r.vs_typical >= 6
                             AND NOT EXISTS (SELECT 1 FROM trader_brain_entry e
                                             WHERE e.call_id = r.call_id AND e.prompt_v = 7)""")
            ids |= {r[0] for r in cur.fetchall()}
    return ids


def weather_at(trades: list[dict], hours: float, min_recent: int):
    """Returns f(t) -> (bank %, closes) over trades that closed in [t - hours, t)."""
    exits = [t["exit"] for t in trades]               # sorted by exit_time
    banks = [0]
    for t in trades:
        banks.append(banks[-1] + t["bank"])
    w = timedelta(hours=hours)

    def f(at: datetime):
        i, j = bisect.bisect_left(exits, at - w), bisect.bisect_left(exits, at)
        n = j - i
        if n < min_recent:
            return None, n
        return (banks[j] - banks[i]) / n * 100, n
    return f


def price_weather(coin: str, hours: float):
    """Returns f(t) -> (% change of SOL/BTC over [t - hours, t], None) from market_prices; None if unknown."""
    col = {"sol": "sol_usd", "btc": "btc_usd"}[coin]
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute("SELECT to_regclass('market_prices') IS NOT NULL")
        rows = []
        if cur.fetchone()[0]:
            cur.execute(f"SELECT at, {col} FROM market_prices WHERE {col} > 0 ORDER BY at")
            rows = [(a, float(v)) for a, v in cur.fetchall()]
    ats = [a for a, _ in rows]
    stale = timedelta(hours=PRICE_STALE_H)

    def at_or_before(t):
        i = bisect.bisect_right(ats, t) - 1
        return rows[i][1] if i >= 0 and t - rows[i][0] <= stale else None

    def f(t: datetime):
        now, then = at_or_before(t), at_or_before(t - timedelta(hours=hours))
        if now is None or then is None:
            return None, 0
        return (now / then - 1) * 100, 1
    return f, len(rows)


def stats(g: list[dict]) -> dict:
    n = len(g)
    if not n:
        return {"n": 0}
    s_in = sum(t["s_in"] for t in g)
    return {"n": n, "bank": sum(t["bank"] for t in g) / n * 100,
            "rug": sum(t["s_out"] / t["s_in"] < 0.3 for t in g) / n * 100,
            "pnl": sum(t["s_out"] - t["s_in"] for t in g) / s_in * 100, "sol": sum(t["s_out"] - t["s_in"] for t in g)}


def line(label: str, s: dict) -> str:
    if not s.get("n"):
        return f"  {label:<18}{0:>6}"
    return (f"  {label:<18}{s['n']:>6}  bank {s['bank']:5.1f}%  rug {s['rug']:5.1f}%  P&L {s['pnl']:+6.1f}%/SOL"
            f"  ({s['sol']:+.3f} SOL)")


def report(a) -> None:
    trades = load(a.since)
    signal = getattr(a, "signal", "bank") or "bank"
    if signal == "bank":
        f = weather_at(trades, a.hours, a.min_recent)
        buckets = [(lo, hi, f"{lo}-{hi - 1 if hi <= 100 else 100}% banked") for lo, hi in BUCKETS]
        gate_levels, gate_label = GATES, lambda x: f"{x:>3}%"
        skip_if = lambda w, x: w < x
    else:
        f, n_prices = price_weather(signal, a.hours)
        if not n_prices:
            print("no market_prices yet — run: python3 price_recorder.py --backfill 90")
            return
        buckets = [(lo, hi, f"{signal.upper()} {lo:+d}..{hi:+d}%" if -100 < lo and hi < 1000 else
                    (f"{signal.upper()} under {hi:+d}%" if lo <= -100 else f"{signal.upper()} over {lo:+d}%"))
                   for lo, hi in PRICE_BUCKETS]
        gate_levels, gate_label = PRICE_GATES, lambda x: f"-{x}%"
        skip_if = lambda w, x: w < -x
    start = datetime.fromisoformat(a.since).replace(tzinfo=timezone.utc)
    pop = [t for t in trades if t["entry"] >= start]
    if a.pop == "v7":
        ids = v7_buys()
        pop = [t for t in pop if t["cid"] in ids]
    if a.channel:
        pop = [t for t in pop if t["ch"] == a.channel]
    for t in pop:
        t["w"], t["w_n"] = f(t["entry"])
    known = [t for t in pop if t["w"] is not None]
    if signal == "bank":
        print(f"\nMARKET WEATHER — share of swt+solearly qsim trades that closed in the {a.hours:g} h before entry "
              f"and banked (needs {a.min_recent}+ closes)")
    else:
        print(f"\nMARKET WEATHER — {signal.upper()}'s % change in USD over the {a.hours:g} h before entry "
              f"(market_prices; unknown if a price is over {PRICE_STALE_H:g} h old)")
    print(f"population: {'v7 BUYs (score 6+)' if a.pop == 'v7' else 'all qsim trades'}"
          f"{' on ' + a.channel if a.channel else ' on both channels'}, entered since {a.since}: {len(pop)} trades, "
          f"{len(known)} with a known weather")
    print(line("ALL", stats(pop)))
    print(f"\n1. BY THE WEATHER AT ENTRY")
    for lo, hi, lab in buckets:
        print(line(f"  {lab}", stats([t for t in known if lo <= t["w"] < hi])))
    print(line("  unknown", stats([t for t in pop if t["w"] is None])))

    def gates(g: list[dict], title: str) -> None:
        print(f"\n{title}")
        print(f"  {'pause below':<12}{'kept':>6}{'kept P&L':>11}{'skipped':>9}{'skipped P&L':>13}{'SOL saved':>11}")
        for x in gate_levels:
            skip = [t for t in g if t["w"] is not None and skip_if(t["w"], x)]
            kept = [t for t in g if not (t["w"] is not None and skip_if(t["w"], x))]
            k, s = stats(kept), stats(skip)
            print(f"  {gate_label(x):<12}{k.get('n', 0):>6}{(f'{k['pnl']:+.1f}%' if k.get('n') else '—'):>11}"
                  f"{s.get('n', 0):>9}{(f'{s['pnl']:+.1f}%' if s.get('n') else '—'):>13}"
                  f"{-s.get('sol', 0):>+11.3f}")
    gates(pop, "2. THE GATE: don't trade when the weather is below X% (unknown weather = trade)" if signal == "bank"
          else f"2. THE GATE: don't trade when {signal.upper()} is down more than X% over {a.hours:g} h (unknown = trade)")
    pop.sort(key=lambda t: t["entry"])
    half = len(pop) // 2
    if half >= 20:
        a1, a2 = pop[:half], pop[half:]
        gates(a1, f"3a. FIRST HALF ({a1[0]['entry']:%m-%d} .. {a1[-1]['entry']:%m-%d})")
        gates(a2, f"3b. SECOND HALF ({a2[0]['entry']:%m-%d} .. {a2[-1]['entry']:%m-%d})")
    print("\nRead: a gate is worth having only if the skipped trades clearly LOSE in both halves and the kept")
    print("ones do better — and it doesn't skip most of the good days. 'SOL saved' is at qsim's 0.05 SOL size.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pop", choices=["v7", "all"], default="v7")
    ap.add_argument("--since", default="2026-11-01")
    ap.add_argument("--hours", type=float, default=3.0)
    ap.add_argument("--min-recent", type=int, default=10)
    ap.add_argument("--channel")
    ap.add_argument("--signal", choices=["bank", "sol", "btc"], default="bank",
                    help="bank = the channels' recent bank rate; sol / btc = that coin's %% change over --hours")
    report(ap.parse_args())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
