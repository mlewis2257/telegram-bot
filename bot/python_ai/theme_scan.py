"""
theme_scan.py — do copycats of a coin that just ran, or coins riding a hot theme, do better?

WHY (2026-10-24). On Oct 22-23 a well-known dev's KIMCHI ran to ~$47M and a wave of
copies (KIMCHI, ogkimchi, GKIMCHI, KIMCHICITY, ...) followed: some ran 4-6x, others rugged
to zero. A trader sees that wave; the bot does not. This measures two things known AT THE
CALL, for every solwhaletrending call qsim traded:

  copies a runner   its symbol or name contains a word (4+ letters) from a coin that
                    had ALREADY peaked at RAN_MCAP+ (and RAN_MULT x its call price) in
                    the RUN_WINDOW_H hours before the call. Runners come from shadow,
                    which follows every call on all four channels; only peaks dated
                    before the call count, so nothing is looked up from the future.
  theme heat        how many OTHER coins called on any channel in the HEAT_WINDOW_H
                    hours before shared a 4+ letter word with it (a wave in progress,
                    whether or not anything in it has run yet).

Results are qsim real-quote trades, banks capped at 2.1x (live-like), split into halves.
Where the followed-wallet data exists it is crossed with "2+ followed wallets at the
first look". Names are matched as lower-case letters/digits; generic words are ignored.

Read-only. Run on the VPS:  python3 theme_scan.py   [--since 2026-10-07] [--lane all|none|low_score]
    [--ran-mcap 1000000] [--ran-mult 5] [--run-window 48] [--heat-window 24] [--list 40]
"""

from __future__ import annotations

import argparse
import re
from collections import defaultdict
from datetime import timedelta

import db

BANK_CAP = 2.1
STOP = {"coin", "token", "the", "official", "solana", "meme", "pump", "with", "this", "that",
        "from", "fun", "just", "real", "community", "dollar", "money", "capital"}


def _alnum(s) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def keys(symbol, name) -> set[str]:
    """Theme words of a coin: its whole symbol and each word of symbol and name, 4+ chars."""
    out = set()
    for part in [_alnum(symbol)] + re.split(r"[^a-z0-9]+", f"{symbol or ''} {name or ''}".lower()):
        if len(part) >= 4 and part not in STOP and not part.isdigit():
            out.add(part)
    return out


def matches(ks: set[str], symbol, name) -> str | None:
    """The first key found inside the coin's symbol or name (letters/digits only), or None."""
    s, n = _alnum(symbol), _alnum(name)
    for k in sorted(ks, key=len, reverse=True):
        if k in s or k in n:
            return k
    return None


def load(since: str, lane: str, ran_mcap: float, ran_mult: float, run_h: float, heat_h: float):
    conn = db.get_conn()
    db.safe_rollback()
    lane_sql = {"all": "", "none": "AND c.skip_reason IS NULL",
                "low_score": "AND c.skip_reason = 'low_score'"}[lane]
    pad = max(run_h, heat_h)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT qp.call_id, c.created_at, qp.entry_time, t.mint_address, t.symbol, t.name,
                   qp.exit_reason LIKE '%%bank%%',
                   CASE WHEN qp.exit_reason LIKE '%%bank%%' THEN LEAST(qp.sol_out / qp.sol_in, {BANK_CAP})
                        ELSE qp.sol_out / qp.sol_in END,
                   (c.skip_reason IS NULL AND t.security_flag = 'safe'
                    AND c.mcap_at_call BETWEEN 80000 AND 120000
                    AND COALESCE(t.hodl_count, 0) < 700
                    AND c.conviction_score >= 70 AND c.conviction_score < 80)
            FROM qsim_positions qp
            JOIN calls c  ON c.id = qp.call_id
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.status = 'closed' AND qp.channel_handle = 'solwhaletrending'
              AND qp.sol_in > 0 AND qp.entry_time >= %s
              {lane_sql}
            ORDER BY qp.entry_time
            """, (since,))
        trades = [dict(zip(("call_id", "called", "entry", "mint", "sym", "name", "bank", "x", "live_filter"), r))
                  for r in cur.fetchall()]
        cur.execute(
            """
            SELECT DISTINCT ON (t.id) t.mint_address, t.symbol, t.name, sp.peak_mcap, sp.peak_at,
                   ltrim(ch.handle, '@')
            FROM shadow_positions sp
            JOIN tokens t   ON t.id = sp.token_id
            JOIN calls c    ON c.id = sp.call_id
            JOIN channels ch ON ch.id = c.channel_id
            WHERE sp.peak_at >= %s::timestamptz - (%s || ' hours')::interval
              AND sp.peak_mcap >= %s AND sp.peak_multiplier >= %s
            ORDER BY t.id, sp.peak_mcap DESC
            """, (since, pad, ran_mcap, ran_mult))
        runners = [dict(zip(("mint", "sym", "name", "peak_mcap", "peak_at", "channel"), r)) for r in cur.fetchall()]
        cur.execute(
            """
            SELECT DISTINCT ON (t.id) c.created_at, t.mint_address, t.symbol, t.name
            FROM calls c JOIN tokens t ON t.id = c.token_id
            WHERE c.created_at >= %s::timestamptz - (%s || ' hours')::interval
              AND t.mint_address NOT LIKE 'UNKNOWN:%%' AND t.mint_address NOT LIKE 'INFERRED:%%'
            ORDER BY t.id, c.created_at
            """, (since, pad))
        calls = [dict(zip(("at", "mint", "sym", "name"), r)) for r in cur.fetchall()]
        followed = _followed(cur, [t["call_id"] for t in trades])
    for r in runners:
        r["keys"] = keys(r["sym"], r["name"])
    calls.sort(key=lambda c: c["at"])
    for c in calls:
        c["keys"] = keys(c["sym"], c["name"])
    return trades, runners, calls, followed


def _followed(cur, ids):
    """{call_id: n followed wallets holding at the first look} from tracked_wallet_holdings, or None."""
    try:
        cur.execute(
            """
            SELECT h.call_id, count(DISTINCT h.wallet)
            FROM tracked_wallet_holdings h
            JOIN qsim_positions qp ON qp.call_id = h.call_id
            WHERE h.call_id = ANY(%s) AND h.prev_balance IS NULL
              AND h.observed_at <= qp.entry_time + interval '4 minutes'
            GROUP BY h.call_id
            """, (ids,))
        out = {cid: n for cid, n in cur.fetchall()}
        cur.execute("SELECT min(observed_at) FROM tracked_wallet_holdings")
        return out, cur.fetchone()[0]
    except Exception:
        db.safe_rollback()
        return None


def annotate(trades, runners, calls, run_h, heat_h, followed):
    import bisect
    call_times = [c["at"] for c in calls]
    for t in trades:
        tk = keys(t["sym"], t["name"])
        t["runner"] = None
        for r in runners:
            if r["mint"] == t["mint"] or r["peak_at"] is None:
                continue
            if not (t["called"] - timedelta(hours=run_h) <= r["peak_at"] < t["called"]):
                continue
            k = matches(r["keys"], t["sym"], t["name"])
            if k:
                if t["runner"] is None or r["peak_mcap"] > t["runner"]["peak_mcap"]:
                    t["runner"] = {**r, "key": k}
        lo = bisect.bisect_left(call_times, t["called"] - timedelta(hours=heat_h))
        hi = bisect.bisect_left(call_times, t["called"])
        mints = set()
        for c in calls[lo:hi]:
            if c["mint"] == t["mint"]:
                continue
            if matches(tk, c["sym"], c["name"]) or matches(c["keys"], t["sym"], t["name"]):
                mints.add(c["mint"])
        t["heat"] = len(mints)
        t["followed"] = None
        if followed is not None:
            per, first = followed
            if first is not None and t["entry"] >= first:
                n = per.get(t["call_id"], 0)
                t["followed"] = "0 wallets" if n == 0 else ("1 wallet" if n == 1 else "2+ wallets")


def report(title, groups, mid, order=None):
    if not groups:
        return
    print(f"\n{title}")
    print(f"  {'value':38s} {'trades':>6s} {'banks':>5s} {'bank %':>6s} {'net':>7s} {'per tr':>7s} "
          f"{'1st half':>8s} {'2nd half':>8s}")
    for val in (order or sorted(groups)):
        g = groups.get(val)
        if not g:
            continue
        n = len(g)
        banks = sum(1 for t in g if t["bank"])
        net = sum(float(t["x"]) - 1 for t in g)
        h1 = sum(float(t["x"]) - 1 for t in g if t["entry"] < mid)
        h2 = net - h1
        print(f"  {val:38s} {n:6d} {banks:5d} {banks / n * 100:5.1f}% {net:+7.2f} {net / n * 100:+6.1f}% "
              f"{h1:+8.2f} {h2:+8.2f}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-10-07")
    ap.add_argument("--lane", default="all", choices=["all", "none", "low_score"])
    ap.add_argument("--ran-mcap", type=float, default=1_000_000)
    ap.add_argument("--ran-mult", type=float, default=5.0)
    ap.add_argument("--run-window", type=float, default=48)
    ap.add_argument("--heat-window", type=float, default=24)
    ap.add_argument("--list", type=int, default=40, help="matched trades to list")
    a = ap.parse_args()
    trades, runners, calls, followed = load(a.since, a.lane, a.ran_mcap, a.ran_mult,
                                            a.run_window, a.heat_window)
    if not trades:
        print("no qsim trades found")
        return
    annotate(trades, runners, calls, a.run_window, a.heat_window, followed)
    mid = trades[len(trades) // 2]["entry"]
    print(f"solwhaletrending qsim trades since {a.since} (lane {a.lane}): {len(trades)}; halves split at "
          f"{mid:%m-%d %H:%M}. Runners = coins that peaked at ${a.ran_mcap:,.0f}+ and {a.ran_mult:g}x+ on "
          f"any channel: {len(runners)}. Calls scanned for theme heat: {len(calls)}.")

    g = defaultdict(list)
    for t in trades:
        g["yes" if t["runner"] else "no"].append(t)
    report(f"COPIES A COIN THAT RAN in the {a.run_window:g}h before the call:", g, mid, ["yes", "no"])

    def heat_bucket(h):
        return "0 others" if h == 0 else "1 other" if h == 1 else "2-4 others" if h <= 4 else "5+ others"
    g = defaultdict(list)
    for t in trades:
        g[heat_bucket(t["heat"])].append(t)
    report(f"THEME HEAT: other coins called (any channel) sharing a word, {a.heat_window:g}h before:", g, mid,
           ["0 others", "1 other", "2-4 others", "5+ others"])

    g = defaultdict(list)
    for t in trades:
        g[("copies runner" if t["runner"] else "no runner") + " / " +
          ("heat 2+" if t["heat"] >= 2 else "heat 0-1")].append(t)
    report("COMBINED:", g, mid)

    g = defaultdict(list)
    for t in trades:
        if t["live_filter"]:
            g["in live filter, " + ("copies runner" if t["runner"] else "no runner")].append(t)
    report("INSIDE THE LIVE FILTER (unlabelled, safe, 80-120k, <700 holders, score 70-79):", g, mid)

    if followed is not None:
        g = defaultdict(list)
        for t in trades:
            if t["followed"]:
                g[f"{t['followed']}, " + ("copies runner" if t["runner"] else "no runner")].append(t)
        report("WITH FOLLOWED WALLETS (first look, only since wallet-holdings started):", g, mid)

    matched = [t for t in trades if t["runner"]]
    if matched and a.list:
        print(f"\nlatest {min(a.list, len(matched))} trades that copied a runner:")
        for t in matched[-a.list:]:
            r = t["runner"]
            print(f"  {t['entry']:%m-%d %H:%M} {str(t['sym'])[:12]:12s} -> {float(t['x']):5.2f}x "
                  f"{'BANK' if t['bank'] else '    '}  copies {str(r['sym'])[:12]:12s} (word '{r['key']}', "
                  f"peaked ${float(r['peak_mcap']) / 1e6:.1f}M on {r['channel']}, "
                  f"{(t['called'] - r['peak_at']).total_seconds() / 3600:.0f}h before)  heat {t['heat']}")
    print("\nA theme rule is worth testing only if it beats the rest in BOTH halves. Name matching is "
          "crude: read the listed coins to check the matches are real copies.")


if __name__ == "__main__":
    main()
