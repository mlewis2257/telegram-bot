"""
qsim_partial_runner.py — bank most of it at 2x, let a slice run to a TARGET.

THE POLICY
----------
    first quote >= bank      -> sell (1 - keep) of the position there
    the remaining `keep`     -> run on, and exit at whichever comes first:
                                  a quote >= target      (the checkpoint)
                                  a quote <= stop        (still protected)
                                  the last quote observed (terminal)

Positions that never reach `bank` are untouched and score exactly what qsim
booked, so they enter both arms identically and contribute nothing to the delta.

WHY THIS IS MEASURABLE WHERE THE TRAIL WAS NOT
-----------------------------------------------
qsim_ratchet_trail could not settle its question because a TRAIL has to observe
the path DOWN to fire, and post-exit quotes are 30-60 minutes apart. A trail on
that data exits at whatever sparse print it happens to see, which is why the
same dataset priced the profit floor at -9.05 when the live config books +4.13
on identical positions.

A TARGET has no such problem. "This coin printed 5x at some point" is a single
observation. Sparse sampling can only make you MISS a target that really
happened; it can never invent one. So every number here is a FLOOR on what the
policy is worth, and the handicap runs one way only.

The runner's stop and its terminal fallback are the two legs that a sparse
series can still distort, and both distort AGAINST the policy: a missed dip
means you exit lower later, and a terminal is whatever the last sparse quote
says. So the bias is consistent.

INTEGRITY
---------
A runner that hits neither target nor stop takes the LAST OBSERVED QUOTE, win or
lose. It never falls back to qsim's own result after reading the series — that
pairing of post-exit upside with stop-protected downside is the bor_ free
option, +13.27 until bounded and -112.75 after.

No outcome filter. Positions with no observations are reported, not absorbed.

    python3 qsim_partial_runner.py --days 17
    python3 qsim_partial_runner.py --days 17 --keep 0.20 --target 5.0
    python3 qsim_partial_runner.py --days 17 --sweep

SCOPE (added 2026-10-07). Defaults to the slice LIVE can trade — LIVE_LANES channels and
the live mcap band — because the first version measured every lane and band and reported
that as what the policy would do for live, the same mistake entry_filter --backtest made.
--all-lanes / --mcap-min 0 --mcap-max 0 widen it for research.

    # the "keep a slice" question, on clean data only, by flag:
    python3 qsim_partial_runner.py --since 2026-10-07 --stop 0 --max-hours 24 --sweep
    python3 qsim_partial_runner.py --since 2026-10-07 --stop 0 --max-hours 24 \
        --keep 0.2 --target 10 --flag warning --detail

--since matters: qsim spent ~90% of its time in 429 backoff until 2026-10-06 and its
post-exit probes were 37% rate-limited, so runner results from before that date are
measured on a path that was mostly not observed.

--stop 0 gives the runner NO stop: it exits only at the target or the horizon. That is a
different policy from the 0.80 default, which sells the slice on the first dip below
entry and therefore cannot hold a coin that runs 17 hours later.

--max-hours bounds the runner to that many hours after qsim's exit, so "terminal" means
"sold at the horizon" for every coin instead of "whatever the last probe happened to be",
which differs by how long ago each coin closed.
"""

from __future__ import annotations

import argparse
import os
import sys
from collections import defaultdict

sys.path.insert(0, os.path.dirname(__file__))

import db  # noqa: E402


def _live_channels() -> list[str]:
    try:
        import lane_policy
        return sorted({k[0] for k in lane_policy.LIVE_LANES})
    except Exception:
        return []


def _scope_sql(days: float, since: str | None, channels: list[str],
               mcap_min: float, mcap_max: float, flag: str | None) -> tuple[str, list]:
    """WHERE fragment + params shared by the positions and series queries, so the two can
    never select different coins."""
    conds = ["qp.status = 'closed'", "qp.sol_in > 0"]
    params: list = []
    if since:
        conds.append("qp.entry_time >= %s::date"); params.append(since)
    else:
        conds.append("qp.entry_time >= now() - (%s || ' days')::interval"); params.append(days)
    if channels:
        conds.append("qp.channel_handle = ANY(%s)"); params.append(channels)
    if mcap_max > 0:
        conds.append("c.mcap_at_call BETWEEN %s AND %s"); params += [mcap_min, mcap_max]
    if flag:
        conds.append("coalesce(nullif(lower(trim(t.security_flag)), ''), 'null') = %s")
        params.append(flag.strip().lower())
    return " AND ".join(conds), params


def _positions(where: str, params: list) -> dict[int, dict]:
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT qp.call_id, qp.sol_in, qp.sol_out, qp.exit_reason, t.symbol
            FROM qsim_positions qp
            JOIN tokens t ON t.id = qp.token_id
            JOIN calls  c ON c.id = qp.call_id
            WHERE {where}
        """, tuple(params))
        return {int(r["call_id"]): dict(r) for r in cur.fetchall()}


def _series(where: str, params: list) -> dict[int, list[tuple[float, bool, float]]]:
    """(multiple, was_still_held) per observation, in time order.

    The flag is load-bearing. The BANK may only be taken on a quote qsim
    actually saw while holding; a coin that hard-stopped at 0.78 and recovered
    to 2.5x afterwards did not give anyone a 2.5x fill. Banking on post-exit
    quotes is the bor_ free option -- post-exit upside with the live config's
    stop-protected downside -- and the first cut of this file did exactly that,
    reporting +15 SOL with 289 positions "reaching" a bank that only 188 really
    did.

    The RUNNER may use post-exit quotes, because it genuinely holds longer than
    qsim did. That is the legitimate half of --include-post-exit."""
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    out: dict[int, list] = defaultdict(list)
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT o.call_id, o.real_mult,
                   (o.observed_at <= qp.exit_time) AS in_life,
                   extract(epoch FROM o.observed_at - qp.exit_time) / 3600.0 AS hrs_after
            FROM qsim_quote_observations o
            JOIN qsim_positions qp ON qp.call_id = o.call_id
            JOIN tokens t ON t.id = qp.token_id
            JOIN calls  c ON c.id = qp.call_id
            WHERE {where}
              AND o.real_mult IS NOT NULL
              AND NOT coalesce(o.no_route, false)
            ORDER BY o.call_id, o.observed_at
        """, tuple(params))
        for r in cur.fetchall():
            out[int(r["call_id"])].append((float(r["real_mult"]),
                                           bool(r["in_life"]),
                                           float(r["hrs_after"] or 0.0)))
    return out


def _simulate(series: list, bank: float, keep: float,
              target: float, stop: float, max_hours: float = 0.0) -> tuple[float, str] | None:
    """Blended multiple for the whole position, and what closed the runner.

    None means the coin never reached `bank`, so the policy did nothing and the
    row keeps qsim's actual result.
    """
    # The bank must land on a quote taken WHILE HELD. See _series.
    idx = next((i for i, row in enumerate(series)
                if row[1] and row[0] >= bank), None)
    if idx is None:
        return None
    bank_mult = series[idx][0]
    # Horizon: the runner is sold at the last quote within max_hours of qsim's exit.
    # In-life quotes (hrs_after <= 0) always count.
    rest = [row[0] for row in series[idx + 1:]
            if max_hours <= 0 or (row[2] if len(row) > 2 else 0.0) <= max_hours]
    if not rest:
        # Banked on the last quote there is; the runner has nowhere to go and
        # is marked at the same price rather than assumed to survive.
        return bank_mult, "no_runner_data"
    for m in rest:
        if target > 0 and m >= target:
            # Credit the TARGET, not the quote that revealed it. Post-exit looks are up
            # to an hour apart, so the first quote past a 10x target can read 25x; a bot
            # watching every few seconds sells near 10x and never sees that print.
            # Booking the overshoot would break the "this is a floor" guarantee above.
            return (1 - keep) * bank_mult + keep * target, "target"
        if stop > 0 and m <= stop:
            return (1 - keep) * bank_mult + keep * m, "stop"
    return (1 - keep) * bank_mult + keep * rest[-1], ("horizon" if max_hours > 0 else "terminal")


def _run(pos: dict, ser: dict, bank: float, keep: float,
         target: float, stop: float, max_hours: float = 0.0) -> dict:
    act = sh = dep = 0.0
    counts: dict[str, int] = defaultdict(int)
    deltas: list[tuple] = []
    engaged = 0
    for cid, p in pos.items():
        sol_in = float(p["sol_in"])
        a = float(p["sol_out"] or 0.0) - sol_in
        act += a
        dep += sol_in
        series = ser.get(cid) or []
        res = _simulate(series, bank, keep, target, stop, max_hours) if series else None
        if res is None:
            sh += a                      # never reached the bank: identical arms
            continue
        mult, why = res
        h = sol_in * mult - sol_in
        sh += h
        engaged += 1
        counts[why] += 1
        deltas.append((h - a, p.get("symbol") or "?", mult, why,
                       max(row[0] for row in series)))
    return {"actual": act, "policy": sh, "deployed": dep, "engaged": engaged,
            "counts": dict(counts), "deltas": deltas}


def report(days: float, bank: float, keep: float, target: float,
           stop: float, detail: bool, sweep: bool, *, since: str | None = None,
           all_lanes: bool = False, mcap_min: float = 80000.0, mcap_max: float = 120000.0,
           flag: str | None = None, max_hours: float = 0.0) -> None:
    channels = [] if all_lanes else _live_channels()
    where, params = _scope_sql(days, since, channels, mcap_min, mcap_max, flag)
    pos = _positions(where, params)
    ser = _series(where, params)
    print(f"SCOPE  {', '.join(channels) if channels else 'ALL lanes'}   "
          + (f"mcap {mcap_min/1000:g}k-{mcap_max/1000:g}k" if mcap_max > 0 else "all mcaps")
          + (f"   flag={flag}" if flag else "   all flags")
          + (f"   since {since}" if since else f"   last {days:g}d")
          + (f"   runner horizon {max_hours:g}h" if max_hours > 0 else "   runner horizon: last quote")
          + (f"   runner stop {stop:g}x" if stop > 0 else "   runner stop: NONE"))
    if not pos:
        print("no closed positions in window")
        return
    missing = sum(1 for cid in pos if not ser.get(cid))
    days = days if not since else float("nan")

    if sweep:
        print(f"SWEEP  last {days:g}d   bank={bank:g}x  stop={stop:g}x   "
              f"({len(pos)} positions, {missing} with no quotes)")
        print(f"  {'keep':>6}{'target':>8}{'engaged':>9}{'actual':>10}"
              f"{'policy':>10}{'delta':>10}")
        for k in (0.0, 0.10, 0.20, 0.30, 0.50):
            for t in (3.0, 5.0, 10.0, 0.0):
                r = _run(pos, ser, bank, k, t, stop, max_hours)
                label = f"{t:g}x" if t > 0 else "none"
                print(f"  {k:>6.0%}{label:>8}{r['engaged']:>9}"
                      f"{r['actual']:>10.3f}{r['policy']:>10.3f}"
                      f"{r['policy'] - r['actual']:>10.3f}")
        print()
        print("  READ keep=0% FIRST. It is the control: bank the WHOLE position")
        print("  at the bank level and keep no runner, so its delta is purely")
        print("  the overlay change against whatever the window actually ran.")
        print("  The 17d window was mostly bank_1p3x with a 70% partial, so that")
        print("  row is large and has nothing to do with running a slice.")
        print("  The RUNNER is worth (its row) minus (the keep=0% row); if that")
        print("  is negative at every keep, running a slice costs you.")
        print()
        print("  target 'none' = the runner only ever exits on the stop or on")
        print("  the last quote, which is the pure hold-longer case.")
        return

    r = _run(pos, ser, bank, keep, target, stop, max_hours)
    print(f"PARTIAL RUNNER  last {days:g}d   bank {bank:g}x, keep {keep:.0%} "
          f"running to {target:g}x   stop={stop:g}x")
    print(f"  positions: {len(pos)}   reached the bank: {r['engaged']}   "
          f"no quotes: {missing}")
    print()
    print(f"  deployed                 {r['deployed']:.2f} SOL")
    print(f"  actual (qsim booked)     {r['actual']:+.4f} SOL   "
          f"{100*r['actual']/r['deployed']:+.2f}%/SOL")
    print(f"  partial runner           {r['policy']:+.4f} SOL   "
          f"{100*r['policy']/r['deployed']:+.2f}%/SOL")
    print(f"  DIFFERENCE               {r['policy'] - r['actual']:+.4f} SOL")
    print()
    print("  how the runner closed:")
    for why, n in sorted(r["counts"].items(), key=lambda kv: -kv[1]):
        sub = [d for d in r["deltas"] if d[3] == why]
        print(f"    {why:<16} n={n:<5} delta {sum(d[0] for d in sub):+8.4f}")

    if detail:
        print()
        print(f"  {'symbol':<14}{'blended_x':>11}{'why':>16}{'peak':>9}{'delta':>10}")
        for d in sorted(r["deltas"], reverse=True)[:25]:
            print(f"  {d[1][:13]:<14}{d[2]:>11.3f}{d[3]:>16}{d[4]:>9.2f}{d[0]:>10.4f}")
        print("  ...")
        for d in sorted(r["deltas"])[:10]:
            print(f"  {d[1][:13]:<14}{d[2]:>11.3f}{d[3]:>16}{d[4]:>9.2f}{d[0]:>10.4f}")

    print()
    print("  A TARGET is robust to sparse quoting in a way a trail is not: a")
    print("  missed quote can only hide a target that happened, never invent")
    print("  one. So this is a FLOOR on the policy, not an estimate.")


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--days", type=float, default=17.0)
    ap.add_argument("--bank", type=float, default=2.0)
    ap.add_argument("--keep", type=float, default=0.20,
                    help="fraction left running after the bank")
    ap.add_argument("--target", type=float, default=3.0,
                    help="runner's exit target (0 = no target)")
    ap.add_argument("--stop", type=float, default=0.80,
                    help="runner's stop as a multiple of entry; 0 = no stop at all")
    ap.add_argument("--since", metavar="YYYY-MM-DD",
                    help="start date instead of --days. Use 2026-10-07 or later: before "
                         "that qsim's post-exit path was mostly unobserved.")
    ap.add_argument("--max-hours", type=float, default=0.0,
                    help="sell the runner at the last quote within this many hours of "
                         "qsim's exit (0 = at the last quote, whenever that was)")
    ap.add_argument("--flag", help="only this security_flag (safe / warning / unknown / null)")
    ap.add_argument("--mcap-min", type=float, default=80000.0)
    ap.add_argument("--mcap-max", type=float, default=120000.0, help="0 = no mcap band")
    ap.add_argument("--all-lanes", action="store_true",
                    help="every channel, not just LIVE_LANES (research only)")
    ap.add_argument("--detail", action="store_true")
    ap.add_argument("--sweep", action="store_true",
                    help="grid over keep x target")
    a = ap.parse_args()
    try:
        report(a.days, a.bank, a.keep, a.target, a.stop, a.detail, a.sweep,
               since=a.since, all_lanes=a.all_lanes, mcap_min=a.mcap_min,
               mcap_max=a.mcap_max, flag=a.flag, max_hours=a.max_hours)
    finally:
        db.close_conn()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
