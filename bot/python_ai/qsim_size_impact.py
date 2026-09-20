"""
qsim_size_impact.py — how large can a position get before slippage eats the edge?

THE QUESTION
------------
Every qsim position is 0.05 SOL, so the book has exactly one point on the
capacity curve: a 2.42% median round trip (p25 is 0.9388, i.e. a QUARTER of
trades already cost 6.1% at the smallest size traded). One point cannot be
extrapolated, because round trip splits into a fixed part (Jupiter fee, pool
fee, spread) and a size-dependent part (price impact, roughly linear in size
against a constant-product pool), and 2.42% is consistent with both:

    all impact      -> 0.25 SOL costs ~12%, the edge is annihilated
    mostly fixed    -> 0.25 SOL costs ~3.4%, the edge survives at 5x the size

Same measurement, opposite conclusions. This measures the curve directly.

WHY IT MATTERS MORE THAN THE EDGE ITSELF
----------------------------------------
%/SOL is scale-invariant; profit is not. At +3%/SOL the difference between a
0.05 and a 0.5 SOL cap is 0.015 vs 0.15 SOL/day. Liquidity, not capital, is what
caps this strategy, so the capacity number decides whether it can ever be worth
running at all -- and it also decides what size a forward test should use.
Testing at 0.05 when 0.25 is equally liquid makes the experiment take five times
longer to reach significance.

METHOD
------
For each size S: buy quote (S SOL -> tokens), then sell quote (those tokens ->
SOL). round trip = sol_back / S. Both are QUOTES -- nothing is executed, no
position is opened, and qsim is not touched. It samples coins qsim has just
entered, so the liquidity is the liquidity the strategy actually faces rather
than whatever a survivor looks like hours later.

The comparison that matters is not cost(S). It is cost(S) - cost(0.05), because
the measured edge is ALREADY net of the 0.05 round trip. That marginal cost is
what scaling actually charges you, and --edge-pct prints the size at which it
overtakes the edge.

    python3 qsim_size_impact.py --once
    python3 qsim_size_impact.py --loop          # under pm2
    python3 qsim_size_impact.py --report --edge-pct 3
"""

from __future__ import annotations

import argparse
import asyncio
import os
import statistics
import sys
import time
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))

import db
import jupiter

DEFAULT_SIZES = [0.05, 0.1, 0.25, 0.5, 1.0, 2.0]


def ensure_table() -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS qsim_size_impact (
                id           bigserial PRIMARY KEY,
                call_id      integer NOT NULL,
                mint_address text,
                size_sol     numeric NOT NULL,
                tokens_out   numeric,
                sol_back     numeric,
                roundtrip    numeric,
                no_route     boolean NOT NULL DEFAULT false,
                observed_at  timestamptz NOT NULL DEFAULT now()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_size_impact_call "
                    "ON qsim_size_impact (call_id)")
    conn.commit()


def _targets(max_age_min: float, limit: int) -> list[dict[str, Any]]:
    """Recently-entered positions we have not sampled yet. Recency matters --
    these coins move fast and stale liquidity is not the liquidity we face."""
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT qp.call_id, t.mint_address, t.symbol
            FROM qsim_positions qp
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.entry_time >= now() - (%s || ' minutes')::interval
              AND t.mint_address IS NOT NULL
              AND t.mint_address NOT LIKE 'UNKNOWN:%%'
              AND NOT EXISTS (SELECT 1 FROM qsim_size_impact s
                              WHERE s.call_id = qp.call_id)
            ORDER BY qp.entry_time DESC
            LIMIT %s
        """, (max_age_min, limit))
        return [dict(r) for r in cur.fetchall()]


def _record(call_id: int, mint: str, size: float, tokens: int | None,
            sol_back: float | None) -> None:
    rt = (sol_back / size) if (sol_back and size > 0) else None
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO qsim_size_impact
                (call_id, mint_address, size_sol, tokens_out, sol_back, roundtrip, no_route)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (call_id, mint, size, tokens, sol_back, rt, tokens is None or not sol_back))
    conn.commit()


async def sample(sizes: list[float], max_age_min: float, limit: int,
                 delay: float) -> int:
    rows = _targets(max_age_min, limit)
    if not rows:
        return 0
    done = 0
    for r in rows:
        mint, cid = r["mint_address"], int(r["call_id"])
        for s in sizes:
            try:
                tokens = await jupiter.get_buy_quote(mint, s)
                sol_back = None
                if tokens:
                    sol_back = await jupiter.get_sell_quote(mint, int(tokens))
                _record(cid, mint, s, tokens, sol_back)
            except Exception as e:
                print(f"[size] {r.get('symbol','?')} @{s}: {type(e).__name__} {e}",
                      flush=True)
            if delay:
                await asyncio.sleep(delay)
        done += 1
        print(f"[size] sampled {r.get('symbol','?')} call_id={cid}", flush=True)
    return done


def report(edge_pct: float) -> None:
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT size_sol, roundtrip
            FROM qsim_size_impact
            WHERE roundtrip IS NOT NULL AND roundtrip > 0
        """)
        rows = [dict(r) for r in cur.fetchall()]
        cur.execute("SELECT size_sol, count(*) FROM qsim_size_impact "
                    "WHERE no_route GROUP BY size_sol ORDER BY size_sol")
        noroute = dict(cur.fetchall())
    if not rows:
        print("no samples yet — run --once or --loop first")
        return

    by: dict[float, list[float]] = {}
    for r in rows:
        by.setdefault(float(r["size_sol"]), []).append(float(r["roundtrip"]))

    base = None
    print(f"{'size':>7}{'n':>7}{'noroute':>9}{'p50_rt':>9}{'p25_rt':>9}"
          f"{'cost%':>8}{'marginal%':>11}")
    print("-" * 60)
    for s in sorted(by):
        v = sorted(by[s])
        p50 = statistics.median(v)
        p25 = v[int(0.25 * len(v))]
        cost = 100.0 * (1.0 - p50)
        if base is None:
            base = cost
        print(f"{s:>7.2f}{len(v):>7}{int(noroute.get(s, 0)):>9}{p50:>9.4f}"
              f"{p25:>9.4f}{cost:>8.2f}{cost - base:>11.2f}")

    print()
    print(f"  marginal% is cost(S) - cost(smallest). THAT is what scaling charges,")
    print(f"  because the measured edge is already net of the 0.05 round trip.")
    print(f"  With an edge of {edge_pct:.1f}%/SOL, the largest workable size is the")
    print(f"  last row whose marginal% stays below {edge_pct:.1f}.")
    print()
    print("  Watch p25 as well as p50. A quarter of coins are much thinner than the")
    print("  median, and at size they are where the strategy actually bleeds —")
    print("  a per-coin size cap keyed to liquidity may beat one global size.")
    print("  noroute counts sizes the pool could not fill at all: a hard ceiling,")
    print("  not a cost.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--loop", action="store_true")
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--sizes", default=",".join(str(s) for s in DEFAULT_SIZES))
    ap.add_argument("--max-age-min", type=float, default=10.0,
                    help="only sample positions entered this recently (default 10)")
    ap.add_argument("--limit", type=int, default=5,
                    help="positions per pass (default 5)")
    ap.add_argument("--delay", type=float, default=1.5,
                    help="seconds between quotes — this shares Jupiter's budget "
                         "with qsim, so keep it gentle (default 1.5)")
    ap.add_argument("--interval", type=float, default=60.0)
    ap.add_argument("--edge-pct", type=float, default=3.0)
    args = ap.parse_args()

    ensure_table()
    sizes = [float(x) for x in args.sizes.split(",") if x.strip()]

    if args.report:
        report(args.edge_pct)
        return 0
    if args.once:
        n = asyncio.run(sample(sizes, args.max_age_min, args.limit, args.delay))
        print(f"sampled {n} positions", flush=True)
        report(args.edge_pct)
        return 0
    if args.loop:
        print(f"[size] loop: sizes={sizes} every {args.interval}s", flush=True)
        while True:
            try:
                asyncio.run(sample(sizes, args.max_age_min, args.limit, args.delay))
            except Exception as e:
                print(f"[size] pass failed: {type(e).__name__} {e}", flush=True)
            time.sleep(args.interval)
    ap.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
