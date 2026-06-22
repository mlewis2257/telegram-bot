"""
honeypot_check.py — is a shadow lane's profit REAL, or honeypot-trapped?

Shadow tracks PRICE, not whether you could SELL. The security_warning lane flags
exactly the coins that can be honeypots — where the price pumps in shadow but you'd be
trapped live. This pulls the lane's closed shadow trades, checks each coin's on-chain
freeze authority (the main Solana "can't sell" vector; Token-2022 transfer hooks too),
and splits the PnL into SELLABLE (authority renounced) vs TRAPPED — so you can tell
whether the lane's edge is real or phantom.

    python3 honeypot_check.py                         # security_warning, all time
    python3 honeypot_check.py --days 14
    python3 honeypot_check.py --skip-reason low_score # sanity-check a "real" lane

Read-only. Makes one cached getAccountInfo per unique coin (throttled).
"""
from __future__ import annotations

import argparse
import os
import sys
import time

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db
import data_fetcher

QUERY = """
SELECT tok.mint_address AS mint, tok.symbol AS symbol,
       sp.pnl_sol AS pnl_sol
FROM shadow_positions sp
JOIN calls c ON c.id = sp.call_id
LEFT JOIN tokens tok ON tok.id = sp.token_id
WHERE sp.status = 'closed'
  AND COALESCE(c.skip_reason, 'none') = %(skip)s
  AND tok.mint_address IS NOT NULL
  AND tok.mint_address NOT LIKE 'INFERRED:%%'
  AND tok.mint_address NOT LIKE 'UNKNOWN:%%'
  AND ( %(days)s = 0 OR sp.entry_time >= now() - (%(days)s || ' days')::interval )
  AND sp.peak_multiplier < 50 AND sp.pnl_pct BETWEEN -100.5 AND 5000   -- drop phantom rows
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=0, help="0 = all time")
    ap.add_argument("--skip-reason", default="security_warning")
    ap.add_argument("--delay-ms", type=int, default=120, help="throttle between RPC checks")
    args = ap.parse_args()

    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(QUERY, {"skip": args.skip_reason, "days": args.days})
        rows = cur.fetchall()

    if not rows:
        print(f"\nNo closed shadow trades for skip_reason={args.skip_reason} in window.\n")
        return

    # Aggregate per unique coin (one authority check covers all its trades).
    mints: dict[str, dict] = {}
    for r in rows:
        m = mints.setdefault(r["mint"], {"symbol": r["symbol"], "n": 0, "pnl": 0.0})
        m["n"] += 1
        m["pnl"] += float(r["pnl_sol"] or 0)

    print(f"\nChecking {len(mints)} unique coins in the '{args.skip_reason}' lane "
          f"({len(rows)} trades) for sellability...\n")

    buckets = {"sellable": [0, 0.0], "trapped": [0, 0.0], "unknown": [0, 0.0]}
    traps = []
    for mint, agg in mints.items():
        auth = data_fetcher.fetch_mint_authorities(mint)
        if args.delay_ms:
            time.sleep(args.delay_ms / 1000.0)
        if auth is None:
            buckets["unknown"][0] += agg["n"]; buckets["unknown"][1] += agg["pnl"]
            continue
        reasons = []
        if auth.get("freeze_authority"):
            reasons.append("freeze")
        if auth.get("is_token2022"):
            reasons.append("token2022")
        if reasons:
            buckets["trapped"][0] += agg["n"]; buckets["trapped"][1] += agg["pnl"]
            traps.append((agg["symbol"], mint, agg["n"], agg["pnl"], ",".join(reasons)))
        else:
            buckets["sellable"][0] += agg["n"]; buckets["sellable"][1] += agg["pnl"]

    total_pnl = sum(b[1] for b in buckets.values())
    print(f"=== '{args.skip_reason}' lane — sellability of the shadow PnL ===")
    print(f"  Total shadow PnL: {total_pnl:+.2f} SOL  ({len(rows)} trades, {len(mints)} coins)\n")
    print(f"  SELLABLE (authority renounced) : {buckets['sellable'][1]:+8.2f} SOL  "
          f"({buckets['sellable'][0]} trades)   <- real, you could exit")
    print(f"  TRAPPED  (freeze auth / t2022)  : {buckets['trapped'][1]:+8.2f} SOL  "
          f"({buckets['trapped'][0]} trades)   <- LIKELY un-realizable")
    print(f"  UNKNOWN  (no RPC data)          : {buckets['unknown'][1]:+8.2f} SOL  "
          f"({buckets['unknown'][0]} trades)\n")

    if traps:
        print("  Freeze-authority / Token-2022 coins (their 'gains' are suspect):")
        for sym, mint, n, pnl, why in sorted(traps, key=lambda t: -t[3]):
            print(f"    {(sym or '?')[:16]:<16} {mint[:8]}…  {n:>3} trades  {pnl:+7.2f} SOL  [{why}]")
        print()

    sellable_pnl = buckets["sellable"][1]
    trapped_pnl = buckets["trapped"][1]
    if total_pnl > 0:
        share = 100 * max(trapped_pnl, 0) / total_pnl if total_pnl else 0
        print(f"  => REAL (sellable) PnL is {sellable_pnl:+.2f} SOL.")
        if trapped_pnl > 0:
            print(f"     {share:.0f}% of the lane's positive PnL came from TRAPPED coins — "
                  f"strip those and the edge {'holds' if sellable_pnl > 0 else 'DISAPPEARS'}.")
    else:
        print(f"  => Lane is net-negative even before sellability ({total_pnl:+.2f}).")
    print()


if __name__ == "__main__":
    main()
