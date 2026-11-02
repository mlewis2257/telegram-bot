"""
dev_profit_sampler.py — what does a FRESH dev actually net on one launch, by dev-buy size,
when the coin got called vs when it did not?

WHY (2026-11-02). launch_outcomes.py showed bigger dev buys from fresh wallets get CALLED far
more often (0 SOL 0.9%, 0.5-1 SOL 7.5%, 3+ SOL 12.9%, both halves). Called is attention, not
profit: a 3 SOL dev buy is only worth it if the dev's real result on the called ones pays for
all the others. This reads the real result from the chain for a sample of launches per cell.

WHO. Launches from new_tokens whose dev made exactly ONE recorded launch (a fresh wallet, the
only kind we would launch from), created --min-age-hours ago or more so the outcome is settled.
Cells: dev buy (<0.5 / 0.5-1 / 1-3 / 3+ SOL) x called by our channels (yes / no).

HOW. Per launch: one getSignaturesForAddress on the dev wallet, then getTransaction for each of
the dev's transactions from creation to --window-hours after it (at most --max-tx per launch;
a dev with more is skipped as 'busy'). Each transaction's SOL change for the dev (fees, tips,
rent and WSOL included) is attributed with dev_launch_audit.classify. Per launch:
  in / out / net   SOL the dev put in and took out on this coin, and out - in
  fees             creator-fee claims in the window
  holding          the dev still held tokens at the end of the window (net is then realised only)

RPC. Hard ceiling --max-calls (default 1500), printed at start and end; the run stops cleanly
when it is reached. Worst case per launch = 1 + --max-tx calls; fresh devs typically use 3-6.

Read-only. Run on the VPS:
  python3 bot/launcher/dev_profit_sampler.py [--per-cell 15] [--max-calls 1500] [--since 2026-10-31]
"""

from __future__ import annotations

import argparse
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / "python_ai"))
import dev_launch_audit as d  # noqa: E402  (loads .env, owns the call counter + ceiling)
import db  # noqa: E402

BUCKETS = [("<0.5", 0.0001, 0.5), ("0.5-1", 0.5, 1.0), ("1-3", 1.0, 3.0), ("3+", 3.0, 1e9)]


def get_tx(sig: str):
    # Version 0 only: dev_launch_audit.get_tx tries an invalid version first, doubling the cost.
    return d.rpc("getTransaction", [sig, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0,
                                          "commitment": "confirmed"}])


def sample(since: str, min_age_h: float, per_cell: int, only: bool | None = None) -> list[dict]:
    conn = db.get_conn()
    out = []
    with conn.cursor() as cur:
        for name, lo, hi in BUCKETS:
            for called in ((True, False) if only is None else (only,)):
                cur.execute(
                    """
                    WITH fresh AS (
                        SELECT dev FROM new_tokens WHERE dev IS NOT NULL GROUP BY dev HAVING count(*) = 1)
                    SELECT nt.mint, nt.name, nt.dev, nt.dev_buy_sol, nt.created_at, nt.migrated_at
                    FROM new_tokens nt JOIN fresh f ON f.dev = nt.dev
                    WHERE nt.created_at >= %s
                      AND nt.created_at < now() - (%s || ' hours')::interval
                      AND nt.dev_buy_sol >= %s AND nt.dev_buy_sol < %s
                      AND EXISTS (SELECT 1 FROM tokens t JOIN calls c ON c.token_id = t.id
                                   WHERE t.mint_address = nt.mint) = %s
                    ORDER BY md5(nt.mint)          -- deterministic 'random' sample
                    LIMIT %s
                    """, (since, min_age_h, lo, hi, called, per_cell))
                for r in cur.fetchall():
                    out.append(dict(zip(("mint", "name", "dev", "dev_buy", "t", "migrated_at"), r),
                                    cell=name, called=called))
    return out


def read_launch(c: dict, window_h: float, max_tx: int) -> dict:
    t0 = int(c["t"].timestamp()) - 120
    t1 = int(c["t"].timestamp() + window_h * 3600)
    # A wallet that created a coin always has history: an empty/None answer is an RPC error or
    # rate limit (the first run lost 35 of 120 launches to it). Retry twice, spaced out.
    sigs = None
    for attempt in range(3):
        sigs = d.rpc("getSignaturesForAddress", [c["dev"], {"limit": 1000}])
        if sigs:
            break
        time.sleep(1.5 * (attempt + 1))
    if not sigs:
        return {"status": "rpc error"}
    if len(sigs) == 1000 and (sigs[-1].get("blockTime") or 0) > t0:
        return {"status": "busy"}                  # launch is further back than one page
    mine = [s for s in sigs if s.get("err") is None and s.get("blockTime") and t0 <= s["blockTime"] <= t1]
    if len(mine) > max_tx:
        return {"status": "busy"}
    dev_in = dev_out = fees = 0.0
    held = 0.0
    created = False
    for s in sorted(mine, key=lambda s: s["blockTime"]):
        tx = get_tx(s["signature"])
        if not tx:
            continue
        r = d.classify(tx, c["dev"])
        if not r:
            continue
        if r["action"] == "fee_claim":
            fees += max(0.0, r["sol"])
            continue
        if r["mint"] != c["mint"] and c["mint"] not in r["toks"]:
            continue                               # the dev's other business
        created = created or r["action"] == "create"
        if r["sol"] < 0:
            dev_in += -r["sol"]
        else:
            dev_out += r["sol"]
        held += r["toks"].get(c["mint"], 0.0)
    if not created:
        return {"status": "create not found"}
    return {"status": "ok", "in": dev_in, "out": dev_out, "fees": fees,
            "net": dev_out + fees - dev_in, "holding": held > 1.0}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", default="2026-10-31")
    ap.add_argument("--per-cell", type=int, default=15)
    ap.add_argument("--max-calls", type=int, default=1500)
    ap.add_argument("--max-tx", type=int, default=12, help="skip a dev with more txs than this in the window")
    ap.add_argument("--only", choices=("called", "not-called"),
                    help="sample one side only (the called cells need a higher --max-tx: those devs trade more)")
    ap.add_argument("--window-hours", type=float, default=24.0)
    ap.add_argument("--min-age-hours", type=float, default=24.0)
    a = ap.parse_args()
    d._max_calls = a.max_calls

    only = None if a.only is None else (a.only == "called")
    launches = sample(a.since, a.min_age_hours, a.per_cell, only)
    worst = len(launches) * (1 + a.max_tx)
    print(f"[sampler] {len(launches)} launches ({a.per_cell} per cell x {len(BUCKETS) * (1 if a.only else 2)} cells). "
          f"RPC ceiling {a.max_calls:,} calls (worst case for this sample {worst:,}; "
          f"typical ~{len(launches) * 5:,}). Stops cleanly at the ceiling.", flush=True)

    results = []
    try:
        for i, c in enumerate(launches, 1):
            c.update(read_launch(c, a.window_hours, a.max_tx))
            results.append(c)
            if i % 20 == 0:
                print(f"[sampler] {i}/{len(launches)} read, {d._calls} calls", flush=True)
    except d.Ceiling:
        print(f"[sampler] STOPPED at the {a.max_calls:,}-call ceiling after {len(results)} launches")

    sol_usd = d.data_fetcher.get_sol_price_usd() or 0.0
    print(f"\nRPC calls used: {d._calls:,} / {a.max_calls:,}. SOL ${sol_usd:.0f}. "
          f"Fresh devs only; net = out + creator fees - in, over {a.window_hours:g} h after creation.\n")
    print(f"  {'dev buy':8s} {'called':6s} {'read':>4s} {'skip':>4s} {'median net':>10s} {'mean net':>9s} "
          f"{'profitable':>10s} {'still hold':>10s} {'mean in':>8s} {'grad':>5s}")
    cells = defaultdict(list)
    for r in results:
        cells[(r["cell"], r["called"])].append(r)
    for name, _, _ in BUCKETS:
        for called in (True, False):
            g = cells.get((name, called), [])
            ok = [r for r in g if r.get("status") == "ok"]
            skip = len(g) - len(ok)
            if not ok:
                print(f"  {name:8s} {('yes' if called else 'no'):6s} {0:4d} {skip:4d}")
                continue
            nets = [r["net"] for r in ok]
            print(f"  {name:8s} {('yes' if called else 'no'):6s} {len(ok):4d} {skip:4d} "
                  f"{statistics.median(nets):+10.3f} {statistics.mean(nets):+9.3f} "
                  f"{sum(n > 0 for n in nets) / len(ok) * 100:9.0f}% "
                  f"{sum(r['holding'] for r in ok) / len(ok) * 100:9.0f}% "
                  f"{statistics.mean(r['in'] for r in ok):8.2f} "
                  f"{sum(r['migrated_at'] is not None for r in ok):5d}")
    skipped = defaultdict(int)
    for r in results:
        if r.get("status") != "ok":
            skipped[r.get("status")] += 1
    if skipped:
        print(f"\nskipped: {dict(skipped)}")
    print("\nTo price a strategy, weight each dev-buy row by the called rate from launch_outcomes.py:\n"
          "  expected net per launch = called% x mean(called) + (1 - called%) x mean(not called).\n"
          "Rows with 'still hold' are realised only (an unsold bag is counted at 0).")


if __name__ == "__main__":
    main()
