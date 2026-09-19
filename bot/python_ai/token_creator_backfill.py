"""
token_creator_backfill.py — who DEPLOYED each token, and do deployers repeat?

WHY THIS EXISTS
---------------
"Has this dev launched a slew of dead coins" is the one entry signal in this
project that has never been tested. The Telegram route is a dead end: the
channels emitted a 'Made: N | Bond: N | Best: $X' line through April 2026 and
stopped in May, so dev_tokens_made exists for ~2,481 tokens from March/April and
is NULL for everything since -- and qsim, the only honest outcome ruler we have,
did not exist until July. The feature and the ruler never overlap.

The chain does not have that problem. A token's deployer is permanent and
readable today for every mint we have ever seen, so backfilling it gives us the
feature for the SAME tokens qsim priced honestly. That is the whole point.

THE QUESTION THIS ANSWERS FIRST (and it may kill the idea outright)
-------------------------------------------------------------------
Serial ruggers frequently deploy from a fresh wallet every single time. If all
~3,800 of our tokens have ~3,800 distinct creators, then "this dev has a history"
is unmeasurable no matter how good the intuition is -- there is no history to
attach to. --report answers that before any feature gets built.

NO LOOK-AHEAD IN THE FEATURE
----------------------------
A creator's track record is counted ONLY from tokens first called strictly
BEFORE the token being scored. Counting a deployer's later rugs against an
earlier call would be look-ahead, and it would make this feature look far better
than it is -- the same error class as --include-post-exit.

THIS SCRIPT WRITES
------------------
It creates ONE side table (token_creators) and fills it. It does not alter
`tokens` -- the bot's DB role does not own that table -- and it never updates or
deletes anything else. --report and --dry-run write nothing at all.

    python3 token_creator_backfill.py --report
    python3 token_creator_backfill.py --limit 200 --dry-run
    python3 token_creator_backfill.py --limit 4000
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from collections import Counter, defaultdict
from typing import Any

import requests

sys.path.insert(0, os.path.dirname(__file__))

import db
import rpc_pool

SOLANA_RPC_URL = os.getenv("SOLANA_RPC_URL", "")
TIMEOUT = float(os.getenv("CREATOR_RPC_TIMEOUT", "15"))

# Programs that own a mint but are NOT the human deployer. pump.fun tokens list
# the bonding-curve program as an authority, so an unfiltered "authority" read
# would collapse thousands of unrelated tokens onto one address and invent a
# creator with a vast fake track record.
NOT_A_DEPLOYER = {
    "TSLvdd1pWpHVjahSpsvCXUbgwsL3JAcvokwaKt1eokM",   # pump.fun authority
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",   # pump.fun fee
    "11111111111111111111111111111111",              # system program
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",   # SPL token program
}


def _rpc(payload: dict) -> dict | None:
    url = rpc_pool.http_url() or SOLANA_RPC_URL
    if not url:
        return None
    try:
        r = requests.post(url, json=payload, timeout=TIMEOUT)
    except requests.RequestException:
        return None
    if r.status_code != 200:
        if hasattr(rpc_pool, "is_quota_error") and rpc_pool.is_quota_error(
                r.status_code, r.text[:300] if r.content else None):
            rpc_pool.penalize(url)
        return None
    try:
        return r.json()
    except ValueError:
        return None


def creator_via_das(mint: str) -> tuple[str | None, str]:
    """Helius DAS getAsset. For pump.fun mints the dev is normally creators[0]."""
    data = _rpc({"jsonrpc": "2.0", "id": 1, "method": "getAsset",
                 "params": {"id": mint}})
    res = (data or {}).get("result") or {}
    for c in res.get("creators") or []:
        addr = c.get("address")
        if addr and addr not in NOT_A_DEPLOYER:
            return addr, "das_creators"
    auth = res.get("authorities") or []
    for a in auth:
        addr = a.get("address")
        if addr and addr not in NOT_A_DEPLOYER:
            return addr, "das_authority"
    return None, ""


def creator_via_first_tx(mint: str) -> tuple[str | None, str]:
    """Oldest signature on the mint; its fee payer deployed it. Two calls, so
    this is the fallback rather than the default."""
    sigs = _rpc({"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress",
                 "params": [mint, {"limit": 1000}]})
    arr = (sigs or {}).get("result") or []
    if not arr:
        return None, ""
    oldest = arr[-1].get("signature")
    if not oldest:
        return None, ""
    tx = _rpc({"jsonrpc": "2.0", "id": 1, "method": "getTransaction",
               "params": [oldest, {"maxSupportedTransactionVersion": 0,
                                   "encoding": "jsonParsed"}]})
    res = (tx or {}).get("result") or {}
    keys = (((res.get("transaction") or {}).get("message") or {})
            .get("accountKeys") or [])
    for k in keys:
        addr = k.get("pubkey") if isinstance(k, dict) else k
        signer = k.get("signer") if isinstance(k, dict) else True
        if signer and addr and addr not in NOT_A_DEPLOYER:
            return addr, "first_tx"
    return None, ""


def ensure_table() -> None:
    """Side table, NOT a column on `tokens`.

    The bot's DB role owns what it creates but is not the owner of `tokens`, so
    ALTER TABLE there fails with InsufficientPrivilege. A side table needs no
    ownership, touches no schema other processes read, and can be dropped
    without consequence if this line of work dies.
    """
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS token_creators (
                token_id        integer PRIMARY KEY,
                mint_address    text,
                creator_address text,
                creator_source  text,
                resolved_at     timestamptz NOT NULL DEFAULT now()
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_token_creators_creator "
                    "ON token_creators (creator_address)")
    conn.commit()


def backfill(limit: int, rps: float, dry_run: bool, order: str, called_only: bool) -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT t.id, t.mint_address, t.symbol
            FROM tokens t
            LEFT JOIN token_creators tc ON tc.token_id = t.id
            WHERE tc.token_id IS NULL
              AND t.mint_address IS NOT NULL
              AND t.mint_address NOT LIKE 'UNKNOWN:%%'
              {called}
            ORDER BY t.id {order}
            LIMIT %s
        """.format(order=('ASC' if order == 'asc' else 'DESC'),
                   called=('AND EXISTS (SELECT 1 FROM calls c WHERE c.token_id = t.id)'
                           if called_only else '')), (limit,))
        todo = cur.fetchall()

    print(f"{len(todo)} tokens need a creator" + (" (dry run)" if dry_run else ""))
    ok = miss = 0
    delay = 1.0 / rps if rps > 0 else 0.0
    for i, (tid, mint, sym) in enumerate(todo, 1):
        addr, src = creator_via_das(mint)
        if not addr:
            addr, src = creator_via_first_tx(mint)
        if addr:
            ok += 1
        else:
            miss += 1
        if not dry_run:
            # Unresolved mints are recorded too, so a re-run resumes instead of
            # re-querying every failure. To retry them:
            #   DELETE FROM token_creators WHERE creator_address IS NULL;
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO token_creators
                        (token_id, mint_address, creator_address, creator_source)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (token_id) DO UPDATE
                        SET creator_address = EXCLUDED.creator_address,
                            creator_source  = EXCLUDED.creator_source,
                            resolved_at     = now()
                """, (tid, mint, addr, src or "unresolved"))
            conn.commit()
        if i % 100 == 0:
            print(f"  {i}/{len(todo)}  resolved={ok} unresolved={miss}")
        if delay:
            time.sleep(delay)
    print(f"done: resolved {ok}, unresolved {miss}")


REPORT_SQL = """
SELECT t.id                       AS token_id,
       tc.creator_address         AS creator,
       min(c.created_at)          AS first_call,
       max(qp.pnl_pct)            AS best_pnl_pct,
       bool_or(qp.pnl_pct <= -80) AS rugged,
       count(qp.call_id)          AS qsim_trades
FROM tokens t
JOIN token_creators tc ON tc.token_id = t.id
JOIN calls c ON c.token_id = t.id
LEFT JOIN qsim_positions qp ON qp.call_id = c.id AND qp.status = 'closed'
WHERE tc.creator_address IS NOT NULL
GROUP BY t.id, tc.creator_address
"""


def report() -> None:
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT count(*),
                   count(tc.creator_address)
            FROM tokens t
            LEFT JOIN token_creators tc ON tc.token_id = t.id
            WHERE t.mint_address NOT LIKE 'UNKNOWN:%%'
        """)
        total, have = cur.fetchone()
    print(f"tokens {total}, creator resolved {have} ({100 * have / total if total else 0:.1f}%)")
    if not have:
        print("nothing to report — run the backfill first")
        return

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(REPORT_SQL)
        rows = [dict(r) for r in cur.fetchall()]

    per = Counter(r["creator"] for r in rows)
    sizes = sorted(per.values())
    repeats = sum(v for v in per.values() if v > 1)
    print(f"\ndistinct creators        {len(per)}")
    print(f"tokens per creator       p50 {statistics.median(sizes):.0f}  "
          f"p90 {sizes[int(0.9 * len(sizes)) - 1]}  max {max(sizes)}")
    top = per.most_common(5)
    print("\ntop creators by token count (a human dev does not deploy hundreds —")
    print("  anything in the hundreds is a launchpad/factory and must be excluded):")
    for addr, cnt in top:
        print(f"   {addr}  {cnt}")
    print()
    print(f"tokens by a REPEAT dev   {repeats} of {len(rows)} "
          f"({100 * repeats / len(rows):.1f}%)")
    print()
    if repeats / max(len(rows), 1) < 0.05:
        print("  -> Under 5% of tokens come from a deployer we have seen before.")
        print("     There is no track record to attach. This feature cannot work,")
        print("     and no amount of scoring logic changes that. Stop here.")
        return

    # ── prior track record, counted ONLY from strictly earlier tokens ────────
    by_creator: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_creator[r["creator"]].append(r)
    for v in by_creator.values():
        v.sort(key=lambda r: r["first_call"])

    buckets: dict[str, list[dict]] = defaultdict(list)
    for creator, toks in by_creator.items():
        prior_n = prior_rugs = 0
        for r in toks:
            if r["qsim_trades"]:
                key = ("0 prior" if prior_n == 0
                       else "1-2 prior, clean" if prior_rugs == 0 and prior_n <= 2
                       else "1-2 prior, rugged" if prior_n <= 2
                       else "3+ prior, clean" if prior_rugs == 0
                       else "3+ prior, rugged")
                buckets[key].append(r)
            prior_n += 1
            prior_rugs += 1 if r["rugged"] else 0

    print(f"{'prior history (as of the call)':<26}{'tokens':>8}{'rug%':>8}{'p50_best':>10}")
    print("-" * 52)
    for key in ("0 prior", "1-2 prior, clean", "1-2 prior, rugged",
                "3+ prior, clean", "3+ prior, rugged"):
        v = buckets.get(key) or []
        if not v:
            continue
        rug = 100.0 * sum(1 for r in v if r["rugged"]) / len(v)
        bests = [float(r["best_pnl_pct"]) for r in v if r["best_pnl_pct"] is not None]
        print(f"{key:<26}{len(v):>8}{rug:>8.1f}"
              f"{statistics.median(bests) if bests else float('nan'):>10.1f}")
    print()
    print("  Read the rug% column. A deployer's prior rugs are only useful if the")
    print("  'rugged' rows carry a materially higher rug rate than '0 prior'.")
    print("  Small buckets mean nothing — check the token counts before believing it.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--report", action="store_true", help="stats only, no fetching, no writes")
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--rps", type=float, default=10.0)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--order", choices=("asc", "desc"), default="asc",
                    help="asc = OLDEST first (default). Prior history lives in "
                         "earlier tokens, so a partial desc run leaves recent "
                         "calls looking like first-time devs and hides the signal.")
    ap.add_argument("--called-only", action="store_true",
                    help="only tokens that actually produced a call — cuts the "
                         "universe to what can ever carry an outcome")
    args = ap.parse_args()

    ensure_table()
    if args.report:
        report()
    else:
        backfill(args.limit, args.rps, args.dry_run, args.order, args.called_only)
        report()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
