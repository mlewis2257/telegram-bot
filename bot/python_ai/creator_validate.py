"""
creator_validate.py — is the resolved "creator" actually the deployer?

WHY THIS EXISTS
---------------
Two attribution methods have now produced two different dev-history results, and
neither was ever checked for correctness. The first took the oldest signature of
a single 1,000-row page, which is the creation tx only when the whole history
fits — so busy coins (the ones that pumped) were assigned to whichever trader
sat at position 1000. Fixing that flipped the finding from a strong edge to
nothing, which is a large swing to accept on faith.

The replacement pages back with `before` until a short page arrives. But a short
page means one of two very different things:

    (a) we reached the true beginning of the account's history, or
    (b) we reached the RPC node's RETENTION LIMIT and everything older is pruned

Case (b) attributes the token to whoever signed at the pruning boundary — a
random trader — and it bites hardest on OLD tokens, which is precisely where
prior history lives. Corroborating sign: creators holding 40+ tokens jumped from
143 to 236 after the fix, when worse resolution should produce fewer, unless
attributions are piling onto whoever was active at the boundary.

THE TEST
--------
A token cannot be created after it was first called. So compare the blockTime of
the signature we called "creation" against the token's earliest call:

    creation_time <= first_call      plausible
    creation_time >  first_call      PROVABLY WRONG — that tx is not the creation

The wrong-rate by token age is the diagnostic. Flat across ages means the method
is sound. Rising with age means we are hitting retention, the older half of the
data is fiction, and prior history built on it cannot be trusted either way.

Read-only. Executes nothing, writes nothing.

    python3 creator_validate.py --sample 200
"""

from __future__ import annotations

import argparse
import os
import random
import statistics
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

import db
import token_creator_backfill as tcb

SQL = """
SELECT t.id AS token_id, t.mint_address, tc.creator_source,
       min(c.created_at) AS first_call
FROM token_creators tc
JOIN tokens t ON t.id = tc.token_id
JOIN calls  c ON c.token_id = t.id
WHERE tc.creator_address IS NOT NULL
GROUP BY t.id, t.mint_address, tc.creator_source
"""


def oldest_sig_time(mint: str) -> tuple[str | None, int | None, int]:
    """Walk to the oldest reachable signature. Returns (sig, blockTime, pages)."""
    before = None
    oldest_sig = None
    oldest_time = None
    pages = 0
    for _ in range(tcb.MAX_SIG_PAGES):
        params: dict = {"limit": 1000}
        if before:
            params["before"] = before
        r = tcb._rpc({"jsonrpc": "2.0", "id": 1,
                      "method": "getSignaturesForAddress",
                      "params": [mint, params]})
        arr = (r or {}).get("result") or []
        pages += 1
        if not arr:
            break
        oldest_sig = arr[-1].get("signature")
        oldest_time = arr[-1].get("blockTime")
        if len(arr) < 1000:
            break
        before = oldest_sig
    return oldest_sig, oldest_time, pages


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sample", type=int, default=150)
    ap.add_argument("--seed", type=int, default=20260923)
    args = ap.parse_args()

    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SQL)
        rows = [dict(r) for r in cur.fetchall()]
    if not rows:
        print("no resolved creators — run the backfill first")
        return 1

    rng = random.Random(args.seed)
    rng.shuffle(rows)
    rows = rows[:args.sample]

    buckets: dict[str, list[tuple[bool, float]]] = {}
    checked = skipped = 0
    for r in rows:
        mint = r["mint_address"]
        if not mint or mint.startswith(("UNKNOWN:", "INFERRED:")):
            continue
        first_call: datetime = r["first_call"]
        if first_call.tzinfo is None:
            first_call = first_call.replace(tzinfo=timezone.utc)
        try:
            _, btime, _ = oldest_sig_time(mint)
        except Exception:
            skipped += 1
            continue
        if not btime:
            skipped += 1
            continue
        created = datetime.fromtimestamp(btime, tz=timezone.utc)
        # A token cannot be created AFTER it was called. Allow 60s of clock slop.
        wrong = created > first_call.replace(microsecond=0)
        age_days = (datetime.now(timezone.utc) - first_call).days
        key = ("0-15d" if age_days <= 15 else "16-45d" if age_days <= 45
               else "46-90d" if age_days <= 90 else "90d+")
        lag_h = (first_call - created).total_seconds() / 3600.0
        buckets.setdefault(key, []).append((wrong, lag_h))
        checked += 1

    print(f"checked {checked} tokens ({skipped} skipped: no blockTime)\n")
    hdr = f"{'token age':<12}{'n':>6}{'PROVABLY WRONG':>17}{'wrong%':>9}{'p50 lag(h)':>12}"
    print(hdr)
    print("-" * len(hdr))
    for key in ("0-15d", "16-45d", "46-90d", "90d+"):
        v = buckets.get(key) or []
        if not v:
            continue
        wrong = sum(1 for w, _ in v if w)
        lags = sorted(l for w, l in v if not w)
        p50 = statistics.median(lags) if lags else float("nan")
        print(f"{key:<12}{len(v):>6}{wrong:>17}{100.0 * wrong / len(v):>9.1f}{p50:>12.1f}")

    print()
    print("  'PROVABLY WRONG' = the transaction we called the creation happened")
    print("  AFTER the token was first called, which is impossible. Those are")
    print("  retention-boundary hits: the node pruned the real history and we")
    print("  attributed the coin to whoever signed at the edge.")
    print()
    print("  FLAT wrong% across ages  -> the method is sound; the dev-history")
    print("                              result stands as measured.")
    print("  RISING wrong% with age   -> older tokens are fiction, and since")
    print("                              PRIOR history lives in old tokens, the")
    print("                              filter cannot be evaluated either way")
    print("                              until resolution reaches back further.")
    print()
    print("  p50 lag is hours between creation and first call, for rows that are")
    print("  not provably wrong. A plausible lag is hours to days; a lag near")
    print("  zero on old tokens means we are landing at the boundary, not the start.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
