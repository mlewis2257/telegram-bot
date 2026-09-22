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

import random
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
    # Found by --report: these hold tens to hundreds of "their own" tokens, which
    # no human deployer does. Left in, they alone drive the repeat-dev bucket.
    "WLHv2UAZm6z4KyaaELi5pjdbJh6RESMva1Rnn8pJVVh",   # 829 tokens, BOTH authority+creator
    "2tgUbS9UMoQD6GkDZBiqKYCURnGrSb6ocYwRABrSJUvY",  # 247, fee payer
    "AgmLJBMDCqWynYnQiPCuj9ewsNNsBJXyzoUhD9LJzN51",  # 191, fee payer
    "gasTzr94Pmp4Gf8vknQnqxeYxdgwFjbgdJa4msYRpnB",   # 68, gas relayer (vanity 'gas')
    "BAGSB9TpGrZxQbEsrEznv5jXXdwyP6AXerN8aVRiAmcv",  # 62, Bags launchpad authority
}

# A launchpad pays the deploy fee for its users, so the first-tx fee payer is
# NOT reliably the dev. Any address that "deploys" more than this many tokens in
# our sample is infrastructure, and the report drops it rather than letting it
# masquerade as an experienced deployer.
FACTORY_MIN_TOKENS = int(os.getenv("CREATOR_FACTORY_MIN", "40"))


class RpcFail(Exception):
    """The RPC could not answer — rate limit, timeout, 5xx, bad JSON.

    LOAD-BEARING. Before this existed, a 429 was indistinguishable from "this
    mint genuinely has no creator", so throttled lookups were WRITTEN to the
    table as permanently unresolved and skipped on every later run. Raising
    the rate from 10 to 20 rps dropped resolution from 98.4% to 38.7% and holed
    4,108 rows that way. A transient failure must never be persisted.
    """


def _helius_url() -> str | None:
    """A HELIUS endpoint specifically.

    getAsset is a Helius extension, but rpc_pool rotates across generic Solana
    RPCs too, which reject it. That is why DAS resolved only 3.4% of tokens and
    almost everything fell through to the slow two-call first_tx path — which in
    turn timed out on 46% of live gate decisions.
    """
    keys = [k.strip() for k in os.getenv("HELIUS_API_KEYS", "").split(",") if k.strip()]
    if keys:
        return f"https://mainnet.helius-rpc.com/?api-key={random.choice(keys)}"
    for env in ("SOLANA_RPC_URL", "SOLANA_RPC_URLS"):
        for u in os.getenv(env, "").split(","):
            if "helius" in u:
                return u.strip()
    return None


def _rpc(payload: dict, retries: int = 4, url_override: str | None = None) -> dict:
    backoff = 1.0
    last = "no endpoint"
    for _ in range(retries):
        url = url_override or rpc_pool.http_url() or SOLANA_RPC_URL
        if not url:
            raise RpcFail("no RPC endpoint configured")
        try:
            r = requests.post(url, json=payload, timeout=TIMEOUT)
        except requests.RequestException as e:
            last = f"transport {type(e).__name__}"
            time.sleep(backoff)
            backoff *= 2
            continue
        if r.status_code == 429 or r.status_code >= 500:
            if hasattr(rpc_pool, "is_quota_error") and rpc_pool.is_quota_error(
                    r.status_code, r.text[:300] if r.content else None):
                rpc_pool.penalize(url)
            last = f"http {r.status_code}"
            time.sleep(backoff)
            backoff *= 2
            continue
        if r.status_code != 200:
            raise RpcFail(f"http {r.status_code}")
        try:
            return r.json()
        except ValueError:
            last = "bad json"
            time.sleep(backoff)
            backoff *= 2
    raise RpcFail(last)


def creator_via_das(mint: str) -> tuple[str | None, str]:
    """Helius DAS getAsset. For pump.fun mints the dev is normally creators[0]."""
    hel = _helius_url()
    if not hel:
        return None, ""          # no Helius endpoint: DAS cannot work at all
    data = _rpc({"jsonrpc": "2.0", "id": 1, "method": "getAsset",
                 "params": {"id": mint}}, url_override=hel)
    res = (data or {}).get("result") or {}   # valid reply, possibly empty
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


# pump.fun has moved its API host more than once and fronts it with Cloudflare,
# which returns 530 to some origins. Try each in turn; this is an OPTIMISATION,
# never a requirement.
PUMPFUN_HOSTS = [h for h in os.getenv(
    "PUMPFUN_API",
    "https://frontend-api-v3.pump.fun/coins,"
    "https://frontend-api-v2.pump.fun/coins,"
    "https://frontend-api.pump.fun/coins",
).split(",") if h.strip()]
_PUMPFUN_DEAD = False


METAPLEX_PROGRAM = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"


def _metadata_pda(mint: str) -> str | None:
    """Metaplex metadata PDA: ['metadata', program, mint]."""
    try:
        from solders.pubkey import Pubkey
    except ImportError:
        return None
    try:
        mp = Pubkey.from_string(METAPLEX_PROGRAM)
        pda, _ = Pubkey.find_program_address(
            [b"metadata", bytes(mp), bytes(Pubkey.from_string(mint))], mp)
        return str(pda)
    except Exception:
        return None


def _signer_of(signature: str) -> str | None:
    """Fee payer of a transaction — the first signing account."""
    tx = _rpc({"jsonrpc": "2.0", "id": 1, "method": "getTransaction",
               "params": [signature, {"maxSupportedTransactionVersion": 0,
                                      "encoding": "jsonParsed"}]})
    keys = ((((tx or {}).get("result") or {}).get("transaction") or {})
            .get("message") or {}).get("accountKeys") or []
    for k in keys:
        addr = k.get("pubkey") if isinstance(k, dict) else k
        signer = k.get("signer") if isinstance(k, dict) else True
        if signer and addr and addr not in NOT_A_DEPLOYER:
            return addr
    return None


def creator_via_metadata(mint: str) -> tuple[str | None, str]:
    """The deployer, via the token's METADATA account rather than the mint.

    The mint itself accumulates a signature for every trade — the coin above has
    1,000+ and needs deep paging, which is slow and fails on exactly the active
    coins that matter. Its Metaplex metadata account is created in the SAME
    transaction but is almost never touched again, so its oldest signature sits
    in a short first page. Two calls, no paging, and it works for any SPL token
    with metadata rather than only pump.fun mints.

    DAS cannot answer this: getAsset returns creators:[] and authorities:[] for
    these tokens (verified), so it is not a fallback, it is a dead end.
    """
    pda = _metadata_pda(mint)
    if not pda:
        return None, ""
    sigs = _rpc({"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress",
                 "params": [pda, {"limit": 1000}]})
    arr = (sigs or {}).get("result") or []
    if not arr or len(arr) >= 1000:
        return None, ""          # no metadata, or unexpectedly busy — don't guess
    oldest = arr[-1].get("signature")
    if not oldest:
        return None, ""
    addr = _signer_of(oldest)
    return (addr, "metadata") if addr else (None, "")


def creator_via_pumpfun(mint: str) -> tuple[str | None, str]:
    """pump.fun publishes the deployer directly. ONE http call, no RPC, no
    paging — and fast enough for the live gate, which was timing out on 46% of
    decisions against the two-call RPC path.

    Most of this flow is pump.fun mints (they end in 'pump'), which is exactly
    the population DAS fails on: getAsset returns no usable creator for them,
    only the pump.fun program as an authority, which is not a deployer.
    """
    global _PUMPFUN_DEAD
    if _PUMPFUN_DEAD:
        return None, ""
    for host in PUMPFUN_HOSTS:
        try:
            r = requests.get(f"{host.strip()}/{mint}", timeout=TIMEOUT,
                             headers={"User-Agent": "Mozilla/5.0"})
        except requests.RequestException:
            continue
        if r.status_code == 404:
            return None, ""                  # genuinely not a pump.fun coin
        if r.status_code != 200:
            continue                         # 530/403/etc — try the next host
        try:
            addr = (r.json() or {}).get("creator")
        except ValueError:
            continue
        if addr and addr not in NOT_A_DEPLOYER:
            return addr, "pumpfun"
        return None, ""
    # NEVER raise. Raising made a blocked API look like a transient RPC fault,
    # so every pump mint was skipped and left permanently unresolved instead of
    # falling through to the RPC path that actually works.
    _PUMPFUN_DEAD = True
    print("[creator] pump.fun API unreachable — using RPC only for this run",
          flush=True)
    return None, ""


MAX_SIG_PAGES = int(os.getenv("CREATOR_MAX_SIG_PAGES", "30"))


def creator_via_first_tx(mint: str, max_pages: int = 0) -> tuple[str | None, str]:
    """The mint's OLDEST transaction; its signer deployed it.

    getSignaturesForAddress returns NEWEST first with no reverse option, so the
    creation tx is only in the first page when the whole history fits. Anything
    busier has to be paged with `before`. Taking arr[-1] of one page was wrong
    in exactly one direction: the coins with the most transactions are the ones
    that pumped, so the WINNERS were the tokens being misattributed to whichever
    trader happened to sit at position 1000.

    Capped at max_pages so one very active mint cannot stall the run; past that
    it returns nothing rather than guessing.
    """
    # Measured: a called coin runs 9k-20k signatures and needs 10-20 pages,
    # returning in 1.3-3.8s. The old cap of 6 gave up on every one of them —
    # and the coins with the most transactions are the ones that pumped, so it
    # failed on precisely the winners.
    before = None
    oldest = None
    for _ in range(max_pages or MAX_SIG_PAGES):
        params: dict = {"limit": 1000}
        if before:
            params["before"] = before
        sigs = _rpc({"jsonrpc": "2.0", "id": 1,
                     "method": "getSignaturesForAddress",
                     "params": [mint, params]})
        arr = (sigs or {}).get("result") or []
        if not arr:
            break
        oldest = arr[-1].get("signature")
        if len(arr) < 1000:
            break                            # reached the start of history
        before = oldest
    else:
        return None, ""                      # never reached the beginning
    if not oldest:
        return None, ""
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


def resolve_creator(mint: str) -> tuple[str | None, str]:
    """Cheapest reliable source first: pump.fun for pump mints, then DAS, then
    the paged first-transaction walk."""
    addr, src = creator_via_metadata(mint)      # cheapest and most general
    if addr:
        return addr, src
    if mint.endswith("pump"):
        addr, src = creator_via_pumpfun(mint)
        if addr:
            return addr, src
    addr, src = creator_via_das(mint)
    if addr:
        return addr, src
    return creator_via_first_tx(mint)


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


def backfill(limit: int, rps: float, dry_run: bool, order: str,
             called_only: bool, workers: int = 1) -> None:
    conn = db.get_conn()
    db.safe_rollback()
    print("selecting work (this query can take a minute on 107k tokens)...",
          flush=True)
    # --called-only used a correlated EXISTS, which on 107k tokens with no index
    # on calls.token_id is one scan per token and effectively hangs. A hash join
    # against a materialised DISTINCT set does the same job in seconds.
    with conn.cursor() as cur:
        cur.execute("""
            SELECT t.id, t.mint_address, t.symbol
            FROM tokens t
            {called}
            LEFT JOIN token_creators tc ON tc.token_id = t.id
            WHERE tc.token_id IS NULL
              AND t.mint_address IS NOT NULL
              AND t.mint_address NOT LIKE 'UNKNOWN:%%'
            ORDER BY t.id {order}
            LIMIT %s
        """.format(order=('ASC' if order == 'asc' else 'DESC'),
                   called=('JOIN (SELECT DISTINCT token_id FROM calls) cc '
                           'ON cc.token_id = t.id' if called_only else '')), (limit,))
        todo = cur.fetchall()

    print(f"{len(todo)} tokens need a creator"
          + (" (dry run)" if dry_run else "")
          + (f"  workers={workers}" if workers > 1 else ""), flush=True)
    ok = miss = transient = 0
    delay = 1.0 / rps if rps > 0 else 0.0

    if workers > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        done_n = 0
        for start in range(0, len(todo), 500):
            chunk = todo[start:start + 500]
            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs = {ex.submit(resolve_creator, m): (t, m) for t, m, _ in chunk}
                for fut in as_completed(futs):
                    tid, mint = futs[fut]
                    done_n += 1
                    try:
                        addr, src = fut.result()
                    except RpcFail:
                        transient += 1
                        continue
                    except Exception:
                        transient += 1
                        continue
                    ok += 1 if addr else 0
                    miss += 0 if addr else 1
                    if not dry_run:
                        # psycopg2 connections are NOT thread-safe — every write
                        # happens here, on the main thread, never in a worker.
                        _record(tid, mint, addr, src or "unresolved")
                    if done_n % 100 == 0:
                        print(f"  {done_n}/{len(todo)}  resolved={ok} "
                              f"unresolved={miss} transient={transient}", flush=True)
        print(f"done: resolved {ok}, genuinely unresolved {miss}, "
              f"transient RPC failures {transient} (not written — rerun to retry)",
              flush=True)
        return

    for i, (tid, mint, sym) in enumerate(todo, 1):
        try:
            addr, src = resolve_creator(mint)
        except RpcFail as e:
            # The node could not answer. That says nothing about the mint, so
            # write NOTHING and leave it for the next run.
            transient += 1
            if transient % 50 == 0:
                print(f"  ...{transient} transient RPC failures "
                      f"(last: {e}) — consider a lower --rps", flush=True)
            if delay:
                time.sleep(delay)
            continue
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
            print(f"  {i}/{len(todo)}  resolved={ok} unresolved={miss} "
                  f"transient={transient}", flush=True)
        if delay:
            time.sleep(delay)
    print(f"done: resolved {ok}, genuinely unresolved {miss}, "
          f"transient RPC failures {transient} (not written — rerun to retry)",
          flush=True)


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

    per_all = Counter(r["creator"] for r in rows)
    factories = {a for a, c in per_all.items() if c >= FACTORY_MIN_TOKENS}
    if factories:
        rows = [r for r in rows if r["creator"] not in factories]
        print(f"\nexcluded {len(factories)} factory addresses "
              f"(>= {FACTORY_MIN_TOKENS} tokens each) — launchpads pay deploy "
              f"fees for their users, so their fee payer is not the dev")
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
    ap.add_argument("--retry-unresolved", action="store_true",
                    help="delete rows with no creator and try them again. Use "
                         "this once after the rate-limit bug: those rows may be "
                         "throttled lookups recorded as permanent failures.")
    ap.add_argument("--limit", type=int, default=500)
    ap.add_argument("--workers", type=int, default=1,
                    help="concurrent RPC lookups. A called coin needs 10-20 "
                         "signature pages (1.3-3.8s), so sequential is ~3 days "
                         "for 104k tokens; 12 workers brings it under 6 hours. "
                         "DB writes stay single-threaded regardless.")
    ap.add_argument("--rps", type=float, default=8.0,
                    help="20 rps throttled badly (98.4%% -> 38.7%% resolution)")
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
    if args.retry_unresolved:
        conn = db.get_conn()
        db.safe_rollback()
        with conn.cursor() as cur:
            cur.execute("DELETE FROM token_creators WHERE creator_address IS NULL")
            print(f"cleared {cur.rowcount} unresolved rows for retry", flush=True)
        conn.commit()
    if args.report:
        report()
    else:
        backfill(args.limit, args.rps, args.dry_run, args.order,
                 args.called_only, args.workers)
        report()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
