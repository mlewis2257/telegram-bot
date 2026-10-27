"""
followed_recency.py — did the followed wallets buy just before the call, or hold old bags?

WHY (2026-10-27). The live wallet lane buys when 3+ followed wallets HOLD a coin at the
call. On its second day six such buys (8-12 wallets each) all failed, and the wallets made
no trade at all while we held; the one bank (TFOMO) was the only coin with active buying.
"Holds it" cannot tell a wallet that bought three minutes ago from one sitting on
yesterday's bag. This records, for each followed wallet holding a coin at the call, when it
first touched the coin and when it last traded it BEFORE the call.

HOW. A wallet holds a coin in its associated token account (classic or Token-2022, one
getMultipleAccounts to see which exists). That account's transaction list
(getSignaturesForAddress, newest first, 100 per page, at most MAX_PAGES pages) gives the
times of every transfer in or out — buys and sells alike, successful only. Because the
history is permanent, past calls can be backfilled with the call time as the reference.
A transaction is "before the call" if its block time is at or before the reference.
Limits: a wallet that moved the coin through another account is not seen; direction (buy or
sell) is not parsed — a wallet still holding at the call whose last transaction was a
minute earlier was, almost always, buying.

    FOLLOWED_RECENCY_ENABLED=true          record at each call (inside sol-listener, after
                                           followed_at_call's snapshot); off by default
    FOLLOWED_RECENCY_MIN_WALLETS=3         only calls this many followed wallets hold
    FOLLOWED_RECENCY_MAX_CALLS_PER_DAY=1500   hard RPC ceiling (UTC day), printed

Backfill (VPS):  python3 followed_recency.py --backfill --since '2026-10-21 07:30' [--max-calls 1500]
Report:          python3 followed_recency.py --report  --since '2026-10-21 07:30'
"""

from __future__ import annotations

import asyncio
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

import db  # noqa: E402
import wallet_holdings as wh  # noqa: E402

ENABLED = os.getenv("FOLLOWED_RECENCY_ENABLED", "false") == "true"
MIN_WALLETS = int(os.getenv("FOLLOWED_RECENCY_MIN_WALLETS", "3"))
MAX_CALLS_PER_DAY = int(os.getenv("FOLLOWED_RECENCY_MAX_CALLS_PER_DAY", "1500"))
PAGE = 100
MAX_PAGES = 3
BANK_CAP = 2.1

_day = None
_calls_today = 0
_capped_noted = False
_table_ok = False

if ENABLED:
    print(f"[followed_recency] on — when {MIN_WALLETS}+ followed wallets hold a coin at the call, "
          f"read when each first and last traded it; ceiling {MAX_CALLS_PER_DAY:,} RPC calls/day "
          f"(1 + up to {MAX_PAGES} per holder)")


# ── RPC ─────────────────────────────────────────────────────────────────────────

def _rpc(method: str, params: list):
    resp = wh.data_fetcher._rpc_post({"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    if resp is None or resp.status_code != 200:
        return None
    data = resp.json()
    if data.get("error"):
        return None
    return data.get("result")


def token_accounts(wallets: list[str], mint: str) -> tuple[dict[str, str], int]:
    """({wallet: its existing token account for mint}, RPC calls used)."""
    pairs = wh.ata_addresses(wallets, mint)
    found: dict[str, str] = {}
    calls = 0
    for i in range(0, len(pairs), wh.MAX_ACCOUNTS_PER_CALL):
        chunk = pairs[i:i + wh.MAX_ACCOUNTS_PER_CALL]
        res = _rpc("getMultipleAccounts", [[a for _, a in chunk], {"encoding": "base64"}])
        calls += 1
        if not res:
            continue
        for (w, a), acc in zip(chunk, res.get("value") or []):
            if acc and w not in found:
                found[w] = a
    return found, calls


def history(account: str, ref_ts: float) -> tuple[dict, int]:
    """Successful transactions on `account` at or before ref_ts: first, last, count.
    Returns (row, RPC calls used). complete=False if the history was longer than read."""
    before = None
    first = last = None
    n = 0
    calls = 0
    complete = False
    for _ in range(MAX_PAGES):
        opts = {"limit": PAGE}
        if before:
            opts["before"] = before
        res = _rpc("getSignaturesForAddress", [account, opts])
        calls += 1
        if res is None:
            break
        for s in res:
            bt = s.get("blockTime")
            if s.get("err") is not None or bt is None or bt > ref_ts:
                continue
            n += 1
            last = bt if last is None else max(last, bt)
            first = bt if first is None else min(first, bt)
        if len(res) < PAGE:
            complete = True
            break
        before = res[-1].get("signature")
    ts = lambda v: None if v is None else datetime.fromtimestamp(v, timezone.utc)
    return {"first_tx_at": ts(first), "last_tx_at": ts(last), "n_tx": n, "complete": complete}, calls


def lookup(holders: list[str], mint: str, ref: datetime) -> tuple[list[dict], int]:
    """One row per holder (wallet, account, first/last tx before ref). Blocking."""
    accts, calls = token_accounts(holders, mint)
    rows = []
    for w in holders:
        a = accts.get(w)
        if not a:
            rows.append({"wallet": w, "account": None, "first_tx_at": None, "last_tx_at": None,
                         "n_tx": 0, "complete": False})
            continue
        h, c = history(a, ref.timestamp())
        calls += c
        rows.append({"wallet": w, "account": a, **h})
    return rows, calls


# ── Storage ─────────────────────────────────────────────────────────────────────

def ensure_table() -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS followed_recency (
                call_id      integer NOT NULL,
                wallet       text NOT NULL,
                account      text,                -- its token account for the coin
                ref_at       timestamptz NOT NULL,  -- the call time used as "now"
                first_tx_at  timestamptz,         -- first successful tx on that account
                last_tx_at   timestamptz,         -- last one at or before ref_at
                n_tx         integer,
                complete     boolean,             -- whole history read
                checked_at   timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (call_id, wallet)
            )
            """
        )
        conn.commit()


def store(call_id: int, ref: datetime, rows: list[dict]) -> None:
    conn = db.get_conn()
    with conn.cursor() as cur:
        for r in rows:
            cur.execute(
                """
                INSERT INTO followed_recency (call_id, wallet, account, ref_at, first_tx_at,
                                              last_tx_at, n_tx, complete)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (call_id, wallet) DO NOTHING
                """, (call_id, r["wallet"], r["account"], ref, r["first_tx_at"], r["last_tx_at"],
                      r["n_tx"], r["complete"]))
        conn.commit()


# ── Live: called from followed_at_call.snapshot ───────────────────────────────────

async def record(call_id, mint: str, holders: list[str] | None) -> None:
    """Record recency for a call's holders. Never raises; respects the daily ceiling."""
    global _day, _calls_today, _capped_noted, _table_ok
    try:
        if not ENABLED or not holders or len(holders) < MIN_WALLETS:
            return
        today = datetime.now(timezone.utc).date()
        if today != _day:
            _day, _calls_today, _capped_noted = today, 0, False
        worst = 1 + len(holders) * MAX_PAGES
        if _calls_today + worst > MAX_CALLS_PER_DAY:
            if not _capped_noted:
                print(f"[followed_recency] daily ceiling {MAX_CALLS_PER_DAY} reached — none until tomorrow")
                _capped_noted = True
            return
        ref = datetime.now(timezone.utc)
        rows, calls = await asyncio.to_thread(lookup, holders, mint, ref)
        _calls_today += calls
        if not _table_ok:
            ensure_table()
            _table_ok = True
        store(int(call_id), ref, rows)
    except Exception as e:
        try:
            db.safe_rollback()
        except Exception:
            pass
        print(f"[followed_recency] call_id={call_id} failed: {type(e).__name__}: {e}")


# ── Backfill ────────────────────────────────────────────────────────────────────

def backfill(since: str, max_calls: int, min_wallets: int) -> None:
    ensure_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        # holders at the call where followed_at_call has them, else wallet_holdings' first look
        cur.execute(
            """
            WITH q AS (
              SELECT qp.call_id, qp.entry_time, c.created_at, t.mint_address AS mint
              FROM qsim_positions qp
              JOIN calls c  ON c.id = qp.call_id
              JOIN tokens t ON t.id = qp.token_id
              WHERE qp.channel_handle = 'solwhaletrending' AND qp.entry_time >= %s
            ),
            fl AS (
              SELECT q.call_id, array_agg(DISTINCT h.wallet) AS holders
              FROM q JOIN tracked_wallet_holdings h ON h.call_id = q.call_id
              WHERE h.prev_balance IS NULL AND h.balance > 0
                AND h.observed_at <= q.entry_time + interval '4 minutes'
              GROUP BY q.call_id
            )
            SELECT q.call_id, q.mint, q.created_at, COALESCE(f.holders, fl.holders) AS holders
            FROM q
            LEFT JOIN followed_at_call f ON f.call_id = q.call_id AND f.rpc_ok
            LEFT JOIN fl ON fl.call_id = q.call_id
            WHERE cardinality(COALESCE(f.holders, fl.holders)) >= %s
              AND NOT EXISTS (SELECT 1 FROM followed_recency r WHERE r.call_id = q.call_id)
            ORDER BY q.created_at
            """, (since, min_wallets))
        todo = cur.fetchall()
    print(f"[followed_recency] backfill: {len(todo)} calls with {min_wallets}+ followed holders "
          f"since {since}; ceiling {max_calls} RPC calls this run")
    used = 0
    done = 0
    for call_id, mint, called_at, holders in todo:
        if used + 1 + len(holders) * MAX_PAGES > max_calls:
            print(f"[followed_recency] stopping at the {max_calls}-call ceiling; run again to continue")
            break
        try:
            rows, calls = lookup(list(holders), mint, called_at)
            used += calls
            store(call_id, called_at, rows)
            done += 1
        except Exception as e:
            db.safe_rollback()
            print(f"[followed_recency] call_id={call_id} failed: {type(e).__name__}: {e}")
    print(f"[followed_recency] backfilled {done} calls using {used} RPC calls")


# ── Report ──────────────────────────────────────────────────────────────────────

def report(since: str) -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT qp.call_id, qp.entry_time,
                   qp.exit_reason LIKE '%%bank%%',
                   CASE WHEN qp.exit_reason LIKE '%%bank%%' THEN LEAST(qp.sol_out / qp.sol_in, %s)
                        ELSE qp.sol_out / qp.sol_in END,
                   array_agg(EXTRACT(EPOCH FROM r.ref_at - r.last_tx_at) / 60.0),
                   array_agg(EXTRACT(EPOCH FROM r.ref_at - r.first_tx_at) / 60.0)
            FROM qsim_positions qp
            JOIN followed_recency r ON r.call_id = qp.call_id
            WHERE qp.status = 'closed' AND qp.channel_handle = 'solwhaletrending'
              AND qp.sol_in > 0 AND qp.entry_time >= %s
              AND extract(isodow FROM qp.entry_time AT TIME ZONE 'UTC') <> 6
            GROUP BY qp.call_id, qp.entry_time, qp.exit_reason, qp.sol_out, qp.sol_in
            ORDER BY qp.entry_time
            """, (BANK_CAP, since))
        rows = cur.fetchall()
    if not rows:
        print("no qsim trades with recency data yet")
        return
    calls = []
    for cid, entry, bank, x, last_ages, first_ages in rows:
        last_ages = sorted(a for a in last_ages if a is not None)
        first_ages = sorted(a for a in first_ages if a is not None)
        calls.append({"entry": entry, "bank": bool(bank), "x": float(x), "n": len(last_ages),
                      "newest": last_ages[0] if last_ages else None,
                      "fresh10": sum(1 for a in last_ages if a <= 10),
                      "fresh60": sum(1 for a in last_ages if a <= 60),
                      "new60": sum(1 for a in first_ages if a <= 60),
                      "median_entry": first_ages[len(first_ages) // 2] if first_ages else None})
    mid = calls[len(calls) // 2]["entry"]

    def show(title, groups, order):
        print(f"\n{title}")
        print(f"  {'value':34s} {'trades':>6s} {'banks':>5s} {'bank %':>6s} {'net':>7s} {'per tr':>7s} "
              f"{'1st half':>8s} {'2nd half':>8s}")
        for val in order:
            g = groups.get(val, [])
            if not g:
                continue
            net = sum(c["x"] - 1 for c in g)
            h1 = sum(c["x"] - 1 for c in g if c["entry"] < mid)
            banks = sum(c["bank"] for c in g)
            print(f"  {val:34s} {len(g):6d} {banks:5d} {banks / len(g) * 100:5.1f}% {net:+7.2f} "
                  f"{net / len(g) * 100:+6.1f}% {h1:+8.2f} {net - h1:+8.2f}")

    def bucket_newest(m):
        if m is None:
            return "e no transaction found"
        return ("a newest trade <=10 min before call" if m <= 10 else
                "b 10-60 min" if m <= 60 else "c 1-6 h" if m <= 360 else "d all older than 6 h")
    g = defaultdict(list)
    for c in calls:
        g[bucket_newest(c["newest"])].append(c)
    print(f"qsim trades (Saturday excluded, banks capped at {BANK_CAP}x) with followed-wallet recency, "
          f"since {since}: {len(calls)}; halves split at {mid:%m-%d %H:%M}")
    show("MOST RECENT trade by any holding followed wallet, before the call:", g,
         ["a newest trade <=10 min before call", "b 10-60 min", "c 1-6 h", "d all older than 6 h",
          "e no transaction found"])

    g = defaultdict(list)
    for c in calls:
        k = c["fresh60"]
        g["0 traded in the last hour" if k == 0 else "1 traded in the last hour" if k == 1 else
          "2 traded in the last hour" if k == 2 else "3+ traded in the last hour"].append(c)
    show("HOW MANY holding wallets traded it in the hour before the call:", g,
         ["0 traded in the last hour", "1 traded in the last hour", "2 traded in the last hour",
          "3+ traded in the last hour"])

    g = defaultdict(list)
    for c in calls:
        m = c["median_entry"]
        g["? unknown" if m is None else "a median entry <=1 h before call" if m <= 60 else
          "b 1-6 h" if m <= 360 else "c 6-24 h" if m <= 1440 else "d over a day"].append(c)
    show("WHEN the holding wallets first got in (median):", g,
         ["a median entry <=1 h before call", "b 1-6 h", "c 6-24 h", "d over a day", "? unknown"])
    print("\nThese are qsim's results with today's regular exits. A cut is worth testing on the live "
          "lane only if it beats the rest in BOTH halves.")


if __name__ == "__main__":
    def arg(name, default):
        return sys.argv[sys.argv.index(name) + 1] if name in sys.argv else default
    since = arg("--since", "2026-10-21 07:30")
    if "--backfill" in sys.argv:
        backfill(since, int(arg("--max-calls", "1500")), int(arg("--min-wallets", str(MIN_WALLETS))))
    elif "--report" in sys.argv:
        report(since)
    else:
        print(__doc__)
