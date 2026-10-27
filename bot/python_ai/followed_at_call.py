"""
followed_at_call.py — do the operator's followed wallets hold a coin AT THE CALL?

WHY (2026-10-23). wallet_holdings.py looks at a coin 0-2 min AFTER qsim buys it. On
Oct 21-23 solwhaletrending coins that 2+ followed wallets held at that first look banked
39% (+22.5%/trade, 36 trades) vs 9% (-16%/trade, 128) with none. Part of that can be
look-ahead: a coin that pumps in those 2 minutes draws the wallets in. This takes the same
reading at the moment the call arrives, before qsim or live buys, which is also exactly
what a live entry rule would see.

HOW. On each call from FOLLOWED_AT_CALL_CHANNELS, one getMultipleAccounts (both token
programs x the followed wallets — the same lookup as wallet_holdings.py) in a worker
thread; the result is one row in followed_at_call. Nothing waits for it and nothing
trades on it. Hard ceiling FOLLOWED_AT_CALL_MAX_PER_DAY calls (UTC day), printed at start.

    FOLLOWED_AT_CALL_ENABLED=true            off unless exactly 'true'
    FOLLOWED_AT_CALL_CHANNELS=solwhaletrending   comma-separated handles
    FOLLOWED_AT_CALL_MAX_PER_DAY=300
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from pathlib import Path

import db
import wallet_holdings as wh

ENABLED = os.getenv("FOLLOWED_AT_CALL_ENABLED", "false") == "true"
CHANNELS = {c.strip().lstrip("@") for c in
            os.getenv("FOLLOWED_AT_CALL_CHANNELS", "solwhaletrending").split(",") if c.strip()}
MAX_PER_DAY = int(os.getenv("FOLLOWED_AT_CALL_MAX_PER_DAY", "300"))

_day = None
_count = 0
_capped_noted = False
_table_ok = False
_wallets: tuple[float, list[str]] | None = None    # (file mtime, wallets)

if ENABLED:
    print(f"[followed_at_call] on for {', '.join(sorted(CHANNELS))} — ceiling {MAX_PER_DAY} "
          f"calls/day (1 RPC call per call)")


def _load_wallets() -> list[str]:
    global _wallets
    try:
        mtime = Path(wh.WALLETS_FILE).stat().st_mtime
    except OSError:
        return []
    if _wallets is None or _wallets[0] != mtime:
        _wallets = (mtime, wh.load_wallets(wh.WALLETS_FILE))
    return _wallets[1]


def _lookup(wallets: list[str], mint: str) -> list[str] | None:
    """Followed wallets holding `mint` now, or None if the RPC failed. Blocking."""
    pairs = wh.ata_addresses(wallets, mint)
    accounts: list = []
    for i in range(0, len(pairs), wh.MAX_ACCOUNTS_PER_CALL):
        chunk = pairs[i:i + wh.MAX_ACCOUNTS_PER_CALL]
        got = wh.fetch_accounts([a for _, a in chunk])
        if got is None or len(got) != len(chunk):
            return None
        accounts.extend(got)
    bal = wh.parse_balances(pairs, accounts, mint)
    return sorted(w for w, b in bal.items() if b > 0)


async def snapshot(call_id, mint, channel: str) -> None:
    """Record which followed wallets hold `mint` right now. Never raises."""
    global _day, _count, _capped_noted, _table_ok
    try:
        if not ENABLED or not call_id or not mint or channel not in CHANNELS:
            return
        if mint.startswith(("UNKNOWN:", "INFERRED:")):
            return
        today = datetime.now(timezone.utc).date()
        if today != _day:
            _day, _count, _capped_noted = today, 0, False
        if _count >= MAX_PER_DAY:
            if not _capped_noted:
                print(f"[followed_at_call] daily ceiling {MAX_PER_DAY} reached — no more lookups today")
                _capped_noted = True
            return
        wallets = _load_wallets()
        if not wallets:
            return
        _count += 1
        holders = await asyncio.to_thread(_lookup, wallets, mint)
        if not _table_ok:
            db.ensure_followed_at_call_table()
            _table_ok = True
        db.insert_followed_at_call(int(call_id), mint, channel, holders, len(wallets))
        # When enough followed wallets hold it: when did each of them get in / last trade it?
        import followed_recency
        if followed_recency.ENABLED and holders:
            await followed_recency.record(call_id, mint, holders)
    except Exception as e:
        try:
            db.safe_rollback()
        except Exception:
            pass
        print(f"[followed_at_call] call_id={call_id} failed: {type(e).__name__}: {e}")
