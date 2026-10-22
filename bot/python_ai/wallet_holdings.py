"""
wallet_holdings.py — do the wallets the operator follows hold the coins the bot trades?

WHY. Watching TSI (2026-10-17) the operator knew a dump would recover partly because
wallets they follow on Axiom were buying it; the bot only sees price. This records,
for every coin the bot is trading or has just sold, how much of it each followed wallet
holds, and every change. A balance going up between two looks is a buy, down is a sell,
already there at the first look means they were in before us. In two to three weeks
that can be lined up against what each coin did after a stop, a bank or an entry.

HOW (cheap on purpose). A wallet's holding of a coin lives at a fixed address derived
from the wallet and the coin (its associated token account), so ONE getMultipleAccounts
call reads every followed wallet's balance of one coin. Both the classic token program
and Token-2022 addresses are read, since a coin uses one or the other: 43 wallets = 86
accounts, under the 100-per-call limit. No websocket, no per-trade fetches, and the cost
does not depend on how much the wallets trade.

WHICH COINS. Live positions that are open or closed within AFTER_EXIT_MINS, then
solwhaletrending qsim positions on the same terms (see db.get_holdings_watch_coins), at
most MAX_COINS per pass, one pass every PASS_SECS. Hard ceiling, printed at startup:
MAX_COINS x passes per day calls. A wallet holding a coin in a non-standard account is
not seen; rare.

Research only: nothing here trades or changes a decision.

Run:   pm2 start wallet_holdings.py --name wallet-holdings --interpreter <the venv python>
List:  TRACKED_WALLETS_FILE (default: tracked_wallets.txt next to this file), one
       address per line, '#' comments allowed. Re-read every pass.
"""

from __future__ import annotations

import os
import time
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

import data_fetcher  # noqa: E402
import db  # noqa: E402

WALLETS_FILE = os.getenv("TRACKED_WALLETS_FILE",
                         str(Path(__file__).with_name("tracked_wallets.txt")))
PASS_SECS = float(os.getenv("WALLET_HOLDINGS_PASS_SECS", "120"))
MAX_COINS = int(os.getenv("WALLET_HOLDINGS_MAX_COINS", "15"))
AFTER_EXIT_MINS = float(os.getenv("WALLET_HOLDINGS_AFTER_EXIT_MINS", "60"))
TALLY_SECS = float(os.getenv("WALLET_HOLDINGS_TALLY_SECS", "1800"))
MAX_ACCOUNTS_PER_CALL = 100

TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN_2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
ATA_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"

_tally: Counter[str] = Counter()
_last: dict[tuple[str, str], float] = {}     # (mint, wallet) -> last balance seen
_ata_cache: dict[str, list[tuple[str, str]]] = {}   # mint -> [(wallet, ata), ...]


def load_wallets(path: str) -> list[str]:
    """Valid, de-duplicated addresses from the list file. Bad lines are reported, not fatal."""
    from solders.pubkey import Pubkey
    try:
        lines = Path(path).read_text().splitlines()
    except OSError as e:
        print(f"[wallet_holdings] cannot read {path}: {e}")
        return []
    out, seen = [], set()
    for n, raw in enumerate(lines, 1):
        addr = raw.split("#", 1)[0].strip()
        if not addr:
            continue
        try:
            Pubkey.from_string(addr)
        except Exception:
            print(f"[wallet_holdings] {path}:{n} is not a valid address, skipped: {addr[:12]}...")
            continue
        if addr not in seen:
            seen.add(addr)
            out.append(addr)
    return out


def ata_addresses(wallets: list[str], mint: str) -> list[tuple[str, str]]:
    """[(wallet, token account address)] for both token programs, for one coin."""
    key = mint + "|" + ",".join(wallets)
    if key in _ata_cache:
        return _ata_cache[key]
    from solders.pubkey import Pubkey
    m = Pubkey.from_string(mint)
    ata_prog = Pubkey.from_string(ATA_PROGRAM)
    out = []
    for prog in (TOKEN_PROGRAM, TOKEN_2022_PROGRAM):
        p = Pubkey.from_string(prog)
        for w in wallets:
            addr, _ = Pubkey.find_program_address(
                [bytes(Pubkey.from_string(w)), bytes(p), bytes(m)], ata_prog)
            out.append((w, str(addr)))
    if len(_ata_cache) > 5000:
        _ata_cache.clear()
    _ata_cache[key] = out
    return out


def parse_balances(pairs: list[tuple[str, str]], accounts: list, mint: str) -> dict[str, float]:
    """wallet -> balance of `mint` from a jsonParsed getMultipleAccounts result. An
    account that does not exist reads as 0; one for a different mint or owner is ignored."""
    bal: dict[str, float] = {w: 0.0 for w, _ in pairs}
    for (wallet, _addr), acc in zip(pairs, accounts):
        if not acc:
            continue
        try:
            info = acc["data"]["parsed"]["info"]
            if info.get("mint") != mint or info.get("owner") != wallet:
                continue
            ta = info.get("tokenAmount") or {}
            amount = float(ta.get("amount") or 0) / (10 ** int(ta.get("decimals") or 0))
            bal[wallet] += amount
        except (KeyError, TypeError, ValueError):
            continue
    return bal


def fetch_accounts(addresses: list[str]) -> list | None:
    """One getMultipleAccounts call through the pooled RPC. None on any failure."""
    try:
        resp = data_fetcher._rpc_post({
            "jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts",
            "params": [addresses, {"encoding": "jsonParsed", "commitment": "confirmed"}],
        })
        if resp is None or resp.status_code != 200:
            return None
        data = resp.json()
        if data.get("error"):
            return None
        return (data.get("result") or {}).get("value")
    except Exception:
        return None


def changes(mint: str, balances: dict[str, float], call_id, source) -> list[dict]:
    """Rows worth storing: a first non-zero reading, or any change since the last one."""
    rows = []
    for wallet, b in balances.items():
        key = (mint, wallet)
        prev = _last.get(key)
        if prev is None:
            if b > 0:
                rows.append({"mint": mint, "wallet": wallet, "balance": b, "prev_balance": None,
                             "call_id": call_id, "source": source})
        elif abs(b - prev) > max(1e-9, 1e-9 * max(abs(b), abs(prev))):
            rows.append({"mint": mint, "wallet": wallet, "balance": b, "prev_balance": prev,
                         "call_id": call_id, "source": source})
        _last[key] = b
    return rows


def run_pass(wallets: list[str]) -> None:
    coins = db.get_holdings_watch_coins(AFTER_EXIT_MINS, MAX_COINS)
    _tally["passes"] += 1
    active = set()
    for c in coins:
        mint = c["mint"]
        active.add(mint)
        try:
            pairs = ata_addresses(wallets, mint)
        except Exception:
            _tally["bad_mint"] += 1
            continue
        accounts: list = []
        ok = True
        for i in range(0, len(pairs), MAX_ACCOUNTS_PER_CALL):
            chunk = pairs[i:i + MAX_ACCOUNTS_PER_CALL]
            got = fetch_accounts([a for _, a in chunk])
            _tally["calls"] += 1
            if got is None or len(got) != len(chunk):
                ok = False
                break
            accounts.extend(got)
        if not ok:
            _tally["call_failed"] += 1
            continue
        _tally["coins_checked"] += 1
        rows = changes(mint, parse_balances(pairs, accounts, mint), c.get("call_id"), c.get("source"))
        if rows:
            _tally["changes_stored"] += db.insert_tracked_wallet_holdings(rows)
    # forget coins no longer watched so memory stays bounded
    for key in [k for k in _last if k[0] not in active]:
        del _last[key]


def check_one(mint: str) -> None:
    """One-off: print every followed wallet's balance of `mint` (2 RPC calls at most).
    Use a coin you know one of them holds to confirm the account lookup works."""
    wallets = load_wallets(WALLETS_FILE)
    if not wallets:
        print(f"no wallets in {WALLETS_FILE}")
        return
    pairs = ata_addresses(wallets, mint)
    accounts: list = []
    for i in range(0, len(pairs), MAX_ACCOUNTS_PER_CALL):
        chunk = pairs[i:i + MAX_ACCOUNTS_PER_CALL]
        got = fetch_accounts([a for _, a in chunk])
        if got is None or len(got) != len(chunk):
            print("RPC call failed")
            return
        accounts.extend(got)
    existing = sum(1 for a in accounts if a)
    bal = parse_balances(pairs, accounts, mint)
    holders = {w: b for w, b in bal.items() if b > 0}
    print(f"{len(wallets)} wallets, {len(pairs)} token accounts checked, {existing} exist, "
          f"{len(holders)} wallets hold {mint[:8]}...")
    for w, b in sorted(holders.items(), key=lambda kv: -kv[1]):
        print(f"  {w[:6]}...{w[-4:]}  {b:,.2f}")
    n_wallets = len(wallets)
    for i, ((w, addr), acc) in enumerate(zip(pairs, accounts)):
        if not acc:
            continue
        prog = "classic" if i < n_wallets else "token-2022"
        data = acc.get("data")
        try:
            info = data["parsed"]["info"]
            ta = info.get("tokenAmount") or {}
            print(f"  account {addr[:6]}... ({prog}) wallet {w[:6]}...{w[-4:]}: "
                  f"mint matches={info.get('mint') == mint} owner matches={info.get('owner') == w} "
                  f"amount={ta.get('uiAmountString', ta.get('amount'))} state={info.get('state')}")
        except (KeyError, TypeError):
            print(f"  account {addr[:6]}... ({prog}) wallet {w[:6]}...{w[-4:]}: NOT PARSED — "
                  f"owner program {acc.get('owner')}, data type {type(data).__name__}: {str(data)[:120]}")


def main() -> None:
    import sys
    if len(sys.argv) == 3 and sys.argv[1] == "--check":
        check_one(sys.argv[2])
        return
    db.ensure_tracked_wallet_holdings_table()
    per_day = MAX_COINS * (86400.0 / PASS_SECS)
    print(f"[wallet_holdings] every {PASS_SECS:g}s, at most {MAX_COINS} coins per pass, "
          f"coins watched until {AFTER_EXIT_MINS:g} min after exit — ceiling "
          f"{per_day:,.0f} calls/day (~{per_day * 30:,.0f}/month), 1 call per coin per pass")
    last_tally = time.monotonic()
    while True:
        started = time.monotonic()
        wallets = load_wallets(WALLETS_FILE)
        if not wallets:
            print(f"[wallet_holdings] no wallets in {WALLETS_FILE} — checking again next pass")
        else:
            if len(wallets) * 2 > MAX_ACCOUNTS_PER_CALL:
                _tally["multi_call_coins"] += 1
            try:
                run_pass(wallets)
            except Exception as e:
                db.safe_rollback()
                _tally["pass_error"] += 1
                print(f"[wallet_holdings] pass failed: {type(e).__name__}: {e}")
        if time.monotonic() - last_tally >= TALLY_SECS:
            t = dict(_tally)
            _tally.clear()
            print(f"[wallet_holdings] {TALLY_SECS/60:g}m: passes={t.get('passes', 0)} "
                  f"coins_checked={t.get('coins_checked', 0)} calls={t.get('calls', 0)} "
                  f"call_failed={t.get('call_failed', 0)} changes_stored={t.get('changes_stored', 0)} "
                  f"wallets={len(wallets)}", flush=True)
            last_tally = time.monotonic()
        time.sleep(max(1.0, PASS_SECS - (time.monotonic() - started)))


if __name__ == "__main__":
    main()
