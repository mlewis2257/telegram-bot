"""
wallet_watch.py — record every trade made by the wallets the operator follows.

WHY. Watching TSI (2026-10-17) the operator knew the dump would recover partly because
wallets they follow on Axiom were buying it. The bot could not see that: every test so
far has been on the price path alone. This records what those wallets buy and sell, on
every coin, as it lands on chain — so in two to three weeks we can ask whether "followed
wallets buying" separates the coins that recover or run from the ones that die, at the
moments the bot decides (the stop, the bank, the entry).

HOW. One Helius `logsSubscribe` per wallet (`mentions: [wallet]`) on a single websocket.
Each notification is one transaction that touched the wallet; failed transactions are
skipped. The transaction is fetched once (getTransaction, through the same rotating RPC
pool and per-method tally as everything else) and the wallet's token balance changes
are read from it: token up + SOL down = buy, token down + SOL up = sell. Works for any
DEX or router, no program-specific decoding. Rows go to tracked_wallet_trades.

COST. Every trade by every followed wallet costs one getTransaction. Active wallets can
make hundreds a day, so fetches are capped (WALLET_WATCH_MAX_FETCH_PER_MIN) and every
TALLY_SECS a line reports notifications, fetches, trades stored and anything dropped.
The data_fetcher `[rpc]` tally counts the getTransaction calls too. Check the Helius
dashboard after the first day.

Research only: nothing here trades or changes a decision.

Run:   pm2 start wallet_watch.py --name wallet-watch --interpreter <the venv python>
List:  TRACKED_WALLETS_FILE (default: tracked_wallets.txt next to this file), one
       address per line, '#' comments allowed. Re-read on every reconnect.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections import Counter, deque
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

import websockets  # noqa: E402

import data_fetcher  # noqa: E402
import db  # noqa: E402
import rpc_pool  # noqa: E402
from order_flow import WSOL_MINT  # noqa: E402

WALLETS_FILE = os.getenv("TRACKED_WALLETS_FILE",
                         str(Path(__file__).with_name("tracked_wallets.txt")))
COMMITMENT = os.getenv("WALLET_WATCH_COMMITMENT", "confirmed")
MAX_FETCH_PER_MIN = int(os.getenv("WALLET_WATCH_MAX_FETCH_PER_MIN", "120"))
TALLY_SECS = float(os.getenv("WALLET_WATCH_TALLY_SECS", "300"))
QUEUE_MAX = int(os.getenv("WALLET_WATCH_QUEUE_MAX", "2000"))
HEARTBEAT_SECS = 30
BACKOFF_MAX = 60

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
# Quote currencies, not coins being traded.
_NOT_A_COIN = {WSOL_MINT, USDC_MINT, USDT_MINT}
_MIN_SOL = 0.0005   # below this a balance change is a transfer or dust, not a trade
# Only fetch transactions whose logs look like a swap. Followed wallets also receive a
# stream of spam airdrops and plain transfers; fetching those would spend credits on
# rows that are never trades. Program log markers of the DEXes and routers these wallets
# use: pump.fun / PumpSwap "Instruction: Buy|Sell", Jupiter "Route", Raydium "ray_log"
# and "SwapBase...", Orca / Meteora / most AMMs "Swap". Set WALLET_WATCH_SWAP_FILTER=false
# to fetch everything (to check the filter is not hiding trades).
SWAP_FILTER = os.getenv("WALLET_WATCH_SWAP_FILTER", "true").strip().lower() != "false"
_SWAP_MARKERS = ("Instruction: Buy", "Instruction: Sell", "Swap", "Route", "ray_log")


def looks_like_swap(logs: list | None) -> bool:
    if not logs:
        return True            # no logs in the notification: cannot tell, so fetch
    return any(any(m in (line or "") for m in _SWAP_MARKERS) for line in logs)

_tally: Counter[str] = Counter()


def load_wallets(path: str) -> list[str]:
    """Valid, de-duplicated addresses from the list file. Bad lines are reported, not fatal."""
    try:
        from solders.pubkey import Pubkey
    except Exception:          # pragma: no cover — solders ships with the bot
        Pubkey = None
    try:
        lines = Path(path).read_text().splitlines()
    except OSError as e:
        print(f"[wallet_watch] cannot read {path}: {e}")
        return []
    out, seen = [], set()
    for n, raw in enumerate(lines, 1):
        addr = raw.split("#", 1)[0].strip()
        if not addr:
            continue
        try:
            if Pubkey is not None:
                Pubkey.from_string(addr)
            elif not (32 <= len(addr) <= 44):
                raise ValueError("bad length")
        except Exception:
            print(f"[wallet_watch] {path}:{n} is not a valid address, skipped: {addr[:12]}...")
            continue
        if addr not in seen:
            seen.add(addr)
            out.append(addr)
    return out


def parse_wallet_trades(tx: dict, wallet: str, signature: str) -> list[dict]:
    """Trades `wallet` made in one transaction, as rows for tracked_wallet_trades.

    For every coin (not SOL/USDC/USDT) whose balance owned by `wallet` changed: up =
    buy, down = sell. SOL size is the wallet's wrapped-SOL change if any, else its
    native SOL change (fee removed when it paid the fee). A transaction moving more
    than one coin keeps every row but leaves the SOL size empty, since it cannot be
    split honestly. Changes with no SOL movement (transfers, airdrops) are not trades.
    """
    try:
        meta = tx.get("meta") or {}
        if meta.get("err") is not None:
            return []
        msg = (tx.get("transaction") or {}).get("message") or {}
        keys = [k.get("pubkey") if isinstance(k, dict) else k for k in (msg.get("accountKeys") or [])]
        pre = meta.get("preTokenBalances") or []
        post = meta.get("postTokenBalances") or []

        def bal(balances: list, mint: str) -> float:
            tot = 0.0
            for b in balances:
                if b.get("mint") == mint and b.get("owner") == wallet:
                    try:
                        tot += float((b.get("uiTokenAmount") or {}).get("uiAmount") or 0)
                    except (TypeError, ValueError):
                        pass
            return tot

        mints = {b.get("mint") for b in pre + post if b.get("owner") == wallet} - _NOT_A_COIN - {None}
        deltas = {m: bal(post, m) - bal(pre, m) for m in mints}
        deltas = {m: d for m, d in deltas.items() if abs(d) > 1e-12}
        if not deltas:
            return []

        sol = abs(bal(post, WSOL_MINT) - bal(pre, WSOL_MINT))
        if sol < _MIN_SOL and wallet in keys:
            i = keys.index(wallet)
            pre_l, post_l = meta.get("preBalances") or [], meta.get("postBalances") or []
            if i < len(pre_l) and i < len(post_l):
                native = (post_l[i] - pre_l[i]) / 1e9
                if i == 0:                       # it paid the fee; don't count it as trade size
                    native += float(meta.get("fee") or 0) / 1e9
                sol = abs(native)
        if sol < _MIN_SOL:
            return []

        ts = tx.get("blockTime")
        single = len(deltas) == 1
        return [{
            "wallet": wallet, "signature": signature, "mint": m,
            "side": "buy" if d > 0 else "sell",
            "sol_amount": round(sol, 9) if single else None,
            "token_amount": abs(d), "ts": ts,
        } for m, d in deltas.items()]
    except Exception:
        return []


def _ws_url() -> str:
    http = os.getenv("WS_RPC_URL") or os.getenv("SOLANA_RPC_URL") or (rpc_pool.endpoints() or [""])[0]
    return http.replace("https://", "wss://").replace("http://", "ws://")


class _Budget:
    """Sliding one-minute cap on transaction fetches."""

    def __init__(self, per_min: int):
        self.per_min = per_min
        self.stamps: deque[float] = deque()

    async def wait(self) -> None:
        while True:
            now = time.monotonic()
            while self.stamps and now - self.stamps[0] > 60.0:
                self.stamps.popleft()
            if self.per_min <= 0 or len(self.stamps) < self.per_min:
                self.stamps.append(now)
                return
            await asyncio.sleep(max(0.05, 60.0 - (now - self.stamps[0])))


async def _fetch_worker(queue: asyncio.Queue, budget: _Budget) -> None:
    while True:
        signature, wallet = await queue.get()
        try:
            await budget.wait()
            tx = await asyncio.to_thread(data_fetcher._rpc_get_transaction, signature, COMMITMENT)
            _tally["fetched"] += 1
            if not tx:
                _tally["fetch_failed"] += 1
                continue
            rows = parse_wallet_trades(tx, wallet, signature)
            if rows:
                new = await asyncio.to_thread(db.insert_tracked_wallet_trades, rows)
                _tally["trades_stored"] += new
            else:
                _tally["not_a_trade"] += 1
        except Exception as e:
            _tally["worker_error"] += 1
            db.safe_rollback()
            print(f"[wallet_watch] fetch/parse failed {signature[:8]}: {type(e).__name__}: {e}")
        finally:
            queue.task_done()


async def _tally_printer(queue: asyncio.Queue) -> None:
    while True:
        await asyncio.sleep(TALLY_SECS)
        t = dict(_tally)
        _tally.clear()
        print(f"[wallet_watch] {TALLY_SECS/60:g}m: notifications={t.get('notifications', 0)} "
              f"failed_tx={t.get('failed_tx', 0)} dupes={t.get('dupes', 0)} "
              f"not_a_swap={t.get('not_a_swap', 0)} "
              f"fetched={t.get('fetched', 0)} fetch_failed={t.get('fetch_failed', 0)} "
              f"trades_stored={t.get('trades_stored', 0)} not_a_trade={t.get('not_a_trade', 0)} "
              f"dropped_queue_full={t.get('dropped', 0)} queue={queue.qsize()}")


async def _heartbeat(ws) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_SECS)
        try:
            await ws.ping()
        except Exception:
            return


async def run() -> None:
    db.ensure_tracked_wallet_trades_table()
    queue: asyncio.Queue = asyncio.Queue(maxsize=QUEUE_MAX)
    budget = _Budget(MAX_FETCH_PER_MIN)
    asyncio.create_task(_fetch_worker(queue, budget))
    asyncio.create_task(_tally_printer(queue))
    seen: deque[str] = deque(maxlen=20000)
    seen_set: set[str] = set()
    ws_url = _ws_url()
    backoff = 1

    while True:
        wallets = load_wallets(WALLETS_FILE)
        if not wallets:
            print(f"[wallet_watch] no wallets in {WALLETS_FILE} — checking again in 60s")
            await asyncio.sleep(60)
            continue
        if not ws_url:
            print("[wallet_watch] no RPC endpoint configured (SOLANA_RPC_URL / WS_RPC_URL)")
            await asyncio.sleep(60)
            ws_url = _ws_url()
            continue
        try:
            async with websockets.connect(ws_url, ping_interval=None, open_timeout=20,
                                          close_timeout=10, max_size=2**22) as ws:
                req_to_wallet: dict[int, str] = {}
                sub_to_wallet: dict[int, str] = {}
                for i, w in enumerate(wallets, 1):
                    req_to_wallet[i] = w
                    await ws.send(json.dumps({
                        "jsonrpc": "2.0", "id": i, "method": "logsSubscribe",
                        "params": [{"mentions": [w]}, {"commitment": COMMITMENT}],
                    }))
                print(f"[wallet_watch] connected {ws_url[:40]}... subscribing {len(wallets)} wallet(s); "
                      f"fetch cap {MAX_FETCH_PER_MIN}/min, commitment {COMMITMENT}")
                hb = asyncio.create_task(_heartbeat(ws))
                connected_at = time.monotonic()
                try:
                    async for raw in ws:
                        msg = json.loads(raw)
                        if "id" in msg and "result" in msg and msg["id"] in req_to_wallet:
                            sub_to_wallet[msg["result"]] = req_to_wallet[msg["id"]]
                            if len(sub_to_wallet) == len(wallets):
                                print(f"[wallet_watch] all {len(wallets)} subscriptions confirmed")
                            continue
                        if "error" in msg:
                            print(f"[wallet_watch] subscription error: {str(msg['error'])[:200]}")
                            continue
                        if msg.get("method") != "logsNotification":
                            continue
                        params = msg.get("params") or {}
                        wallet = sub_to_wallet.get(params.get("subscription"))
                        value = ((params.get("result") or {}).get("value") or {})
                        sig = value.get("signature")
                        if not wallet or not sig:
                            continue
                        _tally["notifications"] += 1
                        if value.get("err") is not None:
                            _tally["failed_tx"] += 1
                            continue
                        if SWAP_FILTER and not looks_like_swap(value.get("logs")):
                            _tally["not_a_swap"] += 1
                            continue
                        key = f"{sig}:{wallet}"
                        if key in seen_set:
                            _tally["dupes"] += 1
                            continue
                        if len(seen) == seen.maxlen:
                            seen_set.discard(seen[0])
                        seen.append(key)
                        seen_set.add(key)
                        try:
                            queue.put_nowait((sig, wallet))
                        except asyncio.QueueFull:
                            _tally["dropped"] += 1
                finally:
                    hb.cancel()
                if time.monotonic() - connected_at >= 30:
                    backoff = 1
                print("[wallet_watch] websocket closed — reconnecting")
        except Exception as e:
            print(f"[wallet_watch] websocket error: {type(e).__name__}: {str(e)[:200]}")
            if rpc_pool.is_quota_error(getattr(e, "status_code", None), repr(e)):
                rpc_pool.penalize(ws_url)
                nxt = rpc_pool.ws_url()
                if not os.getenv("WS_RPC_URL") and nxt and nxt != ws_url:
                    print("[wallet_watch] endpoint out of quota — moving to the next key")
                    ws_url = nxt
                    backoff = 1
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, BACKOFF_MAX)


if __name__ == "__main__":
    asyncio.run(run())
