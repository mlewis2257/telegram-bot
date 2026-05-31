"""
ws_monitor.py — Real-time WebSocket exit monitor for open paper trading positions.

Uses Helius logsSubscribe to detect on-chain activity for monitored tokens.
When a transaction mentions a monitored mint, first tries to derive price/mcap
from the Helius transaction payload itself and falls back to Dex/Jupiter when
needed, then runs exit checks for both Strategy A and B.

Runs ALONGSIDE sol-monitor.py (polling fallback). Both check exits safely since
db.close_paper_position has AND status='open' guard against double-close.

Usage:
    python3 ws_monitor.py
"""

import asyncio
import datetime as _dt
import json
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

import websockets
import db
import data_fetcher
import paper_trader
import paper_trader_b

# ── Config ────────────────────────────────────────────────────────────────────

_rpc_url = os.getenv("SOLANA_RPC_URL", "")
if not _rpc_url:
    raise RuntimeError("SOLANA_RPC_URL not set in .env")
WS_URL = _rpc_url.replace("https://", "wss://").replace("http://", "ws://")

HEARTBEAT_INTERVAL    = 30    # seconds between pings
POLL_INTERVAL         = 2     # seconds between new-position polls
FETCH_COOLDOWN        = 2.0   # min seconds between DexScreener fetches per mint
RECONNECT_BACKOFF_MAX = 60    # max reconnect delay in seconds
LOG_COMMITMENT        = os.getenv("WS_LOG_COMMITMENT", "processed")
TX_MARKET_STATS_INTERVAL = int(os.getenv("WS_TX_MARKET_STATS_INTERVAL", "60"))
TX_MARKET_DEBUG          = os.getenv("WS_TX_MARKET_DEBUG", "").lower() in ("1", "true", "yes")

# ── Rate-limit state ──────────────────────────────────────────────────────────

_last_fetch: dict[str, float] = {}   # mint → monotonic time of last price fetch
_market_stats: Counter[str] = Counter()
_last_market_stats_print = 0.0
# Strategy A-only realtime peak cache (call_id -> peak mcap since A entry).
_a_realtime_peak_mcap: dict[int, float] = {}


def _maybe_print_market_stats(force: bool = False) -> None:
    global _last_market_stats_print
    if TX_MARKET_STATS_INTERVAL <= 0:
        return
    now = time.monotonic()
    if not force and now - _last_market_stats_print < TX_MARKET_STATS_INTERVAL:
        return
    _last_market_stats_print = now
    total = sum(_market_stats.values())
    if total == 0:
        return
    print(
        "[ws_monitor] market stats "
        f"tx_success={_market_stats['tx_success']} "
        f"tx_miss={_market_stats['tx_miss']} "
        f"fallback_success={_market_stats['fallback_success']} "
        f"fallback_miss={_market_stats['fallback_miss']}"
    )


# ── Subscription manager ──────────────────────────────────────────────────────

class SubscriptionManager:
    """
    Tracks active logsSubscribe subscriptions.

    Confirmation matching uses the JSON-RPC request id: when we send
    logsSubscribe with id=N, the server responds {"id": N, "result": sub_id}.
    This is robust even when multiple subscribes are in-flight simultaneously.

    Mappings:
        msg_id  → mint          (_pending)
        mint    → sub_id        (_mint_to_sub)
        sub_id  → call_id       (_sub_to_call)
        mint    → call_id       (_mint_to_call)
    """

    def __init__(self) -> None:
        self._pending:      dict[int, str] = {}   # request msg_id → mint
        self._mint_to_sub:  dict[str, int] = {}   # mint → confirmed sub_id
        self._sub_to_call:  dict[int, int] = {}   # sub_id → call_id
        self._mint_to_call: dict[str, int] = {}   # mint → call_id
        self._next_msg_id = 100

    def _new_id(self) -> int:
        self._next_msg_id += 1
        return self._next_msg_id

    @property
    def subscribed_mints(self) -> set[str]:
        """Mints with a confirmed server-side subscription."""
        return set(self._mint_to_sub.keys())

    @property
    def known_mints(self) -> set[str]:
        """All mints including those with pending (unconfirmed) subscriptions."""
        return set(self._mint_to_call.keys())

    def call_id_for_sub(self, sub_id: int) -> int | None:
        return self._sub_to_call.get(sub_id)

    def mint_for_call(self, call_id: int) -> str | None:
        for m, c in self._mint_to_call.items():
            if c == call_id:
                return m
        return None

    def on_confirm(self, msg_id: int, sub_id: int) -> None:
        """Register the server-assigned sub_id when a subscribe confirmation arrives."""
        mint = self._pending.pop(msg_id, None)
        if mint is None:
            return
        call_id = self._mint_to_call.get(mint)
        if call_id is None:
            return
        self._mint_to_sub[mint] = sub_id
        self._sub_to_call[sub_id] = call_id

    async def subscribe(self, ws, mint: str, call_id: int) -> None:
        if mint in self.known_mints:
            return  # already subscribed or pending
        msg_id = self._new_id()
        self._pending[msg_id]      = mint
        self._mint_to_call[mint]   = call_id
        msg = {
            "jsonrpc": "2.0",
            "id":      msg_id,
            "method":  "logsSubscribe",
            "params":  [
                {"mentions": [mint]},
                {"commitment": LOG_COMMITMENT},
            ],
        }
        await ws.send(json.dumps(msg))
        print(f"[ws_monitor] subscribe {mint[:8]}...  call_id={call_id}")

    async def unsubscribe(self, ws, mint: str) -> None:
        sub_id  = self._mint_to_sub.pop(mint, None)
        call_id = self._mint_to_call.pop(mint, None)
        # Remove any pending confirmation entry
        self._pending = {k: v for k, v in self._pending.items() if v != mint}
        if sub_id is not None:
            self._sub_to_call.pop(sub_id, None)
            try:
                msg = {
                    "jsonrpc": "2.0",
                    "id":      self._new_id(),
                    "method":  "logsUnsubscribe",
                    "params":  [sub_id],
                }
                await ws.send(json.dumps(msg))
                print(f"[ws_monitor] unsubscribed {mint[:8]}...  call_id={call_id}")
            except Exception:
                pass  # connection may already be closed

    async def resubscribe_all(self, ws) -> None:
        """Re-send subscriptions for all known mints after a reconnect."""
        # Clear confirmed state — must re-confirm after reconnect
        self._mint_to_sub.clear()
        self._sub_to_call.clear()
        self._pending.clear()
        mints = list(self._mint_to_call.items())
        self._mint_to_call.clear()
        for mint, call_id in mints:
            await self.subscribe(ws, mint, call_id)

    def remove(self, mint: str) -> None:
        """Remove all state for a mint without sending unsubscribe."""
        sub_id = self._mint_to_sub.pop(mint, None)
        self._mint_to_call.pop(mint, None)
        self._pending = {k: v for k, v in self._pending.items() if v != mint}
        if sub_id is not None:
            self._sub_to_call.pop(sub_id, None)


_mgr = SubscriptionManager()


# ── Subscription sync helper ──────────────────────────────────────────────────

async def sync_open_positions(ws) -> None:
    """
    Sync websocket subscriptions to current open paper positions immediately.

    - Subscribe to mints with open positions that are not currently tracked.
    - Unsubscribe mints that no longer have any open position.
    """
    positions_a = db.get_open_paper_positions(is_strategy_b=False)
    positions_b = db.get_open_paper_positions(is_strategy_b=True)
    all_positions = {p["call_id"]: p for p in positions_a}
    for p in positions_b:
        if p["call_id"] not in all_positions:
            all_positions[p["call_id"]] = p

    open_mints = set()
    for pos in all_positions.values():
        mint = pos.get("mint_address") or ""
        call_id = pos["call_id"]
        if not mint or mint.startswith(("INFERRED:", "UNKNOWN:")):
            continue
        open_mints.add(mint)
        if mint not in _mgr.known_mints:
            await _mgr.subscribe(ws, mint, call_id)

    for mint in list(_mgr.known_mints):
        if mint not in open_mints:
            await _mgr.unsubscribe(ws, mint)


# ── Exit handler ──────────────────────────────────────────────────────────────

async def handle_log_notification(ws, mint: str, call_id: int, signature: str | None = None) -> None:
    """
    Called when Helius fires a log mentioning one of our mints.
    Rate-limited to 1 market fetch per mint per FETCH_COOLDOWN seconds.

    First attempts a Helius getTransaction-derived market snapshot using the
    triggering signature. Falls back to the existing Dex/Jupiter path.
    """
    now = time.monotonic()
    if now - _last_fetch.get(mint, 0) < FETCH_COOLDOWN:
        return
    _last_fetch[mint] = now

    market = None
    if signature:
        try:
            market = data_fetcher.fetch_ws_exit_market_from_transaction(signature, mint)
        except Exception as e:
            print(f"[ws_monitor] tx-price parse error {mint[:8]} sig={signature[:8]}: {e}")
            market = None

    if market and market.get("mcap"):
        _market_stats["tx_success"] += 1
        print(
            f"[ws_monitor] tx market {mint[:8]}..."
            f" sig={signature[:8] if signature else 'none'}"
            f" mcap=${float(market['mcap'])/1000:.1f}k"
        )
    else:
        _market_stats["tx_miss"] += 1
        if TX_MARKET_DEBUG and signature:
            print(f"[ws_monitor] tx market miss {mint[:8]} sig={signature[:8]}")
        try:
            market = data_fetcher.fetch_token_price(mint)
        except Exception as e:
            print(f"[ws_monitor] price fetch error {mint[:8]}: {e}")
            return

    if not market or not market.get("mcap"):
        _market_stats["fallback_miss"] += 1
        _maybe_print_market_stats()
        return
    if market.get("source") != "helius_tx":
        _market_stats["fallback_success"] += 1
        if TX_MARKET_DEBUG:
            print(f"[ws_monitor] fallback market {mint[:8]} source={market.get('source') or 'unknown'}")
    _maybe_print_market_stats()

    current_mcap = float(market["mcap"])

    try:
        db.insert_ws_market_observation(
            call_id=call_id,
            mint_address=mint,
            signature=signature,
            market=market,
        )
    except Exception as e:
        print(f"[ws_monitor] market observation write failed {mint[:8]}: {e}")

    try:
        peak_info = db.get_call_peak_info(call_id)
    except Exception:
        peak_info = None
    if peak_info and peak_info.get("mcap_at_call"):
        try:
            mcap_at_call_val = float(peak_info["mcap_at_call"])
            if mcap_at_call_val > 0:
                db.update_peak_multiplier(call_id, current_mcap / mcap_at_call_val)
        except Exception:
            pass

    # ── Strategy A check ──────────────────────────────────────────────────────
    a_done = False
    try:
        position_a = db.get_open_paper_position(call_id, is_strategy_b=False)
        if position_a:
            entry_price    = position_a["entry_price"]
            peak_mcap_db   = float(position_a.get("peak_mcap") or 0.0)
            peak_mult_db   = float(position_a.get("peak_multiplier") or 0.0)
            current_mult   = (current_mcap / entry_price) if entry_price else 0.0
            if current_mult > peak_mult_db:
                db.update_paper_position_peak(call_id, current_mcap, current_mult, is_strategy_b=False)
                peak_mcap_db = current_mcap

            cached_peak_mcap = _a_realtime_peak_mcap.get(call_id, 0.0)
            peak_mcap = max(peak_mcap_db, cached_peak_mcap)
            if current_mcap > peak_mcap:
                peak_mcap = current_mcap
                _a_realtime_peak_mcap[call_id] = current_mcap
                print(
                    f"[ws_monitor] {mint[:8]} A realtime peak"
                    f" call_id={call_id}"
                    f" mcap=${current_mcap/1000:.1f}k"
                    f" mult={current_mult:.2f}x"
                )

            result_a    = paper_trader.check_exits(
                call_id, current_mcap, peak_mcap, entry_price, mint=mint, is_strategy_b=False
            )
            if result_a.should_exit:
                exit_mcap = result_a.exit_mcap or current_mcap
                paper_trader.close_position(call_id, exit_mcap, result_a.reason, is_strategy_b=False)
                _a_realtime_peak_mcap.pop(call_id, None)
                print(
                    f"[ws_monitor] {mint[:8]} A closed — {result_a.reason}"
                    f" @ ${exit_mcap/1000:.1f}k"
                )
                a_done = True
        else:
            _a_realtime_peak_mcap.pop(call_id, None)
            a_done = True  # no open A position
    except Exception as e:
        print(f"[ws_monitor] A exit check error {mint[:8]}: {e}")

    # ── Strategy B check ──────────────────────────────────────────────────────
    b_done = False
    try:
        position_b = db.get_open_paper_position(call_id, is_strategy_b=True)
        if position_b:
            entry_price      = position_b["entry_price"]
            peak_mcap_b      = float(position_b.get("peak_mcap") or 0.0)
            peak_mult_b      = float(position_b.get("peak_multiplier") or 0.0)
            current_mult_b   = (current_mcap / entry_price) if entry_price else 0.0
            if current_mult_b > peak_mult_b:
                db.update_paper_position_peak(call_id, current_mcap, current_mult_b, is_strategy_b=True)
                peak_mcap_b = current_mcap
            result_b    = paper_trader_b.check_exits(
                call_id, current_mcap, peak_mcap_b, entry_price, mint=mint, is_strategy_b=True
            )
            if result_b.should_exit:
                exit_mcap = result_b.exit_mcap or current_mcap
                paper_trader_b.close_position(call_id, exit_mcap, result_b.reason, is_strategy_b=True)
                print(
                    f"[ws_monitor] {mint[:8]} B closed — {result_b.reason}"
                    f" @ ${exit_mcap/1000:.1f}k"
                )
                b_done = True
        else:
            b_done = True  # no open B position
    except Exception as e:
        print(f"[ws_monitor] B exit check error {mint[:8]}: {e}")

    # ── Unsubscribe when both positions are closed ────────────────────────────
    if a_done and b_done:
        await _mgr.unsubscribe(ws, mint)


# ── Message dispatcher ────────────────────────────────────────────────────────

async def handle_messages(ws) -> None:
    async for raw in ws:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue

        # Subscribe confirmation: {"jsonrpc":"2.0", "id": N, "result": sub_id}
        if "result" in msg and isinstance(msg.get("result"), int) and "id" in msg:
            _mgr.on_confirm(msg["id"], msg["result"])
            continue

        # Log notification: {"method": "logsNotification", "params": {...}}
        if msg.get("method") != "logsNotification":
            continue

        params  = msg.get("params", {})
        sub_id  = params.get("subscription")
        if sub_id is None:
            continue

        value = (params.get("result") or {}).get("value") or {}
        signature = value.get("signature")

        # Skip failed transactions
        if value.get("err"):
            continue

        call_id = _mgr.call_id_for_sub(sub_id)
        if call_id is None:
            continue

        mint = _mgr.mint_for_call(call_id)
        if not mint:
            continue

        asyncio.create_task(handle_log_notification(ws, mint, call_id, signature))


# ── New position poller ───────────────────────────────────────────────────────

async def poll_new_positions(ws_ref: list) -> None:
    """
    Every POLL_INTERVAL seconds:
      - Subscribe to any new open positions not yet monitored.
      - Unsubscribe from mints that no longer have open positions.

    ws_ref is a one-element list holding the current ws (or None).
    """
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        ws = ws_ref[0]
        if ws is None:
            continue
        try:
            await sync_open_positions(ws)

        except Exception as e:
            print(f"[ws_monitor] poll error: {e}")


# ── Heartbeat ─────────────────────────────────────────────────────────────────

async def heartbeat(ws) -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_INTERVAL)
        try:
            await ws.ping()
        except Exception:
            break


# ── Main connection loop ──────────────────────────────────────────────────────

async def connect_with_retry() -> None:
    backoff   = 1
    ws_ref    = [None]   # mutable container for poll_new_positions closure

    poll_task = asyncio.create_task(poll_new_positions(ws_ref))
    try:
        while True:
            try:
                async with websockets.connect(
                    WS_URL,
                    ping_interval=None,   # manual heartbeat
                    open_timeout=20,
                    close_timeout=10,
                ) as ws:
                    ws_ref[0] = ws
                    backoff   = 1
                    print(f"[ws_monitor] connected — {WS_URL[:50]}...")

                    await _mgr.resubscribe_all(ws)
                    await sync_open_positions(ws)
                    print("[ws_monitor] reconnect sync complete")

                    hb_task = asyncio.create_task(heartbeat(ws))
                    try:
                        await handle_messages(ws)
                    finally:
                        hb_task.cancel()
                        ws_ref[0] = None

            except Exception as e:
                ws_ref[0] = None
                print(f"[ws_monitor] disconnected: {e!r} — retrying in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX)
    finally:
        poll_task.cancel()


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    print(f"[ws_monitor] starting — {WS_URL[:60]}...")

    try:
        db.ensure_ws_market_observations_table()
    except Exception as e:
        print(f"[ws_monitor] market observation table setup failed: {e}")

    # Seed subscriptions from currently open positions on startup
    try:
        positions_a = db.get_open_paper_positions(is_strategy_b=False)
        positions_b = db.get_open_paper_positions(is_strategy_b=True)
        all_positions = {p["call_id"]: p for p in positions_a}
        for p in positions_b:
            if p["call_id"] not in all_positions:
                all_positions[p["call_id"]] = p
        positions = list(all_positions.values())
        seeded = 0
        for pos in positions:
            mint = pos.get("mint_address") or ""
            if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
                # Pre-populate mint_to_call so resubscribe_all can send them
                _mgr._mint_to_call[mint] = pos["call_id"]
                seeded += 1
        print(f"[ws_monitor] seeded {seeded} open position(s) from DB")
    except Exception as e:
        print(f"[ws_monitor] startup seed failed: {e}")

    await connect_with_retry()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        db.close_conn()
