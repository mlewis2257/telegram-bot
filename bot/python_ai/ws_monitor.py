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
import order_flow
import rpc_pool
import paper_trader
import paper_trader_b
import live_trader
import peak_guard
import lane_policy
from exit_config import EXIT_A_PAPER, EXIT_B_PAPER

# Exit configs must match what monitor.py uses so WS and polling exits
# are always identical for the same position state.
_EXIT_A    = EXIT_A_PAPER
_EXIT_B    = EXIT_B_PAPER
_EXIT_LIVE = live_trader._LIVE_EXIT_CONFIG

# ── Coarse-exit routing (forward experiment: is shadow's coarse edge capturable?) ──
# Lanes whose exit variant is listed here are NOT exited by ws_monitor's dense per-swap
# (helius_tx) feed. ws still RATCHETS their peak every tick (harmless, monotonic), but the
# EXIT decision is delegated to the coarse Jupiter sweep (exit_monitor / sol-monitor), which
# prices on Jupiter's liquidity-aggregated mcap — so it rides THROUGH single-swap wicks the
# way shadow does, instead of trail-stopping paper out on a transient down-tick (e.g. CATTEARS
# 2.04x -> booked -1% on a one-tick -44% wick, while shadow rode it to 6.74x).
#
# Default empty = ws_monitor owns every paper exit (unchanged legacy behavior). Set
# WS_COARSE_EXIT_VARIANTS="early" to route the low_score anchor (and any other `early` lane)
# to the coarse sweep — the forward test of whether shadow's coarse-anchor edge is realizable
# on real paper positions. Paper-only (live is never gated here). Reversible instantly by
# clearing the env var + restarting ws_monitor. Do NOT add `ride`/`ride_vol` — the coarse-exit
# backtest showed coarsening those lanes RIDES the rugs down (net loss). Anchor/early only.
WS_COARSE_EXIT_VARIANTS = frozenset(
    v.strip() for v in os.getenv("WS_COARSE_EXIT_VARIANTS", "").split(",") if v.strip()
)


def _coarse_routed(position: dict | None) -> bool:
    """True if this lane's exit is delegated to the coarse Jupiter sweep (see
    WS_COARSE_EXIT_VARIANTS). ws_monitor keeps ratcheting the peak but must NOT fire the
    exit on its dense per-swap feed. Resolves the lane's variant exactly like the exit
    resolver everything else uses (lane_policy.lane_exit, honoring per-day overrides)."""
    if not WS_COARSE_EXIT_VARIANTS or not position:
        return False
    # NEVER coarse-route gamble lanes even if they run `early` (vip_mcap_gate does): the
    # coarse-exit backtest showed coarsening gamble/volatile lanes rides their rugs down.
    # Coarse routing is only ever safe on the non-gamble `early` anchors (low_score et al.).
    if position.get("vip_tier") in ("gamble", "gamble_risk"):
        return False
    variant = lane_policy.lane_exit(
        position.get("channel_handle"), position.get("vip_tier"),
        position.get("skip_reason"), position.get("entry_time"),
    )
    return variant in WS_COARSE_EXIT_VARIANTS

# ── Config ────────────────────────────────────────────────────────────────────

if not rpc_pool.count():
    raise RuntimeError("No RPC endpoints configured (set SOLANA_RPC_URL / SOLANA_RPC_URLS in .env)")
# PIN the websocket to ONE endpoint — do NOT rotate it per reconnect. Helius wss
# support is plan/key-specific, so rotating the long-lived socket onto a key whose
# plan handles wss differently caused instant ConnectionClosedError crash-loops.
# The HTTP path (getTransaction etc.) still rotates via rpc_pool — that's where the
# quota actually matters. Override with WS_RPC_URL if a specific key should serve wss.
_ws_http = (os.getenv("WS_RPC_URL") or os.getenv("SOLANA_RPC_URL")
            or (rpc_pool.endpoints()[0] if rpc_pool.count() else ""))
if not _ws_http:
    raise RuntimeError("No RPC endpoint for ws_monitor (set SOLANA_RPC_URL or WS_RPC_URL)")
WS_URL = _ws_http.replace("https://", "wss://").replace("http://", "ws://")

HEARTBEAT_INTERVAL    = 30    # seconds between pings
POLL_INTERVAL         = 2     # seconds between new-position polls
FETCH_COOLDOWN        = float(os.getenv("WS_FETCH_COOLDOWN", "0.5"))  # min seconds between price fetches per mint
RECONNECT_BACKOFF_MAX = 60    # max reconnect delay in seconds
LOG_COMMITMENT        = os.getenv("WS_LOG_COMMITMENT", "processed")
TX_MARKET_STATS_INTERVAL = int(os.getenv("WS_TX_MARKET_STATS_INTERVAL", "60"))
TX_MARKET_DEBUG          = os.getenv("WS_TX_MARKET_DEBUG", "").lower() in ("1", "true", "yes")

# ── Rate-limit state ──────────────────────────────────────────────────────────

_last_fetch: dict[str, float] = {}   # mint → monotonic time of last price fetch
_market_stats: Counter[str] = Counter()
_last_market_stats_print = 0.0
# Per-strategy realtime peak caches (call_id -> peak mcap).
# Live has its own independent cache so closing paper A never corrupts live peak.
_a_realtime_peak_mcap:    dict[int, float] = {}
_live_realtime_peak_mcap: dict[int, float] = {}
# Tracks when ws_monitor first processed each call_id. Used to skip helius_tx
# price parsing for brand-new positions where the price cache is not yet seeded.
_call_first_seen: dict[int, float] = {}
NEW_POSITION_HELIUS_GRACE = float(os.getenv("WS_NEW_POSITION_HELIUS_GRACE", "30.0"))


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
    Sync websocket subscriptions to all open positions (paper A, paper B, live).

    - Subscribe to mints with open positions that are not currently tracked.
    - Unsubscribe mints that no longer have any open position.
    """
    positions_a    = db.get_open_paper_positions(is_strategy_b=False)
    positions_b    = db.get_open_paper_positions(is_strategy_b=True)
    positions_live = db.get_open_live_positions()

    all_positions: dict[int, dict] = {p["call_id"]: p for p in positions_a}
    for p in positions_b:
        if p["call_id"] not in all_positions:
            all_positions[p["call_id"]] = p
    for p in positions_live:
        if p["call_id"] not in all_positions:
            all_positions[p["call_id"]] = p

    open_mints = set()
    for pos in all_positions.values():
        mint    = pos.get("mint_address") or ""
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

    # Track first time we process this call_id so we can seed the price cache
    # before trusting helius_tx delta-derived prices.
    if call_id not in _call_first_seen:
        _call_first_seen[call_id] = now
    position_age = now - _call_first_seen[call_id]
    in_grace = position_age < NEW_POSITION_HELIUS_GRACE

    market = None
    if signature and not in_grace:
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
            market = data_fetcher.fetch_token_price_fast(mint)
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

    # Attach live order flow to EVERY observation, not just helius_tx-success ones.
    # order_flow.ingest already ran during the tx-parse attempt above, so the rolling
    # state is fresh even when we fell back to a non-helius price (which carries no
    # order_flow). Without this, ~82% of swaps compute order_flow then discard it
    # along with the sanity-failed price, capping order_flow coverage at tx_success.
    if market.get("order_flow") is None:
        of = order_flow.metrics(mint)
        if of is not None:
            market = {**market, "order_flow": of}

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

    # ── Strategy A paper check ────────────────────────────────────────────────
    # effective_a_peak is shared with the live section below and must survive
    # the _a_realtime_peak_mcap.pop() that happens when paper A closes.
    effective_a_peak = 0.0
    a_done = False
    try:
        position_a = db.get_open_paper_position(call_id, is_strategy_b=False)
        if position_a:
            entry_price  = position_a["entry_price"]
            peak_mcap_db = float(position_a.get("peak_mcap") or 0.0)
            cached_peak  = _a_realtime_peak_mcap.get(call_id, 0.0)

            # Corroboration guard: a single spurious spike can't ratchet the peak.
            prior_peak  = max(peak_mcap_db, cached_peak)
            peak_mcap_a = peak_guard.guard_peak(f"wsA:{call_id}", current_mcap, prior_peak)
            if peak_mcap_a > prior_peak:
                _a_realtime_peak_mcap[call_id] = peak_mcap_a
                new_mult = (peak_mcap_a / entry_price) if entry_price else 0.0
                db.update_paper_position_peak(call_id, peak_mcap_a, new_mult, is_strategy_b=False)
                peak_mcap_db = peak_mcap_a
                print(
                    f"[ws_monitor] {mint[:8]} A realtime peak"
                    f" call_id={call_id}"
                    f" mcap=${peak_mcap_a/1000:.1f}k"
                    f" mult={new_mult:.2f}x"
                )
            effective_a_peak = peak_mcap_a  # capture before potential pop

            # Coarse-routed lanes (WS_COARSE_EXIT_VARIANTS): peak is ratcheted above, but the
            # EXIT is left to the coarse Jupiter sweep so a single-swap wick can't fire it here.
            if not _coarse_routed(position_a):
                # Guard the exit trigger against un-corroborated up-spikes: on a phantom
                # high, peak_mcap_a holds at the real corroborated level, so this caps the
                # spike while passing genuine climbs and pullbacks. Without it a single bad
                # reading fires a phantom take-profit (closing at a price that never was).
                eff_mcap_a = min(current_mcap, peak_mcap_a) if peak_mcap_a > 0 else current_mcap
                result_a = paper_trader.check_exits(
                    call_id, eff_mcap_a, peak_mcap_a, entry_price,
                    mint=mint, is_strategy_b=False, exit_config=_EXIT_A,
                )
                if result_a.should_exit:
                    exit_mcap = result_a.exit_mcap or eff_mcap_a
                    paper_trader.close_position(call_id, exit_mcap, result_a.reason, is_strategy_b=False)
                    _a_realtime_peak_mcap.pop(call_id, None)
                    print(f"[ws_monitor] {mint[:8]} A closed — {result_a.reason} @ ${exit_mcap/1000:.1f}k")
                    a_done = True
        else:
            _a_realtime_peak_mcap.pop(call_id, None)
            a_done = True
    except Exception as e:
        print(f"[ws_monitor] A exit check error {mint[:8]}: {e}")

    # ── Strategy B paper check ────────────────────────────────────────────────
    b_done = False
    try:
        position_b = db.get_open_paper_position(call_id, is_strategy_b=True)
        if position_b:
            entry_price  = position_b["entry_price"]
            peak_mcap_b  = float(position_b.get("peak_mcap") or 0.0)

            # Corroboration guard before advancing the peak.
            guarded_b = peak_guard.guard_peak(f"wsB:{call_id}", current_mcap, peak_mcap_b)
            if guarded_b > peak_mcap_b:
                new_mult = (guarded_b / entry_price) if entry_price else 0.0
                db.update_paper_position_peak(call_id, guarded_b, new_mult, is_strategy_b=True)
                peak_mcap_b = guarded_b

            # Coarse-routed lanes: peak ratcheted above; exit left to the coarse Jupiter sweep.
            if not _coarse_routed(position_b):
                eff_mcap_b = min(current_mcap, peak_mcap_b) if peak_mcap_b > 0 else current_mcap
                result_b = paper_trader_b.check_exits(
                    call_id, eff_mcap_b, peak_mcap_b, entry_price,
                    mint=mint, is_strategy_b=True, exit_config=_EXIT_B,
                )
                if result_b.should_exit:
                    exit_mcap = result_b.exit_mcap or eff_mcap_b
                    paper_trader_b.close_position(call_id, exit_mcap, result_b.reason, is_strategy_b=True)
                    print(f"[ws_monitor] {mint[:8]} B closed — {result_b.reason} @ ${exit_mcap/1000:.1f}k")
                    b_done = True
        else:
            b_done = True
    except Exception as e:
        print(f"[ws_monitor] B exit check error {mint[:8]}: {e}")

    # ── Live position check ───────────────────────────────────────────────────
    # Live uses its own independent peak cache (_live_realtime_peak_mcap).
    # This decouples live peak tracking from paper A — closing paper A no
    # longer zeroes out the live peak, which was the root cause of live
    # positions exiting at entry price when paper A closed first.
    live_done = False
    try:
        position_live = db.get_open_live_position(call_id)
        if position_live:
            entry_price   = position_live["entry_price"]
            peak_mcap_db  = position_live["peak_mcap"]
            peak_mult_db  = position_live["peak_multiplier"]
            current_mult  = (current_mcap / entry_price) if entry_price else 0.0

            # Merge DB peak, in-memory cache, and paper A's DexScreener peak.
            # Entry prices are confirmed identical, so paper A's peak is a valid
            # proxy for live — ensures a fast DexScreener pump that ws_monitor
            # missed on-chain still arms the live trail.
            cached_peak    = _live_realtime_peak_mcap.get(call_id, 0.0)
            # effective_a_peak is already guard-corroborated (paper A path above),
            # so fold it in directly; only the raw current_mcap needs the guard.
            prior_peak     = max(peak_mcap_db, cached_peak, effective_a_peak)
            peak_mcap_live = peak_guard.guard_peak(f"wsL:{call_id}", current_mcap, prior_peak)
            if peak_mcap_live > max(peak_mcap_db, cached_peak):
                _live_realtime_peak_mcap[call_id] = peak_mcap_live
                live_peak_mult = (peak_mcap_live / entry_price) if entry_price else 0.0
                db.update_live_position_peak(call_id, peak_mcap_live, live_peak_mult)
                print(
                    f"[ws_monitor] {mint[:8]} LIVE realtime peak"
                    f" call_id={call_id}"
                    f" mcap=${peak_mcap_live/1000:.1f}k"
                    f" mult={live_peak_mult:.2f}x"
                )

            # Same spike guard on the LIVE trigger — a phantom take-profit here would
            # close a real position at a price that never existed.
            eff_mcap_live = min(current_mcap, peak_mcap_live) if peak_mcap_live > 0 else current_mcap
            # Phase 2: real (sell-quote) basis when armed, else the feed triple unchanged.
            exit_cur, exit_peak, exit_entry, _basis = await live_trader.live_exit_basis(
                call_id, position_live, eff_mcap_live, peak_mcap_live, entry_price)
            result_live = live_trader.check_live_exits(
                call_id, exit_cur, exit_peak, exit_entry,
                exit_config=_EXIT_LIVE,
            )
            if result_live.should_exit:
                # Decision on real basis; record keeps feed exit mcap (exit_price col)
                # so it stays auditable against exit_price_fill.
                exit_mcap = eff_mcap_live
                closed = await live_trader.close_live_position(
                    call_id, exit_mcap, result_live.reason
                )
                if closed:
                    _live_realtime_peak_mcap.pop(call_id, None)
                    print(
                        f"[ws_monitor] {mint[:8]} LIVE closed — {result_live.reason}"
                        f" @ ${exit_mcap/1000:.1f}k [basis={_basis}]"
                    )
                live_done = True
            elif live_trader.LIVE_EXIT_QUOTE_LOG:
                # Phase-1 observation: quote real sellable value, log real vs feed
                # multiple. Read-only — drives no sells (see LIVE_EXIT_QUOTE_LOG).
                _eff_obs = await live_trader.live_effective_current(position_live)
                if _eff_obs:
                    _synth, _rmult = _eff_obs
                    _fmult = (current_mcap / entry_price) if entry_price else 0.0
                    print(f"[ws_monitor] {mint[:8]} LIVE MULT call_id={call_id} "
                          f"real={_rmult:.2f}x feed={_fmult:.2f}x")
        else:
            _live_realtime_peak_mcap.pop(call_id, None)
            live_done = True
    except Exception as e:
        print(f"[ws_monitor] live exit check error {mint[:8]}: {e}")

    # ── Unsubscribe when all three positions are closed ───────────────────────
    if a_done and b_done and live_done:
        _call_first_seen.pop(call_id, None)
        for _lane in ("wsA", "wsB", "wsL"):
            peak_guard.clear(f"{_lane}:{call_id}")
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
            ws_url = WS_URL   # pinned; HTTP rotation handled separately by rpc_pool
            try:
                async with websockets.connect(
                    ws_url,
                    ping_interval=None,   # manual heartbeat
                    open_timeout=20,
                    close_timeout=10,
                ) as ws:
                    ws_ref[0] = ws
                    connected_at = time.monotonic()
                    print(f"[ws_monitor] connected — {ws_url[:50]}...")

                    await _mgr.resubscribe_all(ws)
                    await sync_open_positions(ws)
                    print("[ws_monitor] reconnect sync complete")

                    hb_task = asyncio.create_task(heartbeat(ws))
                    try:
                        await handle_messages(ws)
                    finally:
                        hb_task.cancel()
                        ws_ref[0] = None

                    # Reset backoff ONLY after a stable connection. A connect that drops
                    # within seconds keeps the backoff growing, so a flapping endpoint
                    # isn't hammered with 1s reconnects (which makes Helius drop it more).
                    if time.monotonic() - connected_at >= 30:
                        backoff = 1

            except Exception as e:
                ws_ref[0] = None
                # If this endpoint 429'd / hit quota, cool it down so the next
                # reconnect rotates to a different key instead of hammering the
                # exhausted one.
                if rpc_pool.is_quota_error(getattr(e, "status_code", None), repr(e)):
                    rpc_pool.penalize(ws_url)
                print(f"[ws_monitor] disconnected: {e!r} — retrying in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, RECONNECT_BACKOFF_MAX)
    finally:
        poll_task.cancel()


# ── Entry point ───────────────────────────────────────────────────────────────

async def main() -> None:
    print(f"[ws_monitor] starting — {WS_URL[:60]}...")
    if WS_COARSE_EXIT_VARIANTS:
        print(f"[ws_monitor] COARSE-EXIT routing ON for variants {sorted(WS_COARSE_EXIT_VARIANTS)} "
              f"— those lanes' paper exits are delegated to the Jupiter sweep (peak still tracked here)")

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
