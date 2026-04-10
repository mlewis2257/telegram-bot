"""
paper_trader.py — Simulated trade tracker for conviction-scored signals.

Logs paper trade entries when alerts fire and checks exit conditions on
each monitor pass. All positions carry is_simulation=TRUE and never
touch real funds.

Public API
----------
open_position(score_result, token_data)             -> None
check_exits(call_id, current_mcap, peak_mcap,
            entry_mcap)                             -> ExitResult
close_position(call_id, current_mcap, reason)       -> None
"""

import asyncio
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

import db
import data_fetcher
import alert_bot

# ── Config ────────────────────────────────────────────────────────────────────

SOL_STRONG_ALERT = 1.0   # simulated SOL for strong_alert (85+)
SOL_ALERT        = 0.5   # simulated SOL for alert (70–84)
SOL_VIP_GAMBLE   = 0.25  # simulated SOL for experimental VIP gamble/gamble_risk entries

# ── Per-channel mcap entry limits ──────────────────────────────────────────────
MCAP_LIMITS = {
    'solhousesignal_vip': 350_000,
    'solwhaletrending':   100_000,
    'solearlytrending':    75_000,
    'solhousesignal':      50_000,
}
DEFAULT_MCAP_LIMIT = 75_000  # fallback for unknown channels

TAKE_PROFIT_5X   = 5.0   # exit at 5x from entry
TAKE_PROFIT_3X   = 3.0   # exit at 3x from entry
TRAIL_PEAK_MIN   = 2.0   # trailing stop only arms once peak >= 2.0x
HARD_STOP_PCT    = 0.50  # hard stop fires on 50% loss from entry
MAX_HOURS        = 24    # time stop after 24 hours open


# ── In-flight mint guard (prevents race-condition duplicate buys) ──────────────

_pending_mints: set[str] = set()
_pending_lock = asyncio.Lock()

# ── Position mint store (call_id → mint, for volume re-fetch in check_exits) ──
_position_mints: dict[int, str]   = {}
_last_vol_check: dict[int, float] = {}   # call_id → last volume check timestamp
VOL_CHECK_INTERVAL = 30                  # seconds between volume checks per position

# ── Per-position VIP tier store (call_id → vip_tier) ──────────────────────────
# Populated on open for confirmed VIP gamble_risk/gamble positions so that
# check_exits can apply tighter hard stop (-35%) and shorter time stop (6h).
_position_tiers: dict[int, str] = {}

# ── Tier-specific exit thresholds ─────────────────────────────────────────────
VIP_GAMBLE_HARD_STOP_PCT = 0.35   # tighter: -35% vs default -50%
VIP_GAMBLE_MAX_HOURS     = 6.0    # shorter: 6h  vs default 24h


# ── Result type ───────────────────────────────────────────────────────────────

@dataclass
class ExitResult:
    should_exit: bool
    reason: str | None = None


# ── Public API ────────────────────────────────────────────────────────────────

async def open_position(score_result: dict, token_data: dict) -> None:
    """
    Open a simulated paper trade position when an alert fires.

    Position size:
      strong_alert → 1.0 SOL simulated
      alert        → 0.5 SOL simulated

    entry_price = mcap_at_call from token_data.
    Never raises — a paper trade failure must never affect alert delivery.
    """
    position_entry_time = datetime.now(timezone.utc)  # capture before any async delays
    symbol = token_data.get("symbol", "?")
    mint   = token_data.get("mint_address")

    # ── In-flight mint guard ───────────────────────────────────────────────────
    async with _pending_lock:
        if mint in _pending_mints:
            print(f"[paper] {symbol} ({(mint or '')[:8]}...) skipped — buy already in-flight for this mint")
            return
        _pending_mints.add(mint)
    try:
        try:
            label = score_result.get("label")
            channel = token_data.get("channel_tag") or token_data.get("channel_handle", "")

            if token_data.get("sol_in_override"):
                sol_in = float(token_data["sol_in_override"])
            elif "solearlytrending" in channel:
                sol_in = SOL_ALERT  # always 0.5 SOL regardless of score
            elif label == "strong_alert":
                sol_in = SOL_STRONG_ALERT  # 1.0 SOL for other channels
            else:
                sol_in = SOL_ALERT  # 0.5 SOL

            call_id  = score_result.get("call_id")
            msg_mcap = float(token_data.get("mcap_at_call") or 0)

            if not call_id or msg_mcap <= 0:
                return

            if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
                if db.has_open_paper_position_for_mint(mint):
                    print(f"[paper] {symbol} skipped — open position already exists for this mint")
                    db.set_call_skip_reason(call_id, "duplicate")
                    return

            actual_entry = None
            entry_volume = None
            if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
                try:
                    market = data_fetcher.fetch_token_price(mint)
                    if market and market.get("mcap"):
                        actual_entry = float(market["mcap"])
                    if market and market.get("volume_h1"):
                        entry_volume = float(market["volume_h1"])
                except Exception as e:
                    print(f"[paper] price fetch failed for {symbol}: {e}")
            if actual_entry is None and mint:
                print(f"[paper] {symbol} DexScreener returned no mcap — using msg price ${msg_mcap/1000:.1f}k")

            # ── VIP gamble-tier filters (gamble_risk / gamble only) ───────────────
            # Checks run against actual_entry (live DexScreener price at open time),
            # not msg_mcap (the VIP call price from 5+ minutes ago).
            is_vip_gamble = (token_data.get("sol_in_override") == SOL_VIP_GAMBLE)
            if is_vip_gamble and actual_entry is not None:
                # Minimum mcap gate — token must be trading above $10k at open time
                if actual_entry < 10_000:
                    print(f"[paper] {symbol} skipped — mcap ${actual_entry/1000:.1f}k below $10k minimum for vip gamble")
                    db.set_call_skip_reason(call_id, "mcap_too_low")
                    return

            entry_price = actual_entry or msg_mcap

            # ── Entry gate ────────────────────────────────────────────────────
            security_flag = token_data.get("security_flag")
            if security_flag == "warning":
                print(f"[paper] {symbol} skipped — security=warning")
                db.set_call_skip_reason(call_id, "security_warning")
                return

            channel_handle = (
                token_data.get("channel_tag") or
                token_data.get("channel_handle") or
                ""
            ).lstrip("@")
            max_mcap = MCAP_LIMITS.get(channel_handle, DEFAULT_MCAP_LIMIT)
            if "solhousesignal" in channel_handle and "vip" not in channel_handle and score_result and int(score_result.get("score", 0)) in (63, 64):
                max_mcap = 100_000  # score 63/64 signals get higher mcap allowance
            if entry_price > max_mcap:
                print(f"[paper] {symbol} skipped — mcap ${entry_price/1000:.0f}k too high for {channel_handle or 'unknown'} (max ${max_mcap/1000:.0f}k)")
                db.set_call_skip_reason(call_id, "mcap_too_high")
                return

            # ── VIP safe tier mcap range gate ─────────────────────────────────
            if channel_handle == "solhousesignal_vip" and token_data.get("vip_tier") == "safe":
                if entry_price < 15_000:
                    print(f"[paper] {symbol} skipped — VIP safe mcap ${entry_price/1000:.1f}k below $15k minimum")
                    db.set_call_skip_reason(call_id, "mcap_too_low")
                    return
                if entry_price > 150_000:
                    print(f"[paper] {symbol} skipped — VIP safe mcap ${entry_price/1000:.0f}k above $150k maximum")
                    db.set_call_skip_reason(call_id, "mcap_too_high")
                    return

            db.open_paper_position(call_id, entry_price, sol_in,
                                   entry_time=position_entry_time,
                                   entry_volume=entry_volume)
            if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
                _position_mints[call_id] = mint
            vip_tier_val = token_data.get("vip_tier") if is_vip_gamble else None
            if vip_tier_val in ("gamble_risk", "gamble"):
                _position_tiers[call_id] = vip_tier_val
            print(f"[paper] opened {symbol}  call_id={call_id}  {sol_in} SOL @ {entry_price:.0f}")

        except Exception as e:
            db.safe_rollback()
            print(f"[paper_trader] open_position failed: {e}")
    finally:
        async with _pending_lock:
            _pending_mints.discard(mint)


def check_exits(
    call_id: int,
    current_mcap: float,
    peak_mcap: float,
    entry_mcap: float,
    mint: str = None,
) -> ExitResult:
    """
    Check whether the open paper position for call_id should be exited.

    Returns ExitResult(should_exit=False) immediately if no open position
    exists — safe to call unconditionally on every monitor pass.

    Exit conditions checked in priority order:
      1. 5x take profit
      2. 3x take profit
      3. Trailing stop  (peak >= 2.5x; tiered threshold: 25% / 30% / 35%)
      4. Hard stop      (down 50% from entry)
      5. Time stop      (open > 24 hours)
    """
    position = db.get_open_paper_position(call_id)
    if not position:
        return ExitResult(False)

    if entry_mcap <= 0:
        return ExitResult(False)

    current_mult   = current_mcap / entry_mcap
    is_vip_gamble_pos = _position_tiers.get(call_id) in ("gamble_risk", "gamble")

    # 10x take profit — checked first so high runners are labelled correctly
    if current_mult >= 10.0:
        return ExitResult(True, "10x_tp")

    # 5x take profit — checked before 3x so a position that bypassed
    # the 3x check is labelled correctly
    if current_mult >= TAKE_PROFIT_5X:
        return ExitResult(True, "5x_tp")

    # 3x take profit — skipped for VIP gamble tiers (let trail stop maximise runners)
    if not is_vip_gamble_pos and current_mult >= TAKE_PROFIT_3X:
        return ExitResult(True, "3x_tp")

    # Trailing stop — tiered by how much the token has run.
    # Only activates at 2.0x+ to avoid exiting on small early bounces.
    if peak_mcap > 0:
        peak_mult = peak_mcap / entry_mcap
        if peak_mult >= 10.0:
            trail_pct = 0.20               # lock in massive gains
        elif peak_mult >= 5.0:
            trail_pct = 0.25
        elif peak_mult >= 3.0:
            trail_pct = 0.30
        elif peak_mult >= TRAIL_PEAK_MIN:  # >= 2.0x
            trail_pct = 0.40               # wide — let small moves breathe
        else:
            trail_pct = None               # below 2.0x — let hard stop handle
        if trail_pct is not None:
            drawdown = (peak_mcap - current_mcap) / peak_mcap
            if drawdown >= trail_pct:
                # Volume confirmation — only hold if volume still healthy AND below 5x
                if peak_mult < 5.0:
                    entry_vol = db.get_paper_position_entry_volume(call_id)
                    mint_addr = mint or _position_mints.get(call_id)
                    if entry_vol and entry_vol > 0 and mint_addr:
                        now        = time.monotonic()
                        last_check = _last_vol_check.get(call_id, 0)
                        if now - last_check < VOL_CHECK_INTERVAL:
                            # Too soon for a fresh volume fetch — hold this cycle
                            pass
                        else:
                            _last_vol_check[call_id] = now
                            try:
                                current_market = data_fetcher.fetch_token_price(mint_addr)
                                current_vol    = (current_market or {}).get("volume_h1") or 0
                                vol_ratio      = float(current_vol) / entry_vol if current_vol else 0.0
                                print(f"[paper] {mint_addr[:8]} vol_ratio={vol_ratio:.2f} — {'holding' if vol_ratio > 0.4 else 'exiting'}")
                                if vol_ratio > 0.4:
                                    pass  # volume still healthy — hold through pullback
                                else:
                                    return ExitResult(True, "trail_stop")
                            except Exception:
                                return ExitResult(True, "trail_stop")
                    else:
                        return ExitResult(True, "trail_stop")
                else:
                    # 5x+ — protect gains regardless of volume
                    return ExitResult(True, "trail_stop")

    # Hard stop — tighter for VIP gamble tiers (-35%) vs default (-50%)
    hard_stop_pct = VIP_GAMBLE_HARD_STOP_PCT if is_vip_gamble_pos else HARD_STOP_PCT
    if current_mult <= (1.0 - hard_stop_pct):
        return ExitResult(True, "hard_stop")

    # Time stop — shorter for VIP gamble tiers (6h) vs default (24h)
    max_hours = VIP_GAMBLE_MAX_HOURS if is_vip_gamble_pos else MAX_HOURS
    entry_time = position["entry_time"]
    if entry_time:
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - entry_time).total_seconds() / 3600
        if age_hours > max_hours:
            return ExitResult(True, "time_stop")

    return ExitResult(False)


def close_position(call_id: int, current_mcap: float, reason: str) -> None:
    """
    Close an open paper trade position at current_mcap.

    sol_out = sol_in * (current_mcap / entry_price)
    P&L columns are GENERATED in the schema and update automatically.
    Never raises.
    """
    try:
        position = db.get_open_paper_position(call_id)
        if not position:
            return

        entry_price = position["entry_price"]
        sol_in      = position["sol_in"]

        if entry_price <= 0:
            return

        sol_out = sol_in * (current_mcap / entry_price)
        db.close_paper_position(call_id, current_mcap, sol_out, reason)
        pnl = sol_out - sol_in
        print(
            f"[paper] closed  call_id={call_id}  reason={reason}"
            f"  sol_in={sol_in:.2f}  sol_out={sol_out:.2f}  pnl={pnl:+.3f} SOL"
        )

    except Exception as e:
        db.safe_rollback()
        print(f"[paper_trader] close_position failed: {e}")
