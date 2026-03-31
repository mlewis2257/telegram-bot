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
from dataclasses import dataclass
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

import db
import data_fetcher
import alert_bot

# ── Config ────────────────────────────────────────────────────────────────────

SOL_STRONG_ALERT = 1.0   # simulated SOL for strong_alert (85+)
SOL_ALERT        = 0.5   # simulated SOL for alert (70–84)

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
TRAIL_PEAK_MIN   = 2.5   # trailing stop only arms once peak >= 2.5x
HARD_STOP_PCT    = 0.50  # hard stop fires on 50% loss from entry
MAX_HOURS        = 24    # time stop after 24 hours open


# ── In-flight mint guard (prevents race-condition duplicate buys) ──────────────

_pending_mints: set[str] = set()
_pending_lock = asyncio.Lock()


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

            if "solearlytrending" in channel:
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
            if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
                try:
                    market = data_fetcher.fetch_token_price(mint)
                    if market and market.get("mcap"):
                        actual_entry = float(market["mcap"])
                except Exception as e:
                    print(f"[paper] price fetch failed for {symbol}: {e}")
            if actual_entry is None and mint:
                print(f"[paper] {symbol} DexScreener returned no mcap — using msg price ${msg_mcap/1000:.1f}k")

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
            if actual_entry and actual_entry > max_mcap:
                print(f"[paper] {symbol} skipped — mcap ${actual_entry/1000:.0f}k too high for {channel_handle or 'unknown'} (max ${max_mcap/1000:.0f}k)")
                db.set_call_skip_reason(call_id, "mcap_too_high")
                return

            db.open_paper_position(call_id, entry_price, sol_in, entry_time=position_entry_time)
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

    current_mult = current_mcap / entry_mcap

    # 10x take profit — checked first so high runners are labelled correctly
    if current_mult >= 10.0:
        return ExitResult(True, "10x_tp")

    # 5x take profit — checked before 3x so a position that bypassed
    # the 3x check is labelled correctly
    if current_mult >= TAKE_PROFIT_5X:
        return ExitResult(True, "5x_tp")

    # 3x take profit
    if current_mult >= TAKE_PROFIT_3X:
        return ExitResult(True, "3x_tp")

    # Trailing stop — tiered by how much the token has run.
    # Only activates at 2.5x+ to avoid exiting on small early bounces.
    if peak_mcap > 0:
        peak_mult = peak_mcap / entry_mcap
        if peak_mult >= 5.0:
            trail_pct = 0.35
        elif peak_mult >= 3.0:
            trail_pct = 0.30
        elif peak_mult >= TRAIL_PEAK_MIN:   # >= 2.5x
            trail_pct = 0.25
        else:
            trail_pct = None               # below 2.5x — let hard stop handle
        if trail_pct is not None:
            drawdown = (peak_mcap - current_mcap) / peak_mcap
            if drawdown >= trail_pct:
                return ExitResult(True, "trail_stop")

    # Hard stop — down 50% from entry (tightened from 60%)
    if current_mult <= (1.0 - HARD_STOP_PCT):
        return ExitResult(True, "hard_stop")

    # Time stop — position open longer than MAX_HOURS
    entry_time = position["entry_time"]
    if entry_time:
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - entry_time).total_seconds() / 3600
        if age_hours > MAX_HOURS:
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
