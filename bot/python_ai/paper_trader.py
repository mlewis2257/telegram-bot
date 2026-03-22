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

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

import db
import data_fetcher

# ── Config ────────────────────────────────────────────────────────────────────

SOL_STRONG_ALERT = 1.0   # simulated SOL for strong_alert (85+)
SOL_ALERT        = 0.5   # simulated SOL for alert (70–84)

TAKE_PROFIT_5X   = 5.0   # exit at 5x from entry
TAKE_PROFIT_3X   = 3.0   # exit at 3x from entry
TRAIL_PEAK_MIN   = 2.0   # trailing stop only arms once peak >= 2x
HARD_STOP_PCT    = 0.50  # hard stop fires on 50% loss from entry
MAX_HOURS        = 24    # time stop after 24 hours open


# ── Result type ───────────────────────────────────────────────────────────────

@dataclass
class ExitResult:
    should_exit: bool
    reason: str | None = None


# ── Public API ────────────────────────────────────────────────────────────────

def open_position(score_result: dict, token_data: dict) -> None:
    """
    Open a simulated paper trade position when an alert fires.

    Position size:
      strong_alert → 1.0 SOL simulated
      alert        → 0.5 SOL simulated

    entry_price = mcap_at_call from token_data.
    Never raises — a paper trade failure must never affect alert delivery.
    """
    try:
        label = score_result.get("label")
        if label == "strong_alert":
            sol_in = SOL_STRONG_ALERT
        elif label == "alert":
            sol_in = SOL_ALERT
        else:
            return  # only paper trade on actionable signals

        call_id  = score_result.get("call_id")
        symbol   = token_data.get("symbol", "?")
        mint     = token_data.get("mint_address")
        msg_mcap = float(token_data.get("mcap_at_call") or 0)

        if not call_id or msg_mcap <= 0:
            return

        actual_entry = None
        if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
            try:
                market = data_fetcher.fetch_token_price(mint)
                if market and market.get("mcap"):
                    actual_entry = float(market["mcap"])
            except Exception as e:
                print(f"[paper] price fetch failed for {symbol}: {e}")

        entry_price = actual_entry or msg_mcap

        max_slippage = float(os.getenv("MAX_ENTRY_SLIPPAGE_PCT", "50"))

        if actual_entry and msg_mcap > 0:
            slippage = ((actual_entry - msg_mcap) / msg_mcap) * 100
            print(f"[paper] {symbol} entry slippage: msg=${msg_mcap/1000:.1f}k actual=${actual_entry/1000:.1f}k ({slippage:+.1f}%)")
            if slippage > max_slippage:
                print(f"[paper] {symbol} SKIPPED — slippage {slippage:.0f}% exceeds max {max_slippage:.0f}%")
                return
            if slippage < -30:
                print(f"[paper] {symbol} SKIPPED — price dropped {slippage:.0f}% since message (dump)")
                return

        db.open_paper_position(call_id, entry_price, sol_in)
        print(f"[paper] opened {symbol}  call_id={call_id}  {sol_in} SOL @ {entry_price:.0f}")

    except Exception as e:
        db.safe_rollback()
        print(f"[paper_trader] open_position failed: {e}")


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
      3. Trailing stop  (peak >= 2x; tiered threshold: 25% / 20% / 15%)
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
    # Base threshold tightens at each tier; tightens a further 5% once
    # the token has ever peaked at 2x ("half secured").
    if peak_mcap > 0:
        peak_mult = peak_mcap / entry_mcap
        if peak_mult >= TRAIL_PEAK_MIN:
            if peak_mult >= 5.0:
                trail_pct = 0.20
            elif peak_mult >= 3.0:
                trail_pct = 0.25
            else:                       # peak >= 2x
                trail_pct = 0.30
            # half_secured: token has ever peaked at 2x (always True here
            # since TRAIL_PEAK_MIN == 2.0, but explicit for future-proofing)
            if peak_mult >= 2.0:
                trail_pct -= 0.05
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
