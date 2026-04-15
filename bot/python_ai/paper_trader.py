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

SOL_STRONG_ALERT = 0.5   # simulated SOL for strong_alert (85+)
SOL_ALERT        = 0.5   # simulated SOL for alert (70–84)
SOL_VIP_GAMBLE   = 0.25  # simulated SOL for experimental VIP gamble/gamble_risk entries

TAKE_PROFIT_5X   = 5.0   # exit at 5x from entry
TAKE_PROFIT_3X   = 3.0   # exit at 3x from entry
TRAIL_PEAK_MIN   = 2.0   # trailing stop only arms once peak >= 2.0x
HARD_STOP_PCT    = 0.35  # hard stop fires on 35% loss from entry
MAX_HOURS        = 24    # time stop after 24 hours open


# ── In-flight mint guard (prevents race-condition duplicate buys) ──────────────

_pending_mints: set[str] = set()
_pending_lock = asyncio.Lock()

# ── Position mint store (call_id → mint, for volume re-fetch in check_exits) ──
_position_mints: dict[int, str]   = {}

# ── Per-position VIP tier store (call_id → vip_tier) ──────────────────────────
# Populated on open for confirmed VIP gamble_risk/gamble positions so that
# check_exits can apply tighter hard stop (-35%) and shorter time stop (6h).
_position_tiers: dict[int, str] = {}

# ── Tier-specific exit thresholds ─────────────────────────────────────────────
VIP_GAMBLE_HARD_STOP_PCT = 0.30   # tighter: -30% vs default -50%
VIP_GAMBLE_MAX_HOURS     = 24.0   # same as default 24h


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

            call_id    = score_result.get("call_id")
            score_val  = float(score_result.get("score") or 0)
            msg_mcap   = float(token_data.get("mcap_at_call") or 0)

            if not call_id or msg_mcap <= 0:
                return

            if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
                if db.has_open_paper_position_for_mint(mint, is_strategy_b=False):
                    if "solwhaletrending" in channel:
                        existing_call_id = db.get_call_id_for_open_mint(mint, is_strategy_b=False)
                        if existing_call_id:
                            existing_pos = db.get_open_paper_position(existing_call_id, is_strategy_b=False)
                            if existing_pos:
                                current_mult = (entry_price / existing_pos["entry_price"]) if existing_pos["entry_price"] > 0 else 1.0
                                if current_mult >= 1.5:
                                    print(f"[paper] {symbol} PYRAMID — solwhaletrending confirms at {current_mult:.2f}x, adding {sol_in} SOL")
                                    db.set_call_skip_reason(call_id, None)
                                    # fall through to open_paper_position
                                else:
                                    print(f"[paper] {symbol} skipped — open position exists but only at {current_mult:.2f}x (need 1.5x for pyramid)")
                                    db.set_call_skip_reason(call_id, "duplicate")
                                    return
                            else:
                                db.set_call_skip_reason(call_id, "duplicate")
                                return
                        else:
                            db.set_call_skip_reason(call_id, "duplicate")
                            return
                    else:
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

            # ── gamble_risk on-chain data filters ─────────────────────────────
            if token_data.get("vip_tier") == "gamble_risk":
                # Fetch fresh on-chain data from DB — token_data always has None for VIP signals
                token_onchain = db.get_token_onchain_data(mint) if mint else {}
                bundle_pct    = token_onchain.get("bundle_pct_remaining")
                dev_tokens    = token_onchain.get("dev_tokens_made")
                security_flag = token_onchain.get("security_flag")

                # VIP gamble_risk messages contain no security data — vip_tier is the classification
                if security_flag == "warning":
                    print(f"[paper] {symbol} skipped — gamble_risk security=warning")
                    db.set_call_skip_reason(call_id, "security_warning")
                    return

                # Only apply bundle/dev filters if data exists
                if bundle_pct is not None and bundle_pct >= 10:
                    print(f"[paper] {symbol} skipped — gamble_risk bundle_pct={bundle_pct}")
                    db.set_call_skip_reason(call_id, "high_bundle")
                    return
                if dev_tokens is not None and dev_tokens >= 10:
                    print(f"[paper] {symbol} skipped — gamble_risk dev_tokens={dev_tokens}")
                    db.set_call_skip_reason(call_id, "serial_rugger")
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

            # ── Bucket quality filters (data-driven) ─────────────────────────
            bundle_pct = token_data.get("bundle_pct_remaining")
            fake_pct   = token_data.get("fake_vol_pct")
            if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")) and (bundle_pct is None or fake_pct is None):
                token_onchain = db.get_token_onchain_data(mint) or {}
                if bundle_pct is None:
                    bundle_pct = token_onchain.get("bundle_pct_remaining")
                if fake_pct is None:
                    fake_pct = token_onchain.get("fake_vol_pct")

            # VIP low-score bucket underperformed heavily; hard block for now.
            if channel_handle == "solhousesignal_vip" and score_val < 63:
                print(f"[paper] {symbol} skipped — vip score {score_val:.1f} < 63")
                db.set_call_skip_reason(call_id, "vip_low_score")
                return

            # Block the known low-quality combo on solhousesignal channels.
            is_bad_bundle = (bundle_pct is None) or (bundle_pct >= 10)
            is_bad_fake   = (fake_pct is None) or (fake_pct >= 5)
            if "solhousesignal" in channel_handle and is_bad_bundle and is_bad_fake:
                print(
                    f"[paper] {symbol} skipped — low_quality_bucket "
                    f"(bundle={bundle_pct}, fake={fake_pct}, score={score_val:.1f})"
                )
                db.set_call_skip_reason(call_id, "low_quality_bucket")
                return

            # Positive pocket marker for observability (sizing unchanged for now).
            if (
                channel_handle == "solhousesignal"
                and 70 <= score_val < 75
                and bundle_pct is not None and bundle_pct < 10
                and fake_pct is not None and fake_pct < 5
            ):
                print(f"[paper] {symbol} priority_bucket matched (70-74, bundle<10, fake<5)")

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

            # ── Free solhousesignal mcap range gate ───────────────────────────
            if "solhousesignal" in channel_handle and "vip" not in channel_handle:
                if entry_price < 20_000:
                    print(f"[paper] {symbol} skipped — solhousesignal mcap ${entry_price/1000:.1f}k below $20k minimum")
                    db.set_call_skip_reason(call_id, "mcap_too_low")
                    return
                if entry_price > 80_000:
                    print(f"[paper] {symbol} skipped — solhousesignal mcap ${entry_price/1000:.0f}k above $80k maximum")
                    db.set_call_skip_reason(call_id, "mcap_too_high")
                    return

            vip_tier_val = token_data.get("vip_tier") if is_vip_gamble else None
            db.open_paper_position(call_id, entry_price, sol_in,
                                   entry_time=position_entry_time,
                                   entry_volume=entry_volume,
                                   vip_tier=vip_tier_val)
            if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
                _position_mints[call_id] = mint
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
    is_strategy_b: bool = False,
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
    position = db.get_open_paper_position(call_id, is_strategy_b=is_strategy_b)
    if not position:
        return ExitResult(False)

    if entry_mcap <= 0:
        return ExitResult(False)

    current_mult      = current_mcap / entry_mcap
    is_vip_gamble_pos = position.get("vip_tier") in ("gamble_risk", "gamble")

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


def close_position(
    call_id: int,
    current_mcap: float,
    reason: str,
    is_strategy_b: bool = False,
) -> None:
    """
    Close an open paper trade position at current_mcap.

    sol_out = sol_in * (current_mcap / entry_price)
    P&L columns are GENERATED in the schema and update automatically.
    Never raises.
    """
    try:
        position = db.get_open_paper_position(call_id, is_strategy_b=is_strategy_b)
        if not position:
            return

        entry_price = position["entry_price"]
        sol_in      = position["sol_in"]

        if entry_price <= 0:
            return

        sol_out = sol_in * (current_mcap / entry_price)
        db.close_paper_position(call_id, current_mcap, sol_out, reason, is_strategy_b=is_strategy_b)
        pnl = sol_out - sol_in
        print(
            f"[paper] closed  call_id={call_id}  reason={reason}"
            f"  sol_in={sol_in:.2f}  sol_out={sol_out:.2f}  pnl={pnl:+.3f} SOL"
        )

    except Exception as e:
        db.safe_rollback()
        print(f"[paper_trader] close_position failed: {e}")
