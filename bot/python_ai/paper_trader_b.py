"""
paper_trader_b.py — Strategy B paper trade tracker (AB test variant).

Identical entry logic to paper_trader.py. Exit strategy differs:
  • 2x take profit (no 3x / 5x / 10x)
  • Trailing stop arms at 1.5x peak, flat 25% drawdown threshold (no tiers,
    no volume confirmation)
  • Hard stop at -35% from entry (tighter than A's -50%)
  • Time stop at 24 hours (same as A)

All positions use is_strategy_b=TRUE in the DB so results are isolated
from Strategy A and can be compared directly.

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

# ── Config ────────────────────────────────────────────────────────────────────

SOL_STRONG_ALERT = 0.5   # simulated SOL for strong_alert (85+)
SOL_ALERT        = 0.5   # simulated SOL for alert (70–84)
SOL_VIP_GAMBLE   = 0.25  # simulated SOL for experimental VIP gamble/gamble_risk entries

TAKE_PROFIT_3X   = 3.0   # exit at 3x from entry (no 5x/10x — let trail stop handle runners)
TRAIL_PEAK_MIN   = 2.0   # trailing stop arms once peak >= 2.0x
TRAIL_PCT        = 0.25  # flat 25% drawdown from peak (no tiers)
HARD_STOP_PCT    = 0.35  # hard stop fires on 35% loss from entry (tighter than A's 50%)
MAX_HOURS        = 24    # time stop after 24 hours open

# ── VIP gamble tier exit thresholds ───────────────────────────────────────────
VIP_GAMBLE_HARD_STOP_PCT = 0.30   # tighter: -30% for gamble_risk positions


# ── In-flight mint guard ───────────────────────────────────────────────────────

_pending_mints_b: set[str] = set()
_pending_lock_b = asyncio.Lock()

# ── Position mint store ────────────────────────────────────────────────────────
_position_mints_b: dict[int, str]   = {}
_last_vol_check_b: dict[int, float] = {}

# ── Per-position VIP tier store ────────────────────────────────────────────────
_position_tiers_b: dict[int, str] = {}


# ── Result type ───────────────────────────────────────────────────────────────

@dataclass
class ExitResult:
    should_exit: bool
    reason: str | None = None


# ── Public API ────────────────────────────────────────────────────────────────

async def open_position(score_result: dict, token_data: dict) -> None:
    """
    Open a Strategy B simulated paper trade position when an alert fires.

    Identical entry logic to paper_trader.py. Uses is_strategy_b=True for
    all DB calls so positions are isolated from Strategy A.
    Never raises — a paper trade failure must never affect alert delivery.
    """
    position_entry_time = datetime.now(timezone.utc)
    symbol = token_data.get("symbol", "?")
    mint   = token_data.get("mint_address")

    async with _pending_lock_b:
        if mint in _pending_mints_b:
            print(f"[paper_b] {symbol} ({(mint or '')[:8]}...) skipped — buy already in-flight for this mint")
            return
        _pending_mints_b.add(mint)
    try:
        try:
            label = score_result.get("label")
            channel = token_data.get("channel_tag") or token_data.get("channel_handle", "")

            if token_data.get("sol_in_override"):
                sol_in = float(token_data["sol_in_override"])
            elif "solearlytrending" in channel:
                sol_in = SOL_ALERT
            elif label == "strong_alert":
                sol_in = SOL_STRONG_ALERT
            else:
                sol_in = SOL_ALERT

            call_id    = score_result.get("call_id")
            score_val  = float(score_result.get("score") or 0)
            msg_mcap   = float(token_data.get("mcap_at_call") or 0)

            if not call_id or msg_mcap <= 0:
                return

            if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
                if db.has_open_paper_position_for_mint(mint, is_strategy_b=True):
                    if "solwhaletrending" in channel:
                        existing_call_id = db.get_call_id_for_open_mint(mint, is_strategy_b=True)
                        if existing_call_id:
                            existing_pos = db.get_open_paper_position(existing_call_id, is_strategy_b=True)
                            if existing_pos:
                                current_mult = (entry_price / existing_pos["entry_price"]) if existing_pos["entry_price"] > 0 else 1.0
                                if current_mult >= 1.5:
                                    print(f"[paper_b] {symbol} PYRAMID — solwhaletrending confirms at {current_mult:.2f}x, adding {sol_in} SOL")
                                    # fall through to open_paper_position
                                else:
                                    print(f"[paper_b] {symbol} skipped — open position exists but only at {current_mult:.2f}x (need 1.5x for pyramid)")
                                    return
                            else:
                                return
                        else:
                            return
                    else:
                        print(f"[paper_b] {symbol} skipped — open position already exists for this mint")
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
                    print(f"[paper_b] price fetch failed for {symbol}: {e}")
            if actual_entry is None and mint:
                print(f"[paper_b] {symbol} DexScreener returned no mcap — using msg price ${msg_mcap/1000:.1f}k")

            is_vip_gamble = (token_data.get("sol_in_override") == SOL_VIP_GAMBLE)
            if is_vip_gamble and actual_entry is not None:
                if actual_entry < 10_000:
                    print(f"[paper_b] {symbol} skipped — mcap ${actual_entry/1000:.1f}k below $10k minimum for vip gamble")
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
                    print(f"[paper_b] {symbol} skipped — gamble_risk security=warning")
                    db.set_call_skip_reason(call_id, "security_warning")
                    return

                # Only apply bundle/dev filters if data exists
                if bundle_pct is not None and bundle_pct >= 10:
                    print(f"[paper_b] {symbol} skipped — gamble_risk bundle_pct={bundle_pct}")
                    db.set_call_skip_reason(call_id, "high_bundle")
                    return
                if dev_tokens is not None and dev_tokens >= 10:
                    print(f"[paper_b] {symbol} skipped — gamble_risk dev_tokens={dev_tokens}")
                    db.set_call_skip_reason(call_id, "serial_rugger")
                    return

            entry_price = actual_entry or msg_mcap

            security_flag = token_data.get("security_flag")
            if security_flag == "warning":
                print(f"[paper_b] {symbol} skipped — security=warning")
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
                print(f"[paper_b] {symbol} skipped — vip score {score_val:.1f} < 63")
                db.set_call_skip_reason(call_id, "vip_low_score")
                return

            # Block the known low-quality combo on solhousesignal channels.
            is_bad_bundle = (bundle_pct is None) or (bundle_pct >= 10)
            is_bad_fake   = (fake_pct is None) or (fake_pct >= 5)
            if channel_handle == "solhousesignal" and is_bad_bundle and is_bad_fake:
                print(
                    f"[paper_b] {symbol} skipped — low_quality_bucket "
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
                print(f"[paper_b] {symbol} priority_bucket matched (70-74, bundle<10, fake<5)")

            if channel_handle == "solhousesignal_vip" and token_data.get("vip_tier") == "safe":
                if entry_price < 15_000:
                    print(f"[paper_b] {symbol} skipped — VIP safe mcap ${entry_price/1000:.1f}k below $15k minimum")
                    return
                if entry_price > 150_000:
                    print(f"[paper_b] {symbol} skipped — VIP safe mcap ${entry_price/1000:.0f}k above $150k maximum")
                    return

            # ── Free solhousesignal mcap range gate ───────────────────────────
            if "solhousesignal" in channel_handle and "vip" not in channel_handle:
                if entry_price < 20_000:
                    print(f"[paper_b] {symbol} skipped — solhousesignal mcap ${entry_price/1000:.1f}k below $20k minimum")
                    return
                if entry_price > 80_000:
                    print(f"[paper_b] {symbol} skipped — solhousesignal mcap ${entry_price/1000:.0f}k above $80k maximum")
                    return

            vip_tier_val = token_data.get("vip_tier") if is_vip_gamble else None
            db.open_paper_position(call_id, entry_price, sol_in,
                                   entry_time=position_entry_time,
                                   entry_volume=entry_volume,
                                   is_strategy_b=True,
                                   vip_tier=vip_tier_val)
            if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
                _position_mints_b[call_id] = mint
            if vip_tier_val in ("gamble_risk", "gamble"):
                _position_tiers_b[call_id] = vip_tier_val
            print(f"[paper_b] opened {symbol}  call_id={call_id}  {sol_in} SOL @ {entry_price:.0f}")

        except Exception as e:
            db.safe_rollback()
            print(f"[paper_trader_b] open_position failed: {e}")
    finally:
        async with _pending_lock_b:
            _pending_mints_b.discard(mint)


def check_exits(
    call_id: int,
    current_mcap: float,
    peak_mcap: float,
    entry_mcap: float,
    mint: str = None,
    is_strategy_b: bool = True,
) -> ExitResult:
    """
    Strategy B exit logic.

    Exit conditions checked in priority order:
      1. 3x take profit (no 5x/10x — trail stop captures runners above 3x)
      2. Trailing stop  (peak >= 2.0x; flat 25% drawdown — no volume check, no tiers)
      3. Hard stop      (down 35% from entry — tighter than Strategy A's 50%)
      4. Time stop      (open > 24 hours)
    """
    position = db.get_open_paper_position(call_id, is_strategy_b=is_strategy_b)
    if not position:
        return ExitResult(False)

    if entry_mcap <= 0:
        return ExitResult(False)

    current_mult      = current_mcap / entry_mcap
    is_vip_gamble_pos = position.get("vip_tier") in ("gamble_risk", "gamble")

    # 3x take profit — skipped for VIP gamble tiers
    if not is_vip_gamble_pos and current_mult >= TAKE_PROFIT_3X:
        return ExitResult(True, "3x_tp")

    # Trailing stop — flat 25% from peak once peak >= 2.0x. No volume check, no tiers.
    if peak_mcap > 0:
        peak_mult = peak_mcap / entry_mcap
        if peak_mult >= TRAIL_PEAK_MIN:
            drawdown = (peak_mcap - current_mcap) / peak_mcap
            if drawdown >= TRAIL_PCT:
                return ExitResult(True, "trail_stop")

    # Hard stop — tighter for VIP gamble positions (-30%) vs standard (-35%)
    hard_stop_pct = VIP_GAMBLE_HARD_STOP_PCT if is_vip_gamble_pos else HARD_STOP_PCT
    if current_mult <= (1.0 - hard_stop_pct):
        return ExitResult(True, "hard_stop")

    # Time stop — 24 hours
    entry_time = position["entry_time"]
    if entry_time:
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - entry_time).total_seconds() / 3600
        if age_hours > MAX_HOURS:
            return ExitResult(True, "time_stop")

    return ExitResult(False)


def close_position(
    call_id: int,
    current_mcap: float,
    reason: str,
    is_strategy_b: bool = True,
) -> None:
    """
    Close an open Strategy B paper trade position at current_mcap.

    sol_out = sol_in * (current_mcap / entry_price)
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
            f"[paper_b] closed  call_id={call_id}  reason={reason}"
            f"  sol_in={sol_in:.2f}  sol_out={sol_out:.2f}  pnl={pnl:+.3f} SOL"
        )

    except Exception as e:
        db.safe_rollback()
        print(f"[paper_trader_b] close_position failed: {e}")
