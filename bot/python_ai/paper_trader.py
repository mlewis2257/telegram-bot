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

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(__file__))

import db
import data_fetcher
import alert_bot
from strategy_config import STRATEGY_A_V2026_05_22
from strategy_engine import StrategyCallContext, evaluate_strategy_a_entry
from exit_config import ExitConfig, ExitResult, apply_exit_config, EXIT_A_PAPER

# ── Config ────────────────────────────────────────────────────────────────────

SOL_STRONG_ALERT = 0.5   # simulated SOL for strong_alert (85+)
SOL_ALERT        = 0.5   # simulated SOL for alert (70–84)
SOL_SOLHOUSE_70_74 = 0.25  # smaller size for weaker free solhousesignal alerts
SOL_VIP_GAMBLE   = 0.25  # simulated SOL for experimental VIP gamble/gamble_risk entries

# Legacy constants — kept so live_trader.py imports still resolve.
# Exit logic now lives in EXIT_A_PAPER (exit_config.py).
TAKE_PROFIT_5X   = 5.0
TRAIL_PEAK_MIN   = 2.0
HARD_STOP_PCT    = 0.35
MAX_HOURS        = 24
LOCAL_TZ         = ZoneInfo("America/Los_Angeles")
QUIET_HOURS_PST  = set(STRATEGY_A_V2026_05_22.quiet_hours_pst)
FREE_SOLHOUSE_BLOCKED_HOURS_PST = set(STRATEGY_A_V2026_05_22.free_blocked_hours_pst)
FREE_SOLHOUSE_WEAK_30K_50K_HOURS_PST = set(STRATEGY_A_V2026_05_22.free_weak_30k_50k_hours_pst)
VIP_SAFE_ALLOWED_HOURS_PST = set(STRATEGY_A_V2026_05_22.vip_safe_allowed_hours_pst)
VIP_GAMBLE_ALLOWED_HOURS_PST = set(STRATEGY_A_V2026_05_22.vip_gamble_allowed_hours_pst)
VIP_GAMBLE_WEAK_15K_25K_HOURS_PST = set(STRATEGY_A_V2026_05_22.vip_gamble_weak_15k_25k_hours_pst)


# ── In-flight mint guard (prevents race-condition duplicate buys) ──────────────

_pending_mints: set[str] = set()
_pending_lock = asyncio.Lock()

# ── Position mint store (call_id → mint, for volume re-fetch in check_exits) ──
_position_mints: dict[int, str]   = {}

# ── Tier-specific exit thresholds ─────────────────────────────────────────────
VIP_GAMBLE_HARD_STOP_PCT = 0.30   # tighter: -30% vs default -35%
VIP_GAMBLE_MAX_HOURS     = 24.0   # same as default 24h


# ExitResult imported from exit_config — single definition shared by all traders.

# ── Public API ────────────────────────────────────────────────────────────────

async def open_position(score_result: dict, token_data: dict) -> None:
    """
    Open a simulated paper trade position when an alert fires.

    Position size:
      strong_alert → 0.5 SOL simulated
      alert        → 0.5 SOL simulated

    entry_price prefers the live fetched mcap at open time, falling back to
    mcap_at_call from token_data when live data is unavailable.
    Never raises — a paper trade failure must never affect alert delivery.
    """
    position_entry_time = datetime.now(timezone.utc)  # capture before any async delays
    symbol = token_data.get("symbol", "?")
    mint   = token_data.get("mint_address")
    call_id = score_result.get("call_id") if score_result else None

    # ── In-flight mint guard ───────────────────────────────────────────────────
    async with _pending_lock:
        if mint in _pending_mints:
            print(f"[paper] {symbol} ({(mint or '')[:8]}...) skipped — buy already in-flight for this mint")
            db.set_call_skip_reason(call_id, "pending_duplicate")
            return
        _pending_mints.add(mint)
    try:
        try:
            label = score_result.get("label")
            channel = token_data.get("channel_tag") or token_data.get("channel_handle", "")

            score_val  = float(score_result.get("score") or 0)
            channel_handle = (
                token_data.get("channel_tag") or
                token_data.get("channel_handle") or
                ""
            ).lstrip("@")

            if token_data.get("sol_in_override"):
                sol_in = float(token_data["sol_in_override"])
            elif "solearlytrending" in channel:
                sol_in = SOL_ALERT  # always 0.5 SOL regardless of score
            elif channel_handle == "solhousesignal" and 70 <= score_val < 75:
                sol_in = SOL_SOLHOUSE_70_74
            elif label == "strong_alert":
                sol_in = SOL_STRONG_ALERT  # 0.5 SOL for other channels
            else:
                sol_in = SOL_ALERT  # 0.5 SOL

            msg_mcap   = float(token_data.get("mcap_at_call") or 0)

            if not call_id:
                return

            local_hour = position_entry_time.astimezone(LOCAL_TZ).hour
            free_uses_custom_filters = (channel_handle == "solhousesignal")
            vip_uses_lane_allowlist = (channel_handle == "solhousesignal_vip")
            if (
                local_hour in QUIET_HOURS_PST
                and not free_uses_custom_filters
                and not vip_uses_lane_allowlist
            ):
                print(f"[paper] {symbol} skipped — quiet hour {local_hour:02d}:00 America/Los_Angeles")
                db.set_call_skip_reason(call_id, "quiet_hours")
                return

            actual_entry = None
            entry_volume = None
            token_onchain = {}
            if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
                try:
                    market = data_fetcher.fetch_token_price(mint)
                    if market and market.get("mcap"):
                        actual_entry = float(market["mcap"])
                    if market and market.get("volume_h1"):
                        entry_volume = float(market["volume_h1"])
                    token_onchain = db.get_token_onchain_data(mint) or {}
                except Exception as e:
                    print(f"[paper] price fetch failed for {symbol}: {e}")
            if actual_entry is None and mint:
                print(f"[paper] {symbol} DexScreener returned no mcap — using msg price ${msg_mcap/1000:.1f}k")

            # ── Live mcap gate for VIP gamble-sized entries ───────────────────────
            # Checks run against actual_entry (live market price at open time),
            # not msg_mcap (the VIP call price from minutes earlier).
            is_vip_gamble = (token_data.get("sol_in_override") == SOL_VIP_GAMBLE)
            if is_vip_gamble and actual_entry is not None:
                # Minimum mcap gate — token must be trading above $10k at open time
                if actual_entry < 10_000:
                    print(f"[paper] {symbol} skipped — mcap ${actual_entry/1000:.1f}k below $10k minimum for vip gamble")
                    db.set_call_skip_reason(call_id, "mcap_too_low")
                    return

            entry_price = actual_entry or msg_mcap
            if entry_price <= 0:
                print(f"[paper] {symbol} skipped — no usable entry mcap (msg={msg_mcap}, fetched={actual_entry})")
                db.set_call_skip_reason(call_id, "no_entry_mcap")
                return

            # Strategy A: only trade the first free solhousesignal call for a mint.
            if (
                mint and not mint.startswith(("INFERRED:", "UNKNOWN:"))
                and channel_handle == "solhousesignal"
            ):
                first_free_call_id = db.get_first_call_id_for_mint_on_channel(mint, "solhousesignal")
                if first_free_call_id and first_free_call_id != call_id:
                    print(
                        f"[paper] {symbol} skipped — later free solhousesignal repeat "
                        f"(first free call_id={first_free_call_id}, current={call_id})"
                    )
                    db.set_call_skip_reason(call_id, "duplicate")
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

            # ── Bucket quality filters (data-driven) ─────────────────────────
            bundle_pct = token_data.get("bundle_pct_remaining")
            fake_pct   = token_data.get("fake_vol_pct")
            if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")) and (bundle_pct is None or fake_pct is None):
                if bundle_pct is None:
                    bundle_pct = token_onchain.get("bundle_pct_remaining")
                if fake_pct is None:
                    fake_pct = token_onchain.get("fake_vol_pct")
            if channel_handle in {"solhousesignal", "solhousesignal_vip"}:
                decision = evaluate_strategy_a_entry(
                    StrategyCallContext(
                        call_id=call_id,
                        strategy_name="A",
                        channel_handle=channel_handle,
                        vip_tier=token_data.get("vip_tier"),
                        score=score_val,
                        local_hour_pst=local_hour,
                        entry_mcap=entry_price,
                        bundle_pct=float(bundle_pct) if bundle_pct is not None else None,
                        fake_pct=float(fake_pct) if fake_pct is not None else None,
                        security_flag=(token_data.get("security_flag") or token_onchain.get("security_flag")),
                        dev_tokens_made=token_onchain.get("dev_tokens_made"),
                        symbol=symbol,
                    ),
                    STRATEGY_A_V2026_05_22,
                )
                if not decision.should_trade:
                    print(f"[paper] {symbol} skipped — {decision.reason}")
                    db.set_call_skip_reason(call_id, decision.reason)
                    return

            # ── Entry gate ────────────────────────────────────────────────────
            security_flag = token_data.get("security_flag")
            if security_flag == "warning":
                print(f"[paper] {symbol} skipped — security=warning")
                db.set_call_skip_reason(call_id, "security_warning")
                return

            # Positive pocket marker for observability (sizing unchanged for now).
            if (
                channel_handle == "solhousesignal"
                and 70 <= score_val < 75
                and bundle_pct is not None and bundle_pct < 10
                and fake_pct is not None and fake_pct < 5
            ):
                print(f"[paper] {symbol} priority_bucket matched (70-74, bundle<10, fake<5)")

            vip_tier_val = token_data.get("vip_tier") if channel_handle == "solhousesignal_vip" else None
            db.open_paper_position(call_id, entry_price, sol_in,
                                   entry_time=position_entry_time,
                                   entry_volume=entry_volume,
                                   vip_tier=vip_tier_val)
            if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
                _position_mints[call_id] = mint
            print(f"[paper] opened {symbol}  call_id={call_id}  {sol_in} SOL @ {entry_price:.0f}")

        except Exception as e:
            db.safe_rollback()
            db.set_call_skip_reason(call_id, "paper_open_failed")
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
    exit_config: ExitConfig = None,
) -> ExitResult:
    """
    Check whether the open paper position for call_id should be exited.

    Returns ExitResult(should_exit=False) immediately if no open position
    exists — safe to call unconditionally on every monitor pass.

    exit_config selects the exit strategy. Defaults to EXIT_A_PAPER which
    preserves the original Strategy A behaviour exactly.
    """
    position = db.get_open_paper_position(call_id, is_strategy_b=is_strategy_b)
    if not position:
        return ExitResult(False)

    if entry_mcap <= 0:
        return ExitResult(False)

    cfg = exit_config if exit_config is not None else EXIT_A_PAPER
    is_vip_gamble_pos = position.get("vip_tier") in ("gamble_risk", "gamble")
    channel_handle    = (position.get("channel_handle") or "").lstrip("@")
    entry_time        = position.get("entry_time")

    return apply_exit_config(
        cfg,
        current_mcap=current_mcap,
        peak_mcap=peak_mcap,
        entry_mcap=entry_mcap,
        is_vip_gamble=is_vip_gamble_pos,
        channel_handle=channel_handle,
        entry_time=entry_time,
    )


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
