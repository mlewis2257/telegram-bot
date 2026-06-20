"""
lane_policy.py — per-lane trade + exit routing (the "production" of shadow research).

A LANE = (channel, vip_tier, category), where `category` is the same label the
listener already computes as skip_reason (low_score, none, mcap_too_low, ...).
Today skip_reason just means "don't trade." This turns each lane into a POLICY:
whether to trade it, how big, and WHICH exit strategy to use — the same
early / ride / ride_vol split the shadow system measures.

WORKFLOW
  shadow_report (research)  ->  fill this table  ->  run as a paper strategy to
  validate on live data  ->  promote to live.  You tune the TABLE, not the code.

EXIT STRATEGIES (what a lane is assigned)
  early    -> EXIT_A_PAPER : take profits early. Best in marginal / loser-ish lanes.
  ride     -> EXIT_RIDE    : let winners run. NOTE: ride is LEVERAGE — it amplifies a
                             lane's edge (bigger wins in real-edge lanes, bigger losses
                             in bad ones). Only assign `ride` to confirmed winners.
  ride_vol -> dynamic      : ride while 5-min RVOL >= RVOL_HOLD (move still alive),
                             otherwise bank early. A middle ground.

This module is pure + import-light so it's unit-testable and safe to call from the
listener. It does NOT execute trades — it just answers "what should this lane do?"
"""
from __future__ import annotations

import os
from typing import Optional

from exit_config import EXIT_A_PAPER, EXIT_RIDE

# ── Exit-strategy names ───────────────────────────────────────────────────────
EXIT_EARLY    = "early"
EXIT_RIDE_S   = "ride"
EXIT_RIDE_VOL = "ride_vol"
VALID_EXITS   = {EXIT_EARLY, EXIT_RIDE_S, EXIT_RIDE_VOL}

# RVOL hold threshold for ride_vol (mirrors shadow's SHADOW_RVOL_HOLD so live matches
# what was measured). RVOL = volume_m5 / (volume_h1 / 12); >=1 means the 5-min pace
# is at/above the hourly average — the move is still alive, so keep riding.
RVOL_HOLD = float(os.getenv("LANE_RVOL_HOLD", "0.8"))

# Master switch — keep the router OFF until you deliberately route a strategy to it.
LANE_POLICY_ENABLED = os.getenv("LANE_POLICY_ENABLED", "false").lower() == "true"


# ── The lane policy table ─────────────────────────────────────────────────────
# key   = (channel, vip_tier, category)
#         channel  : full handle, e.g. "solhousesignal", "solhousesignal_vip",
#                    "solwhaletrending"  (NOTE: shadow_report truncates to 14 chars,
#                    so confirm the FULL handle from the DB before trusting a row).
#         vip_tier : "none" | "safe" | "gamble" | "gamble_risk"
#         category : the listener's skip_reason label ("low_score", "none", ...)
# value = {"trade": bool, "size": <SOL>, "exit": <strategy>}   (unlisted -> DEFAULT)
#
# Numbers in comments are PRELIMINARY shadow_report 2-day totals. Re-confirm on the
# clean 7-day window before sizing up. This is a STARTING table — edit it freely.
DEFAULT: dict = {"trade": False}

# Keys CONFIRMED against the DB (2026-06-20). Trade/exit/size from the recent
# PHANTOM-EXCLUDED windows (shadow_report --days 2/3), NOT the raw all-time PnL
# (which is corrupted by the pre-fix phantom period). Per-lane exit matters: `ride`
# is leverage — great where the edge is real, ruinous where it isn't.
LANE_POLICY: dict[tuple[str, str, str], dict] = {
    # ── TRADE: confirmed winners ──
    # low_score (free): ride_vol best (+9.06 / 3d), ride +7.49, early +3.68 — strongest lane.
    ("solhousesignal",     "none", "low_score"):     {"trade": True, "size": 0.5,  "exit": EXIT_RIDE_VOL},
    # safe/vip_low_score: ride best (+8.22 / 3d, 139 trades), early +1.17 — strong.
    ("solhousesignal_vip", "safe", "vip_low_score"): {"trade": True, "size": 0.5,  "exit": EXIT_RIDE_S},
    # solwhaletrending low_score: ride LOSES here (-1.65); early +3.17 / ride_vol +2.84 (95 trades).
    ("solwhaletrending",   "none", "low_score"):     {"trade": True, "size": 0.25, "exit": EXIT_RIDE_VOL},
    # solwhaletrending none: ride_vol/early ~+3.9 (small n=18) — trade small, watch.
    ("solwhaletrending",   "none", "none"):          {"trade": True, "size": 0.25, "exit": EXIT_RIDE_VOL},

    # ── SKIP: confirmed losers (explicit so intent is auditable) ──
    ("solhousesignal_vip", "gamble_risk", "vip_paused"):               {"trade": False},  # ride -97
    ("solhousesignal_vip", "gamble",      "mcap_too_low"):             {"trade": False},  # ride -32
    ("solhousesignal_vip", "gamble",      "vip_gamble_allowed_hours"): {"trade": False},  # -15
    ("solhousesignal_vip", "gamble",      "vip_low_score"):            {"trade": False},  # -6.8 (gamble loses; SAFE wins)
    ("solhousesignal_vip", "safe",        "vip_safe_allowed_hours"):   {"trade": False},  # -12 (all variants)
    ("solhousesignal_vip", "gamble",      "none"):                     {"trade": False},  # -3
    ("solhousesignal",     "none",        "none"):                     {"trade": False},  # -11 (free traded lane)
}


def resolve(channel: Optional[str], vip_tier: Optional[str], category: Optional[str]) -> dict:
    """
    Return the policy for a lane: at minimum {"trade": bool}; traded lanes also
    carry "size" (SOL) and "exit" (strategy name). Unlisted lanes -> DEFAULT (skip).
    """
    ch   = (channel or "").lstrip("@").strip()
    tier = (vip_tier or "none").strip()
    cat  = (category or "none").strip()
    return LANE_POLICY.get((ch, tier, cat), DEFAULT)


def exit_config_for(strategy: Optional[str], rvol: Optional[float] = None):
    """
    Map an exit-strategy name to a concrete ExitConfig the exit checks already accept.
    `ride_vol` is dynamic — pass the current rvol (see compute_rvol); None -> ride.
    Falls back to `early` (EXIT_A_PAPER) for anything unrecognized — the safe default.
    """
    if strategy == EXIT_RIDE_S:
        return EXIT_RIDE
    if strategy == EXIT_RIDE_VOL:
        if rvol is None:
            return EXIT_RIDE
        return EXIT_RIDE if rvol >= RVOL_HOLD else EXIT_A_PAPER
    return EXIT_A_PAPER  # early / default


def compute_rvol(volume_m5, volume_h1) -> Optional[float]:
    """RVOL = volume_m5 / (volume_h1 / 12). >1 accelerating, <1 cooling. None if no data."""
    try:
        m5 = float(volume_m5)
        h1 = float(volume_h1)
    except (TypeError, ValueError):
        return None
    base = h1 / 12.0
    return (m5 / base) if base > 0 else None
