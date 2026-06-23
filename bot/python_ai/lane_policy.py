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
import sys
from datetime import datetime, timezone
from typing import Optional

try:
    from zoneinfo import ZoneInfo  # py3.9+ (needs system tzdata or the `tzdata` pkg)
except ImportError:  # pragma: no cover
    ZoneInfo = None  # type: ignore

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


# ── Global day-gate (defensive weekday skip) ──────────────────────────────────
# Some weekdays are structurally bad to trade (Sunday — historically weak unless a
# special occasion). This skips them OUTRIGHT, across every lane. It's a *defensive*
# gate (a skip), which is low-regret: worst case you miss the rare good Sunday. That's
# the opposite of a day-*add*, which would need many samples before it's safe to trust.
#
# The weekday is evaluated in LANE_GATE_TZ. Default UTC — which is how the DB stores
# entry_time AND how shadow_report buckets the data, so "skip Sun" skips exactly the
# UTC-Sunday bucket the report measured (one coordinate system end to end). Set
# LANE_GATE_TZ="America/Los_Angeles" to gate on your local calendar day instead; note a
# local day is offset ~7-8h from the UTC buckets, so it straddles two of them.
LANE_GATE_TZ = os.getenv("LANE_GATE_TZ", "UTC")

# Weekdays to skip, as 3-letter abbrevs. Default: Sunday. Override WITHOUT a redeploy
# for a "special occasion" Sunday: LANE_SKIP_WEEKDAYS="" trades every day,
# LANE_SKIP_WEEKDAYS="Sat,Sun" skips both.
_WD_NAMES = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
SKIP_WEEKDAYS = {
    d.strip().title()
    for d in os.getenv("LANE_SKIP_WEEKDAYS", "Sun").split(",")
    if d.strip()
}

# Resolve the gate tz once at import. UTC needs no tzdata; a named zone does — if it
# can't load we degrade to UTC weekdays and warn loudly rather than silently dropping
# the gate.
_GATE_TZ = timezone.utc
if LANE_GATE_TZ.upper() not in ("UTC", "ETC/UTC"):
    loaded = None
    if ZoneInfo is not None:
        try:
            loaded = ZoneInfo(LANE_GATE_TZ)
        except Exception:  # ZoneInfoNotFoundError, etc.
            loaded = None
    if loaded is not None:
        _GATE_TZ = loaded
    elif SKIP_WEEKDAYS:
        print(f"[lane_policy] WARNING: could not load tz '{LANE_GATE_TZ}' "
              f"(install the `tzdata` package) — day-gate will fall back to UTC weekdays.",
              file=sys.stderr)


def gate_weekday(now: Optional[datetime] = None) -> str:
    """
    3-letter weekday ('Mon'..'Sun') for `now` in LANE_GATE_TZ. `now` defaults to the
    current time; a naive datetime is assumed UTC (DB timestamps are UTC).
    """
    if now is None:
        now = datetime.now(timezone.utc)
    elif now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    return _WD_NAMES[now.astimezone(_GATE_TZ).weekday()]


def is_skipped_day(now: Optional[datetime] = None) -> bool:
    """True if `now` (see gate_weekday) lands on a skipped weekday in LANE_GATE_TZ."""
    return bool(SKIP_WEEKDAYS) and gate_weekday(now) in SKIP_WEEKDAYS


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

# Keys CONFIRMED against the DB. Trade/exit kept only for lanes NET-POSITIVE over the
# reliable 5-day phantom-excluded window (shadow_report --by-dow --days 5); lanes that
# flipped negative going 3d->5d were cut (the mcap_too_low trap — a 3d spike isn't an
# edge). Per-lane exit matters: `ride` is leverage — great where the edge is real,
# ruinous where it isn't.
LANE_POLICY: dict[tuple[str, str, str], dict] = {
    # ── TRADE: net-positive over 5d ──
    # low_score (free): ALL variants + over 5d (ride_vol +8.15, early +5.28, ride +4.03) — robust.
    ("solhousesignal",   "none", "low_score"): {"trade": True, "size": 0.5,  "exit": EXIT_RIDE_VOL},
    # solwhaletrending low_score: early +6.63 / 5d (ride_vol +2.97; ride -3.64 loses) — steadiest lane.
    ("solwhaletrending", "none", "low_score"): {"trade": True, "size": 0.25, "exit": EXIT_EARLY},
    # solwhaletrending none: ALL variants + over 5d (early +4.43, ride_vol +4.30, ride +2.27).
    ("solwhaletrending", "none", "none"):      {"trade": True, "size": 0.25, "exit": EXIT_EARLY},

    # ── SKIP: confirmed losers (explicit so intent is auditable) ──
    ("solhousesignal_vip", "gamble_risk", "vip_paused"):               {"trade": False},  # ride -166 / 5d
    ("solhousesignal_vip", "gamble",      "mcap_too_low"):             {"trade": False},  # ride -71 / 5d
    ("solhousesignal_vip", "gamble",      "vip_gamble_allowed_hours"): {"trade": False},  # -16
    ("solhousesignal_vip", "gamble",      "vip_low_score"):            {"trade": False},  # -6.8 (gamble; SAFE different)
    ("solhousesignal_vip", "safe",        "vip_safe_allowed_hours"):   {"trade": False},  # -12 (all variants)
    ("solhousesignal_vip", "gamble",      "none"):                     {"trade": False},  # -3
    ("solhousesignal",     "none",        "none"):                     {"trade": False},  # -20 / 5d

    # ── CUT: flipped NET-NEGATIVE 3d->5d (the trap). Kept here so we don't re-add blindly. ──
    ("solhousesignal_vip", "safe",   "vip_low_score"): {"trade": False},
    ("solhousesignal_vip", "gamble", "vip_mcap_gate"): {"trade": False},

    # ── DAY-GATE WATCH LIST (NOT traded — single-sample so far) ─────────────────
    # These lanes are net-NEGATIVE overall but strongly positive on SPECIFIC weekdays
    # in the 7d --by-dow. 7 days = ONE of each weekday, so each is n=1 — a candidate,
    # not an edge. PROMOTE a row to a gated lane only after its weekday repeats ~3-4x.
    # When ready, the entry becomes e.g.:
    #   ("solhousesignal_vip","safe","vip_low_score"):
    #       {"trade": True, "size": 0.25, "exit": "ride", "days": {"Thu","Fri"}}
    # Candidates (lane -> strong day(s), best variant, 1-sample PnL):
    #   solhousesignal_vip/safe/vip_low_score          -> Thu+Fri  ride      (+12.55)
    #   solhousesignal_vip/gamble/vip_gamble_allowed_h -> Sat       early     (+8.32)
    #   solhousesignal_vip/gamble/none                 -> Mon,Sun   early     (+5.5)
    #   solhousesignal_vip/gamble/vip_low_score        -> Mon       ride_vol  (+3.6)
    #   solhousesignal_vip/gamble/mcap_too_low         -> Fri       early     (+7.53, risky lane)
}


def resolve(channel: Optional[str], vip_tier: Optional[str], category: Optional[str],
            now: Optional[datetime] = None) -> dict:
    """
    Return the policy for a lane: at minimum {"trade": bool}; traded lanes also
    carry "size" (SOL) and "exit" (strategy name). Unlisted lanes -> DEFAULT (skip).

    `now` drives the global weekday day-gate: on a skipped weekday (SKIP_WEEKDAYS,
    evaluated in LANE_GATE_TZ) NO lane trades and the result carries reason="skip_day".
    Pass the call/entry timestamp (naive -> treated as UTC); defaults to the current time.
    """
    if is_skipped_day(now):
        return {"trade": False, "reason": "skip_day", "weekday": gate_weekday(now)}
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
