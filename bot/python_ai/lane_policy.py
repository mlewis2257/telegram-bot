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

from dataclasses import replace as _dc_replace

from exit_config import EXIT_A_PAPER, EXIT_RIDE

# ── Exit-strategy names ───────────────────────────────────────────────────────
EXIT_EARLY    = "early"
EXIT_RIDE_S   = "ride"
EXIT_RIDE_VOL = "ride_vol"
VALID_EXITS   = {EXIT_EARLY, EXIT_RIDE_S, EXIT_RIDE_VOL}

# ── PAPER-ONLY anchor profit_floor test (env-gated, reversible) ───────────────
# 14d save-vs-cost (2026-07-15) on the solhousesignal/low_score `early` anchor: the
# profit_floor COST 14.32 SOL (23 coins where shadow rode further) vs SAVED 5.75 (19
# coins), net -8.6 — it banks anchor runners early on the dense feed where shadow rides
# to +26.8. Systematic (not one outlier), so worth a forward test: strip the floor from
# THIS lane's paper exits only. Live has a separate exit path and shadow a separate
# resolver — neither is touched. Default OFF; set PAPER_ANCHOR_NO_FLOOR=1 + restart the
# paper processes (ws-monitor, monitor, exit-monitor) to arm. Revert = unset + restart.
PAPER_ANCHOR_NO_FLOOR = os.getenv("PAPER_ANCHOR_NO_FLOOR", "").strip().lower() \
    not in ("", "0", "false", "no", "off")


def strip_anchor_floor(cfg, variant, channel_handle, skip_reason):
    """Return `cfg` with profit_floor disabled IFF the anchor no-floor test is armed and
    this is the solhousesignal/low_score `early` lane. Empty profit_floor_channels fully
    disables the floor (see exit_config.ExitConfig). No-op otherwise."""
    if (PAPER_ANCHOR_NO_FLOOR and variant == EXIT_EARLY
            and (channel_handle or "").lstrip("@") == "solhousesignal"
            and (skip_reason or "") == "low_score"):
        return _dc_replace(cfg, profit_floor_channels=frozenset())
    return cfg


if PAPER_ANCHOR_NO_FLOOR:
    print("[lane_policy] PAPER_ANCHOR_NO_FLOOR ON — solhousesignal/low_score `early` "
          "paper exits skip profit_floor (paper-only; live + shadow unaffected)")

# ── Coarse-routed lanes (WS_COARSE_EXIT_VARIANTS) ─────────────────────────────
# A lane whose variant is listed here has its PAPER exit delegated off the dense
# per-swap ws_monitor feed. Single source of truth for BOTH ws_monitor (which skips
# firing their exit) and the sol-monitor sweep (which skips their exit decision so the
# dedicated exit_monitor owns them at its own — coarser — cadence). Gamble lanes are
# NEVER coarse-routed (they need the dense rug-catch, see shadow_overstates_gamble_lanes).
COARSE_EXIT_VARIANTS = frozenset(
    v.strip() for v in os.getenv("WS_COARSE_EXIT_VARIANTS", "").split(",") if v.strip()
)


def is_coarse_routed(channel, vip_tier, skip_reason, entry_time=None) -> bool:
    """True if this lane's paper exit is delegated to the coarse (dedicated exit_monitor)
    sweep rather than the dense ws_monitor feed. Resolves the lane's variant via lane_exit
    (honoring per-day overrides) exactly like the exit resolver everything else uses."""
    if not COARSE_EXIT_VARIANTS:
        return False
    if vip_tier in ("gamble", "gamble_risk"):
        return False
    return lane_exit(channel, vip_tier, skip_reason, entry_time) in COARSE_EXIT_VARIANTS

# RVOL hold threshold for ride_vol (mirrors shadow's SHADOW_RVOL_HOLD so live matches
# what was measured). RVOL = volume_m5 / (volume_h1 / 12); >=1 means the 5-min pace
# is at/above the hourly average — the move is still alive, so keep riding.
RVOL_HOLD = float(os.getenv("LANE_RVOL_HOLD", "0.8"))

# Flow-aware ride_vol (the live, direction-aware upgrade to RVOL). order_flow.metrics()
# gives net_pressure in [-1, +1] (+1 = all buying, -1 = all selling) off the live swap
# stream. ride_vol RIDES while net_pressure >= FLOW_HOLD and BANKS (early) once selling
# takes over — protecting a runner's round-trip WITHOUT the fixed profit cap `early` imposes.
# FLOW_MIN_EVENTS stops a single dust swap from flipping the call; FLOW_WINDOW is the
# look-back (seconds) handed to order_flow.metrics().
FLOW_HOLD       = float(os.getenv("LANE_FLOW_HOLD", "-0.15"))
FLOW_MIN_EVENTS = int(os.getenv("LANE_FLOW_MIN_EVENTS", "3"))
FLOW_WINDOW     = float(os.getenv("LANE_FLOW_WINDOW", "60"))

# Master switch — keep the router OFF until you deliberately route a strategy to it.
LANE_POLICY_ENABLED = os.getenv("LANE_POLICY_ENABLED", "false").lower() == "true"

# Uniform paper-testbed size override. When > 0, EVERY traded lane opens at THIS size instead
# of its per-lane "size" — turning the paper testbed into a clean measurement instrument where
# all lanes are comparable and read directly against the shadow (SHADOW_SOL_IN, default 0.5).
# Set LANE_UNIFORM_SIZE=0.5 in .env to activate. Leave at 0 (default) to use the risk-
# differentiated per-lane sizes in the table below. NOTE: this is a MEASUREMENT choice, not a
# live-risk one — for live trading you'd want smaller sizes on unconfirmed/high-variance lanes,
# so the per-lane sizes are kept intact and simply overridden while this is on.
LANE_UNIFORM_SIZE = float(os.getenv("LANE_UNIFORM_SIZE", "0") or "0")


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

# TESTBED POLICY (2026-06-24). Drives the paper lane-testbed (LANE_TESTBED_ENABLED):
#   strategy "A" trades ANCHORS only; strategy "B" trades ANCHORS + WATCH POCKETS.
# Both honor each lane's research-best exit (the value's "exit"). Optional per-lane keys:
#   "watch": True  -> B-only (an unconfirmed day-pocket inside a net-negative lane)
#   "days": {...}  -> only trade on these LANE_GATE_TZ weekdays (3-letter abbrevs)
# The global Sunday skip (SKIP_WEEKDAYS) still applies on top of everything.
DEFAULT: dict = {"trade": False}

LANE_POLICY: dict[tuple[str, str, str], dict] = {
    # ── ANCHORS — net-positive over 14d; A & B BOTH trade these ──
    # low_score (free): the king. EARLY best over 14d (+20.4; ride_vol +16.8; ride +8.1).
    # (ride_vol led the shorter 5d window — early wins on the larger, more robust sample.)
    # FRIDAY CUT 2026-07-10: lane_policy_review flagged Fri DEMOTE (2/5, mean -0.03, a real
    # 2-wk slide — 3 of last 4 Fridays red). Day-gated to Mon-Thu+Sat (KEEP on all of those:
    # Mon +1.49, Tue +1.43, Wed +1.54, Thu +1.76, Sat +1.48). Sun already global-skipped.
    # Re-add Fri if the review flips it back to KEEP.
    ("solhousesignal",   "none", "low_score"): {"trade": True, "size": 0.5,  "exit": EXIT_EARLY, "days": {"Mon", "Tue", "Wed", "Thu", "Sat"}},
    # solwhaletrending/none DEMOTED from anchor 2026-06-29: marginal on the clean windows
    # (early +0.28/14d, -2.91/7d). solwhale's real edge lives in the low_score sub-lane
    # (Tue/Fri), not none/traded. Re-homed below as a thin Wed-only watch pocket.
    # solwhaletrending/low_score: a DAY-GATED anchor (A & B both trade, but only Tue+Fri,
    # where the edge lives: Tue +7.31, Fri +4.51 / 14d early; Mon -6.47). Aggregate is net-neg
    # so the gate is load-bearing — ungated it would trade Mon/Wed/Thu losers. Promoted to A
    # 2026-06-29 (was a B-only watch pocket). Size 0.1 for now — experimental.
    # EXIT: ride_vol (flow-aware). Back to ride_vol 2026-06-30 PM to LIVE-TEST the new
    # order_flow net_pressure exit (rides while net-buying, banks on the dump) — shadow says
    # ride_vol is the best variant here (+10.51/15d > early +8.41 > ride -1.98; 06/30 ride_vol
    # +7.96 vs early +2.79). Was briefly EARLY after we found live ride_vol degraded to plain
    # RIDE (no volume feed) and gave back a +78% runner; the flow fix now feeds live net_pressure,
    # so this is its first live trial. First gated day = Fri. WATCH: on thin nano-caps with
    # <FLOW_MIN_EVENTS swaps it still falls back to ride — if the [lane_flow] logs show it mostly
    # degrading / it underperforms shadow, revert to EXIT_EARLY. Size held at 0.1 (test the exit,
    # not a size bump too); size up only after Friday validates.
    # FRIDAY CUT 2026-07-10: Tue+Fri -> Tue ONLY. review flagged Fri DEMOTE (2/5, mean -0.24,
    # last 2 Fridays big red: -4.12, -2.66). Tuesday is the star (KEEP 4/4, mean +4.50) and
    # carries the lane's whole edge; Friday was dragging. Re-add Fri if review flips it to KEEP.
    ("solwhaletrending", "none", "low_score"): {"trade": True, "size": 0.1, "exit": EXIT_RIDE_VOL, "days": {"Tue"}},

    # ── WATCH POCKETS — B ONLY, day-gated. Net-NEGATIVE lanes with a repeating green
    #    weekday in --dow-weeks. UNCONFIRMED (2/2 so far) → small size; exit is a tunable
    #    judgement call (thin per-variant data), not a proven number. ──
    # vip_low_score gamble: REMOVED 2026-07-06 — the `vip_low_score` skip label stopped being
    # emitted by the VIP router on 2026-06-24 (verified in DB: 71 coins/day -> 0 for 12+ days,
    # alongside vip_safe_allowed_hours / mcap_too_low / vip_gamble_allowed_hours also dropping to 0).
    # Those coins now pass UNLABELED (skip_reason NULL), shadowed as solhousesignal_vip/gamble/none
    # (deeply negative) and NOT traded. So this pocket had been inert ~2 weeks. Root cause = a
    # separate VIP-routing investigation (why the labels died ~testbed commit fdea57a); pulled here
    # so the board reflects reality. Was: gamble/vip_low_score EARLY 0.1 Mon (hist. Mon 2/2 +6.7).
    # free/none (solhousesignal/none/none) Tue pocket: REMOVED 2026-07-07. Added on a thin 2/2
    # Tuesday sample inside the deeply-negative free/none lane; more data proved it a loser
    # (lane_policy_review flagged Tue 1/3, mean -1.20 -> DEMOTE) and it bled ~-0.9 SOL on 07/07
    # alone. The none/none bucket is a bad lane and the Tuesday exception didn't hold — cut per
    # our own rule (demote when mean<=0). Unlisted now -> DEFAULT skip; shadow still measures it.
    # free/quiet_hours: Saturday 2/2 (+6.75) inside the -15 quiet_hours loser. early.
    ("solhousesignal",     "none",  "quiet_hours"):    {"trade": True, "size": 0.1, "exit": EXIT_EARLY,    "watch": True, "days": {"Sat"}},
    # vip_mcap_gate gamble: PROMOTED from skip 2026-06-29. NOT a mirage — the EARLY variant
    # is green in BOTH 7d (+3.94) and 14d (+2.49), with a *repeating* weekday split: green
    # Mon/Tue/Sat, red Wed/Thu/Fri in both windows. (ride/ride_vol totals flip between
    # windows — a few fat cells — so do NOT use them; early only.) Day-gate Mon/Tue/Sat.
    # SIZE UP 2026-07-06: 0.1 -> 0.5 (full shadow/anchor size). Best-performing traded lane on a
    # confirmed multi-week record (KEEP on Mon/Tue/Sat in lane_policy_review; +16-25 SOL over
    # 7-28d, the top lane in every window). Was 0.1 as an unconfirmed watch pocket; it's earned
    # full size — now matches SHADOW_SOL_IN so realized PnL tracks what the shadow measures.
    # PER-DAY EXIT 2026-07-06: added Fri via day_exits{Fri:ride}. EARLY is red on Fri, but RIDE
    # on Fri is a clean repeating edge — dow-weeks Fri/ride: 3/4 weeks green, mean +3.21, AND
    # +3.20 with 2-of-3 green even after DROPPING the one +9.64 week (verified not an outlier
    # fluke). early Mon/Tue/Sat. Fri (ride) CUT 2026-07-17 — review DEMOTEd it (+2.35 -> -0.02 mean,
    # last 2 Fridays red); the two +4 weeks rolled over. See CUT_WATCH.
    ("solhousesignal_vip", "gamble", "vip_mcap_gate"):  {"trade": True, "size": 0.5, "exit": EXIT_EARLY, "watch": True, "days": {"Mon", "Tue", "Sat"}},
    # solwhaletrending/none: re-homed from anchor. Only consistent green day is Wed, and it's
    # the RIDE variant that carries it (Wed ride +5.26/14d, +5.18/7d; early ~+2). THIN (one
    # day, ~12 trades) — probationary, smallest size, yank if Wed doesn't repeat.
    # Sat added 2026-07-07 (ride_vol) from lane_policy_review --weeks 5: Sat/ride_vol +wk 3/4,
    # mean +1.85, and it SURVIVES dropping its best week (+0.69/3, 2/3 green) — same robustness
    # test that justified vip_mcap_gate Fri->ride. New DAY-cell on an existing lane, not a variant
    # dup. Probationary like Wed: watch B-only, day-keyed exit (Wed->ride, Sat->ride_vol). If next
    # Sat is red (breaks the 3/4) or the review demotes it, yank. Wed (ride) CUT 2026-07-17
    # (SLIDING — its edge was one stale +5.18 spike, last 2 wks red). Sat-only (ride_vol) now. See CUT_WATCH.
    ("solwhaletrending",   "none",  "none"):            {"trade": True, "size": 0.1, "exit": EXIT_RIDE_VOL, "watch": True, "days": {"Sat"}},

    # ── SKIP / CUT — confirmed losers, explicit so intent is auditable (NEVER traded) ──
    ("solhousesignal_vip", "gamble_risk", "vip_paused"):               {"trade": False},  # -471, worst lane
    ("solhousesignal_vip", "gamble",      "mcap_too_low"):             {"trade": False},  # -256
    ("solhousesignal_vip", "safe",        "vip_low_score"):            {"trade": False},  # -104
    ("solhousesignal_vip", "safe",        "vip_safe_allowed_hours"):   {"trade": False},  # -75
    ("solhousesignal_vip", "gamble",      "vip_gamble_allowed_hours"): {"trade": False},  # -63
    ("solhousesignal_vip", "gamble",      "none"):                     {"trade": False},  # -27
    # Entry-side rug gate output (telegram_client HOLDER_SKIP): solwhale low_score coins with
    # hodl_count > HOLDER_SKIP_THRESHOLD (default 700) are reclassified here instead of
    # low_score. NOT traded live; the shadow still tracks it so we can confirm the skip was
    # right — this lane should read net-NEGATIVE in lane_policy_review. If it goes green, the
    # gate is cutting winners: raise the threshold or set HOLDER_SKIP_ENABLED=false.
    ("solwhaletrending",   "none",        "high_holders"):             {"trade": False},
}


# ── CUT_WATCH — day-cells we deliberately REMOVED, kept under observation ──────
# When we day-gate a weekday OUT of a lane (drop it from "days"), that cell leaves
# lane_policy_review's TRADED section — and it only re-surfaces in EMERGING if it
# clears the STRICTER add bar (PROMOTE_RATIO / PROMOTE_MIN_MEAN). A cell that's merely
# RECOVERING (back above the KEEP bar but not yet the add bar) falls into a blind spot:
# gone from TRADED, not yet in EMERGING. This registry lists those cuts so
# lane_policy_review re-evaluates them at the KEEP bar and tells us when to reconsider —
# turning a cut into an explicit, tracked, reversible decision instead of a silent gap.
#
# Each entry = (channel, vip_tier, skip_reason, weekday, variant, note). `variant` is the
# exit the day WOULD run if re-added (so the review reads the RIGHT series); `weekday` is a
# 3-letter LANE_GATE_TZ abbrev. Remove an entry once you either re-add the day (it's back
# in TRADED) or decide the cut is permanent.
CUT_WATCH: list[tuple] = [
    # Friday cut 2026-07-10 (commit e49b1d4): both anchors' Fridays DEMOTEd by the review
    # (a real 2-week slide, 3 of last 4 Fridays red). Watching for recovery at the KEEP bar.
    ("solhousesignal",   "none", "low_score", "Fri", EXIT_EARLY,    "cut 07-10, was Mon-Thu+Sat anchor"),
    ("solwhaletrending", "none", "low_score", "Fri", EXIT_RIDE_VOL, "cut 07-10, Tue+Fri -> Tue only"),
    # 2026-07-17: two B-only pockets the review DEMOTEd once their edge decayed.
    ("solhousesignal_vip", "gamble", "vip_mcap_gate", "Fri", EXIT_RIDE_S, "cut 07-17, ride Fri decayed +2.35 -> -0.02 (last 2 Fri red)"),
    ("solwhaletrending",   "none",   "none",          "Wed", EXIT_RIDE_S, "cut 07-17, ride Wed SLIDING, was one stale +5.18 spike"),
    # 2026-07-17: Sunday is a GLOBAL skip (LANE_SKIP_WEEKDAYS="Sun"), not a per-lane cut — so it
    # never showed in the review at all (the blind spot). Track the two anchors' Sundays here so
    # the review surfaces them weekly. Currently 2/5 green, positive only via the 07-12 +17.7
    # monster — stays SKIPPED until it earns a CONSISTENT record (>=60% green), not one spike.
    ("solhousesignal",   "none", "low_score", "Sun", EXIT_EARLY,    "global Sun skip — monitor; 2/5 green, monster-carried (07-12 +17.7)"),
    ("solwhaletrending", "none", "low_score", "Sun", EXIT_RIDE_VOL, "global Sun skip — monitor; 2/5 green, spike-carried"),
]


def resolve(channel: Optional[str], vip_tier: Optional[str], category: Optional[str],
            now: Optional[datetime] = None, strategy: str = "A") -> dict:
    """
    Return the policy for a lane: at minimum {"trade": bool}; traded lanes also
    carry "size" (SOL) and "exit" (strategy name). Unlisted lanes -> DEFAULT (skip).

    `strategy` is the testbed lane "A" (anchors only) or "B" (anchors + watch pockets).
    `now` drives the weekday gates (evaluated in LANE_GATE_TZ). Layered skips, each with
    a `reason`: global skipped weekday -> "skip_day"; a B-only watch pocket seen by A ->
    "watch_excluded"; a day-gated lane off its weekday -> "off_day". Pass the call/entry
    timestamp (naive -> treated as UTC); defaults to the current time.
    """
    if is_skipped_day(now):
        return {"trade": False, "reason": "skip_day", "weekday": gate_weekday(now)}
    ch   = (channel or "").lstrip("@").strip()
    tier = (vip_tier or "none").strip()
    cat  = (category or "none").strip()
    policy = LANE_POLICY.get((ch, tier, cat), DEFAULT)
    if not policy.get("trade"):
        return policy
    # B-only watch pockets: an unconfirmed day-pocket inside a net-negative lane.
    if policy.get("watch") and str(strategy).upper() != "B":
        return {"trade": False, "reason": "watch_excluded"}
    # Per-lane weekday restriction (the day-gate): only trade on the lane's good day(s).
    days = policy.get("days")
    if days and gate_weekday(now) not in days:
        return {"trade": False, "reason": "off_day", "weekday": gate_weekday(now)}
    # Uniform-size override (paper measurement mode): every traded lane at LANE_UNIFORM_SIZE.
    # Return a COPY so the LANE_POLICY table itself is never mutated.
    if LANE_UNIFORM_SIZE > 0 and policy.get("size") is not None:
        return {**policy, "size": LANE_UNIFORM_SIZE}
    return policy


def lane_exit(channel: Optional[str], vip_tier: Optional[str], category: Optional[str],
              entry_time: Optional[datetime] = None) -> Optional[str]:
    """
    The lane's assigned exit-strategy name, IGNORING trade/day/watch gating. Used at
    exit-check time: the entry decision is already made, so a position must keep
    exiting on its lane's rule even on an off-day or in a watch lane A wouldn't enter.

    PER-DAY EXITS: a lane may carry "day_exits" = {weekday: variant} to override its
    default "exit" on specific weekdays (e.g. vip_mcap_gate runs `early` Mon/Tue/Sat but
    `ride` on Fri, where the dow-weeks data says ride — not early — is the edge). The
    override is keyed by the position's ENTRY weekday via `entry_time`, so a position keeps
    its entry-day exit for its whole life even if checked on a later calendar day. Weekday
    is evaluated in LANE_GATE_TZ (naive entry_time treated as UTC), same as the day-gate.
    If entry_time is omitted, falls back to the lane's default "exit".
    """
    ch   = (channel or "").lstrip("@").strip()
    tier = (vip_tier or "none").strip()
    cat  = (category or "none").strip()
    policy = LANE_POLICY.get((ch, tier, cat), {})
    day_exits = policy.get("day_exits")
    if day_exits and entry_time is not None:
        wd = gate_weekday(entry_time)
        if wd in day_exits:
            return day_exits[wd]
    return policy.get("exit")


def exit_config_for(strategy: Optional[str], rvol: Optional[float] = None,
                    flow: Optional[dict] = None):
    """
    Map an exit-strategy name to a concrete ExitConfig the exit checks already accept.

    `ride_vol` is dynamic, resolved from the best available signal in priority order:
      1. `flow` — order_flow.metrics() dict (live + direction-AWARE). RIDE while
         net_pressure >= FLOW_HOLD, BANK (early) once selling takes over. Ignored
         unless >= FLOW_MIN_EVENTS swaps back it (one dust swap must not flip the exit).
      2. `rvol` — legacy volume ratio (direction-BLIND): RIDE while >= RVOL_HOLD.
      3. neither -> RIDE (historical fallback; e.g. monitor.py has no live swap state).
    Falls back to `early` (EXIT_A_PAPER) for anything unrecognized — the safe default.
    """
    if strategy == EXIT_RIDE_S:
        return EXIT_RIDE
    if strategy == EXIT_RIDE_VOL:
        if flow is not None:
            n_events = int(flow.get("n_buys", 0)) + int(flow.get("n_sells", 0))
            if n_events >= FLOW_MIN_EVENTS:
                net_pressure = float(flow.get("net_pressure", 0.0))
                return EXIT_RIDE if net_pressure >= FLOW_HOLD else EXIT_A_PAPER
        if rvol is not None:
            return EXIT_RIDE if rvol >= RVOL_HOLD else EXIT_A_PAPER
        return EXIT_RIDE
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
