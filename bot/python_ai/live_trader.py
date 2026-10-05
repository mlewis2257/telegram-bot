"""
live_trader.py — Real on-chain execution engine.

Mirrors paper_trader.py but executes trades via jupiter.py.
All positions use is_simulation=FALSE in trading_positions.

Safety guards (enforced in code, not just config):
  1. LIVE_TRADING_ENABLED must be exactly the string 'true'
  2. Open live position count < MAX_OPEN_LIVE_POSITIONS
  3. Daily loss circuit breaker — halts all trading if MAX_DAILY_LOSS_SOL hit
  4. No duplicate position per call_id
  5. SOL balance >= position_size + 0.05 reserve before every buy
  6. Token balance verified on-chain before every sell

Circuit breaker persistence
---------------------------
When the daily loss limit is hit, _circuit_broken is set True in memory AND
a sentinel file is written to .last_run/circuit_breaker.flag. On the next
startup, if that file exists, all live trading is halted immediately.
To re-enable: delete the flag file and restart.
"""

import asyncio
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(__file__))

import db
import entry_quality
import jupiter
import alert_bot
import data_fetcher
import peak_guard
import position_alerts
import wallet as _wallet
import lane_policy
from paper_trader import (
    TAKE_PROFIT_5X,
    TRAIL_PEAK_MIN,
    HARD_STOP_PCT,
    MAX_HOURS,
)
from dataclasses import replace as _replace
from exit_config import ExitConfig, ExitResult, apply_exit_config, get_exit_config, EXIT_LIVE_V2
from strategy_config import STRATEGY_A_V2026_05_22
from strategy_engine import StrategyCallContext, evaluate_strategy_a_entry

LOCAL_TZ        = ZoneInfo("America/Los_Angeles")
QUIET_HOURS_PST = set(STRATEGY_A_V2026_05_22.quiet_hours_pst)

# Load exit strategy from env — defaults to EXIT_LIVE_V2 which mirrors paper
# Strategy A: 10x/5x TP, tiered trailing, no profit floor.
_LIVE_EXIT_CONFIG: ExitConfig = EXIT_LIVE_V2
try:
    _env_exit = os.getenv("EXIT_STRATEGY", "").strip()
    if _env_exit:
        _LIVE_EXIT_CONFIG = get_exit_config(_env_exit)
        print(f"[live] exit strategy: {_LIVE_EXIT_CONFIG.name}")
    else:
        print(f"[live] exit strategy: {_LIVE_EXIT_CONFIG.name} (default)")
except ValueError as _e:
    print(f"[live] WARNING: invalid EXIT_STRATEGY env — {_e}. Using {_LIVE_EXIT_CONFIG.name}.")

# LIVE-ONLY hard-stop override. Backtest (30d real ticks, solwhaletrending) showed a
# -20% stop strictly beats the -35% default: identical runner capture (2x% flat at its
# max) with ~10% less dud bleed; tighter than ~-18% starts choking runners. Applied via
# replace() on the frozen config, so it copies _LIVE_EXIT_CONFIG and leaves paper-A's
# EXIT_A_PAPER constant untouched. Blank/unset keeps the config's own stop. Reverting =
# clear LIVE_HARD_STOP_PCT and restart.
_hs_env = os.getenv("LIVE_HARD_STOP_PCT", "").strip()
if _hs_env:
    try:
        _hs = float(_hs_env)
        if 0.0 < _hs < 1.0:
            _prev = _LIVE_EXIT_CONFIG.hard_stop_pct
            _LIVE_EXIT_CONFIG = _replace(_LIVE_EXIT_CONFIG, hard_stop_pct=_hs)
            print(f"[live] hard_stop override: -{_hs*100:.0f}% (was -{_prev*100:.0f}%)")
        else:
            print(f"[live] WARNING: LIVE_HARD_STOP_PCT={_hs_env} out of (0,1) — ignored")
    except ValueError:
        print(f"[live] WARNING: LIVE_HARD_STOP_PCT={_hs_env} not a number — ignored")

# ── In-flight mint guard (prevents race-condition duplicate buys) ──────────────

_pending_mints: set[str] = set()
_pending_lock = asyncio.Lock()


# ── Circuit breaker state ──────────────────────────────────────────────────────

_STATE_DIR         = Path(os.path.dirname(__file__)) / ".last_run"
_CIRCUIT_FLAG_FILE = _STATE_DIR / "circuit_breaker.flag"

_circuit_broken: bool = _CIRCUIT_FLAG_FILE.exists()

if _circuit_broken:
    print(f"[live] STARTUP: circuit breaker flag found — all live trading halted")
    print(f"[live] To re-enable: delete {_CIRCUIT_FLAG_FILE} and restart")

_startup_allowed_hours = [
    int(h) for h in os.getenv("LIVE_ALLOWED_HOURS_UTC", "").split(",") if h.strip()
]
print(f"[live] allowed hours UTC: {_startup_allowed_hours if _startup_allowed_hours else 'all (no restriction)'}")

LIVE_MAX_ENTRY_EXEC_RATIO = entry_quality.env_float(os.getenv("LIVE_MAX_ENTRY_EXEC_RATIO"), 0.0)
print(f"[live] max entry exec/ref ratio: {LIVE_MAX_ENTRY_EXEC_RATIO:g}x"
      if LIVE_MAX_ENTRY_EXEC_RATIO > 0 else "[live] max entry exec/ref ratio: disabled")
LIVE_ENTRY_ROUNDTRIP_MIN_MULT = entry_quality.env_float(os.getenv("LIVE_ENTRY_ROUNDTRIP_MIN_MULT"), 0.0)
print(f"[live] entry roundtrip min: {LIVE_ENTRY_ROUNDTRIP_MIN_MULT:g}x"
      if LIVE_ENTRY_ROUNDTRIP_MIN_MULT > 0 else "[live] entry roundtrip min: disabled")


# ── Per-channel mcap entry limits ──────────────────────────────────────────────

# Per-channel ENTRY ceiling: reject a call whose market cap is already above this, because
# the multiple you are buying has to come from somewhere — a $150k coin reaching 3x needs
# $450k, a $20M coin needs $60M. 0 disables the ceiling for that channel.
MCAP_LIMITS = {
    'solhousesignal_vip': 200_000,
    # 0 = NO CEILING (2026-09-06). The trending channel's calls skew larger by nature and
    # the 100k cap was rejecting the better half: on the first clean qsim day SWT entries
    # ABOVE 100k ran +1.6%/trade (n=28, incl. BEBE 2.14x and TOA 1.77x) while entries below
    # it ran -15.5% (n=18). Loosening improved monotonically (100k -15.5%, 175k -11.3%,
    # 280k -6.8%, uncapped -5.1%). CIs overlap, so revisit on more data — but the cap was
    # pointed the wrong way. NOTE: solhousesignal is the opposite; its ceiling is doing the
    # heavy lifting there.
    'solwhaletrending':   0,
    'solearlytrending':    75_000,
    # 280k (was 175k) 2026-09-06. The ceiling is load-bearing: on the first clean qsim day
    # solhousesignal ENTRIES ABOVE it lost -33.0%/trade (n=30, CI [-52.3%,-13.7%]) while
    # entries at/below it were -0.7% (n=30) — the lane's whole loss was oversized coins,
    # incl. a rug series (WOTF x4, WOAF, IOAF, VOF, GOAF at $19-33M, all exiting 0.000).
    'solhousesignal':     280_000,  # strategy engine enforces 20k min
}
DEFAULT_MCAP_LIMIT = 75_000  # fallback for unknown channels


# ── Config helpers ─────────────────────────────────────────────────────────────

def _is_enabled() -> bool:
    """Kill switch — must be exactly 'true', not just truthy."""
    return os.getenv("LIVE_TRADING_ENABLED", "false") == "true"


def _position_size(label: str) -> float:
    base = float(os.getenv("LIVE_POSITION_SIZE_SOL", "0.05"))
    if label == "strong_alert":
        mult = float(os.getenv("LIVE_STRONG_ALERT_MULTIPLIER", "2.0"))
        return base * mult
    return base


def _max_positions() -> int:
    return int(os.getenv("MAX_OPEN_LIVE_POSITIONS", "5"))


def _max_daily_loss() -> float:
    return float(os.getenv("MAX_DAILY_LOSS_SOL", "1.0"))


def _max_total_loss() -> float:
    # Cumulative net-loss kill across the whole test window. 0/unset = disabled.
    return float(os.getenv("MAX_TOTAL_LOSS_SOL", "0") or "0")


def _pnl_since() -> str:
    # Count realized P&L only from this timestamp onward, so the total breaker
    # ignores pre-test history (the old contaminated live trades). Set to the test
    # start date, e.g. LIVE_PNL_SINCE=2026-08-13. Unset = beginning of time.
    return os.getenv("LIVE_PNL_SINCE", "").strip() or "1970-01-01"


# ── Lane-policy entry gate ──────────────────────────────────────────────────────
# When ON (default), live opens ONLY lanes that lane_policy.resolve() approves for the
# configured testbed strategy — so live mirrors the SAME refined Strategy A you tune in
# lane_policy.py (same lanes, day-gates, Sunday skip, watch exclusion). Edit lane_policy
# once and BOTH paper and live follow — no separate live lane config to keep in sync.
# Live's own safety filters (mcap cap, quiet hours, blocked channels, balance/dup guards)
# still apply on top, so live trades a SAFE SUBSET of refined-A, not looser.
#   LIVE_USE_LANE_POLICY=false  -> fall back to strategy_engine-only entry (legacy)
#   LIVE_LANE_STRATEGY=B        -> mirror the B lane set (anchors + watch pockets) instead
LIVE_USE_LANE_POLICY = os.getenv("LIVE_USE_LANE_POLICY", "true").lower() == "true"
LIVE_LANE_STRATEGY   = os.getenv("LIVE_LANE_STRATEGY", "A").strip().upper()
print("[live] lane-policy gate: "
      + (f"ON (mirrors testbed strategy {LIVE_LANE_STRATEGY})" if LIVE_USE_LANE_POLICY
         else "OFF (strategy_engine entry, not lane-gated)"))

# ── Sell-quote exit source of truth (Phase 1: observation) ────────────────────
# The feed mcap that drives live exits is unreliable at/after entry (under-reports
# fresh coins), so live's exit multiple can be fictional (CSG "thought +80%", real
# breakeven). LIVE_EXIT_QUOTE_LOG=true quotes the bag's REAL sellable value each tick
# and logs real-vs-feed multiple — READ ONLY, drives no sells. Phase 2 (a future
# LIVE_EXIT_USE_QUOTE flag) will let the quote drive exits once the logs confirm it.
LIVE_EXIT_QUOTE_LOG = os.getenv("LIVE_EXIT_QUOTE_LOG", "false").lower() == "true"
# Phase 2: when ON, the real sell-quote DRIVES exits — check_live_exits keys off the
# wallet's real (current, peak, entry) triple instead of the laggy feed. Requires the
# real_peak_mcap column (see migration). Any quote failure or missing fill silently
# falls back to the feed basis — a bad quote must never force or block a real sell.
LIVE_EXIT_USE_QUOTE = os.getenv("LIVE_EXIT_USE_QUOTE", "false").lower() == "true"
# Decide exits ONLY off a real sell quote, the way qsim does. qsim's ratios are sound
# because entry cancels out of every one of them: real_mult = sol_out/sol_in, synth =
# entry*real_mult, peak_mult = real_peak/entry -- one ruler, real executable SOL. Live's
# feed path had no such cancellation (feed numerator, fill denominator, two independent
# sources) and that mix put 4 of 5 positions under their own hard stop at birth.
# With this on, a tick with no usable quote makes NO DECISION: hold and re-quote next
# tick. That is safe in a way it would not be for paper, because executing a live exit
# needs a Jupiter route too -- a feed-driven decision during a quote outage is a decision
# you cannot act on anyway. Set false to restore the old feed fallback.
LIVE_EXIT_REQUIRE_QUOTE = os.getenv("LIVE_EXIT_REQUIRE_QUOTE", "true").lower() == "true"
LIVE_NOROUTE_WARN_AT    = int(os.getenv("LIVE_NOROUTE_WARN_AT", "6"))  # mirrors QSIM_RUG_FAILS
# With no quote, ONE protective rule may still fire: a collapse. Silencing every rule
# during a quote outage leaves a rugging position unmanaged, and "we couldn't sell anyway"
# is not true — a 429 is a rate limit, and continuous polling is what exhausts the budget,
# not a single sell. So the profit side stays silent (feed entry lag reads HIGH, which
# fires banks EARLY and gives up real upside) while a collapse can still get out.
#
# BOTH legs must agree, and each is chosen to err toward NOT firing:
#   current/peak  <= LIVE_PROTECTIVE_DD   both legs post-entry, so entry lag cannot touch
#                                         this ratio at all.
#   current/entry <= hard stop            needed because drawdown-from-peak ALONE would
#                                         false-fire: a coin that ran 5x then fell 80% is
#                                         back at entry, not rugging. Entry lag biases this
#                                         leg HIGH, so a lagged feed saying "below entry"
#                                         is conservative evidence that it really is.
LIVE_PROTECTIVE_DD = float(os.getenv("LIVE_PROTECTIVE_DD", "0.20"))
# Default OFF, i.e. security_flag='warning' is TRADED. Measured 2026-10-02 over 21d on
# solwhaletrending / mcap 80-120k, against qsim which has no security gate:
#
#   unknown   245   -8.00%/SOL   rug 10.6%      <- blocked by entry_filter (z = 4.51)
#   safe      212   +0.62%/SOL   rug  0.5%
#   warning   160   +4.52%/SOL   rug  2.5%      <- the BEST group, and this gate blocked it
#
# I first kept this gate because 'warning' beating 'safe' is only z = 1.30. That was the
# wrong test: the decision is not "is warning better than safe", it is "is warning worth
# trading at all", and the null for that is ZERO. +4.52%/SOL on 160 trades survives
# stripping 3 of its 11 banks (~1 sigma on the count) and stays at +0.167 SOL.
#
# The tail risk the flag warns about is PRICED, not hidden: qsim books consecutive
# no-routes as a rug (QSIM_RUG_FAILS=6), so an unsellable honeypot lands in that 2.5%,
# and a transfer-tax coin lands in realized PnL. And 'warning' is 160 of 617 in-band
# positions on this lane -- blocking 26% of flow compounds the binding constraint, which
# is coverage, not selection.
#
# LEGACY, and it should stay off. entry_filter's LIVE_ENTRY_BLOCK_SECURITY_FLAGS is now
# the SINGLE authority for security gating — having it in two files is precisely how
# 'warning' (the best group) ended up blocked here while 'unknown' walked past, because
# this gate string-matches one value and knows nothing about the others.
#
# The later measurement, on the bank_2x era only, moved the answer again: 'safe' is the
# group to block (lowest bank rate, worst pnl, zero rugs), not 'warning'. That lives in
# entry_filter, where the whole set is configurable in one place.
#
# Set true only to reproduce historical behaviour. paper_trader_b keeps its own gate
# untouched so its history stays continuous.
LIVE_BLOCK_SECURITY_WARNING = (
    os.getenv("LIVE_BLOCK_SECURITY_WARNING", "false").strip().lower() == "true"
)
_SELL_QUOTE_TTL   = float(os.getenv("LIVE_SELL_QUOTE_TTL", "2.5"))  # seconds
LIVE_QUOTE_PEAK_PENDING_TTL_SECS = float(
    os.getenv(
        "LIVE_QUOTE_PEAK_PENDING_TTL_SECS",
        str(max(peak_guard.PENDING_TTL_SECS, _SELL_QUOTE_TTL * 4.0)),
    )
)
LIVE_NO_BOUNCE_STOP_ENABLED = os.getenv("LIVE_NO_BOUNCE_STOP_ENABLED", "false").lower() == "true"
NO_BOUNCE_ARM_MULT          = float(os.getenv("NO_BOUNCE_ARM_MULT", "1.3"))
NO_BOUNCE_STOP_MULT         = float(os.getenv("NO_BOUNCE_STOP_MULT", "0.9"))
# Anchor sanity ceiling. A multiple this large is not a runner, it is a corrupt entry
# anchor: ZPAD (call_id 296124, 2026-09-29) recorded entry_price_fill=0.103 against a
# feed mcap of 83537 and every ratio off it read 811,024x, which fired bank_2x eight
# seconds after entry and sold a 0.94x bag as a 2x bank. The largest real multiple this
# project has ever observed is 168x, so 1000 rejects the impossible without ever
# touching a genuine trade. qsim has had QSIM_CEILING_MULT for this; live had nothing.
LIVE_MAX_SANE_MULT          = float(os.getenv("LIVE_MAX_SANE_MULT", "1000"))
LIVE_BANK_EXIT_ENABLED      = os.getenv("LIVE_BANK_EXIT_ENABLED", "false").lower() == "true"
LIVE_BANK_EXIT_MULT         = float(os.getenv("LIVE_BANK_EXIT_MULT", "1.3"))
LIVE_EXIT_OVERLAY_STRATEGY  = os.getenv("LIVE_EXIT_OVERLAY_STRATEGY", "").strip()
LIVE_RUNNER_WINDOW_ENABLED  = os.getenv("LIVE_RUNNER_WINDOW_ENABLED", "false").lower() == "true"
RUNNER_WINDOW_ARM_MULT      = float(os.getenv("RUNNER_WINDOW_ARM_MULT", "2.0"))
RUNNER_WINDOW_RELEASE_MULT  = float(os.getenv("RUNNER_WINDOW_RELEASE_MULT", "5.0"))
RUNNER_WINDOW_MINS          = float(os.getenv("RUNNER_WINDOW_MINS", "10"))
RUNNER_WINDOW_FLOOR_MULT    = float(os.getenv("RUNNER_WINDOW_FLOOR_MULT", "1.0"))
RUNNER_WINDOW_PROTECTED_REASONS = {"trail_stop", "profit_floor"}
_sell_quote_cache: dict = {}  # mint -> (sol_out, monotonic_ts)
# mint -> last _exit_sell_quote status ("ok"/"no_route"/"rate_limited"/"error"). Lets the
# exit path tell "this coin is unsellable" from "Jupiter is throttling us", which decide
# very different things: the first is news about the position, the second is news about us.
_LAST_QUOTE_STATUS: dict[str, str] = {}
# mint -> consecutive genuine no-route quotes. Reset by any successful quote. Purely
# observational: it is LOGGED, never acted on. A live bag that Jupiter has dropped cannot
# be sold, so there is no exit to take — and auto-closing it at -100% is precisely the
# bug 63a7e71 fixed for paper (KITWIFMIT booked -100% after running 2.13x).
_noroute_streak: dict[str, int] = {}
# call_id -> monotonic ts of the last "no exit decision" line, so a quote outage does not
# print once per position per tick for as long as it lasts.
_no_quote_logged: dict[int, float] = {}
_NO_QUOTE_LOG_SECS = float(os.getenv("LIVE_NO_QUOTE_LOG_SECS", "60"))
_live_exit_state: dict[int, dict] = {}
_runner_window_until: dict[int, float] = {}


@dataclass(frozen=True)
class LiveExitOverlay:
    name: str
    kind: str
    bank_mult: float | None = None
    confirm_ticks: int = 1
    lock_trigger_mult: float | None = None
    lock_floor_mult: float | None = None
    lock_trail_pct: float | None = None


LIVE_EXIT_OVERLAYS: dict[str, LiveExitOverlay] = {
    "lock_trail_a1p75_f1p35_tr30": LiveExitOverlay("lock_trail_a1p75_f1p35_tr30", "lock_trail", lock_trigger_mult=1.75, lock_floor_mult=1.35, lock_trail_pct=0.3),
    "lock_trail_a1p75_f1p35_tr40": LiveExitOverlay("lock_trail_a1p75_f1p35_tr40", "lock_trail", lock_trigger_mult=1.75, lock_floor_mult=1.35, lock_trail_pct=0.4),
    "lock_trail_a1p5_f1p2_tr30": LiveExitOverlay("lock_trail_a1p5_f1p2_tr30", "lock_trail", lock_trigger_mult=1.5, lock_floor_mult=1.2, lock_trail_pct=0.3),
    "lock_trail_a2x_f1p55_tr30": LiveExitOverlay("lock_trail_a2x_f1p55_tr30", "lock_trail", lock_trigger_mult=2.0, lock_floor_mult=1.55, lock_trail_pct=0.3),
    "bank_1p2x": LiveExitOverlay("bank_1p2x", "bank", bank_mult=1.2),
    "bank_1p3x": LiveExitOverlay("bank_1p3x", "bank", bank_mult=1.3),
    "bank_1p4x": LiveExitOverlay("bank_1p4x", "bank", bank_mult=1.4),
    "bank_1p5x": LiveExitOverlay("bank_1p5x", "bank", bank_mult=1.5),
    "bank_1p75x": LiveExitOverlay("bank_1p75x", "bank", bank_mult=1.75),
    "bank_2x": LiveExitOverlay("bank_2x", "bank", bank_mult=2.0),
    "confirm_bank_1p2x": LiveExitOverlay("confirm_bank_1p2x", "bank", bank_mult=1.2, confirm_ticks=2),
    "confirm_bank_1p3x": LiveExitOverlay("confirm_bank_1p3x", "bank", bank_mult=1.3, confirm_ticks=2),
    "confirm_bank_1p4x": LiveExitOverlay("confirm_bank_1p4x", "bank", bank_mult=1.4, confirm_ticks=2),
    "confirm_bank_1p5x": LiveExitOverlay("confirm_bank_1p5x", "bank", bank_mult=1.5, confirm_ticks=2),
    "confirm_bank_1p75x": LiveExitOverlay("confirm_bank_1p75x", "bank", bank_mult=1.75, confirm_ticks=2),
    "confirm_bank_2x": LiveExitOverlay("confirm_bank_2x", "bank", bank_mult=2.0, confirm_ticks=2),
    "lock_or_bank_1p3x_1p1x": LiveExitOverlay("lock_or_bank_1p3x_1p1x", "lock_or_bank", bank_mult=1.3, lock_trigger_mult=1.3, lock_floor_mult=1.1),
    "lock_or_bank_1p4x_1p15x": LiveExitOverlay("lock_or_bank_1p4x_1p15x", "lock_or_bank", bank_mult=1.4, lock_trigger_mult=1.4, lock_floor_mult=1.15),
    "lock_or_bank_1p5x_1p2x": LiveExitOverlay("lock_or_bank_1p5x_1p2x", "lock_or_bank", bank_mult=1.5, lock_trigger_mult=1.5, lock_floor_mult=1.2),
    "lock_or_bank_1p75x_1p35x": LiveExitOverlay("lock_or_bank_1p75x_1p35x", "lock_or_bank", bank_mult=1.75, lock_trigger_mult=1.75, lock_floor_mult=1.35),
    "lock_or_bank_2x_1p55x": LiveExitOverlay("lock_or_bank_2x_1p55x", "lock_or_bank", bank_mult=2.0, lock_trigger_mult=2.0, lock_floor_mult=1.55),
}


def _resolve_live_exit_overlay() -> LiveExitOverlay | None:
    if LIVE_EXIT_OVERLAY_STRATEGY:
        overlay = LIVE_EXIT_OVERLAYS.get(LIVE_EXIT_OVERLAY_STRATEGY)
        if not overlay:
            print(f"[live] WARNING: unknown LIVE_EXIT_OVERLAY_STRATEGY={LIVE_EXIT_OVERLAY_STRATEGY!r}; overlay disabled")
        return overlay
    if LIVE_BANK_EXIT_ENABLED and LIVE_BANK_EXIT_MULT > 0:
        return LiveExitOverlay(
            name=f"bank_{LIVE_BANK_EXIT_MULT:g}x",
            kind="bank",
            bank_mult=LIVE_BANK_EXIT_MULT,
        )
    return None


_LIVE_EXIT_OVERLAY = _resolve_live_exit_overlay()
print(f"[live] exit overlay: {_LIVE_EXIT_OVERLAY.name if _LIVE_EXIT_OVERLAY else 'none'}")
print(f"[live] exit basis: quote={'ON' if LIVE_EXIT_USE_QUOTE else 'OFF'}"
      f"  require_quote={'ON' if LIVE_EXIT_REQUIRE_QUOTE else 'OFF (feed fallback)'}"
      f"  sane_mult_ceiling={LIVE_MAX_SANE_MULT:g}x")
if LIVE_EXIT_REQUIRE_QUOTE:
    print(f"[live] no quote -> profit-side exits PAUSED, collapse guard armed at "
          f"<={LIVE_PROTECTIVE_DD:.0%} of peak AND below the hard stop")
try:
    import entry_filter as _ef
    print(_ef.describe())
except Exception as _e:
    print(f"[live] entry filter import FAILED — every entry will be skipped: {_e}")
# Printed loudly because the DEFAULT changed 2026-10-02: 'warning' used to be blocked and
# is now traded. A default flip on real money has to be visible in the startup log.
print("[live] security_flag=warning: "
      + ("BLOCKED" if LIVE_BLOCK_SECURITY_WARNING else
         "TRADED (measured +4.52%/SOL, rug 2.5%, n=160/21d — the best of the three)"))


def _apply_live_exit_overlay(
    call_id: int,
    current_mult: float,
    current_mcap: float,
    overlay: LiveExitOverlay | None = None,
) -> tuple[ExitResult, bool]:
    overlay = overlay if overlay is not None else _LIVE_EXIT_OVERLAY
    if overlay is None:
        return ExitResult(False), False

    state = _live_exit_state.setdefault(call_id, {})
    if overlay.kind == "bank":
        threshold = overlay.bank_mult or 0.0
        if threshold <= 0:
            return ExitResult(False), False
        streak_key = f"{overlay.name}:streak"
        streak = int(state.get(streak_key, 0))
        if current_mult >= threshold:
            streak += 1
            state[streak_key] = streak
            if streak >= max(1, overlay.confirm_ticks):
                return ExitResult(True, overlay.name, exit_mcap=current_mcap), False
        else:
            state[streak_key] = 0
        return ExitResult(False), False

    if overlay.kind == "lock_or_bank":
        trigger = overlay.lock_trigger_mult or overlay.bank_mult or 0.0
        floor = overlay.lock_floor_mult or 0.0
        if trigger <= 0 or floor <= 0:
            return ExitResult(False), False
        armed_key = f"{overlay.name}:armed"
        if current_mult >= trigger:
            state[armed_key] = True
            return ExitResult(False), True
        if state.get(armed_key) and current_mult <= floor:
            return ExitResult(True, overlay.name, exit_mcap=current_mcap), True
        return ExitResult(False), bool(state.get(armed_key))

    if overlay.kind == "lock_trail":
        # See qsim.py for the rationale. Exit level is max(floor, peak*(1-trail)):
        # the floor binds early, the trail takes over once the peak is high enough.
        trigger = overlay.lock_trigger_mult or 0.0
        floor   = overlay.lock_floor_mult or 0.0
        trail   = overlay.lock_trail_pct or 0.0
        if trigger <= 0 or floor <= 0 or trail <= 0:
            return ExitResult(False), False
        armed_key = f"{overlay.name}:armed"
        peak_key  = f"{overlay.name}:peak"
        peak = max(float(state.get(peak_key, 0.0)), current_mult)
        state[peak_key] = peak
        if not state.get(armed_key):
            if current_mult >= trigger:
                state[armed_key] = True
                return ExitResult(False), True
            return ExitResult(False), False
        if current_mult <= max(floor, peak * (1.0 - trail)):
            return ExitResult(True, overlay.name, exit_mcap=current_mcap), True
        return ExitResult(False), True

    return ExitResult(False), False


# Exit quotes are the highest-value quote we make (real money on the line), so a
# transient Jupiter 429 is retried briefly before we surrender to the feed basis —
# the feed is exactly the liar the quote path exists to bypass (measured: ~2/3 of live
# exits were deciding on feed because a raw 429 dropped straight to fallback). A genuine
# no-route (None, no 429) is NOT retried: the coin is unsellable this tick, so retrying
# won't help and the caller must fall back. Bounded so a sell never waits > retries*ms.
_EXIT_QUOTE_RETRIES  = int(os.getenv("LIVE_EXIT_QUOTE_RETRIES", "2"))       # extra tries after the first
_EXIT_QUOTE_RETRY_MS = float(os.getenv("LIVE_EXIT_QUOTE_RETRY_MS", "180"))  # backoff between tries (ms)

# The ENTRY side had no retry at all, which is why a 429 deleted a candidate outright.
# Measured 2026-09-30: 27 of 114 live skips were rate limits (19 pre-entry buy quote, 8
# roundtrip sell quote) against 6 trades actually taken — the API was rejecting four
# times more candidates than the strategy was. Two of that day's five in-band banks went
# this way (CAKE +0.0507 got as far as submitting the buy; NTDA +0.0503 never quoted),
# worth more than the day's whole P&L. A gate that drops a coin because the API was busy
# is not selecting anything, it is thinning the flow at random.
_ENTRY_QUOTE_RETRIES  = int(os.getenv("LIVE_ENTRY_QUOTE_RETRIES", "3"))
_ENTRY_QUOTE_RETRY_MS = float(os.getenv("LIVE_ENTRY_QUOTE_RETRY_MS", "250"))


async def _entry_quote_retry(make_call, what: str, symbol: str, call_id: int):
    """Entry-side Jupiter quote with the bounded 429 retry the exit path already had.

    Returns (value, ok). ok=False means rate-limited after every attempt; the caller
    still skips, but only after actually trying. A genuine no-route is NOT a 429 and
    comes back as (None, True) — retrying cannot conjure liquidity, and the caller
    already distinguishes that case.
    """
    attempts = _ENTRY_QUOTE_RETRIES + 1
    for i in range(attempts):
        try:
            return await make_call(), True
        except jupiter.RateLimitError:
            if i < attempts - 1:
                await asyncio.sleep(_ENTRY_QUOTE_RETRY_MS / 1000.0)
                continue
            print(f"[live] {symbol} {what} 429 after {attempts} tries call_id={call_id}")
            return None, False
    return None, False


def _rpc_url() -> str:
    return os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")


# ── Circuit breaker ────────────────────────────────────────────────────────────

async def _trip_circuit_breaker(loss: float, limit: float, kind: str = "daily") -> None:
    global _circuit_broken
    _circuit_broken = True
    _STATE_DIR.mkdir(exist_ok=True)
    _CIRCUIT_FLAG_FILE.write_text(
        f"tripped={datetime.now(timezone.utc).isoformat()}\n"
        f"kind={kind}\n"
        f"net_loss={loss:.4f} SOL\n"
        f"limit={limit:.4f} SOL\n"
    )
    print(
        f"[live] ⛔ CIRCUIT BREAKER TRIPPED ({kind})"
        f"  net_loss={loss:.4f} SOL  limit={limit:.4f} SOL"
    )
    try:
        msg = (
            f"🛑 <b>LIVE TRADING HALTED — {kind} circuit breaker</b>\n"
            f"Net loss: {loss:.3f} SOL\n"
            f"Limit:    {limit:.3f} SOL\n\n"
            f"To re-enable: delete <code>{_CIRCUIT_FLAG_FILE}</code> and restart."
        )
        await alert_bot._get_bot().send_message(
            chat_id=alert_bot._chat_id(),
            text=msg,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
    except Exception as e:
        print(f"[live] failed to send circuit breaker alert: {e}")


# ── Public API ─────────────────────────────────────────────────────────────────

async def open_live_position(score_result: dict, token_data: dict) -> bool:
    """
    Execute a real buy via Jupiter and record the open position.

    All 6 safety guards are checked in order before any trade is attempted.
    Every skip is logged with its reason — this is the audit trail.
    Never raises — a failure must not affect paper tracking or alert delivery.
    """
    label   = score_result.get("label")
    call_id = score_result.get("call_id")
    symbol  = token_data.get("symbol", "?")
    mint    = token_data.get("mint_address")

    # ── In-flight mint guard ───────────────────────────────────────────────────
    async with _pending_lock:
        if mint in _pending_mints:
            print(f"[live] {symbol} ({(mint or '')[:8]}...) skipped — buy already in-flight for this mint")
            return False
        _pending_mints.add(mint)
    try:
        # ── Guard 1: kill switch ───────────────────────────────────────────────
        if not _is_enabled():
            print(f"[live] {symbol} skipped — LIVE_TRADING_ENABLED is not 'true'")
            return False

        # ── Guard 2: circuit breaker ───────────────────────────────────────────
        if _circuit_broken:
            print(f"[live] {symbol} skipped — circuit breaker is active")
            return False

        # ── Guard 3: position count cap ────────────────────────────────────────
        open_count = db.get_live_positions_count()
        if open_count >= _max_positions():
            print(
                f"[live] {symbol} skipped — "
                f"max open positions ({_max_positions()}) reached ({open_count} open)"
            )
            return False

        # ── Guard 4: loss circuit breakers (net P&L — winners offset losers) ───
        # Daily breaker: disabled when MAX_DAILY_LOSS_SOL <= 0.
        max_daily = _max_daily_loss()
        if max_daily > 0:
            today_losses = db.get_today_live_losses()
            if today_losses > max_daily:
                await _trip_circuit_breaker(today_losses, max_daily, "daily")
                return False

        # Total breaker: cumulative net loss since LIVE_PNL_SINCE (the test start),
        # so it ignores pre-test history. Disabled when MAX_TOTAL_LOSS_SOL <= 0.
        max_total = _max_total_loss()
        if max_total > 0:
            total_losses = db.get_live_net_loss_since(_pnl_since())
            if total_losses > max_total:
                await _trip_circuit_breaker(total_losses, max_total, "total")
                return False

        # ── Guard 5: duplicate position ────────────────────────────────────────
        if not call_id or not mint:
            print(f"[live] {symbol} skipped — missing call_id or mint_address")
            return False

        if db.get_open_live_position(call_id):
            print(f"[live] {symbol} skipped — live position already open for call_id={call_id}")
            db.set_call_skip_reason(call_id, "duplicate")
            return False

        if db.has_open_live_position_for_mint(mint):
            print(f"[live] {symbol} skipped — live position already open for mint={mint[:12]}...")
            db.set_call_skip_reason(call_id, "duplicate")
            return False

        # ── Guard 5b: re-entry cooldown ────────────────────────────────────────
        # Don't immediately re-buy a mint we just sold — avoids churning fees on
        # the same name when a fresh call arrives shortly after an exit.
        # Disabled when LIVE_REENTRY_COOLDOWN_SECS <= 0.
        cooldown = float(os.getenv("LIVE_REENTRY_COOLDOWN_SECS", "1800"))
        if cooldown > 0:
            since_exit = db.seconds_since_last_live_exit_for_mint(mint)
            if since_exit is not None and since_exit < cooldown:
                print(
                    f"[live] {symbol} skipped — re-entry cooldown "
                    f"({since_exit:.0f}s since last exit < {cooldown:.0f}s) mint={mint[:12]}..."
                )
                db.set_call_skip_reason(call_id, "reentry_cooldown")
                return False

        # ── Channel / score / hour setup ───────────────────────────────────────
        channel_handle = (
            token_data.get("channel_tag") or
            token_data.get("channel_handle") or
            ""
        ).lstrip("@")
        score_val  = float(score_result.get("score") or 0)
        local_now  = datetime.now(timezone.utc).astimezone(LOCAL_TZ)
        local_hour = local_now.hour

        # ── Lane-policy gate — resolved BEFORE size so a LIVE-allowlist lane sets its own size ──
        # LIVE_LANE_STRATEGY "A"/"B" mirror the paper testbed bench (research-wide); "LIVE" uses
        # lane_policy.resolve_live() — a NARROW live-only allowlist (LIVE_LANES) so live trades a
        # realized-CONFIRMED subset while paper stays wide. Both handle day-gates + Sunday opt-in.
        # Does NOT mutate the call's skip_reason (shared lane label the paper dispatch also reads).
        _lane: dict = {"trade": True}
        if LIVE_USE_LANE_POLICY:
            _cat  = db.get_call_skip_reason(call_id)
            if LIVE_LANE_STRATEGY == "LIVE":
                _lane = lane_policy.resolve_live(channel_handle, token_data.get("vip_tier"), _cat)
            else:
                _lane = lane_policy.resolve(channel_handle, token_data.get("vip_tier"), _cat,
                                            strategy=LIVE_LANE_STRATEGY)
            if not _lane.get("trade"):
                print(f"[live] {symbol} skipped — lane_policy({LIVE_LANE_STRATEGY}) "
                      f"{_lane.get('reason', 'not_a_traded_lane')} "
                      f"[{channel_handle}/{token_data.get('vip_tier') or 'none'}/{_cat or 'none'}]")
                return False

        # ── Guard 6: SOL balance (allowlist per-lane size overrides the default when set) ──
        size = float(_lane.get("size") or _position_size(label))
        try:
            balance = _wallet.get_sol_balance(_rpc_url())
            if balance < size + 0.05:
                print(
                    f"[live] {symbol} skipped — SOL balance {balance:.4f}"
                    f" < required {size + 0.05:.4f} (size={size:.4f} + 0.05 reserve)"
                )
                db.set_call_skip_reason(call_id, "balance")
                return False
        except Exception as e:
            print(f"[live] {symbol} skipped — balance check failed: {e}")
            db.set_call_skip_reason(call_id, "balance")
            return False

        # ── Blocked channels ───────────────────────────────────────────────────
        _blocked = {c.strip() for c in os.getenv("LIVE_BLOCKED_CHANNELS", "solwhaletrending").split(",") if c.strip()}
        if channel_handle in _blocked:
            print(f"[live] {symbol} skipped — channel {channel_handle} is blocked")
            db.set_call_skip_reason(call_id, "blocked_channel")
            return False

        # ── Allowed hours whitelist (UTC) ──────────────────────────────────────
        allowed_hours = [int(h) for h in os.getenv("LIVE_ALLOWED_HOURS_UTC", "").split(",") if h.strip()]
        if allowed_hours and datetime.now(timezone.utc).hour not in allowed_hours:
            print(f"[live] {symbol} skipped — hour {datetime.now(timezone.utc).hour} UTC not in allowed window")
            db.set_call_skip_reason(call_id, "allowed_hours")
            return False

        # ── Quiet hours (PST) — mirrors paper Strategy A ───────────────────────
        # QUIET HOURS BYPASS 2026-09-05: when LIVE_USE_LANE_POLICY is on, lane_policy IS
        # the entry decision (the same reasoning that already bypasses the legacy
        # strategy_engine gate below) — so this hour gate is a SECOND, unmirrored filter.
        # It came from Strategy A research on solhousesignal and solwhaletrending was never
        # exempted, so live silently dropped 04/09/14 PST on its ONLY allowlisted lane while
        # qsim took those calls with no such gate — which invalidates every qsim-vs-live
        # comparison. Revert = set LIVE_USE_LANE_POLICY=false (or drop the clause) + restart.
        free_uses_custom_filters = (channel_handle == "solhousesignal")
        vip_uses_lane_allowlist  = (channel_handle == "solhousesignal_vip")
        if (
            local_hour in QUIET_HOURS_PST
            and not LIVE_USE_LANE_POLICY
            and not free_uses_custom_filters
            and not vip_uses_lane_allowlist
        ):
            print(f"[live] {symbol} skipped — quiet hour {local_hour:02d}:00 PST")
            db.set_call_skip_reason(call_id, "quiet_hours")
            return False

        # ── Price fetch ────────────────────────────────────────────────────────
        msg_mcap      = float(token_data.get("mcap_at_call") or 0)
        actual_entry  = None
        market: dict | None = None
        token_onchain: dict = {}
        if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
            try:
                market = data_fetcher.fetch_token_price(mint)
                if market and market.get("mcap"):
                    actual_entry = float(market["mcap"])
            except Exception as e:
                print(f"[live] price fetch failed for {symbol}: {e}")
            try:
                token_onchain = db.get_token_onchain_data(mint) or {}
            except Exception:
                pass
        if actual_entry is None and mint:
            print(f"[live] {symbol} DexScreener returned no mcap — using msg price ${msg_mcap/1000:.1f}k")

        entry_mcap = actual_entry or msg_mcap

        # ── Security flag ──────────────────────────────────────────────────────
        security_flag = (token_data.get("security_flag") or token_onchain.get("security_flag"))
        if security_flag == "warning" and LIVE_BLOCK_SECURITY_WARNING:
            print(f"[live] {symbol} skipped — security={security_flag} call_id={call_id}")
            db.set_call_skip_reason(call_id, "security_warning")
            return False

        # ── VIP gamble minimum mcap floor ──────────────────────────────────────
        vip_tier_val  = (token_data.get("vip_tier") or "")
        is_vip_gamble = vip_tier_val in ("gamble", "gamble_risk")
        if is_vip_gamble and actual_entry is not None and actual_entry < 10_000:
            print(f"[live] {symbol} skipped — mcap ${actual_entry/1000:.1f}k below $10k minimum for vip gamble")
            db.set_call_skip_reason(call_id, "mcap_too_low")
            return False

        # ── Strategy A entry gate (solhousesignal / VIP) ───────────────────────
        # This is the LEGACY strategy_engine entry (free_min_score=63 etc.). It is
        # INCOMPATIBLE with the lane testbed: the testbed's anchor lane is
        # solhousesignal/low_score — calls that scored BELOW 63 — which this gate
        # rejects as "low_score". When LIVE_USE_LANE_POLICY is on, lane_policy.resolve
        # (above) IS the entry decision, mirroring paper exactly, so this legacy gate
        # is bypassed. Independent safety guards (security, mcap ceiling, blocked
        # channels, balance, circuit breaker, dup/cooldown) still apply regardless.
        if not LIVE_USE_LANE_POLICY and channel_handle in ("solhousesignal", "solhousesignal_vip"):
            bundle_pct = token_data.get("bundle_pct_remaining")
            fake_pct   = token_data.get("fake_vol_pct")
            if bundle_pct is None:
                bundle_pct = token_onchain.get("bundle_pct_remaining")
            if fake_pct is None:
                fake_pct = token_onchain.get("fake_vol_pct")

            decision = evaluate_strategy_a_entry(
                StrategyCallContext(
                    call_id=call_id,
                    strategy_name="A",
                    channel_handle=channel_handle,
                    vip_tier=token_data.get("vip_tier"),
                    score=score_val,
                    local_hour_pst=local_hour,
                    entry_mcap=entry_mcap,
                    bundle_pct=float(bundle_pct) if bundle_pct is not None else None,
                    fake_pct=float(fake_pct) if fake_pct is not None else None,
                    security_flag=security_flag,
                    dev_tokens_made=token_onchain.get("dev_tokens_made"),
                    symbol=symbol,
                ),
                STRATEGY_A_V2026_05_22,
            )
            if not decision.should_trade:
                print(f"[live] {symbol} skipped — {decision.reason}")
                db.set_call_skip_reason(call_id, decision.reason)
                return False

        # ── First-call-only dedup for free solhousesignal ─────────────────────
        if channel_handle == "solhousesignal" and mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
            first_free = db.get_first_call_id_for_mint_on_channel(mint, "solhousesignal")
            if first_free and first_free != call_id:
                print(f"[live] {symbol} skipped — later free solhousesignal repeat (first={first_free})")
                db.set_call_skip_reason(call_id, "duplicate")
                return False

        # ── Channel mcap ceiling (outer safety net) ────────────────────────────
        max_mcap = MCAP_LIMITS.get(channel_handle, DEFAULT_MCAP_LIMIT)
        if max_mcap and actual_entry and actual_entry > max_mcap:
            print(f"[live] {symbol} skipped — mcap ${actual_entry/1000:.0f}k too high for {channel_handle or 'unknown'} (max ${max_mcap/1000:.0f}k)")
            db.set_call_skip_reason(call_id, "mcap_too_high")
            return False

        # ── mcap band + dev_sold filter (REAL MONEY ONLY) ─────────────────────
        # Gates on msg_mcap (= calls.mcap_at_call), NOT actual_entry. The band
        # was measured on mcap_at_call, and the two diverge badly on fast risers
        # where the feed under-records the entry by ~2.3x — gating on the live
        # quote would select a different population than the one measured.
        # Fails CLOSED, unlike dev_gate: positions with missing token metadata
        # ran -17.60%/SOL at a 24.9% win rate against -8.70% and 34.2% for those
        # with it, so declining to trade what we cannot verify is the measured
        # action rather than the merely cautious one.
        try:
            import entry_filter
            _ok, _why = entry_filter.check(mint, msg_mcap)
            if not _ok:
                print(f"[live] {symbol} skipped — entry filter: {_why}")
                db.set_call_skip_reason(call_id, "entry_filter")
                return False
        except Exception as e:
            print(f"[live] {symbol} skipped — entry filter unavailable: "
                  f"{type(e).__name__} {e}")
            db.set_call_skip_reason(call_id, "entry_filter")
            return False

        # ── Clean-deployer gate ───────────────────────────────────────────────
        # Same gate qsim runs. Deployers with 3+ prior tokens and no prior rug
        # rug half as often AND ship coins that run ~4x more (run:lose 0.94 ->
        # 3.65) — see dev_history_edge.py. Honours DEV_GATE_MODE, so it blocks
        # nothing while that is 'shadow'. Fails OPEN on any error: a gate that is
        # down must not quietly become a gate that rejects everything.
        if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
            try:
                import dev_gate
                _g = await dev_gate.check(call_id, mint, channel_handle, context="live")
                if not _g.allowed:
                    print(f"[live] {symbol} skipped — dev gate: {_g.reason} "
                          f"(prior_n={_g.prior_n} rugs={_g.prior_rugs} "
                          f"{_g.latency_ms:.0f}ms)")
                    db.set_call_skip_reason(call_id, "dev_gate")
                    return False
            except Exception as e:
                print(f"[live] dev gate error (allowing): {type(e).__name__} {e}")

        if (
            (LIVE_MAX_ENTRY_EXEC_RATIO > 0 or LIVE_ENTRY_ROUNDTRIP_MIN_MULT > 0)
            and mint
            and not mint.startswith(("INFERRED:", "UNKNOWN:"))
        ):
            quote_tokens, _ok = await _entry_quote_retry(
                lambda: jupiter.get_buy_quote(mint, size, raise_on_ratelimit=True),
                "pre-entry buy quote", symbol, call_id)
            if not _ok:
                print(f"[live] {symbol} skipped — pre-entry buy quote 429 call_id={call_id}")
                db.set_call_skip_reason(call_id, "entry_quote_429")
                return False
            if not quote_tokens or quote_tokens <= 0:
                print(f"[live] {symbol} skipped — pre-entry buy quote no-route call_id={call_id}")
                db.set_call_skip_reason(call_id, "entry_quote_no_route")
                return False
            _s, quote_decimals = db.get_token_supply_and_decimals(mint)
            quote_entry = _effective_fill_mcap(mint, size, quote_tokens, quote_decimals, market=market)
            gate = entry_quality.check_entry_exec_ratio(
                max_ratio=LIVE_MAX_ENTRY_EXEC_RATIO,
                executable_mcap=quote_entry,
                token_data=token_data,
                market=market,
            )
            if not gate.allowed:
                print(
                    f"[live] {symbol} skipped — entry exec/ref ratio "
                    f"{(gate.ratio or 0):.2f}x > {gate.max_ratio:.2f}x "
                    f"exec=${(gate.executable_mcap or 0)/1000:.1f}k "
                    f"ref=${(gate.reference_mcap or 0)/1000:.1f}k "
                    f"source={gate.reference_source or '?'} call_id={call_id}"
                )
                db.set_call_skip_reason(call_id, gate.reason)
                return False
            if LIVE_ENTRY_ROUNDTRIP_MIN_MULT > 0:
                roundtrip_sol, _ok = await _entry_quote_retry(
                    lambda: jupiter.get_sell_quote(mint, quote_tokens, raise_on_ratelimit=True),
                    "pre-entry roundtrip sell quote", symbol, call_id)
                if not _ok:
                    print(f"[live] {symbol} skipped — pre-entry roundtrip sell quote 429 call_id={call_id}")
                    db.set_call_skip_reason(call_id, "entry_roundtrip_429")
                    return False
                roundtrip = entry_quality.check_roundtrip(
                    min_mult=LIVE_ENTRY_ROUNDTRIP_MIN_MULT,
                    sol_in=size,
                    sol_out=roundtrip_sol,
                )
                if not roundtrip.allowed:
                    print(
                        f"[live] {symbol} skipped — entry roundtrip "
                        f"{(roundtrip.mult or 0):.2f}x < {roundtrip.min_mult:.2f}x "
                        f"sol_in={size:.4f} sol_out={(roundtrip.sol_out or 0):.4f} "
                        f"reason={roundtrip.reason} call_id={call_id}"
                    )
                    db.set_call_skip_reason(call_id, roundtrip.reason)
                    return False

        # ── Execute buy ────────────────────────────────────────────────────────
        print(
            f"[live] BUY {symbol}  call_id={call_id}"
            f"  size={size:.4f} SOL  mint={mint[:8]}..."
        )
        result = await jupiter.buy_token(mint, size)

        if not result["success"]:
            print(
                f"[live] BUY FAILED {symbol}  call_id={call_id}"
                f"  error={result.get('error')}  code={result.get('code')}"
            )
            return False

        sig             = result["signature"]
        sol_spent       = result["sol_spent"]
        tokens_received = result["tokens_received"]
        decimals        = result.get("tokens_decimals", 6)
        router          = result.get("router", "unknown")

        entry_price = actual_entry or msg_mcap

        tokens_display = tokens_received / (10 ** decimals) if decimals > 0 else tokens_received

        db.open_live_position(
            call_id=call_id,
            entry_price=entry_price,
            sol_in=sol_spent,
            tokens_held=tokens_received,
            tx_signature=sig,
            router=router,
        )
        print(
            f"[live] BUY OK  {symbol}  call_id={call_id}"
            f"  sol_spent={sol_spent:.4f}  tokens={tokens_received}"
            f"  router={router}  sig={sig[:16]}..."
        )
        # Record the TRUE fill-derived entry mcap alongside the feed value, so we can
        # audit how far the laggy feed (entry_price) sits from what we actually paid.
        entry_fill = _effective_fill_mcap(mint, sol_spent, tokens_received, decimals, market=market)
        if entry_fill is not None:
            db.set_live_fill_price(call_id, entry_price_fill=entry_fill)
            _ratio = (entry_fill / entry_price) if entry_price else 0
            print(f"[live] ENTRY FILL  {symbol}  effective_mcap=${entry_fill/1000:.1f}k"
                  f"  feed=${entry_price/1000:.1f}k  ratio={_ratio:.2f}x")
        await alert_bot.send_live_buy_alert(
            symbol=symbol,
            mint=mint,
            sol_spent=sol_spent,
            tokens_received=tokens_display,
            signature=sig,
        )
        return True
    finally:
        async with _pending_lock:
            _pending_mints.discard(mint)


def _effective_fill_mcap(
    mint: str,
    sol_amount: float,
    tokens_raw: int,
    decimals: int | None = None,
    market: dict | None = None,
) -> float | None:
    """
    The TRUE mcap implied by an actual SOL<->token swap:
        mcap = (sol_amount * sol_usd) * supply_whole / (tokens_raw / 10**decimals)
    i.e. what you *really* paid/received per token, scaled to full supply — the
    ground-truth price the laggy feed (entry_price/exit_price) does NOT capture.
    Supply/decimals: tokens table first, feed-implied (mcap/price_usd) fallback,
    pump.fun 1e9 default. Returns None on any failure — must never block a trade.
    """
    try:
        # Every failure path below SAYS WHY. Three of them used to return None silently,
        # which left qsim's "entry mcap calc failed" skip (397 lost opens in the log)
        # pointing at five possible causes with no way to tell them apart. A skip you
        # cannot attribute is a skip you cannot fix.
        if not mint or not tokens_raw or not sol_amount or sol_amount <= 0:
            print(f"[live] effective mcap: bad inputs mint={bool(mint)} "
                  f"tokens_raw={tokens_raw} sol_amount={sol_amount}")
            return None
        supply_whole = None
        try:
            _s, _d = db.get_token_supply_and_decimals(mint)
            if _d is not None:
                decimals = int(_d)
            if _s:
                supply_whole = float(_s) / (10 ** (int(_d) if _d is not None else (decimals or 6)))
        except Exception:
            pass
        if supply_whole is None and market and market.get("mcap") and market.get("price_usd"):
            supply_whole = float(market["mcap"]) / float(market["price_usd"])
        if supply_whole is None:
            supply_whole = 1_000_000_000.0  # pump.fun standard total supply
        sol_usd = data_fetcher.get_sol_price_usd()
        if not sol_usd:
            print(f"[live] effective mcap: no SOL/USD price for {mint[:8]} "
                  f"— every fill anchor fails while this is down")
            return None
        # decimals=0 is a FAILURE READING, not a valid one. The swap result can carry an
        # explicit 0 (ZPAD, 2026-09-29) and `0 is not None` let it through, so 10**0
        # skipped the divide and returned a mcap 1e6 low — which then drove an instant
        # bank_2x. Both the supply divide above and the caller's tokens_display already
        # coerce 0 -> 6; this was the one site that did not, and the only one whose
        # output becomes the exit anchor.
        dec = decimals if (decimals is not None and decimals > 0) else 6
        tokens_whole = tokens_raw / (10 ** dec)
        if tokens_whole <= 0 or supply_whole <= 0:
            print(f"[live] effective mcap: nonpositive for {mint[:8]} "
                  f"tokens_whole={tokens_whole} supply_whole={supply_whole} "
                  f"(decimals={decimals} -> {dec})")
            return None
        # INVARIANT: a 0.05 SOL buy cannot acquire more tokens than the token has. When
        # the exponent is wrong the two sides disagree by orders of magnitude, which is
        # far cheaper to detect than to guess the right one. Returning None leaves
        # entry_price_fill NULL, so the exit anchors on the feed — degraded (that is the
        # live_exit_feed_anchor_bug behaviour) but never a 1e6 error driving a sell.
        if tokens_whole > supply_whole:
            print(f"[live] effective mcap INCOHERENT for {mint[:8]}: {tokens_whole:.0f} "
                  f"tokens vs supply {supply_whole:.0f} (decimals={decimals}) "
                  f"— no fill anchor recorded, exits stay on the feed basis")
            return None
        return (sol_amount * sol_usd) * supply_whole / tokens_whole
    except Exception as e:
        print(f"[live] effective mcap calc failed: {e}")
        return None


async def _exit_sell_quote(mint: str, tokens_held: int) -> tuple[float | None, str]:
    """
    Sell-quote for the live EXIT path, hardened against transient Jupiter 429s.
    Opts into RateLimitError (raise_on_ratelimit=True) so a rate-limit is distinguished
    from a genuine no-route: a 429 is retried up to _EXIT_QUOTE_RETRIES times with a
    short backoff (the quote usually clears within a few hundred ms), while a no-route
    (None with no 429) returns immediately — retrying won't conjure liquidity.
    Never raises; never blocks a sell beyond retries*retry_ms.

    Returns (sol_out, status). status is "ok", "no_route" (the bag is genuinely
    unsellable this tick), "rate_limited" (throttled — the bag is fine, we aren't)
    or "error". The caller needs that distinction: a no-route says something about
    the COIN, a 429 says something about US, and only the former may ever count
    toward declaring a position dead.
    """
    attempts = _EXIT_QUOTE_RETRIES + 1
    for i in range(attempts):
        try:
            out = await jupiter.get_sell_quote(mint, tokens_held, raise_on_ratelimit=True)
            if out is None:
                return None, "no_route"  # retrying won't conjure liquidity
            return out, "ok"
        except jupiter.RateLimitError:
            if i < attempts - 1:
                await asyncio.sleep(_EXIT_QUOTE_RETRY_MS / 1000.0)
                continue
            print(f"[live] exit quote 429 after {attempts} tries for {mint[:8]}")
            return None, "rate_limited"
        except Exception as e:
            print(f"[live] exit quote error for {mint[:8]}: {e}")
            return None, "error"
    return None, "error"


async def live_effective_current(pos: dict) -> tuple[float, float] | None:
    """
    Price a live position at its TRUE sellable value via a real Jupiter sell-quote
    for the actual bag, expressed as a synthetic 'current mcap' that preserves the
    real multiple (quote_sol_out / sol_in), anchored on the real fill entry.

    Returns (synthetic_current_mcap, real_multiple) or None on any failure — callers
    MUST fall back to the feed path on None. Cached per mint for _SELL_QUOTE_TTL so
    the watchlist loop + end-sweep don't double-quote. Phase 1 uses this for logging
    only; Phase 2 will let it drive check_live_exits.
    """
    try:
        mint         = pos.get("mint_address")
        tokens_held  = int(pos.get("tokens_held") or 0)
        sol_in       = float(pos.get("sol_in") or 0)
        # Anchor on the REAL fill, not the laggy feed entry; fall back to feed only if
        # the fill wasn't recorded (pre-instrumentation position).
        entry_anchor = float(pos.get("entry_price_fill") or pos.get("entry_price") or 0)
        if not mint or tokens_held <= 0 or sol_in <= 0 or entry_anchor <= 0:
            return None
        now = time.monotonic()
        cached = _sell_quote_cache.get(mint)
        if cached and (now - cached[1]) < _SELL_QUOTE_TTL:
            sol_out = cached[0]
        else:
            sol_out, _status = await _exit_sell_quote(mint, tokens_held)
            _LAST_QUOTE_STATUS[mint] = _status
            if _status == "no_route":
                _noroute_streak[mint] = _noroute_streak.get(mint, 0) + 1
                if _noroute_streak[mint] in (LIVE_NOROUTE_WARN_AT, LIVE_NOROUTE_WARN_AT * 3):
                    print(f"[live] {mint[:8]} NO ROUTE x{_noroute_streak[mint]} — bag may be "
                          f"unsellable; holding (a no-route bag cannot be exited)")
            elif _status == "ok":
                _noroute_streak.pop(mint, None)
            if sol_out is None or sol_out <= 0:
                return None
            _sell_quote_cache[mint] = (sol_out, now)
        real_mult = sol_out / sol_in
        return entry_anchor * real_mult, real_mult
    except Exception as e:
        print(f"[live] effective current calc failed: {e}")
        return None


async def live_exit_basis(
    call_id: int,
    pos: dict,
    feed_current: float,
    feed_peak: float,
    feed_entry: float,
) -> tuple[float, float, float, str, float | None] | None:
    """
    Return the (current, peak, entry) triple to hand check_live_exits, plus a basis tag,
    or None meaning MAKE NO DECISION THIS TICK.

    Phase 2: when LIVE_EXIT_USE_QUOTE is on AND we have a real fill anchor + a live
    sell-quote, the triple is on the REAL (wallet) basis:
        current = entry_price_fill * (quote_sol_out / sol_in)   (synthetic real mcap)
        entry   = entry_price_fill                              (the real fill)
        peak    = ratcheted real peak (guard-corroborated, DB-shared across processes)
    so every exit ratio (drawdown-from-peak, multiple-from-entry) reflects what the bag
    is really worth, not the laggy feed. Returns (current, peak, entry, basis, raw_mult).

    WHAT HAPPENS WHEN THERE IS NO QUOTE depends on WHY, and the two cases differ:

      TRANSIENT (quote 429'd or errored) — under LIVE_EXIT_REQUIRE_QUOTE returns None:
        no decision, hold, re-quote next tick. Handing the feed's ruler to the exit
        rules is what produced two false hard stops on live day 1, and a feed decision
        during a quote outage is unactionable anyway since the sell needs a route too.

      PERMANENT (no entry_price_fill recorded, or LIVE_EXIT_USE_QUOTE off) — returns the
        FEED triple. These positions can NEVER reach the quote basis, so returning None
        would mean they never exit at all. A pre-instrumentation row on the feed's ruler
        is worse than qsim but far better than unmanaged.

    `raw_mult` is the UNGUARDED executable multiple (quote_sol_out / sol_in) — the same
    number qsim calls `real_mult`. It is None on the feed basis. check_live_exits uses it
    for the bank overlay and the hard stop so live evaluates those off the RAW quote exactly
    like qsim after 6da293d / e81d4d7; every other exit rule stays on the guarded triple.
    """
    # SINGLE-RULER INVARIANT. feed_current and feed_peak are feed numbers, so the feed
    # triple's entry leg must be the feed entry. A caller handing us entry_price_fill
    # instead makes a still coin read feed_entry/fill_entry — ClapCat and BUTTHOLE (fills
    # 1.39x and 1.40x the feed) sat at 0.718 and 0.715, under the 0.80 hard stop from
    # birth, and were both sold while their real quotes were worth 0.98 and 1.05. All
    # three callsites were fixed, but enforcing it here is what stops a fourth from
    # reintroducing it, and this function already has pos to check against.
    feed_anchor = float(pos.get("entry_price") or 0) or feed_entry
    feed_triple = (feed_current, feed_peak, feed_anchor, "feed", None)

    def _no_quote(why: str):
        """Transient quote failure. None = no decision under LIVE_EXIT_REQUIRE_QUOTE.

        Throttled: during a 429 storm this fires every tick on every open position, and
        the point of the line is to tell you the basis went away, not to fill the log.
        """
        if not LIVE_EXIT_REQUIRE_QUOTE:
            return feed_triple
        _now = time.monotonic()
        if _now - _no_quote_logged.get(call_id, 0.0) >= _NO_QUOTE_LOG_SECS:
            _no_quote_logged[call_id] = _now
            print(f"[live] call_id={call_id} PROFIT-SIDE EXITS PAUSED — {why}; "
                  f"collapse guard still armed")
        # Hand back a PROTECTIVE basis rather than nothing, so a collapse can still get
        # out while every profit-side rule stays silent. If the feed legs are unusable
        # too there is genuinely nothing to decide on.
        if feed_current > 0 and feed_peak > 0 and feed_anchor > 0:
            return feed_current, feed_peak, feed_anchor, "feed_protective", None
        return None

    # PERMANENT cases fall back to the feed: they can never reach the quote basis, so
    # returning None would leave the position unmanaged forever.
    if not LIVE_EXIT_USE_QUOTE:
        return feed_triple
    try:
        real_entry = float(pos.get("entry_price_fill") or 0)
        if real_entry <= 0:
            return feed_triple                      # pre-instrumentation position
        eff = await live_effective_current(pos)
        if not eff:
            mint = pos.get("mint_address") or ""
            return _no_quote(_LAST_QUOTE_STATUS.get(mint, "quote unavailable"))
        synth_current, real_mult = eff
        if synth_current <= 0:
            return _no_quote("quote produced a non-positive value")
        # Ratchet the real peak off observed sell-quote value. Seed from the DB row so the
        # peak is shared across sol-monitor + sol-ws-monitor and survives restarts; the
        # guard adds the same single-tick corroboration used on the feed side.
        # Seed the real-peak floor from the fill: a coin that dips right after entry (first
        # successful quote below the fill) must not leave real_peak reading absurdly below
        # entry — trail/floor arm off peak/entry, so a sub-entry peak understates every ratio
        # (this is what left `buy`'s real_peak at 3711 under an 3838 fill).
        prior_peak = max(float(db.get_live_real_peak(call_id) or 0.0), real_entry)
        real_peak  = peak_guard.guard_peak(
            f"realL:{call_id}",
            synth_current,
            prior_peak,
            pending_ttl_secs=LIVE_QUOTE_PEAK_PENDING_TTL_SECS,
        )
        if real_peak > prior_peak:
            # peak_multiplier rides along on the REAL basis here (real_peak/real_entry),
            # so both legs share one ruler — the qsim-equivalent number.
            db.update_live_real_peak(call_id, real_peak,
                                     real_peak / real_entry if real_entry > 0 else None)
        eff_current = min(synth_current, real_peak) if real_peak > 0 else synth_current
        return eff_current, real_peak, real_entry, "real", real_mult
    except Exception as e:
        print(f"[live] exit-basis calc failed, using feed: {e}")
        return feed_current, feed_peak, feed_anchor, "feed", None


async def close_live_position(
    call_id: int,
    current_mcap: float,
    exit_reason: str,
) -> bool:
    """
    Verify on-chain token balance, execute sell, record the close.
    Never raises.
    """
    sell_executed = False
    mint = None
    symbol = "?"

    def _release_claim() -> None:
        try:
            db.release_live_position_exit_claim(call_id)
        except Exception as release_error:
            print(f"[live] exit-claim release failed call_id={call_id}: {release_error}")

    try:
        pos = db.get_open_live_position(call_id)
        if not pos:
            return False
        try:
            claimed = db.claim_live_position_exit(call_id)
        except Exception as claim_error:
            print(f"[live] close skipped call_id={call_id} — claim failed: {claim_error}")
            return False
        if not claimed:
            print(f"[live] close skipped call_id={call_id} — sell already in progress")
            return False

        mint    = pos.get("mint_address")
        symbol  = pos.get("symbol", "?")
        sol_in  = float(pos["sol_in"])

        if not mint:
            print(f"[live] close skipped call_id={call_id} — no mint in position")
            _release_claim()
            return False

        # ── Use stored token amount — avoids an extra RPC round-trip before sell ───
        # tokens_held is the raw integer amount received at buy time.
        # If it turns out to be 0 or stale, sell_token will fail gracefully and
        # we fall back to a live balance check before retrying next cycle.
        wallet_addr = _wallet.get_public_key()
        tokens_held = int(pos.get("tokens_held") or 0)
        if tokens_held == 0:
            # Rare: DB value missing — verify on-chain before giving up
            tokens_held, _ = await jupiter.get_token_balance(mint, wallet_addr, _rpc_url())
            if tokens_held == 0:
                print(
                    f"[live] ⚠️ tokens_held=0 for {symbol} call_id={call_id}"
                    f" — sell skipped, will retry next cycle  mint={mint}"
                )
                try:
                    await alert_bot._get_bot().send_message(
                        chat_id=alert_bot._chat_id(),
                        text=f"⚠️ Balance 0 for ${symbol} — sell skipped, retrying",
                        disable_web_page_preview=True,
                    )
                except Exception as alert_error:
                    print(f"[live] balance=0 alert failed: {alert_error}")
                _release_claim()
                return False

        # ── Execute sell ───────────────────────────────────────────────────────
        print(
            f"[live] SELL {symbol}  call_id={call_id}"
            f"  tokens={tokens_held}  reason={exit_reason}"
        )
        result = await jupiter.sell_token(mint, tokens_held)
        print(f"[live_sell] sell_token result: {result}")

        if not result["success"]:
            print(
                f"[live] SELL FAILED {symbol}  call_id={call_id}"
                f"  error={result.get('error')} — MANUAL INTERVENTION REQUIRED"
            )
            try:
                await alert_bot.send_live_sell_failed_alert(symbol=symbol, mint=mint)
            except Exception as alert_error:
                print(f"[live] sell-failed alert failed: {alert_error}")
            _release_claim()
            return False

        sell_executed = True
        sig          = result["signature"]
        sol_received = result["sol_received"]

        if sol_received <= 0:
            print(
                f"[live] SELL executed but sol_received=0 for {symbol} "
                f"call_id={call_id} — NOT closing position, will retry. "
                f"Check tx: {sig}"
            )
            try:
                await alert_bot._get_bot().send_message(
                    chat_id=alert_bot._chat_id(),
                    text=(
                        f"⚠️ Sell executed for ${symbol} but SOL received = 0. "
                        f"Position kept open. Check Solscan."
                    ),
                    disable_web_page_preview=True,
                )
            except Exception as alert_error:
                print(f"[live] sol_received=0 alert failed: {alert_error}")
            _release_claim()
            return False

        pnl = sol_received - sol_in

        try:
            db.close_live_position_db(
                call_id=call_id,
                exit_price=current_mcap,
                sol_out=sol_received,
                exit_reason=exit_reason,
                tx_signature=sig,
            )
        except Exception as close_error:
            print(
                f"[live] SELL OK BUT DB CLOSE FAILED {symbol} call_id={call_id}"
                f" sig={sig} error={close_error} — leaving status=closing"
            )
            try:
                await alert_bot._get_bot().send_message(
                    chat_id=alert_bot._chat_id(),
                    text=(
                        f"🚨 ${symbol} sold on-chain but DB close failed. "
                        f"call_id={call_id} sig={sig}"
                    ),
                    disable_web_page_preview=True,
                )
            except Exception as alert_error:
                print(f"[live] db-close-failed alert failed: {alert_error}")
            return False

        # Record the TRUE fill-derived exit mcap alongside the feed value (current_mcap),
        # so wallet-implied vs feed can be audited per leg without inferring from entry.
        exit_fill = _effective_fill_mcap(mint, sol_received, tokens_held)
        if exit_fill is not None:
            db.set_live_fill_price(call_id, exit_price_fill=exit_fill)
            _eratio = (exit_fill / current_mcap) if current_mcap else 0
            print(f"[live] EXIT FILL  {symbol}  effective_mcap=${exit_fill/1000:.1f}k"
                  f"  feed=${current_mcap/1000:.1f}k  ratio={_eratio:.2f}x")
        print(
            f"[live] SELL OK  {symbol}  call_id={call_id}"
            f"  reason={exit_reason}  sol_received={sol_received:.4f}"
            f"  pnl={pnl:+.4f}  sig={sig[:16]}..."
        )
        _runner_window_until.pop(call_id, None)
        _live_exit_state.pop(call_id, None)
        position_alerts.clear(call_id)
        try:
            await alert_bot.send_live_sell_alert(
                symbol=symbol,
                mint=mint,
                sol_received=sol_received,
                pnl=pnl,
                exit_reason=exit_reason,
                signature=sig,
            )
        except Exception as alert_error:
            print(f"[live] sell-ok alert failed: {alert_error}")
        return True
    except Exception as close_error:
        print(f"[live] close error {symbol} call_id={call_id}: {close_error}")
        if not sell_executed:
            _release_claim()
        else:
            print(f"[live] sell may have executed for {symbol}; leaving claim in place")
        return False


def check_live_exits(
    call_id: int,
    current_mcap: float,
    peak_mcap: float,
    entry_mcap: float,
    exit_config: ExitConfig = None,
    raw_mult: float | None = None,
    basis: str = "real",
) -> ExitResult:
    """
    Check whether the open live position for call_id should be exited.

    Uses get_open_live_position() so it only fires for real positions.
    Synchronous — same pattern as paper_trader.check_exits().

    exit_config defaults to the module-level _LIVE_EXIT_CONFIG which is
    loaded from the EXIT_STRATEGY env var at startup.

    `raw_mult` (from live_exit_basis, None on the feed basis) is the UNGUARDED executable
    multiple. When present, the hard stop and the bank overlay read it instead of the
    guarded/peak-capped mcap — mirroring qsim, whose two most important fixes were exactly
    this: 6da293d (bank overlay off the raw quote, not guarded peak data) and e81d4d7 (close
    a raw sell quote below the hard stop before guard_trough can hold the position open).
    Without it live cannot reproduce qsim's exits even in principle. None -> old behavior.
    """
    position = db.get_open_live_position(call_id)
    if not position:
        return ExitResult(False)

    if entry_mcap <= 0:
        return ExitResult(False)

    # ── No quote this tick: collapse guard ONLY ───────────────────────────────────
    # Everything below this block reads the feed's ruler, whose entry lag biases the
    # multiple HIGH — which fires banks and floors EARLY and gives up real upside. So on
    # the protective basis none of it runs. What DOES run needs both legs to agree, each
    # chosen to err toward silence (see LIVE_PROTECTIVE_DD).
    if basis == "feed_protective":
        cfg_p = exit_config or _LIVE_EXIT_CONFIG
        stop_p = cfg_p.hard_stop_pct if cfg_p and cfg_p.hard_stop_pct > 0 else 0.20
        dd = (current_mcap / peak_mcap) if peak_mcap > 0 else 1.0
        from_entry = current_mcap / entry_mcap
        if dd <= LIVE_PROTECTIVE_DD and from_entry <= (1.0 - stop_p):
            print(f"[live] call_id={call_id} COLLAPSE with no quote — "
                  f"{dd:.0%} of its own peak and {from_entry:.2f}x from entry; selling")
            return ExitResult(True, "rug", exit_mcap=current_mcap)
        return ExitResult(False)

    # Never act on a 0/null current_mcap (dead/unavailable feed). On LIVE this would fire
    # an UNNECESSARY real sell of a possibly-healthy position — the swap executes at the
    # real market price, so a feed glitch dumps a winner (opportunity cost + fees), not a
    # fake -100%. A genuine decline still produces a real reading and exits normally. Mirrors
    # the paper_trader guard; see PASSION 2026-06-30 (was +36% at last tick, 0-marked).
    if current_mcap <= 0:
        return ExitResult(False)

    cfg = exit_config if exit_config is not None else _LIVE_EXIT_CONFIG
    is_vip_gamble = position.get("vip_tier") in ("gamble_risk", "gamble")
    channel_handle = (position.get("channel_handle") or "").lstrip("@")
    entry_time = position.get("entry_time")

    # ── RAW executable hard stop (mirrors qsim e81d4d7) ──────────────────────────
    # A real sell quote already below the stop is not a phantom — it is the price the bag
    # would actually fetch. guard_trough exists to survive a bad FEED tick, but on the quote
    # basis it only delays a stop we know is real (in qsim it kept positions open for hours
    # far below the stop). So on the raw basis the stop fires here, before the guard.
    if raw_mult is not None and raw_mult > 0:
        raw_stop_pct = cfg.vip_gamble_hard_stop_pct if is_vip_gamble else cfg.hard_stop_pct
        if raw_stop_pct > 0 and raw_mult <= (1.0 - raw_stop_pct):
            return ExitResult(True, "hard_stop", exit_mcap=entry_mcap * raw_mult)

    # Low-side corroboration: hold a single uncorroborated crater for one reading so one
    # phantom low tick can't trigger a real stop-sell (mirror of guard_peak on the high
    # side; the high side is already guarded in ws_monitor).
    current_mcap = peak_guard.guard_trough(f"tL:{call_id}", current_mcap)
    if current_mcap <= 0:
        return ExitResult(False)

    current_mult = current_mcap / entry_mcap
    # A multiple past the ceiling means entry_mcap is CORRUPT, not that the coin mooned.
    # Every rule below divides by it, so they are all meaningless and all go quiet. The
    # RAW hard stop above is anchor-independent (raw_mult = sol_out / sol_in) and keeps
    # protecting the downside whenever a sell quote is available, so going quiet here
    # costs upside capture, not safety. Acting on the corrupt value is what sold ZPAD.
    if LIVE_MAX_SANE_MULT > 0 and current_mult > LIVE_MAX_SANE_MULT:
        print(f"[live] ANCHOR CORRUPT call_id={call_id}: current_mult={current_mult:,.0f}x"
              f"  current={current_mcap:.6f}  entry={entry_mcap:.6f}"
              f"  — anchor-derived exits suppressed, raw hard stop still live")
        return ExitResult(False)
    peak_mult = (peak_mcap / entry_mcap) if peak_mcap > 0 else current_mult
    if (
        LIVE_NO_BOUNCE_STOP_ENABLED
        and NO_BOUNCE_ARM_MULT > 0
        and NO_BOUNCE_STOP_MULT > 0
        and peak_mult < NO_BOUNCE_ARM_MULT
        and current_mult <= NO_BOUNCE_STOP_MULT
    ):
        return ExitResult(True, "no_bounce_stop", exit_mcap=current_mcap)

    # Bank overlay on the RAW executable multiple when we have one (qsim 6da293d): the
    # guarded value is capped at min(synth, real_peak), and guard_peak withholds a >50%
    # single-tick jump for one reading — so on exactly the violent spikes bank_1p3x exists
    # to catch, the guarded mult reads below the threshold and the bank does not fire.
    overlay_mult = raw_mult if (raw_mult is not None and raw_mult > 0) else current_mult
    overlay_mcap = (entry_mcap * raw_mult) if (raw_mult is not None and raw_mult > 0) else current_mcap
    overlay_result, overlay_suppresses_base = _apply_live_exit_overlay(
        call_id, overlay_mult, overlay_mcap
    )
    if overlay_result.should_exit:
        return overlay_result
    if overlay_suppresses_base:
        if entry_time is not None:
            if entry_time.tzinfo is None:
                entry_time = entry_time.replace(tzinfo=timezone.utc)
            age_hours = (
                datetime.now(timezone.utc) - entry_time
            ).total_seconds() / 3600.0
            if age_hours > cfg.max_hours:
                return ExitResult(True, "time_stop", exit_mcap=current_mcap)
        return ExitResult(False)

    result = apply_exit_config(
        cfg,
        current_mcap=current_mcap,
        peak_mcap=peak_mcap,
        entry_mcap=entry_mcap,
        is_vip_gamble=is_vip_gamble,
        channel_handle=channel_handle,
        entry_time=entry_time,
    )
    if (
        not LIVE_RUNNER_WINDOW_ENABLED
        or RUNNER_WINDOW_ARM_MULT <= 0
        or RUNNER_WINDOW_RELEASE_MULT <= RUNNER_WINDOW_ARM_MULT
        or RUNNER_WINDOW_MINS <= 0
        or RUNNER_WINDOW_FLOOR_MULT <= 0
    ):
        return result

    now = time.monotonic()
    armed = peak_mult >= RUNNER_WINDOW_ARM_MULT and peak_mult < RUNNER_WINDOW_RELEASE_MULT
    if armed and call_id not in _runner_window_until:
        _runner_window_until[call_id] = now + RUNNER_WINDOW_MINS * 60.0

    until = _runner_window_until.get(call_id)
    if until is None:
        return result
    if current_mult <= RUNNER_WINDOW_FLOOR_MULT:
        _runner_window_until.pop(call_id, None)
        return ExitResult(True, "runner_floor_stop", exit_mcap=current_mcap)
    if peak_mult >= RUNNER_WINDOW_RELEASE_MULT or now >= until:
        _runner_window_until.pop(call_id, None)
        return result
    if result.should_exit and result.reason in RUNNER_WINDOW_PROTECTED_REASONS:
        return ExitResult(False)
    return result


def get_live_pnl_summary() -> dict:
    """Aggregate P&L stats for all closed live positions."""
    return db.get_live_pnl_summary()
