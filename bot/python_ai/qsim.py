"""
qsim.py — quote-priced simulation ("live minus the swap").

WHY
---
Shadow/paper price trades off the price FEED, which for thin memecoins is NOT the price
your wallet executes at (pool depth + slippage make the executable number different, and
differently wrong every time). That's why every paper edge kept evaporating live. qsim
prices ENTRIES off a real Jupiter BUY quote and monitors EXITS off real SELL quotes — the
SAME executable numbers live uses — but NOTHING executes. It's the honest forward paper we
never had: a stock-tape-accurate sim for crypto.

DESIGN
------
* SCOPED to a curated candidate-lane set (QSIM_LANES) so the Jupiter budget stays bounded.
  Start with the ONE realized-confirmed lane to CALIBRATE qsim against live wallet fills;
  widen (one line per lane) once qsim's numbers match the wallet.
* BUDGET-CAPPED: a hard quotes/min cap + a per-position cadence, so qsim can never 429 or
  starve live/entries. On a busy day it quotes each position a little less often instead.
* ISOLATED: imports NO execution code path of its own — it calls jupiter quote helpers and
  reuses live_trader's PURE pricing helper + exit_config's PURE decision fn. It has no
  buy/sell/execute call anywhere, so it cannot move real money by construction.

Entries are dispatched from telegram_client (same call events as live/paper). The monitor
loop (run as its own process) sell-quotes open positions and applies the real exit logic.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone

import db
import entry_quality
import jupiter
import peak_guard
import lane_policy
import live_trader                       # reuse the PURE _effective_fill_mcap only
from exit_config import EXIT_A_PAPER, EXIT_RIDE, ExitResult, apply_exit_config

QSIM_ENABLED = os.getenv("QSIM_ENABLED", "false").lower() == "true"

# ── Candidate lanes (the funnel's confirmation queue) ─────────────────────────
# key = (channel, vip_tier, skip_reason) -> {"days": {weekdays}, "size": SOL, "exit": variant}
# Widen this dict (one line per lane) once qsim is calibrated. "days" honors the same
# globally-skipped-day OPT-IN as lane_policy (list Sun to trade it).
QSIM_LANES: dict[tuple[str, str, str], dict] = {
    # vip_mcap_gate EVERY day: qsim costs no money, so trade all days to (a) smoke-test the
    # pipeline immediately instead of waiting for Sat, and (b) gather HONEST realized data on
    # the days we've been unsure about (Tue/Wed/Fri/Sun). Live still trades only Mon/Sat, so
    # the qsim-vs-live CALIBRATION overlap is naturally that subset — nothing lost. Sun trades
    # via the day-set opt-in past the global skip. exit=early held constant across days (clean
    # test: hold the exit, vary the day). Widen to other lanes once the machinery is proven.
    # EDGE SCAN (2026-08-05): RETIRED vip_mcap_gate here — proven the reject pile (worst qsim
    # mean, most feed mirages AND most lying exits across 74 rows; live already dropped it). Its
    # budget is reallocated to the top UNCONFIRMED shadow candidate below (solwhaletrending/
    # low_score) so qsim can finally referee it. (Existing open vip positions still close out;
    # the monitor reads open rows from the DB, not this gate. Reversible — just re-add the key.)
    # Widen 2026-07-24: the faithfully-testable POSITIVE lanes off the clean 14d shadow window
    # (post-07-11 fixes, so no phantom/June contamination). Both are the "early" variant —
    # early maps to a real ExitConfig (EXIT_A_PAPER); ride_vol positives are HELD because qsim
    # maps ride_vol->plain EXIT_RIDE (no order_flow), which is negative on those lanes = a lie.
    #   * solhousesignal/none/low_score early (+30.05/14d): the ADVERSARIAL referee — reprice
    #     already convicted this anchor (booked +12.9, honest -8.4). If qsim lands near -8 it's a
    #     second independent confirmation qsim catches the entry-inflation lie (front-loaded on
    #     07-12 +17.7, then 5 straight red days — expect qsim to gut it).
    ("solhousesignal",   "none", "low_score"): {"days": {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}, "size": 0.05, "exit": "early"},
    # EDGE-SCAN candidate (2026-08-05): the biggest UNCONFIRMED shadow winner (+15.8/7d), on our
    # best-performing channel, that qsim has never priced. All 7 days for day-cell discovery;
    # early = EXIT_A_PAPER (ride_vol still held — qsim lacks order_flow). Higher volume than the
    # others, so watch qsim logs for 429/backoff; if it starves the budget, gate to fewer days.
    ("solwhaletrending", "none", "low_score"): {"days": {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}, "size": 0.05, "exit": "early"},
}


def _normalize_days(raw) -> set[str]:
    if raw in (None, "", "all"):
        return {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
    if isinstance(raw, str):
        parts = raw.split(",")
    else:
        parts = list(raw)
    days = {str(day).strip()[:3].title() for day in parts if str(day).strip()}
    valid = {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}
    bad = days - valid
    if bad:
        raise ValueError(f"invalid day(s): {sorted(bad)}")
    return days or valid


def _load_extra_qsim_lanes() -> None:
    raw = os.getenv("QSIM_EXTRA_LANES_JSON", "").strip()
    if not raw:
        return
    try:
        entries = json.loads(raw)
        if not isinstance(entries, list):
            raise ValueError("expected a JSON list")
        loaded = 0
        for item in entries:
            if not isinstance(item, dict):
                raise ValueError("each lane must be a JSON object")
            channel = str(item.get("channel") or "").lstrip("@").strip()
            tier = str(item.get("vip_tier") or "none").strip()
            lane = str(item.get("lane") or item.get("skip_reason") or "none").strip()
            variant = str(item.get("variant") or item.get("exit") or "early").strip()
            size = float(item.get("size", 0.05))
            if not channel or not lane or size <= 0:
                raise ValueError(f"bad lane entry: {item}")
            if variant not in _VARIANT_CONFIGS:
                raise ValueError(f"unsupported variant {variant!r}; use early, ride, or ride_vol")
            QSIM_LANES[(channel, tier, lane)] = {
                "days": _normalize_days(item.get("days")),
                "size": size,
                "exit": variant,
            }
            loaded += 1
        print(f"[qsim] loaded {loaded} extra lane(s) from QSIM_EXTRA_LANES_JSON")
    except Exception as e:
        print(f"[qsim] WARNING: invalid QSIM_EXTRA_LANES_JSON ignored: {e}")

# variant string -> the SAME ExitConfig shadow/paper use (keep in sync with shadow_monitor).
_VARIANT_CONFIGS = {"early": EXIT_A_PAPER, "ride": EXIT_RIDE, "ride_vol": EXIT_RIDE}
_load_extra_qsim_lanes()

# Defaults are DELIBERATELY conservative: the Jupiter /order endpoint qsim quotes off has
# its OWN rate limit (separate from the DexScreener api_rate_budget) and is SHARED with live
# exit quoting. Overshoot it and (a) we 429-storm live's exits too, and (b) every 429 that
# reaches _qsim_tick as a no-route would book a FAKE rug. Tune QSIM_MAX_QUOTES_PER_MIN up via
# .env only after watching the logs stay 429-free. Backoff auto-protects if we overshoot.
QSIM_MAX_QUOTES_PER_MIN = int(os.getenv("QSIM_MAX_QUOTES_PER_MIN", "12"))
QSIM_TICK_SECS          = float(os.getenv("QSIM_TICK_SECS", "30"))   # per-position quote cadence
QSIM_LOOP_SECS          = float(os.getenv("QSIM_LOOP_SECS", "3"))    # monitor pass interval
QSIM_RUG_FAILS          = int(os.getenv("QSIM_RUG_FAILS", "6"))      # consecutive no-route quotes -> close as rug
QSIM_BACKOFF_SECS       = float(os.getenv("QSIM_BACKOFF_SECS", "30"))  # pause all quoting after a 429
QSIM_PEAK_PENDING_TTL_SECS = float(
    os.getenv(
        "QSIM_PEAK_PENDING_TTL_SECS",
        str(max(peak_guard.PENDING_TTL_SECS, QSIM_TICK_SECS * 2.5)),
    )
)
QSIM_NO_BOUNCE_STOP_ENABLED = os.getenv("QSIM_NO_BOUNCE_STOP_ENABLED", "true").lower() == "true"
NO_BOUNCE_ARM_MULT          = float(os.getenv("NO_BOUNCE_ARM_MULT", "1.3"))
NO_BOUNCE_STOP_MULT         = float(os.getenv("NO_BOUNCE_STOP_MULT", "0.9"))
QSIM_BANK_EXIT_ENABLED      = os.getenv("QSIM_BANK_EXIT_ENABLED", "false").lower() == "true"
QSIM_BANK_EXIT_MULT         = float(os.getenv("QSIM_BANK_EXIT_MULT", "1.3"))
QSIM_EXIT_OVERLAY_STRATEGY  = os.getenv("QSIM_EXIT_OVERLAY_STRATEGY", "").strip()
QSIM_MAX_ENTRY_EXEC_RATIO   = entry_quality.env_float(os.getenv("QSIM_MAX_ENTRY_EXEC_RATIO"), 0.0)
QSIM_ENTRY_ROUNDTRIP_MIN_MULT = entry_quality.env_float(os.getenv("QSIM_ENTRY_ROUNDTRIP_MIN_MULT"), 0.0)
QSIM_HARD_STOP_PCT          = entry_quality.env_float(os.getenv("QSIM_HARD_STOP_PCT"), 0.0)
QSIM_POST_EXIT_OBS_ENABLED  = os.getenv("QSIM_POST_EXIT_OBS_ENABLED", "false").lower() == "true"
QSIM_POST_EXIT_OBS_MINS     = float(os.getenv("QSIM_POST_EXIT_OBS_MINS", "90"))
QSIM_POST_EXIT_OBS_CADENCE_SECS = float(os.getenv("QSIM_POST_EXIT_OBS_CADENCE_SECS", "60"))
QSIM_POST_EXIT_OBS_LIMIT    = int(os.getenv("QSIM_POST_EXIT_OBS_LIMIT", "50"))
QSIM_POST_EXIT_OBS_MAX_PER_MIN = int(os.getenv("QSIM_POST_EXIT_OBS_MAX_PER_MIN", "3"))
# A close is STALE when the position went unobserved longer than this right before the quote
# that closed it. Such a row is not the exit the strategy would have taken — the thresholds were
# never evaluated during the gap — so it gets a 'stale_' exit_reason prefix and is excluded from
# honest PnL by construction. Default 6x the tick: normal jitter/backoff stays clean, a genuinely
# unwatched position does not. 0 disables the labelling (NOT recommended).
QSIM_STALE_DECISION_SECS = float(os.getenv("QSIM_STALE_DECISION_SECS", str(QSIM_TICK_SECS * 6)))
QSIM_RUNNER_WINDOW_ENABLED  = os.getenv("QSIM_RUNNER_WINDOW_ENABLED", "true").lower() == "true"
RUNNER_WINDOW_ARM_MULT      = float(os.getenv("RUNNER_WINDOW_ARM_MULT", "2.0"))
RUNNER_WINDOW_RELEASE_MULT  = float(os.getenv("RUNNER_WINDOW_RELEASE_MULT", "5.0"))
RUNNER_WINDOW_MINS          = float(os.getenv("RUNNER_WINDOW_MINS", "10"))
RUNNER_WINDOW_FLOOR_MULT    = float(os.getenv("RUNNER_WINDOW_FLOOR_MULT", "1.0"))
RUNNER_WINDOW_PROTECTED_REASONS = {"trail_stop", "profit_floor"}

_ensured = False
# monitor-process in-memory state
_last_quote_ts: dict[int, float] = {}     # call_id -> monotonic time of last sell-quote
_last_quote_wall: dict[int, datetime] = {}  # call_id -> WALL time of last sell-quote (staleness)
_last_post_exit_quote_ts: dict[int, float] = {}  # call_id -> monotonic time of last post-exit quote
_runner_window_until: dict[int, float] = {}  # call_id -> monotonic expiry for temporary loose-runner mode
_qsim_exit_state: dict[int, dict] = {}       # call_id -> named overlay state
_noroute_streak: dict[int, int] = {}      # call_id -> consecutive no-route sell quotes
_quote_window: list[float] = []           # monotonic timestamps of recent quotes (budget window)
_post_exit_quote_window: list[float] = [] # monotonic timestamps of recent post-exit quotes
_backoff_until: float = 0.0               # monotonic time until which quoting is paused (429 backoff)


def _qsim_exit_config(variant: str | None):
    cfg = _VARIANT_CONFIGS.get(variant or "early", EXIT_A_PAPER)
    if QSIM_HARD_STOP_PCT > 0:
        return replace(cfg, hard_stop_pct=QSIM_HARD_STOP_PCT)
    return cfg


def _qsim_hard_stop_pct(pos: dict, cfg) -> float:
    is_vip_gamble = (pos.get("vip_tier") in ("gamble", "gamble_risk"))
    return cfg.vip_gamble_hard_stop_pct if is_vip_gamble else cfg.hard_stop_pct


def _no_bounce_stop_result(current_mult: float, peak_mult: float) -> ExitResult:
    if (
        QSIM_NO_BOUNCE_STOP_ENABLED
        and NO_BOUNCE_ARM_MULT > 0
        and NO_BOUNCE_STOP_MULT > 0
        and peak_mult < NO_BOUNCE_ARM_MULT
        and current_mult <= NO_BOUNCE_STOP_MULT
    ):
        return ExitResult(True, "no_bounce_stop")
    return ExitResult(False)


def _bank_exit_result(current_mult: float, current_mcap: float) -> ExitResult:
    if (
        QSIM_BANK_EXIT_ENABLED
        and QSIM_BANK_EXIT_MULT > 0
        and current_mult >= QSIM_BANK_EXIT_MULT
    ):
        return ExitResult(True, f"bank_{QSIM_BANK_EXIT_MULT:g}x", exit_mcap=current_mcap)
    return ExitResult(False)


@dataclass(frozen=True)
class QsimExitOverlay:
    name: str
    kind: str
    bank_mult: float | None = None
    confirm_ticks: int = 1
    lock_trigger_mult: float | None = None
    lock_floor_mult: float | None = None


QSIM_EXIT_OVERLAYS: dict[str, QsimExitOverlay] = {
    "bank_1p2x": QsimExitOverlay("bank_1p2x", "bank", bank_mult=1.2),
    "bank_1p3x": QsimExitOverlay("bank_1p3x", "bank", bank_mult=1.3),
    "bank_1p4x": QsimExitOverlay("bank_1p4x", "bank", bank_mult=1.4),
    "bank_1p5x": QsimExitOverlay("bank_1p5x", "bank", bank_mult=1.5),
    "bank_1p75x": QsimExitOverlay("bank_1p75x", "bank", bank_mult=1.75),
    "bank_2x": QsimExitOverlay("bank_2x", "bank", bank_mult=2.0),
    "confirm_bank_1p2x": QsimExitOverlay("confirm_bank_1p2x", "bank", bank_mult=1.2, confirm_ticks=2),
    "confirm_bank_1p3x": QsimExitOverlay("confirm_bank_1p3x", "bank", bank_mult=1.3, confirm_ticks=2),
    "confirm_bank_1p4x": QsimExitOverlay("confirm_bank_1p4x", "bank", bank_mult=1.4, confirm_ticks=2),
    "confirm_bank_1p5x": QsimExitOverlay("confirm_bank_1p5x", "bank", bank_mult=1.5, confirm_ticks=2),
    "confirm_bank_1p75x": QsimExitOverlay("confirm_bank_1p75x", "bank", bank_mult=1.75, confirm_ticks=2),
    "confirm_bank_2x": QsimExitOverlay("confirm_bank_2x", "bank", bank_mult=2.0, confirm_ticks=2),
    "lock_or_bank_1p3x_1p1x": QsimExitOverlay("lock_or_bank_1p3x_1p1x", "lock_or_bank", bank_mult=1.3, lock_trigger_mult=1.3, lock_floor_mult=1.1),
    "lock_or_bank_1p4x_1p15x": QsimExitOverlay("lock_or_bank_1p4x_1p15x", "lock_or_bank", bank_mult=1.4, lock_trigger_mult=1.4, lock_floor_mult=1.15),
    "lock_or_bank_1p5x_1p2x": QsimExitOverlay("lock_or_bank_1p5x_1p2x", "lock_or_bank", bank_mult=1.5, lock_trigger_mult=1.5, lock_floor_mult=1.2),
    "lock_or_bank_1p75x_1p35x": QsimExitOverlay("lock_or_bank_1p75x_1p35x", "lock_or_bank", bank_mult=1.75, lock_trigger_mult=1.75, lock_floor_mult=1.35),
    "lock_or_bank_2x_1p55x": QsimExitOverlay("lock_or_bank_2x_1p55x", "lock_or_bank", bank_mult=2.0, lock_trigger_mult=2.0, lock_floor_mult=1.55),
}


def _resolve_qsim_exit_overlay() -> QsimExitOverlay | None:
    if QSIM_EXIT_OVERLAY_STRATEGY:
        overlay = QSIM_EXIT_OVERLAYS.get(QSIM_EXIT_OVERLAY_STRATEGY)
        if not overlay:
            print(f"[qsim] WARNING: unknown QSIM_EXIT_OVERLAY_STRATEGY={QSIM_EXIT_OVERLAY_STRATEGY!r}; overlay disabled")
        return overlay
    if QSIM_BANK_EXIT_ENABLED and QSIM_BANK_EXIT_MULT > 0:
        return QsimExitOverlay(
            name=f"bank_{QSIM_BANK_EXIT_MULT:g}x",
            kind="bank",
            bank_mult=QSIM_BANK_EXIT_MULT,
        )
    return None


_QSIM_EXIT_OVERLAY = _resolve_qsim_exit_overlay()


def _apply_qsim_exit_overlay(
    call_id: int,
    current_mult: float,
    current_mcap: float,
    overlay: QsimExitOverlay | None = None,
) -> tuple[ExitResult, bool, str | None]:
    overlay = overlay if overlay is not None else _QSIM_EXIT_OVERLAY
    if overlay is None:
        return ExitResult(False), False, None

    state = _qsim_exit_state.setdefault(call_id, {})
    if overlay.kind == "bank":
        threshold = overlay.bank_mult or 0.0
        if threshold <= 0:
            return ExitResult(False), False, None
        streak_key = f"{overlay.name}:streak"
        streak = int(state.get(streak_key, 0))
        if current_mult >= threshold:
            streak += 1
            state[streak_key] = streak
            note = f"{overlay.name}:streak={streak}/{max(1, overlay.confirm_ticks)}"
            if streak >= max(1, overlay.confirm_ticks):
                return ExitResult(True, overlay.name, exit_mcap=current_mcap), False, note
            return ExitResult(False), False, note
        if streak:
            state[streak_key] = 0
            return ExitResult(False), False, f"{overlay.name}:streak_reset"
        return ExitResult(False), False, None

    if overlay.kind == "lock_or_bank":
        trigger = overlay.lock_trigger_mult or overlay.bank_mult or 0.0
        floor = overlay.lock_floor_mult or 0.0
        if trigger <= 0 or floor <= 0:
            return ExitResult(False), False, None
        armed_key = f"{overlay.name}:armed"
        if current_mult >= trigger:
            state[armed_key] = True
            return ExitResult(False), True, f"{overlay.name}:armed"
        if state.get(armed_key) and current_mult <= floor:
            return ExitResult(True, overlay.name, exit_mcap=current_mcap), True, f"{overlay.name}:floor"
        if state.get(armed_key):
            return ExitResult(False), True, f"{overlay.name}:hold"
        return ExitResult(False), False, None

    return ExitResult(False), False, None


def _apply_runner_window(
    *,
    call_id: int,
    current_mult: float,
    peak_mult: float,
    result: ExitResult,
) -> tuple[ExitResult, str | None]:
    if (
        not QSIM_RUNNER_WINDOW_ENABLED
        or RUNNER_WINDOW_ARM_MULT <= 0
        or RUNNER_WINDOW_RELEASE_MULT <= RUNNER_WINDOW_ARM_MULT
        or RUNNER_WINDOW_MINS <= 0
        or RUNNER_WINDOW_FLOOR_MULT <= 0
    ):
        return result, None

    now = time.monotonic()
    armed = peak_mult >= RUNNER_WINDOW_ARM_MULT and peak_mult < RUNNER_WINDOW_RELEASE_MULT
    if armed and call_id not in _runner_window_until:
        _runner_window_until[call_id] = now + RUNNER_WINDOW_MINS * 60.0

    until = _runner_window_until.get(call_id)
    if until is None:
        return result, None

    if current_mult <= RUNNER_WINDOW_FLOOR_MULT:
        _runner_window_until.pop(call_id, None)
        return ExitResult(True, "runner_floor_stop"), "runner_window_floor"

    if peak_mult >= RUNNER_WINDOW_RELEASE_MULT or now >= until:
        _runner_window_until.pop(call_id, None)
        return result, "runner_window_released"

    if result.should_exit and result.reason in RUNNER_WINDOW_PROTECTED_REASONS:
        return ExitResult(False), f"runner_window_hold:{result.reason}"

    return result, "runner_window_active"


def _decision_gap_secs(pos: dict, prior_quote_at: datetime | None) -> float | None:
    """Seconds this position went UNOBSERVED before the quote now closing it.

    Measured from the previous sell-quote, or from entry_time when this is the first quote
    (a position quoted once after four hours was blind for four hours, not for one tick).
    Returns None only when neither timestamp is usable — never fabricate a clean gap.
    """
    ref = prior_quote_at or pos.get("entry_time")
    if ref is None:
        return None
    try:
        if ref.tzinfo is None:
            ref = ref.replace(tzinfo=timezone.utc)
        return max(0.0, (datetime.now(timezone.utc) - ref).total_seconds())
    except Exception:
        return None


def _stale_reason(reason: str, gap_secs: float | None) -> str:
    """Prefix an exit_reason with 'stale_' when the decision was made on a stale quote.

    The prefix (not just the numeric column) is deliberate: every report that does
    GROUP BY exit_reason separates these automatically, including ad-hoc SQL that knows
    nothing about decision_gap_secs. A column alone can be silently ignored; a different
    label cannot be.
    """
    if (
        QSIM_STALE_DECISION_SECS > 0
        and gap_secs is not None
        and gap_secs > QSIM_STALE_DECISION_SECS
        and not reason.startswith("stale_")
    ):
        return f"stale_{reason}"
    return reason


def _ensure_table() -> None:
    global _ensured
    if not _ensured:
        db.ensure_qsim_positions_table()
        _ensured = True


def qsim_resolve(channel: str | None, vip_tier: str | None, category: str | None,
                 now: datetime | None = None) -> dict:
    """Is this lane a qsim candidate, and does it trade on this weekday? Mirrors
    lane_policy's weekday gate + globally-skipped-day opt-in against QSIM_LANES."""
    ch   = (channel or "").lstrip("@").strip()
    tier = (vip_tier or "none").strip()
    cat  = (category or "none").strip()
    spec = QSIM_LANES.get((ch, tier, cat))
    if not spec:
        return {"trade": False}
    wd   = lane_policy.gate_weekday(now)
    days = spec.get("days")
    if lane_policy.is_skipped_day(now) and not (days and wd in days):
        return {"trade": False}
    if days and wd not in days:
        return {"trade": False}
    return {"trade": True, "size": spec.get("size"), "exit": spec.get("exit", "early")}


async def qsim_open(score_result: dict, token_data: dict) -> None:
    """Open a quote-priced sim position for a candidate-lane call. Never raises."""
    try:
        if not QSIM_ENABLED:
            return
        call_id = score_result.get("call_id")
        if not call_id:
            return
        channel = (token_data.get("channel_tag") or token_data.get("channel_handle") or "").lstrip("@")
        vip_tier = token_data.get("vip_tier")
        cat = db.get_call_skip_reason(call_id)
        spec = qsim_resolve(channel, vip_tier, cat)
        if not spec.get("trade"):
            return

        mint   = token_data.get("mint_address")
        symbol = token_data.get("symbol", "?")
        if not mint:
            return
        # First-call-per-mint dedup (mirror live): skip if a qsim position for this mint is open.
        if any(p.get("mint_address") == mint for p in db.get_open_qsim_positions()):
            return

        _ensure_table()
        size = float(spec["size"])
        try:
            tokens_raw = await jupiter.get_buy_quote(mint, size, raise_on_ratelimit=True)
        except jupiter.RateLimitError:
            # Throttle, not a dead token — just drop this sample (a missed open, never a rug).
            print(f"[qsim] {symbol} open skipped — jupiter 429 (rate-limited) call_id={call_id}")
            return
        if not tokens_raw or tokens_raw <= 0:
            print(f"[qsim] {symbol} skipped — no buy route call_id={call_id}")
            return
        _s, decimals = db.get_token_supply_and_decimals(mint)
        entry_mcap = live_trader._effective_fill_mcap(mint, size, tokens_raw, decimals)
        if not entry_mcap or entry_mcap <= 0:
            print(f"[qsim] {symbol} skipped — entry mcap calc failed call_id={call_id}")
            return
        gate_token_data = dict(token_data or {})
        if not gate_token_data.get("mcap_at_call"):
            stored_mcap = db.get_call_mcap_at_call(call_id)
            if stored_mcap:
                gate_token_data["mcap_at_call"] = stored_mcap
        gate = entry_quality.check_entry_exec_ratio(
            max_ratio=QSIM_MAX_ENTRY_EXEC_RATIO,
            executable_mcap=entry_mcap,
            token_data=gate_token_data,
        )
        if gate.enabled and not gate.allowed:
            print(
                f"[qsim] {symbol} skipped — entry exec/ref ratio "
                f"{(gate.ratio or 0):.2f}x > {gate.max_ratio:.2f}x "
                f"exec=${entry_mcap/1000:.1f}k "
                f"ref=${(gate.reference_mcap or 0)/1000:.1f}k "
                f"source={gate.reference_source or '?'} call_id={call_id}"
            )
            return
        if QSIM_ENTRY_ROUNDTRIP_MIN_MULT > 0:
            try:
                roundtrip_sol = await jupiter.get_sell_quote(mint, tokens_raw, raise_on_ratelimit=True)
            except jupiter.RateLimitError:
                print(f"[qsim] {symbol} open skipped — roundtrip sell quote 429 call_id={call_id}")
                return
            roundtrip = entry_quality.check_roundtrip(
                min_mult=QSIM_ENTRY_ROUNDTRIP_MIN_MULT,
                sol_in=size,
                sol_out=roundtrip_sol,
            )
            if not roundtrip.allowed:
                print(
                    f"[qsim] {symbol} skipped — entry roundtrip "
                    f"{(roundtrip.mult or 0):.2f}x < {roundtrip.min_mult:.2f}x "
                    f"sol_in={size:.4f} sol_out={(roundtrip.sol_out or 0):.4f} "
                    f"reason={roundtrip.reason} call_id={call_id}"
                )
                return

        opened = db.open_qsim_position(
            call_id=call_id, entry_price=entry_mcap, entry_tokens=tokens_raw,
            entry_decimals=(int(decimals) if decimals is not None else None),
            sol_in=size, lane=cat, variant=spec["exit"], vip_tier=vip_tier,
            channel_handle=channel,
        )
        if opened:
            print(f"[qsim] OPEN {symbol} call_id={call_id} entry=${entry_mcap/1000:.1f}k "
                  f"size={size} variant={spec['exit']}")
    except Exception as e:
        print(f"[qsim] open error call_id={score_result.get('call_id')}: {e}")


def _budget_ok() -> bool:
    """True if we're under the per-minute quote cap. Evicts stamps older than 60s."""
    now = time.monotonic()
    while _quote_window and now - _quote_window[0] > 60.0:
        _quote_window.pop(0)
    return len(_quote_window) < QSIM_MAX_QUOTES_PER_MIN


def _post_exit_budget_ok() -> bool:
    """True if post-exit research probes still have their small reserved budget."""
    if QSIM_POST_EXIT_OBS_MAX_PER_MIN <= 0:
        return False
    now = time.monotonic()
    while _post_exit_quote_window and now - _post_exit_quote_window[0] > 60.0:
        _post_exit_quote_window.pop(0)
    return len(_post_exit_quote_window) < QSIM_POST_EXIT_OBS_MAX_PER_MIN and _budget_ok()


async def _qsim_tick(pos: dict) -> None:
    """Price one open qsim position off a real sell-quote and apply the exit logic."""
    call_id = pos["call_id"]
    mint    = pos["mint_address"]
    symbol  = pos.get("symbol", "?")
    sol_in  = float(pos["sol_in"])
    entry   = float(pos["entry_price"])
    tokens  = int(float(pos["entry_tokens"]))
    if sol_in <= 0 or entry <= 0 or tokens <= 0:
        return

    # Captured BEFORE this tick stamps its own time: how long the position has been blind.
    prior_quote_at = _last_quote_wall.get(call_id)

    try:
        sol_out = await jupiter.get_sell_quote(mint, tokens, raise_on_ratelimit=True)
    except jupiter.RateLimitError:
        # 429 = throttle, NOT a rug. Back off ALL quoting for a bit and skip this tick; leave
        # the rug streak untouched so a rate-limit can never be booked as a fake -100% close.
        global _backoff_until
        _backoff_until = time.monotonic() + QSIM_BACKOFF_SECS
        _quote_window.append(time.monotonic())
        _last_quote_ts[call_id] = time.monotonic()
        db.insert_qsim_quote_observation(
            call_id=call_id,
            rate_limited=True,
            note="sell quote rate-limited",
        )
        return
    _quote_window.append(time.monotonic())
    _last_quote_ts[call_id] = time.monotonic()
    _last_quote_wall[call_id] = datetime.now(timezone.utc)
    gap_secs = _decision_gap_secs(pos, prior_quote_at)

    if sol_out is None or sol_out <= 0:
        # No sell route = the bag isn't sellable (rug/illiquid). After a streak, realize it at 0.
        _noroute_streak[call_id] = _noroute_streak.get(call_id, 0) + 1
        db.insert_qsim_quote_observation(
            call_id=call_id,
            no_route=True,
            noroute_streak=_noroute_streak[call_id],
            note="sell quote no-route",
        )
        if _noroute_streak[call_id] >= QSIM_RUG_FAILS:
            reason = _stale_reason("rug", gap_secs)
            db.close_qsim_position(call_id, exit_price=0.0, sol_out=0.0, exit_reason=reason,
                                   decision_gap_secs=gap_secs)
            peak_guard.clear(f"qsim:{call_id}")
            _cleanup(call_id)
            print(f"[qsim] CLOSE {symbol} call_id={call_id} {reason} "
                  f"(no sell route x{QSIM_RUG_FAILS}) pnl={-sol_in:+.4f} gap={gap_secs or 0:.0f}s")
        return
    _noroute_streak.pop(call_id, None)

    real_mult   = sol_out / sol_in
    synth_cur   = entry * real_mult
    cfg = _qsim_exit_config(pos.get("variant") or "early")
    raw_hard_stop_pct = _qsim_hard_stop_pct(pos, cfg)
    raw_hard_stop = raw_hard_stop_pct > 0 and real_mult <= (1.0 - raw_hard_stop_pct)
    if raw_hard_stop:
        db.insert_qsim_quote_observation(
            call_id=call_id,
            sol_out=sol_out,
            real_mult=real_mult,
            synth_mcap=synth_cur,
            eff_mcap=synth_cur,
            exit_reason="hard_stop",
            should_exit=True,
            noroute_streak=0,
            note="raw_exec_hard_stop",
        )
        reason = _stale_reason("hard_stop", gap_secs)
        db.close_qsim_position(call_id, exit_price=synth_cur, sol_out=sol_out,
                               exit_reason=reason, decision_gap_secs=gap_secs)
        peak_guard.clear(f"qsim:{call_id}")
        peak_guard.clear(f"qsimT:{call_id}")
        _cleanup(call_id)
        print(f"[qsim] CLOSE {symbol} call_id={call_id} {reason} "
              f"pnl={sol_out - sol_in:+.4f} ({real_mult:.2f}x) gap={gap_secs or 0:.0f}s")
        return

    prior_peak  = float(pos.get("peak_mcap") or 0.0)
    real_peak   = peak_guard.guard_peak(
        f"qsim:{call_id}",
        synth_cur,
        prior_peak,
        pending_ttl_secs=QSIM_PEAK_PENDING_TTL_SECS,
    )
    if real_peak > prior_peak:
        db.update_qsim_peak(call_id, real_peak, real_peak / entry)
    eff_cur = min(synth_cur, real_peak) if real_peak > 0 else synth_cur
    eff_cur = peak_guard.guard_trough(f"qsimT:{call_id}", eff_cur)
    if eff_cur <= 0:
        return

    is_vip_gamble = (pos.get("vip_tier") in ("gamble", "gamble_risk"))
    channel_handle = (pos.get("channel_handle") or "").lstrip("@")
    result = apply_exit_config(
        cfg, current_mcap=eff_cur, peak_mcap=real_peak, entry_mcap=entry,
        is_vip_gamble=is_vip_gamble, channel_handle=channel_handle,
        entry_time=pos.get("entry_time"),
    )
    no_bounce_result = _no_bounce_stop_result(
        current_mult=eff_cur / entry,
        peak_mult=(real_peak / entry) if real_peak > 0 else real_mult,
    )
    if no_bounce_result.should_exit and not result.should_exit:
        result = ExitResult(True, no_bounce_result.reason, exit_mcap=eff_cur)
    overlay_result, overlay_suppresses_base, overlay_note = _apply_qsim_exit_overlay(
        call_id=call_id,
        current_mult=real_mult,
        current_mcap=synth_cur,
    )
    if overlay_result.should_exit:
        result = overlay_result
    elif overlay_suppresses_base and result.should_exit and result.reason != "time_stop":
        overlay_note = f"{overlay_note};hold:{result.reason}" if overlay_note else f"hold:{result.reason}"
        result = ExitResult(False)

    bank_result = _bank_exit_result(real_mult, synth_cur) if _QSIM_EXIT_OVERLAY is None else ExitResult(False)
    if bank_result.should_exit:
        result = bank_result
    result, runner_note = _apply_runner_window(
        call_id=call_id,
        current_mult=eff_cur / entry,
        peak_mult=(real_peak / entry) if real_peak > 0 else real_mult,
        result=result,
    )
    db.insert_qsim_quote_observation(
        call_id=call_id,
        sol_out=sol_out,
        real_mult=real_mult,
        synth_mcap=synth_cur,
        eff_mcap=eff_cur,
        prior_peak_mcap=prior_peak,
        real_peak_mcap=real_peak,
        peak_mult=(real_peak / entry) if entry else None,
        exit_reason=result.reason,
        should_exit=result.should_exit,
        noroute_streak=0,
        note=";".join(note for note in (overlay_note, runner_note) if note),
    )
    if result.should_exit:
        reason = _stale_reason(result.reason, gap_secs)
        db.close_qsim_position(call_id, exit_price=(result.exit_mcap or eff_cur),
                               sol_out=sol_out, exit_reason=reason,
                               decision_gap_secs=gap_secs)
        peak_guard.clear(f"qsim:{call_id}")
        peak_guard.clear(f"qsimT:{call_id}")
        _cleanup(call_id)
        print(f"[qsim] CLOSE {symbol} call_id={call_id} {reason} "
              f"pnl={sol_out - sol_in:+.4f} ({real_mult:.2f}x) gap={gap_secs or 0:.0f}s")


async def _qsim_post_exit_tick(pos: dict) -> None:
    """Research-only sell quote after qsim has closed, so hold-longer tests have real data."""
    call_id = pos["call_id"]
    mint    = pos["mint_address"]
    symbol  = pos.get("symbol", "?")
    sol_in  = float(pos["sol_in"])
    entry   = float(pos["entry_price"])
    tokens  = int(float(pos["entry_tokens"]))
    if sol_in <= 0 or entry <= 0 or tokens <= 0:
        return

    try:
        sol_out = await jupiter.get_sell_quote(mint, tokens, raise_on_ratelimit=True)
    except jupiter.RateLimitError:
        global _backoff_until
        _backoff_until = time.monotonic() + QSIM_BACKOFF_SECS
        _quote_window.append(time.monotonic())
        _post_exit_quote_window.append(time.monotonic())
        _last_post_exit_quote_ts[call_id] = time.monotonic()
        db.insert_qsim_quote_observation(
            call_id=call_id,
            rate_limited=True,
            note="post_exit_probe rate-limited",
        )
        return
    _quote_window.append(time.monotonic())
    _post_exit_quote_window.append(time.monotonic())
    _last_post_exit_quote_ts[call_id] = time.monotonic()

    if sol_out is None or sol_out <= 0:
        db.insert_qsim_quote_observation(
            call_id=call_id,
            no_route=True,
            note="post_exit_probe no-route",
        )
        return

    real_mult = sol_out / sol_in
    synth_cur = entry * real_mult
    db.insert_qsim_quote_observation(
        call_id=call_id,
        sol_out=sol_out,
        real_mult=real_mult,
        synth_mcap=synth_cur,
        eff_mcap=synth_cur,
        peak_mult=real_mult,
        should_exit=False,
        note="post_exit_probe",
    )
    print(f"[qsim] POST-EXIT QUOTE {symbol} call_id={call_id} {real_mult:.2f}x")


def _cleanup(call_id: int) -> None:
    _last_quote_ts.pop(call_id, None)
    _last_quote_wall.pop(call_id, None)
    _noroute_streak.pop(call_id, None)
    _runner_window_until.pop(call_id, None)
    _qsim_exit_state.pop(call_id, None)


async def run_qsim_monitor() -> None:
    """Monitor loop (own process). Sell-quotes open qsim positions on a budget-capped
    cadence and applies real exit logic. Never executes anything."""
    _ensure_table()
    print(f"[qsim] monitor started — lanes={list(QSIM_LANES)} "
          f"cap={QSIM_MAX_QUOTES_PER_MIN}/min cadence={QSIM_TICK_SECS}s enabled={QSIM_ENABLED}")
    print(f"[qsim] post-exit probes enabled={QSIM_POST_EXIT_OBS_ENABLED} "
          f"mins={QSIM_POST_EXIT_OBS_MINS:g} cadence={QSIM_POST_EXIT_OBS_CADENCE_SECS:g}s "
          f"limit={QSIM_POST_EXIT_OBS_LIMIT} cap={QSIM_POST_EXIT_OBS_MAX_PER_MIN}/min")
    print(f"[qsim] bank exit enabled={QSIM_BANK_EXIT_ENABLED} mult={QSIM_BANK_EXIT_MULT:g}x")
    print(f"[qsim] exit overlay: {_QSIM_EXIT_OVERLAY.name if _QSIM_EXIT_OVERLAY else 'none'}")
    print(f"[qsim] stale-decision threshold: {QSIM_STALE_DECISION_SECS:g}s "
          f"(closes decided on an older quote are labelled stale_*)"
          if QSIM_STALE_DECISION_SECS > 0 else
          "[qsim] stale-decision labelling: DISABLED")
    print(f"[qsim] max entry exec/ref ratio: {QSIM_MAX_ENTRY_EXEC_RATIO:g}x"
          if QSIM_MAX_ENTRY_EXEC_RATIO > 0 else "[qsim] max entry exec/ref ratio: disabled")
    print(f"[qsim] entry roundtrip min: {QSIM_ENTRY_ROUNDTRIP_MIN_MULT:g}x"
          if QSIM_ENTRY_ROUNDTRIP_MIN_MULT > 0 else "[qsim] entry roundtrip min: disabled")
    base_cfg = _VARIANT_CONFIGS.get("early", EXIT_A_PAPER)
    print(f"[qsim] hard_stop override: -{QSIM_HARD_STOP_PCT * 100:.0f}% "
          f"(was -{base_cfg.hard_stop_pct * 100:.0f}%)"
          if QSIM_HARD_STOP_PCT > 0 else
          f"[qsim] hard_stop override: disabled (using -{base_cfg.hard_stop_pct * 100:.0f}%)")
    while True:
        try:
            # Skip the whole quoting pass while in 429 backoff (the loop-end sleep still runs).
            if QSIM_ENABLED and time.monotonic() >= _backoff_until:
                now = time.monotonic()
                positions = db.get_open_qsim_positions()
                for pos in positions:
                    cid = pos["call_id"]
                    # per-position cadence
                    if now - _last_quote_ts.get(cid, 0.0) < QSIM_TICK_SECS:
                        continue
                    if not _budget_ok():
                        break   # over budget this minute — the rest wait for the next pass
                    await _qsim_tick(pos)
                    if time.monotonic() < _backoff_until:
                        break   # a 429 mid-pass tripped backoff — stop quoting immediately
                if QSIM_POST_EXIT_OBS_ENABLED and time.monotonic() >= _backoff_until:
                    closed_positions = db.get_recent_closed_qsim_positions_for_post_exit(
                        QSIM_POST_EXIT_OBS_MINS,
                        QSIM_POST_EXIT_OBS_LIMIT,
                    )
                    for pos in closed_positions:
                        cid = pos["call_id"]
                        if now - _last_post_exit_quote_ts.get(cid, 0.0) < QSIM_POST_EXIT_OBS_CADENCE_SECS:
                            continue
                        if not _post_exit_budget_ok():
                            break
                        await _qsim_post_exit_tick(pos)
                        if time.monotonic() < _backoff_until:
                            break
        except Exception as e:
            db.safe_rollback()
            print(f"[qsim] monitor pass error: {e}")
        await asyncio.sleep(QSIM_LOOP_SECS)


def main() -> None:
    asyncio.run(run_qsim_monitor())


if __name__ == "__main__":
    main()
