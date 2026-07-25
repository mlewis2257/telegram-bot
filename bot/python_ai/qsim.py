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
import os
import time
from datetime import datetime, timezone

import db
import jupiter
import peak_guard
import lane_policy
import live_trader                       # reuse the PURE _effective_fill_mcap only
from exit_config import EXIT_A_PAPER, EXIT_RIDE, apply_exit_config

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
    ("solhousesignal_vip", "gamble", "vip_mcap_gate"): {"days": {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}, "size": 0.05, "exit": "early"},
    # Widen 2026-07-24: the faithfully-testable POSITIVE lanes off the clean 14d shadow window
    # (post-07-11 fixes, so no phantom/June contamination). Both are the "early" variant —
    # early maps to a real ExitConfig (EXIT_A_PAPER); ride_vol positives are HELD because qsim
    # maps ride_vol->plain EXIT_RIDE (no order_flow), which is negative on those lanes = a lie.
    #   * solwhaletrending/none/none early (+10.06/14d): cleanest row on the board — 10/13 days
    #     green, reds are rounding errors, NOT spike-carried. The real widen candidate.
    #   * solhousesignal/none/low_score early (+30.05/14d): the ADVERSARIAL referee — reprice
    #     already convicted this anchor (booked +12.9, honest -8.4). If qsim lands near -8 it's a
    #     second independent confirmation qsim catches the entry-inflation lie (front-loaded on
    #     07-12 +17.7, then 5 straight red days — expect qsim to gut it).
    ("solwhaletrending", "none", "none"):      {"days": {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}, "size": 0.05, "exit": "early"},
    ("solhousesignal",   "none", "low_score"): {"days": {"Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"}, "size": 0.05, "exit": "early"},
}

# variant string -> the SAME ExitConfig shadow/paper use (keep in sync with shadow_monitor).
_VARIANT_CONFIGS = {"early": EXIT_A_PAPER, "ride": EXIT_RIDE, "ride_vol": EXIT_RIDE}

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

_ensured = False
# monitor-process in-memory state
_last_quote_ts: dict[int, float] = {}     # call_id -> monotonic time of last sell-quote
_noroute_streak: dict[int, int] = {}      # call_id -> consecutive no-route sell quotes
_quote_window: list[float] = []           # monotonic timestamps of recent quotes (budget window)
_backoff_until: float = 0.0               # monotonic time until which quoting is paused (429 backoff)


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

    try:
        sol_out = await jupiter.get_sell_quote(mint, tokens, raise_on_ratelimit=True)
    except jupiter.RateLimitError:
        # 429 = throttle, NOT a rug. Back off ALL quoting for a bit and skip this tick; leave
        # the rug streak untouched so a rate-limit can never be booked as a fake -100% close.
        global _backoff_until
        _backoff_until = time.monotonic() + QSIM_BACKOFF_SECS
        _quote_window.append(time.monotonic())
        _last_quote_ts[call_id] = time.monotonic()
        return
    _quote_window.append(time.monotonic())
    _last_quote_ts[call_id] = time.monotonic()

    if sol_out is None or sol_out <= 0:
        # No sell route = the bag isn't sellable (rug/illiquid). After a streak, realize it at 0.
        _noroute_streak[call_id] = _noroute_streak.get(call_id, 0) + 1
        if _noroute_streak[call_id] >= QSIM_RUG_FAILS:
            db.close_qsim_position(call_id, exit_price=0.0, sol_out=0.0, exit_reason="rug")
            peak_guard.clear(f"qsim:{call_id}")
            _cleanup(call_id)
            print(f"[qsim] CLOSE {symbol} call_id={call_id} rug (no sell route x{QSIM_RUG_FAILS}) pnl={-sol_in:+.4f}")
        return
    _noroute_streak.pop(call_id, None)

    real_mult   = sol_out / sol_in
    synth_cur   = entry * real_mult
    prior_peak  = float(pos.get("peak_mcap") or 0.0)
    real_peak   = peak_guard.guard_peak(f"qsim:{call_id}", synth_cur, prior_peak)
    if real_peak > prior_peak:
        db.update_qsim_peak(call_id, real_peak, real_peak / entry)
    eff_cur = min(synth_cur, real_peak) if real_peak > 0 else synth_cur
    eff_cur = peak_guard.guard_trough(f"qsimT:{call_id}", eff_cur)
    if eff_cur <= 0:
        return

    cfg = _VARIANT_CONFIGS.get(pos.get("variant") or "early", EXIT_A_PAPER)
    is_vip_gamble = (pos.get("vip_tier") in ("gamble", "gamble_risk"))
    channel_handle = (pos.get("channel_handle") or "").lstrip("@")
    result = apply_exit_config(
        cfg, current_mcap=eff_cur, peak_mcap=real_peak, entry_mcap=entry,
        is_vip_gamble=is_vip_gamble, channel_handle=channel_handle,
        entry_time=pos.get("entry_time"),
    )
    if result.should_exit:
        db.close_qsim_position(call_id, exit_price=(result.exit_mcap or eff_cur),
                               sol_out=sol_out, exit_reason=result.reason)
        peak_guard.clear(f"qsim:{call_id}")
        peak_guard.clear(f"qsimT:{call_id}")
        _cleanup(call_id)
        print(f"[qsim] CLOSE {symbol} call_id={call_id} {result.reason} "
              f"pnl={sol_out - sol_in:+.4f} ({real_mult:.2f}x)")


def _cleanup(call_id: int) -> None:
    _last_quote_ts.pop(call_id, None)
    _noroute_streak.pop(call_id, None)


async def run_qsim_monitor() -> None:
    """Monitor loop (own process). Sell-quotes open qsim positions on a budget-capped
    cadence and applies real exit logic. Never executes anything."""
    _ensure_table()
    print(f"[qsim] monitor started — lanes={list(QSIM_LANES)} "
          f"cap={QSIM_MAX_QUOTES_PER_MIN}/min cadence={QSIM_TICK_SECS}s enabled={QSIM_ENABLED}")
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
        except Exception as e:
            db.safe_rollback()
            print(f"[qsim] monitor pass error: {e}")
        await asyncio.sleep(QSIM_LOOP_SECS)


def main() -> None:
    asyncio.run(run_qsim_monitor())


if __name__ == "__main__":
    main()
