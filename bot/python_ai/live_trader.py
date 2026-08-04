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
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(__file__))

import db
import jupiter
import alert_bot
import data_fetcher
import peak_guard
import wallet as _wallet
import lane_policy
from paper_trader import (
    TAKE_PROFIT_5X,
    TRAIL_PEAK_MIN,
    HARD_STOP_PCT,
    MAX_HOURS,
)
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


# ── Per-channel mcap entry limits ──────────────────────────────────────────────

MCAP_LIMITS = {
    'solhousesignal_vip': 200_000,
    'solwhaletrending':   100_000,
    'solearlytrending':    75_000,
    'solhousesignal':     175_000,  # matches paper A upper bound; strategy engine enforces 20k min
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
_SELL_QUOTE_TTL   = float(os.getenv("LIVE_SELL_QUOTE_TTL", "2.5"))  # seconds
_sell_quote_cache: dict = {}  # mint -> (sol_out, monotonic_ts)
# Exit quotes are the highest-value quote we make (real money on the line), so a
# transient Jupiter 429 is retried briefly before we surrender to the feed basis —
# the feed is exactly the liar the quote path exists to bypass (measured: ~2/3 of live
# exits were deciding on feed because a raw 429 dropped straight to fallback). A genuine
# no-route (None, no 429) is NOT retried: the coin is unsellable this tick, so retrying
# won't help and the caller must fall back. Bounded so a sell never waits > retries*ms.
_EXIT_QUOTE_RETRIES  = int(os.getenv("LIVE_EXIT_QUOTE_RETRIES", "2"))       # extra tries after the first
_EXIT_QUOTE_RETRY_MS = float(os.getenv("LIVE_EXIT_QUOTE_RETRY_MS", "180"))  # backoff between tries (ms)


def _rpc_url() -> str:
    return os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")


# ── Circuit breaker ────────────────────────────────────────────────────────────

async def _trip_circuit_breaker(today_losses: float) -> None:
    global _circuit_broken
    _circuit_broken = True
    _STATE_DIR.mkdir(exist_ok=True)
    _CIRCUIT_FLAG_FILE.write_text(
        f"tripped={datetime.now(timezone.utc).isoformat()}\n"
        f"today_losses={today_losses:.4f} SOL\n"
        f"limit={_max_daily_loss():.4f} SOL\n"
    )
    print(
        f"[live] ⛔ CIRCUIT BREAKER TRIPPED"
        f"  today_losses={today_losses:.4f} SOL  limit={_max_daily_loss():.4f} SOL"
    )
    try:
        msg = (
            f"🛑 <b>LIVE TRADING HALTED — circuit breaker triggered</b>\n"
            f"Today's losses: {today_losses:.3f} SOL\n"
            f"Limit:          {_max_daily_loss():.3f} SOL\n\n"
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

        # ── Guard 4: daily loss circuit check ──────────────────────────────────
        today_losses = db.get_today_live_losses()
        if today_losses > _max_daily_loss():
            await _trip_circuit_breaker(today_losses)
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
        free_uses_custom_filters = (channel_handle == "solhousesignal")
        vip_uses_lane_allowlist  = (channel_handle == "solhousesignal_vip")
        if (
            local_hour in QUIET_HOURS_PST
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
        if security_flag == "warning":
            print(f"[live] {symbol} skipped — security=warning")
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
        if actual_entry and actual_entry > max_mcap:
            print(f"[live] {symbol} skipped — mcap ${actual_entry/1000:.0f}k too high for {channel_handle or 'unknown'} (max ${max_mcap/1000:.0f}k)")
            db.set_call_skip_reason(call_id, "mcap_too_high")
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
        if not mint or not tokens_raw or not sol_amount or sol_amount <= 0:
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
            return None
        tokens_whole = tokens_raw / (10 ** (decimals if decimals is not None else 6))
        if tokens_whole <= 0 or supply_whole <= 0:
            return None
        return (sol_amount * sol_usd) * supply_whole / tokens_whole
    except Exception as e:
        print(f"[live] effective mcap calc failed: {e}")
        return None


async def _exit_sell_quote(mint: str, tokens_held: int) -> float | None:
    """
    Sell-quote for the live EXIT path, hardened against transient Jupiter 429s.
    Opts into RateLimitError (raise_on_ratelimit=True) so a rate-limit is distinguished
    from a genuine no-route: a 429 is retried up to _EXIT_QUOTE_RETRIES times with a
    short backoff (the quote usually clears within a few hundred ms), while a no-route
    (None with no 429) returns immediately — the bag is unsellable this tick and the
    caller falls back to feed. Never raises; never blocks a sell beyond retries*retry_ms.
    """
    attempts = _EXIT_QUOTE_RETRIES + 1
    for i in range(attempts):
        try:
            out = await jupiter.get_sell_quote(mint, tokens_held, raise_on_ratelimit=True)
            if out is None:
                return None  # genuine no-route — retrying won't conjure liquidity
            return out
        except jupiter.RateLimitError:
            if i < attempts - 1:
                await asyncio.sleep(_EXIT_QUOTE_RETRY_MS / 1000.0)
                continue
            print(f"[live] exit quote 429 after {attempts} tries for {mint[:8]} — feed fallback")
            return None
        except Exception as e:
            print(f"[live] exit quote error for {mint[:8]}: {e} — feed fallback")
            return None
    return None


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
            sol_out = await _exit_sell_quote(mint, tokens_held)
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
) -> tuple[float, float, float, str]:
    """
    Return the (current, peak, entry) triple to hand check_live_exits, plus a basis tag.

    Phase 2: when LIVE_EXIT_USE_QUOTE is on AND we have a real fill anchor + a live
    sell-quote, the triple is on the REAL (wallet) basis:
        current = entry_price_fill * (quote_sol_out / sol_in)   (synthetic real mcap)
        entry   = entry_price_fill                              (the real fill)
        peak    = ratcheted real peak (guard-corroborated, DB-shared across processes)
    so every exit ratio (drawdown-from-peak, multiple-from-entry) reflects what the bag
    is really worth, not the laggy feed. On ANY failure (quote unavailable, no recorded
    fill, flag off) it returns the FEED triple unchanged — a bad quote must never force
    or block a sell. Returns (current, peak, entry, "real"|"feed").
    """
    if not LIVE_EXIT_USE_QUOTE:
        return feed_current, feed_peak, feed_entry, "feed"
    try:
        real_entry = float(pos.get("entry_price_fill") or 0)
        if real_entry <= 0:
            return feed_current, feed_peak, feed_entry, "feed"  # pre-instrumentation position
        eff = await live_effective_current(pos)
        if not eff:
            return feed_current, feed_peak, feed_entry, "feed"  # quote failed → feed fallback
        synth_current, _real_mult = eff
        if synth_current <= 0:
            return feed_current, feed_peak, feed_entry, "feed"
        # Ratchet the real peak off observed sell-quote value. Seed from the DB row so the
        # peak is shared across sol-monitor + sol-ws-monitor and survives restarts; the
        # guard adds the same single-tick corroboration used on the feed side.
        # Seed the real-peak floor from the fill: a coin that dips right after entry (first
        # successful quote below the fill) must not leave real_peak reading absurdly below
        # entry — trail/floor arm off peak/entry, so a sub-entry peak understates every ratio
        # (this is what left `buy`'s real_peak at 3711 under an 3838 fill).
        prior_peak = max(float(db.get_live_real_peak(call_id) or 0.0), real_entry)
        real_peak  = peak_guard.guard_peak(f"realL:{call_id}", synth_current, prior_peak)
        if real_peak > prior_peak:
            db.update_live_real_peak(call_id, real_peak)
        eff_current = min(synth_current, real_peak) if real_peak > 0 else synth_current
        return eff_current, real_peak, real_entry, "real"
    except Exception as e:
        print(f"[live] exit-basis calc failed, using feed: {e}")
        return feed_current, feed_peak, feed_entry, "feed"


async def close_live_position(
    call_id: int,
    current_mcap: float,
    exit_reason: str,
) -> bool:
    """
    Verify on-chain token balance, execute sell, record the close.
    Never raises.
    """
    pos = db.get_open_live_position(call_id)
    if not pos:
        return False

    mint    = pos.get("mint_address")
    symbol  = pos.get("symbol", "?")
    sol_in  = float(pos["sol_in"])

    if not mint:
        print(f"[live] close skipped call_id={call_id} — no mint in position")
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
            except Exception as e:
                print(f"[live] balance=0 alert failed: {e}")
            return False

    # ── Execute sell ───────────────────────────────────────────────────────────
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
        await alert_bot.send_live_sell_failed_alert(symbol=symbol, mint=mint)
        return False

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
        except Exception as e:
            print(f"[live] sol_received=0 alert failed: {e}")
        return False

    pnl = sol_received - sol_in

    db.close_live_position_db(
        call_id=call_id,
        exit_price=current_mcap,
        sol_out=sol_received,
        exit_reason=exit_reason,
        tx_signature=sig,
    )
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
    await alert_bot.send_live_sell_alert(
        symbol=symbol,
        mint=mint,
        sol_received=sol_received,
        pnl=pnl,
        exit_reason=exit_reason,
        signature=sig,
    )
    return True


def check_live_exits(
    call_id: int,
    current_mcap: float,
    peak_mcap: float,
    entry_mcap: float,
    exit_config: ExitConfig = None,
) -> ExitResult:
    """
    Check whether the open live position for call_id should be exited.

    Uses get_open_live_position() so it only fires for real positions.
    Synchronous — same pattern as paper_trader.check_exits().

    exit_config defaults to the module-level _LIVE_EXIT_CONFIG which is
    loaded from the EXIT_STRATEGY env var at startup.
    """
    position = db.get_open_live_position(call_id)
    if not position:
        return ExitResult(False)

    if entry_mcap <= 0:
        return ExitResult(False)

    # Never act on a 0/null current_mcap (dead/unavailable feed). On LIVE this would fire
    # an UNNECESSARY real sell of a possibly-healthy position — the swap executes at the
    # real market price, so a feed glitch dumps a winner (opportunity cost + fees), not a
    # fake -100%. A genuine decline still produces a real reading and exits normally. Mirrors
    # the paper_trader guard; see PASSION 2026-06-30 (was +36% at last tick, 0-marked).
    if current_mcap <= 0:
        return ExitResult(False)

    # Low-side corroboration: hold a single uncorroborated crater for one reading so one
    # phantom low tick can't trigger a real stop-sell (mirror of guard_peak on the high
    # side; the high side is already guarded in ws_monitor).
    current_mcap = peak_guard.guard_trough(f"tL:{call_id}", current_mcap)
    if current_mcap <= 0:
        return ExitResult(False)

    cfg = exit_config if exit_config is not None else _LIVE_EXIT_CONFIG
    is_vip_gamble = position.get("vip_tier") in ("gamble_risk", "gamble")
    channel_handle = (position.get("channel_handle") or "").lstrip("@")
    entry_time = position.get("entry_time")

    return apply_exit_config(
        cfg,
        current_mcap=current_mcap,
        peak_mcap=peak_mcap,
        entry_mcap=entry_mcap,
        is_vip_gamble=is_vip_gamble,
        channel_handle=channel_handle,
        entry_time=entry_time,
    )


def get_live_pnl_summary() -> dict:
    """Aggregate P&L stats for all closed live positions."""
    return db.get_live_pnl_summary()
