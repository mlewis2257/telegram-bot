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

import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))

import db
import jupiter
import alert_bot
import data_fetcher
import wallet as _wallet
from paper_trader import (
    ExitResult,
    TAKE_PROFIT_5X,
    TAKE_PROFIT_3X,
    TRAIL_PEAK_MIN,
    HARD_STOP_PCT,
    MAX_HOURS,
)

# ── Circuit breaker state ──────────────────────────────────────────────────────

_STATE_DIR         = Path(os.path.dirname(__file__)) / ".last_run"
_CIRCUIT_FLAG_FILE = _STATE_DIR / "circuit_breaker.flag"

_circuit_broken: bool = _CIRCUIT_FLAG_FILE.exists()

if _circuit_broken:
    print(f"[live] STARTUP: circuit breaker flag found — all live trading halted")
    print(f"[live] To re-enable: delete {_CIRCUIT_FLAG_FILE} and restart")


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

    # ── Guard 1: kill switch ───────────────────────────────────────────────────
    if not _is_enabled():
        print(f"[live] {symbol} skipped — LIVE_TRADING_ENABLED is not 'true'")
        return False

    # ── Guard 2: circuit breaker ───────────────────────────────────────────────
    if _circuit_broken:
        print(f"[live] {symbol} skipped — circuit breaker is active")
        return False

    # ── Guard 3: position count cap ────────────────────────────────────────────
    open_count = db.get_live_positions_count()
    if open_count >= _max_positions():
        print(
            f"[live] {symbol} skipped — "
            f"max open positions ({_max_positions()}) reached ({open_count} open)"
        )
        return False

    # ── Guard 4: daily loss circuit check ──────────────────────────────────────
    today_losses = db.get_today_live_losses()
    if today_losses > _max_daily_loss():
        await _trip_circuit_breaker(today_losses)
        return False

    # ── Guard 5: duplicate position ────────────────────────────────────────────
    if not call_id or not mint:
        print(f"[live] {symbol} skipped — missing call_id or mint_address")
        return False

    if db.get_open_live_position(call_id):
        print(f"[live] {symbol} skipped — live position already open for call_id={call_id}")
        return False

    if db.has_open_live_position_for_mint(mint):
        print(f"[live] {symbol} skipped — live position already open for mint={mint[:12]}...")
        return False

    # ── Guard 6: SOL balance ───────────────────────────────────────────────────
    size = _position_size(label)
    try:
        balance = _wallet.get_sol_balance(_rpc_url())
        if balance < size + 0.05:
            print(
                f"[live] {symbol} skipped — SOL balance {balance:.4f}"
                f" < required {size + 0.05:.4f} (size={size:.4f} + 0.05 reserve)"
            )
            return False
    except Exception as e:
        print(f"[live] {symbol} skipped — balance check failed: {e}")
        return False

    # ── Slippage check (before spending SOL) ──────────────────────────────────
    msg_mcap     = float(token_data.get("mcap_at_call") or 0)
    actual_entry = None
    if mint and not mint.startswith(("INFERRED:", "UNKNOWN:")):
        try:
            market = data_fetcher.fetch_token_price(mint)
            if market and market.get("mcap"):
                actual_entry = float(market["mcap"])
        except Exception as e:
            print(f"[live] price fetch failed for {symbol}: {e}")
    if actual_entry is None and mint:
        print(f"[live] {symbol} DexScreener returned no mcap — using msg price ${msg_mcap/1000:.1f}k")

    max_slippage = float(os.getenv("MAX_ENTRY_SLIPPAGE_PCT", "50"))
    if actual_entry and msg_mcap > 0:
        slippage = ((actual_entry - msg_mcap) / msg_mcap) * 100
        print(f"[live] {symbol} entry slippage: msg=${msg_mcap/1000:.1f}k actual=${actual_entry/1000:.1f}k ({slippage:+.1f}%)")
        if slippage > max_slippage:
            print(f"[live] {symbol} SKIPPED — slippage {slippage:.0f}% exceeds max {max_slippage:.0f}%")
            return False
        if slippage < -30:
            print(f"[live] {symbol} SKIPPED — price dropped {slippage:.0f}% since message (dump)")
            return False

    # ── Execute buy ────────────────────────────────────────────────────────────
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
    await alert_bot.send_live_buy_alert(
        symbol=symbol,
        mint=mint,
        sol_spent=sol_spent,
        tokens_received=tokens_display,
        signature=sig,
    )
    return True


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

    # ── Verify on-chain balance before selling ─────────────────────────────────
    wallet_addr = _wallet.get_public_key()
    balance, _decimals = await jupiter.get_token_balance(mint, wallet_addr, _rpc_url())
    print(f"[live_sell] on-chain balance for {symbol} call_id={call_id}: {balance}")

    if balance == 0:
        print(
            f"[live] ⚠️ balance=0 for {symbol} call_id={call_id}"
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
        f"  balance={balance}  reason={exit_reason}"
    )
    result = await jupiter.sell_token(mint, balance)
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
) -> ExitResult:
    """
    Identical exit logic to paper_trader.check_exits().
    Uses get_open_live_position() so it only fires for live positions.
    Synchronous — same pattern as paper_trader.check_exits().
    """
    position = db.get_open_live_position(call_id)
    if not position:
        return ExitResult(False)

    if entry_mcap <= 0:
        return ExitResult(False)

    current_mult = current_mcap / entry_mcap

    if current_mult >= 10.0:
        return ExitResult(True, "10x_tp")

    if current_mult >= TAKE_PROFIT_5X:
        return ExitResult(True, "5x_tp")

    if current_mult >= TAKE_PROFIT_3X:
        return ExitResult(True, "3x_tp")

    if peak_mcap > 0:
        peak_mult = peak_mcap / entry_mcap
        if peak_mult >= TRAIL_PEAK_MIN:
            if peak_mult >= 5.0:
                trail_pct = 0.20
            elif peak_mult >= 3.0:
                trail_pct = 0.25
            else:
                trail_pct = 0.30
            if peak_mult >= 2.0:
                trail_pct -= 0.05
            drawdown = (peak_mcap - current_mcap) / peak_mcap
            if drawdown >= trail_pct:
                return ExitResult(True, "trail_stop")

    if current_mult <= (1.0 - HARD_STOP_PCT):
        return ExitResult(True, "hard_stop")

    entry_time = position.get("entry_time")
    if entry_time:
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=timezone.utc)
        age_hours = (datetime.now(timezone.utc) - entry_time).total_seconds() / 3600
        if age_hours > MAX_HOURS:
            return ExitResult(True, "time_stop")

    return ExitResult(False)


def get_live_pnl_summary() -> dict:
    """Aggregate P&L stats for all closed live positions."""
    return db.get_live_pnl_summary()
