"""
alert_bot.py — Telegram alert sender for high-conviction token signals.

Sends formatted HTML messages to a configured Telegram chat when the scorer
returns 'alert' or 'strong_alert', and for 5x+ milestone updates from
lagging channels.

Public API
----------
await send_alert(score_result: dict, token_data: dict) -> bool
await send_milestone(call_id, symbol, stated_multiplier, mcap_at_call, current_mcap) -> bool

Both functions return True on success, False on any error, and never raise —
so a broken alert never crashes the listener.

Environment
-----------
TELEGRAM_BOT_TOKEN       — BotFather token
TELEGRAM_ALERT_CHAT_ID   — destination chat/user ID
"""

import asyncio
import html
import os
import re

from dotenv import load_dotenv
from telegram import Bot
from telegram.constants import ParseMode
from telegram.request import HTTPXRequest

load_dotenv()

# ── Bot singleton ─────────────────────────────────────────────────────────────

_bot: Bot | None = None


def _get_bot() -> Bot:
    global _bot
    if _bot is None:
        request = HTTPXRequest(connection_pool_size=8)
        _bot = Bot(token=os.environ["TELEGRAM_BOT_TOKEN"], request=request)
    return _bot


def _chat_id() -> str:
    return os.environ["TELEGRAM_ALERT_CHAT_ID"]


# ── Formatters ────────────────────────────────────────────────────────────────

def _fmt_mcap(n) -> str:
    """Format a market cap number as $23.4k / $1.2M / $450."""
    if n is None:
        return "n/a"
    n = float(n)
    if n >= 1_000_000:
        return f"${n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"${n / 1_000:.1f}k"
    return f"${n:.0f}"


def _fmt_reason(r: str) -> str:
    """Insert a space before the trailing score delta: 'age=8m+15' → 'age=8m +15'."""
    return re.sub(r'([+-])(\d+)$', r' \1\2', r)


def _fmt_security(flag: str | None) -> str:
    if flag == "safe":
        return "✅ Safe"
    if flag == "warning":
        return "⚠️ Warning"
    return "❓ Unknown"


# ── Message builders ──────────────────────────────────────────────────────────

def _build_alert(score_result: dict, token_data: dict) -> str:
    label   = score_result["label"]
    score   = score_result["score"]
    reasons = score_result.get("reasons", [])

    symbol = html.escape(str(token_data.get("symbol") or "?"))
    mint   = html.escape(str(token_data.get("mint_address") or ""))
    age    = token_data.get("token_age_minutes")
    sec    = _fmt_security(token_data.get("security_flag"))
    mcap   = _fmt_mcap(token_data.get("mcap_at_call"))

    header  = "🚨 <b>STRONG ALERT</b> 🚨" if label == "strong_alert" else "⚡ <b>ALERT</b>"
    age_str = f"{age}m" if age is not None else "n/a"

    reason_lines = (
        "\n".join(f"• {_fmt_reason(r)}" for r in reasons)
        if reasons else "• (no reasons)"
    )

    dex_url   = f"https://dexscreener.com/solana/{mint}"
    axiom_url = f"https://axiom.trade/t/{mint}"

    msg_id  = token_data.get("source_message_id")
    handle  = token_data.get("channel_handle")
    origin_line = (
        f'🔍 <a href="https://t.me/{handle}/{msg_id}">View Original Signal</a>'
        if msg_id and handle else None
    )

    return "\n".join([
        header,
        f"💎 <b>${symbol}</b>",
        f"⛓ <code>{mint}</code>",
        "",
        f"📊 Score: {score}/100",
        f"🏦 Entry MCap: {mcap}",
        f"⏱ Age: {age_str}",
        f"🔒 Security: {sec}",
        "",
        "📈 <b>Reasons:</b>",
        reason_lines,
        "",
        *([origin_line, ""] if origin_line else []),
        f'🔗 <a href="{dex_url}">DexScreener</a>',
        f'🔗 <a href="{axiom_url}">Axiom</a>',
    ])


def _build_milestone(
    symbol: str,
    stated_multiplier: float,
    mcap_at_call,
    current_mcap,
    mint_address: str | None = None,
) -> str:
    sym  = html.escape(str(symbol or "?"))
    mult = f"{stated_multiplier:.0f}x"
    mint = html.escape(str(mint_address or ""))

    lines = [
        "📢 <b>CONFIRMED RUNNER</b>",
        f"💎 <b>${sym}</b> hit {mult} from {_fmt_mcap(mcap_at_call)} entry",
        f"Current MCap: {_fmt_mcap(current_mcap)}",
    ]

    if mint:
        dex_url   = f"https://dexscreener.com/solana/{mint}"
        axiom_url = f"https://axiom.trade/t/{mint}"
        lines += [
            f"⛓ <code>{mint}</code>",
            "",
            f'🔗 <a href="{dex_url}">DexScreener</a>  🔗 <a href="{axiom_url}">Axiom</a>',
        ]

    return "\n".join(lines)


def _build_monitor_milestone(
    symbol: str,
    mint_address: str,
    multiplier: float,
    mcap_at_call,
    current_mcap,
    source_message_id=None,
    channel_handle: str = None,
) -> str:
    sym  = html.escape(str(symbol or "?"))
    mint = html.escape(str(mint_address or ""))
    mult_str  = f"{int(multiplier)}x"
    dex_url   = f"https://dexscreener.com/solana/{mint}"
    axiom_url = f"https://axiom.trade/t/{mint}"
    origin_line = (
        f'🔍 <a href="https://t.me/{channel_handle}/{source_message_id}">View Original Signal</a>'
        if source_message_id and channel_handle else None
    )
    return "\n".join([
        f"🚀 <b>${sym} hit {mult_str}!</b>",
        f"Entry: {_fmt_mcap(mcap_at_call)} → Now: {_fmt_mcap(current_mcap)}",
        f"⛓ <code>{mint}</code>",
        "",
        *([origin_line, ""] if origin_line else []),
        f'🔗 <a href="{dex_url}">DexScreener</a>  🔗 <a href="{axiom_url}">Axiom</a>',
    ])


def _build_drawdown_alert(
    symbol: str,
    mint_address: str,
    peak_mult: float,
    current_mult: float,
    drawdown_pct: float,
    entry_mult: float,
    severe: bool,
) -> str:
    sym     = html.escape(str(symbol or "?"))
    mint    = html.escape(str(mint_address or ""))
    header  = f"🚨 <b>${sym} dumping</b>" if severe else f"⚠️ <b>${sym} pulling back</b>"
    dex_url = f"https://dexscreener.com/solana/{mint}"
    return "\n".join([
        header,
        f"Peak: {peak_mult:.1f}x → Now: {current_mult:.1f}x (-{drawdown_pct:.0f}% from peak)",
        f"Still up {entry_mult:.1f}x from entry",
        f'🔗 <a href="{dex_url}">DexScreener</a>',
    ])


# ── Public API ────────────────────────────────────────────────────────────────

async def send_alert(score_result: dict, token_data: dict) -> bool:
    """
    Send a conviction alert to the configured Telegram chat.

    score_result — dict from scorer.score_call(): {call_id, score, label, reasons, path}
    token_data   — {symbol, mint_address, mcap_at_call, security_flag, token_age_minutes}

    Returns True on success, False on failure. Never raises.
    """
    try:
        text = _build_alert(score_result, token_data)
        await _get_bot().send_message(
            chat_id=_chat_id(),
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        print(
            f"[alert_bot] sent  symbol={token_data.get('symbol', '?')}"
            f"  score={score_result.get('score')}  label={score_result.get('label')}"
        )
        try:
            import paper_trader
            asyncio.create_task(paper_trader.open_position(score_result, token_data))
        except Exception as pe:
            print(f"[paper_trader] open_position failed: {pe}")
        # Live trading integration
        try:
            import live_trader
            asyncio.create_task(
                live_trader.open_live_position(
                    score_result, token_data
                )
            )
        except Exception as le:
            print(f"[live_trader] open failed: {le}")
        return True
    except Exception as e:
        print(f"[alert_bot] failed: {e}")
        return False


async def send_milestone(
    call_id: int,
    symbol: str,
    stated_multiplier: float,
    mcap_at_call,
    current_mcap,
    mint_address: str | None = None,
) -> bool:
    """
    Send a 5x+ milestone notification to the configured Telegram chat.
    Returns True on success, False on failure. Never raises.
    """
    try:
        text = _build_milestone(symbol, stated_multiplier, mcap_at_call, current_mcap, mint_address)
        await _get_bot().send_message(
            chat_id=_chat_id(),
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        print(
            f"[alert_bot] milestone sent"
            f"  call_id={call_id}  symbol={symbol}  {stated_multiplier:.0f}x"
        )
        return True
    except Exception as e:
        print(f"[alert_bot] failed: {e}")
        return False


async def send_monitor_milestone(
    call_id: int,
    symbol: str,
    mint_address: str,
    multiplier: float,
    mcap_at_call,
    current_mcap,
    source_message_id=None,
    channel_handle: str = None,
) -> bool:
    """
    Send a live monitor milestone alert (2x / 5x / 10x threshold hit).
    multiplier should be a round threshold value (2.0, 5.0, 10.0).
    Returns True on success, False on failure. Never raises.
    """
    try:
        text = _build_monitor_milestone(
            symbol, mint_address, multiplier, mcap_at_call, current_mcap,
            source_message_id=source_message_id, channel_handle=channel_handle,
        )
        await _get_bot().send_message(
            chat_id=_chat_id(),
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        print(
            f"[alert_bot] monitor milestone sent"
            f"  call_id={call_id}  symbol={symbol}  {int(multiplier)}x"
        )
        return True
    except Exception as e:
        print(f"[alert_bot] failed: {e}")
        return False


async def send_drawdown_alert(
    call_id: int,
    symbol: str,
    mint_address: str,
    peak_mult: float,
    current_mult: float,
    drawdown_pct: float,
    entry_mult: float,
    severe: bool = False,
) -> bool:
    """
    Send a drawdown alert — ⚠️ pulling back (30%+) or 🚨 dumping (50%+).
    entry_mult is current_mult at alert time (how much token is still up from entry).
    Returns True on success, False on failure. Never raises.
    """
    try:
        text = _build_drawdown_alert(
            symbol, mint_address, peak_mult, current_mult, drawdown_pct, entry_mult, severe
        )
        await _get_bot().send_message(
            chat_id=_chat_id(),
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        kind = "dump" if severe else "drawdown"
        print(
            f"[alert_bot] {kind} alert sent"
            f"  call_id={call_id}  symbol={symbol}  -{drawdown_pct:.0f}%"
        )
        return True
    except Exception as e:
        print(f"[alert_bot] failed: {e}")
        return False


# ── Live trading message builders ─────────────────────────────────────────────

def _build_live_buy(
    symbol: str,
    mint: str,
    sol_spent: float,
    tokens_received: int,
    signature: str,
) -> str:
    sym     = html.escape(str(symbol or "?"))
    mint_e  = html.escape(str(mint or ""))
    sol_url = f"https://solscan.io/tx/{html.escape(signature)}"
    return "\n".join([
        "🟢 <b>LIVE BUY EXECUTED</b>",
        f"💎 <b>${sym}</b>",
        f"⛓ <code>{mint_e}</code>",
        "",
        f"💰 SOL spent: <b>{sol_spent:.4f} SOL</b>",
        f"🪙 Tokens received: {tokens_received:,.2f}" if isinstance(tokens_received, float) else f"🪙 Tokens received: {tokens_received:,}",
        "",
        f'🔗 <a href="{sol_url}">View Transaction</a>',
    ])


def _build_live_sell(
    symbol: str,
    mint: str,
    sol_received: float,
    pnl: float,
    exit_reason: str,
    signature: str,
) -> str:
    sym     = html.escape(str(symbol or "?"))
    mint_e  = html.escape(str(mint or ""))
    sol_url = f"https://solscan.io/tx/{html.escape(signature)}"
    pnl_str = f"{pnl:+.4f} SOL"
    icon    = "📈" if pnl >= 0 else "📉"
    return "\n".join([
        "🔴 <b>LIVE SELL EXECUTED</b>",
        f"💎 <b>${sym}</b>  ({html.escape(exit_reason)})",
        f"⛓ <code>{mint_e}</code>",
        "",
        f"💰 SOL received: <b>{sol_received:.4f} SOL</b>",
        f"{icon} P&L: <b>{pnl_str}</b>",
        "",
        f'🔗 <a href="{sol_url}">View Transaction</a>',
    ])


def _build_live_sell_failed(symbol: str, mint: str) -> str:
    sym    = html.escape(str(symbol or "?"))
    mint_e = html.escape(str(mint or ""))
    return "\n".join([
        "🚨 <b>LIVE SELL FAILED — MANUAL INTERVENTION REQUIRED</b>",
        f"💎 <b>${sym}</b>",
        f"⛓ <code>{mint_e}</code>",
        "",
        "The sell transaction could not be submitted to Jupiter.",
        "Check wallet and close position manually.",
    ])


def _build_slippage_skip(
    symbol: str,
    mint: str,
    score: int,
    label: str,
    msg_mcap: float,
    actual_mcap: float,
    slippage_pct: float,
) -> str:
    sym       = html.escape(str(symbol or "?"))
    mint_e    = html.escape(str(mint or ""))
    dex_url   = f"https://dexscreener.com/solana/{mint_e}"
    axiom_url = f"https://axiom.trade/t/{mint_e}"
    header    = "🚨 <b>SKIP — HIGH CONVICTION</b>" if label == "strong_alert" else "⚡ <b>SKIP — ALERT</b>"
    return "\n".join([
        header,
        f"💎 <b>${sym}</b>",
        f"⛓ <code>{mint_e}</code>",
        "",
        f"📊 Score: {score}/100",
        f"📈 Slippage: +{slippage_pct:.0f}%",
        f"🏦 Message MCap: ${msg_mcap/1000:.1f}k",
        f"💰 Actual MCap: ${actual_mcap/1000:.1f}k",
        "",
        "⏭ Trade skipped — price moved too much.",
        "Manual entry may still be viable.",
        "",
        f'🔗 <a href="{dex_url}">DexScreener</a>  🔗 <a href="{axiom_url}">Axiom</a>',
    ])


# ── Live trading public API ───────────────────────────────────────────────────

async def send_live_buy_alert(
    symbol: str,
    mint: str,
    sol_spent: float,
    tokens_received: int,
    signature: str,
) -> bool:
    """Send a live buy confirmation alert. Never raises."""
    try:
        text = _build_live_buy(symbol, mint, sol_spent, tokens_received, signature)
        await _get_bot().send_message(
            chat_id=_chat_id(),
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        print(f"[alert_bot] live buy alert sent  symbol={symbol}  sol={sol_spent:.4f}")
        return True
    except Exception as e:
        print(f"[alert_bot] live buy alert failed: {e}")
        return False


async def send_live_sell_alert(
    symbol: str,
    mint: str,
    sol_received: float,
    pnl: float,
    exit_reason: str,
    signature: str,
) -> bool:
    """Send a live sell confirmation alert. Never raises."""
    try:
        text = _build_live_sell(symbol, mint, sol_received, pnl, exit_reason, signature)
        await _get_bot().send_message(
            chat_id=_chat_id(),
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        print(
            f"[alert_bot] live sell alert sent  symbol={symbol}"
            f"  sol={sol_received:.4f}  pnl={pnl:+.4f}"
        )
        return True
    except Exception as e:
        print(f"[alert_bot] live sell alert failed: {e}")
        return False


async def send_live_sell_failed_alert(symbol: str, mint: str) -> bool:
    """Send a sell-failed alert requiring manual intervention. Never raises."""
    try:
        text = _build_live_sell_failed(symbol, mint)
        await _get_bot().send_message(
            chat_id=_chat_id(),
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        print(f"[alert_bot] live sell FAILED alert sent  symbol={symbol}")
        return True
    except Exception as e:
        print(f"[alert_bot] live sell failed alert send error: {e}")
        return False


async def send_slippage_skip_alert(
    symbol: str,
    mint: str,
    score: int,
    label: str,
    msg_mcap: float,
    actual_mcap: float,
    slippage_pct: float,
) -> bool:
    """Send alert when a high-conviction signal is skipped due to slippage. Never raises."""
    try:
        text = _build_slippage_skip(symbol, mint, score, label, msg_mcap, actual_mcap, slippage_pct)
        await _get_bot().send_message(
            chat_id=_chat_id(),
            text=text,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        print(f"[alert_bot] slippage skip alert sent  symbol={symbol}  slippage={slippage_pct:.0f}%")
        return True
    except Exception as e:
        print(f"[alert_bot] slippage skip alert failed: {e}")
        return False


async def send_system_alert(message: str) -> None:
    """Send system status alert to Telegram. Never raises."""
    try:
        await _get_bot().send_message(
            chat_id=_chat_id(),
            text=f"🤖 <b>SYSTEM ALERT</b>\n\n{message}",
            parse_mode=ParseMode.HTML,
        )
        print("[alert_bot] system alert sent")
    except Exception as e:
        print(f"[alert_bot] system alert failed: {e}")
