"""
daily_summary.py — Send an end-of-day report to Telegram.

Cron: 55 23 * * * cd /root/telegram-bot/bot/python_ai && python3 daily_summary.py

Usage:
    python3 daily_summary.py           # sends summary for today
    python3 daily_summary.py --dry-run # prints to stdout, no Telegram
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv

load_dotenv()

import asyncio
from datetime import datetime, timezone

import db
import alert_bot


def _fmt_pnl(sol: float) -> str:
    return f"{sol:+.3f} SOL"


def _fmt_pct(pct: float) -> str:
    return f"{pct:+.0f}%"


def _win_rate(winners: int, total: int) -> str:
    if total == 0:
        return "n/a"
    return f"{winners}/{total} ({winners / total * 100:.0f}%)"


def _build_message() -> str:
    today = datetime.now(timezone.utc).strftime("%b %-d, %Y")

    signals  = db.get_daily_signal_summary()
    paper    = db.get_daily_paper_summary()
    live     = db.get_daily_live_summary()
    totals   = db.get_running_totals()

    # ── Signal overview ───────────────────────────────────────────────────────
    label_order = ["strong_alert", "alert", "caution", "watch", "skip"]
    counts      = {r["label"]: r["count"] for r in signals}
    total_sigs  = sum(counts.values())

    sig_lines = [f"  {lbl}: {counts[lbl]}" for lbl in label_order if lbl in counts]
    sig_block = "\n".join([
        f"📡 <b>Signals:</b> {total_sigs} total",
        *sig_lines,
    ])

    # ── Paper trading ─────────────────────────────────────────────────────────
    paper_lines = [
        "📝 <b>Paper Trading</b>",
        f"  Opened: {paper['opened']} | Closed: {paper['closed']}",
    ]
    if paper["closed"] > 0:
        paper_lines.append(
            f"  Winners: {_win_rate(paper['winners'], paper['closed'])}"
        )
        paper_lines.append(f"  Daily P&L: {_fmt_pnl(paper['daily_pnl'])}")
        if paper["best_symbol"] and paper["best_pct"] is not None:
            paper_lines.append(f"  🏆 Best:  ${paper['best_symbol']} {_fmt_pct(paper['best_pct'])}")
        if paper["worst_symbol"] and paper["worst_pct"] is not None:
            paper_lines.append(f"  💀 Worst: ${paper['worst_symbol']} {_fmt_pct(paper['worst_pct'])}")
    paper_block = "\n".join(paper_lines)

    # ── Live trading ──────────────────────────────────────────────────────────
    if live["opened"] > 0 or live["closed"] > 0:
        live_lines = [
            "💰 <b>Live Trading</b>",
            f"  Opened: {live['opened']} | Closed: {live['closed']}",
        ]
        if live["closed"] > 0:
            live_lines.append(
                f"  Winners: {_win_rate(live['winners'], live['closed'])}"
            )
            live_lines.append(f"  Daily P&L: {_fmt_pnl(live['daily_pnl'])}")
            if live["best_symbol"] and live["best_pct"] is not None:
                live_lines.append(f"  🏆 Best:  ${live['best_symbol']} {_fmt_pct(live['best_pct'])}")
            if live["worst_symbol"] and live["worst_pct"] is not None:
                live_lines.append(f"  💀 Worst: ${live['worst_symbol']} {_fmt_pct(live['worst_pct'])}")
        live_block = "\n".join(live_lines)
    else:
        live_block = "💰 <b>Live Trading</b>\n  Live trading disabled"

    # ── Running totals ────────────────────────────────────────────────────────
    totals_block = "\n".join([
        "📈 <b>Running Totals</b>",
        f"  Paper: {totals['paper_trades']} trades | {totals['paper_win_rate']}% win | {_fmt_pnl(totals['paper_pnl'])}",
        f"  Live:  {totals['live_trades']} trades | {totals['live_win_rate']}% win | {_fmt_pnl(totals['live_pnl'])}",
    ])

    sep = "─" * 21
    return "\n\n".join([
        f"📊 <b>DAILY SUMMARY — {today}</b>",
        sep,
        sig_block,
        paper_block,
        live_block,
        totals_block,
    ])


async def build_and_send(dry_run: bool = False) -> None:
    try:
        msg = _build_message()
    except Exception as e:
        print(f"[daily_summary] failed to build message: {e}")
        import traceback
        traceback.print_exc()
        return

    if dry_run:
        print(msg)
        return

    try:
        await alert_bot._get_bot().send_message(
            chat_id=alert_bot._chat_id(),
            text=msg,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        print("[daily_summary] sent")
    except Exception as e:
        print(f"[daily_summary] failed to send: {e}")


def main() -> None:
    dry_run = "--dry-run" in sys.argv
    asyncio.run(build_and_send(dry_run=dry_run))


if __name__ == "__main__":
    main()
