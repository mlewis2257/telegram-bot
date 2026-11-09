"""
api_alerts.py — Telegram alert when a paid API (Anthropic, xAI) runs out of credit, or is about to.

WHY (2026-11-08). The Anthropic balance nearly ran out unnoticed. When it does, trader_brain and
grok_x only log failures, and the LIVE brain lane fails closed — it quietly stops buying. Nothing
said so. This sends the alert bot's chat a message instead.

Three alerts:
  billing       the API refused for money/key reasons (HTTP 401/402/403, or "credit balance",
                "billing", "insufficient", "spending limit" in an HTTP error) — out of credit.
  failing       FAIL_STREAK failures in a row of any kind (overloaded, network, bad answers).
  credit low    estimated spend since you last read the balance has used 75% / 90% of it.
                There is no balance API on a normal key, so you tell it the balance:
                  ANTHROPIC_CREDIT_USD=<balance shown in console.anthropic.com>
                  ANTHROPIC_CREDIT_SINCE=<when you read it, e.g. 2026-11-08 20:00>
                  XAI_CREDIT_USD / XAI_CREDIT_SINCE  (same, console.x.ai)
                Update both after every top-up. Counts only this bot's own use of the key.

Each alert repeats at most once per API_ALERT_THROTTLE_S (default 3 h) per process. Never raises.
Uses TELEGRAM_BOT_TOKEN / TELEGRAM_ALERT_CHAT_ID (the alert bot's settings).
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

THROTTLE_S = float(os.getenv("API_ALERT_THROTTLE_S", "10800"))
FAIL_STREAK = int(os.getenv("API_ALERT_FAIL_STREAK", "10"))
CREDIT_LEVELS = (0.90, 0.75)
_BILLING_WORDS = ("credit balance", "billing", "insufficient", "spending limit", "payment required",
                  "exceeded your", "out of credits")
_last: dict[str, float] = {}


def is_billing_error(err: str | None) -> bool:
    """True for an HTTP error that means no money / no access. Only 'http NNN: ...' strings count,
    so a model answer that happens to mention 'credit' never trips it."""
    low = (err or "").lower()
    if not low.startswith("http "):
        return False
    return low.startswith(("http 401", "http 402", "http 403")) or any(w in low for w in _BILLING_WORDS)


def send(key: str, text: str) -> bool:
    """Telegram the alert chat. The same key at most once per THROTTLE_S. Never raises."""
    now = time.monotonic()
    if key in _last and now - _last[key] < THROTTLE_S:
        return False
    _last[key] = now
    print(f"[api_alerts] ALERT {key}: {text}", flush=True)
    token, chat = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_ALERT_CHAT_ID")
    if not token or not chat:
        print("[api_alerts] TELEGRAM_BOT_TOKEN / TELEGRAM_ALERT_CHAT_ID not set — alert only logged", flush=True)
        return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{token}/sendMessage",
                          json={"chat_id": chat, "text": "🤖 SYSTEM ALERT\n\n" + text}, timeout=10)
        if r.status_code != 200:
            print(f"[api_alerts] telegram http {r.status_code}", flush=True)
        return r.status_code == 200
    except Exception as e:
        # the exception text can contain the URL (with the bot token) — log the type only
        print(f"[api_alerts] telegram send failed: {type(e).__name__}", flush=True)
        return False


class FailStreak:
    """Counts failures in a row; alerts once the streak reaches FAIL_STREAK."""

    def __init__(self, service: str):
        self.service, self.n = service, 0

    def ok(self) -> None:
        self.n = 0

    def fail(self, err: str | None) -> None:
        self.n += 1
        if self.n >= FAIL_STREAK:
            send(f"{self.service}_failing",
                 f"{self.service}: {self.n} failures in a row. Last: {(err or '?')[:200]}")


def credit_settings(prefix: str) -> tuple[float, datetime] | None:
    """(balance USD, read at) from <prefix>_CREDIT_USD / <prefix>_CREDIT_SINCE, or None if unset/bad."""
    raw_usd, raw_since = os.getenv(f"{prefix}_CREDIT_USD", "").strip(), os.getenv(f"{prefix}_CREDIT_SINCE", "").strip()
    if not raw_usd or not raw_since:
        return None
    try:
        usd = float(raw_usd)
        since = datetime.fromisoformat(raw_since)
    except ValueError:
        print(f"[api_alerts] {prefix}_CREDIT_USD / {prefix}_CREDIT_SINCE unreadable "
              f"({raw_usd!r}, {raw_since!r}) — credit warning off", flush=True)
        return None
    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    return (usd, since) if usd > 0 else None


def credit_check(service: str, prefix: str, spent_usd: float, balance_usd: float) -> str:
    """Alert at 75% / 90% of the balance used. Returns a short status for the log."""
    used = spent_usd / balance_usd
    for lv in CREDIT_LEVELS:
        if used >= lv:
            send(f"{service}_credit_{lv}",
                 f"{service}: ~${spent_usd:.2f} of the ${balance_usd:.2f} {prefix} balance used "
                 f"({used * 100:.0f}%, estimated). Top up, then update {prefix}_CREDIT_USD and "
                 f"{prefix}_CREDIT_SINCE in .env.")
            break
    return f"credit ~${spent_usd:.2f}/${balance_usd:.2f} ({used * 100:.0f}%)"
