"""
rpc_pool.py — rotate Solana RPC calls across multiple endpoints / API keys.

Reads one or more RPC URLs from the environment and hands them out round-robin,
skipping any endpoint that recently 429'd / hit its quota (a short cooldown). This
spreads per-second load and fails over automatically when one key is throttled.

IMPORTANT — what this does and does NOT buy you:
  • More keys only raise your MONTHLY ceiling if they're on SEPARATE Helius
    accounts. The `-32429 "max usage reached"` error is a per-ACCOUNT credit cap,
    so two keys under one account draw from the same bucket and rotation won't add
    headroom. Rotation always helps with per-second rate limits + automatic failover.

Env (any/all are read, deduped, in this priority order):
    SOLANA_RPC_URLS      comma / space / newline-separated full URLs   (preferred)
    SOLANA_RPC_URL       single URL (back-compat — still works as-is)
    SOLANA_RPC_URL_2..8  numbered extras
    HELIUS_API_KEYS      comma-separated keys → mainnet.helius-rpc.com URLs
    RPC_KEY_COOLDOWN_S   seconds to skip an endpoint after it 429s (default 60)
"""
from __future__ import annotations

import os
import re
import threading
import time

_COOLDOWN_S = float(os.getenv("RPC_KEY_COOLDOWN_S", "60"))
_SPLIT = re.compile(r"[,\s]+")


def _canon(u: str) -> str:
    """Normalize to an https:// key so ws:// and https:// forms share one cooldown."""
    return u.replace("wss://", "https://").replace("ws://", "http://").strip()


def _load() -> list[str]:
    raw: list[str] = []
    multi = os.getenv("SOLANA_RPC_URLS", "")
    if multi:
        raw += [x for x in _SPLIT.split(multi) if x]
    single = os.getenv("SOLANA_RPC_URL", "")
    if single:
        raw.append(single)
    for i in range(2, 9):
        u = os.getenv(f"SOLANA_RPC_URL_{i}", "")
        if u:
            raw.append(u)
    keys = os.getenv("HELIUS_API_KEYS", "")
    if keys:
        for k in _SPLIT.split(keys):
            if k:
                raw.append(f"https://mainnet.helius-rpc.com/?api-key={k}")
    seen: set[str] = set()
    out: list[str] = []
    for u in raw:
        c = _canon(u)
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


_URLS = _load()
_lock = threading.Lock()
_idx = -1
_cooldown: dict[str, float] = {}


def count() -> int:
    """Number of distinct RPC endpoints configured."""
    return len(_URLS)


def endpoints() -> list[str]:
    return list(_URLS)


def http_url() -> str:
    """Next https RPC URL, round-robin, skipping any endpoint in cooldown.

    Returns "" if none configured. If every endpoint is cooling down we still
    return the next one (a throttled call beats no call at all)."""
    global _idx
    if not _URLS:
        return ""
    now = time.monotonic()
    with _lock:
        n = len(_URLS)
        for _ in range(n):
            _idx = (_idx + 1) % n
            u = _URLS[_idx]
            if _cooldown.get(u, 0.0) <= now:
                return u
        return _URLS[_idx]


def ws_url() -> str:
    """Same rotation as http_url but as a wss:// endpoint — call once per reconnect."""
    u = http_url()
    return u.replace("https://", "wss://").replace("http://", "ws://") if u else ""


def penalize(url: str, seconds: float | None = None) -> None:
    """Mark an endpoint throttled so it's skipped for `seconds` (default cooldown).

    Accepts http(s):// or ws(s):// forms — they're normalized to one key."""
    if not url:
        return
    c = _canon(url)
    with _lock:
        _cooldown[c] = time.monotonic() + (seconds if seconds is not None else _COOLDOWN_S)


def is_quota_error(status_code: int | None, body: str | None) -> bool:
    """True if a response looks like a rate-limit / quota-exhaustion (cool the key)."""
    if status_code == 429:
        return True
    if body and ("-32429" in body or "max usage reached" in body):
        return True
    return False
