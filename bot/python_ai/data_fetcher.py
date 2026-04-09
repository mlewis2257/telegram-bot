"""
data_fetcher.py — Unified token data fetcher.

Fetches price, mcap, liquidity, and metadata for a Solana token mint
from DexScreener. Results are cached for 60 seconds.
"""

import logging
import os
import time
from datetime import datetime, timezone
from typing import Optional

import requests

log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────────────────

DEXSCREENER_URL        = "https://api.dexscreener.com/latest/dex/tokens"
DEXSCREENER_SEARCH_URL = "https://api.dexscreener.com/latest/dex/search"
JUPITER_PRICE_URL      = "https://api.jup.ag/price/v3"
SOLANA_RPC_URL         = os.getenv("SOLANA_RPC_URL", "")
JUPITER_API_KEY        = os.getenv("JUPITER_API_KEY", "")

REQUEST_TIMEOUT    = 10       # seconds per request
MAX_RETRIES        = 3
BACKOFF_BASE       = 1.0      # seconds; doubles each retry
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
CACHE_TTL_SECONDS  = 60

# ── Dead mint blacklist ───────────────────────────────────────────────────────
# Mints that reliably fail every request (SSL EOF, delisted, etc).
# Stopgap until the watchlist query excludes closed-position mints automatically.

DEAD_MINTS: set[str] = {
    '6tjSq5oLHFmAvZbcznCYRrZRpxQWbkepyFQV8csnpump',  # TAGIDO — SSL EOF on every request
}

# ── Cache ─────────────────────────────────────────────────────────────────────

_cache: dict[str, tuple[dict, float]] = {}   # mint → (result, epoch_time)


def _cache_get(mint: str) -> Optional[dict]:
    entry = _cache.get(mint)
    if entry and (time.monotonic() - entry[1]) < CACHE_TTL_SECONDS:
        log.debug(f"[fetcher] cache hit: {mint[:8]}...")
        return entry[0]
    return None


def _cache_set(mint: str, data: dict) -> None:
    _cache[mint] = (data, time.monotonic())


# ── HTTP helper ───────────────────────────────────────────────────────────────

def _get(url: str, params: dict = None) -> Optional[dict]:
    """
    GET request with exponential backoff retry.
    Returns parsed JSON dict on success, None on permanent failure.
    """
    delay = BACKOFF_BASE
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 404:
                log.debug(f"[fetcher] 404 — {url}")
                return None

            if resp.status_code in RETRY_STATUS_CODES:
                log.warning(
                    f"[fetcher] {resp.status_code} on attempt {attempt}/{MAX_RETRIES} "
                    f"— retrying in {delay}s: {url}"
                )
                time.sleep(delay)
                delay *= 2
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.exceptions.ConnectionError as e:
            # DNS failures won't resolve on retry — fail immediately
            if "NameResolutionError" in str(e) or "Failed to resolve" in str(e):
                log.warning(f"[fetcher] DNS failure (no retry): {url}")
                return None
            log.warning(f"[fetcher] Connection error attempt {attempt}/{MAX_RETRIES}: {e}")
            time.sleep(delay)
            delay *= 2
        except requests.exceptions.Timeout:
            log.warning(f"[fetcher] Timeout attempt {attempt}/{MAX_RETRIES}: {url}")
            time.sleep(delay)
            delay *= 2
        except requests.exceptions.RequestException as e:
            log.error(f"[fetcher] Request failed (no retry): {e}")
            return None

    log.error(f"[fetcher] All {MAX_RETRIES} attempts failed: {url}")
    return None


# ── Source fetchers ───────────────────────────────────────────────────────────

def _fetch_dexscreener(mint: str) -> Optional[dict]:
    """
    Fetch metadata, liquidity, and mcap from DexScreener.
    Picks the Solana pair with the highest USD liquidity.
    Returns a partial result dict or None.
    """
    data = _get(f"{DEXSCREENER_URL}/{mint}")
    if not data:
        log.debug(f"[fetcher] DexScreener HTTP failed for {mint[:8]}...")
        return None

    pairs = data.get("pairs") or []
    total_pairs = len(pairs)

    # Filter to Solana pairs only, then sort by liquidity descending
    sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
    if not sol_pairs:
        log.debug(f"[fetcher] DexScreener: {total_pairs} pairs total, 0 Solana — dead/unlisted: {mint[:8]}...")
        return None

    best = max(sol_pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)

    price_str  = best.get("priceUsd")
    liquidity  = (best.get("liquidity") or {}).get("usd")
    mcap       = best.get("marketCap")
    volume_h24 = (best.get("volume") or {}).get("h24")
    volume_h6  = (best.get("volume") or {}).get("h6")
    volume_h1  = (best.get("volume") or {}).get("h1")

    log.debug(f"[fetcher] DexScreener {mint[:8]}... price={price_str} mcap={mcap} liq={liquidity}")

    return {
        "price_usd":     float(price_str) if price_str else None,
        "mcap":          mcap,
        "liquidity_usd": liquidity,
        "name":          (best.get("baseToken") or {}).get("name"),
        "symbol":        (best.get("baseToken") or {}).get("symbol"),
        "dex_url":       best.get("url"),
        "volume_h24":    volume_h24,
        "volume_h6":     volume_h6,
        "volume_h1":     volume_h1,
    }


# ── Public functions ──────────────────────────────────────────────────────────

def fetch_token_data(mint: str) -> Optional[dict]:
    """
    Fetch token data from DexScreener only.

    Returns a unified dict:
        {
            mint, price_usd, mcap, liquidity_usd,
            name, symbol, dex_url,
            price_source, fetched_at
        }

    Returns None if DexScreener has no data for this mint.
    Results are cached for 60 seconds.
    """
    cached = _cache_get(mint)
    if cached:
        return cached

    dex = _fetch_dexscreener(mint)
    if not dex:
        log.info(f"[fetcher] token not yet on DexScreener: {mint[:8]}...")
        return None

    result = {
        "mint":          mint,
        "price_usd":     dex.get("price_usd"),
        "mcap":          dex.get("mcap"),
        "liquidity_usd": dex.get("liquidity_usd"),
        "name":          dex.get("name"),
        "symbol":        dex.get("symbol"),
        "dex_url":       dex.get("dex_url"),
        "price_source":  "dexscreener",
        "fetched_at":    datetime.now(timezone.utc),
    }

    _cache_set(mint, result)
    log.debug(
        f"[fetcher] {mint[:8]}... price={result['price_usd']} (dexscreener) "
        f"mcap={result['mcap']} liq={result['liquidity_usd']}"
    )
    return result


def search_token_by_symbol(symbol: str) -> Optional[dict]:
    """
    Search DexScreener for a Solana token by symbol.

    Used for realtime entry signals that contain no mint address.

    Returns:
        {mint, name, symbol, price_usd, mcap, liquidity_usd, confidence}
        confidence = 'high'  — exactly one distinct Solana mint matched
        confidence = 'low'   — multiple Solana mints share this symbol
        None                 — no Solana pairs found

    Results are cached for 60 seconds by "search:{SYMBOL}" key.
    """
    cache_key = f"search:{symbol.upper()}"
    cached = _cache_get(cache_key)
    if cached:
        return cached

    data = _get(DEXSCREENER_SEARCH_URL, params={"q": symbol})
    if not data:
        return None

    pairs = data.get("pairs") or []
    sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
    if not sol_pairs:
        log.debug(f"[fetcher] search: no Solana pairs for {symbol}")
        return None

    # Narrow to pairs whose base token symbol matches exactly (case-insensitive)
    target = symbol.upper()
    exact = [
        p for p in sol_pairs
        if (p.get("baseToken") or {}).get("symbol", "").upper() == target
    ]
    candidates = exact if exact else sol_pairs

    # Sort by liquidity descending — highest-liquidity pair is the most likely match
    candidates.sort(
        key=lambda p: (p.get("liquidity") or {}).get("usd") or 0,
        reverse=True,
    )
    best = candidates[0]

    unique_mints = {
        (p.get("baseToken") or {}).get("address")
        for p in candidates
        if (p.get("baseToken") or {}).get("address")
    }
    confidence = "low" if len(unique_mints) > 1 else "high"

    mint       = (best.get("baseToken") or {}).get("address")
    price_str  = best.get("priceUsd")
    liquidity  = (best.get("liquidity") or {}).get("usd")
    mcap       = best.get("marketCap")

    log.debug(
        f"[fetcher] search {symbol} → mint={mint} "
        f"liq={liquidity} confidence={confidence}"
    )

    result = {
        "mint":          mint,
        "name":          (best.get("baseToken") or {}).get("name"),
        "symbol":        (best.get("baseToken") or {}).get("symbol"),
        "price_usd":     float(price_str) if price_str else None,
        "mcap":          mcap,
        "liquidity_usd": liquidity,
        "confidence":    confidence,
    }

    if mint:
        _cache_set(cache_key, result)
    return result


def fetch_metadata_only(mint: str) -> Optional[dict]:
    """
    Fetch DexScreener metadata only — no Jupiter price call.

    Use this for lagging calls where the token has already moved and
    live price is meaningless. Returns name, symbol, liquidity, mcap,
    and dex_url without the overhead of a Jupiter request.
    """
    cached = _cache_get(mint)
    if cached:
        return cached

    dex = _fetch_dexscreener(mint)
    if not dex:
        return None

    result = {
        "mint":          mint,
        "price_usd":     dex.get("price_usd"),
        "mcap":          dex.get("mcap"),
        "liquidity_usd": dex.get("liquidity_usd"),
        "name":          dex.get("name"),
        "symbol":        dex.get("symbol"),
        "dex_url":       dex.get("dex_url"),
        "price_source":  "dexscreener",
        "fetched_at":    datetime.now(timezone.utc),
    }
    _cache_set(mint, result)
    return result


def _try_dexscreener_price(mint: str) -> Optional[dict]:
    """
    Lightweight DexScreener price fetch. Bypasses cache.
    Returns result dict or None on any failure.
    """
    url = f"{DEXSCREENER_URL}/{mint}"

    for attempt in range(1, MAX_RETRIES + 2):   # +1 extra slot for the 429 retry
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 404:
                return None

            if resp.status_code == 429:
                log.warning(f"[fetcher] 429 rate-limited on {mint[:8]}... sleeping 30s")
                time.sleep(30)
                continue

            if resp.status_code in RETRY_STATUS_CODES:
                if attempt <= MAX_RETRIES:
                    time.sleep(2)
                    continue
                return None

            resp.raise_for_status()
            data = resp.json()

        except requests.exceptions.RequestException as e:
            log.warning(f"[fetcher] fetch_token_price attempt {attempt}: {e}")
            if attempt <= MAX_RETRIES:
                time.sleep(2)
                continue
            return None

        pairs = data.get("pairs") or []
        sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
        if not sol_pairs:
            return None

        best = max(sol_pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
        price_str = best.get("priceUsd")
        return {
            "price_usd":     float(price_str) if price_str else None,
            "mcap":          best.get("marketCap"),
            "liquidity_usd": (best.get("liquidity") or {}).get("usd"),
            "volume_h1":     (best.get("volume") or {}).get("h1"),
        }

    return None


def _fetch_token_supply_helius(mint: str) -> tuple:
    """
    Fetch token supply and decimals from Helius RPC via getTokenSupply.
    Returns (amount_raw, decimals) or (None, None) on any failure.
    """
    if not SOLANA_RPC_URL:
        return None, None
    try:
        resp = requests.post(
            SOLANA_RPC_URL,
            json={
                "jsonrpc": "2.0",
                "id":      1,
                "method":  "getTokenSupply",
                "params":  [mint],
            },
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            return None, None
        data  = resp.json()
        value = (data.get("result") or {}).get("value") or {}
        amount   = value.get("amount")
        decimals = value.get("decimals")
        if amount is None or decimals is None:
            return None, None
        return int(amount), int(decimals)
    except Exception as e:
        log.warning(f"[fetcher] Helius supply fetch failed for {mint[:8]}...: {e}")
        return None, None


def _fetch_jupiter_price(mint: str) -> Optional[dict]:
    """
    Fallback price fetch from Jupiter Price API when DexScreener is unavailable.
    Attempts to compute mcap via Helius RPC getTokenSupply.
    """
    try:
        headers = {"x-api-key": JUPITER_API_KEY} if JUPITER_API_KEY else {}
        resp = requests.get(
            JUPITER_PRICE_URL,
            params={"ids": mint},
            headers=headers,
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            return None
        data       = resp.json()
        price_data = data.get(mint)
        if not price_data:
            return None
        price = float(price_data.get("usdPrice") or 0)
        if not price:
            return None

        mcap = None
        supply, decimals = _fetch_token_supply_helius(mint)
        if supply and decimals is not None:
            mcap = price * (supply / (10 ** decimals))
            log.debug(f"[fetcher] Jupiter+Helius mcap={mcap:.0f} for {mint[:8]}...")

        return {
            "price_usd":     price,
            "mcap":          mcap,
            "liquidity_usd": None,
            "volume_h1":     None,
            "source":        "jupiter+helius",
        }
    except Exception as e:
        log.warning(f"[fetcher] Jupiter price fetch failed for {mint[:8]}...: {e}")
        return None


def fetch_token_price(mint: str) -> Optional[dict]:
    """
    Lightweight price fetch for monitor/backfill. Bypasses cache.
    Tries DexScreener first; falls back to Jupiter + Helius on failure.

    Returns:
        {"price_usd", "mcap", "liquidity_usd", "volume_h1"}
        or None if all sources fail.
    """
    if mint in DEAD_MINTS:
        return None

    result = _try_dexscreener_price(mint)
    if result and result.get("mcap"):
        return result

    log.warning(f"[fetcher] DexScreener failed, trying Jupiter for {mint[:8]}...")
    return _fetch_jupiter_price(mint)
