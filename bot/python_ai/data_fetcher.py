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
WSOL_MINT              = "So11111111111111111111111111111111111111112"

REQUEST_TIMEOUT    = 10       # seconds per request
MAX_RETRIES        = 3
BACKOFF_BASE       = 1.0      # seconds; doubles each retry
RETRY_STATUS_CODES = {429, 500, 502, 503, 504}
CACHE_TTL_SECONDS  = 60
PRICE_CACHE_TTL_SECONDS = 2.0
STALE_PRICE_GRACE_SECONDS = 8.0
FAILOVER_STALE_GRACE_SECONDS = 30.0
DEX_RATE_LIMIT_COOLDOWN_SECONDS = 5.0
DEX_MINT_FAILURE_BASE_COOLDOWN_SECONDS = 10.0
DEX_MINT_FAILURE_MAX_COOLDOWN_SECONDS = 60.0
SUPPLY_CACHE_TTL = 300  # 5 minutes — meme coin supply rarely changes
HELIUS_TX_MAX_PRICE_RATIO = float(os.getenv("HELIUS_TX_MAX_PRICE_RATIO", "3.0"))

# ── Dead mint blacklist ───────────────────────────────────────────────────────
# Mints that reliably fail every request (SSL EOF, delisted, etc).
# Stopgap until the watchlist query excludes closed-position mints automatically.

DEAD_MINTS: set[str] = {
    '6tjSq5oLHFmAvZbcznCYRrZRpxQWbkepyFQV8csnpump',  # TAGIDO — SSL EOF on every request
}

# ── Cache ─────────────────────────────────────────────────────────────────────

_cache: dict[str, tuple[dict, float]] = {}   # mint → (result, epoch_time)
_price_cache: dict[str, tuple[dict, float]] = {}   # mint → (result, monotonic_time)
_sol_price_cache: tuple[float, float] | None = None   # (price_usd, monotonic_time)
_supply_cache: dict[str, tuple[int, int, float]] = {}  # mint → (supply, decimals, monotonic_time)
_dex_rate_limited_until = 0.0
_dex_mint_cooldowns: dict[str, float] = {}   # mint -> monotonic deadline
_dex_mint_failures: dict[str, int] = {}      # mint -> consecutive Dex failure count


def _cache_get(mint: str) -> Optional[dict]:
    entry = _cache.get(mint)
    if entry and (time.monotonic() - entry[1]) < CACHE_TTL_SECONDS:
        log.debug(f"[fetcher] cache hit: {mint[:8]}...")
        return entry[0]
    return None


def _cache_set(mint: str, data: dict) -> None:
    _cache[mint] = (data, time.monotonic())


def _price_cache_get(mint: str, max_age: float = PRICE_CACHE_TTL_SECONDS) -> Optional[dict]:
    entry = _price_cache.get(mint)
    if entry and (time.monotonic() - entry[1]) < max_age:
        return entry[0]
    return None


def _price_cache_set(mint: str, data: dict) -> None:
    _price_cache[mint] = (data, time.monotonic())


def _sol_price_cache_get(max_age: float = PRICE_CACHE_TTL_SECONDS) -> Optional[float]:
    global _sol_price_cache
    if _sol_price_cache and (time.monotonic() - _sol_price_cache[1]) < max_age:
        return _sol_price_cache[0]
    return None


def _sol_price_cache_set(price_usd: float) -> None:
    global _sol_price_cache
    _sol_price_cache = (price_usd, time.monotonic())


def _set_dex_mint_cooldown(mint: str) -> None:
    failures = _dex_mint_failures.get(mint, 0) + 1
    _dex_mint_failures[mint] = failures
    cooldown = min(
        DEX_MINT_FAILURE_BASE_COOLDOWN_SECONDS * (2 ** (failures - 1)),
        DEX_MINT_FAILURE_MAX_COOLDOWN_SECONDS,
    )
    _dex_mint_cooldowns[mint] = time.monotonic() + cooldown


def _clear_dex_mint_cooldown(mint: str) -> None:
    _dex_mint_failures.pop(mint, None)
    _dex_mint_cooldowns.pop(mint, None)


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
    global _dex_rate_limited_until

    if time.monotonic() < _dex_rate_limited_until:
        return None
    if time.monotonic() < _dex_mint_cooldowns.get(mint, 0.0):
        return None

    url = f"{DEXSCREENER_URL}/{mint}"

    for attempt in range(1, MAX_RETRIES + 2):   # +1 extra slot for the 429 retry
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 404:
                _set_dex_mint_cooldown(mint)
                return None

            if resp.status_code == 429:
                _dex_rate_limited_until = time.monotonic() + DEX_RATE_LIMIT_COOLDOWN_SECONDS
                _set_dex_mint_cooldown(mint)
                log.warning(
                    f"[fetcher] 429 rate-limited on {mint[:8]}... "
                    f"cooling down DexScreener for {DEX_RATE_LIMIT_COOLDOWN_SECONDS:.0f}s"
                )
                return None

            if resp.status_code in RETRY_STATUS_CODES:
                if attempt <= MAX_RETRIES:
                    time.sleep(2)
                    continue
                _set_dex_mint_cooldown(mint)
                return None

            resp.raise_for_status()
            data = resp.json()

        except requests.exceptions.RequestException as e:
            log.warning(f"[fetcher] fetch_token_price attempt {attempt}: {e}")
            if attempt <= MAX_RETRIES:
                time.sleep(2)
                continue
            _set_dex_mint_cooldown(mint)
            return None

        pairs = data.get("pairs") or []
        sol_pairs = [p for p in pairs if p.get("chainId") == "solana"]
        if not sol_pairs:
            _set_dex_mint_cooldown(mint)
            return None

        best = max(sol_pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
        price_str = best.get("priceUsd")
        result = {
            "price_usd":     float(price_str) if price_str else None,
            "mcap":          best.get("marketCap"),
            "liquidity_usd": (best.get("liquidity") or {}).get("usd"),
            "volume_h1":     (best.get("volume") or {}).get("h1"),
        }
        _clear_dex_mint_cooldown(mint)
        return result

    return None


def _fetch_token_supply_helius(mint: str) -> tuple:
    """
    Fetch token supply and decimals from Helius RPC via getTokenSupply.
    Results are cached for SUPPLY_CACHE_TTL seconds.
    Returns (amount_raw, decimals) or (None, None) on any failure.
    """
    cached = _supply_cache.get(mint)
    if cached and (time.monotonic() - cached[2]) < SUPPLY_CACHE_TTL:
        return cached[0], cached[1]
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
        _supply_cache[mint] = (int(amount), int(decimals), time.monotonic())
        return int(amount), int(decimals)
    except Exception as e:
        log.warning(f"[fetcher] Helius supply fetch failed for {mint[:8]}...: {e}")
        return None, None


def _fetch_jupiter_simple_price_usd(mint: str) -> Optional[float]:
    """
    Lightweight Jupiter USD price fetch without supply/mcap logic.
    Used by websocket transaction parsing when we only need a quote asset price.
    """
    if not mint:
        return None
    if mint == WSOL_MINT:
        cached = _sol_price_cache_get()
        if cached:
            return cached
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
        data = resp.json()
        price_data = data.get(mint)
        if not price_data:
            return None
        price = float(price_data.get("usdPrice") or 0)
        if price <= 0:
            return None
        if mint == WSOL_MINT:
            _sol_price_cache_set(price)
        return price
    except Exception as e:
        log.warning(f"[fetcher] Jupiter simple price failed for {mint[:8]}...: {e}")
        return None


def _rpc_get_transaction(
    signature: str,
    commitment: str = "confirmed",
    *,
    attempts: int = 3,
    delay_seconds: float = 0.25,
) -> Optional[dict]:
    """
    Fetch one transaction from the configured Solana RPC using jsonParsed encoding.
    Returns the transaction result object, or None on failure.
    """
    if not SOLANA_RPC_URL or not signature:
        return None
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.post(
                SOLANA_RPC_URL,
                json={
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTransaction",
                    "params": [
                        signature,
                        {
                            "encoding": "jsonParsed",
                            "commitment": commitment,
                            "maxSupportedTransactionVersion": 0,
                        },
                    ],
                },
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code != 200:
                return None
            data = resp.json()
            if data.get("error"):
                return None
            result = data.get("result")
            if result:
                return result
            if attempt < attempts:
                time.sleep(delay_seconds)
        except Exception as e:
            log.warning(f"[fetcher] Helius getTransaction failed {signature[:8]}...: {e}")
            return None
    return None


def _extract_max_abs_token_delta(token_balances: list[dict], mint: str) -> Optional[float]:
    """
    For a given mint, compute the largest absolute per-account token balance delta
    across pre/post token balances in one transaction.

    This heuristic is intentionally simple: for AMM swaps, the biggest changed
    token account on each side is usually enough to estimate swap price quickly.
    """
    if not token_balances or not mint:
        return None

    amounts_by_index: dict[int, tuple[float, float]] = {}
    for entry in token_balances:
        if entry.get("mint") != mint:
            continue
        idx = entry.get("accountIndex")
        if idx is None:
            continue
        pre_amt, post_amt = amounts_by_index.get(int(idx), (0.0, 0.0))
        side = entry.get("_side")
        ui_amount = ((entry.get("uiTokenAmount") or {}).get("uiAmount"))
        if ui_amount is None:
            raw_amount = ((entry.get("uiTokenAmount") or {}).get("amount"))
            decimals = ((entry.get("uiTokenAmount") or {}).get("decimals") or 0)
            try:
                ui_amount = int(raw_amount) / (10 ** int(decimals))
            except Exception:
                ui_amount = 0.0
        try:
            ui_amount = float(ui_amount)
        except Exception:
            ui_amount = 0.0
        if side == "pre":
            pre_amt = ui_amount
        else:
            post_amt = ui_amount
        amounts_by_index[int(idx)] = (pre_amt, post_amt)

    max_abs_delta = 0.0
    for pre_amt, post_amt in amounts_by_index.values():
        delta = post_amt - pre_amt
        if abs(delta) > max_abs_delta:
            max_abs_delta = abs(delta)
    return max_abs_delta or None


def _extract_min_significant_token_delta(
    token_balances: list[dict], mint: str, min_amount: float = 1e-6
) -> Optional[float]:
    """
    Extract the minimum significant absolute token balance delta for a mint.

    Using min instead of max avoids picking up large unrelated operations in the
    same transaction (e.g. a 100-SOL WSOL transfer alongside a 0.05-SOL swap).
    The min_amount filter removes dust/fee changes below the threshold.
    """
    if not token_balances or not mint:
        return None

    amounts_by_index: dict[int, tuple[float, float]] = {}
    for entry in token_balances:
        if entry.get("mint") != mint:
            continue
        idx = entry.get("accountIndex")
        if idx is None:
            continue
        pre_amt, post_amt = amounts_by_index.get(int(idx), (0.0, 0.0))
        side = entry.get("_side")
        ui_amount = (entry.get("uiTokenAmount") or {}).get("uiAmount")
        if ui_amount is None:
            raw_amount = (entry.get("uiTokenAmount") or {}).get("amount")
            decimals = (entry.get("uiTokenAmount") or {}).get("decimals") or 0
            try:
                ui_amount = int(raw_amount) / (10 ** int(decimals))
            except Exception:
                ui_amount = 0.0
        try:
            ui_amount = float(ui_amount)
        except Exception:
            ui_amount = 0.0
        if side == "pre":
            pre_amt = ui_amount
        else:
            post_amt = ui_amount
        amounts_by_index[int(idx)] = (pre_amt, post_amt)

    significant = [
        abs(post - pre)
        for pre, post in amounts_by_index.values()
        if abs(post - pre) >= min_amount
    ]
    return min(significant) if significant else None


def fetch_ws_exit_market_from_transaction(signature: str, mint: str) -> Optional[dict]:
    """
    Try to derive a websocket-exit market snapshot from Helius transaction data.

    The first implementation focuses on the common meme-coin swap case where the
    monitored token is swapped against wrapped SOL. If we can estimate:

        token_price_usd = (wSOL_delta / token_delta) * SOL_USD

    then we can combine it with getTokenSupply to estimate current mcap without
    touching DexScreener.

    Returns a partial market dict shaped like fetch_token_price() on success:
        {"price_usd", "mcap", "liquidity_usd", "volume_h1", "source"}
    """
    tx = _rpc_get_transaction(signature, commitment="confirmed")
    if not tx:
        return None

    meta = tx.get("meta") or {}
    pre = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []
    if not pre and not post:
        return None

    tagged_balances: list[dict] = []
    for entry in pre:
        tagged = dict(entry)
        tagged["_side"] = "pre"
        tagged_balances.append(tagged)
    for entry in post:
        tagged = dict(entry)
        tagged["_side"] = "post"
        tagged_balances.append(tagged)

    # Min for WSOL: avoids large unrelated WSOL transfers (e.g. 100-SOL transfers
    # in the same TX) inflating the implied price ratio. The swap amount is almost
    # always the smallest significant WSOL movement in the transaction.
    # Max for the token: the pool delta is the largest token change in a swap.
    wsol_delta  = _extract_min_significant_token_delta(tagged_balances, WSOL_MINT, min_amount=0.0001)
    token_delta = _extract_max_abs_token_delta(tagged_balances, mint)
    if not token_delta or not wsol_delta:
        return None

    sol_price_usd = _fetch_jupiter_simple_price_usd(WSOL_MINT)
    if not sol_price_usd:
        return None

    token_price_usd = (wsol_delta / token_delta) * sol_price_usd
    if token_price_usd <= 0:
        return None

    mcap = None
    supply, decimals = _fetch_token_supply_helius(mint)
    if supply and decimals is not None:
        mcap = token_price_usd * (supply / (10 ** decimals))
    if not mcap:
        return None

    # Validate computed mcap against cached price. A ratio beyond
    # HELIUS_TX_MAX_PRICE_RATIO (default 3×) in either direction means the
    # delta extraction likely picked up a non-swap operation — discard and let
    # ws_monitor fall back to the DexScreener/Jupiter price path.
    cached = _price_cache_get(mint, max_age=120)
    if cached and cached.get("mcap"):
        cached_mcap = float(cached["mcap"])
        if cached_mcap > 0:
            ratio = mcap / cached_mcap
            if ratio > HELIUS_TX_MAX_PRICE_RATIO or ratio < (1.0 / HELIUS_TX_MAX_PRICE_RATIO):
                log.warning(
                    f"[fetcher] helius_tx sanity fail {mint[:8]}..."
                    f" computed={mcap:.0f} cached={cached_mcap:.0f} ratio={ratio:.2f}x — discarding"
                )
                return None

    return {
        "price_usd": token_price_usd,
        "mcap": mcap,
        "liquidity_usd": None,
        "volume_h1": None,
        "source": "helius_tx",
    }


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


def _blend_stale_dex_with_jupiter(stale: dict, fresh: dict) -> Optional[dict]:
    """
    Preserve Dex-derived market-cap context during outages by scaling the last
    known Dex mcap by the fresh Jupiter price ratio when possible.
    """
    if not stale or not fresh:
        return None

    stale_price = stale.get("price_usd")
    stale_mcap = stale.get("mcap")
    fresh_price = fresh.get("price_usd")
    fresh_mcap = fresh.get("mcap")

    blended_mcap = None
    if stale_price and stale_mcap and fresh_price:
        blended_mcap = stale_mcap * (fresh_price / stale_price)
    elif fresh_mcap:
        blended_mcap = fresh_mcap
    elif stale_mcap:
        blended_mcap = stale_mcap

    if not blended_mcap:
        return None

    return {
        "price_usd": fresh_price or stale_price,
        "mcap": blended_mcap,
        "liquidity_usd": stale.get("liquidity_usd"),
        "volume_h1": stale.get("volume_h1"),
        "source": "jupiter+stale_dex",
    }


def fetch_token_price(mint: str) -> Optional[dict]:
    """
    Lightweight price fetch for monitor/backfill.

    Uses a short-lived shared in-process cache so monitor and websocket exit
    checks can reuse the same fresh snapshot instead of hammering DexScreener.
    Tries DexScreener first; falls back to Jupiter + Helius on failure.

    Returns:
        {"price_usd", "mcap", "liquidity_usd", "volume_h1"}
        or None if all sources fail.
    """
    if mint in DEAD_MINTS:
        return None

    cached = _price_cache_get(mint)
    if cached:
        return cached

    result = _try_dexscreener_price(mint)
    if result and result.get("mcap"):
        _price_cache_set(mint, result)
        return result

    stale = _price_cache_get(mint, max_age=STALE_PRICE_GRACE_SECONDS)
    if stale:
        return stale

    extended_stale = None
    if time.monotonic() < _dex_mint_cooldowns.get(mint, 0.0):
        extended_stale = _price_cache_get(mint, max_age=FAILOVER_STALE_GRACE_SECONDS)
        if extended_stale:
            jupiter = _fetch_jupiter_price(mint)
            blended = _blend_stale_dex_with_jupiter(extended_stale, jupiter) if jupiter else None
            if blended:
                _price_cache_set(mint, blended)
                return blended
            return extended_stale

    log.warning(f"[fetcher] DexScreener failed, trying Jupiter for {mint[:8]}...")
    result = _fetch_jupiter_price(mint)
    if result and extended_stale:
        blended = _blend_stale_dex_with_jupiter(extended_stale, result)
        if blended:
            _price_cache_set(mint, blended)
            return blended

    if result and result.get("mcap"):
        _price_cache_set(mint, result)
    return result


def fetch_prices_batch_jupiter(mints: list[str]) -> dict[str, float]:
    """
    Fetch current USD prices for multiple mints in a single Jupiter API call.
    Automatically includes WSOL to refresh the SOL price cache.
    Returns {mint: usd_price} for mints with valid prices only.
    Handles batches larger than 50 automatically.
    """
    if not mints:
        return {}

    all_mints = list(set(mints) | {WSOL_MINT})
    results: dict[str, float] = {}
    headers = {"x-api-key": JUPITER_API_KEY} if JUPITER_API_KEY else {}

    for i in range(0, len(all_mints), 50):
        batch = all_mints[i:i + 50]
        try:
            resp = requests.get(
                JUPITER_PRICE_URL,
                params={"ids": ",".join(batch)},
                headers=headers,
                timeout=REQUEST_TIMEOUT,
            )
            if resp.status_code != 200:
                log.warning(f"[fetcher] Jupiter batch price HTTP {resp.status_code}")
                continue
            data = resp.json()
            for mint in batch:
                price_data = data.get(mint)
                if not price_data:
                    continue
                price = float(price_data.get("usdPrice") or 0)
                if price > 0:
                    results[mint] = price
        except Exception as e:
            log.warning(f"[fetcher] Jupiter batch price failed: {e}")

    sol_price = results.get(WSOL_MINT)
    if sol_price:
        _sol_price_cache_set(sol_price)

    return results


def get_mcap_blended(mint: str, jup_price_usd: float) -> Optional[float]:
    """
    Compute current USD mcap by scaling a cached DexScreener mcap baseline
    by the Jupiter price ratio. No extra API call needed.
    Returns None if no DexScreener baseline is cached yet.
    """
    stale = _price_cache_get(mint, max_age=300)
    if stale and stale.get("price_usd") and stale.get("mcap"):
        try:
            return float(stale["mcap"]) * (jup_price_usd / float(stale["price_usd"]))
        except (ZeroDivisionError, TypeError):
            return None
    return None


def fetch_token_price_fast(mint: str) -> Optional[dict]:
    """
    Jupiter-first price fetch for time-sensitive exit checks (ws_monitor fallback).
    Falls back to the standard DexScreener path if Jupiter fails or has no mcap.
    """
    if mint in DEAD_MINTS:
        return None
    cached = _price_cache_get(mint)
    if cached:
        return cached
    result = _fetch_jupiter_price(mint)
    if result and result.get("mcap"):
        _price_cache_set(mint, result)
        return result
    return fetch_token_price(mint)
