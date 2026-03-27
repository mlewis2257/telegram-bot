"""
type_a_vip.py — SolHouse VIP channel parser.

Handles three message types from the VIP channel:

  gem_alert    — "💎 New Solana Gem Alert! 💎"
  whale_alert  — "🦅 SolHouse Whale Alert 🦅"
  volume_alert — "VOLUME ALERT!"

All three return a standardised dict with all keys present;
fields not applicable to a message type are set to None.

Public API
----------
parse(text: str) -> dict | None
    Returns parsed dict, or None if no known header is found.
"""

import re
import unicodedata

# ── Detection ─────────────────────────────────────────────────────────────────

IS_GEM_ALERT_RE    = re.compile(r'💎\s*New\s+Solana\s+Gem\s+Alert',  re.IGNORECASE)
IS_WHALE_ALERT_RE  = re.compile(r'🦅\s*SolHouse\s+Whale\s+Alert',    re.IGNORECASE)
IS_VOLUME_ALERT_RE = re.compile(r'VOLUME\s+ALERT!',                   re.IGNORECASE)

# ── Shared field patterns ─────────────────────────────────────────────────────

# Token Name: SomeName
NAME_RE = re.compile(r'Token\s+Name\s*:\s*(.+)', re.IGNORECASE)

# Ticker: $SYMBOL  or  Ticker: SYMBOL
TICKER_RE = re.compile(
    r'Ticker\s*:\s*\$?([A-Za-z][A-Za-z0-9]{0,14})',
    re.IGNORECASE,
)
# Fallback: any $WORD (first occurrence that starts with a letter)
SYMBOL_FALLBACK_RE = re.compile(r'\$([A-Za-z][A-Za-z0-9]{0,14})')

# Market Cap: 25.5K  (may be "N/A")
MCAP_RE = re.compile(r'Market\s+Cap\s*:\s*(\S+)', re.IGNORECASE)

# Volume (24h): 120K  or  Volume: 120K
VOL_RE = re.compile(r'Volume(?:\s*\(24h\))?\s*:\s*(\S+)', re.IGNORECASE)

# Liquidity: 15K
LIQ_RE = re.compile(r'Liquidity\s*:\s*(\S+)', re.IGNORECASE)

# Txns (24h): 450  or  Txns: 450
TXNS_RE = re.compile(r'Txns(?:\s*\(24h\))?\s*:\s*(\S+)', re.IGNORECASE)

# Contract Address: <mint>  — same line (volume alert format)
CONTRACT_INLINE_RE = re.compile(
    r'Contract\s+Address\s*:\s*([1-9A-HJ-NP-Za-km-z]{32,50})',
    re.IGNORECASE,
)
# Contract Address:\n<mint>  — next line (gem + whale format)
CONTRACT_NEXT_LINE_RE = re.compile(
    r'Contract\s+Address\s*:\s*\n\s*([1-9A-HJ-NP-Za-km-z]{32,50})',
    re.IGNORECASE,
)

# Whale wallet: "Wallet: 97 SOL"
WALLET_RE = re.compile(r'Wallet\s*:\s*([\d,.]+)\s*SOL', re.IGNORECASE)

# Whale spend: "9.83 SOL → 0.95%"
SPENT_RE = re.compile(r'([\d.]+)\s*SOL\s*[→>]\s*([\d.]+)%')

# HIGH RISK flag (volume alert)
HIGH_RISK_RE = re.compile(r'HIGH\s+RISK', re.IGNORECASE)

# VIP tier emoji line (gem alert)
VIP_TIER_RE = re.compile(r'(🟢\s*Safe|🔴\s*Gamble\s+Risk|🔵\s*Gamble)', re.IGNORECASE)


# ── Value converter ───────────────────────────────────────────────────────────

def parse_mcap(value: str) -> float | None:
    """
    Parse K/M/plain number strings.

    '25.5K' → 25500.0 | '1.2M' → 1200000.0 | '500' → 500.0
    Returns None for 'N/A', empty string, or unparseable values.
    """
    if not value:
        return None
    value = value.strip().replace(',', '')
    if value.upper() in ('N/A', 'NONE', ''):
        return None
    try:
        if value.upper().endswith('M'):
            return float(value[:-1]) * 1_000_000
        if value.upper().endswith('K'):
            return float(value[:-1]) * 1_000
        return float(value)
    except ValueError:
        return None


# ── VIP tier mapper ───────────────────────────────────────────────────────────

def _parse_vip_tier_line(text: str) -> str | None:
    """
    Detect gem alert tier from emoji line.
    Returns 'safe', 'gamble_risk', 'gamble', or None.
    """
    m = VIP_TIER_RE.search(text)
    if not m:
        return None
    raw = m.group(1)
    if '🟢' in raw:
        return 'safe'
    if 'Risk' in raw or 'risk' in raw:
        return 'gamble_risk'
    if '🔵' in raw:
        return 'gamble'
    return None


# ── Public API ────────────────────────────────────────────────────────────────

def parse(text: str) -> dict | None:
    """
    Parse a VIP channel message.

    Detects the message type from the header and extracts all fields.
    Returns a standardised dict, or None if no known header is found.
    """
    if IS_GEM_ALERT_RE.search(text):
        vip_message_type = 'gem_alert'
    elif IS_WHALE_ALERT_RE.search(text):
        vip_message_type = 'whale_alert'
    elif IS_VOLUME_ALERT_RE.search(text):
        vip_message_type = 'volume_alert'
        print(f"[vip:volume_alert debug] text preview: {repr(text[:200])}")
    else:
        return None

    # ── Token name ──
    token_name = None
    m = NAME_RE.search(text)
    if m:
        token_name = m.group(1).strip()

    # ── Symbol ──
    symbol = None
    m = TICKER_RE.search(text)
    if m:
        symbol = m.group(1).strip()
    else:
        m = SYMBOL_FALLBACK_RE.search(text)
        if m:
            symbol = m.group(1).strip()

    # ── Market cap ──
    mcap_at_call = None
    m = MCAP_RE.search(text)
    if m:
        mcap_at_call = parse_mcap(m.group(1))

    # ── Volume ──
    volume_24h = None
    m = VOL_RE.search(text)
    if m:
        volume_24h = parse_mcap(m.group(1))

    # ── Liquidity ──
    liquidity = None
    m = LIQ_RE.search(text)
    if m:
        liquidity = parse_mcap(m.group(1))

    # ── Txns ──
    txns_24h = None
    m = TXNS_RE.search(text)
    if m:
        txns_24h = parse_mcap(m.group(1))  # handles K suffix

    # ── Mint address ──
    # Try same-line first (all three types may use this format:
    # "🔗 Contract Address: <mint>"), then fall back to next-line.
    mint_address = None
    for line in text.splitlines():
        if "Contract Address:" in line:
            parts = line.split("Contract Address:", 1)
            candidate = parts[1].strip() if len(parts) > 1 else ""
            # Strip invisible unicode control/format characters (common in
            # styled Telegram messages) before attempting the base58 match.
            candidate = ''.join(
                c for c in candidate
                if not unicodedata.category(c).startswith('C')
            )
            candidate = candidate.strip()
            addr_m = re.search(r'[1-9A-HJ-NP-Za-km-z]{32,50}', candidate)
            if addr_m:
                mint_address = addr_m.group(0)
                break
    if not mint_address:
        # Next-line format: "Contract Address:\n<mint>"
        m = CONTRACT_NEXT_LINE_RE.search(text)
        if m:
            raw = m.group(1)
            raw = ''.join(
                c for c in raw
                if not unicodedata.category(c).startswith('C')
            )
            mint_address = raw.strip() or None

    # ── Whale-specific fields ──
    whale_wallet_sol = None
    whale_sol_spent  = None
    whale_pct_spent  = None
    if vip_message_type == 'whale_alert':
        m = WALLET_RE.search(text)
        if m:
            try:
                whale_wallet_sol = float(m.group(1).replace(',', ''))
            except ValueError:
                pass
        m = SPENT_RE.search(text)
        if m:
            try:
                whale_sol_spent = float(m.group(1))
                whale_pct_spent = float(m.group(2))
            except ValueError:
                pass

    # ── VIP tier ──
    if vip_message_type == 'whale_alert':
        vip_tier = 'whale'
    elif vip_message_type == 'volume_alert':
        vip_tier = 'high_risk' if HIGH_RISK_RE.search(text) else None
    else:
        vip_tier = _parse_vip_tier_line(text)

    return {
        "mint_address":     mint_address,
        "symbol":           symbol,
        "token_name":       token_name,
        "mcap_at_call":     mcap_at_call,
        "volume_24h":       volume_24h,
        "liquidity":        liquidity,
        "txns_24h":         txns_24h,
        "vip_tier":         vip_tier,
        "whale_wallet_sol": whale_wallet_sol,
        "whale_sol_spent":  whale_sol_spent,
        "whale_pct_spent":  whale_pct_spent,
        "channel_tag":      "solhousesignal_vip",
        "message_type":     "initial_call",
        "vip_message_type": vip_message_type,
    }
