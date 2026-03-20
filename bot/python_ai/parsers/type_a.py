"""
Type A parser — Lagging channels.

Handles two message subtypes from lagging channels:

1. INITIAL GEM ALERT — posted at time of result, contains contract address:
    💎 Exclusive Solana Gem Alert! 💎
    Token Name: Agent Mar1o | Ticker: Mar1o
    Market Cap: 15.59K ➔ 37.30K (2x ACHIEVED!)
    Contract Address: C76Z58...pump
   → parse() returns full dict including mint

2. MILESTONE UPDATE — follow-up at higher multipliers, no contract address:
    🚀 PUMPERS has reached 4x from our call! 💥
    Initial MC: 25.62K ➔ Now: 109.23K
   → parse_milestone() returns {token_name, entry_mcap, current_mcap, multiplier}
     Caller must look up the original call_id by token name.

3. NOISE (whale alerts, DexScreener paid alerts, etc.)
   → classify() returns 'noise' so the scraper can count and skip cleanly
"""

import re

# ── Detection / classification ────────────────────────────────────────────────

# Initial gem alert: arrow + ACHIEVED in same message
IS_GEM_ALERT_RE = re.compile(r'[➔→>].*?ACHIEV', re.IGNORECASE | re.DOTALL)

# Milestone update: "TOKEN has reached Nx from our call"
# Only matches clean integer or .5 multipliers (2x, 2.5x, 3x … 999x)
IS_MILESTONE_RE = re.compile(r'has reached\s+\d{1,3}(?:\.5)?x\s+from our call', re.IGNORECASE)

# Whale alert — checked BEFORE VIP promo because whale messages contain VIP links
IS_WHALE_ALERT_RE = re.compile(r'whale\s+(alert|signal|just\s+made)', re.IGNORECASE)

# DexScreener paid alert
IS_DEXSCREENER_PAID_RE = re.compile(r'dexscreener\s+paid\s+alert', re.IGNORECASE)

# VIP promo / scanner spam — pure noise
IS_VIP_PROMO_RE = re.compile(
    r'SolHouse\s+VIP'
    r'|Inside\s+SolHouse\s+VIP'
    r'|exclusive.*vip\s+signal'
    r'|SOLHOUSE\s+AI\s+SCANNER'
    r'|solhouseaiscannerbot',
    re.IGNORECASE,
)

# Lightning Boost Alert — team purchased visibility boosts
IS_BOOST_ALERT_RE = re.compile(r'lightning\s+boost\s+alert', re.IGNORECASE)

# Boosts purchased count — "Boosts Purchased: 100"
BOOST_COUNT_RE = re.compile(r'Boosts?\s+Purchased\s*[:\-]?\s*(\d+)', re.IGNORECASE)

# Milestone mcap line: "Initial MC: 25.62K ➔ Now: 109.23K"
MILESTONE_MCAP_RE = re.compile(
    r'Initial\s+MC\s*[:\-]?\s*([\d.]+\s*[KMBkmb]?)\s*[➔→>]\s*Now\s*[:\-]?\s*([\d.]+\s*[KMBkmb]?)',
    re.IGNORECASE,
)

# Milestone multiplier: "has reached 4x" — integer or .5 only, 1–3 digits
MILESTONE_MULT_RE = re.compile(r'has reached\s+(\d{1,3}(?:\.5)?)x', re.IGNORECASE)

# Token name from milestone: "PUMPERS has reached..."
MILESTONE_NAME_RE = re.compile(r'^[\U0001F600-\U0001FFFF\s]*(\S+)\s+has reached', re.MULTILINE)


def is_lagging_message(text: str) -> bool:
    return bool(IS_GEM_ALERT_RE.search(text))


def is_milestone_message(text: str) -> bool:
    return bool(IS_MILESTONE_RE.search(text))


def classify(text: str) -> str:
    """
    Classify a candidate message. Returns one of:
      'gem_alert'        — initial lagging call with contract address + ACHIEVED
      'milestone'        — follow-up Nx update (no contract address)
      'whale_alert'      — 🐋 whale wallet activity on a tracked token
      'dexscreener_paid' — token paid for DexScreener profile
      'boost_alert'      — team purchased Lightning Boost visibility boosts
      'noise'            — VIP promos, join links, unrecognised
    """
    # Whale checked first — whale messages also contain VIP links
    if IS_WHALE_ALERT_RE.search(text):
        return 'whale_alert'
    if IS_DEXSCREENER_PAID_RE.search(text):
        return 'dexscreener_paid'
    if IS_BOOST_ALERT_RE.search(text):
        return 'boost_alert'
    if IS_VIP_PROMO_RE.search(text):
        return 'noise'
    # Milestone checked before gem_alert: some milestone messages contain a
    # trailing "➔ ... (Nx ACHIEVED!)" blurb that would trigger IS_GEM_ALERT_RE
    # even though there is no contract address.
    if IS_MILESTONE_RE.search(text):
        return 'milestone'
    if IS_GEM_ALERT_RE.search(text):
        return 'gem_alert'
    return 'noise'


# ── Field extractors ──────────────────────────────────────────────────────────

# Labeled contract address / CA / Mint field
LABEL_MINT_RE = re.compile(
    r'\*{0,2}(?:Contract\s*Address|CA|Mint)\*{0,2}\s*[:\-]\s*'
    r'([1-9A-HJ-NP-Za-km-z]{32,44})',
    re.IGNORECASE,
)
# Fallback: any bare base58 string long enough to be a Solana address
BARE_MINT_RE = re.compile(r'[1-9A-HJ-NP-Za-km-z]{32,44}')

# **Ticker**: Mar1o  or  Ticker: Mar1o
LABEL_TICKER_RE = re.compile(
    r'\*{0,2}Ticker\*{0,2}\s*[:\-]\s*\$?(\S+)',
    re.IGNORECASE,
)
# Fallback: $TICKER
SYMBOL_FALLBACK_RE = re.compile(r'\$([A-Za-z][A-Za-z0-9]{1,9})')

# **Token Name**: Agent Mar1o
LABEL_NAME_RE = re.compile(
    r'\*{0,2}Token\s*Name\*{0,2}\s*[:\-]\s*(.+)',
    re.IGNORECASE,
)

# Market cap line:
#   **Market Cap**: 15.59K ([VIP Call](link)) ➔ 37.30K (2x ACHIEVED!)
# The optional (link) between entry and arrow is handled by [^➔→>]* lookahead.
MCAP_LINE_RE = re.compile(
    r'Market\s*Cap.*?:\s*'
    r'([\d.]+\s*[KMBkmb]?)'    # group 1 — entry mcap
    r'(?:[^➔→>]*?)'            # optional junk (e.g. VIP link)
    r'[➔→>]\s*'
    r'([\d.]+\s*[KMBkmb]?)'    # group 2 — result mcap
    r'\s*\(',
    re.IGNORECASE,
)

# Stated multiplier — e.g. (2x ACHIEVED!) or (37x ACHIEVED)
# Integer or .5 only, 1–3 digits — rejects garbled mcap values like (10634x ACHIEVED)
MULTIPLIER_RE = re.compile(r'\((\d{1,3}(?:\.5)?)x\s*ACHIEV', re.IGNORECASE)


# ── Multiplier validator ──────────────────────────────────────────────────────

def _is_valid_multiplier(v: float) -> bool:
    """
    Return True only for positive integers and .5 half-steps up to 999.5.
    Rejects arbitrary decimals (4.73x) and large outliers (10634x).
    Used as defense-in-depth after regex extraction.
    """
    if v <= 0 or v > 999:
        return False
    return abs((v * 2) - round(v * 2)) < 1e-9


# ── Mcap value converter ──────────────────────────────────────────────────────

_SUFFIX = {'k': 1_000, 'm': 1_000_000, 'b': 1_000_000_000}


def parse_mcap(value: str) -> float | None:
    """
    Convert a human-readable mcap string to a float.
    '15.59K' → 15590.0 | '1.2M' → 1_200_000.0 | '500' → 500.0
    Returns None if unparseable.
    """
    if not value:
        return None
    value = value.strip()
    m = re.match(r'^([\d.]+)\s*([KMBkmb]?)$', value)
    if not m:
        return None
    num = float(m.group(1))
    suffix = m.group(2).lower()
    return num * _SUFFIX.get(suffix, 1)


# ── Whale alert field patterns ────────────────────────────────────────────────

# Symbol — "**$PUMPERS**", plain "$PUMPERS", or digit-only tickers like "$5"
WHALE_SYMBOL_RE       = re.compile(r'\*{0,2}\$([A-Za-z0-9][A-Za-z0-9]{0,9})\*{0,2}')
# SOL spent — "1.27 SOL →"
WHALE_SOL_AMOUNT_RE   = re.compile(r'([\d.]+)\s*SOL\s*[→➔>]', re.IGNORECASE)
# Wallet total — "Wallet: 125 SOL"
WHALE_WALLET_RE       = re.compile(r'Wallet[:\s]+([\d,.]+)\s*SOL', re.IGNORECASE)
# MCap at buy — "MC: 82.30K"
WHALE_MCAP_RE         = re.compile(r'\bMC[:\s]+([\d.]+\s*[KMBkmb]?)', re.IGNORECASE)
# Signal count — "signal #2"
WHALE_SIGNAL_COUNT_RE = re.compile(r'signal\s*#(\d+)', re.IGNORECASE)

# ── DexScreener paid field patterns ───────────────────────────────────────────

DEX_NAME_RE   = re.compile(r'\*{0,2}Token\s*Name\*{0,2}[:\s]+(.+)',  re.IGNORECASE)
DEX_TICKER_RE = re.compile(r'\*{0,2}Ticker\*{0,2}[:\s]+\$?(\S+)',    re.IGNORECASE)


# ── Whale alert parse function ────────────────────────────────────────────────

def parse_whale_alert(text: str) -> dict | None:
    """
    Parse a 🐋 SolHouse Whale Alert message.
    Returns {symbol, sol_amount, wallet_size_sol, mcap_at_whale, signal_count}
    or None if the minimum fields (symbol) are missing.
    """
    symbol = None
    m = WHALE_SYMBOL_RE.search(text)
    if m:
        symbol = m.group(1).strip()
    if not symbol:
        return None

    sol_amount = None
    m = WHALE_SOL_AMOUNT_RE.search(text)
    if m:
        try:
            sol_amount = float(m.group(1).replace(',', ''))
        except ValueError:
            pass

    wallet_size_sol = None
    m = WHALE_WALLET_RE.search(text)
    if m:
        try:
            wallet_size_sol = float(m.group(1).replace(',', ''))
        except ValueError:
            pass

    mcap_at_whale = None
    m = WHALE_MCAP_RE.search(text)
    if m:
        mcap_at_whale = parse_mcap(m.group(1))

    signal_count = None
    m = WHALE_SIGNAL_COUNT_RE.search(text)
    if m:
        signal_count = int(m.group(1))

    return {
        "symbol":          symbol,
        "sol_amount":      sol_amount,
        "wallet_size_sol": wallet_size_sol,
        "mcap_at_whale":   mcap_at_whale,
        "signal_count":    signal_count,
    }


# ── DexScreener paid parse function ──────────────────────────────────────────

def parse_dexscreener_paid(text: str) -> dict | None:
    """
    Parse a DexScreener Paid Alert message.
    Returns {name, symbol} or None if neither field is found.
    """
    name = None
    m = DEX_NAME_RE.search(text)
    if m:
        name = m.group(1).strip()

    symbol = None
    m = DEX_TICKER_RE.search(text)
    if m:
        symbol = m.group(1).strip()

    if not name and not symbol:
        return None

    return {"name": name, "symbol": symbol}


# ── Lightning Boost parse function ───────────────────────────────────────────

def parse_boost_alert(text: str) -> dict | None:
    """
    Parse a Lightning Boost Alert message.
    Returns {name, symbol, boost_count} or None if no symbol found.
    """
    name = None
    m = DEX_NAME_RE.search(text)   # reuse Token Name pattern
    if m:
        name = m.group(1).strip()

    symbol = None
    m = DEX_TICKER_RE.search(text)  # reuse Ticker pattern
    if m:
        symbol = m.group(1).strip()

    boost_count = 0
    m = BOOST_COUNT_RE.search(text)
    if m:
        try:
            boost_count = int(m.group(1))
        except ValueError:
            pass

    if not symbol and not name:
        return None

    return {"name": name, "symbol": symbol, "boost_count": boost_count}


# ── Milestone parse function ──────────────────────────────────────────────────

def parse_milestone(text: str) -> dict | None:
    """
    Parse a milestone update message.
    Example: '🚀 PUMPERS has reached 4x from our call!\nInitial MC: 25.62K ➔ Now: 109.23K'

    Returns:
        {token_name, entry_mcap, current_mcap, multiplier_stated, multiplier_computed}
    or None if required fields are missing.

    NOTE: No mint address is in these messages. The caller must resolve the
    original call_id by looking up token_name in the calls/tokens tables.
    """
    if not is_milestone_message(text):
        return None

    # Token name
    m = MILESTONE_NAME_RE.search(text)
    token_name = m.group(1).strip() if m else None

    # Stated multiplier from "has reached 4x"
    m = MILESTONE_MULT_RE.search(text)
    multiplier_stated = float(m.group(1)) if m else None
    if multiplier_stated is not None and not _is_valid_multiplier(multiplier_stated):
        multiplier_stated = None  # parse error — store NULL not garbage

    # Entry and current mcap
    entry_mcap   = None
    current_mcap = None
    m = MILESTONE_MCAP_RE.search(text)
    if m:
        entry_mcap   = parse_mcap(m.group(1))
        current_mcap = parse_mcap(m.group(2))

    # Computed multiplier (more accurate than stated)
    multiplier_computed = None
    if entry_mcap and current_mcap and entry_mcap > 0:
        multiplier_computed = round(current_mcap / entry_mcap, 2)

    if not token_name or not multiplier_stated:
        return None

    return {
        "token_name":          token_name,
        "entry_mcap":          entry_mcap,
        "current_mcap":        current_mcap,
        "multiplier_stated":   multiplier_stated,
        "multiplier_computed": multiplier_computed,
    }


# ── Gem alert parse function ──────────────────────────────────────────────────

def parse(text: str) -> dict | None:
    """
    Parse a Type A (lagging) message.

    Returns a dict on success, or None if required fields are missing.

    Required: mint address + at least one mcap value.
    Optional: symbol, name, stated_multiplier.
    """
    if not is_lagging_message(text):
        return None

    # ── Mint address ──
    mint = None
    m = LABEL_MINT_RE.search(text)
    if m:
        mint = m.group(1)
    else:
        for candidate in BARE_MINT_RE.findall(text):
            if len(candidate) >= 32:
                mint = candidate
                break
    if not mint:
        return None

    # ── Symbol ──
    symbol = None
    m = LABEL_TICKER_RE.search(text)
    if m:
        symbol = m.group(1).strip()
    else:
        m = SYMBOL_FALLBACK_RE.search(text)
        if m:
            symbol = m.group(1).strip()

    # ── Token name ──
    name = None
    m = LABEL_NAME_RE.search(text)
    if m:
        name = m.group(1).strip()

    # ── Mcap values ──
    mcap_at_call   = None
    mcap_at_result = None
    m = MCAP_LINE_RE.search(text)
    if m:
        mcap_at_call   = parse_mcap(m.group(1))
        mcap_at_result = parse_mcap(m.group(2))

    # Require at least the entry mcap
    if mcap_at_call is None:
        return None

    # ── Stated multiplier ──
    stated_multiplier = None
    m = MULTIPLIER_RE.search(text)
    if m:
        v = float(m.group(1))
        stated_multiplier = v if _is_valid_multiplier(v) else None  # NULL on parse error

    # ── Computed (accurate) multiplier ──
    peak_multiplier = None
    if mcap_at_call and mcap_at_result and mcap_at_call > 0:
        peak_multiplier = round(mcap_at_result / mcap_at_call, 2)

    return {
        "mint":             mint,
        "symbol":           symbol,
        "name":             name,
        "mcap_at_call":     mcap_at_call,
        "mcap_at_result":   mcap_at_result,
        "stated_multiplier": stated_multiplier,
        "peak_multiplier":  peak_multiplier,
        "is_lagging":       True,
    }
