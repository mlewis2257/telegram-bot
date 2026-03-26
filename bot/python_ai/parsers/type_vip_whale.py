"""
type_vip_whale.py — VIP channel Whale Alert parser.

Handles 🦅 SolHouse Whale Alert messages from the SolHouse VIP channel.

Expected message format:
  🦅 SolHouse Whale Alert

  Token: $SOME | SomeToken
  Contract: ABC123...pump
  Market Cap: 25.5K
  Wallet: 500 SOL
  Spent: 1.5 SOL (0.3%)

Public API
----------
is_whale_alert(text: str) -> bool
parse(text: str)           -> dict | None
    {mint, symbol, token_name, mcap_at_call,
     whale_wallet_sol, whale_sol_spent, whale_pct_spent}
"""

import re

# ── Detection ─────────────────────────────────────────────────────────────────

IS_WHALE_ALERT_RE = re.compile(r'🦅\s*SolHouse\s+Whale\s+Alert', re.IGNORECASE)


def is_whale_alert(text: str) -> bool:
    return bool(IS_WHALE_ALERT_RE.search(text))


# ── Field patterns ────────────────────────────────────────────────────────────

# Contract / CA / Mint address (labeled)
MINT_LABEL_RE = re.compile(
    r'(?:Contract(?:\s*Address)?|CA|Mint)\s*[:\-]?\s*'
    r'([1-9A-HJ-NP-Za-km-z]{32,44})',
    re.IGNORECASE,
)
# Fallback: bare base58 address
BARE_MINT_RE = re.compile(r'[1-9A-HJ-NP-Za-km-z]{32,44}')

# Token: $SOME | SomeToken  — symbol before the pipe
TICKER_RE = re.compile(
    r'(?:Token|Ticker|Symbol)\s*[:\-]\s*\$([A-Za-z][A-Za-z0-9]{0,14})',
    re.IGNORECASE,
)
# Fallback: $TICKER anywhere
SYMBOL_FALLBACK_RE = re.compile(r'\$([A-Za-z][A-Za-z0-9]{1,14})')

# Token name: text after "Token: $SYMBOL |"
NAME_RE = re.compile(
    r'Token\s*[:\-]\s*\$[A-Za-z0-9]+\s*\|\s*(.+)',
    re.IGNORECASE,
)

# Market Cap: 25.5K
MCAP_RE = re.compile(
    r'Market\s*Cap\s*[:\-]\s*([\d.]+\s*[KMBkmb]?)',
    re.IGNORECASE,
)

# Wallet: 500 SOL
WALLET_RE = re.compile(
    r'Wallet\s*[:\-]\s*([\d,.]+)\s*SOL',
    re.IGNORECASE,
)

# Spent: 1.5 SOL (0.3%)   — percentage is optional
SPENT_RE = re.compile(
    r'Spent\s*[:\-]\s*([\d.]+)\s*SOL(?:\s*\((\d+\.?\d*)%\))?',
    re.IGNORECASE,
)


# ── Value converter ───────────────────────────────────────────────────────────

_SUFFIX = {'k': 1_000, 'm': 1_000_000, 'b': 1_000_000_000}


def _parse_k(value: str) -> float | None:
    """Convert '25.5K' → 25500.0 | '1.2M' → 1200000.0 | '500' → 500.0"""
    if not value:
        return None
    value = value.strip()
    m = re.match(r'^([\d.]+)\s*([KMBkmb]?)$', value)
    if not m:
        return None
    num = float(m.group(1))
    suffix = m.group(2).lower()
    return num * _SUFFIX.get(suffix, 1)


# ── Public API ────────────────────────────────────────────────────────────────

def parse(text: str) -> dict | None:
    """
    Parse a 🦅 SolHouse Whale Alert VIP message.

    Returns a dict on success, or None if no symbol is found.
    """
    if not is_whale_alert(text):
        return None

    # ── Mint address ──
    mint = None
    m = MINT_LABEL_RE.search(text)
    if m:
        mint = m.group(1)
    else:
        for candidate in BARE_MINT_RE.findall(text):
            if len(candidate) >= 32:
                mint = candidate
                break

    # ── Symbol ──
    symbol = None
    m = TICKER_RE.search(text)
    if m:
        symbol = m.group(1).strip()
    else:
        m = SYMBOL_FALLBACK_RE.search(text)
        if m:
            symbol = m.group(1).strip()

    if not symbol:
        return None

    # ── Token name ──
    token_name = None
    m = NAME_RE.search(text)
    if m:
        token_name = m.group(1).strip()

    # ── Market cap ──
    mcap_at_call = None
    m = MCAP_RE.search(text)
    if m:
        mcap_at_call = _parse_k(m.group(1))

    # ── Whale wallet size ──
    whale_wallet_sol = None
    m = WALLET_RE.search(text)
    if m:
        try:
            whale_wallet_sol = float(m.group(1).replace(',', ''))
        except ValueError:
            pass

    # ── SOL spent + optional pct ──
    whale_sol_spent = None
    whale_pct_spent = None
    m = SPENT_RE.search(text)
    if m:
        try:
            whale_sol_spent = float(m.group(1))
        except ValueError:
            pass
        if m.group(2):
            try:
                whale_pct_spent = float(m.group(2))
            except ValueError:
                pass

    return {
        "mint":             mint,
        "symbol":           symbol,
        "token_name":       token_name,
        "mcap_at_call":     mcap_at_call,
        "whale_wallet_sol": whale_wallet_sol,
        "whale_sol_spent":  whale_sol_spent,
        "whale_pct_spent":  whale_pct_spent,
    }
