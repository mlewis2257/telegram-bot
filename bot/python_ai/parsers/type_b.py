"""
Type B parser — Realtime channels (@solwhaletrending, @solearlytrending).

Six message types:

  entry_signal_whale    — "New Whale Buy!" (solwhaletrending)
    Has wallet size + SOL spent. Mint embedded in chart URL.

  entry_signal_trending — "New Trending" (solearlytrending)
    No wallet/SOL fields. Mint embedded in chart URL.

  another_whale         — "Another Whale Aped $SYMBOL!"
    Second/third whale buying an already-signalled token.
    Handled as a new whale_alert row linked to existing call.

  price_update          — "📈 SYMBOL is up NX/N% ... from ⚡️ Entry Signal"
    Links back to original call_id by symbol.

  daily_stats           — "24hr Whale Trending Stats"
  leaderboard           — "Top Whale Trending"

Mint extraction priority (all entry signal types):
  1. Soul_Sniper_Bot URL — start=56_wtb_{MINT} or start=15_etb_{MINT}
  2. GeckoTerminal URL   — /solana/tokens/{MINT}
  3. DexScreener URL     — /solana/{MINT}
  Fallback: caller stores UNKNOWN:{SYMBOL} with mint_resolved=FALSE
"""

import re

# =============================================================================
# Markdown link stripping
# =============================================================================

_MARKDOWN_LINK_RE = re.compile(r'\[([^\]]+)\]\([^)]+\)')


def strip_markdown_links(text: str) -> str:
    """Replace '[TokenName](url)' → 'TokenName' throughout the string."""
    return _MARKDOWN_LINK_RE.sub(r'\1', text)


# Matches the Soul_Sniper_Bot hyperlink that wraps the token name in entry signals:
# "[**\u200eToken Name**](https://t.me/Soul_Sniper_Bot?start=...)"
_NAME_LINK_RE = re.compile(
    r'\[([^\]]+)\]\(https://t\.me/Soul_Sniper_Bot',
    re.IGNORECASE,
)


def strip_entry_name(text: str) -> str | None:
    """
    Extract the clean token name from the Soul_Sniper_Bot hyperlink present
    in all entry signal messages (both whale and trending channels).
    Returns None if the link is not found.
    """
    m = _NAME_LINK_RE.search(text)
    if not m:
        return None
    # Strip bold markers (**), LRM (\u200e), RLM (\u200f), and whitespace
    name = (
        m.group(1)
        .replace('**', '')
        .replace('\u200e', '')
        .replace('\u200f', '')
        .strip()
    )
    return name or None


# =============================================================================
# Classification
# =============================================================================

IS_ANOTHER_WHALE_RE      = re.compile(r'Another Whale Aped',       re.IGNORECASE)
IS_WHALE_ACCUMULATING_RE = re.compile(r'Whale Accumulating',        re.IGNORECASE)
IS_ENTRY_WHALE_RE        = re.compile(r'New\s+\*{0,2}\[?\*{0,2}Whale Buy',   re.IGNORECASE)
IS_ENTRY_TRENDING_RE     = re.compile(r'New\s+\*{0,2}\[?\*{0,2}Trending',     re.IGNORECASE)
IS_PRICE_UPDATE_RE       = re.compile(r'📈.*is up.*from.*Entry Signal',
                                       re.IGNORECASE | re.DOTALL)
IS_DAILY_STATS_RE        = re.compile(r'24hr Whale Trending Stats', re.IGNORECASE)
IS_LEADERBOARD_RE        = re.compile(r'Top (?:Early |Whale )?Trending',
                                       re.IGNORECASE)


def classify_b(text: str) -> str:
    """
    Returns one of:
      'entry_signal_whale'    'entry_signal_trending'
      'another_whale'         'price_update'
      'daily_stats'           'leaderboard'
      'noise'
    """
    # another_whale / whale_accumulating checked first — both map to same handler
    if IS_ANOTHER_WHALE_RE.search(text) or IS_WHALE_ACCUMULATING_RE.search(text):
        return 'another_whale'
    if IS_ENTRY_WHALE_RE.search(text):
        return 'entry_signal_whale'
    if IS_ENTRY_TRENDING_RE.search(text):
        return 'entry_signal_trending'
    if IS_PRICE_UPDATE_RE.search(text):
        return 'price_update'
    if IS_DAILY_STATS_RE.search(text):
        return 'daily_stats'
    if IS_LEADERBOARD_RE.search(text):
        return 'leaderboard'
    return 'noise'


# =============================================================================
# Shared helpers
# =============================================================================

_SUFFIX = {'k': 1_000, 'm': 1_000_000, 'b': 1_000_000_000}


def _parse_value(s: str) -> float | None:
    """
    Parse "$81,324", "$22.4K", "55.1K", "1.2M" → float.
    Strips leading "$" and commas.
    """
    if not s:
        return None
    s = s.strip().lstrip('$').replace(',', '').strip()
    m = re.match(r'^([\d.]+)\s*([KMBkmb]?)$', s)
    if not m:
        return None
    num = float(m.group(1))
    return num * _SUFFIX.get(m.group(2).lower(), 1)


def _parse_age_minutes(value: str, unit: str) -> int | None:
    """Normalise age string to integer minutes. '52s'→0, '2m'→2, '1h'→60."""
    try:
        v = int(value)
    except (ValueError, TypeError):
        return None
    u = unit.lower()
    if u == 's':
        return v // 60          # 52s → 0 min
    if u == 'm':
        return v
    if u == 'h':
        return v * 60
    if u == 'd':
        return v * 1440
    return None


# =============================================================================
# Mint extraction
# =============================================================================

# Priority 1 — Soul_Sniper_Bot link always carries the clean mint
MINT_SNIPER_RE = re.compile(
    r'start=(?:56_wtb_|15_etb_)([1-9A-HJ-NP-Za-km-z]{32,44})'
)
# Priority 2 — GeckoTerminal token page
MINT_GECKO_RE  = re.compile(
    r'geckoterminal\.com/solana/tokens/([1-9A-HJ-NP-Za-km-z]{32,44})'
)
# Priority 3 — DexScreener token/pair page
MINT_DEX_RE    = re.compile(
    r'dexscreener\.com/solana/([1-9A-HJ-NP-Za-km-z]{32,44})'
)


def _extract_mint(text: str) -> str | None:
    """Return the first mint address found via the priority chain, or None."""
    for pattern in (MINT_SNIPER_RE, MINT_GECKO_RE, MINT_DEX_RE):
        m = pattern.search(text)
        if m:
            return m.group(1)
    return None


# =============================================================================
# Entry signal field patterns (shared between whale and trending)
# =============================================================================

# Symbol — first letter-starting $WORD; avoids matching "$81,324" etc.
SYMBOL_RE        = re.compile(r'\$([A-Za-z][A-Za-z0-9]{0,14})')

# Age: "Age: 2m" or "**Age**: 12m" (solearlytrending uses bold markers)
AGE_RE           = re.compile(r'Age\*{0,2}:\s*(\d+)([smhd])', re.IGNORECASE)

# Security: "Security: ✅" or "**Security**: ✅" (solearlytrending uses bold markers)
SECURITY_RE      = re.compile(r'Security\*{0,2}:\s*([✅⚠️⚠]+)', re.UNICODE)

# Whale wallet size (whale entry signals only): "Wallet: 184 SOL"
WALLET_RE        = re.compile(r'Wallet:\s*\*{0,2}([\d,.]+)\s*SOL', re.IGNORECASE)

# SOL spent (whale entry signals + another_whale): "1.48 SOL →"
SOL_SPENT_RE     = re.compile(r'([\d.]+)\s*SOL\s*→')

# Market cap: "MC: $81,324" — takes first value before any bullet/peak
MC_RE            = re.compile(r'MC:\s*\$([\d,]+(?:\.\d+)?[KMBkmb]?)', re.IGNORECASE)

# Liquidity
LIQ_RE           = re.compile(r'Liq\*{0,2}:\*{0,2}\s*\$([\d,]+(?:\.\d+)?[KMBkmb]?)', re.IGNORECASE)

# 1h volume — handles both:
#   "Vol: __1h__: $26.3K"   (solearlytrending)
#   "Vol: $20.4K [1h]"      (another_whale / solwhaletrending)
# No DOTALL so .* stays on the same line.
VOL_RE           = re.compile(r'Vol:.*?\$([\d,]+(?:\.\d+)?[KMBkmb]?)', re.IGNORECASE)

# Hodl count + optional CTO flag
HODL_RE          = re.compile(r'Hodls\*{0,2}:\*{0,2}\s*(\d+)', re.IGNORECASE)
CTO_RE           = re.compile(r'Hodls.*?CTO',                  re.IGNORECASE)

# Bundles: "/Bundles: 9 • 50% → 6.8%"
BUNDLES_RE       = re.compile(
    r'/Bundles\*{0,2}:\s*(\d+)\s*•\s*([\d.]+)%\s*→\s*([\d.]+)%', re.IGNORECASE
)

# Snipers: "Snipers: 34 • 35% → 13.2%" or "**Snipers**: 48 • 49% → 0.4%"
SNIPERS_RE       = re.compile(
    r'Snipers\*{0,2}:\s*(\d+)\s*•\s*([\d.]+)%\s*→\s*([\d.]+)%', re.IGNORECASE
)

# First 20
FIRST_20_RE      = re.compile(r'First 20\*{0,4}:\*{0,2}\s*([\d.]+)%', re.IGNORECASE)

# Dev holdings: "Dev: 38 SOL | 0% $SYMBOL"
DEV_RE           = re.compile(r'Dev\*{0,2}:\*{0,2}\s*([\d.]+)\s*SOL\s*\|\s*([\d.]+)%', re.IGNORECASE)

# Dev history line: "Made: 75879 | Bond: 420 | Best: $21.1M"
DEV_HISTORY_RE   = re.compile(
    r'Made:\s*(\d+)\s*\|\s*Bond:\s*(\d+)\s*\|\s*Best:\s*\$([\d,.]+[KMBkmb]?)',
    re.IGNORECASE,
)

# Bundled + sold: "Bundled: 24% ⚠️ | Sold: 24% 🔴"
BUNDLED_RE       = re.compile(r'Bundled:\s*([\d.]+)%.*?Sold:\s*([\d.]+)%', re.IGNORECASE)

# Fake volume: "Fake: $1.1K [2%]"
FAKE_VOL_RE      = re.compile(r'Fake\*{0,2}:\s*\$([\d,.]+[KMBkmb]?)\s*\[([\d.]+)%\]', re.IGNORECASE)


def _parse_entry_fields(text: str, is_whale: bool) -> dict | None:
    """
    Extract all fields common to both entry signal subtypes.
    is_whale=True also extracts wallet size and SOL spent.
    Returns None if no symbol found.
    """
    # ── Mint — extracted from raw text (needs the URLs intact) ──
    mint = _extract_mint(text)

    # ── Strip markdown links for all text-field extraction ──
    # "[TokenName](url) New Whale Buy!" → "TokenName New Whale Buy!"
    clean = strip_markdown_links(text)

    # ── Token name ──
    name = strip_entry_name(text)   # extracts from Soul_Sniper_Bot link in raw text

    # ── Symbol ──
    symbol = None
    m = SYMBOL_RE.search(clean)
    if m:
        symbol = m.group(1).strip()
    if not symbol:
        return None

    # ── Age ──
    token_age_minutes = None
    m = AGE_RE.search(clean)
    if m:
        token_age_minutes = _parse_age_minutes(m.group(1), m.group(2))

    # ── Security ──
    security_flag = 'unknown'
    m = SECURITY_RE.search(clean)
    if m:
        raw = m.group(1)
        security_flag = 'safe' if '✅' in raw else 'warning'

    # ── CTO ──
    is_cto = bool(CTO_RE.search(clean))

    # ── Whale wallet (whale type only) ──
    detecting_wallet_sol = None
    detecting_sol_spent  = None
    if is_whale:
        m = WALLET_RE.search(clean)
        if m:
            try:
                detecting_wallet_sol = float(m.group(1).replace(',', ''))
            except ValueError:
                pass
        m = SOL_SPENT_RE.search(clean)
        if m:
            try:
                detecting_sol_spent = float(m.group(1))
            except ValueError:
                pass

    # ── Market cap ──
    mcap_at_call = None
    m = MC_RE.search(clean)
    if m:
        mcap_at_call = _parse_value(m.group(1))

    # ── Liquidity ──
    liq_at_detection = None
    m = LIQ_RE.search(clean)
    if m:
        liq_at_detection = _parse_value(m.group(1))

    # ── Volume ──
    vol_1h_at_detection = None
    m = VOL_RE.search(clean)
    if m:
        vol_1h_at_detection = _parse_value(m.group(1))

    # ── Hodl count ──
    hodl_count = None
    m = HODL_RE.search(clean)
    if m:
        try:
            hodl_count = int(m.group(1))
        except ValueError:
            pass

    # ── Bundles ──
    bundle_count = bundle_pct_initial = bundle_pct_remaining = None
    m = BUNDLES_RE.search(clean)
    if m:
        try:
            bundle_count         = int(m.group(1))
            bundle_pct_initial   = float(m.group(2))
            bundle_pct_remaining = float(m.group(3))
        except ValueError:
            pass

    # ── Snipers ──
    sniper_count = sniper_pct_initial = sniper_pct_remaining = None
    m = SNIPERS_RE.search(clean)
    if m:
        try:
            sniper_count         = int(m.group(1))
            sniper_pct_initial   = float(m.group(2))
            sniper_pct_remaining = float(m.group(3))
        except ValueError:
            pass

    # ── First 20 ──
    first_20_pct = None
    m = FIRST_20_RE.search(clean)
    if m:
        try:
            first_20_pct = float(m.group(1))
        except ValueError:
            pass

    # ── Dev holdings ──
    dev_sol_held = dev_pct_held = dev_sold = None
    m = DEV_RE.search(clean)
    if m:
        try:
            dev_sol_held = float(m.group(1))
            dev_pct_held = float(m.group(2))
            dev_sold     = (dev_pct_held == 0.0)
        except ValueError:
            pass

    # ── Dev history ──
    dev_tokens_made = dev_bonds = dev_best_mcap = None
    m = DEV_HISTORY_RE.search(clean)
    if m:
        try:
            dev_tokens_made = int(m.group(1))
            dev_bonds       = int(m.group(2))
            dev_best_mcap   = _parse_value(m.group(3))
        except ValueError:
            pass

    # ── Bundled / sold ──
    bundled_pct = bundled_sold_pct = None
    m = BUNDLED_RE.search(clean)
    if m:
        try:
            bundled_pct      = float(m.group(1))
            bundled_sold_pct = float(m.group(2))
        except ValueError:
            pass

    # ── Fake volume ──
    fake_vol_usd = fake_vol_pct = None
    m = FAKE_VOL_RE.search(clean)
    if m:
        try:
            fake_vol_usd = _parse_value(m.group(1))
            fake_vol_pct = float(m.group(2))
        except ValueError:
            pass

    return {
        "mint":                 mint,          # None → caller stores UNKNOWN:
        "name":                 name,
        "symbol":               symbol,
        "mcap_at_call":         mcap_at_call,
        "token_age_minutes":    token_age_minutes,
        "security_flag":        security_flag,
        "is_cto":               is_cto,
        "detecting_wallet_sol": detecting_wallet_sol,
        "detecting_sol_spent":  detecting_sol_spent,
        "liq_at_detection":     liq_at_detection,
        "vol_1h_at_detection":  vol_1h_at_detection,
        "hodl_count":           hodl_count,
        "bundle_count":         bundle_count,
        "bundle_pct_initial":   bundle_pct_initial,
        "bundle_pct_remaining": bundle_pct_remaining,
        "sniper_count":         sniper_count,
        "sniper_pct_initial":   sniper_pct_initial,
        "sniper_pct_remaining": sniper_pct_remaining,
        "first_20_pct":         first_20_pct,
        "dev_sol_held":         dev_sol_held,
        "dev_pct_held":         dev_pct_held,
        "dev_sold":             dev_sold,
        "dev_tokens_made":      dev_tokens_made,
        "dev_bonds":            dev_bonds,
        "dev_best_mcap":        dev_best_mcap,
        "bundled_pct":          bundled_pct,
        "bundled_sold_pct":     bundled_sold_pct,
        "fake_vol_usd":         fake_vol_usd,
        "fake_vol_pct":         fake_vol_pct,
    }


def parse_entry_signal_whale(text: str) -> dict | None:
    """Parse a 'New Whale Buy!' entry signal. Returns None if no symbol found."""
    return _parse_entry_fields(text, is_whale=True)


def parse_entry_signal_trending(text: str) -> dict | None:
    """Parse a 'New Trending' entry signal. Returns None if no symbol found."""
    return _parse_entry_fields(text, is_whale=False)


# =============================================================================
# Another Whale patterns
# =============================================================================

ANOTHER_WHALE_SYMBOL_RE = re.compile(
    r'Another Whale Aped\s+\$([A-Za-z][A-Za-z0-9]{0,14})!',
    re.IGNORECASE,
)


def parse_another_whale(text: str) -> dict | None:
    """
    Parse an 'Another Whale Aped' follow-up message.
    Returns {symbol, mint, wallet_sol, sol_spent, mcap, vol_1h, hodl_count,
             token_age_minutes}
    or None if symbol is missing.
    """
    m = ANOTHER_WHALE_SYMBOL_RE.search(text)
    if not m:
        return None
    symbol = m.group(1).strip()

    mint = _extract_mint(text)

    wallet_sol = None
    m = WALLET_RE.search(text)
    if m:
        try:
            wallet_sol = float(m.group(1).replace(',', ''))
        except ValueError:
            pass

    sol_spent = None
    m = SOL_SPENT_RE.search(text)
    if m:
        try:
            sol_spent = float(m.group(1))
        except ValueError:
            pass

    mcap = None
    m = MC_RE.search(text)
    if m:
        mcap = _parse_value(m.group(1))

    vol_1h = None
    m = VOL_RE.search(text)
    if m:
        vol_1h = _parse_value(m.group(1))

    hodl_count = None
    m = HODL_RE.search(text)
    if m:
        try:
            hodl_count = int(m.group(1))
        except ValueError:
            pass

    token_age_minutes = None
    m = AGE_RE.search(text)
    if m:
        token_age_minutes = _parse_age_minutes(m.group(1), m.group(2))

    return {
        "symbol":             symbol,
        "mint":               mint,
        "wallet_sol":         wallet_sol,
        "sol_spent":          sol_spent,
        "mcap":               mcap,
        "vol_1h":             vol_1h,
        "hodl_count":         hodl_count,
        "token_age_minutes":  token_age_minutes,
    }


# =============================================================================
# Price update patterns
# =============================================================================

# Symbol is the first non-whitespace token after the leading 📈
UPDATE_SYMBOL_RE = re.compile(r'📈\s*(\S+)\s+is up')
UPDATE_MULT_RE   = re.compile(r'is up\s+([\d.]+)X',  re.IGNORECASE)
UPDATE_PCT_RE    = re.compile(r'is up\s+([\d.]+)%',  re.IGNORECASE)
# "$12K —> $25.4K" — handles em-dash, en-dash, double-hyphen, plain arrow
UPDATE_MCAP_RE   = re.compile(
    r'\$([\d,]+(?:\.\d+)?[KMBkmb]?)\s*(?:—>|–>|-->|→|->)\s*'
    r'\$([\d,]+(?:\.\d+)?[KMBkmb]?)',
    re.IGNORECASE,
)


def parse_price_update(text: str) -> dict | None:
    """
    Parse a '📈 SYMBOL is up NX/N%' update message.
    Returns {symbol, pct_change, multiplier, entry_mcap, current_mcap}
    or None if symbol is missing.
    """
    m = UPDATE_SYMBOL_RE.search(text)
    if not m:
        return None
    symbol = m.group(1).strip()

    multiplier = None
    m = UPDATE_MULT_RE.search(text)
    if m:
        try:
            multiplier = float(m.group(1))
        except ValueError:
            pass

    pct_change = None
    m = UPDATE_PCT_RE.search(text)
    if m:
        try:
            pct_change = float(m.group(1))
        except ValueError:
            pass

    entry_mcap = current_mcap = None
    m = UPDATE_MCAP_RE.search(text)
    if m:
        entry_mcap   = _parse_value(m.group(1))
        current_mcap = _parse_value(m.group(2))

    # Compute multiplier from mcaps if not stated explicitly
    if multiplier is None and entry_mcap and current_mcap and entry_mcap > 0:
        multiplier = round(current_mcap / entry_mcap, 2)

    return {
        "symbol":       symbol,
        "pct_change":   pct_change,
        "multiplier":   multiplier,
        "entry_mcap":   entry_mcap,
        "current_mcap": current_mcap,
    }


# =============================================================================
# Daily stats patterns (TYPE 4)
# =============================================================================

STATS_ENTRY_RE      = re.compile(r'Entry Signals:\s*(\d+)',         re.IGNORECASE)
STATS_WINRATE_RE    = re.compile(r'\+50%\s+Win Rate:\s*([\d.]+)%',  re.IGNORECASE)
STATS_AVGPROFIT_RE  = re.compile(r'Wins Avg Profit:\s*([\d.]+)X',   re.IGNORECASE)
STATS_BEST_RE       = re.compile(r'Best:\s*(\S+)\s+([\d.]+)X',      re.IGNORECASE)
STATS_2X_RE         = re.compile(r'\b2X:\s*(\d+)',                  re.IGNORECASE)
STATS_5X_RE         = re.compile(r'\b5X:\s*(\d+)',                  re.IGNORECASE)
STATS_10X_RE        = re.compile(r'\b10X:\s*(\d+)',                 re.IGNORECASE)
STATS_15X_RE        = re.compile(r'\b15X\+:\s*(\d+)',               re.IGNORECASE)


def parse_daily_stats(text: str) -> dict | None:
    """
    Parse a TYPE 4 daily stats summary message.
    Returns a dict of all parsed stats, or None if no recognisable fields found.
    """
    def _int(pat):
        m = pat.search(text)
        try:
            return int(m.group(1)) if m else None
        except ValueError:
            return None

    def _float(pat):
        m = pat.search(text)
        try:
            return float(m.group(1)) if m else None
        except (ValueError, AttributeError):
            return None

    entry_signals   = _int(STATS_ENTRY_RE)
    win_rate_50pct  = _float(STATS_WINRATE_RE)
    wins_avg_profit = _float(STATS_AVGPROFIT_RE)

    best_token = best_multiplier = None
    m = STATS_BEST_RE.search(text)
    if m:
        best_token      = m.group(1).strip()
        best_multiplier = float(m.group(2))

    alerts_2x       = _int(STATS_2X_RE)
    alerts_5x       = _int(STATS_5X_RE)
    alerts_10x      = _int(STATS_10X_RE)
    alerts_15x_plus = _int(STATS_15X_RE)

    if not any([entry_signals, win_rate_50pct, wins_avg_profit, best_token, alerts_2x]):
        return None

    return {
        "entry_signals":   entry_signals,
        "win_rate_50pct":  win_rate_50pct,
        "wins_avg_profit": wins_avg_profit,
        "best_token":      best_token,
        "best_multiplier": best_multiplier,
        "alerts_2x":       alerts_2x,
        "alerts_5x":       alerts_5x,
        "alerts_10x":      alerts_10x,
        "alerts_15x_plus": alerts_15x_plus,
    }


# =============================================================================
# Leaderboard patterns (TYPE 5)
# =============================================================================

LEADERBOARD_ENTRY_RE = re.compile(r'\$([A-Za-z0-9]+)\s*•\s*([\d.]+)X', re.IGNORECASE)


def parse_leaderboard(text: str) -> list | None:
    """
    Parse a TYPE 5 top performers leaderboard message.
    Returns [{symbol, multiplier}, ...] or None if no entries found.
    """
    entries = []
    for sym, mult in LEADERBOARD_ENTRY_RE.findall(text):
        try:
            entries.append({"symbol": sym.strip(), "multiplier": float(mult)})
        except ValueError:
            pass
    return entries if entries else None
