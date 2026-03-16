"""
Narrative tagging engine.

Assigns one or more thematic tags to a token or message based on keyword
patterns. Tags are used by the scoring engine to weight calls by narrative
momentum (e.g. AI meta, Trump cycle, dog season).

Public API
----------
tag_token(symbol, name, text="") -> list[str]
    Combine symbol + name + raw message text and return all matching tags.

tag_message(text) -> list[str]
    Tag a raw message string directly (lower-level helper).

Each function returns a sorted, deduplicated list of lowercase tag strings.
An empty list means no recognised narrative was detected.
"""

import re

# =============================================================================
# Tag definitions
# Each entry: (tag_name, compiled_regex)
# Pattern is matched case-insensitively against the combined input string.
# =============================================================================

_TAGS: list[tuple[str, re.Pattern]] = []


def _tag(name: str, pattern: str) -> None:
    _TAGS.append((name, re.compile(pattern, re.IGNORECASE)))


# ── AI / tech ─────────────────────────────────────────────────────────────────
_tag("ai",
     r'\bai\b|artificial\s+intel|'
     r'\bgpt\b|\bllm\b|\bagi\b|neural|deepseek|openai|'
     r'\bgrok\b|skynet|robot|chatbot|\bbot\b(?!.*sniper)')

# ── Trump / MAGA ──────────────────────────────────────────────────────────────
_tag("trump",
     r'\btrump\b|maga\b|donald\s+trump|potus|melania|barron\s+trump|'
     r'make\s+america|inaugur')

# ── Elon Musk ─────────────────────────────────────────────────────────────────
_tag("elon",
     r'\belon\b|\bmusk\b|tesla|spacex|\bxai\b|'
     r'doge.*elon|elon.*doge|x\.com')

# ── Dog / canine ──────────────────────────────────────────────────────────────
# \binu\b kept here for shib/floki-style names; also caught by 'inu' tag below
_tag("dog",
     r'\bdog\b|\bdoge\b|\bshib\b|shiba\b|puppy|doggo|\bwoof\b|'
     r'canine|hound|\bfloki\b|\bhusky\b|\bpoodle\b')

# ── Cat / feline ──────────────────────────────────────────────────────────────
_tag("cat",
     r'\bcat\b|kitty|kitten|nyan|meow|feline|\bpussy\b|\btabby\b')

# ── Pepe / meme culture ───────────────────────────────────────────────────────
_tag("pepe",
     r'\bpepe\b|\bfrog\b|\bkek\b|wojak|\bchad\b|\bbased\b(?!\s+chain)')

# ── Gaming / play-to-earn ─────────────────────────────────────────────────────
_tag("gaming",
     r'\bgame\b|gaming|\bplay\b|rpg|\bnft\b|metaverse|pixel|'
     r'minecraft|fortnite|arcade|\bquest\b')

# ── Military / war ────────────────────────────────────────────────────────────
_tag("military",
     r'military|army\b|soldier|warfare|\bwar\b|'
     r'\btank\b|weapon|ammo\b|munition|\bnato\b|veteran')

# ── Anime / manga ─────────────────────────────────────────────────────────────
_tag("anime",
     r'\banime\b|manga\b|waifu|\bnaruto\b|\bone\s+piece\b|isekai|'
     r'kawaii|otaku|sensei|\bsama\b|dragonball|goku\b')

# ── Food ──────────────────────────────────────────────────────────────────────
_tag("food",
     r'pizza|burger|taco|donut|doughnut|cake\b|'
     r'coffee|noodle|sushi|ramen|nacho|sandwich|nugget|pretzel')

# ── Baby ──────────────────────────────────────────────────────────────────────
# "baby X" pattern is very common in meme coins (baby doge, baby shib, …)
_tag("baby",
     r'\bbaby\b')

# ── Inu suffix ────────────────────────────────────────────────────────────────
# Matches any token whose symbol/name ends with "inu" (shib, floki variants)
_tag("inu",
     r'\binu\b|inu$')

# ── DeFi / yield ──────────────────────────────────────────────────────────────
_tag("defi",
     r'\byield\b|liquidity\b|\bstake\b|staking|farming\b|'
     r'\bvault\b|\bprotocol\b|\bswap\b|\bpool\b|\bapy\b|\bapr\b')

# ── Sports ────────────────────────────────────────────────────────────────────
_tag("sports",
     r'football|soccer|\bnba\b|\bnfl\b|\bnhl\b|\bmlb\b|cricket|tennis|'
     r'basketball|baseball|\bfifa\b|champion[s]?\s+league')

# ── Celebrity / pop culture ───────────────────────────────────────────────────
_tag("celebrity",
     r'\bsnoop\b|\bkanye\b|\btaylor\s+swift\b|\bbiden\b|\bobama\b|'
     r'\beminem\b|\bdrizzy\b|\bdrake\b|\bcardi\b|\bnicki\b|'
     r'\bjay.?z\b|\brihanna\b')

# ── Space / moon ──────────────────────────────────────────────────────────────
_tag("space",
     r'\bmoon\b|rocket\b|\bnasa\b|astronaut|cosmic|\bsaturn\b|'
     r'\bjupiter\b(?!.*price)|\bpluto\b|\bmars\b|galaxy\b|universe\b')

# ── Christmas / holiday ───────────────────────────────────────────────────────
_tag("holiday",
     r'christmas|xmas|\bsanta\b|halloween|\bturkey\b|thanksgiv|'
     r'easter\b|valentines?\b')

# ── Web3 / Solana ecosystem ───────────────────────────────────────────────────
_tag("web3",
     r'\bweb3\b|\bdao\b|\bdepin\b|\brwa\b|tokeniz|on.?chain|'
     r'\bsolana\b|\bsol\b\s+ecosystem')


# =============================================================================
# Public API
# =============================================================================

def tag_message(text: str) -> list[str]:
    """
    Return all matching tags for a raw message string.
    """
    if not text:
        return []
    found = [name for name, pat in _TAGS if pat.search(text)]
    return sorted(set(found))


def tag_token(symbol: str, name: str = "", text: str = "") -> list[str]:
    """
    Combine symbol + name + raw message text, then return all matching tags.
    Use this when you have the structured token fields available.
    """
    combined = " ".join(filter(None, [symbol, name, text]))
    return tag_message(combined)
