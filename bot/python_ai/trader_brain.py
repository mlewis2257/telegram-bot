"""
trader_brain.py — judge each call like a trader (story + meme + flow + holders), RECORD ONLY.

WHY (2026-11-04). Every rule tested so far looked at one thing at a time. A trader reads the
whole story together: AMD had real news, an original pun ("A Meme Dog"), volume coming in and
clean holders -> hold longer; a lazy copycat with no story -> get out early or skip. The one
ingredient our data never had is the narrative judgement (is the story real and fresh, is the
meme original and funny or the 30th copy). This asks Claude for that judgement on every
solwhaletrending qsim trade and stores its plan next to the trade. Nothing acts on it until
`--report` shows the plans line up with what the coins actually did.

TWO QUESTIONS per trade (2026-11-04 v2 — v1 asked only for a plan after entry, framed as "a bot
already bought", and answered EXIT_EARLY on every coin, even story 0 / meme 0 / risk 3):
  ENTRY  right after qsim's entry, with NO price movement: "would you buy this at the call?"
         buy yes/no + chance of 2x and of a rug (%). This is the decision live would need
         before a buy, and it is timed. Table trader_brain_entry.
  PLAN   TRADER_BRAIN_DELAY_SECS after entry (default 45 s), with the price path so far:
         hold_longer / normal / exit_early / skip + the same two chances. Table trader_brain.
Only data up to the moment of judging goes in: never the outcome. The chances let the report
rank coins and test cut-offs instead of relying on four labels.

CONTEXT per coin: the channel's call message (links stripped), the token row (age, flag,
bundles, snipers, dev history, holders, 1h volume, liquidity), entry vs call, the price path
so far (qsim quotes), followed wallets, top holders at entry (holder_snapshots), the coin's
own metadata (description, twitter link — the tweet's account and age), matching news
headlines from the last 12 h (news_posts), copycats (coins with the same name/ticker in the
last 6 h, from new_tokens), and the dev's buy and launch count.

COST CEILINGS (printed at start): TRADER_BRAIN_MAX_PER_DAY judgements and
TRADER_BRAIN_MAX_USD_PER_DAY spend (estimated from the API's token counts), whichever first.
Model: TRADER_BRAIN_MODEL (default claude-haiku-4-5-20251001). Key: ANTHROPIC_API_KEY in .env.

Run:     pm2 start trader_brain.py --name trader-brain --interpreter <venv python>
Report:  python3 trader_brain.py --report [--since 2026-11-05] [--until 2026-11-10]
         (dates are midnights in REPORT_TZ, default America/Los_Angeles; --until is exclusive)
"""

from __future__ import annotations

import json
import os
import re
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "launcher"))
import db  # noqa: E402
import api_alerts  # noqa: E402

API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
MODEL = os.getenv("TRADER_BRAIN_MODEL", "claude-haiku-4-5-20251001")
CHANNELS = [c.strip().lstrip("@") for c in
            os.getenv("TRADER_BRAIN_CHANNELS", "solwhaletrending").split(",") if c.strip()]
DELAY_SECS = float(os.getenv("TRADER_BRAIN_DELAY_SECS", "45"))
MAX_PER_DAY = int(os.getenv("TRADER_BRAIN_MAX_PER_DAY", "150"))
MAX_USD_PER_DAY = float(os.getenv("TRADER_BRAIN_MAX_USD_PER_DAY", "1.0"))
# $ per million tokens (input, output). Defaults are Haiku 4.5; set both when changing model.
PRICE_IN = float(os.getenv("TRADER_BRAIN_PRICE_IN", "1.0"))
PRICE_OUT = float(os.getenv("TRADER_BRAIN_PRICE_OUT", "5.0"))
PASS_SECS = 10

PLANS = ("hold_longer", "normal", "exit_early", "skip")
PROMPT_V = 5
# v6 (2026-11-06): LEARNING arm. Same as v5 plus "similar_past_coins": the closed coins on the same
# channel (last 7 days) whose setup was most like this one, and how each ended. Even call_ids get
# v6, odd call_ids stay v5 — a live A/B on the same days. The market_today brief is off (it showed
# no effect over Nov 5-6), so the split is free for this test.
LEARN_V = 7
# v7 (2026-11-07): v6 bought NOTHING on Nov 7 (0 of 57; the coins it passed made +3%) because
# almost every coin's neighbours read "6 of 8 stopped" — which is simply the channel norm (~20% bank).
# v7 gives the neighbours' bank / rug rates NEXT TO the channel's real rates with a label
# (better / about normal / worse) and says plainly that most coins stopping is normal.
LEARN_ON = os.getenv("TRADER_BRAIN_LEARN", "1") != "0"
SIMILAR_K = 8
# v5 (2026-11-04): v4 still said PASS / EXIT_EARLY on every coin with p_2x ~12% on all of them
# (no ranking), misread a below-median bundle % as a red flag, and exited on first-minute dips.
# Adds vs_typical 1-10 (5 = ordinary for the channel, ~a third at 6+); BUY = vs_typical >= 6.
# v4 (2026-11-04): v3 rated every coin ~70% rug / PASS / EXIT_EARLY, calling bundles, snipers, dev
# selling and thin liquidity red flags — but nearly every called coin has them. Each coin now
# carries "channel_typical" (7-day medians of those stats on that channel + its real bank and rug
# rates), and the model judges RELATIVE to the typical coin.
# v3 (2026-11-04): a "market_today" brief (what ran / rugged / got copied in the last 24 h, top
# news) is added for EVEN call_ids only — a built-in A/B, so the report can tell whether the
# brief helps instead of guessing. has_brief is stored on every row.
BRIEF_ON = os.getenv("TRADER_BRAIN_BRIEF", "0") != "0"     # off since v6 (no effect Nov 5-6)
ENTRY_ON = os.getenv("TRADER_BRAIN_ENTRY", "1") != "0"

_GUIDE = """Most called coins fail; a few run 5-50x. Typically 10-25% of called coins reach 2x and
5-30% rug, depending on the channel (given in the data). Runners usually combine a REAL, FRESH
story (news, a viral moment, a culture reference people get), an ORIGINAL meme (clever, funny, not
the 30th copy of the same ticker), real buying (volume, many holders, not one bundle), and holders
who are not about to dump. Copycats of a story that already ran, lazy tickers, heavy
bundles/snipers, a dev who launches dozens of coins, or a coin already far above the call price
are the usual losers.
IMPORTANT: "channel_typical" shows what an ORDINARY called coin on this channel looks like (median
bundle %, snipers, liquidity, fake volume, dev selling over the last 7 days) and how often those
coins really bank 2x and rug. Nearly every called coin has bundles, snipers, some dev selling and
thin liquidity — on their own those are NOT red flags. Judge this coin RELATIVE to the typical one:
your p_2x and p_rug should average close to the channel's real rates across coins; BUY / hold
coins that are clearly better than typical, PASS / exit coins that are clearly worse, and treat
the ones in between as ordinary. A stat BETTER than the channel median (fewer bundles, fewer
snipers, more liquidity, less fake volume, less dev selling) is a GOOD sign, not a red flag.
If a "market_today" section is present it lists what ran, what rugged and
what got copied to death in the last 24 h, plus top news: use it to tell a fresh story from a
tired one. Judge ONLY from the data given. Give honest, spread-out probabilities:
p_2x and p_rug should differ from coin to coin, not repeat the same number."""

ENTRY_SYSTEM = """You are an experienced Solana meme-coin trader. A Telegram call channel just called the
coin below. Decide whether YOU would buy it now, at about the entry price shown. You see only what
is known at the call: there is no price movement after it yet. """ + _GUIDE + """

Answer with one JSON object and nothing else:
{"vs_typical": 1-10, "buy": true/false, "p_2x": 0-100, "p_rug": 0-100,
 "story": 0-3, "meme": 0-3, "attention": "early" | "peaking" | "late",
 "reason": "<one sentence>"}
p_2x = chance it reaches 2x of the entry price; p_rug = chance it collapses below 0.3x.
vs_typical: 5 = an ordinary coin for this channel; 1 = far worse; 10 = far better. Use the WHOLE
range: across many coins roughly a third should be 6 or higher and a third 4 or lower.
buy = true exactly when vs_typical >= 6."""

SYSTEM = """You are an experienced Solana meme-coin trader managing a small position bought when a
Telegram call channel called the coin below; the price path since the buy is included.
""" + _GUIDE + """

Answer with one JSON object and nothing else:
{"story": 0-3, "story_note": "<what the story is, or 'none'>",
 "meme": 0-3, "meme_note": "<originality/humour in a few words>",
 "copycat": true/false,
 "flow": 0-3, "attention": "early" | "peaking" | "late",
 "risk": 0-3, "risk_note": "<biggest danger in a few words>",
 "win_condition": "<what has to happen for this trade to work>",
 "invalidated_if": "<what would prove it wrong>",
 "vs_typical": 1-10,
 "plan": "hold_longer" | "normal" | "exit_early" | "skip",
 "p_2x": 0-100, "p_rug": 0-100,
 "confidence": 1-5,
 "reason": "<one sentence>"}
story/meme/flow: 0 = none/bad, 3 = strong. risk: 0 = clean, 3 = likely rug/dump.
attention: early = the story/coin is just starting to get noticed; late = already obvious and run up.
p_2x = chance it reaches 2x of the entry price; p_rug = chance it collapses below 0.3x.
plan: hold_longer = could be a runner, worth holding past the usual 2x take-profit;
normal = the default for an ordinary coin: take the usual 2x take-profit or the stop;
(a dip in the first minute is common, also for coins that later reach 2x — a dip alone is not a
reason to exit early; the stop already handles real dumps)
exit_early = several warning signs line up: take a small profit or get out soon;
skip = clearly should not have been bought: sell now.
vs_typical: 5 = an ordinary coin for this channel at this point; use the whole 1-10 range (about a
third 6+ and a third 4 or lower across coins)."""

_day = None
_n_today = 0
_usd_today = 0.0
_capped_noted = False
_failed: dict[int, int] = {}
# Out of credit / bad key: alert, then pause BILLING_PAUSE_S instead of failing every coin (a coin
# that fails twice is never retried, so an outage would also wipe out that stretch of data).
BILLING_PAUSE_S = 300
_pause_until = 0.0
_streak = api_alerts.FailStreak("trader_brain")


def ensure_table() -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trader_brain (
                call_id     integer PRIMARY KEY,
                judged_at   timestamptz NOT NULL DEFAULT now(),
                model       text,
                delay_secs  numeric,
                context     jsonb,
                plan        text,
                story       integer,
                meme        integer,
                flow        integer,
                risk        integer,
                copycat     boolean,
                confidence  integer,
                verdict     jsonb,
                in_tokens   integer,
                out_tokens  integer
            )""")
        cur.execute("ALTER TABLE trader_brain ADD COLUMN IF NOT EXISTS attention text")
        cur.execute("ALTER TABLE trader_brain ADD COLUMN IF NOT EXISTS ctx_ms integer")   # gathering the data
        cur.execute("ALTER TABLE trader_brain ADD COLUMN IF NOT EXISTS api_ms integer")   # the model's answer
        cur.execute("ALTER TABLE trader_brain ADD COLUMN IF NOT EXISTS p_2x integer")
        cur.execute("ALTER TABLE trader_brain ADD COLUMN IF NOT EXISTS p_rug integer")
        cur.execute("ALTER TABLE trader_brain ADD COLUMN IF NOT EXISTS prompt_v integer")
        cur.execute("ALTER TABLE trader_brain ADD COLUMN IF NOT EXISTS has_brief boolean")
        cur.execute("ALTER TABLE trader_brain ADD COLUMN IF NOT EXISTS vs_typical integer")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trader_brain_entry (
                call_id     integer PRIMARY KEY,
                judged_at   timestamptz NOT NULL DEFAULT now(),
                model       text,
                prompt_v    integer,
                buy         boolean,
                p_2x        integer,
                p_rug       integer,
                story       integer,
                meme        integer,
                attention   text,
                verdict     jsonb,
                context     jsonb,
                ctx_ms      integer,
                api_ms      integer,
                in_tokens   integer,
                out_tokens  integer
            )""")
        cur.execute("ALTER TABLE trader_brain_entry ADD COLUMN IF NOT EXISTS has_brief boolean")
        # paired judging: one row per (coin, version)
        cur.execute("""
            DO $$ BEGIN
              IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'trader_brain_entry_pkey'
                         AND array_length(conkey, 1) = 1) THEN
                ALTER TABLE trader_brain_entry DROP CONSTRAINT trader_brain_entry_pkey;
                UPDATE trader_brain_entry SET prompt_v = 0 WHERE prompt_v IS NULL;
                ALTER TABLE trader_brain_entry ADD PRIMARY KEY (call_id, prompt_v);
              END IF;
            END $$""")
        cur.execute("ALTER TABLE trader_brain_entry ADD COLUMN IF NOT EXISTS vs_typical integer")
        conn.commit()


# ── Context ─────────────────────────────────────────────────────────────────────

_URL_RE = re.compile(r"https?://\S+|t\.me/\S+")
_DROP_TOKEN_KEYS = {"id", "first_seen_at", "updated_at", "mint_resolved", "pool_address",
                    "deployer_address", "last_boost_at", "dexscreener_paid_at"}


def _jsonable(v):
    if isinstance(v, datetime):
        return v.isoformat()
    try:
        json.dumps(v)
        return v
    except TypeError:
        return float(v) if hasattr(v, "__float__") else str(v)


def _words(s: str) -> list[str]:
    stop = {"the", "coin", "token", "inu", "sol", "solana", "pump", "meme", "official", "and"}
    return [w for w in re.findall(r"[a-z0-9]{3,}", (s or "").lower()) if w not in stop][:4]


LEARN_NOTE = """

"similar_past_coins" lists closed coins on this channel from the last 7 days whose setup (bundle
at launch, bundle left, the dev's bundled share and how much of it was sold, snipers, liquidity,
fake volume) was most like this one, and how each actually ended: "rugged", "stopped", "banked 2x
then collapsed" (the 2x bank still paid) or "banked 2x and kept running".
"similar_vs_channel" compares them with the channel's REAL rates. IMPORTANT: on these channels most
coins stop or rug — only about one in five banks 2x — so "6 of 8 stopped" is NORMAL and is not a
warning. Only let the similar coins move you when similar_vs_channel says "worse" (they rugged or
lost clearly more than usual: lean PASS) or "better" (they banked clearly more than usual: lean
BUY). When it says "about normal", ignore them and judge the coin on everything else."""


# 2026-11-08 PAIRED: every coin's at-call verdict is asked of BOTH v5 and the learning version, so
# the two are compared on the SAME coins (the odd/even split let coin luck swamp the comparison:
# on Nov 7 the even coins averaged ~+6% and the odd ones ~-27% whatever the brain said). The 45 s
# plan stays v5 only. TRADER_BRAIN_PAIRED=0 falls back to the odd/even split.
PAIRED = os.getenv("TRADER_BRAIN_PAIRED", "1") != "0"


def entry_versions(call_id: int) -> list[int]:
    if not LEARN_ON:
        return [PROMPT_V]
    if PAIRED:
        return [PROMPT_V, LEARN_V]
    return [LEARN_V if call_id % 2 == 0 else PROMPT_V]


def version_for(call_id: int) -> int:
    """Version for the 45 s plan (and the old odd/even split)."""
    if PAIRED:
        return PROMPT_V
    return LEARN_V if (LEARN_ON and call_id % 2 == 0) else PROMPT_V


_cand_cache: dict[str, tuple[float, list]] = {}


def _candidates(channel: str) -> list[dict]:
    """Closed qsim coins on this channel, last 7 days, with setup + outcome. Cached 10 min."""
    hit = _cand_cache.get(channel)
    if hit and time.monotonic() - hit[0] < 600:
        return hit[1]
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT qp.call_id, t.symbol, t.mint_address, qp.exit_time, qp.exit_reason,
                   qp.sol_out / nullif(qp.sol_in, 0),
                   t.bundle_pct_initial, t.bundle_pct_remaining, t.bundled_pct, t.bundled_sold_pct,
                   t.sniper_count,
                   t.liq_at_detection, t.fake_vol_pct,
                   (SELECT max(o.real_mult) FROM qsim_quote_observations o
                     WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time AND o.real_mult > 0),
                   (SELECT o.real_mult FROM qsim_quote_observations o
                     WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time AND o.real_mult > 0
                     ORDER BY o.observed_at DESC LIMIT 1)
            FROM qsim_positions qp JOIN tokens t ON t.id = qp.token_id
            WHERE qp.channel_handle = %s AND qp.status = 'closed' AND qp.sol_in > 0
              AND qp.entry_time >= now() - interval '7 days' AND qp.exit_time < now()""", (channel,))
        rows = cur.fetchall()
    out = []
    for (cid, sym, mint, ex_t, reason, x, b0, b1, devb, devs, sn, liq, fake, pmax, plast) in rows:
        x = float(x or 0)
        pmax = float(pmax) if pmax is not None else None
        plast = float(plast) if plast is not None else None
        if "bank" in (reason or ""):
            if plast is not None and plast >= 1.5:
                lab = f"banked 2x and kept running (high {pmax or x:.1f}x, still {plast:.1f}x)"
            elif pmax is not None and pmax >= 3:
                lab = f"banked 2x, ran to {pmax:.1f}x, then collapsed to {plast or 0:.2f}x"
            else:
                lab = f"banked 2x then collapsed to {plast if plast is not None else 0:.2f}x"
        elif x < 0.30:
            lab = f"rugged ({x:.2f}x)"
        elif x >= 1.0:
            lab = f"small win ({x:.2f}x)"
        else:
            lab = f"stopped ({x:.2f}x)" + (f", later ran to {pmax:.1f}x" if pmax and pmax >= 2 else "")
        f = lambda v: float(v) if v is not None else None
        out.append({"call_id": cid, "symbol": sym, "mint": mint, "outcome": lab,
                    "kind": "bank" if "bank" in (reason or "") else "rug" if x < 0.30 else "other",
                    "bundle_launch": f(b0), "bundle_left": f(b1), "dev_bundled": f(devb),
                    "dev_bundle_sold": f(devs), "snipers": f(sn),
                    "liq": f(liq), "fake_vol": f(fake)})
    _cand_cache[channel] = (time.monotonic(), out)
    return out


def similar_coins(channel: str, mint: str, tok: dict) -> tuple[list, dict] | None:
    """The SIMILAR_K closed coins with the nearest setup (normalised distance over the stats both have)."""
    import math
    # dev_bundled / dev_bundle_sold come from the call's "Bundled: X% | Sold: Y%" line (tokens.bundled_pct /
    # bundled_sold_pct). tokens.dev_sold_pct is a legacy column nothing writes — never use it.
    scale = {"bundle_launch": 30.0, "bundle_left": 8.0, "dev_bundled": 15.0, "dev_bundle_sold": 15.0,
             "snipers": 15.0, "fake_vol": 5.0}
    me = {"bundle_launch": tok.get("bundle_pct_initial"), "bundle_left": tok.get("bundle_pct_remaining"),
          "dev_bundled": tok.get("bundled_pct"), "dev_bundle_sold": tok.get("bundled_sold_pct"),
          "snipers": tok.get("sniper_count"),
          "liq": tok.get("liq_at_detection"), "fake_vol": tok.get("fake_vol_pct")}
    me = {k: (float(v) if v is not None else None) for k, v in me.items()}
    if sum(v is not None for v in me.values()) < 3:
        return None
    scored = []
    for c in _candidates(channel):
        if c["mint"] == mint:
            continue
        d, n = 0.0, 0
        for k, sc in scale.items():
            if me[k] is not None and c[k] is not None:
                d += abs(me[k] - c[k]) / sc
                n += 1
        if me["liq"] and c["liq"] and me["liq"] > 0 and c["liq"] > 0:
            d += abs(math.log(me["liq"] / c["liq"])) / 0.5
            n += 1
        if n >= 3:
            scored.append((d / n, c))
    if not scored:
        return None
    scored.sort(key=lambda x: x[0])
    near = [c for _, c in scored[:SIMILAR_K]]
    lst = [{"symbol": c["symbol"], "bundle_launch_pct": c["bundle_launch"], "bundle_left_pct": c["bundle_left"],
            "dev_bundled_pct": c["dev_bundled"], "dev_bundle_sold_pct": c["dev_bundle_sold"],
            "snipers": c["snipers"], "liq_usd": c["liq"], "outcome": c["outcome"]}
           for c in near]
    summ = {"of": len(near), "banked": sum(c["kind"] == "bank" for c in near),
            "rugged": sum(c["kind"] == "rug" for c in near),
            "stopped_or_other": sum(c["kind"] == "other" for c in near)}
    return lst, summ


_brief_cache: tuple[float, dict] | None = None


def market_brief() -> dict:
    """What ran / rugged / got copied in the last 24 h + top news. Cached 30 min."""
    global _brief_cache
    if _brief_cache and time.monotonic() - _brief_cache[0] < 1800:
        return _brief_cache[1]
    conn = db.get_conn()
    db.safe_rollback()
    out = {}
    with conn.cursor() as cur:
        cur.execute("""
            SELECT t.symbol, t.name, qp.channel_handle,
                   greatest(coalesce(qp.peak_multiplier, 0),
                            coalesce((SELECT max(o.real_mult) FROM qsim_quote_observations o
                                      WHERE o.call_id = qp.call_id AND o.real_mult IS NOT NULL), 0)) AS hi,
                   qp.sol_out / nullif(qp.sol_in, 0) AS x
            FROM qsim_positions qp JOIN tokens t ON t.id = qp.token_id
            WHERE qp.entry_time >= now() - interval '24 hours' AND qp.status = 'closed'""")
        rows = cur.fetchall()
        ran = sorted((r for r in rows if r[3] and float(r[3]) >= 3), key=lambda r: -float(r[3]))[:15]
        out["ran_3x_plus_24h"] = [f"{r[0]} ({r[1]}) {float(r[3]):.1f}x" for r in ran]
        rugged = [r for r in rows if r[4] is not None and float(r[4]) < 0.3][:15]
        out["rugged_24h"] = [f"{r[0]} ({r[1]})" for r in rugged]
        cur.execute("""
            SELECT regexp_replace(lower(symbol), '[^a-z0-9]', '', 'g') AS k, count(*)
            FROM new_tokens WHERE created_at >= now() - interval '24 hours' AND symbol IS NOT NULL
            GROUP BY 1 HAVING count(*) >= 15 ORDER BY 2 DESC LIMIT 12""")
        out["most_copied_tickers_24h"] = [f"{k} x{n}" for k, n in cur.fetchall() if k]
        cur.execute("""
            SELECT DISTINCT ON (left(coalesce(title, body), 60)) coalesce(title, left(body, 160))
            FROM news_posts WHERE seen_at >= now() - interval '12 hours'
            ORDER BY left(coalesce(title, body), 60), seen_at DESC LIMIT 12""")
        out["news_12h"] = [_URL_RE.sub("", t or "")[:160] for (t,) in cur.fetchall()]
    _brief_cache = (time.monotonic(), out)
    return out


_norms_cache: dict[str, tuple[float, dict]] = {}


def channel_norms(channel: str) -> dict:
    """7-day medians of key stats for this channel's qsim coins + real bank/rug rates. Cached 1 h."""
    hit = _norms_cache.get(channel)
    if hit and time.monotonic() - hit[0] < 3600:
        return hit[1]
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT count(*),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY t.bundle_pct_remaining),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY t.bundle_pct_initial),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY t.sniper_count),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY t.liq_at_detection),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY t.fake_vol_pct),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY t.bundled_sold_pct),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY t.vol_1h_at_detection),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY t.token_age_minutes),
                   avg((qp.exit_reason LIKE '%%bank%%')::int) FILTER (WHERE qp.status = 'closed'),
                   avg((qp.sol_out / nullif(qp.sol_in, 0) < 0.3)::int) FILTER (WHERE qp.status = 'closed')
            FROM qsim_positions qp JOIN tokens t ON t.id = qp.token_id
            WHERE qp.channel_handle = %s AND qp.entry_time >= now() - interval '7 days'""", (channel,))
        r = cur.fetchone()
    f = lambda v, d=1: None if v is None else round(float(v), d)
    out = {"coins_last_7d": r[0], "median_bundle_pct_remaining": f(r[1]), "median_bundle_pct_initial": f(r[2]),
           "median_sniper_count": f(r[3], 0), "median_liquidity_usd": f(r[4], 0), "median_fake_vol_pct": f(r[5]),
           "median_dev_bundle_sold_pct": f(r[6]), "median_vol_1h_usd": f(r[7], 0), "median_age_min": f(r[8], 0),
           "real_bank_2x_rate_pct": f((r[9] or 0) * 100), "real_rug_rate_pct": f((r[10] or 0) * 100)}
    _norms_cache[channel] = (time.monotonic(), out)
    return out


def has_brief(call_id: int) -> bool:
    return BRIEF_ON and call_id % 2 == 0


def build_context(call_id: int, with_path: bool = True, learn: bool = False,
                  fast: bool = False, live: dict | None = None) -> dict | None:
    """fast=True skips the coin's metadata fetch (IPFS, up to ~20 s) — used before a live buy.
    live={"channel":..., "entry_mcap":...} builds from the call itself when qsim has no
    position for it yet (live can decide before qsim opens)."""
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT qp.channel_handle, qp.entry_time, qp.entry_price, qp.sol_in, c.mcap_at_call, c.conviction_score,
                   c.raw_message, c.message_type, c.narrative_tags, c.created_at,
                   to_jsonb(t) AS tok, t.mint_address, t.symbol, t.name
            FROM qsim_positions qp JOIN calls c ON c.id = qp.call_id JOIN tokens t ON t.id = qp.token_id
            WHERE qp.call_id = %s""", (call_id,))
        row = cur.fetchone()
        if not row and live:
            cur.execute("""
                SELECT %s, now(), %s, NULL, c.mcap_at_call, c.conviction_score,
                       c.raw_message, c.message_type, c.narrative_tags, c.created_at,
                       to_jsonb(t) AS tok, t.mint_address, t.symbol, t.name
                FROM calls c JOIN tokens t ON t.id = c.token_id
                WHERE c.id = %s""", (live.get("channel"), live.get("entry_mcap"), call_id))
            row = cur.fetchone()
        if not row:
            return None
        (channel, entry_time, entry_price, sol_in, mcap_call, score, raw, mtype, tags, called_at,
         tok, mint, symbol, name) = row
        judged = datetime.now(timezone.utc)

        cur.execute("""SELECT round(extract(epoch FROM observed_at - %s)) AS s, round(real_mult, 3)
                       FROM qsim_quote_observations
                       WHERE call_id = %s AND real_mult > 0 AND observed_at <= %s
                       ORDER BY observed_at""", (entry_time, call_id, judged))
        path = [{"secs": int(s), "mult": float(m)} for s, m in cur.fetchall()][-12:] if with_path else []

        cur.execute("SELECT n_holding FROM followed_at_call WHERE call_id = %s", (call_id,))
        r = cur.fetchone()
        followed = r[0] if r else None

        cur.execute("""SELECT supply_ui, holders FROM holder_snapshots
                       WHERE call_id = %s AND phase = 'entry' AND ok""", (call_id,))
        r = cur.fetchone()
        holders = None
        if r and r[0]:
            sup = float(r[0])
            real = sorted((float(h["ui_amount"]) for h in r[1] or []
                           if h.get("on_curve") is not False and h.get("ui_amount") is not None), reverse=True)
            holders = {"largest_wallet_pct": round(real[0] / sup * 100, 2) if real else None,
                       "top10_wallets_pct": round(sum(real[:10]) / sup * 100, 2),
                       "wallets_over_2pct": sum(1 for x in real if x >= 0.02 * sup)}

        cur.execute("SELECT dev, dev_buy_sol, created_at FROM new_tokens WHERE mint = %s", (mint,))
        r = cur.fetchone()
        dev = None
        if r:
            cur.execute("SELECT count(*) FROM new_tokens WHERE dev = %s AND created_at >= now() - interval '7 days'",
                        (r[0],))
            dev = {"dev_buy_sol": float(r[1] or 0), "dev_launches_7d": cur.fetchone()[0],
                   "created_min_before_call": round((called_at - r[2]).total_seconds() / 60, 1)}

        sym_key = re.sub(r"[^a-z0-9]", "", (symbol or "").lower())
        name_key = re.sub(r"[^a-z0-9]", "", (name or "").lower())
        copy = None
        if sym_key:
            cur.execute("""SELECT count(*), min(created_at) FROM new_tokens
                           WHERE created_at >= %s - interval '6 hours' AND created_at <= %s
                             AND (regexp_replace(lower(symbol), '[^a-z0-9]', '', 'g') = %s
                                  OR (%s <> '' AND regexp_replace(lower(name), '[^a-z0-9]', '', 'g') = %s))""",
                        (judged, judged, sym_key, name_key, name_key))
            n, first = cur.fetchone()
            copy = {"same_name_or_ticker_launches_6h": n,
                    "first_one_min_ago": round((judged - first).total_seconds() / 60) if first else None}

        news = []
        for w in _words(f"{symbol} {name}"):
            cur.execute("""SELECT seen_at, coalesce(title, left(body, 200)) FROM news_posts
                           WHERE seen_at >= %s - interval '12 hours' AND seen_at <= %s
                             AND lower(coalesce(title, '') || ' ' || coalesce(body, '')) LIKE %s
                           ORDER BY seen_at DESC LIMIT 3""", (judged, judged, f"%{w}%"))
            for at, title in cur.fetchall():
                news.append({"min_ago": round((judged - at).total_seconds() / 60),
                             "headline": _URL_RE.sub("", title or "")[:200]})
        news = news[:6]

    meta = None
    try:
        if fast:
            raise RuntimeError("fast: metadata skipped")
        import tweet_source_report as tsr
        with conn.cursor() as cur:
            cur.execute("SELECT uri FROM new_tokens WHERE mint = %s", (mint,))
            r = cur.fetchone()
        m, _ = tsr.fetch_meta(r[0] if r else None)
        if m:
            cls, acc, tid = tsr.classify(str(m.get("twitter") or ""), str(m.get("website") or ""))
            meta = {"description": str(m.get("description") or "")[:400],
                    "twitter_link": cls, "tweet_account": acc,
                    "tweet_age_min": round((judged - tsr.tweet_time(tid)).total_seconds() / 60) if tid else None,
                    "has_website": bool(m.get("website")), "has_telegram": bool(m.get("telegram"))}
    except Exception:
        pass

    tok = {k: _jsonable(v) for k, v in (tok or {}).items() if v is not None and k not in _DROP_TOKEN_KEYS}
    ctx = {
        "channel": channel, "symbol": symbol, "name": name,
        "call": {"channel_message": _URL_RE.sub("", raw or "")[:1200], "message_type": mtype,
                 "score": _jsonable(score), "tags": tags, "mcap_at_call": _jsonable(mcap_call)},
        "our_entry": {"entry_mcap": _jsonable(entry_price),
                      "entry_vs_call": round(float(entry_price) / float(mcap_call), 2)
                      if entry_price and mcap_call else None,
                      "secs_since_entry": round((judged - entry_time).total_seconds())},
        "price_path_since_entry": path,
        "token": tok,
        "followed_wallets_holding": followed,
        "holders_at_entry": holders,
        "dev": dev,
        "copycats": copy,
        "news_matching_name_last_12h": news,
        "coin_metadata": meta,
    }
    if not with_path:
        ctx.pop("price_path_since_entry")
        ctx["our_entry"].pop("secs_since_entry")
    try:
        ctx["channel_typical"] = channel_norms(channel)
    except Exception:
        db.safe_rollback()
    if has_brief(call_id):
        try:
            ctx["market_today"] = market_brief()
        except Exception:
            db.safe_rollback()
    if learn:
        try:
            sim = similar_coins(channel, mint, tok)
            if sim:
                ctx["similar_past_coins"], ctx["similar_summary"] = sim
                summ, typ = sim[1], ctx.get("channel_typical") or {}
                n = summ["of"] or 1
                bank_pct, rug_pct = summ["banked"] / n * 100, summ["rugged"] / n * 100
                cb = float(typ.get("real_bank_2x_rate_pct") or 20)
                cr = float(typ.get("real_rug_rate_pct") or 20)
                if rug_pct >= cr + 25 or (bank_pct <= cb - 15 and rug_pct >= cr + 10):
                    label = "worse"
                elif bank_pct >= cb + 20 and rug_pct <= cr + 5:
                    label = "better"
                else:
                    label = "about normal"
                ctx["similar_vs_channel"] = {
                    "similar_banked_pct": round(bank_pct), "channel_bank_pct": round(cb),
                    "similar_rugged_pct": round(rug_pct), "channel_rug_pct": round(cr),
                    "verdict": label}
        except Exception:
            db.safe_rollback()
    return ctx


# ── Judge ───────────────────────────────────────────────────────────────────────

def judge(ctx: dict, system: str = None, kind: str = "plan") -> tuple[dict | None, int, int, str | None]:
    """(verdict, input tokens, output tokens, error). kind 'plan' or 'entry' (validation differs)."""
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
        json={"model": MODEL, "max_tokens": 400, "system": system or SYSTEM,
              "messages": [{"role": "user", "content": "Coin data:\n" + json.dumps(ctx, default=str)}]},
        timeout=60)
    if r.status_code != 200:
        return None, 0, 0, f"http {r.status_code}: {r.text[:200]}"
    j = r.json()
    usage = j.get("usage") or {}
    tin, tout = int(usage.get("input_tokens") or 0), int(usage.get("output_tokens") or 0)
    text = "".join(b.get("text", "") for b in j.get("content") or [] if b.get("type") == "text")
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        return None, tin, tout, f"no JSON in reply: {text[:120]}"
    try:
        v = json.loads(m.group(0))
    except ValueError:
        return None, tin, tout, f"bad JSON: {m.group(0)[:120]}"
    if kind == "plan" and v.get("plan") not in PLANS:
        return None, tin, tout, f"bad plan: {v.get('plan')}"
    if kind == "entry" and not isinstance(v.get("buy"), bool):
        return None, tin, tout, f"bad buy: {v.get('buy')}"
    return v, tin, tout, None


# ── Live verdict (used by live_trader's brain lane, BEFORE a real buy) ───────────────────────
# Fails CLOSED: no key, timeout, bad answer, daily cap or any error -> buy=False with a reason.
# Reuses an at-call verdict the recorder already stored for this coin and version; otherwise asks
# now with fast context (no metadata fetch), stores it (verdict.source = "live") so qsim's record
# and the live decision are the same judgement.
LIVE_VERDICT_MAX_PER_DAY = int(os.getenv("LIVE_BRAIN_MAX_VERDICTS_PER_DAY", "120"))
_live_day = None
_live_n = 0


async def live_verdict(call_id: int, channel: str, entry_mcap: float | None, version: int = PROMPT_V,
                       min_score: int = 6, timeout_s: float = 10.0) -> dict:
    """{"buy": bool, "score": int|None, "reason": str, "source": str, "ms": int}."""
    import asyncio
    global _live_day, _live_n
    t0 = time.monotonic()
    out = lambda buy, score, reason, src: {"buy": buy, "score": score, "reason": reason, "source": src,
                                           "ms": int((time.monotonic() - t0) * 1000)}
    try:
        if version not in (PROMPT_V, LEARN_V):
            return out(False, None, f"unknown brain version v{version}", "config")
        ensure_table()
        conn = db.get_conn()
        db.safe_rollback()
        with conn.cursor() as cur:
            cur.execute("SELECT vs_typical FROM trader_brain_entry WHERE call_id = %s AND prompt_v = %s",
                        (call_id, version))
            r = cur.fetchone()
        if r and r[0] is not None:
            sc = int(r[0])
            return out(sc >= min_score, sc, f"stored verdict {sc}/10", "stored")
        if not API_KEY:
            return out(False, None, "ANTHROPIC_API_KEY not set", "config")
        today = datetime.now(timezone.utc).date()
        if today != _live_day:
            _live_day, _live_n = today, 0
        if _live_n >= LIVE_VERDICT_MAX_PER_DAY:
            return out(False, None, f"live verdict cap {LIVE_VERDICT_MAX_PER_DAY}/day reached", "cap")
        ctx = build_context(call_id, with_path=False, learn=(version == LEARN_V), fast=True,
                            live={"channel": channel, "entry_mcap": entry_mcap})
        if not ctx:
            return out(False, None, "no call/token row for this call", "error")
        system = ENTRY_SYSTEM + (LEARN_NOTE if version == LEARN_V else "")
        _live_n += 1
        v, tin, tout, err = await asyncio.wait_for(asyncio.to_thread(judge, ctx, system, "entry"),
                                                   timeout=timeout_s)
        if v is None:
            if api_alerts.is_billing_error(err):
                # fire and forget (never delays the decision): the lane is now buying nothing
                asyncio.get_running_loop().run_in_executor(
                    None, api_alerts.send, "anthropic_billing_live",
                    f"LIVE brain lane: Anthropic refused — out of credit or bad key. The lane buys "
                    f"NOTHING until fixed. {err[:200]}")
            return out(False, None, f"model answer unusable: {err}", "error")
        sc = _int(v.get("vs_typical"))
        if sc is None:
            return out(False, None, "model gave no vs_typical score", "error")
        v["source"] = "live"
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO trader_brain_entry (call_id, model, prompt_v, buy, p_2x, p_rug, story, meme,
                           attention, verdict, context, ctx_ms, api_ms, in_tokens, out_tokens, has_brief, vs_typical)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                        (call_id, MODEL, version, bool(v.get("buy")), _int(v.get("p_2x")), _int(v.get("p_rug")),
                         _int(v.get("story")), _int(v.get("meme")), str(v.get("attention") or "")[:10] or None,
                         json.dumps(v), json.dumps(ctx, default=str), None,
                         int((time.monotonic() - t0) * 1000), tin, tout, False, sc))
        conn.commit()
        return out(sc >= min_score, sc, str(v.get("reason") or "")[:160], "live")
    except asyncio.TimeoutError:
        return out(False, None, f"model timeout after {timeout_s:g}s", "timeout")
    except Exception as e:
        try:
            db.safe_rollback()
        except Exception:
            pass
        return out(False, None, f"{type(e).__name__}: {e}"[:200], "error")


def due() -> list[int]:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT qp.call_id FROM qsim_positions qp
            WHERE qp.channel_handle = ANY(%s)
              AND qp.entry_time <= now() - make_interval(secs => %s)
              AND qp.entry_time >= now() - interval '20 minutes'
              AND NOT EXISTS (SELECT 1 FROM trader_brain b WHERE b.call_id = qp.call_id)
            ORDER BY qp.entry_time""", (CHANNELS, DELAY_SECS))
        return [r[0] for r in cur.fetchall() if _failed.get(("plan", r[0], None), 0) < 2]


def due_entry() -> list[tuple[int, int]]:
    """(call_id, version) pairs for trades opened in the last 10 min that still lack that
    version's at-call verdict (judged at once, no price path)."""
    if not ENTRY_ON:
        return []
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT qp.call_id, coalesce(array_agg(e.prompt_v) FILTER (WHERE e.prompt_v IS NOT NULL), '{}')
            FROM qsim_positions qp
            LEFT JOIN trader_brain_entry e ON e.call_id = qp.call_id
            WHERE qp.channel_handle = ANY(%s)
              AND qp.entry_time >= now() - interval '10 minutes'
            GROUP BY qp.call_id, qp.entry_time
            ORDER BY qp.entry_time DESC""", (CHANNELS,))
        out = []
        for cid, have in cur.fetchall():
            for ver in entry_versions(cid):
                if ver not in (have or []) and _failed.get(("entry", cid, ver), 0) < 2:
                    out.append((cid, ver))
        return out


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _one(kind: str, call_id: int, tally, ver: int | None = None) -> None:
    """Judge one trade (kind 'entry' or 'plan') and store it."""
    global _n_today, _usd_today, _pause_until
    t0 = time.monotonic()
    if ver is None:
        ver = version_for(call_id)
    ctx = build_context(call_id, with_path=(kind == "plan"), learn=(ver == LEARN_V))
    ctx_ms = int((time.monotonic() - t0) * 1000)
    if not ctx:
        return
    t1 = time.monotonic()
    try:
        system = ENTRY_SYSTEM if kind == "entry" else SYSTEM
        if ver == LEARN_V:
            system = system + LEARN_NOTE
        v, tin, tout, err = judge(ctx, system, kind)
    except requests.RequestException as e:
        v, tin, tout, err = None, 0, 0, f"{type(e).__name__}: {e}"
    api_ms = int((time.monotonic() - t1) * 1000)
    _usd_today += tin / 1e6 * PRICE_IN + tout / 1e6 * PRICE_OUT
    if v is None:
        tally["failed"] += 1
        print(f"[trader_brain] {kind} call_id={call_id} {ctx.get('symbol')} failed: {err}", flush=True)
        if api_alerts.is_billing_error(err):
            _pause_until = time.monotonic() + BILLING_PAUSE_S
            api_alerts.send("anthropic_billing",
                            f"trader_brain: Anthropic refused — out of credit or bad key. Judging paused "
                            f"(retry every {BILLING_PAUSE_S // 60} min); the LIVE brain lane buys nothing "
                            f"until fixed. {err[:200]}")
            return                  # not counted against the coin: it is retried after the pause
        key = ("entry", call_id, ver) if kind == "entry" else ("plan", call_id, None)
        _failed[key] = _failed.get(key, 0) + 1
        _streak.fail(err)
        return
    _streak.ok()
    _n_today += 1
    conn = db.get_conn()
    with conn.cursor() as cur:
        if kind == "entry":
            tally["buy" if v["buy"] else "pass"] += 1
            cur.execute("""INSERT INTO trader_brain_entry (call_id, model, prompt_v, buy, p_2x, p_rug, story, meme,
                           attention, verdict, context, ctx_ms, api_ms, in_tokens, out_tokens, has_brief, vs_typical)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                        (call_id, MODEL, ver, v["buy"], _int(v.get("p_2x")), _int(v.get("p_rug")),
                         _int(v.get("story")), _int(v.get("meme")), str(v.get("attention") or "")[:10] or None,
                         json.dumps(v), json.dumps(ctx, default=str), ctx_ms, api_ms, tin, tout,
                         "market_today" in ctx, _int(v.get("vs_typical"))))
        else:
            tally[v["plan"]] += 1
            cur.execute("""INSERT INTO trader_brain (call_id, model, delay_secs, context, plan, story, meme,
                           flow, risk, copycat, confidence, verdict, in_tokens, out_tokens,
                           attention, ctx_ms, api_ms, p_2x, p_rug, prompt_v, has_brief, vs_typical)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT DO NOTHING""",
                        (call_id, MODEL, ctx["our_entry"]["secs_since_entry"], json.dumps(ctx, default=str),
                         v["plan"], _int(v.get("story")), _int(v.get("meme")), _int(v.get("flow")),
                         _int(v.get("risk")), bool(v.get("copycat")), _int(v.get("confidence")),
                         json.dumps(v), tin, tout, str(v.get("attention") or "")[:10] or None,
                         ctx_ms, api_ms, _int(v.get("p_2x")), _int(v.get("p_rug")), ver,
                         "market_today" in ctx, _int(v.get("vs_typical"))))
    conn.commit()
    if kind == "entry":
        print(f"[trader_brain] {ctx.get('symbol')} ({ctx.get('channel')}) call_id={call_id} AT CALL: "
              f"{'BUY' if v['buy'] else 'PASS'} (vs typical {v.get('vs_typical')}/10, 2x {v.get('p_2x')}%, rug {v.get('p_rug')}%, story {v.get('story')}, "
              f"meme {v.get('meme')}, {v.get('attention')}) — {str(v.get('reason'))[:140]} "
              f"[data {ctx_ms} ms, model {api_ms} ms]", flush=True)
    else:
        print(f"[trader_brain] {ctx.get('symbol')} ({ctx.get('channel')}) call_id={call_id} AT "
              f"{ctx['our_entry']['secs_since_entry']}s: {v['plan'].upper()} (vs typical {v.get('vs_typical')}/10, 2x {v.get('p_2x')}%, rug {v.get('p_rug')}%, "
              f"story {v.get('story')}, meme {v.get('meme')}, flow {v.get('flow')}, risk {v.get('risk')}, "
              f"{v.get('attention')}) — {str(v.get('reason'))[:140]} [data {ctx_ms} ms, model {api_ms} ms]",
              flush=True)


def spend_since(since: datetime) -> float:
    """Estimated Anthropic spend (USD) of every brain judgement — recorder and live — since `since`."""
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT coalesce(sum(in_tokens), 0), coalesce(sum(out_tokens), 0) FROM (
              SELECT in_tokens, out_tokens FROM trader_brain WHERE judged_at >= %s
              UNION ALL
              SELECT in_tokens, out_tokens FROM trader_brain_entry WHERE judged_at >= %s) x""",
                    (since, since))
        tin, tout = cur.fetchone()
    return float(tin) / 1e6 * PRICE_IN + float(tout) / 1e6 * PRICE_OUT


def check_credit() -> str:
    """Warn on Telegram at 75% / 90% of ANTHROPIC_CREDIT_USD used. '' when not configured."""
    cs = api_alerts.credit_settings("ANTHROPIC")
    if not cs:
        return ""
    try:
        return " " + api_alerts.credit_check("trader_brain", "ANTHROPIC", spend_since(cs[1]), cs[0])
    except Exception as e:
        db.safe_rollback()
        return f" credit check failed: {type(e).__name__}"


def run() -> None:
    global _day, _n_today, _usd_today, _capped_noted
    global API_KEY
    if not API_KEY:
        # Wait instead of exiting: an exit makes pm2 restart it in a tight loop.
        print("[trader_brain] ANTHROPIC_API_KEY is not set in .env — waiting (re-checking every 10 min).",
              flush=True)
        while not API_KEY:
            time.sleep(600)
            load_dotenv(Path(__file__).with_name(".env"), override=True)
            API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
    ensure_table()
    print(f"[trader_brain] RECORD-ONLY v{PROMPT_V}"
          f"{(f' + v{LEARN_V} on EVERY coin (paired)' if PAIRED else f' + v{LEARN_V} on even call_ids') if LEARN_ON else ''}. model={MODEL}, channels={','.join(CHANNELS)}. Per trade: "
          f"{'a buy/pass verdict at the call (no price data) + ' if ENTRY_ON else ''}a plan {DELAY_SECS:g}s after "
          f"entry; market brief {'on even call_ids (A/B)' if BRIEF_ON else 'off'}. Ceilings: {MAX_PER_DAY} judgements/day and ${MAX_USD_PER_DAY:.2f}/day "
          f"(~${MAX_USD_PER_DAY * 30:.0f}/month max) at ${PRICE_IN:g}/${PRICE_OUT:g} per M tokens.", flush=True)
    print("[trader_brain] alerts: out-of-credit / failing streak → Telegram."
          + (check_credit() or " Credit warning off (set ANTHROPIC_CREDIT_USD + ANTHROPIC_CREDIT_SINCE)."),
          flush=True)
    last_tally = time.monotonic()
    tally = defaultdict(int)
    while True:
        today = datetime.now(timezone.utc).date()
        if today != _day:
            _day, _n_today, _usd_today, _capped_noted = today, 0, 0.0, False
        try:
            work = [] if time.monotonic() < _pause_until else (
                [("entry", c, v) for c, v in due_entry()] + [("plan", c, None) for c in due()])
            for kind, call_id, ver in work:
                if time.monotonic() < _pause_until:
                    break
                if _n_today >= MAX_PER_DAY or _usd_today >= MAX_USD_PER_DAY:
                    if not _capped_noted:
                        print(f"[trader_brain] daily ceiling reached ({_n_today} judgements, ${_usd_today:.2f}) — "
                              f"none until UTC midnight", flush=True)
                        _capped_noted = True
                    break
                _one(kind, call_id, tally, ver)
        except Exception as e:
            db.safe_rollback()
            print(f"[trader_brain] pass failed: {type(e).__name__}: {e}", flush=True)
        if time.monotonic() - last_tally >= 3600:
            print("[trader_brain] 1h: " + " ".join(f"{k}={v}" for k, v in sorted(tally.items()))
                  + f" today={_n_today} ${_usd_today:.3f}" + check_credit(), flush=True)
            tally.clear()
            last_tally = time.monotonic()
        time.sleep(PASS_SECS)


# ── Report ──────────────────────────────────────────────────────────────────────

# 2026-11-09: report days are the USER'S days. A bare date like --since 2026-11-09 used to mean UTC
# midnight (4 pm Pacific the evening before), which folded Sunday evening into "Monday". --since and
# --until are now midnights in REPORT_TZ, and the by-day rows group by the REPORT_TZ date.
REPORT_TZ = os.getenv("REPORT_TZ", "America/Los_Angeles")


def _window(col: str, since: str, until: str | None) -> tuple[str, list]:
    """SQL + params: col inside [since, until) where both are local midnights in REPORT_TZ."""
    sql = f"{col} >= (%s::date)::timestamp AT TIME ZONE %s"
    params = [since, REPORT_TZ]
    if until:
        sql += f" AND {col} < (%s::date)::timestamp AT TIME ZONE %s"
        params += [until, REPORT_TZ]
    return sql, params


def _span(since: str, until: str | None) -> str:
    return f"{since} .. {until} (exclusive), {REPORT_TZ} days" if until else f"since {since}, {REPORT_TZ} days"


def paired_report(since: str, va: int = PROMPT_V, vb: int = LEARN_V, until: str | None = None) -> None:
    """Both versions judged the SAME coins: compare where they disagree, and the combinations."""
    win, wparams = _window("qp.entry_time", since, until)
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT qp.call_id, qp.channel_handle, (qp.entry_time AT TIME ZONE %s)::date,
                   qp.sol_in, qp.sol_out, qp.exit_reason, a.vs_typical, b.vs_typical
            FROM qsim_positions qp
            JOIN trader_brain_entry a ON a.call_id = qp.call_id AND a.prompt_v = %s
            JOIN trader_brain_entry b ON b.call_id = qp.call_id AND b.prompt_v = %s
            WHERE qp.status = 'closed' AND qp.sol_in > 0 AND {win}
              AND a.vs_typical IS NOT NULL AND b.vs_typical IS NOT NULL""",
                    [REPORT_TZ, va, vb] + wparams)
        rows = cur.fetchall()
    print(f"\n{'=' * 24} PAIRED: v{va} vs v{vb} on the SAME coins ({_span(since, until)}) {'=' * 24}")
    if not rows:
        print("  no coins judged by both versions yet")
        return
    items = []
    for cid, ch, day, s_in, s_out, reason, sa, sb in rows:
        x = float(s_out or 0) / float(s_in)
        items.append({"ch": ch, "day": day, "s_in": float(s_in), "pnl": float(s_out or 0) - float(s_in),
                      "bank": "bank" in (reason or ""), "rug": x < 0.30, "a": sa >= 6, "b": sb >= 6})

    def line(label, g):
        if not g:
            print(f"  {label:26s}    0")
            return
        n, st = len(g), sum(i["s_in"] for i in g)
        print(f"  {label:26s} {n:4d}  bank {sum(i['bank'] for i in g) / n * 100:5.1f}%  "
              f"rug {sum(i['rug'] for i in g) / n * 100:5.1f}%  P&L {sum(i['pnl'] for i in g) / st * 100:+6.1f}%/SOL  "
              f"({sum(i['pnl'] for i in g):+.4f} SOL)")
    cells = {
        "both BUY": [i for i in items if i["a"] and i["b"]],
        f"v{va} BUY only (v{vb} PASS)": [i for i in items if i["a"] and not i["b"]],
        f"v{vb} BUY only (v{va} PASS)": [i for i in items if i["b"] and not i["a"]],
        "both PASS": [i for i in items if not i["a"] and not i["b"]],
    }
    print(f"  {len(items)} closed coins judged by both. The DISAGREEMENT rows decide which version is better.")
    for k, g in cells.items():
        line(k, g)
    print("  combinations:")
    line(f"  v{va} BUY (all)", [i for i in items if i["a"]])
    line(f"  v{vb} BUY (all)", [i for i in items if i["b"]])
    line("  AND: both BUY", cells["both BUY"])
    line("  OR: either BUY", [i for i in items if i["a"] or i["b"]])
    line("  everything (no brain)", items)
    print("  disagreements by day (P&L per SOL, n):")
    for day in sorted({i["day"] for i in items}):
        da = [i for i in cells[f"v{va} BUY only (v{vb} PASS)"] if i["day"] == day]
        db_ = [i for i in cells[f"v{vb} BUY only (v{va} PASS)"] if i["day"] == day]
        f = lambda g: f"{sum(i['pnl'] for i in g) / sum(i['s_in'] for i in g) * 100:+6.1f}% ({len(g):2d})" if g else "     -     "
        print(f"    {day}   v{va}-only {f(da)}   v{vb}-only {f(db_)}")
    for ch in sorted({i["ch"] for i in items}):
        print(f"  -- {ch} --")
        for k, g in cells.items():
            line(k, [i for i in g if i["ch"] == ch])
    print("  Rule (fixed in advance): a version wins if its own picks beat the other's by 10+ pts/SOL\n"
          "  and on most days, over ~5 days. Combine if AND clearly beats both. Otherwise keep v5.")


def report(since: str, PROMPT_V: int = PROMPT_V, until: str | None = None) -> None:
    win_b, p_b = _window("b.judged_at", since, until)
    win_e, p_e = _window("e.judged_at", since, until)
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT qp.channel_handle, b.plan, b.story, b.meme, b.flow, b.risk, b.copycat, b.confidence, b.attention,
                   b.ctx_ms, b.api_ms, b.p_2x, b.p_rug, coalesce(b.has_brief, false), t.symbol,
                   b.verdict->>'reason', b.vs_typical,
                   qp.sol_in, qp.sol_out, qp.exit_reason, qp.peak_multiplier, b.judged_at, qp.exit_time,
                   (SELECT max(o.real_mult) FROM qsim_quote_observations o
                     WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time
                       AND o.real_mult IS NOT NULL) AS post_max
            FROM trader_brain b JOIN qsim_positions qp ON qp.call_id = b.call_id
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.status = 'closed' AND qp.sol_in > 0 AND {win_b}
              AND coalesce(b.prompt_v, 1) = %s""", p_b + [PROMPT_V])
        rows = cur.fetchall()
        cur.execute(f"""
            SELECT qp.channel_handle, e.buy, e.p_2x, e.p_rug, e.ctx_ms, e.api_ms,
                   coalesce(e.has_brief, false), t.symbol, e.verdict->>'reason', e.vs_typical,
                   qp.sol_in, qp.sol_out, qp.exit_reason, qp.peak_multiplier,
                   (SELECT max(o.real_mult) FROM qsim_quote_observations o
                     WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time
                       AND o.real_mult IS NOT NULL) AS post_max
            FROM trader_brain_entry e JOIN qsim_positions qp ON qp.call_id = e.call_id
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.status = 'closed' AND qp.sol_in > 0 AND {win_e} AND e.prompt_v = %s""",
                    p_e + [PROMPT_V])
        erows = cur.fetchall()
    if not rows and not erows:
        print(f"no closed, judged trades since {since} (prompt v{PROMPT_V})")
        return
    items = []
    speed = []
    for (channel, plan, story, meme, flow, risk, copycat, conf, attention, ctx_ms, api_ms, p2, pr,
         brief, sym, why, vt, s_in, s_out, reason, peak, judged, exit_t, post) in rows:
        if ctx_ms is not None and api_ms is not None:
            speed.append((ctx_ms, api_ms))
        x = float(s_out or 0) / float(s_in)
        hi = max(float(peak or 0), float(post or 0))
        items.append({"channel": channel, "plan": plan, "attention": attention, "story": story, "meme": meme, "flow": flow, "risk": risk,
                      "copycat": "yes" if copycat else "no", "confidence": conf,
                      "pnl": float(s_out or 0) - float(s_in), "s_in": float(s_in),
                      "bank": "bank" in (reason or ""), "rug": x < 0.30, "hi": hi,
                      "closed_before_judged": exit_t is not None and exit_t <= judged,
                      "p2": p2, "pr": pr, "p2n": p2, "brief": "with brief" if brief else "no brief",
                      "sym": sym, "why": why, "x": x,
                      "vt": "n/a" if vt is None else ("1-3" if vt <= 3 else "4-5" if vt <= 5 else "6-7" if vt <= 7 else "8-10")})

    def line(label, g):
        n = len(g)
        s_in = sum(i["s_in"] for i in g) or 1e-9
        his = sorted(i["hi"] for i in g)
        print(f"  {label:16s} {n:4d}  bank {sum(i['bank'] for i in g) / n * 100:5.1f}%  "
              f"rug {sum(i['rug'] for i in g) / n * 100:5.1f}%  reached 3x {sum(i['hi'] >= 3 for i in g) / n * 100:5.1f}%  "
              f"5x {sum(i['hi'] >= 5 for i in g) / n * 100:5.1f}%  median high {statistics.median(his):5.2f}x  "
              f"P&L {sum(i['pnl'] for i in g) / s_in * 100:+6.1f}%/SOL")

    def misses(g, when):
        """The judgements to read by hand: confident calls that went the other way."""
        bad_buy = sorted((i for i in g if i["rug"] and (i.get("buy") == "BUY" or (i.get("p2n") or 0) >= 30)),
                         key=lambda i: -(i.get("p2n") or 0))[:10]
        bad_pass = sorted((i for i in g if i["hi"] >= 5 and (i.get("buy") == "PASS" or i.get("plan") in
                                                             ("skip", "exit_early"))),
                          key=lambda i: -i["hi"])[:10]
        print(f"\n  WORST MISSES {when} — read these to see WHY it was wrong:")
        print("   liked it, it rugged:")
        for i in bad_buy:
            print(f"    {str(i['sym'])[:12]:12s} {i['channel'][:14]:14s} {i.get('buy') or i.get('plan'):11s} "
                  f"2x {i.get('p2n')}%  exit {i['x']:.2f}x  — {str(i['why'])[:110]}")
        if not bad_buy:
            print("    none")
        print("   passed / wanted out, it ran 5x+:")
        for i in bad_pass:
            print(f"    {str(i['sym'])[:12]:12s} {i['channel'][:14]:14s} {i.get('buy') or i.get('plan'):11s} "
                  f"2x {i.get('p2n')}%  high {i['hi']:.1f}x  — {str(i['why'])[:110]}")
        if not bad_pass:
            print("    none")
        print()

    def bucket(v):
        return "n/a" if v is None else ("<15%" if v < 15 else "15-29%" if v < 30 else "30-49%" if v < 50 else "50%+")

    if erows:
        ent = []
        espeed = []
        for channel, buy, p2, pr, cms, ams, brief, sym, why, vt, s_in, s_out, reason, peak, post in erows:
            x = float(s_out or 0) / float(s_in)
            ent.append({"channel": channel, "buy": "BUY" if buy else "PASS", "p2": bucket(p2), "pr": bucket(pr),
                        "p2n": p2, "brief": "with brief" if brief else "no brief", "sym": sym, "why": why, "x": x,
                        "vt": "n/a" if vt is None else ("1-3" if vt <= 3 else "4-5" if vt <= 5 else "6-7" if vt <= 7 else "8-10"),
                        "pnl": float(s_out or 0) - float(s_in), "s_in": float(s_in),
                        "bank": "bank" in (reason or ""), "rug": x < 0.30,
                        "hi": max(float(peak or 0), float(post or 0))})
            if cms is not None and ams is not None:
                espeed.append(cms + ams)
        print(f"AT THE CALL — would it buy? (no price data; {len(ent)} closed trades, prompt v{PROMPT_V})")
        print("  The question: do PASS coins rug more and lose, and does a high chance of 2x bank more?")
        line("ALL", ent)
        for lab in ("BUY", "PASS"):
            g = [i for i in ent if i["buy"] == lab]
            if g:
                line(lab, g)
        for key, title in (("vt", "vs typical (1-10)"), ("p2", "its chance of 2x"), ("pr", "its chance of a rug")):
            print(f"  by {title}:")
            for val in ("1-3", "4-5", "6-7", "8-10", "<15%", "15-29%", "30-49%", "50%+", "n/a"):
                g = [i for i in ent if i[key] == val]
                if g:
                    line("  " + val, g)
        for ch in sorted({i["channel"] for i in ent}):
            print(f"  -- {ch} --")
            for lab in ("BUY", "PASS"):
                g = [i for i in ent if i["channel"] == ch and i["buy"] == lab]
                if g:
                    line(lab, g)
        print("  A/B — the market_today brief (even call_ids only):")
        for b in ("with brief", "no brief"):
            for lab in ("BUY", "PASS"):
                g = [i for i in ent if i["brief"] == b and i["buy"] == lab]
                if g:
                    line(f"{b.split()[0]}-brief {lab}", g)
        misses(ent, "at the call")
        if espeed:
            es = sorted(espeed)
            print(f"  SPEED at the call: median {es[len(es) // 2] / 1000:.1f}s, "
                  f"90% {es[min(len(es) - 1, int(len(es) * .9))] / 1000:.1f}s (data + model)\n")
    if not items:
        return
    early = sum(i["closed_before_judged"] for i in items)
    print(f"{len(items)} judged + closed trades since {since} ({early} had already closed when judged — "
          f"the plan could not have acted on those). 'high' = qsim peak or highest quote after exit.\n")
    print("BY PLAN (the question: does hold_longer run further, do exit_early/skip rug more?)")
    line("ALL", items)
    for p in PLANS:
        g = [i for i in items if i["plan"] == p]
        if g:
            line(p, g)
    chans = sorted({i["channel"] for i in items})
    if len(chans) > 1:
        for ch in chans:
            print(f"\n  -- {ch} --")
            line("ALL", [i for i in items if i["channel"] == ch])
            for p in PLANS:
                g = [i for i in items if i["channel"] == ch and i["plan"] == p]
                if g:
                    line(p, g)
    if speed:
        cs, ap = sorted(x[0] for x in speed), sorted(x[1] for x in speed)
        tot = sorted(a + b for a, b in speed)
        q = lambda xs, f: xs[min(len(xs) - 1, int(len(xs) * f))] / 1000
        print(f"SPEED (could it decide BEFORE a buy?): data median {q(cs, .5):.1f}s / 90% {q(cs, .9):.1f}s, "
              f"model median {q(ap, .5):.1f}s / 90% {q(ap, .9):.1f}s, total median {q(tot, .5):.1f}s / "
              f"90% {q(tot, .9):.1f}s\n")
    print("A/B — the market_today brief (even call_ids only):")
    for b in ("with brief", "no brief"):
        for p in PLANS:
            g = [i for i in items if i["brief"] == b and i["plan"] == p]
            if g:
                line(f"{b.split()[0]}-brief {p}"[:16], g)
    misses(items, "at the plan")
    for i in items:
        i["p2"], i["pr"] = bucket(i["p2"]), bucket(i["pr"])
    for key in ("vt", "p2", "pr", "attention", "story", "meme", "flow", "risk", "copycat", "confidence"):
        print(f"\nBY {key.upper()}")
        groups = defaultdict(list)
        for i in items:
            groups[i[key]].append(i)
        for val in sorted(groups, key=lambda v: (v is None, str(v))):
            line(str(val), groups[val])
    print("(p2 = its chance of 2x, pr = its chance of a rug, as judged at the plan)")
    print("\nUseful only if hold_longer clearly beats ALL on 3x/5x AND exit_early/skip clearly rug more,\n"
          "on a decent count, and again on a second week. Then (and only then) wire plans into qsim exits.")


if __name__ == "__main__":
    if "--report" in sys.argv:
        # `--report | head -45` closes the pipe early; exit quietly instead of a BrokenPipeError trace
        import signal
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        i = sys.argv.index("--since") if "--since" in sys.argv else -1
        since = sys.argv[i + 1] if i > 0 else "2026-11-05"
        k = sys.argv.index("--until") if "--until" in sys.argv else -1
        until = sys.argv[k + 1] if k > 0 else None
        j = sys.argv.index("--v") if "--v" in sys.argv else -1
        if j > 0:
            report(since, int(sys.argv[j + 1]), until=until)
        else:
            paired_report(since, until=until)
            for ver in (PROMPT_V, LEARN_V):
                print(f"\n{'=' * 30} PROMPT v{ver} {'(learning: similar past coins)' if ver == LEARN_V else ''} {'=' * 30}")
                report(since, ver, until=until)
    elif "--dry" in sys.argv:
        # print the context the model would see for one call_id, without calling the API
        i = sys.argv.index("--dry")
        print(json.dumps(build_context(int(sys.argv[i + 1])), indent=2, default=str))
    else:
        run()
