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
         python3 trader_brain.py --report --bank [--since 2026-11-10]   (the at-2x-bank decisions)
"""

from __future__ import annotations

import json
import os
import re
import statistics
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
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
# 2026-11-09 cost switches (~$2/day at 3 calls per coin).
# TRADER_BRAIN_PLAN=0 stops the 45 s plan call: record-only, it never chose hold_longer, and the exit
# question moved to the at-bank Grok look; the 2x bank stays. TRADER_BRAIN_SKIP_DEV_BUNDLED=40 skips
# coins whose dev bundled 40%+: the brain passed every one (all of Monday's 17 rugs were both-PASS)
# and the live lane's dev rule blocks them anyway. 0 = judge every coin. Unknown bundle = judged.
PLAN_ON = os.getenv("TRADER_BRAIN_PLAN", "1") != "0"
# WHEN the plan call happens. 'bank' (default since 2026-11-09): once, right after qsim banks a coin at
# 2x — "take the 2x or hold for 3x?" — only banked coins (~15-20% of trades), and the question the
# exit work left open. 'entry': the old 45 s plan on every coin. Record-only either way.
PLAN_AT = os.getenv("TRADER_BRAIN_PLAN_AT", "bank").strip().lower()
# prompt version of the at-bank decision (stored in prompt_v). v1 (Nov 9-10) said BANK on all 12
# coins with p_3x mostly <30% — its "Most coins should be bank" line over-steered it. v2 drops that,
# asks for a spread (a third 50+, a third <30) and holds exactly when p_3x >= 50; graded on p_3x.
# v3 (2026-11-11): v2 said BANK on all 27 coins (Nov 10-11) with p_3x mostly <30%, while 63% of them
# went on to 3x before 1.55x — it saw nothing from AFTER the bank, so it always took the profit. v3 is
# asked BANK_LOOK_MIN after the bank and sees what happened since (price path, holders shedding, X
# after the bank) plus the channel's recent rate. The question is the exit the replays left open:
# sell NOW, or hold for BANK_TARGET with a BANK_FLOOR floor (qsim_slice_trail --ladder). Graded on
# selling at the look vs holding (report --bank). A coin that hit the floor or the target during the
# look is recorded without asking (plan 'floor' / 'target').
BANK_V = 3
BANK_WINDOW_MIN = 15          # banks older than this are skipped
BANK_LOOK_MIN = float(os.getenv("TRADER_BRAIN_BANK_LOOK_MIN", "4"))
BANK_TARGET, BANK_FLOOR = 4.0, 1.8
BANK_FLOOR_FILL = 1.7         # where floor sells actually filled in the replays (coins gap through)
SKIP_DEV_BUNDLED = float(os.getenv("TRADER_BRAIN_SKIP_DEV_BUNDLED", "0") or 0)

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

BANK_SYSTEM = """You are an experienced Solana meme-coin trader. Your position in the coin below has JUST
reached 2x of your entry price — the moment you normally take the profit and sell everything.
Decide: take the 2x now ("bank"), or hold the whole position for more ("hold"). If you hold, you
sell at 3x, or at 1.55x if it falls back first.
Know the base rate: of called coins that reach 2x, only about 4 in 10 go on to 3x before falling
back to 1.55x, and roughly 1 in 8 collapse to near zero within minutes (a dev or bundle dump).
Holding pays when your chance is above ~45%. Signs a coin is better than the base rate: real, fresh
attention still building, broad buying (not one bundle), holders not about to dump, a price path
climbing steadily rather than one spike. Signs it is worse: a single spike, a copy of a story that
already ran, heavy bundles still holding, attention already peaked. You see the price path from
your entry up to this moment, the coin's data at the call, its holders, copycats and news. Judge
ONLY from the data given.

Answer with one JSON object and nothing else:
{"p_3x": 0-100, "decision": "hold" | "bank", "attention": "early" | "peaking" | "late",
 "flow": 0-3, "risk": 0-3, "confidence": 1-5, "reason": "<one sentence>"}
p_3x = chance it reaches 3x of the entry price before falling back to 1.55x. Use the WHOLE range:
across many coins they should average near 40, with roughly a third at 50 or higher and a third
below 30 — coins differ, so your numbers must too. decision = "hold" exactly when p_3x >= 50."""

BANK_SYSTEM_V3 = """You are an experienced Solana meme-coin trader. Your position reached 2x of your entry
price a few minutes ago. Instead of selling at 2x you kept it and watched — "after_bank" shows those
minutes: the price path since 2x (multiples of your entry), the high and low since, where it is now,
whether the biggest wallets have been selling, and X activity after the 2x when it was checked.
Decide now: SELL everything at the current price ("bank"), or HOLD the whole position for 4x of
entry, selling if it falls back to 1.8x first (those floor sells usually fill near 1.7x — these
coins gap down, they don't drift).
after_bank.hold_break_even_pct is the chance of reaching 4x before 1.8x at which holding is worth the
same as selling now. channel_base_rate is how often this channel's recent banked coins actually did
it — start from that number and move away from it only on evidence in the data: a path still
climbing on many steps vs one spike already fading, top wallets shedding (danger) vs sitting still,
X posts growing after the 2x from real accounts vs nothing, a fresh story vs a tired copy. The coin's
data at the call, holders, copycats and news are also given. Judge ONLY from the data given.

Answer with one JSON object and nothing else:
{"p_4x": 0-100, "p_3x": 0-100, "decision": "hold" | "bank", "attention": "early" | "peaking" | "late",
 "flow": 0-3, "risk": 0-3, "confidence": 1-5, "reason": "<one sentence>"}
p_4x = chance it reaches 4x of entry before falling to 1.8x; p_3x = chance of 3x before 1.55x.
Coins differ, so your numbers must too — spread them around the channel's base rate.
decision = "hold" exactly when p_4x is above hold_break_even_pct."""

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
        # one row per (coin, phase): '45s' = the old plan 45 s after entry, 'bank' = at the 2x bank
        cur.execute("ALTER TABLE trader_brain ADD COLUMN IF NOT EXISTS phase text NOT NULL DEFAULT '45s'")
        cur.execute("ALTER TABLE trader_brain ADD COLUMN IF NOT EXISTS p_3x integer")
        cur.execute("ALTER TABLE trader_brain ADD COLUMN IF NOT EXISTS secs_after_bank integer")
        cur.execute("ALTER TABLE trader_brain ADD COLUMN IF NOT EXISTS p_4x integer")
        cur.execute("""
            DO $$ BEGIN
              IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'trader_brain_pkey'
                         AND array_length(conkey, 1) = 1) THEN
                ALTER TABLE trader_brain DROP CONSTRAINT trader_brain_pkey;
                ALTER TABLE trader_brain ADD PRIMARY KEY (call_id, phase);
              END IF;
            END $$""")
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

# v8 (2026-11-10): the LEARNING LOOP. v7 + "lessons_from_your_record": every night, per channel, the
# code builds tables of how the base version's BUY/PASS calls did over the past 7 days grouped by what
# was visible at the call (only groups of MIN_N+ coins), a stronger model writes at most 8 lessons that
# must each cite table rows, and v8 reads that channel's playbook. v7 had won the paired test
# (Sun none, Mon v7, Tue v7 on both channels), so the pair is now v7 vs v8 — it isolates one thing:
# do the lessons help? Same 10-pt rule. TRADER_BRAIN_PAIR="5,7" restores the old pair.
# 2026-11-11: v8 RETIRED. The November replay (brain_replay.py, 1144 coins Nov 2-10) had v8 behind v7 on
# solo picks on both channels and in both halves, even after the lesson-fit fix (-6.2 pts/SOL; its extra
# BUYs lost 11%): the weekly lessons chase patterns that flip the next week. The default is now ONE
# version, v7 alone (half the spend). TRADER_BRAIN_PAIR="7,8" brings the pair back.
PLAYBOOK_V = 8
PAIR = [int(x) for x in os.getenv("TRADER_BRAIN_PAIR", "7").split(",") if x.strip()][:2]
PAIR_SINCE = os.getenv("TRADER_BRAIN_PAIR_SINCE", "2026-11-11")   # first Pacific day of this pair


def _learns(ver: int) -> bool:
    return ver in (LEARN_V, PLAYBOOK_V)


PLAYBOOK_NOTE = """

"lessons_from_your_record" are lessons written last night from YOUR OWN graded calls on this
channel over the past week: each says what to lean towards and cites the evidence (how many coins,
how they did). Only the lessons that fit THIS coin are shown — "this_coin" says which of its features
matched — so don't stretch a lesson to coins it wasn't written for. They are tendencies, not rules:
weigh them with everything else as usual. Your score must still use the whole 1-10 range."""


def _system_for(ver: int) -> str:
    return ENTRY_SYSTEM + (LEARN_NOTE if _learns(ver) else "") + (PLAYBOOK_NOTE if ver == PLAYBOOK_V else "")


def entry_versions(call_id: int) -> list[int]:
    if not LEARN_ON:
        return [PROMPT_V]
    if PAIRED:
        return list(PAIR)
    return [LEARN_V if call_id % 2 == 0 else PROMPT_V]


def version_for(call_id: int) -> int:
    """Version for the 45 s plan (and the old odd/even split)."""
    if PAIRED:
        return PAIR[0]
    return LEARN_V if (LEARN_ON and call_id % 2 == 0) else PROMPT_V


_cand_cache: dict[tuple, tuple[float, list]] = {}


def _floor(ts: datetime, minutes: int) -> datetime:
    return ts - timedelta(minutes=ts.minute % minutes, seconds=ts.second, microseconds=ts.microsecond)


def _candidates(channel: str, as_of: datetime | None = None) -> list[dict]:
    """Closed qsim coins on this channel, last 7 days, with setup + outcome. Cached 10 min.
    as_of (the replay): the 7 days before that moment, only coins CLOSED before it and only quotes
    SEEN before it — floored to 10 min, so a little staler, never later."""
    at = _floor(as_of, 10) if as_of else None
    key = (channel, at.isoformat() if at else None)
    hit = _cand_cache.get(key)
    if hit and (at or time.monotonic() - hit[0] < 600):
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
                     WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time AND o.real_mult > 0
                       AND o.observed_at < coalesce(%s, now())),
                   (SELECT o.real_mult FROM qsim_quote_observations o
                     WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time AND o.real_mult > 0
                       AND o.observed_at < coalesce(%s, now())
                     ORDER BY o.observed_at DESC LIMIT 1)
            FROM qsim_positions qp JOIN tokens t ON t.id = qp.token_id
            WHERE qp.channel_handle = %s AND qp.status = 'closed' AND qp.sol_in > 0
              AND qp.entry_time >= coalesce(%s, now()) - interval '7 days'
              AND qp.exit_time < coalesce(%s, now())""", (at, at, channel, at, at))
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
    _cand_cache[key] = (time.monotonic(), out)
    return out


def similar_coins(channel: str, mint: str, tok: dict, as_of: datetime | None = None) -> tuple[list, dict] | None:
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
    for c in _candidates(channel, as_of):
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


_norms_cache: dict[tuple, tuple[float, dict]] = {}


def channel_norms(channel: str, as_of: datetime | None = None) -> dict:
    """7-day medians of key stats for this channel's qsim coins + real bank/rug rates. Cached 1 h.
    as_of (the replay): the 7 days before that moment (floored to the hour), rates only from coins
    that had CLOSED by then."""
    at = _floor(as_of, 60) if as_of else None
    key = (channel, at.isoformat() if at else None)
    hit = _norms_cache.get(key)
    if hit and (at or time.monotonic() - hit[0] < 3600):
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
                   avg((qp.exit_reason LIKE '%%bank%%')::int) FILTER (WHERE qp.status = 'closed'
                       AND qp.exit_time < coalesce(%s, now())),
                   avg((qp.sol_out / nullif(qp.sol_in, 0) < 0.3)::int) FILTER (WHERE qp.status = 'closed'
                       AND qp.exit_time < coalesce(%s, now()))
            FROM qsim_positions qp JOIN tokens t ON t.id = qp.token_id
            WHERE qp.channel_handle = %s AND qp.entry_time >= coalesce(%s, now()) - interval '7 days'
              AND qp.entry_time < coalesce(%s, now())""", (at, at, channel, at, at))
        r = cur.fetchone()
    f = lambda v, d=1: None if v is None else round(float(v), d)
    out = {"coins_last_7d": r[0], "median_bundle_pct_remaining": f(r[1]), "median_bundle_pct_initial": f(r[2]),
           "median_sniper_count": f(r[3], 0), "median_liquidity_usd": f(r[4], 0), "median_fake_vol_pct": f(r[5]),
           "median_dev_bundle_sold_pct": f(r[6]), "median_vol_1h_usd": f(r[7], 0), "median_age_min": f(r[8], 0),
           "real_bank_2x_rate_pct": f((r[9] or 0) * 100), "real_rug_rate_pct": f((r[10] or 0) * 100)}
    _norms_cache[key] = (time.monotonic(), out)
    return out


def has_brief(call_id: int) -> bool:
    return BRIEF_ON and call_id % 2 == 0


def add_similar(ctx: dict, mint: str, as_of: datetime | None = None) -> None:
    """The learn versions' extra (v7+): the nearest closed coins and how they did vs the channel, in place."""
    try:
        sim = similar_coins(ctx["channel"], mint, ctx.get("token") or {}, as_of)
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


def build_context(call_id: int, with_path: bool = True, learn: bool = False,
                  fast: bool = False, live: dict | None = None,
                  as_of: datetime | None = None, playbook: bool = False) -> dict | None:
    """fast=True skips the coin's metadata fetch (IPFS, up to ~20 s) — used before a live buy.
    live={"channel":..., "entry_mcap":...} builds from the call itself when qsim has no
    position for it yet (live can decide before qsim opens).
    as_of: build the view as of that moment (the at-bank call passes the bank time) so a late
    call can never see prices from after it — no lookahead."""
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
        judged = as_of or datetime.now(timezone.utc)

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
            cur.execute("SELECT count(*) FROM new_tokens WHERE dev = %s AND created_at > %s - interval '7 days' "
                        "AND created_at <= %s", (r[0], judged, judged))
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
        ctx["channel_typical"] = channel_norms(channel, as_of)
    except Exception:
        db.safe_rollback()
    if has_brief(call_id):
        try:
            ctx["market_today"] = market_brief()
        except Exception:
            db.safe_rollback()
    if learn:
        add_similar(ctx, mint, as_of)
    if playbook:
        lessons, tables = playbook_lessons(channel, _local_day(judged))
        ctx["lessons_from_your_record"] = lessons_for_coin(lessons, tables, ctx, mint, judged)
    return ctx


# ── Playbook (v8's learning loop) ───────────────────────────────────────────────

LESSON_MODEL = os.getenv("TRADER_BRAIN_LESSON_MODEL", "claude-sonnet-5-5")
LESSON_PRICE_IN = float(os.getenv("TRADER_BRAIN_LESSON_PRICE_IN", "3.0"))     # $ per M tokens (estimate)
LESSON_PRICE_OUT = float(os.getenv("TRADER_BRAIN_LESSON_PRICE_OUT", "15.0"))
PLAYBOOK_DAYS = 7
# 2026-11-11: 8 -> 15. With 8-20 coin groups the lessons flipped day to day (solearly 50-150k mcap was
# "lean BUY" on Nov 2 and "lean PASS" on Nov 4-5, the day v7's BUYs there made +43%) — noise taught as rules.
MIN_N = 15                    # a table row (and so a lesson) needs at least this many coins
MAX_LESSONS = 8
PLAYBOOK_AT = (0, 20)         # Pacific: build the day's playbook after 00:20 (yesterday is closed)
_pb_cache: dict[tuple[str, str], tuple[float, list]] = {}


def _local_day(ts: datetime) -> date:
    from zoneinfo import ZoneInfo
    return ts.astimezone(ZoneInfo(REPORT_TZ)).date()


def ensure_playbook_table() -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS brain_playbook (
                channel     text NOT NULL,
                made_for    date NOT NULL,       -- the Pacific day these lessons are used on
                base_v      integer NOT NULL,    -- whose graded calls the tables are built from
                window_from date NOT NULL,
                window_to   date NOT NULL,       -- inclusive; always before made_for (no hindsight)
                n_coins     integer,
                tables      jsonb,
                lessons     jsonb,
                dropped     jsonb,               -- lessons rejected for citing rows that don't exist
                model       text,
                in_tokens   integer,
                out_tokens  integer,
                created_at  timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (channel, made_for)
            )""")
        conn.commit()


def playbook_lessons(channel: str, day: date) -> tuple[list, list]:
    """(lessons, tables) of the newest playbook made for `day` or earlier; ([], []) when none."""
    key = (channel, day.isoformat())
    hit = _pb_cache.get(key)
    if hit and time.monotonic() - hit[0] < 300:
        return hit[1]
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute("""SELECT lessons, tables FROM brain_playbook WHERE channel = %s AND made_for <= %s
                       ORDER BY made_for DESC LIMIT 1""", (channel, day))
        r = cur.fetchone()
    out = (r[0] or [], r[1] or []) if r else ([], [])
    _pb_cache[key] = (time.monotonic(), out)
    return out


# Features the brain sets itself while judging (they can't be checked before it answers): a lesson that
# cites only these, or only "overall" rows, is shown to every coin.
JUDGE_FEATURES = {"your attention read", "your story score", "your meme score", "your score"}


def lessons_for_coin(lessons: list | None, tables: list | None, ctx: dict, mint: str,
                     at: datetime | None = None) -> list:
    """The lessons that fit this coin. 2026-11-11: shown every lesson, v8 stretched them — the 50-150k
    mcap lesson was applied to $22k-$45k and $154k-$195k coins, the 1.05-1.3x entry lesson to 0.86-0.98x
    entries — and passed coins that went on to bank. A lesson citing coin-feature rows now applies only
    when the coin is in one of those rows' buckets (an unknown feature — e.g. a recorder gap — matches
    nothing); the matched feature is shown as "this_coin"."""
    rows = {t["id"]: t for t in (tables or [])}
    f = _mask_unrecorded(_features(ctx, {}, mint), ctx, at, recorder_gaps())
    out = []
    for les in lessons or []:
        cited = [rows[c] for c in les.get("cites", []) if c in rows]
        coin_rows = [r for r in cited if r["feature"] != "overall" and r["feature"] not in JUDGE_FEATURES]
        hits = [r for r in coin_rows if f.get(r["feature"]) is not None and f.get(r["feature"]) == r["bucket"]]
        if coin_rows and not hits:
            continue
        view = {"lesson": les["lesson"], "direction": les.get("direction"),
                "evidence": [_row_text(r) for r in cited]}
        if hits:
            view["this_coin"] = sorted({f"{r['feature']} = {r['bucket']}" for r in hits})
        out.append(view)
    return out


def lessons_view(lessons: list | None, tables: list | None) -> list:
    """Stored lessons + tables -> what the brain sees: [{lesson, direction, evidence: [row text]}]."""
    rows = {t["id"]: t for t in (tables or [])}
    return [{"lesson": les["lesson"], "direction": les.get("direction"),
             "evidence": [_row_text(rows[c]) for c in les.get("cites", []) if c in rows]}
            for les in lessons or []]


_has_pb: dict[tuple[str, str], tuple[float, bool]] = {}


def has_playbook(channel: str, day: date) -> bool:
    key = (channel, day.isoformat())
    hit = _has_pb.get(key)
    if hit and time.monotonic() - hit[0] < 60:
        return hit[1]
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute("SELECT to_regclass('brain_playbook') IS NOT NULL")
        ok = False
        if cur.fetchone()[0]:
            cur.execute("SELECT 1 FROM brain_playbook WHERE channel = %s AND made_for <= %s LIMIT 1", (channel, day))
            ok = cur.fetchone() is not None
    _has_pb[key] = (time.monotonic(), ok)
    return ok


def _bucket(v, cuts: list, labels: list) -> str | None:
    if v is None:
        return None
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return next((lab for c, lab in zip(cuts, labels) if v < c), labels[-1])


def _features(ctx: dict, verdict: dict, mint: str) -> dict:
    """What was visible AT THE CALL, bucketed. Only fields the brain itself saw."""
    tok, cp, dev = ctx.get("token") or {}, ctx.get("copycats") or {}, ctx.get("dev") or {}
    call, ent = ctx.get("call") or {}, ctx.get("our_entry") or {}
    sim = (ctx.get("similar_vs_channel") or {}).get("verdict")
    pad = {"Loat": "Float", "pump": "pump.fun"}.get((mint or "")[-4:], "other launchpad")
    return {
        "dev bundled %": _bucket(tok.get("bundled_pct"), [10, 25, 40], ["under 10", "10-25", "25-40", "40+"]),
        "same-name launches in 6h": _bucket(cp.get("same_name_or_ticker_launches_6h"), [2, 6, 16], ["0-1", "2-5", "6-15", "16+"]),
        "followed wallets holding": _bucket(ctx.get("followed_wallets_holding"), [1, 2], ["0", "1", "2+"]),
        "launchpad": pad,
        "mcap at call": _bucket(call.get("mcap_at_call"), [50_000, 150_000, 500_000], ["under 50k", "50-150k", "150-500k", "500k+"]),
        "entry vs call price": _bucket(ent.get("entry_vs_call"), [1.05, 1.3], ["under 1.05x", "1.05-1.3x", "1.3x+"]),
        "dev launches in 7 days": _bucket(dev.get("dev_launches_7d"), [2, 10], ["0-1", "2-9", "10+"]),
        "news matching the name": "yes" if ctx.get("news_matching_name_last_12h") else "no",
        "similar past coins": sim,
        "your attention read": verdict.get("attention"),
        "your story score": _bucket(verdict.get("story"), [1, 2], ["0", "1", "2-3"]),
        "your meme score": _bucket(verdict.get("meme"), [1, 2], ["0", "1", "2-3"]),
        "your score": _bucket(verdict.get("vs_typical"), [4, 6, 8], ["1-3", "4-5", "6-7", "8-10"]),
    }


# 2026-11-10: "same-name launches in 6h" and "dev launches in 7 days" are counted from new_tokens (the
# news_recorder feed). While the feed is down they read 0, not "unknown" — it was down Nov 4 06:00 to
# Nov 7 UTC, and started Oct 31 — so a coin whose LOOKBACK window touches a gap gets those features
# set to unknown in the playbook tables. Gaps are found in the feed's own data (no new token for
# RECORDER_GAP_MIN minutes, and everything before its first row), so later outages are caught too.
# A dev-launch count of 10+ is kept even then: a gap can only hide launches, never add them.
RECORDER_GAP_MIN = 30
_gaps_cache: list = [0.0, None]


def recorder_gaps() -> list[tuple[datetime | None, datetime | None]]:
    """[(start, end)] UTC stretches the token feed did NOT record; None = open-ended. Cached 10 min."""
    if _gaps_cache[1] is not None and time.monotonic() - _gaps_cache[0] < 600:
        return _gaps_cache[1]
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute("SELECT min(created_at), max(created_at) FROM new_tokens")
        first, last = cur.fetchone()
        gaps: list = []
        if first is None:
            gaps = [(None, None)]
        else:
            gaps.append((None, first))
            cur.execute("""SELECT created_at, nxt FROM (
                               SELECT created_at, lead(created_at) OVER (ORDER BY created_at) AS nxt FROM new_tokens) x
                           WHERE nxt - created_at > make_interval(mins => %s)""", (RECORDER_GAP_MIN,))
            gaps += [(a, b) for a, b in cur.fetchall()]
            if datetime.now(timezone.utc) - last > timedelta(minutes=RECORDER_GAP_MIN):
                gaps.append((last, None))           # the feed is down right now
    _gaps_cache[0], _gaps_cache[1] = time.monotonic(), gaps
    return gaps


def _recorded(t0: datetime, t1: datetime, gaps: list) -> bool:
    """True when the token feed was recording for the whole of [t0, t1]."""
    for a, b in gaps:
        if (a is None or a < t1) and (b is None or b > t0):
            return False
    return True


def _mask_unrecorded(f: dict, ctx: dict, at: datetime | None, gaps: list) -> dict:
    """Set the feed-derived features to unknown when their lookback window wasn't recorded."""
    if at is None:
        return f
    if not _recorded(at - timedelta(hours=6), at, gaps):
        f["same-name launches in 6h"] = None
    launches = (ctx.get("dev") or {}).get("dev_launches_7d")
    if not _recorded(at - timedelta(days=7), at, gaps) and not (launches is not None and launches >= 10):
        f["dev launches in 7 days"] = None
    return f


def _stats(items: list) -> dict:
    n = len(items)
    s_in = sum(i["s_in"] for i in items)
    return {"n": n, "banked_pct": round(sum(i["bank"] for i in items) / n * 100),
            "rugged_pct": round(sum(i["rug"] for i in items) / n * 100),
            "pnl_pct": round(sum(i["pnl"] for i in items) / s_in * 100, 1) if s_in else None}


def _row_text(t: dict) -> str:
    who = {"BUY": "coins you BUY", "PASS": "coins you PASSED", "all": "all judged coins"}[t["verdict"]]
    where = "" if t["feature"] == "overall" else f" with {t['feature']} = {t['bucket']}"
    return (f"{t['id']}: {who}{where}: {t['n']} coins, banked {t['banked_pct']}%, rugged {t['rugged_pct']}%, "
            f"P&L {t['pnl_pct']:+.1f}%/SOL")


def playbook_tables(channel: str, first: date, last: date, base_v: int) -> tuple[list, int, list, int]:
    """(rows, coins base_v judged, example mistakes, all judged coins) for [first, last] (Pacific days).

    Two kinds of row, kept apart on purpose:
      "all judged coins" — EVERY coin any brain version judged in the window (v5's included): how coins
          with a feature did. A coin's outcome doesn't depend on who judged it, so this is fair evidence
          and it widens the sample (v5 judged from Nov 4, v7 only from Nov 7).
      "coins you BUY / PASSED" — base_v's OWN calls only. Another version's calls were a different
          judge and must not be taught to v8 as its own decisions.
    Only groups of MIN_N+ coins become rows (the overall rows always show)."""
    span = ("qp.entry_time >= (%s::date)::timestamp AT TIME ZONE %s "
            "AND qp.entry_time < ((%s::date) + 1)::timestamp AT TIME ZONE %s")
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        # one context per coin: base_v's if it judged the coin, else the newest version's
        cur.execute(f"""
            SELECT DISTINCT ON (qp.call_id) e.context, qp.sol_in, qp.sol_out, qp.exit_reason, t.mint_address,
                   qp.entry_time
            FROM trader_brain_entry e
            JOIN qsim_positions qp ON qp.call_id = e.call_id
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.channel_handle = %s AND qp.status = 'closed' AND qp.sol_in > 0 AND {span}
            ORDER BY qp.call_id, (e.prompt_v = %s) DESC, e.prompt_v DESC""",
                    (channel, first, REPORT_TZ, last, REPORT_TZ, base_v))
        coins_raw = cur.fetchall()
        cur.execute("""
            SELECT e.vs_typical, e.verdict, e.context, qp.sol_in, qp.sol_out, qp.exit_reason, t.mint_address,
                   t.symbol, qp.entry_time
            FROM trader_brain_entry e
            JOIN qsim_positions qp ON qp.call_id = e.call_id
            JOIN tokens t ON t.id = qp.token_id
            WHERE e.prompt_v = %s AND qp.channel_handle = %s AND qp.status = 'closed' AND qp.sol_in > 0
              AND e.vs_typical IS NOT NULL
              AND qp.entry_time >= (%s::date)::timestamp AT TIME ZONE %s
              AND qp.entry_time < ((%s::date) + 1)::timestamp AT TIME ZONE %s""",
                    (base_v, channel, first, REPORT_TZ, last, REPORT_TZ))
        raw = cur.fetchall()
    gaps = recorder_gaps()
    items = [verdict_item(vt, verdict, ctx, s_in, s_out, reason, mint, sym, at, gaps)
             for vt, verdict, ctx, s_in, s_out, reason, mint, sym, at in raw if judged_now(ctx or {})]
    coins = [coin_item(ctx, s_in, s_out, reason, mint, at, gaps)
             for ctx, s_in, s_out, reason, mint, at in coins_raw if judged_now(ctx or {})]
    rows, misses = assemble_tables(items, coins)
    return rows, len(items), misses, len(coins)


def judged_now(ctx: dict) -> bool:
    """False for coins the brain now skips (dev bundled >= SKIP_DEV_BUNDLED): lessons about coins v8
    will never see waste a slot and skew the base rates. Unknown bundle = still judged."""
    b = (ctx.get("token") or {}).get("bundled_pct")
    try:
        return SKIP_DEV_BUNDLED <= 0 or b is None or float(b) < SKIP_DEV_BUNDLED
    except (TypeError, ValueError):
        return True


def verdict_item(vt, verdict, ctx, s_in, s_out, reason, mint, sym, at, gaps) -> dict:
    """One of base_v's own calls, for the BUY / PASS rows."""
    verdict, ctx = verdict or {}, ctx or {}
    s_in, s_out = float(s_in), float(s_out or 0)
    return {"verdict": "BUY" if vt >= 6 else "PASS", "s_in": s_in, "pnl": s_out - s_in,
            "bank": "bank" in (reason or ""), "rug": s_out / s_in < 0.30, "sym": sym,
            "reason": str(verdict.get("reason") or "")[:160],
            "f": _mask_unrecorded(_features(ctx, {**verdict, "vs_typical": vt}, mint), ctx, at, gaps)}


def coin_item(ctx, s_in, s_out, reason, mint, at, gaps) -> dict:
    """Any judged coin, for the "all judged coins" rows."""
    s_in, s_out = float(s_in), float(s_out or 0)
    # judge-specific fields (attention / story / meme / score) are left out: no verdict passed
    return {"s_in": s_in, "pnl": s_out - s_in, "bank": "bank" in (reason or ""),
            "rug": s_out / s_in < 0.30, "f": _mask_unrecorded(_features(ctx or {}, {}, mint), ctx or {}, at, gaps)}


def assemble_tables(items: list, coins: list) -> tuple[list, list]:
    """(table rows, example mistakes) from verdict_item()s and coin_item()s."""
    rows = []

    def add(feature, bucket, verdict, group):
        if len(group) >= MIN_N or feature == "overall":
            if group:
                rows.append({"id": f"R{len(rows) + 1}", "feature": feature, "bucket": bucket, "verdict": verdict,
                             **_stats(group)})
    add("overall", "", "all", coins)
    for v in ("BUY", "PASS"):
        add("overall", "", v, [i for i in items if i["verdict"] == v])
    features = list((items or coins)[0]["f"]) if (items or coins) else []
    for feat in features:
        buckets = {i["f"][feat] for i in items + coins if i["f"][feat] is not None}
        for b in sorted(buckets):
            add(feat, b, "all", [i for i in coins if i["f"][feat] == b])
            for v in ("BUY", "PASS"):
                add(feat, b, v, [i for i in items if i["f"][feat] == b and i["verdict"] == v])
    misses = ([f"BUY that lost: {i['sym']} — your reason: {i['reason']}" for i in items
               if i["verdict"] == "BUY" and (i["rug"] or i["pnl"] < 0)][-6:]
              + [f"PASS that banked 2x: {i['sym']} — your reason: {i['reason']}" for i in items
                 if i["verdict"] == "PASS" and i["bank"]][-6:])
    return rows, misses


LESSON_SYSTEM = """You are reviewing your own record as a Solana meme-coin trader on one Telegram call
channel. Every called coin was judged AT THE CALL: BUY (score 6+) or PASS. Below are TABLES computed
from the past week — how coins did, grouped by something you could see at the call. "All judged
coins" rows cover every coin judged in the week (some by an earlier version of you): they show how
coins with that feature turned out, whatever the verdict. "Coins you BUY / PASSED" rows are YOUR own
calls only. Only groups of {min_n}+ coins are shown; the "overall" rows are the base rates.
Then a few example mistakes with your own reasons (anecdotes, not evidence).

Write AT MOST 8 lessons that would make your next BUY/PASS calls on this channel better. Rules:
- every lesson cites 1-3 table row ids (e.g. "R7") whose numbers support it — never invent numbers;
- a lesson must be about something visible AT THE CALL, phrased as guidance: when X, lean BUY /
  lean PASS / ignore it;
- only write a lesson where a group is CLEARLY different from the overall rows (P&L 15+ pts apart,
  or bank or rug rate 10+ pts apart); weak differences are noise;
- prefer fewer, stronger lessons; an empty list is a fine answer when nothing clearly stands out;
- no generic trading advice.

Answer with one JSON object and nothing else:
{"lessons": [{"lesson": "<one or two sentences>", "cites": ["R3", "R9"],
              "direction": "lean BUY" | "lean PASS" | "ignore"}]}"""


def _ask(model: str, system: str, user: str, max_tokens: int) -> tuple[dict | None, int, int, str | None]:
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
        json={"model": model, "max_tokens": max_tokens, "system": system,
              "messages": [{"role": "user", "content": user}]},
        timeout=120)
    if r.status_code != 200:
        return None, 0, 0, f"http {r.status_code}: {r.text[:200]}"
    j = r.json()
    usage = j.get("usage") or {}
    tin, tout = int(usage.get("input_tokens") or 0), int(usage.get("output_tokens") or 0)
    text = "".join(b.get("text", "") for b in j.get("content") or [] if b.get("type") == "text")
    cut = j.get("stop_reason") == "max_tokens"
    m = re.search(r"\{.*\}", text, re.S)
    if m:
        try:
            return json.loads(m.group(0)), tin, tout, None
        except ValueError:
            pass
    salvaged = _salvage_lessons(text)
    if salvaged is not None:
        print(f"[trader_brain] lesson answer {'cut off at max_tokens' if cut else 'was malformed'} — kept "
              f"{len(salvaged['lessons'])} complete lessons", flush=True)
        return salvaged, tin, tout, None
    return None, tin, tout, (f"answer cut off at max_tokens ({max_tokens})" if cut else
                             f"bad JSON: {text[:120]}")


def _salvage_lessons(text: str) -> dict | None:
    """The complete lesson objects from a cut-off or malformed {"lessons": [...]} answer, or None."""
    i = text.find('"lessons"')
    i = text.find("[", i) if i >= 0 else -1
    if i < 0:
        return None
    dec, out, k = json.JSONDecoder(), [], i + 1
    while True:
        while k < len(text) and text[k] in " \t\r\n,":
            k += 1
        if k >= len(text) or text[k] != "{":
            break
        try:
            obj, k = dec.raw_decode(text, k)
        except ValueError:
            break                                    # the cut-off one: stop, keep what was complete
        if isinstance(obj, dict):
            out.append(obj)
    return {"lessons": out} if out else None


def write_lessons(channel: str, first: date, last: date, base_v: int, rows: list, n: int, misses: list,
                  n_all: int) -> tuple[list, list, int, int]:
    """(kept lessons, dropped lessons, in tokens, out tokens) from LESSON_MODEL; raises on an API error."""
    lessons, dropped, tin, tout = [], [], 0, 0
    if n_all >= MIN_N:
        user = (f"Channel: {channel}. {first} to {last}: {n_all} judged coins, {n} of them judged by you "
                f"(v{base_v}).\n\nTABLES:\n"
                + "\n".join(_row_text(r) for r in rows)
                + "\n\nEXAMPLE MISTAKES (anecdotes):\n" + ("\n".join(misses) or "none"))
        v, tin, tout, err = _ask(LESSON_MODEL, LESSON_SYSTEM.replace("{min_n}", str(MIN_N)), user, 3000)
        if v is None:
            if api_alerts.is_billing_error(err):
                api_alerts.send("anthropic_billing", f"trader_brain playbook: Anthropic refused — {err[:200]}")
            raise RuntimeError(f"lesson model: {err}")
        ids = {r["id"] for r in rows}
        for les in (v.get("lessons") or [])[:MAX_LESSONS * 2]:
            cites = [c for c in (les.get("cites") or []) if isinstance(c, str)]
            ok = bool(cites) and all(c in ids for c in cites) and str(les.get("lesson") or "").strip()
            (lessons if ok and len(lessons) < MAX_LESSONS else dropped).append(
                {"lesson": str(les.get("lesson") or "")[:400], "cites": cites,
                 "direction": les.get("direction") if les.get("direction") in ("lean BUY", "lean PASS", "ignore")
                 else None})
    return lessons, dropped, tin, tout


def build_playbook(channel: str, made_for: date) -> dict:
    """Tables from PAIR[0]'s calls over the PLAYBOOK_DAYS before made_for, lessons from LESSON_MODEL, stored.
    Returns a summary dict (raises on API / DB errors so the caller can retry later)."""
    ensure_playbook_table()
    base_v = PAIR[0]
    first, last = made_for - timedelta(days=PLAYBOOK_DAYS), made_for - timedelta(days=1)
    rows, n, misses, n_all = playbook_tables(channel, first, last, base_v)
    lessons, dropped, tin, tout = write_lessons(channel, first, last, base_v, rows, n, misses, n_all)
    db.safe_rollback()
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO brain_playbook (channel, made_for, base_v, window_from, window_to, n_coins,
                       tables, lessons, dropped, model, in_tokens, out_tokens)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (channel, made_for) DO UPDATE SET base_v = EXCLUDED.base_v,
                         window_from = EXCLUDED.window_from, window_to = EXCLUDED.window_to,
                         n_coins = EXCLUDED.n_coins, tables = EXCLUDED.tables, lessons = EXCLUDED.lessons,
                         dropped = EXCLUDED.dropped, model = EXCLUDED.model, in_tokens = EXCLUDED.in_tokens,
                         out_tokens = EXCLUDED.out_tokens, created_at = now()""",
                    (channel, made_for, base_v, first, last, n, json.dumps(rows), json.dumps(lessons),
                     json.dumps(dropped), LESSON_MODEL, tin, tout))
        conn.commit()
    _pb_cache.clear()
    _has_pb.clear()
    return {"channel": channel, "made_for": made_for, "coins": n, "rows": len(rows), "lessons": len(lessons),
            "dropped": len(dropped), "usd": tin / 1e6 * LESSON_PRICE_IN + tout / 1e6 * LESSON_PRICE_OUT}


_pb_last_try: dict[str, float] = {}


def playbook_tick() -> None:
    """From the run loop: once a day per channel (after PLAYBOOK_AT Pacific), build today's playbook.
    A failure is retried every 30 min."""
    if PLAYBOOK_V not in PAIR:
        return
    from zoneinfo import ZoneInfo
    now = datetime.now(ZoneInfo(REPORT_TZ))
    if (now.hour, now.minute) < PLAYBOOK_AT:
        return
    ensure_playbook_table()
    for ch in CHANNELS:
        db.safe_rollback()
        with db.get_conn().cursor() as cur:
            cur.execute("SELECT 1 FROM brain_playbook WHERE channel = %s AND made_for = %s", (ch, now.date()))
            if cur.fetchone():
                continue
        if time.monotonic() - _pb_last_try.get(ch, -1e9) < 1800:
            continue
        _pb_last_try[ch] = time.monotonic()
        try:
            s = build_playbook(ch, now.date())
            print(f"[trader_brain] PLAYBOOK {ch} for {s['made_for']}: {s['coins']} graded coins, {s['rows']} table "
                  f"rows, {s['lessons']} lessons kept, {s['dropped']} dropped (~${s['usd']:.3f})", flush=True)
        except Exception as e:
            db.safe_rollback()
            print(f"[trader_brain] playbook {ch} failed (retry in 30 min): {type(e).__name__}: {e}", flush=True)


def report_playbook() -> None:
    ensure_playbook_table()
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute("""SELECT DISTINCT ON (channel) channel, made_for, base_v, window_from, window_to, n_coins,
                              tables, lessons, dropped, model
                       FROM brain_playbook ORDER BY channel, made_for DESC""")
        rows = cur.fetchall()
    if not rows:
        print("no playbook yet")
        return
    for ch, made_for, base_v, w0, w1, n, tables, lessons, dropped, model in rows:
        print(f"\n{'=' * 18} PLAYBOOK {ch} — for {made_for} (from v{base_v}'s calls {w0}..{w1}, {n} coins, {model}) "
              f"{'=' * 18}")
        byid = {t["id"]: t for t in tables or []}
        if not lessons:
            print("  no lessons (nothing clearly stood out)")
        for k, les in enumerate(lessons or [], 1):
            print(f"  {k}. [{les.get('direction') or '-'}] {les['lesson']}")
            for c in les.get("cites", []):
                if c in byid:
                    print(f"       {_row_text(byid[c])}")
        if dropped:
            print(f"  ({len(dropped)} lessons dropped for citing rows that don't exist)")


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
    if kind == "bank" and v.get("decision") not in ("hold", "bank"):
        return None, tin, tout, f"bad decision: {v.get('decision')}"
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
        if version not in (PROMPT_V, LEARN_V, PLAYBOOK_V):
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
        ctx = build_context(call_id, with_path=False, learn=_learns(version), fast=True,
                            live={"channel": channel, "entry_mcap": entry_mcap},
                            playbook=(version == PLAYBOOK_V))
        if not ctx:
            return out(False, None, "no call/token row for this call", "error")
        system = _system_for(version)
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


_DEV_OK = "(%s <= 0 OR t.bundled_pct IS NULL OR t.bundled_pct < %s)"


def _holder_stats(supply, holders) -> dict | None:
    """Real-wallet concentration from a top-20 snapshot (bonding curve / pools left out)."""
    if not supply:
        return None
    sup = float(supply)
    real = sorted((float(h["ui_amount"]) for h in holders or []
                   if h.get("on_curve") is not False and h.get("ui_amount") is not None), reverse=True)
    return {"largest_wallet_pct": round(real[0] / sup * 100, 2) if real else None,
            "top10_wallets_pct": round(sum(real[:10]) / sup * 100, 2),
            "wallets_over_2pct": sum(1 for x in real if x >= 0.02 * sup)}


def after_bank(call_id: int, bank_time: datetime, look_at: datetime) -> dict:
    """What happened between the 2x bank and `look_at` — the v3 at-bank view. 'decided' is set when the
    coin already hit BANK_FLOOR (or lost its route) or BANK_TARGET in that time."""
    conn = db.get_conn()
    db.safe_rollback()
    out: dict = {"look_min": round((look_at - bank_time).total_seconds() / 60, 1)}
    with conn.cursor() as cur:
        cur.execute("SELECT sol_out / nullif(sol_in, 0) FROM qsim_positions WHERE call_id = %s", (call_id,))
        r = cur.fetchone()
        out["sold_2x_at"] = round(float(r[0]), 2) if r and r[0] is not None else None
        cur.execute("""SELECT extract(epoch FROM observed_at - %s),
                              CASE WHEN coalesce(no_route, false) THEN 0 ELSE real_mult END
                       FROM qsim_quote_observations
                       WHERE call_id = %s AND observed_at > %s AND observed_at <= %s
                         AND NOT coalesce(rate_limited, false)
                         AND (real_mult IS NOT NULL OR coalesce(no_route, false))
                       ORDER BY observed_at""", (bank_time, call_id, bank_time, look_at))
        obs = [(float(s), float(m)) for s, m in cur.fetchall()]
        cur.execute("""SELECT phase, supply_ui, holders FROM holder_snapshots
                       WHERE call_id = %s AND phase IN ('entry', 'exit') AND ok""", (call_id,))
        snaps = {ph: _holder_stats(sup, h) for ph, sup, h in cur.fetchall()}
        cur.execute("""SELECT holders FROM holder_watch WHERE call_id = %s AND taken_at <= %s
                       ORDER BY taken_at""", (call_id, look_at))
        watch = [r[0] or [] for r in cur.fetchall()]
        cur.execute("SELECT to_regclass('grok_x') IS NOT NULL")
        x = None
        if cur.fetchone()[0]:
            cur.execute("""SELECT posts_30m, posts_30_120, growing, accounts, accounts_1k, bot_pct
                           FROM grok_x WHERE call_id = %s AND phase = 'bank' AND judged_at <= %s""",
                        (call_id, look_at))
            g = cur.fetchone()
            if g:
                x = {"posts_last_30m": g[0], "posts_30_to_120m_ago": g[1], "growing": g[2], "accounts": g[3],
                     "accounts_over_1k_followers": g[4], "bot_pct": g[5]}
    if obs:
        mults = [m for _s, m in obs]
        step = max(1, len(obs) // 10)
        out.update({"looks": len(obs), "high_since": round(max(mults), 2), "low_since": round(min(mults), 2),
                    "now": round(mults[-1], 2),
                    "path": [{"secs": int(s), "mult": round(m, 2)} for s, m in obs[::step]][-10:]})
        if min(mults) <= BANK_FLOOR:
            out["decided"] = "floor"
        elif max(mults) >= BANK_TARGET:
            out["decided"] = "target"
        now = mults[-1]
    else:
        out["looks"] = 0
        now = out["sold_2x_at"] or 2.0
        out["now"] = round(now, 2)
    out["hold_break_even_pct"] = max(0, min(100, round(
        (now - BANK_FLOOR_FILL) / (BANK_TARGET - BANK_FLOOR_FILL) * 100)))
    hold = {}
    if snaps.get("entry"):
        hold["at_entry"] = snaps["entry"]
    if snaps.get("exit"):
        hold["at_2x"] = snaps["exit"]
    if len(watch) >= 2:
        first = {h["wallet"]: float(h.get("ui_amount") or 0) for h in watch[0]
                 if h.get("on_curve") is not False and h.get("wallet")}
        top = sorted(first, key=lambda w: -first[w])[:10]
        last = {h["wallet"]: float(h.get("ui_amount") or 0) for h in watch[-1] if h.get("wallet")}
        base = sum(first[w] for w in top)
        if base > 0:
            hold["first_top10_wallets_sold_pct"] = round(
                (base - sum(min(last.get(w, 0.0), first[w]) for w in top)) / base * 100, 1)
    if hold:
        out["holders"] = hold
    if x:
        out["x_after_2x"] = x
    return out


_rates_cache: dict[tuple, tuple[float, dict]] = {}


def channel_bank_rates(channel: str, as_of: datetime) -> dict:
    """How this channel's banked coins did after the bank over the 7 days before `as_of` (only banks
    2 h+ old, so each is fully graded). Cached per channel-hour."""
    at = _floor(as_of, 60)
    key = (channel, at.isoformat())
    hit = _rates_cache.get(key)
    if hit:
        return hit[1]
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT count(*),
                   count(*) FILTER (WHERE t4 IS NOT NULL AND (t18 IS NULL OR t4 < t18)),
                   count(*) FILTER (WHERE t3 IS NOT NULL AND (t155 IS NULL OR t3 < t155))
            FROM (
              SELECT (SELECT min(o.observed_at) FROM qsim_quote_observations o WHERE o.call_id = qp.call_id
                        AND o.observed_at > qp.exit_time AND o.real_mult >= %s) AS t4,
                     (SELECT min(o.observed_at) FROM qsim_quote_observations o WHERE o.call_id = qp.call_id
                        AND o.observed_at > qp.exit_time AND NOT coalesce(o.rate_limited, false)
                        AND (o.real_mult <= %s OR coalesce(o.no_route, false))) AS t18,
                     (SELECT min(o.observed_at) FROM qsim_quote_observations o WHERE o.call_id = qp.call_id
                        AND o.observed_at > qp.exit_time AND o.real_mult >= 3) AS t3,
                     (SELECT min(o.observed_at) FROM qsim_quote_observations o WHERE o.call_id = qp.call_id
                        AND o.observed_at > qp.exit_time AND NOT coalesce(o.rate_limited, false)
                        AND (o.real_mult <= 1.55 OR coalesce(o.no_route, false))) AS t155
              FROM qsim_positions qp
              WHERE qp.channel_handle = %s AND qp.status = 'closed' AND qp.exit_reason LIKE '%%bank%%'
                AND qp.exit_time >= %s - interval '7 days' AND qp.exit_time <= %s - interval '2 hours') x""",
                    (BANK_TARGET, BANK_FLOOR, channel, at, at))
        n, n4, n3 = cur.fetchone()
    out = {"banked_coins_last_7d": n,
           "reached_4x_before_1_8x_pct": round(n4 / n * 100) if n else None,
           "reached_3x_before_1_55x_pct": round(n3 / n * 100) if n else None}
    _rates_cache[key] = (time.monotonic(), out)
    return out


def bank_v3_items(since: str, until: str | None = None, ver: int = 3) -> tuple[list, int]:
    """(graded v3 decisions, coins still too fresh to grade). Each item: decision (hold / bank / floor /
    target), p_4x, break-even, the price at the look, what holding from the look paid (4x, the first
    quote at or under the floor — 0 for a dead route — or the last quote), and whether it ran."""
    win, wparams = _window("qp.exit_time", since, until)
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT b.call_id, b.plan, b.p_4x, b.context->'after_bank', qp.exit_time, qp.sol_in,
                   qp.channel_handle, t.symbol, b.verdict->>'reason', qp.exit_time <= now() - interval '2 hours'
            FROM trader_brain b JOIN qsim_positions qp ON qp.call_id = b.call_id
            JOIN tokens t ON t.id = qp.token_id
            WHERE b.phase = 'bank' AND b.prompt_v = %s AND {win}""", [ver] + wparams)
        rows = cur.fetchall()
        fresh = sum(1 for r in rows if not r[9])
        rows = [r for r in rows if r[9]]
        obs: dict[int, list] = defaultdict(list)
        if rows:
            cur.execute("""SELECT o.call_id, o.observed_at,
                                  CASE WHEN coalesce(o.no_route, false) THEN 0 ELSE o.real_mult END
                           FROM qsim_quote_observations o JOIN qsim_positions qp ON qp.call_id = o.call_id
                           WHERE o.call_id = ANY(%s) AND o.observed_at > qp.exit_time
                             AND NOT coalesce(o.rate_limited, false)
                             AND (o.real_mult IS NOT NULL OR coalesce(o.no_route, false))
                           ORDER BY o.call_id, o.observed_at""", ([r[0] for r in rows],))
            for cid, at, m in cur.fetchall():
                obs[cid].append((at, float(m)))
    items = []
    for cid, plan, p4, ab, ex_t, sol_in, ch, sym, reason, _ok in rows:
        ab = ab or {}
        look_at = ex_t + timedelta(minutes=float(ab.get("look_min") or 0))
        now = float(ab.get("now") or 2.0)
        rest = [m for at, m in obs.get(cid, []) if at > look_at]
        paid, ran = (rest[-1] if rest else now), False
        for m in rest:
            if m >= BANK_TARGET:
                paid, ran = BANK_TARGET, True
                break
            if m <= BANK_FLOOR:
                paid = m
                break
        items.append({"cid": cid, "dec": plan, "p4": p4, "be": ab.get("hold_break_even_pct"), "now": now,
                      "paid": paid, "ran": ran, "sol_in": float(sol_in or 0), "ch": ch or "?", "sym": sym,
                      "reason": reason})
    return items, fresh


def bank_v3_group(g: list) -> dict:
    """n, 4x-before-1.8x rate, and holding from the look vs selling at the look (%/SOL and SOL)."""
    n = len(g)
    if not n:
        return {"n": 0}
    dep = sum(i["sol_in"] for i in g) or 1
    d = sum(i["sol_in"] * (i["paid"] - i["now"]) for i in g)
    be = [i["be"] for i in g if i["be"] is not None]
    return {"n": n, "ran": sum(i["ran"] for i in g) / n * 100, "be": statistics.mean(be) if be else None,
            "hold_vs_sell_pct": d / dep * 100, "hold_vs_sell_sol": d}


def report_bank_v3(since: str, until: str | None = None, ver: int = 3) -> None:
    items, fresh = bank_v3_items(since, until, ver)
    print(f"\n{'=' * 18} AFTER THE 2x BANK: sell now or hold for {BANK_TARGET:g}x (floor {BANK_FLOOR:g}x)? "
          f"(prompt v{ver}, {_span(since, until)}) {'=' * 18}")
    if fresh:
        print(f"  ({fresh} more decisions are under 2 h old — graded later)")
    asked = [i for i in items if i["dec"] in ("hold", "bank")]
    auto = [i for i in items if i["dec"] not in ("hold", "bank")]
    if not items:
        print("  no graded v3 decisions yet")
        return

    def line(label, g):
        r = bank_v3_group(g)
        if not r["n"]:
            print(f"  {label:24s}    0")
            return
        print(f"  {label:24s} {r['n']:4d}  4x before {BANK_FLOOR:g}x {r['ran']:5.1f}%"
              f"  (break-even {r['be'] or 0:4.0f}%)   holding vs selling at the look "
              f"{r['hold_vs_sell_pct']:+6.1f}%  ({r['hold_vs_sell_sol']:+.4f} SOL)")
    print(f"  {len(asked)} asked {BANK_LOOK_MIN:g} min after the bank, graded 2 h+ later; "
          f"{len(auto)} decided during the look without asking "
          f"({sum(i['dec'] == 'floor' for i in auto)} floor, {sum(i['dec'] == 'target' for i in auto)} target).")
    line("ALL asked", asked)
    line("said HOLD", [i for i in asked if i["dec"] == "hold"])
    line("said BANK", [i for i in asked if i["dec"] == "bank"])
    print("  by its p_4x minus the break-even:")
    for lo, hi, lab in ((-101, -15, "15+ pts below"), (-15, 0, "0-15 below"), (0, 15, "0-15 above"),
                        (15, 101, "15+ pts above")):
        line(f"  {lab}", [i for i in asked if i["p4"] is not None and i["be"] is not None
                         and lo <= i["p4"] - i["be"] < hi])
    for ch in sorted({i["ch"] for i in asked}):
        print(f"  -- {ch} --")
        line("said HOLD", [i for i in asked if i["ch"] == ch and i["dec"] == "hold"])
        line("said BANK", [i for i in asked if i["ch"] == ch and i["dec"] == "bank"])
    holds = sorted((i for i in asked if i["dec"] == "hold"), key=lambda i: -(i["paid"] - i["now"]))
    if holds:
        print("  its HOLD calls (best first):")
        for i in holds[:10]:
            print(f"    {str(i['sym'])[:12]:12s} {i['ch'][:16]:16s} at the look {i['now']:.2f}x -> "
                  f"{i['paid']:.2f}x {'RAN' if i['ran'] else ''} — {str(i['reason'])[:80]}")
    print(f"  Graded on what it DOES: its HOLD calls must beat selling at the look (positive %), and reach\n"
          f"  {BANK_TARGET:g}x more often than its BANK calls, on 30+ coins and again on the next 30. Only then\n"
          f"  does it earn a live exit rule.")


def _look_secs() -> float:
    return BANK_LOOK_MIN * 60 if BANK_V >= 3 else 0.0


def due_bank() -> list[tuple[int, datetime]]:
    """(call_id, bank time) for coins qsim banked in the last BANK_WINDOW_MIN minutes, not yet judged."""
    if not PLAN_ON or PLAN_AT != "bank":
        return []
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT qp.call_id, qp.exit_time FROM qsim_positions qp
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.channel_handle = ANY(%s)
              AND qp.status = 'closed' AND qp.exit_reason LIKE '%%bank%%'
              AND qp.exit_time >= now() - make_interval(mins => %s)
              AND qp.exit_time <= now() - make_interval(secs => %s)
              AND {_DEV_OK}
              AND NOT EXISTS (SELECT 1 FROM trader_brain b WHERE b.call_id = qp.call_id AND b.phase = 'bank')
            ORDER BY qp.exit_time""", (CHANNELS, BANK_WINDOW_MIN, _look_secs(), SKIP_DEV_BUNDLED, SKIP_DEV_BUNDLED))
        return [(cid, et) for cid, et in cur.fetchall() if _failed.get(("bank", cid, None), 0) < 2]


def due() -> list[int]:
    if not PLAN_ON or PLAN_AT != "entry":
        return []
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT qp.call_id FROM qsim_positions qp
            JOIN tokens t ON t.id = qp.token_id
            WHERE qp.channel_handle = ANY(%s)
              AND qp.entry_time <= now() - make_interval(secs => %s)
              AND qp.entry_time >= now() - interval '20 minutes'
              AND {_DEV_OK}
              AND NOT EXISTS (SELECT 1 FROM trader_brain b WHERE b.call_id = qp.call_id AND b.phase = '45s')
            ORDER BY qp.entry_time""", (CHANNELS, DELAY_SECS, SKIP_DEV_BUNDLED, SKIP_DEV_BUNDLED))
        return [r[0] for r in cur.fetchall() if _failed.get(("plan", r[0], None), 0) < 2]


def due_entry() -> list[tuple[int, int]]:
    """(call_id, version) pairs for trades opened in the last 10 min that still lack that
    version's at-call verdict (judged at once, no price path)."""
    if not ENTRY_ON:
        return []
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT qp.call_id, coalesce(array_agg(e.prompt_v) FILTER (WHERE e.prompt_v IS NOT NULL), '{{}}'),
                   qp.channel_handle
            FROM qsim_positions qp
            JOIN tokens t ON t.id = qp.token_id
            LEFT JOIN trader_brain_entry e ON e.call_id = qp.call_id
            WHERE qp.channel_handle = ANY(%s)
              AND qp.entry_time >= now() - interval '10 minutes'
              AND {_DEV_OK}
            GROUP BY qp.call_id, qp.entry_time, qp.channel_handle
            ORDER BY qp.entry_time DESC""", (CHANNELS, SKIP_DEV_BUNDLED, SKIP_DEV_BUNDLED))
        rows = cur.fetchall()
    out, today = [], _local_day(datetime.now(timezone.utc))
    for cid, have, ch in rows:
        for ver in entry_versions(cid):
            if ver == PLAYBOOK_V and not has_playbook(ch, today):
                continue                    # v8 judges only once its channel has a playbook
            if ver not in (have or []) and _failed.get(("entry", cid, ver), 0) < 2:
                out.append((cid, ver))
    return out


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _one(kind: str, call_id: int, tally, ver=None) -> None:
    """Judge one trade and store it. kind 'entry' (ver = prompt version), 'plan' (the old 45 s plan)
    or 'bank' (ver = the bank time; the view is built as of that moment)."""
    global _n_today, _usd_today, _pause_until
    t0 = time.monotonic()
    bank_time = None
    if kind == "bank":
        bank_time, ver = ver, BANK_V
        look_at = bank_time + timedelta(seconds=_look_secs())
        ctx = build_context(call_id, with_path=True, learn=False, as_of=look_at)
        if ctx and BANK_V >= 3:
            ctx["after_bank"] = after_bank(call_id, bank_time, look_at)
            ctx["channel_base_rate"] = channel_bank_rates(ctx["channel"], bank_time)
            auto = ctx["after_bank"].get("decided")
            if auto:                        # hit the floor or the target during the look: nothing to ask
                tally["bank:" + auto] += 1
                conn = db.get_conn()
                with conn.cursor() as cur:
                    cur.execute("""INSERT INTO trader_brain (call_id, phase, model, context, plan, verdict,
                                   in_tokens, out_tokens, prompt_v, secs_after_bank)
                                   VALUES (%s,'bank',NULL,%s,%s,%s,0,0,%s,%s) ON CONFLICT DO NOTHING""",
                                (call_id, json.dumps(ctx, default=str), auto, json.dumps({"auto": auto}), BANK_V,
                                 round((datetime.now(timezone.utc) - bank_time).total_seconds())))
                conn.commit()
                print(f"[trader_brain] {ctx.get('symbol')} ({ctx.get('channel')}) call_id={call_id} AT 2x BANK: "
                      f"hit the {auto} during the {BANK_LOOK_MIN:g} min look — recorded, not asked", flush=True)
                return
    else:
        if ver is None:
            ver = version_for(call_id)
        ctx = build_context(call_id, with_path=(kind == "plan"), learn=_learns(ver),
                            playbook=(kind == "entry" and ver == PLAYBOOK_V))
    ctx_ms = int((time.monotonic() - t0) * 1000)
    if not ctx:
        return
    t1 = time.monotonic()
    try:
        if kind == "entry":
            system = _system_for(ver)
        elif kind == "bank":
            system = BANK_SYSTEM_V3 if BANK_V >= 3 else BANK_SYSTEM
        else:
            system = SYSTEM + (LEARN_NOTE if _learns(ver) else "")
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
        key = ("entry", call_id, ver) if kind == "entry" else (kind, call_id, None)
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
        elif kind == "bank":
            tally["bank:" + v["decision"]] += 1
            late = round((datetime.now(timezone.utc) - bank_time).total_seconds())
            cur.execute("""INSERT INTO trader_brain (call_id, phase, model, delay_secs, context, plan, flow, risk,
                           confidence, verdict, in_tokens, out_tokens, attention, ctx_ms, api_ms, p_3x, prompt_v,
                           has_brief, secs_after_bank, p_4x)
                           VALUES (%s,'bank',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                           ON CONFLICT DO NOTHING""",
                        (call_id, MODEL, ctx["our_entry"]["secs_since_entry"], json.dumps(ctx, default=str),
                         v["decision"], _int(v.get("flow")), _int(v.get("risk")), _int(v.get("confidence")),
                         json.dumps(v), tin, tout, str(v.get("attention") or "")[:10] or None,
                         ctx_ms, api_ms, _int(v.get("p_3x")), BANK_V, "market_today" in ctx, late,
                         _int(v.get("p_4x"))))
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
    elif kind == "bank":
        print(f"[trader_brain] {ctx.get('symbol')} ({ctx.get('channel')}) call_id={call_id} AT 2x BANK: "
              f"{v['decision'].upper()} (4x before 1.8x {v.get('p_4x')}% vs break-even "
              f"{(ctx.get('after_bank') or {}).get('hold_break_even_pct')}%, 3x before 1.55x {v.get('p_3x')}%, flow {v.get('flow')}, risk {v.get('risk')}, "
              f"{v.get('attention')}, conf {v.get('confidence')}) — {str(v.get('reason'))[:140]} "
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
        cur.execute("SELECT to_regclass('brain_playbook') IS NOT NULL")
        lin = lout = 0
        if cur.fetchone()[0]:
            cur.execute("SELECT coalesce(sum(in_tokens), 0), coalesce(sum(out_tokens), 0) FROM brain_playbook "
                        "WHERE created_at >= %s", (since,))
            lin, lout = cur.fetchone()
        # brain_replay.py's back-test calls draw on the same Anthropic credit
        cur.execute("SELECT to_regclass('brain_replay_verdict') IS NOT NULL")
        if cur.fetchone()[0]:
            cur.execute("SELECT coalesce(sum(in_tokens), 0), coalesce(sum(out_tokens), 0) FROM brain_replay_verdict "
                        "WHERE created_at >= %s", (since,))
            a, b = cur.fetchone()
            tin, tout = tin + a, tout + b
        # exit_manager.py's shadow exit decisions too (priced like the judge; it defaults to the same model)
        cur.execute("SELECT to_regclass('exit_shadow_decisions') IS NOT NULL")
        if cur.fetchone()[0]:
            cur.execute("SELECT coalesce(sum(in_tokens), 0), coalesce(sum(out_tokens), 0) FROM exit_shadow_decisions "
                        "WHERE at >= %s", (since,))
            a, b = cur.fetchone()
            tin, tout = tin + a, tout + b
        cur.execute("SELECT to_regclass('brain_replay_playbook') IS NOT NULL")
        if cur.fetchone()[0]:
            cur.execute("SELECT coalesce(sum(in_tokens), 0), coalesce(sum(out_tokens), 0) FROM brain_replay_playbook "
                        "WHERE created_at >= %s", (since,))
            a, b = cur.fetchone()
            lin, lout = lin + a, lout + b
    return (float(tin) / 1e6 * PRICE_IN + float(tout) / 1e6 * PRICE_OUT
            + float(lin) / 1e6 * LESSON_PRICE_IN + float(lout) / 1e6 * LESSON_PRICE_OUT)


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
    plan_txt = ("OFF (TRADER_BRAIN_PLAN=0)" if not PLAN_ON else
                "AT THE 2x BANK: take it or hold for 3x? (TRADER_BRAIN_PLAN_AT=bank)" if PLAN_AT == "bank" else
                f"{DELAY_SECS:g}s after entry on every coin (TRADER_BRAIN_PLAN_AT=entry)")
    print(f"[trader_brain] cost switches: plan call {plan_txt}; "
          + (f"skipping coins with dev bundled >= {SKIP_DEV_BUNDLED:g}% (TRADER_BRAIN_SKIP_DEV_BUNDLED)"
             if SKIP_DEV_BUNDLED > 0 else "judging every coin (TRADER_BRAIN_SKIP_DEV_BUNDLED=0)"), flush=True)
    print("[trader_brain] alerts: out-of-credit / failing streak → Telegram."
          + (check_credit() or " Credit warning off (set ANTHROPIC_CREDIT_USD + ANTHROPIC_CREDIT_SINCE)."),
          flush=True)
    ensure_playbook_table()
    print((f"[trader_brain] pair: v{PAIR[0]} vs v{PAIR[1]} on every coin" if len(PAIR) > 1
           else f"[trader_brain] one version: v{PAIR[0]} on every coin (no pair, no playbook)")
          + (f"; v{PLAYBOOK_V} reads a nightly per-channel playbook (lessons from v{PAIR[0]}'s graded calls, "
             f"last {PLAYBOOK_DAYS} days, model {LESSON_MODEL}), built after "
             f"{PLAYBOOK_AT[0]:02d}:{PLAYBOOK_AT[1]:02d} {REPORT_TZ}" if PLAYBOOK_V in PAIR else ""), flush=True)
    last_tally = time.monotonic()
    last_pb = 0.0
    tally = defaultdict(int)
    while True:
        today = datetime.now(timezone.utc).date()
        if today != _day:
            _day, _n_today, _usd_today, _capped_noted = today, 0, 0.0, False
        if time.monotonic() - last_pb >= 60 and time.monotonic() >= _pause_until:
            last_pb = time.monotonic()
            try:
                playbook_tick()
            except Exception as e:
                db.safe_rollback()
                print(f"[trader_brain] playbook tick failed: {type(e).__name__}: {e}", flush=True)
        try:
            work = [] if time.monotonic() < _pause_until else (
                [("entry", c, v) for c, v in due_entry()] + [("bank", c, t) for c, t in due_bank()]
                + [("plan", c, None) for c in due()])
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


def paired_report(since: str, va: int | None = None, vb: int | None = None, until: str | None = None) -> None:
    """Both versions judged the SAME coins: compare where they disagree, and the combinations."""
    va = va or PAIR[0]
    vb = vb or (PAIR[1] if len(PAIR) > 1 else None)
    if vb is None:
        print(f"(one version running — v{va}; no paired comparison. --report --v N for one version)")
        return
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
          f"  and on most days, over ~5 days. Combine if AND clearly beats both. Otherwise keep v{va}.")


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
            WHERE qp.status = 'closed' AND qp.sol_in > 0 AND {win_b} AND b.phase = '45s'
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


def report_bank(since: str, until: str | None = None, ver: int = BANK_V) -> None:
    """At-bank decisions graded on what the coin did NEXT: did it hit 3x before falling to 1.55x —
    what holding to 3x would have caught (break-even ~45%) — and did it ever reach 5x.
    Only prompt version `ver` (default: the current one); older versions are counted, not mixed in."""
    if ver >= 3:
        return report_bank_v3(since, until, ver)
    win, wparams = _window("qp.exit_time", since, until)
    wparams = wparams + [ver]
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT b.plan, b.p_3x, b.secs_after_bank, qp.channel_handle, t.symbol,
                   (SELECT min(o.observed_at) FROM qsim_quote_observations o
                     WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time AND o.real_mult >= 3),
                   (SELECT min(o.observed_at) FROM qsim_quote_observations o
                     WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time
                       AND NOT coalesce(o.rate_limited, false)
                       AND (o.real_mult <= 1.55 OR coalesce(o.no_route, false))),
                   (SELECT max(o.real_mult) FROM qsim_quote_observations o
                     WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time),
                   b.verdict->>'reason'
            FROM trader_brain b JOIN qsim_positions qp ON qp.call_id = b.call_id
            JOIN tokens t ON t.id = qp.token_id
            WHERE b.phase = 'bank' AND {win} AND b.prompt_v = %s
              AND qp.exit_time <= now() - interval '2 hours'""", wparams)
        rows = cur.fetchall()
        cur.execute(f"""SELECT count(*) FROM trader_brain b JOIN qsim_positions qp ON qp.call_id = b.call_id
                        WHERE b.phase = 'bank' AND {win} AND b.prompt_v <> %s""", wparams)
        other_versions = cur.fetchone()[0]
    print(f"\n{'=' * 22} AT THE 2x BANK: take it or hold for 3x? (prompt v{ver}, {_span(since, until)}) {'=' * 22}")
    if other_versions:
        print(f"  ({other_versions} decisions from other prompt versions left out — use --bank-v N to see them)")
    if not rows:
        print("  no graded at-bank decisions yet (each needs 2 h of post-bank quotes)")
        return
    items = []
    for dec, p3, late, ch, sym, t3, tfloor, high, reason in rows:
        items.append({"dec": dec, "p3": p3, "late": late, "ch": ch or "?", "sym": sym, "reason": reason,
                      "run": t3 is not None and (tfloor is None or t3 < tfloor),
                      "x5": float(high or 0) >= 5, "high": float(high or 0)})

    def line(label, g):
        if not g:
            print(f"  {label:22s}    0")
            return
        n = len(g)
        print(f"  {label:22s} {n:4d}  hit 3x before 1.55x {sum(i['run'] for i in g) / n * 100:5.1f}%   "
              f"ever 5x {sum(i['x5'] for i in g) / n * 100:5.1f}%")

    print(f"  {len(items)} banked coins judged at the bank, graded 2 h+ later. Break-even for holding ~45%.")
    line("ALL", items)
    line("said HOLD", [i for i in items if i["dec"] == "hold"])
    line("said BANK", [i for i in items if i["dec"] == "bank"])
    print("  by its chance of 3x before 1.55x:")
    for lo, hi in ((0, 30), (30, 45), (45, 60), (60, 101)):
        line(f"  {lo}-{hi - 1 if hi < 101 else 100}%", [i for i in items if i["p3"] is not None and lo <= i["p3"] < hi])
    for ch in sorted({i["ch"] for i in items}):
        print(f"  -- {ch} --")
        line("said HOLD", [i for i in items if i["ch"] == ch and i["dec"] == "hold"])
        line("said BANK", [i for i in items if i["ch"] == ch and i["dec"] == "bank"])
    late = [i["late"] for i in items if i["late"] is not None]
    if late:
        print(f"  asked a median {statistics.median(late):.0f}s after the bank (its view is fixed at the bank either way)")
    holds = sorted((i for i in items if i["dec"] == "hold"), key=lambda i: -i["high"])
    if holds:
        print("  its HOLD calls (best first):")
        for i in holds[:10]:
            print(f"    {str(i['sym'])[:12]:12s} {i['ch'][:16]:16s} {'RAN' if i['run'] else 'fell back':9s} "
                  f"high {i['high']:5.1f}x  — {str(i['reason'])[:90]}")
    print("  Graded on its NUMBER: coins it rates 50%+ (= HOLD) must hit 3x-before-1.55x clearly above ~45%\n"
          "  AND clearly above the coins it rates <30%, on 30+ coins and again on the next 30. Only then does\n"
          "  it earn an exit rule (hold those for 3x).")


if __name__ == "__main__":
    if "--report" in sys.argv:
        # `--report | head -45` closes the pipe early; exit quietly instead of a BrokenPipeError trace
        import signal
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        if "--playbook" in sys.argv:
            report_playbook()
            sys.exit(0)
        if "--bank" in sys.argv:
            i = sys.argv.index("--since") if "--since" in sys.argv else -1
            k = sys.argv.index("--until") if "--until" in sys.argv else -1
            bv = sys.argv.index("--bank-v") if "--bank-v" in sys.argv else -1
            report_bank(sys.argv[i + 1] if i > 0 else "2026-11-10", sys.argv[k + 1] if k > 0 else None,
                        int(sys.argv[bv + 1]) if bv > 0 else BANK_V)
            sys.exit(0)
        i = sys.argv.index("--since") if "--since" in sys.argv else -1
        since = sys.argv[i + 1] if i > 0 else "2026-11-05"
        k = sys.argv.index("--until") if "--until" in sys.argv else -1
        until = sys.argv[k + 1] if k > 0 else None
        j = sys.argv.index("--v") if "--v" in sys.argv else -1
        if j > 0:
            report(since, int(sys.argv[j + 1]), until=until)
        else:
            paired_report(since, until=until)
            for ver in PAIR:
                tag = {LEARN_V: "(learning: similar past coins)", PLAYBOOK_V: "(learning loop: nightly playbook)"}
                print(f"\n{'=' * 30} PROMPT v{ver} {tag.get(ver, '')} {'=' * 30}")
                report(since, ver, until=until)
    elif "--dry" in sys.argv:
        # print the context the model would see for one call_id, without calling the API
        i = sys.argv.index("--dry")
        print(json.dumps(build_context(int(sys.argv[i + 1])), indent=2, default=str))
    else:
        run()
