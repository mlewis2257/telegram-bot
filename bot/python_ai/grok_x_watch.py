"""
grok_x_watch.py — what is X saying about a coin right now? (Grok live X search, RECORD ONLY)

WHY (2026-11-04). The trader checklist asks "is attention accelerating or already exhausted?"
and none of our data sees X. Grok's API can search X live (server-side tool `x_search`). For
each qsim trade on GROK_X_CHANNELS it asks Grok to search X for the coin's contract address and
ticker and report, from the posts it actually fetched: how many posts, how many in the last 30
minutes, whether that is growing, how many distinct and sizeable accounts, how bot-like it looks,
and whether the coin's source tweet went viral. Stored in grok_x; nothing acts on it and the
trader brain does NOT see it yet — it is graded on its own first (`--report`).

COST. x_search is billed per post fetched ($5 per 1k) and per profile ($10 per 1k) on top of the
model's tokens (docs.x.ai, checked 2026-11-04), so this is ~30-80x the cost of a trader_brain
judgement. Hence one channel and tight ceilings: GROK_X_MAX_PER_DAY coins and
GROK_X_MAX_USD_PER_DAY (estimated: tokens at GROK_X_PRICE_IN/OUT + cited posts at $0.005 —
the xAI console's billing page is the real number; set a spending limit there too).
No credits / billing errors: logged once, retried every 30 min.

Key: XAI_API_KEY in .env (GROK_API_KEY also accepted). Model: GROK_X_MODEL (default grok-4.3).

2026-11-08: the 60 s entry look was noise over 94 coins, so the default is now GROK_X_MODE=bank —
one look right after qsim banks a coin at 2x (see MODE below). GROK_X_MODE=entry restores the old look.

Run:     pm2 start grok_x_watch.py --name grok-x --interpreter <venv python>
Report:  python3 grok_x_watch.py --report [--since 2026-11-05]               (entry looks)
         python3 grok_x_watch.py --report --phase bank [--since 2026-11-09]  (at-bank looks)
"""

from __future__ import annotations

import json
import os
import re
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

sys.path.insert(0, str(Path(__file__).resolve().parent))
import db  # noqa: E402
import api_alerts  # noqa: E402

MODEL = os.getenv("GROK_X_MODEL", "grok-4.3")
CHANNELS = [c.strip().lstrip("@") for c in
            os.getenv("GROK_X_CHANNELS", "solwhaletrending").split(",") if c.strip()]
DELAY_SECS = float(os.getenv("GROK_X_DELAY_SECS", "60"))
MAX_PER_DAY = int(os.getenv("GROK_X_MAX_PER_DAY", "40"))
MAX_USD_PER_DAY = float(os.getenv("GROK_X_MAX_USD_PER_DAY", "3.0"))
PRICE_IN = float(os.getenv("GROK_X_PRICE_IN", "1.25"))     # $ per M tokens, grok-4.3
PRICE_OUT = float(os.getenv("GROK_X_PRICE_OUT", "2.50"))
PRICE_PER_POST = 0.005
# Sample every Nth coin (call_id % N == 0) so the daily cap spreads over the whole day instead of
# running out each afternoon (time-of-day bias). 2 = every second coin, ~30/day on solwhaletrending.
EVERY = max(1, int(os.getenv("GROK_X_EVERY", "2")))
PASS_SECS = 15
BILLING_BACKOFF_S = 1800
# 2026-11-08: WHEN to look. 'entry' (the original) searched X DELAY_SECS after the qsim entry; over 94
# coins it was noise — 60 s after a fresh call X holds mostly bot posts, and an X search is too slow to
# gate a buy anyway. 'bank' (default) looks right after qsim BANKS at 2x, every banked coin on
# CHANNELS: the open question is the exit — after 2x, runners and fades look alike on price, and a
# coin whose X attention is taking off is exactly what price cannot show. Graded on whether the coin
# then hit 3x before falling to 1.55x (`--report --phase bank`).
MODE = os.getenv("GROK_X_MODE", "bank").strip().lower()
BANK_WINDOW_MIN = 20          # a bank older than this is skipped (the moment has passed)

PROMPT = """Search X for posts about this Solana meme coin from the last 24 hours. Search for the
contract address first, then the ticker with a $ sign (and the name if the ticker is generic).
Do ONE or TWO searches only; do not fetch more than about 30 posts.

Coin: ticker ${symbol}, name "{name}", contract address {mint}
It was called on Telegram about {mins_ago} minutes ago. Current time (UTC): {now}
{source_tweet}
Answer with ONE JSON object and nothing else, using ONLY the posts you actually fetched (use 0 or
null when you found nothing — never guess):
{{"posts_found": <int>, "posts_last_30min": <int>, "posts_30_to_120min": <int>,
 "growing": true/false, "distinct_accounts": <int>, "accounts_1k_plus_followers": <int>,
 "notable_accounts": ["@handle (followers)", ...],
 "bot_like_pct": 0-100,
 "source_tweet_viral": true/false/null, "source_tweet_note": "<likes/reposts if seen, else null>",
 "summary": "<one sentence: what X is saying>"}}"""

_day = None
_n_today = 0
_usd_today = 0.0
_capped_noted = False
_billing_until = 0.0
_streak = api_alerts.FailStreak("grok_x")


def check_credit() -> str:
    """Warn on Telegram at 75% / 90% of XAI_CREDIT_USD used (from the usd_est column — an estimate)."""
    cs = api_alerts.credit_settings("XAI")
    if not cs:
        return ""
    try:
        conn = db.get_conn()
        db.safe_rollback()
        with conn.cursor() as cur:
            cur.execute("SELECT coalesce(sum(usd_est), 0) FROM grok_x WHERE judged_at >= %s", (cs[1],))
            spent = float(cur.fetchone()[0])
        return " " + api_alerts.credit_check("grok_x", "XAI", spent, cs[0])
    except Exception as e:
        db.safe_rollback()
        return f" credit check failed: {type(e).__name__}"
_failed: dict[int, int] = {}


def api_key() -> str:
    return os.getenv("XAI_API_KEY", "") or os.getenv("GROK_API_KEY", "")


def ensure_table() -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS grok_x (
                call_id      integer PRIMARY KEY,
                judged_at    timestamptz NOT NULL DEFAULT now(),
                model        text,
                posts_found  integer,
                posts_30m    integer,
                posts_30_120 integer,
                growing      boolean,
                accounts     integer,
                accounts_1k  integer,
                bot_pct      integer,
                source_viral boolean,
                verdict      jsonb,
                citations    integer,
                usage        jsonb,
                usd_est      numeric,
                api_ms       integer
            )""")
        # one row per (coin, phase): 'entry' rows are the original 60 s looks, 'bank' rows the at-bank look
        cur.execute("ALTER TABLE grok_x ADD COLUMN IF NOT EXISTS phase text NOT NULL DEFAULT 'entry'")
        cur.execute("""
            DO $$ BEGIN
              IF EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'grok_x_pkey'
                         AND array_length(conkey, 1) = 1) THEN
                ALTER TABLE grok_x DROP CONSTRAINT grok_x_pkey;
                ALTER TABLE grok_x ADD PRIMARY KEY (call_id, phase);
              END IF;
            END $$""")
        conn.commit()


def due() -> list[tuple]:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT qp.call_id, t.symbol, t.name, t.mint_address, c.created_at,
                   (SELECT nt.uri FROM new_tokens nt WHERE nt.mint = t.mint_address)
            FROM qsim_positions qp JOIN tokens t ON t.id = qp.token_id JOIN calls c ON c.id = qp.call_id
            WHERE qp.channel_handle = ANY(%s)
              AND qp.entry_time <= now() - make_interval(secs => %s)
              AND qp.entry_time >= now() - interval '20 minutes'
              AND t.mint_address NOT LIKE 'UNKNOWN:%%' AND t.mint_address NOT LIKE 'INFERRED:%%'
              AND NOT EXISTS (SELECT 1 FROM grok_x g WHERE g.call_id = qp.call_id AND g.phase = 'entry')
            ORDER BY qp.entry_time""", (CHANNELS, DELAY_SECS))
        return [r for r in cur.fetchall() if r[0] % EVERY == 0 and _failed.get(r[0], 0) < 2]


def due_bank() -> list[tuple]:
    """Every coin qsim banked on CHANNELS in the last BANK_WINDOW_MIN minutes, not yet looked at."""
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT qp.call_id, t.symbol, t.name, t.mint_address, c.created_at,
                   (SELECT nt.uri FROM new_tokens nt WHERE nt.mint = t.mint_address)
            FROM qsim_positions qp JOIN tokens t ON t.id = qp.token_id JOIN calls c ON c.id = qp.call_id
            WHERE qp.channel_handle = ANY(%s)
              AND qp.status = 'closed' AND qp.exit_reason LIKE '%%bank%%'
              AND qp.exit_time >= now() - make_interval(mins => %s)
              AND t.mint_address NOT LIKE 'UNKNOWN:%%' AND t.mint_address NOT LIKE 'INFERRED:%%'
              AND NOT EXISTS (SELECT 1 FROM grok_x g WHERE g.call_id = qp.call_id AND g.phase = 'bank')
            ORDER BY qp.exit_time""", (CHANNELS, BANK_WINDOW_MIN))
        return [r for r in cur.fetchall() if _failed.get(r[0], 0) < 2]


def _source_tweet(uri) -> str:
    try:
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "launcher"))
        import tweet_source_report as tsr
        m, _ = tsr.fetch_meta(uri)
        if m:
            cls, acc, tid = tsr.classify(str(m.get("twitter") or ""), str(m.get("website") or ""))
            if cls == "tweet":
                return f"The coin's metadata links this source tweet: https://x.com/{acc}/status/{tid}"
    except Exception:
        pass
    return ""


def ask(symbol, name, mint, called_at, uri) -> tuple[dict | None, dict, int, str | None]:
    """(verdict, usage, citations, error)."""
    now = datetime.now(timezone.utc)
    prompt = PROMPT.format(symbol=symbol or "?", name=name or "?", mint=mint,
                           mins_ago=round((now - called_at).total_seconds() / 60),
                           now=now.strftime("%Y-%m-%d %H:%M"), source_tweet=_source_tweet(uri))
    r = requests.post(
        "https://api.x.ai/v1/responses",
        headers={"Authorization": f"Bearer {api_key()}", "Content-Type": "application/json"},
        json={"model": MODEL, "input": [{"role": "user", "content": prompt}],
              "tools": [{"type": "x_search", "from_date": (now - timedelta(days=1)).strftime("%Y-%m-%d")}]},
        timeout=120)
    if r.status_code != 200:
        return None, {}, 0, f"http {r.status_code}: {r.text[:240]}"
    j = r.json()
    usage = j.get("usage") or {}
    text, cites = "", 0
    for item in j.get("output") or []:
        if item.get("type") == "message":
            for part in item.get("content") or []:
                if part.get("type") in ("output_text", "text"):
                    text += part.get("text") or ""
                    cites += len(part.get("annotations") or [])
    cites = max(cites, len(j.get("citations") or []))
    m = re.search(r"\{.*\}", text, re.S)
    if not m:
        return None, usage, cites, f"no JSON in reply: {text[:160]}"
    try:
        return json.loads(m.group(0)), usage, cites, None
    except ValueError:
        return None, usage, cites, f"bad JSON: {m.group(0)[:160]}"


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _usd(usage: dict, cites: int) -> float:
    tin = int(usage.get("input_tokens") or usage.get("prompt_tokens") or 0)
    tout = int(usage.get("output_tokens") or usage.get("completion_tokens") or 0)
    return tin / 1e6 * PRICE_IN + tout / 1e6 * PRICE_OUT + cites * PRICE_PER_POST


def run() -> None:
    global _day, _n_today, _usd_today, _capped_noted, _billing_until
    if not api_key():
        print("[grok_x] XAI_API_KEY is not set in .env — waiting (re-checking every 10 min).", flush=True)
        while not api_key():
            time.sleep(600)
            load_dotenv(Path(__file__).with_name(".env"), override=True)
    ensure_table()
    if MODE not in ("bank", "entry"):
        print(f"[grok_x] GROK_X_MODE={MODE!r} unknown — using 'bank'", flush=True)
    when = (f"{DELAY_SECS:g}s after entry, every {EVERY}{'nd' if EVERY == 2 else 'th'} coin" if MODE == "entry"
            else f"right after qsim BANKS at 2x (every banked coin, within {BANK_WINDOW_MIN} min)")
    print(f"[grok_x] RECORD-ONLY, mode={MODE}. model={MODEL}, X search for {','.join(CHANNELS)} qsim trades "
          f"{when}. Ceilings: {MAX_PER_DAY} coins/day and ~${MAX_USD_PER_DAY:.2f}/day (estimated; posts "
          f"${PRICE_PER_POST * 1000:g}/1k + tokens — check the xAI console for real spend).", flush=True)
    print("[grok_x] alerts: out-of-credit / failing streak → Telegram."
          + (check_credit() or " Credit warning off (set XAI_CREDIT_USD + XAI_CREDIT_SINCE)."), flush=True)
    last_credit = time.monotonic()
    while True:
        today = datetime.now(timezone.utc).date()
        if today != _day:
            _day, _n_today, _usd_today, _capped_noted = today, 0, 0.0, False
        if time.monotonic() - last_credit >= 3600:
            status = check_credit()
            if status:
                print(f"[grok_x] 1h:{status}", flush=True)
            last_credit = time.monotonic()
        if time.monotonic() < _billing_until:
            time.sleep(PASS_SECS)
            continue
        try:
            phase = "entry" if MODE == "entry" else "bank"
            for call_id, symbol, name, mint, called_at, uri in (due() if phase == "entry" else due_bank()):
                if _n_today >= MAX_PER_DAY or _usd_today >= MAX_USD_PER_DAY:
                    if not _capped_noted:
                        print(f"[grok_x] daily ceiling reached ({_n_today} coins, ~${_usd_today:.2f}) — "
                              f"none until UTC midnight", flush=True)
                        _capped_noted = True
                    break
                t0 = time.monotonic()
                try:
                    v, usage, cites, err = ask(symbol, name, mint, called_at, uri)
                except requests.RequestException as e:
                    v, usage, cites, err = None, {}, 0, f"{type(e).__name__}: {e}"
                api_ms = int((time.monotonic() - t0) * 1000)
                usd = _usd(usage, cites)
                _usd_today += usd
                if v is None:
                    low = (err or "").lower()
                    if any(k in low for k in ("http 402", "http 403", "credit", "billing", "spending limit",
                                              "insufficient")):
                        print(f"[grok_x] xAI billing/permission error — no credits yet? Retrying in "
                              f"{BILLING_BACKOFF_S // 60} min. ({err[:160]})", flush=True)
                        api_alerts.send("xai_billing", f"grok_x: xAI refused — out of credit or bad key. "
                                        f"Retrying every {BILLING_BACKOFF_S // 60} min. {err[:200]}")
                        _billing_until = time.monotonic() + BILLING_BACKOFF_S
                        break
                    _streak.fail(err)
                    _failed[call_id] = _failed.get(call_id, 0) + 1
                    print(f"[grok_x] call_id={call_id} {symbol} failed: {err}", flush=True)
                    continue
                _streak.ok()
                _n_today += 1
                conn = db.get_conn()
                with conn.cursor() as cur:
                    cur.execute("""INSERT INTO grok_x (call_id, model, posts_found, posts_30m, posts_30_120, growing,
                                   accounts, accounts_1k, bot_pct, source_viral, verdict, citations, usage, usd_est,
                                   api_ms, phase) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                   ON CONFLICT DO NOTHING""",
                                (call_id, MODEL, _int(v.get("posts_found")), _int(v.get("posts_last_30min")),
                                 _int(v.get("posts_30_to_120min")), bool(v.get("growing")),
                                 _int(v.get("distinct_accounts")), _int(v.get("accounts_1k_plus_followers")),
                                 _int(v.get("bot_like_pct")),
                                 v.get("source_tweet_viral") if isinstance(v.get("source_tweet_viral"), bool) else None,
                                 json.dumps(v), cites, json.dumps(usage), round(usd, 4), api_ms, phase))
                conn.commit()
                print(f"[grok_x] {symbol} call_id={call_id} at {phase}: posts {v.get('posts_found')} "
                      f"(30m {v.get('posts_last_30min')}, {'growing' if v.get('growing') else 'not growing'}), "
                      f"accounts {v.get('distinct_accounts')} / 1k+ {v.get('accounts_1k_plus_followers')}, "
                      f"bot-like {v.get('bot_like_pct')}% — {str(v.get('summary'))[:120]} "
                      f"[{api_ms / 1000:.1f}s, ~${usd:.3f}, today ~${_usd_today:.2f}]", flush=True)
        except Exception as e:
            db.safe_rollback()
            print(f"[grok_x] pass failed: {type(e).__name__}: {e}", flush=True)
        time.sleep(PASS_SECS)


def report(since: str) -> None:
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT g.posts_found, g.posts_30m, g.growing, g.accounts_1k, g.bot_pct, g.source_viral, g.usd_est,
                   qp.sol_in, qp.sol_out, qp.exit_reason, qp.peak_multiplier,
                   (SELECT max(o.real_mult) FROM qsim_quote_observations o
                     WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time AND o.real_mult IS NOT NULL)
            FROM grok_x g JOIN qsim_positions qp ON qp.call_id = g.call_id
            WHERE qp.status = 'closed' AND qp.sol_in > 0 AND g.judged_at >= %s
              AND g.phase = 'entry'""", (since,))
        rows = cur.fetchall()
    if not rows:
        print(f"no closed trades with X data since {since}")
        return
    items = []
    for pf, p30, grow, a1k, bot, viral, usd, s_in, s_out, reason, peak, post in rows:
        x = float(s_out or 0) / float(s_in)
        b = lambda v, cuts, labs: "n/a" if v is None else next((l for c, l in zip(cuts, labs) if v < c), labs[-1])
        items.append({"posts": b(pf, [1, 5, 20], ["0", "1-4", "5-19", "20+"]),
                      "last 30m": b(p30, [1, 3, 10], ["0", "1-2", "3-9", "10+"]),
                      "growing": "yes" if grow else "no",
                      "1k+ accounts": b(a1k, [1, 3], ["0", "1-2", "3+"]),
                      "bot-like": b(bot, [30, 60], ["<30%", "30-59%", "60%+"]),
                      "source viral": {True: "yes", False: "no"}.get(viral, "n/a"),
                      "pnl": float(s_out or 0) - float(s_in), "s_in": float(s_in), "usd": float(usd or 0),
                      "bank": "bank" in (reason or ""), "rug": x < 0.30,
                      "hi": max(float(peak or 0), float(post or 0))})

    def line(label, g):
        n = len(g)
        s = sum(i["s_in"] for i in g) or 1e-9
        print(f"  {label:14s} {n:4d}  bank {sum(i['bank'] for i in g) / n * 100:5.1f}%  "
              f"rug {sum(i['rug'] for i in g) / n * 100:5.1f}%  3x {sum(i['hi'] >= 3 for i in g) / n * 100:5.1f}%  "
              f"5x {sum(i['hi'] >= 5 for i in g) / n * 100:5.1f}%  P&L {sum(i['pnl'] for i in g) / s * 100:+6.1f}%/SOL")

    print(f"{len(items)} closed trades with X data since {since}; est. spend ${sum(i['usd'] for i in items):.2f} "
          f"(median ${statistics.median(i['usd'] for i in items):.3f}/coin)\n")
    line("ALL", items)
    for key in ("posts", "last 30m", "growing", "1k+ accounts", "bot-like", "source viral"):
        print(f"\nBY {key.upper()}")
        g = defaultdict(list)
        for i in items:
            g[i[key]].append(i)
        for val in sorted(g):
            line(val, g[val])
    print("\nWorth paying for only if a group (e.g. growing + 1k+ accounts) clearly banks more / rugs less on a\n"
          "decent count and again later. Only then feed it to the trader brain.")


def report_bank(since: str) -> None:
    """At-bank looks, graded on what the coin did NEXT: did it hit 3x before falling to 1.55x (what a
    hold-to-3x exit would have caught; break-even ~45%), and did it ever reach 5x."""
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT g.posts_found, g.posts_30m, g.posts_30_120, g.growing, g.accounts_1k, g.bot_pct, g.usd_est,
                   qp.channel_handle,
                   (SELECT min(o.observed_at) FROM qsim_quote_observations o
                     WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time AND o.real_mult >= 3),
                   (SELECT min(o.observed_at) FROM qsim_quote_observations o
                     WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time
                       AND NOT coalesce(o.rate_limited, false)
                       AND (o.real_mult <= 1.55 OR coalesce(o.no_route, false))),
                   (SELECT max(o.real_mult) FROM qsim_quote_observations o
                     WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time)
            FROM grok_x g JOIN qsim_positions qp ON qp.call_id = g.call_id
            WHERE g.phase = 'bank' AND g.judged_at >= %s
              AND qp.exit_time <= now() - interval '2 hours'""", (since,))
        rows = cur.fetchall()
    if not rows:
        print(f"no at-bank X looks older than 2 h since {since} (each needs 2 h of post-bank quotes to grade)")
        return
    b = lambda v, cuts, labs: "n/a" if v is None else next((l for c, l in zip(cuts, labs) if v < c), labs[-1])
    items = []
    for pf, p30, p30_120, grow, a1k, bot, usd, ch, t3, tfloor, high in rows:
        # accelerating = more posts in the last 30 min than in the 90 min before it
        accel = None if p30 is None or p30_120 is None else ("yes" if p30 > p30_120 else "no")
        items.append({"posts": b(pf, [1, 5, 20], ["0", "1-4", "5-19", "20+"]),
                      "last 30m": b(p30, [1, 3, 10], ["0", "1-2", "3-9", "10+"]),
                      "accelerating": accel or "n/a",
                      "growing": "yes" if grow else "no",
                      "1k+ accounts": b(a1k, [1, 3], ["0", "1-2", "3+"]),
                      "bot-like": b(bot, [30, 60], ["<30%", "30-59%", "60%+"]),
                      "channel": ch or "?", "usd": float(usd or 0),
                      "run": t3 is not None and (tfloor is None or t3 < tfloor),
                      "5x": float(high or 0) >= 5})

    def line(label, g):
        n = len(g)
        print(f"  {label:16s} {n:4d}  hit 3x before 1.55x {sum(i['run'] for i in g) / n * 100:5.1f}%   "
              f"ever 5x {sum(i['5x'] for i in g) / n * 100:5.1f}%")

    print(f"{len(items)} banked coins with an at-bank X look since {since} (graded 2 h+ after the bank); "
          f"est. spend ${sum(i['usd'] for i in items):.2f}\n")
    line("ALL", items)
    for key in ("channel", "posts", "last 30m", "accelerating", "growing", "1k+ accounts", "bot-like"):
        print(f"\nBY {key.upper()}")
        g = defaultdict(list)
        for i in items:
            g[i[key]].append(i)
        for val in sorted(g):
            line(val, g[val])
    print("\nThe question: does some X group hit 3x before the 1.55x floor CLEARLY above ~45% (break-even for\n"
          "holding to 3x) on 30+ coins, and again on the next 30? Only then is it worth an exit rule.")


if __name__ == "__main__":
    if "--report" in sys.argv:
        i = sys.argv.index("--since") if "--since" in sys.argv else -1
        since = sys.argv[i + 1] if i > 0 else "2026-11-05"
        j = sys.argv.index("--phase") if "--phase" in sys.argv else -1
        if j > 0 and sys.argv[j + 1] == "bank":
            report_bank(since)
        else:
            report(since)
    else:
        run()
