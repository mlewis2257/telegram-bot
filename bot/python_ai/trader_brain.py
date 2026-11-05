"""
trader_brain.py — judge each call like a trader (story + meme + flow + holders), RECORD ONLY.

WHY (2026-11-04). Every rule tested so far looked at one thing at a time. A trader reads the
whole story together: AMD had real news, an original pun ("A Meme Dog"), volume coming in and
clean holders -> hold longer; a lazy copycat with no story -> get out early or skip. The one
ingredient our data never had is the narrative judgement (is the story real and fresh, is the
meme original and funny or the 30th copy). This asks Claude for that judgement on every
solwhaletrending qsim trade and stores its plan next to the trade. Nothing acts on it until
`--report` shows the plans line up with what the coins actually did.

WHEN. TRADER_BRAIN_DELAY_SECS after qsim's entry (default 45 s), so the first quotes are in —
a trader looks for a moment before deciding. Only data up to that moment goes in: no outcome.

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
Report:  python3 trader_brain.py --report [--since 2026-11-05]
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

SYSTEM = """You are an experienced Solana meme-coin trader reviewing a coin a Telegram call channel just
called. A bot has already bought a small position at the price shown. Most called coins fail; a
few run 5-50x. Runners usually combine a REAL, FRESH story (news, a viral moment, a culture
reference people get), an ORIGINAL meme (clever, funny, not the 30th copy of the same ticker),
real buying (volume, many holders, not one bundle), and holders who are not about to dump.
Copycats of a story that already ran, lazy tickers, heavy bundles/snipers, a dev who launches
dozens of coins, or a coin already far above the call price are the usual losers.

Judge ONLY from the data given. Answer with one JSON object and nothing else:
{"story": 0-3, "story_note": "<what the story is, or 'none'>",
 "meme": 0-3, "meme_note": "<originality/humour in a few words>",
 "copycat": true/false,
 "flow": 0-3, "attention": "early" | "peaking" | "late",
 "risk": 0-3, "risk_note": "<biggest danger in a few words>",
 "win_condition": "<what has to happen for this trade to work>",
 "invalidated_if": "<what would prove it wrong>",
 "plan": "hold_longer" | "normal" | "exit_early" | "skip",
 "confidence": 1-5,
 "reason": "<one sentence>"}
story/meme/flow: 0 = none/bad, 3 = strong. risk: 0 = clean, 3 = likely rug/dump.
attention: early = the story/coin is just starting to get noticed; late = already obvious and run up.
Missing a coin costs nothing; a forced bad trade does — use skip/exit_early freely.
plan: hold_longer = could be a runner, worth holding past the usual 2x take-profit;
normal = take the usual 2x / stop; exit_early = weak, take a small profit or get out soon;
skip = should not have been bought."""

_day = None
_n_today = 0
_usd_today = 0.0
_capped_noted = False
_failed: dict[int, int] = {}


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


def build_context(call_id: int) -> dict | None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT qp.entry_time, qp.entry_price, qp.sol_in, c.mcap_at_call, c.conviction_score,
                   c.raw_message, c.message_type, c.narrative_tags, c.created_at,
                   to_jsonb(t) AS tok, t.mint_address, t.symbol, t.name
            FROM qsim_positions qp JOIN calls c ON c.id = qp.call_id JOIN tokens t ON t.id = qp.token_id
            WHERE qp.call_id = %s""", (call_id,))
        row = cur.fetchone()
        if not row:
            return None
        (entry_time, entry_price, sol_in, mcap_call, score, raw, mtype, tags, called_at,
         tok, mint, symbol, name) = row
        judged = datetime.now(timezone.utc)

        cur.execute("""SELECT round(extract(epoch FROM observed_at - %s)) AS s, round(real_mult, 3)
                       FROM qsim_quote_observations
                       WHERE call_id = %s AND real_mult > 0 AND observed_at <= %s
                       ORDER BY observed_at""", (entry_time, call_id, judged))
        path = [{"secs": int(s), "mult": float(m)} for s, m in cur.fetchall()][-12:]

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
    return {
        "symbol": symbol, "name": name,
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


# ── Judge ───────────────────────────────────────────────────────────────────────

def judge(ctx: dict) -> tuple[dict | None, int, int, str | None]:
    """(verdict, input tokens, output tokens, error)."""
    r = requests.post(
        "https://api.anthropic.com/v1/messages",
        headers={"x-api-key": API_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
        json={"model": MODEL, "max_tokens": 400, "system": SYSTEM,
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
    if v.get("plan") not in PLANS:
        return None, tin, tout, f"bad plan: {v.get('plan')}"
    return v, tin, tout, None


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
        return [r[0] for r in cur.fetchall() if _failed.get(r[0], 0) < 2]


def _int(v):
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


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
    print(f"[trader_brain] RECORD-ONLY. model={MODEL}, channels={','.join(CHANNELS)}, judges each qsim trade "
          f"{DELAY_SECS:g}s after entry. Ceilings: {MAX_PER_DAY} judgements/day and ${MAX_USD_PER_DAY:.2f}/day "
          f"(~${MAX_USD_PER_DAY * 30:.0f}/month max) at ${PRICE_IN:g}/${PRICE_OUT:g} per M tokens.", flush=True)
    last_tally = time.monotonic()
    tally = defaultdict(int)
    while True:
        today = datetime.now(timezone.utc).date()
        if today != _day:
            _day, _n_today, _usd_today, _capped_noted = today, 0, 0.0, False
        try:
            for call_id in due():
                if _n_today >= MAX_PER_DAY or _usd_today >= MAX_USD_PER_DAY:
                    if not _capped_noted:
                        print(f"[trader_brain] daily ceiling reached ({_n_today} judgements, ${_usd_today:.2f}) — "
                              f"none until UTC midnight", flush=True)
                        _capped_noted = True
                    break
                t0 = time.monotonic()
                ctx = build_context(call_id)
                ctx_ms = int((time.monotonic() - t0) * 1000)
                if not ctx:
                    continue
                t1 = time.monotonic()
                try:
                    v, tin, tout, err = judge(ctx)
                except requests.RequestException as e:
                    v, tin, tout, err = None, 0, 0, f"{type(e).__name__}: {e}"
                api_ms = int((time.monotonic() - t1) * 1000)
                _usd_today += tin / 1e6 * PRICE_IN + tout / 1e6 * PRICE_OUT
                if v is None:
                    _failed[call_id] = _failed.get(call_id, 0) + 1
                    tally["failed"] += 1
                    print(f"[trader_brain] call_id={call_id} {ctx.get('symbol')} failed: {err}", flush=True)
                    continue
                _n_today += 1
                tally[v["plan"]] += 1
                conn = db.get_conn()
                with conn.cursor() as cur:
                    cur.execute("""INSERT INTO trader_brain (call_id, model, delay_secs, context, plan, story, meme,
                                   flow, risk, copycat, confidence, verdict, in_tokens, out_tokens,
                                   attention, ctx_ms, api_ms)
                                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                                (call_id, MODEL, ctx["our_entry"]["secs_since_entry"], json.dumps(ctx, default=str),
                                 v["plan"], _int(v.get("story")), _int(v.get("meme")), _int(v.get("flow")),
                                 _int(v.get("risk")), bool(v.get("copycat")), _int(v.get("confidence")),
                                 json.dumps(v), tin, tout, str(v.get("attention") or "")[:10] or None,
                                 ctx_ms, api_ms))
                conn.commit()
                print(f"[trader_brain] {ctx.get('symbol')} call_id={call_id}: {v['plan'].upper()} "
                      f"(story {v.get('story')}, meme {v.get('meme')}, flow {v.get('flow')}, risk {v.get('risk')}, "
                      f"conf {v.get('confidence')}, {v.get('attention')}) — {str(v.get('reason'))[:140]} "
                      f"[data {ctx_ms} ms, model {api_ms} ms]", flush=True)
        except Exception as e:
            db.safe_rollback()
            print(f"[trader_brain] pass failed: {type(e).__name__}: {e}", flush=True)
        if time.monotonic() - last_tally >= 3600:
            print("[trader_brain] 1h: " + " ".join(f"{k}={v}" for k, v in sorted(tally.items()))
                  + f" today={_n_today} ${_usd_today:.3f}", flush=True)
            tally.clear()
            last_tally = time.monotonic()
        time.sleep(PASS_SECS)


# ── Report ──────────────────────────────────────────────────────────────────────

def report(since: str) -> None:
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT qp.channel_handle, b.plan, b.story, b.meme, b.flow, b.risk, b.copycat, b.confidence, b.attention,
                   b.ctx_ms, b.api_ms,
                   qp.sol_in, qp.sol_out, qp.exit_reason, qp.peak_multiplier, b.judged_at, qp.exit_time,
                   (SELECT max(o.real_mult) FROM qsim_quote_observations o
                     WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time
                       AND o.real_mult IS NOT NULL) AS post_max
            FROM trader_brain b JOIN qsim_positions qp ON qp.call_id = b.call_id
            WHERE qp.status = 'closed' AND qp.sol_in > 0 AND b.judged_at >= %s""", (since,))
        rows = cur.fetchall()
    if not rows:
        print(f"no closed, judged trades since {since}")
        return
    items = []
    speed = []
    for (channel, plan, story, meme, flow, risk, copycat, conf, attention, ctx_ms, api_ms,
         s_in, s_out, reason, peak, judged, exit_t, post) in rows:
        if ctx_ms is not None and api_ms is not None:
            speed.append((ctx_ms, api_ms))
        x = float(s_out or 0) / float(s_in)
        hi = max(float(peak or 0), float(post or 0))
        items.append({"channel": channel, "plan": plan, "attention": attention, "story": story, "meme": meme, "flow": flow, "risk": risk,
                      "copycat": "yes" if copycat else "no", "confidence": conf,
                      "pnl": float(s_out or 0) - float(s_in), "s_in": float(s_in),
                      "bank": "bank" in (reason or ""), "rug": x < 0.30, "hi": hi,
                      "closed_before_judged": exit_t is not None and exit_t <= judged})

    def line(label, g):
        n = len(g)
        s_in = sum(i["s_in"] for i in g) or 1e-9
        his = sorted(i["hi"] for i in g)
        print(f"  {label:16s} {n:4d}  bank {sum(i['bank'] for i in g) / n * 100:5.1f}%  "
              f"rug {sum(i['rug'] for i in g) / n * 100:5.1f}%  reached 3x {sum(i['hi'] >= 3 for i in g) / n * 100:5.1f}%  "
              f"5x {sum(i['hi'] >= 5 for i in g) / n * 100:5.1f}%  median high {statistics.median(his):5.2f}x  "
              f"P&L {sum(i['pnl'] for i in g) / s_in * 100:+6.1f}%/SOL")

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
    for key in ("attention", "story", "meme", "flow", "risk", "copycat", "confidence"):
        print(f"\nBY {key.upper()}")
        groups = defaultdict(list)
        for i in items:
            groups[i[key]].append(i)
        for val in sorted(groups, key=lambda v: (v is None, str(v))):
            line(str(val), groups[val])
    print("\nUseful only if hold_longer clearly beats ALL on 3x/5x AND exit_early/skip clearly rug more,\n"
          "on a decent count, and again on a second week. Then (and only then) wire plans into qsim exits.")


if __name__ == "__main__":
    if "--report" in sys.argv:
        i = sys.argv.index("--since") if "--since" in sys.argv else -1
        report(sys.argv[i + 1] if i > 0 else "2026-11-05")
    elif "--dry" in sys.argv:
        # print the context the model would see for one call_id, without calling the API
        i = sys.argv.index("--dry")
        print(json.dumps(build_context(int(sys.argv[i + 1])), indent=2, default=str))
    else:
        run()
