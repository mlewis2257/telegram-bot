"""
exit_manager.py — a thinking exit, tested in the shadow: manage each banked coin like a trader would.

WHY (2026-11-11). Every FIXED exit after the 2x bank (hold for 2.5-5x, floors 1.55-2.0, trailed
slices) came out about even with or worse than banking — yet MAX went 2.75x -> 39x after its bank
and updog 20x. A fixed rule can't tell those from QUACKY (6x, then zero). A trader doesn't use a
fixed rule: at 3x in ten minutes with volume still rising and new buyers coming in, they raise the
target; when it stalls and the big wallets start selling, they sell for the best price they can.
Replays on stored quotes can't measure that, so this runs FORWARD, in real time, on the coins the
live lanes trade, and is graded against the 2x bank on the same coins.

WHAT. When qsim banks a live-lane coin (a v7 BUY on LIVE_BRAIN_CHANNELS / LIVE_BRAIN_CATEGORIES), a
VIRTUAL position starts at the bank. Live keeps banking at 2x for real; nothing here trades.
  price   qsim's own sell-quote probes after the bank (every 15 s while the coin is >= 1.5x, up to
          6 h) — the same ruler as qsim, so its numbers compare with qsim's.
  sells   at the FIRST quote at or under the stop (coins gap — that is the fill, not the stop
          level), at the first quote at or over a take-profit, or at the first quote after the
          brain says sell. A dead route is a sale at 0.
  asks    the brain (EXIT_BRAIN_MODEL) at the bank, the first time it reaches 3x / 4x / 5x / 7x /
          10x ..., after a drop of EXIT_DROP_PCT from the high since the last ask, and every
          EXIT_CHECK_MIN otherwise. It sees: the coin at the call, the path since 2x, volume and
          buys/sells for the last 5 min vs the last hour (DexScreener), the top holders now vs at
          entry (Helius), an X look at the first 3x (Grok, while xAI credit lasts), its own earlier
          decisions. It answers hold (with a stop and an optional take-profit) or sell.
  safety  before the first answer the stop is 1.8x; a stop is never set under 1.6x; over the daily
          ceilings (calls / $) the brain is not asked and the stop trails at 70% of the high.

CEILINGS (printed at start): EXIT_BRAIN_MAX_CALLS_PER_DAY (80), EXIT_BRAIN_MAX_USD_PER_DAY (0.50),
EXIT_GROK_MAX_PER_DAY (10), EXIT_GROK_MAX_USD_PER_DAY (1.00), EXIT_HOLDER_MAX_RPC_PER_DAY (400).
xAI out of credit -> no X looks for EXIT_GROK_PAUSE_H (6 h), then one retry; Anthropic out of credit
-> mechanical trailing stop, retried every 5 min. Both alert on Telegram (throttled).

Run:     pm2 start exit_manager.py --name exit-manager --interpreter <venv python>
Report:  python3 exit_manager.py --report [--since 2026-11-12]   (Pacific days)
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import api_alerts  # noqa: E402
import data_fetcher  # noqa: E402
import db  # noqa: E402
import trader_brain as tb  # noqa: E402

MODEL = os.getenv("EXIT_BRAIN_MODEL", tb.MODEL)
PRICE_IN = float(os.getenv("EXIT_BRAIN_PRICE_IN", str(tb.PRICE_IN)))
PRICE_OUT = float(os.getenv("EXIT_BRAIN_PRICE_OUT", str(tb.PRICE_OUT)))
CHANNELS = [c.strip().lstrip("@") for c in
            os.getenv("LIVE_BRAIN_CHANNELS", "solearlytrending,solwhaletrending").split(",") if c.strip()]
CATEGORIES = [c.strip() for c in os.getenv("LIVE_BRAIN_CATEGORIES", "shadow_only,low_score").split(",") if c.strip()]
MIN_SCORE = int(float(os.getenv("LIVE_BRAIN_MIN_SCORE", "6")))
BRAIN_V = int(os.getenv("LIVE_BRAIN_VERSION", "7"))

TICK_S = 5
START_WINDOW_MIN = 5            # pick up banks younger than this
CHECK_MIN = float(os.getenv("EXIT_CHECK_MIN", "5"))
DROP_PCT = float(os.getenv("EXIT_DROP_PCT", "25"))
MILESTONES = (3, 4, 5, 7, 10, 15, 20, 30, 50, 100)
OPEN_STOP = 1.8                 # before the first answer
MIN_STOP = 1.6
FALLBACK_TRAIL = 0.70           # no brain (ceiling / outage): stop at 70% of the high
MAX_HOLD_H = 6.0                # qsim's fast probes end here
STALE_MIN = 10                  # no fresh quote for this long: don't ask on old data

MAX_CALLS_PER_DAY = int(os.getenv("EXIT_BRAIN_MAX_CALLS_PER_DAY", "80"))
MAX_USD_PER_DAY = float(os.getenv("EXIT_BRAIN_MAX_USD_PER_DAY", "0.50"))
GROK_ON = os.getenv("EXIT_GROK", "1") != "0"
GROK_MAX_PER_DAY = int(os.getenv("EXIT_GROK_MAX_PER_DAY", "10"))
GROK_MAX_USD_PER_DAY = float(os.getenv("EXIT_GROK_MAX_USD_PER_DAY", "1.0"))
GROK_PAUSE_S = float(os.getenv("EXIT_GROK_PAUSE_H", "6")) * 3600
HOLDER_MAX_RPC_PER_DAY = int(os.getenv("EXIT_HOLDER_MAX_RPC_PER_DAY", "400"))
BILLING_PAUSE_S = 300

EXIT_SYSTEM = """You are a seasoned Solana meme-coin trader managing a position that is already a winner.
It reached 2x of your entry; instead of selling everything there you are holding to catch the rare
coin that runs to 5x, 10x, 40x — while most coins that reach 2x fade back or collapse to zero within
minutes (a dev or bundle dump). Every decision you make is graded against simply having sold at 2x
("sold_2x_at"), so hold only when the data says this one is still running.

You are asked at the 2x, the first time it reaches 3x / 4x / 5x / 7x / 10x..., after a sharp drop from
its high, and every few minutes. Between your answers a simple loop sells if the price reaches your
stop (or your take-profit). Prices are multiples of your entry. Sells fill at the next quote, 15 s
apart, and these coins GAP — a stop at 2.5x often fills at 2.2x — so a stop right under the price
gets you shaken out by noise, and a stop far under it gives back a lot. Size it to how the coin moves.

Read it like a trader:
- momentum: a path still making higher highs on many steps vs one spike already fading;
- volume and buyers: last-5-minute volume and buy count vs the hourly pace (rising = demand still
  arriving, falling = buyers gone); sells outnumbering buys while the price holds = distribution;
- holders: top wallets adding or sitting = fine; the big early wallets shedding = they're exiting
  into the buyers — get ahead of them;
- attention (X, when given): posts growing from real accounts vs none / bots;
- the story at the call: fresh and original vs a tired copy.
When it is clearly still running: hold with a stop that respects its swings, and raise the
take-profit (or leave it empty) to let it run. When it stalls, distribution starts or volume dries
up: tighten the stop or sell now for the best price you can get. Unsure after a big run: protect
most of the gain.

Answer with one JSON object and nothing else:
{"action": "hold" | "sell", "stop": <multiple of entry, below the current price>,
 "take_profit": <multiple of entry above the current price, or null>, "confidence": 1-5,
 "reason": "<one or two sentences: what you see and why>"}
The stop is ignored when you sell; it must be at least 1.6."""

_day = None
_n_today = 0
_usd_today = 0.0
_grok_n = 0
_grok_usd = 0.0
_rpc_today = 0
_pause_until = 0.0
_grok_pause_until = 0.0
_streak = api_alerts.FailStreak("exit_manager")


# ── tables ──────────────────────────────────────────────────────────────────────

def ensure_tables() -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS exit_shadow (
                call_id        integer PRIMARY KEY,
                channel        text,
                symbol         text,
                mint           text,
                bank_time      timestamptz NOT NULL,
                bank_x         numeric,          -- what the 2x bank sold at (the yardstick)
                sol_in         numeric,
                status         text NOT NULL DEFAULT 'open',
                stop           numeric,
                take_profit    numeric,
                pending_sell   boolean NOT NULL DEFAULT false,
                high           numeric,
                high_at        timestamptz,
                peak_since_ask numeric,
                last_mult      numeric,
                last_obs_at    timestamptz,
                milestone      numeric NOT NULL DEFAULT 2,
                last_ask_at    timestamptz,
                decisions      integer NOT NULL DEFAULT 0,
                grok_done      boolean NOT NULL DEFAULT false,
                coin_ctx       jsonb,
                last_holders   jsonb,
                exit_x         numeric,
                exit_reason    text,
                exit_at        timestamptz,
                created_at     timestamptz NOT NULL DEFAULT now()
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS exit_shadow_decisions (
                id          bigserial PRIMARY KEY,
                call_id     integer NOT NULL,
                at          timestamptz NOT NULL DEFAULT now(),
                mult        numeric,
                trigger     text,
                action      text,              -- hold / sell / fallback
                stop        numeric,
                take_profit numeric,
                context     jsonb,
                verdict     jsonb,
                model       text,
                in_tokens   integer,
                out_tokens  integer,
                grok_usd    numeric,
                ms          integer
            )""")
        conn.commit()


# ── data for a decision ─────────────────────────────────────────────────────────

_TOKEN_KEYS = ("bundled_pct", "bundled_sold_pct", "bundle_pct_initial", "bundle_pct_remaining", "sniper_count",
               "liq_at_detection", "vol_1h_at_detection", "fake_vol_pct", "token_age_minutes", "security_flag",
               "holder_count", "hodl_count", "top10_pct")


def coin_at_call(call_id: int) -> dict:
    """The coin as the brain saw it at the call (no metadata fetch — keeps the loop fast)."""
    ctx = tb.build_context(call_id, with_path=False, learn=False, fast=True) or {}
    out = {k: ctx.get(k) for k in ("symbol", "name", "channel", "call", "dev", "copycats", "holders_at_entry",
                                   "followed_wallets_holding", "news_matching_name_last_12h") if ctx.get(k)}
    out["token"] = {k: v for k, v in (ctx.get("token") or {}).items() if k in _TOKEN_KEYS}
    if ctx.get("our_entry"):
        out["entry_mcap"] = ctx["our_entry"].get("entry_mcap")
    return out


def dex_flow(mint: str) -> dict | None:
    """Volume and buys/sells now vs the hourly pace, from the most liquid Solana pair (shared budget)."""
    try:
        if not data_fetcher._dex_available():
            return None
        data_fetcher._dex_note_call()
        data = data_fetcher._get(f"{data_fetcher.DEXSCREENER_URL}/{mint}")
    except Exception:
        return None
    pairs = [p for p in (data or {}).get("pairs") or [] if p.get("chainId") == "solana"]
    if not pairs:
        return None
    p = max(pairs, key=lambda x: (x.get("liquidity") or {}).get("usd") or 0)
    tx, vol, ch = p.get("txns") or {}, p.get("volume") or {}, p.get("priceChange") or {}
    m5, h1 = tx.get("m5") or {}, tx.get("h1") or {}
    v5, v1 = vol.get("m5"), vol.get("h1")
    return {"volume_last_5m_usd": v5, "volume_last_1h_usd": v1,
            "last_5m_vs_hourly_pace": round(v5 / (v1 / 12), 2) if v5 and v1 else None,
            "buys_5m": m5.get("buys"), "sells_5m": m5.get("sells"),
            "buys_1h": h1.get("buys"), "sells_1h": h1.get("sells"),
            "price_change_5m_pct": ch.get("m5"), "price_change_1h_pct": ch.get("h1"),
            "liquidity_usd": (p.get("liquidity") or {}).get("usd"), "mcap_usd": p.get("marketCap")}


def holders_now(mint: str) -> dict | None:
    """Top-20 real-wallet concentration right now (2-3 Helius calls, inside the daily RPC ceiling)."""
    global _rpc_today
    if _rpc_today + 3 > HOLDER_MAX_RPC_PER_DAY:
        return None
    try:
        import holder_snapshots as hs
        _rpc_today += 3
        rows = data_fetcher.fetch_top_holders_helius(mint, 20)
        amount, decimals = data_fetcher._fetch_token_supply_helius(mint)
        if not rows or amount is None or decimals is None:
            return None
        rows = [{**h, "on_curve": hs._on_curve(h["wallet"])} for h in rows]
        return tb._holder_stats(amount / (10 ** decimals), rows)
    except Exception:
        return None


def x_look(row: dict) -> tuple[dict | None, float]:
    """One Grok X look (at the first 3x). (summary, usd). Out of credit -> no looks for GROK_PAUSE_S."""
    global _grok_n, _grok_usd, _grok_pause_until
    if not GROK_ON or time.monotonic() < _grok_pause_until:
        return None, 0.0
    if _grok_n >= GROK_MAX_PER_DAY or _grok_usd >= GROK_MAX_USD_PER_DAY:
        return None, 0.0
    try:
        import grok_x_watch as gx
        if not gx.api_key():
            return None, 0.0
        c = row.get("coin_ctx") or {}
        v, usage, cites, err = gx.ask(row.get("symbol"), c.get("name"), row.get("mint"),
                                      datetime.fromisoformat(c["called_at"]) if c.get("called_at") else row["bank_time"],
                                      c.get("uri"))
        usd = gx._usd(usage, cites)
    except Exception as e:
        v, usd, err = None, 0.0, f"{type(e).__name__}: {e}"
    _grok_n += 1
    _grok_usd += usd
    if v is None:
        if api_alerts.is_billing_error(err):
            _grok_pause_until = time.monotonic() + GROK_PAUSE_S
            api_alerts.send("xai_billing", f"exit_manager: xAI refused (out of credit?) — no X looks for "
                                           f"{GROK_PAUSE_S / 3600:g} h, then one retry. {str(err)[:160]}")
        print(f"[exit] {row.get('symbol')} X look failed: {str(err)[:160]}", flush=True)
        return None, usd
    keep = ("posts_found", "posts_30m", "posts_30_120", "growing", "accounts", "accounts_1k", "bot_pct",
            "source_viral", "summary", "verdict")
    return {k: v.get(k) for k in keep if k in v}, usd


def path_since(call_id: int, bank_time: datetime) -> list:
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute("""SELECT extract(epoch FROM observed_at - %s),
                              CASE WHEN coalesce(no_route, false) THEN 0 ELSE real_mult END
                       FROM qsim_quote_observations
                       WHERE call_id = %s AND observed_at > %s AND NOT coalesce(rate_limited, false)
                         AND (real_mult IS NOT NULL OR coalesce(no_route, false))
                       ORDER BY observed_at""", (bank_time, call_id, bank_time))
        pts = [(float(s), float(m)) for s, m in cur.fetchall()]
    step = max(1, len(pts) // 15)
    return [{"min": round(s / 60, 1), "mult": round(m, 2)} for s, m in pts[::step]][-15:]


# ── the loop ────────────────────────────────────────────────────────────────────

def _row(cur, call_id: int) -> dict:
    cur.execute("SELECT * FROM exit_shadow WHERE call_id = %s", (call_id,))
    cols = [d[0] for d in cur.description]
    r = cur.fetchone()
    return dict(zip(cols, r)) if r else {}


def pick_up() -> int:
    """Start a virtual position for each new live-lane bank."""
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT qp.call_id, qp.channel_handle, t.symbol, t.mint_address, qp.exit_time,
                   qp.sol_out / nullif(qp.sol_in, 0), qp.sol_in, c.created_at, n.uri
            FROM qsim_positions qp
            JOIN calls c ON c.id = qp.call_id
            JOIN tokens t ON t.id = qp.token_id
            LEFT JOIN new_tokens n ON n.mint = t.mint_address
            WHERE qp.status = 'closed' AND qp.exit_reason LIKE '%%bank%%' AND qp.sol_in > 0
              AND qp.channel_handle = ANY(%s) AND coalesce(c.skip_reason, 'none') = ANY(%s)
              AND qp.exit_time >= now() - make_interval(mins => %s)
              AND EXISTS (SELECT 1 FROM trader_brain_entry e WHERE e.call_id = qp.call_id
                          AND e.prompt_v = %s AND e.vs_typical >= %s)
              AND NOT EXISTS (SELECT 1 FROM exit_shadow x WHERE x.call_id = qp.call_id)""",
                    (CHANNELS, CATEGORIES, START_WINDOW_MIN, BRAIN_V, MIN_SCORE))
        rows = cur.fetchall()
    for cid, ch, sym, mint, bank_t, bank_x, sol_in, called_at, uri in rows:
        try:
            ctx = coin_at_call(cid)
        except Exception:
            db.safe_rollback()
            ctx = {}
        ctx["called_at"] = called_at.isoformat() if called_at else None
        ctx["uri"] = uri
        bank_x = float(bank_x or 2.0)
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO exit_shadow (call_id, channel, symbol, mint, bank_time, bank_x, sol_in, stop,
                           high, high_at, peak_since_ask, last_mult, last_obs_at, coin_ctx)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING""",
                        (cid, ch, sym, mint, bank_t, bank_x, sol_in, OPEN_STOP, bank_x, bank_t, bank_x, bank_x,
                         bank_t, json.dumps(ctx, default=str)))
        conn.commit()
        print(f"[exit] {sym} ({ch}) call_id={cid} banked at {bank_x:.2f}x — virtual position opened, "
              f"stop {OPEN_STOP}x until the first answer", flush=True)
    return len(rows)


def _close(cur, row: dict, x: float, reason: str, at) -> None:
    cur.execute("""UPDATE exit_shadow SET status = 'closed', exit_x = %s, exit_reason = %s, exit_at = %s,
                   pending_sell = false WHERE call_id = %s AND status = 'open'""", (x, reason, at, row["call_id"]))
    d = (x - float(row["bank_x"])) * float(row["sol_in"])
    print(f"[exit] {row['symbol']} ({row['channel']}) SOLD (shadow) at {x:.2f}x — {reason}; high "
          f"{float(row['high']):.2f}x; vs the 2x bank {float(row['bank_x']):.2f}x: {d:+.4f} SOL", flush=True)


def walk_quotes(row: dict) -> dict:
    """Apply every new quote in order: pending sell, stop, take-profit, high, max hold. Returns the row."""
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""SELECT observed_at, CASE WHEN coalesce(no_route, false) THEN 0 ELSE real_mult END
                       FROM qsim_quote_observations
                       WHERE call_id = %s AND observed_at > %s AND NOT coalesce(rate_limited, false)
                         AND (real_mult IS NOT NULL OR coalesce(no_route, false))
                       ORDER BY observed_at""", (row["call_id"], row["last_obs_at"]))
        quotes = [(at, float(m)) for at, m in cur.fetchall()]
        high, high_at = float(row["high"]), row["high_at"]
        peak = float(row["peak_since_ask"] or high)
        stop = float(row["stop"] or OPEN_STOP)
        tp = float(row["take_profit"]) if row["take_profit"] is not None else None
        last_at, last_m = row["last_obs_at"], float(row["last_mult"] or 0)
        for at, m in quotes:
            last_at, last_m = at, m
            if row["pending_sell"] and (row["last_ask_at"] is None or at > row["last_ask_at"]):
                row.update(high=high)
                _close(cur, row, m, "brain_sell" if m > 0 else "rug", at)
                break
            if m <= stop:
                row.update(high=high)
                _close(cur, row, m, "stop" if m > 0 else "rug", at)
                break
            if tp is not None and m >= tp:
                row.update(high=max(high, m))
                _close(cur, row, m, "take_profit", at)
                break
            if m > high:
                high, high_at = m, at
            peak = max(peak, m)
        else:
            if (datetime.now(timezone.utc) - row["bank_time"]).total_seconds() > MAX_HOLD_H * 3600:
                row.update(high=high)
                _close(cur, row, last_m, "max_hold", last_at)
        cur.execute("""UPDATE exit_shadow SET high = %s, high_at = %s, peak_since_ask = %s, last_mult = %s,
                       last_obs_at = %s WHERE call_id = %s""",
                    (high, high_at, peak, last_m, last_at, row["call_id"]))
        conn.commit()
        return _row(cur, row["call_id"])


def trigger_for(row: dict) -> str | None:
    now = datetime.now(timezone.utc)
    if row["last_obs_at"] and (now - row["last_obs_at"]).total_seconds() > STALE_MIN * 60 and row["decisions"]:
        return None
    if not row["decisions"]:
        return "at_2x"
    high, m = float(row["high"]), float(row["last_mult"] or 0)
    nxt = [x for x in MILESTONES if x > float(row["milestone"]) and high >= x]
    if nxt:
        return f"reached_{max(nxt):g}x"
    if m <= float(row["peak_since_ask"] or high) * (1 - DROP_PCT / 100):
        return f"dropped_{DROP_PCT:g}pct_from_high"
    if row["last_ask_at"] is None or (now - row["last_ask_at"]).total_seconds() >= CHECK_MIN * 60:
        return "check"
    return None


def _clamp(v, lo, hi):
    try:
        v = float(v)
    except (TypeError, ValueError):
        return None
    return max(lo, min(hi, v))


def decide(row: dict, trigger: str) -> None:
    global _n_today, _usd_today, _pause_until
    conn = db.get_conn()
    m, high = float(row["last_mult"] or 0), float(row["high"])
    milestone = max([x for x in MILESTONES if high >= x] + [float(row["milestone"])])
    can_ask = (tb.API_KEY and time.monotonic() >= _pause_until and _n_today < MAX_CALLS_PER_DAY
               and _usd_today < MAX_USD_PER_DAY)
    if not can_ask:
        stop = max(float(row["stop"] or OPEN_STOP), round(high * FALLBACK_TRAIL, 3), MIN_STOP)
        stop = min(stop, m * 0.97) if m > MIN_STOP else stop
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO exit_shadow_decisions (call_id, mult, trigger, action, stop, model)
                           VALUES (%s,%s,%s,'fallback',%s,NULL)""", (row["call_id"], m, trigger, stop))
            cur.execute("""UPDATE exit_shadow SET stop = %s, milestone = %s, last_ask_at = now(), peak_since_ask = %s,
                           decisions = decisions + 1 WHERE call_id = %s""", (stop, milestone, m, row["call_id"]))
        conn.commit()
        return
    t0 = time.monotonic()
    grok_usd, x = 0.0, None
    if milestone >= 3 and not row["grok_done"]:
        x, grok_usd = x_look(row)
    hold = holders_now(row["mint"])
    with conn.cursor() as cur:
        cur.execute("""SELECT trigger, round(mult, 2), action, stop, take_profit, verdict->>'reason'
                       FROM exit_shadow_decisions WHERE call_id = %s ORDER BY at DESC LIMIT 3""", (row["call_id"],))
        earlier = [{"when": t, "at": float(mm or 0), "action": a, "stop": float(s) if s is not None else None,
                    "take_profit": float(p) if p is not None else None, "reason": r}
                   for t, mm, a, s, p, r in cur.fetchall()][::-1]
    mins = (datetime.now(timezone.utc) - row["bank_time"]).total_seconds() / 60
    ctx = {"why_asked_now": trigger,
           "position": {"sold_2x_at": float(row["bank_x"]), "now": m, "high_since_2x": high,
                        "minutes_since_2x": round(mins, 1),
                        "minutes_since_high": round((datetime.now(timezone.utc) - row["high_at"]).total_seconds() / 60, 1)
                        if row["high_at"] else None,
                        "current_stop": float(row["stop"]) if row["stop"] is not None else None,
                        "current_take_profit": float(row["take_profit"]) if row["take_profit"] is not None else None},
           "path_since_2x": path_since(row["call_id"], row["bank_time"]),
           "volume_and_buyers_now": dex_flow(row["mint"]),
           "top_holders_now": hold, "top_holders_previous_check": row.get("last_holders"),
           "coin_at_call": row.get("coin_ctx") or {},
           "your_earlier_decisions": earlier}
    if x:
        ctx["x_attention_now"] = x
    user = "Position data:\n" + json.dumps(ctx, default=str)
    try:
        v, tin, tout, err = tb._ask(MODEL, EXIT_SYSTEM, user, 500)
    except Exception as e:
        v, tin, tout, err = None, 0, 0, f"{type(e).__name__}: {e}"
    ms = int((time.monotonic() - t0) * 1000)
    _n_today += 1
    _usd_today += tin / 1e6 * PRICE_IN + tout / 1e6 * PRICE_OUT
    if v is None or v.get("action") not in ("hold", "sell"):
        err = err or f"bad answer: {str(v)[:120]}"
        if api_alerts.is_billing_error(err):
            _pause_until = time.monotonic() + BILLING_PAUSE_S
            api_alerts.send("anthropic_billing", f"exit_manager: Anthropic refused — shadow exits trail at "
                                                 f"{FALLBACK_TRAIL:.0%} of the high until fixed. {err[:160]}")
        _streak.fail(err)
        print(f"[exit] {row['symbol']} decision failed ({trigger}): {err[:160]} — keeping stop "
              f"{float(row['stop']):.2f}x", flush=True)
        with conn.cursor() as cur:
            cur.execute("""INSERT INTO exit_shadow_decisions (call_id, mult, trigger, action, stop, context, model,
                           in_tokens, out_tokens, grok_usd, ms) VALUES (%s,%s,%s,'failed',%s,%s,%s,%s,%s,%s,%s)""",
                        (row["call_id"], m, trigger, row["stop"], json.dumps(ctx, default=str), MODEL, tin, tout,
                         grok_usd, ms))
            cur.execute("UPDATE exit_shadow SET last_ask_at = now(), grok_done = grok_done OR %s WHERE call_id = %s",
                        (x is not None, row["call_id"]))
        conn.commit()
        return
    _streak.ok()
    action = v["action"]
    stop = _clamp(v.get("stop"), MIN_STOP, max(MIN_STOP, m * 0.97)) or float(row["stop"] or OPEN_STOP)
    tp = _clamp(v.get("take_profit"), m * 1.01, 1e6) if v.get("take_profit") not in (None, "", "null") else None
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO exit_shadow_decisions (call_id, mult, trigger, action, stop, take_profit, context,
                       verdict, model, in_tokens, out_tokens, grok_usd, ms)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (row["call_id"], m, trigger, action, stop, tp, json.dumps(ctx, default=str), json.dumps(v), MODEL,
                     tin, tout, grok_usd, ms))
        cur.execute("""UPDATE exit_shadow SET stop = %s, take_profit = %s, pending_sell = %s, milestone = %s,
                       last_ask_at = now(), peak_since_ask = %s, decisions = decisions + 1,
                       grok_done = grok_done OR %s, last_holders = coalesce(%s, last_holders)
                       WHERE call_id = %s""",
                    (stop, tp, action == "sell", milestone, m, x is not None,
                     json.dumps(hold) if hold else None, row["call_id"]))
    conn.commit()
    print(f"[exit] {row['symbol']} ({row['channel']}) {trigger} at {m:.2f}x (high {high:.2f}x): "
          f"{action.upper()}" + (f" — stop {stop:.2f}x" + (f", take-profit {tp:.2f}x" if tp else "")
                                 if action == "hold" else "")
          + f" — {str(v.get('reason'))[:150]} [{ms} ms{', X look' if x else ''}]", flush=True)


def tick() -> None:
    global _day, _n_today, _usd_today, _grok_n, _grok_usd, _rpc_today
    today = datetime.now(timezone.utc).date()
    if today != _day:
        _day, _n_today, _usd_today, _grok_n, _grok_usd, _rpc_today = today, 0, 0.0, 0, 0.0, 0
    pick_up()
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute("SELECT call_id FROM exit_shadow WHERE status = 'open' ORDER BY bank_time")
        ids = [r[0] for r in cur.fetchall()]
    for cid in ids:
        with db.get_conn().cursor() as cur:
            row = _row(cur, cid)
        row = walk_quotes(row)
        if row.get("status") != "open":
            continue
        trig = trigger_for(row)
        if trig:
            decide(row, trig)


def run() -> None:
    ensure_tables()
    print(f"[exit] shadow exit manager — live-lane coins: {','.join(CHANNELS)} {CATEGORIES}, v{BRAIN_V} score "
          f"{MIN_SCORE}+; brain {MODEL}. Asks at 2x, milestones {MILESTONES[:6]}..., a {DROP_PCT:g}% drop, every "
          f"{CHECK_MIN:g} min. Ceilings: {MAX_CALLS_PER_DAY} calls / ${MAX_USD_PER_DAY:.2f} a day (Anthropic); "
          f"X looks {'on' if GROK_ON else 'off'} — {GROK_MAX_PER_DAY}/day, ${GROK_MAX_USD_PER_DAY:.2f}/day; "
          f"{HOLDER_MAX_RPC_PER_DAY} holder RPC/day. Nothing here trades.", flush=True)
    while True:
        try:
            tick()
        except Exception as e:
            db.safe_rollback()
            print(f"[exit] tick failed: {type(e).__name__}: {e}", flush=True)
        time.sleep(TICK_S)


# ── report ──────────────────────────────────────────────────────────────────────

def report(since: str, until: str | None = None) -> None:
    ensure_tables()
    win, params = tb._window("bank_time", since, until)
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute(f"""SELECT call_id, channel, symbol, bank_x, exit_x, high, exit_reason, sol_in, decisions, status
                        FROM exit_shadow WHERE {win} ORDER BY bank_time""", params)
        rows = cur.fetchall()
        cur.execute(f"""SELECT coalesce(sum(d.in_tokens), 0), coalesce(sum(d.out_tokens), 0), coalesce(sum(d.grok_usd), 0),
                               count(*) FILTER (WHERE d.action IN ('hold', 'sell'))
                        FROM exit_shadow_decisions d JOIN exit_shadow x ON x.call_id = d.call_id WHERE {win.replace('bank_time', 'x.bank_time')}""",
                    params)
        tin, tout, gusd, n_dec = cur.fetchone()
    print(f"\n{'=' * 20} SHADOW EXIT MANAGER vs the 2x bank ({tb._span(since, until)}) {'=' * 20}")
    closed = [r for r in rows if r[9] == "closed"]
    print(f"  {len(closed)} closed, {len(rows) - len(closed)} still open; {n_dec} brain decisions "
          f"(~${float(tin) / 1e6 * PRICE_IN + float(tout) / 1e6 * PRICE_OUT:.2f} + X ${float(gusd):.2f})")
    for ch in sorted({r[1] for r in closed}) + ["ALL"]:
        g = [r for r in closed if ch == "ALL" or r[1] == ch]
        if not g:
            continue
        deltas = sorted(((float(r[4]) - float(r[3])) * float(r[7]), r[2]) for r in g)
        d = sum(x for x, _ in deltas)
        dep = sum(float(r[7]) for r in g) or 1
        why = {}
        for r in g:
            why[r[6]] = why.get(r[6], 0) + 1
        print(f"\n  {ch}: {len(g)} coins   vs the 2x bank {d:+.4f} SOL ({d / dep * 100:+.1f}%/coin)   without the "
              f"best coin ({deltas[-1][1]}) {d - deltas[-1][0]:+.4f}")
        print(f"    avg sold at {sum(float(r[4]) for r in g) / len(g):.2f}x vs bank {sum(float(r[3]) for r in g) / len(g):.2f}x;"
              f" held to 4x+: {sum(float(r[4]) >= 4 for r in g)}; sold under the bank: "
              f"{sum(float(r[4]) < float(r[3]) for r in g)};  exits: "
              + ", ".join(f"{k} {v}" for k, v in sorted(why.items(), key=lambda kv: -kv[1])))
    if closed:
        print(f"\n  {'symbol':<13}{'channel':<18}{'bank':>6}{'high':>8}{'sold':>7}{'exit':>13}{'asks':>6}{'vs bank SOL':>13}")
        for r in sorted(closed, key=lambda r: -(float(r[4]) - float(r[3])) * float(r[7])):
            print(f"  {str(r[2])[:12]:<13}{str(r[1])[:17]:<18}{float(r[3]):>6.2f}{float(r[5]):>8.2f}{float(r[4]):>7.2f}"
                  f"{str(r[6]):>13}{r[8]:>6}{(float(r[4]) - float(r[3])) * float(r[7]):>+13.4f}")
    print("\n  Earns live exits only by beating the 2x bank on 30+ coins and again on the next 30, and not only")
    print("  through one coin ('without the best coin' must stay positive).")


def decisions(call_id: int) -> None:
    """Every decision on one coin, with its reason."""
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute("""SELECT at, trigger, round(mult, 2), action, stop, take_profit, verdict->>'reason'
                       FROM exit_shadow_decisions WHERE call_id = %s ORDER BY at""", (call_id,))
        for at, t, m, a, s, p, r in cur.fetchall():
            print(f"  {at:%H:%M:%S} {t:<22} {float(m or 0):6.2f}x  {a:<8} stop {float(s or 0):.2f}"
                  f"{f'  tp {float(p):.2f}' if p is not None else ''}  — {r or ''}")


if __name__ == "__main__":
    if "--report" in sys.argv:
        import signal
        signal.signal(signal.SIGPIPE, signal.SIG_DFL)
        i = sys.argv.index("--since") if "--since" in sys.argv else -1
        k = sys.argv.index("--until") if "--until" in sys.argv else -1
        report(sys.argv[i + 1] if i > 0 else "2026-11-12", sys.argv[k + 1] if k > 0 else None)
    elif "--coin" in sys.argv:
        decisions(int(sys.argv[sys.argv.index("--coin") + 1]))
    else:
        run()
