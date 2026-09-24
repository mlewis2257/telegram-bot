"""
position_alerts.py — tell me when a LIVE position I actually hold is climbing.

WHY THIS EXISTS (GTF, 2026-09-23)
---------------------------------
GTF was entered at 17:01, banked 70% at 3.01x, and the runner then climbed
steadily for TWO HOURS — 3x → 7x → 16x → 33x → 63.5x — before rugging to 0.024x
in a single 91-second step with no intermediate quote.

Nothing was wrong with the machinery. The trail did exactly what a trail does:
it waits for a retracement. GTF never retraced, so the trail never fired, and
the 30% runner rode to zero.

Two hours is a long time to be unaware that you are sitting on a 30x. The bot
could not act on it, but a human with a phone could have. That is the entire
purpose of this module: it does not change a single exit decision, it just says
out loud what the position is doing while it is doing it.

WHAT IT IS NOT
--------------
This is NOT a strategy change and it must never become one. It fires no sells,
gates no entries, and touches no exit state. If it throws, the caller carries on
exiting exactly as it would have. It is a window, not a lever.

It is also deliberately separate from monitor.py's milestone alerts, which fire
across the whole watchlist on the FEED basis. Those answer "did a coin we track
run". This answers "is money I have on the table right now moving, and where
will the bot get out if I do nothing" — which needs the position, the quote
basis, and the exit config together.

WHAT IT SENDS
-------------
On first crossing of each level in LIVE_POSITION_ALERT_LEVELS:

    🚀 $SYM — 10x on an open LIVE position
    Now 10.4x · peak 10.4x · quote basis
    0.05 SOL in → ~0.52 SOL out if sold now
    Bot exits at ~8.8x (trail_stop)  ·  giving back ~15%
    ⏱ last quote 41s ago
    ⛓ <mint>
    🔗 Axiom  🔗 DexScreener

The "bot exits at" line is the one that matters. It is the difference between
"nice, it's at 10x" and "the bot will not take this until it drops to 8.8x, and
if it rugs from here I get nothing" — which is the decision you are actually
making when you choose to sit on your hands.

STALENESS
---------
GTF's runner phase was sampled every ~100s, with individual gaps of 196, 223,
228, 311, 381, 416 and 470 seconds. A position quoted 7 minutes ago is one you
are flying blind on, so the alert says so. That line is a report on the bot's
own observation quality, and it is the honest thing to show someone who is being
asked to make a manual call.

ENVIRONMENT
-----------
LIVE_POSITION_ALERTS            false — master switch, off by default
LIVE_POSITION_ALERT_LEVELS      "2,3,5,10,20,50,100"
LIVE_POSITION_ALERT_STALE_SECS  180 — flag quotes older than this
LIVE_POSITION_ALERT_MIN_SOL     0 — skip positions smaller than this

Public API
----------
await maybe_alert(call_id, position, mult, peak_mult, basis, cfg) -> None

Never raises. Returns None always. Callers do not check it.
"""

from __future__ import annotations

import asyncio
import html
import json
import os
import time
from datetime import datetime, timezone

import alert_bot

# ── Config ────────────────────────────────────────────────────────────────────

ENABLED = os.getenv("LIVE_POSITION_ALERTS", "false").lower() == "true"


def _levels() -> tuple[float, ...]:
    raw = os.getenv("LIVE_POSITION_ALERT_LEVELS", "2,3,5,10,20,50,100")
    out: list[float] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            v = float(part)
        except ValueError:
            continue
        if v > 1.0:
            out.append(v)
    return tuple(sorted(set(out)))


LEVELS     = _levels()
STALE_SECS = float(os.getenv("LIVE_POSITION_ALERT_STALE_SECS", "180"))
MIN_SOL    = float(os.getenv("LIVE_POSITION_ALERT_MIN_SOL", "0"))

_STATE_DIR  = os.path.join(os.path.dirname(__file__), ".last_run")
_STATE_FILE = os.path.join(_STATE_DIR, "position_alerts.json")

# call_id -> set of level keys already sent
_sent: dict[int, set[str]] = {}
# call_id -> monotonic ts of the last quote we were handed, for the staleness line
_last_seen: dict[int, float] = {}
_loaded = False


def _load() -> None:
    """Survive a restart without re-alerting a position that is already at 50x."""
    global _loaded
    if _loaded:
        return
    _loaded = True
    try:
        with open(_STATE_FILE) as f:
            data = json.load(f)
        for str_id, keys in data.items():
            try:
                _sent[int(str_id)] = set(keys)
            except (TypeError, ValueError):
                continue
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[pos_alert] could not load state: {e}")


def _save() -> None:
    try:
        os.makedirs(_STATE_DIR, exist_ok=True)
        with open(_STATE_FILE, "w") as f:
            json.dump({str(k): sorted(v) for k, v in _sent.items()}, f, indent=2)
    except Exception as e:
        print(f"[pos_alert] could not save state: {e}")


def clear(call_id: int) -> None:
    """Drop state for a closed position so the file does not grow without bound."""
    if _sent.pop(call_id, None) is not None:
        _save()
    _last_seen.pop(call_id, None)


# ── Where would the bot get out? ──────────────────────────────────────────────

def projected_exit(cfg, peak_mult: float, channel_handle: str,
                   is_vip_gamble: bool) -> tuple[float | None, str]:
    """
    The multiple at which the bot would close this position if the price fell
    from here, and which rule would do it.

    Pure arithmetic over the SAME ExitConfig the live exit check uses — no DB, no
    quotes, no state. It reads the config rather than re-stating its numbers so it
    cannot drift out of sync with the thing it is describing. Returns (None, "")
    when nothing is armed yet, i.e. the hard stop is the only thing below you.

    Whichever level is HIGHEST fires first on the way down, so that is the one
    reported.
    """
    candidates: list[tuple[float, str]] = []

    # Trailing stop: first tier whose threshold the peak has cleared.
    if peak_mult >= (cfg.trail_peak_min or 0):
        for threshold, dd in cfg.trail_tiers:
            if peak_mult >= threshold:
                candidates.append((peak_mult * (1.0 - dd), "trail_stop"))
                break

    # Profit floor: channel-gated, so an inapplicable channel must not report one.
    handle = (channel_handle or "").lstrip("@")
    if cfg.profit_floor_tiers and handle in cfg.profit_floor_channels:
        for trigger, floor in cfg.profit_floor_tiers:
            if peak_mult >= trigger:
                candidates.append((floor, "profit_floor"))
                break

    if not candidates:
        return None, ""
    level, reason = max(candidates, key=lambda c: c[0])
    return level, reason


def _next_take_profit(cfg, mult: float, is_vip_gamble: bool) -> float | None:
    """The fixed TP that would close this on the way UP, if any is still ahead."""
    if is_vip_gamble and cfg.skip_fixed_tp_for_vip_gamble:
        return None
    ahead = [lv for lv in cfg.take_profit_levels if lv > mult]
    return min(ahead) if ahead else None


# ── Message ───────────────────────────────────────────────────────────────────

def _fmt_age(secs: float) -> str:
    if secs < 90:
        return f"{secs:.0f}s"
    return f"{secs / 60:.1f}m"


def build_message(
    symbol: str,
    mint: str,
    level: float,
    mult: float,
    peak_mult: float,
    sol_in: float,
    basis: str,
    exit_level: float | None,
    exit_reason: str,
    next_tp: float | None,
    quote_age: float | None,
) -> str:
    sym  = html.escape(str(symbol or "?"))
    mint = html.escape(str(mint or ""))
    worth = sol_in * mult if sol_in > 0 else 0.0

    lines = [
        f"🚀 <b>${sym} — {level:g}x on an open LIVE position</b>",
        f"Now {mult:.2f}x · peak {peak_mult:.2f}x · {html.escape(basis or '?')} basis",
    ]
    if sol_in > 0:
        lines.append(f"{sol_in:.3f} SOL in → <b>~{worth:.3f} SOL</b> if sold now")

    if exit_level is not None and exit_level > 0:
        giveback = max(0.0, (mult - exit_level) / mult) * 100.0
        lines.append(
            f"Bot exits at ~{exit_level:.2f}x ({html.escape(exit_reason)})"
            f" · giving back ~{giveback:.0f}%"
        )
    else:
        # Nothing armed above the hard stop means the only thing under this
        # position is the stop at entry. Say it plainly.
        lines.append("⚠️ No trail armed yet — only the hard stop is below you")

    if next_tp is not None:
        lines.append(f"Fixed take-profit ahead at {next_tp:g}x")

    if quote_age is not None and quote_age >= STALE_SECS:
        lines.append(f"⏱ <b>last quote {_fmt_age(quote_age)} ago</b> — sampling is thin here")

    lines += [
        f"⛓ <code>{mint}</code>",
        "",
        f'🔗 <a href="https://axiom.trade/t/{mint}">Axiom</a>'
        f'  🔗 <a href="https://dexscreener.com/solana/{mint}">DexScreener</a>',
    ]
    return "\n".join(lines)


# ── Public API ────────────────────────────────────────────────────────────────

async def maybe_alert(
    call_id: int,
    position: dict,
    mult: float,
    peak_mult: float,
    basis: str,
    cfg,
) -> None:
    """
    Fire a milestone alert for an open LIVE position, at most once per level.

    `mult` should be the RAW executable multiple when one is available — the same
    number the exit check keys off. Alerting on the feed basis would report a
    multiple the wallet cannot realise, which is the original sin this whole
    project spent six months unwinding.

    Swallows everything. An alert failure must never disturb an exit.
    """
    if not ENABLED or not LEVELS:
        return
    try:
        _load()
        if mult is None or mult <= 0:
            return

        sol_in = float(position.get("sol_in") or 0)
        if MIN_SOL > 0 and sol_in < MIN_SOL:
            return

        now = time.monotonic()
        prev = _last_seen.get(call_id)
        quote_age = (now - prev) if prev is not None else None
        _last_seen[call_id] = now

        peak_mult = max(float(peak_mult or 0), mult)
        sent = _sent.setdefault(call_id, set())

        # Only the highest level crossed this tick, so a gap-up from 1x to 60x
        # sends one message rather than seven. The lower levels are still marked
        # so they never fire retroactively on the way back down.
        crossed = [lv for lv in LEVELS if mult >= lv and f"{lv:g}x" not in sent]
        if not crossed:
            return
        for lv in crossed:
            sent.add(f"{lv:g}x")
        top = max(crossed)

        is_vip_gamble = position.get("vip_tier") in ("gamble_risk", "gamble")
        channel = position.get("channel_handle") or ""
        exit_level, exit_reason = projected_exit(cfg, peak_mult, channel, is_vip_gamble)
        next_tp = _next_take_profit(cfg, mult, is_vip_gamble)

        text = build_message(
            symbol=position.get("symbol", "?"),
            mint=position.get("mint_address", ""),
            level=top,
            mult=mult,
            peak_mult=peak_mult,
            sol_in=sol_in,
            basis=basis,
            exit_level=exit_level,
            exit_reason=exit_reason,
            next_tp=next_tp,
            quote_age=quote_age,
        )

        await alert_bot._get_bot().send_message(
            chat_id=alert_bot._chat_id(),
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
        )
        print(
            f"[pos_alert] {position.get('symbol','?')} call_id={call_id}"
            f" {top:g}x  mult={mult:.2f}x  exit~{exit_level if exit_level else 'none'}"
        )
        _save()
        await asyncio.sleep(1.0)
    except Exception as e:
        print(f"[pos_alert] alert failed for call_id={call_id}: {e}")
