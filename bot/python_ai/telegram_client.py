"""
Telegram listener — multi-channel, type-aware.

Reads active channel configs from the channels table at startup.
For each channel, runs a catchup scan from the last-seen message ID
(stored in .last_run/<handle>.txt), then registers a persistent
NewMessage event handler for all active channels simultaneously.

Routes each message to the correct parser based on channel_type:
  lagging  → parsers/type_a.py  (entry + result in one message)
  realtime → parsers/type_b.py  (entry only; updates handled separately)

Adding a new channel requires only inserting a row in the channels table.
No code changes needed.
"""

import asyncio
import atexit
import os
import signal
import time
from pathlib import Path

from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon import events
from telethon.tl.types import PeerChannel

import db
import data_fetcher
import tags
import scorer
import alert_bot
import paper_trader
import paper_trader_b
import shadow_trader
import lane_policy

# Lane testbed master switch (mirrors paper_trader.LANE_TESTBED_ENABLED). When on, A/B
# trade lane_policy lanes via open_lane_position and the legacy paper dispatch is
# suppressed. Default off — no behaviour change until set.
LANE_TESTBED_ENABLED = os.getenv("LANE_TESTBED_ENABLED", "false").lower() == "true"
import monitor as _monitor
from parsers import type_a, type_b, type_a_vip


def _is_valid_peak_update(
    new_mult,
    current_peak,
    mcap_at_call,
    new_mcap_result,
    label: str = "",
) -> bool:
    """
    Guard against corrupted DexScreener data polluting peak_multiplier / mcap_at_result.

    Rejects:
      1. new_mult > 2000             — absolute hard cap
      2. new_mult > current_peak*10  — single-update jump (e.g. 5x → 11,866x)
      3. entry mcap < $100k AND result mcap > $50M — small-cap data error
    """
    if new_mult is None:
        return True
    if new_mult > 2000:
        print(f"  [peak_guard] {label} rejected: {new_mult:.1f}x exceeds hard cap (2000x)")
        return False
    if current_peak and current_peak > 0 and new_mult > current_peak * 10:
        print(
            f"  [peak_guard] {label} rejected: {new_mult:.1f}x is >10x jump"
            f" from stored peak {current_peak:.1f}x"
        )
        return False
    if (
        mcap_at_call is not None
        and new_mcap_result is not None
        and mcap_at_call < 100_000
        and new_mcap_result > 50_000_000
    ):
        print(
            f"  [peak_guard] {label} rejected: small-cap entry"
            f" (${mcap_at_call:,.0f}) with result ${new_mcap_result / 1_000_000:.1f}M"
            f" — likely corrupted DexScreener data"
        )
        return False
    return True

load_dotenv()

api_id   = int(os.environ["API_ID"])
api_hash = os.environ["API_HASH"]

VIP_CHANNEL = int(os.getenv("SOLHOUSE_VIP_CHANNEL", 0)) or None

# ── Single-process guard ──────────────────────────────────────────────────────

_PID_LOCK_PATH = Path(__file__).parent / ".telegram_client.pid"
_PID_LOCK_OWNER: int | None = None


def _process_is_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False
    return True


def _release_pid_lock(pid: int | None = None) -> None:
    global _PID_LOCK_OWNER

    owner = pid or _PID_LOCK_OWNER
    try:
        existing = _PID_LOCK_PATH.read_text().strip()
        if owner is not None and existing != str(owner):
            return
        _PID_LOCK_PATH.unlink()
    except FileNotFoundError:
        pass
    finally:
        if owner == _PID_LOCK_OWNER:
            _PID_LOCK_OWNER = None


def _handle_shutdown_signal(signum, _frame) -> None:
    raise KeyboardInterrupt


def _acquire_pid_lock() -> None:
    global _PID_LOCK_OWNER

    current_pid = os.getpid()
    while True:
        try:
            fd = os.open(
                _PID_LOCK_PATH,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o644,
            )
        except FileExistsError:
            try:
                existing_pid = int(_PID_LOCK_PATH.read_text().strip())
            except (FileNotFoundError, ValueError):
                existing_pid = 0

            if _process_is_running(existing_pid):
                print(
                    f"[error] Already running as PID {existing_pid}. "
                    f"Kill it first or delete {_PID_LOCK_PATH.name}"
                )
                raise SystemExit(1)

            try:
                _PID_LOCK_PATH.unlink()
            except FileNotFoundError:
                pass
            except OSError as e:
                print(f"[error] Could not remove stale PID lock {_PID_LOCK_PATH}: {e}")
                raise SystemExit(1)
            continue

        with os.fdopen(fd, "w") as lock_file:
            lock_file.write(f"{current_pid}\n")

        _PID_LOCK_OWNER = current_pid
        atexit.register(_release_pid_lock, current_pid)
        signal.signal(signal.SIGTERM, _handle_shutdown_signal)
        return


# ── Last-run state directory ───────────────────────────────────────────────────

_LAST_RUN_DIR = Path(__file__).parent / ".last_run"
_LAST_RUN_DIR.mkdir(exist_ok=True)

CATCHUP_FALLBACK_LIMIT = 50   # used on first run, when no state file exists


def _last_run_path(handle: str) -> Path:
    safe = handle.lstrip("@").replace("/", "_")
    return _LAST_RUN_DIR / f"{safe}.txt"


def _read_last_message_id(handle: str) -> int | None:
    path = _last_run_path(handle)
    try:
        return int(path.read_text().strip())
    except (FileNotFoundError, ValueError):
        return None


def _write_last_message_id(handle: str, message_id: int) -> None:
    _last_run_path(handle).write_text(str(message_id))


# ── Keyword pre-filter ────────────────────────────────────────────────────────
# Applied before the heavier parsers to skip obvious non-call messages cheaply.

CALL_KEYWORDS = [
    "solana", "$", "pump", "call", "launch", "raydium",
    "bonk", "ca:", "contract", "mint", "ticker", "achiev",
    "whale",   # catches realtime entry signals and VIP whale alerts
    "gem alert",  # VIP gem alerts
]


def is_candidate(text: str) -> bool:
    lower = text.lower()
    return any(kw in lower for kw in CALL_KEYWORDS)


# ── Per-channel stats ─────────────────────────────────────────────────────────

_STAT_KEYS = [
    "logged", "milestone", "dupe", "whale_alert",
    "dex_paid", "boost", "update", "daily_stats",
    "leaderboard", "noise", "skipped", "error",
]

_stats: dict[str, dict] = {}

# ── VIP gamble confirmation queue ─────────────────────────────────────────────
# VIP gamble_risk/gamble signals are held here until solhousesignal independently
# calls the same mint 5–10 minutes later (confirming the signal is not an instant rug).

_pending_vip_gamble: dict[str, dict] = {}  # mint → {call_id, score_result, extra, timestamp, symbol}
VIP_CONFIRM_MIN_SECS = 300   # must wait at least 5 minutes for independent confirmation
VIP_CONFIRM_MAX_SECS = 600   # expire after 10 minutes with no confirmation


def _cleanup_stale_vip_gamble() -> None:
    """Expire pending VIP gamble entries older than 10 minutes, logging skip_reason='unconfirmed'."""
    now = time.monotonic()
    expired = [
        mint for mint, entry in _pending_vip_gamble.items()
        if now - entry["timestamp"] > VIP_CONFIRM_MAX_SECS
    ]
    for mint in expired:
        entry = _pending_vip_gamble.pop(mint)
        db.set_call_skip_reason(entry["call_id"], "unconfirmed")
        print(f"[vip_confirm] {entry['symbol']} expired — no free-channel confirmation within 10 min")


def _init_stats(handle: str) -> None:
    _stats[handle] = {k: 0 for k in _STAT_KEYS}


def _bump(handle: str, result: str) -> None:
    key = result if result in _STAT_KEYS else "skipped"
    _stats.setdefault(handle, {k: 0 for k in _STAT_KEYS})[key] += 1


def _print_final_stats() -> None:
    print("\n[listener] ── Final stats ──────────────────────────────────────")
    for handle, counts in _stats.items():
        parts = "  ".join(f"{k}={v}" for k, v in counts.items() if v)
        print(f"  {handle}: {parts or '(nothing processed)'}")
    print("[listener] ────────────────────────────────────────────────────")


def _track_task(task: asyncio.Task, label: str) -> None:
    def _done(done_task: asyncio.Task) -> None:
        try:
            exc = done_task.exception()
        except asyncio.CancelledError:
            print(f"[paper_dispatch] cancelled {label}")
            return
        except Exception as e:
            print(f"[paper_dispatch] could not inspect {label}: {e}")
            return
        if exc:
            print(f"[paper_dispatch] task failed {label}: {exc}")

    task.add_done_callback(_done)


async def _dispatch_paper_open(
    score_result: dict,
    extra: dict,
    *,
    is_strategy_b: bool,
    fallback_reason: str | None = None,
) -> None:
    if LANE_TESTBED_ENABLED:
        return  # legacy A/B paper dispatch is replaced by the lane testbed
    trader = paper_trader_b if is_strategy_b else paper_trader
    call_id = score_result.get("call_id") if score_result else None

    await trader.open_position(score_result, extra)

    if not call_id:
        return
    if db.has_paper_position_for_call(call_id, is_strategy_b=is_strategy_b):
        return
    if db.get_call_skip_reason(call_id) is not None:
        return
    if fallback_reason:
        db.set_call_skip_reason(call_id, fallback_reason)
        print(
            f"[paper_dispatch] call_id={call_id} symbol={extra.get('symbol')} "
            f"fell through dispatch without trade/skip — stamped {fallback_reason}"
        )


async def _dispatch_lane_testbed(score_result: dict, extra: dict) -> None:
    """
    Lane testbed dispatch. Fires AFTER routing (so the call's skip_reason — the lane
    label — is final), resolving lane_policy independently for Strategy A (anchors) and
    Strategy B (anchors + day-gated watch pockets) and opening paper positions for each.
    Never raises — a testbed failure must not affect the listener.
    """
    call_id = score_result.get("call_id") if score_result else None
    if not call_id or not extra:
        return
    try:
        channel  = (extra.get("channel_tag") or extra.get("channel_handle") or "").lstrip("@")
        vip_tier = extra.get("vip_tier")
        category = db.get_call_skip_reason(call_id)  # the lane label; None -> "none"
        pa = lane_policy.resolve(channel, vip_tier, category, strategy="A")
        if pa.get("trade"):
            _track_task(
                asyncio.create_task(paper_trader.open_lane_position(score_result, extra, pa)),
                f"laneA call_id={call_id}",
            )
        pb = lane_policy.resolve(channel, vip_tier, category, strategy="B")
        if pb.get("trade"):
            _track_task(
                asyncio.create_task(paper_trader_b.open_lane_position(score_result, extra, pb)),
                f"laneB call_id={call_id}",
            )
    except Exception as e:
        print(f"[lane_testbed] dispatch failed call_id={call_id}: {e}")


# ── Per-channel-type handlers ─────────────────────────────────────────────────

def handle_lagging(message, caller_id: int, channel_id: int) -> tuple:
    """
    Process one message from a lagging channel.
    Returns (status, score_result, extra) where:
      status       — 'logged' | 'milestone' | 'dupe' | 'noise' | ...
      score_result — scorer dict on gem_alert logged path, else None
      extra        — token_data on logged path; milestone_info dict on 5x+
                     milestone path; None everywhere else
    Catchup callers should discard extra: status, *_ = handle_lagging(...)
    Alert sending (await) must happen in the async _handler, not here.
    """
    text  = message.text.strip()
    kind  = type_a.classify(text)

    if kind == 'noise':
        return 'noise', None, None

    if kind == 'whale_alert':
        whale = type_a.parse_whale_alert(text)
        if not whale:
            return 'noise', None, None
        token_id = db.get_token_id_by_symbol(whale["symbol"]) if whale["symbol"] else None
        call_id  = None
        if token_id:
            call_id = db.get_call_id_by_token_name(whale["symbol"])
        whale_id = db.insert_whale_alert(
            symbol=whale["symbol"],
            sol_amount=whale["sol_amount"],
            wallet_size_sol=whale["wallet_size_sol"],
            mcap_at_whale=whale["mcap_at_whale"],
            signal_count=whale["signal_count"],
            raw_message=text,
            token_id=token_id,
            call_id=call_id,
        )
        print(
            f"  [whale] id={whale_id} symbol={whale['symbol']} "
            f"sol={whale['sol_amount']} wallet={whale['wallet_size_sol']} "
            f"signal#{whale['signal_count']} call_id={call_id}"
        )
        return 'whale_alert', None, None

    if kind == 'dexscreener_paid':
        dex = type_a.parse_dexscreener_paid(text)
        if not dex:
            return 'noise', None, None
        lookup = dex["symbol"] or dex["name"]
        token_id = db.get_token_id_by_symbol(lookup) if lookup else None
        if token_id:
            db.update_token_dexscreener_paid(token_id)
            print(f"  [dex_paid] token_id={token_id} symbol={dex['symbol']} name={dex['name']}")
        return 'dexscreener_paid', None, None

    if kind == 'boost_alert':
        boost = type_a.parse_boost_alert(text)
        if not boost:
            return 'noise', None, None
        lookup = boost["symbol"] or boost["name"]
        token_id = db.get_token_id_by_symbol(lookup) if lookup else None
        if token_id:
            db.update_token_boost(token_id, boost["boost_count"])
            print(
                f"  [boost] token_id={token_id} symbol={boost['symbol']} "
                f"boosts={boost['boost_count']}"
            )
        return 'boost_alert', None, None

    if kind == 'milestone':
        milestone = type_a.parse_milestone(text)
        if not milestone:
            return 'noise', None, None

        call_id = db.get_call_id_by_token_and_channel(milestone["token_name"], channel_id)

        if call_id is None:
            # ── Orphan milestone: the initial call predates our scraper ──
            token_id = db.get_token_id_by_symbol(milestone["token_name"])
            if token_id is None:
                token_id = db.upsert_token(
                    mint_address=f"INFERRED:{milestone['token_name'].upper()}",
                    symbol=milestone["token_name"],
                )
            call_id = db.insert_call(
                caller_id=caller_id,
                token_id=token_id,
                source_platform="telegram",
                source_message_id=f"orphan_{message.id}",
                raw_message=text,
                created_at=message.date,
                message_type="inferred_call",
                channel_id=channel_id,
                mcap_at_call=milestone["entry_mcap"],
                narrative_tags=tags.tag_token(milestone["token_name"], "", text),
            )
            if call_id is None:
                return 'milestone', None, None
            db.insert_outcome(call_id)
            _new_mult = milestone["multiplier_computed"] or milestone["multiplier_stated"]
            if _is_valid_peak_update(
                new_mult=_new_mult,
                current_peak=None,
                mcap_at_call=milestone["entry_mcap"],
                new_mcap_result=milestone["current_mcap"],
                label=f"orphan_milestone call_id={call_id}",
            ):
                db.update_outcome_from_lagging(
                    call_id=call_id,
                    mcap_at_result=milestone["current_mcap"],
                    result_reported_at=message.date,
                    stated_multiplier=milestone["multiplier_stated"],
                    peak_multiplier=_new_mult,
                    outcome_label="runner",
                )
            print(
                f"  [orphan_milestone] call_id={call_id} token={milestone['token_name']} "
                f"entry={milestone['entry_mcap']} result={milestone['current_mcap']} "
                f"stated={milestone['multiplier_stated']}x "
                f"computed={milestone['multiplier_computed']}x"
            )
            return 'milestone', None, None

        _new_mult   = milestone["multiplier_computed"] or milestone["multiplier_stated"]
        _peak_info  = db.get_call_peak_info(call_id)
        if _is_valid_peak_update(
            new_mult=_new_mult,
            current_peak=_peak_info["peak_multiplier"] if _peak_info else None,
            mcap_at_call=_peak_info["mcap_at_call"] if _peak_info else None,
            new_mcap_result=milestone["current_mcap"],
            label=f"milestone call_id={call_id}",
        ):
            db.update_outcome_peak(
                call_id=call_id,
                multiplier_computed=_new_mult,
                multiplier_stated=milestone["multiplier_stated"],
                current_mcap=milestone["current_mcap"],
                reported_at=message.date,
            )
        print(
            f"  [milestone] call_id={call_id} token={milestone['token_name']} "
            f"stated={milestone['multiplier_stated']}x "
            f"computed={milestone['multiplier_computed']}x"
        )
        # Only alert on 5x+ milestones to avoid spam
        milestone_info = None
        if milestone["multiplier_stated"] and milestone["multiplier_stated"] >= 5:
            milestone_info = {
                "call_id":           call_id,
                "symbol":            milestone["token_name"],
                "stated_multiplier": milestone["multiplier_stated"],
                "mcap_at_call":      milestone.get("entry_mcap"),
                "current_mcap":      milestone.get("current_mcap"),
                "mint_address":      db.get_mint_by_call_id(call_id),
            }
        return 'milestone', None, milestone_info

    # kind == 'gem_alert'
    parsed = type_a.parse(text)
    if not parsed:
        return 'noise', None, None

    token_id = db.upsert_token(
        mint_address=parsed["mint"],
        symbol=parsed["symbol"],
        name=parsed["name"],
    )

    call_id = db.insert_call(
        caller_id=caller_id,
        token_id=token_id,
        source_platform="telegram",
        source_message_id=message.id,
        raw_message=text,
        created_at=message.date,
        message_type="lagging_call",
        channel_id=channel_id,
        mcap_at_call=parsed["mcap_at_call"],
        narrative_tags=tags.tag_token(parsed["symbol"] or "", parsed["name"] or "", text),
    )

    if call_id is None:
        return "dupe", None, None

    linked = db.backfill_whale_alert_call_ids(token_id=token_id, call_id=call_id)
    if linked:
        print(f"  [whale-backfill] linked {linked} whale alert(s) to call_id={call_id}")

    market = data_fetcher.fetch_metadata_only(parsed["mint"])
    if market:
        db.update_call_market_data(
            call_id=call_id,
            price_usd=market["price_usd"],
            mcap=market["mcap"],
            liquidity_usd=market["liquidity_usd"],
        )
        if market["name"] or market["symbol"]:
            db.upsert_token(
                mint_address=parsed["mint"],
                symbol=market["symbol"],
                name=market["name"],
            )

    db.insert_outcome(call_id)

    if parsed["mcap_at_result"] is not None and _is_valid_peak_update(
        new_mult=parsed["peak_multiplier"],
        current_peak=None,
        mcap_at_call=parsed["mcap_at_call"],
        new_mcap_result=parsed["mcap_at_result"],
        label=f"lagging_gem call_id={call_id}",
    ):
        db.update_outcome_from_lagging(
            call_id=call_id,
            mcap_at_result=parsed["mcap_at_result"],
            result_reported_at=message.date,
            stated_multiplier=parsed["stated_multiplier"],
            peak_multiplier=parsed["peak_multiplier"],
            outcome_label="runner",
        )

    mint_short = parsed["mint"][:8] + "..."
    print(
        f"  [logged] call_id={call_id} "
        f"mint={mint_short} symbol={parsed['symbol']} "
        f"entry={parsed['mcap_at_call']} result={parsed['mcap_at_result']} "
        f"computed={parsed['peak_multiplier']}x stated={parsed['stated_multiplier']}x"
    )
    score_result = None
    try:
        score_result = scorer.score_call(call_id)
        print(f"  [score] {parsed['symbol']} call_id={call_id}  score={score_result['score']}  label={score_result['label']}")
        print(f"  Reasons: {', '.join(score_result['reasons'])}")
    except Exception as e:
        print(f"  [score] error: {e}")
    token_data = {
        "symbol":            parsed.get("symbol"),
        "mint_address":      parsed.get("mint"),
        "mcap_at_call":      parsed.get("mcap_at_call"),
        "security_flag":     None,
        "token_age_minutes": None,
        "channel_tag":       "solhousesignal",
        "channel_handle":    "solhousesignal",
    }
    return "logged", score_result, token_data


def handle_realtime(message, caller_id: int, channel_id: int, channel_handle: str = None) -> tuple:
    """
    Process one message from a realtime channel (@solwhaletrending, @solearlytrending).
    Returns (status, score_result, token_data) where:
      status       — 'logged' | 'dupe' | 'whale_alert' | 'update' |
                     'daily_stats' | 'leaderboard' | 'skipped' | 'noise'
      score_result — scorer dict on logged path, else None
      token_data   — {symbol, mint_address, mcap_at_call, security_flag,
                     token_age_minutes} on logged path, else None
    Catchup callers should discard extra: status, *_ = handle_realtime(...)
    Alert sending (await) must happen in the async _handler, not here.
    """
    text = message.text.strip()
    kind = type_b.classify_b(text)

    if kind == 'noise':
        return 'noise', None, None

    # ── Another Whale — second/third whale buying an already-signalled token ──
    if kind == 'another_whale':
        whale = type_b.parse_another_whale(text)
        if not whale:
            return 'skipped', None, None

        token_id = db.get_token_id_by_symbol(whale["symbol"]) if whale["symbol"] else None
        call_id  = db.get_call_id_by_token_name(whale["symbol"]) if whale["symbol"] else None

        whale_id = db.insert_whale_alert(
            symbol=whale["symbol"],
            sol_amount=whale["sol_spent"],
            wallet_size_sol=whale["wallet_sol"],
            mcap_at_whale=whale["mcap"],
            signal_count=None,
            raw_message=text,
            token_id=token_id,
            call_id=call_id,
        )
        print(
            f"  [another_whale] id={whale_id} symbol={whale['symbol']} "
            f"sol={whale['sol_spent']} wallet={whale['wallet_sol']} "
            f"call_id={call_id}"
        )
        return 'whale_alert', None, None

    # ── Entry signals (whale or trending) — shared handling ──
    if kind in ('entry_signal_whale', 'entry_signal_trending'):
        if kind == 'entry_signal_whale':
            parsed = type_b.parse_entry_signal_whale(text)
        else:
            parsed = type_b.parse_entry_signal_trending(text)

        if not parsed:
            return 'skipped', None, None

        symbol = parsed.get("symbol")
        mint   = parsed.get("mint")

        mint_resolved = mint is not None
        if not mint_resolved:
            mint = f"UNKNOWN:{symbol.upper()}" if symbol else f"UNKNOWN:msg{message.id}"

        token_id = db.upsert_token(
            mint_address=mint,
            symbol=symbol,
            name=parsed.get("name"),
        )

        # Write on-chain metadata unconditionally — before the dupe check so
        # duplicate calls (same message seen on restart) still update the token.
        db.upsert_token_realtime_metadata(
            token_id=token_id,
            mint_resolved=mint_resolved,
            token_age_minutes=parsed.get("token_age_minutes"),
            security_flag=parsed.get("security_flag"),
            is_cto=parsed.get("is_cto"),
            bundle_count=parsed.get("bundle_count"),
            bundle_pct_initial=parsed.get("bundle_pct_initial"),
            bundle_pct_remaining=parsed.get("bundle_pct_remaining"),
            sniper_count=parsed.get("sniper_count"),
            sniper_pct_initial=parsed.get("sniper_pct_initial"),
            sniper_pct_remaining=parsed.get("sniper_pct_remaining"),
            first_20_pct=parsed.get("first_20_pct"),
            dev_sol_held=parsed.get("dev_sol_held"),
            dev_pct_held=parsed.get("dev_pct_held"),
            dev_sold=parsed.get("dev_sold"),
            dev_tokens_made=parsed.get("dev_tokens_made"),
            dev_bonds=parsed.get("dev_bonds"),
            dev_best_mcap=parsed.get("dev_best_mcap"),
            bundled_pct=parsed.get("bundled_pct"),
            bundled_sold_pct=parsed.get("bundled_sold_pct"),
            hodl_count=parsed.get("hodl_count"),
            liq_at_detection=parsed.get("liq_at_detection"),
            vol_1h_at_detection=parsed.get("vol_1h_at_detection"),
            detecting_wallet_sol=parsed.get("detecting_wallet_sol"),
            detecting_sol_spent=parsed.get("detecting_sol_spent"),
            fake_vol_usd=parsed.get("fake_vol_usd"),
            fake_vol_pct=parsed.get("fake_vol_pct"),
        )

        call_id = db.insert_call(
            caller_id=caller_id,
            token_id=token_id,
            source_platform="telegram",
            source_message_id=str(message.id),
            raw_message=text,
            created_at=message.date,
            message_type="initial_call",
            channel_id=channel_id,
            mcap_at_call=parsed.get("mcap_at_call"),
            narrative_tags=tags.tag_token(symbol or "", parsed.get("name") or "", text),
        )

        if call_id is None:
            return 'dupe', None, None

        if mint_resolved:
            market = data_fetcher.fetch_token_data(mint)
            if market:
                db.update_call_market_data(
                    call_id=call_id,
                    price_usd=market["price_usd"],
                    mcap=market["mcap"],
                    liquidity_usd=market["liquidity_usd"],
                )
                if market["name"] or market["symbol"]:
                    db.upsert_token(
                        mint_address=mint,
                        symbol=market["symbol"],
                        name=market["name"],
                    )

        db.insert_outcome(call_id)

        linked = db.backfill_whale_alert_call_ids(token_id=token_id, call_id=call_id)
        if linked:
            print(f"  [whale-backfill] linked {linked} whale alert(s) to call_id={call_id}")

        mint_short = mint[:12] + ("..." if len(mint) > 12 else "")
        status = "resolved" if mint_resolved else "UNKNOWN"
        print(
            f"  [realtime:{kind}] call_id={call_id} symbol={symbol} "
            f"mint={mint_short} ({status}) "
            f"age={parsed.get('token_age_minutes')}m "
            f"security={parsed.get('security_flag')}"
        )
        score_result = None
        try:
            score_result = scorer.score_call(call_id)
            print(f"  [score] {symbol} call_id={call_id}  score={score_result['score']}  label={score_result['label']}")
            print(f"  Reasons: {', '.join(score_result['reasons'])}")
        except Exception as e:
            print(f"  [score] error: {e}")
        token_data = {
            "symbol":            symbol,
            "mint_address":      mint,
            "mcap_at_call":      parsed.get("mcap_at_call"),
            "security_flag":     parsed.get("security_flag"),
            "token_age_minutes": parsed.get("token_age_minutes"),
            "source_message_id": message.id,
            "channel_handle":    channel_handle,
        }
        return 'logged', score_result, token_data

    # ── Price update ──
    if kind == 'price_update':
        update = type_b.parse_price_update(text)
        if not update:
            return 'skipped', None, None
        call_id = db.get_call_id_by_token_name(update["symbol"])
        if not call_id:
            return 'skipped', None, None
        _new_mult  = update.get("multiplier")
        _peak_info = db.get_call_peak_info(call_id)
        if _is_valid_peak_update(
            new_mult=_new_mult,
            current_peak=_peak_info["peak_multiplier"] if _peak_info else None,
            mcap_at_call=_peak_info["mcap_at_call"] if _peak_info else None,
            new_mcap_result=update.get("current_mcap"),
            label=f"price_update call_id={call_id}",
        ):
            db.update_outcome_peak(
                call_id=call_id,
                multiplier_computed=_new_mult,
                multiplier_stated=_new_mult,
                current_mcap=update.get("current_mcap"),
                reported_at=message.date,
            )
        return 'update', None, None

    # ── Daily stats ──
    if kind == 'daily_stats':
        stats = type_b.parse_daily_stats(text)
        if not stats:
            return 'skipped', None, None
        db.insert_channel_daily_stats(
            channel_id=channel_id,
            stat_date=message.date.date(),
            **stats,
        )
        return 'daily_stats', None, None

    # ── Leaderboard ──
    if kind == 'leaderboard':
        entries = type_b.parse_leaderboard(text)
        if not entries:
            return 'skipped', None, None
        db.insert_channel_daily_stats(
            channel_id=channel_id,
            stat_date=message.date.date(),
            top_performers=entries,
        )
        return 'leaderboard', None, None

    return 'skipped', None, None


def handle_vip(message, caller_id: int, channel_id: int | None) -> tuple:
    """
    Process one message from the SolHouse VIP channel.

    Delegates entirely to type_a_vip.parse(), then routes on vip_message_type:
      gem_alert / volume_alert → log call, score, return for alerting
      whale_alert              → log to whale_alerts table, no scoring

    Returns (status, score_result, token_data) following the same
    convention as handle_lagging / handle_realtime.
    """
    text   = message.text.strip()
    parsed = type_a_vip.parse(text)
    if not parsed:
        return 'noise', None, None

    vip_message_type = parsed["vip_message_type"]

    # ── Gem Alert + Volume Alert → logged call ──────────────────────────────
    if vip_message_type in ('gem_alert', 'volume_alert'):
        symbol = parsed.get("symbol")
        mint   = parsed.get("mint_address")

        mint_resolved = mint is not None
        if not mint_resolved:
            mint = f"UNKNOWN:{symbol.upper()}" if symbol else f"UNKNOWN:msg{message.id}"

        token_id = db.upsert_token(
            mint_address=mint,
            symbol=symbol,
            name=parsed.get("token_name"),
        )

        call_id = db.insert_call(
            caller_id=caller_id,
            token_id=token_id,
            source_platform="telegram",
            source_message_id=str(message.id),
            raw_message=text,
            created_at=message.date,
            message_type="initial_call",
            channel_id=channel_id,
            mcap_at_call=parsed.get("mcap_at_call"),
            narrative_tags=tags.tag_token(symbol or "", parsed.get("token_name") or "", text),
            vip_tier=parsed.get("vip_tier"),
            vip_message_type=parsed.get("vip_message_type"),
        )

        if call_id is None:
            return 'dupe', None, None

        if mint_resolved:
            market = data_fetcher.fetch_token_data(mint)
            if market:
                db.update_call_market_data(
                    call_id=call_id,
                    price_usd=market["price_usd"],
                    mcap=market["mcap"],
                    liquidity_usd=market["liquidity_usd"],
                )
                if market["name"] or market["symbol"]:
                    db.upsert_token(
                        mint_address=mint,
                        symbol=market["symbol"],
                        name=market["name"],
                    )

        db.insert_outcome(call_id)

        linked = db.backfill_whale_alert_call_ids(token_id=token_id, call_id=call_id)
        if linked:
            print(f"  [whale-backfill] linked {linked} whale alert(s) to call_id={call_id}")

        vip_tier   = parsed.get("vip_tier")
        mint_short = mint[:12] + ("..." if len(mint) > 12 else "")
        status_str = "resolved" if mint_resolved else "UNKNOWN"
        print(
            f"  [vip:{vip_message_type}] call_id={call_id} symbol={symbol} "
            f"mint={mint_short} ({status_str}) "
            f"mcap={parsed.get('mcap_at_call')} vip_tier={vip_tier}"
        )

        score_result = None
        try:
            score_result = scorer.score_call(
                call_id,
                extra_fields={"vip_tier": vip_tier} if vip_tier else None,
            )
            print(f"  [score] {symbol} call_id={call_id}  score={score_result['score']}  label={score_result['label']}")
            print(f"  Reasons: {', '.join(score_result['reasons'])}")
        except Exception as e:
            print(f"  [score] error: {e}")

        token_data = {
            "symbol":            symbol,
            "mint_address":      mint,
            "mcap_at_call":      parsed.get("mcap_at_call"),
            "security_flag":     None,
            "token_age_minutes": None,
            "vip_tier":          vip_tier,
            "channel_tag":       "solhousesignal_vip",
        }
        return 'logged', score_result, token_data

    # ── Whale Alert → whale_alerts table only, no scoring ───────────────────
    if vip_message_type == 'whale_alert':
        symbol = parsed.get("symbol")

        token_id = db.get_token_id_by_symbol(symbol) if symbol else None
        call_id  = db.get_call_id_by_token_name(symbol) if symbol else None

        whale_id = db.insert_whale_alert(
            symbol=symbol,
            sol_amount=parsed.get("whale_sol_spent"),
            wallet_size_sol=parsed.get("whale_wallet_sol"),
            mcap_at_whale=parsed.get("mcap_at_call"),
            signal_count=None,
            raw_message=text,
            token_id=token_id,
            call_id=call_id,
        )
        print(
            f"  [vip:whale_alert] id={whale_id} symbol={symbol} "
            f"sol={parsed.get('whale_sol_spent')} wallet={parsed.get('whale_wallet_sol')} "
            f"pct={parsed.get('whale_pct_spent')} call_id={call_id}"
        )
        return 'whale_alert', None, None

    return 'noise', None, None


# ── Catchup ───────────────────────────────────────────────────────────────────

def _catchup(client, entity, channel_cfg: dict, caller_id: int) -> int:
    """
    Scan messages since the last run for a single channel.

    If a last-seen message ID is recorded, fetches only newer messages
    via min_id. Otherwise falls back to the most recent CATCHUP_FALLBACK_LIMIT
    messages (first run). Returns the highest message ID seen (0 if none).
    """
    raw_handle = channel_cfg["handle"]
    handle   = raw_handle if raw_handle.startswith("@") else f"@{raw_handle}"
    c_type   = channel_cfg["channel_type"]
    c_id     = channel_cfg["id"]
    last_id  = _read_last_message_id(handle)

    if last_id is not None:
        kwargs = {"min_id": last_id}
        mode   = f"since msg_id={last_id}"
    else:
        kwargs = {"limit": CATCHUP_FALLBACK_LIMIT}
        mode   = f"limit={CATCHUP_FALLBACK_LIMIT} (first run)"

    print(f"[listener] Catchup {handle} ({mode})...")

    highest_id = last_id or 0

    for message in client.iter_messages(entity, **kwargs):
        if message.id > highest_id:
            highest_id = message.id

        if not message.text:
            continue
        if not is_candidate(message.text):
            continue

        # One malformed/out-of-range message must NOT crash the whole listener.
        # Skip it (and roll back the aborted transaction so the next message's
        # queries don't fail with "current transaction is aborted"). highest_id is
        # already advanced above, so catchup moves past the bad message permanently.
        try:
            if c_type == "lagging":
                status, *_ = handle_lagging(message, caller_id, c_id)
            elif c_type == "realtime":
                status, *_ = handle_realtime(message, caller_id, c_id)
            elif c_type == "vip":
                status, *_ = handle_vip(message, caller_id, c_id)
            else:
                continue
        except Exception as e:
            db.safe_rollback()
            print(f"  [catchup] skipped msg {getattr(message, 'id', '?')} on {handle}: {e}")
            continue

        _bump(handle, status)

    print(
        f"  [catchup done] {handle}  "
        f"logged={_stats[handle].get('logged', 0)}  "
        f"milestone={_stats[handle].get('milestone', 0)}  "
        f"dupe={_stats[handle].get('dupe', 0)}  "
        f"whale_alert={_stats[handle].get('whale_alert', 0)}  "
        f"noise={_stats[handle].get('noise', 0)}"
    )

    return highest_id


# ── Live listener ─────────────────────────────────────────────────────────────

_RECONNECT_BACKOFF = [5, 10, 30, 60]


def normalize_chat_id(chat_id: int) -> int:
    """
    Normalize a Telethon chat ID to a bare positive integer.

    Channel events arrive as negative IDs with a -100 prefix
    (e.g. -1002093384030) while get_entity() returns the bare ID
    (e.g. 2093384030). Stripping the -100 prefix unifies both forms.
    """
    s = str(abs(chat_id))
    if s.startswith("100"):
        return int(s[3:])
    return abs(chat_id)


# Channels re-listed for SHADOW measurement ONLY — kept out of all paper/live entry
# by the guard in the message handler below. solearlytrending was cut from trading
# 2026-03-30 ("losing way too much money"); this lets us re-measure it risk-free.
SHADOW_ONLY_CHANNELS = {
    c.strip() for c in os.getenv("SHADOW_ONLY_CHANNELS", "solearlytrending").split(",") if c.strip()
}


def run_listener() -> None:
    channels = db.get_active_channels()
    # solearlytrending is intentionally re-listed for shadow-only tracking (guarded
    # against trading downstream). If it doesn't show in shadow_report, confirm it's
    # active in the channels table.
    if not channels:
        print("[listener] No active channels found in database. Exiting.")
        return

    client = TelegramClient("Solana_meme_tracker_session", api_id, api_hash)
    client.start()
    client.get_dialogs()   # sync session's channel/dialog list before registering handlers

    # ── Resolve entities, run catchup, build routing map ──────────────────────
    # entity_map: chat_id (int) → {cfg, caller_id, handle}
    entity_map: dict[int, dict] = {}

    for channel_cfg in channels:
        handle    = channel_cfg["handle"]
        tg_handle = handle if handle.startswith("@") else f"@{handle}"

        _init_stats(tg_handle)

        try:
            entity = client.get_entity(tg_handle)
        except Exception as e:
            print(f"[listener] Could not resolve {tg_handle}: {e} — skipping")
            continue

        caller_id = db.upsert_caller(
            platform="telegram",
            handle=tg_handle,
            name=getattr(entity, "title", tg_handle),
        )

        highest_id = _catchup(client, entity, channel_cfg, caller_id)

        if highest_id:
            _write_last_message_id(tg_handle, highest_id)

        entity_map[normalize_chat_id(entity.id)] = {
            "cfg":       channel_cfg,
            "caller_id": caller_id,
            "handle":    tg_handle,
            "entity":    entity,
        }

    # ── Optional: SolHouse VIP channel (env-var, not in DB channels table) ──────

    if VIP_CHANNEL:
        try:
            vip_entity = client.get_entity(PeerChannel(normalize_chat_id(VIP_CHANNEL)))
            vip_handle = "@solhousesignal_vip"
            _init_stats(vip_handle)

            vip_caller_id = db.upsert_caller(
                platform="telegram",
                handle=vip_handle,
                name=getattr(vip_entity, "title", "SolHouse VIP"),
            )

            vip_channel_db = db.get_channel_by_handle('solhousesignal_vip')
            vip_cfg = {
                "id":           vip_channel_db["id"] if vip_channel_db else None,
                "channel_type": "vip",
                "handle":       vip_handle,
            }

            vip_highest = _catchup(client, vip_entity, vip_cfg, vip_caller_id)
            if vip_highest:
                _write_last_message_id(vip_handle, vip_highest)

            entity_map[normalize_chat_id(vip_entity.id)] = {
                "cfg":       vip_cfg,
                "caller_id": vip_caller_id,
                "handle":    vip_handle,
                "entity":    vip_entity,
            }
            print(f"[listener] VIP channel: ACTIVE ({VIP_CHANNEL})")
        except Exception as e:
            print(f"[listener] VIP channel: setup failed: {e}")
    else:
        print("[listener] VIP channel: not configured")

    if not entity_map:
        print("[listener] No channels could be resolved. Exiting.")
        client.disconnect()
        return

    # ── Register NewMessage handler ───────────────────────────────────────────

    client.get_dialogs()   # re-sync after catchup to ensure all entities are known

    @client.on(events.NewMessage(chats=[info["entity"] for info in entity_map.values()]))
    async def _handler(event):
        norm_id = normalize_chat_id(event.chat_id)
        info    = entity_map.get(norm_id)
        if info is None:
            return

        cfg      = info["cfg"]
        handle   = info["handle"]
        msg      = event.message

        if not msg.text or not is_candidate(msg.text):
            return

        try:
            if cfg["channel_type"] == "lagging":
                status, score_result, extra = handle_lagging(msg, info["caller_id"], cfg["id"])
            elif cfg["channel_type"] == "realtime":
                status, score_result, extra = handle_realtime(msg, info["caller_id"], cfg["id"], channel_handle=cfg["handle"])
            elif cfg["channel_type"] == "vip":
                await asyncio.sleep(0.1)  # Give DB commit time to propagate before scoring
                status, score_result, extra = handle_vip(msg, info["caller_id"], cfg["id"])
            else:
                return

            _bump(handle, status)

            # Circuit breaker check — AFTER parsing so every signal is logged to the DB
            if _monitor._dex_circuit_open:
                call_id = score_result.get("call_id") if score_result else None
                if call_id:
                    db.set_call_skip_reason(call_id, "dex_circuit_open")
                print(f"[listener] {handle} circuit open — call logged, skipping position")
                _write_last_message_id(handle, msg.id)
                return

            # Shadow-trade configured channels/lanes for measurement (no-op unless
            # SHADOW_LANES is set). Fires for EVERY scored call regardless of channel;
            # shadow_trader decides which to open. Fully independent of the routing
            # below and of the main exit path — managed only by shadow_monitor.py.
            if score_result and extra and shadow_trader.enabled():
                _track_task(
                    asyncio.create_task(shadow_trader.maybe_open_shadow(score_result, extra)),
                    f"shadow call_id={score_result.get('call_id')}",
                )

            # Lane testbed: capture the scorer result by value BEFORE routing nulls it,
            # so we can dispatch off the FINAL skip_reason once routing has labeled the lane.
            _lane_sr = score_result if (LANE_TESTBED_ENABLED and score_result and extra) else None

            # SHADOW-ONLY guard: shadow fired above (score_result captured by value),
            # so nulling it here blocks every downstream paper/live/alert entry while
            # keeping the call measured in shadow_report. This is what keeps a re-listed
            # but money-losing channel (solearlytrending) out of real trading.
            if score_result and cfg.get("handle") in SHADOW_ONLY_CHANNELS:
                _soc_cid = score_result.get("call_id")
                if _soc_cid:
                    db.set_call_skip_reason(_soc_cid, "shadow_only")
                print(f"[shadow_only] {cfg.get('handle')} call logged + shadowed — not traded")
                score_result = None

            # VIP tier-specific position routing
            if cfg["channel_type"] == "vip" and score_result and extra:
                score       = score_result.get("score", 0)
                vip_tier    = extra.get("vip_tier")
                channel_tag = extra.get("channel_tag", "") or ""
                mcap_at_call = float(extra.get("mcap_at_call") or 0)
                print(f"[vip_debug] tier={vip_tier} mcap={mcap_at_call} score={score} symbol={extra.get('symbol')}")
                if "solhousesignal_vip" in channel_tag:
                    call_id = score_result.get("call_id")
                    if vip_tier is None:
                        # No tier data — can't validate, skip explicitly for analysis.
                        if call_id:
                            db.set_call_skip_reason(call_id, "vip_missing_tier")
                        print(f"[paper] {extra.get('symbol')} skipped — VIP no tier data")
                        score_result = None
                    elif vip_tier == "safe" and score >= 45:
                        # Safe tier trades at 45+ to match Strategy A entry logic.
                        _track_task(
                            asyncio.create_task(
                                _dispatch_paper_open(
                                    score_result,
                                    extra,
                                    is_strategy_b=False,
                                    fallback_reason="paper_dispatch_fallthrough",
                                )
                            ),
                            f"A VIP safe call_id={call_id}",
                        )
                        _track_task(
                            asyncio.create_task(
                                _dispatch_paper_open(
                                    score_result,
                                    extra,
                                    is_strategy_b=True,
                                )
                            ),
                            f"B VIP safe call_id={call_id}",
                        )
                        score_result = None
                    elif vip_tier == "safe":
                        if call_id:
                            db.set_call_skip_reason(call_id, "vip_low_score")
                        print(f"[paper] {extra.get('symbol')} skipped — VIP safe score {score:.1f} < 45")
                        score_result = None
                    elif vip_tier == "gamble_risk":
                        if call_id:
                            db.set_call_skip_reason(call_id, "vip_paused")
                        print(f"[paper] {extra.get('symbol')} skipped — VIP gamble_risk paused")
                        score_result = None
                    elif vip_tier == "gamble" and mcap_at_call > 0 and mcap_at_call < 40_000 and score >= 50:
                        # Strategy A only: allow gamble tier through A filters; keep B paused as control.
                        mint_addr = extra.get("mint_address")
                        if mint_addr and not mint_addr.startswith(("INFERRED:", "UNKNOWN:")):
                            extra["sol_in_override"] = paper_trader.SOL_VIP_GAMBLE  # 0.25 SOL
                            extra["vip_tier"] = vip_tier
                            _track_task(
                                asyncio.create_task(
                                    _dispatch_paper_open(
                                        score_result,
                                        extra,
                                        is_strategy_b=False,
                                        fallback_reason="paper_dispatch_fallthrough",
                                    )
                                ),
                                f"A VIP gamble call_id={call_id}",
                            )
                            print(f"[vip] {extra.get('symbol', '?')} opening gamble position — A only (B paused)")
                        else:
                            call_id_g = score_result.get("call_id")
                            if call_id_g:
                                db.set_call_skip_reason(call_id_g, "unconfirmed")
                            print(f"[vip] {extra.get('symbol', '?')} skipped — no valid mint for gamble")
                        score_result = None
                    elif vip_tier in ("gamble_risk", "gamble") and mcap_at_call >= (25_000 if vip_tier == "gamble_risk" else 40_000):
                        # Mcap too high for experimental gamble trading
                        if call_id:
                            db.set_call_skip_reason(call_id, "vip_mcap_gate")
                        print(f"[paper] {extra.get('symbol')} skipped — VIP {vip_tier} mcap ${mcap_at_call/1000:.0f}k over gate")
                        score_result = None
                    elif vip_tier in ("gamble_risk", "gamble"):
                        if call_id:
                            db.set_call_skip_reason(call_id, "vip_low_score")
                        print(f"[paper] {extra.get('symbol')} skipped — VIP {vip_tier} score {score:.1f} < 50")
                        score_result = None
                    else:
                        # high_risk or unhandled VIP tier — skip explicitly for analysis.
                        if call_id:
                            db.set_call_skip_reason(call_id, "vip_unhandled_tier")
                        print(f"[paper] {extra.get('symbol')} skipped — VIP {vip_tier} tier")
                        score_result = None

            if cfg["channel_type"] == "vip" and score_result and extra:
                channel_tag = extra.get("channel_tag", "") or ""
                if "solhousesignal_vip" in channel_tag:
                    call_id = score_result.get("call_id")
                    if call_id:
                        db.set_call_skip_reason(call_id, "vip_route_fallthrough")
                    print(
                        f"[paper] {extra.get('symbol')} skipped — VIP routing fallthrough "
                        f"(tier={extra.get('vip_tier')}, score={score_result.get('score', 0):.1f})"
                    )
                    score_result = None

            # ── VIP gamble confirmation check (fires on lagging/solhousesignal messages) ──
            if cfg["channel_type"] == "lagging" and extra and extra.get("mint_address"):
                _cleanup_stale_vip_gamble()
                mint_addr = extra["mint_address"]
                if not mint_addr.startswith(("INFERRED:", "UNKNOWN:")):
                    pending = _pending_vip_gamble.pop(mint_addr, None)
                    if pending:
                        elapsed      = time.monotonic() - pending["timestamp"]
                        elapsed_mins = elapsed / 60
                        if elapsed < VIP_CONFIRM_MIN_SECS:
                            # Too soon — likely same signal propagating, not independent confirmation
                            db.set_call_skip_reason(pending["call_id"], "unconfirmed")
                            print(
                                f"[vip_confirm] {pending['symbol']} discarded — free channel too soon"
                                f" ({elapsed_mins:.1f} min, need 5+)"
                            )
                        else:
                            # Independent confirmation ≥ 5 min — open 0.25 SOL paper position
                            print(
                                f"[vip_confirm] {pending['symbol']} confirmed after {elapsed_mins:.1f} min"
                                f" — opening 0.25 SOL"
                            )
                            _track_task(
                                asyncio.create_task(
                                    _dispatch_paper_open(
                                        pending["score_result"],
                                        pending["extra"],
                                        is_strategy_b=False,
                                        fallback_reason="paper_dispatch_fallthrough",
                                    )
                                ),
                                f"A VIP confirm call_id={pending['call_id']}",
                            )
                            _track_task(
                                asyncio.create_task(
                                    _dispatch_paper_open(
                                        pending["score_result"],
                                        pending["extra"],
                                        is_strategy_b=True,
                                    )
                                ),
                                f"B VIP confirm call_id={pending['call_id']}",
                            )

            if score_result and score_result["label"] in ("alert", "strong_alert"):
                # Alert delivery should not gate trade execution.
                await alert_bot.send_alert(score_result, extra)
                if extra:
                    channel_tag = (extra.get("channel_tag") or extra.get("channel_handle") or "")
                    # VIP entries are already routed above; avoid double-open.
                    if "solhousesignal_vip" not in channel_tag:
                        call_id = score_result.get("call_id")
                        _track_task(
                            asyncio.create_task(
                                _dispatch_paper_open(
                                    score_result,
                                    extra,
                                    is_strategy_b=False,
                                    fallback_reason="paper_dispatch_fallthrough",
                                )
                            ),
                            f"A alert call_id={call_id}",
                        )
                        _track_task(
                            asyncio.create_task(
                                _dispatch_paper_open(
                                    score_result,
                                    extra,
                                    is_strategy_b=True,
                                )
                            ),
                            f"B alert call_id={call_id}",
                        )
                        try:
                            import live_trader
                            asyncio.create_task(live_trader.open_live_position(score_result, extra))
                        except Exception as le:
                            print(f"[live_trader] open failed: {le}")
            elif score_result and extra and score_result.get("score", 0) >= 63:
                channel_tag = (extra.get("channel_tag") or extra.get("channel_handle") or "")
                if "solhousesignal" in channel_tag and "vip" not in channel_tag:
                    call_id = score_result.get("call_id")
                    _track_task(
                        asyncio.create_task(
                            _dispatch_paper_open(
                                score_result,
                                extra,
                                is_strategy_b=False,
                                fallback_reason="paper_dispatch_fallthrough",
                            )
                        ),
                        f"A score>=63 call_id={call_id}",
                    )
                    _track_task(
                        asyncio.create_task(
                            _dispatch_paper_open(
                                score_result,
                                extra,
                                is_strategy_b=True,
                            )
                        ),
                        f"B score>=63 call_id={call_id}",
                    )
            elif score_result and extra:
                channel_tag = (extra.get("channel_tag") or extra.get("channel_handle") or "")
                call_id = score_result.get("call_id")
                score = float(score_result.get("score", 0) or 0)
                if (
                    call_id
                    and (
                        "solhousesignal" in channel_tag
                        or "solwhaletrending" in channel_tag
                    )
                    and "vip" not in channel_tag
                ):
                    db.set_call_skip_reason(call_id, "low_score")
                    print(f"[paper] {extra.get('symbol')} skipped — score {score:.1f} < 63")

            # Lane testbed: routing is done, so skip_reason (the lane label) is final.
            # Resolve lane_policy for A (anchors) + B (anchors+pockets) and open paper trades.
            if LANE_TESTBED_ENABLED and _lane_sr and extra:
                await _dispatch_lane_testbed(_lane_sr, extra)

            if status == "milestone" and extra:
                await alert_bot.send_milestone(**extra)

            # Persist the latest seen message ID after each live message
            _write_last_message_id(handle, msg.id)

        except Exception as e:
            # Roll back the aborted transaction so a single bad message (e.g. a numeric
            # overflow) doesn't poison every subsequent message with "current
            # transaction is aborted".
            db.safe_rollback()
            print(f"[listener] handler error ({handle} msg={msg.id}): {e}")
            _bump(handle, "error")

    # ── Print startup summary ─────────────────────────────────────────────────

    channel_summary = ", ".join(
        f"{info['handle']} ({info['cfg']['channel_type']})"
        for info in entity_map.values()
    )
    print(f"\n[listener] Live monitoring {len(entity_map)} channel(s): {channel_summary}")
    print("[listener] Press Ctrl+C to stop.\n")

    # ── Reconnect loop ────────────────────────────────────────────────────────

    attempt = 0
    try:
        while True:
            try:
                client.run_until_disconnected()
                break   # clean return (Ctrl+C propagates as KeyboardInterrupt above)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                delay = _RECONNECT_BACKOFF[min(attempt, len(_RECONNECT_BACKOFF) - 1)]
                print(
                    f"[listener] Disconnected: {e}. "
                    f"Reconnect attempt {attempt + 1} in {delay}s..."
                )
                time.sleep(delay)
                attempt += 1
                try:
                    client.connect()
                    print("[listener] Reconnected successfully.")
                    attempt = 0
                except Exception as e2:
                    print(f"[listener] Reconnect failed: {e2}")

    except KeyboardInterrupt:
        print("\n[listener] Shutting down...")

    finally:
        _print_final_stats()
        db.close_conn()
        client.disconnect()
        print("[listener] Goodbye.")


if __name__ == "__main__":
    _acquire_pid_lock()
    try:
        run_listener()
    finally:
        _release_pid_lock()
