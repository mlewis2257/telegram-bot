"""
monitor.py — Real-time price monitor for active high-conviction calls.

Continuously polls DexScreener for tokens on the active watchlist, updates
peak_multiplier in real time, and sends Telegram alerts when milestone
thresholds (2x / 5x / 10x) or significant drawdowns (30% / 50%) are hit.

WHY THIS EXISTS:
backfill.py captures price at fixed intervals (1h / 4h / 24h). A token that
goes 5x in 2 hours and crashes is labelled 'rug' by backfill because it's
down at 24h. monitor.py fixes this by updating peak_multiplier continuously
so the true intraday high is always captured.

WATCHLIST:
Calls where conviction_score >= 55 (caution+) AND created_at within 24h
AND mint_address is resolved. Ordered by score DESC.

Usage:
    python3 monitor.py              # runs forever, 60s between passes
    python3 monitor.py --once       # single pass, exits
    python3 monitor.py --dry-run    # logs only, no DB writes, no alerts sent
"""

import asyncio
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(__file__))

import db
import data_fetcher
import alert_bot
import paper_trader
import paper_trader_b
import live_trader
import jupiter
import wallet as _wallet
from exit_config import EXIT_A_PAPER, EXIT_B_PAPER

# ── Config ────────────────────────────────────────────────────────────────────

PASS_INTERVAL        = 20     # target; actual interval depends on watchlist size
INTER_CALL_SLEEP     = 0.5    # seconds between each DexScreener call
RATE_LIMIT_SLEEP     = 30     # seconds to back off on 429 responses
MAX_WATCHLIST_SPREAD = 50     # above this, spread calls evenly across the window
MIN_SCORE            = 45     # minimum conviction_score to include (vip_safe floor)
MAX_AGE_HOURS        = 24     # only monitor calls from the last N hours
PAPER_EXIT_SWEEP_EVERY_PASSES = 3  # websocket monitor owns primary protection; poll sweep is fallback

MILESTONE_THRESHOLDS    = [2.0, 5.0, 10.0]  # send alert on first crossing of each
SUPPRESS_HISTORICAL_HOURS = 2  # don't fire milestones/drawdowns for stored peaks on old tokens

DRAWDOWN_WARN = 0.30   # 30% from peak → ⚠️ pulling back alert
DRAWDOWN_DUMP = 0.50   # 50% from peak → 🚨 dump alert

PEAK_MAX_MULT    = 100.0  # above this, treat as data error and skip

# ── Stale live position detection ─────────────────────────────────────────────
# Tracks consecutive passes where a live position has no DexScreener price AND
# zero on-chain balance. After STALE_THRESHOLD hits the position is auto-closed
# with exit_reason='rug'.

_stale_checks:  dict[int, int] = {}  # call_id -> consecutive zero-balance count
STALE_THRESHOLD = 3

# ── Data-only fetch throttle ───────────────────────────────────────────────────
# VIP paused calls only need price checks every 60s, not every 20s.
_data_only_last_fetch: dict[int, float] = {}
DATA_ONLY_FETCH_INTERVAL = 60  # seconds between DexScreener fetches for data_only rows

# ── DexScreener circuit breaker ────────────────────────────────────────────────

_dex_failures: list[float] = []
_failures_this_pass: set[str] = set()   # deduplicate failures per mint per pass
DEX_FAILURE_WINDOW    = 60   # seconds
DEX_FAILURE_THRESHOLD = 8    # failures within window before circuit opens
_dex_circuit_open     = False


def _record_dex_failure(symbol: str = "") -> None:
    global _dex_circuit_open
    # One mint can only contribute one failure per pass — prevents double-counting
    # when both the price check and volume check fail for the same token.
    if symbol and symbol in _failures_this_pass:
        return
    if symbol:
        _failures_this_pass.add(symbol)
    now = time.monotonic()
    _dex_failures.append(now)
    recent = [t for t in _dex_failures if now - t < DEX_FAILURE_WINDOW]
    _dex_failures[:] = recent

    if len(recent) >= DEX_FAILURE_THRESHOLD and not _dex_circuit_open:
        _dex_circuit_open = True
        print("[monitor] DexScreener outage — circuit breaker OPEN")
        asyncio.create_task(alert_bot.send_system_alert(
            "DexScreener outage detected!\n"
            "All buys and exits PAUSED until connection restored."
        ))


def _record_dex_success() -> None:
    global _dex_circuit_open
    if _dex_circuit_open:
        _dex_circuit_open = False
        _dex_failures.clear()
        print("[monitor] DexScreener recovered — circuit breaker CLOSED")
        asyncio.create_task(alert_bot.send_system_alert(
            "DexScreener connection restored.\n"
            "Bot resuming normal operation."
        ))

# ── Alert dedup state — persisted across restarts ─────────────────────────────
# Tracks which alert keys have already been sent per call_id.
# Persisted to _STATE_FILE so restarts don't re-fire dump alerts for dead tokens.
# Entries older than ALERTS_TTL_HOURS are pruned on load.
#
# Keys per call_id: '2x', '5x', '10x', '30pct_drawdown', '50pct_dump'

_STATE_DIR       = os.path.join(os.path.dirname(__file__), ".last_run")
_STATE_FILE      = os.path.join(_STATE_DIR, "monitor_alerts.json")
ALERTS_TTL_HOURS = 48

_alerts_sent:       dict[int, set[str]] = {}
_alerts_first_seen: dict[int, str]      = {}  # call_id → ISO timestamp of first alert


# ── Helpers ───────────────────────────────────────────────────────────────────

def _inter_call_sleep(watchlist_size: int) -> float:
    """Spread calls evenly across PASS_INTERVAL when the watchlist is large."""
    if watchlist_size > MAX_WATCHLIST_SPREAD:
        return max(INTER_CALL_SLEEP, PASS_INTERVAL / watchlist_size)
    return INTER_CALL_SLEEP


def _fmt_mult(m: float) -> str:
    return f"{m:.1f}x"


def _load_alerts_state() -> None:
    """
    Load _alerts_sent from disk on startup.
    Prunes entries whose first-seen timestamp is older than ALERTS_TTL_HOURS.
    """
    global _alerts_sent, _alerts_first_seen
    if not os.path.exists(_STATE_FILE):
        return

    try:
        with open(_STATE_FILE, "r") as f:
            data = json.load(f)
    except Exception as e:
        print(f"[monitor] could not load alert state: {e}")
        return

    cutoff    = datetime.now(timezone.utc) - timedelta(hours=ALERTS_TTL_HOURS)
    first_seen = data.pop("_first_seen", {})
    loaded = pruned = 0

    for str_id, keys in data.items():
        try:
            call_id = int(str_id)
        except ValueError:
            continue

        ts_str = first_seen.get(str_id)
        if ts_str:
            try:
                ts = datetime.fromisoformat(ts_str)
                if ts < cutoff:
                    pruned += 1
                    continue
            except ValueError:
                pass  # malformed ts — keep the entry

        _alerts_sent[call_id]       = set(keys)
        _alerts_first_seen[call_id] = ts_str or datetime.now(timezone.utc).isoformat()
        loaded += 1

    print(f"[monitor] alert state loaded — {loaded} entries, {pruned} pruned (>{ALERTS_TTL_HOURS}h old)")


def _save_alerts_state() -> None:
    """Persist _alerts_sent to disk with first-seen timestamps for TTL tracking."""
    try:
        os.makedirs(_STATE_DIR, exist_ok=True)
        now_iso = datetime.now(timezone.utc).isoformat()
        data: dict = {}
        first_seen: dict = {}
        for call_id, keys in _alerts_sent.items():
            str_id              = str(call_id)
            data[str_id]        = sorted(keys)
            first_seen[str_id]  = _alerts_first_seen.get(call_id, now_iso)
        data["_first_seen"] = first_seen
        with open(_STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[monitor] could not save alert state: {e}")


# ── Per-token processing ──────────────────────────────────────────────────────

async def _process_token(row: dict, dry_run: bool) -> dict:
    """
    Fetch current price for one token, update peak if higher, fire any alerts.
    Returns {new_peak: bool, alerts_sent: int, skipped: bool}.

    Rows with data_only=True (VIP paused calls) only update peak_multiplier.
    All alert, exit, and circuit-breaker logic is skipped for them.
    """
    call_id      = row["call_id"]
    symbol       = row["symbol"] or "?"
    symbol_pad   = symbol.ljust(14)
    mint         = row["mint_address"]
    mcap_at_call = float(row["mcap_at_call"]) if row["mcap_at_call"] else 0.0
    stored_peak  = float(row["peak_multiplier"]) if row["peak_multiplier"] else 0.0
    data_only    = bool(row.get("data_only", False))

    result = {"new_peak": False, "alerts_sent": 0, "skipped": False}

    if mcap_at_call <= 0:
        result["skipped"] = True
        return result

    market = data_fetcher.fetch_token_price(mint)
    if not market or not market.get("mcap"):
        if not data_only:
            _record_dex_failure(symbol)
        result["skipped"] = True
        return result

    current_mcap = float(market["mcap"])

    if current_mcap <= 0:
        if not data_only:
            _record_dex_failure(symbol)
        result["skipped"] = True
        return result

    # Sanity check — skip only if mcap is essentially phantom data (< 2% of entry)
    # DexScreener returned valid data — the token just rugged. Not a fetcher failure.
    if mcap_at_call > 0 and current_mcap < mcap_at_call * 0.02:
        print(f"[monitor] {symbol_pad} suspicious mcap ${current_mcap:,.0f} vs entry ${mcap_at_call:,.0f} — skipping")
        result["skipped"] = True
        return result

    if not data_only:
        _record_dex_success()
    current_mult = current_mcap / mcap_at_call

    if current_mult < 0.01:
        print(f"[monitor] {symbol_pad} dead/delisted, skipping")
        result["skipped"] = True
        return result

    if current_mult > PEAK_MAX_MULT:
        print(f"[monitor] {symbol_pad} suspicious peak {_fmt_mult(current_mult)} — skipping")
        result["skipped"] = True
        return result

    # ── Peak update ───────────────────────────────────────────────────────────
    is_new_peak = current_mult > stored_peak
    if is_new_peak:
        if not dry_run:
            db.update_peak_multiplier(call_id, current_mult)
        active_peak = current_mult
        result["new_peak"] = True
        tag = "[data]" if data_only else "[monitor]"
        print(
            f"{tag} {symbol_pad} call_id={call_id}"
            f"  NEW PEAK {_fmt_mult(current_mult)} ↑"
        )
    else:
        active_peak = stored_peak
        if not data_only:
            print(
                f"[monitor] {symbol_pad} call_id={call_id}"
                f"  {_fmt_mult(current_mult)} (peak: {_fmt_mult(active_peak)})"
            )

    # Data-only rows: peak tracking done — skip all alerts and exit logic
    if data_only:
        return result

    created_at = row.get("created_at")
    recently_created = (
        (datetime.now(timezone.utc) - created_at).total_seconds()
        < SUPPRESS_HISTORICAL_HOURS * 3600
    ) if created_at else False

    sent = _alerts_sent.setdefault(call_id, set())
    if call_id not in _alerts_first_seen and sent:
        _alerts_first_seen[call_id] = datetime.now(timezone.utc).isoformat()

    # ── Milestone threshold alerts ────────────────────────────────────────────
    for threshold in MILESTONE_THRESHOLDS:
        key = f"{int(threshold)}x"
        if active_peak >= threshold and key not in sent:
            # Suppress if token is old and the stored peak already exceeded the
            # threshold — this is a historical peak, not a live crossing.
            if not recently_created and stored_peak >= threshold:
                sent.add(key)  # mark so we never re-evaluate this threshold
                continue
            sent.add(key)
            _alerts_first_seen.setdefault(call_id, datetime.now(timezone.utc).isoformat())
            result["alerts_sent"] += 1
            print(f"  [monitor] → milestone alert: {symbol} hit {key}")
            if not dry_run:
                await alert_bot.send_monitor_milestone(
                    call_id=call_id,
                    symbol=symbol,
                    mint_address=mint,
                    multiplier=threshold,
                    mcap_at_call=row["mcap_at_call"],
                    current_mcap=current_mcap,
                    source_message_id=row.get("source_message_id"),
                    channel_handle=row.get("channel_handle"),
                )
                await asyncio.sleep(1.0)
                _save_alerts_state()

    # ── Drawdown alerts ───────────────────────────────────────────────────────
    if active_peak > 0 and current_mult < active_peak:
        drawdown = (active_peak - current_mult) / active_peak

        # Suppress if we never witnessed the peak live this pass and the token
        # isn't freshly created — avoids firing on stored historical peaks at startup.
        if not recently_created and not is_new_peak:
            sent.update({"30pct_drawdown", "50pct_dump"})
        else:
            # Check 50% first — avoids sending both alerts for the same drop
            if drawdown >= DRAWDOWN_DUMP and "50pct_dump" not in sent:
                sent.add("50pct_dump")
                sent.add("30pct_drawdown")  # severe alert supersedes the milder one
                _alerts_first_seen.setdefault(call_id, datetime.now(timezone.utc).isoformat())
                result["alerts_sent"] += 1
                print(f"  [monitor] → dump alert: {symbol} -{drawdown:.0%} from peak")
                if not dry_run:
                    await alert_bot.send_drawdown_alert(
                        call_id=call_id,
                        symbol=symbol,
                        mint_address=mint,
                        peak_mult=active_peak,
                        current_mult=current_mult,
                        drawdown_pct=drawdown * 100,
                        entry_mult=current_mult,
                        severe=True,
                    )
                    await asyncio.sleep(1.0)
                    _save_alerts_state()

            elif drawdown >= DRAWDOWN_WARN and "30pct_drawdown" not in sent:
                sent.add("30pct_drawdown")
                _alerts_first_seen.setdefault(call_id, datetime.now(timezone.utc).isoformat())
                result["alerts_sent"] += 1
                print(f"  [monitor] → drawdown alert: {symbol} -{drawdown:.0%} from peak")
                if not dry_run:
                    await alert_bot.send_drawdown_alert(
                        call_id=call_id,
                        symbol=symbol,
                        mint_address=mint,
                        peak_mult=active_peak,
                        current_mult=current_mult,
                        drawdown_pct=drawdown * 100,
                        entry_mult=current_mult,
                        severe=False,
                    )
                    await asyncio.sleep(1.0)
                    _save_alerts_state()

    # ── Paper trade exit check ────────────────────────────────────────────────
    if not dry_run:
        peak_mcap   = active_peak * mcap_at_call
        exit_result = paper_trader.check_exits(
            call_id, current_mcap, peak_mcap, mcap_at_call,
            mint=mint, is_strategy_b=False, exit_config=EXIT_A_PAPER,
        )
        if exit_result.should_exit:
            exit_mcap = exit_result.exit_mcap or current_mcap
            paper_trader.close_position(call_id, exit_mcap, exit_result.reason, is_strategy_b=False)
            print(f"  [paper] {symbol} closed — {exit_result.reason}")

        exit_result_b = paper_trader_b.check_exits(
            call_id, current_mcap, peak_mcap, mcap_at_call,
            mint=mint, is_strategy_b=True, exit_config=EXIT_B_PAPER,
        )
        if exit_result_b.should_exit:
            exit_mcap_b = exit_result_b.exit_mcap or current_mcap
            paper_trader_b.close_position(call_id, exit_mcap_b, exit_result_b.reason, is_strategy_b=True)
            print(f"  [paper_b] {symbol} closed — {exit_result_b.reason}")

        # ── Live trade exit check ──────────────────────────────────────────────
        try:
            live_exit = live_trader.check_live_exits(call_id, current_mcap, peak_mcap, mcap_at_call)
            if live_exit.should_exit:
                await live_trader.close_live_position(call_id, current_mcap, live_exit.reason)
                print(f"  [live] {symbol} closed — {live_exit.reason}")
        except Exception as le:
            print(f"  [live] exit check error for {symbol} call_id={call_id}: {le}")

    return result


# ── Stale live position sweep ─────────────────────────────────────────────────

async def _check_live_stale(dry_run: bool) -> None:
    """
    For each open live position, check whether DexScreener has no price AND
    the on-chain token balance is zero. After STALE_THRESHOLD consecutive passes
    in that state, auto-close the position with exit_reason='rug'.
    """
    positions = db.get_open_live_positions()
    if not positions:
        return

    wallet_addr = _wallet.get_public_key()
    rpc_url     = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

    for pos in positions:
        call_id = pos["call_id"]
        symbol  = pos.get("symbol") or "?"
        mint    = pos.get("mint_address")

        if not mint or mint.startswith(("INFERRED:", "UNKNOWN:")):
            continue

        # ── Check DexScreener price ────────────────────────────────────────────
        market   = data_fetcher.fetch_token_price(mint)
        price_ok = bool(market and market.get("mcap"))
        await asyncio.sleep(INTER_CALL_SLEEP)

        if price_ok:
            _stale_checks.pop(call_id, None)
            continue

        # ── No price — verify on-chain balance ────────────────────────────────
        balance_zero = False
        try:
            balance, _ = await jupiter.get_token_balance(mint, wallet_addr, rpc_url)
            balance_zero = (balance == 0)
        except Exception:
            balance_zero = True

        if not balance_zero:
            # Token still held; price feed just temporarily unavailable
            _stale_checks.pop(call_id, None)
            continue

        # ── Both price and balance gone — increment stale counter ─────────────
        _stale_checks[call_id] = _stale_checks.get(call_id, 0) + 1
        count = _stale_checks[call_id]
        print(f"[monitor] {symbol} stale check {count}/{STALE_THRESHOLD}")

        if count >= STALE_THRESHOLD:
            print(
                f"[monitor] {symbol} call_id={call_id}"
                f" — auto-closing as rug after {STALE_THRESHOLD} stale checks"
            )
            if not dry_run:
                db.close_live_position_db(
                    call_id=call_id,
                    exit_price=0,
                    sol_out=0,
                    exit_reason="rug",
                    tx_signature=None,
                )
                try:
                    await alert_bot._get_bot().send_message(
                        chat_id=alert_bot._chat_id(),
                        text=(
                            f"🪦 ${symbol} auto-closed as rug"
                            f" — zero balance for {STALE_THRESHOLD} consecutive checks"
                        ),
                        disable_web_page_preview=True,
                    )
                except Exception as e:
                    print(f"[monitor] rug alert send failed for {symbol}: {e}")
            del _stale_checks[call_id]


# ── Paper position exit sweep ─────────────────────────────────────────────────

async def _check_paper_exits(skip_call_ids: set[int] | None = None) -> int:
    """
    Exit sweep for all open paper positions, independent of watchlist age.

    Runs once per pass AFTER the main watchlist loop to catch positions
    that aged out of the 24-hour watchlist window before being closed.
    Returns the number of positions closed this sweep.
    `skip_call_ids` avoids re-fetching positions already checked earlier in the
    same monitor pass.
    """
    if _dex_circuit_open:
        print("[monitor] Circuit breaker open — skipping paper exit sweep")
        return 0

    skip_call_ids = skip_call_ids or set()

    positions_a = {p["call_id"]: p for p in db.get_open_paper_positions(is_strategy_b=False)}
    positions_b = {p["call_id"]: p for p in db.get_open_paper_positions(is_strategy_b=True)}
    call_ids = sorted(set(positions_a.keys()) | set(positions_b.keys()))
    if not call_ids:
        return 0

    closed = 0
    for call_id in call_ids:
        if call_id in skip_call_ids:
            continue
        pos_a = positions_a.get(call_id)
        pos_b = positions_b.get(call_id)
        ref   = pos_a or pos_b
        symbol = (ref or {}).get("symbol") or "?"
        mint   = (ref or {}).get("mint_address")

        if not mint or mint.startswith(("INFERRED:", "UNKNOWN:")):
            continue

        try:
            market = data_fetcher.fetch_token_price(mint)
            await asyncio.sleep(INTER_CALL_SLEEP)

            if not market or not market.get("mcap"):
                for strategy, pos in (("A", pos_a), ("B", pos_b)):
                    if not pos:
                        continue
                    entry_time = pos["entry_time"]
                    if entry_time:
                        if entry_time.tzinfo is None:
                            entry_time = entry_time.replace(tzinfo=timezone.utc)
                        hours_open = (datetime.now(timezone.utc) - entry_time).total_seconds() / 3600
                        if hours_open > 4:
                            tag = "[paper]" if strategy == "A" else "[paper_b]"
                            print(f"{tag} {symbol} delisted — force closed after {hours_open:.1f}h")
                            if strategy == "A":
                                paper_trader.close_position(call_id, 0, "hard_stop", is_strategy_b=False)
                            else:
                                paper_trader_b.close_position(call_id, 0, "hard_stop", is_strategy_b=True)
                            closed += 1
                continue

            current_mcap = float(market["mcap"])

            # Backfill entry_volume_h1 on first successful volume fetch
            if market.get("volume_h1"):
                if pos_a:
                    existing_vol_a = db.get_paper_position_entry_volume(call_id, is_strategy_b=False)
                    if existing_vol_a is None:
                        db.update_paper_position_entry_volume(call_id, float(market["volume_h1"]), is_strategy_b=False)
                if pos_b:
                    existing_vol_b = db.get_paper_position_entry_volume(call_id, is_strategy_b=True)
                    if existing_vol_b is None:
                        db.update_paper_position_entry_volume(call_id, float(market["volume_h1"]), is_strategy_b=True)

            peak_info = db.get_call_peak_info(call_id)

            # Keep the call-level peak for eventual signal analysis, but record
            # per-position observed peaks on trading_positions for clean A/B comparisons.
            ref_entry_price = float((ref or {}).get("entry_price") or 0.0)
            if peak_info and peak_info.get("mcap_at_call") and ref_entry_price > 0:
                mcap_at_call_val = float(peak_info["mcap_at_call"])
                if mcap_at_call_val > 0:
                    db.update_peak_multiplier(call_id, current_mcap / mcap_at_call_val)

            for strategy, pos in (("A", pos_a), ("B", pos_b)):
                if not pos:
                    continue
                entry_price = float(pos["entry_price"]) if pos.get("entry_price") else 0.0
                if entry_price <= 0:
                    continue
                current_mult = current_mcap / entry_price
                peak_mcap = float(pos.get("peak_mcap") or 0.0)
                peak_mult = float(pos.get("peak_multiplier") or 0.0)
                if current_mult > peak_mult:
                    db.update_paper_position_peak(
                        call_id,
                        current_mcap,
                        current_mult,
                        is_strategy_b=(strategy == "B"),
                    )
                    peak_mcap = current_mcap
                    pos["peak_mcap"] = current_mcap
                    pos["peak_multiplier"] = current_mult

                if strategy == "A":
                    exit_result = paper_trader.check_exits(
                        call_id, current_mcap, peak_mcap, entry_price,
                        mint=mint, is_strategy_b=False, exit_config=EXIT_A_PAPER,
                    )
                    if exit_result.should_exit:
                        exit_mcap = exit_result.exit_mcap or current_mcap
                        print(
                            f"[paper] checking exit: {symbol}"
                            f"  current={current_mult:.2f}x → closing ({exit_result.reason})"
                        )
                        paper_trader.close_position(call_id, exit_mcap, exit_result.reason, is_strategy_b=False)
                        closed += 1
                else:
                    exit_result = paper_trader_b.check_exits(
                        call_id, current_mcap, peak_mcap, entry_price,
                        mint=mint, is_strategy_b=True, exit_config=EXIT_B_PAPER,
                    )
                    if exit_result.should_exit:
                        exit_mcap = exit_result.exit_mcap or current_mcap
                        print(
                            f"[paper_b] checking exit: {symbol}"
                            f"  current={current_mult:.2f}x → closing ({exit_result.reason})"
                        )
                        paper_trader_b.close_position(call_id, exit_mcap, exit_result.reason, is_strategy_b=True)
                        closed += 1

            # ── Live position exit check ───────────────────────────────────────
            try:
                live_entry_price = float((pos_a or ref or {}).get("entry_price") or 0.0)
                live_peak_mcap = float((pos_a or {}).get("peak_mcap") or 0.0)
                live_exit = live_trader.check_live_exits(call_id, current_mcap, live_peak_mcap, live_entry_price)
                if live_exit.should_exit:
                    await live_trader.close_live_position(call_id, current_mcap, live_exit.reason)
                    print(f"  [live] {symbol} closed — {live_exit.reason}")
            except Exception as le:
                print(f"  [live] exit check error for {symbol} call_id={call_id}: {le}")

        except Exception as e:
            db.safe_rollback()
            print(f"[paper] exit check error for {symbol} call_id={call_id}: {e}")

    return closed


# ── Full pass ─────────────────────────────────────────────────────────────────

async def run_pass(pass_num: int, dry_run: bool) -> dict:
    """Run one full monitoring pass across the active watchlist."""
    if _dex_circuit_open:
        print(f"[monitor] Pass {pass_num} — circuit breaker open, skipping pass")
        return {"checked": 0, "new_peaks": 0, "alerts_sent": 0, "errors": 0}

    _failures_this_pass.clear()
    watchlist = db.get_active_watchlist(min_score=MIN_SCORE, max_age_hours=MAX_AGE_HOURS)
    count = len(watchlist)

    print(f"[monitor] Pass {pass_num} — watching {count} active call(s)")

    sleep_per_call = _inter_call_sleep(count) if count > 0 else INTER_CALL_SLEEP
    stats   = {"checked": 0, "new_peaks": 0, "alerts_sent": 0, "errors": 0}
    skipped = 0
    checked_call_ids: set[int] = set()

    for row in watchlist:
        try:
            call_id = row["call_id"]
            if row.get("data_only"):
                now  = time.monotonic()
                last = _data_only_last_fetch.get(call_id, 0)
                if now - last < DATA_ONLY_FETCH_INTERVAL:
                    continue
                _data_only_last_fetch[call_id] = now

            result = await _process_token(row, dry_run)
            checked_call_ids.add(call_id)
            await asyncio.sleep(sleep_per_call)

            if result["skipped"]:
                skipped += 1
            else:
                stats["checked"] += 1
                if result["new_peak"]:
                    stats["new_peaks"] += 1
                stats["alerts_sent"] += result["alerts_sent"]

        except Exception as e:
            sym = row.get("symbol") or "?"
            print(f"[monitor] error on {sym} call_id={row['call_id']}: {e}")
            db.safe_rollback()
            stats["errors"] += 1
            await asyncio.sleep(sleep_per_call)

    if count > 0 and skipped == count:
        print(f"[monitor] WARNING: DexScreener returned no data for any of the {count} token(s)")

    # ── Paper trade exit sweep (all open positions, age-independent) ──────────
    if not dry_run and (pass_num % PAPER_EXIT_SWEEP_EVERY_PASSES == 0):
        paper_closed = await _check_paper_exits(skip_call_ids=checked_call_ids)
        if paper_closed:
            print(f"[paper] exit sweep closed {paper_closed} position(s)")

    # ── Stale live position sweep ──────────────────────────────────────────────
    await _check_live_stale(dry_run=dry_run)

    suffix = " (dry-run)" if dry_run else ""
    print(
        f"[monitor] Pass {pass_num} complete — "
        f"{stats['checked']} checked, "
        f"{stats['new_peaks']} new peak(s), "
        f"{stats['alerts_sent']} alert(s) sent."
        f" Next pass in {PASS_INTERVAL}s.{suffix}"
    )
    return stats


# ── Main ──────────────────────────────────────────────────────────────────────

async def _async_main(once: bool, dry_run: bool) -> None:
    if dry_run:
        print("[monitor] Dry-run mode — no DB writes, no alerts sent")

    _load_alerts_state()

    pass_num = 1
    while True:
        await run_pass(pass_num, dry_run=dry_run)
        if once:
            break
        await asyncio.sleep(PASS_INTERVAL)
        pass_num += 1


def main() -> None:
    args    = sys.argv[1:]
    once    = "--once"    in args
    dry_run = "--dry-run" in args

    try:
        asyncio.run(_async_main(once=once, dry_run=dry_run))
    except KeyboardInterrupt:
        print("\n[monitor] Stopped.")
    finally:
        _save_alerts_state()
        db.close_conn()


if __name__ == "__main__":
    main()
