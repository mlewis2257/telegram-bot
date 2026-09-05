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
import lane_policy
import peak_guard
import jupiter
import wallet as _wallet
from exit_config import EXIT_A_PAPER, EXIT_B_PAPER

# ── Config ────────────────────────────────────────────────────────────────────

PASS_INTERVAL        = 10     # target; actual interval depends on watchlist size
INTER_CALL_SLEEP     = 0.5    # seconds between each DexScreener call

# Coarse-routed lanes (WS_COARSE_EXIT_VARIANTS) exit on a deliberately slower cadence so a
# transient sub-15s dip can't trip profit_floor/trail before it recovers — matching shadow's
# 15s Jupiter sampling, the state proven to ride anchor runners further. exit_monitor is the
# SOLE evaluator of these lanes' exits (sol-monitor skips them), so this in-process last-eval
# map gives them a TRUE cadence without slowing the fast sweep for dense lanes like ride_vol.
COARSE_EXIT_INTERVAL = float(os.getenv("COARSE_EXIT_INTERVAL", "15"))
_coarse_last_eval: dict[int, float] = {}
# Per-batch-miss throttle in the PAPER EXIT sweep. Was hardcoded to INTER_CALL_SLEEP
# (0.5s), which stretched the exit sweep to 20-30s on busy days (batch-misses = fast
# nano-caps), sampling the volatile coins COARSER than shadow_monitor and firing stale
# exits — the paper-vs-shadow gap. shadow_monitor has no such throttle and keeps up fine,
# because every Jupiter fetch already routes through the SHARED api_rate_budget
# (_jup_try_acquire), so 429s are prevented at the acquire layer regardless. Default 0 =
# match shadow. Bump EXIT_FALLBACK_SLEEP only if the shared budget proves insufficient.
EXIT_FALLBACK_SLEEP  = float(os.getenv("EXIT_FALLBACK_SLEEP", "0"))
RATE_LIMIT_SLEEP     = 30     # seconds to back off on 429 responses
MAX_WATCHLIST_SPREAD = 50     # above this, spread calls evenly across the window
MIN_SCORE            = 45     # minimum conviction_score to include (vip_safe floor)
MAX_AGE_HOURS        = int(os.getenv("MONITOR_MAX_AGE_HOURS", "12"))  # only monitor calls from the last N hours (was hardcoded 24; halved + env-tunable to shrink the cold-tier firehose that floods Jupiter)
PAPER_EXIT_SWEEP_EVERY_PASSES = 3  # websocket monitor owns primary protection; poll sweep is fallback
# A paper position we can NEVER price (delisted/rugged, or a call whose mint never
# resolved past an INFERRED:/UNKNOWN: placeholder) must not sit open forever. After
# this many hours with no obtainable price, force it closed at 0 (-100%). Without
# this, unresolved-mint positions were skipped by the sweep and lingered indefinitely.
FORCE_CLOSE_UNPRICEABLE_HOURS = 4.0
# Watchlist tiering: calls with an open position are HOT (polled every pass — exits
# need fresh prices); everything else is COLD (milestone-tracking only, bloated by
# the trending firehose) and polled once every N passes. Keeps exits fast while
# cutting per-pass fetch volume ~Nx — the cold tier is what floods the price APIs.
MONITOR_COLD_EVERY_PASSES = int(os.getenv("MONITOR_COLD_EVERY_PASSES", "6"))

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
# Only PHONE-ALERT on a SUSTAINED outage, not the transient 429 flaps that happen
# constantly now (rate-limiting is handled — exits/monitoring run on Jupiter, so a
# flapping breaker is NOT a real outage). A genuine DexScreener outage lasts minutes;
# 429 flaps recover in seconds. Alert only if the breaker stays open this long.
DEX_OUTAGE_ALERT_AFTER = float(os.getenv("DEX_OUTAGE_ALERT_AFTER", "180"))  # seconds
_dex_open_since      = 0.0
_dex_outage_alerted  = False


def _record_dex_failure(symbol: str = "") -> None:
    global _dex_circuit_open, _dex_open_since, _dex_outage_alerted
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

    if len(recent) >= DEX_FAILURE_THRESHOLD:
        if not _dex_circuit_open:
            _dex_circuit_open = True
            _dex_open_since = now
            print("[monitor] DexScreener degraded — circuit breaker OPEN (running on Jupiter)")
        # Defer the phone alert until the outage is SUSTAINED — kills the 429-flap spam.
        if not _dex_outage_alerted and (now - _dex_open_since) >= DEX_OUTAGE_ALERT_AFTER:
            _dex_outage_alerted = True
            mins = int(DEX_OUTAGE_ALERT_AFTER // 60)
            asyncio.create_task(alert_bot.send_system_alert(
                f"DexScreener degraded for {mins}+ min.\n"
                "Monitoring & exits continue on Jupiter; fresh-coin entries may be delayed."
            ))


def _record_dex_success() -> None:
    global _dex_circuit_open, _dex_open_since, _dex_outage_alerted
    if _dex_circuit_open:
        _dex_circuit_open = False
        _dex_failures.clear()
        _dex_open_since = 0.0
        print("[monitor] DexScreener recovered — circuit breaker CLOSED")
        # Only send a 'recovered' alert if we actually alerted about the outage —
        # transient flaps never alerted, so they stay silent here too.
        if _dex_outage_alerted:
            _dex_outage_alerted = False
            asyncio.create_task(alert_bot.send_system_alert(
                "DexScreener recovered — back to normal."
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

async def _process_token(row: dict, dry_run: bool, prefetched_prices: dict | None = None) -> dict:
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

    # Use pre-fetched Jupiter price when available; fall back to Jupiter-first fetch.
    market = None
    jup_price = (prefetched_prices or {}).get(mint)
    if jup_price:
        mcap = data_fetcher.get_mcap_blended(mint, jup_price)
        if mcap:
            market = {"price_usd": jup_price, "mcap": mcap, "source": "jupiter_batch"}
    if not market:
        market = data_fetcher.fetch_token_price_jupiter_only(mint)

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

    # Log the REST-sweep price to ws_market_observations. This feed updates
    # peak_mcap (paper + live) but was previously never persisted, so phantom
    # peaks originating here were invisible in the observation table. Persisting
    # it makes every peak source auditable via the db_peak vs observed_peak
    # cross-check. See memory: phantom_peak_root_cause.
    if not dry_run:
        try:
            db.insert_ws_market_observation(
                call_id=call_id,
                mint_address=mint,
                signature=None,
                market=market,
            )
        except Exception as e:
            print(f"[monitor] {symbol_pad} observation write failed: {e}")

    # ── Peak update ───────────────────────────────────────────────────────────
    # Corroboration guard: a single spurious high reading from this REST sweep
    # can't ratchet the peak. Guard works in mcap space, so convert through
    # mcap_at_call. See peak_guard.py / memory: phantom_peak_root_cause.
    prior_peak_mcap = stored_peak * mcap_at_call
    guarded_mcap    = peak_guard.guard_peak(f"mon:{call_id}", current_mcap, prior_peak_mcap)
    guarded_mult    = (guarded_mcap / mcap_at_call) if mcap_at_call else 0.0
    is_new_peak = guarded_mult > stored_peak
    if is_new_peak:
        if not dry_run:
            db.update_peak_multiplier(call_id, guarded_mult)
        active_peak = guarded_mult
        result["new_peak"] = True
        tag = "[data]" if data_only else "[monitor]"
        print(
            f"{tag} {symbol_pad} call_id={call_id}"
            f"  NEW PEAK {_fmt_mult(guarded_mult)} ↑"
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
        # Call-level peak (absolute mcap). Kept ONLY as a supplementary proxy for
        # the live trail below — NOT as a paper exit baseline. See the per-trade
        # baselines used in the exit checks that follow.
        peak_mcap = active_peak * mcap_at_call

        # ── Paper A exit ───────────────────────────────────────────────────────
        # CRITICAL: judge the exit from the TRADE's own entry_price and its
        # trade-tracked peak — never from mcap_at_call. When the bot enters above
        # the call price (late entry on an already-pumped coin), using mcap_at_call
        # as the baseline counts the pre-entry pump toward peak_mult and arms/fires
        # the trail or hard-stop instantly (the 5–60s premature trail_stops). This
        # mirrors the live block below and ws_monitor's paper path.
        pos_a = db.get_open_paper_position(call_id, is_strategy_b=False)
        if pos_a and pos_a.get("entry_price"):
            a_entry   = float(pos_a["entry_price"])
            a_db_peak = float(pos_a.get("peak_mcap") or 0)
            a_peak    = peak_guard.guard_peak(f"monPA:{call_id}", current_mcap, a_db_peak)
            if a_peak > a_db_peak and a_entry > 0:
                db.update_paper_position_peak(call_id, a_peak, a_peak / a_entry, is_strategy_b=False)
            # Coarse-routed lanes: peak is ratcheted above, but the EXIT DECISION is left to
            # the dedicated exit_monitor sweep (its coarser cadence) so sol-monitor's dense
            # watchlist poll can't fire it early. Non-coarse lanes exit here as before.
            if not lane_policy.is_coarse_routed(pos_a.get("channel_handle"), pos_a.get("vip_tier"),
                                                pos_a.get("skip_reason"), pos_a.get("entry_time")):
                # Cap an un-corroborated up-spike for the exit trigger: a_peak holds at
                # the real level on a phantom, so min() blocks a phantom take-profit while
                # passing genuine climbs and pullbacks.
                a_eff = min(current_mcap, a_peak) if a_peak > 0 else current_mcap
                exit_result = paper_trader.check_exits(
                    call_id, a_eff, a_peak, a_entry,
                    mint=mint, is_strategy_b=False, exit_config=EXIT_A_PAPER,
                )
                if exit_result.should_exit:
                    exit_mcap = exit_result.exit_mcap or a_eff
                    paper_trader.close_position(call_id, exit_mcap, exit_result.reason, is_strategy_b=False)
                    peak_guard.clear(f"monPA:{call_id}")
                    print(f"  [paper] {symbol} closed — {exit_result.reason}")

        # ── Paper B exit ───────────────────────────────────────────────────────
        pos_b = db.get_open_paper_position(call_id, is_strategy_b=True)
        if pos_b and pos_b.get("entry_price"):
            b_entry   = float(pos_b["entry_price"])
            b_db_peak = float(pos_b.get("peak_mcap") or 0)
            b_peak    = peak_guard.guard_peak(f"monPB:{call_id}", current_mcap, b_db_peak)
            if b_peak > b_db_peak and b_entry > 0:
                db.update_paper_position_peak(call_id, b_peak, b_peak / b_entry, is_strategy_b=True)
            # Coarse-routed lanes: exit decision left to the exit_monitor sweep (see Paper A).
            if not lane_policy.is_coarse_routed(pos_b.get("channel_handle"), pos_b.get("vip_tier"),
                                                pos_b.get("skip_reason"), pos_b.get("entry_time")):
                b_eff = min(current_mcap, b_peak) if b_peak > 0 else current_mcap
                exit_result_b = paper_trader_b.check_exits(
                    call_id, b_eff, b_peak, b_entry,
                    mint=mint, is_strategy_b=True, exit_config=EXIT_B_PAPER,
                )
                if exit_result_b.should_exit:
                    exit_mcap_b = exit_result_b.exit_mcap or b_eff
                    paper_trader_b.close_position(call_id, exit_mcap_b, exit_result_b.reason, is_strategy_b=True)
                    peak_guard.clear(f"monPB:{call_id}")
                    print(f"  [paper_b] {symbol} closed — {exit_result_b.reason}")

        # ── Live trade exit check ──────────────────────────────────────────────
        # Uses the live position's own DB-backed peak — independent of paper A's
        # DexScreener peak so a paper A runner never prematurely arms the live trail.
        try:
            pos_live = db.get_open_live_position(call_id)
            if pos_live:
                # Anchor exit multiples on the REAL fill, not the laggy feed entry: the feed
                # under-records entry on fast risers, which inflates peak/current multiples —
                # arming trails/floors early AND masking losers past the hard-stop. The quote
                # path already anchors on the fill; this fixes the feed-fallback + peak column.
                # Feed entry is the fallback only for pre-instrumentation positions.
                live_entry_price  = pos_live.get("entry_price_fill") or pos_live["entry_price"]
                live_current_mult = (current_mcap / live_entry_price) if live_entry_price else 0.0
                # Use the higher of: live's own DB peak, paper A's (already
                # guard-corroborated) peak, or the guarded current price. peak_mcap
                # is corroborated via the paper peak block above; only the raw
                # current_mcap needs the corroboration guard here.
                live_db_peak = float(pos_live["peak_mcap"] or 0)
                prior_live   = max(live_db_peak, peak_mcap)
                live_peak_mcap = peak_guard.guard_peak(f"monL:{call_id}", current_mcap, prior_live)
                if live_peak_mcap > live_db_peak and live_entry_price > 0:
                    db.update_live_position_peak(call_id, live_peak_mcap, live_peak_mcap / live_entry_price)
                live_eff = min(current_mcap, live_peak_mcap) if live_peak_mcap > 0 else current_mcap
                # Phase 2: swap the feed triple for the real (sell-quote) basis when armed;
                # returns the feed triple unchanged when LIVE_EXIT_USE_QUOTE is off or a quote
                # is unavailable. The feed peak above is still persisted for records/fallback.
                exit_cur, exit_peak, exit_entry, _basis, _raw_mult = await live_trader.live_exit_basis(
                    call_id, pos_live, live_eff, live_peak_mcap, live_entry_price)
                live_exit = live_trader.check_live_exits(
                    call_id, exit_cur, exit_peak, exit_entry,
                    exit_config=live_trader._LIVE_EXIT_CONFIG,
                    raw_mult=_raw_mult,
                )
                if live_exit.should_exit:
                    # Decision uses the real basis; the RECORD keeps the feed exit mcap
                    # (exit_price col) so it stays auditable against exit_price_fill.
                    await live_trader.close_live_position(call_id, live_eff, live_exit.reason)
                    peak_guard.clear(f"monL:{call_id}")
                    print(f"  [live] {symbol} closed — {live_exit.reason} [basis={_basis}]")
                elif live_trader.LIVE_EXIT_QUOTE_LOG:
                    # Phase-1 observation: quote the real sellable value, log real vs
                    # feed multiple. Read-only — drives no sells (see LIVE_EXIT_QUOTE_LOG).
                    _eff_obs = await live_trader.live_effective_current(pos_live)
                    if _eff_obs:
                        _synth, _rmult = _eff_obs
                        _fmult = (current_mcap / live_entry_price) if live_entry_price else 0.0
                        print(f"  [live] MULT {symbol} call_id={call_id} "
                              f"real={_rmult:.2f}x feed={_fmult:.2f}x")
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
        # RPC errors are treated as UNKNOWN (not zero) so a transient Helius
        # outage never triggers an auto-close on a valid position.
        balance_confirmed_zero = False
        try:
            balance, _ = await jupiter.get_token_balance(mint, wallet_addr, rpc_url)
            balance_confirmed_zero = (balance == 0)
        except Exception as rpc_err:
            print(f"[monitor] {symbol} on-chain balance check failed — skipping stale increment: {rpc_err}")
            _stale_checks.pop(call_id, None)
            continue

        if not balance_confirmed_zero:
            # Token still held; price feed just temporarily unavailable
            _stale_checks.pop(call_id, None)
            continue

        # ── Both price feed and on-chain balance confirmed gone ───────────────
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

async def _check_paper_exits(skip_call_ids: set[int] | None = None,
                             include_live: bool = True,
                             handle_coarse: bool = True) -> int:
    """
    Exit sweep for all open paper positions, independent of watchlist age.

    Runs once per pass AFTER the main watchlist loop to catch positions
    that aged out of the 24-hour watchlist window before being closed.
    Returns the number of positions closed this sweep.
    `skip_call_ids` avoids re-fetching positions already checked earlier in the
    same monitor pass.

    `include_live` gates the live-position exit check. The dedicated exit_monitor
    process calls this with include_live=False so that live on-chain SELLS remain
    owned solely by sol-monitor (this process) + ws_monitor — two processes must
    never race to submit the same real sell. Paper closes are idempotent (close
    only WHERE status='open'), so paper is safe to sweep from multiple processes.
    """
    if _dex_circuit_open:
        # The sweep prices off the Jupiter batch first, so a DexScreener outage must
        # NOT stop exits (it used to — that's how a DexScreener flood silently halted
        # closing). Note it and continue on Jupiter.
        print("[monitor] DexScreener breaker open — exit sweep running on Jupiter")

    skip_call_ids = skip_call_ids or set()

    positions_a = {p["call_id"]: p for p in db.get_open_paper_positions(is_strategy_b=False)}
    positions_b = {p["call_id"]: p for p in db.get_open_paper_positions(is_strategy_b=True)}
    call_ids = sorted(set(positions_a.keys()) | set(positions_b.keys()))
    if not call_ids:
        return 0

    # Pre-fetch Jupiter prices for all mints in one batch call.
    mints_needed = []
    for cid in call_ids:
        if cid in skip_call_ids:
            continue
        ref_pos = positions_a.get(cid) or positions_b.get(cid)
        m = (ref_pos or {}).get("mint_address") or ""
        if m and not m.startswith(("INFERRED:", "UNKNOWN:")):
            mints_needed.append(m)
    sweep_prices = data_fetcher.fetch_prices_batch_jupiter(mints_needed) if mints_needed else {}

    closed = 0
    for call_id in call_ids:
        if call_id in skip_call_ids:
            continue
        pos_a = positions_a.get(call_id)
        pos_b = positions_b.get(call_id)
        ref   = pos_a or pos_b
        symbol = (ref or {}).get("symbol") or "?"
        mint   = (ref or {}).get("mint_address")

        def _force_close_unpriceable(reason_word: str) -> int:
            """Close any open A/B leg that's past the grace period when no price
            can be obtained — delisted/rugged, OR a mint that never resolved past
            an INFERRED:/UNKNOWN: placeholder. Closes at 0 (-100%). Returns count.

            This is the backstop that stops unpriceable positions lingering open
            forever; both the unresolved-mint and the delisted paths funnel here so
            they share one threshold (FORCE_CLOSE_UNPRICEABLE_HOURS)."""
            n = 0
            now = datetime.now(timezone.utc)
            for strategy, pos in (("A", pos_a), ("B", pos_b)):
                if not pos:
                    continue
                et = pos.get("entry_time")
                if not et:
                    continue
                if et.tzinfo is None:
                    et = et.replace(tzinfo=timezone.utc)
                hours_open = (now - et).total_seconds() / 3600
                if hours_open <= FORCE_CLOSE_UNPRICEABLE_HOURS:
                    continue
                tag = "[paper]" if strategy == "A" else "[paper_b]"
                print(f"{tag} {symbol} {reason_word} — force closed after {hours_open:.1f}h")
                if strategy == "A":
                    paper_trader.close_position(call_id, 0, "hard_stop", is_strategy_b=False)
                else:
                    paper_trader_b.close_position(call_id, 0, "hard_stop", is_strategy_b=True)
                n += 1
            return n

        if not mint or mint.startswith(("INFERRED:", "UNKNOWN:")):
            # Mint never resolved to a real address — every price fetch is impossible,
            # so don't just skip (that left these open indefinitely). Cut once aged out.
            closed += _force_close_unpriceable("unresolved mint")
            continue

        try:
            market = None
            jup_price = sweep_prices.get(mint)
            if jup_price:
                blended = data_fetcher.get_mcap_blended(mint, jup_price)
                if blended:
                    market = {"price_usd": jup_price, "mcap": blended, "source": "jupiter_batch"}
            if not market:
                # DexScreener-capable fallback, matching shadow_monitor's _mcap chain.
                # Jupiter DROPS thin pump.fun nano-caps mid-life (migration / low liquidity).
                # With a Jupiter-only fallback, an open position that ran 2x then went
                # Jupiter-unpriceable was held blind for FORCE_CLOSE_UNPRICEABLE_HOURS and
                # force-closed at -100% — e.g. KITWIFMIT peaked 2.13x, Jamey 2.61x, both booked
                # -100% while shadow (DexScreener) trail-stopped them at +57%/+45%. That single
                # bug drove most of the paper-vs-shadow gap. Safe here — unlike the cold-tier
                # watchlist firehose that forced jupiter_only, the EXIT sweep only prices OPEN
                # positions (bounded), and DexScreener stays under DEX_MAX_PER_MIN.
                market = data_fetcher.fetch_token_price_fast(mint)
                if EXIT_FALLBACK_SLEEP > 0:
                    await asyncio.sleep(EXIT_FALLBACK_SLEEP)

            if not market or not market.get("mcap"):
                closed += _force_close_unpriceable("delisted")
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

            # Coarse-routed lanes (WS_COARSE_EXIT_VARIANTS): the dedicated exit_monitor
            # process (handle_coarse=True) owns their exit DECISION at its own cadence; the
            # sol-monitor sweep (handle_coarse=False) still ratchets their peak + force-closes
            # them but must NOT fire the exit here, so their effective exit cadence is the
            # single dedicated sweep's interval rather than the OR of every driver.
            _lane_coarse = lane_policy.is_coarse_routed(
                (ref or {}).get("channel_handle"), (ref or {}).get("vip_tier"),
                (ref or {}).get("skip_reason"), (ref or {}).get("entry_time"))
            _decide_exit = handle_coarse or not _lane_coarse
            # Throttle coarse lanes to COARSE_EXIT_INTERVAL (only this owner-sweep evaluates
            # them, so an in-process timer is exact). Peak-ratchet/force-close still run below.
            if _decide_exit and _lane_coarse:
                _now = time.monotonic()
                if _now - _coarse_last_eval.get(call_id, 0.0) < COARSE_EXIT_INTERVAL:
                    _decide_exit = False
                else:
                    _coarse_last_eval[call_id] = _now

            paper_a_peak_mcap = 0.0  # tracked for live peak propagation below
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
                    paper_a_peak_mcap = peak_mcap  # propagate to live section (always)
                    if _decide_exit:
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
                elif _decide_exit:
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
            # Reads peak from the live position's own DB record — independent of
            # paper A so closing paper A never zeroes out the live peak.
            try:
                pos_live = db.get_open_live_position(call_id) if include_live else None
                if pos_live:
                    # Anchor exit multiples on the REAL fill, not the laggy feed entry: the feed
                    # under-records entry on fast risers, which inflates peak/current multiples —
                    # arming trails/floors early AND masking losers past the hard-stop. The quote
                    # path already anchors on the fill; this fixes the feed-fallback + peak column.
                    # Feed entry is the fallback only for pre-instrumentation positions.
                    live_entry_price  = pos_live.get("entry_price_fill") or pos_live["entry_price"]
                    live_current_mult = (current_mcap / live_entry_price) if live_entry_price else 0.0
                    live_peak_mcap = max(
                        float(pos_live["peak_mcap"] or 0),
                        paper_a_peak_mcap,
                        current_mcap,
                    )
                    if live_peak_mcap > float(pos_live["peak_mcap"] or 0) and live_entry_price > 0:
                        db.update_live_position_peak(call_id, live_peak_mcap, live_peak_mcap / live_entry_price)
                    # Phase 2: real (sell-quote) basis when armed, else the feed triple unchanged.
                    exit_cur, exit_peak, exit_entry, _basis, _raw_mult = await live_trader.live_exit_basis(
                        call_id, pos_live, current_mcap, live_peak_mcap, live_entry_price)
                    live_exit = live_trader.check_live_exits(
                        call_id, exit_cur, exit_peak, exit_entry,
                        exit_config=live_trader._LIVE_EXIT_CONFIG,
                        raw_mult=_raw_mult,
                    )
                    if live_exit.should_exit:
                        # Decision on real basis; record keeps feed exit mcap (see site above).
                        await live_trader.close_live_position(call_id, current_mcap, live_exit.reason)
                        print(f"  [live] {symbol} closed — {live_exit.reason} [basis={_basis}]")
            except Exception as le:
                print(f"  [live] exit check error for {symbol} call_id={call_id}: {le}")

        except Exception as e:
            db.safe_rollback()
            print(f"[paper] exit check error for {symbol} call_id={call_id}: {e}")

    return closed


async def run_exit_sweep(skip_call_ids: set[int] | None = None,
                         include_live: bool = True,
                         handle_coarse: bool = True) -> int:
    """
    Public entry point for the dedicated exit_monitor process.

    Delegates to the shared open-position exit sweep so the exit logic (peak
    ratchet, zero-mark guard, guard_trough, lane exit + order_flow, force-close)
    has a SINGLE source of truth used by both sol-monitor and sol-exit-monitor.

    handle_coarse=True (default, used by exit_monitor) means this sweep OWNS the exit
    decision for coarse-routed lanes; sol-monitor calls with False so those lanes exit
    only on exit_monitor's cadence (EXIT_MONITOR_INTERVAL) instead of the fastest driver.
    """
    return await _check_paper_exits(skip_call_ids=skip_call_ids, include_live=include_live,
                                    handle_coarse=handle_coarse)


# ── Full pass ─────────────────────────────────────────────────────────────────

async def run_pass(pass_num: int, dry_run: bool) -> dict:
    """Run one full monitoring pass across the active watchlist."""
    if _dex_circuit_open:
        # The pass prices off the prefetched Jupiter batch, so DON'T skip it when the
        # DexScreener breaker is open — skipping used to stop monitoring + exits every
        # time the trending flood tripped the breaker. Just note it and run on Jupiter.
        print(f"[monitor] Pass {pass_num} — DexScreener breaker open, running on Jupiter")

    _failures_this_pass.clear()
    watchlist = db.get_active_watchlist(min_score=MIN_SCORE, max_age_hours=MAX_AGE_HOURS)
    total = len(watchlist)

    # ── Tier the watchlist ────────────────────────────────────────────────────
    # HOT = calls with an open position (poll every pass; exits need fresh prices).
    # COLD = milestone-tracking only — poll once every MONITOR_COLD_EVERY_PASSES.
    # This is the core of the rate-limit fix: the trending firehose bloats COLD,
    # so polling it every pass was flooding both DexScreener and Jupiter.
    hot_ids: set[int] = set()
    if not dry_run:
        try:
            for p in db.get_open_paper_positions(is_strategy_b=False):
                hot_ids.add(p["call_id"])
            for p in db.get_open_paper_positions(is_strategy_b=True):
                hot_ids.add(p["call_id"])
            for p in db.get_open_live_positions():
                hot_ids.add(p["call_id"])
        except Exception as e:
            print(f"[monitor] hot-set build failed — treating all as hot: {e}")
            hot_ids = {r["call_id"] for r in watchlist}
    cold_due = dry_run or (pass_num % MONITOR_COLD_EVERY_PASSES == 0)
    watchlist = [r for r in watchlist if (r["call_id"] in hot_ids or cold_due)]
    count = len(watchlist)

    print(f"[monitor] Pass {pass_num} — watching {count}/{total} active call(s) "
          f"({len(hot_ids)} hot{', +cold' if cold_due else ''})")

    # One Jupiter batch call for all active mints — avoids N DexScreener calls.
    active_mints = [
        row["mint_address"] for row in watchlist
        if row.get("mint_address")
        and not (row["mint_address"] or "").startswith(("INFERRED:", "UNKNOWN:"))
    ]
    prefetched_prices = data_fetcher.fetch_prices_batch_jupiter(active_mints) if active_mints else {}

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

            result = await _process_token(row, dry_run, prefetched_prices=prefetched_prices)
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
    # handle_coarse=False: coarse-routed lanes are owned by the dedicated exit_monitor
    # sweep, so sol-monitor ratchets their peaks + force-closes but does not fire their
    # exit here (keeps their exit cadence = exit_monitor's interval, not this loop's).
    if not dry_run and (pass_num % PAPER_EXIT_SWEEP_EVERY_PASSES == 0):
        paper_closed = await _check_paper_exits(skip_call_ids=checked_call_ids,
                                                handle_coarse=False)
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
