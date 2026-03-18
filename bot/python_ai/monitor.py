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
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

import db
import data_fetcher
import alert_bot

# ── Config ────────────────────────────────────────────────────────────────────

PASS_INTERVAL        = 60     # seconds between full passes
INTER_CALL_SLEEP     = 0.3    # seconds between each DexScreener call
RATE_LIMIT_SLEEP     = 30     # seconds to back off on 429 responses
MAX_WATCHLIST_SPREAD = 50     # above this, spread calls evenly across the window
MIN_SCORE            = 55     # minimum conviction_score to include (caution+)
MAX_AGE_HOURS        = 24     # only monitor calls from the last N hours

MILESTONE_THRESHOLDS    = [2.0, 5.0, 10.0]  # send alert on first crossing of each
SUPPRESS_HISTORICAL_HOURS = 2  # don't fire milestones/drawdowns for stored peaks on old tokens

DRAWDOWN_WARN = 0.30   # 30% from peak → ⚠️ pulling back alert
DRAWDOWN_DUMP = 0.50   # 50% from peak → 🚨 dump alert

# ── In-memory dedup state ─────────────────────────────────────────────────────
# Tracks which alert keys have already been sent per call_id this session.
# Resets on restart — acceptable since peak_multiplier persists in DB.
#
# Keys per call_id: '2x', '5x', '10x', '30pct_drawdown', '50pct_dump'

_alerts_sent: dict[int, set[str]] = {}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _inter_call_sleep(watchlist_size: int) -> float:
    """Spread calls evenly across PASS_INTERVAL when the watchlist is large."""
    if watchlist_size > MAX_WATCHLIST_SPREAD:
        return max(INTER_CALL_SLEEP, PASS_INTERVAL / watchlist_size)
    return INTER_CALL_SLEEP


def _fmt_mult(m: float) -> str:
    return f"{m:.1f}x"


# ── Per-token processing ──────────────────────────────────────────────────────

async def _process_token(row: dict, dry_run: bool) -> dict:
    """
    Fetch current price for one token, update peak if higher, fire any alerts.
    Returns {new_peak: bool, alerts_sent: int, skipped: bool}.
    """
    call_id      = row["call_id"]
    symbol       = row["symbol"] or "?"
    symbol_pad   = symbol.ljust(14)
    mint         = row["mint_address"]
    mcap_at_call = float(row["mcap_at_call"]) if row["mcap_at_call"] else 0.0
    stored_peak  = float(row["peak_multiplier"]) if row["peak_multiplier"] else 0.0

    result = {"new_peak": False, "alerts_sent": 0, "skipped": False}

    if mcap_at_call <= 0:
        result["skipped"] = True
        return result

    market = data_fetcher.fetch_token_price(mint)
    if not market or not market.get("mcap"):
        result["skipped"] = True
        return result

    current_mcap = float(market["mcap"])
    current_mult = current_mcap / mcap_at_call

    if current_mult < 0.01:
        print(f"[monitor] {symbol_pad} dead/delisted, skipping")
        result["skipped"] = True
        return result

    # ── Peak update ───────────────────────────────────────────────────────────
    is_new_peak = current_mult > stored_peak
    if is_new_peak:
        if not dry_run:
            db.update_peak_multiplier(call_id, current_mult)
        active_peak = current_mult
        result["new_peak"] = True
        print(
            f"[monitor] {symbol_pad} call_id={call_id}"
            f"  NEW PEAK {_fmt_mult(current_mult)} ↑"
        )
    else:
        active_peak = stored_peak
        print(
            f"[monitor] {symbol_pad} call_id={call_id}"
            f"  {_fmt_mult(current_mult)} (peak: {_fmt_mult(active_peak)})"
        )

    created_at = row.get("created_at")
    recently_created = (
        (datetime.now(timezone.utc) - created_at).total_seconds()
        < SUPPRESS_HISTORICAL_HOURS * 3600
    ) if created_at else False

    sent = _alerts_sent.setdefault(call_id, set())

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
                )
                await asyncio.sleep(1.0)

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

            elif drawdown >= DRAWDOWN_WARN and "30pct_drawdown" not in sent:
                sent.add("30pct_drawdown")
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

    return result


# ── Full pass ─────────────────────────────────────────────────────────────────

async def run_pass(pass_num: int, dry_run: bool) -> dict:
    """Run one full monitoring pass across the active watchlist."""
    watchlist = db.get_active_watchlist(min_score=MIN_SCORE, max_age_hours=MAX_AGE_HOURS)
    count = len(watchlist)

    print(f"[monitor] Pass {pass_num} — watching {count} active call(s)")

    if count == 0:
        return {"checked": 0, "new_peaks": 0, "alerts_sent": 0, "errors": 0}

    sleep_per_call = _inter_call_sleep(count)
    stats   = {"checked": 0, "new_peaks": 0, "alerts_sent": 0, "errors": 0}
    skipped = 0

    for row in watchlist:
        try:
            result = await _process_token(row, dry_run)
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
            stats["errors"] += 1
            await asyncio.sleep(sleep_per_call)

    if skipped == count:
        print(f"[monitor] WARNING: DexScreener returned no data for any of the {count} token(s)")

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
        db.close_conn()


if __name__ == "__main__":
    main()
