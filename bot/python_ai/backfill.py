"""
backfill.py — Continuous DexScreener price backfill for realtime call outcomes.

Queries outcomes rows that are old enough for each time interval (1h, 4h, 24h)
but have not yet been snapshotted, fetches current price/mcap from DexScreener,
and writes the result back. After the 24h snapshot, assigns an outcome_label.

Usage:
    python3 backfill.py             # single pass, up to 50 per interval
    python3 backfill.py --loop      # runs forever, 60s sleep between passes
    python3 backfill.py --dry-run   # logs what would be updated, no DB writes

Outcome labelling (applied after 24h backfill):
    pumped_and_dumped — peak_multiplier >= 2.0 AND pct_change_24h < -70
    rug               — pct_change_24h < -70 (no significant peak)
    slow_bleed        — pct_change_24h between -70 and -30
    runner            — (peak >= 2x AND held >= -30%) OR pct_change_24h >= 100%
    flat              — everything else
"""

import sys
import time
import os

sys.path.insert(0, os.path.dirname(__file__))

import db
import data_fetcher

# ── Config ────────────────────────────────────────────────────────────────────

INTERVALS       = [1, 4, 24]   # hours, processed in order
BATCH_LIMIT     = 50           # rows per interval per pass
INTER_CALL_SLEEP = 0.5         # seconds between DexScreener calls
LOOP_SLEEP      = 60           # seconds between passes in --loop mode


# ── Outcome labelling ─────────────────────────────────────────────────────────

def _assign_label(peak_multiplier: float | None, pct_change_24h: float | None) -> str:
    """
    Determine outcome_label from 24h snapshot data.

    Priority order:
      1. pumped_and_dumped — hit 2x but rugged (most specific, checked first)
      2. rug               — down >70% regardless of peak
      3. slow_bleed        — down 30-70%
      4. runner            — hit 2x AND held, OR still up >100% at 24h
      5. flat              — everything else
    """
    peak = float(peak_multiplier) if peak_multiplier is not None else 0.0
    pct  = float(pct_change_24h)  if pct_change_24h  is not None else 0.0

    if peak >= 2.0 and pct < -70:
        return "pumped_and_dumped"
    if pct < -70:
        return "rug"
    if pct < -30:
        return "slow_bleed"
    if peak >= 2.0 and pct >= -30:
        return "runner"
    if pct >= 100:
        return "runner"
    return "flat"


# ── Single interval pass ──────────────────────────────────────────────────────

def _run_interval(interval_hours: int, dry_run: bool) -> int:
    """
    Process one batch of pending rows for a single interval.
    Returns the number of rows successfully written (or would-write in dry-run).
    """
    tag = f"[backfill:{interval_hours}h]"
    rows = db.get_pending_backfill(interval_hours, limit=BATCH_LIMIT)

    if not rows:
        return 0

    written = 0

    for row in rows:
        call_id    = row["call_id"]
        symbol     = row["symbol"] or "?"
        mint       = row["mint_address"]
        mcap_entry = row["mcap_at_call"]

        # ── Fetch current price ───────────────────────────────────────────────
        market = data_fetcher.fetch_token_price(mint)
        time.sleep(INTER_CALL_SLEEP)

        if not market:
            print(f"  {tag}  {symbol:<12} call_id={call_id}  not on DexScreener")
            if not dry_run:
                # Mark as attempted so we don't hammer a dead token every pass.
                # Write NULLs so the backfilled_Xh_at timestamp is set.
                label = None
                if interval_hours == 24:
                    label = _assign_label(row["peak_multiplier"], None)
                db.update_outcome_interval(
                    call_id=call_id,
                    interval_hours=interval_hours,
                    price=None,
                    mcap=None,
                    pct_change=None,
                    outcome_label=label,
                )
            written += 1
            continue

        current_mcap  = market["mcap"]
        current_price = market["price_usd"]

        # ── Calculate pct_change ─────────────────────────────────────────────
        pct_change = None
        if mcap_entry and mcap_entry > 0 and current_mcap is not None:
            entry = float(mcap_entry)
            pct_change = ((current_mcap - entry) / entry) * 100

        # ── Format for logging ───────────────────────────────────────────────
        if pct_change is not None:
            sign = "+" if pct_change >= 0 else ""
            pct_str = f"{sign}{pct_change:.1f}%"
        else:
            pct_str = "n/a"

        mcap_str = f"${current_mcap:,.0f}" if current_mcap else "n/a"

        # ── Determine outcome_label (24h only) ───────────────────────────────
        label = None
        label_str = ""
        if interval_hours == 24:
            # Use the pct_change we just computed (fresh), not the DB value
            label = _assign_label(row["peak_multiplier"], pct_change)
            label_str = f"  → labeled: {label}"

        print(f"  {tag}  {symbol:<12} call_id={call_id}  {pct_str}  mcap={mcap_str}{label_str}")

        if not dry_run:
            db.update_outcome_interval(
                call_id=call_id,
                interval_hours=interval_hours,
                price=current_price,
                mcap=current_mcap,
                pct_change=round(pct_change, 2) if pct_change is not None else None,
                outcome_label=label,
            )

        written += 1

    return written


# ── Main ──────────────────────────────────────────────────────────────────────

def run_pass(dry_run: bool = False) -> None:
    """Run one full pass across all three intervals."""
    total = 0
    for hours in INTERVALS:
        n = _run_interval(hours, dry_run)
        total += n

    suffix = " (dry-run)" if dry_run else ""
    print(f"[backfill] Pass complete — {total} row(s) processed{suffix}")


def main() -> None:
    args     = sys.argv[1:]
    loop     = "--loop"    in args
    dry_run  = "--dry-run" in args

    if dry_run:
        print("[backfill] Dry-run mode — no DB writes")

    if loop:
        print(f"[backfill] Loop mode — sleeping {LOOP_SLEEP}s between passes")
        try:
            while True:
                run_pass(dry_run=dry_run)
                time.sleep(LOOP_SLEEP)
        except KeyboardInterrupt:
            print("\n[backfill] Stopped.")
    else:
        run_pass(dry_run=dry_run)

    db.close_conn()


if __name__ == "__main__":
    main()
