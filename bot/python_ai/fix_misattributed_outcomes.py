"""
fix_misattributed_outcomes.py — One-shot migration for misattributed outcome data.

Finds outcome rows belonging to realtime channel calls (solwhaletrending,
solearlytrending) that have stated_multiplier / mcap_at_result / peak_multiplier
values that were incorrectly written by solhousesignal milestone messages.

For each affected row:
  - Clears  stated_multiplier, mcap_at_result, result_reported_at
  - Resets  peak_multiplier  from mcap_24h / mcap_at_call (or NULL if no data)
  - Resets  outcome_label    via _assign_label() (or NULL if no 24h data)

Safe to re-run — the query will return 0 rows once stated_multiplier is NULL.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

import db
from backfill import _assign_label


# ── Query ─────────────────────────────────────────────────────────────────────

def get_affected_rows() -> list[dict]:
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                o.call_id,
                t.symbol,
                o.stated_multiplier,
                o.mcap_at_result,
                o.peak_multiplier,
                o.outcome_label,
                o.mcap_24h,
                o.pct_change_24h,
                c.mcap_at_call
            FROM outcomes  o
            JOIN calls     c  ON c.id  = o.call_id
            JOIN channels  ch ON ch.id = c.channel_id
            JOIN tokens    t  ON t.id  = c.token_id
            WHERE o.stated_multiplier IS NOT NULL
              AND ch.handle != 'solhousesignal'
            ORDER BY o.call_id
            """
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── Fix a single row ──────────────────────────────────────────────────────────

def _fix_row(row: dict, dry_run: bool) -> str:
    """
    Compute new values, log the change, and write to DB.
    Returns 'with_data' or 'no_data'.
    """
    call_id  = row["call_id"]
    symbol   = (row["symbol"] or "?").ljust(14)
    mcap_24h     = float(row["mcap_24h"])     if row["mcap_24h"]     is not None else None
    pct_24h      = float(row["pct_change_24h"]) if row["pct_change_24h"] is not None else None
    mcap_at_call = float(row["mcap_at_call"]) if row["mcap_at_call"] is not None else None

    # ── Derive new peak_multiplier ────────────────────────────────────────────
    new_peak: float | None = None
    if mcap_24h is not None and mcap_at_call and mcap_at_call > 0:
        new_peak = round(mcap_24h / mcap_at_call, 2)

    # ── Derive new outcome_label ──────────────────────────────────────────────
    new_label: str | None = None
    has_24h = mcap_24h is not None and pct_24h is not None
    if has_24h:
        new_label = _assign_label(new_peak, pct_24h)

    # ── Log ──────────────────────────────────────────────────────────────────
    old_stated = row["stated_multiplier"]
    old_peak   = row["peak_multiplier"]
    old_result = row["mcap_at_result"]
    old_label  = row["outcome_label"]

    def _fmt_mcap(n):
        if n is None:
            return "n/a"
        n = float(n)
        if n >= 1_000_000:
            return f"${n/1_000_000:.1f}M"
        if n >= 1_000:
            return f"${n/1_000:.0f}k"
        return f"${n:.0f}"

    peak_str = f"{new_peak}x" if new_peak is not None else "NULL"
    note     = "" if has_24h else "  [no 24h data]"

    print(f"[fix] call_id={call_id}  {symbol}")
    print(
        f"      was: stated={old_stated}x  "
        f"peak={old_peak}  "
        f"mcap_result={_fmt_mcap(old_result)}  "
        f"label={old_label}"
    )
    print(
        f"      now: stated=NULL  "
        f"peak={peak_str}  "
        f"label={new_label}{note}"
    )

    # ── Write ─────────────────────────────────────────────────────────────────
    if not dry_run:
        conn = db.get_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE outcomes SET
                    stated_multiplier  = NULL,
                    mcap_at_result     = NULL,
                    result_reported_at = NULL,
                    peak_multiplier    = %s,
                    outcome_label      = %s,
                    updated_at         = NOW()
                WHERE call_id = %s
                """,
                (new_peak, new_label, call_id),
            )
        conn.commit()

    return "with_data" if has_24h else "no_data"


# ── Main ──────────────────────────────────────────────────────────────────────

def run_fix(dry_run: bool = False) -> None:
    rows = get_affected_rows()
    print(f"[fix_misattributed_outcomes] {len(rows)} affected row(s) found")
    if dry_run:
        print("[fix_misattributed_outcomes] DRY RUN — no DB writes")
    print()

    with_data = no_data = 0

    for row in rows:
        result = _fix_row(row, dry_run=dry_run)
        if result == "with_data":
            with_data += 1
        else:
            no_data += 1
        print()

    db.close_conn()
    print(
        f"[done] fixed={with_data + no_data}"
        f"  with_24h_data={with_data}"
        f"  no_24h_data={no_data}"
    )


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    run_fix(dry_run=dry_run)
