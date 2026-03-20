"""
fix_suspicious_peaks.py — One-shot cleanup for peak_multiplier > 100.

Two actions:

  Part 1 — AUTO-NULL (safe):
    Clears peak_multiplier on outcomes rows where the token's mint_address
    is INFERRED: or UNKNOWN: prefixed. These mints were never resolved so
    any peak they recorded is fabricated data.

  Part 2 — MANUAL REVIEW (no changes):
    Prints all outcomes with peak_multiplier > 100 on resolved mints.
    These may be legitimate (a real 100x token) or bad data from thin
    liquidity pools. Review the list and null individual rows manually
    if needed.

Usage:
    python3 fix_suspicious_peaks.py --dry-run   # preview Part 1, always prints Part 2
    python3 fix_suspicious_peaks.py             # apply Part 1, print Part 2
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import db


def _fmt_mcap(n) -> str:
    if n is None:
        return "n/a"
    n = float(n)
    if n >= 1_000_000:
        return f"${n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"${n / 1_000:.1f}k"
    return f"${n:.0f}"


def part1_auto_null(dry_run: bool) -> None:
    """Null peak_multiplier on INFERRED/UNKNOWN mint outcomes with peak > 100."""
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*)
            FROM outcomes o
            JOIN calls  c ON c.id  = o.call_id
            JOIN tokens t ON t.id  = c.token_id
            WHERE o.peak_multiplier > 100
              AND (
                t.mint_address LIKE 'INFERRED:%%'
                OR t.mint_address LIKE 'UNKNOWN:%%'
              )
            """
        )
        count = cur.fetchone()[0]

    print(f"\n── Part 1: Auto-null (INFERRED/UNKNOWN mints, peak > 100) ──")
    print(f"   Rows affected: {count}")

    if count == 0:
        print("   Nothing to do.")
        return

    if dry_run:
        print("   [dry-run] No changes written.")
        return

    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE outcomes SET
                peak_multiplier = NULL,
                updated_at      = NOW()
            WHERE peak_multiplier > 100
              AND call_id IN (
                SELECT c.id
                FROM calls  c
                JOIN tokens t ON t.id = c.token_id
                WHERE t.mint_address LIKE 'INFERRED:%%'
                   OR t.mint_address LIKE 'UNKNOWN:%%'
              )
            """
        )
        conn.commit()
    print(f"   Nulled {count} row(s).")


def part2_manual_review() -> None:
    """Print resolved-mint peaks > 100 for manual review. No changes made."""
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                c.id            AS call_id,
                t.symbol,
                t.mint_address,
                o.peak_multiplier,
                c.mcap_at_call,
                c.created_at
            FROM outcomes o
            JOIN calls  c ON c.id  = o.call_id
            JOIN tokens t ON t.id  = c.token_id
            WHERE o.peak_multiplier > 100
              AND t.mint_address IS NOT NULL
              AND t.mint_address NOT LIKE 'INFERRED:%%'
              AND t.mint_address NOT LIKE 'UNKNOWN:%%'
            ORDER BY o.peak_multiplier DESC
            """
        )
        rows = cur.fetchall()

    print(f"\n── Part 2: Manual review (resolved mints, peak > 100) ──")
    print(f"   {len(rows)} row(s) — no changes made automatically.\n")

    if not rows:
        print("   None found.")
        return

    print(f"   {'call_id':<8}  {'symbol':<14}  {'peak':>8}  {'entry_mcap':>12}  mint")
    print(f"   {'─'*8}  {'─'*14}  {'─'*8}  {'─'*12}  {'─'*44}")
    for call_id, symbol, mint, peak, entry_mcap, created_at in rows:
        sym   = (symbol or "?")[:14]
        p     = f"{float(peak):.1f}x"
        entry = _fmt_mcap(entry_mcap)
        print(f"   {call_id:<8}  {sym:<14}  {p:>8}  {entry:>12}  {mint}")

    print(
        "\n   To null a specific row:\n"
        "   UPDATE outcomes SET peak_multiplier = NULL WHERE call_id = <id>;\n"
    )


def main() -> None:
    dry_run = "--dry-run" in sys.argv[1:]
    if dry_run:
        print("[fix_suspicious_peaks] Dry-run mode — no changes will be written.")

    part1_auto_null(dry_run)
    part2_manual_review()


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
