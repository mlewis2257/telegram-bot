"""
fix_missing_metadata.py — One-time backfill for tokens missing on-chain fields.

Targets tokens where liq_at_detection IS NULL AND security_flag IS NOT NULL —
realtime channel tokens that have a security flag (confirming type_b origin) but
are missing fields whose regexes were fixed after initial logging.

For each such token, finds its most recent initial_call raw_message, re-runs
the type_b parser, and writes the extracted fields back to the tokens table.

Safe to re-run — upsert_token_realtime_metadata uses COALESCE so existing
non-NULL values are never overwritten.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import db
from parsers import type_b


def get_tokens_missing_metadata() -> list[dict]:
    """
    Return tokens that have a resolved mint but are missing bundle_count
    (used as the sentinel for 'on-chain fields never written').
    """
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.id AS token_id, t.symbol, t.mint_address
            FROM tokens t
            WHERE t.liq_at_detection IS NULL
              AND t.security_flag IS NOT NULL
              AND t.mint_resolved = TRUE
              AND t.mint_address NOT LIKE 'UNKNOWN:%%'
              AND t.mint_address NOT LIKE 'INFERRED:%%'
            ORDER BY t.id
            """
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_latest_raw_message(token_id: int) -> str | None:
    """
    Return the raw_message from the most recent initial_call for this token.
    """
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT raw_message
            FROM calls
            WHERE token_id = %s
              AND message_type IN ('initial_call', 'lagging_call')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (token_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def run_fix():
    tokens = get_tokens_missing_metadata()
    print(f"[fix] {len(tokens)} token(s) with missing metadata.")

    updated = skipped = 0

    for tok in tokens:
        token_id = tok["token_id"]
        symbol   = tok["symbol"] or "?"

        raw = get_latest_raw_message(token_id)
        if not raw:
            print(f"  [fix] {symbol:<14} token_id={token_id}  no raw message found")
            skipped += 1
            continue

        # Try whale parser first, fall back to trending
        parsed = type_b.parse_entry_signal_whale(raw)
        if not parsed:
            parsed = type_b.parse_entry_signal_trending(raw)
        if not parsed:
            print(f"  [fix] {symbol:<14} token_id={token_id}  no parseable message")
            skipped += 1
            continue

        db.upsert_token_realtime_metadata(
            token_id=token_id,
            mint_resolved=True,
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

        print(
            f"  [fix] {symbol:<14} token_id={token_id}"
            f"  bundle_count={parsed.get('bundle_count')}"
            f"  holders={parsed.get('hodl_count')}"
            f"  liq=${parsed.get('liq_at_detection') or 'n/a'}"
        )
        updated += 1

    db.close_conn()
    print(f"\n[fix] Done — updated={updated}  skipped={skipped}")


if __name__ == "__main__":
    run_fix()
