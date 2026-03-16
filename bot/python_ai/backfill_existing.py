"""
backfill_existing.py — One-shot script to re-parse the 33 existing calls.

The original scraper logged raw_message and token/caller data but did not:
  - set message_type
  - set channel_id
  - set mcap_at_call (entry mcap from message)
  - write outcome result data (mcap_at_result, multipliers, outcome_label)

This script re-parses every call where message_type IS NULL using the
Type A parser and updates all missing fields in-place.

Safe to re-run — rows with message_type already set are skipped.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))

import db
from parsers import type_a


def get_unparsed_calls():
    """Fetch all calls that haven't been typed yet."""
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.id, c.raw_message, c.created_at, c.mcap_at_call
            FROM calls c
            WHERE c.message_type IS NULL
            ORDER BY c.id
            """
        )
        return cur.fetchall()


def get_channel_id(platform: str, handle: str) -> int | None:
    """Look up a channel's id by platform + handle."""
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM channels
            WHERE platform = %s AND handle = %s
            """,
            (platform, handle),
        )
        row = cur.fetchone()
        return row[0] if row else None


def update_call_meta(call_id: int, message_type: str, channel_id: int, mcap_at_call: float | None):
    """Patch message_type, channel_id, and mcap_at_call on an existing call."""
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE calls SET
                message_type  = %s,
                channel_id    = %s,
                mcap_at_call  = COALESCE(calls.mcap_at_call, %s)
            WHERE id = %s
            """,
            (message_type, channel_id, mcap_at_call, call_id),
        )
        conn.commit()


def run_backfill():
    # All existing calls came from solhousesignal (lagging)
    channel_id = get_channel_id("telegram", "solhousesignal")
    if channel_id is None:
        print("[backfill] ERROR: solhousesignal not found in channels table.")
        print("           Run migrate_v2.sql first.")
        return

    calls = get_unparsed_calls()
    print(f"[backfill] Found {len(calls)} unparsed call(s).")

    updated = skipped = failed = 0

    for (call_id, raw_message, created_at, existing_mcap) in calls:
        if not raw_message:
            print(f"  [skip] call_id={call_id} — no raw_message")
            skipped += 1
            continue

        parsed = type_a.parse(raw_message)

        if not parsed:
            # Message matched call keywords but isn't a proper lagging message.
            # Mark it so it's not retried, but leave outcome data blank.
            print(f"  [skip] call_id={call_id} — type_a.parse returned None")
            update_call_meta(call_id, "lagging_call", channel_id, None)
            skipped += 1
            continue

        # Update calls row
        update_call_meta(
            call_id=call_id,
            message_type="lagging_call",
            channel_id=channel_id,
            mcap_at_call=parsed["mcap_at_call"],
        )

        # Update outcomes row with result data
        if parsed["mcap_at_result"] is not None:
            db.update_outcome_from_lagging(
                call_id=call_id,
                mcap_at_result=parsed["mcap_at_result"],
                result_reported_at=created_at,
                stated_multiplier=parsed["stated_multiplier"],
                peak_multiplier=parsed["peak_multiplier"],
                outcome_label="runner",
            )

        print(
            f"  [updated] call_id={call_id} "
            f"entry={parsed['mcap_at_call']} "
            f"result={parsed['mcap_at_result']} "
            f"computed={parsed['peak_multiplier']}x "
            f"stated={parsed['stated_multiplier']}x"
        )
        updated += 1

    db.close_conn()
    print(f"\n[backfill] Done — updated={updated}  skipped={skipped}  failed={failed}")


if __name__ == "__main__":
    run_backfill()
