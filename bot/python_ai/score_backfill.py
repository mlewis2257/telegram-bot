"""
score_backfill.py — One-shot conviction scorer for existing unscored calls.

Finds all calls where conviction_score IS NULL, runs scorer.score_call()
on each, and writes the result back to the DB.

Usage:
    python3 score_backfill.py
"""

import sys
import os
import time

sys.path.insert(0, os.path.dirname(__file__))

import db
import scorer

SLEEP_BETWEEN_CALLS = 0.1   # seconds — avoids hammering DexScreener (lagging path)


def get_unscored_calls() -> list[tuple[int, str]]:
    """Return [(call_id, symbol), ...] for all calls with conviction_score IS NULL."""
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.id, COALESCE(t.symbol, '?')
            FROM calls c
            JOIN tokens t ON t.id = c.token_id
            WHERE c.conviction_score IS NULL
            ORDER BY c.id ASC
            """
        )
        return cur.fetchall()


def main() -> None:
    rows = get_unscored_calls()
    print(f"[score_backfill] {len(rows)} call(s) to score")

    scored = skipped = errors = 0
    label_counts = {
        "strong_alert": 0,
        "alert":        0,
        "caution":      0,
        "watch":        0,
        "skip":         0,
    }

    for call_id, symbol in rows:
        try:
            result = scorer.score_call(call_id)
        except Exception as exc:
            print(f"  [skip] call_id={call_id} — scorer error: {exc}")
            errors += 1
            continue

        if not result:
            print(f"  [skip] call_id={call_id} — no result returned")
            skipped += 1
            continue

        label = result.get("label", "skip")
        score = result.get("score", 0)
        print(
            f"  [score_backfill] {symbol:<14} call_id={call_id}"
            f"  score={score:<3}  label={label}"
        )

        label_counts[label] = label_counts.get(label, 0) + 1
        scored += 1

        time.sleep(SLEEP_BETWEEN_CALLS)

    db.close_conn()

    print(f"\n[done] scored={scored}  skipped={skipped}  errors={errors}")
    print("\nScore distribution:")
    print(f"  strong_alert (90+): {label_counts.get('strong_alert', 0)}")
    print(f"  alert (75-89):      {label_counts.get('alert', 0)}")
    print(f"  caution (60-74):    {label_counts.get('caution', 0)}")
    print(f"  watch (40-59):      {label_counts.get('watch', 0)}")
    print(f"  skip (<40):         {label_counts.get('skip', 0)}")


if __name__ == "__main__":
    main()
