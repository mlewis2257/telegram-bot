"""
ml_dataset_profile.py — Quick profile for exported ML datasets.

Usage:
    python3 ml_dataset_profile.py exports/ml_dataset_20260530_000528.csv
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter


TARGET_COLUMNS = [
    "label_signal_peak_ge_2x",
    "label_signal_peak_ge_5x",
    "label_signal_peak_lt_1x",
    "label_a_pnl_positive",
    "label_a_hard_stop",
    "label_a_traded",
    "label_b_pnl_positive",
    "label_b_hard_stop",
    "label_b_traded",
]

KEY_FEATURES = [
    "conviction_score",
    "channel_handle",
    "channel_type",
    "vip_tier",
    "skip_reason",
    "mcap_at_call",
    "security_flag",
    "token_age_minutes",
    "bundle_pct_remaining",
    "liq_at_detection",
    "hodl_count",
    "fake_vol_pct",
    "dev_tokens_made",
    "vol_1h_at_detection",
    "snap_t0_volume_h1",
    "snap_t30_volume_h1",
    "snap_t60_volume_h1",
    "snap_t30_volume_over_t0",
    "snap_t60_volume_over_t0",
    "ws_obs_count",
    "ws_helius_obs_count",
    "ws_first_mcap_over_entry",
    "ws_last_mcap_over_entry",
    "ws_min_mcap_over_entry",
    "ws_max_mcap_over_entry",
    "ws_drawdown_from_max",
    "ws_last_drawdown_from_max",
    "ws_last_mcap_1m_over_entry",
    "ws_last_mcap_5m_over_entry",
    "ws_last_mcap_15m_over_entry",
    "feature_has_ws_observations",
]


def _is_missing(value: str | None) -> bool:
    return value in (None, "")


def _score_bucket(raw: str | None) -> str:
    if _is_missing(raw):
        return "missing"
    score = float(raw)
    if score >= 85:
        return "85+"
    if score >= 75:
        return "75-84"
    if score >= 70:
        return "70-74"
    if score >= 63:
        return "63-69"
    return "<63"


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile an exported ML dataset CSV")
    parser.add_argument("path", help="Path to exported CSV")
    parser.add_argument("--top-n", type=int, default=10, help="Top N values to show in distributions")
    args = parser.parse_args()

    with open(args.path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []

        rows = 0
        missing_counts = Counter()
        channel_counts = Counter()
        score_bucket_counts = Counter()
        vip_counts = Counter()
        skip_counts = Counter()
        target_counts = {name: Counter() for name in TARGET_COLUMNS if name in fieldnames}

        for row in reader:
            rows += 1
            channel_counts[row.get("channel_handle") or "missing"] += 1
            score_bucket_counts[_score_bucket(row.get("conviction_score"))] += 1
            vip_counts[row.get("vip_tier") or "none"] += 1
            skip_counts[row.get("skip_reason") or "none"] += 1

            for name in fieldnames:
                if _is_missing(row.get(name)):
                    missing_counts[name] += 1

            for target in target_counts:
                value = row.get(target)
                if _is_missing(value):
                    target_counts[target]["missing"] += 1
                elif value in ("0", "1"):
                    target_counts[target][value] += 1
                else:
                    target_counts[target]["other"] += 1

    print("=" * 72)
    print("ML Dataset Profile")
    print("=" * 72)
    print(f"rows={rows}")
    print(f"columns={len(fieldnames)}")

    print("\nTarget balance")
    print("-" * 72)
    for target, counts in target_counts.items():
        pos = counts.get("1", 0)
        neg = counts.get("0", 0)
        miss = counts.get("missing", 0)
        pos_rate = (pos * 100.0 / (pos + neg)) if (pos + neg) else 0.0
        print(f"{target:<28} pos={pos:>6} neg={neg:>6} missing={miss:>6} pos_rate={pos_rate:>6.1f}%")

    print("\nChannel mix")
    print("-" * 72)
    for key, count in channel_counts.most_common(args.top_n):
        print(f"{key:<24} {count:>8}")

    print("\nScore bucket mix")
    print("-" * 72)
    for bucket in ("85+", "75-84", "70-74", "63-69", "<63", "missing"):
        if bucket in score_bucket_counts:
            print(f"{bucket:<24} {score_bucket_counts[bucket]:>8}")

    print("\nVIP tier mix")
    print("-" * 72)
    for key, count in vip_counts.most_common(args.top_n):
        print(f"{key:<24} {count:>8}")

    print("\nSkip reason mix")
    print("-" * 72)
    for key, count in skip_counts.most_common(args.top_n):
        print(f"{key:<24} {count:>8}")

    print("\nKey feature missingness")
    print("-" * 72)
    for name in KEY_FEATURES:
        if name not in fieldnames:
            continue
        missing = missing_counts.get(name, 0)
        pct = (missing * 100.0 / rows) if rows else 0.0
        print(f"{name:<28} missing={missing:>6} ({pct:>5.1f}%)")


if __name__ == "__main__":
    main()
