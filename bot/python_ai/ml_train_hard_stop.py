"""
ml_train_hard_stop.py — Lightweight offline baseline for predicting
Strategy A hard stops from exported ML datasets.

This intentionally avoids heavyweight dependencies so we can iterate on the
first model shape using the standard library only.

Current target:
    label_a_hard_stop

Current intended cohort:
    ml_dataset_export.py --subset a_traded

Usage:
    python3 ml_train_hard_stop.py exports/ml_dataset_20260530_005300.csv
    python3 ml_train_hard_stop.py exports/file.csv --epochs 30 --learning-rate 0.03
    python3 ml_train_hard_stop.py exports/file.csv --drop-categorical skip_reason
    python3 ml_train_hard_stop.py exports/file.csv --preset no_leak
    python3 ml_train_hard_stop.py exports/file.csv --preset no_leak --threshold-sweep
"""

from __future__ import annotations

import argparse
import csv
import math
import random
from dataclasses import dataclass


NUMERIC_FEATURES = [
    "conviction_score",
    "mcap_at_call",
    "local_hour_pst",
    "local_dow_pst",
]

CATEGORICAL_FEATURES = [
    "channel_handle",
    "channel_type",
    "vip_tier",
    "message_type",
    "vip_message_type",
    "skip_reason",
]

TARGET_COLUMN = "label_a_hard_stop"
ROW_ID_COLUMN = "call_id"
TIME_COLUMN = "created_at"
PRESET_DROPS = {
    "none": set(),
    "no_skip_reason": {"skip_reason"},
    "no_leak": {"skip_reason", "vip_message_type"},
}


@dataclass
class Example:
    row_id: str
    created_at: str
    label: int
    features: dict[str, float]


def _safe_float(value: str | None) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def _log1p_or_zero(value: float | None) -> float:
    if value is None or value <= 0:
        return 0.0
    return math.log1p(value)


def _build_features(
    row: dict[str, str],
    *,
    drop_numeric: set[str],
    drop_categorical: set[str],
) -> dict[str, float]:
    feats: dict[str, float] = {}

    score = _safe_float(row.get("conviction_score"))
    if "conviction_score" not in drop_numeric:
        if score is not None:
            feats["num:conviction_score"] = score / 100.0
        else:
            feats["missing:conviction_score"] = 1.0

    mcap = _safe_float(row.get("mcap_at_call"))
    if "mcap_at_call" not in drop_numeric:
        if mcap is not None:
            feats["num:log_mcap_at_call"] = _log1p_or_zero(mcap)
        else:
            feats["missing:mcap_at_call"] = 1.0

    local_hour = _safe_float(row.get("local_hour_pst"))
    if "local_hour_pst" not in drop_numeric:
        if local_hour is not None:
            # cyclical encoding
            angle = 2.0 * math.pi * (local_hour / 24.0)
            feats["num:hour_sin"] = math.sin(angle)
            feats["num:hour_cos"] = math.cos(angle)
        else:
            feats["missing:local_hour_pst"] = 1.0

    local_dow = _safe_float(row.get("local_dow_pst"))
    if "local_dow_pst" not in drop_numeric:
        if local_dow is not None:
            angle = 2.0 * math.pi * (local_dow / 7.0)
            feats["num:dow_sin"] = math.sin(angle)
            feats["num:dow_cos"] = math.cos(angle)
        else:
            feats["missing:local_dow_pst"] = 1.0

    for name in CATEGORICAL_FEATURES:
        if name in drop_categorical:
            continue
        value = (row.get(name) or "").strip()
        if value == "":
            value = "__missing__"
        feats[f"cat:{name}={value}"] = 1.0

    return feats


def _load_examples(
    path: str,
    *,
    drop_numeric: set[str],
    drop_categorical: set[str],
) -> list[Example]:
    examples: list[Example] = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            label_raw = row.get(TARGET_COLUMN)
            if label_raw not in ("0", "1"):
                continue
            features = _build_features(
                row,
                drop_numeric=drop_numeric,
                drop_categorical=drop_categorical,
            )
            examples.append(
                Example(
                    row_id=row.get(ROW_ID_COLUMN) or "",
                    created_at=row.get(TIME_COLUMN) or "",
                    label=int(label_raw),
                    features=features,
                )
            )
    examples.sort(key=lambda ex: (ex.created_at, ex.row_id))
    return examples


def _split_examples(examples: list[Example], test_ratio: float) -> tuple[list[Example], list[Example]]:
    if not examples:
        return [], []
    split_idx = int(len(examples) * (1.0 - test_ratio))
    split_idx = max(1, min(len(examples) - 1, split_idx))
    return examples[:split_idx], examples[split_idx:]


def _dot(weights: dict[str, float], feats: dict[str, float]) -> float:
    return sum(weights.get(name, 0.0) * value for name, value in feats.items())


def _train_logreg(
    train_rows: list[Example],
    *,
    epochs: int,
    learning_rate: float,
    l2: float,
    seed: int,
) -> tuple[dict[str, float], float]:
    rng = random.Random(seed)
    weights: dict[str, float] = {}
    bias = 0.0

    rows = list(train_rows)
    for _epoch in range(epochs):
        rng.shuffle(rows)
        for ex in rows:
            z = bias + _dot(weights, ex.features)
            pred = _sigmoid(z)
            err = pred - ex.label

            bias -= learning_rate * err
            for name, value in ex.features.items():
                old_w = weights.get(name, 0.0)
                grad = err * value + l2 * old_w
                weights[name] = old_w - learning_rate * grad

    return weights, bias


def _predict_prob(weights: dict[str, float], bias: float, ex: Example) -> float:
    return _sigmoid(bias + _dot(weights, ex.features))


def _roc_auc(labels_and_scores: list[tuple[int, float]]) -> float:
    if not labels_and_scores:
        return 0.0
    positives = sum(label for label, _ in labels_and_scores)
    negatives = len(labels_and_scores) - positives
    if positives == 0 or negatives == 0:
        return 0.0

    ranked = sorted(labels_and_scores, key=lambda item: item[1])
    rank_sum = 0.0
    rank = 1
    i = 0
    while i < len(ranked):
        j = i
        while j + 1 < len(ranked) and ranked[j + 1][1] == ranked[i][1]:
            j += 1
        avg_rank = (rank + rank + (j - i)) / 2.0
        positive_in_group = sum(label for label, _ in ranked[i : j + 1])
        rank_sum += positive_in_group * avg_rank
        rank += (j - i + 1)
        i = j + 1

    return (rank_sum - positives * (positives + 1) / 2.0) / (positives * negatives)


def _classification_metrics(labels_and_scores: list[tuple[int, float]], threshold: float) -> dict[str, float]:
    tp = fp = tn = fn = 0
    for label, score in labels_and_scores:
        pred = 1 if score >= threshold else 0
        if pred == 1 and label == 1:
            tp += 1
        elif pred == 1 and label == 0:
            fp += 1
        elif pred == 0 and label == 0:
            tn += 1
        else:
            fn += 1

    total = tp + fp + tn + fn
    accuracy = (tp + tn) / total if total else 0.0
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) else 0.0
    return {
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "f1": f1,
    }


def _print_threshold_sweep(labels_and_scores: list[tuple[int, float]], thresholds: list[float]) -> None:
    print()
    print("Threshold sweep on test set")
    print("-" * 72)
    print(f"{'thr':>5} {'pred+':>6} {'prec':>7} {'recall':>7} {'f1':>7} {'fp':>5} {'fn':>5}")
    for threshold in thresholds:
        metrics = _classification_metrics(labels_and_scores, threshold)
        pred_pos = metrics["tp"] + metrics["fp"]
        print(
            f"{threshold:>5.2f} {pred_pos:>6} "
            f"{metrics['precision']:>7.3f} {metrics['recall']:>7.3f} "
            f"{metrics['f1']:>7.3f} {metrics['fp']:>5} {metrics['fn']:>5}"
        )


def _write_predictions(
    path: str,
    test_rows: list[Example],
    labels_and_scores: list[tuple[int, float]],
) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["call_id", "created_at", "label_a_hard_stop", "pred_hard_stop_prob"])
        for ex, (label, score) in zip(test_rows, labels_and_scores):
            writer.writerow([ex.row_id, ex.created_at, label, f"{score:.6f}"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Train a lightweight hard-stop baseline on exported ML data")
    parser.add_argument("path", help="Path to exported CSV dataset")
    parser.add_argument("--epochs", type=int, default=25)
    parser.add_argument("--learning-rate", type=float, default=0.03)
    parser.add_argument("--l2", type=float, default=0.0005)
    parser.add_argument("--test-ratio", type=float, default=0.2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--threshold-sweep", action="store_true", help="Print test-set metrics across thresholds")
    parser.add_argument("--top-features", type=int, default=12)
    parser.add_argument("--predictions-out", default=None, help="Optional CSV path for test-set predictions")
    parser.add_argument(
        "--preset",
        choices=sorted(PRESET_DROPS.keys()),
        default="none",
        help="Predefined feature drops for leak checks",
    )
    parser.add_argument(
        "--drop-categorical",
        action="append",
        default=[],
        choices=CATEGORICAL_FEATURES,
        help="Categorical feature name to exclude; repeatable",
    )
    parser.add_argument(
        "--drop-numeric",
        action="append",
        default=[],
        choices=NUMERIC_FEATURES,
        help="Numeric feature name to exclude; repeatable",
    )
    args = parser.parse_args()

    drop_categorical = set(args.drop_categorical) | PRESET_DROPS[args.preset]
    drop_numeric = set(args.drop_numeric)

    examples = _load_examples(
        args.path,
        drop_numeric=drop_numeric,
        drop_categorical=drop_categorical,
    )
    train_rows, test_rows = _split_examples(examples, args.test_ratio)
    weights, bias = _train_logreg(
        train_rows,
        epochs=args.epochs,
        learning_rate=args.learning_rate,
        l2=args.l2,
        seed=args.seed,
    )

    train_scores = [(ex.label, _predict_prob(weights, bias, ex)) for ex in train_rows]
    test_scores = [(ex.label, _predict_prob(weights, bias, ex)) for ex in test_rows]

    train_auc = _roc_auc(train_scores)
    test_auc = _roc_auc(test_scores)
    train_metrics = _classification_metrics(train_scores, args.threshold)
    test_metrics = _classification_metrics(test_scores, args.threshold)

    sorted_weights = sorted(weights.items(), key=lambda item: item[1], reverse=True)
    top_positive = sorted_weights[: args.top_features]
    top_negative = sorted(weights.items(), key=lambda item: item[1])[: args.top_features]

    print("=" * 72)
    print("ML Hard Stop Baseline")
    print("=" * 72)
    print(f"rows={len(examples)}  train={len(train_rows)}  test={len(test_rows)}")
    print(f"target={TARGET_COLUMN}")
    print(f"epochs={args.epochs}  learning_rate={args.learning_rate}  l2={args.l2}")
    print(f"threshold={args.threshold}")
    print(f"preset={args.preset}")
    print(f"drop_categorical={sorted(drop_categorical) if drop_categorical else []}")
    print(f"drop_numeric={sorted(drop_numeric) if drop_numeric else []}")
    print(f"train_positive_rate={sum(label for label, _ in train_scores) * 100.0 / len(train_scores):.1f}%")
    print(f"test_positive_rate={sum(label for label, _ in test_scores) * 100.0 / len(test_scores):.1f}%")
    print()
    print("Train metrics")
    print("-" * 72)
    print(f"auc={train_auc:.4f}  accuracy={train_metrics['accuracy']:.4f}  precision={train_metrics['precision']:.4f}  recall={train_metrics['recall']:.4f}  f1={train_metrics['f1']:.4f}")
    print(f"tp={train_metrics['tp']}  fp={train_metrics['fp']}  tn={train_metrics['tn']}  fn={train_metrics['fn']}")
    print()
    print("Test metrics")
    print("-" * 72)
    print(f"auc={test_auc:.4f}  accuracy={test_metrics['accuracy']:.4f}  precision={test_metrics['precision']:.4f}  recall={test_metrics['recall']:.4f}  f1={test_metrics['f1']:.4f}")
    print(f"tp={test_metrics['tp']}  fp={test_metrics['fp']}  tn={test_metrics['tn']}  fn={test_metrics['fn']}")
    print()
    print("Top positive features (more hard-stop risk)")
    print("-" * 72)
    for name, weight in top_positive:
        print(f"{name:<40} {weight:>10.4f}")
    print()
    print("Top negative features (less hard-stop risk)")
    print("-" * 72)
    for name, weight in top_negative:
        print(f"{name:<40} {weight:>10.4f}")

    if args.threshold_sweep:
        _print_threshold_sweep(
            test_scores,
            thresholds=[0.20, 0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70],
        )

    if args.predictions_out:
        _write_predictions(args.predictions_out, test_rows, test_scores)
        print()
        print(f"predictions_out={args.predictions_out}")


if __name__ == "__main__":
    main()
