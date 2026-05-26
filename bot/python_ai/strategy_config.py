"""
strategy_config.py — Versioned strategy configuration objects.

These configs are the first step toward making strategy behavior explicit,
replayable, and auditable outside the live paper trader code paths.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class StrategyConfig:
    strategy_name: str
    version: str
    quiet_hours_pst: frozenset[int] = field(default_factory=frozenset)
    free_allowed_mcap_buckets_by_hour_pst: dict[int, frozenset[str]] = field(default_factory=dict)
    free_blocked_hours_pst: frozenset[int] = field(default_factory=frozenset)
    free_weak_30k_50k_hours_pst: frozenset[int] = field(default_factory=frozenset)
    free_min_score: float = 63.0
    free_min_entry_mcap: float = 20_000
    free_max_entry_mcap: float = 175_000
    whale_min_score: float = 70.0
    vip_generic_min_score: float | None = None
    vip_safe_allowed_hours_pst: frozenset[int] = field(default_factory=frozenset)
    vip_safe_min_score: float = 45.0
    vip_safe_min_entry_mcap: float = 20_000
    vip_safe_max_entry_mcap: float = 150_000
    vip_gamble_allowed_hours_pst: frozenset[int] = field(default_factory=frozenset)
    vip_gamble_min_score: float = 50.0
    vip_gamble_min_entry_mcap: float = 10_000
    vip_gamble_max_entry_mcap: float = 40_000
    vip_gamble_weak_15k_25k_hours_pst: frozenset[int] = field(default_factory=frozenset)


STRATEGY_A_V2026_05_22 = StrategyConfig(
    strategy_name="A",
    version="a_2026_05_23_relaxed_free",
    quiet_hours_pst=frozenset({4, 9, 14}),
    free_blocked_hours_pst=frozenset(),
    free_weak_30k_50k_hours_pst=frozenset(),
    vip_safe_allowed_hours_pst=frozenset({13, 15, 16, 17, 18, 22}),
    vip_gamble_allowed_hours_pst=frozenset({12, 15, 20, 21, 22, 23}),
    vip_gamble_weak_15k_25k_hours_pst=frozenset({8, 11, 13, 15, 16}),
)


STRATEGY_A_V2026_05_20_MATRIX = StrategyConfig(
    strategy_name="A",
    version="a_2026_05_20_matrix",
    quiet_hours_pst=frozenset({4, 9, 14}),
    free_allowed_mcap_buckets_by_hour_pst={
        2: frozenset({"30k-50k"}),
        3: frozenset({"20k-30k"}),
        5: frozenset({"30k-50k", "50k-100k"}),
        6: frozenset({"20k-30k", "50k-100k"}),
        7: frozenset({"20k-30k", "30k-50k"}),
        10: frozenset({"20k-30k", "30k-50k"}),
        11: frozenset({"50k-100k", "100k+"}),
        15: frozenset({"20k-30k"}),
        17: frozenset({"20k-30k"}),
        18: frozenset({"20k-30k", "50k-100k"}),
        22: frozenset({"50k-100k"}),
        23: frozenset({"20k-30k", "30k-50k", "50k-100k"}),
    },
    vip_safe_allowed_hours_pst=frozenset({13, 15, 16, 17, 18, 22}),
    vip_gamble_allowed_hours_pst=frozenset({12, 15, 20, 21, 22, 23}),
    vip_gamble_weak_15k_25k_hours_pst=frozenset({8, 11, 13, 15, 16}),
)


STRATEGY_A_V2026_05_22_NO_H14 = StrategyConfig(
    strategy_name="A",
    version="a_2026_05_22_no_h14",
    quiet_hours_pst=frozenset({4, 9, 14}),
    free_blocked_hours_pst=frozenset(),
    free_weak_30k_50k_hours_pst=frozenset({8, 11, 18}),
    vip_safe_allowed_hours_pst=frozenset({13, 15, 16, 17, 18, 22}),
    vip_gamble_allowed_hours_pst=frozenset({12, 15, 20, 21, 22, 23}),
    vip_gamble_weak_15k_25k_hours_pst=frozenset({8, 11, 13, 15, 16}),
)


STRATEGY_A_V2026_05_22_NO_WEAK = StrategyConfig(
    strategy_name="A",
    version="a_2026_05_22_no_weak",
    quiet_hours_pst=frozenset({4, 9, 14}),
    free_blocked_hours_pst=frozenset({14}),
    free_weak_30k_50k_hours_pst=frozenset(),
    vip_safe_allowed_hours_pst=frozenset({13, 15, 16, 17, 18, 22}),
    vip_gamble_allowed_hours_pst=frozenset({12, 15, 20, 21, 22, 23}),
    vip_gamble_weak_15k_25k_hours_pst=frozenset({8, 11, 13, 15, 16}),
)


STRATEGY_A_V2026_05_22_NO_H14_NO_WEAK = StrategyConfig(
    strategy_name="A",
    version="a_2026_05_22_no_h14_no_weak",
    quiet_hours_pst=frozenset({4, 9, 14}),
    free_blocked_hours_pst=frozenset(),
    free_weak_30k_50k_hours_pst=frozenset(),
    vip_safe_allowed_hours_pst=frozenset({13, 15, 16, 17, 18, 22}),
    vip_gamble_allowed_hours_pst=frozenset({12, 15, 20, 21, 22, 23}),
    vip_gamble_weak_15k_25k_hours_pst=frozenset({8, 11, 13, 15, 16}),
)


STRATEGY_A_V2026_05_26_VIP_TIGHT_B = StrategyConfig(
    strategy_name="A",
    version="a_2026_05_26_vip_tight_b",
    quiet_hours_pst=frozenset({4, 9, 14}),
    free_blocked_hours_pst=frozenset(),
    free_weak_30k_50k_hours_pst=frozenset(),
    vip_safe_allowed_hours_pst=frozenset({13, 15, 16, 17, 18, 22}),
    vip_safe_min_score=63.0,
    vip_gamble_allowed_hours_pst=frozenset({12, 15, 20, 21, 22, 23}),
    vip_gamble_min_score=70.0,
    vip_gamble_min_entry_mcap=15_000,
    vip_gamble_max_entry_mcap=30_000,
    vip_gamble_weak_15k_25k_hours_pst=frozenset({8, 11, 13, 15, 16}),
)


STRATEGY_B_V2026_05_22 = StrategyConfig(
    strategy_name="B",
    version="b_2026_05_22",
    quiet_hours_pst=frozenset({4, 9, 14}),
    free_min_score=63.0,
    free_min_entry_mcap=20_000,
    free_max_entry_mcap=80_000,
    vip_generic_min_score=63.0,
    vip_safe_min_entry_mcap=15_000,
    vip_safe_max_entry_mcap=150_000,
)


def get_strategy_config(strategy: str) -> StrategyConfig:
    key = strategy.strip().lower()
    if key in {"a", "a_simplified", STRATEGY_A_V2026_05_22.version}:
        return STRATEGY_A_V2026_05_22
    if key in {"a_matrix", STRATEGY_A_V2026_05_20_MATRIX.version}:
        return STRATEGY_A_V2026_05_20_MATRIX
    if key in {"a_no_h14", STRATEGY_A_V2026_05_22_NO_H14.version}:
        return STRATEGY_A_V2026_05_22_NO_H14
    if key in {"a_no_weak", STRATEGY_A_V2026_05_22_NO_WEAK.version}:
        return STRATEGY_A_V2026_05_22_NO_WEAK
    if key in {"a_no_h14_no_weak", STRATEGY_A_V2026_05_22_NO_H14_NO_WEAK.version}:
        return STRATEGY_A_V2026_05_22_NO_H14_NO_WEAK
    if key in {"a_vip_tight_b", "a_tight_vip_b", STRATEGY_A_V2026_05_26_VIP_TIGHT_B.version}:
        return STRATEGY_A_V2026_05_26_VIP_TIGHT_B
    if key in {"b", STRATEGY_B_V2026_05_22.version}:
        return STRATEGY_B_V2026_05_22
    raise ValueError(f"Unknown strategy {strategy!r}")
