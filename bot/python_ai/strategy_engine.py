"""
strategy_engine.py — Shared entry decision helpers for strategy replay.

This module is intentionally narrower than the live paper trader today.
It captures the highest-signal deterministic entry gates so they can be
replayed against historical calls without requiring live network fetches.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from strategy_config import StrategyConfig


@dataclass(frozen=True)
class StrategyCallContext:
    call_id: int | None = None
    strategy_name: str = "A"
    channel_handle: str = ""
    vip_tier: str | None = None
    score: float = 0.0
    local_hour_pst: int = 0
    entry_mcap: float = 0.0
    bundle_pct: float | None = None
    fake_pct: float | None = None
    security_flag: str | None = None
    dev_tokens_made: int | None = None
    symbol: str | None = None


@dataclass(frozen=True)
class StrategyDecision:
    should_trade: bool
    reason: str
    strategy_version: str
    metadata: dict[str, Any] = field(default_factory=dict)


def classify_mcap_bucket(entry_mcap: float) -> str | None:
    if entry_mcap <= 0:
        return None
    if entry_mcap < 20_000:
        return "under_20k"
    if entry_mcap < 30_000:
        return "20k-30k"
    if entry_mcap < 50_000:
        return "30k-50k"
    if entry_mcap < 100_000:
        return "50k-100k"
    return "100k+"


def evaluate_strategy_a_entry(
    ctx: StrategyCallContext,
    config: StrategyConfig,
) -> StrategyDecision:
    channel = (ctx.channel_handle or "").lstrip("@")
    bucket = classify_mcap_bucket(ctx.entry_mcap)
    metadata = {
        "channel_handle": channel,
        "vip_tier": ctx.vip_tier,
        "hour_pst": ctx.local_hour_pst,
        "entry_mcap": ctx.entry_mcap,
        "mcap_bucket": bucket,
        "score": ctx.score,
    }

    if ctx.security_flag == "warning":
        return StrategyDecision(False, "security_warning", config.version, metadata)

    if channel == "solhousesignal":
        if ctx.score < config.free_min_score:
            return StrategyDecision(False, "low_score", config.version, metadata)
        if ctx.entry_mcap < config.free_min_entry_mcap:
            return StrategyDecision(False, "mcap_too_low", config.version, metadata)
        if ctx.entry_mcap > config.free_max_entry_mcap:
            return StrategyDecision(False, "mcap_too_high", config.version, metadata)
        if config.free_allowed_mcap_buckets_by_hour_pst:
            allowed_buckets = config.free_allowed_mcap_buckets_by_hour_pst.get(ctx.local_hour_pst)
            if allowed_buckets is None or bucket not in allowed_buckets:
                return StrategyDecision(False, "free_allowed_bucket", config.version, metadata)
        if ctx.local_hour_pst in config.free_blocked_hours_pst:
            return StrategyDecision(False, "free_blocked_hour", config.version, metadata)
        if bucket == "30k-50k" and ctx.local_hour_pst in config.free_weak_30k_50k_hours_pst:
            return StrategyDecision(False, "free_weak_pocket", config.version, metadata)
        if (
            ctx.bundle_pct is not None
            and ctx.bundle_pct >= 10
            and ctx.fake_pct is not None
            and ctx.fake_pct >= 5
        ):
            return StrategyDecision(False, "low_quality_bucket", config.version, metadata)
        return StrategyDecision(True, "trade", config.version, metadata)

    if channel == "solhousesignal_vip":
        if ctx.vip_tier not in {"safe", "gamble", "gamble_risk"}:
            reason = "vip_missing_tier" if not ctx.vip_tier else "vip_unhandled_tier"
            return StrategyDecision(False, reason, config.version, metadata)

        if ctx.vip_tier == "safe":
            if ctx.local_hour_pst not in config.vip_safe_allowed_hours_pst:
                return StrategyDecision(False, "vip_safe_allowed_hours", config.version, metadata)
            if ctx.score < config.vip_safe_min_score:
                return StrategyDecision(False, "vip_low_score", config.version, metadata)
            if ctx.entry_mcap < config.vip_safe_min_entry_mcap:
                return StrategyDecision(False, "vip_mcap_too_low", config.version, metadata)
            if ctx.entry_mcap > config.vip_safe_max_entry_mcap:
                return StrategyDecision(False, "mcap_too_high", config.version, metadata)
            if ctx.bundle_pct is not None and ctx.bundle_pct > 10:
                return StrategyDecision(False, "high_bundle", config.version, metadata)
            if ctx.fake_pct is not None and ctx.fake_pct > 4:
                return StrategyDecision(False, "high_fake_vol", config.version, metadata)
            return StrategyDecision(True, "trade", config.version, metadata)

        if ctx.vip_tier == "gamble":
            if ctx.local_hour_pst not in config.vip_gamble_allowed_hours_pst:
                return StrategyDecision(False, "vip_gamble_allowed_hours", config.version, metadata)
            if ctx.score < config.vip_gamble_min_score:
                return StrategyDecision(False, "vip_low_score", config.version, metadata)
            if ctx.entry_mcap < config.vip_gamble_min_entry_mcap:
                return StrategyDecision(False, "mcap_too_low", config.version, metadata)
            if ctx.entry_mcap >= config.vip_gamble_max_entry_mcap:
                return StrategyDecision(False, "vip_mcap_gate", config.version, metadata)
            if (
                15_000 <= ctx.entry_mcap < 25_000
                and ctx.local_hour_pst in config.vip_gamble_weak_15k_25k_hours_pst
            ):
                return StrategyDecision(False, "vip_gamble_weak_pocket", config.version, metadata)
            if (
                ctx.bundle_pct is not None
                and ctx.bundle_pct >= 10
                and ctx.fake_pct is not None
                and ctx.fake_pct >= 5
            ):
                return StrategyDecision(False, "vip_mcap_gate", config.version, metadata)
            return StrategyDecision(True, "trade", config.version, metadata)

        return StrategyDecision(False, "vip_paused", config.version, metadata)

    return StrategyDecision(False, "unsupported_channel", config.version, metadata)


def evaluate_strategy_b_entry(
    ctx: StrategyCallContext,
    config: StrategyConfig,
) -> StrategyDecision:
    channel = (ctx.channel_handle or "").lstrip("@")
    bucket = classify_mcap_bucket(ctx.entry_mcap)
    metadata = {
        "channel_handle": channel,
        "vip_tier": ctx.vip_tier,
        "hour_pst": ctx.local_hour_pst,
        "entry_mcap": ctx.entry_mcap,
        "mcap_bucket": bucket,
        "score": ctx.score,
    }

    if ctx.security_flag == "warning":
        return StrategyDecision(False, "security_warning", config.version, metadata)

    if ctx.local_hour_pst in config.quiet_hours_pst:
        return StrategyDecision(False, "quiet_hours", config.version, metadata)

    if channel == "solhousesignal":
        if ctx.score < config.free_min_score:
            return StrategyDecision(False, "low_score", config.version, metadata)
        if ctx.entry_mcap < config.free_min_entry_mcap:
            return StrategyDecision(False, "mcap_too_low", config.version, metadata)
        if ctx.entry_mcap > config.free_max_entry_mcap:
            return StrategyDecision(False, "mcap_too_high", config.version, metadata)
        if (
            ctx.bundle_pct is not None
            and ctx.bundle_pct >= 10
            and ctx.fake_pct is not None
            and ctx.fake_pct >= 5
        ):
            return StrategyDecision(False, "low_quality_bucket", config.version, metadata)
        return StrategyDecision(True, "trade", config.version, metadata)

    if channel == "solwhaletrending":
        if ctx.score < config.free_min_score:
            return StrategyDecision(False, "low_score", config.version, metadata)
        return StrategyDecision(True, "trade", config.version, metadata)

    if channel == "solhousesignal_vip":
        if ctx.vip_tier not in {"safe", "gamble", "gamble_risk"}:
            reason = "vip_missing_tier" if not ctx.vip_tier else "vip_unhandled_tier"
            return StrategyDecision(False, reason, config.version, metadata)

        if ctx.vip_tier in {"gamble", "gamble_risk"}:
            return StrategyDecision(False, "vip_paused", config.version, metadata)

        min_score = config.vip_generic_min_score or config.vip_safe_min_score
        if ctx.score < min_score:
            return StrategyDecision(False, "vip_low_score", config.version, metadata)
        if ctx.entry_mcap < config.vip_safe_min_entry_mcap:
            return StrategyDecision(False, "vip_mcap_too_low", config.version, metadata)
        if ctx.entry_mcap > config.vip_safe_max_entry_mcap:
            return StrategyDecision(False, "mcap_too_high", config.version, metadata)
        return StrategyDecision(True, "trade", config.version, metadata)

    return StrategyDecision(False, "unsupported_channel", config.version, metadata)
