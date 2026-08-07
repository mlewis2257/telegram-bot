"""
exit_config.py — Versioned exit strategy configurations.

Mirrors strategy_config.py for entry logic. Each ExitConfig is a frozen
dataclass that fully specifies how open positions are managed:
  • take profit levels (ordered descending)
  • trailing stop tiers (peak threshold → drawdown %)
  • hard stop percentages (standard + VIP gamble override)
  • profit floor rules (channel-specific gain protection)
  • time stop

All paper and live exit-check code consumes an ExitConfig via
apply_exit_config() rather than hardcoded constants. This makes exit
behaviour explicit, auditable, and switchable without code changes.

Live bot selects its config via the EXIT_STRATEGY env var:
    EXIT_STRATEGY=exit_live_v2

Usage:
    from exit_config import get_exit_config, apply_exit_config, ExitResult
    cfg = get_exit_config(os.getenv("EXIT_STRATEGY", "exit_live_v2"))
    result = apply_exit_config(cfg, current_mcap=..., ...)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional


# ── Result type (shared across paper and live) ────────────────────────────────

@dataclass
class ExitResult:
    should_exit: bool
    reason: Optional[str] = None
    exit_mcap: Optional[float] = None


# ── Config dataclass ──────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ExitConfig:
    """
    Fully describes an exit strategy. All fields are immutable after creation.

    take_profit_levels
        Tuple of multiplier thresholds, descending. Each fires a named TP exit.
        e.g. (10.0, 5.0) → "10x_tp" then "5x_tp".
        Label format: f"{level:g}x_tp" so 10.0 → "10x_tp", 7.5 → "7.5x_tp".

    skip_fixed_tp_for_vip_gamble
        When True, ALL fixed take-profit checks are skipped for VIP gamble/
        gamble_risk positions (they rely solely on trail stop and hard stop).
        Strategy B uses True so VIP gamble runners aren't capped at 3x.

    trail_peak_min
        Minimum peak multiplier required before the trailing stop arms.
        Must equal the threshold of the lowest tier in trail_tiers.

    trail_tiers
        Tuple of (peak_threshold, drawdown_pct) pairs, highest threshold first.
        The first tier whose peak_threshold <= current peak_mult is used.
        e.g. ((10.0, 0.12), (5.0, 0.15), (3.0, 0.22), (2.0, 0.25))

    hard_stop_pct
        Fraction loss from entry that triggers a hard stop (e.g. 0.35 = -35%).

    vip_gamble_hard_stop_pct
        Tighter hard stop for VIP gamble/gamble_risk positions (e.g. 0.30 = -30%).

    profit_floor_tiers
        Tuple of (peak_trigger_mult, floor_mult) pairs, highest trigger first.
        When peak has reached peak_trigger and current drops to floor, exit.
        e.g. ((10.0, 4.0), (5.0, 2.5), (3.0, 1.75))
        Empty tuple disables profit floor entirely.

    profit_floor_channels
        frozenset of channel handles where profit floor exits are active.
        Empty frozenset disables profit floor entirely.

    max_hours
        Time stop: close position after this many hours open.
    """
    name: str
    take_profit_levels: tuple[float, ...]
    skip_fixed_tp_for_vip_gamble: bool
    trail_peak_min: float
    trail_tiers: tuple[tuple[float, float], ...]
    hard_stop_pct: float
    vip_gamble_hard_stop_pct: float
    profit_floor_tiers: tuple[tuple[float, float], ...]
    profit_floor_channels: frozenset[str]
    max_hours: float


# ── Core exit logic ───────────────────────────────────────────────────────────

def apply_exit_config(
    config: ExitConfig,
    current_mcap: float,
    peak_mcap: float,
    entry_mcap: float,
    is_vip_gamble: bool,
    channel_handle: str,
    entry_time: Optional[datetime],
) -> ExitResult:
    """
    Apply an ExitConfig to a position's current state and return an ExitResult.

    Call this from check_exits() / check_live_exits() after fetching the
    position record — it is pure logic with no DB access.

    Parameters
    ----------
    current_mcap    Current market cap of the token.
    peak_mcap       Highest observed mcap since position open (0 if unknown).
    entry_mcap      Market cap at the time the position was opened.
    is_vip_gamble   True when position vip_tier is 'gamble' or 'gamble_risk'.
    channel_handle  Originating channel handle (used for profit floor gate).
    entry_time      UTC-aware datetime of position open (for time stop).
    """
    if entry_mcap <= 0:
        return ExitResult(False)

    current_mult = current_mcap / entry_mcap

    # ── 1. Fixed take profit ──────────────────────────────────────────────────
    # Skip all TP levels for VIP gamble when configured — let trail/hard handle.
    if not (is_vip_gamble and config.skip_fixed_tp_for_vip_gamble):
        for level in sorted(config.take_profit_levels, reverse=True):
            if current_mult >= level:
                label = f"{level:g}x_tp"
                return ExitResult(True, label, exit_mcap=current_mcap)

    # ── 2. Profit floor (channel-gated, gain protection) ─────────────────────
    if (
        config.profit_floor_tiers
        and channel_handle in config.profit_floor_channels
        and peak_mcap > 0
    ):
        peak_mult = peak_mcap / entry_mcap
        for peak_trigger, floor_mult in sorted(
            config.profit_floor_tiers, key=lambda t: t[0], reverse=True
        ):
            if peak_mult >= peak_trigger and current_mult <= floor_mult:
                # Record the real price at exit, not the theoretical floor — the
                # floor is the trigger condition, current_mcap is what we sell at.
                return ExitResult(
                    True, "profit_floor", exit_mcap=current_mcap
                )

    # ── 3. Trailing stop ──────────────────────────────────────────────────────
    if config.trail_tiers and peak_mcap > 0:
        peak_mult = peak_mcap / entry_mcap
        if peak_mult >= config.trail_peak_min:
            trail_pct: Optional[float] = None
            for peak_threshold, pct in sorted(
                config.trail_tiers, key=lambda t: t[0], reverse=True
            ):
                if peak_mult >= peak_threshold:
                    trail_pct = pct
                    break
            if trail_pct is not None:
                drawdown = (peak_mcap - current_mcap) / peak_mcap
                if drawdown >= trail_pct:
                    # Record the real price at exit, not the trail trigger level.
                    # peak_mcap * (1 - trail_pct) is the threshold that fires the
                    # stop; the actual sell happens at current_mcap, which is at
                    # or below that threshold. Recording the threshold overstated
                    # every trail exit (paper booked fake wins, live's recorded
                    # exit_price did not match the real fill).
                    return ExitResult(
                        True,
                        "trail_stop",
                        exit_mcap=current_mcap,
                    )

    # ── 4. Hard stop ──────────────────────────────────────────────────────────
    hard_stop_pct = (
        config.vip_gamble_hard_stop_pct if is_vip_gamble else config.hard_stop_pct
    )
    if current_mult <= (1.0 - hard_stop_pct):
        return ExitResult(True, "hard_stop", exit_mcap=current_mcap)

    # ── 5. Time stop ──────────────────────────────────────────────────────────
    if entry_time is not None:
        if entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=timezone.utc)
        age_hours = (
            datetime.now(timezone.utc) - entry_time
        ).total_seconds() / 3600.0
        if age_hours > config.max_hours:
            return ExitResult(True, "time_stop", exit_mcap=current_mcap)

    return ExitResult(False)


# ── Named presets ─────────────────────────────────────────────────────────────

# Paper Strategy A — lets runners breathe.
# No profit floor: A holds through retracements and trails out via tiered stop.
# Tiered trailing tightens as the token runs up to lock in gains on big moves.
EXIT_A_PAPER = ExitConfig(
    name="exit_a_paper",
    take_profit_levels=(10.0, 5.0),
    skip_fixed_tp_for_vip_gamble=False,
    trail_peak_min=2.0,
    trail_tiers=(
        (10.0, 0.12),  # 12% allowed for proven 10x+ runners
        (5.0,  0.15),
        (3.0,  0.22),
        (2.0,  0.25),
    ),
    hard_stop_pct=0.35,
    vip_gamble_hard_stop_pct=0.30,
    # Profit floor below the 2x trail-arm. The old mcap_at_call exit bug was
    # accidentally trailing positions out near their local top (~1.3-1.8x),
    # which WAS the win rate. Fixing the baseline removed that, so coins now pump
    # 1.3-1.8x and ride back to the -35% hard stop with no protection. These tiers
    # lock gains on a pullback FROM a peak — a steady climber never pulls back to
    # the floor, so true runners still reach the trail; only reversers are caught.
    profit_floor_tiers=(
        (2.0,  1.55),   # peaked 2.0x → exit if it falls back to 1.55x (~+55%)
        (1.6,  1.30),   # peaked 1.6x → exit if it falls back to 1.30x (~+30%)
        (1.35, 1.15),   # peaked 1.35x → exit if it falls back to 1.15x (~+15%)
    ),
    profit_floor_channels=frozenset({"solhousesignal", "solhousesignal_vip"}),
    max_hours=24.0,
)

# Test variant (2026-08-07): EXIT_A_PAPER with the SAME gain-protection floor turned ON for
# solwhaletrending. That lane's faders pump +30-80% then ride all the way back to the -35% hard
# stop with NO protection (it isn't in profit_floor_channels) — measured: 46% of its losers
# peaked >=+20% first. This isolates the "protect solwhaletrending's green trades" hypothesis;
# everything else is identical. Backtest/forward-test before enabling live.
from dataclasses import replace as _replace
EXIT_A_FLOOR = _replace(
    EXIT_A_PAPER,
    name="exit_a_floor",
    profit_floor_channels=EXIT_A_PAPER.profit_floor_channels | frozenset({"solwhaletrending"}),
)

# Paper Strategy B — conservative.
# 3x TP caps most positions early. Profit floor protects accumulated gains on
# solhousesignal by exiting if price retraces significantly from a peak.
# VIP gamble skips the 3x TP and relies on trail + hard stop instead.
EXIT_B_PAPER = ExitConfig(
    name="exit_b_paper",
    take_profit_levels=(3.0,),
    skip_fixed_tp_for_vip_gamble=True,
    trail_peak_min=2.0,
    trail_tiers=(
        (2.0, 0.25),   # flat 25% drawdown from peak
    ),
    hard_stop_pct=0.35,
    vip_gamble_hard_stop_pct=0.30,
    profit_floor_tiers=(
        (10.0, 4.0),   # peaked 10x → exit if back to 4x
        (5.0,  2.5),   # peaked 5x  → exit if back to 2.5x
        (3.0,  1.75),  # peaked 3x  → exit if back to 1.75x
    ),
    profit_floor_channels=frozenset({"solhousesignal"}),
    max_hours=24.0,
)

# Live v1 — current live_trader.py behaviour (preserved exactly for reference)
EXIT_LIVE_V1 = ExitConfig(
    name="exit_live_v1",
    take_profit_levels=(10.0, 5.0, 3.0),  # includes 3x TP
    skip_fixed_tp_for_vip_gamble=False,
    trail_peak_min=2.0,
    trail_tiers=(
        (5.0, 0.15),
        (3.0, 0.20),   # 20% vs paper's 22% at this tier
        (2.0, 0.25),
        # NOTE: no 10x tier — large runners trail at 15% from 5x+
    ),
    hard_stop_pct=0.35,
    vip_gamble_hard_stop_pct=0.30,
    profit_floor_tiers=(),
    profit_floor_channels=frozenset(),
    max_hours=24.0,
)

# Live v2 — aligned with paper Strategy A: lets runners breathe, no profit floor
EXIT_LIVE_V2 = ExitConfig(
    name="exit_live_v2",
    take_profit_levels=(10.0, 5.0),
    skip_fixed_tp_for_vip_gamble=False,
    trail_peak_min=2.0,
    trail_tiers=(
        (10.0, 0.12),
        (5.0,  0.15),
        (3.0,  0.22),
        (2.0,  0.25),
    ),
    hard_stop_pct=0.35,
    vip_gamble_hard_stop_pct=0.30,
    profit_floor_tiers=(),               # no floor — mirrors EXIT_A_PAPER
    profit_floor_channels=frozenset(),
    max_hours=24.0,
)

# Live v3 — conservative: aligned with paper Strategy B
# 3x TP + profit floor protects gains on retracements
EXIT_LIVE_V3 = ExitConfig(
    name="exit_live_v3",
    take_profit_levels=(10.0, 5.0, 3.0),
    skip_fixed_tp_for_vip_gamble=True,
    trail_peak_min=2.0,
    trail_tiers=(
        (2.0, 0.25),
    ),
    hard_stop_pct=0.35,
    vip_gamble_hard_stop_pct=0.30,
    profit_floor_tiers=(
        (10.0, 4.0),
        (5.0,  2.5),
        (3.0,  1.75),
    ),
    profit_floor_channels=frozenset({"solhousesignal"}),
    max_hours=24.0,
)

# Ride — experimental "let winners run" profile for the shadow experiment.
# The thesis under test: the multi-move VIP runners pay for the dumps IF you don't
# cap them early. So: NO profit floor (the early-take that capped A at +15%), no TP
# cap until 50x, and a WIDE trail that arms at 2x and gives runners room to breathe.
# The -35% hard stop still cuts the coins that never run. Compared head-to-head
# against EXIT_A_PAPER ("early") on the same calls via shadow_monitor.
EXIT_RIDE = ExitConfig(
    name="exit_ride",
    take_profit_levels=(50.0,),          # effectively uncapped — don't bail on a runner
    skip_fixed_tp_for_vip_gamble=False,
    trail_peak_min=2.0,
    trail_tiers=(
        (10.0, 0.30),   # at 10x+, allow a 30% pullback before exiting
        (5.0,  0.32),
        (3.0,  0.35),
        (2.0,  0.40),   # wide trail near 2x so chop doesn't shake us out
    ),
    hard_stop_pct=0.35,
    vip_gamble_hard_stop_pct=0.35,
    profit_floor_tiers=(),               # NO early profit-taking — the whole point
    profit_floor_channels=frozenset(),
    max_hours=48.0,                      # hold through multi-day second moves
)


# ── Registry ──────────────────────────────────────────────────────────────────

_REGISTRY: dict[str, ExitConfig] = {
    cfg.name: cfg
    for cfg in (
        EXIT_A_PAPER,
        EXIT_B_PAPER,
        EXIT_LIVE_V1,
        EXIT_LIVE_V2,
        EXIT_LIVE_V3,
        EXIT_RIDE,
    )
}

# Convenient aliases
_ALIASES: dict[str, str] = {
    "paper_a":   EXIT_A_PAPER.name,
    "paper_b":   EXIT_B_PAPER.name,
    "live_v1":   EXIT_LIVE_V1.name,
    "live_v2":   EXIT_LIVE_V2.name,
    "live_v3":   EXIT_LIVE_V3.name,
    "live":      EXIT_LIVE_V2.name,   # current recommended live default
}


def get_exit_config(name: str) -> ExitConfig:
    """
    Look up a named ExitConfig. Accepts both the full config name and aliases.
    Raises ValueError for unknown names so misconfigured envs fail fast.
    """
    key = (name or "").strip().lower()
    resolved = _ALIASES.get(key, key)
    cfg = _REGISTRY.get(resolved)
    if cfg is None:
        known = sorted(list(_REGISTRY) + list(_ALIASES))
        raise ValueError(
            f"Unknown exit config {name!r}. Known: {known}"
        )
    return cfg
