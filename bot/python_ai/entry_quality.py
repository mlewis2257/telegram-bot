from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EntryRatioDecision:
    allowed: bool
    enabled: bool
    reason: str
    executable_mcap: float | None = None
    reference_mcap: float | None = None
    reference_source: str | None = None
    ratio: float | None = None
    max_ratio: float | None = None


@dataclass(frozen=True)
class RoundtripDecision:
    allowed: bool
    enabled: bool
    reason: str
    sol_in: float | None = None
    sol_out: float | None = None
    mult: float | None = None
    min_mult: float | None = None


def env_float(raw: str | None, default: float = 0.0) -> float:
    try:
        return float((raw or "").strip() or default)
    except (TypeError, ValueError):
        return default


def trusted_reference_mcap(
    token_data: dict[str, Any] | None,
    market: dict[str, Any] | None = None,
) -> tuple[float | None, str | None]:
    token_data = token_data or {}
    market = market or {}

    msg_mcap = _positive_float(token_data.get("mcap_at_call"))
    if msg_mcap:
        return msg_mcap, "mcap_at_call"

    market_mcap = _positive_float(market.get("mcap"))
    if market_mcap:
        source = market.get("source") or market.get("price_source") or "market"
        return market_mcap, str(source)

    return None, None


def check_entry_exec_ratio(
    *,
    max_ratio: float,
    executable_mcap: float | None,
    token_data: dict[str, Any] | None,
    market: dict[str, Any] | None = None,
) -> EntryRatioDecision:
    if max_ratio <= 0:
        return EntryRatioDecision(True, False, "disabled", max_ratio=max_ratio)

    exec_mcap = _positive_float(executable_mcap)
    if not exec_mcap:
        return EntryRatioDecision(False, True, "missing_executable_entry", max_ratio=max_ratio)

    ref_mcap, ref_source = trusted_reference_mcap(token_data, market)
    if not ref_mcap:
        return EntryRatioDecision(
            True,
            True,
            "missing_reference",
            executable_mcap=exec_mcap,
            max_ratio=max_ratio,
        )

    ratio = exec_mcap / ref_mcap
    return EntryRatioDecision(
        allowed=ratio <= max_ratio,
        enabled=True,
        reason="ok" if ratio <= max_ratio else "entry_exec_ratio",
        executable_mcap=exec_mcap,
        reference_mcap=ref_mcap,
        reference_source=ref_source,
        ratio=ratio,
        max_ratio=max_ratio,
    )


def check_roundtrip(
    *,
    min_mult: float,
    sol_in: float | None,
    sol_out: float | None,
) -> RoundtripDecision:
    if min_mult <= 0:
        return RoundtripDecision(True, False, "disabled", min_mult=min_mult)

    in_sol = _positive_float(sol_in)
    if not in_sol:
        return RoundtripDecision(False, True, "missing_roundtrip_input", min_mult=min_mult)

    out_sol = _positive_float(sol_out)
    if not out_sol:
        return RoundtripDecision(
            False,
            True,
            "entry_roundtrip_no_route",
            sol_in=in_sol,
            min_mult=min_mult,
        )

    mult = out_sol / in_sol
    return RoundtripDecision(
        allowed=mult >= min_mult,
        enabled=True,
        reason="ok" if mult >= min_mult else "entry_roundtrip",
        sol_in=in_sol,
        sol_out=out_sol,
        mult=mult,
        min_mult=min_mult,
    )


def _positive_float(value: Any) -> float | None:
    try:
        out = float(value or 0)
    except (TypeError, ValueError):
        return None
    return out if out > 0 else None
