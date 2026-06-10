"""
shadow_trader.py — open shadow paper trades for normally-skipped VIP lanes.

Purpose: measure the TRUE tradeable PnL of lanes the main strategy gates out
(e.g. gamble / gamble_risk), using a realistic entry (Jupiter price at trade time)
and the real exit engine — without polluting the main A/B numbers. Shadow positions
are flagged is_shadow=TRUE and managed entirely by shadow_monitor.py.

OFF by default. Enable per lane via env:
    SHADOW_LANES=safe,gamble,gamble_risk # which vip_tiers to shadow-trade
    SHADOW_SOL_IN=0.5                     # nominal size

This module only OPENS positions; it never touches the main exit path.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import db
import data_fetcher

SHADOW_LANES = {l.strip() for l in os.getenv("SHADOW_LANES", "").split(",") if l.strip()}
SHADOW_SOL_IN = float(os.getenv("SHADOW_SOL_IN", "0.5"))
# Which exit profiles to shadow-trade per call, head-to-head on the same coins.
# Each must map to an exit_config in shadow_monitor._VARIANT_CONFIGS.
SHADOW_VARIANTS = [v.strip() for v in os.getenv("SHADOW_VARIANTS", "early,ride").split(",") if v.strip()]

_table_ready = False


def enabled() -> bool:
    return bool(SHADOW_LANES)


def _ensure_table_once() -> None:
    global _table_ready
    if not _table_ready:
        db.ensure_shadow_positions_table()
        _table_ready = True


async def maybe_open_shadow(score_result: dict, token_data: dict) -> None:
    """
    Open a shadow position if this call's vip_tier is in SHADOW_LANES.
    Never raises — must not affect the main pipeline.
    """
    try:
        if not SHADOW_LANES:
            return
        call_id = score_result.get("call_id") if score_result else None
        vip_tier = (token_data or {}).get("vip_tier")
        mint = (token_data or {}).get("mint_address")
        symbol = (token_data or {}).get("symbol", "?")

        if not call_id or not vip_tier or vip_tier not in SHADOW_LANES:
            return
        if not mint or mint.startswith(("INFERRED:", "UNKNOWN:")):
            return

        _ensure_table_once()

        # Real entry on the SAME feed used for monitoring/exit (consistent ruler).
        entry = None
        try:
            market = data_fetcher.fetch_token_price_fast(mint)
            if market and market.get("mcap"):
                entry = float(market["mcap"])
        except Exception as e:
            print(f"[shadow] entry price fetch failed for {symbol}: {e}")
        if not entry or entry <= 0:
            entry = float(token_data.get("mcap_at_call") or 0)
        if entry <= 0:
            return

        opened = []
        for variant in SHADOW_VARIANTS:
            if db.open_shadow_position(call_id, entry, SHADOW_SOL_IN, vip_tier, exit_variant=variant):
                opened.append(variant)
        if opened:
            print(f"[shadow] opened {symbol} call_id={call_id} tier={vip_tier} "
                  f"variants={','.join(opened)} @ ${entry/1000:.1f}k")
    except Exception as e:
        print(f"[shadow] maybe_open_shadow error: {e}")
