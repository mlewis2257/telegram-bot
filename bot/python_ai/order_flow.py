"""
order_flow.py — live buy/sell order-flow from on-chain swaps.

`ws_monitor` already fetches each swap's transaction (to derive price). This module
extracts the ORDER FLOW from that same transaction — direction (buy/sell), size, and
the trader wallet — and keeps rolling per-mint state so the bot can read "is this
move backed by real buying, or rolling over?" in real time, the way a trader reads a
live chart. No vision model, no new fetch — just parse what we already pull.

Pure in-memory + stdlib. parse_swap is a pure function (unit-testable); the rolling
state is module-level, keyed by mint.

NOTE on completeness: ws_monitor samples (1 fetch per mint per FETCH_COOLDOWN), so
this sees a SAMPLE of swaps, not every one. Good enough for a directional pressure
read; for complete flow you'd lower the cooldown (more RPC) or move to gRPC streaming.
"""

from __future__ import annotations

import time
from collections import deque

WSOL_MINT = "So11111111111111111111111111111111111111112"


def parse_swap(tx: dict, mint: str) -> dict | None:
    """
    Extract {side, sol_size, wallet, token_amount, ts} from a getTransaction result,
    or None if it isn't a parseable swap of `mint` by the fee payer.

    Direction: the fee payer (accountKeys[0]) is the trader; if their balance of
    `mint` went UP it's a buy, DOWN a sell. Size: their net native-SOL movement
    (approximate — fine for relative pressure).
    """
    try:
        meta = tx.get("meta") or {}
        msg = (tx.get("transaction") or {}).get("message") or {}
        keys = msg.get("accountKeys") or []
        if not keys:
            return None
        trader = keys[0]
        if isinstance(trader, dict):           # jsonParsed encoding
            trader = trader.get("pubkey")
        if not trader:
            return None

        pre = meta.get("preTokenBalances") or []
        post = meta.get("postTokenBalances") or []

        def owner_bal(balances: list, owner: str, m: str) -> float:
            tot = 0.0
            for b in balances:
                if b.get("mint") == m and b.get("owner") == owner:
                    ua = (b.get("uiTokenAmount") or {}).get("uiAmount")
                    try:
                        tot += float(ua or 0)
                    except Exception:
                        pass
            return tot

        tok_delta = owner_bal(post, trader, mint) - owner_bal(pre, trader, mint)
        if abs(tok_delta) < 1e-9:
            return None                         # trader's mint balance didn't change
        side = "buy" if tok_delta > 0 else "sell"

        # Size in SOL. Prefer the trader's WSOL balance change (covers wrapped-SOL
        # swaps); fall back to net native-SOL movement (fee-adjusted) for unwrapped.
        wsol_delta = abs(owner_bal(post, trader, WSOL_MINT) - owner_bal(pre, trader, WSOL_MINT))
        sol_size = wsol_delta if wsol_delta > 1e-6 else None
        if sol_size is None:
            pre_l = meta.get("preBalances") or []
            post_l = meta.get("postBalances") or []
            if pre_l and post_l:
                fee = float(meta.get("fee") or 0) / 1e9
                native_delta = abs((post_l[0] - pre_l[0]) / 1e9)
                sol_size = max(native_delta - fee, 0.0) if side == "buy" else native_delta

        # A swap MUST exchange SOL. ~0 SOL with a token balance change is a transfer
        # or dust, not a trade — exclude it so it doesn't pollute buy/sell pressure.
        if not sol_size or sol_size < 0.0005:
            return None

        return {
            "side": side,
            "sol_size": sol_size,
            "wallet": trader,
            "token_amount": abs(tok_delta),
            "ts": tx.get("blockTime"),
        }
    except Exception:
        return None


# ── Rolling per-mint state ─────────────────────────────────────────────────────
# mint -> deque[(monotonic_ts, side, sol_size, wallet)]
_flow: dict[str, deque] = {}
_MAX_WINDOW = 300  # keep 5 min of swaps


def ingest(mint: str, swap: dict | None) -> None:
    if not swap or not mint:
        return
    dq = _flow.setdefault(mint, deque())
    dq.append((time.monotonic(), swap["side"], float(swap.get("sol_size") or 0.0), swap.get("wallet")))
    cut = time.monotonic() - _MAX_WINDOW
    while dq and dq[0][0] < cut:
        dq.popleft()


def metrics(mint: str, window: float = 60.0) -> dict | None:
    """Order-flow over the last `window` seconds, or None if no swaps seen."""
    dq = _flow.get(mint)
    if not dq:
        return None
    cut = time.monotonic() - window
    recent = [x for x in dq if x[0] >= cut]
    if not recent:
        return None
    buys = [x for x in recent if x[1] == "buy"]
    sells = [x for x in recent if x[1] == "sell"]
    buy_vol = sum(x[2] for x in buys)
    sell_vol = sum(x[2] for x in sells)
    tot = buy_vol + sell_vol
    return {
        "window_s": window,
        "n_buys": len(buys),
        "n_sells": len(sells),
        "buy_vol_sol": round(buy_vol, 3),
        "sell_vol_sol": round(sell_vol, 3),
        # net pressure in [-1, 1]: +1 = all buying, -1 = all selling
        "net_pressure": round((buy_vol - sell_vol) / tot, 3) if tot else 0.0,
        "unique_buyers": len({x[3] for x in buys if x[3]}),
    }


def clear(mint: str) -> None:
    _flow.pop(mint, None)
