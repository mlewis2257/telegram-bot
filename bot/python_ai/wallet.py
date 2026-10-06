"""
wallet.py — Solana keypair management for live trading.

Loads the trading keypair from environment and provides balance checks.
All functions are synchronous — called from both sync and async contexts.
"""

import os

import httpx
from solders.keypair import Keypair

MIN_SOL_RESERVE = 0.05  # always keep this much SOL free for transaction fees


def get_keypair() -> Keypair:
    """Load the trading keypair from TRADING_WALLET_PRIVATE_KEY env var."""
    raw = os.getenv("TRADING_WALLET_PRIVATE_KEY")
    if not raw:
        raise EnvironmentError("TRADING_WALLET_PRIVATE_KEY is not set")
    return Keypair.from_base58_string(raw)


def get_public_key() -> str:
    """Return the wallet's public key as a base58 string."""
    return str(get_keypair().pubkey())


def get_sol_balance(rpc_url: str) -> float:
    """
    Fetch current SOL balance via getBalance RPC call.
    Returns balance in SOL (not lamports).
    Raises ValueError if balance is below MIN_SOL_RESERVE.
    """
    pubkey = get_public_key()
    resp = httpx.post(
        rpc_url,
        json={
            "jsonrpc": "2.0",
            "id":      1,
            "method":  "getBalance",
            "params":  [pubkey, {"commitment": "confirmed"}],
        },
        timeout=10.0,
    )
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"RPC getBalance error: {data['error']}")
    lamports = data["result"]["value"]
    sol = lamports / 1_000_000_000
    if sol < MIN_SOL_RESERVE:
        raise ValueError(
            f"SOL balance {sol:.4f} is below minimum reserve {MIN_SOL_RESERVE} SOL"
        )
    return sol


def get_sol_balance_any(fallback_url: str = "") -> float:
    """Wallet SOL balance, tried across every configured RPC endpoint.

    Order: rpc_pool's next healthy endpoint first, then every other configured endpoint
    INCLUDING ones in cooldown (a cooled key is a worse bet, not a forbidden one), and
    `fallback_url` only when no pool is configured at all.

    A genuine low balance is NOT an RPC failure and is raised at once: another endpoint
    cannot add SOL. get_sol_balance signals it with ValueError — but so does a non-JSON
    body (JSONDecodeError subclasses ValueError), which IS an endpoint failure, so that
    one is caught first.
    """
    import json
    import rpc_pool

    order: list[str] = []
    first = rpc_pool.http_url()
    for u in ([first] if first else []) + rpc_pool.endpoints():
        if u and u not in order:
            order.append(u)
    if not order:
        order = [fallback_url]

    last_err: Exception | None = None
    for url in order:
        try:
            return get_sol_balance(url)
        except json.JSONDecodeError as e:
            last_err = e
            rpc_pool.penalize(url)
        except ValueError:
            raise
        except Exception as e:
            last_err = e
            rpc_pool.penalize(url)
    raise RuntimeError(f"all {len(order)} RPC endpoint(s) failed; last: "
                       f"{type(last_err).__name__} {str(last_err)[:120]}")
