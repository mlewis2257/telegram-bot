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
