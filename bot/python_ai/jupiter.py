"""
jupiter.py — Jupiter Swap API v2 (Ultra) wrapper.

Handles order creation, partial signing, and execution for Solana token swaps.
Devnet mode mocks all Jupiter and RPC calls while exercising the full
DB / alert / position-tracking flow end-to-end.

Signing approach
----------------
Jupiter v2 /order returns a VersionedTransaction (v0) that may already carry
Jupiter's own partial signature. We preserve existing signatures and slot our
keypair's signature at index 0 (fee payer) using VersionedTransaction.populate.
Falls back to sole-signer construction if populate is unavailable (solders < 0.18).

Environment
-----------
JUPITER_API_KEY            — optional; sent as x-api-key header if set
LIVE_TRADING_ENABLED       — must be the string 'true' to execute real swaps
SOLANA_NETWORK             — 'mainnet' or 'devnet' (default: mainnet)
SOLANA_RPC_URL             — Solana RPC endpoint for balance/token checks
"""

import asyncio
import base64
import os
import time
import uuid

import httpx
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction

BASE_URL = "https://api.jup.ag/swap/v2"
SOL_MINT = "So11111111111111111111111111111111111111112"


# ── Custom exceptions ──────────────────────────────────────────────────────────

class RetryError(Exception):
    """Transient failure — caller should fetch a fresh order and retry."""


class ExecutionError(Exception):
    """Permanent failure — do not retry."""

    def __init__(self, message: str, code: int | None = None):
        super().__init__(message)
        self.code = code


# ── HTTP client singleton ──────────────────────────────────────────────────────

_client: httpx.AsyncClient | None = None


def _get_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        headers = {"Content-Type": "application/json"}
        api_key = os.getenv("JUPITER_API_KEY")
        if api_key:
            headers["x-api-key"] = api_key
        _client = httpx.AsyncClient(headers=headers, timeout=30.0)
    return _client


# ── Helpers ────────────────────────────────────────────────────────────────────

def _is_devnet() -> bool:
    return os.getenv("SOLANA_NETWORK", "mainnet").lower() == "devnet"


def _rpc_url() -> str:
    return os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")


# ── Core API calls ─────────────────────────────────────────────────────────────

async def get_order(
    input_mint: str,
    output_mint: str,
    amount_lamports: int,
    wallet_address: str,
) -> dict:
    """
    GET /order — request a swap order from Jupiter.
    No optional params passed so all 4 routers compete for best price.
    Raises ValueError if the response is missing required fields.
    """
    params = {
        "inputMint":  input_mint,
        "outputMint": output_mint,
        "amount":     str(amount_lamports),
        "taker":      wallet_address,
    }
    resp = await _get_client().get(f"{BASE_URL}/order", params=params)
    resp.raise_for_status()
    data = resp.json()
    if "transaction" not in data:
        raise ValueError(f"Jupiter /order response missing 'transaction': {data}")
    if "requestId" not in data:
        raise ValueError(f"Jupiter /order response missing 'requestId': {data}")
    return data


async def sign_transaction(order: dict, keypair: Keypair) -> str:
    """
    Sign the Jupiter-provided VersionedTransaction with our keypair.

    Preserves any existing partial signatures (e.g. Jupiter's own signer)
    by slotting our signature at index 0 (fee payer) without clearing others.
    Falls back to sole-signer construction if VersionedTransaction.populate
    is unavailable in the installed solders version (< 0.18).
    """
    tx_bytes = base64.b64decode(order["transaction"])
    tx = VersionedTransaction.from_bytes(tx_bytes)
    sig = keypair.sign_message(b"\x80" + bytes(tx.message))

    try:
        sigs = list(tx.signatures)
        sigs[0] = sig
        signed_tx = VersionedTransaction.populate(tx.message, sigs)
    except AttributeError:
        # solders < 0.18 — sole-signer fallback.
        # Safe when our wallet is the only required signer.
        print(
            "[jupiter] WARNING: VersionedTransaction.populate unavailable"
            " — using sole-signer fallback (upgrade solders >= 0.18)"
        )
        signed_tx = VersionedTransaction(tx.message, [keypair])

    return base64.b64encode(bytes(signed_tx)).decode()


async def execute_order(signed_tx: str, request_id: str) -> dict:
    """
    POST /execute — submit the signed transaction to Jupiter.

    Error code mapping:
      -1, -1000        → RetryError (transient: quote/landing failure)
      -2003, -2004     → RetryError (transient: quote/swap expired or rejected)
      status='Failed'  → ExecutionError (permanent)
    """
    resp = await _get_client().post(
        f"{BASE_URL}/execute",
        json={"signedTransaction": signed_tx, "requestId": request_id},
    )
    if resp.is_error:
        print(f"[jupiter] HTTP {resp.status_code} response body: {resp.text}")
    resp.raise_for_status()
    data = resp.json()

    code   = data.get("code")
    status = data.get("status")

    if code in (-1, -1000, -2003, -2004):
        raise RetryError(
            f"Jupiter transient error code={code}: {data.get('error', '')}"
        )
    if status == "Failed":
        raise ExecutionError(
            f"Jupiter execution failed: {data.get('error', '')}", code=code
        )

    return data


# ── High-level buy / sell ──────────────────────────────────────────────────────

async def buy_token(
    mint_address: str,
    sol_amount: float,
    max_retries: int = 3,
) -> dict:
    """
    Full buy flow: SOL → token, with retry on transient failures.

    Returns on success:
      {success, signature, sol_spent, tokens_received, mint, router}
    Returns on failure:
      {success=False, error, code}
    """
    if os.getenv("LIVE_TRADING_ENABLED", "false") != "true":
        print("[jupiter] buy_token skipped — LIVE_TRADING_ENABLED is not 'true'")
        return {"success": False, "error": "trading disabled", "code": 0}

    if _is_devnet():
        lamports = int(sol_amount * 1_000_000_000)
        return await _mock_buy_response(mint_address, lamports)

    import wallet as _wallet
    try:
        _wallet.get_sol_balance(_rpc_url())
    except ValueError as e:
        print(f"[jupiter] buy_token skipped — {e}")
        return {"success": False, "error": str(e), "code": 0}

    keypair       = _wallet.get_keypair()
    wallet_address = str(keypair.pubkey())
    lamports      = int(sol_amount * 1_000_000_000)
    last_error    = None

    for attempt in range(1, max_retries + 1):
        try:
            order      = await get_order(SOL_MINT, mint_address, lamports, wallet_address)
            order_time = time.time()
            signed_tx  = await sign_transaction(order, keypair)
            sign_time  = time.time()
            print(f"[jupiter] sign took {sign_time - order_time:.2f}s")
            result     = await execute_order(signed_tx, order["requestId"])
            exec_time  = time.time()
            print(f"[jupiter] total order→execute: {exec_time - order_time:.2f}s")

            sig             = result.get("signature", "")
            tokens_received = int(result.get("outputAmount", 0))
            router          = result.get("router", "unknown")
            # Fallback: if Jupiter didn't return token count, check on-chain
            if tokens_received == 0:
                print(f"[jupiter] outputAmount missing from /execute response, checking on-chain balance...")
                await asyncio.sleep(2)  # wait for tx to finalize
                try:
                    tokens_received, _decimals = await get_token_balance(mint_address, wallet_address, _rpc_url())
                    print(f"[jupiter] on-chain balance fallback: {tokens_received}")
                except Exception as bal_err:
                    print(f"[jupiter] on-chain balance check failed: {bal_err}")
                    # Still return success — the buy executed, we just don't know exact tokens
            # Get decimals for human-readable display if not already fetched above
            if tokens_received > 0 and '_decimals' not in dir():
                try:
                    _, _decimals = await get_token_balance(mint_address, wallet_address, _rpc_url())
                except Exception:
                    _decimals = 6  # default for most pump.fun tokens
            print(
                f"[jupiter] buy OK  mint={mint_address[:8]}..."
                f"  sol={sol_amount:.4f}  tokens={tokens_received}  sig={sig[:16]}..."
            )
            return {
                "success":         True,
                "signature":       sig,
                "sol_spent":       sol_amount,
                "tokens_received": tokens_received,
                "tokens_decimals": _decimals if '_decimals' in dir() else 6,
                "mint":            mint_address,
                "router":          router,
            }

        except RetryError as e:
            last_error = e
            print(f"[jupiter] buy attempt {attempt}/{max_retries} transient: {e}")
            if attempt < max_retries:
                await asyncio.sleep(2)

        except ExecutionError as e:
            print(f"[jupiter] buy permanent failure: {e}")
            return {"success": False, "error": str(e), "code": getattr(e, "code", None)}

        except Exception as e:
            last_error = e
            print(f"[jupiter] buy attempt {attempt}/{max_retries} error: {e}")
            if attempt < max_retries:
                await asyncio.sleep(2)

    return {"success": False, "error": str(last_error), "code": None}


async def sell_token(
    mint_address: str,
    token_amount: int,
    max_retries: int = 3,
) -> dict:
    """
    Full sell flow: token → SOL, with retry on transient failures.

    Returns on success:
      {success, signature, sol_received, tokens_sold, mint}
    Returns on failure:
      {success=False, error, code}
    """
    if os.getenv("LIVE_TRADING_ENABLED", "false") != "true":
        print("[jupiter] sell_token skipped — LIVE_TRADING_ENABLED is not 'true'")
        return {"success": False, "error": "trading disabled", "code": 0}

    if _is_devnet():
        return await _mock_sell_response(mint_address, token_amount)

    import wallet as _wallet
    keypair        = _wallet.get_keypair()
    wallet_address = str(keypair.pubkey())
    last_error     = None

    for attempt in range(1, max_retries + 1):
        try:
            # Capture SOL balance before sell for delta calculation (direct RPC,
            # no MIN_SOL_RESERVE check that would raise on low balances)
            sol_before = None
            try:
                async with httpx.AsyncClient(timeout=10) as _bc:
                    _resp = await _bc.post(_rpc_url(), json={
                        "jsonrpc": "2.0", "id": 1,
                        "method": "getBalance",
                        "params": [wallet_address, {"commitment": "confirmed"}],
                    })
                    sol_before = _resp.json()["result"]["value"] / 1_000_000_000
                    print(f"[jupiter] sell: SOL before = {sol_before:.6f}")
            except Exception as e:
                print(f"[jupiter] sell: could not capture SOL before balance: {e}")

            order      = await get_order(mint_address, SOL_MINT, token_amount, wallet_address)
            order_time = time.time()
            signed_tx  = await sign_transaction(order, keypair)
            sign_time  = time.time()
            print(f"[jupiter] sign took {sign_time - order_time:.2f}s")
            result     = await execute_order(signed_tx, order["requestId"])
            exec_time  = time.time()
            print(f"[jupiter] total order→execute: {exec_time - order_time:.2f}s")

            # Log full response so we know exactly what Jupiter returns
            print(f"[jupiter] sell /execute response keys: {list(result.keys())}")
            print(
                f"[jupiter] sell /execute outputAmount={result.get('outputAmount')} "
                f"inputAmount={result.get('inputAmount')} "
                f"status={result.get('status')}"
            )

            sig          = result.get("signature", "")
            sol_received = int(result.get("outputAmount", 0)) / 1_000_000_000

            # Fallback: balance delta with retries if outputAmount is missing/zero
            if sol_received <= 0 and sol_before is not None:
                print("[jupiter] sell: outputAmount missing, using balance delta...")
                for wait_attempt in range(3):
                    await asyncio.sleep(2)
                    try:
                        async with httpx.AsyncClient(timeout=10) as _ac:
                            _resp = await _ac.post(_rpc_url(), json={
                                "jsonrpc": "2.0", "id": 1,
                                "method": "getBalance",
                                "params": [wallet_address, {"commitment": "confirmed"}],
                            })
                            sol_after = _resp.json()["result"]["value"] / 1_000_000_000
                            delta = sol_after - sol_before
                            print(
                                f"[jupiter] sell: SOL after = {sol_after:.6f}, "
                                f"delta = {delta:.6f} (attempt {wait_attempt + 1})"
                            )
                            if delta > 0:
                                sol_received = delta
                                break
                    except Exception as e:
                        print(f"[jupiter] sell: balance delta attempt {wait_attempt + 1} failed: {e}")

                if sol_received <= 0:
                    print(
                        f"[jupiter] sell: WARNING — could not determine SOL received. "
                        f"sol_before={sol_before:.6f}"
                    )

            if sol_received > 0 and sol_received < 0.001:
                print(
                    f"[jupiter] sell: WARNING — sol_received={sol_received:.6f} "
                    f"seems unusually low for {token_amount} tokens of {mint_address[:12]}..."
                )

            print(
                f"[jupiter] sell OK  mint={mint_address[:8]}..."
                f"  tokens={token_amount}  sol={sol_received:.6f}  sig={sig[:16]}..."
            )
            return {
                "success":      True,
                "signature":    sig,
                "sol_received": sol_received,
                "tokens_sold":  token_amount,
                "mint":         mint_address,
            }

        except RetryError as e:
            last_error = e
            print(f"[jupiter] sell attempt {attempt}/{max_retries} transient: {e}")
            if attempt < max_retries:
                await asyncio.sleep(2)

        except ExecutionError as e:
            print(f"[jupiter] sell permanent failure: {e}")
            return {"success": False, "error": str(e), "code": getattr(e, "code", None)}

        except Exception as e:
            last_error = e
            print(f"[jupiter] sell attempt {attempt}/{max_retries} error: {e}")
            if attempt < max_retries:
                await asyncio.sleep(2)

    return {"success": False, "error": str(last_error), "code": None}


async def get_token_balance(
    mint_address: str,
    wallet_address: str,
    rpc_url: str,
) -> int:
    """
    Return the wallet's token balance in raw integer units (smallest denomination).
    Returns 0 only if no token account exists for this mint.
    Raises on RPC errors or unexpected response shape so callers are not silently
    misled into thinking a balance is zero when the query actually failed.
    Uses {"mint": mint_address} filter which works across Token Program and Token-2022.
    """
    if _is_devnet():
        return _mock_token_balance(mint_address)

    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id":      1,
                "method":  "getTokenAccountsByOwner",
                "params":  [
                    wallet_address,
                    {"mint": mint_address},
                    {"encoding": "jsonParsed", "commitment": "confirmed"},
                ],
            },
        )
    resp.raise_for_status()
    data = resp.json()

    if "error" in data:
        raise RuntimeError(f"RPC getTokenAccountsByOwner error: {data['error']}")

    accounts = data.get("result", {}).get("value", [])
    if not accounts:
        print(f"[jupiter] get_token_balance  mint={mint_address[:12]}...  no account found → 0")
        return 0, 0

    try:
        token_amount = accounts[0]["account"]["data"]["parsed"]["info"]["tokenAmount"]
        amount_str   = token_amount["amount"]
        dec          = int(token_amount.get("decimals", 0))
        balance      = int(amount_str)
        print(
            f"[jupiter] get_token_balance  mint={mint_address[:12]}..."
            f"  balance={balance}  decimals={dec}"
        )
        return balance, dec
    except (KeyError, IndexError) as e:
        print(f"[jupiter] get_token_balance parse error ({e}) — raw response: {data}")
        raise RuntimeError(f"Failed to parse token balance response: {e}") from e


# ── Devnet mocks ───────────────────────────────────────────────────────────────

async def _mock_buy_response(mint: str, amount_lamports: int) -> dict:
    """
    Return a fake buy success response for devnet / integration testing.
    Exercises the full DB-write, alert, and position-tracking flow without
    touching real Jupiter or spending real SOL.
    """
    sig        = uuid.uuid4().hex
    sol_spent  = amount_lamports / 1_000_000_000
    tokens     = 1_000_000
    print(
        f"[DEVNET MOCK] buy  mint={mint[:8]}..."
        f"  lamports={amount_lamports}  tokens={tokens}  sig={sig[:16]}..."
    )
    return {
        "success":         True,
        "signature":       sig,
        "sol_spent":       sol_spent,
        "tokens_received": tokens,
        "mint":            mint,
        "router":          "MOCK",
    }


async def _mock_sell_response(mint: str, token_amount: int) -> dict:
    """
    Return a fake sell success response for devnet / integration testing.
    Paired with _mock_token_balance so the sell flow actually triggers.
    """
    sig          = uuid.uuid4().hex
    sol_received = 0.001
    print(
        f"[DEVNET MOCK] sell  mint={mint[:8]}..."
        f"  tokens={token_amount}  sol_received={sol_received}  sig={sig[:16]}..."
    )
    return {
        "success":      True,
        "signature":    sig,
        "sol_received": sol_received,
        "tokens_sold":  token_amount,
        "mint":         mint,
    }


def _mock_token_balance(mint: str) -> int:
    """
    Return a fake non-zero token balance for devnet sell-flow testing.
    Without this, get_token_balance returns 0 and close_live_position
    marks the position as 'tokens already gone' without calling sell_token.
    """
    balance = 1_000_000
    print(f"[DEVNET MOCK] token_balance  mint={mint[:8]}...  → {balance}")
    return balance, 6
