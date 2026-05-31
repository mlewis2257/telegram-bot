"""
ws_tx_market_probe.py — Manually test websocket transaction-derived market data.

This probes the same Helius getTransaction parser used by ws_monitor.py without
requiring an open position or websocket subscription.

Usage:
    python3 ws_tx_market_probe.py --signature <tx_sig> --mint <token_mint>
    python3 ws_tx_market_probe.py --signature <tx_sig> --mint <token_mint> --compare-fallback
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import data_fetcher
import db


def _fmt_money(value) -> str:
    if value is None:
        return "n/a"
    try:
        return f"${float(value):,.6f}"
    except Exception:
        return str(value)


def _fmt_mcap(value) -> str:
    if value is None:
        return "n/a"
    try:
        return f"${float(value):,.2f}"
    except Exception:
        return str(value)


def _print_market(title: str, market: dict | None) -> None:
    print(title)
    print("-" * 72)
    if not market:
        print("No market result.")
        return
    print(f"source        {market.get('source') or market.get('price_source') or 'unknown'}")
    print(f"price_usd     {_fmt_money(market.get('price_usd'))}")
    print(f"mcap          {_fmt_mcap(market.get('mcap'))}")
    print(f"liquidity     {_fmt_mcap(market.get('liquidity_usd'))}")
    print(f"volume_h1     {_fmt_mcap(market.get('volume_h1'))}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Probe Helius transaction-derived market data")
    parser.add_argument("--signature", required=True, help="Solana transaction signature")
    parser.add_argument("--mint", required=True, help="Token mint address to price from the transaction")
    parser.add_argument(
        "--compare-fallback",
        action="store_true",
        help="Also fetch the old Dex/Jupiter fallback market for comparison",
    )
    parser.add_argument(
        "--store",
        action="store_true",
        help="Persist the probe result as a ws_market_observation when --call-id is provided",
    )
    parser.add_argument("--call-id", type=int, default=None, help="Optional call_id for --store")
    args = parser.parse_args()

    print("=" * 72)
    print("Websocket Transaction Market Probe")
    print("=" * 72)
    print(f"signature     {args.signature}")
    print(f"mint          {args.mint}")
    print()

    tx_market = data_fetcher.fetch_ws_exit_market_from_transaction(args.signature, args.mint)
    _print_market("Helius transaction-derived market", tx_market)

    if args.store:
        if not args.call_id:
            raise SystemExit("--store requires --call-id")
        if not tx_market:
            raise SystemExit("No tx market result to store")
        db.ensure_ws_market_observations_table()
        db.insert_ws_market_observation(
            call_id=args.call_id,
            mint_address=args.mint,
            signature=args.signature,
            market=tx_market,
        )
        print()
        print(f"stored observation for call_id={args.call_id}")

    if args.compare_fallback:
        print()
        fallback = data_fetcher.fetch_token_price(args.mint)
        _print_market("Fallback Dex/Jupiter market", fallback)
        if tx_market and fallback and tx_market.get("mcap") and fallback.get("mcap"):
            tx_mcap = float(tx_market["mcap"])
            fb_mcap = float(fallback["mcap"])
            diff_pct = ((tx_mcap - fb_mcap) / fb_mcap * 100.0) if fb_mcap else 0.0
            print()
            print(f"mcap_diff_pct {diff_pct:+.2f}%")


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
