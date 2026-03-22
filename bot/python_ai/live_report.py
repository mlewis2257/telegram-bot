"""
live_report.py — Print a live trading P&L report to stdout.

Usage:
    python3 live_report.py

Shows:
  - Wallet address and current SOL balance
  - All open live positions with unrealised P&L estimate
  - Closed-position summary (win rate, total P&L, exit breakdown)
"""

import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv

load_dotenv()

import db
import wallet as _wallet
import live_trader


def _fmt_sol(v: float) -> str:
    return f"{v:+.4f} SOL" if v != 0 else "  0.0000 SOL"


def _fmt_mcap(n) -> str:
    if n is None:
        return "n/a"
    n = float(n)
    if n >= 1_000_000:
        return f"${n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"${n / 1_000:.1f}k"
    return f"${n:.0f}"


def _age(dt) -> str:
    if dt is None:
        return "?"
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    secs = (datetime.now(timezone.utc) - dt).total_seconds()
    if secs < 3600:
        return f"{int(secs // 60)}m"
    return f"{secs / 3600:.1f}h"


def main() -> None:
    rpc = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")

    # ── Wallet ────────────────────────────────────────────────────────────────
    print("=" * 60)
    print("LIVE TRADING REPORT")
    print("=" * 60)

    try:
        pub  = _wallet.get_public_key()
        bal  = _wallet.get_sol_balance(rpc)
        print(f"Wallet : {pub}")
        print(f"Balance: {bal:.4f} SOL")
    except Exception as e:
        print(f"Wallet : (error — {e})")
    print()

    # ── Open positions ────────────────────────────────────────────────────────
    open_positions = db.get_open_live_positions()
    print(f"OPEN POSITIONS ({len(open_positions)})")
    print("-" * 60)

    if not open_positions:
        print("  (none)")
    else:
        for pos in open_positions:
            symbol     = pos.get("symbol") or "?"
            sol_in     = float(pos["sol_in"]) if pos["sol_in"] else 0.0
            entry_time = pos.get("entry_time")
            router     = pos.get("router") or "?"
            sig        = pos.get("tx_signature") or ""
            sig_short  = sig[:16] + "..." if len(sig) > 16 else sig

            print(
                f"  {symbol:<14} sol_in={sol_in:.4f}"
                f"  age={_age(entry_time)}"
                f"  router={router}"
                f"  sig={sig_short}"
            )
    print()

    # ── Closed positions ──────────────────────────────────────────────────────
    summary = live_trader.get_live_pnl_summary()

    total    = summary.get("total_trades", 0)
    wins     = summary.get("wins", 0)
    losses   = summary.get("losses", 0)
    total_pnl = summary.get("total_pnl_sol", 0.0)
    avg_win  = summary.get("avg_win_sol", 0.0)
    avg_loss = summary.get("avg_loss_sol", 0.0)
    breakdown = summary.get("by_exit_reason", {})

    win_rate = (wins / total * 100) if total > 0 else 0.0

    print(f"CLOSED POSITIONS ({total})")
    print("-" * 60)

    if total == 0:
        print("  (none)")
    else:
        print(f"  Win rate  : {wins}/{total} ({win_rate:.0f}%)")
        print(f"  Total P&L : {_fmt_sol(total_pnl)}")
        print(f"  Avg win   : {_fmt_sol(avg_win)}")
        print(f"  Avg loss  : {_fmt_sol(avg_loss)}")
        print()
        print("  Exit breakdown:")
        for reason, count in sorted(breakdown.items()):
            if count:
                print(f"    {reason:<14} {count}")

    print("=" * 60)


if __name__ == "__main__":
    main()
