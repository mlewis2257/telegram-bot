"""
paper_report.py — Print a summary of simulated paper trading performance.

Usage:
    python3 paper_report.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import db
import data_fetcher


def _fmt_pct(v) -> str:
    if v is None:
        return "n/a"
    return f"{v:+.1f}%"


def _fmt_sol(v) -> str:
    if v is None:
        return "n/a"
    return f"{v:+.3f} SOL"


def _fmt_mcap(n) -> str:
    if n is None:
        return "n/a"
    n = float(n)
    if n >= 1_000_000:
        return f"${n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"${n / 1_000:.1f}k"
    return f"${n:.0f}"


def _get_best_worst() -> tuple[dict | None, dict | None]:
    """Return (best_trade_row, worst_trade_row) with symbol and pnl_pct."""
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.symbol, tp.pnl_pct, tp.pnl_sol, tp.exit_reason
            FROM trading_positions tp
            JOIN calls  c ON c.id  = tp.call_id
            JOIN tokens t ON t.id  = c.token_id
            WHERE tp.is_simulation = TRUE
              AND tp.status        = 'closed'
              AND tp.pnl_pct IS NOT NULL
            ORDER BY tp.pnl_pct DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        best = {"symbol": row[0], "pnl_pct": float(row[1]), "pnl_sol": float(row[2]), "reason": row[3]} if row else None

        cur.execute(
            """
            SELECT t.symbol, tp.pnl_pct, tp.pnl_sol, tp.exit_reason
            FROM trading_positions tp
            JOIN calls  c ON c.id  = tp.call_id
            JOIN tokens t ON t.id  = c.token_id
            WHERE tp.is_simulation = TRUE
              AND tp.status        = 'closed'
              AND tp.pnl_pct IS NOT NULL
            ORDER BY tp.pnl_pct ASC
            LIMIT 1
            """
        )
        row = cur.fetchone()
        worst = {"symbol": row[0], "pnl_pct": float(row[1]), "pnl_sol": float(row[2]), "reason": row[3]} if row else None

    return best, worst


def main() -> None:
    s    = db.get_paper_pnl_summary()
    best, worst = _get_best_worst()
    open_positions = db.get_open_paper_positions()

    total = s["open"] + s["closed"]
    win_rate = (s["winners"] / s["closed"] * 100) if s["closed"] > 0 else 0.0
    avg_pnl_sol = (s["total_pnl"] / s["closed"]) if s["closed"] > 0 else 0.0

    print("=" * 40)
    print("  PAPER TRADING REPORT")
    print("=" * 40)
    print(f"Open positions:    {s['open']}")
    print(f"Closed positions:  {s['closed']}")
    print()

    if s["closed"] == 0:
        print("No closed positions yet.")
    else:
        print(f"Win rate:          {win_rate:.1f}%")
        print(f"Total P&L:         {_fmt_sol(s['total_pnl'])}")
        print(f"Avg trade P&L:     {_fmt_sol(avg_pnl_sol)} ({_fmt_pct(s['avg_pnl_pct'])})")

        if best:
            mult = (best["pnl_pct"] / 100) + 1
            print(f"Best trade:        ${best['symbol']} {_fmt_pct(best['pnl_pct'])} ({mult:.1f}x)")
        if worst:
            print(f"Worst trade:       ${worst['symbol']} {_fmt_pct(worst['pnl_pct'])}")

        print()
        print("Exit breakdown:")
        bd = s["exit_breakdown"]
        print(f"  10x take profit: {bd.get('10x_tp', 0)} trades")
        print(f"  5x take profit:  {bd.get('5x_tp', 0)} trades")
        print(f"  3x take profit:  {bd.get('3x_tp', 0)} trades")
        print(f"  Trailing stop:   {bd.get('trail_stop', 0)} trades")
        print(f"  Hard stop:       {bd.get('hard_stop', 0)} trades")
        print(f"  Time stop:       {bd.get('time_stop', 0)} trades")

    if open_positions:
        print()
        print("Open positions:")
        for pos in open_positions:
            symbol = pos["symbol"] or "?"
            mint   = pos.get("mint_address")
            entry  = float(pos["entry_price"]) if pos["entry_price"] else None

            current_mcap = None
            if mint:
                market = data_fetcher.fetch_token_price(mint)
                if market and market.get("mcap"):
                    current_mcap = float(market["mcap"])

            if entry and current_mcap:
                pct = (current_mcap - entry) / entry * 100
                print(
                    f"  ${symbol:<12}  entry={_fmt_mcap(entry)}"
                    f"  current={_fmt_mcap(current_mcap)}  {pct:+.1f}%"
                )
            else:
                print(f"  ${symbol:<12}  entry={_fmt_mcap(entry)}  current=n/a")

    print()


if __name__ == "__main__":
    try:
        main()
    finally:
        db.close_conn()
