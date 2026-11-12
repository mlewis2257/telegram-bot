"""
price_recorder.py — SOL and BTC in USD every few minutes, for the market-weather test. Research only.

WHY (2026-11-12). The go-live night was a bloodbath (6 of 100 coins banked) while BTC and SOL fell, and
the user's read is that a falling market should have meant trading less. market_weather.py found the
channels' own recent results don't warn in time; this records the outside market so the same test can
ask: does SOL / BTC falling over the last few hours predict bad bank rates? (market_weather.py --signal sol)

  live      every PRICE_RECORDER_SECS (300): SOL-USD and BTC-USD spot from Coinbase's public API (no key),
            CoinGecko's simple price as the fallback. 2 calls per look (~576 a day), printed at start.
  backfill  --backfill [days]: CoinGecko's free hourly history (up to 90 days), so the test can run on the
            weeks we already have trades for, today.

Run:      pm2 start price_recorder.py --name price-recorder --interpreter <venv python>
Backfill: python3 price_recorder.py --backfill 90
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import db  # noqa: E402

SECS = float(os.getenv("PRICE_RECORDER_SECS", "300"))
TIMEOUT = 15


def ensure_table() -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS market_prices (
                at       timestamptz PRIMARY KEY,
                sol_usd  numeric,
                btc_usd  numeric,
                source   text
            )""")
        conn.commit()


def _coinbase() -> tuple[float, float] | None:
    out = []
    for pair in ("SOL-USD", "BTC-USD"):
        r = requests.get(f"https://api.coinbase.com/v2/prices/{pair}/spot", timeout=TIMEOUT)
        if r.status_code != 200:
            return None
        out.append(float(r.json()["data"]["amount"]))
    return out[0], out[1]


def _coingecko() -> tuple[float, float] | None:
    r = requests.get("https://api.coingecko.com/api/v3/simple/price",
                     params={"ids": "solana,bitcoin", "vs_currencies": "usd"}, timeout=TIMEOUT)
    if r.status_code != 200:
        return None
    j = r.json()
    return float(j["solana"]["usd"]), float(j["bitcoin"]["usd"])


def fetch_now() -> tuple[float, float, str] | None:
    for name, fn in (("coinbase", _coinbase), ("coingecko", _coingecko)):
        try:
            p = fn()
        except Exception:
            p = None
        if p:
            return p[0], p[1], name
    return None


def record(sol: float, btc: float, source: str, at: datetime | None = None) -> None:
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("INSERT INTO market_prices (at, sol_usd, btc_usd, source) VALUES (coalesce(%s, now()), %s, %s, %s) "
                    "ON CONFLICT (at) DO NOTHING", (at, sol, btc, source))
    conn.commit()


def backfill(days: int = 90) -> int:
    """CoinGecko hourly history (free API: hourly granularity for 2-90 days). Returns rows added."""
    series = {}
    for coin in ("solana", "bitcoin"):
        r = requests.get(f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart",
                         params={"vs_currency": "usd", "days": min(int(days), 90)}, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"coingecko {coin}: http {r.status_code}: {r.text[:160]}")
        # bucket to the hour: the two series' timestamps differ by seconds
        series[coin] = {int(ms // 3_600_000): float(p) for ms, p in r.json().get("prices") or []}
        time.sleep(3)                       # stay well inside the free rate limit
    hours = sorted(set(series["solana"]) & set(series["bitcoin"]))
    ensure_table()
    conn = db.get_conn()
    n = 0
    with conn.cursor() as cur:
        for h in hours:
            cur.execute("INSERT INTO market_prices (at, sol_usd, btc_usd, source) VALUES (%s,%s,%s,'coingecko_hourly') "
                        "ON CONFLICT (at) DO NOTHING",
                        (datetime.fromtimestamp(h * 3600, tz=timezone.utc), series["solana"][h], series["bitcoin"][h]))
            n += cur.rowcount
    conn.commit()
    return n


def run() -> None:
    ensure_table()
    print(f"[prices] SOL + BTC in USD every {SECS:g}s (Coinbase public spot, CoinGecko fallback): "
          f"2 calls a look, ~{int(86400 / SECS * 2)} a day. Research only.", flush=True)
    fails = 0
    while True:
        p = fetch_now()
        if p:
            try:
                record(*p)
                fails = 0
            except Exception as e:
                db.safe_rollback()
                print(f"[prices] store failed: {type(e).__name__}: {e}", flush=True)
        else:
            fails += 1
            if fails in (1, 12) or fails % 288 == 0:
                print(f"[prices] no price from Coinbase or CoinGecko ({fails} looks in a row)", flush=True)
        time.sleep(SECS)


if __name__ == "__main__":
    if "--backfill" in sys.argv:
        i = sys.argv.index("--backfill")
        days = int(sys.argv[i + 1]) if len(sys.argv) > i + 1 else 90
        print(f"[prices] backfilled {backfill(days)} hourly rows ({days} days)")
    else:
        run()
