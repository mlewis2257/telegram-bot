"""
followed_buy_detector.py — our own calls: when 3+ followed wallets buy the same coin.

WHY (2026-10-31). Every live trade depended on one Telegram channel: a format change cut
recorded calls from ~75 to 3 a day and a Telegram outage stopped them for 6 hours. The live
lane's signal is "3+ of the operator's followed wallets hold the coin" — that can be seen
directly. This watches the followed wallets' trades and raises a signal when MIN_WALLETS
distinct wallets have bought the same coin within WINDOW_MIN minutes. RECORD-ONLY: signals
are stored and compared with the channel's calls (same coins? earlier? coins it never
called?); nothing trades on them yet.

SOURCES. FOLLOWED_DETECT_SOURCE = auto (default: pumpportal if PUMPPORTAL_API_KEY is set, else
helius) | helius | pumpportal.

  helius (FREE in money, 2026-11-03): one logsSubscribe per wallet on the pooled Helius websocket.
  pump.fun emits a TradeEvent ("Program data:" log line) with mint, SOL, tokens, buy/sell, user
  and reserves, so a buy is read straight from the log notification — no getTransaction calls.
  Only pump.fun bonding-curve trades are decoded (PumpSwap/other DEX trades are counted, not
  read). Helius bills websockets by data: 2 credits per 0.1 MB. Hard daily cap
  FOLLOWED_DETECT_MAX_MB_PER_DAY [40] MB -> at most ~800 credits/day (~24,000/month), printed at
  start; when reached the socket closes until the next UTC day.

  pumpportal: PumpPortal's data websocket, subscribeAccountTrade (pump.fun + PumpSwap trades by
the listed wallets). Metered by PumpPortal (0.01 SOL per 10,000 messages at the time of
writing) and needs an API key linked to a funded wallet — create it yourself on pumpportal.fun,
put it ONLY in the VPS .env, never in chat or git. ONE connection; reconnects wait at least
RECONNECT_MIN_S (repeated reconnects can get the key banned for an hour). A hard daily message
cap closes the connection until the next UTC day.

    PUMPPORTAL_API_KEY=...                   off (sleeps) without it
    FOLLOWED_DETECT_MIN_WALLETS=3
    FOLLOWED_DETECT_WINDOW_MIN=60            distinct buyers counted within this window
    FOLLOWED_DETECT_MIN_SOL=0.05             ignore smaller buys (dust / tests)
    FOLLOWED_DETECT_MAX_MSGS_PER_DAY=5000    ~0.005 SOL/day at most at the published rate
                                             (measured 2026-10-31: the 43 wallets made ~2,850 txs
                                             of all kinds in 24 h -> ~$10/month expected)

Run:     pm2 start followed_buy_detector.py --name wallet-detector --interpreter <venv python>
Report:  python3 followed_buy_detector.py --report [--since '2026-11-01']
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

import db  # noqa: E402
import wallet_holdings as wh  # noqa: E402

WS_URL = "wss://pumpportal.fun/api/data?api-key={key}"
API_KEY = os.getenv("PUMPPORTAL_API_KEY", "").strip()
MIN_WALLETS = int(os.getenv("FOLLOWED_DETECT_MIN_WALLETS", "3"))
WINDOW_MIN = float(os.getenv("FOLLOWED_DETECT_WINDOW_MIN", "60"))
MIN_SOL = float(os.getenv("FOLLOWED_DETECT_MIN_SOL", "0.05"))
MAX_MSGS_PER_DAY = int(os.getenv("FOLLOWED_DETECT_MAX_MSGS_PER_DAY", "5000"))
SOURCE = os.getenv("FOLLOWED_DETECT_SOURCE", "auto").strip().lower()
MAX_MB_PER_DAY = float(os.getenv("FOLLOWED_DETECT_MAX_MB_PER_DAY", "40"))
PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
RECONNECT_MIN_S = 60
RAW_LOG_FIRST = 5          # print the first raw trade messages so the format can be checked


# ── Storage ─────────────────────────────────────────────────────────────────────

def ensure_tables() -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS followed_trades (
                signature   text PRIMARY KEY,
                wallet      text NOT NULL,
                mint        text NOT NULL,
                side        text NOT NULL,          -- buy | sell
                sol         numeric,
                tokens      numeric,
                mcap_sol    numeric,                -- market cap in SOL after the trade
                pool        text,
                received_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_ft_mint_time ON followed_trades (mint, received_at)")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS followed_signals (
                mint            text PRIMARY KEY,
                first_buy_at    timestamptz NOT NULL,   -- first followed buy in the window
                signal_at       timestamptz NOT NULL,   -- when the MIN_WALLETS-th wallet bought
                n_wallets       integer NOT NULL,
                wallets         text[],
                total_sol       numeric,
                mcap_sol        numeric                 -- market cap (SOL) at the signal trade
            )
            """
        )
        conn.commit()


def store_trade(t: dict) -> None:
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO followed_trades (signature, wallet, mint, side, sol, tokens, mcap_sol, pool)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (signature) DO NOTHING
            """, (t["sig"], t["wallet"], t["mint"], t["side"], t["sol"], t["tokens"], t["mcap_sol"], t["pool"]))
        conn.commit()


def store_signal(mint: str, first_at: datetime, wallets: list[str], total_sol: float, mcap_sol) -> bool:
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO followed_signals (mint, first_buy_at, signal_at, n_wallets, wallets, total_sol, mcap_sol)
            VALUES (%s, %s, now(), %s, %s, %s, %s) ON CONFLICT (mint) DO NOTHING
            """, (mint, first_at, len(wallets), wallets, total_sol, mcap_sol))
        new = cur.rowcount == 1
        conn.commit()
    return new


# ── Detection (pure, testable) ──────────────────────────────────────────────────

def parse_trade(msg: dict, followed: set[str]) -> dict | None:
    """A PumpPortal trade message -> normalized trade, or None if it is not one of ours."""
    wallet = msg.get("traderPublicKey")
    side = (msg.get("txType") or "").lower()
    mint = msg.get("mint")
    if not wallet or wallet not in followed or side not in ("buy", "sell") or not mint:
        return None

    def num(k):
        try:
            return float(msg.get(k)) if msg.get(k) is not None else None
        except (TypeError, ValueError):
            return None
    return {"sig": msg.get("signature") or f"{wallet}:{mint}:{time.time()}", "wallet": wallet,
            "mint": mint, "side": side, "sol": num("solAmount"), "tokens": num("tokenAmount"),
            "mcap_sol": num("marketCapSol"), "pool": msg.get("pool")}


def _trade_event_disc() -> bytes:
    import hashlib
    return hashlib.sha256(b"event:TradeEvent").digest()[:8]


TRADE_EVENT = _trade_event_disc()


def parse_pump_logs(logs: list[str], wallet: str, signature: str) -> list[dict]:
    """pump.fun TradeEvents in a transaction's logs whose user is `wallet` -> normalized trades.
    Layout (verified 2026-11-03 on a real buy): disc(8) mint(32) sol_amount u64 token_amount u64
    is_buy u8 user(32) timestamp i64 virtual_sol u64 virtual_token u64 ..."""
    import base64
    from solders.pubkey import Pubkey
    out = []
    for line in logs or []:
        if not line.startswith("Program data: "):
            continue
        try:
            b = base64.b64decode(line[14:])
        except Exception:
            continue
        if len(b) < 8 + 32 + 17 + 32 + 24 or b[:8] != TRADE_EVENT:
            continue
        u = lambda a: int.from_bytes(b[a:a + 8], "little")
        mint = str(Pubkey.from_bytes(b[8:40]))
        sol, tok, is_buy = u(40), u(48), b[56]
        user = str(Pubkey.from_bytes(b[57:89]))
        vsol, vtok = u(97), u(105)
        if user != wallet:
            continue
        out.append({"sig": f"{signature}:{len(out)}" if out else signature, "wallet": wallet, "mint": mint,
                    "side": "buy" if is_buy else "sell", "sol": sol / 1e9, "tokens": tok / 1e6,
                    "mcap_sol": (vsol * 1e6 / vtok) if vtok else None, "pool": "pump"})
    return out


class Detector:
    """Distinct followed wallets buying each coin within a sliding window."""

    def __init__(self, min_wallets: int, window_min: float, min_sol: float):
        self.min_wallets, self.window, self.min_sol = min_wallets, timedelta(minutes=window_min), min_sol
        self.buys: dict[str, dict[str, tuple[datetime, float]]] = defaultdict(dict)   # mint -> wallet -> (t, sol)
        self.signalled: set[str] = set()

    def add(self, t: dict, now: datetime) -> dict | None:
        """Returns a signal dict the first time a coin reaches min_wallets distinct buyers."""
        if t["side"] != "buy" or (t["sol"] is not None and t["sol"] < self.min_sol):
            return None
        mint = t["mint"]
        per = self.buys[mint]
        per.setdefault(t["wallet"], (now, t["sol"] or 0.0))
        for w in [w for w, (at, _) in per.items() if now - at > self.window]:
            del per[w]
        if mint in self.signalled or len(per) < self.min_wallets:
            return None
        self.signalled.add(mint)
        return {"mint": mint, "first_at": min(at for at, _ in per.values()), "wallets": sorted(per),
                "total_sol": sum(s for _, s in per.values()), "mcap_sol": t["mcap_sol"]}

    def prune(self, now: datetime) -> None:
        for mint in [m for m, per in self.buys.items()
                     if not per or all(now - at > self.window for at, _ in per.values())]:
            del self.buys[mint]


# ── Websocket loop ──────────────────────────────────────────────────────────────

async def run_helius() -> None:
    """Free source: logsSubscribe(mentions=wallet) per followed wallet on the Helius websocket."""
    import websockets
    import rpc_pool
    ensure_tables()
    det = Detector(MIN_WALLETS, WINDOW_MIN, MIN_SOL)
    day, nbytes = None, 0
    stats: dict = defaultdict(int)
    last_tally = time.monotonic()
    cap_bytes = MAX_MB_PER_DAY * 1_000_000
    print(f"[detector] FREE source (Helius logsSubscribe): signal at {MIN_WALLETS}+ distinct followed buyers "
          f"within {WINDOW_MIN:g} min (buys >= {MIN_SOL:g} SOL). Websocket billed by data: cap "
          f"{MAX_MB_PER_DAY:g} MB/day = at most ~{MAX_MB_PER_DAY * 20:,.0f} credits/day "
          f"(~{MAX_MB_PER_DAY * 20 * 30:,.0f}/month). No getTransaction calls. RECORD-ONLY.", flush=True)
    while True:
        wallets = wh.load_wallets(wh.WALLETS_FILE)
        if not wallets:
            print(f"[detector] no wallets in {wh.WALLETS_FILE}; retrying in 10 min", flush=True)
            await asyncio.sleep(600)
            continue
        url = rpc_pool.ws_url()
        if not url:
            print("[detector] no Helius websocket endpoint configured; retrying in 10 min", flush=True)
            await asyncio.sleep(600)
            continue
        started = time.monotonic()
        capped = False
        try:
            async with websockets.connect(url, ping_interval=30, ping_timeout=30, max_size=2 ** 22) as ws:
                req_wallet, sub_wallet = {}, {}
                for i, w in enumerate(wallets, 1):
                    req_wallet[i] = w
                    await ws.send(json.dumps({"jsonrpc": "2.0", "id": i, "method": "logsSubscribe",
                                              "params": [{"mentions": [w]}, {"commitment": "confirmed"}]}))
                print(f"[detector] connected; subscribing to {len(wallets)} wallets", flush=True)
                async for raw in ws:
                    now = datetime.now(timezone.utc)
                    if now.date() != day:
                        day, nbytes = now.date(), 0
                    nbytes += len(raw)
                    if nbytes > cap_bytes:
                        print(f"[detector] daily data cap {MAX_MB_PER_DAY:g} MB reached — closing until the "
                              f"next UTC day", flush=True)
                        capped = True
                        break
                    msg = json.loads(raw)
                    if "id" in msg and msg.get("id") in req_wallet:
                        if "result" in msg:
                            sub_wallet[msg["result"]] = req_wallet[msg["id"]]
                            if len(sub_wallet) == len(wallets):
                                print(f"[detector] subscribed to all {len(wallets)} wallets", flush=True)
                        else:
                            print(f"[detector] subscribe failed for {req_wallet[msg['id']][:8]}: "
                                  f"{str(msg.get('error'))[:150]}", flush=True)
                            if rpc_pool.is_quota_error(None, json.dumps(msg.get("error"))):
                                rpc_pool.penalize(url)
                        continue
                    if msg.get("method") != "logsNotification":
                        continue
                    p = msg.get("params") or {}
                    wallet = sub_wallet.get(p.get("subscription"))
                    val = ((p.get("result") or {}).get("value")) or {}
                    stats["txs"] += 1
                    if not wallet or val.get("err") is not None:
                        continue
                    logs = val.get("logs") or []
                    trades = parse_pump_logs(logs, wallet, val.get("signature") or "")
                    if not trades:
                        stats["non_pump_or_other"] += 1 if not any(PUMP_PROGRAM in l for l in logs) else 0
                        continue
                    for t in trades:
                        stats[t["side"] + "s"] += 1
                        try:
                            store_trade(t)
                            sig = det.add(t, now)
                            if sig and store_signal(sig["mint"], sig["first_at"], sig["wallets"],
                                                    sig["total_sol"], sig["mcap_sol"]):
                                stats["signals"] += 1
                                print(f"[detector] SIGNAL {sig['mint']} — {len(sig['wallets'])} followed wallets "
                                      f"bought within {(now - sig['first_at']).total_seconds() / 60:.0f} min, "
                                      f"{sig['total_sol']:.2f} SOL, mcap {sig['mcap_sol'] or 0:.0f} SOL", flush=True)
                        except Exception as e:
                            db.safe_rollback()
                            print(f"[detector] store failed: {type(e).__name__}: {e}", flush=True)
                    if stats["txs"] % 500 == 0:
                        det.prune(now)
                    if time.monotonic() - last_tally > 1800:
                        last_tally = time.monotonic()
                        print(f"[detector] 30m: " + " ".join(f"{k}={v}" for k, v in sorted(stats.items()))
                              + f" | today {nbytes / 1e6:.1f} MB (~{nbytes / 1e6 * 20:.0f} credits)", flush=True)
                        stats.clear()
        except Exception as e:
            print(f"[detector] connection ended: {type(e).__name__}: {e}", flush=True)
        if capped:
            tomorrow = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
            await asyncio.sleep(max(60.0, (tomorrow - datetime.now(timezone.utc)).total_seconds()))
            continue
        await asyncio.sleep(max(RECONNECT_MIN_S, RECONNECT_MIN_S * 3 - (time.monotonic() - started)))


async def run() -> None:
    import websockets
    if SOURCE == "helius" or (SOURCE == "auto" and not API_KEY):
        await run_helius()
        return
    if not API_KEY:
        print("[detector] PUMPPORTAL_API_KEY not set — detector idle (set it in the .env and restart)", flush=True)
        while True:
            await asyncio.sleep(3600)
    ensure_tables()
    det = Detector(MIN_WALLETS, WINDOW_MIN, MIN_SOL)
    day, msgs, raw_logged = None, 0, 0
    print(f"[detector] watching followed wallets via PumpPortal: signal at {MIN_WALLETS}+ distinct buyers "
          f"within {WINDOW_MIN:g} min (buys >= {MIN_SOL:g} SOL); cap {MAX_MSGS_PER_DAY:,} messages/day "
          f"(~{MAX_MSGS_PER_DAY / 10000 * 0.01:.3f} SOL/day at 0.01 SOL per 10k). RECORD-ONLY.", flush=True)
    while True:
        wallets = wh.load_wallets(wh.WALLETS_FILE)
        if not wallets:
            print(f"[detector] no wallets in {wh.WALLETS_FILE}; retrying in 10 min", flush=True)
            await asyncio.sleep(600)
            continue
        followed = set(wallets)
        started = time.monotonic()
        try:
            async with websockets.connect(WS_URL.format(key=API_KEY), ping_interval=30, ping_timeout=30,
                                          max_size=2 ** 20) as ws:
                await ws.send(json.dumps({"method": "subscribeAccountTrade", "keys": wallets}))
                print(f"[detector] connected; subscribed to {len(wallets)} wallets", flush=True)
                async for raw in ws:
                    now = datetime.now(timezone.utc)
                    if now.date() != day:
                        day, msgs = now.date(), 0
                    msgs += 1
                    if msgs > MAX_MSGS_PER_DAY:
                        print(f"[detector] daily cap {MAX_MSGS_PER_DAY:,} reached — disconnecting until "
                              f"the next UTC day", flush=True)
                        break
                    try:
                        msg = json.loads(raw)
                    except ValueError:
                        continue
                    if "txType" not in msg:
                        print(f"[detector] server: {str(msg)[:200]}", flush=True)
                        continue
                    if raw_logged < RAW_LOG_FIRST:
                        raw_logged += 1
                        print(f"[detector] raw trade #{raw_logged}: {str(raw)[:300]}", flush=True)
                    t = parse_trade(msg, followed)
                    if not t:
                        continue
                    try:
                        store_trade(t)
                        sig = det.add(t, now)
                        if sig and store_signal(sig["mint"], sig["first_at"], sig["wallets"],
                                                sig["total_sol"], sig["mcap_sol"]):
                            print(f"[detector] SIGNAL {sig['mint']} — {len(sig['wallets'])} followed wallets bought "
                                  f"within {(now - sig['first_at']).total_seconds() / 60:.0f} min, "
                                  f"{sig['total_sol']:.2f} SOL, mcap {sig['mcap_sol'] or 0:.0f} SOL", flush=True)
                    except Exception as e:
                        db.safe_rollback()
                        print(f"[detector] store failed: {type(e).__name__}: {e}", flush=True)
                    if msgs % 500 == 0:
                        det.prune(now)
        except Exception as e:
            print(f"[detector] connection ended: {type(e).__name__}: {e}", flush=True)
        if msgs > MAX_MSGS_PER_DAY:
            tomorrow = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
            await asyncio.sleep(max(60.0, (tomorrow - datetime.now(timezone.utc)).total_seconds()))
            continue
        await asyncio.sleep(max(RECONNECT_MIN_S, RECONNECT_MIN_S * 3 - (time.monotonic() - started)))


# ── Report: our signals vs the channel's calls ──────────────────────────────────

def report(since: str) -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.mint, s.signal_at, s.n_wallets, s.mcap_sol,
                   (SELECT min(c.created_at) FROM calls c JOIN tokens t ON t.id = c.token_id
                     JOIN channels ch ON ch.id = c.channel_id
                     WHERE t.mint_address = s.mint AND ch.handle ILIKE '%%solwhaletrending%%'),
                   (SELECT c.mcap_at_call FROM calls c JOIN tokens t ON t.id = c.token_id
                     JOIN channels ch ON ch.id = c.channel_id
                     WHERE t.mint_address = s.mint AND ch.handle ILIKE '%%solwhaletrending%%'
                     ORDER BY c.created_at LIMIT 1),
                   (SELECT min(c.created_at) FROM calls c JOIN tokens t ON t.id = c.token_id
                     WHERE t.mint_address = s.mint)
            FROM followed_signals s
            WHERE s.signal_at >= %s
            ORDER BY s.signal_at
            """, (since,))
        sigs = cur.fetchall()
        cur.execute(
            """
            SELECT count(*), count(*) FILTER (WHERE EXISTS (
                       SELECT 1 FROM followed_signals s WHERE s.mint = t.mint_address))
            FROM followed_at_call f JOIN calls c ON c.id = f.call_id JOIN tokens t ON t.id = c.token_id
            WHERE f.rpc_ok AND f.n_holding >= %s AND f.observed_at >= %s
            """, (MIN_WALLETS, since))
        lane_calls, lane_seen = cur.fetchone()
    print(f"followed-wallet signals since {since}: {len(sigs)}")
    if not sigs:
        return
    with_swt = [r for r in sigs if r[4] is not None]
    leads = [(r[4] - r[1]).total_seconds() / 60 for r in with_swt]
    print(f"  also called by solwhaletrending: {len(with_swt)} ({len(with_swt) / len(sigs) * 100:.0f}%); "
          f"called by any channel: {sum(1 for r in sigs if r[6] is not None)}")
    if leads:
        leads.sort()
        print(f"  our signal came BEFORE the channel call on {sum(1 for x in leads if x > 0)} of {len(leads)}; "
              f"median lead {leads[len(leads) // 2]:+.1f} min (positive = we were earlier)")
    print(f"  channel calls with {MIN_WALLETS}+ followed wallets at the call (the live lane's trades): "
          f"{lane_calls}; of those our detector also signalled: {lane_seen}")
    print(f"\n  {'signal (UTC)':16s} {'mint':12s} {'wallets':>7s} {'mcap SOL':>9s} {'swt call':>9s} {'lead min':>9s}")
    for mint, at, n, mcap, swt_at, swt_mcap, any_at in sigs[-40:]:
        lead = f"{(swt_at - at).total_seconds() / 60:+.1f}" if swt_at else ""
        print(f"  {at:%m-%d %H:%M:%S} {mint[:10] + '..':12s} {n:7d} {float(mcap or 0):9.0f} "
              f"{('yes' if swt_at else 'no'):>9s} {lead:>9s}")


if __name__ == "__main__":
    if "--report" in sys.argv:
        i = sys.argv.index("--since") if "--since" in sys.argv else -1
        report(sys.argv[i + 1] if i > 0 else "2026-11-01")
    else:
        asyncio.run(run())
