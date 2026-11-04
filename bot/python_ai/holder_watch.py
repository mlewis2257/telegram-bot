"""
holder_watch.py — do big holders visibly start selling BEFORE a rug, early enough to get out?

WHY (2026-11-04). On 2026-11-04 solearlytrending would have been positive without its ~20
full rugs (1.9x -> 0.04x between two quotes). Rugs and runners look the same at entry, so
entry filters that remove rugs also remove banks. A rug still needs someone to sell first:
if the top holders start unloading a minute or more before the drain, selling then (around
the price at that moment) saves most of the stake without touching the runners whose big
holders sit still. holder_snapshots.py only looks at entry / exit / exit+10 min, so it
cannot see this. The stop side is closed (holder_snapshots --report, 2026-11-04).

WHAT. While a qsim position on HOLDER_WATCH_CHANNELS is OPEN (up to HOLDER_WATCH_MAX_MIN
minutes), every HOLDER_WATCH_SECS: the coin's top 20 token accounts (getTokenLargestAccounts,
1 call) with their owner wallets (getMultipleAccounts, only for accounts not seen before on
this coin — owners never change) and supply (cached). Stored per snapshot in holder_watch.
Owners are tagged on_curve (real wallet) or not (bonding curve / pool / program).

Research only: nothing here trades. Hard daily ceiling HOLDER_WATCH_MAX_CALLS_PER_DAY,
printed at start; when reached, no more snapshots until UTC midnight.

REPORT (--report): the "sold" signal = how much of the supply the top-10 real wallets of the
FIRST watch snapshot have shed since (a wallet that drops out of the top 20 counts as fully
sold — an upper bound). For each threshold it shows, per outcome (rug / stop / bank / other):
how often it fired before our exit and at what price, and the P&L of the rule "sell at the
first quote after it fires" against what qsim actually did. A threshold is worth anything only
if it fires on rugs well BEFORE the drain and rarely on banks.

Run:     pm2 start holder_watch.py --name holder-watch --interpreter <venv python>
Report:  python3 holder_watch.py --report [--since 2026-11-05]
"""

from __future__ import annotations

import json
import os
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

import data_fetcher  # noqa: E402
import db  # noqa: E402

WATCH_SECS = float(os.getenv("HOLDER_WATCH_SECS", "45"))
MAX_MIN = float(os.getenv("HOLDER_WATCH_MAX_MIN", "30"))
MAX_CALLS_PER_DAY = int(os.getenv("HOLDER_WATCH_MAX_CALLS_PER_DAY", "5000"))
CHANNELS = [c.strip().lstrip("@") for c in
            os.getenv("HOLDER_WATCH_CHANNELS", "solearlytrending,solwhaletrending").split(",") if c.strip()]
TOP_N = 20

_owner_cache: dict[str, dict[str, tuple[str, bool | None]]] = {}   # mint -> token acct -> (owner, on_curve)
_last_snap: dict[int, float] = {}                                   # call_id -> monotonic time
_day = None
_calls_today = 0
_capped_noted = False


def ensure_table() -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS holder_watch (
                call_id    integer NOT NULL,
                taken_at   timestamptz NOT NULL DEFAULT now(),
                supply_ui  numeric,
                holders    jsonb,          -- [{wallet, ui_amount, on_curve}], largest first
                PRIMARY KEY (call_id, taken_at)
            )""")
        conn.commit()


def _on_curve(addr: str) -> bool | None:
    try:
        from solders.pubkey import Pubkey
        return Pubkey.from_string(addr).is_on_curve()
    except Exception:
        return None


def snapshot(mint: str) -> tuple[list[dict], float | None, int]:
    """(holders, supply_ui, rpc calls used). holders == [] on failure."""
    calls = 1
    r = data_fetcher._rpc_post({"jsonrpc": "2.0", "id": 1, "method": "getTokenLargestAccounts",
                                "params": [mint]})
    if not r or r.status_code != 200:
        return [], None, calls
    val = (((r.json().get("result") or {}).get("value")) or [])[:TOP_N]
    if not val:
        return [], None, calls
    known = _owner_cache.setdefault(mint, {})
    new = [v["address"] for v in val if v["address"] not in known]
    if new:
        calls += 1
        r2 = data_fetcher._rpc_post({"jsonrpc": "2.0", "id": 1, "method": "getMultipleAccounts",
                                     "params": [new, {"encoding": "jsonParsed"}]})
        if r2 is not None and r2.status_code == 200:
            for acct, info in zip(new, ((r2.json().get("result") or {}).get("value")) or []):
                owner = (((info or {}).get("data") or {}).get("parsed") or {}).get("info", {}).get("owner")
                if owner:
                    known[acct] = (owner, _on_curve(owner))
    supply_ui = None
    before = mint in data_fetcher._supply_cache
    try:
        amount, decimals = data_fetcher._fetch_token_supply_helius(mint)
        if amount is not None and decimals is not None:
            supply_ui = amount / (10 ** decimals)
    except Exception:
        pass
    if not before:
        calls += 1
    rows = []
    for v in val:
        own = known.get(v["address"])
        if own:
            rows.append({"wallet": own[0], "ui_amount": v.get("uiAmount"), "on_curve": own[1]})
    return rows, supply_ui, calls


def due() -> list[tuple[int, str]]:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT qp.call_id, t.mint_address
            FROM qsim_positions qp JOIN tokens t ON t.id = qp.token_id
            WHERE qp.status = 'open' AND qp.channel_handle = ANY(%s)
              AND qp.entry_time >= now() - make_interval(mins => %s)
              AND t.mint_address NOT LIKE 'UNKNOWN:%%' AND t.mint_address NOT LIKE 'INFERRED:%%'
            ORDER BY qp.entry_time""", (CHANNELS, int(MAX_MIN)))
        rows = cur.fetchall()
    now = time.monotonic()
    open_ids = {cid for cid, _ in rows}
    for cid in list(_last_snap):
        if cid not in open_ids:
            _last_snap.pop(cid, None)
    # most overdue first, never-seen first of all
    rows.sort(key=lambda r: _last_snap.get(r[0], 0.0))
    return [(cid, m) for cid, m in rows if now - _last_snap.get(cid, 0.0) >= WATCH_SECS]


def run() -> None:
    global _day, _calls_today, _capped_noted
    ensure_table()
    print(f"[holder_watch] every {WATCH_SECS:g}s while a {','.join(CHANNELS)} qsim position is open "
          f"(max {MAX_MIN:g} min): top {TOP_N} holders, 1-3 RPC calls each — ceiling "
          f"{MAX_CALLS_PER_DAY:,} calls/day (~{MAX_CALLS_PER_DAY * 30:,}/month). Record-only.", flush=True)
    tally = defaultdict(int)
    last_tally = time.monotonic()
    while True:
        started = time.monotonic()
        today = datetime.now(timezone.utc).date()
        if today != _day:
            _day, _calls_today, _capped_noted = today, 0, False
            _owner_cache.clear()
        try:
            for call_id, mint in due():
                if _calls_today + 3 > MAX_CALLS_PER_DAY:
                    if not _capped_noted:
                        print(f"[holder_watch] daily ceiling {MAX_CALLS_PER_DAY:,} reached — "
                              f"no more snapshots until UTC midnight", flush=True)
                        _capped_noted = True
                    break
                _last_snap[call_id] = time.monotonic()
                try:
                    rows, supply_ui, used = snapshot(mint)
                    _calls_today += used
                    if not rows:
                        tally["empty"] += 1
                        continue
                    conn = db.get_conn()
                    with conn.cursor() as cur:
                        cur.execute("INSERT INTO holder_watch (call_id, supply_ui, holders) VALUES (%s,%s,%s) "
                                    "ON CONFLICT DO NOTHING", (call_id, supply_ui, json.dumps(rows)))
                    conn.commit()
                    tally["snaps"] += 1
                except Exception as e:
                    db.safe_rollback()
                    tally["failed"] += 1
                    print(f"[holder_watch] call_id={call_id} failed: {type(e).__name__}: {e}")
        except Exception as e:
            db.safe_rollback()
            print(f"[holder_watch] pass failed: {type(e).__name__}: {e}")
        if time.monotonic() - last_tally >= 1800:
            print("[holder_watch] 30m: " + " ".join(f"{k}={v}" for k, v in sorted(tally.items()))
                  + f" calls_today={_calls_today}", flush=True)
            tally.clear()
            last_tally = time.monotonic()
        time.sleep(max(1.0, 5.0 - (time.monotonic() - started)))


# ── Report ──────────────────────────────────────────────────────────────────────

THRESHOLDS = (1.0, 2.0, 3.0, 5.0, 8.0)   # % of supply shed by the first snapshot's top-10 real wallets


def _real(snap) -> dict[str, float]:
    return {h["wallet"]: float(h["ui_amount"]) for h in snap or []
            if h.get("on_curve") and h.get("ui_amount") is not None}


def report(since: str) -> None:
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT qp.call_id, qp.channel_handle, t.symbol, qp.sol_in, qp.sol_out, qp.exit_reason,
                   qp.entry_time, qp.exit_time
            FROM qsim_positions qp JOIN tokens t ON t.id = qp.token_id
            WHERE qp.status = 'closed' AND qp.sol_in > 0 AND qp.entry_time >= %s
              AND EXISTS (SELECT 1 FROM holder_watch w WHERE w.call_id = qp.call_id)""", (since,))
        pos = {r[0]: dict(zip(("call_id", "channel", "symbol", "sol_in", "sol_out", "exit_reason",
                               "entry", "exit"), r)) for r in cur.fetchall()}
        if not pos:
            print(f"no closed qsim positions with holder_watch snapshots since {since}")
            return
        ids = list(pos)
        cur.execute("SELECT call_id, taken_at, supply_ui, holders FROM holder_watch "
                    "WHERE call_id = ANY(%s) ORDER BY call_id, taken_at", (ids,))
        snaps = defaultdict(list)
        for cid, at, sup, h in cur.fetchall():
            snaps[cid].append((at, float(sup) if sup else None, h))
        cur.execute("SELECT call_id, observed_at, real_mult FROM qsim_quote_observations "
                    "WHERE call_id = ANY(%s) AND real_mult IS NOT NULL AND real_mult > 0 "
                    "ORDER BY call_id, observed_at", (ids,))
        obs = defaultdict(list)
        for cid, at, m in cur.fetchall():
            obs[cid].append((at, float(m)))

    def outcome(p):
        x = float(p["sol_out"] or 0) / float(p["sol_in"])
        if "bank" in (p["exit_reason"] or ""):
            return "bank", x
        if x < 0.30:
            return "rug", x
        if "stop" in (p["exit_reason"] or ""):
            return "stop", x
        return "other", x

    def sold_path(cid):
        """[(time, % of supply shed by the first snapshot's top-10 real wallets)]"""
        s = snaps[cid]
        sup = next((x[1] for x in s if x[1]), None)
        if not sup or len(s) < 2:
            return []
        first = _real(s[0][2])
        ref = dict(sorted(first.items(), key=lambda kv: -kv[1])[:10])
        out = []
        for at, _, h in s[1:]:
            cur_ = _real(h)
            shed = sum(max(0.0, amt - cur_.get(w, 0.0)) for w, amt in ref.items())
            out.append((at, shed / sup * 100))
        return out

    def mult_after(cid, t):
        for at, m in obs[cid]:
            if at > t:
                return at, m
        return None, None

    n_by = defaultdict(int)
    for p in pos.values():
        n_by[outcome(p)[0]] += 1
    n_snap = [len(snaps[c]) for c in pos]
    print(f"closed qsim positions since {since} with in-trade holder snapshots: {len(pos)} "
          f"(" + ", ".join(f"{k} {v}" for k, v in sorted(n_by.items())) + f"); snapshots per position "
          f"median {statistics.median(n_snap):.0f}\n")
    print("Signal = the top-10 real wallets of the first in-trade snapshot have shed X% of the supply.\n"
          "'fired' = before our exit. 'at' = qsim's next quote after the snapshot (the price we would\n"
          "get if we sold on it). delta = P&L of 'sell at that quote' minus what qsim actually did, in SOL.\n")
    for th in THRESHOLDS:
        rows = defaultdict(lambda: {"n": 0, "fired": 0, "at": [], "delta": 0.0})
        for cid, p in pos.items():
            kind, x = outcome(p)
            r = rows[kind]
            r["n"] += 1
            hit = next(((at, v) for at, v in sold_path(cid) if v >= th and (p["exit"] is None or at < p["exit"])),
                       None)
            if not hit:
                continue
            q_at, m = mult_after(cid, hit[0])
            if m is None or (p["exit"] is not None and q_at >= p["exit"]):
                continue   # no quote between the signal and our real exit: nothing to change
            r["fired"] += 1
            r["at"].append(m)
            r["delta"] += float(p["sol_in"]) * (m - x)
        tot = sum(r["delta"] for r in rows.values())
        print(f"  shed >= {th:g}% of supply:   net delta {tot:+.4f} SOL")
        for kind in ("rug", "stop", "bank", "other"):
            r = rows.get(kind)
            if not r or not r["n"]:
                continue
            med = f"{statistics.median(r['at']):.2f}x" if r["at"] else "-"
            print(f"    {kind:6s} {r['n']:4d} positions   fired {r['fired']:4d} "
                  f"({r['fired'] / r['n'] * 100:5.1f}%)   median price when it fired {med:>6s}   "
                  f"delta {r['delta']:+.4f}")
    print("\nWorth building into exits only if a threshold has a clearly positive net delta, fires on most\n"
          "rugs at a price well above where they ended, and rarely fires on banks. Check it again on a\n"
          "second week before trusting it.")


if __name__ == "__main__":
    if "--report" in sys.argv:
        i = sys.argv.index("--since") if "--since" in sys.argv else -1
        report(sys.argv[i + 1] if i > 0 else "2026-11-05")
    else:
        run()
