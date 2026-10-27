"""
holder_snapshots.py — who was selling when we got stopped out?

WHY (2026-10-27). At the moment of a stop, a coin about to run and a coin about to die
look the same on everything recorded so far (price shape, mcap, liquidity at the call,
holders, bundles, snipers, age, score, followed-wallet buys). Pool liquidity cannot help on
pump.fun coins: in a constant-product pool whose LP cannot be pulled, the SOL reserve is
fixed by the price. What does differ is WHO sold: a dump is the biggest holders (dev,
insiders, bundles) exiting, often with more left to sell; a shakeout is small holders
panicking while the big ones sit still.

WHAT. For every solwhaletrending qsim position, the coin's top 20 token holders
(getTokenLargestAccounts + their owners, data_fetcher.fetch_top_holders_helius, 2 RPC
calls) at three moments:
  entry    within ENTRY_WINDOW_S of qsim's entry
  exit     within EXIT_WINDOW_S of qsim's exit
  exit10   10-14 minutes after qsim's exit
Each holder is tagged on_curve: a real wallet. Pools, bonding curves and program accounts
are off-curve addresses and are kept but tagged, so analyses can leave them out. Supply
(getTokenSupply, cached) is stored so holdings read as a share of the coin.

Research only: nothing here trades. Hard daily ceiling HOLDER_SNAP_MAX_CALLS_PER_DAY
(printed at start). A holder that drops out of the top 20 between two snapshots is not
seen in the later one; analyses treat it as having sold (an upper bound).

Run:     pm2 start holder_snapshots.py --name holder-snapshots --interpreter <venv python>
Report:  python3 holder_snapshots.py --report [--since 2026-10-28]
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))

import data_fetcher  # noqa: E402
import db  # noqa: E402

PASS_SECS = float(os.getenv("HOLDER_SNAP_PASS_SECS", "30"))
MAX_CALLS_PER_DAY = int(os.getenv("HOLDER_SNAP_MAX_CALLS_PER_DAY", "2000"))
ENTRY_WINDOW_S = 180
EXIT_WINDOW_S = 180
AFTER_EXIT_S = (600, 840)
TOP_N = 20

_day = None
_calls_today = 0
_capped_noted = False


def ensure_table() -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS holder_snapshots (
                call_id    integer NOT NULL,
                mint       text NOT NULL,
                phase      text NOT NULL,          -- entry | exit | exit10
                taken_at   timestamptz NOT NULL DEFAULT now(),
                supply_ui  numeric,                -- total supply in coin units
                holders    jsonb,                  -- [{wallet, ui_amount, rank, on_curve}]
                ok         boolean NOT NULL,
                PRIMARY KEY (call_id, phase)
            )
            """
        )
        conn.commit()


def due_snapshots() -> list[tuple[int, str, str]]:
    """[(call_id, mint, phase)] that are due now and not yet taken."""
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT qp.call_id, t.mint_address, x.phase
            FROM qsim_positions qp
            JOIN tokens t ON t.id = qp.token_id
            CROSS JOIN LATERAL (VALUES
                ('entry',  qp.entry_time >= now() - make_interval(secs => %s)),
                ('exit',   qp.exit_time IS NOT NULL AND qp.exit_time >= now() - make_interval(secs => %s)),
                ('exit10', qp.exit_time IS NOT NULL
                           AND qp.exit_time <= now() - make_interval(secs => %s)
                           AND qp.exit_time >= now() - make_interval(secs => %s))
            ) AS x(phase, due)
            WHERE qp.channel_handle = 'solwhaletrending'
              AND x.due
              AND t.mint_address NOT LIKE 'UNKNOWN:%%' AND t.mint_address NOT LIKE 'INFERRED:%%'
              AND NOT EXISTS (SELECT 1 FROM holder_snapshots h
                              WHERE h.call_id = qp.call_id AND h.phase = x.phase)
            ORDER BY CASE x.phase WHEN 'exit' THEN 0 WHEN 'entry' THEN 1 ELSE 2 END
            """, (ENTRY_WINDOW_S, EXIT_WINDOW_S, AFTER_EXIT_S[0], AFTER_EXIT_S[1]))
        return cur.fetchall()


def _on_curve(addr: str) -> bool | None:
    try:
        from solders.pubkey import Pubkey
        return Pubkey.from_string(addr).is_on_curve()
    except Exception:
        return None


def take(call_id: int, mint: str, phase: str) -> int:
    """Take and store one snapshot. Returns RPC calls used (an estimate: 2-3)."""
    calls = 2
    holders = data_fetcher.fetch_top_holders_helius(mint, TOP_N)
    supply_ui = None
    try:
        before = mint in data_fetcher._supply_cache
        amount, decimals = data_fetcher._fetch_token_supply_helius(mint)
        if not before:
            calls += 1
        if amount is not None and decimals is not None:
            supply_ui = amount / (10 ** decimals)
    except Exception:
        pass
    rows = [{**h, "on_curve": _on_curve(h["wallet"])} for h in holders]
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO holder_snapshots (call_id, mint, phase, supply_ui, holders, ok)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (call_id, phase) DO NOTHING
            """, (call_id, mint, phase, supply_ui, json.dumps(rows), bool(rows)))
        conn.commit()
    return calls


def run() -> None:
    global _day, _calls_today, _capped_noted
    ensure_table()
    print(f"[holder_snapshots] every {PASS_SECS:g}s: top {TOP_N} holders of each solwhaletrending "
          f"qsim coin at entry, exit and 10 min after exit — ceiling {MAX_CALLS_PER_DAY:,} RPC "
          f"calls/day (~{MAX_CALLS_PER_DAY * 30:,}/month), 2-3 calls per snapshot", flush=True)
    tally = defaultdict(int)
    last_tally = time.monotonic()
    while True:
        started = time.monotonic()
        today = datetime.now(timezone.utc).date()
        if today != _day:
            _day, _calls_today, _capped_noted = today, 0, False
        try:
            for call_id, mint, phase in due_snapshots():
                if _calls_today + 3 > MAX_CALLS_PER_DAY:
                    if not _capped_noted:
                        print(f"[holder_snapshots] daily ceiling {MAX_CALLS_PER_DAY} reached — "
                              f"no more snapshots today", flush=True)
                        _capped_noted = True
                    break
                try:
                    _calls_today += take(call_id, mint, phase)
                    tally[phase] += 1
                except Exception as e:
                    db.safe_rollback()
                    tally["failed"] += 1
                    print(f"[holder_snapshots] call_id={call_id} {phase} failed: {type(e).__name__}: {e}")
        except Exception as e:
            db.safe_rollback()
            print(f"[holder_snapshots] pass failed: {type(e).__name__}: {e}")
        if time.monotonic() - last_tally >= 1800:
            print(f"[holder_snapshots] 30m: " + " ".join(f"{k}={v}" for k, v in sorted(tally.items()))
                  + f" calls_today={_calls_today}", flush=True)
            tally.clear()
            last_tally = time.monotonic()
        time.sleep(max(1.0, PASS_SECS - (time.monotonic() - started)))


# ── Report ──────────────────────────────────────────────────────────────────────

def _wallets(snap) -> dict[str, float]:
    """Real wallets (on-curve) -> holding, from a stored snapshot."""
    out = {}
    for h in snap or []:
        if h.get("on_curve") and h.get("ui_amount") is not None:
            out[h["wallet"]] = float(h["ui_amount"])
    return out


def report(since: str) -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT qp.call_id, qp.exit_time, qp.exit_reason,
                   max(h.supply_ui) AS supply,
                   jsonb_object_agg(h.phase, h.holders) AS snaps,
                   (SELECT max(ob.real_mult) FROM qsim_quote_observations ob
                     WHERE ob.call_id = qp.call_id AND ob.observed_at > qp.exit_time
                       AND ob.observed_at <= qp.exit_time + interval '24 hours'
                       AND NOT ob.rate_limited AND ob.real_mult IS NOT NULL) AS post_peak
            FROM qsim_positions qp
            JOIN holder_snapshots h ON h.call_id = qp.call_id AND h.ok
            WHERE qp.status = 'closed' AND qp.channel_handle = 'solwhaletrending'
              AND qp.exit_reason = 'hard_stop' AND qp.entry_time >= %s
              AND qp.exit_time < now() - interval '24 hours'
            GROUP BY qp.call_id, qp.exit_time, qp.exit_reason
            """, (since,))
        rows = cur.fetchall()
    stops = []
    for call_id, _exit, _reason, supply, snaps, post_peak in rows:
        if not supply or "entry" not in snaps or "exit" not in snaps:
            continue
        supply = float(supply)
        e, x = _wallets(snaps["entry"]), _wallets(snaps["exit"])
        x10 = _wallets(snaps.get("exit10")) if "exit10" in snaps else None
        top = sorted(e.items(), key=lambda kv: -kv[1])[:10]
        sold_into_drop = sum(max(0.0, bal - x.get(w, 0.0)) for w, bal in top) / supply * 100
        held_at_exit = sum(x.get(w, 0.0) for w, _ in top) / supply * 100
        sold_after = None
        if x10 is not None:
            xt = sorted(x.items(), key=lambda kv: -kv[1])[:10]
            sold_after = sum(max(0.0, bal - x10.get(w, 0.0)) for w, bal in xt) / supply * 100
        stops.append({"back": post_peak is not None and float(post_peak) >= 2.0,
                      "sold_into_drop": sold_into_drop, "held_at_exit": held_at_exit,
                      "sold_after": sold_after})
    if not stops:
        print("no stopped positions with entry and exit snapshots yet (needs 24h after exit)")
        return
    base = sum(s["back"] for s in stops) / len(stops) * 100
    print(f"hard stops since {since} with holder snapshots: {len(stops)}; came back to 2x within 24h: "
          f"{base:.0f}% overall")
    for key, title, edges in (
        ("sold_into_drop", "top-10 wallets (at entry) SOLD between entry and our stop, % of supply",
         [(0.5, "under 0.5%"), (2, "0.5-2%"), (5, "2-5%"), (1e9, "5%+")]),
        ("held_at_exit", "top-10 wallets (at entry) STILL HOLD at our stop, % of supply",
         [(5, "under 5%"), (10, "5-10%"), (20, "10-20%"), (1e9, "20%+")]),
        ("sold_after", "top-10 wallets (at our stop) sold in the next 10 min, % of supply",
         [(0.5, "under 0.5%"), (2, "0.5-2%"), (5, "2-5%"), (1e9, "5%+")]),
    ):
        groups = defaultdict(list)
        for s in stops:
            v = s[key]
            if v is None:
                continue
            for hi, label in edges:
                if v < hi:
                    groups[label].append(s)
                    break
        print(f"\n{title}:")
        for _, label in edges:
            g = groups.get(label, [])
            if g:
                print(f"  {label:12s} {len(g):4d} stops   came back {sum(s['back'] for s in g) / len(g) * 100:5.1f}%")
    print(f"\nA value is useful only if its 'came back' rate is far from {base:.0f}% on a decent number "
          f"of stops — and it needs ~25-30% to make holding (or buying back) pay.")


def _clones(values: list[float], tol: float = 0.10, floor: float = 0.001) -> int:
    """Largest group of holdings (each >= floor of supply) within +-tol of each other —
    many wallets holding near-identical amounts is how one operator's wallets look."""
    vals = sorted(v for v in values if v >= floor)
    best = 0
    j = 0
    for i in range(len(vals)):
        while vals[i] > vals[j] * (1 + 2 * tol):
            j += 1
        best = max(best, i - j + 1)
    return best


def entry_report(since: str) -> None:
    """2026-10-28, WANSULEK: at our entry one wallet held 89% of supply; 8 minutes later it
    was spread over ~15 wallets (2-5.6% each) and 10 minutes after that consolidated into one
    seller (35%) who dripped it out and dumped. This splits EVERY qsim trade by what the ENTRY
    snapshot showed (real wallets only; pools/programs excluded): the largest wallet's share,
    how many wallets held 2%+, and the largest group of near-identical holdings."""
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT qp.entry_time, h.supply_ui, h.holders,
                   qp.exit_reason LIKE '%%bank%%',
                   CASE WHEN qp.exit_reason LIKE '%%bank%%' THEN LEAST(qp.sol_out / qp.sol_in, 2.1)
                        ELSE qp.sol_out / qp.sol_in END,
                   (SELECT ob.real_mult FROM qsim_quote_observations ob
                     WHERE ob.call_id = qp.call_id AND ob.real_mult IS NOT NULL AND NOT ob.rate_limited
                       AND ob.observed_at <= qp.exit_time + interval '24 hours'
                     ORDER BY ob.observed_at DESC LIMIT 1)
            FROM qsim_positions qp
            JOIN holder_snapshots h ON h.call_id = qp.call_id AND h.phase = 'entry' AND h.ok
            WHERE qp.status = 'closed' AND qp.channel_handle = 'solwhaletrending'
              AND qp.sol_in > 0 AND qp.entry_time >= %s
            ORDER BY qp.entry_time
            """, (since,))
        rows = cur.fetchall()
    trades = []
    for entry, supply, holders, bank, x, last in rows:
        if not supply:
            continue
        shares = [float(hd["ui_amount"]) / float(supply) for hd in (holders or [])
                  if hd.get("on_curve") and hd.get("ui_amount") is not None]
        trades.append({"entry": entry, "bank": bool(bank), "x": float(x),
                       "dead": last is not None and float(last) < 0.10,
                       "top1": max(shares) * 100 if shares else 0.0,
                       "n2": sum(1 for s in shares if s >= 0.02),
                       "clones": _clones(shares)})
    if not trades:
        print("no closed qsim trades with an entry holder snapshot yet")
        return
    mid = trades[len(trades) // 2]["entry"]

    def show(title, key, edges):
        print(f"\n{title}")
        print(f"  {'value':14s} {'trades':>6s} {'banks':>5s} {'bank %':>6s} {'per tr':>7s} {'1st half':>8s} "
              f"{'2nd half':>8s} {'dead in 24h':>11s}")
        groups = defaultdict(list)
        for t in trades:
            for hi, label in edges:
                if t[key] < hi:
                    groups[label].append(t)
                    break
        for _, label in edges:
            g = groups.get(label, [])
            if not g:
                continue
            net = sum(t["x"] - 1 for t in g)
            h1 = sum(t["x"] - 1 for t in g if t["entry"] < mid)
            banks = sum(t["bank"] for t in g)
            print(f"  {label:14s} {len(g):6d} {banks:5d} {banks / len(g) * 100:5.1f}% {net / len(g) * 100:+6.1f}% "
                  f"{h1:+8.2f} {net - h1:+8.2f} {sum(t['dead'] for t in g) / len(g) * 100:10.0f}%")

    print(f"solwhaletrending qsim trades since {since} with an entry holder snapshot: {len(trades)} "
          f"(banks capped at 2.1x; 'dead' = last look within 24h of exit under 0.10x of entry)")
    show("LARGEST single wallet at our entry, % of supply:", "top1",
         [(5, "under 5%"), (10, "5-10%"), (20, "10-20%"), (50, "20-50%"), (101, "50%+")])
    show("WALLETS holding 2%+ of supply at our entry:", "n2",
         [(1, "0"), (2, "1"), (4, "2-3"), (7, "4-6"), (1e9, "7+")])
    show("CLONES: largest group of wallets (0.1%+) holding within +-10% of each other:", "clones",
         [(3, "under 3"), (5, "3-4"), (8, "5-7"), (1e9, "8+")])


if __name__ == "__main__":
    if "--entry-report" in sys.argv:
        i = sys.argv.index("--since") if "--since" in sys.argv else -1
        entry_report(sys.argv[i + 1] if i > 0 else "2026-10-27")
    elif "--report" in sys.argv:
        i = sys.argv.index("--since") if "--since" in sys.argv else -1
        report(sys.argv[i + 1] if i > 0 else "2026-10-28")
    else:
        run()
