"""
launcher.py — transparent pump.fun launches, one at a time, each approved by hand.

STATUS (2026-11-03): built for a 20-launch trial with ~0.3 SOL. DRY-RUN BY DEFAULT.

RULES (agreed, enforced where code can): one visible dev wallet (LAUNCH_WALLET_PRIVATE_KEY,
never the trading wallet); no bundles, no extra wallets, no wash trading; no "official" /
endorsement claims for real people or brands (names containing 'official' are refused); every
live launch typed-confirmed; hard caps below.

COMMANDS (run on the VPS from bot/launcher/):
  python3 launcher.py launch --name "Kim Coin" --symbol KIM --image kim.png \
        [--twitter URL] [--website URL] [--description "..."] [--note "viral tweet"] [--live]
      Without --live: validates, prints the plan and costs, builds the real create transaction
      through PumpPortal and SIMULATES it on-chain (nothing is sent, nothing uploaded).
      With --live: uploads metadata, sends the create + dev buy, then runs the exit rules.
  python3 launcher.py paper [--mint MINT]
      Exit-rule rehearsal on a REAL coin: pretend we were the dev of the next new pump.fun coin
      (or MINT), buy LAUNCH_DEV_BUY_SOL at curve start, poll its bonding curve and log what the
      rules would sell, when, for how much. No transactions. Approximate (our buy isn't in the curve).
  python3 launcher.py report

EXIT RULES (env, defaults in brackets):
  LAUNCH_NO_TRACTION_SECS [120]  if others have put in < LAUNCH_TRACTION_SOL [0.5] by then: sell all
  LAUNCH_STOP_MULT [0.6]         bag worth < 0.6x of what we paid, before any step sold: sell all
  LAUNCH_SELL_STEPS ["2:0.5,3:0.3"]  at 2x bag value sell 50% of the original bag, at 3x 30% more
  LAUNCH_TRAIL_PCT [0.40]        after the first step: remaining bag sold if value drops 40% from peak
  LAUNCH_MAX_HOLD_MIN [120]      sell whatever is left
  graduation (curve complete)    sell whatever is left

CAPS (live only): LAUNCH_ENABLED=true required; LAUNCH_DEV_BUY_SOL [0.12]; LAUNCH_MIN_WALLET_SOL
[0.15] (refuse to launch below it); LAUNCH_MAX_PER_DAY [3]; LAUNCH_MAX_TOTAL [20].

RPC: one getAccountInfo per poll — every 3 s for 2 min, 10 s to 30 min, 60 s after — about
300 per launch, hard ceiling LAUNCH_MAX_RPC_PER_LAUNCH [400], printed at start. With 3 launches a
day: <= 1,200/day, ~8,400/week, ~36,000/month. Dry-run: 2 calls. Paper: same as a launch.
Fees: PumpPortal 0.5% per trade (create's dev buy and each sell), pump.fun ~1%, network + tips.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent / "python_ai"))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(HERE.parent / "python_ai" / ".env")

import requests  # noqa: E402
from solders.keypair import Keypair  # noqa: E402
from solders.pubkey import Pubkey  # noqa: E402
from solders.transaction import VersionedTransaction  # noqa: E402

import data_fetcher  # noqa: E402
import db  # noqa: E402

PUMP_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
TRADE_LOCAL = "https://pumpportal.fun/api/trade-local"
LAMPORTS = 1_000_000_000
TOKEN_DECIMALS = 6
V_SOL0, V_TOK0 = 30 * LAMPORTS, 1_073_000_000 * 10 ** TOKEN_DECIMALS   # curve start
FEE = 0.015                       # pump.fun ~1% + PumpPortal 0.5%, each way


def env_f(k, d): return float(os.getenv(k, str(d)))


DEV_BUY = env_f("LAUNCH_DEV_BUY_SOL", 0.12)
MIN_WALLET = env_f("LAUNCH_MIN_WALLET_SOL", 0.15)
MAX_PER_DAY = int(env_f("LAUNCH_MAX_PER_DAY", 3))
MAX_TOTAL = int(env_f("LAUNCH_MAX_TOTAL", 20))
NO_TRACTION_S = env_f("LAUNCH_NO_TRACTION_SECS", 120)
TRACTION_SOL = env_f("LAUNCH_TRACTION_SOL", 0.5)
STOP_MULT = env_f("LAUNCH_STOP_MULT", 0.6)
TRAIL = env_f("LAUNCH_TRAIL_PCT", 0.40)
MAX_HOLD_MIN = env_f("LAUNCH_MAX_HOLD_MIN", 120)
STEPS = sorted((float(a), float(b)) for a, b in
               (s.split(":") for s in os.getenv("LAUNCH_SELL_STEPS", "2:0.5,3:0.3").split(",") if s))
MAX_RPC = int(env_f("LAUNCH_MAX_RPC_PER_LAUNCH", 400))
PRIORITY_FEE = env_f("LAUNCH_PRIORITY_FEE_SOL", 0.0005)

_rpc_calls = 0


class Ceiling(Exception):
    pass


def rpc(method: str, params):
    global _rpc_calls
    if _rpc_calls >= MAX_RPC:
        raise Ceiling()
    _rpc_calls += 1
    r = data_fetcher._rpc_post({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=20)
    if r is None or r.status_code != 200:
        return None
    j = r.json()
    if j.get("error"):
        print(f"[launcher] rpc {method} error: {j['error']}")
        return None
    return j.get("result")


# ── bonding curve ─────────────────────────────────────────────────────────────

def curve(mint: str) -> dict | None:
    pda, _ = Pubkey.find_program_address([b"bonding-curve", bytes(Pubkey.from_string(mint))],
                                         Pubkey.from_string(PUMP_PROGRAM))
    res = rpc("getAccountInfo", [str(pda), {"encoding": "base64"}])
    v = (res or {}).get("value")
    if not v:
        return None
    raw = base64.b64decode(v["data"][0])
    u = lambda a, b: int.from_bytes(raw[a:b], "little")
    return {"v_tok": u(8, 16), "v_sol": u(16, 24), "real_sol": u(32, 40) / LAMPORTS,
            "complete": bool(raw[48]) if len(raw) > 48 else False}


def buy_tokens(sol: float, v_sol=V_SOL0, v_tok=V_TOK0) -> int:
    x = int(sol * (1 - FEE) * LAMPORTS)
    return v_tok - (v_sol * v_tok) // (v_sol + x)


def sell_value(tokens: int, c: dict) -> float:
    """SOL we would receive selling `tokens` into the curve now, after fees."""
    if tokens <= 0 or not c or c["complete"]:
        return 0.0
    out = c["v_sol"] - (c["v_sol"] * c["v_tok"]) // (c["v_tok"] + tokens)
    return out / LAMPORTS * (1 - FEE)


# ── database ──────────────────────────────────────────────────────────────────

def ensure_table() -> None:
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS launches (
                id          bigserial PRIMARY KEY,
                created_at  timestamptz NOT NULL DEFAULT now(),
                mode        text NOT NULL,          -- dry | paper | live
                name        text, symbol text, note text,
                mint        text,
                dev_buy_sol numeric,
                create_sig  text,
                status      text,                   -- planned | simulated | open | closed | failed
                sol_in      numeric, sol_out numeric,
                events      jsonb NOT NULL DEFAULT '[]'::jsonb,
                rpc_calls   integer
            )""")
        conn.commit()


def save(row: dict) -> int:
    conn = db.get_conn()
    with conn.cursor() as cur:
        if row.get("id"):
            cur.execute("""UPDATE launches SET mint=%s, create_sig=%s, status=%s, sol_in=%s, sol_out=%s,
                           events=%s, rpc_calls=%s WHERE id=%s""",
                        (row.get("mint"), row.get("create_sig"), row.get("status"), row.get("sol_in"),
                         row.get("sol_out"), json.dumps(row.get("events", [])), _rpc_calls, row["id"]))
        else:
            cur.execute("""INSERT INTO launches (mode, name, symbol, note, mint, dev_buy_sol, status, events)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                        (row["mode"], row.get("name"), row.get("symbol"), row.get("note"), row.get("mint"),
                         row.get("dev_buy_sol"), row.get("status"), json.dumps(row.get("events", []))))
            row["id"] = cur.fetchone()[0]
        conn.commit()
    return row["id"]


def launches_count(since_sql: str | None) -> int:
    conn = db.get_conn()
    with conn.cursor() as cur:
        q = "SELECT count(*) FROM launches WHERE mode = 'live' AND status <> 'failed'"
        if since_sql:
            q += f" AND created_at >= {since_sql}"
        cur.execute(q)
        return cur.fetchone()[0]


# ── wallet / transactions (live only) ─────────────────────────────────────────

def wallet() -> Keypair:
    raw = os.getenv("LAUNCH_WALLET_PRIVATE_KEY")
    if not raw:
        sys.exit("LAUNCH_WALLET_PRIVATE_KEY is not set in bot/python_ai/.env (the LAUNCHER wallet, not trading)")
    kp = Keypair.from_base58_string(raw)
    trading = os.getenv("TRADING_WALLET_PRIVATE_KEY")
    if trading and Keypair.from_base58_string(trading).pubkey() == kp.pubkey():
        sys.exit("LAUNCH_WALLET_PRIVATE_KEY is the TRADING wallet — use a separate launcher wallet")
    return kp


def balance(pub: str) -> float | None:
    r = rpc("getBalance", [pub])
    return None if r is None else r["value"] / LAMPORTS


def trade_local(body: dict) -> bytes:
    r = requests.post(TRADE_LOCAL, json=body, timeout=20)
    if r.status_code != 200:
        raise RuntimeError(f"PumpPortal trade-local HTTP {r.status_code}: {r.text[:200]}")
    return r.content


def send(tx_bytes: bytes, signers: list) -> str | None:
    tx = VersionedTransaction.from_bytes(tx_bytes)
    signed = VersionedTransaction(tx.message, signers)
    res = rpc("sendTransaction", [base64.b64encode(bytes(signed)).decode(),
                                  {"encoding": "base64", "preflightCommitment": "confirmed"}])
    return res


def confirmed(sig: str, wait_s: float = 40) -> bool:
    t = time.time()
    while time.time() - t < wait_s:
        r = rpc("getSignatureStatuses", [[sig]])
        st = ((r or {}).get("value") or [None])[0]
        if st and st.get("err") is None and st.get("confirmationStatus") in ("confirmed", "finalized"):
            return True
        if st and st.get("err") is not None:
            return False
        time.sleep(2)
    return False


def our_tokens(owner: str, mint: str) -> int:
    r = rpc("getTokenAccountsByOwner", [owner, {"mint": mint}, {"encoding": "jsonParsed"}])
    return sum(int(a["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"])
               for a in (r or {}).get("value", []))


def upload_metadata(a) -> str:
    img = Path(a.image)
    jwt = os.getenv("PINATA_JWT")
    if jwt:
        h = {"Authorization": f"Bearer {jwt}"}
        r = requests.post("https://uploads.pinata.cloud/v3/files", headers=h, timeout=60,
                          data={"network": "public"}, files={"file": (img.name, img.read_bytes())})
        r.raise_for_status()
        image_uri = f"https://ipfs.io/ipfs/{r.json()['data']['cid']}"
        meta = {"name": a.name, "symbol": a.symbol, "description": a.description or "", "image": image_uri,
                "showName": True, "twitter": a.twitter or "", "website": a.website or ""}
        r = requests.post("https://uploads.pinata.cloud/v3/files", headers=h, timeout=60,
                          data={"network": "public"},
                          files={"file": ("metadata.json", json.dumps(meta).encode(), "application/json")})
        r.raise_for_status()
        return f"https://ipfs.io/ipfs/{r.json()['data']['cid']}"
    r = requests.post("https://pump.fun/api/ipfs", timeout=60,
                      data={"name": a.name, "symbol": a.symbol, "description": a.description or "",
                            "twitter": a.twitter or "", "website": a.website or "", "showName": "true"},
                      files={"file": (img.name, img.read_bytes())})
    r.raise_for_status()
    return r.json()["metadataUri"]


# ── exit rules (shared by live and paper) ─────────────────────────────────────

def run_exits(row: dict, mint: str, tokens0: int, cost: float, sell_fn, paper_shift=None) -> None:
    """Poll the curve and sell per the rules. sell_fn(fraction_of_current, reason, value) -> SOL got."""
    t0 = time.time()
    held = tokens0
    sold_steps = 0
    peak = 0.0
    got = 0.0
    last = (0.0, 0.0)
    while held > 0:
        age = time.time() - t0
        try:
            c = curve(mint)
        except Ceiling:
            # never strand a live bag on the ceiling: a small allowance for the exit sell itself
            global MAX_RPC
            MAX_RPC += 20
            out = sell_fn(1.0, "rpc ceiling", None)
            got += out
            row["events"].append({"t": round(age), "reason": "rpc ceiling", "sol_out": round(out, 4)})
            break
        if not c:
            time.sleep(5)
            continue
        if paper_shift and not c["complete"]:
            # paper: our pretend buy is not in the real curve — add it, so selling our bag
            # meets the price impact our own buy would have created
            # constant product: the curve after every buy is the same whatever the order, so
            # "real curve + our SOL" = the state where we bought FIRST (as the dev would)
            k = c["v_sol"] * c["v_tok"]
            vs = c["v_sol"] + paper_shift[0]
            c = {**c, "v_sol": vs, "v_tok": k // vs}
        if c["complete"]:
            # graduated: the curve is closed (live sells go to the AMM via pool=auto); for the
            # estimate use the last curve reading — at graduation that is the curve's top
            value, bag_mult = last
        else:
            value = sell_value(held, c)
            bag_mult = sell_value(tokens0, c) / cost if cost else 0   # price move vs our entry
            last = (value, bag_mult)
        # live: the curve's real SOL includes our own buy; paper: it does not (our buy is pretend)
        others = max(0.0, c["real_sol"] - (0.0 if paper_shift else cost * (1 - FEE)))
        peak = max(peak, bag_mult)
        reason = None
        frac = 0.0
        if c["complete"]:
            reason, frac = "graduated", 1.0
        elif age / 60 >= MAX_HOLD_MIN:
            reason, frac = "max hold", 1.0
        elif sold_steps == 0 and bag_mult < STOP_MULT:
            reason, frac = f"stop {bag_mult:.2f}x", 1.0
        elif sold_steps == 0 and age >= NO_TRACTION_S and others < TRACTION_SOL:
            reason, frac = f"no traction ({others:.2f} SOL from others)", 1.0
        elif sold_steps > 0 and bag_mult < peak * (1 - TRAIL):
            reason, frac = f"trail {bag_mult:.2f}x from peak {peak:.2f}x", 1.0
        elif sold_steps < len(STEPS) and bag_mult >= STEPS[sold_steps][0]:
            lvl, part = STEPS[sold_steps]
            sold_steps += 1
            reason, frac = f"step {lvl:g}x", min(1.0, part * tokens0 / held)
        if reason:
            out = sell_fn(frac, reason, value * frac)
            got += out
            held = int(held * (1 - frac))
            print(f"[launcher]        curve now v_sol={c['v_sol'] / LAMPORTS:.3f} "
                  f"v_tok={c['v_tok'] / 1e12:.1f}M real_sol={c['real_sol']:.3f}", flush=True)
            row["events"].append({"t": round(age), "reason": reason, "bag_x": round(bag_mult, 2),
                                  "sold_frac_of_held": round(frac, 3), "sol_out": round(out, 4),
                                  "others_sol": round(others, 2)})
            print(f"[launcher] {round(age):5d}s {reason}: sold {frac:.0%} of bag for {out:.4f} SOL "
                  f"(bag {bag_mult:.2f}x, others in {others:.2f} SOL)", flush=True)
            save(row)
            if frac >= 1.0:
                break
        time.sleep(3 if age < 120 else 10 if age < 1800 else 60)
    row["sol_out"] = round(got, 6)
    row["status"] = "closed"
    save(row)
    print(f"[launcher] done: in {cost:.4f} SOL, out {got:.4f} SOL, net {got - cost:+.4f} SOL; "
          f"RPC calls {_rpc_calls}/{MAX_RPC}")


# ── commands ──────────────────────────────────────────────────────────────────

def cmd_launch(a) -> None:
    if "official" in f"{a.name} {a.symbol} {a.description or ''}".lower():
        sys.exit("refused: no 'official' claims (launch rules)")
    if not (1 <= len(a.symbol) <= 10) or not a.name:
        sys.exit("name required, symbol 1-10 characters")
    if a.image and not Path(a.image).is_file():
        sys.exit(f"image not found: {a.image}")
    ensure_table()
    mode = "live" if a.live else "dry"
    est_tokens = buy_tokens(DEV_BUY)
    print(f"[launcher] {mode.upper()} launch '{a.name}' (${a.symbol}) dev buy {DEV_BUY} SOL "
          f"(~{est_tokens / 10 ** TOKEN_DECIMALS / 1e6:.1f}M tokens, {est_tokens / 1e15 * 100:.2f}% of supply). "
          f"Costs ~{DEV_BUY * FEE + PRIORITY_FEE + 0.02:.3f} SOL on top (fees, priority, rent). "
          f"RPC ceiling {MAX_RPC} for this launch.")
    print(f"[launcher] exits: no traction {NO_TRACTION_S:g}s (<{TRACTION_SOL} SOL from others) | stop "
          f"{STOP_MULT}x | steps {STEPS} | trail {TRAIL:.0%} | max hold {MAX_HOLD_MIN:g} min")
    if a.live:
        if os.getenv("LAUNCH_ENABLED", "false") != "true":
            sys.exit("refused: LAUNCH_ENABLED is not 'true' in the .env")
        today = launches_count("date_trunc('day', now())")
        total = launches_count(None)
        if today >= MAX_PER_DAY or total >= MAX_TOTAL:
            sys.exit(f"refused: caps reached (today {today}/{MAX_PER_DAY}, total {total}/{MAX_TOTAL})")
        if not a.image:
            sys.exit("refused: --image is required for a live launch")
    kp = wallet() if a.live else (Keypair.from_base58_string(os.environ["LAUNCH_WALLET_PRIVATE_KEY"])
                                  if os.getenv("LAUNCH_WALLET_PRIVATE_KEY") else Keypair())
    pub = str(kp.pubkey())
    bal = balance(pub)
    print(f"[launcher] wallet {pub[:6]}…{pub[-4:]} balance "
          f"{'?' if bal is None else f'{bal:.4f}'} SOL"
          + ("" if os.getenv("LAUNCH_WALLET_PRIVATE_KEY") else " (throwaway keypair: no launcher wallet set)"))
    if a.live and (bal is None or bal < MIN_WALLET + DEV_BUY + 0.03):
        sys.exit(f"refused: balance {bal} < floor {MIN_WALLET} + dev buy {DEV_BUY} + ~0.03 costs")

    mint_kp = Keypair()
    row = {"mode": mode, "name": a.name, "symbol": a.symbol, "note": a.note, "mint": str(mint_kp.pubkey()),
           "dev_buy_sol": DEV_BUY, "status": "planned", "events": []}
    save(row)

    if not a.live:
        uri = "https://example.invalid/dry-run.json"          # nothing uploaded in a dry run
        try:
            txb = trade_local({"publicKey": pub, "action": "create", "mint": str(mint_kp.pubkey()),
                               "tokenMetadata": {"name": a.name, "symbol": a.symbol, "uri": uri},
                               "denominatedInSol": "true", "amount": DEV_BUY, "slippage": 10,
                               "priorityFee": PRIORITY_FEE, "pool": "pump"})
            tx = VersionedTransaction.from_bytes(txb)
            signed = VersionedTransaction(tx.message, [mint_kp, kp])
            sim = rpc("simulateTransaction", [base64.b64encode(bytes(signed)).decode(),
                                              {"encoding": "base64", "sigVerify": True,
                                               "replaceRecentBlockhash": False, "commitment": "confirmed"}])
            err = (sim or {}).get("value", {}).get("err")
            logs = (sim or {}).get("value", {}).get("logs") or []
            print(f"[launcher] create transaction built ({len(txb)} bytes, signed by mint + wallet). "
                  f"Simulation: {'OK' if sim and err is None else f'error {err}'}")
            if err and (str(err) == "AccountNotFound" or any("insufficient" in l.lower() for l in logs)):
                print("[launcher]   (expected for an unfunded or throwaway wallet: the transaction itself "
                      "is well-formed; fund the launcher wallet and the simulation should pass)")
            row["status"] = "simulated"
            row["events"].append({"simulated": err is None, "err": str(err) if err else None})
        except Exception as e:
            print(f"[launcher] dry-run build/simulate failed: {type(e).__name__}: {e}")
            row["status"] = "failed"
        save(row)
        print("[launcher] DRY RUN — nothing uploaded, nothing sent. Add --live to launch for real.")
        return

    confirm = input(f"Type the ticker {a.symbol} to launch for real with {DEV_BUY} SOL: ").strip()
    if confirm != a.symbol:
        row["status"] = "failed"
        save(row)
        sys.exit("cancelled")
    uri = upload_metadata(a)
    txb = trade_local({"publicKey": pub, "action": "create", "mint": str(mint_kp.pubkey()),
                       "tokenMetadata": {"name": a.name, "symbol": a.symbol, "uri": uri},
                       "denominatedInSol": "true", "amount": DEV_BUY, "slippage": 10,
                       "priorityFee": PRIORITY_FEE, "pool": "pump"})
    bal0 = bal
    sig = send(txb, [mint_kp, kp])
    if not sig or not confirmed(sig):
        row["status"] = "failed"
        save(row)
        sys.exit(f"create not confirmed (sig {sig}) — check the wallet on Solscan before retrying")
    row["create_sig"] = sig
    toks = our_tokens(pub, str(mint_kp.pubkey()))
    bal1 = balance(pub)
    row["sol_in"] = round((bal0 - bal1), 6) if bal0 is not None and bal1 is not None else DEV_BUY
    row["status"] = "open"
    save(row)
    print(f"[launcher] LAUNCHED https://pump.fun/coin/{mint_kp.pubkey()} sig {sig} "
          f"tokens {toks / 10 ** TOKEN_DECIMALS:,.0f}, cost {row['sol_in']} SOL")

    def sell_fn(frac, reason, est):
        pct = f"{max(1, min(100, round(frac * 100)))}%"
        try:
            b0 = balance(pub)
            txs = trade_local({"publicKey": pub, "action": "sell", "mint": str(mint_kp.pubkey()),
                               "denominatedInSol": "false", "amount": pct, "slippage": 15,
                               "priorityFee": PRIORITY_FEE, "pool": "auto"})
            s = send(txs, [kp])
            if s and confirmed(s):
                b1 = balance(pub)
                return max(0.0, (b1 - b0)) if b0 is not None and b1 is not None else (est or 0.0)
            print(f"[launcher] SELL {pct} not confirmed ({reason}) — retrying next poll")
        except Ceiling:
            raise
        except Exception as e:
            print(f"[launcher] SELL {pct} failed ({reason}): {type(e).__name__}: {e}")
        return 0.0

    run_exits(row, str(mint_kp.pubkey()), toks, float(row["sol_in"]), sell_fn)


def cmd_paper(a) -> None:
    ensure_table()
    mint = a.mint
    if not mint:
        conn = db.get_conn()
        start = datetime.now(timezone.utc)
        print("[launcher] paper: waiting for the next new pump.fun coin (news-recorder feed)…", flush=True)
        while not mint:
            with conn.cursor() as cur:
                cur.execute("SELECT mint, name FROM new_tokens WHERE created_at > %s AND name IS NOT NULL "
                            "ORDER BY created_at LIMIT 1", (start,))
                r = cur.fetchone()
            conn.commit()
            if r:
                mint = r[0]
                print(f"[launcher] paper: following {r[1]} {mint}")
            else:
                time.sleep(2)
    # Price our pretend buy off the coin's REAL curve, not the textbook starting reserves:
    # 2026-11-03 the first paper run valued the bag at 26.6x at second 0 (nobody else in) and
    # "sold" it, because this coin's curve did not start at 30 SOL / 1.073B tokens.
    c0 = None
    for _ in range(10):
        c0 = curve(mint)
        if c0 and c0["v_sol"] > 0 and c0["v_tok"] > 0 and not c0["complete"]:
            break
        time.sleep(1)
    if not c0 or c0["v_sol"] <= 0 or c0["v_tok"] <= 0 or c0["complete"]:
        print(f"[launcher] paper: no usable bonding curve for {mint} (not a pump.fun curve, or already "
              f"graduated) — skipped")
        return
    # We are the DEV in this rehearsal: our buy goes in first, at the curve's starting state
    # (before the real dev's buy, which then counts as an outside buyer). The second paper run
    # bought on top of the real dev instead and lost 0.015-0.064 SOL per dud when that dev sold:
    # a sniper's downside, not a dev's (a first buyer gets ~cost minus fees back).
    k0 = c0["v_sol"] * c0["v_tok"]
    vs0 = c0["v_sol"] - int(round(c0["real_sol"] * LAMPORTS))
    if vs0 <= 0:
        print(f"[launcher] paper: curve for {mint} has no starting state — skipped")
        return
    toks = buy_tokens(DEV_BUY, vs0, k0 // vs0)
    our = int(DEV_BUY * (1 - FEE) * LAMPORTS)
    vs1 = c0["v_sol"] + our
    start_x = sell_value(toks, {**c0, "v_sol": vs1, "v_tok": k0 // vs1}) / DEV_BUY
    print(f"[launcher] paper: curve at first look v_sol={c0['v_sol'] / LAMPORTS:.3f} "
          f"v_tok={c0['v_tok'] / 1e12:.1f}M real_sol={c0['real_sol']:.3f} -> start state "
          f"v_sol={vs0 / LAMPORTS:.3f}; our bag at entry {start_x:.2f}x", flush=True)
    if not 0.9 < start_x < 1.1:
        print(f"[launcher] paper: curve maths for {mint} gives a {start_x:.2f}x bag at entry — "
              f"unexpected curve layout, skipped")
        return
    row = {"mode": "paper", "name": None, "symbol": None, "note": "paper", "mint": mint,
           "dev_buy_sol": DEV_BUY, "status": "open", "sol_in": DEV_BUY, "events": []}
    save(row)
    print(f"[launcher] PAPER: as if we bought {DEV_BUY} SOL at curve start ({toks / 1e12:.1f}M tokens). "
          f"RPC ceiling {MAX_RPC}. Nothing is sent.")
    run_exits(row, mint, toks, DEV_BUY, lambda frac, reason, est: est or 0.0,
              paper_shift=(our, None))


def cmd_report(_a) -> None:
    ensure_table()
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("""SELECT id, created_at, mode, symbol, note, status, sol_in, sol_out, mint
                       FROM launches ORDER BY id DESC LIMIT 50""")
        rows = cur.fetchall()
    print(f"  {'id':>4s} {'when (UTC)':16s} {'mode':5s} {'ticker':10s} {'status':9s} {'in':>7s} {'out':>7s} "
          f"{'net':>8s}  note / mint")
    live_net = []
    for i, t, mode, sym, note, st, si, so, mint in rows:
        net = (float(so) - float(si)) if si is not None and so is not None else None
        if mode == "live" and net is not None:
            live_net.append(net)
        print(f"  {i:4d} {t:%m-%d %H:%M}      {mode:5s} {str(sym or '-'):10s} {str(st):9s} "
              f"{(float(si) if si is not None else 0):7.4f} {(float(so) if so is not None else 0):7.4f} "
              f"{(f'{net:+8.4f}' if net is not None else '       -')}  {note or ''} {mint or ''}")
    if live_net:
        print(f"\nlive launches closed: {len(live_net)}, net {sum(live_net):+.4f} SOL, "
              f"profitable {sum(n > 0 for n in live_net)}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("launch")
    p.add_argument("--name", required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--image")
    p.add_argument("--description", default="")
    p.add_argument("--twitter")
    p.add_argument("--website")
    p.add_argument("--note", default="", help="what the pick was based on (viral tweet, animal, stock meme…)")
    p.add_argument("--live", action="store_true", help="really launch (default: dry run)")
    p.set_defaults(fn=cmd_launch)
    p = sub.add_parser("paper")
    p.add_argument("--mint")
    p.set_defaults(fn=cmd_paper)
    p = sub.add_parser("report")
    p.set_defaults(fn=cmd_report)
    a = ap.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
