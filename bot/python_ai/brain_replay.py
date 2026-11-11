"""
brain_replay.py — back-test the trader brain on past coins: calibrate first, then replay v7 vs v8 on November.

WHY (2026-11-10). v8 (the nightly playbook) went live on Nov 11, and the v7/v8 decision needs ~5 days of
paired judging. The coins called Nov 1-10 are already closed, so a replay can say early whether the
playbook helps. A replay can also mislead: the brain's view of a coin has to be rebuilt as it was at the
call, and the model answers a little differently every time. So it runs in two steps, and the second
is only worth the money if the first passes.

STEP 1 — CALIBRATE on days where live v7 already judged every coin (default Nov 8-10):
  fields      (free) rebuild each coin's view as of the moment live v7 judged it and compare it,
              field by field, with the view live v7 actually saw. Two rebuilds:
                rebuilt  everything from the database as of then (what Nov 1-3 coins get)
                v5+sim   live v5's stored view + similar coins as of then (what Nov 4-6 coins get)
  calibrate   ask v7 again, on (a) the stored live view — the noise floor: same input, a second answer —
              and (b) the rebuilt view. Compares each with live v7's answers: BUY agreement and the
              "solo picks" gap (the P&L difference between the coins only one side bought — the number
              the 10-pts/SOL decision rule uses). If the rebuilt arm's gap is about the stored arm's, the
              replay is as good as a second live run; if it is much bigger, the replay can't be trusted.
STEP 2 — NOVEMBER (default Nov 1-10), days in order, per channel:
  1. v7 on every coin of day D: live v7's answer where there is one, else a replay v7 answer on the
     best view available (live v7's, else live v5's + similar coins, else rebuilt).
  2. a replay playbook for D, exactly like the nightly one: tables from v7's calls on D-7..D-1, only
     coins CLOSED before D 00:20 Pacific, dev-bundled skip and recorder-gap masking applied; lessons
     from the same lesson model.
  3. v8 on every coin of D: the same view v7 had + that playbook's lessons.
  Then `report`: per day v7 vs v8 on solo picks, to read against the calibration's noise gap.

Nothing here touches the live tables: answers go to brain_replay_verdict, playbooks to
brain_replay_playbook (both counted in trader_brain's credit warning). One call at a time with a sleep,
a hard --max-usd per run (default $2), and it resumes where it stopped (answers already stored are kept).
A billing error stops the run.

Run (VPS, one-off — not a pm2 service):
  python3 brain_replay.py fields                       # free; read before spending anything
  python3 brain_replay.py calibrate --max-usd 2        # then: python3 brain_replay.py calreport
  python3 brain_replay.py november --max-usd 6         # only if calibration passed
  python3 brain_replay.py report
Options: --from YYYY-MM-DD --to YYYY-MM-DD (Pacific days, inclusive), --per-day N (calibrate: coins per
channel per day, default 25), --sleep S (default 1.5), --run NAME (default cal / nov), --fast (fields:
skip the coin-metadata fetch), --v7-from RUN (november / report: reuse RUN's replayed v7 answers, so a
v8 change is re-tested for the price of v8 alone — e.g. --run nov2 --v7-from nov).

2026-11-11 run 'nov' (Nov 2-5): v8 bought 33 coins to v7's 73 and the 44 it dropped made +8.8%/SOL —
it stretched lessons to coins they didn't fit. Fixed in trader_brain (lessons_for_coin, MIN_N 15);
run 'nov2' re-tests it.
"""

from __future__ import annotations

import json
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import db  # noqa: E402
import api_alerts  # noqa: E402
import trader_brain as tb  # noqa: E402

V7, V8 = tb.LEARN_V, tb.PLAYBOOK_V
ENTRY_LAG = timedelta(seconds=10)     # live judges a coin seconds after qsim opens it


def ensure_tables() -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS brain_replay_verdict (
                run         text NOT NULL,
                call_id     integer NOT NULL,
                arm         text NOT NULL,       -- v7_stored / v7_rebuilt (calibrate), v7 / v8 (november)
                day         date NOT NULL,       -- Pacific day of the entry
                channel     text NOT NULL,
                as_of       timestamptz,         -- the moment the view was rebuilt for
                ctx_source  text,                -- live_v7 / live_v5+sim / rebuilt
                vs_typical  integer,
                buy         boolean,
                verdict     jsonb,
                context     jsonb,
                model       text,
                in_tokens   integer,
                out_tokens  integer,
                created_at  timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (run, call_id, arm)
            )""")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS brain_replay_playbook (
                run         text NOT NULL,
                channel     text NOT NULL,
                made_for    date NOT NULL,
                n_coins     integer,             -- v7 calls in the tables
                n_all       integer,             -- all judged coins in the tables
                tables      jsonb,
                lessons     jsonb,
                dropped     jsonb,
                model       text,
                in_tokens   integer,
                out_tokens  integer,
                created_at  timestamptz NOT NULL DEFAULT now(),
                PRIMARY KEY (run, channel, made_for)
            )""")
        conn.commit()


def _day_bounds(d: date) -> tuple[datetime, datetime]:
    from zoneinfo import ZoneInfo
    z = ZoneInfo(tb.REPORT_TZ)
    start = lambda x: datetime(x.year, x.month, x.day, tzinfo=z).astimezone(timezone.utc)
    return start(d), start(d + timedelta(days=1))


def _days(a: date, b: date) -> list[date]:
    return [a + timedelta(days=i) for i in range((b - a).days + 1)]


def coins(channel: str, d: date) -> list[dict]:
    """Closed qsim coins entered on Pacific day d, minus the ones the brain skips (dev bundled)."""
    t0, t1 = _day_bounds(d)
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute(f"""
            SELECT qp.call_id, qp.entry_time, qp.exit_time, qp.sol_in, qp.sol_out, qp.exit_reason,
                   t.mint_address, t.symbol
            FROM qsim_positions qp JOIN tokens t ON t.id = qp.token_id
            WHERE qp.channel_handle = %s AND qp.status = 'closed' AND qp.sol_in > 0
              AND qp.entry_time >= %s AND qp.entry_time < %s AND {tb._DEV_OK}
            ORDER BY qp.entry_time""", (channel, t0, t1, tb.SKIP_DEV_BUNDLED, tb.SKIP_DEV_BUNDLED))
        return [{"call_id": r[0], "entry": r[1], "exit": r[2], "s_in": float(r[3]), "s_out": float(r[4] or 0),
                 "reason": r[5], "mint": r[6], "sym": r[7], "day": d, "channel": channel} for r in cur.fetchall()]


def live_rows(call_ids: list[int]) -> dict[tuple[int, int], dict]:
    """{(call_id, prompt_v): {vs_typical, buy, verdict, context, judged_at}} from trader_brain_entry."""
    if not call_ids:
        return {}
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute("""SELECT call_id, prompt_v, vs_typical, buy, verdict, context, judged_at
                       FROM trader_brain_entry WHERE call_id = ANY(%s) AND context IS NOT NULL""", (call_ids,))
        return {(r[0], r[1]): {"vs_typical": r[2], "buy": r[3], "verdict": r[4] or {}, "context": r[5] or {},
                               "judged_at": r[6]} for r in cur.fetchall()}


V7_FROM = None     # --v7-from RUN: reuse that run's replayed v7 answers (re-test v8 without re-asking v7)


def stored(run: str) -> dict[tuple[int, str], dict]:
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute("""SELECT call_id, arm, vs_typical, buy, verdict, context, ctx_source, run
                       FROM brain_replay_verdict WHERE run = %s OR (run = %s AND arm = 'v7')
                       ORDER BY (run = %s)""", (run, V7_FROM, run))
        # this run's own rows come last, so they win over the borrowed v7 answers
        return {(r[0], r[1]): {"vs_typical": r[2], "buy": r[3], "verdict": r[4] or {}, "context": r[5] or {},
                               "ctx_source": r[6]} for r in cur.fetchall()}


def is_buy(row: dict) -> bool:
    vt = row.get("vs_typical")
    return vt >= 6 if vt is not None else bool(row.get("buy"))


# ── views ───────────────────────────────────────────────────────────────────────

def rebuilt_view(c: dict, as_of: datetime, fast: bool = False) -> dict | None:
    """Everything from the database as of `as_of` (v7's view: similar coins included)."""
    return tb.build_context(c["call_id"], with_path=False, learn=True, fast=fast, as_of=as_of)


def v5sim_view(c: dict, v5: dict) -> dict:
    """Live v5's stored view + similar coins as of the moment v5 judged (v5 and v7 views differ only there)."""
    ctx = json.loads(json.dumps(v5["context"]))
    for k in ("similar_past_coins", "similar_summary", "similar_vs_channel", "lessons_from_your_record"):
        ctx.pop(k, None)
    tb.add_similar(ctx, c["mint"], v5["judged_at"])
    return ctx


def best_v7_view(c: dict, live: dict) -> tuple[dict | None, str, datetime]:
    """(view, source, as_of) — live v7's own view when there is one, else v5's + similar, else rebuilt."""
    cid = c["call_id"]
    if (cid, V7) in live:
        r = live[(cid, V7)]
        return r["context"], "live_v7", r["judged_at"]
    others = sorted((v for (k, v) in live if k == cid), reverse=True)
    if others:
        r = live[(cid, others[0])]
        return v5sim_view(c, r), f"live_v{others[0]}+sim", r["judged_at"]
    at = c["entry"] + ENTRY_LAG
    return rebuilt_view(c, at), "rebuilt", at


# ── asking ──────────────────────────────────────────────────────────────────────

class Budget:
    def __init__(self, run: str, max_usd: float, sleep: float):
        self.run, self.max_usd, self.sleep = run, max_usd, sleep
        self.calls = self.failed = 0
        self.stopped = ""

    def spent(self) -> float:
        db.safe_rollback()
        with db.get_conn().cursor() as cur:
            cur.execute("SELECT coalesce(sum(in_tokens), 0), coalesce(sum(out_tokens), 0) FROM brain_replay_verdict "
                        "WHERE run = %s", (self.run,))
            a, b = cur.fetchone()
            cur.execute("SELECT coalesce(sum(in_tokens), 0), coalesce(sum(out_tokens), 0) FROM brain_replay_playbook "
                        "WHERE run = %s", (self.run,))
            c, d = cur.fetchone()
        return (float(a) / 1e6 * tb.PRICE_IN + float(b) / 1e6 * tb.PRICE_OUT
                + float(c) / 1e6 * tb.LESSON_PRICE_IN + float(d) / 1e6 * tb.LESSON_PRICE_OUT)

    def ok(self) -> bool:
        if self.stopped:
            return False
        if self.spent() >= self.max_usd:
            self.stopped = f"budget reached (${self.max_usd:.2f} for run '{self.run}')"
            return False
        return True


def ask(budget: Budget, c: dict, arm: str, ver: int, ctx: dict, source: str, as_of: datetime | None) -> dict | None:
    """One replay answer, stored. None when skipped (budget / failure)."""
    if not budget.ok():
        return None
    time.sleep(budget.sleep)
    try:
        v, tin, tout, err = tb.judge(ctx, tb._system_for(ver), "entry")
    except Exception as e:
        v, tin, tout, err = None, 0, 0, f"{type(e).__name__}: {e}"
    budget.calls += 1
    if v is None:
        budget.failed += 1
        print(f"  {arm} {c['sym']} call_id={c['call_id']} failed: {err}", flush=True)
        if api_alerts.is_billing_error(err):
            budget.stopped = f"Anthropic refused (out of credit or bad key): {err[:160]}"
        if tin or tout:                     # a paid answer that didn't parse still costs
            _store(budget.run, c, arm + "_failed", as_of, source, None, None, {"error": err}, None, tin, tout)
        return None
    vt = tb._int(v.get("vs_typical"))
    _store(budget.run, c, arm, as_of, source, vt, v.get("buy"), v, ctx, tin, tout)
    return {"vs_typical": vt, "buy": v.get("buy"), "verdict": v, "context": ctx, "ctx_source": source}


def _store(run, c, arm, as_of, source, vt, buy, verdict, ctx, tin, tout) -> None:
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO brain_replay_verdict (run, call_id, arm, day, channel, as_of, ctx_source, vs_typical,
                       buy, verdict, context, model, in_tokens, out_tokens)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT (run, call_id, arm) DO UPDATE SET in_tokens = brain_replay_verdict.in_tokens
                         + EXCLUDED.in_tokens, out_tokens = brain_replay_verdict.out_tokens + EXCLUDED.out_tokens""",
                    (run, c["call_id"], arm, c["day"], c["channel"], as_of, source, vt, buy, json.dumps(verdict),
                     json.dumps(ctx, default=str) if ctx is not None else None, tb.MODEL, tin, tout))
    conn.commit()


# ── step 1: fields + calibrate ──────────────────────────────────────────────────

def _jacc(a: list, b: list) -> float | None:
    sa = {x.get("symbol") for x in a or []}
    sb = {x.get("symbol") for x in b or []}
    return len(sa & sb) / len(sa | sb) if sa | sb else None


def _same(a, b) -> bool:
    return json.dumps(a, sort_keys=True, default=str) == json.dumps(b, sort_keys=True, default=str)


SKIP_KEYS = {"similar_past_coins", "similar_summary", "similar_vs_channel", "channel_typical",
             "lessons_from_your_record", "market_today"}


def fields(d0: date, d1: date, fast: bool) -> None:
    """Free: rebuilt and v5+sim views vs the view live v7 saw, field by field."""
    same = {"rebuilt": defaultdict(int), "v5+sim": defaultdict(int)}
    n = {"rebuilt": 0, "v5+sim": 0}
    sim_j = {"rebuilt": [], "v5+sim": []}
    sim_v = {"rebuilt": [0, 0], "v5+sim": [0, 0]}
    bank_d, tok_keys, feat_same, feat_n = [], defaultdict(int), defaultdict(int), defaultdict(int)
    for ch in tb.CHANNELS:
        for d in _days(d0, d1):
            cs = coins(ch, d)
            live = live_rows([c["call_id"] for c in cs])
            for c in cs:
                lv = live.get((c["call_id"], V7))
                if not lv:
                    continue
                L = lv["context"]
                views = {"rebuilt": rebuilt_view(c, lv["judged_at"], fast=fast)}
                v5 = live.get((c["call_id"], tb.PROMPT_V))
                if v5:
                    views["v5+sim"] = v5sim_view(c, v5)
                for name, R in views.items():
                    if not R:
                        continue
                    n[name] += 1
                    for k in set(L) | set(R):
                        if k in SKIP_KEYS or (fast and name == "rebuilt" and k == "coin_metadata"):
                            continue
                        same[name][k] += _same(L.get(k), R.get(k))
                    j = _jacc(L.get("similar_past_coins"), R.get("similar_past_coins"))
                    if j is not None:
                        sim_j[name].append(j)
                    lvv = (L.get("similar_vs_channel") or {}).get("verdict")
                    rvv = (R.get("similar_vs_channel") or {}).get("verdict")
                    sim_v[name][0] += lvv == rvv
                    sim_v[name][1] += 1
                    if name == "rebuilt":
                        lb = (L.get("channel_typical") or {}).get("real_bank_2x_rate_pct")
                        rb = (R.get("channel_typical") or {}).get("real_bank_2x_rate_pct")
                        if lb is not None and rb is not None:
                            bank_d.append(abs(lb - rb))
                        for k in set(L.get("token") or {}) | set(R.get("token") or {}):
                            if not _same((L.get("token") or {}).get(k), (R.get("token") or {}).get(k)):
                                tok_keys[k] += 1
                        fl, fr = tb._features(L, {}, c["mint"]), tb._features(R, {}, c["mint"])
                        for k in fl:
                            if fl[k] is not None or fr[k] is not None:
                                feat_n[k] += 1
                                feat_same[k] += fl[k] == fr[k]
    print(f"\nFIELDS — views rebuilt for {d0}..{d1} vs what live v7 saw ({', '.join(tb.CHANNELS)})")
    for name in ("rebuilt", "v5+sim"):
        if not n[name]:
            print(f"\n  {name}: no coins")
            continue
        print(f"\n  {name}: {n[name]} coins — % identical per field")
        for k, v in sorted(same[name].items(), key=lambda kv: kv[1]):
            print(f"    {k:<34} {v / n[name] * 100:5.1f}%")
        if sim_j[name]:
            print(f"    similar coins overlap (Jaccard)    {sum(sim_j[name]) / len(sim_j[name]) * 100:5.1f}% avg")
        if sim_v[name][1]:
            print(f"    'similar vs channel' verdict       {sim_v[name][0] / sim_v[name][1] * 100:5.1f}% same")
    if bank_d:
        print(f"\n  rebuilt channel bank rate: off by {sum(bank_d) / len(bank_d):.1f} pts on average")
    if feat_n:
        print("\n  rebuilt playbook features (what the lessons are built from) — % same bucket")
        for k in sorted(feat_n, key=lambda k: feat_same[k] / feat_n[k]):
            print(f"    {k:<34} {feat_same[k] / feat_n[k] * 100:5.1f}%  (n={feat_n[k]})")
    if tok_keys:
        print("\n  rebuilt token fields that differ most (the token row is today's, not the call's):")
        for k, v in sorted(tok_keys.items(), key=lambda kv: -kv[1])[:10]:
            print(f"    {k:<34} {v} coins")
    print("\nRead: 'call', 'our_entry', 'dev', 'copycats', 'holders_at_entry' should be ~100% for rebuilt; the token"
          "\nrow and coin_metadata can drift. v5+sim should match everything but the similar coins.")


def calibrate(d0: date, d1: date, budget: Budget, per_day: int) -> None:
    ensure_tables()
    have = stored(budget.run)
    for d in _days(d0, d1):
        for ch in tb.CHANNELS:
            cs = coins(ch, d)
            live = live_rows([c["call_id"] for c in cs])
            cs = [c for c in cs if (c["call_id"], V7) in live]
            cs.sort(key=lambda c: (c["call_id"] * 2654435761) % 2 ** 32)     # a fixed, spread-out sample
            cs = cs[:per_day]
            print(f"[replay] calibrate {d} {ch}: {len(cs)} coins", flush=True)
            for c in cs:
                lv = live[(c["call_id"], V7)]
                if (c["call_id"], "v7_stored") not in have:
                    ask(budget, c, "v7_stored", V7, lv["context"], "live_v7", lv["judged_at"])
                if (c["call_id"], "v7_rebuilt") not in have:
                    ctx = rebuilt_view(c, lv["judged_at"])
                    if ctx:
                        ask(budget, c, "v7_rebuilt", V7, ctx, "rebuilt", lv["judged_at"])
                if budget.stopped:
                    print(f"[replay] stopped: {budget.stopped}")
                    return
    print(f"[replay] calibrate done: {budget.calls} calls ({budget.failed} failed), run '{budget.run}' "
          f"~${budget.spent():.2f}")


def _pts(items: list) -> float | None:
    s = sum(c["s_in"] for c in items)
    return sum(c["s_out"] - c["s_in"] for c in items) / s * 100 if s else None


def _f(x) -> str:
    return "   n/a" if x is None else f"{x:+6.1f}"


def solo(a_buy: set, b_buy: set, by_id: dict) -> tuple[int, float | None, int, float | None, float | None]:
    """(n A-only, A-only pts/SOL, n B-only, B-only pts/SOL, A minus B in pts/SOL over all solo picks)."""
    a_only = [by_id[i] for i in a_buy - b_buy]
    b_only = [by_id[i] for i in b_buy - a_buy]
    s = sum(c["s_in"] for c in a_only + b_only) or None
    gap = None
    if s:
        gap = (sum(c["s_out"] - c["s_in"] for c in a_only) - sum(c["s_out"] - c["s_in"] for c in b_only)) / s * 100
    return len(a_only), _pts(a_only), len(b_only), _pts(b_only), gap


def calreport(d0: date, d1: date, run: str) -> None:
    have = stored(run)
    print(f"\nCALIBRATION — run '{run}', {d0}..{d1}: each arm vs live v7 on the same coins")
    for ch in tb.CHANNELS + ["ALL"]:
        chans = tb.CHANNELS if ch == "ALL" else [ch]
        cs = [c for cc in chans for d in _days(d0, d1) for c in coins(cc, d)]
        live = live_rows([c["call_id"] for c in cs])
        by_id = {c["call_id"]: c for c in cs}
        print(f"\n  {ch}")
        for arm in ("v7_stored", "v7_rebuilt"):
            ids = [i for i in by_id if (i, arm) in have and (i, V7) in live]
            if not ids:
                print(f"    {arm:<11} no answers")
                continue
            lb = {i for i in ids if is_buy(live[(i, V7)])}
            ab = {i for i in ids if is_buy(have[(i, arm)])}
            agree = sum((i in lb) == (i in ab) for i in ids) / len(ids) * 100
            sd = [abs((have[(i, arm)]["vs_typical"] or 0) - (live[(i, V7)]["vs_typical"] or 0)) for i in ids]
            na, pa, nb, pb, gap = solo(ab, lb, by_id)
            print(f"    {arm:<11} n={len(ids):<4} BUY agree {agree:5.1f}%  score off {sum(sd) / len(sd):.2f} avg | "
                  f"BUYs live {len(lb)} ({_f(_pts([by_id[i] for i in lb])).strip()}) replay {len(ab)} "
                  f"({_f(_pts([by_id[i] for i in ab])).strip()}) | solo: replay-only {na} {_f(pa).strip()}, "
                  f"live-only {nb} {_f(pb).strip()}, gap {_f(gap).strip()} pts/SOL")
    print("\nRead: v7_stored is the noise floor — the same view asked twice. If v7_rebuilt's agreement is within"
          "\n~5 pts of it and its solo gap is of the same size, the November replay is as good as a second live"
          "\nrun. A solo gap near 10 pts/SOL even for v7_stored means one day's v7-vs-v8 winner is mostly noise.")


# ── step 2: november ────────────────────────────────────────────────────────────

def replay_playbook(budget: Budget, ch: str, made_for: date, have: dict) -> dict | None:
    """The playbook the nightly build would have made for `made_for`, from v7's calls (live or replayed)."""
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute("SELECT tables, lessons FROM brain_replay_playbook WHERE run = %s AND channel = %s AND made_for = %s",
                    (budget.run, ch, made_for))
        r = cur.fetchone()
    if r:
        return {"tables": r[0], "lessons": r[1]}
    first, last = made_for - timedelta(days=tb.PLAYBOOK_DAYS), made_for - timedelta(days=1)
    from zoneinfo import ZoneInfo
    cutoff = datetime(made_for.year, made_for.month, made_for.day, *tb.PLAYBOOK_AT,
                      tzinfo=ZoneInfo(tb.REPORT_TZ)).astimezone(timezone.utc)
    gaps = tb.recorder_gaps()
    items, all_coins = [], []
    for d in _days(first, last):
        cs = [c for c in coins(ch, d) if c["exit"] and c["exit"] < cutoff]
        live = live_rows([c["call_id"] for c in cs])
        for c in cs:
            cid = c["call_id"]
            v7 = live.get((cid, V7)) or have.get((cid, "v7"))
            others = sorted((v for (k, v) in live if k == cid), reverse=True)
            ctx = (v7 or {}).get("context") or (live[(cid, others[0])]["context"] if others else None)
            if not ctx or not tb.judged_now(ctx):
                continue
            all_coins.append(tb.coin_item(ctx, c["s_in"], c["s_out"], c["reason"], c["mint"], c["entry"], gaps))
            if v7 and v7.get("vs_typical") is not None:
                items.append(tb.verdict_item(v7["vs_typical"], v7["verdict"], ctx, c["s_in"], c["s_out"], c["reason"],
                                             c["mint"], c["sym"], c["entry"], gaps))
    rows, misses = tb.assemble_tables(items, all_coins)
    if not budget.ok():
        return None
    lessons, dropped, tin, tout = tb.write_lessons(ch, first, last, V7, rows, len(items), misses, len(all_coins))
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO brain_replay_playbook (run, channel, made_for, n_coins, n_all, tables, lessons,
                       dropped, model, in_tokens, out_tokens) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                       ON CONFLICT DO NOTHING""",
                    (budget.run, ch, made_for, len(items), len(all_coins), json.dumps(rows), json.dumps(lessons),
                     json.dumps(dropped), tb.LESSON_MODEL, tin, tout))
    conn.commit()
    print(f"[replay] playbook {ch} for {made_for}: {len(all_coins)} coins ({len(items)} v7 calls), "
          f"{len(lessons)} lessons", flush=True)
    return {"tables": rows, "lessons": lessons}


def november(d0: date, d1: date, budget: Budget) -> None:
    ensure_tables()
    for d in _days(d0, d1):
        for ch in tb.CHANNELS:
            cs = coins(ch, d)
            live = live_rows([c["call_id"] for c in cs])
            have = stored(budget.run)
            n7 = sum((c["call_id"], V7) in live for c in cs)
            print(f"[replay] {d} {ch}: {len(cs)} coins, {n7} with a live v7 answer", flush=True)
            views = {}
            for c in cs:                                   # 1. v7
                cid = c["call_id"]
                if (cid, V7) in live:
                    views[cid] = (live[(cid, V7)]["context"], "live_v7", live[(cid, V7)]["judged_at"])
                elif (cid, "v7") in have:
                    h = have[(cid, "v7")]
                    views[cid] = (h["context"], h["ctx_source"], None)
                else:
                    ctx, src, at = best_v7_view(c, live)
                    if ctx and ask(budget, c, "v7", V7, ctx, src, at):
                        views[cid] = (ctx, src, at)
                if budget.stopped:
                    print(f"[replay] stopped: {budget.stopped}")
                    return
            if d == d0 and not _has_window(ch, d):
                print(f"[replay] {d} {ch}: no earlier judged coins for a playbook — v8 starts the next day")
                continue
            pb = replay_playbook(budget, ch, d, stored(budget.run))   # 2. playbook
            if pb is None:
                print(f"[replay] stopped: {budget.stopped}")
                return
            have = stored(budget.run)
            for c in cs:                                   # 3. v8
                cid = c["call_id"]
                if (cid, "v8") in have or cid not in views:
                    continue
                ctx, src, at = views[cid]
                ctx = {**ctx, "lessons_from_your_record": tb.lessons_for_coin(pb["lessons"], pb["tables"], ctx,
                                                                              c["mint"], at or c["entry"])}
                ask(budget, c, "v8", V8, ctx, src, at)
                if budget.stopped:
                    print(f"[replay] stopped: {budget.stopped}")
                    return
    print(f"[replay] november done: {budget.calls} calls ({budget.failed} failed), run '{budget.run}' "
          f"~${budget.spent():.2f}")


def _has_window(ch: str, d: date) -> bool:
    """Any live-judged coin in the week before d (else the first replay day has nothing to learn from)."""
    t0, _ = _day_bounds(d - timedelta(days=tb.PLAYBOOK_DAYS))
    t1, _ = _day_bounds(d)
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute("""SELECT 1 FROM trader_brain_entry e JOIN qsim_positions qp ON qp.call_id = e.call_id
                       WHERE qp.channel_handle = %s AND qp.entry_time >= %s AND qp.entry_time < %s LIMIT 1""",
                    (ch, t0, t1))
        return cur.fetchone() is not None


def report(d0: date, d1: date, run: str) -> None:
    have = stored(run)
    print(f"\nNOVEMBER REPLAY — run '{run}', {d0}..{d1}: v7 (live where it judged, else replayed) vs replayed v8")
    print("Solo picks = coins only one version bought; 'v8 - v7' is the pts/SOL gap the decision rule reads.")
    tot = defaultdict(lambda: {"cs": [], "v7": set(), "v8": set()})
    for ch in tb.CHANNELS:
        print(f"\n  {ch}")
        print(f"    {'day':<11}{'coins':>6}{'v7 src':>14}{'v7 BUY':>13}{'v8 BUY':>13}   solo v7-only    v8-only   v8 - v7")
        for d in _days(d0, d1):
            cs = coins(ch, d)
            live = live_rows([c["call_id"] for c in cs])
            by_id = {c["call_id"]: c for c in cs}
            v7 = {i: live.get((i, V7)) or have.get((i, "v7")) for i in by_id}
            ids = [i for i in by_id if v7[i] and (i, "v8") in have]
            if not ids:
                print(f"    {d}  no v8 answers")
                continue
            b7 = {i for i in ids if is_buy(v7[i])}
            b8 = {i for i in ids if is_buy(have[(i, "v8")])}
            n_live = sum((i, V7) in live for i in ids)
            n8, p8, n7, p7, gap = solo(b8, b7, by_id)
            win = "v8" if gap is not None and gap >= 10 else "v7" if gap is not None and gap <= -10 else "-"
            print(f"    {d}  {len(ids):>4}  {n_live:>4} live/{len(ids) - n_live:<3}  "
                  f"{len(b7):>3} {_f(_pts([by_id[i] for i in b7]))}  {len(b8):>3} {_f(_pts([by_id[i] for i in b8]))}"
                  f"   {n7:>3} {_f(p7)}  {n8:>3} {_f(p8)}  {_f(gap)}  {win}")
            for k in (ch, "ALL"):
                tot[k]["cs"] += [by_id[i] for i in ids]
                tot[k]["v7"] |= b7
                tot[k]["v8"] |= b8
    print("\n  TOTALS")
    for k, t in tot.items():
        by_id = {c["call_id"]: c for c in t["cs"]}
        n8, p8, n7, p7, gap = solo(t["v8"], t["v7"], by_id)
        print(f"    {k:<18} coins {len(by_id):<4} v7 BUY {len(t['v7'])} {_f(_pts([by_id[i] for i in t['v7']]))} | "
              f"v8 BUY {len(t['v8'])} {_f(_pts([by_id[i] for i in t['v8']]))} | solo v7-only {n7} {_f(p7)}, "
              f"v8-only {n8} {_f(p8)}, v8 - v7 {_f(gap)} pts/SOL")
    print("\nRead against calreport's v7_stored solo gap: a v8 lead smaller than that is noise.")


def _arg(name: str, default=None):
    return sys.argv[sys.argv.index(name) + 1] if name in sys.argv else default


if __name__ == "__main__":
    import signal
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    V7_FROM = _arg("--v7-from")
    D = lambda s: date.fromisoformat(s)
    if cmd in ("fields", "calibrate", "calreport"):
        d0, d1 = D(_arg("--from", "2026-11-08")), D(_arg("--to", "2026-11-10"))
        run = _arg("--run", "cal")
    else:
        d0, d1 = D(_arg("--from", "2026-11-01")), D(_arg("--to", "2026-11-10"))
        run = _arg("--run", "nov")
    if cmd in ("calibrate", "november"):
        if not tb.API_KEY:
            sys.exit("no ANTHROPIC_API_KEY in .env")
        ensure_tables()
        b = Budget(run, float(_arg("--max-usd", "2")), float(_arg("--sleep", "1.5")))
        print(f"[replay] {cmd} {d0}..{d1} run '{run}': ceiling ${b.max_usd:.2f} (spent so far ${b.spent():.2f}), "
              f"model {tb.MODEL}, {b.sleep:g}s between calls, dev-bundled skip {tb.SKIP_DEV_BUNDLED:g}", flush=True)
        if cmd == "calibrate":
            calibrate(d0, d1, b, int(_arg("--per-day", "25")))
        else:
            november(d0, d1, b)
    elif cmd == "fields":
        fields(d0, d1, "--fast" in sys.argv)
    elif cmd == "calreport":
        ensure_tables()
        calreport(d0, d1, run)
    elif cmd == "report":
        ensure_tables()
        report(d0, d1, run)
    else:
        print(__doc__)
