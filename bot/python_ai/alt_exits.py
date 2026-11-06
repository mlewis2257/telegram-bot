"""
alt_exits.py — forward test of early-exit rules on qsim's REAL quotes (shadow only).

WHY (2026-11-06). Looking back over Nov 5-6, three early exits beat qsim's hold-to-stop/bank on
solearlytrending (worse or flat on solwhaletrending):
  sell45        sell at the first quote 45 s or more after entry (the control: no brain at all)
  brain_plan    sell at the first quote after trader_brain's 45 s plan says exit_early / skip
                (keep holding when it says normal / hold_longer)
  holder1/3     sell at the first quote after holder_watch shows the top-10 real wallets of the
                first in-trade snapshot have shed >= 1% / 3% of supply
  brain_or_h1   whichever of brain_plan / holder1 fires first
Those numbers were measured after the fact on two rug-heavy days. This records, on every new
qsim quote, the first quote at which each rule would have sold — so the days from now on are
a clean forward test with real prices, on the same coins qsim actually trades.

Shadow only: observe() never changes qsim's exit and swallows every error. One row per
(call_id, rule) in qsim_alt_exits, written the first time the rule fires. A rule that never
fires before qsim's own exit simply ends where qsim ended.

Report:  python3 alt_exits.py --report [--since 2026-11-07]
"""

from __future__ import annotations

import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import db  # noqa: E402

SELL_AFTER_SECS = 45
RULES = ("sell45", "brain_plan", "holder1", "holder3", "brain_or_h1")
_LOOKUP_EVERY_S = 10.0

_done: dict[int, set] = {}            # call_id -> rules already recorded
_plan: dict[int, tuple] = {}          # call_id -> (judged_at, plan) once found
_last_lookup: dict[int, float] = {}   # call_id -> monotonic time of last brain/holder lookup
_shed: dict[int, tuple] = {}          # call_id -> (snapshot time, shed % of supply) latest
_table_ok = False


def _ensure_table() -> None:
    global _table_ok
    if _table_ok:
        return
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS qsim_alt_exits (
                call_id      integer NOT NULL,
                rule         text NOT NULL,
                triggered_at timestamptz NOT NULL DEFAULT now(),
                real_mult    numeric,
                sol_out      numeric,
                detail       text,
                PRIMARY KEY (call_id, rule)
            )""")
    conn.commit()
    _table_ok = True


def _real(snap) -> dict[str, float]:
    return {h["wallet"]: float(h["ui_amount"]) for h in snap or []
            if h.get("on_curve") and h.get("ui_amount") is not None}


def _refresh(call_id: int) -> None:
    """Load the brain plan (once) and the latest holder shed %, at most every 10 s per position."""
    now = time.monotonic()
    if now - _last_lookup.get(call_id, 0.0) < _LOOKUP_EVERY_S:
        return
    _last_lookup[call_id] = now
    conn = db.get_conn()
    with conn.cursor() as cur:
        if call_id not in _plan:
            cur.execute("SELECT judged_at, plan FROM trader_brain WHERE call_id = %s", (call_id,))
            r = cur.fetchone()
            if r:
                _plan[call_id] = (r[0], r[1])
        cur.execute("SELECT taken_at, supply_ui, holders FROM holder_watch WHERE call_id = %s "
                    "ORDER BY taken_at", (call_id,))
        snaps = cur.fetchall()
    if len(snaps) >= 2:
        sup = next((float(s[1]) for s in snaps if s[1]), None)
        if sup:
            ref = dict(sorted(_real(snaps[0][2]).items(), key=lambda kv: -kv[1])[:10])
            cur_ = _real(snaps[-1][2])
            shed = sum(max(0.0, amt - cur_.get(w, 0.0)) for w, amt in ref.items()) / sup * 100
            _shed[call_id] = (snaps[-1][0], shed)


def observe(pos: dict, real_mult: float, sol_out: float) -> None:
    """Called by qsim on every fresh (pre-bank) sell quote of an open position. Never raises."""
    try:
        _ensure_table()
        call_id = int(pos["call_id"])
        if call_id not in _done:
            conn = db.get_conn()
            with conn.cursor() as cur:
                cur.execute("SELECT rule FROM qsim_alt_exits WHERE call_id = %s", (call_id,))
                _done[call_id] = {r[0] for r in cur.fetchall()}
        done = _done[call_id]
        if len(done) == len(RULES):
            return
        _refresh(call_id)
        now = datetime.now(timezone.utc)
        entry_time = pos.get("entry_time")
        if entry_time is not None and entry_time.tzinfo is None:
            entry_time = entry_time.replace(tzinfo=timezone.utc)
        fire = {}
        if entry_time is not None and (now - entry_time).total_seconds() >= SELL_AFTER_SECS:
            fire["sell45"] = "45s"
        plan = _plan.get(call_id)
        if plan and plan[1] in ("exit_early", "skip") and plan[0] < now:
            fire["brain_plan"] = plan[1]
        shed = _shed.get(call_id)
        if shed and shed[0] < now:
            if shed[1] >= 1.0:
                fire["holder1"] = f"shed {shed[1]:.1f}%"
            if shed[1] >= 3.0:
                fire["holder3"] = f"shed {shed[1]:.1f}%"
        if "brain_plan" in fire or "holder1" in fire:
            fire["brain_or_h1"] = fire.get("brain_plan") or fire.get("holder1")
        new = {k: v for k, v in fire.items() if k not in done}
        if not new:
            return
        conn = db.get_conn()
        with conn.cursor() as cur:
            for rule, detail in new.items():
                cur.execute("INSERT INTO qsim_alt_exits (call_id, rule, real_mult, sol_out, detail) "
                            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                            (call_id, rule, real_mult, sol_out, detail))
        conn.commit()
        done.update(new)
    except Exception:
        try:
            db.safe_rollback()
        except Exception:
            pass


def forget(call_id: int) -> None:
    for d in (_done, _plan, _last_lookup, _shed):
        d.pop(call_id, None)


def report(since: str) -> None:
    conn = db.get_conn()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT qp.call_id, qp.channel_handle, qp.entry_time::date, qp.sol_in, qp.sol_out, qp.exit_time,
                   coalesce(json_object_agg(a.rule, json_build_array(a.sol_out, a.triggered_at))
                            FILTER (WHERE a.rule IS NOT NULL), '{}')
            FROM qsim_positions qp
            LEFT JOIN qsim_alt_exits a ON a.call_id = qp.call_id
            WHERE qp.status = 'closed' AND qp.sol_in > 0 AND qp.entry_time >= %s
              AND EXISTS (SELECT 1 FROM qsim_alt_exits x WHERE x.call_id = qp.call_id)
            GROUP BY 1, 2, 3, 4, 5, 6""", (since,))
        rows = cur.fetchall()
    if not rows:
        print(f"no closed qsim positions with alt-exit data since {since}")
        return

    def alt_pnl(sol_in, sol_out, exit_t, fired, rule):
        f = fired.get(rule)
        if f:
            t = datetime.fromisoformat(str(f[1]).replace("Z", "+00:00")) if isinstance(f[1], str) else f[1]
            if exit_t is None or t < exit_t:
                return float(f[0]) - sol_in, True
        return float(sol_out or 0) - sol_in, False

    by = defaultdict(lambda: defaultdict(lambda: [0, 0.0, 0.0, 0, 0.0]))   # scope -> rule -> [n, actual, alt, fired, staked]
    for call_id, ch, day, s_in, s_out, exit_t, fired in rows:
        s_in = float(s_in)
        actual = float(s_out or 0) - s_in
        for scope in ("ALL", ch, f"{day} {ch}"):
            for rule in RULES:
                p, did = alt_pnl(s_in, s_out, exit_t, fired, rule)
                b = by[scope][rule]
                b[0] += 1
                b[1] += actual
                b[2] += p
                b[3] += did
                b[4] += s_in
    print(f"{len(rows)} closed qsim positions since {since} with alt-exit data (forward, real quotes).")
    print("actual = what qsim did; alt = P&L if the rule had sold at the first quote after it fired.\n")
    order = ["ALL"] + sorted(k for k in by if k != "ALL" and " " not in k) + sorted(k for k in by if " " in k)
    for scope in order:
        print(f"  {scope}")
        for rule in RULES:
            n, act, alt, fired, stake = by[scope][rule]
            print(f"    {rule:12s} n={n:4d}  fired {fired / n * 100:5.1f}%   actual {act:+8.4f}   "
                  f"alt {alt:+8.4f}   delta {alt - act:+8.4f} SOL   ({(alt - act) / stake * 100 if stake else 0:+.1f} pts)")
    print("\nsell45 is the control: a rule only adds something if it beats sell45 as well as actual,\n"
          "per channel, on several separate days.")


if __name__ == "__main__":
    i = sys.argv.index("--since") if "--since" in sys.argv else -1
    report(sys.argv[i + 1] if i > 0 else "2026-11-07")
