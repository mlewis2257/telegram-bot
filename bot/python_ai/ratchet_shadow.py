"""
ratchet_shadow.py — run the ratcheting trail as a SHADOW on live qsim positions.

WHY THIS EXISTS
---------------
`qsim_ratchet_trail.py` measured the policy on post-exit quotes and could not
settle it. The tail capture is real and survived every variant (+6.10 SOL on the
63 coins that reached 5x+ in 17d), but the cost of NOT banking at 2x came back
as -4.60 from a dataset that also reported the profit floor as -9.05 when the
live config books +4.13 on the same positions. That contradiction is the
measurement, not the policy: once qsim sells, it drops the coin to a ~30-minute
post-exit probe, and both a trail and a floor need to SEE the path down to work.

This runs the same policy against the 15-90s quote stream a HELD position
actually gets, decides what it would have done, and records it. It changes
nothing. qsim's real exit is untouched.

THE POLICY
----------
    exit level = max(hard_stop, armed_floor, peak * (1 - trail))

    trail:  peak < 3x -> base;  >=3x -> t3;  >=5x -> t5;  >=10x -> t10
    floor:  arms once peak >= floor_arm, then holds floor

The tighter trail at each checkpoint is the point: lock_or_bank and lock_trail
hold ONE level set when the coin first proved itself, so a coin at 12x is still
protected by a line drawn at 1.75x.

READING THE RESULT
------------------
`--report` pairs every shadow decision against what qsim actually booked on the
SAME position. Same coin, same quotes, same window — so the comparison cannot be
an exposure artifact, which is what sank the dev gate and the weekday split.

Do NOT read it before the 5x+ bucket has meaningful n. The whole thesis is that
a handful of monsters pay for the coins that stall at 2x, so the answer is
dominated by trades that occur ~4% of the time. At ~90 positions/day that is
~4/day, so a fortnight is the minimum and the PnL column will still be noisy
while the rate columns are not.

STATE IS IN MEMORY. A pm2 restart loses each open position's running peak, so
its shadow decision restarts from the current price and will look tighter than
it should. `obs_seen` records how many quotes the decision actually saw — treat
a low count on a long-lived position as a restart artifact, not a decision.

    RATCHET_SHADOW_ENABLED=true          # off by default
    RATCHET_SHADOW_BASE=0.50             # trail below 3x
    RATCHET_SHADOW_T3=0.40 / _T5=0.30 / _T10=0.20
    RATCHET_SHADOW_FLOOR_ARM=1.30        # 0 disables the floor
    RATCHET_SHADOW_FLOOR=1.10
    RATCHET_SHADOW_STOP=0.80

    python3 ratchet_shadow.py --report
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))

import db  # noqa: E402

ENABLED   = os.getenv("RATCHET_SHADOW_ENABLED", "false").strip().lower() == "true"
BASE      = float(os.getenv("RATCHET_SHADOW_BASE", "0.50"))
T3        = float(os.getenv("RATCHET_SHADOW_T3", "0.40"))
T5        = float(os.getenv("RATCHET_SHADOW_T5", "0.30"))
T10       = float(os.getenv("RATCHET_SHADOW_T10", "0.20"))
FLOOR_ARM = float(os.getenv("RATCHET_SHADOW_FLOOR_ARM", "1.30"))
FLOOR     = float(os.getenv("RATCHET_SHADOW_FLOOR", "1.10"))
STOP      = float(os.getenv("RATCHET_SHADOW_STOP", "0.80"))

# call_id -> {"peak": float, "armed": bool, "seen": int, "done": bool}
_state: dict[int, dict] = {}


def ensure_table() -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS qsim_ratchet_shadow (
                id          bigserial PRIMARY KEY,
                call_id     integer NOT NULL UNIQUE,
                decision    text NOT NULL,
                exit_mult   numeric NOT NULL,
                peak_mult   numeric NOT NULL,
                obs_seen    integer NOT NULL,
                decided_at  timestamptz NOT NULL DEFAULT now()
            )
        """)
    conn.commit()


def _trail_for(peak: float) -> float:
    if peak >= 10.0:
        return T10
    if peak >= 5.0:
        return T5
    if peak >= 3.0:
        return T3
    return BASE


def _record(call_id: int, decision: str, exit_mult: float,
            peak: float, seen: int) -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO qsim_ratchet_shadow
                (call_id, decision, exit_mult, peak_mult, obs_seen)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (call_id) DO NOTHING
        """, (call_id, decision, exit_mult, peak, seen))
    conn.commit()


def observe(call_id: int, mult: float) -> None:
    """One quote tick on a held position. Never raises, never affects the exit."""
    if not ENABLED:
        return
    try:
        if mult is None or mult <= 0:
            return
        st = _state.get(call_id)
        if st is None:
            st = _state[call_id] = {"peak": mult, "armed": False,
                                    "seen": 0, "done": False}
        st["seen"] += 1
        if st["done"]:
            return
        if mult > st["peak"]:
            st["peak"] = mult
        peak = st["peak"]
        if FLOOR_ARM > 0 and peak >= FLOOR_ARM:
            st["armed"] = True
        lvl_floor = FLOOR if st["armed"] else 0.0
        level = max(STOP, lvl_floor, peak * (1.0 - _trail_for(peak)))
        if mult <= level:
            if lvl_floor > 0 and level == lvl_floor:
                why = "floor"
            elif level == STOP:
                why = "stop"
            else:
                why = "trail"
            st["done"] = True
            _record(call_id, why, mult, peak, st["seen"])
    except Exception:
        pass


def finalize(call_id: int, mult: float) -> None:
    """qsim closed the position. If the ratchet never fired, book the terminal
    value — the last price it saw, win or lose.

    Not optional: letting an un-fired row fall back to qsim's own result would
    pair this policy's upside with the live config's stop-protected downside,
    which is the bor_ free option (+13.27 until bounded, then -112.75).
    """
    if not ENABLED:
        return
    try:
        st = _state.get(call_id)
        if st is not None and not st["done"] and mult is not None and mult >= 0:
            _record(call_id, "terminal", mult, max(st["peak"], mult), st["seen"])
    except Exception:
        pass


def clear(call_id: int) -> None:
    _state.pop(call_id, None)


def report(days: float = 14.0) -> None:
    from psycopg2.extras import RealDictCursor
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("""
            SELECT s.decision, s.exit_mult, s.peak_mult, s.obs_seen,
                   qp.sol_in, qp.sol_out, qp.exit_reason
            FROM qsim_ratchet_shadow s
            JOIN qsim_positions qp ON qp.call_id = s.call_id
            WHERE qp.status = 'closed'
              AND qp.entry_time >= now() - (%s || ' days')::interval
              AND qp.sol_in > 0
        """, (days,))
        rows = [dict(r) for r in cur.fetchall()]
    if not rows:
        print("no paired shadow decisions yet — "
              "needs RATCHET_SHADOW_ENABLED=true and closed positions")
        return

    dep = sum(float(r["sol_in"]) for r in rows)
    act = sum(float(r["sol_out"] or 0.0) - float(r["sol_in"]) for r in rows)
    sh = sum(float(r["sol_in"]) * float(r["exit_mult"]) - float(r["sol_in"])
             for r in rows)

    print(f"RATCHET SHADOW  last {days:g}d   paired positions: {len(rows)}")
    print(f"  trail base {BASE:.0%} / 3x {T3:.0%} / 5x {T5:.0%} / 10x {T10:.0%}"
          f"   floor {FLOOR_ARM:g}->{FLOOR:g}   stop {STOP:g}")
    print()
    print(f"  deployed                 {dep:.2f} SOL")
    print(f"  actual (qsim booked)     {act:+.4f} SOL   {100*act/dep:+.2f}%/SOL")
    print(f"  ratchet (shadow)         {sh:+.4f} SOL   {100*sh/dep:+.2f}%/SOL")
    print(f"  DIFFERENCE               {sh - act:+.4f} SOL")
    print()
    print(f"  {'bucket':<16}{'n':>5}{'actual':>11}{'ratchet':>11}{'delta':>11}")
    tiers = (("reached 10x+", 10.0), ("reached 5-10x", 5.0), ("reached 3-5x", 3.0),
             ("reached 2-3x", 2.0), ("never 2x", 0.0))
    hi = float("inf")
    for label, lo in tiers:
        sub = [r for r in rows if lo <= float(r["peak_mult"]) < hi]
        hi = lo
        if not sub:
            continue
        a = sum(float(r["sol_out"] or 0.0) - float(r["sol_in"]) for r in sub)
        h = sum(float(r["sol_in"]) * float(r["exit_mult"]) - float(r["sol_in"])
                for r in sub)
        print(f"  {label:<16}{len(sub):>5}{a:>11.4f}{h:>11.4f}{h - a:>11.4f}")

    print()
    print(f"  {'decision':<16}{'n':>5}{'actual':>11}{'ratchet':>11}{'delta':>11}")
    for why in ("trail", "floor", "stop", "terminal"):
        sub = [r for r in rows if r["decision"] == why]
        if not sub:
            continue
        a = sum(float(r["sol_out"] or 0.0) - float(r["sol_in"]) for r in sub)
        h = sum(float(r["sol_in"]) * float(r["exit_mult"]) - float(r["sol_in"])
                for r in sub)
        print(f"  {why:<16}{len(sub):>5}{a:>11.4f}{h:>11.4f}{h - a:>11.4f}")

    thin = [r for r in rows if int(r["obs_seen"]) <= 2]
    if thin:
        print()
        print(f"  {len(thin)} decisions saw <=2 quotes — likely pm2-restart "
              f"artifacts, not decisions")
    print()
    print("  Paired on identical positions, so this cannot be an exposure")
    print("  artifact. The 5x+ rows carry the thesis and are ~4% of trades;")
    print("  give them real n before reading the total.")


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--report", action="store_true")
    ap.add_argument("--days", type=float, default=14.0)
    a = ap.parse_args()
    try:
        ensure_table()
        if a.report:
            report(a.days)
        else:
            print(f"ratchet_shadow: enabled={ENABLED} base={BASE} t3={T3} "
                  f"t5={T5} t10={T10} floor={FLOOR_ARM}->{FLOOR} stop={STOP}")
            print("run with --report once positions have closed")
    finally:
        db.close_conn()
