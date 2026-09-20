"""
dev_gate.py — the clean-deployer entry filter, live.

WHAT IT GATES ON
----------------
The token's on-chain DEPLOYER. Backtested in dev_history_edge.py over 30 days on
the realistic information set (deployments known at once, rug verdicts only once
they rug):

    bucket          %/SOL    rug%   2x%   run:lose
    any prior rug  -20.75    27.7  18.8     0.68
    0 prior        -17.39    17.1  16.0     0.94
    1-2 clean      -12.22    12.3  20.2     1.64
    3+ clean        -2.71     7.5  27.5     3.65   <- what this admits

It is not merely subtractive: rugs fall 56% AND the 2x rate rises 72%, the 5x
rate 167%, the 10x rate 229%. On solwhaletrending alone the bucket is +1.33%/SOL
with a CI disjoint from its own 0-prior baseline. On solhousesignal it is
-14.96%, so DEV_GATE_CHANNELS exists and should list solwhaletrending only.

It is NOT established as profitable — the bucket's own CI straddles zero. This
runs in shadow first for exactly that reason.

THREE MODES
-----------
  off      no evaluation at all
  shadow   evaluate, LOG, and always allow. Costs one RPC call per candidate and
           blocks nothing. This is how you learn the real latency and the real
           block rate before any decision depends on them.
  enforce  actually block

Shadow is the default. Every decision is written to dev_gate_decisions, so after
a few days you can ask: how often would it have blocked, how long did resolution
take, and -- once those positions close -- did the blocked ones really do worse.
That last question is the whole point, and it cannot be answered by a backtest
on the sample the filter was found in.

LATENCY IS THE RISK
-------------------
Resolution needs an RPC round trip before entry, and the first observable quote
is already 0.9762 of the call price. On a coin moving several percent per second
a slow gate costs more than it saves. Two things keep it cheap: DAS getAsset is
tried first (one call), and the first_tx fallback is fast HERE even though it
was slow in the backfill, because a freshly-called mint has a handful of
signatures rather than thousands. DEV_GATE_TIMEOUT_MS bounds it regardless, and
a timeout FAILS OPEN.

FAIL-OPEN, ALWAYS
-----------------
Any error, timeout, or unresolved creator allows the trade. A filter that is
down must not silently become a filter that rejects everything -- that failure
mode looks like "the strategy stopped losing" and is indistinguishable from
success until you notice volume went to zero.
"""

from __future__ import annotations

import asyncio
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import db

# One mode cannot serve both callers. qsim needs SHADOW to keep trading the
# blocked group — that is the control arm of the forward test, and without it a
# filtered book can only be compared to a different month, which is the confound
# that made September look 10 points better than August on the mcap ceiling
# alone. Live needs ENFORCE, because there the point is to not take the trade.
MODE = os.getenv("DEV_GATE_MODE", "shadow").strip().lower()
MODE_QSIM = (os.getenv("DEV_GATE_MODE_QSIM", "").strip().lower() or MODE)
MODE_LIVE = (os.getenv("DEV_GATE_MODE_LIVE", "").strip().lower() or MODE)


def mode_for(context: str) -> str:
    return MODE_LIVE if context == "live" else MODE_QSIM
MIN_PRIOR = int(os.getenv("DEV_GATE_MIN_PRIOR", "3"))
MAX_PRIOR_RUGS = int(os.getenv("DEV_GATE_MAX_PRIOR_RUGS", "0"))
TIMEOUT_MS = float(os.getenv("DEV_GATE_TIMEOUT_MS", "1200"))
FACTORY_MIN = int(os.getenv("DEV_GATE_FACTORY_MIN", "40"))
# Empty = every channel. The edge is channel-dependent; solhousesignal is
# -14.96%/SOL even filtered, so it should not be listed.
CHANNELS = {c.strip().lstrip("@").lower()
            for c in os.getenv("DEV_GATE_CHANNELS", "").split(",") if c.strip()}


_TABLE_READY = False
_LOG_FAILED_ONCE = False


@dataclass(frozen=True)
class Decision:
    allowed: bool
    reason: str
    creator: str | None
    source: str | None
    prior_n: int
    prior_rugs: int
    latency_ms: float


def ensure_table() -> None:
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dev_gate_decisions (
                id          bigserial PRIMARY KEY,
                call_id     integer,
                mint_address text,
                channel     text,
                mode        text,
                allowed     boolean,
                reason      text,
                creator     text,
                source      text,
                prior_n     integer,
                prior_rugs  integer,
                latency_ms  numeric,
                context     text,
                decided_at  timestamptz NOT NULL DEFAULT now()
            )
        """)
        # Added after the table shipped, so existing installs get it too.
        cur.execute("ALTER TABLE dev_gate_decisions "
                    "ADD COLUMN IF NOT EXISTS context text")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_dev_gate_call "
                    "ON dev_gate_decisions (call_id)")
    conn.commit()


def _prior_history(creator: str, as_of: datetime) -> tuple[int, int]:
    """(prior_n, prior_rugs) on the live information set.

    prior_n counts tokens FIRST CALLED before now — a deployment is visible on
    chain immediately. prior_rugs counts only tokens whose position had already
    closed at <= -80% — a rug verdict is not knowable before it happens. Getting
    this split wrong is what made the first backtest look far better than it was.
    """
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            WITH mine AS (
                SELECT t.id AS token_id, min(c.created_at) AS first_call
                FROM token_creators tc
                JOIN tokens t ON t.id = tc.token_id
                JOIN calls  c ON c.token_id = t.id
                WHERE tc.creator_address = %s
                GROUP BY t.id
            )
            SELECT
              count(*) FILTER (WHERE m.first_call < %s)                       AS prior_n,
              count(*) FILTER (WHERE m.first_call < %s AND r.rugged_at < %s)  AS prior_rugs,
              count(*)                                                        AS total_n
            FROM mine m
            LEFT JOIN LATERAL (
                SELECT min(qp.exit_time) AS rugged_at
                FROM qsim_positions qp
                JOIN calls c2 ON c2.id = qp.call_id
                WHERE c2.token_id = m.token_id
                  AND qp.status = 'closed' AND qp.pnl_pct <= -80
            ) r ON true
        """, (creator, as_of, as_of, as_of))
        row = cur.fetchone()
    prior_n, prior_rugs, total_n = (int(row[0] or 0), int(row[1] or 0), int(row[2] or 0))
    if total_n >= FACTORY_MIN:
        # A launchpad pays deploy fees for its users, so its fee payer shows up
        # as the "deployer" of hundreds of unrelated tokens and would otherwise
        # present a long spotless record. Treat it as no information.
        return (0, 0)
    return (prior_n, prior_rugs)


async def _resolve_creator(mint: str) -> tuple[str | None, str | None]:
    import token_creator_backfill as tcb
    loop = asyncio.get_running_loop()
    addr, src = await loop.run_in_executor(None, tcb.creator_via_das, mint)
    if not addr:
        addr, src = await loop.run_in_executor(None, tcb.creator_via_first_tx, mint)
    return (addr or None, src or None)


def _remember(mint: str, creator: str, source: str) -> None:
    """Cache into token_creators so the history query sees it next time."""
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO token_creators (token_id, mint_address, creator_address, creator_source)
            SELECT t.id, %s, %s, %s FROM tokens t WHERE t.mint_address = %s
            ON CONFLICT (token_id) DO NOTHING
        """, (mint, creator, source, mint))
    conn.commit()


async def check(call_id: int | None, mint: str, channel: str | None,
                context: str = "qsim") -> Decision:
    """Never raises. Any failure allows the trade — see FAIL-OPEN above.

    `context` selects the mode, so qsim can stay in shadow (keeping both arms of
    the forward test) while live enforces.
    """
    t0 = time.monotonic()
    mode = mode_for(context)

    def done(allowed, reason, creator=None, source=None, p_n=0, p_r=0) -> Decision:
        ms = (time.monotonic() - t0) * 1000.0
        d = Decision(allowed, reason, creator, source, p_n, p_r, ms)
        try:
            _log(call_id, mint, channel, d, context, mode)
        except Exception as e:
            global _LOG_FAILED_ONCE
            if not _LOG_FAILED_ONCE:
                _LOG_FAILED_ONCE = True
                print(f"[dev_gate] LOGGING FAILED — decisions are not being "
                      f"recorded: {type(e).__name__} {e}", flush=True)
        return d

    if mode == "off":
        return done(True, "gate_off")
    ch = (channel or "").lstrip("@").lower()
    if CHANNELS and ch not in CHANNELS:
        return done(True, "channel_not_gated")

    try:
        creator, source = await asyncio.wait_for(
            _resolve_creator(mint), timeout=TIMEOUT_MS / 1000.0)
    except asyncio.TimeoutError:
        return done(True, "resolve_timeout")
    except Exception as e:
        return done(True, f"resolve_error:{type(e).__name__}")

    if not creator:
        return done(True, "creator_unresolved")

    try:
        await asyncio.get_running_loop().run_in_executor(
            None, _remember, mint, creator, source or "live")
        p_n, p_r = await asyncio.get_running_loop().run_in_executor(
            None, _prior_history, creator, datetime.now(timezone.utc))
    except Exception as e:
        return done(True, f"history_error:{type(e).__name__}", creator, source)

    ok = p_n >= MIN_PRIOR and p_r <= MAX_PRIOR_RUGS
    reason = "pass" if ok else (f"prior_rugs={p_r}" if p_r > MAX_PRIOR_RUGS
                                else f"prior_n={p_n}<{MIN_PRIOR}")
    if mode == "shadow":
        # Evaluated and recorded, but the trade proceeds. The reason column
        # still says what enforce WOULD have done.
        return done(True, f"shadow:{reason}", creator, source, p_n, p_r)
    return done(ok, reason, creator, source, p_n, p_r)


def _log(call_id, mint, channel, d: Decision, context: str, mode: str) -> None:
    global _TABLE_READY
    if not _TABLE_READY:
        ensure_table()          # nothing else calls this — the gate is the only writer
        _TABLE_READY = True
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO dev_gate_decisions
                (call_id, mint_address, channel, mode, allowed, reason,
                 creator, source, prior_n, prior_rugs, latency_ms, context)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (call_id, mint, channel, mode, d.allowed, d.reason, d.creator,
              d.source, d.prior_n, d.prior_rugs, round(d.latency_ms, 1), context))
    conn.commit()
