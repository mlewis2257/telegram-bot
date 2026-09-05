"""
qsim_quote_capture_replay.py — replay qsim rows using raw Jupiter quote spikes.

This is a read-only diagnostic for the live/shadow/qsim gap:

    "If Jupiter actually showed a temporary sell quote spike, how much did qsim
     leave on the table by not banking it?"

It does NOT use shadow feed prices to invent exits. Shadow is included only as a
comparison column. The replay variants are intentionally simple:

    floor_2x / floor_3x / floor_5x
        If any observed Jupiter quote reaches the threshold, exit at exactly the
        threshold. This is conservative and ignores upside beyond the threshold.

    obs_2x / obs_3x / obs_5x
        If any observed Jupiter quote reaches the threshold, exit at the first
        observed quote multiple that crossed it.

    confirm_2x / confirm_3x / confirm_5x
        Same as obs_*, but requires two consecutive quote observations above the
        threshold. Useful for measuring whether a rule would be too slow.

    bank_1.2x ... bank_2x
        Exit at the first observed quote multiple at/above that level.

    p50_bank_1.3x
        Sell 50% at the first observed quote >= 1.3x, then let the remaining
        50% use the original qsim outcome. Tests runner-preserving de-risking.

    p50_bank_1.3x_stop_1x
        Sell 50% at 1.3x, then exit the remaining 50% if the quote falls back
        to 1x. Tests de-risking plus "do not let the moonbag round-trip."

    no_1.2x_stop_0.85x
        Exit if quote falls to 0.85x before ever reaching 1.2x. Tests cutting
        dead-on-arrival trades before the standard hard stop.

    lock_1.5x_1.2x
        Arm after quote reaches 1.5x, then exit if it falls back to 1.2x.

    lock_or_bank_1.5x_1.2x
        Same as lock_*, but if the floor never fires before qsim closes, bank at
        the final observed quote. This tests "never let an armed winner become a
        full loser" and is intentionally less conservative.

    bor_b1p4_a1p7_t3x_f1p25_w10m_s3
        "Bank-or-run": bank normal 1.4x winners immediately, but if the first
        bank quote is already a fast 1.7x+ strength print, hold up to 10 minutes
        for 3x while protecting with a 1.25x floor and 3 stalled ticks.

    cdelay_b1p4_hs25_band0p5to0p7_life0p9_age5_w3_floor0p4_rec1x
        "Conditional delay": bank normal 1.4x winners immediately. If a hard
        stop triggers quickly inside a recoverable-looking 0.5x-0.7x band after
        prior quote life near 0.9x, delay briefly for a bounce while protecting
        with a disaster floor. This tests the COGE/XCAT/PUMP recovery shape
        without forgiving slow bleeds or near-zero rugs.

Examples:
    python3 qsim_quote_capture_replay.py --days 1 --channel solwhaletrending --lane low_score --variant early --detail
    python3 qsim_quote_capture_replay.py --days 7 --channel solwhaletrending --lane low_score --variant early --max-entry-ratio 2
    python3 qsim_quote_capture_replay.py --days 30 --since '2026-08-28 00:00 UTC' --include-post-exit --fallback last_quote
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

sys.path.insert(0, os.path.dirname(__file__))

from exit_config import EXIT_A_PAPER, EXIT_RIDE, apply_exit_config


MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5
MAX_QOBS_MULT = float(os.getenv("QSIM_REPLAY_MAX_QOBS_MULT", "50"))
THRESHOLDS = (2.0, 3.0, 5.0)
BANK_LEVELS = (1.20, 1.30, 1.40, 1.50, 1.75, 2.0)
BANK_FRACTIONS = (0.25, 0.50, 0.75)
BANK_REMAINDER_STOPS = (0.85, 1.0, 1.10)
NO_BOUNCE_THRESHOLDS = (1.10, 1.20, 1.30)
NO_BOUNCE_STOPS = (0.90, 0.85, 0.80, 0.75)
LOCK_FLOORS = (
    (1.30, 1.10),
    (1.40, 1.15),
    (1.50, 1.20),
    (1.75, 1.35),
    (2.00, 1.55),
)
RUNNER_WINDOW_POLICIES = (
    {
        "name": "runner_a2x_w10m_f1x_r5x",
        "arm": 2.0,
        "window_mins": 10.0,
        "floor": 1.0,
        "release": 5.0,
    },
    {
        "name": "runner_a2x_w12m_f1x_r5x",
        "arm": 2.0,
        "window_mins": 12.0,
        "floor": 1.0,
        "release": 5.0,
    },
    {
        "name": "runner_a2x_w10m_f1p2_r5x",
        "arm": 2.0,
        "window_mins": 10.0,
        "floor": 1.2,
        "release": 5.0,
    },
    {
        "name": "runner_a3x_w10m_f1p2_r5x",
        "arm": 3.0,
        "window_mins": 10.0,
        "floor": 1.2,
        "release": 5.0,
    },
)
BANK_OR_RUN_POLICIES = (
    {
        "name": "bor_b1p3_a1p7_t3x_f1p2_w10m_s3",
        "bank": 1.30,
        "runner_arm": 1.70,
        "target": 3.0,
        "floor": 1.20,
        "window_mins": 10.0,
        "stall_ticks": 3,
    },
    {
        "name": "bor_b1p4_a1p7_t3x_f1p25_w10m_s3",
        "bank": 1.40,
        "runner_arm": 1.70,
        "target": 3.0,
        "floor": 1.25,
        "window_mins": 10.0,
        "stall_ticks": 3,
    },
    {
        "name": "bor_b1p4_a1p7_t5x_f1p35_w10m_s3",
        "bank": 1.40,
        "runner_arm": 1.70,
        "target": 5.0,
        "floor": 1.35,
        "window_mins": 10.0,
        "stall_ticks": 3,
    },
    {
        "name": "bor_b1p4_a2x_t5x_f1p55_w10m_s3",
        "bank": 1.40,
        "runner_arm": 2.0,
        "target": 5.0,
        "floor": 1.55,
        "window_mins": 10.0,
        "stall_ticks": 3,
    },
)
SOFT_STOP_POLICIES = (
    {
        "name": "soft_hs25_life1p1_w3m_floor0p55_rec1x",
        "hard_stop": 0.75,
        "life": 1.10,
        "window_mins": 3.0,
        "disaster_floor": 0.55,
        "recover": 1.0,
    },
    {
        "name": "soft_hs25_life1p15_w5m_floor0p55_rec1p2",
        "hard_stop": 0.75,
        "life": 1.15,
        "window_mins": 5.0,
        "disaster_floor": 0.55,
        "recover": 1.20,
    },
    {
        "name": "soft_hs25_life1p2_w5m_floor0p5_rec1p3",
        "hard_stop": 0.75,
        "life": 1.20,
        "window_mins": 5.0,
        "disaster_floor": 0.50,
        "recover": 1.30,
    },
    {
        "name": "soft_hs30_life1p15_w5m_floor0p5_rec1p2",
        "hard_stop": 0.70,
        "life": 1.15,
        "window_mins": 5.0,
        "disaster_floor": 0.50,
        "recover": 1.20,
    },
)
BANK_SOFT_STOP_POLICIES = (
    {
        "name": "bank1p3_soft_hs25_life1p1_w3m_floor0p55_rec1x",
        "bank": 1.30,
        "hard_stop": 0.75,
        "life": 1.10,
        "window_mins": 3.0,
        "disaster_floor": 0.55,
        "recover": 1.0,
    },
    {
        "name": "bank1p4_soft_hs25_life1p1_w3m_floor0p55_rec1x",
        "bank": 1.40,
        "hard_stop": 0.75,
        "life": 1.10,
        "window_mins": 3.0,
        "disaster_floor": 0.55,
        "recover": 1.0,
    },
    {
        "name": "bank1p4_soft_hs25_life1p15_w5m_floor0p55_rec1p2",
        "bank": 1.40,
        "hard_stop": 0.75,
        "life": 1.15,
        "window_mins": 5.0,
        "disaster_floor": 0.55,
        "recover": 1.20,
    },
    {
        "name": "bank1p4_soft_hs25_life1p2_w5m_floor0p5_rec1p3",
        "bank": 1.40,
        "hard_stop": 0.75,
        "life": 1.20,
        "window_mins": 5.0,
        "disaster_floor": 0.50,
        "recover": 1.30,
    },
    {
        "name": "bank1p5_soft_hs25_life1p15_w5m_floor0p55_rec1p2",
        "bank": 1.50,
        "hard_stop": 0.75,
        "life": 1.15,
        "window_mins": 5.0,
        "disaster_floor": 0.55,
        "recover": 1.20,
    },
)
CONDITIONAL_STOP_DELAY_POLICIES = (
    {
        "name": "cdelay_b1p4_hs25_band0p5to0p7_life0p9_age5_w2_floor0p4_rec1x",
        "bank": 1.40,
        "hard_stop": 0.75,
        "delay_min": 0.50,
        "delay_max": 0.70,
        "life": 0.90,
        "max_age_mins": 5.0,
        "window_mins": 2.0,
        "disaster_floor": 0.40,
        "recover": 1.0,
    },
    {
        "name": "cdelay_b1p4_hs25_band0p5to0p7_life0p9_age5_w3_floor0p4_rec1x",
        "bank": 1.40,
        "hard_stop": 0.75,
        "delay_min": 0.50,
        "delay_max": 0.70,
        "life": 0.90,
        "max_age_mins": 5.0,
        "window_mins": 3.0,
        "disaster_floor": 0.40,
        "recover": 1.0,
    },
    {
        "name": "cdelay_b1p4_hs25_band0p5to0p7_life0p9_age5_w5_floor0p4_rec1x",
        "bank": 1.40,
        "hard_stop": 0.75,
        "delay_min": 0.50,
        "delay_max": 0.70,
        "life": 0.90,
        "max_age_mins": 5.0,
        "window_mins": 5.0,
        "disaster_floor": 0.40,
        "recover": 1.0,
    },
    {
        "name": "cdelay_b1p4_hs25_band0p55to0p7_life0p9_age5_w3_floor0p45_rec1x",
        "bank": 1.40,
        "hard_stop": 0.75,
        "delay_min": 0.55,
        "delay_max": 0.70,
        "life": 0.90,
        "max_age_mins": 5.0,
        "window_mins": 3.0,
        "disaster_floor": 0.45,
        "recover": 1.0,
    },
    {
        "name": "cdelay_b1p4_hs25_band0p5to0p7_life1x_age5_w3_floor0p4_rec1p2",
        "bank": 1.40,
        "hard_stop": 0.75,
        "delay_min": 0.50,
        "delay_max": 0.70,
        "life": 1.0,
        "max_age_mins": 5.0,
        "window_mins": 3.0,
        "disaster_floor": 0.40,
        "recover": 1.20,
    },
    {
        "name": "cdelay_b1p3_hs25_band0p5to0p7_life0p9_age5_w3_floor0p4_rec1x",
        "bank": 1.30,
        "hard_stop": 0.75,
        "delay_min": 0.50,
        "delay_max": 0.70,
        "life": 0.90,
        "max_age_mins": 5.0,
        "window_mins": 3.0,
        "disaster_floor": 0.40,
        "recover": 1.0,
    },
)
FALLBACK_CHOICES = ("current", "raw_config", "last_quote", "hard_stop", "hard_stop_35")
DYNAMIC_EXIT_POLICIES = (
    {
        "name": "dyn_a1p3_f1p05_s3_nb1p2_0p9",
        "arm": 1.30,
        "floor": 1.05,
        "stall_ticks": 3,
        "confirm_ticks": 1,
        "bounce": 1.20,
        "dead_stop": 0.90,
    },
    {
        "name": "dyn_a1p4_f1p1_s3_nb1p2_0p9",
        "arm": 1.40,
        "floor": 1.10,
        "stall_ticks": 3,
        "confirm_ticks": 1,
        "bounce": 1.20,
        "dead_stop": 0.90,
    },
    {
        "name": "dyn_a1p5_f1p15_s3_nb1p2_0p9",
        "arm": 1.50,
        "floor": 1.15,
        "stall_ticks": 3,
        "confirm_ticks": 1,
        "bounce": 1.20,
        "dead_stop": 0.90,
    },
    {
        "name": "dyn_c2_a1p3_f1p05_s3_nb1p2_0p9",
        "arm": 1.30,
        "floor": 1.05,
        "stall_ticks": 3,
        "confirm_ticks": 2,
        "bounce": 1.20,
        "dead_stop": 0.90,
    },
    {
        "name": "dyn_c2_a1p4_f1p1_s3_nb1p2_0p9",
        "arm": 1.40,
        "floor": 1.10,
        "stall_ticks": 3,
        "confirm_ticks": 2,
        "bounce": 1.20,
        "dead_stop": 0.90,
    },
    {
        "name": "dyn_c2_a1p5_f1p15_s3_nb1p2_0p9",
        "arm": 1.50,
        "floor": 1.15,
        "stall_ticks": 3,
        "confirm_ticks": 2,
        "bounce": 1.20,
        "dead_stop": 0.90,
    },
)
WINNER_ONLY_POLICIES = (
    {
        "name": "win_bank_a1p3",
        "arm": 1.30,
        "mode": "bank",
        "confirm_ticks": 1,
    },
    {
        "name": "win_bank_a1p4",
        "arm": 1.40,
        "mode": "bank",
        "confirm_ticks": 1,
    },
    {
        "name": "win_bank_a1p5",
        "arm": 1.50,
        "mode": "bank",
        "confirm_ticks": 1,
    },
    {
        "name": "win_c2_bank_a1p3",
        "arm": 1.30,
        "mode": "bank",
        "confirm_ticks": 2,
    },
    {
        "name": "win_c2_bank_a1p4",
        "arm": 1.40,
        "mode": "bank",
        "confirm_ticks": 2,
    },
    {
        "name": "win_c2_bank_a1p5",
        "arm": 1.50,
        "mode": "bank",
        "confirm_ticks": 2,
    },
    {
        "name": "win_a1p3_f1p05",
        "arm": 1.30,
        "floor": 1.05,
        "mode": "floor",
        "confirm_ticks": 1,
    },
    {
        "name": "win_a1p4_f1p1",
        "arm": 1.40,
        "floor": 1.10,
        "mode": "floor",
        "confirm_ticks": 1,
    },
    {
        "name": "win_a1p5_f1p15",
        "arm": 1.50,
        "floor": 1.15,
        "mode": "floor",
        "confirm_ticks": 1,
    },
    {
        "name": "win_a1p3_stall3",
        "arm": 1.30,
        "stall_ticks": 3,
        "mode": "stall",
        "confirm_ticks": 1,
    },
    {
        "name": "win_a1p4_stall3",
        "arm": 1.40,
        "stall_ticks": 3,
        "mode": "stall",
        "confirm_ticks": 1,
    },
    {
        "name": "win_a1p5_stall3",
        "arm": 1.50,
        "stall_ticks": 3,
        "mode": "stall",
        "confirm_ticks": 1,
    },
)

FILTER_ALIASES = {
    "score": "score",
    "entry_mcap": "qsim_entry",
    "qsim_entry": "qsim_entry",
    "shadow_entry": "shadow_entry",
    "feed_entry": "feed_entry_mcap",
    "feed_entry_mcap": "feed_entry_mcap",
    "q_feed_entry": "feed_entry_ratio",
    "liquidity": "liquidity",
    "vol_1h": "vol_1h",
    "turnover": "turnover",
    "vol_liq": "turnover",
    "vol_mcap": "vol_to_mcap",
    "vol_to_mcap": "vol_to_mcap",
    "age_min": "age_min",
    "holders": "holder_count",
    "holder_count": "holder_count",
    "hodlers": "hodl_count",
    "hodl_count": "hodl_count",
    "top10_pct": "top10_pct",
    "first20_pct": "first20_pct",
    "detector_sol": "detector_sol",
    "spent_sol": "detecting_sol_spent",
    "detecting_sol_spent": "detecting_sol_spent",
    "dev_best_mc": "dev_best_mcap",
    "dev_best_mcap": "dev_best_mcap",
    "dev_held_pct": "dev_pct_held",
    "dev_pct_held": "dev_pct_held",
    "dev_sold_pct": "dev_sold_pct",
    "dev_tokens": "dev_tokens_made",
    "dev_tokens_made": "dev_tokens_made",
    "bundle_pct": "bundle_pct",
    "fake_vol_pct": "fake_vol_pct",
    "sniper_pct": "sniper_pct",
    "bundle_cnt": "bundle_count",
    "bundle_count": "bundle_count",
    "sniper_cnt": "sniper_count",
    "sniper_count": "sniper_count",
}


SQL = """
WITH base AS (
    SELECT
        q.call_id,
        t.symbol,
        COALESCE(ch.handle, '?') AS channel,
        COALESCE(c.skip_reason, 'none') AS lane,
        q.variant,
        q.vip_tier,
        q.entry_time,
        q.exit_time,
        q.entry_price AS qsim_entry,
        q.exit_price AS qsim_exit,
        q.peak_multiplier AS qsim_peak,
        q.sol_in AS qsim_sol_in,
        q.pnl_sol AS qsim_pnl,
        q.pnl_pct AS qsim_pnl_pct,
        q.exit_reason AS qsim_reason,
        sp.entry_price AS shadow_entry,
        sp.exit_price AS shadow_exit,
        sp.peak_multiplier AS shadow_peak,
        sp.sol_in AS shadow_sol_in,
        sp.pnl_sol AS shadow_pnl,
        sp.pnl_pct AS shadow_pnl_pct,
        sp.exit_reason AS shadow_reason,
        c.mcap_at_call AS feed_entry_mcap,
        c.conviction_score AS score,
        t.liq_at_detection AS liquidity,
        t.vol_1h_at_detection AS vol_1h,
        t.vol_1h_at_detection / NULLIF(t.liq_at_detection, 0) AS turnover,
        t.vol_1h_at_detection / NULLIF(q.entry_price, 0) AS vol_to_mcap,
        t.token_age_minutes AS age_min,
        t.holder_count,
        t.hodl_count,
        t.top_10_holder_pct AS top10_pct,
        t.first_20_pct AS first20_pct,
        t.detecting_wallet_sol AS detector_sol,
        t.detecting_sol_spent,
        t.dev_best_mcap,
        t.dev_pct_held,
        t.dev_sold_pct,
        t.dev_tokens_made,
        t.bundle_pct_remaining AS bundle_pct,
        t.fake_vol_pct,
        t.sniper_pct_remaining AS sniper_pct,
        t.bundle_count,
        t.sniper_count,
        q.entry_price / NULLIF(c.mcap_at_call, 0) AS feed_entry_ratio
    FROM qsim_positions q
    JOIN calls c ON c.id = q.call_id
    JOIN tokens t ON t.id = q.token_id
    LEFT JOIN channels ch ON ch.id = c.channel_id
    LEFT JOIN shadow_positions sp
      ON sp.call_id = q.call_id
     AND sp.exit_variant = q.variant
     AND sp.status = 'closed'
    WHERE q.status = 'closed'
      AND q.entry_time >= now() - (%(days)s || ' days')::interval
      AND (%(since)s IS NULL OR q.entry_time >= %(since)s::timestamptz)
      AND (%(channel)s = 'any' OR COALESCE(ch.handle, '?') = %(channel)s)
      AND (%(lane)s = 'any' OR COALESCE(c.skip_reason, 'none') = %(lane)s)
      AND (%(variant)s = 'any' OR q.variant = %(variant)s)
      -- DATA QUALITY. A starved row is not a strategy observation: the 2026-09-05 incident
      -- left positions quoted ONCE in four hours, whose single quote is simultaneously their
      -- "peak" and their "exit". Replaying a policy over that path measures the outage, not
      -- the policy. Stale rows are excluded by default (--include-stale to override); NULL
      -- quality columns mean "not yet backfilled", and are kept so an un-backfilled window
      -- still returns rows rather than silently reading as empty.
      AND (%(include_stale)s OR left(COALESCE(q.exit_reason, ''), 6) <> 'stale_')
      AND (%(min_obs)s <= 0 OR COALESCE(q.obs_count, %(min_obs)s) >= %(min_obs)s)
      AND (%(max_gap_secs)s <= 0 OR COALESCE(q.max_gap_secs, 0) <= %(max_gap_secs)s)
      AND (%(min_entry_ratio)s IS NULL OR q.entry_price / NULLIF(sp.entry_price, 0) >= %(min_entry_ratio)s)
      AND (%(max_entry_ratio)s IS NULL OR q.entry_price / NULLIF(sp.entry_price, 0) <= %(max_entry_ratio)s)
      AND (%(raw)s OR NOT (
          COALESCE(sp.peak_multiplier, 0) > %(max_peak)s
       OR COALESCE(sp.pnl_pct, 0) > %(max_pnl)s
       OR COALESCE(sp.pnl_pct, 0) < %(min_pnl)s
       OR COALESCE(q.peak_multiplier, 0) > %(max_peak)s
       OR COALESCE(q.pnl_pct, 0) > %(max_pnl)s
       OR COALESCE(q.pnl_pct, 0) < %(min_pnl)s
      ))
)
SELECT
    b.*,
    qobs.qobs_count,
    qobs.exit_signal_count,
    qobs.no_route_count,
    qobs.max_qobs_mult,
    qobs.observations
FROM base b
LEFT JOIN LATERAL (
    SELECT
        -- Only PRICED observations count as coverage. qobs_count is used solely as a
        -- `> 0` gate (replay --require-qobs, lane_scan join, forward_referee's
        -- referee-grade test), and counting 429/no-route rows made a row with zero
        -- usable quotes read as covered — which is how the 2026-09-05 starved window
        -- passed the referee. (qsim_positions.obs_count is a different question —
        -- 'did we look' — so it does count no-route.)
        COUNT(*) FILTER (WHERE qo.real_mult IS NOT NULL) AS qobs_count,
        COUNT(*) FILTER (WHERE qo.should_exit) AS exit_signal_count,
        COUNT(*) FILTER (WHERE qo.no_route) AS no_route_count,
        MAX(qo.real_mult) AS max_qobs_mult,
        COALESCE(
            jsonb_agg(
                jsonb_build_object(
                    'observed_at', qo.observed_at,
                    'real_mult', qo.real_mult,
                    'exit_reason', qo.exit_reason,
                    'should_exit', qo.should_exit,
                    'no_route', qo.no_route,
                    'rate_limited', qo.rate_limited
                )
                ORDER BY qo.observed_at
            ) FILTER (WHERE qo.id IS NOT NULL),
            '[]'::jsonb
        ) AS observations
    FROM qsim_quote_observations qo
    WHERE qo.call_id = b.call_id
      AND qo.observed_at BETWEEN b.entry_time AND
          CASE
              WHEN %(include_post_exit)s AND b.exit_time IS NOT NULL
                  THEN b.exit_time + (%(post_exit_mins)s || ' minutes')::interval
              ELSE COALESCE(b.exit_time, now())
          END
) qobs ON TRUE
ORDER BY b.entry_time DESC
"""


@dataclass
class ReplayRow:
    row: dict[str, Any]
    qsim_return: float
    shadow_return: float | None
    max_quote_mult: float | None
    returns: dict[str, float]
    first_hits: dict[str, float | None]


def _rows(params: dict[str, Any]) -> list[dict[str, Any]]:
    from psycopg2.extras import RealDictCursor
    import db

    db.ensure_qsim_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(SQL, params)
        return [dict(row) for row in cur.fetchall()]


def _f(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _ratio(num: Any, den: Any) -> float | None:
    den_f = _f(den)
    if den_f <= 0:
        return None
    return _f(num) / den_f


def _json_default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    return str(value)


def _observations(raw: Any) -> list[dict[str, Any]]:
    if raw is None:
        return []
    if isinstance(raw, str):
        try:
            loaded = json.loads(raw)
        except json.JSONDecodeError:
            return []
        return loaded if isinstance(loaded, list) else []
    if isinstance(raw, list):
        return raw
    try:
        loaded = json.loads(json.dumps(raw, default=_json_default))
    except (TypeError, ValueError):
        return []
    return loaded if isinstance(loaded, list) else []


def _quote_mults(row: dict[str, Any]) -> list[float]:
    mults: list[float] = []
    for obs in _observations(row.get("observations")):
        mult = _f(obs.get("real_mult"))
        if 0 < mult <= MAX_QOBS_MULT:
            mults.append(mult)
    return mults


def _parse_dt(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _quote_points(row: dict[str, Any]) -> list[tuple[datetime | None, float]]:
    points: list[tuple[datetime | None, float]] = []
    for obs in _observations(row.get("observations")):
        mult = _f(obs.get("real_mult"))
        if 0 < mult <= MAX_QOBS_MULT:
            points.append((_parse_dt(obs.get("observed_at")), mult))
    return points


def _parse_where(where_values: list[str] | None) -> list[tuple[str, str, float]]:
    clauses = []
    for where in where_values or []:
        match = re.fullmatch(
            r"\s*([A-Za-z0-9_/-]+)\s*(<=|>=|<|>)\s*(-?\d+(?:\.\d+)?(?:e[+-]?\d+)?)\s*",
            where,
            flags=re.IGNORECASE,
        )
        if not match:
            raise SystemExit(
                "Invalid --where. Use a known numeric feature like --where vol_mcap>=1.331"
            )
        raw_feature, op, raw_threshold = match.groups()
        feature_key = raw_feature.replace("/", "_")
        feature = FILTER_ALIASES.get(feature_key)
        if feature is None:
            known = ", ".join(sorted(FILTER_ALIASES))
            raise SystemExit(f"Unknown --where feature '{raw_feature}'. Known: {known}")
        clauses.append((feature, op, float(raw_threshold)))
    return clauses


def _passes_where(row: dict[str, Any], clauses: list[tuple[str, str, float]]) -> bool:
    for feature, op, threshold in clauses:
        value = row.get(feature)
        if value is None:
            return False
        value_f = _f(value)
        if op == ">=" and value_f < threshold:
            return False
        if op == ">" and value_f <= threshold:
            return False
        if op == "<=" and value_f > threshold:
            return False
        if op == "<" and value_f >= threshold:
            return False
    return True


def _first_cross(mults: list[float], threshold: float) -> float | None:
    for mult in mults:
        if mult >= threshold:
            return mult
    return None


def _confirmed_cross(mults: list[float], threshold: float) -> float | None:
    prev_hit = False
    for mult in mults:
        hit = mult >= threshold
        if hit and prev_hit:
            return mult
        prev_hit = hit
    return None


def _bank_return(mults: list[float], level: float, current_return: float) -> float:
    """Exit at the first observed quote >= level; fallback to current qsim result."""
    first = _first_cross(mults, level)
    return first - 1.0 if first is not None else current_return


def _confirm_bank_return(mults: list[float], level: float, current_return: float) -> float:
    """Exit at the second consecutive observed quote >= level."""
    confirmed = _confirmed_cross(mults, level)
    return confirmed - 1.0 if confirmed is not None else current_return


def _partial_bank_return(
    mults: list[float],
    level: float,
    fraction: float,
    current_return: float,
) -> float:
    """
    Sell fraction at first quote >= level; remainder follows current qsim result.

    This keeps tail upside from existing winners while reducing the round-trip
    damage on trades that briefly touch 1.2x-1.7x before collapsing.
    """
    first = _first_cross(mults, level)
    if first is None:
        return current_return
    banked_return = first - 1.0
    return fraction * banked_return + (1.0 - fraction) * current_return


def _confirm_partial_bank_return(
    mults: list[float],
    level: float,
    fraction: float,
    current_return: float,
) -> float:
    confirmed = _confirmed_cross(mults, level)
    if confirmed is None:
        return current_return
    banked_return = confirmed - 1.0
    return fraction * banked_return + (1.0 - fraction) * current_return


def _partial_bank_with_stop_return(
    mults: list[float],
    level: float,
    fraction: float,
    stop: float,
    current_return: float,
) -> float:
    """
    Sell fraction at first quote >= level; remainder exits if it later falls to stop.

    If the remainder stop never fires, the remainder follows the original qsim
    result. This is the practical candidate live shape: take money when the
    quote proves strength, then stop the remaining bag from becoming a disaster.
    """
    banked_mult = None
    armed = False
    for mult in mults:
        if not armed and mult >= level:
            banked_mult = mult
            armed = True
            continue
        if armed and mult <= stop:
            banked_return = (banked_mult or level) - 1.0
            stop_return = mult - 1.0
            return fraction * banked_return + (1.0 - fraction) * stop_return

    if banked_mult is None:
        return current_return
    banked_return = banked_mult - 1.0
    return fraction * banked_return + (1.0 - fraction) * current_return


def _no_bounce_stop_return(
    mults: list[float],
    bounce: float,
    stop: float,
    current_return: float,
) -> float:
    """
    Exit if quote falls to stop before the trade ever proves a bounce.

    Once the quote reaches bounce, this defensive rule stands down and the row
    falls back to the original qsim result. This targets the majority bucket:
    trades that never reach even 1.1x-1.3x and then bleed into ugly exits.
    """
    for mult in mults:
        if mult >= bounce:
            return current_return
        if mult <= stop:
            return mult - 1.0
    return current_return


def _lock_floor_return(
    mults: list[float],
    trigger: float,
    floor: float,
    current_return: float,
) -> float:
    """
    Arm after quote peak reaches trigger, then exit if quote falls to floor.

    This models "bank the move before it round-trips" without pretending the bot
    could sell at the peak. If the floor never fires, fallback to current qsim.
    """
    armed = False
    for mult in mults:
        if not armed and mult >= trigger:
            armed = True
            continue
        if armed and mult <= floor:
            return mult - 1.0
    return current_return


def _lock_floor_or_bank_return(
    mults: list[float],
    trigger: float,
    floor: float,
    current_return: float,
) -> float:
    """
    Arm at trigger; if floor never fires, bank at the final observed quote.

    This is an optimistic-but-plausible "don't let an armed trade become a full
    loser" replay. It is less conservative than lock_floor_*.
    """
    armed = False
    last_mult = None
    for mult in mults:
        last_mult = mult
        if not armed and mult >= trigger:
            armed = True
            continue
        if armed and mult <= floor:
            return mult - 1.0
    if armed and last_mult is not None:
        return last_mult - 1.0
    return current_return


def _dynamic_exit_return(
    mults: list[float],
    *,
    arm: float,
    floor: float,
    stall_ticks: int,
    confirm_ticks: int,
    bounce: float,
    dead_stop: float,
    current_return: float,
) -> float:
    """
    Path-aware quote exit:
      * before a bounce proves itself, cut dead trades at dead_stop
      * after arm is confirmed, exit on round-trip floor or stalled quote highs

    This intentionally sells at the observed quote on the triggering tick, not
    the theoretical floor/arm level.
    """
    if not mults:
        return current_return

    bounced = False
    armed = False
    arm_streak = 0
    armed_peak = 0.0
    ticks_since_high = 0

    for mult in mults:
        if mult >= bounce:
            bounced = True
        elif not bounced and mult <= dead_stop:
            return mult - 1.0

        if not armed:
            if mult >= arm:
                arm_streak += 1
                if arm_streak >= confirm_ticks:
                    armed = True
                    armed_peak = mult
                    ticks_since_high = 0
            else:
                arm_streak = 0
            continue

        if mult > armed_peak:
            armed_peak = mult
            ticks_since_high = 0
            continue

        ticks_since_high += 1
        if mult <= floor:
            return mult - 1.0
        if ticks_since_high >= stall_ticks:
            return mult - 1.0

    return current_return


def _winner_only_return(
    mults: list[float],
    *,
    arm: float,
    mode: str,
    confirm_ticks: int,
    current_return: float,
    floor: float | None = None,
    stall_ticks: int | None = None,
) -> float:
    """
    Winner-only quote management.

    Unlike _dynamic_exit_return, this never cuts losers early. It only changes
    the outcome after the quote path proves strength at the arm level.
    """
    armed = False
    arm_streak = 0
    armed_peak = 0.0
    ticks_since_high = 0

    for mult in mults:
        if not armed:
            if mult >= arm:
                arm_streak += 1
                if arm_streak >= confirm_ticks:
                    if mode == "bank":
                        return mult - 1.0
                    armed = True
                    armed_peak = mult
                    ticks_since_high = 0
            else:
                arm_streak = 0
            continue

        if mult > armed_peak:
            armed_peak = mult
            ticks_since_high = 0
            continue

        ticks_since_high += 1
        if mode == "floor" and floor is not None and mult <= floor:
            return mult - 1.0
        if mode == "stall" and stall_ticks is not None and ticks_since_high >= stall_ticks:
            return mult - 1.0

    return current_return


def _no_bounce_hit_mult(mults: list[float], bounce: float, stop: float) -> float | None:
    for mult in mults:
        if mult >= bounce:
            return None
        if mult <= stop:
            return mult
    return None


def _lock_levels_from_policy(policy: str) -> tuple[float, float]:
    suffix = (
        policy.removeprefix("lock_or_bank_")
        if policy.startswith("lock_or_bank_")
        else policy.removeprefix("lock_")
    )
    trigger_token, floor_token = suffix.split("_", 1)
    return (
        float(trigger_token.removesuffix("x").replace("p", ".")),
        float(floor_token.removesuffix("x").replace("p", ".")),
    )


def _lock_hit_mult(mults: list[float], trigger: float, floor: float, *, or_bank: bool) -> float | None:
    armed = False
    last_mult = None
    for mult in mults:
        last_mult = mult
        if not armed and mult >= trigger:
            armed = True
            continue
        if armed and mult <= floor:
            return mult
    if or_bank and armed and last_mult is not None:
        return last_mult
    return None


def _dynamic_hit_mult(
    mults: list[float],
    *,
    arm: float,
    floor: float,
    stall_ticks: int,
    confirm_ticks: int,
    bounce: float,
    dead_stop: float,
) -> float | None:
    bounced = False
    armed = False
    arm_streak = 0
    armed_peak = 0.0
    ticks_since_high = 0

    for mult in mults:
        if mult >= bounce:
            bounced = True
        elif not bounced and mult <= dead_stop:
            return mult

        if not armed:
            if mult >= arm:
                arm_streak += 1
                if arm_streak >= confirm_ticks:
                    armed = True
                    armed_peak = mult
                    ticks_since_high = 0
            else:
                arm_streak = 0
            continue

        if mult > armed_peak:
            armed_peak = mult
            ticks_since_high = 0
            continue

        ticks_since_high += 1
        if mult <= floor:
            return mult
        if ticks_since_high >= stall_ticks:
            return mult
    return None


def _winner_only_hit_mult(
    mults: list[float],
    *,
    arm: float,
    mode: str,
    confirm_ticks: int,
    floor: float | None = None,
    stall_ticks: int | None = None,
) -> float | None:
    armed = False
    arm_streak = 0
    armed_peak = 0.0
    ticks_since_high = 0

    for mult in mults:
        if not armed:
            if mult >= arm:
                arm_streak += 1
                if arm_streak >= confirm_ticks:
                    if mode == "bank":
                        return mult
                    armed = True
                    armed_peak = mult
                    ticks_since_high = 0
            else:
                arm_streak = 0
            continue

        if mult > armed_peak:
            armed_peak = mult
            ticks_since_high = 0
            continue

        ticks_since_high += 1
        if mode == "floor" and floor is not None and mult <= floor:
            return mult
        if mode == "stall" and stall_ticks is not None and ticks_since_high >= stall_ticks:
            return mult
    return None


def _config_for_variant(variant: str | None):
    if variant in {"ride", "ride_vol"}:
        return EXIT_RIDE
    return EXIT_A_PAPER


def _raw_config_exit(row: dict[str, Any], mults: list[float]) -> tuple[float, str]:
    """
    Replay the real exit_config over raw Jupiter quote multiples.

    This intentionally bypasses peak_guard/trough_guard and treats a Jupiter
    sell quote as the executable current price. Time-stop is not simulated here
    because qobs contains quote samples, not a complete wall-clock runner path
    for older partially instrumented positions.
    """
    if not mults:
        return (_ratio(row.get("qsim_pnl"), row.get("qsim_sol_in")) or 0.0, "current")

    cfg = _config_for_variant(row.get("variant"))
    channel_handle = (row.get("channel") or "").lstrip("@")
    is_vip_gamble = row.get("vip_tier") in {"gamble", "gamble_risk"}
    peak_mult = 0.0
    for mult in mults:
        peak_mult = max(peak_mult, mult)
        result = apply_exit_config(
            cfg,
            current_mcap=mult,
            peak_mcap=peak_mult,
            entry_mcap=1.0,
            is_vip_gamble=is_vip_gamble,
            channel_handle=channel_handle,
            entry_time=None,
        )
        if result.should_exit:
            return mult - 1.0, result.reason or "raw_config"

    return (_ratio(row.get("qsim_pnl"), row.get("qsim_sol_in")) or 0.0, "held_to_current")


def _fallback_return(
    row: dict[str, Any],
    mults: list[float],
    qsim_return: float,
    raw_config_return: float,
    mode: str,
    hard_stop_pct: float = 0.35,
) -> float:
    if mode == "current":
        return qsim_return
    if mode == "raw_config":
        return raw_config_return
    if mode == "last_quote":
        return mults[-1] - 1.0 if mults else qsim_return
    if mode == "hard_stop":
        return -abs(hard_stop_pct)
    if mode == "hard_stop_35":
        return -0.35
    raise ValueError(f"unknown fallback mode: {mode}")


def _runner_window_return(
    row: dict[str, Any],
    points: list[tuple[datetime | None, float]],
    *,
    arm: float,
    window_mins: float,
    floor: float,
    release: float,
    current_return: float,
) -> float:
    if not points:
        return current_return

    cfg = _config_for_variant(row.get("variant"))
    channel_handle = (row.get("channel") or "").lstrip("@")
    is_vip_gamble = row.get("vip_tier") in {"gamble", "gamble_risk"}
    protected_reasons = {"trail_stop", "profit_floor"}
    peak_mult = 0.0
    runner_until: float | None = None
    last_mult = points[-1][1]

    for observed_at, mult in points:
        last_mult = mult
        peak_mult = max(peak_mult, mult)
        if runner_until is None and arm <= peak_mult < release and observed_at is not None:
            runner_until = observed_at.timestamp() + window_mins * 60.0

        result = apply_exit_config(
            cfg,
            current_mcap=mult,
            peak_mcap=peak_mult,
            entry_mcap=1.0,
            is_vip_gamble=is_vip_gamble,
            channel_handle=channel_handle,
            entry_time=None,
        )
        if runner_until is None:
            if result.should_exit:
                return mult - 1.0
            continue

        if mult <= floor:
            return mult - 1.0
        if peak_mult >= release:
            if result.should_exit:
                return mult - 1.0
            continue
        if observed_at is not None and observed_at.timestamp() >= runner_until:
            if result.should_exit:
                return mult - 1.0
            continue
        if result.should_exit and result.reason in protected_reasons:
            continue
        if result.should_exit:
            return mult - 1.0

    return last_mult - 1.0


def _bank_or_run_return(
    points: list[tuple[datetime | None, float]],
    *,
    bank: float,
    runner_arm: float,
    target: float,
    floor: float,
    window_mins: float,
    stall_ticks: int,
    current_return: float,
) -> float:
    """
    Bank ordinary winners, but hold fast strength briefly for a bigger target.

    The policy stays deliberately executable:
      * first quote >= bank is the safe bank point
      * only if that same quote already reaches runner_arm does it enter runner mode
      * runner mode exits on target, floor, stall, or window expiry

    This avoids pretending the bot can pause at 1.4x, watch for a later 2x, and
    still magically sell the old 1.4x if momentum vanishes.
    """
    if not points:
        return current_return

    runner_started_at: float | None = None
    runner_peak = 0.0
    ticks_since_high = 0
    last_mult = points[-1][1]

    for observed_at, mult in points:
        last_mult = mult

        if runner_started_at is None:
            if mult < bank:
                continue
            if mult < runner_arm:
                return mult - 1.0
            runner_started_at = (
                observed_at.timestamp() if observed_at is not None else None
            )
            runner_peak = mult
            ticks_since_high = 0
            if mult >= target:
                return mult - 1.0
            continue

        if mult >= target:
            return mult - 1.0
        if mult <= floor:
            return mult - 1.0

        if mult > runner_peak:
            runner_peak = mult
            ticks_since_high = 0
        else:
            ticks_since_high += 1
            if ticks_since_high >= stall_ticks:
                return mult - 1.0

        if (
            runner_started_at is not None
            and observed_at is not None
            and observed_at.timestamp() >= runner_started_at + window_mins * 60.0
        ):
            return mult - 1.0

    if runner_started_at is not None:
        return last_mult - 1.0
    return current_return


def _bank_or_run_hit_mult(
    points: list[tuple[datetime | None, float]],
    *,
    bank: float,
    runner_arm: float,
    target: float,
    floor: float,
    window_mins: float,
    stall_ticks: int,
) -> float | None:
    if not points:
        return None

    runner_started_at: float | None = None
    runner_peak = 0.0
    ticks_since_high = 0
    last_mult = points[-1][1]

    for observed_at, mult in points:
        last_mult = mult

        if runner_started_at is None:
            if mult < bank:
                continue
            if mult < runner_arm:
                return mult
            runner_started_at = (
                observed_at.timestamp() if observed_at is not None else None
            )
            runner_peak = mult
            ticks_since_high = 0
            if mult >= target:
                return mult
            continue

        if mult >= target:
            return mult
        if mult <= floor:
            return mult

        if mult > runner_peak:
            runner_peak = mult
            ticks_since_high = 0
        else:
            ticks_since_high += 1
            if ticks_since_high >= stall_ticks:
                return mult

        if (
            runner_started_at is not None
            and observed_at is not None
            and observed_at.timestamp() >= runner_started_at + window_mins * 60.0
        ):
            return mult

    return last_mult if runner_started_at is not None else None


def _soft_stop_recovery_return(
    points: list[tuple[datetime | None, float]],
    *,
    hard_stop: float,
    life: float,
    window_mins: float,
    disaster_floor: float,
    recover: float,
    current_return: float,
) -> float:
    """
    Delay only selected hard stops that showed enough prior life to deserve grace.

    If a trade falls through hard_stop without ever printing life, this exits at
    that observed hard-stop quote. If it did print life first, it gets a short
    recovery window. The recovery window exits on:
      * recover target hit
      * disaster floor breach
      * timeout at the latest observed quote

    This models "temporary flush rescue" without forgiving true dead rugs.
    """
    if not points:
        return current_return

    saw_life = False
    in_recovery = False
    recovery_started_at: float | None = None
    last_mult = points[-1][1]

    for observed_at, mult in points:
        last_mult = mult
        if mult >= life:
            saw_life = True

        if not in_recovery:
            if mult <= hard_stop:
                if not saw_life:
                    return mult - 1.0
                in_recovery = True
                recovery_started_at = (
                    observed_at.timestamp() if observed_at is not None else None
                )
            continue

        if mult >= recover:
            return mult - 1.0
        if mult <= disaster_floor:
            return mult - 1.0
        if (
            recovery_started_at is not None
            and observed_at is not None
            and observed_at.timestamp() >= recovery_started_at + window_mins * 60.0
        ):
            return mult - 1.0

    return last_mult - 1.0 if in_recovery else current_return


def _soft_stop_hit_mult(
    points: list[tuple[datetime | None, float]],
    *,
    hard_stop: float,
    life: float,
    window_mins: float,
    disaster_floor: float,
    recover: float,
) -> float | None:
    if not points:
        return None

    saw_life = False
    in_recovery = False
    recovery_started_at: float | None = None
    last_mult = points[-1][1]

    for observed_at, mult in points:
        last_mult = mult
        if mult >= life:
            saw_life = True

        if not in_recovery:
            if mult <= hard_stop:
                if not saw_life:
                    return mult
                in_recovery = True
                recovery_started_at = (
                    observed_at.timestamp() if observed_at is not None else None
                )
            continue

        if mult >= recover:
            return mult
        if mult <= disaster_floor:
            return mult
        if (
            recovery_started_at is not None
            and observed_at is not None
            and observed_at.timestamp() >= recovery_started_at + window_mins * 60.0
        ):
            return mult

    return last_mult if in_recovery else None


def _bank_soft_stop_return(
    points: list[tuple[datetime | None, float]],
    *,
    bank: float,
    hard_stop: float,
    life: float,
    window_mins: float,
    disaster_floor: float,
    recover: float,
    current_return: float,
) -> float:
    """
    Combined bank + soft hard-stop replay.

    This is the practical policy shape:
      * if bank is reached first, sell immediately
      * if hard stop is reached first with no prior life, sell immediately
      * if hard stop is reached after prior life, allow a short recovery window

    The recovery window exits at recover, disaster_floor, or timeout. It does
    not rewrite winners and it does not forgive rugs that never showed strength.
    """
    if not points:
        return current_return

    saw_life = False
    in_recovery = False
    recovery_started_at: float | None = None
    last_mult = points[-1][1]

    for observed_at, mult in points:
        last_mult = mult

        if not in_recovery and mult >= bank:
            return mult - 1.0

        if mult >= life:
            saw_life = True

        if not in_recovery:
            if mult <= hard_stop:
                if not saw_life:
                    return mult - 1.0
                in_recovery = True
                recovery_started_at = (
                    observed_at.timestamp() if observed_at is not None else None
                )
            continue

        if mult >= recover:
            return mult - 1.0
        if mult <= disaster_floor:
            return mult - 1.0
        if (
            recovery_started_at is not None
            and observed_at is not None
            and observed_at.timestamp() >= recovery_started_at + window_mins * 60.0
        ):
            return mult - 1.0

    return last_mult - 1.0 if in_recovery else current_return


def _bank_soft_stop_hit_mult(
    points: list[tuple[datetime | None, float]],
    *,
    bank: float,
    hard_stop: float,
    life: float,
    window_mins: float,
    disaster_floor: float,
    recover: float,
) -> float | None:
    if not points:
        return None

    saw_life = False
    in_recovery = False
    recovery_started_at: float | None = None
    last_mult = points[-1][1]

    for observed_at, mult in points:
        last_mult = mult

        if not in_recovery and mult >= bank:
            return mult

        if mult >= life:
            saw_life = True

        if not in_recovery:
            if mult <= hard_stop:
                if not saw_life:
                    return mult
                in_recovery = True
                recovery_started_at = (
                    observed_at.timestamp() if observed_at is not None else None
                )
            continue

        if mult >= recover:
            return mult
        if mult <= disaster_floor:
            return mult
        if (
            recovery_started_at is not None
            and observed_at is not None
            and observed_at.timestamp() >= recovery_started_at + window_mins * 60.0
        ):
            return mult

    return last_mult if in_recovery else None


def _conditional_stop_delay_return(
    row: dict[str, Any],
    points: list[tuple[datetime | None, float]],
    *,
    bank: float,
    hard_stop: float,
    delay_min: float,
    delay_max: float,
    life: float,
    max_age_mins: float,
    window_mins: float,
    disaster_floor: float,
    recover: float,
    current_return: float,
) -> float:
    """
    Bank normal winners; delay only fast hard stops that look recoverable.

    A delayed stop is allowed only when the hard-stop print is not a near-zero
    nuke, happened quickly after entry, and the trade had already shown enough
    quote life. Otherwise the replay exits immediately at the hard-stop quote.
    """
    if not points:
        return current_return

    entry_time = _parse_dt(row.get("entry_time"))
    peak = 0.0
    in_delay = False
    delay_started_at: float | None = None
    last_mult = points[-1][1]

    for observed_at, mult in points:
        last_mult = mult

        if not in_delay and mult >= bank:
            return mult - 1.0

        peak = max(peak, mult)

        if not in_delay:
            if mult > hard_stop:
                continue

            age_mins = None
            if entry_time is not None and observed_at is not None:
                age_mins = (observed_at - entry_time).total_seconds() / 60.0
            can_delay = (
                delay_min <= mult <= delay_max
                and peak >= life
                and (age_mins is None or age_mins <= max_age_mins)
            )
            if not can_delay:
                return mult - 1.0

            in_delay = True
            delay_started_at = observed_at.timestamp() if observed_at is not None else None
            continue

        if mult >= recover:
            return mult - 1.0
        if mult <= disaster_floor:
            return mult - 1.0
        if (
            delay_started_at is not None
            and observed_at is not None
            and observed_at.timestamp() >= delay_started_at + window_mins * 60.0
        ):
            return mult - 1.0

    return last_mult - 1.0 if in_delay else current_return


def _conditional_stop_delay_hit_mult(
    row: dict[str, Any],
    points: list[tuple[datetime | None, float]],
    *,
    bank: float,
    hard_stop: float,
    delay_min: float,
    delay_max: float,
    life: float,
    max_age_mins: float,
    window_mins: float,
    disaster_floor: float,
    recover: float,
) -> float | None:
    if not points:
        return None

    entry_time = _parse_dt(row.get("entry_time"))
    peak = 0.0
    in_delay = False
    delay_started_at: float | None = None
    last_mult = points[-1][1]

    for observed_at, mult in points:
        last_mult = mult

        if not in_delay and mult >= bank:
            return mult

        peak = max(peak, mult)

        if not in_delay:
            if mult > hard_stop:
                continue

            age_mins = None
            if entry_time is not None and observed_at is not None:
                age_mins = (observed_at - entry_time).total_seconds() / 60.0
            can_delay = (
                delay_min <= mult <= delay_max
                and peak >= life
                and (age_mins is None or age_mins <= max_age_mins)
            )
            if not can_delay:
                return None

            in_delay = True
            delay_started_at = observed_at.timestamp() if observed_at is not None else None
            continue

        if mult >= recover:
            return mult
        if mult <= disaster_floor:
            return mult
        if (
            delay_started_at is not None
            and observed_at is not None
            and observed_at.timestamp() >= delay_started_at + window_mins * 60.0
        ):
            return mult

    return last_mult if in_delay else None


def _view(
    row: dict[str, Any],
    fallback_mode: str = "current",
    fallback_hard_stop_pct: float = 0.35,
) -> ReplayRow:
    qsim_return = _ratio(row.get("qsim_pnl"), row.get("qsim_sol_in")) or 0.0
    shadow_return = _ratio(row.get("shadow_pnl"), row.get("shadow_sol_in"))
    mults = _quote_mults(row)
    points = _quote_points(row)
    max_quote_mult = max(mults) if mults else None
    returns = {"current": qsim_return}
    first_hits: dict[str, float | None] = {}

    if max_quote_mult is not None:
        returns["best_raw"] = max_quote_mult - 1.0
    else:
        returns["best_raw"] = qsim_return

    raw_config_return, raw_config_reason = _raw_config_exit(row, mults)
    returns["raw_config"] = raw_config_return
    row["raw_config_reason"] = raw_config_reason
    fallback_return = _fallback_return(
        row, mults, qsim_return, raw_config_return, fallback_mode, fallback_hard_stop_pct
    )
    returns[f"fallback_{fallback_mode}"] = fallback_return

    for level in BANK_LEVELS:
        suffix = _level_suffix(level)
        returns[f"bank_{suffix}"] = _bank_return(mults, level, fallback_return)
        returns[f"confirm_bank_{suffix}"] = _confirm_bank_return(mults, level, fallback_return)
        for fraction in BANK_FRACTIONS:
            frac_suffix = _fraction_suffix(fraction)
            returns[f"{frac_suffix}_bank_{suffix}"] = _partial_bank_return(
                mults, level, fraction, fallback_return
            )
            returns[f"confirm_{frac_suffix}_bank_{suffix}"] = _confirm_partial_bank_return(
                mults, level, fraction, fallback_return
            )
            for stop in BANK_REMAINDER_STOPS:
                stop_suffix = _level_suffix(stop)
                returns[f"{frac_suffix}_bank_{suffix}_stop_{stop_suffix}"] = (
                    _partial_bank_with_stop_return(
                        mults, level, fraction, stop, fallback_return
                    )
                )

    for trigger, floor in LOCK_FLOORS:
        suffix = f"{_level_suffix(trigger)}_{_level_suffix(floor)}"
        returns[f"lock_{suffix}"] = _lock_floor_return(mults, trigger, floor, fallback_return)
        returns[f"lock_or_bank_{suffix}"] = _lock_floor_or_bank_return(
            mults, trigger, floor, fallback_return
        )

    for policy in DYNAMIC_EXIT_POLICIES:
        returns[policy["name"]] = _dynamic_exit_return(
            mults,
            arm=policy["arm"],
            floor=policy["floor"],
            stall_ticks=policy["stall_ticks"],
            confirm_ticks=policy["confirm_ticks"],
            bounce=policy["bounce"],
            dead_stop=policy["dead_stop"],
            current_return=fallback_return,
        )

    for policy in WINNER_ONLY_POLICIES:
        returns[policy["name"]] = _winner_only_return(
            mults,
            arm=policy["arm"],
            mode=policy["mode"],
            confirm_ticks=policy["confirm_ticks"],
            current_return=fallback_return,
            floor=policy.get("floor"),
            stall_ticks=policy.get("stall_ticks"),
        )

    for policy in RUNNER_WINDOW_POLICIES:
        returns[policy["name"]] = _runner_window_return(
            row,
            points,
            arm=policy["arm"],
            window_mins=policy["window_mins"],
            floor=policy["floor"],
            release=policy["release"],
            current_return=fallback_return,
        )

    for policy in BANK_OR_RUN_POLICIES:
        returns[policy["name"]] = _bank_or_run_return(
            points,
            bank=policy["bank"],
            runner_arm=policy["runner_arm"],
            target=policy["target"],
            floor=policy["floor"],
            window_mins=policy["window_mins"],
            stall_ticks=policy["stall_ticks"],
            current_return=fallback_return,
        )

    for policy in SOFT_STOP_POLICIES:
        returns[policy["name"]] = _soft_stop_recovery_return(
            points,
            hard_stop=policy["hard_stop"],
            life=policy["life"],
            window_mins=policy["window_mins"],
            disaster_floor=policy["disaster_floor"],
            recover=policy["recover"],
            current_return=fallback_return,
        )

    for policy in BANK_SOFT_STOP_POLICIES:
        returns[policy["name"]] = _bank_soft_stop_return(
            points,
            bank=policy["bank"],
            hard_stop=policy["hard_stop"],
            life=policy["life"],
            window_mins=policy["window_mins"],
            disaster_floor=policy["disaster_floor"],
            recover=policy["recover"],
            current_return=fallback_return,
        )

    for policy in CONDITIONAL_STOP_DELAY_POLICIES:
        returns[policy["name"]] = _conditional_stop_delay_return(
            row,
            points,
            bank=policy["bank"],
            hard_stop=policy["hard_stop"],
            delay_min=policy["delay_min"],
            delay_max=policy["delay_max"],
            life=policy["life"],
            max_age_mins=policy["max_age_mins"],
            window_mins=policy["window_mins"],
            disaster_floor=policy["disaster_floor"],
            recover=policy["recover"],
            current_return=fallback_return,
        )

    for bounce in NO_BOUNCE_THRESHOLDS:
        bounce_suffix = _level_suffix(bounce)
        for stop in NO_BOUNCE_STOPS:
            stop_suffix = _level_suffix(stop)
            returns[f"no_{bounce_suffix}_stop_{stop_suffix}"] = _no_bounce_stop_return(
                mults, bounce, stop, fallback_return
            )

    for threshold in THRESHOLDS:
        suffix = f"{int(threshold)}x"
        first = _first_cross(mults, threshold)
        confirmed = _confirmed_cross(mults, threshold)
        first_hits[suffix] = first
        returns[f"floor_{suffix}"] = threshold - 1.0 if first is not None else fallback_return
        returns[f"obs_{suffix}"] = first - 1.0 if first is not None else fallback_return
        returns[f"confirm_{suffix}"] = confirmed - 1.0 if confirmed is not None else fallback_return

    return ReplayRow(
        row=row,
        qsim_return=qsim_return,
        shadow_return=shadow_return,
        max_quote_mult=max_quote_mult,
        returns=returns,
        first_hits=first_hits,
    )


def _pct(value: float | None) -> str:
    if value is None:
        return "   n/a "
    return f"{value * 100:+7.1f}%"


def _mult(value: float | None) -> str:
    if value is None:
        return "  n/a"
    return f"{value:>5.2f}"


def _level_suffix(level: float) -> str:
    return f"{level:g}x".replace(".", "p")


def _fraction_suffix(fraction: float) -> str:
    return f"p{int(round(fraction * 100))}"


def _entry_ratio(view: ReplayRow) -> float | None:
    return _ratio(view.row.get("qsim_entry"), view.row.get("shadow_entry"))


def _print_summary(views: list[ReplayRow]) -> None:
    if not views:
        print("No closed qsim rows found for that filter.\n")
        return

    n = len(views)
    with_qobs = [view for view in views if int(view.row.get("qobs_count") or 0) > 0]
    with_shadow = [view for view in views if view.shadow_return is not None]
    current = sum(view.returns["current"] for view in views)
    current_wins = sum(1 for view in views if view.returns["current"] > 0)

    print("\nQSIM QUOTE CAPTURE REPLAY")
    print("PnL is normalized per 1 SOL deployed; replay exits use raw Jupiter quote observations only.\n")
    print(f"closed qsim rows:   {n}")
    print(f"rows with qobs:     {len(with_qobs)}/{n}")
    print(f"rows with shadow:   {len(with_shadow)}/{n}")
    print(f"current qsim sum:   {current:+.2f} win%={current_wins / n:>5.1%} avg={current / n:+.3f}")
    print("strategy win columns: all_win% = wins over all rows; hit% = rows where the strategy triggered")
    if with_shadow:
        shadow_sum = sum(view.shadow_return or 0.0 for view in with_shadow)
        print(f"shadow compare sum: {shadow_sum:+.2f} ({len(with_shadow)} matched)")

    print("\nReplay Totals")
    print(f"{'policy':<14} {'sum':>10} {'delta':>10} {'all_win%':>9} {'avg':>8} {'hit%':>7} {'hits':>7} {'avg_hit':>9}")
    print("-" * 83)
    policies = [
        "best_raw", "raw_config",
        "floor_2x", "obs_2x", "confirm_2x",
        "floor_3x", "obs_3x", "confirm_3x",
        "floor_5x", "obs_5x", "confirm_5x",
    ]
    for policy in policies:
        _print_policy_row(policy, views, current)

    print("\nEarly Bank Totals")
    print(f"{'policy':<18} {'sum':>10} {'delta':>10} {'all_win%':>9} {'avg':>8} {'hit%':>7} {'hits':>7} {'avg_hit':>9}")
    print("-" * 87)
    for level in BANK_LEVELS:
        suffix = _level_suffix(level)
        _print_policy_row(f"bank_{suffix}", views, current, width=18)
        _print_policy_row(f"confirm_bank_{suffix}", views, current, width=18)

    print("\nPartial Bank Totals")
    print(f"{'policy':<24} {'sum':>10} {'delta':>10} {'all_win%':>9} {'avg':>8} {'hit%':>7} {'hits':>7} {'avg_hit':>9}")
    print("-" * 93)
    for level in BANK_LEVELS:
        suffix = _level_suffix(level)
        for fraction in BANK_FRACTIONS:
            frac_suffix = _fraction_suffix(fraction)
            _print_policy_row(f"{frac_suffix}_bank_{suffix}", views, current, width=24)
            _print_policy_row(
                f"confirm_{frac_suffix}_bank_{suffix}", views, current, width=24
            )

    print("\nPartial Bank + Remainder Stop Totals")
    print(f"{'policy':<32} {'sum':>10} {'delta':>10} {'all_win%':>9} {'avg':>8} {'hit%':>7} {'hits':>7} {'avg_hit':>9}")
    print("-" * 101)
    for level in (1.30, 1.40, 1.50):
        suffix = _level_suffix(level)
        for fraction in BANK_FRACTIONS:
            frac_suffix = _fraction_suffix(fraction)
            for stop in BANK_REMAINDER_STOPS:
                stop_suffix = _level_suffix(stop)
                _print_policy_row(
                    f"{frac_suffix}_bank_{suffix}_stop_{stop_suffix}",
                    views,
                    current,
                    width=32,
                )

    print("\nNo-Bounce Stop Totals")
    print(f"{'policy':<24} {'sum':>10} {'delta':>10} {'all_win%':>9} {'avg':>8} {'hit%':>7} {'hits':>7} {'avg_hit':>9}")
    print("-" * 93)
    for bounce in NO_BOUNCE_THRESHOLDS:
        bounce_suffix = _level_suffix(bounce)
        for stop in NO_BOUNCE_STOPS:
            stop_suffix = _level_suffix(stop)
            _print_policy_row(
                f"no_{bounce_suffix}_stop_{stop_suffix}",
                views,
                current,
                width=24,
            )

    print("\nPeak Lock Totals")
    print(f"{'policy':<22} {'sum':>10} {'delta':>10} {'all_win%':>9} {'avg':>8} {'hit%':>7} {'hits':>7} {'avg_hit':>9}")
    print("-" * 91)
    for trigger, floor in LOCK_FLOORS:
        suffix = f"{_level_suffix(trigger)}_{_level_suffix(floor)}"
        _print_policy_row(f"lock_{suffix}", views, current, width=22)
        _print_policy_row(f"lock_or_bank_{suffix}", views, current, width=22)

    print("\nDynamic Exit Totals")
    print(f"{'policy':<32} {'sum':>10} {'delta':>10} {'all_win%':>9} {'avg':>8} {'hit%':>7} {'hits':>7} {'avg_hit':>9}")
    print("-" * 101)
    for policy in DYNAMIC_EXIT_POLICIES:
        _print_policy_row(policy["name"], views, current, width=32)

    print("\nBank-Or-Run Totals")
    print(f"{'policy':<32} {'sum':>10} {'delta':>10} {'all_win%':>9} {'avg':>8} {'hit%':>7} {'hits':>7} {'avg_hit':>9}")
    print("-" * 101)
    for policy in BANK_OR_RUN_POLICIES:
        _print_policy_row(policy["name"], views, current, width=32)

    print("\nSoft Stop Recovery Totals")
    print(f"{'policy':<36} {'sum':>10} {'delta':>10} {'all_win%':>9} {'avg':>8} {'hit%':>7} {'hits':>7} {'avg_hit':>9}")
    print("-" * 105)
    for policy in SOFT_STOP_POLICIES:
        _print_policy_row(policy["name"], views, current, width=36)

    print("\nBank + Soft Stop Totals")
    print(f"{'policy':<48} {'sum':>10} {'delta':>10} {'all_win%':>9} {'avg':>8} {'hit%':>7} {'hits':>7} {'avg_hit':>9}")
    print("-" * 117)
    for policy in BANK_SOFT_STOP_POLICIES:
        _print_policy_row(policy["name"], views, current, width=48)

    print("\nConditional Stop Delay Totals")
    print(f"{'policy':<58} {'sum':>10} {'delta':>10} {'all_win%':>9} {'avg':>8} {'hit%':>7} {'hits':>7} {'avg_hit':>9}")
    print("-" * 127)
    for policy in CONDITIONAL_STOP_DELAY_POLICIES:
        _print_policy_row(policy["name"], views, current, width=58)

    print("\nWinner-Only Exit Totals")
    print(f"{'policy':<22} {'sum':>10} {'delta':>10} {'all_win%':>9} {'avg':>8} {'hit%':>7} {'hits':>7} {'avg_hit':>9}")
    print("-" * 91)
    for policy in WINNER_ONLY_POLICIES:
        _print_policy_row(policy["name"], views, current, width=22)

    print("\nRunner Window Totals")
    print(f"{'policy':<28} {'sum':>10} {'delta':>10} {'all_win%':>9} {'avg':>8} {'hit%':>7} {'hits':>7} {'avg_hit':>9}")
    print("-" * 97)
    for policy in RUNNER_WINDOW_POLICIES:
        _print_policy_row(policy["name"], views, current, width=28)


def _policy_hit_mults(policy: str, views: list[ReplayRow]) -> list[float]:
    if policy == "best_raw":
        return [view.max_quote_mult for view in views if view.max_quote_mult is not None]
    if policy == "raw_config":
        return [
            view.returns[policy] + 1.0
            for view in views
            if view.row.get("raw_config_reason") not in {"current", "held_to_current"}
        ]
    if policy.startswith("confirm_"):
        level = _level_from_policy(policy)
        return [
            value
            for view in views
            if (value := _confirmed_cross(_quote_mults(view.row), level)) is not None
        ]
    if policy.startswith("lock_") or policy.startswith("lock_or_bank_"):
        trigger, floor = _lock_levels_from_policy(policy)
        or_bank = policy.startswith("lock_or_bank_")
        return [
            value
            for view in views
            if (value := _lock_hit_mult(_quote_mults(view.row), trigger, floor, or_bank=or_bank)) is not None
        ]
    if policy.startswith("dyn_"):
        spec = next(item for item in DYNAMIC_EXIT_POLICIES if item["name"] == policy)
        return [
            value
            for view in views
            if (
                value := _dynamic_hit_mult(
                    _quote_mults(view.row),
                    arm=spec["arm"],
                    floor=spec["floor"],
                    stall_ticks=spec["stall_ticks"],
                    confirm_ticks=spec["confirm_ticks"],
                    bounce=spec["bounce"],
                    dead_stop=spec["dead_stop"],
                )
            ) is not None
        ]
    if policy.startswith("bor_"):
        spec = next(item for item in BANK_OR_RUN_POLICIES if item["name"] == policy)
        return [
            value
            for view in views
            if (
                value := _bank_or_run_hit_mult(
                    _quote_points(view.row),
                    bank=spec["bank"],
                    runner_arm=spec["runner_arm"],
                    target=spec["target"],
                    floor=spec["floor"],
                    window_mins=spec["window_mins"],
                    stall_ticks=spec["stall_ticks"],
                )
            ) is not None
        ]
    if policy.startswith("soft_"):
        spec = next(item for item in SOFT_STOP_POLICIES if item["name"] == policy)
        return [
            value
            for view in views
            if (
                value := _soft_stop_hit_mult(
                    _quote_points(view.row),
                    hard_stop=spec["hard_stop"],
                    life=spec["life"],
                    window_mins=spec["window_mins"],
                    disaster_floor=spec["disaster_floor"],
                    recover=spec["recover"],
                )
            ) is not None
        ]
    if policy.startswith("bank") and "_soft_" in policy:
        spec = next(item for item in BANK_SOFT_STOP_POLICIES if item["name"] == policy)
        return [
            value
            for view in views
            if (
                value := _bank_soft_stop_hit_mult(
                    _quote_points(view.row),
                    bank=spec["bank"],
                    hard_stop=spec["hard_stop"],
                    life=spec["life"],
                    window_mins=spec["window_mins"],
                    disaster_floor=spec["disaster_floor"],
                    recover=spec["recover"],
                )
            ) is not None
        ]
    if policy.startswith("cdelay_"):
        spec = next(item for item in CONDITIONAL_STOP_DELAY_POLICIES if item["name"] == policy)
        return [
            value
            for view in views
            if (
                value := _conditional_stop_delay_hit_mult(
                    view.row,
                    _quote_points(view.row),
                    bank=spec["bank"],
                    hard_stop=spec["hard_stop"],
                    delay_min=spec["delay_min"],
                    delay_max=spec["delay_max"],
                    life=spec["life"],
                    max_age_mins=spec["max_age_mins"],
                    window_mins=spec["window_mins"],
                    disaster_floor=spec["disaster_floor"],
                    recover=spec["recover"],
                )
            ) is not None
        ]
    if policy.startswith("win_"):
        spec = next(item for item in WINNER_ONLY_POLICIES if item["name"] == policy)
        return [
            value
            for view in views
            if (
                value := _winner_only_hit_mult(
                    _quote_mults(view.row),
                    arm=spec["arm"],
                    mode=spec["mode"],
                    confirm_ticks=spec["confirm_ticks"],
                    floor=spec.get("floor"),
                    stall_ticks=spec.get("stall_ticks"),
                )
            ) is not None
        ]
    if policy.startswith("no_"):
        parts = policy.split("_")
        bounce = float(parts[1].removesuffix("x").replace("p", "."))
        stop = float(parts[3].removesuffix("x").replace("p", "."))
        return [
            value
            for view in views
            if (value := _no_bounce_hit_mult(_quote_mults(view.row), bounce, stop)) is not None
        ]
    if (
        policy.startswith("bank_")
        or policy.startswith("floor_")
        or policy.startswith("obs_")
        or "_bank_" in policy
    ):
        level = _bank_level_from_policy(policy) if "_bank_" in policy else _level_from_policy(policy)
        return [
            value
            for view in views
            if (value := _first_cross(_quote_mults(view.row), level)) is not None
        ]
    return []


def _level_from_policy(policy: str) -> float:
    parts = policy.split("_")
    token = next(part for part in reversed(parts) if part.endswith("x"))
    return float(token.removesuffix("x").replace("p", "."))


def _bank_level_from_policy(policy: str) -> float:
    parts = policy.split("_")
    bank_idx = parts.index("bank")
    token = parts[bank_idx + 1]
    return float(token.removesuffix("x").replace("p", "."))


def _print_policy_row(
    policy: str,
    views: list[ReplayRow],
    current: float,
    *,
    width: int = 14,
) -> None:
    total = sum(view.returns[policy] for view in views)
    wins = sum(1 for view in views if view.returns[policy] > 0)
    avg = total / len(views) if views else 0.0
    hit_values = _policy_hit_mults(policy, views)
    avg_hit = sum(hit_values) / len(hit_values) if hit_values else None
    hit_rate = len(hit_values) / len(views) if views else 0.0
    print(
        f"{policy:<{width}} {total:>+10.2f} {total - current:>+10.2f} "
        f"{wins / len(views):>8.1%} {avg:>+8.3f} {hit_rate:>6.1%} {len(hit_values):>7} {_mult(avg_hit):>9}"
    )


def _print_detail(views: list[ReplayRow], limit: int) -> None:
    rows = sorted(
        views,
        key=lambda view: max(
            view.returns["bank_1p3x"],
            view.returns["p50_bank_1p3x"],
            view.returns["lock_or_bank_1p5x_1p2x"],
            view.returns["dyn_a1p4_f1p1_s3_nb1p2_0p9"],
            view.returns["bor_b1p4_a1p7_t3x_f1p25_w10m_s3"],
            view.returns["soft_hs25_life1p15_w5m_floor0p55_rec1p2"],
            view.returns["bank1p4_soft_hs25_life1p15_w5m_floor0p55_rec1p2"],
            view.returns["cdelay_b1p4_hs25_band0p5to0p7_life0p9_age5_w3_floor0p4_rec1x"],
            view.returns["win_a1p4_stall3"],
            view.returns["obs_2x"],
        ) - view.returns["current"],
        reverse=True,
    )[:limit]

    print("\nDetail — biggest bank/lock improvement first")
    hdr = (
        f"{'call':>7} {'symbol':<12} {'ent':>5} {'qpk':>5} {'qmax':>5} {'spk':>5} "
        f"{'cur':>8} {'bank14':>8} {'bor14':>8} {'b+soft':>8} {'obs2':>8} {'shadow':>8} "
        f"{'raw_reason':<12} {'q_reason/shadow_reason':<28}"
    )
    print(hdr)
    print("-" * len(hdr))
    for view in rows:
        row = view.row
        reason_pair = f"{row.get('qsim_reason')}/{row.get('shadow_reason')}"
        print(
            f"{int(row['call_id']):>7} "
            f"{(row.get('symbol') or '?')[:12]:<12} "
            f"{_mult(_entry_ratio(view)):>5} "
            f"{_mult(_f(row.get('qsim_peak'))):>5} "
            f"{_mult(view.max_quote_mult):>5} "
            f"{_mult(_f(row.get('shadow_peak'))):>5} "
            f"{_pct(view.returns['current']):>8} "
            f"{_pct(view.returns['bank_1p4x']):>8} "
            f"{_pct(view.returns['bor_b1p4_a1p7_t3x_f1p25_w10m_s3']):>8} "
            f"{_pct(view.returns['bank1p4_soft_hs25_life1p15_w5m_floor0p55_rec1p2']):>8} "
            f"{_pct(view.returns['obs_2x']):>8} "
            f"{_pct(view.shadow_return):>8} "
            f"{(row.get('raw_config_reason') or '?')[:12]:<12} "
            f"{reason_pair[:28]:<28}"
        )


def main() -> None:
    global MAX_QOBS_MULT

    parser = argparse.ArgumentParser(
        description="Replay qsim using raw Jupiter quote observations."
    )
    parser.add_argument("--days", type=int, default=1)
    parser.add_argument("--since", default=None, help="only include qsim entries at/after this timestamp")
    parser.add_argument("--channel", default="any")
    parser.add_argument("--lane", default="any")
    parser.add_argument("--variant", default="early")
    parser.add_argument("--min-entry-ratio", type=float, default=None)
    parser.add_argument("--max-entry-ratio", type=float, default=None)
    parser.add_argument(
        "--where",
        action="append",
        default=[],
        help="pre-filter rows with a numeric condition; repeat for combos",
    )
    parser.add_argument("--include-stale", action="store_true",
                        help="include rows whose close was decided on a stale quote "
                             "(exit_reason stale_*). Off by default — those paths measure the "
                             "quote outage, not the strategy.")
    parser.add_argument("--min-obs", type=int, default=0,
                        help="minimum priced quote observations over the position's life "
                             "(qsim_positions.obs_count). 0 = off. Try 5+ for policy work.")
    parser.add_argument("--max-gap-secs", type=float, default=0.0,
                        help="reject rows with a blind hole longer than this anywhere in the "
                             "position's life (qsim_positions.max_gap_secs). 0 = off. A hole "
                             "can hide a 1.3x touch that fell back before the next quote.")
    parser.add_argument("--require-qobs", action="store_true",
                        help="only include rows with quote observations")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--raw", action="store_true", help="include phantom-price rows")
    parser.add_argument(
        "--max-qmax",
        type=float,
        default=MAX_QOBS_MULT,
        help="ignore quote observations above this multiple as quote artifacts",
    )
    parser.add_argument(
        "--include-post-exit",
        action="store_true",
        help="include qsim post-exit quote probes in replay observations",
    )
    parser.add_argument(
        "--post-exit-mins",
        type=float,
        default=90.0,
        help="minutes of post-exit qsim quote probes to include with --include-post-exit",
    )
    parser.add_argument(
        "--fallback",
        choices=FALLBACK_CHOICES,
        default="current",
        help=(
            "return used when a replay policy does not trigger: current keeps old behavior; "
            "raw_config replays the base exit config; last_quote holds to the last observed quote; "
            "hard_stop uses --fallback-hard-stop-pct; hard_stop_35 is legacy fixed -35%%"
        ),
    )
    parser.add_argument(
        "--fallback-hard-stop-pct",
        type=float,
        default=0.35,
        help="loss fraction for --fallback hard_stop, e.g. 0.30 means -30%%",
    )
    parser.add_argument("--detail", action="store_true", help="print per-trade rows")
    args = parser.parse_args()
    MAX_QOBS_MULT = args.max_qmax
    where_clauses = _parse_where(args.where)

    params = {
        "days": args.days,
        "since": args.since,
        "channel": args.channel,
        "lane": args.lane,
        "variant": args.variant,
        "min_entry_ratio": args.min_entry_ratio,
        "max_entry_ratio": args.max_entry_ratio,
        "raw": args.raw,
        "max_peak": MAX_SANE_PEAK,
        "max_pnl": MAX_SANE_PNL_PCT,
        "min_pnl": MIN_SANE_PNL_PCT,
        "max_qmax": args.max_qmax,
        "include_post_exit": args.include_post_exit,
        "post_exit_mins": args.post_exit_mins,
        "include_stale": args.include_stale,
        "min_obs": args.min_obs,
        "max_gap_secs": args.max_gap_secs,
    }

    rows = _rows(params)
    rows = [row for row in rows if _passes_where(row, where_clauses)]
    views = [
        _view(
            row,
            fallback_mode=args.fallback,
            fallback_hard_stop_pct=args.fallback_hard_stop_pct,
        )
        for row in rows
    ]
    if args.require_qobs:
        views = [view for view in views if int(view.row.get("qobs_count") or 0) > 0]
    print(
        f"filters: days={args.days} channel={args.channel} lane={args.lane} "
        f"variant={args.variant} since={args.since} min_entry_ratio={args.min_entry_ratio} "
        f"max_entry_ratio={args.max_entry_ratio} require_qobs={args.require_qobs} "
        f"include_stale={args.include_stale} min_obs={args.min_obs} "
        f"max_gap_secs={args.max_gap_secs:g} "
        f"max_qmax={args.max_qmax} include_post_exit={args.include_post_exit} "
        f"post_exit_mins={args.post_exit_mins:g} fallback={args.fallback} "
        f"fallback_hard_stop_pct={args.fallback_hard_stop_pct:g} "
        f"where={args.where or 'none'}"
    )
    _print_summary(views)
    if views and args.detail:
        _print_detail(views, args.limit)
    elif views:
        print("\nRun with --detail for per-trade rows.\n")


if __name__ == "__main__":
    main()
