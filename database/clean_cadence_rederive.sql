-- clean_cadence_rederive.sql
-- Re-derive the three numbers that were measured on a STARVED qsim.
--
-- WHY. Until 2026-10-05, QSIM_MAX_QUOTES_PER_MIN=15 could not sustain a 30s cadence on
-- more than ~7 open positions (20 / (15/60) = 80s). Observed median decision gap was
-- 64-91s and 12% of exits were stale_*. Late exit decisions realise WORSE than real, and a
-- sampler at 3x its interval misses 2x crossings -- both push the numbers DOWN. The cap was
-- raised to 30/min and the median immediately dropped to 25s with zero stale.
--
-- So the lane baseline (~-3%/SOL), the 15.46% bank rate and the safe-vs-rest split are all
-- suspect and probably PESSIMISTIC.
--
-- CORRECTION 2026-10-06: THE CAP WAS NEVER THE CONSTRAINT, and the paragraph above is
-- wrong about the cause. Over 36h the monitor made 15,838 quote attempts of which 3,905
-- (24.7%) were 429s -- 3.06 successes per 429, 7.3 quotes/min against cap=30. It fired due
-- positions back to back, tripped Jupiter's per-SECOND limit, then sat out a 30s backoff:
-- about 32 of those 36 hours. The "25s with zero stale" reading was a quiet moment; the
-- 24h after it read a 61s median with 10% stale. Fixed by pacing the monitor's quotes 2s
-- apart (QSIM_MIN_QUOTE_INTERVAL_SECS, commit ec60097), deployed 2026-10-06.
--
-- RUN IT:
--   sudo -u postgres psql -d solana_signals -v start="'2026-10-07'" -f clean_cadence_rederive.sql
--
-- 2026-10-07 is the first full UTC day after the pacing deploy. Data before it is NOT
-- comparable. Wait until ~2026-10-14 for a full week.
--
-- DO NOT CHANGE AN ENTRY GATE INSIDE THE WINDOW. The config must hold still or the
-- comparison measures the config change instead. Frozen config as of 2026-10-05:
-- solwhaletrending / mcap 80-120k / security_flag safe only / Mon-Thu+Sun / bank_2x.

-- No pager: psql otherwise hands wide output to `less`, which looks like a hang.
\pset pager off

\echo '=============================================================='
\echo 'STEP 0 — CADENCE GATE. If this fails, everything below is void.'
\echo '=============================================================='
-- Want over_ceiling_pct < 5 and pct_429 in low single digits.
--
-- NOT "median <= 40s". That target cannot be met and would restart the clock forever: the
-- adaptive cadence quotes a position 15-30 points from a threshold every 60s and one
-- further out every 90s, and a coin sitting at entry is 20 points above a 20% stop. A
-- ~61s closing gap is the scheduler working as written. What marks starvation is a close
-- decided across MORE than the 90s design ceiling.
--
-- THIS GATE VOIDS THE WINDOW, NEVER INDIVIDUAL ROWS. Do not drop stale_ or long-gap rows
-- from any step below. Closing-gap length is tied to outcome by the cadence itself -- a
-- coin near its stop is quoted every 15s, a coin that goes on to bank is usually further
-- away. Measured 2026-10-04..06: closes with a gap <= 35s banked 1 of 25, longer gaps
-- about 20%. Filtering on gap selects against banks, the same trap as --min-obs.
SELECT round(percentile_cont(0.5) WITHIN GROUP
             (ORDER BY qp.decision_gap_secs)::numeric, 0)             AS med_gap_s,
       count(*)                                                       AS n,
       count(*) FILTER (WHERE qp.decision_gap_secs > 100)              AS over_ceiling,
       round(100.0*avg((qp.decision_gap_secs > 100)::int)::numeric, 1) AS over_ceiling_pct,
       count(*) FILTER (WHERE qp.exit_reason LIKE 'stale%')            AS stale,
       round(100.0*avg((qp.exit_reason LIKE 'stale%')::int)::numeric, 1) AS stale_pct
FROM qsim_positions qp
WHERE qp.status = 'closed' AND qp.entry_time >= :start::date;

-- Starved baseline: open position 22.8% rate-limited, post-exit probe 37.0%, 7.3/min.
SELECT CASE WHEN o.note LIKE 'post_exit%' THEN 'post-exit probe'
            ELSE 'open position' END                                  AS kind,
       count(*)                                                       AS attempts,
       count(*) FILTER (WHERE o.rate_limited)                          AS n_429,
       round(100.0*avg(o.rate_limited::int)::numeric, 1)              AS pct_429
FROM qsim_quote_observations o
WHERE o.observed_at >= :start::date
GROUP BY 1 ORDER BY 1;

\echo ''
\echo '=============================================================='
\echo 'STEP 1 — BANK RATE vs BREAK-EVEN. The only number that matters.'
\echo '=============================================================='
-- Starved baseline: 15.46% observed against a 17.80% break-even -> short 2.3 points.
-- break-even = bleed / (bank + bleed), both per-trade and both positive magnitudes.
-- If bank_rate_pct now EXCEEDS breakeven_pct, the lane pays and the gap was measurement.
WITH legs AS (
  SELECT CASE WHEN qp.exit_reason LIKE '%bank%' THEN 'bank' ELSE 'rest' END AS leg,
         count(*)                                   AS n,
         avg(qp.sol_out - qp.sol_in)                AS avg_per_trade
  FROM qsim_positions qp
  JOIN calls c ON c.id = qp.call_id
  WHERE qp.status = 'closed'
    AND qp.entry_time >= :start::date
    AND qp.channel_handle = 'solwhaletrending'
    AND c.mcap_at_call BETWEEN 80000 AND 120000
  GROUP BY 1
)
SELECT (SELECT n FROM legs WHERE leg='bank')                          AS banks,
       (SELECT n FROM legs WHERE leg='rest')                          AS non_banks,
       round(100.0 * (SELECT n FROM legs WHERE leg='bank')
             / NULLIF((SELECT sum(n) FROM legs), 0), 2)               AS bank_rate_pct,
       round((SELECT avg_per_trade FROM legs WHERE leg='bank')::numeric, 5)  AS bank_per_trade,
       round((SELECT avg_per_trade FROM legs WHERE leg='rest')::numeric, 5)  AS bleed_per_trade,
       round(100.0 * abs((SELECT avg_per_trade FROM legs WHERE leg='rest'))
             / NULLIF((SELECT avg_per_trade FROM legs WHERE leg='bank')
                      + abs((SELECT avg_per_trade FROM legs WHERE leg='rest')), 0),
             2)                                                       AS breakeven_pct;

\echo ''
\echo '=============================================================='
\echo 'STEP 2 — LANE BASELINE. Starved reading was ~-3%/SOL.'
\echo '=============================================================='
-- Three independent cuts agreed on ~-3%/SOL (-3.14 / -2.89 / -3.69) while qsim was
-- starved. Expect this to be LESS negative. Also prints live for comparison -- on paired
-- trades live beat qsim 8 of 9 on 10-05 purely because qsim was deciding on stale quotes.
SELECT 'qsim in-band swt' AS src, count(*) AS n,
       round(sum(qp.pnl_sol)::numeric, 4)                             AS pnl_sol,
       round((100.0*sum(qp.pnl_sol)
              /NULLIF(sum(qp.sol_in), 0))::numeric, 2)                AS pct_per_sol,
       count(*) FILTER (WHERE qp.exit_reason LIKE '%bank%')            AS banks,
       round(100.0*avg((qp.pnl_pct <= -90)::int)::numeric, 1)         AS rug_pct,
       round(avg(qp.sol_out/NULLIF(qp.sol_in, 0))
             FILTER (WHERE qp.exit_reason LIKE '%hard_stop%')::numeric, 4) AS avg_stop_x
FROM qsim_positions qp JOIN calls c ON c.id = qp.call_id
WHERE qp.status = 'closed' AND qp.entry_time >= :start::date
  AND qp.channel_handle = 'solwhaletrending'
  AND c.mcap_at_call BETWEEN 80000 AND 120000
UNION ALL
SELECT 'live', count(*), round(sum(tp.pnl_sol)::numeric, 4),
       round((100.0*sum(tp.pnl_sol)/NULLIF(sum(tp.sol_in), 0))::numeric, 2),
       count(*) FILTER (WHERE tp.exit_reason LIKE '%bank%'),
       round(100.0*avg((tp.pnl_pct <= -90)::int)::numeric, 1),
       round(avg(tp.sol_out/NULLIF(tp.sol_in, 0))
             FILTER (WHERE tp.exit_reason LIKE '%hard_stop%')::numeric, 4)
FROM trading_positions tp
WHERE tp.is_simulation = FALSE AND tp.status = 'closed'
  AND tp.entry_time >= :start::date;

\echo ''
\echo '=============================================================='
\echo 'STEP 3 — safe vs the rest. safe FLIPPED SIGN on starved data.'
\echo '=============================================================='
-- In-band swt by flag, starved readings for reference:
--   Sep 23 -> Oct 2:  safe -8.44%  unknown +0.85%  warning +4.80%
--   Sep 27 -> Oct 4:  safe +13.69% unknown -0.89%  warning -0.69%
-- Same overlapping windows, opposite sign on safe. That instability is WHY live trades
-- safe only on a "best available read", not a proven edge. If this window puts safe
-- clearly first again, the read holds. If warning/unknown lead, revert the block:
--   LIVE_ENTRY_BLOCK_SECURITY_FLAGS=
-- Live trades only 'safe', so qsim is the ONLY source of a counterfactual on the others.
SELECT COALESCE(t.security_flag, '(null)')                            AS sec,
       count(*)                                                       AS n,
       round(sum(qp.pnl_sol)::numeric, 4)                             AS pnl_sol,
       round((100.0*sum(qp.pnl_sol)
              /NULLIF(sum(qp.sol_in), 0))::numeric, 2)                AS pct_per_sol,
       count(*) FILTER (WHERE qp.exit_reason LIKE '%bank%')            AS banks,
       round(100.0*avg((qp.exit_reason LIKE '%bank%')::int)::numeric, 1) AS bank_rate,
       round(100.0*avg((qp.pnl_pct > 0)::int)::numeric, 1)            AS win_pct,
       round(100.0*avg((qp.pnl_pct <= -90)::int)::numeric, 1)         AS rug_pct,
       round(avg(t.token_age_minutes)::numeric, 1)                    AS avg_age_m
FROM qsim_positions qp
JOIN tokens t ON t.id = qp.token_id
JOIN calls  c ON c.id = qp.call_id
WHERE qp.status = 'closed' AND qp.entry_time >= :start::date
  AND qp.channel_handle = 'solwhaletrending'
  AND c.mcap_at_call BETWEEN 80000 AND 120000
GROUP BY 1 ORDER BY pnl_sol DESC;

\echo ''
\echo '=============================================================='
\echo 'STEP 4 — PAIRED CALIBRATION. Is qsim still reading worse than live?'
\echo '=============================================================='
-- The same coins, traded by both. Starved baseline, 39 coins 2026-09-29 -> 10-06:
--
--   tokens/SOL at entry, qsim over live   0.996   (no entry bias; per-coin scatter ~10%)
--   return per SOL          qsim +0.5%    live +5.3%
--   average bank            qsim 2.38x    live 2.10x
--   average non-bank trade  qsim 0.75x    live 0.90x
--   break-even bank rate    qsim 15.1%    live 8.4%
--
-- qsim exaggerated BOTH tails: deeper stops (late look) and fatter banks (late look lands
-- past 2x). Three flips carried most of the gap -- JITI, XEET, ANON, where qsim hard-stopped
-- at 0.78 / 0.28 / 0.47 and live exited profit_floor at 1.00 / 1.26 / 1.05.
--
-- That paired gap was under one standard error at n=39, so it is a lead, not a result.
-- WHAT TO LOOK FOR: if x_gap closes toward zero and the two break-evens converge, qsim is
-- a usable ruler again for ranking lanes and flags. If qsim still reads several points
-- worse, its cadence near the stop needs tightening before any stop or runner replay
-- (stop_grace, partial_runner, floor_checkpoint) can be trusted -- and until then the
-- strategy is judged on live's wallet. Step 1's break-even is on QSIM's ruler; this is
-- the step that says whether that ruler can be believed.
WITH paired AS (
  SELECT (qp.entry_tokens / qp.sol_in) / (tp.tokens_held / tp.sol_in)  AS tok_ratio,
         qp.sol_out / qp.sol_in                                        AS qx,
         tp.sol_out / tp.sol_in                                        AS lx,
         qp.exit_reason LIKE '%bank%'                                  AS qbank,
         tp.exit_reason LIKE '%bank%'                                  AS lbank
  FROM trading_positions tp
  JOIN qsim_positions qp ON qp.call_id = tp.call_id
  WHERE tp.is_simulation = FALSE AND tp.status = 'closed' AND qp.status = 'closed'
    AND tp.entry_time >= :start::date
    AND tp.tokens_held > 0 AND tp.sol_in > 0 AND qp.sol_in > 0
)
SELECT count(*)                                                       AS n,
       round(avg(tok_ratio)::numeric, 3)                              AS entry_tok_ratio,
       round(100.0*(avg(qx) - 1)::numeric, 2)                         AS qsim_pct_per_sol,
       round(100.0*(avg(lx) - 1)::numeric, 2)                         AS live_pct_per_sol,
       round(100.0*avg(lx - qx)::numeric, 2)                          AS x_gap_pts,
       round((avg(lx - qx) / NULLIF(stddev_samp(lx - qx) / sqrt(count(*)), 0))::numeric, 2)
                                                                      AS gap_t,
       count(*) FILTER (WHERE qbank)                                  AS qsim_banks,
       count(*) FILTER (WHERE lbank)                                  AS live_banks,
       round(avg(qx) FILTER (WHERE NOT qbank)::numeric, 3)            AS qsim_nonbank_x,
       round(avg(lx) FILTER (WHERE NOT lbank)::numeric, 3)            AS live_nonbank_x,
       round(100.0*((1 - avg(qx) FILTER (WHERE NOT qbank))
             / NULLIF((avg(qx) FILTER (WHERE qbank) - 1)
                      + (1 - avg(qx) FILTER (WHERE NOT qbank)), 0))::numeric, 1)
                                                                      AS qsim_breakeven_pct,
       round(100.0*((1 - avg(lx) FILTER (WHERE NOT lbank))
             / NULLIF((avg(lx) FILTER (WHERE lbank) - 1)
                      + (1 - avg(lx) FILTER (WHERE NOT lbank)), 0))::numeric, 1)
                                                                      AS live_breakeven_pct
FROM paired;

-- The flips: same coin, the two rulers disagree by more than 25 points. Read these by
-- hand -- a handful of them is the whole gap, in either direction.
SELECT tp.call_id, t.symbol,
       round((qp.sol_out / qp.sol_in)::numeric, 3)                    AS qsim_x,
       round((tp.sol_out / tp.sol_in)::numeric, 3)                    AS live_x,
       qp.exit_reason AS qsim_exit, tp.exit_reason AS live_exit,
       round(qp.decision_gap_secs)                                    AS qsim_gap_s
FROM trading_positions tp
JOIN qsim_positions qp ON qp.call_id = tp.call_id
JOIN calls c  ON c.id = tp.call_id
JOIN tokens t ON t.id = c.token_id
WHERE tp.is_simulation = FALSE AND tp.status = 'closed' AND qp.status = 'closed'
  AND tp.entry_time >= :start::date AND tp.sol_in > 0 AND qp.sol_in > 0
  AND abs(tp.sol_out / tp.sol_in - qp.sol_out / qp.sol_in) > 0.25
ORDER BY (tp.sol_out / tp.sol_in - qp.sol_out / qp.sol_in) DESC;

\echo ''
\echo 'READ IT IN ORDER. Step 0 gates the WINDOW, never single rows. n under ~100 in'
\echo 'step 1 means wait longer. Step 4 says whether step 1 is on a ruler you can believe.'
