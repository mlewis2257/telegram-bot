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
-- RUN IT:
--   sudo -u postgres psql -d solana_signals -v start="'2026-10-05'" -f clean_cadence_rederive.sql
--
-- 2026-10-05 is when the cadence fix landed. Wait until ~2026-10-12 for a full week.
--
-- DO NOT CHANGE AN ENTRY GATE INSIDE THE WINDOW. The config must hold still or the
-- comparison measures the config change instead. Frozen config as of 2026-10-05:
-- solwhaletrending / mcap 80-120k / security_flag safe only / Mon-Thu+Sun / bank_2x.

\echo '=============================================================='
\echo 'STEP 0 — CADENCE GATE. If this fails, everything below is void.'
\echo '=============================================================='
-- Want med_gap_s <= 40 and stale_pct < 5. Target cadence is QSIM_TICK_SECS=30.
-- A median near 80s means the cap is starved again (check open position count vs
-- QSIM_MAX_QUOTES_PER_MIN: positions / (cap/60) = seconds per position).
SELECT round(percentile_cont(0.5) WITHIN GROUP
             (ORDER BY qp.decision_gap_secs)::numeric, 0)             AS med_gap_s,
       round(avg(qp.decision_gap_secs)::numeric, 0)                   AS avg_gap_s,
       count(*)                                                       AS n,
       count(*) FILTER (WHERE qp.exit_reason LIKE 'stale%')            AS stale,
       round(100.0*avg((qp.exit_reason LIKE 'stale%')::int)::numeric, 1) AS stale_pct
FROM qsim_positions qp
WHERE qp.status = 'closed' AND qp.entry_time >= :start::date;

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
\echo 'READ IT IN ORDER. Step 0 gates the rest. n under ~100 in step 1 means'
\echo 'wait longer -- the whole point is a better-powered read, not a faster one.'
