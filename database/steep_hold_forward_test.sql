-- steep_hold_forward_test.sql — score the steep-hold rule on qsim banks it has NOT seen.
--
-- The rule (live_trader LIVE_STEEP_HOLD, qsim QSIM_STEEP_TRACK_HOURS): at the 2x bank, if
-- the coin's real multiple 3 minutes earlier was under 1.20x of entry ("steep"), hold
-- instead of banking; sell at the target, at the floor (multiples of entry) or after 6h.
--
-- Found 2026-10-17 on banks up to that day, so ONLY banks after it are a fair test. qsim
-- keeps steep banks on 15s looks until 5x / 1.2x / 6h, so these paths are dense.
--
-- Plain SQL (TablePlus). Change the date on the line marked FORWARD WINDOW to move it.
-- Reads as: net_positions > 0 means holding beat banking for that group. The steep
-- group should be positive and the slow group negative for the rule to be believed.
-- Targets are credited at exactly the target (slightly generous); floors are sold at
-- the first look through the line (honest at 15s).

WITH b AS (
  SELECT qp.call_id, qp.exit_time, qp.sol_out / qp.sol_in AS bank_x,
         (SELECT o.real_mult FROM qsim_quote_observations o
           WHERE o.call_id = qp.call_id AND o.real_mult IS NOT NULL
             AND o.observed_at <= qp.exit_time - interval '3 minutes'
           ORDER BY o.observed_at DESC LIMIT 1) AS x3
  FROM qsim_positions qp
  WHERE qp.status = 'closed' AND qp.exit_reason LIKE '%bank%'
    AND qp.channel_handle = 'solwhaletrending'
    AND qp.entry_time >= '2026-10-18'                     -- FORWARD WINDOW
    AND qp.exit_time  <  now() - interval '6 hours'       -- every coin has its full 6h
), g AS (
  SELECT b.*, CASE WHEN x3 IS NULL THEN '0 banked within 3 min'
                   WHEN x3 < 1.20 THEN '1 steep (under 1.20x)'
                   WHEN x3 < 1.60 THEN '2 middle'
                   ELSE '3 slow (1.60x+)' END AS grp
  FROM b
), r AS (
  SELECT g.grp, p.tgt, p.flr, g.bank_x,
         CASE WHEN th.t_at IS NOT NULL AND (fl.f_at IS NULL OR th.t_at < fl.f_at) THEN p.tgt
              WHEN fl.f_at IS NOT NULL THEN fl.f_x
              ELSE COALESCE(en.end_x, g.bank_x) END AS new_x,
         (th.t_at IS NOT NULL AND (fl.f_at IS NULL OR th.t_at < fl.f_at)) AS hit,
         (fl.f_at IS NOT NULL AND (th.t_at IS NULL OR fl.f_at < th.t_at)) AS floored
  FROM g
  CROSS JOIN (VALUES (3.0, 1.2), (5.0, 1.2)) AS p(tgt, flr)
  LEFT JOIN LATERAL (
    SELECT o.observed_at AS t_at FROM qsim_quote_observations o
    WHERE o.call_id = g.call_id AND o.observed_at > g.exit_time
      AND o.observed_at <= g.exit_time + interval '6 hours' AND o.real_mult >= p.tgt
    ORDER BY o.observed_at LIMIT 1) th ON TRUE
  LEFT JOIN LATERAL (
    SELECT o.observed_at AS f_at, CASE WHEN o.no_route THEN 0 ELSE o.real_mult END AS f_x
    FROM qsim_quote_observations o
    WHERE o.call_id = g.call_id AND o.observed_at > g.exit_time
      AND o.observed_at <= g.exit_time + interval '6 hours' AND NOT o.rate_limited
      AND (o.no_route OR o.real_mult <= p.flr)
    ORDER BY o.observed_at LIMIT 1) fl ON TRUE
  LEFT JOIN LATERAL (
    SELECT CASE WHEN o.no_route THEN 0 ELSE o.real_mult END AS end_x
    FROM qsim_quote_observations o
    WHERE o.call_id = g.call_id AND o.observed_at > g.exit_time
      AND o.observed_at <= g.exit_time + interval '6 hours' AND NOT o.rate_limited
    ORDER BY o.observed_at DESC LIMIT 1) en ON TRUE
)
SELECT grp, tgt AS target, flr AS floor,
       count(*)                              AS banks,
       count(*) FILTER (WHERE hit)           AS hit_target,
       count(*) FILTER (WHERE floored)       AS hit_floor,
       round(avg(bank_x), 2)                 AS avg_if_banked,
       round(avg(new_x), 2)                  AS avg_if_held,
       round(sum(new_x - bank_x), 2)         AS net_positions
FROM r
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
