# QSIM / Live Trading Bot Handoff

Last updated: 2026-09-05 PT

This document is the working handoff for the Solana meme-coin Telegram trading bot. It explains what changed, why it changed, what is currently being tested, and what a new agent should trust or not trust.

## Application Overview

The app watches Telegram meme-coin call channels, records token/call data in Postgres, and runs multiple trading/simulation layers:

- **Listener**: reads Telegram calls, parses tokens, classifies lanes such as `low_score`, and dispatches paper/shadow/qsim/live paths.
- **Shadow**: paper system using feed/reference market-cap prices. Useful for broad screening, but can overstate edge due stale feed entries, unsellable peaks, or source mismatches.
- **QSIM**: quote-priced forward simulation. It uses executable Jupiter buy/sell quote data, stores qsim positions and quote observations, and is now treated as the main source of truth for whether a strategy could actually execute.
- **Live trader**: real wallet execution path. Live is controlled by env flags, lane allowlists, entry quality gates, and exit overlays.

## Core Lesson Learned

Do not trust one report in isolation.

The project has repeatedly been fooled by:

- Feed prices that looked profitable but were not executable.
- Shadow exits crediting ideal peaks that qsim could not quote/sell.
- Mixed historical windows where qsim logic changed mid-run.
- Replay output being compared against live qsim output from a different strategy or hard-stop configuration.
- Post-exit observations being interpreted as live-capturable when they are really opportunity/research data.

The safest hierarchy right now is:

1. **Actual live fills** when live is enabled.
2. **QSIM live-forward rows** using executable quotes.
3. **QSIM quote replay** for strategy comparison, only on clean windows.
4. **Shadow** for screening and day-of-week hints, not execution truth.

## Current Main Research Window

The current clean window starts after the stale-hard-stop fix and lane cleanup:

```bash
SINCE='2026-09-03 23:00:00 UTC'
```

Reason: earlier data includes strategy/config/code changes that muddy results, especially stale qsim opens that should have been hard-stopped.

Use this window for current qsim checks unless intentionally doing historical research.

## Current Production Intent

Current active qsim research lanes:

- `solwhaletrending / none / low_score / early`
- `solhousesignal / none / low_score / early`

Removed from qsim:

- `solwhaletrending / none / none / early`

Current live allowlist:

- `solwhaletrending / none / low_score / early`
- Live skips Friday in code.
- qsim still observes Friday for research.

Current tested strategy/env intent:

```bash
QSIM_ENABLED=true
QSIM_EXIT_OVERLAY_STRATEGY=bank_1p3x
QSIM_HARD_STOP_PCT=0.20
QSIM_NO_BOUNCE_STOP_ENABLED=false
QSIM_RUNNER_WINDOW_ENABLED=false
QSIM_MAX_ENTRY_EXEC_RATIO=1.5
QSIM_ENTRY_ROUNDTRIP_MIN_MULT=0.65
QSIM_TICK_SECS=30
QSIM_MAX_QUOTES_PER_MIN=10
QSIM_POST_EXIT_OBS_ENABLED=true
QSIM_POST_EXIT_OBS_MINS=90
QSIM_POST_EXIT_OBS_CADENCE_SECS=60
QSIM_POST_EXIT_OBS_LIMIT=100
QSIM_POST_EXIT_OBS_MAX_PER_MIN=3

LIVE_EXIT_OVERLAY_STRATEGY=bank_1p3x
LIVE_HARD_STOP_PCT=0.20
LIVE_MAX_ENTRY_EXEC_RATIO=1.5
LIVE_ENTRY_ROUNDTRIP_MIN_MULT=0.65
LIVE_NO_BOUNCE_STOP_ENABLED=false
LIVE_USE_LANE_POLICY=true
LIVE_LANE_STRATEGY=LIVE
LIVE_POSITION_SIZE_SOL=0.05
```

Do not paste or commit real secrets from `.env`.

## Recent Code Changes

Recent important commits:

- **2026-09-05 (uncommitted): live/qsim symmetry pass.** Reviewing the real `.env` against the
  code surfaced three live-only divergences; all three are now fixed.
  - `live_trader.py` quiet hours: live skipped `QUIET_HOURS_PST = {4, 9, 14}` for every channel
    except solhousesignal/solhousesignal_vip. `solwhaletrending` was never exempted, so live
    dropped ~3 hours/day of its ONLY allowlisted lane while qsim took those calls ungated —
    every qsim-vs-live comparison silently included trades live could not make. Now bypassed
    when `LIVE_USE_LANE_POLICY=true`, matching how the legacy strategy_engine gate is already
    bypassed (lane_policy IS the entry decision). Revert = `LIVE_USE_LANE_POLICY=false`.
  - `live_exit_basis()` now returns a 5-tuple `(current, peak, entry, basis, raw_mult)`, where
    `raw_mult` is the unguarded executable multiple (qsim's `real_mult`). None on feed basis.
  - `check_live_exits(..., raw_mult=...)` uses it for the bank overlay (qsim `6da293d`) and adds
    a raw-exec hard stop before `guard_trough` (qsim `e81d4d7`). Live had NEITHER fix: its
    overlay read the `min(synth, real_peak)`-capped value, and `guard_peak` withholds a >50%
    single-tick jump for one reading — so on exactly the violent spikes `bank_1p3x` exists to
    catch, the guarded mult read below 1.3x and the bank did not fire. Verified: same tick,
    raw=1.35x banks, guarded=1.00x holds. Any quote failure falls back to old behavior.
  - Call sites updated: `monitor.py` (x2), `ws_monitor.py` (x1). `WS_COARSE_EXIT_VARIANTS=early`
    was checked and is PAPER-only — it does not slow live exits.

- `6da293d Fix qsim bank overlay quote source`
  - Bank overlay now uses raw executable Jupiter quote data instead of guarded/filtered peak data.
  - This fixed qsim closing at negative/low values even when the real quote had crossed the bank threshold.

- `6525342 Add executable entry ratio gate`
  - Added `entry_quality.py`.
  - Added `QSIM_MAX_ENTRY_EXEC_RATIO` and `LIVE_MAX_ENTRY_EXEC_RATIO`.
  - Purpose: skip entries where executable quote market cap is too far above the reference/feed market cap.

- `7d27739 Add entry roundtrip quote gate`
  - Added `QSIM_ENTRY_ROUNDTRIP_MIN_MULT` and `LIVE_ENTRY_ROUNDTRIP_MIN_MULT`.
  - Purpose: avoid entering positions where immediate buy/sell roundtrip quality is terrible.

- `21c197e qsim hardstop env import`
  - Added `QSIM_HARD_STOP_PCT` support.
  - Before this, qsim was still using hard-stop defaults even when the env was set differently.

- `7ca1e9a Add bank-or-run replay policies`
  - Added replay-only hybrid bank/run policies for research.

- `edcef13 Add soft stop recovery replay policies`
  - Added replay-only tests for delaying exits on possible recovery candidates.

- `e0c99d6 Add combined bank soft-stop replay policies`
  - Added combined replay policies mixing early banking and soft-stop recovery behavior.

- `689fbc5 Add qsim recovery classifier report`
  - Added report to classify loser recovery patterns after qsim exits.

- `fe4ffbc Add conditional qsim stop delay replay`
  - Added conditional delay replay policies based on recovery-classifier style signals.

- `e81d4d7 Fix qsim stale hard stops`
  - Critical fix.
  - qsim now closes raw executable sell quotes below hard stop before `guard_trough` can keep a position alive indefinitely.
  - qsim now quotes open positions before post-exit research probes.
  - `db.get_open_qsim_positions()` now returns oldest opens first.
  - This closed zombie positions that had been open for hours/day despite quote values far below hard stop.

- `0aa05cd Remove flat swt qsim lane`
  - Removed `solwhaletrending / none / none` from qsim monitoring.

- `95e8fb5 trades everday except Friday. F is historically the worst day to trade`
  - Live `solwhaletrending / none / low_score` now excludes Friday.
  - qsim still observes Friday because qsim lanes list all days.

## Key Files

- `bot/python_ai/qsim.py`
  - qsim lane definitions, entry gates, quote monitor, overlays, post-exit probes, hard-stop override.

- `bot/python_ai/lane_policy.py`
  - live lane allowlist and day gates.
  - `LIVE_LANES` currently allows SWT low_score every day except Friday.

- `bot/python_ai/live_trader.py`
  - real live entry/execution path.
  - reads live entry gates and live exit overlay env.

- `bot/python_ai/entry_quality.py`
  - shared entry quality helpers for executable/ref ratio and roundtrip gates.

- `bot/python_ai/qsim_quote_capture_replay.py`
  - main replay tool for testing exit strategies on qsim quote observations.

- `bot/python_ai/qsim_lane_scan.py`
  - compares current qsim by lane and best replay policy.

- `bot/python_ai/qsim_shadow_trade_reconcile.py`
  - reconciles shadow vs qsim entries/exits/peaks. Useful for diagnosing shadow/qsim disagreement.

- `bot/python_ai/qsim_recovery_classifier.py`
  - analyzes whether qsim losers recover after exit.

- `bot/python_ai/qsim_after_exit_move_report.py`
  - analyzes post-exit moves.

## What Is Being Tested Right Now

### Test 1: Bank Early Strategy

Current strategy:

- `bank_1p3x`
- Hard stop: `20%`
- Entry quality gates enabled.

Why:

- Most coins do not reach huge multiples.
- A meaningful subset hits 1.3x quickly.
- Banking early turns many small runners into realized wins.
- The goal is to stack frequent wins and keep unavoidable rugs smaller.

Current clean-window behavior from pasted results:

- SWT bank wins were positive and 100% winners.
- Hard stops were still negative, but the average loss tightened compared with old 35% behavior.
- SHS looked worse than SWT but is still being observed.

### Test 2: Hard Stop Tightness

Tested or discussed:

- 35% hard stop: gave bigger breathing room but worse losses when rugs keep going.
- 25% hard stop: looked better than 35% in some runs.
- 20% hard stop: currently active; may reduce loss size, but can also cut recoverable trades too early.
- 40/45/50% hard stops: tested and “sucked”; not current candidates.

Current active choice:

- `QSIM_HARD_STOP_PCT=0.20`
- `LIVE_HARD_STOP_PCT=0.20`

### Test 3: Post-Exit Recovery Research

Post-exit observations are enabled.

Why:

- Many qsim trades that exit at a loss later recover to 0.9x, 1.0x, 1.2x, or higher.
- This suggests a possible future smarter strategy: do not instantly exit every weak dip if there is evidence of recoverable structure.

Important warning:

- Post-exit replay is **not live-aligned** unless the live bot would actually still be holding.
- Treat `--include-post-exit` as opportunity mapping, not proof of live PnL.

### Test 4: Hybrid Runner / Partial Bank Strategy

The user’s thesis:

- Bank early on weak/normal coins.
- If a coin shows real strength, hold some exposure longer to catch 2x/4x/8x runners.

Evidence:

Some banked trades later moved much higher after exit, e.g. examples the user identified:

- `MAGATARD`: banked around `1.807x`, later around `4.097x`.
- `BEN`: banked around `1.467x`, later around `8.035x`.
- `USTF`: banked around `1.395x`, later around `2.846x`.
- `WOTF`: banked around `1.499x`, later around `2.681x`.
- `COIN`: banked around `1.368x`, later around `7.555x`.

Potential future strategy shape:

- Full bank at 1.3x is safest/simple.
- Partial bank could sell 50-75% at 1.3x, then trail or target higher with the rest.
- But this must be tested live-forward or qsim-forward because replay can over-credit if post-exit data is used incorrectly.

## FIRST CLEAN DAY — 2026-09-06 (106 trades)

The plumbing held: **0 stale rows**, max_gap p50 67s / worst 136s (nothing near the 180s
threshold), post-exit probes median 154 per bank (was 39). This data is trustworthy.

The strategy is a confirmed loser — but 73% of it was one bucket qsim should never have traded:

| | n | pnl | mean/trade | 95% CI | |
|---|---|---|---|---|---|
| All closed | 106 | -0.6220 | -11.74% | [-21.9%, -1.6%] | **NEGATIVE** |
| solhousesignal > $5M | 13 | **-0.4536** | -69.78% | [-100.8%, -38.8%] | **NEGATIVE** |
| Everything else | 93 | -0.1684 | -3.62% | [-13.3%, +6.1%] | inconclusive |

Ten of those 13 exited at ~0.000, including an obvious rug series cycled through
solhousesignal: **WOTF x4, WOAF, IOAF, VOF, GOAF — all $19-33M, all exactly 0.000.**

ROOT CAUSE: qsim had NO mcap ceiling while live has always had one (`MCAP_LIMITS`). Same family
as the quiet-hours gate and the position cap — a live guard qsim did not mirror, so qsim was
pricing a strategy live cannot run. FIXED 2026-09-06: `qsim_open` now enforces `MCAP_LIMITS`,
checked against the FEED/reference mcap the way live does (NOT qsim's executable `entry_price`,
which is demonstrably wrong on some tokens — URANUS read 283 against a real 260k, AOC 213 vs
230k; harmless for PnL since exit_mult is sol_out/sol_in, but it would make the ceiling leak).

CEILINGS RETUNED, and they point in OPPOSITE directions per channel:

| lane | above ceiling | at/below | decision |
|---|---|---|---|
| solhousesignal | -33.0%/trade (n=30) | -0.68% (n=30) | ceiling **$280k** (was 175k) |
| solwhaletrending | +1.60% (n=28) | -15.47% (n=18) | **ceiling REMOVED** (0 = off) |

solhousesignal's whole loss is oversized entries; the ceiling rescues the lane from -16.8% to
-0.68%. solwhaletrending is the reverse — its 100k cap was rejecting the better half (BEBE
2.14x, TOA 1.77x), and loosening improved monotonically (100k -15.5%, 175k -11.3%, 280k -6.8%,
uncapped -5.1%). CIs overlap on SWT, so revisit with more data.

STILL OPEN: whether to keep solhousesignal in QSIM_LANES at all. With the ceiling it is flat
rather than losing, so it costs nothing to keep and it is the adversarial referee. Cutting it
would free the whole quote budget for solwhaletrending, the only live lane. One-line change.

## THE RUNNER FINDING (2026-09-05, measured on real post-exit quotes)

`bank_1p3x` is systematically selling the front of real moves. Of 40 banked trades in the
09-04/09-05 window, measured on qsim's OWN executable post-exit quotes (not shadow, so the
shadow entry-inflation problem does not touch this):

| post-exit peak | banks (n=40) | hard stops (n=72) |
|---|---|---|
| >= 1.5x | 27 (67.5%) | 5 (6.9%) |
| >= 2.0x | **16 (40.0%)** | 5 (6.9%) |
| >= 3.0x | 6 (15.0%) | 0 |
| >= 5.0x | 3 (7.5%) | 0 |

IBRL banked 2.186 -> 6.086. CAT 1.877 -> 5.473. CLIFFORD 1.546 -> 5.323.

Rough EV of keeping a 30% runner (cost = giving up 30% of the bank on every bank that did not
run, exiting the runner at a 1.1x floor): **+1.24%/trade at 40% capture of the observed run,
+1.94% at 50%, +3.35% at 70%.** Top 3 runners are 41% of the gain — fat-tailed, but 16 events,
not one outlier.

WHAT IS AND IS NOT UNCERTAIN: the frequency is solid (16/40, 95% CI roughly [25%, 55%]). The
CAPTURE FRACTION is not — how much of a run a trailing stop actually gets. That is what the
post-exit cadence change (60s -> 30s, banks probed first) exists to measure. Post-exit sampling
was a median of 39 quotes over 90 min, ~2.3 min apart; you cannot simulate a 25-35% trail on
that (you miss the peak, or miss the trigger on the way down) and the three biggest runners had
the thinnest coverage (CAT's 5.473x rests on 6 quotes).

Under-sampling MISSES peaks, so 40% is a floor, not a ceiling.

NOT accessible to a runner leg: the 5 hard stops that later recovered past 2x. You already sold
at the stop. Capturing those means not stopping out — a different and much riskier change.

`pbr_*` policies in the replay implement the shape (sell 70% at 1.3x, runner exits on target /
1.1x floor / 30% trail from its own peak / 10 min with no new high). Verified against the spec
on synthetic paths. NOTE from that test: a 2x target DEFEATS the runner on the biggest movers —
IBRL's bank quote was already 2.186, past the target, so the whole position exits there. The
`t3x` variant is the one that rides those. Read `pbr_*` rows as an UPPER BOUND until the denser
post-exit data lands, since the runner exits later than qsim did and needs `--include-post-exit`.

## THE CLEAN WINDOW PROTOCOL (2026-09-06 onward)

Everything before `2026-09-06 00:00:00 UTC` is research scrap, not evidence. Not only the 19
starved rows — the whole window ran with the quote budget pinned at ~100%, so cadence was ~60s
and hard stops realized -30% against a configured -20%. Removing the fabricated rows leaves
degraded rows.

`qsim_clean_window.py` is the ONE report for this window. It asks two pre-registered questions
and refuses to answer either until the sample supports it:

```bash
python3 qsim_clean_window.py --since '2026-09-06 00:00:00 UTC' --fee-pct 0.02
```

WHY ONLY TWO QUESTIONS: `qsim_quote_capture_replay.py` sweeps ~150 policies. Picking the best
on a small sample finds noise nearly every time — that is exactly how a 28-trade window produced
a "best policy" (`no_1p3x_stop_0p85x`, +1.25%/trade) that was inside round-trip fee cost. Do not
run the sweep for decisions in this window. Run it to generate hypotheses, then test ONE.

SAMPLE SIZE IS THE BINDING CONSTRAINT. Per-trade volatility is ~37-47%, so:

| effect | trades needed | at ~14 SWT/day |
|---|---|---|
| +10%/trade | ~53 | 4 days |
| +5%/trade | ~211 | 15 days |
| +2%/trade | ~1315 | 3+ months |

Anything too small to confirm inside two weeks is also too small to be worth trading at 0.05
SOL after fees — the detectability bar and the profitability bar are the same bar. Corollary:
tuning between `bank_1p2x` and `bank_1p3x` (a ~1% difference) is unmeasurable here. Only look
for large effects.

The paired A/B (Q2) is far cheaper than absolute profitability (Q1) because both policies run
over the SAME rows and the per-row difference cancels most of the volatility. Expect Q2 to
resolve in days while Q1 stays INCONCLUSIVE for weeks. That asymmetry is not a bug.

The report also prints DATA QUALITY. If dropped rows exceed 15% it warns — that means the
scheduler fixes have regressed and every number below it is suspect.

## Reports / Commands To Run

Set clean window:

```bash
cd ~/telegram-bot/bot/python_ai
source .venv/bin/activate
SINCE='2026-09-03 23:00:00 UTC'
```

Check qsim lanes:

```bash
python3 qsim_lane_scan.py \
  --days 30 \
  --since "$SINCE" \
  --min-n 5 \
  --sort best
```

Replay SWT without post-exit, live-aligned:

```bash
python3 qsim_quote_capture_replay.py \
  --days 30 \
  --since "$SINCE" \
  --channel solwhaletrending \
  --lane low_score \
  --variant early \
  --max-qmax 50 \
  --fallback hard_stop \
  --fallback-hard-stop-pct 0.20
```

Replay SHS without post-exit:

```bash
python3 qsim_quote_capture_replay.py \
  --days 30 \
  --since "$SINCE" \
  --channel solhousesignal \
  --lane low_score \
  --variant early \
  --max-qmax 50 \
  --fallback hard_stop \
  --fallback-hard-stop-pct 0.20
```

Replay with post-exit only for opportunity research:

```bash
python3 qsim_quote_capture_replay.py \
  --days 30 \
  --since "$SINCE" \
  --channel solwhaletrending \
  --lane low_score \
  --variant early \
  --include-post-exit \
  --post-exit-mins 90 \
  --max-qmax 50 \
  --fallback hard_stop \
  --fallback-hard-stop-pct 0.20
```

Check qsim/live process logs:

```bash
pm2 logs sol-qsim --nostream --lines 120 \
  | grep -iE "monitor started|hard_stop override|exit overlay|max entry exec|entry roundtrip|429|backoff"
```

Expected current qsim log shape:

```text
[qsim] monitor started — lanes=[('solhousesignal', 'none', 'low_score'), ('solwhaletrending', 'none', 'low_score')] cap=10/min cadence=30.0s enabled=True
[qsim] exit overlay: bank_1p3x
[qsim] max entry exec/ref ratio: 1.5x
[qsim] entry roundtrip min: 0.65x
[qsim] hard_stop override: -20% (was -35%)
```

Check closes:

```bash
pm2 logs sol-qsim --nostream --lines 1000 \
  | grep -iE "OPEN|CLOSE|bank_1p3x|hard_stop|profit_floor|entry roundtrip|entry exec/ref|429|backoff|no-route"
```

## Useful SQL Checks

### Lane / exit reason summary since clean window

```sql
SELECT
  qp.channel_handle,
  COALESCE(qp.vip_tier, 'none') AS vip_tier,
  COALESCE(qp.lane, 'none') AS lane,
  COALESCE(qp.variant, 'none') AS variant,
  COALESCE(qp.exit_reason, 'open') AS exit_reason,
  COUNT(*) AS positions,
  ROUND(SUM(qp.pnl_sol) FILTER (WHERE qp.status = 'closed')::numeric, 4) AS pnl_sol,
  ROUND(AVG(qp.pnl_sol / NULLIF(qp.sol_in, 0)) FILTER (WHERE qp.status = 'closed')::numeric, 4) AS avg_return,
  ROUND(100.0 * AVG((qp.pnl_sol > 0)::int) FILTER (WHERE qp.status = 'closed'), 1) AS win_pct
FROM qsim_positions qp
WHERE qp.entry_time >= '2026-09-03 23:00:00 UTC'::timestamptz
GROUP BY
  qp.channel_handle,
  COALESCE(qp.vip_tier, 'none'),
  COALESCE(qp.lane, 'none'),
  COALESCE(qp.variant, 'none'),
  COALESCE(qp.exit_reason, 'open')
ORDER BY qp.channel_handle, lane, exit_reason;
```

### Hard-stop health by channel

```sql
SELECT
  qp.channel_handle,
  COUNT(*) AS hard_stops,
  ROUND(AVG(qp.sol_out / NULLIF(qp.sol_in, 0))::numeric, 3) AS avg_exit_mult,
  ROUND(MIN(qp.sol_out / NULLIF(qp.sol_in, 0))::numeric, 3) AS worst_exit_mult,
  ROUND(MAX(qp.sol_out / NULLIF(qp.sol_in, 0))::numeric, 3) AS best_exit_mult
FROM qsim_positions qp
WHERE qp.entry_time >= '2026-09-03 23:00:00 UTC'::timestamptz
  AND qp.status = 'closed'
  AND qp.exit_reason = 'hard_stop'
GROUP BY qp.channel_handle
ORDER BY qp.channel_handle;
```

### Open-position sanity check

```sql
WITH qobs AS (
  SELECT
    call_id,
    MAX(observed_at) AS last_obs_at,
    MAX(real_mult) FILTER (WHERE sol_out IS NOT NULL AND real_mult > 0 AND real_mult <= 50) AS max_mult,
    COUNT(*) FILTER (WHERE sol_out IS NOT NULL) AS obs_count
  FROM qsim_quote_observations
  GROUP BY call_id
), last_q AS (
  SELECT DISTINCT ON (call_id)
    call_id,
    observed_at,
    real_mult
  FROM qsim_quote_observations
  WHERE sol_out IS NOT NULL
  ORDER BY call_id, observed_at DESC
)
SELECT
  qp.call_id,
  t.symbol,
  qp.channel_handle,
  qp.entry_time,
  now() - qp.entry_time AS age,
  ROUND(qp.entry_price::numeric, 2) AS entry_mcap,
  qp.status,
  qp.exit_reason,
  lq.observed_at AS last_quote_at,
  ROUND(lq.real_mult::numeric, 3) AS last_mult,
  ROUND(qobs.max_mult::numeric, 3) AS max_mult,
  qobs.obs_count
FROM qsim_positions qp
JOIN tokens t ON t.id = qp.token_id
LEFT JOIN qobs ON qobs.call_id = qp.call_id
LEFT JOIN last_q lq ON lq.call_id = qp.call_id
WHERE qp.entry_time >= '2026-09-03 23:00:00 UTC'::timestamptz
  AND qp.status = 'open'
ORDER BY qp.entry_time;
```

Open positions are only suspicious if they are very old and last executable quote is clearly below the hard stop. After `e81d4d7`, that should not happen unless quote routing fails or the process is stuck.

### Trade details with before/after quote max

```sql
WITH qobs AS (
  SELECT
    qp.call_id,
    MAX(qo.real_mult) FILTER (
      WHERE qo.observed_at BETWEEN qp.entry_time AND COALESCE(qp.exit_time, now())
        AND qo.sol_out IS NOT NULL
        AND qo.real_mult > 0
        AND qo.real_mult <= 50
    ) AS max_before_exit,
    MAX(qo.real_mult) FILTER (
      WHERE qp.exit_time IS NOT NULL
        AND qo.observed_at > qp.exit_time
        AND qo.observed_at <= qp.exit_time + interval '90 minutes'
        AND qo.sol_out IS NOT NULL
        AND qo.real_mult > 0
        AND qo.real_mult <= 50
    ) AS max_after_exit
  FROM qsim_positions qp
  LEFT JOIN qsim_quote_observations qo ON qo.call_id = qp.call_id
  WHERE qp.entry_time >= '2026-09-03 23:00:00 UTC'::timestamptz
  GROUP BY qp.call_id
)
SELECT
  qp.call_id,
  t.symbol,
  t.mint,
  qp.entry_time AT TIME ZONE 'America/Los_Angeles' AS entry_pt,
  qp.exit_time AT TIME ZONE 'America/Los_Angeles' AS exit_pt,
  qp.channel_handle,
  COALESCE(qp.vip_tier, 'none') AS vip_tier,
  COALESCE(qp.lane, 'none') AS lane,
  qp.variant,
  qp.status,
  qp.exit_reason,
  ROUND(qp.sol_in::numeric, 4) AS sol_in,
  ROUND(qp.sol_out::numeric, 4) AS sol_out,
  ROUND(qp.pnl_sol::numeric, 4) AS pnl_sol,
  ROUND((qp.pnl_sol / NULLIF(qp.sol_in, 0))::numeric, 4) AS ret,
  ROUND((qp.sol_out / NULLIF(qp.sol_in, 0))::numeric, 3) AS exit_mult,
  ROUND(qp.entry_price::numeric, 2) AS entry_mcap,
  ROUND(qp.exit_price::numeric, 2) AS exit_mcap,
  ROUND(qobs.max_before_exit::numeric, 3) AS max_before_exit,
  ROUND(qobs.max_after_exit::numeric, 3) AS max_after_exit,
  ROUND((qobs.max_after_exit - (qp.sol_out / NULLIF(qp.sol_in, 0)))::numeric, 3) AS missed_after_exit
FROM qsim_positions qp
JOIN tokens t ON t.id = qp.token_id
LEFT JOIN qobs ON qobs.call_id = qp.call_id
WHERE qp.entry_time >= '2026-09-03 23:00:00 UTC'::timestamptz
ORDER BY qp.entry_time DESC;
```

## Known Pitfalls

### qsim quote budget starvation spiral (CONFIRMED 2026-09-05, fixed)

`QSIM_MAX_QUOTES_PER_MIN` was 10. Post-exit probes draw from the same pool
(`_post_exit_budget_ok` requires `_budget_ok()`), taking ~3/min, leaving ~7/min for open
positions — enough for only ~3.5 at `QSIM_TICK_SECS=30`.

Both exits (`bank_1p3x` AND the hard stop) only fire when a sell quote comes back. So once
concurrency exceeds capacity it becomes a feedback loop: slow quoting -> slow closes -> more
open positions -> slower quoting. Measured on 2026-09-05:

- concurrency climbed 4 -> 21 monotonically over 6h, no plateau
- opens flat at ~4/hour (no volume surge), closes 6 against 23 opens
- five of those six closes landed in ONE hour, zero in the others
- 22,549 observations over a ~37.6h window against a 10/min cap = budget pinned at ~100%
- Jupiter `/order` 429s: 2 of 22,549 (0.01%) — the endpoint was never the constraint

Raised to **50/min** (0.83 rps). The cap is a CEILING, not a target: once the backlog drains,
steady state should settle near 15-20/min. Re-check the gap query after a day; if p95 is back
near 35s, drop it to 20.

**Data-quality consequence:** exits recorded while starved fire minutes after their threshold
was crossed, at drifted prices. Hard-stop losses in that period read WORSE than the strategy
would actually take, and 1.3x touches shorter than the effective cadence were missed entirely.
Degradation was progressive from 09-04 evening (when concurrency first passed ~3.5), going
critical around 09-05 14:00 UTC. Consider restarting the clean window after the fix lands.

### Stale-decision labelling (the durable fix, shipped 2026-09-05)

Root cause of the starvation incident was NOT the budget — it was allocation. Three flat
positions took 89.4% of the day's quotes (KEYCAT alone 40%: 1,162 quotes over 10.7h sitting at
0.996x), while 19 positions held >20min got 21 quotes BETWEEN them. Cadence is uniform
regardless of information value, and under scarcity the loop serves oldest-first and `break`s —
so the shortage landed entirely on the newest, highest-information positions.

The invariant now defended: **no exit decision is made on a stale quote — and when one is, the
row says so.**

- `qsim_positions.decision_gap_secs` — seconds the position went UNOBSERVED immediately before
  the quote that closed it (from the prior quote, or from entry if it was the first).
- Past `QSIM_STALE_DECISION_SECS` (default 6x tick = 180s), `exit_reason` is written as
  `stale_hard_stop` / `stale_bank_1p3x` / `stale_rug` instead of the bare reason.

The prefix is the load-bearing part: every report doing GROUP BY exit_reason separates these
automatically, including ad-hoc SQL that has never heard of `decision_gap_secs`. A column alone
can be ignored; a different label cannot. PnL columns are untouched — the data is preserved,
only the label is made honest.

`decision_gap_secs` only proves the EXIT was honest. Two more columns describe the whole life:

- `max_gap_secs` — worst hole ANYWHERE in the position (first gap measured from entry). A row
  can end on a fresh quote yet have been blind for 10 minutes mid-life, crossing 1.3x and
  falling back unseen. That trade is wrong even though its exit is clean, and the bias is
  one-way: missed banks, never missed losses.
- `obs_count` — real quotes over the whole life. Also the only thing that makes
  `peak_multiplier` meaningful, since the peak is ratcheted purely from observations (a
  1-observation row reports peak == exit, i.e. "never went up", which is usually false).

All three are computed by ONE function, `db.get_qsim_quote_quality()`, called by the monitor at
close and by the backfill for history — so live and backfilled labelling cannot drift apart.
Deliberately DB-derived, not in-memory: process state dies on `pm2 restart`, which would make a
well-quoted position look blind since entry and mislabel a good close as stale.

Retroactive: `qsim_backfill_decision_gap.py` applies the identical rule to already-collected
rows, so a starved window is salvaged rather than discarded. Dry-run by default; `--include-open`
also stamps still-open rows (quality columns only — an open row has no exit to relabel).

```bash
python3 qsim_backfill_decision_gap.py --since '2026-09-03 23:00 UTC'          # inspect
python3 qsim_backfill_decision_gap.py --since '2026-09-03 23:00 UTC' --apply  # write
```

Reverting is a prefix strip (the script prints the SQL). After backfilling, every honest-PnL
query should read `WHERE exit_reason NOT LIKE 'stale\_%'`.

Related gaps closed in the same pass (2026-09-05 audit):

- `qobs_count` in `qsim_quote_capture_replay.py` / `qsim_lane_scan.py` /
  `qsim_shadow_path_report.py` counted rate-limited and no-route rows, so a position with ZERO
  usable quotes read as "covered". It is used only as a `> 0` gate — including by
  `qsim_forward_referee`'s referee-grade test, which is how the starved window passed. Now
  `COUNT(*) FILTER (WHERE real_mult IS NOT NULL)`.
- `qsim_shadow_path_report.py` compared `q_reason == "hard_stop"`, which a `stale_hard_stop`
  silently fails. It now strips the prefix for comparison and returns a `qsim_starved_decision`
  verdict first, so a starved row is explained by starvation rather than by a fake path
  difference.

KNOWN, NOT YET CLOSED: the live path has no equivalent staleness record. `trading_positions`
rows carry no measure of how stale the quote driving the exit was. Live is currently OFF, and
its cadence is driven by the watchlist rather than a fixed tick, but this is the same class of
gap and should be closed before live restarts.

### #3 SHIPPED 2026-09-05 — adaptive cadence + most-overdue-first scheduling

Two defects caused the blackout, both in the monitor loop:

1. **Uniform cadence.** Every position got 30s regardless of whether a quote could change
   anything. Cadence now scales with distance to the nearest ACTIONABLE threshold (the hard
   stop below, the bank overlay above):

   | distance to nearest threshold | cadence |
   |---|---|
   | <= 5 pts (about to trigger) | 15s |
   | <= 15 pts | 30s |
   | <= 30 pts | 60s |
   | further / flat drifter | 90s |
   | never quoted yet | 15s |

   Clamped to half of `QSIM_STALE_DECISION_SECS`, so a deliberately stretched cadence can
   never itself trip a stale label — that label must only ever mean starvation. Tier bounds
   are rounded before comparison because `1.3 - 1.15` is `0.15000000000000002` in float, which
   would drop a position sitting exactly on an edge into the slower tier.

2. **Priority inversion.** The loop iterated oldest-entry-first and `break`ed when the budget
   ran out, so scarcity fell entirely on the tail — the newest positions, which are the ones
   near their thresholds. It now ranks by how far past its OWN target cadence each position is
   and serves the most overdue first. Never-quoted positions sort first. This strictly
   supersedes `e81d4d7`'s oldest-first zombie fix: a zombie is by definition the most overdue
   thing in the queue.

Post-exit probes now yield the shared budget entirely when any open position is
`QSIM_POST_EXIT_YIELD_OVERDUE` (2x) past its target — research must not be bought with the
quotes that decide real exits.

Simulated against the incident's exact conditions (21 concurrent, the real multiples):

| budget | scheduler | never quoted | worst gap | drifter's share |
|---|---|---|---|---|
| 10/min | old | **11 / 21** | **60.0 min** | 10% |
| 10/min | new | 0 / 21 | 5.2 min | 3% |
| 50/min | old | 0 / 21 | 0.6 min | 5% |
| 50/min | new | 0 / 21 | 1.1 min | 2% |

At the starved budget the old loop blacked out half the book; the new one keeps everyone
observed and degrades visibly (and any close still decided across a >180s gap gets labelled).
At the current 50/min both are healthy, but the new one reallocates: the flat drifter drops
from 5% to 2% of spend while positions near a threshold get 15s instead of 30s. New worst gap
is 1.1 min rather than 0.6 because far positions sit at 90s deliberately — still well inside
the 180s stale bound by construction.

STILL TO DO — #4, admission control: refuse to open a position the monitor cannot service at
target cadence (record `skipped_budget`), mirroring `MAX_OPEN_LIVE_POSITIONS`. That is the only
fix that holds regardless of volume, and it closes the qsim/live position-cap divergence at the
same time. Once #3 has a day of data, the cap can likely come back down to 20.

### qsim has no position cap; live has 5

`MAX_OPEN_LIVE_POSITIONS=5`, but qsim opens every lane call (only per-mint dedup, `qsim.py`).
At 21 concurrent, qsim was measuring "hold 21 simultaneously" while live would have logged
`max open positions reached` and skipped everything past the 5th — so those rows have no live
counterpart. Same family as the quiet-hours divergence: a live-only gate qsim doesn't mirror.

Unresolved. Three options: cap qsim at 5 (live-aligned, bounds the budget permanently at
~13/min, loses samples); leave it uncapped (discovery breadth, unbounded budget need); or
record open-position rank at entry so the referee can filter to the live-aligned subset at
analysis time (keeps both — recommended).

### Post-exit replay inflation

If a strategy only looks great with `--include-post-exit`, it is not automatically tradable. It means the coin moved after qsim exited. That is useful for designing better hold logic, but it is not proof that current live would capture it.

### Hard-stop fallback confusion

Use:

```bash
--fallback hard_stop --fallback-hard-stop-pct 0.20
```

Do not use `--fallback hard_stop_30`; that is not a valid option. The generic `hard_stop` fallback plus `--fallback-hard-stop-pct` is the correct interface.

### Win rate confusion

The replay now distinguishes concepts better, but always check column definitions:

- “hit rate” can mean trades that touched a threshold.
- “win rate” should mean final simulated result is net positive.
- These are not the same.

### Friday data

Friday has been weak in both shadow and qsim/live-forward behavior. Current live code excludes Friday for SWT low_score. qsim still tracks Friday so it can prove/disprove the cut over time.

### Shadow disagreement

If qsim and shadow differ wildly:

- Check `entry_ratio = qsim_entry / shadow_entry`.
- Check qsim `qmax` vs shadow peak.
- If qsim quote max never confirms shadow peak, shadow likely credited an unsellable/feed-only move.
- If qsim entry is much higher than shadow entry within sub-second timing, inspect shadow price source metadata.

## Current Interpretation

As of the latest pasted checks:

- The bot is no longer obviously broken in the same way it was before.
- Friday looked bad across systems, so it likely was a bad tape/day, not purely code failure.
- SWT is the stronger candidate than SHS.
- SHS is still worth observing but should not be promoted live without clean qsim proof.
- The simplest viable strategy under test is `bank_1p3x` with a tight hard stop.
- The biggest future upside is a partial-bank / runner-hold approach, but only after current simple banking has clean forward evidence.

## Recommended Next Steps

1. Keep qsim running on the two lanes through at least 2-3 clean non-Friday days.
2. Use `SINCE='2026-09-03 23:00:00 UTC'` or a later clean restart timestamp for all forward checks.
3. Compare SWT and SHS separately; do not combine lanes when deciding live readiness.
4. If SWT `bank_1p3x` with 20% hard stop stays positive, consider live at tiny size only.
5. Do not enable SHS live until it independently proves positive on qsim.
6. Use post-exit reports only to design the next hybrid/partial-bank strategy.
7. Before going live, verify logs show the intended strategy, hard stop, entry gates, and lane list.

## Pre-Live Checklist

Run this before enabling live:

```bash
pm2 logs sol-listener --nostream --lines 120 \
  | grep -iE "exit overlay|hard_stop override|max entry exec|entry roundtrip|LIVE_TRADING_ENABLED|lane_policy"

pm2 logs sol-qsim --nostream --lines 120 \
  | grep -iE "monitor started|hard_stop override|exit overlay|max entry exec|entry roundtrip|429|backoff"
```

Two `.env` keys that silently disable live if wrong — check them FIRST:

- `LIVE_BLOCKED_CHANNELS=` **must be present and empty.** If the key is absent the code default
  is `"solwhaletrending"` (`live_trader.py`), which blocks the only lane in `LIVE_LANES`. Live
  logs "channel ... is blocked" and takes zero trades.
- `LIVE_EXIT_USE_QUOTE=true`. With it off, `live_exit_basis` returns the FEED triple, so
  `bank_1p3x` and the 20% stop fire off the laggy feed mcap instead of executable quotes — a
  different ruler from qsim, and the qsim-vs-live comparison means nothing.

Expected live intent:

- `LIVE_LANE_STRATEGY=LIVE`
- `LIVE_EXIT_OVERLAY_STRATEGY=bank_1p3x`
- `LIVE_HARD_STOP_PCT=0.20`
- `LIVE_MAX_ENTRY_EXEC_RATIO=1.5`
- `LIVE_ENTRY_ROUNDTRIP_MIN_MULT=0.65`
- live lane should be SWT low_score only, no Friday.

## Emotional / Product Context

The user has been building this since March and is understandably frustrated because multiple previous simulations looked profitable but failed live. The current priority is not flashy optimization; it is truth, clean windows, and not getting fooled again.

The core working principle:

> Prefer smaller real executable wins over big fantasy paper numbers.

