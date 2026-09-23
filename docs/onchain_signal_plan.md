# Plan: pointing the measurement rig at on-chain signals

Written 2026-09-23, after six months of Telegram-caller signals measured honestly
and found to have no edge (-14.8%/SOL over 3,766 trades, tight CIs).

## What this is and is not

This is NOT a plan to make the current strategy work. That question is closed:
every entry filter tested is null, every exit variant is marginal, the tail is
insufficient even with perfect foresight, and the one positive result
(clean-deployer filter) was an attribution artifact. See
`memory/clean_deployer_filter.md`.

This is a plan to reuse the one thing worth keeping — the measurement rig — on a
different class of signal. The rig is: quote-priced entries and exits (qsim),
post-exit probing, permutation tests with family-wise control, bootstrap CIs,
look-ahead controls, and a habit of validating the input before the statistics.

## The honest prior

Low. The base rate of freshly launched Solana meme coins is brutal, and every
signal that fires at the 5-8 minute mark inherits it. Two of the three ideas
below are cheap enough that a low prior is still worth paying; the third is not,
and is listed mainly to be ruled out.

Be willing to stop after Phase 0.

---

## Phase 0 — order-flow features (days, zero build)

**Why first:** it costs nothing. `ws_market_observations` already stores
order-flow snapshots (`net_pressure`, `buy_vol_sol`, `unique_buyers`, `n_buys`),
and `qsim_entry_filter_sweep.py` already does family-wise threshold sweeps. The
17 features swept there were static token metadata; order flow is behavioural and
has never been tested against the honest book.

**Do:** extend `qsim_entry_filter_sweep.py` to join the earliest order-flow
snapshot within 2 minutes of entry (the join already exists in
`feature_edge.py`), and sweep with the same permutation test.

**Kill criterion:** family-wise p > 0.05, or a surviving filter whose retained
population is still worse than -5%/SOL. Then flow features are null like the
static ones and Phase 0 ends the project.

**Trap to avoid:** ws_* features measured AFTER entry leak the outcome. Only use
snapshots strictly at or before entry time. This killed the earlier ML attempt
(`memory/ml_entry_filter_dead_end.md`) and will silently produce a beautiful
result if repeated.

---

## Phase 1 — post-graduation entries (1-2 weeks build)

**The structural argument, and why it differs from everything tested:** every
signal tried so far fires 5-8 minutes after launch, when nothing has been
demonstrated. A pump.fun coin that completes its bonding curve has raised ~$69k
of real buys and migrated to Raydium. That is a FACT about demand, not a
prediction — and most rugs happen before graduation, so the population is
structurally different rather than merely filtered.

**Build:** a listener for the pump.fun migration instruction (Helius webhook or
websocket on the program), writing graduation events as a new signal source.
Route to qsim as its own lane. No money.

**Measure:** 30 days in qsim, honest quote pricing, the same rig.

**Kill criterion, set BEFORE building:** the graduated population must reach
-5%/SOL or better on the qsim book. Not profitable — just materially better than
the -14.8% the caller flow produces. If it lands in the same band, the lifecycle
stage is not the variable and the thesis is wrong.

**Known counter-argument:** graduation is public and heavily traded; price
frequently dumps immediately after migration. If the measured result is a sharp
drop at t+0, the entry needs to be delayed or the idea is dead. Do not fix this
by hunting for an entry offset that happens to work — that is curve-fitting on
one sample.

**Volume estimate:** roughly 1-2% of launches graduate. At current launch rates
that is plausibly 20-60/day, comparable to current flow.

---

## Phase 2 — NOT recommended: new-pool sniping

Detecting liquidity adds to be first is a latency arms race against funded bots
with co-located infrastructure. Listed only so it is explicitly ruled out rather
than rediscovered.

---

## What must be true to continue past Phase 1

Capacity, measured not assumed. Every qsim position is 0.05 SOL and p25 round
trip is already 6.1%. `qsim_size_impact.py` measures the curve. Even a real edge
caps out around 0.1-0.2 SOL/day if 0.25 SOL is the practical position limit, and
that ceiling should be known before any further build.

## Method rules carried forward

1. Validate the INPUT before trusting the statistics. Five attacks on the
   deployer filter all tested the maths while the join was wrong. A CI cannot
   detect a broken join.
2. Hold a result that KILLS an idea to the same bar as one that supports it.
   The deployer retraction was initially accepted after one round with no
   validation, which is the same error in the other direction.
3. Any "improvement" that reduces trade count must be checked against %/SOL.
   Confirmation entry, cell selection and stop tightening all improved the book
   by trading less, and the optimum of each was zero exposure.
4. Pre-register the kill criterion before building, not after seeing the result.
