# Six months measuring a Solana meme-coin trading bot

## A negative result, and the four ways I nearly fooled myself

I spent six months building a bot that traded Solana meme coins from Telegram
caller signals. It doesn't work. Two independent signal operators, 3,500+ closed
trades priced off real executable quotes, both landing between **−14% and −18%
per SOL deployed**, with tight confidence intervals and no profitable day.

That's the result. This write-up is about how I got to a number I trust, because
for most of those six months I had a different number that was wrong.

---

## The paper book said +16.6%/SOL. It was lying.

The bot ran a paper-trading simulator that recorded entries and exits against a
price feed. One lane showed **+16.6%/SOL**. Live trading the same lane lost money.

The gap wasn't slippage or fees. It was the entry price.

Meme coins spike violently in the seconds after a call. The price feed lags that
spike, so it records an entry *below* what you'd actually fill at. Every
subsequent multiple is computed from a price you never paid. On one coin, the
feed recorded entry at $36k when the real fill was around $84k — a "+132%"
trade that broke even in reality.

The correction was to stop using the feed entirely:

- **Entries** priced off a real Jupiter buy quote for the exact position size
- **Exits** priced off a real sell quote for the exact token bag held
- Nothing executed — it's a simulator, but every price is one you could have transacted at

The measured round trip is **0.9762** — you lose 2.4% to spread the instant you
enter, before the coin does anything. A quarter of trades cost 6.1% round trip.
None of that exists in a feed-priced backtest.

When the same lanes were re-measured this way, +16.6%/SOL became −14%/SOL. The
"edge" was entirely an artifact of recording entries at prices that weren't
available.

**If you're backtesting crypto against a price API, your entry prices are
probably fiction.** That's the single most transferable thing here.

---

## What was tested, and what it did

Once the ruler was honest, everything got re-measured:

| Tested | Result |
|---|---|
| ML model predicting stop-outs | AUC 0.60 — coin flip |
| "Smart money" wallet identity | χ²/df = 1.052 across 333 wallets — null |
| Holder counts, bundle metrics, sniper metrics | null |
| 17 static entry features, every threshold, both directions | family-wise p = 0.16 |
| Order-flow features | data doesn't exist before entry |
| Channel × lane × day-of-week cells | family-wise p = 0.13–0.34 |
| ~150 exit policy variants | best marginally positive, none meaningful |
| Deployer track record | **looked strong, was an artifact** (below) |
| Buying the confirmation instead of the call | exposure reduction, not edge |
| Re-entering after exit | negative on every path once one coin is removed |

One thing did work: a **market-cap ceiling** at entry cut the rug rate from 19%
to 6.5% on one channel. Structural filters — "this coin is already too big for
the multiple you need" — beat predictive ones. It wasn't enough to reach
breakeven.

---

## Failure mode 1: selecting on the outcome

I wrote a flag called `--min-obs` that required a position to have at least five
price observations before including it in analysis. It reads like a data-quality
control. It is not.

**Coins that die fastest produce the fewest observations.** The filter dropped
112 of 220 trades carrying **97% of the total loss**, turning a −9.94%/SOL book
into −0.71%. I'd built an outcome filter and labelled it a sanity check.

The same trap reappeared later in a different tool, after I had already written
the lesson down.

> If a filter's criterion correlates with the outcome, it isn't cleaning data —
> it's choosing the answer.

---

## Failure mode 2: exposure reduction wearing a filter's clothes

Three separate "improvements" turned out to be the same thing:

| Change | Looked like | Actually was |
|---|---|---|
| Buy the 1.3x confirmation | −13.3% → −10.7%/SOL | took 18% of the trades |
| Trade only the best day/lane cell | best cell −4.7% | traded one day in seven |
| Tighten the stop | −7.5% → −5.7% | exited before anything happened |

The tell: **a losing book improves whenever you discard trades at random.** Every
one of these was optimised at *zero exposure* — the limiting case of each is "do
not trade," which returns the round-trip cost and beats all of them.

The check is to look at return per unit of capital deployed, not total PnL. If
`%/SOL` is unchanged and only the trade count fell, nothing improved.

---

## Failure mode 3: look-ahead, in four disguises

Four separate times, a result depended on information that didn't exist yet:

1. **Post-exit quotes fed to exit policies.** A policy that banks at 1.4x instead
   of 1.3x can only ever exit *earlier*. Letting it see prices after the position
   closed isn't a counterfactual, it's time travel. Caught by an impossible
   result: a replayed policy beat the live one by +10.68 when the live system
   already ran that exact policy.

2. **A rejected price used as a fallback.** I added a sanity cap to ignore absurd
   quotes, then used the last-seen price — including the one I'd just
   rejected — when coverage went stale. One position booked **+121 SOL** by
   itself.

3. **Fills on a spike that had already reverted.** A confirmation strategy filled
   at "the next observation," which after a spike is often the collapse. A
   microscopic entry basis makes every later quote a huge multiple. The tell:
   returns *rose* with the confirmation threshold (+382% at 2x, +203% at 1.5x,
   +0.8% at 1.2x) when a stricter filter should give fewer, better trades.

4. **Outcomes not yet known at decision time.** A deployer's rug history counted
   rugs that hadn't happened when the trade was placed.

Each produced a plausible, exciting number. The pattern that caught three of them
was an impossible gradient — a result too good, or moving the wrong direction as
a filter tightened.

---

## Failure mode 4: the one that survived every test

Late on, a filter appeared that looked real: only trade coins whose **deployer had
3+ prior tokens and none of them rugged**.

It survived five deliberate attacks:

- Look-ahead removed — held
- Launchpad addresses excluded — held
- Profit concentration checked — top 5 coins were 33% of gross wins, healthy
- Stability across the exclusion threshold — moved under 2 points
- Split by channel — held on one, correctly failed on the other

The numbers were the best of the project: rugs down 56%, the 2x rate **up 72%**,
the 5x rate up 167%, the 10x rate up 229%. Runners-to-rugs went from 0.94 to 3.65.
Confidence intervals disjoint from baseline.

It was an artifact.

Resolving a token's deployer meant finding the oldest transaction on the mint.
`getSignaturesForAddress` returns **newest first**, capped at 1,000 per page — so
taking the last row of one page gives the creation transaction *only if the coin
has fewer than 1,000 transactions total*. A called coin runs 9,000–20,000.

So busy coins were attributed to whichever trader happened to sit at position
1,000. **Busy means heavily traded, which means it pumped.** A trader recurring
across several such coins manufactured a shared "deployer with a clean history"
that was really a proxy for trade volume — which correlates with performance.

Paging properly to the real creation transaction, the effect collapsed to 2.8
points with overlapping intervals and a broken gradient.

> Every one of those five attacks tested the **statistics**. The **input** was
> wrong. A confidence interval cannot detect a broken join.

---

## The mistake I made about the mistake

When the corrected data killed the filter, I accepted it after one round — no
validation of the new attribution at all.

The person I was working with pushed back on instinct, with no specific
objection. That prompted a correctness test that didn't exist yet: *a token
cannot be created after it was first called*, so any resolved "creation"
transaction postdating the first call is provably wrong. The corrected method
passed, and independently agreed with token ages parsed from the alert text.

So the retraction was right. But I'd have accepted it either way, because I held
the negative result to a far lower standard than the positive one.

> Skepticism applied in one direction is just a different bias.

---

## What the ceiling looked like even if everything had worked

Worth stating, because it reframes the whole exercise.

- 0.99% of coins reach 20x at a 24h+ horizon; 0.33% reach 50x
- Perfect-foresight capture of **every** 10x+ coin: +4.6 SOL against a 6.27 SOL
  gap to breakeven — flawless tail capture still leaves you losing
- Half of all exits were stops realising −40% against a −20% nominal, because
  the coins gap rather than slide; polling faster doesn't help, the drop is
  invariant to the observation window
- Liquidity caps position size, so even a real edge topped out around 0.1–0.2
  SOL/day

**Breakeven required roughly 50% of trades to reach 1.3x. The book delivered
30%.** No exit parameter closes a 20-point gap in win rate.

---

## What I'd do differently

1. **Price off executable quotes from day one.** Everything before that was
   measuring a fiction. Months of work rested on it.
2. **Pre-register the kill criterion** before building the thing that tests it.
   It's much harder to argue with a number you wrote down first.
3. **Validate the input before trusting the statistics.** Permutation tests and
   bootstrap CIs are excellent and cannot see a broken join.
4. **Check whether an "improvement" just reduced exposure.** Return per unit of
   capital, not total PnL.
5. **Hold negative results to the same bar as positive ones.**

---

## The honest summary

Two independent operators, measured the same way, landed four points apart at
−14% and −18% per SOL. That convergence is more informative than either number
alone: different people, different selection, same outcome.

The bot worked correctly. The strategy didn't exist.

The thing worth keeping was never the strategy — it was a measurement setup
honest enough to say no, four times in one week, to results I wanted to believe.
