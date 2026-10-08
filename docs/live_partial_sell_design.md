# Design: bank most at 2x, trail a slice (live)

**Status:** proposal, nothing built. **Written:** 2026-10-08.
**Decision needed from you:** the six open questions in §8, then whether to build.

---

## 1. What this is for

Live sells the whole bag the moment a real sell quote is worth 2x. That is the right call
on most coins, which die within the hour. But some keep going, and live has already sold:

| coin (2026-10-08) | banked at | afterwards |
|---|---|---|
| TikTok (safe, in band, live traded it) | 2.04x | 12.7x 45 minutes later, never below 1.92x on the way |
| Pumpcord (unknown, 121K) | 2.25x | 5.58x 29 minutes later, then 0.04x |
| INU (safe, in band, live traded it) | 2.52x | 3.25x one minute later, then 0.08x |

The idea: at the 2x bank, sell most of the bag as now and keep a small slice, managed by
its own rules. The dead coins cost a little of the slice; the runners pay for them.

## 2. What the first three dense paths say

Replayed with `qsim_slice_trail.py` on the 15-second post-bank record, 20% slice, 0.05 SOL
position, against selling everything at the bank:

| trail width | Pumpcord slice | INU slice | TikTok slice | net, 3 coins | net without TikTok |
|---|---|---|---|---|---|
| 25% | 2.23x | 2.37x | 3.05x | +0.0085 SOL | -0.0017 |
| 30% | 1.79x | 2.08x | 3.19x | +0.0026 | -0.0090 |
| 35% | 1.79x | 2.08x | 12.73x (still held) | +0.0980 | -0.0090 |
| 45% | 2.77x | 1.72x | 12.73x (still held) | +0.1042 | -0.0028 |
| 60% | 2.07x | 1.10x | 12.73x (still held) | +0.0910 | -0.0160 |

What that does and does not show:

- **A tight trail is the worst of both.** At 30% the slice is shaken out of every coin
  early; it missed TikTok's run and still lost on the other two.
- **The whole gain is one coin.** Without TikTok every width is slightly negative. That is
  the expected shape of this policy — small losses, rare large wins — and it is also
  exactly what an overfit result looks like at n=3.
- **TikTok's 12.73x is unrealised.** It was the last look, still at its high.
- **This is three coins.** It justifies building the test, not the conclusion.

The number that decides whether this works is the base rate: how many banks out of a
hundred run like TikTok. Three coins cannot tell you that; two to three weeks of banks can.

## 3. Behaviour, as proposed

1. Bank overlay fires at 2x on the raw sell quote, as now.
2. Sell `1 - keep` of `tokens_held` (default keep = 20%). Record the partial.
3. The remaining slice becomes a **runner**. From here it is subject only to runner rules:
   - **trail**: sell when the quote falls `trail%` below the slice's running high
   - **tightening**: narrower trail once the high passes set levels (e.g. 35% above 5x)
   - **floor**: optional hard floor as a multiple of entry
   - **target**: optional sell-at level
   - **horizon**: sell after N hours regardless
   - **dead coin**: N consecutive no-route quotes → written off at zero
4. The normal hard stop, profit floor and bank no longer apply to a runner.

Everything is a setting, default **off**. With it off, live behaves exactly as today.

## 4. What has to change

| area | change |
|---|---|
| **Sell path** | `close_live_position` sells all of `tokens_held`. Needs a partial variant: sell N tokens, keep the position open. |
| **Database** | New columns on `trading_positions`: `partial_sol_out`, `partial_tokens_sold`, `partial_exit_time`, `partial_reason`, `runner_peak_mult`, `runner_high_at`. A new status `runner` (or a flag). A migration, and new `exit_reason` values (`runner_trail`, `runner_floor`, `runner_target`, `runner_horizon`, `runner_rug`) added to the CHECK constraint. |
| **P&L** | `pnl_sol` is generated as `sol_out - sol_in`. Final `sol_out` must be partial + runner proceeds, so the generated column stays right with no change. Until the runner closes, the banked SOL is realised but not in `pnl_sol`. |
| **Exit claim** | `claim_live_position_exit` flips `open → closing` so two monitors cannot both sell. The partial sell needs the same guard, then must return the row to `runner`, not `closed`. |
| **Exit decision** | `check_live_exits` gains a runner branch that runs before everything else and returns early, the way qsim's does. |
| **State** | The runner's peak must live in the database, not in memory. Two processes evaluate exits and either can restart. |
| **Alerts** | A partial-bank alert and a runner-closed alert. |

qsim already has a partial-bank-plus-runner mode (`QSIM_PARTIAL_BANK_ENABLED`). The live
build should mirror its rules and column names so the two can be compared directly.

## 5. Things that will bite if ignored

1. **Position slots.** `MAX_OPEN_LIVE_POSITIONS=5` counts every row that is `open` or
   `closing`. A runner that lives for hours would occupy a slot and block new entries, on a
   lane where coverage is the binding constraint. **Runners must not count toward the cap**
   (they get their own small cap instead, e.g. 3).

2. **Helius usage.** `sol-ws-monitor` fetches a transaction for swaps on every held coin,
   up to two a second per coin. A coin running to 12x has thousands of swaps an hour. One
   runner held for a few hours could cost more Helius credits than a normal day. **Runners
   should be managed by timed sell quotes only, with the per-swap feed unsubscribed.**

3. **Jupiter quota.** A trail is only as good as how often it looks. Runners need a fresh
   quote every 2–3 seconds, not the 10-second reuse live uses now. At 3 seconds that is 20
   quotes a minute per runner; three runners would be the whole key. **Cap runners at 2–3
   and give them their own cadence setting**, leaving ordinary positions as they are.

4. **Fees.** Two sells instead of one. At 0.05 SOL the slice is worth about 0.02 SOL at
   the bank, and a sell costs priority fee plus slippage. Expect a few percent of the slice
   lost to the extra transaction. This is why the slice cannot be much smaller than 20% at
   this size.

5. **A failed partial sell.** If the partial sell fails, the position must stay fully
   open under normal rules and retry — not be left half-flagged. If the partial succeeds
   but the database write fails, that is the existing "sold on-chain, record stuck" case
   and needs the same loud alert.

6. **Selling into a collapse.** Pumpcord went from 3.88x to 2.07x between two 15-second
   looks. The trail will fire late and fill lower than its line. The replay already books
   the breaching quote rather than the line; live will be somewhat worse again.

7. **Loss breaker and cooldown.** The banked SOL should count toward the breaker's running
   P&L as soon as it is realised, or a good day looks worse than it is until runners close.
   The 30-minute re-entry cooldown keys off the last exit for that mint; it should start at
   the partial, so a new call for the same coin does not open a second position beside the
   runner.

8. **The measurement window.** This changes exits, not entries, so it does not disturb the
   qsim re-derive. It does change live's own P&L per trade, so live results before and
   after it ships should be reported separately.

## 6. How I would roll it out

1. **Now → ~2 weeks:** collect dense bank paths (already running) and re-run the replay as
   they accumulate. No money involved.
2. **Build behind a flag, default off.** Migration first, then the partial sell, then the
   runner branch, each tested with the network faked, as with this week's fixes.
3. **Dry run in live:** flag on in *log-only* mode — live banks the whole bag as today, but
   logs "would have kept 20%, runner would have sold at X for reason Y". This checks the
   live decision logic against real quotes at live's cadence with nothing at risk.
4. **Live at current size**, one runner at a time, for a week. The slice is about 0.02 SOL.
   What this tests is the one thing no replay can: whether live's sells actually land near
   where the trail fired.
5. **Then** decide slice size, trail width and whether to raise the runner cap.

Steps 2 and 3 are a few days of work. Step 3 is the cheap, honest check before step 4.

## 7. What would make me stop

- After two to three weeks, the replay is negative or flat across widths once the single
  best coin is removed **and** TikTok-like runs turn out to be rarer than about 1 bank in 15.
- Live's runner sells land consistently far below where the replay booked them.
- Runners push Helius or Jupiter usage past what the keys can carry even with the caps.

## 8. Decisions I need from you

1. **Slice size.** 20% is the working number. Smaller loses too much to fees at 0.05 SOL;
   larger gives up more of the bank on the coins that die.
2. **Trail shape.** The three paths favour a wide trail (45%) tightening as the multiple
   grows. Do you want a hard floor as well (e.g. never below 1.5x), accepting it will
   sometimes sell a coin that dips and recovers?
3. **Horizon.** How long may a runner live — 6 hours, 24?
4. **Runner cap.** I suggest 2 at once to start.
5. **Which banks qualify.** All of them, or only when live's position is otherwise healthy?
   I suggest all: any selection rule here would be fitted to three coins.
6. **Log-only first, or straight to live at 0.05?** I recommend log-only for a few days.
