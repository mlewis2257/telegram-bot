# Handoff — Solana meme-coin trading bot

**Written 2026-10-05.** For whoever picks this up next. Assume no prior context.

---

## 0. The goal

> "At the end of the day I just want to figure out how to make money with this damn bot."

Live money has been running since **2026-09-29** at 0.05 SOL per trade. It is small on
purpose. The point of the live test is not the money — it is that **live is the only honest
ruler**, and it found four real bugs in its first 24 hours that no backtest could have
shown.

---

## 1. What is running right now

Four pm2 processes. **Which process does what matters a lot** and cost hours to work out:

| process | role | gotcha |
|---|---|---|
| `sol-listener` | telegram listener. Dispatches **live**, **qsim** and **shadow** opens from `telegram_client.py:392` as parallel `asyncio.create_task` | **`qsim_open()` runs HERE, not in `sol-qsim`.** All qsim open/skip log lines are in `sol-listener-out.log` |
| `sol-monitor` | paper + live exit sweep (polling) | |
| `sol-ws-monitor` (`sol-ws-m`) | per-swap exits off Helius tx stream | prices from `helius_tx` when it can, price API otherwise |
| `sol-qsim` | **qsim exit monitor ONLY** | its restarts cannot lose an open; it never opens anything |

Live and qsim fire from the **same dispatch at the same instant** and race for the same
Jupiter `/swap/v2/order` budget. Each call can burn 4 quotes between them. Live usually
wins; qsim loses the counterfactual. ~1,032 qsim opens have been lost to 429s.

---

## 2. Live config (verify against the startup banner, not `.env`)

```
lane            solwhaletrending / none / low_score   (LIVE_LANES in lane_policy.py)
days            Mon Tue Wed Thu Sun                   (Fri AND Sat cut)
mcap band       80k-120k                              (entry_filter)
security_flag   safe ONLY                             LIVE_ENTRY_BLOCK_SECURITY_FLAGS=warning,unknown
age exemption   off                                   LIVE_ENTRY_SAFE_MIN_AGE_MIN=0
out-of-band     blocked                               LIVE_ENTRY_OUT_BAND_ALLOW_FLAGS=
exit            bank_2x + 20% hard stop + profit floor
size            0.05 SOL, max 5 open
risk cap        MAX_TOTAL_LOSS_SOL=0.35, MAX_DAILY_LOSS_SOL=0
exit basis      LIVE_EXIT_USE_QUOTE=true, LIVE_EXIT_REQUIRE_QUOTE=true
quote tuning    LIVE_EXIT_QUOTE_RETRIES=2, LIVE_EXIT_QUOTE_RETRY_MS=400, LIVE_SELL_QUOTE_TTL=10
guards          LIVE_MAX_SANE_MULT=1000, LIVE_PROTECTIVE_DD=0.20
legacy, keep 0  LIVE_BLOCK_SECURITY_WARNING=false
qsim            QSIM_MAX_QUOTES_PER_MIN=30, QSIM_ENTRY_ROUNDTRIP_MIN_MULT=0, QSIM_TICK_SECS=30
shadow          RATCHET_SHADOW_ENABLED=false
```

**Verification commands** — `.env` is not proof, the startup log is:

```bash
grep -a -E "entry_filter|exit basis|exit overlay|hard_stop|security_flag=warning" \
  /root/.pm2/logs/sol-listener-out.log | tail -6
pm2 logs sol-qsim --lines 20 --nostream | grep -a "cap="
```

Expect `security_flag blocked: unknown,warning`, `exit overlay: bank_2x`,
`require_quote=ON`, `hard_stop in force: -20%`, `cap=30/min`, and **no**
`shadow fast lane` line.

**`hard_stop` is in that grep for a reason.** `LIVE_HARD_STOP_PCT` is an *override*; with
it unset the config default applies, which is **-35%**, not the -20% every analysis in this
project assumes. It used to fail silently — `live_trader` now always prints the stop in
force and says whether an override is set.

### Code defaults now match the settled config
As of 2026-10-05 the `entry_filter` defaults are `block warning,unknown`, age exemption
**off**, out-of-band **blocked**, `dev_sold` gate **off**. They previously defaulted to the
**losing** config (block safe / 15m exemption / out-of-band warning allowed / dev_sold on),
so three or four `.env` lines were the only thing holding correct behaviour — and a `.env`
append has silently dropped a line before. Config should never be one bad append away from
a known-losing state.

---

## 3. Results so far

```
Sep 29    6 trades   +0.0327   0 banks
Sep 30    8 trades   +0.0496   2 banks
Oct  1    9 trades   +0.0993   2 banks
Oct  2    ?                      (not separately measured)
Oct  3    ?                      (not separately measured)
Oct  4   19 trades   -0.1186   2 banks   <- my filter cost 0.22; see §6
Oct  5   12 trades   +0.0713   1 bank
```

Get the real total:

```sql
SELECT entry_time::date AS day, count(*) AS n,
       round(sum(pnl_sol)::numeric,4) AS pnl_sol,
       count(*) FILTER (WHERE exit_reason LIKE '%bank%') AS banks
FROM trading_positions
WHERE is_simulation = FALSE AND entry_time >= DATE '2026-09-29'
GROUP BY 1 ORDER BY 1;
```

**The P&L is bank-carried by design.** One 2x pays ~+0.05; everything else bleeds ~-0.005
to -0.015. Do NOT flag "one trade is the whole day" as a caveat — the user corrected that
explicitly. The right frame is the **break-even bank rate**: `bleed / (bank + bleed)`.

---

## 4. What is settled

### Entry (keep)
- **mcap_at_call 80–120k.** Replicated in both halves on three metrics, out-of-sample at
  **5.3σ** on rug rate (~3,300 trades), with a mechanism: liq/mcap falls as mcap rises
  while rug rate climbs. **The strongest finding in the project. Do not widen it on thin
  data.**
- **Saturday cut.** −14.12%/SOL on **149 trades**, in *both* mcap bands and *both* exit
  eras at near-identical magnitude (−12.46 / −13.18). Only 3 Saturdays, but the magnitude
  held when the strategy underneath changed.
- **`security_flag = safe` only.** Both rulers agree over 7d (live +25.64%/SOL on 20
  fills, qsim +13.69% on 63). **But safe's sign FLIPS across overlapping windows**
  (−8.44% → +13.69%). Best available read, not a proven edge.

### Exit (DONE — stop looking here)
`bank_2x` already fires on essentially every coin that reaches 2x (15.4% of in-band
positions reach it, observed bank rate 15.46%). Everything else tested and excluded:

| tested | result |
|---|---|
| hold past 2x | median terminal **0.06** in every speed bucket |
| time-to-2x as a signal | flat in-band (0.35σ) — no chart model warranted |
| floor + checkpoint at 3x/5x/10x | positive only on cells of n=11–21 |
| partial runners | negative at all 16 keep×target cells |
| **stop grace (suppress stop N min)** | **−4.69 SOL at EVERY window 2–60 min** |
| looser stops, rung ladders, trail tuning | dead before this session |

**Stop grace is the important one.** `med armed` = **0.06** against a 0.50 break-even —
the coins that don't recover **rug**, they don't drift. Ansemmas (stopped at −20%, then ran
8.4x) was **17 of 399**. The 20% stop is correct.

### Dead ends (do not re-open)
- cross-channel / same-mint dedupe — **refuted twice.** Repeat calls are *better*
  (−0.65%/SOL vs −5.19% for first calls)
- token age as the driver — not monotone; within the `<15m` bucket holding 80% of flow,
  ages are 3.5 / 4.9 / 3.4 min across safe/unknown/warning and the ordering still separates
- burst-opened positions being better (the 429-bias theory) — burst banks 9.3% vs 11.6%
  quiet, z=0.58

---

## 5. The open question, and it is the only one that matters

**Bank rate ~15.5% against a ~17.8–19.2% break-even. Two points.**

It is **not** closeable on the exit side (§4). It is entry selection or survival. And:

**qsim was a degraded instrument for most of this week.** Its `decision_gap_secs` ran
64–91s against a 30s target because `QSIM_MAX_QUOTES_PER_MIN=15` cannot sustain a 30s
cadence on more than ~7 open positions (`20 ÷ (15/60) = 80s` — matched the observed
median). 12% of exits were `stale_*`. Raised to 30/min on 2026-10-05 and the median
immediately dropped to **25s with zero stale**.

**The qsim quote cap is PER PROCESS — checked 2026-10-05, NOT a problem.** `_quote_window`
(`qsim.py:259`) is in-memory and qsim is imported by two processes (opens in
`sol-listener`, monitor in `sol-qsim`), so `cap=30` permits 30/min in *each* while live's
own quotes are counted nowhere, against a ~60/min Jupiter ceiling. The worry was that the
10-05 raise from 15 to 30 bought qsim's cadence with live's entries. Measured since the
last listener restart:

```
                 lost to 429   taken   coverage
before fixes          9           2       18%
after                 8          13       62%
```

Coverage went UP, so demand reduction more than paid for the raise — entry retries
(`fad317b`), `QSIM_ENTRY_ROUNDTRIP_MIN_MULT=0`, killing both `qsim_size_impact` processes,
and `LIVE_SELL_QUOTE_TTL=10`. **Re-check with the commands below if the cap is ever raised
again**, and if 429s rise the fix is demand reduction or separate per-process caps, not a
higher cap — buying qsim cadence with live entries is backwards when coverage binds.

```bash
tac /root/.pm2/logs/sol-listener-out.log | sed '/\[live\] exit basis:/q' \
  | grep -acE "skipped — pre-entry (buy|roundtrip sell) quote 429"
tac /root/.pm2/logs/sol-listener-out.log | sed '/\[live\] exit basis:/q' | grep -ac "BUY OK"
```

So these three need re-deriving on clean data, in roughly a week:

1. **The lane baseline.** Three cuts said ~−3%/SOL. Late exit decisions realize worse than
   real, so it is probably **pessimistic**.
2. **The 15.46% bank rate.** A sampler at 3x its interval misses 2x crossings. May be
   understated — and it is the number the whole strategy question hangs on.
3. **`safe` vs the others**, since safe's sign flipped across windows.

Watch cadence first, or the re-derivation is worthless:

```sql
SELECT round(percentile_cont(0.5) WITHIN GROUP
             (ORDER BY qp.decision_gap_secs)::numeric,0)              AS med_gap_s,
       count(*)                                                       AS n,
       count(*) FILTER (WHERE qp.exit_reason LIKE 'stale%')            AS stale
FROM qsim_positions qp
WHERE qp.status = 'closed' AND qp.entry_time >= now() - interval '24 hours';
```

Want median ≤40s and stale <5%.

---

## 6. Mistakes I made, so you don't repeat them

This is the most useful section. I was wrong a lot this week, in patterns.

**Four reversals on one column (`security_flag`) in one session.** block unknown (4.51σ)
→ unblock warning → block safe → trade safe only. Causes:
- **68% of the 4.51σ evidence came from a window with ZERO bank exits.** Everything before
  2026-09-23 ran no bank overlay, so it describes a different strategy. The tell was
  `banks = 0` across a whole window — in my own output.
- **Applied a between-group test to a trade/don't-trade decision.** "Is warning better than
  safe" (z=1.30) is not the question; "better than zero" is.
- **Live's wallet was saying the opposite the whole time** and I read past it because I was
  looking at qsim.

**The safe block cost 0.22 SOL and 4 banks in 24 hours.** Shipped on 1.83σ the night of
10-03; on 10-04 it blocked 10 in-band safe coins worth +0.2208 including 4 of the 5 banks
live missed. Live booked −0.1186 where it would have been +0.1022. **The user turned the
bot off over it**, then took the blame — it was not theirs. Trading unknown+warning was my
recommendation and the safe block was my evidence and my code.

**Both verification tools I built were wrong in the same direction as the conclusion.**
`--backtest` measured all lanes and all eras (reported −3.36% where live's slice was
+2.70%); and the era warning I added to catch exactly that only fired when a window was
*entirely* pre-overlay, so the straddling case — the common one — passed silently.

**Two hypotheses refuted within minutes of proposing them** (dedupe, burst bias). I was
concluding on the first plausible reading.

**Claimed a post-exit peak was a missed bank.** My query took `max(real_mult)` with no time
bound, so a print that landed *after* live exited looked actionable. Then I built a precise
`guard_peak` mechanism on it that never happened.

### Rules that follow
1. **When live's fills and qsim disagree on the same lane, LIVE WINS.** qsim is the
   better-powered ruler for *ranking* lanes; it is not the ruler for *which coins live
   should take*, because live's own gates make it a different sample.
2. **Nothing entry-side ships under ~2σ.** Machinery fixes are the opposite — ship those
   fast, they found four bugs in a day.
3. **Replication across windows before magnitude.**
4. **Check the tool before trusting the number.**
5. **Split in-life from post-exit before reading any peak comparison.**
6. **Verify the join, not the sigma** — CIs cannot detect a broken join.

---

## 7. Bugs fixed this session (all deployed)

| commit | what |
|---|---|
| `4a54aea` | `decimals=0` made `entry_price_fill` **1e6 low** → fake `bank_2x` sold a 0.94x bag 8s after entry. The narrow `exit_reason` CHECK constraint is the ONLY reason it surfaced |
| `589390f` | feed exit triple mixed rulers (feed numerator / fill denominator) → **4 of 5 positions opened BELOW their own hard stop**; 2 false stops |
| `8aa3466` | exits decide off `sol_out/sol_in` only, qsim-style |
| `35e11ea` | no quote → profit side pauses, **collapse guard** stays armed |
| `fad317b` | entry quotes retry a 429 instead of dropping the candidate |
| `e5ee0c3` | qsim's open path quoted **off-budget** — `cap=15` never meant 15 |
| `d61dcf8` | qsim's fast-lane banner printed whether or not the lane ran |
| `dc3c8bf` | seed `peak_multiplier=1.0` so straight-down losers stop recording NULL |
| `1dcc662` | one configurable security list, replacing two booleans in two files |
| `3a4a2ab` | `--backtest` defaults to LIVE_LANES + `--since` |
| `7fe8841` | era warning on the window's **start date** |
| `2d74cc0` | Saturday cut |
| `735c030` | every `_effective_fill_mcap` failure now says which of 5 paths it was |

New analysis scripts (all **pure DB replays**, zero Jupiter calls):
`qsim_time_to_2x.py`, `qsim_floor_checkpoint.py`, `qsim_stop_grace.py`

---

## 8. Hard-won gotchas

- **`grep -a` is required** on `sol-listener-out.log` — a null byte makes grep call it
  binary and **silently suppress matches**
- **`pm2 restart X --update-env` is mandatory**; `load_dotenv()` does not override an
  existing env var. `sol-qsim` ran `cap=50` for weeks while `.env` said 30 then 15, purely
  because nobody restarted it
- **Only the startup log proves what loaded**
- **`skip_reason` on `calls` is the LANE KEY** (`lane_policy`: `(skip_reason or "") ==
  "low_score"`). Never overwrite it. `set_call_skip_reason` only writes when it is NULL, so
  it was a **no-op for every call live could trade** — fixed by adding `live_skip_reason`
  (`ALTER TABLE calls ADD COLUMN IF NOT EXISTS live_skip_reason text;`). **Known flaw: the
  router also calls that function, so the column holds whoever wrote last. Live's values
  are `entry_filter`, `entry_quote_429`, `entry_roundtrip_429`, `security_warning`,
  `mcap_too_high`, `duplicate`, `entry_quote_no_route`**
- **`pnl_sol` and `pnl_pct` are GENERATED columns** — never set them in an UPDATE
- **`exit_reason` has a CHECK constraint.** It saved us once by refusing a fabricated
  `bank_2x`. Keep it explicit, not a regex
- **TablePlus cannot ALTER** (app user, no rights). Use
  `sudo -u postgres psql -d solana_signals` on the VPS — peer auth, no password
- **`peak_multiplier` semantics CHANGED at `589390f`** (now the real multiple). Do not
  compare across that commit
- **`shadow_report` is ~21x optimistic** on this lane and runs at 0.5 sizing — divide by 10
  for live comparability, and never treat it as a forecast
- **`.env` lives at `/root/telegram-bot/bot/python_ai/.env`**, Mac copy is gitignored.
  **Never paste or commit its secrets**
- Server is **UTC**. `LANE_GATE_TZ=UTC`, so Saturday closes Friday 5pm Pacific

---

## 9. Loose ends

- **owlin, call_id 268428** — `status='closing'` since **2026-08-30**, `sol_out` NULL, no
  sell signature. Needs a **Solscan balance check on its mint before any DB edit**. If the
  bag is still held that is 0.05 SOL of unmonitored exposure; writing `closed` would hide
  it from the monitor
- **397 `entry mcap calc failed`** qsim opens — now attributable thanks to `735c030`. Run:
  `grep -ao "\[live\] effective mcap[^0-9]*" /root/.pm2/logs/sol-listener-out.log | sort | uniq -c | sort -rn`
- **`live_skip_reason` prefix fix** — offered, not done (see §8)
- **Watch, do not trade:** out-of-band `warning` (+33%/SOL but **11 trades, 3 banks**) and
  `safe` aged ≥15m (+3.97%, **n=21**). Both would be answerable in 2–3 weeks
- **Friday is cut and the data says it is NEUTRAL** (−1.82% pre-bank, +0.12% bank era, 93
  trades). That cut was made on pre-bank data — it is the stale one. Restoring it is ~1/6th
  more flow at ~zero PnL cost, on a lane where **coverage is the binding constraint**

---

## 10. Memory files

Persistent notes live in
`~/.claude/projects/-Users-sw33tlew5th-Documents-telegram-bot/memory/`, indexed by
`MEMORY.md`. Most relevant here:

| file | why |
|---|---|
| `security_flag_trade_safe_only.md` | the four-reversal story + the live-wins rule |
| `stop_grace_closed_rugs_are_fast.md` | exits are DONE; rugs are fast |
| `live_exit_triple_mixed_rulers.md` | the mixed-ruler bug + `realized > peak` invariant |
| `fill_anchor_decimals_zero.md` | `decimals=0`; why the CHECK constraint mattered |
| `live_surfaces_what_backtests_cannot.md` | ship machinery fast, gates slow |
| `saturday_is_the_one_real_weekday_cut.md` | the only replicated weekday result |
| `mcap_band_80_120k.md` | the 5.3σ band + live go-live details |
| `qsim_analysis_gotchas.md` | the four ways qsim reports lie |
| `qsim_is_not_reading_low.md` | **narrower than it reads** — covers cadence for *measuring* a series, NOT a stale *decision* |

---

## 10b. The clean-cadence week — what to actually do

**Window opens 2026-10-05** (cadence fix). Re-measure at ~**2026-10-12**.

**DO NOTHING to the entry gates.** The window only works if the config holds still;
change one and the comparison measures the change. Frozen config is §2.

**Daily, 20 seconds** — confirm the window is still clean:

```sql
SELECT round(percentile_cont(0.5) WITHIN GROUP
             (ORDER BY decision_gap_secs)::numeric,0) AS med_gap_s,
       count(*) AS n,
       count(*) FILTER (WHERE exit_reason LIKE 'stale%') AS stale
FROM qsim_positions
WHERE status='closed' AND entry_time >= now() - interval '24 hours';
```

Want `med_gap_s` <= 40 and `stale` near 0. If the median climbs back toward 80s, qsim is
starved again — `positions / (QSIM_MAX_QUOTES_PER_MIN/60) = seconds per position`, so more
open positions need a higher cap. **Any day that fails this is a day of dirty data**, and
the re-measurement should start from the day after it recovers.

**At the end of the week, one command:**

```bash
sudo -u postgres psql -d solana_signals -v start="'2026-10-05'" \
  -f /root/telegram-bot/database/clean_cadence_rederive.sql
```

That file re-derives all three in order, with the starved readings printed inline for
comparison and a cadence gate up front that voids the rest if it fails.

**What each outcome means:**

| result | action |
|---|---|
| `bank_rate_pct` > `breakeven_pct` | the lane pays; the 2-point gap was measurement. Consider sizing up |
| still short by ~2 points | real. Entry selection is the only lever left — and it needs better method, not more gates (§6) |
| lane baseline much less negative than -3% | every qsim-based conclusion this week was pessimistic by that margin |
| `safe` clearly first again | the safe-only block holds |
| `warning`/`unknown` lead | revert it: `LIVE_ENTRY_BLOCK_SECURITY_FLAGS=` |

If step 1's `n` is under ~100, **wait longer**. The point is a better-powered read, not a
faster one.

---

## 11. If you do one thing

Let it run and **re-derive the bank rate on clean cadence data in a week.** Everything
mechanical is fixed and verified; every exit lever is exhausted; the entry work needs
better method rather than more gates. Two points of bank rate is the whole difference
between a marginal lane and the user's 0.1–0.3 SOL/day target.

And when the user pushes back on a number — they have been right more often than I have.
