# Python AI Trading Bot

This directory contains the live Telegram listener, paper trading strategies,
reporting scripts, and the first pieces of a strategy research workflow.

## What lives here

- `telegram_client.py`
  - Main Telegram listener and router.
  - Parses calls, scores them, writes DB rows, and dispatches paper-trade opens.
- `paper_trader.py`
  - Strategy A paper trader.
  - Contains live entry gating, position sizing constants, and A exit logic.
- `paper_trader_b.py`
  - Strategy B paper trader.
  - Uses a different trade-management profile for comparison.
- `scorer.py`
  - Conviction scoring and alert classification.
- `parsers/`
  - Channel-specific message parsers.
- `db.py`
  - Postgres helpers for calls, positions, outcomes, and audit queries.
- `compare_report_b.py`
  - Strategy A vs B daily comparison report.
- `today_calls_audit.py`
  - Daily call audit with skip reasons, missed runners, and coverage checks.

## Strategy research layer

These files are the start of a cleaner experimentation workflow:

- `strategy_config.py`
  - Versioned strategy config objects.
  - Holds replayable strategy rules outside the live trader code path.
- `strategy_engine.py`
  - Deterministic entry-decision helpers shared by replay tooling.
- `replay_strategy.py`
  - Replays historical calls against a chosen strategy config and summarizes:
    - replay decisions
    - skip reasons
    - mismatches vs stored outcomes
    - hour and mcap-bucket behavior

The goal is to make strategy changes easier to compare without having to wait
for live paper trading alone.

## Current live shape

### Strategy A

- Free `solhousesignal` uses simplified filtering:
  - blocks hour `14`
  - blocks `30k-50k` at hours `8`, `11`, `18`
  - still applies min/max mcap and low-quality bundle/fake checks
- VIP `safe` and `gamble` use lane-specific allowed-hour logic
- VIP `gamble_risk` is paused
- Additional fallthrough protections stamp explicit skip reasons instead of
  leaving silent `none` cases where possible

### Strategy B

- Broader entry profile than A
- Different exit profile
- Useful as a comparison book, but not a perfect control for every question

## Replay usage

Run from the repo root with DB access available:

```bash
.venv/bin/python3 bot/python_ai/replay_strategy.py --strategy a --days 7
.venv/bin/python3 bot/python_ai/replay_strategy.py --strategy b --days 7
.venv/bin/python3 bot/python_ai/replay_strategy.py --strategy a --compare-against a_matrix --days 7 --limit 25
```

Useful options:

```bash
--date-from 2026-05-15
--date-to 2026-05-22
--limit 30
```

Replay output includes:

- decision counts
- stored skip counts
- replay by channel
- replay by hour
- replay by hour + mcap bucket
- mismatch pair breakdown
- detailed mismatch rows
- optional primary-vs-compare decision changes when `--compare-against` is used

Useful strategy keys:

- `a`
- `a_simplified`
- `a_matrix`
- `a_no_h14`
- `a_no_weak`
- `a_no_h14_no_weak`
- `b`

## Reporting workflow

Common scripts:

```bash
.venv/bin/python3 bot/python_ai/compare_report_b.py --today
.venv/bin/python3 bot/python_ai/today_calls_audit.py --tz America/Los_Angeles
```

These are still useful, but the direction of travel is:

1. version strategy rules
2. replay them on historical calls
3. use paper trading as confirmation instead of the only source of truth

## Next improvements

- Move more live Strategy A / B entry logic into `strategy_engine.py`
- Add richer replay comparisons between configs and strategy versions
- Add decision-event logging instead of relying on a single terminal
  `skip_reason`
- Automate recurring reports and trend summaries
