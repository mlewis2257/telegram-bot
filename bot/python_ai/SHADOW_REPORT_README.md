# Shadow Report — Command Reference

`shadow_report.py` reads the `shadow_positions` table (every signal is shadow-traded
with **real entry prices and real-exit replays**) and breaks PnL down by lane, exit
variant, day, weekday, hour, or week. Phantom-price rows are excluded by default.

Run from `bot/python_ai/`:

```bash
python3 shadow_report.py [MODE] [FLAGS]
```

A **lane** = `(channel, vip_tier, skip_reason)`. A **variant** = the exit strategy
replayed: `early`, `ride`, `ride_vol`.

---

## Modes (pick one; default is the summary)

| Mode | What it shows |
|------|---------------|
| *(none)* | Summary: total PnL per lane×variant over the window, plus the phantom rows that were excluded. |
| `--by-day` | One row per lane×variant, one **column per UTC calendar day**. Spot "consistent vs one lucky day." |
| `--by-dow` | Per-day-of-week pivot + volume by weekday. Needs weeks of data. |
| `--by-hour` | Weekday × hour-of-day grid of total_sol (intra-day shape; UTC). |
| `--dow-weeks` | One lane's weekday PnL split **by calendar week** + a `+wk` consistency count. The persistence test. Use `--days 28+`. |

### Examples

```bash
# Summary, last 7 days
python3 shadow_report.py --days 7

# Per-day consistency, last 5 full UTC days
python3 shadow_report.py --by-day --days 5

# Weekday shape over 4 weeks
python3 shadow_report.py --by-dow --days 28

# Intra-day shape for one lane (see lane filters below)
python3 shadow_report.py --by-hour --days 14 --channel solhousesignal --skip-reason low_score

# Persistence test for one lane over 4 weeks
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal --vip-tier none --skip-reason low_score
```

---

## Global flags (apply to any mode)

| Flag | Default | Meaning |
|------|---------|---------|
| `--days N` | `0` (all time) | Window size. **`--by-day` is calendar-aligned** (N full UTC days ending today); other modes use a rolling `now()-N days` window. |
| `--min-trades N` | `10` | `--by-day`/`--by-dow`: hide lanes with fewer than N closed trades. |
| `--normalize` | off | Report PnL **per 1 SOL deployed** (`pnl_sol / sol_in`) — comparable across `SHADOW_SOL_IN` changes. |
| `--size X` | — | Project PnL as if every position were X SOL (e.g. `--size 2`). Implies `--normalize` at that size. |
| `--raw` | off | Include phantom-price rows (peak > 50x, or pnl outside [-100%, +5000%]). Off by default = phantom-excluded. |

---

## Lane filters (`--by-hour` and `--dow-weeks`)

| Flag | Matches | Notes |
|------|---------|-------|
| `--channel X` | `ch.handle ILIKE %X%` | **Substring, case-insensitive.** `solhousesignal` matches BOTH the free channel *and* `solhousesignal_vip`. Use `solhousesignal_vip` to isolate VIP, or add `--vip-tier none` to isolate the free channel. |
| `--vip-tier X` | `none` \| `safe` \| `gamble` \| `gamble_risk` | Free channels are tier `none`; VIP is `safe`/`gamble`/`gamble_risk`. |
| `--skip-reason X` | exact lane category | e.g. `low_score`, `none`, `vip_paused`, `quiet_hours`. `none` = traded/unskipped. |

> `--dow-weeks` **sums all three exit variants** for the filtered lane (there is no
> per-variant filter in that mode), so its magnitudes are the early+ride+ride_vol total.

---

## Reading the output

- **`Read each row L→R: a real edge is positive across MOST days, not one spike.`** A lane that's red 4 days and +10 on one is noise, not an edge.
- **`0.00` in `--by-day` = no closed positions in that cell**, NOT a zero-PnL result. (Real trades never sum to exactly 0.00.) A 0.00 means the lane wasn't traded/labeled that day — e.g. a channel went quiet, or routing re-labeled the call into a different lane.
- **`--by-day` columns:** interior days are complete and stable across runs; **today's column keeps growing** as positions close (inherent to a live day).
- **`--dow-weeks` `+wk` = weeks-that-weekday-was-positive / weeks-it-traded.** A real day-of-week edge is a HIGH, *repeating* ratio (need ~5–6 weeks), not one green week. `·` = the lane didn't trade that weekday that week.

---

## See ALL lanes in `--dow-weeks`

`--dow-weeks` reports **one lane at a time** (there is no single "all lanes" command),
so run it once per lane. Use `--days 28+` so each weekday has several weeks behind it.

### Run EVERY lane in one go (copy-paste)

```bash
for lane in \
  "solhousesignal|none|low_score" \
  "solhousesignal|none|none" \
  "solhousesignal|none|quiet_hours" \
  "solhousesignal|none|security_warning" \
  "solwhaletrending|none|none" \
  "solwhaletrending|none|low_score" \
  "solearlytrending|none|shadow_only" \
  "solhousesignal_vip|gamble|vip_low_score" \
  "solhousesignal_vip|gamble|vip_mcap_gate" \
  "solhousesignal_vip|gamble|vip_gamble_allowed_hours" \
  "solhousesignal_vip|gamble|vip_paused" \
  "solhousesignal_vip|gamble|mcap_too_low" \
  "solhousesignal_vip|gamble|none" \
  "solhousesignal_vip|safe|vip_low_score" \
  "solhousesignal_vip|safe|vip_safe_allowed_hours" \
  "solhousesignal_vip|safe|none" \
  "solhousesignal_vip|gamble_risk|vip_paused" ; do
  IFS='|' read -r ch tier skip <<< "$lane"
  echo "================ $ch | $tier | $skip ================"
  python3 shadow_report.py --dow-weeks --days 28 \
    --channel "$ch" --vip-tier "$tier" --skip-reason "$skip"
done
```

Run the individual commands below instead when you want just one lane.

### Free channel — `solhousesignal` (tier `none`)
```bash
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal --vip-tier none --skip-reason low_score
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal --vip-tier none --skip-reason none
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal --vip-tier none --skip-reason quiet_hours
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal --vip-tier none --skip-reason security_warning
```

### `solwhaletrending` (tier `none`)
```bash
python3 shadow_report.py --dow-weeks --days 28 --channel solwhaletrending --skip-reason none
python3 shadow_report.py --dow-weeks --days 28 --channel solwhaletrending --skip-reason low_score
```

### `solearlytrending` (tier `none`)
```bash
python3 shadow_report.py --dow-weeks --days 28 --channel solearlytrending --skip-reason shadow_only
```

### VIP — `solhousesignal_vip`, tier `gamble`
```bash
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal_vip --vip-tier gamble --skip-reason vip_low_score
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal_vip --vip-tier gamble --skip-reason vip_mcap_gate
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal_vip --vip-tier gamble --skip-reason vip_gamble_allowed_hours
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal_vip --vip-tier gamble --skip-reason vip_paused
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal_vip --vip-tier gamble --skip-reason mcap_too_low
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal_vip --vip-tier gamble --skip-reason none
```

### VIP — `solhousesignal_vip`, tier `safe`
```bash
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal_vip --vip-tier safe --skip-reason vip_low_score
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal_vip --vip-tier safe --skip-reason vip_safe_allowed_hours
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal_vip --vip-tier safe --skip-reason none
```

### VIP — `solhousesignal_vip`, tier `gamble_risk`
```bash
python3 shadow_report.py --dow-weeks --days 28 --channel solhousesignal_vip --vip-tier gamble_risk --skip-reason vip_paused
```

> **Note:** since the 06/25 routing change, most VIP calls land in `skip_reason=none`
> rather than `vip_low_score` / `vip_safe_allowed_hours` / `vip_gamble_allowed_hours` /
> `mcap_too_low`. Those older lanes will show little/no recent data — they were
> re-labeled, not removed. Keep them in the list for historical comparison.
