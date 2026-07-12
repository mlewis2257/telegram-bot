"""
lane_policy_review.py — weekly adaptability check for lane_policy.

Reads the SAME shadow_positions data the reports read and, for every lane/day
lane_policy CURRENTLY trades, tells you whether it still earns — KEEP, SLIDING,
or DEMOTE? — re-checks any RECENTLY CUT days (lane_policy.CUT_WATCH) at the KEEP
bar to catch a recovery, then surfaces EMERGING cells that now qualify but aren't
traded yet.

It changes NOTHING. It prints a recommendation; you decide and hand-edit
lane_policy. Run it weekly (or whenever you're second-guessing a lane) so decay
gets caught by a cold rule instead of a gut feeling — the point isn't to remove
things, it's to KNOW.

    python3 lane_policy_review.py            # last 4 ISO weeks
    python3 lane_policy_review.py --weeks 6

Verdicts use the same phantom-excluded, UTC-weekday buckets as
`shadow_report.py --dow-weeks`, and evaluate each lane on ITS assigned exit
variant (early / ride / ride_vol) — so the numbers tie out to the reports you
already read.

Thresholds (env-overridable — defaults are deliberately forgiving on demotion,
strict on promotion, because a day-ADD is higher-regret than a defensive skip):
    REVIEW_WEEKS            4     lookback in ISO weeks
    REVIEW_MIN_WEEKS        3     min weeks of samples before judging a cell
    REVIEW_MIN_N           10     min total trades in a cell before judging
    REVIEW_KEEP_RATIO      0.60   keep if >= this share of weeks were positive (and mean>0)
    REVIEW_PROMOTE_RATIO   0.75   higher bar to SUGGEST adding a new cell
    REVIEW_PROMOTE_MIN_MEAN 0.50  ...and mean weekly pnl_sol must clear this
"""

from __future__ import annotations

import argparse
import os
import sys

from psycopg2.extras import RealDictCursor

sys.path.insert(0, os.path.dirname(__file__))

import db
import lane_policy

# ── Phantom guard — identical to shadow_report so verdicts match the reports ──
MAX_SANE_PEAK = 50.0
MAX_SANE_PNL_PCT = 5000.0
MIN_SANE_PNL_PCT = -100.5

# ── Thresholds ────────────────────────────────────────────────────────────────
WEEKS            = int(os.getenv("REVIEW_WEEKS", "4"))
MIN_WEEKS        = int(os.getenv("REVIEW_MIN_WEEKS", "3"))
MIN_N            = int(os.getenv("REVIEW_MIN_N", "10"))
KEEP_RATIO       = float(os.getenv("REVIEW_KEEP_RATIO", "0.60"))
PROMOTE_RATIO    = float(os.getenv("REVIEW_PROMOTE_RATIO", "0.75"))
PROMOTE_MIN_MEAN = float(os.getenv("REVIEW_PROMOTE_MIN_MEAN", "0.50"))

_WD_TO_DOW = {"Mon": 1, "Tue": 2, "Wed": 3, "Thu": 4, "Fri": 5, "Sat": 6, "Sun": 7}
_DOW_TO_WD = {v: k for k, v in _WD_TO_DOW.items()}

# One row per (lane, exit variant, ISO week, weekday): summed pnl_sol + trade count.
QUERY = """
SELECT
  COALESCE(ch.handle, '?')                                     AS channel,
  COALESCE(sp.vip_tier, 'none')                                AS vip_tier,
  COALESCE(c.skip_reason, 'none')                              AS skip_reason,
  sp.exit_variant                                              AS variant,
  date_trunc('week', (sp.entry_time AT TIME ZONE 'UTC'))::date AS wk,
  EXTRACT(ISODOW FROM (sp.entry_time AT TIME ZONE 'UTC'))::int AS dow,
  round(sum(sp.pnl_sol), 3)                                    AS wk_sol,
  count(*)                                                     AS n
FROM shadow_positions sp
JOIN calls c ON c.id = sp.call_id
LEFT JOIN channels ch ON ch.id = c.channel_id
WHERE sp.status = 'closed'
  AND sp.entry_time >= now() - (%(days)s || ' days')::interval
  AND NOT (
      sp.peak_multiplier > %(max_peak)s
   OR sp.pnl_pct > %(max_pnl)s
   OR sp.pnl_pct < %(min_pnl)s
  )
GROUP BY 1, 2, 3, 4, 5, 6
"""


def _traded_cells() -> dict:
    """Expand LANE_POLICY into the set of (channel, tier, skip, variant, dow) cells
    actually traded, mapped to {cell: {'watch': bool}}. Un-gated lanes expand to every
    weekday except the global SKIP_WEEKDAYS; gated lanes only to their `days`."""
    all_days = [d for wd, d in _WD_TO_DOW.items() if wd not in lane_policy.SKIP_WEEKDAYS]
    cells: dict = {}
    for (ch, tier, skip), pol in lane_policy.LANE_POLICY.items():
        if not pol.get("trade"):
            continue
        default_variant = pol.get("exit")
        day_exits = pol.get("day_exits") or {}   # {weekday: variant} per-day exit overrides
        days = pol.get("days")
        # (dow_int, weekday_str) pairs for the lane's traded days.
        dows = ([(_WD_TO_DOW[d], d) for d in days] if days
                else [(dow, _DOW_TO_WD[dow]) for dow in all_days])
        for dow, wd in dows:
            variant = day_exits.get(wd, default_variant)  # Fri->ride, others->default
            cells[(ch, tier, skip, variant, dow)] = {"watch": bool(pol.get("watch"))}
    return cells


def _cut_watch_cells() -> dict:
    """lane_policy.CUT_WATCH -> {(channel, tier, skip, variant, dow): {'note': str}}.
    These are day-cells we deliberately removed from a lane's `days`; we keep evaluating
    them at the KEEP bar (not the stricter ADD bar) so a recovering cut day doesn't fall
    into a visibility blind spot between the TRADED and EMERGING sections."""
    cells: dict = {}
    for entry in getattr(lane_policy, "CUT_WATCH", []):
        ch, tier, skip, wd, variant = entry[:5]
        note = entry[5] if len(entry) > 5 else ""
        dow = _WD_TO_DOW.get(wd)
        if dow is None:
            continue
        cells[(ch, tier, skip, variant, dow)] = {"note": note}
    return cells


def _series(cell_weeks: dict) -> list:
    """cell_weeks {wk_date: (sol, n)} -> list of (sol, n) oldest→newest."""
    return [cell_weeks[w] for w in sorted(cell_weeks)]


def _classify(series: list) -> tuple:
    """Return (status, ratio, mean, n_weeks, total_n, recent) for a traded cell."""
    n_weeks = len(series)
    total_n = sum(s[1] for s in series)
    sols = [s[0] for s in series]
    if n_weeks < MIN_WEEKS or total_n < MIN_N:
        return ("THIN", 0.0, 0.0, n_weeks, total_n, sols[-2:])
    pos = sum(1 for s in sols if s > 0)
    ratio = pos / n_weeks
    mean = sum(sols) / n_weeks
    recent = sols[-2:]
    # `mean` is the MONEY signal (what compounds); `ratio` is the RELIABILITY signal.
    # A cell that loses on average is a real cut candidate; a cell that MAKES money but
    # only in a minority of weeks is merely noisy (+EV, hold) — never cut a +EV cell.
    if mean <= 0:
        return ("DEMOTE", ratio, mean, n_weeks, total_n, recent)
    if ratio >= KEEP_RATIO:
        if len(recent) == 2 and all(s < 0 for s in recent):
            return ("SLIDING", ratio, mean, n_weeks, total_n, recent)
        return ("KEEP", ratio, mean, n_weeks, total_n, recent)
    return ("MARGINAL", ratio, mean, n_weeks, total_n, recent)


def _fmt_cell(ch, tier, skip, variant, dow) -> str:
    lane = f"{ch[:16]:<16} {tier[:6]:<6} {skip[:13]:<13}"
    return f"{lane} {variant:<8} {_DOW_TO_WD[dow]}"


def _wkstr(series: list, k: int = 4) -> str:
    return "[" + ", ".join(f"{s[0]:+.2f}" for s in series[-k:]) + "]"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--weeks", type=int, default=WEEKS, help="lookback in ISO weeks")
    args = ap.parse_args()
    days = args.weeks * 7

    db.ensure_shadow_positions_table()
    conn = db.get_conn()
    db.safe_rollback()
    params = {"days": days, "max_peak": MAX_SANE_PEAK,
              "max_pnl": MAX_SANE_PNL_PCT, "min_pnl": MIN_SANE_PNL_PCT}
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(QUERY, params)
        rows = cur.fetchall()

    # Nest: cell -> {wk_date: (sol, n)}
    data: dict = {}
    for r in rows:
        cell = (r["channel"], r["vip_tier"], r["skip_reason"], r["variant"], int(r["dow"]))
        data.setdefault(cell, {})[r["wk"]] = (float(r["wk_sol"] or 0), int(r["n"]))

    traded = _traded_cells()

    print(f"\nLANE POLICY REVIEW — last {args.weeks} ISO weeks "
          f"(phantom-excluded, UTC weekdays; keep>= {KEEP_RATIO:.0%} +wk & mean>0)\n")

    # ── SECTION 1: currently-traded cells, health check ──
    print("═" * 78)
    print("  CURRENTLY TRADED — is each lane/day still earning on ITS exit variant?")
    print("═" * 78)
    buckets = {"DEMOTE": [], "SLIDING": [], "MARGINAL": [], "THIN": [], "KEEP": []}
    for cell, meta in sorted(traded.items()):
        series = _series(data.get(cell, {}))
        status, ratio, mean, n_weeks, total_n, recent = _classify(series)
        who = "B-only" if meta["watch"] else "A & B"
        if status == "THIN":
            detail = f"n_wk={n_weeks} n={total_n} — insufficient, hold"
        else:
            pos = int(round(ratio * n_weeks))
            detail = f"+wk {pos}/{n_weeks}  mean {mean:+.2f}  {_wkstr(series)}"
            if status == "SLIDING":
                detail += "  ← last 2 wks red, WATCH"
            elif status == "MARGINAL":
                detail += "  ← +EV but streaky, HOLD & watch"
            elif status == "DEMOTE":
                detail += "  ← losing on avg, CUT candidate"
        buckets[status].append(f"  [{status:<8}] {_fmt_cell(*cell)}  ({who})   {detail}")

    for status in ("DEMOTE", "SLIDING", "MARGINAL", "THIN", "KEEP"):
        for line in buckets[status]:
            print(line)
    if not any(buckets.values()):
        print("  (no traded cells found — is LANE_POLICY populated?)")

    # ── SECTION 2: recently-cut day-cells, recovery watch (KEEP bar, not ADD bar) ──
    cut = _cut_watch_cells()
    if cut:
        print("\n" + "═" * 78)
        print("  RECENTLY CUT — days we removed; re-checked at the KEEP bar for recovery")
        print("═" * 78)
        for cell, meta in sorted(cut.items()):
            series = _series(data.get(cell, {}))
            status, ratio, mean, n_weeks, total_n, recent = _classify(series)
            note = f"  — {meta['note']}" if meta.get("note") else ""
            if status == "THIN":
                tag, detail = "THIN", f"n_wk={n_weeks} n={total_n} — insufficient, still cut"
            else:
                pos = int(round(ratio * n_weeks))
                base = f"+wk {pos}/{n_weeks}  mean {mean:+.2f}  {_wkstr(series)}"
                if status == "KEEP":
                    tag, detail = "RECOVERED", base + "  ← clears KEEP bar, CONSIDER RE-ADDING"
                elif status == "MARGINAL":
                    tag, detail = "WARMING", base + "  ← +EV but streaky, keep watching"
                elif status == "SLIDING":
                    tag, detail = "STILL CUT", base + "  ← good ratio but last 2 red, stay cut"
                else:  # DEMOTE
                    tag, detail = "STILL CUT", base + "  ← losing on avg, stay cut"
            print(f"  [{tag:<9}] {_fmt_cell(*cell)}   {detail}{note}")

    # ── SECTION 3: emerging cells not currently traded ──
    print("\n" + "═" * 78)
    print(f"  EMERGING — cells that clear the ADD bar ({PROMOTE_RATIO:.0%} +wk, "
          f"mean>{PROMOTE_MIN_MEAN}) but aren't traded")
    print("═" * 78)
    candidates = []
    for cell, cell_weeks in data.items():
        if cell in traded or cell in cut:
            continue
        series = _series(cell_weeks)
        n_weeks = len(series)
        total_n = sum(s[1] for s in series)
        if n_weeks < MIN_WEEKS or total_n < MIN_N:
            continue
        sols = [s[0] for s in series]
        ratio = sum(1 for s in sols if s > 0) / n_weeks
        mean = sum(sols) / n_weeks
        if ratio >= PROMOTE_RATIO and mean >= PROMOTE_MIN_MEAN:
            ch, tier, skip, variant, dow = cell
            pol = lane_policy.LANE_POLICY.get((ch, tier, skip))
            note = ""
            if pol is not None and not pol.get("trade"):
                note = "  (in a SKIP lane — extra scrutiny)"
            elif pol is not None:
                note = "  (lane traded, different day/variant)"
            candidates.append((mean, ratio, n_weeks,
                               f"  [PROMOTE?] {_fmt_cell(*cell)}   +wk "
                               f"{int(round(ratio*n_weeks))}/{n_weeks}  mean {mean:+.2f}  "
                               f"{_wkstr(series)}{note}"))
    candidates.sort(reverse=True)
    if candidates:
        for _, _, _, line in candidates[:12]:
            print(line)
    else:
        print("  (nothing new clears the add bar — no action)")

    print("\n  Reminder: this only RECOMMENDS. Nothing trades differently until you edit")
    print("  lane_policy.py. Adds are higher-regret than skips — demote freely, add slowly.\n")
    db.close_conn()


if __name__ == "__main__":
    main()
