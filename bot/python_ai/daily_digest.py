"""
daily_digest.py — the nightly brain dashboard, sent to Telegram.

WHY (2026-11-10). Every night the same five checks were pasted by hand: the paired v5/v7 report,
v5's solwhaletrending BUYs with the frozen filters F1/F2, the at-bank brain and Grok looks, the
Float forward test and the API spend. This gathers all of it for one PACIFIC day and posts it
through the alert bot: a short text summary plus a self-contained HTML dashboard (a document you
open on the phone). Read-only: no API calls, no trading access, nothing exposed on the VPS.

Run once:  python3 daily_digest.py --day 2026-11-10 [--stdout] [--html out.html]
           (no --day = yesterday, Pacific; --stdout prints instead of sending)
Nightly:   pm2 start daily_digest.py --name daily-digest --interpreter <venv python> -- --loop
           (sends yesterday's digest at DIGEST_AT, default 00:10 Pacific)
Uses TELEGRAM_BOT_TOKEN / TELEGRAM_ALERT_CHAT_ID (the alert bot's settings).
"""

from __future__ import annotations

import html
import os
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).with_name(".env"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
import db  # noqa: E402

TZ = os.getenv("REPORT_TZ", "America/Los_Angeles")
CHANNELS = ["solwhaletrending", "solearlytrending"]
SHORT = {"solwhaletrending": "swt", "solearlytrending": "solearly"}
PAIRED_SINCE = "2026-11-08"     # first day both versions judged every coin
SWT_SINCE = "2026-11-05"        # v5 on solwhaletrending
FILTERS_SINCE = "2026-11-10"    # F1 / F2 frozen
BANK_SINCE = "2026-11-10"       # at-bank brain prompt v2
GROK_SINCE = "2026-11-09"       # Grok at-bank looks
PRICE_IN = float(os.getenv("TRADER_BRAIN_PRICE_IN", "1.0"))
PRICE_OUT = float(os.getenv("TRADER_BRAIN_PRICE_OUT", "5.0"))
DIGEST_AT = os.getenv("DIGEST_AT", "00:10")


# ── data ────────────────────────────────────────────────────────────────────────

def _q(sql: str, params: list) -> list[tuple]:
    db.safe_rollback()
    with db.get_conn().cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def _span(col: str, first_day: str, last_day: str) -> tuple[str, list]:
    """col within [first_day 00:00, last_day+1 00:00) in TZ."""
    return (f"{col} >= (%s::date)::timestamp AT TIME ZONE %s "
            f"AND {col} < ((%s::date) + 1)::timestamp AT TIME ZONE %s", [first_day, TZ, last_day, TZ])


def agg(rows: list) -> dict:
    """rows of (sol_in, sol_out, exit_reason) -> n / banks / rugs / pnl."""
    n = len(rows)
    if not n:
        return {"n": 0, "banks": 0, "rugs": 0, "pnl_pct": None, "pnl_sol": 0.0}
    s_in = sum(float(r[0]) for r in rows)
    pnl = sum(float(r[1] or 0) - float(r[0]) for r in rows)
    return {"n": n, "banks": sum("bank" in (r[2] or "") for r in rows),
            "rugs": sum(float(r[1] or 0) / float(r[0]) < 0.30 for r in rows),
            "pnl_pct": pnl / s_in * 100 if s_in else None, "pnl_sol": pnl}


_RUN = """(SELECT min(o.observed_at) FROM qsim_quote_observations o
           WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time AND o.real_mult >= 3),
          (SELECT min(o.observed_at) FROM qsim_quote_observations o
           WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time
             AND NOT coalesce(o.rate_limited, false)
             AND (o.real_mult <= 1.55 OR coalesce(o.no_route, false))),
          (SELECT max(o.real_mult) FROM qsim_quote_observations o
           WHERE o.call_id = qp.call_id AND o.observed_at > qp.exit_time)"""


def _ran(t3, tfloor) -> bool:
    return t3 is not None and (tfloor is None or t3 < tfloor)


def paired(day: str) -> dict:
    span, sp = _span("qp.entry_time", PAIRED_SINCE, day)
    rows = _q(f"""
        SELECT qp.channel_handle, (qp.entry_time AT TIME ZONE %s)::date, a.vs_typical, b.vs_typical,
               qp.sol_in, qp.sol_out, qp.exit_reason
        FROM qsim_positions qp
        JOIN trader_brain_entry a ON a.call_id = qp.call_id AND a.prompt_v = 5
        JOIN trader_brain_entry b ON b.call_id = qp.call_id AND b.prompt_v = 7
        WHERE qp.status = 'closed' AND qp.sol_in > 0 AND qp.channel_handle = ANY(%s)
          AND a.vs_typical IS NOT NULL AND b.vs_typical IS NOT NULL AND {span}""", [TZ, CHANNELS] + sp)
    d = date.fromisoformat(day)

    def cells(rs):
        t = lambda r: (r[4], r[5], r[6])
        return {"both BUY": agg([t(r) for r in rs if r[2] >= 6 and r[3] >= 6]),
                "v5 only": agg([t(r) for r in rs if r[2] >= 6 and r[3] < 6]),
                "v7 only": agg([t(r) for r in rs if r[3] >= 6 and r[2] < 6]),
                "both PASS": agg([t(r) for r in rs if r[2] < 6 and r[3] < 6]),
                "v5 BUY": agg([t(r) for r in rs if r[2] >= 6]),
                "v7 BUY": agg([t(r) for r in rs if r[3] >= 6]),
                "no brain": agg([t(r) for r in rs])}

    today = [r for r in rows if r[1] == d]
    by_ch = {ch: cells([r for r in today if r[0] == ch]) for ch in CHANNELS}
    by_ch["both"] = cells(today)
    tally = []
    for day_i in sorted({r[1] for r in rows}):
        c = cells([r for r in rows if r[1] == day_i])
        tally.append((day_i, c["v5 only"], c["v7 only"], winner(c["v5 only"], c["v7 only"])))
    return {"by_ch": by_ch, "tally": tally}


def winner(v5only: dict, v7only: dict) -> str:
    """The fixed rule: a version's own picks beat the other's by 10+ pts/SOL. No side = no call."""
    if not v5only["n"] or not v7only["n"]:
        return "none"
    gap = v7only["pnl_pct"] - v5only["pnl_pct"]
    return "v7" if gap >= 10 else "v5" if gap <= -10 else "none"


def swt(day: str) -> dict:
    span, sp = _span("qp.entry_time", SWT_SINCE, day)
    rows = _q(f"""
        SELECT (qp.entry_time AT TIME ZONE %s)::date, a.vs_typical >= 6, c.mcap_at_call,
               coalesce(f.n_holding, 0), qp.sol_in, qp.sol_out, qp.exit_reason
        FROM qsim_positions qp
        JOIN calls c ON c.id = qp.call_id
        JOIN trader_brain_entry a ON a.call_id = qp.call_id AND a.prompt_v = 5
        LEFT JOIN followed_at_call f ON f.call_id = qp.call_id
        WHERE qp.status = 'closed' AND qp.sol_in > 0 AND qp.channel_handle = 'solwhaletrending'
          AND a.vs_typical IS NOT NULL AND {span}""", [TZ] + sp)
    d, f0 = date.fromisoformat(day), date.fromisoformat(FILTERS_SINCE)
    t = lambda r: (r[4], r[5], r[6])
    f1 = lambda r: r[1] and r[2] is not None and float(r[2]) < 150_000
    f2 = lambda r: r[1] and r[3] >= 1
    today = [r for r in rows if r[0] == d]
    since_f = [r for r in rows if r[0] >= f0]
    return {"today": {"BUY": agg([t(r) for r in today if r[1]]), "pass": agg([t(r) for r in today if not r[1]]),
                      "F1": agg([t(r) for r in today if f1(r)]), "F2": agg([t(r) for r in today if f2(r)])},
            "cum": {"BUY": agg([t(r) for r in rows if r[1]]), "pass": agg([t(r) for r in rows if not r[1]]),
                    "F1": agg([t(r) for r in since_f if f1(r)]), "F2": agg([t(r) for r in since_f if f2(r)]),
                    "BUY since filters": agg([t(r) for r in since_f if r[1]])}}


def bank_v() -> int:
    """The brain's current at-bank prompt version (read from the brain, so it can't go stale)."""
    import trader_brain
    return trader_brain.BANK_V


def bank_brain(day: str) -> dict:
    span, sp = _span("qp.exit_time", BANK_SINCE, day)
    rows = _q(f"""
        SELECT b.p_3x, {_RUN}
        FROM trader_brain b JOIN qsim_positions qp ON qp.call_id = b.call_id
        WHERE b.phase = 'bank' AND b.prompt_v = %s AND {span}
          AND qp.exit_time <= now() - interval '2 hours'""", [bank_v()] + sp)
    groups = {"HOLD (p_3x 50+)": [r for r in rows if r[0] is not None and r[0] >= 50],
              "p_3x 30-49": [r for r in rows if r[0] is not None and 30 <= r[0] < 50],
              "p_3x under 30": [r for r in rows if r[0] is not None and r[0] < 30],
              "all": rows}
    return {k: _runs(v) for k, v in groups.items()}


def _runs(rows: list) -> dict:
    n = len(rows)
    return {"n": n, "ran": sum(_ran(r[-3], r[-2]) for r in rows) / n * 100 if n else None,
            "x5": sum(float(r[-1] or 0) >= 5 for r in rows) / n * 100 if n else None}


def grok_bank(day: str) -> dict:
    span, sp = _span("qp.exit_time", GROK_SINCE, day)
    rows = _q(f"""
        SELECT g.posts_30m, g.posts_30_120, g.accounts_1k, {_RUN}
        FROM grok_x g JOIN qsim_positions qp ON qp.call_id = g.call_id
        WHERE g.phase = 'bank' AND {span} AND qp.exit_time <= now() - interval '2 hours'""", sp)
    accel = lambda r: r[0] is not None and r[1] is not None and r[0] > r[1]
    return {"all": _runs(rows),
            "accelerating": _runs([r for r in rows if accel(r)]),
            "not accelerating": _runs([r for r in rows if not accel(r)]),
            "1k+ accounts": _runs([r for r in rows if (r[2] or 0) >= 1])}


def market(day: str) -> dict:
    span, sp = _span("qp.entry_time", day, day)
    rows = _q(f"""
        SELECT qp.channel_handle, right(t.mint_address, 4), qp.sol_in, qp.sol_out, qp.exit_reason
        FROM qsim_positions qp JOIN tokens t ON t.id = qp.token_id
        WHERE qp.status = 'closed' AND qp.sol_in > 0 AND qp.channel_handle = ANY(%s) AND {span}""",
              [CHANNELS] + sp)
    pad = lambda s: {"Loat": "Float", "pump": "pump"}.get(s, "other")
    out = {"channels": {ch: agg([r[2:] for r in rows if r[0] == ch]) for ch in CHANNELS}, "launchpads": {}}
    for ch in CHANNELS:
        for p in ("Float", "pump", "other"):
            out["launchpads"][(ch, p)] = agg([r[2:] for r in rows if r[0] == ch and pad(r[1]) == p])
    return out


def spend(day: str) -> dict:
    span, sp = _span("judged_at", day, day)
    tin, tout = _q(f"""SELECT coalesce(sum(in_tokens), 0), coalesce(sum(out_tokens), 0) FROM (
                         SELECT in_tokens, out_tokens FROM trader_brain WHERE {span}
                         UNION ALL SELECT in_tokens, out_tokens FROM trader_brain_entry WHERE {span}) x""",
                   sp + sp)[0]
    calls = _q(f"""SELECT (SELECT count(*) FROM trader_brain WHERE {span}),
                          (SELECT count(*) FROM trader_brain_entry WHERE {span})""", sp + sp)[0]
    xai = _q(f"SELECT coalesce(sum(usd_est), 0), count(*) FROM grok_x WHERE {span}", sp)[0]
    return {"anthropic": float(tin) / 1e6 * PRICE_IN + float(tout) / 1e6 * PRICE_OUT,
            "brain_calls": int(calls[0]) + int(calls[1]), "xai": float(xai[0]), "grok_calls": int(xai[1])}


SECTIONS = [("paired", paired), ("swt", swt), ("bank_brain", bank_brain), ("grok_bank", grok_bank),
            ("market", market), ("spend", spend)]


def collect(day: str) -> dict:
    """Every section on its own: one broken section reports its error instead of sinking the digest."""
    out = {"day": day}
    for name, fn in SECTIONS:
        try:
            out[name] = fn(day)
        except Exception as e:
            db.safe_rollback()
            out[name] = {"error": f"{type(e).__name__}: {str(e)[:160]}"}
    return out


# ── text ────────────────────────────────────────────────────────────────────────

def pct(a: dict | None) -> str:
    if not a or not a.get("n"):
        return "— (0)"
    return f"{a['pnl_pct']:+.0f}% ({a['n']})"


def rate(r: dict | None, key: str = "ran") -> str:
    if not r or not r.get("n"):
        return "— (0)"
    return f"{r[key]:.0f}% ({r['n']})"


def text(d: dict) -> str:
    day = date.fromisoformat(d["day"])
    L = [f"🧠 Brain daily — {day:%a %b %-d} (Pacific)", ""]
    p = d.get("paired", {})
    if "error" in p:
        L.append(f"v5 vs v7: error {p['error']}")
    else:
        L.append("v5 vs v7 — where they disagreed (P&L/SOL, n):")
        for ch in CHANNELS:
            c = p["by_ch"][ch]
            L.append(f"  {SHORT[ch]}: v5-only {pct(c['v5 only'])} · v7-only {pct(c['v7 only'])}"
                     f" → {winner(c['v5 only'], c['v7 only'])}")
        wins = defaultdict(int)
        for _d, _a, _b, w in p["tally"]:
            wins[w] += 1
        L.append(f"  Day winner: {winner(p['by_ch']['both']['v5 only'], p['by_ch']['both']['v7 only'])}"
                 f" | tally since {PAIRED_SINCE[5:]}: v7 {wins['v7']} · v5 {wins['v5']} · none {wins['none']}")
        for ch in CHANNELS:
            c = p["by_ch"][ch]
            L.append(f"  {SHORT[ch]} BUYs: v5 {pct(c['v5 BUY'])} · v7 {pct(c['v7 BUY'])} · "
                     f"both {pct(c['both BUY'])} · no brain {pct(c['no brain'])}")
    s = d.get("swt", {})
    L.append("")
    if "error" in s:
        L.append(f"swt v5: error {s['error']}")
    else:
        L.append(f"swt v5 today: BUY {pct(s['today']['BUY'])} vs pass {pct(s['today']['pass'])}"
                 f" · F1 {pct(s['today']['F1'])} · F2 {pct(s['today']['F2'])}")
        L.append(f"swt v5 since {SWT_SINCE[5:]}: BUY {pct(s['cum']['BUY'])} vs pass {pct(s['cum']['pass'])}")
        L.append(f"Filters since {FILTERS_SINCE[5:]}: plain BUY {pct(s['cum']['BUY since filters'])} · "
                 f"F1 {pct(s['cum']['F1'])} · F2 {pct(s['cum']['F2'])}")
    b, g = d.get("bank_brain", {}), d.get("grok_bank", {})
    L.append("")
    L.append("At the 2x bank — hit 3x before 1.55x (break-even ~45%):"
             if "error" not in b else f"At-bank brain: error {b['error']}")
    if "error" not in b:
        L.append(f"  brain v{bank_v()}: HOLD {rate(b['HOLD (p_3x 50+)'])} · under 30% {rate(b['p_3x under 30'])}"
                 f" · all {rate(b['all'])}")
    if "error" in g:
        L.append(f"  Grok: error {g['error']}")
    else:
        L.append(f"  Grok: all {rate(g['all'])} · accelerating {rate(g['accelerating'])}"
                 f" · 1k+ accounts {rate(g['1k+ accounts'])}")
    m = d.get("market", {})
    if "error" not in m:
        fl = [m["launchpads"][(ch, "Float")] for ch in CHANNELS]
        n_float = sum(a["n"] for a in fl)
        L.append("")
        L.append("Channels (no brain): " + " · ".join(f"{SHORT[ch]} {pct(m['channels'][ch])}" for ch in CHANNELS))
        L.append(f"Float coins today: {n_float}" + (" · " + " · ".join(
            f"{SHORT[ch]} {pct(m['launchpads'][(ch, 'Float')])}" for ch in CHANNELS) if n_float else ""))
    sp = d.get("spend", {})
    if "error" not in sp:
        L.append(f"Spend today: Anthropic ~${sp['anthropic']:.2f} ({sp['brain_calls']} calls) · "
                 f"xAI ~${sp['xai']:.2f} ({sp['grok_calls']} looks)")
    L.append("")
    L.append("Full dashboard: the attached HTML file.")
    return "\n".join(L)


# ── html ────────────────────────────────────────────────────────────────────────

CSS = """
:root{--bg:#f7f7f5;--card:#fff;--fg:#1d1d1b;--mut:#6b6b66;--line:#e4e4df;--pos:#13795b;--neg:#b42318;--acc:#3b5bdb}
@media (prefers-color-scheme:dark){:root{--bg:#141413;--card:#1e1e1c;--fg:#ececea;--mut:#9c9c96;--line:#2e2e2b;--pos:#4ade80;--neg:#f87171;--acc:#8ea2ff}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.45 -apple-system,system-ui,Segoe UI,Roboto,sans-serif}
main{max-width:860px;margin:0 auto;padding:16px}h1{font-size:22px;margin:4px 0 2px}h2{font-size:16px;margin:0 0 10px}
.sub{color:var(--mut);font-size:13px;margin-bottom:16px}.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:14px;margin:0 0 14px;overflow-x:auto}
table{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}th,td{text-align:right;padding:6px 8px;border-bottom:1px solid var(--line);white-space:nowrap}
th:first-child,td:first-child{text-align:left}th{color:var(--mut);font-weight:600;font-size:12px;text-transform:uppercase;letter-spacing:.03em}
.pos{color:var(--pos)}.neg{color:var(--neg)}.mut{color:var(--mut)}.tag{display:inline-block;padding:1px 8px;border-radius:999px;border:1px solid var(--line);font-size:12px}
.win{color:var(--acc);font-weight:700}.note{color:var(--mut);font-size:12px;margin-top:8px}.err{color:var(--neg)}
"""


def _e(s) -> str:
    return html.escape(str(s))


def _pnl(a: dict) -> str:
    if not a.get("n"):
        return '<td class="mut">—</td>'
    cls = "pos" if a["pnl_pct"] >= 0 else "neg"
    return f'<td class="{cls}">{a["pnl_pct"]:+.1f}%</td>'


def _row(label: str, a: dict) -> str:
    return (f"<tr><td>{_e(label)}</td><td>{a['n']}</td><td>{a['banks']}</td><td>{a['rugs']}</td>"
            f"{_pnl(a)}<td>{a['pnl_sol']:+.4f}</td></tr>")


_HEAD = "<tr><th></th><th>n</th><th>banks</th><th>rugs</th><th>P&amp;L/SOL</th><th>SOL</th></tr>"


def _rate_row(label: str, r: dict) -> str:
    if not r.get("n"):
        return f'<tr><td>{_e(label)}</td><td>0</td><td class="mut">—</td><td class="mut">—</td></tr>'
    cls = "pos" if r["ran"] > 45 else "neg" if r["ran"] < 40 else ""
    return (f'<tr><td>{_e(label)}</td><td>{r["n"]}</td><td class="{cls}">{r["ran"]:.0f}%</td>'
            f'<td>{r["x5"]:.0f}%</td></tr>')


def _err(sec: dict) -> str | None:
    return f'<p class="err">error: {_e(sec["error"])}</p>' if "error" in sec else None


def render_html(d: dict) -> str:
    day = date.fromisoformat(d["day"])
    out = [f"<!doctype html><html lang=en><head><meta charset=utf-8>"
           f"<meta name=viewport content='width=device-width,initial-scale=1'><title>Brain Daily {day:%b %-d}</title>"
           f"<style>{CSS}</style></head><body><main>",
           f"<h1>Brain daily — {day:%A, %B %-d}</h1>",
           f"<div class=sub>Pacific day · qsim real-quote trades · generated "
           f"{datetime.now(ZoneInfo(TZ)):%b %-d %H:%M} {_e(TZ.split('/')[-1].replace('_', ' '))}</div>"]

    p = d.get("paired", {})
    out.append("<div class=card><h2>v5 vs v7 — same coins</h2>")
    if _err(p):
        out.append(_err(p))
    else:
        for ch in CHANNELS + ["both"]:
            c = p["by_ch"][ch]
            w = winner(c["v5 only"], c["v7 only"])
            out.append(f"<p><b>{_e(SHORT.get(ch, 'both channels'))}</b> "
                       f"<span class=tag>day: <span class=win>{_e(w)}</span></span></p><table>{_HEAD}")
            for k in ("v5 only", "v7 only", "both BUY", "both PASS", "v5 BUY", "v7 BUY", "no brain"):
                out.append(_row(k, c[k]))
            out.append("</table>")
        out.append(f"<p><b>Running tally since {_e(PAIRED_SINCE)}</b> (rule: own picks beat the other's by 10+ "
                   f"pts/SOL)</p><table><tr><th>day</th><th>v5 only</th><th>v7 only</th><th>winner</th></tr>")
        for day_i, a, b, w in p["tally"]:
            out.append(f"<tr><td>{day_i:%a %b %-d}</td><td>{_e(pct(a))}</td><td>{_e(pct(b))}</td>"
                       f"<td class=win>{_e(w)}</td></tr>")
        out.append("</table>")
    out.append("</div>")

    s = d.get("swt", {})
    out.append("<div class=card><h2>solwhaletrending — v5 (the live channel)</h2>")
    if _err(s):
        out.append(_err(s))
    else:
        out.append(f"<table>{_HEAD}")
        for k, lab in (("BUY", "today: BUY"), ("pass", "today: pass"), ("F1", "today: F1 (BUY, call <150k)"),
                       ("F2", "today: F2 (BUY + followed wallet)")):
            out.append(_row(lab, s["today"][k]))
        for k, lab in (("BUY", f"since {SWT_SINCE}: BUY"), ("pass", f"since {SWT_SINCE}: pass"),
                       ("BUY since filters", f"since {FILTERS_SINCE}: plain BUY"),
                       ("F1", f"since {FILTERS_SINCE}: F1"), ("F2", f"since {FILTERS_SINCE}: F2")):
            out.append(_row(lab, s["cum"][k]))
        out.append("</table><div class=note>Live bar: BUYs positive after ~3% fees. F1/F2 earn a place only by "
                   "beating plain BUY on new days.</div>")
    out.append("</div>")

    b, g = d.get("bank_brain", {}), d.get("grok_bank", {})
    out.append("<div class=card><h2>At the 2x bank — did it hit 3x before 1.55x?</h2>"
               "<table><tr><th></th><th>n</th><th>3x first</th><th>ever 5x</th></tr>")
    if _err(b):
        out.append(f"<tr><td colspan=4>{_err(b)}</td></tr>")
    else:
        for k in ("HOLD (p_3x 50+)", "p_3x 30-49", "p_3x under 30", "all"):
            out.append(_rate_row(f"brain v{bank_v()}: {k}", b[k]))
    if _err(g):
        out.append(f"<tr><td colspan=4>{_err(g)}</td></tr>")
    else:
        for k in ("all", "accelerating", "not accelerating", "1k+ accounts"):
            out.append(_rate_row(f"Grok: {k}", g[k]))
    out.append("</table><div class=note>Graded 2 h+ after each bank, since the start of each test. Break-even "
               "for holding to 3x ≈ 45%; a group needs 30+ coins above it, twice.</div></div>")

    m = d.get("market", {})
    out.append("<div class=card><h2>The day's trades (no brain)</h2>")
    if _err(m):
        out.append(_err(m))
    else:
        out.append(f"<table>{_HEAD}")
        for ch in CHANNELS:
            out.append(_row(f"{SHORT[ch]} — all", m["channels"][ch]))
            for pad in ("Float", "pump", "other"):
                a = m["launchpads"][(ch, pad)]
                if a["n"]:
                    out.append(_row(f"   {pad}", a))
        out.append("</table>")
    out.append("</div>")

    sp = d.get("spend", {})
    out.append("<div class=card><h2>Spend</h2>")
    out.append(_err(sp) or (f"<p>Anthropic ~${sp['anthropic']:.2f} ({sp['brain_calls']} brain calls) · "
                            f"xAI ~${sp['xai']:.2f} ({sp['grok_calls']} Grok looks)</p>"
                            "<div class=note>Estimates from stored token counts; the consoles are the real number.</div>"))
    out.append("</div></main></body></html>")
    return "".join(out)


# ── send ────────────────────────────────────────────────────────────────────────

def send(msg: str, page: str, day: str) -> bool:
    token, chat = os.getenv("TELEGRAM_BOT_TOKEN"), os.getenv("TELEGRAM_ALERT_CHAT_ID")
    if not token or not chat:
        print("[digest] TELEGRAM_BOT_TOKEN / TELEGRAM_ALERT_CHAT_ID not set — not sent", flush=True)
        return False
    base = f"https://api.telegram.org/bot{token}"
    try:
        r1 = requests.post(f"{base}/sendMessage", json={"chat_id": chat, "text": msg[:4000]}, timeout=20)
        r2 = requests.post(f"{base}/sendDocument", data={"chat_id": chat, "caption": f"Brain dashboard {day}"},
                           files={"document": (f"brain-{day}.html", page.encode(), "text/html")}, timeout=30)
        ok = r1.status_code == 200 and r2.status_code == 200
        print(f"[digest] {day}: message http {r1.status_code}, dashboard http {r2.status_code}", flush=True)
        return ok
    except Exception as e:
        print(f"[digest] send failed: {type(e).__name__}", flush=True)   # never the URL (it has the token)
        return False


def yesterday() -> str:
    return (datetime.now(ZoneInfo(TZ)).date() - timedelta(days=1)).isoformat()


def run_once(day: str, to_stdout: bool = False, html_path: str | None = None) -> bool:
    d = collect(day)
    msg, page = text(d), render_html(d)
    if html_path:
        Path(html_path).write_text(page)
    if to_stdout:
        print(msg)
        return True
    return send(msg, page, day)


def loop() -> None:
    hh, mm = (int(x) for x in DIGEST_AT.split(":"))
    print(f"[digest] nightly brain dashboard at {DIGEST_AT} {TZ} for the day just ended", flush=True)
    sent_for = None
    while True:
        now = datetime.now(ZoneInfo(TZ))
        due_day = (now.date() - timedelta(days=1)).isoformat()
        if (now.hour, now.minute) >= (hh, mm) and sent_for != due_day:
            for attempt in range(3):
                try:
                    if run_once(due_day):
                        break
                except Exception as e:
                    db.safe_rollback()
                    print(f"[digest] run failed: {type(e).__name__}: {e}", flush=True)
                time.sleep(60)
            sent_for = due_day      # one try-set per night; a failure is in the log, not retried all day
        time.sleep(30)


if __name__ == "__main__":
    a = sys.argv
    if "--loop" in a:
        loop()
    else:
        i = a.index("--day") if "--day" in a else -1
        h = a.index("--html") if "--html" in a else -1
        run_once(a[i + 1] if i > 0 else yesterday(), "--stdout" in a, a[h + 1] if h > 0 else None)
