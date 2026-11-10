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
:root{--bg:#f5f4f0;--surface:#fff;--sunk:#f9f8f5;--fg:#191917;--mut:#77756e;--faint:#a8a59d;--line:#e7e5df;
--pos:#1d7556;--pos-bg:rgba(29,117,86,.09);--neg:#b0392f;--neg-bg:rgba(176,57,47,.09);
--acc:#4a4ed1;--acc-bg:rgba(74,78,209,.09);--shadow:0 1px 2px rgba(25,25,20,.04),0 6px 20px rgba(25,25,20,.04)}
@media (prefers-color-scheme:dark){:root{--bg:#0e0e0d;--surface:#171715;--sunk:#1c1c1a;--fg:#ecebe6;--mut:#9a978e;
--faint:#64625b;--line:#292825;--pos:#62cfa3;--pos-bg:rgba(98,207,163,.11);--neg:#ef8a80;--neg-bg:rgba(239,138,128,.11);
--acc:#a3a6ff;--acc-bg:rgba(163,166,255,.13);--shadow:none}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);-webkit-font-smoothing:antialiased;
font:15px/1.5 -apple-system,BlinkMacSystemFont,"SF Pro Text",Inter,system-ui,"Segoe UI",Roboto,sans-serif}
main{max-width:760px;margin:0 auto;padding:30px 16px 56px}
.num,td,.v{font-variant-numeric:tabular-nums}
.eyebrow{font-size:11px;font-weight:600;letter-spacing:.14em;text-transform:uppercase;color:var(--mut)}
h1{font-size:29px;line-height:1.12;letter-spacing:-.025em;font-weight:700;margin:8px 0 6px}
.meta{font-size:13px;color:var(--mut)}
.kpis{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:24px 0 18px}
@media (min-width:640px){.kpis{grid-template-columns:repeat(4,1fr)}}
.kpi{background:var(--surface);border:1px solid var(--line);border-radius:14px;padding:13px 14px;box-shadow:var(--shadow)}
.kpi .l{font-size:11px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--mut)}
.kpi .v{font-size:23px;font-weight:700;letter-spacing:-.02em;margin-top:6px;line-height:1.1}
.kpi .s{font-size:12px;color:var(--mut);margin-top:4px}
section{background:var(--surface);border:1px solid var(--line);border-radius:16px;padding:18px 18px 12px;
margin:0 0 14px;box-shadow:var(--shadow)}
.hd{display:flex;align-items:baseline;justify-content:space-between;gap:12px;flex-wrap:wrap}
h2{font-size:15.5px;font-weight:650;letter-spacing:-.01em;margin:0}
.lede{font-size:13px;color:var(--mut);margin:3px 0 14px}
.scroll{overflow-x:auto;margin:0 -4px;padding:0 4px}
table{border-collapse:collapse;width:100%}
.grid th{font-size:11px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:var(--mut);
text-align:right;padding:0 8px 10px;white-space:nowrap}
.grid th:first-child,.grid td:first-child{text-align:left;padding-left:0}
.grid td{text-align:right;padding:9px 8px;border-top:1px solid var(--line);vertical-align:top}
.grid td:first-child{font-size:14px;color:var(--fg);white-space:nowrap}
.grid tr.sep td{border-top:1px solid var(--faint)}
.cell .num{font-size:15px;font-weight:650;letter-spacing:-.01em}
.cell .n{font-size:11.5px;color:var(--faint);margin-top:1px;white-space:nowrap}
.pos{color:var(--pos)}.neg{color:var(--neg)}.mut{color:var(--faint)}.acc{color:var(--acc)}
.pill{display:inline-block;margin-top:5px;padding:2px 9px;border-radius:999px;font-size:11px;font-weight:650;
letter-spacing:.02em;text-transform:none;background:var(--sunk);color:var(--mut)}
.pill.v5,.pill.v7{background:var(--acc-bg);color:var(--acc)}
.chips{display:flex;flex-wrap:wrap;gap:8px;margin:14px 0 6px}
.chip{border:1px solid var(--line);background:var(--sunk);border-radius:11px;padding:8px 11px;min-width:104px}
.chip .d{font-size:11px;color:var(--mut);font-weight:600;letter-spacing:.04em;text-transform:uppercase}
.chip .w{font-size:14px;font-weight:700;margin-top:2px}
.chip .w.v5,.chip .w.v7{color:var(--acc)}.chip .w.none{color:var(--faint)}
.chip .x{font-size:11.5px;color:var(--faint);margin-top:1px}
.rows td{padding:9px 0;border-top:1px solid var(--line);vertical-align:middle}
.rows tr:first-child td{border-top:0}
.rows .lab{font-size:14px;padding-right:12px;white-space:nowrap}
.rows .lab .sub{display:block;font-size:11.5px;color:var(--faint)}
.rows .bar{width:42%;min-width:90px}
.rows .val{text-align:right;font-weight:650;white-space:nowrap;padding-left:12px;width:1%}
.rows .cnt{text-align:right;color:var(--faint);font-size:12px;white-space:nowrap;padding-left:10px;width:1%}
.rows tr.group td{padding-top:16px;font-size:11px;font-weight:600;letter-spacing:.1em;text-transform:uppercase;
color:var(--mut);border-top:0}
.dbar,.rbar{position:relative;height:8px;border-radius:99px;background:var(--sunk);overflow:hidden}
.dbar .mid{position:absolute;left:50%;top:-2px;bottom:-2px;width:1px;background:var(--faint);opacity:.7}
.dbar .f,.rbar .f{position:absolute;top:0;bottom:0;border-radius:99px}
.f.pos{background:var(--pos)}.f.neg{background:var(--neg)}.f.acc{background:var(--acc)}
.rbar .mark{position:absolute;top:-3px;bottom:-3px;width:2px;background:var(--fg);opacity:.55;left:45%}
.note{font-size:12px;color:var(--faint);margin:10px 0 4px;line-height:1.45}
.err{color:var(--neg);font-size:13px}
footer{font-size:12px;color:var(--faint);text-align:center;margin-top:22px;line-height:1.6}
"""


def _e(s) -> str:
    return html.escape(str(s))


def _cls(v: float | None) -> str:
    return "mut" if v is None else "pos" if v >= 0 else "neg"


def _fmt(v: float | None, plus: bool = True) -> str:
    return "—" if v is None else (f"{v:+.1f}%" if plus else f"{v:.0f}%")


def _cell(a: dict) -> str:
    if not a.get("n"):
        return '<td class=cell><div class="num mut">—</div><div class=n>0 trades</div></td>'
    extra = f" · {a['banks']} bank" + ("s" if a["banks"] != 1 else "")
    extra += f" · {a['rugs']} rug" + ("s" if a["rugs"] != 1 else "") if a["rugs"] else ""
    return (f'<td class=cell><div class="num {_cls(a["pnl_pct"])}">{_fmt(a["pnl_pct"])}</div>'
            f'<div class=n>{a["n"]} trade{"s" if a["n"] != 1 else ""}{extra}</div></td>')


def _dbar(v: float | None) -> str:
    """Diverging bar from 0, clipped at ±100%."""
    if v is None:
        return '<div class=dbar><span class=mid></span></div>'
    w = min(abs(v), 100) / 2
    side = f"left:50%;width:{w:.1f}%" if v >= 0 else f"right:50%;width:{w:.1f}%"
    return f'<div class=dbar><span class=mid></span><span class="f {_cls(v)}" style="{side}"></span></div>'


def _pnl_row(label: str, a: dict, sub: str = "") -> str:
    sub_html = f"<span class=sub>{_e(sub)}</span>" if sub else ""
    cnt = f"{a['n']} · {a['banks']}b" + (f" · {a['rugs']}r" if a["rugs"] else "") if a.get("n") else "0"
    return (f"<tr><td class=lab>{_e(label)}{sub_html}</td><td class=bar>{_dbar(a['pnl_pct'] if a.get('n') else None)}</td>"
            f"<td class='val {_cls(a['pnl_pct'] if a.get('n') else None)}'>{_fmt(a['pnl_pct'] if a.get('n') else None)}</td>"
            f"<td class=cnt>{cnt}</td></tr>")


def _rate_row(label: str, r: dict, sub: str = "") -> str:
    sub_html = f"<span class=sub>{_e(sub)}</span>" if sub else ""
    if not r.get("n"):
        return (f"<tr><td class=lab>{_e(label)}{sub_html}</td><td class=bar><div class=rbar><span class=mark></span>"
                f"</div></td><td class='val mut'>—</td><td class=cnt>0</td></tr>")
    cls = "pos" if r["ran"] > 45 else "neg" if r["ran"] < 40 else "acc"
    return (f"<tr><td class=lab>{_e(label)}{sub_html}</td><td class=bar><div class=rbar>"
            f"<span class='f {cls}' style='width:{min(r['ran'], 100):.1f}%'></span><span class=mark></span></div></td>"
            f"<td class='val {cls if cls != 'acc' else ''}'>{r['ran']:.0f}%</td>"
            f"<td class=cnt>{r['n']} · 5x {r['x5']:.0f}%</td></tr>")


def _group(title: str) -> str:
    return f"<tr class=group><td colspan=4>{_e(title)}</td></tr>"


def _err(sec: dict) -> str | None:
    return f'<p class="err">Couldn’t load: {_e(sec["error"])}</p>' if "error" in sec else None


def _kpi(label: str, value: str, sub: str, cls: str = "") -> str:
    return (f'<div class=kpi><div class=l>{_e(label)}</div><div class="v {cls}">{value}</div>'
            f'<div class=s>{sub}</div></div>')


def render_html(d: dict) -> str:
    day = date.fromisoformat(d["day"])
    p, s, b, g, m, sp = (d.get(k, {}) for k in ("paired", "swt", "bank_brain", "grok_bank", "market", "spend"))
    out = [f"<!doctype html><html lang=en><head><meta charset=utf-8>"
           f"<meta name=viewport content='width=device-width,initial-scale=1'>"
           f"<meta name=color-scheme content='light dark'><title>Brain Daily · {day:%b %-d}</title>"
           f"<style>{CSS}</style></head><body><main>",
           "<div class=eyebrow>Trader brain · daily</div>",
           f"<h1>{day:%A, %B %-d}</h1>",
           f"<div class=meta>Pacific day · qsim real-quote trades · generated "
           f"{datetime.now(ZoneInfo(TZ)):%b %-d, %-I:%M %p}</div>"]

    # headline strip
    kp = []
    if "error" not in p:
        both = p["by_ch"]["both"]
        w = winner(both["v5 only"], both["v7 only"])
        kp.append(_kpi("Day winner", {"none": "No call"}.get(w, w), f"solo picks: v5 {_e(pct(both['v5 only']))}"
                       f" · v7 {_e(pct(both['v7 only']))}", "acc" if w != "none" else "mut"))
        wins = defaultdict(int)
        for *_x, wv in p["tally"]:
            wins[wv] += 1
        kp.append(_kpi(f"Tally since {date.fromisoformat(PAIRED_SINCE):%b %-d}", f"v7 {wins['v7']} · v5 {wins['v5']}",
                       f"{wins['none']} day{'s' if wins['none'] != 1 else ''} without a winner"))
    if "error" not in s:
        t = s["today"]
        kp.append(_kpi("swt v5 BUYs", _fmt(t["BUY"]["pnl_pct"] if t["BUY"]["n"] else None),
                       f"{t['BUY']['n']} trades · passes {_e(pct(t['pass']))}",
                       _cls(t["BUY"]["pnl_pct"] if t["BUY"]["n"] else None)))
    if "error" not in sp:
        kp.append(_kpi("API spend", f"${sp['anthropic'] + sp['xai']:.2f}",
                       f"Anthropic ${sp['anthropic']:.2f} · xAI ${sp['xai']:.2f}"))
    out.append(f"<div class=kpis>{''.join(kp)}</div>")

    # v5 vs v7
    out.append("<section><div class=hd><h2>v5 vs v7 — same coins</h2></div>"
               "<p class=lede>The solo-pick rows decide it: a version wins a day when its own picks beat the "
               "other’s by 10+ pts per SOL.</p>")
    if _err(p):
        out.append(_err(p))
    else:
        cols = CHANNELS + ["both"]
        head = "".join(
            f"<th>{_e(SHORT.get(c, 'both'))}<br><span class='pill {w}'>{_e({'none': 'no call'}.get(w, w + ' wins'))}</span></th>"
            for c in cols for w in [winner(p["by_ch"][c]["v5 only"], p["by_ch"][c]["v7 only"])])
        out.append(f"<div class=scroll><table class=grid><tr><th></th>{head}</tr>")
        for i, (k, lab) in enumerate((("v5 only", "v5 only"), ("v7 only", "v7 only"), ("both BUY", "Both BUY"),
                                      ("v5 BUY", "All v5 BUYs"), ("v7 BUY", "All v7 BUYs"),
                                      ("both PASS", "Both PASS"), ("no brain", "No brain"))):
            sep = " class=sep" if i in (2, 5) else ""
            out.append(f"<tr{sep}><td>{_e(lab)}</td>" + "".join(_cell(p["by_ch"][c][k]) for c in cols) + "</tr>")
        out.append("</table></div><div class=chips>")
        for day_i, a, bb, w in p["tally"]:
            out.append(f"<div class=chip><div class=d>{day_i:%a %b %-d}</div><div class='w {w}'>"
                       f"{_e({'none': 'No call'}.get(w, w))}</div><div class=x>v5 {_e(pct(a))} · v7 {_e(pct(bb))}</div></div>")
        out.append("</div>")
    out.append("</section>")

    # solwhaletrending v5
    out.append("<section><div class=hd><h2>solwhaletrending — v5</h2></div>"
               "<p class=lede>The live channel. Live bar: BUYs positive after ~3% fees.</p>")
    if _err(s):
        out.append(_err(s))
    else:
        out.append("<table class=rows>")
        out.append(_group("Today"))
        out.append(_pnl_row("BUY", s["today"]["BUY"]) + _pnl_row("Pass", s["today"]["pass"]))
        out.append(_group(f"Since {date.fromisoformat(SWT_SINCE):%b %-d}"))
        out.append(_pnl_row("BUY", s["cum"]["BUY"]) + _pnl_row("Pass", s["cum"]["pass"]))
        out.append(_group(f"Frozen filters · since {date.fromisoformat(FILTERS_SINCE):%b %-d}"))
        out.append(_pnl_row("Plain BUY", s["cum"]["BUY since filters"])
                   + _pnl_row("F1", s["cum"]["F1"], "BUY, called under 150k")
                   + _pnl_row("F2", s["cum"]["F2"], "BUY + a followed wallet holding"))
        out.append("</table><p class=note>Counts: trades · banks (b) · rugs (r). A filter earns a place only by "
                   "beating plain BUY on new days.</p>")
    out.append("</section>")

    # at the bank
    out.append("<section><div class=hd><h2>At the 2x bank</h2></div>"
               "<p class=lede>Did the coin reach 3x before falling back to 1.55x? The marker is the ~45% "
               "break-even for holding.</p><table class=rows>")
    if _err(b):
        out.append(f"<tr><td colspan=4>{_err(b)}</td></tr>")
    else:
        out.append(_group(f"Brain · prompt v{bank_v()}"))
        out.append(_rate_row("Says HOLD", b["HOLD (p_3x 50+)"], "its 3x odds 50%+")
                   + _rate_row("Middling", b["p_3x 30-49"], "odds 30–49%")
                   + _rate_row("Says BANK", b["p_3x under 30"], "odds under 30%")
                   + _rate_row("All", b["all"]))
    if _err(g):
        out.append(f"<tr><td colspan=4>{_err(g)}</td></tr>")
    else:
        out.append(_group("Grok · X attention at the bank"))
        out.append(_rate_row("Accelerating", g["accelerating"], "more posts in the last 30 min")
                   + _rate_row("Not accelerating", g["not accelerating"])
                   + _rate_row("1k+ follower accounts", g["1k+ accounts"])
                   + _rate_row("All", g["all"]))
    out.append("</table><p class=note>Graded 2 h+ after each bank, since each test began. A group needs 30+ "
               "coins clearly above the marker, twice, before it earns an exit rule.</p></section>")

    # the day's trades
    out.append("<section><div class=hd><h2>The day’s trades</h2></div>"
               "<p class=lede>Everything qsim traded, before any brain filter, by launchpad.</p>")
    if _err(m):
        out.append(_err(m))
    else:
        out.append("<table class=rows>")
        for ch in CHANNELS:
            out.append(_group(ch))
            out.append(_pnl_row("All", m["channels"][ch]))
            for pad in ("Float", "pump", "other"):
                a = m["launchpads"][(ch, pad)]
                if a["n"]:
                    out.append(_pnl_row(pad, a))
        out.append("</table>")
    out.append("</section>")

    if "error" not in sp:
        out.append(f"<footer>{sp['brain_calls']} brain calls · {sp['grok_calls']} Grok looks · spend is estimated "
                   f"from stored token counts; the provider consoles are the real number.</footer>")
    else:
        out.append(f"<footer>{_err(sp)}</footer>")
    out.append("</main></body></html>")
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
