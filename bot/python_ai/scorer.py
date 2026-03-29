"""
scorer.py — Rule-based conviction scorer for incoming token calls.

Two scoring paths:
  realtime — solwhaletrending / solearlytrending (initial_call)
             Scores 0-100 based on on-chain features at detection time.
  lagging  — solhousesignal
             Scores continuation potential only (entry already missed).

Public API
----------
score_call(call_id: int) -> dict
    {call_id, score, label, reasons, path}
"""

import db
import data_fetcher

# ── Cross-channel confirmation window ─────────────────────────────────────────

CROSS_CHANNEL_WINDOW_SECONDS = 3600  # 1 hour

# ── Label thresholds ──────────────────────────────────────────────────────────

_THRESHOLDS = [
    (85, "strong_alert"),
    (70, "alert"),
    (55, "caution"),
    (40, "watch"),
    (0,  "skip"),
]


def _label(score: int) -> str:
    for threshold, name in _THRESHOLDS:
        if score >= threshold:
            return name
    return "skip"


def _clamp(score: int) -> int:
    return max(0, min(95, score))


# ── Path A: Realtime scoring ──────────────────────────────────────────────────

def _score_realtime(row: dict) -> tuple[int, list[str]]:
    # Scoring weights updated based on empirical analysis of 1,000+ outcomes
    # Runners avg: liq=$19k, holders=370, dev_tokens=5972, mcap=$29k
    # Rugs avg:    liq=$71k, holders=584, dev_tokens=2129, mcap=$71k
    score = 35
    reasons: list[str] = []

    # ── Tier 1: always applied ─────────────────────────────────────────────

    is_vip = (
        row.get("channel_tag") == "solhousesignal_vip"
        or row.get("handle") == "solhousesignal_vip"
    )
    channel = row.get("handle", "") or row.get("channel_tag", "")
    is_realtime = "solearlytrending" in channel or "solwhaletrending" in channel

    # security_flag — nearly all runners are "safe"
    # VIP channel curates security itself so missing flag is neutral, not a penalty.
    sf = row.get("security_flag")
    if sf == "safe":
        score += 8
        reasons.append("security=safe+8")
    elif sf == "warning":
        score -= 25
        reasons.append("security=warning-25")
    elif (sf == "unknown" or sf is None) and not is_vip:
        score -= 15
        reasons.append("security=unknown-15")

    # token_age_minutes — very early entry is opportunity, not risk
    age = row.get("token_age_minutes")
    if age is not None:
        age = int(age)
        if age < 2:
            score += 5
            reasons.append(f"age={age}m+5")
        elif age <= 10:
            score += 8
            reasons.append(f"age={age}m+8")
        elif age <= 30:
            score += 10
            reasons.append(f"age={age}m+10")
        elif age <= 60:
            score += 3
            reasons.append(f"age={age}m+3")
        else:
            score -= 15
            reasons.append(f"age={age}m-15")

    # bundle_pct_remaining — unchanged
    bpr = row.get("bundle_pct_remaining")
    if bpr is not None:
        bpr = float(bpr)
        if bpr < 5:
            score += 10
            reasons.append(f"bundles_remaining={bpr:.1f}%+10")
        elif bpr < 15:
            score += 5
            reasons.append(f"bundles_remaining={bpr:.1f}%+5")
        elif bpr < 30:
            score -= 10
            reasons.append(f"bundles_remaining={bpr:.1f}%-10")
        else:
            score -= 20
            reasons.append(f"bundles_remaining={bpr:.1f}%-20")

    # liq_at_detection — FLIPPED: high liq = rug indicator
    liq = row.get("liq_at_detection")
    if liq is not None:
        liq = float(liq)
        if 13_000 <= liq <= 25_000:
            score += 12
            reasons.append(f"liq=${liq/1000:.0f}k+12")
        elif liq <= 50_000:
            score += 5
            reasons.append(f"liq=${liq/1000:.0f}k+5")
        elif liq >= 8_000:
            score += 3
            reasons.append(f"liq=${liq/1000:.0f}k+3")
        elif liq > 50_000:
            score -= 15
            reasons.append(f"liq=${liq/1000:.0f}k-15")
        else:
            score -= 15
            reasons.append(f"liq=${liq/1000:.0f}k-15")

    # hodl_count — high holders = rug indicator on realtime channels
    hodl = row.get("hodl_count")
    if hodl is not None:
        hodl = int(hodl)
        if is_realtime:
            # Tighter thresholds: early-stage tokens have very few holders
            if hodl < 200:
                score += 8
                reasons.append(f"holders={hodl}+8")
            elif hodl <= 300:
                score += 4
                reasons.append(f"holders={hodl}+4")
            elif hodl <= 400:
                pass  # neutral band
            else:
                score -= 8
                reasons.append(f"holders={hodl}-8")
        else:
            if 200 <= hodl <= 450:
                score += 8
                reasons.append(f"holders={hodl}+8")
            elif hodl < 200 and hodl >= 100:
                score += 4
                reasons.append(f"holders={hodl}+4")
            elif hodl <= 600:
                pass  # neutral band (450-600)
            else:
                score -= 8
                reasons.append(f"holders={hodl}-8")

    # mcap_at_call — strongest runner signal, strengthened
    mcap_entry = row.get("mcap_at_call")
    if mcap_entry is not None:
        mcap_entry = float(mcap_entry)
        if mcap_entry < 15_000:
            score += 15
            reasons.append(f"entry_mcap=${mcap_entry/1000:.0f}k+15")
        elif mcap_entry < 25_000:
            score += 10
            reasons.append(f"entry_mcap=${mcap_entry/1000:.0f}k+10")
        elif mcap_entry < 40_000:
            score += 5
            reasons.append(f"entry_mcap=${mcap_entry/1000:.0f}k+5")
        elif mcap_entry <= 60_000:
            pass  # neutral band
        elif mcap_entry <= 100_000:
            score -= 15
            reasons.append(f"entry_mcap=${mcap_entry/1000:.0f}k-15")
        else:
            score -= 30
            reasons.append(f"entry_mcap=${mcap_entry/1000:.0f}k-30")

    # ── Tier 2: applied only when not NULL ────────────────────────────────

    # bundle_count — unchanged
    bc = row.get("bundle_count")
    if bc is not None:
        bc = int(bc)
        if bc < 5:
            score += 15
            reasons.append(f"bundle_count={bc}+15")
        elif bc < 15:
            score += 5
            reasons.append(f"bundle_count={bc}+5")
        elif bc <= 25:
            score -= 10
            reasons.append(f"bundle_count={bc}-10")
        else:
            score -= 20
            reasons.append(f"bundle_count={bc}-20")

    # sniper_count — unchanged
    sc = row.get("sniper_count")
    if sc is not None:
        sc = int(sc)
        if sc < 10:
            score += 10
            reasons.append(f"snipers={sc}+10")
        elif sc <= 30:
            pass  # neutral band
        elif sc <= 50:
            score -= 10
            reasons.append(f"snipers={sc}-10")
        else:
            score -= 20
            reasons.append(f"snipers={sc}-20")

    # fake_vol_pct — unchanged
    fvp = row.get("fake_vol_pct")
    if fvp is not None:
        fvp = float(fvp)
        if fvp < 1:
            score += 5
            reasons.append(f"fake_vol={fvp:.1f}%+5")
        elif fvp < 5:
            pass  # neutral band
        elif fvp < 15:
            score -= 5
            reasons.append(f"fake_vol={fvp:.1f}%-5")
        else:
            score -= 15
            reasons.append(f"fake_vol={fvp:.1f}%-15")

    # first_20_pct — unchanged
    f20 = row.get("first_20_pct")
    if f20 is not None:
        f20 = float(f20)
        if f20 < 20:
            score += 10
            reasons.append(f"first_20={f20:.0f}%+10")
        elif f20 < 40:
            score += 5
            reasons.append(f"first_20={f20:.0f}%+5")
        else:
            score -= 10
            reasons.append(f"first_20={f20:.0f}%-10")

    # ── Tier 3: sparse fields ─────────────────────────────────────────────

    # dev_tokens_made — interpretation flips by channel type
    # Realtime (solearlytrending/solwhaletrending): high dev_tokens = serial rugger
    # Other channels: high dev_tokens = experienced deployer = runner signal
    dtm = row.get("dev_tokens_made")
    if dtm is not None:
        dtm = int(dtm)
        if is_realtime:
            if dtm < 5:
                score += 8
                reasons.append(f"dev_tokens={dtm}+8")
            elif dtm <= 20:
                score += 4
                reasons.append(f"dev_tokens={dtm}+4")
            elif dtm <= 50:
                pass  # neutral band
            elif dtm <= 200:
                score -= 10
                reasons.append(f"dev_tokens={dtm}-10")
            else:
                score -= 20
                reasons.append(f"dev_tokens={dtm}-20")
        else:
            if dtm < 5:
                pass  # neutral — low dev_tokens = inexperienced, not a runner signal
            elif dtm <= 50:
                score += 5
                reasons.append(f"dev_tokens={dtm}+5")
            elif dtm <= 200:
                score += 8
                reasons.append(f"dev_tokens={dtm}+8")
            elif dtm <= 1000:
                score += 10
                reasons.append(f"dev_tokens={dtm}+10")
            elif dtm <= 5000:
                score += 12
                reasons.append(f"dev_tokens={dtm}+12")
            else:
                score += 8
                reasons.append(f"dev_tokens={dtm}+8")

    # dev_best_mcap — unchanged
    dbm = row.get("dev_best_mcap")
    if dbm is not None:
        dbm = float(dbm)
        if dbm > 1_000_000:
            score += 10
            reasons.append(f"dev_best=${dbm/1000:.0f}k+10")
        elif dbm >= 100_000:
            score += 5
            reasons.append(f"dev_best=${dbm/1000:.0f}k+5")
        # < 100k → neutral

    # ── Data confidence penalty — unchanged ───────────────────────────────
    available_tier1 = sum(1 for v in [
        row.get("security_flag"),
        row.get("token_age_minutes"),
        row.get("bundle_pct_remaining"),
        row.get("liq_at_detection"),
        row.get("hodl_count"),
    ] if v is not None)
    if available_tier1 < 2:
        if is_vip:
            score -= 5   # VIP: small penalty, on-chain data just not in message
            reasons.append("low_data_confidence(vip)-5")
        else:
            score -= 25
            reasons.append("low_data_confidence-25")
    elif available_tier1 < 4:
        if not is_vip:   # VIP: partial data is normal, no penalty
            score -= 15
            reasons.append("partial_data-15")

    # ── Cross-channel confirmation — reduced (token already pumped by the time both call)
    if row.get("cross_channel_confirmed"):
        score += 5
        reasons.append("cross_channel_confirmed+5")

    # ── VIP tier (only present on VIP channel calls) ───────────────────────
    vip_tier = row.get("vip_tier")
    if vip_tier == "safe":
        score += 15
        reasons.append("vip_tier=safe+15")
    elif vip_tier == "gamble_risk":
        score -= 10
        reasons.append("vip_tier=gamble_risk-10")
    elif vip_tier == "high_risk":
        score -= 20
        reasons.append("vip_tier=high_risk-20")
    elif vip_tier == "whale":
        score += 10
        reasons.append("vip_tier=whale+10")
    # gamble / None → neutral (no adjustment)

    # ── Channel floor scores ───────────────────────────────────────────────
    if "solhousesignal_vip" in channel:
        if score < 45:
            score = 45
            reasons.append("vip_channel_floor=45")

    return score, reasons


# ── Path B: Lagging scoring ───────────────────────────────────────────────────

def _score_lagging(row: dict) -> tuple[int, list[str]]:
    score = 60
    reasons: list[str] = []

    # mcap_at_call (current mcap when posted to solhousesignal)
    mcap = row.get("mcap_at_call")
    if mcap is not None:
        mcap = float(mcap)
        if mcap < 50_000:
            score += 20
            reasons.append(f"mcap=${mcap/1000:.0f}k+20")
        elif mcap < 200_000:
            score += 10
            reasons.append(f"mcap=${mcap/1000:.0f}k+10")
        elif mcap < 1_000_000:
            pass  # neutral band
        else:
            score -= 20
            reasons.append(f"mcap=${mcap/1000:.0f}k-20")

    # stated_multiplier (how much it already ran before channel posted it)
    mult = row.get("stated_multiplier")
    if mult is not None:
        mult = float(mult)
        if mult < 3:
            score += 10
            reasons.append(f"mult={mult:.1f}x+10")
        elif mult < 10:
            score += 5
            reasons.append(f"mult={mult:.1f}x+5")
        elif mult < 20:
            pass  # neutral band
        else:
            score -= 10
            reasons.append(f"mult={mult:.1f}x-10")

    # ── Current price vs peak drawdown ────────────────────────────────────
    mint = row.get("mint_address")
    peak_mcap = row.get("mcap_at_result")
    if mint and peak_mcap:
        current = data_fetcher.fetch_token_price(mint)
        if current and current.get("mcap"):
            drawdown = (float(peak_mcap) - float(current["mcap"])) / float(peak_mcap)
            if drawdown > 0.7:
                score -= 20
                reasons.append(f"dumped_{drawdown:.0%}_from_peak-20")
            elif drawdown > 0.4:
                score -= 10
                reasons.append(f"pulling_back_{drawdown:.0%}-10")
            elif drawdown < 0.2:
                score += 10
                reasons.append("holding_near_peak+10")

    return score, reasons


# ── Public API ────────────────────────────────────────────────────────────────

def score_call(call_id: int, extra_fields: dict | None = None) -> dict:
    """
    Score a logged call by call_id.

    Fetches required fields from DB, applies path-appropriate rules,
    writes conviction_score back to calls.conviction_score, and returns
    a result dict with keys: call_id, score, label, reasons, path.

    extra_fields — optional dict merged into the DB row before scoring.
    Use this to pass fields not stored in the DB (e.g. vip_tier).
    """
    row = db.get_call_for_scoring(call_id, cross_channel_window_seconds=CROSS_CHANNEL_WINDOW_SECONDS)
    if row and extra_fields:
        row = {**row, **extra_fields}
    if not row:
        return {
            "call_id": call_id,
            "score":   0,
            "label":   "skip",
            "reasons": ["call not found"],
            "path":    "unknown",
        }

    channel_type = row.get("channel_type")

    if channel_type == "realtime":
        raw_score, reasons = _score_realtime(row)
        handle = row.get("handle", "")
        multiplier = 0.95 if "solearlytrending" in handle else 1.0
        path = "realtime"
    else:
        raw_score, reasons = _score_lagging(row)
        multiplier = 0.7
        path = "lagging"

    final = _clamp(round(raw_score * multiplier))
    label = _label(final)

    db.update_call_conviction_score(call_id, final)

    return {
        "call_id": call_id,
        "score":   final,
        "label":   label,
        "reasons": reasons,
        "path":    path,
    }
