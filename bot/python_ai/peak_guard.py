"""
peak_guard.py — corroboration fail-safe for peak_mcap tracking.

WHY THIS EXISTS
---------------
The trailing stop arms and fires off `peak_mcap`, which is a max() across
multiple price feeds (ws_monitor's helius_tx/Jupiter, monitor.py's REST sweep).
Because it's a max(), a single spurious high reading from ANY feed becomes a
*permanent* peak the trail can never recover from — it then fires instantly
because the real price sits far below the phantom peak, force-exiting a position
that never actually moved. This was the root cause of the live-vs-paper bleed
(see memory: phantom_peak_root_cause).

The get_mcap_blended fix removed the main phantom source, but the max() ratchet
is still structurally fragile to any one-off bad reading (e.g. helius_tx delta
math occasionally picking up an unrelated transfer). This module is the durable
fail-safe: a candidate new peak that JUMPS more than a sane amount above the
current peak is not accepted until a SECOND consecutive reading corroborates it.
One-tick spikes never stick; sustained real moves arm the trail one tick later.

DESIGN NOTES
------------
* Pure in-memory, per-process. ws_monitor and monitor.py run as separate
  processes; each corroborates within its own reading stream. The DB peak is the
  shared persistence, and it's always passed in as `prior_peak`.
* Under-recording the peak is SAFE for a trailing stop — it can only make the
  trail arm/fire later, never on a phantom. So when in doubt, we hold.
* Small advances are accepted immediately, so normal gradual climbs are tracked
  precisely; only large suspicious jumps require a second reading.
"""

from __future__ import annotations

import os
import time

# A single reading may raise the peak by up to this fraction without
# corroboration. Jumps beyond it must be confirmed by a second reading.
MAX_UNCONFIRMED_JUMP_PCT = float(os.getenv("PEAK_MAX_UNCONFIRMED_JUMP_PCT", "0.50"))

# Symmetric threshold for the DOWN direction (guard_trough): a single reading may DROP by
# up to this fraction below the last accepted level without corroboration. Sudden craters
# beyond it (phantom lows / null-derived prices) must be confirmed by a second reading
# before the exit acts on them, so one bad tick can't fire a hard_stop.
MAX_UNCONFIRMED_DROP_PCT = float(os.getenv("PEAK_MAX_UNCONFIRMED_DROP_PCT", "0.50"))

# A pending (unconfirmed) candidate is forgotten after this many seconds with no
# corroborating reading.
PENDING_TTL_SECS = float(os.getenv("PEAK_PENDING_TTL_SECS", "30.0"))

# The trough baseline (last accepted mcap per position) is evicted after this long with no
# readings — i.e. the position has closed. Bounds memory without dropping active positions.
TROUGH_REF_TTL_SECS = float(os.getenv("PEAK_TROUGH_REF_TTL_SECS", "1800.0"))

# The corroborating reading must be at least (candidate * (1 - this)) to confirm.
CONFIRM_TOLERANCE = float(os.getenv("PEAK_CONFIRM_TOLERANCE", "0.15"))

# key -> (candidate_mcap, monotonic_time) for jumps awaiting corroboration.
_pending: dict[str, tuple[float, float]] = {}
# Down-direction counterparts for guard_trough.
_pending_low: dict[str, tuple[float, float]] = {}   # craters awaiting corroboration
_trough_ref:  dict[str, tuple[float, float]] = {}   # last accepted mcap (the drop baseline)


def guard_peak(
    key: str,
    current_mcap: float,
    prior_peak: float,
    *,
    max_jump_pct: float | None = None,
    pending_ttl_secs: float | None = None,
    confirm_tolerance: float | None = None,
) -> float:
    """
    Return the peak that should be recorded given a new reading.

    Parameters
    ----------
    key          Unique per (position, lane), e.g. "wsL:1234".
    current_mcap The new mcap reading.
    prior_peak   The existing accepted peak BEFORE this reading (max of DB peak
                 and in-memory caches), 0 if none yet.

    Returns the accepted peak: either `prior_peak` unchanged (candidate held for
    corroboration) or an advanced value.

    The optional threshold overrides let slower quote-based monitors use the same
    two-reading protection without forcing feed/ws monitors to hold pending spikes
    longer than their normal tick cadence.
    """
    max_jump_pct = MAX_UNCONFIRMED_JUMP_PCT if max_jump_pct is None else max_jump_pct
    pending_ttl_secs = PENDING_TTL_SECS if pending_ttl_secs is None else pending_ttl_secs
    confirm_tolerance = CONFIRM_TOLERANCE if confirm_tolerance is None else confirm_tolerance

    if current_mcap is None or current_mcap <= 0:
        return prior_peak

    # Not a new high — nothing to corroborate. A genuine pullback also means any
    # pending spike has failed to sustain, so drop it.
    if current_mcap <= prior_peak:
        _pending.pop(key, None)
        return prior_peak

    # No baseline yet — can't judge a jump; accept the first real high.
    if prior_peak <= 0:
        _pending.pop(key, None)
        return current_mcap

    jump = current_mcap / prior_peak
    if jump <= 1.0 + max_jump_pct:
        # Plausible incremental advance — accept immediately.
        _pending.pop(key, None)
        return current_mcap

    # Large jump — require a second consecutive elevated reading to confirm.
    now = time.monotonic()
    pend = _pending.get(key)
    if (
        pend is not None
        and (now - pend[1]) <= pending_ttl_secs
        and current_mcap >= pend[0] * (1.0 - confirm_tolerance)
    ):
        # Corroborated: two readings in a row agree the price moved up sharply.
        _pending.pop(key, None)
        return current_mcap

    # First sighting of this jump (or it changed too much) — hold, don't advance.
    _pending[key] = (current_mcap, now)
    _maybe_sweep(now)
    return prior_peak


def guard_trough(key: str, current_mcap: float) -> float:
    """
    Mirror image of guard_peak for the DOWN direction. Returns the mcap the exit check
    should ACT ON, holding back a single uncorroborated crater so one bad-low tick (a
    phantom / null-derived price) cannot fire a hard_stop. A real, sustained drop is
    confirmed by the very next reading and exits one tick (~2-3s) later — the same safe
    "act one tick late, never on a phantom" trade-off guard_peak makes on the high side.

    Tracks the last accepted mcap per `key` internally as the drop baseline, so callers
    just pass each new reading. Gradual pullbacks (<= MAX_UNCONFIRMED_DROP_PCT per tick)
    pass straight through, so normal trailing stops are unaffected.
    """
    now = time.monotonic()
    if current_mcap is None or current_mcap <= 0:
        # No usable price this tick — hand back the trusted baseline so the exit doesn't
        # act on a null (callers also hard-guard <=0; this is belt-and-suspenders).
        ref = _trough_ref.get(key)
        return ref[0] if ref else 0.0

    ref_entry = _trough_ref.get(key)
    ref = ref_entry[0] if ref_entry else 0.0

    if ref <= 0 or current_mcap >= ref or (1.0 - current_mcap / ref) <= MAX_UNCONFIRMED_DROP_PCT:
        # No baseline yet, not a drop, or a plausible incremental pullback — accept the
        # reading and advance the baseline.
        _pending_low.pop(key, None)
        _trough_ref[key] = (current_mcap, now)
        return current_mcap

    # Large sudden crater — require a 2nd consecutive low reading to confirm it's real.
    pend = _pending_low.get(key)
    if (
        pend is not None
        and (now - pend[1]) <= PENDING_TTL_SECS
        and current_mcap <= pend[0] * (1.0 + CONFIRM_TOLERANCE)
    ):
        _pending_low.pop(key, None)
        _trough_ref[key] = (current_mcap, now)
        return current_mcap

    # First sighting of the crater — HOLD at the trusted baseline, don't act on it yet.
    _pending_low[key] = (current_mcap, now)
    _maybe_sweep(now)
    return ref


def clear(key: str) -> None:
    """Drop any pending/baseline state for a position (call on close)."""
    _pending.pop(key, None)
    _pending_low.pop(key, None)
    _trough_ref.pop(key, None)


def _maybe_sweep(now: float) -> None:
    """Opportunistically evict expired entries to bound memory."""
    if len(_pending) < 256 and len(_pending_low) < 256 and len(_trough_ref) < 1024:
        return
    for store, ttl in (
        (_pending, PENDING_TTL_SECS),
        (_pending_low, PENDING_TTL_SECS),
        (_trough_ref, TROUGH_REF_TTL_SECS),
    ):
        expired = [k for k, (_, t) in store.items() if (now - t) > ttl]
        for k in expired:
            store.pop(k, None)
