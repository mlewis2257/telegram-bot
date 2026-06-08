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

# A pending (unconfirmed) candidate is forgotten after this many seconds with no
# corroborating reading.
PENDING_TTL_SECS = float(os.getenv("PEAK_PENDING_TTL_SECS", "30.0"))

# The corroborating reading must be at least (candidate * (1 - this)) to confirm.
CONFIRM_TOLERANCE = float(os.getenv("PEAK_CONFIRM_TOLERANCE", "0.15"))

# key -> (candidate_mcap, monotonic_time) for jumps awaiting corroboration.
_pending: dict[str, tuple[float, float]] = {}


def guard_peak(key: str, current_mcap: float, prior_peak: float) -> float:
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
    """
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
    if jump <= 1.0 + MAX_UNCONFIRMED_JUMP_PCT:
        # Plausible incremental advance — accept immediately.
        _pending.pop(key, None)
        return current_mcap

    # Large jump — require a second consecutive elevated reading to confirm.
    now = time.monotonic()
    pend = _pending.get(key)
    if (
        pend is not None
        and (now - pend[1]) <= PENDING_TTL_SECS
        and current_mcap >= pend[0] * (1.0 - CONFIRM_TOLERANCE)
    ):
        # Corroborated: two readings in a row agree the price moved up sharply.
        _pending.pop(key, None)
        return current_mcap

    # First sighting of this jump (or it changed too much) — hold, don't advance.
    _pending[key] = (current_mcap, now)
    _maybe_sweep(now)
    return prior_peak


def clear(key: str) -> None:
    """Drop any pending state for a position (call on close)."""
    _pending.pop(key, None)


def _maybe_sweep(now: float) -> None:
    """Opportunistically evict expired pending entries to bound memory."""
    if len(_pending) < 256:
        return
    expired = [k for k, (_, t) in _pending.items() if (now - t) > PENDING_TTL_SECS]
    for k in expired:
        _pending.pop(k, None)
