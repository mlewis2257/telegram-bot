import os
import time
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# ── Connection ────────────────────────────────────────────────────────────────

_conn = None


def get_conn():
    """Lazy singleton connection. Re-opens if closed or broken."""
    global _conn
    if _conn is not None and not _conn.closed:
        try:
            _conn.isolation_level
            cur = _conn.cursor()
            cur.execute("SELECT 1")
            cur.close()
        except Exception:
            print("[db] stale connection detected — reconnecting")
            try:
                _conn.close()
            except Exception:
                pass
            _conn = None
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=os.environ["DB_PORT"],
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
        )
        _conn.autocommit = False
    return _conn


def close_conn():
    global _conn
    if _conn and not _conn.closed:
        _conn.close()


def safe_rollback() -> None:
    """
    Roll back any failed transaction on the shared connection.

    psycopg2 marks a connection as "in failed transaction" after any error,
    blocking all subsequent queries until rollback() is called. Call this
    in every except block that catches a DB write failure so the connection
    is clean for the next query.
    """
    try:
        if _conn and not _conn.closed:
            _conn.rollback()
    except Exception:
        pass


# ── Channels ──────────────────────────────────────────────────────────────────

def get_active_channels():
    """
    Return all active channels from the channels table.
    Each row is a dict: {id, handle, platform, channel_type, weight}
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, handle, platform, channel_type, weight
            FROM channels
            WHERE is_active = TRUE
            ORDER BY id
            """
        )
        return cur.fetchall()


def get_channel_by_handle(handle: str) -> dict | None:
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT id, handle, channel_type FROM channels WHERE handle = %s",
            (handle,)
        )
        row = cur.fetchone()
        if not row:
            return None
        return {"id": row[0], "handle": row[1], "channel_type": row[2]}


# ── Callers ───────────────────────────────────────────────────────────────────

def upsert_caller(platform: str, handle: str, name: str) -> int:
    """
    Insert a new caller or return the existing one's id.
    Updates the display name if the record already exists.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO callers (platform, handle, name)
            VALUES (%s, %s, %s)
            ON CONFLICT (platform, handle)
            DO UPDATE SET name = EXCLUDED.name, updated_at = NOW()
            RETURNING id
            """,
            (platform, handle, name),
        )
        conn.commit()
        return cur.fetchone()[0]


# ── Tokens ────────────────────────────────────────────────────────────────────

def upsert_token(mint_address: str, symbol: str = None, name: str = None) -> int:
    """
    Insert a new token mint or return the existing one's id.
    Symbol and name are stored if provided; existing values are not overwritten.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO tokens (mint_address, symbol, name)
            VALUES (%s, %s, %s)
            ON CONFLICT (mint_address)
            DO UPDATE SET
                symbol     = COALESCE(tokens.symbol, EXCLUDED.symbol),
                name       = COALESCE(tokens.name,   EXCLUDED.name),
                updated_at = NOW()
            RETURNING id
            """,
            (mint_address, symbol, name),
        )
        conn.commit()
        return cur.fetchone()[0]


# ── Calls ─────────────────────────────────────────────────────────────────────

def insert_call(
    caller_id: int,
    token_id: int,
    source_platform: str,
    source_message_id: str,
    raw_message: str,
    created_at,
    message_type: str = None,
    channel_id: int = None,
    mcap_at_call: float = None,
    narrative_tags: list = None,
    vip_tier: str = None,
    vip_message_type: str = None,
) -> int | None:
    """
    Insert a call event. Returns the new call id, or None if the message
    was already logged (dedup on source_platform + source_message_id).
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO calls
                (caller_id, token_id, call_type, source_platform,
                 source_message_id, raw_message, created_at,
                 message_type, channel_id, mcap_at_call, narrative_tags,
                 vip_tier, vip_message_type)
            VALUES (%s, %s, 'reactive', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_platform, source_message_id)
            WHERE source_message_id IS NOT NULL
            DO NOTHING
            RETURNING id
            """,
            (
                caller_id,
                token_id,
                source_platform,
                str(source_message_id),
                raw_message,
                created_at,
                message_type,
                channel_id,
                mcap_at_call,
                narrative_tags or [],
                vip_tier,
                vip_message_type,
            ),
        )
        conn.commit()
        row = cur.fetchone()
        return row[0] if row else None


# ── Token / call lookups ──────────────────────────────────────────────────────

def get_call_id_by_token_name(token_name: str) -> int | None:
    """
    Look up the most recent call_id whose token symbol or name matches
    token_name (case-insensitive). Channel-agnostic — used for cross-channel
    lookups (e.g. linking whale alerts to any call for that token).
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.id
            FROM calls c
            JOIN tokens t ON t.id = c.token_id
            WHERE LOWER(t.symbol) = LOWER(%s)
               OR LOWER(t.name)   = LOWER(%s)
            ORDER BY c.created_at DESC
            LIMIT 1
            """,
            (token_name, token_name),
        )
        row = cur.fetchone()
        return row[0] if row else None


def get_call_id_by_token_and_channel(token_name: str, channel_id: int) -> int | None:
    """
    Look up the most recent call_id for a token FROM A SPECIFIC CHANNEL.
    Used in lagging milestone attribution so a solhousesignal milestone
    only ever updates solhousesignal's own call, never a realtime channel's
    call for the same token.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.id
            FROM calls c
            JOIN tokens t ON t.id = c.token_id
            WHERE (LOWER(t.symbol) = LOWER(%s) OR LOWER(t.name) = LOWER(%s))
              AND c.channel_id = %s
            ORDER BY c.created_at DESC
            LIMIT 1
            """,
            (token_name, token_name, channel_id),
        )
        row = cur.fetchone()
        return row[0] if row else None


# ── Market data ───────────────────────────────────────────────────────────────

def update_call_market_data(
    call_id: int,
    price_usd: float = None,
    mcap: float = None,
    liquidity_usd: float = None,
) -> None:
    """
    Write fetched market data back onto the calls row.
    Uses COALESCE so existing values are never overwritten with None.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE calls SET
                price_at_call     = COALESCE(price_at_call,     %s),
                mcap_at_call      = COALESCE(mcap_at_call,      %s),
                liquidity_at_call = COALESCE(liquidity_at_call, %s)
            WHERE id = %s
            """,
            (price_usd, mcap, liquidity_usd, call_id),
        )
        conn.commit()


# ── Outcomes ──────────────────────────────────────────────────────────────────

def insert_outcome(call_id: int) -> None:
    """
    Create a blank outcomes row immediately when a call is logged.
    The backfill job will populate price_1h / price_4h / price_24h later.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO outcomes (call_id)
            VALUES (%s)
            ON CONFLICT (call_id) DO NOTHING
            """,
            (call_id,),
        )
        conn.commit()


def update_outcome_peak(
    call_id: int,
    multiplier_computed: float,
    multiplier_stated: float,
    current_mcap: float,
    reported_at,
) -> None:
    """
    Update peak fields on an outcomes row if the new multiplier is higher
    than what's already stored. Called when a milestone update message is parsed.
    Only writes if the new computed multiplier exceeds the existing one.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE outcomes SET
                peak_multiplier    = GREATEST(COALESCE(peak_multiplier, 0), %s),
                stated_multiplier  = CASE
                    WHEN %s > COALESCE(stated_multiplier, 0)
                    THEN %s ELSE stated_multiplier END,
                mcap_at_result     = CASE
                    WHEN %s > COALESCE(peak_multiplier, 0)
                    THEN %s ELSE mcap_at_result END,
                result_reported_at = CASE
                    WHEN %s > COALESCE(peak_multiplier, 0)
                    THEN %s ELSE result_reported_at END,
                outcome_label      = 'runner',
                updated_at         = NOW()
            WHERE call_id = %s
            """,
            (
                multiplier_computed,
                multiplier_stated, multiplier_stated,
                multiplier_computed, current_mcap,
                multiplier_computed, reported_at,
                call_id,
            ),
        )
        conn.commit()


def get_token_supply_and_decimals(mint: str) -> tuple:
    """
    Return (total_supply, decimals) for a mint address.
    Used by data_fetcher to compute mcap from Jupiter price.
    Returns (None, None) if the token is not found or columns are NULL.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT total_supply, decimals FROM tokens WHERE mint_address = %s",
            (mint,),
        )
        row = cur.fetchone()
        if not row or row[0] is None or row[1] is None:
            return (None, None)
        return (float(row[0]), int(row[1]))


def get_token_id_by_symbol(symbol: str) -> int | None:
    """
    Look up the most recent token_id by symbol (case-insensitive).
    Used to link whale alerts to a known token.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id FROM tokens
            WHERE LOWER(symbol) = LOWER(%s)
            ORDER BY first_seen_at DESC
            LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def insert_whale_alert(
    symbol: str,
    sol_amount: float,
    wallet_size_sol: float,
    mcap_at_whale: float,
    signal_count: int,
    raw_message: str,
    token_id: int = None,
    call_id: int = None,
) -> int:
    """
    Insert a whale alert row. call_id may be NULL if the token hasn't
    been called yet — backfill_whale_alert_call_ids() links it later.
    Returns the new row id.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO whale_alerts
                (call_id, token_id, symbol, sol_amount,
                 wallet_size_sol, mcap_at_whale, signal_count, raw_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (call_id, token_id, symbol, sol_amount,
             wallet_size_sol, mcap_at_whale, signal_count, raw_message),
        )
        conn.commit()
        return cur.fetchone()[0]


def update_token_dexscreener_paid(token_id: int) -> None:
    """
    Mark a token as having purchased a DexScreener paid profile.
    Only sets the timestamp on the first detection (COALESCE keeps original).
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tokens SET
                dexscreener_paid    = TRUE,
                dexscreener_paid_at = COALESCE(dexscreener_paid_at, NOW()),
                updated_at          = NOW()
            WHERE id = %s
            """,
            (token_id,),
        )
        conn.commit()


def update_token_boost(token_id: int, boost_count: int) -> None:
    """
    Increment boost_count by the purchased amount and update last_boost_at.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tokens SET
                boost_count   = boost_count + %s,
                last_boost_at = NOW(),
                updated_at    = NOW()
            WHERE id = %s
            """,
            (boost_count, token_id),
        )
        conn.commit()


def backfill_whale_alert_call_ids(token_id: int, call_id: int) -> int:
    """
    Link unlinked whale_alerts rows (call_id IS NULL) to a newly-logged call
    for the same token. Returns the number of rows updated.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE whale_alerts
            SET call_id = %s
            WHERE token_id = %s
              AND call_id IS NULL
            """,
            (call_id, token_id),
        )
        conn.commit()
        return cur.rowcount


def upsert_token_realtime_metadata(
    token_id: int,
    token_age_minutes: int = None,
    security_flag: str = None,
    is_cto: bool = None,
    bundle_count: int = None,
    bundle_pct_initial: float = None,
    bundle_pct_remaining: float = None,
    sniper_count: int = None,
    sniper_pct_initial: float = None,
    sniper_pct_remaining: float = None,
    first_20_pct: float = None,
    dev_sol_held: float = None,
    dev_pct_held: float = None,
    dev_sold: bool = None,
    dev_tokens_made: int = None,
    dev_bonds: int = None,
    dev_best_mcap: float = None,
    bundled_pct: float = None,
    bundled_sold_pct: float = None,
    hodl_count: int = None,
    liq_at_detection: float = None,
    vol_1h_at_detection: float = None,
    detecting_wallet_sol: float = None,
    detecting_sol_spent: float = None,
    fake_vol_usd: float = None,
    fake_vol_pct: float = None,
    mint_resolved: bool = None,
) -> None:
    """
    Write realtime entry signal metadata onto a tokens row.
    COALESCE on each numeric/text field preserves data from the first
    detection — subsequent calls for the same token don't overwrite.
    is_cto uses OR so a TRUE value is sticky and never reverted.
    mint_resolved is written whenever a non-None value is provided
    (FALSE for UNKNOWN: synthetic mints, TRUE once a real mint is found).
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE tokens SET
                token_age_minutes    = COALESCE(token_age_minutes,    %s),
                security_flag        = COALESCE(security_flag,        %s),
                is_cto               = is_cto OR COALESCE(%s, FALSE),
                bundle_count         = COALESCE(bundle_count,         %s),
                bundle_pct_initial   = COALESCE(bundle_pct_initial,   %s),
                bundle_pct_remaining = COALESCE(bundle_pct_remaining, %s),
                sniper_count         = COALESCE(sniper_count,         %s),
                sniper_pct_initial   = COALESCE(sniper_pct_initial,   %s),
                sniper_pct_remaining = COALESCE(sniper_pct_remaining, %s),
                first_20_pct         = COALESCE(first_20_pct,         %s),
                dev_sol_held         = COALESCE(dev_sol_held,         %s),
                dev_pct_held         = COALESCE(dev_pct_held,         %s),
                dev_sold             = COALESCE(dev_sold,             %s),
                dev_tokens_made      = COALESCE(dev_tokens_made,      %s),
                dev_bonds            = COALESCE(dev_bonds,            %s),
                dev_best_mcap        = COALESCE(dev_best_mcap,        %s),
                bundled_pct          = COALESCE(bundled_pct,          %s),
                bundled_sold_pct     = COALESCE(bundled_sold_pct,     %s),
                hodl_count           = COALESCE(hodl_count,           %s),
                liq_at_detection     = COALESCE(liq_at_detection,     %s),
                vol_1h_at_detection  = COALESCE(vol_1h_at_detection,  %s),
                detecting_wallet_sol = COALESCE(detecting_wallet_sol, %s),
                detecting_sol_spent  = COALESCE(detecting_sol_spent,  %s),
                fake_vol_usd         = COALESCE(fake_vol_usd,         %s),
                fake_vol_pct         = COALESCE(fake_vol_pct,         %s),
                mint_resolved        = COALESCE(%s, mint_resolved),
                updated_at           = NOW()
            WHERE id = %s
            """,
            (
                token_age_minutes,
                security_flag,
                is_cto,
                bundle_count,
                bundle_pct_initial,
                bundle_pct_remaining,
                sniper_count,
                sniper_pct_initial,
                sniper_pct_remaining,
                first_20_pct,
                dev_sol_held,
                dev_pct_held,
                dev_sold,
                dev_tokens_made,
                dev_bonds,
                dev_best_mcap,
                bundled_pct,
                bundled_sold_pct,
                hodl_count,
                liq_at_detection,
                vol_1h_at_detection,
                detecting_wallet_sol,
                detecting_sol_spent,
                fake_vol_usd,
                fake_vol_pct,
                mint_resolved,
                token_id,
            ),
        )
        conn.commit()


def insert_channel_daily_stats(
    channel_id: int,
    stat_date,
    entry_signals: int = None,
    win_rate_50pct: float = None,
    wins_avg_profit: float = None,
    best_token: str = None,
    best_multiplier: float = None,
    alerts_2x: int = None,
    alerts_5x: int = None,
    alerts_10x: int = None,
    alerts_15x_plus: int = None,
    top_performers=None,
) -> None:
    """
    Insert or update a channel daily stats row (one per channel per day).
    ON CONFLICT merges TYPE 4 (aggregate) and TYPE 5 (leaderboard) data:
    COALESCE keeps the first value received for each aggregate field.
    top_performers is always updated when a non-None value is provided.
    """
    top_performers_db = Json(top_performers) if top_performers is not None else None

    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO channel_daily_stats
                (channel_id, stat_date,
                 entry_signals, win_rate_50pct, wins_avg_profit,
                 best_token, best_multiplier,
                 alerts_2x, alerts_5x, alerts_10x, alerts_15x_plus,
                 top_performers)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (channel_id, stat_date) DO UPDATE SET
                entry_signals   = COALESCE(channel_daily_stats.entry_signals,   EXCLUDED.entry_signals),
                win_rate_50pct  = COALESCE(channel_daily_stats.win_rate_50pct,  EXCLUDED.win_rate_50pct),
                wins_avg_profit = COALESCE(channel_daily_stats.wins_avg_profit, EXCLUDED.wins_avg_profit),
                best_token      = COALESCE(channel_daily_stats.best_token,      EXCLUDED.best_token),
                best_multiplier = COALESCE(channel_daily_stats.best_multiplier, EXCLUDED.best_multiplier),
                alerts_2x       = COALESCE(channel_daily_stats.alerts_2x,       EXCLUDED.alerts_2x),
                alerts_5x       = COALESCE(channel_daily_stats.alerts_5x,       EXCLUDED.alerts_5x),
                alerts_10x      = COALESCE(channel_daily_stats.alerts_10x,      EXCLUDED.alerts_10x),
                alerts_15x_plus = COALESCE(channel_daily_stats.alerts_15x_plus, EXCLUDED.alerts_15x_plus),
                top_performers  = COALESCE(EXCLUDED.top_performers,             channel_daily_stats.top_performers),
                recorded_at     = NOW()
            """,
            (
                channel_id, stat_date,
                entry_signals, win_rate_50pct, wins_avg_profit,
                best_token, best_multiplier,
                alerts_2x, alerts_5x, alerts_10x, alerts_15x_plus,
                top_performers_db,
            ),
        )
        conn.commit()


def update_outcome_from_lagging(
    call_id: int,
    mcap_at_result: float,
    result_reported_at,
    stated_multiplier: float,
    peak_multiplier: float,
    outcome_label: str = "runner",
) -> None:
    """
    Write Type A (lagging) result data into the outcomes row.
    Called immediately after insert_call for lagging channel messages
    since both entry and result are known at parse time.

    - mcap_at_result     → what the channel reported (channel's number)
    - result_reported_at → timestamp of the message itself
    - stated_multiplier  → parsed from "2x ACHIEVED" (channel's claim)
    - peak_multiplier    → computed: mcap_at_result / mcap_at_call (accurate)
    - outcome_label      → always 'runner' for lagging calls (they only post wins)
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE outcomes SET
                mcap_at_result     = %s,
                result_reported_at = %s,
                stated_multiplier  = %s,
                peak_multiplier    = %s,
                outcome_label      = %s,
                updated_at         = NOW()
            WHERE call_id = %s
            """,
            (
                mcap_at_result,
                result_reported_at,
                stated_multiplier,
                peak_multiplier,
                outcome_label,
                call_id,
            ),
        )
        conn.commit()


# ── Backfill helpers ──────────────────────────────────────────────────────────

def get_pending_backfill(interval_hours: int, limit: int = 50) -> list[dict]:
    """
    Return up to `limit` calls that:
      - have passed the interval window (created_at < NOW() - INTERVAL)
      - have not yet been backfilled for that interval
      - have a resolved mint address (not UNKNOWN: or INFERRED: prefixed)

    Returns a list of dicts with keys:
      call_id, symbol, mint_address, mcap_at_call,
      peak_multiplier, created_at, pct_change_24h
    """
    col = {1: "backfilled_1h_at", 4: "backfilled_4h_at", 24: "backfilled_24h_at"}[interval_hours]

    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT
                c.id            AS call_id,
                t.symbol,
                t.mint_address,
                c.mcap_at_call,
                o.peak_multiplier,
                c.created_at,
                o.pct_change_24h
            FROM calls c
            JOIN tokens  t ON t.id = c.token_id
            JOIN outcomes o ON o.call_id = c.id
            WHERE o.{col} IS NULL
              AND c.created_at < NOW() - INTERVAL '{interval_hours} hours'
              AND t.mint_address IS NOT NULL
              AND t.mint_address NOT LIKE 'UNKNOWN:%%'
              AND t.mint_address NOT LIKE 'INFERRED:%%'
            ORDER BY c.created_at
            LIMIT %s
            """,
            (limit,),
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def update_outcome_interval(
    call_id: int,
    interval_hours: int,
    price: float | None,
    mcap: float | None,
    pct_change: float | None,
    outcome_label: str | None = None,
) -> None:
    """
    Write DexScreener snapshot data for one time interval into the outcomes row.
    Set outcome_label when provided (after 24h backfill).
    """
    sets = {
        1:  ("price_1h",  "mcap_1h",  "pct_change_1h",  "backfilled_1h_at"),
        4:  ("price_4h",  "mcap_4h",  "pct_change_4h",  "backfilled_4h_at"),
        24: ("price_24h", "mcap_24h", "pct_change_24h", "backfilled_24h_at"),
    }
    price_col, mcap_col, pct_col, ts_col = sets[interval_hours]

    label_clause = ", outcome_label = %s" if outcome_label is not None else ""
    params = [price, mcap, pct_change, call_id]
    if outcome_label is not None:
        params.insert(-1, outcome_label)   # before call_id

    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE outcomes SET
                {price_col} = %s,
                {mcap_col}  = %s,
                {pct_col}   = %s,
                {ts_col}    = NOW(){label_clause},
                updated_at  = NOW()
            WHERE call_id = %s
            """,
            params,
        )
        conn.commit()


# ── Scorer helpers ────────────────────────────────────────────────────────────

def get_call_for_scoring(
    call_id: int,
    cross_channel_window_seconds: int = 3600,
) -> dict | None:
    """
    Return a flat dict of every field needed by scorer.py for a single call.
    Joins calls → tokens → channels → outcomes (LEFT).

    Includes cross_channel_confirmed: True when either:
      - the same token has calls from BOTH solwhaletrending and solearlytrending
        within cross_channel_window_seconds of this call, OR
      - solhousesignal_vip AND solhousesignal (free) both called the same token
        within 4 hours of this call (lagging channel is slower)
    Used by the realtime scorer's cross-channel bonus rule (+5).

    Returns None if the call_id doesn't exist.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                c.id              AS call_id,
                c.mcap_at_call,
                c.message_type,
                ch.handle,
                ch.channel_type,
                t.id              AS token_id,
                t.symbol,
                t.security_flag,
                t.token_age_minutes,
                t.bundle_pct_remaining,
                t.liq_at_detection,
                t.hodl_count,
                t.bundle_count,
                t.sniper_count,
                t.fake_vol_pct,
                t.first_20_pct,
                t.dev_tokens_made,
                t.dev_best_mcap,
                t.mint_address,
                o.stated_multiplier,
                o.mcap_at_result,
                (
                    -- Both realtime channels called within window
                    (
                        SELECT COUNT(DISTINCT ch2.handle)
                        FROM calls c2
                        JOIN channels ch2 ON ch2.id = c2.channel_id
                        WHERE c2.token_id = t.id
                          AND ch2.handle IN ('solwhaletrending', 'solearlytrending')
                          AND ABS(EXTRACT(EPOCH FROM (c2.created_at - c.created_at))) <= %s
                    ) >= 2
                    OR
                    -- VIP + free channel both called within 4 hours
                    (
                        EXISTS (
                            SELECT 1 FROM calls c2
                            JOIN channels ch2 ON ch2.id = c2.channel_id
                            WHERE c2.token_id = t.id
                              AND ch2.handle = 'solhousesignal_vip'
                              AND ABS(EXTRACT(EPOCH FROM (c2.created_at - c.created_at))) <= 14400
                        )
                        AND EXISTS (
                            SELECT 1 FROM calls c2
                            JOIN channels ch2 ON ch2.id = c2.channel_id
                            WHERE c2.token_id = t.id
                              AND ch2.handle = 'solhousesignal'
                              AND ABS(EXTRACT(EPOCH FROM (c2.created_at - c.created_at))) <= 14400
                        )
                    )
                ) AS cross_channel_confirmed
            FROM calls c
            JOIN tokens   t  ON t.id  = c.token_id
            LEFT JOIN channels ch ON ch.id = c.channel_id
            LEFT JOIN outcomes o ON o.call_id = c.id
            WHERE c.id = %s
            """,
            (cross_channel_window_seconds, call_id),
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = [d.name for d in cur.description]
        return dict(zip(cols, row))


def get_mint_by_call_id(call_id: int) -> str | None:
    """Return the mint_address for the token linked to a call, or None."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.mint_address
            FROM calls c
            JOIN tokens t ON t.id = c.token_id
            WHERE c.id = %s
            """,
            (call_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


# ── Monitor helpers ───────────────────────────────────────────────────────────

def get_active_watchlist(min_score: int = 45, max_age_hours: int = 24) -> list[dict]:
    """
    Return calls that are recent, resolved, and scored high enough to monitor.

    Two groups are returned, regular rows first:
      data_only=False  — normal calls: peak tracking + alerts + exit logic
      data_only=True   — VIP paused calls: peak tracking only, no exits/alerts

    Ordered regular-first (data_only ASC) then conviction_score DESC within each group.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                c.id                AS call_id,
                t.symbol,
                t.mint_address,
                c.mcap_at_call,
                c.conviction_score,
                c.created_at,
                c.source_message_id,
                ch.handle           AS channel_handle,
                o.peak_multiplier,
                FALSE               AS data_only
            FROM calls    c
            JOIN tokens   t  ON t.id      = c.token_id
            JOIN channels ch ON ch.id     = c.channel_id
            JOIN outcomes o  ON o.call_id = c.id
            WHERE c.created_at > NOW() - INTERVAL '1 hour' * %s
              AND c.conviction_score >= %s
              AND c.skip_reason IS NULL
              AND t.mint_resolved = TRUE
              AND t.mint_address IS NOT NULL
              AND t.mint_address NOT LIKE 'UNKNOWN:%%'
              AND t.mint_address NOT LIKE 'INFERRED:%%'
              AND t.mint_address NOT IN (
                  SELECT DISTINCT t2.mint_address
                  FROM trading_positions tp2
                  JOIN calls  c2 ON c2.id  = tp2.call_id
                  JOIN tokens t2 ON t2.id  = c2.token_id
                  WHERE tp2.status        = 'closed'
                    AND tp2.is_simulation = TRUE
              )

            UNION ALL

            -- VIP paused calls: peak tracking only for data collection
            SELECT
                c.id                AS call_id,
                t.symbol,
                t.mint_address,
                c.mcap_at_call,
                c.conviction_score,
                c.created_at,
                c.source_message_id,
                ch.handle           AS channel_handle,
                o.peak_multiplier,
                TRUE                AS data_only
            FROM calls    c
            JOIN tokens   t  ON t.id      = c.token_id
            JOIN channels ch ON ch.id     = c.channel_id
            JOIN outcomes o  ON o.call_id = c.id
            WHERE c.skip_reason = 'vip_paused'
              AND c.created_at > NOW() - INTERVAL '24 hours'
              AND t.mint_resolved = TRUE
              AND t.mint_address IS NOT NULL
              AND t.mint_address NOT LIKE 'UNKNOWN:%%'
              AND t.mint_address NOT LIKE 'INFERRED:%%'

            UNION ALL

            -- mcap_too_high calls from solhousesignal/solwhaletrending: peak tracking only
            SELECT
                c.id                AS call_id,
                t.symbol,
                t.mint_address,
                c.mcap_at_call,
                c.conviction_score,
                c.created_at,
                c.source_message_id,
                ch.handle           AS channel_handle,
                o.peak_multiplier,
                TRUE                AS data_only
            FROM calls    c
            JOIN tokens   t  ON t.id      = c.token_id
            JOIN channels ch ON ch.id     = c.channel_id
            JOIN outcomes o  ON o.call_id = c.id
            WHERE c.skip_reason = 'mcap_too_high'
              AND c.created_at > NOW() - INTERVAL '24 hours'
              AND ch.handle IN ('solhousesignal', 'solwhaletrending')
              AND t.mint_resolved = TRUE
              AND t.mint_address IS NOT NULL
              AND t.mint_address NOT LIKE 'UNKNOWN:%%'
              AND t.mint_address NOT LIKE 'INFERRED:%%'

            UNION ALL

            -- VIP safe tier calls with no skip_reason: peak tracking only for analysis
            SELECT
                c.id                AS call_id,
                t.symbol,
                t.mint_address,
                c.mcap_at_call,
                c.conviction_score,
                c.created_at,
                c.source_message_id,
                ch.handle           AS channel_handle,
                o.peak_multiplier,
                TRUE                AS data_only
            FROM calls    c
            JOIN tokens   t  ON t.id      = c.token_id
            JOIN channels ch ON ch.id     = c.channel_id
            JOIN outcomes o  ON o.call_id = c.id
            WHERE ch.handle = 'solhousesignal_vip'
              AND c.vip_tier = 'safe'
              AND c.skip_reason IS NULL
              AND c.created_at > NOW() - INTERVAL '24 hours'
              AND t.mint_resolved = TRUE
              AND t.mint_address IS NOT NULL
              AND t.mint_address NOT LIKE 'UNKNOWN:%%'
              AND t.mint_address NOT LIKE 'INFERRED:%%'

            ORDER BY data_only ASC, conviction_score DESC
            """,
            (max_age_hours, min_score),
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def get_call_peak_info(call_id: int) -> dict | None:
    """Return {peak_multiplier, mcap_at_call, peak_reached_at} for peak tracking."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.mcap_at_call, o.peak_multiplier, o.peak_reached_at
            FROM calls c
            LEFT JOIN outcomes o ON o.call_id = c.id
            WHERE c.id = %s
            """,
            (call_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {"mcap_at_call":    float(row[0]) if row[0] else None,
            "peak_multiplier": float(row[1]) if row[1] else None,
            "peak_reached_at": row[2]}


def update_peak_multiplier(
    call_id: int,
    new_peak: float,
    peak_mult_from_entry: float = None,
) -> bool:
    """
    Conditionally update peak_multiplier — only when new_peak exceeds the stored value.
    Also promotes outcome_label to 'runner' when new_peak >= 2.0 and it isn't already.

    When peak_mult_from_entry is provided, also conditionally updates
    peak_multiplier_from_entry (multiplier relative to actual trading entry price).
    This is only set for calls where a paper position was opened at a price
    different from mcap_at_call (e.g. VIP gamble confirmation trades).

    Returns True if peak_multiplier was updated (new_peak was a genuine new high),
    False if the stored value was already equal or higher.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE outcomes SET
                peak_multiplier = %s,
                peak_reached_at = NOW(),
                updated_at      = NOW()
            WHERE call_id = %s
              AND (peak_multiplier IS NULL OR peak_multiplier < %s)
            """,
            (new_peak, call_id, new_peak),
        )
        updated = cur.rowcount > 0

        if peak_mult_from_entry is not None:
            cur.execute(
                """
                UPDATE outcomes SET
                    peak_multiplier_from_entry = %s,
                    updated_at                 = NOW()
                WHERE call_id = %s
                  AND (peak_multiplier_from_entry IS NULL OR peak_multiplier_from_entry < %s)
                """,
                (peak_mult_from_entry, call_id, peak_mult_from_entry),
            )

        if new_peak >= 2.0:
            cur.execute(
                """
                UPDATE outcomes SET outcome_label = 'runner'
                WHERE call_id = %s
                  AND (outcome_label IS NULL OR outcome_label != 'runner')
                """,
                (call_id,),
            )

        conn.commit()
    return updated


def update_call_conviction_score(call_id: int, score: float) -> None:
    """Write the scorer output back to calls.conviction_score."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE calls SET conviction_score = %s WHERE id = %s",
            (score, call_id),
        )
        conn.commit()


# ── Paper trading helpers ──────────────────────────────────────────────────────

def open_paper_position(
    call_id: int,
    entry_price: float,
    sol_in: float,
    entry_time: datetime | None = None,
    entry_volume: float | None = None,
    is_strategy_b: bool = False,
    vip_tier: str | None = None,
) -> None:
    """
    Open a simulated paper trade position for a scored call.
    Resolves token_id via subquery. No-ops silently if a simulation
    position already exists for this call (prevents duplicates).
    entry_time should be captured by the caller before any async operations
    to avoid exit_time < entry_time race conditions.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trading_positions
                (token_id, call_id, is_simulation, is_strategy_b, entry_price, sol_in,
                 entry_time, entry_volume_h1, vip_tier, status)
            SELECT c.token_id, %s, TRUE, %s, %s, %s, %s, %s, %s, 'open'
            FROM calls c
            WHERE c.id = %s
              AND NOT EXISTS (
                SELECT 1 FROM trading_positions
                WHERE call_id = %s AND is_simulation = TRUE AND is_strategy_b = %s
              )
            """,
            (call_id, is_strategy_b, entry_price, sol_in,
             entry_time or datetime.now(timezone.utc),
             entry_volume, vip_tier, call_id, call_id, is_strategy_b),
        )
        conn.commit()


def get_paper_position_entry_volume(call_id: int, is_strategy_b: bool = False) -> float | None:
    """Return the entry_volume_h1 stored when the paper position was opened."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT entry_volume_h1
            FROM trading_positions
            WHERE call_id = %s
              AND is_simulation = TRUE
              AND is_strategy_b = %s
            """,
            (call_id, is_strategy_b),
        )
        row = cur.fetchone()
        return float(row[0]) if row and row[0] else None


def update_paper_position_entry_volume(call_id: int, volume: float, is_strategy_b: bool = False) -> None:
    """Backfill entry_volume_h1 on first successful volume fetch."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trading_positions
            SET entry_volume_h1 = %s
            WHERE call_id = %s
              AND is_simulation = TRUE
              AND is_strategy_b = %s
              AND entry_volume_h1 IS NULL
            """,
            (volume, call_id, is_strategy_b),
        )
        conn.commit()


def get_open_paper_position(call_id: int, is_strategy_b: bool = False) -> dict | None:
    """
    Return {entry_price, sol_in, entry_time, vip_tier, channel_handle, peak_*} for the open simulation
    position on this call, or None if no open position exists.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tp.entry_price, tp.sol_in, tp.entry_time, tp.vip_tier,
                   ch.handle, tp.peak_mcap, tp.peak_multiplier, tp.peak_at,
                   c.skip_reason
            FROM trading_positions tp
            JOIN calls c ON c.id = tp.call_id
            LEFT JOIN channels ch ON ch.id = c.channel_id
            WHERE tp.call_id = %s
              AND tp.is_simulation = TRUE
              AND tp.is_strategy_b = %s
              AND tp.status = 'open'
            """,
            (call_id, is_strategy_b),
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "entry_price": float(row[0]),
            "sol_in": float(row[1]),
            "entry_time": row[2],
            "vip_tier": row[3],
            "channel_handle": row[4],
            "peak_mcap": float(row[5]) if row[5] is not None else None,
            "peak_multiplier": float(row[6]) if row[6] is not None else None,
            "peak_at": row[7],
            "skip_reason": row[8],   # the lane label, for the lane-testbed exit resolver
        }


def update_paper_position_peak(
    call_id: int,
    current_mcap: float,
    current_mult: float,
    is_strategy_b: bool = False,
) -> bool:
    """
    Persist the highest observed post-entry peak for one open paper position.

    Returns True only when this write established a genuine new per-position peak.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trading_positions
            SET peak_mcap = %s,
                peak_multiplier = %s,
                peak_at = NOW(),
                updated_at = NOW()
            WHERE call_id = %s
              AND is_simulation = TRUE
              AND is_strategy_b = %s
              AND status = 'open'
              AND (peak_multiplier IS NULL OR peak_multiplier < %s)
            """,
            (current_mcap, current_mult, call_id, is_strategy_b, current_mult),
        )
        conn.commit()
        return cur.rowcount > 0


def close_paper_position(
    call_id: int,
    exit_price: float,
    sol_out: float,
    exit_reason: str,
    is_strategy_b: bool = False,
) -> None:
    """Close an open simulation position with exit data."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trading_positions SET
                exit_price  = %s,
                sol_out     = %s,
                exit_time   = NOW(),
                exit_reason = %s,
                status      = 'closed'
            WHERE call_id      = %s
              AND is_simulation = TRUE
              AND is_strategy_b = %s
              AND status        = 'open'
            """,
            (exit_price, sol_out, exit_reason, call_id, is_strategy_b),
        )
        conn.commit()


# ── Shadow paper positions (gated-lane experiment) ─────────────────────────────
# Shadow positions measure the TRUE tradeable PnL of lanes the MAIN strategy skips,
# without touching the A/B numbers. They live in their OWN table (shadow_positions),
# so NO existing query over trading_positions can ever see them — isolation by
# construction. Managed solely by shadow_monitor.py.

def ensure_shadow_positions_table() -> None:
    """
    Create / migrate the shadow_positions table. Idempotent.

    Holds one row per (call_id, exit_variant) so the SAME call can be shadow-traded
    under multiple exit profiles (e.g. 'early' vs 'ride') for head-to-head comparison.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS shadow_positions (
                id              bigserial PRIMARY KEY,
                call_id         integer NOT NULL,
                exit_variant    text NOT NULL DEFAULT 'early',
                token_id        integer NOT NULL,
                vip_tier        text,
                status          text NOT NULL DEFAULT 'open',
                entry_price     numeric NOT NULL,
                sol_in          numeric NOT NULL,
                entry_time      timestamptz NOT NULL DEFAULT now(),
                peak_mcap       numeric,
                peak_multiplier numeric,
                peak_at         timestamptz,
                exit_price      numeric,
                sol_out         numeric,
                exit_time       timestamptz,
                exit_reason     text,
                pnl_sol numeric GENERATED ALWAYS AS (sol_out - sol_in) STORED,
                pnl_pct numeric(8,2) GENERATED ALWAYS AS (
                    CASE WHEN sol_in > 0 THEN round((sol_out - sol_in) / sol_in * 100, 2)
                         ELSE NULL END) STORED,
                created_at      timestamptz NOT NULL DEFAULT now(),
                updated_at      timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        # Migrate older tables (call_id-only unique → composite) idempotently.
        cur.execute("ALTER TABLE shadow_positions ADD COLUMN IF NOT EXISTS exit_variant text NOT NULL DEFAULT 'early'")
        cur.execute("ALTER TABLE shadow_positions ADD COLUMN IF NOT EXISTS entry_volume numeric")
        cur.execute("ALTER TABLE shadow_positions DROP CONSTRAINT IF EXISTS shadow_positions_call_id_key")
        cur.execute(
            """
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'shadow_positions_call_variant_key'
                ) THEN
                    ALTER TABLE shadow_positions
                        ADD CONSTRAINT shadow_positions_call_variant_key UNIQUE (call_id, exit_variant);
                END IF;
            END $$;
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_shadow_status ON shadow_positions (status)")
        conn.commit()


def open_shadow_position(
    call_id: int,
    entry_price: float,
    sol_in: float,
    vip_tier: str | None,
    exit_variant: str = "early",
    entry_volume: float | None = None,
    entry_time: datetime | None = None,
) -> bool:
    """Open a shadow position for a normally-skipped call under one exit variant.
    No-ops if a position for this (call_id, exit_variant) already exists."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO shadow_positions
                (call_id, exit_variant, token_id, vip_tier, entry_price, sol_in, entry_volume, entry_time, status)
            SELECT %s, %s, c.token_id, %s, %s, %s, %s, %s, 'open'
            FROM calls c
            WHERE c.id = %s
            ON CONFLICT (call_id, exit_variant) DO NOTHING
            """,
            (call_id, exit_variant, vip_tier, entry_price, sol_in, entry_volume,
             entry_time or datetime.now(timezone.utc), call_id),
        )
        affected = cur.rowcount
        conn.commit()
        return affected > 0


def get_open_shadow_positions() -> list[dict]:
    """All open shadow positions with the context shadow_monitor needs to manage them."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                sp.call_id, sp.exit_variant, t.symbol, t.mint_address,
                sp.entry_price, sp.sol_in, sp.entry_time, sp.entry_volume,
                sp.vip_tier, sp.peak_mcap, sp.peak_multiplier,
                ch.handle AS channel_handle,
                c.skip_reason
            FROM shadow_positions sp
            JOIN calls    c  ON c.id = sp.call_id
            JOIN tokens   t  ON t.id = sp.token_id
            LEFT JOIN channels ch ON ch.id = c.channel_id
            WHERE sp.status = 'open'
            ORDER BY sp.entry_time DESC
            """,
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def update_shadow_position_peak(call_id: int, exit_variant: str, current_mcap: float, current_mult: float) -> bool:
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE shadow_positions
            SET peak_mcap = %s, peak_multiplier = %s, peak_at = NOW(), updated_at = NOW()
            WHERE call_id = %s AND exit_variant = %s AND status = 'open'
              AND (peak_multiplier IS NULL OR peak_multiplier < %s)
            """,
            (current_mcap, current_mult, call_id, exit_variant, current_mult),
        )
        conn.commit()
        return cur.rowcount > 0


def close_shadow_position(call_id: int, exit_variant: str, exit_price: float, sol_out: float, exit_reason: str) -> None:
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE shadow_positions SET
                exit_price = %s, sol_out = %s, exit_time = NOW(),
                exit_reason = %s, status = 'closed', updated_at = NOW()
            WHERE call_id = %s AND exit_variant = %s AND status = 'open'
            """,
            (exit_price, sol_out, exit_reason, call_id, exit_variant),
        )
        conn.commit()


def get_paper_pnl_summary(is_strategy_b: bool = False) -> dict:
    """
    Return aggregate P&L stats for all closed simulation positions,
    plus open count and exit breakdown by reason.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        # Aggregate stats
        cur.execute(
            """
            SELECT
                COUNT(*)                                          AS total,
                SUM(pnl_sol)                                     AS total_pnl,
                AVG(pnl_pct)                                     AS avg_pnl_pct,
                COUNT(CASE WHEN pnl_sol > 0  THEN 1 END)        AS winners,
                COUNT(CASE WHEN pnl_sol <= 0 THEN 1 END)        AS losers,
                MAX(pnl_pct)                                     AS best_pct,
                MIN(pnl_pct)                                     AS worst_pct
            FROM trading_positions
            WHERE is_simulation = TRUE
              AND is_strategy_b = %s
              AND status = 'closed'
            """,
            (is_strategy_b,),
        )
        row = cur.fetchone()
        summary = {
            "closed":      int(row[0] or 0),
            "total_pnl":   float(row[1] or 0),
            "avg_pnl_pct": float(row[2] or 0),
            "winners":     int(row[3] or 0),
            "losers":      int(row[4] or 0),
            "best_pct":    float(row[5]) if row[5] is not None else None,
            "worst_pct":   float(row[6]) if row[6] is not None else None,
        }

        # Open count
        cur.execute(
            """
            SELECT COUNT(*) FROM trading_positions
            WHERE is_simulation = TRUE AND is_strategy_b = %s AND status = 'open'
            """,
            (is_strategy_b,),
        )
        summary["open"] = int(cur.fetchone()[0])

        # Exit breakdown
        cur.execute(
            """
            SELECT exit_reason, COUNT(*) AS n
            FROM trading_positions
            WHERE is_simulation = TRUE
              AND is_strategy_b = %s
              AND status = 'closed'
            GROUP BY exit_reason
            """,
            (is_strategy_b,),
        )
        breakdown = {"10x_tp": 0, "5x_tp": 0, "3x_tp": 0, "profit_floor": 0, "trail_stop": 0, "hard_stop": 0, "time_stop": 0}
        for reason, count in cur.fetchall():
            if reason in breakdown:
                breakdown[reason] = int(count)
        summary["exit_breakdown"] = breakdown

    return summary


def get_open_paper_positions(is_strategy_b: bool = False) -> list[dict]:
    """
    Return all open simulation positions with symbol and mint_address
    for use in the paper trading report.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                tp.call_id,
                t.symbol,
                t.mint_address,
                tp.entry_price,
                tp.sol_in,
                tp.entry_time,
                tp.peak_mcap,
                tp.peak_multiplier,
                tp.peak_at,
                tp.vip_tier,
                c.skip_reason,
                ch.handle AS channel_handle
            FROM trading_positions tp
            JOIN calls  c ON c.id       = tp.call_id
            JOIN tokens t ON t.id       = c.token_id
            LEFT JOIN channels ch ON ch.id = c.channel_id
            WHERE tp.is_simulation = TRUE
              AND tp.is_strategy_b  = %s
              AND tp.status        = 'open'
            ORDER BY tp.entry_time DESC
            """,
            (is_strategy_b,),
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── Live trading helpers ───────────────────────────────────────────────────────

def open_live_position(
    call_id: int,
    entry_price: float,
    sol_in: float,
    tokens_held: int,
    tx_signature: str,
    router: str | None = None,
) -> None:
    """
    Open a real (non-simulation) trading position.
    No-ops silently if a live position already exists for this call.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO trading_positions
                (token_id, call_id, is_simulation, entry_price, sol_in,
                 tokens_held, tx_signature, router, entry_time, status)
            SELECT c.token_id, %s, FALSE, %s, %s, %s, %s, %s, NOW(), 'open'
            FROM calls c
            WHERE c.id = %s
              AND NOT EXISTS (
                SELECT 1 FROM trading_positions
                WHERE call_id = %s AND is_simulation = FALSE
              )
            """,
            (call_id, entry_price, sol_in, tokens_held, tx_signature, router,
             call_id, call_id),
        )
        conn.commit()


def get_open_live_position(call_id: int) -> dict | None:
    """
    Return the open live position for call_id, or None.
    Includes peak_mcap and peak_multiplier so live exit logic is independent
    of paper position peaks.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tp.entry_price, tp.sol_in, tp.entry_time,
                   tp.tokens_held, t.mint_address, t.symbol,
                   tp.peak_mcap, tp.peak_multiplier, tp.entry_price_fill,
                   c.vip_tier, ch.handle
            FROM trading_positions tp
            JOIN calls  c ON c.id  = tp.call_id
            JOIN tokens t ON t.id  = c.token_id
            LEFT JOIN channels ch ON ch.id = c.channel_id
            WHERE tp.call_id       = %s
              AND tp.is_simulation = FALSE
              AND tp.status        = 'open'
            """,
            (call_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "entry_price":      float(row[0]),
        "sol_in":           float(row[1]),
        "entry_time":       row[2],
        "tokens_held":      int(row[3]) if row[3] else 0,
        "mint_address":     row[4],
        "symbol":           row[5],
        "peak_mcap":        float(row[6]) if row[6] is not None else 0.0,
        "peak_multiplier":  float(row[7]) if row[7] is not None else 0.0,
        "entry_price_fill": float(row[8]) if row[8] is not None else None,
        # vip_tier + channel_handle so check_live_exits can apply the vip_gamble hard-stop
        # (0.30) and the channel-gated profit floor — without these, live silently runs the
        # generic no-floor/0.35 config and won't reproduce the validated `early` exit.
        "vip_tier":         row[9],
        "channel_handle":   row[10],
    }


def update_live_position_peak(
    call_id: int,
    current_mcap: float,
    current_mult: float,
) -> None:
    """Persist the highest observed peak for one open live position."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trading_positions
            SET peak_mcap       = %s,
                peak_multiplier = %s,
                peak_at         = NOW(),
                updated_at      = NOW()
            WHERE call_id       = %s
              AND is_simulation = FALSE
              AND status        = 'open'
              AND (peak_multiplier IS NULL OR peak_multiplier < %s)
            """,
            (current_mcap, current_mult, call_id, current_mult),
        )
        conn.commit()


def get_live_real_peak(call_id: int) -> float | None:
    """
    Best-effort read of the REAL-basis peak (ratcheted off Jupiter sell-quotes, not
    the laggy feed) for one open live position. Returns None if the position is gone,
    the value is unset, or the real_peak_mcap column doesn't exist yet (pre-migration).
    Never raises — a missing column must NOT break the exit path (caller falls back to
    the feed basis). Requires column real_peak_mcap on trading_positions (see the
    Phase-2 quote-driven-exit migration).
    """
    try:
        conn = get_conn()
        safe_rollback()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT real_peak_mcap FROM trading_positions"
                " WHERE call_id = %s AND is_simulation = FALSE AND status = 'open'",
                (call_id,),
            )
            row = cur.fetchone()
        if not row or row[0] is None:
            return None
        return float(row[0])
    except Exception as e:
        safe_rollback()
        print(f"[db] get_live_real_peak skipped (call_id={call_id}): {e}")
        return None


def update_live_real_peak(call_id: int, real_peak_mcap: float) -> None:
    """
    Best-effort ratchet of the REAL-basis peak for one open live position. Only raises
    the stored value (never lowers it), so it is safe to call from both sol-monitor and
    sol-ws-monitor — the DB row is the cross-process shared floor for the real-peak trail.
    Never raises (missing column pre-migration is a silent no-op). See get_live_real_peak.
    """
    if not real_peak_mcap or real_peak_mcap <= 0:
        return
    try:
        conn = get_conn()
        safe_rollback()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE trading_positions
                SET real_peak_mcap = %s,
                    updated_at     = NOW()
                WHERE call_id       = %s
                  AND is_simulation = FALSE
                  AND status        = 'open'
                  AND (real_peak_mcap IS NULL OR real_peak_mcap < %s)
                """,
                (real_peak_mcap, call_id, real_peak_mcap),
            )
            conn.commit()
    except Exception as e:
        safe_rollback()
        print(f"[db] update_live_real_peak skipped (call_id={call_id}): {e}")


def has_open_live_position_for_mint(mint_address: str) -> bool:
    """Check if ANY open live position exists for this mint address."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM trading_positions tp
            JOIN calls  c ON c.id = tp.call_id
            JOIN tokens t ON t.id = c.token_id
            WHERE t.mint_address = %s
              AND tp.is_simulation = FALSE
              AND tp.status = 'open'
            LIMIT 1
            """,
            (mint_address,),
        )
        return cur.fetchone() is not None


def seconds_since_last_live_exit_for_mint(mint_address: str) -> float | None:
    """
    Seconds since the most recent CLOSED live position for this mint exited.

    Returns None if the mint has never had a closed live position. Used to
    enforce a re-entry cooldown so the bot doesn't immediately re-buy a coin
    it just sold (churning fees on the same name).
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXTRACT(EPOCH FROM (now() - tp.exit_time))
            FROM trading_positions tp
            JOIN calls  c ON c.id = tp.call_id
            JOIN tokens t ON t.id = c.token_id
            WHERE t.mint_address = %s
              AND tp.is_simulation = FALSE
              AND tp.status = 'closed'
              AND tp.exit_time IS NOT NULL
            ORDER BY tp.exit_time DESC
            LIMIT 1
            """,
            (mint_address,),
        )
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else None


def has_open_paper_position_for_mint(mint: str, is_strategy_b: bool = False) -> bool:
    """Check if ANY open paper position exists for this mint address."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM trading_positions tp
            JOIN calls  c ON c.id = tp.call_id
            JOIN tokens t ON t.id = c.token_id
            WHERE t.mint_address = %s
              AND tp.is_simulation = TRUE
              AND tp.is_strategy_b = %s
              AND tp.status = 'open'
            LIMIT 1
            """,
            (mint, is_strategy_b),
        )
        return cur.fetchone() is not None


def get_call_id_for_open_mint(mint: str, is_strategy_b: bool = False) -> int | None:
    """Return call_id of open paper position for this mint, or None."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.id FROM trading_positions tp
            JOIN calls  c ON c.id = tp.call_id
            JOIN tokens t ON t.id = c.token_id
            WHERE t.mint_address = %s
              AND tp.is_simulation = TRUE
              AND tp.is_strategy_b = %s
              AND tp.status = 'open'
            LIMIT 1
            """,
            (mint, is_strategy_b),
        )
        row = cur.fetchone()
        return row[0] if row else None


def get_first_call_id_for_mint_on_channel(mint: str, channel_handle: str) -> int | None:
    """Return the earliest call_id seen for this mint on the given channel handle."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.id
            FROM calls c
            JOIN tokens t   ON t.id = c.token_id
            JOIN channels ch ON ch.id = c.channel_id
            WHERE t.mint_address = %s
              AND ch.handle = %s
            ORDER BY c.created_at ASC, c.id ASC
            LIMIT 1
            """,
            (mint, channel_handle),
        )
        row = cur.fetchone()
        return row[0] if row else None


def get_token_onchain_data(mint_address: str) -> dict:
    """Return on-chain security data for a token by mint address."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT security_flag, bundle_pct_remaining,
                   dev_tokens_made, hodl_count, sniper_count,
                   fake_vol_pct, liq_at_detection
            FROM tokens
            WHERE mint_address = %s
            """,
            (mint_address,),
        )
        row = cur.fetchone()
        if not row:
            return {}
        return {
            "security_flag":        row[0],
            "bundle_pct_remaining": float(row[1]) if row[1] is not None else None,
            "dev_tokens_made":      int(row[2])   if row[2] is not None else None,
            "hodl_count":           int(row[3])   if row[3] is not None else None,
            "sniper_count":         int(row[4])   if row[4] is not None else None,
            "fake_vol_pct":         float(row[5]) if row[5] is not None else None,
            "liq_at_detection":     float(row[6]) if row[6] is not None else None,
        }


def get_open_live_positions() -> list[dict]:
    """Return all open live positions with symbol and mint for reporting."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tp.call_id, t.symbol, t.mint_address,
                   tp.entry_price, tp.sol_in, tp.entry_time, tp.tokens_held
            FROM trading_positions tp
            JOIN calls  c ON c.id  = tp.call_id
            JOIN tokens t ON t.id  = c.token_id
            WHERE tp.is_simulation = FALSE
              AND tp.status        = 'open'
            ORDER BY tp.entry_time DESC
            """
        )
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── Volume snapshots ─────────────────────────────────────────────────────────

def ensure_token_volume_snapshots_table() -> None:
    """
    Create the token_volume_snapshots table if it does not already exist.

    This is intentionally lightweight so research scripts can bootstrap the
    table in environments where a formal migration has not been applied yet.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS token_volume_snapshots (
                id BIGSERIAL PRIMARY KEY,
                call_id BIGINT NOT NULL REFERENCES calls(id) ON DELETE CASCADE,
                token_id BIGINT NOT NULL REFERENCES tokens(id) ON DELETE CASCADE,
                mint_address TEXT NOT NULL,
                snapshot_label TEXT NOT NULL,
                minutes_since_call INTEGER NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                age_minutes REAL,
                channel_handle TEXT,
                vip_tier TEXT,
                conviction_score REAL,
                mcap NUMERIC,
                liquidity_usd NUMERIC,
                volume_h1 NUMERIC,
                price_usd NUMERIC,
                source TEXT,
                market_json JSONB,
                UNIQUE (call_id, snapshot_label)
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_token_volume_snapshots_call_id
                ON token_volume_snapshots(call_id)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_token_volume_snapshots_observed_at
                ON token_volume_snapshots(observed_at DESC)
            """
        )
        conn.commit()


# ── Coin holders capture (smart-money bootstrap) ──────────────────────────────
# Snapshot each coin's top holders shortly after the call. Combined with our
# existing coin outcomes (peak_multiplier), this lets us RETROACTIVELY score which
# wallets keep holding winners — bootstrapping a smart-money signal from our own
# data instead of a paid third party. Needs a real (Helius) RPC.

def ensure_coin_holders_table() -> None:
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS coin_holders (
                id            bigserial PRIMARY KEY,
                call_id       integer,
                mint_address  text NOT NULL,
                wallet        text NOT NULL,
                ui_amount     numeric,
                holder_rank   int,
                captured_at   timestamptz NOT NULL DEFAULT now(),
                UNIQUE (call_id, wallet)
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_coin_holders_wallet ON coin_holders (wallet)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_coin_holders_mint ON coin_holders (mint_address)")
        conn.commit()


def insert_coin_holders(call_id: int, mint_address: str, holders: list[dict]) -> int:
    """holders = [{wallet, ui_amount, rank}]. Returns rows inserted."""
    if not holders:
        return 0
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO coin_holders (call_id, mint_address, wallet, ui_amount, holder_rank)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (call_id, wallet) DO NOTHING
            """,
            [(call_id, mint_address, h["wallet"], h.get("ui_amount"), h.get("rank")) for h in holders],
        )
        n = cur.rowcount
        conn.commit()
        return n


def get_calls_needing_holders(
    since_hours: int = 48, limit: int = 50, selection: str = "traded"
) -> list[dict]:
    """Recent calls with no coin_holders snapshot yet."""
    conn = get_conn()
    safe_rollback()
    selection_sql = {
        "traded": """
            AND EXISTS (SELECT 1 FROM trading_positions tp
                        WHERE tp.call_id = c.id AND tp.is_simulation = TRUE)
        """,
        "not_skipped": " AND COALESCE(c.skip_reason,'') IN ('', 'none') ",
        "all": "",
    }.get(selection, "")
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT c.id AS call_id, t.mint_address, t.symbol, c.created_at
            FROM calls c
            JOIN tokens t ON t.id = c.token_id
            WHERE c.created_at >= NOW() - (%s * INTERVAL '1 hour')
              AND t.mint_address IS NOT NULL
              AND t.mint_address NOT LIKE 'UNKNOWN:%%'
              AND t.mint_address NOT LIKE 'INFERRED:%%'
              {selection_sql}
              AND NOT EXISTS (SELECT 1 FROM coin_holders ch WHERE ch.call_id = c.id)
            ORDER BY c.created_at DESC
            LIMIT %s
            """,
            (since_hours, limit),
        )
        return cur.fetchall()


def get_due_volume_snapshot_calls(
    snapshot_minutes: int,
    *,
    since_hours: int = 48,
    limit: int = 500,
    selection: str = "traded",
) -> list[dict]:
    """
    Return recent calls old enough for the requested snapshot offset and not yet
    recorded for that call/snapshot label.
    """
    conn = get_conn()
    safe_rollback()
    snapshot_label = f"t_plus_{snapshot_minutes:02d}m"
    selection_clauses = {
        "traded": """
            AND EXISTS (
                SELECT 1
                FROM trading_positions tp
                WHERE tp.call_id = c.id
                  AND tp.is_simulation = TRUE
                  AND tp.is_strategy_b = FALSE
            )
        """,
        "not_skipped": """
            AND COALESCE(c.skip_reason, '') IN ('', 'none')
        """,
        "all": "",
    }
    if selection not in selection_clauses:
        raise ValueError(f"Unsupported volume snapshot selection: {selection!r}")
    selection_sql = selection_clauses[selection]
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT
                c.id AS call_id,
                c.token_id,
                c.created_at,
                c.vip_tier,
                c.conviction_score,
                t.mint_address,
                t.symbol,
                ch.handle AS channel_handle,
                EXTRACT(EPOCH FROM (NOW() - c.created_at)) / 60.0 AS age_minutes
            FROM calls c
            JOIN tokens t ON t.id = c.token_id
            LEFT JOIN channels ch ON ch.id = c.channel_id
            WHERE c.created_at >= NOW() - (%s * INTERVAL '1 hour')
              AND c.created_at <= NOW() - (%s * INTERVAL '1 minute')
              AND t.mint_address IS NOT NULL
              AND t.mint_address <> ''
              {selection_sql}
              AND NOT EXISTS (
                  SELECT 1
                  FROM token_volume_snapshots tvs
                  WHERE tvs.call_id = c.id
                    AND tvs.snapshot_label = %s
              )
            ORDER BY c.created_at ASC
            LIMIT %s
            """,
            (since_hours, snapshot_minutes, snapshot_label, limit),
        )
        return cur.fetchall()


def insert_token_volume_snapshot(
    *,
    call_id: int,
    token_id: int,
    mint_address: str,
    snapshot_label: str,
    minutes_since_call: int,
    created_at,
    age_minutes: float | None,
    channel_handle: str | None,
    vip_tier: str | None,
    conviction_score: float | None,
    market: dict | None,
) -> bool:
    """
    Insert one token volume snapshot. Returns True only when a row was inserted.
    """
    conn = get_conn()
    safe_rollback()
    market = market or {}
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO token_volume_snapshots (
                call_id,
                token_id,
                mint_address,
                snapshot_label,
                minutes_since_call,
                created_at,
                age_minutes,
                channel_handle,
                vip_tier,
                conviction_score,
                mcap,
                liquidity_usd,
                volume_h1,
                price_usd,
                source,
                market_json
            )
            VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (call_id, snapshot_label) DO NOTHING
            """,
            (
                call_id,
                token_id,
                mint_address,
                snapshot_label,
                minutes_since_call,
                created_at,
                age_minutes,
                channel_handle,
                vip_tier,
                conviction_score,
                market.get("mcap"),
                market.get("liquidity_usd"),
                market.get("volume_h1"),
                market.get("price_usd"),
                market.get("source"),
                Json(market),
            ),
        )
        inserted = cur.rowcount > 0
        conn.commit()
        return inserted


def get_recent_volume_snapshot_counts(days: int = 3) -> list[dict]:
    """Return snapshot coverage counts by label for the last N days of calls."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                snapshot_label,
                COUNT(*) AS snapshots,
                ROUND(AVG(age_minutes)::numeric, 1) AS avg_age_minutes
            FROM token_volume_snapshots
            WHERE created_at >= NOW() - (%s * INTERVAL '1 day')
            GROUP BY snapshot_label
            ORDER BY snapshot_label
            """,
            (days,),
        )
        return cur.fetchall()


# ── Shared cross-process API rate budget ─────────────────────────────────────

def ensure_api_rate_budget_table() -> None:
    """
    Per-minute request counter shared by ALL processes (sol-monitor, sol-ws-monitor,
    sol-listener) so they draw from ONE budget per API key. Without this each process
    kept a private in-memory bucket, so 3 processes each spent the full quota -> ~3x the
    real ceiling -> the recurring Jupiter 429 storms. One row per (bucket, unix-minute).
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS api_rate_budget (
                bucket     TEXT   NOT NULL,
                window_min BIGINT NOT NULL,
                count      INT    NOT NULL DEFAULT 0,
                PRIMARY KEY (bucket, window_min)
            )
            """
        )
    conn.commit()


def rate_budget_try_acquire(bucket: str, limit: int, window_min: int | None = None) -> bool:
    """
    Atomically reserve one request against the shared per-minute budget for `bucket`.
    Returns True if we were under `limit` (reserved -> may fire the request), False if
    at/over it (caller should skip / fall back). Atomic across processes: the conditional
    ON CONFLICT increment only bumps the count while it is below the limit, and RETURNING
    yields a row only when the insert-or-increment actually happened. Raises on DB error so
    the caller can fall back to its per-process in-memory bucket (never blocks trading).
    """
    if window_min is None:
        window_min = int(time.time() // 60)
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO api_rate_budget (bucket, window_min, count)
            VALUES (%s, %s, 1)
            ON CONFLICT (bucket, window_min)
            DO UPDATE SET count = api_rate_budget.count + 1
            WHERE api_rate_budget.count < %s
            RETURNING count
            """,
            (bucket, window_min, limit),
        )
        row = cur.fetchone()
        # First reservation of a new minute (count == 1) -> prune stale windows. Runs ~once
        # per minute per bucket, so the table stays tiny with no per-call overhead.
        if row is not None and row[0] == 1:
            cur.execute("DELETE FROM api_rate_budget WHERE window_min < %s", (window_min - 5,))
    conn.commit()
    return row is not None


# ── Websocket market observations ────────────────────────────────────────────

def ensure_ws_market_observations_table() -> None:
    """
    Create a lightweight audit table for websocket market snapshots.

    These rows let us compare Helius transaction-derived prices against fallback
    market sources without changing the exit behavior itself.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ws_market_observations (
                id BIGSERIAL PRIMARY KEY,
                call_id BIGINT NOT NULL REFERENCES calls(id) ON DELETE CASCADE,
                mint_address TEXT NOT NULL,
                signature TEXT,
                source TEXT,
                price_usd NUMERIC,
                mcap NUMERIC,
                liquidity_usd NUMERIC,
                volume_h1 NUMERIC,
                observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                market_json JSONB
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ws_market_observations_call_id
                ON ws_market_observations(call_id, observed_at DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ws_market_observations_signature
                ON ws_market_observations(signature)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_ws_market_observations_observed_at
                ON ws_market_observations(observed_at DESC)
            """
        )
        conn.commit()


def insert_ws_market_observation(
    *,
    call_id: int,
    mint_address: str,
    signature: str | None,
    market: dict,
) -> None:
    """Persist one websocket market snapshot for later exit-path analysis."""
    conn = get_conn()
    safe_rollback()
    market = market or {}
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ws_market_observations (
                call_id,
                mint_address,
                signature,
                source,
                price_usd,
                mcap,
                liquidity_usd,
                volume_h1,
                market_json
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                call_id,
                mint_address,
                signature,
                market.get("source") or market.get("price_source"),
                market.get("price_usd"),
                market.get("mcap"),
                market.get("liquidity_usd"),
                market.get("volume_h1"),
                Json(market),
            ),
        )
        conn.commit()


def get_recent_ws_market_observation_counts(hours: int = 24) -> list[dict]:
    """Return recent websocket market observation counts by source."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            """
            SELECT
                COALESCE(source, 'unknown') AS source,
                COUNT(*) AS observations,
                COUNT(DISTINCT call_id) AS calls,
                ROUND(AVG(mcap)::numeric, 2) AS avg_mcap,
                MAX(observed_at) AS last_seen
            FROM ws_market_observations
            WHERE observed_at >= NOW() - (%s * INTERVAL '1 hour')
            GROUP BY COALESCE(source, 'unknown')
            ORDER BY observations DESC
            """,
            (hours,),
        )
        return cur.fetchall()


def close_live_position_db(
    call_id: int,
    exit_price: float,
    sol_out: float,
    exit_reason: str,
    tx_signature: str | None,
) -> None:
    """Update a live position to closed with exit data."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE trading_positions SET
                exit_price   = %s,
                sol_out      = %s,
                exit_time    = NOW(),
                exit_reason  = %s,
                tx_signature = COALESCE(%s, tx_signature),
                status       = 'closed'
            WHERE call_id       = %s
              AND is_simulation  = FALSE
              AND status         = 'open'
            """,
            (exit_price, sol_out, exit_reason, tx_signature, call_id),
        )
        conn.commit()


def set_live_fill_price(
    call_id: int,
    entry_price_fill: float | None = None,
    exit_price_fill: float | None = None,
) -> None:
    """
    Best-effort: record the TRUE fill-derived mcap (from the real SOL<->token
    exchange) alongside the feed-based entry_price/exit_price, for phantom-price
    auditing. Never raises — a missing column (pre-migration) or bad value must
    NOT affect trading. Requires columns entry_price_fill / exit_price_fill on
    trading_positions (see ALTER TABLE in the fill-price migration).
    """
    sets, params = [], []
    if entry_price_fill is not None:
        sets.append("entry_price_fill = %s")
        params.append(entry_price_fill)
    if exit_price_fill is not None:
        sets.append("exit_price_fill = %s")
        params.append(exit_price_fill)
    if not sets:
        return
    params.append(call_id)
    try:
        conn = get_conn()
        safe_rollback()
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE trading_positions SET {', '.join(sets)} "
                f"WHERE call_id = %s AND is_simulation = FALSE",
                params,
            )
            conn.commit()
    except Exception as e:
        safe_rollback()
        print(f"[db] set_live_fill_price skipped (call_id={call_id}): {e}")


def get_live_positions_count() -> int:
    """Count of currently open live positions."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM trading_positions"
            " WHERE is_simulation = FALSE AND status = 'open'"
        )
        return int(cur.fetchone()[0])


def get_today_live_losses() -> float:
    """
    Net loss (SOL) on closed live positions today.
    Returns a positive number only when the day is net negative.
    Winners offset losers — the breaker trips on wallet drawdown, not gross losses.
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT GREATEST(0, COALESCE(-SUM(pnl_sol), 0))
            FROM trading_positions
            WHERE is_simulation = FALSE
              AND status        = 'closed'
              AND exit_time     >= CURRENT_DATE
            """
        )
        return float(cur.fetchone()[0])


def get_live_pnl_summary() -> dict:
    """Aggregate P&L stats for all closed live positions."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)                                          AS total,
                SUM(pnl_sol)                                     AS total_pnl,
                AVG(pnl_pct)                                     AS avg_pnl_pct,
                COUNT(CASE WHEN pnl_sol > 0  THEN 1 END)        AS winners,
                COUNT(CASE WHEN pnl_sol <= 0 THEN 1 END)        AS losers
            FROM trading_positions
            WHERE is_simulation = FALSE
              AND status        = 'closed'
            """
        )
        row = cur.fetchone()
        summary = {
            "closed":      int(row[0] or 0),
            "total_pnl":   float(row[1] or 0),
            "avg_pnl_pct": float(row[2] or 0),
            "winners":     int(row[3] or 0),
            "losers":      int(row[4] or 0),
        }

        cur.execute(
            "SELECT COUNT(*) FROM trading_positions"
            " WHERE is_simulation = FALSE AND status = 'open'"
        )
        summary["open"] = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT exit_reason, COUNT(*) AS n
            FROM trading_positions
            WHERE is_simulation = FALSE AND status = 'closed'
            GROUP BY exit_reason
            """
        )
        breakdown = {
            "10x_tp": 0, "5x_tp": 0, "3x_tp": 0,
            "trail_stop": 0, "hard_stop": 0, "time_stop": 0,
        }
        for reason, count in cur.fetchall():
            if reason in breakdown:
                breakdown[reason] = int(count)
        summary["exit_breakdown"] = breakdown

    return summary


# ── Daily summary queries ──────────────────────────────────────────────────────

def get_daily_signal_summary() -> list[dict]:
    """Count today's calls grouped by score label."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
              CASE WHEN conviction_score >= 85 THEN 'strong_alert'
                   WHEN conviction_score >= 70 THEN 'alert'
                   WHEN conviction_score >= 55 THEN 'caution'
                   WHEN conviction_score >= 40 THEN 'watch'
                   ELSE 'skip' END AS label,
              COUNT(*) AS count
            FROM calls
            WHERE created_at >= CURRENT_DATE
              AND conviction_score IS NOT NULL
            GROUP BY label
            ORDER BY count DESC
            """
        )
        return [{"label": r[0], "count": int(r[1])} for r in cur.fetchall()]


def _daily_position_summary(is_simulation: bool) -> dict:
    """Shared query logic for paper and live daily summaries."""
    conn = get_conn()
    safe_rollback()
    sim = is_simulation
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT COUNT(*) FROM trading_positions
            WHERE is_simulation = %s
              AND entry_time >= CURRENT_DATE
            """,
            (sim,),
        )
        opened = int(cur.fetchone()[0])

        cur.execute(
            """
            SELECT
                COUNT(*)                                   AS total,
                COUNT(CASE WHEN pnl_sol > 0  THEN 1 END)  AS winners,
                COUNT(CASE WHEN pnl_sol <= 0 THEN 1 END)  AS losers,
                COALESCE(SUM(pnl_sol), 0)                 AS daily_pnl
            FROM trading_positions
            WHERE is_simulation = %s
              AND status    = 'closed'
              AND exit_time >= CURRENT_DATE
            """,
            (sim,),
        )
        row = cur.fetchone()

        cur.execute(
            """
            SELECT t.symbol, tp.pnl_pct
            FROM trading_positions tp
            JOIN calls  c ON c.id = tp.call_id
            JOIN tokens t ON t.id = c.token_id
            WHERE tp.is_simulation = %s
              AND tp.status    = 'closed'
              AND tp.exit_time >= CURRENT_DATE
              AND tp.pnl_pct IS NOT NULL
            ORDER BY tp.pnl_pct DESC
            LIMIT 1
            """,
            (sim,),
        )
        best = cur.fetchone()

        cur.execute(
            """
            SELECT t.symbol, tp.pnl_pct
            FROM trading_positions tp
            JOIN calls  c ON c.id = tp.call_id
            JOIN tokens t ON t.id = c.token_id
            WHERE tp.is_simulation = %s
              AND tp.status    = 'closed'
              AND tp.exit_time >= CURRENT_DATE
              AND tp.pnl_pct IS NOT NULL
            ORDER BY tp.pnl_pct ASC
            LIMIT 1
            """,
            (sim,),
        )
        worst = cur.fetchone()

    return {
        "opened":       opened,
        "closed":       int(row[0]),
        "winners":      int(row[1]),
        "losers":       int(row[2]),
        "daily_pnl":    float(row[3]),
        "best_symbol":  best[0]        if best  else None,
        "best_pct":     float(best[1]) if best  else None,
        "worst_symbol": worst[0]       if worst else None,
        "worst_pct":    float(worst[1]) if worst else None,
    }


def get_daily_paper_summary() -> dict:
    """Paper trading stats for today."""
    return _daily_position_summary(is_simulation=True)


def get_daily_live_summary() -> dict:
    """Live trading stats for today. Returns all-zeros dict if no live trades."""
    return _daily_position_summary(is_simulation=False)


def get_running_totals() -> dict:
    """All-time totals for paper and live trading."""
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*)                                  AS total,
                COUNT(CASE WHEN pnl_sol > 0 THEN 1 END)  AS winners,
                COALESCE(SUM(pnl_sol), 0)                AS total_pnl
            FROM trading_positions
            WHERE is_simulation = TRUE
              AND status = 'closed'
            """
        )
        p = cur.fetchone()

        cur.execute(
            """
            SELECT
                COUNT(*)                                  AS total,
                COUNT(CASE WHEN pnl_sol > 0 THEN 1 END)  AS winners,
                COALESCE(SUM(pnl_sol), 0)                AS total_pnl
            FROM trading_positions
            WHERE is_simulation = FALSE
              AND status = 'closed'
            """
        )
        lv = cur.fetchone()

    paper_total = int(p[0])
    live_total  = int(lv[0])
    return {
        "paper_trades":   paper_total,
        "paper_win_rate": round(int(p[1])  / paper_total * 100, 1) if paper_total else 0.0,
        "paper_pnl":      float(p[2]),
        "live_trades":    live_total,
        "live_win_rate":  round(int(lv[1]) / live_total  * 100, 1) if live_total  else 0.0,
        "live_pnl":       float(lv[2]),
    }


def get_hourly_performance(since=None) -> list:
    """
    Hourly win rate and P&L for paper trades (UTC hour).

    Returns a list of dicts sorted by hour:
        {hour_utc, total, winners, win_rate, pnl}

    Optionally filtered to rows with exit_time >= since (a datetime).
    """
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        date_filter = ""
        params: list = [True]
        if since:
            date_filter = "AND exit_time >= %s"
            params.append(since)

        cur.execute(
            f"""
            SELECT
                EXTRACT(HOUR FROM exit_time AT TIME ZONE 'UTC')::int AS hour_utc,
                COUNT(*)                                              AS total,
                COUNT(CASE WHEN pnl_sol > 0 THEN 1 END)             AS winners,
                COALESCE(SUM(pnl_sol), 0)                           AS pnl
            FROM trading_positions
            WHERE is_simulation = %s
              AND status = 'closed'
              {date_filter}
            GROUP BY 1
            ORDER BY 1
            """,
            params,
        )
        rows = cur.fetchall()

    result = []
    for row in rows:
        total = int(row[1])
        result.append({
            "hour_utc": int(row[0]),
            "total":    total,
            "winners":  int(row[2]),
            "win_rate": round(int(row[2]) / total * 100, 1) if total else 0.0,
            "pnl":      float(row[3]),
        })
    return result


def set_call_skip_reason(call_id: int, reason: str) -> None:
    """
    Record why a call was not traded.

    Only writes if skip_reason is currently NULL — first skip reason wins.
    Silently no-ops if call_id is falsy or the UPDATE affects 0 rows.
    """
    if not call_id:
        return
    try:
        conn = get_conn()
        safe_rollback()
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE calls
                SET skip_reason = %s
                WHERE id = %s
                  AND skip_reason IS NULL
                """,
                (reason, call_id),
            )
            conn.commit()
    except Exception as e:
        safe_rollback()
        print(f"[db] set_call_skip_reason failed call_id={call_id} reason={reason}: {e}")


def get_call_skip_reason(call_id: int) -> str | None:
    """Return the current skip_reason for a call, or None if unset/missing."""
    if not call_id:
        return None
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT skip_reason
            FROM calls
            WHERE id = %s
            """,
            (call_id,),
        )
        row = cur.fetchone()
        return row[0] if row else None


def has_paper_position_for_call(call_id: int, is_strategy_b: bool = False) -> bool:
    """Return True if any paper position row exists for this call/strategy."""
    if not call_id:
        return False
    conn = get_conn()
    safe_rollback()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM trading_positions
            WHERE call_id = %s
              AND is_simulation = TRUE
              AND is_strategy_b = %s
            LIMIT 1
            """,
            (call_id, is_strategy_b),
        )
        return cur.fetchone() is not None
