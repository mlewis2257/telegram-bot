import os
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from dotenv import load_dotenv

load_dotenv()

# ── Connection ────────────────────────────────────────────────────────────────

_conn = None


def get_conn():
    """Lazy singleton connection. Re-opens if closed or broken."""
    global _conn
    if _conn is None or _conn.closed:
        _conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=os.environ["DB_PORT"],
            dbname=os.environ["DB_NAME"],
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
        )
    return _conn


def close_conn():
    global _conn
    if _conn and not _conn.closed:
        _conn.close()


# ── Channels ──────────────────────────────────────────────────────────────────

def get_active_channels():
    """
    Return all active channels from the channels table.
    Each row is a dict: {id, handle, platform, channel_type, weight}
    """
    conn = get_conn()
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


# ── Callers ───────────────────────────────────────────────────────────────────

def upsert_caller(platform: str, handle: str, name: str) -> int:
    """
    Insert a new caller or return the existing one's id.
    Updates the display name if the record already exists.
    """
    conn = get_conn()
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
) -> int | None:
    """
    Insert a call event. Returns the new call id, or None if the message
    was already logged (dedup on source_platform + source_message_id).
    """
    conn = get_conn()
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO calls
                (caller_id, token_id, call_type, source_platform,
                 source_message_id, raw_message, created_at,
                 message_type, channel_id, mcap_at_call, narrative_tags)
            VALUES (%s, %s, 'reactive', %s, %s, %s, %s, %s, %s, %s, %s)
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
            ),
        )
        conn.commit()
        row = cur.fetchone()
        return row[0] if row else None


# ── Token / call lookups ──────────────────────────────────────────────────────

def get_call_id_by_token_name(token_name: str) -> int | None:
    """
    Look up the most recent call_id whose token symbol or name matches
    token_name (case-insensitive). Used to link milestone updates back to
    the original call.
    """
    conn = get_conn()
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


def get_token_id_by_symbol(symbol: str) -> int | None:
    """
    Look up the most recent token_id by symbol (case-insensitive).
    Used to link whale alerts to a known token.
    """
    conn = get_conn()
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
