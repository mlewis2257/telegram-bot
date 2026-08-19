-- =============================================================================
-- solana_signals — PostgreSQL Schema
-- =============================================================================
-- Tables (in dependency order):
--   1. callers           — call sources (Telegram channels, Twitter accounts)
--   2. tokens            — unique Solana token mints
--   3. channels          — monitored channel config (type, weight, platform)
--   4. calls             — detected call events (reactive + predictive)
--   5. outcomes          — price performance backfill (1h / 4h / 24h)
--   6. whale_alerts      — whale wallet activity on tracked tokens
--   7. trading_positions — future auto-trading agent state
--   8. news_sources      — monitored RSS/API news feeds
--   9. news_events       — individual news items with meme potential scores
--  10. trending_topics   — Twitter/platform trends with narrative cross-refs
-- =============================================================================


-- -----------------------------------------------------------------------------
-- 1. callers
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS callers (
    id               SERIAL PRIMARY KEY,
    name             TEXT        NOT NULL,                         -- display name
    platform         TEXT        NOT NULL,                         -- telegram | twitter | discord
    handle           TEXT        NOT NULL,                         -- @handle or channel username
    total_calls      INTEGER     NOT NULL DEFAULT 0,
    win_rate_1h      NUMERIC(5,2),                                 -- % calls that 2x'd in 1h
    win_rate_4h      NUMERIC(5,2),                                 -- % calls that 2x'd in 4h
    win_rate_24h     NUMERIC(5,2),                                 -- % calls that 2x'd in 24h
    reputation_score NUMERIC(5,2),                                 -- 0–100, updated by AI layer
    is_active        BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- No two callers share the same handle on the same platform
CREATE UNIQUE INDEX IF NOT EXISTS idx_callers_platform_handle
    ON callers (platform, handle);


-- -----------------------------------------------------------------------------
-- 2. tokens
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS tokens (
    id                 SERIAL PRIMARY KEY,
    mint_address       TEXT        NOT NULL UNIQUE,    -- Solana mint pubkey
    symbol             TEXT,
    name               TEXT,
    decimals           INTEGER,
    total_supply       NUMERIC,
    token_age_seconds  INTEGER,                        -- age at first detection (Helius)
    lp_locked          BOOLEAN,
    lp_lock_pct        NUMERIC(5,2),                  -- % of LP that is locked
    holder_count       INTEGER,                        -- at first detection
    top_10_holder_pct  NUMERIC(5,2),                  -- concentration risk
    deployer_address   TEXT,                           -- wallet that deployed the token
    pool_address           TEXT,                       -- primary Raydium / pump.fun pool
    dexscreener_paid       BOOLEAN     NOT NULL DEFAULT FALSE,
    dexscreener_paid_at    TIMESTAMPTZ,               -- when we first saw the paid alert
    boost_count            INTEGER     NOT NULL DEFAULT 0,
    last_boost_at          TIMESTAMPTZ,               -- most recent Lightning Boost alert

    -- Realtime entry signal metadata (from @solwhaletrending / @solearlytrending)
    token_age_minutes      INTEGER,                   -- age at detection in minutes
    security_flag          TEXT,                      -- 'safe' | 'warning' | 'unknown'
    is_cto                 BOOLEAN     NOT NULL DEFAULT FALSE,
    bundle_count           INTEGER,
    bundle_pct_initial     NUMERIC(5,2),
    bundle_pct_remaining   NUMERIC(5,2),
    sniper_count           INTEGER,
    sniper_pct_initial     NUMERIC(5,2),
    sniper_pct_remaining   NUMERIC(5,2),
    first_20_pct           NUMERIC(5,2),
    dev_sol_held           NUMERIC,
    dev_pct_held           NUMERIC(5,2),
    dev_sold               BOOLEAN,
    bundled_pct            NUMERIC(5,2),
    bundled_sold_pct       NUMERIC(5,2),
    hodl_count             INTEGER,
    liq_at_detection       NUMERIC,
    vol_1h_at_detection    NUMERIC,
    detecting_wallet_sol   NUMERIC,                   -- SOL in wallet that triggered alert
    detecting_sol_spent    NUMERIC,                   -- SOL the whale spent on entry

    -- Dev history (from "Made: N | Bond: N | Best: $NM" line)
    dev_tokens_made        INTEGER,                   -- serial deployer flag: >100 bad, >1000 extreme
    dev_bonds              INTEGER,                   -- number of bonding curve interactions
    dev_best_mcap          NUMERIC,                   -- peak mcap of dev's best previous token
    dev_sold_pct           NUMERIC(5,2),              -- % of supply dev has sold since launch

    -- Fake volume signal
    fake_vol_usd           NUMERIC,
    fake_vol_pct           NUMERIC(5,2),

    -- Mint resolution flag (FALSE = synthetic UNKNOWN: mint, pending resolution)
    mint_resolved          BOOLEAN     NOT NULL DEFAULT TRUE,

    first_seen_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tokens_first_seen_at
    ON tokens (first_seen_at);


-- -----------------------------------------------------------------------------
-- 3. channels
-- -----------------------------------------------------------------------------
-- One row per monitored source. Adding a new channel = inserting a row only.
-- The scraper reads this table at startup and routes each message accordingly.
CREATE TABLE IF NOT EXISTS channels (
    id           SERIAL PRIMARY KEY,
    handle       TEXT        NOT NULL,
    platform     TEXT        NOT NULL CHECK (platform IN ('telegram', 'twitter', 'discord')),
    channel_type TEXT        NOT NULL CHECK (channel_type IN ('lagging', 'realtime')),
    weight       NUMERIC(3,2) NOT NULL DEFAULT 1.0
                              CHECK (weight >= 0.0 AND weight <= 1.0),
    is_active    BOOLEAN     NOT NULL DEFAULT TRUE,
    added_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_channels_platform_handle
    ON channels (platform, handle);

-- Seed: channels
INSERT INTO channels (handle, platform, channel_type, weight) VALUES
    ('solhousesignal',   'telegram', 'lagging',  0.6),
    ('solwhaletrending', 'telegram', 'realtime', 1.0),
    ('solearlytrending', 'telegram', 'realtime', 1.0)
ON CONFLICT (platform, handle) DO NOTHING;


-- -----------------------------------------------------------------------------
-- 4. calls
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS calls (
    id                SERIAL PRIMARY KEY,
    caller_id         INTEGER     REFERENCES callers(id) ON DELETE SET NULL,
    token_id          INTEGER     NOT NULL REFERENCES tokens(id) ON DELETE RESTRICT,
    call_type         TEXT        NOT NULL CHECK (call_type IN ('reactive', 'predictive')),
    source_platform   TEXT        NOT NULL,            -- telegram | twitter | news
    source_message_id TEXT,                            -- platform-native ID (dedup key)
    raw_message       TEXT,                            -- full original message text
    narrative_tags    TEXT[],                          -- e.g. {ai, trump, dog}
    price_at_call     NUMERIC,                         -- DexScreener at call time
    mcap_at_call      NUMERIC,                         -- DexScreener at call time
    liquidity_at_call NUMERIC,                         -- DexScreener at call time
    message_type      TEXT CHECK (message_type IN ('lagging_call', 'initial_call', 'update', 'inferred_call')),
    channel_id        INTEGER     REFERENCES channels(id) ON DELETE SET NULL,
    conviction_score  NUMERIC(5,2),                   -- 0–100, scoring engine output
    alert_sent        BOOLEAN     NOT NULL DEFAULT FALSE,
    alert_sent_at     TIMESTAMPTZ,
    skip_reason       TEXT CHECK (skip_reason IN (
        'slippage', 'quiet_hours', 'low_score', 'duplicate',
        'balance', 'allowed_hours', 'security_warning',
        'mcap_too_high', 'no_data', 'dex_circuit_open', 'vip_mcap_gate',
        'momentum_dump', 'mcap_too_low', 'unconfirmed', 'vip_paused',
        'high_bundle', 'serial_rugger', 'low_quality_bucket',
        'vip_low_score', 'no_entry_mcap', 'vip_mcap_too_low',
        'high_fake_vol', 'no_base_position', 'pending_duplicate',
        'paper_open_failed', 'blocked_channel', 'reentry_cooldown',
        'shadow_only', 'vip_missing_tier', 'vip_unhandled_tier',
        'vip_route_fallthrough', 'vip_gamble_allowed_hours',
        'vip_safe_allowed_hours', 'vip_gamble_weak_pocket',
        'free_allowed_bucket', 'free_blocked_hour', 'free_weak_pocket',
        'high_holders', 'unsupported_channel', 'paper_dispatch_fallthrough'
    )),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_calls_token_id
    ON calls (token_id);

CREATE INDEX IF NOT EXISTS idx_calls_caller_id
    ON calls (caller_id);

CREATE INDEX IF NOT EXISTS idx_calls_created_at
    ON calls (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_calls_call_type
    ON calls (call_type);

CREATE INDEX IF NOT EXISTS idx_calls_channel_id
    ON calls (channel_id);

CREATE INDEX IF NOT EXISTS idx_calls_message_type
    ON calls (message_type);

-- Partial unique index: only deduplicate when source_message_id is present
CREATE UNIQUE INDEX IF NOT EXISTS idx_calls_source_dedup
    ON calls (source_platform, source_message_id)
    WHERE source_message_id IS NOT NULL;


-- -----------------------------------------------------------------------------
-- 5. outcomes
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS outcomes (
    id                 SERIAL PRIMARY KEY,
    call_id            INTEGER     NOT NULL UNIQUE REFERENCES calls(id) ON DELETE CASCADE,

    -- 1-hour interval
    price_1h           NUMERIC,
    mcap_1h            NUMERIC,
    pct_change_1h      NUMERIC(8,2),                  -- relative to price_at_call
    backfilled_1h_at   TIMESTAMPTZ,

    -- 4-hour interval
    price_4h           NUMERIC,
    mcap_4h            NUMERIC,
    pct_change_4h      NUMERIC(8,2),
    backfilled_4h_at   TIMESTAMPTZ,

    -- 24-hour interval
    price_24h          NUMERIC,
    mcap_24h           NUMERIC,
    pct_change_24h     NUMERIC(8,2),
    backfilled_24h_at  TIMESTAMPTZ,

    -- Channel-reported result (Type A / lagging calls only)
    -- Kept separate from DexScreener backfill columns above
    mcap_at_result       NUMERIC,                     -- mcap the channel reported at posting time
    result_reported_at   TIMESTAMPTZ,                 -- when the channel posted the result message
    stated_multiplier    NUMERIC(8,2),                -- parsed from "2x ACHIEVED" text (channel's claim)

    -- Peak tracking — meme coins often spike and dump within a single interval window
    -- peak_multiplier is computed: result_mcap / entry_mcap (more accurate than stated)
    peak_price         NUMERIC,
    peak_multiplier    NUMERIC(8,2),                  -- computed: mcap_at_result / mcap_at_call
    peak_reached_at    TIMESTAMPTZ,

    -- Set once all three intervals are filled
    outcome_label      TEXT CHECK (outcome_label IN ('runner', 'rug', 'flat', 'slow_bleed', 'pumped_and_dumped')),

    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_outcomes_outcome_label
    ON outcomes (outcome_label);

-- Partial indexes for the backfill job:
-- "give me all calls that have passed the 1h window but aren't filled yet"
CREATE INDEX IF NOT EXISTS idx_outcomes_pending_1h
    ON outcomes (call_id)
    WHERE backfilled_1h_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_outcomes_pending_4h
    ON outcomes (call_id)
    WHERE backfilled_4h_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_outcomes_pending_24h
    ON outcomes (call_id)
    WHERE backfilled_24h_at IS NULL;


-- -----------------------------------------------------------------------------
-- 6. whale_alerts
-- -----------------------------------------------------------------------------
-- Whale wallet activity detected on tracked tokens.
-- call_id is nullable — a whale may buy before the call is posted.
-- backfill_whale_alert_call_ids() links them retroactively when a call is logged.
CREATE TABLE IF NOT EXISTS whale_alerts (
    id               SERIAL PRIMARY KEY,
    call_id          INTEGER     REFERENCES calls(id)  ON DELETE SET NULL,
    token_id         INTEGER     REFERENCES tokens(id) ON DELETE SET NULL,
    symbol           TEXT,
    sol_amount       NUMERIC,                          -- SOL spent by whale (e.g. 1.27)
    wallet_size_sol  NUMERIC,                          -- total wallet size (e.g. 125)
    mcap_at_whale    NUMERIC,                          -- mcap at time of whale buy
    signal_count     INTEGER,                          -- which signal # for this token (1, 2, 3…)
    raw_message      TEXT,
    detected_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_whale_alerts_token_id
    ON whale_alerts (token_id);

CREATE INDEX IF NOT EXISTS idx_whale_alerts_call_id
    ON whale_alerts (call_id);

CREATE INDEX IF NOT EXISTS idx_whale_alerts_detected_at
    ON whale_alerts (detected_at DESC);

-- Partial index — unlinked alerts waiting for a call to be logged
CREATE INDEX IF NOT EXISTS idx_whale_alerts_unlinked
    ON whale_alerts (token_id)
    WHERE call_id IS NULL;


-- -----------------------------------------------------------------------------
-- 7. trading_positions
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trading_positions (
    id               SERIAL PRIMARY KEY,
    call_id          INTEGER     REFERENCES calls(id) ON DELETE SET NULL,
    token_id         INTEGER     NOT NULL REFERENCES tokens(id) ON DELETE RESTRICT,
    status           TEXT        NOT NULL DEFAULT 'open'
                                 CHECK (status IN ('open', 'closing', 'closed', 'cancelled')),

    -- Entry
    entry_price      NUMERIC     NOT NULL,             -- SOL per token at buy
    entry_mcap       NUMERIC,
    entry_tx_hash    TEXT,                             -- Solana tx signature
    entry_time       TIMESTAMPTZ,
    sol_in           NUMERIC     NOT NULL,             -- SOL spent
    tokens_received  NUMERIC,
    tokens_held      BIGINT,                            -- raw token units held for live sells
    tx_signature     TEXT,                              -- most recent live swap signature
    router           TEXT,                              -- swap router used by Jupiter
    entry_price_fill NUMERIC,                           -- fill-implied entry mcap
    peak_mcap        NUMERIC,                          -- highest observed mcap after entry
    peak_multiplier  NUMERIC(10,4),                   -- highest observed multiplier from entry
    peak_at          TIMESTAMPTZ,                      -- when peak_mcap / peak_multiplier were seen
    real_peak_mcap   DOUBLE PRECISION,                 -- sell-quote ratcheted peak for live exits

    -- Exit
    exit_price       NUMERIC,                          -- SOL per token at sell
    exit_price_fill  NUMERIC,                          -- fill-implied exit mcap
    exit_mcap        NUMERIC,
    exit_tx_hash     TEXT,
    exit_time        TIMESTAMPTZ,
    sol_out          NUMERIC,                          -- SOL received on exit

    -- Safety flag — TRUE by default so nothing goes live accidentally
    is_simulation    BOOLEAN     NOT NULL DEFAULT TRUE,

    -- P&L — both generated; NULL while position is open (sol_out IS NULL)
    pnl_sol          NUMERIC     GENERATED ALWAYS AS (sol_out - sol_in) STORED,
    pnl_pct          NUMERIC(8,2) GENERATED ALWAYS AS (
                         CASE WHEN sol_in > 0
                         THEN ROUND(((sol_out - sol_in) / sol_in * 100)::numeric, 2)
                         ELSE NULL END
                     ) STORED,

    exit_reason      TEXT CHECK (exit_reason IN (
                         'take_profit', 'stop_loss', 'manual', 'timeout', 'rug',
                         '3x_tp', '5x_tp', '10x_tp', 'profit_floor',
                         'trail_stop', 'hard_stop', 'time_stop', 'data_error'
                     )),

    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_positions_call_id
    ON trading_positions (call_id);

CREATE INDEX IF NOT EXISTS idx_positions_token_id
    ON trading_positions (token_id);

-- Include is_simulation so simulation vs live performance can be queried separately
CREATE INDEX IF NOT EXISTS idx_positions_status
    ON trading_positions (status, is_simulation);

CREATE INDEX IF NOT EXISTS idx_positions_entry_time
    ON trading_positions (entry_time DESC);


-- -----------------------------------------------------------------------------
-- 8. news_sources
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS news_sources (
    id             SERIAL PRIMARY KEY,
    name           TEXT        NOT NULL,
    url            TEXT        NOT NULL UNIQUE,
    source_type    TEXT        NOT NULL CHECK (source_type IN ('rss', 'api', 'scrape')),
    category       TEXT        NOT NULL CHECK (category IN (
                       'politics', 'crypto', 'sports',
                       'entertainment', 'general', 'geopolitical'
                   )),
    is_active      BOOLEAN     NOT NULL DEFAULT TRUE,
    last_fetched_at TIMESTAMPTZ,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed: pre-populated news sources
-- ON CONFLICT DO NOTHING makes this safe to re-run
INSERT INTO news_sources (name, url, source_type, category) VALUES
    ('CoinDesk',         'https://www.coindesk.com/arc/outboundfeeds/rss/', 'rss', 'crypto'),
    ('CoinTelegraph',    'https://cointelegraph.com/rss',                   'rss', 'crypto'),
    ('Decrypt',          'https://decrypt.co/feed',                         'rss', 'crypto'),
    ('The Block',        'https://www.theblock.co/rss.xml',                 'rss', 'crypto'),
    ('Reuters Politics', 'https://feeds.reuters.com/Reuters/PoliticsNews',  'rss', 'politics'),
    ('Politico',         'https://www.politico.com/rss/politicopicks.xml',  'rss', 'politics'),
    ('BBC News',         'https://feeds.bbci.co.uk/news/rss.xml',           'rss', 'general')
ON CONFLICT (url) DO NOTHING;


-- -----------------------------------------------------------------------------
-- 9. news_events
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS news_events (
    id                   SERIAL PRIMARY KEY,
    source_id            INTEGER     REFERENCES news_sources(id) ON DELETE SET NULL,
    headline             TEXT        NOT NULL,
    summary              TEXT,
    url                  TEXT,
    published_at         TIMESTAMPTZ,
    detected_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    narrative_tags       TEXT[],                        -- extracted keywords/themes
    meme_potential_score NUMERIC(5,2),                 -- 0–100, AI-rated
    related_call_ids     INTEGER[],                     -- calls that matched this narrative
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_news_events_source_id
    ON news_events (source_id);

CREATE INDEX IF NOT EXISTS idx_news_events_published_at
    ON news_events (published_at DESC);

CREATE INDEX IF NOT EXISTS idx_news_events_meme_potential
    ON news_events (meme_potential_score DESC)
    WHERE meme_potential_score IS NOT NULL;


-- -----------------------------------------------------------------------------
-- 10. trending_topics
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS trending_topics (
    id                     SERIAL PRIMARY KEY,
    topic                  TEXT        NOT NULL,
    platform               TEXT        NOT NULL,       -- twitter | reddit | google
    trend_score            NUMERIC,
    first_seen_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    peak_at                TIMESTAMPTZ,
    related_news_event_id  INTEGER     REFERENCES news_events(id) ON DELETE SET NULL,
    related_call_ids       INTEGER[],
    narrative_tags         TEXT[],
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_trending_platform
    ON trending_topics (platform);

CREATE INDEX IF NOT EXISTS idx_trending_first_seen_at
    ON trending_topics (first_seen_at DESC);

CREATE INDEX IF NOT EXISTS idx_trending_score
    ON trending_topics (trend_score DESC)
    WHERE trend_score IS NOT NULL;


-- -----------------------------------------------------------------------------
-- 11. channel_daily_stats
-- -----------------------------------------------------------------------------
-- One row per channel per day. TYPE 4 (daily aggregate) and TYPE 5 (leaderboard)
-- messages from the same channel merge into the same row via ON CONFLICT upsert.
CREATE TABLE IF NOT EXISTS channel_daily_stats (
    id                  SERIAL PRIMARY KEY,
    channel_id          INTEGER     NOT NULL REFERENCES channels(id) ON DELETE CASCADE,
    stat_date           DATE        NOT NULL,

    -- TYPE 4: daily aggregate stats
    entry_signals       INTEGER,
    win_rate_50pct      NUMERIC(5,2),
    wins_avg_profit     NUMERIC(5,2),
    best_token          TEXT,
    best_multiplier     NUMERIC(8,2),
    alerts_2x           INTEGER,
    alerts_5x           INTEGER,
    alerts_10x          INTEGER,
    alerts_15x_plus     INTEGER,

    -- TYPE 5: top performers leaderboard [{symbol, multiplier}, ...]
    top_performers      JSONB,

    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_channel_daily_stats_channel_date
    ON channel_daily_stats (channel_id, stat_date);

CREATE INDEX IF NOT EXISTS idx_channel_daily_stats_date
    ON channel_daily_stats (stat_date DESC);
