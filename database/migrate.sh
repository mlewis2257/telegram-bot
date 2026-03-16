#!/usr/bin/env bash
# =============================================================================
# migrate.sh — Create the solana_signals database and apply schema
# =============================================================================
# Usage:
#   ./database/migrate.sh
#
# Prerequisites:
#   - PostgreSQL running locally
#   - psql available in PATH
#   - The DB_USER in database/.env has CREATEDB rights (or the DB already exists)
#
# Credentials are loaded from database/.env (DB_USER, DB_HOST, DB_PORT,
# DB_PASSWORD). Any of these can be overridden by exporting them in the shell
# before running the script.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Load database/.env if present ─────────────────────────────────────────────
ENV_FILE="$SCRIPT_DIR/.env"
if [[ -f "$ENV_FILE" ]]; then
    set -o allexport
    # shellcheck source=/dev/null
    source "$ENV_FILE"
    set +o allexport
fi

DB_NAME="solana_signals"
DB_USER="${DB_USER:-postgres}"
DB_HOST="${DB_HOST:-127.0.0.1}"
DB_PORT="${DB_PORT:-5432}"

# Export password so psql/createdb pick it up without an interactive prompt
export PGPASSWORD="${DB_PASSWORD:-}"

SCHEMA_FILE="$SCRIPT_DIR/schema.sql"

echo "========================================"
echo " solana_signals — migration"
echo "========================================"
echo " Host : $DB_HOST:$DB_PORT"
echo " User : $DB_USER"
echo " DB   : $DB_NAME"
echo "========================================"

# ── 1. Create the database if it doesn't exist ────────────────────────────────
if psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" \
        -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    echo "[1/3] Database '$DB_NAME' already exists — skipping creation"
else
    echo "[1/3] Creating database '$DB_NAME'..."
    createdb -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" "$DB_NAME"
    echo "      Done."
fi

# ── 2. Apply schema ───────────────────────────────────────────────────────────
echo "[2/3] Applying schema..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
     -v ON_ERROR_STOP=1 -f "$SCHEMA_FILE"
echo "      Done."

# ── 3. Seed news_sources ──────────────────────────────────────────────────────
echo "[3/3] Seeding news_sources..."
psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" \
     -v ON_ERROR_STOP=1 <<'SQL'

INSERT INTO news_sources (name, url, source_type, category)
VALUES
    ('CoinDesk',          'https://www.coindesk.com/arc/outboundfeeds/rss/',  'rss', 'crypto'),
    ('CoinTelegraph',     'https://cointelegraph.com/rss',                    'rss', 'crypto'),
    ('Decrypt',           'https://decrypt.co/feed',                          'rss', 'crypto'),
    ('Reuters Politics',  'https://feeds.reuters.com/Reuters/PoliticsNews',   'rss', 'politics'),
    ('BBC News',          'https://feeds.bbci.co.uk/news/rss.xml',            'rss', 'general'),
    ('Crypto Twitter',    'https://api.twitter.com/2/trends',                 'api', 'crypto')
ON CONFLICT (url) DO NOTHING;

SELECT id, name, source_type, category FROM news_sources ORDER BY id;

SQL

echo ""
echo "========================================"
echo " Migration complete."
echo " Connect with: psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"
echo "========================================"
