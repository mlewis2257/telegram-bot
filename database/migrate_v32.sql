-- migrate_v32.sql — make call de-duplication PER CHANNEL.
--
-- Telegram numbers messages separately in every channel. calls was unique on
-- (source_platform, source_message_id), so once two channels' message counters overlapped,
-- every new call from one channel collided with an already-stored message of another and
-- was silently dropped as a 'dupe' (2026-10-30: solwhaletrending recorded ~3 calls a day
-- while the channel kept posting). The new key adds the channel.
--
-- ORDER: deploy the code that uses plain "ON CONFLICT DO NOTHING" (db.insert_call) FIRST,
-- then run this. The old code names the old index in its ON CONFLICT clause and would fail
-- once that index is dropped.
--
-- RUN (VPS):  sudo -u postgres psql -d solana_signals -f /root/telegram-bot/database/migrate_v32.sql

BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS idx_calls_source_dedup_v2
    ON calls (source_platform, COALESCE(channel_id, 0), source_message_id)
    WHERE source_message_id IS NOT NULL;

DROP INDEX IF EXISTS idx_calls_source_dedup;

COMMIT;

-- Check:
--   SELECT indexname, indexdef FROM pg_indexes WHERE tablename = 'calls' AND indexname LIKE 'idx_calls_source_dedup%';
