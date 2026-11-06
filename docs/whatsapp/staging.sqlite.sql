-- Staging tables for docs/whatsapp/webhooks/whatsapp-inbound.yaml (SQLite).
--
-- Column order matters: the webhook insert matches by position, so each
-- table has exactly the route's declared columns, in order, followed by
-- ingested_at. The server checks this at startup and refuses to start on a
-- mismatch.
--
-- Timestamps are TEXT and land as ISO-8601 UTC instants
-- ("2025-06-08T20:59:43Z"). Meta's timestamps are whole seconds, so those
-- compare as strings; ingested_at carries milliseconds and a zero fraction
-- is dropped on write, so the merge jobs compare and order it as a
-- timestamp (arrow_cast), never as text.
--
--   sqlite3 staging.db < docs/whatsapp/staging.sqlite.sql

CREATE TABLE IF NOT EXISTS wa_events_messages (
    message_id           TEXT    NOT NULL,
    waba_id              TEXT    NOT NULL,
    phone_number_id      TEXT    NOT NULL,
    display_phone_number TEXT,
    from_wa_id           TEXT    NOT NULL,
    to_wa_id             TEXT,
    contact_name         TEXT,
    received_at          TEXT    NOT NULL,
    message_type         TEXT    NOT NULL,
    text                 TEXT,
    media_id             TEXT,
    media_mime_type      TEXT,
    caption              TEXT,
    reply_to_message_id  TEXT,
    interactive_reply_id TEXT,
    raw                  TEXT    NOT NULL,
    ingested_at          TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS wa_events_statuses (
    message_id          TEXT    NOT NULL,
    waba_id             TEXT    NOT NULL,
    phone_number_id     TEXT    NOT NULL,
    status              TEXT    NOT NULL,
    status_at           TEXT    NOT NULL,
    recipient_id        TEXT    NOT NULL,
    conversation_id     TEXT,
    conversation_origin TEXT,
    pricing_category    TEXT,
    billable            BOOLEAN,
    error_code          INTEGER,
    raw                 TEXT    NOT NULL,
    ingested_at         TEXT    NOT NULL
);

-- Rows the receiver could not map to a route's columns (an unknown message
-- type, a key missing where the route declared it non-null). Fixed shape;
-- one table serves every route. Query it, fix the route, re-insert.
CREATE TABLE IF NOT EXISTS wa_events_dead_letter (
    route       TEXT NOT NULL,
    raw         TEXT NOT NULL,
    error       TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);

-- The merge jobs watermark on arrival time.
CREATE INDEX IF NOT EXISTS wa_events_messages_ingested_at ON wa_events_messages (ingested_at);
CREATE INDEX IF NOT EXISTS wa_events_statuses_ingested_at ON wa_events_statuses (ingested_at);
