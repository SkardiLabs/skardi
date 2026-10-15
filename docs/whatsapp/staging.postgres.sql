-- Staging tables for docs/whatsapp/webhooks/whatsapp-inbound.yaml (Postgres).
--
-- Same column order as the SQLite variant: the webhook insert matches by
-- position. The Postgres path is the jobs destination's insert path; the
-- server's test suite exercises SQLite, so verify the first delivery here
-- with a `Send test webhook` from the App Dashboard.
--
--   psql "$DSN" -f docs/whatsapp/staging.postgres.sql

CREATE TABLE IF NOT EXISTS wa_events_messages (
    message_id           TEXT        NOT NULL,
    waba_id              TEXT        NOT NULL,
    phone_number_id      TEXT        NOT NULL,
    display_phone_number TEXT,
    from_wa_id           TEXT        NOT NULL,
    to_wa_id             TEXT,
    contact_name         TEXT,
    received_at          TIMESTAMPTZ NOT NULL,
    message_type         TEXT        NOT NULL,
    text                 TEXT,
    media_id             TEXT,
    media_mime_type      TEXT,
    caption              TEXT,
    reply_to_message_id  TEXT,
    interactive_reply_id TEXT,
    raw                  TEXT        NOT NULL,
    ingested_at          TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS wa_events_statuses (
    message_id          TEXT        NOT NULL,
    waba_id             TEXT        NOT NULL,
    phone_number_id     TEXT        NOT NULL,
    status              TEXT        NOT NULL,
    status_at           TIMESTAMPTZ NOT NULL,
    recipient_id        TEXT        NOT NULL,
    conversation_id     TEXT,
    conversation_origin TEXT,
    pricing_category    TEXT,
    billable            BOOLEAN,
    error_code          BIGINT,
    raw                 TEXT        NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS wa_events_messages_ingested_at ON wa_events_messages (ingested_at);
CREATE INDEX IF NOT EXISTS wa_events_statuses_ingested_at ON wa_events_statuses (ingested_at);
