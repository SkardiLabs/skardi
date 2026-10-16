# WhatsApp: inbound messages and receipts, end to end

A runnable bundle for the `kind: webhook` receiver described in
[docs/webhooks.md](../webhooks.md): Meta delivers inbound WhatsApp messages
and delivery receipts to skardi-server, the server verifies and appends them
to a SQLite staging catalog, and two jobs dedup them into Lance.

```
Meta Cloud API ──POST──► /webhooks/whatsapp-inbound ──► staging.main.wa_events_messages
   (messages,            verify X-Hub-Signature-256       staging.main.wa_events_statuses
    statuses)            unwrap, map columns, append              │
                                                                  │ kind: job (upsert)
                                                                  ▼
                                                   wa_messages.lance / wa_statuses.lance
```

| File | What it is |
|------|------------|
| [ctx_whatsapp.yaml](ctx_whatsapp.yaml) | The staging SQLite catalog (read-write) and the two Lance datasets |
| [staging.sqlite.sql](staging.sqlite.sql) | DDL for the two staging tables (in the column order the routes write) and the dead-letter table |
| [staging.postgres.sql](staging.postgres.sql) | The same tables for Postgres |
| [webhooks/whatsapp-inbound.yaml](webhooks/whatsapp-inbound.yaml) | The webhook: provider preset, secrets, two routes, every column |
| [jobs/wa-merge-messages.yaml](jobs/wa-merge-messages.yaml) | Dedup messages into Lance on `message_id`, watermarked on `ingested_at` |
| [jobs/wa-merge-statuses.yaml](jobs/wa-merge-statuses.yaml) | Dedup receipts into Lance on `(message_id, status)` |

The server's test suite loads these exact files, preflights the webhook
against the DDL, delivers Meta's documented sample payloads through it, and
plans both jobs, so the bundle cannot drift from the code.

## 1. Create the staging tables

```bash
sqlite3 docs/whatsapp/staging.db < docs/whatsapp/staging.sqlite.sql
```

Column order is the contract: the insert matches by position, and the
server refuses to start if a table's columns differ from the route's
declared columns plus `ingested_at`.

## 2. Start the server

```bash
export WHATSAPP_VERIFY_TOKEN='pick-any-string'
export WHATSAPP_APP_SECRET='<App settings → Basic → App Secret>'

cargo run --release -p skardi-server -- \
  --ctx docs/whatsapp/ctx_whatsapp.yaml \
  --webhooks docs/whatsapp/webhooks \
  --jobs docs/whatsapp/jobs \
  --port 8080
```

Startup logs `Webhook loaded: whatsapp-inbound (provider meta_whatsapp) → 2
route(s)`. A missing environment variable or a table with the wrong columns
is a startup error that names the problem.

## 3. Register the callback with Meta

Meta needs a public HTTPS URL with a CA-issued certificate. For a local
server, a tunnel works:

```bash
cloudflared tunnel --url http://localhost:8080
```

In the [App Dashboard](https://developers.facebook.com/apps), under
**WhatsApp → Configuration**: set the Callback URL to
`https://<tunnel-host>/webhooks/whatsapp-inbound`, the Verify token to the
value of `WHATSAPP_VERIFY_TOKEN`, click **Verify and save**, then subscribe
to the **`messages`** field. The server logs `subscription handshake
accepted`.

## 4. Deliver something

Use **Send test webhook** in the same dashboard page, or send a message to
the business number from the test phone. Then:

```bash
curl -s localhost:8080/query -H 'content-type: application/json' -d '{
  "sql": "SELECT message_id, from_wa_id, contact_name, message_type, text, received_at, ingested_at FROM staging.main.wa_events_messages ORDER BY ingested_at DESC LIMIT 5"
}'
```

Meta's epoch-seconds `timestamp` lands as an ISO-8601 UTC instant
(`2025-06-08T20:59:43Z`); `ingested_at` is the server's own write time. Both
are `TEXT` in SQLite — they compare and sort correctly as strings, and the
merge jobs cast them to real timestamps on the way into Lance.

To deliver a sample without Meta, sign one of the documented payloads
yourself:

```bash
BODY=$(cat crates/server/tests/fixtures/webhooks/text_message.json)
SIG=$(printf '%s' "$BODY" | openssl dgst -sha256 -hmac "$WHATSAPP_APP_SECRET" | awk '{print $NF}')
curl -s -X POST localhost:8080/webhooks/whatsapp-inbound \
  -H 'content-type: application/json' -H "x-hub-signature-256: sha256=$SIG" \
  --data-binary "$BODY"
```

The response is `{"success":true,"rows":1,"routes":[...]}`. Send it twice
and the staging table holds two copies: staging is an append-only log, and
Meta redelivers on any non-200, so duplicates are expected there.

## 5. Merge into Lance

```bash
curl -s -X POST localhost:8080/jobs/wa-merge-messages/run \
  -H 'content-type: application/json' -d '{"since": "2026-01-01T00:00:00Z"}'
curl -s -X POST localhost:8080/jobs/wa-merge-statuses/run \
  -H 'content-type: application/json' -d '{"since": "2026-01-01T00:00:00Z"}'
```

The first run creates `docs/whatsapp/lake/wa_messages.lance`; later runs
upsert on `message_id`, so the two staging copies become one row. Lance
datasets are registered as tables at startup, so after the **first** run
restart the server once before querying `wa_messages` (the startup log says
`does not exist yet — skipping table registration` until then). Schedule
the jobs from cron with `{since}` set to the previous run's start minus a
few minutes of overlap — the upsert makes the overlap harmless, and
watermarking on `ingested_at` rather than Meta's timestamp is what keeps a
retried or backfilled delivery from being skipped.

```sql
-- inbound volume per business number per day, after the merge
SELECT phone_number_id, date_trunc('day', received_at) AS day, count(*) AS inbound
FROM wa_messages
GROUP BY 1, 2 ORDER BY 2 DESC, 3 DESC;
```

## What is not here

- **Outbound message content.** Meta echoes receipts, never the text of
  messages the business sends; the sender records its own copy, with
  `to_wa_id` and `reply_to_message_id` filled, if whole conversations are
  wanted.
- **History from before the subscription**, and **template or phone-number
  inventories** — those come from Open Connector's `whatsapp` provider, not
  from the webhook.
- **Postgres as the staging store** is supported by the same insert path the
  jobs use; the test suite exercises SQLite. Use
  [staging.postgres.sql](staging.postgres.sql) and a `type: postgres`
  source with `access_mode: read_write`.
