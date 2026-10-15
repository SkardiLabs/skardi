# Webhooks

A `kind: webhook` resource gives skardi-server a public `GET|POST
/webhooks/<name>` route that accepts a provider's event deliveries, verifies
them with the provider's own proof, flattens the JSON envelope into rows, and
appends those rows to a table you can query immediately. It exists for
sources that only *push*: the WhatsApp Cloud API has no endpoint for message
history, so the only copy of an inbound message is the one Meta delivers to
your endpoint.

**In one sentence:** a pipeline answers a query and a job commits a query's
result; a webhook commits what a provider sends you.

Everything downstream of the envelope is reused: the `columns:` grammar is
the Open Connector source pack's, the writer is the jobs destination, and
dedup is the Lance upsert a merge job runs later. The only provider-specific
pieces are the verification preset and the envelope unwrapper.

Provider presets today: `meta_whatsapp` (WhatsApp Cloud API).

---

## When to use a webhook

- **Capturing WhatsApp conversations.** Inbound messages and delivery
  receipts are delivered once, by webhook, and retried for up to seven days
  when your endpoint does not answer 200. There is no backfill: the earlier
  the webhook is live, the more history you keep.
- **Any provider that pushes change events** whose shape fits a JSON envelope
  with a list of rows, once a preset exists for its verification scheme.

Use Open Connector sources instead when the provider has a list endpoint you
can pull from: a pack gives you the current state on demand, with no public
ingress to run.

---

## Writing a webhook YAML

```yaml
kind: webhook

metadata:
  name: whatsapp-inbound                 # served at /webhooks/whatsapp-inbound
  version: "1.0.0"

spec:
  provider: meta_whatsapp                # verification preset
  verify_token_env: WHATSAPP_VERIFY_TOKEN
  app_secret_env: WHATSAPP_APP_SECRET
  max_body_bytes: 4194304                # default; Meta sends at most 3 MB

  routes:
    - field: messages                    # rows: value.messages[] of every change
      destination: { table: staging.main.wa_events_messages, mode: append }
      columns:
        - { name: message_id,      path: id,                        type: utf8, nullable: false }
        - { name: waba_id,         path: _waba_id,                  type: utf8, nullable: false }
        - { name: phone_number_id, path: _metadata.phone_number_id, type: utf8, nullable: false }
        - { name: from_wa_id,      path: from,                      type: utf8, nullable: false }
        - { name: received_at,     path: timestamp,                 type: timestamp_s_string_utc, nullable: false }
        - { name: text,            path: text.body,                 type: utf8, nullable: true }
        - { name: raw,             path: _raw,                      type: json, nullable: false }
    - field: statuses
      destination: { table: staging.main.wa_events_statuses }
      columns:
        - { name: message_id,      path: id,                        type: utf8, nullable: false }
        - { name: status,          path: status,                    type: utf8, nullable: false }
        - { name: status_at,       path: timestamp,                 type: timestamp_s_string_utc, nullable: false }
        - { name: raw,             path: _raw,                      type: json, nullable: false }
```

The complete WhatsApp definition, with every column the design captures, is
in [docs/whatsapp/webhooks/whatsapp-inbound.yaml](whatsapp/webhooks/whatsapp-inbound.yaml).

### `kind:`

Every resource YAML carries a `kind:` discriminator at the root. A server
started with `--webhooks <dir>` scans every `.yaml` / `.yml` file in the
directory and silently skips files that are not `kind: webhook`. Keep
webhooks in their own directory: the `--jobs` and `--pipeline` loaders are
strict and reject a `kind: webhook` file.

### `metadata.name`

Becomes the URL path segment, so it must start with a letter or digit and
contain only letters, digits, `-` and `_`. Names are unique per server.

### `spec.provider`

The verification preset. `meta_whatsapp` implements Meta's two proofs:

- **GET handshake.** Meta calls `GET /webhooks/<name>?hub.mode=subscribe&hub.verify_token=…&hub.challenge=…`
  when you register the callback URL. The server compares the token with
  `verify_token_env` (constant-time) and answers `200 text/plain` with the
  challenge verbatim; anything else is `403`.
- **POST signature.** Every delivery carries
  `X-Hub-Signature-256: sha256=<hex HMAC-SHA256 of the raw body, keyed by the app secret>`.
  The server verifies it over the exact bytes received, before parsing
  anything; a missing, malformed, or wrong signature is `401`.

### `spec.verify_token_env` / `spec.app_secret_env`

Names of environment variables, never the values. Both are read once at
startup, and a variable that is unset or empty fails startup — a missing
secret must not surface as the first failed delivery. Secrets are redacted
from every log line and debug dump.

### `spec.max_body_bytes`

Cap on the request body; a larger delivery is `413`. The default is 4 MiB,
one megabyte above Meta's documented 3 MB maximum. The minimum is 1 KiB.

### `spec.routes[]`

A delivery is applied to every route, in order. Each route has:

| Key | Meaning |
|-----|---------|
| `field` | For `rows: list`, the array inside each change's `value` to read (`messages`, `statuses`). For `rows: value`, the change's own `field` label to match. |
| `rows` | `list` (default) or `value`; see below. |
| `destination.table` | A DataFusion table identifier. The first dotted segment names the data source. `mode` accepts only `append`. |
| `columns` | The declared columns, in destination order. |

**`rows: list`** emits one row per element of `value.<field>[]`. A change
matches by *carrying that array*, not by its label: Meta files both
`messages` and `statuses` arrays under `field: "messages"`, so one change can
feed a `messages` route and a `statuses` route. Each row is the original
element plus these synthetic keys, which is how a column reaches data the
dotted-path grammar cannot (the grammar walks object keys only — no
wildcards, no array indexes, exactly as in a source pack):

| Key | Value |
|-----|-------|
| `_waba_id` | `entry.id` — the WhatsApp Business Account the change belongs to |
| `_metadata` | `value.metadata` (`phone_number_id`, `display_phone_number`) |
| `_contact` | the `value.contacts[]` entry whose `wa_id` equals the row's `from` (messages) or `recipient_id` (statuses) |
| `_media` | the object named by the row's `type`, for `image` / `video` / `audio` / `document` / `sticker` |
| `_interactive_reply` | `interactive.button_reply` or `interactive.list_reply` |
| `_error` | `errors[0]` — set on `type: unsupported` messages and `status: failed` receipts |
| `_raw` | the original element, untouched, for a `json` column |

**`rows: value`** treats the change's whole `value` object as one row, and a
change matches by its `field` label. That is the shape of every management
field (`message_template_status_update`, `phone_number_quality_update`,
`account_update`, …), which deliver one object per change rather than an
array. The row carries `_waba_id` and `_raw`.

### `columns[]`

Identical to a source pack's column grammar:

```yaml
- { name: <column>, path: <row-relative dotted path>, type: <type>, nullable: <bool> }
```

| `type` | Arrow type | Accepts |
|--------|------------|---------|
| `utf8` | Utf8 | a JSON string |
| `boolean` | Boolean | a JSON boolean |
| `int64` / `uint64` | Int64 / UInt64 | a JSON integer |
| `float64` | Float64 | a JSON number |
| `timestamp_s_string_utc` | Timestamp(ms, UTC) | a string of epoch seconds — Meta's `timestamp` |
| `timestamp_s_utc` / `timestamp_ms_utc` | Timestamp(ms, UTC) | an integer of epoch seconds / milliseconds |
| `timestamp_ms_string_utc` / `timestamp_s_frac_string_utc` | Timestamp(ms, UTC) | strings of epoch milliseconds / fractional seconds |
| `utf8_list` | List\<Utf8\> | an array of strings |
| `json` | Utf8 | anything; stored as compact JSON text |

`nullable: true` turns a missing key or a JSON null into NULL. With
`nullable: false`, a missing key fails the delivery — the server answers
`500` and the provider retries, so reserve it for keys Meta always sends.

### `ingested_at`

Every route gets one more column, `ingested_at` (Timestamp(ms, UTC), the
server clock at write time), appended after the declared columns. You do not
declare it — declaring a column with that name is an error — but the
destination table must have it, last. It exists so a merge job can
watermark on *arrival* rather than on Meta's own timestamp, which a retried
or backfilled delivery carries unchanged.

---

## The destination table

A route appends to a transactional SQL source — **Postgres, MySQL, or
SQLite** — declared with `access_mode: read_write` in the context. Lance is
deliberately not allowed as a direct destination: every Lance commit is a
new dataset version, so per-delivery writes would fragment the dataset and
blow the provider's 250 ms response budget. Land in SQL, then merge into
Lance with a job (below).

The insert matches columns **by position**, so the table must have exactly
the route's declared columns, in order, followed by `ingested_at`. The
server checks this at startup for every route and refuses to start on a
mismatch, naming the table, the columns it found, and the columns the route
writes. It also refuses a table that is not registered (the catalog is read
once, at startup — create the table first), a source that is not
transactional, and a source that is not `read_write`.

SQLite DDL for the two WhatsApp routes is in
[docs/whatsapp/staging.sqlite.sql](whatsapp/staging.sqlite.sql); a Postgres
variant is next to it. Timestamp columns are declared `TEXT` in SQLite and
land as ISO-8601 UTC instants (`2025-06-08T20:59:43Z`), which compare and
sort correctly as strings.

```yaml
# ctx.yaml
data_sources:
  - name: staging
    type: sqlite
    path: ./staging.db
    hierarchy_level: catalog        # tables addressed as staging.main.<table>
    access_mode: read_write
```

---

## Request handling and status codes

For a `POST /webhooks/<name>`, in order: look the name up (`404` if not
configured), read the body up to `max_body_bytes` (`413`), verify the
signature over the raw bytes (`401`), parse JSON (`400`), unwrap and
convert each route's rows, write each route's batch in one transaction, and
only then answer `200` with a count per route.

| Status | Meaning | Provider retries? |
|--------|---------|-------------------|
| `200` | every route's rows are committed (possibly zero rows) | no |
| `401` | signature missing, malformed, or wrong | yes — and it will keep failing; check the app secret |
| `403` | handshake token wrong (GET only) | n/a |
| `404` | no webhook with that name | yes — check the route name |
| `413` | body over `max_body_bytes` | yes |
| `400` | verified body is not JSON, or not an envelope with an `entry` array | yes |
| `500` | rows could not be mapped to the declared columns, or the write failed | yes, for up to seven days |

Nothing is acknowledged early, and nothing is buffered in memory: a `200`
means the transaction committed. That is deliberate — Meta's retry is the
only durability guarantee a webhook has, so the server never claims success
it has not earned.

Two consequences worth knowing:

- **Duplicates are normal.** Meta retries on any non-200 and delivers to
  every subscribed app, so the same message can arrive more than once. The
  staging table is append-only and keeps every copy; the merge job's upsert
  collapses them.
- **Multi-route deliveries commit per table.** If a later route fails after
  an earlier one committed, the server answers `500`, Meta redelivers, and
  the earlier route's rows are appended again — which the same upsert
  collapses.

---

## Running the server with webhooks

```bash
export WHATSAPP_VERIFY_TOKEN='a-string-you-choose'
export WHATSAPP_APP_SECRET='<App Secret from App Dashboard → App settings → Basic>'

skardi-server --ctx ctx.yaml --webhooks ./webhooks --port 8080
```

`--webhooks` takes a file or a directory. The route must be reachable by
Meta over HTTPS with a certificate from a public CA (self-signed is
rejected). For local development, a tunnel such as `cloudflared tunnel
--url http://localhost:8080` gives you a public URL.

The route is **not** behind the API session check that `/query`,
`/:name/execute`, and `/jobs/*` enforce when authentication is enabled —
Meta carries no Skardi token. The provider's own proof is the
authentication.

### Registering the callback with Meta

In the Meta App Dashboard, under **WhatsApp → Configuration**:

1. Set the **Callback URL** to `https://<your-host>/webhooks/<name>` and the
   **Verify token** to the value of `WHATSAPP_VERIFY_TOKEN`, then click
   Verify and save. The server logs `subscription handshake accepted`.
2. Subscribe to the **`messages`** field (requires
   `whatsapp_business_messaging`). It carries both inbound messages and
   delivery receipts.
3. Use **Send test webhook** to deliver a sample, then query the staging
   table.

The App Secret is under **App settings → Basic**. Rotating it means
restarting the server with the new value.

---

## Merging into Lance

The staging table is a log. A `kind: job` turns it into a deduplicated,
queryable table, run every few minutes by whatever schedules your jobs:

```yaml
kind: job
metadata: { name: wa-merge-messages, version: "1.0.0" }
spec:
  query: |
    SELECT message_id, waba_id, phone_number_id, from_wa_id, received_at,
           message_type, text, raw, ingested_at
    FROM staging.main.wa_events_messages
    WHERE ingested_at >= {since}
  destination:
    table: wa_messages
    mode: upsert
    merge_keys: [message_id]
    repeated_keys: keep_last
    create_if_missing: true
```

Watermark on `ingested_at`, not on `received_at`, and pass `{since}` as the
previous run's start time minus a small overlap: re-merging the overlap is
harmless because the upsert is idempotent, while a watermark on Meta's
timestamp would skip a retried or backfilled row forever. Receipts merge on
`[message_id, status]`, since a message has at most one `sent`, one
`delivered`, and one `read`. The two jobs are in
[docs/whatsapp/jobs/](whatsapp/jobs/).

---

## Observability

| Metric | Type | Labels |
|--------|------|--------|
| `webhook_requests_total` | Counter | `name`, `outcome` |
| `webhook_rows_total` | Counter | `name`, `field` |
| `webhook_latency_ms` | Histogram | `name` |

`outcome` is `ok`, `handshake_ok`, `handshake_rejected`,
`signature_rejected`, `payload_too_large`, `invalid_payload`,
`conversion_error`, or `write_error`. Requests for an unconfigured name are
not counted: the name is caller-controlled and would be an unbounded label.

Each accepted delivery logs one line with the webhook name, rows per route,
the WABA ids in the envelope, and the latency. Rejections log at `warn`
with the reason; conversion and write failures at `error`. Message content
is never logged.

---

## What a webhook does not give you

- **Outbound message content.** Meta echoes only the *status* of messages
  your business sends, never their text. To keep whole conversations, the
  sender records its own copy at send time (the design doc's
  "Outbound message content" section describes the row shape).
- **History from before the subscription.** There is no backfill.
- **A full list of templates or phone numbers.** Management fields deliver
  change events, not inventories; pull those through Open Connector.

---

## A complete worked example

[docs/whatsapp/](whatsapp/README.md) holds a runnable bundle: the context
file, the staging DDL for SQLite and Postgres, the full webhook definition,
and the two merge jobs. The server's test suite loads those exact files and
preflights them against the DDL, so they cannot drift from the code.
