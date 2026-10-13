# WhatsApp Integration Design

**Status:** draft, pending review
**Date:** 2026-10-13
**Reconciled against:** skardi `main` @ `a24a9b5`; oomol-lab/open-connector local
checkout @ `4237c391` (`src/providers/whatsapp/` targets Graph API **v23.0**);
Meta's official documentation as published on 2026-10-13 (Graph API latest is v26.0).

## Summary

**One-line conclusion:** the only WhatsApp data that reaches Skardi through the
existing Open Connector (OC) path is *account-side* data — phone numbers,
templates, business profile, and (after a small OC change) analytics.
**Messages themselves cannot be pulled.** That is not an OC gap; it is a hard
constraint of the Meta platform. The Cloud API has no endpoint for fetching
message history. Inbound messages are delivered once, by webhook (retried for up
to 7 days on failure), and the business must persist them itself.

A complete integration is therefore two complementary paths, not a choice
between them: **a pull path** (a source pack over OC, for account-side data) **and
a push path** (a webhook receiver in skardi-server, for messages). Neither can
substitute for the other: the webhook never delivers a template or phone-number
list, and OC never delivers a message. A third component, analytics tables, is
**optional**: most of its value is already covered by the delivery receipts the
webhook captures.

| Component | Data it yields | Repos touched | Existing components reused |
|---|---|---|---|
| 1. `whatsapp` source pack (pull) | Current-state snapshots: `phone_numbers`, `business_profile`, `message_templates` | skardi (pack YAML plus the fixed accessor boilerplate) | OC's `whatsapp` provider, the pack engine, `open_connector_query`, the pack docs template |
| 2. `kind: webhook` receiver (push) | Inbound messages and delivery receipts: `wa_messages`, `wa_statuses`, for RAG and BI | skardi-server (one new resource kind plus one route) | The pack column grammar and `JsonToArrow`, the jobs `SqlDmlDestination` / `LanceDestination` writers with upsert dedup, the existing Postgres/SQLite/Lance sources |
| 3. Analytics tables (pull, **optional**) | Meta's own aggregates: `message_analytics`, `pricing_analytics`, `template_analytics` | OC (three new read actions, about half a day) + skardi (tables added to the pack) | Same as component 1, plus `kind: job` to snapshot into Lance (escaping Meta's one-year lookback) |
| Deferred follow-ups: media download, outbound send, multi-tenant Embedded Signup, Coexistence history import | — | mostly OC | — |

The two required components share no code and can be built, reviewed, and
shipped independently, in either order. Rejected alternatives (OC triggers +
Open Flow, third-party BSPs, unofficial providers) are covered one by one in
[Alternatives considered](#alternatives-considered).

## Background and goals

Skardi queries SaaS data as SQL tables through the OC gateway: provider
credentials live only in OC, and Skardi holds only the gateway URL and a runtime
token. Each provider is a "source pack" in Skardi
(`crates/skardi/src/sources/providers/open_connector/packs/<p>.yaml`) that
declares the action, row path, pagination, column mapping, and pushable filters.
Eleven packs exist today: Slack, Feishu, Discord, Gmail, Notion, GitHub, Dropbox,
Google Drive, OneDrive, Outlook, and a mock.

Users want WhatsApp to work the same way. Typical asks:

- **Operations / BI:** sent and delivered volume per number, cost by category,
  template review status.
- **Support / RAG:** land customer messages (text, button replies, media) and
  our delivery statuses in Lance for retrieval, summarization, and analysis.
- **Federated queries:** join against CRM (Postgres) and tickets (Notion / Jira)
  by phone number.

### Goals

1. No new WhatsApp-specific credential store; the Meta token stays in OC.
2. Account-side and analytics data with zero new infrastructure: one pack.
3. Message data landed with **the least new code**, and that code built as a
   generic capability so other webhook-shaped sources (Slack Events, Stripe,
   GitHub) can reuse it.
4. Each component is independently deliverable and verifiable.

### Non-goals

- Sending WhatsApp messages from Skardi. OC sources are read-only in Skardi
  (DML and job destinations are rejected); sending belongs to OC's
  `whatsapp.send_*` actions or OC's MCP surface.
- Unofficial WhatsApp Web protocols (Baileys / WAHA / Evolution / Z-API /
  Green-API). They carry account-ban risk and are not on the product roadmap;
  see alternative D.
- A conversational UI or support console.

## Research findings

### 1. What Open Connector offers for WhatsApp today

OC ships an official Meta Cloud API provider, `whatsapp`
(`src/providers/whatsapp/`):

| Item | Current state | Effect on this design |
|---|---|---|
| Auth | `api_key`: a permanent Meta System User token (Bearer), plus an optional extra field `wabaId` as the default WABA | No new credential shape; `wabaId` can live on the OC connection or be passed as a binding resource |
| Graph version | `v23.0` (`runtime.ts:10`) | Usable, but Meta's latest is v26.0; bump to v25+/v26 in the component 3 OC PR |
| Read actions | `get_phone_numbers`, `get_phone_number`, `get_business_profile`, `get_message_templates`, `get_template_status`, `get_media_info` | The first three groups become tables; `get_media_info` only returns a 5-minute URL that needs a Bearer header to download, so it is not table material |
| Write actions | `send_message`, `send_template_message`, `send_media(_by_id)`, `upload_media`, `send_location/contacts/interactive_*`, `create/delete_message_template` | Non-goal |
| **List messages / conversations** | **None** | The platform does not provide it; see the next section |
| Pagination | Only `get_message_templates` forwards an `after` cursor; `get_phone_numbers` has only `limit` (≤100) and **cannot page** | `phone_numbers` uses `single_page` with a `next_cursor_path` tripwire |
| Rate limits / timeouts | No retry, no `Retry-After` parsing, a hand-written fetch with no timeout | Skardi's `max_attempts` retries only on 429, which is enough; OC can be tidied in passing |
| Triggers | A generic trigger framework exists (`src/triggers/`: reconcile / receive / poll), **but `whatsapp` declares no trigger**; public ingress is owned by the external **Open Flow** service, and OC itself stores no events | Going through OC triggers means writing an OC trigger *and* deploying Open Flow; see alternative B2 |
| Proxy | `POST /v1/proxy/whatsapp` forwards any Graph GET | Fine for debugging and one-off reads; Skardi's `open_connector_scan` accepts actions only, not the proxy |

OC also carries nine third-party WhatsApp-adjacent providers; see
[Third-party providers](#4-third-party-whatsapp-providers).

### 2. Hard constraints of the WhatsApp platform (Meta docs, 2026-10-13)

- **No message-history API.** Meta states it outright: "There are no APIs for
  fetching historical webhook data, so capture and store webhook payloads
  accordingly." (webhooks/create-webhook-endpoint)
- **Messages arrive only by webhook.** The `messages` field needs
  `whatsapp_business_messaging`; the other 18 fields (template status, phone
  number quality, account review, and so on) need `whatsapp_business_management`.
  One POST is at most 3 MB and may batch up to 1000 updates.
- **Envelope:** `{object:"whatsapp_business_account", entry:[{id:<WABA_ID>,
  changes:[{field:"messages", value:{messaging_product, metadata:{display_phone_number,
  phone_number_id}, contacts:[{profile.name, wa_id}], messages:[...]}}]}]}`.
  Outbound delivery receipts use `value.statuses[]` (`sent` / `delivered` /
  `read` / `failed`, with `conversation`, `pricing`, `errors`).
- **Verification:** on subscription, a GET with
  `hub.mode=subscribe&hub.verify_token&hub.challenge` must echo the challenge;
  every POST carries `X-Hub-Signature-256: sha256=<HMAC-SHA256(body, app_secret)>`.
  The endpoint must serve CA-issued HTTPS; self-signed certificates are rejected.
- **Retries and duplicates:** a non-200 or unreachable endpoint is retried with
  decreasing frequency **for up to 7 days**; retries go to every subscribed app,
  so **duplicates are guaranteed**; Meta expects a median response under 250 ms.
  The receiver must be idempotent.
- **Analytics are fields on the WABA node:**
  `GET /<WABA_ID>?fields=analytics.start(..).end(..).granularity(DAY)`, plus
  `conversation_analytics` (conversation-based pricing was retired on 2025-07-01;
  prefer `pricing_analytics`), `pricing_analytics`, and `template_analytics`
  (DAILY only, ≤10 templates, 90-day window). **Since 2025-12-01 the lookback
  window shrank from 10 years to 1 year** — anything older must be stored by us.
- **Media:** `GET /<MEDIA_ID>` returns a URL valid for 5 minutes; the download
  needs a Bearer header; media IDs from webhooks stay downloadable for 7 days;
  inbound media is capped at 100 MB.
- **Tokens:** direct developers use a permanent System User token with
  `business_management` + `whatsapp_business_management` +
  `whatsapp_business_messaging`; multi-tenant setups use Embedded Signup (v2 is
  retired on 2026-10-15; use v4) to obtain a Business Integration System User token.
- **Other limits:** outside the 24-hour customer-service window only template
  messages can be sent; BUC rate limits are roughly 200 req/h per app per WABA
  (5000 for active WABAs), so analytics polling must stay modest.

### 3. Existing Skardi components this design reuses

| Component | Location | How it is reused |
|---|---|---|
| OC source registration, client, pagination, fingerprint gate | `crates/source-pack/src/{client,pagination,action_registry}.rs`, `providers/open_connector/{mod,table,exec}.rs` | Every table in components 1 and 3, unchanged |
| Pack YAML grammar (`row_path`, `row_shape: object`, `single_page`, `cursor`, `resources`, `fixed_inputs`, `filters`) | `providers/open_connector/packs/loader.rs:645-760`; templates `slack.yaml`, `feishu.yaml` (`document_content` is the `row_shape: object` precedent) | Components 1 and 3 use it as-is; component 2 borrows the same `columns:` grammar |
| JSON-to-Arrow column mapping | `providers/open_connector/json_to_arrow.rs` (`JsonToArrow::from_columns`; types include `timestamp_s_string_utc`, which matches Meta's string epoch-seconds timestamps exactly) | Component 2's receiver hands flattened rows to it instead of writing a second converter |
| `kind: job` and destination writers | `crates/skardi/src/jobs/destination/{lance,sql_dml}.rs`; `mode: upsert` + `merge_keys` + `repeated_keys: keep_last` (Lance only) | Component 3 snapshots; component 2 dedup-merge from staging into Lance (**no new dedup code**) |
| Pipeline `INSERT … VALUES {rows}` | `docs/pipelines.md:105-190` | Component 2's manual backfill / repair path, and the outbound-content path |
| Writable SQL sources (Postgres / MySQL / SQLite, `access_mode: read_write`) | `crates/server/src/config.rs:904 WRITABLE_SOURCE_TYPES` | Component 2's staging table |
| Auth layer | `crates/server/src/auth/` | Component 2's webhook route must be **exempted** from it and use HMAC instead |
| Pack docs template and live-verification flow | `docs/open-connector-slack.md`, `docs/superpowers/skills/source-pack/` | Components 1 and 3 follow the flow verbatim |
| skardi-cloud ETL generator | `skardi-cloud/code/rust/crates/skardi-etl`, already generates "OC table → Lance" job bundles | Component 3 snapshot jobs can be generated by it |

**What Skardi does not have:** any inbound webhook / ingest route; any push or
streaming source (no Kafka / NATS / Redis Streams); a job scheduler (runs start
only from `POST /jobs/:name/run` or `skardi job run`). Of these three gaps, only
the first must be closed by this design.

### 4. Third-party WhatsApp providers

| OC provider | Can list message history? | Meta-sanctioned? | Assessment |
|---|---|---|---|
| `twilio` (BSP) | Yes: `list_messages` (`to` / `from` accept `whatsapp:+…`, `pageToken` paging, **no date filter**), 13-month retention by default | Yes | A `twilio_whatsapp` pack is viable for customers already on Twilio, after live-verifying that the `whatsapp:` filter passes through |
| `unipile` | Yes: `list_chats` / `list_chat_messages` (cursor plus `before` / `after`; a perfect fit for the pack grammar) | **No** — links a WhatsApp app account by QR code; self-described as "not a Meta Partner" | Ban risk; not a product line |
| `timelinesai` | Yes: `list_chats` / `list_chat_messages` (`fromMe`, page-number paging) | No (WhatsApp Web link) | Same |
| `chat_api_for_whatsapp`, `z_api`, `waboxapp`, `whatsable`, `gupshup`, `twochat`, `superchat` | Only the first; the rest cannot | Mostly unofficial, or templates / contacts only | Not considered |
| 360dialog | No OC provider; the official BSP is webhook-only too | — | Not considered |

Conclusion: on the official path, **Twilio is the only** way to get messages
without running a webhook receiver, and only for customers who already route
WhatsApp through Twilio. It is listed as an optional pack under deferred follow-ups.

## Alternatives considered

| Alternative | What it yields | New code | New operational dependency | Verdict |
|---|---|---|---|---|
| **A. Official OC pack (pull only)** | Numbers, templates, business profile; analytics after an OC action | Pack YAML; ~100 lines in OC | None | **Adopted as components 1 and 3** |
| **B1. Native Skardi webhook receiver** | Inbound messages, delivery statuses, and all 19 management event types | One route in skardi-server + a `kind: webhook` resource + a Meta verification preset (est. 1–2 weeks incl. tests) | One public HTTPS address | **Adopted as component 2**; built as a generic capability |
| B2. OC trigger + Open Flow | Same as B1 | A `whatsapp` trigger in OC (~250 lines, modelled on telegram) + Skardi implementing Open Flow's `receive` callback protocol | Open Flow must be deployed (public ingress, subscription leases, dedup all live there) | Not the main line: the WhatsApp trigger does not exist today, and it would tie Skardi to Open Flow's subscription state machine. B1's verification step can later delegate to OC's `receive` operation; the interface leaves room for that |
| C. Third-party BSP pack (Twilio) | WhatsApp messages in a Twilio account | Pack YAML | Twilio account and fees | Optional, deferred |
| D. Unofficial WhatsApp Web providers | Full chat history | Pack YAML | Account-ban risk | Rejected |
| E. Coexistence `history` webhook | 180 days of WhatsApp Business **App** history from before onboarding | Depends on B1 + Embedded Signup | Tech Provider status | Deferred; only meaningful for app-migration users |

## Recommended architecture

Both paths use the official Meta Cloud API, from opposite directions. The pull
path goes Skardi → OC → Graph API and yields account-side snapshots. The push
path goes Meta → skardi-server and yields messages and receipts; it does not go
through OC, and the Meta app secret it needs lives in skardi-server's
environment, a different secret from the access token OC holds (separation of
duties). A deployment that wants messages *and* template / phone-number lists
runs both.

```
 ┌──────────────────── pull: component 1 (+ optional component 3) ────────────────────┐
 │                                                                                    │
 │ Meta Graph API ◄── OC gateway ◄── Bearer runtime token ── skardi-server             │
 │ (WABA node)        whatsapp.get_phone_numbers        saas.wa.phone_numbers          │
 │                    whatsapp.get_message_templates    saas.wa.message_templates      │
 │                    whatsapp.get_business_profile     saas.wa.business_profile       │
 │                    whatsapp.get_*_analytics (new)     saas.wa.*_analytics  (optional)│
 │                                                          │ kind: job (upsert)       │
 │                                                          ▼                          │
 │                                                     Lance: wa_analytics_history      │
 └────────────────────────────────────────────────────────────────────────────────────┘

 ┌──────────────────── push: component 2 ─────────────────────────────────────────────┐
 │ Meta Cloud API ──► https://<public>/webhooks/whatsapp-inbound  (skardi-server)      │
 │   webhooks:      │  1. GET  hub.challenge handshake  3. unwrap entry[]/changes[]    │
 │   messages       │  2. POST X-Hub-Signature-256      4. JsonToArrow (pack grammar)  │
 │   statuses       │                                   5. SqlDmlDestination → staging │
 │                  │                                        │ 200 OK (<250 ms)        │
 │                  │                                        ▼                         │
 │                  │   SQLite / Postgres: wa_events_messages, wa_events_statuses       │
 │                  │                                        │ kind: job, mode: upsert, │
 │                  │                                        │ merge_keys:[message_id]  │
 │                  │                                        ▼                         │
 │                  │   Lance: wa_messages, wa_statuses ──► pipelines / RAG / joins    │
 └────────────────────────────────────────────────────────────────────────────────────┘
```

**Which component answers which question:**

- **Component 2 (webhook)** — every inbound message; every outbound receipt (with
  per-message `pricing.category` / `billable`, so sent / delivered volume and
  cost can be aggregated straight from `wa_statuses`, at finer granularity than
  Meta's analytics fields and without the one-year lookback); and, once the
  management fields are subscribed, *change events* for template review,
  phone-number quality, and account review, by adding a `field` under `routes`.
  It never yields a full list of templates or numbers, only deltas, and it has no
  backfill: history from before the subscription is gone (**the earlier it ships,
  the more it accumulates**). It also never carries **the content of outbound
  messages**; see [Outbound message content](#outbound-message-content).
- **Component 1 (pack)** — the current template list, phone-number list, and
  business profile, as Meta sees them right now. No messages.
- **Component 3 (analytics, optional)** — Meta's own aggregate numbers. Worth
  building only when Meta's reporting figures are required to match exactly, or
  to recover up to a year of metrics from before the webhook subscription.

## Detailed design

### Component 1: the `whatsapp` source pack (pull)

Files: `packs/whatsapp.yaml` + `packs/whatsapp.rs` (`OnceLock` + `loader::builtin`)
+ one line in `packs/mod.rs` + the `builtin_packs.rs` registry entry
+ `packs/fixtures/whatsapp/` + `docs/open-connector-whatsapp.md`. All of it
follows the `source-pack` skill.

| Table | Action | Row path | Pagination | Resources | Notes |
|---|---|---|---|---|---|
| `phone_numbers` | `whatsapp.get_phone_numbers` | `$.phone_numbers` | `single_page`, `page_size_input: limit`, `page_size: 100`, `next_cursor_path: "$.paging.cursors.after"` as a tripwire | optional `wabaId` (defaults to the WABA on the OC connection) | The action does not forward `after`; more than 100 numbers trips the wire instead of silently truncating — the pack's established behavior |
| `business_profile` | `whatsapp.get_business_profile` | `$`, `row_shape: object` | `single_page` | required `phoneNumberId` | A point read; one binding per phone number (precedent: `feishu.document_content`) |
| `message_templates` | `whatsapp.get_message_templates` | `$.templates` | `cursor`, `cursor_input: after`, `next_cursor_path: "$.paging.cursors.after"`, `page_size: 100` | optional `wabaId` | Meta omits `paging.next` on the last page but may still return `cursors.after`: **the termination condition must be live-verified**, and may need `has_more_path` (OC has no normalized `hasMore` today, so OC may need to add one) |

`phone_numbers` columns (from OC's `phoneNumber` schema): `id`,
`display_phone_number`, `verified_name`, `quality_rating`,
`code_verification_status`, `platform_type`, `last_onboarded_time` (utf8; Meta
returns an ISO string — decide on a timestamp type after live verification),
`throughput_level` (path `throughput.level`), `webhook_application` (path
`webhook_configuration.application`).

`message_templates` columns: `id`, `name`, `status`, `category`, `language`,
`components` (`json`). Filter pushdown: `status = {status}`, `category = {category}`,
`language = {language}`, all `op: eq`, `fidelity: exact` (Meta filters server-side).

Context binding example:

```yaml
- name: saas
  type: open_connector
  connection_string: http://open-connector:3000
  hierarchy_level: catalog
  open_connector:
    runtime_token_env: OPEN_CONNECTOR_TOKEN
    bindings:
      - name: wa
        source_pack: whatsapp
        connection_alias: acme-waba           # OC connection holds the access token + default wabaId
        tables: [phone_numbers, message_templates]
      - name: wa_support
        source_pack: whatsapp
        connection_alias: acme-waba
        resource: { phoneNumberId: "1065xxxxxxxxxxx" }
        tables: [business_profile]
```

```sql
SELECT display_phone_number, quality_rating, throughput_level
FROM saas.wa.phone_numbers;

SELECT name, language, status
FROM saas.wa.message_templates
WHERE status = 'REJECTED';
```

**Fingerprint note:** OC's `phoneNumber` and `template` schemas are
`looseObject` (`additionalProperties: true`), so the fingerprint covers only
declared keys and undeclared columns sit outside the gate — the same situation
as GitHub / Notion passthrough rows. Live verification must confirm a non-NULL
value for every mapped column, as the skill requires.

### Component 2: the `kind: webhook` receiver (push)

This is the only part that adds a new feature to skardi-server. Principle: **the
only Meta-specific pieces are the verification preset and the envelope
unwrapping**; everything else (column mapping, writing, dedup, querying) is reused.

#### Resource YAML

```yaml
kind: webhook

metadata:
  name: whatsapp-inbound                 # route: /webhooks/whatsapp-inbound
  version: "1.0.0"

spec:
  provider: meta_whatsapp                # verification preset: hub.challenge handshake + X-Hub-Signature-256
  verify_token_env: WHATSAPP_VERIFY_TOKEN
  app_secret_env: WHATSAPP_APP_SECRET
  max_body_bytes: 4194304                # Meta caps at 3 MB; leave headroom

  routes:
    - field: messages                    # entry[].changes[] where field == "messages" and value.messages is non-empty
      destination: { table: staging.main.wa_events_messages, mode: append }
      columns:                           # identical grammar to pack columns
        - { name: message_id,          path: id,                       type: utf8, nullable: false }
        - { name: waba_id,             path: _waba_id,                 type: utf8, nullable: false }
        - { name: phone_number_id,     path: _metadata.phone_number_id, type: utf8, nullable: false }
        - { name: display_phone_number,path: _metadata.display_phone_number, type: utf8, nullable: true }
        - { name: from_wa_id,          path: from,                     type: utf8, nullable: false }
        - { name: contact_name,        path: _contact.profile.name,    type: utf8, nullable: true }
        - { name: received_at,         path: timestamp,                type: timestamp_s_string_utc, nullable: false }
        - { name: message_type,        path: type,                     type: utf8, nullable: false }
        - { name: text,                path: text.body,                type: utf8, nullable: true }
        - { name: media_id,            path: _media.id,                type: utf8, nullable: true }
        - { name: media_mime_type,     path: _media.mime_type,         type: utf8, nullable: true }
        - { name: caption,             path: _media.caption,           type: utf8, nullable: true }
        - { name: reply_to_message_id, path: context.id,               type: utf8, nullable: true }
        - { name: interactive_reply_id,path: _interactive_reply.id,    type: utf8, nullable: true }
        - { name: raw,                 path: _raw,                     type: json, nullable: false }
    - field: statuses
      destination: { table: staging.main.wa_events_statuses, mode: append }
      columns:
        - { name: message_id,       path: id,                       type: utf8, nullable: false }
        - { name: status,           path: status,                   type: utf8, nullable: false }
        - { name: status_at,        path: timestamp,                type: timestamp_s_string_utc, nullable: false }
        - { name: recipient_id,     path: recipient_id,             type: utf8, nullable: false }
        - { name: conversation_id,  path: conversation.id,          type: utf8, nullable: true }
        - { name: conversation_origin, path: conversation.origin.type, type: utf8, nullable: true }
        - { name: pricing_category, path: pricing.category,         type: utf8, nullable: true }
        - { name: billable,         path: pricing.billable,         type: boolean, nullable: true }
        - { name: error_code,       path: _error.code,              type: int64, nullable: true }
        - { name: raw,              path: _raw,                     type: json, nullable: false }
```

#### Request handling

1. **Route** `GET|POST /webhooks/:name`, added next to the route table in
   `server.rs`; it **bypasses** `auth_layer` (Meta will not carry our API token)
   and relies on the preset for authentication. An undeclared `name` returns 404.
2. **GET handshake** (`provider: meta_whatsapp` preset): check
   `hub.mode == subscribe` and compare `hub.verify_token` with
   `WHATSAPP_VERIFY_TOKEN` in constant time; reply 200 `text/plain` with
   `hub.challenge` verbatim; otherwise 403.
3. **POST verification:** read the raw body (**before** any JSON parsing),
   compute `HMAC-SHA256(body, WHATSAPP_APP_SECRET)`, and compare with
   `X-Hub-Signature-256` in constant time; on mismatch return 401 and `warn!`.
   Bodies over `max_body_bytes` return 413.
4. **Envelope unwrapping** (Meta-specific, ~60 lines of Rust): walk
   `entry[i].changes[j]`, match `change.field` against `routes[].field`; for each
   `value.messages[k]` emit one flat object: the original message object plus
   synthetic keys `_waba_id` (`entry.id`), `_metadata` (`value.metadata`),
   `_contact` (the `value.contacts[]` entry whose `wa_id == from`), `_media`
   (`message[message.type]`, only for type ∈ image / video / audio / document /
   sticker), `_interactive_reply` (`interactive.button_reply` or
   `interactive.list_reply`), `_error` (`errors[0]`), and `_raw` (the whole
   original object, for the `json` column). `statuses` is handled the same way.
   Why unwrap instead of letting the mapper walk wildcard paths: `RowPath`
   deliberately supports object keys only (header comment in `row_path.rs`), and
   column paths likewise support neither `$` nor array indexes
   (`FieldMapping::path` in `schema.rs`). That is the pack contract and it is not
   relaxed for webhooks, so first-element picks and whole-row passthrough go
   through synthetic keys.
5. **Column mapping:** hand the flat rows to `JsonToArrow::from_columns(columns)`
   to get a `RecordBatch`. Type errors use the pack wording (column name, row
   index, JSON kind; never the value).
6. **Write to staging:** `SqlDmlDestination::write` (`mode: append`, one
   transaction). The target source must be a Postgres / MySQL / SQLite entry in
   `WRITABLE_SOURCE_TYPES` with `access_mode: read_write`. **Do not write Lance
   directly:** every Lance commit is a new version, so per-webhook writes fragment
   the dataset and eat into the 250 ms response budget.
7. **Response:** 200 on a successful write; 500 on failure so Meta retries (for up
   to 7 days). That retry is our only durability guarantee, so **do not** buffer
   asynchronously and acknowledge early.
8. **Observability:** `webhook_requests_total{name, outcome}`,
   `webhook_rows_total{name, field}`, a processing-latency histogram; single-line
   logs carrying `name`, `waba_id`, `phone_number_id`, and the row count.

#### Dedup and merge into Lance (zero new code)

The staging table is an append-only event log and tolerates duplicates (Meta
retries and multi-app subscriptions both redeliver). Dedup is delegated to the
existing Lance upsert:

```yaml
kind: job
metadata: { name: wa-merge-messages, version: "1.0.0" }
spec:
  query: |
    SELECT message_id, waba_id, phone_number_id, from_wa_id, contact_name,
           received_at, message_type, text, media_id, media_mime_type, caption,
           reply_to_message_id, interactive_reply_id, raw
    FROM staging.main.wa_events_messages
    WHERE received_at >= {since}
  destination:
    table: wa_messages
    mode: upsert
    merge_keys: [message_id]
    repeated_keys: keep_last
    create_if_missing: true
```

`wa_statuses` merges on `[message_id, status]` (a message has at most three
receipts: sent / delivered / read). The job is triggered every few minutes by an
external cron; the staging table can be pruned on a retention schedule.

**Staging backend (decided):** the staging table lives in a transactional SQL
source Skardi can already write to — Postgres, MySQL, or SQLite with
`access_mode: read_write`. Open-source users choose: SQLite gives a
zero-dependency single-node setup (a local file plus a Lance directory), at the
cost of a single writer that may queue under peak load and hand Meta a 500 and
a retry; Postgres suits multi-replica or high-traffic deployments. The docs ship
both examples. On skardi-cloud the workspace already has Postgres, and the
operator renders the destination to it; SQLite is not used there. Switching is
a change to `destination.table` only.

The staging table is not just a buffer: it is queryable by SQL before the merge
job runs, it is where repairs and manual backfills land, and it can be pruned
on a retention schedule once rows have been merged into Lance.

#### Public ingress on skardi-cloud (decided)

Self-hosted deployments expose skardi-server's `/webhooks/:name` themselves.
On skardi-cloud, each tenant gets its endpoint through the existing
`skardi-gateway`, which already serves a public, unauthenticated, per-tenant
URL shape (`https://<org-slug>.api.skardi.cloud/external/<tenant_slug>/<endpoint>`)
behind an ALB with a CA-issued wildcard certificate — exactly what Meta
requires. The webhook path reuses that host and routing model:

```
GET|POST https://<org-slug>.api.skardi.cloud/webhooks/<tenant_slug>/<name>
   └─ gateway: host → org, path → workspace route table
        └─ raw passthrough → skardi-server.<workspace>.svc /webhooks/<name>
```

Division of responsibilities:

- **The gateway** does route-table lookup, a body-size cap (Meta sends at most
  3 MB; cap at 4 MB), per-tenant rate limiting, and metering. It forwards both
  GET and POST with the body **byte-for-byte** and the headers intact, and
  stamps no identity — the request is unauthenticated by design.
- **skardi-server** does the `hub.challenge` handshake and the HMAC check. The
  verify token and app secret stay in the workspace's environment; the gateway
  never holds a tenant's Meta secrets, matching the existing boundary where OC
  credentials never enter the gateway either.
- **The route table** only lists `<name>`s that the workspace declares as a
  `kind: webhook` resource (the operator renders it from the bound context, as
  it does for pipelines). Undeclared names are 404 at the gateway, so junk
  traffic never reaches a worker.

Why not reuse `/external/<endpoint>`: it is POST-only, its meaning is "run this
pipeline" (and it is metered as a pipeline run), and it may reshape the request
body — any of which breaks the GET handshake or the signature check.

Two things to size for: the gateway is single-replica today, so webhook
availability equals gateway availability (Meta's 7-day retry turns an outage
into a delay, not a loss, but gateway rollout windows must be short); and
metering needs its own counter (ingest events), because webhook deliveries are
not pipeline runs.

#### Outbound message content

`wa_messages` is in practice an **inbound** table. For messages the business
sends, the Cloud API echoes only `statuses` (delivered, read, billing), never the
body; `smb_message_echoes` exists only in Coexistence setups and covers messages
typed by hand in the WhatsApp Business App. A complete conversation requires the
sending side to record its own copy at send time:

- A caller that sends through OC `whatsapp.send_*` takes the returned
  `messages[0].id` and writes a row into `wa_events_messages` through the
  existing pipeline `INSERT … VALUES {rows}`, with `from_wa_id` set to the
  business number and `message_type` set to an agreed value such as `outbound_text`.
- The merge job does not distinguish direction: `message_id` is unique for both
  inbound (`wamid.…`) and outbound (Meta returns ids in the same namespace), and
  the status table joins on `message_id` as usual.

This is outside component 2's code scope but must be documented, or users will
assume the table holds full conversations.

#### Query examples

```sql
-- customer messages in the last 24h with no reply (joined against statuses)
SELECT m.from_wa_id, m.contact_name, m.text, m.received_at
FROM wa_messages m
LEFT JOIN wa_statuses s ON s.recipient_id = m.from_wa_id AND s.status_at > m.received_at
WHERE m.received_at >= now() - INTERVAL '1 day' AND s.message_id IS NULL;

-- join against the CRM by phone number
SELECT c.account_name, count(*) AS inbound
FROM wa_messages m JOIN crm.public.contacts c ON c.phone_e164 = concat('+', m.from_wa_id)
GROUP BY 1 ORDER BY 2 DESC;
```

RAG: `wa_messages.text` feeds the existing `chunk` / embedding UDFs and the
`documents` flow directly, with `message_id` as the document id.

#### Why a generic `kind: webhook` rather than a hard-coded WhatsApp route

The only extra abstraction is the `provider` preset layer. Beyond
`meta_whatsapp`, Slack Events (`url_verification` + `X-Slack-Signature`), Stripe
(`Stripe-Signature`), and GitHub (`X-Hub-Signature-256`) all have the same shape.
Column mapping and writing are generic already. The first version implements only
the `meta_whatsapp` preset, but neither the YAML nor the route mentions WhatsApp.

### Component 3 (optional): analytics tables (needs one OC PR)

Optional, see [Recommended architecture](#recommended-architecture) for when it
is worth building.

**OC side — submitted upstream as
[oomol-lab/open-connector#731](https://github.com/oomol-lab/open-connector/pull/731)**
(branch `bakey:feat/whatsapp-analytics`, commit `30db2fd5`). The PR adds the
three read actions below plus the `after` cursor on `get_phone_numbers`, with a
provider test covering the modifier rendering, the `data[]` flattening, the
template edge's query encoding, and the window validation. `npm run fix-check`,
catalog generation, the action-example check, and the provider guard suites all
pass. Not yet done: a live call against a WABA (no credentials in the authoring
environment), which skardi's own live verification covers before the pack lands.

| New read action | Graph call | Inputs (all optional, with defaults) | Normalized output |
|---|---|---|---|
| `get_message_analytics` | `GET /{waba_id}?fields=analytics.start(s).end(e).granularity(g)[.phone_numbers([..])][.product_types([..])][.country_codes([..])]` | `waba_id`, `start` / `end` (epoch seconds; default end = now, start = end − 30 d), `granularity` (`HALF_HOUR\|DAY\|MONTH`, default `DAY`), `phone_numbers[]`, `product_types[]`, `country_codes[]` | `{ granularity, data_points: [{start, end, sent, delivered}] }` |
| `get_pricing_analytics` | Same shape, field `pricing_analytics`, `granularity ∈ DAILY\|HALF_HOUR\|MONTHLY`, `metric_types`, `pricing_types`, `pricing_categories`, `dimensions` | As above plus dimension parameters | `{ data_points: [{start, end, phone_number?, country?, pricing_category?, pricing_type?, tier?, volume, cost}] }` (flattening `data[].data_points[]`) |
| `get_template_analytics` | **Edge**, not a field: `GET /{waba_id}/template_analytics?start&end&granularity=DAILY&template_ids=[..]&metric_types=[..]&use_waba_timezone&after` (DAILY is the only granularity the edge accepts; ≤10 templates; 90-day history; template analytics must be enabled on the WABA first) | `template_ids[]` (required), `start` / `end`, `metric_types[]`, `use_waba_timezone`, `after` | `{ data_points: [{template_id, start, end, sent, delivered, read, clicked: [{type, button_content, count}], cost: [{type, value}]}], paging }` |

Encoding details settled in the PR: inside the dot-chained modifiers, scalars are
bare (`granularity(DAY)` must be unquoted) and lists are JSON arrays; empty
lists are omitted, which Meta reads the same as an absent filter. `start` /
`end` default to the last 30 days, and `start >= end` is a 400 before any request.

Left for a separate OC PR, deliberately: the Graph version bump (v23.0 → v26.0)
and `Retry-After` parsing. Both touch every WhatsApp action, so they should not
ride on a feature PR.

**Skardi side:** three more pack tables with `row_path: "$.data_points"` and
`single_page` (Meta's analytics fields do not paginate). Time ranges: pack
`filters` support only `eq | gt | gt_eq`, so `period_start >= {ts}` is pushed as
`start` with `format: epoch_seconds`, `fidelity: inexact`, and `end` defaults to
now on the OC side. For an exact window or half-hour granularity, use
`open_connector_query('saas', 'whatsapp.pricing_analytics', '{"start":…,"end":…,"granularity":"HALF_HOUR"}', 'acme-waba')`.

**Persisting into Lance (escaping the one-year lookback):** one `kind: job`,
triggered daily by an external cron (Skardi has no scheduler), `mode: upsert`,
`merge_keys: [phone_number, pricing_category, period_start]`,
`repeated_keys: keep_last` — re-running is idempotent. skardi-cloud's `skardi-etl`
can generate this bundle directly.

### Deferred follow-ups (on demand)

- **Media archiving:** `media_id` from a webhook is valid for 7 days. Needs a new
  OC `download_media` action (a bounded fetch with Bearer → transit file, modelled
  on `feishu/shared/media.ts:158`), then a job to move files into object storage.
  The existing `get_media_info` URL expires in 5 minutes, so a table over it is pointless.
- **Twilio pack:** for customers on the Twilio BSP, `twilio.list_messages` with a
  `from: whatsapp:+…` filter; pure pack, but live-verify the filter passthrough first.
- **Multi-tenancy:** Embedded Signup v4 → one OC connection per alias; a single
  webhook endpoint fans out by `entry.id` (WABA) into the `waba_id` column of one
  table, which is multi-tenant by construction.
- **Coexistence history import:** for numbers migrated from the WhatsApp Business
  App, the `history` field replays 180 days; add `field: history` under `routes`,
  with slightly different unwrapping (chunking, `progress`).
- **Delegating to OC triggers:** if Open Flow is ever deployed, steps 2–4 of
  component 2 can call OC's `POST /v1/providers/whatsapp/triggers/:id/execute` with
  the `receive` operation and the app secret moves back into OC. Model
  "verify + unwrap" as a trait now so it can be swapped.

## Execution plan

| Milestone | Repo / branch | Content | Estimate | Prerequisites |
|---|---|---|---|---|
| M1 pack (component 1) | skardi `feature/open-connector-whatsapp-pack` | Three tables, fixtures (null / null-parent / empty / nested / extra-field / schema-mismatch), fingerprint capture and drift-refusal tests, MockGateway e2e, `docs/open-connector-whatsapp.md`, status paragraph in `docs/open-connector.md` | 2–3 days | A Meta developer account with a WABA and a test number, plus a local OC gateway (`~/Workspace/open-connector`) |
| M2 webhook (component 2) | skardi `feature/webhook-source` | `kind: webhook` loading, route, `meta_whatsapp` preset, unwrapper, reuse of column mapping and writers, metrics, docs `docs/webhooks.md` + `docs/whatsapp/` examples | 1–2 weeks | One public HTTPS address (cloudflared / ngrok for local development) |
| M2b merge jobs | skardi (with M2) | `wa-merge-messages` / `wa-merge-statuses` example jobs + skardi-etl template | 0.5 day | M2 |
| M3a OC actions (component 3, optional) | open-connector [#731](https://github.com/oomol-lab/open-connector/pull/731) — **submitted** | Three analytics read actions and `after` on `get_phone_numbers` (Graph version bump and `Retry-After` split out to a follow-up) | done, awaiting upstream review | None |
| M3b analytics tables (component 3, optional) | skardi, same branch as M1 or a new one | Three pack tables + snapshot job example + docs | 1–2 days | M3a merged and included in the OC version Skardi pins |
| Deferred | on demand | See deferred follow-ups | — | — |

M1 and M2 are both required for a complete integration. They share no code and
can run in parallel by different people, each as its own PR. If they have to be
sequenced, **start M2**: it is the only path that yields messages, and with no
backfill the earlier it ships the more data accumulates; M1 is cheap and can
follow at any time. M3 is optional and follows the upstream OC merge cadence.
Each milestone carries tests and docs as AGENTS.md requires, with no drive-by
refactors.

## Verification plan

- **M1 / M3 (packs):** follow the `source-pack` skill's live verification
  strictly: run OC locally, read every table across multiple pages with a real
  token, and confirm at least one real non-NULL value per mapped column; the
  termination condition of `message_templates` (the shape of `paging` on the
  last page) must be proven by a live capture; fingerprints come from live
  captures, never hand-written. Without credentials, use the 403-vs-400 trick to
  validate input shapes first.
- **M2 (webhook):**
  - Unit tests for HMAC and the challenge using fixed vectors from Meta's docs;
    one case each for a bad signature, a missing header, and an oversized body.
  - Unwrapper fixtures built from the official `messages` samples (text / image /
    interactive / unsupported with errors) and `statuses` samples (sent /
    delivered / read / failed), asserting every synthetic key.
  - End-to-end: the same payload delivered twice → two staging rows → one Lance
    row after the merge job; a failed write returns 500 and does not swallow the error.
  - Real device: App Dashboard "Send test webhook" plus messages to and from the
    test number, tunnelled via cloudflared to a local skardi-server, rows visible by SQL.
  - Load: a POST batching 1000 updates returns within 250 ms (Meta's latency target).

## Risks and open questions

1. **Public ingress — decided.** Self-hosted users expose skardi-server's
   `/webhooks/:name` themselves; on skardi-cloud, `skardi-gateway` serves
   `/webhooks/<tenant_slug>/<name>` on the existing per-org host as a raw
   passthrough, with verification staying in skardi-server. See
   [Public ingress on skardi-cloud](#public-ingress-on-skardi-cloud-decided).
   Still open on the cloud side: the metering counter for ingest events, and
   the gateway's single-replica availability during rollouts.
2. **Staging backend — decided.** skardi-cloud uses the workspace's Postgres;
   open-source users choose between SQLite (zero dependencies, single writer)
   and Postgres. See [Staging backend](#dedup-and-merge-into-lance-zero-new-code).
3. **`message_templates` paging termination.** The OC action forwards Meta's
   `paging` as-is; whether `cursors.after` is empty on the last page needs live
   verification. If it is non-empty with no `next`, OC must normalize a `hasMore`
   (the same class of problem Dropbox had).
4. **Upstream OC cadence.** The OC PR is submitted
   ([oomol-lab/open-connector#731](https://github.com/oomol-lab/open-connector/pull/731));
   component 3 still depends on it merging and entering the OC version Skardi
   pins. Until then analytics can be fetched ad hoc through the OC proxy, but
   cannot enter a pack (`open_connector_scan` rejects the proxy). The request
   shapes were cross-checked against two open-source clients but not yet
   exercised against a live WABA.
5. **Privacy and compliance.** `from_wa_id` is a phone number and `text` is the
   customer's own words. The Lance tables should fall under the existing query
   audit (`--query-audit-db`), masked views at the pipeline layer should be
   considered, and a retention policy is still to be decided.
6. **Meta version and policy drift.** OC is on v23.0; Graph versions retire after
   about two years. Embedded Signup v2 retires on 2026-10-15. Analytics lookback
   is one year. All of this goes into the OC PR and the docs.
7. **Job scheduling stays external.** Both the merge jobs and the analytics
   snapshot jobs need cron / Claude routines / skardi-cloud scheduling. A built-in
   scheduler is out of scope here, but it will become the natural ask the moment a
   second webhook-shaped source appears.
8. **Whether to build the Twilio pack.** Only if there are customers routing
   WhatsApp through Twilio; without demand, skip it.
