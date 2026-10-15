# WhatsApp Source Pack

The built-in `whatsapp` source pack exposes the **account side** of a
WhatsApp Business Account (WABA) — its phone numbers, each number's
business profile, its message templates, and Meta's three analytics
surfaces — as stable SQL tables through an
[Open Connector gateway](open-connector.md). The Meta access token lives
in Open Connector; Skardi holds only the gateway runtime token.

**Messages are not in this pack, and cannot be.** The WhatsApp Cloud API has
no endpoint for message history: Meta delivers an inbound message once, by
webhook, and your copy is the only one. Capturing messages is the
[`kind: webhook` receiver](webhooks.md)'s job; this pack and that receiver
are the two halves of the
[WhatsApp integration design](superpowers/specs/2026-10-13-whatsapp-integration-design.md),
and a deployment that wants both runs both.

**The wire contract is Open Connector's, not the Graph API's.** Two things
distinguish this provider from the packs before it. Its inputs are
**snake_case** — `waba_id`, `phone_number_id`, `template_ids` — because the
gateway's `whatsapp` executors forward Meta's own parameter spellings. And
its rows are **normalized**: every executor rebuilds its output through a
`compactObject`, so a field Meta did not send is an omitted key, never an
explicit null. Everything below is reconciled against a live gateway
(v1.9.0, commit `9a2ac5b6`, 2026-10-15), with every table's generated input
set accepted by the gateway's strict schemas. **Live verification against a
real WABA is in progress**; the verification section at the end states
exactly what has and has not been observed on the wire.

## Binding

```yaml
spec:
  data_sources:
    - name: saas
      type: open_connector
      connection_string: http://open-connector:3000
      hierarchy_level: catalog
      open_connector:
        runtime_token_env: OPEN_CONNECTOR_TOKEN
        bindings:
          - name: wa                    # schema name in SQL
            source_pack: whatsapp
            connection_alias: acme-waba   # the OC connection holding the token (+ default wabaId)
            tables: [phone_numbers, message_templates, message_analytics, pricing_analytics]
            # `waba_id` may be omitted when the OC connection carries a default WABA.

          - name: wa_support
            source_pack: whatsapp
            connection_alias: acme-waba
            resource: { phone_number_id: "106540352242922" }   # one binding per number
            tables: [business_profile]

          - name: wa_templates
            source_pack: whatsapp
            connection_alias: acme-waba
            resource:
              template_ids: ["1421988012088524", "2632273056924580"]   # ≤ 10 per Meta
              start: 1757000000                                        # optional; default: last 30 days
              end: 1760000000
            tables: [template_analytics]
```

```sql
SELECT display_phone_number, quality_rating, throughput_level
FROM saas.wa.phone_numbers;

SELECT name, language, status
FROM saas.wa.message_templates
WHERE status = 'REJECTED';                 -- pushed to Meta, re-applied locally

-- Cost by category over an explicit window, ad hoc, without a binding:
SELECT pricing_category, sum(volume) AS messages, sum(cost) AS cost
FROM open_connector_query(
  'saas', 'whatsapp.pricing_analytics',
  '{"start": 1757000000, "end": 1760000000, "granularity": "DAILY", "dimensions": ["PRICING_CATEGORY"]}',
  'acme-waba')
GROUP BY 1 ORDER BY 3 DESC;
```

## Tables

| Table | Action | Resources | Pagination | Filter pushdown |
|-------|--------|-----------|------------|-----------------|
| `phone_numbers` | `whatsapp.get_phone_numbers` | optional `waba_id` | cursor (`after`, 100 per page) | — |
| `business_profile` | `whatsapp.get_business_profile` | **required** `phone_number_id` | single object row | — |
| `message_templates` | `whatsapp.get_message_templates` | optional `waba_id`, `name_or_content` | cursor (`after`, 100 per page) | `status =`, `category =`, `language =` (all Inexact) |
| `message_analytics` | `whatsapp.get_message_analytics` | optional `waba_id`, `start`, `end`, `granularity`, `phone_numbers`, `product_types`, `country_codes` | single page | — |
| `pricing_analytics` | `whatsapp.get_pricing_analytics` | optional `waba_id`, `start`, `end`, `granularity`, `phone_numbers`, `country_codes`, `metric_types`, `pricing_types`, `pricing_categories`, `dimensions` | single page | — |
| `template_analytics` | `whatsapp.get_template_analytics` | **required** `template_ids`; optional `waba_id`, `start`, `end`, `metric_types`, `product_type`, `use_waba_timezone` | cursor (`after`; no page-size input) | — |

Columns, per table:

- **`phone_numbers`** — `id`, `display_phone_number`, `verified_name`,
  `quality_rating`, `code_verification_status`, `platform_type`,
  `last_onboarded_time` (text, Meta's ISO-8601-with-offset spelling),
  `throughput_level` (from `throughput.level`), `webhook_application`
  (from `webhook_configuration.application`).
- **`business_profile`** — `messaging_product`, `about`, `address`,
  `description`, `email`, `profile_picture_url`, `websites` (list),
  `vertical`. One row; the binding names the number.
- **`message_templates`** — `id`, `name`, `status`, `category`, `language`,
  `components` (JSON: header/body/footer/buttons as Meta shapes them).
  Every language variant of a template name is its own row with its own
  id and status, which is Meta's model.
- **`message_analytics`** — `period_start`, `period_end` (timestamps),
  `sent`, `delivered`.
- **`pricing_analytics`** — `period_start`, `period_end`, `volume`, `cost`,
  and one column per requested dimension: `phone_number`, `country`,
  `pricing_category`, `pricing_type`, `tier`. A dimension not in
  `dimensions` is NULL; `tier` (`"0:750000"`, `"0:MAX"`) appears only on
  billable utility and authentication rows, Meta's volume-tier rule. This
  table is the only source of cost **amounts**: delivery receipts captured
  by the webhook carry a pricing category and a billable flag, never money.
- **`template_analytics`** — `template_id`, `period_start`, `period_end`,
  `sent`, `delivered`, `read`, `clicked` (JSON list of
  `{type, button_content, count}`), `cost` (JSON list of `{type, value}`).

Behavior:

- **Window arguments are resources, not pushed filters.** `start`/`end` are
  UNIX seconds; a binding that omits them gets the gateway's rolling
  default (the last 30 days), and an ad hoc `open_connector_query` passes
  an exact window in its resource JSON. They are not a `period_start >=`
  pushdown because the window is a pair and the loader refuses a filter
  whose input is also a declared resource.
- **Template filters are Inexact.** `status`, `category`, and `language` are
  forwarded to Meta's server-side filters, and DataFusion re-applies the
  predicate afterwards: a string enum Meta does not recognise may be
  answered with the unfiltered listing. `name_or_content` is Meta's
  substring search over names and bodies, hence a resource. A `name = …`
  predicate is never pushed (the executor has no such input) and stays
  local.
- **Cursor pagination rides Meta's Graph cursor.** The gateway forwards
  `after` and re-emits Meta's `paging` object (`cursors.before/after`,
  `previous`, `next`) with absent keys dropped. Termination is an absent
  `paging.cursors.after`. A repeated cursor fails as a pagination loop; a
  scan never silently returns a prefix.
- **`template_analytics` has no page-size input.** The edge pages by cursor
  only; the pack's `page_size` is the `LIMIT` ceiling and is never sent.
  Meta caps `template_ids` at ten per call and accepts only `DAILY`
  granularity, which the gateway pins. Template analytics must be enabled
  once on the WABA (`POST /{waba_id}?is_enabled_for_insights=true`, or in
  WhatsApp Manager) or the action errors rather than returning empty data.
  With `use_waba_timezone: true`, timestamps passed as `start`/`end` are
  converted by the gateway to `YYYY-MM-DD`, which Meta requires in that mode.
- **Absent is omitted, never null.** The executors' normalizers drop keys
  Meta did not send, so a number without a webhook configuration, a free
  message row without a tier, or a template with no button clicks simply
  lacks the key — SQL NULL. A nested parent of the wrong shape still fails
  the scan loudly.
- **Excluded actions.** `get_phone_number` and `get_template_status` (point
  reads of rows the listings carry), `get_media_info` (a five-minute
  download URL that needs a Bearer header), and every `send_*`,
  `upload_media`, `create_message_template`, `delete_message_template`
  (writes; Open Connector sources are read-only in Skardi — send through
  the gateway's own action surface).
- **Provider errors surface as the gateway's own failure envelope**
  (`authorization_failed`, `rate_limited`, …) naming the action; never as a
  row-path error. `error_path` is unset everywhere.
- **Action-contract fingerprints are pinned** from live discovery, output
  and input schemas alike (`packs/fixtures/whatsapp/contracts/`). Any schema
  change in the gateway's `whatsapp` provider — additive included — fails
  registration with the table and action named; re-capture and re-pin on a
  gateway upgrade.

## Authorization and permissions

The Open Connector connection holds a Meta **System User** access token
(`api_key` auth) with an optional default `wabaId`. The pack's read actions
need `whatsapp_business_management`; nothing here needs
`whatsapp_business_messaging`. A token scoped to management only serves the
whole pack, which is a documentable authorization boundary.

## Rate limits and freshness

Business Management API calls are subject to Meta's business-use-case rate
limits (on the order of 200 calls per hour per app per WABA, higher for
active WABAs). Analytics fields are aggregates over a window and cheap to
poll daily; the listings are small. Reads are live by default; set
`cache_ttl_seconds` on the gateway to share a page across scans. Meta's
analytics lookback is one year (down from ten since 2025-12-01) — a
`kind: job` that snapshots `pricing_analytics` into Lance keeps history
beyond it.

## Verification

Reconciled against gateway v1.9.0 (commit `9a2ac5b6`) on 2026-10-15:

- The six action ids, every input key, every row path, and every column
  path were read from live discovery and the executor source; each table's
  full generated input set (pagination keys, resources, filter inputs) was
  sent to the gateway and reached the credential wall, so the strict
  schemas accept it.
- Fingerprints are pinned from that capture, output and input.

**Pending — live pass against a real WABA** (requires a token in the
gateway; the maintainer runs it before the pack is advertised as supported):

- registration through the fingerprint gate against live discovery;
- every mapped column extracting a real non-NULL value;
- Meta's terminal-page spelling observed with `limit: 1` on
  `message_templates` and `phone_numbers` — whether the last page omits
  `cursors.after` or carries it beside an omitted `next`. If it is the
  latter, the gateway needs a normalized has-more boolean before these
  tables can terminate soundly, and the pack will declare `has_more_path`
  against it;
- the three analytics fields returning rows on a WABA with traffic, and
  `template_analytics` on a WABA with insights enabled.

The fixtures under `packs/fixtures/whatsapp/` are executor-shaped pages
until that pass replaces them with redacted live captures.
