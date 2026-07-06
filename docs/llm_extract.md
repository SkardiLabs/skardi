# `llm_extract` — LLM structured-extraction scalar UDF

`llm_extract` turns a column of unstructured text (with an optional image
reference for multimodal escalation) into a list of **typed struct** entities,
extracted by a cheap LLM chat model and guided by a JSON Schema. It is
source-agnostic: any pipeline that has text rows can call it and `UNNEST` the
result into entity rows, then project the schema's fields directly (no JSON
parsing — `entity.model`, `entity.price`, ...). There is **no** build
dependency on the `documents` connector.

Extraction is an easy task, so the default providers are cheap OpenAI-compatible
chat models (DeepSeek, GLM, Gemini, OpenAI). A native Anthropic provider is
available but is optional and **not** the default.

It is behind the `llm-extract` Cargo feature:

```toml
# crates/skardi/Cargo.toml
llm-extract = ["dep:reqwest", "dep:base64"]
# crates/server/Cargo.toml
llm-extract = ["skardi/llm-extract"]
```

## Signature

```sql
llm_extract(text, image_ref, json_schema) -> List<Struct<...schema fields, _confidence, _status, _error>>
```

| arg | kind | meaning |
|-----|------|---------|
| `text` | `Utf8` **column** | the unstructured text for the row |
| `image_ref` | `Utf8` **column**, nullable | URI of an image for multimodal escalation; `NULL` for pure-text sources |
| `json_schema` | `Utf8` **literal** | JSON Schema describing the fields to extract per entity |

`image_ref` accepts `data:<mime>;base64,<payload>` URIs by default. `http(s)://`
URLs and local file paths (optionally `file://`-prefixed) are accepted **only**
when `LLM_EXTRACT_IMAGE_FETCH=1` is set — see the SSRF / path-traversal note
under [Multimodal escalation](#️-image-fetching--ssrf--path-traversal).

The return type is **derived from the `json_schema` literal's value at plan
time**, not just its type — the same mechanism DataFusion's own
`arrow_cast(x, 'Int16')` uses (`ScalarUDFImpl::return_field_from_args`, which
sees the actual literal argument, not just its `DataType`). Since `json_schema`
is already required to be a string literal, `llm_extract` parses it once per
query and returns `List<Struct<...>>` with one field per schema property, so
`UNNEST` gives you typed columns directly — no downstream JSON parsing, no
extra UDF needed.

JSON-Schema-to-Arrow type mapping (deliberately scoped to the common
flat-extraction case, not a general-purpose mapper):

| JSON Schema `type` | Arrow type |
|---|---|
| `string` | `Utf8` |
| `number` | `Float64` |
| `integer` | `Int64` |
| `boolean` | `Boolean` |
| `array` of `string` items | `List<Utf8>` |
| `array` of anything else | `Utf8` (the array's raw JSON text — not dropped, just not broken into a typed list) |
| `object` | `Struct` of its own nested `properties`, recursively |
| missing / unrecognized / union (`["string","null"]`) | `Utf8` |

Field order in the resulting struct is **alphabetical by property name**, not
the order written in the schema — `serde_json`'s default `Map` is a
`BTreeMap` here. This only matters if you're projecting struct fields
positionally rather than by name (don't; use `entity.field_name`).

## Output contract

Each list element is a struct with one field per `json_schema` property
**plus** reserved fields:

| key | type | meaning |
|-----|------|---------|
| `_confidence` | `Float64` | model confidence 0–1 for this entity |
| `_status` | `Utf8` | `ok` \| `low_confidence` \| `error` |
| `_error` | `Utf8`, nullable | present only when `_status = "error"` |

A field the model didn't populate (or a whole entity in the `error` case) is
simply `NULL` on that struct field — no sentinel values, no missing-key
JSON lookups.

Behavior per input row:

1. **Text-first pass** — call Claude with `json_schema` + `text`, forcing
   structured output via tool-use. May return N entities → N list elements.
2. **Confidence gate** — an entity is *weak* if `_confidence < threshold` or a
   schema-`required` field is missing/null.
3. **Multimodal escalation** — if any entity is weak **and** `image_ref` is
   non-null (and the cost guard allows it), re-call Claude with the image
   attached; the escalated result replaces the row's entities.
4. **Never drop** — a provider/parse failure for a row yields a single
   `{_status:"error",_error:…}` entity. Other rows are unaffected; the query
   never fails because of one bad row.
5. **Empty/NULL text** — returns an empty list, with no LLM call.

## Configuration

| env var | default | meaning |
|---------|---------|---------|
| `LLM_EXTRACT_PROVIDER` | `deepseek` | which provider to use: `deepseek` \| `glm` \| `gemini` \| `openai` \| `anthropic` |
| `LLM_EXTRACT_MODEL` | per-provider default (below) | chat model id |
| `LLM_EXTRACT_THRESHOLD` | `0.75` | confidence gate |
| `LLM_EXTRACT_MAX_CALLS` | unlimited | per-query cap on multimodal escalation calls; once exhausted, weak rows stay `low_confidence` |
| `LLM_EXTRACT_VISION` | model-id heuristic | `true`/`false` override for whether the active model is vision-capable (gates multimodal escalation — see below) |
| `LLM_EXTRACT_IMAGE_FETCH` | `0` (off) | opt-in to fetch `http(s)://`/`file://`/local-path `image_ref`s; **off by default for SSRF/path-traversal safety** (see below) |
| `<PROVIDER>_API_KEY` | — | API key for the selected provider (see table); warned about at startup if missing |

### Providers

All four built-in providers are OpenAI-compatible chat APIs; a single
`OpenAiCompatibleCompletionProvider` covers them. The active one is chosen by
`LLM_EXTRACT_PROVIDER`. Structured output is forced via
`response_format: {type:"json_schema", …}`, with a `record_entities` tool +
`tool_choice` fallback for providers/models that reject `response_format`.

| `LLM_EXTRACT_PROVIDER` | base URL | API-key env | default model |
|------------------------|----------|-------------|---------------|
| `deepseek` (default) | `https://api.deepseek.com/v1` | `DEEPSEEK_API_KEY` | `deepseek-chat` |
| `glm` | `https://open.bigmodel.cn/api/paas/v4` | `GLM_API_KEY` | `glm-4-flash` |
| `gemini` | `https://generativelanguage.googleapis.com/v1beta/openai` | `GEMINI_API_KEY` | `gemini-2.0-flash` |
| `openai` | `https://api.openai.com/v1` | `OPENAI_API_KEY` | `gpt-4o-mini` |
| `anthropic` (optional, non-default) | Anthropic Messages API | `ANTHROPIC_API_KEY` | `claude-opus-4-8` |

`LLM_EXTRACT_MODEL` overrides the per-provider default model. Every
OpenAI-compatible provider is constructed at startup and warns (does not panic)
if its API-key env var is unset. An unknown `LLM_EXTRACT_PROVIDER` falls back to
`deepseek` with a warning.

### Multimodal escalation — vision gate

**Escalation requires a vision-capable model.** When a weak entity has a non-null
`image_ref`, `llm_extract` may re-run with the image attached (OpenAI `image_url`
base64 block, or an Anthropic image block). A text-only chat model — including
the **default `deepseek-chat`** — would reject an image block (HTTP 400), so
escalation is **gated on model vision capability**:

- At startup, vision capability is inferred from the model id (a heuristic
  matching `4o`, `-vl`, `glm-4v`, `vision`, `gemini-`, `claude`, …).
- Override explicitly with `LLM_EXTRACT_VISION=true|false`.
- When the active model is **not** vision-capable, escalation is **skipped** and
  weak entities stay `low_confidence` (they are never turned into `error` rows
  by a doomed image call).

If you rely on `image_ref` escalation, select a vision-capable model (e.g.
`gpt-4o-mini`, `gemini-2.0-flash`, a `*-vl`/`glm-4v` model, or a Claude model)
via `LLM_EXTRACT_PROVIDER` + `LLM_EXTRACT_MODEL`.

### ⚠️ Image fetching — SSRF / path-traversal

`image_ref` is **data-derived** (a column value), so fetching it is an
**SSRF and path-traversal risk**: an `http(s)://` ref can point at internal
metadata endpoints (e.g. `169.254.169.254`) or intranet hosts, and a
`file://`/local-path ref can read arbitrary files on the host. Therefore:

- **Default-deny.** Only `data:<mime>;base64,…` URIs (which carry the image bytes
  inline, no I/O) are accepted out of the box.
- `http(s)://`, `file://`, and bare filesystem paths are **refused** unless you
  explicitly opt in with `LLM_EXTRACT_IMAGE_FETCH=1`.
- Enable it only when `image_ref` values are trusted and the deployment can
  tolerate outbound fetches / local reads from a data-derived value. There is no
  built-in host allowlist; restrict egress and filesystem access at the
  deployment layer if you turn this on.

## Composition examples

Extract entities from a plain text column, expand them into rows, and project
typed fields directly — `UNNEST`'s result is a struct, so a wrapping query (a
CTE here) lets you reference it by field name with `.`:

```sql
WITH extracted AS (
  SELECT UNNEST(
    llm_extract(
      t.body,
      NULL,
      '{"type":"object","properties":{"name":{"type":"string"},"price":{"type":"number"}}}'
    )
  ) AS entity
  FROM (SELECT 'Widget Pro costs $19.99.' AS body) t
)
SELECT entity.name, entity.price, entity._confidence, entity._status
FROM extracted;
```

Over parsed documents (the `documents` connector is just one producer of text —
the coupling is purely SQL, not Rust), with multimodal escalation using a page
image reference:

```sql
WITH extracted AS (
  SELECT path, page, UNNEST(llm_extract(page.markdown, page.image_uri, '{ ...schema... }')) AS entity
  FROM document_pages page
)
SELECT path, page, entity.model, entity.price
FROM extracted;
```

Over Feishu / Notion / a DB text field — identical shape, only the source
table changes.

The struct fields are ordinary typed Arrow columns from here on — write them
to a job destination (Lance, Postgres, SQLite, ...) or query them directly,
with no JSON parsing step anywhere in the pipeline.

## Testing

Deterministic offline tests mock the `CompletionProvider` — no network:

```bash
cargo test -p skardi --lib model::llm_extract --features llm-extract
cargo test -p skardi --test llm_extract_composition --features llm-extract
```

Opt-in live test against the configured provider (off by default). It uses
whatever `LLM_EXTRACT_PROVIDER` / `LLM_EXTRACT_MODEL` select, defaulting to
DeepSeek:

```bash
# default provider (deepseek)
LLM_EXTRACT_LIVE=1 DEEPSEEK_API_KEY=sk-... \
  cargo test -p skardi --test llm_extract_live --features llm-extract -- --ignored

# or pick another provider
LLM_EXTRACT_LIVE=1 LLM_EXTRACT_PROVIDER=openai OPENAI_API_KEY=sk-... \
  cargo test -p skardi --test llm_extract_live --features llm-extract -- --ignored
```
