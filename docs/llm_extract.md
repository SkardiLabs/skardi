# `llm_extract` — LLM structured-extraction scalar UDF

`llm_extract` turns a column of unstructured text (with an optional image
reference for multimodal escalation) into a list of structured JSON entities,
extracted by Claude and guided by a JSON Schema. It is source-agnostic: any
pipeline that has text rows can call it and `UNNEST` the result into entity
rows. There is **no** build dependency on the `documents` connector.

It is behind the `llm-extract` Cargo feature:

```toml
# crates/skardi/Cargo.toml
llm-extract = ["dep:reqwest", "dep:base64"]
# crates/server/Cargo.toml
llm-extract = ["skardi/llm-extract"]
```

## Signature

```sql
llm_extract(text, image_ref, json_schema) -> List<Utf8>
```

| arg | kind | meaning |
|-----|------|---------|
| `text` | `Utf8` **column** | the unstructured text for the row |
| `image_ref` | `Utf8` **column**, nullable | URI of an image for multimodal escalation; `NULL` for pure-text sources |
| `json_schema` | `Utf8` **literal** | JSON Schema describing the fields to extract per entity |

`image_ref` accepts `data:<mime>;base64,<payload>` URIs, `http(s)://` URLs, and
local file paths (optionally `file://`-prefixed).

The return type is **always** `List<Utf8>` — a list of JSON entity strings. A
scalar UDF's `return_type` only sees argument *types*, not the `json_schema`
literal value, so it cannot synthesize a typed struct. Callers `UNNEST` the list
and extract/cast fields downstream.

## Output contract

Each list element is a JSON object string containing the `json_schema`
properties **plus** reserved keys:

| key | type | meaning |
|-----|------|---------|
| `_confidence` | number | model confidence 0–1 for this entity |
| `_status` | string | `ok` \| `low_confidence` \| `error` |
| `_error` | string | present only when `_status = "error"` |

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
| `ANTHROPIC_API_KEY` | — | required; warned about at startup if missing |
| `LLM_EXTRACT_MODEL` | `claude-opus-4-8` | Claude model id |
| `LLM_EXTRACT_THRESHOLD` | `0.75` | confidence gate |
| `LLM_EXTRACT_MAX_CALLS` | unlimited | per-query cap on multimodal escalation calls; once exhausted, weak rows stay `low_confidence` |

## Composition examples

Extract entities from a plain text column and expand them into rows:

```sql
SELECT UNNEST(
  llm_extract(
    t.body,
    NULL,
    '{"type":"object","properties":{"name":{"type":"string"},"price":{"type":"number"}}}'
  )
) AS entity
FROM (SELECT 'Widget Pro costs $19.99.' AS body) t;
```

Over parsed documents (the `documents` connector is just one producer of text —
the coupling is purely SQL, not Rust), with multimodal escalation using a page
image reference:

```sql
SELECT UNNEST(llm_extract(page.markdown, page.image_uri, '{ ...schema... }'))
FROM document_pages page;
```

Over Feishu / Notion / a DB text field — identical shape, only the source
table changes.

Downstream, the JSON entity strings can be parsed (e.g. via a DataFusion JSON
function, or by landing them in a Postgres `JSONB` column exposed as typed
generated columns).

## Testing

Deterministic offline tests mock the `CompletionProvider` — no network:

```bash
cargo test -p skardi --lib model::llm_extract --features llm-extract
cargo test -p skardi --test llm_extract_composition --features llm-extract
```

Opt-in live test against real Claude (off by default):

```bash
LLM_EXTRACT_LIVE=1 ANTHROPIC_API_KEY=sk-... \
  cargo test -p skardi --test llm_extract_live --features llm-extract -- --ignored
```
