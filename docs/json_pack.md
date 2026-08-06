# JSON Encoding in SQL (`json_pack`)

> **Build flags:** none — `json_pack` is registered unconditionally on
> every `skardi-server` build.

`json_pack` is a DataFusion scalar UDF that builds a JSON **object** from
`(key, value)` argument pairs, entirely inside SQL. It exists because
nothing else can serialize JSON on this engine: DataFusion core has never
shipped a JSON encoder (checked through 54.x), and
`datafusion-functions-json` is read-side only (`json_get`, …).

Its flagship consumer is the etl generator's `metadata` packing, which
also makes it a **security boundary**: values are encoded by
`serde_json`, so untrusted provider strings — quotes, backslashes,
control characters — can never break out of the serialization. If you are
tempted to build JSON with string concatenation in SQL, use this instead.

## Function Signature

```sql
json_pack(key1, value1 [, key2, value2, ...]) -> Utf8
```

| Argument | Type | Description |
|----------|------|-------------|
| `key` | string **literal** | Object key. Non-null, unique across the call — the object's shape is author-defined, never data-driven. |
| `value` | column or literal | `Utf8`, `Boolean`, any `Int`/`UInt`/`Float`, `Timestamp` (encodes as epoch **milliseconds**), or `List<Utf8>` (encodes as a JSON string array). SQL `NULL` encodes as JSON `null`. |

**Returns:** `Utf8` — one JSON object per row.

## Contract

- Arguments come in pairs; an odd count is an error.
- Duplicate keys are an **error**, not last-wins — a statement carrying
  one is a bug worth surfacing.
- Keys appear in **argument order** (the crate enables `serde_json`'s
  `preserve_order`), so output is byte-deterministic — the property the
  etl generator's golden bundles pin.
- Non-finite floats (`NaN`, `±Inf`) are refused with a targeted error:
  JSON has no spelling for them, and silently encoding `null` would hide
  data corruption.
- Nested types other than `List<Utf8>` are rejected with a targeted
  error naming the offending type.

## Examples

### Pack row columns into a metadata object

```sql
SELECT json_pack('number', number, 'state', state) AS metadata
FROM saas.github_demo.issues;
-- {"number":42,"state":"open"}
```

### Mixed types, NULLs, and lists

```sql
SELECT json_pack(
  'value',      value,        -- Float64, NULL row → null
  'tags',       tags,         -- List<Utf8> → ["a","b"]
  'created_at', created_at    -- Timestamp → epoch millis
) AS metadata
FROM items;
-- {"value":0.5,"tags":["physics","qc"],"created_at":1767225600000}
```

### Why not string concatenation

```sql
-- DON'T: a title containing `", "admin": true` breaks out of your JSON.
SELECT '{"title": "' || title || '"}' FROM docs;

-- DO: the encoder escapes everything; there is no injection path.
SELECT json_pack('title', title) FROM docs;
```
