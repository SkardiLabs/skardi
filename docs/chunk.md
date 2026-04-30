# Text Chunking (`chunk`)

> **Requires `--features chunking`** — build with:
> ```bash
> cargo build --release -p skardi-server --features chunking
> ```
>
> To chain chunking with inline embeddings in a single pipeline, combine features:
> ```bash
> cargo build --release -p skardi-server --features "chunking,embedding"
> ```

`chunk` is a DataFusion scalar UDF that splits text into smaller pieces directly inside SQL, so document ingestion can chunk inline alongside embedding and writing — no out-of-band Python step. It wraps the [`text-splitter`](https://crates.io/crates/text-splitter) crate.

## Function Signature

```sql
chunk(mode, text, size [, overlap]) -> List<Utf8>
```

| Argument | Type | Description |
|----------|------|-------------|
| `mode` | string literal | Splitter to use. Supported: `'character'`, `'markdown'`. |
| `text` | `Utf8` | The text to split. May be a literal, a scalar subquery, or a column reference. `NULL` rows pass through unchanged. |
| `size` | integer literal | Target maximum chunk length in characters. Must be `> 0`. |
| `overlap` | integer literal | Optional. Characters of overlap between adjacent chunks. Must be strictly less than `size`. Defaults to `0`. |

**Returns:** `List<Utf8>` — one element per chunk. A `NULL` input row produces a `NULL` list element.

Use `UNNEST(chunk(...))` to expand the list into one row per chunk.

## Modes

| Mode | Behavior |
|------|----------|
| `'character'` | Generic recursive splitter: tries paragraph → sentence → word → grapheme boundaries before falling back to character splits. Use for arbitrary plain text. |
| `'markdown'` | Markdown-aware: prefers heading / paragraph / code-block boundaries so chunks stay semantically coherent. Use when the input is Markdown (READMEs, wiki pages, knowledge bases). |

Both modes count length in characters (Unicode scalar values), not bytes or tokens.

> Token-based and code-aware splitters are planned follow-ups. See the [roadmap](../README.md#roadmap).

## Examples

### Split a literal

```sql
SELECT UNNEST(chunk('character', 'a long document body...', 1000)) AS piece;
```

### Per-row chunking over a table

```sql
SELECT id, UNNEST(chunk('markdown', body, 1000, 200)) AS chunk_text
FROM docs;
```

Each row in `docs` produces one row per chunk. `id` repeats; `chunk_text` carries the split.

### Inline ingestion: chunk → embed → write

The motivating use case — RAG ingest expressed as one SQL statement, no application code in between:

```sql
INSERT INTO doc_chunks
SELECT
  doc_id,
  chunk_text,
  candle('models/bge-small-en-v1.5', chunk_text) AS embedding
FROM (
  SELECT
    doc_id,
    UNNEST(chunk('markdown', body, 1000, 200)) AS chunk_text
  FROM raw_docs
);
```

Requires the `chunking` and `embedding` (or `candle`) features.

### Keep chunks as a list column

Skip the `UNNEST` to keep all chunks for a document together:

```sql
SELECT id, chunk('character', body, 500) AS chunks
FROM articles;
```

`chunks` is a `List<Utf8>` you can index, count with `array_length`, or expand later.

### As a pipeline

```yaml
params:
  source_table: { type: string }
  chunk_size:   { type: integer, default: 1000 }
  overlap:      { type: integer, default: 200 }
  model:        { type: string,  default: "models/bge-small-en-v1.5" }
sql: |
  WITH split AS (
    SELECT id,
           UNNEST(chunk('markdown', body, ${chunk_size}, ${overlap})) AS text
    FROM ${source_table}
  )
  SELECT id, text, candle('${model}', text) AS embedding
  FROM split
```

## Key Behaviors

- **Splitter is built once per batch.** Per-row construction would be wasteful; the underlying `TextSplitter` / `MarkdownSplitter` is constructed once and reused for every row in the batch.
- **`NULL` text → `NULL` list element.** Null inputs do not error and do not produce empty lists.
- **`size` and `overlap` must be literal integers.** They are read once per call, not per row.
- **`overlap < size` is enforced.** Equal or greater overlap returns an execution error rather than looping.
- **Subqueries work transparently.** Because `chunk` is a scalar UDF, scalar subqueries collapse to a single value before invocation, and relational subqueries simply feed rows into the parent scan — no special syntax needed.

## Troubleshooting

**"chunk: unsupported mode '<x>'"** — only `'character'` and `'markdown'` are supported today.

**"chunk: 'overlap' (N) must be strictly less than 'size' (M)"** — pick `overlap < size`. Setting `overlap = 0` is always valid.

**"chunk expects 3 or 4 arguments"** — the call shape is `chunk(mode, text, size [, overlap])`.

**Long literal embedded into SQL fails to parse** — large text passed as a SQL string literal must escape single quotes. Pass via a parameter / scalar subquery / column reference for anything beyond toy examples.
