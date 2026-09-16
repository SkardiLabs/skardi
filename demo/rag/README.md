# Hybrid Search / RAG Demo

This demo has two flavours: the **server / PostgreSQL** version and a **local / SQLite** version. Both run the same way now: a `skardi-server` process registers the data source and hosts the pipeline endpoints, and `skardi` — a thin HTTP client — drives ingestion and search against it. The SQLite flavour just needs a local file and no Docker/Postgres.

---

## Server version — `skardi-server` + PostgreSQL + pgvector

This demo shows a complete hybrid search pipeline using Skardi,
backed by a **single PostgreSQL table** that holds both the raw content and
the vector embedding:

- **Vector search** — candle (bge-small-en-v1.5) embeddings + pgvector KNN (`pg_knn`)
- **Full-text search** — PostgreSQL `tsvector` / `websearch_to_tsquery` (`pg_fts`)
- **Hybrid search** — RRF (Reciprocal Rank Fusion) merging both results in SQL
- **One-shot ingestion** — a single INSERT writes content + embedding to the same row

```
                    ┌──────────────────────────────┐
                    │          Write Path           │
                    │                               │
   text ──────────► │  INSERT documents             │
                    │    (content, candle(content)) │
                    │                               │
                    │  ─► row is now visible to     │
                    │     both pg_fts and pg_knn    │
                    └──────────────────────────────┘

                    ┌──────────────────────────────┐
                    │          Read Path            │
                    │                               │
   query ─────────► │  pg_knn()  (top 80)           │──┐
                    │  pg_fts()  (top 60)           │──┤ RRF merge
                    │                               │  │
                    │  FULL OUTER JOIN + RRF        │◄─┘
                    │  ORDER BY rrf_score DESC      │
                    └──────────────────────────────┘
```

Because both signals live on the same row, you only need **one** data source
and **one** ingestion request — no MongoDB, no second write, no cross-store
consistency problem.

## Quick Start

### 1. Start PostgreSQL with pgvector

```bash
docker run --name rag-postgres \
  -e POSTGRES_DB=ragdb \
  -e POSTGRES_USER=skardi_user \
  -e POSTGRES_PASSWORD=skardi_pass \
  -p 5432:5432 \
  -d pgvector/pgvector:pg16
```

### 2. Create the schema and indexes

```bash
docker exec -i rag-postgres psql -U skardi_user -d ragdb << 'EOF'
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE documents (
    id BIGINT PRIMARY KEY,
    content TEXT NOT NULL,
    embedding vector(384)   -- bge-small-en-v1.5 dimension
);

-- HNSW index for vector search
CREATE INDEX ON documents USING hnsw (embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);

-- GIN index for full-text search over the same `content` column
CREATE INDEX documents_content_fts_idx
  ON documents
  USING GIN (to_tsvector('english', content));
EOF
```

### 3. Download the embedding model

```bash
# Requires Python 3.12 — run through the repo venv (huggingface_hub is
# installed there, not on the system interpreter)
source .venv/bin/activate

python -c "
from huggingface_hub import hf_hub_download
import os
model_dir = 'models/generated/bge-small-en-v1.5'
os.makedirs(model_dir, exist_ok=True)
for f in ['model.safetensors', 'config.json', 'tokenizer.json']:
    hf_hub_download('BAAI/bge-small-en-v1.5', f, local_dir=model_dir)
print(f'Model downloaded to {model_dir}')
"
```

### 4. Set credentials and start the server

```bash
export PG_USER="skardi_user"
export PG_PASSWORD="skardi_pass"

cargo run --bin skardi-server --features candle -- \
  --ctx demo/rag/server/ctx.yaml \
  --pipeline demo/rag/server/pipelines/ \
  --port 8080
```

---

## Write Path: Unified Ingestion

A **single request** writes both the FTS-searchable content and the pgvector
embedding into the same Postgres row. The `candle()` UDF embeds the text
inline during INSERT, so there is no second hop and nothing to keep in sync.

```bash
# Ingest document 1
curl -X POST http://localhost:8080/ingest/execute \
  -H "Content-Type: application/json" \
  -d '{
    "doc_id": 1,
    "content": "Vector databases store high-dimensional vectors and enable fast similarity search at scale."
  }' | jq .

# Ingest document 2
curl -X POST http://localhost:8080/ingest/execute \
  -H "Content-Type: application/json" \
  -d '{
    "doc_id": 2,
    "content": "Retrieval-Augmented Generation combines retrieval with a language model to ground responses in factual content."
  }' | jq .

# Ingest document 3
curl -X POST http://localhost:8080/ingest/execute \
  -H "Content-Type: application/json" \
  -d '{
    "doc_id": 3,
    "content": "The Transformer architecture introduced multi-head self-attention to replace recurrent networks."
  }' | jq .
```

Under the hood the pipeline SQL is simply:

```sql
INSERT INTO documents (id, content, embedding)
VALUES (
  {doc_id},
  {content},
  candle('models/generated/bge-small-en-v1.5', {content})
)
```

One row, one write — immediately searchable by both `pg_fts` and `pg_knn`.

### Long-form ingestion: chunk → embed → write

`/ingest` above expects each `content` to already be chunk-sized. For real
documents (a wiki page, a chapter, a long support thread) Skardi can do the
chunking inline — `chunk('markdown', ...)` splits the body, `UNNEST` expands
each chunk into its own row, and `candle()` embeds every chunk. One request,
N rows, all atomically searchable.

```bash
curl -X POST http://localhost:8080/ingest-chunked/execute \
  -H "Content-Type: application/json" \
  -d '{
    "doc_id": 100,
    "content": "# Vector Search\n\nVector databases index high-dimensional embeddings. They use approximate nearest-neighbour algorithms — HNSW, IVF, and product quantisation are common — to keep recall high while bounding latency.\n\n# Hybrid Search\n\nHybrid search merges semantic similarity with keyword relevance. Reciprocal Rank Fusion (RRF) is a common merge: each candidate gets a score of `weight / (60 + rank)` from each ranker, and the sums are sorted.\n\n# Practical Notes\n\nChunk size and overlap matter. Too small and chunks lose context; too large and precision drops. Markdown splitters preserve heading boundaries so each chunk stays semantically coherent.",
    "chunk_size": 250,
    "overlap": 50
  }' | jq .
```

Server SQL (see [server/pipelines/ingest_chunked.yaml](server/pipelines/ingest_chunked.yaml)):

```sql
INSERT INTO documents (id, content, embedding)
SELECT
  {doc_id} * 1000 + (ROW_NUMBER() OVER (ORDER BY 1) - 1) AS id,
  chunk_text                                              AS content,
  candle('models/generated/bge-small-en-v1.5', chunk_text) AS embedding
FROM (
  SELECT UNNEST(chunk('markdown', {content}, {chunk_size}, {overlap})) AS chunk_text
) c
```

Synthesised ids are `doc_id * 1000 + chunk_idx` (0-based), so doc 100
above becomes rows `100000, 100001, 100002 …`. Pick `doc_id`s that don't
collide with the single-row `/ingest` path.

Requires `--features rag` on `skardi-server` (the umbrella that bundles
`embedding` + `chunking`).

---

## Read Path: Searching

### Vector search only

Embeds the query with candle and finds nearest neighbours via pgvector:

```bash
curl -X POST http://localhost:8080/search-vector/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "how does similarity search work?", "limit": 10}' | jq .
```

### Full-text search only

Keyword search via PostgreSQL's `websearch_to_tsquery` / `ts_rank` over the
`content` column:

```bash
curl -X POST http://localhost:8080/search-fulltext/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "vector similarity search", "limit": 10}' | jq .
```

`pg_fts` accepts web-search-style queries: `foo bar` (AND), `"foo bar"`
(phrase), `foo or bar` (OR), `-foo` (NOT).

### Hybrid search (RRF)

Combines vector and full-text results using Reciprocal Rank Fusion:

```bash
curl -X POST http://localhost:8080/search-hybrid/execute \
  -H "Content-Type: application/json" \
  -d '{
    "query": "how does similarity search work?",
    "text_query": "vector similarity search",
    "vector_weight": 0.5,
    "text_weight": 0.5,
    "limit": 10
  }' | jq .
```

**How RRF works:**

Each result gets a score based on its rank in each search:

```
rrf_score = vector_weight / (60 + vector_rank) + text_weight / (60 + text_rank)
```

Documents appearing in both searches get boosted. The constant 60 prevents
top-ranked results from dominating.

**Example response:**
```json
{
  "success": true,
  "data": [
    {
      "id": 1,
      "content": "Vector databases store high-dimensional vectors and enable fast similarity search at scale.",
      "rrf_score": 0.01639344262295082
    },
    {
      "id": 2,
      "content": "Retrieval-Augmented Generation combines retrieval with a language model to ground responses in factual content.",
      "rrf_score": 0.008064516129032258
    },
    {
      "id": 3,
      "content": "The Transformer architecture introduced multi-head self-attention to replace recurrent networks.",
      "rrf_score": 0.007936507936507936
    }
  ],
  "rows": 3,
  "execution_time_ms": 232,
  "timestamp": "2026-04-13T09:52:00.177238+00:00"
}
```

Because both the vector and the text come from the same row, hybrid search
joins `pg_knn` and `pg_fts` on the `documents.id` primary key — no cross-store
lookup, no id-type conversion.

---

## Pipelines

| Pipeline | Endpoint | Description |
|---|---|---|
| [server/pipelines/ingest.yaml](server/pipelines/ingest.yaml) | `/ingest/execute` | Single INSERT writes content + candle embedding into `documents` |
| [server/pipelines/ingest_chunked.yaml](server/pipelines/ingest_chunked.yaml) | `/ingest-chunked/execute` | Inline `chunk('markdown', …)` + `candle(…)` per chunk; one row per chunk |
| [server/pipelines/search_vector.yaml](server/pipelines/search_vector.yaml) | `/search-vector/execute` | Semantic search via `pg_knn` |
| [server/pipelines/search_fulltext.yaml](server/pipelines/search_fulltext.yaml) | `/search-fulltext/execute` | Keyword search via `pg_fts` over `documents.content` |
| [server/pipelines/search_hybrid.yaml](server/pipelines/search_hybrid.yaml) | `/search-hybrid/execute` | RRF hybrid search combining `pg_knn` + `pg_fts` |

---

## Cleanup

```bash
docker stop rag-postgres && docker rm rag-postgres
pkill -f skardi-server
```
---

## SQLite version — `skardi-server` + SQLite + `sqlite-vec` + FTS5

The same hybrid search pipeline (vector + FTS + RRF) runs against a local
SQLite file — no Docker, no Postgres. A `skardi-server` process registers
the file as its one data source and hosts the pipeline endpoints; `skardi`,
the thin HTTP client, drives ingestion and search against it exactly the
way it does against the Postgres server above. Vectors live in a
[`sqlite-vec`](https://github.com/asg017/sqlite-vec) `vec0` virtual table,
text lives in an FTS5 virtual table, and a regular `documents` table with an
`AFTER INSERT` trigger fans new rows out to both. Each pipeline YAML under
[cli/pipelines/](cli/pipelines/) is loaded on the server with `--pipeline`
and called by its `metadata.name`: `skardi run ingest ...`, `skardi run
ingest-chunked ...`, `skardi run search-hybrid ...`, `skardi run
search-vector ...`, `skardi run search-fulltext ...`.

### 1. Install the CLI

```bash
cargo install --locked --path crates/cli
```

The CLI has no cargo feature flags of its own anymore — it's a thin HTTP
client, so one build works against any server. `--locked` makes cargo
honor the checked-in `Cargo.lock` instead of re-resolving transitive deps,
which can otherwise pull a newer crate whose MSRV is higher than your
toolchain (e.g. `constant_time_eq@0.4.3 requires rustc 1.95.0`).

### 2. Get the `sqlite-vec` extension

Build or download the `vec0` shared library — see the
[sqlite-vec install guide](https://alexgarcia.xyz/sqlite-vec/installation.html).
Then point Skardi at it:

```bash
export SQLITE_VEC_PATH=/absolute/path/to/vec0.dylib   # or .so / .dll

#    If using the pip package (installed in the repo venv):
export SQLITE_VEC_PATH=$(.venv/bin/python -c "import sqlite_vec; print(sqlite_vec.loadable_path())")
```

### 3. Download the embedding model

```bash
# Requires Python 3.12 — run through the repo venv (huggingface_hub is
# installed there, not on the system interpreter)
source .venv/bin/activate

python -c "
from huggingface_hub import hf_hub_download
import os
model_dir = 'models/generated/bge-small-en-v1.5'
os.makedirs(model_dir, exist_ok=True)
for f in ['model.safetensors', 'config.json', 'tokenizer.json']:
    hf_hub_download('BAAI/bge-small-en-v1.5', f, local_dir=model_dir)
"
```

### 4. Create the database

```bash
.venv/bin/python demo/rag/setup.py
```

The script loads the `sqlite-vec` extension via the `sqlite_vec` Python
package (sidestepping the `sqlite3` CLI's missing `enable_load_extension` on
many systems), drops any prior `demo/rag/rag.db`, and creates the `documents`
base table, the `documents_fts` FTS5 mirror, the `documents_vec` `vec0`
mirror, and the `AFTER INSERT` trigger that fans new rows out to both mirrors
atomically. See [setup.py](setup.py) for the schema.

### 5. Config layout

Everything the *server* needs for this demo lives under [cli/](cli/):

```
demo/rag/cli/
  ctx.yaml        # registers rag.db as a SQLite catalog data source
  pipelines/      # pipeline YAMLs, one per verb (ingest, search, ...)
```

(`cli/aliases.yaml` also still sits in that directory but is no longer
read by anything — the CLI's alias system was removed, and each pipeline's
`metadata.name` is now called directly.)

[cli/ctx.yaml](cli/ctx.yaml) registers one SQLite source in `catalog` mode,
which auto-discovers every table, loads `sqlite-vec` once on the shared
connection pool, and exposes each table under `<catalog>.main.<table>` for
both SQL and `sqlite_knn` / `sqlite_fts` lookups:

```yaml
kind: context

metadata:
  name: example-context
  version: 1.0.0

spec:
  data_sources:
    - name: rag
      type: sqlite
      path: demo/rag/rag.db
      access_mode: read_write
      hierarchy_level: catalog
      options:
        extensions_env: SQLITE_VEC_PATH
```

The pipeline YAMLs in [cli/pipelines/](cli/pipelines/) use the same
`metadata` + `query` shape as the server/Postgres pipelines, with `{param}`
placeholders — just targeting the SQLite stack (`sqlite_knn` / `sqlite_fts`
/ `vec_to_binary(candle(...))`) instead of `pg_knn` / `pg_fts`. Each
pipeline's `metadata.name` — `ingest`, `ingest-chunked`, `search-fulltext`,
`search-hybrid`, `search-vector` — is the name you pass to `skardi run`.

### 6. Start the server

```bash
cargo run -p skardi-server --features rag -- \
  --ctx demo/rag/cli/ctx.yaml \
  --pipeline demo/rag/cli/pipelines/ \
  --port 8080
```

`--features rag` is the umbrella that bundles `embedding` (which pulls in
the `candle` backend every pipeline below uses) and `chunking` (needed by
`ingest-chunked`'s inline `chunk('markdown', ...)`), so this one server
build covers the whole demo.

### 7. `ingest` — write one document

```bash
skardi run ingest -p doc_id=1 -p content="Vector databases store high-dimensional vectors and enable fast similarity search at scale."
skardi run ingest -p doc_id=2 -p content="Retrieval-Augmented Generation combines retrieval with a language model to ground responses in factual content."
skardi run ingest -p doc_id=3 -p content="The Transformer architecture introduced multi-head self-attention to replace recurrent networks."
```

`ingest` calls [cli/pipelines/ingest.yaml](cli/pipelines/ingest.yaml),
which INSERTs the row and computes the embedding inline with
`vec_to_binary(candle(...))`. The `AFTER INSERT` trigger atomically mirrors
the row to `documents_fts` and `documents_vec`, so a single call makes the
document searchable by both `sqlite_fts` and `sqlite_knn`.

> Why does [cli/pipelines/ingest.yaml](cli/pipelines/ingest.yaml) wrap the
> seed row as `SELECT {doc_id} AS id, {content} AS content FROM (...)`
> instead of using `VALUES`? DataFusion's INSERT planner currently
> propagates the INSERT target schema (3 columns) down into any
> immediate-child `VALUES` clause and validates row width against it,
> ignoring the intermediate projection that adds
> `vec_to_binary(candle(...))`. The SELECT-wrapper keeps the subquery's own
> schema in scope so the projection lands the row at full width.

### 8. `ingest-chunked` — write one *long* document, chunked inline

`ingest` above expects content that's already chunk-sized. For a real
document, `ingest-chunked` chunks it inline with the markdown splitter,
embeds each chunk with `candle()`, and writes one row per chunk — all in
one statement, all going through the same `AFTER INSERT` trigger:

```bash
skardi run ingest-chunked -p doc_id=100 -p chunk_size=250 -p overlap=50 -p content="# Vector Search

Vector databases index high-dimensional embeddings. They use approximate
nearest-neighbour algorithms — HNSW, IVF, and product quantisation are
common.

# Hybrid Search

Hybrid search merges semantic similarity with keyword relevance.
Reciprocal Rank Fusion (RRF) is a common merge.

# Practical Notes

Chunk size and overlap matter. Too small and chunks lose context; too
large and precision drops."
```

Override the chunk shape, or load the body from a file, the same way:

```bash
skardi run ingest-chunked -p doc_id=101 -p chunk_size=400 -p overlap=80 \
  -p content="$(cat my-long-doc.md)"
```

Synthesised ids are `doc_id * 1000 + chunk_idx` (0-based), so the
example above becomes ids `100000, 100001, 100002, …`. Pick `doc_id`s
that don't collide with single-row `ingest` calls.

### 9. `search-hybrid` — hybrid search (RRF over FTS + vector)

```bash
skardi run search-hybrid -p query="similarity search at scale" -p text_query="similarity search at scale" -p vector_weight=0.5 -p text_weight=0.5 -p limit=10
```

`query` is embedded with `candle()` for `sqlite_knn`; `text_query` goes to
`sqlite_fts` — pass them independently to tune each side:

```bash
skardi run search-hybrid \
  -p query="similarity search" \
  -p text_query="vector similarity search" \
  -p vector_weight=0.3 -p text_weight=0.7 -p limit=5
```

> `text_query` is search text, not an FTS5 expression: `sqlite_fts` parses it
> the way `pg_fts` parses its own `query` (web-search syntax — AND by default,
> `"phrase"`, `or`, `-not`), so punctuation is searched for rather than
> executed and no input can turn a question into a query error.

The structure mirrors the server's `search_hybrid.yaml` exactly —
`sqlite_knn` and `sqlite_fts` replace `pg_knn` / `pg_fts`, RRF is the same
SQL, and `candle()` is reused unchanged for the query embedding. Run
`skardi pipeline show search-hybrid` to see every param the pipeline
exposes and where each value is substituted.

### 10. `search-vector` / `search-fulltext` — single-signal search

If you want just one side of hybrid, these two pipelines skip the RRF
wrapper:

```bash
# Vector-only KNN via sqlite_knn (+ JOIN back to documents for content)
skardi run search-vector -p query="similarity search" -p limit=5

# Full-text-only via sqlite_fts
skardi run search-fulltext -p query="vector similarity search" -p limit=5
```

### 11. Fall back to raw SQL

`skardi run` is a thin layer over the pipeline YAMLs — the underlying
queries are still plain SQL. Query the server directly (same one started
in step 6) if you want to experiment ad-hoc:

```bash
skardi query -e "SELECT id, content FROM rag.main.documents LIMIT 10"
```

### Cleanup

```bash
pkill -f skardi-server
rm demo/rag/rag.db
```
