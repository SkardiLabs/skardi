# Hybrid Search / RAG Demo

This demo has two flavours: the **server / PostgreSQL** version and a **CLI / SQLite** version that runs entirely through `skardi-cli` against a local file (no server, no Docker).

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
# Requires Python 3.12
pip install huggingface_hub

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

## CLI version — `skardi-cli` + SQLite + `sqlite-vec` + FTS5

The same hybrid search pipeline (vector + FTS + RRF) runs end-to-end through
`skardi` — **no server, no Docker, no HTTP**. Vectors live in a
[`sqlite-vec`](https://github.com/asg017/sqlite-vec) `vec0` virtual table,
text lives in an FTS5 virtual table, and a regular `documents` table with an
`AFTER INSERT` trigger fans new rows out to both. Each pipeline YAML under
[cli/pipelines/](cli/pipelines/) is exposed through a short verb alias
defined in [cli/aliases.yaml](cli/aliases.yaml): `skardi ingest <id> "..."`,
`skardi grep "..."`, `skardi vec "..."`, `skardi fts "..."`.

### 1. Install the CLI with embedding support

```bash
cargo install --locked --path crates/cli --features candle
```

`--locked` makes cargo honor the checked-in `Cargo.lock` instead of
re-resolving transitive deps, which can otherwise pull a newer crate whose
MSRV is higher than your toolchain (e.g. `constant_time_eq@0.4.3 requires
rustc 1.95.0`).

### 2. Get the `sqlite-vec` extension

Build or download the `vec0` shared library — see the
[sqlite-vec install guide](https://alexgarcia.xyz/sqlite-vec/installation.html).
Then point Skardi at it:

```bash
export SQLITE_VEC_PATH=/absolute/path/to/vec0.dylib   # or .so / .dll

#    If using the pip package:
export SQLITE_VEC_PATH=$(python -c "import sqlite_vec; print(sqlite_vec.loadable_path())")
```

### 3. Download the embedding model

```bash
pip install huggingface_hub
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
pip install sqlite-vec
python demo/rag/setup.py
```

The script loads the `sqlite-vec` extension via the `sqlite_vec` Python
package (sidestepping the `sqlite3` CLI's missing `enable_load_extension` on
many systems), drops any prior `demo/rag/rag.db`, and creates the `documents`
base table, the `documents_fts` FTS5 mirror, the `documents_vec` `vec0`
mirror, and the `AFTER INSERT` trigger that fans new rows out to both mirrors
atomically. See [setup.py](setup.py) for the schema.

### 5. Config layout

Everything the CLI needs for the demo lives under [cli/](cli/):

```
demo/rag/cli/
  ctx.yaml        # registers rag.db as a SQLite catalog data source
  aliases.yaml    # short verbs → pipeline bindings
  pipelines/      # pipeline YAMLs (one per verb)
```

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
`metadata` + `query` shape as the server pipelines, with `{param}`
placeholders — just targeting the SQLite stack (`sqlite_knn` / `sqlite_fts`
/ `vec_to_binary(candle(...))`) instead of `pg_knn` / `pg_fts`. Verb →
pipeline bindings live in [cli/aliases.yaml](cli/aliases.yaml).

**Export the config dir once** so the verbs below don't need `--ctx` on
every line. `SKARDICONFIG` accepts either a config directory (which the CLI
looks inside for `ctx.yaml`, `aliases.yaml`, and `pipelines/`) or an
individual ctx file. `--ctx PATH` still works and takes precedence:

```bash
export SKARDICONFIG=demo/rag/cli
```

### 6. `ingest` — write one document

```bash
skardi ingest 1 "Vector databases store high-dimensional vectors and enable fast similarity search at scale."
skardi ingest 2 "Retrieval-Augmented Generation combines retrieval with a language model to ground responses in factual content."
skardi ingest 3 "The Transformer architecture introduced multi-head self-attention to replace recurrent networks."
```

The `ingest` alias invokes [cli/pipelines/ingest.yaml](cli/pipelines/ingest.yaml),
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

### 7. `grep` — hybrid search (RRF over FTS + vector)

```bash
skardi grep "similarity search at scale"
```

One positional arg binds to both `{query}` (embedded with `candle()` for
`sqlite_knn`) and `{text_query}` (via the `text_query: "{query}"` default in
the alias). Override either independently:

```bash
skardi grep "similarity search" \
  --text_query="vector similarity search" \
  --vector_weight=0.3 --text_weight=0.7 --limit=5
```

> FTS5 reserves `?`, `"`, `+`, `-`, `~`, `^`, and parentheses as query
> syntax. A bare `?` in the text query will fail with an `fts5: syntax
> error` — phrase-quote or strip it out for natural-language queries.

The structure mirrors the server's `search_hybrid.yaml` exactly —
`sqlite_knn` and `sqlite_fts` replace `pg_knn` / `pg_fts`, RRF is the same
SQL, and `candle()` is reused unchanged for the query embedding. Run
`skardi grep --help` to see every param the alias exposes and where each
value comes from.

### 8. `vec` / `fts` — single-signal search

If you want just one side of hybrid, the alias pair avoids the RRF wrapper:

```bash
# Vector-only KNN via sqlite_knn (+ JOIN back to documents for content)
skardi vec "similarity search" --limit=5

# Full-text-only via sqlite_fts
skardi fts "vector similarity search" --limit=5
```

### 9. Fall back to raw SQL

`skardi run` and the aliases above are a thin layer over the pipeline YAMLs.
The underlying queries are still plain SQL — if you want to experiment
ad-hoc, `skardi query --sql "..."` works just as before (same exported
`SKARDICONFIG`).

### Cleanup

```bash
rm demo/rag/rag.db
