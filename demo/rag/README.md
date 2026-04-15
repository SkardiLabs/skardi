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
  --ctx demo/rag/ctx.yaml \
  --pipeline demo/rag/pipelines/ \
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
| `ingest.yaml` | `/ingest/execute` | Single INSERT writes content + candle embedding into `documents` |
| `search_vector.yaml` | `/search-vector/execute` | Semantic search via `pg_knn` |
| `search_fulltext.yaml` | `/search-fulltext/execute` | Keyword search via `pg_fts` over `documents.content` |
| `search_hybrid.yaml` | `/search-hybrid/execute` | RRF hybrid search combining `pg_knn` + `pg_fts` |

---

## Cleanup

```bash
docker stop rag-postgres && docker rm rag-postgres
pkill -f skardi-server
```
---

## CLI version — `skardi-cli` + SQLite + `sqlite-vec` + FTS5

The same hybrid search pipeline (vector + FTS + RRF) runs end-to-end through
`skardi query`. Vectors live in a [`sqlite-vec`](https://github.com/asg017/sqlite-vec)
`vec0` virtual table, text lives in an FTS5 virtual table, and a regular
`documents` table with `AFTER INSERT` triggers fans new rows out to both — so a
single `INSERT` keeps content and embedding in sync. Embeddings are computed
inline by the `candle()` UDF (same model as the server version).

### 1. Install the CLI with embedding support

```bash
cargo install --path crates/cli --features candle
```

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

### 5. Context file

One source in `catalog` mode auto-discovers every table in the database, loads
the `sqlite-vec` extension once on the shared connection pool, and registers
each table under `<catalog>.main.<table>` for both SQL and `sqlite_knn` /
`sqlite_fts` lookups. Save as `demo/rag/cli-ctx.yaml`:

```yaml
data_sources:
  - name: rag
    type: sqlite
    path: demo/rag/rag.db
    access_mode: read_write
    hierarchy_level: catalog
    options:
      extensions_env: SQLITE_VEC_PATH
```

### 6. Ingest — one statement, embedding computed inline

```bash
skardi query --ctx demo/rag/cli-ctx.yaml --sql "
  INSERT INTO rag.main.documents (id, content, embedding)
  SELECT id, content,
         vec_to_binary(candle('models/generated/bge-small-en-v1.5', content))
  FROM (
    SELECT 1 AS id, 'Vector databases store high-dimensional vectors and enable fast similarity search at scale.' AS content
    UNION ALL
    SELECT 2, 'Retrieval-Augmented Generation combines retrieval with a language model to ground responses in factual content.'
    UNION ALL
    SELECT 3, 'The Transformer architecture introduced multi-head self-attention to replace recurrent networks.'
  ) AS t
"
```

> Why `UNION ALL` of `SELECT`s instead of `VALUES`? DataFusion's INSERT planner
> currently propagates the INSERT target schema (here, 3 columns) down into any
> immediate-child `VALUES` clause and validates row width against it, ignoring
> the intermediate `SELECT` projection that adds `vec_to_binary(candle(...))`.
> Wrapping the seed rows as `SELECT … UNION ALL SELECT …` keeps the subquery's
> own schema in scope and the projection lands the row at full width.

`candle()` produces a `List<Float32>`; `vec_to_binary()` packs it to the
little-endian f32 BLOB that `vec0` expects. The `AFTER INSERT` trigger then
mirrors the row to `documents_fts` and `documents_vec` atomically.

### 7. Hybrid search (RRF in one DataFusion query)

```bash
skardi query --ctx demo/rag/cli-ctx.yaml --sql "
  WITH vec AS (
    SELECT id, ROW_NUMBER() OVER (ORDER BY _score ASC) AS rk
    FROM sqlite_knn('rag.main.documents_vec', 'embedding',
        (SELECT candle('models/generated/bge-small-en-v1.5',
                       'how does similarity search work?')),
        80)
  ),
  fts AS (
    SELECT id, content, ROW_NUMBER() OVER (ORDER BY _score DESC) AS rk
    FROM sqlite_fts('rag.main.documents_fts', 'content', 'vector similarity search', 60)
  )
  SELECT
    COALESCE(v.id, f.id) AS id,
    COALESCE(f.content, d.content) AS content,
    COALESCE(0.5 / (60.0 + v.rk), 0)
      + COALESCE(0.5 / (60.0 + f.rk), 0) AS rrf_score
  FROM vec v
  FULL OUTER JOIN fts f ON v.id = f.id
  LEFT JOIN rag.main.documents d ON d.id = v.id
  ORDER BY rrf_score DESC
  LIMIT 10
"
```

The structure mirrors the server's `search_hybrid.yaml` exactly — `sqlite_knn`
and `sqlite_fts` replace `pg_knn` / `pg_fts`, RRF is the same SQL, and `candle()`
is reused unchanged for the query embedding.

### Cleanup

```bash
rm demo/rag/rag.db
