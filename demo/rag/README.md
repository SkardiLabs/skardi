# Hybrid Search / RAG Demo

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
