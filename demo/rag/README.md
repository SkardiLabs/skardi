# Hybrid Search / RAG Demo

This demo shows a complete FastGPT-style hybrid search pipeline using Skardi:

- **Vector search** — candle (bge-small-en-v1.5) embeddings + pgvector KNN
- **Full-text search** — MongoDB `$text` index with pre-tokenized text
- **Hybrid search** — RRF (Reciprocal Rank Fusion) merging both results in SQL

```
                    ┌─────────────────────────┐
                    │      Write Path          │
                    │                          │
   text ──────────► │  candle() → pgvector     │  (vector embedding)
                    │  tokenize → MongoDB      │  (full-text index)
                    └─────────────────────────┘

                    ┌─────────────────────────┐
                    │      Read Path           │
                    │                          │
   query ─────────► │  pg_knn()  (top 80)      │──┐
                    │  mongo_fts() (top 60)    │──┤ RRF merge
                    │                          │  │
                    │  FULL OUTER JOIN + RRF   │◄─┘
                    │  ORDER BY rrf_score DESC │
                    └─────────────────────────┘
```

## Quick Start

### 1. Start PostgreSQL with pgvector and MongoDB

```bash
# PostgreSQL with pgvector
docker run --name rag-postgres \
  -e POSTGRES_DB=ragdb \
  -e POSTGRES_USER=skardi_user \
  -e POSTGRES_PASSWORD=skardi_pass \
  -p 5432:5432 \
  -d pgvector/pgvector:pg16

# MongoDB
docker run --name rag-mongo \
  -e MONGO_INITDB_ROOT_USERNAME=root \
  -e MONGO_INITDB_ROOT_PASSWORD=rootpass \
  -p 27017:27017 \
  -d mongo:7.0
```

### 2. Create schemas and indexes

```bash
# PostgreSQL: documents table with pgvector
docker exec -i rag-postgres psql -U skardi_user -d ragdb << 'EOF'
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE documents (
    id BIGINT PRIMARY KEY,
    content TEXT NOT NULL,
    embedding vector(384)   -- bge-small-en-v1.5 dimension
);

CREATE INDEX ON documents USING hnsw (embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);
EOF

# MongoDB: document_texts collection with text index
docker exec -i rag-mongo mongosh -u root -p rootpass --authenticationDatabase admin << 'EOF'
use ragdb

db.createCollection("document_texts", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["doc_id", "fullTextToken"],
      properties: {
        doc_id: { bsonType: "string" },
        fullTextToken: { bsonType: "string" }
      }
    }
  }
})
db.document_texts.createIndex(
  { fullTextToken: "text" },
  { default_language: "none" }
)
EOF
```

### 3. Download embedding model

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

### 4. Set credentials and start server

```bash
export PG_USER="skardi_user"
export PG_PASSWORD="skardi_pass"
export MONGO_USER="root"
export MONGO_PASS="rootpass"

cargo run --bin skardi-server --features candle -- \
  --ctx demo/rag/ctx.yaml \
  --pipeline demo/rag/pipelines/ \
  --port 8080
```

---

## Write Path: Ingesting Documents

Each document is ingested into both stores — pgvector for vector search and MongoDB for full-text search.

### Step 1: Insert vector embedding into PostgreSQL

The `candle()` UDF embeds the text inline during INSERT:

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

### Step 2: Insert tokenized text into MongoDB

In production, the caller (e.g. FastGPT) runs Jieba tokenization before calling this endpoint. For this demo we use pre-tokenized English text:

```bash
curl -X POST http://localhost:8080/ingest-text/execute \
  -H "Content-Type: application/json" \
  -d '{
    "doc_id": "1",
    "full_text_token": "vector databases store high dimensional vectors fast similarity search scale"
  }' | jq .

curl -X POST http://localhost:8080/ingest-text/execute \
  -H "Content-Type: application/json" \
  -d '{
    "doc_id": "2",
    "full_text_token": "retrieval augmented generation RAG language model ground responses factual content"
  }' | jq .

curl -X POST http://localhost:8080/ingest-text/execute \
  -H "Content-Type: application/json" \
  -d '{
    "doc_id": "3",
    "full_text_token": "transformer architecture multi head self attention replace recurrent networks"
  }' | jq .
```

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

Keyword search via MongoDB `$text` index:

```bash
curl -X POST http://localhost:8080/search-fulltext/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "vector similarity search", "limit": 10}' | jq .
```

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

Documents appearing in both searches get boosted. The constant 60 prevents top-ranked results from dominating.

**Example response:**
```json
{
  "data": [
    {"id": 1, "content": "Vector databases store...", "rrf_score": 0.0163},
    {"id": 2, "content": "Retrieval-Augmented...",    "rrf_score": 0.0081},
    {"id": 3, "content": "The Transformer...",        "rrf_score": 0.0040}
  ],
  "rows": 3,
  "success": true
}
```

---

## Pipelines

| Pipeline | Endpoint | Description |
|---|---|---|
| `ingest.yaml` | `/ingest/execute` | Embed text with candle → insert into pgvector |
| `ingest_text.yaml` | `/ingest-text/execute` | Insert tokenized text → MongoDB `$text` index |
| `search_vector.yaml` | `/search-vector/execute` | Semantic search via pg_knn |
| `search_fulltext.yaml` | `/search-fulltext/execute` | Keyword search via mongo_fts |
| `search_hybrid.yaml` | `/search-hybrid/execute` | RRF hybrid search combining both |

---

## Cleanup

```bash
docker stop rag-postgres rag-mongo && docker rm rag-postgres rag-mongo
pkill -f skardi-server
```
