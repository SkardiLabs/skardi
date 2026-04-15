# Combine Demo — Migrate `mongo_fts` → `pg_fts` Instantly

This demo combines the [fastGPT](../fastGPT/) and [rag](../rag/) demos into a
single Skardi deployment to show how you can **swap a full-text backend with
zero data migration and zero downtime** by editing pipeline YAML.

The two source demos already solve the same problem — hybrid RAG search with
vector + FTS + RRF — but pick different FTS backends:

| Demo | Vector | FTS | Write path |
|---|---|---|---|
| [fastGPT](../fastGPT/) | `pg_knn` | `mongo_fts` (MongoDB `$text`) | 2 writes (Postgres + Mongo) |
| [rag](../rag/) | `pg_knn` | `pg_fts` (Postgres `tsvector`) | 1 write (Postgres only) |

Because **fastGPT's ingest already stores the raw `content` column in
Postgres** (it needs it for the embedding), the row that `mongo_fts` is
searching over a tokenized copy of is *already* sitting in Postgres, ready
for `pg_fts`. Migration is therefore reduced to:

1. Add a GIN index on `to_tsvector('english', content)`.
2. Point the client at `search-fulltext-pg` / `search-hybrid-pg` instead of
   the `-mongo` variants.
3. Stop writing to MongoDB.

Every single file in `pipelines/` is lifted **unchanged** from the two source
demos — only renamed so both backends can be served simultaneously during the
cutover.

```
┌─────────── BEFORE ───────────┐      ┌─────────── AFTER ───────────┐
│                              │      │                              │
│  ingest  ─► Postgres (vec)   │      │  ingest  ─► Postgres (vec +  │
│  ingest-text ─► Mongo (fts)  │      │            GIN on content)   │
│                              │      │                              │
│  search-hybrid-mongo:        │      │  search-hybrid-pg:           │
│    pg_knn  ⨝  mongo_fts      │      │    pg_knn  ⨝  pg_fts         │
│    (CAST doc_id ⇒ BIGINT)    │      │    (id = id)                 │
└──────────────────────────────┘      └──────────────────────────────┘
```

---

## Quick Start

### 1. Start Postgres + Mongo

```bash
docker run --name rag-postgres \
  -e POSTGRES_DB=ragdb \
  -e POSTGRES_USER=skardi_user \
  -e POSTGRES_PASSWORD=skardi_pass \
  -p 5432:5432 \
  -d pgvector/pgvector:pg16

docker run --name rag-mongo \
  -e MONGO_INITDB_ROOT_USERNAME=root \
  -e MONGO_INITDB_ROOT_PASSWORD=rootpass \
  -p 27017:27017 \
  -d mongo:7.0
```

### 2. Create schemas — including the pg_fts index up-front

The key change from fastGPT: we create the GIN index on `content` at the same
time as the HNSW index. That's the *only* schema change needed for the
migration — no ALTER TABLE later, no backfill.

```bash
docker exec -i rag-postgres psql -U skardi_user -d ragdb << 'EOF'
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE documents (
    id BIGINT PRIMARY KEY,
    content TEXT NOT NULL,
    embedding vector(384)   -- bge-small-en-v1.5 dimension
);

-- Vector index (used by pg_knn — both before and after migration)
CREATE INDEX ON documents USING hnsw (embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);

-- Full-text index (used by pg_fts — lights up the "after" path)
CREATE INDEX documents_content_fts_idx
  ON documents
  USING GIN (to_tsvector('english', content));
EOF

docker exec -i rag-mongo mongosh -u root -p rootpass --authenticationDatabase admin << 'EOF'
use ragdb

db.createCollection("document_texts", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: ["doc_id", "fullTextToken"],
      properties: {
        doc_id:        { bsonType: "string" },
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

### 4. Start Skardi with the combined pipeline dir

```bash
export PG_USER="skardi_user"
export PG_PASSWORD="skardi_pass"
export MONGO_USER="root"
export MONGO_PASS="rootpass"

cargo run --bin skardi-server --features candle -- \
  --ctx demo/combine/ctx.yaml \
  --pipeline demo/combine/pipelines/ \
  --port 8080
```

One server, one context file, **both FTS backends exposed at once**:

| Endpoint | Backend |
|---|---|
| `/search-fulltext-mongo/execute` | `mongo_fts` (legacy) |
| `/search-fulltext-pg/execute`    | `pg_fts` (new) |
| `/search-hybrid-mongo/execute`   | `pg_knn` + `mongo_fts` |
| `/search-hybrid-pg/execute`      | `pg_knn` + `pg_fts` |
| `/search-vector/execute`         | `pg_knn` (unchanged) |

---

## Phase 1 — Run as if nothing changed (mongo_fts path)

This is the fastGPT behaviour: ingest into Postgres for the vector, ingest
tokenized text into Mongo for FTS, and serve reads from `search-hybrid-mongo`.

```bash
# Vector + content → Postgres
for doc in \
  '{"doc_id":1,"content":"Vector databases store high-dimensional vectors and enable fast similarity search at scale."}' \
  '{"doc_id":2,"content":"Retrieval-Augmented Generation combines retrieval with a language model to ground responses in factual content."}' \
  '{"doc_id":3,"content":"The Transformer architecture introduced multi-head self-attention to replace recurrent networks."}' ; do
  curl -s -X POST http://localhost:8080/ingest/execute \
    -H "Content-Type: application/json" -d "$doc" | jq .
done

# Tokenized text → Mongo (legacy path)
for t in \
  '{"doc_id":"1","full_text_token":"vector databases store high dimensional vectors fast similarity search scale"}' \
  '{"doc_id":"2","full_text_token":"retrieval augmented generation RAG language model ground responses factual content"}' \
  '{"doc_id":"3","full_text_token":"transformer architecture multi head self attention replace recurrent networks"}' ; do
  curl -s -X POST http://localhost:8080/ingest-text/execute \
    -H "Content-Type: application/json" -d "$t" | jq .
done

# Vector-only search (backend-agnostic — same endpoint used in every phase)
curl -s -X POST http://localhost:8080/search-vector/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "how does similarity search work?", "limit": 10}' | jq .

# Full-text-only search via the legacy mongo_fts path
curl -s -X POST http://localhost:8080/search-fulltext-mongo/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "vector similarity search", "limit": 10}' | jq .

# Serve hybrid search via mongo_fts
curl -s -X POST http://localhost:8080/search-hybrid-mongo/execute \
  -H "Content-Type: application/json" \
  -d '{
    "query": "how does similarity search work?",
    "text_query": "vector similarity search",
    "vector_weight": 0.5,
    "text_weight": 0.5,
    "limit": 10
  }' | jq .
```

At this point you have a faithful reproduction of the fastGPT demo.

---

## Phase 2 — Cut over to pg_fts with zero data migration

**Here is the whole migration:** change the endpoint the client hits. Nothing
else. No rewriting rows. No dual-read consistency check. The `content` column
has been in Postgres all along; the GIN index from step 2 made it searchable.

```bash
# Full-text-only search via the new pg_fts path — note the web-search syntax
# (AND by default, "quoted phrase", or, -not) instead of space-tokenized input.
curl -s -X POST http://localhost:8080/search-fulltext-pg/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "vector similarity search", "limit": 10}' | jq .

# Same documents, same query, different FTS backend.
curl -s -X POST http://localhost:8080/search-hybrid-pg/execute \
  -H "Content-Type: application/json" \
  -d '{
    "query": "how does similarity search work?",
    "text_query": "vector similarity search",
    "vector_weight": 0.5,
    "text_weight": 0.5,
    "limit": 10
  }' | jq .
```

The response shape is identical: `id`, `content`, `rrf_score`. Note that
`search-hybrid-pg` no longer needs `CAST(doc_id AS BIGINT)` because both
the vector and the text come from the same Postgres row — compare the SQL
in [search_hybrid_mongo.yaml](pipelines/search_hybrid_mongo.yaml) vs
[search_hybrid_pg.yaml](pipelines/search_hybrid_pg.yaml) to see exactly
what changed.

You can run both endpoints in parallel for as long as you need — shadow
traffic, A/B, diff the result sets — and flip the client over when you're
satisfied.

---

## Phase 3 — Decommission MongoDB

Once `search-hybrid-pg` is serving all production traffic:

1. Stop calling `/ingest-text/execute`.
2. Delete `pipelines/search_fulltext_mongo.yaml`,
   `pipelines/search_hybrid_mongo.yaml`, and `pipelines/ingest_text.yaml`.
3. Remove the `document_texts` data source from [ctx.yaml](ctx.yaml).
4. `docker rm rag-mongo`.

The remaining demo is now byte-for-byte the [rag demo](../rag/) — same
`ctx.yaml` shape, same pipelines, same endpoints. That's the point: the
migration collapses one demo into the other without rewriting application
code or reshaping data.

---

## Why this works with Skardi

- **Pipelines are data, not code.** `mongo_fts` and `pg_fts` are both plain
  UDFs exposed through DataFusion SQL, so swapping backends is a YAML edit
  — no recompile, no redeploy of the server binary.
- **Source of truth is already in Postgres.** fastGPT stored raw `content`
  in Postgres to feed `candle()`; that same column is what `pg_fts` reads.
  MongoDB was only holding a *derived* tokenized view.
- **Multiple pipelines can share a data source.** `ctx.yaml` declares the
  `documents` Postgres table once, and every search pipeline — mongo-era
  and pg-era — references it. Adding `search-hybrid-pg` costs one file.
- **RRF is backend-agnostic.** The fusion SQL cares about ranks, not where
  the ranks came from. Both hybrid pipelines produce the same
  `{id, content, rrf_score}` shape, so the client doesn't change.

---

## Cleanup

```bash
docker stop rag-postgres rag-mongo && docker rm rag-postgres rag-mongo
pkill -f skardi-server
```
