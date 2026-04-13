# LLM Wiki Demo

A data layer for Karpathy's [LLM Wiki](https://gist.github.com/karpathy/442a6bf555914893e9891c11519de94f)
idea, built on Skardi. The wiki is stored in a **single PostgreSQL table** that
holds markdown content + pgvector embedding on the same row, so every page is
atomically searchable via both full-text search and semantic vector search.

Rather than chunked RAG, the LLM agent maintains a persistent, compounding
knowledge artifact — entity pages, concept pages, summaries, an index — and
uses the endpoints below as its file-system-like primitives (`open`, `write`,
`grep`, `ls`, `log`).

- **Vector search** — candle (bge-small-en-v1.5) embeddings + pgvector KNN (`pg_knn`)
- **Full-text search** — PostgreSQL `tsvector` / `websearch_to_tsquery` (`pg_fts`)
- **Hybrid search** — RRF (Reciprocal Rank Fusion) merging both results in SQL
- **Atomic edits** — `wiki-create` (INSERT) and `wiki-update` (UPDATE) both re-embed the page inline with `candle()` in a single statement, so content and vector stay in sync

```
                    ┌─────────────────────────────────┐
                    │           Write Path             │
                    │                                  │
  markdown ───────► │  INSERT (wiki-create)  or        │
  (by slug)         │  UPDATE (wiki-update)            │
                    │    SET content,                  │
                    │        embedding = candle(...)   │
                    │                                  │
                    │  ─► row is now visible to        │
                    │     both pg_fts and pg_knn       │
                    └─────────────────────────────────┘

                    ┌─────────────────────────────────┐
                    │            Read Path             │
                    │                                  │
  query ──────────► │  pg_knn()  (top 80)              │──┐
                    │  pg_fts()  (top 60)              │──┤ RRF merge
                    │                                  │  │
                    │  FULL OUTER JOIN ON slug + RRF   │◄─┘
                    │  ORDER BY rrf_score DESC         │
                    └─────────────────────────────────┘
```

Because content and embedding live on the same row, a single upsert keeps FTS
and vector in sync — no second store, no cross-store consistency problem.

## Quick Start

### 1. Start PostgreSQL with pgvector

```bash
docker run --name wiki-postgres \
  -e POSTGRES_DB=wikidb \
  -e POSTGRES_USER=skardi_user \
  -e POSTGRES_PASSWORD=skardi_pass \
  -p 5432:5432 \
  -d pgvector/pgvector:pg16
```

### 2. Create the schema and indexes

```bash
docker exec -i wiki-postgres psql -U skardi_user -d wikidb << 'EOF'
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE wiki_pages (
    slug        TEXT PRIMARY KEY,          -- e.g. "entity/alan-turing"
    title       TEXT NOT NULL,
    page_type   TEXT NOT NULL,             -- entity | concept | summary | index | schema
    content     TEXT NOT NULL,             -- markdown body
    embedding   vector(384),               -- bge-small-en-v1.5 dimension
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON wiki_pages USING hnsw (embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);

CREATE INDEX wiki_pages_content_fts_idx
  ON wiki_pages
  USING GIN (to_tsvector('english', content));

CREATE INDEX wiki_pages_type_idx ON wiki_pages (page_type, updated_at DESC);

CREATE TABLE wiki_log (
    id          BIGSERIAL PRIMARY KEY,
    event_type  TEXT NOT NULL,             -- ingest | query | lint | note
    slug        TEXT NOT NULL,             -- "" if not page-specific
    message     TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
EOF
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
print(f'Model downloaded to {model_dir}')
"
```

### 4. Set credentials and start the server

```bash
export PG_USER="skardi_user"
export PG_PASSWORD="skardi_pass"

cargo run --bin skardi-server --features candle -- \
  --ctx demo/llm_wiki/ctx.yaml \
  --pipeline demo/llm_wiki/pipelines/ \
  --port 8080
```

---

## Agent Primitives

The wiki exposes five HTTP endpoints that mirror the verbs an LLM agent needs
to maintain a compounding knowledge base. Each one corresponds to one pipeline
file under [pipelines/](pipelines/).

| Endpoint | Verb | Pipeline |
|---|---|---|
| `/wiki-create/execute`        | `write` (new)  | [pipelines/create.yaml](pipelines/create.yaml) |
| `/wiki-update/execute`        | `write` (edit) | [pipelines/update.yaml](pipelines/update.yaml) |
| `/wiki-get/execute`           | `open`         | [pipelines/get.yaml](pipelines/get.yaml) |
| `/wiki-search-hybrid/execute` | `grep`         | [pipelines/search_hybrid.yaml](pipelines/search_hybrid.yaml) |
| `/wiki-list/execute`          | `ls`           | [pipelines/list.yaml](pipelines/list.yaml) |
| `/wiki-log-append/execute`    | `log`          | [pipelines/log_append.yaml](pipelines/log_append.yaml) |

> DataFusion's SQL planner does not support `INSERT ... ON CONFLICT`, so
> create and edit are exposed as two explicit endpoints. The agent's pattern
> is: try `wiki-update` first; if it affects zero rows, fall back to
> `wiki-create`. Both re-embed the page inline in a single statement, so
> FTS and vector stay consistent either way.

---

## Write Path: Creating and Editing Pages

Both endpoints re-embed the page inline with `candle()` in a single SQL
statement, so the pgvector column and the FTS `content` column are written
together from the same row.

### Create a new page

```bash
# Create an entity page
curl -X POST http://localhost:8080/wiki-create/execute \
  -H "Content-Type: application/json" \
  -d '{
    "slug": "entity/alan-turing",
    "title": "Alan Turing",
    "page_type": "entity",
    "content": "# Alan Turing\n\nBritish mathematician and logician who formalized the concepts of algorithm and computation with the Turing machine. Considered a founder of theoretical computer science and artificial intelligence."
  }' | jq .

# Create a concept page that references it
curl -X POST http://localhost:8080/wiki-create/execute \
  -H "Content-Type: application/json" \
  -d '{
    "slug": "concept/turing-machine",
    "title": "Turing Machine",
    "page_type": "concept",
    "content": "# Turing Machine\n\nAn abstract computational model introduced by [Alan Turing](entity/alan-turing) in 1936. Consists of an infinite tape, a head, and a finite state machine; captures the notion of effective computability."
  }' | jq .

# Create a summary page
curl -X POST http://localhost:8080/wiki-create/execute \
  -H "Content-Type: application/json" \
  -d '{
    "slug": "summary/foundations-of-computation",
    "title": "Foundations of Computation",
    "page_type": "summary",
    "content": "# Foundations of Computation\n\nThe theoretical basis for modern computing emerged in the 1930s through the work of Church, Turing, and Gödel. The Church–Turing thesis unified lambda calculus and Turing machines as equivalent models of computation."
  }' | jq .
```

### Edit an existing page

`wiki-update` rewrites the row in place and refreshes the embedding, so an
edit is atomically reflected in both pg_fts and pg_knn.

```bash
curl -X POST http://localhost:8080/wiki-update/execute \
  -H "Content-Type: application/json" \
  -d '{
    "slug": "entity/alan-turing",
    "title": "Alan Turing",
    "page_type": "entity",
    "content": "# Alan Turing\n\nBritish mathematician, logician, and cryptanalyst. Formalized algorithm and computation via the Turing machine; led the Bletchley Park team that broke the Enigma cipher during WWII."
  }' | jq .
```

Under the hood the two pipelines are:

```sql
-- wiki-create
INSERT INTO wikidb.public.wiki_pages (slug, title, page_type, content, embedding, updated_at)
VALUES (
  {slug}, {title}, {page_type}, {content},
  candle('models/generated/bge-small-en-v1.5', {content}),
  now()
);

-- wiki-update
UPDATE wikidb.public.wiki_pages
SET title      = {title},
    page_type  = {page_type},
    content    = {content},
    embedding  = candle('models/generated/bge-small-en-v1.5', {content}),
    updated_at = now()
WHERE slug = {slug};
```

DataFusion's planner does not support `INSERT ... ON CONFLICT`, which is why
create and update are separate endpoints. The agent pattern is: try
`wiki-update` first; if it reports zero rows affected, fall back to
`wiki-create`.

---

## Read Path: Agent Retrieval Loop

A typical LLM agent turn looks like:

1. **`grep`** the wiki with hybrid search to find candidate slugs
2. **`open`** each top-ranked page with `wiki-get` to read the full body
3. Synthesize the answer, optionally **`write`** new pages back
4. **`log`** the activity so the next session knows what happened

### `grep` — hybrid search

```bash
curl -X POST http://localhost:8080/wiki-search-hybrid/execute \
  -H "Content-Type: application/json" \
  -d '{
    "query": "who invented the theoretical model of a computer?",
    "text_query": "turing machine computation",
    "vector_weight": 0.5,
    "text_weight": 0.5,
    "limit": 10
  }' | jq .
```

Returns `slug`, `title`, `page_type`, and `rrf_score` for each candidate page.
The RRF join is on `slug` (the wiki's primary key), so there is no cross-store
lookup and no id-type conversion.

### `open` — fetch a full page

```bash
curl -X POST http://localhost:8080/wiki-get/execute \
  -H "Content-Type: application/json" \
  -d '{"slug": "entity/alan-turing"}' | jq .
```

### `ls` — browse by type or prefix

Rebuild `index.md`, find orphan pages, or list a category:

```bash
# All entity pages, newest first
curl -X POST http://localhost:8080/wiki-list/execute \
  -H "Content-Type: application/json" \
  -d '{
    "page_type_pattern": "entity",
    "slug_prefix": "%",
    "limit": 100
  }' | jq .

# Everything under concept/
curl -X POST http://localhost:8080/wiki-list/execute \
  -H "Content-Type: application/json" \
  -d '{
    "page_type_pattern": "%",
    "slug_prefix": "concept/%",
    "limit": 100
  }' | jq .
```

### `log` — append an activity entry

```bash
curl -X POST http://localhost:8080/wiki-log-append/execute \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "ingest",
    "slug": "entity/alan-turing",
    "message": "Created from Wikipedia article; cross-linked from concept/turing-machine."
  }' | jq .
```

---

## Pipelines

| Pipeline | Endpoint | Description |
|---|---|---|
| [create.yaml](pipelines/create.yaml) | `/wiki-create/execute` | INSERT a new page; re-embeds with `candle()` inline |
| [update.yaml](pipelines/update.yaml) | `/wiki-update/execute` | UPDATE an existing page by slug; re-embeds with `candle()` inline |
| [get.yaml](pipelines/get.yaml) | `/wiki-get/execute` | Fetch one page by slug |
| [search_hybrid.yaml](pipelines/search_hybrid.yaml) | `/wiki-search-hybrid/execute` | RRF hybrid search over `pg_knn` + `pg_fts` |
| [list.yaml](pipelines/list.yaml) | `/wiki-list/execute` | Filter pages by `page_type` + slug prefix, newest first |
| [log_append.yaml](pipelines/log_append.yaml) | `/wiki-log-append/execute` | Append to the `wiki_log` activity log |

---

## Relationship to the RAG Demo

This demo is a schema-level evolution of [demo/rag/](../rag/): same stack
(Postgres + pgvector + candle + `pg_fts` + `pg_knn` + RRF), but the table is
keyed by a human-readable `slug`, carries page metadata (`title`, `page_type`),
and uses `INSERT ... ON CONFLICT` so pages can be edited in place. The RAG
demo ingests immutable chunks; the LLM Wiki demo ingests a living, editable
knowledge base that the LLM itself curates.

---

## Cleanup

```bash
docker stop wiki-postgres && docker rm wiki-postgres
pkill -f skardi-server
```
