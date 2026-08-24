# LLM Wiki Demo

A durable, editable wiki for an LLM agent, built entirely on Skardi
primitives: every agent verb (`write`, `open`, `grep`, `ls`, `log`) is one
pipeline YAML, invoked with `skardi run <pipeline-name>`, and the whole
thing runs on `candle()` inline embeddings, `pg_knn` / `sqlite_knn`,
`pg_fts` / `sqlite_fts`, and RRF hybrid search — all in the same SQL. The
demo shows what **full data autonomy** looks like for an agent: it curates
a compounding knowledge base itself, with no external orchestration code.

Two flavours ship side by side:

- **Server / PostgreSQL + pgvector** — the "production" shape: each verb is
  a REST pipeline, invoked over HTTP. Useful when the agent is running
  somewhere else (Claude Desktop, a web backend, a managed runtime) and
  talks to skardi across the wire.
- **CLI / SQLite + `sqlite-vec` + FTS5** — the "drop into any agent" shape:
  same pipeline YAMLs, but invoked as `skardi run wiki-<verb>` commands
  against a `skardi-server` running locally over SQLite — no Docker, no
  Postgres. `skardi` itself is a thin HTTP client with no query engine of
  its own (see [docs/cli.md](../../docs/cli.md)), so this flavour still
  needs a `skardi-server` process, just one you start and forget on your
  own machine in a few minutes.

Both flavours use the **same pipeline YAML format**, because the whole point
of the Skardi design is that one declaration is every agent-facing surface:
REST today, shell today, skills soon, MCP soon after. For the thinking
behind that, see [the loop in the main README](../../README.md#the-loop).

---

## Server version — `skardi-server` + PostgreSQL + pgvector

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
  --ctx demo/llm_wiki/server/ctx.yaml \
  --pipeline demo/llm_wiki/server/pipelines/ \
  --port 8080
```

---

## Agent Primitives

The wiki exposes five HTTP endpoints that mirror the verbs an LLM agent needs
to maintain a compounding knowledge base. Each one corresponds to one pipeline
file under [server/pipelines/](server/pipelines/).

| Endpoint | Verb | Pipeline |
|---|---|---|
| `/wiki-create/execute`        | `write` (new)  | [server/pipelines/create.yaml](server/pipelines/create.yaml) |
| `/wiki-update/execute`        | `write` (edit) | [server/pipelines/update.yaml](server/pipelines/update.yaml) |
| `/wiki-get/execute`           | `open`         | [server/pipelines/get.yaml](server/pipelines/get.yaml) |
| `/wiki-search-hybrid/execute` | `grep`         | [server/pipelines/search_hybrid.yaml](server/pipelines/search_hybrid.yaml) |
| `/wiki-list/execute`          | `ls`           | [server/pipelines/list.yaml](server/pipelines/list.yaml) |
| `/wiki-log-append/execute`    | `log`          | [server/pipelines/log_append.yaml](server/pipelines/log_append.yaml) |

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

### Bulk-create from a long document — `wiki-bulk-create`

`wiki-create` keeps one page = one row, which is the wiki's editing model.
But for **seeding** the wiki — turning a long markdown doc into many pages
in one shot — `wiki-bulk-create` chunks the body inline with
[`chunk('markdown', …)`](../../docs/chunk.md), embeds each chunk with
`candle()`, and writes one page per chunk:

```bash
curl -X POST http://localhost:8080/wiki-bulk-create/execute \
  -H "Content-Type: application/json" \
  -d '{
    "slug_prefix": "rfc/9110",
    "title": "RFC 9110: HTTP Semantics",
    "page_type": "rfc",
    "content": "<the full RFC body in markdown>",
    "chunk_size": 800,
    "overlap": 150
  }' | jq .
```

Synthesised slugs follow `{slug_prefix}/p<NNN>` (`rfc/9110/p001`,
`/p002`, …) and titles get a `#N` suffix so each chunk is a distinct page
visible to `wiki-list`, `wiki-search-hybrid`, etc. Pages later edited
via `wiki-update` keep their slug — the bulk-create path is a
one-way seeder, not a re-runnable upsert.

Requires `--features rag` on `skardi-server` (the umbrella that bundles
`embedding` + `chunking`). See
[server/pipelines/bulk_create.yaml](server/pipelines/bulk_create.yaml).

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
| [server/pipelines/create.yaml](server/pipelines/create.yaml) | `/wiki-create/execute` | INSERT a new page; re-embeds with `candle()` inline |
| [server/pipelines/update.yaml](server/pipelines/update.yaml) | `/wiki-update/execute` | UPDATE an existing page by slug; re-embeds with `candle()` inline |
| [server/pipelines/bulk_create.yaml](server/pipelines/bulk_create.yaml) | `/wiki-bulk-create/execute` | Chunk a long doc with `chunk('markdown', …)`, embed each chunk with `candle()`, write one page per chunk |
| [server/pipelines/get.yaml](server/pipelines/get.yaml) | `/wiki-get/execute` | Fetch one page by slug |
| [server/pipelines/search_hybrid.yaml](server/pipelines/search_hybrid.yaml) | `/wiki-search-hybrid/execute` | RRF hybrid search over `pg_knn` + `pg_fts` |
| [server/pipelines/list.yaml](server/pipelines/list.yaml) | `/wiki-list/execute` | Filter pages by `page_type` + slug prefix, newest first |
| [server/pipelines/log_append.yaml](server/pipelines/log_append.yaml) | `/wiki-log-append/execute` | Append to the `wiki_log` activity log |

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

---

## CLI version — `skardi` + local `skardi-server` + SQLite + `sqlite-vec` + FTS5

The same wiki primitives (`create`, `get`, `grep`, `ls`, `log`, plus an edit
workflow built from `delete` + `create`) run through `skardi` talking to a
`skardi-server` running locally over SQLite — **no Docker, no Postgres**.
Each primitive is a pipeline YAML under [cli/pipelines/](cli/pipelines/)
(same format as the server pipelines) and is invoked directly by its
pipeline name: `skardi run wiki-search-hybrid -p query="..."`,
`skardi run wiki-list`, `skardi run wiki-get -p slug=<slug>`,
`skardi run wiki-create -p slug=... -p title=...`, etc.

A regular `wiki_pages` table holds canonical state (slug, title, page_type,
content, embedding); `AFTER INSERT` / `AFTER UPDATE` triggers fan rows out to
an FTS5 virtual table for keyword search and a
[`sqlite-vec`](https://github.com/asg017/sqlite-vec) `vec0` virtual table for
KNN, so a single `INSERT` (or `UPDATE`) keeps content and embedding in sync.
Embeddings are computed inline by the `candle()` UDF (same model as the server
version).

### 1. Install the CLI

```bash
cargo install --locked --path crates/cli
```

`--locked` makes cargo honor the checked-in `Cargo.lock` instead of
re-resolving transitive deps, which can otherwise pull a newer crate whose
MSRV is higher than your toolchain.

The CLI itself builds featureless — it's a thin HTTP client with no query
engine of its own. Embedding support (`candle`) is a `skardi-server`
feature, enabled when you start the server in step 5 below.

### 2. Get the `sqlite-vec` extension

Build or download the `vec0` shared library — see the
[sqlite-vec install guide](https://alexgarcia.xyz/sqlite-vec/installation.html).
Then:

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
python demo/llm_wiki/setup.py
```

The script loads the `sqlite-vec` extension via the `sqlite_vec` Python
package  and drops any prior `demo/llm_wiki/wiki.db`. `vec0` requires an
`INTEGER` rowid, so the human-readable `slug` lives on the base `wiki_pages`
table (and as an `UNINDEXED` FTS5 column) and the integer `id` carries the
JOIN. The script also creates `wiki_pages_fts`, `wiki_pages_vec`, the
`wiki_log` activity table, and `AFTER INSERT` / `AFTER UPDATE` triggers that
keep both mirrors in sync. See [setup.py](setup.py) for the schema.

### 4b. (Optional) Seed sample pages

To explore the demo with non-empty data, [seed.py](seed.py) splits the
markdown files in [`data/test_corpus/`](../../data/test_corpus/) (Alice in
Wonderland, Jane Eyre, The Art of War) into ~110 paragraph-sized pages,
computes a real `bge-small-en-v1.5` embedding for each one in Python, and
INSERTs them through the same triggers — so `skardi run wiki-search-hybrid`
and `skardi run wiki-list` work end-to-end without writing a page yourself.

```bash
pip install transformers torch
python demo/llm_wiki/seed.py
```

Embeddings use CLS pooling + L2 normalization, matching what the `candle()`
UDF produces, so seeded pages and pages later written via
`skardi run wiki-create` share the same vector space. Pages are tagged by
source (`page_type = alice | jane-eyre | art-of-war`) so
`skardi run wiki-list -p page_type_pattern=alice -p slug_prefix=% -p limit=100`
shows only Alice excerpts.

Re-running `seed.py` on an already-seeded DB will fail on the
`UNIQUE(slug)` constraint — re-run `setup.py` first to drop and recreate.

### 5. Start `skardi-server` for the CLI flavour

Everything the demo needs is under [cli/](cli/):

```
demo/llm_wiki/cli/
  ctx.yaml        # registers wiki.db as a SQLite catalog data source
  pipelines/      # pipeline YAMLs (one per verb)
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
  name: cli-context
  version: 1.0.0

spec:
  data_sources:
    - name: wiki
      type: sqlite
      path: demo/llm_wiki/wiki.db
      access_mode: read_write
      hierarchy_level: catalog
      options:
        extensions_env: SQLITE_VEC_PATH
```

The pipeline YAMLs in [cli/pipelines/](cli/pipelines/) use the same
`metadata` + `query` shape as the server pipelines, with `{param}`
placeholders for named parameters — just targeting the SQLite stack
(`sqlite_knn` / `sqlite_fts` / `vec_to_binary(candle(...))`) instead of
`pg_knn` / `pg_fts`.

Start a `skardi-server` pointed at this ctx and pipeline directory. Use a
different `--port` from the Postgres flavour above so both can run at once
if you want to compare them; `SQLITE_VEC_PATH` must still be set in the
shell that starts the server, since it's the server process that now opens
`wiki.db` and loads the extension:

```bash
cargo run -p skardi-server --features candle -- \
  --ctx demo/llm_wiki/cli/ctx.yaml \
  --pipeline demo/llm_wiki/cli/pipelines/ \
  --port 8081
```

Point the CLI at it once so the commands below don't need `--server` on
every line:

```bash
export SKARDI_SERVER_URL=http://127.0.0.1:8081
```

### 6. Discover the pipelines

There's no alias layer to set up anymore — every pipeline is called
directly by its `metadata.name`. List and inspect what the server loaded:

```bash
skardi pipeline list
skardi pipeline show wiki-search-hybrid
```

If you want a shorter verb for one you type constantly, an ordinary shell
alias works fine and needs nothing from skardi itself:

```bash
alias wiki-grep='skardi run wiki-search-hybrid -p'
# wiki-grep query="white rabbit" -p text_query="white rabbit" -p vector_weight=0.5 -p text_weight=0.5 -p limit=5
```

### 7. `wiki-create` — create a new page

```bash
skardi run wiki-create \
  -p slug=entity/alan-turing \
  -p title="Alan Turing" \
  -p page_type=entity \
  -p content='# Alan Turing

British mathematician and logician who formalized the concepts of algorithm and computation with the Turing machine.'
```

`wiki-create` ([cli/pipelines/create.yaml](cli/pipelines/create.yaml))
computes the embedding inline with `candle()`, packs it with
`vec_to_binary()`, and INSERTs the row. The `AFTER INSERT` trigger then
mirrors the row to `wiki_pages_fts` and `wiki_pages_vec` atomically.

> Why does [create.yaml](cli/pipelines/create.yaml) wrap the seed row as
> `SELECT {slug} AS slug, ... FROM (...)` instead of using `VALUES`?
> DataFusion's INSERT planner currently propagates the INSERT target schema
> (5 columns) down into any immediate-child `VALUES` clause and validates row
> width against it, ignoring the intermediate projection that adds
> `vec_to_binary(candle(...))`. The SELECT-wrapper keeps the subquery's own
> schema in scope so the projection lands the row at full width.

### 7b. `wiki-bulk-create` — seed many pages from one long markdown doc

`wiki-create` is for hand-curated pages, one per call. To seed a long
document — a chapter, an RFC, a transcript — `wiki-bulk-create` chunks the
body inline with [`chunk('markdown', …)`](../../docs/chunk.md), embeds each
chunk with `candle()`, and inserts one page per chunk. Each call goes
through the same `AFTER INSERT` trigger as `wiki-create`, so the resulting
pages are immediately searchable via `wiki-search-hybrid`.

```bash
skardi run wiki-bulk-create \
  -p slug_prefix=rfc/9110 \
  -p title="RFC 9110: HTTP Semantics" \
  -p page_type=rfc \
  -p content="$(cat rfc9110.md)"
```

Override the chunk shape on the same line:

```bash
skardi run wiki-bulk-create \
  -p slug_prefix=alice/ch1 \
  -p title="Alice in Wonderland — Chapter 1" \
  -p page_type=alice \
  -p content="$(cat data/test_corpus/alice_sample.md)" \
  -p chunk_size=400 -p overlap=80
```

Synthesised slugs follow `{slug_prefix}/p<NNN>` (`rfc/9110/p001`,
`/p002`, …) and titles get a `#N` suffix so each chunk is a distinct
page visible to `wiki-list` and `wiki-search-hybrid`. Per-page edits stay on
`wiki-delete` + `wiki-create` (or direct UPDATE against SQLite) —
`wiki-bulk-create` is a one-shot seeder, not a re-runnable upsert.
Re-running it on the same slug prefix will fail on the `UNIQUE(slug)`
constraint.

This is the same flow [seed.py](seed.py) implements in Python today,
but in pure SQL: `chunk('markdown', body, ...)` replaces the manual
paragraph splitter and `candle()` replaces the Python embedding step.
Seeding a fresh corpus needs only `setup.py` + `wiki-bulk-create` calls —
no Python embedding loop.

`wiki-bulk-create` calls `chunk()` in addition to `candle()`, so start the
server with the `rag` feature umbrella (bundles `embedding` + `chunking`)
instead of just `candle` if you want to use it:

```bash
cargo run -p skardi-server --features rag -- \
  --ctx demo/llm_wiki/cli/ctx.yaml \
  --pipeline demo/llm_wiki/cli/pipelines/ \
  --port 8081
```

### 8. Edit an existing page (`wiki-delete` + `wiki-create`)

DataFusion's UPDATE planner unparses each `SET` expression back to SQL for
the underlying SQLite connection to execute, and it can't currently render a
Binary scalar (the packed-f32 embedding from `vec_to_binary(candle(...))`)
as a SQL literal. The portable workaround is **delete + re-insert** — the
`AFTER DELETE` trigger cleans both mirrors, the `AFTER INSERT` trigger
repopulates them, and the new row picks up a fresh `updated_at`.

```bash
skardi run wiki-delete -p slug=entity/alan-turing

skardi run wiki-create \
  -p slug=entity/alan-turing \
  -p title="Alan Turing" \
  -p page_type=entity \
  -p content='# Alan Turing

British mathematician, logician, and cryptanalyst who broke the Enigma cipher at Bletchley Park.'
```

If you'd rather edit the row in place from a SQLite client (e.g. `sqlite3`),
the original `AFTER UPDATE` trigger is still installed and will refresh both
mirrors when an `UPDATE wiki_pages SET ...` runs against the underlying
database directly — only the `skardi run` path needs the delete-and-reinsert
dance.

### 9. `wiki-get` — fetch one page by slug

If you ran the optional seed step in 4b, every paragraph in the corpus is
already a page. The opening of *Alice in Wonderland* is at
`alice/chapter-i-down-the-rabbit-hole/p001`:

```bash
skardi run wiki-get -p slug=alice/chapter-i-down-the-rabbit-hole/p001
```

Under the hood this runs [cli/pipelines/get.yaml](cli/pipelines/get.yaml):
`SELECT slug, title, page_type, content, updated_at FROM wiki.main.wiki_pages WHERE slug = {slug}`.

### 10. `wiki-search-hybrid` — hybrid search (RRF over FTS + vector)

Both halves of the RRF merge come from the same call — semantic
nearest-neighbour over `sqlite_knn` and BM25 over `sqlite_fts`. Unlike the
old alias, `query` and `text_query` are independent params you set
explicitly on every call:

```bash
# Pure semantic intent — finds the rabbit-hole paragraphs in Alice ch.1
skardi run wiki-search-hybrid \
  -p query="white rabbit pocket watch" \
  -p text_query="white rabbit pocket watch" \
  -p vector_weight=0.5 -p text_weight=0.5 -p limit=5

# Same query crosses corpora — top hits mix Alice and Jane Eyre
skardi run wiki-search-hybrid \
  -p query="young girl sent away to her room" \
  -p text_query="young girl sent away to her room" \
  -p vector_weight=0.5 -p text_weight=0.5 -p limit=5
```

Set `query` and `text_query` to different strings when you want a loose
semantic intent paired with strict keyword filters:

```bash
skardi run wiki-search-hybrid \
  -p query="Sun Tzu" \
  -p text_query="strategy OR victory OR planning" \
  -p vector_weight=0.3 -p text_weight=0.7 -p limit=5
```

See [cli/pipelines/search_hybrid.yaml](cli/pipelines/search_hybrid.yaml) for
the full RRF merge, or run `skardi pipeline show wiki-search-hybrid` to see
every parameter it expects.

### 11. `wiki-list` — browse by type or slug prefix

```bash
skardi run wiki-list -p page_type_pattern=% -p slug_prefix=% -p limit=100

# All Alice excerpts (page_type tags pages by source after seeding)
skardi run wiki-list -p page_type_pattern=alice -p slug_prefix=% -p limit=100

# Just one chapter
skardi run wiki-list -p page_type_pattern=% \
  -p slug_prefix='art-of-war/chapter-i-laying-plans/%' -p limit=100
```

### 12. `wiki-log-append` — append an activity entry

```bash
skardi run wiki-log-append \
  -p event_type=ingest \
  -p slug=entity/alan-turing \
  -p message="Created from Wikipedia article."
```

### Falling back to raw SQL

`skardi run` is a thin layer over the pipeline YAMLs — the underlying
queries are still plain SQL against the `wiki` source registered in
[cli/ctx.yaml](cli/ctx.yaml). If you want to experiment ad-hoc, `skardi
query` sends any SQL straight to the same server (same exported
`SKARDI_SERVER_URL` from step 5):

```bash
skardi query -e "SELECT slug, title, page_type FROM wiki.main.wiki_pages ORDER BY updated_at DESC LIMIT 10" --table
```

### Cleanup

```bash
pkill -f skardi-server   # or Ctrl-C the shell it's running in
rm demo/llm_wiki/wiki.db
```
