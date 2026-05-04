<div align="center">
<p align="center">

<img src="asset/logo.png" alt="Skardi Logo" width="700">

**Skardi is an open-source agent data plane** — the single layer every data call your agent makes flows through. RAG retrieval, table lookups, vector search, audit writes: declare each one as parameterized SQL in a YAML pipeline, and Skardi serves it as both a REST endpoint and a shell verb your agent can call as a tool, federated across Postgres, SQLite, MongoDB, S3, data lakes, and vector stores in a single query.

**Federated** · one engine over every source &nbsp;·&nbsp; **Declarative** · YAML pipelines &nbsp;·&nbsp; **Agent-native** · REST + shell + MCP-soon

<a href="https://skardilabs.github.io/skardi-docs/">Documentation</a> •
<a href="#roadmap">Roadmap</a> •
<a href="https://discord.gg/S5YQQPEV2m">Discord</a>

[License]: https://opensource.org/licenses/Apache-2.0
[License Badge]: https://img.shields.io/badge/License-Apache%202.0-orange.svg
[CI]: https://github.com/SkardiLabs/skardi/actions/workflows/ci.yml
[CI Badge]: https://github.com/SkardiLabs/skardi/actions/workflows/ci.yml/badge.svg
[Codecov]: https://codecov.io/gh/SkardiLabs/skardi
[Codecov Badge]: https://codecov.io/gh/SkardiLabs/skardi/branch/main/graph/badge.svg
[crates.io]: https://crates.io/crates/skardi
[crates.io Badge]: https://img.shields.io/crates/v/skardi.svg
[Docs]: https://docs.rs/skardi
[Docs Badge]: https://docs.rs/skardi/badge.svg
[Discord]: https://discord.gg/S5YQQPEV2m
[Discord Badge]: https://img.shields.io/badge/Discord-Join-5865F2?logo=discord&logoColor=white

[![License Badge]][License]
[![CI Badge]][CI]
[![Codecov Badge]][Codecov]
[![crates.io Badge]][crates.io]
[![Docs Badge]][Docs]
[![Discord Badge]][Discord]

[![Deploy on Sealos](https://sealos.io/Deploy-on-Sealos.svg)](https://sealos.io/products/app-store/skardi/)

</p>
</div>

<hr />

## What is an "agent data plane"?

Borrowing the phrase from cloud infra: your AI agent has two layers. The **control plane** is the reasoning loop — prompts, tool selection, your orchestration code, your YAML configs. The **data plane** is where every byte of context actually comes from and goes to: vector DB hits, SQL queries, file reads, writes back, audit trails. The control plane is mostly fine these days (LLMs are smart, agent SDKs are mature). The data plane is the bottleneck — most agents today have a tangled one: a Pinecone client here, a Postgres query there, a hand-rolled RRF merge in Python, an HTTP wrapper around an internal API, glue code on top of glue code.

**Skardi *is* the data plane.** A single open-source server (and CLI) where every one of your agent's data calls goes, declared once as parameterized SQL in YAML, then served as both a REST endpoint and a shell verb. Federated, so one query can JOIN across Postgres, SQLite, MongoDB, S3, data lakes, and vector stores. Fast enough to sit in your agent's request path — typically tens of milliseconds for a parameterized Postgres / SQLite query, dominated by your data source's own latency.

**Concretely, you're replacing this:**

```python
# What an agent retrieval tool usually looks like today:
def search_wiki(query: str, limit: int = 10):
    embedding = openai.embeddings.create(input=query, model="...").data[0].embedding
    vec_hits  = pg.execute("SELECT id FROM pages ORDER BY emb <=> %s LIMIT 80", [embedding])
    fts_hits  = pg.execute("SELECT id FROM pages WHERE tsv @@ plainto_tsquery(%s) LIMIT 60", [query])
    fused     = rrf_merge(vec_hits, fts_hits)            # hand-rolled
    return pg.execute("SELECT slug,title FROM pages WHERE id = ANY(%s)", [fused[:limit]])
# + a Flask/FastAPI route exposing it, + auth, + logging, + a schema for the LLM...
```

**…with a 20-line YAML pipeline** (full version in [Quick Start](#quick-start) below). Skardi handles the embedding call, the hybrid-search merge, the HTTP route, the parameter parsing, and the JSON response shape. Your agent calls it as `POST /wiki-search-hybrid/execute` over REST or `skardi grep "..."` from any shell — every retrieval flows through one engine instead of N hand-rolled tools.

Build RAG, hybrid search, agent-callable APIs, and async batch writes across databases, files, data lakes, and vector stores — all behind the same SQL surface.

```text
   your agent  ──▶  skardi-server  ──┬─▶  Postgres / MySQL / SQLite / MongoDB / Redis
   (Claude / GPT /     │              ├─▶  S3 / GCS / Azure (CSV, Parquet, Lance)
    Cursor / your      │              ├─▶  Apache Iceberg, Lance datasets
    own loop)          │              └─▶  pgvector, sqlite-vec, Lance KNN, SeekDB HNSW
                       │
                  parameterized SQL  ──▶  one JOIN can span all of the above
                  (YAML pipelines)
```

- **`skardi` CLI** — run federated SQL or any pipeline directly from a shell. Drop it into Claude Code, Cursor, or any agent with a Bash tool and it's wired with no MCP config.
- **`skardi-server`** — same engine over HTTP, with two surfaces: **online serving** (a YAML pipeline becomes a parameterized REST endpoint with an inferred request/response schema) and **offline jobs** (async batch writes into Lance or any read-write DB; if a job fails halfway you don't get a corrupted dataset, and every run is logged in a SQLite ledger you can list and inspect).
- **Skardi-server is stateful but lightweight** — a single Rust process, plus a small SQLite file for the run ledger and (optional) auth. One server can serve many agents; deploy it next to your data, behind your usual auth.

> **Glossary (for terms used below).**
> **Pipeline** — a YAML file with a parameterized SQL query; becomes one REST endpoint + one CLI verb.
> **Job** — a YAML file like a pipeline, but runs asynchronously and writes its result rows to a destination table.
> **`ctx.yaml`** — the config that lists your data sources (a Postgres URL, a SQLite path, an S3 bucket, etc.) and gives each one a name you can reference in SQL.
> **DataFusion** — an in-process Rust SQL engine ([Apache project](https://datafusion.apache.org/)). Runs inside `skardi-server`, so there is no separate cluster to manage. "Single-node" means the engine itself is in-process; the data sources it queries can be remote.
> **Lance** — an open columnar file format with built-in vector and full-text indexes ([lancedb.github.io/lance](https://lancedb.github.io/lance/)). Useful as a job destination when you want a self-contained queryable dataset on disk or S3.
> **Hybrid search / RRF** — combining keyword (full-text, "FTS") and semantic (vector, "KNN") results into one ranking. RRF (Reciprocal Rank Fusion) is the standard merge formula. Skardi does it in a single SQL query so there is no Python re-ranking layer.
> **Catalog mode** — point Skardi at a database connection and let it auto-discover every table, instead of registering tables one at a time. Catalog-registered tables are addressed in SQL with the 3-part `catalog.schema.table` name (e.g. `wiki.main.wiki_pages_vec` below: `wiki` is the source name, `main` is the SQLite schema, `wiki_pages_vec` is the table).
> **`sqlite_knn` / `sqlite_fts`** — Skardi UDFs that wrap [sqlite-vec](https://github.com/asg017/sqlite-vec) KNN and SQLite FTS5 inside SQL; analogous `pg_knn` / `pg_fts` exist for Postgres + pgvector. Full UDF reference in [docs/sqlite/](docs/sqlite/) and [docs/postgres/](docs/postgres/).
> **SeekDB** — a MySQL-wire-compatible store with native HNSW vector indexes and FULLTEXT FTS, usable as a single-source replacement for "Postgres + pgvector + tsvector" (see [docs/seekdb/](docs/seekdb/)).

> **Beta.** Skardi is under active development. APIs may move. Hit us on [Discord](https://discord.gg/S5YQQPEV2m) if you want to co-design a POC.

<p align="center">
  <a href="https://htmlpreview.github.io/?https://github.com/SkardiLabs/skardi/blob/main/asset/architecture-open-source.html">
    <picture>
      <img src="asset/architecture-open-source.svg" alt="Skardi open source architecture — between any AI agent and your data sources" width="100%"/>
    </picture>
  </a>
  <br>
  <sub><em>fig. 01 — skardi open source topology.</em> <a href="https://htmlpreview.github.io/?https://github.com/SkardiLabs/skardi/blob/main/asset/architecture-open-source.html">View interactive diagram →</a></sub>
</p>

---

## Drop-in skills — go from zero to grounded retrieval in 60 seconds

Don't want to hand-write YAML to see what Skardi does for your agent? The fastest path is to install one of our ready-made skills from **[skardi-skills](https://github.com/SkardiLabs/skardi-skills)**. Each skill renders the `ctx.yaml` + pipelines for you and exposes them as agent-callable verbs — zero config to write yourself, your agent can start retrieving immediately.

- **[`auto_knowledge_base`](https://github.com/SkardiLabs/skardi-skills/tree/main/auto_knowledge_base)** — point it at a directory of documents and you have a queryable local RAG one command later. Chunking, embedding, indexing, and hybrid search are exposed to your agent as a `skardi grep` verb. Zero infra by default (SQLite + local embeddings), so any Claude Code / Cursor session gets a grounded, citable knowledge base over your files.
- **[`auto_rag`](https://github.com/SkardiLabs/skardi-skills/tree/main/auto_rag)** — server-backed hybrid-search RAG via `skardi-server` on top of a datastore you already control (Postgres + pgvector, MongoDB, or Lance). The skill renders the config, starts the server, and drives ingestion and queries through REST — for when retrieval needs to be shared across multiple agents or processes.

Drop either skill into Claude Code or Cursor and your agent's data plane is wired in one prompt. Want to see what's happening under the hood, or build pipelines of your own? Read on.

---

## Why not just write a Python function that wraps SQL?

Fair question — that is the alternative, and for one tool it's fine. Skardi earns its keep when an agent needs more than one of these at once:

1. **One engine over every source.** Skardi runs SQL across Postgres, MySQL, SQLite, MongoDB, Redis, S3 / GCS / Azure files, Iceberg, Lance, and SeekDB — and `JOIN`s across them in one query. A handrolled Python wrapper hits one source per function; cross-source JOINs become application code.
2. **Same query, two surfaces.** The same pipeline YAML is callable both as a REST endpoint (for hosted agents, multi-process setups) and as a `skardi` CLI verb (for Claude Code, Cursor, any Bash-tool agent) with no extra glue.
3. **Retrieval primitives in SQL.** Vector KNN, full-text search, hybrid (RRF) merge, inline embedding, inline chunking — all UDFs you can compose in plain SQL instead of stitching together Python libraries. So a RAG pipeline (chunk → embed → write → search) is one file, not a service.
4. **Async writes you can trust.** A job that writes 100k rows to Lance commits atomically; if the process dies mid-write, the dataset is not corrupted, and every run is logged with `submitted/running/succeeded/failed` plus parameters in a SQLite ledger you can list and inspect.
5. **A discovery surface for the agent.** `GET /data_source` returns each table's schema plus the natural-language description you wrote in YAML, so the agent picks the right pipeline before querying — instead of discovering it by trial and error.

If your agent only ever needs one parameterized Postgres query, a Python function is simpler. If it needs five, plus a vector store, plus a CSV in S3, plus an audit log — that is what Skardi is for.

For a deeper read on the agent-data-plane idea — what it borrows from cloud infra, why agents need their own, how it differs from a normal data warehouse — see [docs/agent_data_plane.md](docs/agent_data_plane.md).

---

## ⭐️ Star the Repository

If you find Skardi useful or interesting, a GitHub Star ⭐️ would be greatly appreciated — it helps others discover the project and signals which directions are worth pushing on.

<p align="center">
  <a href="https://github.com/SkardiLabs/skardi">
    <img src="asset/skardi-star.gif" alt="Star Skardi" width="700">
  </a>
</p>

---
## Quick Start

### Install the CLI

```bash
# From source (recommended during beta)
git clone https://github.com/SkardiLabs/skardi.git
cd skardi
cargo install --locked --path crates/cli
```

Or grab a pre-built binary:

```bash
curl -fSL "https://github.com/SkardiLabs/skardi/releases/latest/download/skardi-$(uname -m | sed 's/arm64/aarch64/')-$(uname -s | sed 's/Linux/unknown-linux-gnu/' | sed 's/Darwin/apple-darwin/').tar.gz" | tar xz
sudo mv skardi /usr/local/bin/
```

| Platform | Target |
|----------|--------|
| Linux x86_64 | `skardi-x86_64-unknown-linux-gnu.tar.gz` |
| Linux ARM64 | `skardi-aarch64-unknown-linux-gnu.tar.gz` |
| macOS ARM64 (Apple Silicon) | `skardi-aarch64-apple-darwin.tar.gz` |

> macOS Intel binaries are not published. [Build from source](#building-from-source) if you need one.

### First-time agent loop (two minutes)

**Step 1 — ad-hoc SQL, no server, no pre-registration.** The CLI prints results as a pretty-printed table to stdout — see [docs/cli.md](docs/cli.md).

```bash
skardi query --sql "SELECT * FROM './data/products.csv' LIMIT 10"
skardi query --sql "SELECT * FROM 's3://mybucket/events.parquet' LIMIT 10"
```

**Step 2 — register named sources in a `ctx.yaml`.** Five example lines:

```yaml
# ctx.yaml — describes where your data lives. Each entry gets a name you use in SQL.
kind: context
spec:
  data_sources:
    - name: products            # referenceable as `products` in SQL
      type: sqlite
      path: ./shop.db
      access_mode: read_write
      options: { table: products }       # register one specific table…
    - name: warehouse
      type: postgres
      connection_string: "postgresql://localhost:5432/warehouse"
      hierarchy_level: catalog           # …or auto-discover every table in the DB.
                                         # Reference catalog tables in SQL as
                                         # `warehouse.<schema>.<table>` (3-part name).
```

```bash
skardi query --ctx ./ctx.yaml --sql "SELECT * FROM products LIMIT 10"
```

**Step 3 — turn a parameterized SQL into an agent-callable verb.** Two YAMLs from [`demo/llm_wiki/cli/`](demo/llm_wiki/cli/) — the actual files, not pseudo-code:

```yaml
# pipelines/search_hybrid.yaml — declares the SQL once; Skardi infers the params
kind: pipeline
metadata: { name: wiki-search-hybrid }
spec:
  query: |
    WITH vec AS (
      SELECT id, ROW_NUMBER() OVER (ORDER BY _score ASC) AS rk
      FROM sqlite_knn('wiki.main.wiki_pages_vec', 'embedding',
           (SELECT candle('models/bge-small-en-v1.5', {query})), 80)
    ),
    fts AS (
      SELECT id, slug, title, ROW_NUMBER() OVER (ORDER BY _score DESC) AS rk
      FROM sqlite_fts('wiki.main.wiki_pages_fts', 'content', {text_query}, 60)
    )
    SELECT COALESCE(f.slug, p.slug) AS slug, COALESCE(f.title, p.title) AS title,
           COALESCE({vector_weight}/(60.0 + v.rk), 0)
         + COALESCE({text_weight} /(60.0 + f.rk), 0) AS rrf_score
    FROM vec v FULL OUTER JOIN fts f USING (id)
    LEFT JOIN wiki.main.wiki_pages p ON p.id = COALESCE(v.id, f.id)
    ORDER BY rrf_score DESC LIMIT {limit}
```

```yaml
# aliases.yaml — gives the pipeline a short shell verb, with positional + default args
kind: aliases
spec:
  grep:
    pipeline: wiki-search-hybrid
    positional: [query]
    defaults: { text_query: "{query}", text_weight: "0.5", vector_weight: "0.5", limit: "10" }
    description: Hybrid search over the wiki (RRF of sqlite_knn + sqlite_fts)
```

Now any agent with a shell can call it:

```bash
skardi grep "turing machine computation" --limit=10
```

The output your agent sees is the standard Arrow-pretty table on stdout (`+----+--------+ ...`). Over the server (next section), the same pipeline is mounted at `POST /wiki-search-hybrid/execute` — the request body is a JSON object whose keys match the `{...}` placeholders in the SQL (Skardi infers this schema and serves it on `GET /data_source` so the agent can read it). One full cycle:

```bash
curl -X POST http://localhost:8080/wiki-search-hybrid/execute \
  -H "Content-Type: application/json" \
  -d '{"query": "turing machine computation",
       "text_query": "turing machine computation",
       "vector_weight": 0.5, "text_weight": 0.5, "limit": 10}'
```

```json
{ "success": true,
  "data": [ { "slug": "concept/turing-machine", "title": "Turing machine", "rrf_score": 0.0312 }, ... ],
  "rows": 10, "execution_time_ms": 23 }
```

Drop `skardi` into a Claude Code or Cursor session and the agent can already use any pipeline you've declared as a tool via its Bash integration. No MCP config, no separate server — that's the MVP design intent.

### Skardi Server — online serving + offline jobs

```bash
cargo run --bin skardi-server -- \
  --ctx ctx.yaml \
  --pipeline pipelines/ \
  --jobs jobs/ \
  --port 8080
```

```bash
# Pipelines: synchronous answer
curl -X POST http://localhost:8080/product-search-demo/execute \
  -H "Content-Type: application/json" \
  -d '{"brand": null, "max_price": 100.0, "limit": 5}'

# Jobs: submit an async write-to-destination
skardi job run backfill-to-lake --param from_date='2026-01-01'
skardi job status <run_id>
```

Full reference:
- **CLI** — [docs/cli.md](docs/cli.md)
- **Server** — [docs/server.md](docs/server.md)
- **Pipelines (online serving)** — [docs/pipelines.md](docs/pipelines.md)
- **Jobs (offline batch)** — [docs/jobs.md](docs/jobs.md)
- **Table descriptions for agent discovery** — [docs/semantics.md](docs/semantics.md)
- **Background — design intent** — [docs/agent_data_plane.md](docs/agent_data_plane.md)

---

## Worked examples

For end-to-end walkthroughs — RAG, recommendations, an agent-native wiki, a simple REST backend — see the [`demo/`](demo/) directory. Each demo ships as a self-contained `ctx.yaml` plus pipelines (and sometimes jobs), so reading the YAML shows the Skardi shape in practice. Full list in [Demo & Examples](#demo--examples) below.

---

## Supported Data Sources

| Type | CRUD | Description | Docs |
|------|------|-------------|------|
| CSV | Read | Local or remote CSV files | [docs/server.md](docs/server.md) |
| Parquet | Read | Local or remote Parquet files | [docs/server.md](docs/server.md) |
| JSON / NDJSON | Read | Local or remote JSON files | [docs/cli.md](docs/cli.md) |
| PostgreSQL | Full | Table or catalog registration, pgvector KNN | [docs/postgres/](docs/postgres/) |
| MySQL | Full | Table or catalog registration | [docs/mysql/](docs/mysql/) |
| SQLite | Full | Table or catalog registration, sqlite-vec KNN, FTS | [docs/sqlite/](docs/sqlite/) |
| MongoDB | Full | Collections with point lookups | [docs/mongo/](docs/mongo/) |
| Redis | Full | Hashes mapped to SQL rows | [docs/redis/](docs/redis/) |
| SeekDB | Full | MySQL-wire CRUD, native FULLTEXT FTS, HNSW VECTOR KNN | [docs/seekdb/](docs/seekdb/) |
| Apache Iceberg | Read | Schema evolution, partition pruning | [docs/iceberg/](docs/iceberg/) |
| Lance | Read (job-write) | KNN vector search, BM25 FTS; job destination | [docs/lance/](docs/lance/) |
| S3 / GCS / Azure | Read | CSV, Parquet, Lance from object stores | [docs/S3_USAGE.md](docs/S3_USAGE.md) |

---

## Additional Features

- **Federated queries** — JOIN across different source types in one SQL query (CSV vs Postgres vs Lance, etc). See [docs/federated-queries.md](docs/federated-queries.md).
- **Table descriptions for agents** — write natural-language descriptions of each table and column in YAML; Skardi serves them on `GET /data_source` so the agent can read what each table is for before it queries. See [docs/semantics.md](docs/semantics.md).
- **Authentication** — session-based via better-auth + SQLite. See [docs/auth/](docs/auth/).
- **ONNX inference** — inline model predictions in SQL via an `onnx_predict` UDF. See [docs/onnx_predict.md](docs/onnx_predict.md).
- **Embedding inference** — call embedding models from inside SQL via the `candle()` UDF (local GGUF / Candle models, or remote OpenAI-style APIs). See [docs/embeddings/](docs/embeddings/).
- **Observability** — OpenTelemetry traces / metrics / logs with a pre-configured Grafana stack. See [docs/observability.md](docs/observability.md).

---

## Architecture

<details>
<summary>Click to expand Skardi's architecture diagram</summary>

<p align="center">
  <img src="asset/architecture.png" alt="Skardi Architecture" width="800">
</p>

</details>

---

## Docker

```bash
# Build
docker build -t skardi .
docker build -t skardi --build-arg FEATURES=rag .   # adds embedding + chunk UDFs

# Or pull pre-built
docker pull ghcr.io/skardilabs/skardi/skardi-server:latest
docker pull ghcr.io/skardilabs/skardi/skardi-server-rag:latest   # embedding + chunk UDFs

# Run
docker run --rm \
  -v /path/to/your/ctx.yaml:/config/ctx.yaml \
  -v /path/to/your/pipelines:/config/pipelines \
  -p 8080:8080 \
  skardi \
  --ctx /config/ctx.yaml \
  --pipeline /config/pipelines \
  --port 8080
```

## Cloud (Sealos)

The fastest cloud path is the [Sealos](https://sealos.io) template in **[skardi-skills](https://github.com/SkardiLabs/skardi-skills)** — our growing library of ready-to-use Skardi setups. One-click launch, no local setup.

## Building from Source

```bash
git clone https://github.com/SkardiLabs/skardi.git
cd skardi

cargo build --release -p skardi-cli
cargo build --release -p skardi-server

# With the full RAG kit (embedding UDFs + chunk UDF)
cargo build --release -p skardi-server --features rag

# Or just the embedding UDFs (ONNX, GGUF, Candle, remote embed) without chunking
cargo build --release -p skardi-server --features embedding
```

---

## Demo & Examples

| Directory | Description |
|-----------|-------------|
| [demo/llm_wiki/](demo/llm_wiki/) | Agent-native wiki (server + CLI flavors) — hybrid search, inline embeddings, agent verbs |
| [demo/simple_backend/](demo/simple_backend/) | REST backend with SQLite and optional auth |
| [demo/rag/](demo/rag/) | Retrieval-augmented generation pipeline |
| [demo/movie_recommendation/](demo/movie_recommendation/) | Movie recommendations with ONNX NCF model |

For data-source-specific demos, see the entries in [Supported Data Sources](#supported-data-sources).

---

## Roadmap

**Coming soon (not yet shipped)**: a skills generator that emits Claude Code skill files per pipeline, an MCP binding for non-Claude hosts, a first-class memory primitive (one YAML block giving an agent a memory store with keyword + semantic recall, automatic expiration, and per-session provenance), lineage capture, and snapshot-as-branch checkpoints (roll back a destructive agent write — e.g. an agent that updated 1,000 rows you don't like — in one call).

We're **building in public**. `[x]` means shipped today, `[ ]` means open for contribution. Open an issue or hop into [Discord](https://discord.gg/S5YQQPEV2m) on anything unchecked.

`1` Federated SQL engine
   - [x] One SQL engine ([DataFusion](https://datafusion.apache.org/), in-process) over CSV, Parquet, JSON, S3 / GCS / Azure, Postgres, MySQL, SQLite, MongoDB, Redis, Iceberg, Lance, SeekDB — all joinable in one query
   - [x] Register either one specific table, or point Skardi at a database (Postgres / MySQL / SQLite) and let it auto-discover all tables — one config line either way
   - [ ] Graph database sources (Neo4j / Kuzu) — to unlock graphRAG patterns alongside vector / full-text retrieval

`2` Retrieval primitives
   - [x] Vector search (KNN) — `pg_knn` (pgvector), `sqlite_knn` (sqlite-vec), Lance KNN, SeekDB HNSW
   - [x] Full-text search (FTS) — `pg_fts`, `sqlite_fts`, Lance BM25 inverted indexes, SeekDB FULLTEXT
   - [x] Hybrid search — combine keyword and semantic search results in one SQL query (RRF merge), no Python re-ranking layer
   - [x] Inline embeddings — `candle()` UDF (local GGUF / Candle models, or remote embedding APIs) called inside SQL, so content + vector stay on the same row atomically
   - [x] ONNX inference — `onnx_predict` UDF for inline model predictions in SQL
   - [x] Chunking UDF — `chunk()` with character / markdown splitters (via [`text-splitter`](https://crates.io/crates/text-splitter)) so ingestion can chunk inline in SQL ([docs](docs/chunk.md)); token / code splitters next
   - [ ] Memory primitive — give your agent a memory store (keyword + semantic recall, TTL/expiration, per-session provenance) defined in one YAML block

`3` Online serving (pipelines)
   - [x] Declarative YAML → parameterized REST endpoint with inferred request / response schema
   - [x] Built-in pipeline dashboard
   - [x] CLI pipeline binding + aliases — `skardi run <pipeline> --param=…` and user-defined verb aliases ([#90](https://github.com/SkardiLabs/skardi/pull/90))
   - [x] CLI federated SQL — `skardi query` against files, object stores, datalake formats, and databases with no server required

`4` Offline jobs
   - [x] Async batch execution with submit / poll / cancel ([#98](https://github.com/SkardiLabs/skardi/pull/98))
   - [x] Lance dataset destinations with atomic commit + crash recovery
   - [x] SQL-DML destinations (Postgres / MySQL / SQLite)
   - [x] SQLite-backed run ledger with submit-time schema diff

`5` Agent-facing bindings
   - [x] REST — every pipeline served as a parameterized HTTP endpoint
   - [x] Shell — every pipeline runnable as a `skardi` command; works in Claude Code, Cursor, and any agent with a Bash tool
   - [ ] Skills generator — `skardi skills generate --ctx <ctx.yaml> --out .claude/skills/` emits a skill Markdown per pipeline for Claude Code / Desktop auto-discovery
   - [ ] MCP binding — same pipeline YAML projected to MCP tools for non-Claude hosts

`6` Governance & lineage
   - [x] Plain-English table descriptions — a `kind: semantics` YAML overlay attaching natural-language descriptions to tables / columns (supports both bare source names and fully-qualified `catalog.schema.table` paths); served on `GET /data_source` so agents can discover what each table is for before querying
   - [ ] Agent-callable `describe` verb — CLI / pipeline form on top of the discovery endpoint
   - [ ] Lineage capture — `agent_id`, `session_id`, `tool_call_id`, `timestamp` on writes; queryable from metadata tables
   - [ ] Agent identity passthrough — any binding injects client identity into a SQL context var pipelines can read
   - [ ] Snapshot-as-branch / agent checkpoints — Iceberg / Lance-backed `git checkout`-like semantics: if your agent updates 1,000 rows and you don't like the result, roll back in one call

`7` Ops
   - [x] Session auth — drop-in user auth via [better-auth](https://www.better-auth.com/) backed by SQLite
   - [x] Observability — OpenTelemetry traces / metrics / logs with a pre-configured Grafana stack
   - [x] Docker + pre-built binaries — Linux x86_64 / ARM64, macOS ARM64

---

## Community

Building an agent on top of Skardi, or want to influence the roadmap above? Join us on [Discord](https://discord.gg/S5YQQPEV2m), file an issue, or open a PR. We read everything.

## License

Apache 2.0 — see [LICENSE](LICENSE).
