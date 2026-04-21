<div align="center">
<p align="center">

<img src="asset/logo.png" alt="Skardi Logo" width="700">

**Spark for Agents — a data platform that gives AI agents full data autonomy so every dataset in your stack becomes something an agent can actually use.**

<a href="https://skardilabs.github.io/skardi-docs/">Documentation</a> •
<a href="docs/spark_for_agents.md">"Spark for Agents" narrative</a> •
<a href="#public-roadmap">Public roadmap</a> •
<a href="https://discord.gg/S5YQQPEV2m">Discord</a>

[License]: https://opensource.org/licenses/Apache-2.0
[License Badge]: https://img.shields.io/badge/License-Apache%202.0-orange.svg
[CI]: https://github.com/SkardiLabs/skardi/actions/workflows/ci.yml
[CI Badge]: https://github.com/SkardiLabs/skardi/actions/workflows/ci.yml/badge.svg
[crates.io]: https://crates.io/crates/skardi
[crates.io Badge]: https://img.shields.io/crates/v/skardi.svg
[Docs]: https://docs.rs/skardi
[Docs Badge]: https://docs.rs/skardi/badge.svg
[Discord]: https://discord.gg/S5YQQPEV2m
[Discord Badge]: https://img.shields.io/badge/Discord-Join-5865F2?logo=discord&logoColor=white

[![License Badge]][License]
[![CI Badge]][CI]
[![crates.io Badge]][crates.io]
[![Docs Badge]][Docs]
[![Discord Badge]][Discord]

[![Deploy on Sealos](https://sealos.io/Deploy-on-Sealos.svg)](https://sealos.io/products/app-store/skardi/)

</p>
</div>

<hr />

## What is Skardi?

Skardi is an **open-source data platform for AI agents** — Pick any data in your stack (CSV, Parquet, S3, Postgres, MySQL, SQLite, MongoDB, Redis, Iceberg, Lance, SeekDB) and Skardi turns it into something an agent can query, join, write to, and operate on autonomously — through SQL, REST, shell, and (soon) MCP.

Skardi is **Spark for Agents**. Spark gave data teams a single engine over every storage format; the agent era needs the same, shaped for how agents actually work — schemas agents can *read*, outputs agents can *parse*, tools agents can *discover*, and writes agents can *trust*.

- **`skardi` CLI** — federated SQL + parameterized pipelines as shell commands. Drop it into any agent that has a Bash tool (Claude Code, Cursor, custom loops) and it's wired.
- **`skardi-server`** — declarative SQL pipelines served as REST endpoints, plus async batch writes into Lance or any read-write DB. The place long-running agent work lives.
- **Soon** — skills generation for auto-discovery, MCP binding for non-Claude hosts, a first-class **memory primitive** (structured + vector + FTS + provenance + TTL), lineage, and agent-scoped governance.

> **Beta.** Skardi is under active development. APIs may move. Hit us on [Discord](https://discord.gg/S5YQQPEV2m) if you want to co-design a POC.

---

## Why "Spark for Agents"?

Agents don't lack intelligence — they lack **data autonomy**. Hand an LLM a raw schema dump and it hallucinates; hand it a bag of bespoke REST endpoints and it gets lost; hand it a vector store and it still can't JOIN. The gap isn't the model. The gap is that today's data stack was designed for humans writing queries, not agents calling tools.

Skardi closes that gap with three deliberate choices:

1. **One engine over every source.** DataFusion-based single-node federation. An agent can `JOIN` a CSV against Postgres against a Lance dataset in one query.
2. **Online serving and offline jobs, one declarative shape.** Pipelines serve parameterized SQL synchronously; jobs run the same SQL asynchronously into a durable destination (Lance or a read-write DB) with a run ledger and atomic commit — both share every binding (REST, shell, soon Claude skills + MCP).

Read the full narrative in [docs/spark_for_agents.md](docs/spark_for_agents.md).

---

## Public Roadmap

We're **building in public**, so you can see exactly where we are and what's next. The list below covers everything — foundations that already ship, work currently in flight, and the "Spark for Agents" primitives we're still building out. Help wanted on anything unchecked; open an issue or hop into Discord.

Status: ✅ shipped · 🚧 in progress · ⬜ planned

| Status | Task |
|:---:|---|
| ✅ | **Federated SQL engine** — DataFusion single-node federation across CSV, Parquet, JSON, S3 / GCS / Azure, Postgres, MySQL, SQLite, MongoDB, Redis, Iceberg, Lance, SeekDB — all joinable in a single query. |
| ✅ | **Vector search** — `pg_knn` (pgvector), `sqlite_knn` (sqlite-vec), Lance KNN, SeekDB HNSW. |
| ✅ | **Full-text search** — `pg_fts`, `sqlite_fts`, Lance BM25 inverted indexes, SeekDB FULLTEXT; RRF hybrid search in plain SQL. |
| ✅ | **Inline embeddings** — `candle()` UDF (GGUF / Candle / remote embed APIs) runs directly inside SQL, so content + vector stay on the same row atomically. |
| ✅ | **Online serving** — `skardi-server` turns pipeline YAML into parameterized REST endpoints with inferred request / response schemas and a built-in dashboard. |
| ✅ | **CLI federated SQL** — `skardi query` against local files, remote object stores, datalake formats, and databases with no server required. |
| ✅ | **CLI pipeline binding + aliases** — `skardi run <pipeline> --param=…` and user-defined verb aliases; pipeline YAML as the single source of truth. ([#90](https://github.com/SkardiLabs/skardi/pull/90)) |
| ✅ | **Session auth** — drop-in user auth via [better-auth](https://www.better-auth.com/) backed by SQLite. |
| ✅ | **Observability** — OpenTelemetry traces / metrics / logs with a pre-configured Grafana stack. |
| 🚧 | **Pipelines-as-jobs** — async `kind: job` batch execution, status, submit/poll/cancel; Lance + SQL-DML destinations. ([#98](https://github.com/SkardiLabs/skardi/pull/98), in review) |
| ⬜ | **Skills generator** — `skardi skills generate --ctx <ctx.yaml> --out .claude/skills/` emits a skill Markdown per pipeline for Claude Code / Desktop auto-discovery. |
| ⬜ | **Catalog with semantics** — NL `description` field on catalog / table / column; an agent-callable `describe` pipeline. |
| ⬜ | **Basic lineage capture** — `agent_id`, `session_id`, `tool_call_id`, `timestamp` on writes; queryable from metadata tables. |
| ⬜ | **Agent identity passthrough** — any binding injects client identity into a SQL context var pipelines can read. |
| ⬜ | **Snapshot-as-branch / agent checkpoints** — Iceberg / Lance-backed; `git checkout`-like semantics for destructive agent experiments. |

---

## What's already in the box

### Engine
- **Federated SQL across every major source** — CSV, Parquet, JSON, S3 / GCS / Azure, Postgres, MySQL, SQLite, MongoDB, Redis, Iceberg, Lance, SeekDB — all joinable in one query.
- **Register by table or by catalog** — pick per source: expose a single named table, or load an entire Postgres / MySQL / SQLite database as a DataFusion catalog. One config line either way.
- **Vector search** — native KNN via Lance, `pg_knn` (pgvector), `sqlite_knn` (sqlite-vec), SeekDB HNSW.
- **Full-text search** — Lance BM25 inverted indexes, `pg_fts`, `sqlite_fts`, SeekDB native FULLTEXT.
- **Inline embeddings** — `candle()` UDF (GGUF / Candle / remote embed APIs) directly inside SQL, so content + vector stay on the same row atomically.
- **ONNX inference** — `onnx_predict` UDF for inline model predictions in SQL.
- **Hybrid search** — RRF merge of FTS + KNN in plain SQL (see [llm_wiki demo](demo/llm_wiki/)).

### Agent-facing surfaces
- **CLI `skardi run <pipeline>`** — parameterized pipeline invocation from any shell; works in Claude Code / Cursor / any agent with a Bash tool.
- **User-defined aliases** — `skardi grep "…"` → `run wiki-search-hybrid`. Collapses multi-line SQL into agent-ergonomic verbs.
- **Declarative REST pipelines** — YAML → parameterized HTTP endpoint, with an inferred request / response schema and a built-in dashboard.
- **Batch Jobs** (in review, [#98](https://github.com/SkardiLabs/skardi/pull/98)) — async pipeline that commits to Lance or a DB destination, with a SQLite run ledger and submit / poll / cancel.

### Ops
- **Session auth** — drop-in user auth via [better-auth](https://www.better-auth.com/) backed by SQLite.
- **Observability** — OpenTelemetry traces / metrics / logs with a pre-configured Grafana stack.
- **Docker + pre-built binaries** — Linux x86_64 / ARM64, macOS ARM64.

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

```bash
# 1. Ad-hoc SQL across local + remote data — no server, no pre-registration
skardi query --sql "SELECT * FROM './data/products.csv' LIMIT 10"
skardi query --sql "SELECT * FROM 's3://mybucket/events.parquet' LIMIT 10"

# 2. Register named sources in a ctx, query them by name
skardi query --ctx ./ctx.yaml --sql "SELECT * FROM products LIMIT 10"

# 3. Turn a parameterized SQL into an agent-callable verb (alias + pipeline)
#    — now any agent with a shell can call it:
skardi grep "turing machine computation" --limit=10
```

Drop `skardi` into a Claude Code or Cursor session and the agent can already use any pipeline you've declared as a tool via its Bash integration. No MCP config, no separate server — that's the MVP design intent.

### Skardi Server — pipelines as REST, plus batch jobs

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
- **CLI** — [crates/cli/README.md](crates/cli/README.md)
- **Server** — [docs/server.md](docs/server.md)
- **Jobs** — [docs/jobs.md](docs/jobs.md)
- **Spark for Agents narrative** — [docs/spark_for_agents.md](docs/spark_for_agents.md)

---

## Worked example: [`demo/llm_wiki/`](demo/llm_wiki/) — an agent-native wiki

The fullest end-to-end demo in the repo. `llm_wiki` is a durable, editable wiki for an LLM agent — entity pages, concept pages, summaries, an index, an activity log — stored in one table that carries markdown + vector + FTS on the same row. Every agent verb (`write`, `open`, `grep`, `ls`, `log`) is one pipeline YAML plus an alias:

```bash
skardi write --slug=entity/alan-turing --title="Alan Turing" --page_type=entity --content='…'
skardi grep "turing machine computation" --limit=10
skardi open entity/alan-turing
skardi ls --slug_prefix='concept/%'
skardi log --event_type=ingest --slug=entity/alan-turing --message="…"
```

Two flavors — one on Postgres + pgvector (server), one on SQLite + sqlite-vec (pure CLI, no server, no Docker) — both driven by the same pipeline YAML format. The SQLite flavor is the clearest MVP proof: drop the CLI into any agent, get the full agent-memory loop working locally with zero infra.

---

## Supported Data Sources

| Type | CRUD | Description | Docs |
|------|------|-------------|------|
| CSV | Read | Local or remote CSV files | [docs/server.md](docs/server.md) |
| Parquet | Read | Local or remote Parquet files | [docs/server.md](docs/server.md) |
| JSON / NDJSON | Read | Local or remote JSON files | [crates/cli/README.md](crates/cli/README.md) |
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

- **Federated queries** — JOIN across different source types. See [docs/federated-queries.md](docs/federated-queries.md).
- **Authentication** — session-based via better-auth + SQLite. See [docs/auth/](docs/auth/).
- **ONNX inference** — inline model predictions in SQL. See [docs/onnx_predict.md](docs/onnx_predict.md).
- **Embedding inference** — GGUF, Candle, or remote APIs. See [docs/embeddings/](docs/embeddings/).
- **Observability** — OTel traces / metrics / logs with Grafana. See [docs/observability.md](docs/observability.md).

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
docker build -t skardi --build-arg FEATURES=embedding .

# Or pull pre-built
docker pull ghcr.io/skardilabs/skardi/skardi-server:latest

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

The fastest path to a running server is **[skardi-skills](https://github.com/SkardiLabs/skardi-skills)** — ready-to-deploy Skardi templates for [Sealos](https://sealos.io). One-click launch, no local setup.

## Building from Source

```bash
git clone https://github.com/SkardiLabs/skardi.git
cd skardi

cargo build --release -p skardi-cli
cargo build --release -p skardi-server

# With embedding support (ONNX, GGUF, Candle, remote embed)
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

## Community

Building an agent on top of Skardi, or want to influence the roadmap above? Join us on [Discord](https://discord.gg/S5YQQPEV2m), file an issue, or open a PR. We read everything.

## License

Apache 2.0 — see [LICENSE](LICENSE).
