# Roadmap

**Coming soon (not yet shipped)**: a skills generator that emits Claude Code skill files per pipeline, an MCP binding for non-Claude hosts, a first-class memory primitive (one YAML block giving an agent a memory store with keyword + semantic recall, automatic expiration, and per-session provenance), lineage capture, and snapshot-as-branch checkpoints (roll back a destructive agent write — e.g. an agent that updated 1,000 rows you don't like — in one call).

We're **building in public**. `[x]` means shipped today, `[ ]` means open for contribution. Open an issue or hop into [Discord](https://discord.gg/S5YQQPEV2m) on anything unchecked.

`1` Federated SQL engine
   - [x] One SQL engine ([DataFusion](https://datafusion.apache.org/), in-process) over CSV, Parquet, S3 / GCS / Azure, Postgres, MySQL, SQLite, MongoDB, Redis, Iceberg, Lance, SeekDB — all joinable in one query
   - [x] Register either one specific table, or point Skardi at a database (Postgres / MySQL / SQLite) and let it auto-discover all tables — one config line either way
   - [ ] Graph database sources (Neo4j / Kuzu) — to unlock graphRAG patterns alongside vector / full-text retrieval

`2` Retrieval primitives
   - [x] Vector search (KNN) — `pg_knn` (pgvector), `sqlite_knn` (sqlite-vec), Lance KNN, SeekDB HNSW
   - [x] Full-text search (FTS) — `pg_fts`, `sqlite_fts`, Lance BM25 inverted indexes, SeekDB FULLTEXT
   - [x] Hybrid search — combine keyword and semantic search results in one SQL query (RRF merge), no Python re-ranking layer
   - [x] Inline embeddings — `candle()` UDF (local GGUF / Candle models, or remote embedding APIs) called inside SQL, so content + vector stay on the same row atomically
   - [x] ONNX inference — `onnx_predict` UDF for inline model predictions in SQL
   - [x] Chunking UDFs — `chunk()` and index-carrying `chunk_parts()` with character / markdown splitters (via [`text-splitter`](https://crates.io/crates/text-splitter)) so ingestion can chunk inline in SQL ([docs](chunk.md)); token / code splitters next
   - [x] JSON encoding UDF — `json_pack()` builds JSON objects in SQL through `serde_json` (deterministic key order, no injection path) ([docs](json_pack.md))
   - [ ] Memory primitive — give your agent a memory store (keyword + semantic recall, TTL/expiration, per-session provenance) defined in one YAML block

`3` Online serving (pipelines)
   - [x] Declarative YAML → parameterized REST endpoint with inferred request / response schema
   - [x] Built-in pipeline dashboard
   - [x] CLI pipeline binding — `skardi run <pipeline> -p name=value` calls any named, server-loaded pipeline directly ([#90](https://github.com/SkardiLabs/skardi/pull/90))
   - [x] CLI as a thin HTTP client — `skardi query` / `skardi run` send ad-hoc SQL and pipeline calls to a running `skardi-server` over the network; federation across sources happens server-side (see [docs/cli.md](cli.md))

`4` Offline jobs
   - [x] Async batch execution with submit / poll / cancel ([#98](https://github.com/SkardiLabs/skardi/pull/98))
   - [x] Lance dataset destinations with atomic commit + crash recovery
   - [x] SQL-DML destinations (Postgres / MySQL / SQLite)
   - [x] SQLite-backed run ledger with submit-time schema diff

`5` Agent-facing bindings
   - [x] REST — every pipeline served as a parameterized HTTP endpoint
   - [x] Shell — every pipeline runnable as a `skardi` command; works in Claude Code, Cursor, and any agent with a Bash tool
   - [ ] Skills generator — `skardi skills generate --server <URL> --out .claude/skills/` emits a skill Markdown per pipeline for Claude Code / Desktop auto-discovery
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
