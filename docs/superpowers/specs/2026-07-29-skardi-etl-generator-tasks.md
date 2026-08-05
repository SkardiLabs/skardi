# Tasks: skardi-etl-generator

Design: [skardi-cloud/design_docs/skardi_etl_generator.md](https://github.com/SkardiLabs/skardi-cloud/blob/main/design_docs/skardi_etl_generator.md)
PRD: [skardi-cloud/design_docs/skardi_etl_generator_prd.md](https://github.com/SkardiLabs/skardi-cloud/blob/main/design_docs/skardi_etl_generator_prd.md)

The generator compiles one `kind: etl` config (source pack × target
format × destination engine) into a validated, deterministic bundle:
destination DDL, ingest jobs, search/read pipelines, a ctx fragment, and
a README. Implementation lives in this repo (`crates/skardi/src/etl/` +
the `crates/skardi-etl` binary); the design and PRD live in skardi-cloud
and are the normative reference — every decision recorded there
(rebuild-first refresh, no-PK `doc_id`, rowid-keyed vec0 mirror, the
`skardi-etl` standalone binary, SELECT-order ≡ DDL-order as a generator
invariant) binds these tasks.

Milestones are one reviewable PR each, per the Open Connector rollout
precedent. Each milestone ticks its entries here with a verification
blurb and counted tests.

Legend: `[x]` done and merged/in PR · `[~]` in progress · `[ ]` not started

---

## Milestone 1 — Core + hybrid-search on SQLite (PR: feature/etl-generator-m1)

Proves the whole architecture: recipes → format plan → dialect render →
validate → atomic write, with the first automated hybrid-path e2e in the
repo. Three layers, in dependency order.

### 1a. Runtime prerequisites (library-wide, independently testable)

- [x] 1a.1 `chunk_parts(mode, text, size [, overlap])` UDF →
      `List<Struct<chunk_idx Int32, chunk_text Utf8>>`. The ONLY sound
      stable-ordinal path: datafusion-sql 52.5.0 rejects
      `UNNEST … WITH ORDINALITY` (`not_impl_err`; apache/datafusion#11419
      open, no released DF implements it), and `ROW_NUMBER() OVER (ORDER
      BY 1)` renumbers across plan changes. Same literal-args contract,
      argument decoding, and registry pattern as `chunk()`
      (`model/chunking/mod.rs`); registered alongside it. Verification
      MUST include a plannability test that goes through SQL — `SELECT …
      FROM t, UNNEST(chunk_parts(…))` with struct-field access — because
      the exact unnest spelling this proves out is what the generator's
      job SQL templates will emit (the design defers that spelling to
      this task on purpose).
- [x] 1a.2 `json_pack(key, value [, key, value …])` UDF → Utf8 (a JSON
      object). The only SQL-callable JSON *encoder* (DataFusion core has
      none through 54.x; `datafusion-functions-json` is read-side only
      and not a dependency). Keys are Utf8 literals; values accept the
      JSON scalar set + NULL; string values are JSON-escaped by
      serde_json — this UDF is the injection boundary the design's
      Security Model leans on, so escaping-adversarial tests (quotes,
      backslashes, control chars, non-BMP) are the core of its suite.
      Odd argument counts, non-literal keys, and unsupported value types
      fail with targeted errors.
- [x] 1a.3 `SourcePackRegistry::packs()` iterator (name-sorted,
      deterministic). `builtins()` currently exposes lookup only; the
      recipe contract suite and `skardi-etl recipes` both need
      enumeration.

**1a verification**: 839 lib tests green (`cargo test -p skardi --lib
--features chunking`). The plannability pin
(`sql_chunk_parts_unnest_yields_ordered_indexed_rows`) settles the ingest
templates' unnest spelling: projection-position
`UNNEST(chunk_parts(...)) AS part` in a subquery, `part['chunk_idx']` /
`part['chunk_text']` field access outside — per-source ordinal restart
and overlap-0 reassembly asserted end to end. `json_pack`'s
injection-boundary test round-trips quotes/backslashes/control
chars/non-ASCII byte-exact through a real JSON parser; non-finite floats
refuse. Registered on both server session contexts.

### 1b. The etl library (`crates/skardi/src/etl/`), bottom-up

- [x] 1b.1 `config.rs` — `kind: etl` envelope, strict parsing
      (`deny_unknown_fields` everywhere), cross-field validation:
      `embedding`/`chunking` required for hybrid_search and rejected for
      okf, `overlap < size`, `dimensions > 0`, engine-specific
      destination fields validated per dialect (postgres
      `{host, port, database, user_env, pass_env}` — env var NAMES only;
      sqlite `{extensions_env}`), binding is `catalog.schema`, slug
      source `metadata.name` non-empty.
- [x] 1b.2 `recipe.rs` — recipe loader (same strictness), role typing
      (`id` → non-null column; `content` exactly one, Utf8-family;
      `timestamp` → timestamp type; `title`/`author` nullable),
      `incremental: auto | full` (auto = the timestamp-role column has a
      pack `GtEq` FilterMapping), `metadata` columns exist in the pack.
      Embedded assets: `mock.hybrid_search`, `github.hybrid_search`
      (flagship = `issues`: id=number, content=body, title=title,
      author=author_login, timestamp=updated_at riding the pack's one
      real `GtEq` pushdown; `issue_comments` is deliberately absent —
      its required per-issue `issueNumber` binding resource cannot
      express repo-wide search). Contract suite walks every embedded
      recipe against `SourcePackRegistry::packs()` column-by-column —
      pack drift fails the build.
- [x] 1b.3 `format.rs` — `TargetFormat` trait + `hybrid_search`
      implementation producing engine-neutral plans: the 10-column
      `documents` table (design's exact order — doc_id, source_table,
      source_id, title, content, chunk_index, author, created_at,
      metadata, embedding; `doc_id` deliberately NOT a primary key, DDL
      comment says why), the ingest plan (chunk_parts explode, json_pack
      metadata, `{limit}` retained on incremental, `{since}` on the
      pack timestamp column when incremental), and the pipeline pair
      (`<slug>-search-hybrid` with FR-9 params returning
      source_table/source_id/chunk_index/doc_id per hit +
      read-time `doc_id` dedup; `<slug>-get-document` by
      `(source_table, source_id)` ordered by chunk_index).
- [x] 1b.4 `dialect.rs` + `dialects/sqlite.rs` — `EngineDialect` trait
      (embedding expr, fts/knn call builders taking the FULL per-engine
      argument set, ingest SELECT rendering, ctx fragment, capabilities,
      `validate_ddl`/`apply_ddl`/`reset`). SQLite dialect: fts5
      external-content mirror + vec0 mirror **keyed on
      `documents.rowid`** (vec0 requires an INTEGER key; `documents`
      deliberately has no integer column), sync triggers inserting
      `NEW.rowid`, ranked join-back on rowid, `vec_to_binary` on the
      WRITE side only (sqlite_knn packs the query `List<Float32>`
      itself), `validate_ddl` executes the DDL against a throwaway
      in-memory connection (the strongest check in the matrix),
      `--reset` = `DROP … IF EXISTS` every bundle-owned artifact +
      re-apply; plain re-apply is a no-op (`IF NOT EXISTS` throughout).
**1b.1–1b.4 verification**: 24 `etl::` tests green inside the 855-test
lib suite. Config: the design's normative §6.1 example parses verbatim;
13 table-driven cross-field rejections + 5 unknown-key probes each name
the offending field. Recipes: the contract suite resolves every embedded
recipe against the real registry column-by-column (flagship `issues`
rides the real `updated_at` GtEq pushdown; drift mutations produce
targeted errors). Format: `DOCUMENT_COLUMNS` pins the 10-column order;
`inner_columns()` dedups the id/metadata overlap order-preservingly.
SQLite dialect: DDL declares `rid INTEGER PRIMARY KEY` first then the
ten neutral columns in DDL order, and the ingest SELECT's aliases are
asserted to appear in exactly that order (the positional-INSERT
invariant, lexical form — the plan-check in 1b.6 asserts it on planned
schemas); the ingest uses the pinned projection-position
`UNNEST(chunk_parts(...)) AS part` spelling with `{since}`+`{limit}`
retained; search joins back on `rid`, dedups by `doc_id` at read time,
and never packs the query vector; `validate_ddl` really executes
apply → re-apply → reset → re-apply on an in-memory connection (fts5,
table, triggers), degrading vec0 to shape-only with an explicit warning
when sqlite-vec isn't loadable — and hard-failing if a configured
extension path fails to load.

- [x] 1b.5 `bundle.rs` — `Bundle` as `BTreeMap<RelPath, FileContents>`;
      one slug function feeding every artifact name (job files+names,
      pipeline files+names, ctx keys) with the 6-hex BLAKE3 suffix on
      lossy normalization; atomic write (stage in sibling
      `.etl-tmp-<pid>`, swap via rename, `.etl-bak-<pid>` under
      `--force` only, backup removed last; the between-renames crash
      window is documented and recoverable by one rename).
- [x] 1b.6 `validate.rs` — the four-gate valid-by-construction pipeline:
      (1) loader round-trips through skardi's REAL job/pipeline/ctx
      loaders; (2) plan-check against a synthetic SessionContext (pack
      FieldMappings → MemTables, destination schema, real
      chunk/chunk_parts/json_pack/vec_to_binary registrations, embedding
      stub, dialect UDTF stubs) asserting every statement plans AND the
      ingest SELECT's planned field ORDER equals the destination DDL
      order — the executor preflights by name order-insensitively while
      the write is positional, so this assertion is the generator's own
      invariant, nobody else's; (3) dialect `validate_ddl`; (4)
      debug-build double-render byte-equality.

**1b.5–1b.6 verification**: 37 `etl::` tests green (876 lib tests with
`--features chunking`; 854 default — `validate` and generation are
feature-gated because the plan-check registers the REAL `chunk_parts`).
Bundle: the flagship renders exactly the PRD §6.2 six-file tree;
double-render is byte-identical; `write` refuses a non-empty dir without
`--force`, force-swaps cleanly, and leaves no `.etl-tmp-*`/`.etl-bak-*`
siblings. Slug: identity on conforming names, 6-hex BLAKE3 suffix on any
lossy normalization (`foo_bar` vs `foo-bar` stay distinct; case-only
changes too). Validation: `generate_hybrid(flagship)` passes all four
gates — real `JobDefinition`/`StandardPipeline` loaders round-trip the
YAML against the synthetic context (pack FieldMapping MemTables under
`saas.github_demo`, provider-derived destination schema under
`gh_search.main`, real chunk/chunk_parts/json_pack/vec_to_binary, a
volatile `candle` stub, dialect UDTF stubs), the ingest SELECT's planned
`(name, type)` sequence equals the destination schema exactly, the DDL
executes apply→re-apply→reset→re-apply in memory, and re-rendering is
byte-equal. The order gate provably catches a swapped `source_table`/
`source_id` projection that every other check would miss. Building the
gate flushed out three real type bugs the executor's exact preflight
would have hit at run time — all fixed in the dialect: SQL
`CAST(… AS VARCHAR)`/concat plan as Utf8View (→ `arrow_cast(…, 'Utf8')`),
`chunk_idx` Int32 vs INTEGER-column Int64 (→ `CAST(… AS BIGINT)`), and
fts5's text-typed `doc_rowid` vs the vec arm's Int64 (→ CAST in the fts
CTE). `created_at` is RFC 3339 TEXT on sqlite (no timestamp affinity;
PRAGMA-derived schemas read Utf8), and the ctx fragment now matches the
real `DataSource` model (`path` + `hierarchy_level: catalog`).

### 1c. The `skardi-etl` binary (`crates/skardi-etl`, new crate)

- [x] 1c.1 `generate -f etl.yaml -o out/ [--recipe r.yaml] [--force]`,
      `setup -f out/setup.sql --dest <path> | --dest-env <VAR> | --ctx
      <ctx.yaml> --catalog <name> [--reset]`, `recipes [--pack]
      [--format] [--show <pack> <format>]`. Exit codes 0/1/2; no
      credential-bearing argv (Postgres via env var or ctx lookup —
      enforced in M3 when the dialect lands, the flag surface ships
      now). NOT a `crates/cli` subcommand: that CLI is a pure HTTP
      client since #170 and an offline generator must link the library.

**1c verification**: `crates/skardi-etl` builds as a workspace member
(skardi dep with `features = ["chunking"]`); server and skardi-cli still
build. Live run against the flagship config: `generate` wrote the
six-file tree with the vec0 shape-only warning surfaced; regenerating
without `--force` refused with exit 1; `--force` swapped cleanly leaving
no `.etl-tmp-*`/`.etl-bak-*` siblings; `setup --dest` on a machine
without sqlite-vec failed with the pointed `--extension`/$SQLITE_VEC_PATH
hint (the fts5/table/trigger lifecycle apply→re-apply→reset→re-apply is
unit-tested against a real file DB); `recipes` lists both built-ins and
`--show github hybrid_search` dumps the annotated YAML; a wrong table
errors naming the recipe's actual tables. `--recipe` loads a user file
through the SAME parser and refuses pack/format mismatches
(`generate_hybrid_with`). `setup --reset` derives its DROP list from
setup.sql's own CREATE statements in reverse creation order, so the
command stays bundle-agnostic. Exit codes: 0 success, 1 expected
failure, 2 environment failure.

### 1d. Verification (the M1 gate)

- [x] 1d.1 Golden bundles: `mock × hybrid_search × sqlite` and
      `github(issues) × hybrid_search × sqlite` snapshotted;
      regeneration byte-equality pinned.
- [x] 1d.2 Plan-check self-test: deliberately corrupted SQL (typo'd
      column, wrong column order vs DDL, unplannable construct) MUST
      fail validation — proving the gate can fail, not just pass.
- [x] 1d.3 Mock e2e in CI (the repo's first automated hybrid path):
      generate → `setup` against a real SQLite file → run the generated
      job through the real executor → execute the generated search and
      get-document pipelines → assert RRF-ranked rows and ordered-chunk
      reassembly. Read-time dedup pinned by double-ingesting.
- [x] 1d.4 Full-suite regression zero; `cargo fmt` + clippy clean;
      counted verification blurb recorded here.

**1d verification (the M1 gate)**: 880 lib tests green with
`--features chunking` (854 default; the delta is the feature-gated
chunking + etl validation suites), plus 3 `skardi-etl` bin tests and the
mock e2e — `cargo fmt --all --check` clean, zero clippy warnings in
`etl/` and the new crate. 1d.1: both golden bundles
(`github-issues-search`, `mock-items-search`) checked in under
`src/etl/testdata/golden/` and compared byte-for-byte in BOTH directions
(drift and stale files each fail; `UPDATE_ETL_GOLDEN=1` regenerates);
gate 4 additionally re-renders on every generate. 1d.2: the gate
provably fails — a typo'd column fails the plan naming the column, the
`WITH ORDINALITY` spelling (the exact regression the plannability pin
guards) fails to plan, and a swapped `source_table`/`source_id`
projection — which still plans and still passes the executor's
name-keyed preflight — is caught ONLY by the order assertion. 1d.3:
`tests/etl_mock_e2e.rs` runs generate (vec0 DDL executed for real via
`SQLITE_VEC_PATH`) → atomic write → setup.sql applied + re-applied on a
real SQLite file → the generated job through the REAL `JobExecutor`
twice (double-ingest; ≥5 rows/pass at character/40) → the generated
search pipeline (RRF scores strictly descending, `doc_id`s unique
despite 2× table rows — the read-time dedup pin — and the quantum
document tops a quantum query) → get-document (0-based ordered
chunk_index; whitespace-insensitive overlap-0 reassembly equals the
source text; the splitter trims boundary whitespace). Source = MemTable
with the pack's exact FieldMapping schema; embedding = deterministic
`candle` fake (List<Float32>, text-derived) — the OC read path and real
inference have their own suites. CI (`ci.yml`) installs the sqlite-vec
wheel and exports `SQLITE_VEC_PATH` before the test step, so the e2e
runs (not skips) on every push — the repo's first automated hybrid
path.

## Milestone 2 — OKF format

- [ ] 2.1 `okf` recipes (github: issues, pull_requests, releases; mock)
      with collision-free path templates (owner/repo components
      mandatory for multi-repo bindings; validated at recipe load).
- [ ] 2.2 OKF ingest planning (13-column `okf_documents`) + the two read
      pipelines (`<slug>-list-documents` deduping by
      `(path, source_id)`; `<slug>-get-okf-document` newest-only by
      path).
- [ ] 2.3 `export-okf`: same-`source_id` newest-wins, different-
      `source_id` hard error naming both rows; reserved filenames;
      `index.md`; OKF v0.2 conformance checker module (own unit tests,
      reused by CI).
- [ ] 2.4 MySQL dialect (OKF-only) + capability-refusal messages naming
      the engines that would work.
- [ ] 2.5 OKF golden bundles + conformance CI gate + refusal-matrix
      tests.

## Milestone 3 — Postgres + Lance dialects

- [ ] 3.1 Postgres dialect: tsvector generated column + GIN, pgvector
      `vector(N)` + index, template-static `validate_ddl`, full-arity
      `pg_fts`/`pg_knn` call shapes (metric argument included), gated
      testcontainers e2e (real PG + pgvector).
- [ ] 3.2 Lance dialect: OKF-capable, hybrid_search refuses per FR-5;
      `reset` = guarded dataset removal (path from the bundle's own
      config only, Lance-manifest marker check, prints before delete),
      recreated by the job's `create_if_missing`.
- [ ] 3.3 Capability matrix complete + matrix↔code sync pin test;
      `github × hybrid × postgres` and `github × okf × lance` golden
      bundles.

## Milestone 4 — Polish + docs

- [ ] 4.1 Generated README quality pass: first-contact checklist
      (binding existence, destination extensions, embedding-dimension
      truth), §6.8 rebuild-first refresh guidance, atomic-write recovery
      note.
- [ ] 4.2 `docs/etl-generator.md` user guide (config reference, UC-3
      recipe authoring from `recipes --show`, capability matrix, refresh
      semantics).
- [ ] 4.3 The PRD §8 15-minute manual verification on SQLite and
      Postgres (`{limit}`-bounded backfill, provisioned destination
      excluded), results recorded here.

**Cross-milestone invariants** (each PR's self-review checks them):
valid-by-construction (nothing unvalidated is written), SELECT order ≡
DDL order, deterministic bytes under regeneration, no credentials in
YAML/argv/logs, refusals name the engine + capability + alternatives.
