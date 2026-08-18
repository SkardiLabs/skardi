# Dropbox pack — personal-account live evaluation

**Runbook for phases 2 + 4/5 of the source-pack skill, instantiated for
`packs/dropbox.{yaml,rs}` on `feature/open-connector-dropbox-pack`.**

Generic method: `docs/superpowers/skills/source-pack/references/live-verification.md`.
This document is the Dropbox-specific version — exact actions, exact
inputs, exact SQL, and the specific claims on this branch that only a
live wire can settle.

## 0. Why this run exists

The pack was authored from source-reading alone. The execution plan
(`specs/2026-08-16-dropbox-source-pack-design.md`, steps 2 and 5) marks
contract capture and live verification **blocked** on two things this box
did not have: a Node runtime and a Dropbox account. Everything downstream
of that block is provisional:

| Artifact | Current state | What this run turns it into |
|---|---|---|
| `fingerprint:` on 3 tables, `continuation.fingerprint:` on 2 | hashes of source-derived schemas; labelled as such, and expected to fail the gate live | hashes of `data.outputSchema` from a live gateway |
| `fixtures/dropbox/contracts/*.json` (5 files) | derived from the executor source; each `*_continue.json` is byte-identical to its opener *because it was derived that way* | five independent captures, which either confirm the identity or split it |
| `fixtures/dropbox/*.json` (6 row fixtures) | authored in the executor's shape (`id:aaaa…`, `Redacted Folder`) | redacted live captures |
| Every mapped column | never observed carrying a real value | non-NULL somewhere in a seeded account, or dropped |
| `limit: 2000` / `maxResults: 1000` | the schemas' declared maxima, unprobed | probed at the boundary (a declared cap can exceed the wire's) |
| Q1 — `{path, cursor}` together on `list_shared_links` | shipped as an inference, labelled as one | answered; if rejected, `shared_links` gains a `cursor_only` continuation (no code change) |
| Q2 — `list_folder` cursor lifetime | unknown | observed, and documented as a bound if it matters |
| Q3 — can `matches[].metadata` be null? | contract says no, so `file_search.tag`/`name` are non-nullable and the fixture that tests it is labelled synthetic | answered; if yes, those columns become nullable and the fixture graduates |
| `cursor_only` on the two continue actions | gated at registration against the discovered **input** schema, but only ever seen against a derived schema | gated against the real one |

Read that table as the acceptance criteria. The run is done when every
row's middle column is gone. The claims themselves are already labelled
honestly in the pack (module-doc provenance banner, yaml headers) — this
run is what removes the labels.

## 1. Prerequisites

**On this box.**

```bash
# Node — the gateway is a Node/TypeScript service; `node`/`npm` are absent here.
curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.1/install.sh | bash
. ~/.nvm/nvm.sh && nvm install --lts && node --version   # v20+ is enough

git clone https://github.com/oomol-lab/open-connector ~/code/open-connector
cd ~/code/open-connector && git checkout v1.3.5 && npm ci
```

Local constraints that have cost time before: **port 18080 is already
taken** by a running dashboard — use 8087 for `skardi-server` and 3000
for the gateway. Root fs is 29 GB and cargo builds are 3–5 GB; run one
cargo invocation at a time and `cargo clean` when done.

**Dropbox side (you do this, not the agent).** A free personal account is
fine and explicitly acceptable for this gate.

1. https://www.dropbox.com/developers/apps → **Create app** → *Scoped
   access* → *Full Dropbox* (App-folder access hides everything outside
   the app folder, which makes `files` and `file_search` untestable).
2. **Permissions** tab → tick `files.metadata.read` and `sharing.read`
   → **Submit**. These are the only two scopes the whole pack needs; if
   anything later demands a third, that is a finding.
3. **Settings** tab → add the gateway's OAuth redirect URI
   (`http://localhost:3000/oauth/callback` unless your gateway config
   says otherwise) → copy the **App key** and **App secret**.

```bash
export DROPBOX_APP_KEY=…        # your shell only; never into a file, never into chat
export DROPBOX_APP_SECRET=…
export OOMOL_CONNECT_ADMIN_TOKEN=…   # admin routes 401 without it
export OPEN_CONNECTOR_TOKEN=…        # runtime token, the only secret Skardi sees
export GATEWAY=http://localhost:3000
```

> Every permission change invalidates the grant snapshot. If you tick a
> scope later, **re-run the authorization** — do not debug a stale token.

## 2. Bring the gateway up and connect Dropbox

```bash
cd ~/code/open-connector && npm run dev &        # or the repo's documented start script
curl -fsS "$GATEWAY/v1/health" -H "Authorization: Bearer $OPEN_CONNECTOR_TOKEN"
```

Confirm the five actions this pack needs actually exist on this build —
a too-old gateway fails Skardi registration with `action
'dropbox.list_folder_continue' was not found`, which reads like a typo
and means "upgrade the gateway":

```bash
for a in dropbox.list_folder dropbox.list_folder_continue \
         dropbox.list_shared_links dropbox.search_files dropbox.search_files_continue; do
  printf '%-34s %s\n' "$a" \
    "$(curl -s -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $OPEN_CONNECTOR_TOKEN" "$GATEWAY/v1/actions/$a")"
done   # expect 200 five times
```

OAuth setup — field names come from `src/providers/dropbox/definition.ts`
in the checkout; verify them there rather than trusting this snippet:

```bash
curl -X PUT "$GATEWAY/api/oauth/configs/dropbox" \
  -H "Authorization: Bearer $OOMOL_CONNECT_ADMIN_TOKEN" -H 'content-type: application/json' \
  -d "{\"clientId\":\"$DROPBOX_APP_KEY\",\"clientSecret\":\"$DROPBOX_APP_SECRET\"}"

curl -X POST "$GATEWAY/api/oauth/authorizations" \
  -H "Authorization: Bearer $OOMOL_CONNECT_ADMIN_TOKEN" -H 'content-type: application/json' \
  -d '{"service":"dropbox"}'
# → open the returned authorizationUrl in YOUR browser and approve.
```

Two failure modes worth recognizing on sight:

- **Sub-100 ms OAuth failure with no resolved address** — the gateway's
  SSRF/egress guard rejected a reserved IP. Zero-trust VPNs that map
  domains into `198.18.0.0/15` (aTrust, Clash TUN) cause this. Fix on
  your side; do not patch the guard.
- **Dropbox refuses the authorize request** — the gateway may request
  the union of every `dropbox.*` action's scopes, including write
  scopes. If so, narrow the scope list in
  `src/providers/dropbox/definition.ts` as a clearly-marked local patch
  and file it upstream (precedent: oomol-lab/open-connector#267).

## 3. Seed the account so every column can be non-NULL

An all-NULL column over data you *know* exists is the always-NULL bug.
Seed deliberately — this table is the shopping list, one row per thing
the pack claims:

| Seed | Proves |
|---|---|
| A folder at the root | `tag='folder'`, and that `size_bytes`/`is_downloadable`/`server_modified`/`rev`/`content_hash` come back **SQL NULL**, not zero values |
| Two ordinary files with real content | `size_bytes`, `content_hash`, `rev`, `client_modified`, `server_modified`, `is_downloadable` |
| One **empty** file (0 bytes) | `size_bytes = 0` is distinguishable from NULL |
| A file **nested two levels deep** | `recursive: true` actually recurses (the pack's most consequential pin) |
| A file inside a **shared** folder | `sharing_info` non-NULL |
| A shared link on a file, and one on a folder | `shared_links.url`, `link_permissions`, and `tag='folder'` on a link row |
| A shared link with an **expiry**, if your tier allows it | `expires_at`. Link expiry is a paid Dropbox feature — if you cannot set one, record `expires_at` as **unverified** rather than claiming coverage |
| A file whose **name** matches a query word | `file_search.match_type='filename'` |
| A file whose **content** matches that word | `match_type='content'`. Content indexing lags upload by minutes and is tier-dependent; if it never appears, record it unverified |
| A **mounted** shared folder from another account, if you can | `includeMountedFolders: true` is a real pin, not decoration |

Do **not** seed thousands of files to force pagination. §5 does that by
lowering the page size instead.

## 4. Probe every action directly, before Skardi is involved

Contract probes bypass Skardi entirely, which is the point: they
separate "the pack is wrong" from "the gateway is wrong". Record every
response — these become the fixture sources in §7.

```bash
mkdir -p /tmp/dropbox-probe && cd /tmp/dropbox-probe
ex() { curl -s -X POST "$GATEWAY/v1/actions/$1" \
        -H "Authorization: Bearer $OPEN_CONNECTOR_TOKEN" -H 'content-type: application/json' \
        -d "{\"input\":$2}"; }   # note: NO /execute suffix; body is {"input": …}
```

> `open_connector_scan` is **not** an option for this step. Raw scans are
> default-deny and additionally require `execution.readOnly` in the
> action metadata, which Open Connector does not publish — every raw scan
> against a real gateway is refused at planning time. Raw wire shapes
> come from `curl`, not from SQL.

### 4a. `files` — the pack's exact page-one input

```bash
ex dropbox.list_folder '{"path":"","recursive":true,"includeMountedFolders":true,"includeDeleted":false,"limit":2000}' | tee files_p1.json | head -c 2000
```

- **The root path is `""`, not `"/"`.** The pack makes `path` an
  *optional* resource, so with no binding resource the input carries no
  `path` key at all — confirm the gateway defaults that to the root
  rather than 400-ing. If it does not, `path` must become required.
- Record `sorted(keys)` of a few entries and diff both directions
  against the twelve mapped columns (§6).
- **Boundary:** re-send with `limit: 2000` (already above) and then
  `limit: 2001`. The declared max is not always the wire max — Feishu
  declared 100 and hard-failed above 50. If 2000 is rejected, the pack's
  `page_size` is wrong.

### 4b. The split-action claim — the reason this pack needed an engine change

```bash
# The claim the module doc states as fact. Expect 400 invalid_input.
ex dropbox.list_folder '{"cursor":"abc"}'

# Force page 2 with a tiny page size, then continue.
ex dropbox.list_folder '{"path":"","recursive":true,"includeMountedFolders":true,"includeDeleted":false,"limit":2}' > files_small_p1.json
CUR=$(python3 -c "import json;print(json.load(open('files_small_p1.json'))['data']['cursor'])")
ex dropbox.list_folder_continue "{\"cursor\":\"$CUR\"}" > files_small_p2.json

# The cursor_only claim, tested from the other side: anything extra must 400.
ex dropbox.list_folder_continue "{\"cursor\":\"$CUR\",\"limit\":2}"
```

Then **follow the listing to its real end** and inspect the final page:
the pack asserts `list_folder` answers `hasMore: false` beside a
**non-empty** cursor, which is the entire justification for
`has_more_path` being load-bearing here. Confirm it, or the declaration
changes.

### 4c. `shared_links` — Open question 1

```bash
ex dropbox.list_shared_links '{}' > links_p1.json
# THE open question: does this action accept path and cursor together?
ex dropbox.list_shared_links '{"path":"/Some Folder/file.pdf","cursor":"…"}'
```

The pack ships the answer "yes" as a comment marked *verified live*. If
the real answer is no, `shared_links` needs
`continuation: {inputs: cursor_only}` too — a yaml change, no code.
Also confirm this action really nulls its cursor at end-of-collection
(the pack claims it does, unlike `list_folder`), and confirm it truly
has no page-size input.

### 4d. `file_search`

```bash
ex dropbox.search_files '{"query":"report","fileStatus":"active","maxResults":1000}' > search_p1.json
ex dropbox.search_files '{"query":"report","fileStatus":"active","maxResults":2}' > search_small_p1.json
SCUR=$(python3 -c "import json;print(json.load(open('search_small_p1.json'))['data']['cursor'])")
ex dropbox.search_files_continue "{\"cursor\":\"$SCUR\"}"
```

- Check whether `matches[].metadata` can ever be **null** on the wire.
  The pack ships a `file_search_null_parent.json` fixture built on that
  shape while its own captured contract declares `metadata` a required
  non-nullable object — exactly one of the two is right, and only this
  probe says which. It matters: `tag` and `name` are declared
  `nullable: false`, so a legal null parent would fail live scans.
- Check whether `highlightSpans` is `[]` or `null` when highlights are
  not requested (the module doc records this as a wire-vs-contract
  contradiction).

### 4e. Open question 2 — cursor lifetime

Take a `list_folder` cursor, wait ~15 minutes, add and delete a file in
the listed tree, then continue from it. Record whether it survives,
returns `reset`, or errors. A long recursive scan of a real account can
outlive a cursor, and if it can, that belongs in the pack doc as a
documented bound.

## 5. Force real multi-page pagination through Skardi

`files` requests `limit: 2000` and `file_search` requests
`maxResults: 1000`, so a personal account will never reach page 2 on the
shipped constants. Lower them **locally, for the run only**:

```bash
cd ~/code/skardi
git diff --exit-code crates/skardi/src/sources/providers/open_connector/packs/dropbox.yaml  # start clean
sed -i 's/^      page_size: 2000$/      page_size: 2/;s/^      page_size: 1000$/      page_size: 2/' \
  crates/skardi/src/sources/providers/open_connector/packs/dropbox.yaml
```

Two consequences to keep in mind while the edit is in place: the
`complete_collection_pins_ride_every_files_request` and
`files_pages_through_the_continue_action_with_only_a_cursor` tests pin
`limit: 2000` and will fail, and `max_pages` (default 100) now caps the
scan at ~200 rows. **Revert this edit before committing anything** — the
shipped `page_size` must be the value §4a proved at the boundary.

## 6. Register and scan end to end through `skardi-server`

```yaml
# /tmp/dropbox-probe/ctx.yaml
kind: context
metadata: { name: dropbox-live, version: 1.0.0 }
spec:
  data_sources:
    - name: saas
      type: open_connector
      connection_string: http://localhost:3000
      hierarchy_level: catalog
      open_connector:
        runtime_token_env: OPEN_CONNECTOR_TOKEN
        cache_ttl_seconds: 0            # live reads; a cache hit proves nothing here
        bindings:
          - name: me                    # files + shared_links need no resource
            source_pack: dropbox
            tables: [files, shared_links]
          - name: search                # file_search requires `query`
            source_pack: dropbox
            resource: { query: report }
            tables: [file_search]
```

```bash
cargo build -p skardi-server        # NOT skardi-cli: its default `gguf` feature
                                    # pulls llama-cpp-sys, whose bindgen fails on this box
OPEN_CONNECTOR_TOKEN=$OPEN_CONNECTOR_TOKEN RUST_LOG=info \
  ./target/debug/skardi-server --ctx /tmp/dropbox-probe/ctx.yaml --port 8087 2>server.log &

# Ad-hoc SQL goes through the server's own endpoint — no CLI needed.
q() { curl -s -X POST http://localhost:8087/query -H 'content-type: application/json' \
        -d "$(python3 -c 'import json,sys;print(json.dumps({"sql":sys.argv[1],"max_rows":10000}))' "$1")"; }
```

(If your ctx enables auth, `POST /query` needs a session header; the
simplest evaluation ctx leaves auth off.)

**Registration will fail first, and that is the expected first result.**
The pins on this branch are placeholders, so you should see
`action 'dropbox.list_folder' fingerprint mismatch (expected 87502bce…,
discovered <hash>)`. That is the fingerprint gate doing its job. Fix it
properly rather than by pasting the discovered hash blind:

```bash
# 1. Capture the five contracts independently — do NOT copy an opener's file
#    onto its continuation, which is how the current fixtures got identical.
for a in list_folder list_folder_continue list_shared_links search_files search_files_continue; do
  curl -s -H "Authorization: Bearer $OPEN_CONNECTOR_TOKEN" "$GATEWAY/v1/actions/dropbox.$a" \
    | python3 -c 'import json,sys;print(json.dumps(json.load(sys.stdin)["data"]["outputSchema"],indent=2))' \
    > crates/skardi/src/sources/providers/open_connector/packs/fixtures/dropbox/contracts/$a.json
  # keep the INPUT schema too — it is what proves `inputs: cursor_only`
  curl -s -H "Authorization: Bearer $OPEN_CONNECTOR_TOKEN" "$GATEWAY/v1/actions/dropbox.$a" \
    | python3 -c 'import json,sys;print(json.dumps(json.load(sys.stdin)["data"]["inputSchema"],indent=2))' \
    > /tmp/dropbox-probe/input-schema-$a.json
done
diff crates/.../contracts/list_folder.json crates/.../contracts/list_folder_continue.json  # now a FACT either way

# 2. Re-pin from the captures. The sync test prints `pinned X, actual Y`.
cargo test -p skardi --lib packs::dropbox::tests::pinned_fingerprints_match_the_captured_contracts
# paste each `actual` into dropbox.yaml: 3 table `fingerprint:` + 2 `continuation.fingerprint:`
```

Then restart the server and scan. The per-column non-NULL sweep is the
whole point of this step — run each of these through `q '<sql>'`:

```sql
-- files
SELECT count(*) AS rows,
       count(tag) t, count(name) n, count(id) i, count(path_display) pd, count(path_lower) pl,
       count(client_modified) cm, count(server_modified) sm, count(rev) r, count(size_bytes) sz,
       count(is_downloadable) dl, count(content_hash) ch, count(sharing_info) si
FROM saas.me.files;

-- shared_links
SELECT count(*) AS rows,
       count(tag) t, count(name) n, count(url) u, count(id) i, count(path_display) pd,
       count(path_lower) pl, count(expires_at) ea, count(client_modified) cm,
       count(server_modified) sm, count(rev) r, count(size_bytes) sz, count(is_downloadable) dl,
       count(content_hash) ch, count(sharing_info) si, count(link_permissions) lp
FROM saas.me.shared_links;

-- file_search
SELECT count(*) AS rows,
       count(match_type) mt, count(tag) t, count(name) n, count(id) i, count(path_display) pd,
       count(path_lower) pl, count(client_modified) cm, count(server_modified) sm, count(rev) r,
       count(size_bytes) sz, count(is_downloadable) dl, count(content_hash) ch, count(sharing_info) si
FROM saas.search.file_search;
```

Any zero is a finding: either the wire spells the field differently (map
the real spelling) or the field does not exist on that object type (drop
the column). "The contract declared it" is not a defence — the Notion
pack shipped `archived` against a wire that says `is_archived`.

Beyond counts, check values, not just non-NULLness:

```sql
-- recursive:true actually recurses, and timestamps parsed from ISO 8601
SELECT path_display, server_modified, size_bytes FROM saas.me.files
WHERE path_display LIKE '%/%/%' ORDER BY server_modified DESC LIMIT 10;

-- a folder nulls file metadata; an empty file is 0, not NULL
SELECT tag, name, size_bytes, is_downloadable FROM saas.me.files ORDER BY tag, name;

-- match_type carries both spellings if content indexing landed
SELECT match_type, count(*) FROM saas.search.file_search GROUP BY match_type;

-- LIMIT stops the scan early
SELECT name FROM saas.me.files LIMIT 1;
```

Confirm pagination really happened, from the server's own completion
event rather than by inference:

```bash
grep 'Open Connector scan completed' server.log   # pages=2+ on the unlimited scans, pages=1 under LIMIT 1
```

## 7. Re-derive the fixtures as redacted live captures

Replace all six row fixtures with real pages captured in §4, redacted:

- synthetic ids from a deterministic counter so cross-references survive
  (a shared link's `url` tail vs its row's `id`);
- placeholder names/titles; keep Dropbox URL *shapes*
  (`https://www.dropbox.com/scl/fi/<synthetic>/…`) with synthetic tails;
- keep structural enums, `.tag` discriminators, booleans and timestamp
  *spellings* verbatim;
- **audit mechanically**: walk every string leaf against an allowlist
  and eyeball whatever survives — real file names hide in
  `path_display`, `path_lower` **and** URL slugs, three places for the
  same leak;
- **decode one level deeper**: `sharing_info` and `link_permissions` are
  mapped as JSON text — their leaves need the same allowlist;
- ship the audit as an in-repo tripwire test so CI enforces it;
- `files_type_mismatch.json` stays synthetic — it encodes a shape the
  gateway cannot produce. Say so in a comment.

If any real file name reaches a commit, rewriting the branch tip is not
enough — rewrite the history so no reachable commit carries it.

## 8. Land the evidence

Files that must change as a result of this run:

- `packs/dropbox.yaml` — five real fingerprints; `page_size` set to what
  §4a proved at the boundary; Q1's real answer (with or without a
  `continuation` block on `shared_links`); the `page_size`-only-bounds-
  LIMIT-pushdown comment corrected (for a cursor table with no
  `page_size_input`, `page_size` is inert — see `PaginationStrategy::Cursor`).
- `packs/dropbox.rs` — module doc: "verified live" claims either
  substantiated with the date and gateway version or removed; the
  `metadata`-nullability answer from §4d; a live-verification banner in
  the same shape as `docs/open-connector-feishu.md`'s.
- `fixtures/dropbox/contracts/*.json` and `fixtures/dropbox/*.json` — as
  captured and redacted.
- **`docs/open-connector-dropbox.md` — does not exist yet.** Step 4 of
  the execution plan; model it on `docs/open-connector-feishu.md`
  (binding YAML, per-table reference, continuation mechanics, the two
  scopes, rate limits, a live-verified banner).
- `docs/open-connector.md` — add Dropbox to the supported-packs
  paragraph, and document the new `pagination.continuation` block in the
  pagination section; it is a YAML-authored engine feature that no
  user-facing doc mentions.
- `docs/superpowers/specs/2026-07-11-open-connector-integration-tasks.md`
  — the 5.5 entry, ticked only now.
- `docs/superpowers/specs/2026-08-16-dropbox-source-pack-design.md` —
  status is still "no implementation started"; close out steps 2 and 5
  and answer the open questions.
- PR #216 — replace the design-only body with per-table live evidence
  (row counts, which columns carried real values, which pins returned
  rows, pages observed) and flip it out of Draft.
- Any gateway defect found here gets **filed** on oomol-lab/open-connector
  and linked from the pack doc. Findings that live only in a PR body get
  lost.

Then the phase-5 gate: `cargo fmt --all`, `cargo clippy --workspace
--all-targets`, and the **full** `cargo test -p skardi --lib` (the
engine change ripples past the pack filter), with test counts in the
docs re-derived from a fresh run.

Baseline measured on this branch (2026-08-18, after the self-review fix
commit), so you can tell a regression from the inherited state:

- `cargo fmt --all -- --check` — clean.
- `cargo test -p skardi --lib` — **965 passed, 0 failed**, 202 ignored;
  `…--lib sources::providers::open_connector` 324; `…--lib packs::dropbox` 25.
- `cargo clippy -p skardi --all-targets` — **5 errors, none in this
  diff**: `sources/providers/mongo/mod.rs:1329,1349`,
  `sources/providers/sqlx/pg/postgres.rs:1521`,
  `jobs/destination/mod.rs:228`. Pre-existing; the pack's own files are
  clippy-clean. Don't let them mask a new one.

## 9. Teardown

```bash
pkill -f skardi-server; pkill -f 'open-connector'
cargo clean                      # reclaims ~3.5 GB on this 29 GB box
rm -rf /tmp/dropbox-probe        # raw, UNREDACTED captures live here
git checkout crates/skardi/src/sources/providers/open_connector/packs/dropbox.yaml  # if §5's page_size edit survived
```

Revoke the Dropbox app's access token from the account's connected-apps
page, and rotate the app secret if it was ever pasted anywhere but your
own shell.
