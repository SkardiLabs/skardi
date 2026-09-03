# Obsidian vault source

Query an [Obsidian](https://obsidian.md) vault with SQL. Skardi reads the
Markdown files directly — no plugin, no sync service, no cache — and exposes
the vault as three tables: every note, every link (resolved the way Obsidian
resolves it), and every tag.

## Enabling

The provider is behind a Cargo feature so the default build does not carry
its dependencies (`pulldown-cmark`, `glob`, `object_store`):

```bash
cargo build -p skardi-server --features obsidian
```

## Configuration

```yaml
data_sources:
  - name: vault                       # becomes the catalog name
    type: obsidian
    path: /Users/me/Notes             # vault root; s3://bucket/prefix also works
    hierarchy_level: catalog          # required — tables live under vault.main.*
    options:                          # all optional
      exclude_globs: ".obsidian/**,.trash/**"   # comma-separated; default shown
      max_file_bytes: "16777216"                # default 16 MiB; larger notes are skipped with a warning
```

Rules enforced at registration (the server refuses to start otherwise):

- `hierarchy_level` must be `catalog`; the source always owns the whole
  `<name>.main` namespace.
- `access_mode` must be read-only. Obsidian sources cannot be job destinations.
- `path` must exist and be a directory (for `s3://`, one non-recursive list must succeed).
- Unknown option keys are rejected by name. `max_file_bytes` must be a positive integer.
- `name` cannot be `datafusion` or `information_schema`, and the `table` /
  `schema` / `database` options are not accepted.

Nothing is parsed at registration: a vault that is unreadable at query time
fails the query, not the server.

## Tables

All three live under `<name>.main`. Rows are ordered by note path
(byte order), then by position within the note.

### `notes` — one row per `.md` file

| column | type | notes |
|---|---|---|
| `path` | Utf8 | vault-relative, `/`-separated, e.g. `Projects/Design.md` |
| `name` | Utf8 | file stem, what `[[Name]]` refers to |
| `folder` | Utf8 | parent folder, `""` at the root |
| `body` | Utf8 | Markdown *after* the frontmatter block |
| `frontmatter_json` | Utf8, nullable | YAML frontmatter as a JSON object; `NULL` when absent or invalid |
| `frontmatter_error` | Utf8, nullable | parse error text when the block is present but invalid |
| `aliases` | List<Utf8>, nullable | the `aliases` key, one string or a list; `NULL` when absent |
| `size_bytes` | Int64 | file size |
| `modified_at` | Timestamp(ms, UTC) | file mtime |

### `links` — one row per link, resolved

| column | type | notes |
|---|---|---|
| `from_path` | Utf8 | note containing the link |
| `to_path` | Utf8, nullable | resolved target path; `NULL` for `missing`, `ambiguous`, `external` |
| `target` | Utf8 | the link text as written, before resolution (full URL for externals) |
| `kind` | Utf8 | `wikilink`, `embed`, `markdown`, `external` |
| `display_text` | Utf8, nullable | `[[X\|text]]`, `[text](x)`, image alt; `NULL` for autolinks and bare wikilinks |
| `heading` | Utf8, nullable | `[[Note#Heading]]` → `Heading` |
| `block_id` | Utf8, nullable | `[[Note#^abc]]` → `abc` |
| `resolution` | Utf8 | `exact`, `name`, `ambiguous`, `missing`, `external` |
| `source` | Utf8 | `body` or `frontmatter` |
| `line` | Int32, nullable | 1-based source line; `NULL` for frontmatter links |

### `tags` — one row per (note, tag), deduplicated per source

| column | type | notes |
|---|---|---|
| `path` | Utf8 | |
| `tag` | Utf8 | without `#`, nested tags keep their `/` (`project/skardi`) |
| `source` | Utf8 | `body` or `frontmatter` |

Every schema carries the metadata key `skardi.obsidian.surface_version`
(currently `1`); it changes only when a column is renamed, removed or retyped.

## What gets parsed

- **Files:** every `*.md` under the root, minus `exclude_globs`
  (case-insensitive, `**` crosses folders), minus files over `max_file_bytes`,
  minus symlinks (never followed, never read — see Security).
- **Frontmatter:** a `---` block starting on line 1, closed by `---` or `...`.
  Invalid YAML or a non-mapping document keeps the note and fills
  `frontmatter_error`. `tags` (or `tag`) may be a list or a comma/space
  separated string; a leading `#` is stripped. `[[wikilinks]]` inside string
  values are extracted as `source = 'frontmatter'` links with `line = NULL`.
- **Body tags:** `#tag` at the start of a line or after whitespace, letters,
  digits, `_`, `/`, `-` (Unicode letters included). Not inside code spans or
  fenced blocks.
- **Body links:** `[[wikilinks]]` and `![[embeds]]` by regex over the text
  with code masked out; `[text](target)`, `![alt](target)`, `<autolinks>` via
  pulldown-cmark. Targets are percent-decoded and split at `#` into path,
  heading, and `^block`.

## How links resolve

Skardi mirrors Obsidian's rules, not a general file resolver:

| link as written | rule | `resolution` |
|---|---|---|
| `[[Name]]`, `[[Name#H]]`, `[[Name\|t]]` | exactly one note named `Name` anywhere → that note | `name` |
| same, several notes share `Name` | none picked | `ambiguous` |
| `[[Folder/Name]]` | root-relative exact path (`.md` optional) | `exact` / `missing` |
| `[[./X]]`, `[[../X]]` | relative to the linking note's folder | `exact` / `missing` |
| `[[Note.md]]`, `[[Note v2.1]]` | tries a root-level exact path first, then falls back to the name rule | `exact` or `name` |
| `[text](Note.md)`, `[text](../Note.md)` | relative to the linking note (Markdown semantics), then name fallback | `exact` / `name` / `missing` |
| `[text](/Folder/Note.md)` | root-relative | `exact` / `missing` |
| `[[]]` / `[[#Heading]]` | the note itself | `exact` |
| `https://…`, `mailto:…`, any `scheme:` | never resolved; `target` keeps the full URL | `external` |

Aliases are **not** resolved. Obsidian only offers aliases in autocomplete;
`[[Alias]]` in a file is a plain `Name` lookup and shows up as `missing`.
That is exactly what makes the alias-repair query below possible.

## Cost model

Every query scans the whole vault: list, read, parse. There is no cache, so
edits are visible on the next query and nothing needs reloading. A query that
touches two tables scans twice. For a few thousand notes this is tens of
milliseconds; for an `s3://` root every query is one `LIST` plus one `GET`
per note — budget the egress accordingly.

Notes larger than `max_file_bytes` are skipped with a `warn` log naming the
path. A vault where *every* read fails (permissions, a mounted drive that went
away) fails the query with the first cause instead of silently returning zero
rows.

## Security

- Symlinks under the root are never followed. Listing skips them, and each
  read opens the file with `O_NOFOLLOW`, so a symlink swapped in between
  listing and reading is refused rather than followed.
- On non-Unix targets the no-follow open is approximated by a `symlink_metadata`
  check before the read, which leaves a small race window. Run on Unix if that
  matters.
- Nothing is written, ever. `access_mode: read_write` is a startup error.

## Example queries

Most-linked notes:

```sql
SELECT to_path, count(*) AS n
FROM vault.main.links
WHERE to_path IS NOT NULL
GROUP BY to_path
ORDER BY n DESC, to_path
LIMIT 10;
```

Orphans — notes nothing links to:

```sql
SELECT n.path
FROM vault.main.notes n
LEFT JOIN vault.main.links l ON l.to_path = n.path
WHERE l.to_path IS NULL
ORDER BY n.path;
```

Alias repair — broken links that match another note's alias:

```sql
SELECT l.from_path, l.target, a.path AS probably_meant
FROM vault.main.links l
JOIN (
  SELECT path, unnest(aliases) AS alias
  FROM vault.main.notes
  WHERE aliases IS NOT NULL
) a ON a.alias = l.target
WHERE l.resolution = 'missing'
ORDER BY l.from_path;
```

Frontmatter fields as columns, using the server's built-in JSON getters
(see [`docs/graph.md`](graph.md#json-getters-without-the-operator-rewrite) —
the same `json_get_str` family the graph source uses for `properties`):

```sql
SELECT path,
       json_get_str(frontmatter_json, 'status') AS status
FROM vault.main.notes
WHERE frontmatter_json IS NOT NULL;
```

Tags per folder:

```sql
SELECT n.folder, t.tag, count(*) AS notes
FROM vault.main.tags t
JOIN vault.main.notes n ON n.path = t.path
GROUP BY n.folder, t.tag
ORDER BY notes DESC;
```
