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
(byte order), then by position within the note. The tables declare that order
to the planner, so `ORDER BY path` (`from_path` on `links`) plans no extra sort.

### `notes` — one row per `.md` file

| column | type | notes |
|---|---|---|
| `path` | Utf8 | vault-relative, `/`-separated, e.g. `Projects/Design.md` |
| `name` | Utf8 | file stem, what `[[Name]]` refers to |
| `folder` | Utf8 | parent folder, `""` at the root |
| `body` | Utf8 | Markdown *after* the frontmatter block |
| `frontmatter_json` | Utf8, nullable | YAML frontmatter as a JSON object; `NULL` when absent or invalid |
| `frontmatter_error` | Utf8, nullable | parse error text when the block is present but invalid |
| `aliases` | List<Utf8>, nullable | the `aliases` key, one string or a list of strings; `NULL` when absent, not a string/list, or empty once blanks and non-strings are dropped |
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
  minus symlinks (never followed, never read — see Security). Bytes that are
  not valid UTF-8 are decoded lossily (U+FFFD), never an error. A note skipped
  for size, or unreadable mid-scan, is still in the listing and so still a
  valid link *target*: `links.to_path` can name a path with no `notes` row.
- **Frontmatter:** a `---` block starting on line 1, closed by `---` or `...`.
  Invalid YAML or a non-mapping document keeps the note and fills
  `frontmatter_error`. `tags` (or `tag`) may be a list or a comma/space
  separated string; exactly one leading `#` is stripped and a tag that is only
  digits is dropped, as in the body. `[[wikilinks]]` inside string values are
  extracted as `source = 'frontmatter'` links with `line = NULL` — **only when
  the value is quoted**. `related: "[[Home]]"` is a link; unquoted
  `related: [[Home]]` is YAML for a nested list holding the string `Home` and
  yields no link and no error (Obsidian applies the same rule). Markdown-style
  `[text](target)` in a property is plain text.
- **Body tags:** `#tag` at the start of a line or after whitespace, letters,
  digits, `_`, `/`, `-` (Unicode letters included); a tag that is only digits
  (`#2026`) is ignored, `#y2026` counts. Not inside code spans or fenced
  blocks. Comments (`%%…%%`, `<!-- … -->`) and inline math are *not*
  excluded — a `#tag` inside them is a row.
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
| `[text](/Folder/Note.md)` | root-relative; the linking note's folder is never tried | `exact` / `missing` |
| `[[]]` / `[[#Heading]]` | the note itself | `exact` |
| `https://…`, `mailto:…`, any `scheme:` | never resolved; `target` keeps the full URL | `external` |

Aliases are **not** resolved. Obsidian only offers aliases in autocomplete;
`[[Alias]]` in a file is a plain `Name` lookup and shows up as `missing`.
That is exactly what makes the alias-repair query below possible.

Matching is case-insensitive on both sides: `[[home]]` finds `Home.md`, and
two files that differ only by case resolve to the later one in listing order.
A colon with no space after it reads as a URL scheme — `[[Note:subtitle]]` is
`external`, `[[Note: subtitle]]` is a note name.

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

## Failure modes

| Situation | Behavior |
|---|---|
| `path` missing, not a directory, or the S3 list fails | Registration fails naming the path; the server does not start. |
| `hierarchy_level` not `catalog`; `access_mode: read_write`; unknown option key; `name` is `datafusion` or `information_schema` | Registration fails naming the field. |
| Malformed or non-mapping frontmatter | Row kept; `frontmatter_json` NULL; `frontmatter_error` set. |
| Invalid UTF-8 | Row kept; lossy decode. |
| Note larger than `max_file_bytes` | Skipped before it is read (the size comes from the listing); `warn` with path and size. The one case that drops a row. |
| Symlinked file or directory inside the vault | Skipped at listing time; `warn` with path. |
| Listed file replaced by a symlink before it is read | Open refused (`O_NOFOLLOW`); counted as a read failure. |
| Some notes unreadable mid-scan | Skipped with a `warn` naming path and cause; the rest are returned. |
| Every attempted read fails (permissions, S3 `List` without `Get`, expired credentials) | The query fails naming the root, the attempted count and the first failure — never an empty result. Size-cap and symlink skips are not attempts. |
| No `.md` listed (empty vault, everything excluded or oversized) | Three empty tables; no error. |
| Root gone or unreadable between registration and a query | The query fails naming the root. |
| Link to a file that exists only with different case | Resolves (`exact` / `name`); matching is case-insensitive. |

Errors name paths, never note contents.

## Security

- Symlinks under the root are never followed. Listing skips them, and each
  read opens the file with `O_NOFOLLOW`, so a *file* swapped for a symlink
  between listing and reading is refused rather than followed. `O_NOFOLLOW`
  guards the final path component only: a *directory* on the path swapped for
  a symlink after listing would still be traversed — the residual window on
  Unix.
- The vault root itself may be a symlink. It is operator configuration, not
  vault content, so registration follows it.
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

Notes by tag:

```sql
SELECT n.path, n.name
FROM vault.main.notes n
JOIN vault.main.tags t ON t.path = n.path
WHERE t.tag = 'project/skardi'
ORDER BY n.path;
```

Dangling links — targets that resolve to nothing:

```sql
SELECT from_path, target, line
FROM vault.main.links
WHERE resolution = 'missing'
ORDER BY from_path, line;
```

External sites referenced, most-cited first:

```sql
SELECT split_part(split_part(target, '://', 2), '/', 1) AS host, count(*) AS n
FROM vault.main.links
WHERE kind = 'external' AND target LIKE '%://%'
GROUP BY 1
ORDER BY n DESC, host;
```
