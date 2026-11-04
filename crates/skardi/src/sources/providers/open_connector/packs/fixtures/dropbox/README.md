# Dropbox fixture provenance

Two directories, two provenances. Read this before citing any file here
as evidence of what Dropbox puts on the wire.

## `contracts/*.json` — CAPTURED (output schemas only)

The five output schemas were recaptured on 2026-11-01 from
`GET /v1/actions/<id>` on an isolated Open Connector gateway built from
source revision `0723029f7b792b4eabac794d7b17f884cabdcdf5`, using
Node 24.22.1. No provider credentials were configured and no Dropbox
actions were executed. The current fingerprints include the required,
nullable `hasExplicitSharedMembers` metadata field.

These HTTP metadata captures replace the schemas recorded during the
2026-08-18 pass at `a3efa99`; they are not new payload or live-account
evidence. The folder and search continuation schemas remain identical to
their respective opener schemas; all five were requested independently.

These are the files `pinned_fingerprints_match_the_committed_contracts`
hashes, so drift in either the schema or the pin fails the suite.

Note the scope: every file here is an **output** schema, which is all the
`fingerprint:` pins cover (`fingerprint_schema` hashes the output schema
and nothing else). The input half lives one directory down.

## `contracts/inputs/*.json` — TRANSCRIBED shape, not a byte-exact capture

`list_folder_continue.json` and `search_files_continue.json`: the input
schemas of the two continue actions, on the same
`contracts/inputs/` convention the gmail and outlook packs use. They are
what `inputs: cursor_only` claims about the wire — `cursor` the only
property, `required`, `additionalProperties: false` — and the mock
gateway's discovery serves them, so every registration test in
`packs/dropbox.rs` exercises the PASS side of the `cursor_only` input gate
against a committed artifact rather than an inline constant.

Provenance differs from the output captures above, and the difference
matters: the 2026-08-18 probe wrote `data.inputSchema` to
`/tmp/dropbox-probe/` and only the **shape** was carried into the repo, so
these two files are transcriptions of what the pass observed, not saved
captures. Re-capture them byte-for-byte on the next live pass (the runbook
now writes them straight into this directory).

`the_cursor_only_claim_holds_against_the_committed_input_contracts` checks
each one against the table that declares it, so a re-capture that grows a
second `required` key, or drops `cursor`, fails CI. That catches drift **on
re-capture, not live** — a registration-time input fingerprint is tracked
engine work, exactly as documented for gmail and outlook.

## `*.json` (this directory) — AUTHORED, not captured

Every row fixture is **hand-authored using the earlier normalized
metadata shape**. These payloads were not recaptured for the 2026-11-01
schema update and do not provide live evidence for the new
`hasExplicitSharedMembers` field. None is a redacted live capture: the
account used for the 2026-08-18 pass holds personal files, so its pages
were never committed. The placeholders make their authored origin clear: `id:aaaaaaaaaaaaaaaaaaaaaa`, `Redacted Folder`,
`AAGxYzRedactedCursorValue`.

What each one is for, and how far it can be trusted:

| File | Shape | Provenance |
|---|---|---|
| `files.json` | a folder row plus a file row, with nested `sharingInfo` and an undeclared extra key | authored; **corrected to the live shape** — the keys the live pass found `list_folder` never populates are null here, as the wire has them |
| `files_empty.json` | zero entries, `hasMore: false` | authored |
| `files_type_mismatch.json` | a valid row followed by `sizeBytes: "not-a-number"` | authored, and deliberately **impossible**: it encodes a shape the captured contract forbids, to pin the converter's error identity |
| `shared_links.json` | one link row with the link-only columns populated | authored; **corrected to the live shape** — the four keys the live pass removed as unpopulatable are null here |
| `file_search.json` | two matches, `filename` and `content` | authored. `matchType: "content"` is the one value the live pass never observed (Dropbox content indexing did not land during the run); it is structurally reachable, not confirmed |
| `file_search_null_parent.json` | a match with `metadata: null` | authored, and deliberately **impossible**: the captured contract declares `matches[].metadata` a required non-nullable object. Pins that the converter fails and names the non-nullable column rather than emitting a quiet all-NULL row |

Re-deriving the four non-impossible fixtures from redacted live captures
is open work; it is tracked in
`docs/superpowers/plans/2026-08-18-dropbox-live-evaluation.md`. Until
then these files exercise the mapper, and only the mapper — the wire
evidence lives in `contracts/` and in the module-doc provenance banner in
`packs/dropbox.rs`.
