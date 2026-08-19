# Dropbox fixture provenance

Two directories, two provenances. Read this before citing any file here
as evidence of what Dropbox puts on the wire.

## `contracts/*.json` — CAPTURED

Five real captures from `GET /v1/actions/<id>` on a self-hosted Open
Connector gateway at commit `a3efa99`, taken during the 2026-08-18 live
pass. Each came back **byte-identical** to the source-derived schema it
replaced, which is why the `fingerprint:` pins in `dropbox.yaml` matched
live discovery unchanged. `list_folder_continue.json` being identical to
`list_folder.json` (and likewise for the search pair) is a fact about the
wire, not an artifact of derivation.

These are the files `pinned_fingerprints_match_the_committed_contracts`
hashes, so drift in either the schema or the pin fails the suite.

## `*.json` (this directory) — AUTHORED, not captured

Every row fixture is **hand-authored in the executor's normalized shape**
(`mapDropboxMetadata`'s fifteen camelCase keys). None is a redacted live
capture, and none should be read as one — the account used for the live
pass holds personal files, so its pages were never committed. The
placeholder tells: `id:aaaaaaaaaaaaaaaaaaaaaa`, `Redacted Folder`,
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
