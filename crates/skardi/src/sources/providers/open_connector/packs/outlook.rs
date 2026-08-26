//! Outlook (Microsoft 365) source pack: `messages` and `mail_folders`
//! over Open Connector's `outlook` service, reconciled against a live
//! gateway (v1.3.4, open-connector at `2410fbe`) on 2026-08-14. Design
//! record: `docs/superpowers/specs/2026-08-14-open-connector-m365-packs-design.md`.
//!
//! **Status: phases 1–4 complete.** The phase-4 live pass ran on
//! 2026-08-19 against the same gateway pin (v1.3.4, `2410fbe`) and a
//! real personal (MSA) mailbox over OAuth: all 22 selected message
//! fields and all 8 folder fields arrived with the pinned spellings
//! and carried non-NULL values through a skardi-server SQL scan; live
//! discovery schemas were byte-identical to the committed contract
//! captures (both halves, both actions); a forced `top=2` walk
//! followed real cursors five pages to a genuinely-null terminal
//! `nextLink`; and `messages.json`/`mail_folders.json` are redacted
//! live captures (enforced by `fixtures_stay_redacted`). Live findings
//! that shape the pack:
//!
//! - **Folder-scoped pagination was broken upstream** — fixed in
//!   open-connector#372 (merged 2026-08-19); tagged releases through
//!   v1.3.5 predate the fix. Graph's continuation URL for a scoped
//!   listing is the OData parenthesized form
//!   `/v1.0/me/mailFolders('{id}')/messages`, and the executor's path
//!   allowlist accepted only the slash form — so against a pre-fix
//!   gateway a `mailFolderId`-scoped scan 400s on its second page
//!   ("nextLink must target Outlook message pagination endpoints"),
//!   refusing the very cursor its previous response returned. Loud,
//!   never silent; folders within one page (`page_size: 100`) are
//!   unaffected, and the whole-mailbox path (`/v1.0/me/messages`)
//!   paginates fine everywhere.
//! - **Wire rows carry unmapped extras**: `@odata.etag` on messages
//!   and `sizeInBytes` on folders, both left unmapped deliberately
//!   (a volatile concurrency token and a constantly-fluctuating
//!   operational metric — neither answers an analytical question).
//!   `wellKnownName` was the third such extra and got PROMOTED to the
//!   `well_known_name` column after the live pass: the live mailbox's
//!   `displayName`s were all CJK, so folder semantics need Graph's
//!   language-independent discriminator (explicit null on custom
//!   folders).
//! - **This mailbox had no hidden folders**: `includeHiddenFolders:
//!   true` was accepted and returned the complete root set, but
//!   on/off returned identical rows, so the pin's effect is
//!   unobservable against this account. `is_hidden` itself converts
//!   fine (false on every live row; the true arm is inline-synthetic).
//!
//! Design decisions, each held by a test:
//!
//! - **One pack per Open Connector service.** There is no
//!   `microsoft365` service upstream: Graph is split into `outlook`
//!   (mail only — no calendar, no contacts), `one_drive`, and `excel`,
//!   each with its own OAuth connection, and a Skardi binding carries
//!   exactly one `connection_alias`. A cross-service pack would
//!   silently span two OAuth grants and fail half its tables at scan
//!   time. `one_drive` ships as its own pack; the whole `excel`
//!   service is deferred at the admission gate (its list actions emit
//!   `nextLink` but accept none, so pagination cannot be completed).
//! - **Rows are RAW Graph passthrough** (`payload.value` untouched,
//!   GitHub-style), and the declared item schema under-declares
//!   heavily: `list_messages` declares 15 properties and **no date
//!   field of any kind**. `receivedDateTime`, `sentDateTime`,
//!   `hasAttachments`, `conversationId`, `parentFolderId`,
//!   `categories`, `internetMessageId` — and the `emailAddress`
//!   nesting under the declared-but-loose `from`/`sender` — all ride
//!   `additionalProperties` passthrough OUTSIDE the fingerprint gate.
//!   The coverage-gap pin (`columns_the_coverage_walker_cannot_resolve
//!   _are_pinned`) keeps that surface an explicitly reviewed set:
//!   thirteen uncovered columns on `messages`, one on `mail_folders`
//!   (`well_known_name`, the deliberate post-live-pass addition).
//! - **`select` is pinned to exactly the mapped fields** on
//!   `messages`. Unpinned, Graph returns its default full
//!   representation — `body.content` (HTML mail) on every row — which
//!   at the declared `top` ceiling would blow the client's 16 MiB
//!   response cap; pinned, payloads shrink by orders of magnitude,
//!   rows become deterministic, and a misspelled passthrough column
//!   turns into a loud Graph 400 instead of an always-NULL column.
//!   Cost: the pin and the column set must move together —
//!   `select_pin_mirrors_the_mapped_columns` enforces that
//!   mechanically, so neither can drift without the other. `body` is
//!   deliberately outside both the columns and the pin (full message
//!   content belongs behind an explicit content surface, not in every
//!   `SELECT *`). `page_size: 100` (not the schema's 1000) keeps even
//!   selected pages far from the response cap. Phase 4 confirmed both
//!   load-bearing assumptions live: `top=1000` is accepted on the
//!   wire (1001 is a schema 400 before credentials — unlike Feishu,
//!   the declared bound IS the wire bound), and a misspelled select
//!   field fails loudly with a Graph 400 naming the bad property.
//! - **The cursor is a URL.** Graph's `@odata.nextLink` is re-exposed
//!   as a `nextLink` input/output pair: `format: uri` is enforced
//!   before credentials, and the executor pins host
//!   `graph.microsoft.com` plus an allowlisted path set. Termination
//!   has exactly ONE spelling on this service: the executor writes
//!   `nextLink` unconditionally (`typeof payload["@odata.nextLink"]
//!   === "string" ? … : null` — not the `readNextLink` helper, which
//!   belongs to `one_drive`/`excel`), so a terminal page is an
//!   explicit `null`, never an omitted key and never `""`. The engine
//!   tolerates all three spellings for providers that use them, all
//!   three pinned together by `pagination.rs`'s
//!   `cursor_ends_on_missing_null_or_empty_next`; both cursor e2es
//!   here therefore terminate on `null`, the only shape this gateway
//!   can produce. Same doctrine covers the cursor's own form: every
//!   fixture and mock cursor is URI-shaped, since an opaque
//!   `"cursor-2"` token
//!   would pass the mocks and 400 against the real gateway, the exact
//!   mock-encoded-wrong-assumption class the original GitHub pack
//!   shipped. The engine sends `top` on continuation requests too;
//!   the executor ignores every input once `nextLink` is present
//!   (Graph embeds `$top`/`$select` in the link), pinned in the
//!   cursor e2e so nobody assumes the page size is re-applied.
//! - **Zero filter pushdown, structurally.** The only filter input is
//!   `filter`, a raw OData *expression* string; a
//!   `FilterMapping` renders one value into one input field and
//!   cannot compose an expression (the Notion pack is the precedent).
//!   Predicates re-apply locally in DataFusion after the bounded
//!   fetch; the practical scoping tools are the `mailFolderId`
//!   resource and `LIMIT` early-stop. `filter`, `orderby`, and
//!   `bodyContentType` never reach the wire — negative-space guarded
//!   by the exact-key-set assertions in every e2e.
//! - **`mailFolderId` is an optional resource** (the verbatim OC input
//!   key): omitted, the table is the whole mailbox
//!   (`/v1.0/me/messages`); bound, it is one folder's listing
//!   (`/v1.0/me/mailFolders/{id}/messages`). Both collections are
//!   well-defined, hence optional rather than required. Live caveat:
//!   scoped listings larger than one page die on the upstream
//!   allowlist defect above until the gateway fix lands.
//! - **`mail_folders` pins `includeHiddenFolders: true`** — the
//!   `state=all` move: Graph hides hidden folders by default, the pin
//!   makes the table the complete root-level set, and `is_hidden`
//!   keeps the distinction queryable. Root-level only: the executor
//!   calls `me/mailFolders` without recursion; nested folders are
//!   revealed by `child_folder_count`, not enumerated. That
//!   collection terminates completely — an honest small collection,
//!   not a truncated large one.
//! - **`error_path: None`**: `assertOutlookResponse` throws on any
//!   non-2xx, so Graph's error envelope never reaches Skardi as an
//!   HTTP-200 body; a scope failure surfaces through the gateway
//!   failure envelope, pinned end to end.
//! - **The registration gate is output-only** (`fingerprint_schema`
//!   hashes the output schema; nothing reads
//!   `ActionMetadata::input_schema`), and these actions are
//!   `additionalProperties: false` strict. This pack ships the input
//!   half of its own capture (`fixtures/outlook/contracts/inputs/`,
//!   same gateway pin, cross-checked byte-identical against
//!   per-action discovery) plus
//!   `generated_inputs_are_accepted_by_the_captured_input_contracts`
//!   — both sides committed artifacts, so it catches drift on
//!   re-capture, not live; the registration-time input fingerprint
//!   remains tracked engine work.
//! - **Excluded actions**: `get_message`, `get_profile`,
//!   `get_mailbox_settings` are single-object reads (no collection to
//!   list); `create_draft`, `update_draft`, `send_draft`,
//!   `send_email`, `reply_email`, `update_mailbox_settings` are
//!   writes, out by the read-only allowlist. That accounts for all
//!   eleven `outlook` actions.
//! - **The OAuth consent is read-write for a read-only pack.** The
//!   service's scope union requests `Mail.ReadWrite` + `Mail.Send` +
//!   `MailboxSettings.ReadWrite`, and the read actions themselves
//!   declare `requiredScopes: [User.Read, Mail.ReadWrite]` although
//!   `Mail.Read` suffices at the Graph level — so the gateway's own
//!   scope check would refuse a correctly-scoped read-only token.
//!   Nothing Skardi-side can narrow the consent screen; the pack doc
//!   says so plainly, the tables stay read-only by construction, and
//!   the misdeclaration is an upstream issue candidate (same class as
//!   open-connector#268).

use std::sync::OnceLock;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use crate::sources::providers::open_connector::source_pack::SourcePack;

use super::loader;

static PACK: OnceLock<Result<SourcePack, String>> = OnceLock::new();

/// The Outlook pack, parsed once from the embedded YAML asset.
pub fn pack() -> Result<&'static SourcePack, OpenConnectorError> {
    loader::builtin("outlook.yaml", include_str!("outlook.yaml"), &PACK)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::hierarchy::HierarchyLevel;
    use crate::sources::providers::open_connector::action_registry::fingerprint_schema;
    use crate::sources::providers::open_connector::json_to_arrow::RowConverter;
    use crate::sources::providers::open_connector::pagination::PaginationStrategy;
    use crate::sources::providers::open_connector::row_path::RowPath;
    use crate::sources::providers::open_connector::source_pack::{FixedValue, SourcePackTable};
    use crate::sources::providers::open_connector::testutil::{
        EnvVarGuard, MockGateway, MockResponse, boolean, collect, column_values, convert_page,
        discovery_ok, envelope_err, envelope_ok, fingerprint_uncovered_columns, input_keys, utf8,
    };
    use crate::sources::providers::open_connector::{
        OpenConnectorConfig, OpenConnectorGateways, register_open_connector_tables,
        register_open_connector_udtfs,
    };
    use arrow::array::{
        Array, BooleanArray, Int64Array, ListArray, StringArray, TimestampMillisecondArray,
    };
    use arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use serde_json::{Value, json};
    use std::collections::BTreeSet;

    /// Every cursor in this module is URI-shaped on purpose: the real
    /// gateway validates `nextLink` as `format: uri` before credentials
    /// and the executor pins its host and path — an opaque token would
    /// pass these mocks and 400 live.
    const PAGE2_URI: &str =
        "https://graph.microsoft.com/v1.0/me/messages?%24top=100&%24skiptoken=RFRM9Page2";
    const FOLDERS_PAGE2_URI: &str =
        "https://graph.microsoft.com/v1.0/me/mailFolders?%24top=1000&%24skiptoken=RFRM9Folders2";

    /// Look up a table by short name; the assets are test-pinned to parse.
    fn table(short: &str) -> &'static SourcePackTable {
        pack()
            .expect("embedded asset is test-pinned to parse")
            .tables
            .iter()
            .find(|t| t.id.rsplit('.').next() == Some(short))
            .unwrap_or_else(|| panic!("table {short}"))
    }

    /// Discovery serving the captured contracts, so every mock
    /// registration exercises the fingerprint gate's pass side.
    fn outlook_discovery(path: &str) -> MockResponse {
        let output_schema = if path.ends_with("outlook.list_messages") {
            include_str!("fixtures/outlook/contracts/list_messages.json")
        } else if path.ends_with("outlook.list_mail_folders") {
            include_str!("fixtures/outlook/contracts/list_mail_folders.json")
        } else {
            r#"{"type": "object"}"#
        };
        MockResponse::ok(&discovery_ok("{}", output_schema, true, None))
    }

    // ── Contract tests: `messages.json` and `mail_folders.json` are
    // REDACTED LIVE CAPTURES (2026-08-19, MSA mailbox through the
    // pinned gateway) — real envelope keys, real field presence per
    // row, ids/names/free text replaced deterministically, capture
    // timestamps coarsened to the minute. `fixtures_stay_redacted`
    // re-audits them mechanically on every run. Row shapes the live
    // wire does not produce (explicit nulls, hidden folders, the
    // schema-mismatch page) stay inline-synthetic and say so. ─
    //
    // ── The redaction convention, written down because the script that
    // applied it was a one-off: the next re-capture re-derives it from
    // here, and `fixtures_stay_redacted` enforces the mechanical half.
    // Two rules govern every choice below — nothing carrying the
    // mailbox's identity survives, and nothing the tests reason about
    // changes shape.
    //
    // PRESERVED VERBATIM (this is what makes a capture worth more than
    // a hand-written page): key spellings; which keys each row carries
    // (under the select pin every field is present — emptiness is
    // spelled "" / [], and `wellKnownName` is an explicit null on the
    // custom folder, key present); row count and wire order (newest
    // first); the unmapped extra `@odata.etag`; `wellKnownName`,
    // `importance`, `flagStatus` and the other enum-ish values (Graph's
    // vocabulary, not the account's); and Graph's habit of repeating an
    // address as the display name (seven recipient entries here) — a
    // quirk a hand-written page got wrong.
    //
    // REPLACED, deterministically and length-preserving so id handling
    // stays honest: ids keep their real prefix and length and carry the
    // `Synthetic` marker — messages `AQMkADAwATM3SyntheticMessage0001`
    // padded to 144, folders and `parentFolderId` to 112,
    // `conversationId` to 72, `@odata.etag` inside its `W/"…"` wrapper;
    // identities become `Person N` / `personN@example.com`;
    // `internetMessageId` becomes `<synthetic-NNNN@mail.example.com>`
    // (brackets and subdomain kept — real ones have both); free text
    // becomes `Synthetic subject N` / `Synthetic preview N` /
    // `Category A`; `webLink` keeps its host and embeds the row's OWN
    // synthetic id; timestamps are coarsened to the minute with their
    // ordering intact — the live wire sent whole seconds (`…:SSZ`, no
    // fractional part, on all four fields of all nine rows), and the
    // coarsening rewrites only a trailing `:SSZ`, so sub-second digits
    // in a future capture survive redaction rather than being
    // flattened: they carry no identity, and they are the half of
    // RFC 3339 the converter actually has to cope with (that branch is
    // exercised today by the notion/gmail fixtures over the same
    // `parse_timestamp`). Folder `displayName`s were CJK on the live
    // mailbox and became their English equivalents — which is exactly
    // why `well_known_name` is a column.
    //
    // CURSORS: these captures terminate, so `nextLink` is null. A
    // multi-page fixture may be added, but its cursor must be redacted
    // like an id and carry the `Synthetic` marker — the audit admits a
    // `graph.microsoft.com` URL on no other terms, because an
    // as-captured cursor holds live skiptoken state and, in the
    // folder-scoped form, a real folder id.
    //
    // A new placeholder family must be added to the audit's
    // `PLACEHOLDERS` list in the same change, or the audit rejects it.
    // Fault-injection fixtures (`messages_type_mismatch.json`) are
    // NEVER re-captured: the wire does not produce a type-wrong row, so
    // that page stays hand-written and synthetic. ─

    fn convert_fixture(table: &SourcePackTable, fixture: &str) -> RecordBatch {
        let page: Value = serde_json::from_str(fixture).expect("fixture parses");
        convert_page(table, &page)
    }

    #[test]
    fn messages_fixture_converts_the_live_row_shapes() {
        // Redacted live capture: nine rows exactly as the pinned
        // gateway returned them under the select pin (wire order,
        // newest first). What live rows established that the old
        // synthetic page had wrong: under the pin NO field is ever
        // absent or null — emptiness is spelled "" / [] — and the only
        // extra key is @odata.etag (no changeKey, no
        // inferenceClassification, no replyTo).
        let batch = convert_fixture(
            table("messages"),
            include_str!("fixtures/outlook/messages.json"),
        );
        assert_eq!(batch.num_rows(), 9);
        assert!(utf8(&batch, "id").value(0).starts_with("AQMkADAwATM3"));
        assert_eq!(utf8(&batch, "subject").value(0), "Synthetic subject 1");
        // The draft (row 8): subject and recipients are EMPTY on the
        // real wire, never null — "" and [] respectively.
        assert!(boolean(&batch, "is_draft").value(8));
        // EMPTY, not null — the distinction is the point, and `value`
        // alone cannot see it (a null slot also reads as ""), so the
        // validity check carries the claim.
        assert!(!utf8(&batch, "subject").is_null(8));
        assert_eq!(utf8(&batch, "subject").value(8), "");
        assert_eq!(utf8(&batch, "to_recipients").value(8), "[]");
        // Nested scalar paths through the loose from/sender objects.
        // Both pairs assert positively: these four columns sit in the
        // fingerprint coverage gap and the select pin only guards the
        // top-level `from`/`sender` keys, so a broken `emailAddress.*`
        // segment would surface nowhere but here.
        assert_eq!(utf8(&batch, "from_address").value(0), "person1@example.com");
        assert_eq!(utf8(&batch, "from_name").value(0), "Person 1");
        assert_eq!(
            utf8(&batch, "sender_address").value(0),
            "person1@example.com"
        );
        assert_eq!(utf8(&batch, "sender_name").value(0), "Person 1");
        // Recipient lists survive as opaque JSON; live Graph often
        // repeats the address as the display name — shape preserved.
        let cc: Value =
            serde_json::from_str(utf8(&batch, "cc_recipients").value(2)).expect("valid JSON");
        assert_eq!(cc[0]["emailAddress"]["address"], "person3@example.com");
        assert_eq!(cc[0]["emailAddress"]["name"], "person3@example.com");
        // The bcc'd send (row 0) — bcc is only ever visible on the
        // sender's own copy, so the capture holds exactly one.
        let bcc: Value =
            serde_json::from_str(utf8(&batch, "bcc_recipients").value(0)).expect("valid JSON");
        assert_eq!(bcc.as_array().map(Vec::len), Some(1));
        // Booleans and passthrough columns extract on every row.
        assert!(boolean(&batch, "has_attachments").value(5));
        assert!(!boolean(&batch, "is_read").value(2));
        let flag: Value = serde_json::from_str(utf8(&batch, "flag").value(7)).expect("valid JSON");
        assert_eq!(flag["flagStatus"], "flagged");
        let ts: &TimestampMillisecondArray = batch
            .column_by_name("received_date_time")
            .expect("column")
            .as_any()
            .downcast_ref()
            .expect("timestamp");
        assert!((0..9).all(|i| !ts.is_null(i)));
        assert!(ts.value(0) > ts.value(8), "wire order is newest-first");
        // `parentFolderId` rides passthrough (coverage gap, so the
        // fingerprint gate is blind to it) and is the documented join
        // key to `mail_folders.id` — a silent null here answers folder
        // accounting with an empty join rather than an error. Pin a real
        // value, not just distinctness: a null slot reads as "".
        assert!(
            utf8(&batch, "parent_folder_id")
                .value(0)
                .starts_with("AQMkADAwATM3SyntheticFolder")
        );
        assert_eq!(
            (0..9)
                .map(|i| utf8(&batch, "parent_folder_id").value(i))
                .collect::<std::collections::HashSet<_>>()
                .len(),
            4,
            "nine messages across four folders"
        );
        // The reply pair (rows 1 and 7) shares a conversation.
        assert_eq!(
            utf8(&batch, "conversation_id").value(1),
            utf8(&batch, "conversation_id").value(7),
        );
        assert_eq!(
            batch
                .column_by_name("conversation_id")
                .expect("column")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8")
                .iter()
                .flatten()
                .collect::<std::collections::HashSet<_>>()
                .len(),
            8,
            "nine rows, eight conversations"
        );
        // categories: the one categorized row keeps its entry; empty
        // rows are empty lists, not NULL.
        let categories: &ListArray = batch
            .column_by_name("categories")
            .expect("column")
            .as_any()
            .downcast_ref()
            .expect("List column");
        assert_eq!(categories.value(4).len(), 1);
        assert!(!categories.is_null(1));
        assert_eq!(categories.value(1).len(), 0);
        // Self-consistency the redaction must preserve: webLink embeds
        // the row's own (synthetic) id.
        assert!(
            utf8(&batch, "web_link")
                .value(0)
                .ends_with(utf8(&batch, "id").value(0))
        );
    }

    #[test]
    fn mail_folders_fixture_converts_the_live_root_set() {
        // Redacted live capture: the complete root-level set of a real
        // MSA mailbox under the includeHiddenFolders pin — eight
        // well-known folders plus one custom folder (its subfolder is
        // counted, not listed). Every row is visible: this account had
        // no hidden folders, so the is_hidden=true arm lives in the
        // synthetic test below. sizeInBytes rides along unmapped (a
        // deferred candidate, see module doc); well_known_name is
        // mapped, and the wire spells its custom-folder case as an
        // EXPLICIT null (key present), pinned here.
        let batch = convert_fixture(
            table("mail_folders"),
            include_str!("fixtures/outlook/mail_folders.json"),
        );
        assert_eq!(batch.num_rows(), 9);
        assert_eq!(utf8(&batch, "display_name").value(0), "Custom Folder");
        assert_eq!(utf8(&batch, "display_name").value(7), "Inbox");
        // well_known_name: Graph's locale-independent discriminator —
        // a value on every system folder, explicit null on the custom
        // one.
        assert_eq!(utf8(&batch, "well_known_name").value(7), "inbox");
        assert_eq!(utf8(&batch, "well_known_name").value(6), "sentitems");
        assert!(utf8(&batch, "well_known_name").is_null(0));
        let hidden: &BooleanArray = batch
            .column_by_name("is_hidden")
            .expect("column")
            .as_any()
            .downcast_ref()
            .expect("Boolean column");
        assert!((0..9).all(|i| !hidden.value(i)));
        let children: &Int64Array = batch
            .column_by_name("child_folder_count")
            .expect("column")
            .as_any()
            .downcast_ref()
            .expect("Int64 column");
        assert_eq!(children.value(0), 1, "the custom folder's subfolder");
        let counts: &Int64Array = batch
            .column_by_name("total_item_count")
            .expect("column")
            .as_any()
            .downcast_ref()
            .expect("Int64 column");
        assert_eq!(counts.value(7), 5, "the five seeded inbox messages");
        // Root-level listing: one shared parent across all nine rows —
        // the mailbox root, which is itself NOT one of the rows. Pin the
        // id and not just the set size: `StringArray::value` returns ""
        // for a null slot (it checks bounds, not validity), so nine
        // nulls would also collapse to a one-element set and a vanished
        // `parentFolderId` would pass unnoticed. This column has no
        // select pin to lean on either — only `messages` pins select.
        let parents: std::collections::HashSet<_> = (0..9)
            .map(|i| utf8(&batch, "parent_folder_id").value(i))
            .collect();
        assert_eq!(parents.len(), 1);
        let root = utf8(&batch, "parent_folder_id").value(0);
        assert!(root.starts_with("AQMkADAwATM3SyntheticFolder0006"));
        assert_eq!(root.len(), 112);
    }

    #[test]
    fn fixtures_stay_redacted() {
        // Mechanical re-audit of the redaction guarantee, so it is
        // enforced by CI rather than by memory: every string leaf in
        // every row fixture must be ASCII (the live mailbox's real
        // names, folder names, and categories were all CJK — a cheap
        // broad net), every @-bearing value must be an example.com
        // identity, every URL must keep only the allowed provider
        // shapes, every value under an id-bearing key must carry a
        // redaction marker, and any string that itself parses as JSON
        // is decoded and audited one level deeper (the Feishu round-2
        // lesson).
        // Today's fixtures are clean; what this guards is the NEXT
        // re-capture, so each rule is written to fail closed on the
        // shapes a raw capture would actually carry.

        /// BOTH halves of every `@` must be placeholders: the domain
        /// example.com or a subdomain, the local part one of the
        /// capture's placeholder families. Each rule here answers a way
        /// a real identity slips through — `contains("example.com")`
        /// passes `user@example.com.evil.tld` and lets one placeholder
        /// vouch for a real address sharing the same string; checking
        /// only the domain passes a half-redacted `bob.smith@example.com`
        /// (a regex that swaps domains leaves real names behind). Tokens
        /// are cut at the first character no address may hold, so
        /// `<id@mail.example.com>` sheds its brackets.
        fn is_redacted_identity(value: &str) -> bool {
            const PLACEHOLDERS: [&str; 4] = ["person", "synthetic-", "redacted-", "sender"];
            fn is_local_char(c: char) -> bool {
                c.is_ascii_alphanumeric() || "._+-".contains(c)
            }
            fn is_domain_char(c: char) -> bool {
                c.is_ascii_alphanumeric() || c == '.' || c == '-'
            }
            value.match_indices('@').all(|(at, _)| {
                let head = &value[..at];
                let local = &head[head.rfind(|c: char| !is_local_char(c)).map_or(0, |i| i + 1)..];
                let tail = &value[at + 1..];
                let domain = &tail[..tail
                    .find(|c: char| !is_domain_char(c))
                    .unwrap_or(tail.len())];
                (domain == "example.com" || domain.ends_with(".example.com"))
                    && PLACEHOLDERS.iter().any(|prefix| local.starts_with(prefix))
            })
        }

        /// An allowed host is necessary but not sufficient: on every one
        /// of them the interesting half of the URL is live mailbox state.
        /// A captured cursor on `graph.microsoft.com` carries a
        /// `$skiptoken` (and, folder-scoped, a real folder id); the
        /// message `EntryId` behind OWA's `?ItemID=` encodes a persistent
        /// mailbox identifier — the same class of value, on the host a
        /// real `webLink` actually uses.
        /// Matching a host prefix alone would pre-authorize that leak by
        /// passing an as-captured URL unchanged, so every arm must also
        /// carry a redaction marker: a future multi-page or re-captured
        /// fixture stays possible, one that skipped the id remap fails.
        fn is_redacted_url(url: &str) -> bool {
            let host_allowed = url.starts_with("https://outlook.live.com/owa/?ItemID=")
                || url.starts_with("https://outlook.office365.com/owa/?ItemID=")
                || url.starts_with("https://graph.microsoft.com/");
            host_allowed && (url.contains("Synthetic") || url.contains("redacted-"))
        }

        fn audit(value: &Value, path: &str) {
            // The one key-aware rule, in the same spirit as
            // `is_redacted_url` requiring a marker on an allowed host:
            // the value rules below cannot see a raw Graph id — it is
            // ASCII, `@`-free and scheme-free. On `messages` the gap
            // is masked (`webLink` embeds the row's own id, so a
            // forgotten message-id remap trips the URL rule), but
            // `mail_folders` has no URL column, and its ids are
            // persistent mailbox-scoped handles.
            const ID_KEYS: [&str; 4] = ["id", "parentFolderId", "conversationId", "@odata.etag"];
            match value {
                Value::Object(map) => {
                    for (key, child) in map {
                        assert!(key.is_ascii(), "non-ASCII key at {path}: {key:?}");
                        if ID_KEYS.contains(&key.as_str()) {
                            if let Value::String(v) = child {
                                assert!(
                                    v.contains("Synthetic") || v.contains("redacted-"),
                                    "id-bearing value at {path}.{key} carries no redaction marker"
                                );
                            }
                        }
                        audit(child, &format!("{path}.{key}"));
                    }
                }
                Value::Array(items) => {
                    for (i, child) in items.iter().enumerate() {
                        audit(child, &format!("{path}[{i}]"));
                    }
                }
                Value::String(s) => {
                    assert!(s.is_ascii(), "non-ASCII string at {path}: {s:?}");
                    if s.contains('@') {
                        assert!(
                            is_redacted_identity(s),
                            "@-bearing value at {path} is not a redacted identity: {s:?}"
                        );
                    }
                    // Every occurrence, not just a leading one: the
                    // capture carries `bodyPreview` (255 chars of real
                    // mail body), where a link sits mid-sentence — ASCII
                    // and @-free, so nothing else here would catch it.
                    // The token is cut at the first character no URL can
                    // hold, so the marker must sit inside the URL itself
                    // — prose later in the same value cannot vouch for
                    // it.
                    let lowered = s.to_ascii_lowercase();
                    for (offset, _) in lowered.match_indices("http") {
                        let url = &s[offset..];
                        let url = &url[..url
                            .find(|c: char| c.is_whitespace() || c == '"' || c == '<' || c == '>')
                            .unwrap_or(url.len())];
                        assert!(
                            is_redacted_url(url),
                            "URL at {path} outside the allowed shapes: {url:?}"
                        );
                    }
                    if let Ok(nested) = serde_json::from_str::<Value>(s) {
                        if nested.is_object() || nested.is_array() {
                            audit(&nested, &format!("{path}<decoded>"));
                        }
                    }
                }
                _ => {}
            }
        }
        for (name, fixture) in [
            ("messages", include_str!("fixtures/outlook/messages.json")),
            (
                "mail_folders",
                include_str!("fixtures/outlook/mail_folders.json"),
            ),
            (
                "messages_type_mismatch",
                include_str!("fixtures/outlook/messages_type_mismatch.json"),
            ),
        ] {
            audit(
                &serde_json::from_str(fixture).expect("fixture parses"),
                name,
            );
        }
    }

    #[test]
    fn hidden_folder_row_converts() {
        // SYNTHETIC: the live mailbox had no hidden folders (the pin's
        // on/off responses were identical), so the is_hidden=true
        // conversion arm is pinned here rather than by the capture.
        let batch = convert_page(
            table("mail_folders"),
            &json!({"mailFolders": [
                {"id": "f-hidden", "displayName": "Hidden", "isHidden": true},
            ]}),
        );
        assert!(boolean(&batch, "is_hidden").value(0));
    }

    #[test]
    fn null_parent_on_a_nested_path_becomes_sql_null() {
        // SYNTHETIC, and a converter-contract pin rather than a wire
        // shape: the live capture's draft (row 8 of `messages.json`)
        // carries `from` and `sender` like every other row, and under
        // the select pin no field is ever absent or null. But these
        // paths ride `additionalProperties` passthrough, which promises
        // nothing of the sort, so the conversion contract has to hold on
        // its own terms — a nullable column behind a null (or absent)
        // parent becomes SQL NULL, never an error. Deleting this test
        // because "the wire never does that" would drop the only
        // coverage of the admission gate's null-parent category.
        let batch = convert_page(
            table("messages"),
            &json!({"messages": [
                {"id": "m-1", "from": null},
                {"id": "m-2"},
            ]}),
        );
        assert!(utf8(&batch, "from_address").is_null(0));
        assert!(utf8(&batch, "from_name").is_null(0));
        assert!(utf8(&batch, "sender_address").is_null(1));
        assert!(utf8(&batch, "to_recipients").is_null(1));
    }

    #[test]
    fn absent_passthrough_keys_convert_as_pinned() {
        // The passthrough columns sit wholly outside the declared
        // schema, so "key not present" is a legal row shape the select
        // pin can only mitigate live: absent keys are SQL NULL, and the
        // whole row still converts. Row 2 pins the explicit-null arm
        // (SYNTHETIC — the live wire under the select pin never nulls
        // a field, but passthrough offers no such guarantee): null is
        // SQL NULL while "" stays an empty string.
        let batch = convert_page(
            table("messages"),
            &json!({"messages": [
                {"id": "m-1", "subject": "s"},
                {"id": "m-2", "subject": null, "bodyPreview": ""},
            ]}),
        );
        assert!(boolean(&batch, "has_attachments").is_null(0));
        assert!(utf8(&batch, "conversation_id").is_null(0));
        assert!(utf8(&batch, "internet_message_id").is_null(0));
        assert!(
            batch
                .column_by_name("received_date_time")
                .expect("column")
                .is_null(0)
        );
        assert!(utf8(&batch, "subject").is_null(1));
        // The other half of that distinction needs the validity bit:
        // were "" to collapse into SQL NULL, `value` would still hand
        // back "" and this test — the one test for the difference —
        // would keep passing.
        assert!(!utf8(&batch, "body_preview").is_null(1));
        assert_eq!(utf8(&batch, "body_preview").value(1), "");
    }

    #[test]
    fn empty_page_keeps_schema_stable() {
        // Zero rows still yield the full column set — an empty mailbox
        // must DESCRIBE like a populated one.
        let batch = convert_page(table("messages"), &json!({"messages": []}));
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), table("messages").fields.len());
    }

    #[test]
    fn messages_mismatch_fixture_fails_with_the_targeted_error() {
        // Admission-gate schema-mismatch fixture (deliberately synthetic
        // — a live wire cannot be made to produce one): a string where
        // boolean is declared fails with the full row-scoped identity,
        // never a quiet null and never the offending value.
        let page: Value =
            serde_json::from_str(include_str!("fixtures/outlook/messages_type_mismatch.json"))
                .expect("fixture parses");
        let t = table("messages");
        let rows = RowPath::parse(t.row_path)
            .expect("row path")
            .rows(&page, 1)
            .expect("row array");
        let err = RowConverter::new(t.fields)
            .expect("converter")
            .convert(rows, 1)
            .expect_err("a string where boolean is declared must fail conversion");
        match err {
            OpenConnectorError::ConversionFailed {
                column,
                page,
                row,
                found,
                ..
            } => {
                assert_eq!(column, "is_read");
                assert_eq!(page, 1);
                assert_eq!(row, 1, "the valid first row converts");
                assert_eq!(found, "a string");
            }
            other => panic!("expected ConversionFailed, got {other}"),
        }
    }

    #[test]
    fn pinned_fingerprints_match_the_reconciled_contracts() {
        // Pin <-> captured-contract lock through the SAME function
        // registration uses; mismatch output is also how pins are
        // (re)taken after an upstream upgrade.
        let contracts = [
            (
                "messages",
                include_str!("fixtures/outlook/contracts/list_messages.json"),
            ),
            (
                "mail_folders",
                include_str!("fixtures/outlook/contracts/list_mail_folders.json"),
            ),
        ];
        let mut mismatches = Vec::new();
        for (short, contract) in contracts {
            let schema: Value = serde_json::from_str(contract).expect("contract fixture parses");
            let actual = fingerprint_schema(Some(&schema));
            let t = table(short);
            if t.expected_fingerprint != Some(actual.as_str()) {
                mismatches.push(format!(
                    "{}: pinned {:?}, contract fixture hashes to {actual}",
                    t.id, t.expected_fingerprint
                ));
            }
        }
        assert!(mismatches.is_empty(), "{}", mismatches.join("\n"));
    }

    #[test]
    fn generated_inputs_are_accepted_by_the_captured_input_contracts() {
        // The fingerprint gate hashes OUTPUT schemas only, and these
        // actions are `additionalProperties: false` strict — an
        // undeclared key is a hard 400 on every scan. Both sides here
        // are committed artifacts (gateway v1.3.4 capture), so this
        // locks the pack's declarations against the captured input
        // halves and catches drift on re-capture, not live; the
        // registration-time input fingerprint stays tracked engine work.
        for (short, contract) in [
            (
                "messages",
                include_str!("fixtures/outlook/contracts/inputs/list_messages.json"),
            ),
            (
                "mail_folders",
                include_str!("fixtures/outlook/contracts/inputs/list_mail_folders.json"),
            ),
        ] {
            let schema: Value =
                serde_json::from_str(contract).expect("input contract fixture parses");
            let properties = &schema["properties"];
            let t = table(short);

            assert_eq!(
                schema["additionalProperties"],
                json!(false),
                "{short}: the action's input schema is strict"
            );

            let mut generated: Vec<&str> = t
                .required_resources
                .iter()
                .chain(t.optional_resources)
                .copied()
                .collect();
            generated.extend(t.fixed_inputs.iter().map(|(key, _)| *key));
            match t.pagination {
                PaginationStrategy::Cursor {
                    cursor_param,
                    page_size_param,
                    ..
                } => {
                    generated.push(cursor_param);
                    generated.extend(page_size_param);
                }
                _ => panic!("{short}: both tables declare the cursor strategy"),
            }
            for key in &generated {
                assert!(
                    !properties[*key].is_null(),
                    "{short}: `{key}` is not declared by the action's input schema"
                );
            }

            if let Some(required) = schema["required"].as_array() {
                for entry in required {
                    let entry = entry.as_str().expect("required entries are strings");
                    assert!(
                        generated.contains(&entry),
                        "{short}: the action requires `{entry}`, which this table never sends"
                    );
                }
            }

            if let PaginationStrategy::Cursor {
                page_size_param: Some(param),
                page_size,
                ..
            } = t.pagination
            {
                let declared = &properties[param];
                if let Some(minimum) = declared["minimum"].as_u64() {
                    assert!(
                        u64::from(page_size) >= minimum,
                        "{short}: page size {page_size} is below `{param}`'s minimum {minimum}"
                    );
                }
                if let Some(maximum) = declared["maximum"].as_u64() {
                    assert!(
                        u64::from(page_size) <= maximum,
                        "{short}: page size {page_size} exceeds `{param}`'s maximum {maximum}"
                    );
                }
            }
        }
    }

    #[test]
    fn select_pin_mirrors_the_mapped_columns() {
        // The select pin's one cost is that the pin and the column set
        // must move together: a mapped column whose top-level wire key
        // is missing from the pin would be always-NULL by our own hand.
        // This makes the two inseparable — and pins that the list has
        // no strays and no duplicates.
        let t = table("messages");
        let select = t
            .fixed_inputs
            .iter()
            .find_map(|(key, value)| match (key, value) {
                (&"select", FixedValue::StrList(items)) => Some(*items),
                _ => None,
            })
            .expect("messages pins a select StrList");
        let pinned: BTreeSet<&str> = select.iter().copied().collect();
        assert_eq!(select.len(), pinned.len(), "select entries are unique");
        let mapped: BTreeSet<&str> = t
            .fields
            .iter()
            .map(|f| f.path.split('.').next().expect("non-empty path"))
            .collect();
        assert_eq!(
            pinned, mapped,
            "select must be exactly the mapped columns' top-level wire keys"
        );
    }

    #[test]
    fn columns_the_coverage_walker_cannot_resolve_are_pinned() {
        // messages: the walker resolves only the declared top-level
        // properties, so every passthrough column — and the emailAddress
        // nesting under the declared-but-loose from/sender — is outside
        // the gate: drift there surfaces at scan time (or as a Graph 400
        // through the select pin), never at registration. Pinned so the
        // gap stays a reviewed set. mail_folders: seven declared columns
        // plus one deliberate passthrough — well_known_name, added after
        // the live pass because display_name is locale-dependent (no
        // select pin exists on this action, so its only guards are this
        // pin and the live-derived fixture).
        for (short, contract, expected) in [
            (
                "messages",
                include_str!("fixtures/outlook/contracts/list_messages.json"),
                &[
                    "from_address",
                    "from_name",
                    "sender_address",
                    "sender_name",
                    "received_date_time",
                    "sent_date_time",
                    "created_date_time",
                    "last_modified_date_time",
                    "has_attachments",
                    "conversation_id",
                    "parent_folder_id",
                    "categories",
                    "internet_message_id",
                ] as &[&str],
            ),
            (
                "mail_folders",
                include_str!("fixtures/outlook/contracts/list_mail_folders.json"),
                &["well_known_name"],
            ),
        ] {
            let t = table(short);
            assert_eq!(
                fingerprint_uncovered_columns(contract, t.row_path, t.fields),
                expected,
                "fingerprint coverage changed for {short}"
            );
        }
    }

    // ── Integration: the pack against a mock gateway, end to end. ───────

    fn outlook_config(token_env: &str, tables: &str, resource: &str) -> OpenConnectorConfig {
        let resource_line = if resource.is_empty() {
            String::new()
        } else {
            format!("resource: {resource}")
        };
        serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
bindings:
  - name: m365
    source_pack: outlook
    {resource_line}
    tables: [{tables}]
"#
        ))
        .expect("config parses")
    }

    async fn setup_with_gateway(
        gateway: MockGateway,
        token_env: &'static str,
        tables: &str,
        resource: &str,
    ) -> (MockGateway, SessionContext) {
        let _token = EnvVarGuard::set(token_env, "test-token");
        let gateways = OpenConnectorGateways::default();
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&outlook_config(token_env, tables, resource)),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect("gateway registration succeeds");
        register_open_connector_udtfs(&ctx, gateways).expect("UDTF registration succeeds");
        (gateway, ctx)
    }

    fn execute_inputs(gateway: &MockGateway, action_path: &str) -> Vec<Value> {
        gateway
            .requests()
            .into_iter()
            .filter(|r| r.method == "POST" && r.path.ends_with(action_path))
            .map(|r| {
                serde_json::from_str::<Value>(&r.body).expect("request body is JSON")["input"]
                    .clone()
            })
            .collect()
    }

    /// The full select pin, as the wire must carry it on every request.
    /// Derived from the pack, never transcribed: the e2e wire pins and
    /// the YAML `select` are then the same list by construction, so a
    /// column change touches two places (the YAML columns and the YAML
    /// select, held together by `select_pin_mirrors_the_mapped_columns`)
    /// and never this file — a hand-copied twenty-two-entry array is a
    /// third place to forget.
    fn select_json() -> Value {
        table("messages")
            .fixed_inputs
            .iter()
            .find_map(|(key, value)| (*key == "select").then(|| value.to_json()))
            .expect("messages pins a select")
    }

    fn message_row(id: &str) -> Value {
        json!({
            "id": id,
            "subject": format!("subject {id}"),
            "isRead": false,
            "isDraft": false,
            "receivedDateTime": "2026-08-14T09:15:42Z",
            "conversationId": format!("conv-{id}"),
            "from": {"emailAddress": {"name": "Redacted", "address": "sender@example.com"}}
        })
    }

    /// Health, discovery, and one empty page per table — the shared stub
    /// for e2es that assert on the *inputs* a scan sends rather than on
    /// rows (slack's `conversations_gateway` precedent).
    async fn empty_collections_gateway() -> MockGateway {
        MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return outlook_discovery(&req.path);
            }
            if req.method == "POST" {
                let empty = match req.path.as_str() {
                    "/v1/actions/outlook.list_messages" => {
                        json!({"messages": [], "nextLink": null})
                    }
                    "/v1/actions/outlook.list_mail_folders" => {
                        json!({"mailFolders": [], "nextLink": null})
                    }
                    _ => return MockResponse::new(404, "{}"),
                };
                return MockResponse::ok(&envelope_ok(&empty.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await
    }

    fn folder_row(id: &str, hidden: bool) -> Value {
        json!({
            "id": id,
            "displayName": format!("Folder {id}"),
            "parentFolderId": "root",
            "childFolderCount": 0,
            "unreadItemCount": 1,
            "totalItemCount": 2,
            "isHidden": hidden
        })
    }

    #[tokio::test]
    async fn messages_cursor_scan_pages_with_its_own_declared_inputs() {
        // Two-page cursor scan pinning MESSAGES' wire declaration: no
        // nextLink on page 1, the URI cursor verbatim afterwards, the
        // select pin and top=100 on EVERY request (top rides
        // continuation requests too — the executor ignores it there,
        // but the engine sends it and this pins that shape), explicit
        // null termination (the executor's spelling), row identity
        // across pages, exact key sets.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return outlook_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/outlook.list_messages" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("nextLink").and_then(Value::as_str) {
                    None => json!({"messages": [message_row("m-1"), message_row("m-2")],
                                    "nextLink": PAGE2_URI}),
                    Some(uri) if uri == PAGE2_URI => {
                        json!({"messages": [message_row("m-3")], "nextLink": null})
                    }
                    Some(other) => return MockResponse::new(400, format!("bad cursor {other}")),
                };
                return MockResponse::ok(&envelope_ok(&page.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_OUTLOOK_MESSAGES", "messages", "").await;

        let batches = collect(&ctx, "SELECT id FROM saas.m365.messages ORDER BY id").await;
        assert_eq!(column_values(&batches, "id"), vec!["m-1", "m-2", "m-3"]);

        let inputs = execute_inputs(&gateway, "outlook.list_messages");
        assert_eq!(inputs.len(), 2, "two cursor pages");
        assert_eq!(inputs[1]["nextLink"], PAGE2_URI, "the URI cursor verbatim");
        for (page, (input, expected_keys)) in inputs
            .iter()
            .zip([vec!["select", "top"], vec!["nextLink", "select", "top"]])
            .enumerate()
        {
            assert_eq!(input["top"], 100, "bounded page size: {input}");
            assert_eq!(input["select"], select_json(), "select pin: {input}");
            // Exactly the declared inputs, nothing else — filter,
            // orderby, bodyContentType and the unbound mailFolderId
            // must never reach a strict schema.
            assert_eq!(input_keys(input), expected_keys, "page {} keys", page + 1);
        }
    }

    #[tokio::test]
    async fn mail_folders_cursor_scan_pages_with_its_own_declared_inputs() {
        // mail_folders' own wire pin: includeHiddenFolders=true and
        // top=1000 on every request, terminating on the one spelling
        // outlook actually emits. The executor writes `nextLink`
        // unconditionally (`typeof payload["@odata.nextLink"] ===
        // "string" ? … : null`), so an omitted key is a shape this
        // gateway cannot produce and has no business in a mock; the
        // engine's tolerance for the omitted and empty-string spellings
        // is pinned where it belongs, in `pagination.rs`'s
        // `cursor_ends_on_missing_null_or_empty_next`.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return outlook_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/outlook.list_mail_folders" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                let page = match body["input"].get("nextLink").and_then(Value::as_str) {
                    None => json!({"mailFolders": [folder_row("f-1", false)],
                                    "nextLink": FOLDERS_PAGE2_URI}),
                    Some(uri) if uri == FOLDERS_PAGE2_URI => {
                        json!({"mailFolders": [folder_row("f-2", true)], "nextLink": null})
                    }
                    Some(other) => return MockResponse::new(400, format!("bad cursor {other}")),
                };
                return MockResponse::ok(&envelope_ok(&page.to_string()));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_OUTLOOK_FOLDERS",
            "mail_folders",
            "",
        )
        .await;

        let batches = collect(
            &ctx,
            "SELECT id, is_hidden FROM saas.m365.mail_folders ORDER BY id",
        )
        .await;
        assert_eq!(column_values(&batches, "id"), vec!["f-1", "f-2"]);

        let inputs = execute_inputs(&gateway, "outlook.list_mail_folders");
        assert_eq!(inputs.len(), 2, "the null cursor terminates the scan");
        for (page, (input, expected_keys)) in inputs
            .iter()
            .zip([
                vec!["includeHiddenFolders", "top"],
                vec!["includeHiddenFolders", "nextLink", "top"],
            ])
            .enumerate()
        {
            assert_eq!(
                input["includeHiddenFolders"],
                json!(true),
                "the state=all pin"
            );
            assert_eq!(input["top"], 1000);
            assert_eq!(input_keys(input), expected_keys, "page {} keys", page + 1);
        }
    }

    #[tokio::test]
    async fn optional_resource_forwards_verbatim_and_only_where_declared() {
        // One binding carries mailFolderId: messages receives it
        // verbatim, mail_folders — which declares no resources — never
        // sees it (a strict schema would 400 the stray key).
        let (gateway, ctx) = setup_with_gateway(
            empty_collections_gateway().await,
            "SKARDI_TEST_OC_OUTLOOK_RESOURCES",
            "messages, mail_folders",
            r#"{ mailFolderId: "AQMkAGE1M2IyNGNmLjAAAA-folder-inbox" }"#,
        )
        .await;

        for sql in [
            "SELECT * FROM saas.m365.messages",
            "SELECT * FROM saas.m365.mail_folders",
        ] {
            let batches = collect(&ctx, sql).await;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
        }

        let inputs = execute_inputs(&gateway, "outlook.list_messages");
        assert_eq!(
            input_keys(&inputs[0]),
            vec!["mailFolderId", "select", "top"]
        );
        assert_eq!(
            inputs[0]["mailFolderId"],
            "AQMkAGE1M2IyNGNmLjAAAA-folder-inbox"
        );

        let inputs = execute_inputs(&gateway, "outlook.list_mail_folders");
        assert_eq!(input_keys(&inputs[0]), vec!["includeHiddenFolders", "top"]);
    }

    #[tokio::test]
    async fn predicates_stay_local_against_a_provider_that_cannot_narrow() {
        // The no-pushdown guard, row identity included: a subject
        // equality predicate never reaches the wire (no filter mappings
        // exist — Outlook's `filter` is an OData expression no mapping
        // can compose) and DataFusion applies it locally.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return outlook_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/outlook.list_messages" {
                let mut wanted = message_row("m-1");
                wanted["subject"] = json!("needle");
                return MockResponse::ok(&envelope_ok(
                    &json!({"messages": [wanted, message_row("m-2"), message_row("m-3")],
                             "nextLink": null})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_OUTLOOK_LOCAL", "messages", "").await;

        let batches = collect(
            &ctx,
            "SELECT id FROM saas.m365.messages WHERE subject = 'needle'",
        )
        .await;
        assert_eq!(column_values(&batches, "id"), vec!["m-1"]);

        let inputs = execute_inputs(&gateway, "outlook.list_messages");
        assert_eq!(
            input_keys(&inputs[0]),
            vec!["select", "top"],
            "the predicate stayed local; no filter/orderby key was pushed"
        );
    }

    #[tokio::test]
    async fn limit_stops_cursor_pagination_early() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return outlook_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/outlook.list_messages" {
                // Every page advertises another; only LIMIT can stop this.
                return MockResponse::ok(&envelope_ok(
                    &json!({"messages": [message_row("m-1"), message_row("m-2")],
                             "nextLink": PAGE2_URI})
                    .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_OUTLOOK_LIMIT", "messages", "").await;

        let batches = collect(&ctx, "SELECT id FROM saas.m365.messages LIMIT 2").await;
        assert_eq!(column_values(&batches, "id").len(), 2);
        assert_eq!(
            execute_inputs(&gateway, "outlook.list_messages").len(),
            1,
            "one page satisfied LIMIT"
        );
    }

    #[tokio::test]
    async fn scan_of_an_empty_mailbox_is_clean() {
        // An empty collection is an empty result set, not an error, and
        // still costs exactly one request per table.
        let (gateway, ctx) = setup_with_gateway(
            empty_collections_gateway().await,
            "SKARDI_TEST_OC_OUTLOOK_EMPTY",
            "messages, mail_folders",
            "",
        )
        .await;

        for (table, action) in [
            ("messages", "outlook.list_messages"),
            ("mail_folders", "outlook.list_mail_folders"),
        ] {
            let batches = collect(&ctx, &format!("SELECT id FROM saas.m365.{table}")).await;
            assert_eq!(
                batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
                0,
                "{table}: an empty collection is an empty result set"
            );
            assert_eq!(
                execute_inputs(&gateway, action).len(),
                1,
                "{table}: an empty page still means exactly one request"
            );
        }
    }

    #[tokio::test]
    async fn a_repeated_cursor_fails_as_a_pagination_loop() {
        // A gateway that stops advancing must fail loudly, not spin —
        // and with URI cursors the repeated value is a graph.microsoft.com
        // URL, not row data, so the engine's loop error may name it.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return outlook_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/outlook.list_messages" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"messages": [message_row("m-1")], "nextLink": PAGE2_URI}).to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_OUTLOOK_LOOP", "messages", "").await;

        let err = ctx
            .sql("SELECT id FROM saas.m365.messages")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a non-advancing cursor must fail the scan");
        let message = err.to_string();
        assert!(
            message.contains("pagination loop") && message.contains("RFRM9Page2"),
            "loop identity is named: {message}"
        );
    }

    #[tokio::test]
    async fn a_non_string_cursor_fails_as_invalid() {
        // A present-but-non-string nextLink must fail as cursor drift,
        // never read as end-of-collection: treating it as termination
        // would silently truncate the scan.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return outlook_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/outlook.list_messages" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"messages": [message_row("m-1")], "nextLink": 42}).to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_OUTLOOK_BADCUR", "messages", "").await;

        let err = ctx
            .sql("SELECT id FROM saas.m365.messages")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a non-string cursor must fail the scan");
        let message = err.to_string();
        assert!(
            message.contains("$.nextLink")
                && message.contains("a number")
                && message.contains("not a string"),
            "cursor path and found kind are named: {message}"
        );
    }

    #[tokio::test]
    async fn provider_errors_surface_through_the_gateway_failure_envelope() {
        // error_path is None on purpose: assertOutlookResponse throws on
        // any non-2xx, so a Graph failure (e.g. the scope misdeclaration
        // biting a narrowly-consented token) arrives as the gateway's
        // failure envelope — whose errorCode must reach the user, named.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return outlook_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/outlook.list_messages" {
                return MockResponse::new(
                    403,
                    envelope_err(
                        "authorization_failed",
                        "Access token does not carry the required scopes.",
                    ),
                );
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_OUTLOOK_SCOPE", "messages", "").await;

        let err = ctx
            .sql("SELECT id FROM saas.m365.messages")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a missing-scope failure must fail the scan");
        let message = err.to_string();
        assert!(
            message.contains("authorization_failed") && message.contains("outlook.list_messages"),
            "the gateway's error code and the action are named: {message}"
        );
        assert!(
            !message.contains("row path"),
            "never the misleading row-path error: {message}"
        );
    }

    #[tokio::test]
    async fn a_failure_after_the_first_page_fails_the_whole_scan() {
        // The pack's loudest live finding — a folder-scoped scan 400ing
        // on page two against the gateway's own cursor — is a MID-scan
        // failure, and every "loud, never silent" promise in this file
        // rests on the engine failing the scan instead of serving page
        // one as the whole collection. Nothing pinned that arm: the
        // engine's own failure test fails on the FIRST request
        // (`pages_fetched: 0`), and the loop/drift tests fail on the
        // cursor's shape rather than on a page that never arrives. So
        // this replays the real defect's envelope, one page in. The
        // defect itself is fixed upstream (open-connector#372), but
        // the pin outlives it: any gateway refusing a mid-scan cursor
        // — a pre-fix release, a future allowlist regression — must
        // fail the scan the same way.
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return outlook_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/outlook.list_messages" {
                let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
                if body["input"].get("nextLink").is_none() {
                    return MockResponse::ok(&envelope_ok(
                        &json!({"messages": [message_row("m-1"), message_row("m-2")],
                                 "nextLink": PAGE2_URI})
                        .to_string(),
                    ));
                }
                return MockResponse::new(
                    400,
                    envelope_err(
                        "provider_request_failed",
                        "nextLink must target Outlook message pagination endpoints",
                    ),
                );
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (gateway, ctx) = setup_with_gateway(
            gateway,
            "SKARDI_TEST_OC_OUTLOOK_PAGE_TWO_FAILS",
            "messages",
            "",
        )
        .await;

        let err = ctx
            .sql("SELECT id FROM saas.m365.messages")
            .await
            .expect("plan")
            .collect()
            .await
            .expect_err("a failure on page two must fail the scan, never truncate it");
        let message = err.to_string();
        assert!(
            message.contains("provider_request_failed")
                && message.contains("outlook.list_messages"),
            "the gateway's error code and the action are named: {message}"
        );
        assert_eq!(
            execute_inputs(&gateway, "outlook.list_messages").len(),
            2,
            "the scan reached page two — page one's rows are not a result"
        );
    }

    #[tokio::test]
    async fn udtf_parity_for_mail_folders() {
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return outlook_discovery(&req.path);
            }
            if req.method == "POST" && req.path == "/v1/actions/outlook.list_mail_folders" {
                return MockResponse::ok(&envelope_ok(
                    &json!({"mailFolders": [folder_row("f-1", false)], "nextLink": null})
                        .to_string(),
                ));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let (_gateway, ctx) =
            setup_with_gateway(gateway, "SKARDI_TEST_OC_OUTLOOK_UDTF", "mail_folders", "").await;

        let from_table = collect(
            &ctx,
            "SELECT id, display_name, is_hidden FROM saas.m365.mail_folders",
        )
        .await;
        let from_udtf = collect(
            &ctx,
            "SELECT id, display_name, is_hidden \
             FROM open_connector_query('saas', 'outlook.mail_folders', '{}')",
        )
        .await;
        assert_eq!(from_table[0].schema(), from_udtf[0].schema());
        assert_eq!(
            arrow::util::pretty::pretty_format_batches(&from_table)
                .unwrap()
                .to_string(),
            arrow::util::pretty::pretty_format_batches(&from_udtf)
                .unwrap()
                .to_string()
        );
    }

    #[tokio::test]
    async fn drifted_contract_fails_registration_not_the_scan() {
        // The pin's refusal side: a gateway whose discovered output
        // schema differs from the captured contract is refused at
        // REGISTRATION, table and action named. (Every other e2e proves
        // the pass side via outlook_discovery's captured contracts.)
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return MockResponse::ok(&discovery_ok("{}", r#"{"type": "object"}"#, true, None));
            }
            MockResponse::new(404, "{}")
        })
        .await;
        let _token = EnvVarGuard::set("SKARDI_TEST_OC_OUTLOOK_DRIFT", "test-token");
        let mut ctx = SessionContext::new();
        let gateways = OpenConnectorGateways::default();
        let err = register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&outlook_config(
                "SKARDI_TEST_OC_OUTLOOK_DRIFT",
                "messages",
                "",
            )),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect_err("a drifted contract must fail registration");
        let message = err.to_string();
        assert!(
            message.contains("outlook.messages")
                && message.contains("outlook.list_messages")
                && message.contains("fingerprint mismatch"),
            "table, action, and cause are named: {message}"
        );
    }
}
