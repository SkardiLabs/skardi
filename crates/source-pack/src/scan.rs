//! The page walk every Open Connector consumer performs.
//!
//! One action, one resource, N pages: assemble the input, execute, check the
//! provider's in-band error, extract the rows, advance the cursor, stop at the
//! page budget or the deadline. Nothing in that list is specific to what the
//! rows become afterwards, and yet it was written four times over — once in
//! this workspace's query engine and, in cloud, once in the ETL's folder
//! walker and once per connector in the rbac syncer (six `for page in 1..`
//! loops, one each for Drive, OneDrive, GitHub, Slack, Feishu and Notion).
//!
//! The engine's copy was the only one that could not be shared, because it sat
//! inside a DataFusion `RecordBatchStream` and interleaved Arrow conversion,
//! projection and LIMIT pushdown with the walk. Those three are the ONLY parts
//! that need Arrow. Everything above them — and it is most of the loop — reads
//! and produces `serde_json::Value`.
//!
//! So the seam is the row slice. [`ActionScan::fetch_page`](crate::scan::ActionScan::fetch_page) and
//! [`ActionScan::rows`](crate::scan::ActionScan::rows) hand a consumer the raw rows of one page and
//! [`ActionScan::advance`](crate::scan::ActionScan::advance) moves to the next, which is the shape the engine
//! needs because it must do Arrow work BETWEEN the extraction and the advance
//! (a satisfied LIMIT means there is no next page to prepare, and `advance`
//! validates continuation state that a complete scan should never be failed
//! by). A consumer with no such interleaving calls
//! [`ActionScan::collect_all`](crate::scan::ActionScan::collect_all) and gets the whole collection.
//!
//! The three errors this raises — [`OpenConnectorError::ScanTimeout`],
//! [`OpenConnectorError::ScanBoundsExceeded`] and
//! [`OpenConnectorError::ProviderReportedError`] — already lived in this
//! crate before the walk did.

use std::slice;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use crate::client::OpenConnectorClient;
use crate::error::OpenConnectorError;
use crate::pagination::{CursorContinuation, Pagination, PaginationStrategy};
use crate::row_path::RowPath;
use crate::source_pack::{FixedValue, RowShape, SourcePackTable};

/// How pages 2..N are requested, when they differ from page one.
///
/// The two facts a WALK needs out of [`CursorContinuation`], which carries a
/// third — `expected_fingerprint` — that only registration uses. Splitting them
/// is not tidiness: a consumer building a `ScanTarget` by hand registers no
/// pack table, so a mandatory fingerprint field would force it to invent a
/// value that nothing checks and that reads, to the next person, like a pin
/// that is being enforced. cloud's rbac syncer is exactly that consumer —
/// OneDrive's ACL action continues through ITSELF while accepting only
/// `nextLink` on pages 2..N.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanContinuation {
    /// Action serving pages 2..N. Spelled even when it equals the table's own
    /// action, so a same-action continuation still names itself.
    pub action_id: &'static str,
    /// Whether pages 2..N carry ONLY the cursor. Required whenever the
    /// continuation action's schema accepts nothing else.
    pub cursor_only: bool,
}

impl From<CursorContinuation> for ScanContinuation {
    fn from(c: CursorContinuation) -> Self {
        Self {
            action_id: c.action_id,
            cursor_only: c.cursor_only,
        }
    }
}

/// Everything about WHAT is being scanned, as opposed to how far the scan has
/// got. Bound once, then immutable for the walk.
#[derive(Debug, Clone)]
pub struct ScanTarget {
    /// Stable table ID (`mock.items`) or a raw-action label, for errors and
    /// tracing.
    pub table_id: Arc<str>,
    /// Open Connector action to execute.
    pub action_id: Arc<str>,
    /// Pagination contract.
    pub pagination: PaginationStrategy,
    /// In-band provider-error location (see `SourcePackTable::error_path`);
    /// `None` for raw scans and packs whose providers error at HTTP level.
    pub error_path: Option<&'static str>,
    /// Fixed action inputs sent with every request (see
    /// [`SourcePackTable::fixed_inputs`]); empty for raw scans, whose whole
    /// input is caller-supplied.
    pub fixed_inputs: &'static [(&'static str, FixedValue)],
    /// Source-pack version. Not read here — it is part of the engine's scan
    /// cache key, and travels with the target so the two cannot disagree
    /// about which pack a cached result came from.
    pub source_pack_version: u32,
    /// How pages 2..N are requested, when they differ (see
    /// [`ScanContinuation`]); `None` for raw scans and for every table whose
    /// provider accepts the cursor alongside the original inputs.
    pub continuation: Option<ScanContinuation>,
    /// Whether the row path locates an array of rows or a single row object
    /// (see [`RowShape`]). Carried on the target rather than passed alongside
    /// it: this is the per-table response contract, exactly like `pagination`
    /// and `error_path`. Raw scans are always [`RowShape::Array`].
    pub row_shape: RowShape,
}

impl ScanTarget {
    /// The target of a bound source-pack table.
    pub fn from_pack_table(table: &SourcePackTable, source_pack_version: u32) -> Self {
        Self {
            table_id: Arc::from(table.id),
            action_id: Arc::from(table.action_id),
            pagination: table.pagination,
            error_path: table.error_path,
            fixed_inputs: table.fixed_inputs,
            source_pack_version,
            continuation: table.continuation.map(ScanContinuation::from),
            row_shape: table.row_shape,
        }
    }
}

/// The bounds a scan runs under.
///
/// Explicit and without a `Default`, for the reason
/// [`crate::client::TransportPolicy`] has none: the consumers genuinely
/// differ. A query engine bounds rows because a runaway scan is a memory
/// problem; an ACL walk bounds pages because a truncated member list is a
/// revocation. Picking one silently would give the other the wrong ceiling.
#[derive(Debug, Clone, Copy)]
pub struct ScanBounds {
    /// Maximum pages to fetch. Exceeding it is
    /// [`OpenConnectorError::ScanBoundsExceeded`], never a quiet stop —
    /// stopping would report a PREFIX of the collection as the whole of it.
    pub max_pages: u32,
    /// Wall-clock budget for the entire walk, request I/O and client-internal
    /// retry backoff included.
    pub timeout: Duration,
}

/// One action's paginated walk.
///
/// Holds the position, not the output: rows are handed back per page and never
/// accumulated here, so a consumer that streams them never pays for a copy it
/// does not want. [`Self::collect_all`] is the accumulating convenience for
/// the consumers that do.
#[derive(Debug)]
pub struct ActionScan {
    client: Arc<OpenConnectorClient>,
    target: ScanTarget,
    connection_alias: Option<String>,
    /// The binding's resource inputs — a JSON object by construction.
    resource: Value,
    /// Caller-supplied inputs layered over the pack's fixed ones. The engine
    /// puts pushed-down filters here; a syncer puts whatever its action needs.
    /// Applied AFTER `fixed_inputs` so a caller can override a pack default,
    /// which is what makes a pushed `state = 'open'` beat a declared
    /// `state=all`.
    extra_inputs: Vec<(String, Value)>,
    row_path: RowPath,
    error_path: Option<RowPath>,
    pagination: Pagination,
    bounds: ScanBounds,
    deadline: Instant,
    pages_fetched: u32,
}

impl ActionScan {
    /// Bind a scan. Fails here rather than mid-walk if a declared path is
    /// malformed.
    pub fn new(
        client: Arc<OpenConnectorClient>,
        target: ScanTarget,
        connection_alias: Option<String>,
        resource: Value,
        extra_inputs: Vec<(String, Value)>,
        row_path: &str,
        bounds: ScanBounds,
    ) -> Result<Self, OpenConnectorError> {
        let parsed_row_path = match target.row_shape {
            RowShape::Array => RowPath::parse(row_path)?,
            RowShape::Object => RowPath::parse_object_root(row_path)?,
        };
        let error_path = target.error_path.map(RowPath::parse).transpose()?;
        let pagination = Pagination::new(target.pagination)?;
        Ok(Self {
            client,
            target,
            connection_alias,
            resource,
            extra_inputs,
            row_path: parsed_row_path,
            error_path,
            pagination,
            bounds,
            deadline: Instant::now() + bounds.timeout,
            pages_fetched: 0,
        })
    }

    /// What is being scanned. Consumers log and key caches off it, and
    /// borrowing it here keeps one copy rather than making every caller hold
    /// its own beside the scan's.
    pub fn target(&self) -> &ScanTarget {
        &self.target
    }

    /// The 1-based number of the page about to be requested.
    pub fn page(&self) -> usize {
        self.pagination.page()
    }

    /// Pages actually requested from the gateway.
    ///
    /// Gateway traffic, not rows delivered: a page that arrives and then fails
    /// extraction was still a real request against a real rate-limit budget,
    /// and a failure event that under-reports it misleads whoever reads it.
    pub fn pages_fetched(&self) -> u32 {
        self.pages_fetched
    }

    /// The scan's deadline, for a consumer that interleaves its own work and
    /// must not emit a result produced after the budget ran out.
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// The error a consumer should raise when it observes the deadline passing
    /// during its own work.
    pub fn timeout_error(&self) -> OpenConnectorError {
        OpenConnectorError::ScanTimeout {
            table: self.target.table_id.to_string(),
            seconds: self.bounds.timeout.as_secs(),
        }
    }

    /// The row path, as declared. For error messages that must name it.
    pub fn row_path(&self) -> &RowPath {
        &self.row_path
    }

    /// Assemble the input for the page about to be requested, and name the
    /// action that will serve it.
    fn request(&self) -> (String, Map<String, Value>) {
        // Some providers serve pages 2..N from a DIFFERENT action than the one
        // that began the listing (Dropbox's `list_folder` →
        // `list_folder_continue`), and that action's schema commonly accepts
        // the cursor and nothing else. Page one always uses the table's own
        // action with the full input.
        let continuation = self
            .target
            .continuation
            .filter(|_| self.pagination.page() > 1);
        let action_id: String = match &continuation {
            Some(continuation) => continuation.action_id.to_string(),
            None => self.target.action_id.to_string(),
        };

        let input = if continuation.is_some_and(|c| c.cursor_only) {
            // Everything the other branch assembles is a hard 400 here: the
            // continue action declares `cursor` as its only property under
            // `additionalProperties: false`. The listing's resources, fixed
            // inputs, extra inputs and page size were all committed by the
            // request that opened it, and the cursor carries that state
            // forward on the provider's side.
            let mut input = Map::new();
            self.pagination.apply_cursor_only(&mut input);
            input
        } else {
            let mut input = self.resource.as_object().cloned().unwrap_or_default();
            for (field, value) in self.target.fixed_inputs {
                input.insert((*field).to_string(), value.to_json());
            }
            for (field, value) in &self.extra_inputs {
                input.insert(field.clone(), value.clone());
            }
            self.pagination.apply(&mut input);
            input
        };
        (action_id, input)
    }

    /// Fetch one page's response envelope.
    ///
    /// Charges the page budget and the deadline first, so neither is exceeded
    /// by the request this is about to make rather than only detected after
    /// it.
    pub async fn fetch_page(&mut self) -> Result<Value, OpenConnectorError> {
        if Instant::now() >= self.deadline {
            return Err(self.timeout_error());
        }
        if self.pagination.page() > self.bounds.max_pages as usize {
            return Err(OpenConnectorError::ScanBoundsExceeded {
                table: self.target.table_id.to_string(),
                bound: "max_pages",
                limit: u64::from(self.bounds.max_pages),
            });
        }

        let page = self.pagination.page();
        let (action_id, input) = self.request();
        // The deadline covers the whole gateway operation, request I/O and any
        // retry backoff inside the client included. Dropping this future on
        // timeout also prevents another retry from being sent.
        let envelope = tokio::time::timeout_at(
            tokio::time::Instant::from_std(self.deadline),
            self.client.execute(
                &action_id,
                &Value::Object(input),
                self.connection_alias.as_deref(),
            ),
        )
        .await
        .map_err(|_| self.timeout_error())??;
        // Counted at fetch time on purpose — see `pages_fetched`.
        self.pages_fetched += 1;
        if Instant::now() >= self.deadline {
            return Err(self.timeout_error());
        }

        // Some gateways forward a provider's in-band application errors
        // unchanged (Slack-style HTTP 200, `ok: false` + `error`). Packs
        // targeting such a gateway declare `error_path` so the provider's own
        // code surfaces instead of the misleading row-path error the missing
        // row array would raise.
        if let Some(error_path) = &self.error_path
            && let Ok(code) = error_path.extract(&envelope, page)
            && !code.is_null()
        {
            let code = match code.as_str() {
                Some(text) => crate::text::truncate_chars(text, 128),
                None => format!("<{}>", crate::row_path::json_kind(code)),
            };
            return Err(OpenConnectorError::ProviderReportedError {
                // The action that actually answered — on a continuation page
                // that is the continue action, and naming the table's opening
                // action instead would misdirect the reader.
                action_id,
                page,
                code,
            });
        }
        Ok(envelope)
    }

    /// The rows of a fetched page.
    ///
    /// Borrowed out of the envelope rather than cloned, so a consumer that
    /// converts them into some other representation never materialises an
    /// intermediate `Vec`. An object-shaped table hands its single response
    /// object to the SAME slice, via `from_ref`, so there is no second
    /// extraction path to keep in sync.
    pub fn rows<'a>(&self, envelope: &'a Value) -> Result<&'a [Value], OpenConnectorError> {
        match self.target.row_shape {
            RowShape::Array => self.row_path.rows(envelope, self.pagination.page()),
            RowShape::Object => Ok(slice::from_ref(
                self.row_path.row_object(envelope, self.pagination.page())?,
            )),
        }
    }

    /// Advance past the page just handled; `false` when the scan is complete.
    ///
    /// Separate from [`Self::fetch_page`] because a consumer may decide, after
    /// seeing the rows, that there is no next page to prepare — the engine
    /// does exactly that when a pushed-down LIMIT is satisfied. Calling this
    /// anyway would parse and validate continuation state on a final page,
    /// failing a scan whose result is already complete.
    pub fn advance(
        &mut self,
        envelope: &Value,
        rows: &[Value],
    ) -> Result<bool, OpenConnectorError> {
        self.pagination.advance(envelope, rows.len(), rows.last())
    }

    /// Walk every page and return all rows.
    ///
    /// For consumers that want the collection rather than the stream — an ACL
    /// walk, a folder listing. Rows are cloned out of each envelope, which is
    /// what "return them all" costs; a consumer that cannot afford that drives
    /// [`Self::fetch_page`] itself.
    ///
    /// Every bound still applies, and every one of them REFUSES rather than
    /// truncating. That is the whole reason a syncer can use this: a short
    /// member list is not a smaller answer, it is a revocation.
    pub async fn collect_all(&mut self) -> Result<Vec<Value>, OpenConnectorError> {
        let mut all = Vec::new();
        loop {
            let envelope = self.fetch_page().await?;
            let rows = self.rows(&envelope)?;
            all.extend(rows.iter().cloned());
            if !self.advance(&envelope, rows)? {
                return Ok(all);
            }
        }
    }
}

#[cfg(all(test, feature = "testing"))]
mod tests {
    use super::*;
    use crate::pagination::AbsentCursor;
    use crate::testing::{MockGateway, MockResponse, RecordedRequest};
    use serde_json::json;

    const TABLE: &str = "mock.items";

    /// `RecordedRequest` keeps the body as text; every assertion here is about
    /// the JSON inside it.
    fn body_of(req: &RecordedRequest) -> Value {
        serde_json::from_str(&req.body).unwrap_or(Value::Null)
    }

    fn target(pagination: PaginationStrategy) -> ScanTarget {
        ScanTarget {
            table_id: Arc::from(TABLE),
            action_id: Arc::from("mock.list"),
            pagination,
            error_path: None,
            fixed_inputs: &[],
            source_pack_version: 1,
            continuation: None,
            row_shape: RowShape::Array,
        }
    }

    fn cursor_pagination(absent_cursor: AbsentCursor) -> PaginationStrategy {
        PaginationStrategy::Cursor {
            cursor_param: "cursor",
            next_cursor_path: "$.nextCursor",
            page_size_param: Some("limit"),
            page_size: 2,
            has_more_path: None,
            absent_cursor,
        }
    }

    fn bounds(max_pages: u32) -> ScanBounds {
        ScanBounds {
            max_pages,
            timeout: Duration::from_secs(30),
        }
    }

    async fn scan_against(
        gateway: &MockGateway,
        pagination: PaginationStrategy,
        bounds: ScanBounds,
    ) -> ActionScan {
        let client = Arc::new(
            OpenConnectorClient::new(&gateway.url, "t", Duration::from_secs(5)).expect("client"),
        );
        ActionScan::new(
            client,
            target(pagination),
            None,
            json!({ "folderId": "f1" }),
            Vec::new(),
            "$.items",
            bounds,
        )
        .expect("scan binds")
    }

    /// A page of rows plus a cursor, or a terminal page.
    fn page_of(ids: &[u64], next: Option<&str>) -> MockResponse {
        let items: Vec<Value> = ids.iter().map(|i| json!({ "id": i })).collect();
        let data = match next {
            Some(cursor) => json!({ "items": items, "nextCursor": cursor }),
            None => json!({ "items": items, "nextCursor": null }),
        };
        MockResponse::new(
            200,
            serde_json::to_string(&json!({
                "success": true, "message": "", "data": data,
                "errorCode": null, "meta": { "actionId": "mock.list" }
            }))
            .expect("serializable"),
        )
    }

    /// The whole point: one call replaces a hand-written `for page in 1..`.
    #[tokio::test]
    async fn collect_all_walks_every_page() {
        let gateway = MockGateway::start(|req: &RecordedRequest| {
            let cursor = body_of(req)["input"]["cursor"].as_str().map(str::to_owned);
            match cursor.as_deref() {
                None => page_of(&[1, 2], Some("c1")),
                Some("c1") => page_of(&[3, 4], Some("c2")),
                _ => page_of(&[5], None),
            }
        })
        .await;
        let mut scan = scan_against(
            &gateway,
            cursor_pagination(AbsentCursor::EndsTheScan),
            bounds(10),
        )
        .await;

        let rows = scan.collect_all().await.expect("walk completes");

        let ids: Vec<u64> = rows.iter().map(|r| r["id"].as_u64().expect("id")).collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5], "every page's rows, in order");
        assert_eq!(scan.pages_fetched(), 3, "three gateway requests");
    }

    /// **The bound a syncer depends on.** Running out of pages must REFUSE.
    /// Returning the rows gathered so far would hand back a prefix that reads
    /// exactly like a complete collection — and for an ACL walk, a prefix is a
    /// revocation of everyone past it.
    #[tokio::test]
    async fn exceeding_the_page_budget_refuses_rather_than_truncating() {
        // Never terminates on its own: only the budget can stop this. The
        // cursor must GROW rather than repeat — an unchanging cursor is a
        // non-advancing gateway, which the paginator's loop detector catches
        // first, and this test would then pass for the wrong reason.
        let gateway = MockGateway::start(|req: &RecordedRequest| {
            let seen = body_of(req)["input"]["cursor"]
                .as_str()
                .unwrap_or("")
                .to_owned();
            page_of(&[1], Some(&format!("{seen}x")))
        })
        .await;
        let mut scan = scan_against(
            &gateway,
            cursor_pagination(AbsentCursor::EndsTheScan),
            bounds(2),
        )
        .await;

        let err = scan
            .collect_all()
            .await
            .expect_err("the budget must fail the walk, not end it");

        match err {
            OpenConnectorError::ScanBoundsExceeded { table, bound, .. } => {
                assert_eq!(bound, "max_pages");
                assert_eq!(table, TABLE);
            }
            other => panic!("expected ScanBoundsExceeded, got {other}"),
        }
        assert_eq!(
            scan.pages_fetched(),
            2,
            "the budget is spent, not exceeded, before the refusal"
        );
    }

    /// The strict cursor contract reaches the walk, not just the paginator: a
    /// dropped key fails the scan rather than ending it.
    #[tokio::test]
    async fn a_dropped_cursor_key_fails_a_strict_walk() {
        let gateway = MockGateway::start(|req: &RecordedRequest| {
            let first = body_of(req)["input"]["cursor"].is_null();
            if first {
                page_of(&[1], Some("c1"))
            } else {
                // Second page omits `nextCursor` entirely.
                MockResponse::new(
                    200,
                    r#"{"success":true,"message":"","data":{"items":[{"id":2}]},
                        "errorCode":null,"meta":{"actionId":"mock.list"}}"#,
                )
            }
        })
        .await;
        let mut scan = scan_against(
            &gateway,
            cursor_pagination(AbsentCursor::IsDrift),
            bounds(10),
        )
        .await;

        let err = scan
            .collect_all()
            .await
            .expect_err("a dropped key is drift");
        assert!(
            matches!(err, OpenConnectorError::PaginationCursorInvalid { .. }),
            "expected cursor drift, got {err:?}"
        );
    }

    /// Caller inputs layer OVER the pack's fixed ones — the rule that lets a
    /// pushed-down filter beat a declared default.
    #[tokio::test]
    async fn extra_inputs_override_the_packs_fixed_inputs() {
        static FIXED: &[(&str, FixedValue)] = &[("state", FixedValue::Str("all"))];
        let gateway = MockGateway::start(|_| page_of(&[1], None)).await;
        let client = Arc::new(
            OpenConnectorClient::new(&gateway.url, "t", Duration::from_secs(5)).expect("client"),
        );
        let mut t = target(cursor_pagination(AbsentCursor::EndsTheScan));
        t.fixed_inputs = FIXED;
        let mut scan = ActionScan::new(
            client,
            t,
            None,
            json!({ "folderId": "f1" }),
            vec![("state".to_string(), json!("open"))],
            "$.items",
            bounds(10),
        )
        .expect("scan binds");

        scan.collect_all().await.expect("walk completes");

        let sent = gateway.requests();
        let input = body_of(&sent[0])["input"].clone();
        assert_eq!(
            input["state"], "open",
            "the caller's input must win over the pack's fixed one"
        );
        assert_eq!(
            input["folderId"], "f1",
            "the binding's resource inputs are still sent"
        );
    }

    /// A provider that reports failure in-band (HTTP 200, `ok: false`) must
    /// surface its OWN code, not the row-path error the absent rows would
    /// otherwise raise.
    #[tokio::test]
    async fn an_in_band_provider_error_surfaces_its_own_code() {
        let gateway = MockGateway::start(|_| {
            MockResponse::new(
                200,
                r#"{"success":true,"message":"","data":{"error":"ratelimited"},
                    "errorCode":null,"meta":{"actionId":"mock.list"}}"#,
            )
        })
        .await;
        let client = Arc::new(
            OpenConnectorClient::new(&gateway.url, "t", Duration::from_secs(5)).expect("client"),
        );
        let mut t = target(cursor_pagination(AbsentCursor::EndsTheScan));
        t.error_path = Some("$.error");
        let mut scan = ActionScan::new(
            client,
            t,
            None,
            json!({}),
            Vec::new(),
            "$.items",
            bounds(10),
        )
        .expect("scan binds");

        let err = scan.collect_all().await.expect_err("in-band error");
        match err {
            OpenConnectorError::ProviderReportedError { code, page, .. } => {
                assert_eq!(code, "ratelimited", "the provider's own code, not ours");
                assert_eq!(page, 1);
            }
            other => panic!("expected ProviderReportedError, got {other}"),
        }
    }
}
