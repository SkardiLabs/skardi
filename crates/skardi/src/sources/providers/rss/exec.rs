//! Physical execution for `rss` scans: one [`RssScanExec`] with one partition
//! per subscription, and the sole consumer of [`RssEngine`].
//!
//! A partition is one feed: it serves that feed's window (`items`) or its
//! health row (`feeds`), applies the projection, and ends. Nothing here can
//! fail a scan — [`RssEngine::serve_feed`] has no `Err` at all, and the two
//! bounds this module owns degrade the partition that hit them rather than the
//! query.
//!
//! ## The LIMIT launch gate
//!
//! `serve_feed` takes a gate closure and re-reads it *after* acquiring its
//! politeness permit; this module supplies the closure, and it reads state
//! shared by every partition of the scan (`ScanShared::emitted`). Both halves
//! of the check matter, for different reasons:
//!
//! - The pre-check, before `serve_feed` is called at all, is what makes a
//!   sequentially drained scan stop working once the LIMIT is met.
//! - The gate the engine re-reads after the permit is the only one that can
//!   stop a launch under concurrency, because DataFusion drives partitions
//!   simultaneously — `CoalescePartitionsExec::execute` spawns one independent
//!   task per input partition (datafusion-physical-plan 52.5.0,
//!   `src/coalesce_partitions.rs:201-206`) — so every partition passes any
//!   pre-check while `emitted` is still 0 and then queues on the semaphore.
//!
//! The counter is `SeqCst` on both sides, so the load a queued partition
//! performs after being handed the permit is ordered against the `fetch_add`
//! that the emitting partition already completed. What the gate does *not*
//! promise is a row-exact stop: two partitions that pass the gate together
//! both serve, and the surplus is truncated above this plan. `push_down_limit`
//! copies the fetch into the `TableScan` but rebuilds the `Limit` node over it
//! rather than removing it (datafusion-optimizer 52.5.0,
//! `src/push_down_limit.rs:93-110` and the `make_limit` both arms return
//! through, `:222-238`), so the pushed limit is a hint to the scan and the
//! operator above still enforces the row count. The gate exists to stop
//! *requests*, not to count rows.
//!
//! ## The scan deadline
//!
//! [`RssEngine::scan_timeout`] is enforced here and nowhere else. Each
//! partition wraps its serve in `timeout_at` against one deadline computed once
//! per scan, and a partition that outruns it emits zero rows and a `warn!`. It
//! must not surface an error: a slow feed failing the whole query is the
//! failure mode this provider's design rejects, and it is why `serve_feed`
//! returns no `Err` in the first place.
//!
//! Three consequences worth naming.
//!
//! The fetcher's own per-request timeout is normally the shorter of the two —
//! 10 seconds against a 60-second scan deadline at the shipped defaults — so
//! this is a backstop for the aggregate rather than the common path. It bites
//! when enough feeds queue behind the politeness bound that a later one runs
//! out of scan budget before its own request timeout fires.
//!
//! `timeout_at` polls the wrapped future before it polls the delay (tokio
//! 1.52.3, `src/time/timeout.rs:216-221`), so an already-elapsed deadline still
//! gives the serve one poll — the deadline bounds how long a partition *waits*,
//! not whether it starts.
//!
//! And a serve dropped at the deadline writes no health state, by construction:
//! the future is cancelled before the engine records anything. That is right for
//! `feeds.last_status` — nothing was observed, so claiming an error would be a
//! lie — but it means a feed that *reliably* outruns the scan deadline reads
//! `never` with a NULL `last_error` indefinitely, and the only signal is this
//! module's `warn!`. Whoever chases "why is this feed always empty" should look
//! for that log line rather than at the `feeds` row.
//!
//! ## Projection, including the empty one
//!
//! `SELECT count(*)` reaches a scan as `projection: Some(vec![])`, and a
//! zero-column batch that lost its row count would make the count wrong — the
//! bug `providers/mod.rs`'s `CountSafeTable` exists to work around for two
//! other providers. `RecordBatch::project` carries the source batch's row
//! count into the projected batch and derives the projected schema from it
//! (arrow-array 57.3.1, `src/record_batch.rs:433-458`, and its own
//! `project_empty` test), so the empty projection needs no special case here:
//! it produces exactly the zero-column, row-count-bearing batch, under the
//! same schema — surface-version metadata included — that this plan
//! advertises.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::stream::{self, StreamExt};

use super::engine::RssEngine;
use super::schema::{feeds_schema, items_schema};

/// Ceiling on the configured scan timeout.
const MAX_SCAN_TIMEOUT: Duration = Duration::from_secs(60 * 60);

/// Which of the provider's two tables a scan serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RssTableKind {
    /// Feed health, one row per subscription.
    Feeds,
    /// Feed items, one window per subscription.
    Items,
}

impl RssTableKind {
    /// The table's full (unprojected) schema.
    pub fn schema(self) -> SchemaRef {
        match self {
            Self::Feeds => feeds_schema(),
            Self::Items => items_schema(),
        }
    }

    /// Lower-case table name, for `EXPLAIN` output and log fields.
    fn as_str(self) -> &'static str {
        match self {
            Self::Feeds => "feeds",
            Self::Items => "items",
        }
    }
}

/// State every partition of one scan shares, built once in
/// [`RssScanExec::new`].
///
/// Living on the plan rather than the stream makes the plan object
/// single-execution unless `reset_state` rebuilds it — the consumer-facing
/// statement of that contract is in `table.rs`'s module doc ("The plan
/// `scan()` returns is single-execution").
struct ScanShared {
    /// Rows served by this scan so far, across all partitions — the value the
    /// launch gate compares against the LIMIT. Counts rows *served*, not rows
    /// the consumer kept: this scan does not truncate to the LIMIT itself.
    emitted: AtomicUsize,
    /// When every partition of this scan must stop waiting. Measured from
    /// planning time rather than from the first `execute`, which is the
    /// conservative direction: it cannot outlast the configured budget.
    deadline: Instant,
}

impl ScanShared {
    /// Whether a launch is still worth making. `None` means no LIMIT, so the
    /// gate is always open.
    fn gate_open(&self, limit: Option<usize>) -> bool {
        limit.is_none_or(|rows| self.emitted.load(Ordering::SeqCst) < rows)
    }
}

/// A partition-per-feed scan of one `rss` source.
pub struct RssScanExec {
    engine: Arc<RssEngine>,
    kind: RssTableKind,
    feeds: Vec<String>,
    projection: Option<Vec<usize>>,
    limit: Option<usize>,
    schema: SchemaRef,
    properties: PlanProperties,
    shared: Arc<ScanShared>,
}

impl fmt::Debug for RssScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RssScanExec")
            .field("kind", &self.kind)
            .field("feeds", &self.feeds.len())
            .field("projection", &self.projection)
            .field("limit", &self.limit)
            .finish()
    }
}

impl RssScanExec {
    /// Build a scan over `feeds`, already pruned to the subscriptions this
    /// scan must visit, in subscription order.
    pub fn new(
        engine: Arc<RssEngine>,
        kind: RssTableKind,
        feeds: Vec<String>,
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> DFResult<Self> {
        let full_schema = kind.schema();
        let schema = match &projection {
            Some(indices) => Arc::new(full_schema.project(indices)?),
            None => full_schema,
        };
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(feeds.len().max(1)),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        let configured_timeout = engine.scan_timeout();
        if configured_timeout > MAX_SCAN_TIMEOUT {
            tracing::warn!(
                configured_scan_timeout_seconds = configured_timeout.as_secs(),
                effective_scan_timeout_seconds = MAX_SCAN_TIMEOUT.as_secs(),
                "rss scan_timeout_seconds clamped to the exec layer's ceiling"
            );
        }
        let shared = Arc::new(ScanShared {
            emitted: AtomicUsize::new(0),
            deadline: scan_deadline(Instant::now(), configured_timeout),
        });
        Ok(Self {
            engine,
            kind,
            feeds,
            projection,
            limit,
            schema,
            properties,
            shared,
        })
    }
}

impl DisplayAs for RssScanExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "RssScanExec: kind={} feeds={} limit={:?}",
            self.kind.as_str(),
            self.feeds.len(),
            self.limit
        )
    }
}

impl ExecutionPlan for RssScanExec {
    fn name(&self) -> &str {
        "RssScanExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "RssScanExec is a leaf plan and takes no children".to_string(),
            ))
        }
    }

    /// Rebuild this plan around a *fresh* `ScanShared`: a zeroed counter and a
    /// deadline recomputed from now.
    ///
    /// The default implementation is `with_new_children(children())`
    /// (datafusion-physical-plan 52.5.0, `src/execution_plan.rs:232-236`), and
    /// for a leaf plan that hands back the very same `Arc` — carrying a counter
    /// that already satisfies the LIMIT and a deadline that has already been
    /// spent. A re-executed scan would then serve zero rows without issuing a
    /// request. `RecursiveQueryExec` reaches exactly that: it calls
    /// `reset_plan_states` on its recursive term and then `execute(0, …)` once
    /// per iteration (`src/recursive_query.rs:359-362`, and the `reset_state`
    /// walk itself at `:396-402`), so a
    /// `WITH RECURSIVE` whose recursive term scans a feed would silently shrink
    /// after its first iteration. The hook exists for precisely this and its doc
    /// says stateful implementations must override it
    /// (`src/execution_plan.rs:211-222`); its one requirement on an override is
    /// that the cached plan properties stay valid, which they do — the schema,
    /// the partitioning, and the feed list are rebuilt from the same inputs.
    fn reset_state(self: Arc<Self>) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(
            Arc::clone(&self.engine),
            self.kind,
            self.feeds.clone(),
            self.projection.clone(),
            self.limit,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let partitions = self.properties.partitioning.partition_count();
        if partition >= partitions {
            return Err(DataFusionError::Internal(format!(
                "RssScanExec has {partitions} partitions, got partition {partition}"
            )));
        }
        // The partition count floors at 1, so a scan pruned to no feeds at all
        // is still a legal single-partition plan — with no feed to serve.
        let Some(feed) = self.feeds.get(partition).cloned() else {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                stream::empty(),
            )));
        };

        let engine = Arc::clone(&self.engine);
        let shared = Arc::clone(&self.shared);
        let kind = self.kind;
        let limit = self.limit;
        let projection = self.projection.clone();
        // Everything below runs on the first poll, not here: the gate's
        // pre-check has to read the shared counter when the partition is
        // actually driven, and dropping the stream before that point must leave
        // the feed untouched.
        let served = async move {
            let batch = match kind {
                // `feeds_row` is synchronous and issues no request, so there is
                // no launch to gate and no wait to bound — the two things the
                // `items` path below needs. It cannot even reach the fetcher:
                // there is no `await` to hang one off of.
                RssTableKind::Feeds => Some(engine.feeds_row(&feed)),
                RssTableKind::Items => serve_items(&engine, &feed, limit, &shared).await,
            };
            batch.map(|batch| project(batch, projection.as_deref()))
        };
        // Zero or one batch per partition: `stream::iter` over the `Option`
        // yields nothing at all for a feed that served no rows, which is what
        // makes a dead feed contribute an empty partition rather than an empty
        // batch.
        let stream = stream::once(served).flat_map(stream::iter);
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        )))
    }
}

/// One `items` partition's rows, or `None` for a partition that serves none —
/// because the feed had nothing to serve, because the LIMIT was already
/// satisfied, or because the scan deadline passed.
async fn serve_items(
    engine: &RssEngine,
    feed: &str,
    limit: Option<usize>,
    shared: &ScanShared,
) -> Option<RecordBatch> {
    // Cheap pre-check: with the LIMIT already met there is nothing to serve, so
    // skip the resolve, the cache read, and the permit queue entirely. This is
    // the half that stops work; the gate below is the half that is correct
    // under concurrency (see the module doc).
    if !shared.gate_open(limit) {
        return None;
    }
    let deadline = tokio::time::Instant::from_std(shared.deadline);
    match tokio::time::timeout_at(
        deadline,
        engine.serve_feed(feed, || shared.gate_open(limit)),
    )
    .await
    {
        Ok(batch) => {
            if let Some(batch) = &batch {
                shared.emitted.fetch_add(batch.num_rows(), Ordering::SeqCst);
            }
            batch
        }
        // Degrade this partition, not the scan: zero rows and a warning.
        // Dropping the serve mid-fetch is a case Task 11 already pinned from
        // its own side — `a_cancelled_serve_releases_the_politeness_permit`
        // covers the permit and the untouched health state — and
        // `the_scan_deadline_degrades_one_partition_to_zero_rows` re-checks the
        // health half through this path.
        Err(_elapsed) => {
            tracing::warn!(
                feed,
                scan_timeout_seconds = engine.scan_timeout().as_secs(),
                "rss scan deadline reached"
            );
            None
        }
    }
}

/// Apply the scan's projection to one full-width batch.
///
/// The indices were validated against the same schema in [`RssScanExec::new`],
/// so this cannot fail for a batch the engine built; the error is mapped rather
/// than unwrapped because "the engine built it" is an argument about another
/// module, not something this function can check.
fn project(batch: RecordBatch, projection: Option<&[usize]>) -> DFResult<RecordBatch> {
    match projection {
        Some(indices) => batch.project(indices).map_err(DataFusionError::from),
        None => Ok(batch),
    }
}

/// `now + timeout`, with `timeout` clamped to [`MAX_SCAN_TIMEOUT`].
///
/// `RssConfig` stores `scan_timeout_seconds` raw — validation only rejects `0`
/// — and this is the first code that turns it into an `Instant`, so it carries
/// the ceiling. The clamp is also what makes the fallback unreachable in
/// practice: after it, `checked_add` can only fail on a platform `Instant`
/// within an hour of its representable maximum. Falling back to `now` there
/// gives an already-elapsed deadline, so every partition degrades to zero rows
/// with a warning — chosen over `Instant`'s `+`, which "may panic if the
/// resulting point in time cannot be represented" and, per `std`'s own example,
/// "panics on macOS" for a large enough add. A visibly empty result beats a
/// panic mid-scan.
fn scan_deadline(now: Instant, timeout: Duration) -> Instant {
    now.checked_add(timeout.min(MAX_SCAN_TIMEOUT))
        .unwrap_or(now)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use datafusion::common::stats::Precision;

    use super::*;
    use crate::sources::providers::open_connector::testutil::{CapturedEvent, capture_events};
    use crate::sources::providers::rss::schema::WINDOW_STATUS_IDX;
    use crate::sources::providers::rss::testutil::{
        MockFeedServer, MockResponse, RSS2_MINIMAL, collect_stream, feed_urls, str_col,
        test_engine, total_rows,
    };

    fn ctx() -> Arc<TaskContext> {
        Arc::new(TaskContext::default())
    }

    /// An engine whose subscriptions point at a host nothing listens on — for
    /// the plan-shape tests, which must not reach the network at all.
    fn offline_engine(feeds: &[&str]) -> Arc<RssEngine> {
        let urls: Vec<(String, String)> = feeds
            .iter()
            .map(|name| {
                (
                    (*name).to_string(),
                    format!("http://feed.invalid/{name}.xml"),
                )
            })
            .collect();
        Arc::new(test_engine(&urls, |_| {}))
    }

    fn items_exec(
        engine: Arc<RssEngine>,
        feeds: &[&str],
        projection: Option<Vec<usize>>,
        limit: Option<usize>,
    ) -> RssScanExec {
        RssScanExec::new(
            engine,
            RssTableKind::Items,
            feeds.iter().map(|f| (*f).to_string()).collect(),
            projection,
            limit,
        )
        .expect("build the items exec")
    }

    /// Captured events with `message`, in emission order.
    ///
    /// Every test in this binary that reaches one of this module's `warn!`
    /// callsites must hold a [`capture_events`] guard, whether or not it asserts
    /// on the events. `tracing` caches a callsite's `Interest` globally on first
    /// use, and with a single registered dispatcher it computes that interest
    /// from whatever default is installed on the *registering* thread
    /// (tracing-core 0.1.36, `src/callsite.rs:490-506` and the `JustOne`
    /// rebuilder at `:544-560`). A guardless test that emits the callsite first
    /// therefore caches `Interest::never` for the whole binary and silently
    /// empties the assertions here — measured, not hypothesised: it is how
    /// `the_scan_deadline_degrades_one_partition_to_zero_rows` started failing
    /// only when run alongside
    /// `a_deadlined_partition_releases_the_permit_for_the_next_scan`.
    fn events_with_message(
        events: &Arc<Mutex<Vec<CapturedEvent>>>,
        message: &str,
    ) -> Vec<CapturedEvent> {
        events
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|event| event.message == message)
            .cloned()
            .collect()
    }

    #[tokio::test]
    async fn items_partitions_stream_independently_and_stamp_status() {
        let server = MockFeedServer::start(|req| match req.path.as_str() {
            "/a.xml" => MockResponse::xml(RSS2_MINIMAL),
            _ => MockResponse::status(500),
        })
        .await;
        let engine = Arc::new(test_engine(
            &feed_urls(&server, &[("a", "/a.xml"), ("b", "/b.xml")]),
            |_| {},
        ));
        let exec = items_exec(Arc::clone(&engine), &["a", "b"], None, None);
        assert_eq!(exec.properties().partitioning.partition_count(), 2);

        let a = collect_stream(exec.execute(0, ctx())).await;
        let b = collect_stream(exec.execute(1, ctx())).await;

        assert_eq!(total_rows(&a), 1, "the healthy feed serves its window");
        assert_eq!(str_col(&a[0], "feed"), vec!["a"]);
        assert_eq!(str_col(&a[0], "window_status"), vec!["fresh"]);
        assert!(
            b.is_empty(),
            "the 500 feed has no window to serve, and degrades to zero rows rather than \
             failing the partition"
        );
        assert_eq!(
            str_col(&engine.feeds_row("b"), "last_status"),
            vec!["error"],
            "the failure is data on the feeds table, not a scan error"
        );
    }

    /// The cheap pre-check: once the LIMIT is met, a later partition does not
    /// call `serve_feed` at all.
    #[tokio::test]
    async fn limit_satisfied_stops_launching_fetches() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = Arc::new(test_engine(
            &feed_urls(&server, &[("a", "/a"), ("b", "/b"), ("c", "/c")]),
            |config| config.max_concurrent = 1,
        ));
        let exec = items_exec(Arc::clone(&engine), &["a", "b", "c"], None, Some(1));

        // Drained sequentially, so the counter's state at each partition's
        // first poll is deterministic.
        let mut rows = 0;
        for partition in 0..3 {
            rows += total_rows(&collect_stream(exec.execute(partition, ctx())).await);
        }

        assert_eq!(rows, 1);
        assert_eq!(server.requests().len(), 1, "one fetch for one row of LIMIT");
        for feed in ["b", "c"] {
            assert_eq!(
                str_col(&engine.feeds_row(feed), "last_status"),
                vec!["never"],
                "a gated-off feed is neither fetched nor health-refreshed"
            );
        }
    }

    /// The half the pre-check cannot cover: a partition that passed the
    /// pre-check while nothing had been emitted and is already queued on the
    /// politeness permit when the LIMIT fills.
    ///
    /// One permit and two feeds, polled concurrently. `#[tokio::test]` builds a
    /// current-thread runtime (tokio-macros 2.7.0, `src/entry.rs:91`:
    /// `default_flavor` is `CurrentThread` when `is_test`) and `biased;` makes
    /// `join!` poll top to bottom (documented in tokio's `join!`: it "will cause
    /// `join` to poll the futures in the order they appear from top to bottom"),
    /// so the interleaving is fixed: partition 0 passes the pre-check and parks
    /// in its fetch holding the only permit; partition 1 passes the pre-check
    /// (nothing emitted yet) and parks on the semaphore; partition 0's row then
    /// lands and closes the gate. Only a gate re-read after the permit stops
    /// partition 1 — which is why `RssEngine` re-reads it there.
    #[tokio::test]
    async fn a_queued_partition_is_stopped_by_the_gate_after_the_permit() {
        let server = MockFeedServer::start(|_| {
            MockResponse::xml(RSS2_MINIMAL).with_delay(Duration::from_millis(50))
        })
        .await;
        let engine = Arc::new(test_engine(
            &feed_urls(&server, &[("a", "/a.xml"), ("b", "/b.xml")]),
            |config| config.max_concurrent = 1,
        ));
        let exec = items_exec(Arc::clone(&engine), &["a", "b"], None, Some(1));

        let (a, b) = tokio::join!(
            biased;
            collect_stream(exec.execute(0, ctx())),
            collect_stream(exec.execute(1, ctx())),
        );

        assert_eq!(total_rows(&a), 1);
        assert!(b.is_empty(), "the queued partition serves nothing");
        let paths: Vec<String> = server
            .requests()
            .iter()
            .map(|request| request.path.clone())
            .collect();
        assert_eq!(
            paths,
            vec!["/a.xml".to_string()],
            "the queued partition's fetch must never be launched"
        );
        assert_eq!(
            str_col(&engine.feeds_row("b"), "last_status"),
            vec!["never"]
        );
    }

    /// `SELECT count(*)` reaches exec as `Some(vec![])`. A zero-column batch
    /// that lost its row count would make the count wrong.
    #[tokio::test]
    async fn empty_projection_preserves_row_count() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = Arc::new(test_engine(&feed_urls(&server, &[("a", "/a.xml")]), |_| {}));
        let exec = items_exec(engine, &["a"], Some(vec![]), None);
        assert_eq!(exec.schema().fields().len(), 0);

        let batches = collect_stream(exec.execute(0, ctx())).await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 0);
        assert_eq!(
            batches[0].num_rows(),
            1,
            "a zero-column batch must still carry the window's row count"
        );
        assert_eq!(
            batches[0].schema(),
            exec.schema(),
            "the emitted batch's schema is the one the plan advertises, metadata included"
        );
    }

    #[tokio::test]
    async fn feeds_kind_never_fetches() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = Arc::new(test_engine(
            &feed_urls(&server, &[("a", "/a.xml"), ("b", "/b.xml")]),
            |_| {},
        ));
        let exec = RssScanExec::new(
            engine,
            RssTableKind::Feeds,
            vec!["a".to_string(), "b".to_string()],
            None,
            None,
        )
        .expect("build the feeds exec");
        assert_eq!(exec.schema(), feeds_schema());

        let a = collect_stream(exec.execute(0, ctx())).await;
        let b = collect_stream(exec.execute(1, ctx())).await;

        assert_eq!(total_rows(&a), 1);
        assert_eq!(total_rows(&b), 1);
        assert_eq!(str_col(&a[0], "name"), vec!["a"]);
        assert_eq!(str_col(&a[0], "last_status"), vec!["never"]);
        assert_eq!(str_col(&b[0], "name"), vec!["b"]);
        assert!(
            server.requests().is_empty(),
            "a feeds scan issues no requests"
        );
    }

    #[tokio::test]
    async fn projection_prunes_columns() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = Arc::new(test_engine(&feed_urls(&server, &[("a", "/a.xml")]), |_| {}));
        let exec = items_exec(engine, &["a"], Some(vec![0, WINDOW_STATUS_IDX]), None);
        let schema = exec.schema();
        let names: Vec<&str> = schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect();
        assert_eq!(names, vec!["feed", "window_status"]);

        let batches = collect_stream(exec.execute(0, ctx())).await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(batches[0].schema(), exec.schema());
        assert_eq!(str_col(&batches[0], "feed"), vec!["a"]);
        assert_eq!(str_col(&batches[0], "window_status"), vec!["fresh"]);
    }

    /// The scan deadline degrades the partition that outran it, and nothing
    /// else: no error, and the other partitions are untouched.
    ///
    /// Real time with a tight bound and an explicit outer timeout, because
    /// `start_paused` and the mock server's real socket I/O do not mix (Task 4).
    /// `scan_timeout_seconds: 1` against the helper's 5-second request timeout
    /// makes the scan deadline the bound that fires; in production the
    /// per-request timeout is usually the shorter of the two, which is why this
    /// is a backstop for the aggregate rather than the common path.
    #[tokio::test]
    async fn the_scan_deadline_degrades_one_partition_to_zero_rows() {
        let server = MockFeedServer::start(|_| {
            MockResponse::xml(RSS2_MINIMAL).with_delay(Duration::from_secs(3))
        })
        .await;
        let engine = Arc::new(test_engine(
            &feed_urls(&server, &[("a", "/a.xml")]),
            |config| config.scan_timeout_seconds = 1,
        ));
        let exec = items_exec(Arc::clone(&engine), &["a"], None, None);
        let (_guard, events) = capture_events();

        // A regression here would hang, so bound the wait and fail instead.
        let batches = tokio::time::timeout(
            Duration::from_secs(20),
            collect_stream(exec.execute(0, ctx())),
        )
        .await
        .expect("the partition must end at its deadline rather than hang");

        assert!(
            batches.is_empty(),
            "a partition past the scan deadline emits zero rows"
        );
        assert_eq!(
            server.requests().len(),
            1,
            "the fetch was launched and then abandoned, so it was the deadline that cut it"
        );
        let warns = events_with_message(&events, "rss scan deadline reached");
        assert_eq!(warns.len(), 1, "one warning for the degraded partition");
        assert_eq!(warns[0].level, tracing::Level::WARN);
        assert_eq!(warns[0].field("feed"), Some("a"));
        assert_eq!(
            str_col(&engine.feeds_row("a"), "last_status"),
            vec!["never"],
            "an abandoned fetch writes no health state"
        );
    }

    /// `scan_timeout_seconds` is stored raw by `RssConfig` (validation only
    /// rejects `0`), and this is the first code turning it into an `Instant`.
    #[tokio::test]
    async fn an_absurd_scan_timeout_is_clamped_rather_than_overflowing() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = Arc::new(test_engine(
            &feed_urls(&server, &[("a", "/a.xml")]),
            |config| config.scan_timeout_seconds = u64::MAX,
        ));
        let (_guard, events) = capture_events();
        let exec = items_exec(engine, &["a"], None, None);

        assert_eq!(total_rows(&collect_stream(exec.execute(0, ctx())).await), 1);
        // The clamp is silent otherwise: an operator who configured a longer
        // budget than they get has to be able to see that from the log.
        let clamped = events_with_message(
            &events,
            "rss scan_timeout_seconds clamped to the exec layer's ceiling",
        );
        assert_eq!(clamped.len(), 1, "one warning per plan built");
        assert_eq!(clamped[0].level, tracing::Level::WARN);
        assert_eq!(
            clamped[0].field("configured_scan_timeout_seconds"),
            Some(u64::MAX.to_string().as_str())
        );
        assert_eq!(
            clamped[0].field("effective_scan_timeout_seconds"),
            Some(MAX_SCAN_TIMEOUT.as_secs().to_string().as_str())
        );
    }

    /// A LIMIT of zero closes the gate before anything is served, so no
    /// partition fetches at all.
    #[tokio::test]
    async fn a_zero_limit_closes_the_gate_for_every_partition() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = Arc::new(test_engine(
            &feed_urls(&server, &[("a", "/a.xml"), ("b", "/b.xml")]),
            |_| {},
        ));
        let exec = items_exec(Arc::clone(&engine), &["a", "b"], None, Some(0));

        for partition in 0..2 {
            assert!(
                collect_stream(exec.execute(partition, ctx()))
                    .await
                    .is_empty(),
                "partition {partition} serves nothing under LIMIT 0"
            );
        }
        assert!(server.requests().is_empty(), "and fetches nothing");
        for feed in ["a", "b"] {
            assert_eq!(
                str_col(&engine.feeds_row(feed), "last_status"),
                vec!["never"]
            );
        }
    }

    /// `count(*) FROM feeds` is the same empty projection down the other branch
    /// of `execute`, and it must not lose the health row either.
    #[tokio::test]
    async fn empty_projection_over_feeds_preserves_row_count() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = Arc::new(test_engine(&feed_urls(&server, &[("a", "/a.xml")]), |_| {}));
        let exec = RssScanExec::new(
            engine,
            RssTableKind::Feeds,
            vec!["a".to_string()],
            Some(vec![]),
            None,
        )
        .expect("build the feeds exec");

        let batches = collect_stream(exec.execute(0, ctx())).await;
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 0);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[0].schema(), exec.schema());
        assert!(server.requests().is_empty());
    }

    /// A partition dropped at the scan deadline gives its politeness permit
    /// back, so the *next* scan of the same source can still fetch.
    ///
    /// Two plans over one engine with one permit: the first is deadlined mid-fetch
    /// and the second, built afterwards, gets its own deadline — which is why
    /// this needs two plans rather than two partitions of one. A leaked permit
    /// parks the second scan forever (a semaphore acquire has no timeout of its
    /// own), so the wait is bounded and fails instead of hanging.
    #[tokio::test]
    async fn a_deadlined_partition_releases_the_permit_for_the_next_scan() {
        let server = MockFeedServer::start(|req| match req.path.as_str() {
            "/slow.xml" => MockResponse::xml(RSS2_MINIMAL).with_delay(Duration::from_secs(3)),
            _ => MockResponse::xml(RSS2_MINIMAL),
        })
        .await;
        let engine = Arc::new(test_engine(
            &feed_urls(&server, &[("slow", "/slow.xml"), ("fast", "/fast.xml")]),
            |config| {
                config.scan_timeout_seconds = 1;
                config.max_concurrent = 1;
            },
        ));

        // The guard is mandatory, not decorative — see `events_with_message`.
        let (_guard, events) = capture_events();
        let deadlined = items_exec(Arc::clone(&engine), &["slow"], None, None);
        assert!(
            collect_stream(deadlined.execute(0, ctx())).await.is_empty(),
            "the slow feed outruns the scan deadline"
        );
        let warns = events_with_message(&events, "rss scan deadline reached");
        assert_eq!(warns.len(), 1);
        assert_eq!(warns[0].field("feed"), Some("slow"));

        // A second scan, so a second deadline measured from now.
        let next = items_exec(Arc::clone(&engine), &["fast"], None, None);
        let batches = tokio::time::timeout(
            Duration::from_secs(20),
            collect_stream(next.execute(0, ctx())),
        )
        .await
        .expect("a leaked permit would park this scan forever");
        assert_eq!(total_rows(&batches), 1);
        assert_eq!(
            str_col(&engine.feeds_row("fast"), "last_status"),
            vec!["fresh"]
        );
    }

    /// `ScanShared` must not survive re-execution: DataFusion re-executes a plan
    /// object through `reset_state` (`RecursiveQueryExec` does it once per
    /// iteration), and a carried-over counter would make every iteration after
    /// the first serve zero rows without a request.
    ///
    /// Asserts the behaviour — rows served, a request made — rather than the
    /// identity of the rebuilt state, so it still holds if the rebuild changes
    /// shape.
    #[tokio::test]
    async fn reset_state_rebuilds_a_scan_whose_limit_was_already_satisfied() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        // ttl 0, so the second execution is a live fetch rather than a cache hit
        // — the point is that a request is issued at all.
        let engine = Arc::new(test_engine(
            &feed_urls(&server, &[("a", "/a.xml")]),
            |config| {
                config.ttl_seconds = 0;
            },
        ));
        let exec: Arc<dyn ExecutionPlan> =
            Arc::new(items_exec(Arc::clone(&engine), &["a"], None, Some(1)));

        assert_eq!(
            total_rows(&collect_stream(exec.execute(0, ctx())).await),
            1,
            "the first execution fills the LIMIT"
        );
        assert_eq!(server.requests().len(), 1);
        assert!(
            collect_stream(Arc::clone(&exec).execute(0, ctx()))
                .await
                .is_empty(),
            "re-executing the same plan object sees its own satisfied LIMIT"
        );
        assert_eq!(server.requests().len(), 1, "and issues no request");

        let reset = Arc::clone(&exec)
            .reset_state()
            .expect("reset_state rebuilds the plan");
        assert_eq!(
            total_rows(&collect_stream(reset.execute(0, ctx())).await),
            1,
            "the reset plan serves again: its counter and deadline are fresh"
        );
        assert_eq!(
            server.requests().len(),
            2,
            "and it actually fetched rather than replaying anything"
        );
    }

    #[test]
    fn scan_deadline_clamps_to_the_ceiling() {
        let now = Instant::now();
        assert_eq!(
            scan_deadline(now, Duration::from_secs(30)),
            now + Duration::from_secs(30)
        );
        assert_eq!(scan_deadline(now, Duration::MAX), now + MAX_SCAN_TIMEOUT);
    }

    /// Task 13 prunes `WHERE feed = 'nope'` to an empty feed list, which must
    /// still be a legal single-partition plan.
    #[tokio::test]
    async fn an_empty_feed_list_is_one_partition_serving_nothing() {
        let exec = items_exec(offline_engine(&["a"]), &[], None, None);
        assert_eq!(exec.properties().partitioning.partition_count(), 1);
        assert!(collect_stream(exec.execute(0, ctx())).await.is_empty());
    }

    #[tokio::test]
    async fn executing_a_partition_past_the_last_feed_is_an_internal_error() {
        let exec = items_exec(offline_engine(&["a", "b"]), &["a", "b"], None, None);
        // Not `expect_err`: a `SendableRecordBatchStream` is not `Debug`.
        let message = match exec.execute(2, ctx()) {
            Ok(_) => panic!("there is no third partition to execute"),
            Err(error) => error.to_string(),
        };
        assert!(
            message.contains("RssScanExec has 2 partitions, got partition 2"),
            "the error names the bound it broke: {message}"
        );
    }

    #[tokio::test]
    async fn with_new_children_rejects_children() {
        let exec: Arc<RssScanExec> =
            Arc::new(items_exec(offline_engine(&["a"]), &["a"], None, None));
        let child: Arc<dyn ExecutionPlan> = Arc::clone(&exec) as Arc<dyn ExecutionPlan>;
        let error = Arc::clone(&exec)
            .with_new_children(vec![child])
            .expect_err("a leaf plan takes no children");
        let message = error.to_string();
        assert!(
            message.contains("RssScanExec is a leaf plan and takes no children"),
            "the error says why: {message}"
        );
        assert!(
            Arc::clone(&exec).with_new_children(vec![]).is_ok(),
            "an empty child list is the identity"
        );
    }

    #[tokio::test]
    async fn an_out_of_range_projection_is_rejected_at_construction() {
        let error = RssScanExec::new(
            offline_engine(&["a"]),
            RssTableKind::Items,
            vec!["a".to_string()],
            Some(vec![99]),
            None,
        )
        .expect_err("99 is not a column of items");
        let message = error.to_string();
        assert!(
            message.contains("project index 99 out of bounds"),
            "the error names the bad index: {message}"
        );
    }

    /// A scan advertises no statistics. `items` genuinely cannot know its row
    /// count before fetching, and `feeds` must not claim one either: both tables
    /// go through this one plan, and a row count is only knowable for `feeds`
    /// because a subscription has exactly one health row — a coincidence, not a
    /// property worth teaching the optimizer. Measured against DataFusion 52,
    /// whose `ExecutionPlan` default is `Statistics::new_unknown`
    /// (datafusion-physical-plan 52.5.0, `src/execution_plan.rs:485-500`); this
    /// test is what keeps that inherited default from changing unnoticed.
    #[tokio::test]
    async fn a_scan_advertises_no_row_count() {
        for kind in [RssTableKind::Items, RssTableKind::Feeds] {
            let exec = RssScanExec::new(
                offline_engine(&["a", "b"]),
                kind,
                vec!["a".to_string(), "b".to_string()],
                None,
                None,
            )
            .expect("build the exec");
            let stats = exec.partition_statistics(None).expect("plan statistics");
            assert_eq!(stats.num_rows, Precision::Absent, "{kind:?}");
            let partition = exec.partition_statistics(Some(1)).expect("partition stats");
            assert_eq!(partition.num_rows, Precision::Absent, "{kind:?}");
        }
    }

    #[tokio::test]
    async fn display_names_the_kind_feed_count_and_limit() {
        let exec = items_exec(
            offline_engine(&["a", "b", "c"]),
            &["a", "b", "c"],
            None,
            Some(1),
        );
        let plan: Arc<dyn ExecutionPlan> = Arc::new(exec);
        let display = datafusion::physical_plan::displayable(plan.as_ref())
            .one_line()
            .to_string();
        assert_eq!(
            display.trim_end(),
            "RssScanExec: kind=items feeds=3 limit=Some(1)"
        );
    }
}
