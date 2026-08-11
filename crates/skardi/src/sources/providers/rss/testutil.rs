//! Test support for the rss suites: the mock feed server (a thin flavor
//! over the crate-shared mock HTTP server), feed fixtures, and engine/batch
//! helpers.
//!
//! The server itself lives in [`crate::util::mock_http`] — hand-rolled over
//! `tokio::net::TcpListener`, speaking just enough HTTP/1.1 for `reqwest`.
//! Everything the fetcher's tests lean on is core behavior there: raw byte
//! bodies (so a gzip fixture can be served without a text detour), no
//! automatically injected response header (the tests control `etag`,
//! `last-modified`, `location`, `retry-after`, and `content-encoding`
//! precisely, including their absence), an artificial response delay for
//! the timeout path, and a body cut off mid-transfer for the body-phase
//! retry path. This module re-exports it under this suite's historical
//! names and adds the feed-flavored response constructor.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, RecordBatch, StringArray};
use datafusion::error::Result as DFResult;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;

use super::ResolvedSubscription;
use super::cache::MemoryFeedCache;
use super::config::{FeedSubscription, RssConfig, inline_config};
use super::engine::{CACHE_MAX_BYTES, RssEngine};
use super::fetch::FeedFetcher;

/// A well-formed RSS 2.0 document carrying every `channel`-level field the
/// dialect requires plus exactly one item, so a batch built from it has one
/// row and `conformance_notes` comes out empty. The baseline body for the
/// engine's tests; a test that needs a *defect* spells the defect out inline
/// rather than editing this.
pub(crate) const RSS2_MINIMAL: &str = concat!(
    r#"<rss version="2.0"><channel>"#,
    r#"<title>Minimal Feed</title>"#,
    r#"<link>https://feed.example/</link>"#,
    r#"<description>A minimal feed.</description>"#,
    r#"<item><guid>https://feed.example/1</guid><title>First post</title>"#,
    r#"<link>https://feed.example/1</link></item>"#,
    r#"</channel></rss>"#,
);

/// The `Utf8` column named `name`, or a panic naming the column — every
/// caller is a test asserting against a fixed schema, so a missing or
/// retyped column is a bug in the test's expectations either way.
fn utf8_column<'a>(batch: &'a RecordBatch, name: &str) -> &'a StringArray {
    let index = batch
        .schema()
        .index_of(name)
        .unwrap_or_else(|e| panic!("batch has no column {name:?}: {e}"));
    batch
        .column(index)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("column {name:?} is not Utf8"))
}

/// Read a `Utf8` column as owned strings, panicking on a NULL — for the
/// columns the schema declares non-nullable (`window_status`,
/// `last_status`, `feed`, …).
pub(crate) fn str_col(batch: &RecordBatch, name: &str) -> Vec<String> {
    let column = utf8_column(batch, name);
    (0..column.len())
        .map(|row| {
            assert!(
                column.is_valid(row),
                "column {name:?} row {row} is NULL but the schema declares it non-nullable"
            );
            column.value(row).to_string()
        })
        .collect()
}

/// Read a nullable `Utf8` column, preserving NULLs — for `last_error`,
/// `dialect_declared`, `conformance_notes`, and friends, where the
/// difference between NULL and `""` is the assertion.
pub(crate) fn str_opt_col(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let column = utf8_column(batch, name);
    (0..column.len())
        .map(|row| {
            if column.is_valid(row) {
                Some(column.value(row).to_string())
            } else {
                None
            }
        })
        .collect()
}

/// `(name, url)` pairs pointing at `server`, from `(name, path)` pairs — the
/// subscription list shape [`test_engine`] takes.
pub(crate) fn feed_urls(server: &MockFeedServer, feeds: &[(&str, &str)]) -> Vec<(String, String)> {
    feeds
        .iter()
        .map(|(name, path)| ((*name).to_string(), format!("{}{path}", server.url())))
        .collect()
}

/// An [`RssEngine`] over `feeds` (`(name, url)` pairs, e.g. from
/// [`feed_urls`]), with `tune` applied to the spec-default config before the
/// engine is assembled — the seam for `ttl_seconds`, `max_concurrent`, and
/// `scan_timeout_seconds`.
///
/// The fetcher is built with no injected policy (`None`) — the OSS default,
/// no destination filtering — so the subscriptions can name
/// [`MockFeedServer`].
/// `request_timeout_seconds` is pulled down from the spec default (30) to 5
/// so a test that means to hit a *different* bound does not first have to
/// wait out a request timeout.
///
/// `engine.rs`'s own tests have a near-identical private helper. It cannot be
/// shared in either direction: it lives inside that module's `#[cfg(test)] mod
/// tests`, which nothing outside the module can name. This copy is the one the
/// exec layer's tests build on.
pub(crate) fn test_engine(
    feeds: &[(String, String)],
    tune: impl FnOnce(&mut RssConfig),
) -> RssEngine {
    let subscriptions: Vec<ResolvedSubscription> = feeds
        .iter()
        .map(|(name, url)| ResolvedSubscription {
            name: name.clone(),
            url: url.clone(),
        })
        .collect();
    let mut config = inline_config(
        subscriptions
            .iter()
            .map(|sub| FeedSubscription {
                url: sub.url.clone(),
                name: Some(sub.name.clone()),
            })
            .collect(),
    );
    config.request_timeout_seconds = 5;
    tune(&mut config);
    let fetcher = FeedFetcher::new(
        None,
        Duration::from_secs(config.request_timeout_seconds),
        config.max_response_bytes,
        config.user_agent.clone(),
    )
    .expect("build the test fetcher");
    let cache = Arc::new(MemoryFeedCache::new(
        CACHE_MAX_BYTES,
        subscriptions.len() + 8,
    ));
    RssEngine::with_parts(
        "rss_test".to_string(),
        subscriptions,
        &config,
        fetcher,
        cache,
    )
}

/// Drain one partition's stream to its batches.
///
/// Panics on an `Err` item rather than returning it: no `rss` partition may
/// ever surface an error — a dead or slow feed degrades to zero rows — so an
/// error here is a failed assertion, and the panic message says which.
pub(crate) async fn collect_stream(
    stream: DFResult<SendableRecordBatchStream>,
) -> Vec<RecordBatch> {
    let mut stream = stream.expect("execute returned a stream");
    let mut batches = Vec::new();
    while let Some(item) = stream.next().await {
        batches.push(item.expect("an rss partition must never yield an error"));
    }
    batches
}

/// Total rows across a partition's batches.
pub(crate) fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

pub(crate) use crate::util::mock_http::{
    MockHttpServer as MockFeedServer, MockResponse, RecordedRequest,
};

impl MockResponse {
    /// `200 OK`, `content-type: application/xml`, UTF-8 body — the common
    /// case for a well-formed feed response.
    pub(crate) fn xml(body: &str) -> Self {
        Self::new(200, body.as_bytes().to_vec()).with_header("content-type", "application/xml")
    }
}
