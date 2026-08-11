//! External-crate proof of the RSS egress-injection seam.
//!
//! This file compiles as its own crate against `skardi`'s public API — the
//! same position an embedder (Skardi Cloud, or an operator using Skardi as a
//! library) occupies. It exists to keep the cloud egress design's claim
//! honest ("no OSS source change is required — only the injected object",
//! `docs/superpowers/specs/2026-08-03-rss-cloud-egress-design.md`) by proving
//! what no in-crate test can:
//!
//! - `EgressPolicy`, `EgressReason`, and `register_rss_tables_with_policy`
//!   are reachable from outside the crate — nothing in the injection path
//!   leans on `pub(crate)` visibility;
//! - the trait's bounds admit an implementation written outside the crate,
//!   used as an `Arc<dyn EgressPolicy>` trait object;
//! - a refusal from that external policy surfaces through plain SQL —
//!   `feeds.last_status`/`last_error` — with zero rows in `items`.
//!
//! The behavioral matrix (redirect hops, warm-cache denial, the failure
//! fuse, mixed healthy/denied feeds) stays with the in-crate suites, which
//! reach the private mock server. This test needs no server at all: the
//! subscription's host is an IP literal the policy refuses, so the fetcher
//! consults the policy and denies the fetch before attempting any
//! connection.

#![cfg(feature = "rss")]

use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Array, RecordBatch, StringArray};
use datafusion::prelude::SessionContext;
use serde_json::json;
use skardi::sources::hierarchy::HierarchyLevel;
use skardi::sources::providers::rss::{
    EgressPolicy, EgressReason, RssConfig, register_rss_tables_with_policy,
};

/// Backstop on each query: the denial happens before any connection is
/// attempted, so both queries return almost immediately — the ceiling only
/// turns a regression that parks the scan into a named failure instead of a
/// hung test binary.
const QUERY_CEILING: Duration = Duration::from_secs(60);

/// The embedder-side implementation: refuses loopback, allows everything
/// else. OSS ships no such policy — writing one *here*, outside the crate,
/// against the re-exported contract alone, is the point of this test.
#[derive(Debug)]
struct DenyLoopback;

impl EgressPolicy for DenyLoopback {
    fn check_ip(&self, ip: IpAddr) -> Result<(), EgressReason> {
        if ip.is_loopback() {
            Err("test-loopback".into())
        } else {
            Ok(())
        }
    }
}

/// Run one query to completion, under [`QUERY_CEILING`].
async fn sql(ctx: &SessionContext, query: &str) -> Vec<RecordBatch> {
    tokio::time::timeout(QUERY_CEILING, async {
        ctx.sql(query)
            .await
            .unwrap_or_else(|e| panic!("plan {query:?}: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("execute {query:?}: {e}"))
    })
    .await
    .unwrap_or_else(|_| panic!("{query:?} did not finish within {QUERY_CEILING:?}"))
}

fn total_rows(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// Column `name` across `batches` as one string per row, `None` for nulls.
fn str_col(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|batch| {
            let col = batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("column {name} exists"))
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap_or_else(|| panic!("column {name} is Utf8"));
            (0..col.len())
                .map(|i| (!col.is_null(i)).then(|| col.value(i).to_string()))
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn an_externally_implemented_policy_denies_through_the_public_seam() {
    // Port 9 (discard) is never connected to: the host is an IP literal, so
    // the fetcher checks the policy against it directly, and the policy
    // refuses it before any connect.
    let config: RssConfig = serde_json::from_value(json!({
        "feeds": [{ "url": "http://127.0.0.1:9/feed.xml", "name": "blocked" }]
    }))
    .expect("a minimal inline config deserializes with spec defaults");

    let mut ctx = SessionContext::new();
    register_rss_tables_with_policy(
        &mut ctx,
        "ext",
        Some(&config),
        false,
        HierarchyLevel::Catalog,
        Arc::new(DenyLoopback),
    )
    .await
    .expect("registration through the public seam succeeds");

    // The `items` scan triggers the (refused) fetch. A denial degrades the
    // feed rather than failing the query: zero rows, not an error.
    let items = sql(&ctx, "SELECT guid FROM ext.main.items").await;
    assert_eq!(
        total_rows(&items),
        0,
        "a refused destination serves no rows"
    );

    // The refusal is queryable, reason verbatim, from the health table.
    let feeds = sql(
        &ctx,
        "SELECT name, last_status, last_error FROM ext.main.feeds",
    )
    .await;
    assert_eq!(total_rows(&feeds), 1);
    assert_eq!(str_col(&feeds, "name"), vec![Some("blocked".into())]);
    assert_eq!(str_col(&feeds, "last_status"), vec![Some("error".into())]);
    let error = str_col(&feeds, "last_error")[0]
        .clone()
        .expect("last_error records the refusal");
    assert!(
        error.contains("egress blocked"),
        "last_error names the refusal: {error}"
    );
    assert!(
        error.contains("test-loopback"),
        "the policy's reason string is stored verbatim: {error}"
    );
}
