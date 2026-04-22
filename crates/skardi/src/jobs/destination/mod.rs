//! Destination writers — where a job's rows go.
//!
//! Backends live in their own submodules:
//!
//! * [`lance::LanceDestination`] — writes to a Lance dataset on disk; commit-at-end
//!   atomicity.
//! * [`sql_dml::SqlDmlDestination`] — `INSERT INTO <table> SELECT ...` (or
//!   `DELETE FROM`+`INSERT` for `overwrite`) against a federated DB table.
//!
//! A third "kind" — Iceberg — is reserved for v1.1; when it lands it will be
//! its own submodule here.

use anyhow::Result;
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use std::sync::Arc;

use super::definition::DestinationMode;

mod lance;
mod sql_dml;

pub use lance::LanceDestination;
pub use sql_dml::SqlDmlDestination;

/// Per-destination outcome — how many rows landed and, when available, the
/// version identifier the destination assigned to this commit.
#[derive(Debug, Clone)]
pub struct WriteOutcome {
    pub rows_written: u64,
    pub snapshot_id: Option<String>,
}

/// A destination is either a lake (has commit semantics, schema on write)
/// or a DB (schema out-of-band, writes via `INSERT INTO`). The executor
/// branches on this for the pre-flight check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobDestinationKind {
    Lake,
    Db,
}

/// A sink that accepts a fully-materialized batch set and commits it.
///
/// Jobs are batch-only in the MVP, so the writer takes a `Vec<RecordBatch>`
/// rather than a stream. The executor collects the DataFusion output
/// in-memory (first shipable version) and hands it to the destination.
/// Streaming is a v1.1 concern.
#[async_trait]
pub trait JobDestination: Send + Sync {
    /// Classification used by the pre-flight resolver.
    fn kind(&self) -> JobDestinationKind;

    /// Does the destination already exist in durable storage?
    async fn exists(&self) -> Result<bool>;

    /// Fetch the destination's Arrow schema if it exists (used by the
    /// submit-time pre-flight to diff against the query's output schema).
    async fn schema(&self) -> Result<Option<Arc<Schema>>>;

    /// Commit a batch of rows with the chosen mode.
    async fn write(&self, batches: Vec<RecordBatch>, mode: DestinationMode)
    -> Result<WriteOutcome>;
}

/// Quote a DataFusion table reference so dotted identifiers survive parsing
/// intact. `foo.bar.baz` → `"foo"."bar"."baz"`. Internal quotes in segment
/// names are doubled per SQL rules.
pub(crate) fn quote_table_ref(name: &str) -> String {
    name.split('.')
        .map(|seg| format!("\"{}\"", seg.replace('"', "\"\"")))
        .collect::<Vec<_>>()
        .join(".")
}

/// Resolve the Arrow schema of an already-registered table via a
/// `SELECT * LIMIT 0`. Returns `Ok(None)` when DataFusion reports the table
/// is not registered; other errors propagate.
pub(crate) async fn lookup_table_schema(
    ctx: &SessionContext,
    table: &str,
) -> Result<Option<Arc<Schema>>> {
    let sql = format!("SELECT * FROM {} LIMIT 0", quote_table_ref(table));
    match ctx.sql(&sql).await {
        Ok(df) => Ok(Some(Arc::new(df.schema().as_arrow().clone()))),
        Err(e) => {
            // DataFusion surfaces multiple shapes for "table is not
            // registered": `not found`, `does not exist`,
            // `table 'foo' not found`, and `Unsupported compound
            // identifier '<quoted>'. Expected 1, 2 or 3 parts, got 4`
            // when the caller used a four-part identifier that couldn't
            // be resolved. Treat all of these as a missing destination
            // (the pre-flight will then branch on kind).
            let msg = e.to_string();
            let missing = msg.contains("not found")
                || msg.contains("does not exist")
                || msg.contains("Unsupported compound identifier");
            if missing {
                Ok(None)
            } else {
                Err(anyhow::anyhow!(
                    "Failed to resolve destination table '{}': {}",
                    table,
                    e
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quote_table_ref_handles_dotted_idents_and_quotes() {
        assert_eq!(quote_table_ref("plain"), "\"plain\"");
        assert_eq!(
            quote_table_ref("cat.schema.tbl"),
            "\"cat\".\"schema\".\"tbl\""
        );
        assert_eq!(quote_table_ref("has\"quote"), "\"has\"\"quote\"");
    }
}
