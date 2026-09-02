//! The dedicated writer task: drains the bounded channel, batching up
//! to [`FLUSH_BATCH_ROWS`] rows per multi-row INSERT, each flush bounded at
//! [`FLUSH_TIMEOUT`]. A failed or timed-out flush drops its batch, counts it
//! (`ledger_insert_failures_total{reason="pg"}`), and logs the request ids —
//! loss is accepted by design and never silent (see the module doc for the
//! contract).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use sqlx::PgPool;
use tokio::sync::mpsc::Receiver;
use tokio::sync::watch;

use super::{FLUSH_BATCH_ROWS, FLUSH_TIMEOUT, LedgerRow, METRICS, WriterControl, queries};

pub(crate) async fn run(
    pool: PgPool,
    mut rx: Receiver<LedgerRow>,
    control: Arc<WriterControl>,
    done: watch::Sender<bool>,
) {
    // The completion latch every shutdown() caller awaits. A drop guard,
    // not a final statement, so it fires on EVERY exit: graceful return,
    // panic unwind, and task drop at runtime teardown.
    struct Done(watch::Sender<bool>);
    impl Drop for Done {
        fn drop(&mut self) {
            let _ = self.0.send(true);
        }
    }
    let _done = Done(done);
    let mut batch: Vec<LedgerRow> = Vec::with_capacity(FLUSH_BATCH_ROWS);
    loop {
        batch.clear();
        // Block for the first row, then drain whatever else is queued up to
        // the batch bound — natural batching under load, no ticker at rest.
        // The drain signal closes the receiver: senders start failing fast
        // (counted by `record` as channel losses) while recv_many hands over
        // everything already buffered, then returns 0 and the task exits —
        // the graceful path never abandons an accepted row.
        tokio::select! {
            n = rx.recv_many(&mut batch, FLUSH_BATCH_ROWS) => {
                if n == 0 {
                    // Channel closed and drained: every sender is gone, or a
                    // shutdown finished handing over the backlog.
                    return;
                }
                flush(&pool, &batch).await;
            }
            _ = control.drain.notified() => {
                rx.close();
            }
        }
    }
}

async fn flush(pool: &PgPool, batch: &[LedgerRow]) {
    let mut qb: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(queries::INSERT_HEAD);
    qb.push_values(batch, |mut b, row| {
        // Binding order = queries::INSERT_HEAD column order.
        b.push_bind(&row.org_id)
            .push_bind(&row.workspace_id)
            .push_bind(&row.user_id)
            .push_bind(&row.request_id)
            .push_bind(&row.session_id)
            .push_bind(row.created_at)
            .push_bind(row.finished_at)
            .push_bind(&row.sql)
            .push_bind(row.sql_truncated)
            .push_bind(&row.ai_context)
            .push_bind(row.statement_kind)
            .push_bind(row.max_rows)
            .push_bind(row.status.as_str())
            .push_bind(row.row_count)
            .push_bind(&row.error);
    });
    let fut = qb.build().execute(pool);
    let outcome = tokio::time::timeout(FLUSH_TIMEOUT, fut).await;
    let err: Option<String> = match outcome {
        Ok(Ok(_)) => None,
        Ok(Err(e)) => Some(e.to_string()),
        Err(_) => Some(format!("flush timed out after {FLUSH_TIMEOUT:?}")),
    };
    if let Some(err) = err {
        METRICS
            .insert_failures_pg
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        let request_ids: Vec<&str> = batch.iter().map(|r| r.request_id.as_str()).collect();
        // The error string is a driver/connectivity message, not row content;
        // request ids are gateway-minted UUIDs. Nothing caller-supplied here.
        tracing::warn!(
            dropped = batch.len(),
            request_ids = ?request_ids,
            "ledger flush failed; batch dropped: {err}"
        );
    }
}
