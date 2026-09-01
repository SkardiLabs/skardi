//! Live-Postgres integration tests for the query-audit Postgres backend —
//! the SQLite suite's key invariants, replayed against a real server.
//!
//! Gated like the AGE live tests: `#[ignore]` by default, run by CI's
//! integration step (and locally) with
//!
//! ```sh
//! SKARDI_QUERY_AUDIT_LIVE_URL=postgres://user:pass@127.0.0.1:5432/postgres \
//!     cargo test -p skardi-query-audit -- --ignored
//! ```
//!
//! The URL points at a SERVER (any database); each test creates its own
//! throwaway database on it, so tests are independent and re-runs are clean.

use serde_json::json;
use skardi_query_audit::{QueryAuditStatus, QueryAuditStore, QueryIdentity};

const LIVE_URL_ENV: &str = "SKARDI_QUERY_AUDIT_LIVE_URL";

fn live_url() -> Option<String> {
    std::env::var(LIVE_URL_ENV)
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// A fresh database on the live server, plus the DSN pointing at it and a
/// raw pool for direct assertions. Unique per call so tests cannot see each
/// other's rows.
async fn fresh_db(url: &str) -> (String, sqlx::PgPool) {
    // Unique across THREE axes, because each has failed alone: a per-process
    // counter (two threads can read the same coarse-clock nanos), the pid
    // (nextest runs each test in its own process, where the counter resets),
    // and the nanos (so back-to-back suite runs never collide either).
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let db = format!(
        "qa_{}_{}_{}",
        std::process::id(),
        std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let boot = sqlx::PgPool::connect(url).await.expect("connect server");
    // Concurrent CREATE DATABASE calls serialize on the template lock and
    // the loser errors ("source database template1 is being accessed by
    // other users") — every test in this suite creates one, and nextest
    // runs them in separate processes, so retry rather than mutex.
    let mut attempts = 0;
    loop {
        match sqlx::raw_sql(&format!(r#"CREATE DATABASE "{db}""#))
            .execute(&boot)
            .await
        {
            Ok(_) => break,
            Err(e)
                if attempts < 50 && e.to_string().contains("is being accessed by other users") =>
            {
                attempts += 1;
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(e) => panic!("create test db: {e}"),
        }
    }
    boot.close().await;
    let mut parsed = url::Url::parse(url).expect("url");
    parsed.set_path(&format!("/{db}"));
    let dsn = parsed.to_string();
    let pool = sqlx::PgPool::connect(&dsn).await.expect("connect test db");
    (dsn, pool)
}

/// Concurrent FIRST boots against one fresh database must all succeed:
/// `CREATE TABLE/INDEX IF NOT EXISTS` is not race-safe in Postgres (the
/// loser of the catalog race dies on 42P07 and would crash-loop a
/// replica), so the boot DDL runs under an advisory lock. Eight parallel
/// opens are the regression canary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn concurrent_first_boots_are_race_free() {
    let Some(url) = live_url() else { return };
    let (dsn, _pool) = fresh_db(&url).await;
    // join_all, not spawn: the opens interleave at every await, so the DDL
    // statements are genuinely concurrent on the server side (spawn trips a
    // rustc higher-ranked-lifetime limitation on sqlx's `&mut conn`
    // executor borrows).
    let results =
        futures::future::join_all((0..8).map(|_| QueryAuditStore::open_postgres(&dsn))).await;
    for result in results {
        result.expect("every concurrent first boot must succeed");
    }
}

/// Round trip with identity and ai_context: every column lands and reads
/// back; `session_id` is denormalised out of the context for the index.
#[tokio::test]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn round_trip_all_columns_and_identity() {
    let Some(url) = live_url() else { return };
    let (dsn, _pool) = fresh_db(&url).await;
    let store = QueryAuditStore::open_postgres(&dsn).await.expect("open");

    let identity = QueryIdentity {
        request_id: Some("req-9c41".into()),
        org_id: Some("acme".into()),
        workspace_id: Some("ws-core".into()),
        user_id: Some("user:acme/alice".into()),
        run_id: None,
    };
    let ai = json!({"session_id": "sess-1", "purpose": "learn"});
    let id = store
        .record_started_for("SELECT 1", Some(&ai), 10, "query", Some(&identity))
        .await
        .expect("record");
    store
        .record_outcome(&id, QueryAuditStatus::Succeeded, Some(3), None)
        .await
        .expect("outcome");

    let record = store.get(&id).await.expect("get").expect("row");
    assert_eq!(record["sql"], json!("SELECT 1"));
    assert_eq!(record["session_id"], json!("sess-1"));
    assert_eq!(record["ai_context"]["purpose"], json!("learn"));
    assert_eq!(record["status"], json!("succeeded"));
    assert_eq!(record["row_count"], json!(3));
    assert_eq!(record["org_id"], json!("acme"));
    assert_eq!(record["workspace_id"], json!("ws-core"));
    assert!(record["run_id"].is_null());
    assert_eq!(store.count().await.expect("count"), 1);

    // The store never leaks the credential through its identity surfaces.
    assert!(!format!("{store:?}").contains("hunter2"));
    assert!(
        store.path().to_str().unwrap().starts_with("postgres://"),
        "{:?}",
        store.path()
    );
}

/// The `started`-only guard: outcome stamps are monotonic, so a second stamp
/// (or a stamp racing reconciliation) cannot resurrect a terminal row.
#[tokio::test]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn outcome_stamps_are_monotonic() {
    let Some(url) = live_url() else { return };
    let (dsn, _pool) = fresh_db(&url).await;
    let store = QueryAuditStore::open_postgres(&dsn).await.expect("open");

    let id = store
        .record_started("SELECT 1", None, 10, "query")
        .await
        .expect("record");
    store
        .record_outcome(&id, QueryAuditStatus::Succeeded, Some(1), None)
        .await
        .expect("first stamp");
    store
        .record_outcome(&id, QueryAuditStatus::Failed, None, Some("late"))
        .await
        .expect("late stamp is a no-op, not an error");
    let record = store.get(&id).await.expect("get").expect("row");
    assert_eq!(record["status"], json!("succeeded"));
    assert!(record["error"].is_null());
}

/// Reconcile is writer-scoped (review P1): several servers sharing one DSN
/// is the NATURAL Postgres topology, and server B's boot must not rewrite
/// server A's live in-flight rows to `unknown` — the monotonic outcome
/// guard would make that permanent. Ownerless NULL-writer rows stay
/// reconcilable by anyone.
#[tokio::test]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn reconcile_is_scoped_to_the_writer() {
    let Some(url) = live_url() else { return };
    let (dsn, pool) = fresh_db(&url).await;
    let a = QueryAuditStore::open_postgres(&dsn)
        .await
        .expect("open a")
        .with_writer_identity("server-a");
    let b = QueryAuditStore::open_postgres(&dsn)
        .await
        .expect("open b")
        .with_writer_identity("server-b");

    let live_on_a = a
        .record_started("SELECT pg_sleep(600)", None, 10, "query")
        .await
        .expect("a records");
    // A legacy pre-writer-column row: started, writer NULL.
    sqlx::query(
        "INSERT INTO query_audit (id, created_at, sql, max_rows, statement_kind, status) \
         VALUES ('legacy-1', '2020-01-01T00:00:00Z', 'SELECT 1', 10, 'query', 'started')",
    )
    .execute(&pool)
    .await
    .expect("stage legacy row");

    // B reboots mid-A's-query: it claims only the ownerless legacy row.
    assert_eq!(b.reconcile_orphaned("b restarted").await.expect("b"), 1);
    let row = a.get(&live_on_a).await.expect("get").expect("row");
    assert_eq!(row["status"], json!("started"), "A's live row untouched");

    // A's real outcome still lands — the failure mode this scoping kills is
    // exactly this stamp matching zero rows.
    a.record_outcome(&live_on_a, QueryAuditStatus::Succeeded, Some(1), None)
        .await
        .expect("a stamps");
    let row = a.get(&live_on_a).await.expect("get").expect("row");
    assert_eq!(row["status"], json!("succeeded"));
    assert_eq!(row["writer"], json!("server-a"));
}

/// Startup reconcile: rows left `started` by a crash rewrite to `unknown`
/// with the reason, terminal rows untouched — and the count is reported.
#[tokio::test]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn reconcile_rewrites_started_to_unknown() {
    let Some(url) = live_url() else { return };
    let (dsn, _pool) = fresh_db(&url).await;
    let store = QueryAuditStore::open_postgres(&dsn).await.expect("open");

    let orphan_a = store
        .record_started("SELECT 1", None, 10, "query")
        .await
        .expect("a");
    let orphan_b = store
        .record_started("SELECT 2", None, 10, "query")
        .await
        .expect("b");
    let finished = store
        .record_started("SELECT 3", None, 10, "query")
        .await
        .expect("c");
    store
        .record_outcome(&finished, QueryAuditStatus::Succeeded, Some(1), None)
        .await
        .expect("finish c");

    let n = store
        .reconcile_orphaned("server restarted before the query completed")
        .await
        .expect("reconcile");
    assert_eq!(n, 2);
    for id in [&orphan_a, &orphan_b] {
        let record = store.get(id).await.expect("get").expect("row");
        assert_eq!(record["status"], json!("unknown"));
        assert_eq!(
            record["error"],
            json!("server restarted before the query completed")
        );
    }
    let record = store.get(&finished).await.expect("get").expect("row");
    assert_eq!(record["status"], json!("succeeded"));
}

/// Retention: everything before the cutoff goes (across batch boundaries),
/// everything after stays.
#[tokio::test]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn prune_deletes_only_before_the_cutoff() {
    let Some(url) = live_url() else { return };
    let (dsn, pool) = fresh_db(&url).await;
    let store = QueryAuditStore::open_postgres(&dsn).await.expect("open");

    for i in 0..5 {
        store
            .record_started(&format!("SELECT {i}"), None, 10, "query")
            .await
            .expect("record");
    }
    // Age three of them past the cutoff by editing created_at directly —
    // the store deliberately has no API for backdating.
    sqlx::query(
        "UPDATE query_audit SET created_at = $1 WHERE id IN \
         (SELECT id FROM query_audit ORDER BY id ASC LIMIT 3)",
    )
    .bind((chrono::Utc::now() - chrono::Duration::days(30)).to_rfc3339())
    .execute(&pool)
    .await
    .expect("backdate");

    let pruned = store
        .prune_before(chrono::Utc::now() - chrono::Duration::days(7))
        .await
        .expect("prune");
    assert_eq!(pruned, 3);
    assert_eq!(store.count().await.expect("count"), 2);
}

/// Pipeline and job rows, the session listing's total order, and the
/// backfill flow: submitted → reconciled to `unknown` → listed as missing →
/// backfilled exactly once, status left `unknown` (the truth).
#[tokio::test]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn pipeline_job_rows_and_backfill() {
    let Some(url) = live_url() else { return };
    let (dsn, _pool) = fresh_db(&url).await;
    let store = QueryAuditStore::open_postgres(&dsn).await.expect("open");

    let p = store
        .record_pipeline_started("revenue", "3", Some("sess-9"))
        .await
        .expect("pipeline");
    let j = store
        .record_job_submitted("nightly", "7", Some("sess-9"))
        .await
        .expect("job");

    let session = store.list_by_session("sess-9").await.expect("list");
    assert_eq!(session.len(), 2);
    assert_eq!(session[0]["id"], json!(p.clone()));
    assert_eq!(session[0]["sql"], json!("revenue@3"));
    assert_eq!(session[0]["statement_kind"], json!("pipeline"));
    assert_eq!(session[1]["sql"], json!("nightly@7"));
    assert_eq!(session[1]["statement_kind"], json!("job"));

    // Crash before the outcome: the job row goes unknown with no run id...
    store.reconcile_orphaned("crash").await.expect("reconcile");
    let missing = store.job_rows_missing_run_id().await.expect("missing");
    assert_eq!(missing, vec![j.clone()]);
    // ...the linkage is recoverable exactly once...
    assert!(store.backfill_job_run_id(&j, "run-42").await.expect("fill"));
    assert!(
        !store
            .backfill_job_run_id(&j, "run-43")
            .await
            .expect("refill")
    );
    let record = store.get(&j).await.expect("get").expect("row");
    assert_eq!(record["job_run_id"], json!("run-42"));
    // ...and the status stays `unknown`: only the linkage was recovered.
    assert_eq!(record["status"], json!("unknown"));
}

/// A live job outcome stamps run id, status, and respects the guard.
#[tokio::test]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn job_outcome_bridges_to_the_run() {
    let Some(url) = live_url() else { return };
    let (dsn, _pool) = fresh_db(&url).await;
    let store = QueryAuditStore::open_postgres(&dsn).await.expect("open");

    let j = store
        .record_job_submitted("nightly", "7", None)
        .await
        .expect("job");
    store
        .record_job_outcome(&j, Some("run-1"), QueryAuditStatus::Succeeded, None)
        .await
        .expect("outcome");
    let record = store.get(&j).await.expect("get").expect("row");
    assert_eq!(record["status"], json!("succeeded"));
    assert_eq!(record["job_run_id"], json!("run-1"));
}

/// Opening twice is idempotent, and the open normalises the legacy
/// `statement_kind` casing a pre-#219 writer left behind — the same
/// reconcile-on-open the file backend does.
#[tokio::test]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn open_is_idempotent_and_normalises_legacy_casing() {
    let Some(url) = live_url() else { return };
    let (dsn, pool) = fresh_db(&url).await;
    let store = QueryAuditStore::open_postgres(&dsn).await.expect("open 1");
    let id = store
        .record_started("SELECT 1", None, 10, "query")
        .await
        .expect("record");
    // Stage a legacy-cased row the way an old binary would have left it.
    sqlx::query("UPDATE query_audit SET statement_kind = 'Query' WHERE id = $1")
        .bind(&id)
        .execute(&pool)
        .await
        .expect("stage legacy casing");

    let store = QueryAuditStore::open_postgres(&dsn).await.expect("open 2");
    let record = store.get(&id).await.expect("get").expect("row");
    assert_eq!(record["statement_kind"], json!("query"));
}

/// Boot-fatal: a configured audit ledger that cannot be reached refuses to
/// open — never a silent downgrade. (An unroutable port, so this needs no
/// live server; it is still `#[ignore]`d with the rest so the default test
/// run stays network-free.)
#[tokio::test]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn unreachable_postgres_is_boot_fatal() {
    let err = QueryAuditStore::open_postgres("postgres://u:hunter2@127.0.0.1:9/audit")
        .await
        .expect_err("must refuse");
    let msg = format!("{err:#}");
    assert!(msg.contains("query-audit"), "{msg}");
    assert!(
        !msg.contains("hunter2"),
        "the DSN must stay out of errors: {msg}"
    );
}
