//! Live-Postgres tests for the best-effort ledger module (`ledger::*`) —
//! the queued writer, the read page, and the RLS posture its consumer's
//! per-workspace roles rely on, against a real server.
//!
//! Gated exactly like `pg_live.rs`: `#[ignore]` by default, run with
//! `SKARDI_QUERY_AUDIT_LIVE_URL=postgres://… cargo test -- --ignored`.
//! Each test creates its own throwaway database and applies
//! [`queries::QUERY_LEDGER_MIGRATION_0001`] — the module runs no DDL itself.

use serde_json::{Value, json};
use skardi_query_audit::ledger::{self, PgLedger, RowDraft, RowStatus, queries, read};
use std::time::{Duration, Instant};

const LIVE_URL_ENV: &str = "SKARDI_QUERY_AUDIT_LIVE_URL";
const WS: &str = "ws-core";

fn live_url() -> Option<String> {
    std::env::var(LIVE_URL_ENV)
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// A fresh database with the ledger DDL applied. Same three-axis unique
/// name as `pg_live.rs`'s, for the same reasons.
async fn fresh_db(url: &str) -> (String, sqlx::PgPool) {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let db = format!(
        "lgr_{}_{}_{}",
        std::process::id(),
        std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let boot = sqlx::PgPool::connect(url).await.expect("connect server");
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
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => panic!("create test db: {e}"),
        }
    }
    boot.close().await;
    let mut parsed = url::Url::parse(url).expect("url");
    parsed.set_path(&format!("/{db}"));
    let dsn = parsed.to_string();
    let pool = sqlx::PgPool::connect(&dsn).await.expect("connect test db");
    sqlx::raw_sql(queries::QUERY_LEDGER_MIGRATION_0001)
        .execute(&pool)
        .await
        .expect("apply the ledger DDL");
    (dsn, pool)
}

fn draft(ws: &str, sql: &str, request_id: &str) -> RowDraft {
    RowDraft::capture(
        "acme",
        ws,
        "user:acme/u1",
        request_id,
        sql,
        Some(&json!({"session_id": "s-1", "purpose": "test"})),
        Some(100),
        1000,
    )
}

/// Queue-full contract, no server needed: a full channel DROPS the row and
/// COUNTS it — `record` never waits and never errors to the caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_queue_drops_and_counts_instead_of_blocking() {
    let pg = PgLedger::spawn_with_capacity("postgres://u:p@192.0.2.1:5432/skardi_ledger", 1)
        .expect("lazy pool");
    let before = ledger::METRICS
        .insert_failures_channel_full
        .load(std::sync::atomic::Ordering::Relaxed);

    let started = Instant::now();
    for i in 0..200 {
        let d = draft(WS, "SELECT 1", &format!("r-full-{i}"));
        pg.record(d.finish(RowStatus::Succeeded, Some(1), None));
    }
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "record must never block, even with the queue full"
    );
    let dropped = ledger::METRICS
        .insert_failures_channel_full
        .load(std::sync::atomic::Ordering::Relaxed)
        - before;
    assert!(dropped >= 100, "losses must be counted; got {dropped}");
}

/// Store round trip through the real writer: all three statuses land and
/// read back with filter semantics; the poisoned-batch clamp holds; an
/// 8 MiB statement is stored truncated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn store_round_trip_and_filters() {
    let Some(url) = live_url() else { return };
    let (dsn, pool) = fresh_db(&url).await;
    let pg = PgLedger::spawn(&dsn).expect("pool");

    pg.record(draft(WS, "SELECT 1", "r-ok").finish(RowStatus::Succeeded, Some(3), None));
    pg.record(draft(WS, "SELECT boom", "r-fail").finish(
        RowStatus::Failed,
        None,
        Some("execution-failed: boom".into()),
    ));
    // The poison case: max_rows beyond i32, plus an 8 MiB statement.
    let big = "S".repeat(8 * 1024 * 1024);
    let poison = RowDraft::capture(
        "acme",
        WS,
        "user:acme/u1",
        "r-refused",
        &big,
        None,
        Some(3_000_000_000),
        1000,
    );
    pg.record(poison.finish(RowStatus::Refused, None, Some("plan-error: nope".into())));

    // Drain: the first flush pays the batch window plus the lazy pool's
    // first real connect.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM query_ledger")
            .fetch_one(&pool)
            .await
            .expect("count");
        if n == 3 {
            break;
        }
        assert!(Instant::now() < deadline, "rows never landed (n={n})");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let page = read::list_page(&pool, WS, read::PageQuery::default())
        .await
        .expect("page");
    assert_eq!(page["rows"].as_array().unwrap().len(), 3);

    let refused_only = read::list_page(
        &pool,
        WS,
        read::PageQuery {
            status: Some("refused".into()),
            ..Default::default()
        },
    )
    .await
    .expect("page");
    let rows = refused_only["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["request_id"], "r-refused");
    assert_eq!(rows[0]["max_rows"], json!(3_000_000_000i64));
    assert_eq!(rows[0]["sql_truncated"], json!(true));
    assert!(rows[0]["sql"].as_str().unwrap().len() <= 32 * 1024);

    let by_session = read::list_page(
        &pool,
        WS,
        read::PageQuery {
            session_id: Some("s-1".into()),
            ..Default::default()
        },
    )
    .await
    .expect("page");
    assert_eq!(by_session["rows"].as_array().unwrap().len(), 2);
}

/// Keyset pagination: rows sharing ONE created_at paginate without skips or
/// repeats, and a returned `next_cursor` fed back yields the tail.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn pagination_is_stable_across_shared_timestamps() {
    let Some(url) = live_url() else { return };
    let (_dsn, pool) = fresh_db(&url).await;
    let at = chrono::Utc::now();
    for i in 0..5 {
        sqlx::query(
            "INSERT INTO query_ledger (org_id, workspace_id, user_id, request_id, \
             created_at, finished_at, sql, statement_kind, max_rows, status) \
             VALUES ('acme', $1, 'u', $2, $3, $3, 'SELECT 1', 'query', 100, 'succeeded')",
        )
        .bind(WS)
        .bind(format!("r-{i}"))
        .bind(at)
        .execute(&pool)
        .await
        .expect("insert");
    }

    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    loop {
        let page = read::list_page(
            &pool,
            WS,
            read::PageQuery {
                limit: Some(2),
                cursor: cursor.clone(),
                ..Default::default()
            },
        )
        .await
        .expect("page");
        for r in page["rows"].as_array().unwrap() {
            seen.push(r["request_id"].as_str().unwrap().to_string());
        }
        match page["next_cursor"].as_str() {
            Some(c) => cursor = Some(c.to_string()),
            None => break,
        }
    }
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), 5, "every row exactly once: {seen:?}");
}

/// Two workspace roles under FORCE RLS — role A cannot read or write role
/// B's rows even with a deliberately unscoped statement — and the grant set
/// a workspace role needs (schema USAGE + identity-sequence USAGE/SELECT)
/// is proven by an INSERT *through* the role. This is the database half of
/// the consumer's isolation story; the roles are provisioned here exactly
/// as its operator does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn rls_isolates_workspace_roles() {
    let Some(url) = live_url() else { return };
    let (dsn, pool) = fresh_db(&url).await;

    // Roles are cluster-wide: unique per run so reruns cannot collide.
    let tag = format!(
        "{}_{}",
        std::process::id(),
        std::time::UNIX_EPOCH.elapsed().unwrap().as_millis()
    );
    let role_a = format!("lgr_a_{tag}");
    let role_b = format!("lgr_b_{tag}");
    for (role, ws) in [(&role_a, "ws-a"), (&role_b, "ws-b")] {
        for stmt in [
            format!("CREATE ROLE {role} LOGIN PASSWORD 'pw' NOBYPASSRLS"),
            format!("GRANT USAGE ON SCHEMA public TO {role}"),
            format!("GRANT INSERT, SELECT ON query_ledger TO {role}"),
            format!("GRANT USAGE, SELECT ON SEQUENCE query_ledger_id_seq TO {role}"),
            format!(
                "CREATE POLICY ws_{role} ON query_ledger FOR ALL TO {role} \
                 USING (workspace_id = '{ws}') WITH CHECK (workspace_id = '{ws}')"
            ),
        ] {
            sqlx::raw_sql(&stmt)
                .execute(&pool)
                .await
                .expect("provision");
        }
    }

    let base = dsn.rsplit_once('@').expect("dsn").1;
    let dsn_a = format!("postgres://{role_a}:pw@{base}");
    let pool_a = sqlx::PgPool::connect(&dsn_a).await.expect("connect as A");

    sqlx::query(
        "INSERT INTO query_ledger (org_id, workspace_id, user_id, request_id, \
         created_at, finished_at, sql, statement_kind, max_rows, status) \
         VALUES ('acme', 'ws-a', 'u', 'r-a', now(), now(), 'SELECT 1', 'query', 10, 'succeeded')",
    )
    .execute(&pool_a)
    .await
    .expect("insert through role A must succeed (grants incomplete otherwise)");

    sqlx::query(
        "INSERT INTO query_ledger (org_id, workspace_id, user_id, request_id, \
         created_at, finished_at, sql, statement_kind, max_rows, status) \
         VALUES ('acme', 'ws-b', 'u', 'r-b', now(), now(), 'SECRET OF B', 'query', 10, 'succeeded')",
    )
    .execute(&pool)
    .await
    .expect("admin insert");

    let visible: Vec<String> = sqlx::query_scalar("SELECT request_id FROM query_ledger")
        .fetch_all(&pool_a)
        .await
        .expect("select as A");
    assert_eq!(visible, vec!["r-a".to_string()], "RLS must hide B's rows");

    let smuggle = sqlx::query(
        "INSERT INTO query_ledger (org_id, workspace_id, user_id, request_id, \
         created_at, finished_at, sql, statement_kind, max_rows, status) \
         VALUES ('acme', 'ws-b', 'u', 'r-smuggle', now(), now(), 'x', 'query', 10, 'succeeded')",
    )
    .execute(&pool_a)
    .await;
    assert!(
        smuggle.is_err(),
        "WITH CHECK must refuse cross-workspace writes"
    );

    let del = sqlx::query("DELETE FROM query_ledger")
        .execute(&pool_a)
        .await;
    assert!(del.is_err(), "the workspace role must hold no DELETE");

    // Cleanup: roles are cluster-wide and would otherwise outlive the db.
    drop(pool_a);
    for role in [&role_a, &role_b] {
        let _ = sqlx::raw_sql(&format!("DROP OWNED BY {role}"))
            .execute(&pool)
            .await;
        let _ = sqlx::raw_sql(&format!("DROP ROLE {role}"))
            .execute(&pool)
            .await;
    }
}

/// One ai_context/session/status shape lands typed (JSONB in, JSON out) —
/// the field the learn loop reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn ai_context_lands_as_jsonb_and_reads_back() {
    let Some(url) = live_url() else { return };
    let (dsn, pool) = fresh_db(&url).await;
    let pg = PgLedger::spawn(&dsn).expect("pool");
    pg.record(draft(WS, "SELECT 1", "r-ctx").finish(RowStatus::Succeeded, Some(1), None));
    let deadline = Instant::now() + Duration::from_secs(30);
    let row: Value = loop {
        let page = read::list_page(&pool, WS, read::PageQuery::default())
            .await
            .expect("page");
        if let Some(r) = page["rows"].as_array().unwrap().first() {
            break r.clone();
        }
        assert!(Instant::now() < deadline, "row never landed");
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(row["ai_context"]["purpose"], json!("test"));
    assert_eq!(row["session_id"], json!("s-1"));
    assert_eq!(row["status"], json!("succeeded"));
}

/// The writer's loss accounting, no server needed: a blackholed DSN makes
/// the first flush fail (connect timeout inside the 5 s flush bound), the
/// batch is dropped, and `insert_failures_total{reason="pg"}` counts it —
/// the caller saw none of this. Also pins the pool accessor and that a
/// dropped handle shuts the writer down rather than leaking it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_flush_counts_its_losses() {
    let pg = PgLedger::spawn("postgres://u:p@192.0.2.1:5432/skardi_ledger").expect("lazy pool");
    assert!(!pg.pool().is_closed(), "lazy pool, no connection yet");
    let before = ledger::METRICS
        .insert_failures_pg
        .load(std::sync::atomic::Ordering::Relaxed);

    pg.record(draft(WS, "SELECT 1", "r-flush-fail").finish(RowStatus::Succeeded, Some(1), None));

    // The flush pays its 5 s bound (plus the doomed connect) before counting.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let now = ledger::METRICS
            .insert_failures_pg
            .load(std::sync::atomic::Ordering::Relaxed);
        if now > before {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the dropped batch was never counted"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }

    // Dropping the handle closes the channel; the writer's recv sees it and
    // exits. Nothing to assert beyond "this does not hang or panic" — the
    // yield gives the writer task a turn to observe the close.
    drop(pg);
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// The read page's byte budget elides from the tail and pages on: ~300
/// worst-case rows (32 KiB sql each) cannot fit the 8 MiB body, so the page
/// ends early with `truncated: true` and a cursor — never an over-budget
/// body (the consumer's relay 413s one byte over).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn the_byte_budget_elides_the_tail_and_pages_on() {
    let Some(url) = live_url() else { return };
    let (_dsn, pool) = fresh_db(&url).await;
    sqlx::query(
        "INSERT INTO query_ledger (org_id, workspace_id, user_id, request_id, \
         created_at, finished_at, sql, sql_truncated, statement_kind, max_rows, \
         status, error) \
         SELECT 'acme', $1, 'user:acme/u1', 'req-' || n, now(), now(), \
                repeat('x', 32768), true, 'query', 100, 'failed', \
                'execution-failed: ' || repeat('e', 4096) \
         FROM generate_series(1, 300) AS n",
    )
    .bind(WS)
    .execute(&pool)
    .await
    .expect("seed 300 worst-case rows");

    let page = read::list_page(
        &pool,
        WS,
        read::PageQuery {
            limit: Some(500),
            ..Default::default()
        },
    )
    .await
    .expect("page");
    let rows = page["rows"].as_array().unwrap();
    assert!(
        !rows.is_empty() && rows.len() < 300,
        "the byte cap, not the row limit, must end this page (got {})",
        rows.len()
    );
    assert_eq!(page["truncated"], json!(true));
    assert!(page["next_cursor"].as_str().is_some());
    assert!(
        page.to_string().len() <= read::RESPONSE_MAX_BYTES,
        "the whole body must fit the budget"
    );
}

/// The flush-error arm that is NOT a timeout: the server answers and says
/// no (here: the table is gone). The batch drops, the loss is counted, the
/// caller never heard about any of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn a_rejected_insert_counts_its_losses() {
    let Some(url) = live_url() else { return };
    let (dsn, pool) = fresh_db(&url).await;
    sqlx::raw_sql("DROP TABLE query_ledger")
        .execute(&pool)
        .await
        .expect("sabotage");
    let pg = PgLedger::spawn(&dsn).expect("pool");
    let before = ledger::METRICS
        .insert_failures_pg
        .load(std::sync::atomic::Ordering::Relaxed);
    pg.record(draft(WS, "SELECT 1", "r-rejected").finish(RowStatus::Succeeded, Some(1), None));
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        if ledger::METRICS
            .insert_failures_pg
            .load(std::sync::atomic::Ordering::Relaxed)
            > before
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the rejected batch was never counted"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The P1 regression against real Postgres: a row whose caller-supplied
/// text carried U+0000 lands IN THE SAME BATCH as clean rows, and every row
/// survives — before assembly scrubbed NULs, that one row rejected the
/// whole multi-row INSERT and up to 63 other callers' rows were lost.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn a_nul_bearing_row_cannot_poison_its_batch() {
    let Some(url) = live_url() else { return };
    let (dsn, pool) = fresh_db(&url).await;
    let pg = PgLedger::spawn(&dsn).expect("pool");

    // Enqueued back-to-back so the writer drains them as ONE batch.
    pg.record(draft(WS, "SELECT 1", "r-clean-1").finish(RowStatus::Succeeded, Some(1), None));
    pg.record(
        RowDraft::capture(
            "acme",
            WS,
            "user:acme/u1",
            "r-poison",
            "SELECT '\u{0}'",
            Some(&json!({"session_id": "s\u{0}", "purpose": "p"})),
            None,
            1000,
        )
        .finish(RowStatus::Failed, None, Some("bad\u{0}input".into())),
    );
    pg.record(draft(WS, "SELECT 2", "r-clean-2").finish(RowStatus::Succeeded, Some(1), None));

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM query_ledger")
            .fetch_one(&pool)
            .await
            .expect("count");
        if n == 3 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the batch was poisoned: only {n} of 3 rows landed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let (sql, err, session): (String, String, Option<String>) = sqlx::query_as(
        "SELECT sql, error, session_id FROM query_ledger WHERE request_id = 'r-poison'",
    )
    .fetch_one(&pool)
    .await
    .expect("poisoned row landed");
    assert_eq!(sql, "SELECT '\u{FFFD}'");
    assert_eq!(err, "bad\u{FFFD}input");
    assert!(session.is_none(), "the NUL session id was dropped");
}

/// The P1 regression: a row written PAST the assembly (DDL has no length
/// constraints) with a body far over the 8 MiB budget must not livelock the
/// cursor. The read path re-applies the ingestion bounds, so the monster
/// row comes back bounded (sql cut to 32 KiB with the flag forced true,
/// oversized ai_context elided), the page stays under budget, and the
/// keyset advances to the rows behind it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn a_monster_row_reads_back_bounded_and_never_livelocks() {
    let Some(url) = live_url() else { return };
    let (_dsn, pool) = fresh_db(&url).await;
    // Newest first: the monster is the FIRST row of the page.
    sqlx::query(
        "INSERT INTO query_ledger (org_id, workspace_id, user_id, request_id, \
         created_at, finished_at, sql, statement_kind, max_rows, status, error, ai_context) \
         VALUES ('acme', $1, 'u', 'r-old', now() - interval '1 hour', now(), \
                 'SELECT 1', 'query', 10, 'succeeded', NULL, NULL)",
    )
    .bind(WS)
    .execute(&pool)
    .await
    .expect("normal row");
    sqlx::query(
        "INSERT INTO query_ledger (org_id, workspace_id, user_id, request_id, \
         created_at, finished_at, sql, statement_kind, max_rows, status, error, ai_context) \
         VALUES ('acme', $1, 'u', 'r-monster', now(), now(), \
                 repeat('m', 10 * 1024 * 1024), 'query', 10, 'failed', \
                 repeat('e', 9 * 1024 * 1024), \
                 jsonb_build_object('blob', repeat('b', 64 * 1024)))",
    )
    .bind(WS)
    .execute(&pool)
    .await
    .expect("monster row");

    let page = read::list_page(&pool, WS, read::PageQuery::default())
        .await
        .expect("the monster must not fail the page");
    assert!(
        page.to_string().len() <= read::RESPONSE_MAX_BYTES,
        "the body must fit the budget even with the monster first"
    );
    let rows = page["rows"].as_array().unwrap();
    assert_eq!(rows.len(), 2, "the page advances past the monster");
    assert_eq!(rows[0]["request_id"], "r-monster");
    assert!(rows[0]["sql"].as_str().unwrap().len() <= 32 * 1024);
    assert_eq!(rows[0]["sql_truncated"], json!(true), "the cut is declared");
    assert!(
        rows[0]["ai_context"].is_null(),
        "over-bound document elided"
    );
    assert!(rows[0]["error"].as_str().unwrap().len() <= 4 * 1024);
    assert_eq!(rows[1]["request_id"], "r-old");
}

/// The P2 regression: schema drift (here: a mutant table whose max_rows is
/// TEXT) must surface as ReadError::Unavailable — the designed 503 — never
/// a decode panic that kills the request task.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn schema_drift_is_a_read_error_not_a_panic() {
    let Some(url) = live_url() else { return };
    // A bare database WITHOUT the real DDL: build a mutant query_ledger
    // whose columns exist but max_rows decodes as TEXT.
    let (_dsn, pool) = {
        // fresh_db applies the real DDL; make our own db instead.
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let db = format!(
            "lgrmut_{}_{}_{}",
            std::process::id(),
            std::time::UNIX_EPOCH.elapsed().unwrap().as_nanos(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let boot = sqlx::PgPool::connect(&url).await.expect("connect server");
        sqlx::raw_sql(&format!(r#"CREATE DATABASE "{db}""#))
            .execute(&boot)
            .await
            .expect("create");
        boot.close().await;
        let mut parsed = url::Url::parse(&url).expect("url");
        parsed.set_path(&format!("/{db}"));
        let dsn = parsed.to_string();
        let pool = sqlx::PgPool::connect(&dsn).await.expect("connect");
        (dsn, pool)
    };
    sqlx::raw_sql(
        "CREATE TABLE query_ledger (
            id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            org_id TEXT NOT NULL, workspace_id TEXT NOT NULL,
            user_id TEXT NOT NULL, request_id TEXT NOT NULL, session_id TEXT,
            created_at TIMESTAMPTZ NOT NULL, finished_at TIMESTAMPTZ NOT NULL,
            sql TEXT NOT NULL, sql_truncated BOOLEAN NOT NULL DEFAULT FALSE,
            ai_context JSONB, statement_kind TEXT NOT NULL,
            max_rows TEXT NOT NULL, status TEXT NOT NULL,
            row_count BIGINT, error TEXT);
         INSERT INTO query_ledger (org_id, workspace_id, user_id, request_id,
            created_at, finished_at, sql, statement_kind, max_rows, status)
         VALUES ('acme', 'ws-core', 'u', 'r-drift', now(), now(),
                 'SELECT 1', 'query', 'not-a-number', 'succeeded');",
    )
    .execute(&pool)
    .await
    .expect("mutant schema");

    let err = read::list_page(&pool, WS, read::PageQuery::default())
        .await
        .expect_err("drift must be an error, not a panic");
    assert!(matches!(err, read::ReadError::Unavailable(_)), "got {err}");
}

/// Graceful shutdown drains: rows enqueued before shutdown() LAND (the
/// drain hands the backlog to a final flush instead of dying with the
/// runtime), and rows recorded after are counted as channel losses — the
/// loss-is-never-silent contract survives a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn shutdown_drains_the_queue_before_exiting() {
    let Some(url) = live_url() else { return };
    let (dsn, pool) = fresh_db(&url).await;
    let pg = PgLedger::spawn(&dsn).expect("pool");
    for i in 0..10 {
        pg.record(draft(WS, "SELECT 1", &format!("r-drain-{i}")).finish(
            RowStatus::Succeeded,
            Some(1),
            None,
        ));
    }
    pg.shutdown().await;
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM query_ledger")
        .fetch_one(&pool)
        .await
        .expect("count");
    assert_eq!(
        n, 10,
        "every accepted row must land before shutdown returns"
    );

    // Post-shutdown rows are refused by the closed channel and COUNTED.
    let before = ledger::METRICS
        .insert_failures_channel_full
        .load(std::sync::atomic::Ordering::Relaxed);
    pg.record(draft(WS, "SELECT 1", "r-late").finish(RowStatus::Succeeded, Some(1), None));
    let after = ledger::METRICS
        .insert_failures_channel_full
        .load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(after - before, 1, "a post-shutdown row is a counted loss");
}

/// Shutdown against an unreachable server still terminates (the drain's
/// final flush pays its bound, fails, COUNTS the batch) — a stalled PG must
/// not turn graceful shutdown into a hang.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_with_a_dead_server_counts_and_returns() {
    let pg = PgLedger::spawn("postgres://u:p@192.0.2.1:5432/skardi_ledger").expect("lazy pool");
    let before = ledger::METRICS
        .insert_failures_pg
        .load(std::sync::atomic::Ordering::Relaxed);
    pg.record(draft(WS, "SELECT 1", "r-dead-drain").finish(RowStatus::Succeeded, Some(1), None));
    tokio::time::timeout(Duration::from_secs(20), pg.shutdown())
        .await
        .expect("shutdown must complete within the flush bound, not hang");
    let after = ledger::METRICS
        .insert_failures_pg
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        after > before,
        "the drained-but-unflushable batch is counted"
    );
}

/// EVERY concurrent shutdown caller awaits the same drain (review): the
/// completion latch is shared watch state, so a second caller — even one
/// that controls runtime teardown — holds until the writer has really
/// exited, and no caller's return depends on being "first".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_shutdown_callers_all_await_the_drain() {
    let pg = PgLedger::spawn("postgres://u:p@192.0.2.1:5432/skardi_ledger").expect("lazy pool");
    pg.record(draft(WS, "SELECT 1", "r-cc").finish(RowStatus::Succeeded, Some(1), None));
    let before = ledger::METRICS
        .insert_failures_pg
        .load(std::sync::atomic::Ordering::Relaxed);

    let a = pg.clone();
    let b = pg.clone();
    let (ra, rb) = tokio::join!(
        tokio::time::timeout(Duration::from_secs(20), async move { a.shutdown().await }),
        tokio::time::timeout(Duration::from_secs(20), async move { b.shutdown().await }),
    );
    ra.expect("caller A completes");
    rb.expect("caller B completes");

    // BOTH returned only after the drain finished — by then the doomed
    // batch has been counted, so a caller that tears the runtime down next
    // cannot be racing an in-flight flush.
    let after = ledger::METRICS
        .insert_failures_pg
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(after > before, "the drain completed before either returned");
}

/// The ai_context transport bound (review): Postgres renders jsonb::text
/// with whitespace the compact form lacks, so a document that PASSED the
/// 4 KiB compact ingestion bound can measure larger in SQL — it must still
/// read back, not silently null. Built with enough small members that the
/// jsonb::text form crosses 4096 while the compact form stays under.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs SKARDI_QUERY_AUDIT_LIVE_URL"]
async fn near_limit_ai_context_survives_the_read_bound() {
    let Some(url) = live_url() else { return };
    let (dsn, pool) = fresh_db(&url).await;
    // ~330 members of "keyNNNN":1 — compact ≈ 3960 bytes (< 4096), while
    // jsonb::text adds a space after every ':' and ',' ≈ +660 (> 4096).
    let mut obj = serde_json::Map::new();
    for i in 0..330 {
        obj.insert(format!("key{i:04}"), json!(1));
    }
    let ctx = Value::Object(obj);
    let compact = serde_json::to_vec(&ctx).unwrap().len();
    assert!(compact <= 4096, "fixture must pass ingestion: {compact}");

    let pg = PgLedger::spawn(&dsn).expect("pool");
    let draft = RowDraft::capture(
        "acme",
        WS,
        "user:acme/u1",
        "r-near",
        "SELECT 1",
        Some(&ctx),
        None,
        1000,
    );
    pg.record(draft.finish(RowStatus::Succeeded, Some(1), None));
    pg.shutdown().await;

    // Prove the premise: the stored jsonb's text form exceeds the old bound.
    let text_len: i32 =
        sqlx::query_scalar("SELECT octet_length(ai_context::text) FROM query_ledger")
            .fetch_one(&pool)
            .await
            .expect("len");
    assert!(
        text_len > 4096,
        "fixture must exercise the expansion: {text_len}"
    );

    let page = read::list_page(&pool, WS, read::PageQuery::default())
        .await
        .expect("page");
    let row = &page["rows"].as_array().unwrap()[0];
    assert!(
        row["ai_context"].is_object(),
        "valid near-limit context must survive the read: {}",
        row["ai_context"]
    );
    assert_eq!(row["ai_context"]["key0000"], json!(1));
}
