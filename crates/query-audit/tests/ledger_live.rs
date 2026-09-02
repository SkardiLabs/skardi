//! Live-Postgres tests for the best-effort ledger module (`ledger::*`) —
//! the queued writer, the read page, and the RLS posture its consumer's
//! per-workspace roles rely on, against a real server.
//!
//! Gated exactly like `pg_live.rs`: `#[ignore]` by default, run with
//! `SKARDI_QUERY_AUDIT_LIVE_URL=postgres://… cargo test -- --ignored`.
//! Each test creates its own throwaway database and applies
//! [`queries::QUERY_LEDGER_DDL`] — the module runs no DDL itself.

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
    sqlx::raw_sql(queries::QUERY_LEDGER_DDL)
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
