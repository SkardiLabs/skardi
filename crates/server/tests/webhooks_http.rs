//! End-to-end: a `kind: webhook` definition served over HTTP, appending Meta
//! WhatsApp deliveries into a real read-write SQLite catalog, then read back
//! through `/query`. The fixtures are Meta's own documented payloads.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use sha2::Sha256;
use skardi::jobs::{JobDefinition, JobExecutor, JobRunStatus, JobStore, SqliteJobStore};
use skardi::sources::DataSourceType;
use skardi_server::auth::layer::AuthLayer;
use skardi_server::auth::mode::AuthMode;
use skardi_server::config::{AccessMode, CliArgs, DataSource, HierarchyLevel, ServerConfig};
use skardi_server::semantics::SemanticsRegistry;
use skardi_server::server::{AppState, configure_routes, setup_app_state};
use skardi_server::webhooks::WebhookDefinition;
use tempfile::TempDir;
use tower::ServiceExt;

const VERIFY_TOKEN: &str = "verify-me";
const APP_SECRET: &str = "app-secret-for-tests";

const MESSAGES_DDL: &str = "CREATE TABLE wa_events_messages (
    message_id TEXT NOT NULL, waba_id TEXT NOT NULL, phone_number_id TEXT NOT NULL,
    display_phone_number TEXT, from_wa_id TEXT NOT NULL, to_wa_id TEXT, contact_name TEXT,
    received_at TEXT NOT NULL, message_type TEXT NOT NULL, text TEXT, media_id TEXT,
    media_mime_type TEXT, caption TEXT, reply_to_message_id TEXT, interactive_reply_id TEXT,
    raw TEXT NOT NULL, ingested_at TEXT NOT NULL)";

const STATUSES_DDL: &str = "CREATE TABLE wa_events_statuses (
    message_id TEXT NOT NULL, waba_id TEXT NOT NULL, phone_number_id TEXT NOT NULL,
    status TEXT NOT NULL, status_at TEXT NOT NULL, recipient_id TEXT NOT NULL,
    conversation_id TEXT, conversation_origin TEXT, pricing_category TEXT, billable BOOLEAN,
    error_code INTEGER, raw TEXT NOT NULL, ingested_at TEXT NOT NULL)";

const DEAD_LETTER_DDL: &str = "CREATE TABLE wa_events_dead_letter (
    route TEXT NOT NULL, raw TEXT NOT NULL, error TEXT NOT NULL, ingested_at TEXT NOT NULL)";

const WEBHOOK_YAML: &str = r#"
kind: webhook
metadata: { name: wa-inbound, version: "1.0.0" }
spec:
  provider: meta_whatsapp
  verify_token_env: TEST_WA_VERIFY
  app_secret_env: TEST_WA_SECRET
  routes:
    - field: messages
      destination: { table: staging.main.wa_events_messages, mode: append }
      columns:
        - { name: message_id,          path: id,                       type: utf8, nullable: false }
        - { name: waba_id,             path: _waba_id,                 type: utf8, nullable: false }
        - { name: phone_number_id,     path: _metadata.phone_number_id, type: utf8, nullable: false }
        - { name: display_phone_number,path: _metadata.display_phone_number, type: utf8, nullable: true }
        - { name: from_wa_id,          path: from,                     type: utf8, nullable: false }
        - { name: to_wa_id,            path: _to_wa_id,                type: utf8, nullable: true }
        - { name: contact_name,        path: _contact.profile.name,    type: utf8, nullable: true }
        - { name: received_at,         path: timestamp,                type: timestamp_s_string_utc, nullable: false }
        - { name: message_type,        path: type,                     type: utf8, nullable: false }
        - { name: text,                path: text.body,                type: utf8, nullable: true }
        - { name: media_id,            path: _media.id,                type: utf8, nullable: true }
        - { name: media_mime_type,     path: _media.mime_type,         type: utf8, nullable: true }
        - { name: caption,             path: _media.caption,           type: utf8, nullable: true }
        - { name: reply_to_message_id, path: context.id,               type: utf8, nullable: true }
        - { name: interactive_reply_id,path: _interactive_reply.id,    type: utf8, nullable: true }
        - { name: raw,                 path: _raw,                     type: json, nullable: false }
    - field: statuses
      destination: { table: staging.main.wa_events_statuses, mode: append }
      columns:
        - { name: message_id,          path: id,                       type: utf8, nullable: false }
        - { name: waba_id,             path: _waba_id,                 type: utf8, nullable: false }
        - { name: phone_number_id,     path: _metadata.phone_number_id, type: utf8, nullable: false }
        - { name: status,              path: status,                   type: utf8, nullable: false }
        - { name: status_at,           path: timestamp,                type: timestamp_s_string_utc, nullable: false }
        - { name: recipient_id,        path: recipient_id,             type: utf8, nullable: false }
        - { name: conversation_id,     path: conversation.id,          type: utf8, nullable: true }
        - { name: conversation_origin, path: conversation.origin.type, type: utf8, nullable: true }
        - { name: pricing_category,    path: pricing.category,         type: utf8, nullable: true }
        - { name: billable,            path: pricing.billable,         type: boolean, nullable: true }
        - { name: error_code,          path: _error.code,              type: int64, nullable: true }
        - { name: raw,                 path: _raw,                     type: json, nullable: false }
"#;

/// A second webhook with a tiny body cap, for the 413 path.
const TINY_YAML: &str = r#"
kind: webhook
metadata: { name: tiny }
spec:
  provider: meta_whatsapp
  verify_token_env: TEST_WA_VERIFY
  app_secret_env: TEST_WA_SECRET
  max_body_bytes: 1024
  routes:
    - field: messages
      destination: { table: staging.main.wa_events_messages }
      columns:
        - { name: message_id,          path: id,                       type: utf8, nullable: false }
        - { name: waba_id,             path: _waba_id,                 type: utf8, nullable: false }
        - { name: phone_number_id,     path: _metadata.phone_number_id, type: utf8, nullable: false }
        - { name: display_phone_number,path: _metadata.display_phone_number, type: utf8, nullable: true }
        - { name: from_wa_id,          path: from,                     type: utf8, nullable: false }
        - { name: to_wa_id,            path: _to_wa_id,                type: utf8, nullable: true }
        - { name: contact_name,        path: _contact.profile.name,    type: utf8, nullable: true }
        - { name: received_at,         path: timestamp,                type: timestamp_s_string_utc, nullable: false }
        - { name: message_type,        path: type,                     type: utf8, nullable: false }
        - { name: text,                path: text.body,                type: utf8, nullable: true }
        - { name: media_id,            path: _media.id,                type: utf8, nullable: true }
        - { name: media_mime_type,     path: _media.mime_type,         type: utf8, nullable: true }
        - { name: caption,             path: _media.caption,           type: utf8, nullable: true }
        - { name: reply_to_message_id, path: context.id,               type: utf8, nullable: true }
        - { name: interactive_reply_id,path: _interactive_reply.id,    type: utf8, nullable: true }
        - { name: raw,                 path: _raw,                     type: json, nullable: false }
"#;

fn test_env(name: &str) -> Option<String> {
    match name {
        "TEST_WA_VERIFY" => Some(VERIFY_TOKEN.to_string()),
        "TEST_WA_SECRET" => Some(APP_SECRET.to_string()),
        _ => None,
    }
}

fn fixture(name: &str) -> Vec<u8> {
    let path = format!(
        "{}/tests/fixtures/webhooks/{name}.json",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::read(&path).unwrap_or_else(|e| panic!("{path}: {e}"))
}

fn sign(body: &[u8], secret: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

async fn create_db(dir: &TempDir, ddl: &[&str]) -> PathBuf {
    let path = dir.path().join("staging.db");
    let conn = tokio_rusqlite::Connection::open(&path).await.unwrap();
    let statements: Vec<String> = ddl.iter().map(|s| s.to_string()).collect();
    conn.call(move |c| {
        for stmt in &statements {
            c.execute_batch(stmt)?;
        }
        Ok::<(), tokio_rusqlite::Error>(())
    })
    .await
    .unwrap();
    conn.close().await.unwrap();
    path
}

fn staging_source(path: PathBuf) -> DataSource {
    DataSource {
        name: "staging".to_string(),
        source_type: DataSourceType::Sqlite,
        path,
        connection_string: None,
        schema: None,
        options: None,
        hierarchy_level: HierarchyLevel::Catalog,
        access_mode: AccessMode::ReadWrite,
        enable_cache: false,
        description: None,
        open_connector: None,
        rss: None,
        graph: None,
    }
}

fn config(path: PathBuf, yamls: &[&str]) -> ServerConfig {
    let mut webhooks = HashMap::new();
    for yaml in yamls {
        let def = WebhookDefinition::from_yaml_with_env(yaml, test_env)
            .unwrap()
            .unwrap();
        webhooks.insert(def.name.clone(), def);
    }
    ServerConfig {
        pipelines: HashMap::new(),
        jobs: HashMap::new(),
        webhooks,
        data_sources: vec![staging_source(path)],
        semantics: SemanticsRegistry::default(),
        args: CliArgs::default(),
    }
}

/// A server with both tables and both webhooks, plus the SQLite path and
/// the tempdir keeping it alive.
async fn server() -> (AppState, PathBuf, TempDir) {
    let dir = TempDir::new().unwrap();
    let path = create_db(&dir, &[MESSAGES_DDL, STATUSES_DDL]).await;
    let state = setup_app_state(config(path.clone(), &[WEBHOOK_YAML, TINY_YAML]))
        .await
        .unwrap();
    (state, path, dir)
}

async fn deliver(
    state: &AppState,
    name: &str,
    body: Vec<u8>,
    signature: Option<&str>,
) -> axum::response::Response {
    let mut req = Request::builder()
        .method("POST")
        .uri(format!("/webhooks/{name}"))
        .header("content-type", "application/json");
    if let Some(sig) = signature {
        req = req.header("x-hub-signature-256", sig);
    }
    configure_routes(state.clone())
        .oneshot(req.body(Body::from(body)).unwrap())
        .await
        .unwrap()
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes)))
}

async fn body_text(resp: axum::response::Response) -> String {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

async fn query(state: &AppState, sql: &str) -> Vec<Value> {
    let resp = configure_routes(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/query")
                .header("content-type", "application/json")
                .body(Body::from(json!({ "sql": sql }).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "query failed: {sql}");
    let body = body_json(resp).await;
    assert_eq!(body["success"], json!(true), "{body}");
    body["data"].as_array().cloned().unwrap_or_default()
}

#[tokio::test]
async fn handshake_echoes_the_challenge_and_refuses_a_wrong_token() {
    let (state, _path, _dir) = server().await;
    let resp = configure_routes(state.clone())
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/webhooks/wa-inbound?hub.mode=subscribe&hub.verify_token={VERIFY_TOKEN}&hub.challenge=1158201444"
                ))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        resp.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/plain")
    );
    assert_eq!(body_text(resp).await, "1158201444");

    let resp = configure_routes(state)
        .oneshot(
            Request::builder()
                .uri(
                    "/webhooks/wa-inbound?hub.mode=subscribe&hub.verify_token=nope&hub.challenge=1",
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    assert_eq!(body_json(resp).await["error_type"], "handshake_rejected");
}

#[tokio::test]
async fn a_text_message_lands_in_the_messages_table_with_every_mapped_column() {
    let (state, _path, _dir) = server().await;
    let body = fixture("text_message");
    let resp = deliver(
        &state,
        "wa-inbound",
        body.clone(),
        Some(&sign(&body, APP_SECRET)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let ack = body_json(resp).await;
    assert_eq!(ack["success"], json!(true));
    assert_eq!(ack["rows"], json!(1));
    assert_eq!(ack["routes"][0]["field"], "messages");
    assert_eq!(ack["routes"][0]["rows"], json!(1));
    assert_eq!(ack["routes"][1]["field"], "statuses");
    assert_eq!(ack["routes"][1]["rows"], json!(0));

    let rows = query(
        &state,
        "SELECT message_id, waba_id, phone_number_id, display_phone_number, from_wa_id, to_wa_id, \
         contact_name, received_at, message_type, text, media_id, raw, ingested_at \
         FROM staging.main.wa_events_messages",
    )
    .await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(
        row["message_id"],
        "wamid.HBgLMTY1MDM4Nzk0MzkVAgASGBQzQTRBNjU5OUFFRTAzODEwMTQ0RgA="
    );
    assert_eq!(row["waba_id"], "102290129340398");
    assert_eq!(row["phone_number_id"], "106540352242922");
    assert_eq!(row["display_phone_number"], "15550783881");
    assert_eq!(row["from_wa_id"], "16505551234");
    assert_eq!(row["to_wa_id"], Value::Null);
    assert_eq!(row["contact_name"], "Sheena Nelson");
    assert_eq!(row["message_type"], "text");
    assert_eq!(row["text"], "Does it come in another color?");
    assert_eq!(row["media_id"], Value::Null);
    // Meta's "1749416383" (seconds) is stored as an ISO-8601 instant in UTC.
    assert_eq!(row["received_at"], "2025-06-08T20:59:43Z");
    let raw: Value = serde_json::from_str(row["raw"].as_str().unwrap()).unwrap();
    assert_eq!(raw["text"]["body"], "Does it come in another color?");
    assert!(raw.get("_waba_id").is_none(), "raw is the untouched object");
    let ingested_at = row["ingested_at"].as_str().unwrap();
    assert!(
        ingested_at.ends_with('Z') && ingested_at.len() >= 20,
        "{ingested_at}"
    );
    assert!(
        query(&state, "SELECT 1 FROM staging.main.wa_events_statuses")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn media_interactive_and_unsupported_messages_fill_their_synthetic_columns() {
    let (state, _path, _dir) = server().await;
    for name in [
        "image_message",
        "interactive_button_reply",
        "unsupported_message",
    ] {
        let body = fixture(name);
        let resp = deliver(
            &state,
            "wa-inbound",
            body.clone(),
            Some(&sign(&body, APP_SECRET)),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "{name}");
    }
    let rows = query(
        &state,
        "SELECT message_type, media_id, media_mime_type, caption, interactive_reply_id, \
         reply_to_message_id FROM staging.main.wa_events_messages ORDER BY ingested_at, message_type",
    )
    .await;
    assert_eq!(rows.len(), 3);
    let by_type: HashMap<&str, &Value> = rows
        .iter()
        .map(|r| (r["message_type"].as_str().unwrap(), r))
        .collect();
    assert_eq!(by_type["image"]["media_id"], "1003383421387256");
    assert_eq!(by_type["image"]["media_mime_type"], "image/jpeg");
    assert_eq!(by_type["image"]["caption"], "Taj Mahal");
    assert_eq!(
        by_type["interactive"]["interactive_reply_id"],
        "cancel-button"
    );
    assert!(
        by_type["interactive"]["reply_to_message_id"]
            .as_str()
            .unwrap()
            .starts_with("wamid.")
    );
    assert_eq!(by_type["unsupported"]["media_id"], Value::Null);
}

#[tokio::test]
async fn statuses_land_in_the_statuses_table_including_failures() {
    let (state, _path, _dir) = server().await;
    for name in ["status_sent_pmp", "status_delivered", "status_failed"] {
        let body = fixture(name);
        let resp = deliver(
            &state,
            "wa-inbound",
            body.clone(),
            Some(&sign(&body, APP_SECRET)),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "{name}");
        assert_eq!(
            body_json(resp).await["routes"][1]["rows"],
            json!(1),
            "{name}"
        );
    }
    let rows = query(
        &state,
        "SELECT status, waba_id, phone_number_id, recipient_id, conversation_origin, \
         pricing_category, billable, error_code FROM staging.main.wa_events_statuses ORDER BY status",
    )
    .await;
    assert_eq!(rows.len(), 3);
    let by_status: HashMap<&str, &Value> = rows
        .iter()
        .map(|r| (r["status"].as_str().unwrap(), r))
        .collect();
    assert_eq!(by_status["sent"]["pricing_category"], "marketing");
    assert_eq!(by_status["sent"]["billable"], json!(true));
    assert_eq!(by_status["sent"]["conversation_origin"], "marketing");
    assert_eq!(by_status["delivered"]["pricing_category"], "service");
    assert_eq!(by_status["failed"]["error_code"], json!(131049));
    assert_eq!(by_status["failed"]["pricing_category"], Value::Null);
    for row in &rows {
        assert_eq!(row["waba_id"], "102290129340398");
        assert_eq!(row["phone_number_id"], "106540352242922");
        assert_eq!(row["recipient_id"], "16505551234");
    }
    assert!(
        query(&state, "SELECT 1 FROM staging.main.wa_events_messages")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn a_redelivery_appends_a_second_row_for_the_merge_job_to_collapse() {
    let (state, _path, _dir) = server().await;
    let body = fixture("text_message");
    let sig = sign(&body, APP_SECRET);
    for _ in 0..2 {
        let resp = deliver(&state, "wa-inbound", body.clone(), Some(&sig)).await;
        assert_eq!(resp.status(), StatusCode::OK);
    }
    let rows = query(
        &state,
        "SELECT count(*) AS n, count(DISTINCT message_id) AS ids FROM staging.main.wa_events_messages",
    )
    .await;
    assert_eq!(rows[0]["n"], json!(2));
    assert_eq!(rows[0]["ids"], json!(1));
}

#[tokio::test]
async fn an_unsigned_or_missigned_delivery_is_refused_before_anything_is_written() {
    let (state, _path, _dir) = server().await;
    let body = fixture("text_message");

    let resp = deliver(&state, "wa-inbound", body.clone(), None).await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(body_json(resp).await["error_type"], "signature_rejected");

    let resp = deliver(
        &state,
        "wa-inbound",
        body.clone(),
        Some(&sign(&body, "wrong-secret")),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let mut tampered = body.clone();
    tampered.extend_from_slice(b" ");
    let resp = deliver(
        &state,
        "wa-inbound",
        tampered,
        Some(&sign(&body, APP_SECRET)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    assert!(
        query(&state, "SELECT 1 FROM staging.main.wa_events_messages")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn an_oversized_body_is_refused_with_413() {
    let (state, _path, _dir) = server().await;
    // A valid, signed envelope whose text body alone exceeds the 1 KiB cap.
    let mut envelope: Value = serde_json::from_slice(&fixture("text_message")).unwrap();
    envelope["entry"][0]["changes"][0]["value"]["messages"][0]["text"]["body"] =
        Value::String("x".repeat(2048));
    let body = serde_json::to_vec(&envelope).unwrap();
    assert!(body.len() > 1024);
    let resp = deliver(&state, "tiny", body.clone(), Some(&sign(&body, APP_SECRET))).await;
    assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body_json(resp).await["error_type"], "payload_too_large");
    // The same delivery is fine under the default cap.
    let resp = deliver(
        &state,
        "wa-inbound",
        body.clone(),
        Some(&sign(&body, APP_SECRET)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn a_verified_body_that_is_not_an_envelope_is_a_400() {
    let (state, _path, _dir) = server().await;
    for body in [
        br#"{"object":"whatsapp_business_account"}"#.to_vec(),
        b"not json".to_vec(),
    ] {
        let resp = deliver(
            &state,
            "wa-inbound",
            body.clone(),
            Some(&sign(&body, APP_SECRET)),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(body_json(resp).await["error_type"], "invalid_payload");
    }
}

#[tokio::test]
async fn an_unknown_webhook_name_is_a_404_on_both_methods() {
    let (state, _path, _dir) = server().await;
    let resp = deliver(&state, "nope", b"{}".to_vec(), None).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    assert_eq!(body_json(resp).await["error_type"], "webhook_not_found");
    let resp = configure_routes(state)
        .oneshot(
            Request::builder()
                .uri("/webhooks/nope?hub.mode=subscribe&hub.verify_token=x&hub.challenge=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_write_failure_after_verification_answers_500_so_meta_retries() {
    let (state, path, _dir) = server().await;
    // Pull the table out from under the registered source.
    let conn = tokio_rusqlite::Connection::open(&path).await.unwrap();
    conn.call(|c| {
        c.execute_batch("DROP TABLE wa_events_messages")
            .map_err(tokio_rusqlite::Error::from)
    })
    .await
    .unwrap();
    conn.close().await.unwrap();

    let body = fixture("text_message");
    let resp = deliver(
        &state,
        "wa-inbound",
        body.clone(),
        Some(&sign(&body, APP_SECRET)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let err = body_json(resp).await;
    assert_eq!(err["error_type"], "write_error");
    assert_eq!(err["details"]["route"], "messages");
}

#[tokio::test]
async fn the_route_stays_reachable_when_api_auth_is_enabled() {
    let (mut state, _path, _dir) = server().await;
    // SAFETY: test-only env mutation, scoped to this process; the auth layer
    // reads these once while it is built.
    unsafe {
        std::env::set_var("AUTH_SECRET", "a-secret-long-enough-for-tests-0123456789");
        std::env::set_var("AUTH_DB_PATH", ":memory:");
    }
    state.auth_layer = AuthLayer::build(&AuthMode::BetterAuthDieselSqlite)
        .await
        .unwrap();
    assert!(state.auth_layer.is_enabled());

    // /query now demands a session...
    let resp = configure_routes(state.clone())
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/query")
                .header("content-type", "application/json")
                .body(Body::from(json!({"sql": "SELECT 1"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // ...while a signed delivery still lands.
    let body = fixture("text_message");
    let resp = deliver(
        &state,
        "wa-inbound",
        body.clone(),
        Some(&sign(&body, APP_SECRET)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn startup_refuses_a_destination_whose_columns_do_not_match() {
    let dir = TempDir::new().unwrap();
    // Statuses table missing; messages table without ingested_at.
    let short_ddl = MESSAGES_DDL.replace(", ingested_at TEXT NOT NULL", "");
    let path = create_db(&dir, &[&short_ddl]).await;
    let err = setup_app_state(config(path, &[WEBHOOK_YAML]))
        .await
        .err()
        .expect("startup must fail");
    let msg = format!("{err:#}");
    assert!(msg.contains("wa_events_messages"), "{msg}");
    assert!(msg.contains("ingested_at"), "{msg}");
    assert!(msg.contains("matches by position"), "{msg}");
}

#[tokio::test]
async fn startup_refuses_a_destination_on_a_read_only_or_unknown_source() {
    let dir = TempDir::new().unwrap();
    let path = create_db(&dir, &[MESSAGES_DDL, STATUSES_DDL]).await;

    let mut cfg = config(path.clone(), &[WEBHOOK_YAML]);
    cfg.data_sources[0].access_mode = AccessMode::ReadOnly;
    let err = setup_app_state(cfg)
        .await
        .err()
        .expect("read-only must fail");
    assert!(format!("{err:#}").contains("read_write"), "{err:#}");

    let yaml = WEBHOOK_YAML.replace(
        "staging.main.wa_events_messages",
        "elsewhere.main.wa_events_messages",
    );
    let err = setup_app_state(config(path, &[&yaml]))
        .await
        .err()
        .expect("unknown source must fail");
    assert!(
        format!("{err:#}").contains("does not name a configured data source"),
        "{err:#}"
    );
}

#[tokio::test]
async fn a_server_without_webhooks_serves_none() {
    let dir = TempDir::new().unwrap();
    let path = create_db(&dir, &[MESSAGES_DDL]).await;
    let state = setup_app_state(config(path, &[])).await.unwrap();
    assert!(state.webhooks.is_empty());
    let resp = deliver(&state, "wa-inbound", b"{}".to_vec(), None).await;
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

/// The documented bundle under docs/whatsapp/ is loaded as-is: its webhook
/// YAML must preflight against its own SQLite DDL, accept every documented
/// Meta payload, and its merge jobs must plan against the staging tables.
/// This is what keeps the docs from drifting away from the code.
#[tokio::test]
async fn the_documented_whatsapp_bundle_loads_against_its_own_ddl() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/whatsapp");
    let ddl = std::fs::read_to_string(root.join("staging.sqlite.sql")).unwrap();
    let dir = TempDir::new().unwrap();
    let path = create_db(&dir, &[&ddl]).await;

    let yaml = std::fs::read_to_string(root.join("webhooks/whatsapp-inbound.yaml")).unwrap();
    let def = WebhookDefinition::from_yaml_with_env(&yaml, |name| match name {
        "WHATSAPP_VERIFY_TOKEN" => Some(VERIFY_TOKEN.to_string()),
        "WHATSAPP_APP_SECRET" => Some(APP_SECRET.to_string()),
        _ => None,
    })
    .unwrap()
    .expect("docs/whatsapp/webhooks/whatsapp-inbound.yaml is kind: webhook");
    let mut cfg = config(path, &[]);
    cfg.webhooks.insert(def.name.clone(), def);
    let state = setup_app_state(cfg).await.unwrap();

    for name in [
        "text_message",
        "image_message",
        "interactive_list_reply",
        "interactive_button_reply",
        "unsupported_message",
        "status_sent_pmp",
        "status_delivered",
        "status_failed",
    ] {
        let body = fixture(name);
        let resp = deliver(
            &state,
            "whatsapp-inbound",
            body.clone(),
            Some(&sign(&body, APP_SECRET)),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "{name}");
    }
    let rows = query(
        &state,
        "SELECT count(*) AS n FROM staging.main.wa_events_messages",
    )
    .await;
    assert_eq!(rows[0]["n"], json!(5));
    let rows = query(
        &state,
        "SELECT count(*) AS n FROM staging.main.wa_events_statuses",
    )
    .await;
    assert_eq!(rows[0]["n"], json!(3));

    for job in ["wa-merge-messages", "wa-merge-statuses"] {
        let loaded = JobDefinition::load_from_file(
            root.join(format!("jobs/{job}.yaml")),
            Arc::clone(&state.session_ctx),
        )
        .await
        .unwrap_or_else(|e| panic!("{job}: {e:#}"))
        .expect("kind: job");
        assert_eq!(loaded.name(), job);
    }
}

/// An envelope whose single change carries both arrays, so one delivery
/// feeds the messages route and the statuses route.
fn both_arrays_envelope() -> Vec<u8> {
    let mut envelope: Value = serde_json::from_slice(&fixture("text_message")).unwrap();
    let statuses: Value = serde_json::from_slice(&fixture("status_delivered")).unwrap();
    envelope["entry"][0]["changes"][0]["value"]["statuses"] =
        statuses["entry"][0]["changes"][0]["value"]["statuses"].clone();
    serde_json::to_vec(&envelope).unwrap()
}

#[tokio::test]
async fn a_later_route_failure_reports_what_earlier_routes_committed() {
    let (state, path, _dir) = server().await;
    let conn = tokio_rusqlite::Connection::open(&path).await.unwrap();
    conn.call(|c| {
        c.execute_batch("DROP TABLE wa_events_statuses")
            .map_err(tokio_rusqlite::Error::from)
    })
    .await
    .unwrap();
    conn.close().await.unwrap();

    let body = both_arrays_envelope();
    let resp = deliver(
        &state,
        "wa-inbound",
        body.clone(),
        Some(&sign(&body, APP_SECRET)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let err = body_json(resp).await;
    assert_eq!(err["error_type"], "write_error");
    assert_eq!(err["details"]["route"], "statuses");
    assert_eq!(err["details"]["committed"][0]["field"], "messages");
    assert_eq!(err["details"]["committed"][0]["rows"], json!(1));
    // The messages route really did commit before the statuses route failed.
    let rows = query(
        &state,
        "SELECT count(*) AS n FROM staging.main.wa_events_messages",
    )
    .await;
    assert_eq!(rows[0]["n"], json!(1));
}

#[tokio::test]
async fn a_delivery_carrying_both_arrays_feeds_both_routes() {
    let (state, _path, _dir) = server().await;
    let body = both_arrays_envelope();
    let resp = deliver(
        &state,
        "wa-inbound",
        body.clone(),
        Some(&sign(&body, APP_SECRET)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let ack = body_json(resp).await;
    assert_eq!(ack["rows"], json!(2));
    assert_eq!(ack["routes"][0]["rows"], json!(1));
    assert_eq!(ack["routes"][1]["rows"], json!(1));
}

#[tokio::test]
async fn a_payload_that_lacks_a_non_nullable_column_is_a_conversion_error() {
    // Same as WEBHOOK_YAML but `text` must be present — an image has none.
    let yaml = WEBHOOK_YAML.replace(
        "{ name: text,                path: text.body,                type: utf8, nullable: true }",
        "{ name: text,                path: text.body,                type: utf8, nullable: false }",
    );
    assert_ne!(yaml, WEBHOOK_YAML);
    let dir = TempDir::new().unwrap();
    let path = create_db(&dir, &[MESSAGES_DDL, STATUSES_DDL]).await;
    let state = setup_app_state(config(path, &[&yaml])).await.unwrap();

    let body = fixture("image_message");
    let resp = deliver(
        &state,
        "wa-inbound",
        body.clone(),
        Some(&sign(&body, APP_SECRET)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let err = body_json(resp).await;
    assert_eq!(err["error_type"], "conversion_error");
    assert_eq!(err["details"]["route"], "messages");
    let detail = err["details"]["error"].as_str().unwrap();
    assert!(detail.contains("text"), "{detail}");
    assert!(
        !detail.contains("Taj Mahal"),
        "values never appear: {detail}"
    );
    assert!(
        query(&state, "SELECT 1 FROM staging.main.wa_events_messages")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn every_handshake_rejection_is_a_403_with_the_reason() {
    let (state, _path, _dir) = server().await;
    for (query_string, reason) in [
        (
            "hub.mode=unsubscribe&hub.verify_token=verify-me&hub.challenge=1",
            "hub.mode",
        ),
        (
            "hub.mode=subscribe&hub.verify_token=verify-me",
            "hub.challenge",
        ),
        ("hub.mode=subscribe&hub.challenge=1", "hub.verify_token"),
    ] {
        let resp = configure_routes(state.clone())
            .oneshot(
                Request::builder()
                    .uri(format!("/webhooks/wa-inbound?{query_string}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN, "{query_string}");
        let err = body_json(resp).await;
        assert_eq!(err["error_type"], "handshake_rejected");
        assert!(err["error"].as_str().unwrap().contains(reason), "{err}");
    }
}

#[tokio::test]
async fn startup_refuses_a_destination_column_of_the_wrong_type() {
    let dir = TempDir::new().unwrap();
    let ddl = MESSAGES_DDL.replace("message_id TEXT NOT NULL", "message_id INTEGER NOT NULL");
    assert_ne!(ddl, MESSAGES_DDL);
    let path = create_db(&dir, &[&ddl, STATUSES_DDL]).await;
    let err = setup_app_state(config(path, &[WEBHOOK_YAML]))
        .await
        .err()
        .expect("a text id into an integer column must fail at startup");
    let msg = format!("{err:#}");
    assert!(msg.contains("message_id"), "{msg}");
    assert!(msg.contains("Int64"), "{msg}");
    assert!(msg.contains("Utf8"), "{msg}");
}

/// The server-config loader: `--webhooks` over a directory with a webhook,
/// a non-webhook file to skip, and the duplicate-name refusal.
#[tokio::test]
async fn load_server_config_scans_a_webhooks_directory() {
    use skardi_server::config::load_server_config;

    let dir = TempDir::new().unwrap();
    let db = create_db(&dir, &[MESSAGES_DDL, STATUSES_DDL]).await;
    let ctx = dir.path().join("ctx.yaml");
    std::fs::write(
        &ctx,
        format!(
            "kind: context\nmetadata: {{ name: t }}\nspec:\n  data_sources:\n    - name: staging\n      type: sqlite\n      path: {}\n      hierarchy_level: catalog\n      access_mode: read_write\n",
            db.display()
        ),
    )
    .unwrap();
    let hooks = dir.path().join("webhooks");
    std::fs::create_dir(&hooks).unwrap();
    let yaml = WEBHOOK_YAML
        .replace("TEST_WA_VERIFY", "LSC_WA_VERIFY")
        .replace("TEST_WA_SECRET", "LSC_WA_SECRET");
    std::fs::write(hooks.join("wa.yaml"), &yaml).unwrap();
    std::fs::write(
        hooks.join("notes.yaml"),
        "kind: semantics\nmetadata: { name: n }\nspec: {}\n",
    )
    .unwrap();
    // SAFETY: test-only, unique variable names, read once by the loader.
    unsafe {
        std::env::set_var("LSC_WA_VERIFY", VERIFY_TOKEN);
        std::env::set_var("LSC_WA_SECRET", APP_SECRET);
    }

    let args = CliArgs {
        ctx_file: Some(ctx.clone()),
        webhooks_path: Some(hooks.clone()),
        ..CliArgs::default()
    };
    let config = load_server_config(args).await.unwrap();
    assert_eq!(config.webhooks.len(), 1);
    assert!(config.webhooks.contains_key("wa-inbound"));

    // A second file with the same name is refused.
    std::fs::write(hooks.join("wa-again.yaml"), &yaml).unwrap();
    let args = CliArgs {
        ctx_file: Some(ctx.clone()),
        webhooks_path: Some(hooks.clone()),
        ..CliArgs::default()
    };
    let err = load_server_config(args)
        .await
        .err()
        .expect("duplicate must fail");
    assert!(
        format!("{err:#}").contains("Duplicate webhook name"),
        "{err:#}"
    );

    // A directory with no webhook at all loads nothing (and warns).
    let empty = dir.path().join("nohooks");
    std::fs::create_dir(&empty).unwrap();
    std::fs::write(
        empty.join("notes.yaml"),
        "kind: semantics\nmetadata: { name: n }\nspec: {}\n",
    )
    .unwrap();
    let args = CliArgs {
        ctx_file: Some(ctx),
        webhooks_path: Some(empty),
        ..CliArgs::default()
    };
    let config = load_server_config(args).await.unwrap();
    assert!(config.webhooks.is_empty());
}

/// The example webhook plus a dead-letter table, with `text` non-null so an
/// image message (no `text.body`) is a rejected row.
fn dead_letter_yaml() -> String {
    WEBHOOK_YAML
        .replace(
            "  routes:\n",
            "  dead_letter: { table: staging.main.wa_events_dead_letter }\n  routes:\n",
        )
        .replace(
            "{ name: text,                path: text.body,                type: utf8, nullable: true }",
            "{ name: text,                path: text.body,                type: utf8, nullable: false }",
        )
}

#[tokio::test]
async fn a_rejected_row_is_quarantined_when_a_dead_letter_table_exists() {
    let yaml = dead_letter_yaml();
    assert!(yaml.contains("dead_letter") && yaml.contains("type: utf8, nullable: false }"));
    let dir = TempDir::new().unwrap();
    let path = create_db(&dir, &[MESSAGES_DDL, STATUSES_DDL, DEAD_LETTER_DDL]).await;
    let state = setup_app_state(config(path, &[&yaml])).await.unwrap();

    // One change carrying a text message (converts) and an image message
    // (no text.body → rejected): the delivery is acknowledged, the good row
    // lands, the bad one is quarantined with its reason.
    let mut envelope: Value = serde_json::from_slice(&fixture("text_message")).unwrap();
    let image: Value = serde_json::from_slice(&fixture("image_message")).unwrap();
    let image_row = image["entry"][0]["changes"][0]["value"]["messages"][0].clone();
    envelope["entry"][0]["changes"][0]["value"]["messages"]
        .as_array_mut()
        .unwrap()
        .push(image_row);
    let body = serde_json::to_vec(&envelope).unwrap();
    let resp = deliver(
        &state,
        "wa-inbound",
        body.clone(),
        Some(&sign(&body, APP_SECRET)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::OK);
    let ack = body_json(resp).await;
    assert_eq!(ack["rows"], json!(1));
    assert_eq!(ack["quarantined"], json!(1));
    assert_eq!(ack["routes"][0]["quarantined"], json!(1));

    let rows = query(
        &state,
        "SELECT message_type FROM staging.main.wa_events_messages",
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["message_type"], "text");
    let dead = query(
        &state,
        "SELECT route, raw, error, ingested_at FROM staging.main.wa_events_dead_letter",
    )
    .await;
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0]["route"], "messages");
    let raw: Value = serde_json::from_str(dead[0]["raw"].as_str().unwrap()).unwrap();
    assert_eq!(raw["type"], "image", "the original object, untouched");
    assert!(raw.get("_waba_id").is_none());
    let error = dead[0]["error"].as_str().unwrap();
    assert!(error.contains("text"), "{error}");
    assert!(!error.contains("Taj Mahal"), "values never appear: {error}");
    assert!(dead[0]["ingested_at"].as_str().unwrap().ends_with('Z'));
}

#[tokio::test]
async fn another_meta_products_envelope_is_a_400_not_a_write() {
    let (state, _path, _dir) = server().await;
    let mut envelope: Value = serde_json::from_slice(&fixture("text_message")).unwrap();
    envelope["object"] = Value::String("page".into());
    let body = serde_json::to_vec(&envelope).unwrap();
    let resp = deliver(
        &state,
        "wa-inbound",
        body.clone(),
        Some(&sign(&body, APP_SECRET)),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let err = body_json(resp).await;
    assert_eq!(err["error_type"], "invalid_payload");
    assert!(err["error"].as_str().unwrap().contains("'page'"), "{err}");
    assert!(
        query(&state, "SELECT 1 FROM staging.main.wa_events_messages")
            .await
            .is_empty()
    );
}

#[tokio::test]
async fn startup_refuses_a_not_null_column_the_route_may_leave_null() {
    let dir = TempDir::new().unwrap();
    let ddl = MESSAGES_DDL.replace("text TEXT,", "text TEXT NOT NULL,");
    assert_ne!(ddl, MESSAGES_DDL);
    let path = create_db(&dir, &[&ddl, STATUSES_DDL]).await;
    let err = setup_app_state(config(path, &[WEBHOOK_YAML]))
        .await
        .err()
        .expect("a nullable route column into NOT NULL must fail at startup");
    let msg = format!("{err:#}");
    assert!(msg.contains("'text'") && msg.contains("NOT NULL"), "{msg}");
}

/// Runs the documented merge jobs end to end: a redelivery collapses under
/// upsert + keep_last, the `{since}` watermark selects on arrival time, the
/// casts land real timestamps in Lance, and a second run is idempotent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_documented_merge_jobs_dedup_into_lance_and_are_idempotent() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/whatsapp");
    let ddl = std::fs::read_to_string(root.join("staging.sqlite.sql")).unwrap();
    let dir = TempDir::new().unwrap();
    let path = create_db(&dir, &[&ddl]).await;
    let lake = dir.path().join("lake");
    std::fs::create_dir(&lake).unwrap();

    let yaml = std::fs::read_to_string(root.join("webhooks/whatsapp-inbound.yaml")).unwrap();
    let def = WebhookDefinition::from_yaml_with_env(&yaml, |name| match name {
        "WHATSAPP_VERIFY_TOKEN" => Some(VERIFY_TOKEN.to_string()),
        "WHATSAPP_APP_SECRET" => Some(APP_SECRET.to_string()),
        _ => None,
    })
    .unwrap()
    .unwrap();
    let lance_source = |name: &str| DataSource {
        name: name.to_string(),
        source_type: DataSourceType::Lance,
        path: lake.join(format!("{name}.lance")),
        connection_string: None,
        schema: None,
        options: None,
        hierarchy_level: HierarchyLevel::Table,
        access_mode: AccessMode::ReadOnly,
        enable_cache: false,
        description: None,
        open_connector: None,
        rss: None,
        graph: None,
    };
    let make_config = || {
        let mut cfg = config(path.clone(), &[]);
        cfg.webhooks.insert(def.name.clone(), def.clone());
        cfg.data_sources.push(lance_source("wa_messages"));
        cfg.data_sources.push(lance_source("wa_statuses"));
        cfg
    };
    let state = setup_app_state(make_config()).await.unwrap();

    let since = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true);
    for name in [
        "text_message",
        "image_message",
        "interactive_list_reply",
        "interactive_button_reply",
        "unsupported_message",
        "status_sent_pmp",
        "status_delivered",
        "status_failed",
        "text_message", // the redelivery
    ] {
        let body = fixture(name);
        let resp = deliver(
            &state,
            "whatsapp-inbound",
            body.clone(),
            Some(&sign(&body, APP_SECRET)),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "{name}");
    }
    assert_eq!(
        query(
            &state,
            "SELECT count(*) AS n FROM staging.main.wa_events_messages"
        )
        .await[0]["n"],
        json!(6)
    );

    let mut jobs = HashMap::new();
    for job in ["wa-merge-messages", "wa-merge-statuses"] {
        let loaded = JobDefinition::load_from_file(
            root.join(format!("jobs/{job}.yaml")),
            Arc::clone(&state.session_ctx),
        )
        .await
        .unwrap_or_else(|e| panic!("{job}: {e:#}"))
        .expect("kind: job");
        jobs.insert(loaded.name().to_string(), loaded);
    }
    let written = run_merge_jobs(
        Arc::clone(&state.session_ctx),
        jobs.clone(),
        lake.clone(),
        since.clone(),
    )
    .await;
    assert_eq!(
        written["wa-merge-messages"], 6,
        "every staging row is read on the first merge"
    );

    // Datasets created by a job register on the next start: build a fresh
    // state over the same context file and read the lake back.
    let state2 = setup_app_state(make_config()).await.unwrap();
    let rows = query(&state2, "SELECT count(*) AS n, arrow_typeof(received_at) AS t, arrow_typeof(ingested_at) AS i FROM wa_messages GROUP BY 2, 3").await;
    // Meta's reference payloads reuse message ids: the text and image samples
    // share one, the button reply and the unsupported message share another.
    // Six staging rows (five fixtures plus the redelivery) therefore hold
    // three distinct ids, and that is what the upsert must leave.
    assert_eq!(
        rows[0]["n"],
        json!(3),
        "redelivery and id reuse collapsed under upsert + keep_last"
    );
    assert_eq!(rows[0]["t"], json!("Timestamp(ms, \"UTC\")"));
    assert_eq!(rows[0]["i"], json!("Timestamp(ms, \"UTC\")"));
    let rows = query(&state2, "SELECT count(*) AS n FROM wa_statuses").await;
    assert_eq!(rows[0]["n"], json!(3));

    // Same window again: nothing changes.
    let _ = run_merge_jobs(Arc::clone(&state2.session_ctx), jobs, lake.clone(), since).await;
    let state3 = setup_app_state(make_config()).await.unwrap();
    assert_eq!(
        query(&state3, "SELECT count(*) AS n FROM wa_messages").await[0]["n"],
        json!(3)
    );
    assert_eq!(
        query(&state3, "SELECT count(*) AS n FROM wa_statuses").await[0]["n"],
        json!(3)
    );
}

/// Submit both documented merge jobs against `ctx` and wait for them.
async fn run_merge_jobs(
    ctx: Arc<datafusion::prelude::SessionContext>,
    jobs: HashMap<String, JobDefinition>,
    lake: PathBuf,
    since: String,
) -> HashMap<&'static str, u64> {
    let store = Arc::new(SqliteJobStore::open_in_memory().await.unwrap());
    let types: HashMap<String, DataSourceType> = [
        ("staging".to_string(), DataSourceType::Sqlite),
        ("wa_messages".to_string(), DataSourceType::Lance),
        ("wa_statuses".to_string(), DataSourceType::Lance),
    ]
    .into_iter()
    .collect();
    let paths: HashMap<String, String> = ["wa_messages", "wa_statuses"]
        .iter()
        .map(|n| {
            (
                n.to_string(),
                lake.join(format!("{n}.lance")).display().to_string(),
            )
        })
        .collect();
    let executor = JobExecutor::new(jobs, store.clone() as Arc<dyn JobStore>, ctx, types, paths);
    let mut written = HashMap::new();
    for job in ["wa-merge-messages", "wa-merge-statuses"] {
        let params: HashMap<String, Value> = [("since".to_string(), json!(since.clone()))]
            .into_iter()
            .collect();
        let run_id = executor
            .submit(job, params, None)
            .await
            .unwrap_or_else(|e| panic!("{job}: {e}"));
        let run = loop {
            let run = store.get_run(&run_id).await.unwrap().expect("run exists");
            match run.status {
                JobRunStatus::Succeeded | JobRunStatus::Failed => break run,
                _ => tokio::time::sleep(std::time::Duration::from_millis(50)).await,
            }
        };
        assert_eq!(
            run.status,
            JobRunStatus::Succeeded,
            "{job}: {:?}",
            run.error
        );
        written.insert(job, run.rows_written.unwrap_or(0));
    }
    written
}
