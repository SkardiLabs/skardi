//! End-to-end: spawn the real `skardi mcp` binary and speak MCP to it over
//! stdio with an rmcp client, against a wiremock "server". Also the
//! permanent guard for the stdout-is-protocol-only invariant — any stray
//! print to stdout corrupts the JSON-RPC framing and breaks the initialize
//! handshake below.

#![cfg(unix)]

use rmcp::ServiceExt;
use rmcp::model::CallToolRequestParams;
use rmcp::transport::{ConfigureCommandExt, TokioChildProcess};
use serde_json::json;
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn spawn_transport(server_url: &str, home: &std::path::Path) -> TokioChildProcess {
    TokioChildProcess::new(
        tokio::process::Command::new(env!("CARGO_BIN_EXE_skardi")).configure(|cmd| {
            cmd.env("HOME", home)
                .env_remove("SKARDI_SERVER_URL")
                .env_remove("SKARDI_API_TOKEN")
                .env_remove("SKARDI_CONTEXT")
                .args(["mcp", "--server", server_url]);
        }),
    )
    .expect("spawn skardi mcp")
}

fn inventory() -> serde_json::Value {
    json!({"success": true, "count": 1, "data_sources": 0,
           "pipelines": [{"name": "product-search", "version": "1.0.0",
             "endpoint": "/product-search/execute",
             "description": "Filter products by brand",
             "parameters": [{"name": "brand", "data_type": "Utf8",
                             "json_schema": {"type": ["string", "null"]}}]}]})
}

#[tokio::test]
async fn initialize_list_and_call_round_trip() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/pipelines"))
        .respond_with(ResponseTemplate::new(200).set_body_json(inventory()))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/product-search/execute"))
        .and(body_json(json!({"brand": "acme"})))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"success": true, "data": [{"id": 1}], "rows": 1})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let home = tempfile::TempDir::new().unwrap();
    let client = ()
        .serve(spawn_transport(&server.uri(), home.path()))
        .await
        .expect("initialize handshake (fails on any stray stdout output)");

    let info = client.peer_info().expect("server info");
    assert!(info.capabilities.tools.is_some(), "tools capability");
    assert!(
        info.instructions.as_deref().is_some_and(|i| !i.is_empty()),
        "instructions should be set"
    );

    let tools = client.list_all_tools().await.unwrap();
    let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
    assert!(names.contains(&"product-search"), "{names:?}");
    assert!(names.contains(&"query"), "{names:?}");
    assert!(names.contains(&"list_data_sources"), "{names:?}");

    // CallToolRequestParams is #[non_exhaustive]: construct via its builder.
    let args = json!({"brand": "acme"})
        .as_object()
        .cloned()
        .expect("literal is an object");
    let result = client
        .call_tool(CallToolRequestParams::new("product-search").with_arguments(args))
        .await
        .unwrap();
    assert_eq!(result.is_error, Some(false));

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn unreachable_server_fails_list_tools_but_not_the_handshake() {
    let home = tempfile::TempDir::new().unwrap();
    // Nothing listens on port 1: the handshake still succeeds (initialize
    // needs no REST round trip); tools/list surfaces the connect error.
    let client =
        ().serve(spawn_transport("http://127.0.0.1:1", home.path()))
            .await
            .expect("handshake needs no server");
    let err = client.list_all_tools().await.unwrap_err();
    let msg = format!("{err}");
    assert!(msg.contains("cannot reach skardi-server"), "{msg}");
    client.cancel().await.unwrap();
}
