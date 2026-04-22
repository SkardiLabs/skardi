//! Server-wide HTTP handlers. Holds the two endpoints that are neither
//! pipeline- nor job-specific:
//!
//! * `GET /`        — the dashboard UI (renders one card per registered pipeline)
//! * `GET /health`  — liveness probe for the server process itself
//!
//! Pipeline endpoints live in [`crate::pipeline_handlers`]; job endpoints in
//! [`crate::jobs_handlers`].

use axum::{Json, extract::State, http::StatusCode};
use serde_json::Value;
use skardi::pipeline::pipeline::Pipeline;

use crate::server::AppState;

/// Health check endpoint - GET /health
pub async fn health_check() -> Result<Json<Value>, StatusCode> {
    let response = serde_json::json!({
        "status": "healthy",
        "service": "skardi-server",
        "timestamp": chrono::Utc::now().to_rfc3339()
    });

    Ok(Json(response))
}

const DASHBOARD_TEMPLATE: &str = include_str!("templates/dashboard.html");
const LOGO_PNG: &[u8] = include_bytes!("../../../asset/logo.png");

static LOGO_DATA_URI: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn logo_data_uri() -> &'static str {
    LOGO_DATA_URI.get_or_init(|| {
        use base64::Engine;
        let encoded = base64::engine::general_purpose::STANDARD.encode(LOGO_PNG);
        format!("data:image/png;base64,{encoded}")
    })
}

const PIPELINE_CARD_TEMPLATE: &str = r#"<article class="pipeline-card">
    <header>
        <h2>{{NAME}}</h2>
        <span class="version">v{{VERSION}}</span>
    </header>
    <div class="endpoint">
        <span class="method">POST</span>
        <code>{{ENDPOINT}}</code>
        <button class="copy-btn" onclick="copyToClipboard('{{ENDPOINT}}')">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
            </svg>
        </button>
    </div>
    <div class="params-section">
        <h3>Parameters</h3>
        <div class="params-list">
            {{PARAMS}}
        </div>
    </div>
    <div class="example-section">
        <h3>Example Request</h3>
        <div class="code-block">
            <pre><code class="curl">curl -X POST http://localhost:8080{{ENDPOINT}} \
  -H "Content-Type: application/json" \
  -d '{{EXAMPLE_JSON_ESCAPED}}'</code></pre>
            <button class="copy-btn" onclick="copyCodeBlock(this)">
                <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                    <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                </svg>
            </button>
        </div>
    </div>
    <div class="try-section">
        <h3>Try It</h3>
        <textarea class="request-body" placeholder="Enter JSON body...">{{EXAMPLE_JSON}}</textarea>
        <button class="execute-btn" onclick="executeRequest('{{ENDPOINT}}', this)">
            Execute
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polygon points="5 3 19 12 5 21 5 3"></polygon>
            </svg>
        </button>
        <div class="response-area" style="display: none;">
            <h4>Response</h4>
            <pre class="response-output"></pre>
        </div>
    </div>
</article>"#;

/// Serve the dashboard UI - GET /
pub async fn serve_dashboard(State(app_state): State<AppState>) -> axum::response::Html<String> {
    let config = app_state.config.read().ok();

    let pipelines_html = if let Some(config) = config {
        config
            .pipelines
            .iter()
            .map(|(name, pipeline)| {
                let params: Vec<String> = pipeline
                    .request_schema()
                    .fields
                    .iter()
                    .map(|(param_name, field_type)| {
                        format!(
                            r#"<div class="param"><span class="param-name">{}</span><span class="param-type">{:?}</span></div>"#,
                            param_name, field_type
                        )
                    })
                    .collect();

                let example_body: serde_json::Map<String, serde_json::Value> = pipeline
                    .request_schema()
                    .fields
                    .iter()
                    .map(|(param_name, field_type)| {
                        let example_value = match format!("{:?}", field_type).as_str() {
                            s if s.contains("Utf8") => serde_json::Value::String("example".to_string()),
                            s if s.contains("Int") => serde_json::Value::Number(42.into()),
                            s if s.contains("Float") => serde_json::json!(3.14),
                            s if s.contains("Boolean") => serde_json::Value::Bool(true),
                            _ => serde_json::Value::String("value".to_string()),
                        };
                        (param_name.clone(), example_value)
                    })
                    .collect();

                let example_json = serde_json::to_string_pretty(&example_body).unwrap_or_default();
                let endpoint = format!("/{}/execute", name);
                let params_html = if params.is_empty() {
                    r#"<div class="no-params">No parameters required</div>"#.to_string()
                } else {
                    params.join("\n")
                };

                PIPELINE_CARD_TEMPLATE
                    .replace("{{NAME}}", name)
                    .replace("{{VERSION}}", pipeline.version())
                    .replace("{{ENDPOINT}}", &endpoint)
                    .replace("{{PARAMS}}", &params_html)
                    .replace("{{EXAMPLE_JSON_ESCAPED}}", &example_json.replace('\'', "\\'"))
                    .replace("{{EXAMPLE_JSON}}", &example_json)
            })
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        r#"<div class="error">Failed to load pipelines</div>"#.to_string()
    };

    let html = DASHBOARD_TEMPLATE
        .replace("{{LOGO_DATA_URI}}", logo_data_uri())
        .replace("{{PIPELINES_CONTENT}}", &pipelines_html);
    axum::response::Html(html)
}
