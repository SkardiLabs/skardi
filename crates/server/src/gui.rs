//! Dashboard UI (`GET /`) — server-side renders the interactive HTML
//! page that exposes the three primitives this server hosts (pipelines,
//! jobs, semantics) as a tabbed catalog with Try It / Submit Run panels.
//!
//! The dashboard template lives at `src/templates/dashboard.html`; the
//! per-card fragments and empty-state strings live as `const &str`
//! literals in this module so cargo's `include_str!` keeps them
//! inlined into the binary. All user-supplied strings flow through
//! [`escape_html`] before they reach the templates.
//!
//! Other HTTP handlers — pipeline endpoints, job endpoints, the
//! `/health` probe — live in [`crate::pipeline_handlers`],
//! [`crate::jobs_handlers`], and [`crate::handlers`] respectively.

use axum::extract::State;
use serde_json::Value;
use skardi::jobs::JobDefinition;
use skardi::pipeline::pipeline::{Pipeline, StandardPipeline};

use crate::config::{DataSource, DataSourceType};
use crate::pipeline_handlers::get_table_schema;
use crate::semantics::SemanticsRegistry;
use crate::server::AppState;

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

const JOB_CARD_TEMPLATE: &str = r#"<article class="pipeline-card job-card">
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
    <div class="destination-section">
        <h3>Destination</h3>
        <div class="destination-info">
            {{DESTINATION_PAIRS}}
        </div>
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
        <h3>Submit Run</h3>
        <textarea class="request-body" placeholder="Enter JSON body...">{{EXAMPLE_JSON}}</textarea>
        <button class="execute-btn" onclick="submitJobRun('{{NAME}}', this)">
            Submit Run
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <polygon points="5 3 19 12 5 21 5 3"></polygon>
            </svg>
        </button>
        <div class="response-area" style="display: none;">
            <h4>Response</h4>
            <pre class="response-output"></pre>
        </div>
    </div>
    <div class="runs-section">
        <header>
            <h3>Recent Runs</h3>
            <button class="runs-refresh" onclick="refreshRuns('{{NAME}}')">Refresh</button>
        </header>
        <div class="runs-list" data-runs-for="{{NAME}}">
            <div class="runs-empty">Loading…</div>
        </div>
    </div>
</article>"#;

const SEMANTICS_CARD_TEMPLATE: &str = r#"<article class="semantics-card">
    <header>
        <h2>{{NAME}}</h2>
        <span class="source-type">{{TYPE}}</span>
    </header>
    {{DESCRIPTION_BLOCK}}
    <div class="columns-section">
        <h3>Columns</h3>
        <div class="columns-table">
            {{COLUMNS}}
        </div>
    </div>
</article>"#;

const JOBS_DISABLED_EMPTY: &str = r#"<div class="empty-state">
    <p>Jobs subsystem is not enabled on this server.</p>
    <p style="margin-top:0.5rem;font-size:0.85rem;">Start <code>skardi-server</code> with <code>--jobs &lt;path&gt;</code> to register job definitions.</p>
</div>"#;

const JOBS_NONE_EMPTY: &str = r#"<div class="empty-state">
    <p>No jobs registered.</p>
</div>"#;

const SEMANTICS_NONE_EMPTY: &str = r#"<div class="empty-state">
    <p>No data sources registered.</p>
</div>"#;

/// HTML-escape a string so user-supplied descriptions and SQL identifiers
/// can be safely interpolated into the dashboard template.
fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            other => out.push(other),
        }
    }
    out
}

fn render_params(params: &[(String, String)]) -> String {
    if params.is_empty() {
        r#"<div class="no-params">No parameters required</div>"#.to_string()
    } else {
        params
            .iter()
            .map(|(name, ty)| {
                format!(
                    r#"<div class="param"><span class="param-name">{}</span><span class="param-type">{}</span></div>"#,
                    escape_html(name),
                    escape_html(ty),
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn example_value_for_type(field_type: &str) -> Value {
    match field_type {
        s if s.contains("Utf8") => Value::String("example".to_string()),
        s if s.contains("Int") => Value::Number(42.into()),
        s if s.contains("Float") => serde_json::json!(3.14),
        s if s.contains("Boolean") => Value::Bool(true),
        _ => Value::String("value".to_string()),
    }
}

fn render_pipeline_card(name: &str, pipeline: &impl Pipeline) -> String {
    let schema = pipeline.request_schema();
    let params: Vec<(String, String)> = schema
        .fields
        .iter()
        .map(|(n, t)| (n.clone(), format!("{:?}", t)))
        .collect();

    let example_body: serde_json::Map<String, Value> = schema
        .fields
        .iter()
        .map(|(n, t)| (n.clone(), example_value_for_type(&format!("{:?}", t))))
        .collect();
    let example_json = serde_json::to_string_pretty(&example_body).unwrap_or_default();
    let endpoint = format!("/{}/execute", name);

    PIPELINE_CARD_TEMPLATE
        .replace("{{NAME}}", &escape_html(name))
        .replace("{{VERSION}}", &escape_html(pipeline.version()))
        .replace("{{ENDPOINT}}", &endpoint)
        .replace("{{PARAMS}}", &render_params(&params))
        .replace(
            "{{EXAMPLE_JSON_ESCAPED}}",
            &example_json.replace('\'', "\\'"),
        )
        .replace("{{EXAMPLE_JSON}}", &escape_html(&example_json))
}

fn render_destination_pairs(job: &JobDefinition) -> String {
    let mode = format!("{:?}", job.destination.mode).to_lowercase();
    let mut parts = vec![
        format!(
            r#"<span class="dest-pair"><span class="dest-key">Table</span><span class="dest-value">{}</span></span>"#,
            escape_html(&job.destination.table)
        ),
        format!(
            r#"<span class="dest-pair"><span class="dest-key">Mode</span><span class="dest-value">{}</span></span>"#,
            escape_html(&mode)
        ),
        format!(
            r#"<span class="dest-pair"><span class="dest-key">Create if missing</span><span class="dest-value">{}</span></span>"#,
            job.destination.create_if_missing
        ),
    ];
    if let Some(timeout_ms) = job.execution.timeout_ms {
        parts.push(format!(
            r#"<span class="dest-pair"><span class="dest-key">Timeout</span><span class="dest-value">{}ms</span></span>"#,
            timeout_ms
        ));
    }
    parts.join("\n")
}

fn render_job_card(job: &JobDefinition) -> String {
    let schema = &job.pipeline.request_schema;
    let params: Vec<(String, String)> = schema
        .fields
        .iter()
        .map(|(n, t)| (n.clone(), format!("{:?}", t)))
        .collect();

    let example_body: serde_json::Map<String, Value> = schema
        .fields
        .iter()
        .map(|(n, t)| (n.clone(), example_value_for_type(&format!("{:?}", t))))
        .collect();
    let example_json = serde_json::to_string_pretty(&example_body).unwrap_or_default();
    let endpoint = format!("/jobs/{}/run", job.name());

    JOB_CARD_TEMPLATE
        .replace("{{NAME}}", &escape_html(job.name()))
        .replace("{{VERSION}}", &escape_html(job.version()))
        .replace("{{ENDPOINT}}", &endpoint)
        .replace("{{DESTINATION_PAIRS}}", &render_destination_pairs(job))
        .replace("{{PARAMS}}", &render_params(&params))
        .replace(
            "{{EXAMPLE_JSON_ESCAPED}}",
            &example_json.replace('\'', "\\'"),
        )
        .replace("{{EXAMPLE_JSON}}", &escape_html(&example_json))
}

fn data_source_type_str(t: &DataSourceType) -> &'static str {
    match t {
        DataSourceType::Csv => "csv",
        DataSourceType::Parquet => "parquet",
        DataSourceType::Postgres => "postgres",
        DataSourceType::Mysql => "mysql",
        DataSourceType::Iceberg => "iceberg",
        DataSourceType::Mongo => "mongo",
        DataSourceType::Sqlite => "sqlite",
        DataSourceType::Lance => "lance",
        DataSourceType::Redis => "redis",
        DataSourceType::Seekdb => "seekdb",
        DataSourceType::Influxdb => "influxdb",
        DataSourceType::Documents => "documents",
        DataSourceType::Dynamodb => "dynamodb",
    }
}

async fn render_semantics_card(
    ds: &DataSource,
    semantics: &SemanticsRegistry,
    session_ctx: &datafusion::prelude::SessionContext,
) -> String {
    let description_block = match semantics.table_description(&ds.name) {
        Some(desc) => format!(
            r#"<div class="source-description">{}</div>"#,
            escape_html(desc)
        ),
        None => {
            r#"<div class="source-description no-desc">No description provided.</div>"#.to_string()
        }
    };

    let columns_html = match get_table_schema(session_ctx, &ds.name, semantics).await {
        Ok(fields) if !fields.is_empty() => fields
            .iter()
            .map(|f| {
                let desc_html = match f.description.as_deref() {
                    Some(d) => format!(
                        r#"<div class="col-description">{}</div>"#,
                        escape_html(d)
                    ),
                    None => r#"<div class="col-description no-desc">No description.</div>"#
                        .to_string(),
                };
                format!(
                    r#"<div class="col-row"><span class="col-name">{}</span><span class="col-type">{}</span>{}</div>"#,
                    escape_html(&f.name),
                    escape_html(&f.r#type),
                    desc_html
                )
            })
            .collect::<Vec<_>>()
            .join("\n"),
        Ok(_) => r#"<div class="col-row"><span class="col-name no-desc">Schema not available.</span></div>"#.to_string(),
        Err(_) => r#"<div class="col-row"><span class="col-name no-desc">Schema not available.</span></div>"#.to_string(),
    };

    SEMANTICS_CARD_TEMPLATE
        .replace("{{NAME}}", &escape_html(&ds.name))
        .replace("{{TYPE}}", data_source_type_str(&ds.source_type))
        .replace("{{DESCRIPTION_BLOCK}}", &description_block)
        .replace("{{COLUMNS}}", &columns_html)
}

/// Serve the dashboard UI - GET /
pub async fn serve_dashboard(State(app_state): State<AppState>) -> axum::response::Html<String> {
    let jobs_enabled = app_state.jobs.is_some();

    // Render the synchronous sections (pipelines, jobs) while holding the
    // read lock; clone what semantics rendering needs so the lock can be
    // released before any `.await` on schema lookups.
    let (pipelines_html, jobs_html, sources, semantics, pipeline_count, job_count) = {
        let Ok(cfg) = app_state.config.read() else {
            let err = r#"<div class="empty-state"><p>Failed to load configuration.</p></div>"#
                .to_string();
            let html = DASHBOARD_TEMPLATE
                .replace("{{LOGO_DATA_URI}}", logo_data_uri())
                .replace("{{PIPELINE_COUNT}}", "0")
                .replace("{{JOB_COUNT}}", "0")
                .replace("{{SEMANTICS_COUNT}}", "0")
                .replace("{{PIPELINES_CONTENT}}", &err)
                .replace("{{JOBS_CONTENT}}", &err)
                .replace("{{SEMANTICS_CONTENT}}", &err);
            return axum::response::Html(html);
        };

        let mut pipeline_entries: Vec<(&String, &StandardPipeline)> =
            cfg.pipelines.iter().collect();
        pipeline_entries.sort_by(|a, b| a.0.cmp(b.0));

        let pipelines_html = if pipeline_entries.is_empty() {
            r#"<div class="empty-state"><p>No pipelines registered.</p></div>"#.to_string()
        } else {
            pipeline_entries
                .iter()
                .map(|(name, pipeline)| render_pipeline_card(name, *pipeline))
                .collect::<Vec<_>>()
                .join("\n")
        };

        let mut job_entries: Vec<&JobDefinition> = cfg.jobs.values().collect();
        job_entries.sort_by(|a, b| a.name().cmp(b.name()));

        let jobs_html = if !jobs_enabled {
            JOBS_DISABLED_EMPTY.to_string()
        } else if job_entries.is_empty() {
            JOBS_NONE_EMPTY.to_string()
        } else {
            job_entries
                .iter()
                .map(|j| render_job_card(j))
                .collect::<Vec<_>>()
                .join("\n")
        };

        let mut sources: Vec<DataSource> = cfg.data_sources.clone();
        sources.sort_by(|a, b| a.name.cmp(&b.name));

        (
            pipelines_html,
            jobs_html,
            sources,
            cfg.semantics.clone(),
            cfg.pipelines.len(),
            cfg.jobs.len(),
        )
    };

    let semantics_count = sources.len();
    let semantics_html = if sources.is_empty() {
        SEMANTICS_NONE_EMPTY.to_string()
    } else {
        let session_ctx = app_state.engine.session_context();
        let mut rendered = Vec::with_capacity(sources.len());
        for ds in &sources {
            rendered.push(render_semantics_card(ds, &semantics, session_ctx).await);
        }
        rendered.join("\n")
    };

    let html = DASHBOARD_TEMPLATE
        .replace("{{LOGO_DATA_URI}}", logo_data_uri())
        .replace("{{PIPELINE_COUNT}}", &pipeline_count.to_string())
        .replace("{{JOB_COUNT}}", &job_count.to_string())
        .replace("{{SEMANTICS_COUNT}}", &semantics_count.to_string())
        .replace("{{PIPELINES_CONTENT}}", &pipelines_html)
        .replace("{{JOBS_CONTENT}}", &jobs_html)
        .replace("{{SEMANTICS_CONTENT}}", &semantics_html);
    axum::response::Html(html)
}
