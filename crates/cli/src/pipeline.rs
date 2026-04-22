//! Pipeline YAML loader and parameter binding for the CLI.
//!
//! The CLI reuses the server's pipeline YAML format:
//!
//! ```yaml
//! kind: pipeline
//! metadata:
//!   name: "wiki-search-hybrid"
//!   version: "1.0.0"
//!   description: "..."
//! spec:
//!   query: |
//!     SELECT ... WHERE slug = {slug} LIMIT {limit}
//! ```
//!
//! Placeholders of the form `{name}` in the SQL are converted to DataFusion's
//! native `$name` parameter markers and bound at execution time. This matches
//! the server's execution path (see `crates/server/src/handlers.rs::execute_pipeline_by_name`).

use anyhow::{Context, Result};
use datafusion::common::ScalarValue;
use serde::Deserialize;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
pub struct PipelineMetadata {
    pub name: String,
}

#[derive(Debug, Deserialize)]
pub struct PipelineSpec {
    pub query: String,
}

/// On-disk envelope for a pipeline YAML.
#[derive(Debug, Deserialize)]
struct PipelineEnvelope {
    kind: String,
    metadata: PipelineMetadata,
    spec: PipelineSpec,
}

#[derive(Debug)]
pub struct PipelineFile {
    pub metadata: PipelineMetadata,
    pub query: String,
}

/// Load a single pipeline YAML from disk.
///
/// Returns an error if the file is not a `kind: pipeline` document —
/// `kind: job` files have their own loader path (`JobDefinition`), and
/// any other kind is unsupported here.
pub fn load_pipeline_from_path(path: &Path) -> Result<PipelineFile> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read pipeline file: {}", path.display()))?;
    let env: PipelineEnvelope = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse pipeline YAML: {}", path.display()))?;
    if env.kind != "pipeline" {
        anyhow::bail!(
            "Expected `kind: pipeline` in {}, got `kind: {}`",
            path.display(),
            env.kind
        );
    }
    Ok(PipelineFile {
        metadata: env.metadata,
        query: env.spec.query,
    })
}

/// Scan a set of directories for `*.yaml` / `*.yml` pipeline files and
/// return a map keyed by `metadata.name`. Later directories shadow earlier
/// ones on name collision so per-project dirs can override shared ones.
///
/// Files whose root `kind:` is anything other than `pipeline` (e.g.
/// `kind: job`) are skipped silently — a pipelines dir is expected to
/// hold a mixed bag of resource YAMLs.
pub fn discover_pipelines(dirs: &[PathBuf]) -> Result<HashMap<String, (PathBuf, PipelineFile)>> {
    let mut out: HashMap<String, (PathBuf, PipelineFile)> = HashMap::new();
    for dir in dirs {
        if !dir.is_dir() {
            continue;
        }
        let entries = std::fs::read_dir(dir)
            .with_context(|| format!("Failed to read pipeline dir: {}", dir.display()))?;
        for entry in entries {
            let entry = entry?;
            let p = entry.path();
            if !p.is_file() {
                continue;
            }
            let ext = p
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            if ext != "yaml" && ext != "yml" {
                continue;
            }
            // Peek at `kind:` so non-pipeline files (jobs, contexts, etc.)
            // that happen to sit in the pipelines dir are skipped rather
            // than erroring out the whole discovery pass.
            let raw = std::fs::read_to_string(&p)
                .with_context(|| format!("Failed to read pipeline file: {}", p.display()))?;
            let peek: serde_yaml::Value = match serde_yaml::from_str(&raw) {
                Ok(v) => v,
                Err(_) => continue,
            };
            if peek.get("kind").and_then(|v| v.as_str()) != Some("pipeline") {
                continue;
            }

            let pipeline = load_pipeline_from_path(&p)?;
            out.insert(pipeline.metadata.name.clone(), (p, pipeline));
        }
    }
    Ok(out)
}

/// A segment yielded by [`scan_segments`]: either literal text passed
/// through verbatim, or a recognised `{ident}` placeholder.
pub(crate) enum Segment<'a> {
    Literal(&'a str),
    Placeholder(&'a str),
}

/// Scan a template string into literal and `{ident}` placeholder segments.
///
/// An "ident" is a non-empty run of `{ch | ch.is_alphanumeric() || ch == '_'}`
/// followed immediately by `}`. Anything else (bare `{`, `{}`,
/// `{name with space}`, unterminated `{foo`) is emitted as `Literal`.
///
/// This is the single source of truth for placeholder scanning — callers
/// that need param names, inline SQL substitution, or alias template
/// expansion all drive off these segments so they agree on what counts as a
/// placeholder.
pub(crate) fn scan_segments(s: &str) -> Vec<Segment<'_>> {
    let mut out: Vec<Segment<'_>> = Vec::new();
    let bytes = s.as_bytes();
    let mut literal_start = 0usize;
    let mut i = 0usize;

    while let Some(rel) = bytes[i..].iter().position(|&b| b == b'{') {
        let brace_at = i + rel;
        let body_start = brace_at + 1;

        // Consume the longest ident-char run starting at body_start.
        let mut cursor = body_start;
        for (rel, ch) in s[body_start..].char_indices() {
            if ch.is_alphanumeric() || ch == '_' {
                cursor = body_start + rel + ch.len_utf8();
            } else {
                break;
            }
        }

        let is_placeholder = cursor > body_start && cursor < bytes.len() && bytes[cursor] == b'}';
        if is_placeholder {
            if brace_at > literal_start {
                out.push(Segment::Literal(&s[literal_start..brace_at]));
            }
            out.push(Segment::Placeholder(&s[body_start..cursor]));
            literal_start = cursor + 1;
            i = literal_start;
        } else {
            // Leave the '{' as part of the surrounding literal; keep scanning
            // after it so a later valid placeholder on the same line is still
            // recognised (e.g. `{not a param} then {real}`).
            i = brace_at + 1;
        }
    }

    if literal_start < bytes.len() {
        out.push(Segment::Literal(&s[literal_start..]));
    }
    out
}

/// Render a SQL template by substituting `{name}` placeholders with
/// SQL-safe literal syntax for the bound parameter values.
///
/// We substitute inline rather than use DataFusion's `$name` parameter
/// binding because some UDTFs (notably `pg_fts` / `sqlite_fts`) call
/// `extract_string` on their args at plan time — before
/// `DataFrame::with_param_values` substitutes `Placeholder` expressions —
/// and reject anything that is not a `Literal`. Inline substitution happens
/// before DataFusion sees the SQL, so UDTFs get plain literals. This is
/// safe for the CLI (single-tenant, args sourced from the user's own shell
/// or from pipeline-owned YAML defaults) as long as strings are quoted with
/// standard SQL escaping, which [`scalar_to_sql_literal`] does.
pub fn render_sql_with_inline_params(
    sql: &str,
    params: &HashMap<String, ScalarValue>,
) -> Result<String> {
    let mut out = String::with_capacity(sql.len());
    for seg in scan_segments(sql) {
        match seg {
            Segment::Literal(s) => out.push_str(s),
            Segment::Placeholder(name) => {
                let value = params
                    .get(name)
                    .ok_or_else(|| anyhow::anyhow!("Missing value for parameter {{{name}}}"))?;
                out.push_str(&scalar_to_sql_literal(value)?);
            }
        }
    }
    Ok(out)
}

/// Render a finite float as a SQL literal. Integers get a `.0` suffix so the
/// result always parses as a float; non-finite floats (NaN, ±Inf) are
/// rejected since `NaN` / `inf` are not valid SQL literals.
fn render_float(f: f64) -> Result<String> {
    if !f.is_finite() {
        anyhow::bail!("Non-finite float ({f}) cannot be rendered as a SQL literal");
    }
    Ok(if f.fract() == 0.0 {
        format!("{f:.1}")
    } else {
        f.to_string()
    })
}

/// Format a `ScalarValue` as a SQL literal. Strings are single-quoted with
/// internal single quotes doubled (`'` → `''`); numbers emit raw; booleans
/// emit `TRUE`/`FALSE`; NULL emits `NULL`.
pub fn scalar_to_sql_literal(value: &ScalarValue) -> Result<String> {
    Ok(match value {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            format!("'{}'", s.replace('\'', "''"))
        }
        ScalarValue::Utf8(None) | ScalarValue::LargeUtf8(None) | ScalarValue::Null => {
            "NULL".to_string()
        }
        ScalarValue::Int64(Some(i)) => i.to_string(),
        ScalarValue::Int32(Some(i)) => i.to_string(),
        ScalarValue::Float64(Some(f)) => render_float(*f)?,
        ScalarValue::Float32(Some(f)) => render_float(*f as f64)?,
        ScalarValue::Boolean(Some(b)) => {
            if *b {
                "TRUE".to_string()
            } else {
                "FALSE".to_string()
            }
        }
        ScalarValue::Int64(None)
        | ScalarValue::Int32(None)
        | ScalarValue::Float64(None)
        | ScalarValue::Float32(None)
        | ScalarValue::Boolean(None) => "NULL".to_string(),
        other => anyhow::bail!("Unsupported parameter scalar for inline substitution: {other:?}"),
    })
}

/// Extract the unique parameter names from `{name}` placeholders, preserving
/// first-seen order. Used to validate that every placeholder has a bound value.
pub fn extract_param_names(sql: &str) -> Vec<String> {
    let mut params: Vec<String> = Vec::new();
    for seg in scan_segments(sql) {
        if let Segment::Placeholder(name) = seg
            && !params.iter().any(|p| p == name)
        {
            params.push(name.to_string());
        }
    }
    params
}

/// Infer a `ScalarValue` for a raw CLI parameter string by parsing it with
/// `serde_json`. This deliberately mirrors the server's JSON-typed parameter
/// binding (see `crates/server/src/handlers.rs::json_to_scalar`), so an
/// invocation of
///
/// | CLI                                | Server JSON equivalent |
/// |------------------------------------|------------------------|
/// | `--param foo=42`                   | `{"foo": 42}`          |
/// | `--param foo=3.14`                 | `{"foo": 3.14}`        |
/// | `--param foo=true`                 | `{"foo": true}`        |
/// | `--param foo=null`                 | `{"foo": null}`        |
/// | `--param foo=hello`                | `{"foo": "hello"}`     |
/// | `--param 'foo:str=42'`             | `{"foo": "42"}`        |
///
/// produces the same `ScalarValue` on both sides. For anything that does not
/// parse as a JSON primitive (objects, arrays, or just plain free text that
/// isn't a JSON value) we fall back to treating the raw input as `Utf8`,
/// which is the only reasonable default for an arbitrary shell argument.
///
/// Callers that want to pin a type unambiguously can still use
/// `NAME:TYPE=VALUE` (see [`parse_param_flag`]).
pub fn string_to_scalar(value: &str) -> ScalarValue {
    if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(value) {
        match parsed {
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    return ScalarValue::Int64(Some(i));
                }
                if let Some(f) = n.as_f64() {
                    return ScalarValue::Float64(Some(f));
                }
            }
            serde_json::Value::Bool(b) => return ScalarValue::Boolean(Some(b)),
            serde_json::Value::Null => return ScalarValue::Utf8(None),
            serde_json::Value::String(s) => return ScalarValue::Utf8(Some(s)),
            // Objects / arrays: fall through to the raw-string path. The CLI
            // can't render a List as a SQL literal anyway (see
            // `scalar_to_sql_literal`), and objects aren't a supported
            // pipeline-param shape on either CLI or server.
            _ => {}
        }
    }
    ScalarValue::Utf8(Some(value.to_string()))
}

/// Parse a `NAME=VALUE` flag (optionally `NAME:TYPE=VALUE`) into a typed
/// `(name, scalar)` pair. Supported explicit types: `str`, `int`, `float`,
/// `bool`. Without an explicit type, [`string_to_scalar`] is used.
pub fn parse_param_flag(raw: &str) -> Result<(String, ScalarValue)> {
    let (name_part, value) = raw
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("Parameter flag must be NAME=VALUE: {raw}"))?;

    let (name, ty) = match name_part.split_once(':') {
        Some((n, t)) => (n.to_string(), Some(t)),
        None => (name_part.to_string(), None),
    };

    if name.is_empty() {
        anyhow::bail!("Parameter name must not be empty: {raw}");
    }

    let scalar = match ty {
        None => string_to_scalar(value),
        Some("str") | Some("string") => ScalarValue::Utf8(Some(value.to_string())),
        Some("int") | Some("i64") => ScalarValue::Int64(Some(
            value
                .parse::<i64>()
                .with_context(|| format!("Expected integer for {name}: {value}"))?,
        )),
        Some("float") | Some("f64") => ScalarValue::Float64(Some(
            value
                .parse::<f64>()
                .with_context(|| format!("Expected float for {name}: {value}"))?,
        )),
        Some("bool") => ScalarValue::Boolean(Some(
            value
                .parse::<bool>()
                .with_context(|| format!("Expected bool for {name}: {value}"))?,
        )),
        Some(other) => anyhow::bail!("Unknown parameter type '{other}' in {raw}"),
    };

    Ok((name, scalar))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_param_names_preserves_order_and_dedups() {
        let sql = "SELECT {a}, {b}, {a}, {c_1}";
        let got = extract_param_names(sql);
        assert_eq!(got, vec!["a", "b", "c_1"]);
    }

    #[test]
    fn string_to_scalar_mirrors_server_json_typing() {
        // Primitives that have a natural JSON form should produce the same
        // ScalarValue that the server's `json_to_scalar` would produce for
        // `{"x": <same value>}`.
        assert_eq!(string_to_scalar("42"), ScalarValue::Int64(Some(42)));
        assert_eq!(string_to_scalar("-7"), ScalarValue::Int64(Some(-7)));
        assert_eq!(string_to_scalar("0.5"), ScalarValue::Float64(Some(0.5)));
        assert_eq!(string_to_scalar("1e3"), ScalarValue::Float64(Some(1e3)));
        assert_eq!(string_to_scalar("true"), ScalarValue::Boolean(Some(true)));
        assert_eq!(string_to_scalar("false"), ScalarValue::Boolean(Some(false)));
        // JSON null → Utf8(None), matching `json_to_scalar`'s handling of
        // `Value::Null`. This is the only way to pass SQL NULL via --param.
        assert_eq!(string_to_scalar("null"), ScalarValue::Utf8(None));
        // A JSON-quoted string strips the quotes and becomes Utf8.
        assert_eq!(
            string_to_scalar("\"hello\""),
            ScalarValue::Utf8(Some("hello".to_string()))
        );
        // Anything that isn't a JSON primitive is just a string.
        assert_eq!(
            string_to_scalar("hello"),
            ScalarValue::Utf8(Some("hello".to_string()))
        );
        // Strict JSON rules mean `TRUE` / `True` are NOT recognised as
        // booleans — matching the server, which only honours JSON's lowercase
        // `true`/`false`. This also eliminates the case-insensitive quirk of
        // the old heuristic.
        assert_eq!(
            string_to_scalar("TRUE"),
            ScalarValue::Utf8(Some("TRUE".to_string()))
        );
        // Strict JSON also means `.5` (no leading zero) is not a number.
        assert_eq!(
            string_to_scalar(".5"),
            ScalarValue::Utf8(Some(".5".to_string()))
        );
    }

    #[test]
    fn parse_param_flag_handles_explicit_types() {
        let (name, scalar) = parse_param_flag("limit:int=10").unwrap();
        assert_eq!(name, "limit");
        assert_eq!(scalar, ScalarValue::Int64(Some(10)));

        let (name, scalar) = parse_param_flag("query:str=42").unwrap();
        assert_eq!(name, "query");
        assert_eq!(scalar, ScalarValue::Utf8(Some("42".to_string())));
    }

    #[test]
    fn parse_param_flag_requires_equals() {
        assert!(parse_param_flag("limit 10").is_err());
    }

    #[test]
    fn render_inline_handles_shared_prefix_params_independently() {
        // Regression guard: {user} and {user_id} must each substitute against
        // their own binding, not leak into the prefix of a longer name.
        let mut params: HashMap<String, ScalarValue> = HashMap::new();
        params.insert(
            "user".to_string(),
            ScalarValue::Utf8(Some("bob".to_string())),
        );
        params.insert("user_id".to_string(), ScalarValue::Int64(Some(42)));
        let out = render_sql_with_inline_params(
            "SELECT * WHERE name = {user} AND id = {user_id}",
            &params,
        )
        .unwrap();
        assert_eq!(out, "SELECT * WHERE name = 'bob' AND id = 42");
    }

    #[test]
    fn render_inline_substitutes_string_with_escaping() {
        let mut params: HashMap<String, ScalarValue> = HashMap::new();
        params.insert(
            "q".to_string(),
            ScalarValue::Utf8(Some("foo's bar".to_string())),
        );
        params.insert("n".to_string(), ScalarValue::Int64(Some(42)));
        let out = render_sql_with_inline_params("SELECT {q}, {n}", &params).unwrap();
        assert_eq!(out, "SELECT 'foo''s bar', 42");
    }

    #[test]
    fn render_inline_errors_on_missing_param() {
        let params: HashMap<String, ScalarValue> = HashMap::new();
        assert!(render_sql_with_inline_params("WHERE x = {y}", &params).is_err());
    }

    #[test]
    fn render_inline_preserves_non_placeholder_braces() {
        let params: HashMap<String, ScalarValue> = HashMap::new();
        // `{not a param}` contains spaces → treated as literal, not a placeholder.
        let out = render_sql_with_inline_params("SELECT '{not a param}'", &params).unwrap();
        assert_eq!(out, "SELECT '{not a param}'");
    }

    #[test]
    fn scalar_to_sql_literal_covers_common_cases() {
        assert_eq!(
            scalar_to_sql_literal(&ScalarValue::Int64(Some(42))).unwrap(),
            "42"
        );
        assert_eq!(
            scalar_to_sql_literal(&ScalarValue::Float64(Some(0.5))).unwrap(),
            "0.5"
        );
        assert_eq!(
            scalar_to_sql_literal(&ScalarValue::Float64(Some(1.0))).unwrap(),
            "1.0"
        );
        assert_eq!(
            scalar_to_sql_literal(&ScalarValue::Boolean(Some(true))).unwrap(),
            "TRUE"
        );
        assert_eq!(
            scalar_to_sql_literal(&ScalarValue::Utf8(None)).unwrap(),
            "NULL"
        );
    }

    #[test]
    fn scalar_to_sql_literal_rejects_non_finite_floats() {
        // NaN / ±Inf have no valid SQL literal form; silently emitting
        // `NaN` / `inf` would let a malformed pipeline run and fail later
        // inside DataFusion with a confusing SQL parse error.
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            assert!(scalar_to_sql_literal(&ScalarValue::Float64(Some(bad))).is_err());
        }
        for bad in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            assert!(scalar_to_sql_literal(&ScalarValue::Float32(Some(bad))).is_err());
        }
    }
}
