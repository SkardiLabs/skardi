//! Pipeline YAML loader and parameter binding for the CLI.
//!
//! The CLI reuses the server's pipeline YAML format:
//!
//! ```yaml
//! metadata:
//!   name: "wiki-search-hybrid"
//!   version: "1.0.0"
//!   description: "..."
//! query: |
//!   SELECT ... WHERE slug = {slug} LIMIT {limit}
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
pub struct PipelineFile {
    pub metadata: PipelineMetadata,
    pub query: String,
}

/// Load a single pipeline YAML from disk.
pub fn load_pipeline_from_path(path: &Path) -> Result<PipelineFile> {
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("Failed to read pipeline file: {}", path.display()))?;
    let pipeline: PipelineFile = serde_yaml::from_str(&content)
        .with_context(|| format!("Failed to parse pipeline YAML: {}", path.display()))?;
    Ok(pipeline)
}

/// Scan a set of directories for `*.yaml` / `*.yml` pipeline files and
/// return a map keyed by `metadata.name`. Later directories shadow earlier
/// ones on name collision so per-project dirs can override shared ones.
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
            let pipeline = load_pipeline_from_path(&p)?;
            out.insert(pipeline.metadata.name.clone(), (p, pipeline));
        }
    }
    Ok(out)
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
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '{' {
            let mut name = String::new();
            let mut found_close = false;
            for inner in chars.by_ref() {
                if inner == '}' {
                    found_close = true;
                    break;
                }
                name.push(inner);
            }
            if found_close
                && !name.is_empty()
                && name.chars().all(|ch| ch.is_alphanumeric() || ch == '_')
            {
                let value = params
                    .get(&name)
                    .ok_or_else(|| anyhow::anyhow!("Missing value for parameter {{{name}}}"))?;
                result.push_str(&scalar_to_sql_literal(value)?);
            } else {
                result.push('{');
                result.push_str(&name);
                if found_close {
                    result.push('}');
                }
            }
        } else {
            result.push(c);
        }
    }
    Ok(result)
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
        ScalarValue::Float64(Some(f)) => {
            if f.fract() == 0.0 && f.is_finite() {
                format!("{f:.1}")
            } else {
                f.to_string()
            }
        }
        ScalarValue::Float32(Some(f)) => {
            if f.fract() == 0.0 && f.is_finite() {
                format!("{f:.1}")
            } else {
                f.to_string()
            }
        }
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
    let mut chars = sql.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '{' {
            let mut name = String::new();
            let mut found = false;
            for inner in chars.by_ref() {
                if inner == '}' {
                    found = true;
                    break;
                }
                name.push(inner);
            }
            if found
                && !name.is_empty()
                && name.chars().all(|ch| ch.is_alphanumeric() || ch == '_')
                && !params.contains(&name)
            {
                params.push(name);
            }
        }
    }
    params
}

/// Convert a CLI string value into a `ScalarValue` using a simple heuristic:
/// bool → Boolean, integer → Int64, float → Float64, otherwise Utf8.
///
/// Users who need to force a string for a numeric-looking value should wrap it
/// as a raw `str:` prefix (see `parse_param_flag`).
pub fn string_to_scalar(value: &str) -> ScalarValue {
    if value.eq_ignore_ascii_case("true") {
        return ScalarValue::Boolean(Some(true));
    }
    if value.eq_ignore_ascii_case("false") {
        return ScalarValue::Boolean(Some(false));
    }
    if let Ok(i) = value.parse::<i64>() {
        return ScalarValue::Int64(Some(i));
    }
    if let Ok(f) = value.parse::<f64>() {
        return ScalarValue::Float64(Some(f));
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
    fn string_to_scalar_heuristic_covers_common_cases() {
        assert_eq!(string_to_scalar("42"), ScalarValue::Int64(Some(42)));
        assert_eq!(string_to_scalar("3.14"), ScalarValue::Float64(Some(3.14)));
        assert_eq!(string_to_scalar("true"), ScalarValue::Boolean(Some(true)));
        assert_eq!(
            string_to_scalar("hello"),
            ScalarValue::Utf8(Some("hello".to_string()))
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
}
