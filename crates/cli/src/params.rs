//! `-p NAME=VALUE` parameter parsing and `-d`/`-p` request-body building.
//!
//! Parameters are substituted into pipeline SQL as typed literals, so values
//! are parsed as JSON first (numbers, booleans, arrays, `null`, quoted
//! strings) and only fall back to a plain string when they aren't valid JSON
//! on their own — e.g. `user_id=1` must arrive at the server as the number
//! `1`, not the string `"1"`.

use anyhow::{Context, Result, bail};
use serde_json::{Map, Value};
use std::io::Read;

/// Parse one `-p NAME=VALUE` token into a `(name, value)` pair.
///
/// Splits on the *first* `=` only, so values may themselves contain `=`
/// (e.g. `expr=a=b` yields `("expr", "a=b")`). The value is parsed as JSON
/// first — numbers, booleans, `null`, arrays, and quoted strings all come
/// through typed — and falls back to a plain JSON string when it isn't
/// valid JSON on its own (e.g. `s=hello`).
pub fn parse_param(raw: &str) -> Result<(String, Value)> {
    let Some(eq_index) = raw.find('=') else {
        bail!("invalid -p parameter {raw:?}: expected NAME=VALUE");
    };

    let name = &raw[..eq_index];
    if name.is_empty() {
        bail!("invalid -p parameter {raw:?}: empty parameter name");
    };

    let raw_value = &raw[eq_index + 1..];
    let value = match serde_json::from_str::<Value>(raw_value) {
        Ok(value) => value,
        Err(_) => Value::String(raw_value.to_string()),
    };

    Ok((name.to_string(), value))
}

/// Build the JSON request body from an optional `-d` argument and zero or
/// more `-p NAME=VALUE` overrides.
///
/// The `-d` value (if any) is parsed as the base JSON object; each `-p`
/// then sets (or overrides) one key on top of it. Supplying neither `-d`
/// nor any `-p` yields an empty object.
pub fn build_body(data: Option<&str>, params: &[String]) -> Result<Map<String, Value>> {
    let mut body = match data {
        Some(data) => {
            let raw = resolve_data_arg(data)?;
            parse_json_object(&raw)?
        }
        None => Map::new(),
    };

    for raw_param in params {
        let (name, value) = parse_param(raw_param)?;
        body.insert(name, value);
    }

    Ok(body)
}

/// Resolve a `-d` argument to its raw text:
/// - `@path` reads the file at `path`, with a contextual error on failure.
/// - `-` reads all of stdin.
/// - anything else is treated as the JSON text itself.
fn resolve_data_arg(data: &str) -> Result<String> {
    if let Some(path) = data.strip_prefix('@') {
        std::fs::read_to_string(path)
            .with_context(|| format!("failed to read -d parameter file {path:?}"))
    } else if data == "-" {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("failed to read -d data from stdin")?;
        Ok(buf)
    } else {
        Ok(data.to_string())
    }
}

/// Parse `raw` as JSON and require it to be an object, erroring with the
/// actual JSON type name when it is not (e.g. "must be a JSON object, got
/// an array").
fn parse_json_object(raw: &str) -> Result<Map<String, Value>> {
    let value: Value =
        serde_json::from_str(raw).with_context(|| format!("invalid -d JSON: {raw:?}"))?;

    match value {
        Value::Object(map) => Ok(map),
        other => bail!(
            "invalid -d value: must be a JSON object, got {}",
            json_type_name(&other)
        ),
    }
}

/// Human-readable JSON type name for error messages.
fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use std::io::Write;
    use tempfile::NamedTempFile;

    use super::{build_body, parse_param};

    // -- parse_param --------------------------------------------------

    #[test]
    fn parse_param_json_number() {
        let (name, value) = parse_param("n=42").unwrap();
        assert_eq!(name, "n");
        assert_eq!(value, json!(42));
    }

    #[test]
    fn parse_param_json_float() {
        let (name, value) = parse_param("f=0.5").unwrap();
        assert_eq!(name, "f");
        assert_eq!(value, json!(0.5));
    }

    #[test]
    fn parse_param_json_bool() {
        let (name, value) = parse_param("b=true").unwrap();
        assert_eq!(name, "b");
        assert_eq!(value, json!(true));
    }

    #[test]
    fn parse_param_json_null() {
        let (name, value) = parse_param("z=null").unwrap();
        assert_eq!(name, "z");
        assert_eq!(value, json!(null));
    }

    #[test]
    fn parse_param_json_array() {
        let (name, value) = parse_param("a=[1,2]").unwrap();
        assert_eq!(name, "a");
        assert_eq!(value, json!([1, 2]));
    }

    #[test]
    fn parse_param_non_json_falls_back_to_plain_string() {
        let (name, value) = parse_param("s=hello").unwrap();
        assert_eq!(name, "s");
        assert_eq!(value, json!("hello"));
    }

    #[test]
    fn parse_param_quoted_json_string_is_unquoted() {
        let (name, value) = parse_param(r#"q="42""#).unwrap();
        assert_eq!(name, "q");
        assert_eq!(value, json!("42"));
    }

    #[test]
    fn parse_param_splits_on_first_equals_only() {
        let (name, value) = parse_param("expr=a=b").unwrap();
        assert_eq!(name, "expr");
        assert_eq!(value, json!("a=b"));
    }

    #[test]
    fn parse_param_missing_equals_is_error() {
        let err = parse_param("noequals").unwrap_err();
        assert!(
            err.to_string().contains("noequals"),
            "error should name the offending token: {err}"
        );
    }

    #[test]
    fn parse_param_empty_name_is_error() {
        let err = parse_param("=value").unwrap_err();
        assert!(
            err.to_string().contains("=value"),
            "error should name the offending token: {err}"
        );
    }

    // -- build_body / data-object parsing ------------------------------

    #[test]
    fn build_body_data_object_alone() {
        let body = build_body(Some(r#"{"a":1}"#), &[]).unwrap();
        assert_eq!(body, json!({"a": 1}).as_object().unwrap().clone());
    }

    #[test]
    fn build_body_data_array_is_error_naming_type() {
        let err = build_body(Some("[1,2]"), &[]).unwrap_err();
        assert!(
            err.to_string().contains("must be a JSON object"),
            "error should mention 'must be a JSON object': {err}"
        );
        assert!(
            err.to_string().contains("array"),
            "error should name the actual type (array): {err}"
        );
    }

    #[test]
    fn build_body_data_not_json_is_error() {
        let err = build_body(Some("not json"), &[]).unwrap_err();
        assert!(!err.to_string().is_empty());
    }

    #[test]
    fn build_body_merges_params_over_data() {
        let body = build_body(
            Some(r#"{"a":1,"b":"x"}"#),
            &["b=override".to_string(), "c=3".to_string()],
        )
        .unwrap();

        assert_eq!(
            body,
            json!({"a": 1, "b": "override", "c": 3})
                .as_object()
                .unwrap()
                .clone()
        );
    }

    #[test]
    fn build_body_neither_flag_is_empty_object() {
        let body = build_body(None, &[]).unwrap();
        assert!(body.is_empty());
    }

    #[test]
    fn build_body_data_at_path_reads_file() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(br#"{"user_id": 7}"#).unwrap();
        let path = format!("@{}", file.path().display());

        let body = build_body(Some(&path), &[]).unwrap();

        assert_eq!(body, json!({"user_id": 7}).as_object().unwrap().clone());
    }

    #[test]
    fn build_body_data_at_nonexistent_path_is_error() {
        let err = build_body(Some("@/nonexistent/params.json"), &[]).unwrap_err();
        assert!(!err.to_string().is_empty());
    }
}
