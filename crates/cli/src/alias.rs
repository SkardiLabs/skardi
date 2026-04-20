//! CLI-only user-defined aliases.
//!
//! An alias maps a short verb to a pipeline invocation, e.g. `skardi grep "..."`
//! → `skardi run wiki-search-hybrid --query="..." ...`. Aliases are a CLI-only
//! concept (the server does not read alias files); they live in an aliases
//! YAML file managed via `skardi alias add/list/remove`.
//!
//! Shape of a single entry:
//!
//! ```yaml
//! grep:
//!   pipeline: wiki-search-hybrid
//!   positional: [query]
//!   defaults:
//!     text_query: "{query}"
//!     vector_weight: "0.5"
//!     limit: "10"
//!   description: "Hybrid search over the wiki"
//! ```
//!
//! A `{name}` token in a default value is substituted from another
//! already-bound parameter (one level only). Unknown tokens are left intact.

use anyhow::Result;
use datafusion::common::ScalarValue;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::pipeline::{parse_param_flag, string_to_scalar};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasDef {
    /// Pipeline name (matches `metadata.name` of a pipeline YAML).
    pub pipeline: String,
    /// Positional CLI arg names, bound in order to pipeline params.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub positional: Vec<String>,
    /// Default values for pipeline params. A value of the form `{other_param}`
    /// is resolved against already-bound params at invocation time.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub defaults: HashMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

/// Resolution result: the target pipeline + fully-bound parameters.
pub struct ResolvedAlias {
    pub pipeline: String,
    pub params: Vec<(String, ScalarValue)>,
}

/// Resolve an alias invocation into a pipeline name + bound parameters.
///
/// Argument handling:
/// - `--name=value` / `--name value` bind (and override any positional or default).
/// - Positional args bind in order to `alias.positional`.
/// - Any pipeline param not yet bound falls back to `alias.defaults`.
/// - A default of the form `{other}` is substituted from an already-bound param.
pub fn resolve_alias(alias: &AliasDef, raw_args: &[String]) -> Result<ResolvedAlias> {
    let (flag_params, positional_vals) = split_args(raw_args)?;

    // Bind positional args → string params.
    let mut string_params: HashMap<String, String> = HashMap::new();
    for (idx, val) in positional_vals.iter().enumerate() {
        let name = alias.positional.get(idx).ok_or_else(|| {
            anyhow::anyhow!(
                "Alias takes {} positional arg(s), got {}",
                alias.positional.len(),
                positional_vals.len()
            )
        })?;
        string_params.insert(name.clone(), val.clone());
    }

    // Typed params from explicit flags win over anything else.
    let mut typed_params: HashMap<String, ScalarValue> = HashMap::new();
    for (name, scalar) in flag_params {
        typed_params.insert(name, scalar);
    }

    // Apply defaults for any param not bound via flag or positional.
    for (name, template) in &alias.defaults {
        if typed_params.contains_key(name) || string_params.contains_key(name) {
            continue;
        }
        let resolved = substitute_template(template, &string_params, &typed_params);
        string_params.insert(name.clone(), resolved);
    }

    // Merge: heuristically type string params; flag-typed params win.
    let mut merged: HashMap<String, ScalarValue> = HashMap::new();
    for (name, value) in string_params {
        merged.insert(name, string_to_scalar(&value));
    }
    for (name, scalar) in typed_params {
        merged.insert(name, scalar);
    }

    Ok(ResolvedAlias {
        pipeline: alias.pipeline.clone(),
        params: merged.into_iter().collect(),
    })
}

/// Split raw args into `--flag=value` / `--flag value` pairs and positional args.
fn split_args(raw_args: &[String]) -> Result<(Vec<(String, ScalarValue)>, Vec<String>)> {
    let mut flags: Vec<(String, ScalarValue)> = Vec::new();
    let mut positional: Vec<String> = Vec::new();

    let mut i = 0;
    while i < raw_args.len() {
        let a = &raw_args[i];
        if let Some(rest) = a.strip_prefix("--") {
            if rest.contains('=') {
                let (name, scalar) = parse_param_flag(rest)?;
                flags.push((name, scalar));
                i += 1;
            } else {
                let next = raw_args
                    .get(i + 1)
                    .ok_or_else(|| anyhow::anyhow!("Flag --{rest} is missing a value"))?;
                let combined = format!("{rest}={next}");
                let (name, scalar) = parse_param_flag(&combined)?;
                flags.push((name, scalar));
                i += 2;
            }
        } else {
            positional.push(a.clone());
            i += 1;
        }
    }

    Ok((flags, positional))
}

fn substitute_template(
    template: &str,
    string_params: &HashMap<String, String>,
    typed_params: &HashMap<String, ScalarValue>,
) -> String {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
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
            let is_ident =
                !name.is_empty() && name.chars().all(|ch| ch.is_alphanumeric() || ch == '_');
            if found_close && is_ident {
                if let Some(v) = string_params.get(&name) {
                    out.push_str(v);
                } else if let Some(sv) = typed_params.get(&name) {
                    out.push_str(&scalar_display(sv));
                } else {
                    out.push('{');
                    out.push_str(&name);
                    out.push('}');
                }
            } else {
                out.push('{');
                out.push_str(&name);
                if found_close {
                    out.push('}');
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn scalar_display(sv: &ScalarValue) -> String {
    match sv {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => s.clone(),
        ScalarValue::Int64(Some(i)) => i.to_string(),
        ScalarValue::Float64(Some(f)) => f.to_string(),
        ScalarValue::Boolean(Some(b)) => b.to_string(),
        _ => format!("{sv}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn alias_def(positional: &[&str], defaults: &[(&str, &str)]) -> AliasDef {
        AliasDef {
            pipeline: "wiki-search-hybrid".to_string(),
            positional: positional.iter().map(|s| s.to_string()).collect(),
            defaults: defaults
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            description: None,
        }
    }

    fn params_map(resolved: &ResolvedAlias) -> HashMap<String, ScalarValue> {
        resolved.params.iter().cloned().collect()
    }

    #[test]
    fn positional_plus_defaults_with_template_substitution() {
        let alias = alias_def(
            &["query"],
            &[
                ("text_query", "{query}"),
                ("vector_weight", "0.5"),
                ("limit", "10"),
            ],
        );
        let args = vec!["who invented the computer?".to_string()];
        let r = resolve_alias(&alias, &args).unwrap();
        let m = params_map(&r);
        assert_eq!(
            m["query"],
            ScalarValue::Utf8(Some("who invented the computer?".to_string()))
        );
        assert_eq!(
            m["text_query"],
            ScalarValue::Utf8(Some("who invented the computer?".to_string()))
        );
        assert_eq!(m["vector_weight"], ScalarValue::Float64(Some(0.5)));
        assert_eq!(m["limit"], ScalarValue::Int64(Some(10)));
    }

    #[test]
    fn flag_overrides_default() {
        let alias = alias_def(&["query"], &[("limit", "10")]);
        let args = vec!["hello".to_string(), "--limit=20".to_string()];
        let r = resolve_alias(&alias, &args).unwrap();
        let m = params_map(&r);
        assert_eq!(m["limit"], ScalarValue::Int64(Some(20)));
    }

    #[test]
    fn extra_positional_errors() {
        let alias = alias_def(&["query"], &[]);
        let args = vec!["one".to_string(), "two".to_string()];
        assert!(resolve_alias(&alias, &args).is_err());
    }

    #[test]
    fn flag_space_form_parses() {
        let alias = alias_def(&[], &[]);
        let args = vec!["--limit".to_string(), "5".to_string()];
        let r = resolve_alias(&alias, &args).unwrap();
        let m = params_map(&r);
        assert_eq!(m["limit"], ScalarValue::Int64(Some(5)));
    }
}
