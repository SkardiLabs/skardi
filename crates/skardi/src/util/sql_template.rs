//! Rendering `{name}` placeholders in SQL templates.
//!
//! Pipelines (in the server) and jobs both turn a SQL template plus JSON
//! parameters into SQL text. Each renders its parameter values its own way,
//! then both use [`fill_placeholders`] to put them into the template, so the
//! substitution itself is a single forward pass with one implementation.

use std::collections::HashMap;

/// Render a string as a single-quoted SQL string literal, doubling any
/// embedded single quote.
///
/// Quote-doubling is the complete escape only while the SQL parser treats a
/// backslash as an ordinary character. That holds for the engine's dialect:
/// sessions are built from DataFusion's defaults, whose
/// `datafusion.sql_parser.dialect` is `Generic`, and sqlparser's
/// `GenericDialect` keeps `supports_string_literal_backslash_escape` off.
/// Nothing in Skardi changes that setting, and the server's ad-hoc `/query`
/// rejects `SET`. A dialect with backslash escapes (MySQL, BigQuery,
/// ClickHouse, Snowflake, Redshift) would read `\'` as an escaped quote and
/// this escape would no longer be sufficient. Tests in this module and in the
/// server's pipeline handlers pin the assumption.
///
/// # Example
///
/// ```rust
/// use skardi::util::sql_template::quote_sql_string;
///
/// assert_eq!(quote_sql_string("plain"), "'plain'");
/// assert_eq!(quote_sql_string("O'Brien"), "'O''Brien'");
/// ```
pub fn quote_sql_string(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

/// Copy `template` into a new string, replacing each `{name}` whose `name` is
/// a key of `values` with that value.
///
/// The scan only moves forward through `template`: text inserted from a value
/// is never examined again. A value that itself contains `{other}` is kept
/// verbatim, and the result does not depend on the order of the
/// substitutions. Everything else, including a `{name}` whose `name` is not a
/// key of `values`, is copied as written. `{{name}}` still has its inner
/// `{name}` replaced.
///
/// # Example
///
/// ```rust
/// use std::collections::HashMap;
///
/// use skardi::util::sql_template::{fill_placeholders, quote_sql_string};
///
/// let mut values = HashMap::new();
/// // This value contains `{b}`, but inserted text is never rescanned, so it
/// // stays literal instead of picking up `b`'s value.
/// values.insert("a", quote_sql_string("it's {b}"));
/// values.insert("b", "42".to_string());
///
/// let sql = fill_placeholders("SELECT {a}, {b}, {missing}", &values);
/// assert_eq!(sql, "SELECT 'it''s {b}', 42, {missing}");
/// ```
pub fn fill_placeholders(template: &str, values: &HashMap<&str, String>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after_open = &rest[open + 1..];
        let Some(close) = after_open.find('}') else {
            // No closing brace anywhere after this point: nothing left to
            // substitute.
            out.push('{');
            rest = after_open;
            break;
        };
        if let Some(value) = values.get(&after_open[..close]) {
            out.push_str(value);
            rest = &after_open[close + 1..];
        } else {
            // Not a placeholder we substitute. Emit only the `{` and resume
            // just after it, so `{{name}}` still finds the inner `{name}`.
            out.push('{');
            rest = after_open;
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::config::Dialect;
    use datafusion::prelude::SessionConfig;

    fn values<'a>(pairs: &[(&'a str, &str)]) -> HashMap<&'a str, String> {
        pairs.iter().map(|(k, v)| (*k, v.to_string())).collect()
    }

    #[test]
    fn replaces_known_placeholders() {
        let out = fill_placeholders("SELECT {a}, {b}", &values(&[("a", "1"), ("b", "'x'")]));
        assert_eq!(out, "SELECT 1, 'x'");
    }

    #[test]
    fn inserted_text_is_not_rescanned() {
        let out = fill_placeholders(
            "SELECT {a}, {b}",
            &values(&[("a", "'{b}'"), ("b", "''' || version() || '''")]),
        );
        assert_eq!(out, "SELECT '{b}', ''' || version() || '''");
    }

    #[test]
    fn unknown_and_unbalanced_braces_are_copied() {
        let out = fill_placeholders("{x} '{nope}' '{{x}}' '{ open' '}'", &values(&[("x", "7")]));
        assert_eq!(out, "7 '{nope}' '{7}' '{ open' '}'");
    }

    #[test]
    fn empty_template_and_no_values() {
        assert_eq!(fill_placeholders("", &values(&[])), "");
        assert_eq!(fill_placeholders("SELECT {a}", &values(&[])), "SELECT {a}");
    }

    #[test]
    fn quote_sql_string_doubles_quotes_only() {
        assert_eq!(quote_sql_string("o'brien"), "'o''brien'");
        assert_eq!(quote_sql_string(r"C:\dir\"), r"'C:\dir\'");
    }

    #[test]
    fn default_parser_dialect_is_generic() {
        // `quote_sql_string` relies on this: see its doc comment.
        assert_eq!(
            SessionConfig::new().options().sql_parser.dialect,
            Dialect::Generic
        );
    }
}
