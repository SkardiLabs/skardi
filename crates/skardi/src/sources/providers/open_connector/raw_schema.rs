//! Deterministic row types for raw-action scans.
//!
//! `open_connector_scan` has no source pack to declare its Arrow schema, so
//! the schema is derived at planning time from the *discovered* action
//! output JSON Schema (in-memory — planning never contacts the gateway).
//! The derivation is deliberately conservative:
//!
//! - the row-path target must be an array of objects with declared
//!   properties — otherwise planning fails with a message recommending a
//!   built-in source-pack table or a new source-pack contribution;
//! - stable primitives (`string`, `integer`, `number`, `boolean`) become
//!   typed nullable columns, including the `["T", "null"]` union spelling;
//! - everything else (objects, arrays, unions, undeclared types) becomes an
//!   opaque JSON-string column rather than a guess.

use serde_json::Value;

use super::error::OpenConnectorError;
use super::json_to_arrow::{ColumnSpec, FieldType};
use super::row_path::RowPath;

/// Derive the raw-scan columns for `action_id` from its discovered output
/// schema, at the row array located by `row_path`.
///
/// Columns are sorted by name so the derived Arrow schema does not depend on
/// the gateway's property serialization order.
pub(crate) fn derive_raw_columns(
    action_id: &str,
    output_schema: Option<&Value>,
    row_path: &RowPath,
) -> Result<Vec<ColumnSpec>, OpenConnectorError> {
    let fail = |reason: String| OpenConnectorError::RawRowTypeIndeterminate {
        action_id: action_id.to_string(),
        row_path: row_path.as_str().to_string(),
        reason,
    };

    let mut node =
        output_schema.ok_or_else(|| fail("the action declares no output schema".to_string()))?;

    // Walk the row path through nested `properties` declarations.
    for segment in row_path.segments() {
        let properties = node
            .get("properties")
            .and_then(Value::as_object)
            .ok_or_else(|| {
                fail(format!(
                    "the schema containing segment '{segment}' declares no object properties"
                ))
            })?;
        node = properties.get(segment).ok_or_else(|| {
            fail(format!(
                "segment '{segment}' is not declared in the output schema"
            ))
        })?;
    }

    if schema_type(node) != Some("array") {
        return Err(fail(format!(
            "the row-path target is declared as {}, not an array of row objects",
            schema_type(node).unwrap_or("an undeclared type")
        )));
    }
    let items = node
        .get("items")
        .filter(|items| items.is_object())
        .ok_or_else(|| fail("the row array declares no single item schema".to_string()))?;
    if schema_type(items) != Some("object") {
        return Err(fail(format!(
            "the row items are declared as {}, not objects",
            schema_type(items).unwrap_or("an undeclared type")
        )));
    }
    let properties = items
        .get("properties")
        .and_then(Value::as_object)
        .filter(|properties| !properties.is_empty())
        .ok_or_else(|| fail("the row object schema declares no properties".to_string()))?;

    let mut columns = Vec::with_capacity(properties.len());
    for (name, property) in properties {
        // A column's path round-trips through the converter's dotted-path
        // parser, so a dotted property name would silently address a nested
        // key instead of itself.
        if name.is_empty() || name.contains('.') {
            return Err(fail(format!(
                "property '{name}' cannot be exposed as a column (empty or dotted name)"
            )));
        }
        let (field_type, nullable) = column_type(property);
        columns.push(ColumnSpec {
            name: name.clone(),
            path: name.clone(),
            field_type,
            nullable,
        });
    }
    columns.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(columns)
}

/// The declared `type` of a schema node, collapsing the `["T", "null"]`
/// union spelling to `T`. `None` for undeclared or wider unions.
fn schema_type(node: &Value) -> Option<&str> {
    match node.get("type") {
        Some(Value::String(t)) => Some(t.as_str()),
        Some(Value::Array(types)) => {
            let mut non_null = types
                .iter()
                .filter_map(Value::as_str)
                .filter(|t| *t != "null");
            match (non_null.next(), non_null.next()) {
                (Some(t), None) => Some(t),
                _ => None,
            }
        }
        _ => None,
    }
}

/// Map one property schema to a column type. All raw-scan columns are
/// nullable: a discovered `required` list is provider-authored metadata, and
/// trusting it would turn a provider omission into a failed scan.
fn column_type(property: &Value) -> (FieldType, bool) {
    let field_type = match schema_type(property) {
        Some("string") => FieldType::Utf8,
        Some("integer") => FieldType::Int64,
        Some("number") => FieldType::Float64,
        Some("boolean") => FieldType::Boolean,
        // Objects, arrays, wide unions, undeclared types: opaque JSON.
        _ => FieldType::Json,
    };
    (field_type, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn path(raw: &str) -> RowPath {
        RowPath::parse(raw).expect("valid path")
    }

    fn issues_schema() -> Value {
        json!({
            "type": "object",
            "properties": {
                "issues": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "number": {"type": "integer"},
                            "title": {"type": "string"},
                            "score": {"type": "number"},
                            "open": {"type": "boolean"},
                            "body": {"type": ["string", "null"]},
                            "user": {"type": "object", "properties": {"login": {"type": "string"}}},
                            "labels": {"type": "array", "items": {"type": "string"}},
                            "meta": {}
                        }
                    }
                }
            }
        })
    }

    #[test]
    fn derives_sorted_typed_columns_with_json_fallback() {
        let columns =
            derive_raw_columns("github.x", Some(&issues_schema()), &path("$.issues")).unwrap();
        let summary: Vec<(&str, FieldType)> = columns
            .iter()
            .map(|c| (c.name.as_str(), c.field_type))
            .collect();
        assert_eq!(
            summary,
            vec![
                ("body", FieldType::Utf8),   // ["string","null"] union
                ("labels", FieldType::Json), // array → opaque JSON
                ("meta", FieldType::Json),   // undeclared type → opaque JSON
                ("number", FieldType::Int64),
                ("open", FieldType::Boolean),
                ("score", FieldType::Float64),
                ("title", FieldType::Utf8),
                ("user", FieldType::Json), // object → opaque JSON
            ],
            "columns are sorted by name and typed conservatively"
        );
        assert!(
            columns.iter().all(|c| c.nullable),
            "raw columns are nullable"
        );
        assert!(
            columns.iter().all(|c| c.name == c.path),
            "raw columns read top-level row keys"
        );
    }

    #[test]
    fn nested_row_paths_descend_through_properties() {
        let schema = json!({
            "type": "object",
            "properties": {
                "data": {
                    "type": "object",
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": {"type": "object", "properties": {"id": {"type": "integer"}}}
                        }
                    }
                }
            }
        });
        let columns = derive_raw_columns("a.b", Some(&schema), &path("$.data.items")).unwrap();
        assert_eq!(columns.len(), 1);
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[0].field_type, FieldType::Int64);
    }

    #[test]
    fn missing_output_schema_is_indeterminate() {
        let err = derive_raw_columns("a.b", None, &path("$.items")).unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::RawRowTypeIndeterminate { ref reason, .. }
                if reason.contains("no output schema")
        ));
    }

    #[test]
    fn unknown_segment_is_indeterminate() {
        let err = derive_raw_columns("a.b", Some(&issues_schema()), &path("$.rows")).unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::RawRowTypeIndeterminate { ref reason, .. }
                if reason.contains("segment 'rows'")
        ));
    }

    #[test]
    fn non_array_target_is_indeterminate() {
        let schema = json!({
            "type": "object",
            "properties": {"total": {"type": "integer"}}
        });
        let err = derive_raw_columns("a.b", Some(&schema), &path("$.total")).unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::RawRowTypeIndeterminate { ref reason, .. }
                if reason.contains("not an array")
        ));
    }

    #[test]
    fn array_of_non_objects_is_indeterminate() {
        let schema = json!({
            "type": "object",
            "properties": {"ids": {"type": "array", "items": {"type": "integer"}}}
        });
        let err = derive_raw_columns("a.b", Some(&schema), &path("$.ids")).unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::RawRowTypeIndeterminate { ref reason, .. }
                if reason.contains("not objects")
        ));
    }

    #[test]
    fn propertyless_row_objects_are_indeterminate() {
        // An object with no declared properties has no deterministic
        // relational shape — the error must point at the source-pack road.
        let schema = json!({
            "type": "object",
            "properties": {"items": {"type": "array", "items": {"type": "object"}}}
        });
        let err = derive_raw_columns("a.b", Some(&schema), &path("$.items")).unwrap_err();
        assert!(err.to_string().contains("source-pack"), "got: {err}");
    }

    #[test]
    fn dotted_property_names_are_rejected() {
        // "a.b" as a column name would be parsed as a nested path by the
        // converter and silently read the wrong key.
        let schema = json!({
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {"type": "object", "properties": {"a.b": {"type": "string"}}}
                }
            }
        });
        let err = derive_raw_columns("a.b", Some(&schema), &path("$.items")).unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::RawRowTypeIndeterminate { ref reason, .. }
                if reason.contains("'a.b'")
        ));
    }

    #[test]
    fn wide_unions_fall_back_to_json() {
        let schema = json!({
            "type": "object",
            "properties": {
                "items": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {"value": {"type": ["string", "integer"]}}
                    }
                }
            }
        });
        let columns = derive_raw_columns("a.b", Some(&schema), &path("$.items")).unwrap();
        assert_eq!(columns[0].field_type, FieldType::Json);
    }
}
