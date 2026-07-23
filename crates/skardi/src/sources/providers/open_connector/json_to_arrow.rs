//! JSON → Arrow conversion against a table's fixed schema.
//!
//! Conversion never infers from data: every field comes from a source-pack
//! [`FieldMapping`], so upstream additive changes are ignored and upstream
//! *breaking* changes fail conversion with the column, page, row, expected
//! type, and the found JSON *kind* (never the value, which may be
//! sensitive).

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, ListBuilder, RecordBatch, StringArray,
    StringBuilder, TimestampMillisecondArray, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use chrono::DateTime;
use serde_json::Value;

use super::error::OpenConnectorError;
use super::row_path::RowPath;

/// Column type a source-pack field can declare. Deliberately small: it is
/// the contract Skardi maintains, not every Arrow type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    /// JSON boolean ↔ Arrow Boolean.
    Boolean,
    /// JSON integer ↔ Arrow Int64.
    Int64,
    /// JSON non-negative integer ↔ Arrow UInt64.
    UInt64,
    /// JSON number ↔ Arrow Float64.
    Float64,
    /// JSON string ↔ Arrow Utf8.
    Utf8,
    /// RFC 3339 string or epoch-millis number ↔ Arrow Timestamp(Millisecond, UTC).
    TimestampMillisUtc,
    /// JSON array of strings ↔ Arrow List\<Utf8\>.
    Utf8List,
    /// JSON array of objects, each contributing the string under the given
    /// key ↔ Arrow List\<Utf8\> — the design's `$.labels[*].name` /
    /// `$.assignees[*].login` flattening for GitHub-style shapes.
    Utf8ListFromObjectKey(&'static str),
    /// Any JSON value serialized to a JSON string ↔ Arrow Utf8. For
    /// intentionally opaque fields (arbitrary maps, unstable unions). A
    /// present JSON null is SQL NULL (per the shared null rules), not the
    /// string `"null"`.
    Json,
}

impl FieldType {
    /// The Arrow data type this field type maps to.
    pub fn arrow_type(&self) -> DataType {
        match self {
            Self::Boolean => DataType::Boolean,
            Self::Int64 => DataType::Int64,
            Self::UInt64 => DataType::UInt64,
            Self::Float64 => DataType::Float64,
            Self::Utf8 | Self::Json => DataType::Utf8,
            Self::TimestampMillisUtc => {
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
            }
            Self::Utf8List | Self::Utf8ListFromObjectKey(_) => {
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
            }
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Int64 => "integer",
            Self::UInt64 => "non-negative integer",
            Self::Float64 => "number",
            Self::Utf8 => "string",
            Self::TimestampMillisUtc => "RFC 3339 timestamp or epoch millis",
            Self::Utf8List => "array of strings",
            Self::Utf8ListFromObjectKey(_) => "array of objects each carrying a string key",
            Self::Json => "any JSON value",
        }
    }
}

/// One source-pack field: where in the row JSON it lives, and its type.
#[derive(Debug, Clone, Copy)]
pub struct FieldMapping {
    /// Arrow column name.
    pub name: &'static str,
    /// Row-relative dotted path, e.g. `user.login`. Must point at an object
    /// key; array indexing is out of scope for relational mappings.
    pub path: &'static str,
    /// Column type.
    pub field_type: FieldType,
    /// Whether missing keys / JSON nulls become Arrow nulls (true) or fail
    /// conversion (false).
    pub nullable: bool,
}

/// An owned [`FieldMapping`]. Source packs declare columns statically; raw
/// scans (`open_connector_scan`) derive them at planning time from discovered
/// action metadata, so their names cannot be `&'static str`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    /// Arrow column name.
    pub name: String,
    /// Row-relative dotted path (see [`FieldMapping::path`]).
    pub path: String,
    /// Column type.
    pub field_type: FieldType,
    /// Whether missing keys / JSON nulls become Arrow nulls (true) or fail
    /// conversion (false).
    pub nullable: bool,
}

impl From<&FieldMapping> for ColumnSpec {
    fn from(mapping: &FieldMapping) -> Self {
        Self {
            name: mapping.name.to_string(),
            path: mapping.path.to_string(),
            field_type: mapping.field_type,
            nullable: mapping.nullable,
        }
    }
}

/// A column spec with its path pre-parsed.
#[derive(Debug)]
struct CompiledField {
    spec: ColumnSpec,
    path: RowPath,
}

/// Converts rows against one fixed schema. Build once per table; the Arrow
/// schema it produces never changes for the converter's lifetime.
#[derive(Debug)]
pub struct RowConverter {
    fields: Vec<CompiledField>,
    schema: SchemaRef,
}

impl RowConverter {
    /// Compile a converter from static field mappings.
    ///
    /// # Errors
    /// Returns [`OpenConnectorError::InvalidRowPath`] if any mapping path is
    /// not a dotted object-key path.
    pub fn new(mappings: &[FieldMapping]) -> Result<Self, OpenConnectorError> {
        Self::from_columns(mappings.iter().map(ColumnSpec::from).collect())
    }

    /// Compile a converter from owned column specs (the raw-scan path).
    pub fn from_columns(columns: Vec<ColumnSpec>) -> Result<Self, OpenConnectorError> {
        let mut fields = Vec::with_capacity(columns.len());
        let mut arrow_fields = Vec::with_capacity(columns.len());
        for spec in columns {
            // Column paths are row-relative (`user.login`); the `$.` marker
            // belongs to whole-document row paths only. Reject it rather
            // than strip: `$.{path}` would otherwise parse into a literal
            // `"$"` first segment and silently read the wrong key —
            // all-NULL columns instead of a loud error.
            if spec.path.starts_with("$.") {
                return Err(OpenConnectorError::InvalidRowPath {
                    path: spec.path.clone(),
                    reason: format!(
                        "column paths are row-relative object keys \
                         (write '{}', not '{}')",
                        &spec.path[2..],
                        spec.path
                    ),
                });
            }
            let path = RowPath::parse(&format!("$.{}", spec.path))?;
            arrow_fields.push(Field::new(
                &spec.name,
                spec.field_type.arrow_type(),
                spec.nullable,
            ));
            fields.push(CompiledField { spec, path });
        }
        Ok(Self {
            fields,
            schema: Arc::new(Schema::new(arrow_fields)),
        })
    }

    /// The fixed Arrow schema.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Convert one page of row objects to a RecordBatch. `page` is 1-based
    /// and only used for error context. An empty page yields an empty batch
    /// with the fixed schema.
    pub fn convert(&self, rows: &[Value], page: usize) -> Result<RecordBatch, OpenConnectorError> {
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            columns.push(self.convert_column(field, rows, page)?);
        }
        RecordBatch::try_new(Arc::clone(&self.schema), columns).map_err(|e| {
            OpenConnectorError::ConversionFailed {
                path: "$".to_string(),
                column: "<batch>".to_string(),
                page,
                row: 0,
                expected: "a batch matching the fixed schema".to_string(),
                found: e.to_string(),
            }
        })
    }

    fn convert_column(
        &self,
        field: &CompiledField,
        rows: &[Value],
        page: usize,
    ) -> Result<ArrayRef, OpenConnectorError> {
        let spec = &field.spec;
        let mut cells: Vec<Option<&Value>> = Vec::with_capacity(rows.len());
        for (row_index, row) in rows.iter().enumerate() {
            match field.path.extract(row, page) {
                Ok(value) => cells.push(Some(value)),
                // Absence may become null for a nullable column: either the
                // key is genuinely missing, or a parent on the path is JSON
                // null — providers null out whole objects routinely (GitHub's
                // `commit.author: null`, `issue.user: null`), and a null
                // parent means every leaf under it is absent. A
                // present-but-wrong-shape parent (e.g. `user` changed from
                // object to string) is an upstream *breaking* change and
                // must fail, per this module's contract.
                Err(OpenConnectorError::RowPathNotFound { .. }) if spec.nullable => {
                    cells.push(None)
                }
                Err(OpenConnectorError::RowPathNotFound { .. }) => {
                    return Err(self.failure(field, page, row_index, "missing key"));
                }
                Err(OpenConnectorError::RowPathNotObject { ref found, .. })
                    if found == "null" && spec.nullable =>
                {
                    cells.push(None)
                }
                Err(OpenConnectorError::RowPathNotObject { ref found, .. }) => {
                    return Err(self.failure(field, page, row_index, found));
                }
                Err(e) => return Err(e),
            }
        }

        let fail = |index: usize, found: &str| self.failure(field, page, index, found);
        match spec.field_type {
            FieldType::Boolean => Ok(Arc::new(BooleanArray::from(collect_cells(
                &cells,
                spec,
                |v| v.as_bool(),
                fail,
            )?))),
            FieldType::Int64 => Ok(Arc::new(Int64Array::from(collect_cells(
                &cells,
                spec,
                |v| v.as_i64(),
                fail,
            )?))),
            FieldType::UInt64 => Ok(Arc::new(UInt64Array::from(collect_cells(
                &cells,
                spec,
                |v| v.as_u64(),
                fail,
            )?))),
            FieldType::Float64 => Ok(Arc::new(Float64Array::from(collect_cells(
                &cells,
                spec,
                |v| v.as_f64(),
                fail,
            )?))),
            FieldType::Utf8 => Ok(Arc::new(StringArray::from(collect_cells(
                &cells,
                spec,
                |v| v.as_str().map(str::to_string),
                fail,
            )?))),
            // Opaque values serialize to canonical JSON text, but a present
            // JSON null is SQL NULL like everywhere else — never the 4-char
            // string "null", which would break `IS NULL` and match
            // `= 'null'`. Routing through collect_cells also makes a null in
            // a non-nullable Json column a targeted per-column failure.
            FieldType::Json => Ok(Arc::new(StringArray::from(collect_cells(
                &cells,
                spec,
                |v| Some(v.to_string()),
                fail,
            )?))),
            FieldType::TimestampMillisUtc => Ok(Arc::new(
                TimestampMillisecondArray::from(collect_cells(
                    &cells,
                    spec,
                    parse_timestamp,
                    fail,
                )?)
                .with_timezone("UTC"),
            )),
            FieldType::Utf8List => self.convert_string_list(field, &cells, page, None),
            FieldType::Utf8ListFromObjectKey(key) => {
                self.convert_string_list(field, &cells, page, Some(key))
            }
        }
    }

    /// Convert a list column. With `pluck: None` elements must be strings;
    /// with `pluck: Some(key)` elements must be objects carrying a string
    /// under `key` (the flattened value).
    fn convert_string_list(
        &self,
        field: &CompiledField,
        cells: &[Option<&Value>],
        page: usize,
        pluck: Option<&'static str>,
    ) -> Result<ArrayRef, OpenConnectorError> {
        let mut builder = ListBuilder::new(StringBuilder::new());
        for (row_index, cell) in cells.iter().enumerate() {
            let Some(value) = cell else {
                builder.append(false);
                continue;
            };
            if value.is_null() {
                if field.spec.nullable {
                    builder.append(false);
                    continue;
                }
                return Err(self.failure(field, page, row_index, "null"));
            }
            let items = value
                .as_array()
                .ok_or_else(|| self.failure(field, page, row_index, json_kind(value)))?;
            for item in items {
                // A JSON null wherever an item's text would come from is an
                // Arrow null item — the list's item field is declared
                // nullable, and a provider null is data, not shape drift. A
                // *missing* pluck key stays a failure: unlike columns, items
                // carry no pack-declared nullability that authorizes reading
                // absence as null, so a dropped upstream key fails loudly
                // instead of silently yielding all-null lists.
                let text = match pluck {
                    None => match item {
                        Value::Null => None,
                        Value::String(text) => Some(text.as_str()),
                        other => {
                            return Err(self.failure(
                                field,
                                page,
                                row_index,
                                &format!("{} array element", json_kind(other)),
                            ));
                        }
                    },
                    Some(key) => match item {
                        Value::Null => None,
                        Value::Object(object) => match object.get(key) {
                            Some(Value::Null) => None,
                            Some(Value::String(text)) => Some(text.as_str()),
                            Some(other) => {
                                return Err(self.failure(
                                    field,
                                    page,
                                    row_index,
                                    &format!("array element whose '{key}' is {}", json_kind(other)),
                                ));
                            }
                            None => {
                                return Err(self.failure(
                                    field,
                                    page,
                                    row_index,
                                    &format!("array element without key '{key}'"),
                                ));
                            }
                        },
                        other => {
                            return Err(self.failure(
                                field,
                                page,
                                row_index,
                                &format!("{} array element", json_kind(other)),
                            ));
                        }
                    },
                };
                match text {
                    Some(text) => builder.values().append_value(text),
                    None => builder.values().append_null(),
                }
            }
            builder.append(true);
        }
        Ok(Arc::new(builder.finish()))
    }

    fn failure(
        &self,
        field: &CompiledField,
        page: usize,
        row: usize,
        found: &str,
    ) -> OpenConnectorError {
        OpenConnectorError::ConversionFailed {
            path: field.path.as_str().to_string(),
            column: field.spec.name.clone(),
            page,
            row,
            expected: field.spec.field_type.label().to_string(),
            found: found.to_string(),
        }
    }
}

/// Project `cells` through `convert`, mapping JSON null to Arrow null for
/// nullable fields and failing otherwise.
fn collect_cells<T, F, E>(
    cells: &[Option<&Value>],
    spec: &ColumnSpec,
    convert: F,
    fail: E,
) -> Result<Vec<Option<T>>, OpenConnectorError>
where
    F: Fn(&Value) -> Option<T>,
    E: Fn(usize, &str) -> OpenConnectorError,
{
    let mut out = Vec::with_capacity(cells.len());
    for (index, cell) in cells.iter().enumerate() {
        match cell {
            None => out.push(None),
            Some(value) if value.is_null() => {
                if spec.nullable {
                    out.push(None);
                } else {
                    return Err(fail(index, "null"));
                }
            }
            Some(value) => match convert(value) {
                Some(converted) => out.push(Some(converted)),
                None => return Err(fail(index, json_kind(value))),
            },
        }
    }
    Ok(out)
}

/// RFC 3339 string or epoch-millis number → epoch millis.
fn parse_timestamp(value: &Value) -> Option<i64> {
    if let Some(text) = value.as_str() {
        let parsed = DateTime::parse_from_rfc3339(text).ok()?;
        return Some(parsed.timestamp_millis());
    }
    value.as_i64()
}
/// Short human-readable kind of a JSON value, for error messages.
fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray};
    use serde_json::json;

    const FIELDS: &[FieldMapping] = &[
        FieldMapping {
            name: "id",
            path: "id",
            field_type: FieldType::UInt64,
            nullable: false,
        },
        FieldMapping {
            name: "title",
            path: "title",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "author_login",
            path: "user.login",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "score",
            path: "score",
            field_type: FieldType::Float64,
            nullable: true,
        },
        FieldMapping {
            name: "labels",
            path: "labels",
            field_type: FieldType::Utf8List,
            nullable: true,
        },
        FieldMapping {
            name: "created_at",
            path: "created_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "raw",
            path: "raw",
            field_type: FieldType::Json,
            nullable: true,
        },
    ];

    fn converter() -> RowConverter {
        RowConverter::new(FIELDS).unwrap()
    }

    fn row(id: u64, title: &str) -> Value {
        json!({
            "id": id,
            "title": title,
            "user": {"login": "octocat"},
            "score": 9.5,
            "labels": ["bug", "p1"],
            "created_at": "2026-01-01T00:00:00Z",
            "raw": {"nested": [1, 2]},
            "extra_upstream_field": "ignored"
        })
    }

    #[test]
    fn converts_full_rows_and_ignores_extra_fields() {
        let batch = converter().convert(&[row(1, "a"), row(2, "b")], 1).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 7);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(ids.value(1), 2);
        let authors = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(authors.value(0), "octocat");
    }

    #[test]
    fn missing_required_key_fails_with_context() {
        let mut value = row(1, "a");
        value.as_object_mut().unwrap().remove("title");
        let err = converter().convert(&[value], 2).unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::ConversionFailed { ref column, page: 2, row: 0, ref found, .. }
                if column == "title" && found == "missing key"
        ));
    }

    #[test]
    fn missing_nullable_key_becomes_null() {
        let mut value = row(1, "a");
        value.as_object_mut().unwrap().remove("score");
        let batch = converter().convert(&[value], 1).unwrap();
        let scores = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert!(scores.is_null(0));
    }

    #[test]
    fn nullable_column_fails_on_shape_mismatch() {
        // `user` changed from object to string upstream: a nullable
        // user.login column must NOT quietly become all-null — this is a
        // breaking change and has to fail conversion.
        let mut value = row(1, "a");
        value["user"] = json!("octocat");
        let err = converter().convert(&[value], 1).unwrap_err();
        match err {
            OpenConnectorError::ConversionFailed { column, found, .. } => {
                assert_eq!(column, "author_login");
                assert_eq!(found, "a string");
            }
            other => panic!("expected ConversionFailed, got {other}"),
        }
    }

    #[test]
    fn null_parent_object_nulls_for_nullable_column() {
        // Providers null out whole objects (GitHub `commit.author: null`,
        // `issue.user: null`): a JSON-null parent means every leaf under it
        // is absent — NULL for a nullable column, not a structural failure.
        let mut value = row(1, "a");
        value["user"] = Value::Null;
        let batch = converter().convert(&[value], 1).unwrap();
        let authors = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(authors.is_null(0));
    }

    #[test]
    fn null_parent_object_fails_for_required_column() {
        let converter = RowConverter::new(&[FieldMapping {
            name: "author_login",
            path: "user.login",
            field_type: FieldType::Utf8,
            nullable: false,
        }])
        .unwrap();
        let err = converter
            .convert(&[serde_json::json!({"user": null})], 1)
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::ConversionFailed { ref column, ref found, .. }
                if column == "author_login" && found == "null"
        ));
    }

    #[test]
    fn object_list_plucks_the_declared_string_key() {
        // The design's `$.labels[*].name` flattening: an array of objects
        // becomes List<Utf8> of one declared key.
        let converter = RowConverter::new(&[FieldMapping {
            name: "labels",
            path: "labels",
            field_type: FieldType::Utf8ListFromObjectKey("name"),
            nullable: true,
        }])
        .unwrap();
        let batch = converter
            .convert(
                &[
                    serde_json::json!({"labels": [{"name": "bug", "color": "red"}, {"name": "p1"}]}),
                    serde_json::json!({"labels": []}),
                    serde_json::json!({"labels": null}),
                    serde_json::json!({}),
                ],
                1,
            )
            .unwrap();
        let lists = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        let first = lists.value(0);
        let first = first.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(first.value(0), "bug");
        assert_eq!(first.value(1), "p1");
        assert_eq!(lists.value(1).len(), 0, "empty array stays an empty list");
        assert!(lists.is_null(2), "JSON null becomes NULL");
        assert!(lists.is_null(3), "absent key becomes NULL");
    }

    #[test]
    fn object_list_shape_mismatches_fail_with_the_specific_kind() {
        let converter = RowConverter::new(&[FieldMapping {
            name: "labels",
            path: "labels",
            field_type: FieldType::Utf8ListFromObjectKey("name"),
            nullable: true,
        }])
        .unwrap();
        for (bad, found) in [
            // A dropped upstream key is shape drift and must fail loudly —
            // it must never silently become an all-null list.
            (
                serde_json::json!({"labels": [{"color": "red"}]}),
                "array element without key 'name'",
            ),
            (
                serde_json::json!({"labels": [{"name": 42}]}),
                "array element whose 'name' is number",
            ),
            (
                serde_json::json!({"labels": ["bug"]}),
                "string array element",
            ),
        ] {
            let err = converter.convert(&[bad], 1).unwrap_err();
            assert!(
                matches!(
                    err,
                    OpenConnectorError::ConversionFailed { found: ref f, .. } if f == found
                ),
                "expected found={found}, got {err}"
            );
        }
    }

    #[test]
    fn json_null_list_elements_become_null_items() {
        // The item field is declared nullable (List<Utf8> with nullable
        // items), so an explicit provider null — a null element, or a pluck
        // key holding null — must convert to an Arrow null item, not fail.
        let converter = RowConverter::new(&[
            FieldMapping {
                name: "tags",
                path: "tags",
                field_type: FieldType::Utf8List,
                nullable: true,
            },
            FieldMapping {
                name: "labels",
                path: "labels",
                field_type: FieldType::Utf8ListFromObjectKey("name"),
                nullable: true,
            },
        ])
        .unwrap();
        let batch = converter
            .convert(
                &[serde_json::json!({
                    "tags": ["a", null, "b"],
                    "labels": [{"name": null}, {"name": "bug"}, null]
                })],
                1,
            )
            .unwrap();

        let tags = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap()
            .value(0);
        let tags = tags.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(tags.value(0), "a");
        assert!(tags.is_null(1), "null element is a null item");
        assert_eq!(tags.value(2), "b");

        let labels = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap()
            .value(0);
        let labels = labels.as_any().downcast_ref::<StringArray>().unwrap();
        assert!(labels.is_null(0), "pluck key holding null is a null item");
        assert_eq!(labels.value(1), "bug");
        assert!(labels.is_null(2), "null element is a null item");
    }

    #[test]
    fn missing_parent_object_still_nulls_for_nullable_column() {
        // A genuinely-absent parent (no `user` key at all) is absence, not a
        // shape change — nullable user.login becomes null.
        let mut value = row(1, "a");
        value.as_object_mut().unwrap().remove("user");
        let batch = converter().convert(&[value], 1).unwrap();
        let authors = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(authors.is_null(0));
    }

    #[test]
    fn null_in_required_field_fails() {
        let mut value = row(1, "a");
        value["title"] = Value::Null;
        let err = converter().convert(&[value], 1).unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::ConversionFailed { ref column, ref found, .. }
                if column == "title" && found == "null"
        ));
    }

    #[test]
    fn wrong_type_fails_with_kind_not_value() {
        let mut value = row(1, "a");
        value["id"] = json!("not-a-number");
        let err = converter().convert(&[value], 1).unwrap_err();
        match err {
            OpenConnectorError::ConversionFailed { found, column, .. } => {
                assert_eq!(column, "id");
                assert_eq!(found, "string");
                assert!(!found.contains("not-a-number"), "error must not echo data");
            }
            other => panic!("expected ConversionFailed, got {other}"),
        }
    }

    #[test]
    fn float_for_int_fails() {
        let mut value = row(1, "a");
        value["id"] = json!(1.5);
        let err = converter().convert(&[value], 1).unwrap_err();
        assert!(matches!(err, OpenConnectorError::ConversionFailed { .. }));
    }

    #[test]
    fn timestamps_parse_rfc3339_and_epoch() {
        let mut value = row(1, "a");
        let batch = converter().convert(&[value.clone()], 1).unwrap();
        let ts = batch
            .column(5)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1767225600000);

        value["created_at"] = json!(1767225600000i64);
        let batch = converter().convert(&[value], 1).unwrap();
        let ts = batch
            .column(5)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(ts.value(0), 1767225600000);
    }

    #[test]
    fn non_string_list_element_fails() {
        let mut value = row(1, "a");
        value["labels"] = json!(["bug", 42]);
        let err = converter().convert(&[value], 1).unwrap_err();
        assert!(matches!(err, OpenConnectorError::ConversionFailed { .. }));
    }

    #[test]
    fn opaque_field_serializes_json() {
        let batch = converter().convert(&[row(1, "a")], 1).unwrap();
        let raw = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(raw.value(0).contains("nested"));
    }

    #[test]
    fn json_null_becomes_arrow_null_not_the_string_null() {
        // A present JSON null in a nullable Json column is SQL NULL — never
        // the 4-char string "null", which would make `IS NULL` miss and
        // `= 'null'` match. Raw scans type every object/array field as Json,
        // so nullable nested SaaS fields (assignee: null) hit this arm.
        let mut with_null = row(1, "a");
        with_null["raw"] = Value::Null;
        let mut absent = row(2, "b");
        absent.as_object_mut().unwrap().remove("raw");

        let batch = converter().convert(&[with_null, absent], 1).unwrap();
        let raw = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(raw.is_null(0), "present JSON null must be Arrow null");
        assert!(raw.is_null(1), "absent key stays Arrow null");
    }

    #[test]
    fn json_null_fails_for_non_nullable_json_column() {
        // Same null discipline as every other type: a required opaque field
        // fails with a targeted per-column error, not a batch-level one.
        let converter = RowConverter::new(&[FieldMapping {
            name: "raw",
            path: "raw",
            field_type: FieldType::Json,
            nullable: false,
        }])
        .unwrap();
        let err = converter
            .convert(&[serde_json::json!({"raw": null})], 3)
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::ConversionFailed { ref column, page: 3, row: 0, ref found, .. }
                if column == "raw" && found == "null"
        ));
    }

    #[test]
    fn empty_page_yields_empty_batch_with_schema() {
        let converter = converter();
        let batch = converter.convert(&[], 1).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().fields().len(), 7);
    }

    #[test]
    fn owned_columns_build_the_same_converter_as_static_mappings() {
        // The raw-scan path derives ColumnSpecs at planning time; they must
        // produce exactly the schema and conversion the static path does.
        let owned = RowConverter::from_columns(FIELDS.iter().map(ColumnSpec::from).collect())
            .expect("owned converter");
        assert_eq!(owned.schema(), converter().schema());

        let batch = owned.convert(&[row(7, "seven")], 1).unwrap();
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 7);
    }

    #[test]
    fn dollar_prefixed_column_paths_are_rejected_not_silently_misread() {
        // `$.user.login` would survive the `$.{path}` concatenation as a
        // literal `"$"` first segment and read the wrong key — nullable
        // columns would silently go all-NULL. Reject it with the corrected
        // spelling instead.
        let err = RowConverter::new(&[FieldMapping {
            name: "author",
            path: "$.user.login",
            field_type: FieldType::Utf8,
            nullable: true,
        }])
        .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::InvalidRowPath { ref path, ref reason }
                if path == "$.user.login"
                    && reason.contains("write 'user.login', not '$.user.login'")
        ));
    }

    #[test]
    fn invalid_mapping_path_is_rejected() {
        let err = RowConverter::new(&[FieldMapping {
            name: "x",
            path: "a..b",
            field_type: FieldType::Utf8,
            nullable: false,
        }])
        .unwrap_err();
        assert!(matches!(err, OpenConnectorError::InvalidRowPath { .. }));
    }
}
