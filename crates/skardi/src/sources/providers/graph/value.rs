//! Graph values → Arrow, type-directed by the caller-declared columns.
//!
//! Two halves, both backend-shared:
//!
//! - **agtype text → JSON.** AGE serializes results as agtype — JSON with
//!   `::vertex` / `::edge` / `::path` (and `::numeric`) annotations
//!   appended after the value they classify. [`parse_agtype`] strips the
//!   annotations *outside string literals* and parses the remainder as
//!   JSON. The annotations are not needed afterwards: conversion is
//!   type-directed by the DECLARED column type (design §Schema handling —
//!   there is no schema inference anywhere), so a `node` column expects
//!   the vertex object shape and fails with a typed error otherwise.
//! - **JSON → Arrow.** [`build_batch`] converts one buffered batch of rows
//!   against the declared columns. Every declared column is nullable
//!   (Cypher can produce null in any position); a non-null value of the
//!   wrong JSON kind is [`GraphError::TypeMismatch`] carrying kinds, never
//!   values.
//!
//! The canonical STRUCT shapes (design §Result flattening):
//! - node: `STRUCT<id Utf8, labels List<Utf8>, properties Utf8>` —
//!   properties are JSON text so the schema is independent of which keys
//!   any row happens to carry.
//! - relationship: `STRUCT<id Utf8, start_id Utf8, end_id Utf8, type Utf8,
//!   properties Utf8>`.
//! - path: `STRUCT<nodes List<node>, relationships List<relationship>>` —
//!   two parallel typed lists (an Arrow list has ONE element type and a
//!   path alternates two shapes); relationship *i* connects node *i* to
//!   node *i+1*.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, ListArray, ListBuilder, StringBuilder,
    StructArray,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::Value;

use super::error::{GraphError, json_kind};

/// The declared-type vocabulary — the repo's friendly lowercase names
/// (dynamodb/mongo/seekdb precedent), never Arrow PascalCase. One
/// spelling, shared verbatim by the future YAML views' `type:` fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphType {
    /// `string` (aliases `str`, `utf8`) → Utf8.
    String,
    /// `int` (aliases `integer`, `bigint`) → Int64.
    Int,
    /// `float` (alias `double`) → Float64.
    Float,
    /// `bool` (alias `boolean`) → Boolean.
    Bool,
    /// `json` → Utf8 carrying the value as JSON text verbatim.
    Json,
    /// `node` → the canonical vertex STRUCT.
    Node,
    /// `relationship` → the canonical edge STRUCT.
    Relationship,
    /// `path` → parallel node/relationship lists.
    Path,
}

/// The accepted set, for error messages — kept in one place so the
/// diagnostic can never drift from the parser.
pub const ACCEPTED_TYPES: &str = "string|str|utf8, int|integer|bigint, float|double, bool|boolean, json, \
     node, relationship, path";

impl GraphType {
    /// Parse one friendly type name.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "string" | "str" | "utf8" => Some(Self::String),
            "int" | "integer" | "bigint" => Some(Self::Int),
            "float" | "double" => Some(Self::Float),
            "bool" | "boolean" => Some(Self::Bool),
            "json" => Some(Self::Json),
            "node" => Some(Self::Node),
            "relationship" => Some(Self::Relationship),
            "path" => Some(Self::Path),
            _ => None,
        }
    }

    /// The canonical (post-alias) name, for diagnostics.
    pub fn canonical(&self) -> &'static str {
        match self {
            Self::String => "string",
            Self::Int => "int",
            Self::Float => "float",
            Self::Bool => "bool",
            Self::Json => "json",
            Self::Node => "node",
            Self::Relationship => "relationship",
            Self::Path => "path",
        }
    }

    /// The Arrow type this declared type plans as.
    pub fn arrow_type(&self) -> DataType {
        match self {
            Self::String | Self::Json => DataType::Utf8,
            Self::Int => DataType::Int64,
            Self::Float => DataType::Float64,
            Self::Bool => DataType::Boolean,
            Self::Node => DataType::Struct(node_fields()),
            Self::Relationship => DataType::Struct(relationship_fields()),
            Self::Path => DataType::Struct(path_fields()),
        }
    }
}

/// One declared output column.
#[derive(Debug, Clone)]
pub struct DeclaredColumn {
    pub name: String,
    pub ty: GraphType,
}

/// The vertex STRUCT's fields — one definition feeds the planned type AND
/// the builder so they cannot drift.
pub fn node_fields() -> Fields {
    Fields::from(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new(
            "labels",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new("properties", DataType::Utf8, true),
    ])
}

/// The edge STRUCT's fields.
pub fn relationship_fields() -> Fields {
    Fields::from(vec![
        Field::new("id", DataType::Utf8, true),
        Field::new("start_id", DataType::Utf8, true),
        Field::new("end_id", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, true),
        Field::new("properties", DataType::Utf8, true),
    ])
}

/// The path STRUCT's fields: parallel typed lists.
pub fn path_fields() -> Fields {
    Fields::from(vec![
        Field::new(
            "nodes",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(node_fields()),
                true,
            ))),
            true,
        ),
        Field::new(
            "relationships",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::Struct(relationship_fields()),
                true,
            ))),
            true,
        ),
    ])
}

/// The planned schema for a set of declared columns. Every field is
/// nullable — Cypher can produce null in any position, and there is no
/// way to declare otherwise on the ad-hoc surface (design §Schema
/// handling).
pub fn declared_schema(columns: &[DeclaredColumn]) -> Arc<Schema> {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(&c.name, c.ty.arrow_type(), true))
            .collect::<Vec<_>>(),
    ))
}

/// Strip agtype's `::identifier` annotations outside string literals and
/// parse the remainder as JSON.
///
/// The scanner tracks JSON string state (including escapes), so a literal
/// like `"a::vertex"` survives untouched; only structural annotations —
/// which always follow a value (`}`, `]`, digit, or scalar keyword) — are
/// removed.
pub fn parse_agtype(text: &str) -> Result<Value, String> {
    // BYTES in, BYTES out, one UTF-8 decode at the end. `u8 as char` is
    // Latin-1 semantics and would shred every multi-byte UTF-8 character
    // into mojibake that still parses as JSON — a SILENT corruption
    // (CJK entity names are the common case in knowledge graphs, not an
    // edge case). Byte-level scanning is sound because everything the
    // scanner matches ('"', '\\', ':', the annotation identifiers) is
    // ASCII, and UTF-8 continuation bytes are all >= 0x80 — a multi-byte
    // character can never alias a structural byte.
    let mut cleaned: Vec<u8> = Vec::with_capacity(text.len());
    let bytes = text.as_bytes();
    let mut i = 0;
    let mut in_string = false;
    let mut escaped = false;
    while i < bytes.len() {
        let b = bytes[i];
        if in_string {
            cleaned.push(b);
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                in_string = false;
            }
            i += 1;
            continue;
        }
        match b {
            b'"' => {
                in_string = true;
                cleaned.push(b'"');
                i += 1;
            }
            b':' if i + 1 < bytes.len() && bytes[i + 1] == b':' => {
                // `::identifier` — skip the annotation.
                i += 2;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    i += 1;
                }
            }
            _ => {
                cleaned.push(b);
                i += 1;
            }
        }
    }
    let cleaned = String::from_utf8(cleaned)
        .map_err(|e| format!("agtype text is not valid UTF-8 after annotation strip: {e}"))?;
    serde_json::from_str(&cleaned).map_err(|e| e.to_string())
}

/// Convert one buffered batch of rows (each row = one JSON value per
/// declared column) into a RecordBatch. Batch-atomic: any type mismatch
/// fails the WHOLE batch before anything is emitted (design §Schema
/// handling). `row_base` is the absolute index of the batch's first row,
/// so errors name the result row, not the batch-relative one.
pub fn build_batch(
    columns: &[DeclaredColumn],
    rows: &[Vec<Value>],
    row_base: usize,
) -> Result<RecordBatch, GraphError> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (col_idx, col) in columns.iter().enumerate() {
        let cells = rows.iter().map(|r| &r[col_idx]);
        let array: ArrayRef = match col.ty {
            GraphType::String => {
                let mut b = StringBuilder::new();
                for (i, v) in cells.enumerate() {
                    match v {
                        Value::Null => b.append_null(),
                        Value::String(s) => b.append_value(s),
                        other => return Err(mismatch(col, row_base + i, "string", other)),
                    }
                }
                Arc::new(b.finish())
            }
            GraphType::Int => {
                let mut b = Int64Builder::new();
                for (i, v) in cells.enumerate() {
                    match v {
                        Value::Null => b.append_null(),
                        Value::Number(n) if n.is_i64() => b.append_value(n.as_i64().unwrap()),
                        other => return Err(mismatch(col, row_base + i, "int", other)),
                    }
                }
                Arc::new(b.finish())
            }
            GraphType::Float => {
                let mut b = Float64Builder::new();
                for (i, v) in cells.enumerate() {
                    match v {
                        Value::Null => b.append_null(),
                        // Integers widen losslessly enough for a declared
                        // float; the reverse (float → int) never does.
                        Value::Number(n) => match n.as_f64() {
                            Some(f) => b.append_value(f),
                            None => return Err(mismatch(col, row_base + i, "float", v)),
                        },
                        other => return Err(mismatch(col, row_base + i, "float", other)),
                    }
                }
                Arc::new(b.finish())
            }
            GraphType::Bool => {
                let mut b = BooleanBuilder::new();
                for (i, v) in cells.enumerate() {
                    match v {
                        Value::Null => b.append_null(),
                        Value::Bool(x) => b.append_value(*x),
                        other => return Err(mismatch(col, row_base + i, "bool", other)),
                    }
                }
                Arc::new(b.finish())
            }
            GraphType::Json => {
                // Verbatim: any JSON kind is legal, null stays SQL NULL.
                let mut b = StringBuilder::new();
                for v in cells {
                    match v {
                        Value::Null => b.append_null(),
                        other => b.append_value(other.to_string()),
                    }
                }
                Arc::new(b.finish())
            }
            GraphType::Node => {
                let mut parts = Vec::with_capacity(rows.len());
                for (i, v) in cells.enumerate() {
                    parts.push(node_parts(col, row_base + i, v)?);
                }
                Arc::new(node_struct_array(&parts))
            }
            GraphType::Relationship => {
                let mut parts = Vec::with_capacity(rows.len());
                for (i, v) in cells.enumerate() {
                    parts.push(relationship_parts(col, row_base + i, v)?);
                }
                Arc::new(relationship_struct_array(&parts))
            }
            GraphType::Path => {
                let mut parts = Vec::with_capacity(rows.len());
                for (i, v) in cells.enumerate() {
                    parts.push(path_parts(col, row_base + i, v)?);
                }
                Arc::new(path_struct_array(&parts))
            }
        };
        arrays.push(array);
    }
    RecordBatch::try_new(declared_schema(columns), arrays).map_err(|e| {
        // Unreachable by construction (builders match the schema); kept as
        // a typed error rather than a panic.
        GraphError::backend("<conversion>", "arrow", &e.to_string())
    })
}

fn mismatch(col: &DeclaredColumn, row: usize, expected: &'static str, v: &Value) -> GraphError {
    GraphError::TypeMismatch {
        column: col.name.clone(),
        row,
        expected,
        found: json_kind(v),
    }
}

/// One vertex, decomposed: (id, label, properties-json). AGE vertex
/// objects carry `id` (number), `label` (string), `properties` (object).
/// Ids are stringified: opaque tokens, stable for the entity's life
/// within one database (design §Backend abstraction).
struct NodeParts {
    id: String,
    label: String,
    properties: String,
}

/// One edge, decomposed.
struct RelParts {
    id: String,
    start_id: String,
    end_id: String,
    rel_type: String,
    properties: String,
}

fn node_parts(
    col: &DeclaredColumn,
    row: usize,
    v: &Value,
) -> Result<Option<NodeParts>, GraphError> {
    match v {
        Value::Null => Ok(None),
        Value::Object(map) => {
            let (Some(id), Some(label)) = (map.get("id"), map.get("label").and_then(Value::as_str))
            else {
                return Err(mismatch(col, row, "node", v));
            };
            Ok(Some(NodeParts {
                id: scalar_to_string(id),
                label: label.to_string(),
                properties: map
                    .get("properties")
                    .cloned()
                    .unwrap_or(Value::Null)
                    .to_string(),
            }))
        }
        other => Err(mismatch(col, row, "node", other)),
    }
}

fn relationship_parts(
    col: &DeclaredColumn,
    row: usize,
    v: &Value,
) -> Result<Option<RelParts>, GraphError> {
    match v {
        Value::Null => Ok(None),
        Value::Object(map) => {
            let (Some(id), Some(start), Some(end), Some(label)) = (
                map.get("id"),
                map.get("start_id"),
                map.get("end_id"),
                map.get("label").and_then(Value::as_str),
            ) else {
                return Err(mismatch(col, row, "relationship", v));
            };
            Ok(Some(RelParts {
                id: scalar_to_string(id),
                start_id: scalar_to_string(start),
                end_id: scalar_to_string(end),
                rel_type: label.to_string(),
                properties: map
                    .get("properties")
                    .cloned()
                    .unwrap_or(Value::Null)
                    .to_string(),
            }))
        }
        other => Err(mismatch(col, row, "relationship", other)),
    }
}

/// An AGE path is a JSON array alternating vertex/edge objects
/// (`[v, e, v, …]` — nodes at the even positions, odd total length).
/// A path, decomposed into its two parallel element sequences.
type PathParts = (Vec<NodeParts>, Vec<RelParts>);

fn path_parts(
    col: &DeclaredColumn,
    row: usize,
    v: &Value,
) -> Result<Option<PathParts>, GraphError> {
    match v {
        Value::Null => Ok(None),
        Value::Array(elements) => {
            if elements.len().is_multiple_of(2) && !elements.is_empty() {
                return Err(mismatch(col, row, "path", v));
            }
            let mut nodes = Vec::with_capacity(elements.len() / 2 + 1);
            let mut rels = Vec::with_capacity(elements.len() / 2);
            for (i, el) in elements.iter().enumerate() {
                if i.is_multiple_of(2) {
                    match node_parts(col, row, el)? {
                        Some(n) => nodes.push(n),
                        None => return Err(mismatch(col, row, "path", v)),
                    }
                } else {
                    match relationship_parts(col, row, el)? {
                        Some(r) => rels.push(r),
                        None => return Err(mismatch(col, row, "path", v)),
                    }
                }
            }
            Ok(Some((nodes, rels)))
        }
        other => Err(mismatch(col, row, "path", other)),
    }
}

/// Assemble the canonical vertex STRUCT column: explicit child arrays +
/// a validity buffer (arrow's `StructBuilder` erases List child builders,
/// so explicit assembly is both clearer and the only reliable spelling).
fn node_struct_array(items: &[Option<NodeParts>]) -> StructArray {
    let mut ids = StringBuilder::new();
    let mut labels = ListBuilder::new(StringBuilder::new());
    let mut props = StringBuilder::new();
    let mut validity = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Some(n) => {
                ids.append_value(&n.id);
                labels.values().append_value(&n.label);
                labels.append(true);
                props.append_value(&n.properties);
                validity.push(true);
            }
            None => {
                ids.append_null();
                labels.append_null();
                props.append_null();
                validity.push(false);
            }
        }
    }
    StructArray::new(
        node_fields(),
        vec![
            Arc::new(ids.finish()),
            Arc::new(labels.finish()),
            Arc::new(props.finish()),
        ],
        Some(NullBuffer::from(validity)),
    )
}

/// Assemble the canonical edge STRUCT column.
fn relationship_struct_array(items: &[Option<RelParts>]) -> StructArray {
    let mut ids = StringBuilder::new();
    let mut starts = StringBuilder::new();
    let mut ends = StringBuilder::new();
    let mut types = StringBuilder::new();
    let mut props = StringBuilder::new();
    let mut validity = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Some(r) => {
                ids.append_value(&r.id);
                starts.append_value(&r.start_id);
                ends.append_value(&r.end_id);
                types.append_value(&r.rel_type);
                props.append_value(&r.properties);
                validity.push(true);
            }
            None => {
                ids.append_null();
                starts.append_null();
                ends.append_null();
                types.append_null();
                props.append_null();
                validity.push(false);
            }
        }
    }
    StructArray::new(
        relationship_fields(),
        vec![
            Arc::new(ids.finish()),
            Arc::new(starts.finish()),
            Arc::new(ends.finish()),
            Arc::new(types.finish()),
            Arc::new(props.finish()),
        ],
        Some(NullBuffer::from(validity)),
    )
}

/// Assemble the path STRUCT column: two parallel typed lists over
/// flattened node/edge values, offsets per row.
fn path_struct_array(items: &[Option<PathParts>]) -> StructArray {
    let mut flat_nodes: Vec<Option<NodeParts>> = Vec::new();
    let mut flat_rels: Vec<Option<RelParts>> = Vec::new();
    let mut node_lengths = Vec::with_capacity(items.len());
    let mut rel_lengths = Vec::with_capacity(items.len());
    let mut validity = Vec::with_capacity(items.len());
    for item in items {
        match item {
            Some((nodes, rels)) => {
                node_lengths.push(nodes.len());
                rel_lengths.push(rels.len());
                validity.push(true);
                flat_nodes.extend(nodes.iter().map(|n| {
                    Some(NodeParts {
                        id: n.id.clone(),
                        label: n.label.clone(),
                        properties: n.properties.clone(),
                    })
                }));
                flat_rels.extend(rels.iter().map(|r| {
                    Some(RelParts {
                        id: r.id.clone(),
                        start_id: r.start_id.clone(),
                        end_id: r.end_id.clone(),
                        rel_type: r.rel_type.clone(),
                        properties: r.properties.clone(),
                    })
                }));
            }
            None => {
                node_lengths.push(0);
                rel_lengths.push(0);
                validity.push(false);
            }
        }
    }
    let nodes_list = ListArray::new(
        Arc::new(Field::new("item", DataType::Struct(node_fields()), true)),
        OffsetBuffer::from_lengths(node_lengths),
        Arc::new(node_struct_array(&flat_nodes)),
        Some(NullBuffer::from(validity.clone())),
    );
    let rels_list = ListArray::new(
        Arc::new(Field::new(
            "item",
            DataType::Struct(relationship_fields()),
            true,
        )),
        OffsetBuffer::from_lengths(rel_lengths),
        Arc::new(relationship_struct_array(&flat_rels)),
        Some(NullBuffer::from(validity)),
    );
    StructArray::new(
        path_fields(),
        vec![Arc::new(nodes_list), Arc::new(rels_list)],
        None,
    )
}

/// Ids arrive as agtype integers; stringify without float detours.
fn scalar_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, StringArray, StructArray};

    #[test]
    fn agtype_annotations_strip_outside_strings_only() {
        let text = r#"{"id": 1, "label": "A", "properties": {"note": "x::vertex"}}::vertex"#;
        let v = parse_agtype(text).expect("parses");
        assert_eq!(v["properties"]["note"], "x::vertex", "string survives");
        assert_eq!(v["id"], 1);

        // Numeric annotation mid-array.
        let v = parse_agtype(r#"[1.5::numeric, "a", {"x": 2}::vertex]"#).expect("parses");
        assert_eq!(v[0], 1.5);
        assert_eq!(v[2]["x"], 2);
    }

    #[test]
    fn non_ascii_string_values_survive_byte_faithfully() {
        // Latin-1 `u8 as char` shredding would turn 颱風 into mojibake
        // that STILL parses as JSON — this pins the byte-faithful path
        // (CJK, emoji, and a combining accent, inside and outside
        // annotated objects).
        let text = r#"{"id": 1, "label": "城市", "properties": {"name": "颱風", "note": "café ☔"}}::vertex"#;
        let v = parse_agtype(text).expect("parses");
        assert_eq!(v["label"], "城市");
        assert_eq!(v["properties"]["name"], "颱風");
        assert_eq!(v["properties"]["note"], "café ☔");

        let v = parse_agtype(r#"["中文", 1.5::numeric, "🎈"]"#).expect("parses");
        assert_eq!(v[0], "中文");
        assert_eq!(v[2], "🎈");
    }

    #[test]
    fn declared_types_parse_with_aliases_and_reject_pascal_case() {
        assert_eq!(GraphType::parse("utf8"), Some(GraphType::String));
        assert_eq!(GraphType::parse("bigint"), Some(GraphType::Int));
        assert_eq!(GraphType::parse("double"), Some(GraphType::Float));
        assert_eq!(GraphType::parse("path"), Some(GraphType::Path));
        // Arrow PascalCase is NOT the vocabulary (design: the repo's
        // friendly lowercase names, one spelling everywhere).
        assert_eq!(GraphType::parse("Utf8"), None);
        assert_eq!(GraphType::parse("Int64"), None);
    }

    fn col(name: &str, ty: GraphType) -> DeclaredColumn {
        DeclaredColumn {
            name: name.to_string(),
            ty,
        }
    }

    #[test]
    fn scalars_convert_and_nulls_pass_through() {
        let columns = vec![
            col("s", GraphType::String),
            col("i", GraphType::Int),
            col("f", GraphType::Float),
            col("b", GraphType::Bool),
            col("j", GraphType::Json),
        ];
        let rows = vec![
            vec![
                serde_json::json!("hi"),
                serde_json::json!(7),
                serde_json::json!(1.5),
                serde_json::json!(true),
                serde_json::json!({"k": [1, 2]}),
            ],
            vec![
                Value::Null,
                Value::Null,
                serde_json::json!(2), // int widens into a declared float
                Value::Null,
                Value::Null,
            ],
        ];
        let batch = build_batch(&columns, &rows, 0).expect("converts");
        assert_eq!(batch.num_rows(), 2);
        let s = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(s.value(0), "hi");
        assert!(s.is_null(1));
        let j = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(j.value(0), r#"{"k":[1,2]}"#);
    }

    #[test]
    fn type_mismatch_names_column_row_and_kinds_never_values() {
        let columns = vec![col("n", GraphType::Int)];
        let rows = vec![
            vec![serde_json::json!(1)],
            vec![serde_json::json!("secret-value")],
        ];
        let err = build_batch(&columns, &rows, 10).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("'n'"), "{msg}");
        assert!(msg.contains("row 11"), "absolute row index: {msg}");
        assert!(msg.contains("a string"), "{msg}");
        assert!(!msg.contains("secret-value"), "values never leak: {msg}");
    }

    #[test]
    fn a_float_does_not_pass_as_a_declared_int() {
        let columns = vec![col("n", GraphType::Int)];
        let rows = vec![vec![serde_json::json!(1.5)]];
        let err = build_batch(&columns, &rows, 0).unwrap_err();
        assert!(err.to_string().contains("declared 'int'"), "{err}");
    }

    #[test]
    fn node_converts_to_the_canonical_struct() {
        let columns = vec![col("v", GraphType::Node)];
        let vertex = parse_agtype(
            r#"{"id": 844424930131969, "label": "Person", "properties": {"name": "redacted"}}::vertex"#,
        )
        .unwrap();
        let rows = vec![vec![vertex], vec![Value::Null]];
        let batch = build_batch(&columns, &rows, 0).expect("converts");
        let s = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let ids = s
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.value(0), "844424930131969", "id is stringified");
        assert!(s.is_null(1), "null node row is a null struct");
        let props = s
            .column_by_name("properties")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(props.value(0), r#"{"name":"redacted"}"#);
    }

    #[test]
    fn path_splits_into_parallel_typed_lists() {
        let columns = vec![col("p", GraphType::Path)];
        let path = parse_agtype(
            r#"[{"id": 1, "label": "A", "properties": {}}::vertex, \
                {"id": 9, "label": "KNOWS", "start_id": 1, "end_id": 2, "properties": {}}::edge, \
                {"id": 2, "label": "B", "properties": {}}::vertex]::path"#
                .replace("\\\n", "")
                .as_str(),
        )
        .unwrap();
        let rows = vec![vec![path]];
        let batch = build_batch(&columns, &rows, 0).expect("converts");
        let s = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let nodes = s.column_by_name("nodes").unwrap();
        let rels = s.column_by_name("relationships").unwrap();
        let nodes = nodes
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        let rels = rels
            .as_any()
            .downcast_ref::<arrow::array::ListArray>()
            .unwrap();
        assert_eq!(nodes.value(0).len(), 2, "nodes is one longer");
        assert_eq!(rels.value(0).len(), 1);
    }

    #[test]
    fn an_even_length_path_is_a_typed_mismatch() {
        let columns = vec![col("p", GraphType::Path)];
        let rows = vec![vec![
            serde_json::json!([{"id":1,"label":"A","properties":{}},
                                                {"id":2,"label":"B","properties":{}}]),
        ]];
        let err = build_batch(&columns, &rows, 0).unwrap_err();
        assert!(err.to_string().contains("declared 'path'"), "{err}");
    }
}
