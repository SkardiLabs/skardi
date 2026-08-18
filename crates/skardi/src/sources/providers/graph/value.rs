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
//!   against the declared columns. Ad-hoc declared columns are always
//!   nullable (Cypher can produce null in any position); YAML views may
//!   declare `nullable: false` as an author's assertion, and a null met
//!   under it is [`GraphError::NotNullViolation`]. A non-null value of the
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
    /// Ad-hoc `cypher_query` columns are always nullable (Cypher can
    /// produce null in any position and the ad-hoc JSON object cannot
    /// declare otherwise, design §Schema handling); YAML views may
    /// declare `false` as an author's assertion, enforced by
    /// [`build_batch`] as [`GraphError::NotNullViolation`].
    pub nullable: bool,
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

/// The planned schema for a set of declared columns. Each field carries
/// its declaration's nullable bit — `true` everywhere on the ad-hoc
/// surface, where nulls cannot be declared away (design §Schema
/// handling); YAML views may declare `nullable: false`.
pub fn declared_schema(columns: &[DeclaredColumn]) -> Arc<Schema> {
    Arc::new(Schema::new(
        columns
            .iter()
            .map(|c| Field::new(&c.name, c.ty.arrow_type(), c.nullable))
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
            b if b.is_ascii_alphabetic() => {
                // agtype's float specials are bare non-JSON tokens —
                // VERIFIED live: `RETURN 1.0e308 * 10` emits `Infinity`
                // through agtype_out (and `-Infinity`/`NaN` are its
                // siblings). JSON has no spelling for them, so they
                // decode to null — the proportionate outcome for a
                // declared float (AGE's own sqrt(-1.0) already answers
                // SQL NULL), instead of a whole-scan MalformedCell for a
                // legitimate value. Other bare words (true/false/null)
                // pass through untouched; strings are untouched by
                // construction (this arm is outside-string only).
                let start = i;
                while i < bytes.len() && bytes[i].is_ascii_alphabetic() {
                    i += 1;
                }
                let word = &text[start..i];
                if word == "NaN" || word == "Infinity" {
                    // A leading sign belongs to the token (`-Infinity`),
                    // not to the null replacing it.
                    if cleaned.last() == Some(&b'-') {
                        cleaned.pop();
                    }
                    cleaned.extend_from_slice(b"null");
                } else {
                    cleaned.extend_from_slice(word.as_bytes());
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
    // Arity first: a backend row shorter than the declared schema must
    // be a typed error, not an index panic (future Neo4j/Kuzu drivers
    // make this reachable even if the AGE client can't produce it).
    for (i, row) in rows.iter().enumerate() {
        if row.len() != columns.len() {
            return Err(GraphError::RowArityMismatch {
                row: row_base + i,
                expected: columns.len(),
                found: row.len(),
            });
        }
    }
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(columns.len());
    for (col_idx, col) in columns.iter().enumerate() {
        // A declared `nullable: false` is the author's assertion about
        // the view's Cypher — enforce it BEFORE conversion so the null
        // surfaces as its own typed error, not as a struct field that
        // happens to be empty.
        if !col.nullable {
            for (i, row) in rows.iter().enumerate() {
                if matches!(row[col_idx], Value::Null) {
                    return Err(GraphError::NotNullViolation {
                        column: col.name.clone(),
                        row: row_base + i,
                    });
                }
            }
        }
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
                        Value::Number(n) => match n.as_i64() {
                            Some(i) => b.append_value(i),
                            None => return Err(mismatch(col, row_base + i, "int", v)),
                        },
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
            // Endpoint keys are the structural tell of an EDGE: an edge
            // object ({id, label, start_id, end_id, properties}) would
            // otherwise satisfy the node shape completely — id and label
            // present — and a `RETURN r` declared as `node` would decode
            // silently, dropping start_id/end_id on the floor. The
            // reverse direction already fails naturally (a vertex lacks
            // the endpoint keys a relationship column requires); this
            // check closes the hole from the other side, turning a
            // silent wrong answer into the typed error the design
            // promises. Sound because both keys are structural fields,
            // never property names: properties live under their own
            // `properties` sub-object.
            if map.contains_key("start_id") || map.contains_key("end_id") {
                return Err(mismatch(col, row, "node", v));
            }
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
            // A path is ODD-length by construction: n nodes alternate
            // with n-1 edges, and the minimum is one node (the design's
            // zero-hop path). Even lengths AND the empty array are both
            // malformed — an empty array must not be silently accepted
            // as a "0-node path".
            if elements.len().is_multiple_of(2) {
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
        Some(NullBuffer::from(validity.clone())),
    );
    // The parent STRUCT carries the SAME validity as its child lists —
    // with None here, a NULL path row would read as a valid struct whose
    // lists happen to be null, and `path IS NULL` would answer false.
    StructArray::new(
        path_fields(),
        vec![Arc::new(nodes_list), Arc::new(rels_list)],
        Some(NullBuffer::from(validity)),
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
    fn agtype_float_specials_decode_to_null_outside_strings_only() {
        // Verified live: AGE emits bare `Infinity` for float overflow
        // (1.0e308 * 10). JSON has no spelling for the specials, so they
        // decode to null — proportionate for a declared float — while
        // string CONTENT is untouched and ordinary words pass through.
        let v = parse_agtype(r#"[Infinity, -Infinity, NaN, 1.5, true]"#).expect("parses");
        assert_eq!(v[0], Value::Null);
        assert_eq!(v[1], Value::Null, "the sign is consumed with the token");
        assert_eq!(v[2], Value::Null);
        assert_eq!(v[3], 1.5);
        assert_eq!(v[4], true);

        let v =
            parse_agtype(r#"{"note": "to Infinity and beyond", "x": Infinity}"#).expect("parses");
        assert_eq!(v["note"], "to Infinity and beyond", "strings untouched");
        assert_eq!(v["x"], Value::Null);
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
            nullable: true,
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
    fn relationships_convert_to_the_canonical_struct_with_null_rows() {
        let columns = vec![col("r", GraphType::Relationship)];
        let edge = parse_agtype(
            r#"{"id": 9, "label": "KNOWS", "start_id": 1, "end_id": 2, "properties": {"since": 2019}}::edge"#,
        )
        .unwrap();
        let rows = vec![vec![edge], vec![Value::Null]];
        let batch = build_batch(&columns, &rows, 0).expect("converts");
        let s = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(s.is_null(1), "null relationship row is a null struct");
        let types = s
            .column_by_name("type")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(types.value(0), "KNOWS");
        let starts = s
            .column_by_name("start_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(starts.value(0), "1", "endpoint ids stringify");
    }

    #[test]
    fn every_wrong_kind_is_a_typed_mismatch_per_declared_type() {
        // One wrong-kind row per scalar declared type, plus the malformed
        // struct shapes — each must name the declared type, never a value.
        let cases: Vec<(GraphType, Value, &str)> = vec![
            (GraphType::String, serde_json::json!(7), "string"),
            (GraphType::Bool, serde_json::json!("yes"), "bool"),
            (GraphType::Float, serde_json::json!("1.5"), "float"),
            (GraphType::Node, serde_json::json!({"id": 1}), "node"), // no label
            (
                // An EDGE object satisfies the node keys (id + label) —
                // the endpoint keys are what must reject it, or a
                // `RETURN r` declared `node` decodes silently with
                // start_id/end_id dropped.
                GraphType::Node,
                serde_json::json!({"id": 9, "label": "KNOWS", "start_id": 1,
                                   "end_id": 2, "properties": {}}),
                "node",
            ),
            (
                GraphType::Relationship,
                serde_json::json!({"id": 1, "label": "K"}), // no endpoints
                "relationship",
            ),
            (GraphType::Path, serde_json::json!("not-a-list"), "path"),
        ];
        for (ty, bad, name) in cases {
            let columns = vec![col("c", ty)];
            let err = build_batch(&columns, &[vec![bad]], 0).unwrap_err();
            assert!(
                err.to_string().contains(&format!("declared '{name}'")),
                "{name}: {err}"
            );
        }
    }

    #[test]
    fn canonical_names_and_arrow_types_cover_every_variant() {
        for (ty, canonical) in [
            (GraphType::String, "string"),
            (GraphType::Int, "int"),
            (GraphType::Float, "float"),
            (GraphType::Bool, "bool"),
            (GraphType::Json, "json"),
            (GraphType::Node, "node"),
            (GraphType::Relationship, "relationship"),
            (GraphType::Path, "path"),
        ] {
            assert_eq!(ty.canonical(), canonical);
            // Round-trip: the canonical name parses back to the same type.
            assert_eq!(GraphType::parse(canonical), Some(ty));
            // And every variant plans a concrete Arrow type.
            let _ = ty.arrow_type();
        }
    }

    #[test]
    fn a_path_with_a_malformed_element_is_a_typed_mismatch() {
        // An edge slot holding a non-edge object fails as 'relationship'
        // (the element conversion), with the path row named.
        let columns = vec![col("p", GraphType::Path)];
        let rows = vec![vec![serde_json::json!([
            {"id": 1, "label": "A", "properties": {}},
            {"id": 9}, // not an edge
            {"id": 2, "label": "B", "properties": {}}
        ])]];
        let err = build_batch(&columns, &rows, 3).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("declared 'relationship'"), "{msg}");
        assert!(msg.contains("row 3"), "{msg}");
    }

    #[test]
    fn a_null_path_row_is_a_null_struct() {
        // With a None parent validity the null row would read as a VALID
        // struct whose child lists are null — and SQL `path IS NULL`
        // would answer false. The parent must carry the row validity.
        let columns = vec![col("p", GraphType::Path)];
        let rows = vec![
            vec![serde_json::json!([{"id":1,"label":"A","properties":{}}])],
            vec![Value::Null],
        ];
        let batch = build_batch(&columns, &rows, 0).expect("converts");
        let paths = batch
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(!paths.is_null(0));
        assert!(paths.is_null(1), "NULL path row must be a NULL struct");
    }

    #[test]
    fn a_short_row_is_a_typed_arity_error_not_a_panic() {
        // A backend row narrower than the declared schema (reachable via
        // future drivers) must surface as a typed error with identity.
        let columns = vec![col("a", GraphType::Int), col("b", GraphType::Int)];
        let rows = vec![
            vec![serde_json::json!(1), serde_json::json!(2)],
            vec![serde_json::json!(3)], // one column short
        ];
        let err = build_batch(&columns, &rows, 5).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("row 6"), "{msg}");
        assert!(
            msg.contains("carries 1 columns but 2 were declared"),
            "{msg}"
        );
    }

    #[test]
    fn a_null_under_nullable_false_is_a_typed_violation_with_identity() {
        // YAML views may assert `nullable: false`; a null met under it is
        // NotNullViolation naming the column and the ABSOLUTE row — never
        // a value (there is none to leak, but the discipline holds).
        let columns = vec![DeclaredColumn {
            name: "n".to_string(),
            ty: GraphType::Int,
            nullable: false,
        }];
        let rows = vec![vec![serde_json::json!(1)], vec![Value::Null]];
        let err = build_batch(&columns, &rows, 10).unwrap_err();
        assert!(matches!(err, GraphError::NotNullViolation { .. }), "{err}");
        let msg = err.to_string();
        assert!(msg.contains("'n'"), "{msg}");
        assert!(msg.contains("row 11"), "absolute row index: {msg}");
        assert!(msg.contains("nullable: false"), "{msg}");
        // The same null under a nullable declaration keeps passing.
        build_batch(&[col("n", GraphType::Int)], &rows, 10).expect("nullable passes");
    }

    #[test]
    fn declared_schema_carries_each_columns_nullable_bit() {
        let columns = vec![
            col("opt", GraphType::String),
            DeclaredColumn {
                name: "req".to_string(),
                ty: GraphType::Int,
                nullable: false,
            },
        ];
        let schema = declared_schema(&columns);
        assert!(schema.field(0).is_nullable());
        assert!(!schema.field(1).is_nullable());
    }

    #[test]
    fn even_length_and_empty_paths_are_typed_mismatches() {
        let columns = vec![col("p", GraphType::Path)];
        let rows = vec![vec![
            serde_json::json!([{"id":1,"label":"A","properties":{}},
                                                {"id":2,"label":"B","properties":{}}]),
        ]];
        let err = build_batch(&columns, &rows, 0).unwrap_err();
        assert!(err.to_string().contains("declared 'path'"), "{err}");

        // The empty array is malformed too — an AGE path always carries
        // at least one node, and silently accepting [] as a "0-node
        // path" would hide upstream corruption.
        let rows = vec![vec![serde_json::json!([])]];
        let err = build_batch(&columns, &rows, 0).unwrap_err();
        assert!(err.to_string().contains("declared 'path'"), "{err}");

        // The zero-hop minimum stays legal: one node, no edges.
        let rows = vec![vec![
            serde_json::json!([{"id":1,"label":"A","properties":{}}]),
        ]];
        build_batch(&columns, &rows, 0).expect("a single-node path is the legal minimum");
    }
}
