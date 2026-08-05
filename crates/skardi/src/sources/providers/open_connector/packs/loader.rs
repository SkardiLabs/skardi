//! Embedded-YAML source-pack loader.
//!
//! Built-in packs are declarative YAML assets (`packs/*.yaml`) compiled
//! into the binary with `include_str!` — the design doc's illustrative
//! pack format, chosen over Rust statics so a pack is reviewable and
//! generatable as plain data and so the format is ready for the designed
//! second tier (user-authored packs from a directory). To be explicit
//! about what this is NOT: there is no dynamic loading here — adding a
//! built-in pack still means a YAML asset plus a small Rust accessor
//! module, a `mod` declaration, a registry entry, and a rebuild.
//! Directory-loaded user packs remain the design's deliberately deferred
//! tier; this refactor stabilizes the format they will use. Nothing about
//! the contract boundary changes: packs are still shipped inside the
//! Skardi binary, versioned, fingerprint-gated, and never user-editable
//! configuration.
//!
//! Each asset is parsed exactly once (first registry access) and leaked
//! into the `&'static` shapes the scan engine borrows — a bounded,
//! process-lifetime allocation replacing what used to be `static` data.
//! Parsing is strict (`deny_unknown_fields` everywhere) so a misspelled
//! key in an asset fails loudly instead of silently disabling what it was
//! meant to set, and the loader cross-validates the document (duplicate
//! columns, filters referencing undeclared columns, input-key collisions)
//! before converting it to runtime objects. A malformed embedded asset is
//! a build defect, but it surfaces as a targeted
//! [`OpenConnectorError::SourcePackAssetInvalid`] at registration / UDTF
//! setup — never a panic — so a generated pack that slips past review
//! produces a startup diagnostic, and the parse-all test below keeps
//! shipped assets valid in the first place.

use std::collections::BTreeMap;
use std::sync::OnceLock;

use datafusion::logical_expr::Operator;
use serde::Deserialize;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use crate::sources::providers::open_connector::filters::{Fidelity, FilterMapping, ValueFormat};
use crate::sources::providers::open_connector::json_to_arrow::RowConverter;
use crate::sources::providers::open_connector::json_to_arrow::{FieldMapping, FieldType};
use crate::sources::providers::open_connector::pagination::PaginationStrategy;
use crate::sources::providers::open_connector::row_path::RowPath;
use crate::sources::providers::open_connector::source_pack::{
    FixedValue, SourcePack, SourcePackTable,
};

/// Parse an embedded pack asset, memoized in `cell`.
///
/// A malformed asset yields [`OpenConnectorError::SourcePackAssetInvalid`]
/// on every access — a registration/startup diagnostic, not a panic.
/// `builtin_assets_parse_and_validate` pins every shipped asset as valid.
pub(crate) fn builtin(
    asset: &'static str,
    yaml: &'static str,
    cell: &'static OnceLock<Result<SourcePack, String>>,
) -> Result<&'static SourcePack, OpenConnectorError> {
    cell.get_or_init(|| parse_pack(yaml))
        .as_ref()
        .map_err(|reason| OpenConnectorError::SourcePackAssetInvalid {
            asset: asset.to_string(),
            reason: reason.clone(),
        })
}

/// Parse one pack document into the engine's static shapes.
pub(crate) fn parse_pack(yaml: &str) -> Result<SourcePack, String> {
    let doc: PackDoc = serde_yaml::from_str(yaml).map_err(|e| e.to_string())?;
    if doc.tables.is_empty() {
        return Err("a pack must declare at least one table".to_string());
    }
    let pack_name = leak_str(doc.pack);
    let mut tables = Vec::with_capacity(doc.tables.len());
    for (short_name, table) in doc.tables {
        if short_name.contains('.') {
            return Err(format!(
                "table key '{short_name}' must be a bare name; the id is derived as \
                 <pack>.<table>"
            ));
        }
        tables.push(convert_table(pack_name, &short_name, table)?);
    }
    Ok(SourcePack {
        name: pack_name,
        version: doc.version,
        tables: leak_slice(tables),
    })
}

fn convert_table(
    pack: &'static str,
    short_name: &str,
    doc: TableDoc,
) -> Result<SourcePackTable, String> {
    let id = leak_str(format!("{pack}.{short_name}"));
    let mut fields = Vec::with_capacity(doc.columns.len());
    for column in doc.columns {
        fields.push(convert_column(id, column)?);
    }
    let filters = doc
        .filters
        .into_iter()
        .map(convert_filter)
        .collect::<Vec<_>>();
    let mut fixed_inputs = Vec::with_capacity(doc.fixed_inputs.len());
    for (key, value) in doc.fixed_inputs {
        // YAML happily parses `.nan` / `.inf` / `-.inf`, but JSON has no
        // spelling for them — FixedValue::to_json would silently send null
        // at query time, bypassing the startup diagnostic this loader
        // promises. Finite-only, enforced here. The nested case CANNOT be
        // checked after the fact on a `serde_json::Value`: serde_json's
        // f64 visitor maps a non-finite to `Value::Null` during
        // deserialization (`Number::from_f64(...).map_or(Value::Null, …)`),
        // so the loss has already happened by then — which is why the
        // `Json` variant captures `serde_yaml::Value` and converts here,
        // where the non-finite is still observable.
        let fixed = match value {
            FixedValueDoc::Float(v) if !v.is_finite() => {
                return Err(format!(
                    "{id}: fixed input '{key}' contains {v}, which has no JSON \
                     spelling; pin finite numbers only"
                ));
            }
            FixedValueDoc::Bool(v) => FixedValue::Bool(v),
            FixedValueDoc::Int(v) => FixedValue::Int(v),
            FixedValueDoc::Float(v) => FixedValue::Float(v),
            FixedValueDoc::Str(v) => FixedValue::Str(leak_str(v)),
            FixedValueDoc::StrList(v) => FixedValue::StrList(leak_str_slice(v)),
            FixedValueDoc::Json(v) => {
                let json = yaml_to_json(v)
                    .map_err(|reason| format!("{id}: fixed input '{key}' {reason}"))?;
                FixedValue::Json(Box::leak(Box::new(json)))
            }
        };
        fixed_inputs.push((leak_str(key), fixed));
    }
    let table = SourcePackTable {
        id,
        action_id: leak_str(doc.action),
        row_path: leak_str(doc.row_path),
        fields: leak_slice(fields),
        pagination: doc.pagination.into_strategy(),
        required_resources: leak_str_slice(doc.resources.required),
        optional_resources: leak_str_slice(doc.resources.optional),
        fixed_inputs: leak_slice(fixed_inputs),
        filters: leak_slice(filters),
        error_path: doc.error_path.map(leak_str),
        expected_fingerprint: doc.fingerprint.map(leak_str),
    };
    validate_table(&table)?;
    Ok(table)
}

/// Cross-field validation, run before the document becomes a runtime
/// object. Serde guarantees shape; these are the semantic invariants the
/// engine assumes but does not re-check per scan.
fn validate_table(table: &SourcePackTable) -> Result<(), String> {
    let id = table.id;
    // Structural pieces the engine parses lazily elsewhere fail HERE so a
    // generated asset gets one complete diagnostic pass.
    RowPath::parse(table.row_path).map_err(|e| format!("{id}: {e}"))?;
    if let Some(path) = table.error_path {
        RowPath::parse(path).map_err(|e| format!("{id}: {e}"))?;
    }
    table
        .pagination
        .validate()
        .map_err(|e| format!("{id}: {e}"))?;
    RowConverter::new(table.fields).map_err(|e| format!("{id}: {e}"))?;

    let mut columns = std::collections::HashSet::new();
    for field in table.fields {
        if !columns.insert(field.name) {
            return Err(format!("{id}: duplicate column '{}'", field.name));
        }
    }
    let mut mappings = std::collections::HashSet::new();
    for filter in table.filters {
        if !columns.contains(filter.column) {
            return Err(format!(
                "{id}: filter references undeclared column '{}'",
                filter.column
            ));
        }
        if !mappings.insert((filter.column, format!("{:?}", filter.operator))) {
            return Err(format!(
                "{id}: duplicate filter mapping for column '{}' and operator {:?}",
                filter.column, filter.operator
            ));
        }
        // Epoch renderings floor sub-second precision, which only ever
        // WIDENS a lower bound; under any other operator the floored
        // literal compares at a different instant and the provider drops
        // rows that Inexact re-filtering cannot recover — and under Exact,
        // DataFusion never re-filters the widened fetch at all. Both rules
        // enforced here so the drift is a load failure, not silent row
        // loss (the ValueFormat docs state the same contract).
        if matches!(
            filter.value_format,
            ValueFormat::EpochSeconds | ValueFormat::EpochSecondsString
        ) {
            if !matches!(filter.operator, Operator::Gt | Operator::GtEq) {
                return Err(format!(
                    "{id}: filter on '{}' renders as flooring epoch seconds, which is only \
                     sound for lower bounds — operator {:?} would drop rows; declare gt/gt_eq \
                     or use the rfc3339 format",
                    filter.column, filter.operator
                ));
            }
            if filter.fidelity != Fidelity::Inexact {
                return Err(format!(
                    "{id}: filter on '{}' floors sub-second literals into a WIDER fetch, so it \
                     must be declared inexact (DataFusion re-filters locally); exact would \
                     surface rows the predicate excludes",
                    filter.column
                ));
            }
        }
    }
    for required in table.required_resources {
        if table.optional_resources.contains(required) {
            return Err(format!(
                "{id}: resource '{required}' is declared both required and optional"
            ));
        }
    }
    // The request-input namespace is shared by resources, fixed inputs,
    // filter inputs, and pagination parameters; exec.rs applies pagination
    // LAST, so a collision would silently overwrite an Exact pushed
    // predicate DataFusion never reapplies. Filter-vs-fixed-input overlap
    // is the one DELIBERATE collision (a pushed predicate overrides the
    // complete-collection pin) and stays legal.
    let pagination_params: Vec<&str> = match table.pagination {
        PaginationStrategy::PageNumber {
            page_param,
            per_page_param,
            ..
        } => vec![page_param, per_page_param],
        PaginationStrategy::Cursor {
            cursor_param,
            page_size_param,
            ..
        } => std::iter::once(cursor_param)
            .chain(page_size_param)
            .collect(),
        PaginationStrategy::SinglePage => Vec::new(),
    };
    match table.pagination {
        PaginationStrategy::PageNumber {
            page_param,
            per_page_param,
            per_page,
            ..
        } => {
            if page_param == per_page_param {
                return Err(format!(
                    "{id}: pagination declares '{page_param}' as both the page and page-size input"
                ));
            }
            if per_page == 0 {
                return Err(format!("{id}: pagination page size must be positive"));
            }
        }
        PaginationStrategy::Cursor {
            cursor_param,
            page_size_param,
            page_size,
            ..
        } => {
            if page_size_param == Some(cursor_param) {
                return Err(format!(
                    "{id}: pagination declares '{cursor_param}' as both the cursor and page-size input"
                ));
            }
            if page_size_param.is_some() && page_size == 0 {
                return Err(format!("{id}: pagination page size must be positive"));
            }
        }
        PaginationStrategy::SinglePage => {}
    }
    // NOT rejected on purpose: a filter input equal to a FIXED input is
    // the override mechanism itself (a pushed predicate replacing the
    // complete-collection pin — the github state=all pattern, pinned
    // end-to-end by the pack tests). Two filters sharing an input are
    // rejected below even though the scan-time claimed-inputs guard makes
    // them safe (the loser stays local): which predicate pushes would
    // depend on query order, and that ambiguity is an authoring mistake.
    let mut filter_inputs = std::collections::HashSet::new();
    for filter in table.filters {
        if !filter_inputs.insert(filter.input_field) {
            return Err(format!(
                "{id}: two filter mappings target input '{}'; declare one mapping per input",
                filter.input_field
            ));
        }
        if pagination_params.contains(&filter.input_field) {
            return Err(format!(
                "{id}: filter input '{}' collides with a pagination input, which is applied last and would overwrite the pushed predicate",
                filter.input_field
            ));
        }
        if table.declares_resource(filter.input_field) {
            return Err(format!(
                "{id}: filter input '{}' collides with a declared resource",
                filter.input_field
            ));
        }
    }
    for (key, _) in table.fixed_inputs {
        if table.declares_resource(key) {
            return Err(format!(
                "{id}: fixed input '{key}' collides with a declared resource — the \
                 request would carry an ambiguous value"
            ));
        }
        if pagination_params.contains(key) {
            return Err(format!(
                "{id}: fixed input '{key}' collides with a pagination input"
            ));
        }
    }
    Ok(())
}

fn convert_column(table_id: &str, doc: ColumnDoc) -> Result<FieldMapping, String> {
    let field_type = match (doc.column_type, doc.key) {
        (ColumnType::Utf8ListFromObjectKey, Some(key)) => {
            FieldType::Utf8ListFromObjectKey(leak_str(key))
        }
        (ColumnType::Utf8ListFromObjectKey, None) => {
            return Err(format!(
                "{table_id}: column '{}' has type utf8_list_from_object_key and needs `key`",
                doc.name
            ));
        }
        (other, Some(_)) => {
            return Err(format!(
                "{table_id}: column '{}' declares `key`, which only \
                 utf8_list_from_object_key accepts (got {other:?})",
                doc.name
            ));
        }
        (ColumnType::Boolean, None) => FieldType::Boolean,
        (ColumnType::Int64, None) => FieldType::Int64,
        (ColumnType::Uint64, None) => FieldType::UInt64,
        (ColumnType::Float64, None) => FieldType::Float64,
        (ColumnType::Utf8, None) => FieldType::Utf8,
        (ColumnType::TimestampMsUtc, None) => FieldType::TimestampMillisUtc,
        (ColumnType::TimestampSUtc, None) => FieldType::TimestampSecondsUtc,
        (ColumnType::TimestampMsStringUtc, None) => FieldType::TimestampMillisStringUtc,
        (ColumnType::TimestampSStringUtc, None) => FieldType::TimestampSecondsStringUtc,
        (ColumnType::Utf8List, None) => FieldType::Utf8List,
        (ColumnType::Json, None) => FieldType::Json,
    };
    Ok(FieldMapping {
        name: leak_str(doc.name),
        path: leak_str(doc.path),
        field_type,
        nullable: doc.nullable,
    })
}

fn convert_filter(doc: FilterDoc) -> FilterMapping {
    FilterMapping {
        column: leak_str(doc.column),
        operator: match doc.op {
            OpDoc::Eq => Operator::Eq,
            OpDoc::Gt => Operator::Gt,
            OpDoc::GtEq => Operator::GtEq,
        },
        input_field: leak_str(doc.input),
        fidelity: match doc.fidelity {
            FidelityDoc::Exact => Fidelity::Exact,
            FidelityDoc::Inexact => Fidelity::Inexact,
        },
        value_format: match doc.format {
            FormatDoc::Verbatim => ValueFormat::Verbatim,
            FormatDoc::Rfc3339 => ValueFormat::Rfc3339,
            FormatDoc::EpochSeconds => ValueFormat::EpochSeconds,
            FormatDoc::EpochSecondsString => ValueFormat::EpochSecondsString,
        },
    }
}

fn leak_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn leak_slice<T>(v: Vec<T>) -> &'static [T] {
    Box::leak(v.into_boxed_slice())
}

fn leak_str_slice(v: Vec<String>) -> &'static [&'static str] {
    leak_slice(v.into_iter().map(leak_str).collect())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PackDoc {
    #[allow(dead_code, reason = "the tag's value is its validation")]
    kind: KindTag,
    pack: String,
    version: u32,
    /// BTreeMap so table order (and thus catalog listings) is
    /// deterministic regardless of asset layout.
    tables: BTreeMap<String, TableDoc>,
}

#[derive(Deserialize)]
enum KindTag {
    #[serde(rename = "pack")]
    Pack,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TableDoc {
    action: String,
    row_path: String,
    #[serde(default)]
    fingerprint: Option<String>,
    pagination: PaginationDoc,
    #[serde(default)]
    resources: ResourcesDoc,
    #[serde(default)]
    fixed_inputs: BTreeMap<String, FixedValueDoc>,
    #[serde(default)]
    error_path: Option<String>,
    columns: Vec<ColumnDoc>,
    #[serde(default)]
    filters: Vec<FilterDoc>,
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ResourcesDoc {
    #[serde(default)]
    required: Vec<String>,
    #[serde(default)]
    optional: Vec<String>,
}

#[derive(Deserialize)]
#[serde(tag = "strategy", rename_all = "snake_case", deny_unknown_fields)]
enum PaginationDoc {
    PageNumber {
        page_input: String,
        page_size_input: String,
        page_size: u32,
        #[serde(default)]
        total_pages_path: Option<String>,
        #[serde(default)]
        raw_page_size_path: Option<String>,
    },
    Cursor {
        cursor_input: String,
        next_cursor_path: String,
        #[serde(default)]
        page_size_input: Option<String>,
        page_size: u32,
        #[serde(default)]
        has_more_path: Option<String>,
    },
}

impl PaginationDoc {
    fn into_strategy(self) -> PaginationStrategy {
        match self {
            Self::PageNumber {
                page_input,
                page_size_input,
                page_size,
                total_pages_path,
                raw_page_size_path,
            } => PaginationStrategy::PageNumber {
                page_param: leak_str(page_input),
                per_page_param: leak_str(page_size_input),
                per_page: page_size,
                total_pages_path: total_pages_path.map(leak_str),
                raw_page_size_path: raw_page_size_path.map(leak_str),
            },
            Self::Cursor {
                cursor_input,
                next_cursor_path,
                page_size_input,
                page_size,
                has_more_path,
            } => PaginationStrategy::Cursor {
                cursor_param: leak_str(cursor_input),
                next_cursor_path: leak_str(next_cursor_path),
                page_size_param: page_size_input.map(leak_str),
                page_size,
                has_more_path: has_more_path.map(leak_str),
            },
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ColumnDoc {
    name: String,
    path: String,
    #[serde(rename = "type")]
    column_type: ColumnType,
    /// Object key to pluck; only valid with `utf8_list_from_object_key`.
    #[serde(default)]
    key: Option<String>,
    nullable: bool,
}

#[derive(Debug, Deserialize, Clone, Copy)]
enum ColumnType {
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "int64")]
    Int64,
    #[serde(rename = "uint64")]
    Uint64,
    #[serde(rename = "float64")]
    Float64,
    #[serde(rename = "utf8")]
    Utf8,
    #[serde(rename = "timestamp_ms_utc")]
    TimestampMsUtc,
    #[serde(rename = "timestamp_s_utc")]
    TimestampSUtc,
    #[serde(rename = "timestamp_ms_string_utc")]
    TimestampMsStringUtc,
    #[serde(rename = "timestamp_s_string_utc")]
    TimestampSStringUtc,
    #[serde(rename = "utf8_list")]
    Utf8List,
    #[serde(rename = "utf8_list_from_object_key")]
    Utf8ListFromObjectKey,
    #[serde(rename = "json")]
    Json,
}

/// Untagged so the YAML scalar's own type selects the variant; order puts
/// the narrower matches first.
#[derive(Deserialize)]
#[serde(untagged)]
enum FixedValueDoc {
    Bool(bool),
    Int(i64),
    Float(f64),
    Str(String),
    StrList(Vec<String>),
    /// Object-shaped inputs (e.g. Notion's search `filter`). Captured as a
    /// YAML value — NOT `serde_json::Value`, whose f64 visitor converts a
    /// nested `.nan`/`.inf` to `Value::Null` during deserialization,
    /// destroying the evidence before any guard can run — and converted
    /// fallibly by [`yaml_to_json`] at the call site, where non-finite
    /// floats, non-string mapping keys, and YAML tags are rejected with
    /// targeted messages.
    Json(serde_yaml::Value),
}

/// Convert a YAML value to JSON, rejecting everything JSON cannot spell:
/// non-finite floats (`.nan` / `.inf` / `-.inf`, which `serde_json` would
/// silently write as `null`), non-string mapping keys, and YAML tags. The
/// error is a reason fragment; callers prefix the table/key identity.
fn yaml_to_json(value: serde_yaml::Value) -> Result<serde_json::Value, String> {
    use serde_yaml::Value as Yaml;
    Ok(match value {
        Yaml::Null => serde_json::Value::Null,
        Yaml::Bool(b) => serde_json::Value::from(b),
        Yaml::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_json::Value::from(i)
            } else if let Some(u) = n.as_u64() {
                serde_json::Value::from(u)
            } else {
                let f = n.as_f64().unwrap_or(f64::NAN);
                if !f.is_finite() {
                    return Err(format!(
                        "contains {f}, which has no JSON spelling; pin finite numbers only"
                    ));
                }
                serde_json::Value::from(f)
            }
        }
        Yaml::String(s) => serde_json::Value::from(s),
        Yaml::Sequence(items) => serde_json::Value::Array(
            items
                .into_iter()
                .map(yaml_to_json)
                .collect::<Result<_, _>>()?,
        ),
        Yaml::Mapping(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                let Yaml::String(k) = k else {
                    return Err(
                        "contains a non-string mapping key, which JSON cannot represent"
                            .to_string(),
                    );
                };
                out.insert(k, yaml_to_json(v)?);
            }
            serde_json::Value::Object(out)
        }
        // Unreachable through the untagged FixedValueDoc path (serde's
        // untagged buffering rejects tags during deserialization, pinned by
        // the loader test), kept as defense in depth for any future direct
        // caller.
        Yaml::Tagged(_) => {
            return Err("contains a YAML tag, which JSON cannot represent".to_string());
        }
    })
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FilterDoc {
    column: String,
    op: OpDoc,
    input: String,
    fidelity: FidelityDoc,
    #[serde(default)]
    format: FormatDoc,
}

#[derive(Deserialize)]
enum OpDoc {
    #[serde(rename = "eq")]
    Eq,
    #[serde(rename = "gt")]
    Gt,
    #[serde(rename = "gt_eq")]
    GtEq,
}

#[derive(Deserialize, Default)]
enum FidelityDoc {
    #[serde(rename = "exact")]
    #[default]
    Exact,
    #[serde(rename = "inexact")]
    Inexact,
}

/// Defaults to `verbatim` — the safe spelling: non-timestamp literals
/// render naturally and a timestamp literal refuses to push.
#[derive(Deserialize, Default)]
enum FormatDoc {
    #[serde(rename = "verbatim")]
    #[default]
    Verbatim,
    #[serde(rename = "rfc3339")]
    Rfc3339,
    #[serde(rename = "epoch_seconds")]
    EpochSeconds,
    #[serde(rename = "epoch_seconds_string")]
    EpochSecondsString,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every shipped asset parses AND passes the same structural checks
    /// binding performs — this is what keeps `builtin`'s panic
    /// unreachable in a released binary.
    #[test]
    fn builtin_assets_parse_and_validate() {
        for (asset, yaml) in [
            ("mock.yaml", include_str!("mock.yaml")),
            ("github.yaml", include_str!("github.yaml")),
            ("slack.yaml", include_str!("slack.yaml")),
            ("notion.yaml", include_str!("notion.yaml")),
            ("feishu.yaml", include_str!("feishu.yaml")),
        ] {
            // parse_pack performs the full structural + cross-field
            // validation pass itself; parsing IS the gate.
            let pack = parse_pack(yaml).unwrap_or_else(|e| panic!("{asset}: {e}"));
            assert!(!pack.tables.is_empty(), "{asset}: no tables");
        }
    }

    /// The pass side of the nested-value conversion: a finite nested float
    /// (and the rest of the JSON scalar set) survives the YAML→JSON
    /// conversion faithfully — proving the strict rejection above is a
    /// guard, not a ban on nesting.
    #[test]
    fn nested_finite_values_in_a_json_pin_convert_faithfully() {
        let pack = parse_pack(
            r#"kind: pack
pack: demo
version: 1
tables:
  things:
    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    fixed_inputs:
      filter:
        threshold: 1.5
        flags: [true, 2, "three"]
        inner: { level: null }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
"#,
        )
        .expect("nested finite values are legal");
        let (key, value) = &pack.tables[0].fixed_inputs[0];
        assert_eq!(*key, "filter");
        assert_eq!(
            value.to_json(),
            serde_json::json!({
                "threshold": 1.5,
                "flags": [true, 2, "three"],
                "inner": { "level": null }
            })
        );
    }

    #[test]
    fn misspelled_keys_fail_loudly() {
        // deny_unknown_fields end to end: a typo'd pagination key must not
        // silently disable what it was meant to set.
        let err = parse_pack(
            r#"
kind: pack
pack: demo
version: 1
tables:
  items:
    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10, total_page_path: "$.pages" }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
"#,
        )
        .unwrap_err();
        assert!(err.contains("total_page_path"), "{err}");
    }

    #[test]
    fn key_field_is_bound_to_the_plucking_type() {
        let base = |columns: &str| {
            format!(
                r#"
kind: pack
pack: demo
version: 1
tables:
  items:
    action: demo.list
    row_path: "$.items"
    pagination: {{ strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }}
    columns:
{columns}
"#
            )
        };
        let err = parse_pack(&base(
            "      - { name: labels, path: labels, type: utf8_list_from_object_key, nullable: true }",
        ))
        .unwrap_err();
        assert!(err.contains("needs `key`"), "{err}");

        let err = parse_pack(&base(
            "      - { name: id, path: id, type: uint64, key: name, nullable: false }",
        ))
        .unwrap_err();
        assert!(err.contains("only"), "{err}");
    }

    #[test]
    fn epoch_formats_are_lower_bound_inexact_only() {
        // Flooring widens LOWER bounds only; any other operator (or an
        // Exact claim over the widened fetch) drops or surfaces wrong
        // rows silently — so both are load failures, not runtime hazards.
        let base = |filter: &str| {
            format!(
                r#"
kind: pack
pack: demo
version: 1
tables:
  items:
    action: demo.list
    row_path: "$.items"
    pagination: {{ strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }}
    columns:
      - {{ name: created, path: created, type: timestamp_ms_utc, nullable: true }}
    filters:
{filter}
"#
            )
        };
        let err = parse_pack(&base(
            "      - { column: created, op: eq, input: at, fidelity: inexact, format: epoch_seconds }",
        ))
        .unwrap_err();
        assert!(
            err.contains("only") && err.contains("lower bounds"),
            "{err}"
        );

        // (The YAML op enum admits only eq/gt/gt_eq today, so eq is the one
        // declarable non-lower-bound operator; the validation still guards
        // any future upper-bound additions.)
        let err = parse_pack(&base(
            "      - { column: created, op: gt_eq, input: since, fidelity: exact, format: epoch_seconds }",
        ))
        .unwrap_err();
        assert!(err.contains("inexact"), "{err}");

        // The sound shape — Feishu's startTime — still loads.
        parse_pack(&base(
            "      - { column: created, op: gt_eq, input: since, fidelity: inexact, format: epoch_seconds_string }",
        ))
        .expect("lower-bound inexact epoch mapping is legal");
    }

    #[test]
    fn dotted_table_keys_are_rejected() {
        let err = parse_pack(
            r#"
kind: pack
pack: demo
version: 1
tables:
  demo.items:
    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
"#,
        )
        .unwrap_err();
        assert!(err.contains("bare name"), "{err}");
    }

    /// One minimal valid table, with a substitution point per invariant.
    fn pack_with(table_body: &str) -> Result<SourcePack, String> {
        parse_pack(&format!(
            r#"
kind: pack
pack: demo
version: 1
tables:
  items:
{table_body}
"#
        ))
    }

    #[test]
    fn semantic_invariants_are_rejected_with_targeted_errors() {
        // (yaml, expected fragment) — each row violates exactly one
        // cross-field invariant the engine assumes but never re-checks.
        for (body, expected) in [
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
      - { name: id, path: other, type: utf8, nullable: true }"#,
                "duplicate column 'id'",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
    filters:
      - { column: missing, op: eq, input: q, fidelity: inexact }"#,
                "undeclared column 'missing'",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
    filters:
      - { column: id, op: eq, input: a, fidelity: inexact }
      - { column: id, op: eq, input: b, fidelity: inexact }"#,
                "duplicate filter mapping",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    resources: { required: [owner], optional: [owner] }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "both required and optional",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    resources: { required: [owner] }
    fixed_inputs:
      owner: acme
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "collides with a declared resource",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    fixed_inputs:
      page: 1
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "collides with a pagination input",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
    filters:
      - { column: id, op: eq, input: perPage, fidelity: exact }"#,
                "collides with a pagination input",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    resources: { required: [owner] }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
    filters:
      - { column: id, op: eq, input: owner, fidelity: exact }"#,
                "collides with a declared resource",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
      - { name: score, path: score, type: uint64, nullable: true }
    filters:
      - { column: id, op: eq, input: q, fidelity: inexact }
      - { column: score, op: eq, input: q, fidelity: inexact }"#,
                "two filter mappings target input 'q'",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    fixed_inputs:
      threshold: .nan
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "no JSON spelling",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    fixed_inputs:
      threshold: .inf
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "no JSON spelling",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    fixed_inputs:
      threshold: -.inf
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "no JSON spelling",
            ),
            // NESTED non-finites, through the untagged Json variant. These
            // are the regression for the dead first_non_finite guard: a
            // `serde_json::Value` capture had already converted the nested
            // `.nan` to null before any check could run (serde_json's f64
            // visitor maps non-finite to Value::Null), so the pin silently
            // became `{"threshold": null}`. The YAML capture keeps the
            // non-finite observable and the conversion rejects it.
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    fixed_inputs:
      filter:
        threshold: .nan
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "no JSON spelling",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    fixed_inputs:
      filter:
        bounds: [1.5, .inf]
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "no JSON spelling",
            ),
            // The other two YAML shapes JSON cannot spell, same variant.
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    fixed_inputs:
      filter:
        1: numeric-key
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "non-string mapping key",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    fixed_inputs:
      filter:
        payload: !custom tagged
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                // Rejected before yaml_to_json ever runs: serde's untagged
                // buffering cannot represent a YAML tag, so deserialization
                // itself fails — the Tagged arm in yaml_to_json is defense
                // in depth behind this.
                "do not support enum input",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: page, page_size: 10 }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "both the page and page-size input",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 0 }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "page size must be positive",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: cursor, cursor_input: cursor, next_cursor_path: "$.next", page_size_input: cursor, page_size: 10 }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "both the cursor and page-size input",
            ),
            (
                r#"    action: demo.list
    row_path: "items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "must start with '$.'",
            ),
        ] {
            let err = pack_with(body).expect_err(expected);
            assert!(err.contains(expected), "want {expected:?} in: {err}");
        }
    }
}
