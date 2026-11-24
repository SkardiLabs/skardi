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

use std::collections::{BTreeMap, HashSet};
use std::sync::OnceLock;

use serde::Deserialize;

use crate::sources::providers::open_connector::error::OpenConnectorError;
use skardi_source_pack::derive::{
    Explode, RowDerivation, Unpivot, UnpivotField, template_placeholders,
};
// `Operator` here is the pack's neutral declaration, not DataFusion's.
// The loader reads YAML and writes declarations; nothing about that needs
// a query planner, and it used to convert to DataFusion's on the way in
// for no reason other than where the type happened to live.
use crate::sources::providers::open_connector::json_to_arrow::RowConverter;
use crate::sources::providers::open_connector::json_to_arrow::{
    ColumnDoc, FieldMapping, FieldType,
};
use crate::sources::providers::open_connector::pagination::{
    AbsentCursor, CursorContinuation, PaginationStrategy,
};
use crate::sources::providers::open_connector::row_path::RowPath;
use crate::sources::providers::open_connector::source_pack::{
    FixedValue, RowShape, SourcePack, SourcePackTable,
};
use skardi_source_pack::filters::{Fidelity, FilterMapping, Operator, ValueFormat};

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
        let fixed = convert_fixed_value(id, "fixed input", &key, value)?;
        fixed_inputs.push((leak_str(key), fixed));
    }
    let derive = match doc.derive {
        None => None,
        Some(derive) => {
            let mut constants = Vec::with_capacity(derive.constants.len());
            for (key, value) in derive.constants {
                let constant = convert_fixed_value(id, "derive constant", &key, value)?;
                constants.push((leak_str(key), constant));
            }
            Some(RowDerivation {
                skip_when_null: derive.skip_when_null.map(leak_str),
                explode: derive.explode.map(|explode| Explode {
                    paths: leak_str_slice(explode.paths),
                    into: leak_str(explode.into),
                    index: explode.index.map(leak_str),
                }),
                unpivot: derive.unpivot.map(|unpivot| Unpivot {
                    from: leak_slice(
                        unpivot
                            .from
                            .into_iter()
                            .map(|field| UnpivotField {
                                label: leak_str(field.label),
                                path: leak_str(field.path),
                            })
                            .collect(),
                    ),
                    into: leak_str(unpivot.into),
                    label: leak_str(unpivot.label),
                }),
                constants: leak_slice(constants),
                format: leak_slice(
                    derive
                        .format
                        .into_iter()
                        .map(|(key, template)| (leak_str(key), leak_str(template)))
                        .collect(),
                ),
            })
        }
    };
    let action_id = leak_str(doc.action);
    let (pagination, continuation) = doc.pagination.into_parts(action_id);
    let table = SourcePackTable {
        id,
        action_id,
        row_path: leak_str(doc.row_path),
        row_shape: doc.row_shape.into(),
        fields: leak_slice(fields),
        pagination,
        required_resources: leak_str_slice(doc.resources.required),
        optional_resources: leak_str_slice(doc.resources.optional),
        exclusive_resources: leak_str_groups(doc.resources.exclusive),
        fixed_inputs: leak_slice(fixed_inputs),
        filters: leak_slice(filters),
        error_path: doc.error_path.map(leak_str),
        expected_fingerprint: doc.fingerprint.map(leak_str),
        continuation,
        derive,
    };
    validate_table(&table)?;
    Ok(table)
}

/// One pinned literal — a fixed action input or a derive constant — from
/// its YAML spelling. `what` names the declaration in errors.
///
/// YAML happily parses `.nan` / `.inf` / `-.inf`, but JSON has no spelling
/// for them — FixedValue::to_json would silently send null at query time,
/// bypassing the startup diagnostic this loader promises. Finite-only,
/// enforced here. The nested case CANNOT be checked after the fact on a
/// `serde_json::Value`: serde_json's f64 visitor maps a non-finite to
/// `Value::Null` during deserialization
/// (`Number::from_f64(...).map_or(Value::Null, …)`), so the loss has
/// already happened by then — which is why the `Json` variant captures
/// `serde_yaml::Value` and converts here, where the non-finite is still
/// observable.
fn convert_fixed_value(
    id: &str,
    what: &str,
    key: &str,
    value: FixedValueDoc,
) -> Result<FixedValue, String> {
    Ok(match value {
        FixedValueDoc::Float(v) if !v.is_finite() => {
            return Err(format!(
                "{id}: {what} '{key}' contains {v}, which has no JSON spelling; pin finite \
                 numbers only"
            ));
        }
        FixedValueDoc::Bool(v) => FixedValue::Bool(v),
        FixedValueDoc::Int(v) => FixedValue::Int(v),
        FixedValueDoc::Float(v) => FixedValue::Float(v),
        FixedValueDoc::Str(v) => FixedValue::Str(leak_str(v)),
        FixedValueDoc::StrList(v) => FixedValue::StrList(leak_str_slice(v)),
        FixedValueDoc::Json(v) => {
            let json =
                yaml_to_json(v).map_err(|reason| format!("{id}: {what} '{key}' {reason}"))?;
            FixedValue::Json(Box::leak(Box::new(json)))
        }
    })
}

/// A row-relative dotted path as the converter and the derivation walk it:
/// object keys only, no `$` prefix, no empty segment. The same rule
/// `ColumnDoc::into_spec` applies to column paths, for the paths a
/// `derive:` block names.
fn check_row_relative_path(id: &str, what: &str, path: &str) -> Result<(), String> {
    if path.is_empty() {
        return Err(format!("{id}: {what} has an empty path"));
    }
    if path == "$" || path.starts_with("$.") {
        return Err(format!(
            "{id}: {what} path '{path}' must not start with '$' — derive paths are row-relative \
             object keys"
        ));
    }
    if path.split('.').any(str::is_empty) {
        return Err(format!("{id}: {what} path '{path}' has an empty segment"));
    }
    Ok(())
}

/// A key a derivation writes onto a row: one object key, never a path.
fn check_derived_key(id: &str, what: &str, key: &str) -> Result<(), String> {
    if key.trim().is_empty() {
        return Err(format!("{id}: {what} has an empty key"));
    }
    if key.contains('.') {
        return Err(format!(
            "{id}: {what} key '{key}' contains '.'; a derived key is one object key, not a path"
        ));
    }
    Ok(())
}

/// Whether the row-relative path `read` names `path` itself or a key below
/// it (`summary.steps.0` below `summary.steps`; `summary.stepsTaken` is a
/// sibling, not below).
fn reads_at_or_below(read: &str, path: &str) -> bool {
    read.strip_prefix(path)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('.'))
}

/// Whether the row-relative path `read` descends below the top-level key
/// `key` (`summary.id` below `summary`); reading `key` itself does not.
fn reads_below(read: &str, key: &str) -> bool {
    read.split_once('.')
        .is_some_and(|(parent, _)| parent == key)
}

/// Refuse a step's consumed paths when one is the same as another or lies
/// below it (`payload` and `payload.items`). No answer satisfies both — the
/// upper path must hold `holds` (an array, a scalar), the lower one needs it
/// to be the object it descends into — so the step could only ever read one
/// of them, and the other's drift would be consumed unseen.
fn check_disjoint_paths(id: &str, step: &str, holds: &str, paths: &[&str]) -> Result<(), String> {
    for (position, first) in paths.iter().enumerate() {
        for second in &paths[position + 1..] {
            if first == second {
                return Err(format!("{id}: {step} reads the path '{first}' twice"));
            }
            let (upper, lower) = if reads_at_or_below(second, first) {
                (first, second)
            } else if reads_at_or_below(first, second) {
                (second, first)
            } else {
                continue;
            };
            return Err(format!(
                "{id}: {step} reads '{lower}' below '{upper}'; no answer holds {holds} at \
                 '{upper}' and a value inside it"
            ));
        }
    }
    Ok(())
}

/// The `derive:` block's own invariants. Serde guarantees its shape; these
/// are the ways a derivation could be declared and do nothing, or do
/// something other than what its author read.
fn validate_derive(table: &SourcePackTable) -> Result<(), String> {
    let Some(derive) = &table.derive else {
        return Ok(());
    };
    let id = table.id;
    if derive.is_empty() {
        return Err(format!(
            "{id}: `derive` declares nothing; drop the block or declare one of skip_when_null, \
             explode, unpivot, constants, format"
        ));
    }
    if let Some(path) = derive.skip_when_null {
        check_row_relative_path(id, "derive skip_when_null", path)?;
    }
    if let Some(explode) = &derive.explode {
        if explode.paths.is_empty() {
            return Err(format!("{id}: derive explode names no `paths`"));
        }
        for path in explode.paths {
            check_row_relative_path(id, "derive explode", path)?;
        }
        check_disjoint_paths(id, "derive explode", "an array", explode.paths)?;
        check_derived_key(id, "derive explode `into`", explode.into)?;
        if let Some(index) = explode.index {
            check_derived_key(id, "derive explode `index`", index)?;
        }
        // A short or empty page is a termination signal under these two
        // strategies, read off the RAW page — which stays sound — but the
        // consumer's row count no longer tracks the wire's, so a page
        // budget sized for wire rows would be wrong. Refused rather than
        // documented: no table has needed it.
        if matches!(
            table.pagination,
            PaginationStrategy::PageNumber { .. } | PaginationStrategy::Keyset { .. }
        ) {
            return Err(format!(
                "{id}: derive explode is declared on a page_number/keyset table; explode is \
                 supported with the cursor and single_page strategies"
            ));
        }
    }
    if let Some(unpivot) = &derive.unpivot {
        // Two row-multiplying steps on one table would multiply each
        // other's rows, and the consumed paths of one could hide below the
        // other's; no table has needed both.
        if derive.explode.is_some() {
            return Err(format!(
                "{id}: derive declares both explode and unpivot; a table turns its rows one way"
            ));
        }
        if unpivot.from.is_empty() {
            return Err(format!("{id}: derive unpivot names no `from` fields"));
        }
        let mut labels = HashSet::new();
        let mut paths = HashSet::new();
        for field in unpivot.from {
            if field.label.trim().is_empty() {
                return Err(format!(
                    "{id}: derive unpivot field '{}' has an empty label",
                    field.path
                ));
            }
            if !labels.insert(field.label) {
                return Err(format!(
                    "{id}: derive unpivot names the label '{}' twice",
                    field.label
                ));
            }
            check_row_relative_path(id, "derive unpivot", field.path)?;
            if !paths.insert(field.path) {
                return Err(format!(
                    "{id}: derive unpivot reads the path '{}' twice",
                    field.path
                ));
            }
        }
        let from: Vec<&str> = unpivot.from.iter().map(|field| field.path).collect();
        check_disjoint_paths(id, "derive unpivot", "a scalar", &from)?;
        check_derived_key(id, "derive unpivot `into`", unpivot.into)?;
        check_derived_key(id, "derive unpivot `label`", unpivot.label)?;
        // The same reason explode is refused there: the consumer's row
        // count no longer tracks the wire's.
        if matches!(
            table.pagination,
            PaginationStrategy::PageNumber { .. } | PaginationStrategy::Keyset { .. }
        ) {
            return Err(format!(
                "{id}: derive unpivot is declared on a page_number/keyset table; unpivot is \
                 supported with the cursor and single_page strategies"
            ));
        }
    }
    for (key, _) in derive.constants {
        check_derived_key(id, "derive constant", key)?;
    }
    for (key, template) in derive.format {
        check_derived_key(id, "derive format", key)?;
        let placeholders = template_placeholders(template)
            .map_err(|reason| format!("{id}: derive format '{key}' template {reason}"))?;
        for path in placeholders {
            check_row_relative_path(id, &format!("derive format '{key}' placeholder"), path)?;
        }
    }
    // Explode consumes its paths: a derived row carries the element under
    // `into`, never the arrays it came from (see `Explode`). A column or a
    // template reading at or below an exploded path would read nothing on
    // every row, silently NULL on a nullable column, so it is refused here.
    if let Some(explode) = &derive.explode {
        for path in explode.paths {
            if let Some(field) = table
                .fields
                .iter()
                .find(|field| reads_at_or_below(field.path, path))
            {
                return Err(format!(
                    "{id}: column '{}' reads '{}', which derive explode consumes; a derived row \
                     carries the element under '{}', not the array",
                    field.name, field.path, explode.into
                ));
            }
            for (key, template) in derive.format {
                let placeholders = template_placeholders(template).unwrap_or_default();
                if let Some(read) = placeholders
                    .iter()
                    .find(|read| reads_at_or_below(read, path))
                {
                    return Err(format!(
                        "{id}: derive format '{key}' reads '{read}', which derive explode \
                         consumes; a derived row carries the element under '{}', not the array",
                        explode.into
                    ));
                }
            }
        }
    }
    // Unpivot consumes its paths the same way: a derived row carries the
    // field's value under `into`, never the field itself.
    if let Some(unpivot) = &derive.unpivot {
        for path in unpivot.from.iter().map(|field| field.path) {
            if let Some(field) = table
                .fields
                .iter()
                .find(|field| reads_at_or_below(field.path, path))
            {
                return Err(format!(
                    "{id}: column '{}' reads '{}', which derive unpivot consumes; a derived row \
                     carries the value under '{}', not the field",
                    field.name, field.path, unpivot.into
                ));
            }
            for (key, template) in derive.format {
                let placeholders = template_placeholders(template).unwrap_or_default();
                if let Some(read) = placeholders
                    .iter()
                    .find(|read| reads_at_or_below(read, path))
                {
                    return Err(format!(
                        "{id}: derive format '{key}' reads '{read}', which derive unpivot \
                         consumes; a derived row carries the value under '{}', not the field",
                        unpivot.into
                    ));
                }
            }
        }
    }
    // One key, one writer: a key two steps write would be whichever ran
    // last, and the order is an implementation fact, not a declaration.
    let written = derive.written_keys();
    let mut seen = HashSet::new();
    for key in &written {
        if !seen.insert(*key) {
            return Err(format!(
                "{id}: derive writes '{key}' twice (across explode, unpivot, constants and format)"
            ));
        }
    }
    // Templates render one after another in key order, each onto the row
    // the next one reads, so a template reading another template's key
    // would see the wire value or the rendered one depending on how the
    // two keys sort — the same order-as-implementation-fact problem as a
    // key with two writers. A template reading its OWN key is not refused:
    // it always reads the value from before format, whatever the order.
    for (key, template) in derive.format {
        let placeholders = template_placeholders(template).unwrap_or_default();
        for read in placeholders {
            let head = read.split('.').next().unwrap_or(read);
            if head != *key && derive.format.iter().any(|(other, _)| *other == head) {
                return Err(format!(
                    "{id}: derive format '{key}' reads '{read}', which derive format '{head}' \
                     writes; a template reads wire, explode and constant keys, never another \
                     template's key"
                ));
            }
        }
    }
    // A derived key nothing reads is a declaration with no effect — and
    // most likely a column path misspelt on one side or the other. A
    // template's read of its own key sees the pre-format value, never the
    // derived one, so it does not count as reading it.
    for key in &written {
        let read_by_a_column = table
            .fields
            .iter()
            .any(|field| field.path.split('.').next() == Some(*key));
        let read_by_a_template = derive
            .format
            .iter()
            .filter(|(template_key, _)| template_key != key)
            .any(|(_, template)| {
                template_placeholders(template)
                    .map(|paths| paths.iter().any(|p| p.split('.').next() == Some(*key)))
                    .unwrap_or(false)
            });
        if !read_by_a_column && !read_by_a_template {
            return Err(format!(
                "{id}: derive writes '{key}', which no column path and no format template reads"
            ));
        }
    }
    // A key written with a non-object value replaces whatever the wire had
    // there, so a path below it never reaches the wire object it names: on
    // a null it reads NULL on every row (failing a non-null column), on a
    // scalar the converter or the template fails every scan. Such a key may
    // only be read whole. Explode `into` is exempt: its value is an array
    // element whose shape only the wire knows, and reading fields of an
    // object element (`item.text`) is what `into` is for. A template below
    // its own key is exempt too: it renders before its key is written, so
    // it reads the wire object.
    let mut non_object_keys: Vec<(&str, &str)> = Vec::new();
    if let Some(index) = derive.explode.and_then(|explode| explode.index) {
        non_object_keys.push(("explode `index`", index));
    }
    // Unpivot yields a row only for a scalar value, and its label is text.
    if let Some(unpivot) = &derive.unpivot {
        non_object_keys.push(("unpivot `into`", unpivot.into));
        non_object_keys.push(("unpivot `label`", unpivot.label));
    }
    for (key, value) in derive.constants {
        if !matches!(value, FixedValue::Json(json) if json.is_object()) {
            non_object_keys.push(("constant", *key));
        }
    }
    for (key, _) in derive.format {
        non_object_keys.push(("format", *key));
    }
    for (what, key) in non_object_keys {
        if let Some(field) = table
            .fields
            .iter()
            .find(|field| reads_below(field.path, key))
        {
            return Err(format!(
                "{id}: column '{}' reads '{}' below derive {what} '{key}', which writes no \
                 object; a derived key is read whole (path '{key}'), never as a parent",
                field.name, field.path
            ));
        }
        for (format_key, template) in derive.format {
            if *format_key == key {
                continue;
            }
            let placeholders = template_placeholders(template).unwrap_or_default();
            if let Some(read) = placeholders.iter().find(|read| reads_below(read, key)) {
                return Err(format!(
                    "{id}: derive format '{format_key}' reads '{read}' below derive {what} \
                     '{key}', which writes no object; a derived key is read whole (path \
                     '{key}'), never as a parent"
                ));
            }
        }
    }
    Ok(())
}

/// Cross-field validation, run before the document becomes a runtime
/// object. Serde guarantees shape; these are the semantic invariants the
/// engine assumes but does not re-check per scan.
fn validate_table(table: &SourcePackTable) -> Result<(), String> {
    let id = table.id;
    validate_derive(table)?;
    // Structural pieces the engine parses lazily elsewhere fail HERE so a
    // generated asset gets one complete diagnostic pass.
    // Two shapes, two contracts. Array tables keep the strict path parser;
    // object tables get the root-only one, so `$` never becomes valid for a
    // table that is supposed to locate a row array.
    match table.row_shape {
        RowShape::Array => {
            RowPath::parse(table.row_path).map_err(|e| format!("{id}: {e}"))?;
        }
        RowShape::Object => {
            RowPath::parse_object_root(table.row_path).map_err(|e| format!("{id}: {e}"))?;
            // An object row IS the whole response, so there is nothing for a
            // second request to return. Any multi-page strategy here is a
            // declaration error that would otherwise re-fetch the same row
            // until a page limit stopped it.
            if !matches!(table.pagination, PaginationStrategy::SinglePage { .. }) {
                return Err(format!(
                    "{id}: row_shape 'object' requires pagination strategy 'single_page'"
                ));
            }
        }
    }
    if let Some(path) = table.error_path {
        RowPath::parse(path).map_err(|e| format!("{id}: {e}"))?;
    }
    table
        .pagination
        .validate()
        .map_err(|e| format!("{id}: {e}"))?;
    RowConverter::new(table.fields).map_err(|e| format!("{id}: {e}"))?;

    let mut columns = HashSet::new();
    for field in table.fields {
        if !columns.insert(field.name) {
            return Err(format!("{id}: duplicate column '{}'", field.name));
        }
    }
    let mut mappings = HashSet::new();
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
        // The gateway evaluates a pushed predicate against WIRE rows, but a
        // column the derivation writes has no wire value: its value exists
        // only after the fetch. Pushed, the predicate filters something else
        // (whole source rows for an exploded element) and, under exact,
        // DataFusion never re-checks it; under inexact the upstream subset
        // is still not a superset of the matching rows. Refused whatever the
        // fidelity. A column read off the source row is left alone: every
        // derived row inherits it unchanged, so filtering source rows before
        // explode or skip_when_null keeps exactly the rows that match —
        // unless explode removed something below the column's path, which
        // leaves the derived value different from the wire value.
        if let Some(derive) = &table.derive
            && let Some(field) = table.fields.iter().find(|f| f.name == filter.column)
        {
            if let Some(key) = derive
                .written_keys()
                .into_iter()
                .find(|key| field.path.split('.').next() == Some(*key))
            {
                return Err(format!(
                    "{id}: filter on '{}' ({:?}) reads '{}', which derive writes as '{key}' after \
                     the fetch; the gateway would evaluate the pushed predicate against wire rows \
                     that do not carry it",
                    filter.column, filter.operator, field.path
                ));
            }
            if let Some(explode) = &derive.explode
                && let Some(path) = explode
                    .paths
                    .iter()
                    .find(|path| reads_at_or_below(path, field.path))
            {
                return Err(format!(
                    "{id}: filter on '{}' ({:?}) reads '{}', from which derive explode removes \
                     '{path}'; the gateway would evaluate the pushed predicate against the wire \
                     value, which still carries it",
                    filter.column, filter.operator, field.path
                ));
            }
            if let Some(unpivot) = &derive.unpivot
                && let Some(path) = unpivot
                    .from
                    .iter()
                    .map(|field| field.path)
                    .find(|path| reads_at_or_below(path, field.path))
            {
                return Err(format!(
                    "{id}: filter on '{}' ({:?}) reads '{}', from which derive unpivot removes \
                     '{path}'; the gateway would evaluate the pushed predicate against the wire \
                     value, which still carries it",
                    filter.column, filter.operator, field.path
                ));
            }
        }
        // A value domain is a finite list of equalities; under a range
        // operator it has no meaning the translation could honor, so an
        // author who wrote one has misread what it gates. Blanks and
        // repeats are the same kind of slip — a list that says nothing.
        if !filter.values.is_empty() {
            if filter.operator != Operator::Eq {
                return Err(format!(
                    "{id}: filter on '{}' declares `values`, which only an eq mapping can \
                     honor — operator {:?} has no finite domain to list",
                    filter.column, filter.operator
                ));
            }
            let mut seen = HashSet::new();
            for value in filter.values {
                if value.is_empty() {
                    return Err(format!(
                        "{id}: filter on '{}' declares an empty string in `values`",
                        filter.column
                    ));
                }
                if !seen.insert(*value) {
                    return Err(format!(
                        "{id}: filter on '{}' lists '{value}' twice in `values`",
                        filter.column
                    ));
                }
            }
            // A domain is a list of strings and the translation admits only
            // a string literal into one, so on any other column every
            // literal would fall outside it and every predicate would
            // answer empty — a pushdown switched off (or, for a boolean
            // column, a spelling-dependent one) with nothing saying so.
            // The column exists: undeclared ones were refused above.
            if let Some(field) = table.fields.iter().find(|f| f.name == filter.column)
                && field.field_type != FieldType::Utf8
            {
                return Err(format!(
                    "{id}: filter on '{}' declares `values` on a column of type {}; a value \
                     domain is a list of strings and only a utf8 column can carry one",
                    filter.column,
                    field.field_type.label()
                ));
            }
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
    // Alternative groups. Each rule below is a way the declaration could
    // be self-defeating rather than protective: a group that cannot
    // conflict, one that names a key no request carries, one that puts a
    // key in two groups (making the reported pair arbitrary), and one
    // containing a REQUIRED key — that key is always present, so every
    // other member becomes unusable and the group silently forbids
    // configuration the pack still advertises.
    let mut grouped: Vec<&str> = Vec::new();
    for group in table.exclusive_resources {
        if group.len() < 2 {
            return Err(format!(
                "{id}: exclusive resource group {group:?} needs at least two members to be a \
                 choice"
            ));
        }
        for key in *group {
            if !table.declares_resource(key) {
                return Err(format!(
                    "{id}: exclusive resource group names '{key}', which the table does not \
                     declare as a resource"
                ));
            }
            if table.required_resources.contains(key) {
                return Err(format!(
                    "{id}: resource '{key}' is required, so it cannot be one of a group of \
                     alternatives — every other member would be unreachable"
                ));
            }
            if grouped.contains(key) {
                return Err(format!(
                    "{id}: resource '{key}' appears in more than one exclusive group"
                ));
            }
            grouped.push(key);
        }
    }
    // The request-input namespace is shared by resources, fixed inputs,
    // filter inputs, and pagination parameters; exec.rs applies pagination
    // LAST, so a collision would silently overwrite an Exact pushed
    // predicate DataFusion never reapplies. Filter-vs-fixed-input overlap
    // is the one DELIBERATE collision (a pushed predicate overrides the
    // complete-collection pin) and stays legal.
    //
    // The strategy -> params mapping lives on `SourcePackTable` so this
    // gate and the continuation input gates read the same list: this
    // function used to carry its own copy of the match, and the two had
    // already drifted once (`Keyset`/`single_page` arrived with the
    // Discord pack and only one side was widened).
    let pagination_params: Vec<&str> = table.pagination_input_keys();
    // An empty param name — under ANY strategy — would survive to scan
    // time as a literal `""` input key: a strict gateway's 400 blamed on
    // the request, or a lax gateway's page-1 refetch misdiagnosed as a
    // provider loop. The loader's promise is one complete diagnostic
    // pass at load, so the check covers every strategy here rather than
    // living in one arm below.
    if pagination_params.iter().any(|p| p.is_empty()) {
        return Err(format!("{id}: pagination input names must be non-empty"));
    }
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
        PaginationStrategy::Keyset {
            cursor_param,
            page_size_param,
            page_size,
            ..
        } => {
            // (row_cursor_field is validated by the strategy itself at
            // construction; empty input names are rejected above, for
            // every strategy.)
            if page_size_param == cursor_param {
                return Err(format!(
                    "{id}: pagination declares '{cursor_param}' as both the cursor and page-size input"
                ));
            }
            if page_size == 0 {
                return Err(format!("{id}: pagination page size must be positive"));
            }
        }
        PaginationStrategy::SinglePage { .. } => {}
    }
    // NOT rejected on purpose: a filter input equal to a FIXED input is
    // the override mechanism itself (a pushed predicate replacing the
    // complete-collection pin — the github state=all pattern, pinned
    // end-to-end by the pack tests). Two filters sharing an input are
    // rejected below even though the scan-time claimed-inputs guard makes
    // them safe (the loser stays local): which predicate pushes would
    // depend on query order, and that ambiguity is an authoring mistake.
    let mut filter_inputs = HashSet::new();
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
    // The fourth pair of the shared namespace: a pagination input naming a
    // declared RESOURCE. exec.rs builds the request from the resource
    // first and applies pagination LAST, so from page 2 onward the cursor
    // would silently overwrite the binding's resource value — the scan
    // walks a different collection than the binding asked for, with no
    // error anywhere. (`after` is an entirely plausible name for both.)
    for param in &pagination_params {
        if table.declares_resource(param) {
            return Err(format!(
                "{id}: pagination input '{param}' collides with a declared resource — \
                 pagination is applied last and would overwrite the binding's value \
                 from page 2 onward"
            ));
        }
    }
    if let Some(continuation) = table.continuation {
        // Half a gate is worse than an obvious hole: a table pinning its
        // continuation action while leaving its OWN action unpinned would
        // read as gated and verify only pages 2..N.
        if table.expected_fingerprint.is_none() {
            return Err(format!(
                "{id}: continuation declares a fingerprint but the table does not; pin \
                 the table's own action too or neither is gated"
            ));
        }
        if continuation.action_id == table.action_id && !continuation.cursor_only {
            // Same action, same inputs, same everything — the declaration
            // describes exactly the default behavior, so it is either a
            // no-op or (more likely) a missing `inputs: cursor_only`.
            return Err(format!(
                "{id}: continuation repeats the table's own action with the full input, \
                 which is the default; drop the block or declare `inputs: cursor_only`"
            ));
        }
        // One action, one discovered schema, one fingerprint. Two pins that
        // disagree cannot BOTH be satisfied by any gateway, so this is
        // unsatisfiable by construction rather than merely suspicious —
        // and left to runtime it surfaces as a contract-gate failure
        // pointing at the schema instead of at the two lines that cannot
        // both be right. (The `open_connector_query` path looks the
        // expectation up by action id and would silently check the first
        // of the two; refusing here is what makes that lookup sound.)
        if continuation.action_id == table.action_id
            && table.expected_fingerprint != Some(continuation.expected_fingerprint)
        {
            return Err(format!(
                "{id}: continuation names the table's own action '{}' but pins a different \
                 fingerprint ({} vs {}); one action has one contract, so no gateway can \
                 satisfy both",
                continuation.action_id,
                table.expected_fingerprint.unwrap_or("<none>"),
                continuation.expected_fingerprint
            ));
        }
        // `cursor_only` deliberately drops every non-cursor input on pages
        // 2..N. An `Exact` filter is mapped to
        // `TableProviderFilterPushDown::Exact`, which DELETES the `Filter`
        // node from the plan — so page one applies the predicate as an
        // action input and pages 2..N cannot, with no node left to
        // re-apply it. That returns rows the query excluded, silently:
        // the same failure class as a pushdown leak, and refused the same
        // way. `Inexact` stays legal — the `Filter` node survives, so
        // DataFusion re-applies the predicate and the ROWS are right. The
        // waste is not free, though: pages 2..N fetch the unfiltered
        // collection, and the scan bounds count what the gateway returned,
        // not what survived the filter (`exec.rs` raises
        // `ScanBoundsExceeded` off `rows_emitted`/page count pre-filter).
        // So a selective `Inexact` filter moves the effective volume from
        // the filtered subset to the whole collection, and wants
        // `max_rows`/`max_pages` headroom sized for it — otherwise a query
        // that works today becomes a hard scan failure, not a slower one.
        if continuation.cursor_only
            && let Some(filter) = table
                .filters
                .iter()
                .find(|f| matches!(f.fidelity, Fidelity::Exact))
        {
            return Err(format!(
                "{id}: filter on '{}' is Exact, which removes the Filter node from the \
                 plan, but `inputs: cursor_only` cannot carry the predicate past page one; \
                 declare the filter Inexact or drop `cursor_only`",
                filter.input_field
            ));
        }
    }
    Ok(())
}

fn convert_column(table_id: &str, doc: ColumnDoc) -> Result<FieldMapping, String> {
    // The grammar and its checks are the shared `ColumnDoc`'s (a webhook
    // route parses the same type); the pack only pins the spec's strings
    // into the `'static` mapping the engine keeps for the process lifetime.
    let spec = doc.into_spec(table_id)?;
    Ok(FieldMapping {
        name: leak_str(spec.name),
        path: leak_str(spec.path),
        field_type: spec.field_type,
        nullable: spec.nullable,
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
        values: leak_str_slice(doc.values),
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

fn leak_str_groups(v: Vec<Vec<String>>) -> &'static [&'static [&'static str]] {
    leak_slice(v.into_iter().map(leak_str_slice).collect())
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
    /// Response shape. Omitted means `array`, so every pack written before
    /// object rows existed keeps its exact behaviour.
    #[serde(default)]
    row_shape: RowShapeDoc,
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
    /// How the wire rows are rewritten before the columns read them; see
    /// `skardi_source_pack::derive`. Absent for every table whose columns
    /// map straight onto wire keys.
    #[serde(default)]
    derive: Option<DeriveDoc>,
}

/// The `derive:` block as written in YAML. Every part optional, every key
/// strict: a misspelt step must fail the asset, not silently leave a
/// contract column unwritten.
///
/// `explode` consumes its `paths`: a derived row carries each element under
/// `into` and none of the listed arrays, so no column path and no `format`
/// placeholder may read at or below one (`validate_derive` refuses it).
/// `unpivot` consumes its `from` paths the same way.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeriveDoc {
    #[serde(default)]
    skip_when_null: Option<String>,
    #[serde(default)]
    explode: Option<ExplodeDoc>,
    #[serde(default)]
    unpivot: Option<UnpivotDoc>,
    /// BTreeMap so the written keys (and so any collision error) are
    /// deterministic regardless of asset layout.
    #[serde(default)]
    constants: BTreeMap<String, FixedValueDoc>,
    #[serde(default)]
    format: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExplodeDoc {
    paths: Vec<String>,
    into: String,
    #[serde(default)]
    index: Option<String>,
}

/// `unpivot` consumes its `from` paths: a derived row carries the field's
/// value under `into` and its label under `label`, and none of the fields,
/// so no column path and no `format` placeholder may read at or below one.
/// `from` is a list, not a map, so the rows come out in the order written.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UnpivotDoc {
    from: Vec<UnpivotFieldDoc>,
    into: String,
    label: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct UnpivotFieldDoc {
    label: String,
    path: String,
}

#[derive(Deserialize, Default, Clone, Copy)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
enum RowShapeDoc {
    #[default]
    Array,
    Object,
}

impl From<RowShapeDoc> for RowShape {
    fn from(doc: RowShapeDoc) -> Self {
        match doc {
            RowShapeDoc::Array => Self::Array,
            RowShapeDoc::Object => Self::Object,
        }
    }
}

#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct ResourcesDoc {
    #[serde(default)]
    required: Vec<String>,
    #[serde(default)]
    optional: Vec<String>,
    /// Groups of declared resources that are ALTERNATIVES: a binding may
    /// set at most one member of each group. See
    /// `SourcePackTable::exclusive_resources`.
    #[serde(default)]
    exclusive: Vec<Vec<String>>,
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
        /// Split-action continuation. Nested under the cursor variant so a
        /// `page_number` table declaring one is a `deny_unknown_fields`
        /// parse error — the invariant costs no hand-written check.
        #[serde(default)]
        continuation: Option<ContinuationDoc>,
    },
    /// No pagination envelope: the next cursor is a field of the previous
    /// page's LAST ROW (Discord's `after`). Only an empty page terminates
    /// — `page_size` is a throughput knob, never a termination signal, so
    /// a provider that silently clamps it cannot truncate the scan.
    Keyset {
        cursor_input: String,
        row_cursor_field: String,
        page_size_input: String,
        page_size: u32,
    },
    /// One request is the complete collection: for actions whose API has
    /// no pagination at all (Discord's `/users/@me/connections`) or whose
    /// strict input schema declares no pagination keys (Gmail
    /// `list_labels` / `list_filters`), where injecting a page or cursor
    /// parameter would be rejected as `invalid_input`. The scan issues a
    /// single request and never advances. A braced variant (not a unit
    /// one) so `deny_unknown_fields` keeps rejecting stray keys — a
    /// misspelled pagination field must fail loudly, not ride along
    /// ignored.
    SinglePage {
        /// Optional: where this action would spell "there is more", if it
        /// has such a field. Declaring it turns the strategy's premise
        /// into a checked assertion — a live continuation fails the scan
        /// instead of returning a short answer as a complete one. Declare
        /// it whenever the action's envelope has such a field, even one
        /// the provider never populates today: that is precisely the
        /// tripwire for the day it starts.
        #[serde(default)]
        next_cursor_path: Option<String>,
    },
}

/// How pages 2..N are served, when not by repeating the table's action
/// with the full input.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContinuationDoc {
    /// Action serving pages 2..N; defaults to the table's own action, for
    /// providers that keep the action but reject its opening inputs
    /// alongside a cursor.
    #[serde(default)]
    action: Option<String>,
    /// Contract fingerprint of the continuation action — never optional:
    /// this action serves most of a long scan.
    fingerprint: String,
    #[serde(default)]
    inputs: ContinuationInputsDoc,
}

/// Which inputs a continuation request carries.
#[derive(Deserialize, Default)]
enum ContinuationInputsDoc {
    /// The full assembled input, as page one — the conservative default.
    #[serde(rename = "full")]
    #[default]
    Full,
    /// The cursor and nothing else (Dropbox's `*_continue` actions).
    #[serde(rename = "cursor_only")]
    CursorOnly,
}

impl PaginationDoc {
    /// Split into the engine's strategy and the table-level continuation
    /// decl. `table_action` supplies the continuation's default action.
    fn into_parts(
        self,
        table_action: &'static str,
    ) -> (PaginationStrategy, Option<CursorContinuation>) {
        match self {
            Self::PageNumber {
                page_input,
                page_size_input,
                page_size,
                total_pages_path,
                raw_page_size_path,
            } => (
                PaginationStrategy::PageNumber {
                    page_param: leak_str(page_input),
                    per_page_param: leak_str(page_size_input),
                    per_page: page_size,
                    total_pages_path: total_pages_path.map(leak_str),
                    raw_page_size_path: raw_page_size_path.map(leak_str),
                },
                None,
            ),
            Self::Cursor {
                cursor_input,
                next_cursor_path,
                page_size_input,
                page_size,
                has_more_path,
                continuation,
            } => (
                PaginationStrategy::Cursor {
                    cursor_param: leak_str(cursor_input),
                    next_cursor_path: leak_str(next_cursor_path),
                    page_size_param: page_size_input.map(leak_str),
                    page_size,
                    has_more_path: has_more_path.map(leak_str),
                    // Every YAML-declared pack keeps the behaviour it has
                    // today. `AbsentCursor::IsDrift` is a MEASUREMENT — that
                    // the action sends its cursor key on every page and
                    // spells the end as null — and no pack asset carries
                    // that measurement yet, so there is no YAML key to read
                    // it from. One goes in when a provider's terminal page
                    // has actually been observed, not before.
                    absent_cursor: AbsentCursor::EndsTheScan,
                },
                continuation.map(|doc| CursorContinuation {
                    action_id: doc.action.map_or(table_action, leak_str),
                    expected_fingerprint: leak_str(doc.fingerprint),
                    cursor_only: matches!(doc.inputs, ContinuationInputsDoc::CursorOnly),
                }),
            ),
            Self::Keyset {
                cursor_input,
                row_cursor_field,
                page_size_input,
                page_size,
            } => (
                PaginationStrategy::Keyset {
                    cursor_param: leak_str(cursor_input),
                    row_cursor_field: leak_str(row_cursor_field),
                    page_size_param: leak_str(page_size_input),
                    page_size,
                },
                None,
            ),
            Self::SinglePage { next_cursor_path } => (
                PaginationStrategy::SinglePage {
                    next_cursor_path: next_cursor_path.map(leak_str),
                },
                None,
            ),
        }
    }
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
    /// The literals an `eq` mapping may forward; absent means any. See
    /// [`FilterMapping::values`] for why a closed provider enum needs it.
    #[serde(default)]
    values: Vec<String>,
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
    use rstest::rstest;

    use super::*;

    /// Every registered asset parses AND passes the same structural
    /// checks binding performs. Driven off `crate::sources::providers::open_connector::builtin_pack_registry()`
    /// rather than a hand-listed roster: the roster construct had already
    /// drifted for a full milestone (`discord.yaml` shipped in 5.6
    /// unlisted), and whatever registration sees, this test now sees —
    /// an asset cannot be shipped and skipped. WHAT is registered is the
    /// registry name-list test's pin, not this one's.
    #[test]
    fn builtin_assets_parse_and_validate() {
        let registry = crate::sources::providers::open_connector::builtin_pack_registry()
            .expect("every embedded asset parses and validates");
        for pack in registry.packs() {
            // parse_pack rejects an empty `tables` today; this stays as
            // the second layer should that rejection ever move.
            assert!(!pack.tables.is_empty(), "{}: no tables", pack.name);
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
    fn row_shape_defaults_to_array_for_every_existing_pack() {
        // The zero-behaviour-change invariant: a pack that says nothing
        // about shape must load exactly as it did before object rows.
        let pack = parse_pack(
            r#"
kind: pack
pack: demo
version: 1
tables:
  items:
    action: demo.list
    row_path: "$.items"
    pagination: { strategy: single_page }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
"#,
        )
        .expect("a pack without row_shape still parses");
        assert_eq!(pack.tables[0].row_shape, RowShape::Array);
    }

    #[test]
    fn object_row_shape_parses_with_root_path_and_single_page() {
        let pack = parse_pack(
            r#"
kind: pack
pack: demo
version: 1
tables:
  document_content:
    action: demo.get_document_content
    row_path: "$"
    row_shape: object
    pagination: { strategy: single_page }
    columns:
      - { name: content, path: content, type: utf8, nullable: true }
"#,
        )
        .expect("object rows at the root with single_page are valid");
        assert_eq!(pack.tables[0].row_shape, RowShape::Object);
        assert_eq!(pack.tables[0].row_path, "$");
    }

    #[test]
    fn object_row_shape_requires_single_page() {
        // An object row IS the whole response: a second page could only
        // re-fetch the same row.
        let err = parse_pack(
            r#"
kind: pack
pack: demo
version: 1
tables:
  document_content:
    action: demo.get_document_content
    row_path: "$"
    row_shape: object
    pagination:
      strategy: cursor
      cursor_input: pageToken
      next_cursor_path: "$.pageToken"
      page_size_input: pageSize
      page_size: 100
    columns:
      - { name: content, path: content, type: utf8, nullable: true }
"#,
        )
        .unwrap_err();
        assert!(err.contains("single_page"), "{err}");
    }

    #[test]
    fn object_row_shape_rejects_a_non_root_path() {
        let err = parse_pack(
            r#"
kind: pack
pack: demo
version: 1
tables:
  document_content:
    action: demo.get_document_content
    row_path: "$.document"
    row_shape: object
    pagination: { strategy: single_page }
    columns:
      - { name: content, path: content, type: utf8, nullable: true }
"#,
        )
        .unwrap_err();
        assert!(err.contains("'$'"), "{err}");
    }

    #[test]
    fn array_row_shape_still_rejects_the_root_path() {
        // The array contract is unchanged: `$` remains invalid there.
        let err = parse_pack(
            r#"
kind: pack
pack: demo
version: 1
tables:
  items:
    action: demo.list
    row_path: "$"
    pagination: { strategy: single_page }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
"#,
        )
        .unwrap_err();
        assert!(err.contains("row path"), "{err}");
    }

    #[test]
    fn single_page_strategy_parses_and_rejects_stray_keys() {
        // The pass side: an action with no pagination inputs at all
        // (Gmail list_labels-style) declares `single_page` and converts
        // to the engine's SinglePage strategy.
        let pack = parse_pack(
            r#"
kind: pack
pack: demo
version: 1
tables:
  items:
    action: demo.list
    row_path: "$.items"
    pagination: { strategy: single_page }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
"#,
        )
        .expect("single_page is a valid strategy");
        assert!(matches!(
            pack.tables[0].pagination,
            PaginationStrategy::SinglePage {
                next_cursor_path: None
            }
        ));

        // Its one optional parameter: the premise check. Declaring it does
        // not make the strategy paginate — it makes a live continuation
        // fail the scan instead of truncating it silently.
        let pack = parse_pack(
            r#"
kind: pack
pack: demo
version: 1
tables:
  items:
    action: demo.list
    row_path: "$.items"
    pagination: { strategy: single_page, next_cursor_path: "$.nextPageToken" }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
"#,
        )
        .expect("single_page accepts an optional next_cursor_path");
        assert!(matches!(
            pack.tables[0].pagination,
            PaginationStrategy::SinglePage {
                next_cursor_path: Some("$.nextPageToken")
            }
        ));

        // The refusal side: single_page takes no OTHER parameters, and a
        // stray key (a page size someone expected to matter) fails loudly
        // instead of riding along ignored.
        let err = parse_pack(
            r#"
kind: pack
pack: demo
version: 1
tables:
  items:
    action: demo.list
    row_path: "$.items"
    pagination: { strategy: single_page, page_size: 10 }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
"#,
        )
        .unwrap_err();
        assert!(err.contains("page_size"), "{err}");
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
    fn a_values_domain_parses_on_eq_and_is_refused_where_it_means_nothing() {
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
      - {{ name: state, path: state, type: utf8, nullable: false }}
      - {{ name: created, path: created, type: timestamp_ms_utc, nullable: true }}
      - {{ name: done, path: done, type: boolean, nullable: true }}
      - {{ name: count, path: count, type: int64, nullable: true }}
    filters:
{filter}
"#
            )
        };
        // The github shape round-trips, in declaration order.
        let pack = parse_pack(&base(
            "      - { column: state, op: eq, input: state, fidelity: inexact, values: [open, closed] }",
        ))
        .expect("an eq domain is legal");
        let filter = &pack.tables[0].filters[0];
        assert_eq!(filter.values, &["open", "closed"]);
        // Omitted means unconstrained — the shape every other pack has.
        let pack = parse_pack(&base(
            "      - { column: state, op: eq, input: state, fidelity: inexact }",
        ))
        .expect("no domain is legal");
        assert!(pack.tables[0].filters[0].values.is_empty());

        for (filter, expected) in [
            // A range has no finite domain to list.
            (
                "      - { column: created, op: gt_eq, input: since, fidelity: inexact, format: rfc3339, values: [x] }",
                "only an eq mapping",
            ),
            // A list that says nothing is a slip, not a declaration.
            (
                "      - { column: state, op: eq, input: state, fidelity: inexact, values: [open, open] }",
                "twice",
            ),
            (
                "      - { column: state, op: eq, input: state, fidelity: inexact, values: [open, ''] }",
                "empty string",
            ),
            // A domain is a list of strings; on any other column every
            // literal would be outside it.
            (
                "      - { column: done, op: eq, input: done, fidelity: inexact, values: [yes] }",
                "column of type boolean",
            ),
            (
                "      - { column: count, op: eq, input: count, fidelity: inexact, values: ['1'] }",
                "column of type integer",
            ),
        ] {
            let err = parse_pack(&base(filter)).expect_err(expected);
            assert!(
                err.contains(expected) && err.contains("demo.items"),
                "want {expected:?} naming the table in: {err}"
            );
        }
    }

    #[test]
    fn keyset_and_single_page_pagination_parse_and_validate() {
        let base = |pagination: &str| {
            format!(
                r#"
kind: pack
pack: demo
version: 1
tables:
  items:
    action: demo.list
    row_path: "$.items"
    pagination: {pagination}
    columns:
      - {{ name: id, path: id, type: utf8, nullable: false }}
"#
            )
        };
        // The two new spellings load…
        let pack = parse_pack(&base(
            "{ strategy: keyset, cursor_input: after, row_cursor_field: id, page_size_input: limit, page_size: 200 }",
        ))
        .expect("keyset parses");
        assert!(matches!(
            pack.tables[0].pagination,
            PaginationStrategy::Keyset { page_size: 200, .. }
        ));
        let pack = parse_pack(&base("{ strategy: single_page }")).expect("single_page parses");
        assert!(matches!(
            pack.tables[0].pagination,
            PaginationStrategy::SinglePage { .. }
        ));

        // …and keyset's authoring mistakes fail at load.
        let err = parse_pack(&base(
            "{ strategy: keyset, cursor_input: limit, row_cursor_field: id, page_size_input: limit, page_size: 200 }",
        ))
        .unwrap_err();
        assert!(err.contains("both the cursor and page-size input"), "{err}");
        let err = parse_pack(&base(
            "{ strategy: keyset, cursor_input: after, row_cursor_field: id, page_size_input: limit, page_size: 0 }",
        ))
        .unwrap_err();
        assert!(err.contains("page size must be positive"), "{err}");
        let err = parse_pack(&base(
            "{ strategy: keyset, cursor_input: after, row_cursor_field: \"$.id\", page_size_input: limit, page_size: 200 }",
        ))
        .unwrap_err();
        assert!(err.contains("relative to the row"), "{err}");
        // An empty input name would reach the wire as a literal "" key —
        // rejected at load, not misdiagnosed at scan time. The check is
        // strategy-agnostic: every arm that names inputs is covered.
        for pagination in [
            "{ strategy: keyset, cursor_input: \"\", row_cursor_field: id, page_size_input: limit, page_size: 200 }",
            "{ strategy: keyset, cursor_input: after, row_cursor_field: id, page_size_input: \"\", page_size: 200 }",
            "{ strategy: cursor, cursor_input: \"\", next_cursor_path: \"$.next\", page_size: 100 }",
            "{ strategy: cursor, cursor_input: cursor, next_cursor_path: \"$.next\", page_size_input: \"\", page_size: 100 }",
            "{ strategy: page_number, page_input: \"\", page_size_input: perPage, page_size: 100 }",
            "{ strategy: page_number, page_input: page, page_size_input: \"\", page_size: 100 }",
        ] {
            let err = parse_pack(&base(pagination)).unwrap_err();
            assert!(err.contains("must be non-empty"), "{err}");
        }
    }

    #[test]
    fn a_valid_exclusive_group_survives_the_round_trip() {
        // The positive arm: the four rejections above are only protective
        // if the shape they guard actually loads and reaches the runtime
        // object registration reads.
        let pack = pack_with(
            r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    resources: { optional: [byId, byPath], exclusive: [[byId, byPath]] }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
        )
        .expect("two declared optional resources are a valid group");
        let table = &pack.tables[0];
        let groups: Vec<Vec<&str>> = table
            .exclusive_resources
            .iter()
            .map(|group| group.to_vec())
            .collect();
        assert_eq!(groups, vec![vec!["byId", "byPath"]]);
        assert_eq!(
            table.conflicting_resources(|key| ["byId", "byPath"].contains(&key)),
            Some(("byId", "byPath")),
            "both supplied is the ambiguity"
        );
        for one in ["byId", "byPath"] {
            assert_eq!(
                table.conflicting_resources(|key| key == one),
                None,
                "{one} alone names one scope"
            );
        }
        assert_eq!(
            table.conflicting_resources(|_| false),
            None,
            "neither supplied is the unscoped default"
        );
    }

    #[test]
    fn tables_without_an_exclusive_group_declare_none() {
        // The field is additive: every other pack omits it, and a table
        // that does must never report a conflict.
        let pack = pack_with(
            r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    resources: { optional: [byId, byPath] }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
        )
        .expect("omitting the group stays valid");
        assert!(pack.tables[0].exclusive_resources.is_empty());
        assert_eq!(pack.tables[0].conflicting_resources(|_| true), None);
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
            // Four ways an `exclusive` group defeats itself instead of
            // protecting anything. All four would otherwise load as a
            // check that never fires, or one that forbids configuration
            // the pack still advertises.
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    resources: { optional: [byId], exclusive: [[byId]] }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "at least two members",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    resources: { optional: [byId], exclusive: [[byId, byPath]] }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "does not declare as a resource",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    resources: { required: [byId], optional: [byPath], exclusive: [[byId, byPath]] }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "every other member would be unreachable",
            ),
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    resources: { optional: [byId, byPath, byName], exclusive: [[byId, byPath], [byPath, byName]] }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "more than one exclusive group",
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
                // The keyset shape the review named: `after` as BOTH a
                // required resource and the cursor input — pagination is
                // applied last, so page 2+ would silently overwrite the
                // binding's value and walk a different collection.
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: keyset, cursor_input: after, page_size_input: limit, page_size: 10, row_cursor_field: id }
    resources: { required: [after] }
    columns:
      - { name: id, path: id, type: utf8, nullable: false }"#,
                "pagination input 'after' collides with a declared resource",
            ),
            (
                // Same rule under page_number (page_size_input vs an
                // optional resource) — the check covers every strategy.
                r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    resources: { optional: [perPage] }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "pagination input 'perPage' collides with a declared resource",
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

    /// A table that declares no `derive` loads with `None`, which is what
    /// keeps every pre-existing pack's scan byte-for-byte unchanged.
    #[test]
    fn derive_is_absent_unless_declared() {
        let pack = pack_with(
            r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: single_page }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
        )
        .expect("a pack without derive still parses");
        assert!(pack.tables[0].derive.is_none());
    }

    #[test]
    fn derive_parses_every_step_into_the_runtime_declaration() {
        let pack = pack_with(
            r#"    action: demo.get
    row_path: "$"
    row_shape: object
    pagination: { strategy: single_page }
    resources: { required: [id] }
    derive:
      skip_when_null: summary
      explode: { paths: [summary.edited.steps, summary.steps], into: text, index: position }
      constants: { provider: zoom, owner_name: null, rank: 2 }
      format: { item_id: "{summary.id}#{position}" }
    columns:
      - { name: provider, path: provider, type: utf8, nullable: false }
      - { name: item_id, path: item_id, type: utf8, nullable: false }
      - { name: text, path: text, type: utf8, nullable: true }
      - { name: owner_name, path: owner_name, type: utf8, nullable: true }
      - { name: rank, path: rank, type: int64, nullable: true }"#,
        )
        .expect("a full derive block parses");
        let derive = pack.tables[0].derive.expect("declared");
        assert_eq!(derive.skip_when_null, Some("summary"));
        let explode = derive.explode.expect("explode");
        assert_eq!(explode.paths, ["summary.edited.steps", "summary.steps"]);
        assert_eq!(explode.into, "text");
        assert_eq!(explode.index, Some("position"));
        // BTreeMap order: the asset's layout cannot reorder the constants.
        assert_eq!(
            derive.constants,
            [
                ("owner_name", FixedValue::Json(&serde_json::Value::Null)),
                ("provider", FixedValue::Str("zoom")),
                ("rank", FixedValue::Int(2)),
            ]
        );
        assert_eq!(derive.format, [("item_id", "{summary.id}#{position}")]);
        // The derived rows then convert through the ordinary converter.
        let page = serde_json::json!({
            "summary": {"id": "m-1", "steps": ["a", "b"], "edited": null}
        });
        let rows = derive
            .apply(pack.tables[0].id, std::slice::from_ref(&page), 1)
            .expect("derives");
        let batch = RowConverter::new(pack.tables[0].fields)
            .expect("converter")
            .convert(&rows, 1)
            .expect("converts");
        assert_eq!(batch.num_rows(), 2);
    }

    #[test]
    fn derive_invariants_are_rejected_with_targeted_errors() {
        const HEAD: &str = r#"    action: demo.get
    row_path: "$"
    row_shape: object
    pagination: { strategy: single_page }
    columns:
      - { name: id, path: summary.id, type: utf8, nullable: false }
      - { name: text, path: text, type: utf8, nullable: true }
      - { name: provider, path: provider, type: utf8, nullable: true }
"#;
        for (derive, expected) in [
            ("    derive: {}", "declares nothing"),
            (
                "    derive: { skip_when_null: \"$.summary\" }",
                "must not start with '$'",
            ),
            (
                "    derive: { explode: { paths: [], into: text } }",
                "names no `paths`",
            ),
            (
                "    derive: { explode: { paths: [summary..steps], into: text } }",
                "empty segment",
            ),
            (
                "    derive: { explode: { paths: [summary.steps], into: a.b } }",
                "contains '.'",
            ),
            (
                "    derive: { constants: { provider: zoom, text: x }, explode: { paths: [summary.steps], into: text } }",
                "writes 'text' twice",
            ),
            (
                "    derive: { constants: { unread: zoom } }",
                "writes 'unread', which no column path and no format template reads",
            ),
            // Explode consumes its paths, the chosen array and every
            // fallback: nothing may read one afterwards.
            (
                "    derive: { explode: { paths: [summary.id], into: text } }",
                "column 'id' reads 'summary.id', which derive explode consumes",
            ),
            (
                "    derive: { explode: { paths: [steps, summary], into: text } }",
                "column 'id' reads 'summary.id', which derive explode consumes",
            ),
            (
                "    derive: { explode: { paths: [summary.steps], into: text }, format: { provider: \"{summary.steps}\" } }",
                "derive format 'provider' reads 'summary.steps', which derive explode consumes",
            ),
            (
                "    derive: { format: { provider: \"{summary.id\" } }",
                "no matching '}'",
            ),
            (
                "    derive: { format: { provider: \"fixed\" } }",
                "no placeholder",
            ),
            (
                "    derive: { constants: { provider: .nan } }",
                "no JSON spelling",
            ),
            (
                "    derive: { constants: { provider: zoom }, typo: 1 }",
                "typo",
            ),
        ] {
            let err = pack_with(&format!("{HEAD}{derive}")).expect_err(expected);
            assert!(err.contains(expected), "want {expected:?} in: {err}");
        }
        // Explode is refused where a short page is a termination signal.
        let err = pack_with(
            r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    derive: { explode: { paths: [steps], into: text } }
    columns:
      - { name: text, path: text, type: utf8, nullable: true }"#,
        )
        .expect_err("explode on page_number");
        assert!(err.contains("page_number/keyset"), "{err}");
        // ...and a format placeholder may read a derived key (the exploded
        // position), which is the whole point of the fixed step order.
        pack_with(
            r#"    action: demo.get
    row_path: "$"
    row_shape: object
    pagination: { strategy: single_page }
    derive:
      explode: { paths: [steps], into: text, index: position }
      format: { item_id: "{id}#{position}" }
    columns:
      - { name: item_id, path: item_id, type: utf8, nullable: false }
      - { name: text, path: text, type: utf8, nullable: true }"#,
        )
        .expect("a template reading the exploded index is legal");
    }

    /// An object-row table turning two sibling tokens into one row each,
    /// the Feishu meeting-notes shape.
    const UNPIVOT_TABLE: &str = r#"    action: demo.get
    row_path: "$"
    row_shape: object
    pagination: { strategy: single_page }
    resources: { required: [id] }
    derive:
      unpivot:
        into: token
        label: kind
        from:
          - { label: notes, path: meeting.artifacts.notes }
          - { label: transcript, path: meeting.artifacts.verbatim }
      constants: { provider: demo }
      format: { note_key: "{meeting.id}#{kind}" }
    columns:
      - { name: provider, path: provider, type: utf8, nullable: false }
      - { name: meeting_id, path: meeting.id, type: utf8, nullable: false }
      - { name: note_id, path: token, type: utf8, nullable: false }
      - { name: kind, path: kind, type: utf8, nullable: true }
      - { name: note_key, path: note_key, type: utf8, nullable: true }"#;

    #[test]
    fn derive_parses_unpivot_in_the_written_order() {
        let pack = pack_with(UNPIVOT_TABLE).expect("an unpivot block parses");
        let derive = pack.tables[0].derive.expect("declared");
        assert!(derive.explode.is_none());
        let unpivot = derive.unpivot.expect("unpivot");
        assert_eq!(unpivot.into, "token");
        assert_eq!(unpivot.label, "kind");
        assert_eq!(
            unpivot.from,
            [
                UnpivotField {
                    label: "notes",
                    path: "meeting.artifacts.notes"
                },
                UnpivotField {
                    label: "transcript",
                    path: "meeting.artifacts.verbatim"
                },
            ]
        );
        // The derived rows convert through the ordinary converter, the
        // empty token yielding no row.
        let page = serde_json::json!({
            "meeting": {"id": "m-1", "artifacts": {"notes": "doc-a", "verbatim": ""}}
        });
        let rows = derive
            .apply(pack.tables[0].id, std::slice::from_ref(&page), 1)
            .expect("derives");
        let batch = RowConverter::new(pack.tables[0].fields)
            .expect("converter")
            .convert(&rows, 1)
            .expect("converts");
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(rows[0]["note_key"], "m-1#notes");
    }

    /// An exploded null element is a row at its own position; the column
    /// that reads it decides what it means: NULL for a nullable one, a
    /// failed conversion for a non-null one. (Pinned through a pack first;
    /// held here so it does not depend on any one pack.)
    #[test]
    fn an_exploded_null_element_converts_by_its_columns_nullability() {
        use arrow::array::{Array, StringArray};

        const TABLE: &str = r#"    action: demo.get
    row_path: "$"
    row_shape: object
    pagination: { strategy: single_page }
    derive:
      explode: { paths: [summary.steps], into: text, index: position }
      format: { item_id: "{summary.id}#{position}" }
    columns:
      - { name: item_id, path: item_id, type: utf8, nullable: false }
      - { name: text, path: text, type: utf8, nullable: NULLABLE }"#;
        let page = serde_json::json!({
            "summary": {"id": "m-1", "steps": ["Send the agenda", null, "Book the room"]}
        });

        let pack = pack_with(&TABLE.replace("NULLABLE", "true")).expect("parses");
        let table = &pack.tables[0];
        let rows = table
            .derive
            .expect("declared")
            .apply(table.id, std::slice::from_ref(&page), 1)
            .expect("derives");
        let batch = RowConverter::new(table.fields)
            .expect("converter")
            .convert(&rows, 1)
            .expect("a nullable column reads the null element as NULL");
        assert_eq!(batch.num_rows(), 3);
        let text = batch
            .column_by_name("text")
            .expect("text")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert!(text.is_null(1));
        assert_eq!(text.value(2), "Book the room");
        let ids = batch
            .column_by_name("item_id")
            .expect("item_id")
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("utf8");
        assert_eq!(ids.value(1), "m-1#1", "the null keeps its own position");
        assert_eq!(ids.value(2), "m-1#2", "positions after it do not move");

        let pack = pack_with(&TABLE.replace("NULLABLE", "false")).expect("parses");
        let table = &pack.tables[0];
        let rows = table
            .derive
            .expect("declared")
            .apply(table.id, std::slice::from_ref(&page), 1)
            .expect("derives");
        let err = RowConverter::new(table.fields)
            .expect("converter")
            .convert(&rows, 1)
            .expect_err("a non-null column refuses the null element");
        assert!(
            matches!(
                err,
                OpenConnectorError::ConversionFailed { ref column, row: 1, .. } if column == "text"
            ),
            "{err}"
        );
    }

    #[test]
    fn unpivot_invariants_are_rejected_with_targeted_errors() {
        const HEAD: &str = r#"    action: demo.get
    row_path: "$"
    row_shape: object
    pagination: { strategy: single_page }
    columns:
      - { name: id, path: meeting.id, type: utf8, nullable: false }
      - { name: token, path: token, type: utf8, nullable: true }
      - { name: kind, path: kind, type: utf8, nullable: true }
"#;
        for (derive, expected) in [
            (
                "    derive: { unpivot: { into: token, label: kind, from: [] } }",
                "names no `from` fields",
            ),
            (
                "    derive: { unpivot: { into: token, label: kind, from: [{ label: a, path: x }, { label: a, path: y }] } }",
                "names the label 'a' twice",
            ),
            (
                "    derive: { unpivot: { into: token, label: kind, from: [{ label: a, path: x }, { label: b, path: x }] } }",
                "reads the path 'x' twice",
            ),
            (
                "    derive: { unpivot: { into: token, label: kind, from: [{ label: \" \", path: x }] } }",
                "has an empty label",
            ),
            (
                "    derive: { unpivot: { into: token, label: kind, from: [{ label: a, path: \"$.x\" }] } }",
                "must not start with '$'",
            ),
            (
                "    derive: { unpivot: { into: a.b, label: kind, from: [{ label: a, path: x }] } }",
                "contains '.'",
            ),
            (
                "    derive: { unpivot: { into: token, label: kind, from: [{ label: a, path: x }] }, explode: { paths: [steps], into: token } }",
                "both explode and unpivot",
            ),
            (
                "    derive: { unpivot: { into: token, label: token, from: [{ label: a, path: x }] } }",
                "writes 'token' twice",
            ),
            // Unpivot consumes its fields: nothing may read one afterwards.
            (
                "    derive: { unpivot: { into: token, label: kind, from: [{ label: a, path: meeting.id }] } }",
                "column 'id' reads 'meeting.id', which derive unpivot consumes",
            ),
            (
                "    derive: { unpivot: { into: token, label: kind, from: [{ label: a, path: x }] }, format: { other: \"{x}\" } }",
                "derive format 'other' reads 'x', which derive unpivot consumes",
            ),
            (
                "    derive: { unpivot: { into: token, label: kind, from: [{ label: a, path: x }], typo: 1 } }",
                "typo",
            ),
        ] {
            let err = pack_with(&format!("{HEAD}{derive}")).expect_err(expected);
            assert!(err.contains(expected), "want {expected:?} in: {err}");
        }
        // A derived key unpivot writes is read whole, never as a parent.
        let err = pack_with(
            r#"    action: demo.get
    row_path: "$"
    row_shape: object
    pagination: { strategy: single_page }
    derive: { unpivot: { into: token, label: kind, from: [{ label: a, path: x }] } }
    columns:
      - { name: token, path: token.id, type: utf8, nullable: true }
      - { name: kind, path: kind, type: utf8, nullable: true }"#,
        )
        .expect_err("a column below unpivot `into`");
        assert!(err.contains("below derive unpivot `into` 'token'"), "{err}");
        // Unpivot is refused where a short page is a termination signal.
        let err = pack_with(
            r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: page_number, page_input: page, page_size_input: perPage, page_size: 10 }
    derive: { unpivot: { into: token, label: kind, from: [{ label: a, path: x }] } }
    columns:
      - { name: token, path: token, type: utf8, nullable: true }
      - { name: kind, path: kind, type: utf8, nullable: true }"#,
        )
        .expect_err("unpivot on page_number");
        assert!(err.contains("page_number/keyset"), "{err}");
    }

    #[test]
    fn consumed_paths_at_or_below_one_another_are_refused() {
        // Explode: an array cannot also be the object holding another path.
        const EXPLODE_HEAD: &str = r#"    action: demo.get
    row_path: "$"
    row_shape: object
    pagination: { strategy: single_page }
    columns:
      - { name: id, path: summary.id, type: utf8, nullable: false }
      - { name: text, path: text, type: utf8, nullable: true }
"#;
        // Unpivot: a scalar cannot either.
        const UNPIVOT_HEAD: &str = r#"    action: demo.get
    row_path: "$"
    row_shape: object
    pagination: { strategy: single_page }
    columns:
      - { name: id, path: meeting.id, type: utf8, nullable: false }
      - { name: token, path: token, type: utf8, nullable: true }
      - { name: kind, path: kind, type: utf8, nullable: true }
"#;
        for (table, expected) in [
            (
                format!(
                    "{EXPLODE_HEAD}    derive: {{ explode: {{ paths: [payload, payload.items], into: text }} }}"
                ),
                "derive explode reads 'payload.items' below 'payload'; no answer holds an array at 'payload' and a value inside it",
            ),
            (
                format!(
                    "{EXPLODE_HEAD}    derive: {{ explode: {{ paths: [payload.items, payload], into: text }} }}"
                ),
                "derive explode reads 'payload.items' below 'payload'",
            ),
            (
                format!(
                    "{EXPLODE_HEAD}    derive: {{ explode: {{ paths: [payload, payload], into: text }} }}"
                ),
                "derive explode reads the path 'payload' twice",
            ),
            (
                format!(
                    "{UNPIVOT_HEAD}    derive: {{ unpivot: {{ into: token, label: kind, from: [{{ label: a, path: x }}, {{ label: b, path: x.y }}] }} }}"
                ),
                "derive unpivot reads 'x.y' below 'x'; no answer holds a scalar at 'x' and a value inside it",
            ),
            (
                format!(
                    "{UNPIVOT_HEAD}    derive: {{ unpivot: {{ into: token, label: kind, from: [{{ label: a, path: x.y.z }}, {{ label: b, path: x.y }}] }} }}"
                ),
                "derive unpivot reads 'x.y.z' below 'x.y'",
            ),
        ] {
            let err = pack_with(&table).expect_err(expected);
            assert!(err.contains(expected), "want {expected:?} in: {err}");
        }
        // Siblings and keys that merely share a text prefix are not nested.
        for table in [
            format!(
                "{EXPLODE_HEAD}    derive: {{ explode: {{ paths: [payload, payloads], into: text }} }}"
            ),
            format!(
                "{EXPLODE_HEAD}    derive: {{ explode: {{ paths: [payload.a, payload.b], into: text }} }}"
            ),
            format!(
                "{UNPIVOT_HEAD}    derive: {{ unpivot: {{ into: token, label: kind, from: [{{ label: a, path: x.y }}, {{ label: b, path: x.yz }}] }} }}"
            ),
        ] {
            pack_with(&table).unwrap_or_else(|err| panic!("{table}: {err}"));
        }
    }

    #[test]
    fn a_filter_reading_above_an_unpivoted_field_is_refused() {
        let err = pack_with(
            r#"    action: demo.list
    row_path: "$.items"
    pagination: { strategy: single_page }
    derive: { unpivot: { into: token, label: kind, from: [{ label: a, path: meeting.artifacts.notes }] } }
    columns:
      - { name: token, path: token, type: utf8, nullable: true }
      - { name: kind, path: kind, type: utf8, nullable: true }
      - { name: artifacts, path: meeting.artifacts, type: json, nullable: true }
    filters:
      - { column: artifacts, op: eq, input: artifacts, fidelity: exact }"#,
        )
        .expect_err("a filter on the parent of an unpivoted field");
        assert!(
            err.contains("from which derive unpivot removes 'meeting.artifacts.notes'"),
            "{err}"
        );
    }

    /// A derive table for the filter-pushdown checks: one exploded element
    /// per row, a constant, a formatted id, a column read off the source
    /// row, and a JSON column holding the parent of the exploded array.
    fn derive_table_with_filter(filter: &str) -> Result<SourcePack, String> {
        pack_with(&format!(
            r#"    action: demo.list
    row_path: "$.items"
    pagination: {{ strategy: single_page }}
    derive:
      skip_when_null: summary
      explode: {{ paths: [summary.steps], into: text, index: position }}
      constants: {{ provider: zoom }}
      format: {{ item_id: "{{summary.meetingUuid}}#{{position}}" }}
    columns:
      - {{ name: meeting_id, path: summary.meetingUuid, type: utf8, nullable: false }}
      - {{ name: item_id, path: item_id, type: utf8, nullable: false }}
      - {{ name: text, path: text, type: utf8, nullable: true }}
      - {{ name: position, path: position, type: uint64, nullable: false }}
      - {{ name: provider, path: provider, type: utf8, nullable: false }}
      - {{ name: summary, path: summary, type: json, nullable: true }}
    filters:
      - {filter}"#
        ))
    }

    /// The gateway evaluates a pushed predicate against wire rows, which do
    /// not carry what derive writes (or carry what explode removed).
    #[rstest]
    #[case::exploded_element(
        "{ column: text, op: eq, input: text, fidelity: exact }",
        "filter on 'text' (Eq) reads 'text', which derive writes as 'text'"
    )]
    #[case::exploded_index(
        "{ column: position, op: gt, input: after, fidelity: inexact }",
        "filter on 'position' (Gt) reads 'position', which derive writes as 'position'"
    )]
    #[case::constant(
        "{ column: provider, op: eq, input: provider, fidelity: exact }",
        "filter on 'provider' (Eq) reads 'provider', which derive writes as 'provider'"
    )]
    #[case::inexact_constant(
        "{ column: provider, op: eq, input: provider, fidelity: inexact }",
        "which derive writes as 'provider'"
    )]
    #[case::formatted(
        "{ column: item_id, op: eq, input: id, fidelity: exact }",
        "filter on 'item_id' (Eq) reads 'item_id', which derive writes as 'item_id'"
    )]
    #[case::parent_of_an_exploded_path(
        "{ column: summary, op: eq, input: summary, fidelity: exact }",
        "filter on 'summary' (Eq) reads 'summary', from which derive explode removes 'summary.steps'"
    )]
    fn a_filter_on_a_derived_column_is_refused(#[case] filter: &str, #[case] expected: &str) {
        let err = derive_table_with_filter(filter).expect_err(expected);
        assert!(err.contains("items"), "names the table: {err}");
        assert!(err.contains(expected), "want {expected:?} in: {err}");
    }

    /// A column read off the source row is inherited unchanged by every
    /// derived row, so filtering source rows upstream keeps exactly the
    /// derived rows that match — explode and skip_when_null included.
    #[test]
    fn a_filter_on_a_source_column_of_a_derive_table_loads() {
        let pack = derive_table_with_filter(
            "{ column: meeting_id, op: eq, input: meetingUuid, fidelity: exact }",
        )
        .expect("a filter on a column the derivation does not write is legal");
        assert_eq!(pack.tables[0].filters.len(), 1);
        assert_eq!(pack.tables[0].filters[0].column, "meeting_id");
    }

    /// Templates render in key order, so one reading another's key would
    /// depend on how the two keys sort; both orders are refused alike.
    #[rstest]
    #[case::earlier_key_reads_a_later_one(
        r#"{ a_id: "{b_key}#x", b_key: "{id}" }"#,
        "derive format 'a_id' reads 'b_key', which derive format 'b_key' writes"
    )]
    #[case::later_key_reads_an_earlier_one(
        r#"{ z_id: "{b_key}#x", b_key: "{id}" }"#,
        "derive format 'z_id' reads 'b_key', which derive format 'b_key' writes"
    )]
    #[case::nested_path_under_another_template_key(
        r#"{ a_id: "{b_key.x}", b_key: "{id}" }"#,
        "derive format 'a_id' reads 'b_key.x', which derive format 'b_key' writes"
    )]
    // Reading its own key reads the pre-format wire value, so it does not
    // make the derived key "read" by anything.
    #[case::self_read_does_not_count_as_a_reader(
        r#"{ unread: "{unread}#x", a_id: "{id}" }"#,
        "derive writes 'unread', which no column path and no format template reads"
    )]
    fn a_format_template_cannot_read_another_templates_key(
        #[case] format: &str,
        #[case] expected: &str,
    ) {
        let err = pack_with(&format!(
            r#"    action: demo.get
    row_path: "$"
    row_shape: object
    pagination: {{ strategy: single_page }}
    derive:
      format: {format}
    columns:
      - {{ name: a_id, path: a_id, type: utf8, nullable: true }}
      - {{ name: z_id, path: z_id, type: utf8, nullable: true }}
      - {{ name: b_key, path: b_key, type: utf8, nullable: true }}"#
        ))
        .expect_err(expected);
        assert!(err.contains(expected), "want {expected:?} in: {err}");
    }

    /// Constants and explode run before format, so a template may read
    /// both; and a template may read its own key, which is the wire value
    /// whatever the key order.
    #[test]
    fn a_format_template_reads_constants_explode_keys_and_its_own_key() {
        let pack = pack_with(
            r#"    action: demo.get
    row_path: "$"
    row_shape: object
    pagination: { strategy: single_page }
    derive:
      explode: { paths: [steps], into: text, index: position }
      constants: { provider: zoom }
      format: { item_id: "{provider}:{uuid}#{position}", meeting: "zoom-{meeting}" }
    columns:
      - { name: meeting, path: meeting, type: utf8, nullable: false }
      - { name: item_id, path: item_id, type: utf8, nullable: false }
      - { name: text, path: text, type: utf8, nullable: true }"#,
        )
        .expect("a template reading a constant, the exploded index and its own key is legal");
        let derive = pack.tables[0].derive.expect("declared");
        let page = serde_json::json!({ "uuid": "u-1", "meeting": "m-1", "steps": ["a"] });
        let rows = derive
            .apply(pack.tables[0].id, std::slice::from_ref(&page), 1)
            .expect("derives");
        assert_eq!(
            rows,
            [serde_json::json!({
                "uuid": "u-1",
                "meeting": "zoom-m-1",
                "text": "a",
                "position": 0,
                "provider": "zoom",
                "item_id": "zoom:u-1#0",
            })]
        );
    }

    /// A key written with a non-object value replaces the wire object of
    /// that name, so a path below it would read NULL on every row (a null
    /// constant) or fail every scan (a scalar); it is refused at load.
    #[rstest]
    #[case::column_below_a_null_constant(
        "constants: { summary: null }",
        "column 'id' reads 'summary.id' below derive constant 'summary'"
    )]
    #[case::column_below_a_scalar_constant(
        "constants: { summary: zoom }",
        "column 'id' reads 'summary.id' below derive constant 'summary'"
    )]
    #[case::column_below_a_format_key(
        r#"format: { summary: "{provider}#x" }"#,
        "column 'id' reads 'summary.id' below derive format 'summary'"
    )]
    #[case::column_below_the_explode_index(
        "explode: { paths: [steps], into: text, index: summary }",
        "column 'id' reads 'summary.id' below derive explode `index` 'summary'"
    )]
    #[case::template_below_a_constant(
        r#"constants: { provider: zoom }, format: { text: "{provider.name}" }"#,
        "derive format 'text' reads 'provider.name' below derive constant 'provider'"
    )]
    fn a_non_object_derived_key_cannot_be_read_as_a_parent(
        #[case] derive: &str,
        #[case] expected: &str,
    ) {
        let err = pack_with(&format!(
            r#"    action: demo.get
    row_path: "$"
    row_shape: object
    pagination: {{ strategy: single_page }}
    derive: {{ {derive} }}
    columns:
      - {{ name: id, path: summary.id, type: utf8, nullable: false }}
      - {{ name: text, path: text, type: utf8, nullable: true }}
      - {{ name: provider, path: provider, type: utf8, nullable: true }}"#
        ))
        .expect_err(expected);
        assert!(err.contains(expected), "want {expected:?} in: {err}");
    }

    /// Reading a derived key whole stays legal, and so does reading below
    /// one that may hold an object: an exploded element (the planned Teams
    /// pack reads `item.text`), an object constant, or a template below its
    /// own key, which renders from the wire value before the key is written.
    #[rstest]
    #[case::whole_read_of_a_null_constant("constants: { summary: null }", "summary")]
    #[case::below_an_explode_into_key("explode: { paths: [actionItems], into: item }", "item.text")]
    #[case::below_an_object_constant("constants: { owner: { name: zoom } }", "owner.name")]
    #[case::template_below_its_own_key(r#"format: { summary: "{summary.id}" }"#, "summary")]
    fn a_derived_key_read_whole_or_below_a_possible_object_loads(
        #[case] derive: &str,
        #[case] path: &str,
    ) {
        pack_with(&format!(
            r#"    action: demo.get
    row_path: "$"
    row_shape: object
    pagination: {{ strategy: single_page }}
    derive: {{ {derive} }}
    columns:
      - {{ name: derived, path: {path}, type: utf8, nullable: true }}"#
        ))
        .expect("a derived key read whole or below a possible object loads");
    }

    #[test]
    fn split_action_continuation_parses_with_its_defaults() {
        let pack = pack_with(
            r#"    action: demo.list
    row_path: "$.entries"
    fingerprint: aa
    pagination:
      strategy: cursor
      cursor_input: cursor
      next_cursor_path: "$.cursor"
      page_size_input: limit
      page_size: 2000
      has_more_path: "$.hasMore"
      continuation:
        action: demo.list_continue
        fingerprint: bb
        inputs: cursor_only
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
        )
        .expect("continuation parses");
        let continuation = pack.tables[0].continuation.expect("declared");
        assert_eq!(continuation.action_id, "demo.list_continue");
        assert_eq!(continuation.expected_fingerprint, "bb");
        assert!(continuation.cursor_only);

        // Both actions are executed, and both are gated.
        let table = &pack.tables[0];
        assert_eq!(
            table.actions().collect::<Vec<_>>(),
            vec!["demo.list", "demo.list_continue"]
        );
        assert_eq!(
            table.gated_actions().collect::<Vec<_>>(),
            vec![("demo.list", "aa"), ("demo.list_continue", "bb")]
        );
    }

    #[test]
    fn continuation_action_defaults_to_the_tables_own() {
        // The same-action flavor: a provider that keeps the action but
        // rejects the opening inputs alongside a cursor.
        let pack = pack_with(
            r#"    action: demo.list
    row_path: "$.entries"
    fingerprint: aa
    pagination:
      strategy: cursor
      cursor_input: cursor
      next_cursor_path: "$.cursor"
      page_size: 100
      continuation:
        fingerprint: aa
        inputs: cursor_only
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
        )
        .expect("continuation parses");
        let continuation = pack.tables[0].continuation.expect("declared");
        assert_eq!(continuation.action_id, "demo.list");
        assert!(continuation.cursor_only);
    }

    #[test]
    fn continuation_invariants_are_rejected_with_targeted_errors() {
        for (body, expected) in [
            // A continuation on a page-number table is nonsense; the YAML
            // nesting makes it a deny_unknown_fields parse error for free.
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination:
      strategy: page_number
      page_input: page
      page_size_input: perPage
      page_size: 10
      continuation: { fingerprint: bb, inputs: cursor_only }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "unknown field",
            ),
            // Half a gate: pages 2..N pinned, page one unpinned.
            (
                r#"    action: demo.list
    row_path: "$.items"
    pagination:
      strategy: cursor
      cursor_input: cursor
      next_cursor_path: "$.cursor"
      page_size: 10
      continuation: { action: demo.list_continue, fingerprint: bb }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "the table does not",
            ),
            // Describes the default behavior — a no-op block, or (far more
            // likely) a forgotten `inputs: cursor_only`.
            (
                r#"    action: demo.list
    row_path: "$.items"
    fingerprint: aa
    pagination:
      strategy: cursor
      cursor_input: cursor
      next_cursor_path: "$.cursor"
      page_size: 10
      continuation: { fingerprint: aa }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "which is the default",
            ),
            // The fingerprint is never optional on a continuation.
            (
                r#"    action: demo.list
    row_path: "$.items"
    fingerprint: aa
    pagination:
      strategy: cursor
      cursor_input: cursor
      next_cursor_path: "$.cursor"
      page_size: 10
      continuation: { action: demo.list_continue, inputs: cursor_only }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "missing field `fingerprint`",
            ),
            // One action has one contract, so two disagreeing pins on it
            // cannot both be satisfied by any gateway.
            (
                r#"    action: demo.list
    row_path: "$.items"
    fingerprint: aa
    pagination:
      strategy: cursor
      cursor_input: cursor
      next_cursor_path: "$.cursor"
      page_size: 10
      continuation: { fingerprint: bb, inputs: cursor_only }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }"#,
                "pins a different fingerprint",
            ),
            // The silent-wrong-rows pairing: an Exact filter deletes the
            // Filter node from the plan, and `cursor_only` cannot carry
            // the predicate past page one.
            (
                r#"    action: demo.list
    row_path: "$.items"
    fingerprint: aa
    pagination:
      strategy: cursor
      cursor_input: cursor
      next_cursor_path: "$.cursor"
      page_size: 10
      continuation: { action: demo.list_continue, fingerprint: bb, inputs: cursor_only }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
      - { name: state, path: state, type: utf8, nullable: true }
    filters:
      - { column: state, op: eq, input: state, fidelity: exact }"#,
                "cannot carry the predicate past page one",
            ),
        ] {
            let err = pack_with(body).expect_err(expected);
            assert!(err.contains(expected), "want {expected:?} in: {err}");
        }
    }

    #[test]
    fn an_inexact_filter_stays_legal_alongside_a_cursor_only_continuation() {
        // The contrast case for the gate above. `Inexact` maps to
        // `TableProviderFilterPushDown::Inexact`, which KEEPS the `Filter`
        // node, so pages 2..N losing the pushed input is merely wasteful —
        // DataFusion re-applies the predicate and the rows stay correct.
        // Refusing this too would have been a false alarm.
        let pack = pack_with(
            r#"    action: demo.list
    row_path: "$.items"
    fingerprint: aa
    pagination:
      strategy: cursor
      cursor_input: cursor
      next_cursor_path: "$.cursor"
      page_size: 10
      continuation: { action: demo.list_continue, fingerprint: bb, inputs: cursor_only }
    columns:
      - { name: id, path: id, type: uint64, nullable: false }
      - { name: state, path: state, type: utf8, nullable: true }
    filters:
      - { column: state, op: eq, input: state, fidelity: inexact }"#,
        )
        .expect("an Inexact filter cannot return rows the query excluded");
        assert_eq!(pack.tables[0].filters.len(), 1);
    }
}
