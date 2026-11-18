//! Row derivation: the declarative step between "the rows the action
//! answered" and "the rows the table's columns read".
//!
//! Every pack before the meeting packs mapped columns onto keys the wire
//! already carried. A pack that must land a provider's answer in a shape
//! shared with OTHER providers (the meeting-minutes contract: one
//! `meetings` / `meeting_notes` / `meeting_action_items` shape for Zoom,
//! Google Meet, Microsoft Teams and Feishu) needs five things no wire key
//! gives it:
//!
//! - a **constant** on every row — `provider: zoom`, `kind: summary`, or
//!   an explicit `null` for a contract column the tool has no field for;
//! - a row per element of an **array** inside the answer — Zoom's
//!   `nextSteps[]` strings are the action items, one row each;
//! - an identity **formatted** from several fields — `<note_id>#<position>`,
//!   never a bare position, so one meeting's first item cannot overwrite
//!   another's;
//! - **skipping** a point-read whose payload is null — a summary Zoom will
//!   not return (`available: false`, `summary: null`) is zero rows, not a
//!   failed scan and not a row of NULLs;
//! - a row per **named field** that carries a value — Feishu answers a
//!   meeting's notes document and its transcript document as two sibling
//!   tokens (`note_doc_token`, `verbatim_doc_token`), each one note row of
//!   its own kind, and an empty token is a document not generated yet, not
//!   a row.
//!
//! A [`RowDerivation`](crate::derive::RowDerivation) declares those five,
//! and nothing else: no
//! expressions, no lookups, no reading of prose. It rewrites rows as JSON
//! and hands them to the SAME converter every other table uses, so a
//! derived column has the same typing, nullability and error identity as a
//! wire column. Steps run in a fixed order — skip, explode, unpivot,
//! constants, format — so a template can read an exploded element's
//! position or an unpivoted field's label, and a constant can never be
//! shadowed by a wire key of the same name (derived keys overwrite; the
//! pack's label is the pack's, not the provider's).
//!
//! Pagination never sees derived rows. The scan advances on the RAW page
//! (its row count and last row), so an explode that yields nothing from a
//! non-empty page cannot read as an empty page and end a listing early.
//! Derivation runs after the page is fetched and before conversion, in the
//! shared scan, so the engine and every other consumer of a pack table see
//! the same rows.

use std::fmt::Write as _;

use serde_json::{Map, Value};

use crate::error::OpenConnectorError;
use crate::json::json_kind;
use crate::source_pack::FixedValue;

/// One row per element of an array found inside each source row.
///
/// `paths` are tried in order and the first that resolves to a NON-EMPTY
/// array supplies the elements; a path that is missing, null, or an empty
/// array is passed over, and a path present with any other shape fails the
/// scan with its kind — every listed path, the ones after the chosen array
/// included, since explode consumes them all (below) and a drifted
/// fallback must not vanish unseen. When no path supplies elements the
/// source row yields zero rows. Each element lands under `into` on a copy
/// of the source row, and its 0-based position under `index` when one is
/// named.
///
/// **Explode consumes its paths.** The copy each element lands on is the
/// source row WITHOUT the value at any of `paths` — the array that was
/// chosen and every fallback beside it — so a derived row never carries
/// the array it came from. Everything else stays reachable, siblings under
/// the same parent included (`summary.meetingUuid` beside
/// `summary.nextSteps`). Copying the array onto each of its own `n` rows
/// would cost `n²` elements before the converter or a row bound could
/// refuse the page; a column or a `format` placeholder therefore cannot
/// read an exploded path (the loader refuses one that tries) and reads the
/// element through `into` instead. Each derived row does still carry every
/// other key of the source row, so `n` elements cost `n` copies of the row
/// without its arrays.
///
/// **Elements are not inspected.** Every element of the chosen array is a
/// row, a `null` one included: its row carries a null under `into` and its
/// own position under `index`, so a position is always the element's index
/// in the array. What a null or wrongly typed element means is the reading
/// column's call, made by the converter like for any wire value: a
/// nullable column reads NULL, and a non-null column or a type mismatch
/// fails the scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Explode {
    /// Row-relative dotted paths, tried in order.
    pub paths: &'static [&'static str],
    /// The key each element is written under.
    pub into: &'static str,
    /// The key the element's 0-based position is written under, if any.
    pub index: Option<&'static str>,
}

/// One named field of an [`Unpivot`]: the label its row carries and the
/// row-relative dotted path its value is read from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UnpivotField {
    /// Written under [`Unpivot::label`] on this field's row (`notes`).
    pub label: &'static str,
    /// Where the field's value is read (`meeting.related_artifacts.note_doc_token`).
    pub path: &'static str,
}

/// One row per named field that carries a value: the column-to-row turn a
/// provider forces when it answers several things of one kind as sibling
/// keys instead of an array.
///
/// For each source row, `from` is read in order. A field whose value is
/// missing, null or the EMPTY STRING yields no row — an empty string is how
/// a provider says "permitted, but nothing there yet" (Feishu sends `""`
/// for a meeting document not generated yet), and a row carrying it would
/// be an identity that names nothing. A field with any other scalar yields
/// one row: the source row with the value under `into` and the field's
/// label under `label`. A field holding an array or an object fails the
/// scan with its kind, as does a non-object parent along a path — every
/// field is checked before any row is yielded, so a drifted sibling cannot
/// vanish behind one that still reads.
///
/// **Unpivot consumes its paths**, as [`Explode`] does: a derived row
/// carries none of the `from` values, so one field's row never also
/// carries the other field's value, and a column or a template reads the
/// value through `into` (the loader refuses one reading a consumed path).
/// When no field carries a value the source row yields zero rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unpivot {
    /// The fields, in the order their rows are yielded.
    pub from: &'static [UnpivotField],
    /// The key each field's value is written under.
    pub into: &'static str,
    /// The key each field's label is written under.
    pub label: &'static str,
}

/// The derivation a table declares; see the module doc for the five steps
/// and their order. `Default` is the no-op derivation, which `apply` turns
/// into a plain clone of the rows — callers check [`Self::is_empty`] to
/// skip even that.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct RowDerivation {
    /// Drop every source row where this row-relative path is missing or
    /// null (a null parent counts as missing). A present value of another
    /// shape keeps the row. A parent along the path that is present but not
    /// an object fails the scan with its kind.
    pub skip_when_null: Option<&'static str>,
    /// One row per array element; see [`Explode`].
    pub explode: Option<Explode>,
    /// One row per named field that carries a value; see [`Unpivot`]. Runs
    /// on the rows explode yields (the loader refuses a table declaring
    /// both, so in a pack it runs on the source rows).
    pub unpivot: Option<Unpivot>,
    /// Keys written on every row with a literal value, overwriting a wire
    /// key of the same name. `FixedValue::Json(&Value::Null)` spells an
    /// explicit NULL.
    pub constants: &'static [(&'static str, FixedValue)],
    /// Keys written on every row by rendering a template over the row:
    /// `"{summary.meetingUuid}#{position}"`. A placeholder is a
    /// row-relative dotted path in braces; it renders a string verbatim, a
    /// number or boolean as JSON text, and a missing or null value makes
    /// the WHOLE rendered value null (so a non-null identity column fails
    /// loudly rather than reading as `"#0"`). An array or object under a
    /// placeholder fails the scan with its kind. A template cannot read
    /// another template's key (templates render in key order, so the loader
    /// refuses it); reading its own key reads the value from before format.
    pub format: &'static [(&'static str, &'static str)],
}

impl RowDerivation {
    /// Whether the derivation declares nothing — the shape every pre-existing
    /// pack table has, for which the scan hands the wire rows straight to
    /// the converter.
    pub fn is_empty(&self) -> bool {
        self.skip_when_null.is_none()
            && self.explode.is_none()
            && self.unpivot.is_none()
            && self.constants.is_empty()
            && self.format.is_empty()
    }

    /// Every key the derivation writes onto a row, in declaration order:
    /// the explode's `into` and `index`, the unpivot's `into` and `label`,
    /// then the constants, then the formats. The loader cross-checks these
    /// against the table's columns.
    pub fn written_keys(&self) -> Vec<&'static str> {
        let mut keys = Vec::new();
        if let Some(explode) = &self.explode {
            keys.push(explode.into);
            keys.extend(explode.index);
        }
        if let Some(unpivot) = &self.unpivot {
            keys.push(unpivot.into);
            keys.push(unpivot.label);
        }
        keys.extend(self.constants.iter().map(|(key, _)| *key));
        keys.extend(self.format.iter().map(|(key, _)| *key));
        keys
    }

    /// Derive the rows of one page. `table` and `page` are carried into
    /// errors; `rows` are the wire rows the scan extracted.
    ///
    /// # Errors
    /// [`OpenConnectorError::RowDerivationFailed`] names the step, the path,
    /// the page, the source row and the JSON *kind* found — never a value.
    pub fn apply(
        &self,
        table: &str,
        rows: &[Value],
        page: usize,
    ) -> Result<Vec<Value>, OpenConnectorError> {
        let mut out = Vec::with_capacity(rows.len());
        for (row_index, row) in rows.iter().enumerate() {
            let fail = |step: &str, path: &str, expected: &str, found: &str| {
                OpenConnectorError::RowDerivationFailed {
                    table: table.to_string(),
                    step: step.to_string(),
                    path: path.to_string(),
                    page,
                    row: row_index,
                    expected: expected.to_string(),
                    found: found.to_string(),
                }
            };
            let Value::Object(source) = row else {
                return Err(fail("row", "$", "an object", json_kind(row)));
            };
            if let Some(path) = self.skip_when_null {
                match resolve(source, path) {
                    Ok(None) => continue,
                    Ok(Some(_)) => {}
                    Err(found) => {
                        return Err(fail("skip_when_null", path, "an object parent", &found));
                    }
                }
            }
            let derived: Vec<Map<String, Value>> = match &self.explode {
                None => vec![source.clone()],
                Some(explode) => {
                    let mut elements: Option<&[Value]> = None;
                    // No early exit once an array is chosen: the paths after
                    // it are consumed too, so a drifted fallback must fail
                    // here rather than be dropped silently by `clone_without`.
                    for path in explode.paths {
                        match resolve(source, path) {
                            Ok(None) => {}
                            Ok(Some(Value::Array(items))) => {
                                if elements.is_none() && !items.is_empty() {
                                    elements = Some(items.as_slice());
                                }
                            }
                            Ok(Some(other)) => {
                                return Err(fail("explode", path, "an array", json_kind(other)));
                            }
                            Err(found) => {
                                return Err(fail("explode", path, "an object parent", &found));
                            }
                        }
                    }
                    match elements {
                        None => Vec::new(),
                        Some(items) => {
                            // Built once per source row and never holding
                            // the arrays: cloning `source` per element
                            // would copy the n-element array n times.
                            let base = clone_without(source, explode.paths);
                            items
                                .iter()
                                .enumerate()
                                .map(|(position, item)| {
                                    let mut row = base.clone();
                                    row.insert(explode.into.to_string(), item.clone());
                                    if let Some(index) = explode.index {
                                        row.insert(index.to_string(), Value::from(position as u64));
                                    }
                                    row
                                })
                                .collect()
                        }
                    }
                }
            };
            let derived = match &self.unpivot {
                None => derived,
                Some(unpivot) => {
                    let mut unpivoted = Vec::with_capacity(derived.len() * unpivot.from.len());
                    for row in &derived {
                        let mut values: Vec<(&'static str, &Value)> = Vec::new();
                        // Every field is resolved before a row is yielded:
                        // all of them are consumed, so a drifted one must
                        // fail here, not be dropped by `clone_without`.
                        for field in unpivot.from {
                            match resolve(row, field.path) {
                                Ok(None) => {}
                                Ok(Some(Value::String(text))) if text.is_empty() => {}
                                Ok(Some(
                                    value @ (Value::String(_) | Value::Number(_) | Value::Bool(_)),
                                )) => values.push((field.label, value)),
                                Ok(Some(other)) => {
                                    return Err(fail(
                                        "unpivot",
                                        field.path,
                                        "a scalar",
                                        json_kind(other),
                                    ));
                                }
                                Err(found) => {
                                    return Err(fail(
                                        "unpivot",
                                        field.path,
                                        "an object parent",
                                        &found,
                                    ));
                                }
                            }
                        }
                        if values.is_empty() {
                            continue;
                        }
                        let paths: Vec<&str> = unpivot.from.iter().map(|f| f.path).collect();
                        let base = clone_without(row, &paths);
                        for (label, value) in values {
                            let mut out_row = base.clone();
                            out_row.insert(unpivot.into.to_string(), value.clone());
                            out_row.insert(unpivot.label.to_string(), Value::from(label));
                            unpivoted.push(out_row);
                        }
                    }
                    unpivoted
                }
            };
            for mut row in derived {
                for (key, value) in self.constants {
                    row.insert(key.to_string(), value.to_json());
                }
                for (key, template) in self.format {
                    let rendered = render(&row, template)
                        .map_err(|(path, found)| fail("format", path, "a scalar", &found))?;
                    row.insert(key.to_string(), rendered);
                }
                out.push(Value::Object(row));
            }
        }
        Ok(out)
    }
}

/// Clone `row` leaving out the value at each row-relative dotted path in
/// `paths`, without ever copying what is left out. A path is followed only
/// through object parents; one that does not resolve removes nothing, and
/// every key no path names — siblings of a removed key included — is
/// cloned as it is.
fn clone_without(row: &Map<String, Value>, paths: &[&str]) -> Map<String, Value> {
    let mut out = Map::with_capacity(row.len());
    for (key, value) in row {
        let mut removed = false;
        let mut below: Vec<&str> = Vec::new();
        for path in paths {
            match path.split_once('.') {
                None => removed |= *path == key.as_str(),
                Some((head, rest)) if head == key.as_str() => below.push(rest),
                Some(_) => {}
            }
        }
        if removed {
            continue;
        }
        let kept = match value {
            Value::Object(child) if !below.is_empty() => {
                Value::Object(clone_without(child, &below))
            }
            other => other.clone(),
        };
        out.insert(key.clone(), kept);
    }
    out
}

/// Walk a row-relative dotted path. `Ok(None)` when a key is absent or a
/// value on the way (or at the end) is null; `Err(kind)` when a parent that
/// must be descended into is present with a non-object shape — the same
/// split the converter makes between "absent, maybe null" and "the shape
/// changed upstream".
fn resolve<'a>(row: &'a Map<String, Value>, path: &str) -> Result<Option<&'a Value>, String> {
    let mut segments = path.split('.');
    let Some(first) = segments.next() else {
        return Ok(None);
    };
    let mut current = match row.get(first) {
        None | Some(Value::Null) => return Ok(None),
        Some(value) => value,
    };
    for segment in segments {
        let map = match current {
            Value::Object(map) => map,
            Value::Null => return Ok(None),
            other => return Err(json_kind(other).to_string()),
        };
        current = match map.get(segment) {
            None | Some(Value::Null) => return Ok(None),
            Some(value) => value,
        };
    }
    Ok(Some(current))
}

/// The placeholder paths of a template, in order, or why the template is
/// malformed. Shared by the loader (which refuses a bad template at parse
/// time) and the renderer (which therefore never meets one).
///
/// # Example
/// ```
/// use skardi_source_pack::derive::template_placeholders;
///
/// assert_eq!(
///     template_placeholders("{summary.meetingUuid}#{position}").unwrap(),
///     vec!["summary.meetingUuid", "position"]
/// );
/// assert!(template_placeholders("{unclosed").is_err());
/// assert!(template_placeholders("no placeholder").is_err());
/// ```
pub fn template_placeholders(template: &str) -> Result<Vec<&str>, String> {
    let mut placeholders = Vec::new();
    let mut rest = template;
    while let Some(open) = rest.find(['{', '}']) {
        if rest.as_bytes()[open] == b'}' {
            return Err("has a '}' with no matching '{'".to_string());
        }
        let after_open = &rest[open + 1..];
        let Some(close) = after_open.find(['{', '}']) else {
            return Err("has a '{' with no matching '}'".to_string());
        };
        if after_open.as_bytes()[close] == b'{' {
            return Err("nests a '{' inside a placeholder".to_string());
        }
        let path = &after_open[..close];
        if path.is_empty() {
            return Err("has an empty '{}' placeholder".to_string());
        }
        if path.split('.').any(str::is_empty) {
            return Err(format!(
                "placeholder '{{{path}}}' has an empty path segment"
            ));
        }
        placeholders.push(path);
        rest = &after_open[close + 1..];
    }
    if placeholders.is_empty() {
        return Err("has no placeholder; a fixed text belongs in `constants`".to_string());
    }
    Ok(placeholders)
}

/// Render a template over one row. `Ok(Value::Null)` when any placeholder
/// resolves to nothing; `Err((path, kind))` when a placeholder meets an
/// array, an object, or a non-object parent.
fn render<'t>(row: &Map<String, Value>, template: &'t str) -> Result<Value, (&'t str, String)> {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after_open = &rest[open + 1..];
        // The loader validated the template, so a '}' follows.
        let close = after_open.find('}').unwrap_or(after_open.len());
        let path = &after_open[..close];
        match resolve(row, path) {
            Ok(None) => return Ok(Value::Null),
            Ok(Some(Value::String(text))) => out.push_str(text),
            Ok(Some(Value::Number(number))) => {
                let _ = write!(out, "{number}");
            }
            Ok(Some(Value::Bool(flag))) => {
                let _ = write!(out, "{flag}");
            }
            Ok(Some(other)) => return Err((path, json_kind(other).to_string())),
            Err(found) => return Err((path, found)),
        }
        rest = after_open.get(close + 1..).unwrap_or("");
    }
    out.push_str(rest);
    Ok(Value::String(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NULL: Value = Value::Null;

    fn summary_answer(next_steps: Value, edited: Value) -> Value {
        json!({
            "available": true,
            "summary": {
                "meetingUuid": "uuid-1",
                "meetingId": 81234567890u64,
                "nextSteps": next_steps,
                "editedSummary": edited,
            }
        })
    }

    #[test]
    fn an_empty_derivation_clones_the_rows() {
        let derivation = RowDerivation::default();
        assert!(derivation.is_empty());
        let rows = vec![json!({"a": 1}), json!({"a": 2})];
        assert_eq!(derivation.apply("t", &rows, 1).unwrap(), rows);
        assert!(derivation.written_keys().is_empty());
    }

    #[test]
    fn constants_are_written_on_every_row_and_win_over_wire_keys() {
        let derivation = RowDerivation {
            constants: &[
                ("provider", FixedValue::Str("zoom")),
                ("attendee_ids", FixedValue::Json(&NULL)),
                ("count", FixedValue::Int(3)),
            ],
            ..RowDerivation::default()
        };
        let rows = vec![
            json!({"provider": "spoofed", "id": "a"}),
            json!({"id": "b"}),
        ];
        let out = derivation.apply("t", &rows, 1).unwrap();
        assert_eq!(
            out[0]["provider"], "zoom",
            "a wire key never shadows the pack's label"
        );
        assert_eq!(out[1]["provider"], "zoom");
        assert!(out[0].get("attendee_ids").is_some_and(Value::is_null));
        assert_eq!(out[1]["count"], 3);
        assert_eq!(
            out[0]["id"], "a",
            "wire keys the derivation does not name stay"
        );
    }

    #[test]
    fn explode_yields_one_row_per_element_with_its_position() {
        let derivation = RowDerivation {
            explode: Some(Explode {
                paths: &["summary.nextSteps"],
                into: "text",
                index: Some("position"),
            }),
            ..RowDerivation::default()
        };
        let rows = vec![summary_answer(
            json!(["Send the agenda", "Book the room"]),
            NULL,
        )];
        let out = derivation.apply("t", &rows, 1).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["text"], "Send the agenda");
        assert_eq!(out[0]["position"], 0);
        assert_eq!(out[1]["text"], "Book the room");
        assert_eq!(out[1]["position"], 1);
        // The parent's keys stay reachable on every derived row.
        assert_eq!(out[1]["summary"]["meetingUuid"], "uuid-1");
        assert_eq!(derivation.written_keys(), vec!["text", "position"]);
    }

    #[test]
    fn an_exploded_row_carries_none_of_the_listed_arrays_and_keeps_their_siblings() {
        let derivation = RowDerivation {
            explode: Some(Explode {
                paths: &["summary.editedSummary.nextSteps", "summary.nextSteps"],
                into: "text",
                index: Some("position"),
            }),
            format: &[("item_id", "{summary.meetingUuid}#{position}")],
            ..RowDerivation::default()
        };
        // Both lists non-empty: the edited one is chosen, and the legacy
        // fallback is the same hazard, so neither may ride on a derived row.
        let edited = json!({"nextSteps": ["Edited one", "Edited two"], "summaryTitle": "Edited"});
        let rows = vec![summary_answer(json!(["Legacy step"]), edited)];
        let out = derivation.apply("t", &rows, 1).unwrap();
        assert_eq!(out.len(), 2);
        for (position, row) in out.iter().enumerate() {
            let summary = row["summary"].as_object().expect("summary stays an object");
            assert!(
                !summary.contains_key("nextSteps"),
                "the fallback array is consumed"
            );
            let edited = summary["editedSummary"]
                .as_object()
                .expect("the edited summary stays an object");
            assert!(
                !edited.contains_key("nextSteps"),
                "the chosen array is consumed"
            );
            // Siblings under each parent, and the keys above them, stay.
            assert_eq!(edited["summaryTitle"], "Edited");
            assert_eq!(summary["meetingUuid"], "uuid-1");
            assert_eq!(summary["meetingId"], 81234567890u64);
            assert_eq!(row["available"], true);
            // ...which is what `format` and the columns read.
            assert_eq!(row["item_id"], format!("uuid-1#{position}"));
        }
        assert_eq!(out[0]["text"], "Edited one");
        assert_eq!(out[1]["text"], "Edited two");
        // The source rows are borrowed, never rewritten.
        assert_eq!(rows[0]["summary"]["nextSteps"], json!(["Legacy step"]));

        // A top-level exploded path is consumed the same way.
        let top_level = RowDerivation {
            explode: Some(Explode {
                paths: &["steps"],
                into: "text",
                index: None,
            }),
            ..RowDerivation::default()
        };
        let out = top_level
            .apply("t", &[json!({"id": "a", "steps": ["one", "two"]})], 1)
            .unwrap();
        assert_eq!(
            out,
            vec![
                json!({"id": "a", "text": "one"}),
                json!({"id": "a", "text": "two"})
            ]
        );
    }

    #[test]
    fn a_large_array_explodes_into_rows_that_do_not_grow_with_it() {
        // Linear, pinned without a clock: 10 000 elements make 10 000 rows,
        // and no row carries the array, so every row's serialized size is a
        // small constant instead of growing with the element count (which
        // is what made the whole page quadratic).
        const ELEMENTS: usize = 10_000;
        let steps: Vec<Value> = (0..ELEMENTS)
            .map(|step| Value::String(format!("step {step}")))
            .collect();
        let derivation = RowDerivation {
            explode: Some(Explode {
                paths: &["summary.editedSummary.nextSteps", "summary.nextSteps"],
                into: "text",
                index: Some("position"),
            }),
            ..RowDerivation::default()
        };
        let rows = vec![summary_answer(Value::Array(steps), NULL)];
        let out = derivation.apply("t", &rows, 1).unwrap();
        assert_eq!(out.len(), ELEMENTS);
        for (position, row) in out.iter().enumerate() {
            assert!(
                row["summary"].get("nextSteps").is_none(),
                "row {position} carries the source array"
            );
            assert_eq!(row["position"], position as u64);
            let size = row.to_string().len();
            assert!(
                size < 256,
                "row {position} serializes to {size} bytes; it grew with the array"
            );
        }
        assert_eq!(out[ELEMENTS - 1]["text"], format!("step {}", ELEMENTS - 1));
    }

    #[test]
    fn explode_takes_the_first_path_with_a_non_empty_array() {
        let derivation = RowDerivation {
            explode: Some(Explode {
                paths: &["summary.editedSummary.nextSteps", "summary.nextSteps"],
                into: "text",
                index: None,
            }),
            ..RowDerivation::default()
        };
        // Edited and non-empty: the edited list wins.
        let edited = json!({"nextSteps": ["Edited step"]});
        let out = derivation
            .apply("t", &[summary_answer(json!(["Legacy step"]), edited)], 1)
            .unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["text"], "Edited step");
        assert!(
            out[0].get("position").is_none(),
            "no index key was asked for"
        );
        // Edited present but empty: passed over, the legacy list serves.
        let edited = json!({"nextSteps": []});
        let out = derivation
            .apply("t", &[summary_answer(json!(["Legacy step"]), edited)], 1)
            .unwrap();
        assert_eq!(out[0]["text"], "Legacy step");
        // Edited null (a null parent) and the legacy list missing: zero rows,
        // no error.
        let row = json!({"summary": {"meetingUuid": "uuid-1"}});
        assert!(derivation.apply("t", &[row], 1).unwrap().is_empty());
        // Both empty arrays: zero rows.
        let row = summary_answer(json!([]), json!({"nextSteps": []}));
        assert!(derivation.apply("t", &[row], 1).unwrap().is_empty());
    }

    /// Skipping a null element would shift every later position away from
    /// its array index, and so every identity formatted from it.
    #[test]
    fn a_null_element_is_a_row_at_its_own_position() {
        let derivation = RowDerivation {
            explode: Some(Explode {
                paths: &["summary.nextSteps"],
                into: "text",
                index: Some("position"),
            }),
            format: &[("item_id", "{summary.meetingUuid}#{position}")],
            ..RowDerivation::default()
        };
        let rows = vec![summary_answer(
            json!(["Send the agenda", null, "Book the room"]),
            NULL,
        )];
        let out = derivation.apply("t", &rows, 1).unwrap();
        assert_eq!(out.len(), 3, "a null element is not skipped");
        assert_eq!(out[1].get("text"), Some(&NULL), "written, and null");
        assert_eq!(out[1]["position"], 1);
        assert_eq!(out[1]["item_id"], "uuid-1#1");
        assert_eq!(out[2]["text"], "Book the room");
        assert_eq!(out[2]["position"], 2, "positions stay array indices");
        assert_eq!(out[2]["item_id"], "uuid-1#2");
    }

    #[test]
    fn explode_fails_with_the_kind_when_the_path_is_not_an_array() {
        let derivation = RowDerivation {
            explode: Some(Explode {
                paths: &["summary.nextSteps"],
                into: "text",
                index: None,
            }),
            ..RowDerivation::default()
        };
        let err = derivation
            .apply(
                "zoom.meeting_action_items",
                &[summary_answer(json!("one step"), NULL)],
                2,
            )
            .unwrap_err();
        match err {
            OpenConnectorError::RowDerivationFailed {
                table,
                step,
                path,
                page,
                row,
                expected,
                found,
            } => {
                assert_eq!(table, "zoom.meeting_action_items");
                assert_eq!(step, "explode");
                assert_eq!(path, "summary.nextSteps");
                assert_eq!(page, 2);
                assert_eq!(row, 0);
                assert_eq!(expected, "an array");
                assert_eq!(found, "a string");
            }
            other => panic!("expected RowDerivationFailed, got {other}"),
        }
        // A non-object parent on the way is a shape change, not an absence.
        let err = derivation
            .apply("t", &[json!({"summary": "flattened"})], 1)
            .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("explode") && text.contains("a string"),
            "{text}"
        );
        assert!(!text.contains("flattened"), "values never appear: {text}");
    }

    #[test]
    fn explode_fails_on_a_wrong_shaped_path_after_the_chosen_array() {
        let derivation = RowDerivation {
            explode: Some(Explode {
                paths: &["summary.editedSummary.nextSteps", "summary.nextSteps"],
                into: "text",
                index: None,
            }),
            ..RowDerivation::default()
        };
        let edited = json!({"nextSteps": ["Edited step"]});
        let err = derivation
            .apply(
                "zoom.meeting_action_items",
                &[summary_answer(json!("drifted legacy"), edited)],
                3,
            )
            .unwrap_err();
        match err {
            OpenConnectorError::RowDerivationFailed {
                step,
                path,
                page,
                row,
                expected,
                found,
                ..
            } => {
                assert_eq!(step, "explode");
                assert_eq!(path, "summary.nextSteps", "the fallback is named");
                assert_eq!(page, 3);
                assert_eq!(row, 0);
                assert_eq!(expected, "an array");
                assert_eq!(found, "a string");
            }
            other => panic!("expected RowDerivationFailed, got {other}"),
        }

        // A non-object parent of a later path is a shape change too.
        let derivation = RowDerivation {
            explode: Some(Explode {
                paths: &["steps", "legacy.steps"],
                into: "text",
                index: None,
            }),
            ..RowDerivation::default()
        };
        let err = derivation
            .apply(
                "t",
                &[json!({"steps": ["Send the agenda"], "legacy": 7})],
                1,
            )
            .unwrap_err();
        let text = err.to_string();
        assert!(
            text.contains("legacy.steps") && text.contains("a number"),
            "{text}"
        );
        assert!(!text.contains("agenda"), "values never appear: {text}");
    }

    #[test]
    fn explode_passes_over_a_later_path_that_is_missing_null_or_empty() {
        let derivation = RowDerivation {
            explode: Some(Explode {
                paths: &["summary.editedSummary.nextSteps", "summary.nextSteps"],
                into: "text",
                index: Some("position"),
            }),
            ..RowDerivation::default()
        };
        let edited = json!({"nextSteps": ["Edited one", "Edited two"]});
        let cases = [
            (
                "missing",
                json!({"summary": {"meetingUuid": "uuid-1", "editedSummary": edited}}),
            ),
            ("null", summary_answer(NULL, edited.clone())),
            ("empty_array", summary_answer(json!([]), edited.clone())),
        ];
        for (case, row) in cases {
            let out = derivation
                .apply("t", &[row], 1)
                .unwrap_or_else(|err| panic!("{case}: {err}"));
            assert_eq!(out.len(), 2, "{case}");
            assert_eq!(out[0]["text"], "Edited one", "{case}");
            assert_eq!(out[1]["text"], "Edited two", "{case}");
            assert_eq!(out[1]["position"], 1, "{case}");
            assert!(
                out[0]["summary"].get("nextSteps").is_none(),
                "{case}: the fallback is still consumed"
            );
        }
    }

    #[test]
    fn format_renders_scalars_and_nulls_the_whole_value_on_a_missing_part() {
        let derivation = RowDerivation {
            explode: Some(Explode {
                paths: &["summary.nextSteps"],
                into: "text",
                index: Some("position"),
            }),
            format: &[
                ("item_id", "{summary.meetingUuid}#{position}"),
                ("series_id", "{summary.meetingId}"),
                ("flagged", "{available}!"),
                ("missing", "{summary.absent}-{position}"),
            ],
            ..RowDerivation::default()
        };
        let out = derivation
            .apply("t", &[summary_answer(json!(["a", "b"]), NULL)], 1)
            .unwrap();
        assert_eq!(out[0]["item_id"], "uuid-1#0");
        assert_eq!(out[1]["item_id"], "uuid-1#1");
        assert_eq!(
            out[0]["series_id"], "81234567890",
            "a number renders as its JSON text"
        );
        assert_eq!(out[0]["flagged"], "true!");
        assert!(
            out[0]["missing"].is_null(),
            "one missing placeholder nulls the whole value, never renders '-0'"
        );
    }

    #[test]
    fn format_refuses_a_non_scalar_placeholder_by_kind() {
        let derivation = RowDerivation {
            format: &[("bad", "{summary}")],
            ..RowDerivation::default()
        };
        let err = derivation
            .apply("t", &[summary_answer(json!([]), NULL)], 1)
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::RowDerivationFailed { ref step, ref path, ref found, .. }
                if step == "format" && path == "summary" && found == "an object"
        ));
    }

    #[test]
    fn skip_when_null_drops_rows_without_the_payload() {
        let derivation = RowDerivation {
            skip_when_null: Some("summary"),
            constants: &[("provider", FixedValue::Str("zoom"))],
            ..RowDerivation::default()
        };
        let rows = vec![
            json!({"available": false, "summary": null}),
            json!({"available": true, "summary": {"meetingUuid": "uuid-2"}}),
            json!({"available": false}),
        ];
        let out = derivation.apply("t", &rows, 1).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["summary"]["meetingUuid"], "uuid-2");
        assert_eq!(out[0]["provider"], "zoom");
        // A nested skip path treats a null parent as absent, and a wrong
        // shaped parent as a failure.
        let nested = RowDerivation {
            skip_when_null: Some("summary.editedSummary"),
            ..RowDerivation::default()
        };
        assert!(
            nested
                .apply("t", &[json!({"summary": null})], 1)
                .unwrap()
                .is_empty()
        );
        let err = nested.apply("t", &[json!({"summary": 7})], 1).unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::RowDerivationFailed { ref step, ref found, .. }
                if step == "skip_when_null" && found == "a number"
        ));
    }

    #[test]
    fn a_row_that_is_not_an_object_fails_before_any_step() {
        let derivation = RowDerivation {
            constants: &[("provider", FixedValue::Str("zoom"))],
            ..RowDerivation::default()
        };
        let err = derivation.apply("t", &[json!("text")], 1).unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::RowDerivationFailed { ref step, ref found, .. }
                if step == "row" && found == "a string"
        ));
    }

    #[test]
    fn template_parsing_names_each_malformation() {
        for (template, expected) in [
            ("{a}}", "no matching '{'"),
            ("{a", "no matching '}'"),
            ("{a{b}}", "nests"),
            ("{}", "empty '{}'"),
            ("{a..b}", "empty path segment"),
            ("plain", "no placeholder"),
        ] {
            let err = template_placeholders(template).unwrap_err();
            assert!(err.contains(expected), "{template}: {err}");
        }
        assert_eq!(
            template_placeholders("{a.b}-{c}").unwrap(),
            vec!["a.b", "c"]
        );
    }

    const ARTIFACTS: &[UnpivotField] = &[
        UnpivotField {
            label: "notes",
            path: "meeting.related_artifacts.note_doc_token",
        },
        UnpivotField {
            label: "transcript",
            path: "meeting.related_artifacts.verbatim_doc_token",
        },
    ];

    fn artifacts_derivation() -> RowDerivation {
        RowDerivation {
            unpivot: Some(Unpivot {
                from: ARTIFACTS,
                into: "doc_token",
                label: "kind",
            }),
            constants: &[("provider", FixedValue::Str("feishu"))],
            ..RowDerivation::default()
        }
    }

    fn meeting_answer(artifacts: Value) -> Value {
        json!({"meeting": {"id": "m-1", "start_time": "1700000000", "related_artifacts": artifacts}})
    }

    #[test]
    fn unpivot_yields_one_row_per_field_in_declared_order() {
        let derivation = artifacts_derivation();
        let rows = vec![meeting_answer(
            json!({"note_doc_token": "doc-notes", "verbatim_doc_token": "doc-verbatim"}),
        )];
        let out = derivation.apply("t", &rows, 1).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["kind"], "notes");
        assert_eq!(out[0]["doc_token"], "doc-notes");
        assert_eq!(out[1]["kind"], "transcript");
        assert_eq!(out[1]["doc_token"], "doc-verbatim");
        // The source row's other keys stay reachable on both rows, and the
        // constants are written after the turn.
        assert_eq!(out[1]["meeting"]["id"], "m-1");
        assert_eq!(out[1]["meeting"]["start_time"], "1700000000");
        assert_eq!(out[0]["provider"], "feishu");
        assert_eq!(
            derivation.written_keys(),
            vec!["doc_token", "kind", "provider"]
        );
    }

    #[test]
    fn an_unpivoted_row_carries_none_of_the_fields() {
        let rows = vec![meeting_answer(
            json!({"note_doc_token": "doc-notes", "verbatim_doc_token": "doc-verbatim", "other": 1}),
        )];
        let out = artifacts_derivation().apply("t", &rows, 1).unwrap();
        for row in &out {
            let artifacts = &row["meeting"]["related_artifacts"];
            assert!(artifacts.get("note_doc_token").is_none());
            assert!(artifacts.get("verbatim_doc_token").is_none());
            assert_eq!(artifacts["other"], 1, "a sibling of the fields stays");
        }
    }

    #[test]
    fn unpivot_skips_a_missing_null_or_empty_field() {
        let cases: [(&str, Value, &[&str]); 6] = [
            ("both missing", json!({}), &[]),
            ("parent missing", NULL, &[]),
            (
                "both null",
                json!({"note_doc_token": null, "verbatim_doc_token": null}),
                &[],
            ),
            (
                "both empty",
                json!({"note_doc_token": "", "verbatim_doc_token": ""}),
                &[],
            ),
            (
                "notes only",
                json!({"note_doc_token": "doc-notes", "verbatim_doc_token": ""}),
                &["notes"],
            ),
            (
                "transcript only",
                json!({"verbatim_doc_token": "doc-verbatim"}),
                &["transcript"],
            ),
        ];
        for (case, artifacts, kinds) in cases {
            let out = artifacts_derivation()
                .apply("t", &[meeting_answer(artifacts)], 1)
                .unwrap();
            let got: Vec<&str> = out
                .iter()
                .map(|row| row["kind"].as_str().unwrap())
                .collect();
            assert_eq!(got, kinds, "{case}");
        }
    }

    #[test]
    fn unpivot_keeps_a_non_string_scalar() {
        let out = artifacts_derivation()
            .apply(
                "t",
                &[meeting_answer(
                    json!({"note_doc_token": 42, "verbatim_doc_token": false}),
                )],
                1,
            )
            .unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0]["doc_token"], 42);
        assert_eq!(out[1]["doc_token"], false);
    }

    #[test]
    fn unpivot_fails_with_the_kind_on_a_drifted_field() {
        // The object case drifts the SECOND field while the first still
        // reads: every field is checked before a row is yielded.
        let cases = [
            (
                json!({"note_doc_token": ["a"]}),
                "meeting.related_artifacts.note_doc_token",
                "an array",
            ),
            (
                json!({"note_doc_token": "ok", "verbatim_doc_token": {"t": 1}}),
                "meeting.related_artifacts.verbatim_doc_token",
                "an object",
            ),
            (
                json!("drifted"),
                "meeting.related_artifacts.note_doc_token",
                "a string",
            ),
        ];
        for (artifacts, expected_path, expected_kind) in cases {
            let err = artifacts_derivation()
                .apply("t", &[meeting_answer(artifacts)], 3)
                .unwrap_err();
            match err {
                OpenConnectorError::RowDerivationFailed {
                    step,
                    path,
                    page,
                    found,
                    ..
                } => {
                    assert_eq!(step, "unpivot");
                    assert_eq!(path, expected_path);
                    assert_eq!(page, 3);
                    assert_eq!(found, expected_kind);
                }
                other => panic!("expected RowDerivationFailed, got {other}"),
            }
        }
    }

    #[test]
    fn a_format_template_reads_the_unpivoted_value_and_label() {
        let derivation = RowDerivation {
            format: &[("note_key", "{meeting.id}#{kind}#{doc_token}")],
            ..artifacts_derivation()
        };
        let out = derivation
            .apply(
                "t",
                &[meeting_answer(json!({"note_doc_token": "doc-notes"}))],
                1,
            )
            .unwrap();
        assert_eq!(out[0]["note_key"], "m-1#notes#doc-notes");
    }
}
