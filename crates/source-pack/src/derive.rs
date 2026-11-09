//! Row derivation: the declarative step between "the rows the action
//! answered" and "the rows the table's columns read".
//!
//! Every pack before the meeting packs mapped columns onto keys the wire
//! already carried. A pack that must land a provider's answer in a shape
//! shared with OTHER providers (the meeting-minutes contract: one
//! `meetings` / `meeting_notes` / `meeting_action_items` shape for Zoom,
//! Google Meet, Microsoft Teams and Feishu) needs four things no wire key
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
//!   failed scan and not a row of NULLs.
//!
//! A [`RowDerivation`] declares those four, and nothing else: no
//! expressions, no lookups, no reading of prose. It rewrites rows as JSON
//! and hands them to the SAME converter every other table uses, so a
//! derived column has the same typing, nullability and error identity as a
//! wire column. Steps run in a fixed order — skip, explode, constants,
//! format — so a template can read an exploded element's position and a
//! constant can never be shadowed by a wire key of the same name (derived
//! keys overwrite; the pack's label is the pack's, not the provider's).
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
/// scan with its kind. When no path supplies elements the source row
/// yields zero rows. Each element lands under `into` on a copy of the
/// source row (whose own keys stay reachable), and its 0-based position
/// under `index` when one is named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Explode {
    /// Row-relative dotted paths, tried in order.
    pub paths: &'static [&'static str],
    /// The key each element is written under.
    pub into: &'static str,
    /// The key the element's 0-based position is written under, if any.
    pub index: Option<&'static str>,
}

/// The derivation a table declares; see the module doc for the four steps
/// and their order. `Default` is the no-op derivation, which `apply` turns
/// into a plain clone of the rows — callers check [`Self::is_empty`] to
/// skip even that.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct RowDerivation {
    /// Drop every source row where this row-relative path is missing or
    /// null (a null parent counts as missing). A present value of another
    /// shape keeps the row.
    pub skip_when_null: Option<&'static str>,
    /// One row per array element; see [`Explode`].
    pub explode: Option<Explode>,
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
    /// placeholder fails the scan with its kind.
    pub format: &'static [(&'static str, &'static str)],
}

impl RowDerivation {
    /// Whether the derivation declares nothing — the shape every pre-existing
    /// pack table has, for which the scan hands the wire rows straight to
    /// the converter.
    pub fn is_empty(&self) -> bool {
        self.skip_when_null.is_none()
            && self.explode.is_none()
            && self.constants.is_empty()
            && self.format.is_empty()
    }

    /// Every key the derivation writes onto a row, in declaration order:
    /// the explode's `into` and `index`, then the constants, then the
    /// formats. The loader cross-checks these against the table's columns.
    pub fn written_keys(&self) -> Vec<&'static str> {
        let mut keys = Vec::new();
        if let Some(explode) = &self.explode {
            keys.push(explode.into);
            keys.extend(explode.index);
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
                    for path in explode.paths {
                        match resolve(source, path) {
                            Ok(None) => {}
                            Ok(Some(Value::Array(items))) => {
                                if !items.is_empty() {
                                    elements = Some(items.as_slice());
                                    break;
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
                        Some(items) => items
                            .iter()
                            .enumerate()
                            .map(|(position, item)| {
                                let mut row = source.clone();
                                row.insert(explode.into.to_string(), item.clone());
                                if let Some(index) = explode.index {
                                    row.insert(index.to_string(), Value::from(position as u64));
                                }
                                row
                            })
                            .collect(),
                    }
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
}
