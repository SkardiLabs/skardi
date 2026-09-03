//! Row-path parsing and extraction.
//!
//! A source-pack table declares a fixed *row path* — the JSON location of
//! the rows inside an action's response envelope (`$.issues`,
//! `$.data.items`, or `$` for the envelope itself). Paths are deliberately
//! limited to object-key segments: they are relational contracts maintained
//! by Skardi, not a user query language, so arrays/wildcards are out of
//! scope.
//!
//! The target is normally an array. It may instead be a single OBJECT that
//! is itself one row — the shape of every "read one thing" endpoint, where a
//! document's text or a file's content arrives as fields rather than as a
//! one-element list. Which of the two a table expects is DECLARED
//! (`row_shape`), never sniffed: inferring it would turn a mistyped path
//! that happens to land on an object into a silent one-row table instead of
//! the loud failure [`OpenConnectorError::RowPathNotArray`] exists to be.

use serde_json::Value;

use super::error::OpenConnectorError;

/// A parsed row path (`$.key[.key…]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowPath {
    raw: String,
    segments: Vec<String>,
}

impl RowPath {
    /// Parse a row path. Segments must be non-empty object keys.
    ///
    /// `$` alone is the envelope itself, and carries no segments. It is what
    /// a "read one thing" action needs: the response object IS the row, with
    /// no wrapper key to descend through. It used to be rejected, back when
    /// every target had to be an array — see the module doc for why the two
    /// shapes are declared rather than sniffed.
    ///
    /// # Example
    /// ```
    /// use skardi::sources::providers::open_connector::row_path::RowPath;
    ///
    /// let path = RowPath::parse("$.data.items").unwrap();
    /// assert_eq!(path.as_str(), "$.data.items");
    /// assert!(RowPath::parse("data.items").is_err()); // missing `$.` prefix
    /// assert!(RowPath::parse("$.").is_err()); // empty segment
    ///
    /// let envelope = RowPath::parse("$").unwrap(); // the whole response
    /// assert_eq!(envelope.segments().count(), 0);
    /// ```
    pub fn parse(path: &str) -> Result<Self, OpenConnectorError> {
        if path == "$" {
            return Ok(Self {
                raw: path.to_string(),
                segments: Vec::new(),
            });
        }
        let body = path
            .strip_prefix("$.")
            .ok_or_else(|| OpenConnectorError::InvalidRowPath {
                path: path.to_string(),
                reason: "must start with '$.'".to_string(),
            })?;

        let segments: Vec<String> = body.split('.').map(str::to_string).collect();
        if segments.iter().any(|s| s.is_empty()) {
            return Err(OpenConnectorError::InvalidRowPath {
                path: path.to_string(),
                reason: "segments must be non-empty object keys".to_string(),
            });
        }

        Ok(Self {
            raw: path.to_string(),
            segments,
        })
    }

    /// The path as written, e.g. `$.data.items`.
    pub fn as_str(&self) -> &str {
        &self.raw
    }

    /// The object-key segments, in traversal order.
    pub fn segments(&self) -> impl Iterator<Item = &str> {
        self.segments.iter().map(String::as_str)
    }

    /// Walk the path inside `value`. A key absent from an object fails with
    /// [`OpenConnectorError::RowPathNotFound`]; a present non-object where
    /// the path must descend fails with
    /// [`OpenConnectorError::RowPathNotObject`]. `page` is the 1-based page
    /// number, carried into the error for scan diagnostics.
    pub fn extract<'a>(
        &self,
        value: &'a Value,
        page: usize,
    ) -> Result<&'a Value, OpenConnectorError> {
        let mut current = value;
        for segment in &self.segments {
            let map = current
                .as_object()
                .ok_or_else(|| OpenConnectorError::RowPathNotObject {
                    path: self.raw.clone(),
                    segment: segment.clone(),
                    page,
                    found: json_kind(current),
                })?;
            current = map
                .get(segment)
                .ok_or_else(|| OpenConnectorError::RowPathNotFound {
                    path: self.raw.clone(),
                    segment: segment.clone(),
                    page,
                })?;
        }
        Ok(current)
    }

    /// Extract the row array at the path. An absent path fails with
    /// [`OpenConnectorError::RowPathNotFound`]; a present non-array target
    /// fails with [`OpenConnectorError::RowPathNotArray`].
    pub fn rows<'a>(
        &self,
        value: &'a Value,
        page: usize,
    ) -> Result<&'a [Value], OpenConnectorError> {
        let target = self.extract(value, page)?;
        target
            .as_array()
            .map(Vec::as_slice)
            .ok_or_else(|| OpenConnectorError::RowPathNotArray {
                path: self.raw.clone(),
                page,
                found: json_kind(target),
            })
    }

    /// Extract a single-object row at the path, as a one-element slice.
    ///
    /// The counterpart of [`Self::rows`] for a table whose action reads ONE
    /// thing: the target must be an object, and it is that row. An absent
    /// path fails with [`OpenConnectorError::RowPathNotFound`]; a present
    /// non-object fails with [`OpenConnectorError::RowPathNotObjectRow`].
    ///
    /// An array target is refused rather than accepted-and-flattened. A table
    /// that declares one row and receives a list has had its contract
    /// changed upstream, and reading the first element would publish a
    /// silently truncated table — the same class of harm as an undetected
    /// short page.
    pub fn single_row<'a>(
        &self,
        value: &'a Value,
        page: usize,
    ) -> Result<&'a [Value], OpenConnectorError> {
        let target = self.extract(value, page)?;
        if target.is_object() {
            Ok(std::slice::from_ref(target))
        } else {
            Err(OpenConnectorError::RowPathNotObjectRow {
                path: self.raw.clone(),
                page,
                found: json_kind(target),
            })
        }
    }
}

/// Short human-readable kind of a JSON value, for error messages —
/// the shared repo-wide vocabulary, in this module's String shape.
pub(crate) fn json_kind(value: &Value) -> String {
    crate::util::json::json_kind(value).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_dotted_paths() {
        let path = RowPath::parse("$.data.items").unwrap();
        assert_eq!(path.as_str(), "$.data.items");
    }

    #[test]
    fn rejects_paths_without_prefix_or_empty_segments() {
        for bad in ["data.items", "$.", "$.a..b", "$.a."] {
            assert!(
                matches!(
                    RowPath::parse(bad),
                    Err(OpenConnectorError::InvalidRowPath { .. })
                ),
                "{bad} should be rejected"
            );
        }
    }

    #[test]
    fn rows_extracts_nested_array() {
        let page = json!({"data": {"items": [{"id": 1}, {"id": 2}]}});
        let rows = RowPath::parse("$.data.items")
            .unwrap()
            .rows(&page, 1)
            .unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn missing_key_fails_with_segment_and_page() {
        let page = json!({"data": {}});
        let err = RowPath::parse("$.data.items")
            .unwrap()
            .rows(&page, 3)
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::RowPathNotFound { ref segment, page: 3, .. }
                if segment == "items"
        ));
    }

    #[test]
    fn traversing_a_non_object_fails() {
        let page = json!({"data": [1, 2, 3]});
        let err = RowPath::parse("$.data.items")
            .unwrap()
            .rows(&page, 1)
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::RowPathNotObject { ref segment, ref found, .. }
                if segment == "items" && found == "an array"
        ));
    }

    /// `$` is the envelope itself — the only path a "read one thing" action
    /// can name, since its response object IS the row with no wrapper key.
    /// It was rejected for as long as every target had to be an array.
    #[test]
    fn the_bare_dollar_is_the_envelope() {
        let path = RowPath::parse("$").unwrap();
        assert_eq!(path.as_str(), "$");
        assert_eq!(path.segments().count(), 0);
        let page = json!({"documentId": "d1", "content": "hello"});
        assert_eq!(path.extract(&page, 1).unwrap(), &page);
    }

    /// The single-object shape, and its symmetry with `rows`: one object
    /// becomes one row, and the two error variants name opposite
    /// expectations so a reader can tell "upstream started returning a list"
    /// from "this table's declared shape is wrong".
    #[test]
    fn single_row_takes_one_object_and_refuses_a_list() {
        let page = json!({"documentId": "d1", "content": "hello"});
        let rows = RowPath::parse("$").unwrap().single_row(&page, 1).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0], page);

        // Nested is fine too — the shape is about the TARGET, not the depth.
        let wrapped = json!({"data": {"id": 7}});
        let rows = RowPath::parse("$.data")
            .unwrap()
            .single_row(&wrapped, 1)
            .unwrap();
        assert_eq!(rows.len(), 1);

        // An array target is refused rather than silently truncated to its
        // first element.
        let listed = json!({"data": [{"id": 1}, {"id": 2}]});
        let err = RowPath::parse("$.data")
            .unwrap()
            .single_row(&listed, 4)
            .unwrap_err();
        assert!(
            matches!(
                err,
                OpenConnectorError::RowPathNotObjectRow { page: 4, ref found, .. }
                    if found == "an array"
            ),
            "{err}"
        );

        // And the two shapes disagree in both directions: what `rows`
        // accepts, `single_row` refuses, and vice versa.
        let path = RowPath::parse("$.data").unwrap();
        assert!(path.rows(&listed, 1).is_ok());
        assert!(path.single_row(&listed, 1).is_err());
        assert!(path.rows(&wrapped, 1).is_err());
        assert!(path.single_row(&wrapped, 1).is_ok());
    }

    #[test]
    fn non_array_target_fails_with_kind() {
        let page = json!({"items": {"count": 2}});
        let err = RowPath::parse("$.items")
            .unwrap()
            .rows(&page, 2)
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::RowPathNotArray { page: 2, ref found, .. }
                if found == "an object"
        ));
    }
}
