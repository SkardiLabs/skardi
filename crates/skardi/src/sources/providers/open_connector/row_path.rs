//! Row-path parsing and extraction.
//!
//! A source-pack table declares a fixed *row path* — the JSON location of
//! the rows inside an action's response envelope (`$.issues`,
//! `$.data.items`). Paths are deliberately limited to object-key segments:
//! they are relational contracts maintained by Skardi, not a user query
//! language, so arrays/wildcards are out of scope.
//!
//! Root `$` is a separate, narrower case: it is accepted ONLY when a table
//! declares `row_shape: object`, for actions whose whole response IS the
//! single row (a point read such as `feishu.get_document_content`). It is
//! parsed through [`RowPath::parse_object_root`], never through
//! [`RowPath::parse`], so the array contract stays exactly as strict as it
//! was for every table that locates a row array.

use serde_json::Value;

use super::error::OpenConnectorError;

/// A parsed row path (`$.key[.key…]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowPath {
    raw: String,
    segments: Vec<String>,
}

impl RowPath {
    /// Parse a row path. Segments must be non-empty object keys; the path
    /// must have at least one segment (`$` alone is not a row location).
    ///
    /// # Example
    /// ```
    /// use skardi::sources::providers::open_connector::row_path::RowPath;
    ///
    /// let path = RowPath::parse("$.data.items").unwrap();
    /// assert_eq!(path.as_str(), "$.data.items");
    /// assert!(RowPath::parse("data.items").is_err()); // missing `$.` prefix
    /// assert!(RowPath::parse("$").is_err()); // no segments
    /// ```
    pub fn parse(path: &str) -> Result<Self, OpenConnectorError> {
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
    /// Parse the root path `$` for an object-row table.
    ///
    /// Separate from [`RowPath::parse`] on purpose: making `$` valid there
    /// would silently legalise a rowless path for the 37 array-shaped
    /// tables, where it can only ever be a mistake. Only the object-row
    /// loader reaches this constructor, and only after it has checked that
    /// the table declares `row_shape: object`.
    ///
    /// # Example
    /// ```
    /// use skardi::sources::providers::open_connector::row_path::RowPath;
    ///
    /// let path = RowPath::parse_object_root("$").unwrap();
    /// assert_eq!(path.as_str(), "$");
    /// assert!(RowPath::parse_object_root("$.data").is_err()); // only root
    /// ```
    pub fn parse_object_root(path: &str) -> Result<Self, OpenConnectorError> {
        if path != "$" {
            return Err(OpenConnectorError::InvalidRowPath {
                path: path.to_string(),
                reason: "object row shape selects the response root; the only valid path is '$'"
                    .to_string(),
            });
        }
        Ok(Self {
            raw: path.to_string(),
            segments: Vec::new(),
        })
    }

    /// Extract the single row object at the path.
    ///
    /// A response that is null, an array, or a primitive fails loudly with
    /// [`OpenConnectorError::RowPathNotObject`] rather than degrading to an
    /// empty result: a point read that returns "no object" is a contract
    /// break at the gateway, and a silent zero-row scan would report it as
    /// "this document has no content".
    pub fn row_object<'a>(
        &self,
        value: &'a Value,
        page: usize,
    ) -> Result<&'a Value, OpenConnectorError> {
        let target = self.extract(value, page)?;
        if !target.is_object() {
            return Err(OpenConnectorError::RowPathNotObject {
                path: self.raw.clone(),
                segment: "<root>".to_string(),
                page,
                found: json_kind(target),
            });
        }
        Ok(target)
    }

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
        for bad in ["data.items", "$", "$.", "$.a..b", "$.a."] {
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
    fn object_root_parses_only_the_bare_root() {
        assert_eq!(RowPath::parse_object_root("$").unwrap().as_str(), "$");
        for bad in ["$.data", "data", "", "$$"] {
            assert!(
                matches!(
                    RowPath::parse_object_root(bad),
                    Err(OpenConnectorError::InvalidRowPath { .. })
                ),
                "{bad} should be rejected for object rows"
            );
        }
    }

    #[test]
    fn object_root_stays_invalid_for_the_array_parser() {
        // The whole point of a separate constructor: `$` must not become a
        // legal row path for the array-shaped tables.
        assert!(matches!(
            RowPath::parse("$"),
            Err(OpenConnectorError::InvalidRowPath { .. })
        ));
    }

    #[test]
    fn row_object_returns_the_response_root() {
        let page = json!({"documentId": "doc-1", "content": "hello"});
        let row = RowPath::parse_object_root("$")
            .unwrap()
            .row_object(&page, 1)
            .unwrap();
        assert_eq!(row, &page);
    }

    #[test]
    fn row_object_fails_loudly_on_null_array_and_primitive() {
        // A point read that did not return an object is a broken contract;
        // it must never degrade into an empty (zero-row) scan.
        for bad in [json!(null), json!([{"id": 1}]), json!("text"), json!(7)] {
            let err = RowPath::parse_object_root("$")
                .unwrap()
                .row_object(&bad, 1)
                .unwrap_err();
            assert!(
                matches!(err, OpenConnectorError::RowPathNotObject { .. }),
                "{bad} should fail as not-an-object, got {err}"
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
