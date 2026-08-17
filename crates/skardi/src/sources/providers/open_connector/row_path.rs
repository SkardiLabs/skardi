//! Row-path parsing and extraction.
//!
//! A source-pack table declares a fixed *row path* — the JSON location of
//! the row array inside an action's response envelope (`$.issues`,
//! `$.data.items`). Paths are deliberately limited to object-key segments:
//! they are relational contracts maintained by Skardi, not a user query
//! language, so arrays/wildcards are out of scope.

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
