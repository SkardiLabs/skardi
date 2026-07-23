//! Typed pagination strategies.
//!
//! Source packs declare pagination with typed strategies rather than
//! callbacks, so the engine can inject page inputs, read continuation state
//! from responses, and — critically — verify that every strategy actually
//! advances. A repeated cursor fails the scan instead of looping forever.

use std::collections::HashSet;

use serde_json::{Map, Value};

use super::error::OpenConnectorError;
use super::row_path::RowPath;

/// How a source-pack table paginates.
#[derive(Debug, Clone, Copy)]
pub enum PaginationStrategy {
    /// Page-number pagination: the request carries a 1-based page number and
    /// a page size; a short or empty page ends the scan.
    PageNumber {
        /// Action input field for the page number.
        page_param: &'static str,
        /// Action input field for the page size.
        per_page_param: &'static str,
        /// Page size to request (also the limit-pushdown ceiling).
        per_page: u32,
    },
    /// Cursor pagination: the request carries the previous page's cursor
    /// (absent on the first page); the response carries the next cursor at a
    /// fixed path, and an absent/empty cursor ends the scan.
    Cursor {
        /// Action input field for the cursor.
        cursor_param: &'static str,
        /// Row-path-style location of the next cursor in the response
        /// envelope, e.g. `$.next_cursor`.
        next_cursor_path: &'static str,
        /// Optional action input field for the page size.
        page_size_param: Option<&'static str>,
        /// Page size to request (ignored when `page_size_param` is None).
        page_size: u32,
    },
}

/// Mutable pagination state for one scan.
#[derive(Debug)]
pub struct Pagination {
    strategy: PaginationStrategy,
    /// 1-based number of the page about to be requested.
    page: usize,
    /// Page number (PageNumber) or next cursor (Cursor) for the next request.
    next_token: Option<String>,
    /// Cursors already consumed, for loop detection.
    seen_tokens: HashSet<String>,
    /// Pre-parsed next-cursor path (Cursor only) — parsed once at
    /// construction instead of on every page.
    cursor_path: Option<RowPath>,
}

impl PaginationStrategy {
    /// Validate any embedded paths. Called at binding time so a malformed
    /// pack-authored path fails at registration, not mid-scan.
    pub fn validate(&self) -> Result<(), OpenConnectorError> {
        if let PaginationStrategy::Cursor {
            next_cursor_path, ..
        } = self
        {
            RowPath::parse(next_cursor_path)?;
        }
        Ok(())
    }
}

impl Pagination {
    /// Start a scan at page 1, parsing any strategy paths exactly once.
    ///
    /// # Errors
    /// [`OpenConnectorError::InvalidRowPath`] when a Cursor strategy's
    /// `next_cursor_path` is malformed (a pack bug).
    pub fn new(strategy: PaginationStrategy) -> Result<Self, OpenConnectorError> {
        let next_token = match &strategy {
            PaginationStrategy::PageNumber { .. } => Some("1".to_string()),
            PaginationStrategy::Cursor { .. } => None,
        };
        let cursor_path = match &strategy {
            PaginationStrategy::Cursor {
                next_cursor_path, ..
            } => Some(RowPath::parse(next_cursor_path)?),
            PaginationStrategy::PageNumber { .. } => None,
        };
        Ok(Self {
            strategy,
            page: 1,
            next_token,
            seen_tokens: HashSet::new(),
            cursor_path,
        })
    }

    /// The 1-based number of the page about to be requested.
    pub fn page(&self) -> usize {
        self.page
    }

    /// Inject this page's parameters into the action input object.
    pub fn apply(&self, input: &mut Map<String, Value>) {
        match &self.strategy {
            PaginationStrategy::PageNumber {
                page_param,
                per_page_param,
                per_page,
            } => {
                let page: u64 = self
                    .next_token
                    .as_deref()
                    .and_then(|t| t.parse().ok())
                    .unwrap_or(1);
                input.insert((*page_param).to_string(), Value::from(page));
                input.insert((*per_page_param).to_string(), Value::from(*per_page));
            }
            PaginationStrategy::Cursor {
                cursor_param,
                page_size_param,
                page_size,
                ..
            } => {
                if let Some(token) = &self.next_token {
                    input.insert((*cursor_param).to_string(), Value::from(token.as_str()));
                }
                if let Some(param) = page_size_param {
                    input.insert((*param).to_string(), Value::from(*page_size));
                }
            }
        }
    }

    /// Advance after a fetched page. Returns `true` when another page should
    /// be requested, `false` when the scan is complete.
    ///
    /// `envelope` is the raw action response; `rows_in_page` is the number of
    /// rows extracted from it (post row-path).
    pub fn advance(
        &mut self,
        envelope: &Value,
        rows_in_page: usize,
    ) -> Result<bool, OpenConnectorError> {
        match &self.strategy {
            PaginationStrategy::PageNumber { per_page, .. } => {
                // Short or empty page → last page. Page numbers advance by
                // construction, so no loop detection is needed here.
                let more = rows_in_page >= *per_page as usize;
                if more {
                    self.page += 1;
                    self.next_token = Some(self.page.to_string());
                }
                Ok(more)
            }
            PaginationStrategy::Cursor { .. } => {
                let path = self.cursor_path.as_ref().ok_or_else(|| {
                    OpenConnectorError::InvalidRowPath {
                        path: "<cursor>".to_string(),
                        reason: "cursor strategy without a parsed path".to_string(),
                    }
                })?;
                let next = match path.extract(envelope, self.page) {
                    Ok(Value::String(s)) if !s.is_empty() => Some(s.clone()),
                    // Missing, null, or empty cursor → scan complete.
                    _ => None,
                };

                let Some(next) = next else {
                    return Ok(false);
                };
                if !self.seen_tokens.insert(next.clone()) {
                    return Err(OpenConnectorError::PaginationLoop { token: next });
                }
                self.page += 1;
                self.next_token = Some(next);
                Ok(true)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn page_number(per_page: u32) -> Pagination {
        Pagination::new(PaginationStrategy::PageNumber {
            page_param: "page",
            per_page_param: "per_page",
            per_page,
        })
        .unwrap()
    }

    fn cursor() -> Pagination {
        Pagination::new(PaginationStrategy::Cursor {
            cursor_param: "cursor",
            next_cursor_path: "$.next_cursor",
            page_size_param: Some("limit"),
            page_size: 50,
        })
        .unwrap()
    }

    #[test]
    fn malformed_cursor_path_fails_at_construction() {
        let err = Pagination::new(PaginationStrategy::Cursor {
            cursor_param: "cursor",
            next_cursor_path: "not-a-path",
            page_size_param: None,
            page_size: 50,
        })
        .unwrap_err();
        assert!(matches!(err, OpenConnectorError::InvalidRowPath { .. }));
    }

    #[test]
    fn validate_rejects_malformed_cursor_path() {
        let bad = PaginationStrategy::Cursor {
            cursor_param: "cursor",
            next_cursor_path: "$.a..b",
            page_size_param: None,
            page_size: 50,
        };
        assert!(matches!(
            bad.validate(),
            Err(OpenConnectorError::InvalidRowPath { .. })
        ));
        cursor().strategy.validate().expect("valid strategy");
    }

    #[test]
    fn page_number_injects_params_and_stops_on_short_page() {
        let mut pagination = page_number(2);
        let mut input = Map::new();
        pagination.apply(&mut input);
        assert_eq!(input.get("page"), Some(&json!(1)));
        assert_eq!(input.get("per_page"), Some(&json!(2)));

        // Full page → advance to page 2.
        assert!(pagination.advance(&json!({}), 2).unwrap());
        let mut input = Map::new();
        pagination.apply(&mut input);
        assert_eq!(input.get("page"), Some(&json!(2)));

        // Short page → done.
        assert!(!pagination.advance(&json!({}), 1).unwrap());
    }

    #[test]
    fn page_number_stops_on_empty_page() {
        let mut pagination = page_number(10);
        assert!(!pagination.advance(&json!({}), 0).unwrap());
    }

    #[test]
    fn cursor_omits_token_on_first_page_then_follows() {
        let mut pagination = cursor();
        let mut input = Map::new();
        pagination.apply(&mut input);
        assert!(!input.contains_key("cursor"));
        assert_eq!(input.get("limit"), Some(&json!(50)));

        assert!(
            pagination
                .advance(&json!({"next_cursor": "c2"}), 50)
                .unwrap()
        );
        let mut input = Map::new();
        pagination.apply(&mut input);
        assert_eq!(input.get("cursor"), Some(&json!("c2")));
    }

    #[test]
    fn cursor_ends_on_missing_or_empty_next() {
        let mut pagination = cursor();
        assert!(!pagination.advance(&json!({}), 50).unwrap());

        let mut pagination = cursor();
        assert!(!pagination.advance(&json!({"next_cursor": ""}), 50).unwrap());
    }

    #[test]
    fn repeated_cursor_fails_as_loop() {
        let mut pagination = cursor();
        assert!(
            pagination
                .advance(&json!({"next_cursor": "same"}), 50)
                .unwrap()
        );
        // Gateway returns the same cursor again — must fail, not loop.
        let err = pagination
            .advance(&json!({"next_cursor": "same"}), 50)
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::PaginationLoop { ref token } if token == "same"
        ));
    }

    #[test]
    fn distinct_cursors_keep_advancing() {
        let mut pagination = cursor();
        assert!(
            pagination
                .advance(&json!({"next_cursor": "c2"}), 50)
                .unwrap()
        );
        assert!(
            pagination
                .advance(&json!({"next_cursor": "c3"}), 50)
                .unwrap()
        );
        assert!(
            !pagination
                .advance(&json!({"next_cursor": null}), 10)
                .unwrap()
        );
    }
}
