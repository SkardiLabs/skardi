//! Typed pagination strategies.
//!
//! Source packs declare pagination with typed strategies rather than
//! callbacks, so the engine can inject page inputs, read continuation state
//! from responses, and — critically — verify that every strategy actually
//! advances. A repeated cursor fails the scan instead of looping forever.

use std::collections::HashSet;

use serde_json::{Map, Value};

use super::error::OpenConnectorError;
use super::row_path::{RowPath, json_kind};

/// How a source-pack table paginates.
#[derive(Debug, Clone, Copy)]
pub enum PaginationStrategy {
    /// Page-number pagination: the request carries a 1-based page number and
    /// a page size.
    ///
    /// Termination: when `total_pages_path` is declared, the scan trusts the
    /// provider's authoritative page count and only ends once
    /// `page >= total pages` — a short or even empty non-final page keeps
    /// going, because providers that filter rows *after* paginating
    /// (permission checks, deletions) can legally return short middle pages.
    /// When `raw_page_size_path` is declared, the scan trusts the provider's
    /// reported RAW page length (the count before any post-pagination
    /// filtering) and continues while it equals the requested page size —
    /// the signal Open Connector's `github.list_repository_issues`
    /// publishes as `$.pageInfo.fetched` (oomol-lab/open-connector#228),
    /// whose filtered row array says nothing about whether more pages
    /// exist.
    ///
    /// Without either path, the short/empty-page heuristic ends the scan —
    /// sound only when the returned rows ARE the raw page.
    PageNumber {
        /// Action input field for the page number.
        page_param: &'static str,
        /// Action input field for the page size.
        per_page_param: &'static str,
        /// Page size to request (also the limit-pushdown ceiling).
        per_page: u32,
        /// Row-path-style location of the provider's total page count in the
        /// response envelope (e.g. Slack's `$.paging.pages`). Mutually
        /// exclusive with `raw_page_size_path`; with neither, the heuristic
        /// applies.
        total_pages_path: Option<&'static str>,
        /// Row-path-style location of the raw (pre-filter) page length in
        /// the response envelope (e.g. `$.pageInfo.fetched`).
        raw_page_size_path: Option<&'static str>,
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
        /// Row-path-style location of the provider's authoritative
        /// has-more boolean (e.g. Feishu's `$.hasMore`). Some providers
        /// return a NON-empty cursor on the final page and signal the end
        /// only here — Feishu's wiki `list_spaces` answers `has_more:
        /// false` with `page_token: "0||…"`, so null-cursor termination
        /// alone would refetch and fail as a `PaginationLoop`. When
        /// declared: `false` ends the scan regardless of the cursor;
        /// `true` requires a usable cursor, and its absence is contract
        /// drift (`PaginationCursorInvalid`), never a quiet stop. A
        /// non-boolean value fails as `PaginationHasMoreInvalid`. When
        /// absent, the cursor's null/empty/missing spellings terminate as
        /// before.
        has_more_path: Option<&'static str>,
    },
    /// One request, one page: no pagination inputs are injected and the scan
    /// completes after the first response. Used by `open_connector_scan`,
    /// whose raw actions declare no pagination contract — callers pass any
    /// paging inputs explicitly in the action input JSON.
    SinglePage {
        /// Where the provider would spell "there is more", when the action
        /// has such a field. Never used to fetch: this strategy issues one
        /// request either way. It exists to CHECK the premise — a live
        /// continuation there means one request is not the whole
        /// collection, and quietly stopping would be this engine's only
        /// silent truncation (`SinglePageIncomplete` instead).
        ///
        /// `None` keeps the historic behaviour, which is what
        /// `open_connector_scan` needs: its raw actions declare no
        /// pagination contract for the engine to check against, and their
        /// callers drive paging through the action input themselves.
        next_cursor_path: Option<&'static str>,
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
    /// Pre-parsed total-pages path (PageNumber only), same discipline.
    total_pages_path: Option<RowPath>,
    /// Pre-parsed raw-page-size path (PageNumber only).
    raw_page_size_path: Option<RowPath>,
    /// Pre-parsed has-more path (Cursor only, when declared).
    has_more_path: Option<RowPath>,
}

impl PaginationStrategy {
    /// Validate any embedded paths. Called at binding time so a malformed
    /// pack-authored path fails at registration, not mid-scan.
    pub fn validate(&self) -> Result<(), OpenConnectorError> {
        match self {
            PaginationStrategy::Cursor {
                next_cursor_path,
                has_more_path,
                ..
            } => {
                RowPath::parse(next_cursor_path)?;
                if let Some(path) = has_more_path {
                    RowPath::parse(path)?;
                }
            }
            PaginationStrategy::PageNumber {
                total_pages_path,
                raw_page_size_path,
                ..
            } => {
                if let Some(path) = total_pages_path {
                    RowPath::parse(path)?;
                }
                if let Some(path) = raw_page_size_path {
                    RowPath::parse(path)?;
                }
                // Two authoritative signals cannot coexist: a page where
                // they disagree would have no defensible winner.
                if total_pages_path.is_some() && raw_page_size_path.is_some() {
                    return Err(OpenConnectorError::InvalidRowPath {
                        path: "<pagination>".to_string(),
                        reason: "total_pages_path and raw_page_size_path are mutually \
                                 exclusive; declare the one signal the provider makes \
                                 authoritative"
                            .to_string(),
                    });
                }
            }
            // The premise-check path is pack-authored too, so a typo in it
            // must fail at registration like any other — never leave the
            // check silently unarmed until a scan happens to run.
            PaginationStrategy::SinglePage {
                next_cursor_path: Some(path),
            } => {
                RowPath::parse(path)?;
            }
            _ => {}
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
            PaginationStrategy::Cursor { .. } | PaginationStrategy::SinglePage { .. } => None,
        };
        let cursor_path = match &strategy {
            PaginationStrategy::Cursor {
                next_cursor_path, ..
            } => Some(RowPath::parse(next_cursor_path)?),
            // Parsed the same way, read for a different purpose: the
            // single-page premise check, never to fetch a next page.
            PaginationStrategy::SinglePage {
                next_cursor_path: Some(path),
            } => Some(RowPath::parse(path)?),
            PaginationStrategy::PageNumber { .. } | PaginationStrategy::SinglePage { .. } => None,
        };
        let total_pages_path = match &strategy {
            PaginationStrategy::PageNumber {
                total_pages_path: Some(path),
                ..
            } => Some(RowPath::parse(path)?),
            _ => None,
        };
        let raw_page_size_path = match &strategy {
            PaginationStrategy::PageNumber {
                raw_page_size_path: Some(path),
                ..
            } => Some(RowPath::parse(path)?),
            _ => None,
        };
        let has_more_path = match &strategy {
            PaginationStrategy::Cursor {
                has_more_path: Some(path),
                ..
            } => Some(RowPath::parse(path)?),
            _ => None,
        };
        Ok(Self {
            strategy,
            page: 1,
            next_token,
            seen_tokens: HashSet::new(),
            cursor_path,
            total_pages_path,
            raw_page_size_path,
            has_more_path,
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
                ..
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
            PaginationStrategy::SinglePage { .. } => {}
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
                // Page numbers advance by construction, so no loop detection
                // is needed here.
                let more = match &self.total_pages_path {
                    // Authoritative total: only page >= pages ends the scan.
                    // A short or empty non-final page keeps going — providers
                    // that filter rows after paginating can legally return
                    // them, and the heuristic would silently truncate.
                    Some(path) => {
                        let total = path.extract(envelope, self.page)?;
                        let total = total.as_u64().ok_or_else(|| {
                            OpenConnectorError::PaginationTotalInvalid {
                                path: path.as_str().to_string(),
                                page: self.page,
                                found: json_kind(total),
                            }
                        })?;
                        (self.page as u64) < total
                    }
                    // Raw page length: the provider filters rows AFTER
                    // paginating but reports how many it fetched — continue
                    // while the raw page was full, no matter how short (or
                    // empty) the filtered row array is.
                    None => match &self.raw_page_size_path {
                        Some(path) => {
                            let fetched = path.extract(envelope, self.page)?;
                            let fetched = fetched.as_u64().ok_or_else(|| {
                                OpenConnectorError::PaginationRawPageSizeInvalid {
                                    path: path.as_str().to_string(),
                                    page: self.page,
                                    found: json_kind(fetched),
                                }
                            })?;
                            fetched >= u64::from(*per_page)
                        }
                        // Heuristic: short or empty page → last page (all
                        // the signal the provider gives).
                        None => rows_in_page >= *per_page as usize,
                    },
                };
                if more {
                    self.page += 1;
                    self.next_token = Some(self.page.to_string());
                }
                Ok(more)
            }
            PaginationStrategy::Cursor { .. } => {
                // The authoritative has-more signal, when the pack declares
                // one, is consulted FIRST: some providers return a non-empty
                // cursor on the final page (Feishu wiki spaces answer
                // `has_more: false` with `page_token: "0||…"`), so the
                // cursor's spellings alone would refetch a finished scan.
                if let Some(path) = &self.has_more_path {
                    let has_more = match path.extract(envelope, self.page) {
                        Ok(Value::Bool(b)) => *b,
                        Ok(other) => {
                            return Err(OpenConnectorError::PaginationHasMoreInvalid {
                                path: path.as_str().to_string(),
                                page: self.page,
                                found: json_kind(other).to_string(),
                            });
                        }
                        // The pack declared the signal; a page without it is
                        // contract drift, not a quiet stop or continue.
                        Err(OpenConnectorError::RowPathNotFound { .. }) => {
                            return Err(OpenConnectorError::PaginationHasMoreInvalid {
                                path: path.as_str().to_string(),
                                page: self.page,
                                found: "absent".to_string(),
                            });
                        }
                        Err(e) => return Err(e),
                    };
                    if !has_more {
                        return Ok(false);
                    }
                }

                let path = self.cursor_path.as_ref().ok_or_else(|| {
                    OpenConnectorError::InvalidRowPath {
                        path: "<cursor>".to_string(),
                        reason: "cursor strategy without a parsed path".to_string(),
                    }
                })?;
                let next = match path.extract(envelope, self.page) {
                    Ok(Value::String(s)) if !s.is_empty() => Some(s.clone()),
                    // Null or empty-string cursor → scan complete (the two
                    // in-band end-of-collection spellings).
                    Ok(Value::String(_)) | Ok(Value::Null) => None,
                    // A cursor that is present but not a string is contract
                    // drift, not termination — reading it as end-of-collection
                    // would silently truncate the scan.
                    Ok(other) => {
                        return Err(OpenConnectorError::PaginationCursorInvalid {
                            path: path.as_str().to_string(),
                            page: self.page,
                            found: json_kind(other),
                        });
                    }
                    // An entirely absent cursor (any missing segment) is the
                    // omitted end-of-collection spelling.
                    Err(OpenConnectorError::RowPathNotFound { .. }) => None,
                    // Structural failures — traversing through a non-object —
                    // are drift and propagate as themselves.
                    Err(e) => return Err(e),
                };

                let Some(next) = next else {
                    // With a declared has-more signal saying `true`, a missing
                    // cursor is drift — stopping here would silently truncate
                    // the scan the provider says is unfinished.
                    if self.has_more_path.is_some() {
                        return Err(OpenConnectorError::PaginationCursorInvalid {
                            path: path.as_str().to_string(),
                            page: self.page,
                            found: "null, empty, or absent while the has-more signal is true"
                                .to_string(),
                        });
                    }
                    return Ok(false);
                };
                if !self.seen_tokens.insert(next.clone()) {
                    return Err(OpenConnectorError::PaginationLoop { token: next });
                }
                self.page += 1;
                self.next_token = Some(next);
                Ok(true)
            }
            PaginationStrategy::SinglePage { .. } => {
                // One request, one page — but when the pack declared where
                // the provider spells "there is more", VERIFY that rather
                // than assume it. Undeclared (raw scans) keeps the historic
                // unconditional stop.
                let Some(path) = &self.cursor_path else {
                    return Ok(false);
                };
                match path.extract(envelope, self.page) {
                    // The premise holds: the same three spellings the cursor
                    // strategy reads as end-of-collection.
                    Err(OpenConnectorError::RowPathNotFound { .. }) | Ok(Value::Null) => Ok(false),
                    Ok(Value::String(token)) if token.is_empty() => Ok(false),
                    // Anything else is the provider saying the collection
                    // continues. This strategy cannot follow it — issuing a
                    // second request would send pagination inputs the strict
                    // schema rejects — so the scan fails rather than passing
                    // off a partial answer as complete.
                    Ok(other) => Err(OpenConnectorError::SinglePageIncomplete {
                        path: path.as_str().to_string(),
                        page: self.page,
                        found: match other {
                            Value::String(_) => "a continuation token".to_string(),
                            other => format!("{} where a token would be", json_kind(other)),
                        },
                    }),
                    // Structural failures — traversing through a non-object —
                    // are drift and propagate as themselves.
                    Err(e) => Err(e),
                }
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
            total_pages_path: None,
            raw_page_size_path: None,
        })
        .unwrap()
    }

    fn cursor() -> Pagination {
        Pagination::new(PaginationStrategy::Cursor {
            cursor_param: "cursor",
            next_cursor_path: "$.next_cursor",
            page_size_param: Some("limit"),
            page_size: 50,
            has_more_path: None,
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
            has_more_path: None,
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
            has_more_path: None,
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

    fn page_number_with_raw(per_page: u32) -> Pagination {
        Pagination::new(PaginationStrategy::PageNumber {
            page_param: "page",
            per_page_param: "perPage",
            per_page,
            total_pages_path: None,
            raw_page_size_path: Some("$.pageInfo.fetched"),
        })
        .unwrap()
    }

    #[test]
    fn raw_page_size_drives_termination_regardless_of_filtered_rows() {
        // Full raw page + short (even empty) filtered rows → continue: the
        // provider filtered rows AFTER paginating, so the filtered count
        // says nothing. Short raw page → done, even if rows LOOK full.
        let mut pagination = page_number_with_raw(100);
        assert!(
            pagination
                .advance(&json!({"pageInfo": {"fetched": 100}}), 37)
                .unwrap(),
            "full raw page with a short filtered page continues"
        );
        assert!(
            pagination
                .advance(&json!({"pageInfo": {"fetched": 100}}), 0)
                .unwrap(),
            "an all-filtered (empty) page continues while the raw page was full"
        );
        assert!(
            !pagination
                .advance(&json!({"pageInfo": {"fetched": 99}}), 99)
                .unwrap(),
            "a short raw page terminates"
        );
    }

    #[test]
    fn missing_or_invalid_raw_page_size_fails_the_scan() {
        // The declared signal going missing or changing type is drift, not
        // end-of-collection — reading it as termination would truncate.
        let mut pagination = page_number_with_raw(100);
        assert!(matches!(
            pagination.advance(&json!({"issues": []}), 0),
            Err(OpenConnectorError::RowPathNotFound { .. })
        ));

        let mut pagination = page_number_with_raw(100);
        assert!(matches!(
            pagination.advance(&json!({"pageInfo": {"fetched": "100"}}), 0),
            Err(OpenConnectorError::PaginationRawPageSizeInvalid { page: 1, ref found, .. })
                if found == "a string"
        ));
    }

    #[test]
    fn total_and_raw_page_size_are_mutually_exclusive() {
        let err = PaginationStrategy::PageNumber {
            page_param: "page",
            per_page_param: "perPage",
            per_page: 100,
            total_pages_path: Some("$.paging.pages"),
            raw_page_size_path: Some("$.pageInfo.fetched"),
        }
        .validate()
        .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::InvalidRowPath { ref reason, .. }
                if reason.contains("mutually")
        ));
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
    fn cursor_ends_on_missing_null_or_empty_next() {
        let mut pagination = cursor();
        assert!(!pagination.advance(&json!({}), 50).unwrap());

        let mut pagination = cursor();
        assert!(!pagination.advance(&json!({"next_cursor": ""}), 50).unwrap());

        let mut pagination = cursor();
        assert!(
            !pagination
                .advance(&json!({"next_cursor": null}), 50)
                .unwrap()
        );
    }

    #[test]
    fn non_string_cursors_fail_instead_of_terminating() {
        // `next_cursor: 123` (or an object) is contract drift: reading it as
        // end-of-collection would return a truncated scan as success.
        for (envelope, kind) in [
            (json!({"next_cursor": 123}), "a number"),
            (json!({"next_cursor": {}}), "an object"),
            (json!({"next_cursor": true}), "a boolean"),
        ] {
            let mut pagination = cursor();
            let err = pagination.advance(&envelope, 50).unwrap_err();
            assert!(
                matches!(
                    err,
                    OpenConnectorError::PaginationCursorInvalid { page: 1, ref found, .. }
                        if found == kind
                ),
                "{envelope} should fail with kind {kind}, got {err}"
            );
        }
    }

    #[test]
    fn structural_cursor_path_failures_propagate() {
        // A non-object where the cursor path must descend is drift, not the
        // omitted end-of-collection spelling.
        let mut pagination = Pagination::new(PaginationStrategy::Cursor {
            cursor_param: "cursor",
            next_cursor_path: "$.meta.next",
            page_size_param: None,
            page_size: 50,
            has_more_path: None,
        })
        .unwrap();
        let err = pagination
            .advance(&json!({"meta": [1, 2]}), 50)
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::RowPathNotObject { ref segment, .. } if segment == "next"
        ));

        // A missing PARENT segment stays a termination: Slack's raw shape
        // omits the whole `response_metadata` object on the last page.
        let mut pagination = Pagination::new(PaginationStrategy::Cursor {
            cursor_param: "cursor",
            next_cursor_path: "$.meta.next",
            page_size_param: None,
            page_size: 50,
            has_more_path: None,
        })
        .unwrap();
        assert!(!pagination.advance(&json!({"other": 1}), 50).unwrap());
    }

    /// A cursor strategy with a declared has-more signal.
    fn cursor_with_has_more() -> Pagination {
        Pagination::new(PaginationStrategy::Cursor {
            cursor_param: "pageToken",
            next_cursor_path: "$.pageToken",
            page_size_param: Some("pageSize"),
            page_size: 50,
            has_more_path: Some("$.hasMore"),
        })
        .unwrap()
    }

    #[test]
    fn has_more_false_terminates_even_with_a_nonempty_cursor() {
        // The Feishu wiki shape this field exists for: the final page still
        // carries a non-empty page token ("0||…"), and only has_more says
        // the scan is over. Null-cursor termination alone would refetch and
        // trip the loop guard.
        let mut pagination = cursor_with_has_more();
        assert!(
            !pagination
                .advance(
                    &json!({"hasMore": false, "pageToken": "0||7027059242666328066"}),
                    1
                )
                .unwrap()
        );
    }

    #[test]
    fn has_more_true_continues_with_the_cursor() {
        let mut pagination = cursor_with_has_more();
        assert!(
            pagination
                .advance(&json!({"hasMore": true, "pageToken": "tok-2"}), 50)
                .unwrap()
        );
        let mut input = Map::new();
        pagination.apply(&mut input);
        assert_eq!(input.get("pageToken"), Some(&Value::from("tok-2")));
    }

    #[test]
    fn has_more_true_without_a_cursor_is_drift_not_termination() {
        // The provider says more pages exist but hands nothing to follow —
        // stopping would silently truncate a scan the signal says is
        // unfinished.
        for envelope in [
            json!({"hasMore": true, "pageToken": null}),
            json!({"hasMore": true}),
        ] {
            let mut pagination = cursor_with_has_more();
            let err = pagination.advance(&envelope, 50).unwrap_err();
            assert!(
                matches!(
                    err,
                    OpenConnectorError::PaginationCursorInvalid { ref found, .. }
                        if found.contains("has-more")
                ),
                "got {err}"
            );
        }
    }

    #[test]
    fn non_boolean_or_absent_has_more_fails_with_its_kind() {
        // The pack declared the signal, so a page without a usable one is
        // contract drift — guessing either way could truncate or loop.
        for (envelope, found) in [
            (json!({"hasMore": "yes", "pageToken": "t"}), "a string"),
            (json!({"hasMore": 1, "pageToken": "t"}), "a number"),
            (json!({"pageToken": "t"}), "absent"),
        ] {
            let mut pagination = cursor_with_has_more();
            let err = pagination.advance(&envelope, 50).unwrap_err();
            assert!(
                matches!(
                    err,
                    OpenConnectorError::PaginationHasMoreInvalid { found: ref f, .. } if f == found
                ),
                "for {envelope}: got {err}"
            );
        }
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

    fn page_number_with_total(per_page: u32) -> Pagination {
        Pagination::new(PaginationStrategy::PageNumber {
            page_param: "page",
            per_page_param: "count",
            per_page,
            total_pages_path: Some("$.paging.pages"),
            raw_page_size_path: None,
        })
        .unwrap()
    }

    #[test]
    fn total_pages_termination_survives_short_and_empty_middle_pages() {
        // Providers that filter rows after paginating can legally return
        // short — even empty — non-final pages; with an authoritative page
        // count the scan must keep going until page >= pages.
        let mut pagination = page_number_with_total(2);
        let envelope = json!({"paging": {"pages": 3}});
        assert!(pagination.advance(&envelope, 2).unwrap(), "full page 1");
        assert!(
            pagination.advance(&envelope, 1).unwrap(),
            "short page 2 continues"
        );
        assert!(
            !pagination.advance(&envelope, 0).unwrap(),
            "page 3 is the last"
        );

        let mut pagination = page_number_with_total(2);
        assert!(
            pagination
                .advance(&json!({"paging": {"pages": 2}}), 0)
                .unwrap(),
            "an empty non-final page continues"
        );

        // pages: 0 (empty collection) stops immediately.
        let mut pagination = page_number_with_total(2);
        assert!(
            !pagination
                .advance(&json!({"paging": {"pages": 0}}), 0)
                .unwrap()
        );
    }

    #[test]
    fn missing_or_non_numeric_totals_fail_the_scan() {
        // A declared total-pages location is a contract: silence or a wrong
        // kind must fail loudly, never fall back to the truncating
        // heuristic.
        let mut pagination = page_number_with_total(2);
        let err = pagination.advance(&json!({"ok": true}), 2).unwrap_err();
        assert!(matches!(err, OpenConnectorError::RowPathNotFound { .. }));

        let mut pagination = page_number_with_total(2);
        let err = pagination
            .advance(&json!({"paging": {"pages": "three"}}), 2)
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::PaginationTotalInvalid { ref found, page: 1, .. }
                if found == "a string"
        ));
    }

    #[test]
    fn malformed_total_pages_path_fails_at_bind_time() {
        let strategy = PaginationStrategy::PageNumber {
            page_param: "page",
            per_page_param: "count",
            per_page: 2,
            total_pages_path: Some("not-a-path"),
            raw_page_size_path: None,
        };
        assert!(matches!(
            strategy.validate(),
            Err(OpenConnectorError::InvalidRowPath { .. })
        ));
        assert!(matches!(
            Pagination::new(strategy),
            Err(OpenConnectorError::InvalidRowPath { .. })
        ));
    }

    #[test]
    fn single_page_injects_nothing_and_never_advances() {
        let strategy = PaginationStrategy::SinglePage {
            next_cursor_path: None,
        };
        let mut pagination = Pagination::new(strategy).unwrap();
        assert_eq!(pagination.page(), 1);

        let mut input = Map::new();
        pagination.apply(&mut input);
        assert!(input.is_empty(), "no pagination inputs for a raw scan");

        // Even a "full-looking" page ends the scan: raw actions declare no
        // pagination contract, so there is nothing to advance — and nothing
        // to check it against either.
        assert!(
            !pagination
                .advance(&json!({"next_cursor": "c2"}), 100)
                .unwrap()
        );
        strategy.validate().expect("nothing to validate");
    }

    #[test]
    fn a_declared_single_page_premise_is_checked_not_assumed() {
        let single_page = |envelope: &Value| {
            Pagination::new(PaginationStrategy::SinglePage {
                next_cursor_path: Some("$.nextPageToken"),
            })
            .unwrap()
            .advance(envelope, 10)
        };

        // The three end-of-collection spellings all mean "the premise
        // held": absent, explicit null, empty string.
        for complete in [
            json!({"labels": []}),
            json!({"labels": [], "nextPageToken": null}),
            json!({"labels": [], "nextPageToken": ""}),
        ] {
            assert!(
                !single_page(&complete).expect("a complete collection scans cleanly"),
                "{complete}: no continuation means one request was the whole collection"
            );
        }

        // A live token is the provider saying otherwise. Stopping here
        // would be a silent truncation, so the scan fails instead.
        let err = single_page(&json!({"labels": [], "nextPageToken": "tok-2"}))
            .expect_err("a live continuation must fail the scan");
        assert!(
            matches!(
                err,
                OpenConnectorError::SinglePageIncomplete { ref path, page: 1, .. }
                    if path == "$.nextPageToken"
            ),
            "the premise break is named: {err}"
        );
        // The value never rides along into the message.
        assert!(!err.to_string().contains("tok-2"), "{err}");
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
