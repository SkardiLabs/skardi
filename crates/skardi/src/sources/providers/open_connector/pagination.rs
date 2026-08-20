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

/// How a cursor-paginated table continues past its first page, when the
/// provider does not accept the cursor on the action that started the
/// listing.
///
/// Open Connector's Dropbox provider is the motivating shape: `list_folder`
/// begins a folder listing and `list_folder_continue` — a **separate
/// action** whose input schema declares `cursor` as its only property —
/// serves pages 2..N. Feeding the cursor back to `list_folder` is not a
/// quiet truncation but a hard 400, because Open Connector's action schemas
/// are `additionalProperties: false`.
///
/// Absent (the default for every pre-existing pack), pages 2..N repeat the
/// table's own action with the full assembled input, exactly as before.
#[derive(Debug, Clone, Copy)]
pub struct CursorContinuation {
    /// Action serving pages 2..N. Spelled explicitly even when it equals
    /// the table's own action: a same-action continuation that only needs
    /// `cursor_only` still names itself, so the fingerprint gate below can
    /// never be handed a hole to fall through.
    pub action_id: &'static str,
    /// Contract fingerprint of `action_id`. Mandatory: the continuation
    /// action serves most of a large scan, so gating only the action that
    /// served page one would leave the rest of the collection unguarded
    /// against contract drift.
    ///
    /// Scope worth knowing before relying on it: a fingerprint hashes the
    /// action's OUTPUT schema, so this pin guards the ROW shape pages 2..N
    /// deliver. Where an opener and its continuation publish the same
    /// output schema — the Dropbox case — the two hashes are equal and this
    /// pin can only fail together with the opener's. What actually differs
    /// between the two actions is their INPUTS, and those are covered by
    /// `SourcePackTable::check_continuation_inputs` instead, not here.
    pub expected_fingerprint: &'static str,
    /// Whether pages 2..N carry ONLY the cursor. Required whenever the
    /// continuation action's schema accepts nothing else; kept separate
    /// from `action_id` because the two vary independently — a provider can
    /// continue through the same action while still rejecting the original
    /// inputs alongside a cursor.
    ///
    /// Checked at registration against the continuation action's discovered
    /// input schema (`SourcePackTable::check_continuation_inputs`), because
    /// the fingerprint above cannot: a wrong claim here is otherwise a hard
    /// 400 on page two of a live scan rather than a startup error.
    pub cursor_only: bool,
}

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
    /// Keyset pagination: the provider emits NO pagination envelope at all
    /// — the next request's cursor is a field of the previous page's LAST
    /// ROW (Discord's `/users/@me/guilds` takes `after=<last guild id>`).
    ///
    /// Termination: ONLY an empty page ends the scan; every non-empty
    /// page continues from its last row's cursor field. Deliberately NOT
    /// short-page termination: providers silently clamp page sizes (the
    /// Feishu live pass caught a declared 100 clamped to 50 on the wire),
    /// and under a clamp every page is "short" — a short-page rule would
    /// end the scan after page 1 and read as complete, the silent
    /// truncation this engine treats as the worst failure class. The
    /// empty-page rule is clamp-proof because asking a keyset endpoint
    /// for rows after the true last row returns nothing, at the cost of
    /// one extra (empty) request per scan — the standard keyset tax. That
    /// terminator also SPENDS a `max_pages` unit (the guard runs before
    /// each fetch), so a keyset table's real capacity is `max_pages - 1`
    /// full pages: a collection of exactly `max_pages × page_size` rows
    /// fails loudly on the terminator rather than completing — an
    /// asymmetry cursor/page-number don't have (their end signal arrives
    /// inside page N), chosen and pinned by the pack e2e
    /// (`max_pages_budget_includes_the_keyset_terminator`).
    ///
    /// The scan assumes the provider orders rows by the cursor field in
    /// the direction the cursor input walks; the loop guard converts a
    /// provider that violates this (repeating a cursor) into an error
    /// instead of an infinite scan.
    Keyset {
        /// Action input field for the cursor (e.g. `after`).
        cursor_param: &'static str,
        /// Field of the last row whose value is the next cursor — a plain
        /// key or dotted path relative to the ROW (not the envelope).
        row_cursor_field: &'static str,
        /// Action input field for the page size.
        page_size_param: &'static str,
        /// Page size to request — a throughput knob only, never a
        /// termination signal.
        page_size: u32,
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
            // A row-relative path: plain key or dotted segments, no `$.`
            // envelope syntax and no empty segment.
            PaginationStrategy::Keyset {
                row_cursor_field, ..
            } if row_cursor_field.is_empty()
                || row_cursor_field.starts_with('$')
                || row_cursor_field.split('.').any(str::is_empty) =>
            {
                return Err(OpenConnectorError::InvalidRowPath {
                    path: (*row_cursor_field).to_string(),
                    reason: "keyset row_cursor_field must be a plain key or dotted path \
                             relative to the row (no `$.` prefix, no empty segment)"
                        .to_string(),
                });
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
            PaginationStrategy::Cursor { .. }
            | PaginationStrategy::Keyset { .. }
            | PaginationStrategy::SinglePage { .. } => None,
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
            PaginationStrategy::PageNumber { .. }
            | PaginationStrategy::Keyset { .. }
            | PaginationStrategy::SinglePage { .. } => None,
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
        strategy.validate()?;
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
            PaginationStrategy::Keyset {
                cursor_param,
                page_size_param,
                page_size,
                ..
            } => {
                if let Some(token) = &self.next_token {
                    input.insert((*cursor_param).to_string(), Value::from(token.as_str()));
                }
                input.insert((*page_size_param).to_string(), Value::from(*page_size));
            }
            PaginationStrategy::SinglePage { .. } => {}
        }
    }

    /// Inject ONLY the cursor input, for a continuation request whose action
    /// accepts nothing else (see [`CursorContinuation::cursor_only`]).
    ///
    /// Deliberately omits the page-size input that [`Self::apply`] sends:
    /// Dropbox's `list_folder_continue` declares `cursor` as its sole
    /// property, so a `limit` alongside it is a 400. Continuation pages are
    /// sized by the request that began the listing — a provider guarantee,
    /// not something this engine enforces.
    ///
    /// A no-op on the first page (no token yet) and for non-cursor
    /// strategies, neither of which a continuation can reach.
    pub fn apply_cursor_only(&self, input: &mut Map<String, Value>) {
        if let PaginationStrategy::Cursor { cursor_param, .. } = &self.strategy
            && let Some(token) = &self.next_token
        {
            input.insert((*cursor_param).to_string(), Value::from(token.as_str()));
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
        last_row: Option<&Value>,
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
            PaginationStrategy::Keyset {
                row_cursor_field, ..
            } => {
                // ONLY an empty page ends the keyset walk. A short page is
                // NOT termination: page-size clamping providers make every
                // page short, and a short-page rule would read a clamped
                // scan as complete after page 1 — silent truncation.
                if rows_in_page == 0 {
                    return Ok(false);
                }
                // A non-empty page continues from the last row's cursor
                // field. rows_in_page ≥ 1, so the row exists; the defensive
                // arm covers a caller passing inconsistent args. Every
                // failure below is `PaginationKeysetCursorInvalid` (or the
                // value-free `PaginationKeysetLoop`), whose wording is
                // supplied here — reusing `PaginationCursorInvalid` would
                // graft its fixed "not a string" tail onto reasons where it
                // is wrong or self-contradictory (an empty string IS a
                // string).
                let invalid =
                    |page, reason: &str| OpenConnectorError::PaginationKeysetCursorInvalid {
                        path: (*row_cursor_field).to_string(),
                        page,
                        reason: reason.to_string(),
                    };
                let Some(row) = last_row else {
                    return Err(invalid(
                        self.page,
                        "cannot be read: the page is non-empty but no last row was \
                         supplied (caller bug)",
                    ));
                };
                let mut value = row;
                for segment in row_cursor_field.split('.') {
                    value = match value.get(segment) {
                        Some(v) => v,
                        None => {
                            // The pack declared a cursor field real rows do
                            // not carry — contract drift, never a quiet stop
                            // (stopping would silently truncate the scan).
                            return Err(invalid(self.page, "is absent from the page's last row"));
                        }
                    };
                }
                let next = match value {
                    Value::String(s) if !s.is_empty() => s.clone(),
                    Value::String(_) => {
                        return Err(invalid(self.page, "is an empty string"));
                    }
                    other => {
                        // Snowflakes are strings on the wire; a number here
                        // is drift (and stringifying it would silently paper
                        // over a provider change).
                        return Err(invalid(
                            self.page,
                            &format!("is {}, not a string", json_kind(other)),
                        ));
                    }
                };
                // A provider violating its own ordering (repeating the
                // cursor) fails as a loop, not an infinite scan. NOT
                // `PaginationLoop`: that error quotes the token, which is
                // fine for an envelope-level gateway cursor but this one is
                // ROW data — a field a future pack can point anywhere — and
                // row values never appear in errors.
                if !self.seen_tokens.insert(next.clone()) {
                    return Err(OpenConnectorError::PaginationKeysetLoop {
                        path: (*row_cursor_field).to_string(),
                        page: self.page,
                    });
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

    fn keyset() -> Pagination {
        Pagination::new(PaginationStrategy::Keyset {
            cursor_param: "after",
            row_cursor_field: "id",
            page_size_param: "limit",
            page_size: 2,
        })
        .unwrap()
    }

    fn row(id: &str) -> Value {
        json!({ "id": id, "name": "x" })
    }

    #[test]
    fn keyset_walks_from_the_last_rows_field_and_stops_only_on_an_empty_page() {
        let mut p = keyset();

        // Page 1: no cursor yet, page size injected.
        let mut input = Map::new();
        p.apply(&mut input);
        assert!(input.get("after").is_none(), "first page carries no cursor");
        assert_eq!(input.get("limit"), Some(&Value::from(2)));

        // Full page → continue from the LAST row's id.
        let rows = [row("100"), row("200")];
        let more = p.advance(&json!({}), rows.len(), rows.last()).unwrap();
        assert!(more);
        let mut input = Map::new();
        p.apply(&mut input);
        assert_eq!(input.get("after"), Some(&Value::from("200")));
        assert_eq!(p.page(), 2);

        // A SHORT page also continues — a page-size-clamping provider
        // makes every page short, and stopping here would silently
        // truncate a clamped scan after page 1.
        let rows = [row("300")];
        let more = p.advance(&json!({}), rows.len(), rows.last()).unwrap();
        assert!(more, "a short page continues the walk (clamp-proofing)");
        let mut input = Map::new();
        p.apply(&mut input);
        assert_eq!(input.get("after"), Some(&Value::from("300")));

        // ONLY the empty page ends the walk.
        let more = p.advance(&json!({}), 0, None).unwrap();
        assert!(!more, "the empty page is the one termination signal");
    }

    #[test]
    fn keyset_empty_first_page_terminates_immediately() {
        let mut p = keyset();
        let more = p.advance(&json!({}), 0, None).unwrap();
        assert!(!more);
    }

    #[test]
    fn keyset_final_page_costs_one_empty_request_then_ends() {
        let mut p = keyset();
        let rows = [row("1"), row("2")];
        assert!(p.advance(&json!({}), rows.len(), rows.last()).unwrap());
        // The provider has nothing after "2": the extra request comes back
        // empty and the scan completes — the standard keyset tax.
        assert!(!p.advance(&json!({}), 0, None).unwrap());
    }

    #[test]
    fn keyset_missing_cursor_field_on_a_nonempty_page_is_drift_not_a_quiet_stop() {
        let mut p = keyset();
        let rows = [row("1"), json!({ "name": "no id" })];
        let err = p.advance(&json!({}), rows.len(), rows.last()).unwrap_err();
        // The RENDERED diagnostic is the contract, not just the variant: a
        // wrong wording here misdirects the person debugging a real drift.
        assert_eq!(
            err.to_string(),
            "Open Connector keyset cursor field 'id' on page 1 is absent from the \
             page's last row; refusing to treat it as end-of-collection"
        );
    }

    #[test]
    fn keyset_non_string_cursor_fails_with_the_json_kind_named() {
        let mut p = keyset();
        // Snowflakes are strings on the wire; a number is contract drift,
        // and stringifying it would paper over a provider change.
        let rows = [row("1"), json!({ "id": 42 })];
        let err = p.advance(&json!({}), rows.len(), rows.last()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Open Connector keyset cursor field 'id' on page 1 is a number, not a \
             string; refusing to treat it as end-of-collection"
        );
        assert!(!err.to_string().contains("42"), "the value never appears");
    }

    #[test]
    fn keyset_empty_string_cursor_fails_without_calling_a_string_not_a_string() {
        let mut p = keyset();
        // An empty string IS a string — the diagnosis must say "empty",
        // not the self-contradictory "is a string, not a string" that a
        // json-kind rendering would produce.
        let rows = [row("1"), json!({ "id": "" })];
        let err = p.advance(&json!({}), rows.len(), rows.last()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Open Connector keyset cursor field 'id' on page 1 is an empty string; \
             refusing to treat it as end-of-collection"
        );
    }

    #[test]
    fn keyset_repeated_cursor_fails_as_a_loop_without_quoting_the_row_value() {
        let mut p = keyset();
        let rows = [row("1"), row("sensitive-row-value")];
        assert!(p.advance(&json!({}), rows.len(), rows.last()).unwrap());
        // A provider violating its own ordering re-serves the same last id.
        // The failure names the loop and carries identity only: the cursor
        // is ROW data, and row values never appear in errors (unlike
        // PaginationLoop's envelope-level token).
        let rows = [row("3"), row("sensitive-row-value")];
        let err = p.advance(&json!({}), rows.len(), rows.last()).unwrap_err();
        assert_eq!(
            err.to_string(),
            "Open Connector keyset cursor field 'id' on page 2 repeats a value \
             already consumed by an earlier page; refusing to loop the scan"
        );
        assert!(
            !err.to_string().contains("sensitive-row-value"),
            "the row value must not appear in the error"
        );
    }

    #[test]
    fn keyset_dotted_row_cursor_field_traverses_nested_rows() {
        let mut p = Pagination::new(PaginationStrategy::Keyset {
            cursor_param: "after",
            row_cursor_field: "meta.id",
            page_size_param: "limit",
            page_size: 1,
        })
        .unwrap();
        let rows = [json!({ "meta": { "id": "abc" } })];
        assert!(p.advance(&json!({}), rows.len(), rows.last()).unwrap());
        let mut input = Map::new();
        p.apply(&mut input);
        assert_eq!(input.get("after"), Some(&Value::from("abc")));
    }

    #[test]
    fn keyset_rejects_malformed_row_cursor_fields_at_construction() {
        for bad in ["", "$.id", "a..b", "a."] {
            let err = Pagination::new(PaginationStrategy::Keyset {
                cursor_param: "after",
                row_cursor_field: Box::leak(bad.to_string().into_boxed_str()),
                page_size_param: "limit",
                page_size: 2,
            })
            .unwrap_err();
            assert!(
                matches!(err, OpenConnectorError::InvalidRowPath { .. }),
                "{bad:?} must be rejected"
            );
        }
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
        assert!(pagination.advance(&json!({}), 2, None).unwrap());
        let mut input = Map::new();
        pagination.apply(&mut input);
        assert_eq!(input.get("page"), Some(&json!(2)));

        // Short page → done.
        assert!(!pagination.advance(&json!({}), 1, None).unwrap());
    }

    #[test]
    fn page_number_stops_on_empty_page() {
        let mut pagination = page_number(10);
        assert!(!pagination.advance(&json!({}), 0, None).unwrap());
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
                .advance(&json!({"pageInfo": {"fetched": 100}}), 37, None)
                .unwrap(),
            "full raw page with a short filtered page continues"
        );
        assert!(
            pagination
                .advance(&json!({"pageInfo": {"fetched": 100}}), 0, None)
                .unwrap(),
            "an all-filtered (empty) page continues while the raw page was full"
        );
        assert!(
            !pagination
                .advance(&json!({"pageInfo": {"fetched": 99}}), 99, None)
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
            pagination.advance(&json!({"issues": []}), 0, None),
            Err(OpenConnectorError::RowPathNotFound { .. })
        ));

        let mut pagination = page_number_with_raw(100);
        assert!(matches!(
            pagination.advance(&json!({"pageInfo": {"fetched": "100"}}), 0, None),
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
                .advance(&json!({"next_cursor": "c2"}), 50, None)
                .unwrap()
        );
        let mut input = Map::new();
        pagination.apply(&mut input);
        assert_eq!(input.get("cursor"), Some(&json!("c2")));
    }

    #[test]
    fn cursor_ends_on_missing_null_or_empty_next() {
        let mut pagination = cursor();
        assert!(!pagination.advance(&json!({}), 50, None).unwrap());

        let mut pagination = cursor();
        assert!(
            !pagination
                .advance(&json!({"next_cursor": ""}), 50, None)
                .unwrap()
        );

        let mut pagination = cursor();
        assert!(
            !pagination
                .advance(&json!({"next_cursor": null}), 50, None)
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
            let err = pagination.advance(&envelope, 50, None).unwrap_err();
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
            .advance(&json!({"meta": [1, 2]}), 50, None)
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
        assert!(!pagination.advance(&json!({"other": 1}), 50, None).unwrap());
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
                    1,
                    None
                )
                .unwrap()
        );
    }

    #[test]
    fn has_more_true_continues_with_the_cursor() {
        let mut pagination = cursor_with_has_more();
        assert!(
            pagination
                .advance(&json!({"hasMore": true, "pageToken": "tok-2"}), 50, None)
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
            let err = pagination.advance(&envelope, 50, None).unwrap_err();
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
            let err = pagination.advance(&envelope, 50, None).unwrap_err();
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
    fn cursor_only_carries_the_token_and_nothing_else() {
        // The whole point of the continuation mode: Dropbox's
        // list_folder_continue declares `cursor` as its ONLY property under
        // additionalProperties: false, so the page-size input `apply` sends
        // would be a hard 400 rather than a tolerated extra.
        let mut pagination = cursor();
        assert!(
            pagination
                .advance(&json!({"next_cursor": "c2"}), 50, None)
                .unwrap()
        );

        let mut input = Map::new();
        pagination.apply_cursor_only(&mut input);
        assert_eq!(input.get("cursor"), Some(&json!("c2")));
        assert_eq!(input.len(), 1, "cursor-only means exactly one key");

        // The same paginator's full `apply` still sends the page size, so
        // page one is unaffected by the continuation mode.
        let mut full = Map::new();
        pagination.apply(&mut full);
        assert_eq!(full.get("limit"), Some(&json!(50)));
    }

    #[test]
    fn cursor_only_is_a_noop_before_a_token_exists() {
        // Page one has no cursor to send; a continuation can never be
        // reached there, but an empty body beats inventing a null cursor.
        let pagination = cursor();
        let mut input = Map::new();
        pagination.apply_cursor_only(&mut input);
        assert!(input.is_empty());

        // Non-cursor strategies have no cursor input at all.
        let pagination = page_number(10);
        let mut input = Map::new();
        pagination.apply_cursor_only(&mut input);
        assert!(input.is_empty());
    }

    #[test]
    fn repeated_cursor_fails_as_loop() {
        let mut pagination = cursor();
        assert!(
            pagination
                .advance(&json!({"next_cursor": "same"}), 50, None)
                .unwrap()
        );
        // Gateway returns the same cursor again — must fail, not loop.
        let err = pagination
            .advance(&json!({"next_cursor": "same"}), 50, None)
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
        assert!(
            pagination.advance(&envelope, 2, None).unwrap(),
            "full page 1"
        );
        assert!(
            pagination.advance(&envelope, 1, None).unwrap(),
            "short page 2 continues"
        );
        assert!(
            !pagination.advance(&envelope, 0, None).unwrap(),
            "page 3 is the last"
        );

        let mut pagination = page_number_with_total(2);
        assert!(
            pagination
                .advance(&json!({"paging": {"pages": 2}}), 0, None)
                .unwrap(),
            "an empty non-final page continues"
        );

        // pages: 0 (empty collection) stops immediately.
        let mut pagination = page_number_with_total(2);
        assert!(
            !pagination
                .advance(&json!({"paging": {"pages": 0}}), 0, None)
                .unwrap()
        );
    }

    #[test]
    fn missing_or_non_numeric_totals_fail_the_scan() {
        // A declared total-pages location is a contract: silence or a wrong
        // kind must fail loudly, never fall back to the truncating
        // heuristic.
        let mut pagination = page_number_with_total(2);
        let err = pagination
            .advance(&json!({"ok": true}), 2, None)
            .unwrap_err();
        assert!(matches!(err, OpenConnectorError::RowPathNotFound { .. }));

        let mut pagination = page_number_with_total(2);
        let err = pagination
            .advance(&json!({"paging": {"pages": "three"}}), 2, None)
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
                .advance(&json!({"next_cursor": "c2"}), 100, None)
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
            .advance(envelope, 10, None)
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
                .advance(&json!({"next_cursor": "c2"}), 50, None)
                .unwrap()
        );
        assert!(
            pagination
                .advance(&json!({"next_cursor": "c3"}), 50, None)
                .unwrap()
        );
        assert!(
            !pagination
                .advance(&json!({"next_cursor": null}), 10, None)
                .unwrap()
        );
    }
}
