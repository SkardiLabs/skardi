//! GitHub source pack: stable relational contracts over the Open Connector
//! `github.*` read actions (API-key auth, page-number pagination).
//!
//! Design decisions, per the integration design spec and the source-pack
//! admission gate:
//!
//! - **Page-number pagination everywhere** (`page`/`per_page`, 100 per page
//!   — GitHub's maximum). A short or empty page terminates the scan, which
//!   is GitHub's documented end-of-collection signal.
//! - **Filters are allowlisted only where faithful.** `issues.state` and
//!   `pull_requests.state` translate exactly. `issues.updated_at >=` maps
//!   to `since` as [`Fidelity::Inexact`]: GitHub documents issue `since` as
//!   "updated at *or after*" (a superset of the predicate under any
//!   timestamp-granularity fuzz), so DataFusion reapplies the predicate
//!   locally. The commits endpoint's `since` is documented as commits
//!   *after* the date — strictly-after cannot guarantee a superset of a
//!   `>=` predicate (the boundary row would be unrecoverable), so it is
//!   deliberately **not** mapped, exactly like the mock pack's `>=` note.
//! - **`issues` includes pull requests**, because GitHub's issues endpoint
//!   does. The stable schema exposes the `pull_request` marker as an opaque
//!   nullable JSON column: `WHERE pull_request IS NULL` selects pure
//!   issues.
//! - **Nullability is conservative**: only identity fields (`id`, `number`,
//!   `sha`, `tag_name`, …) are non-null. GitHub nulls out whole objects
//!   (`commit.author: null`, `issue.user: null`); nullable columns under
//!   them become SQL NULL per the converter's null-parent rule.
//! - **No fingerprint pins yet** (`expected_fingerprint: None`), same as
//!   the mock pack: a pin must be taken from a live gateway's discovered
//!   contract, and this pack has not been validated against one. The
//!   bundled fixtures (see tests) are the build-time conversion contract;
//!   pins land when the pack is validated against a live catalog.

use datafusion::logical_expr::Operator;

use crate::sources::providers::open_connector::filters::{Fidelity, FilterMapping};
use crate::sources::providers::open_connector::json_to_arrow::{FieldMapping, FieldType};
use crate::sources::providers::open_connector::pagination::PaginationStrategy;
use crate::sources::providers::open_connector::source_pack::{SourcePack, SourcePackTable};

/// GitHub's maximum page size; also the short-page termination threshold.
const GITHUB_PAGINATION: PaginationStrategy = PaginationStrategy::PageNumber {
    page_param: "page",
    per_page_param: "per_page",
    per_page: 100,
};

/// Repositories visible to the connected account.
static REPOSITORIES: SourcePackTable = SourcePackTable {
    id: "github.repositories",
    action_id: "github.list_repositories",
    row_path: "$.repositories",
    fields: &[
        FieldMapping {
            name: "id",
            path: "id",
            field_type: FieldType::UInt64,
            nullable: false,
        },
        FieldMapping {
            name: "name",
            path: "name",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "full_name",
            path: "full_name",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "private",
            path: "private",
            field_type: FieldType::Boolean,
            nullable: false,
        },
        FieldMapping {
            name: "description",
            path: "description",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "default_branch",
            path: "default_branch",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "language",
            path: "language",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "stargazers_count",
            path: "stargazers_count",
            field_type: FieldType::UInt64,
            nullable: true,
        },
        FieldMapping {
            name: "forks_count",
            path: "forks_count",
            field_type: FieldType::UInt64,
            nullable: true,
        },
        FieldMapping {
            name: "open_issues_count",
            path: "open_issues_count",
            field_type: FieldType::UInt64,
            nullable: true,
        },
        FieldMapping {
            name: "archived",
            path: "archived",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "created_at",
            path: "created_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "updated_at",
            path: "updated_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "pushed_at",
            path: "pushed_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "html_url",
            path: "html_url",
            field_type: FieldType::Utf8,
            nullable: true,
        },
    ],
    pagination: GITHUB_PAGINATION,
    required_resources: &[],
    fixed_inputs: &[],
    filters: &[],
    expected_fingerprint: None,
};

/// Issues of one repository. GitHub's issues endpoint also returns pull
/// requests; the nullable `pull_request` JSON marker distinguishes them
/// (`IS NULL` → pure issues).
static ISSUES: SourcePackTable = SourcePackTable {
    id: "github.issues",
    action_id: "github.list_repository_issues",
    row_path: "$.issues",
    fields: &[
        FieldMapping {
            name: "id",
            path: "id",
            field_type: FieldType::UInt64,
            nullable: false,
        },
        FieldMapping {
            name: "number",
            path: "number",
            field_type: FieldType::UInt64,
            nullable: false,
        },
        FieldMapping {
            name: "title",
            path: "title",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "state",
            path: "state",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "body",
            path: "body",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "author_login",
            path: "user.login",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "assignees",
            path: "assignees",
            field_type: FieldType::Utf8ListFromObjectKey("login"),
            nullable: true,
        },
        FieldMapping {
            name: "labels",
            path: "labels",
            field_type: FieldType::Utf8ListFromObjectKey("name"),
            nullable: true,
        },
        FieldMapping {
            name: "comments",
            path: "comments",
            field_type: FieldType::UInt64,
            nullable: true,
        },
        FieldMapping {
            name: "created_at",
            path: "created_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "updated_at",
            path: "updated_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "closed_at",
            path: "closed_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "pull_request",
            path: "pull_request",
            field_type: FieldType::Json,
            nullable: true,
        },
        FieldMapping {
            name: "html_url",
            path: "html_url",
            field_type: FieldType::Utf8,
            nullable: true,
        },
    ],
    pagination: GITHUB_PAGINATION,
    required_resources: &["owner", "repo"],
    // GitHub lists only open issues by default; pin `state=all` so the
    // table reads as the complete collection (a pushed `state` predicate
    // overrides the pin).
    fixed_inputs: &[("state", "all")],
    filters: &[
        // `state` takes exactly the SQL literal (open/closed/all).
        FilterMapping {
            column: "state",
            operator: Operator::Eq,
            input_field: "state",
            fidelity: Fidelity::Exact,
        },
        // GitHub documents issue `since` as "updated at or after this
        // time" — a superset of `updated_at >= X` under any granularity
        // fuzz — so the push narrows the fetch and DataFusion reapplies
        // the predicate (Inexact).
        FilterMapping {
            column: "updated_at",
            operator: Operator::GtEq,
            input_field: "since",
            fidelity: Fidelity::Inexact,
        },
    ],
    expected_fingerprint: None,
};

/// Comments of one issue (or pull request — GitHub shares issue comments).
static ISSUE_COMMENTS: SourcePackTable = SourcePackTable {
    id: "github.issue_comments",
    action_id: "github.list_issue_comments",
    row_path: "$.comments",
    fields: &[
        FieldMapping {
            name: "id",
            path: "id",
            field_type: FieldType::UInt64,
            nullable: false,
        },
        FieldMapping {
            name: "body",
            path: "body",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "author_login",
            path: "user.login",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "created_at",
            path: "created_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "updated_at",
            path: "updated_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "html_url",
            path: "html_url",
            field_type: FieldType::Utf8,
            nullable: true,
        },
    ],
    pagination: GITHUB_PAGINATION,
    required_resources: &["owner", "repo", "issue_number"],
    fixed_inputs: &[],
    filters: &[],
    expected_fingerprint: None,
};

/// Pull requests of one repository.
static PULL_REQUESTS: SourcePackTable = SourcePackTable {
    id: "github.pull_requests",
    action_id: "github.list_pull_requests",
    row_path: "$.pull_requests",
    fields: &[
        FieldMapping {
            name: "id",
            path: "id",
            field_type: FieldType::UInt64,
            nullable: false,
        },
        FieldMapping {
            name: "number",
            path: "number",
            field_type: FieldType::UInt64,
            nullable: false,
        },
        FieldMapping {
            name: "title",
            path: "title",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "state",
            path: "state",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "body",
            path: "body",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "author_login",
            path: "user.login",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "draft",
            path: "draft",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "head_ref",
            path: "head.ref",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "base_ref",
            path: "base.ref",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "created_at",
            path: "created_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "updated_at",
            path: "updated_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "closed_at",
            path: "closed_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "merged_at",
            path: "merged_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "html_url",
            path: "html_url",
            field_type: FieldType::Utf8,
            nullable: true,
        },
    ],
    pagination: GITHUB_PAGINATION,
    required_resources: &["owner", "repo"],
    // Same open-by-default listing as issues; pin the complete collection.
    fixed_inputs: &[("state", "all")],
    filters: &[FilterMapping {
        column: "state",
        operator: Operator::Eq,
        input_field: "state",
        fidelity: Fidelity::Exact,
    }],
    expected_fingerprint: None,
};

/// Reviews of one pull request.
static REVIEWS: SourcePackTable = SourcePackTable {
    id: "github.reviews",
    action_id: "github.list_pull_request_reviews",
    row_path: "$.reviews",
    fields: &[
        FieldMapping {
            name: "id",
            path: "id",
            field_type: FieldType::UInt64,
            nullable: false,
        },
        FieldMapping {
            name: "state",
            path: "state",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "author_login",
            path: "user.login",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "body",
            path: "body",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "commit_id",
            path: "commit_id",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "submitted_at",
            path: "submitted_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "html_url",
            path: "html_url",
            field_type: FieldType::Utf8,
            nullable: true,
        },
    ],
    pagination: GITHUB_PAGINATION,
    required_resources: &["owner", "repo", "pull_number"],
    fixed_inputs: &[],
    filters: &[],
    expected_fingerprint: None,
};

/// Commits of one repository. `author` (the GitHub account) is routinely
/// JSON null when no account is linked to the commit email — the nullable
/// `author_login` becomes SQL NULL, while the git-level name and dates
/// under `commit.*` stay available.
static COMMITS: SourcePackTable = SourcePackTable {
    id: "github.commits",
    action_id: "github.list_commits",
    row_path: "$.commits",
    fields: &[
        FieldMapping {
            name: "sha",
            path: "sha",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "message",
            path: "commit.message",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "author_login",
            path: "author.login",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "author_name",
            path: "commit.author.name",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "authored_at",
            path: "commit.author.date",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "committed_at",
            path: "commit.committer.date",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "html_url",
            path: "html_url",
            field_type: FieldType::Utf8,
            nullable: true,
        },
    ],
    pagination: GITHUB_PAGINATION,
    required_resources: &["owner", "repo"],
    fixed_inputs: &[],
    // NOTE: GitHub's commits `since`/`until` are documented as commits
    // *after*/*before* the date. Strictly-after cannot represent
    // `committed_at >= X` even as Inexact — a boundary commit the provider
    // drops is unrecoverable — so no time filter is mapped (the mock pack's
    // `>=` rule, applied here).
    filters: &[],
    expected_fingerprint: None,
};

/// Workflow runs (GitHub Actions) of one repository. `conclusion` is JSON
/// null while a run is in progress.
static WORKFLOW_RUNS: SourcePackTable = SourcePackTable {
    id: "github.workflow_runs",
    action_id: "github.list_workflow_runs",
    row_path: "$.workflow_runs",
    fields: &[
        FieldMapping {
            name: "id",
            path: "id",
            field_type: FieldType::UInt64,
            nullable: false,
        },
        FieldMapping {
            name: "name",
            path: "name",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "workflow_id",
            path: "workflow_id",
            field_type: FieldType::UInt64,
            nullable: true,
        },
        FieldMapping {
            name: "run_number",
            path: "run_number",
            field_type: FieldType::UInt64,
            nullable: true,
        },
        FieldMapping {
            name: "event",
            path: "event",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "status",
            path: "status",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "conclusion",
            path: "conclusion",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "head_branch",
            path: "head_branch",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "head_sha",
            path: "head_sha",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "created_at",
            path: "created_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "updated_at",
            path: "updated_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "html_url",
            path: "html_url",
            field_type: FieldType::Utf8,
            nullable: true,
        },
    ],
    pagination: GITHUB_PAGINATION,
    required_resources: &["owner", "repo"],
    fixed_inputs: &[],
    // GitHub's `status` query parameter matches status OR conclusion values
    // interchangeably — not a faithful translation of either column.
    filters: &[],
    expected_fingerprint: None,
};

/// Releases of one repository. `published_at` is JSON null for drafts.
static RELEASES: SourcePackTable = SourcePackTable {
    id: "github.releases",
    action_id: "github.list_releases",
    row_path: "$.releases",
    fields: &[
        FieldMapping {
            name: "id",
            path: "id",
            field_type: FieldType::UInt64,
            nullable: false,
        },
        FieldMapping {
            name: "tag_name",
            path: "tag_name",
            field_type: FieldType::Utf8,
            nullable: false,
        },
        FieldMapping {
            name: "name",
            path: "name",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "draft",
            path: "draft",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "prerelease",
            path: "prerelease",
            field_type: FieldType::Boolean,
            nullable: true,
        },
        FieldMapping {
            name: "body",
            path: "body",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "author_login",
            path: "author.login",
            field_type: FieldType::Utf8,
            nullable: true,
        },
        FieldMapping {
            name: "created_at",
            path: "created_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "published_at",
            path: "published_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
        FieldMapping {
            name: "html_url",
            path: "html_url",
            field_type: FieldType::Utf8,
            nullable: true,
        },
    ],
    pagination: GITHUB_PAGINATION,
    required_resources: &["owner", "repo"],
    fixed_inputs: &[],
    filters: &[],
    expected_fingerprint: None,
};

/// The GitHub source pack (version 1).
pub static GITHUB_PACK: SourcePack = SourcePack {
    name: "github",
    version: 1,
    tables: &[
        REPOSITORIES,
        ISSUES,
        ISSUE_COMMENTS,
        PULL_REQUESTS,
        REVIEWS,
        COMMITS,
        WORKFLOW_RUNS,
        RELEASES,
    ],
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::open_connector::json_to_arrow::RowConverter;
    use crate::sources::providers::open_connector::row_path::RowPath;
    use arrow::array::{
        Array, BooleanArray, ListArray, StringArray, TimestampMillisecondArray, UInt64Array,
    };
    use arrow::record_batch::RecordBatch;

    // ── Contract tests: bundled redacted fixtures are the build-time
    // conversion contract (null-bearing, nested, empty, and extra upstream
    // fields per the source-pack admission gate). ───────────────────────

    /// Convert one bundled fixture page through a table's declared contract.
    fn convert_fixture(table: &SourcePackTable, fixture: &str) -> RecordBatch {
        let page: serde_json::Value = serde_json::from_str(fixture).expect("fixture parses");
        let rows = RowPath::parse(table.row_path)
            .expect("row path")
            .rows(&page, 1)
            .expect("row array");
        RowConverter::new(table.fields)
            .expect("converter")
            .convert(rows, 1)
            .expect("fixture converts")
    }

    fn strings<'a>(batch: &'a RecordBatch, column: &str) -> &'a StringArray {
        batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column {column}"))
            .as_any()
            .downcast_ref()
            .expect("Utf8 column")
    }

    fn u64s<'a>(batch: &'a RecordBatch, column: &str) -> &'a UInt64Array {
        batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column {column}"))
            .as_any()
            .downcast_ref()
            .expect("UInt64 column")
    }

    fn bools<'a>(batch: &'a RecordBatch, column: &str) -> &'a BooleanArray {
        batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column {column}"))
            .as_any()
            .downcast_ref()
            .expect("Boolean column")
    }

    fn timestamps<'a>(batch: &'a RecordBatch, column: &str) -> &'a TimestampMillisecondArray {
        batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column {column}"))
            .as_any()
            .downcast_ref()
            .expect("Timestamp column")
    }

    fn string_list(batch: &RecordBatch, column: &str, row: usize) -> Vec<String> {
        let lists: &ListArray = batch
            .column_by_name(column)
            .unwrap_or_else(|| panic!("column {column}"))
            .as_any()
            .downcast_ref()
            .expect("List column");
        let values = lists.value(row);
        let values = values
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("Utf8 items");
        (0..values.len())
            .map(|i| values.value(i).to_string())
            .collect()
    }

    #[test]
    fn issues_fixture_converts_with_nulls_lists_and_pr_marker() {
        let batch = convert_fixture(&ISSUES, include_str!("fixtures/github/issues.json"));
        assert_eq!(batch.num_rows(), 3);

        assert_eq!(u64s(&batch, "id").value(0), 101);
        assert_eq!(u64s(&batch, "number").value(2), 3);
        assert_eq!(
            strings(&batch, "title").value(0),
            "Scan panics on empty page"
        );
        assert_eq!(strings(&batch, "state").value(1), "closed");

        // JSON null body and null `user` parent become SQL NULL.
        assert!(strings(&batch, "body").is_null(1));
        assert_eq!(strings(&batch, "author_login").value(0), "octocat");
        assert!(strings(&batch, "author_login").is_null(1));

        // Object lists pluck the declared key; empty arrays stay empty lists.
        assert_eq!(
            string_list(&batch, "assignees", 0),
            vec!["octocat", "hubot"]
        );
        assert!(string_list(&batch, "assignees", 1).is_empty());
        assert_eq!(string_list(&batch, "labels", 0), vec!["bug", "p1"]);

        // Timestamps parse; closed_at is NULL while open.
        assert_eq!(
            timestamps(&batch, "created_at").value(0),
            1_767_225_600_000,
            "2026-01-01T00:00:00Z"
        );
        assert!(timestamps(&batch, "closed_at").is_null(0));
        assert!(!timestamps(&batch, "closed_at").is_null(1));

        // The pull_request marker: absent for issues (NULL), opaque JSON
        // for pull requests — `WHERE pull_request IS NULL` → pure issues.
        let markers = strings(&batch, "pull_request");
        assert!(markers.is_null(0));
        assert!(markers.is_null(1));
        assert!(markers.value(2).contains("pulls/3"));
    }

    #[test]
    fn repositories_fixture_converts_with_nullable_metadata() {
        let batch = convert_fixture(
            &REPOSITORIES,
            include_str!("fixtures/github/repositories.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(strings(&batch, "full_name").value(0), "acme/widgets");
        assert!(bools(&batch, "private").value(1));
        assert!(bools(&batch, "archived").value(1));
        assert_eq!(strings(&batch, "language").value(0), "Rust");
        assert!(strings(&batch, "language").is_null(1));
        assert!(strings(&batch, "description").is_null(1));
        assert!(timestamps(&batch, "pushed_at").is_null(1));
        assert_eq!(u64s(&batch, "stargazers_count").value(0), 42);
    }

    #[test]
    fn issue_comments_fixture_converts_with_null_author() {
        let batch = convert_fixture(
            &ISSUE_COMMENTS,
            include_str!("fixtures/github/issue_comments.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(strings(&batch, "body").value(0), "Reproduced on main.");
        assert!(strings(&batch, "body").is_null(1));
        assert!(strings(&batch, "author_login").is_null(1));
    }

    #[test]
    fn pull_requests_fixture_converts_with_nested_refs_and_merge_state() {
        let batch = convert_fixture(
            &PULL_REQUESTS,
            include_str!("fixtures/github/pull_requests.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(strings(&batch, "head_ref").value(0), "feature/dark-mode");
        assert_eq!(strings(&batch, "base_ref").value(0), "main");
        assert!(bools(&batch, "draft").value(0));
        assert!(timestamps(&batch, "merged_at").is_null(0), "open PR");
        assert!(!timestamps(&batch, "merged_at").is_null(1), "merged PR");
    }

    #[test]
    fn reviews_fixture_converts_with_null_bearing_row() {
        let batch = convert_fixture(&REVIEWS, include_str!("fixtures/github/reviews.json"));
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(strings(&batch, "state").value(0), "APPROVED");
        assert!(strings(&batch, "author_login").is_null(1));
        assert!(strings(&batch, "commit_id").is_null(1));
        assert!(timestamps(&batch, "submitted_at").is_null(1));
    }

    #[test]
    fn commits_fixture_converts_with_null_github_account() {
        // The classic GitHub shape: `author` (the account) is JSON null for
        // unlinked commit emails, while git-level identity under `commit.*`
        // stays available.
        let batch = convert_fixture(&COMMITS, include_str!("fixtures/github/commits.json"));
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(strings(&batch, "author_login").value(0), "octocat");
        assert!(strings(&batch, "author_login").is_null(1));
        assert_eq!(strings(&batch, "author_name").value(1), "Legacy Importer");
        assert_eq!(strings(&batch, "message").value(0), "feat: add dark mode");
        assert!(!timestamps(&batch, "committed_at").is_null(0));
    }

    #[test]
    fn workflow_runs_fixture_converts_with_in_progress_run() {
        let batch = convert_fixture(
            &WORKFLOW_RUNS,
            include_str!("fixtures/github/workflow_runs.json"),
        );
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(strings(&batch, "conclusion").value(0), "success");
        assert!(
            strings(&batch, "conclusion").is_null(1),
            "conclusion is NULL while a run is in progress"
        );
        assert!(strings(&batch, "name").is_null(1));
        assert_eq!(strings(&batch, "status").value(1), "in_progress");
        assert_eq!(u64s(&batch, "run_number").value(0), 128);
    }

    #[test]
    fn releases_fixture_converts_with_unpublished_draft() {
        let batch = convert_fixture(&RELEASES, include_str!("fixtures/github/releases.json"));
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(strings(&batch, "tag_name").value(0), "v1.2.0");
        assert!(bools(&batch, "draft").value(1));
        assert!(timestamps(&batch, "published_at").is_null(1), "draft");
        assert!(strings(&batch, "name").is_null(1));
        assert!(strings(&batch, "author_login").is_null(1));
    }

    #[test]
    fn every_table_converts_an_empty_page_and_keeps_its_schema() {
        for table in GITHUB_PACK.tables {
            let converter = RowConverter::new(table.fields).expect("converter");
            let batch = converter.convert(&[], 1).expect("empty page");
            assert_eq!(batch.num_rows(), 0, "{}", table.id);
            assert_eq!(
                batch.schema().fields().len(),
                table.fields.len(),
                "{} keeps its stable schema on empty results",
                table.id
            );
        }
    }

    // ── Integration tests: the issues table end to end through a mock
    // gateway — pagination, the state=all pin, Exact and Inexact pushdown,
    // the PR marker, LIMIT, and UDTF parity. ─────────────────────────────

    use crate::sources::hierarchy::HierarchyLevel;
    use crate::sources::providers::open_connector::testutil::{
        MockGateway, MockResponse, RecordedRequest,
    };
    use crate::sources::providers::open_connector::{
        OpenConnectorConfig, OpenConnectorGateways, register_open_connector_tables,
        register_open_connector_udtfs,
    };
    use datafusion::prelude::SessionContext;
    use serde_json::{Value, json};

    /// One minimal issue row: only the non-null contract fields plus
    /// whatever the test cares about (missing nullable keys become NULL).
    fn issue(n: u64, state: &str, updated_at: &str) -> Value {
        json!({
            "id": n,
            "number": n,
            "title": format!("issue-{n}"),
            "state": state,
            "updated_at": updated_at
        })
    }

    /// Mock gateway serving `github.list_repository_issues` over `rows`:
    /// honors `state` exactly, pages at `per_page`, and deliberately
    /// IGNORES `since` — returning a superset is exactly what an Inexact
    /// mapping permits, and DataFusion must trim it.
    fn issues_handler(req: &RecordedRequest, rows: &[Value]) -> MockResponse {
        if req.method == "GET" && req.path == "/v1/health" {
            return MockResponse::ok("{}");
        }
        if req.method == "GET" && req.path == "/v1/actions/github.list_repository_issues" {
            return MockResponse::ok(
                r#"{"input_schema": {}, "output_schema": {"type": "object"},
                    "locally_executable": true, "connection_aliases": []}"#,
            );
        }
        if req.method == "POST" && req.path == "/v1/actions/github.list_repository_issues/execute" {
            let body: Value = serde_json::from_str(&req.body).unwrap_or_default();
            let input = body.get("input").cloned().unwrap_or_default();
            let page = input.get("page").and_then(Value::as_u64).unwrap_or(1) as usize;
            let per_page = input.get("per_page").and_then(Value::as_u64).unwrap_or(30) as usize;
            let state = input
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("open")
                .to_string();
            let slice: Vec<_> = rows
                .iter()
                .filter(|row| {
                    state == "all" || row.get("state").and_then(Value::as_str) == Some(&state)
                })
                .skip((page - 1) * per_page)
                .take(per_page)
                .cloned()
                .collect();
            return MockResponse::ok(&json!({"output": {"issues": slice}}).to_string());
        }
        MockResponse::new(404, "{}")
    }

    fn issues_config(token_env: &str) -> OpenConnectorConfig {
        serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
bindings:
  - name: gh
    source_pack: github
    resource: {{ owner: acme, repo: widgets }}
    tables: [issues]
"#
        ))
        .expect("parse config")
    }

    /// Register the gateway (catalog + UDTFs) against `rows`.
    async fn setup(rows: Vec<Value>, token_env: &str) -> (MockGateway, SessionContext) {
        let served = std::sync::Arc::new(rows);
        let gateway = {
            let served = std::sync::Arc::clone(&served);
            MockGateway::start(move |req| issues_handler(req, &served)).await
        };
        unsafe {
            std::env::set_var(token_env, "test-token");
        }
        let gateways = OpenConnectorGateways::default();
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&issues_config(token_env)),
            false,
            HierarchyLevel::Catalog,
            Some(&gateways),
        )
        .await
        .expect("gateway registration succeeds");
        unsafe {
            std::env::remove_var(token_env);
        }
        register_open_connector_udtfs(&ctx, gateways);
        (gateway, ctx)
    }

    async fn collect(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        ctx.sql(sql)
            .await
            .expect("plan")
            .collect()
            .await
            .expect("collect")
    }

    fn rows_of(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    fn execute_bodies(gateway: &MockGateway) -> Vec<String> {
        gateway
            .requests()
            .into_iter()
            .filter(|r| r.method == "POST")
            .map(|r| r.body)
            .collect()
    }

    /// 150 issues: odd numbers open, even numbers closed.
    fn many_issues() -> Vec<Value> {
        (1..=150)
            .map(|n| {
                issue(
                    n,
                    if n % 2 == 1 { "open" } else { "closed" },
                    "2026-01-01T00:00:00Z",
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn full_scan_paginates_the_complete_collection() {
        let (gateway, ctx) = setup(many_issues(), "SKARDI_TEST_OC_GITHUB_SCAN").await;

        let batches = collect(&ctx, "SELECT count(*) AS n FROM saas.gh.issues").await;
        let count = u64s_i64(&batches[0], "n");
        assert_eq!(
            count, 150,
            "closed issues included, not GitHub's open-only default"
        );

        let bodies = execute_bodies(&gateway);
        assert_eq!(bodies.len(), 2, "150 rows at per_page=100 → 2 pages");
        assert!(bodies[0].contains(r#""page":1"#) && bodies[0].contains(r#""per_page":100"#));
        assert!(bodies[1].contains(r#""page":2"#));
        assert!(
            bodies.iter().all(|body| body.contains(r#""state":"all""#)),
            "the state=all pin makes the table the complete collection"
        );
        assert!(
            bodies
                .iter()
                .all(|body| body.contains(r#""owner":"acme""#)
                    && body.contains(r#""repo":"widgets""#)),
            "resource inputs ride on every request"
        );
    }

    /// Extract the single Int64 count value (count(*) output).
    fn u64s_i64(batch: &RecordBatch, column: &str) -> i64 {
        batch
            .column_by_name(column)
            .expect("count column")
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("Int64")
            .value(0)
    }

    #[tokio::test]
    async fn state_predicate_overrides_the_fixed_input_exactly() {
        let (gateway, ctx) = setup(many_issues(), "SKARDI_TEST_OC_GITHUB_STATE").await;

        let batches = collect(&ctx, "SELECT id FROM saas.gh.issues WHERE state = 'open'").await;
        assert_eq!(rows_of(&batches), 75, "odd-numbered issues are open");
        assert!(
            execute_bodies(&gateway).iter().all(
                |body| body.contains(r#""state":"open""#) && !body.contains(r#""state":"all""#)
            ),
            "the pushed Exact predicate replaces the state=all pin"
        );
    }

    #[tokio::test]
    async fn since_pushdown_is_inexact_and_reapplied_locally() {
        // The gateway ignores `since` entirely — the harshest legal Inexact
        // provider (a full superset). DataFusion must trim it back to the
        // predicate, so the boundary row stays and older rows never leak.
        let rows = vec![
            issue(1, "open", "2026-01-01T00:00:00Z"),
            issue(2, "open", "2026-01-02T00:00:00Z"),
            issue(3, "open", "2026-01-03T00:00:00Z"),
        ];
        let (gateway, ctx) = setup(rows, "SKARDI_TEST_OC_GITHUB_SINCE").await;

        let batches = collect(
            &ctx,
            "SELECT id FROM saas.gh.issues \
             WHERE updated_at >= TIMESTAMP '2026-01-02T00:00:00Z' ORDER BY id",
        )
        .await;
        assert_eq!(
            rows_of(&batches),
            2,
            "rows 2 and 3: the superset row 1 is re-filtered, the boundary row 2 kept"
        );
        assert!(
            execute_bodies(&gateway)
                .iter()
                .all(|body| body.contains(r#""since":"2026-01-02T00:00:00Z""#)),
            "the predicate still narrows the fetch as GitHub's since"
        );
    }

    #[tokio::test]
    async fn pull_request_marker_separates_issues_from_prs() {
        let fixture: Value =
            serde_json::from_str(include_str!("fixtures/github/issues.json")).unwrap();
        let rows = fixture["issues"].as_array().unwrap().clone();
        let (_gateway, ctx) = setup(rows, "SKARDI_TEST_OC_GITHUB_PR_MARKER").await;

        let batches = collect(
            &ctx,
            "SELECT number FROM saas.gh.issues WHERE pull_request IS NULL ORDER BY number",
        )
        .await;
        assert_eq!(rows_of(&batches), 2, "rows 1 and 2 are pure issues");

        let batches = collect(
            &ctx,
            "SELECT number FROM saas.gh.issues WHERE pull_request IS NOT NULL",
        )
        .await;
        assert_eq!(rows_of(&batches), 1, "row 3 is the pull request");
    }

    #[tokio::test]
    async fn limit_stops_github_pagination_after_the_first_page() {
        let (gateway, ctx) = setup(many_issues(), "SKARDI_TEST_OC_GITHUB_LIMIT").await;

        let batches = collect(&ctx, "SELECT id FROM saas.gh.issues LIMIT 5").await;
        assert_eq!(rows_of(&batches), 5);
        assert_eq!(
            execute_bodies(&gateway).len(),
            1,
            "LIMIT 5 must stop after the first page"
        );
    }

    #[tokio::test]
    async fn query_udtf_matches_the_yaml_bound_issues_table() {
        let (_gateway, ctx) = setup(many_issues(), "SKARDI_TEST_OC_GITHUB_UDTF").await;

        let from_table = collect(
            &ctx,
            "SELECT id, title, state FROM saas.gh.issues ORDER BY id",
        )
        .await;
        let from_udtf = collect(
            &ctx,
            r#"SELECT id, title, state
               FROM open_connector_query('saas', 'github.issues',
                                         '{"owner":"acme","repo":"widgets"}')
               ORDER BY id"#,
        )
        .await;
        assert_eq!(from_table[0].schema(), from_udtf[0].schema());
        assert_eq!(
            arrow::util::pretty::pretty_format_batches(&from_table)
                .unwrap()
                .to_string(),
            arrow::util::pretty::pretty_format_batches(&from_udtf)
                .unwrap()
                .to_string()
        );
    }

    #[tokio::test]
    async fn numeric_yaml_resource_values_reach_the_gateway_as_numbers() {
        // `issue_number: 42` in a YAML binding must arrive as JSON 42 — the
        // same value an equivalent UDTF resource JSON sends — never "42".
        let gateway = MockGateway::start(|req| {
            if req.method == "GET" && req.path == "/v1/health" {
                return MockResponse::ok("{}");
            }
            if req.method == "GET" && req.path.starts_with("/v1/actions/") {
                return MockResponse::ok(
                    r#"{"input_schema": {}, "output_schema": {"type": "object"},
                        "locally_executable": true, "connection_aliases": []}"#,
                );
            }
            if req.method == "POST" && req.path == "/v1/actions/github.list_issue_comments/execute"
            {
                return MockResponse::ok(
                    &serde_json::json!({"output": {"comments": []}}).to_string(),
                );
            }
            MockResponse::new(404, "{}")
        })
        .await;

        let token_env = "SKARDI_TEST_OC_GITHUB_NUMERIC_RESOURCE";
        unsafe {
            std::env::set_var(token_env, "test-token");
        }
        let config: OpenConnectorConfig = serde_yaml::from_str(&format!(
            r#"
runtime_token_env: {token_env}
bindings:
  - name: gh
    source_pack: github
    resource: {{ owner: acme, repo: widgets, issue_number: 42 }}
    tables: [issue_comments]
"#
        ))
        .expect("parse config");
        let mut ctx = SessionContext::new();
        register_open_connector_tables(
            &mut ctx,
            "saas",
            &gateway.url,
            Some(&config),
            false,
            HierarchyLevel::Catalog,
            None,
        )
        .await
        .expect("gateway registration succeeds");
        unsafe {
            std::env::remove_var(token_env);
        }

        let batches = collect(&ctx, "SELECT id FROM saas.gh.issue_comments").await;
        assert_eq!(rows_of(&batches), 0, "stub serves an empty collection");

        let bodies = execute_bodies(&gateway);
        assert!(!bodies.is_empty());
        assert!(
            bodies
                .iter()
                .all(|body| body.contains(r#""issue_number":42"#)),
            "numeric resource must stay a JSON number: {bodies:?}"
        );
        assert!(
            bodies
                .iter()
                .all(|body| !body.contains(r#""issue_number":"42""#)),
            "never stringified: {bodies:?}"
        );
    }

    #[test]
    fn every_table_binds_and_declares_a_complete_contract() {
        // Bind-time validation (row paths, field paths, pagination) plus the
        // admission-gate basics: page-number pagination that terminates, and
        // owner/repo-style resources spelled out.
        for table in GITHUB_PACK.tables {
            RowPath::parse(table.row_path).unwrap_or_else(|e| panic!("{}: {e}", table.id));
            RowConverter::new(table.fields).unwrap_or_else(|e| panic!("{}: {e}", table.id));
            table
                .pagination
                .validate()
                .unwrap_or_else(|e| panic!("{}: {e}", table.id));
            assert!(
                matches!(
                    table.pagination,
                    PaginationStrategy::PageNumber { per_page: 100, .. }
                ),
                "{} uses GitHub's page-number pagination at the 100 maximum",
                table.id
            );
            assert!(
                table.id.starts_with("github."),
                "{} carries the pack namespace",
                table.id
            );
            assert!(
                table.action_id.starts_with("github."),
                "{} executes a github action",
                table.id
            );
        }
    }
}
