//! Synthetic mock source pack: proves the source-pack abstraction without
//! a real SaaS. The mock gateway in tests implements the `mock.list_items`
//! action with page-number pagination.

use datafusion::logical_expr::Operator;

use crate::sources::providers::open_connector::filters::{Fidelity, FilterMapping, ValueFormat};
use crate::sources::providers::open_connector::json_to_arrow::{FieldMapping, FieldType};
use crate::sources::providers::open_connector::pagination::PaginationStrategy;
use crate::sources::providers::open_connector::source_pack::{SourcePack, SourcePackTable};

/// The mock `items` table.
static MOCK_ITEMS: SourcePackTable = SourcePackTable {
    id: "mock.items",
    action_id: "mock.list_items",
    row_path: "$.items",
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
            name: "value",
            path: "value",
            field_type: FieldType::Float64,
            nullable: true,
        },
        FieldMapping {
            name: "tags",
            path: "tags",
            field_type: FieldType::Utf8List,
            nullable: true,
        },
        FieldMapping {
            name: "created_at",
            path: "created_at",
            field_type: FieldType::TimestampMillisUtc,
            nullable: true,
        },
    ],
    // Small page size so tests exercise multi-page scans with few rows.
    pagination: PaginationStrategy::PageNumber {
        page_param: "page",
        per_page_param: "per_page",
        per_page: 2,
        total_pages_path: None,
    },
    required_resources: &["workspace"],
    fixed_inputs: &[],
    // NOTE: only `>` is mapped — the gateway's `min_value` is strictly
    // greater-than. Mapping `>=` to the same input would be classified Exact
    // and silently drop the boundary row (provider excludes it, DataFusion
    // never reapplies). `>=` therefore stays in DataFusion.
    filters: &[FilterMapping {
        column: "value",
        operator: Operator::Gt,
        input_field: "min_value",
        fidelity: Fidelity::Exact,
        value_format: ValueFormat::Rfc3339,
    }],
    // The mock gateway's action schema is test-controlled, so no fingerprint
    // is pinned; real packs pin one.
    expected_fingerprint: None,
};

/// The synthetic mock source pack (version 1).
pub static MOCK_PACK: SourcePack = SourcePack {
    name: "mock",
    version: 1,
    tables: &[MOCK_ITEMS],
};
