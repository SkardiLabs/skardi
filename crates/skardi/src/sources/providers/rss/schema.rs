//! The Arrow surface of the `feeds` and `items` tables: the two schemas, the
//! builders that turn parsed rows into `RecordBatch`es, and the one column
//! rewrite the engine performs after a batch is built.
//!
//! Column order *is* the batch shape — the exec layer projects by index — so
//! these schemas are the single source of truth for both, and the tests pin
//! every name, type, and nullability against the plan's tables. No projection
//! or filtering happens here; that is exec's job.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use arrow::array::{
    ArrayRef, ListBuilder, RecordBatch, StringArray, StringBuilder, TimestampMillisecondBuilder,
    UInt16Builder, UInt32Array, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};

use super::RSS_SURFACE_VERSION;
use super::parse::ItemRow;

/// Position of `items.window_status`. The engine builds a window's batch once
/// and re-labels it per serve (`fresh` / `revalidated` / `stale-error`), so this
/// index is part of the module's contract — see [`with_window_status`].
pub const WINDOW_STATUS_IDX: usize = 15;

/// Schema-metadata key carrying [`RSS_SURFACE_VERSION`], so a batch that
/// outlives its producer still names the surface generation it was built for.
const SURFACE_VERSION_KEY: &str = "skardi.rss.surface_version";

static ITEMS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(
        Schema::new(vec![
            Field::new("feed", DataType::Utf8, false),
            Field::new("feed_url", DataType::Utf8, false),
            Field::new("guid", DataType::Utf8, false),
            Field::new("title", DataType::Utf8, true),
            Field::new("link", DataType::Utf8, true),
            Field::new("author", DataType::Utf8, true),
            Field::new(
                "published",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "updated",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new("content", DataType::Utf8, true),
            Field::new("summary", DataType::Utf8, true),
            Field::new(
                "categories",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("enclosure_url", DataType::Utf8, true),
            Field::new("enclosure_type", DataType::Utf8, true),
            Field::new("enclosure_length", DataType::UInt64, true),
            Field::new("position", DataType::UInt32, false),
            Field::new("window_status", DataType::Utf8, false),
            Field::new("extensions_json", DataType::Utf8, true),
        ])
        .with_metadata(surface_metadata()),
    )
});

static FEEDS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(
        Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("url", DataType::Utf8, false),
            Field::new("title", DataType::Utf8, true),
            Field::new("site_url", DataType::Utf8, true),
            Field::new("description", DataType::Utf8, true),
            Field::new(
                "last_fetch",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
                true,
            ),
            Field::new("last_status", DataType::Utf8, false),
            Field::new("http_status", DataType::UInt16, true),
            Field::new("last_error", DataType::Utf8, true),
            Field::new("etag", DataType::Utf8, true),
            Field::new("last_modified", DataType::Utf8, true),
            Field::new("dialect", DataType::Utf8, true),
            Field::new("dialect_declared", DataType::Utf8, true),
            Field::new("conformance_notes", DataType::Utf8, true),
            Field::new("item_count", DataType::UInt64, true),
        ])
        .with_metadata(surface_metadata()),
    )
});

fn surface_metadata() -> HashMap<String, String> {
    HashMap::from([(
        SURFACE_VERSION_KEY.to_string(),
        RSS_SURFACE_VERSION.to_string(),
    )])
}

/// One `feeds` row, flat and owned so this module stays independent of the
/// cache's and engine's own types.
#[derive(Debug, Clone)]
pub struct FeedsRow {
    pub name: String,
    pub url: String,
    pub title: Option<String>,
    pub site_url: Option<String>,
    pub description: Option<String>,
    pub last_fetch_ms: Option<i64>,
    pub last_status: &'static str,
    pub http_status: Option<u16>,
    pub last_error: Option<String>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub dialect: Option<String>,
    pub dialect_declared: Option<String>,
    pub conformance_notes: Option<String>,
    pub item_count: Option<u64>,
}

/// The 17-column `items` surface.
pub fn items_schema() -> SchemaRef {
    ITEMS_SCHEMA.clone()
}

/// The 15-column `feeds` surface.
pub fn feeds_schema() -> SchemaRef {
    FEEDS_SCHEMA.clone()
}

/// Encode one feed's window as an `items` batch.
///
/// `feed`/`feed_url` are constant for the batch (a partition is one feed), and
/// `position` is the row's index in the window — the feed's own ordering, which
/// is the only ordering a feed guarantees. `window_status` is filled `"fresh"`;
/// serving a cached window re-labels it via [`with_window_status`].
pub fn build_items_batch(feed: &str, feed_url: &str, items: &[ItemRow]) -> RecordBatch {
    let rows = items.len();
    let mut guid = StringBuilder::new();
    let mut title = StringBuilder::new();
    let mut link = StringBuilder::new();
    let mut author = StringBuilder::new();
    let mut published = TimestampMillisecondBuilder::new();
    let mut updated = TimestampMillisecondBuilder::new();
    let mut content = StringBuilder::new();
    let mut summary = StringBuilder::new();
    let mut categories = ListBuilder::new(StringBuilder::new());
    let mut enclosure_url = StringBuilder::new();
    let mut enclosure_type = StringBuilder::new();
    let mut enclosure_length = UInt64Builder::new();
    let mut extensions_json = StringBuilder::new();

    for item in items {
        guid.append_value(&item.guid);
        title.append_option(item.title.as_deref());
        link.append_option(item.link.as_deref());
        author.append_option(item.author.as_deref());
        published.append_option(item.published_ms);
        updated.append_option(item.updated_ms);
        content.append_option(item.content.as_deref());
        summary.append_option(item.summary.as_deref());
        // No categories is NULL, not `[]` — absence then reads the same as every
        // other absent field on the row, which is what the column's declared
        // nullability is for.
        if item.categories.is_empty() {
            categories.append_null();
        } else {
            for category in &item.categories {
                categories.values().append_value(category);
            }
            categories.append(true);
        }
        enclosure_url.append_option(item.enclosure_url.as_deref());
        enclosure_type.append_option(item.enclosure_type.as_deref());
        enclosure_length.append_option(item.enclosure_length);
        extensions_json.append_option(item.extensions_json.as_deref());
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![feed; rows])),
        Arc::new(StringArray::from(vec![feed_url; rows])),
        Arc::new(guid.finish()),
        Arc::new(title.finish()),
        Arc::new(link.finish()),
        Arc::new(author.finish()),
        Arc::new(published.finish().with_timezone("UTC")),
        Arc::new(updated.finish().with_timezone("UTC")),
        Arc::new(content.finish()),
        Arc::new(summary.finish()),
        Arc::new(categories.finish()),
        Arc::new(enclosure_url.finish()),
        Arc::new(enclosure_type.finish()),
        Arc::new(enclosure_length.finish()),
        Arc::new(UInt32Array::from_iter_values(0..rows as u32)),
        Arc::new(StringArray::from(vec!["fresh"; rows])),
        Arc::new(extensions_json.finish()),
    ];

    RecordBatch::try_new(items_schema(), columns)
        .expect("built columns match the items schema declared above")
}

/// Re-label a window's freshness without rebuilding it.
///
/// Every other column and the schema are shared by `Arc`, so this costs one
/// `status`-wide string array — cheap enough to run on every serve of a cached
/// window.
pub fn with_window_status(batch: &RecordBatch, status: &str) -> RecordBatch {
    let mut columns = batch.columns().to_vec();
    columns[WINDOW_STATUS_IDX] = Arc::new(StringArray::from(vec![status; batch.num_rows()]));
    RecordBatch::try_new(batch.schema(), columns)
        .expect("only window_status changed, and it stayed Utf8 with the same length")
}

/// Encode feed health observations as a `feeds` batch.
pub fn build_feeds_batch(rows: &[FeedsRow]) -> RecordBatch {
    let mut name = StringBuilder::new();
    let mut url = StringBuilder::new();
    let mut title = StringBuilder::new();
    let mut site_url = StringBuilder::new();
    let mut description = StringBuilder::new();
    let mut last_fetch = TimestampMillisecondBuilder::new();
    let mut last_status = StringBuilder::new();
    let mut http_status = UInt16Builder::new();
    let mut last_error = StringBuilder::new();
    let mut etag = StringBuilder::new();
    let mut last_modified = StringBuilder::new();
    let mut dialect = StringBuilder::new();
    let mut dialect_declared = StringBuilder::new();
    let mut conformance_notes = StringBuilder::new();
    let mut item_count = UInt64Builder::new();

    for row in rows {
        name.append_value(&row.name);
        url.append_value(&row.url);
        title.append_option(row.title.as_deref());
        site_url.append_option(row.site_url.as_deref());
        description.append_option(row.description.as_deref());
        last_fetch.append_option(row.last_fetch_ms);
        last_status.append_value(row.last_status);
        http_status.append_option(row.http_status);
        last_error.append_option(row.last_error.as_deref());
        etag.append_option(row.etag.as_deref());
        last_modified.append_option(row.last_modified.as_deref());
        dialect.append_option(row.dialect.as_deref());
        dialect_declared.append_option(row.dialect_declared.as_deref());
        conformance_notes.append_option(row.conformance_notes.as_deref());
        item_count.append_option(row.item_count);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(name.finish()),
        Arc::new(url.finish()),
        Arc::new(title.finish()),
        Arc::new(site_url.finish()),
        Arc::new(description.finish()),
        Arc::new(last_fetch.finish().with_timezone("UTC")),
        Arc::new(last_status.finish()),
        Arc::new(http_status.finish()),
        Arc::new(last_error.finish()),
        Arc::new(etag.finish()),
        Arc::new(last_modified.finish()),
        Arc::new(dialect.finish()),
        Arc::new(dialect_declared.finish()),
        Arc::new(conformance_notes.finish()),
        Arc::new(item_count.finish()),
    ];

    RecordBatch::try_new(feeds_schema(), columns)
        .expect("built columns match the feeds schema declared above")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ListArray, StringArray, TimestampMillisecondArray, UInt16Array, UInt32Array,
        UInt64Array,
    };
    use arrow::datatypes::{DataType, Field, TimeUnit};

    use super::*;

    const FEED: &str = "news";
    const FEED_URL: &str = "https://example.com/feed.xml";

    /// The expected types are restated here rather than shared with the
    /// implementation on purpose: a helper used by both would let a wrong type
    /// satisfy its own assertion.
    fn ts_utc() -> DataType {
        DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
    }

    fn utf8_list() -> DataType {
        DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
    }

    fn assert_surface(schema: &SchemaRef, expected: &[(&str, DataType, bool)], count: usize) {
        assert_eq!(expected.len(), count, "the test's own expectation list");
        assert_eq!(schema.fields().len(), count, "column count");
        for (idx, (name, data_type, nullable)) in expected.iter().enumerate() {
            let field = schema.field(idx);
            assert_eq!(field.name(), name, "column {idx} name");
            assert_eq!(field.data_type(), data_type, "column {idx} `{name}` type");
            assert_eq!(
                field.is_nullable(),
                *nullable,
                "column {idx} `{name}` nullability"
            );
        }
        // Spelled out rather than taken from the constant: the key is the
        // wire-visible name, so a rename must fail here.
        assert_eq!(
            schema
                .metadata()
                .get("skardi.rss.surface_version")
                .map(String::as_str),
            Some("1"),
            "surface-version metadata"
        );
    }

    fn strings(batch: &RecordBatch, idx: usize) -> Vec<Option<&str>> {
        batch
            .column(idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap_or_else(|| panic!("column {idx} is not Utf8"))
            .iter()
            .collect()
    }

    fn two_items() -> Vec<ItemRow> {
        vec![
            ItemRow {
                guid: "urn:uuid:1".into(),
                title: Some("First post".into()),
                link: Some("https://example.com/1".into()),
                author: Some("Ada".into()),
                published_ms: Some(1_700_000_000_000),
                updated_ms: Some(1_700_000_060_000),
                content: Some("# body".into()),
                summary: Some("teaser".into()),
                categories: vec!["rust".into(), "arrow".into()],
                enclosure_url: Some("https://example.com/1.mp3".into()),
                enclosure_type: Some("audio/mpeg".into()),
                enclosure_length: Some(1024),
                extensions_json: Some(r#"{"rights":"CC0"}"#.into()),
            },
            // Every optional absent: the null-propagation half of the round trip.
            ItemRow {
                guid: "urn:uuid:2".into(),
                title: None,
                link: None,
                author: None,
                published_ms: None,
                updated_ms: None,
                content: None,
                summary: None,
                categories: Vec::new(),
                enclosure_url: None,
                enclosure_type: None,
                enclosure_length: None,
                extensions_json: None,
            },
        ]
    }

    fn never_row() -> FeedsRow {
        FeedsRow {
            name: FEED.into(),
            url: FEED_URL.into(),
            title: None,
            site_url: None,
            description: None,
            last_fetch_ms: None,
            last_status: "never",
            http_status: None,
            last_error: None,
            etag: None,
            last_modified: None,
            dialect: None,
            dialect_declared: None,
            conformance_notes: None,
            item_count: None,
        }
    }

    #[test]
    fn items_schema_matches_spec() {
        let schema = items_schema();
        let expected: Vec<(&str, DataType, bool)> = vec![
            ("feed", DataType::Utf8, false),
            ("feed_url", DataType::Utf8, false),
            ("guid", DataType::Utf8, false),
            ("title", DataType::Utf8, true),
            ("link", DataType::Utf8, true),
            ("author", DataType::Utf8, true),
            ("published", ts_utc(), true),
            ("updated", ts_utc(), true),
            ("content", DataType::Utf8, true),
            ("summary", DataType::Utf8, true),
            ("categories", utf8_list(), true),
            ("enclosure_url", DataType::Utf8, true),
            ("enclosure_type", DataType::Utf8, true),
            ("enclosure_length", DataType::UInt64, true),
            ("position", DataType::UInt32, false),
            ("window_status", DataType::Utf8, false),
            ("extensions_json", DataType::Utf8, true),
        ];
        assert_surface(&schema, &expected, 17);
        // The engine rewrites this column by index; a reorder must fail here
        // rather than silently mislabel every row's freshness.
        assert_eq!(schema.field(WINDOW_STATUS_IDX).name(), "window_status");
    }

    #[test]
    fn feeds_schema_matches_spec() {
        let expected: Vec<(&str, DataType, bool)> = vec![
            ("name", DataType::Utf8, false),
            ("url", DataType::Utf8, false),
            ("title", DataType::Utf8, true),
            ("site_url", DataType::Utf8, true),
            ("description", DataType::Utf8, true),
            ("last_fetch", ts_utc(), true),
            ("last_status", DataType::Utf8, false),
            ("http_status", DataType::UInt16, true),
            ("last_error", DataType::Utf8, true),
            ("etag", DataType::Utf8, true),
            ("last_modified", DataType::Utf8, true),
            ("dialect", DataType::Utf8, true),
            ("dialect_declared", DataType::Utf8, true),
            ("conformance_notes", DataType::Utf8, true),
            ("item_count", DataType::UInt64, true),
        ];
        assert_surface(&feeds_schema(), &expected, 15);
    }

    #[test]
    fn items_batch_round_trips_rows() {
        let batch = build_items_batch(FEED, FEED_URL, &two_items());
        assert_eq!(batch.schema(), items_schema());
        assert_eq!(batch.num_rows(), 2);

        assert_eq!(strings(&batch, 0), vec![Some(FEED), Some(FEED)]);
        assert_eq!(strings(&batch, 1), vec![Some(FEED_URL), Some(FEED_URL)]);
        assert_eq!(
            strings(&batch, 2),
            vec![Some("urn:uuid:1"), Some("urn:uuid:2")]
        );
        assert_eq!(strings(&batch, 3), vec![Some("First post"), None]);
        assert_eq!(
            strings(&batch, 4),
            vec![Some("https://example.com/1"), None]
        );
        assert_eq!(strings(&batch, 5), vec![Some("Ada"), None]);

        let published = batch
            .column(6)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("published is Timestamp(ms)");
        assert_eq!(
            published.iter().collect::<Vec<_>>(),
            vec![Some(1_700_000_000_000), None]
        );
        assert_eq!(
            published.data_type(),
            &ts_utc(),
            "the array must carry the column's timezone, not a bare Timestamp"
        );
        let updated = batch
            .column(7)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("updated is Timestamp(ms)");
        assert_eq!(
            updated.iter().collect::<Vec<_>>(),
            vec![Some(1_700_000_060_000), None]
        );

        assert_eq!(strings(&batch, 8), vec![Some("# body"), None]);
        assert_eq!(strings(&batch, 9), vec![Some("teaser"), None]);

        let categories = batch
            .column(10)
            .as_any()
            .downcast_ref::<ListArray>()
            .expect("categories is a List");
        let first = categories.value(0);
        assert_eq!(
            first
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("list items are Utf8")
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("rust"), Some("arrow")]
        );
        // NULL rather than `[]`: every other absent field is NULL, and that
        // consistency is what makes the column's declared nullability mean
        // something.
        assert!(
            categories.is_null(1),
            "an item with no categories is NULL, not an empty list"
        );

        assert_eq!(
            strings(&batch, 11),
            vec![Some("https://example.com/1.mp3"), None]
        );
        assert_eq!(strings(&batch, 12), vec![Some("audio/mpeg"), None]);
        let length = batch
            .column(13)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("enclosure_length is UInt64");
        assert_eq!(length.iter().collect::<Vec<_>>(), vec![Some(1024), None]);

        let position = batch
            .column(14)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .expect("position is UInt32");
        assert_eq!(
            position.iter().collect::<Vec<_>>(),
            vec![Some(0), Some(1)],
            "position is the row's index in the window"
        );
        assert_eq!(
            strings(&batch, WINDOW_STATUS_IDX),
            vec![Some("fresh"), Some("fresh")]
        );
        assert_eq!(strings(&batch, 16), vec![Some(r#"{"rights":"CC0"}"#), None]);
    }

    #[test]
    fn with_window_status_swaps_only_column_15() {
        let batch = build_items_batch(FEED, FEED_URL, &two_items());
        let relabelled = with_window_status(&batch, "stale-error");

        for idx in 0..batch.num_columns() {
            if idx == WINDOW_STATUS_IDX {
                continue;
            }
            assert!(
                Arc::ptr_eq(batch.column(idx), relabelled.column(idx)),
                "column {idx} was rebuilt instead of shared"
            );
        }
        assert!(
            Arc::ptr_eq(&batch.schema(), &relabelled.schema()),
            "the schema was rebuilt instead of shared"
        );
        assert_eq!(
            strings(&relabelled, WINDOW_STATUS_IDX),
            vec![Some("stale-error"), Some("stale-error")]
        );
        assert_eq!(
            strings(&batch, WINDOW_STATUS_IDX),
            vec![Some("fresh"), Some("fresh")],
            "the source batch must be untouched — it is the cached window"
        );
    }

    #[test]
    fn feeds_batch_round_trips() {
        let observed = FeedsRow {
            name: "blog".into(),
            url: "https://blog.example.com/atom.xml".into(),
            title: Some("The Blog".into()),
            site_url: Some("https://blog.example.com/".into()),
            description: Some("posts".into()),
            last_fetch_ms: Some(1_700_000_000_000),
            last_status: "revalidated",
            http_status: Some(304),
            last_error: None,
            etag: Some("\"abc\"".into()),
            last_modified: Some("Wed, 21 Oct 2015 07:28:00 GMT".into()),
            dialect: Some("atom".into()),
            dialect_declared: Some("atom".into()),
            conformance_notes: Some("sanitation: stripped-control-chars".into()),
            item_count: Some(7),
        };
        let batch = build_feeds_batch(&[never_row(), observed]);
        assert_eq!(batch.schema(), feeds_schema());
        assert_eq!(batch.num_rows(), 2);

        // Row 0 — never fetched: only identity and status are known.
        assert_eq!(strings(&batch, 0), vec![Some(FEED), Some("blog")]);
        assert_eq!(strings(&batch, 1)[0], Some(FEED_URL));
        assert_eq!(strings(&batch, 6), vec![Some("never"), Some("revalidated")]);
        for idx in [2, 3, 4, 5, 7, 8, 9, 10, 11, 12, 13, 14] {
            assert!(
                batch.column(idx).is_null(0),
                "column {idx} `{}` must be NULL before the first fetch",
                batch.schema().field(idx).name()
            );
        }

        // Row 1 — everything the engine can observe, round-tripped.
        assert_eq!(strings(&batch, 2)[1], Some("The Blog"));
        assert_eq!(strings(&batch, 3)[1], Some("https://blog.example.com/"));
        assert_eq!(strings(&batch, 4)[1], Some("posts"));
        let last_fetch = batch
            .column(5)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("last_fetch is Timestamp(ms)");
        assert_eq!(last_fetch.data_type(), &ts_utc());
        assert_eq!(last_fetch.value(1), 1_700_000_000_000);
        let http_status = batch
            .column(7)
            .as_any()
            .downcast_ref::<UInt16Array>()
            .expect("http_status is UInt16");
        assert_eq!(http_status.value(1), 304);
        assert_eq!(
            strings(&batch, 8)[1],
            None,
            "a revalidated feed carries no error"
        );
        assert_eq!(strings(&batch, 9)[1], Some("\"abc\""));
        assert_eq!(
            strings(&batch, 10)[1],
            Some("Wed, 21 Oct 2015 07:28:00 GMT")
        );
        assert_eq!(strings(&batch, 11)[1], Some("atom"));
        assert_eq!(strings(&batch, 12)[1], Some("atom"));
        assert_eq!(
            strings(&batch, 13)[1],
            Some("sanitation: stripped-control-chars")
        );
        let item_count = batch
            .column(14)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("item_count is UInt64");
        assert_eq!(item_count.value(1), 7);
    }

    #[test]
    fn empty_items_batch_has_zero_rows_17_cols() {
        let batch = build_items_batch(FEED, FEED_URL, &[]);
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 17);
        assert_eq!(batch.schema(), items_schema());
        // A feed can legitimately serve an empty window; re-labelling one must
        // not be the path that panics.
        let relabelled = with_window_status(&batch, "revalidated");
        assert_eq!(relabelled.num_rows(), 0);
        assert_eq!(relabelled.num_columns(), 17);
    }
}
