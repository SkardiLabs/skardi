//! The Arrow surface: three schemas, one `TableProvider` parameterized by
//! table kind, and one single-partition `ExecutionPlan` that runs the scan on
//! the blocking pool and emits one `RecordBatch`. Column order *is* the batch
//! shape (the exec projects by index), so the schemas here are the single
//! source of truth and the tests pin every name, type and nullability.

use std::any::Any;
use std::collections::HashMap;
use std::fmt::{self, Formatter};
use std::sync::{Arc, LazyLock};

use arrow::array::{
    ArrayRef, Int32Array, Int64Array, ListBuilder, RecordBatch, RecordBatchOptions, StringArray,
    StringBuilder, TimestampMillisecondArray,
};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalSortExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::prelude::Expr;
use futures::stream;

use super::config::ScanOptions;
use super::scan::{LinkRow, ParsedNote, TagRow, run_scan};
use super::{LINKS_TABLE, NOTES_TABLE, OBSIDIAN_SURFACE_VERSION, TAGS_TABLE};

/// Schema-metadata key carrying [`OBSIDIAN_SURFACE_VERSION`].
const SURFACE_VERSION_KEY: &str = "skardi.obsidian.surface_version";

fn surface_metadata() -> HashMap<String, String> {
    HashMap::from([(
        SURFACE_VERSION_KEY.to_string(),
        OBSIDIAN_SURFACE_VERSION.to_string(),
    )])
}

fn utc_millis() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

static NOTES_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(
        Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("folder", DataType::Utf8, false),
            Field::new("body", DataType::Utf8, false),
            Field::new("frontmatter_json", DataType::Utf8, true),
            Field::new("frontmatter_error", DataType::Utf8, true),
            Field::new(
                "aliases",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new("size_bytes", DataType::Int64, false),
            Field::new("modified_at", utc_millis(), false),
        ])
        .with_metadata(surface_metadata()),
    )
});

static LINKS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(
        Schema::new(vec![
            Field::new("from_path", DataType::Utf8, false),
            Field::new("to_path", DataType::Utf8, true),
            Field::new("target", DataType::Utf8, false),
            Field::new("kind", DataType::Utf8, false),
            Field::new("display_text", DataType::Utf8, true),
            Field::new("heading", DataType::Utf8, true),
            Field::new("block_id", DataType::Utf8, true),
            Field::new("resolution", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, false),
            Field::new("line", DataType::Int32, true),
        ])
        .with_metadata(surface_metadata()),
    )
});

static TAGS_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(
        Schema::new(vec![
            Field::new("path", DataType::Utf8, false),
            Field::new("tag", DataType::Utf8, false),
            Field::new("source", DataType::Utf8, false),
        ])
        .with_metadata(surface_metadata()),
    )
});

/// Which of the three tables a provider serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableKind {
    Notes,
    Links,
    Tags,
}

impl TableKind {
    pub fn table_name(self) -> &'static str {
        match self {
            TableKind::Notes => NOTES_TABLE,
            TableKind::Links => LINKS_TABLE,
            TableKind::Tags => TAGS_TABLE,
        }
    }

    pub fn schema(self) -> SchemaRef {
        match self {
            TableKind::Notes => NOTES_SCHEMA.clone(),
            TableKind::Links => LINKS_SCHEMA.clone(),
            TableKind::Tags => TAGS_SCHEMA.clone(),
        }
    }
}

/// Read-only `TableProvider` over one vault root for one table kind.
#[derive(Debug)]
pub struct ObsidianTable {
    kind: TableKind,
    root: String,
    opts: ScanOptions,
}

impl ObsidianTable {
    pub fn new(kind: TableKind, root: String, opts: ScanOptions) -> Self {
        Self { kind, root, opts }
    }
}

#[async_trait]
impl TableProvider for ObsidianTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.kind.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let full = self.kind.schema();
        let projection = projection
            .cloned()
            .unwrap_or_else(|| (0..full.fields().len()).collect());
        let projected_schema = Arc::new(full.project(&projection)?);
        let properties = PlanProperties::new(
            EquivalenceProperties::new_with_orderings(
                projected_schema.clone(),
                declared_ordering(self.kind, &projected_schema),
            ),
            Partitioning::UnknownPartitioning(1),
            // One batch after the whole scan; nothing is emitted incrementally.
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Ok(Arc::new(ObsidianScanExec {
            kind: self.kind,
            root: self.root.clone(),
            opts: self.opts.clone(),
            projected_schema,
            projection,
            limit,
            properties,
        }))
    }
}

/// Single-partition scan: one `spawn_blocking` vault scan, one batch.
#[derive(Debug)]
struct ObsidianScanExec {
    kind: TableKind,
    root: String,
    opts: ScanOptions,
    projected_schema: SchemaRef,
    projection: Vec<usize>,
    limit: Option<usize>,
    properties: PlanProperties,
}

impl DisplayAs for ObsidianScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "ObsidianScanExec: table={}, root={}, projected_cols={:?}, limit={:?}",
                self.kind.table_name(),
                self.root,
                self.projection,
                self.limit
            ),
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "ObsidianScanExec({}: {})",
                    self.kind.table_name(),
                    self.root
                )
            }
        }
    }
}

impl ExecutionPlan for ObsidianScanExec {
    fn name(&self) -> &str {
        "ObsidianScanExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let kind = self.kind;
        let root = self.root.clone();
        let opts = self.opts.clone();
        let projection = self.projection.clone();
        let limit = self.limit;
        let batch_schema = schema.clone();
        // The scan runs on the blocking pool (see scan.rs); nothing here blocks
        // the worker that polls this stream.
        let batch = async move {
            let notes = run_scan(root, opts)
                .await
                .map_err(|e| DataFusionError::External(e.into()))?;
            build_batch(kind, &notes, &batch_schema, &projection, limit)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(batch),
        )))
    }
}

/// The order [`build_batch`] emits rows in, as far as the projection keeps the
/// leading columns: `notes` by `path`; `links` by `from_path` (within a note
/// frontmatter links precede body links by line, which is not a column order,
/// so only that prefix is declared); `tags` by `(path, tag, source)`. Declaring
/// it lets the planner drop an `ORDER BY` on these columns instead of sorting
/// again. Empty when the leading column is projected away.
fn declared_ordering(kind: TableKind, projected: &Schema) -> Vec<Vec<PhysicalSortExpr>> {
    let columns: &[&str] = match kind {
        TableKind::Notes => &["path"],
        TableKind::Links => &["from_path"],
        TableKind::Tags => &["path", "tag", "source"],
    };
    let mut ordering = Vec::new();
    for name in columns {
        let Ok(idx) = projected.index_of(name) else {
            break;
        };
        ordering.push(PhysicalSortExpr::new(
            Arc::new(Column::new(name, idx)),
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        ));
    }
    if ordering.is_empty() {
        Vec::new()
    } else {
        vec![ordering]
    }
}

/// Materialize `projection` through `column`, which yields `None` for an index
/// outside the table's schema. `Schema::project` already rejected such an
/// index in `scan`, so this is an internal error, not a panic.
fn columns(
    projection: &[usize],
    column: impl Fn(usize) -> Option<ArrayRef>,
) -> datafusion::common::Result<Vec<ArrayRef>> {
    projection
        .iter()
        .map(|&i| {
            column(i).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "obsidian: projection index {i} is outside the table schema"
                ))
            })
        })
        .collect()
}

/// Project + limit the parsed notes into one batch of `kind`. An empty
/// projection (`count(*)`) still carries the row count.
pub(crate) fn build_batch(
    kind: TableKind,
    notes: &[ParsedNote],
    projected_schema: &SchemaRef,
    projection: &[usize],
    limit: Option<usize>,
) -> datafusion::common::Result<RecordBatch> {
    let (row_count, arrays) = match kind {
        TableKind::Notes => {
            let rows = truncate(notes, limit);
            (rows.len(), columns(projection, |i| notes_column(i, rows))?)
        }
        TableKind::Links => {
            let all: Vec<(&str, &LinkRow)> = notes
                .iter()
                .flat_map(|n| n.links.iter().map(move |l| (n.path.as_str(), l)))
                .collect();
            let rows = truncate(&all, limit);
            (rows.len(), columns(projection, |i| links_column(i, rows))?)
        }
        TableKind::Tags => {
            let all: Vec<(&str, &TagRow)> = notes
                .iter()
                .flat_map(|n| n.tags.iter().map(move |t| (n.path.as_str(), t)))
                .collect();
            let rows = truncate(&all, limit);
            (rows.len(), columns(projection, |i| tags_column(i, rows))?)
        }
    };
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    RecordBatch::try_new_with_options(projected_schema.clone(), arrays, &options)
        .map_err(|e| DataFusionError::Execution(format!("obsidian: building RecordBatch: {e}")))
}

fn truncate<T>(rows: &[T], limit: Option<usize>) -> &[T] {
    match limit {
        Some(max) if max < rows.len() => &rows[..max],
        _ => rows,
    }
}

fn utf8<'a>(values: impl Iterator<Item = Option<&'a str>>) -> ArrayRef {
    Arc::new(StringArray::from_iter(values))
}

fn notes_column(idx: usize, rows: &[ParsedNote]) -> Option<ArrayRef> {
    let array: ArrayRef = match idx {
        0 => utf8(rows.iter().map(|n| Some(n.path.as_str()))),
        1 => utf8(rows.iter().map(|n| Some(n.name.as_str()))),
        2 => utf8(rows.iter().map(|n| Some(n.folder.as_str()))),
        3 => utf8(rows.iter().map(|n| Some(n.body.as_str()))),
        4 => utf8(rows.iter().map(|n| n.frontmatter_json.as_deref())),
        5 => utf8(rows.iter().map(|n| n.frontmatter_error.as_deref())),
        6 => {
            let mut builder = ListBuilder::new(StringBuilder::new());
            for note in rows {
                match &note.aliases {
                    Some(aliases) => {
                        for alias in aliases {
                            builder.values().append_value(alias);
                        }
                        builder.append(true);
                    }
                    None => builder.append_null(),
                }
            }
            Arc::new(builder.finish())
        }
        7 => Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|n| n.size_bytes),
        )),
        8 => Arc::new(
            TimestampMillisecondArray::from_iter_values(rows.iter().map(|n| n.modified_ms))
                .with_timezone("UTC"),
        ),
        _ => return None,
    };
    Some(array)
}

fn links_column(idx: usize, rows: &[(&str, &LinkRow)]) -> Option<ArrayRef> {
    let array: ArrayRef = match idx {
        0 => utf8(rows.iter().map(|(from, _)| Some(*from))),
        1 => utf8(rows.iter().map(|(_, l)| l.to_path.as_deref())),
        2 => utf8(rows.iter().map(|(_, l)| Some(l.target.as_str()))),
        3 => utf8(rows.iter().map(|(_, l)| Some(l.kind.as_str()))),
        4 => utf8(rows.iter().map(|(_, l)| l.display_text.as_deref())),
        5 => utf8(rows.iter().map(|(_, l)| l.heading.as_deref())),
        6 => utf8(rows.iter().map(|(_, l)| l.block_id.as_deref())),
        7 => utf8(rows.iter().map(|(_, l)| Some(l.resolution.as_str()))),
        8 => utf8(rows.iter().map(|(_, l)| Some(l.source.as_str()))),
        9 => Arc::new(Int32Array::from_iter(
            rows.iter()
                .map(|(_, l)| l.line.and_then(|line| i32::try_from(line).ok())),
        )),
        _ => return None,
    };
    Some(array)
}

fn tags_column(idx: usize, rows: &[(&str, &TagRow)]) -> Option<ArrayRef> {
    let array: ArrayRef = match idx {
        0 => utf8(rows.iter().map(|(path, _)| Some(*path))),
        1 => utf8(rows.iter().map(|(_, t)| Some(t.tag.as_str()))),
        2 => utf8(rows.iter().map(|(_, t)| Some(t.source.as_str()))),
        _ => return None,
    };
    Some(array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::obsidian::resolve::Index;
    use crate::sources::providers::obsidian::scan::parse_note;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::Int64Type;
    use chrono::{DateTime, Utc};
    use datafusion::prelude::SessionContext;
    use std::path::Path;
    use std::time::UNIX_EPOCH;

    fn epoch() -> DateTime<Utc> {
        DateTime::<Utc>::from(UNIX_EPOCH)
    }

    fn two_notes() -> Vec<ParsedNote> {
        let index = Index::build(&["A.md", "Sub/B.md"]);
        vec![
            parse_note(
                "A.md",
                10,
                epoch(),
                "---\naliases: [x, y]\nrel: \"[[B]]\"\ntags: t\n---\nbody [[B|bee]] #a\n",
                &index,
            ),
            parse_note("Sub/B.md", 0, epoch(), "", &index),
        ]
    }

    fn field(schema: &SchemaRef, name: &str) -> (DataType, bool) {
        let f = schema.field_with_name(name).unwrap();
        (f.data_type().clone(), f.is_nullable())
    }

    #[test]
    fn schemas_pin_the_spec_tables() {
        let ts = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        let list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));

        let notes = TableKind::Notes.schema();
        let names: Vec<&str> = notes.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "path",
                "name",
                "folder",
                "body",
                "frontmatter_json",
                "frontmatter_error",
                "aliases",
                "size_bytes",
                "modified_at"
            ]
        );
        assert_eq!(field(&notes, "path"), (DataType::Utf8, false));
        assert_eq!(field(&notes, "body"), (DataType::Utf8, false));
        assert_eq!(field(&notes, "frontmatter_json"), (DataType::Utf8, true));
        assert_eq!(field(&notes, "frontmatter_error"), (DataType::Utf8, true));
        assert_eq!(field(&notes, "aliases"), (list, true));
        assert_eq!(field(&notes, "size_bytes"), (DataType::Int64, false));
        assert_eq!(field(&notes, "modified_at"), (ts, false));

        let links = TableKind::Links.schema();
        let names: Vec<&str> = links.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "from_path",
                "to_path",
                "target",
                "kind",
                "display_text",
                "heading",
                "block_id",
                "resolution",
                "source",
                "line"
            ]
        );
        assert_eq!(field(&links, "to_path"), (DataType::Utf8, true));
        assert_eq!(field(&links, "target"), (DataType::Utf8, false));
        assert_eq!(field(&links, "kind"), (DataType::Utf8, false));
        assert_eq!(field(&links, "line"), (DataType::Int32, true));

        let tags = TableKind::Tags.schema();
        let names: Vec<&str> = tags.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(names, vec!["path", "tag", "source"]);
        assert!(tags.fields().iter().all(|f| !f.is_nullable()));

        for kind in [TableKind::Notes, TableKind::Links, TableKind::Tags] {
            assert_eq!(
                kind.schema()
                    .metadata()
                    .get("skardi.obsidian.surface_version")
                    .map(String::as_str),
                Some("1")
            );
        }
        assert_eq!(TableKind::Notes.table_name(), "notes");
        assert_eq!(TableKind::Links.table_name(), "links");
        assert_eq!(TableKind::Tags.table_name(), "tags");
    }

    #[test]
    fn notes_batch_has_aliases_list_and_nulls() {
        let notes = two_notes();
        let schema = TableKind::Notes.schema();
        let all: Vec<usize> = (0..schema.fields().len()).collect();
        let batch = build_batch(TableKind::Notes, &notes, &schema, &all, None).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let aliases = batch.column(6).as_list::<i32>();
        assert!(!aliases.is_null(0));
        assert!(aliases.is_null(1));
        let first = aliases.value(0);
        let first = first.as_string::<i32>();
        assert_eq!(first.value(0), "x");
        assert_eq!(first.value(1), "y");
        assert!(batch.column(4).is_null(1)); // no frontmatter → NULL json
        assert!(batch.column(5).is_null(0)); // valid frontmatter → NULL error
        let sizes = batch.column(7).as_primitive::<Int64Type>();
        assert_eq!(sizes.value(0), 10);
        assert_eq!(sizes.value(1), 0);
        assert_eq!(
            batch.column(8).data_type(),
            &DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
        );
    }

    #[test]
    fn limit_and_projection_apply_to_every_kind() {
        let notes = two_notes();
        // notes: LIMIT 1, only `name`.
        let schema = Arc::new(TableKind::Notes.schema().project(&[1]).unwrap());
        let batch = build_batch(TableKind::Notes, &notes, &schema, &[1], Some(1)).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(0).as_string::<i32>().value(0), "A");

        // links: A.md has 2 (one frontmatter, one body); LIMIT 5 keeps both.
        let schema = TableKind::Links.schema();
        let all: Vec<usize> = (0..schema.fields().len()).collect();
        let batch = build_batch(TableKind::Links, &notes, &schema, &all, Some(5)).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.column(8).as_string::<i32>().value(0), "frontmatter");
        assert!(batch.column(9).is_null(0)); // frontmatter line NULL
        assert_eq!(batch.column(8).as_string::<i32>().value(1), "body");
        assert_eq!(
            batch
                .column(9)
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(1),
            6
        );
        assert_eq!(batch.column(4).as_string::<i32>().value(1), "bee");

        // tags: (a, body), (t, frontmatter); LIMIT 1.
        let schema = TableKind::Tags.schema();
        let batch = build_batch(TableKind::Tags, &notes, &schema, &[0, 1, 2], Some(1)).unwrap();
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.column(1).as_string::<i32>().value(0), "a");
    }

    #[test]
    fn empty_projection_keeps_the_row_count() {
        let notes = two_notes();
        let schema = Arc::new(TableKind::Links.schema().project(&[]).unwrap());
        let batch = build_batch(TableKind::Links, &notes, &schema, &[], None).unwrap();
        assert_eq!(batch.num_columns(), 0);
        assert_eq!(batch.num_rows(), 2);
    }

    #[tokio::test]
    async fn table_is_queryable_through_datafusion() -> datafusion::common::Result<()> {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src/sources/providers/obsidian/fixtures/vault")
            .to_string_lossy()
            .into_owned();
        let opts =
            ScanOptions::from_map(None).map_err(|e| DataFusionError::External(Box::new(e)))?;
        let ctx = SessionContext::new();
        ctx.register_table(
            "notes",
            Arc::new(ObsidianTable::new(TableKind::Notes, root, opts)),
        )?;
        let batches = ctx
            .sql("SELECT count(*) FROM notes")
            .await?
            .collect()
            .await?;
        assert_eq!(
            batches[0].column(0).as_primitive::<Int64Type>().value(0),
            12
        );

        let plan = ctx
            .sql("EXPLAIN SELECT path FROM notes LIMIT 2")
            .await?
            .collect()
            .await?;
        let text = arrow::util::pretty::pretty_format_batches(&plan)?.to_string();
        assert!(text.contains("ObsidianScanExec"), "{text}");
        Ok(())
    }
}
