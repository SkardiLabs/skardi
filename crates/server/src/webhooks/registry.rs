//! Webhook definitions compiled against the live session.
//!
//! Compilation does the work that must not wait for the first delivery:
//! building each route's column converter, and checking that each
//! destination table exists, belongs to a transactional read-write source,
//! and has exactly the columns the route will write — in order, with
//! compatible types, and no `NOT NULL` where the route may produce NULL.
//! The SQL writer matches columns **by position** (`INSERT INTO t SELECT *
//! FROM staging`), so a table whose columns are shuffled or one short would
//! not fail loudly at write time — it would land values in the wrong
//! columns, or fail every delivery with a planning error that Meta then
//! retries for a week. Refusing to start is the cheaper failure.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow::array::{ArrayRef, StringArray, TimestampMillisecondArray};
use arrow::compute::{can_cast_types, concat_batches};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use serde_json::Value;
use skardi::jobs::{JobDestination, SqlDmlDestination};
use skardi::sources::DataSourceType;
use skardi::sources::providers::open_connector::json_to_arrow::RowConverter;

use super::INGESTED_AT_COLUMN;
use super::definition::{RouteDefinition, RowSelector, WebhookDefinition};
use crate::config::DataSource;

/// Source types whose insert path wraps a delivery's rows in one
/// transaction, so a failed write leaves nothing behind for the provider's
/// retry to duplicate. Mirrors the jobs executor's rule.
const TRANSACTIONAL_SOURCE_TYPES: &[DataSourceType] = &[
    DataSourceType::Postgres,
    DataSourceType::Mysql,
    DataSourceType::Sqlite,
];

/// Columns of a dead-letter table, in order: the route that rejected the
/// row, the row as received (JSON text), the converter's error, and the
/// arrival time. Fixed, so one table serves every route of a webhook.
pub const DEAD_LETTER_COLUMNS: [&str; 4] = ["route", "raw", "error", INGESTED_AT_COLUMN];

fn ingested_at_type() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()))
}

/// One route, ready to turn rows into a batch shaped for its table.
pub struct CompiledRoute {
    /// See [`RouteDefinition::field`].
    pub field: String,
    /// See [`RouteDefinition::rows`].
    pub rows: RowSelector,
    /// See [`RouteDefinition::table`].
    pub table: String,
    converter: RowConverter,
    output_schema: SchemaRef,
}

/// A row the converter refused, with everything a dead-letter row needs.
#[derive(Debug)]
pub struct RejectedRow {
    /// Index of the row within the route's rows for this delivery.
    pub index: usize,
    /// The flat row as the unwrapper produced it (its `_raw` is inside).
    pub row: Value,
    /// The converter's error: column, row, and JSON kind — never the value.
    pub error: String,
}

/// What a route's rows became: the convertible ones as a batch, the rest
/// as rejections.
pub struct ConvertedRows {
    /// `None` when no row converted.
    pub batch: Option<RecordBatch>,
    /// Rows the converter refused, in delivery order.
    pub rejected: Vec<RejectedRow>,
}

impl CompiledRoute {
    fn compile(webhook: &str, route: &RouteDefinition) -> Result<Self> {
        let converter = RowConverter::from_columns(route.columns.clone()).map_err(|e| {
            anyhow!(
                "webhook '{webhook}': route '{}' has an invalid column mapping: {e}",
                route.field
            )
        })?;
        let mut fields: Vec<Field> = converter
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        fields.push(Field::new(INGESTED_AT_COLUMN, ingested_at_type(), false));
        Ok(Self {
            field: route.field.clone(),
            rows: route.rows,
            table: route.table.clone(),
            converter,
            output_schema: Arc::new(Schema::new(fields)),
        })
    }

    /// The batch schema this route writes: the declared columns followed by
    /// [`INGESTED_AT_COLUMN`].
    pub fn output_schema(&self) -> &SchemaRef {
        &self.output_schema
    }

    /// Column names in write order, for the preflight and for error messages.
    pub fn column_names(&self) -> Vec<&str> {
        self.output_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect()
    }

    /// Map flat rows to a batch and stamp every row with `ingested_at_ms`.
    ///
    /// Fails on the first row the converter refuses; the error names the
    /// column, the row index, and the JSON kind found, never the value.
    pub fn convert(&self, rows: &[Value], ingested_at_ms: i64) -> Result<RecordBatch> {
        let mapped = self
            .converter
            .convert(rows, 1)
            .map_err(|e| anyhow!("route '{}': {e}", self.field))?;
        self.stamp(mapped, ingested_at_ms)
    }

    /// Map flat rows to a batch, keeping the rows that convert and
    /// returning the ones that do not.
    ///
    /// The whole delivery is tried first — one converter call is the
    /// common case — and only a refusal falls back to row-by-row, so a
    /// single malformed row costs one extra pass rather than the delivery.
    pub fn convert_rows(&self, rows: &[Value], ingested_at_ms: i64) -> Result<ConvertedRows> {
        if rows.is_empty() {
            return Ok(ConvertedRows {
                batch: None,
                rejected: Vec::new(),
            });
        }
        if let Ok(batch) = self.converter.convert(rows, 1) {
            return Ok(ConvertedRows {
                batch: Some(self.stamp(batch, ingested_at_ms)?),
                rejected: Vec::new(),
            });
        }
        let mut good = Vec::with_capacity(rows.len());
        let mut rejected = Vec::new();
        for (index, row) in rows.iter().enumerate() {
            match self.converter.convert(std::slice::from_ref(row), 1) {
                Ok(batch) => good.push(batch),
                Err(e) => rejected.push(RejectedRow {
                    index,
                    row: row.clone(),
                    error: format!("route '{}': {e}", self.field),
                }),
            }
        }
        let batch = if good.is_empty() {
            None
        } else {
            let merged = concat_batches(&good[0].schema(), &good)
                .with_context(|| format!("route '{}': merging converted rows", self.field))?;
            Some(self.stamp(merged, ingested_at_ms)?)
        };
        Ok(ConvertedRows { batch, rejected })
    }

    fn stamp(&self, mapped: RecordBatch, ingested_at_ms: i64) -> Result<RecordBatch> {
        let ingested_at: ArrayRef = Arc::new(
            TimestampMillisecondArray::from(vec![ingested_at_ms; mapped.num_rows()])
                .with_timezone("UTC"),
        );
        let mut columns = mapped.columns().to_vec();
        columns.push(ingested_at);
        RecordBatch::try_new(Arc::clone(&self.output_schema), columns)
            .with_context(|| format!("route '{}': assembling the output batch", self.field))
    }
}

/// The webhook's quarantine table, compiled.
pub struct CompiledDeadLetter {
    /// Destination table for rejected rows.
    pub table: String,
    schema: SchemaRef,
}

impl CompiledDeadLetter {
    fn new(table: &str) -> Self {
        Self {
            table: table.to_string(),
            schema: Arc::new(Schema::new(vec![
                Field::new(DEAD_LETTER_COLUMNS[0], DataType::Utf8, false),
                Field::new(DEAD_LETTER_COLUMNS[1], DataType::Utf8, false),
                Field::new(DEAD_LETTER_COLUMNS[2], DataType::Utf8, false),
                Field::new(DEAD_LETTER_COLUMNS[3], ingested_at_type(), false),
            ])),
        }
    }

    /// The batch schema: `route`, `raw`, `error`, `ingested_at`.
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Rows rejected by `route`, as a dead-letter batch.
    pub fn batch(
        &self,
        route: &str,
        rejected: &[RejectedRow],
        ingested_at_ms: i64,
    ) -> Result<RecordBatch> {
        let routes: ArrayRef = Arc::new(StringArray::from(vec![route; rejected.len()]));
        let raws: ArrayRef = Arc::new(StringArray::from(
            rejected
                .iter()
                .map(|r| r.row.get("_raw").unwrap_or(&r.row).to_string())
                .collect::<Vec<_>>(),
        ));
        let errors: ArrayRef = Arc::new(StringArray::from(
            rejected
                .iter()
                .map(|r| r.error.as_str())
                .collect::<Vec<_>>(),
        ));
        let ingested_at: ArrayRef = Arc::new(
            TimestampMillisecondArray::from(vec![ingested_at_ms; rejected.len()])
                .with_timezone("UTC"),
        );
        RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![routes, raws, errors, ingested_at],
        )
        .context("assembling the dead-letter batch")
    }
}

/// A definition plus its compiled routes.
pub struct CompiledWebhook {
    /// The definition as loaded.
    pub definition: WebhookDefinition,
    /// Routes in declaration order; a delivery is applied to each in turn.
    pub routes: Vec<CompiledRoute>,
    /// Where rows the converter refuses go, when the definition names one.
    pub dead_letter: Option<CompiledDeadLetter>,
}

/// Every webhook the server serves, keyed by route name.
#[derive(Default)]
pub struct WebhookRegistry {
    webhooks: HashMap<String, Arc<CompiledWebhook>>,
}

impl std::fmt::Debug for WebhookRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookRegistry")
            .field("names", &self.names())
            .finish()
    }
}

impl WebhookRegistry {
    /// A registry with no webhooks: every `/webhooks/:name` is a 404.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Compile every definition and preflight every destination.
    ///
    /// Fails on the first webhook that cannot be served, naming the webhook,
    /// the route, and what is wrong — a server that starts with a broken
    /// receiver would acknowledge nothing and lose nothing, but it would
    /// hide the problem until someone reads the logs.
    ///
    /// # Example
    /// ```no_run
    /// use std::collections::HashMap;
    /// use std::sync::Arc;
    /// use datafusion::prelude::SessionContext;
    /// use skardi_server::webhooks::WebhookRegistry;
    ///
    /// # async fn demo() {
    /// let ctx = Arc::new(SessionContext::new());
    /// let registry = WebhookRegistry::build(&HashMap::new(), &[], &ctx).await.unwrap();
    /// assert!(registry.is_empty());
    /// # }
    /// ```
    pub async fn build(
        definitions: &HashMap<String, WebhookDefinition>,
        data_sources: &[DataSource],
        ctx: &Arc<SessionContext>,
    ) -> Result<Self> {
        let mut webhooks = HashMap::with_capacity(definitions.len());
        for (name, definition) in definitions {
            let mut routes = Vec::with_capacity(definition.routes.len());
            for route in &definition.routes {
                let compiled = CompiledRoute::compile(name, route)?;
                preflight_destination(
                    name,
                    &format!("route '{}'", compiled.field),
                    &compiled.table,
                    compiled.output_schema(),
                    data_sources,
                    ctx,
                )
                .await?;
                routes.push(compiled);
            }
            let dead_letter = match &definition.dead_letter {
                Some(table) => {
                    let compiled = CompiledDeadLetter::new(table);
                    preflight_destination(
                        name,
                        "dead_letter",
                        &compiled.table,
                        compiled.schema(),
                        data_sources,
                        ctx,
                    )
                    .await?;
                    Some(compiled)
                }
                None => None,
            };
            tracing::info!(
                "Webhook loaded: {} (provider {}) → {} route(s): {}{}",
                name,
                definition.provider.label(),
                routes.len(),
                routes
                    .iter()
                    .map(|r| format!("{}→{}", r.field, r.table))
                    .collect::<Vec<_>>()
                    .join(", "),
                dead_letter
                    .as_ref()
                    .map(|d| format!("; dead_letter→{}", d.table))
                    .unwrap_or_default()
            );
            webhooks.insert(
                name.clone(),
                Arc::new(CompiledWebhook {
                    definition: definition.clone(),
                    routes,
                    dead_letter,
                }),
            );
        }
        Ok(Self { webhooks })
    }

    /// Look a webhook up by route name.
    pub fn get(&self, name: &str) -> Option<Arc<CompiledWebhook>> {
        self.webhooks.get(name).map(Arc::clone)
    }

    /// Whether any webhook is served.
    pub fn is_empty(&self) -> bool {
        self.webhooks.is_empty()
    }

    /// Route names, sorted.
    pub fn names(&self) -> Vec<&str> {
        let mut names: Vec<&str> = self.webhooks.keys().map(String::as_str).collect();
        names.sort_unstable();
        names
    }
}

/// Refuse a destination the receiver could not write safely or correctly.
///
/// `what` labels the writer in errors (`route 'messages'`, `dead_letter`);
/// `produced` is the batch schema that writer emits.
async fn preflight_destination(
    webhook: &str,
    what: &str,
    table: &str,
    produced: &SchemaRef,
    data_sources: &[DataSource],
    ctx: &Arc<SessionContext>,
) -> Result<()> {
    let root = table
        .split('.')
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow!("webhook '{webhook}': {what} destination '{table}' is not a table identifier")
        })?;
    let source = data_sources
        .iter()
        .find(|ds| ds.name == root)
        .ok_or_else(|| {
            anyhow!(
                "webhook '{webhook}': {what} destination '{table}' does not name a configured \
                 data source ('{root}' is not in the context)"
            )
        })?;
    if !TRANSACTIONAL_SOURCE_TYPES.contains(&source.source_type) {
        return Err(anyhow!(
            "webhook '{webhook}': {what} destination '{table}' is a {} source; a webhook can \
             only append to a transactional SQL source (postgres, mysql, sqlite) so a failed \
             delivery leaves no partial rows behind for the retry",
            source.source_type.as_str()
        ));
    }
    if !source.access_mode.is_read_write() {
        return Err(anyhow!(
            "webhook '{webhook}': {what} destination '{table}' is on source '{root}', which is \
             not `access_mode: read_write`"
        ));
    }

    let destination = SqlDmlDestination::new(Arc::clone(ctx), table.to_string());
    let actual = destination
        .schema()
        .await
        .with_context(|| {
            format!("webhook '{webhook}': {what} could not inspect destination '{table}'")
        })?
        .ok_or_else(|| {
            anyhow!(
                "webhook '{webhook}': {what} destination '{table}' is not registered; create the \
                 table before starting the server (the catalog is read once, at startup)"
            )
        })?;

    let expected: Vec<&str> = produced
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let found: Vec<&str> = actual.fields().iter().map(|f| f.name().as_str()).collect();
    if expected != found {
        return Err(anyhow!(
            "webhook '{webhook}': {what} destination '{table}' has columns [{}] but the writer \
             produces [{}]; the insert matches by position, so the table must have exactly \
             these columns in this order",
            found.join(", "),
            expected.join(", ")
        ));
    }
    for (produced, target) in produced.fields().iter().zip(actual.fields().iter()) {
        if !write_compatible(produced.data_type(), target.data_type()) {
            return Err(anyhow!(
                "webhook '{webhook}': {what} destination '{table}' column '{}' is {} in the \
                 table but the writer produces {}; the insert would fail on every delivery and \
                 the provider would retry it for days, so the server refuses to start",
                produced.name(),
                target.data_type(),
                produced.data_type()
            ));
        }
        // SQLite registration carries each column's NOT NULL; a nullable
        // route column into such a column fails the insert on the first row
        // without that key — the same week-of-retries outcome. Postgres
        // registration reports every column nullable, so there this check
        // cannot fire and nullability stays the operator's responsibility.
        if produced.is_nullable() && !target.is_nullable() {
            return Err(anyhow!(
                "webhook '{webhook}': {what} destination '{table}' column '{}' is NOT NULL in \
                 the table but the writer declares it nullable; a row without that key would \
                 fail the whole delivery on every retry, so the server refuses to start — make \
                 the table column nullable or the route column `nullable: false`",
                produced.name()
            ));
        }
    }
    Ok(())
}

/// Whether a column the route produces can be written into a table column
/// of `to`'s type by the insert planner's cast.
///
/// Anything may be written into a text column (that is how a timestamp
/// lands in SQLite `TEXT`), but a text column may only be written into
/// text: the planner would accept `Utf8 → Int64` and then fail at run time
/// on the first non-numeric value, which for a `wamid.…` id is every value.
/// Every other pair defers to Arrow's own cast table.
fn write_compatible(from: &DataType, to: &DataType) -> bool {
    let is_text =
        |t: &DataType| matches!(t, DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View);
    if is_text(to) {
        return true;
    }
    if is_text(from) {
        return false;
    }
    can_cast_types(from, to)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::webhooks::definition::WebhookDefinition;
    use arrow::array::{Array, StringArray};
    use serde_json::json;

    const YAML: &str = r#"
kind: webhook
metadata: { name: t }
spec:
  provider: meta_whatsapp
  verify_token_env: V
  app_secret_env: S
  routes:
    - field: messages
      destination: { table: staging.main.events }
      columns:
        - { name: message_id, path: id, type: utf8, nullable: false }
        - { name: phone_number_id, path: _metadata.phone_number_id, type: utf8, nullable: false }
        - { name: text, path: text.body, type: utf8, nullable: true }
        - { name: received_at, path: timestamp, type: timestamp_s_string_utc, nullable: false }
        - { name: raw, path: _raw, type: json, nullable: false }
"#;

    fn definition() -> WebhookDefinition {
        WebhookDefinition::from_yaml_with_env(YAML, |_| Some("x".into()))
            .unwrap()
            .unwrap()
    }

    fn row(id: &str, ts: &str) -> Value {
        json!({"id": id, "_metadata": {"phone_number_id": "p"}, "text": {"body": "hi"},
               "timestamp": ts, "_raw": {"id": id}})
    }

    #[test]
    fn a_compiled_route_appends_ingested_at_after_the_declared_columns() {
        let def = definition();
        let route = CompiledRoute::compile("t", &def.routes[0]).unwrap();
        assert_eq!(
            route.column_names(),
            [
                "message_id",
                "phone_number_id",
                "text",
                "received_at",
                "raw",
                "ingested_at"
            ]
        );
        let rows = vec![
            row("a", "1749416383"),
            json!({"id": "b", "_metadata": {"phone_number_id": "p"},
                   "timestamp": "1749416384", "_raw": {"id": "b"}}),
        ];
        let batch = route.convert(&rows, 1_760_000_000_000).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 6);
        let text = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(text.value(0), "hi");
        assert!(text.is_null(1));
        let stamped = batch
            .column(5)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap();
        assert_eq!(stamped.value(0), 1_760_000_000_000);
        assert_eq!(stamped.value(1), 1_760_000_000_000);
        assert_eq!(stamped.timezone(), Some("UTC"));
    }

    #[test]
    fn a_conversion_failure_names_the_column_and_row_but_not_the_value() {
        let def = definition();
        let route = CompiledRoute::compile("t", &def.routes[0]).unwrap();
        let rows = vec![json!({"id": "a", "_metadata": {"phone_number_id": "p"},
                               "timestamp": "not-a-number-SECRET", "_raw": {}})];
        let err = route.convert(&rows, 0).unwrap_err().to_string();
        assert!(err.contains("received_at"), "{err}");
        assert!(err.contains("row 0"), "{err}");
        assert!(!err.contains("SECRET"), "{err}");
    }

    #[test]
    fn convert_rows_keeps_the_good_rows_and_returns_the_rejected_ones() {
        let def = definition();
        let route = CompiledRoute::compile("t", &def.routes[0]).unwrap();
        let rows = vec![
            row("a", "1749416383"),
            row("b", "not-digits"),
            row("c", "1749416385"),
        ];
        let converted = route.convert_rows(&rows, 7).unwrap();
        let batch = converted.batch.expect("two rows convert");
        assert_eq!(batch.num_rows(), 2);
        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.value(0), "a");
        assert_eq!(ids.value(1), "c");
        assert_eq!(converted.rejected.len(), 1);
        assert_eq!(converted.rejected[0].index, 1);
        assert_eq!(converted.rejected[0].row["id"], "b");
        assert!(converted.rejected[0].error.contains("received_at"));

        // A fully convertible delivery takes the one-call path.
        let converted = route.convert_rows(&rows[..1], 7).unwrap();
        assert_eq!(converted.batch.unwrap().num_rows(), 1);
        assert!(converted.rejected.is_empty());
        // And one with no convertible row yields no batch, every row rejected.
        let converted = route.convert_rows(&rows[1..2], 7).unwrap();
        assert!(converted.batch.is_none());
        assert_eq!(converted.rejected.len(), 1);
    }

    #[test]
    fn a_dead_letter_batch_carries_route_raw_error_and_arrival() {
        let dead = CompiledDeadLetter::new("staging.main.dead");
        let rejected = vec![RejectedRow {
            index: 3,
            row: json!({"id": "x", "_raw": {"id": "x", "type": "poll"}}),
            error: "route 'messages': boom".into(),
        }];
        let batch = dead.batch("messages", &rejected, 42).unwrap();
        assert_eq!(
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            DEAD_LETTER_COLUMNS
        );
        let raw = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            raw.value(0),
            r#"{"id":"x","type":"poll"}"#,
            "the original object, not the flat row"
        );
        let error = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(error.value(0), "route 'messages': boom");
    }

    #[test]
    fn an_empty_delivery_converts_to_an_empty_batch_with_the_full_schema() {
        let def = definition();
        let route = CompiledRoute::compile("t", &def.routes[0]).unwrap();
        let batch = route.convert(&[], 0).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema().fields().len(), 6);
    }

    #[test]
    fn write_compatibility_allows_text_targets_and_refuses_text_sources_elsewhere() {
        let ts = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        assert!(write_compatible(&DataType::Utf8, &DataType::Utf8));
        assert!(write_compatible(&ts, &DataType::Utf8));
        assert!(write_compatible(&DataType::Boolean, &DataType::Utf8));
        assert!(write_compatible(&ts, &DataType::Int64));
        assert!(write_compatible(&DataType::Boolean, &DataType::Int64));
        assert!(write_compatible(&DataType::Int64, &DataType::Float64));
        assert!(write_compatible(
            &ts,
            &DataType::Timestamp(TimeUnit::Microsecond, Some("+00:00".into()))
        ));
        assert!(!write_compatible(&DataType::Utf8, &DataType::Int64));
        assert!(!write_compatible(&DataType::Utf8, &DataType::Boolean));
        assert!(!write_compatible(&DataType::Utf8, &ts));
    }

    fn source(name: &str, source_type: DataSourceType, read_write: bool) -> DataSource {
        DataSource {
            name: name.to_string(),
            source_type,
            path: Default::default(),
            connection_string: None,
            schema: None,
            options: None,
            hierarchy_level: Default::default(),
            access_mode: if read_write {
                skardi::sources::AccessMode::ReadWrite
            } else {
                skardi::sources::AccessMode::ReadOnly
            },
            enable_cache: false,
            description: None,
            open_connector: None,
            rss: None,
            graph: None,
        }
    }

    #[tokio::test]
    async fn preflight_refuses_a_non_transactional_source_and_a_malformed_table() {
        let ctx = Arc::new(SessionContext::new());
        let mut defs = HashMap::new();
        defs.insert("t".to_string(), definition());

        let lake = [source("staging", DataSourceType::Lance, true)];
        let err = WebhookRegistry::build(&defs, &lake, &ctx)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("transactional SQL source"),
            "{err}"
        );

        let yaml = YAML.replace("staging.main.events", ".main.events");
        let def = WebhookDefinition::from_yaml_with_env(&yaml, |_| Some("x".into()))
            .unwrap()
            .unwrap();
        defs.insert("t".to_string(), def);
        let sqlite = [source("staging", DataSourceType::Sqlite, true)];
        let err = WebhookRegistry::build(&defs, &sqlite, &ctx)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("not a table identifier"), "{err}");
    }

    #[tokio::test]
    async fn an_empty_registry_has_no_names_and_a_redacted_debug() {
        let ctx = Arc::new(SessionContext::new());
        let registry = WebhookRegistry::build(&HashMap::new(), &[], &ctx)
            .await
            .unwrap();
        assert!(registry.is_empty());
        assert!(registry.names().is_empty());
        assert!(registry.get("nope").is_none());
        assert_eq!(format!("{registry:?}"), "WebhookRegistry { names: [] }");
    }

    #[tokio::test]
    async fn preflight_refuses_an_unknown_source_before_touching_the_session() {
        let def = definition();
        let ctx = Arc::new(SessionContext::new());
        let mut defs = HashMap::new();
        defs.insert("t".to_string(), def);
        let err = WebhookRegistry::build(&defs, &[], &ctx).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("does not name a configured data source"),
            "{err}"
        );
    }
}
