//! Webhook definitions compiled against the live session.
//!
//! Compilation does the work that must not wait for the first delivery:
//! building each route's column converter, and checking that each
//! destination table exists, belongs to a transactional read-write source,
//! and has exactly the columns the route will write, in order. The SQL
//! writer matches columns **by position** (`INSERT INTO t SELECT * FROM
//! staging`), so a table whose columns are shuffled or one short would not
//! fail loudly at write time — it would land values in the wrong columns, or
//! fail every delivery with a planning error that Meta then retries for a
//! week. Refusing to start is the cheaper failure.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow::array::{ArrayRef, TimestampMillisecondArray};
use arrow::compute::can_cast_types;
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
        fields.push(Field::new(
            INGESTED_AT_COLUMN,
            DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into())),
            false,
        ));
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
    /// A conversion failure names the column, the row index, and the JSON
    /// kind found, never the value — the converter's own wording.
    pub fn convert(&self, rows: &[Value], ingested_at_ms: i64) -> Result<RecordBatch> {
        let mapped = self
            .converter
            .convert(rows, 1)
            .map_err(|e| anyhow!("route '{}': {e}", self.field))?;
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

/// A definition plus its compiled routes.
pub struct CompiledWebhook {
    /// The definition as loaded.
    pub definition: WebhookDefinition,
    /// Routes in declaration order; a delivery is applied to each in turn.
    pub routes: Vec<CompiledRoute>,
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
                preflight_destination(name, &compiled, data_sources, ctx).await?;
                routes.push(compiled);
            }
            tracing::info!(
                "Webhook loaded: {} (provider {}) → {} route(s): {}",
                name,
                definition.provider.label(),
                routes.len(),
                routes
                    .iter()
                    .map(|r| format!("{}→{}", r.field, r.table))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            webhooks.insert(
                name.clone(),
                Arc::new(CompiledWebhook {
                    definition: definition.clone(),
                    routes,
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
async fn preflight_destination(
    webhook: &str,
    route: &CompiledRoute,
    data_sources: &[DataSource],
    ctx: &Arc<SessionContext>,
) -> Result<()> {
    let root = route
        .table
        .split('.')
        .next()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "webhook '{webhook}': route '{}' destination '{}' is not a table identifier",
                route.field,
                route.table
            )
        })?;
    let source = data_sources
        .iter()
        .find(|ds| ds.name == root)
        .ok_or_else(|| {
            anyhow!(
                "webhook '{webhook}': route '{}' destination '{}' does not name a configured data \
             source ('{root}' is not in the context)",
                route.field,
                route.table
            )
        })?;
    if !TRANSACTIONAL_SOURCE_TYPES.contains(&source.source_type) {
        return Err(anyhow!(
            "webhook '{webhook}': route '{}' destination '{}' is a {} source; a webhook can \
             only append to a transactional SQL source (postgres, mysql, sqlite) so a failed \
             delivery leaves no partial rows behind for the retry",
            route.field,
            route.table,
            source.source_type.as_str()
        ));
    }
    if !source.access_mode.is_read_write() {
        return Err(anyhow!(
            "webhook '{webhook}': route '{}' destination '{}' is on source '{root}', which is \
             not `access_mode: read_write`",
            route.field,
            route.table
        ));
    }

    let destination = SqlDmlDestination::new(Arc::clone(ctx), route.table.clone());
    let actual = destination
        .schema()
        .await
        .with_context(|| {
            format!(
                "webhook '{webhook}': route '{}' could not inspect destination '{}'",
                route.field, route.table
            )
        })?
        .ok_or_else(|| {
            anyhow!(
                "webhook '{webhook}': route '{}' destination '{}' is not registered; create the \
                 table before starting the server (the catalog is read once, at startup)",
                route.field,
                route.table
            )
        })?;

    let expected = route.column_names();
    let found: Vec<&str> = actual.fields().iter().map(|f| f.name().as_str()).collect();
    if expected != found {
        return Err(anyhow!(
            "webhook '{webhook}': route '{}' destination '{}' has columns [{}] but the route \
             writes [{}] (declared columns followed by '{INGESTED_AT_COLUMN}'); the insert \
             matches by position, so the table must have exactly these columns in this order",
            route.field,
            route.table,
            found.join(", "),
            expected.join(", ")
        ));
    }
    for (produced, target) in route
        .output_schema()
        .fields()
        .iter()
        .zip(actual.fields().iter())
    {
        if !write_compatible(produced.data_type(), target.data_type()) {
            return Err(anyhow!(
                "webhook '{webhook}': route '{}' destination '{}' column '{}' is {} in the \
                 table but the route writes {}; the insert would fail on every delivery and \
                 the provider would retry it for days, so the server refuses to start",
                route.field,
                route.table,
                produced.name(),
                target.data_type(),
                produced.data_type()
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
            json!({"id": "a", "_metadata": {"phone_number_id": "p"}, "text": {"body": "hi"},
                   "timestamp": "1749416383", "_raw": {"id": "a"}}),
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
