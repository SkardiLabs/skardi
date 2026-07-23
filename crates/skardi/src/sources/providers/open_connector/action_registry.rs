//! Action-schema registry for an Open Connector gateway.
//!
//! At gateway registration, Skardi discovers the metadata of every action the
//! configuration references (the raw-action allowlist; source-pack actions
//! join them when the pack registry lands). The registry keeps the results in
//! memory so that **query planning never performs network I/O**.
//!
//! Each entry also records a *compatibility fingerprint*: a stable hash of
//! the action's output schema. Later milestones compare the fingerprint a
//! source pack was built against with the live one, so an incompatible
//! upstream action change fails registration with a targeted error instead of
//! silently changing a table's behavior.

use std::collections::BTreeMap;

use futures::stream::{self, StreamExt, TryStreamExt};
use serde_json::Value;

use super::client::{DiscoveredAction, OpenConnectorClient};
use super::error::OpenConnectorError;
use crate::util::json::blake3_hex;
use crate::util::json::canonical_json;

/// Maximum concurrent discovery calls while loading the registry.
const DISCOVERY_CONCURRENCY: usize = 8;

/// Metadata for one Open Connector action, as discovered from the gateway.
#[derive(Debug, Clone)]
pub struct ActionMetadata {
    action_id: String,
    input_schema: Option<Value>,
    output_schema: Option<Value>,
    read_only: Option<bool>,
    connection_aliases: Vec<String>,
    fingerprint: String,
}

impl ActionMetadata {
    /// Build one entry from a discovered action, computing the output-schema
    /// compatibility fingerprint.
    fn from_discovered(action_id: &str, discovered: DiscoveredAction) -> Self {
        let fingerprint = fingerprint_schema(discovered.output_schema.as_ref());
        Self {
            action_id: action_id.to_string(),
            input_schema: discovered.input_schema,
            output_schema: discovered.output_schema,
            read_only: discovered.read_only,
            connection_aliases: discovered.connection_aliases,
            fingerprint,
        }
    }

    /// The Open Connector action ID, e.g. `github.list_repository_issues`.
    pub fn action_id(&self) -> &str {
        &self.action_id
    }

    /// Declared input JSON Schema, if the gateway provides one.
    pub fn input_schema(&self) -> Option<&Value> {
        self.input_schema.as_ref()
    }

    /// Declared output JSON Schema, if the gateway provides one.
    pub fn output_schema(&self) -> Option<&Value> {
        self.output_schema.as_ref()
    }

    /// Whether the gateway classifies this action as a non-mutating read.
    /// `None` means the gateway did not say — default-deny consumers (the
    /// raw-action UDTF) must refuse to execute the action in that case.
    pub fn read_only(&self) -> Option<bool> {
        self.read_only
    }

    /// Connection aliases available for this action.
    pub fn connection_aliases(&self) -> &[String] {
        &self.connection_aliases
    }

    /// Stable hash of the output schema, used for compatibility checks.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }
}

/// In-memory registry of discovered action metadata for one gateway.
///
/// `BTreeMap` keeps iteration order deterministic (sorted by action ID) so
/// logs and downstream behavior don't depend on discovery completion order.
#[derive(Debug, Default)]
pub struct ActionRegistry {
    actions: BTreeMap<String, ActionMetadata>,
}

impl ActionRegistry {
    /// Discover every action in `action_ids` (deduplicated, sorted) from the
    /// gateway and build the registry.
    ///
    /// Discovery is the only network step; it fails fast on
    /// [`OpenConnectorError::ActionNotFound`],
    /// [`OpenConnectorError::ActionNotLocallyExecutable`], or any client
    /// error — a partially loaded registry is never returned.
    ///
    /// # Example
    /// ```no_run
    /// use skardi::sources::providers::open_connector::{
    ///     ActionRegistry, OpenConnectorClient, OpenConnectorConfig,
    /// };
    ///
    /// # async fn example() -> Result<(), skardi::sources::providers::open_connector::OpenConnectorError> {
    /// let config: OpenConnectorConfig =
    ///     serde_yaml::from_str("runtime_token_env: OPEN_CONNECTOR_TOKEN").unwrap();
    /// let client = OpenConnectorClient::from_config("http://open-connector:3000", &config)?;
    /// let registry =
    ///     ActionRegistry::load(&client, &["github.list_repository_issues".to_string()]).await?;
    /// assert!(registry.get("github.list_repository_issues").is_some());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn load(
        client: &OpenConnectorClient,
        action_ids: &[String],
    ) -> Result<Self, OpenConnectorError> {
        let mut ids: Vec<&str> = action_ids.iter().map(String::as_str).collect();
        ids.sort_unstable();
        ids.dedup();

        let discovered = stream::iter(ids.into_iter().map(|action_id| async move {
            let action = client.discover_action(action_id).await?;
            // Default-deny: only an explicit `true` admits an action into the
            // registry. `false` and "field missing" fail differently so the
            // operator can tell a refused action from a metadata gap.
            match action.locally_executable {
                Some(true) => {}
                Some(false) => {
                    return Err(OpenConnectorError::ActionNotLocallyExecutable {
                        action_id: action_id.to_string(),
                    });
                }
                None => {
                    return Err(OpenConnectorError::ActionExecutabilityUnknown {
                        action_id: action_id.to_string(),
                    });
                }
            }
            Ok(ActionMetadata::from_discovered(action_id, action))
        }))
        .buffer_unordered(DISCOVERY_CONCURRENCY)
        .try_collect::<Vec<_>>()
        .await?;

        let actions = discovered
            .into_iter()
            .map(|meta| (meta.action_id.clone(), meta))
            .collect();
        Ok(Self { actions })
    }

    /// Look up one action's metadata.
    pub fn get(&self, action_id: &str) -> Option<&ActionMetadata> {
        self.actions.get(action_id)
    }

    /// Number of actions in the registry.
    pub fn len(&self) -> usize {
        self.actions.len()
    }

    /// Whether the registry is empty (no actions were requested).
    pub fn is_empty(&self) -> bool {
        self.actions.is_empty()
    }
}

/// Compute the compatibility fingerprint of an output schema.
///
/// The schema is canonicalized first (object keys sorted recursively, so two
/// semantically identical schemas with different key orders fingerprint
/// equally), then hashed with BLAKE3 and hex-encoded.
fn fingerprint_schema(output_schema: Option<&Value>) -> String {
    let canonical = match output_schema {
        Some(schema) => canonical_json(schema),
        None => "null".to_string(),
    };
    blake3_hex(canonical.as_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::open_connector::client::OpenConnectorClient;
    use crate::sources::providers::open_connector::testutil::{MockGateway, MockResponse};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn action_response(output_schema: &str) -> String {
        format!(
            r#"{{"input_schema": {{}}, "output_schema": {output_schema}, "locally_executable": true, "connection_aliases": ["work"]}}"#
        )
    }

    fn client(gateway: &MockGateway) -> OpenConnectorClient {
        OpenConnectorClient::new(&gateway.url, "test-token", Duration::from_secs(2))
            .expect("build client")
    }

    #[tokio::test]
    async fn load_discovers_dedupes_and_registers_all() {
        let hits = Arc::new(AtomicUsize::new(0));
        let hits2 = Arc::clone(&hits);
        let gateway = MockGateway::start(move |_req| {
            hits2.fetch_add(1, Ordering::SeqCst);
            let schema = r#"{"type": "object"}"#;
            MockResponse::ok(&action_response(schema))
        })
        .await;

        let ids = vec![
            "github.b".to_string(),
            "github.a".to_string(),
            "github.a".to_string(), // duplicate must only be fetched once
        ];
        let registry = ActionRegistry::load(&client(&gateway), &ids)
            .await
            .expect("load");

        assert_eq!(registry.len(), 2);
        assert!(!registry.is_empty());
        assert!(registry.get("github.a").is_some());
        assert!(registry.get("github.b").is_some());
        assert_eq!(
            hits.load(Ordering::SeqCst),
            2,
            "duplicates are deduplicated"
        );
    }

    #[tokio::test]
    async fn load_empty_allowlist_yields_empty_registry() {
        let gateway = MockGateway::start(|_| MockResponse::new(500, "{}")).await;
        let registry = ActionRegistry::load(&client(&gateway), &[])
            .await
            .expect("load empty");
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
        assert!(gateway.requests().is_empty(), "no discovery calls at all");
    }

    #[tokio::test]
    async fn load_rejects_non_executable_action() {
        let gateway = MockGateway::start(|_| {
            MockResponse::ok(
                r#"{"input_schema": {}, "output_schema": {}, "locally_executable": false}"#,
            )
        })
        .await;

        let err = ActionRegistry::load(&client(&gateway), &["github.x".to_string()])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::ActionNotLocallyExecutable { ref action_id }
                if action_id == "github.x"
        ));
    }

    #[tokio::test]
    async fn load_rejects_missing_executability_flag() {
        // Default-deny: a gateway that omits `locally_executable` must not be
        // read as "executable". The error is a distinct variant so operators
        // can tell a metadata gap from an explicit refusal.
        let gateway = MockGateway::start(|_| {
            MockResponse::ok(r#"{"input_schema": {}, "output_schema": {}}"#)
        })
        .await;

        let err = ActionRegistry::load(&client(&gateway), &["github.x".to_string()])
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            OpenConnectorError::ActionExecutabilityUnknown { ref action_id }
                if action_id == "github.x"
        ));
    }

    #[tokio::test]
    async fn load_propagates_discovery_errors() {
        let gateway = MockGateway::start(|_| MockResponse::new(404, "{}")).await;
        let err = ActionRegistry::load(&client(&gateway), &["github.missing".to_string()])
            .await
            .unwrap_err();
        assert!(matches!(err, OpenConnectorError::ActionNotFound { .. }));
    }

    #[tokio::test]
    async fn metadata_exposes_discovered_fields() {
        let gateway =
            MockGateway::start(|_| MockResponse::ok(&action_response(r#"{"type": "array"}"#)))
                .await;
        let registry = ActionRegistry::load(&client(&gateway), &["github.x".to_string()])
            .await
            .expect("load");
        let meta = registry.get("github.x").expect("present");
        assert_eq!(meta.action_id(), "github.x");
        assert_eq!(
            meta.output_schema(),
            Some(&serde_json::json!({"type": "array"}))
        );
        assert_eq!(meta.connection_aliases(), &["work".to_string()]);
        assert_eq!(meta.fingerprint().len(), 64, "BLAKE3 hash as hex");
        assert_eq!(
            meta.read_only(),
            None,
            "an absent read_only flag must stay absent (default-deny input)"
        );
    }

    #[tokio::test]
    async fn metadata_carries_explicit_read_only_classification() {
        let gateway = MockGateway::start(|_| {
            MockResponse::ok(
                r#"{"input_schema": {}, "output_schema": {}, "locally_executable": true,
                    "read_only": true, "connection_aliases": []}"#,
            )
        })
        .await;
        let registry = ActionRegistry::load(&client(&gateway), &["github.x".to_string()])
            .await
            .expect("load");
        assert_eq!(registry.get("github.x").unwrap().read_only(), Some(true));
    }

    #[test]
    fn fingerprint_is_stable_across_key_order() {
        let a = serde_json::json!({
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "title": {"type": "string"}
            }
        });
        let b = serde_json::json!({
            "properties": {
                "title": {"type": "string"},
                "id": {"type": "integer"}
            },
            "type": "object"
        });
        assert_eq!(fingerprint_schema(Some(&a)), fingerprint_schema(Some(&b)));
    }

    #[test]
    fn fingerprint_changes_with_schema() {
        let a = serde_json::json!({"type": "object"});
        let b = serde_json::json!({"type": "array"});
        assert_ne!(fingerprint_schema(Some(&a)), fingerprint_schema(Some(&b)));
    }

    #[test]
    fn fingerprint_distinguishes_missing_schema() {
        let a = serde_json::json!({"type": "object"});
        assert_ne!(fingerprint_schema(None), fingerprint_schema(Some(&a)));
        assert_eq!(fingerprint_schema(None), fingerprint_schema(None));
    }
}
