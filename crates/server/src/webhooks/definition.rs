//! The `kind: webhook` resource: parsing, validation, and secret resolution.
//!
//! A definition is pure data about *what* to accept and *where* to put it.
//! Compiling it against the live session (column converters, destination
//! preflight) happens in [`super::registry`], so a definition can be loaded
//! and inspected without a database.

use std::collections::HashSet;
use std::fmt;
use std::path::Path;

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
use skardi::sources::providers::open_connector::json_to_arrow::{ColumnDoc, ColumnSpec};

use super::INGESTED_AT_COLUMN;

/// The `kind:` value that marks a webhook resource file.
pub const WEBHOOK_KIND: &str = "webhook";

/// Default request-body cap. Meta documents a 3 MB maximum per delivery;
/// one megabyte of headroom keeps a payload at the documented limit from
/// failing on framing.
pub const DEFAULT_MAX_BODY_BYTES: usize = 4 * 1024 * 1024;

/// Smallest body cap a definition may set; below this a GET handshake's own
/// echo could not fit, let alone a delivery.
const MIN_MAX_BODY_BYTES: usize = 1024;

/// A secret resolved from the environment at load time.
///
/// Its `Debug` output is redacted so a `ServerConfig` dump, a panic message,
/// or a log line never carries the value.
#[derive(Clone)]
pub struct Secret(String);

impl Secret {
    /// The secret's bytes, for the verifier that needs them.
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(<redacted>)")
    }
}

/// Which provider's verification and envelope the route speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookProvider {
    /// WhatsApp Cloud API: `hub.challenge` handshake on GET,
    /// `X-Hub-Signature-256` (HMAC-SHA256 over the raw body with the app
    /// secret) on POST, `entry[].changes[].value` envelope.
    MetaWhatsapp,
}

impl WebhookProvider {
    /// Stable label for logs and metrics.
    pub fn label(self) -> &'static str {
        match self {
            WebhookProvider::MetaWhatsapp => "meta_whatsapp",
        }
    }
}

/// How a route picks rows out of a change's `value`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowSelector {
    /// One row per element of `value.<field>[]` — the shape of `messages`
    /// and `statuses`. A change is matched by carrying that array, not by
    /// its own `field` label: Meta files both arrays under `field:
    /// "messages"`, so one change can feed a `messages` route and a
    /// `statuses` route.
    #[default]
    List,
    /// The change's `value` object is the single row — the shape of every
    /// management field (`message_template_status_update`,
    /// `phone_number_quality_update`, …). A change is matched by its `field`
    /// label.
    Value,
}

/// One route: which rows to take and where to append them.
#[derive(Debug, Clone)]
pub struct RouteDefinition {
    /// For `rows: list`, the array key inside `value`; for `rows: value`,
    /// the change's `field` label.
    pub field: String,
    /// Row selection rule.
    pub rows: RowSelector,
    /// Destination table, as a DataFusion identifier. The first dotted
    /// segment names the data source.
    pub table: String,
    /// Declared columns, in destination order. The receiver appends
    /// [`INGESTED_AT_COLUMN`] after these.
    pub columns: Vec<ColumnSpec>,
}

/// A loaded, validated `kind: webhook` resource.
#[derive(Debug, Clone)]
pub struct WebhookDefinition {
    /// Route name: `/webhooks/<name>`.
    pub name: String,
    /// Free-form version string from `metadata.version`.
    pub version: Option<String>,
    /// Free-form description from `metadata.description`.
    pub description: Option<String>,
    /// Verification preset.
    pub provider: WebhookProvider,
    /// Token echoed back during the subscription handshake.
    pub verify_token: Secret,
    /// HMAC key for delivery signatures.
    pub app_secret: Secret,
    /// Maximum accepted request body, in bytes.
    pub max_body_bytes: usize,
    /// Routes, applied in order to every delivery.
    pub routes: Vec<RouteDefinition>,
    /// Table that receives rows the converter rejects, with the route, the
    /// original object, the error, and the arrival time. Without one, a
    /// rejected row fails the whole delivery with a 500.
    pub dead_letter: Option<String>,
}

// ---------------------------------------------------------------------------
// YAML shape
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WebhookDoc {
    /// Checked before deserialization (see `from_yaml_with_env`); kept so
    /// `deny_unknown_fields` accepts the discriminator.
    #[serde(rename = "kind")]
    _kind: String,
    metadata: MetadataDoc,
    spec: SpecDoc,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataDoc {
    name: String,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    description: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SpecDoc {
    provider: WebhookProvider,
    verify_token_env: String,
    app_secret_env: String,
    #[serde(default = "default_max_body_bytes")]
    max_body_bytes: usize,
    routes: Vec<RouteDoc>,
    #[serde(default)]
    dead_letter: Option<DeadLetterDoc>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DeadLetterDoc {
    table: String,
}

fn default_max_body_bytes() -> usize {
    DEFAULT_MAX_BODY_BYTES
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteDoc {
    field: String,
    #[serde(default)]
    rows: RowSelector,
    destination: DestinationDoc,
    columns: Vec<ColumnDoc>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DestinationDoc {
    table: String,
    /// Only `append` exists. The variant is an enum rather than a validated
    /// string so `mode: upsert` fails at parse time naming the one accepted
    /// value; dedup belongs to the merge job, not the receiver.
    #[serde(default)]
    #[allow(dead_code)]
    mode: WriteMode,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum WriteMode {
    #[default]
    Append,
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

impl WebhookDefinition {
    /// Load one file. Returns `Ok(None)` when the file's root `kind:` is not
    /// `webhook`, so a directory can be scanned the way `--jobs` and
    /// `--semantics` scan theirs.
    ///
    /// # Example
    /// ```no_run
    /// use skardi_server::webhooks::WebhookDefinition;
    ///
    /// let loaded = WebhookDefinition::load_from_file("webhooks/whatsapp-inbound.yaml").unwrap();
    /// if let Some(webhook) = loaded {
    ///     println!("{} has {} route(s)", webhook.name, webhook.routes.len());
    /// }
    /// ```
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Option<Self>> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("Failed to read webhook file {}", path.display()))?;
        Self::from_yaml_with_env(&text, |name| std::env::var(name).ok())
            .with_context(|| format!("Failed to load webhook from {}", path.display()))
    }

    /// Parse YAML text, resolving secrets through `env` (a lookup by variable
    /// name). Returns `Ok(None)` when `kind:` is not `webhook`.
    ///
    /// Secrets are resolved here, at load time, so a missing variable fails
    /// startup rather than the first delivery.
    ///
    /// # Example
    /// ```
    /// use skardi_server::webhooks::WebhookDefinition;
    ///
    /// let yaml = r#"
    /// kind: webhook
    /// metadata: { name: demo }
    /// spec:
    ///   provider: meta_whatsapp
    ///   verify_token_env: DEMO_VERIFY
    ///   app_secret_env: DEMO_SECRET
    ///   routes:
    ///     - field: messages
    ///       destination: { table: staging.main.events }
    ///       columns:
    ///         - { name: message_id, path: id, type: utf8, nullable: false }
    /// "#;
    /// let env = |name: &str| Some(format!("value-of-{name}"));
    /// let webhook = WebhookDefinition::from_yaml_with_env(yaml, env).unwrap().unwrap();
    /// assert_eq!(webhook.name, "demo");
    /// assert_eq!(webhook.routes[0].table, "staging.main.events");
    /// ```
    pub fn from_yaml_with_env<F>(yaml: &str, env: F) -> Result<Option<Self>>
    where
        F: Fn(&str) -> Option<String>,
    {
        let root: serde_yaml::Value =
            serde_yaml::from_str(yaml).context("webhook YAML is not valid YAML")?;
        match root.get("kind").and_then(serde_yaml::Value::as_str) {
            Some(kind) if kind == WEBHOOK_KIND => {}
            _ => return Ok(None),
        }
        let doc: WebhookDoc =
            serde_yaml::from_value(root).context("webhook YAML does not match the schema")?;
        Self::from_doc(doc, env).map(Some)
    }

    fn from_doc<F>(doc: WebhookDoc, env: F) -> Result<Self>
    where
        F: Fn(&str) -> Option<String>,
    {
        let name = doc.metadata.name;
        validate_name(&name)?;
        let spec = doc.spec;
        if spec.routes.is_empty() {
            return Err(anyhow!(
                "webhook '{name}': spec.routes must list at least one route"
            ));
        }
        if spec.max_body_bytes < MIN_MAX_BODY_BYTES {
            return Err(anyhow!(
                "webhook '{name}': max_body_bytes must be at least {MIN_MAX_BODY_BYTES}"
            ));
        }
        let verify_token = resolve_secret(&name, "verify_token_env", &spec.verify_token_env, &env)?;
        let app_secret = resolve_secret(&name, "app_secret_env", &spec.app_secret_env, &env)?;

        let mut routes = Vec::with_capacity(spec.routes.len());
        for route in spec.routes {
            routes.push(convert_route(&name, route)?);
        }
        let dead_letter = match spec.dead_letter {
            Some(DeadLetterDoc { table }) if table.trim().is_empty() => {
                return Err(anyhow!("webhook '{name}': dead_letter.table is empty"));
            }
            Some(DeadLetterDoc { table }) => Some(table),
            None => None,
        };

        Ok(Self {
            name,
            version: doc.metadata.version,
            description: doc.metadata.description,
            provider: spec.provider,
            verify_token,
            app_secret,
            max_body_bytes: spec.max_body_bytes,
            routes,
            dead_letter,
        })
    }
}

fn validate_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    let ok = match chars.next() {
        Some(first) if first.is_ascii_alphanumeric() => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        }
        _ => false,
    };
    if !ok {
        return Err(anyhow!(
            "webhook name '{name}' is invalid: it becomes a URL path segment, so it must start \
             with a letter or digit and contain only letters, digits, '-' and '_'"
        ));
    }
    Ok(())
}

fn resolve_secret<F>(webhook: &str, key: &str, var: &str, env: &F) -> Result<Secret>
where
    F: Fn(&str) -> Option<String>,
{
    if var.trim().is_empty() {
        return Err(anyhow!(
            "webhook '{webhook}': {key} names an empty environment variable"
        ));
    }
    match env(var) {
        Some(value) if !value.is_empty() => Ok(Secret(value)),
        _ => Err(anyhow!(
            "webhook '{webhook}': environment variable {var} ({key}) is not set or is empty"
        )),
    }
}

fn convert_route(webhook: &str, route: RouteDoc) -> Result<RouteDefinition> {
    let label = format!("{webhook}/{}", route.field);
    if route.field.trim().is_empty() {
        return Err(anyhow!("webhook '{webhook}': a route has an empty field"));
    }
    if route.destination.table.trim().is_empty() {
        return Err(anyhow!(
            "webhook '{webhook}': route '{}' has an empty destination table",
            route.field
        ));
    }
    if route.columns.is_empty() {
        return Err(anyhow!(
            "webhook '{webhook}': route '{}' declares no columns",
            route.field
        ));
    }
    let mut seen = HashSet::new();
    let mut columns = Vec::with_capacity(route.columns.len());
    for column in route.columns {
        let spec = column
            .into_spec(&label)
            .map_err(|e| anyhow!("webhook '{webhook}': {e}"))?;
        if spec.name == INGESTED_AT_COLUMN {
            return Err(anyhow!(
                "webhook '{webhook}': route '{}' declares a column named '{INGESTED_AT_COLUMN}', \
                 which the receiver appends itself",
                route.field
            ));
        }
        if !seen.insert(spec.name.clone()) {
            return Err(anyhow!(
                "webhook '{webhook}': route '{}' declares column '{}' twice",
                route.field,
                spec.name
            ));
        }
        columns.push(spec);
    }
    Ok(RouteDefinition {
        field: route.field,
        rows: route.rows,
        table: route.destination.table,
        columns,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const MINIMAL: &str = r#"
kind: webhook
metadata: { name: wa-inbound, version: "1.0.0" }
spec:
  provider: meta_whatsapp
  verify_token_env: WA_VERIFY
  app_secret_env: WA_SECRET
  routes:
    - field: messages
      destination: { table: staging.main.wa_events_messages, mode: append }
      columns:
        - { name: message_id, path: id, type: utf8, nullable: false }
        - { name: received_at, path: timestamp, type: timestamp_s_string_utc, nullable: false }
    - field: account_update
      rows: value
      destination: { table: staging.main.wa_events_account }
      columns:
        - { name: raw, path: _raw, type: json, nullable: false }
"#;

    fn env_all(name: &str) -> Option<String> {
        Some(format!("secret-for-{name}"))
    }

    #[test]
    fn minimal_definition_loads_with_defaults() {
        let def = WebhookDefinition::from_yaml_with_env(MINIMAL, env_all)
            .unwrap()
            .expect("kind: webhook is recognised");
        assert_eq!(def.name, "wa-inbound");
        assert_eq!(def.version.as_deref(), Some("1.0.0"));
        assert_eq!(def.provider, WebhookProvider::MetaWhatsapp);
        assert_eq!(def.max_body_bytes, DEFAULT_MAX_BODY_BYTES);
        assert_eq!(def.verify_token.expose(), "secret-for-WA_VERIFY");
        assert_eq!(def.app_secret.expose(), "secret-for-WA_SECRET");
        assert_eq!(def.routes.len(), 2);
        assert_eq!(def.routes[0].rows, RowSelector::List);
        assert_eq!(def.routes[1].rows, RowSelector::Value);
        assert_eq!(def.routes[0].columns[1].name, "received_at");
    }

    #[test]
    fn other_kinds_are_skipped_not_rejected() {
        let yaml = "kind: job\nmetadata: { name: x }\nspec: { query: 'SELECT 1' }\n";
        assert!(
            WebhookDefinition::from_yaml_with_env(yaml, env_all)
                .unwrap()
                .is_none()
        );
        let yaml = "metadata: { name: x }\n";
        assert!(
            WebhookDefinition::from_yaml_with_env(yaml, env_all)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn a_missing_secret_fails_at_load_time_and_names_the_variable() {
        let err = WebhookDefinition::from_yaml_with_env(MINIMAL, |name| {
            (name == "WA_VERIFY").then(|| "t".to_string())
        })
        .unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("WA_SECRET"), "{msg}");
        assert!(msg.contains("app_secret_env"), "{msg}");
    }

    #[test]
    fn secrets_never_appear_in_debug_output() {
        let def = WebhookDefinition::from_yaml_with_env(MINIMAL, env_all)
            .unwrap()
            .unwrap();
        let dump = format!("{def:?}");
        assert!(!dump.contains("secret-for"), "{dump}");
        assert!(dump.contains("<redacted>"));
    }

    #[test]
    fn upsert_mode_is_refused_at_parse_time() {
        let yaml = MINIMAL.replace("mode: append", "mode: upsert");
        let err = WebhookDefinition::from_yaml_with_env(&yaml, env_all).unwrap_err();
        assert!(format!("{err:#}").contains("append"), "{err:#}");
    }

    #[test]
    fn an_ingested_at_column_cannot_be_declared() {
        let yaml = MINIMAL.replace("name: raw, path: _raw", "name: ingested_at, path: _raw");
        let err = WebhookDefinition::from_yaml_with_env(&yaml, env_all).unwrap_err();
        assert!(format!("{err:#}").contains("ingested_at"), "{err:#}");
    }

    #[test]
    fn duplicate_columns_and_bad_names_are_refused() {
        let yaml = MINIMAL.replace(
            "name: received_at, path: timestamp",
            "name: message_id, path: timestamp",
        );
        let err = WebhookDefinition::from_yaml_with_env(&yaml, env_all).unwrap_err();
        assert!(format!("{err:#}").contains("twice"), "{err:#}");

        let yaml = MINIMAL.replace("name: wa-inbound", "name: 'wa inbound'");
        let err = WebhookDefinition::from_yaml_with_env(&yaml, env_all).unwrap_err();
        assert!(format!("{err:#}").contains("URL path segment"), "{err:#}");
    }

    #[test]
    fn unknown_keys_anywhere_are_refused() {
        let yaml = MINIMAL.replace(
            "provider: meta_whatsapp",
            "provider: meta_whatsapp\n  retries: 3",
        );
        assert!(WebhookDefinition::from_yaml_with_env(&yaml, env_all).is_err());
        let yaml = MINIMAL.replace("provider: meta_whatsapp", "provider: slack_events");
        assert!(WebhookDefinition::from_yaml_with_env(&yaml, env_all).is_err());
    }

    #[test]
    fn load_from_file_reads_webhooks_and_skips_other_kinds() {
        let dir = tempfile::tempdir().unwrap();
        let hook = dir.path().join("hook.yaml");
        std::fs::write(&hook, MINIMAL).unwrap();
        let job = dir.path().join("job.yaml");
        std::fs::write(
            &job,
            "kind: job\nmetadata: { name: j }\nspec: { query: 'SELECT 1' }\n",
        )
        .unwrap();
        let broken = dir.path().join("broken.yaml");
        std::fs::write(&broken, "kind: webhook\nmetadata: [\n").unwrap();

        // SAFETY: test-only, unique variable names, read once by this call.
        unsafe {
            std::env::set_var("WA_VERIFY", "v");
            std::env::set_var("WA_SECRET", "s");
        }
        let loaded = WebhookDefinition::load_from_file(&hook).unwrap().unwrap();
        assert_eq!(loaded.name, "wa-inbound");
        assert!(WebhookDefinition::load_from_file(&job).unwrap().is_none());
        let err = WebhookDefinition::load_from_file(&broken).unwrap_err();
        assert!(format!("{err:#}").contains("broken.yaml"), "{err:#}");
        let err = WebhookDefinition::load_from_file(dir.path().join("missing.yaml")).unwrap_err();
        assert!(format!("{err:#}").contains("missing.yaml"), "{err:#}");
    }

    #[rstest::rstest]
    #[case::tiny_body(
        "  app_secret_env: WA_SECRET\n",
        "  app_secret_env: WA_SECRET\n  max_body_bytes: 10\n",
        "max_body_bytes"
    )]
    #[case::empty_field("    - field: messages\n", "    - field: ''\n", "empty field")]
    #[case::empty_table(
        "table: staging.main.wa_events_messages",
        "table: ''",
        "empty destination table"
    )]
    #[case::no_columns(
        "        - { name: raw, path: _raw, type: json, nullable: false }\n",
        "",
        "declares no columns"
    )]
    #[case::empty_env_name(
        "verify_token_env: WA_VERIFY",
        "verify_token_env: ''",
        "empty environment variable"
    )]
    fn each_validation_rule_names_the_problem(
        #[case] old: &str,
        #[case] new: &str,
        #[case] expected: &str,
    ) {
        let yaml = MINIMAL.replace(old, new);
        assert_ne!(yaml, MINIMAL, "the case must change the document");
        let err = WebhookDefinition::from_yaml_with_env(&yaml, env_all).unwrap_err();
        assert!(format!("{err:#}").contains(expected), "{err:#}");
    }

    #[test]
    fn dead_letter_is_optional_and_its_table_must_not_be_empty() {
        let def = WebhookDefinition::from_yaml_with_env(MINIMAL, env_all)
            .unwrap()
            .unwrap();
        assert!(def.dead_letter.is_none());
        let yaml = MINIMAL.replace(
            "  routes:\n",
            "  dead_letter: { table: staging.main.wa_events_dead_letter }\n  routes:\n",
        );
        let def = WebhookDefinition::from_yaml_with_env(&yaml, env_all)
            .unwrap()
            .unwrap();
        assert_eq!(
            def.dead_letter.as_deref(),
            Some("staging.main.wa_events_dead_letter")
        );
        let yaml = MINIMAL.replace("  routes:\n", "  dead_letter: { table: '' }\n  routes:\n");
        let err = WebhookDefinition::from_yaml_with_env(&yaml, env_all).unwrap_err();
        assert!(
            format!("{err:#}").contains("dead_letter.table is empty"),
            "{err:#}"
        );
        let yaml = MINIMAL.replace(
            "  routes:\n",
            "  dead_letter: { table: x, mode: append }\n  routes:\n",
        );
        assert!(
            WebhookDefinition::from_yaml_with_env(&yaml, env_all).is_err(),
            "unknown keys refused"
        );
    }

    #[test]
    fn a_definition_without_routes_is_refused() {
        let yaml = "kind: webhook\nmetadata: { name: r }\nspec:\n  provider: meta_whatsapp\n  verify_token_env: WA_VERIFY\n  app_secret_env: WA_SECRET\n  routes: []\n";
        let err = WebhookDefinition::from_yaml_with_env(yaml, env_all).unwrap_err();
        assert!(format!("{err:#}").contains("at least one route"), "{err:#}");
    }

    #[test]
    fn provider_label_and_optional_metadata_round_trip() {
        let def = WebhookDefinition::from_yaml_with_env(MINIMAL, env_all)
            .unwrap()
            .unwrap();
        assert_eq!(def.provider.label(), "meta_whatsapp");
        assert!(def.description.is_none());
        let yaml = MINIMAL.replace(
            "version: \"1.0.0\"",
            "version: \"1.0.0\", description: \"d\"",
        );
        let def = WebhookDefinition::from_yaml_with_env(&yaml, env_all)
            .unwrap()
            .unwrap();
        assert_eq!(def.description.as_deref(), Some("d"));
    }
}
