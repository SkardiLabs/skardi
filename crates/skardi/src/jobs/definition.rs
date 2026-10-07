//! `kind: job` YAML loader.
//!
//! A job YAML uses the same `{ kind, metadata, spec }` envelope as pipelines,
//! contexts, and aliases. The `spec:` block holds the query plus the
//! job-specific destination and execution sections:
//!
//! ```yaml
//! kind: job
//! metadata:
//!   name: "backfill-wiki"
//!   version: "1.0.0"
//!   description: "Backfill wiki pages into the lake."
//! spec:
//!   query: |
//!     SELECT id, title, content
//!     FROM wiki.public.wiki_pages
//!     WHERE updated_at >= {from_date}
//!       AND updated_at <  {to_date}
//!   destination:
//!     table: "wiki_lake"          # DataFusion table ident (or dotted path)
//!     mode: append                # append | upsert (overwrite is deferred — see below)
//!     create_if_missing: true     # lake destinations only
//!     # merge_keys: [id]          # required with `mode: upsert`, refused otherwise
//!     # repeated_keys: keep_last  # upsert only: refuse (default) | keep_last
//!   execution:
//!     timeout_ms: 3600000         # optional; default = no timeout
//! ```
//!
//! `{placeholder}` tokens in the SQL are inferred as typed scalar params by
//! the pipeline loader — the same mechanism pipelines use. There is no
//! separate `parameters:` block.

use anyhow::{Context, Result, anyhow};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use crate::pipeline::pipeline::{Pipeline, StandardPipeline};

/// Root-level `kind:` values the job loader recognizes. Any other value —
/// or a missing `kind:` — is an error. `Pipeline` is accepted so that
/// `JobDefinition::load_from_file` can short-circuit with `Ok(None)` when
/// the caller accidentally points it at a pipeline YAML.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobKind {
    #[serde(rename = "pipeline")]
    Pipeline,
    #[serde(rename = "job")]
    Job,
}

/// Write mode for the destination.
///
/// `append` adds every row. `upsert` merges on [`Destination::merge_keys`]:
/// a row whose keys match an existing row replaces it, and any other row is
/// inserted. Upsert is Lance-only — the SQL destinations refuse it at submit,
/// since DataFusion's DML has no MERGE to drive them with.
///
/// `overwrite` is deferred because the
/// DataFusion SQL interface we drive DB destinations through cannot wrap
/// a `DELETE FROM` + `INSERT INTO` pair in a single transaction, so an
/// overwrite whose INSERT failed after a successful DELETE would silently
/// leave the destination empty — violating the job atomicity contract.
/// Re-enable alongside provider-native transactional DML or a staging-swap
/// strategy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DestinationMode {
    #[default]
    Append,
    Upsert,
}

fn default_create_if_missing() -> bool {
    true
}

/// The `destination:` block of a job YAML.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Destination {
    /// DataFusion table identifier. Can be bare (`wiki_lake`) or dotted
    /// (`catalog.schema.table`). Resolves the same way as any `FROM` clause
    /// in pipeline SQL.
    pub table: String,
    #[serde(default)]
    pub mode: DestinationMode,
    /// For lake destinations only: create the dataset on first run if it
    /// does not yet exist. DB destinations always reject submit if the
    /// table is missing, regardless of this flag.
    #[serde(default = "default_create_if_missing")]
    pub create_if_missing: bool,
    /// The columns an `upsert` matches rows on. Required with `mode: upsert`
    /// and refused with any other mode, so a key list can never be silently
    /// ignored. Each must be a column of the query's output; the executor
    /// checks that at submit, once the output schema is known.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub merge_keys: Vec<String>,
    /// What an `upsert` does with a key that appears more than once in one
    /// run's output: [`RepeatedKeys::Refuse`] (the default) or
    /// [`RepeatedKeys::KeepLast`]. Only meaningful with `mode: upsert`.
    #[serde(default, skip_serializing_if = "RepeatedKeys::is_refuse")]
    pub repeated_keys: RepeatedKeys,
}

/// What an upsert does with a merge key repeated within one run's output.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RepeatedKeys {
    /// Fail the write, naming the key, and commit nothing: the run is never
    /// resolved by picking one copy silently.
    #[default]
    Refuse,
    /// Keep the copy that comes LAST in the query's output order, for a
    /// source whose listing can return one record twice in a window (a
    /// record updated mid-pagination reappears on a later page, so the last
    /// copy is the newest). The run's whole output is buffered to find the
    /// last copy, and `rows_written` counts every row the query produced, so
    /// a caller comparing it against a LIMIT still sees a full page as full.
    KeepLast,
}

impl RepeatedKeys {
    fn is_refuse(&self) -> bool {
        *self == RepeatedKeys::Refuse
    }
}

impl Destination {
    /// The load-time half of the upsert contract: what can be checked
    /// without planning the query.
    pub fn validate(&self) -> Result<()> {
        match self.mode {
            DestinationMode::Upsert if self.merge_keys.is_empty() => Err(anyhow!(
                "destination `{}`: `mode: upsert` needs `merge_keys`, the columns rows are \
                 matched on",
                self.table
            )),
            DestinationMode::Append if !self.merge_keys.is_empty() => Err(anyhow!(
                "destination `{}`: `merge_keys` only applies to `mode: upsert`; with \
                 `mode: append` it would be ignored",
                self.table
            )),
            DestinationMode::Append if self.repeated_keys != RepeatedKeys::Refuse => Err(anyhow!(
                "destination `{}`: `repeated_keys` only applies to `mode: upsert`; with \
                 `mode: append` it would be ignored",
                self.table
            )),
            _ => {
                let mut seen = HashSet::new();
                for key in &self.merge_keys {
                    if key.trim().is_empty() {
                        return Err(anyhow!(
                            "destination `{}`: `merge_keys` has an empty column name",
                            self.table
                        ));
                    }
                    if !seen.insert(key.as_str()) {
                        return Err(anyhow!(
                            "destination `{}`: `merge_keys` names `{key}` twice",
                            self.table
                        ));
                    }
                }
                Ok(())
            }
        }
    }
}

/// The `execution:` block of a job YAML.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Execution {
    /// Optional wall-clock timeout. When unset, the job runs until it
    /// succeeds or errors out.
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// A loaded job definition — wraps a `StandardPipeline` (for metadata,
/// query, and inferred request/response schemas) and adds the
/// job-specific destination + execution blocks.
#[derive(Debug, Clone)]
pub struct JobDefinition {
    pub pipeline: StandardPipeline,
    pub destination: Destination,
    pub execution: Execution,
}

impl JobDefinition {
    /// Job name, from the pipeline's metadata.
    pub fn name(&self) -> &str {
        self.pipeline.name()
    }

    /// Job version, from the pipeline's metadata.
    pub fn version(&self) -> &str {
        self.pipeline.version()
    }

    /// Parsed SQL template for the job.
    pub fn sql(&self) -> &str {
        &self.pipeline.query_definition().sql
    }

    /// Load a job definition from a YAML file.
    ///
    /// Returns `Ok(None)` if the file is a pipeline (no `kind: job` at the
    /// root) and `Ok(Some(..))` if it's a job. Errors surface as anyhow
    /// with context about which file failed to parse.
    pub async fn load_from_file<P: AsRef<Path> + Send>(
        path: P,
        ctx: Arc<SessionContext>,
    ) -> Result<Option<Self>> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read job file: {}", path.display()))?;

        let root: serde_yaml::Value = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse job YAML: {}", path.display()))?;

        let kind_value = root.get("kind").ok_or_else(|| {
            anyhow!(
                "Missing `kind:` at root of {} — expected `kind: job` or `kind: pipeline`",
                path.display()
            )
        })?;
        let kind: JobKind = serde_yaml::from_value(kind_value.clone())
            .with_context(|| format!("Invalid `kind:` in {}", path.display()))?;

        if kind != JobKind::Job {
            return Ok(None);
        }

        let spec = root.get("spec").ok_or_else(|| {
            anyhow!(
                "`kind: job` YAML {} is missing a `spec:` block",
                path.display()
            )
        })?;

        let destination_value = spec.get("destination").ok_or_else(|| {
            anyhow!(
                "`kind: job` YAML {} is missing `spec.destination`",
                path.display()
            )
        })?;
        let destination: Destination = serde_yaml::from_value(destination_value.clone())
            .with_context(|| format!("Failed to parse destination block in {}", path.display()))?;
        destination
            .validate()
            .with_context(|| format!("Invalid destination block in {}", path.display()))?;

        let execution: Execution = match spec.get("execution") {
            Some(v) => serde_yaml::from_value(v.clone()).with_context(|| {
                format!("Failed to parse execution block in {}", path.display())
            })?,
            None => Execution::default(),
        };

        // Reuse the pipeline builder for metadata + query + schema inference.
        // We've already parsed `root` and validated `kind: job`, so we hand
        // the parsed value straight to the shared builder rather than
        // re-reading and re-parsing the file through the pipeline loader
        // (which is now strict to `kind: pipeline`).
        let pipeline = StandardPipeline::build_from_parsed(root, ctx)
            .await
            .with_context(|| format!("Failed to load pipeline section of {}", path.display()))?;

        Ok(Some(Self {
            pipeline,
            destination,
            execution,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    async fn write_and_load(
        yaml: &str,
        ctx: Arc<SessionContext>,
    ) -> anyhow::Result<Option<JobDefinition>> {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(yaml.as_bytes()).unwrap();
        JobDefinition::load_from_file(f.path(), ctx).await
    }

    #[tokio::test]
    async fn pipeline_yaml_loads_as_none() {
        let yaml = r#"
kind: pipeline
metadata:
  name: "p1"
  version: "1.0.0"
spec:
  query: |
    SELECT 1 AS v
"#;
        let ctx = Arc::new(SessionContext::new());
        let res = write_and_load(yaml, ctx).await.unwrap();
        assert!(res.is_none(), "plain pipeline should return Ok(None)");
    }

    #[tokio::test]
    async fn job_yaml_loads_with_destination() {
        let yaml = r#"
kind: job
metadata:
  name: "ingest-j1"
  version: "1.0.0"
  description: "Ingest job"
spec:
  query: |
    SELECT 1 AS id, 'a' AS name
  destination:
    table: "target_table"
    mode: append
    create_if_missing: true
  execution:
    timeout_ms: 60000
"#;
        let ctx = Arc::new(SessionContext::new());
        let res = write_and_load(yaml, ctx).await.unwrap().unwrap();
        assert_eq!(res.name(), "ingest-j1");
        assert_eq!(res.destination.table, "target_table");
        assert_eq!(res.destination.mode, DestinationMode::Append);
        assert!(res.destination.create_if_missing);
        assert_eq!(res.execution.timeout_ms, Some(60000));
    }

    #[tokio::test]
    async fn job_yaml_defaults_create_if_missing_to_true() {
        let yaml = r#"
kind: job
metadata:
  name: "ingest-j2"
  version: "1.0.0"
spec:
  query: "SELECT 1 AS id"
  destination:
    table: "target_table"
"#;
        let ctx = Arc::new(SessionContext::new());
        let res = write_and_load(yaml, ctx).await.unwrap().unwrap();
        // `mode:` omitted → defaults to Append.
        assert_eq!(res.destination.mode, DestinationMode::Append);
        assert!(res.destination.create_if_missing);
        assert!(res.execution.timeout_ms.is_none());
    }

    #[tokio::test]
    async fn job_yaml_rejects_overwrite_mode() {
        let yaml = r#"
kind: job
metadata:
  name: "bad"
  version: "1.0.0"
spec:
  query: "SELECT 1"
  destination:
    table: "t"
    mode: overwrite
"#;
        let ctx = Arc::new(SessionContext::new());
        // Format with the alternate Display so the full anyhow cause chain
        // — which is where serde's "unknown variant" message lives — shows
        // up for the assertion.
        let err = format!("{:#}", write_and_load(yaml, ctx).await.unwrap_err());
        assert!(
            err.contains("overwrite") || err.contains("unknown variant"),
            "unexpected error: {err}"
        );
    }

    fn upsert_yaml(destination: &str) -> String {
        format!(
            r#"
kind: job
metadata:
  name: "merge"
  version: "1.0.0"
spec:
  query: "SELECT 1 AS id"
  destination:
{destination}
"#
        )
    }

    #[tokio::test]
    async fn job_yaml_reads_repeated_keys_and_refuses_it_without_upsert() {
        let load = |destination: &str| {
            let yaml = upsert_yaml(destination);
            async move { write_and_load(&yaml, Arc::new(SessionContext::new())).await }
        };
        let keep_last = load(
            "    table: \"t\"\n    mode: upsert\n    merge_keys: [id]\n    repeated_keys: keep_last",
        )
        .await
        .expect("keep_last loads")
        .expect("it is a job");
        assert_eq!(keep_last.destination.repeated_keys, RepeatedKeys::KeepLast);

        let default = load("    table: \"t\"\n    mode: upsert\n    merge_keys: [id]")
            .await
            .expect("loads")
            .expect("it is a job");
        assert_eq!(default.destination.repeated_keys, RepeatedKeys::Refuse);

        let err = load("    table: \"t\"\n    mode: append\n    repeated_keys: keep_last")
            .await
            .expect_err("repeated_keys without upsert");
        assert!(
            format!("{err:#}").contains("only applies to `mode: upsert`"),
            "{err:#}"
        );
    }

    #[tokio::test]
    async fn job_yaml_accepts_upsert_with_merge_keys() {
        let yaml = upsert_yaml("    table: \"t\"\n    mode: upsert\n    merge_keys: [id, part]");
        let job = write_and_load(&yaml, Arc::new(SessionContext::new()))
            .await
            .expect("upsert with keys loads")
            .expect("it is a job");
        assert_eq!(job.destination.mode, DestinationMode::Upsert);
        assert_eq!(job.destination.merge_keys, vec!["id", "part"]);
    }

    #[rstest::rstest]
    #[case::upsert_without_keys("    table: \"t\"\n    mode: upsert", "needs `merge_keys`")]
    #[case::keys_without_upsert(
        "    table: \"t\"\n    mode: append\n    merge_keys: [id]",
        "only applies to `mode: upsert`"
    )]
    #[case::keys_with_default_mode("    table: \"t\"\n    merge_keys: [id]", "only applies")]
    #[case::duplicate_key(
        "    table: \"t\"\n    mode: upsert\n    merge_keys: [id, id]",
        "names `id` twice"
    )]
    #[case::empty_key(
        "    table: \"t\"\n    mode: upsert\n    merge_keys: [\"\"]",
        "empty column"
    )]
    #[tokio::test]
    async fn job_yaml_refuses_an_inconsistent_upsert(
        #[case] destination: &str,
        #[case] expected: &str,
    ) {
        let yaml = upsert_yaml(destination);
        let err = format!(
            "{:#}",
            write_and_load(&yaml, Arc::new(SessionContext::new()))
                .await
                .unwrap_err()
        );
        assert!(err.contains(expected), "expected {expected:?} in: {err}");
    }

    #[tokio::test]
    async fn job_yaml_missing_destination_errors() {
        let yaml = r#"
kind: job
metadata:
  name: "bad-job"
  version: "1.0.0"
spec:
  query: "SELECT 1"
"#;
        let ctx = Arc::new(SessionContext::new());
        let err = write_and_load(yaml, ctx).await.unwrap_err().to_string();
        assert!(err.contains("destination"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn job_yaml_unknown_kind_errors() {
        let yaml = r#"
kind: stream
metadata:
  name: "bad"
  version: "1.0.0"
spec:
  query: "SELECT 1"
"#;
        let ctx = Arc::new(SessionContext::new());
        let err = write_and_load(yaml, ctx).await.unwrap_err().to_string();
        assert!(err.contains("kind"), "unexpected error: {err}");
    }
}
