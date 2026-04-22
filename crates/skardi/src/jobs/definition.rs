//! `kind: job` YAML loader.
//!
//! A job YAML has the same `metadata` + `query` shape as a pipeline, with
//! two additional sections:
//!
//! ```yaml
//! kind: job                    # discriminator — pipelines default to `pipeline`
//! metadata:
//!   name: "backfill-wiki"
//!   version: "1.0.0"
//!   description: "Backfill wiki pages into the lake."
//! query: |
//!   SELECT id, title, content
//!   FROM wiki.public.wiki_pages
//!   WHERE updated_at >= {from_date}
//!     AND updated_at <  {to_date}
//! destination:
//!   table: "wiki_lake"          # DataFusion table ident (or dotted path)
//!   mode: append                # append (overwrite is deferred — see below)
//!   create_if_missing: true     # lake destinations only
//! execution:
//!   timeout_ms: 3600000         # optional; default = no timeout
//! ```
//!
//! `{placeholder}` tokens in the SQL are inferred as typed scalar params by
//! the pipeline loader — the same mechanism pipelines use. There is no
//! separate `parameters:` block.

use anyhow::{Context, Result, anyhow};
use datafusion::prelude::SessionContext;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::sync::Arc;

use crate::pipeline::pipeline::{Pipeline, StandardPipeline};

/// Discriminator at the YAML root that tells the loader whether the file is
/// a pipeline or a job. The default is `Pipeline` so existing pipeline
/// YAMLs — which do not set `kind:` — continue to load unchanged.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobKind {
    #[serde(rename = "pipeline")]
    Pipeline,
    #[serde(rename = "job")]
    Job,
}

impl Default for JobKind {
    fn default() -> Self {
        Self::Pipeline
    }
}

/// Write mode for the destination.
///
/// MVP supports `append` only. `overwrite` is deferred because the
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

        let kind: JobKind = match root.get("kind") {
            Some(v) => serde_yaml::from_value(v.clone())
                .with_context(|| format!("Invalid `kind:` in {}", path.display()))?,
            None => JobKind::Pipeline,
        };

        if kind != JobKind::Job {
            return Ok(None);
        }

        let destination_value = root
            .get("destination")
            .ok_or_else(|| anyhow!("`kind: job` YAML missing `destination:` block"))?;
        let destination: Destination = serde_yaml::from_value(destination_value.clone())
            .with_context(|| format!("Failed to parse destination block in {}", path.display()))?;

        let execution: Execution = match root.get("execution") {
            Some(v) => serde_yaml::from_value(v.clone()).with_context(|| {
                format!("Failed to parse execution block in {}", path.display())
            })?,
            None => Execution::default(),
        };

        // Reuse the pipeline loader for metadata + query + schema inference.
        // `StandardPipeline::load_from_file` reads the file itself, so it
        // happily ignores extra top-level keys like `kind`, `destination`,
        // and `execution`.
        let pipeline = StandardPipeline::load_from_file(path, ctx)
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
metadata:
  name: "p1"
  version: "1.0.0"
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

    #[tokio::test]
    async fn job_yaml_missing_destination_errors() {
        let yaml = r#"
kind: job
metadata:
  name: "bad-job"
  version: "1.0.0"
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
query: "SELECT 1"
"#;
        let ctx = Arc::new(SessionContext::new());
        let err = write_and_load(yaml, ctx).await.unwrap_err().to_string();
        assert!(err.contains("kind"), "unexpected error: {err}");
    }
}
