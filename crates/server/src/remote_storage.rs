use anyhow::Result;
use datafusion::prelude::*;
use std::path::PathBuf;
use std::sync::Arc;

use crate::config::{ConfigError, DataSource};

/// Trait for remote storage operations
#[async_trait::async_trait]
pub trait RemoteStorage {
    /// Determine if a path is a remote storage path
    fn is_remote_path(&self, path: &PathBuf) -> bool;

    /// Validate configuration for remote storage
    fn validate_configuration(&self, source: &DataSource) -> Result<()>;

    /// Setup remote storage object store with DataFusion
    async fn setup_object_store(
        &self,
        session_ctx: &mut SessionContext,
        source_name: &str,
        storage_path: &str,
    ) -> Result<()>;

    /// Test connectivity to remote storage
    async fn test_connectivity(
        &self,
        store: &Arc<dyn object_store::ObjectStore>,
        source_name: &str,
        storage_path: &str,
        region: &str,
    ) -> Result<()>;
}

/// S3 remote storage implementation
pub struct S3Storage;

impl S3Storage {
    pub fn new() -> Self {
        Self
    }

    /// Reject `aws_*` credential/region keys in a source's `options` — these must
    /// come from the environment / IAM, never from a config file.
    fn reject_credential_options(source: &DataSource) -> Result<()> {
        let Some(options) = &source.options else {
            return Ok(());
        };
        let forbidden_keys = [
            "aws_access_key_id",
            "aws_secret_access_key",
            "aws_session_token",
            "aws_region", // Also reject aws_region since it should come from env vars
        ];
        for key in &forbidden_keys {
            if options.contains_key(*key) {
                return Err(ConfigError::S3ObjectStoreRegistrationFailed {
                    name: source.name.clone(),
                    error: format!(
                        "AWS configuration ('{}') must not be stored in configuration files. \
                         Please use environment variables instead:\n\
                         - Set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY for credentials\n\
                         - Set AWS_REGION or AWS_DEFAULT_REGION for region configuration\n\
                         - Or use AWS_PROFILE to specify an AWS credentials profile\n\
                         - Or use IAM roles/instance profiles on AWS infrastructure",
                        key
                    ),
                }
                .into());
            }
        }
        Ok(())
    }

    /// Validate S3 configuration for a `documents` source, where the source
    /// `path` and the `image_store` option may **each independently** be a local
    /// directory or an `s3://` prefix.
    ///
    /// - Rejects `aws_*` credential keys in `options` when *either* side is
    ///   `s3://` (closing the gap where [`validate_configuration`] only fires for
    ///   an `s3://` `path`, letting a local-`path` + `s3://`-`image_store` source
    ///   smuggle credentials into `options`).
    /// - When **both** are `s3://`, requires the **same bucket**: a single
    ///   registered store / region / credential set cannot serve two buckets
    ///   (see design doc §4). Cross-bucket support is a tracked follow-up.
    ///
    /// [`validate_configuration`]: RemoteStorage::validate_configuration
    pub fn validate_documents_configuration(
        &self,
        path: &str,
        image_store: Option<&str>,
        source: &DataSource,
    ) -> Result<()> {
        // Reject an `image_store` that equals or is an ancestor of the source
        // `path`. `list_docs` excludes everything under `image_store` as a
        // self-ingestion guard, so such overlap silently drops every source
        // document and the scan returns an empty-but-successful result. Applies
        // to any backend (local or s3://); `image_store` nested *inside* the
        // source is still allowed — that's the guard's intended use.
        if let Some(img) = image_store {
            if loc_is_under_or_equal(path, img) {
                return Err(ConfigError::DataSourceRegistrationFailed {
                    name: source.name.clone(),
                    error: format!(
                        "documents: `image_store` ('{img}') equals or is an ancestor of the \
                         source `path` ('{path}'). Every source document lives under `image_store` \
                         and would be excluded from the scan (self-ingestion guard), yielding an \
                         empty result. Point `image_store` at a location nested inside or disjoint \
                         from `path`.",
                    ),
                }
                .into());
            }
        }

        let path_is_s3 = path.starts_with("s3://");
        let image_is_s3 = image_store.is_some_and(|s| s.starts_with("s3://"));

        if !path_is_s3 && !image_is_s3 {
            return Ok(()); // fully local — nothing S3 to validate
        }

        // Once any side is S3, credentials must not live in config.
        Self::reject_credential_options(source)?;

        let path_bucket = if path_is_s3 {
            Some(parse_bucket(path, &source.name)?)
        } else {
            None
        };
        let image_bucket = if image_is_s3 {
            Some(parse_bucket(image_store.unwrap(), &source.name)?)
        } else {
            None
        };

        if let (Some(pb), Some(ib)) = (&path_bucket, &image_bucket) {
            if pb != ib {
                return Err(ConfigError::S3ObjectStoreRegistrationFailed {
                    name: source.name.clone(),
                    error: format!(
                        "documents: `path` bucket '{pb}' and `image_store` bucket '{ib}' differ. \
                         When both are s3://, they must be in the same bucket — a single object \
                         store / region / credential set cannot serve two buckets. Cross-bucket \
                         (cross-region/account) support is a tracked follow-up."
                    ),
                }
                .into());
            }
        }
        Ok(())
    }

    /// Registration-time reachability checks for a `documents` source's S3
    /// endpoints (no-op for local paths):
    ///
    /// - **Read connectivity** — a *prefix-aware* `list` of the source `path`
    ///   (the object `head` used for CSV/Parquet returns `NotFound` for a
    ///   prefix). Only the stream's first item is polled — a full top-level
    ///   inventory of a large prefix is never drained at startup — so an
    ///   empty-but-reachable prefix is OK while auth/network errors still surface.
    /// - **Write preflight** — put+delete a probe object under `image_store` so a
    ///   missing `s3:PutObject` fails loudly here rather than silently dropping
    ///   crops mid-scan.
    pub async fn preflight_documents_s3(
        &self,
        path: &str,
        image_store: Option<&str>,
        source_name: &str,
    ) -> Result<()> {
        use futures::StreamExt;
        use object_store::PutPayload;
        use object_store::path::Path as ObjectPath;

        if path.starts_with("s3://") {
            let bucket = parse_bucket(path, source_name)?;
            let (store, region) = build_bucket_store(&bucket, source_name)?;
            let prefix = s3_key_prefix(path);
            let op = (!prefix.is_empty()).then(|| ObjectPath::from(prefix.as_str()));
            // Poll only the first stream item: this proves auth/network/prefix
            // reachability without draining a full inventory of a large prefix.
            // `None` (empty but reachable) is a pass; a first-item error fails.
            if let Some(res) = store.list(op.as_ref()).next().await {
                res.map_err(|e| ConfigError::S3ObjectStoreRegistrationFailed {
                    name: source_name.to_string(),
                    error: format!(
                        "documents: S3 read connectivity check failed for '{}' (region '{}'): {}\n\
                         Ensure s3:ListBucket + s3:GetObject on the bucket/prefix and that \
                         credentials/region are configured via the environment.",
                        path, region, e
                    ),
                })?;
            }
        }

        if let Some(img) = image_store.filter(|s| s.starts_with("s3://")) {
            let bucket = parse_bucket(img, source_name)?;
            let (store, region) = build_bucket_store(&bucket, source_name)?;
            let base = s3_key_prefix(img);
            let probe = ObjectPath::from(write_probe_key(&base));
            store
                .put(&probe, PutPayload::from_static(b"skardi-write-probe"))
                .await
                .map_err(|e| ConfigError::S3ObjectStoreRegistrationFailed {
                    name: source_name.to_string(),
                    error: format!(
                        "documents: image_store write preflight failed for '{}' (region '{}'): {}\n\
                         Ensure s3:PutObject on the image_store bucket/prefix.",
                        img, region, e
                    ),
                })?;
            // Best-effort cleanup — a leftover probe object is harmless.
            let _ = store.delete(&probe).await;
        }
        Ok(())
    }
}

/// Extract the bucket name from an `s3://bucket/key` URI.
fn parse_bucket(s3_uri: &str, source_name: &str) -> Result<String> {
    let url = url::Url::parse(s3_uri).map_err(|e| ConfigError::InvalidS3Path {
        path: format!("Invalid S3 URL '{}': {}", s3_uri, e),
    })?;
    url.host_str()
        .map(|h| h.to_string())
        .filter(|h| !h.is_empty())
        .ok_or_else(|| {
            ConfigError::S3ObjectStoreRegistrationFailed {
                name: source_name.to_string(),
                error: format!("No bucket name found in S3 URL: {}", s3_uri),
            }
            .into()
        })
}

/// Build the write-probe object key placed under an `image_store` prefix during
/// the write preflight. An empty prefix probes the bucket root; otherwise the
/// probe sits directly under the (single-slash-normalized) prefix.
fn write_probe_key(base: &str) -> String {
    if base.is_empty() {
        ".skardi-write-probe".to_string()
    } else {
        format!("{}/.skardi-write-probe", base.trim_end_matches('/'))
    }
}

/// Whether `inner` is the same location as, or nested under, `outer`
/// (path-segment aware, trailing slashes ignored). Used at registration to
/// detect a documents `image_store` that would swallow the source `path`.
/// Cross-backend pairs (local vs `s3://`) never match, mirroring `list_docs`'s
/// `loc_is_under`.
fn loc_is_under_or_equal(inner: &str, outer: &str) -> bool {
    let inner = inner.trim_end_matches('/');
    let outer = outer.trim_end_matches('/');
    inner == outer || inner.starts_with(&format!("{outer}/"))
}

/// The key/prefix portion of an `s3://bucket/key` URI (no leading `/`).
fn s3_key_prefix(s3_uri: &str) -> String {
    url::Url::parse(s3_uri)
        .ok()
        .map(|u| u.path().trim_start_matches('/').to_string())
        .unwrap_or_default()
}

/// Validate that a region and some credential source are present, returning the
/// resolved region. Pure over its inputs (the caller reads the env), so the
/// missing-config branches are unit-testable without mutating process-global
/// env vars.
///
/// `AWS_PROFILE` satisfies this check for parity with
/// [`S3Storage::setup_object_store`], but `object_store` never reads `~/.aws/`
/// profiles — with only `AWS_PROFILE` set, request signing has no credentials
/// and the preflight list/put in [`S3Storage::preflight_documents_s3`] fails at
/// startup with the underlying S3 error. Exporting real env credentials
/// (`aws configure export-credentials --format env`) is the working path; see
/// docs/documents.md § "S3 / object store".
fn require_s3_region_and_creds(
    region: Option<String>,
    has_access_key: bool,
    has_profile: bool,
    source_name: &str,
) -> Result<String> {
    let region = region.ok_or_else(|| ConfigError::MissingAwsConfig {
        name: source_name.to_string(),
        field: "AWS_REGION or AWS_DEFAULT_REGION environment variable".to_string(),
    })?;
    if !has_access_key && !has_profile {
        return Err(ConfigError::MissingAwsConfig {
            name: source_name.to_string(),
            field: "AWS_ACCESS_KEY_ID environment variable or AWS_PROFILE".to_string(),
        }
        .into());
    }
    Ok(region)
}

/// Build an S3 object store for `bucket` using env/IAM credentials and the
/// `AWS_REGION`/`AWS_DEFAULT_REGION` region. Returns the store and the region
/// (for error messages). Mirrors the credential contract of
/// [`S3Storage::setup_object_store`].
fn build_bucket_store(
    bucket: &str,
    source_name: &str,
) -> Result<(Arc<dyn object_store::ObjectStore>, String)> {
    use object_store::aws::AmazonS3Builder;

    let region = std::env::var("AWS_REGION")
        .ok()
        .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok());
    let has_access_key = std::env::var("AWS_ACCESS_KEY_ID").is_ok();
    let has_profile = std::env::var("AWS_PROFILE").is_ok();
    let region = require_s3_region_and_creds(region, has_access_key, has_profile, source_name)?;

    let store = AmazonS3Builder::from_env()
        .with_bucket_name(bucket)
        .with_region(&region)
        .build()
        .map_err(|e| ConfigError::S3ObjectStoreRegistrationFailed {
            name: source_name.to_string(),
            error: format!(
                "Failed to build S3 object store for bucket '{}': {}",
                bucket, e
            ),
        })?;
    Ok((Arc::new(store), region))
}

#[async_trait::async_trait]
impl RemoteStorage for S3Storage {
    /// Determine if a path is an S3 path
    fn is_remote_path(&self, path: &PathBuf) -> bool {
        path.to_str()
            .map(|s| s.starts_with("s3://"))
            .unwrap_or(false)
    }

    /// Validate S3 configuration for remote data sources
    fn validate_configuration(&self, source: &DataSource) -> Result<()> {
        let path_str = source.path.to_str().unwrap_or("");

        // Validate S3 path format
        if !path_str.starts_with("s3://") {
            return Err(ConfigError::InvalidS3Path {
                path: path_str.to_string(),
            }
            .into());
        }

        // Security check: Reject credentials in configuration file if options exist
        Self::reject_credential_options(source)?;

        tracing::debug!(
            "S3 configuration validated for data source: {}",
            source.name
        );
        Ok(())
    }

    /// Setup S3 object store for DataFusion SessionContext using environment variables
    async fn setup_object_store(
        &self,
        session_ctx: &mut SessionContext,
        source_name: &str,
        s3_path: &str,
    ) -> Result<()> {
        use object_store::aws::AmazonS3Builder;
        use std::sync::Arc;

        tracing::info!(
            "Setting up S3 object store for data source: {}",
            source_name
        );

        // Parse bucket name from S3 path
        let s3_url_parsed = url::Url::parse(s3_path).map_err(|e| ConfigError::InvalidS3Path {
            path: format!("Invalid S3 URL '{}': {}", s3_path, e),
        })?;

        let bucket_name = s3_url_parsed
            .host_str()
            .ok_or_else(|| ConfigError::InvalidS3Path {
                path: format!("No bucket name found in S3 URL: {}", s3_path),
            })?;

        tracing::debug!(
            "Extracted bucket name: {} from S3 path: {}",
            bucket_name,
            s3_path
        );

        let mut builder = AmazonS3Builder::new().with_bucket_name(bucket_name);

        // Get AWS region from environment variables
        let aws_region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .map_err(|_| ConfigError::MissingAwsConfig {
                name: source_name.to_string(),
                field: "AWS_REGION or AWS_DEFAULT_REGION environment variable".to_string(),
            })?;

        tracing::debug!("Using AWS region from environment: {}", aws_region);
        builder = builder.with_region(&aws_region);

        // Credentials must come from environment variables only (security best practice)
        // Check that required environment variables are set
        let aws_access_key = std::env::var("AWS_ACCESS_KEY_ID");
        let aws_secret_key = std::env::var("AWS_SECRET_ACCESS_KEY");
        let aws_profile = std::env::var("AWS_PROFILE");

        tracing::debug!(
            "AWS_ACCESS_KEY_ID: {}",
            if aws_access_key.is_ok() {
                "SET"
            } else {
                "NOT SET"
            }
        );
        tracing::debug!(
            "AWS_SECRET_ACCESS_KEY: {}",
            if aws_secret_key.is_ok() {
                "SET"
            } else {
                "NOT SET"
            }
        );
        tracing::debug!(
            "AWS_PROFILE: {}",
            aws_profile.as_deref().unwrap_or("NOT SET")
        );

        if aws_access_key.is_err() && aws_profile.is_err() {
            return Err(ConfigError::MissingAwsConfig {
                name: source_name.to_string(),
                field: "AWS_ACCESS_KEY_ID environment variable or AWS_PROFILE".to_string(),
            }
            .into());
        }

        if aws_access_key.is_ok() && aws_secret_key.is_err() {
            return Err(ConfigError::MissingAwsConfig {
                name: source_name.to_string(),
                field: "AWS_SECRET_ACCESS_KEY environment variable".to_string(),
            }
            .into());
        }

        // Configure AWS authentication on the builder
        if let (Ok(access_key), Ok(secret_key)) = (&aws_access_key, &aws_secret_key) {
            tracing::info!("Configuring S3 builder with explicit AWS credentials from environment");
            builder = builder
                .with_access_key_id(access_key)
                .with_secret_access_key(secret_key);

            // AWS Session Token (optional, for temporary credentials)
            if let Ok(session_token) = std::env::var("AWS_SESSION_TOKEN") {
                builder = builder.with_token(&session_token);
                tracing::debug!("Using AWS session token from environment variable");
            }
        } else if let Ok(profile_name) = &aws_profile {
            tracing::info!("Configuring S3 builder with AWS profile: {}", profile_name);
            // The object_store crate will automatically use the AWS_PROFILE environment variable
            // and load credentials from ~/.aws/credentials and ~/.aws/config
            // No explicit configuration needed - just ensure AWS_PROFILE is set
            tracing::debug!(
                "AWS profile '{}' will be used for authentication",
                profile_name
            );
        }

        tracing::info!(
            "AWS authentication configured for S3 data source: {}",
            source_name
        );

        // Build S3 object store with bucket name
        let s3_store: Arc<dyn object_store::ObjectStore> =
            Arc::new(builder.build().map_err(|e| {
                ConfigError::S3ObjectStoreRegistrationFailed {
                    name: source_name.to_string(),
                    error: format!(
                        "Failed to build S3 object store for bucket '{}': {}",
                        bucket_name, e
                    ),
                }
            })?);

        // Test S3 connectivity before registering
        self.test_connectivity(&s3_store, source_name, s3_path, &aws_region)
            .await?;

        // Register S3 object store with DataFusion for the s3:// scheme
        // Use a dummy URL with the S3 scheme - DataFusion will use this for all s3:// URLs
        let s3_scheme = url::Url::parse(&format!("s3://{}/", bucket_name)).map_err(|e| {
            ConfigError::S3ObjectStoreRegistrationFailed {
                name: source_name.to_string(),
                error: format!("Failed to parse S3 scheme URL: {}", e),
            }
        })?;

        tracing::info!("Registering S3 object store with scheme URL: {}", s3_scheme);

        session_ctx
            .runtime_env()
            .register_object_store(&s3_scheme, s3_store.clone());

        tracing::info!(
            "✓ S3 object store registered and verified for data source: {}",
            source_name
        );
        Ok(())
    }

    /// Test S3 connectivity by attempting to access the specified path
    async fn test_connectivity(
        &self,
        s3_store: &Arc<dyn object_store::ObjectStore>,
        source_name: &str,
        s3_path: &str,
        aws_region: &str,
    ) -> Result<()> {
        use object_store::path::Path as ObjectPath;

        tracing::debug!(
            "Testing S3 connectivity for '{}' at path: {}",
            source_name,
            s3_path
        );

        // Parse S3 URL to extract bucket and object path
        let s3_url = url::Url::parse(s3_path).map_err(|e| ConfigError::InvalidS3Path {
            path: format!("Invalid S3 URL '{}': {}", s3_path, e),
        })?;

        let bucket_name = s3_url
            .host_str()
            .ok_or_else(|| ConfigError::InvalidS3Path {
                path: format!("No bucket name found in S3 URL: {}", s3_path),
            })?;

        // Extract object path (remove leading slash)
        let object_path_str = s3_url.path().trim_start_matches('/');
        let object_store_path = ObjectPath::from(object_path_str);

        tracing::debug!(
            "S3 connectivity test - Bucket: {}, Object: {}",
            bucket_name,
            object_path_str
        );

        // Test connectivity by attempting to get object metadata
        match s3_store.head(&object_store_path).await {
            Ok(metadata) => {
                tracing::info!(
                    "✅ S3 connectivity verified for '{}' - Object size: {} bytes",
                    source_name,
                    metadata.size
                );
                tracing::debug!(
                    "S3 object metadata - Last modified: {:?}, E-tag: {:?}",
                    metadata.last_modified,
                    metadata.e_tag
                );
            }
            Err(object_store::Error::NotFound { .. }) => {
                return Err(ConfigError::S3ObjectStoreRegistrationFailed {
                    name: source_name.to_string(),
                    error: format!(
                        "S3 object not found: '{}'\n\
                         Please verify:\n\
                         1. The S3 path '{}' exists\n\
                         2. Your AWS credentials have s3:GetObject permissions\n\
                         3. The bucket '{}' is accessible from your current AWS region '{}'",
                        s3_path, s3_path, bucket_name, aws_region
                    ),
                }
                .into());
            }
            Err(e) => {
                return Err(ConfigError::S3ObjectStoreRegistrationFailed {
                    name: source_name.to_string(),
                    error: format!(
                        "S3 connectivity test failed: {}\n\
                         Please verify:\n\
                         1. AWS credentials are correctly configured\n\
                         2. AWS region '{}' is correct\n\
                         3. S3 path '{}' exists and is accessible\n\
                         4. IAM permissions allow s3:GetObject and s3:HeadObject on the bucket/object",
                        e, aws_region, s3_path
                    ),
                }.into());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DataSourceType;
    use serde_yaml;

    #[test]
    fn test_s3_path_detection() {
        let s3_storage = S3Storage::new();

        // Test S3 paths
        assert!(s3_storage.is_remote_path(&PathBuf::from("s3://bucket/file.csv")));
        assert!(s3_storage.is_remote_path(&PathBuf::from("s3://my-bucket/folder/file.parquet")));

        // Test non-S3 paths
        assert!(!s3_storage.is_remote_path(&PathBuf::from("data/file.csv")));
        assert!(!s3_storage.is_remote_path(&PathBuf::from("/path/to/file.csv")));
        assert!(!s3_storage.is_remote_path(&PathBuf::from("file://path/to/file.csv")));
    }

    #[test]
    fn test_s3_configuration_validation_clean() {
        let s3_storage = S3Storage::new();

        // Test that S3 paths are correctly detected and validated
        let yaml_content = r#"
name: "s3_test"
type: "parquet"
path: "s3://bucket/file.parquet"
options:
  has_header: true
  delimiter: ","
"#;

        let data_source: DataSource = serde_yaml::from_str(yaml_content).unwrap();

        assert_eq!(data_source.name, "s3_test");
        assert!(matches!(data_source.source_type, DataSourceType::Parquet));
        assert!(s3_storage.is_remote_path(&data_source.path)); // Should be detected as S3 path
        assert_eq!(data_source.path, PathBuf::from("s3://bucket/file.parquet"));
        assert!(data_source.options.is_some());

        // Validate S3 configuration should pass (no forbidden AWS keys in config)
        let result = s3_storage.validate_configuration(&data_source);
        assert!(
            result.is_ok(),
            "S3 configuration validation should pass for clean config"
        );

        // Verify options don't contain AWS configuration
        let options = data_source.options.unwrap();
        assert!(!options.contains_key("aws_region"));
        assert!(!options.contains_key("aws_access_key_id"));
        assert!(!options.contains_key("aws_secret_access_key"));
    }

    #[test]
    fn test_s3_validation_rejects_credentials() {
        let s3_storage = S3Storage::new();

        // Test that S3 validation rejects configurations with credentials
        let yaml_content = r#"
name: "invalid_s3_test"
type: "parquet"
path: "s3://bucket/file.parquet"
options:
  aws_access_key_id: "should_be_rejected"
"#;

        let data_source: DataSource = serde_yaml::from_str(yaml_content).unwrap();
        let result = s3_storage.validate_configuration(&data_source);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("AWS configuration"));
        assert!(
            error
                .to_string()
                .contains("must not be stored in configuration files")
        );
    }

    #[test]
    fn test_s3_validation_rejects_region_in_config() {
        let s3_storage = S3Storage::new();

        // Test that S3 validation rejects aws_region in configuration
        let yaml_content = r#"
name: "invalid_s3_test"
type: "parquet"
path: "s3://bucket/file.parquet"
options:
  aws_region: "us-east-1"
"#;

        let data_source: DataSource = serde_yaml::from_str(yaml_content).unwrap();
        let result = s3_storage.validate_configuration(&data_source);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("AWS configuration"));
        assert!(error.to_string().contains("aws_region"));
        assert!(error.to_string().contains("environment variables"));
    }

    fn documents_source(yaml: &str) -> DataSource {
        serde_yaml::from_str(yaml).unwrap()
    }

    #[test]
    fn s3_key_prefix_extracts_path_without_leading_slash() {
        assert_eq!(s3_key_prefix("s3://bucket/in/corpus"), "in/corpus");
        assert_eq!(s3_key_prefix("s3://bucket/in/corpus/"), "in/corpus/");
        assert_eq!(s3_key_prefix("s3://bucket"), "");
        assert_eq!(s3_key_prefix("s3://bucket/"), "");
    }

    #[test]
    fn parse_bucket_extracts_bucket_or_errors() {
        assert_eq!(
            parse_bucket("s3://my-bucket/key", "src").unwrap(),
            "my-bucket"
        );
        assert_eq!(parse_bucket("s3://my-bucket", "src").unwrap(), "my-bucket");
        // No host → bucketless → error.
        assert!(parse_bucket("s3:///key-only", "src").is_err());
    }

    #[test]
    fn write_probe_key_scopes_under_prefix_or_bucket_root() {
        // Empty prefix probes the bucket root.
        assert_eq!(write_probe_key(""), ".skardi-write-probe");
        // A prefix (with or without trailing slash) sits the probe directly
        // under it, never doubling the separator.
        assert_eq!(write_probe_key("crops"), "crops/.skardi-write-probe");
        assert_eq!(
            write_probe_key("out/crops/"),
            "out/crops/.skardi-write-probe"
        );
    }

    #[test]
    fn require_s3_region_and_creds_branches() {
        // Missing region → error names the region env vars.
        let err = require_s3_region_and_creds(None, true, false, "src").unwrap_err();
        assert!(err.to_string().contains("AWS_REGION"), "unexpected: {err}");

        // Region present but neither access key nor profile → credential error.
        let err =
            require_s3_region_and_creds(Some("us-east-1".into()), false, false, "src").unwrap_err();
        assert!(
            err.to_string().contains("AWS_ACCESS_KEY_ID"),
            "unexpected: {err}"
        );

        // Region + access key, or region + profile → ok, returns the region.
        assert_eq!(
            require_s3_region_and_creds(Some("us-east-1".into()), true, false, "src").unwrap(),
            "us-east-1"
        );
        assert_eq!(
            require_s3_region_and_creds(Some("eu-west-2".into()), false, true, "src").unwrap(),
            "eu-west-2"
        );
    }

    #[test]
    fn documents_fully_local_needs_no_s3_validation() {
        let s3 = S3Storage::new();
        let src = documents_source(
            r#"
name: "docs"
type: "documents"
path: "/data/corpus"
"#,
        );
        assert!(
            s3.validate_documents_configuration("/data/corpus", Some("/data/crops"), &src)
                .is_ok()
        );
    }

    #[test]
    fn documents_same_bucket_ok_different_bucket_rejected() {
        let s3 = S3Storage::new();
        let src = documents_source(
            r#"
name: "docs"
type: "documents"
path: "s3://corpus-bucket/in/"
"#,
        );
        // Same bucket for path + image_store → ok.
        assert!(
            s3.validate_documents_configuration(
                "s3://corpus-bucket/in/",
                Some("s3://corpus-bucket/crops/"),
                &src
            )
            .is_ok()
        );
        // Different buckets → rejected with a clear message.
        let err = s3
            .validate_documents_configuration(
                "s3://corpus-bucket/in/",
                Some("s3://other-bucket/crops/"),
                &src,
            )
            .unwrap_err();
        assert!(err.to_string().contains("same bucket"), "unexpected: {err}");
    }

    #[test]
    fn documents_rejects_image_store_equal_or_ancestor_of_path() {
        let s3 = S3Storage::new();
        let src = documents_source(
            r#"
name: "docs"
type: "documents"
path: "/data/corpus"
"#,
        );

        // Local: image_store == path → rejected (all docs excluded → empty scan).
        let err = s3
            .validate_documents_configuration("/data/corpus", Some("/data/corpus"), &src)
            .unwrap_err();
        assert!(err.to_string().contains("ancestor"), "unexpected: {err}");
        // Local: image_store is an ancestor of path → rejected.
        let err = s3
            .validate_documents_configuration("/data/corpus", Some("/data"), &src)
            .unwrap_err();
        assert!(err.to_string().contains("ancestor"), "unexpected: {err}");
        // Local: image_store nested inside path → allowed (self-ingestion guard).
        assert!(
            s3.validate_documents_configuration("/data/corpus", Some("/data/corpus/crops"), &src)
                .is_ok()
        );
        // A sibling that shares a name prefix but not a path segment is disjoint.
        assert!(
            s3.validate_documents_configuration("/data/corpus", Some("/data/corpus-2"), &src)
                .is_ok()
        );

        // S3: image_store == path → rejected.
        let err = s3
            .validate_documents_configuration(
                "s3://bucket/corpus/",
                Some("s3://bucket/corpus/"),
                &src,
            )
            .unwrap_err();
        assert!(err.to_string().contains("ancestor"), "unexpected: {err}");
        // S3: image_store is the bucket root (ancestor of every key) → rejected.
        let err = s3
            .validate_documents_configuration("s3://bucket/corpus/", Some("s3://bucket"), &src)
            .unwrap_err();
        assert!(err.to_string().contains("ancestor"), "unexpected: {err}");
        // S3: image_store nested inside path → allowed.
        assert!(
            s3.validate_documents_configuration(
                "s3://bucket/corpus/",
                Some("s3://bucket/corpus/crops/"),
                &src
            )
            .is_ok()
        );
    }

    #[test]
    fn documents_rejects_credentials_when_only_image_store_is_s3() {
        // The gap-closer: local `path` + s3 `image_store` must still reject
        // aws_* keys in options (validate_configuration alone would skip this).
        let s3 = S3Storage::new();
        let src = documents_source(
            r#"
name: "docs"
type: "documents"
path: "/data/corpus"
options:
  image_store: "s3://crops-bucket/out/"
  aws_secret_access_key: "should_be_rejected"
"#,
        );
        let err = s3
            .validate_documents_configuration("/data/corpus", Some("s3://crops-bucket/out/"), &src)
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("must not be stored in configuration files"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn documents_rejects_credentials_when_path_is_s3() {
        let s3 = S3Storage::new();
        let src = documents_source(
            r#"
name: "docs"
type: "documents"
path: "s3://corpus-bucket/in/"
options:
  aws_access_key_id: "nope"
"#,
        );
        let err = s3
            .validate_documents_configuration("s3://corpus-bucket/in/", None, &src)
            .unwrap_err();
        assert!(
            err.to_string().contains("AWS configuration"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn test_s3_path_validation_clean_config() {
        let s3_storage = S3Storage::new();

        // Test that S3 path validation works without AWS configuration checks
        let yaml_content = r#"
name: "s3_path_test"
type: "csv"
path: "s3://valid-bucket/file.csv"
"#;

        let data_source: DataSource = serde_yaml::from_str(yaml_content).unwrap();

        // This should pass S3 path validation (just checking the s3:// prefix)
        // Note: This doesn't test AWS connectivity, just configuration validation
        assert!(s3_storage.is_remote_path(&data_source.path));
        assert_eq!(
            data_source.path.to_str().unwrap(),
            "s3://valid-bucket/file.csv"
        );

        // Validate S3 configuration should pass (no forbidden AWS keys in config)
        let result = s3_storage.validate_configuration(&data_source);
        assert!(
            result.is_ok(),
            "S3 configuration validation should pass for clean config"
        );
    }
}
