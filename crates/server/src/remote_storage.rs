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
        if let Some(options) = &source.options {
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
        }

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
        assert!(error
            .to_string()
            .contains("must not be stored in configuration files"));
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
