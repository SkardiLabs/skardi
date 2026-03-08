//! Apache Iceberg table provider for DataFusion
//!
//! This module provides integration with Apache Iceberg tables using the iceberg-rust library.
//! It supports reading Iceberg tables from filesystem-based warehouses.

use anyhow::{Context, Result};
use datafusion::prelude::SessionContext;
use iceberg::NamespaceIdent;
use iceberg::TableIdent;
use iceberg::io::FileIO;
use iceberg::table::Table;
use iceberg_datafusion::IcebergStaticTableProvider;
use std::collections::HashMap;
use std::sync::Arc;

/// Register an Iceberg table with DataFusion SessionContext
///
/// The table is registered directly in the default catalog, allowing simple queries like:
/// ```sql
/// SELECT * FROM my_iceberg_table
/// ```
///
/// This also enables easy joins with other data sources:
/// ```sql
/// SELECT * FROM my_iceberg_table JOIN my_csv_table ON ...
/// ```
///
/// # Arguments
/// * `session_ctx` - DataFusion session context to register the table into
/// * `name` - Name to register the table as in DataFusion (used directly in queries)
/// * `warehouse_path` - Path to the Iceberg warehouse (e.g., "file:///path/to/warehouse" or "s3://bucket/warehouse")
/// * `options` - Configuration options for the Iceberg table
///
/// # Options
/// * `namespace` - Iceberg namespace/database (required)
/// * `table` - Iceberg table name (required)
/// * `aws_region` - AWS region for S3 (optional, for S3 warehouses)
pub async fn register_iceberg_table(
    session_ctx: &mut SessionContext,
    name: &str,
    warehouse_path: &str,
    options: Option<&HashMap<String, String>>,
) -> Result<()> {
    tracing::info!(
        "Registering Iceberg table: {} from warehouse: {}",
        name,
        warehouse_path
    );

    let opts = options.ok_or_else(|| {
        anyhow::anyhow!(
            "Iceberg data source '{}' requires options (namespace, table)",
            name
        )
    })?;

    let namespace = opts.get("namespace").ok_or_else(|| {
        anyhow::anyhow!("Iceberg data source '{}' requires 'namespace' option", name)
    })?;

    let table_name = opts
        .get("table")
        .ok_or_else(|| anyhow::anyhow!("Iceberg data source '{}' requires 'table' option", name))?;

    tracing::debug!(
        "Iceberg config: namespace={}, table={}",
        namespace,
        table_name
    );

    let file_io = build_file_io(warehouse_path, opts).await?;
    let table = load_filesystem_table(&file_io, warehouse_path, namespace, table_name).await?;

    let table_provider = IcebergStaticTableProvider::try_new_from_table(table)
        .await
        .map_err(|e| anyhow::anyhow!("Failed to create Iceberg table provider: {}", e))?;

    session_ctx
        .register_table(name, Arc::new(table_provider))
        .with_context(|| format!("Failed to register Iceberg table '{}'", name))?;

    tracing::info!(
        "Successfully registered Iceberg table '{}' (from {}.{})",
        name,
        namespace,
        table_name
    );

    Ok(())
}

async fn load_filesystem_table(
    file_io: &FileIO,
    warehouse_path: &str,
    namespace: &str,
    table_name: &str,
) -> Result<Table> {
    use iceberg::spec::TableMetadata;

    let table_path = format!("{}/{}/{}", warehouse_path, namespace, table_name);
    let metadata_location = find_latest_metadata(file_io, &table_path).await?;

    tracing::debug!("Loading Iceberg metadata from: {}", metadata_location);

    let metadata_content = file_io
        .new_input(&metadata_location)
        .with_context(|| format!("Failed to create input for metadata: {}", metadata_location))?
        .read()
        .await
        .with_context(|| format!("Failed to read metadata file: {}", metadata_location))?;

    let metadata: TableMetadata = serde_json::from_slice(&metadata_content)
        .with_context(|| format!("Failed to parse Iceberg metadata: {}", metadata_location))?;

    let namespace_ident = NamespaceIdent::new(namespace.to_string());
    let table_ident = TableIdent::new(namespace_ident, table_name.to_string());

    Ok(Table::builder()
        .file_io(file_io.clone())
        .metadata_location(metadata_location)
        .metadata(metadata)
        .identifier(table_ident)
        .build()
        .with_context(|| "Failed to build Iceberg table")?)
}

async fn build_file_io(warehouse_path: &str, options: &HashMap<String, String>) -> Result<FileIO> {
    let mut props = HashMap::new();

    // TODO: Add support for other cloud providers, right now only S3 is supported.
    if warehouse_path.starts_with("s3://") || warehouse_path.starts_with("s3a://") {
        if let Some(region) = options.get("aws_region") {
            props.insert("s3.region".to_string(), region.clone());
        } else if let Ok(region) = std::env::var("AWS_REGION") {
            props.insert("s3.region".to_string(), region);
        } else if let Ok(region) = std::env::var("AWS_DEFAULT_REGION") {
            props.insert("s3.region".to_string(), region);
        }

        if let Some(access_key) = options.get("aws_access_key_id") {
            props.insert("s3.access-key-id".to_string(), access_key.clone());
        }

        if let Some(secret_key) = options.get("aws_secret_access_key") {
            props.insert("s3.secret-access-key".to_string(), secret_key.clone());
        }

        if let Some(endpoint) = options.get("s3_endpoint") {
            props.insert("s3.endpoint".to_string(), endpoint.clone());
        }
    }

    FileIO::from_path(warehouse_path)
        .with_context(|| format!("Failed to determine FileIO scheme for: {}", warehouse_path))?
        .with_props(props)
        .build()
        .with_context(|| format!("Failed to build FileIO for warehouse: {}", warehouse_path))
}

async fn find_latest_metadata(file_io: &FileIO, table_path: &str) -> Result<String> {
    let metadata_dir = format!("{}/metadata", table_path);

    let version_hint_path = format!("{}/version-hint.text", metadata_dir);
    if let Ok(input) = file_io.new_input(&version_hint_path) {
        if let Ok(content) = input.read().await {
            if let Ok(version_str) = String::from_utf8(content.to_vec()) {
                let version: i32 = version_str.trim().parse().unwrap_or(1);
                let metadata_path = format!("{}/v{}.metadata.json", metadata_dir, version);
                tracing::debug!("Found version hint pointing to: {}", metadata_path);
                return Ok(metadata_path);
            }
        }
    }

    for version in (1..=100).rev() {
        let metadata_path = format!("{}/v{}.metadata.json", metadata_dir, version);
        if let Ok(input) = file_io.new_input(&metadata_path) {
            if input.read().await.is_ok() {
                tracing::debug!("Found Spark-style metadata file: {}", metadata_path);
                return Ok(metadata_path);
            }
        }
    }

    // Try PyIceberg-style patterns (00000-{uuid}.metadata.json, 00001-{uuid}.metadata.json, etc.)
    // These are sequence-based with UUIDs. We scan for the highest sequence number.
    if let Ok(latest) = find_pyiceberg_metadata(&metadata_dir).await {
        tracing::debug!("Found PyIceberg-style metadata file: {}", latest);
        return Ok(latest);
    }

    Err(anyhow::anyhow!(
        "Could not find Iceberg metadata file in {}. Ensure the table exists and has valid metadata.",
        metadata_dir
    ))
}

async fn find_pyiceberg_metadata(metadata_dir: &str) -> Result<String> {
    use std::fs;
    use std::path::Path;

    let local_path = metadata_dir.strip_prefix("file://").unwrap_or(metadata_dir);

    let path = Path::new(local_path);
    if !path.exists() {
        return Err(anyhow::anyhow!("Metadata directory does not exist"));
    }

    let mut metadata_files: Vec<(i32, String)> = Vec::new();

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_name = entry.file_name().to_string_lossy().to_string();

        // Match pattern: {sequence}-{uuid}.metadata.json (e.g., 00001-abc123.metadata.json)
        if file_name.ends_with(".metadata.json") && file_name.contains('-') {
            if let Some(seq_str) = file_name.split('-').next() {
                if let Ok(seq) = seq_str.parse::<i32>() {
                    let full_path = format!("{}/{}", metadata_dir, file_name);
                    metadata_files.push((seq, full_path));
                }
            }
        }
    }

    metadata_files.sort_by(|a, b| b.0.cmp(&a.0));

    metadata_files
        .into_iter()
        .next()
        .map(|(_, path)| path)
        .ok_or_else(|| anyhow::anyhow!("No metadata.json files found in {}", metadata_dir))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_missing_options() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut session_ctx = SessionContext::new();
            let result = register_iceberg_table(
                &mut session_ctx,
                "test_iceberg",
                "file:///tmp/warehouse",
                None,
            )
            .await;

            assert!(result.is_err());
            assert!(result.unwrap_err().to_string().contains("requires options"));
        });
    }

    #[test]
    fn test_missing_namespace_option() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut session_ctx = SessionContext::new();
            let mut options = HashMap::new();
            options.insert("table".to_string(), "test_table".to_string());

            let result = register_iceberg_table(
                &mut session_ctx,
                "test_iceberg",
                "file:///tmp/warehouse",
                Some(&options),
            )
            .await;

            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("requires 'namespace' option")
            );
        });
    }

    #[test]
    fn test_missing_table_option() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut session_ctx = SessionContext::new();
            let mut options = HashMap::new();
            options.insert("namespace".to_string(), "default".to_string());

            let result = register_iceberg_table(
                &mut session_ctx,
                "test_iceberg",
                "file:///tmp/warehouse",
                Some(&options),
            )
            .await;

            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("requires 'table' option")
            );
        });
    }

    #[test]
    fn test_build_file_io_local_path() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let options = HashMap::new();
            let result = build_file_io("file:///tmp/warehouse", &options).await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_build_file_io_s3_path_with_region() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut options = HashMap::new();
            options.insert("aws_region".to_string(), "us-west-2".to_string());
            let result = build_file_io("s3://my-bucket/warehouse", &options).await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_build_file_io_s3a_path() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut options = HashMap::new();
            options.insert("aws_region".to_string(), "eu-west-1".to_string());
            let result = build_file_io("s3a://my-bucket/warehouse", &options).await;
            assert!(result.is_ok());
        });
    }

    #[test]
    fn test_find_pyiceberg_metadata_empty_dir() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let metadata_dir = format!("file://{}", temp_dir.path().display());
            let result = find_pyiceberg_metadata(&metadata_dir).await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_find_pyiceberg_metadata_nonexistent_dir() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let result = find_pyiceberg_metadata("/nonexistent/path/metadata").await;
            assert!(result.is_err());
        });
    }

    #[test]
    fn test_find_pyiceberg_metadata_single_file() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let metadata_file = temp_dir.path().join("00001-abc123-def456.metadata.json");
            fs::write(&metadata_file, "{}").unwrap();

            let metadata_dir = format!("file://{}", temp_dir.path().display());
            let result = find_pyiceberg_metadata(&metadata_dir).await;
            assert!(result.is_ok());
            assert!(
                result
                    .unwrap()
                    .contains("00001-abc123-def456.metadata.json")
            );
        });
    }

    #[test]
    fn test_find_pyiceberg_metadata_multiple_files_returns_latest() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            fs::write(temp_dir.path().join("00001-uuid1.metadata.json"), "{}").unwrap();
            fs::write(temp_dir.path().join("00002-uuid2.metadata.json"), "{}").unwrap();
            fs::write(temp_dir.path().join("00003-uuid3.metadata.json"), "{}").unwrap();

            let metadata_dir = format!("file://{}", temp_dir.path().display());
            let result = find_pyiceberg_metadata(&metadata_dir).await;
            assert!(result.is_ok());
            let path = result.unwrap();
            assert!(path.contains("00003-uuid3.metadata.json"));
        });
    }

    #[test]
    fn test_find_pyiceberg_metadata_ignores_non_metadata_files() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            fs::write(temp_dir.path().join("00001-uuid1.metadata.json"), "{}").unwrap();
            fs::write(temp_dir.path().join("readme.txt"), "test").unwrap();
            fs::write(temp_dir.path().join("other.json"), "{}").unwrap();
            fs::write(temp_dir.path().join("snapshot-123.avro"), "binary").unwrap();

            let metadata_dir = format!("file://{}", temp_dir.path().display());
            let result = find_pyiceberg_metadata(&metadata_dir).await;
            assert!(result.is_ok());
            assert!(result.unwrap().contains("00001-uuid1.metadata.json"));
        });
    }

    #[test]
    fn test_find_latest_metadata_nonexistent_table() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let file_io = FileIO::from_path(&format!("file://{}", temp_dir.path().display()))
                .unwrap()
                .build()
                .unwrap();

            let table_path = format!("file://{}/ns/table", temp_dir.path().display());
            let result = find_latest_metadata(&file_io, &table_path).await;
            assert!(result.is_err());
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("Could not find Iceberg metadata")
            );
        });
    }

    #[test]
    fn test_find_latest_metadata_with_version_hint() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let metadata_dir = temp_dir.path().join("metadata");
            fs::create_dir_all(&metadata_dir).unwrap();

            fs::write(metadata_dir.join("version-hint.text"), "2").unwrap();
            fs::write(metadata_dir.join("v1.metadata.json"), "{}").unwrap();
            fs::write(metadata_dir.join("v2.metadata.json"), "{}").unwrap();

            let file_io = FileIO::from_path(&format!("file://{}", temp_dir.path().display()))
                .unwrap()
                .build()
                .unwrap();

            let table_path = format!("file://{}", temp_dir.path().display());
            let result = find_latest_metadata(&file_io, &table_path).await;
            assert!(result.is_ok());
            assert!(result.unwrap().contains("v2.metadata.json"));
        });
    }

    #[test]
    fn test_find_latest_metadata_spark_style() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let temp_dir = TempDir::new().unwrap();
            let metadata_dir = temp_dir.path().join("metadata");
            fs::create_dir_all(&metadata_dir).unwrap();

            fs::write(metadata_dir.join("v1.metadata.json"), "{}").unwrap();
            fs::write(metadata_dir.join("v2.metadata.json"), "{}").unwrap();
            fs::write(metadata_dir.join("v3.metadata.json"), "{}").unwrap();

            let file_io = FileIO::from_path(&format!("file://{}", temp_dir.path().display()))
                .unwrap()
                .build()
                .unwrap();

            let table_path = format!("file://{}", temp_dir.path().display());
            let result = find_latest_metadata(&file_io, &table_path).await;
            assert!(result.is_ok());
            assert!(result.unwrap().contains("v3.metadata.json"));
        });
    }

    #[test]
    fn test_register_with_nonexistent_warehouse() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut session_ctx = SessionContext::new();
            let mut options = HashMap::new();
            options.insert("namespace".to_string(), "default".to_string());
            options.insert("table".to_string(), "test_table".to_string());

            let result = register_iceberg_table(
                &mut session_ctx,
                "test_iceberg",
                "file:///nonexistent/warehouse/path",
                Some(&options),
            )
            .await;

            assert!(result.is_err());
        });
    }
}
