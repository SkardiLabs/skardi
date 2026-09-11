use serde::{Deserialize, Serialize};
use std::fmt;

/// Supported data source types across the Skardi provider layer.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Hash, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum DataSourceType {
    Csv,
    Parquet,
    Postgres,
    Mysql,
    Sqlite,
    Iceberg,
    Mongo,
    Redis,
    Lance,
    Seekdb,
    Influxdb,
    Clickhouse,
    Documents,
    Dynamodb,
    Rss,
    /// Obsidian vault exposed as `notes` / `links` / `tags` (read-only, catalog-level).
    Obsidian,
    Graph,
    // Explicit rename: the `lowercase` rule would produce `openconnector`,
    // but the public YAML spelling (and the design spec) is `open_connector`.
    #[serde(rename = "open_connector")]
    OpenConnector,
}

impl DataSourceType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Csv => "csv",
            Self::Parquet => "parquet",
            Self::Postgres => "postgres",
            Self::Mysql => "mysql",
            Self::Sqlite => "sqlite",
            Self::Iceberg => "iceberg",
            Self::Mongo => "mongo",
            Self::Redis => "redis",
            Self::Lance => "lance",
            Self::Seekdb => "seekdb",
            Self::Influxdb => "influxdb",
            Self::Clickhouse => "clickhouse",
            Self::Documents => "documents",
            Self::Dynamodb => "dynamodb",
            Self::Rss => "rss",
            Self::Obsidian => "obsidian",
            Self::Graph => "graph",
            Self::OpenConnector => "open_connector",
        }
    }
}

impl fmt::Display for DataSourceType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn documents_variant_roundtrips() {
        let t: DataSourceType = serde_yaml::from_str("documents").unwrap();
        assert_eq!(t, DataSourceType::Documents);
        assert_eq!(t.as_str(), "documents");
    }

    #[test]
    fn clickhouse_variant_roundtrips() {
        let t: DataSourceType = serde_yaml::from_str("clickhouse").unwrap();
        assert_eq!(t, DataSourceType::Clickhouse);
        assert_eq!(t.as_str(), "clickhouse");
    }

    #[test]
    fn dynamodb_variant_roundtrips() {
        let t: DataSourceType = serde_yaml::from_str("dynamodb").unwrap();
        assert_eq!(t, DataSourceType::Dynamodb);
        assert_eq!(t.as_str(), "dynamodb");
    }

    #[test]
    fn rss_variant_roundtrips() {
        let t: DataSourceType = serde_yaml::from_str("rss").unwrap();
        assert_eq!(t, DataSourceType::Rss);
        assert_eq!(t.as_str(), "rss");
    }

    #[test]
    fn obsidian_variant_roundtrips() {
        let t: DataSourceType = serde_yaml::from_str("obsidian").unwrap();
        assert_eq!(t, DataSourceType::Obsidian);
        assert_eq!(t.as_str(), "obsidian");
    }

    /// The JSON spelling is load-bearing too: `/data_source` serialises the
    /// type, so pin both directions rather than only the YAML config path.
    #[test]
    fn obsidian_roundtrips_as_lowercase_json_string() {
        let json = serde_json::to_string(&DataSourceType::Obsidian).unwrap();
        assert_eq!(json, "\"obsidian\"");
        let back: DataSourceType = serde_json::from_str("\"obsidian\"").unwrap();
        assert_eq!(back, DataSourceType::Obsidian);
    }

    #[test]
    fn open_connector_variant_roundtrips() {
        let t: DataSourceType = serde_yaml::from_str("open_connector").unwrap();
        assert_eq!(t, DataSourceType::OpenConnector);
        assert_eq!(t.as_str(), "open_connector");
    }

    #[test]
    fn graph_variant_roundtrips() {
        let t: DataSourceType = serde_yaml::from_str("graph").unwrap();
        assert_eq!(t, DataSourceType::Graph);
        assert_eq!(t.as_str(), "graph");
    }
}
