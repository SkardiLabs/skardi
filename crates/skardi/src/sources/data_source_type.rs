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
    Documents,
    Dynamodb,
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
            Self::Documents => "documents",
            Self::Dynamodb => "dynamodb",
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
    fn dynamodb_variant_roundtrips() {
        let t: DataSourceType = serde_yaml::from_str("dynamodb").unwrap();
        assert_eq!(t, DataSourceType::Dynamodb);
        assert_eq!(t.as_str(), "dynamodb");
    }

    #[test]
    fn open_connector_variant_roundtrips() {
        let t: DataSourceType = serde_yaml::from_str("open_connector").unwrap();
        assert_eq!(t, DataSourceType::OpenConnector);
        assert_eq!(t.as_str(), "open_connector");
    }
}
