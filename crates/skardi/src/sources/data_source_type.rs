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
}
