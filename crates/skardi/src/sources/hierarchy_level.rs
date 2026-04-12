use serde::{Deserialize, Serialize};

/// How much of an upstream database to expose in DataFusion (single table vs whole catalog).
#[derive(Debug, Clone, Copy, Deserialize, Serialize, Hash, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum HierarchyLevel {
    /// One table under the default catalog.
    #[default]
    Table,
    /// Whole database as a named catalog (schemas + tables).
    Catalog,
}

impl HierarchyLevel {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Table => "table",
            Self::Catalog => "catalog",
        }
    }
}
