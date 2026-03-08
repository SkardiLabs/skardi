//! Access mode types for data sources

use serde::{Deserialize, Serialize};

/// Access mode for data sources
#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    #[default]
    ReadOnly,
    ReadWrite,
}

impl AccessMode {
    /// Returns true if the access mode allows write operations
    pub fn is_read_write(&self) -> bool {
        matches!(self, AccessMode::ReadWrite)
    }

    /// Returns true if the access mode is read-only
    pub fn is_read_only(&self) -> bool {
        matches!(self, AccessMode::ReadOnly)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_mode_default() {
        let mode: AccessMode = Default::default();
        assert_eq!(mode, AccessMode::ReadOnly);
        assert!(mode.is_read_only());
        assert!(!mode.is_read_write());
    }

    #[test]
    fn test_access_mode_read_write() {
        let mode = AccessMode::ReadWrite;
        assert!(mode.is_read_write());
        assert!(!mode.is_read_only());
    }

    #[test]
    fn test_access_mode_serde() {
        // Test deserialization
        let read_only: AccessMode = serde_json::from_str(r#""read_only""#).unwrap();
        assert_eq!(read_only, AccessMode::ReadOnly);

        let read_write: AccessMode = serde_json::from_str(r#""read_write""#).unwrap();
        assert_eq!(read_write, AccessMode::ReadWrite);

        // Test serialization
        assert_eq!(
            serde_json::to_string(&AccessMode::ReadOnly).unwrap(),
            r#""read_only""#
        );
        assert_eq!(
            serde_json::to_string(&AccessMode::ReadWrite).unwrap(),
            r#""read_write""#
        );
    }
}
