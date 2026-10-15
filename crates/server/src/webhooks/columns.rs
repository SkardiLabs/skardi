//! The `columns:` grammar of a webhook route.
//!
//! This is the source-pack column grammar, spelled the same way
//! (`{ name, path, type, nullable }` with the same `type` names), so a user
//! who has written a pack table writes a webhook route without learning a
//! second vocabulary. The pack loader's own deserializer is private to the
//! engine crate, so the grammar is restated here and mapped onto the shared
//! [`ColumnSpec`], which the engine's converter consumes directly.
//!
//! `utf8_list_from_object_key` is deliberately absent: it needs a leaked
//! `&'static str` key and no webhook row has used it. Add it when one does.

use serde::Deserialize;
use skardi::sources::providers::open_connector::json_to_arrow::{ColumnSpec, FieldType};

/// One `columns:` entry as written in YAML.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnDoc {
    /// Arrow column name.
    pub name: String,
    /// Row-relative dotted path of object keys (`_metadata.phone_number_id`).
    /// No `$.` prefix, no array indexes: that is the pack contract, kept here.
    pub path: String,
    /// Column type, in the pack spelling.
    #[serde(rename = "type")]
    pub column_type: ColumnType,
    /// Whether a missing key or JSON null becomes an Arrow NULL (`true`) or
    /// fails the whole delivery (`false`).
    pub nullable: bool,
}

/// Column types a webhook route can declare; the serde names are the pack's.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    Boolean,
    Int64,
    Uint64,
    Float64,
    Utf8,
    TimestampMsUtc,
    TimestampSUtc,
    TimestampMsStringUtc,
    TimestampSStringUtc,
    TimestampSFracStringUtc,
    Utf8List,
    Json,
}

impl From<ColumnType> for FieldType {
    fn from(value: ColumnType) -> Self {
        match value {
            ColumnType::Boolean => FieldType::Boolean,
            ColumnType::Int64 => FieldType::Int64,
            ColumnType::Uint64 => FieldType::UInt64,
            ColumnType::Float64 => FieldType::Float64,
            ColumnType::Utf8 => FieldType::Utf8,
            ColumnType::TimestampMsUtc => FieldType::TimestampMillisUtc,
            ColumnType::TimestampSUtc => FieldType::TimestampSecondsUtc,
            ColumnType::TimestampMsStringUtc => FieldType::TimestampMillisStringUtc,
            ColumnType::TimestampSStringUtc => FieldType::TimestampSecondsStringUtc,
            ColumnType::TimestampSFracStringUtc => FieldType::TimestampSecondsFractionalStringUtc,
            ColumnType::Utf8List => FieldType::Utf8List,
            ColumnType::Json => FieldType::Json,
        }
    }
}

impl ColumnDoc {
    /// Check the entry and turn it into the converter's column spec.
    ///
    /// `route` and the column name only label the error.
    pub fn into_spec(self, route: &str) -> Result<ColumnSpec, String> {
        if self.name.trim().is_empty() {
            return Err(format!("route '{route}': a column has an empty name"));
        }
        if self.path.is_empty() {
            return Err(format!(
                "route '{route}': column '{}' has an empty path",
                self.name
            ));
        }
        if self.path.starts_with("$.") || self.path == "$" {
            return Err(format!(
                "route '{route}': column '{}' path '{}' must not start with '$' — paths are \
                 row-relative object keys, exactly as in a source pack",
                self.name, self.path
            ));
        }
        if self.path.split('.').any(str::is_empty) {
            return Err(format!(
                "route '{route}': column '{}' path '{}' has an empty segment",
                self.name, self.path
            ));
        }
        Ok(ColumnSpec {
            name: self.name,
            path: self.path,
            field_type: self.column_type.into(),
            nullable: self.nullable,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(yaml: &str) -> ColumnDoc {
        serde_yaml::from_str(yaml).expect("column yaml parses")
    }

    #[test]
    fn pack_spellings_map_onto_the_shared_field_types() {
        let spec = doc(
            "{ name: received_at, path: timestamp, type: timestamp_s_string_utc, nullable: false }",
        )
        .into_spec("messages")
        .unwrap();
        assert_eq!(spec.field_type, FieldType::TimestampSecondsStringUtc);
        assert!(!spec.nullable);
        let spec = doc("{ name: raw, path: _raw, type: json, nullable: false }")
            .into_spec("messages")
            .unwrap();
        assert_eq!(spec.field_type, FieldType::Json);
    }

    #[test]
    fn nullable_is_required_and_unknown_keys_are_refused() {
        assert!(serde_yaml::from_str::<ColumnDoc>("{ name: a, path: a, type: utf8 }").is_err());
        assert!(
            serde_yaml::from_str::<ColumnDoc>(
                "{ name: a, path: a, type: utf8, nullable: true, key: x }"
            )
            .is_err()
        );
        assert!(
            serde_yaml::from_str::<ColumnDoc>("{ name: a, path: a, type: text, nullable: true }")
                .is_err()
        );
    }

    #[test]
    fn dollar_prefixed_and_empty_segment_paths_are_refused() {
        let err = doc("{ name: a, path: $.a, type: utf8, nullable: true }")
            .into_spec("r")
            .unwrap_err();
        assert!(err.contains("must not start with '$'"), "{err}");
        let err = doc("{ name: a, path: a..b, type: utf8, nullable: true }")
            .into_spec("r")
            .unwrap_err();
        assert!(err.contains("empty segment"), "{err}");
    }
}
