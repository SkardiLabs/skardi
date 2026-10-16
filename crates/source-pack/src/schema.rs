//! What a provider's rows look like, as a DECLARATION.
//!
//! These types were already neutral when they lived in `json_to_arrow.rs`:
//! [`FieldType`](crate::schema::FieldType)'s variants describe JSON shapes and name Arrow only in their
//! prose. They looked coupled because they sat beside the conversion, not
//! because they carried an Arrow type. Separating them is what lets a syncer
//! read a provider's rows without linking a columnar format it will never
//! build.
//!
//! The conversion itself stays in the engine, where the `RecordBatch` is
//! wanted.
//!
//! The YAML spelling of a column — `{ name, path, type, nullable }` — lives
//! here too, as [`ColumnDoc`] and [`ColumnType`], so that every place that
//! lets a user declare columns (a source pack asset, a webhook route) parses
//! the same grammar from the same definition. A type added here is a type
//! added everywhere, and the one spelling rule worth remembering
//! (`timestamp_s_string_utc` is digits-only) has one home.

use serde::Deserialize;

/// Column type a source-pack field can declare. Deliberately small: it is
/// the contract Skardi maintains, not every Arrow type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FieldType {
    /// JSON boolean ↔ Arrow Boolean.
    Boolean,
    /// JSON integer ↔ Arrow Int64.
    Int64,
    /// JSON non-negative integer ↔ Arrow UInt64.
    UInt64,
    /// JSON number ↔ Arrow Float64.
    Float64,
    /// JSON string ↔ Arrow Utf8.
    Utf8,
    /// RFC 3339 string or epoch-millis number ↔ Arrow Timestamp(Millisecond, UTC).
    TimestampMillisUtc,
    /// JSON integer epoch **seconds** ↔ Arrow Timestamp(Millisecond, UTC).
    /// For Slack-style APIs whose `created` / `updated` fields are whole
    /// seconds — reading those through [`FieldType::TimestampMillisUtc`]
    /// would silently misparse them as millis (dates in January 1970).
    /// Strictly integers: fractional or string values fail with their kind.
    TimestampSecondsUtc,
    /// JSON string of decimal digits carrying epoch **milliseconds** ↔
    /// Arrow Timestamp(Millisecond, UTC). For Feishu-style APIs whose
    /// `create_time` / `update_time` fields are millisecond epochs
    /// serialized as STRINGS (`"1609296809000"`) — neither an RFC 3339
    /// string nor a JSON number, so [`FieldType::TimestampMillisUtc`]
    /// rejects them. Strictly ASCII-digit strings: anything else fails
    /// with its kind or a shape description, never parses as zero.
    TimestampMillisStringUtc,
    /// JSON string of decimal digits carrying epoch **seconds** ↔ Arrow
    /// Timestamp(Millisecond, UTC) — the seconds-as-string sibling of
    /// [`FieldType::TimestampMillisStringUtc`] (Feishu wiki
    /// `obj_create_time`), with the same strict digit-string rule and the
    /// same overflow guard as [`FieldType::TimestampSecondsUtc`].
    TimestampSecondsStringUtc,
    /// JSON string of epoch **seconds with an optional fractional part**
    /// (`"1700000000.123456"`) ↔ Arrow Timestamp(Millisecond, UTC). Slack's
    /// message `ts` is this shape, and it is the only one of the four
    /// timestamp readers that accepts it:
    /// [`FieldType::TimestampSecondsStringUtc`] is digits-only by design
    /// (Feishu shape drift must fail loudly), and relaxing it would make a
    /// fractional Feishu value parse instead of failing.
    ///
    /// Sub-millisecond precision is **floored**, not rounded: Arrow's
    /// millisecond unit cannot hold Slack's microseconds, and flooring
    /// keeps the column monotonic with the `ts` string it is derived from.
    /// A column of this type is therefore a *time*, not an identity — Slack
    /// `ts` values one microsecond apart land on the same millisecond, so
    /// keep the raw string in its own `utf8` column wherever the row needs
    /// a key.
    TimestampSecondsFractionalStringUtc,
    /// JSON array of strings ↔ Arrow List\<Utf8\>.
    Utf8List,
    /// JSON array of objects, each contributing the string under the given
    /// key ↔ Arrow List\<Utf8\> — the design's `$.labels[*].name` /
    /// `$.assignees[*].login` flattening for GitHub-style shapes.
    Utf8ListFromObjectKey(&'static str),
    /// Any JSON value serialized to a JSON string ↔ Arrow Utf8. For
    /// intentionally opaque fields (arbitrary maps, unstable unions). A
    /// present JSON null is SQL NULL (per the shared null rules), not the
    /// string `"null"`.
    Json,
}

/// One source-pack field: where in the row JSON it lives, and its type.
#[derive(Debug, Clone, Copy)]
pub struct FieldMapping {
    /// Arrow column name.
    pub name: &'static str,
    /// Row-relative dotted path, e.g. `user.login`. Must point at an object
    /// key; array indexing is out of scope for relational mappings.
    pub path: &'static str,
    /// Column type.
    pub field_type: FieldType,
    /// Whether missing keys / JSON nulls become Arrow nulls (true) or fail
    /// conversion (false).
    pub nullable: bool,
}

/// An owned [`FieldMapping`]. Source packs declare columns statically; raw
/// scans (`open_connector_scan`) derive them at planning time from discovered
/// action metadata, so their names cannot be `&'static str`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnSpec {
    /// Arrow column name.
    pub name: String,
    /// Row-relative dotted path (see [`FieldMapping::path`]).
    pub path: String,
    /// Column type.
    pub field_type: FieldType,
    /// Whether missing keys / JSON nulls become Arrow nulls (true) or fail
    /// conversion (false).
    pub nullable: bool,
}

impl From<&FieldMapping> for ColumnSpec {
    fn from(mapping: &FieldMapping) -> Self {
        Self {
            name: mapping.name.to_string(),
            path: mapping.path.to_string(),
            field_type: mapping.field_type,
            nullable: mapping.nullable,
        }
    }
}

impl FieldType {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::Int64 => "integer",
            Self::UInt64 => "non-negative integer",
            Self::Float64 => "number",
            Self::Utf8 => "string",
            Self::TimestampMillisUtc => "RFC 3339 timestamp or epoch millis",
            Self::TimestampSecondsUtc => "epoch-seconds timestamp",
            Self::TimestampMillisStringUtc => "epoch-millis digit string",
            Self::TimestampSecondsStringUtc => "epoch-seconds digit string",
            Self::TimestampSecondsFractionalStringUtc => "fractional epoch-seconds string",
            Self::Utf8List => "array of strings",
            Self::Utf8ListFromObjectKey(_) => "array of objects each carrying a string key",
            Self::Json => "any JSON value",
        }
    }
}

/// Column type as it is spelled in YAML. The serde names are the user
/// vocabulary; [`FieldType`] is what the converter runs on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum ColumnType {
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "int64")]
    Int64,
    #[serde(rename = "uint64")]
    Uint64,
    #[serde(rename = "float64")]
    Float64,
    #[serde(rename = "utf8")]
    Utf8,
    /// RFC 3339 string or epoch-millis number.
    #[serde(rename = "timestamp_ms_utc")]
    TimestampMsUtc,
    /// Epoch-seconds number.
    #[serde(rename = "timestamp_s_utc")]
    TimestampSUtc,
    /// Epoch-millis STRING.
    #[serde(rename = "timestamp_ms_string_utc")]
    TimestampMsStringUtc,
    /// Epoch-seconds STRING, digits only — a sign, a dot, or whitespace
    /// fails on purpose, so a provider whose spelling drifts fails loudly
    /// instead of landing as January 1970.
    #[serde(rename = "timestamp_s_string_utc")]
    TimestampSStringUtc,
    /// Fractional epoch-seconds STRING (`"1700000000.123456"`) — Slack's
    /// message `ts`. Kept distinct from `timestamp_s_string_utc`, which is
    /// digits-only on purpose so Feishu shape drift fails loudly.
    #[serde(rename = "timestamp_s_frac_string_utc")]
    TimestampSFracStringUtc,
    #[serde(rename = "utf8_list")]
    Utf8List,
    /// Pluck one key out of each object of an array (`$.labels[*].name`);
    /// needs [`ColumnDoc::key`].
    #[serde(rename = "utf8_list_from_object_key")]
    Utf8ListFromObjectKey,
    /// Anything, stored as compact JSON text.
    #[serde(rename = "json")]
    Json,
}

/// One `columns:` entry as written in YAML.
///
/// # Example
/// ```
/// use skardi_source_pack::schema::{ColumnDoc, FieldType};
///
/// let doc: ColumnDoc = serde_yaml::from_str(
///     "{ name: received_at, path: timestamp, type: timestamp_s_string_utc, nullable: false }",
/// )
/// .unwrap();
/// let spec = doc.into_spec("messages").unwrap();
/// assert_eq!(spec.field_type, FieldType::TimestampSecondsStringUtc);
/// assert!(!spec.nullable);
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnDoc {
    /// Arrow column name.
    pub name: String,
    /// Row-relative dotted path of object keys (`user.login`). No `$.`
    /// prefix, no array indexes.
    pub path: String,
    /// Column type, in the YAML spelling.
    #[serde(rename = "type")]
    pub column_type: ColumnType,
    /// Object key to pluck; only valid with `utf8_list_from_object_key`.
    #[serde(default)]
    pub key: Option<String>,
    /// Whether a missing key or JSON null becomes an Arrow NULL (`true`) or
    /// fails conversion (`false`).
    pub nullable: bool,
}

impl ColumnDoc {
    /// Check the entry and turn it into the converter's column spec.
    ///
    /// `scope` (a table id, a webhook route) only labels the error. The
    /// `utf8_list_from_object_key` key is leaked into a `'static` string
    /// because [`FieldType`] carries it by reference: declarations are
    /// parsed once per process, so the leak is bounded by the configuration.
    pub fn into_spec(self, scope: &str) -> Result<ColumnSpec, String> {
        if self.name.trim().is_empty() {
            return Err(format!("{scope}: a column has an empty name"));
        }
        if self.path.is_empty() {
            return Err(format!("{scope}: column '{}' has an empty path", self.name));
        }
        if self.path == "$" || self.path.starts_with("$.") {
            return Err(format!(
                "{scope}: column '{}' path '{}' must not start with '$' — paths are \
                 row-relative object keys",
                self.name, self.path
            ));
        }
        if self.path.split('.').any(str::is_empty) {
            return Err(format!(
                "{scope}: column '{}' path '{}' has an empty segment",
                self.name, self.path
            ));
        }
        let field_type = match (self.column_type, self.key) {
            (ColumnType::Utf8ListFromObjectKey, Some(key)) => {
                FieldType::Utf8ListFromObjectKey(Box::leak(key.into_boxed_str()))
            }
            (ColumnType::Utf8ListFromObjectKey, None) => {
                return Err(format!(
                    "{scope}: column '{}' has type utf8_list_from_object_key and needs `key`",
                    self.name
                ));
            }
            (other, Some(_)) => {
                return Err(format!(
                    "{scope}: column '{}' declares `key`, which only \
                     utf8_list_from_object_key accepts (got {other:?})",
                    self.name
                ));
            }
            (ColumnType::Boolean, None) => FieldType::Boolean,
            (ColumnType::Int64, None) => FieldType::Int64,
            (ColumnType::Uint64, None) => FieldType::UInt64,
            (ColumnType::Float64, None) => FieldType::Float64,
            (ColumnType::Utf8, None) => FieldType::Utf8,
            (ColumnType::TimestampMsUtc, None) => FieldType::TimestampMillisUtc,
            (ColumnType::TimestampSUtc, None) => FieldType::TimestampSecondsUtc,
            (ColumnType::TimestampMsStringUtc, None) => FieldType::TimestampMillisStringUtc,
            (ColumnType::TimestampSStringUtc, None) => FieldType::TimestampSecondsStringUtc,
            (ColumnType::TimestampSFracStringUtc, None) => {
                FieldType::TimestampSecondsFractionalStringUtc
            }
            (ColumnType::Utf8List, None) => FieldType::Utf8List,
            (ColumnType::Json, None) => FieldType::Json,
        };
        Ok(ColumnSpec {
            name: self.name,
            path: self.path,
            field_type,
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
    fn every_type_spelling_maps_to_its_field_type() {
        for (spelling, expected) in [
            ("boolean", FieldType::Boolean),
            ("int64", FieldType::Int64),
            ("uint64", FieldType::UInt64),
            ("float64", FieldType::Float64),
            ("utf8", FieldType::Utf8),
            ("timestamp_ms_utc", FieldType::TimestampMillisUtc),
            ("timestamp_s_utc", FieldType::TimestampSecondsUtc),
            (
                "timestamp_ms_string_utc",
                FieldType::TimestampMillisStringUtc,
            ),
            (
                "timestamp_s_string_utc",
                FieldType::TimestampSecondsStringUtc,
            ),
            (
                "timestamp_s_frac_string_utc",
                FieldType::TimestampSecondsFractionalStringUtc,
            ),
            ("utf8_list", FieldType::Utf8List),
            ("json", FieldType::Json),
        ] {
            let spec = doc(&format!(
                "{{ name: c, path: p, type: {spelling}, nullable: true }}"
            ))
            .into_spec("t")
            .unwrap();
            assert_eq!(spec.field_type, expected, "{spelling}");
            assert!(spec.nullable);
        }
        let spec = doc("{ name: labels, path: labels, type: utf8_list_from_object_key, key: name, nullable: true }")
            .into_spec("t")
            .unwrap();
        assert_eq!(spec.field_type, FieldType::Utf8ListFromObjectKey("name"));
    }

    #[test]
    fn nullable_is_required_and_unknown_keys_and_types_are_refused() {
        assert!(serde_yaml::from_str::<ColumnDoc>("{ name: a, path: a, type: utf8 }").is_err());
        assert!(
            serde_yaml::from_str::<ColumnDoc>(
                "{ name: a, path: a, type: utf8, nullable: true, extra: 1 }"
            )
            .is_err()
        );
        assert!(
            serde_yaml::from_str::<ColumnDoc>("{ name: a, path: a, type: text, nullable: true }")
                .is_err()
        );
    }

    #[test]
    fn each_validation_rule_names_the_problem() {
        for (yaml, expected) in [
            (
                "{ name: ' ', path: a, type: utf8, nullable: true }",
                "empty name",
            ),
            (
                "{ name: a, path: '', type: utf8, nullable: true }",
                "empty path",
            ),
            (
                "{ name: a, path: '$', type: utf8, nullable: true }",
                "must not start with '$'",
            ),
            (
                "{ name: a, path: $.a, type: utf8, nullable: true }",
                "must not start with '$'",
            ),
            (
                "{ name: a, path: a..b, type: utf8, nullable: true }",
                "empty segment",
            ),
            (
                "{ name: a, path: a, type: utf8_list_from_object_key, nullable: true }",
                "needs `key`",
            ),
            (
                "{ name: a, path: a, type: utf8, key: x, nullable: true }",
                "only utf8_list_from_object_key accepts",
            ),
        ] {
            let err = doc(yaml).into_spec("scope").unwrap_err();
            assert!(
                err.contains(expected) && err.starts_with("scope:"),
                "{yaml}: {err}"
            );
        }
    }
}
