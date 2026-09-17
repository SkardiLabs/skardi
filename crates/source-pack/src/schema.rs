//! What a provider's rows look like, as a DECLARATION.
//!
//! These types were already neutral when they lived in `json_to_arrow.rs`:
//! [`FieldType`]'s variants describe JSON shapes and name Arrow only in their
//! prose. They looked coupled because they sat beside the conversion, not
//! because they carried an Arrow type. Separating them is what lets a syncer
//! read a provider's rows without linking a columnar format it will never
//! build.
//!
//! The conversion itself stays in the engine, where the `RecordBatch` is
//! wanted.

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
