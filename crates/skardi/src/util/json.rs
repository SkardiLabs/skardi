//! Canonical JSON helpers shared by fingerprinting and cache keys —
//! plus [`json_kind`], the one kind-vocabulary for error messages.

use serde_json::Value;

/// The JSON kind of a value, for diagnostics — kinds only, NEVER the
/// value itself (the never-echo-values discipline every error path in
/// this repo shares). This is the single definition: graph errors, Open
/// Connector conversion and row-path extraction, and the server's
/// parameter substitution all point here, so the vocabulary cannot
/// drift between the places that enforce the same guarantee.
pub fn json_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}
/// Serialize a JSON value in canonical form: object keys sorted recursively,
/// arrays kept in order, strings via `serde_json` escaping. Two semantically
/// equal values always produce the same string, which is what compatibility
/// fingerprints and cache keys require.
///
/// # Example
/// ```
/// use skardi::util::json::canonical_json;
///
/// let a = serde_json::json!({"b": 1, "a": [true, null]});
/// let b = serde_json::json!({"a": [true, null], "b": 1});
/// assert_eq!(canonical_json(&a), canonical_json(&b));
/// ```
pub fn canonical_json(value: &Value) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

/// Write a JSON value in canonical form into `out` (see [`canonical_json`]).
pub fn write_canonical(value: &Value, out: &mut String) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) => {
            out.push_str(&serde_json::to_string(s).unwrap_or_else(|_| "\"\"".to_string()))
        }
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (index, key) in keys.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&serde_json::to_string(key).unwrap_or_else(|_| "\"\"".to_string()));
                out.push(':');
                if let Some(value) = map.get(*key) {
                    write_canonical(value, out);
                }
            }
            out.push('}');
        }
    }
}

/// BLAKE3 digest of `bytes`, hex-encoded (64 chars).
///
/// Compatibility fingerprints reject changed upstream action contracts, so
/// they require collision resistance rather than a small non-cryptographic
/// checksum. BLAKE3 is already used for stable document IDs in this crate.
/// Pairs with [`canonical_json`]: canonicalize the structured value, then
/// digest the canonical form for a stable fingerprint or cache key.
///
/// # Example
/// ```
/// use skardi::util::json::blake3_hex;
///
/// assert_eq!(blake3_hex(b"abc"), blake3_hex(b"abc"));
/// assert_ne!(blake3_hex(b"abc"), blake3_hex(b"abd"));
/// ```
pub fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}
