//! Stable cryptographic hashing for fingerprints and cache keys.

/// BLAKE3 digest of `bytes`, hex-encoded (64 chars).
///
/// Compatibility fingerprints reject changed upstream action contracts, so
/// they require collision resistance rather than a small non-cryptographic
/// checksum. BLAKE3 is already used for stable document IDs in this crate.
///
/// # Example
/// ```
/// use skardi::util::hash::blake3_hex;
///
/// assert_eq!(blake3_hex(b"abc"), blake3_hex(b"abc"));
/// assert_ne!(blake3_hex(b"abc"), blake3_hex(b"abd"));
/// ```
pub fn blake3_hex(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}
