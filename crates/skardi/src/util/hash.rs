//! Dependency-free hashing for fingerprints (drift detection, cache keys).
//!
//! FNV-1a is used because it is stable across processes and compiler
//! releases without pulling in a cryptographic hash crate — fingerprints
//! detect *changes*, they are not a security boundary.

/// FNV-1a 64-bit offset basis.
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
/// FNV-1a 64-bit prime.
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a 64-bit hash of `bytes`, hex-encoded (16 chars).
///
/// # Example
/// ```
/// use skardi::util::hash::fnv1a_hex;
///
/// assert_eq!(fnv1a_hex(b""), "cbf29ce484222325");
/// assert_eq!(fnv1a_hex(b"abc"), fnv1a_hex(b"abc"));
/// assert_ne!(fnv1a_hex(b"abc"), fnv1a_hex(b"abd"));
/// ```
pub fn fnv1a_hex(bytes: &[u8]) -> String {
    let mut hash: u64 = FNV_OFFSET_BASIS;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hex::encode(hash.to_be_bytes())
}
