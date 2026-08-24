//! PKCE S256 and the `state` nonce (§6.1 step 2, §9.1).
//!
//! Both values exist to bind the authorization response to THIS process: the
//! verifier proves the code was requested by whoever redeems it, and `state`
//! proves the callback belongs to the request we made. Both therefore come
//! from the OS CSPRNG directly — `getrandom` and nothing more, with no PRNG
//! state to seed, fork-safety to reason about, or reseeding policy to get
//! wrong.

use anyhow::{Result, anyhow};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use sha2::{Digest, Sha256};

/// Bytes of entropy behind the verifier and `state`. 32 bytes is 256 bits and
/// encodes to 43 unreserved characters, which is also RFC 7636's floor for a
/// verifier (43..=128 characters).
const ENTROPY_BYTES: usize = 32;

/// A PKCE pair: the secret kept in memory and the challenge that goes over the
/// wire.
pub struct Pkce {
    /// Sent only to the token endpoint, in the exchange that redeems the code.
    pub verifier: String,
    /// `base64url(sha256(verifier))`, sent to the authorization endpoint.
    pub challenge: String,
}

impl Pkce {
    pub fn generate() -> Result<Pkce> {
        let verifier = random_urlsafe()?;
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        Ok(Pkce {
            verifier,
            challenge,
        })
    }
}

/// A fresh unreserved-character random string, used for the verifier and for
/// `state`.
///
/// base64url WITHOUT padding on purpose: every character is in RFC 3986's
/// unreserved set, so the value survives a URL, a query string, and a
/// redirect without encoding — and `=` would not.
pub fn random_urlsafe() -> Result<String> {
    let mut bytes = [0u8; ENTROPY_BYTES];
    getrandom::getrandom(&mut bytes)
        .map_err(|err| anyhow!("read random bytes from the operating system: {err}"))?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

#[cfg(test)]
mod tests {
    use super::{Pkce, URL_SAFE_NO_PAD, random_urlsafe};
    use base64::Engine as _;
    use sha2::{Digest, Sha256};

    /// RFC 7636 appendix B's worked example, so the transform is pinned to the
    /// spec rather than to itself.
    #[test]
    fn s256_matches_rfc_7636_appendix_b() {
        let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        assert_eq!(challenge, "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM");
    }

    #[test]
    fn generated_pairs_are_unreserved_fresh_and_self_consistent() {
        let first = Pkce::generate().unwrap();
        let second = Pkce::generate().unwrap();

        assert_ne!(first.verifier, second.verifier, "verifier must not repeat");
        assert_ne!(first.challenge, second.challenge);
        assert_eq!(first.verifier.len(), 43, "RFC 7636 requires 43..=128");
        assert_eq!(
            first.challenge,
            URL_SAFE_NO_PAD.encode(Sha256::digest(first.verifier.as_bytes()))
        );
        for value in [
            &first.verifier,
            &first.challenge,
            &random_urlsafe().unwrap(),
        ] {
            assert!(
                value
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-._~".contains(&b)),
                "not URL-safe: {value}"
            );
        }
    }
}
