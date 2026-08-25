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
        Ok(Pkce::from_verifier(random_urlsafe()?))
    }

    /// The S256 transform on its own, split out from [`Self::generate`] so a
    /// KNOWN verifier can go through the production code path. With the
    /// verifier random, every assertion about the pair is true by construction
    /// — a switch to standard base64, or to SHA-1, would leave a
    /// self-consistency test passing and only fail against a real provider.
    fn from_verifier(verifier: String) -> Pkce {
        let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
        Pkce {
            verifier,
            challenge,
        }
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
    use super::{Pkce, random_urlsafe};

    /// RFC 7636 appendix B's worked example, run THROUGH the production
    /// transform — so this pins `Pkce`'s algorithm choice (S256, base64url
    /// unpadded), not `sha2` and `base64`. Recomputing the expected value
    /// inline would pass for SHA-1 too.
    #[test]
    fn s256_matches_rfc_7636_appendix_b() {
        let pkce = Pkce::from_verifier("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk".to_string());
        assert_eq!(
            pkce.challenge,
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }

    /// What `generate` adds over the transform: fresh, correctly sized, and
    /// URL-safe. Deliberately no challenge/verifier comparison here — with a
    /// random verifier that assertion is a tautology, and the vector above is
    /// what actually pins the algorithm.
    #[test]
    fn generated_pairs_are_unreserved_and_fresh() {
        let first = Pkce::generate().unwrap();
        let second = Pkce::generate().unwrap();

        assert_ne!(first.verifier, second.verifier, "verifier must not repeat");
        assert_ne!(first.challenge, second.challenge);
        assert_eq!(first.verifier.len(), 43, "RFC 7636 requires 43..=128");
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
