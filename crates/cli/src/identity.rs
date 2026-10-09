//! What the CLI says about itself on every request it makes.
//!
//! Two headers, built once per HTTP client and installed as its defaults:
//!
//! * `User-Agent: skardi-cli/<version> (<os>; <arch>)` — the CLI's own version
//!   and Rust's `env::consts::{OS, ARCH}`, compile-time constants. Nothing
//!   is probed and nothing about the person or the machine is included. The MCP
//!   bridge marks itself after the product token
//!   (`skardi-cli/<version> mcp (<os>; <arch>)`), because "reached through an
//!   agent" is the split worth knowing.
//! * `DNT: 1`, only when `DO_NOT_TRACK` is set. The User-Agent then shrinks to
//!   the bare `skardi-cli`. skardi-cloud records nothing about the client for
//!   a request that carries `DNT: 1`.
//!
//! The headers go only where the CLI already sends requests: the server or
//! gateway it is pointed at, and the control plane it signs in to. Never to
//! the identity provider a `--client-id` login redeems its code at — that is
//! a third party (`login::control_plane::identity_provider_client`). The CLI
//! makes no request of its own to report them, and against a local
//! skardi-server they stay on the machine.

use std::env;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue, USER_AGENT};

/// The product token every User-Agent from this binary starts with.
pub const PRODUCT: &str = "skardi-cli";

/// The `DNT` request header. Not in `reqwest::header`'s constants.
const DNT: HeaderName = HeaderName::from_static("dnt");

/// Which part of the CLI is making the request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Surface {
    /// An ordinary command, typed by a person or a script.
    Cli,
    /// `skardi mcp`: an agent host driving the CLI over stdio.
    Mcp,
}

/// The identity one HTTP client presents.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ClientIdentity {
    pub surface: Surface,
    pub do_not_track: bool,
}

impl ClientIdentity {
    /// `surface`, with the opt-out read from `DO_NOT_TRACK`.
    pub fn from_env(surface: Surface) -> ClientIdentity {
        ClientIdentity {
            surface,
            do_not_track: do_not_track(env::var("DO_NOT_TRACK").ok().as_deref()),
        }
    }

    /// The `User-Agent` value.
    pub fn user_agent(&self) -> String {
        if self.do_not_track {
            return PRODUCT.to_string();
        }
        let marker = match self.surface {
            Surface::Cli => "",
            Surface::Mcp => " mcp",
        };
        format!(
            "{PRODUCT}/{}{marker} ({}; {})",
            env!("CARGO_PKG_VERSION"),
            env::consts::OS,
            env::consts::ARCH
        )
    }

    /// The headers a client built for this identity sends on every request.
    pub fn headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        // Infallible: every part is ASCII — the product token, a Cargo version
        // (SemVer's alphabet), and Rust's own OS and ARCH names.
        let user_agent = HeaderValue::from_str(&self.user_agent())
            .expect("a User-Agent built from ASCII constants is a valid header value");
        headers.insert(USER_AGENT, user_agent);
        if self.do_not_track {
            headers.insert(DNT, HeaderValue::from_static("1"));
        }
        headers
    }
}

/// Whether a `DO_NOT_TRACK` value opts out.
///
/// The convention (<https://consoledonottrack.com>) is `DO_NOT_TRACK=1`.
/// `true` and `yes` are honoured too, since someone who typed either meant it.
/// Unset, empty, `0` and `false` do not opt out.
pub fn do_not_track(value: Option<&str>) -> bool {
    match value.map(|v| v.trim().to_ascii_lowercase()) {
        Some(v) => matches!(v.as_str(), "1" | "true" | "yes"),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_user_agent_names_the_cli_its_version_and_its_platform() {
        let cli = ClientIdentity {
            surface: Surface::Cli,
            do_not_track: false,
        };
        assert_eq!(
            cli.user_agent(),
            format!(
                "skardi-cli/{} ({}; {})",
                env!("CARGO_PKG_VERSION"),
                env::consts::OS,
                env::consts::ARCH
            )
        );
        assert!(!cli.headers().contains_key("dnt"));
    }

    #[test]
    fn the_mcp_bridge_marks_itself_after_the_product_token() {
        let mcp = ClientIdentity {
            surface: Surface::Mcp,
            do_not_track: false,
        };
        assert_eq!(
            mcp.user_agent(),
            format!(
                "skardi-cli/{} mcp ({}; {})",
                env!("CARGO_PKG_VERSION"),
                env::consts::OS,
                env::consts::ARCH
            )
        );
    }

    #[test]
    fn do_not_track_reduces_the_user_agent_and_sends_dnt() {
        for surface in [Surface::Cli, Surface::Mcp] {
            let identity = ClientIdentity {
                surface,
                do_not_track: true,
            };
            let headers = identity.headers();
            assert_eq!(headers[USER_AGENT], "skardi-cli");
            assert_eq!(headers["dnt"], "1");
        }
    }

    #[test]
    fn only_an_affirmative_do_not_track_opts_out() {
        for (value, opted_out) in [
            (Some("1"), true),
            (Some("true"), true),
            (Some("TRUE"), true),
            (Some(" yes "), true),
            (Some("0"), false),
            (Some("false"), false),
            (Some(""), false),
            (None, false),
        ] {
            assert_eq!(do_not_track(value), opted_out, "{value:?}");
        }
    }
}
