//! Connection config resolution for the CLI: where the server URL and API
//! token come from, and in what order they win.
//!
//! Precedence is per-field: `--server`/`--token` flag > `SKARDI_SERVER_URL`/
//! `SKARDI_API_TOKEN` env var > `~/.skardi/config.yaml` > built-in default.
//! Each field (server, token) is resolved independently, so e.g. the server
//! can come from the environment while the token comes from the config file.

use serde::Deserialize;
use std::path::Path;

/// Server URL used when no flag, env var, or config file supplies one.
pub const DEFAULT_SERVER_URL: &str = "http://127.0.0.1:8080";

const SERVER_URL_ENV: &str = "SKARDI_SERVER_URL";
const API_TOKEN_ENV: &str = "SKARDI_API_TOKEN";

/// Resolved connection settings for talking to a skardi-server instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientConfig {
    pub server: String,
    pub token: Option<String>,
}

impl ClientConfig {
    /// Resolve the effective client config from CLI flags, environment
    /// variables, and the user's `~/.skardi/config.yaml`, in that precedence
    /// order (flag > env > file > default).
    ///
    /// Not unit-tested directly: it reads process-global environment
    /// variables, which race under parallel test execution. All precedence
    /// logic lives in `resolve_from`, which is tested exhaustively below.
    // TODO(task 6): called from `main` once subcommands are wired up; remove
    // this `allow` when that lands.
    #[allow(dead_code)]
    pub fn resolve(flag_server: Option<String>, flag_token: Option<String>) -> ClientConfig {
        let env_server = std::env::var(SERVER_URL_ENV).ok();
        let env_token = std::env::var(API_TOKEN_ENV).ok();

        let config_path = dirs::home_dir().map(|home| home.join(".skardi").join("config.yaml"));
        let file_config = config_path.as_deref().and_then(load_file_config);

        resolve_from(flag_server, flag_token, env_server, env_token, file_config)
    }
}

/// `spec:` block of `~/.skardi/config.yaml`:
///
/// ```yaml
/// kind: client
/// metadata:
///   name: default
/// spec:
///   server: http://127.0.0.1:8080
///   token: optional-bearer-token
/// ```
///
/// Both fields are optional so a partial `spec:` (or a manifest with no
/// `spec:` at all) is a valid, if unhelpful, config file.
#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
struct FileConfig {
    #[serde(default)]
    server: Option<String>,
    #[serde(default)]
    token: Option<String>,
}

/// Top-level manifest envelope. Only `spec` is read here — `kind` and
/// `metadata` exist in the on-disk format for consistency with the repo's
/// other manifests but nothing at runtime needs them, so they are left
/// unmodeled and simply ignored by serde.
#[derive(Debug, Deserialize)]
struct ConfigManifest {
    #[serde(default)]
    spec: Option<FileConfig>,
}

/// Pure per-field precedence resolution: flag > env > file > default.
/// No I/O and no env reads — everything needed is passed in — so this is
/// the unit-testable core of `ClientConfig::resolve`.
fn resolve_from(
    flag_server: Option<String>,
    flag_token: Option<String>,
    env_server: Option<String>,
    env_token: Option<String>,
    file_config: Option<FileConfig>,
) -> ClientConfig {
    let (file_server, file_token) = match file_config {
        Some(file) => (file.server, file.token),
        None => (None, None),
    };

    let server = flag_server
        .or(env_server)
        .or(file_server)
        .unwrap_or_else(|| DEFAULT_SERVER_URL.to_string());
    let token = flag_token.or(env_token).or(file_token);

    ClientConfig { server, token }
}

/// Load and parse `path` as a config manifest, returning its `spec` block.
///
/// - Missing file: resolves silently to `None` (this is the common case —
///   most users never create `~/.skardi/config.yaml`).
/// - Present but unparsable YAML: prints a `warning: ...` to stderr and
///   resolves to `None`. Never fatal.
/// - Present, valid YAML, but no `spec:` section: resolves to `None`.
fn load_file_config(path: &Path) -> Option<FileConfig> {
    let content = std::fs::read_to_string(path).ok()?;

    match serde_yaml::from_str::<ConfigManifest>(&content) {
        Ok(manifest) => manifest.spec,
        Err(err) => {
            eprintln!(
                "warning: ignoring malformed config file {}: {}",
                path.display(),
                err
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn file_config(server: &str, token: &str) -> FileConfig {
        FileConfig {
            server: Some(server.to_string()),
            token: Some(token.to_string()),
        }
    }

    fn write_manifest(contents: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(contents.as_bytes()).unwrap();
        file
    }

    #[test]
    fn resolve_from_nothing_set_uses_default_server_and_no_token() {
        let resolved = resolve_from(None, None, None, None, None);

        assert_eq!(resolved.server, DEFAULT_SERVER_URL);
        assert_eq!(resolved.token, None);
    }

    #[test]
    fn resolve_from_file_only_uses_file_values() {
        let resolved = resolve_from(
            None,
            None,
            None,
            None,
            Some(file_config("http://file-server:9000", "file-token")),
        );

        assert_eq!(resolved.server, "http://file-server:9000");
        assert_eq!(resolved.token, Some("file-token".to_string()));
    }

    #[test]
    fn resolve_from_env_beats_file() {
        let resolved = resolve_from(
            None,
            None,
            Some("http://env-server:9000".to_string()),
            Some("env-token".to_string()),
            Some(file_config("http://file-server:9000", "file-token")),
        );

        assert_eq!(resolved.server, "http://env-server:9000");
        assert_eq!(resolved.token, Some("env-token".to_string()));
    }

    #[test]
    fn resolve_from_flag_beats_env_and_file() {
        let resolved = resolve_from(
            Some("http://flag-server:9000".to_string()),
            Some("flag-token".to_string()),
            Some("http://env-server:9000".to_string()),
            Some("env-token".to_string()),
            Some(file_config("http://file-server:9000", "file-token")),
        );

        assert_eq!(resolved.server, "http://flag-server:9000");
        assert_eq!(resolved.token, Some("flag-token".to_string()));
    }

    #[test]
    fn resolve_from_per_field_independence_server_from_env_token_from_file() {
        let resolved = resolve_from(
            None,
            None,
            Some("http://env-server:9000".to_string()),
            None,
            Some(file_config("http://file-server:9000", "file-token")),
        );

        assert_eq!(resolved.server, "http://env-server:9000");
        assert_eq!(resolved.token, Some("file-token".to_string()));
    }

    #[test]
    fn load_file_config_valid_manifest_returns_spec_values() {
        let file = write_manifest(
            "kind: client\n\
             metadata:\n  name: default\n\
             spec:\n  server: http://127.0.0.1:8080\n  token: optional-bearer-token\n",
        );

        let loaded = load_file_config(file.path());

        assert_eq!(
            loaded,
            Some(file_config(
                "http://127.0.0.1:8080",
                "optional-bearer-token"
            ))
        );
    }

    #[test]
    fn load_file_config_manifest_without_spec_is_absent() {
        let file = write_manifest("kind: client\nmetadata:\n  name: default\n");

        let loaded = load_file_config(file.path());

        assert_eq!(loaded, None);
    }

    #[test]
    fn load_file_config_malformed_yaml_is_absent() {
        let file = write_manifest("kind: client\n  this: [is not, valid yaml\n");

        let loaded = load_file_config(file.path());

        assert_eq!(loaded, None);
    }

    #[test]
    fn load_file_config_nonexistent_path_is_absent() {
        let loaded = load_file_config(Path::new("/nonexistent/path/does/not/exist/config.yaml"));

        assert_eq!(loaded, None);
    }
}
