//! Scan options parsed from the flat `options` map of a `type: obsidian`
//! source. Two keys only; anything else is a registration error so a typo
//! (`exclude_glob`) can never silently disable an exclusion.

use std::collections::HashMap;

use glob::{MatchOptions, Pattern};

/// Default `exclude_globs`: Obsidian's own config and its trash.
pub const DEFAULT_EXCLUDE_GLOBS: &str = ".obsidian/**,.trash/**";
/// Default `max_file_bytes`: 16 MiB.
pub const DEFAULT_MAX_FILE_BYTES: u64 = 16 * 1024 * 1024;

/// Why an `options` map was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OptionsError {
    #[error("unknown option `{key}` (supported: exclude_globs, max_file_bytes)")]
    UnknownKey { key: String },
    #[error("exclude_globs entry `{glob}` is not a valid glob: {message}")]
    InvalidGlob { glob: String, message: String },
    #[error("max_file_bytes must be a positive integer, got `{value}`")]
    InvalidMaxFileBytes { value: String },
}

/// Parsed, validated scan options.
#[derive(Debug, Clone)]
pub struct ScanOptions {
    exclude_globs: Vec<String>,
    patterns: Vec<Pattern>,
    /// Files whose listed size exceeds this are skipped before any read.
    pub max_file_bytes: u64,
}

/// Globs match the forward-slash relative path case-insensitively, as in
/// `documents`; `**` may span separators and a leading `.` needs no literal.
const MATCH_OPTIONS: MatchOptions = MatchOptions {
    case_sensitive: false,
    require_literal_separator: false,
    require_literal_leading_dot: false,
};

impl ScanOptions {
    /// Parse the flat `options` map. Keys are visited in sorted order so the
    /// error for several unknown keys is deterministic. `exclude_globs`
    /// *replaces* the default when present (an empty string excludes nothing).
    pub fn from_map(options: Option<&HashMap<String, String>>) -> Result<Self, OptionsError> {
        let mut exclude = DEFAULT_EXCLUDE_GLOBS.to_string();
        let mut max_file_bytes = DEFAULT_MAX_FILE_BYTES;
        if let Some(map) = options {
            let mut pairs: Vec<(&String, &String)> = map.iter().collect();
            pairs.sort();
            for (key, value) in pairs {
                let value = value.trim();
                match key.as_str() {
                    "exclude_globs" => exclude = value.to_string(),
                    "max_file_bytes" => {
                        max_file_bytes = value
                            .parse::<u64>()
                            .ok()
                            .filter(|n| *n > 0)
                            .ok_or_else(|| OptionsError::InvalidMaxFileBytes {
                                value: value.to_string(),
                            })?;
                    }
                    other => {
                        return Err(OptionsError::UnknownKey {
                            key: other.to_string(),
                        });
                    }
                }
            }
        }
        let globs = exclude
            .split(',')
            .map(str::trim)
            .filter(|g| !g.is_empty())
            .map(str::to_string)
            .collect();
        Self::new(globs, max_file_bytes)
    }

    /// Build from already-split globs (tests and embedders).
    pub fn new(exclude_globs: Vec<String>, max_file_bytes: u64) -> Result<Self, OptionsError> {
        let mut patterns = Vec::with_capacity(exclude_globs.len());
        for glob in &exclude_globs {
            let pattern = Pattern::new(glob).map_err(|e| OptionsError::InvalidGlob {
                glob: glob.clone(),
                message: e.msg.to_string(),
            })?;
            patterns.push(pattern);
        }
        Ok(Self {
            exclude_globs,
            patterns,
            max_file_bytes,
        })
    }

    /// The globs in effect, as written (trimmed).
    pub fn exclude_globs(&self) -> &[String] {
        &self.exclude_globs
    }

    /// Whether a `/`-separated vault-relative path matches any exclude glob.
    pub fn is_excluded(&self, rel_path: &str) -> bool {
        self.patterns
            .iter()
            .any(|p| p.matches_with(rel_path, MATCH_OPTIONS))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn map(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn defaults_when_no_options() {
        let opts = ScanOptions::from_map(None).unwrap();
        assert_eq!(opts.exclude_globs(), &[".obsidian/**", ".trash/**"]);
        assert_eq!(opts.max_file_bytes, 16_777_216);
        assert!(opts.is_excluded(".obsidian/app.json"));
        assert!(opts.is_excluded(".trash/Deleted.md"));
        assert!(!opts.is_excluded("Projects/Design.md"));
    }

    #[test]
    fn exclusion_is_case_insensitive_on_the_relative_path() {
        let opts = ScanOptions::from_map(None).unwrap();
        assert!(opts.is_excluded(".Obsidian/App.json"));
        assert!(opts.is_excluded(".TRASH/x.md"));
    }

    #[test]
    fn custom_exclude_globs_replace_the_default() {
        let m = map(&[("exclude_globs", " templates/** , drafts/*.md ")]);
        let opts = ScanOptions::from_map(Some(&m)).unwrap();
        assert_eq!(opts.exclude_globs(), &["templates/**", "drafts/*.md"]);
        assert!(opts.is_excluded("templates/daily.md"));
        assert!(opts.is_excluded("drafts/a.md"));
        // The default is gone, as the spec says ("replaces the default").
        assert!(!opts.is_excluded(".obsidian/app.json"));
    }

    #[test]
    fn empty_exclude_globs_excludes_nothing() {
        let m = map(&[("exclude_globs", "")]);
        let opts = ScanOptions::from_map(Some(&m)).unwrap();
        assert!(opts.exclude_globs().is_empty());
        assert!(!opts.is_excluded(".obsidian/app.json"));
    }

    #[test]
    fn max_file_bytes_parses_and_rejects_garbage() {
        let m = map(&[("max_file_bytes", " 2048 ")]);
        assert_eq!(ScanOptions::from_map(Some(&m)).unwrap().max_file_bytes, 2048);
        for bad in ["0", "-1", "abc", "1.5", ""] {
            let m = map(&[("max_file_bytes", bad)]);
            assert_eq!(
                ScanOptions::from_map(Some(&m)).unwrap_err(),
                OptionsError::InvalidMaxFileBytes { value: bad.trim().to_string() },
                "value {bad:?}"
            );
        }
    }

    #[test]
    fn unknown_key_is_rejected_and_the_first_sorted_key_is_named() {
        let m = map(&[("zeta", "1"), ("exclude_glob", "x"), ("max_file_bytes", "1")]);
        assert_eq!(
            ScanOptions::from_map(Some(&m)).unwrap_err(),
            OptionsError::UnknownKey { key: "exclude_glob".to_string() }
        );
    }

    #[test]
    fn invalid_glob_is_rejected() {
        let m = map(&[("exclude_globs", "[unclosed")]);
        assert!(matches!(
            ScanOptions::from_map(Some(&m)).unwrap_err(),
            OptionsError::InvalidGlob { glob, .. } if glob == "[unclosed"
        ));
    }

    #[test]
    fn errors_display_the_offending_value() {
        let e = OptionsError::UnknownKey { key: "foo".into() };
        assert_eq!(
            e.to_string(),
            "unknown option `foo` (supported: exclude_globs, max_file_bytes)"
        );
    }
}
