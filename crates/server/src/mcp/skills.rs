//! Skills over MCP: the `io.modelcontextprotocol/skills` extension (SEP-2640),
//! pinned to the Beta specification at
//! <https://github.com/modelcontextprotocol/ext-skills/blob/ff224337b01f0c9b18b81dcf99eefe66e9a52b5c/specification/beta/skills.mdx>
//! against base protocol `2026-07-28`.
//!
//! The catalog is a snapshot of a directory of skills (`--mcp-skills-dir`),
//! read once at startup and held in memory. Serving digests and bytes from
//! the same snapshot is what makes the spec's integrity rule hold by
//! construction: a `resources/read` always returns exactly the bytes its
//! `skills/list` digest describes, even if the directory changes on disk
//! afterwards (the server picks that up on restart).
//!
//! Scope of this first slice: `skills/list`, `skills/get` and
//! `resources/read` over `skill://` URIs. `resources/directory/read` is
//! optional in the spec and not implemented, so `directoryRead` is not
//! declared.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;
use std::str::from_utf8;

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use rmcp::model::{ErrorData, JsonObject, Resource, ResourceContents};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use skardi_mcp_core::encode_component;

/// The extension identifier declared under `capabilities.extensions`.
pub(crate) const EXTENSION_ID: &str = "io.modelcontextprotocol/skills";

const SCHEME: &str = "skill://";
const SKILL_MD: &str = "SKILL.md";

/// Freshness hint on every cacheable result. The catalog only changes on
/// restart, so five minutes (the spec's own example value) costs a host at
/// most one stale listing after a redeploy, and a stale entry is caught
/// anyway by digest verification on read.
pub(crate) const TTL_MS: u64 = 300_000;

/// `skills/list` page size. Each entry carries a full file manifest, so a
/// page is bounded in entries rather than bytes.
const PAGE_SIZE: usize = 50;

/// Per-skill limits every conforming host must accept (spec § Limits). A
/// skill over either one is skipped with a warning rather than served,
/// because no host is guaranteed to load it.
const MAX_FILES_PER_SKILL: usize = 512;
const MAX_BYTES_PER_SKILL: usize = 16 * 1024 * 1024;

/// One file of the snapshot, addressed by its `skill://` URI.
#[derive(Debug)]
struct SkillFile {
    bytes: Vec<u8>,
    /// `sha256:` + 64 lowercase hex chars over `bytes`.
    digest: String,
    mime_type: &'static str,
}

#[derive(Debug)]
struct SkillEntry {
    /// URI of the skill's `SKILL.md`.
    uri: String,
    frontmatter: JsonObject,
    /// Every file under the skill directory (nested skills included, per
    /// the spec's completeness rule), sorted, `SKILL.md` among them.
    resource_uris: Vec<String>,
}

/// The skills a server serves, keyed for both protocol paths: entries by
/// `SKILL.md` URI (`skills/list`, `skills/get`) and file bytes by URI
/// (`resources/read`). Both maps are `BTreeMap`s so listing order — and
/// with it the offset cursor — is stable across requests.
#[derive(Debug, Default)]
pub(crate) struct SkillCatalog {
    skills: BTreeMap<String, SkillEntry>,
    files: BTreeMap<String, SkillFile>,
}

impl SkillCatalog {
    /// Snapshot every skill under `root`: each directory below it that
    /// contains a `SKILL.md` is a skill, and its path relative to `root` is
    /// the skill path. A missing or non-directory `root` is an error (the
    /// operator asked for skills and would otherwise get none silently);
    /// an individual invalid skill is skipped with a warning so one bad
    /// `SKILL.md` does not take the server down.
    ///
    /// Blocking: call from `spawn_blocking`.
    pub(crate) fn load(root: &Path) -> Result<Self> {
        let meta = fs::metadata(root)
            .with_context(|| format!("--mcp-skills-dir {}: cannot read", root.display()))?;
        if !meta.is_dir() {
            bail!("--mcp-skills-dir {}: not a directory", root.display());
        }

        let mut paths = Vec::new();
        collect_files(root, &mut Vec::new(), &mut paths)?;

        let skill_dirs: Vec<Vec<String>> = paths
            .iter()
            .filter(|segments| {
                segments.len() >= 2 && segments.last().is_some_and(|s| s == SKILL_MD)
            })
            .map(|segments| segments[..segments.len() - 1].to_vec())
            .collect();

        let mut catalog = SkillCatalog::default();
        for dir in skill_dirs {
            let members: Vec<&Vec<String>> = paths.iter().filter(|p| p.starts_with(&dir)).collect();
            match build_entry(root, &dir, &members) {
                Ok((entry, files)) => {
                    catalog.files.extend(files);
                    catalog.skills.insert(entry.uri.clone(), entry);
                }
                Err(e) => tracing::warn!(
                    "Skipping skill {}: {e:#}",
                    root.join(dir.join("/")).display()
                ),
            }
        }
        tracing::info!(
            "Serving {} skill(s) over MCP from {}: {:?}",
            catalog.skills.len(),
            root.display(),
            catalog.skills.keys().collect::<Vec<_>>()
        );
        Ok(catalog)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.skills.is_empty()
    }

    /// `skills/list`. The cursor is an opaque decimal offset into the
    /// URI-sorted listing; anything that does not parse is invalid params,
    /// as the base protocol's list methods treat a bad cursor.
    pub(crate) fn list(&self, cursor: Option<&str>) -> Result<Value, ErrorData> {
        let start = match cursor {
            None => 0,
            Some(c) => c
                .parse::<usize>()
                .ok()
                .filter(|n| *n <= self.skills.len())
                .ok_or_else(|| ErrorData::invalid_params(format!("invalid cursor: {c}"), None))?,
        };
        let skills: Vec<Value> = self
            .skills
            .values()
            .skip(start)
            .take(PAGE_SIZE)
            .map(|entry| self.entry_json(entry))
            .collect();
        let end = start + skills.len();
        let mut result = json!({
            "resultType": "complete",
            "skills": skills,
            "ttlMs": TTL_MS,
            "cacheScope": "private",
        });
        if end < self.skills.len() {
            result["nextCursor"] = Value::String(end.to_string());
        }
        Ok(result)
    }

    /// `skills/get`: `-32602` for a URI that is not a served skill's
    /// `SKILL.md`, as the spec requires.
    pub(crate) fn get(&self, uri: &str) -> Result<Value, ErrorData> {
        let entry = self.skills.get(uri).ok_or_else(|| {
            ErrorData::invalid_params(format!("No skill is served at {uri}"), None)
        })?;
        Ok(json!({
            "resultType": "complete",
            "skill": self.entry_json(entry),
            "ttlMs": TTL_MS,
            "cacheScope": "private",
        }))
    }

    /// `resources/read` contents for a skill file, `None` when the URI is
    /// not one. UTF-8 files go out as `text` (byte-identical to what the
    /// digest covers); anything else as base64 `blob`.
    pub(crate) fn read(&self, uri: &str) -> Option<ResourceContents> {
        let file = self.files.get(uri)?;
        let contents = match from_utf8(&file.bytes) {
            Ok(text) => ResourceContents::text(text, uri),
            Err(_) => ResourceContents::blob(BASE64.encode(&file.bytes), uri),
        };
        Some(contents.with_mime_type(file.mime_type))
    }

    /// One `resources/list` entry per skill's `SKILL.md`, with the metadata
    /// the spec recommends (§ Resource Metadata), so a client that does not
    /// implement the extension still sees the skills as ordinary resources.
    /// Supporting files are reachable by `resources/read` but not listed:
    /// the extension's manifest is where they are enumerated.
    pub(crate) fn skill_md_resources(&self) -> Vec<Resource> {
        self.skills
            .values()
            .map(|entry| {
                let name = entry.frontmatter.get("name").and_then(Value::as_str);
                let description = entry.frontmatter.get("description").and_then(Value::as_str);
                let mut resource = Resource::new(entry.uri.clone(), name.unwrap_or_default())
                    .with_mime_type("text/markdown");
                if let Some(description) = description {
                    resource = resource.with_description(description);
                }
                if let Some(file) = self.files.get(&entry.uri) {
                    resource = resource.with_size(file.bytes.len() as u64);
                }
                resource
            })
            .collect()
    }

    fn entry_json(&self, entry: &SkillEntry) -> Value {
        let resources: Vec<Value> = entry
            .resource_uris
            .iter()
            .filter_map(|uri| {
                let file = self.files.get(uri)?;
                Some(json!({"uri": uri, "digest": file.digest, "size": file.bytes.len()}))
            })
            .collect();
        json!({
            "uri": entry.uri,
            "frontmatter": entry.frontmatter,
            "resources": resources,
        })
    }
}

/// Recursively collect regular files under `dir` as path segments relative
/// to the walk root. Dot-entries (`.git`, `.DS_Store`) are skipped, and so
/// are symlinks: following one could serve a file outside the configured
/// directory. Non-UTF-8 names are skipped because they cannot become a URI
/// segment faithfully. An unreadable root is an error; an unreadable entry
/// below it is skipped with a warning, so one bad folder does not take the
/// server down.
fn collect_files(dir: &Path, prefix: &mut Vec<String>, out: &mut Vec<Vec<String>>) -> Result<()> {
    let entries = fs::read_dir(dir).and_then(|rd| rd.collect::<io::Result<Vec<_>>>());
    let mut entries = match entries {
        Ok(entries) => entries,
        Err(e) if prefix.is_empty() => {
            return Err(e).with_context(|| format!("reading {}", dir.display()));
        }
        Err(e) => {
            tracing::warn!("Skipping unreadable {}: {e}", dir.display());
            return Ok(());
        }
    };
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            tracing::warn!("Skipping non-UTF-8 path under {}", dir.display());
            continue;
        };
        // shortcut: skips dot-entries and Python bytecode caches (what running
        // a skill's scripts in place leaves behind, and what skardi-skills
        // gitignores) rather than honoring .gitignore; serve only git-tracked
        // files if operators start keeping other untracked files in the dir.
        if name.starts_with('.') || name == "__pycache__" || name.ends_with(".pyc") {
            continue;
        }
        let file_type = match entry.file_type() {
            Ok(file_type) => file_type,
            Err(e) => {
                tracing::warn!("Skipping unreadable {}: {e}", entry.path().display());
                continue;
            }
        };
        if file_type.is_symlink() {
            tracing::warn!("Skipping symlink {}", entry.path().display());
            continue;
        }
        prefix.push(name);
        if file_type.is_dir() {
            collect_files(&entry.path(), prefix, out)?;
        } else if file_type.is_file() {
            out.push(prefix.clone());
        }
        prefix.pop();
    }
    Ok(())
}

type BuiltSkill = (SkillEntry, Vec<(String, SkillFile)>);

fn build_entry(root: &Path, dir: &[String], members: &[&Vec<String>]) -> Result<BuiltSkill> {
    let skill_name = dir.last().context("skill path is empty")?;
    if members.len() > MAX_FILES_PER_SKILL {
        bail!(
            "{} files exceeds the {MAX_FILES_PER_SKILL}-file limit",
            members.len()
        );
    }

    let paths: Vec<_> = members
        .iter()
        .map(|segments| segments.iter().fold(root.to_path_buf(), |p, s| p.join(s)))
        .collect();
    // Sizes first, so an oversized file is rejected before it is read into
    // memory. The byte count below is still what the limit is judged on.
    let mut declared = 0u64;
    for path in &paths {
        declared += fs::metadata(path)
            .with_context(|| format!("reading {}", path.display()))?
            .len();
    }
    if declared > MAX_BYTES_PER_SKILL as u64 {
        bail!("{declared} bytes exceeds the {MAX_BYTES_PER_SKILL}-byte limit");
    }

    let mut files = Vec::with_capacity(members.len());
    let mut total = 0usize;
    let mut skill_md: Option<Vec<u8>> = None;
    for (segments, path) in members.iter().zip(&paths) {
        let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        total += bytes.len();
        if segments.len() == dir.len() + 1 && segments.last().is_some_and(|s| s == SKILL_MD) {
            skill_md = Some(bytes.clone());
        }
        let file = SkillFile {
            digest: format!("sha256:{}", hex::encode(Sha256::digest(&bytes))),
            mime_type: mime_type(segments.last().map(String::as_str).unwrap_or_default()),
            bytes,
        };
        files.push((uri_for(segments), file));
    }
    if total > MAX_BYTES_PER_SKILL {
        bail!("{total} bytes exceeds the {MAX_BYTES_PER_SKILL}-byte limit");
    }

    let skill_md = skill_md.context("no SKILL.md at the skill root")?;
    let text = from_utf8(&skill_md).context("SKILL.md is not UTF-8")?;
    let frontmatter = parse_frontmatter(text)?;
    let name = frontmatter
        .get("name")
        .and_then(Value::as_str)
        .context("frontmatter has no string `name`")?;
    validate_name(name)?;
    if name != skill_name {
        bail!("frontmatter name `{name}` does not match its directory `{skill_name}`");
    }
    match frontmatter.get("description").and_then(Value::as_str) {
        Some(d) if !d.trim().is_empty() => {}
        _ => bail!("frontmatter has no non-empty string `description`"),
    }

    let mut skill_md_path = dir.to_vec();
    skill_md_path.push(SKILL_MD.to_string());
    let mut resource_uris: Vec<String> = files.iter().map(|(uri, _)| uri.clone()).collect();
    resource_uris.sort();
    let entry = SkillEntry {
        uri: uri_for(&skill_md_path),
        frontmatter,
        resource_uris,
    };
    Ok((entry, files))
}

/// `skill://<skill-path>/<file-path>`, each segment percent-encoded so a
/// file name with a space or `#` still yields a single, round-trippable URI
/// (unreserved characters — every name in skardi-skills today — pass
/// through unchanged).
fn uri_for(segments: &[String]) -> String {
    let encoded: Vec<String> = segments.iter().map(|s| encode_component(s)).collect();
    format!("{SCHEME}{}", encoded.join("/"))
}

/// The YAML block between the opening `---` line and the next `---` line,
/// rendered as a JSON object. The spec requires `frontmatter` to be the
/// author's frontmatter verbatim, so the whole mapping passes through —
/// not a curated subset.
fn parse_frontmatter(text: &str) -> Result<JsonObject> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let rest = text
        .strip_prefix("---\n")
        .or_else(|| text.strip_prefix("---\r\n"))
        .context("SKILL.md does not start with a `---` frontmatter line")?;
    let mut offset = 0;
    let mut yaml = None;
    for line in rest.split_inclusive('\n') {
        if line.trim_end_matches(['\r', '\n']) == "---" {
            yaml = Some(&rest[..offset]);
            break;
        }
        offset += line.len();
    }
    let yaml = yaml.context("SKILL.md frontmatter has no closing `---` line")?;
    match serde_yaml::from_str::<Value>(yaml).context("SKILL.md frontmatter is not valid YAML")? {
        Value::Object(map) => Ok(map),
        _ => bail!("SKILL.md frontmatter is not a YAML mapping"),
    }
}

/// Agent Skills naming rules: 1–64 chars of lowercase ASCII letters, digits
/// and hyphens, no leading, trailing or doubled hyphen.
fn validate_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        && !name.starts_with('-')
        && !name.ends_with('-')
        && !name.contains("--");
    if !ok {
        bail!("skill name `{name}` breaks the Agent Skills naming rules");
    }
    Ok(())
}

fn mime_type(file_name: &str) -> &'static str {
    let ext = file_name
        .rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase());
    match ext.as_deref() {
        Some("md") => "text/markdown",
        Some("py") => "text/x-python",
        Some("sh") => "text/x-shellscript",
        Some("yaml") | Some("yml") => "application/yaml",
        Some("json") => "application/json",
        Some("toml") => "application/toml",
        Some("txt") | Some("tpl") => "text/plain",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn write(root: &Path, rel: &str, body: &[u8]) {
        let path = root.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, body).unwrap();
    }

    const DEMO_MD: &str = "---\nname: demo\ndescription: A demo skill\nlicense: MIT\nmetadata:\n  owner: team\n---\n\n# Demo\n";

    fn demo_root() -> TempDir {
        let tmp = TempDir::new().unwrap();
        write(tmp.path(), "demo/SKILL.md", DEMO_MD.as_bytes());
        write(tmp.path(), "demo/references/guide.md", b"# Guide\n");
        write(tmp.path(), "demo/assets/logo.bin", &[0xff, 0x00, 0xfe]);
        write(tmp.path(), "demo/.DS_Store", b"junk");
        write(
            tmp.path(),
            "demo/scripts/__pycache__/run.cpython-312.pyc",
            b"bytecode",
        );
        write(tmp.path(), "README.md", b"not part of any skill");
        tmp
    }

    #[test]
    fn entry_is_a_complete_manifest_with_verbatim_frontmatter() {
        let tmp = demo_root();
        let catalog = SkillCatalog::load(tmp.path()).unwrap();
        let got = catalog.get("skill://demo/SKILL.md").unwrap();
        let skill = &got["skill"];
        assert_eq!(skill["frontmatter"]["license"], json!("MIT"));
        assert_eq!(skill["frontmatter"]["metadata"]["owner"], json!("team"));
        let uris: Vec<&str> = skill["resources"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["uri"].as_str().unwrap())
            .collect();
        assert_eq!(
            uris,
            vec![
                "skill://demo/SKILL.md",
                "skill://demo/assets/logo.bin",
                "skill://demo/references/guide.md",
            ],
            "dotfiles, bytecode caches and files outside the skill are not part of it"
        );
        let skill_md = &skill["resources"][0];
        assert_eq!(skill_md["size"], json!(DEMO_MD.len()));
        assert_eq!(
            skill_md["digest"],
            json!(format!(
                "sha256:{}",
                hex::encode(Sha256::digest(DEMO_MD.as_bytes()))
            ))
        );
    }

    #[test]
    fn read_serves_text_verbatim_and_binary_as_blob() {
        let tmp = demo_root();
        let catalog = SkillCatalog::load(tmp.path()).unwrap();
        match catalog.read("skill://demo/SKILL.md").unwrap() {
            ResourceContents::TextResourceContents {
                text, mime_type, ..
            } => {
                assert_eq!(text, DEMO_MD);
                assert_eq!(mime_type.as_deref(), Some("text/markdown"));
            }
            other => panic!("expected text: {other:?}"),
        }
        match catalog.read("skill://demo/assets/logo.bin").unwrap() {
            ResourceContents::BlobResourceContents { blob, .. } => {
                assert_eq!(BASE64.decode(blob).unwrap(), vec![0xff, 0x00, 0xfe]);
            }
            other => panic!("expected blob: {other:?}"),
        }
        assert!(catalog.read("skill://demo/.DS_Store").is_none());
        assert!(catalog.read("skill://demo/missing.md").is_none());
    }

    #[test]
    fn unknown_skill_is_invalid_params() {
        let tmp = demo_root();
        let catalog = SkillCatalog::load(tmp.path()).unwrap();
        for uri in ["skill://nope/SKILL.md", "skill://demo/references/guide.md"] {
            let err = catalog.get(uri).unwrap_err();
            assert_eq!(err.code, rmcp::model::ErrorCode::INVALID_PARAMS, "{uri}");
            assert!(err.message.contains(uri), "{}", err.message);
        }
    }

    #[test]
    fn name_must_match_directory_and_naming_rules() {
        let tmp = TempDir::new().unwrap();
        write(
            tmp.path(),
            "wrong-dir/SKILL.md",
            b"---\nname: other\ndescription: d\n---\n",
        );
        write(
            tmp.path(),
            "Bad_Name/SKILL.md",
            b"---\nname: Bad_Name\ndescription: d\n---\n",
        );
        write(tmp.path(), "no-desc/SKILL.md", b"---\nname: no-desc\n---\n");
        write(tmp.path(), "no-fm/SKILL.md", b"# no frontmatter\n");
        write(
            tmp.path(),
            "good/SKILL.md",
            b"---\nname: good\ndescription: d\n---\n",
        );
        let catalog = SkillCatalog::load(tmp.path()).unwrap();
        let listed = catalog.list(None).unwrap();
        let uris: Vec<&str> = listed["skills"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["uri"].as_str().unwrap())
            .collect();
        assert_eq!(uris, vec!["skill://good/SKILL.md"]);
    }

    #[test]
    fn nested_skill_is_listed_flat_and_counted_in_the_enclosing_manifest() {
        let tmp = TempDir::new().unwrap();
        write(
            tmp.path(),
            "acme/outer/SKILL.md",
            b"---\nname: outer\ndescription: d\n---\n",
        );
        write(
            tmp.path(),
            "acme/outer/inner/SKILL.md",
            b"---\nname: inner\ndescription: d\n---\n",
        );
        let catalog = SkillCatalog::load(tmp.path()).unwrap();
        assert!(catalog.get("skill://acme/outer/inner/SKILL.md").is_ok());
        let outer = catalog.get("skill://acme/outer/SKILL.md").unwrap();
        let uris: Vec<&str> = outer["skill"]["resources"]
            .as_array()
            .unwrap()
            .iter()
            .map(|r| r["uri"].as_str().unwrap())
            .collect();
        assert!(
            uris.contains(&"skill://acme/outer/inner/SKILL.md"),
            "{uris:?}"
        );
    }

    #[test]
    fn list_paginates_by_offset_cursor() {
        let tmp = TempDir::new().unwrap();
        for i in 0..(PAGE_SIZE + 3) {
            let name = format!("s{i:03}");
            write(
                tmp.path(),
                &format!("{name}/SKILL.md"),
                format!("---\nname: {name}\ndescription: d\n---\n").as_bytes(),
            );
        }
        let catalog = SkillCatalog::load(tmp.path()).unwrap();
        let first = catalog.list(None).unwrap();
        assert_eq!(first["skills"].as_array().unwrap().len(), PAGE_SIZE);
        let cursor = first["nextCursor"].as_str().unwrap().to_string();
        let second = catalog.list(Some(&cursor)).unwrap();
        assert_eq!(second["skills"].as_array().unwrap().len(), 3);
        assert!(second.get("nextCursor").is_none());
        assert_eq!(second["ttlMs"], json!(TTL_MS));
        assert_eq!(second["resultType"], json!("complete"));
        assert!(catalog.list(Some("garbage")).is_err());
        assert!(catalog.list(Some("9999")).is_err());
    }

    #[test]
    fn skill_over_the_size_limit_is_skipped() {
        let tmp = TempDir::new().unwrap();
        write(tmp.path(), "demo/SKILL.md", DEMO_MD.as_bytes());
        let big = fs::File::create(tmp.path().join("demo/big.bin")).unwrap();
        big.set_len(MAX_BYTES_PER_SKILL as u64 + 1).unwrap();
        write(
            tmp.path(),
            "good/SKILL.md",
            b"---\nname: good\ndescription: d\n---\n",
        );
        let catalog = SkillCatalog::load(tmp.path()).unwrap();
        assert!(catalog.get("skill://demo/SKILL.md").is_err());
        assert!(catalog.get("skill://good/SKILL.md").is_ok());
    }

    #[test]
    fn missing_root_is_an_error() {
        let tmp = TempDir::new().unwrap();
        assert!(SkillCatalog::load(&tmp.path().join("absent")).is_err());
    }

    #[test]
    fn file_names_are_percent_encoded_into_one_uri() {
        let tmp = TempDir::new().unwrap();
        write(tmp.path(), "demo/SKILL.md", DEMO_MD.as_bytes());
        write(tmp.path(), "demo/a b#c.md", b"x");
        let catalog = SkillCatalog::load(tmp.path()).unwrap();
        assert!(catalog.read("skill://demo/a%20b%23c.md").is_some());
    }
}
