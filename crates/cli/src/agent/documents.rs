//! `skardi documents add <file>` (design 2026-10-27 §2.4.1, row 10): upload
//! one local file into a **personal** workspace as a Documents source.
//!
//! The console's upload, from a terminal, in its order:
//!
//! 1. `GET /v1/workspaces/:ws` — the workspace must be `active`;
//! 2. `POST …/saas-sources` with `saas: documents`, under the **control**
//!    token: declare the file — stored name, content type, sha256, length —
//!    before a byte moves. On a team workspace this is
//!    `403 team_upload_not_agent_callable` (§5.0 invariant 6: a grant uploads
//!    nothing a team can read), copied through;
//! 3. the gateway's data-plane `PUT /documents/:schema_name`, under the
//!    **mcp** token with `Skardi-Workspace`: the bytes, which the engine holds
//!    to the declared digest and length — so they are the very bytes that
//!    were hashed, read once before anything was declared (see
//!    [`snapshot`]). One retry on
//!    `unknown-document-source`, the engine not having caught up with the new
//!    source yet, which its next tick closes;
//! 4. `POST …/saas-sources/:source/sync` — an uploaded file has no cadence, so
//!    nothing else would ever start its run.
//!
//! The naming rules are the console's (`documentUpload.ts`,
//! `DocumentsScreen.tsx`), ported so a file uploaded from either place gets
//! the same stored name and the same source name, and the console's
//! duplicate check sees the CLI's uploads.

use super::control::Control;
use super::{AgentError, EXIT_REFUSED, EXIT_UNAVAILABLE, Outcome, shell_quote};
use crate::client::encode_component;
use crate::identity::{ClientIdentity, Surface};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The tables a Documents source declares, as the console's install does.
const DOCUMENT_TABLES: [&str; 2] = ["document", "document_section"];

/// The engine's code for "my ctx does not declare this source yet".
pub const UNKNOWN_DOCUMENT_SOURCE: &str = "unknown-document-source";

/// How long to wait before the one retry. Declaring a source restarts the
/// workspace's engine (its ctx changed), and that is tens of seconds.
pub const UPLOAD_RETRY_WAIT: Duration = Duration::from_secs(20);

/// An upload's own ceiling: a real file over whatever link the person has.
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(15 * 60);

/// The largest file this reads: 1 GiB, the contract's per-file ceiling
/// (`DOCUMENT_BYTE_LENGTH_CEILING`, the enterprise plan's cap). Every plan's
/// cap sits at or under it and the engine refuses a larger body, so a file
/// over it could never be uploaded — and it bounds the snapshot this holds.
pub const MAX_DOCUMENT_BYTES: u64 = 1024 * 1024 * 1024;

/// The accepted types, by extension: the console's `DOCUMENT_TYPES`.
const DOCUMENT_TYPES: [(&str, &str); 10] = [
    (".pdf", "application/pdf"),
    (
        ".docx",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    ),
    (
        ".xlsx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ),
    (
        ".pptx",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    ),
    (".md", "text/markdown"),
    (".markdown", "text/markdown"),
    (".txt", "text/plain"),
    (".png", "image/png"),
    (".jpg", "image/jpeg"),
    (".jpeg", "image/jpeg"),
];

/// The declared content type for a filename, or `None` when its extension is
/// not accepted.
pub fn content_type(filename: &str) -> Option<&'static str> {
    let dot = filename.rfind('.').filter(|at| *at > 0)?;
    let extension = filename[dot..].to_ascii_lowercase();
    DOCUMENT_TYPES
        .iter()
        .find(|(ext, _)| *ext == extension)
        .map(|(_, ty)| *ty)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// `foldToStorageCharset`: everything outside `[A-Za-z0-9._-]` to `-`,
/// EXCEPT a separator or a control character, which are left in place for
/// [`filename_refusal`] to refuse; runs collapsed, edges trimmed.
fn fold(filename: &str) -> String {
    let keep = |c: char| {
        c.is_ascii_alphanumeric()
            || matches!(c, '.' | '_' | '-' | '/' | '\\')
            || c.is_ascii_control()
    };
    let mut out = String::new();
    let mut in_run = false;
    for c in filename.chars() {
        if keep(c) {
            out.push(c);
            in_run = false;
        } else if !in_run {
            out.push('-');
            in_run = true;
        }
    }
    // `-{2,}` → `-`.
    let mut collapsed = String::new();
    for c in out.chars() {
        if c == '-' && collapsed.ends_with('-') {
            continue;
        }
        collapsed.push(c);
    }
    let trimmed = collapsed.trim_start_matches('-');
    // `-+(?=\.[^.]*$)`: dashes right before the last dot.
    let mut result = match trimmed.rfind('.') {
        Some(dot) => {
            let (stem, extension) = trimmed.split_at(dot);
            format!("{}{extension}", stem.trim_end_matches('-'))
        }
        None => trimmed.to_string(),
    };
    while result.ends_with('-') {
        result.pop();
    }
    result
}

/// `documentStorageFilename`: the name a file is declared and stored under.
/// An ASCII name is only folded; anything else gets a short hash of the
/// ORIGINAL name on its stem, so two names that fold alike never collide.
pub fn storage_filename(filename: &str) -> String {
    let folded = fold(filename);
    if filename.is_ascii() {
        return folded;
    }
    let (stem, extension) = match folded.rfind('.') {
        Some(dot) if !folded[dot..].contains(['/', '\\']) => folded.split_at(dot),
        _ => (folded.as_str(), ""),
    };
    let stem = stem.trim_start_matches(['.', '_', '-']);
    let tag = &sha256_hex(filename.as_bytes())[..8];
    if stem.is_empty() {
        format!("{tag}{extension}")
    } else {
        format!("{stem}-{tag}{extension}")
    }
}

/// `documentFilenameRefusal`: why a stored name cannot be used, or `None`.
pub fn filename_refusal(filename: &str) -> Option<&'static str> {
    if filename.is_empty() {
        return Some("is empty");
    }
    if filename.contains(['/', '\\']) {
        return Some("contains a path separator");
    }
    if filename.starts_with('.') {
        return Some("starts with a dot");
    }
    if filename.len() > 255 {
        return Some("is longer than 255 bytes");
    }
    if filename.chars().any(|c| c.is_ascii_control()) {
        return Some("contains a control character");
    }
    let mut chars = filename.chars();
    let first_ok = chars.next().is_some_and(|c| c.is_ascii_alphanumeric());
    if !first_ok || !chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-')) {
        return Some("contains an unsupported character");
    }
    if content_type(filename).is_none() {
        return Some("is not one of the accepted types (pdf, docx, xlsx, pptx, md, txt, png, jpg)");
    }
    None
}

/// `dnsLabelPart`: lowercased, every run outside `[a-z0-9]` one hyphen, none
/// at the ends.
fn dns_label_part(text: &str) -> String {
    let mut out = String::new();
    for c in text.to_lowercase().chars() {
        if c.is_ascii_lowercase() || c.is_ascii_digit() {
            out.push(c);
        } else if !out.ends_with('-') {
            out.push('-');
        }
    }
    out.trim_matches('-').to_string()
}

/// `documentSourceName`: the source's name, a DNS label derived from the
/// stored name, which is what skardi-global mints schema names from.
pub fn source_name(stored: &str) -> String {
    let dot = stored.rfind('.').filter(|at| *at > 0);
    let (stem, extension) = match dot {
        Some(dot) => (&stored[..dot], dns_label_part(&stored[dot + 1..])),
        None => (stored, String::new()),
    };
    let room = if extension.is_empty() {
        63
    } else {
        63usize.saturating_sub(extension.len() + 1)
    };
    let folded = dns_label_part(stem);
    let joined = if extension.is_empty() {
        folded.clone()
    } else {
        format!("{folded}-{extension}")
    };
    let reserved = joined.starts_with("src-");
    let part = if reserved {
        format!("doc-{folded}")
    } else {
        folded
    };
    let label = if !reserved && part.len() <= room {
        part
    } else {
        let cut: String = part.chars().take(room.saturating_sub(9)).collect();
        format!(
            "{}-{}",
            cut.trim_end_matches('-'),
            &sha256_hex(stored.as_bytes())[..8]
        )
    };
    let base = if label.is_empty() {
        "document".to_string()
    } else {
        label
    };
    if extension.is_empty() {
        base
    } else {
        format!("{base}-{extension}")
    }
}

/// Where the bytes go: the gateway's data plane, under the context's query
/// token. `Debug` is hand-written: it carries that token.
pub struct DataPlane {
    pub gateway: String,
    pub workspace: String,
    pub mcp_token: String,
    pub retry_wait: Duration,
}

impl std::fmt::Debug for DataPlane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataPlane")
            .field("gateway", &self.gateway)
            .field("workspace", &self.workspace)
            .field("mcp_token", &"(redacted)")
            .finish()
    }
}

/// What the file is, as declared: its names, type, digest and length.
struct Declared {
    display_label: String,
    stored: String,
    source_name: String,
    content_type: &'static str,
    sha256: String,
    byte_length: u64,
}

/// The file, read once: its declaration and the very bytes it describes.
///
/// One read, not one to hash and another to send. The engine holds the
/// upload to the declared digest, so a file that changed between two reads
/// was refused only after its source had been created, stranding the
/// source. Sending what was hashed makes a later change irrelevant, and a
/// change DURING the read is caught before anything is created.
struct Snapshot {
    declared: Declared,
    bytes: Vec<u8>,
}

/// Read and hash the file off the async runtime (AGENTS.md: no blocking I/O
/// in async code) — a large file is seconds of disk and SHA-256.
async fn snapshot(path: &Path) -> Result<Snapshot, AgentError> {
    let owned: PathBuf = path.to_path_buf();
    tokio::task::spawn_blocking(move || read_snapshot(&owned, || {}))
        .await
        .map_err(|err| {
            AgentError::usage(
                "file_unreadable",
                format!("reading {} failed: {err}", path.display()),
            )
        })?
}

/// The blocking half of [`snapshot`]. `between` runs after the read and
/// before the check that the file held still, so a test can change it there.
fn read_snapshot(path: &Path, between: impl FnOnce()) -> Result<Snapshot, AgentError> {
    let unreadable = |err: std::io::Error| {
        AgentError::usage(
            "file_unreadable",
            format!("cannot read {}: {err}", path.display()),
        )
    };
    let display_label = path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .ok_or_else(|| {
            AgentError::usage(
                "file_unreadable",
                format!("{} names no file", path.display()),
            )
        })?;
    let stored = storage_filename(&display_label);
    if let Some(reason) = filename_refusal(&stored) {
        return Err(AgentError::usage(
            "unsupported_filename",
            format!("cannot upload {display_label}: its stored name '{stored}' {reason}"),
        ));
    }
    let content_type = content_type(&stored).expect("filename_refusal checked the type");

    let too_large = || {
        AgentError::usage(
            "file_too_large",
            format!(
                "{display_label} is larger than {} MiB, the most any plan accepts for one file",
                MAX_DOCUMENT_BYTES / (1024 * 1024)
            ),
        )
    };
    let file = std::fs::File::open(path).map_err(unreadable)?;
    let before = file.metadata().map_err(unreadable)?;
    if before.len() > MAX_DOCUMENT_BYTES {
        return Err(too_large());
    }
    let mut bytes = Vec::with_capacity(usize::try_from(before.len()).unwrap_or(0));
    // `take` one past the ceiling: a file growing while it is read is caught
    // by the check below, and is never read without bound.
    file.take(MAX_DOCUMENT_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(unreadable)?;
    if bytes.len() as u64 > MAX_DOCUMENT_BYTES {
        return Err(too_large());
    }
    between();
    // Read by PATH, after: an edit in place changes the length or the mtime,
    // and a replace-by-rename hands the path a different file. Either way
    // the bytes read may be a mix of before and after, which is not a
    // document anyone wrote — so nothing is declared from them.
    let after = std::fs::metadata(path).map_err(unreadable)?;
    let held_still = after.len() == bytes.len() as u64
        && before.len() == after.len()
        && before.modified().ok() == after.modified().ok();
    if !held_still {
        return Err(AgentError::usage(
            "file_changed",
            format!(
                "{} changed while it was being read, so nothing was uploaded or created. Upload it once nothing is writing to it",
                path.display()
            ),
        ));
    }
    let sha256 = sha256_hex(&bytes);
    Ok(Snapshot {
        declared: Declared {
            source_name: source_name(&stored),
            display_label,
            stored,
            content_type,
            sha256,
            byte_length: bytes.len() as u64,
        },
        bytes,
    })
}

/// Row 10.
pub async fn add(control: &Control, data: &DataPlane, path: &Path) -> Result<Outcome, AgentError> {
    let Snapshot { declared, bytes } = snapshot(path).await?;

    // ① The workspace must be serving. Asked first because a declaration
    // against a provisioning workspace would be refused anyway, with a
    // message about the install rather than about the workspace.
    let workspace = control.get("", &[]).await?;
    let state = workspace
        .get("provisioning_state")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    if state != "active" {
        return Err(AgentError::local(
            EXIT_REFUSED,
            "workspace_not_active",
            format!("the workspace is {state}, not active; upload once it is serving"),
        ));
    }

    // ② Declare. The uploader is stamped by the server from the caller.
    let body = json!({
        "source_name": declared.source_name,
        "saas": "documents",
        "upstream_resource": {
            "filename": declared.stored,
            "contentType": declared.content_type,
            "sha256": declared.sha256,
        },
        "byte_length": declared.byte_length,
        "display_label": declared.display_label,
        "tables": DOCUMENT_TABLES,
    });
    let source = control.post("/saas-sources", Some(&body), &[]).await?;
    let source_name = source
        .get("source_name")
        .and_then(Value::as_str)
        .unwrap_or(&declared.source_name)
        .to_string();
    let Some(schema_name) = source.get("schema_name").and_then(Value::as_str) else {
        return Err(AgentError::local(
            EXIT_UNAVAILABLE,
            "invalid_response",
            "the control plane declared the document but named no schema to upload it under",
        ));
    };

    // ③ The bytes: the snapshot's, which are what was declared.
    let mut attempt = 0;
    loop {
        match upload(data, schema_name, declared.content_type, bytes.clone()).await {
            Ok(()) => break,
            Err((err, retryable)) if retryable && attempt == 0 => {
                attempt += 1;
                retry_note(&err);
                tokio::time::sleep(data.retry_wait).await;
            }
            Err((err, _)) => return Err(stranded(&source_name, err)),
        }
    }

    // ④ Start the run.
    let sync = control
        .post(
            &format!("/saas-sources/{}/sync", encode_component(&source_name)),
            None,
            &[],
        )
        .await
        .map_err(|err| {
            err.with_next(format!(
                "skardi integrations sync {}",
                shell_quote(&source_name)
            ))
        })?;
    let result = json!({
        "source": source,
        "upload": {
            "filename": declared.stored,
            "content_type": declared.content_type,
            "sha256": declared.sha256,
            "byte_length": declared.byte_length,
        },
        "sync": sync,
    });
    Ok(Outcome::new(
        result,
        format!(
            "uploaded {} as source {source_name}, and asked for its first run",
            declared.display_label
        ),
    )
    .with_next(format!(
        "skardi integrations status {}",
        shell_quote(&source_name)
    )))
}

/// The retry wait is the engine restarting; say so on stderr rather than
/// leave a twenty-second pause unexplained. A function so the one place that
/// prints during a command is easy to find.
fn retry_note(err: &AgentError) {
    eprintln!(
        "note: the workspace engine has not picked up the new source yet ({}); retrying once",
        err.code()
    );
}

/// A failure after the source was declared leaves a source with no file. The
/// agent cannot delete one (teardown is not on the control table), so the
/// message says who can.
fn stranded(source_name: &str, err: AgentError) -> AgentError {
    let message = format!(
        "{} — the source {source_name} exists without its file; remove it in the console (Integrations → Documents) before uploading again",
        err.message()
    );
    let mut err = err;
    if let Some(object) = err.error.as_object_mut() {
        object.insert("message".to_string(), json!(message));
        object.insert("source_name".to_string(), json!(source_name));
    }
    err
}

/// One `PUT /documents/:schema_name`. `Err((error, retryable))`.
async fn upload(
    data: &DataPlane,
    schema_name: &str,
    content_type: &str,
    bytes: Vec<u8>,
) -> Result<(), (AgentError, bool)> {
    let url = format!(
        "{}/documents/{}",
        data.gateway.trim_end_matches('/'),
        encode_component(schema_name)
    );
    let local =
        |code: &str, message: String| (AgentError::local(EXIT_UNAVAILABLE, code, message), false);
    let http = reqwest::Client::builder()
        .no_proxy()
        .timeout(UPLOAD_TIMEOUT)
        .default_headers(ClientIdentity::from_env(Surface::Cli).headers())
        .build()
        .map_err(|err| local("client_error", format!("{err}")))?;
    let mut auth = HeaderValue::from_str(&format!("Bearer {}", data.mcp_token)).map_err(|_| {
        (
            AgentError::usage(
                "token_unsendable",
                "the context's query token cannot be sent in an HTTP header",
            ),
            false,
        )
    })?;
    auth.set_sensitive(true);
    let response = http
        .put(&url)
        .header(AUTHORIZATION, auth)
        .header(crate::client::WORKSPACE_HEADER, data.workspace.as_str())
        .header(CONTENT_TYPE, content_type)
        .body(bytes)
        .send()
        .await
        .map_err(|err| {
            (
                AgentError::local(
                    EXIT_UNAVAILABLE,
                    "gateway_unreachable",
                    format!("cannot reach the gateway at {url}: {err}"),
                ),
                true,
            )
        })?;
    let status = response.status().as_u16();
    if (200..300).contains(&status) {
        return Ok(());
    }
    let text = response.text().await.unwrap_or_default();
    let err = data_plane_refusal(status, &text);
    let retryable = err.code() == UNKNOWN_DOCUMENT_SOURCE || matches!(status, 502..=504);
    Err((err, retryable))
}

/// The data plane's refusal as an [`AgentError`].
///
/// The engine answers `{"success": false, "code", "error"}` and the gateway
/// `{"error", "error_type"}` — neither is the control plane's envelope — and a
/// gateway refusal relayed from skardi-global is that nested one. All three
/// become `{code, message}`, keeping any other field.
pub fn data_plane_refusal(status: u16, text: &str) -> AgentError {
    let exit = if status >= 500 {
        EXIT_UNAVAILABLE
    } else {
        EXIT_REFUSED
    };
    let parsed: Option<Value> = serde_json::from_str(text).ok();
    let error = match parsed {
        Some(Value::Object(mut body)) => {
            if let Some(Value::Object(nested)) = body.get("error").cloned()
                && nested.contains_key("code")
            {
                Value::Object(nested)
            } else {
                let code = body
                    .remove("code")
                    .or_else(|| body.remove("error_type"))
                    .and_then(|c| c.as_str().map(str::to_string))
                    .unwrap_or_else(|| format!("http_{status}"));
                let message = body
                    .remove("error")
                    .or_else(|| body.remove("message"))
                    .and_then(|m| m.as_str().map(str::to_string))
                    .unwrap_or_else(|| code.clone());
                body.remove("success");
                body.insert("code".to_string(), json!(code));
                body.insert("message".to_string(), json!(message));
                Value::Object(body)
            }
        }
        _ => json!({
            "code": format!("http_{status}"),
            "message": text.lines().next().unwrap_or_default().chars().take(200).collect::<String>(),
        }),
    };
    AgentError {
        exit,
        error,
        next: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{EXIT_REFUSED, EXIT_UNAVAILABLE};
    use rstest::rstest;
    use tempfile::TempDir;
    use wiremock::matchers::{header, method, path as path_is};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const CONTROL: &str = "skardi_pat_control_secret";
    const MCP: &str = "skardi_pat_mcp_token";
    const WS: &str = "/control/v1/workspaces/acme-prod";

    /// The console's own cases (`documentUpload.test.ts`, `DocumentsScreen`),
    /// so a file uploaded from either place is named the same.
    #[rstest]
    #[case::plain("report.pdf", "report.pdf", "report-pdf")]
    #[case::space_and_parens("Q3 (final).pdf", "Q3-final.pdf", "q3-final-pdf")]
    #[case::chen("Chen CRA.docx", "Chen-CRA.docx", "chen-cra-docx")]
    fn names_follow_the_consoles_rules(
        #[case] picked: &str,
        #[case] stored: &str,
        #[case] source: &str,
    ) {
        assert_eq!(storage_filename(picked), stored);
        assert_eq!(source_name(stored), source);
    }

    /// `src-` belongs to model-2 sources, so a document named into it is
    /// moved out, and always hashed so it cannot collide with a document
    /// that was already called `doc-src-…`.
    #[test]
    fn the_reserved_prefix_is_moved_aside_and_hashed() {
        let name = source_name("src.pdf");
        assert!(
            name.starts_with("doc-src-") && name.ends_with("-pdf"),
            "{name}"
        );
        assert_eq!(name.len(), "doc-src-".len() + 8 + "-pdf".len(), "{name}");
    }

    #[test]
    fn a_long_name_is_cut_to_a_dns_label_with_a_hash() {
        let stored = format!("{}.pdf", "a".repeat(100));
        let name = source_name(&stored);
        assert!(name.len() <= 63, "{name}");
        assert!(name.ends_with("-pdf"), "{name}");
    }

    #[test]
    fn a_non_ascii_name_keeps_a_hash_of_itself() {
        let stored = storage_filename("39系列金属架.pdf");
        assert!(
            stored.starts_with("39-") && stored.ends_with(".pdf"),
            "{stored}"
        );
        assert_ne!(stored, storage_filename("39系列金属柜.pdf"));
        assert_eq!(filename_refusal(&stored), None);
    }

    #[rstest]
    #[case::separator("a/b.pdf")]
    #[case::dotfile(".env")]
    #[case::wrong_type("archive.zip")]
    #[case::no_extension("README")]
    fn refused_names(#[case] name: &str) {
        assert!(
            filename_refusal(&storage_filename(name)).is_some(),
            "{name}"
        );
    }

    #[rstest]
    #[case::pdf("a.pdf", Some("application/pdf"))]
    #[case::upper("A.PNG", Some("image/png"))]
    #[case::markdown("a.markdown", Some("text/markdown"))]
    #[case::unknown("a.gif", None)]
    fn content_types(#[case] name: &str, #[case] expected: Option<&str>) {
        assert_eq!(content_type(name), expected);
    }

    fn write_file(dir: &TempDir, name: &str, contents: &[u8]) -> PathBuf {
        let path = dir.path().join(name);
        std::fs::write(&path, contents).unwrap();
        path
    }

    async fn gateway() -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_is(WS))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    json!({"tenant_slug": "acme-prod", "provisioning_state": "active"}),
                ),
            )
            .mount(&server)
            .await;
        server
    }

    fn sides(server: &MockServer) -> (Control, DataPlane) {
        (
            Control::new(&server.uri(), "acme-prod", CONTROL).unwrap(),
            DataPlane {
                gateway: server.uri(),
                workspace: "acme-prod".to_string(),
                mcp_token: MCP.to_string(),
                retry_wait: Duration::from_millis(1),
            },
        )
    }

    /// The whole sequence: declare under the control token, upload under the
    /// mcp token with the workspace header, then sync.
    #[tokio::test]
    async fn add_declares_uploads_with_the_mcp_token_and_syncs() {
        let server = gateway().await;
        Mock::given(method("POST"))
            .and(path_is(format!("{WS}/saas-sources")))
            .and(header(
                "authorization",
                format!("Bearer {CONTROL}").as_str(),
            ))
            .respond_with(
                ResponseTemplate::new(201).set_body_json(
                    json!({"source_name": "notes-md", "schema_name": "doc_notes_md"}),
                ),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path_is("/documents/doc_notes_md"))
            .and(header("authorization", format!("Bearer {MCP}").as_str()))
            .and(header("skardi-workspace", "acme-prod"))
            .and(header("content-type", "text/markdown"))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({"success": true})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path_is(format!("{WS}/saas-sources/notes-md/sync")))
            .and(header(
                "authorization",
                format!("Bearer {CONTROL}").as_str(),
            ))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({})))
            .expect(1)
            .mount(&server)
            .await;

        let dir = TempDir::new().unwrap();
        let file = write_file(&dir, "notes.md", b"# hello\n");
        let (control, data) = sides(&server);
        let outcome = add(&control, &data, &file).await.unwrap();
        assert_eq!(
            outcome.next.as_deref(),
            Some("skardi integrations status notes-md")
        );

        let seen = server.received_requests().await.unwrap();
        let declared: Value = seen
            .iter()
            .find(|r| r.url.path() == format!("{WS}/saas-sources"))
            .map(|r| serde_json::from_slice(&r.body).unwrap())
            .unwrap();
        assert_eq!(declared["saas"], "documents");
        assert_eq!(declared["source_name"], "notes-md");
        assert_eq!(declared["byte_length"], 8);
        assert_eq!(declared["display_label"], "notes.md");
        assert_eq!(declared["upstream_resource"]["filename"], "notes.md");
        assert_eq!(
            declared["upstream_resource"]["contentType"],
            "text/markdown"
        );
        assert_eq!(
            declared["upstream_resource"]["sha256"],
            sha256_hex(b"# hello\n")
        );
        assert_eq!(declared["tables"], json!(["document", "document_section"]));
        let put = seen
            .iter()
            .find(|r| r.method == wiremock::http::Method::PUT)
            .unwrap();
        assert_eq!(put.body, b"# hello\n");
        // The control token never went to the data plane.
        assert!(
            !put.headers
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap()
                .contains(CONTROL)
        );
    }

    #[tokio::test]
    async fn a_team_workspace_is_refused_at_the_declaration_and_nothing_is_uploaded() {
        let server = gateway().await;
        Mock::given(method("POST"))
            .and(path_is(format!("{WS}/saas-sources")))
            .respond_with(ResponseTemplate::new(403).set_body_json(json!({"error": {"code": "team_upload_not_agent_callable", "message": "upload it in the console"}})))
            .mount(&server)
            .await;
        let dir = TempDir::new().unwrap();
        let file = write_file(&dir, "notes.md", b"x");
        let (control, data) = sides(&server);
        let err = add(&control, &data, &file).await.unwrap_err();
        assert_eq!(err.code(), "team_upload_not_agent_callable");
        assert_eq!(err.exit, EXIT_REFUSED);
        let seen = server.received_requests().await.unwrap();
        assert!(seen.iter().all(|r| r.method != wiremock::http::Method::PUT));
    }

    /// One retry on the engine not having caught up, and only one.
    #[rstest]
    #[case::recovers(1, true)]
    #[case::gives_up(2, false)]
    #[tokio::test]
    async fn an_unknown_document_source_is_retried_once(
        #[case] refusals: u64,
        #[case] succeeds: bool,
    ) {
        let server = gateway().await;
        Mock::given(method("POST"))
            .and(path_is(format!("{WS}/saas-sources")))
            .respond_with(
                ResponseTemplate::new(201).set_body_json(
                    json!({"source_name": "notes-md", "schema_name": "doc_notes_md"}),
                ),
            )
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({"success": false, "code": "unknown-document-source", "error": "no such source"})))
            .up_to_n_times(refusals)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({"success": true})))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path_is(format!("{WS}/saas-sources/notes-md/sync")))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({})))
            .mount(&server)
            .await;
        let dir = TempDir::new().unwrap();
        let file = write_file(&dir, "notes.md", b"x");
        let (control, data) = sides(&server);
        let result = add(&control, &data, &file).await;
        assert_eq!(result.is_ok(), succeeds, "{result:?}");
        if let Err(err) = result {
            assert_eq!(err.code(), UNKNOWN_DOCUMENT_SOURCE);
            assert!(err.message().contains("exists without its file"), "{err:?}");
        }
        let puts = server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.method == wiremock::http::Method::PUT)
            .count();
        assert_eq!(puts, 2);
    }

    #[rstest]
    #[case::engine(
        422,
        r#"{"success":false,"code":"digest-mismatch","error":"the body does not match"}"#,
        "digest-mismatch",
        EXIT_REFUSED
    )]
    #[case::gateway(
        402,
        r#"{"error":"lapsed","error_type":"subscription_inactive","org_slug":"acme"}"#,
        "subscription_inactive",
        EXIT_REFUSED
    )]
    #[case::nested(
        403,
        r#"{"error":{"code":"not-the-uploader","message":"no"}}"#,
        "not-the-uploader",
        EXIT_REFUSED
    )]
    #[case::html(502, "<html>", "http_502", EXIT_UNAVAILABLE)]
    fn data_plane_refusals_become_one_shape(
        #[case] status: u16,
        #[case] body: &str,
        #[case] code: &str,
        #[case] exit: u8,
    ) {
        let err = data_plane_refusal(status, body);
        assert_eq!(err.code(), code);
        assert_eq!(err.exit, exit);
        assert!(err.error.get("message").is_some());
        assert!(err.error.get("success").is_none());
    }

    /// Rewrites the file the moment its declaration arrives — the window
    /// between hashing and uploading that a second read used to leave open
    /// — then answers as the control plane does.
    struct RewriteOnDeclare {
        file: PathBuf,
    }

    impl wiremock::Respond for RewriteOnDeclare {
        fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
            std::fs::write(&self.file, b"# rewritten after it was declared\n").unwrap();
            ResponseTemplate::new(201)
                .set_body_json(json!({"source_name": "notes-md", "schema_name": "doc_notes_md"}))
        }
    }

    /// Codex on #311: the file was read once to hash it and again to send
    /// it, so a change in between uploaded bytes that did not match the
    /// declared digest, after the source existed. Now the bytes sent are the
    /// bytes hashed, whatever happens to the file after.
    #[tokio::test]
    async fn the_upload_sends_the_bytes_that_were_declared_even_if_the_file_changes() {
        let server = gateway().await;
        let dir = TempDir::new().unwrap();
        let file = write_file(&dir, "notes.md", b"# hello\n");
        Mock::given(method("POST"))
            .and(path_is(format!("{WS}/saas-sources")))
            .respond_with(RewriteOnDeclare { file: file.clone() })
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path_is("/documents/doc_notes_md"))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({"success": true})))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path_is(format!("{WS}/saas-sources/notes-md/sync")))
            .respond_with(ResponseTemplate::new(202).set_body_json(json!({})))
            .mount(&server)
            .await;

        let (control, data) = sides(&server);
        add(&control, &data, &file).await.unwrap();

        let seen = server.received_requests().await.unwrap();
        let declared: Value = seen
            .iter()
            .find(|r| r.url.path() == format!("{WS}/saas-sources"))
            .map(|r| serde_json::from_slice(&r.body).unwrap())
            .unwrap();
        let put = seen
            .iter()
            .find(|r| r.method == wiremock::http::Method::PUT)
            .unwrap();
        assert_eq!(put.body, b"# hello\n");
        assert_eq!(
            declared["upstream_resource"]["sha256"],
            sha256_hex(&put.body)
        );
        assert_eq!(declared["byte_length"], put.body.len());
        assert!(std::fs::read(&file).unwrap().starts_with(b"# rewritten"));
    }

    /// A file written to WHILE it is read is a mix of two versions: refused,
    /// with a sentence saying so, before anything is declared.
    #[rstest]
    #[case::appended(|file: &Path| {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new().append(true).open(file).unwrap();
        f.write_all(b"more").unwrap();
    })]
    #[case::replaced(|file: &Path| {
        let swap = file.with_extension("swap");
        std::fs::write(&swap, b"a different document, longer").unwrap();
        std::fs::rename(&swap, file).unwrap();
    })]
    fn a_file_that_changes_while_it_is_read_is_refused(#[case] change: fn(&Path)) {
        let dir = TempDir::new().unwrap();
        let file = write_file(&dir, "notes.md", b"# hello\n");
        let Err(err) = read_snapshot(&file, || change(&file)) else {
            panic!("a changing file was snapshotted");
        };
        assert_eq!(err.code(), "file_changed");
        assert_eq!(err.exit, crate::agent::EXIT_USAGE);
        assert!(
            err.message().contains("changed while it was being read"),
            "{err:?}"
        );
        assert!(
            err.message().contains("nothing was uploaded or created"),
            "{err:?}"
        );
    }

    #[test]
    fn a_snapshot_is_the_file_and_its_digest() {
        let dir = TempDir::new().unwrap();
        let file = write_file(&dir, "Q3 (final).pdf", b"%PDF-1.7");
        let Ok(snapshot) = read_snapshot(&file, || {}) else {
            panic!("a still file is snapshotted");
        };
        assert_eq!(snapshot.bytes, b"%PDF-1.7");
        assert_eq!(snapshot.declared.sha256, sha256_hex(b"%PDF-1.7"));
        assert_eq!(snapshot.declared.byte_length, 8);
        assert_eq!(snapshot.declared.display_label, "Q3 (final).pdf");
        assert_eq!(snapshot.declared.stored, "Q3-final.pdf");
        assert_eq!(snapshot.declared.source_name, "q3-final-pdf");
        assert_eq!(snapshot.declared.content_type, "application/pdf");
    }

    /// Over the largest per-file cap any plan has, refused from its length
    /// alone — the file is sparse, so nothing is read or allocated.
    #[tokio::test]
    async fn a_file_over_the_ceiling_is_refused_before_it_is_read() {
        let server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let file = dir.path().join("huge.pdf");
        std::fs::File::create(&file)
            .unwrap()
            .set_len(MAX_DOCUMENT_BYTES + 1)
            .unwrap();
        let (control, data) = sides(&server);
        let err = add(&control, &data, &file).await.unwrap_err();
        assert_eq!(err.code(), "file_too_large");
        assert!(err.message().contains("1024 MiB"), "{err:?}");
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_missing_file_issues_no_request() {
        let server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let (control, data) = sides(&server);
        let err = add(&control, &data, &dir.path().join("gone.md"))
            .await
            .unwrap_err();
        assert_eq!(err.code(), "file_unreadable");
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_unsupported_file_issues_no_request() {
        let server = MockServer::start().await;
        let dir = TempDir::new().unwrap();
        let file = write_file(&dir, "archive.zip", b"PK");
        let (control, data) = sides(&server);
        let err = add(&control, &data, &file).await.unwrap_err();
        assert_eq!(err.code(), "unsupported_filename");
        assert!(server.received_requests().await.unwrap().is_empty());
    }
}
