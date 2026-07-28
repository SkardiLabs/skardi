//! Optional operator-controlled sink that appends raw ad-hoc SQL to a local
//! file.
//!
//! Off unless the operator sets `--query-log <path>`. When enabled, every
//! statement the `/query` endpoint hands to the engine is appended here as one
//! JSON line (raw SQL, caller `ai_context`, `max_rows`, timestamp).
//!
//! Because this file records **raw SQL** — which may embed literal secrets or
//! PII — securing, rotating, and retaining it is the **operator's
//! responsibility**. Nothing else in the server writes query text to logs or
//! traces; this dedicated file is the only sink, so enabling it never pushes
//! query text to external OTLP collectors by accident.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::sync::Mutex;

use serde_json::{Value, json};

/// Append-only sink for raw ad-hoc SQL. Constructed once at startup when the
/// operator supplies a path; cloned across requests behind an `Arc`.
pub struct QueryLog {
    file: Mutex<File>,
}

impl QueryLog {
    /// Open (creating if needed) the query-log file for appending.
    pub fn open(path: &Path) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }

    /// Append one JSON line recording an executed statement. A write failure is
    /// logged and swallowed — a broken audit file must never fail the query.
    pub fn record(&self, sql: &str, ai_context: Option<&Value>, max_rows: usize) {
        let line = json!({
            "timestamp": chrono::Utc::now().to_rfc3339(),
            "sql": sql,
            "ai_context": ai_context,
            "max_rows": max_rows,
        });
        let mut guard = self.file.lock().unwrap_or_else(|p| p.into_inner());
        if let Err(e) = writeln!(guard, "{line}") {
            tracing::error!("Failed to append to query log: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::NamedTempFile;

    #[test]
    fn record_appends_sql_and_ai_context() {
        let tmp = NamedTempFile::new().unwrap();
        let log = QueryLog::open(tmp.path()).unwrap();

        let ctx = json!({"purpose": "kyc", "session_id": "sess-1"});
        log.record("SELECT * FROM t WHERE ssn = '123-45-6789'", Some(&ctx), 100);

        let contents = fs::read_to_string(tmp.path()).unwrap();
        assert!(
            contents.contains("SELECT * FROM t WHERE ssn = '123-45-6789'"),
            "raw SQL should be recorded: {contents}"
        );
        assert!(
            contents.contains("kyc") && contents.contains("sess-1"),
            "ai_context should be recorded: {contents}"
        );
    }

    #[test]
    fn record_appends_one_line_per_call() {
        let tmp = NamedTempFile::new().unwrap();
        let log = QueryLog::open(tmp.path()).unwrap();

        log.record("SELECT 1", None, 1);
        log.record("SELECT 2", None, 1);

        let contents = fs::read_to_string(tmp.path()).unwrap();
        assert_eq!(
            contents.lines().count(),
            2,
            "one line per record: {contents}"
        );
    }
}
