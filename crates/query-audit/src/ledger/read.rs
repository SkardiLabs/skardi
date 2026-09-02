//! The read half: one page of one workspace's rows, newest-first over the
//! stable `(created_at, id)` keyset.
//!
//! The route owns authorization (envelope + admin re-check); this module
//! owns semantics: filters, the cursor, the row cap (≤ 500, default 100),
//! and the 8 MiB response byte cap — rows are elided from the tail and the
//! response says so (`truncated: true` + `next_cursor`), because 500 rows of
//! even ingestion-bounded fields can pass the `/data_source` budget.

use std::error::Error;
use std::fmt;

use base64::Engine as _;
use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use sqlx::postgres::PgRow;
use sqlx::{Error as SqlxError, PgPool, Row};

use super::{ERROR_MAX_BYTES, SQL_MAX_BYTES, queries};

pub const LIMIT_DEFAULT: i64 = 100;
pub const LIMIT_MAX: i64 = 500;

/// The whole response body stays under this. The consumer's relay enforces
/// the same 8 MiB as a hard cap and answers 413 above it, so this is a HARD
/// wire contract, not a soft target: a body that lands even one byte over
/// turns into a deterministic 413 — and the same cursor fetches the same
/// page, so the caller is livelocked until the rows age out.
pub const RESPONSE_MAX_BYTES: usize = 8 * 1024 * 1024;

/// Read-side bound on any single text field. The DDL deliberately carries
/// no length constraints (ingestion bounds live at assembly), so a row
/// written PAST the assembly — by hand, by another tool, by a future writer
/// with a bug — can be arbitrarily large. The read path re-applies the
/// ingestion bounds when serializing, which makes the 8 MiB body budget
/// STRUCTURAL: every emitted row is ≤ ~50 KiB, so even the page's first row
/// can never overflow the budget, and the keyset always advances past a
/// monster row instead of livelocking the cursor on it.
const READ_FIELD_MAX_BYTES: usize = 4 * 1024;

/// Headroom [`list_page`] holds back from [`RESPONSE_MAX_BYTES`] for
/// everything that is not a row: the envelope framing
/// (`{"success":…,"rows":[…],"truncated":…,"next_cursor":"…"}`, with the
/// cursor well under 100 bytes) plus the `rows` array's brackets. The
/// per-row comma is counted per row instead, so this only has to cover the
/// fixed part — 1 KiB is an order of magnitude more than it needs, and one
/// elided row against a 32 KiB-sql worst case is noise.
const ENVELOPE_RESERVE_BYTES: usize = 1024;

/// Parsed, validated query inputs. The route builds this from the query
/// string; `workspace` always comes from the envelope.
#[derive(Debug, Default)]
pub struct PageQuery {
    pub session_id: Option<String>,
    pub status: Option<String>,
    pub since: Option<DateTime<Utc>>,
    pub until: Option<DateTime<Utc>>,
    pub limit: Option<i64>,
    pub cursor: Option<String>,
}

/// Hand-rolled rather than derived: this crate deliberately carries no
/// `thiserror` (its only other error surface is `anyhow` + one bespoke
/// timeout enum), and two variants do not justify the dependency.
#[derive(Debug)]
pub enum ReadError {
    /// Caller-shaped: a malformed filter/cursor (400).
    BadRequest(String),
    /// Backend-shaped: PG unreachable or the query failed (503; the ledger
    /// is degraded, the caller should retry).
    Unavailable(SqlxError),
}

impl fmt::Display for ReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadError::BadRequest(m) => write!(f, "{m}"),
            ReadError::Unavailable(_) => write!(f, "ledger read failed"),
        }
    }
}

impl Error for ReadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            ReadError::BadRequest(_) => None,
            ReadError::Unavailable(e) => Some(e),
        }
    }
}

/// The opaque cursor: base64 of `<created_at micros>:<id>`. Opaque to
/// callers by contract — the encoding may change; only round-tripping a
/// returned value is supported.
fn encode_cursor(created_at: DateTime<Utc>, id: i64) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(format!(
        "{}:{}",
        created_at.timestamp_micros(),
        id
    ))
}

fn decode_cursor(cursor: &str) -> Result<(DateTime<Utc>, i64), ReadError> {
    let bad = || ReadError::BadRequest("cursor is not a value a prior response returned".into());
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| bad())?;
    let s = String::from_utf8(bytes).map_err(|_| bad())?;
    let (t, id) = s.split_once(':').ok_or_else(bad)?;
    let micros: i64 = t.parse().map_err(|_| bad())?;
    let id: i64 = id.parse().map_err(|_| bad())?;
    let created_at = DateTime::<Utc>::from_timestamp_micros(micros).ok_or_else(bad)?;
    Ok((created_at, id))
}

/// One page. Returns the JSON body the consumer's route serves verbatim.
pub async fn list_page(pool: &PgPool, workspace: &str, q: PageQuery) -> Result<Value, ReadError> {
    let limit = match q.limit {
        None => LIMIT_DEFAULT,
        Some(n) if n < 1 => {
            return Err(ReadError::BadRequest("limit must be >= 1".into()));
        }
        Some(n) => n.min(LIMIT_MAX),
    };
    let (cursor_at, cursor_id) = match &q.cursor {
        Some(c) => decode_cursor(c)?,
        // First page: a keyset upper bound above every real row, so the one
        // prepared statement serves both cases (queries::SELECT_PAGE).
        None => (DateTime::<Utc>::MAX_UTC, i64::MAX),
    };

    let rows = sqlx::query(queries::SELECT_PAGE)
        .bind(workspace)
        .bind(cursor_at)
        .bind(cursor_id)
        .bind(q.since)
        .bind(q.until)
        .bind(&q.session_id)
        .bind(&q.status)
        .bind(limit)
        .fetch_all(pool)
        .await
        .map_err(ReadError::Unavailable)?;

    let mut out: Vec<Value> = Vec::with_capacity(rows.len());
    let mut bytes = 0usize;
    let mut truncated = false;
    let mut last_key: Option<(DateTime<Utc>, i64)> = None;
    let fetched = rows.len();
    for row in rows {
        // `try_get`, never `get`: `get` PANICS on a missing column, a type
        // mismatch, or a decode failure — a momentary migration/engine skew
        // downstream must surface as the designed 503, not kill the task.
        let obj = serialize_row(&row).map_err(ReadError::Unavailable)?;
        let id = obj["id"].as_i64().expect("serialize_row sets id");
        let created_at = obj["__created_at_key"]
            .as_i64()
            .expect("serialize_row sets the key");
        let created_at =
            DateTime::<Utc>::from_timestamp_micros(created_at).expect("stored timestamp");
        let obj = strip_key(obj);
        // Byte cap: elide from the tail once the budget is spent. The
        // budget is the WHOLE body's, not the rows': the consumer's relay
        // 413s one byte over `RESPONSE_MAX_BYTES` and the same cursor
        // re-fetches the same page, so an over-budget body is a permanent
        // 413. Each row is charged its serialized length plus its
        // separating comma; the fixed framing comes out of the reserve.
        // `serialize_row` bounds every field ([`READ_FIELD_MAX_BYTES`]), so
        // even the FIRST row fits and the loop always makes progress.
        let row_bytes = obj.to_string().len() + 1;
        if bytes + row_bytes > RESPONSE_MAX_BYTES - ENVELOPE_RESERVE_BYTES && !out.is_empty() {
            truncated = true;
            break;
        }
        bytes += row_bytes;
        last_key = Some((created_at, id));
        out.push(obj);
    }
    // A full fetch means the keyset may continue; an elided tail always does.
    let more_may_exist = truncated || fetched == limit as usize;
    let next_cursor = match (more_may_exist, last_key) {
        (true, Some((at, id))) => Some(encode_cursor(at, id)),
        _ => None,
    };

    Ok(json!({
        "success": true,
        "rows": out,
        "truncated": truncated,
        "next_cursor": next_cursor,
    }))
}

/// One row → the wire object, every step fallible and every text field
/// bounded (see [`READ_FIELD_MAX_BYTES`]). Carries the keyset key in a
/// private `__created_at_key` member that [`strip_key`] removes, so the
/// caller never sees it and the loop never re-parses RFC 3339.
fn serialize_row(row: &PgRow) -> Result<Value, SqlxError> {
    fn bound(s: String, max: usize) -> String {
        if s.len() <= max {
            return s;
        }
        let mut end = max;
        while end > 0 && !s.is_char_boundary(end) {
            end -= 1;
        }
        s[..end].to_string()
    }
    let created_at: DateTime<Utc> = row.try_get("created_at")?;
    let finished_at: DateTime<Utc> = row.try_get("finished_at")?;
    let sql: String = row.try_get("sql")?;
    let sql_over = sql.len() > SQL_MAX_BYTES;
    let ai_context: Option<Value> = row.try_get("ai_context")?;
    // The same drop-not-truncate rule as ingestion: an over-bound document
    // (only writable past the assembly) is elided whole.
    let ai_context = ai_context.filter(|c| {
        serde_json::to_vec(c)
            .map(|v| v.len() <= ERROR_MAX_BYTES)
            .unwrap_or(false)
    });
    Ok(json!({
        "id": row.try_get::<i64, _>("id")?,
        "org_id": bound(row.try_get("org_id")?, READ_FIELD_MAX_BYTES),
        "workspace_id": bound(row.try_get("workspace_id")?, READ_FIELD_MAX_BYTES),
        "user_id": bound(row.try_get("user_id")?, READ_FIELD_MAX_BYTES),
        "request_id": bound(row.try_get("request_id")?, READ_FIELD_MAX_BYTES),
        "session_id": row
            .try_get::<Option<String>, _>("session_id")?
            .map(|s| bound(s, READ_FIELD_MAX_BYTES)),
        "created_at": created_at.to_rfc3339(),
        "finished_at": finished_at.to_rfc3339(),
        "sql": bound(sql, SQL_MAX_BYTES),
        // True when ingestion truncated it OR the read had to: either way
        // the reader is told the text is not the whole statement.
        "sql_truncated": row.try_get::<bool, _>("sql_truncated")? || sql_over,
        "ai_context": ai_context,
        "statement_kind": bound(row.try_get("statement_kind")?, READ_FIELD_MAX_BYTES),
        "max_rows": row.try_get::<i64, _>("max_rows")?,
        "status": bound(row.try_get("status")?, READ_FIELD_MAX_BYTES),
        "row_count": row.try_get::<Option<i64>, _>("row_count")?,
        "error": row
            .try_get::<Option<String>, _>("error")?
            .map(|e| bound(e, ERROR_MAX_BYTES)),
        "__created_at_key": created_at.timestamp_micros(),
    }))
}

fn strip_key(mut obj: Value) -> Value {
    obj.as_object_mut()
        .expect("serialize_row emits an object")
        .remove("__created_at_key");
    obj
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The hand-rolled error impls (no `thiserror` in this crate): Display
    /// keeps driver detail out of the caller-facing variant, and source()
    /// exposes it for operators.
    #[test]
    fn read_error_display_and_source() {
        let bad = ReadError::BadRequest("limit must be >= 1".into());
        assert_eq!(bad.to_string(), "limit must be >= 1");
        assert!(Error::source(&bad).is_none());

        let unavailable = ReadError::Unavailable(SqlxError::PoolClosed);
        assert_eq!(unavailable.to_string(), "ledger read failed");
        let src = Error::source(&unavailable).expect("driver cause");
        assert!(src.to_string().contains("closed"), "{src}");
    }

    /// `limit < 1` refuses BEFORE any query: a lazy pool to a blackhole
    /// address proves no connection is attempted.
    #[tokio::test]
    async fn non_positive_limits_refuse_before_any_query() {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://u:p@192.0.2.1:5432/nowhere")
            .expect("lazy");
        for n in [0, -5] {
            let err = list_page(
                &pool,
                "ws",
                PageQuery {
                    limit: Some(n),
                    ..Default::default()
                },
            )
            .await
            .expect_err("must refuse");
            assert!(matches!(err, ReadError::BadRequest(_)), "{err}");
        }
    }

    #[test]
    fn cursor_round_trips() {
        let at = Utc::now();
        let enc = encode_cursor(at, 42);
        let (back_at, back_id) = decode_cursor(&enc).expect("round trip");
        assert_eq!(back_id, 42);
        assert_eq!(back_at.timestamp_micros(), at.timestamp_micros());
    }

    #[test]
    fn garbage_cursors_are_bad_requests() {
        for c in ["", "!!!", "bm9jb2xvbg", "MTIz"] {
            assert!(matches!(decode_cursor(c), Err(ReadError::BadRequest(_))));
        }
    }
}
