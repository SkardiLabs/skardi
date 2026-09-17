//! Test support for the gateway wire: the mock server, and the envelope
//! shapes `POST /v1/actions/{id}` and `GET /v1/actions/{id}` actually
//! return.
//!
//! Behind the `testing` feature rather than `#[cfg(test)]`, because the
//! engine's own suites use these too and a `cfg(test)` module is invisible
//! across a crate boundary. A production build of this crate carries none
//! of it.
//!
//! The visibility widened from `pub(crate)` to `pub` for exactly that
//! reason: the audience is unchanged — this crate's client tests and the
//! engine's pack suites — but they are no longer the same crate.

pub use crate::mock_http::{
    MockHttpServer as MockGateway, MockResponse, RecordedRequest,
};

impl MockResponse {
    /// `200 OK` with a JSON body.
    ///
    /// No `content-type` header travels with it (the shared server injects
    /// none, and this constructor adds none): nothing in
    /// `OpenConnectorClient` reads a response content type — bodies are
    /// parsed as JSON regardless — so declaring one would pin a header no
    /// test observes.
    pub fn ok(body: &str) -> Self {
        Self::new(200, body)
    }
}

/// Wrap executor output (or any `data` payload) in the gateway's uniform
/// success envelope, exactly as `POST /v1/actions/{id}` returns it.
pub fn envelope_ok(data: &str) -> String {
    format!(r#"{{"success":true,"message":"OK","data":{data},"meta":{{}}}}"#)
}

/// A failed gateway envelope with an `errorCode`, as the gateway returns
/// alongside a 4xx/5xx status.
pub fn envelope_err(error_code: &str, message: &str) -> String {
    format!(
        r#"{{"success":false,"message":"{message}","data":null,"errorCode":"{error_code}","meta":{{}}}}"#
    )
}

/// A discovery envelope (`GET /v1/actions/{{id}}`) whose `data` carries the
/// given schemas and execution block. `read_only` renders the
/// forward-compatible `execution.readOnly` field when present — today's
/// gateway omits it.
pub fn discovery_ok(
    input_schema: &str,
    output_schema: &str,
    locally_executable: bool,
    read_only: Option<bool>,
) -> String {
    let read_only = match read_only {
        Some(value) => format!(r#","readOnly":{value}"#),
        None => String::new(),
    };
    envelope_ok(&format!(
        r#"{{"inputSchema":{input_schema},"outputSchema":{output_schema},"execution":{{"locallyExecutable":{locally_executable}{read_only}}}}}"#
    ))
}

