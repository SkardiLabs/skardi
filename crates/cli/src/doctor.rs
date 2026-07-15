//! `skardi doctor` — quick self-check of the skardi binary.
//!
//! Scope (v1): this checks the BINARY itself — its version, and that the query
//! engine can build a context and run SQL — plus which optional features were
//! compiled in. It deliberately does NOT read your `ctx.yaml`, so it does not
//! verify that your configured data sources are reachable or correctly set up.
//! Verifying data sources is planned for a future version (via `--ctx`).
//!
//! Environment concerns outside the engine (Python deps, the
//! `auto_knowledge_base` skill) are handled by the skills/setup side, not here.

use anyhow::Result;

/// Run the checks, print a report, and return whether all CORE checks passed.
/// The caller decides the process exit code, so this stays testable and free of
/// `process::exit`.
pub async fn run() -> Result<bool> {
    let query = check_query_engine().await;
    let feats = compiled_optional_features();
    let query_err = query.as_ref().err().map(|e| e.to_string());
    print!("{}", render_report(query_err.as_deref(), &feats));
    Ok(query.is_ok())
}

/// Render the full report text. Pure (no I/O), so it can be unit-tested.
/// `query_err` is `None` when the query check passed, `Some(msg)` on failure.
fn render_report(query_err: Option<&str>, feats: &[&str]) -> String {
    let mut out = String::from("skardi doctor — self-check\n\n");

    // Core: CLI version (always reportable).
    out.push_str(&format!("  ✓ CLI version {}\n", env!("CARGO_PKG_VERSION")));

    // Core: query engine.
    match query_err {
        None => out
            .push_str("  ✓ Query engine: basic self-check passed (context built, SELECT 1 ran)\n"),
        Some(e) => out.push_str(&format!("  ✗ Query engine: {e}\n")),
    }

    // Info: which optional features were compiled in — NOT a capability claim.
    if feats.is_empty() {
        out.push_str("  · Optional features compiled in: none\n");
    } else {
        out.push_str(&format!(
            "  · Optional features compiled in: {}\n",
            feats.join(", ")
        ));
    }
    out.push_str("    (compile-time only; some still need runtime config — e.g. an API key — or ");
    out.push_str(
        "data to be useful. Vector search over existing embeddings works regardless.)\n\n",
    );

    // Summary.
    if query_err.is_none() {
        out.push_str("Core checks passed — the skardi binary is functional.\n");
        out.push_str("Note: data sources were NOT checked. Verifying configured data sources is ");
        out.push_str("planned for a future version (via `--ctx`).\n");
    } else {
        out.push_str("Core check FAILED — see ✗ above.\n");
    }
    out
}

/// Build the real skardi session context (the same `new_session_context()` the
/// `query` command uses) and run a trivial query. Confirms context construction
/// and basic SQL execution; does NOT exercise every UDF/UDTF or any configured
/// data source.
async fn check_query_engine() -> Result<()> {
    let (ctx, _registry) = crate::new_session_context();
    let batches = ctx.sql("SELECT 1 AS ok").await?.collect().await?;
    anyhow::ensure!(!batches.is_empty(), "query returned no result batches");
    Ok(())
}

/// Which optional (feature-gated) capabilities were compiled into this binary.
/// Reflects compile-time features only — not whether they are usable at runtime
/// (which may additionally need API keys, models, or data).
fn compiled_optional_features() -> Vec<&'static str> {
    let mut v = Vec::new();
    if cfg!(feature = "onnx") {
        v.push("onnx");
    }
    if cfg!(feature = "candle") {
        v.push("candle");
    }
    if cfg!(feature = "gguf") {
        v.push("gguf");
    }
    if cfg!(feature = "remote-embed") {
        v.push("remote-embed");
    }
    if cfg!(feature = "chunking") {
        v.push("chunking");
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The reported list must agree with the actual compile-time cfg flags —
    /// not just be "a subset of known names" (which an empty list would pass).
    #[test]
    fn features_match_cfg_flags() {
        let f = compiled_optional_features();
        assert_eq!(f.contains(&"onnx"), cfg!(feature = "onnx"));
        assert_eq!(f.contains(&"candle"), cfg!(feature = "candle"));
        assert_eq!(f.contains(&"gguf"), cfg!(feature = "gguf"));
        assert_eq!(f.contains(&"remote-embed"), cfg!(feature = "remote-embed"));
        assert_eq!(f.contains(&"chunking"), cfg!(feature = "chunking"));
    }

    #[test]
    fn report_on_success_is_honest_about_scope() {
        let s = render_report(None, &["onnx", "candle"]);
        assert!(s.contains("✓ Query engine"));
        assert!(s.contains("onnx, candle"));
        assert!(s.contains("Core checks passed"));
        assert!(s.contains("data sources were NOT checked"));
        assert!(!s.contains("Core check FAILED"));
        // Must not present a not-yet-implemented command as runnable.
        assert!(!s.contains("skardi doctor --ctx"));
    }

    #[test]
    fn report_on_failure_is_flagged() {
        let s = render_report(Some("boom"), &[]);
        assert!(s.contains("✗ Query engine: boom"));
        assert!(s.contains("Optional features compiled in: none"));
        assert!(s.contains("Core check FAILED"));
        assert!(!s.contains("Core checks passed"));
    }

    #[tokio::test]
    async fn query_engine_check_passes_on_healthy_build() {
        // A healthy build must be able to construct a context and run SELECT 1.
        assert!(check_query_engine().await.is_ok());
    }
}
