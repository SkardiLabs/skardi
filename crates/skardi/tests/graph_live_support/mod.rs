//! Helpers shared by the graph live suites (`graph_age_live`,
//! `graph_neo4j_live`). One copy on purpose: `split_creds` is the code
//! that keeps live passwords out of registered configs — a fix to it
//! must reach every backend's suite at once.

use arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;

/// The gating env var's value, if set non-empty (each suite names its
/// own variable).
pub fn live_url(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|u| !u.trim().is_empty())
}

/// Split a maybe-credentialed URL into (cred-free URL, user, pass):
/// config validation rejects URL-embedded passwords, so the registered
/// source takes credentials the designed way — env-var NAMES.
pub fn split_creds(url: &str) -> (String, Option<String>, Option<String>) {
    let mut parsed = url::Url::parse(url).expect("live URL parses");
    let user = (!parsed.username().is_empty()).then(|| parsed.username().to_string());
    let pass = parsed.password().map(str::to_string);
    parsed.set_username("").expect("URL takes userinfo");
    parsed.set_password(None).expect("URL takes userinfo");
    (parsed.to_string(), user, pass)
}

pub async fn collect(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .expect("plans")
        .collect()
        .await
        .expect("executes")
}
