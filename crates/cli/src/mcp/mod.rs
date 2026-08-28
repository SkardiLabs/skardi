//! `skardi mcp` — serve MCP over stdio, proxying every tool call to a
//! running skardi-server over REST. The host (Claude Desktop, Cursor, ...)
//! spawns this subcommand as a long-lived child process; stdout is the
//! JSON-RPC channel, so nothing on this path may print to it.

mod bridge;
mod projection;

use rmcp::ServiceExt;
use rmcp::transport::stdio;

use crate::client::ApiClient;

pub async fn run(client: ApiClient) -> anyhow::Result<()> {
    let service = bridge::McpBridge::new(client)
        .serve(stdio())
        .await
        .map_err(|e| anyhow::anyhow!("MCP initialize handshake failed: {e}"))?;
    // Runs until the host closes stdin (or cancels). In-flight request tasks
    // die with the process — nobody is left to read their results. Falling
    // through to Ok(()) is the decided "host closed → exit 0" behavior.
    service.waiting().await?;
    Ok(())
}
