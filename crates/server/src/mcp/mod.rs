//! MCP over streamable HTTP at `/mcp` — a protocol adapter over the
//! server's own router, not a second execution path. Design:
//! `docs/superpowers/specs/2026-08-28-mcp-http-transport-design.md`.

pub(crate) mod handler;
