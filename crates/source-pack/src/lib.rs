//! Executable SaaS connector runtime, shared by the engine and by cloud.
//!
//! # Why this crate exists
//!
//! Three consumers read the same SaaS sources through Open Connector, and each
//! grew its own client and its own paging loop:
//!
//! * the **engine**, for SQL over a SaaS table;
//! * **skardi-etl**, for content ingest;
//! * **skardi-rbac**'s syncer, for ACL snapshots.
//!
//! They cannot converge on the engine's implementation, because importing the
//! engine drags in DataFusion and Arrow — a syncer that mirrors an ACL has no
//! business compiling a query planner. So the runtime moves here, where all
//! three can reach it.
//!
//! Measured before the move (2026-09-17): the engine's `client.rs` and
//! `pagination.rs` are 1 371 production lines; skardi-rbac's `connectors/oc.rs`
//! and `connectors/http.rs` are 1 005; skardi-etl's `oc_client.rs` and
//! `folder_dialect.rs` are 828 — and on top of those, six rbac connectors each
//! carry a hand-written paging loop. Three implementations of "call an action
//! and page through the result", none of which can be fixed once.
//!
//! # The boundary, which is the whole design
//!
//! **Nothing in this crate may depend on DataFusion, Arrow, `skardi`,
//! `skardi-rbac`, SQLx, Kubernetes or SpiceDB**, transitively included. That is
//! what makes it importable by a syncer, and it is a boundary rather than a
//! size target: the day one of them appears, the reason the crate exists is
//! gone.
//!
//! The split that boundary implies, and it is sharper than "move the shared
//! bits": **this crate owns the DECLARATION, the engine owns the
//! TRANSLATION.** A provider's pack declares which filters it supports and how
//! a value renders into an action input; turning a DataFusion `Expr` into that
//! input stays in the engine, because only the engine has an `Expr`. The same
//! rule sorts the rest — a JSON-shape descriptor belongs here, the Arrow
//! `DataType` it maps to does not.
//!
//! # What is NOT here
//!
//! Open Connector itself stays a separate runtime service. This crate moves the
//! Rust client and the interpretation of what OC returns; it does not move OC's
//! OAuth broker or its provider action implementations.

/// The Open Connector data-source configuration: which gateway, which
/// connection alias, and which action each bound table reads.
/// The Open Connector HTTP client: one action call, its retries, and the
/// gateway envelope it unwraps.
pub mod client;
/// The action catalog: what the gateway says an action takes and returns,
/// and the identity a discovered action is cached under.
pub mod action_registry;
pub mod config;
pub mod error;
/// HTTP client behaviour shared by every caller: `Retry-After` parsing and the
/// jitter a retry waits. Lives here rather than in the engine because the pack
/// is what makes the requests, and the engine re-exports it for its own
/// non-SaaS callers (`rss`, the model clients).
pub mod http;
/// Canonical JSON and its hash — how an action's identity is computed — plus
/// the value-kind names used in error messages.
pub mod json;
/// One shared paging executor for every strategy a provider uses — page
/// number, cursor, explicit has-more, keyset, single page, and split-action
/// continuation.
pub mod pagination;
/// Where a provider's rows live inside its response body.
pub mod row_path;
/// The character bound that keeps a provider's error body from becoming a log
/// flood.
pub mod text;
#[cfg(feature = "testing")]
pub mod mock_http;
#[cfg(feature = "testing")]
pub mod testing;

pub use client::OpenConnectorClient;
pub use config::OpenConnectorConfig;
pub use error::OpenConnectorError;
