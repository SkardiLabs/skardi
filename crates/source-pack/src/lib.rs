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

pub mod error;

pub use error::OpenConnectorError;
