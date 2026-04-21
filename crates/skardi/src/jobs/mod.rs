//! Batch job primitive — the pipelines-as-jobs feature.
//!
//! A `kind: job` YAML describes a parameterized SELECT whose output rows are
//! written to a durable destination (a Lance dataset or a read-write DB
//! table). Jobs are submitted through `skardi-server`, persisted in a SQLite
//! run ledger at `~/.skardi/jobs.db` (or any path the server was started
//! with), executed by a background Tokio task, and polled via the CLI.
//!
//! Unlike pipelines (HTTP request/response), jobs are **asynchronous**: the
//! HTTP submit call returns immediately with a `run_id`, and status is
//! polled. See the design doc at `.claude/plans/pipelines_as_jobs.md` for
//! the full rationale.

pub mod definition;
pub mod destination;
pub mod executor;
pub mod store;

pub use definition::{Destination, DestinationMode, Execution, JobDefinition, JobKind};
pub use destination::{
    JobDestination, JobDestinationKind, LanceDestination, SqlDmlDestination, WriteOutcome,
};
pub use executor::{JobExecutor, JobSubmitError};
pub use store::{JobRun, JobRunStatus, JobStore, SqliteJobStore};
