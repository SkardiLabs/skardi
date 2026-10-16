//! `kind: webhook` resources: public `GET|POST /webhooks/:name` routes that
//! accept a provider's event deliveries, verify them, flatten the envelope
//! into rows, and append those rows to a writable SQL table.
//!
//! The receiver exists because some sources only *push*. The WhatsApp Cloud
//! API has no endpoint for message history: an inbound message is delivered
//! once, by webhook, and retried for up to seven days when the endpoint does
//! not answer 200. Everything downstream of the envelope is reused rather
//! than new — the column grammar and converter are the Open Connector source
//! pack's, the writer is the jobs destination, dedup is the Lance upsert a
//! merge job runs later. Only two pieces are provider-specific, and both live
//! in [`meta_whatsapp`]: the verification preset and the envelope unwrapper.
//!
//! Durability rests on the provider's retry: a delivery is acknowledged with
//! 200 only after its rows are committed, and every failure after
//! verification answers 500 so the provider sends it again. There is no
//! early acknowledgement and no in-memory buffer, on purpose.
//!
//! Module layout:
//! - [`definition`] — the YAML resource and its validation.
//! - the `columns:` grammar is the source pack's own `ColumnDoc`, shared
//!   through `skardi-source-pack` so a webhook route and a pack table parse
//!   one definition.
//! - [`meta_whatsapp`] — the `meta_whatsapp` provider preset.
//! - [`registry`] — definitions compiled against the live session at startup,
//!   including the destination-table preflight.
//! - [`handlers`] — the axum handlers.

pub mod definition;
pub mod handlers;
pub mod meta_whatsapp;
pub mod registry;

pub use definition::{RouteDefinition, RowSelector, WebhookDefinition, WebhookProvider};
pub use registry::{CompiledRoute, CompiledWebhook, WebhookRegistry};

/// Name of the arrival-time column the receiver appends to every route's
/// rows. It is not declared in `columns:` because it is not part of the
/// payload; it exists so a merge job can watermark on arrival rather than on
/// the provider's own timestamp, which a retried or backfilled delivery
/// carries unchanged.
pub const INGESTED_AT_COLUMN: &str = "ingested_at";
