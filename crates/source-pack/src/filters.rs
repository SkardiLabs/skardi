//! The filter vocabulary a provider DECLARES.
//!
//! This is the declaration half of the split this crate runs on: a pack says
//! which column, which operator, which action input, and how a literal
//! renders. Turning a SQL predicate into that input is the ENGINE's half,
//! because only the engine has a `datafusion::logical_expr::Expr` to turn.
//!
//! [`Operator`](crate::filters::Operator) is why the split had to be made explicit. It was
//! `datafusion::logical_expr::Operator` — the single genuine type-level
//! coupling in the whole Open Connector module — and a syncer cannot carry a
//! query planner to name three comparison operators. The neutral enum already
//! existed as the pack loader's private YAML spelling, so this promotes that
//! rather than inventing a parallel vocabulary, and the engine converts at its
//! own boundary.

use serde::Deserialize;

/// A comparison a provider can push down.
///
/// Three, and deliberately only three: these are what the YAML asset grammar
/// accepts and what any provider has been asked for. Widening this is a
/// decision about pushdown rather than a formality — a new variant needs a
/// provider that implements it and an engine translation that earns the
/// fidelity claim below.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum Operator {
    #[serde(rename = "eq")]
    Eq,
    #[serde(rename = "gt")]
    Gt,
    #[serde(rename = "gt_eq")]
    GtEq,
}

/// How faithfully a mapping's provider input represents the SQL predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fidelity {
    /// The provider filter is exactly the SQL predicate; DataFusion does not
    /// re-evaluate it, so a wrong `Exact` claim silently drops rows.
    Exact,
    /// The provider filter is conservative: it may return *more* rows than
    /// the predicate allows (fuzzy semantics, coarser timestamp granularity),
    /// and DataFusion reapplies the predicate locally. A mapping may only be
    /// `Inexact` if the provider can never return **fewer** matching rows —
    /// rows the provider drops are unrecoverable, re-filtering or not.
    Inexact,
}

/// How a mapping's literal renders into the provider input. Timestamps are
/// the polymorphic case: JSON has no timestamp type and providers disagree
/// — GitHub's `since` takes RFC 3339, Slack-style inputs take epoch
/// seconds. Making the format part of the mapping keeps a future pack from
/// silently sending the wrong spelling. Non-timestamp scalars render
/// identically under every format, so `Verbatim` exists to *say* that no
/// timestamp spelling applies rather than lean on that coincidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueFormat {
    /// The literal's natural JSON rendering, for non-timestamp inputs
    /// (strings, numbers, booleans). A timestamp literal reaching a
    /// Verbatim mapping does NOT translate — the mapping never declared a
    /// timestamp spelling, and guessing one risks sending the wrong
    /// format, so the predicate stays local instead.
    Verbatim,
    /// Timestamps render as RFC 3339 UTC strings (`2026-01-01T00:00:00Z`).
    Rfc3339,
    /// Timestamps render as whole epoch seconds, flooring sub-second
    /// precision. Flooring widens a *lower* bound (a superset — safe under
    /// [`Fidelity::Inexact`] re-filtering); for an upper-bound provider
    /// parameter it would narrow the fetch and drop rows, so epoch-seconds
    /// mappings are for lower-bound inputs (`ts_from`-style) only — the
    /// pack loader REJECTS any other operator (and any fidelity but
    /// Inexact) at load time. Pre-epoch instants clamp to `0`: still a
    /// lower-bound superset, and providers with unsigned epoch inputs
    /// (Feishu 400s a `-` sign) never see a negative.
    EpochSeconds,
    /// [`ValueFormat::EpochSeconds`] rendered as a JSON *string* of decimal
    /// digits, for providers whose strict input schemas type their epoch
    /// inputs as strings (Feishu's `startTime`, `minLength: 1`) — a JSON
    /// number there is a hard 400. Same flooring semantics, so the same
    /// lower-bound-only rule applies.
    EpochSecondsString,
}

/// One allowlisted pushdown rule: `column <operator> literal` → `input_field: literal`.
///
/// Exactly one operator per mapping, on purpose: a single
/// `(input_field, literal)` pair can only faithfully represent one operator's
/// semantics. Listing several operators against one input field lets two
/// operators with *different* semantics (e.g. `>` vs `>=` against a
/// strictly-greater input) both be classified Exact — and the wrong one
/// silently drops rows DataFusion never reapplies. If a provider input is
/// exact for two operators, declare two mappings.
#[derive(Debug, Clone, Copy)]
pub struct FilterMapping {
    /// Arrow column name the predicate references.
    pub column: &'static str,
    /// The comparison operator this mapping accepts.
    pub operator: Operator,
    /// Action input field the translated value is written to.
    pub input_field: &'static str,
    /// Whether the provider input represents the predicate exactly or
    /// conservatively (see [`Fidelity`]).
    pub fidelity: Fidelity,
    /// How the literal renders into the provider input (see [`ValueFormat`]).
    pub value_format: ValueFormat,
}
