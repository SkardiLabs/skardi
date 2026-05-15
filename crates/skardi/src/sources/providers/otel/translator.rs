//! SQL → backend-query translator.
//!
//! Tier-1 path: the `metrics` and `logs` [`TableProvider`]s call into
//! this module to translate DataFusion `Expr` predicates, projections,
//! and aggregates into a single Prometheus / Loki HTTP call.
//!
//! Predicates outside the supported matrix surface as
//! [`OtelError::UnsupportedPushdown`] with a `hint` pointing at the
//! `prom_query` / `loki_query` escape hatch — see `design.md`
//! Decision 4.
//!
//! [`TableProvider`]: datafusion::catalog::TableProvider
//!
//! # Supported predicate matrix
//!
//! ## `metrics` table (Prometheus backend)
//!
//! | SQL shape                                                  | Translation                       | Endpoint                       |
//! | ---------------------------------------------------------- | --------------------------------- | ------------------------------ |
//! | `WHERE name = '<metric>'` (REQUIRED)                       | selector `<metric>{…}`            | both                           |
//! | `WHERE labels['k'] = '<v>'`                                | matcher `k="<v>"`                 | both                           |
//! | `WHERE labels['k'] != '<v>'`                               | matcher `k!="<v>"`                | both                           |
//! | `WHERE labels['k'] LIKE '<pat>'`                           | matcher `k=~"<regex_from_like>"`  | both                           |
//! | `WHERE labels['k'] NOT LIKE '<pat>'`                       | matcher `k!~"<regex_from_like>"`  | both                           |
//! | `WHERE labels['k'] IN ('a', 'b', …)`                       | matcher `k=~"^(a|b|…)$"`          | both                           |
//! | `WHERE labels['k'] NOT IN ('a', 'b', …)`                   | matcher `k!~"^(a|b|…)$"`          | both                           |
//! | `WHERE ts BETWEEN <start> AND <end>`                       | `start=`, `end=`, `step=<default>`| `/api/v1/query_range`          |
//! | `WHERE ts >= <start> AND ts <= <end>`                      | same                              | `/api/v1/query_range`          |
//! | `WHERE ts > <start>` (no upper bound)                      | `start=<v>`, `end=now`, `step=…`  | `/api/v1/query_range`          |
//! | `WHERE ts = <ts>`                                          | `time=<ts>`                       | `/api/v1/query`                |
//! | no `ts` predicate                                          | `default_window` applied          | `/api/v1/query_range`          |
//! | `LIMIT n`                                                  | client-side cap after translation | both                           |
//!
//! `GROUP BY` + aggregate pushdown is still tracked under task 4.2 —
//! see [`Aggregate`].
//!
//! ## `logs` table (Loki backend)
//!
//! | SQL shape                                                  | Translation                       |
//! | ---------------------------------------------------------- | --------------------------------- |
//! | `WHERE labels['k'] = '<v>'` (and !=, LIKE, NOT LIKE, IN)   | LogQL matcher inside `{…}`        |
//! | `WHERE ts BETWEEN <start> AND <end>`                       | `start=`, `end=`                  |
//! | `WHERE line LIKE '%<substr>%'` (pure substring)            | LogQL `\|= "<substr>"`            |
//! | `WHERE line LIKE 'foo%'` (anchored / wildcard)             | LogQL `\|~ "^foo.*$"`             |
//! | `WHERE line NOT LIKE '%<substr>%'`                         | LogQL `!= "<substr>"`             |
//! | `LIMIT n`                                                  | LogQL `limit=n`                   |
//!
//! At least one stream-label predicate (`labels['app'] = '…'` and
//! friends) is REQUIRED — LogQL rejects the empty selector `{}`. The
//! provider in section 5 short-circuits empty-selector queries before
//! they reach the upstream so the error message is clear instead of
//! the generic LogQL one.
//!
//! # Why `UnsupportedPushdown` instead of fetch-then-filter
//!
//! Prometheus and Loki are append-only firehoses; pulling without a
//! tight selector will OOM the server or DOS the upstream. We
//! deliberately refuse to fetch-and-filter for unrecognized predicates
//! — see `design.md` Decision 4 *"Why this shape"*.

use std::time::Duration;

use chrono::{DateTime, Utc};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::expr::{InList, ScalarFunction};
use datafusion::logical_expr::{Between, BinaryExpr, Expr, Like, Operator};

use super::error::OtelError;
use super::time::{TimeDefaults, TimeWindow, resolve_window};

/// DataFusion lowers SQL subscript `labels['k']` against a Map column
/// to `get_field(labels, 'k')` (see `datafusion-functions/src/core/getfield.rs`
/// — the function dispatches polymorphically on Struct vs. Map at exec
/// time). Anchoring the recognizer on this name keeps it stable across
/// equivalent SQL surface forms (`labels['k']`, `labels["k"]`).
const GET_FIELD_FN: &str = "get_field";

/// `labels` column name on both `metrics` and `logs` tables — fixed by
/// the provider schemas in `prometheus.rs` and `loki.rs`.
const LABELS_COLUMN: &str = "labels";

// ---------------------------------------------------------------------------
// IR types (task 3.5.1)
// ---------------------------------------------------------------------------

/// Aggregate functions the translator can fold into a PromQL
/// `<agg> by(<keys>)(<selector>)` form. Set on [`PromQuerySpec::agg`]
/// when section 4.2 (aggregate pushdown) detects a recognizable
/// `GROUP BY` pattern; left `None` for raw scans.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregate {
    Sum,
    Avg,
    Min,
    Max,
    Count,
}

impl Aggregate {
    /// PromQL keyword for this aggregate.
    pub fn promql_name(self) -> &'static str {
        match self {
            Self::Sum => "sum",
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
            Self::Count => "count",
        }
    }
}

/// Whether a Prometheus query maps to `/api/v1/query` (instant) or
/// `/api/v1/query_range`. Determined by the translator from the
/// `ts`-predicate shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryKind {
    /// Single point-in-time evaluation. Set when the caller uses
    /// `WHERE ts = <ts>`.
    Instant { time: DateTime<Utc> },
    /// Sample over a `[start, end]` range at `step` cadence.
    Range,
}

/// IR consumed by the Prometheus provider in section 4.
#[derive(Debug, Clone)]
pub struct PromQuerySpec {
    /// Full PromQL selector, e.g. `http_requests_total{service="api"}`
    /// — pre-rendered from `metric_name` + `label_matchers` by the
    /// translator so providers can pass it through unchanged.
    pub selector: String,
    /// Metric name extracted from the required `name = '...'` predicate.
    /// Kept alongside `selector` so callers (e.g. aggregate-pushdown in
    /// 4.2) can re-render `<agg> by(...)({selector})` without
    /// re-parsing.
    pub metric_name: String,
    /// Recognized `labels['k'] OP <literal>` predicates, in declaration
    /// order. Folded into `selector` already; exposed so future
    /// aggregate-pushdown can introspect them.
    pub label_matchers: Vec<LabelMatcher>,
    /// Optional `GROUP BY` keys for aggregate pushdown.
    pub group_by: Option<Vec<String>>,
    /// Optional aggregate folded into `<agg> by(<keys>)(<selector>)`.
    pub agg: Option<Aggregate>,
    /// Resolved time window (range queries) or zero-width window
    /// (instant queries).
    pub window: TimeWindow,
    /// Endpoint hint derived from the predicate shape.
    pub query_kind: QueryKind,
    /// Optional client-side row cap derived from SQL `LIMIT n`.
    pub limit: Option<usize>,
}

/// Whether a line filter includes (`|=` / `|~`) or excludes (`!=` /
/// `!~`) matching log lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LineFilterKind {
    Include,
    Exclude,
}

/// Whether the filter compares as a literal substring or a regex.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternKind {
    Substring,
    Regex,
}

/// A single LogQL line-filter stage, e.g. `|= "error"` or `|~ "^/api/"`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineFilter {
    pub kind: LineFilterKind,
    pub pattern_kind: PatternKind,
    pub pattern: String,
}

impl LineFilter {
    /// Render to LogQL syntax.
    pub fn to_logql(&self) -> String {
        let op = match (self.kind, self.pattern_kind) {
            (LineFilterKind::Include, PatternKind::Substring) => "|=",
            (LineFilterKind::Include, PatternKind::Regex) => "|~",
            (LineFilterKind::Exclude, PatternKind::Substring) => "!=",
            (LineFilterKind::Exclude, PatternKind::Regex) => "!~",
        };
        format!("{op} {}", escape_logql_string(&self.pattern))
    }
}

/// IR consumed by the Loki provider in section 5.
#[derive(Debug, Clone)]
pub struct LokiQuerySpec {
    /// LogQL stream selector, e.g. `{app="checkout"}` — pre-rendered
    /// from `label_matchers`. Stays `{}` when no `labels['k']`
    /// predicate was recognized; section 5's `LokiLogsTable` rejects
    /// empty selectors before issuing an upstream call.
    pub selector: String,
    /// Recognized `labels['k'] OP <literal>` predicates, in declaration
    /// order.
    pub label_matchers: Vec<LabelMatcher>,
    /// Ordered list of line-filter stages applied after the selector.
    pub line_filters: Vec<LineFilter>,
    /// Resolved time window for the range query.
    pub window: TimeWindow,
    /// Optional row cap (becomes Loki's `limit=` query param).
    pub limit: Option<usize>,
}

// ---------------------------------------------------------------------------
// Label matchers (tasks 3.5.2 / 3.5.3 — `labels['k']` predicate pushdown)
// ---------------------------------------------------------------------------

/// The four label-matcher operators Prometheus and Loki share.
///
/// PromQL and LogQL both encode label matching as `<key><op>"<value>"`
/// where `<op>` is one of these four, and the wire format is identical
/// between backends (LogQL is intentionally a strict superset of
/// PromQL's selector grammar). Keeping a single enum lets the same
/// recognizer feed both translators.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LabelMatcherOp {
    /// `<key>="<value>"` — exact equality.
    Eq,
    /// `<key>!="<value>"` — exact inequality.
    Ne,
    /// `<key>=~"<regex>"` — regex match. Anchored (LogQL anchors
    /// implicitly; PromQL requires full-string match).
    Regex,
    /// `<key>!~"<regex>"` — negated regex match.
    NRegex,
}

impl LabelMatcherOp {
    fn as_selector_op(self) -> &'static str {
        match self {
            Self::Eq => "=",
            Self::Ne => "!=",
            Self::Regex => "=~",
            Self::NRegex => "!~",
        }
    }
}

/// A single `<key><op>"<value>"` selector entry. Both backends share
/// the same wire form, so a `Vec<LabelMatcher>` renders to the same
/// `{...}` body whether it lands in a PromQL selector
/// (`http_requests_total{...}`) or a LogQL stream selector (`{...}`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelMatcher {
    pub key: String,
    pub op: LabelMatcherOp,
    /// Raw value or regex source. For [`LabelMatcherOp::Regex`] /
    /// [`LabelMatcherOp::NRegex`], pre-anchored (`^...$`) so PromQL's
    /// full-string semantics match SQL `LIKE` / `IN`.
    pub value: String,
}

impl LabelMatcher {
    /// Render to the `<key><op>"<value>"` form used inside selector
    /// braces.
    pub fn to_selector_str(&self) -> String {
        format!(
            "{}{}{}",
            self.key,
            self.op.as_selector_op(),
            escape_logql_string(&self.value)
        )
    }
}

/// Render an ordered list of matchers as a `{m1,m2,m3}` block. Used
/// by both backends to build their selector strings.
pub fn render_matchers_block(matchers: &[LabelMatcher]) -> String {
    let mut out = String::from("{");
    for (i, m) in matchers.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&m.to_selector_str());
    }
    out.push('}');
    out
}

// ---------------------------------------------------------------------------
// LIKE → regex / IN → regex helpers (tasks 3.5.4 / 3.5.5)
// ---------------------------------------------------------------------------

/// Translate a SQL `LIKE` pattern into a Prometheus/Loki regex literal.
///
/// `%` becomes `.*`, `_` becomes `.`. Every other character — including
/// regex metacharacters like `.`, `*`, `+`, `?`, `\`, `(`, `)`, `[`,
/// `]`, `{`, `}`, `|`, `^`, `$` — is escaped so it matches literally.
/// The result is anchored with `^…$` so it behaves like SQL `LIKE`
/// (full-string match, not partial).
///
/// This is the shared helper used by both backends' translators for
/// `labels['k'] LIKE '<pat>'` (section 4 / 5 work) and by the logs
/// translator for `line LIKE '<pat>'`.
pub fn like_to_regex(pattern: &str) -> String {
    let mut out = String::with_capacity(pattern.len() + 2);
    out.push('^');
    for ch in pattern.chars() {
        match ch {
            '%' => out.push_str(".*"),
            '_' => out.push('.'),
            // Regex metacharacters that need escaping. Everything else
            // (alphanumerics, spaces, '/' etc.) is literal so we pass
            // it through unchanged.
            '.' | '*' | '+' | '?' | '\\' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^' | '$' => {
                out.push('\\');
                out.push(ch);
            }
            other => out.push(other),
        }
    }
    out.push('$');
    out
}

/// Detect the special-case `LIKE '%<literal>%'` shape — a pure
/// substring match with no embedded wildcards. Returns the literal
/// substring if recognized so Loki's translator can emit `|= "foo"`
/// (cheaper than `|~ "^.*foo.*$"`) and Prometheus can keep regex
/// semantics for the general case.
pub fn extract_substring_like(pattern: &str) -> Option<&str> {
    let bytes = pattern.as_bytes();
    if bytes.len() < 2 || bytes[0] != b'%' || bytes[bytes.len() - 1] != b'%' {
        return None;
    }
    let inner = &pattern[1..pattern.len() - 1];
    // No further wildcards / escape chars inside.
    if inner.contains(['%', '_', '\\']) {
        return None;
    }
    Some(inner)
}

/// Translate a SQL `IN (a, b, c)` list of string literals into a single
/// Prometheus/Loki regex matcher of the form `^(a|b|c)$` with each
/// alternative regex-escaped. Returns `None` if the list is empty (an
/// empty `IN` is illegal SQL but defensive code is cheap).
pub fn in_list_to_regex(values: &[&str]) -> Option<String> {
    if values.is_empty() {
        return None;
    }
    let mut out = String::from("^(");
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push('|');
        }
        for ch in v.chars() {
            match ch {
                '.' | '*' | '+' | '?' | '\\' | '(' | ')' | '[' | ']' | '{' | '}' | '|' | '^'
                | '$' => {
                    out.push('\\');
                    out.push(ch);
                }
                other => out.push(other),
            }
        }
    }
    out.push_str(")$");
    Some(out)
}

/// Quote a string literal for inclusion in a LogQL filter, escaping
/// embedded `"` and `\` characters. (Prometheus uses the same rules
/// for its label-matcher values, so the helper is shared.)
fn escape_logql_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

// ---------------------------------------------------------------------------
// Label-matcher recognizer (tasks 3.5.2 / 3.5.3)
// ---------------------------------------------------------------------------

/// Match `Expr::ScalarFunction { name: "get_field", args: [labels_col,
/// 'k'_lit] }` and return the key.
///
/// DataFusion's SQL planner lowers `labels['k']` (where `labels` is a
/// `Map` column and `'k'` is a string literal) to this form via the
/// `FieldAccessPlanner` in `datafusion-functions-nested`. Returns
/// `None` for anything else (other functions, non-`labels` columns,
/// non-string indices) so the caller can keep walking the filter set.
fn try_labels_get_field(expr: &Expr) -> Option<String> {
    let Expr::ScalarFunction(ScalarFunction { func, args }) = expr else {
        return None;
    };
    if func.name() != GET_FIELD_FN || args.len() != 2 {
        return None;
    }
    let Expr::Column(col) = &args[0] else {
        return None;
    };
    if col.name() != LABELS_COLUMN {
        return None;
    }
    let Expr::Literal(scalar, _) = &args[1] else {
        return None;
    };
    scalar_as_str(scalar).map(String::from)
}

/// Recognize the predicates from the design.md Decision 4 matrix that
/// reference `labels['k']`:
///
/// | SQL shape                                  | LabelMatcherOp |
/// | ------------------------------------------ | -------------- |
/// | `labels['k'] = '<v>'`                      | Eq             |
/// | `labels['k'] != '<v>'` / `<> '<v>'`        | Ne             |
/// | `labels['k'] LIKE '<pat>'`                 | Regex          |
/// | `labels['k'] NOT LIKE '<pat>'`             | NRegex         |
/// | `labels['k'] IN ('a', 'b', 'c')`           | Regex (`^(a|b|c)$`) |
/// | `labels['k'] NOT IN ('a', 'b', 'c')`       | NRegex              |
///
/// Returns:
/// - `Ok(Some(matcher))` when the predicate matches the matrix.
/// - `Ok(None)` when the predicate is not a `labels['k']` shape at all —
///   the caller continues walking the filter set.
/// - `Err(UnsupportedPushdown)` when the predicate clearly targets
///   `labels['k']` but in a shape we cannot safely translate (e.g.
///   `ILIKE`, comparison against a non-string literal) so we surface a
///   clear error pointing at the escape hatch rather than silently
///   skipping the predicate.
fn try_label_matcher(expr: &Expr, source_name: &str) -> Result<Option<LabelMatcher>, OtelError> {
    match expr {
        // `labels['k'] = '<v>'` or `'<v>' = labels['k']` (and `!=` / `<>`).
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (key, value_expr) = match (try_labels_get_field(left), try_labels_get_field(right))
            {
                (Some(k), _) => (k, right.as_ref()),
                (_, Some(k)) => (k, left.as_ref()),
                _ => return Ok(None),
            };
            let matcher_op = match op {
                Operator::Eq => LabelMatcherOp::Eq,
                Operator::NotEq => LabelMatcherOp::Ne,
                other => {
                    return Err(OtelError::UnsupportedPushdown {
                        source_name: source_name.to_string(),
                        predicate: format!("{expr}"),
                        hint: format!(
                            "`labels['k']` predicates support `=`, `!=`, `LIKE`, `NOT LIKE`, \
                             `IN`, `NOT IN` only; `{other}` falls outside that matrix. \
                             For value comparisons use the prom_query/loki_query escape hatch."
                        ),
                    });
                }
            };
            let value = label_value_literal(value_expr, expr, source_name)?;
            Ok(Some(LabelMatcher {
                key,
                op: matcher_op,
                value,
            }))
        }
        // `labels['k'] LIKE '<pat>'` / `labels['k'] NOT LIKE '<pat>'`.
        Expr::Like(Like {
            negated,
            expr: target,
            pattern,
            case_insensitive,
            ..
        }) => {
            let Some(key) = try_labels_get_field(target.as_ref()) else {
                return Ok(None);
            };
            if *case_insensitive {
                return Err(OtelError::UnsupportedPushdown {
                    source_name: source_name.to_string(),
                    predicate: format!("{expr}"),
                    hint: "ILIKE / case-insensitive matching is not pushed down for \
                           `labels['k']`; use a regex via prom_query / loki_query or \
                           normalize at write time"
                        .to_string(),
                });
            }
            let pattern_str = label_value_literal(pattern.as_ref(), expr, source_name)?;
            Ok(Some(LabelMatcher {
                key,
                op: if *negated {
                    LabelMatcherOp::NRegex
                } else {
                    LabelMatcherOp::Regex
                },
                value: like_to_regex(&pattern_str),
            }))
        }
        // `labels['k'] IN ('a', 'b')` / `labels['k'] NOT IN (...)`.
        Expr::InList(InList {
            expr: target,
            list,
            negated,
        }) => {
            let Some(key) = try_labels_get_field(target.as_ref()) else {
                return Ok(None);
            };
            let mut values: Vec<String> = Vec::with_capacity(list.len());
            for item in list {
                values.push(label_value_literal(item, expr, source_name)?);
            }
            let refs: Vec<&str> = values.iter().map(String::as_str).collect();
            let Some(regex) = in_list_to_regex(&refs) else {
                // Empty `IN ()` is illegal SQL; defensive guard so we
                // don't emit `^()$` which would match the empty string.
                return Err(OtelError::UnsupportedPushdown {
                    source_name: source_name.to_string(),
                    predicate: format!("{expr}"),
                    hint: "empty IN list is not valid SQL — supply at least one value".to_string(),
                });
            };
            Ok(Some(LabelMatcher {
                key,
                op: if *negated {
                    LabelMatcherOp::NRegex
                } else {
                    LabelMatcherOp::Regex
                },
                value: regex,
            }))
        }
        _ => Ok(None),
    }
}

/// Extract a UTF-8 string literal — used to extract the RHS of every
/// label predicate. Non-string literals (numbers, booleans, NULL)
/// surface as `UnsupportedPushdown` rather than silently skipping the
/// predicate.
fn label_value_literal(
    expr: &Expr,
    full_predicate: &Expr,
    source_name: &str,
) -> Result<String, OtelError> {
    let Expr::Literal(scalar, _) = expr else {
        return Err(OtelError::UnsupportedPushdown {
            source_name: source_name.to_string(),
            predicate: format!("{full_predicate}"),
            hint: "`labels['k']` predicates must compare against a string literal \
                   (a column-to-column join over labels isn't pushable to PromQL/LogQL)"
                .to_string(),
        });
    };
    match scalar_as_str(scalar) {
        Some(s) => Ok(s.to_string()),
        None => Err(OtelError::UnsupportedPushdown {
            source_name: source_name.to_string(),
            predicate: format!("{full_predicate}"),
            hint: "`labels['k']` predicates must compare against a UTF-8 string literal; \
                   wire-level matchers are always strings"
                .to_string(),
        }),
    }
}

// ---------------------------------------------------------------------------
// translate_metrics_filters (task 3.5.2)
// ---------------------------------------------------------------------------

/// Translate a DataFusion filter / aggregate plan into the
/// [`PromQuerySpec`] consumed by the Prometheus provider.
///
/// Recognizes:
/// - `name = '<metric>'` (REQUIRED — the translator errors without it).
/// - `labels['k']` predicates (`=`, `!=`, `LIKE`, `NOT LIKE`, `IN`,
///   `NOT IN`) — folded into PromQL label matchers inside the selector.
/// - `ts` predicates: `BETWEEN`, `>=`, `<=`, `>`, `<`, `=`.
/// - `LIMIT n` (passed via the `limit` argument).
///
/// Anything else returns [`OtelError::UnsupportedPushdown`] with the
/// offending predicate rendered into the `predicate` field and a
/// copy-pasteable `prom_query(...)` invocation in `hint`.
///
/// `group_by` and `agg` parameters are reserved for section 4.2's
/// aggregate-pushdown path; in v1 both are required to be `None` —
/// non-`None` values surface as `UnsupportedPushdown` until 4.2 lands.
pub fn translate_metrics_filters(
    filters: &[Expr],
    group_by: Option<&[Expr]>,
    agg: Option<Aggregate>,
    limit: Option<usize>,
    defaults: &TimeDefaults,
    source_name: &str,
) -> Result<PromQuerySpec, OtelError> {
    if group_by.is_some() || agg.is_some() {
        return Err(OtelError::UnsupportedPushdown {
            source_name: source_name.to_string(),
            predicate: "GROUP BY / aggregate pushdown".to_string(),
            hint: "aggregate pushdown lands in section 4.2; for v1 use \
                   prom_query('<agg> by(<keys>)(<selector>)') instead"
                .to_string(),
        });
    }

    let mut metric_name: Option<String> = None;
    let mut label_matchers: Vec<LabelMatcher> = Vec::new();
    let mut ts_lower: Option<DateTime<Utc>> = None;
    let mut ts_upper: Option<DateTime<Utc>> = None;
    let mut ts_exact: Option<DateTime<Utc>> = None;

    for filter in filters {
        if let Some(name) = try_metric_name(filter) {
            if metric_name.replace(name).is_some() {
                return Err(OtelError::UnsupportedPushdown {
                    source_name: source_name.to_string(),
                    predicate: format!("{filter}"),
                    hint: "multiple `name = ...` predicates aren't supported; \
                           use prom_query('{__name__=~\"a|b\"}') for unions"
                        .to_string(),
                });
            }
            continue;
        }

        if let Some(matcher) = try_label_matcher(filter, source_name)? {
            label_matchers.push(matcher);
            continue;
        }

        match try_ts_predicate(filter)? {
            Some(TsPredicate::Between { low, high }) => {
                ts_lower = Some(low);
                ts_upper = Some(high);
                continue;
            }
            Some(TsPredicate::Lower(t)) => {
                ts_lower = Some(t);
                continue;
            }
            Some(TsPredicate::Upper(t)) => {
                ts_upper = Some(t);
                continue;
            }
            Some(TsPredicate::Exact(t)) => {
                ts_exact = Some(t);
                continue;
            }
            None => {}
        }

        return Err(OtelError::UnsupportedPushdown {
            source_name: source_name.to_string(),
            predicate: format!("{filter}"),
            hint: "predicate is outside the v1 supported matrix; use \
                   prom_query('<your PromQL here>') instead. Recognized shapes: \
                   `name = '<metric>'`, `labels['k']` (=, !=, LIKE, NOT LIKE, IN, NOT IN), \
                   `ts` range/equality, `LIMIT`. See the translator module \
                   rustdoc for the full supported set."
                .to_string(),
        });
    }

    let metric_name = metric_name.ok_or_else(|| OtelError::UnsupportedPushdown {
        source_name: source_name.to_string(),
        predicate: "SELECT ... FROM metrics (no `name = '...'`)".to_string(),
        hint: "the `metrics` table requires a `WHERE name = '<metric>'` predicate \
               to avoid fanning out across every metric in the catalog. \
               For an explicit fan-out, use prom_query('{__name__=~\".+\"}')."
            .to_string(),
    })?;

    let selector = format!("{metric_name}{}", render_matchers_block(&label_matchers));

    let (window, query_kind) = if let Some(time) = ts_exact {
        // Zero-width window — provider passes `time` directly to /query.
        (
            TimeWindow {
                start: time,
                end: time,
                step: defaults.default_step,
            },
            QueryKind::Instant { time },
        )
    } else {
        let resolved = resolve_window(ts_lower, ts_upper, None, defaults, source_name)?;
        (resolved, QueryKind::Range)
    };

    Ok(PromQuerySpec {
        selector,
        metric_name,
        label_matchers,
        group_by: None,
        agg: None,
        window,
        query_kind,
        limit,
    })
}

/// If `expr` is exactly `name = '<literal>'` or `'<literal>' = name`,
/// return the literal. Otherwise `None`.
fn try_metric_name(expr: &Expr) -> Option<String> {
    let bin = match expr {
        Expr::BinaryExpr(b) if b.op == Operator::Eq => b,
        _ => return None,
    };
    match (bin.left.as_ref(), bin.right.as_ref()) {
        (Expr::Column(col), Expr::Literal(scalar, _)) if col.name() == "name" => {
            scalar_as_str(scalar).map(String::from)
        }
        (Expr::Literal(scalar, _), Expr::Column(col)) if col.name() == "name" => {
            scalar_as_str(scalar).map(String::from)
        }
        _ => None,
    }
}

enum TsPredicate {
    Between {
        low: DateTime<Utc>,
        high: DateTime<Utc>,
    },
    Lower(DateTime<Utc>),
    Upper(DateTime<Utc>),
    Exact(DateTime<Utc>),
}

fn try_ts_predicate(expr: &Expr) -> Result<Option<TsPredicate>, OtelError> {
    match expr {
        Expr::Between(Between {
            expr: target,
            negated,
            low,
            high,
        }) if !*negated && is_ts_column(target.as_ref()) => {
            let low = literal_as_timestamp(low.as_ref())?;
            let high = literal_as_timestamp(high.as_ref())?;
            Ok(Some(TsPredicate::Between { low, high }))
        }
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let (col_first, ts_lit) = match (left.as_ref(), right.as_ref()) {
                (l, r) if is_ts_column(l) => (true, literal_as_timestamp(r)),
                (l, r) if is_ts_column(r) => (false, literal_as_timestamp(l)),
                _ => return Ok(None),
            };
            let ts = ts_lit?;
            // Normalize operator so `ts > X` and `X < ts` mean the same thing.
            let op = if col_first { *op } else { flip_operator(*op) };
            Ok(Some(match op {
                Operator::Gt | Operator::GtEq => TsPredicate::Lower(ts),
                Operator::Lt | Operator::LtEq => TsPredicate::Upper(ts),
                Operator::Eq => TsPredicate::Exact(ts),
                _ => return Ok(None),
            }))
        }
        _ => Ok(None),
    }
}

fn is_ts_column(expr: &Expr) -> bool {
    matches!(expr, Expr::Column(c) if c.name() == "ts")
}

fn flip_operator(op: Operator) -> Operator {
    match op {
        Operator::Gt => Operator::Lt,
        Operator::Lt => Operator::Gt,
        Operator::GtEq => Operator::LtEq,
        Operator::LtEq => Operator::GtEq,
        other => other,
    }
}

fn scalar_as_str(scalar: &ScalarValue) -> Option<&str> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => Some(s.as_str()),
        ScalarValue::LargeUtf8(Some(s)) => Some(s.as_str()),
        ScalarValue::Utf8View(Some(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// Best-effort conversion of a DataFusion timestamp/literal Expr into
/// `DateTime<Utc>`. Returns `Backend` error for non-timestamp literals
/// so the caller can surface a clear message rather than silently
/// truncating.
fn literal_as_timestamp(expr: &Expr) -> Result<DateTime<Utc>, OtelError> {
    let scalar = match expr {
        Expr::Literal(s, _) => s,
        other => {
            return Err(OtelError::Backend {
                source_name: String::new(),
                message: format!("expected a timestamp literal for `ts` predicate, got `{other}`"),
            });
        }
    };
    let nanos = match scalar {
        ScalarValue::TimestampNanosecond(Some(v), _) => *v,
        ScalarValue::TimestampMicrosecond(Some(v), _) => v.saturating_mul(1_000),
        ScalarValue::TimestampMillisecond(Some(v), _) => v.saturating_mul(1_000_000),
        ScalarValue::TimestampSecond(Some(v), _) => v.saturating_mul(1_000_000_000),
        other => {
            return Err(OtelError::Backend {
                source_name: String::new(),
                message: format!(
                    "expected a timestamp literal for `ts` predicate, got `{other:?}`"
                ),
            });
        }
    };
    DateTime::<Utc>::from_timestamp(
        nanos.div_euclid(1_000_000_000),
        (nanos.rem_euclid(1_000_000_000)) as u32,
    )
    .ok_or_else(|| OtelError::Backend {
        source_name: String::new(),
        message: format!("timestamp {nanos} (ns) is outside the chrono range"),
    })
}

// ---------------------------------------------------------------------------
// translate_logs_filters (task 3.5.3)
// ---------------------------------------------------------------------------

/// Translate a DataFusion filter plan into the [`LokiQuerySpec`]
/// consumed by the Loki provider.
///
/// Recognizes:
/// - `labels['k']` predicates (`=`, `!=`, `LIKE`, `NOT LIKE`, `IN`,
///   `NOT IN`) — folded into the LogQL stream selector.
/// - `ts` predicates: `BETWEEN`, `>=`, `<=`, `>`, `<`.
/// - `line LIKE '<pat>'` / `line NOT LIKE '<pat>'`.
/// - `LIMIT n`.
///
/// At least one stream-label predicate is REQUIRED per `design.md` —
/// LogQL rejects the empty selector `{}`. When no `labels['k']`
/// predicate is recognized, this returns a [`LokiQuerySpec`] with
/// `selector == "{}"`; section 5's `LokiLogsTable` short-circuits
/// before issuing an upstream call so the error message points at the
/// escape hatch instead of LogQL's generic "stream selector required".
pub fn translate_logs_filters(
    filters: &[Expr],
    limit: Option<usize>,
    defaults: &TimeDefaults,
    source_name: &str,
) -> Result<LokiQuerySpec, OtelError> {
    let mut line_filters: Vec<LineFilter> = Vec::new();
    let mut label_matchers: Vec<LabelMatcher> = Vec::new();
    let mut ts_lower: Option<DateTime<Utc>> = None;
    let mut ts_upper: Option<DateTime<Utc>> = None;

    for filter in filters {
        if let Some(matcher) = try_label_matcher(filter, source_name)? {
            label_matchers.push(matcher);
            continue;
        }

        if let Some(lf) = try_line_like(filter)? {
            line_filters.push(lf);
            continue;
        }

        match try_ts_predicate(filter)? {
            Some(TsPredicate::Between { low, high }) => {
                ts_lower = Some(low);
                ts_upper = Some(high);
                continue;
            }
            Some(TsPredicate::Lower(t)) => {
                ts_lower = Some(t);
                continue;
            }
            Some(TsPredicate::Upper(t)) => {
                ts_upper = Some(t);
                continue;
            }
            Some(TsPredicate::Exact(_)) => {
                return Err(OtelError::UnsupportedPushdown {
                    source_name: source_name.to_string(),
                    predicate: format!("{filter}"),
                    hint: "Loki has no instant-query semantics; convert to a \
                           narrow time-range predicate or use \
                           loki_query('{...}')"
                        .to_string(),
                });
            }
            None => {}
        }

        return Err(OtelError::UnsupportedPushdown {
            source_name: source_name.to_string(),
            predicate: format!("{filter}"),
            hint: "predicate is outside the v1 supported matrix; use \
                   loki_range('{<stream-selector>} <line-filters>', start, end) \
                   instead. Recognized shapes: `labels['k']` matchers (=, !=, LIKE, \
                   NOT LIKE, IN, NOT IN), `line LIKE/NOT LIKE`, `ts` range, `LIMIT`."
                .to_string(),
        });
    }

    let window = resolve_window(ts_lower, ts_upper, None, defaults, source_name)?;

    // Selector renders to `{}` when no label matcher was recognized;
    // section 5's `LokiLogsTable` rejects empty selectors before
    // issuing an upstream call.
    let selector = render_matchers_block(&label_matchers);

    Ok(LokiQuerySpec {
        selector,
        label_matchers,
        line_filters,
        window,
        limit,
    })
}

/// If `expr` is `line LIKE 'pat'` or `line NOT LIKE 'pat'`, return a
/// [`LineFilter`]. The substring case `'%foo%'` becomes a faster
/// `|= "foo"`; everything else becomes a regex `|~ "<re>"`.
fn try_line_like(expr: &Expr) -> Result<Option<LineFilter>, OtelError> {
    let Expr::Like(Like {
        negated,
        expr: target,
        pattern,
        case_insensitive,
        ..
    }) = expr
    else {
        return Ok(None);
    };

    if *case_insensitive {
        return Err(OtelError::UnsupportedPushdown {
            source_name: String::new(),
            predicate: format!("{expr}"),
            hint: "ILIKE / case-insensitive matching is not pushed down; \
                   either use a regex via loki_query() or normalize at \
                   write time"
                .to_string(),
        });
    }

    if !matches!(target.as_ref(), Expr::Column(c) if c.name() == "line") {
        // Some other column LIKE; not handled in v1.
        return Ok(None);
    }

    let pattern_str = match pattern.as_ref() {
        Expr::Literal(s, _) => scalar_as_str(s).ok_or_else(|| OtelError::Backend {
            source_name: String::new(),
            message: format!("LIKE pattern must be a string literal, got {s:?}"),
        })?,
        other => {
            return Err(OtelError::Backend {
                source_name: String::new(),
                message: format!("LIKE pattern must be a literal, got `{other}`"),
            });
        }
    };

    let kind = if *negated {
        LineFilterKind::Exclude
    } else {
        LineFilterKind::Include
    };

    Ok(Some(
        if let Some(substr) = extract_substring_like(pattern_str) {
            LineFilter {
                kind,
                pattern_kind: PatternKind::Substring,
                pattern: substr.to_string(),
            }
        } else {
            LineFilter {
                kind,
                pattern_kind: PatternKind::Regex,
                pattern: like_to_regex(pattern_str),
            }
        },
    ))
}

// ---------------------------------------------------------------------------
// Unused-import suppression for the `Duration` re-export that
// downstream sections will reach for.
// ---------------------------------------------------------------------------
#[allow(dead_code)]
const _DURATION_REEXPORT_REMINDER: Option<Duration> = None;

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::Column;
    use datafusion::functions::core::expr_fn::get_field;
    use datafusion::logical_expr::{BinaryExpr, lit};

    fn defaults() -> TimeDefaults {
        TimeDefaults::DEFAULT
    }

    fn col(name: &str) -> Expr {
        Expr::Column(Column::new_unqualified(name))
    }

    fn ts_millis(ms: i64) -> Expr {
        Expr::Literal(ScalarValue::TimestampMillisecond(Some(ms), None), None)
    }

    fn eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::Eq,
            right: Box::new(right),
        })
    }

    fn neq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::NotEq,
            right: Box::new(right),
        })
    }

    fn gt(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::Gt,
            right: Box::new(right),
        })
    }

    fn lt_eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr(BinaryExpr {
            left: Box::new(left),
            op: Operator::LtEq,
            right: Box::new(right),
        })
    }

    /// Build the `get_field(labels, 'k')` Expr that DataFusion's SQL
    /// planner produces for `labels['k']`.
    fn labels_at(key: &str) -> Expr {
        get_field(col("labels"), key)
    }

    // ---- like_to_regex ----

    #[test]
    fn like_percent_becomes_dotstar() {
        assert_eq!(like_to_regex("foo%"), "^foo.*$");
        assert_eq!(like_to_regex("%foo%"), "^.*foo.*$");
    }

    #[test]
    fn like_underscore_becomes_dot() {
        assert_eq!(like_to_regex("a_b"), "^a.b$");
    }

    #[test]
    fn like_escapes_regex_metacharacters() {
        // Critical correctness case: `LIKE 'a.b%'` must not match `aXb...`.
        assert_eq!(like_to_regex("a.b%"), r"^a\.b.*$");
        assert_eq!(like_to_regex("a+b"), r"^a\+b$");
        assert_eq!(like_to_regex("/api/v1"), r"^/api/v1$");
        assert_eq!(like_to_regex("foo(bar)"), r"^foo\(bar\)$");
        assert_eq!(like_to_regex("a|b"), r"^a\|b$");
    }

    #[test]
    fn like_anchors_with_caret_and_dollar() {
        let re = like_to_regex("foo");
        assert!(re.starts_with('^'));
        assert!(re.ends_with('$'));
    }

    // ---- extract_substring_like ----

    #[test]
    fn substring_like_recognises_percent_wrapped_literal() {
        assert_eq!(extract_substring_like("%error%"), Some("error"));
    }

    #[test]
    fn substring_like_rejects_anchored_patterns() {
        assert_eq!(extract_substring_like("error%"), None);
        assert_eq!(extract_substring_like("%error"), None);
        assert_eq!(extract_substring_like("error"), None);
    }

    #[test]
    fn substring_like_rejects_embedded_wildcards() {
        assert_eq!(extract_substring_like("%a%b%"), None);
        assert_eq!(extract_substring_like("%a_b%"), None);
        assert_eq!(extract_substring_like("%a\\b%"), None);
    }

    // ---- in_list_to_regex ----

    #[test]
    fn in_list_produces_anchored_alternation() {
        assert_eq!(
            in_list_to_regex(&["a", "b", "c"]),
            Some("^(a|b|c)$".to_string())
        );
    }

    #[test]
    fn in_list_escapes_metacharacters_in_alternatives() {
        assert_eq!(
            in_list_to_regex(&["a.b", "c+d"]),
            Some(r"^(a\.b|c\+d)$".to_string())
        );
    }

    #[test]
    fn in_list_empty_returns_none() {
        assert_eq!(in_list_to_regex(&[]), None);
    }

    // ---- LineFilter::to_logql ----

    #[test]
    fn line_filter_substring_include_renders_pipe_eq() {
        let lf = LineFilter {
            kind: LineFilterKind::Include,
            pattern_kind: PatternKind::Substring,
            pattern: "error".into(),
        };
        assert_eq!(lf.to_logql(), r#"|= "error""#);
    }

    #[test]
    fn line_filter_regex_exclude_renders_bang_tilde() {
        let lf = LineFilter {
            kind: LineFilterKind::Exclude,
            pattern_kind: PatternKind::Regex,
            pattern: "^/health".into(),
        };
        assert_eq!(lf.to_logql(), r#"!~ "^/health""#);
    }

    #[test]
    fn line_filter_escapes_quotes_and_backslashes() {
        let lf = LineFilter {
            kind: LineFilterKind::Include,
            pattern_kind: PatternKind::Substring,
            pattern: r#"say "hi" \o/"#.into(),
        };
        assert_eq!(lf.to_logql(), r#"|= "say \"hi\" \\o/""#);
    }

    // ---- translate_metrics_filters: supported ----

    #[test]
    fn metrics_required_name_predicate_produces_simple_selector() {
        let filters = vec![eq(col("name"), lit("http_requests_total"))];
        let spec =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap();
        assert_eq!(spec.selector, "http_requests_total{}");
        assert!(matches!(spec.query_kind, QueryKind::Range));
        // No explicit ts → default window applied.
        assert_eq!(spec.window.span(), std::time::Duration::from_secs(15 * 60));
    }

    #[test]
    fn metrics_ts_between_overrides_default_window() {
        let filters = vec![
            eq(col("name"), lit("up")),
            Expr::Between(Between {
                expr: Box::new(col("ts")),
                negated: false,
                low: Box::new(ts_millis(1_000_000)),
                high: Box::new(ts_millis(2_000_000)),
            }),
        ];
        let spec =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap();
        assert_eq!(spec.window.start.timestamp_millis(), 1_000_000);
        assert_eq!(spec.window.end.timestamp_millis(), 2_000_000);
        assert!(matches!(spec.query_kind, QueryKind::Range));
    }

    #[test]
    fn metrics_ts_gt_only_yields_range_from_provided_start_to_now() {
        // Use a start that's recent enough not to bust `max_window`
        // (which is 24h by default). We pin to "5 minutes before now"
        // so the assertion stays meaningful regardless of clock skew.
        let start_ms = (Utc::now() - chrono::Duration::minutes(5)).timestamp_millis();
        let filters = vec![
            eq(col("name"), lit("up")),
            gt(col("ts"), ts_millis(start_ms)),
        ];
        let spec =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap();
        assert_eq!(spec.window.start.timestamp_millis(), start_ms);
        // `ts > X` with no upper bound → resolve_window falls back to `now`
        // for the end, so the window is positive.
        assert!(spec.window.end > spec.window.start);
    }

    #[test]
    fn metrics_ts_eq_yields_instant_query_kind() {
        let filters = vec![
            eq(col("name"), lit("up")),
            eq(col("ts"), ts_millis(1_700_000_000_000)),
        ];
        let spec =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap();
        match spec.query_kind {
            QueryKind::Instant { time } => {
                assert_eq!(time.timestamp_millis(), 1_700_000_000_000);
            }
            QueryKind::Range => panic!("expected Instant"),
        }
    }

    #[test]
    fn metrics_ts_predicate_with_flipped_operands_is_recognized() {
        let start_ms = (Utc::now() - chrono::Duration::minutes(5)).timestamp_millis();
        let filters = vec![
            eq(col("name"), lit("up")),
            // `ts_millis(...) <= ts` — literal on left, column on right.
            lt_eq(ts_millis(start_ms), col("ts")),
        ];
        let spec =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap();
        // Flipped to `ts >= ts_millis(...)` → lower bound.
        assert_eq!(spec.window.start.timestamp_millis(), start_ms);
    }

    // ---- label-matcher recognizer (tasks 3.5.2 / 3.5.3) ----

    #[test]
    fn label_matcher_eq_renders_to_selector_form() {
        let m = LabelMatcher {
            key: "service".into(),
            op: LabelMatcherOp::Eq,
            value: "api".into(),
        };
        assert_eq!(m.to_selector_str(), r#"service="api""#);
    }

    #[test]
    fn label_matcher_render_block_orders_and_joins_with_commas() {
        let matchers = vec![
            LabelMatcher {
                key: "app".into(),
                op: LabelMatcherOp::Eq,
                value: "checkout".into(),
            },
            LabelMatcher {
                key: "level".into(),
                op: LabelMatcherOp::NRegex,
                value: "^(debug|trace)$".into(),
            },
        ];
        assert_eq!(
            render_matchers_block(&matchers),
            r#"{app="checkout",level!~"^(debug|trace)$"}"#
        );
    }

    #[test]
    fn label_matcher_value_escapes_quotes_and_backslashes() {
        let m = LabelMatcher {
            key: "path".into(),
            op: LabelMatcherOp::Eq,
            value: r#"C:\Program Files\"App""#.into(),
        };
        let rendered = m.to_selector_str();
        assert!(rendered.contains(r#"\\"#), "got: {rendered}");
        assert!(rendered.contains(r#"\""#), "got: {rendered}");
    }

    // ---- translate_metrics_filters: labels['k'] pushdown ----

    #[test]
    fn metrics_label_eq_folds_into_selector() {
        let filters = vec![
            eq(col("name"), lit("http_requests_total")),
            eq(labels_at("service"), lit("api")),
        ];
        let spec =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap();
        assert_eq!(spec.selector, r#"http_requests_total{service="api"}"#);
        assert_eq!(spec.label_matchers.len(), 1);
        assert_eq!(spec.label_matchers[0].op, LabelMatcherOp::Eq);
        assert_eq!(spec.label_matchers[0].key, "service");
    }

    #[test]
    fn metrics_label_ne_folds_into_selector() {
        let filters = vec![
            eq(col("name"), lit("http_requests_total")),
            neq(labels_at("level"), lit("debug")),
        ];
        let spec =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap();
        assert_eq!(spec.selector, r#"http_requests_total{level!="debug"}"#);
    }

    #[test]
    fn metrics_label_like_folds_to_regex_matcher_with_anchors() {
        let filters = vec![
            eq(col("name"), lit("http_requests_total")),
            Expr::Like(Like {
                negated: false,
                expr: Box::new(labels_at("path")),
                pattern: Box::new(lit("/api/v1/%")),
                escape_char: None,
                case_insensitive: false,
            }),
        ];
        let spec =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap();
        assert_eq!(
            spec.selector,
            r#"http_requests_total{path=~"^/api/v1/.*$"}"#
        );
    }

    #[test]
    fn metrics_label_not_like_folds_to_negated_regex_matcher() {
        let filters = vec![
            eq(col("name"), lit("http_requests_total")),
            Expr::Like(Like {
                negated: true,
                expr: Box::new(labels_at("path")),
                pattern: Box::new(lit("/health")),
                escape_char: None,
                case_insensitive: false,
            }),
        ];
        let spec =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap();
        assert!(
            spec.selector.contains(r#"path!~"^/health$""#),
            "selector: {}",
            spec.selector
        );
    }

    #[test]
    fn metrics_label_in_list_folds_to_alternation_regex() {
        let filters = vec![
            eq(col("name"), lit("http_requests_total")),
            Expr::InList(InList {
                expr: Box::new(labels_at("status")),
                list: vec![lit("error"), lit("warn")],
                negated: false,
            }),
        ];
        let spec =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap();
        assert_eq!(
            spec.selector,
            r#"http_requests_total{status=~"^(error|warn)$"}"#
        );
    }

    #[test]
    fn metrics_label_not_in_list_folds_to_negated_alternation_regex() {
        let filters = vec![
            eq(col("name"), lit("http_requests_total")),
            Expr::InList(InList {
                expr: Box::new(labels_at("status")),
                list: vec![lit("ok"), lit("info")],
                negated: true,
            }),
        ];
        let spec =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap();
        assert!(
            spec.selector.contains(r#"status!~"^(ok|info)$""#),
            "selector: {}",
            spec.selector
        );
    }

    #[test]
    fn metrics_label_eq_with_flipped_operands_is_recognized() {
        // `'api' = labels['service']` — literal on the left.
        let filters = vec![
            eq(col("name"), lit("http_requests_total")),
            eq(lit("api"), labels_at("service")),
        ];
        let spec =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap();
        assert_eq!(spec.selector, r#"http_requests_total{service="api"}"#);
    }

    #[test]
    fn metrics_multiple_label_predicates_preserve_declaration_order() {
        let filters = vec![
            eq(col("name"), lit("http_requests_total")),
            eq(labels_at("service"), lit("api")),
            eq(labels_at("method"), lit("GET")),
        ];
        let spec =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap();
        assert_eq!(
            spec.selector,
            r#"http_requests_total{service="api",method="GET"}"#
        );
    }

    #[test]
    fn metrics_label_predicate_value_must_be_string_literal() {
        // `labels['service'] = 42` (numeric literal) is rejected with a
        // clear pointer at the escape hatch.
        let filters = vec![
            eq(col("name"), lit("up")),
            eq(labels_at("service"), lit(42_i64)),
        ];
        let err =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap_err();
        match err {
            OtelError::UnsupportedPushdown { hint, .. } => {
                assert!(
                    hint.contains("string literal") || hint.contains("UTF-8"),
                    "hint should explain literal type requirement: {hint}"
                );
            }
            other => panic!("expected UnsupportedPushdown, got {other:?}"),
        }
    }

    #[test]
    fn metrics_label_ilike_is_rejected_with_pointer_to_escape_hatch() {
        let filters = vec![
            eq(col("name"), lit("up")),
            Expr::Like(Like {
                negated: false,
                expr: Box::new(labels_at("service")),
                pattern: Box::new(lit("API%")),
                escape_char: None,
                case_insensitive: true,
            }),
        ];
        let err =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap_err();
        match err {
            OtelError::UnsupportedPushdown { hint, .. } => {
                assert!(
                    hint.contains("ILIKE") || hint.contains("case-insensitive"),
                    "hint: {hint}"
                );
            }
            other => panic!("expected UnsupportedPushdown, got {other:?}"),
        }
    }

    // ---- translate_metrics_filters: unsupported ----

    #[test]
    fn metrics_without_name_predicate_is_rejected() {
        let filters: Vec<Expr> = vec![];
        let err =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap_err();
        match err {
            OtelError::UnsupportedPushdown {
                source_name, hint, ..
            } => {
                assert_eq!(source_name, "prom");
                assert!(
                    hint.contains("prom_query("),
                    "hint must point at prom_query: {hint}"
                );
                assert!(hint.contains("name = '<metric>'") || hint.contains("name ="));
            }
            other => panic!("expected UnsupportedPushdown, got {other:?}"),
        }
    }

    #[test]
    fn metrics_value_filter_falls_through_to_unsupported_with_hint() {
        // `value > 0.5` (a non-`name`, non-`ts` predicate) is the
        // canonical "fall-through to escape hatch" case.
        let filters = vec![eq(col("name"), lit("up")), gt(col("value"), lit(0.5_f64))];
        let err =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap_err();
        match err {
            OtelError::UnsupportedPushdown {
                predicate, hint, ..
            } => {
                assert!(predicate.contains("value"), "got: {predicate}");
                assert!(hint.contains("prom_query("), "got: {hint}");
            }
            other => panic!("expected UnsupportedPushdown, got {other:?}"),
        }
    }

    #[test]
    fn metrics_aggregate_pushdown_is_explicitly_not_yet_supported() {
        let filters = vec![eq(col("name"), lit("up"))];
        let err = translate_metrics_filters(
            &filters,
            None,
            Some(Aggregate::Sum),
            None,
            &defaults(),
            "prom",
        )
        .unwrap_err();
        match err {
            OtelError::UnsupportedPushdown { hint, .. } => {
                assert!(
                    hint.contains("section 4.2"),
                    "hint should reference 4.2: {hint}"
                );
            }
            other => panic!("expected UnsupportedPushdown, got {other:?}"),
        }
    }

    #[test]
    fn metrics_duplicate_name_predicate_is_rejected() {
        let filters = vec![eq(col("name"), lit("a")), eq(col("name"), lit("b"))];
        let err =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap_err();
        assert!(matches!(err, OtelError::UnsupportedPushdown { .. }));
    }

    #[test]
    fn metrics_window_exceeding_max_is_rejected_at_translator() {
        let filters = vec![
            eq(col("name"), lit("up")),
            Expr::Between(Between {
                expr: Box::new(col("ts")),
                negated: false,
                low: Box::new(ts_millis(0)),
                high: Box::new(ts_millis(48 * 60 * 60 * 1000)), // 48h ≫ 24h max
            }),
        ];
        let err =
            translate_metrics_filters(&filters, None, None, None, &defaults(), "prom").unwrap_err();
        match err {
            OtelError::WindowTooLarge { source_name, .. } => assert_eq!(source_name, "prom"),
            other => panic!("expected WindowTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn metrics_limit_is_passed_through_to_spec() {
        let filters = vec![eq(col("name"), lit("up"))];
        let spec = translate_metrics_filters(&filters, None, None, Some(100), &defaults(), "prom")
            .unwrap();
        assert_eq!(spec.limit, Some(100));
    }

    // ---- translate_logs_filters: supported ----

    #[test]
    fn logs_line_like_substring_becomes_pipe_eq() {
        let filters = vec![Expr::Like(Like {
            negated: false,
            expr: Box::new(col("line")),
            pattern: Box::new(lit("%error%")),
            escape_char: None,
            case_insensitive: false,
        })];
        let spec = translate_logs_filters(&filters, None, &defaults(), "loki").unwrap();
        assert_eq!(spec.line_filters.len(), 1);
        let lf = &spec.line_filters[0];
        assert_eq!(lf.kind, LineFilterKind::Include);
        assert_eq!(lf.pattern_kind, PatternKind::Substring);
        assert_eq!(lf.pattern, "error");
        assert_eq!(lf.to_logql(), r#"|= "error""#);
    }

    #[test]
    fn logs_line_not_like_substring_becomes_bang_eq() {
        let filters = vec![Expr::Like(Like {
            negated: true,
            expr: Box::new(col("line")),
            pattern: Box::new(lit("%retry%")),
            escape_char: None,
            case_insensitive: false,
        })];
        let spec = translate_logs_filters(&filters, None, &defaults(), "loki").unwrap();
        let lf = &spec.line_filters[0];
        assert_eq!(lf.kind, LineFilterKind::Exclude);
        assert_eq!(lf.to_logql(), r#"!= "retry""#);
    }

    #[test]
    fn logs_line_like_anchored_falls_back_to_regex() {
        let filters = vec![Expr::Like(Like {
            negated: false,
            expr: Box::new(col("line")),
            pattern: Box::new(lit("err%")),
            escape_char: None,
            case_insensitive: false,
        })];
        let spec = translate_logs_filters(&filters, None, &defaults(), "loki").unwrap();
        let lf = &spec.line_filters[0];
        assert_eq!(lf.pattern_kind, PatternKind::Regex);
        assert_eq!(lf.pattern, "^err.*$");
        assert_eq!(lf.to_logql(), r#"|~ "^err.*$""#);
    }

    #[test]
    fn logs_chained_line_filters_preserve_declaration_order() {
        let filters = vec![
            Expr::Like(Like {
                negated: false,
                expr: Box::new(col("line")),
                pattern: Box::new(lit("%error%")),
                escape_char: None,
                case_insensitive: false,
            }),
            Expr::Like(Like {
                negated: true,
                expr: Box::new(col("line")),
                pattern: Box::new(lit("%retry%")),
                escape_char: None,
                case_insensitive: false,
            }),
        ];
        let spec = translate_logs_filters(&filters, None, &defaults(), "loki").unwrap();
        assert_eq!(spec.line_filters.len(), 2);
        assert_eq!(spec.line_filters[0].kind, LineFilterKind::Include);
        assert_eq!(spec.line_filters[1].kind, LineFilterKind::Exclude);
    }

    // ---- translate_logs_filters: labels['k'] pushdown ----

    #[test]
    fn logs_label_eq_produces_stream_selector() {
        let filters = vec![eq(labels_at("app"), lit("checkout"))];
        let spec = translate_logs_filters(&filters, None, &defaults(), "loki").unwrap();
        assert_eq!(spec.selector, r#"{app="checkout"}"#);
        assert_eq!(spec.label_matchers.len(), 1);
    }

    #[test]
    fn logs_label_in_produces_alternation_regex_matcher() {
        let filters = vec![Expr::InList(InList {
            expr: Box::new(labels_at("level")),
            list: vec![lit("error"), lit("warn")],
            negated: false,
        })];
        let spec = translate_logs_filters(&filters, None, &defaults(), "loki").unwrap();
        assert_eq!(spec.selector, r#"{level=~"^(error|warn)$"}"#);
    }

    #[test]
    fn logs_label_matchers_and_line_filter_compose() {
        let filters = vec![
            eq(labels_at("app"), lit("checkout")),
            Expr::Like(Like {
                negated: false,
                expr: Box::new(col("line")),
                pattern: Box::new(lit("%timeout%")),
                escape_char: None,
                case_insensitive: false,
            }),
        ];
        let spec = translate_logs_filters(&filters, None, &defaults(), "loki").unwrap();
        assert_eq!(spec.selector, r#"{app="checkout"}"#);
        assert_eq!(spec.line_filters.len(), 1);
        assert_eq!(spec.line_filters[0].to_logql(), r#"|= "timeout""#);
    }

    #[test]
    fn logs_label_matcher_with_no_predicate_yields_empty_selector_for_provider_rejection() {
        // No label-matcher predicate → empty selector. Section 5's
        // `LokiLogsTable` short-circuits empty selectors before the
        // upstream call, so this remains a safe interim state.
        let filters = vec![Expr::Like(Like {
            negated: false,
            expr: Box::new(col("line")),
            pattern: Box::new(lit("%error%")),
            escape_char: None,
            case_insensitive: false,
        })];
        let spec = translate_logs_filters(&filters, None, &defaults(), "loki").unwrap();
        assert_eq!(spec.selector, "{}");
        assert!(spec.label_matchers.is_empty());
    }

    // ---- translate_logs_filters: unsupported ----

    #[test]
    fn logs_ilike_is_rejected_with_pointer_to_loki_query() {
        let filters = vec![Expr::Like(Like {
            negated: false,
            expr: Box::new(col("line")),
            pattern: Box::new(lit("%error%")),
            escape_char: None,
            case_insensitive: true,
        })];
        let err = translate_logs_filters(&filters, None, &defaults(), "loki").unwrap_err();
        match err {
            OtelError::UnsupportedPushdown { hint, .. } => {
                assert!(hint.contains("ILIKE") || hint.contains("case-insensitive"));
                assert!(hint.contains("loki_query(") || hint.contains("regex"));
            }
            other => panic!("expected UnsupportedPushdown, got {other:?}"),
        }
    }

    #[test]
    fn logs_ts_eq_is_rejected_because_loki_has_no_instant_query() {
        let filters = vec![eq(col("ts"), ts_millis(1_700_000_000_000))];
        let err = translate_logs_filters(&filters, None, &defaults(), "loki").unwrap_err();
        match err {
            OtelError::UnsupportedPushdown { hint, .. } => {
                assert!(hint.contains("loki_query("), "got: {hint}");
            }
            other => panic!("expected UnsupportedPushdown, got {other:?}"),
        }
    }

    #[test]
    fn logs_arbitrary_value_predicate_falls_through() {
        let filters = vec![gt(col("value"), lit(0.5_f64))];
        let err = translate_logs_filters(&filters, None, &defaults(), "loki").unwrap_err();
        match err {
            OtelError::UnsupportedPushdown { hint, .. } => {
                assert!(hint.contains("loki_range(") || hint.contains("loki_query("));
            }
            other => panic!("expected UnsupportedPushdown, got {other:?}"),
        }
    }
}
