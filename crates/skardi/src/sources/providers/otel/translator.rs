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
//! # Supported predicate matrix (v1)
//!
//! ## `metrics` table (Prometheus backend)
//!
//! | SQL shape                                                  | Translation                       | Endpoint                       |
//! | ---------------------------------------------------------- | --------------------------------- | ------------------------------ |
//! | `WHERE name = '<metric>'` (REQUIRED)                       | selector `<metric>{}`             | both                           |
//! | `WHERE ts BETWEEN <start> AND <end>`                       | `start=`, `end=`, `step=<default>`| `/api/v1/query_range`          |
//! | `WHERE ts >= <start> AND ts <= <end>`                      | same                              | `/api/v1/query_range`          |
//! | `WHERE ts > <start>` (no upper bound)                      | `start=<v>`, `end=now`, `step=…`  | `/api/v1/query_range`          |
//! | `WHERE ts = <ts>`                                           | `time=<ts>`                       | `/api/v1/query`                |
//! | no `ts` predicate                                          | `default_window` applied          | `/api/v1/query_range`          |
//! | `LIMIT n`                                                  | client-side cap after translation | both                           |
//!
//! Label-key matchers (`WHERE labels['k'] = '<v>'`, `LIKE`, `IN`, etc.)
//! are documented in `design.md` Decision 4 and recognized by the
//! provider in section 4 of the change. v1 of this translator returns
//! [`OtelError::UnsupportedPushdown`] for any predicate it does not
//! recognize, with a hint pointing the caller at `prom_query`.
//!
//! ## `logs` table (Loki backend)
//!
//! | SQL shape                                                  | Translation                       |
//! | ---------------------------------------------------------- | --------------------------------- |
//! | `WHERE ts BETWEEN <start> AND <end>`                       | `start=`, `end=`                  |
//! | `WHERE line LIKE '%<substr>%'` (pure substring)            | LogQL `\|= "<substr>"`            |
//! | `WHERE line LIKE 'foo%'` (anchored / wildcard)             | LogQL `\|~ "^foo.*$"`             |
//! | `WHERE line NOT LIKE '%<substr>%'`                         | LogQL `!= "<substr>"`             |
//! | `LIMIT n`                                                  | LogQL `limit=n`                   |
//!
//! At least one stream-label predicate (`labels['app'] = '…'`) is
//! REQUIRED per `design.md`; v1 enforces this by rejecting empty
//! selectors. Recognition of `labels['k']` predicates lands in
//! section 5 alongside the `LokiLogsTable` provider.
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
use datafusion::logical_expr::{Between, BinaryExpr, Expr, Like, Operator};

use super::error::OtelError;
use super::time::{TimeDefaults, TimeWindow, resolve_window};

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
    /// Full PromQL selector, e.g. `http_requests_total{service="api"}`.
    /// For v1 (label matchers not yet implemented) this is always
    /// `<metric>{}`.
    pub selector: String,
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
    /// LogQL stream selector, e.g. `{app="checkout"}`. v1 emits the
    /// empty selector `{}` since label-key matchers are not yet
    /// recognized — section 5 will reject empty selectors before they
    /// reach the upstream.
    pub selector: String,
    /// Ordered list of line-filter stages applied after the selector.
    pub line_filters: Vec<LineFilter>,
    /// Resolved time window for the range query.
    pub window: TimeWindow,
    /// Optional row cap (becomes Loki's `limit=` query param).
    pub limit: Option<usize>,
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
// translate_metrics_filters (task 3.5.2)
// ---------------------------------------------------------------------------

/// Translate a DataFusion filter / aggregate plan into the
/// [`PromQuerySpec`] consumed by the Prometheus provider.
///
/// v1 recognizes:
/// - `name = '<metric>'` (REQUIRED — the translator errors without it).
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
                   prom_query('<your PromQL here>') instead. Note: `labels['k']` \
                   matchers, value comparisons, and arithmetic on projected \
                   columns are not yet pushed down. See the translator module \
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

    let selector = format!("{metric_name}{{}}");

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
/// v1 recognizes:
/// - `ts` predicates: `BETWEEN`, `>=`, `<=`, `>`, `<`.
/// - `line LIKE '<pat>'` / `line NOT LIKE '<pat>'`.
/// - `LIMIT n`.
///
/// At least one stream-label predicate (`labels['app'] = '…'`) is
/// REQUIRED per `design.md`. v1 of this translator does not yet
/// recognize label predicates and returns an empty selector — section
/// 5 (the `LokiLogsTable` provider) rejects empty selectors before
/// reaching the upstream so this remains a safe interim state.
pub fn translate_logs_filters(
    filters: &[Expr],
    limit: Option<usize>,
    defaults: &TimeDefaults,
    source_name: &str,
) -> Result<LokiQuerySpec, OtelError> {
    let mut line_filters: Vec<LineFilter> = Vec::new();
    let mut ts_lower: Option<DateTime<Utc>> = None;
    let mut ts_upper: Option<DateTime<Utc>> = None;

    for filter in filters {
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
                   instead. Note: `labels['k']` matchers and LogQL pipelines \
                   (| json, | line_format, …) are not yet pushed down."
                .to_string(),
        });
    }

    let window = resolve_window(ts_lower, ts_upper, None, defaults, source_name)?;

    Ok(LokiQuerySpec {
        // Empty selector — section 5 rejects this before issuing an upstream
        // call; updated once label-key matcher recognition lands.
        selector: "{}".to_string(),
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
