//! The two `rss` [`TableProvider`]s, and the partition pruning that decides
//! which subscriptions a scan visits.
//!
//! Both tables are read-only by construction: no mutating hook is implemented
//! here, so [`TableProvider`]'s own defaults refuse writes — `insert_into` and
//! `delete_from` each return `not_impl_err` (datafusion-catalog 52.5.0,
//! `src/table.rs:329` and `src/table.rs:341`). `the_tables_are_read_only` pins
//! the `insert_into` refusal by its message.
//!
//! ## One allowlist, consulted by both hooks
//!
//! `supports_filters_pushdown` promises DataFusion which predicates this
//! provider applies itself, and `prune_feeds` is what actually applies them. A
//! disagreement between the two is a correctness bug in one direction: for
//! every predicate reported `Exact`, DataFusion drops that predicate from the
//! `Filter` it rebuilds above the scan (datafusion-optimizer 52.5.0,
//! `src/push_down_filter.rs:1172-1174` — the surviving predicates are the ones
//! whose classification `!= Exact`). A predicate claimed and then not applied
//! therefore returns extra rows silently. So both hooks route through one
//! classifier, `feed_filter`, and nothing else in this module decides what is
//! prunable. `non_prunable_filters_are_applied_above_the_scan` is the test that
//! fails if they drift apart.
//!
//! Pruning is what makes the claim true. The engine stamps a row's `feed` and
//! `feed_url` with the subscription's own `name` and `url`
//! (`engine.rs:472`, `build_items_batch(&sub.name, &sub.url, …)`), so
//! restricting *which subscriptions a scan visits* is exactly equivalent to
//! applying a predicate that constrains those two columns to a fixed set of
//! values to the rows — which is what earns `Exact` rather than `Inexact`.
//!
//! ## Why the allowlist is this narrow
//!
//! Only three shapes over `items.feed` / `items.feed_url` prune — equality, a
//! non-negated `IN`, and a disjunction of those over *one* of the two columns —
//! and everything else stays `Unsupported`. That is not an oversight, it is the
//! shape of the source. An RSS subscription is one fixed URL with no query
//! parameters, so *no* predicate can narrow what the wire returns — there is no
//! request for `WHERE title = …` to become part of. Choosing a subset of feeds
//! to fetch is the only work a predicate can do here, so the allowlist is
//! exactly the predicates that name a fixed set of feeds.
//!
//! A conjunction needs no splitting here: the optimizer calls
//! `split_conjunction` on the filter predicate before consulting the provider
//! (datafusion-optimizer 52.5.0, `src/push_down_filter.rs:1135`), so `AND`
//! arrives as separate `Expr`s. That function recurses only through
//! `Operator::And` and through aliases (datafusion-expr 52.5.0,
//! `src/utils.rs:968-984`), so a disjunction arrives whole, as a single
//! `BinaryExpr` — which the classifier walks itself.
//!
//! ## Disjunction, and where the walk stops
//!
//! A disjunction prunes to the *union* of its leaves, and is `Exact` only when
//! every leaf is prunable over the same column. `feed = 'a' OR feed = 'b'`
//! visits `{a, b}`; a single leaf the classifier does not understand rejects the
//! whole predicate, because a row satisfying that leaf could come from any
//! subscription. So `feed = 'a' OR title = 't'`, `feed = 'a' OR feed > 'b'`, a
//! negated leaf, and an `AND` nested under the `OR` are all `Unsupported`.
//!
//! Mixing the two feed columns — `feed = 'a' OR feed_url = '…'` — is refused
//! too, and that one is a choice rather than a necessity: both columns name a
//! subscription, so the union across them would be sound. It is left out so the
//! rule stays "one feed column per disjunction" and a `FeedFilter` stays one
//! key against a set of values. `docs/rss.md` records it as the residual
//! limitation, and
//! `a_disjunction_mixing_feed_and_feed_url_does_not_prune` in
//! `integration_tests.rs` is what would have to change to support it.
//!
//! This is also what makes a short `IN` list prune. DataFusion rewrites
//! `col IN (…)` into a left-deep chain of `OR`ed equalities when the list holds
//! one value, or at most `THRESHOLD_INLINE_INLIST` (3) values with a plain
//! column on the left (datafusion-optimizer 52.5.0,
//! `src/simplify_expressions/inlist_simplifier.rs:38-56`, the non-negated fold
//! at `:82-90`, and the constant at
//! `src/simplify_expressions/expr_simplifier.rs:111`), so below four values the
//! `InList` arm below is unreachable from SQL and the disjunction arm is what
//! prunes. `integration_tests.rs` pins both lengths, from the request counts:
//! `a_short_in_list_is_rewritten_to_a_disjunction_and_still_prunes` at two
//! values and `a_long_in_list_prunes_to_its_members` at four.
//!
//! ## Intersection, and the empty result
//!
//! Each prunable predicate narrows the surviving subscription list, so several
//! of them intersect: `feed IN ('a','b') AND feed = 'b'` visits only `b`. A
//! value that names no subscription narrows it to nothing, and a scan pruned to
//! no feeds performs no fetches at all. [`RssScanExec`] still advertises one
//! partition in that case (its partition count floors at 1), and that single
//! partition serves zero rows — `exec.rs`'s
//! `an_empty_feed_list_is_one_partition_serving_nothing` pins it from the other
//! side.
//!
//! ## `feeds` pushes down nothing
//!
//! The `feeds` table reports `Unsupported` for every predicate, including ones
//! naming its own `name`/`url` columns, and its scan visits every subscription.
//! Pruning it by `name` would work mechanically — the column *is* the
//! subscription key — and is left out deliberately: `feeds` is a pure state read
//! whose whole job is to be total. It is the only place a never-fetched or dead
//! subscription appears at all, and the absence check the design prescribes
//! (`feeds LEFT JOIN items … WHERE items.feed IS NULL`) is only meaningful if
//! every subscription has a row. `a_feeds_scan_is_total_over_subscriptions`
//! covers that query end to end.
//!
//! ## The plan `scan()` returns is single-execution
//!
//! LIMIT bookkeeping and the scan deadline are *scan-scoped* state, minted
//! when the plan is constructed (`exec.rs`'s `ScanShared`): they have to
//! live on the plan object, because one execution's partitions are just N
//! `execute(i)` calls on that object and DataFusion offers no other place
//! for cross-partition state. The consequences for anyone holding a plan:
//! executing the same plan object a second time finds its own already
//! satisfied LIMIT and serves zero rows, and executing a plan more than
//! `scan_timeout_seconds` after it was built finds its deadline already
//! passed — both silently, by design. DataFusion's own re-execution path
//! (`RecursiveQueryExec` driving `WITH RECURSIVE`) rebuilds the state by
//! calling `reset_state`, which this plan overrides; any caller that caches
//! a physical plan and re-collects it outside that path must do the same,
//! or the scan silently shrinks.
//! `reset_state_rebuilds_a_scan_whose_limit_was_already_satisfied` in
//! `exec.rs` pins both halves.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::Statistics;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{Expr, Operator, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;

use super::ResolvedSubscription;
use super::engine::RssEngine;
use super::exec::{RssScanExec, RssTableKind};

/// `items.feed`: the name of the subscription a row came from.
const FEED_COLUMN: &str = "feed";
/// `items.feed_url`: that subscription's configured URL.
const FEED_URL_COLUMN: &str = "feed_url";

/// Which field of a subscription a prunable predicate constrains.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FeedKey {
    /// Matched against [`ResolvedSubscription::name`].
    Name,
    /// Matched against [`ResolvedSubscription::url`], which is how a
    /// `feed_url` predicate reaches a subscription *name* — the only thing the
    /// scan below takes.
    Url,
}

impl FeedKey {
    /// The key a column name selects, or `None` for any other column.
    ///
    /// The column's table qualifier is ignored: a predicate that reached a
    /// table scan's filter list can only reference that scan's own columns, and
    /// this provider serves one table per scan.
    fn of(column: &str) -> Option<Self> {
        match column {
            FEED_COLUMN => Some(Self::Name),
            FEED_URL_COLUMN => Some(Self::Url),
            _ => None,
        }
    }

    /// This key's value on one subscription.
    fn read(self, sub: &ResolvedSubscription) -> &str {
        match self {
            Self::Name => &sub.name,
            Self::Url => &sub.url,
        }
    }
}

/// A predicate this provider prunes on: one subscription field restricted to a
/// fixed set of string values.
struct FeedFilter<'a> {
    key: FeedKey,
    values: Vec<&'a str>,
}

impl FeedFilter<'_> {
    /// Whether `sub` can contribute a row satisfying this predicate.
    fn admits(&self, sub: &ResolvedSubscription) -> bool {
        self.values.contains(&self.key.read(sub))
    }
}

/// Classify one predicate: `Some` for the shapes this provider prunes on (and
/// therefore reports `Exact`), `None` for everything else.
///
/// This is the single definition of "prunable" — see the module doc on why both
/// `TableProvider` hooks must consult it rather than each deciding for itself.
fn feed_filter(expr: &Expr) -> Option<FeedFilter<'_>> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::Eq => {
            // Either operand order. A column on *both* sides also matches this
            // pattern; it is rejected below, when `string_literal` finds the
            // other operand is not a literal.
            let (column, literal) = match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(column), literal) | (literal, Expr::Column(column)) => {
                    (column, literal)
                }
                _ => return None,
            };
            Some(FeedFilter {
                key: FeedKey::of(&column.name)?,
                values: vec![string_literal(literal)?],
            })
        }
        Expr::InList(in_list) if !in_list.negated => {
            let Expr::Column(column) = in_list.expr.as_ref() else {
                return None;
            };
            let key = FeedKey::of(&column.name)?;
            let mut values = Vec::with_capacity(in_list.list.len());
            for item in &in_list.list {
                // One non-literal element makes the whole predicate
                // unprunable: a partial reading of an `IN` list would drop
                // feeds the predicate might still admit.
                values.push(string_literal(item)?);
            }
            Some(FeedFilter { key, values })
        }
        // A disjunction prunes when *every* leaf does, over one column: the
        // union of the leaves' value sets is then exactly the subscriptions the
        // predicate can admit a row from, so pruning to that union applies the
        // predicate rather than approximating it. One unprunable leaf makes the
        // union unbounded — `feed = 'a' OR title = 't'` admits a row from every
        // subscription — so it rejects the whole disjunction rather than
        // pruning to the part it understood.
        //
        // Recursing through this same function is what makes nesting fall out:
        // `Or` is left-deep as the parser and the `IN` rewrite both build it,
        // and an `And` under an `Or` has no arm here, so it returns `None` and
        // takes the disjunction with it.
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            let left = feed_filter(&binary.left)?;
            let right = feed_filter(&binary.right)?;
            // One column per disjunction. A union across `feed` and `feed_url`
            // would also be sound — both name a subscription — and is refused
            // to keep the rule statable and `FeedFilter` a single key; see the
            // module doc.
            if left.key != right.key {
                return None;
            }
            let mut values = left.values;
            values.extend(right.values);
            Some(FeedFilter {
                key: left.key,
                values,
            })
        }
        _ => None,
    }
}

/// The non-NULL string a literal expression holds.
///
/// `None` covers three cases that all mean the same thing here — not a literal,
/// not a string, or NULL. NULL is excluded rather than treated as a value that
/// matches nothing: a subscription name is never NULL, so the two readings
/// agree on the rows, and refusing to classify it keeps the `Exact` claim to
/// predicates whose three-valued logic this module does not have to reason
/// about. DataFusion then filters it above the scan.
fn string_literal(expr: &Expr) -> Option<&str> {
    match expr {
        // `try_as_str` reports `Some(None)` for a NULL of a string type and
        // `None` for a non-string scalar; both flatten to `None`.
        Expr::Literal(value, _) => value.try_as_str().flatten(),
        _ => None,
    }
}

/// The subscriptions an `items` scan must visit, in subscription order.
///
/// Every prunable predicate in `filters` narrows the list, so they intersect;
/// predicates outside the allowlist are skipped, and with none of them prunable
/// the result is every subscription. A value naming no subscription narrows the
/// list to empty, which is a scan that fetches nothing.
pub(crate) fn prune_feeds(filters: &[Expr], subs: &[ResolvedSubscription]) -> Vec<String> {
    let mut kept: Vec<&ResolvedSubscription> = subs.iter().collect();
    for filter in filters {
        if let Some(prunable) = feed_filter(filter) {
            kept.retain(|sub| prunable.admits(sub));
        }
    }
    kept.into_iter().map(|sub| sub.name.clone()).collect()
}

/// One of the two `rss` tables, bound to the engine that serves it.
pub struct RssTableProvider {
    engine: Arc<RssEngine>,
    kind: RssTableKind,
    schema: SchemaRef,
}

impl fmt::Debug for RssTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RssTableProvider")
            .field("kind", &self.kind)
            .field("subscriptions", &self.engine.subscriptions().len())
            .finish()
    }
}

impl RssTableProvider {
    /// The `feeds` health table: one row per subscription, no pushdown.
    pub fn feeds(engine: Arc<RssEngine>) -> Self {
        Self::new(engine, RssTableKind::Feeds)
    }

    /// The `items` table: one window per subscription, with feed pushdown.
    pub fn items(engine: Arc<RssEngine>) -> Self {
        Self::new(engine, RssTableKind::Items)
    }

    fn new(engine: Arc<RssEngine>, kind: RssTableKind) -> Self {
        Self {
            engine,
            kind,
            schema: kind.schema(),
        }
    }

    /// Every subscription's name, in subscription order.
    fn all_feeds(&self) -> Vec<String> {
        self.engine
            .subscriptions()
            .iter()
            .map(|sub| sub.name.clone())
            .collect()
    }
}

#[async_trait::async_trait]
impl TableProvider for RssTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    /// Classify each filter with the same allowlist [`Self::scan`] prunes on,
    /// so planning and execution cannot disagree about what was applied.
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|filter| match self.kind {
                RssTableKind::Items if feed_filter(filter).is_some() => {
                    TableProviderFilterPushDown::Exact
                }
                // `feeds` claims nothing, and no `items` predicate outside the
                // allowlist can reach the wire — see the module doc.
                _ => TableProviderFilterPushDown::Unsupported,
            })
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let feeds = match self.kind {
            RssTableKind::Items => prune_feeds(filters, self.engine.subscriptions()),
            // Every predicate on `feeds` was reported `Unsupported`, so
            // DataFusion keeps a `Filter` above this scan and ignoring
            // `filters` here loses nothing.
            RssTableKind::Feeds => self.all_feeds(),
        };
        Ok(Arc::new(RssScanExec::new(
            Arc::clone(&self.engine),
            self.kind,
            feeds,
            projection.cloned(),
            limit,
        )?))
    }

    /// No statistics, for the reason [`RssScanExec`] advertises none: an
    /// `items` row count is unknowable before fetching, and `feeds` must not
    /// claim one either when both tables share one plan.
    fn statistics(&self) -> Option<Statistics> {
        None
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType;
    use datafusion::common::ScalarValue;
    use datafusion::execution::TaskContext;
    use datafusion::logical_expr::TableProviderFilterPushDown::{Exact, Unsupported};
    use datafusion::logical_expr::{cast, col, lit};
    use datafusion::prelude::SessionContext;

    use super::*;
    use crate::sources::providers::rss::schema::{feeds_schema, items_schema};
    use crate::sources::providers::rss::testutil::{
        MockFeedServer, MockResponse, RSS2_MINIMAL, collect_stream, feed_urls, str_col,
        test_engine, total_rows,
    };

    /// A subscription list for the pruning tests, which need no engine.
    fn subs(pairs: &[(&str, &str)]) -> Vec<ResolvedSubscription> {
        pairs
            .iter()
            .map(|(name, url)| ResolvedSubscription {
                name: (*name).to_string(),
                url: (*url).to_string(),
            })
            .collect()
    }

    /// An engine whose subscriptions point at a host nothing listens on — for
    /// the planning tests, which must not reach the network at all.
    fn offline_engine(feeds: &[&str]) -> Arc<RssEngine> {
        let urls: Vec<(String, String)> = feeds
            .iter()
            .map(|name| {
                (
                    (*name).to_string(),
                    format!("http://feed.invalid/{name}.xml"),
                )
            })
            .collect();
        Arc::new(test_engine(&urls, |_| {}))
    }

    fn items_provider_with_feeds(feeds: &[&str]) -> RssTableProvider {
        RssTableProvider::items(offline_engine(feeds))
    }

    /// One-line `EXPLAIN` of a plan, for asserting the pruned feed count that
    /// `scan` handed the exec.
    fn one_line(plan: &Arc<dyn ExecutionPlan>) -> String {
        datafusion::physical_plan::displayable(plan.as_ref())
            .one_line()
            .to_string()
            .trim_end()
            .to_string()
    }

    /// A context with both tables registered as plain `items`/`feeds`.
    ///
    /// Task 14 owns the catalog wiring (`news.main.items`); the properties here
    /// — classification, pruning, and what DataFusion filters above the scan —
    /// do not depend on the path a table is reachable at.
    fn sql_context(engine: &Arc<RssEngine>) -> SessionContext {
        let ctx = SessionContext::new();
        ctx.register_table(
            "items",
            Arc::new(RssTableProvider::items(Arc::clone(engine))),
        )
        .expect("register items");
        ctx.register_table(
            "feeds",
            Arc::new(RssTableProvider::feeds(Arc::clone(engine))),
        )
        .expect("register feeds");
        ctx
    }

    async fn query(ctx: &SessionContext, sql: &str) -> Vec<arrow::record_batch::RecordBatch> {
        ctx.sql(sql)
            .await
            .unwrap_or_else(|e| panic!("plan {sql:?}: {e}"))
            .collect()
            .await
            .unwrap_or_else(|e| panic!("execute {sql:?}: {e}"))
    }

    /// Values of one `Utf8` column across every batch of a result.
    fn column(batches: &[arrow::record_batch::RecordBatch], name: &str) -> Vec<String> {
        batches.iter().flat_map(|b| str_col(b, name)).collect()
    }

    #[tokio::test]
    async fn pushdown_classification_is_exact_only_for_feed_predicates() {
        let p = items_provider_with_feeds(&["a", "b"]);
        let feed_eq = col("feed").eq(lit("a"));
        let url_in = col("feed_url").in_list(vec![lit("http://x/1")], false);
        let title_eq = col("title").eq(lit("t"));
        let feed_gt = col("feed").gt(lit("a"));
        let neg_in = col("feed").in_list(vec![lit("a")], true);
        let got = p
            .supports_filters_pushdown(&[&feed_eq, &url_in, &title_eq, &feed_gt, &neg_in])
            .expect("classify");
        assert_eq!(
            got,
            vec![Exact, Exact, Unsupported, Unsupported, Unsupported]
        );
    }

    /// `feeds` reports `Unsupported` for everything, including the shapes
    /// `items` reports `Exact` — the kind check is not incidental.
    #[tokio::test]
    async fn the_feeds_table_pushes_down_nothing() {
        let p = RssTableProvider::feeds(offline_engine(&["a", "b"]));
        let name_eq = col("name").eq(lit("a"));
        let url_in = col("url").in_list(vec![lit("http://x/1")], false);
        let feed_eq = col("feed").eq(lit("a"));
        let got = p
            .supports_filters_pushdown(&[&name_eq, &url_in, &feed_eq])
            .expect("classify");
        assert_eq!(got, vec![Unsupported, Unsupported, Unsupported]);
    }

    #[test]
    fn prune_intersects_predicates_and_maps_urls() {
        let subs = subs(&[
            ("a", "http://x/a"),
            ("b", "http://x/b"),
            ("c", "http://x/c"),
        ]);
        assert_eq!(prune_feeds(&[col("feed").eq(lit("b"))], &subs), vec!["b"]);
        assert_eq!(
            prune_feeds(
                &[col("feed").in_list(vec![lit("a"), lit("c")], false)],
                &subs
            ),
            vec!["a", "c"]
        );
        assert_eq!(
            prune_feeds(&[col("feed_url").eq(lit("http://x/b"))], &subs),
            vec!["b"]
        );
        // Intersection: feed IN (a, b) AND feed = 'b' → [b]
        assert_eq!(
            prune_feeds(
                &[
                    col("feed").in_list(vec![lit("a"), lit("b")], false),
                    col("feed").eq(lit("b")),
                ],
                &subs
            ),
            vec!["b"]
        );
        // Unknown value → empty (zero partitions of work, zero fetches)
        assert!(prune_feeds(&[col("feed").eq(lit("nope"))], &subs).is_empty());
        // Two prunable predicates that cannot both hold → empty.
        assert!(
            prune_feeds(&[col("feed").eq(lit("a")), col("feed").eq(lit("b"))], &subs).is_empty()
        );
        // Reversed operands
        assert_eq!(prune_feeds(&[lit("a").eq(col("feed"))], &subs), vec!["a"]);
        // Mixed keys still intersect: the url names `a`, the name names `b`.
        assert!(
            prune_feeds(
                &[
                    col("feed_url").eq(lit("http://x/a")),
                    col("feed").eq(lit("b")),
                ],
                &subs
            )
            .is_empty()
        );
        // No prunable predicate → all
        assert_eq!(prune_feeds(&[col("title").eq(lit("t"))], &subs).len(), 3);
        assert_eq!(prune_feeds(&[], &subs).len(), 3);
    }

    /// Surviving subscriptions come back in subscription order, not in the
    /// order the predicate listed them: the exec's partition *n* must be the
    /// *n*th surviving subscription.
    #[test]
    fn pruning_preserves_subscription_order() {
        let subs = subs(&[
            ("a", "http://x/a"),
            ("b", "http://x/b"),
            ("c", "http://x/c"),
        ]);
        assert_eq!(
            prune_feeds(
                &[col("feed").in_list(vec![lit("c"), lit("a")], false)],
                &subs
            ),
            vec!["a", "c"]
        );
    }

    /// The disjunction shapes *inside* the allowlist, each asserting both
    /// obligations: the exact feeds it prunes to, and the `Exact` claim that
    /// tells DataFusion to stop filtering it above the scan. A shape that
    /// pruned correctly but classified `Unsupported` would only be slow; a
    /// shape that classified `Exact` and pruned to the wrong set would return
    /// wrong rows, so neither assertion stands alone.
    #[tokio::test]
    async fn disjunctions_of_feed_equalities_prune_to_the_union_and_claim_exact() {
        let subs = subs(&[
            ("a", "http://x/a"),
            ("b", "http://x/b"),
            ("c", "http://x/c"),
        ]);
        let provider = items_provider_with_feeds(&["a", "b", "c"]);
        let cases: Vec<(&str, Expr, Vec<&str>)> = vec![
            (
                "two equalities",
                col("feed").eq(lit("a")).or(col("feed").eq(lit("b"))),
                vec!["a", "b"],
            ),
            (
                "three equalities, left-deep as the simplifier builds them",
                col("feed")
                    .eq(lit("a"))
                    .or(col("feed").eq(lit("b")))
                    .or(col("feed").eq(lit("c"))),
                vec!["a", "b", "c"],
            ),
            (
                "three equalities, right-deep",
                col("feed")
                    .eq(lit("a"))
                    .or(col("feed").eq(lit("b")).or(col("feed").eq(lit("c")))),
                vec!["a", "b", "c"],
            ),
            (
                "the union in predicate order, reported in subscription order",
                col("feed").eq(lit("c")).or(col("feed").eq(lit("a"))),
                vec!["a", "c"],
            ),
            (
                "a reversed-operand leaf",
                lit("a").eq(col("feed")).or(col("feed").eq(lit("c"))),
                vec!["a", "c"],
            ),
            (
                "a duplicated leaf, which names one feed once",
                col("feed").eq(lit("a")).or(col("feed").eq(lit("a"))),
                vec!["a"],
            ),
            (
                "a leaf naming no subscription, which adds nothing",
                col("feed").eq(lit("a")).or(col("feed").eq(lit("nope"))),
                vec!["a"],
            ),
            (
                "every leaf naming no subscription, which is unsatisfiable",
                col("feed").eq(lit("nope")).or(col("feed").eq(lit("nor"))),
                vec![],
            ),
            (
                "leaves on feed_url, mapped back to subscription names",
                col("feed_url")
                    .eq(lit("http://x/a"))
                    .or(col("feed_url").eq(lit("http://x/c"))),
                vec!["a", "c"],
            ),
            (
                "an IN list as a leaf, whose members join the union",
                col("feed")
                    .eq(lit("a"))
                    .or(col("feed").in_list(vec![lit("b"), lit("c")], false)),
                vec!["a", "b", "c"],
            ),
        ];
        for (why, filter, expected) in cases {
            assert_eq!(
                prune_feeds(std::slice::from_ref(&filter), &subs),
                expected,
                "{why} must prune to the union of its leaves: {filter}"
            );
            assert_eq!(
                provider
                    .supports_filters_pushdown(&[&filter])
                    .expect("classify"),
                vec![Exact],
                "{why} prunes, so it must be claimed Exact: {filter}"
            );
        }
    }

    /// A disjunction still intersects with the other predicates beside it: the
    /// union is one predicate's contribution, not the whole feed list.
    #[test]
    fn a_disjunction_intersects_with_the_predicates_beside_it() {
        let subs = subs(&[
            ("a", "http://x/a"),
            ("b", "http://x/b"),
            ("c", "http://x/c"),
        ]);
        assert_eq!(
            prune_feeds(
                &[
                    col("feed").eq(lit("a")).or(col("feed").eq(lit("b"))),
                    col("feed").eq(lit("b")).or(col("feed").eq(lit("c"))),
                ],
                &subs
            ),
            vec!["b"]
        );
    }

    /// Every shape outside the allowlist must do the *same two* things: prune
    /// nothing, and be classified `Unsupported`. Failing to classify must never
    /// be read as "matches nothing".
    #[tokio::test]
    async fn shapes_outside_the_allowlist_prune_nothing_and_claim_nothing() {
        let subs = subs(&[
            ("a", "http://x/a"),
            ("b", "http://x/b"),
            ("c", "http://x/c"),
        ]);
        let provider = items_provider_with_feeds(&["a", "b", "c"]);
        let cases: Vec<(&str, Expr)> = vec![
            ("an inequality", col("feed").gt(lit("a"))),
            ("a negated equality", col("feed").not_eq(lit("a"))),
            ("a negated IN", col("feed").in_list(vec![lit("a")], true)),
            ("another column", col("title").eq(lit("t"))),
            ("a non-string literal", col("feed").eq(lit(1_i64))),
            (
                "a NULL literal",
                col("feed").eq(Expr::Literal(ScalarValue::Utf8(None), None)),
            ),
            (
                "a non-literal IN element",
                col("feed").in_list(vec![lit("a"), col("feed_url")], false),
            ),
            (
                "a wrapped column",
                cast(col("feed"), DataType::Utf8View).eq(lit("a")),
            ),
            ("a column on both sides", col("feed").eq(col("feed_url"))),
            ("an IS NULL", col("feed").is_null()),
            (
                "a disjunction with a non-feed leaf",
                col("feed").eq(lit("a")).or(col("title").eq(lit("t"))),
            ),
            (
                "a disjunction with an inequality leaf",
                col("feed").eq(lit("a")).or(col("feed").gt(lit("b"))),
            ),
            (
                "a disjunction with a negated leaf",
                col("feed").eq(lit("a")).or(col("feed").not_eq(lit("b"))),
            ),
            (
                "a disjunction mixing the two feed columns",
                col("feed")
                    .eq(lit("a"))
                    .or(col("feed_url").eq(lit("http://x/c"))),
            ),
            (
                "a conjunction nested inside a disjunction",
                col("feed")
                    .eq(lit("a"))
                    .or(col("feed").eq(lit("b")).and(col("title").eq(lit("t")))),
            ),
            (
                "a disjunction whose non-feed leaf is itself a disjunction",
                col("feed")
                    .eq(lit("a"))
                    .or(col("title").eq(lit("t")).or(col("feed").eq(lit("b")))),
            ),
        ];
        for (why, filter) in cases {
            assert_eq!(
                prune_feeds(std::slice::from_ref(&filter), &subs).len(),
                3,
                "{why} must prune nothing, not prune to empty: {filter}"
            );
            assert_eq!(
                provider
                    .supports_filters_pushdown(&[&filter])
                    .expect("classify"),
                vec![Unsupported],
                "{why} must not be claimed Exact: {filter}"
            );
        }
    }

    #[tokio::test]
    async fn schema_and_table_type_match_the_kind() {
        let items = items_provider_with_feeds(&["a"]);
        let feeds = RssTableProvider::feeds(offline_engine(&["a"]));
        assert_eq!(items.schema(), items_schema());
        assert_eq!(feeds.schema(), feeds_schema());
        assert_eq!(items.table_type(), TableType::Base);
        assert_eq!(feeds.table_type(), TableType::Base);
        assert!(items.statistics().is_none());
    }

    #[tokio::test]
    async fn the_tables_are_read_only() {
        let provider = items_provider_with_feeds(&["a"]);
        let ctx = SessionContext::new();
        let state = ctx.state();
        let input = provider
            .scan(&state, None, &[], None)
            .await
            .expect("build a scan to feed the insert");
        let error = provider
            .insert_into(
                &state,
                input,
                datafusion::logical_expr::dml::InsertOp::Append,
            )
            .await
            .expect_err("an rss table takes no writes");
        let message = error.to_string();
        assert!(
            message.contains("Insert into not implemented for this table"),
            "the refusal names the unimplemented operation: {message}"
        );
    }

    #[tokio::test]
    async fn scan_hands_the_exec_the_pruned_feeds_the_projection_and_the_limit() {
        let provider = items_provider_with_feeds(&["a", "b", "c"]);
        let ctx = SessionContext::new();
        let state = ctx.state();
        let plan = provider
            .scan(&state, Some(&vec![0]), &[col("feed").eq(lit("b"))], Some(5))
            .await
            .expect("scan");
        assert_eq!(
            one_line(&plan),
            "RssScanExec: kind=items feeds=1 limit=Some(5)"
        );
        assert_eq!(plan.properties().partitioning.partition_count(), 1);
        assert_eq!(
            plan.schema().fields().len(),
            1,
            "the projection reached the exec"
        );
    }

    /// A zero-match predicate is still a legal plan: one partition, no rows,
    /// and — because the feed list is empty — no fetch to make.
    #[tokio::test]
    async fn a_zero_match_predicate_yields_one_empty_partition() {
        let provider = items_provider_with_feeds(&["a", "b"]);
        let ctx = SessionContext::new();
        let plan = provider
            .scan(&ctx.state(), None, &[col("feed").eq(lit("nope"))], None)
            .await
            .expect("scan");
        assert_eq!(
            one_line(&plan),
            "RssScanExec: kind=items feeds=0 limit=None"
        );
        assert_eq!(plan.properties().partitioning.partition_count(), 1);
        let batches = collect_stream(plan.execute(0, Arc::new(TaskContext::default()))).await;
        assert!(batches.is_empty(), "no feed to visit means no rows");
    }

    /// `feeds` ignores whatever filters reach it. The optimizer cannot put any
    /// there — they are all `Unsupported` — but a hand-built `TableScan` can,
    /// and ignoring them is only safe because DataFusion still filters above.
    #[tokio::test]
    async fn a_feeds_scan_visits_every_subscription_whatever_the_filters() {
        let provider = RssTableProvider::feeds(offline_engine(&["a", "b", "c"]));
        let ctx = SessionContext::new();
        let plan = provider
            .scan(&ctx.state(), None, &[col("name").eq(lit("a"))], None)
            .await
            .expect("scan");
        assert_eq!(
            one_line(&plan),
            "RssScanExec: kind=feeds feeds=3 limit=None"
        );
        assert_eq!(plan.properties().partitioning.partition_count(), 3);
    }

    #[tokio::test]
    async fn end_to_end_sql_prunes_to_one_fetch() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = Arc::new(test_engine(
            &feed_urls(&server, &[("a", "/a.xml"), ("b", "/b.xml")]),
            |_| {},
        ));
        let ctx = sql_context(&engine);

        let batches = query(&ctx, "SELECT feed, guid FROM items WHERE feed = 'a'").await;

        assert_eq!(total_rows(&batches), 1);
        assert_eq!(column(&batches, "feed"), vec!["a"]);
        let paths: Vec<String> = server.requests().iter().map(|r| r.path.clone()).collect();
        assert_eq!(
            paths,
            vec!["/a.xml".to_string()],
            "the pruned-away subscription must never be fetched"
        );
    }

    /// The same query shape through `feed_url`, which has to be mapped back to
    /// a subscription name before the scan can use it.
    #[tokio::test]
    async fn end_to_end_sql_prunes_on_feed_url() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let feeds = feed_urls(&server, &[("a", "/a.xml"), ("b", "/b.xml")]);
        let engine = Arc::new(test_engine(&feeds, |_| {}));
        let ctx = sql_context(&engine);
        let url = feeds[1].1.clone();

        let batches = query(
            &ctx,
            &format!("SELECT feed FROM items WHERE feed_url = '{url}'"),
        )
        .await;

        assert_eq!(column(&batches, "feed"), vec!["b"]);
        let paths: Vec<String> = server.requests().iter().map(|r| r.path.clone()).collect();
        assert_eq!(paths, vec!["/b.xml".to_string()]);
    }

    /// A predicate naming no subscription reaches the wire zero times.
    #[tokio::test]
    async fn end_to_end_sql_with_an_unknown_feed_fetches_nothing() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = Arc::new(test_engine(
            &feed_urls(&server, &[("a", "/a.xml"), ("b", "/b.xml")]),
            |_| {},
        ));
        let ctx = sql_context(&engine);

        let batches = query(&ctx, "SELECT guid FROM items WHERE feed = 'nope'").await;

        assert_eq!(total_rows(&batches), 0);
        assert!(
            server.requests().is_empty(),
            "an unsatisfiable feed predicate must issue no requests"
        );
    }

    /// The divergence guard: predicates this provider does **not** prune on are
    /// classified `Unsupported`, so DataFusion keeps filtering them above the
    /// scan. If `supports_filters_pushdown` ever claimed `Exact` for a shape
    /// `prune_feeds` ignores, DataFusion would drop that `Filter` and both
    /// assertions below would see unfiltered rows.
    #[tokio::test]
    async fn non_prunable_filters_are_applied_above_the_scan() {
        let server = MockFeedServer::start(|_| MockResponse::xml(RSS2_MINIMAL)).await;
        let engine = Arc::new(test_engine(
            &feed_urls(&server, &[("a", "/a.xml"), ("b", "/b.xml")]),
            |_| {},
        ));
        let ctx = sql_context(&engine);

        // A column that cannot prune at all.
        let none = query(&ctx, "SELECT guid FROM items WHERE title = 'nope'").await;
        assert_eq!(
            total_rows(&none),
            0,
            "a filter on a non-prunable column must still remove rows"
        );

        // A feed predicate whose *operator* is outside the allowlist: the
        // column is prunable, the shape is not.
        let after_a = query(&ctx, "SELECT feed FROM items WHERE feed > 'a'").await;
        assert_eq!(
            column(&after_a, "feed"),
            vec!["b"],
            "an unsupported operator on `feed` must be filtered, not ignored"
        );

        assert_eq!(
            server.requests().len(),
            2,
            "neither query prunes, so both subscriptions are visited"
        );
    }

    /// `feeds` stays total, which is what makes the prescribed absence check
    /// work: the subscription that served nothing is exactly the one it finds.
    #[tokio::test]
    async fn a_feeds_scan_is_total_over_subscriptions() {
        let server = MockFeedServer::start(|req| match req.path.as_str() {
            "/a.xml" => MockResponse::xml(RSS2_MINIMAL),
            _ => MockResponse::status(500),
        })
        .await;
        let engine = Arc::new(test_engine(
            &feed_urls(&server, &[("a", "/a.xml"), ("b", "/b.xml")]),
            |_| {},
        ));
        let ctx = sql_context(&engine);

        let all = query(&ctx, "SELECT name FROM feeds ORDER BY name").await;
        assert_eq!(column(&all, "name"), vec!["a", "b"]);

        let missing = query(
            &ctx,
            "SELECT f.name FROM feeds f LEFT JOIN items i ON i.feed = f.name \
             WHERE i.feed IS NULL ORDER BY f.name",
        )
        .await;
        assert_eq!(
            column(&missing, "name"),
            vec!["b"],
            "the dead subscription is the one with no items"
        );
    }
}
