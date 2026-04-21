//! Table function for SQLite KNN vector search via sqlite-vec.
//!
//! Usage:
//! ```sql
//! -- Basic KNN search on a vec0 virtual table
//! SELECT * FROM sqlite_knn('vec_items', 'embedding', [0.1, 0.2, 0.3], 10)
//!
//! -- With WHERE clause filter pushdown
//! SELECT * FROM sqlite_knn('vec_items', 'embedding', [0.1, 0.2, ...], 10)
//! WHERE category = 'electronics'
//! ```
//!
//! The first argument must be a registered SQLite vec0 virtual table (from the
//! sqlite-vec extension). The distance metric is determined at table creation time
//! (e.g. `distance_metric=L2` or `distance_metric=cosine`).
//!
//! Vectors must be stored in vec0 virtual tables. The query vector is passed as
//! packed little-endian f32 bytes via the MATCH operator.
//!
//! Returns all non-vector columns from the table plus `_score Float64`.
//! The score is the raw distance value — lower means more similar.

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableFunctionImpl, TableProvider};
use datafusion::common::{Result as DFResult, plan_err};
use datafusion::datasource::TableType;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use std::any::Any;
use std::sync::Arc;
use tokio_rusqlite::Connection;

use super::knn_exec::SqliteKnnExec;
use super::{expr_to_sqlite_sql, extract_string};
use crate::sources::providers::knn_utils::{extract_k, extract_literal_vector};
use crate::sources::providers::{DatasetEntry, DatasetRegistry};

/// Entry stored in the registry for each registered SQLite table.
#[derive(Clone, Debug)]
pub struct SqliteEntry {
    /// Connection to the SQLite database.
    pub conn: Arc<Connection>,
    /// Table name in the database.
    pub table_name: String,
    /// All columns and their Arrow types (from PRAGMA table_info).
    pub columns: Vec<(String, DataType)>,
}

// ─── TableFunctionImpl ───────────────────────────────────────────────────────

/// Table function that performs KNN vector search on SQLite vec0 virtual tables.
#[derive(Debug)]
pub struct SqliteKnnTableFunction {
    registry: DatasetRegistry,
}

impl SqliteKnnTableFunction {
    pub fn new(registry: DatasetRegistry) -> Self {
        Self { registry }
    }
}

impl TableFunctionImpl for SqliteKnnTableFunction {
    fn call(&self, exprs: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if exprs.len() != 4 {
            return plan_err!(
                "sqlite_knn(table, vector_col, query_vec, k) expects 4 arguments, got {}. \
                 The distance metric is configured at vec0 table creation time.",
                exprs.len()
            );
        }

        let table_name = extract_string(&exprs[0], "table")?;
        let vector_col = extract_string(&exprs[1], "vector_col")?;
        let k = extract_k(&exprs[3], "sqlite_knn")?;

        // Try to extract a literal vector.
        let literal_vector = extract_literal_vector(&exprs[2], "sqlite_knn").ok();

        // Look up connection + columns from registry.
        let entry = {
            let reg = self.registry.read().map_err(|e| {
                DataFusionError::Internal(format!("sqlite_knn registry lock error: {}", e))
            })?;
            let raw = reg.get(&table_name).cloned().ok_or_else(|| {
                DataFusionError::Plan(format!(
                    "sqlite_knn: table '{}' not found in registry. \
                     Make sure the data source is declared with type 'sqlite' \
                     and the sqlite-vec extension is loaded via the 'extensions' option.",
                    table_name
                ))
            })?;
            match raw {
                DatasetEntry::Sqlite(e) => e,
                _ => {
                    return plan_err!("sqlite_knn: table '{}' is not a SQLite dataset", table_name);
                }
            }
        };

        // Build output schema: all non-vector columns + _score.
        // vec0 tables have vector columns (type BLOB/BINARY) which we exclude.
        let mut fields: Vec<Field> = entry
            .columns
            .iter()
            .filter(|(name, _)| name != &vector_col)
            .map(|(name, dtype)| Field::new(name.clone(), dtype.clone(), true))
            .collect();
        fields.push(Field::new("_score", DataType::Float64, true));
        let schema: SchemaRef = Arc::new(Schema::new(fields));

        Ok(Arc::new(SqliteKnnProvider {
            conn: entry.conn,
            table_name: entry.table_name,
            vector_col,
            literal_vector,
            query_vector_expr: exprs[2].clone(),
            schema,
            k,
        }))
    }
}

// ─── TableProvider ───────────────────────────────────────────────────────────

struct SqliteKnnProvider {
    conn: Arc<Connection>,
    table_name: String,
    vector_col: String,
    /// Pre-computed query vector for the literal path. `None` for subquery path.
    literal_vector: Option<Vec<f32>>,
    /// Original expression for the query vector argument (used for subquery path).
    query_vector_expr: Expr,
    schema: SchemaRef,
    k: usize,
}

impl std::fmt::Debug for SqliteKnnProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteKnnProvider")
            .field("table_name", &self.table_name)
            .field("vector_col", &self.vector_col)
            .field("k", &self.k)
            .finish()
    }
}

#[async_trait]
impl TableProvider for SqliteKnnProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|expr| {
                if expr_to_sqlite_sql(expr).is_some() {
                    TableProviderFilterPushDown::Exact
                } else {
                    TableProviderFilterPushDown::Unsupported
                }
            })
            .collect())
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        // Combine WHERE-clause filters.
        let mut parts: Vec<String> = Vec::new();
        for expr in filters {
            if let Some(sql) = expr_to_sqlite_sql(expr) {
                parts.push(sql);
            }
        }
        let filter = if parts.is_empty() {
            None
        } else {
            Some(parts.join(" AND "))
        };

        // Build schema respecting column projection.
        let schema = if let Some(proj) = projection {
            let fields: Vec<Field> = proj.iter().map(|&i| self.schema.field(i).clone()).collect();
            Arc::new(Schema::new(fields))
        } else {
            self.schema.clone()
        };

        let mut exec: SqliteKnnExec = if let Some(ref vec) = self.literal_vector {
            // Literal path — vector known at planning time.
            SqliteKnnExec::new(
                Arc::clone(&self.conn),
                self.table_name.clone(),
                self.vector_col.clone(),
                vec.clone(),
                filter,
                schema,
                self.k,
            )
        } else if let Expr::ScalarSubquery(subquery) = &self.query_vector_expr {
            // Subquery path — create physical plan for deferred evaluation.
            let physical_plan = state
                .create_physical_plan(subquery.subquery.as_ref())
                .await?;
            SqliteKnnExec::new_with_subquery(
                Arc::clone(&self.conn),
                self.table_name.clone(),
                self.vector_col.clone(),
                physical_plan,
                filter,
                schema,
                self.k,
            )
        } else {
            return plan_err!(
                "sqlite_knn: query_vec must be a literal array or a scalar subquery, \
                 e.g. [0.1, 0.2, ...] or (SELECT embedding FROM t WHERE id = {{id}})"
            );
        };

        if let Some(n) = limit {
            exec = exec.with_scan_limit(n);
        }

        Ok(Arc::new(exec))
    }
}

// ─── Registration ────────────────────────────────────────────────────────────

/// Register the `sqlite_knn` table function with the DataFusion session.
pub fn register_sqlite_knn_udtf(ctx: &SessionContext, registry: DatasetRegistry) {
    ctx.register_udtf(
        "sqlite_knn",
        Arc::new(SqliteKnnTableFunction::new(registry)),
    );
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::ScalarValue;
    use std::collections::HashMap;
    use std::sync::RwLock;

    fn make_knn_function() -> SqliteKnnTableFunction {
        let registry: DatasetRegistry = Arc::new(RwLock::new(HashMap::new()));
        SqliteKnnTableFunction::new(registry)
    }

    fn lit_str(s: &str) -> Expr {
        Expr::Literal(ScalarValue::Utf8(Some(s.to_string())), None)
    }

    fn lit_int(n: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(Some(n)), None)
    }

    fn lit_null() -> Expr {
        Expr::Literal(ScalarValue::Null, None)
    }

    fn lit_vec(values: &[f64]) -> Expr {
        use arrow::array::{ArrayRef, Float64Array, ListArray};
        use arrow::buffer::OffsetBuffer;
        let arr = Float64Array::from(values.to_vec());
        let list = ListArray::new(
            Arc::new(Field::new_list_field(DataType::Float64, true)),
            OffsetBuffer::from_lengths([values.len()]),
            Arc::new(arr) as ArrayRef,
            None,
        );
        Expr::Literal(ScalarValue::List(Arc::new(list)), None)
    }

    #[test]
    fn test_wrong_arg_count_rejected() {
        let func = make_knn_function();
        let result = func.call(&[lit_str("table"), lit_str("col"), lit_vec(&[0.1, 0.2])]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("expects 4 arguments"),
            "expected arg count error, got: {err}"
        );
    }

    #[test]
    fn test_k_over_max_rejected() {
        let func = make_knn_function();
        let result = func.call(&[
            lit_str("some_table"),
            lit_str("col"),
            lit_vec(&[0.1, 0.2]),
            lit_int(501),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("must be between 1 and 500"),
            "expected k limit error, got: {err}"
        );
    }

    #[test]
    fn test_k_zero_rejected() {
        let func = make_knn_function();
        let result = func.call(&[
            lit_str("some_table"),
            lit_str("col"),
            lit_vec(&[0.1, 0.2]),
            lit_int(0),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("positive integer"),
            "expected positive-integer error, got: {err}"
        );
    }

    #[test]
    fn test_k_negative_rejected() {
        let func = make_knn_function();
        let result = func.call(&[
            lit_str("some_table"),
            lit_str("col"),
            lit_vec(&[0.1, 0.2]),
            lit_int(-1),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("positive integer") && err.contains("-1"),
            "expected positive-integer error with original value, got: {err}"
        );
    }

    #[test]
    fn test_null_placeholders_accepted() {
        let func = make_knn_function();
        let result = func.call(&[
            lit_str("some_table"),
            lit_str("col"),
            lit_null(),
            lit_null(),
        ]);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("not found in registry"),
            "expected registry error, got: {err}"
        );
    }

    // ─── sqlite_knn integration tests ────────────────────────────────────
    // Require the sqlite-vec extension. Set SQLITE_VEC_PATH to the path of
    // the vec0 shared library (e.g. /usr/local/lib/vec0 or vec0.dylib).

    use arrow::array::{Float64Array as F64Array, Int64Array, RecordBatch};

    use super::super::register_sqlite_tables;
    use crate::sources::hierarchy::HierarchyLevel;

    fn vec_ext_path() -> Option<String> {
        std::env::var("SQLITE_VEC_PATH").ok()
    }

    async fn create_knn_test_db(ext_path: &str) -> tempfile::TempPath {
        let tmp = tempfile::NamedTempFile::new().expect("create temp file");
        let path = tmp.into_temp_path();
        let db_path = path.to_str().unwrap().to_string();
        let ext = ext_path.to_string();

        let conn = Connection::open(&db_path).await.expect("open temp sqlite");
        conn.call(move |conn| -> Result<(), tokio_rusqlite::rusqlite::Error> {
            // Load sqlite-vec extension.
            unsafe { conn.load_extension_enable()? };
            unsafe { conn.load_extension(&ext, None::<&str>)? };
            conn.load_extension_disable()?;

            // Create a vec0 virtual table with 3-dimensional float vectors.
            conn.execute_batch(
                "CREATE VIRTUAL TABLE vec_items USING vec0(
                     item_id INTEGER PRIMARY KEY,
                     embedding float[3]
                 );",
            )?;

            // Insert sample vectors as packed f32 bytes.
            let vectors: Vec<(i64, Vec<f32>)> = vec![
                (1, vec![1.0, 0.0, 0.0]),
                (2, vec![0.0, 1.0, 0.0]),
                (3, vec![0.0, 0.0, 1.0]),
                (4, vec![1.0, 1.0, 0.0]),
                (5, vec![0.5, 0.5, 0.5]),
            ];

            let mut stmt =
                conn.prepare("INSERT INTO vec_items (item_id, embedding) VALUES (?1, ?2)")?;

            for (id, vec) in &vectors {
                let blob: Vec<u8> = vec.iter().flat_map(|f| f.to_le_bytes()).collect();
                stmt.execute(tokio_rusqlite::rusqlite::params![id, blob])?;
            }

            Ok(())
        })
        .await
        .expect("seed knn table");
        conn.close().await.expect("close seed connection");

        path
    }

    async fn register_ci_knn(
        ctx: &mut SessionContext,
        ext_path: &str,
    ) -> (DatasetRegistry, tempfile::TempPath) {
        let registry: DatasetRegistry = Arc::new(RwLock::new(HashMap::new()));
        let db_path = create_knn_test_db(ext_path).await;
        let db = db_path.to_str().unwrap();

        let mut options = HashMap::new();
        options.insert("table".to_string(), "vec_items".to_string());
        options.insert("extensions".to_string(), ext_path.to_string());

        register_sqlite_tables(
            ctx,
            "vec_items",
            db,
            Some(&options),
            false,
            Some(&registry),
            HierarchyLevel::Table,
        )
        .await
        .expect("register vec_items table failed");

        register_sqlite_knn_udtf(ctx, Arc::clone(&registry));
        (registry, db_path)
    }

    async fn query_all(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
        let df = ctx.sql(sql).await.expect("parse sql");
        df.collect().await.expect("collect results")
    }

    fn total_rows(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    #[tokio::test]
    #[ignore]
    async fn test_knn_basic_search() {
        let ext_path = match vec_ext_path() {
            Some(p) => p,
            None => {
                eprintln!("skipping: SQLITE_VEC_PATH not set");
                return;
            }
        };
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_knn(&mut ctx, &ext_path).await;

        let batches = query_all(
            &ctx,
            "SELECT item_id, _score FROM sqlite_knn('vec_items', 'embedding', [1.0, 0.0, 0.0], 3)",
        )
        .await;

        let rows = total_rows(&batches);
        assert_eq!(rows, 3, "expected 3 results, got {rows}");

        // First result should be item 1 (exact match, distance 0)
        let ids = batches[0]
            .column_by_name("item_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1, "closest vector should be item 1");

        let scores = batches[0]
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<F64Array>()
            .unwrap();
        assert!(
            (scores.value(0) - 0.0).abs() < 1e-6,
            "exact match should have distance ~0"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_knn_distance_ordering() {
        let ext_path = match vec_ext_path() {
            Some(p) => p,
            None => {
                eprintln!("skipping: SQLITE_VEC_PATH not set");
                return;
            }
        };
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_knn(&mut ctx, &ext_path).await;

        let batches = query_all(
            &ctx,
            "SELECT _score FROM sqlite_knn('vec_items', 'embedding', [1.0, 0.0, 0.0], 5)",
        )
        .await;

        let scores = batches[0]
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<F64Array>()
            .unwrap();

        for i in 1..scores.len() {
            assert!(
                scores.value(i - 1) <= scores.value(i),
                "distances should be ascending: {} <= {}",
                scores.value(i - 1),
                scores.value(i)
            );
        }
    }

    #[tokio::test]
    #[ignore]
    async fn test_knn_excludes_vector_column() {
        let ext_path = match vec_ext_path() {
            Some(p) => p,
            None => {
                eprintln!("skipping: SQLITE_VEC_PATH not set");
                return;
            }
        };
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_knn(&mut ctx, &ext_path).await;

        let batches = query_all(
            &ctx,
            "SELECT * FROM sqlite_knn('vec_items', 'embedding', [1.0, 0.0, 0.0], 1)",
        )
        .await;

        let schema = batches[0].schema();
        let field_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert!(
            !field_names.contains(&"embedding"),
            "vector column should be excluded from output; fields={field_names:?}"
        );
        assert!(
            field_names.contains(&"_score"),
            "output should include _score; fields={field_names:?}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_knn_respects_k() {
        let ext_path = match vec_ext_path() {
            Some(p) => p,
            None => {
                eprintln!("skipping: SQLITE_VEC_PATH not set");
                return;
            }
        };
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_knn(&mut ctx, &ext_path).await;

        let batches = query_all(
            &ctx,
            "SELECT item_id FROM sqlite_knn('vec_items', 'embedding', [1.0, 0.0, 0.0], 2)",
        )
        .await;

        assert_eq!(total_rows(&batches), 2);
    }

    // ─── Read-your-own-write ─────────────────────────────────────────

    /// Register vec0 table as read_write so INSERT and sqlite_knn share the
    /// same connection (write conn), ensuring immediate visibility.
    async fn register_ci_knn_rw(
        ctx: &mut SessionContext,
        ext_path: &str,
    ) -> (DatasetRegistry, tempfile::TempPath) {
        let registry: DatasetRegistry = Arc::new(RwLock::new(HashMap::new()));
        let db_path = create_knn_test_db(ext_path).await;
        let db = db_path.to_str().unwrap();

        let mut options = HashMap::new();
        options.insert("table".to_string(), "vec_items".to_string());
        options.insert("extensions".to_string(), ext_path.to_string());

        register_sqlite_tables(
            ctx,
            "vec_items",
            db,
            Some(&options),
            true, // read_write
            Some(&registry),
            HierarchyLevel::Table,
        )
        .await
        .expect("register vec_items table (rw) failed");

        register_sqlite_knn_udtf(ctx, Arc::clone(&registry));
        (registry, db_path)
    }

    #[tokio::test]
    #[ignore]
    async fn test_knn_read_own_write_insert_then_search() {
        let ext_path = match vec_ext_path() {
            Some(p) => p,
            None => {
                eprintln!("skipping: SQLITE_VEC_PATH not set");
                return;
            }
        };
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_knn_rw(&mut ctx, &ext_path).await;

        // Verify 5 initial items
        let batches = query_all(
            &ctx,
            "SELECT item_id FROM sqlite_knn('vec_items', 'embedding', [0.0, 0.0, 1.0], 10)",
        )
        .await;
        assert_eq!(total_rows(&batches), 5);

        // INSERT a new vector: [0.0, 0.0, 0.9] — very close to [0.0, 0.0, 1.0]
        ctx.sql(
            "INSERT INTO vec_items (item_id, embedding) \
             VALUES (6, X'000000000000000066663F3F')",
            // 0.0f32, 0.0f32, 0.75f32 in little-endian
        )
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

        // Search must now find 6 items
        let batches = query_all(
            &ctx,
            "SELECT item_id FROM sqlite_knn('vec_items', 'embedding', [0.0, 0.0, 1.0], 10)",
        )
        .await;
        assert_eq!(
            total_rows(&batches),
            6,
            "inserted vector must be immediately searchable via KNN"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_knn_read_own_write_delete_then_search() {
        let ext_path = match vec_ext_path() {
            Some(p) => p,
            None => {
                eprintln!("skipping: SQLITE_VEC_PATH not set");
                return;
            }
        };
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_knn_rw(&mut ctx, &ext_path).await;

        // Verify 5 initial items
        let batches = query_all(
            &ctx,
            "SELECT item_id FROM sqlite_knn('vec_items', 'embedding', [1.0, 0.0, 0.0], 10)",
        )
        .await;
        assert_eq!(total_rows(&batches), 5);

        // DELETE item_id = 1
        ctx.sql("DELETE FROM vec_items WHERE item_id = 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Search must now find 4 items — deleted vector is gone
        let batches = query_all(
            &ctx,
            "SELECT item_id FROM sqlite_knn('vec_items', 'embedding', [1.0, 0.0, 0.0], 10)",
        )
        .await;
        assert_eq!(
            total_rows(&batches),
            4,
            "deleted vector must not appear in KNN results"
        );
    }

    // ─── Scalar subquery tests ───────────────────────────────────────

    #[tokio::test]
    #[ignore]
    async fn test_knn_subquery_find_similar() {
        let ext_path = match vec_ext_path() {
            Some(p) => p,
            None => {
                eprintln!("skipping: SQLITE_VEC_PATH not set");
                return;
            }
        };
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_knn(&mut ctx, &ext_path).await;

        // Use item 1's embedding as the query vector via scalar subquery.
        // Item 1 has [1,0,0] so item 4 [1,1,0] should be the second closest.
        let batches = query_all(
            &ctx,
            "SELECT item_id, _score \
             FROM sqlite_knn('vec_items', 'embedding', \
                 (SELECT embedding FROM vec_items WHERE item_id = 1), 3)",
        )
        .await;

        let rows = total_rows(&batches);
        assert_eq!(rows, 3, "subquery KNN should return 3 results, got {rows}");

        // First result should be item 1 itself (distance 0)
        let ids = batches[0]
            .column_by_name("item_id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 1, "closest should be the seed item itself");

        let scores = batches[0]
            .column_by_name("_score")
            .unwrap()
            .as_any()
            .downcast_ref::<F64Array>()
            .unwrap();
        assert!(
            scores.value(0).abs() < 1e-6,
            "seed item distance to itself should be ~0, got {}",
            scores.value(0)
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_knn_subquery_no_match_returns_empty() {
        let ext_path = match vec_ext_path() {
            Some(p) => p,
            None => {
                eprintln!("skipping: SQLITE_VEC_PATH not set");
                return;
            }
        };
        let mut ctx = SessionContext::new();
        let (_reg, _db) = register_ci_knn(&mut ctx, &ext_path).await;

        // Subquery for a non-existent item should return empty result.
        let batches = query_all(
            &ctx,
            "SELECT item_id FROM sqlite_knn('vec_items', 'embedding', \
                 (SELECT embedding FROM vec_items WHERE item_id = 999), 3)",
        )
        .await;

        assert_eq!(
            total_rows(&batches),
            0,
            "subquery for non-existent item should return empty"
        );
    }
}
