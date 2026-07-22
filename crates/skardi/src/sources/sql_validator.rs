// Parse with DataFusion's own re-exported sqlparser so the validator and the
// engine can never disagree about a statement's shape: they are, by
// construction, the same parser at the same version. A DataFusion bump that
// moves sqlparser surfaces here as a compile error, not a silent
// parse-divergence.
use datafusion::sql::sqlparser::ast::{
    FromTable, ObjectName, ObjectNamePart, Statement, TableFactor, TableObject, TableWithJoins,
    visit_relations,
};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;
use thiserror::Error;

// Re-export AccessMode for convenience
pub use crate::sources::access_mode::AccessMode;

/// Per-table access modes. Shared by both validation entry points; carries
/// no trust-boundary state of its own, so it cannot silently grant or deny
/// depending on which validator receives it.
#[derive(Debug, Clone, Default)]
pub struct SqlValidatorConfig {
    pub table_access_modes: HashMap<String, AccessMode>,
}

impl SqlValidatorConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_table(mut self, table_name: &str, mode: AccessMode) -> Self {
        self.table_access_modes
            .insert(table_name.to_lowercase(), mode);
        self
    }
}

/// Policy for **untrusted, ad-hoc SQL** (the `/query` endpoint).
///
/// Distinct from [`SqlValidatorConfig`] on purpose: the untrusted entry point
/// [`validate_single_sql`] requires *this* type, so the strict allowlist and
/// the reserved-schema denial cannot be bypassed by reaching for the
/// trusted-path [`validate_sql`] with a bare [`SqlValidatorConfig`]. The trust
/// boundary lives in the type, not in which function a future caller happens
/// to pick.
#[derive(Debug, Clone, Default)]
pub struct AdhocSqlPolicy {
    /// Per-table access modes (same map the trusted path uses).
    pub access: SqlValidatorConfig,
    /// Schemas an ad-hoc caller may never reference in any form.
    pub denied_schemas: HashSet<String>,
}

impl AdhocSqlPolicy {
    pub fn new(access: SqlValidatorConfig) -> Self {
        Self {
            access,
            denied_schemas: HashSet::new(),
        }
    }

    pub fn with_denied_schema(mut self, schema: &str) -> Self {
        self.denied_schemas.insert(schema.to_lowercase());
        self
    }
}

#[derive(Error, Debug)]
pub enum SqlValidationError {
    #[error("SQL parse error: {0}")]
    ParseError(String),

    #[error(
        "DDL operation not allowed: {operation}. DDL operations (CREATE, DROP, ALTER, TRUNCATE) are not permitted on any data source."
    )]
    DdlNotAllowed { operation: String },

    #[error(
        "Write operation '{operation}' not allowed on table '{table}'. The table is configured with 'read_only' access mode."
    )]
    WriteNotAllowed { operation: String, table: String },

    #[error("Expected exactly one SQL statement, found {count}.")]
    NotExactlyOneStatement { count: usize },

    #[error(
        "Statement type '{operation}' not allowed. Ad-hoc SQL is limited to queries, DML on read_write sources, EXPLAIN, SHOW, and DESCRIBE."
    )]
    StatementNotAllowed { operation: String },

    #[error("Access to table '{table}' is not allowed: schema '{schema}' is reserved.")]
    SchemaNotAllowed { schema: String, table: String },
}

pub fn validate_sql(sql: &str, config: &SqlValidatorConfig) -> Result<(), SqlValidationError> {
    // Replace {param_name} placeholders with valid SQL strings before parsing
    // This allows validation of parameterized queries
    let preprocessed_sql = preprocess_parameters(sql);

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, &preprocessed_sql)
        .map_err(|e| SqlValidationError::ParseError(e.to_string()))?;

    for statement in statements {
        validate_statement(&statement, config)?;
    }

    Ok(())
}

/// Shape of a statement validated by [`validate_single_sql`], so callers can
/// pick an execution path without depending on sqlparser types (crates
/// outside this one may link a different sqlparser version).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementKind {
    /// A query (SELECT/...) — safe to wrap in a plan-level LIMIT.
    Query,
    /// Anything else that passed validation (DML writes, SHOW, EXPLAIN, ...).
    Other,
}

/// Validate a single ad-hoc SQL statement from an untrusted caller.
///
/// Stricter than [`validate_sql`]: the input must parse to exactly one
/// statement, only allowlisted statement types are accepted (queries,
/// access-checked DML, EXPLAIN of an allowed statement, SHOW/DESCRIBE),
/// and references into [`AdhocSqlPolicy::denied_schemas`] are rejected.
/// Returns the statement's [`StatementKind`] on success.
pub fn validate_single_sql(
    sql: &str,
    policy: &AdhocSqlPolicy,
) -> Result<StatementKind, SqlValidationError> {
    // Ad-hoc SQL has no `{param}` templates, so it is parsed as-is:
    // brace substitution is not quote-aware and would rewrite string
    // literals, rejecting valid queries.
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)
        .map_err(|e| SqlValidationError::ParseError(e.to_string()))?;

    if statements.len() != 1 {
        return Err(SqlValidationError::NotExactlyOneStatement {
            count: statements.len(),
        });
    }

    let statement = &statements[0];
    validate_statement_strict(statement, &policy.access)?;
    check_denied_schemas(statement, &policy.denied_schemas)?;

    Ok(if matches!(statement, Statement::Query(_)) {
        StatementKind::Query
    } else {
        StatementKind::Other
    })
}

/// Strict allowlist applied to ad-hoc SQL from untrusted callers: only
/// queries, access-checked DML, EXPLAIN of an allowed statement, and
/// SHOW/DESCRIBE are permitted. Everything else — including statement
/// types added by future sqlparser upgrades — is rejected, so a parser
/// bump can never silently reopen an executable statement type.
fn validate_statement_strict(
    statement: &Statement,
    config: &SqlValidatorConfig,
) -> Result<(), SqlValidationError> {
    // Reuse the shared checks first so DDL and read-only-source writes keep
    // their specific error variants.
    validate_statement(statement, config)?;

    match statement {
        Statement::Query(_)
        | Statement::Insert(_)
        | Statement::Update { .. }
        | Statement::Delete(_) => Ok(()),

        // EXPLAIN wraps an inner statement that DataFusion may execute
        // (EXPLAIN ANALYZE runs the plan), so the inner statement must
        // itself pass the allowlist.
        Statement::Explain { statement, .. } => validate_statement_strict(statement, config),

        // Metadata reads.
        Statement::ExplainTable { .. }
        | Statement::ShowTables { .. }
        | Statement::ShowColumns { .. }
        | Statement::ShowVariable { .. }
        | Statement::ShowVariables { .. }
        | Statement::ShowDatabases { .. }
        | Statement::ShowSchemas { .. }
        | Statement::ShowFunctions { .. } => Ok(()),

        other => Err(SqlValidationError::StatementNotAllowed {
            operation: statement_keyword(other).to_string(),
        }),
    }
}

/// Keyword naming a rejected statement, for error messages. Maps the enum
/// variant directly rather than `Display`-rendering the whole AST just to
/// split off the first token.
fn statement_keyword(statement: &Statement) -> &'static str {
    match statement {
        Statement::Merge { .. } => "MERGE",
        Statement::StartTransaction { .. } => "START TRANSACTION",
        Statement::Commit { .. } => "COMMIT",
        Statement::Rollback { .. } => "ROLLBACK",
        Statement::Savepoint { .. } => "SAVEPOINT",
        Statement::Grant { .. } => "GRANT",
        Statement::Deny(_) => "DENY",
        Statement::Set(_) => "SET",
        Statement::Deallocate { .. } => "DEALLOCATE",
        Statement::Prepare { .. } => "PREPARE",
        Statement::Execute { .. } => "EXECUTE",
        Statement::Copy { .. } | Statement::CopyIntoSnowflake { .. } => "COPY",
        Statement::Use(_) => "USE",
        Statement::Pragma { .. } => "PRAGMA",
        Statement::Call(_) => "CALL",
        Statement::Unload { .. } => "UNLOAD",
        Statement::Cache { .. } => "CACHE",
        Statement::UNCache { .. } => "UNCACHE",
        _ => "STATEMENT",
    }
}

/// Reject any table reference whose schema qualifier names a denied schema
/// (e.g. `auth.sessions`, `datafusion.auth.sessions`). Walks every relation
/// in the statement — FROM, JOINs, subqueries, CTEs, DML targets, EXPLAIN
/// bodies, DESCRIBE targets, and table-function arguments (`visit_relations`
/// descends into nested queries).
///
/// This is a syntactic guard over relation names. It cannot see through an
/// indirect handle that does not surface as an `auth`-qualified relation —
/// e.g. an operator-defined view or federated alias over `auth.sessions`.
/// Ad-hoc SQL cannot create such a handle (DDL is rejected by the allowlist),
/// so the residual surface is operator config, which is trusted; a
/// `/query`-scoped context that never registers the auth schema would close
/// even that (tracked as a follow-up).
fn check_denied_schemas(
    statement: &Statement,
    denied_schemas: &HashSet<String>,
) -> Result<(), SqlValidationError> {
    if denied_schemas.is_empty() {
        return Ok(());
    }

    let flow = visit_relations(statement, |relation: &ObjectName| {
        let parts = &relation.0;
        if parts.len() > 1 {
            // Every component except the last is a qualifier (catalog or
            // schema); a bare table that happens to share the name is fine.
            for qualifier in &parts[..parts.len() - 1] {
                let qualifier = object_name_part_value(qualifier);
                if denied_schemas.contains(&qualifier) {
                    return ControlFlow::Break(SqlValidationError::SchemaNotAllowed {
                        schema: qualifier,
                        table: extract_table_name(relation),
                    });
                }
            }
        }
        ControlFlow::Continue(())
    });

    match flow {
        ControlFlow::Break(err) => Err(err),
        ControlFlow::Continue(()) => Ok(()),
    }
}

fn preprocess_parameters(sql: &str) -> String {
    // `(NULL)` parses both as a scalar expression (e.g. `WHERE x = (NULL)`)
    // and as a single-row VALUES tuple (e.g. `INSERT … VALUES (NULL)`),
    // so the same substitution covers both `{scalar}` and `VALUES {rows}`
    // pipeline shapes. The runtime renderer is responsible for emitting
    // shape-correct SQL; this stand-in only needs to be parseable.
    const REPLACEMENT: &str = "(NULL)";

    let mut result = sql.to_string();
    let mut start = 0;

    while let Some(open) = result[start..].find('{') {
        let open = start + open;
        if let Some(close) = result[open..].find('}') {
            let close = open + close;
            result = format!("{}{}{}", &result[..open], REPLACEMENT, &result[close + 1..]);
            start = open + REPLACEMENT.len();
        } else {
            break;
        }
    }

    result
}

fn validate_statement(
    statement: &Statement,
    config: &SqlValidatorConfig,
) -> Result<(), SqlValidationError> {
    match statement {
        // DDL operations - always blocked
        Statement::CreateTable { .. } => Err(SqlValidationError::DdlNotAllowed {
            operation: "CREATE TABLE".to_string(),
        }),
        Statement::CreateIndex { .. } => Err(SqlValidationError::DdlNotAllowed {
            operation: "CREATE INDEX".to_string(),
        }),
        Statement::CreateView { .. } => Err(SqlValidationError::DdlNotAllowed {
            operation: "CREATE VIEW".to_string(),
        }),
        Statement::CreateSchema { .. } => Err(SqlValidationError::DdlNotAllowed {
            operation: "CREATE SCHEMA".to_string(),
        }),
        Statement::CreateDatabase { .. } => Err(SqlValidationError::DdlNotAllowed {
            operation: "CREATE DATABASE".to_string(),
        }),
        Statement::CreateFunction { .. } => Err(SqlValidationError::DdlNotAllowed {
            operation: "CREATE FUNCTION".to_string(),
        }),
        Statement::CreateProcedure { .. } => Err(SqlValidationError::DdlNotAllowed {
            operation: "CREATE PROCEDURE".to_string(),
        }),
        Statement::CreateSequence { .. } => Err(SqlValidationError::DdlNotAllowed {
            operation: "CREATE SEQUENCE".to_string(),
        }),
        Statement::CreateType { .. } => Err(SqlValidationError::DdlNotAllowed {
            operation: "CREATE TYPE".to_string(),
        }),
        Statement::Drop { .. } => Err(SqlValidationError::DdlNotAllowed {
            operation: "DROP".to_string(),
        }),
        Statement::AlterTable { .. } => Err(SqlValidationError::DdlNotAllowed {
            operation: "ALTER TABLE".to_string(),
        }),
        Statement::AlterIndex { .. } => Err(SqlValidationError::DdlNotAllowed {
            operation: "ALTER INDEX".to_string(),
        }),
        Statement::AlterView { .. } => Err(SqlValidationError::DdlNotAllowed {
            operation: "ALTER VIEW".to_string(),
        }),
        Statement::Truncate { .. } => Err(SqlValidationError::DdlNotAllowed {
            operation: "TRUNCATE".to_string(),
        }),

        // DML write operations - check access mode
        Statement::Insert(insert) => match &insert.table {
            TableObject::TableName(name) => {
                check_write_access("INSERT", &extract_table_name(name), config)
            }
            // Non-table targets (`INSERT INTO FUNCTION ...`) have no
            // registered name to access-check — fail closed.
            _ => Err(SqlValidationError::StatementNotAllowed {
                operation: "INSERT INTO FUNCTION".to_string(),
            }),
        },
        Statement::Update { table, .. } => {
            let table_name = extract_table_name_from_table_with_joins(table);
            check_write_access("UPDATE", &table_name, config)
        }
        Statement::Delete(delete) => {
            let table_name = extract_table_name_from_from_table(&delete.from);
            check_write_access("DELETE", &table_name, config)
        }

        // EXPLAIN wraps an inner statement that DataFusion may execute
        // (EXPLAIN ANALYZE runs the plan). Validate the inner statement so
        // EXPLAIN can never smuggle past DDL/write-access checks.
        Statement::Explain { statement, .. } => validate_statement(statement, config),

        // Everything else (COPY exports, SET, transactions, ...) is allowed
        // here: this path validates operator-authored pipeline SQL, where
        // those are legitimate. Untrusted ad-hoc SQL goes through
        // `validate_statement_strict`, which allowlists instead.
        _ => Ok(()),
    }
}

/// Lowercased bare value of one component of an [`ObjectName`], without
/// quoting (`"Auth"` → `auth`).
fn object_name_part_value(part: &ObjectNamePart) -> String {
    match part.as_ident() {
        Some(ident) => ident.value.to_lowercase(),
        // Non-identifier parts (e.g. BigQuery function-call parts) have no
        // bare value; fall back to their SQL rendering.
        None => part.to_string().to_lowercase(),
    }
}

fn extract_table_name(table: &ObjectName) -> String {
    // Keep the full qualified name (`schema_a.orders` must not be confused
    // with an unrelated flat source named `orders`), joining bare component
    // values so quoting cannot change identity (`"users"` == `users`).
    table
        .0
        .iter()
        .map(object_name_part_value)
        .collect::<Vec<_>>()
        .join(".")
}

fn extract_table_name_from_table_with_joins(table: &TableWithJoins) -> String {
    match &table.relation {
        TableFactor::Table { name, .. } => extract_table_name(name),
        _ => String::new(),
    }
}

fn extract_table_name_from_from_table(from_table: &FromTable) -> String {
    match from_table {
        FromTable::WithFromKeyword(tables) | FromTable::WithoutKeyword(tables) => {
            if let Some(first_table) = tables.first() {
                extract_table_name_from_table_with_joins(first_table)
            } else {
                String::new()
            }
        }
    }
}

/// DataFusion's default catalog and schema: flat sources register as
/// `datafusion.public.<name>`, so `users`, `public.users`, and
/// `datafusion.public.users` all resolve to the same table.
const DEFAULT_CATALOG: &str = "datafusion";
const DEFAULT_SCHEMA: &str = "public";

/// Drop default-catalog/schema qualifiers so every spelling of a flat
/// table reference is checked under its registered (bare) name.
fn strip_default_qualifiers(parts: &[&str]) -> Vec<String> {
    let parts = match parts {
        [DEFAULT_CATALOG, DEFAULT_SCHEMA, table] => vec![*table],
        [DEFAULT_SCHEMA, table] => vec![*table],
        // Hierarchical sources live as schemas in the default catalog:
        // `datafusion.mysrc.child` → `mysrc.child`.
        [DEFAULT_CATALOG, schema, table] => vec![*schema, *table],
        other => other.to_vec(),
    };
    parts.into_iter().map(str::to_string).collect()
}

fn check_write_access(
    operation: &str,
    table_name: &str,
    config: &SqlValidatorConfig,
) -> Result<(), SqlValidationError> {
    let raw_parts: Vec<&str> = table_name.split('.').collect();
    let parts = strip_default_qualifiers(&raw_parts);

    // Look up the normalized qualified name first; for `source.table`
    // references also honor the source's access mode, since hierarchical
    // sources register their tables under the source name as schema.
    let mode = config
        .table_access_modes
        .get(&parts.join("."))
        .or_else(|| config.table_access_modes.get(&parts[0]));

    if mode == Some(&AccessMode::ReadOnly) {
        return Err(SqlValidationError::WriteNotAllowed {
            operation: operation.to_string(),
            table: table_name.to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> SqlValidatorConfig {
        SqlValidatorConfig::new()
            .with_table("users", AccessMode::ReadOnly)
            .with_table("orders", AccessMode::ReadWrite)
            .with_table("readonly_table", AccessMode::ReadOnly)
    }

    /// Wrap access modes into an ad-hoc policy with no denied schemas.
    fn adhoc(access: SqlValidatorConfig) -> AdhocSqlPolicy {
        AdhocSqlPolicy::new(access)
    }

    #[test]
    fn test_select_allowed() {
        let config = test_config();
        assert!(validate_sql("SELECT * FROM users", &config).is_ok());
        assert!(validate_sql("SELECT * FROM orders", &config).is_ok());
        assert!(validate_sql("SELECT * FROM unknown_table", &config).is_ok());
    }

    #[test]
    fn test_ddl_blocked() {
        let config = test_config();

        let ddl_statements = vec![
            "CREATE TABLE test (id INT)",
            "DROP TABLE users",
            "ALTER TABLE users ADD COLUMN name VARCHAR(100)",
            "TRUNCATE TABLE orders",
            "CREATE INDEX idx ON users(id)",
            "CREATE VIEW v AS SELECT * FROM users",
            "DROP INDEX idx",
        ];

        for sql in ddl_statements {
            let result = validate_sql(sql, &config);
            assert!(result.is_err(), "DDL should be blocked: {}", sql);
            match result {
                Err(SqlValidationError::DdlNotAllowed { .. }) => {}
                _ => panic!("Expected DdlNotAllowed error for: {}", sql),
            }
        }
    }

    #[test]
    fn test_insert_readonly_blocked() {
        let config = test_config();
        let result = validate_sql("INSERT INTO users (id, name) VALUES (1, 'test')", &config);
        assert!(result.is_err());
        match result {
            Err(SqlValidationError::WriteNotAllowed { operation, table }) => {
                assert_eq!(operation, "INSERT");
                assert_eq!(table, "users");
            }
            _ => panic!("Expected WriteNotAllowed error"),
        }
    }

    #[test]
    fn test_insert_readwrite_allowed() {
        let config = test_config();
        let result = validate_sql("INSERT INTO orders (id, amount) VALUES (1, 100.0)", &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_update_readonly_blocked() {
        let config = test_config();
        let result = validate_sql("UPDATE users SET name = 'new' WHERE id = 1", &config);
        assert!(result.is_err());
        match result {
            Err(SqlValidationError::WriteNotAllowed { operation, table }) => {
                assert_eq!(operation, "UPDATE");
                assert_eq!(table, "users");
            }
            _ => panic!("Expected WriteNotAllowed error"),
        }
    }

    #[test]
    fn test_delete_readonly_blocked() {
        let config = test_config();
        let result = validate_sql("DELETE FROM users WHERE id = 1", &config);
        assert!(result.is_err());
        match result {
            Err(SqlValidationError::WriteNotAllowed { operation, table }) => {
                assert_eq!(operation, "DELETE");
                assert_eq!(table, "users");
            }
            _ => panic!("Expected WriteNotAllowed error"),
        }
    }

    #[test]
    fn test_unknown_table_insert_allowed() {
        let config = test_config();
        let result = validate_sql("INSERT INTO unknown_table (id) VALUES (1)", &config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_case_insensitive() {
        let config = test_config();
        let result = validate_sql("INSERT INTO USERS (id) VALUES (1)", &config);
        assert!(result.is_err());
    }

    #[test]
    fn test_insert_with_select() {
        let config = test_config();
        let result = validate_sql(
            "INSERT INTO orders (id, user_id) SELECT id, id FROM users",
            &config,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_complex_select_allowed() {
        let config = test_config();
        let result = validate_sql(
            "SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 1",
            &config,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_sql_parse_error() {
        let config = test_config();

        // Test various invalid SQL statements
        // Note: `SELECT FROM users` (empty projection) is not in this list —
        // sqlparser 0.59 parses it; it fails later at DataFusion planning.
        let invalid_statements = vec![
            "SELEKT * FROM users",       // Misspelled keyword
            "SELECT * FORM users",       // Misspelled FROM
            "INSERT INTO",               // Incomplete statement
            "SELECT * FROM users WHERE", // Incomplete WHERE clause
            "UPDATE SET name = 'test'",  // Missing table name
            "DELETE WHERE id = 1",       // Missing FROM
            "This is not SQL at all",    // Not SQL
        ];

        for sql in invalid_statements {
            let result = validate_sql(sql, &config);
            assert!(
                result.is_err(),
                "Invalid SQL should return error: '{}'",
                sql
            );
            match result {
                Err(SqlValidationError::ParseError(msg)) => {
                    assert!(
                        !msg.is_empty(),
                        "Parse error message should not be empty for: '{}'",
                        sql
                    );
                }
                Err(other) => panic!("Expected ParseError for '{}', got: {:?}", sql, other),
                Ok(_) => panic!("Expected error for invalid SQL: '{}'", sql),
            }
        }
    }

    #[test]
    fn test_empty_sql_is_valid() {
        // Empty SQL string is technically valid (no statements to execute)
        let config = test_config();
        let result = validate_sql("", &config);
        assert!(result.is_ok(), "Empty SQL should be valid (no statements)");
    }

    #[test]
    fn test_parameterized_query_valid() {
        let config = test_config();

        // Parameterized queries should be preprocessed and validated
        let result = validate_sql(
            "SELECT * FROM users WHERE name = {name} AND id = {user_id}",
            &config,
        );
        assert!(result.is_ok());

        let result = validate_sql(
            "INSERT INTO orders (user_id, amount) VALUES ({user_id}, {amount})",
            &config,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_parameterized_values_tuple_list() {
        // The runtime renderer expands `{rows}` into a multi-row tuple list
        // (`(c1, c2), (c1, c2)`) for batched inserts. The validator must accept
        // this shape — replacing `{rows}` with a quoted scalar literal would
        // produce `VALUES '__PARAM__'`, which fails SQL parsing and previously
        // crashed config load.
        let config = test_config();

        let result = validate_sql("INSERT INTO orders (id, amount) VALUES {rows}", &config);
        assert!(
            result.is_ok(),
            "VALUES {{rows}} (multi-row tuple list shape) should validate, got: {:?}",
            result
        );

        let result = validate_sql(
            "INSERT INTO orders (id, embedding) VALUES {rows} ON CONFLICT (id) DO NOTHING",
            &config,
        );
        assert!(
            result.is_ok(),
            "VALUES {{rows}} with ON CONFLICT clause should validate, got: {:?}",
            result
        );

        // Access-mode enforcement must still apply to the tuple-list shape.
        let result = validate_sql("INSERT INTO users (id, name) VALUES {rows}", &config);
        match result {
            Err(SqlValidationError::WriteNotAllowed { operation, table }) => {
                assert_eq!(operation, "INSERT");
                assert_eq!(table, "users");
            }
            other => panic!(
                "Expected WriteNotAllowed for read-only table, got: {:?}",
                other
            ),
        }
    }

    #[test]
    fn test_copy_allowed_on_pipeline_path() {
        // Pipeline SQL is operator-authored config; COPY ... TO is a
        // legitimate DataFusion export step and was allowed before the
        // /query endpoint existed. Only the ad-hoc path rejects it.
        let config = test_config();
        let result = validate_sql("COPY users TO 'out.csv'", &config);
        assert!(
            result.is_ok(),
            "COPY must stay allowed for pipeline SQL, got: {:?}",
            result
        );
    }

    #[test]
    fn test_copy_rejected_on_adhoc_path() {
        let config = adhoc(test_config());
        let result = validate_single_sql("COPY users TO 'out.csv'", &config);
        assert!(
            matches!(result, Err(SqlValidationError::StatementNotAllowed { .. })),
            "COPY must be rejected for ad-hoc SQL, got: {:?}",
            result
        );
    }

    #[test]
    fn test_set_allowed_on_pipeline_path() {
        let config = test_config();
        let result = validate_sql("SET a = 1", &config);
        assert!(
            result.is_ok(),
            "SET must stay allowed for pipeline SQL, got: {:?}",
            result
        );
    }

    #[test]
    fn test_adhoc_allowlist_rejects_unlisted_statements() {
        // The ad-hoc path is a strict allowlist: statement types that are
        // not explicitly permitted are rejected, including ones today's
        // sqlparser knows but the old denylist let through.
        let config = adhoc(test_config());
        let denied = vec![
            "MERGE INTO orders USING users ON orders.id = users.id \
             WHEN MATCHED THEN UPDATE SET amount = 0",
            "START TRANSACTION",
            "COMMIT",
            "ROLLBACK",
            "DEALLOCATE p",
            "GRANT SELECT ON users TO joe",
            "SET TIME ZONE 'UTC'",
        ];
        for sql in denied {
            let result = validate_single_sql(sql, &config);
            assert!(
                matches!(result, Err(SqlValidationError::StatementNotAllowed { .. })),
                "'{}' must be rejected by the ad-hoc allowlist, got: {:?}",
                sql,
                result
            );
        }
    }

    #[test]
    fn test_adhoc_allows_show_and_describe() {
        let config = adhoc(test_config());
        for sql in ["SHOW TABLES", "DESCRIBE users"] {
            let kind = validate_single_sql(sql, &config);
            assert!(
                matches!(kind, Ok(StatementKind::Other)),
                "'{}' should be allowed ad-hoc, got: {:?}",
                sql,
                kind
            );
        }
    }

    #[test]
    fn test_denied_schema_rejects_reads() {
        let config = adhoc(test_config()).with_denied_schema("auth");
        let denied = vec![
            "SELECT token FROM auth.sessions",
            "SELECT * FROM users u JOIN auth.sessions s ON u.id = s.user_id",
            "SELECT * FROM (SELECT token FROM auth.sessions) t",
            "WITH x AS (SELECT token FROM auth.sessions) SELECT * FROM x",
            "SELECT * FROM datafusion.auth.sessions",
            "EXPLAIN SELECT token FROM auth.sessions",
            "DESCRIBE auth.sessions",
            "SELECT * FROM AUTH.SESSIONS",
        ];
        for sql in denied {
            let result = validate_single_sql(sql, &config);
            assert!(
                matches!(result, Err(SqlValidationError::SchemaNotAllowed { .. })),
                "'{}' must be rejected (denied schema), got: {:?}",
                sql,
                result
            );
        }
    }

    #[test]
    fn test_denied_schema_rejects_writes() {
        let config = adhoc(test_config()).with_denied_schema("auth");
        let result = validate_single_sql("INSERT INTO auth.users (id) VALUES (1)", &config);
        assert!(
            matches!(result, Err(SqlValidationError::SchemaNotAllowed { .. })),
            "writes into the denied schema must be rejected, got: {:?}",
            result
        );
    }

    #[test]
    fn test_denied_schema_ignores_bare_table_named_auth() {
        // Only the schema qualifier is reserved; a table that happens to be
        // named `auth` is not in the `auth` schema.
        let config = adhoc(test_config()).with_denied_schema("auth");
        let result = validate_single_sql("SELECT * FROM auth", &config);
        assert!(result.is_ok(), "got: {:?}", result);
    }

    #[test]
    fn test_denied_schema_not_enforced_on_pipeline_path() {
        // Pipelines are operator-authored and may legitimately read auth
        // tables; the denial is scoped to ad-hoc SQL.
        // The trusted path carries no denied-schema notion at all, so it
        // can read auth tables regardless.
        let config = test_config();
        let result = validate_sql("SELECT token FROM auth.sessions", &config);
        assert!(result.is_ok(), "got: {:?}", result);
    }

    #[test]
    fn test_denied_schema_reached_through_indirect_relations() {
        // The denial must descend into every relation-bearing position, not
        // just the top-level FROM: set operations, scalar subqueries in the
        // projection, IN-subqueries, and table-function arguments. This pins
        // that `visit_relations` covers those forms (the guard's completeness
        // for a single ad-hoc statement rests on it).
        let config = adhoc(test_config()).with_denied_schema("auth");
        let indirect = vec![
            "SELECT id FROM users UNION SELECT token FROM auth.sessions",
            "SELECT (SELECT token FROM auth.sessions LIMIT 1) AS t",
            "SELECT id FROM users WHERE id IN (SELECT user_id FROM auth.sessions)",
            "SELECT id FROM users WHERE EXISTS (SELECT 1 FROM auth.sessions s WHERE s.user_id = users.id)",
        ];
        for sql in indirect {
            let result = validate_single_sql(sql, &config);
            assert!(
                matches!(result, Err(SqlValidationError::SchemaNotAllowed { .. })),
                "'{}' must be rejected via an indirect relation, got: {:?}",
                sql,
                result
            );
        }
    }

    #[test]
    fn test_adhoc_path_does_not_preprocess_braces() {
        // Ad-hoc SQL has no template parameters, so `{…}` spans must be
        // validated as-is. Brace substitution here would rewrite string
        // literals and reject valid queries (the braces below cross quote
        // boundaries, so the substituted SQL does not even parse).
        let config = adhoc(test_config());
        let result = validate_single_sql(r#"SELECT 'a{b' AS "c}d" FROM users"#, &config);
        assert!(
            matches!(result, Ok(StatementKind::Query)),
            "brace-containing literals must validate ad-hoc, got: {:?}",
            result
        );
    }

    #[test]
    fn test_qualified_write_does_not_match_unrelated_flat_source() {
        // `schema_a.readonly_table` is not the flat source named
        // `readonly_table`; matching on the bare last segment wrongly
        // rejected it.
        let config = test_config();
        let result = validate_sql(
            "INSERT INTO schema_a.readonly_table (id) VALUES (1)",
            &config,
        );
        assert!(
            result.is_ok(),
            "unrelated qualified table must not match a flat source, got: {:?}",
            result
        );
    }

    #[test]
    fn test_default_qualifiers_cannot_bypass_write_access() {
        // DataFusion resolves `users`, `public.users`, and
        // `datafusion.public.users` to the same flat table, so qualifying
        // with the default catalog/schema must not skip the access check.
        let config = test_config();
        let denied = vec![
            "INSERT INTO public.users (id) VALUES (1)",
            "INSERT INTO datafusion.public.users (id) VALUES (1)",
            "UPDATE public.users SET name = 'x' WHERE id = 1",
            "DELETE FROM datafusion.public.users WHERE id = 1",
        ];
        for sql in denied {
            let result = validate_sql(sql, &config);
            assert!(
                matches!(result, Err(SqlValidationError::WriteNotAllowed { .. })),
                "'{}' must be rejected (default-qualified read-only table), got: {:?}",
                sql,
                result
            );
        }
        // The read_write source stays writable through the same forms.
        let result = validate_sql(
            "INSERT INTO datafusion.public.orders (id) VALUES (1)",
            &config,
        );
        assert!(result.is_ok(), "got: {:?}", result);
    }

    #[test]
    fn test_insert_into_table_function_target_rejected() {
        // `INSERT INTO FUNCTION ...` (ClickHouse syntax) has no registered
        // table name to access-check; it must fail closed, not fall through.
        use datafusion::sql::sqlparser::dialect::ClickHouseDialect;
        let statements = Parser::parse_sql(
            &ClickHouseDialect {},
            "INSERT INTO FUNCTION remote('addr', db.tbl) VALUES (1)",
        )
        .expect("ClickHouse dialect parses INSERT INTO FUNCTION");
        let result = validate_statement(&statements[0], &test_config());
        assert!(
            matches!(result, Err(SqlValidationError::StatementNotAllowed { .. })),
            "non-table INSERT target must be rejected, got: {:?}",
            result
        );
    }

    #[test]
    fn test_quoted_identifiers_match_access_modes_and_denied_schemas() {
        // Quoting must not change identity: `"users"` is the read-only
        // `users` source, and `"auth"."sessions"` is still the auth schema.
        let config = adhoc(test_config()).with_denied_schema("auth");
        let result = validate_single_sql(r#"INSERT INTO "users" (id) VALUES (1)"#, &config);
        assert!(
            matches!(result, Err(SqlValidationError::WriteNotAllowed { .. })),
            "quoted read-only table must still be rejected, got: {:?}",
            result
        );
        let result = validate_single_sql(r#"SELECT * FROM "auth"."sessions""#, &config);
        assert!(
            matches!(result, Err(SqlValidationError::SchemaNotAllowed { .. })),
            "quoted auth schema must still be denied, got: {:?}",
            result
        );
    }

    #[test]
    fn test_qualified_write_checks_source_schema_access_mode() {
        // Hierarchical sources register their tables under the source name
        // as schema; a write to `mysrc.child` must honor `mysrc`'s access
        // mode.
        let config = SqlValidatorConfig::new().with_table("mysrc", AccessMode::ReadOnly);
        let result = validate_sql("INSERT INTO mysrc.child (id) VALUES (1)", &config);
        assert!(
            matches!(result, Err(SqlValidationError::WriteNotAllowed { .. })),
            "write into a read-only source's schema must be rejected, got: {:?}",
            result
        );
    }

    #[test]
    fn test_validate_single_sql_query_ok() {
        let config = adhoc(test_config());
        let kind = validate_single_sql("SELECT * FROM users", &config).unwrap();
        assert_eq!(kind, StatementKind::Query);
    }

    #[test]
    fn test_validate_single_sql_write_is_other() {
        let config = adhoc(test_config());
        let kind = validate_single_sql("INSERT INTO orders (id) VALUES (1)", &config).unwrap();
        assert_eq!(kind, StatementKind::Other);
    }

    #[test]
    fn test_validate_single_sql_multi_statement_rejected() {
        let config = adhoc(test_config());
        let result = validate_single_sql("SELECT 1; SELECT 2", &config);
        assert!(matches!(
            result,
            Err(SqlValidationError::NotExactlyOneStatement { count: 2 })
        ));
    }

    #[test]
    fn test_validate_single_sql_empty_rejected() {
        let config = adhoc(test_config());
        let result = validate_single_sql("", &config);
        assert!(matches!(
            result,
            Err(SqlValidationError::NotExactlyOneStatement { count: 0 })
        ));
    }

    #[test]
    fn test_validate_single_sql_enforces_existing_rules() {
        let config = adhoc(test_config());
        assert!(matches!(
            validate_single_sql("DROP TABLE users", &config),
            Err(SqlValidationError::DdlNotAllowed { .. })
        ));
        assert!(matches!(
            validate_single_sql("DELETE FROM users WHERE id = 1", &config),
            Err(SqlValidationError::WriteNotAllowed { .. })
        ));
        assert!(matches!(
            validate_single_sql("COPY users TO 'out.csv'", &config),
            Err(SqlValidationError::StatementNotAllowed { .. })
        ));
    }

    #[test]
    fn test_explain_analyze_insert_into_read_only_blocked() {
        let config = adhoc(test_config());
        let result = validate_single_sql(
            "EXPLAIN ANALYZE INSERT INTO users (id, name) VALUES (1, 'x')",
            &config,
        );
        assert!(
            matches!(result, Err(SqlValidationError::WriteNotAllowed { .. })),
            "EXPLAIN ANALYZE must inherit the inner statement's verdict, got: {:?}",
            result
        );
    }

    #[test]
    fn test_explain_ddl_blocked() {
        let config = adhoc(test_config());
        let result = validate_single_sql("EXPLAIN DROP TABLE users", &config);
        assert!(
            matches!(result, Err(SqlValidationError::DdlNotAllowed { .. })),
            "EXPLAIN of DDL must be rejected, got: {:?}",
            result
        );
    }

    #[test]
    fn test_explain_select_allowed() {
        let config = adhoc(test_config());
        let kind = validate_single_sql("EXPLAIN SELECT * FROM users", &config).unwrap();
        assert_eq!(kind, StatementKind::Other);
    }

    #[test]
    fn test_set_statement_blocked() {
        let config = adhoc(test_config());
        let result = validate_single_sql("SET a = 1", &config);
        assert!(
            matches!(result, Err(SqlValidationError::StatementNotAllowed { .. })),
            "SET must be rejected, got: {:?}",
            result
        );
    }

    #[test]
    fn test_prepare_statement_blocked() {
        let config = adhoc(test_config());
        let result = validate_single_sql(
            "PREPARE p AS INSERT INTO users (id, name) VALUES (1, 'x')",
            &config,
        );
        assert!(
            matches!(result, Err(SqlValidationError::StatementNotAllowed { .. })),
            "PREPARE must be rejected, got: {:?}",
            result
        );
    }

    #[test]
    fn test_execute_statement_blocked() {
        let config = adhoc(test_config());
        let result = validate_single_sql("EXECUTE p", &config);
        assert!(
            matches!(result, Err(SqlValidationError::StatementNotAllowed { .. })),
            "EXECUTE must be rejected, got: {:?}",
            result
        );
    }
}
