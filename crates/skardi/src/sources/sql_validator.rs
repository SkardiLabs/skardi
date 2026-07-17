use sqlparser::ast::{FromTable, ObjectName, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use thiserror::Error;

// Re-export AccessMode for convenience
pub use crate::sources::access_mode::AccessMode;

#[derive(Debug, Clone)]
pub struct SqlValidatorConfig {
    pub table_access_modes: HashMap<String, AccessMode>,
}

impl Default for SqlValidatorConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl SqlValidatorConfig {
    pub fn new() -> Self {
        Self {
            table_access_modes: HashMap::new(),
        }
    }

    pub fn with_table(mut self, table_name: &str, mode: AccessMode) -> Self {
        self.table_access_modes
            .insert(table_name.to_lowercase(), mode);
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

    #[error(
        "COPY operation not allowed. COPY can read or write files on the server and is not permitted on any data source."
    )]
    CopyNotAllowed,

    #[error("Expected exactly one SQL statement, found {count}.")]
    NotExactlyOneStatement { count: usize },

    #[error(
        "Statement type '{operation}' not allowed. It can mutate shared session state and is not permitted on any data source."
    )]
    StatementNotAllowed { operation: String },
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

/// Validate SQL that must consist of exactly one statement.
///
/// Applies the same rules as [`validate_sql`] (DDL and COPY always rejected,
/// writes checked against per-table access modes) and additionally rejects
/// input that parses to zero or more than one statement. Returns the
/// statement's [`StatementKind`] on success.
pub fn validate_single_sql(
    sql: &str,
    config: &SqlValidatorConfig,
) -> Result<StatementKind, SqlValidationError> {
    let preprocessed_sql = preprocess_parameters(sql);

    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, &preprocessed_sql)
        .map_err(|e| SqlValidationError::ParseError(e.to_string()))?;

    if statements.len() != 1 {
        return Err(SqlValidationError::NotExactlyOneStatement {
            count: statements.len(),
        });
    }

    let statement = &statements[0];
    validate_statement(statement, config)?;

    Ok(if matches!(statement, Statement::Query(_)) {
        StatementKind::Query
    } else {
        StatementKind::Other
    })
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

        // File-transfer operations - always blocked (can touch the server's filesystem)
        Statement::Copy { .. } => Err(SqlValidationError::CopyNotAllowed),
        Statement::CopyIntoSnowflake { .. } => Err(SqlValidationError::CopyNotAllowed),

        // DML write operations - check access mode
        Statement::Insert(insert) => {
            let table_name = extract_table_name(&insert.table_name);
            check_write_access("INSERT", &table_name, config)
        }
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
        // EXPLAIN can never smuggle past DDL/COPY/write-access checks.
        Statement::Explain { statement, .. } => validate_statement(statement, config),

        // Session-mutating statements affect the process-wide SessionContext
        // shared across all requests — always blocked.
        Statement::SetVariable { .. } => Err(SqlValidationError::StatementNotAllowed {
            operation: "SET".to_string(),
        }),
        Statement::SetRole { .. } => Err(SqlValidationError::StatementNotAllowed {
            operation: "SET ROLE".to_string(),
        }),
        Statement::SetNames { .. } => Err(SqlValidationError::StatementNotAllowed {
            operation: "SET NAMES".to_string(),
        }),

        // Read operations and others - always allowed
        _ => Ok(()),
    }
}

fn extract_table_name(table: &ObjectName) -> String {
    table
        .0
        .last()
        .map(|ident| ident.value.to_lowercase())
        .unwrap_or_default()
}

fn extract_table_name_from_table_with_joins(table: &sqlparser::ast::TableWithJoins) -> String {
    match &table.relation {
        sqlparser::ast::TableFactor::Table { name, .. } => extract_table_name(name),
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

fn check_write_access(
    operation: &str,
    table_name: &str,
    config: &SqlValidatorConfig,
) -> Result<(), SqlValidationError> {
    if let Some(mode) = config.table_access_modes.get(table_name)
        && *mode == AccessMode::ReadOnly
    {
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
        let invalid_statements = vec![
            "SELEKT * FROM users",       // Misspelled keyword
            "SELECT FROM users",         // Missing column list
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
    fn test_copy_blocked() {
        let config = test_config();
        let result = validate_sql("COPY users TO 'out.csv'", &config);
        assert!(
            matches!(result, Err(SqlValidationError::CopyNotAllowed)),
            "COPY must be rejected, got: {:?}",
            result
        );
    }

    #[test]
    fn test_validate_single_sql_query_ok() {
        let config = test_config();
        let kind = validate_single_sql("SELECT * FROM users", &config).unwrap();
        assert_eq!(kind, StatementKind::Query);
    }

    #[test]
    fn test_validate_single_sql_write_is_other() {
        let config = test_config();
        let kind = validate_single_sql("INSERT INTO orders (id) VALUES (1)", &config).unwrap();
        assert_eq!(kind, StatementKind::Other);
    }

    #[test]
    fn test_validate_single_sql_multi_statement_rejected() {
        let config = test_config();
        let result = validate_single_sql("SELECT 1; SELECT 2", &config);
        assert!(matches!(
            result,
            Err(SqlValidationError::NotExactlyOneStatement { count: 2 })
        ));
    }

    #[test]
    fn test_validate_single_sql_empty_rejected() {
        let config = test_config();
        let result = validate_single_sql("", &config);
        assert!(matches!(
            result,
            Err(SqlValidationError::NotExactlyOneStatement { count: 0 })
        ));
    }

    #[test]
    fn test_validate_single_sql_enforces_existing_rules() {
        let config = test_config();
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
            Err(SqlValidationError::CopyNotAllowed)
        ));
    }

    #[test]
    fn test_explain_analyze_insert_into_read_only_blocked() {
        let config = test_config();
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
        let config = test_config();
        let result = validate_single_sql("EXPLAIN DROP TABLE users", &config);
        assert!(
            matches!(result, Err(SqlValidationError::DdlNotAllowed { .. })),
            "EXPLAIN of DDL must be rejected, got: {:?}",
            result
        );
    }

    #[test]
    fn test_explain_select_allowed() {
        let config = test_config();
        let kind = validate_single_sql("EXPLAIN SELECT * FROM users", &config).unwrap();
        assert_eq!(kind, StatementKind::Other);
    }

    #[test]
    fn test_set_statement_blocked() {
        let config = test_config();
        let result = validate_single_sql("SET a = 1", &config);
        assert!(
            matches!(result, Err(SqlValidationError::StatementNotAllowed { .. })),
            "SET must be rejected, got: {:?}",
            result
        );
    }
}
