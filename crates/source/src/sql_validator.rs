use sqlparser::ast::{FromTable, ObjectName, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use thiserror::Error;

// Re-export AccessMode for convenience
pub use crate::access_mode::AccessMode;

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

fn preprocess_parameters(sql: &str) -> String {
    let mut result = sql.to_string();
    let mut start = 0;

    while let Some(open) = result[start..].find('{') {
        let open = start + open;
        if let Some(close) = result[open..].find('}') {
            let close = open + close;
            // Replace {param_name} with a placeholder string
            result = format!(
                "{}'{}'{}",
                &result[..open],
                "__PARAM__",
                &result[close + 1..]
            );
            start = open + "'__PARAM__'".len();
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
    if let Some(mode) = config.table_access_modes.get(table_name) {
        if *mode == AccessMode::ReadOnly {
            return Err(SqlValidationError::WriteNotAllowed {
                operation: operation.to_string(),
                table: table_name.to_string(),
            });
        }
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
}
