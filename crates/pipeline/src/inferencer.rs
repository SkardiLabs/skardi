use anyhow::{Result, anyhow};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use datafusion::sql::sqlparser::ast::{Expr, SetExpr, Statement};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;
use std::collections::HashMap;
use std::sync::Arc;

use crate::types::{InferredFieldType, RequestSchema, ResponseSchema};

/// SQL schema inference engine for extracting parameter and response schemas from SQL queries
/// TODO: Fix the issue that extra parameters in the request is allowed
pub struct SqlSchemaInferrer {
    datafusion_ctx: Arc<SessionContext>,
}

impl std::fmt::Debug for SqlSchemaInferrer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqlSchemaInferrer")
            .field("datafusion_ctx", &"<SessionContext>")
            .finish()
    }
}

/// Named parameter structure with enhanced metadata
#[derive(Debug, Clone)]
pub struct NamedParameter {
    pub name: String,        // Parameter name from {name} syntax
    pub column_name: String, // Inferred column that this parameter binds to
    pub field_type: DataType,
    pub nullable: bool,
    pub source_location: String,
    pub occurrences: usize, // Number of times this parameter appears
}

/// Validation report for SQL queries
#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
    pub parameter_count: usize,
    pub response_field_count: usize,
}

impl SqlSchemaInferrer {
    /// Create a new SQL schema inferrer with the provided Arc<SessionContext>
    pub fn new(datafusion_ctx: Arc<SessionContext>) -> Result<Self> {
        Ok(Self { datafusion_ctx })
    }

    /// Extract named parameters from SQL query using Rust-native {name} syntax
    /// and infer types from table schemas in the SessionContext
    pub async fn extract_parameters(
        &self,
        sql: &str,
    ) -> Result<HashMap<String, InferredFieldType>> {
        let named_params = self.extract_named_parameters(sql).await?;
        let mut parameters = HashMap::new();

        for (param_name, named_param) in named_params {
            parameters.insert(
                param_name,
                InferredFieldType {
                    field_type: named_param.field_type,
                    nullable: named_param.nullable,
                    source_location: named_param.source_location,
                },
            );
        }

        Ok(parameters)
    }

    /// Extract request schema from SQL query using named parameter analysis
    pub async fn extract_request_schema(&self, sql: &str) -> Result<RequestSchema> {
        let parameters = self.extract_parameters(sql).await?;
        Ok(RequestSchema { fields: parameters })
    }

    /// Extract response schema from SQL query using SELECT clause analysis
    pub async fn extract_response_schema(&self, sql: &str) -> Result<ResponseSchema> {
        let fields = self.extract_response_fields(sql).await?;
        Ok(ResponseSchema { fields })
    }

    /// Extract named parameters with full metadata using Rust-native {name} syntax
    /// Uses AST analysis for robust column-to-parameter mapping
    pub async fn extract_named_parameters(
        &self,
        sql: &str,
    ) -> Result<HashMap<String, NamedParameter>> {
        // Step 1: Convert {parameter_name} to ? placeholders and track parameter mapping
        let (sql_with_placeholders, parameter_order) = self.convert_named_to_placeholders(sql)?;

        // Step 2: Parse SQL with placeholders to get AST
        let statements = self.parse_sql_syntax(&sql_with_placeholders)?;

        // Step 3: Use AST to find column-parameter relationships
        let placeholder_to_column = self.extract_placeholder_columns(&statements)?;

        // Step 4: Build named parameter metadata using AST analysis + table schemas
        let mut named_parameters = HashMap::new();

        for (placeholder_index, param_name) in parameter_order.iter().enumerate() {
            // Count occurrences of this parameter in original SQL
            let param_pattern = format!(r"\{{{}\}}", regex::escape(param_name));
            let occurrences = regex::Regex::new(&param_pattern)
                .unwrap()
                .find_iter(sql)
                .count();

            // Get column name from AST analysis (more reliable than pattern matching)
            let column_name = placeholder_to_column
                .get(&placeholder_index)
                .cloned()
                .unwrap_or_else(|| self.infer_column_from_parameter_name(param_name));

            // Get column type from registered table schema or special context
            let column_type = self
                .get_column_type_with_special_cases(&column_name, sql)
                .await?;

            named_parameters.insert(
                param_name.clone(),
                NamedParameter {
                    name: param_name.clone(),
                    column_name: column_name.clone(),
                    field_type: column_type,
                    nullable: true, // Named parameters are typically nullable
                    source_location: format!(
                        "AST analysis: parameter '{}' → column '{}' (placeholder index {})",
                        param_name, column_name, placeholder_index
                    ),
                    occurrences,
                },
            );
        }

        Ok(named_parameters)
    }

    /// Convert {parameter_name} syntax to ? placeholders while tracking parameter order
    /// Returns (sql_with_placeholders, parameter_order)
    fn convert_named_to_placeholders(&self, sql: &str) -> Result<(String, Vec<String>)> {
        let mut parameter_order = Vec::new();
        let mut sql_with_placeholders = sql.to_string();

        // Extract all {parameter_name} patterns and replace with ? placeholders
        let parameter_pattern = regex::Regex::new(r"\{([a-zA-Z_][a-zA-Z0-9_]*)\}").unwrap();

        // Find all matches and collect unique parameter names in order
        let mut seen_params = std::collections::HashSet::new();
        for cap in parameter_pattern.captures_iter(sql) {
            let param_name = cap[1].to_string();
            if !seen_params.contains(&param_name) {
                seen_params.insert(param_name.clone());
                parameter_order.push(param_name);
            }
        }

        // Replace each unique {parameter_name} with ? placeholders
        for param_name in &parameter_order {
            let pattern = format!(r"\{{{}\}}", regex::escape(param_name));
            let regex = regex::Regex::new(&pattern).unwrap();
            sql_with_placeholders = regex.replace_all(&sql_with_placeholders, "?").to_string();
        }

        Ok((sql_with_placeholders, parameter_order))
    }

    /// Extract column names that are used with ? placeholders from AST
    /// Returns mapping from placeholder index to column name
    fn extract_placeholder_columns(
        &self,
        statements: &[Statement],
    ) -> Result<HashMap<usize, String>> {
        let mut placeholder_to_column = HashMap::new();
        let mut placeholder_index = 0;

        for statement in statements {
            if let Statement::Query(query) = statement {
                match &*query.body {
                    SetExpr::Select(select) => {
                        // Look for parameters in WHERE clauses
                        if let Some(selection) = &select.selection {
                            self.collect_parameter_columns_ast(
                                selection,
                                &mut placeholder_to_column,
                                &mut placeholder_index,
                            );
                        }
                    }
                    _ => continue,
                }

                // Look for parameters in LIMIT/OFFSET at the Query level
                // Note: In datafusion 50, LimitClause is now an enum with variants
                if let Some(limit_clause) = &query.limit_clause {
                    use datafusion::sql::sqlparser::ast::LimitClause;
                    match limit_clause {
                        LimitClause::LimitOffset { limit, offset, .. } => {
                            // Check for placeholder in LIMIT expression
                            if let Some(limit_expr) = limit {
                                if self.is_placeholder_ast(limit_expr) {
                                    placeholder_to_column
                                        .insert(placeholder_index, "limit".to_string());
                                    placeholder_index += 1;
                                }
                            }
                            // Check for placeholder in OFFSET expression
                            if let Some(offset_info) = offset {
                                if self.is_placeholder_ast(&offset_info.value) {
                                    placeholder_to_column
                                        .insert(placeholder_index, "offset".to_string());
                                    placeholder_index += 1;
                                }
                            }
                        }
                        LimitClause::OffsetCommaLimit {
                            offset: offset_expr,
                            limit: limit_expr,
                        } => {
                            // MySQL-style: LIMIT offset, limit
                            if self.is_placeholder_ast(offset_expr) {
                                placeholder_to_column
                                    .insert(placeholder_index, "offset".to_string());
                                placeholder_index += 1;
                            }
                            if self.is_placeholder_ast(limit_expr) {
                                placeholder_to_column
                                    .insert(placeholder_index, "limit".to_string());
                                placeholder_index += 1;
                            }
                        }
                    }
                }
            }
        }

        Ok(placeholder_to_column)
    }

    /// Recursively collect column names that are used with ? placeholders in AST expressions
    fn collect_parameter_columns_ast(
        &self,
        expr: &Expr,
        placeholder_to_column: &mut HashMap<usize, String>,
        placeholder_index: &mut usize,
    ) {
        match expr {
            Expr::BinaryOp { left, op: _, right } => {
                // Look for patterns like: column_name = ? or ? = column_name
                if let (Some(column_name), true) = (
                    self.extract_column_name_ast(left),
                    self.is_placeholder_ast(right),
                ) {
                    placeholder_to_column.insert(*placeholder_index, column_name);
                    *placeholder_index += 1;
                }
                if let (Some(column_name), true) = (
                    self.extract_column_name_ast(right),
                    self.is_placeholder_ast(left),
                ) {
                    placeholder_to_column.insert(*placeholder_index, column_name);
                    *placeholder_index += 1;
                }

                // Recursively search in sub-expressions
                self.collect_parameter_columns_ast(left, placeholder_to_column, placeholder_index);
                self.collect_parameter_columns_ast(right, placeholder_to_column, placeholder_index);
            }
            Expr::Nested(inner) => {
                self.collect_parameter_columns_ast(inner, placeholder_to_column, placeholder_index);
            }
            Expr::InList { expr, list, .. } => {
                // Handle IN clauses: column_name IN (?, ?, ?)
                if let Some(column_name) = self.extract_column_name_ast(expr) {
                    for item in list {
                        if self.is_placeholder_ast(item) {
                            placeholder_to_column.insert(*placeholder_index, column_name.clone());
                            *placeholder_index += 1;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    /// Check if an AST expression is a placeholder (?)
    fn is_placeholder_ast(&self, expr: &Expr) -> bool {
        matches!(
            expr,
            Expr::Value(datafusion::sql::sqlparser::ast::ValueWithSpan {
                value: datafusion::sql::sqlparser::ast::Value::Placeholder(_),
                ..
            })
        )
    }

    /// Extract column name from an AST expression if it's a simple identifier
    fn extract_column_name_ast(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Identifier(ident) => Some(ident.value.clone()),
            Expr::CompoundIdentifier(parts) => {
                // Handle quoted column names like "Brand" or table.column
                if parts.len() == 1 {
                    Some(parts[0].value.clone())
                } else if parts.len() == 2 {
                    Some(parts[1].value.clone()) // table.column -> column
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    /// Infer likely column name from parameter name using common patterns
    fn infer_column_from_parameter_name(&self, param_name: &str) -> String {
        // Handle common parameter naming patterns
        if param_name.starts_with("min_") {
            param_name
                .strip_prefix("min_")
                .unwrap_or(param_name)
                .to_string()
        } else if param_name.starts_with("max_") {
            param_name
                .strip_prefix("max_")
                .unwrap_or(param_name)
                .to_string()
        } else if param_name.ends_with("_min") {
            param_name
                .strip_suffix("_min")
                .unwrap_or(param_name)
                .to_string()
        } else if param_name.ends_with("_max") {
            param_name
                .strip_suffix("_max")
                .unwrap_or(param_name)
                .to_string()
        } else {
            // Default: assume parameter name matches column name
            param_name.to_string()
        }
    }

    /// Get column type from registered tables using the SessionContext
    async fn get_column_type_from_registered_tables(
        &self,
        column_name: &str,
    ) -> Result<Option<DataType>> {
        // Get table names from the default catalog
        // TODO: Allow custom catalog and schema
        let catalog = self
            .datafusion_ctx
            .catalog("datafusion")
            .ok_or_else(|| anyhow!("Default catalog not found"))?;
        let schema = catalog
            .schema("public")
            .ok_or_else(|| anyhow!("Public schema not found"))?;

        for table_name in schema.table_names() {
            if let Ok(Some(table)) = schema.table(&table_name).await {
                let table_schema = table.schema();

                // Look for the column in this table's schema
                for field in table_schema.fields() {
                    if field.name() == column_name
                        || field.name() == &format!("\"{}\"", column_name)
                    {
                        return Ok(Some(field.data_type().clone()));
                    }
                }
            }
        }

        Ok(None)
    }

    /// Get column type from context (registered tables or inferred)
    async fn get_column_type_from_context(&self, column_name: &str, sql: &str) -> Result<DataType> {
        // First try to get type from registered table schema
        if let Some(schema_type) = self
            .get_column_type_from_registered_tables(column_name)
            .await?
        {
            Ok(schema_type)
        } else {
            // Fallback to pattern-based inference from SQL context
            Ok(self.infer_type_from_sql_context(sql, column_name))
        }
    }

    /// Get column type with special case handling for LIMIT/OFFSET parameters
    async fn get_column_type_with_special_cases(
        &self,
        column_name: &str,
        sql: &str,
    ) -> Result<DataType> {
        // Handle special cases for SQL keywords that don't correspond to table columns
        match column_name.to_lowercase().as_str() {
            "limit" | "offset" => {
                // LIMIT and OFFSET parameters are always integers
                Ok(DataType::Int64)
            }
            _ => {
                // Use normal column type resolution for actual table columns
                self.get_column_type_from_context(column_name, sql).await
            }
        }
    }

    /// Extract response fields from SQL SELECT clause using DataFusion parsing
    /// against registered tables in the SessionContext
    pub async fn extract_response_fields(
        &self,
        sql: &str,
    ) -> Result<HashMap<String, InferredFieldType>> {
        // Parse the SQL to get the logical plan using the SessionContext
        let logical_plan = self.parse_sql_to_logical_plan(sql).await?;

        // Extract schema from the logical plan
        let schema = logical_plan.schema();
        let mut fields = HashMap::new();

        for field in schema.fields() {
            let field_name = field.name().clone();
            fields.insert(
                field_name.clone(),
                InferredFieldType {
                    field_type: field.data_type().clone(),
                    nullable: field.is_nullable(),
                    source_location: format!("SELECT field '{}'", field_name),
                },
            );
        }

        Ok(fields)
    }

    /// Validate SQL syntax and structure against registered tables in the SessionContext
    pub async fn validate_sql(&self, sql: &str) -> Result<ValidationReport> {
        let mut errors = Vec::new();
        let mut warnings = Vec::new();

        // Replace parameters for parsing and syntax validation
        let sql_for_parsing = self.replace_parameters_for_parsing(sql)?;

        // Check SQL syntax with parameters replaced
        let parse_result = self.parse_sql_syntax(&sql_for_parsing);
        if let Err(e) = parse_result {
            errors.push(format!("SQL syntax error: {}", e));
            return Ok(ValidationReport {
                is_valid: false,
                errors,
                warnings,
                parameter_count: 0,
                response_field_count: 0,
            });
        }

        // Try to create logical plan
        let plan_result = self.parse_sql_to_logical_plan(sql).await;
        let (parameter_count, response_field_count) = match plan_result {
            Ok(_) => {
                // Count parameters and response fields
                let params = self.extract_parameters(sql).await.unwrap_or_default();
                let fields = self.extract_response_fields(sql).await.unwrap_or_default();
                (params.len(), fields.len())
            }
            Err(e) => {
                warnings.push(format!("Logical plan creation warning: {}", e));
                // Still try to count parameters from the original SQL
                let params = self.extract_parameters(sql).await.unwrap_or_default();
                (params.len(), 0)
            }
        };

        // Check for common issues
        if parameter_count == 0 {
            warnings.push("No parameters found in SQL query".to_string());
        }

        if response_field_count == 0 {
            warnings.push("No response fields found in SQL query".to_string());
        }

        // Validate named parameters follow Rust identifier rules
        let named_params = self.extract_named_parameters(sql).await.unwrap_or_default();
        for (param_name, _) in &named_params {
            if !self.is_valid_rust_identifier(param_name) {
                warnings.push(format!(
                    "Parameter '{}' does not follow Rust identifier naming rules",
                    param_name
                ));
            }
        }

        Ok(ValidationReport {
            is_valid: errors.is_empty(),
            errors,
            warnings,
            parameter_count,
            response_field_count,
        })
    }

    /// Check if parameter name follows Rust identifier rules
    fn is_valid_rust_identifier(&self, name: &str) -> bool {
        if name.is_empty() {
            return false;
        }

        let first_char = name.chars().next().unwrap();
        if !first_char.is_alphabetic() && first_char != '_' {
            return false;
        }

        name.chars().all(|c| c.is_alphanumeric() || c == '_')
    }

    /// Parse SQL syntax using sqlparser
    fn parse_sql_syntax(&self, sql: &str) -> Result<Vec<Statement>> {
        let dialect = GenericDialect {};
        Parser::parse_sql(&dialect, sql).map_err(|e| anyhow!("Failed to parse SQL syntax: {}", e))
    }

    /// Parse SQL to DataFusion logical plan using the SessionContext
    async fn parse_sql_to_logical_plan(&self, sql: &str) -> Result<LogicalPlan> {
        // Replace parameters with placeholder values for parsing
        let sql_for_parsing = self.replace_parameters_for_parsing(sql)?;

        // Create logical plan using the SessionContext
        let plan = self
            .datafusion_ctx
            .sql(&sql_for_parsing)
            .await?
            .into_optimized_plan()
            .map_err(|e| anyhow!("Failed to create logical plan: {}", e))?;

        Ok(plan)
    }

    /// Replace {parameter_name} placeholders with NULL for DataFusion parsing
    fn replace_parameters_for_parsing(&self, sql: &str) -> Result<String> {
        // Replace {parameter_name} patterns with NULL for logical plan creation
        let parameter_pattern = regex::Regex::new(r"\{[a-zA-Z_][a-zA-Z0-9_]*\}").unwrap();
        let replaced_sql = parameter_pattern.replace_all(sql, "NULL");
        Ok(replaced_sql.to_string())
    }

    /// Infer data type from SQL context when table schema is not available
    fn infer_type_from_sql_context(&self, sql: &str, column_name: &str) -> DataType {
        if self.is_integer_context(sql, column_name) {
            DataType::Int64
        } else if self.is_numeric_context(sql, column_name) {
            DataType::Float64
        } else {
            // Default to string type
            DataType::Utf8
        }
    }

    /// Check if a column appears in numeric context (e.g., comparison with numbers)
    /// TODO: Handle compare with timestamps
    fn is_numeric_context(&self, sql: &str, column_name: &str) -> bool {
        let column_pattern = format!(r"\b{}\b", regex::escape(column_name));
        let numeric_patterns = [
            format!(r"{}\s*[<>=!]+\s*\d+\.?\d*", column_pattern),
            format!(r"\d+\.?\d*\s*[<>=!]+\s*{}", column_pattern),
            format!(r"{}\s*[<>=!]+\s*\d+", column_pattern),
            format!(r"\d+\s*[<>=!]+\s*{}", column_pattern),
        ];

        numeric_patterns
            .iter()
            .any(|pattern| regex::Regex::new(pattern).map_or(false, |regex| regex.is_match(sql)))
    }

    /// Check if a column appears in integer context (e.g., LIMIT clause)
    fn is_integer_context(&self, sql: &str, column_name: &str) -> bool {
        let column_pattern = regex::escape(column_name);
        let integer_patterns = [
            format!(r"LIMIT\s+{}", column_pattern),
            format!(r"OFFSET\s+{}", column_pattern),
            format!(r"TOP\s+{}", column_pattern),
        ];

        let sql_upper = sql.to_uppercase();
        let column_upper = column_name.to_uppercase();

        integer_patterns.iter().any(|pattern| {
            let pattern_upper = pattern.replace(column_name, &column_upper);
            regex::Regex::new(&pattern_upper).map_or(false, |regex| regex.is_match(&sql_upper))
        })
    }
}

impl Default for SqlSchemaInferrer {
    fn default() -> Self {
        Self::new(Arc::new(SessionContext::new())).expect("Failed to create SqlSchemaInferrer")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Float64Array, Int64Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    async fn create_test_context() -> SessionContext {
        let ctx = SessionContext::new();

        // Create a test products table schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("product_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("brand", DataType::Utf8, true),
            Field::new("price", DataType::Float64, false),
            Field::new("category", DataType::Utf8, true),
            Field::new("stock", DataType::Int64, false),
        ]));

        // Create sample data
        let product_ids = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let names = Arc::new(StringArray::from(vec![
            "Laptop Pro",
            "Gaming Mouse",
            "Wireless Headphones",
            "Tablet",
            "Smartphone",
        ]));
        let brands = Arc::new(StringArray::from(vec![
            Some("Apple"),
            Some("Logitech"),
            Some("Sony"),
            Some("Samsung"),
            Some("Google"),
        ]));
        let prices = Arc::new(Float64Array::from(vec![
            1299.99, 79.99, 199.99, 599.99, 899.99,
        ]));
        let categories = Arc::new(StringArray::from(vec![
            Some("Electronics"),
            Some("Accessories"),
            Some("Audio"),
            Some("Electronics"),
            Some("Electronics"),
        ]));
        let stock = Arc::new(Int64Array::from(vec![50, 120, 75, 30, 85]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![product_ids, names, brands, prices, categories, stock],
        )
        .unwrap();

        // Register the table
        ctx.register_batch("products", batch).unwrap();

        ctx
    }

    #[tokio::test]
    async fn test_extract_parameters() {
        let ctx = create_test_context().await;
        let inferrer = SqlSchemaInferrer::new(Arc::new(ctx)).unwrap();

        let sql = r#"
            SELECT product_id, name, price
            FROM products
            WHERE brand = {brand}
            AND price < {price}
        "#;

        let params = inferrer.extract_parameters(sql).await.unwrap();

        assert_eq!(params.len(), 2);
        assert!(params.contains_key("brand"));
        assert!(params.contains_key("price"));

        // All parameters should be nullable
        assert!(params["brand"].nullable);
        assert!(params["price"].nullable);

        // Check type inference based on registered table schema
        assert_eq!(params["brand"].field_type, DataType::Utf8);
        assert_eq!(params["price"].field_type, DataType::Float64);
    }

    #[tokio::test]
    async fn test_extract_response_fields() {
        let ctx = create_test_context().await;
        let inferrer = SqlSchemaInferrer::new(Arc::new(ctx)).unwrap();

        let sql = r#"
            SELECT
                product_id,
                name as product_name,
                price,
                brand
            FROM products
            WHERE price > {price}
        "#;

        let fields = inferrer.extract_response_fields(sql).await.unwrap();

        assert_eq!(fields.len(), 4);
        assert!(fields.contains_key("product_id"));
        assert!(fields.contains_key("product_name"));
        assert!(fields.contains_key("price"));
        assert!(fields.contains_key("brand"));

        // Check field types match table schema
        assert_eq!(fields["product_id"].field_type, DataType::Int64);
        assert_eq!(fields["product_name"].field_type, DataType::Utf8);
        assert_eq!(fields["price"].field_type, DataType::Float64);
        assert_eq!(fields["brand"].field_type, DataType::Utf8);

        // Check nullability
        assert!(!fields["product_id"].nullable);
        assert!(!fields["product_name"].nullable);
        assert!(!fields["price"].nullable);
        assert!(fields["brand"].nullable);
    }

    #[tokio::test]
    async fn test_validate_sql_with_registered_table() {
        let ctx = create_test_context().await;
        let inferrer = SqlSchemaInferrer::new(Arc::new(ctx)).unwrap();

        // Valid SQL with registered table
        let valid_sql = "SELECT product_id, name FROM products WHERE price > {price}";
        let report = inferrer.validate_sql(valid_sql).await.unwrap();
        assert!(report.is_valid);
        assert_eq!(report.parameter_count, 1);
        assert_eq!(report.response_field_count, 2);

        // Invalid SQL - clear syntax error
        let invalid_sql = "SELECT * FROM table WHERE column =";
        let report = inferrer.validate_sql(invalid_sql).await.unwrap();
        assert!(!report.is_valid);
        assert!(!report.errors.is_empty());

        // Valid syntax but references non-existent table
        let nonexistent_table_sql = "SELECT id FROM missing_table WHERE value = {value}";
        let report = inferrer.validate_sql(nonexistent_table_sql).await.unwrap();
        // This should generate warnings but not necessarily be invalid
        assert_eq!(report.parameter_count, 1); // Fallback inference should still detect the parameter
    }

    #[test]
    fn test_parameter_type_inference() {
        let ctx = SessionContext::new();
        let inferrer = SqlSchemaInferrer::new(Arc::new(ctx)).unwrap();

        // Test numeric context
        assert!(inferrer.is_numeric_context("WHERE price > 100", "price"));
        assert!(inferrer.is_numeric_context("WHERE score <= 100.5", "score"));
        assert!(inferrer.is_numeric_context("WHERE value = 42", "value"));

        // Test integer context - note that these patterns need the column in a LIMIT/OFFSET context
        assert!(inferrer.is_integer_context("SELECT * FROM table LIMIT count", "count"));
        assert!(inferrer.is_integer_context("SELECT * FROM table OFFSET skip", "skip"));

        // Test string context (default)
        assert!(!inferrer.is_numeric_context("WHERE name = 'brand'", "name"));
        assert!(!inferrer.is_integer_context("WHERE category = 'type'", "category"));
    }

    #[tokio::test]
    async fn test_fallback_type_inference() {
        // Test fallback type inference when no tables are registered
        let ctx = SessionContext::new(); // No tables registered
        let inferrer = SqlSchemaInferrer::new(Arc::new(ctx)).unwrap();

        // Test the pattern-based inference logic directly
        assert_eq!(
            inferrer.infer_type_from_sql_context("WHERE score > 100", "score"),
            DataType::Float64
        );
        assert_eq!(
            inferrer.infer_type_from_sql_context("LIMIT count", "count"),
            DataType::Int64
        );
        assert_eq!(
            inferrer.infer_type_from_sql_context("WHERE name = 'test'", "name"),
            DataType::Utf8
        );

        // Test full parameter extraction with fallback
        let sql = r#"
            SELECT * FROM unknown_table
            WHERE name = {name}
            AND description = {description}
        "#;

        let params = inferrer.extract_parameters(sql).await.unwrap();

        assert_eq!(params.len(), 2);

        // Since there are no numeric patterns in this SQL, both should default to string
        assert_eq!(params["name"].field_type, DataType::Utf8);
        assert_eq!(params["description"].field_type, DataType::Utf8);

        // Test that all parameters are nullable
        assert!(params["name"].nullable);
        assert!(params["description"].nullable);
    }

    #[tokio::test]
    async fn test_complex_parameter_patterns() {
        let ctx = create_test_context().await;
        let inferrer = SqlSchemaInferrer::new(Arc::new(ctx)).unwrap();

        let complex_sql = r#"
            SELECT product_id, name, price, brand
            FROM products
            WHERE brand = {brand}
            AND price > {price}
            AND stock > {stock}
            ORDER BY price DESC
        "#;

        let params = inferrer.extract_parameters(complex_sql).await.unwrap();

        // Should find parameters for the named parameters
        assert_eq!(params.len(), 3); // brand, price, stock
        assert!(params.contains_key("brand"));
        assert!(params.contains_key("price"));
        assert!(params.contains_key("stock"));

        // Check type inference from table schema
        assert_eq!(params["brand"].field_type, DataType::Utf8);
        assert_eq!(params["price"].field_type, DataType::Float64);
        assert_eq!(params["stock"].field_type, DataType::Int64);
    }

    #[tokio::test]
    async fn test_named_parameter_extraction() {
        let ctx = create_test_context().await;
        let inferrer = SqlSchemaInferrer::new(Arc::new(ctx)).unwrap();

        let sql = r#"
            SELECT product_id, name, price, brand
            FROM products
            WHERE brand = {brand}
            AND price >= {min_price}
            AND price <= {max_price}
            AND category = {category}
            LIMIT {limit}
        "#;

        let named_params = inferrer.extract_named_parameters(sql).await.unwrap();

        // Should find all 5 named parameters
        assert_eq!(named_params.len(), 5);
        assert!(named_params.contains_key("brand"));
        assert!(named_params.contains_key("min_price"));
        assert!(named_params.contains_key("max_price"));
        assert!(named_params.contains_key("category"));
        assert!(named_params.contains_key("limit"));

        // Check parameter metadata
        let brand_param = &named_params["brand"];
        assert_eq!(brand_param.name, "brand");
        assert_eq!(brand_param.column_name, "brand");
        assert_eq!(brand_param.field_type, DataType::Utf8);
        assert!(brand_param.nullable);
        assert_eq!(brand_param.occurrences, 1);

        // Check min_price and max_price both map to price column
        let min_price_param = &named_params["min_price"];
        assert_eq!(min_price_param.name, "min_price");
        assert_eq!(min_price_param.column_name, "price");
        assert_eq!(min_price_param.field_type, DataType::Float64);

        let max_price_param = &named_params["max_price"];
        assert_eq!(max_price_param.name, "max_price");
        assert_eq!(max_price_param.column_name, "price");
        assert_eq!(max_price_param.field_type, DataType::Float64);

        // Check limit parameter
        let limit_param = &named_params["limit"];
        assert_eq!(limit_param.name, "limit");
        assert_eq!(limit_param.field_type, DataType::Int64);
    }

    #[tokio::test]
    async fn test_parameter_replacement_for_parsing() {
        let ctx = SessionContext::new();
        let inferrer = SqlSchemaInferrer::new(Arc::new(ctx)).unwrap();

        let sql =
            "SELECT * FROM products WHERE brand = {brand} AND price > {min_price} LIMIT {limit}";
        let replaced = inferrer.replace_parameters_for_parsing(sql).unwrap();

        // Should replace all {param} with NULL
        let expected = "SELECT * FROM products WHERE brand = NULL AND price > NULL LIMIT NULL";
        assert_eq!(replaced, expected);
    }

    #[tokio::test]
    async fn test_rust_identifier_validation() {
        let ctx = SessionContext::new();
        let inferrer = SqlSchemaInferrer::new(Arc::new(ctx)).unwrap();

        // Valid Rust identifiers
        assert!(inferrer.is_valid_rust_identifier("valid_name"));
        assert!(inferrer.is_valid_rust_identifier("_underscore"));
        assert!(inferrer.is_valid_rust_identifier("name123"));
        assert!(inferrer.is_valid_rust_identifier("CamelCase"));

        // Invalid Rust identifiers
        assert!(!inferrer.is_valid_rust_identifier("123invalid"));
        assert!(!inferrer.is_valid_rust_identifier("invalid-name"));
        assert!(!inferrer.is_valid_rust_identifier("invalid.name"));
        assert!(!inferrer.is_valid_rust_identifier(""));
    }

    #[tokio::test]
    async fn test_parameter_name_to_column_inference() {
        let ctx = SessionContext::new();
        let inferrer = SqlSchemaInferrer::new(Arc::new(ctx)).unwrap();

        // Test common naming patterns
        assert_eq!(
            inferrer.infer_column_from_parameter_name("min_price"),
            "price"
        );
        assert_eq!(
            inferrer.infer_column_from_parameter_name("max_price"),
            "price"
        );
        assert_eq!(
            inferrer.infer_column_from_parameter_name("price_min"),
            "price"
        );
        assert_eq!(
            inferrer.infer_column_from_parameter_name("price_max"),
            "price"
        );
        assert_eq!(inferrer.infer_column_from_parameter_name("brand"), "brand");
        assert_eq!(
            inferrer.infer_column_from_parameter_name("category"),
            "category"
        );
    }

    #[tokio::test]
    async fn test_ast_based_parameter_extraction() {
        let ctx = create_test_context().await;
        let inferrer = SqlSchemaInferrer::new(Arc::new(ctx)).unwrap();

        // Test complex SQL with multiple parameter types - demonstrates AST analysis superiority
        let sql = r#"
            SELECT product_id, name, price, brand, category
            FROM products
            WHERE (brand = {brand_filter} OR brand = {backup_brand})
            AND price >= {min_price} AND price <= {max_price}
            AND category IN ({primary_category}, {secondary_category})
            AND stock > {min_stock}
            ORDER BY price ASC
            LIMIT {limit}
        "#;

        let named_params = inferrer.extract_named_parameters(sql).await.unwrap();

        // Should find all unique named parameters
        assert_eq!(named_params.len(), 8); // brand_filter, backup_brand, min_price, max_price, primary_category, secondary_category, min_stock, limit

        // Test AST-based column mapping accuracy
        let brand_filter = &named_params["brand_filter"];
        assert_eq!(brand_filter.column_name, "brand");
        assert_eq!(brand_filter.field_type, DataType::Utf8);

        let backup_brand = &named_params["backup_brand"];
        assert_eq!(backup_brand.column_name, "brand");
        assert_eq!(backup_brand.field_type, DataType::Utf8);

        // Test BETWEEN clause mapping (AST can handle complex operators)
        let min_price = &named_params["min_price"];
        assert_eq!(min_price.column_name, "price");
        assert_eq!(min_price.field_type, DataType::Float64);

        let max_price = &named_params["max_price"];
        assert_eq!(max_price.column_name, "price");
        assert_eq!(max_price.field_type, DataType::Float64);

        // Test IN clause mapping (multiple parameters for same column)
        let primary_category = &named_params["primary_category"];
        assert_eq!(primary_category.column_name, "category");
        assert_eq!(primary_category.field_type, DataType::Utf8);

        let secondary_category = &named_params["secondary_category"];
        assert_eq!(secondary_category.column_name, "category");
        assert_eq!(secondary_category.field_type, DataType::Utf8);

        // Test stock comparison
        let min_stock = &named_params["min_stock"];
        assert_eq!(min_stock.column_name, "stock");
        assert_eq!(min_stock.field_type, DataType::Int64);

        // Test LIMIT special case handling
        let limit_param = &named_params["limit"];
        assert_eq!(limit_param.column_name, "limit");
        assert_eq!(limit_param.field_type, DataType::Int64); // Special case: always integer

        // Verify source location indicates AST analysis
        assert!(brand_filter.source_location.contains("AST analysis"));
        assert!(limit_param.source_location.contains("placeholder index"));
    }

    #[tokio::test]
    async fn test_convert_named_to_placeholders() {
        let ctx = SessionContext::new();
        let inferrer = SqlSchemaInferrer::new(Arc::new(ctx)).unwrap();

        let sql = "SELECT * FROM products WHERE brand = {brand} AND price > {min_price} AND price < {min_price} LIMIT {limit}";
        let (sql_with_placeholders, parameter_order) =
            inferrer.convert_named_to_placeholders(sql).unwrap();

        // Should replace all {param} with ? and preserve unique parameter order
        assert_eq!(
            sql_with_placeholders,
            "SELECT * FROM products WHERE brand = ? AND price > ? AND price < ? LIMIT ?"
        );
        assert_eq!(parameter_order, vec!["brand", "min_price", "limit"]); // Unique parameters only

        // Test parameter order preservation with repeated parameters
        assert_eq!(parameter_order.len(), 3); // Only unique parameters
        assert!(parameter_order.contains(&"brand".to_string()));
        assert!(parameter_order.contains(&"min_price".to_string()));
        assert!(parameter_order.contains(&"limit".to_string()));
    }
}
