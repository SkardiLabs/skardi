use anyhow::{Result, anyhow};
use datafusion::arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// Legacy TypeConverter traits kept for backward compatibility but simplified
// Now that we work directly with DataFusion types, most of this complexity is unnecessary

/// Validates individual pipeline components for correctness
///
/// This trait ensures that pipeline components (metadata, request, response schemas)
/// are internally consistent and follow business rules before being processed.
pub trait SchemaValidation {
    /// Validate a complete pipeline component for internal consistency
    ///
    /// # Arguments
    /// * `component` - Pipeline component to validate (metadata, request, or response)
    ///
    /// # Returns
    /// * `Result<()>` - Success or detailed validation error with field paths
    fn validate_component(&self, component: &PipelineComponent) -> Result<()>;

    /// Check field type compatibility within a component
    ///
    /// # Arguments
    /// * `field_name` - Name of the field being validated
    /// * `field_type` - Type definition for the field
    /// * `nullable` - Whether the field can be null
    ///
    /// # Returns
    /// * `Result<()>` - Success or validation error with specific field context
    fn validate_field_type(
        &self,
        field_name: &str,
        field_type: &DataType,
        nullable: bool,
    ) -> Result<()>;

    /// Validate nullable constraints and default value compatibility
    ///
    /// # Arguments
    /// * `field_name` - Name of the field
    /// * `nullable` - Whether field allows null values
    /// * `default_value` - Optional default value for the field
    ///
    /// # Returns
    /// * `Result<()>` - Success or constraint violation error
    fn validate_nullable_constraints(
        &self,
        field_name: &str,
        nullable: bool,
        default_value: Option<&FieldValue>,
    ) -> Result<()>;

    /// Generate detailed validation error report with field paths
    ///
    /// # Arguments
    /// * `errors` - Collection of validation errors
    ///
    /// # Returns
    /// * `ValidationReport` - Structured error report with field locations and suggestions
    fn generate_error_report(&self, errors: &[ValidationError]) -> ValidationReport;
}

/// Handles serialization to/from YAML format
///
/// This trait manages the conversion between pipeline data structures and their
/// YAML representations with type validation and normalization.
pub trait YamlSerializable {
    /// Serialize component to YAML string
    ///
    /// # Returns
    /// * `Result<String>` - YAML string representation or serialization error
    fn to_yaml(&self) -> Result<String>;

    /// Deserialize component from YAML string
    ///
    /// # Arguments
    /// * `yaml_str` - YAML string to parse
    ///
    /// # Returns
    /// * `Result<Self>` - Parsed component or deserialization error
    fn from_yaml(yaml_str: &str) -> Result<Self>
    where
        Self: Sized;
}

impl YamlSerializable for PipelineComponent {
    /// Serialize component to YAML string
    ///
    /// This function converts the PipelineComponent to YAML format using direct
    /// DataFusion type serialization through our custom serde module.
    ///
    /// # Returns
    /// * `Ok(String)` - YAML string representation of the component
    /// * `Err(...)` - Serialization error with context
    ///
    /// # Examples
    /// ```
    /// use pipeline::types::{PipelineComponent, ComponentMetadata, YamlSerializable};
    ///
    /// let metadata = ComponentMetadata {
    ///     name: "test_pipeline".to_string(),
    ///     version: "1.0.0".to_string(),
    ///     description: Some("Test pipeline".to_string()),
    ///     created_at: Some("2025-01-15T10:00:00.000+00:00".to_string()),
    ///     updated_at: Some("2025-01-15T10:00:00.000+00:00".to_string()),
    /// };
    /// let component = PipelineComponent::Metadata(metadata);
    ///
    /// let yaml_output = component.to_yaml().unwrap();
    /// assert!(yaml_output.contains("name: test_pipeline"));
    /// assert!(yaml_output.contains("version: 1.0.0"));
    /// ```
    fn to_yaml(&self) -> Result<String> {
        // Direct serialization using serde - DataType serialization handled by custom module
        serde_yaml::to_string(self)
            .map_err(|e| anyhow!("Failed to serialize component to YAML: {}", e))
    }

    fn from_yaml(yaml_str: &str) -> Result<Self>
    where
        Self: Sized,
    {
        // Direct deserialization using serde - DataType deserialization handled by custom module
        let component: PipelineComponent = serde_yaml::from_str(yaml_str)
            .map_err(|e| anyhow!("Failed to deserialize YAML: {}", e))?;

        Ok(component)
    }
}

// ============================================================================
// Type Definitions for Trait Usage
// ============================================================================

/// Pipeline component enum for validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PipelineComponent {
    Metadata(ComponentMetadata),
    Query(QueryDefinition),
}

/// Component metadata structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentMetadata {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

/// Inferred field type information with DataFusion types
#[derive(Debug, Clone)]
pub struct InferredFieldType {
    pub field_type: DataType,
    pub nullable: bool,
    pub source_location: String,
}

/// Request schema definition using inferred types
#[derive(Debug, Clone)]
pub struct RequestSchema {
    pub fields: HashMap<String, InferredFieldType>,
}

/// Query definition structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryDefinition {
    pub sql: String,
    pub parameters: Vec<String>,
}

/// Response schema definition using inferred types
#[derive(Debug, Clone)]
pub struct ResponseSchema {
    pub fields: HashMap<String, InferredFieldType>,
}

/// Legacy field definition for backward compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldDefinition {
    pub field_type: String, // Store as string, convert to DataType when needed
    pub nullable: bool,
    pub default_value: Option<FieldValue>,
    pub description: Option<String>,
}

/// Field value enum for different data types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FieldValue {
    String(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Null,
}

/// Validation error with field context
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub field_path: String,
    pub error_type: ValidationErrorType,
    pub message: String,
    pub suggestion: Option<String>,
}

/// Types of validation errors
#[derive(Debug, Clone)]
pub enum ValidationErrorType {
    TypeMismatch,
    NullabilityViolation,
    InvalidValue,
    MissingRequired,
    InvalidFormat,
}

/// Validation report with structured errors
#[derive(Debug, Clone)]
pub struct ValidationReport {
    pub errors: Vec<ValidationError>,
    pub warnings: Vec<String>,
    pub summary: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    // ============================================================================
    // Basic YAML Serialization Tests
    // ============================================================================

    #[test]
    fn test_inferred_field_type_creation() {
        // Test that InferredFieldType can be created and accessed properly
        let field = InferredFieldType {
            field_type: DataType::Int64,
            nullable: false,
            source_location: "test_location".to_string(),
        };

        assert_eq!(field.field_type, DataType::Int64);
        assert_eq!(field.nullable, false);
        assert_eq!(field.source_location, "test_location");
    }

    #[test]
    fn test_to_yaml_metadata_component() {
        let metadata = ComponentMetadata {
            name: "test_pipeline".to_string(),
            version: "1.0.0".to_string(),
            description: Some("Test pipeline description".to_string()),
            created_at: Some("2025-01-15T10:00:00.000+00:00".to_string()),
            updated_at: Some("2025-01-15T10:00:00.000+00:00".to_string()),
        };
        let component = PipelineComponent::Metadata(metadata);

        let yaml_result = component.to_yaml();
        assert!(yaml_result.is_ok());

        let yaml_output = yaml_result.unwrap();

        assert!(yaml_output.contains("name: test_pipeline"));
        assert!(yaml_output.contains("version: 1.0.0"));
        assert!(yaml_output.contains("description: Test pipeline description"));
        assert!(yaml_output.contains("created_at: 2025-01-15T10:00:00.000+00:00"));
        assert!(yaml_output.contains("updated_at: 2025-01-15T10:00:00.000+00:00"));
    }

    #[test]
    fn test_to_yaml_query_component() {
        let query = QueryDefinition {
            sql: "SELECT * FROM table WHERE id = {user_id}".to_string(),
            parameters: vec!["user_id".to_string(), "limit".to_string()],
        };
        let component = PipelineComponent::Query(query);

        let yaml_result = component.to_yaml();
        assert!(yaml_result.is_ok());

        let yaml_output = yaml_result.unwrap();
        assert!(yaml_output.contains("sql: SELECT * FROM table WHERE id = {user_id}"));
        assert!(yaml_output.contains("user_id"));
        assert!(yaml_output.contains("limit"));
    }

    #[test]
    fn test_from_yaml_metadata_component() {
        let yaml_input = r#"
!Metadata
name: test_pipeline
version: 1.0.0
description: Test pipeline description
created_at: 2025-01-15T10:00:00.000+00:00
updated_at: 2025-01-15T10:00:00.000+00:00
"#;

        let result = PipelineComponent::from_yaml(yaml_input);
        assert!(result.is_ok());

        let component = result.unwrap();
        match component {
            PipelineComponent::Metadata(metadata) => {
                assert_eq!(metadata.name, "test_pipeline");
                assert_eq!(metadata.version, "1.0.0");
                assert_eq!(
                    metadata.description,
                    Some("Test pipeline description".to_string())
                );
                assert_eq!(
                    metadata.created_at,
                    Some("2025-01-15T10:00:00.000+00:00".to_string())
                );
                assert_eq!(
                    metadata.updated_at,
                    Some("2025-01-15T10:00:00.000+00:00".to_string())
                );
            }
            _ => panic!("Expected Metadata component"),
        }
    }

    #[test]
    fn test_from_yaml_query_component() {
        let yaml_input = r#"
!Query
sql: SELECT * FROM table WHERE id = {user_id}
parameters:
  - user_id
  - limit
"#;

        let result = PipelineComponent::from_yaml(yaml_input);
        assert!(result.is_ok());

        let component = result.unwrap();
        match component {
            PipelineComponent::Query(query) => {
                assert_eq!(query.sql, "SELECT * FROM table WHERE id = {user_id}");
                assert_eq!(query.parameters, vec!["user_id", "limit"]);
            }
            _ => panic!("Expected Query component"),
        }
    }

    #[test]
    fn test_from_yaml_malformed_yaml_error() {
        let yaml_input = r#"
!Request
fields:
  name:
    field_type: string
    nullable: false
    # Missing closing structure - malformed YAML
        invalid_syntax
"#;

        let result = PipelineComponent::from_yaml(yaml_input);
        assert!(result.is_err());

        let error_msg = result.unwrap_err().to_string();
        assert!(error_msg.contains("Failed to deserialize YAML"));
    }

    // ============================================================================
    // Round-trip Tests for Components that don't use field types
    // ============================================================================

    #[test]
    fn test_round_trip_metadata_component() {
        let original = ComponentMetadata {
            name: "test_pipeline".to_string(),
            version: "1.0.0".to_string(),
            description: Some("Test pipeline description".to_string()),
            created_at: Some("2025-01-15T10:00:00.000+00:00".to_string()),
            updated_at: Some("2025-01-15T10:00:00.000+00:00".to_string()),
        };
        let component = PipelineComponent::Metadata(original.clone());

        // Round trip: component → YAML → component → YAML
        let yaml1 = component.to_yaml().unwrap();
        let parsed_component = PipelineComponent::from_yaml(&yaml1).unwrap();
        let yaml2 = parsed_component.to_yaml().unwrap();

        // Both YAML outputs should be identical
        assert_eq!(yaml1, yaml2);

        // Verify the parsed component matches original
        match parsed_component {
            PipelineComponent::Metadata(metadata) => {
                assert_eq!(metadata.name, original.name);
                assert_eq!(metadata.version, original.version);
                assert_eq!(metadata.description, original.description);
                assert_eq!(metadata.created_at, original.created_at);
                assert_eq!(metadata.updated_at, original.updated_at);
            }
            _ => panic!("Expected Metadata component"),
        }
    }

    #[test]
    fn test_round_trip_query_component() {
        let original_query = QueryDefinition {
            sql: "SELECT * FROM table WHERE id = {user_id} AND status = {status}".to_string(),
            parameters: vec![
                "user_id".to_string(),
                "status".to_string(),
                "limit".to_string(),
            ],
        };
        let component = PipelineComponent::Query(original_query.clone());

        // Round trip: component → YAML → component → YAML
        let yaml1 = component.to_yaml().unwrap();
        let parsed_component = PipelineComponent::from_yaml(&yaml1).unwrap();
        let yaml2 = parsed_component.to_yaml().unwrap();

        // Both YAML outputs should be identical
        assert_eq!(yaml1, yaml2);

        // Verify the parsed component matches original
        match parsed_component {
            PipelineComponent::Query(query) => {
                assert_eq!(query.sql, original_query.sql);
                assert_eq!(query.parameters, original_query.parameters);
            }
            _ => panic!("Expected Query component"),
        }
    }
}
