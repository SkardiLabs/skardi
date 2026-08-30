//! Projection of the enriched pipeline inventory into MCP tool definitions.
//! Pure functions — everything here is unit-testable without a server.

use std::collections::{HashMap, HashSet};

use rmcp::model::{JsonObject, Tool};
use serde_json::{Value, json};

/// The built-in tool names. `builtin_tools()` and the bridge's dispatch
/// match use these same constants, and `RESERVED_NAMES` is built from them,
/// so the three sites cannot drift apart.
pub(crate) const QUERY: &str = "query";
pub(crate) const LIST_DATA_SOURCES: &str = "list_data_sources";

/// Tool names claimed by the built-ins; a pipeline sanitizing to one of
/// these is renamed with a `_pipeline` suffix.
pub(crate) const RESERVED_NAMES: [&str; 2] = [QUERY, LIST_DATA_SOURCES];

/// MCP clients commonly enforce `^[a-zA-Z0-9_-]{1,64}$` for tool names.
const MAX_TOOL_NAME: usize = 64;

/// Replace every char outside `[a-zA-Z0-9_-]` with `_` and truncate to 64.
/// Every kept char is ASCII, so char count == byte count downstream.
fn sanitize(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(MAX_TOOL_NAME)
        .collect()
}

/// Assign unique tool names in sorted order of ORIGINAL pipeline name.
///
/// The taken set starts with the reserved built-ins; an empty sanitized
/// candidate (the server does not reject `name: ""` at load time) falls
/// back to `pipeline` so the listing never carries a name below the
/// 1-char MCP minimum; a candidate equal to a reserved name is renamed
/// with `_pipeline` (stderr warning); then the collision pass appends
/// `_2`, `_3`, ... re-truncating the base so base + suffix never exceeds
/// 64, iterating until unique against every name already assigned
/// (reserved and previously suffixed ones included).
fn assign_tool_names(original_names: &[&str]) -> Vec<(String, String)> {
    let mut sorted: Vec<&str> = original_names.to_vec();
    sorted.sort_unstable();
    let mut taken: HashSet<String> = RESERVED_NAMES.iter().map(|s| s.to_string()).collect();
    let mut assigned = Vec::with_capacity(sorted.len());
    for original in sorted {
        let mut candidate = sanitize(original);
        if candidate.is_empty() {
            eprintln!(
                "warning: pipeline name '{original}' sanitizes to an empty MCP tool name; exposing it as `pipeline`"
            );
            candidate = "pipeline".to_string();
        }
        if RESERVED_NAMES.contains(&candidate.as_str()) {
            eprintln!(
                "warning: pipeline '{original}' collides with the built-in `{candidate}` tool; exposing it as `{candidate}_pipeline`"
            );
            candidate = format!("{candidate}_pipeline");
            candidate.truncate(MAX_TOOL_NAME);
        }
        if taken.contains(&candidate) {
            let mut n = 2usize;
            loop {
                let suffix = format!("_{n}");
                let mut base = candidate.clone();
                base.truncate(MAX_TOOL_NAME - suffix.len());
                let renamed = format!("{base}{suffix}");
                if !taken.contains(&renamed) {
                    candidate = renamed;
                    break;
                }
                n += 1;
            }
        }
        taken.insert(candidate.clone());
        assigned.push((candidate, original.to_string()));
    }
    assigned
}

fn object_schema(properties: Value, required: Vec<String>) -> JsonObject {
    serde_json::from_value(json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false
    }))
    .expect("assembled schema is a JSON object")
}

/// One pipeline entry from the enriched inventory → one MCP tool. Every
/// parameter is required (key presence is the only thing the server
/// validates; explicit JSON `null` is accepted per the null unions), and
/// the object is closed because the server rejects unsupported keys.
fn pipeline_tool(tool_name: &str, pipeline_name: &str, entry: &Value) -> Tool {
    // The original pipeline name is echoed so the model can correlate with
    // server-side errors even after sanitization/suffixing renamed the tool.
    let description = match entry["description"].as_str() {
        Some(d) if !d.trim().is_empty() => format!("{d} (pipeline `{pipeline_name}`)"),
        _ => format!("Execute pipeline `{pipeline_name}`"),
    };
    // No `parameters` key means a server predating the enriched inventory
    // (the bridge may front a remote deployment, so version skew is a
    // supported state). Publish an OPEN schema rather than a closed empty
    // one: the model's attempt then reaches the server, whose error names
    // the missing parameters — degraded but usable. A present-but-empty
    // `parameters: []` is a real zero-parameter pipeline and keeps the
    // closed empty schema below.
    let Some(params) = entry.get("parameters").and_then(Value::as_array) else {
        eprintln!(
            "warning: pipeline '{pipeline_name}' carries no `parameters` in the inventory \
             (skardi-server older than the CLI?); publishing an open input schema"
        );
        let open_schema: JsonObject = serde_json::from_value(json!({
            "type": "object",
            "additionalProperties": true
        }))
        .expect("open schema is a JSON object");
        return Tool::new(tool_name.to_string(), description, open_schema);
    };
    let mut properties = serde_json::Map::new();
    let mut required = Vec::new();
    for param in params {
        if let Some(name) = param["name"].as_str() {
            // Version-skew guard: a property's schema must be an object or
            // boolean; anything else (Null from a missing key) would make
            // the whole listing invalid for strict hosts. `{}` = accept
            // anything, the server stays the validator.
            let schema = match &param["json_schema"] {
                v @ (Value::Object(_) | Value::Bool(_)) => v.clone(),
                _ => json!({}),
            };
            properties.insert(name.to_string(), schema);
            required.push(name.to_string());
        }
    }
    Tool::new(
        tool_name.to_string(),
        description,
        object_schema(Value::Object(properties), required),
    )
}

pub(crate) fn builtin_tools() -> Vec<Tool> {
    let query_schema = object_schema(
        json!({
            "sql": {"type": "string"},
            "max_rows": {
                "type": "integer",
                // The server rejects 0 ("must be a positive integer") and a
                // negative value fails its usize deserialization outright.
                "minimum": 1,
                "description": "Result row cap; server default 1000."
            },
            "purpose": {
                "type": "string",
                "description": "One line on why you are running this query; recorded in the query audit log."
            }
        }),
        vec!["sql".to_string()],
    );
    let list_data_sources_schema = object_schema(json!({}), Vec::new());
    vec![
        Tool::new(
            QUERY,
            "Run ad-hoc SQL against Skardi's federated engine. DML is only accepted on \
             data sources configured with access_mode: read_write; DDL is always \
             rejected. Use list_data_sources first to see available tables.",
            query_schema,
        ),
        Tool::new(
            LIST_DATA_SOURCES,
            "List Skardi's data sources: tables, column schemas, and plain-English \
             semantic descriptions. Call this before writing ad-hoc SQL with `query`.",
            list_data_sources_schema,
        ),
    ]
}

/// Project the enriched `GET /pipelines` body into (tools, tool → pipeline
/// name map). The built-ins are appended after the pipeline tools; the map
/// covers pipeline tools only.
pub(crate) fn project(inventory: &Value) -> (Vec<Tool>, HashMap<String, String>) {
    let entries: HashMap<&str, &Value> = inventory["pipelines"]
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(|entry| entry["name"].as_str().map(|name| (name, entry)))
                .collect()
        })
        .unwrap_or_default();
    let originals: Vec<&str> = entries.keys().copied().collect();
    let mut tools = Vec::new();
    let mut map = HashMap::new();
    for (tool_name, pipeline_name) in assign_tool_names(&originals) {
        let entry = entries[pipeline_name.as_str()];
        tools.push(pipeline_tool(&tool_name, &pipeline_name, entry));
        map.insert(tool_name, pipeline_name);
    }
    tools.extend(builtin_tools());
    (tools, map)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inventory(pipelines: Value) -> Value {
        json!({"success": true, "pipelines": pipelines, "count": 0})
    }

    #[test]
    fn sanitizes_names_to_the_mcp_charset() {
        assert_eq!(sanitize("product-search"), "product-search");
        assert_eq!(sanitize("my.pipe/v2"), "my_pipe_v2");
        // replacement is per-char: two hanzi + one space → three underscores
        assert_eq!(sanitize("空 格"), "___");
        assert_eq!(sanitize(&"x".repeat(80)).len(), 64);
    }

    #[test]
    fn reserved_names_get_the_pipeline_suffix() {
        let (tools, map) = project(&inventory(json!([
            {"name": "query", "version": "1", "endpoint": "/query/execute",
             "description": null, "parameters": []}
        ])));
        assert_eq!(map.get("query_pipeline").map(String::as_str), Some("query"));
        assert!(tools.iter().any(|t| t.name.as_ref() == "query_pipeline"));
        // the built-in `query` is still present and distinct
        assert!(tools.iter().any(|t| t.name.as_ref() == "query"));
    }

    #[test]
    fn collisions_suffix_in_sorted_original_name_order() {
        let (_, map) = project(&inventory(json!([
            {"name": "a.b", "version": "1", "endpoint": "/a.b/execute",
             "description": null, "parameters": []},
            {"name": "a_b", "version": "1", "endpoint": "/a_b/execute",
             "description": null, "parameters": []},
            {"name": "a_b_2", "version": "1", "endpoint": "/a_b_2/execute",
             "description": null, "parameters": []}
        ])));
        // sorted originals: "a.b" < "a_b" < "a_b_2". "a.b" sanitizes to
        // "a_b" and claims it first; the literal "a_b" collides and takes
        // "_2"; the literal "a_b_2" then finds ITS candidate taken and
        // suffixes that, landing on "a_b_2_2" — it must not silently merge.
        assert_eq!(map.get("a_b").map(String::as_str), Some("a.b"));
        assert_eq!(map.get("a_b_2").map(String::as_str), Some("a_b"));
        assert_eq!(map.get("a_b_2_2").map(String::as_str), Some("a_b_2"));
        assert_eq!(map.len(), 3);
    }

    #[test]
    fn empty_name_falls_back_and_still_collides_cleanly() {
        // The server loads `name: ""` without complaint; the tool name must
        // still satisfy the 1-char minimum. Sorted originals: "" < "pipeline",
        // so the empty name claims the fallback and the literal one suffixes.
        let (tools, map) = project(&inventory(json!([
            {"name": "", "version": "1", "endpoint": "//execute",
             "description": null, "parameters": []},
            {"name": "pipeline", "version": "1", "endpoint": "/pipeline/execute",
             "description": null, "parameters": []}
        ])));
        assert_eq!(map.get("pipeline").map(String::as_str), Some(""));
        assert_eq!(map.get("pipeline_2").map(String::as_str), Some("pipeline"));
        assert!(tools.iter().all(|t| !t.name.is_empty()));
    }

    #[test]
    fn suffixes_never_push_past_64_chars() {
        let long = "x".repeat(64);
        let (_, map) = project(&inventory(json!([
            {"name": long.clone(), "version": "1", "endpoint": "/x/execute",
             "description": null, "parameters": []},
            {"name": format!("{long}y"), "version": "1", "endpoint": "/xy/execute",
             "description": null, "parameters": []}
        ])));
        assert!(map.keys().all(|k| k.len() <= 64), "keys: {:?}", map.keys());
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn description_falls_back_when_yaml_omits_it() {
        let (tools, _) = project(&inventory(json!([
            {"name": "p", "version": "1", "endpoint": "/p/execute",
             "description": null, "parameters": []}
        ])));
        let tool = tools.iter().find(|t| t.name.as_ref() == "p").unwrap();
        assert_eq!(tool.description.as_deref(), Some("Execute pipeline `p`"));
    }

    #[test]
    fn input_schema_assembles_fragments_with_required_and_closed_object() {
        let (tools, _) = project(&inventory(json!([
            {"name": "p", "version": "1", "endpoint": "/p/execute",
             "description": "Search products",
             "parameters": [
                {"name": "brand", "data_type": "Utf8",
                 "json_schema": {"type": ["string", "null"]}},
                {"name": "max_price", "data_type": "Float64",
                 "json_schema": {"type": ["number", "null"]}}
             ]}
        ])));
        let tool = tools.iter().find(|t| t.name.as_ref() == "p").unwrap();
        let schema = serde_json::to_value(tool.input_schema.as_ref()).unwrap();
        assert_eq!(
            schema,
            json!({
                "type": "object",
                "properties": {
                    "brand": {"type": ["string", "null"]},
                    "max_price": {"type": ["number", "null"]}
                },
                "required": ["brand", "max_price"],
                "additionalProperties": false
            })
        );
        // original pipeline name echoed for error correlation
        assert_eq!(
            tool.description.as_deref(),
            Some("Search products (pipeline `p`)")
        );
    }

    #[test]
    fn missing_parameters_key_publishes_an_open_schema() {
        // Version skew: a server predating the enriched inventory has no
        // `parameters` key at all. A closed empty schema would make the tool
        // silently unusable; the open schema lets attempts reach the server.
        let (tools, _) = project(&inventory(json!([
            {"name": "old", "version": "1", "endpoint": "/old/execute"}
        ])));
        let tool = tools.iter().find(|t| t.name.as_ref() == "old").unwrap();
        let schema = serde_json::to_value(tool.input_schema.as_ref()).unwrap();
        assert_eq!(
            schema,
            json!({"type": "object", "additionalProperties": true})
        );
        // an explicitly empty list is a real zero-parameter pipeline and
        // keeps the closed empty schema
        let (tools, _) = project(&inventory(json!([
            {"name": "empty", "version": "1", "endpoint": "/empty/execute",
             "description": null, "parameters": []}
        ])));
        let tool = tools.iter().find(|t| t.name.as_ref() == "empty").unwrap();
        let schema = serde_json::to_value(tool.input_schema.as_ref()).unwrap();
        assert_eq!(schema["additionalProperties"], json!(false));
        assert_eq!(schema["required"], json!([]));
    }

    #[test]
    fn missing_json_schema_falls_back_to_accept_anything() {
        // `param["json_schema"]` on an entry without the key yields Null,
        // which is not a legal property schema — strict hosts would reject
        // the whole listing. It must degrade to `{}` instead.
        let (tools, _) = project(&inventory(json!([
            {"name": "p", "version": "1", "endpoint": "/p/execute",
             "description": null,
             "parameters": [{"name": "brand", "data_type": "Utf8"}]}
        ])));
        let tool = tools.iter().find(|t| t.name.as_ref() == "p").unwrap();
        let schema = serde_json::to_value(tool.input_schema.as_ref()).unwrap();
        assert_eq!(schema["properties"]["brand"], json!({}));
        assert_eq!(schema["required"], json!(["brand"]));
    }

    #[test]
    fn builtin_tool_names_match_the_reserved_set() {
        // The reserved set seeds `taken` in assign_tool_names while project()
        // appends builtin_tools() unconditionally — a built-in missing from
        // RESERVED_NAMES would let a pipeline claim its name and the listing
        // would carry duplicates. This pins the two sets to each other.
        let tools = builtin_tools();
        let names: Vec<&str> = tools.iter().map(|t| t.name.as_ref()).collect();
        assert_eq!(names, RESERVED_NAMES);
    }

    #[test]
    fn builtins_have_the_specified_schemas() {
        let tools = builtin_tools();
        let query = tools.iter().find(|t| t.name.as_ref() == "query").unwrap();
        let schema = serde_json::to_value(query.input_schema.as_ref()).unwrap();
        assert_eq!(schema["required"], json!(["sql"]));
        assert_eq!(schema["properties"]["sql"], json!({"type": "string"}));
        assert_eq!(schema["properties"]["max_rows"]["type"], json!("integer"));
        assert_eq!(schema["properties"]["max_rows"]["minimum"], json!(1));
        assert!(schema["properties"]["purpose"].is_object());
        assert_eq!(schema["additionalProperties"], json!(false));
        let lds = tools
            .iter()
            .find(|t| t.name.as_ref() == "list_data_sources")
            .unwrap();
        let schema = serde_json::to_value(lds.input_schema.as_ref()).unwrap();
        assert_eq!(schema["type"], json!("object"));
        assert_eq!(schema["additionalProperties"], json!(false));
    }
}
