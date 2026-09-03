//! Frontmatter: locate the `---` block, parse it to JSON, and lift the three
//! things the tables need from it — `aliases`, `tags`/`tag`, and `[[…]]`
//! links inside string values.

use serde_json::{Map, Value as Json};
use serde_yaml::Value as Yaml;

use super::markdown::{RawLink, find_wikilinks};

/// A note split into its frontmatter YAML (without the fences) and body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Split<'a> {
    /// `None` when line 1 is not exactly `---` or the block is never closed.
    pub yaml: Option<&'a str>,
    /// Everything after the closing fence line; the whole text otherwise.
    pub body: &'a str,
    /// 1-based source line on which `body` begins (closing fence line + 1;
    /// 1 when there is no block).
    pub body_first_line: u32,
}

/// Recognize a frontmatter block: line 1 exactly `---`, closed by a later
/// line exactly `---` or `...`. CRLF line endings and a leading BOM are
/// tolerated; anything else (trailing spaces on a fence, `----`) is body.
pub fn split(text: &str) -> Split<'_> {
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    let no_block = Split {
        yaml: None,
        body: text,
        body_first_line: 1,
    };
    let mut lines = text.split_inclusive('\n');
    let Some(first) = lines.next() else {
        return no_block;
    };
    if first.trim_end_matches(['\r', '\n']) != "---" {
        return no_block;
    }
    let yaml_start = first.len();
    let mut offset = yaml_start;
    let mut line_no: u32 = 1;
    for line in lines {
        line_no += 1;
        let fence = line.trim_end_matches(['\r', '\n']);
        if fence == "---" || fence == "..." {
            return Split {
                yaml: Some(&text[yaml_start..offset]),
                body: &text[offset + line.len()..],
                body_first_line: line_no + 1,
            };
        }
        offset += line.len();
    }
    no_block
}

/// Parse a frontmatter block to a JSON object. Empty/null YAML is `{}`; a
/// non-mapping document is an error; YAML-only features are stringified
/// (non-string keys) or unwrapped (tags). The error string is the parser's
/// message, which carries line/column.
pub fn parse(yaml: &str) -> Result<Json, String> {
    if yaml.trim().is_empty() {
        return Ok(Json::Object(Map::new()));
    }
    let value: Yaml = serde_yaml::from_str(yaml).map_err(|e| e.to_string())?;
    match yaml_to_json(value) {
        Json::Null => Ok(Json::Object(Map::new())),
        object @ Json::Object(_) => Ok(object),
        other => Err(format!(
            "frontmatter is not a mapping (found {})",
            json_kind(&other)
        )),
    }
}

fn json_kind(value: &Json) -> &'static str {
    match value {
        Json::Null => "null",
        Json::Bool(_) => "boolean",
        Json::Number(_) => "number",
        Json::String(_) => "string",
        Json::Array(_) => "sequence",
        Json::Object(_) => "mapping",
    }
}

fn yaml_to_json(value: Yaml) -> Json {
    match value {
        Yaml::Null => Json::Null,
        Yaml::Bool(b) => Json::Bool(b),
        Yaml::Number(n) => {
            if let Some(i) = n.as_i64() {
                Json::from(i)
            } else if let Some(u) = n.as_u64() {
                Json::from(u)
            } else {
                n.as_f64()
                    .and_then(serde_json::Number::from_f64)
                    .map(Json::Number)
                    .unwrap_or_else(|| Json::String(n.to_string()))
            }
        }
        Yaml::String(s) => Json::String(s),
        Yaml::Sequence(items) => Json::Array(items.into_iter().map(yaml_to_json).collect()),
        Yaml::Mapping(map) => {
            let mut out = Map::new();
            for (key, val) in map {
                out.insert(key_to_string(key), yaml_to_json(val));
            }
            Json::Object(out)
        }
        Yaml::Tagged(tagged) => yaml_to_json(tagged.value),
    }
}

/// JSON keys must be strings; YAML allows anything.
fn key_to_string(key: Yaml) -> String {
    match key {
        Yaml::String(s) => s,
        Yaml::Null => "null".to_string(),
        Yaml::Bool(b) => b.to_string(),
        Yaml::Number(n) => n.to_string(),
        other => serde_yaml::to_string(&other)
            .map(|s| s.trim_end().to_string())
            .unwrap_or_default(),
    }
}

/// `aliases:` — a string becomes a one-element list; a list keeps its string
/// items (trimmed, empties dropped); any other shape is `None`.
pub fn aliases(frontmatter: &Json) -> Option<Vec<String>> {
    match frontmatter.get("aliases")? {
        Json::String(s) => Some(vec![s.trim().to_string()]),
        Json::Array(items) => Some(
            items
                .iter()
                .filter_map(Json::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
        ),
        _ => None,
    }
}

/// `tags:` and `tag:` — a list of strings, or one string split on commas and
/// whitespace. Leading `#` stripped, duplicates collapsed, document order.
pub fn tags(frontmatter: &Json) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for key in ["tags", "tag"] {
        match frontmatter.get(key) {
            Some(Json::String(s)) => {
                for part in s.split(|c: char| c == ',' || c.is_whitespace()) {
                    push_tag(&mut out, part);
                }
            }
            Some(Json::Array(items)) => {
                for item in items.iter().filter_map(Json::as_str) {
                    push_tag(&mut out, item);
                }
            }
            _ => {}
        }
    }
    out
}

fn push_tag(out: &mut Vec<String>, raw: &str) {
    let tag = raw.trim().trim_start_matches('#');
    if tag.is_empty() || out.iter().any(|t| t == tag) {
        return;
    }
    out.push(tag.to_string());
}

/// Every `[[…]]` in every string value — top-level scalars, list elements,
/// and strings inside nested maps, in document order. Only the wikilink
/// syntax counts (Obsidian's rule for properties); `[text](target)` is text.
pub fn links(frontmatter: &Json) -> Vec<RawLink> {
    let mut out = Vec::new();
    walk_strings(frontmatter, &mut |s| {
        out.extend(find_wikilinks(s).into_iter().map(|(_, link)| link));
    });
    out
}

fn walk_strings<'a>(value: &'a Json, visit: &mut dyn FnMut(&'a str)) {
    match value {
        Json::String(s) => visit(s),
        Json::Array(items) => items.iter().for_each(|v| walk_strings(v, visit)),
        Json::Object(map) => map.values().for_each(|v| walk_strings(v, visit)),
        Json::Null | Json::Bool(_) | Json::Number(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn split_recognizes_a_block_only_at_line_one() {
        let s = split("---\ntitle: x\ntags: [a]\n---\nbody line\n");
        assert_eq!(s.yaml, Some("title: x\ntags: [a]\n"));
        assert_eq!(s.body, "body line\n");
        assert_eq!(s.body_first_line, 5);

        let s = split("intro\n---\nnot: frontmatter\n---\n");
        assert_eq!(s.yaml, None);
        assert_eq!(s.body_first_line, 1);
        assert!(s.body.starts_with("intro"));

        let s = split("no block at all");
        assert_eq!(
            s,
            Split {
                yaml: None,
                body: "no block at all",
                body_first_line: 1
            }
        );
    }

    #[test]
    fn split_accepts_dots_terminator_crlf_bom_and_empty_block() {
        let s = split("---\na: 1\n...\nbody");
        assert_eq!(s.yaml, Some("a: 1\n"));
        assert_eq!(s.body, "body");
        assert_eq!(s.body_first_line, 4);

        let s = split("---\r\na: 1\r\n---\r\nbody\r\n");
        assert_eq!(s.yaml, Some("a: 1\r\n"));
        assert_eq!(s.body, "body\r\n");
        assert_eq!(s.body_first_line, 4);

        let s = split("\u{feff}---\na: 1\n---\nbody");
        assert_eq!(s.yaml, Some("a: 1\n"));
        assert_eq!(s.body, "body");

        let s = split("---\n---\nbody");
        assert_eq!(s.yaml, Some(""));
        assert_eq!(s.body, "body");
        assert_eq!(s.body_first_line, 3);
    }

    #[test]
    fn split_rejects_unterminated_and_inexact_fences() {
        let s = split("---\na: 1\nbody without closing fence");
        assert_eq!(s.yaml, None);
        assert_eq!(s.body_first_line, 1);
        // "--- " (trailing space) is not exactly "---".
        let s = split("--- \na: 1\n---\nbody");
        assert_eq!(s.yaml, None);
        // A `---` at line 1 followed by a `----` is not closed either.
        let s = split("---\na: 1\n----\nbody");
        assert_eq!(s.yaml, None);
    }

    #[test]
    fn parse_preserves_order_and_stringifies_odd_keys() {
        let v = parse("zeta: 1\nalpha: [x, y]\nnested:\n  room: B12\n1: one\ntrue: yes\n").unwrap();
        assert_eq!(
            serde_json::to_string(&v).unwrap(),
            r#"{"zeta":1,"alpha":["x","y"],"nested":{"room":"B12"},"1":"one","true":"yes"}"#
        );
    }

    #[test]
    fn parse_handles_empty_null_tagged_and_floats() {
        assert_eq!(parse("").unwrap(), json!({}));
        assert_eq!(parse("   \n").unwrap(), json!({}));
        assert_eq!(parse("k: !custom value").unwrap(), json!({"k": "value"}));
        assert_eq!(
            parse("f: 1.5\nn: -3\nb: false\nz: ~").unwrap(),
            json!({"f": 1.5, "n": -3, "b": false, "z": null})
        );
    }

    #[test]
    fn parse_reports_malformed_and_non_mapping() {
        let err = parse("title: [unclosed").unwrap_err();
        assert!(err.contains("line"), "should carry a position: {err}");
        let err = parse("- a\n- b").unwrap_err();
        assert!(err.contains("not a mapping"), "{err}");
        let err = parse("just a scalar").unwrap_err();
        assert!(err.contains("not a mapping"), "{err}");
    }

    #[test]
    fn aliases_scalar_list_or_null() {
        assert_eq!(
            aliases(&json!({"aliases": "Standup"})),
            Some(vec!["Standup".to_string()])
        );
        assert_eq!(
            aliases(&json!({"aliases": ["Start", " Landing ", 7]})),
            Some(vec!["Start".to_string(), "Landing".to_string()])
        );
        assert_eq!(aliases(&json!({"aliases": 42})), None);
        assert_eq!(aliases(&json!({"aliases": {"a": 1}})), None);
        assert_eq!(aliases(&json!({"title": "x"})), None);
    }

    #[test]
    fn tags_from_list_string_and_tag_key() {
        assert_eq!(
            tags(&json!({"tags": ["index", "project/skardi"]})),
            vec!["index", "project/skardi"]
        );
        assert_eq!(
            tags(&json!({"tags": "draft, design"})),
            vec!["draft", "design"]
        );
        assert_eq!(tags(&json!({"tags": "#a  b\tc"})), vec!["a", "b", "c"]);
        assert_eq!(tags(&json!({"tag": "solo"})), vec!["solo"]);
        // Both keys contribute; duplicates collapse; non-strings are ignored.
        assert_eq!(
            tags(&json!({"tags": ["a", "#b", 3], "tag": "b, c"})),
            vec!["a", "b", "c"]
        );
        assert!(tags(&json!({"title": "x"})).is_empty());
        assert!(tags(&json!({"tags": ["", "#"]})).is_empty());
    }

    #[test]
    fn links_walk_every_string_in_document_order() {
        let fm = parse(
            "related: \"[[Projects/Design]]\"\n\
             attendees:\n  - \"[[People/Alice]]\"\n  - \"[[People/Bob|Bob]]\"\n\
             location:\n  room: \"[[Rooms/B12#Layout|Room]]\"\n\
             raw: [[Home]]\n\
             md: \"[Home](Home.md)\"\n\
             aliases: [\"[[Alias Link]]\"]\n",
        )
        .unwrap();
        let got = links(&fm);
        let targets: Vec<&str> = got.iter().map(|l| l.target.as_str()).collect();
        // `raw: [[Home]]` is a nested YAML list containing "Home" — no link,
        // as in Obsidian; `md:` is plain text in a property.
        assert_eq!(
            targets,
            vec![
                "Projects/Design",
                "People/Alice",
                "People/Bob",
                "Rooms/B12",
                "Alias Link"
            ]
        );
        assert_eq!(got[2].display_text.as_deref(), Some("Bob"));
        assert_eq!(got[3].heading.as_deref(), Some("Layout"));
        assert_eq!(got[3].display_text.as_deref(), Some("Room"));
        assert!(got.iter().all(|l| l.line.is_none()));
    }
}
