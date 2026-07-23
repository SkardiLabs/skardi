//! JSON and table rendering for command results, plus the stderr notice for
//! truncated result sets.
//!
//! stdout carries only the result data (so command output pipes cleanly into
//! `jq` and similar tools); anything else — including the truncation notice
//! — goes to stderr.

use serde_json::Value;

/// Print a command's success envelope: the result data to stdout, and (in
/// table mode) a row-count summary — plus, whenever the envelope is
/// truncated, a notice to stderr.
///
/// Default mode (`table == false`) prints exactly the envelope's `data`
/// array, pretty-printed, and nothing else, so command output pipes cleanly
/// into `jq`. Table mode prints [`render_table`]'s output followed by a
/// `<n> row(s) returned` line, where `n` prefers the envelope's `rows` field
/// and falls back to the data array's length. The envelope's `data` is
/// treated as empty when missing or not an array.
///
/// Not unit-tested here: it writes to real stdout/stderr. Covered by
/// command-level tests (Tasks 6-7) asserting it doesn't panic, and by the
/// end-to-end test.
// TODO(tasks 6, 7): consumed once subcommands issue requests and render
// their responses; remove this `allow` when that lands.
#[allow(dead_code)]
pub fn print_result(body: &Value, table: bool) {
    let empty: Vec<Value> = Vec::new();
    let rows = body.get("data").and_then(Value::as_array).unwrap_or(&empty);

    if table {
        print!("{}", render_table(rows));
        let row_count = body
            .get("rows")
            .and_then(Value::as_u64)
            .unwrap_or(rows.len() as u64);
        println!("{row_count} row(s) returned");
    } else {
        let pretty = serde_json::to_string_pretty(rows)
            .expect("Vec<Value> of already-parsed JSON always serializes");
        println!("{pretty}");
    }

    let truncated = body
        .get("truncated")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if truncated {
        eprintln!("note: results truncated; pass a higher --max-rows to see the rest");
    }
}

/// Render `rows` as a left-aligned, padded plain-text table.
///
/// Columns are the first row's keys, in that map's iteration order — since
/// the workspace `serde_json` build has no `preserve_order` feature,
/// `Value::Object` iterates keys alphabetically, so column order is
/// alphabetical and deterministic. Later rows are read by column name;
/// missing keys render as empty cells, as does explicit `null`. Strings
/// render bare (unquoted); numbers and bools use their display form; nested
/// arrays/objects render as compact JSON.
///
/// Cells are left-aligned and padded to their column's max width (header
/// included), columns are joined with `" | "`, a separator row of `-` runs
/// joined with `"-+-"` follows the header, and every emitted line is
/// trimmed of trailing whitespace. An empty `rows` renders the literal line
/// `(no rows)`.
pub fn render_table(rows: &[Value]) -> String {
    let Some(first) = rows.first().and_then(Value::as_object) else {
        return "(no rows)\n".to_string();
    };

    let columns: Vec<&str> = first.keys().map(String::as_str).collect();

    let data_rows: Vec<Vec<String>> = rows
        .iter()
        .map(|row| columns.iter().map(|col| cell_text(row.get(*col))).collect())
        .collect();

    let widths: Vec<usize> = columns
        .iter()
        .enumerate()
        .map(|(i, col)| {
            data_rows
                .iter()
                .map(|row| row[i].len())
                .max()
                .unwrap_or(0)
                .max(col.len())
        })
        .collect();

    let header: Vec<String> = columns.iter().map(|col| col.to_string()).collect();

    let mut lines = Vec::with_capacity(data_rows.len() + 2);
    lines.push(format_row(&header, &widths));
    lines.push(separator_line(&widths));
    for row in &data_rows {
        lines.push(format_row(row, &widths));
    }

    let mut rendered = lines.join("\n");
    rendered.push('\n');
    rendered
}

/// Render one JSON value as its table-cell text: empty for `null`/missing,
/// bare for strings, and display form (compact JSON for arrays/objects)
/// otherwise.
fn cell_text(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

/// Left-pad each cell to its column width and join with `" | "`, trimming
/// trailing whitespace from the result.
fn format_row(cells: &[String], widths: &[usize]) -> String {
    let padded: Vec<String> = cells
        .iter()
        .zip(widths)
        .map(|(cell, &width)| format!("{cell:<width$}"))
        .collect();
    padded.join(" | ").trim_end().to_string()
}

/// Build the header/data separator: one `-` run per column width, joined by
/// `"-+-"`.
fn separator_line(widths: &[usize]) -> String {
    widths
        .iter()
        .map(|&width| "-".repeat(width))
        .collect::<Vec<_>>()
        .join("-+-")
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::render_table;

    #[test]
    fn render_table_two_rows_mixed_widths_with_null() {
        let rows = vec![
            json!({"name": "Alice", "age": 30, "city": "NYC"}),
            json!({"name": "Bob", "age": 25, "city": null}),
        ];

        let expected = "age | city | name\n\
                         ----+------+------\n\
                         30  | NYC  | Alice\n\
                         25  |      | Bob\n";

        assert_eq!(render_table(&rows), expected);
    }

    #[test]
    fn render_table_nested_values_are_compact_json() {
        let rows = vec![json!({"arr": [1, 2], "obj": {"k": "v"}})];

        let table = render_table(&rows);

        assert!(
            table.contains("[1,2]"),
            "expected compact array JSON in table:\n{table}"
        );
        assert!(
            table.contains(r#"{"k":"v"}"#),
            "expected compact object JSON in table:\n{table}"
        );
    }

    #[test]
    fn render_table_empty_slice_is_no_rows_literal() {
        let rows: Vec<serde_json::Value> = vec![];

        assert_eq!(render_table(&rows), "(no rows)\n");
    }

    #[test]
    fn render_table_missing_key_in_later_row_renders_empty_and_trims() {
        let rows = vec![
            json!({"a": "x", "b": "y"}),
            json!({"a": "z"}), // missing "b" entirely
        ];

        let table = render_table(&rows);
        let lines: Vec<&str> = table.lines().collect();

        // header + separator + 2 data rows
        assert_eq!(lines.len(), 4);
        let second_data_row = lines[3];
        assert_eq!(second_data_row, "z |");
        assert_eq!(second_data_row, second_data_row.trim_end());
    }
}
