//! The fast-path keyword guard — UX, not the security boundary.
//!
//! Screens CALLER-supplied Cypher only (the text passed to
//! `cypher_query`); engine-authored introspection (`graph_schema`'s
//! `ag_catalog` reads) never passes through it. It exists to hand an
//! agent a fast, actionable error naming the blocked keyword before any
//! network round-trip; the backend's READ ONLY transaction is what
//! actually guarantees read-only (design §Security and operational
//! bounds). Deliberately conservative: keyword-shaped string literals
//! (`RETURN 'DELETE'`) false-positive — an accepted tax. Word-boundary
//! matching keeps identifiers like `created_at` out of the blast radius.

use super::error::GraphError;

/// Mutating and escape-hatch keywords, screened on word boundaries.
/// `CALL` is here because procedures can mutate without any write
/// keyword; `LOAD` (as in `LOAD CSV`) because it makes the *graph server*
/// fetch arbitrary URLs.
const BLOCKED: &[&str] = &[
    "CREATE", "SET", "DELETE", "DETACH", "REMOVE", "MERGE", "DROP", "CALL", "LOAD",
];

/// Reject Cypher containing a blocked keyword. The error names the
/// keyword and its byte offset — never the query text. The FIRST match
/// in the query wins, not the first keyword in `BLOCKED`: the error
/// exists to hand an LLM the place to start rewriting, and
/// `DETACH DELETE` must point at `DETACH`, not at the later `DELETE`
/// that happens to sort earlier in the list.
pub fn reject_mutations(cypher: &str) -> Result<(), GraphError> {
    let upper = cypher.to_ascii_uppercase();
    let bytes = upper.as_bytes();
    let mut earliest: Option<(&'static str, usize)> = None;
    for keyword in BLOCKED {
        let mut from = 0;
        while let Some(pos) = upper[from..].find(keyword) {
            let start = from + pos;
            let end = start + keyword.len();
            let boundary_before = start == 0 || !is_word_byte(bytes[start - 1]);
            let boundary_after = end == bytes.len() || !is_word_byte(bytes[end]);
            if boundary_before && boundary_after {
                if earliest.is_none_or(|(_, best)| start < best) {
                    earliest = Some((keyword, start));
                }
                break; // later occurrences of this keyword can't be earlier
            }
            from = end;
        }
    }
    match earliest {
        Some((keyword, offset)) => Err(GraphError::MutationRejected { keyword, offset }),
        None => Ok(()),
    }
}

fn is_word_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn write_keywords_are_rejected_case_insensitively() {
        for q in [
            "CREATE (n)",
            "match (n) set n.x = 1",
            "MATCH (n) DETACH DELETE n",
            "merge (n:X)",
            "DROP GRAPH g",
            "CALL db.labels()",
            "LOAD CSV FROM 'https://x' AS row RETURN row",
        ] {
            let err = reject_mutations(q).unwrap_err();
            assert!(
                matches!(err, GraphError::MutationRejected { .. }),
                "{q}: {err}"
            );
        }
    }

    #[test]
    fn word_boundaries_spare_identifiers() {
        for q in [
            "MATCH (n) WHERE n.created_at > 0 RETURN n", // CREATE inside created_at
            "MATCH (n) RETURN n.dataset",                // SET inside dataset
            "MATCH (n) RETURN n.calls",                  // CALL inside calls
            "MATCH (n) RETURN n.reload",                 // LOAD inside reload
        ] {
            assert!(reject_mutations(q).is_ok(), "{q}");
        }
    }

    #[test]
    fn keyword_shaped_literals_false_positive_by_design() {
        // The accepted tax (design §Security): the guard is conservative
        // and the backend READ ONLY transaction is the real boundary.
        assert!(reject_mutations("MATCH (n) WHERE n.op = 'DELETE' RETURN n").is_err());
    }

    #[test]
    fn the_error_names_keyword_and_offset_not_the_query() {
        let err = reject_mutations("MATCH (n) SET n.secret = 'v'").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("'SET'"), "{msg}");
        assert!(!msg.contains("secret"), "query text never leaks: {msg}");
    }

    #[test]
    fn the_first_keyword_in_the_query_wins_not_the_first_in_the_list() {
        // DETACH sits earlier in the text, DELETE earlier in BLOCKED —
        // the LLM's rewrite anchor is the text position.
        let err = reject_mutations("MATCH (n) DETACH DELETE n").unwrap_err();
        let GraphError::MutationRejected { keyword, offset } = err else {
            panic!("mutation");
        };
        assert_eq!(keyword, "DETACH");
        assert_eq!(offset, 10);
    }
}
