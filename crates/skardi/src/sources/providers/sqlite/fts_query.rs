//! Translate what a person typed into an FTS5 `MATCH` expression.
//!
//! `pg_fts` hands its `query` argument to PostgreSQL's
//! `websearch_to_tsquery`, whose whole purpose is to accept the contents of a
//! search box: apostrophes, colons and hyphens are ordinary characters to it,
//! and no user text can make it raise a parse error. `sqlite_fts` used to hand
//! the same argument straight to FTS5's `MATCH` grammar, which has no such
//! forgiving entry point — so `read-only`, `doesn't` and `note:` each turned an
//! ordinary question into an execution error naming a column the user never
//! wrote. Two backends, one documented parameter, two contracts.
//!
//! [`websearch_to_fts5`] closes that gap on the SQLite side by parsing the
//! text here and emitting an expression FTS5 can always parse.

/// One parsed search term.
struct Term {
    negated: bool,
    /// The term's literal text, before FTS5 phrase quoting.
    text: String,
}

/// True when a term carries something FTS5 could tokenize. A run of pure
/// punctuation (`---`, `:::`) has no tokens, and an empty phrase pair
/// contributes nothing to the match, so such terms are dropped rather than
/// emitted.
fn is_searchable(text: &str) -> bool {
    text.chars().any(char::is_alphanumeric)
}

/// Wrap a term as an FTS5 phrase, doubling any embedded double quote.
///
/// Quoting is what makes the translation total: inside a phrase, every
/// character except `"` is literal, so a colon cannot start a column filter, a
/// hyphen cannot split the word, and an apostrophe cannot open a string. FTS5
/// still applies the table's own tokenizer inside the phrase, so `"read-only"`
/// matches text indexed as `read` followed by `only` — the hyphenated term is
/// found, not merely tolerated.
fn as_phrase(text: &str) -> String {
    format!("\"{}\"", text.replace('"', "\"\""))
}

/// Parse `input` into terms plus the positions where the user wrote a bare
/// `or`. Returns the terms and, for each term after the first, whether an `or`
/// joined it to the previous one.
fn parse(input: &str) -> Vec<(Term, bool)> {
    let chars: Vec<char> = input.chars().collect();
    let mut out: Vec<(Term, bool)> = Vec::new();
    let mut i = 0;
    // Set by a bare `or`, consumed by the term that follows it.
    let mut pending_or = false;

    while i < chars.len() {
        if chars[i].is_whitespace() {
            i += 1;
            continue;
        }

        // A `-` directly against the start of a term negates it; a detached
        // `-` is just punctuation and falls through to the term scanner,
        // where it is dropped for having no tokens.
        let mut negated = false;
        if chars[i] == '-' && i + 1 < chars.len() && !chars[i + 1].is_whitespace() {
            negated = true;
            i += 1;
        }

        let text: String = if chars[i] == '"' {
            // A quoted phrase runs to the closing quote, or to the end of the
            // input when the user never typed one — the same tolerance
            // websearch_to_tsquery shows an unbalanced quote.
            i += 1;
            let start = i;
            while i < chars.len() && chars[i] != '"' {
                i += 1;
            }
            let phrase: String = chars[start..i].iter().collect();
            if i < chars.len() {
                i += 1; // consume the closing quote
            }
            phrase
        } else {
            let start = i;
            while i < chars.len() && !chars[i].is_whitespace() {
                i += 1;
            }
            let word: String = chars[start..i].iter().collect();
            // A bare `or` is the operator, not a term to search for. Only
            // unquoted, un-negated occurrences count, so `"or"` and `-or`
            // still search for the word.
            if !negated && word.eq_ignore_ascii_case("or") {
                pending_or = true;
                continue;
            }
            word
        };

        if !is_searchable(&text) {
            continue;
        }
        // A negated term never participates in an OR group; the `or` the user
        // typed beside it is dropped rather than silently changing what the
        // negation applies to.
        let joined_by_or = pending_or && !negated && !out.is_empty();
        pending_or = false;
        out.push((Term { negated, text }, joined_by_or));
    }

    out
}

/// Translate user-typed search text into an FTS5 `MATCH` expression, mirroring
/// PostgreSQL's `websearch_to_tsquery` so that `sqlite_fts` and `pg_fts` honour
/// the same contract for the same documented parameter.
///
/// The grammar it accepts:
///
/// - words separated by whitespace are ANDed;
/// - `"…"` is a phrase, and an unterminated quote runs to the end of the input;
/// - a bare `or` (any case) between two words makes them alternatives, binding
///   tighter than the implicit AND — `a b or c` is `a AND (b OR c)`;
/// - a `-` against the front of a word excludes it.
///
/// Everything else is literal text. No input can produce a parse error: every
/// term is emitted as a quoted phrase, and text with nothing to tokenize
/// (`""`, `"   "`, `---`) yields `None`, meaning "no rows" — the caller must
/// not send an empty string to FTS5, which rejects it as a syntax error.
///
/// Two places where FTS5 cannot follow `websearch_to_tsquery` exactly:
///
/// - FTS5's `NOT` is binary, so exclusions are subtracted from the positive
///   side as `(positives) NOT (a OR b)`, and the positive side is parenthesised
///   because `NOT` binds tighter than `AND` there. A query that is *only*
///   exclusions has nothing to subtract from and yields `None` rather than an
///   invented positive side.
/// - Whether a term matches at all is still the tokenizer's call. Under
///   `tokenize='trigram'` a one- or two-character term cannot match through the
///   index, exactly as before this translation.
///
/// # Examples
///
/// ```
/// # use skardi::sources::providers::sqlite::websearch_to_fts5;
/// assert_eq!(
///     websearch_to_fts5("read-only source").as_deref(),
///     Some(r#""read-only" AND "source""#)
/// );
/// assert_eq!(
///     websearch_to_fts5("what's the retry policy").as_deref(),
///     Some(r#""what's" AND "the" AND "retry" AND "policy""#)
/// );
/// assert_eq!(websearch_to_fts5("   "), None);
/// ```
pub fn websearch_to_fts5(input: &str) -> Option<String> {
    let terms = parse(input);

    // Positives become AND-separated groups; an `or` merges a term into the
    // group its predecessor is in.
    let mut groups: Vec<Vec<String>> = Vec::new();
    let mut negatives: Vec<String> = Vec::new();

    for (term, joined_by_or) in terms {
        if term.negated {
            negatives.push(as_phrase(&term.text));
        } else if joined_by_or && !groups.is_empty() {
            let last = groups.len() - 1;
            groups[last].push(as_phrase(&term.text));
        } else {
            groups.push(vec![as_phrase(&term.text)]);
        }
    }

    if groups.is_empty() {
        return None;
    }

    let positive = groups
        .iter()
        .map(|group| {
            if group.len() == 1 {
                group[0].clone()
            } else {
                format!("({})", group.join(" OR "))
            }
        })
        .collect::<Vec<_>>()
        .join(" AND ");

    if negatives.is_empty() {
        return Some(positive);
    }

    let excluded = negatives.join(" OR ");
    Some(format!("({positive}) NOT ({excluded})"))
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Every one of these returned HTTP 500 before the translation existed
    /// (measured 2026-09-04 against a live workspace, issue
    /// SkardiLabs/skardi-skills#39). Each character that used to be read as
    /// syntax — apostrophe, colon, hyphen — now arrives inside a phrase.
    /// `fts_table_function`'s integration tests prove FTS5 accepts these and
    /// that they still find the rows.
    #[test]
    fn ordinary_english_questions_translate() {
        let cases = [
            (
                "why doesn't the sync work",
                r#""why" AND "doesn't" AND "the" AND "sync" AND "work""#,
            ),
            (
                "it's returning nothing",
                r#""it's" AND "returning" AND "nothing""#,
            ),
            (
                "note: check the gateway",
                r#""note:" AND "check" AND "the" AND "gateway""#,
            ),
            ("full-text search", r#""full-text" AND "search""#),
            ("read-only mode", r#""read-only" AND "mode""#),
        ];
        for (input, expected) in cases {
            assert_eq!(
                websearch_to_fts5(input).as_deref(),
                Some(expected),
                "{input:?}"
            );
        }
    }

    #[test]
    fn words_are_anded_and_each_is_a_phrase() {
        assert_eq!(
            websearch_to_fts5("read-only source").as_deref(),
            Some(r#""read-only" AND "source""#)
        );
    }

    #[test]
    fn a_quoted_phrase_stays_one_term() {
        assert_eq!(
            websearch_to_fts5(r#""retry backoff" policy"#).as_deref(),
            Some(r#""retry backoff" AND "policy""#)
        );
    }

    #[test]
    fn an_unterminated_quote_runs_to_the_end() {
        assert_eq!(
            websearch_to_fts5(r#""retry backoff"#).as_deref(),
            Some(r#""retry backoff""#)
        );
    }

    #[test]
    fn a_double_quote_inside_a_word_is_doubled_not_left_to_close_the_phrase() {
        // `"` is the one character a phrase cannot hold literally. Without the
        // doubling this closes the phrase early and hands the rest of the term
        // back to the FTS5 parser as syntax.
        assert_eq!(
            websearch_to_fts5(r#"say"hi"#).as_deref(),
            Some(r#""say""hi""#)
        );
    }

    #[test]
    fn quotes_around_words_open_phrases_rather_than_being_searched_for() {
        assert_eq!(
            websearch_to_fts5(r#"say "hi" loudly"#).as_deref(),
            Some(r#""say" AND "hi" AND "loudly""#)
        );
    }

    #[test]
    fn a_bare_or_binds_tighter_than_the_implicit_and() {
        assert_eq!(
            websearch_to_fts5("sync fails or stalls").as_deref(),
            Some(r#""sync" AND ("fails" OR "stalls")"#)
        );
        assert_eq!(
            websearch_to_fts5("fat OR rat").as_deref(),
            Some(r#"("fat" OR "rat")"#)
        );
    }

    #[test]
    fn a_quoted_or_is_a_search_term_not_an_operator() {
        assert_eq!(
            websearch_to_fts5(r#"fat "or" rat"#).as_deref(),
            Some(r#""fat" AND "or" AND "rat""#)
        );
    }

    #[test]
    fn a_leading_hyphen_excludes_and_the_positive_side_is_parenthesised() {
        // FTS5's NOT binds tighter than AND, so an unparenthesised
        // `a AND b NOT c` would subtract c from b alone.
        assert_eq!(
            websearch_to_fts5("sync failure -postgres").as_deref(),
            Some(r#"("sync" AND "failure") NOT ("postgres")"#)
        );
    }

    #[test]
    fn several_exclusions_are_ored_on_the_subtracted_side() {
        assert_eq!(
            websearch_to_fts5("sync -postgres -mysql").as_deref(),
            Some(r#"("sync") NOT ("postgres" OR "mysql")"#)
        );
    }

    #[test]
    fn a_hyphen_inside_a_word_is_not_an_exclusion() {
        assert_eq!(
            websearch_to_fts5("read-only").as_deref(),
            Some(r#""read-only""#)
        );
    }

    #[test]
    fn a_detached_hyphen_is_dropped_as_punctuation() {
        assert_eq!(
            websearch_to_fts5("sync - failure").as_deref(),
            Some(r#""sync" AND "failure""#)
        );
    }

    /// FTS5 rejects an empty match string outright, so "nothing to search for"
    /// has to be answerable without asking it. Each of these used to reach
    /// FTS5 and produce a 500, or — for the empty string — a 200 carrying rows
    /// that had nothing to do with any question.
    #[test]
    fn text_with_nothing_to_tokenize_yields_no_expression() {
        for input in ["", "   ", "---", ":::", r#""""#, r#""   ""#] {
            assert_eq!(websearch_to_fts5(input), None, "input {input:?}");
        }
    }

    #[test]
    fn exclusions_alone_yield_no_expression() {
        // FTS5's NOT is binary: there is no positive side to subtract from,
        // and inventing one would answer a different question.
        assert_eq!(websearch_to_fts5("-postgres"), None);
        assert_eq!(websearch_to_fts5("-postgres -mysql"), None);
    }

    #[test]
    fn cjk_terms_survive_unchanged_inside_the_phrase() {
        // #35 made CJK full-text search work by rebuilding the index with
        // tokenize='trigram'. Quoting must not undo it: the term reaches the
        // tokenizer exactly as typed.
        assert_eq!(
            websearch_to_fts5("上下文 检索").as_deref(),
            Some(r#""上下文" AND "检索""#)
        );
    }
}
