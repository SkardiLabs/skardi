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

/// Parse `input` into terms, each paired with whether a bare `or` came
/// immediately before it — the point at which a new OR alternative starts.
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
        let after_or = pending_or;
        pending_or = false;
        out.push((Term { negated, text }, after_or));
    }

    out
}

/// One OR-separated alternative: the terms that must all match, and the terms
/// excluded from them.
#[derive(Default)]
struct Branch {
    positives: Vec<String>,
    negatives: Vec<String>,
}

impl Branch {
    /// This alternative as an FTS5 expression, or `None` when there is no
    /// positive side for the exclusions to be subtracted from.
    ///
    /// FTS5 binds `NOT` tighter than `AND` and `AND` tighter than `OR`. Only
    /// the subtracted side truly needs parentheses: unparenthesised,
    /// `a NOT b OR c` offers `c` as an alternative to the whole subtraction
    /// instead of adding it to what is excluded. The positive side is
    /// parenthesised for legibility in `EXPLAIN` output and logs — `AND` is
    /// associative, so `a AND b NOT c` selects the same rows as
    /// `(a AND b) NOT c`.
    fn render(&self) -> Option<String> {
        if self.positives.is_empty() {
            return None;
        }
        let positive = self.positives.join(" AND ");
        if self.negatives.is_empty() {
            return Some(positive);
        }
        let positive = if self.positives.len() > 1 {
            format!("({positive})")
        } else {
            positive
        };
        let excluded = self.negatives.join(" OR ");
        let excluded = if self.negatives.len() > 1 {
            format!("({excluded})")
        } else {
            excluded
        };
        Some(format!("{positive} NOT {excluded}"))
    }

    /// True when [`Branch::render`] emits an operator, so the alternative
    /// needs parentheses of its own to read unambiguously beside a sibling
    /// `OR`.
    fn is_compound(&self) -> bool {
        self.positives.len() > 1 || !self.negatives.is_empty()
    }
}

/// Translate user-typed search text into an FTS5 `MATCH` expression, mirroring
/// PostgreSQL's `websearch_to_tsquery` so that `sqlite_fts` and `pg_fts` honour
/// the same contract for the same documented parameter.
///
/// The grammar it accepts:
///
/// - words separated by whitespace are ANDed;
/// - `"…"` is a phrase, and an unterminated quote runs to the end of the input;
/// - a bare `or` (any case, as `websearch_to_tsquery` also matches it)
///   separates alternatives and binds loosest of all — `a b or c` is
///   `(a AND b) OR c`, because tsquery gives `|` the lowest precedence and
///   FTS5 orders `NOT`, `AND` and `OR` the same way;
/// - a `-` against the front of a word excludes it from the alternative it
///   sits in.
///
/// Everything else is literal text. No input can produce a parse error: every
/// term is emitted as a quoted phrase, and text with nothing to tokenize
/// (`""`, `"   "`, `---`) yields `None`, meaning "no rows" — the caller must
/// not send an empty string to FTS5, which rejects it as a syntax error.
///
/// Two places where FTS5 cannot follow `websearch_to_tsquery` exactly:
///
/// - FTS5's `NOT` is binary, tsquery's `!` is unary. An exclusion is therefore
///   subtracted from the alternative it sits in, and an alternative that is
///   *only* exclusions has nothing to subtract from and is dropped: `-a or b`
///   searches for `b`, where tsquery's `!a | b` would also return everything
///   lacking `a`. Dropping it narrows the result rather than inventing a
///   positive side; a query that is only exclusions yields `None`.
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
/// assert_eq!(
///     websearch_to_fts5("gateway mode or runbook").as_deref(),
///     Some(r#"("gateway" AND "mode") OR "runbook""#)
/// );
/// assert_eq!(websearch_to_fts5("   "), None);
/// ```
pub fn websearch_to_fts5(input: &str) -> Option<String> {
    // A bare `or` opens the next alternative; every other term lands in the
    // one currently open.
    let mut branches: Vec<Branch> = vec![Branch::default()];

    for (term, after_or) in parse(input) {
        if after_or {
            branches.push(Branch::default());
        }
        // `branches` starts with one alternative and only ever grows.
        if let Some(branch) = branches.last_mut() {
            if term.negated {
                branch.negatives.push(as_phrase(&term.text));
            } else {
                branch.positives.push(as_phrase(&term.text));
            }
        }
    }

    let mut rendered: Vec<(String, bool)> = branches
        .iter()
        .filter_map(|branch| Some((branch.render()?, branch.is_compound())))
        .collect();

    match rendered.len() {
        0 => None,
        // A lone alternative is the whole expression and needs no parentheses.
        1 => rendered.pop().map(|(expr, _)| expr),
        _ => Some(
            rendered
                .into_iter()
                .map(
                    |(expr, compound)| {
                        if compound { format!("({expr})") } else { expr }
                    },
                )
                .collect::<Vec<_>>()
                .join(" OR "),
        ),
    }
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

    /// `websearch_to_tsquery` ANDs adjacent words and then turns `or` into
    /// `|`, which has the lowest precedence in tsquery — the documented
    /// `'"sad cat" or "fat rat"'` comes back as
    /// `'sad' <-> 'cat' | 'fat' <-> 'rat'`, ungrouped. So the words on either
    /// side of `or` collect into alternatives; the `or` does not join one word
    /// into the AND chain. FTS5 orders `NOT`, `AND`, `OR` the same way, so the
    /// same grouping is expressible.
    #[test]
    fn a_bare_or_separates_alternatives_and_binds_loosest() {
        assert_eq!(
            websearch_to_fts5("sync fails or stalls").as_deref(),
            Some(r#"("sync" AND "fails") OR "stalls""#)
        );
        assert_eq!(
            websearch_to_fts5("fat OR rat").as_deref(),
            Some(r#""fat" OR "rat""#)
        );
        assert_eq!(
            websearch_to_fts5("sync fails or replication stalls").as_deref(),
            Some(r#"("sync" AND "fails") OR ("replication" AND "stalls")"#)
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
        // The parentheses group what the exclusion applies to explicitly.
        // They do not change the row set — FTS5 binds NOT tighter than AND
        // and AND is associative — but the emitted expression shows up in
        // EXPLAIN output, so it should read the way it means.
        assert_eq!(
            websearch_to_fts5("sync failure -postgres").as_deref(),
            Some(r#"("sync" AND "failure") NOT "postgres""#)
        );
    }

    #[test]
    fn several_exclusions_are_ored_on_the_subtracted_side() {
        // These parentheses are load-bearing: OR binds looser than NOT, so an
        // unparenthesised `a NOT b OR c` returns everything matching c as
        // well, instead of excluding both b and c.
        assert_eq!(
            websearch_to_fts5("sync -postgres -mysql").as_deref(),
            Some(r#""sync" NOT ("postgres" OR "mysql")"#)
        );
    }

    /// tsquery binds `!` tightest and `|` loosest, so `a or b -c` is
    /// `a | (b & !c)`: the exclusion belongs to one alternative, not to the
    /// whole query.
    #[test]
    fn an_exclusion_applies_only_to_the_alternative_it_sits_in() {
        assert_eq!(
            websearch_to_fts5("gateway or runbook -policy").as_deref(),
            Some(r#""gateway" OR ("runbook" NOT "policy")"#)
        );
        assert_eq!(
            websearch_to_fts5("gateway -mode or runbook").as_deref(),
            Some(r#"("gateway" NOT "mode") OR "runbook""#)
        );
    }

    #[test]
    fn an_alternative_that_is_only_exclusions_is_dropped() {
        // tsquery's `!a | b` returns everything lacking `a` as well, but
        // FTS5's NOT is binary and has no unary spelling. Dropping the
        // alternative narrows the result; inventing a positive side for it
        // would answer a different question.
        assert_eq!(
            websearch_to_fts5("-postgres or sync").as_deref(),
            Some(r#""sync""#)
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
