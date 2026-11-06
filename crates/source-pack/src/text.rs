//! Text bounding shared by the providers' diagnostic strings.

/// Bound `text` to `max_chars` *characters*, cutting on a char boundary so a
/// multi-byte sequence is never split.
///
/// A length bound only: nothing here removes content, so what may appear in a
/// string this bounds is decided by which strings are passed in, not by this.
/// No ellipsis is appended — the callers store or log the result where a
/// marker would be noise, and a bound hit is visible from the length itself.
pub fn truncate_chars(text: &str, max_chars: usize) -> String {
    match text.char_indices().nth(max_chars) {
        Some((byte_index, _)) => text[..byte_index].to_string(),
        None => text.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_chars_bounds_length_on_char_boundaries() {
        // Multi-byte characters count as one char each; the cut never
        // lands inside a UTF-8 sequence.
        let text = "héllo wörld";
        assert_eq!(truncate_chars(text, 4), "héll");
        assert_eq!(truncate_chars(text, 0), "");
        assert_eq!(truncate_chars(text, 100), text);
        assert_eq!(truncate_chars("", 8), "");
    }
}
