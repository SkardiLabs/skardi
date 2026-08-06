//! OPML subscription-list reader, and the point where [`RssConfig`]'s two
//! mutually exclusive input forms converge into one resolved subscription
//! list.
//!
//! [`RssConfig::validate`] is deliberately zero-I/O, so it can check that
//! an `opml:` path is *present* but never what is inside it. Once this
//! module actually reads that file — on the registration path, not the
//! validate path — every check `validate()` already performs for the
//! inline `feeds:` form has to be re-run against whatever the file
//! contains: non-emptiness, name defaulting, effective-name uniqueness, and
//! http/https scheme validation were all invisible before now. That re-run
//! is `config::finalize` — the same single implementation `validate()`
//! itself calls for the inline form — applied here to both input forms, so
//! there is exactly one place either one's invariants are enforced.
//!
//! Gated behind the `rss` feature: this is the only file in the provider
//! that links an XML parser (`quick-xml`) today. A later task adds
//! `feed-rs` (whose own XML backend is also `quick-xml`) for parsing feed
//! bodies themselves — see the dependency comment in `Cargo.toml` for how
//! the two crate versions are kept from diverging.

use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use quick_xml::events::Event;
use quick_xml::{Reader, XmlVersion};

use super::ResolvedSubscription;
use super::config::{RssConfig, finalize};
use super::error::RssError;

/// Byte cap on an OPML file, enforced by [`read_opml`] *while* reading — the
/// file handle is `take`n at this bound, never read to EOF first. The
/// configured path is operator-controlled but otherwise arbitrary: without
/// the cap, a huge (regular) file is an unbounded allocation. 1 MiB is
/// far above any real subscription list — an OPML outline runs ~100 bytes,
/// so this admits on the order of ten thousand feeds.
///
/// One of three independent bounds `read_opml` places on the read; see its
/// doc for how this, the regular-file check, and [`OPML_READ_TIMEOUT`]
/// divide the failure modes between them.
pub(crate) const MAX_OPML_BYTES: u64 = 1024 * 1024;

/// Wall-clock bound on one OPML read, enforced by [`read_opml`] via
/// [`read_bytes_bounded`]. [`MAX_OPML_BYTES`] bounds how *much* is read,
/// not how *long* reading takes: a FIFO whose writer never sends enough
/// bytes (or never closes) blocks a bounded read forever without ever
/// touching the byte cap. The regular-file check catches a FIFO that is
/// already at the configured path, but only this timeout covers the
/// race where the path changes between that check and the open, or a
/// regular file on storage that has stopped answering (a wedged network
/// mount). 5s is three orders of magnitude above what a 1 MiB local read
/// needs, so it can only fire when something is genuinely wrong.
pub(crate) const OPML_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Resolve `config`'s inline `feeds:` list or `opml:` file into one flat,
/// ready-to-fetch subscription list.
///
/// `name` is the data source's actual name; it attributes the one error
/// only this path can hit, a file that cannot be read
/// ([`RssError::OpmlUnreadable`]). Config-*content* errors stay nameless
/// ([`RssError::InvalidConfig`] — see its doc for why) and are named by
/// whichever caller wraps them.
///
/// Every later stage of the provider (fetcher, TTL cache, freshness engine,
/// per-feed partitions) consumes only the [`ResolvedSubscription`]s this
/// returns and never looks at `RssConfig`'s input shape again.
///
/// Blocking, but boundedly so: the `opml:` form refuses non-regular files,
/// then performs synchronous file I/O capped at [`MAX_OPML_BYTES`] and
/// [`OPML_READ_TIMEOUT`] — never a read to EOF, never an unbounded wait.
/// Callers on an async runtime (registration, when a later task adds it)
/// should still wrap the call in `spawn_blocking`, as with any filesystem
/// touch.
pub fn resolve_subscriptions(
    name: &str,
    config: &RssConfig,
) -> Result<Vec<ResolvedSubscription>, RssError> {
    let raw = if let Some(path) = &config.opml {
        read_opml(name, path)?
    } else if let Some(feeds) = &config.feeds {
        feeds
            .iter()
            .map(|feed| (feed.url.clone(), feed.name.clone()))
            .collect()
    } else {
        // `RssConfig::validate` rejects this shape long before a source is
        // registered; kept as a returned error rather than a panic in case
        // this function is ever reached without validation having run.
        return Err(RssError::InvalidConfig {
            reason: "exactly one of `feeds` or `opml` must be set".to_string(),
        });
    };

    finalize(raw)
}

/// Read an OPML file into flat `(url, name)` pairs, one per `<outline
/// xmlUrl="…">`.
///
/// OPML lets subscriptions nest inside grouping outlines — an untyped,
/// folder-like `<outline text="Tech">` wrapping its feeds as children.
/// quick-xml's event stream is flat regardless of nesting depth, so no
/// recursion is needed: every `outline` start/empty tag is visited once, in
/// document order, and only those carrying an `xmlUrl` become a
/// subscription. A grouping outline with no `xmlUrl` of its own — like the
/// "Tech" folder above — is silently skipped; it is structure, not a feed.
fn read_opml(name: &str, path: &Path) -> Result<Vec<(String, Option<String>)>, RssError> {
    let unreadable = |reason: String| RssError::OpmlUnreadable {
        name: name.to_string(),
        path: path.display().to_string(),
        reason,
    };

    // Only regular files are read at all. A FIFO or device file is not a
    // subscription list under any encoding, and several of them defeat the
    // byte cap below by blocking rather than delivering bytes — a FIFO with
    // no writer blocks at open(), one with an idle writer blocks mid-read.
    // `fs::metadata` follows symlinks, so a symlink *to* a regular file
    // still registers fine; a symlink to anything else is refused like the
    // thing it points at.
    let meta = fs::metadata(path).map_err(|e| unreadable(e.to_string()))?;
    if !meta.is_file() {
        return Err(unreadable(
            "not a regular file (FIFOs and device files are refused)".to_string(),
        ));
    }

    // Bounded read: take one byte past the cap so "exactly at the cap" and
    // "over it" are distinguishable, without ever reading further than that —
    // see MAX_OPML_BYTES for why reading to EOF first is not an option. The
    // wall-clock bound backstops the check above against a path that changes
    // between stat and open — see OPML_READ_TIMEOUT.
    let bytes = read_bytes_bounded(path, OPML_READ_TIMEOUT).map_err(unreadable)?;
    // Size check before decoding: `take` cuts at a byte offset, so an
    // over-cap file can end mid-UTF-8-sequence. Decoding first would then
    // report an "invalid UTF-8" error for what is really an over-size file,
    // sending the operator after an encoding bug that isn't there.
    if bytes.len() as u64 > MAX_OPML_BYTES {
        return Err(unreadable(format!(
            "file exceeds the {MAX_OPML_BYTES}-byte OPML size limit"
        )));
    }
    let content = String::from_utf8(bytes).map_err(|e| {
        unreadable(format!(
            "OPML file '{}' is not valid UTF-8: {e}",
            path.display()
        ))
    })?;

    let invalid = |reason: String| RssError::InvalidConfig { reason };

    let mut reader = Reader::from_str(&content);
    let mut subs: Vec<(String, Option<String>)> = Vec::new();

    loop {
        let event = reader.read_event().map_err(|e| {
            invalid(format!(
                "OPML file '{}' is not well-formed XML: {e}",
                path.display()
            ))
        })?;

        let tag = match event {
            Event::Eof => break,
            Event::Start(tag) | Event::Empty(tag) if tag.name().as_ref() == b"outline" => tag,
            _ => continue,
        };

        let decoder = reader.decoder();
        let mut xml_url = None;
        let mut text_attr = None;
        let mut title_attr = None;

        for attr in tag.attributes() {
            let attr = attr.map_err(|e| {
                invalid(format!(
                    "OPML file '{}' has a malformed attribute: {e}",
                    path.display()
                ))
            })?;
            let decode = || -> Result<String, RssError> {
                // `decode_and_unescape_value` is deprecated in quick-xml 0.41 in
                // favor of this, and `Implicit1_0` is what its own deprecated
                // body passes. Relative to the 0.37 this bump replaced, though,
                // the behavior is *not* identical: 0.37's
                // `decode_and_unescape_value` was decode + `unescape_with`, i.e.
                // entity resolution only, while 0.41 routes through
                // `normalize_xml10_attribute_value`, which also performs XML
                // attribute-value normalization — `is_xml10_normalization_char`
                // (quick-xml-0.41.0/src/escape.rs) fires on `\t`, `\r`, `\n` and
                // `&`, so a literal tab or newline inside an OPML attribute
                // value now collapses to a space (`a\tb` → `a b`, pinned in this
                // module's tests). That is the spec-correct reading of an
                // attribute value, and it only ever touches whitespace inside a
                // feed's display name or URL, so it is kept deliberately.
                attr.decoded_and_normalized_value(XmlVersion::Implicit1_0, decoder)
                    .map(|v| v.into_owned())
                    .map_err(|e| {
                        invalid(format!(
                            "OPML file '{}' has a malformed attribute value: {e}",
                            path.display()
                        ))
                    })
            };
            // The OPML 2.0 spec (http://opml.org/spec2.opml) names this
            // attribute `xmlUrl`, but some real-world OPML exports lowercase
            // it to `xmlurl`; accept both rather than silently dropping
            // those subscriptions.
            match attr.key.as_ref() {
                b"xmlUrl" | b"xmlurl" => xml_url = Some(decode()?),
                b"text" => text_attr = Some(decode()?),
                b"title" => title_attr = Some(decode()?),
                _ => {}
            }
        }

        // name = `text`, else `title`, else (left as `None` here) the URL —
        // `finalize` applies that last fallback uniformly for both input
        // forms.
        if let Some(url) = xml_url {
            subs.push((url, text_attr.or(title_attr)));
        }
    }

    // Not checked here: an OPML file with zero `xmlUrl` outlines is
    // structurally fine XML, just an empty subscription list — `finalize`
    // rejects that uniformly for both input forms, so an inline `feeds: []`
    // reached without `validate()` having run first gets the same check
    // instead of silently resolving to zero subscriptions.
    Ok(subs)
}

/// Open `path` and read its raw bytes, bounded two ways: at most
/// [`MAX_OPML_BYTES`] `+ 1` bytes (via `take`, so the extra byte is how the
/// caller distinguishes at-cap from over-cap), and at most `timeout` of
/// wall-clock time for the whole open-and-read.
///
/// Raw bytes, not a `String`, on purpose: `take` slices at a byte offset
/// with no regard for UTF-8 character boundaries, so an over-cap file can
/// leave a multibyte sequence cut in half at the end of the buffer. Decoding
/// here would then fail with an "invalid UTF-8" error for what is really an
/// over-size file — the caller must apply the size check to these bytes
/// *before* attempting to decode, so the size limit is the diagnostic the
/// operator sees.
///
/// std has no timed read for files, so the bound is imposed from outside:
/// the open and read run on a dedicated helper thread, and this function
/// waits on a channel with `recv_timeout`. On timeout the helper thread is
/// *abandoned*, not killed — there is no portable way to interrupt a read
/// stuck in the kernel. That leak is deliberately acceptable here: it is at
/// most one thread per failed registration attempt, holding at most the
/// `take`-bounded ~1 MiB, and the alternative (no timeout) is the
/// registration path itself hanging instead — the thing this bound exists
/// to prevent.
///
/// Errors are returned as bare reason strings: only the caller knows the
/// source `name` that [`RssError::OpmlUnreadable`] wants.
fn read_bytes_bounded(path: &Path, timeout: Duration) -> Result<Vec<u8>, String> {
    let (tx, rx) = mpsc::channel();
    let path: PathBuf = path.to_path_buf();
    thread::spawn(move || {
        let result = (|| {
            let file = File::open(&path).map_err(|e| e.to_string())?;
            let mut bytes = Vec::new();
            file.take(MAX_OPML_BYTES + 1)
                .read_to_end(&mut bytes)
                .map_err(|e| e.to_string())?;
            Ok(bytes)
        })();
        // The receiver is gone if the timeout already fired; nothing to do
        // with the result in that case.
        let _ = tx.send(result);
    });
    match rx.recv_timeout(timeout) {
        Ok(result) => result,
        Err(_) => Err(format!(
            "did not finish reading within {}s (OPML read timeout)",
            timeout.as_secs()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sources::providers::rss::FeedSubscription;
    use crate::sources::providers::rss::config::inline_config;
    #[cfg(unix)]
    use std::process::Command;
    #[cfg(unix)]
    use std::time::Instant;

    #[test]
    fn opml_outlines_resolve_to_subscriptions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("subs.opml");
        std::fs::write(
            &path,
            r#"<?xml version="1.0"?>
<opml version="2.0">
  <head><title>subs</title></head>
  <body>
    <outline text="Tech">
      <outline type="rss" text="Rust Blog" xmlUrl="https://blog.rust-lang.org/feed.xml" htmlUrl="https://blog.rust-lang.org/"/>
      <outline title="TWiR" xmlUrl="https://this-week-in-rust.org/rss.xml"/>
    </outline>
    <outline xmlUrl="https://example.com/no-name.xml"/>
  </body>
</opml>"#,
        )
        .unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let subs = resolve_subscriptions("news", &config).unwrap();
        assert_eq!(subs.len(), 3);
        assert_eq!(
            subs[0],
            ResolvedSubscription {
                name: "Rust Blog".into(),
                url: "https://blog.rust-lang.org/feed.xml".into()
            }
        );
        assert_eq!(subs[1].name, "TWiR"); // title attr fallback
        assert_eq!(subs[2].name, "https://example.com/no-name.xml"); // name defaults to URL
    }

    #[test]
    fn literal_tabs_and_newlines_in_attribute_values_collapse_to_spaces() {
        // XML attribute-value normalization, which quick-xml applies from 0.41
        // on (see the `decode` comment above); 0.37 passed both through as-is.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("subs.opml");
        std::fs::write(
            &path,
            "<opml version=\"2.0\"><body>\
             <outline text=\"a\tb\nc\" xmlUrl=\"https://e.com/f\tx\"/>\
             </body></opml>",
        )
        .unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let subs = resolve_subscriptions("news", &config).unwrap();
        assert_eq!(subs[0].name, "a b c");
        assert_eq!(subs[0].url, "https://e.com/f x");
    }

    #[test]
    fn inline_feeds_resolve_without_io() {
        // No tempdir, no filesystem access at all: config.opml is None, so
        // resolve_subscriptions must never touch the filesystem for this
        // input form.
        let config = inline_config(vec![
            FeedSubscription {
                url: "https://blog.rust-lang.org/feed.xml".to_string(),
                name: Some("rust-blog".to_string()),
            },
            FeedSubscription {
                url: "https://this-week-in-rust.org/rss.xml".to_string(),
                name: None,
            },
        ]);
        let subs = resolve_subscriptions("news", &config).unwrap();
        assert_eq!(
            subs,
            vec![
                ResolvedSubscription {
                    name: "rust-blog".into(),
                    url: "https://blog.rust-lang.org/feed.xml".into()
                },
                ResolvedSubscription {
                    name: "https://this-week-in-rust.org/rss.xml".into(),
                    url: "https://this-week-in-rust.org/rss.xml".into()
                },
            ]
        );
    }

    #[test]
    fn oversized_opml_is_rejected_without_reading_it_all() {
        // One byte over the cap. The content never needs to parse: the size
        // check fires before the XML reader ever sees it.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("huge.opml");
        std::fs::write(&path, vec![b' '; (MAX_OPML_BYTES + 1) as usize]).unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            RssError::OpmlUnreadable { reason, .. } => assert!(
                reason.contains("OPML size limit"),
                "reason should name the size limit: {reason}"
            ),
            other => panic!("expected OpmlUnreadable, got {other:?}"),
        }
    }

    #[test]
    fn oversized_opml_ending_mid_utf8_still_reports_the_size_limit() {
        // Regression: `take(cap + 1)` cuts at a byte offset, so an over-cap
        // file whose truncation point lands inside a multibyte character
        // leaves an incomplete UTF-8 sequence at the buffer's end. Decoding
        // before the size check would surface an "invalid UTF-8" error —
        // sending the operator after an encoding bug that isn't there —
        // instead of the size-limit message. The `oversized_*` test above
        // uses ASCII padding, which never triggers this, so it is pinned
        // separately here with a multibyte pad ('é' is two UTF-8 bytes,
        // which cannot tile an odd overflow without a boundary landing
        // mid-character somewhere past the cap).
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("huge-utf8.opml");
        let bytes = "é".repeat((MAX_OPML_BYTES as usize / 2) + 1).into_bytes();
        assert!(bytes.len() as u64 > MAX_OPML_BYTES);
        std::fs::write(&path, bytes).unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            RssError::OpmlUnreadable { reason, .. } => assert!(
                reason.contains("OPML size limit"),
                "over-size file must report the size limit, not a UTF-8 error: {reason}"
            ),
            other => panic!("expected OpmlUnreadable, got {other:?}"),
        }
    }

    #[test]
    fn opml_exactly_at_the_size_cap_is_accepted() {
        // The bound is "over the cap", not "at it": a valid document padded
        // with trailing whitespace to exactly MAX_OPML_BYTES still resolves.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("at-cap.opml");
        let body = r#"<opml version="2.0"><body><outline text="A" xmlUrl="https://a.example/f.xml"/></body></opml>"#;
        let mut content = body.to_string();
        content.push_str(&" ".repeat(MAX_OPML_BYTES as usize - body.len()));
        assert_eq!(content.len() as u64, MAX_OPML_BYTES);
        std::fs::write(&path, content).unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let subs = resolve_subscriptions("news", &config).unwrap();
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].name, "A");
    }

    #[cfg(unix)]
    #[test]
    fn non_regular_file_is_rejected_before_any_read() {
        // A FIFO is the dangerous case: opening it for read blocks until a
        // writer appears, so if this rejection regressed to happen *after*
        // open, this test would hit the read timeout (slow) or hang (without
        // one). Its fast failure is itself part of what is being asserted.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("subs.fifo");
        let status = Command::new("mkfifo")
            .arg(&path)
            .status()
            .expect("run mkfifo");
        assert!(status.success(), "mkfifo failed");
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let start = Instant::now();
        let err = resolve_subscriptions("news", &config).unwrap_err();
        assert!(
            start.elapsed() < OPML_READ_TIMEOUT,
            "rejection must come from the metadata check, not the read timeout"
        );
        match err {
            RssError::OpmlUnreadable { reason, .. } => assert!(
                reason.contains("not a regular file"),
                "reason should name the file-type refusal: {reason}"
            ),
            other => panic!("expected OpmlUnreadable, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn device_file_is_rejected_before_any_read() {
        // /dev/zero would defeat the byte cap by supply rather than by
        // blocking (infinite bytes, no EOF); the regular-file check refuses
        // it before a single byte is read.
        let config = RssConfig {
            opml: Some("/dev/zero".into()),
            ..inline_config(vec![])
        };
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            RssError::OpmlUnreadable { reason, .. } => assert!(
                reason.contains("not a regular file"),
                "reason should name the file-type refusal: {reason}"
            ),
            other => panic!("expected OpmlUnreadable, got {other:?}"),
        }
    }

    #[cfg(unix)]
    #[test]
    fn stalled_read_hits_the_wall_clock_timeout() {
        // Exercises read_bytes_bounded's timeout arm directly (with a
        // short timeout — the 5s production value would just slow the suite):
        // a FIFO with no writer blocks at open(), which is exactly the shape
        // of stall the timeout exists to bound. Going through read_opml
        // instead would be rejected earlier by the metadata check — this is
        // the backstop *behind* that check, for the stat-to-open race it
        // cannot close.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("stalled.fifo");
        let status = Command::new("mkfifo")
            .arg(&path)
            .status()
            .expect("run mkfifo");
        assert!(status.success(), "mkfifo failed");
        let err = read_bytes_bounded(&path, Duration::from_millis(50)).unwrap_err();
        assert!(
            err.contains("OPML read timeout"),
            "error should name the timeout: {err}"
        );
        // The helper thread stays parked in open() — the documented,
        // deliberate leak; the tempdir unlink at drop does not wake it.
    }

    #[test]
    fn missing_opml_file_is_opml_unreadable() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("does-not-exist.opml");
        let config = RssConfig {
            opml: Some(path.clone()),
            ..inline_config(vec![])
        };
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            RssError::OpmlUnreadable { path: p, .. } => assert_eq!(
                p,
                path.display().to_string(),
                "error should name the unreadable path"
            ),
            other => panic!("expected OpmlUnreadable, got {other:?}"),
        }
    }

    #[test]
    fn malformed_opml_is_invalid_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.opml");
        // Genuinely ill-formed XML: the `<outline>` here is opened with a
        // start tag (not self-closed) and never gets a matching `</outline>`
        // before `</body>` appears — an end-tag mismatch quick-xml's
        // tokenizer rejects. Plain non-XML text (e.g. "not xml at all") is
        // *not* a regression case for this branch: quick-xml is a
        // non-validating tokenizer with no root-element requirement, so
        // text with zero `<` bytes parses as one `Text` event followed by
        // `Eof` — no error — and would only exercise the (already covered
        // elsewhere) empty-subscription-list path instead.
        std::fs::write(
            &path,
            r#"<opml><body><outline xmlUrl="https://a.example/f.xml"></body>"#,
        )
        .unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            // "well-formed XML" is unique to this branch — the
            // empty-subscription-list message (asserted in
            // `opml_without_any_xmlurl_is_rejected`) says "at least one
            // subscription" instead, so this can't pass by accidentally
            // matching that other branch.
            RssError::InvalidConfig { reason, .. } => assert!(
                reason.contains("well-formed XML"),
                "reason should flag the XML syntax error: {reason}"
            ),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn opml_without_any_xmlurl_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.opml");
        std::fs::write(
            &path,
            r#"<?xml version="1.0"?>
<opml version="2.0">
  <head><title>subs</title></head>
  <body>
    <outline text="Tech"/>
  </body>
</opml>"#,
        )
        .unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            RssError::InvalidConfig { reason, .. } => assert!(
                reason.contains("at least one subscription"),
                "reason should require at least one subscription: {reason}"
            ),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn inline_empty_feed_list_is_rejected_by_resolve_too() {
        // RssConfig::validate() already rejects an empty inline `feeds: []`
        // with zero I/O, so this path is unreachable through the normal
        // validate-then-resolve registration flow. It covers `finalize`'s
        // own non-empty check directly, so `resolve_subscriptions` doesn't
        // silently resolve to zero subscriptions if it is ever reached
        // ahead of `validate()` — the same invariant
        // `opml_without_any_xmlurl_is_rejected` covers for the OPML form.
        let config = inline_config(vec![]);
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            RssError::InvalidConfig { reason, .. } => assert!(
                reason.contains("at least one subscription"),
                "reason should require at least one subscription: {reason}"
            ),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_names_across_opml_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dup.opml");
        std::fs::write(
            &path,
            r#"<?xml version="1.0"?>
<opml version="2.0">
  <head><title>subs</title></head>
  <body>
    <outline text="Same Name" xmlUrl="https://a.example/feed.xml"/>
    <outline text="Same Name" xmlUrl="https://b.example/feed.xml"/>
  </body>
</opml>"#,
        )
        .unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            RssError::InvalidConfig { reason, .. } => assert!(
                reason.contains("duplicate subscription name"),
                "reason should flag the duplicate name: {reason}"
            ),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn opml_bad_scheme_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("badscheme.opml");
        std::fs::write(
            &path,
            r#"<?xml version="1.0"?>
<opml version="2.0">
  <head><title>subs</title></head>
  <body>
    <outline text="FTP Feed" xmlUrl="ftp://example.com/feed.xml"/>
  </body>
</opml>"#,
        )
        .unwrap();
        let config = RssConfig {
            opml: Some(path),
            ..inline_config(vec![])
        };
        let err = resolve_subscriptions("news", &config).unwrap_err();
        match err {
            RssError::InvalidConfig { reason, .. } => assert!(
                reason.contains("http or https"),
                "reason should flag the bad scheme: {reason}"
            ),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }
}
