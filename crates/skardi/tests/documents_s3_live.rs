//! Opt-in live tests for the `documents` connector's object-store backend.
//!
//! Disabled by default (`#[ignore]`) so the default suite stays offline and
//! deterministic. These are the only tests that exercise the real S3 code path —
//! everything else stubs at [`Loc`]/`BlobStore` level — so run them whenever the
//! object-store code changes.
//!
//! Against real AWS S3:
//!   DOCUMENTS_S3_LIVE=1 DOCUMENTS_S3_BUCKET=my-bucket \
//!     AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... \
//!     cargo test -p skardi --test documents_s3_live \
//!       --features documents,llm-extract -- --ignored
//!
//! Against MinIO (no AWS account needed):
//!   docker run -d -p 127.0.0.1:9000:9000 -e MINIO_ROOT_USER=minioadmin \
//!     -e MINIO_ROOT_PASSWORD=minioadmin quay.io/minio/minio server /data
//!   aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://skardi-test
//!   DOCUMENTS_S3_LIVE=1 DOCUMENTS_S3_BUCKET=skardi-test \
//!     AWS_ENDPOINT=http://127.0.0.1:9000 AWS_ALLOW_HTTP=true \
//!     AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID=minioadmin \
//!     AWS_SECRET_ACCESS_KEY=minioadmin \
//!     cargo test -p skardi --test documents_s3_live \
//!       --features documents,llm-extract -- --ignored
//!
//! The bucket must already exist; each test writes under a distinct prefix and
//! cleans up after itself.

#![cfg(feature = "documents")]

use std::sync::Arc;

use object_store::aws::AmazonS3Builder;
use object_store::path::Path as OsPath;
use object_store::{ObjectStore, PutPayload};

/// Fixture PDF shipped with the crate — a real 2-page PDF liteparse can parse
/// without LibreOffice or ImageMagick installed.
const FIXTURE_PDF: &str = "tests/fixtures/documents/two_pages.pdf";

fn live_enabled() -> bool {
    std::env::var("DOCUMENTS_S3_LIVE").ok().as_deref() == Some("1")
}

fn bucket() -> String {
    std::env::var("DOCUMENTS_S3_BUCKET")
        .expect("DOCUMENTS_S3_BUCKET must be set when DOCUMENTS_S3_LIVE=1")
}

fn store(bucket: &str) -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .build()
            .expect("build S3 store from environment"),
    )
}

/// Upload the fixture PDF at `key`, returning its bytes.
async fn seed(store: &Arc<dyn ObjectStore>, key: &str) -> Vec<u8> {
    let bytes = std::fs::read(FIXTURE_PDF).expect("read fixture pdf");
    store
        .put(&OsPath::from(key), PutPayload::from(bytes.clone()))
        .await
        .expect("seed object");
    bytes
}

async fn purge(store: &Arc<dyn ObjectStore>, prefix: &str) {
    use futures::StreamExt;
    let mut listing = store.list(Some(&OsPath::from(prefix)));
    while let Some(Ok(meta)) = listing.next().await {
        let _ = store.delete(&meta.location).await;
    }
}

/// End-to-end: list + fetch + parse from S3, and write page renders / crops back
/// to an S3 `image_store`. Covers the whole object-store round trip that only
/// manual testing exercised before.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: requires DOCUMENTS_S3_LIVE=1, DOCUMENTS_S3_BUCKET and AWS credentials"]
async fn live_s3_scan_reads_objects_and_writes_image_store() {
    if !live_enabled() {
        eprintln!("skipping live S3 test: set DOCUMENTS_S3_LIVE=1 to run");
        return;
    }
    let bucket = bucket();
    let st = store(&bucket);
    let base = "skardi-live/scan";
    purge(&st, base).await;

    // Two objects, one nested, to cover recursive listing and '/'-separated keys.
    seed(&st, &format!("{base}/docs/flat.pdf")).await;
    seed(&st, &format!("{base}/docs/nested/sub/deep.pdf")).await;

    let opts = skardi::sources::providers::documents::ParseOptions {
        recursive: true,
        include_globs: vec!["*.pdf".into()],
        image_mode: skardi::sources::providers::documents::ImageMode::Embedded,
        image_store: Some(format!("s3://{bucket}/{base}/extracted")),
        render_page_images: true,
        ocr: skardi::sources::providers::documents::OcrMode::Off,
        ocr_server_url: None,
    };

    let root = format!("s3://{bucket}/{base}/docs");
    let rows = skardi::sources::providers::documents::parse_source(&root, &opts)
        .expect("parse_source over s3");

    // Both files, both pages each.
    assert_eq!(rows.len(), 4, "expected 2 files x 2 pages, got {rows:?}");

    let mut paths: Vec<&str> = rows.iter().map(|r| r.path.as_str()).collect();
    paths.sort();
    paths.dedup();
    assert_eq!(
        paths,
        vec!["flat.pdf", "nested/sub/deep.pdf"],
        "prefix-relative paths must use '/' separators, identical to the local backend"
    );

    // Every row got a page render, and each ref is an s3:// URI under image_store.
    assert!(
        rows.iter().all(|r| r.page_image_ref.is_some()),
        "render_page_images=true must set page_image_ref on every row"
    );
    let refs: Vec<&String> = rows
        .iter()
        .filter_map(|r| r.page_image_ref.as_ref())
        .collect();
    assert!(
        refs.iter()
            .all(|r| r.starts_with(&format!("s3://{bucket}/{base}/extracted/"))),
        "page_image_ref must point into the configured s3 image_store: {refs:?}"
    );

    // The referenced objects must actually exist and be non-empty — the write
    // path only `warn!`s on failure, so a silent drop would otherwise pass.
    for r in &refs {
        let key = r
            .strip_prefix(&format!("s3://{bucket}/"))
            .expect("ref has bucket prefix");
        let meta = st
            .head(&OsPath::from(key))
            .await
            .unwrap_or_else(|e| panic!("page image {key} missing from S3: {e}"));
        assert!(meta.size > 0, "page image {key} is empty");
    }

    purge(&st, base).await;
}

/// The scan must not re-ingest its own output when `image_store` nests inside
/// the source prefix and matches `include_globs` (self-ingestion guard).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: requires DOCUMENTS_S3_LIVE=1, DOCUMENTS_S3_BUCKET and AWS credentials"]
async fn live_s3_self_ingestion_guard_holds_across_scans() {
    if !live_enabled() {
        eprintln!("skipping live S3 test: set DOCUMENTS_S3_LIVE=1 to run");
        return;
    }
    let bucket = bucket();
    let st = store(&bucket);
    let base = "skardi-live/selfingest";
    purge(&st, base).await;
    seed(&st, &format!("{base}/docs/report.pdf")).await;

    let opts = skardi::sources::providers::documents::ParseOptions {
        recursive: true,
        // *.png matches the crops/page renders the scan itself writes.
        include_globs: vec!["*.pdf".into(), "*.png".into()],
        image_mode: skardi::sources::providers::documents::ImageMode::Embedded,
        image_store: Some(format!("s3://{bucket}/{base}/docs/crops")),
        render_page_images: true,
        ocr: skardi::sources::providers::documents::OcrMode::Off,
        ocr_server_url: None,
    };
    let root = format!("s3://{bucket}/{base}/docs");

    let first = skardi::sources::providers::documents::parse_source(&root, &opts).expect("scan 1");
    // Crops now exist inside the scanned prefix.
    let second = skardi::sources::providers::documents::parse_source(&root, &opts).expect("scan 2");

    assert_eq!(
        first.len(),
        second.len(),
        "row count must be stable across scans; the guard failed to exclude image_store output"
    );
    assert!(
        second.iter().all(|r| !r.path.starts_with("crops/")),
        "no row may originate from inside image_store: {:?}",
        second.iter().map(|r| &r.path).collect::<Vec<_>>()
    );

    purge(&st, base).await;
}

/// A listing that is non-empty but whose every object fails to parse must be a
/// hard error, not a silently-empty result (the credentials-expired-after-list
/// scenario).
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: requires DOCUMENTS_S3_LIVE=1, DOCUMENTS_S3_BUCKET and AWS credentials"]
async fn live_s3_wholesale_parse_failure_is_an_error() {
    if !live_enabled() {
        eprintln!("skipping live S3 test: set DOCUMENTS_S3_LIVE=1 to run");
        return;
    }
    let bucket = bucket();
    let st = store(&bucket);
    let base = "skardi-live/allfail";
    purge(&st, base).await;

    // Valid extension, garbage bytes — matches the glob, fails to parse.
    for name in ["a.pdf", "b.pdf"] {
        st.put(
            &OsPath::from(format!("{base}/docs/{name}")),
            PutPayload::from_static(b"not a pdf at all"),
        )
        .await
        .expect("seed junk object");
    }

    let opts = skardi::sources::providers::documents::ParseOptions {
        recursive: true,
        include_globs: vec!["*.pdf".into()],
        ocr: skardi::sources::providers::documents::OcrMode::Off,
        ..Default::default()
    };
    let err = skardi::sources::providers::documents::parse_source(
        &format!("s3://{bucket}/{base}/docs"),
        &opts,
    )
    .expect_err("all-fail listing must be a hard error, not zero rows");
    let msg = err.to_string();
    assert!(
        msg.contains("failed to fetch/parse"),
        "unexpected error text: {msg}"
    );

    purge(&st, base).await;
}

/// An empty-but-reachable prefix is a valid, currently-empty corpus — zero rows,
/// no error. Distinguishes "nothing there" from "cannot read".
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: requires DOCUMENTS_S3_LIVE=1, DOCUMENTS_S3_BUCKET and AWS credentials"]
async fn live_s3_empty_prefix_yields_no_rows_without_error() {
    if !live_enabled() {
        eprintln!("skipping live S3 test: set DOCUMENTS_S3_LIVE=1 to run");
        return;
    }
    let bucket = bucket();
    let opts = skardi::sources::providers::documents::ParseOptions {
        recursive: true,
        include_globs: vec!["*.pdf".into()],
        ocr: skardi::sources::providers::documents::OcrMode::Off,
        ..Default::default()
    };
    let rows = skardi::sources::providers::documents::parse_source(
        &format!("s3://{bucket}/skardi-live/definitely-empty-prefix"),
        &opts,
    )
    .expect("an empty but reachable prefix must not error");
    assert!(rows.is_empty(), "expected zero rows, got {}", rows.len());
}

/// `llm_extract` must be able to read an `s3://` `image_ref` — the refs the
/// connector writes when `image_store` is `s3://`. Regression test: this used to
/// fall through to `std::fs::read("s3://…")` and fail with a filesystem error,
/// silently breaking multimodal escalation on S3.
#[cfg(feature = "llm-extract")]
#[tokio::test(flavor = "multi_thread")]
#[ignore = "live: requires DOCUMENTS_S3_LIVE=1, DOCUMENTS_S3_BUCKET and AWS credentials"]
async fn live_s3_image_ref_is_readable_by_llm_extract() {
    if !live_enabled() {
        eprintln!("skipping live S3 test: set DOCUMENTS_S3_LIVE=1 to run");
        return;
    }
    let bucket = bucket();
    let st = store(&bucket);
    let base = "skardi-live/imageref";
    purge(&st, base).await;
    seed(&st, &format!("{base}/docs/report.pdf")).await;

    let opts = skardi::sources::providers::documents::ParseOptions {
        recursive: true,
        include_globs: vec!["*.pdf".into()],
        image_store: Some(format!("s3://{bucket}/{base}/extracted")),
        render_page_images: true,
        ocr: skardi::sources::providers::documents::OcrMode::Off,
        ..Default::default()
    };
    let rows = skardi::sources::providers::documents::parse_source(
        &format!("s3://{bucket}/{base}/docs"),
        &opts,
    )
    .expect("scan");

    let image_ref = rows
        .iter()
        .find_map(|r| r.page_image_ref.clone())
        .expect("a page_image_ref");
    assert!(image_ref.starts_with("s3://"), "got {image_ref}");

    // Fetching is default-deny for data-derived refs; the opt-in gates s3:// too.
    let img = skardi::model::llm_extract::fetch_image_for_test(&image_ref, true)
        .unwrap_or_else(|e| panic!("llm_extract could not read {image_ref}: {e:#}"));
    assert_eq!(
        img.mime, "image/png",
        "mime inferred from the key extension"
    );
    assert!(!img.base64.is_empty(), "decoded image must be non-empty");

    // Without the opt-in it must be refused, not fetched.
    let err = skardi::model::llm_extract::fetch_image_for_test(&image_ref, false)
        .expect_err("s3:// must be refused when the fetch opt-in is off");
    assert!(
        err.to_string().contains("refusing to fetch"),
        "unexpected error: {err}"
    );

    purge(&st, base).await;
}
