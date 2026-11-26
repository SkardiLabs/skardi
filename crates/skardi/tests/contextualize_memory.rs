//! Peak memory of `contextualize` on one large document.
//!
//! The ingest pipeline passes the document as a literal (`{content}`) next
//! to a column of its chunks. Copying that literal once per chunk made a
//! 2 MB document cut into ~3,400 chunks peak at about 7 GB. This test runs
//! that shape against a mock provider under a counting allocator and checks
//! the peak stays near the size of the input, not chunks × document.
#![cfg(feature = "contextualize")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use skardi::model::contextualize::ContextualizeRegistry;
use skardi::model::contextualize::provider::{ContextProvider, ContextRequest};

struct Counting;

static CURRENT: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let now = CURRENT.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        CURRENT.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

/// Answers instantly; the test is about memory, not the model.
struct Instant;

#[async_trait]
impl ContextProvider for Instant {
    fn name(&self) -> &str {
        "mock"
    }
    async fn contextualize(&self, _req: ContextRequest<'_>) -> anyhow::Result<String> {
        Ok("From the test document.".to_string())
    }
}

const MB: usize = 1024 * 1024;

#[tokio::test(flavor = "multi_thread")]
async fn a_2mb_document_in_3400_chunks_does_not_copy_the_document_per_chunk() {
    let mut doc = String::with_capacity(2 * MB + 64);
    let mut i = 0usize;
    while doc.len() < 2 * MB {
        doc.push_str(&format!(
            "Paragraph {i:07} covers topic {} in detail.\n",
            i % 97
        ));
        i += 1;
    }
    // 800-byte chunks every 600 bytes, like chunk(…, 800, 200).
    let chunks: Vec<&str> = (0..)
        .map(|n| n * 600)
        .take_while(|start| start + 800 <= doc.len())
        .map(|start| &doc[start..start + 800])
        .collect();
    assert!(chunks.len() > 3400, "{} chunks", chunks.len());

    let mut ctx = SessionContext::new();
    Arc::new(ContextualizeRegistry::new(
        vec![Arc::new(Instant) as Arc<dyn ContextProvider>],
        32_000,
        Some(4),
    ))
    .register(&mut ctx);
    let schema = Arc::new(Schema::new(vec![Field::new("c", DataType::Utf8, false)]));
    let batch = RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(chunks))]).unwrap();
    ctx.register_batch("chunks", batch).unwrap();
    let sql = format!("SELECT contextualize('mock', 'm', '{doc}', c) FROM chunks");

    let before = CURRENT.load(Ordering::Relaxed);
    PEAK.store(before, Ordering::Relaxed);
    let out = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let peak = PEAK.load(Ordering::Relaxed) - before;

    let rows: usize = out.iter().map(|b| b.num_rows()).sum();
    assert!(rows > 3400);
    eprintln!(
        "peak heap above baseline: {:.1} MB",
        peak as f64 / MB as f64
    );
    // Copying the document per chunk would be > 6.8 GB.
    assert!(
        peak < 200 * MB,
        "peak {:.1} MB grows with the number of chunks",
        peak as f64 / MB as f64
    );
}
