//! A 20 MiB streamed put keeps memory near one part (CT-C6), on the local
//! driver and on the in-process S3 server. A counting allocator measures
//! the peak of live heap bytes above the level before the put; this binary
//! holds one test so nothing else allocates meanwhile.
mod support;

use bytes::Bytes;
use merezha_blob::PART_BYTES;
use merezha_platform::ports::{BlobAttributes, BlobBody, BlobStore};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
};

struct Counting;
static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call forwards to `System` unchanged; the counters only
// observe sizes.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: same contract as the caller's.
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let live = LIVE.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            PEAK.fetch_max(live, Ordering::Relaxed);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // SAFETY: same contract as the caller's.
        unsafe { System.dealloc(ptr, layout) };
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const TOTAL: usize = 20 * 1024 * 1024;
const CHUNK: usize = 64 * 1024;

/// 20 MiB produced on demand in 64 KiB chunks, never held at once.
fn generated() -> BlobBody {
    let stream = futures_util::stream::iter(0..TOTAL / CHUNK)
        .map(|i| Ok(Bytes::from(vec![(i % 251) as u8; CHUNK])));
    BlobBody::Stream {
        body: Box::pin(stream),
        len: Some(TOTAL as u64),
    }
}

use futures_util::StreamExt;

async fn peak_during_put(store: &dyn BlobStore) -> usize {
    let key = merezha_platform::ports::BlobKey::new("content/app/sha256/20/mb/twenty").unwrap();
    let attrs = BlobAttributes {
        content_type: "video/mp4".into(),
        content_disposition: "attachment".into(),
        cache_control: None,
    };
    let before = LIVE.load(Ordering::SeqCst);
    PEAK.store(before, Ordering::SeqCst);
    let stat = store.put(&key, generated(), &attrs).await.unwrap();
    assert_eq!(stat.size, TOTAL as u64);
    let peak = PEAK.load(Ordering::SeqCst).saturating_sub(before);
    assert_eq!(store.head(&key).await.unwrap().size, TOTAL as u64);
    peak
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_twenty_mebibyte_streamed_put_keeps_memory_near_one_part() {
    // Warm up both drivers so one-time allocations (TLS roots, the HTTP
    // pool, thread-locals) are not counted.
    let (local, _dir) = support::local_store();
    let mock = support::MockS3::start().await;
    let s3 = mock.open();
    for store in [&local as &dyn BlobStore, &s3] {
        let key = merezha_platform::ports::BlobKey::new("warm/up").unwrap();
        let attrs = BlobAttributes {
            content_type: "text/plain".into(),
            content_disposition: "attachment".into(),
            cache_control: None,
        };
        store
            .put(&key, BlobBody::Bytes(Bytes::from_static(b"w")), &attrs)
            .await
            .unwrap();
    }

    // One 5 MiB part buffered, plus the chunk in hand and the HTTP and
    // file layers' own buffers; the S3 figure also counts the in-process
    // server receiving the part. 10 MiB, half the 20 MiB body. Fixed, so a
    // larger part size fails here too.
    assert_eq!(PART_BYTES, 5 * 1024 * 1024);
    let limit = 10 * 1024 * 1024;
    let local_peak = peak_during_put(&local).await;
    let s3_peak = peak_during_put(&s3).await;
    eprintln!(
        "peak heap above baseline: local {} KiB, s3 {} KiB (part {} KiB, limit {} KiB)",
        local_peak / 1024,
        s3_peak / 1024,
        PART_BYTES / 1024,
        limit / 1024
    );
    assert!(local_peak <= limit, "local peak {local_peak}");
    assert!(s3_peak <= limit, "s3 peak {s3_peak}");
}
