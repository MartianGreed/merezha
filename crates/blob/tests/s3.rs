//! S3 driver behaviour against the in-process server: request signing,
//! attributes, signed URL expiry, bounded retries, timeouts, failure
//! classes, and input validation before any request.
mod support;

use bytes::Bytes;
use merezha_blob::{DriverConfig, MAX_RETRIES, ObjectBlobStore, RETRY_TIMEOUT};
use merezha_platform::{
    Secret,
    ports::{BlobAttributes, BlobBody, BlobError, BlobKey, BlobStore},
};
use std::{
    ops::Range,
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

const UNAVAILABLE: BlobError = BlobError::Unavailable("blob_unavailable");
const REJECTED: BlobError = BlobError::Rejected("blob_backend_rejected");

fn attrs() -> BlobAttributes {
    BlobAttributes {
        content_type: "image/webp".into(),
        content_disposition: "attachment".into(),
        cache_control: Some("public, max-age=60".into()),
    }
}

fn key() -> BlobKey {
    BlobKey::new("content/app/sha256/9f/2c/9f2ce01a").unwrap()
}

fn bytes(data: &'static [u8]) -> BlobBody {
    BlobBody::Bytes(Bytes::from_static(data))
}

#[tokio::test]
async fn attributes_set_at_put_are_served_through_the_signed_url() {
    let mock = support::MockS3::start().await;
    let store = mock.open();
    store.put(&key(), bytes(b"webp"), &attrs()).await.unwrap();
    let object = mock.object("content/app/sha256/9f/2c/9f2ce01a").unwrap();
    assert_eq!(object.content_type.as_deref(), Some("image/webp"));
    assert_eq!(object.content_disposition.as_deref(), Some("attachment"));
    assert_eq!(object.cache_control.as_deref(), Some("public, max-age=60"));

    let url = store
        .signed_get_url(&key(), Duration::from_secs(60))
        .await
        .unwrap()
        .unwrap();
    assert!(url.starts_with(&format!("{}/media/content/app/", mock.endpoint())));
    for param in [
        "X-Amz-Algorithm=AWS4-HMAC-SHA256",
        "X-Amz-Expires=60",
        "X-Amz-SignedHeaders=host",
        "X-Amz-Signature=",
    ] {
        assert!(url.contains(param), "{param} in {url}");
    }
    let response = reqwest::get(&url).await.unwrap();
    assert_eq!(response.status(), 200);
    let header = |name: &str| response.headers()[name].to_str().unwrap().to_owned();
    assert_eq!(header("content-type"), "image/webp");
    assert_eq!(header("content-disposition"), "attachment");
    assert_eq!(header("cache-control"), "public, max-age=60");
    assert_eq!(response.bytes().await.unwrap(), "webp");
}

#[tokio::test]
async fn signed_urls_expire_and_cannot_be_altered() {
    let mock = support::MockS3::start().await;
    let store = mock.open();
    store.put(&key(), bytes(b"data"), &attrs()).await.unwrap();
    let url = store
        .signed_get_url(&key(), Duration::from_secs(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reqwest::get(&url).await.unwrap().status(), 200);
    let other = url.replace("9f2ce01a", "9f2ce01b");
    assert_eq!(reqwest::get(&other).await.unwrap().status(), 403);
    let longer = url.replace("X-Amz-Expires=1", "X-Amz-Expires=3600");
    assert_eq!(reqwest::get(&longer).await.unwrap().status(), 403);
    tokio::time::sleep(Duration::from_millis(2100)).await;
    assert_eq!(
        reqwest::get(&url).await.unwrap().status(),
        403,
        "expired after its ttl"
    );
}

#[tokio::test]
async fn wrong_credentials_are_rejected_without_retries() {
    let mock = support::MockS3::start().await;
    let mut config = mock.config();
    config.secret_access_key = Secret::new("not-the-secret");
    let store = ObjectBlobStore::open(&DriverConfig::S3(config)).unwrap();
    assert_eq!(
        store.put(&key(), bytes(b"x"), &attrs()).await.unwrap_err(),
        REJECTED
    );
    assert_eq!(store.head(&key()).await.unwrap_err(), REJECTED);
    assert_eq!(
        store.ready(Duration::from_secs(2)).await.unwrap_err(),
        REJECTED
    );
    assert_eq!(mock.request_count(), 3, "4xx is never retried");
    assert_eq!(mock.object_count(), 0);
}

#[tokio::test]
async fn server_errors_are_retried_a_bounded_number_of_times() {
    let mock = support::MockS3::start().await;
    let store = mock.open();
    store.put(&key(), bytes(b"data"), &attrs()).await.unwrap();

    // Three failures, then success: the retries absorb them.
    mock.requests.store(0, Ordering::SeqCst);
    mock.fail_next(&[503, 500, 503]);
    assert_eq!(store.head(&key()).await.unwrap().size, 4);
    assert_eq!(mock.request_count(), 1 + MAX_RETRIES);

    // One failure more than the retries: transient Unavailable.
    mock.requests.store(0, Ordering::SeqCst);
    mock.fail_next(&[503; 1 + MAX_RETRIES]);
    assert_eq!(store.head(&key()).await.unwrap_err(), UNAVAILABLE);
    assert_eq!(mock.request_count(), 1 + MAX_RETRIES);

    // A put retries too, and still stores the object exactly once.
    mock.requests.store(0, Ordering::SeqCst);
    mock.fail_next(&[500]);
    store.put(&key(), bytes(b"again"), &attrs()).await.unwrap();
    assert_eq!(mock.request_count(), 2);
    assert_eq!(store.head(&key()).await.unwrap().size, 5);
}

/// Pins how `object_store` renders statuses, which the classification
/// reads (`src/errors.rs`): 4xx permanent, 5xx and 429 transient.
#[tokio::test]
async fn every_status_class_maps_to_its_failure() {
    let mock = support::MockS3::start().await;
    let store = mock.open();
    store.put(&key(), bytes(b"data"), &attrs()).await.unwrap();
    for (status, expected) in [
        (400, REJECTED),
        (403, REJECTED),
        (404, BlobError::NotFound),
        (409, REJECTED),
        (412, REJECTED),
        (416, BlobError::Rejected("range_not_satisfiable")),
    ] {
        mock.requests.store(0, Ordering::SeqCst);
        mock.fail_next(&[status]);
        let got = if status == 416 {
            store.get(&key(), Some(0..2)).await.map(|_| ()).unwrap_err()
        } else {
            store.get(&key(), None).await.map(|_| ()).unwrap_err()
        };
        // A refused ranged read heads the object; 0..2 is satisfiable, so
        // the 416 here is reported as the backend's refusal.
        let expected = if status == 416 { REJECTED } else { expected };
        assert_eq!(got, expected, "{status}");
        let requests = mock.request_count();
        assert!(requests <= 2, "{status}: no retries ({requests})");
    }
    // Redirects mean a wrong region or endpoint: permanent, not retried.
    // S3's 301 PermanentRedirect carries no Location; the 300 does.
    for status in [300, 301, 307] {
        mock.requests.store(0, Ordering::SeqCst);
        mock.fail_next(&[status]);
        assert_eq!(store.head(&key()).await.unwrap_err(), REJECTED, "{status}");
        assert_eq!(mock.request_count(), 1, "{status}: no retries");
    }
    for status in [408, 429, 500, 502, 503] {
        mock.fail_next(&[status; 1 + MAX_RETRIES]);
        assert_eq!(
            store.head(&key()).await.unwrap_err(),
            UNAVAILABLE,
            "{status}"
        );
    }
}

#[tokio::test]
async fn a_stopped_server_is_unavailable_within_the_timeout() {
    // A port that was bound and released: connections are refused.
    let addr = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };
    let mut config = support::MockS3::start().await.config();
    config.endpoint = Some(format!("http://{addr}"));
    config.timeout = Duration::from_secs(1);
    let store = ObjectBlobStore::open(&DriverConfig::S3(config)).unwrap();
    let started = Instant::now();
    assert_eq!(store.head(&key()).await.unwrap_err(), UNAVAILABLE);
    assert_eq!(
        store.put(&key(), bytes(b"x"), &attrs()).await.unwrap_err(),
        UNAVAILABLE
    );
    assert_eq!(store.delete(&key()).await.unwrap_err(), UNAVAILABLE);
    assert_eq!(
        store.ready(Duration::from_secs(1)).await.unwrap_err(),
        UNAVAILABLE
    );
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(4),
        "four refused operations, each retried, within the 1 s timeout each: {elapsed:?}"
    );
}

#[tokio::test]
async fn a_server_that_never_answers_times_out_and_is_bounded() {
    let mock = support::MockS3::start().await;
    let mut config = mock.config();
    config.timeout = Duration::from_secs(1);
    let store = ObjectBlobStore::open(&DriverConfig::S3(config)).unwrap();
    mock.hang.store(true, Ordering::SeqCst);
    let started = Instant::now();
    assert_eq!(store.head(&key()).await.unwrap_err(), UNAVAILABLE);
    let elapsed = started.elapsed();
    assert!(elapsed >= Duration::from_secs(1), "{elapsed:?}");
    assert!(
        elapsed < RETRY_TIMEOUT + Duration::from_secs(1 + 2),
        "timeout + retry window + backoff: {elapsed:?}"
    );
    assert_eq!(
        mock.request_count(),
        1 + MAX_RETRIES,
        "timeouts are retried"
    );

    // Readiness answers within its own deadline.
    let started = Instant::now();
    assert_eq!(
        store.ready(Duration::from_millis(300)).await.unwrap_err(),
        UNAVAILABLE
    );
    assert!(started.elapsed() < Duration::from_millis(800));
}

#[tokio::test]
async fn invalid_input_never_reaches_the_backend() {
    let mock = support::MockS3::start().await;
    let mut config = mock.config();
    config.key_prefix = "p".repeat(200);
    let store = ObjectBlobStore::open(&DriverConfig::S3(config)).unwrap();
    // Valid on its own; `p…p/` (201 bytes) plus 824 bytes is 1025 once
    // prefixed, one byte over.
    let long = BlobKey::new(format!("content/{}", "k".repeat(816))).unwrap();
    assert_eq!(long.as_str().len(), 824);
    let invalid_key = BlobError::Invalid("invalid_blob_key");
    assert_eq!(
        store.put(&long, bytes(b"x"), &attrs()).await.unwrap_err(),
        invalid_key
    );
    assert_eq!(store.head(&long).await.unwrap_err(), invalid_key);
    assert_eq!(
        store.get(&long, None).await.map(|_| ()).unwrap_err(),
        invalid_key
    );
    assert_eq!(store.delete(&long).await.unwrap_err(), invalid_key);
    assert_eq!(
        store
            .signed_get_url(&long, Duration::from_secs(60))
            .await
            .unwrap_err(),
        invalid_key
    );
    let bad_attrs = BlobAttributes {
        content_type: "text/plain\n".into(),
        ..attrs()
    };
    assert_eq!(
        store
            .put(&key(), bytes(b"x"), &bad_attrs)
            .await
            .unwrap_err(),
        BlobError::Invalid("invalid_blob_attributes")
    );
    assert_eq!(
        store
            .get(&key(), Some(Range { start: 5, end: 2 }))
            .await
            .map(|_| ())
            .unwrap_err(),
        BlobError::Invalid("invalid_blob_range")
    );
    assert_eq!(
        store
            .signed_get_url(&key(), Duration::from_millis(10))
            .await
            .unwrap_err(),
        BlobError::Invalid("invalid_signed_url_ttl")
    );
    let too_large = BlobBody::File {
        path: "/nonexistent".into(),
        len: merezha_blob::MAX_BLOB_BYTES + 1,
    };
    assert_eq!(
        store.put(&key(), too_large, &attrs()).await.unwrap_err(),
        BlobError::Invalid("blob_too_large")
    );
    assert_eq!(mock.request_count(), 0, "no request reached the server");
}

#[tokio::test]
async fn an_oversized_declared_body_is_refused_before_any_request() {
    let mock = support::MockS3::start().await;
    let store = mock.open();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spool");
    std::fs::write(&path, b"tiny").unwrap();
    let body = BlobBody::File {
        path,
        len: merezha_blob::MAX_BLOB_BYTES + 1,
    };
    assert_eq!(
        store.put(&key(), body, &attrs()).await.unwrap_err(),
        BlobError::Invalid("blob_too_large")
    );
    assert_eq!(mock.request_count(), 0);
}

#[tokio::test]
async fn a_failed_multipart_upload_is_aborted() {
    let mock = support::MockS3::start().await;
    let store = mock.open();
    let data = vec![7u8; merezha_blob::PART_BYTES + 10];
    let chunks: Vec<std::io::Result<Bytes>> = data
        .chunks(1 << 20)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    // The first part uploads, then the backend fails every retry of the
    // second.
    let body = BlobBody::Stream {
        body: Box::pin(futures_util::stream::iter(chunks)),
        len: None,
    };
    let store_ref = &store;
    let put = async move { store_ref.put(&key(), body, &attrs()).await };
    let failing = async {
        // Wait until the upload is open and its first part sent.
        while mock.open_uploads() == 0
            || !mock.seen().iter().any(|r| r.query.contains("partNumber=1"))
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        mock.fail_next(&[503; 1 + MAX_RETRIES]);
    };
    let (result, ()) = tokio::join!(put, failing);
    assert_eq!(result.unwrap_err(), UNAVAILABLE);
    assert_eq!(mock.open_uploads(), 0, "the upload was aborted");
    assert_eq!(mock.object_count(), 0);
}

#[tokio::test]
async fn startup_refuses_invalid_settings() {
    let mock = support::MockS3::start().await;
    let mut config = mock.config();
    config.allow_http = false;
    assert_eq!(
        ObjectBlobStore::open(&DriverConfig::S3(config))
            .unwrap_err()
            .code,
        "blob_config_invalid"
    );
}

#[tokio::test]
async fn a_prefixed_key_of_exactly_1024_bytes_is_accepted() {
    let mock = support::MockS3::start().await;
    let mut config = mock.config();
    config.key_prefix = "p".repeat(200);
    let store = ObjectBlobStore::open(&DriverConfig::S3(config)).unwrap();
    let key = BlobKey::new(format!("content/{}", "k".repeat(815))).unwrap();
    store.put(&key, bytes(b"edge"), &attrs()).await.unwrap();
    let stored = format!("{}/{}", "p".repeat(200), key.as_str());
    assert_eq!(stored.len(), 1024);
    assert!(mock.object(&stored).is_some());
}

#[tokio::test]
async fn a_missing_bucket_is_not_ready_and_refuses_writes() {
    let mock = support::MockS3::start().await;
    let mut config = mock.config();
    config.bucket = "no-such-bucket".into();
    let store = ObjectBlobStore::open(&DriverConfig::S3(config)).unwrap();
    assert_eq!(
        store.ready(Duration::from_secs(2)).await.unwrap_err(),
        REJECTED,
        "a head alone would answer 404 and read as ready"
    );
    assert_eq!(
        store.put(&key(), bytes(b"x"), &attrs()).await.unwrap_err(),
        REJECTED
    );
    let large = BlobBody::Bytes(Bytes::from(vec![1u8; merezha_blob::PART_BYTES + 1]));
    assert_eq!(
        store.put(&key(), large, &attrs()).await.unwrap_err(),
        REJECTED,
        "the multipart path too"
    );
    // The same server's real bucket is ready.
    mock.open().ready(Duration::from_secs(2)).await.unwrap();
}

#[tokio::test]
async fn a_body_of_exactly_one_part_is_one_request() {
    let mock = support::MockS3::start().await;
    let store = mock.open();
    let data = vec![9u8; merezha_blob::PART_BYTES];
    let chunks: Vec<std::io::Result<Bytes>> = data
        .chunks(64 * 1024)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    let body = BlobBody::Stream {
        body: Box::pin(futures_util::stream::iter(chunks)),
        len: None,
    };
    store.put(&key(), body, &attrs()).await.unwrap();
    assert_eq!(mock.request_count(), 1);
    assert_eq!(mock.plain_requests(axum::http::Method::PUT), 1);
    // One byte more is a multipart upload: create, two parts, complete.
    let body = BlobBody::Bytes(Bytes::from(vec![9u8; merezha_blob::PART_BYTES + 1]));
    store.put(&key(), body, &attrs()).await.unwrap();
    assert_eq!(mock.request_count(), 1 + 4);
}

#[tokio::test]
async fn the_size_cap_applies_to_streams_of_unknown_length() {
    let mock = support::MockS3::start().await;
    let cap = 3 * 1024 * 1024;
    let store = mock.open().with_max_bytes(cap);
    let stream = |len: usize| BlobBody::Stream {
        body: Box::pin(futures_util::stream::iter(
            vec![1u8; len]
                .chunks(64 * 1024)
                .map(|c| Ok(Bytes::copy_from_slice(c)))
                .collect::<Vec<std::io::Result<Bytes>>>(),
        )),
        len: None,
    };
    let at_cap = store
        .put(&key(), stream(cap as usize), &attrs())
        .await
        .unwrap();
    assert_eq!(at_cap.size, cap);
    store.delete(&key()).await.unwrap();
    assert_eq!(
        store
            .put(&key(), stream(cap as usize + 1), &attrs())
            .await
            .unwrap_err(),
        BlobError::Invalid("blob_too_large")
    );
    assert_eq!(store.head(&key()).await.unwrap_err(), BlobError::NotFound);
    // A declared length over the cap fails before any request.
    let requests = mock.request_count();
    let declared = BlobBody::Stream {
        body: Box::pin(futures_util::stream::empty()),
        len: Some(cap + 1),
    };
    assert_eq!(
        store.put(&key(), declared, &attrs()).await.unwrap_err(),
        BlobError::Invalid("blob_too_large")
    );
    assert_eq!(mock.request_count(), requests);
    // The cap never rises above the 5 GiB ceiling.
    let raised = mock.open().with_max_bytes(u64::MAX);
    let declared = BlobBody::Stream {
        body: Box::pin(futures_util::stream::empty()),
        len: Some(merezha_blob::MAX_BLOB_BYTES + 1),
    };
    assert_eq!(
        raised.put(&key(), declared, &attrs()).await.unwrap_err(),
        BlobError::Invalid("blob_too_large")
    );
    assert_eq!(merezha_blob::MAX_BLOB_BYTES, 5 * 1024 * 1024 * 1024);
}
