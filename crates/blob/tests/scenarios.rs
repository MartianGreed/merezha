//! One scenario suite (CT-C6) run on every driver: a temp directory, the
//! in-process S3 server (with and without a key prefix), and the external
//! S3-compatible service named by `MEREZHA_TEST_S3_*` when it is set.
mod support;

use bytes::Bytes;
use futures_util::StreamExt;
use merezha_blob::{Driver, ObjectBlobStore, PART_BYTES};
use merezha_platform::ports::{BlobAttributes, BlobBody, BlobError, BlobKey, BlobStore};
use sha2::{Digest, Sha256};
use std::{io, time::Duration};

fn attrs() -> BlobAttributes {
    BlobAttributes {
        content_type: "image/png".into(),
        content_disposition: "inline".into(),
        cache_control: Some("public, max-age=3600".into()),
    }
}

/// Deterministic, non-repeating bytes.
fn pattern(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| (i as u32).wrapping_mul(2_654_435_761).to_le_bytes()[1] ^ seed)
        .collect()
}

async fn read_all(store: &dyn BlobStore, key: &BlobKey) -> Vec<u8> {
    let read = store.get(key, None).await.unwrap();
    collect(read.body).await
}

async fn collect(
    mut body: merezha_platform::ports::BoxStream<'static, io::Result<Bytes>>,
) -> Vec<u8> {
    let mut out = Vec::new();
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    out
}

fn chunked(data: Vec<u8>, chunk: usize, len: Option<u64>) -> BlobBody {
    let chunks: Vec<io::Result<Bytes>> = data
        .chunks(chunk)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    BlobBody::Stream {
        body: Box::pin(futures_util::stream::iter(chunks)),
        len,
    }
}

fn key(name: &str) -> BlobKey {
    BlobKey::new(format!("content/app/sha256/{name}")).unwrap()
}

async fn assert_absent(store: &dyn BlobStore, key: &BlobKey) {
    assert_eq!(store.head(key).await.unwrap_err(), BlobError::NotFound);
}

/// Every port behaviour CT-C6 lists, identical on each driver.
async fn suite(store: &ObjectBlobStore, label: &str) {
    let tmp = tempfile::tempdir().unwrap();

    // Put from bytes; head and get agree with put.
    let small = key("aa/bb/small");
    let data = b"hello, blob store".to_vec();
    let stat = store
        .put(&small, BlobBody::Bytes(Bytes::from(data.clone())), &attrs())
        .await
        .unwrap();
    assert_eq!(stat.size, data.len() as u64, "{label}");
    let head = store.head(&small).await.unwrap();
    assert_eq!(head.size, stat.size, "{label}");
    assert_eq!(head.etag, stat.etag, "{label}: head etag matches put");
    assert!(head.etag.is_some(), "{label}");
    let read = store.get(&small, None).await.unwrap();
    assert_eq!(read.range, None, "{label}");
    assert_eq!(read.stat.size, data.len() as u64, "{label}");
    assert_eq!(collect(read.body).await, data, "{label}");

    // Ranged get: the served range, clamped at the end; past the end is
    // not satisfiable; empty ranges never reach the backend.
    let ranged = store.get(&small, Some(1..5)).await.unwrap();
    assert_eq!(ranged.range, Some(1..5), "{label}");
    assert_eq!(ranged.stat.size, data.len() as u64, "{label}");
    assert_eq!(collect(ranged.body).await, &data[1..5], "{label}");
    let clamped = store.get(&small, Some(7..1000)).await.unwrap();
    assert_eq!(clamped.range, Some(7..data.len() as u64), "{label}");
    assert_eq!(collect(clamped.body).await, &data[7..], "{label}");
    let len = data.len() as u64;
    for past in [len..len + 1, len + 10..len + 20] {
        assert_eq!(
            store.get(&small, Some(past)).await.unwrap_err(),
            BlobError::Rejected("range_not_satisfiable"),
            "{label}"
        );
    }
    assert_eq!(
        store.get(&small, Some(3..3)).await.unwrap_err(),
        BlobError::Invalid("invalid_blob_range"),
        "{label}"
    );

    // Put from a spooled file across three parts.
    let file_data = pattern(2 * PART_BYTES + 12_345, 7);
    let spool = tmp.path().join("spool");
    std::fs::write(&spool, &file_data).unwrap();
    let from_file = key("cc/dd/file");
    let stat = store
        .put(
            &from_file,
            BlobBody::File {
                path: spool.clone(),
                len: file_data.len() as u64,
            },
            &attrs(),
        )
        .await
        .unwrap();
    assert_eq!(stat.size, file_data.len() as u64, "{label}");
    assert_eq!(
        store.head(&from_file).await.unwrap().etag,
        stat.etag,
        "{label}"
    );
    let back = read_all(store, &from_file).await;
    assert_eq!(Sha256::digest(&back), Sha256::digest(&file_data), "{label}");
    let tail = store
        .get(
            &from_file,
            Some(PART_BYTES as u64 - 3..PART_BYTES as u64 + 3),
        )
        .await
        .unwrap();
    assert_eq!(
        collect(tail.body).await,
        &file_data[PART_BYTES - 3..PART_BYTES + 3],
        "{label}: a range across a part boundary"
    );

    // Put from streams: unknown length across parts, declared length,
    // and exactly one part.
    for (name, size, chunk, declared) in [
        ("ee/ff/stream", PART_BYTES + 777, 100_000, None),
        ("ee/ff/declared", 3 * 1024, 1000, Some(3 * 1024)),
        (
            "ee/ff/one-part",
            PART_BYTES,
            64 * 1024,
            Some(PART_BYTES as u64),
        ),
        ("ee/ff/empty", 0, 1, Some(0)),
    ] {
        let data = pattern(size, 3);
        let stream_key = key(name);
        let stat = store
            .put(
                &stream_key,
                chunked(data.clone(), chunk, declared),
                &attrs(),
            )
            .await
            .unwrap();
        assert_eq!(stat.size, size as u64, "{label}: {name}");
        let back = read_all(store, &stream_key).await;
        assert_eq!(back.len(), size, "{label}: {name}");
        assert_eq!(
            Sha256::digest(&back),
            Sha256::digest(&data),
            "{label}: {name}"
        );
    }

    // Replacing an object swaps it whole.
    store
        .put(&small, BlobBody::Bytes(Bytes::from_static(b"v2")), &attrs())
        .await
        .unwrap();
    assert_eq!(read_all(store, &small).await, b"v2", "{label}");

    // Declared lengths are enforced and a refused put stores nothing.
    let short = key("gg/hh/short");
    for (len, data) in [
        (file_data.len() as u64 + 1, file_data.clone()),
        (file_data.len() as u64 - 1, file_data.clone()),
        (10, b"0123456789-".to_vec()),
    ] {
        std::fs::write(&spool, &data).unwrap();
        let body = BlobBody::File {
            path: spool.clone(),
            len,
        };
        assert_eq!(
            store.put(&short, body, &attrs()).await.unwrap_err(),
            BlobError::Invalid("blob_length_mismatch"),
            "{label}: len {len}"
        );
        assert_absent(store, &short).await;
    }
    let declared_long = chunked(
        pattern(PART_BYTES + 10, 1),
        65_536,
        Some(PART_BYTES as u64 * 2),
    );
    assert_eq!(
        store
            .put(&short, declared_long, &attrs())
            .await
            .unwrap_err(),
        BlobError::Invalid("blob_length_mismatch"),
        "{label}"
    );
    assert_absent(store, &short).await;

    // A failing body source aborts the put and stores nothing.
    let mut chunks: Vec<io::Result<Bytes>> = pattern(PART_BYTES + 1, 2)
        .chunks(65_536)
        .map(|c| Ok(Bytes::copy_from_slice(c)))
        .collect();
    chunks.push(Err(io::Error::other("client went away")));
    let failing = BlobBody::Stream {
        body: Box::pin(futures_util::stream::iter(chunks)),
        len: None,
    };
    assert_eq!(
        store.put(&short, failing, &attrs()).await.unwrap_err(),
        BlobError::Unavailable("blob_body_unreadable"),
        "{label}"
    );
    assert_absent(store, &short).await;

    // Attributes are validated the same way on both drivers.
    for bad in [
        BlobAttributes {
            content_type: String::new(),
            ..attrs()
        },
        BlobAttributes {
            content_disposition: "inline\r\nx-evil: 1".into(),
            ..attrs()
        },
        BlobAttributes {
            cache_control: Some("x".repeat(1025)),
            ..attrs()
        },
    ] {
        let body = BlobBody::Bytes(Bytes::from_static(b"x"));
        assert_eq!(
            store.put(&short, body, &bad).await.unwrap_err(),
            BlobError::Invalid("invalid_blob_attributes"),
            "{label}"
        );
    }
    assert_absent(store, &short).await;

    // Missing objects; idempotent delete.
    let missing = key("zz/zz/missing");
    assert_absent(store, &missing).await;
    assert_eq!(
        store.get(&missing, None).await.unwrap_err(),
        BlobError::NotFound,
        "{label}"
    );
    assert_eq!(
        store.get(&missing, Some(0..1)).await.unwrap_err(),
        BlobError::NotFound,
        "{label}"
    );
    store.delete(&missing).await.unwrap();
    store.delete(&small).await.unwrap();
    store.delete(&small).await.unwrap();
    assert_absent(store, &small).await;

    // Signed URLs: only S3 signs; lifetimes are bounded on both.
    let url = store
        .signed_get_url(&from_file, Duration::from_secs(300))
        .await
        .unwrap();
    assert_eq!(url.is_some(), store.driver().signs_urls(), "{label}");
    for ttl in [Duration::ZERO, Duration::from_secs(7 * 86_400 + 1)] {
        assert_eq!(
            store.signed_get_url(&from_file, ttl).await.unwrap_err(),
            BlobError::Invalid("invalid_signed_url_ttl"),
            "{label}"
        );
    }
    if let Some(url) = url {
        let response = reqwest::get(&url).await.unwrap();
        assert_eq!(response.status(), 200, "{label}");
        let body = response.bytes().await.unwrap();
        assert_eq!(Sha256::digest(&body), Sha256::digest(&file_data), "{label}");
    }

    // Readiness: the probe key is absent, which counts as ready.
    store.ready(Duration::from_secs(5)).await.unwrap();

    for name in [
        "cc/dd/file",
        "ee/ff/stream",
        "ee/ff/declared",
        "ee/ff/one-part",
        "ee/ff/empty",
    ] {
        store.delete(&key(name)).await.unwrap();
    }
}

#[tokio::test]
async fn the_suite_passes_on_a_temp_directory() {
    let (store, _dir) = support::local_store();
    assert_eq!(store.driver(), Driver::Local);
    suite(&store, "local").await;
}

#[tokio::test]
async fn the_suite_passes_on_the_in_process_s3_server() {
    let mock = support::MockS3::start().await;
    let store = mock.open();
    assert_eq!(store.driver(), Driver::S3);
    suite(&store, "mock s3").await;
    assert_eq!(mock.open_uploads(), 0, "every failed multipart was aborted");
    assert_eq!(mock.object_count(), 0);
}

#[tokio::test]
async fn the_suite_passes_with_a_key_prefix() {
    let mock = support::MockS3::start().await;
    let mut config = mock.config();
    config.key_prefix = "tenant-a/media".into();
    let store = ObjectBlobStore::open(&merezha_blob::DriverConfig::S3(config)).unwrap();
    suite(&store, "mock s3 with prefix").await;
    let probe = key("pp/qq/probe");
    store
        .put(&probe, BlobBody::Bytes(Bytes::from_static(b"p")), &attrs())
        .await
        .unwrap();
    assert!(
        mock.object("tenant-a/media/content/app/sha256/pp/qq/probe")
            .is_some()
    );
    assert!(
        mock.seen()
            .iter()
            .all(|r| r.path.starts_with("/media/tenant-a/media/content/")
                || r.path == "/media"
                    && r.query
                        .contains("prefix=tenant-a%2Fmedia%2Fcontent%2Freadiness%2F")),
        "every request carries the prefix, the readiness list included"
    );
}

#[tokio::test]
async fn the_suite_passes_on_an_external_s3_service() {
    let Some(store) = support::external_s3() else {
        return;
    };
    suite(&store, "external s3").await;
}
