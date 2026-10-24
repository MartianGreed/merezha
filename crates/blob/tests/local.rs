//! Local driver specifics: the root's permissions, readiness on a lost
//! root, no signed URLs and no staging files left behind.
#![cfg(unix)]
mod support;

use bytes::Bytes;
use merezha_blob::{DriverConfig, LocalConfig, ObjectBlobStore, PART_BYTES};
use merezha_platform::ports::{BlobAttributes, BlobBody, BlobError, BlobKey, BlobStore};
use std::{os::unix::fs::PermissionsExt, time::Duration};

fn attrs() -> BlobAttributes {
    BlobAttributes {
        content_type: "application/pdf".into(),
        content_disposition: "attachment".into(),
        cache_control: None,
    }
}

fn open(root: &std::path::Path) -> Result<ObjectBlobStore, merezha_domain::Failure> {
    ObjectBlobStore::open(&DriverConfig::Local(LocalConfig::new(root)))
}

#[test]
fn a_group_writable_root_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("media");
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o775)).unwrap();
    assert_eq!(open(&root).unwrap_err().code, "blob_root_insecure");
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o757)).unwrap();
    assert_eq!(open(&root).unwrap_err().code, "blob_root_insecure");
    for mode in [0o755, 0o750, 0o701] {
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(mode)).unwrap();
        assert_eq!(
            open(&root).unwrap_err().code,
            "blob_root_insecure",
            "{mode:o}"
        );
    }
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    assert!(open(&root).is_ok());
}

#[tokio::test]
async fn readiness_fails_when_the_root_mode_drifts() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("media");
    let store = open(&root).unwrap();
    store.ready(Duration::from_secs(1)).await.unwrap();
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(
        store.ready(Duration::from_secs(1)).await.unwrap_err(),
        BlobError::Unavailable("blob_unavailable")
    );
    std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700)).unwrap();
    store.ready(Duration::from_secs(1)).await.unwrap();

    // object_store recreates a deleted root on the next put with the
    // umask's mode; readiness refuses it.
    std::fs::remove_dir_all(&root).unwrap();
    let key = BlobKey::new("content/app/sha256/ab/cd/abcd").unwrap();
    let _ = store
        .put(&key, BlobBody::Bytes(Bytes::from_static(b"x")), &attrs())
        .await;
    if root.exists() {
        let mode = std::fs::metadata(&root).unwrap().permissions().mode() & 0o777;
        assert_ne!(mode & 0o077, 0, "recreated with the umask, {mode:o}");
    }
    assert!(store.ready(Duration::from_secs(1)).await.is_err());
}

#[test]
fn a_missing_root_is_created_with_mode_0700() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("deep/media");
    open(&root).unwrap();
    let mode = std::fs::metadata(&root).unwrap().permissions().mode() & 0o777;
    assert_eq!(mode, 0o700);
    assert_eq!(
        open(std::path::Path::new("relative/media"))
            .unwrap_err()
            .code,
        "blob_config_invalid"
    );
}

#[tokio::test]
async fn readiness_fails_once_the_root_is_gone() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("media");
    let store = open(&root).unwrap();
    store.ready(Duration::from_secs(1)).await.unwrap();
    std::fs::remove_dir_all(&root).unwrap();
    assert_eq!(
        store.ready(Duration::from_secs(1)).await.unwrap_err(),
        BlobError::Unavailable("blob_unavailable")
    );
}

#[tokio::test]
async fn objects_land_under_the_root_and_failures_leave_no_staging_files() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("media");
    let store = open(&root).unwrap();
    let key = BlobKey::new("content/app/sha256/ab/cd/abcd").unwrap();
    store
        .put(&key, BlobBody::Bytes(Bytes::from_static(b"%PDF")), &attrs())
        .await
        .unwrap();
    assert_eq!(
        std::fs::read(root.join("content/app/sha256/ab/cd/abcd")).unwrap(),
        b"%PDF"
    );
    assert_eq!(
        store
            .signed_get_url(&key, Duration::from_secs(60))
            .await
            .unwrap(),
        None
    );

    // A multipart put that fails after its first part.
    let mut chunks: Vec<std::io::Result<Bytes>> = vec![Ok(Bytes::from(vec![1u8; PART_BYTES]))];
    chunks.push(Ok(Bytes::from_static(b"more")));
    chunks.push(Err(std::io::Error::other("gone")));
    let failing = BlobBody::Stream {
        body: Box::pin(futures_util::stream::iter(chunks)),
        len: None,
    };
    let other = BlobKey::new("content/app/sha256/ef/01/ef01").unwrap();
    assert_eq!(
        store.put(&other, failing, &attrs()).await.unwrap_err(),
        BlobError::Unavailable("blob_body_unreadable")
    );
    let mut files = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                files.push(path);
            }
        }
    }
    assert_eq!(files, vec![root.join("content/app/sha256/ab/cd/abcd")]);
}
