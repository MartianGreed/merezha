//! The `BlobStore` port (CT §6, R13): streaming put and get, `head`,
//! idempotent delete and signed URLs where the backend supports them.
use crate::BoxFuture;
use bytes::Bytes;
use merezha_domain::Failure;
use std::{io, ops::Range, path::PathBuf, pin::Pin, time::Duration};

/// A boxed `Send` stream, the streaming counterpart of [`BoxFuture`].
pub type BoxStream<'a, T> = Pin<Box<dyn futures_core::Stream<Item = T> + Send + 'a>>;

/// Longest blob key.
pub const MAX_BLOB_KEY_BYTES: usize = 1024;

/// A validated key: 1 to 1024 bytes of `/`-separated segments of
/// `[A-Za-z0-9._-]`, no empty, `.` or `..` segment. Keys never contain user
/// input; content derives them from hashes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlobKey(String);
impl BlobKey {
    /// Fails [`BlobError::Invalid`] `invalid_blob_key`.
    pub fn new(value: impl Into<String>) -> Result<Self, BlobError> {
        let value = value.into();
        let valid = (1..=MAX_BLOB_KEY_BYTES).contains(&value.len())
            && value.split('/').all(|segment| {
                !segment.is_empty()
                    && segment != "."
                    && segment != ".."
                    && segment
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
            });
        if valid {
            Ok(Self(value))
        } else {
            Err(BlobError::Invalid("invalid_blob_key"))
        }
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// What to store. Every variant is written in parts as it arrives.
pub enum BlobBody {
    Bytes(Bytes),
    /// A spooled upload.
    File {
        path: PathBuf,
        len: u64,
    },
    Stream {
        body: BoxStream<'static, io::Result<Bytes>>,
        len: Option<u64>,
    },
}
impl std::fmt::Debug for BlobBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BlobBody::Bytes(bytes) => f.debug_struct("Bytes").field("len", &bytes.len()).finish(),
            BlobBody::File { len, .. } => f
                .debug_struct("File")
                .field("len", len)
                .finish_non_exhaustive(),
            BlobBody::Stream { len, .. } => f
                .debug_struct("Stream")
                .field("len", len)
                .finish_non_exhaustive(),
        }
    }
}
/// Headers set at put time and served by redirect mode.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobAttributes {
    pub content_type: String,
    pub content_disposition: String,
    pub cache_control: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlobStat {
    pub size: u64,
    pub etag: Option<String>,
}
/// A read, possibly of one byte range.
pub struct BlobRead {
    pub stat: BlobStat,
    pub range: Option<Range<u64>>,
    pub body: BoxStream<'static, io::Result<Bytes>>,
}
impl std::fmt::Debug for BlobRead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobRead")
            .field("stat", &self.stat)
            .field("range", &self.range)
            .finish_non_exhaustive()
    }
}

/// Typed failures; each maps to a classified [`Failure`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BlobError {
    /// Permanent `blob_not_found`.
    NotFound,
    /// Permanent; never reached the backend.
    Invalid(&'static str),
    /// Permanent; a 4xx from the backend.
    Rejected(&'static str),
    /// Transient, after bounded retries.
    Unavailable(&'static str),
}
impl From<BlobError> for Failure {
    fn from(error: BlobError) -> Self {
        match error {
            BlobError::NotFound => Failure::permanent("blob_not_found"),
            BlobError::Invalid(code) | BlobError::Rejected(code) => Failure::permanent(code),
            BlobError::Unavailable(code) => Failure::transient(code),
        }
    }
}

/// Object storage by key.
///
/// Contract: `put` streams the body and replaces the object atomically;
/// `get` and `head` fail [`BlobError::NotFound`] for a missing key; `delete`
/// is idempotent; `signed_get_url` returns `None` when the backend cannot
/// sign. Implementations bound every call and never buffer a whole body.
pub trait BlobStore: Send + Sync + 'static {
    fn put<'a>(
        &'a self,
        key: &'a BlobKey,
        body: BlobBody,
        attrs: &'a BlobAttributes,
    ) -> BoxFuture<'a, Result<BlobStat, BlobError>>;
    fn get<'a>(
        &'a self,
        key: &'a BlobKey,
        range: Option<Range<u64>>,
    ) -> BoxFuture<'a, Result<BlobRead, BlobError>>;
    fn head<'a>(&'a self, key: &'a BlobKey) -> BoxFuture<'a, Result<BlobStat, BlobError>>;
    fn delete<'a>(&'a self, key: &'a BlobKey) -> BoxFuture<'a, Result<(), BlobError>>;
    fn signed_get_url<'a>(
        &'a self,
        key: &'a BlobKey,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<String>, BlobError>>;
}

/// Fallback without a configured store: every call fails permanent
/// `blob_store_unavailable` ([`BlobError::Invalid`]: nothing reached a
/// backend), so no upload is accepted and then lost.
#[derive(Debug, Default)]
pub struct UnavailableBlobStore;
const DISABLED: BlobError = BlobError::Invalid("blob_store_unavailable");
impl BlobStore for UnavailableBlobStore {
    fn put<'a>(
        &'a self,
        _: &'a BlobKey,
        _: BlobBody,
        _: &'a BlobAttributes,
    ) -> BoxFuture<'a, Result<BlobStat, BlobError>> {
        Box::pin(async { Err(DISABLED) })
    }
    fn get<'a>(
        &'a self,
        _: &'a BlobKey,
        _: Option<Range<u64>>,
    ) -> BoxFuture<'a, Result<BlobRead, BlobError>> {
        Box::pin(async { Err(DISABLED) })
    }
    fn head<'a>(&'a self, _: &'a BlobKey) -> BoxFuture<'a, Result<BlobStat, BlobError>> {
        Box::pin(async { Err(DISABLED) })
    }
    fn delete<'a>(&'a self, _: &'a BlobKey) -> BoxFuture<'a, Result<(), BlobError>> {
        Box::pin(async { Err(DISABLED) })
    }
    fn signed_get_url<'a>(
        &'a self,
        _: &'a BlobKey,
        _: Duration,
    ) -> BoxFuture<'a, Result<Option<String>, BlobError>> {
        Box::pin(async { Err(DISABLED) })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_are_validated_segments() {
        assert!(BlobKey::new("content/app/sha256/9f/2c/9f2ce01a").is_ok());
        assert!(BlobKey::new(format!("a/{}", "b".repeat(1022))).is_ok());
        for bad in [
            "".to_owned(),
            "/abs".into(),
            "a//b".into(),
            "a/../b".into(),
            "./a".into(),
            "a/".into(),
            "a b".into(),
            "a\\b".into(),
            "x".repeat(1025),
        ] {
            assert_eq!(
                BlobKey::new(bad.clone()),
                Err(BlobError::Invalid("invalid_blob_key")),
                "{bad}"
            );
        }
    }
    #[tokio::test]
    async fn the_fallback_refuses_every_operation_permanently() {
        let store = UnavailableBlobStore;
        let key = BlobKey::new("content/app/x").unwrap();
        let attrs = BlobAttributes {
            content_type: "image/png".into(),
            content_disposition: "attachment".into(),
            cache_control: None,
        };
        let body = BlobBody::Bytes(Bytes::from_static(b"png"));
        assert_eq!(store.put(&key, body, &attrs).await.unwrap_err(), DISABLED);
        assert_eq!(store.get(&key, Some(0..2)).await.unwrap_err(), DISABLED);
        assert_eq!(store.head(&key).await.unwrap_err(), DISABLED);
        assert_eq!(store.delete(&key).await.unwrap_err(), DISABLED);
        assert_eq!(
            store
                .signed_get_url(&key, Duration::from_secs(60))
                .await
                .unwrap_err(),
            DISABLED
        );
        let failure: Failure = DISABLED.into();
        assert_eq!(
            (failure.code.as_str(), failure.classification),
            (
                "blob_store_unavailable",
                merezha_domain::Classification::Permanent
            )
        );
        let transient: Failure = BlobError::Unavailable("blob_backend_unavailable").into();
        assert_eq!(
            transient.classification,
            merezha_domain::Classification::Transient
        );
        assert_eq!(Failure::from(BlobError::NotFound).code, "blob_not_found");
    }
}
