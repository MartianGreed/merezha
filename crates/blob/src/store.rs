//! [`ObjectBlobStore`]: the `BlobStore` port over `object_store`'s local
//! filesystem and S3 drivers.
use crate::{
    config::{DriverConfig, LocalConfig, S3Config, normalized_prefix},
    errors::{
        BACKEND_UNAVAILABLE, BODY_UNREADABLE, CONFIG_INVALID, Cause, INVALID_ATTRIBUTES,
        INVALID_RANGE, INVALID_TTL, LENGTH_MISMATCH, RANGE_NOT_SATISFIABLE, TOO_LARGE, classify,
    },
    fs::ensure_private_dir,
};
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, TryStreamExt};
use merezha_domain::Failure;
use merezha_platform::{
    BoxFuture,
    ports::{
        BlobAttributes, BlobBody, BlobError, BlobKey, BlobRead, BlobStat, BlobStore, BoxStream,
        MAX_BLOB_KEY_BYTES,
    },
};
use object_store::{
    Attribute, AttributeValue, Attributes, BackoffConfig, ClientOptions, GetOptions, GetRange,
    ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutPayloadMut,
    RetryConfig,
    aws::{AmazonS3, AmazonS3Builder},
    local::LocalFileSystem,
    path::Path as ObjectPath,
    signer::Signer,
};
use std::{io, ops::Range, path::PathBuf, time::Duration};
use tokio::io::AsyncReadExt;

/// Largest object a put accepts: `media.max_upload_bytes`' ceiling.
pub const MAX_BLOB_BYTES: u64 = 5 * 1024 * 1024 * 1024;
/// Multipart part size, S3's minimum. Bodies up to one part are written
/// with a single request; larger ones in parts of exactly this size, one
/// part in flight, so a put holds about one part in memory.
pub const PART_BYTES: usize = 5 * 1024 * 1024;
/// Retries after the first attempt of each request (CT §6).
pub const MAX_RETRIES: usize = 3;
/// No retry starts later than this after the first attempt (CT §6). An
/// operation that never gets an answer fails after at most
/// `RETRY_TIMEOUT + timeout` plus one backoff.
pub const RETRY_TIMEOUT: Duration = Duration::from_secs(10);
/// Longest and shortest signed URL lifetime (SigV4 allows 7 days).
pub const SIGNED_URL_TTL_RANGE: (Duration, Duration) =
    (Duration::from_secs(1), Duration::from_secs(7 * 86_400));
/// The key the readiness probe heads (CT §9). Found and not found both
/// count as ready.
pub const READINESS_KEY: &str = "content/readiness/probe";
/// Longest attribute value.
const MAX_ATTRIBUTE_BYTES: usize = 1024;
/// Read size for file bodies.
const FILE_CHUNK_BYTES: usize = 64 * 1024;
/// Longest S3 connect wait, within the per-request timeout.
const MAX_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The configured driver, for telemetry (`content.driver`) and the serving
/// policy (redirect mode needs signed URLs).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Driver {
    Local,
    S3,
}
impl Driver {
    pub fn as_str(self) -> &'static str {
        match self {
            Driver::Local => "local",
            Driver::S3 => "s3",
        }
    }
    /// Whether [`BlobStore::signed_get_url`] returns URLs.
    pub fn signs_urls(self) -> bool {
        matches!(self, Driver::S3)
    }
}

enum Backend {
    Local {
        store: LocalFileSystem,
        root: PathBuf,
    },
    S3 {
        store: AmazonS3,
        prefix: String,
    },
}

/// A `BlobStore` on the local filesystem or an S3-compatible bucket.
///
/// Every call validates its input before any I/O: keys (and the prefixed
/// S3 key, at most 1024 bytes), attributes, ranges and signed URL
/// lifetimes. S3 requests are bounded by the configured timeout and retried
/// at most [`MAX_RETRIES`] times within [`RETRY_TIMEOUT`]: every operation
/// is idempotent on a content-addressed key.
pub struct ObjectBlobStore {
    backend: Backend,
}
impl std::fmt::Debug for ObjectBlobStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ObjectBlobStore")
            .field("driver", &self.driver())
            .finish_non_exhaustive()
    }
}

impl ObjectBlobStore {
    /// Open the configured driver. Validation problems and unusable roots
    /// fail permanent (`blob_config_invalid`, `blob_root_unusable`,
    /// `blob_root_insecure`); nothing contacts the bucket, so a backend
    /// that is down shows in [`ObjectBlobStore::ready`], not here.
    pub fn open(config: &DriverConfig) -> Result<Self, Failure> {
        match config {
            DriverConfig::Local(local) => Self::local(local),
            DriverConfig::S3(s3) => Self::s3(s3),
        }
    }

    fn local(config: &LocalConfig) -> Result<Self, Failure> {
        if !config.validate().is_empty() {
            return Err(Failure::permanent(CONFIG_INVALID));
        }
        let root = ensure_private_dir(&config.root)?;
        let store = LocalFileSystem::new_with_prefix(&root)
            .map_err(|_| Failure::permanent(crate::errors::ROOT_UNUSABLE))?
            .with_fsync(true);
        Ok(Self {
            backend: Backend::Local { store, root },
        })
    }

    fn s3(config: &S3Config) -> Result<Self, Failure> {
        let prefix = normalized_prefix(&config.key_prefix);
        let (Some(prefix), true) = (prefix, config.validate().is_empty()) else {
            return Err(Failure::permanent(CONFIG_INVALID));
        };
        let options = ClientOptions::new()
            .with_timeout(config.timeout)
            .with_connect_timeout(config.timeout.min(MAX_CONNECT_TIMEOUT))
            .with_allow_http(config.allow_http);
        let retry = RetryConfig {
            // 100 ms, then jittered doubling: three retries sleep under
            // 0.7 s in total.
            backoff: BackoffConfig {
                init_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(1),
                base: 2.0,
            },
            max_retries: MAX_RETRIES,
            retry_timeout: RETRY_TIMEOUT,
        };
        // `new()`, not `from_env()`: no setting comes from AWS_* variables
        // or instance metadata.
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&config.bucket)
            .with_region(&config.region)
            .with_access_key_id(config.access_key_id.expose())
            .with_secret_access_key(config.secret_access_key.expose())
            .with_virtual_hosted_style_request(!config.path_style)
            .with_allow_http(config.allow_http)
            // One `DELETE /key` per blob: core S3 API on every provider,
            // where bulk `POST /?delete` is not.
            .with_disable_bulk_delete(true)
            .with_client_options(options)
            .with_retry(retry);
        if let Some(endpoint) = &config.endpoint {
            builder = builder.with_endpoint(endpoint.trim_end_matches('/'));
        }
        let store = builder
            .build()
            .map_err(|_| Failure::permanent(CONFIG_INVALID))?;
        Ok(Self {
            backend: Backend::S3 { store, prefix },
        })
    }

    pub fn driver(&self) -> Driver {
        match self.backend {
            Backend::Local { .. } => Driver::Local,
            Backend::S3 { .. } => Driver::S3,
        }
    }

    /// The readiness probe (CT §9): a `head` of [`READINESS_KEY`] within
    /// `deadline`. Found and not found are both ready; the local driver
    /// also needs its root to still be a directory. Anything else, or the
    /// deadline passing, is the failure.
    pub async fn ready(&self, deadline: Duration) -> Result<(), BlobError> {
        let unavailable = BlobError::Unavailable(BACKEND_UNAVAILABLE);
        let probe = async {
            if let Backend::Local { root, .. } = &self.backend {
                let metadata = tokio::fs::metadata(root).await.map_err(|_| unavailable)?;
                if !metadata.is_dir() {
                    return Err(unavailable);
                }
            }
            let key = BlobKey::new(READINESS_KEY)?;
            match self.head(&key).await {
                Ok(_) | Err(BlobError::NotFound) => Ok(()),
                Err(error) => Err(error),
            }
        };
        tokio::time::timeout(deadline, probe)
            .await
            .unwrap_or(Err(unavailable))
    }

    fn store(&self) -> &dyn ObjectStore {
        match &self.backend {
            Backend::Local { store, .. } => store,
            Backend::S3 { store, .. } => store,
        }
    }

    /// The backend path for `key`: the prefix applied, at most 1024 bytes.
    fn path(&self, key: &BlobKey) -> Result<ObjectPath, BlobError> {
        let invalid = BlobError::Invalid("invalid_blob_key");
        let full = match &self.backend {
            Backend::S3 { prefix, .. } if !prefix.is_empty() => {
                format!("{prefix}{}", key.as_str())
            }
            _ => key.as_str().to_owned(),
        };
        if full.len() > MAX_BLOB_KEY_BYTES {
            return Err(invalid);
        }
        ObjectPath::parse(full).map_err(|_| invalid)
    }

    fn attributes(&self, attrs: &BlobAttributes) -> Result<Attributes, BlobError> {
        let invalid = BlobError::Invalid(INVALID_ATTRIBUTES);
        let safe = |value: &str| {
            (1..=MAX_ATTRIBUTE_BYTES).contains(&value.len())
                && value.bytes().all(|b| (0x20..0x7f).contains(&b))
        };
        if !safe(&attrs.content_type)
            || !safe(&attrs.content_disposition)
            || attrs.cache_control.as_deref().is_some_and(|v| !safe(v))
        {
            return Err(invalid);
        }
        let mut attributes = Attributes::new();
        // The local driver stores bytes only; proxy serving takes the type
        // from the asset row. S3 keeps the headers for redirect mode.
        if let Backend::S3 { .. } = self.backend {
            let value = |v: &str| AttributeValue::from(v.to_owned());
            attributes.insert(Attribute::ContentType, value(&attrs.content_type));
            attributes.insert(
                Attribute::ContentDisposition,
                value(&attrs.content_disposition),
            );
            if let Some(cache) = &attrs.cache_control {
                attributes.insert(Attribute::CacheControl, value(cache));
            }
        }
        Ok(attributes)
    }

    async fn put_body(
        &self,
        key: &BlobKey,
        body: BlobBody,
        attrs: &BlobAttributes,
    ) -> Result<BlobStat, BlobError> {
        let path = self.path(key)?;
        let attributes = self.attributes(attrs)?;
        let mut source = Source::open(body).await?;
        let (first, eof) = source.fill(PART_BYTES).await?;
        if eof {
            source.finish()?;
            let opts = PutOptions {
                attributes,
                ..PutOptions::default()
            };
            let result = self
                .store()
                .put_opts(&path, first.freeze(), opts)
                .await
                .map_err(|e| classify(&e).0)?;
            return Ok(BlobStat {
                size: source.read,
                etag: result.e_tag,
            });
        }
        let opts = PutMultipartOptions {
            attributes,
            ..PutMultipartOptions::default()
        };
        let mut upload = self
            .store()
            .put_multipart_opts(&path, opts)
            .await
            .map_err(|e| classify(&e).0)?;
        let written = async {
            let (mut part, mut eof) = (first, false);
            loop {
                if !part.is_empty() {
                    upload
                        .put_part(PutPayload::from(part))
                        .await
                        .map_err(|e| classify(&e).0)?;
                }
                if eof {
                    break;
                }
                (part, eof) = source.fill(PART_BYTES).await?;
            }
            source.finish()?;
            upload.complete().await.map_err(|e| classify(&e).0)
        }
        .await;
        match written {
            Ok(result) => Ok(BlobStat {
                size: source.read,
                etag: result.e_tag,
            }),
            Err(error) => {
                // Best effort: S3 drops the uploaded parts, the local driver
                // its staging file. A lifecycle rule for incomplete
                // multipart uploads covers a crash here.
                let _ = upload.abort().await;
                Err(error)
            }
        }
    }

    async fn get_range(
        &self,
        key: &BlobKey,
        range: Option<Range<u64>>,
    ) -> Result<BlobRead, BlobError> {
        let path = self.path(key)?;
        if range.as_ref().is_some_and(|r| r.start >= r.end) {
            return Err(BlobError::Invalid(INVALID_RANGE));
        }
        let options = GetOptions {
            range: range.clone().map(GetRange::Bounded),
            ..GetOptions::default()
        };
        match self.store().get_opts(&path, options).await {
            Ok(result) => {
                let stat = BlobStat {
                    size: result.meta.size,
                    etag: result.meta.e_tag.clone(),
                };
                let served = range.is_some().then(|| result.range.clone());
                let body: BoxStream<'static, io::Result<Bytes>> = Box::pin(
                    result
                        .into_stream()
                        .map_err(|_| io::Error::other("blob read failed")),
                );
                Ok(BlobRead {
                    stat,
                    range: served,
                    body,
                })
            }
            Err(error) => {
                let (mapped, cause) = classify(&error);
                // A read past the end is a 416 on S3 and an unexplained
                // range error locally; a head tells both apart from other
                // refusals the same way.
                if let Some(range) = range
                    && matches!(cause, Cause::Refused | Cause::Unexplained)
                    && let Ok(stat) = self.head_stat(&path).await
                    && range.start >= stat.size
                {
                    return Err(BlobError::Rejected(RANGE_NOT_SATISFIABLE));
                }
                Err(mapped)
            }
        }
    }

    async fn head_stat(&self, path: &ObjectPath) -> Result<BlobStat, BlobError> {
        let meta = self.store().head(path).await.map_err(|e| classify(&e).0)?;
        Ok(BlobStat {
            size: meta.size,
            etag: meta.e_tag,
        })
    }

    async fn sign(&self, key: &BlobKey, ttl: Duration) -> Result<Option<String>, BlobError> {
        let path = self.path(key)?;
        let (min, max) = SIGNED_URL_TTL_RANGE;
        if ttl < min || ttl > max {
            return Err(BlobError::Invalid(INVALID_TTL));
        }
        match &self.backend {
            Backend::Local { .. } => Ok(None),
            Backend::S3 { store, .. } => store
                .signed_url(http::Method::GET, &path, ttl)
                .await
                .map(|url| Some(url.to_string()))
                .map_err(|e| classify(&e).0),
        }
    }
}

/// One body read as chunks, counting bytes against its declared length and
/// [`MAX_BLOB_BYTES`].
struct Source {
    kind: SourceKind,
    expected: Option<u64>,
    read: u64,
    /// A chunk split at a part boundary, read again first.
    carry: Option<Bytes>,
    /// The source returned its end; it is not polled again.
    done: bool,
}
enum SourceKind {
    Bytes(Option<Bytes>),
    File(tokio::fs::File),
    Stream(BoxStream<'static, io::Result<Bytes>>),
}
impl Source {
    async fn open(body: BlobBody) -> Result<Self, BlobError> {
        let declared = match &body {
            BlobBody::Bytes(bytes) => Some(bytes.len() as u64),
            BlobBody::File { len, .. } => Some(*len),
            BlobBody::Stream { len, .. } => *len,
        };
        if declared.is_some_and(|len| len > MAX_BLOB_BYTES) {
            return Err(BlobError::Invalid(TOO_LARGE));
        }
        let (kind, expected) = match body {
            BlobBody::Bytes(bytes) => {
                let len = bytes.len() as u64;
                (SourceKind::Bytes(Some(bytes)), Some(len))
            }
            BlobBody::File { path, len } => {
                let file = tokio::fs::File::open(&path)
                    .await
                    .map_err(|_| BlobError::Unavailable(BODY_UNREADABLE))?;
                (SourceKind::File(file), Some(len))
            }
            BlobBody::Stream { body, len } => (SourceKind::Stream(body), len),
        };
        Ok(Self {
            kind,
            expected,
            read: 0,
            carry: None,
            done: false,
        })
    }

    async fn next(&mut self) -> Result<Option<Bytes>, BlobError> {
        if let Some(carry) = self.carry.take() {
            return Ok(Some(carry));
        }
        if self.done {
            return Ok(None);
        }
        let unreadable = BlobError::Unavailable(BODY_UNREADABLE);
        let chunk = match &mut self.kind {
            SourceKind::Bytes(bytes) => bytes.take(),
            SourceKind::File(file) => {
                let mut buffer = BytesMut::with_capacity(FILE_CHUNK_BYTES);
                match file.read_buf(&mut buffer).await.map_err(|_| unreadable)? {
                    0 => None,
                    _ => Some(buffer.freeze()),
                }
            }
            SourceKind::Stream(stream) => {
                stream.next().await.transpose().map_err(|_| unreadable)?
            }
        };
        let Some(chunk) = chunk else {
            self.done = true;
            return Ok(None);
        };
        self.read += chunk.len() as u64;
        if self.expected.is_some_and(|len| self.read > len) {
            return Err(BlobError::Invalid(LENGTH_MISMATCH));
        }
        if self.read > MAX_BLOB_BYTES {
            return Err(BlobError::Invalid(TOO_LARGE));
        }
        Ok(Some(chunk))
    }

    /// Up to `size` bytes, and whether the source ended within them.
    async fn fill(&mut self, size: usize) -> Result<(PutPayloadMut, bool), BlobError> {
        let mut part = PutPayloadMut::new();
        while part.content_length() < size {
            let Some(mut chunk) = self.next().await? else {
                return Ok((part, true));
            };
            let room = size - part.content_length();
            if chunk.len() > room {
                self.carry = Some(chunk.split_off(room));
            }
            if !chunk.is_empty() {
                part.push(chunk);
            }
        }
        // Peek, so a body of exactly one part is a single request.
        match self.next().await? {
            None => Ok((part, true)),
            Some(chunk) => {
                self.carry = Some(chunk);
                Ok((part, false))
            }
        }
    }

    /// At the end: the declared length must match what was read.
    fn finish(&self) -> Result<(), BlobError> {
        match self.expected {
            Some(len) if len != self.read => Err(BlobError::Invalid(LENGTH_MISMATCH)),
            _ => Ok(()),
        }
    }
}

impl BlobStore for ObjectBlobStore {
    fn put<'a>(
        &'a self,
        key: &'a BlobKey,
        body: BlobBody,
        attrs: &'a BlobAttributes,
    ) -> BoxFuture<'a, Result<BlobStat, BlobError>> {
        Box::pin(self.put_body(key, body, attrs))
    }
    fn get<'a>(
        &'a self,
        key: &'a BlobKey,
        range: Option<Range<u64>>,
    ) -> BoxFuture<'a, Result<BlobRead, BlobError>> {
        Box::pin(self.get_range(key, range))
    }
    fn head<'a>(&'a self, key: &'a BlobKey) -> BoxFuture<'a, Result<BlobStat, BlobError>> {
        Box::pin(async move { self.head_stat(&self.path(key)?).await })
    }
    fn delete<'a>(&'a self, key: &'a BlobKey) -> BoxFuture<'a, Result<(), BlobError>> {
        Box::pin(async move {
            let path = self.path(key)?;
            match self.store().delete(&path).await {
                Ok(()) => Ok(()),
                Err(error) => match classify(&error).0 {
                    BlobError::NotFound => Ok(()),
                    other => Err(other),
                },
            }
        })
    }
    fn signed_get_url<'a>(
        &'a self,
        key: &'a BlobKey,
        ttl: Duration,
    ) -> BoxFuture<'a, Result<Option<String>, BlobError>> {
        Box::pin(self.sign(key, ttl))
    }
}
