//! Blob storage drivers for the platform `BlobStore` port (CT §6, slice
//! CT-C6): the local filesystem and S3-compatible buckets (AWS, MinIO, R2,
//! Garage), both through `object_store`.
//!
//! The port itself, `BlobKey` validation and the failure classes live in
//! `merezha_platform::ports::blob`; this crate keeps `object_store` out of
//! the kernel. [`ObjectBlobStore::open`] builds a store from a
//! [`DriverConfig`]; the owning module installs it with
//! `AppContext::builder().blobs(..)`, registers [`ERRORS`], and answers its
//! readiness with [`ObjectBlobStore::ready`].
//!
//! ```
//! use merezha_blob::{DriverConfig, LocalConfig, ObjectBlobStore};
//! use merezha_platform::ports::{BlobAttributes, BlobBody, BlobKey, BlobStore};
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! let dir = tempfile::tempdir().unwrap();
//! let store = ObjectBlobStore::open(&DriverConfig::Local(LocalConfig::new(
//!     dir.path().join("media"),
//! )))
//! .unwrap();
//! let key = BlobKey::new("content/app/sha256/9f/2c/9f2c").unwrap();
//! let attrs = BlobAttributes {
//!     content_type: "text/plain".into(),
//!     content_disposition: "attachment".into(),
//!     cache_control: None,
//! };
//! let stat = store
//!     .put(&key, BlobBody::Bytes("hello".into()), &attrs)
//!     .await
//!     .unwrap();
//! assert_eq!(stat.size, 5);
//! assert_eq!(store.head(&key).await.unwrap().size, 5);
//! store.delete(&key).await.unwrap();
//! store.delete(&key).await.unwrap(); // idempotent
//! # });
//! ```
//!
//! Seams for CT-C7: the content block parses `[modules.content.media]`
//! into [`LocalConfig`] / [`S3Config`] (nested secrets included), refuses
//! `serve = "redirect"` unless [`Driver::signs_urls`], reuses
//! [`ensure_private_dir`] for `media.spool_dir`, and records
//! `merezha.content.blob.duration` around these calls with
//! [`ObjectBlobStore::driver`].
mod config;
mod errors;
mod fs;
mod store;

pub use config::{
    DEFAULT_S3_TIMEOUT, DriverConfig, LocalConfig, MAX_KEY_PREFIX_BYTES, S3_TIMEOUT_RANGE, S3Config,
};
pub use errors::{
    BACKEND_REJECTED, BACKEND_UNAVAILABLE, BODY_UNREADABLE, CONFIG_INVALID, ERRORS,
    INVALID_ATTRIBUTES, INVALID_RANGE, INVALID_TTL, LENGTH_MISMATCH, RANGE_NOT_SATISFIABLE,
    ROOT_INSECURE, ROOT_UNUSABLE, TOO_LARGE,
};
pub use fs::ensure_private_dir;
pub use store::{
    Driver, MAX_BLOB_BYTES, MAX_RETRIES, ObjectBlobStore, PART_BYTES, READINESS_KEY, RETRY_TIMEOUT,
    SIGNED_URL_TTL_RANGE,
};
