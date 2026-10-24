//! Mapping `object_store` failures onto the port's typed [`BlobError`].
use merezha_platform::{ErrorCode, ports::BlobError};

/// Codes the drivers return, for the owning module's `Module::errors` (R8).
/// `blob_not_found`, `invalid_blob_key` and `blob_store_unavailable` are
/// kernel codes and stay registered by the kernel.
pub const ERRORS: &[ErrorCode] = &[
    ErrorCode::new(BACKEND_UNAVAILABLE, 503),
    ErrorCode::new(BACKEND_REJECTED, 502),
    ErrorCode::new(RANGE_NOT_SATISFIABLE, 416),
    ErrorCode::new(LENGTH_MISMATCH, 422),
    ErrorCode::new(TOO_LARGE, 413),
    ErrorCode::new(BODY_UNREADABLE, 503),
    ErrorCode::new(INVALID_ATTRIBUTES, 422),
    ErrorCode::new(INVALID_RANGE, 422),
    ErrorCode::new(INVALID_TTL, 422),
    ErrorCode::new(ROOT_INSECURE, 422),
    ErrorCode::new(ROOT_UNUSABLE, 422),
    ErrorCode::new(CONFIG_INVALID, 422),
];

/// Transient: the backend did not answer, timed out or answered 5xx after
/// the bounded retries.
pub const BACKEND_UNAVAILABLE: &str = "blob_unavailable";
/// Permanent: the backend answered 4xx (credentials, bucket, policy).
pub const BACKEND_REJECTED: &str = "blob_backend_rejected";
/// Permanent: a ranged read starting at or past the end of the object.
pub const RANGE_NOT_SATISFIABLE: &str = "range_not_satisfiable";
/// Permanent: a body shorter or longer than its declared length. Nothing
/// is stored.
pub const LENGTH_MISMATCH: &str = "blob_length_mismatch";
/// Permanent: a body over [`crate::MAX_BLOB_BYTES`].
pub const TOO_LARGE: &str = "blob_too_large";
/// Transient: the body's own source (file or stream) failed while it was
/// read. Nothing is stored.
pub const BODY_UNREADABLE: &str = "blob_body_unreadable";
/// Permanent: an attribute that is empty, too long or not header-safe.
pub const INVALID_ATTRIBUTES: &str = "invalid_blob_attributes";
/// Permanent: an empty or reversed byte range.
pub const INVALID_RANGE: &str = "invalid_blob_range";
/// Permanent: a signed URL lifetime outside 1 s to 7 days.
pub const INVALID_TTL: &str = "invalid_signed_url_ttl";
/// Startup: the local root is writable by group or others.
pub const ROOT_INSECURE: &str = "blob_root_insecure";
/// Startup: the local root cannot be created, is not a directory or is not
/// absolute.
pub const ROOT_UNUSABLE: &str = "blob_root_unusable";
/// Startup: the driver settings failed validation or the client could not
/// be built.
pub const CONFIG_INVALID: &str = "blob_config_invalid";

/// How a failure arose, beyond its [`BlobError`]: ranged reads diagnose a
/// rejected or unexplained failure with a `head`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Cause {
    NotFound,
    /// A definite answer from the backend (4xx) or a local refusal.
    Refused,
    /// The network, a timeout, a 5xx or a local I/O error.
    Unreachable,
    /// A failure that names no status and no transport error, such as the
    /// local driver's out-of-range read.
    Unexplained,
}

/// The port error and cause for one `object_store` failure.
///
/// Typed variants map directly. A generic failure is classified by walking
/// its source chain: a transport error is transient, an I/O error by kind,
/// and an HTTP status by class (3xx and 4xx permanent except 408 and 429). `object_store` keeps its status error type
/// private, so the status is read from the `non-2xx status code: NNN` text
/// it renders; `tests/s3.rs` pins that rendering for every class.
pub(crate) fn classify(error: &object_store::Error) -> (BlobError, Cause) {
    use object_store::Error as E;
    match error {
        E::NotFound { .. } => (BlobError::NotFound, Cause::NotFound),
        E::PermissionDenied { .. }
        | E::Unauthenticated { .. }
        | E::Precondition { .. }
        | E::AlreadyExists { .. }
        | E::NotModified { .. }
        | E::NotSupported { .. }
        | E::NotImplemented { .. }
        | E::UnknownConfigurationKey { .. } => {
            (BlobError::Rejected(BACKEND_REJECTED), Cause::Refused)
        }
        E::InvalidPath { .. } => (BlobError::Invalid("invalid_blob_key"), Cause::Refused),
        E::Generic { source, .. } => classify_chain(source.as_ref()),
        _ => unavailable(Cause::Unreachable),
    }
}

fn classify_chain(error: &(dyn std::error::Error + 'static)) -> (BlobError, Cause) {
    let mut current = Some(error);
    while let Some(error) = current {
        if error
            .downcast_ref::<object_store::client::HttpError>()
            .is_some()
        {
            return unavailable(Cause::Unreachable);
        }
        if let Some(io) = error.downcast_ref::<std::io::Error>() {
            return match io.kind() {
                std::io::ErrorKind::NotFound => (BlobError::NotFound, Cause::NotFound),
                _ => unavailable(Cause::Unreachable),
            };
        }
        let text = error.to_string();
        // A redirect is a wrong region or endpoint, never transient. S3's
        // `PermanentRedirect` carries no `Location`.
        if text.contains(BARE_REDIRECT) {
            return (BlobError::Rejected(BACKEND_REJECTED), Cause::Refused);
        }
        if let Some(status) = rendered_status(&text) {
            // 408 and 429 are the backend asking for time, like a 5xx.
            return if (300..500).contains(&status) && status != 408 && status != 429 {
                (BlobError::Rejected(BACKEND_REJECTED), Cause::Refused)
            } else {
                unavailable(Cause::Unreachable)
            };
        }
        current = error.source();
    }
    unavailable(Cause::Unexplained)
}

fn unavailable(cause: Cause) -> (BlobError, Cause) {
    (BlobError::Unavailable(BACKEND_UNAVAILABLE), cause)
}

/// `object_store`'s rendering of a 3xx without `Location`.
const BARE_REDIRECT: &str = "Received redirect without LOCATION";

/// The status in `object_store`'s `Server returned non-2xx status code:
/// 416 Range Not Satisfiable: ...` rendering.
fn rendered_status(text: &str) -> Option<u16> {
    const MARKER: &str = "non-2xx status code: ";
    let at = text.find(MARKER)? + MARKER.len();
    let digits = text.get(at..at + 3)?;
    digits
        .bytes()
        .all(|b| b.is_ascii_digit())
        .then(|| digits.parse().ok())
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statuses_are_read_from_the_rendering() {
        assert_eq!(
            rendered_status("Server returned non-2xx status code: 416 Range Not Satisfiable: x"),
            Some(416)
        );
        assert_eq!(
            rendered_status("non-2xx status code: 503 Service Unavailable"),
            Some(503)
        );
        assert_eq!(rendered_status("non-2xx status code: 5x"), None);
        assert_eq!(rendered_status("connection refused"), None);
    }

    #[test]
    fn typed_variants_map_directly() {
        let source = || -> Box<dyn std::error::Error + Send + Sync> { "x".into() };
        let not_found = object_store::Error::NotFound {
            path: "k".into(),
            source: source(),
        };
        assert_eq!(classify(&not_found).0, BlobError::NotFound);
        let denied = object_store::Error::PermissionDenied {
            path: "k".into(),
            source: source(),
        };
        assert_eq!(
            classify(&denied).0,
            BlobError::Rejected("blob_backend_rejected")
        );
        let io = object_store::Error::Generic {
            store: "LocalFileSystem",
            source: Box::new(std::io::Error::other("disk")),
        };
        assert_eq!(classify(&io).0, BlobError::Unavailable("blob_unavailable"));
        let unexplained = object_store::Error::Generic {
            store: "LocalFileSystem",
            source: source(),
        };
        assert_eq!(classify(&unexplained).1, Cause::Unexplained);
    }

    #[test]
    fn the_codes_register_beside_the_kernel() {
        let mut registry = merezha_platform::ErrorRegistry::kernel();
        registry.register("content", ERRORS).expect("no conflicts");
        assert_eq!(
            registry
                .status_of("range_not_satisfiable")
                .map(|s| s.as_u16()),
            Some(416)
        );
    }
}
