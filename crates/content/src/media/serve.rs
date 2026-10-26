//! Serving asset bytes (section 6): proxy mode streams from the blob store
//! with the asset's type, `inline` only for `media.inline_types` and
//! otherwise `attachment` with the filename, `nosniff` and a sandbox CSP
//! always, the SHA-256 as a strong ETag (304 on a match) and one byte
//! range (206; several ranges get the full body). Redirect mode answers
//! 302 to a presigned URL of `media.signed_url_ttl`. Delivery serves an
//! asset only when a published variant of a type the caller may read
//! references it.
use super::{AssetView, Assets, not_found, sql, sql_error};
use crate::{model::Access, store::Rejection};
use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
};
use merezha_domain::{Failure, Identity};
use merezha_platform::ports::{Authorizer, BlobError, BlobKey, BlobStore, Principal};
use std::ops::Range;

/// `Content-Security-Policy` of every asset response.
pub const ASSET_CSP: &str = "default-src 'none'; sandbox";
/// `Cache-Control` of an asset no `public` type delivers.
pub const PRIVATE_ASSET_CACHE: &str = "private, no-cache";
/// What a redirect's `Cache-Control` keeps off the presigned URL's
/// lifetime, so a cached redirect never outlives its URL.
const REDIRECT_MARGIN_SECS: u64 = 30;

/// What a `Range` header asks of a body of `size` bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Ranged {
    /// No range, an unknown unit, several ranges or a malformed one: the
    /// full body.
    Full,
    /// One satisfiable range, `start..end` (end exclusive, clamped).
    Part(Range<u64>),
    /// One range starting at or past the end (416).
    Unsatisfiable,
}

/// Parse `Range` for a body of `size` bytes (RFC 9110 §14.1.2): only one
/// `bytes=` range is honoured; `a-b`, `a-` and `-n` forms; numbers of at
/// most 19 digits.
pub(crate) fn parse_range(value: Option<&str>, size: u64) -> Ranged {
    let Some(value) = value else {
        return Ranged::Full;
    };
    let Some(spec) = value
        .trim()
        .strip_prefix("bytes=")
        .or_else(|| value.trim().strip_prefix("BYTES="))
    else {
        return Ranged::Full;
    };
    if spec.contains(',') {
        return Ranged::Full;
    }
    let Some((first, last)) = spec.trim().split_once('-') else {
        return Ranged::Full;
    };
    let number = |s: &str| -> Option<u64> {
        (!s.is_empty() && s.len() <= 19 && s.bytes().all(|b| b.is_ascii_digit()))
            .then(|| s.parse().ok())
            .flatten()
    };
    match (first.trim(), last.trim()) {
        ("", suffix) => match number(suffix) {
            None => Ranged::Full,
            Some(0) => Ranged::Unsatisfiable,
            Some(_) if size == 0 => Ranged::Unsatisfiable,
            Some(n) => Ranged::Part(size.saturating_sub(n)..size),
        },
        (start, "") => match number(start) {
            None => Ranged::Full,
            Some(start) if start >= size => Ranged::Unsatisfiable,
            Some(start) => Ranged::Part(start..size),
        },
        (start, end) => match (number(start), number(end)) {
            (Some(start), Some(end)) if start <= end => {
                if start >= size {
                    Ranged::Unsatisfiable
                } else {
                    Ranged::Part(start..end.saturating_add(1).min(size))
                }
            }
            _ => Ranged::Full,
        },
    }
}

/// The strong ETag of an asset's bytes.
pub(crate) fn etag(asset: &AssetView) -> String {
    format!("\"{}\"", asset.sha256)
}

/// Whether `If-None-Match` names `tag` (or `*`); weak tags compare weakly,
/// as RFC 9110 says for this header.
fn none_match(headers: &HeaderMap, tag: &str) -> bool {
    headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .any(|candidate| candidate == "*" || candidate.trim_start_matches("W/") == tag)
}

/// A `Range` applies unless `If-Range` names another validator (a date,
/// or another tag).
fn range_header<'a>(headers: &'a HeaderMap, tag: &str) -> Option<&'a str> {
    if let Some(condition) = headers.get(header::IF_RANGE)
        && condition.to_str().ok().map(str::trim) != Some(tag)
    {
        return None;
    }
    headers.get(header::RANGE).and_then(|v| v.to_str().ok())
}

/// How a response presents the bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Disposition {
    /// `inline` for `media.inline_types`, else `attachment`.
    Delivery,
    /// Always `attachment` (management reads).
    Attachment,
}

fn insert(headers: &mut HeaderMap, name: header::HeaderName, value: &str) {
    if let Ok(value) = HeaderValue::from_str(value) {
        headers.insert(name, value);
    }
}

/// The headers every asset response carries.
fn common(headers: &mut HeaderMap, tag: &str, cache_control: &str, delivery: bool) {
    insert(headers, header::ETAG, tag);
    insert(headers, header::CACHE_CONTROL, cache_control);
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(ASSET_CSP),
    );
    if delivery {
        headers.insert(
            header::VARY,
            HeaderValue::from_static("Authorization, Cookie"),
        );
    }
}

fn range_refused(size: i64, tag: &str, cache_control: &str, delivery: bool) -> Response {
    let mut response = crate::http::error_response(Rejection::new(Failure::permanent(
        merezha_blob::RANGE_NOT_SATISFIABLE,
    )));
    let headers = response.headers_mut();
    insert(headers, header::CONTENT_RANGE, &format!("bytes */{size}"));
    common(headers, tag, cache_control, delivery);
    response
}

fn blob_failure(error: BlobError) -> Rejection {
    Rejection::new(Failure::from(error))
}

impl Assets {
    /// Stream `asset` through the host.
    pub(crate) async fn proxy(
        &self,
        asset: &AssetView,
        headers: &HeaderMap,
        disposition: Disposition,
        cache_control: &str,
        delivery: bool,
    ) -> Result<Response, Rejection> {
        let tag = etag(asset);
        if none_match(headers, &tag) {
            let mut response = Response::new(Body::empty());
            *response.status_mut() = StatusCode::NOT_MODIFIED;
            common(response.headers_mut(), &tag, cache_control, delivery);
            return Ok(response);
        }
        let size = u64::try_from(asset.size).unwrap_or(0);
        let range = match parse_range(range_header(headers, &tag), size) {
            Ranged::Unsatisfiable => {
                return Ok(range_refused(asset.size, &tag, cache_control, delivery));
            }
            Ranged::Full => None,
            Ranged::Part(range) => Some(range),
        };
        let key = BlobKey::new(asset.storage_key.clone()).map_err(blob_failure)?;
        // An empty object has no range to read.
        let read = if size == 0 {
            None
        } else {
            match self.blobs.get(&key, range.clone()).await {
                // The object must be the bytes the row describes, or the
                // declared length would be wrong.
                Ok(read) if read.stat.size != size => {
                    return Err(Rejection::new(Failure::transient("internal")));
                }
                Ok(read) => Some(read),
                Err(BlobError::Rejected(code)) if code == merezha_blob::RANGE_NOT_SATISFIABLE => {
                    return Ok(range_refused(asset.size, &tag, cache_control, delivery));
                }
                Err(error) => return Err(blob_failure(error)),
            }
        };
        let served = read.as_ref().and_then(|r| r.range.clone()).or(range);
        let length = served.as_ref().map_or(size, |r| r.end - r.start);
        let body = match read {
            Some(read) => Body::from_stream(read.body),
            None => Body::empty(),
        };
        let mut response = Response::new(body);
        let headers = response.headers_mut();
        insert(
            headers,
            header::CONTENT_TYPE,
            &served_type(&asset.content_type),
        );
        insert(headers, header::CONTENT_LENGTH, &length.to_string());
        headers.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
        let inline =
            disposition == Disposition::Delivery && self.settings().inline(&asset.content_type);
        if inline {
            headers.insert(
                header::CONTENT_DISPOSITION,
                HeaderValue::from_static("inline"),
            );
        } else {
            // The stored filename is `[A-Za-z0-9._-]` only: nothing to quote.
            insert(
                headers,
                header::CONTENT_DISPOSITION,
                &format!("attachment; filename=\"{}\"", asset.filename),
            );
        }
        common(headers, &tag, cache_control, delivery);
        if let Some(served) = served {
            insert(
                headers,
                header::CONTENT_RANGE,
                &format!("bytes {}-{}/{size}", served.start, served.end - 1),
            );
            *response.status_mut() = StatusCode::PARTIAL_CONTENT;
        }
        Ok(response)
    }

    /// 302 to a presigned GET URL valid for `media.signed_url_ttl`, cached
    /// privately for 30 seconds less. Type and disposition are the ones
    /// set at put time.
    pub(crate) async fn redirect(&self, asset: &AssetView) -> Result<Response, Rejection> {
        let ttl = self.settings().signed_url_ttl;
        let key = BlobKey::new(asset.storage_key.clone()).map_err(blob_failure)?;
        let url = self
            .blobs
            .signed_get_url(&key, ttl)
            .await
            .map_err(blob_failure)?
            .ok_or_else(|| Rejection::new(Failure::permanent("media_config_invalid")))?;
        let location = HeaderValue::from_str(&url)
            .map_err(|_| Rejection::new(Failure::transient("internal")))?;
        let mut response = Response::new(Body::empty());
        *response.status_mut() = StatusCode::FOUND;
        let headers = response.headers_mut();
        headers.insert(header::LOCATION, location);
        insert(
            headers,
            header::CACHE_CONTROL,
            &format!(
                "private, max-age={}",
                ttl.as_secs().saturating_sub(REDIRECT_MARGIN_SECS)
            ),
        );
        headers.insert(
            header::VARY,
            HeaderValue::from_static("Authorization, Cookie"),
        );
        headers.insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
        Ok(response)
    }

    /// The live asset `id` named `filename`, when a published variant of
    /// a type `principal` may read on this path references it, with
    /// whether a `public` type does. On the application path (`org` is
    /// `None`) that is an application asset referenced from the
    /// application scope. On an organization's path it is an asset of that
    /// organization, or an application asset its entries reference, and
    /// only that organization's published refs count, read under the
    /// path's access rules (section 8, C9): another organization's assets
    /// and references never reach the answer. Anything else is
    /// `asset_not_found`, so a draft-only upload stays unreachable even
    /// with its id.
    pub(crate) async fn deliverable(
        &self,
        org: Option<&Identity>,
        id: &str,
        filename: &str,
        principal: Option<&Principal>,
        policy: &dyn Authorizer,
    ) -> Result<(AssetView, bool), Rejection> {
        // The scope whose published refs decide: the path's.
        let refs_scope = match org {
            Some(org) => format!("org:{}", org.as_str()),
            None => "app".to_owned(),
        };
        // Issue #56: nothing on a deleted organization's path, the
        // application assets its entries used included.
        self.store.refuse_deleted(&refs_scope).await?;
        let asset = match self.live(&refs_scope, id).await? {
            Some(asset) => asset,
            None if org.is_some() => self.live("app", id).await?.ok_or_else(not_found)?,
            None => return Err(not_found()),
        };
        if asset.filename != filename {
            return Err(not_found());
        }
        let db = self.db().clone();
        let client = db.acquire(self.deadline()).await?;
        let types: Vec<String> = db
            .query(&client, &sql::ASSET_PUBLISHED_TYPES, &[&id, &refs_scope])
            .await
            .map_err(|e| sql_error(&e))?
            .iter()
            .map(|row| row.get(0))
            .collect();
        let model = self.state().model();
        let mut readable = false;
        let mut public = false;
        for name in &types {
            // A type this host's model lacks (a newer one) is not served.
            let Some(content_type) = model.content_type(name) else {
                continue;
            };
            // The entries' own delivery rule (C5's) on this path.
            let allowed = crate::delivery::access(policy, principal, content_type, org).is_ok();
            public |= allowed && content_type.access == Access::Public;
            readable |= allowed;
        }
        if !readable {
            return Err(not_found());
        }
        Ok((asset, public))
    }
}

/// The `Content-Type` an asset is served with: UTF-8 checked text types
/// say so.
fn served_type(content_type: &str) -> String {
    match content_type {
        "text/plain" | "text/csv" => format!("{content_type}; charset=utf-8"),
        other => other.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_byte_ranges_are_honoured_and_bounded() {
        let size = 1000;
        for (header, expected) in [
            (None, Ranged::Full),
            (Some("bytes=0-99"), Ranged::Part(0..100)),
            (Some("bytes=900-"), Ranged::Part(900..1000)),
            (Some("bytes=-100"), Ranged::Part(900..1000)),
            (Some("bytes=-5000"), Ranged::Part(0..1000)),
            (Some("bytes=990-5000"), Ranged::Part(990..1000)),
            (Some("bytes=999-999"), Ranged::Part(999..1000)),
            (Some("bytes=1000-"), Ranged::Unsatisfiable),
            (Some("bytes=1000-1001"), Ranged::Unsatisfiable),
            (Some("bytes=-0"), Ranged::Unsatisfiable),
            // Several ranges, other units and malformed ones: full body.
            (Some("bytes=0-1,5-6"), Ranged::Full),
            (Some("items=0-1"), Ranged::Full),
            (Some("bytes=5-1"), Ranged::Full),
            (Some("bytes=a-b"), Ranged::Full),
            (Some("bytes=-"), Ranged::Full),
            (Some("bytes=99999999999999999999-"), Ranged::Full),
        ] {
            assert_eq!(parse_range(header, size), expected, "{header:?}");
        }
        assert_eq!(parse_range(Some("bytes=0-"), 0), Ranged::Unsatisfiable);
        assert_eq!(parse_range(Some("bytes=-1"), 0), Ranged::Unsatisfiable);
    }

    #[test]
    fn if_none_match_and_if_range_compare_the_hash_tag() {
        let tag = "\"abc\"";
        let mut headers = HeaderMap::new();
        assert!(!none_match(&headers, tag));
        headers.insert(
            header::IF_NONE_MATCH,
            HeaderValue::from_static("\"x\", W/\"abc\""),
        );
        assert!(none_match(&headers, tag));
        headers.insert(header::RANGE, HeaderValue::from_static("bytes=0-1"));
        assert_eq!(range_header(&headers, tag), Some("bytes=0-1"));
        headers.insert(header::IF_RANGE, HeaderValue::from_static("\"other\""));
        assert_eq!(range_header(&headers, tag), None);
        headers.insert(header::IF_RANGE, HeaderValue::from_static("\"abc\""));
        assert_eq!(range_header(&headers, tag), Some("bytes=0-1"));
    }
}
