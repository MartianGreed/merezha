//! Typed driver settings (CT §9, `[modules.content.media]`).
//!
//! The content block reads its TOML section into these types; this crate
//! validates them and opens the store. Issues name the key relative to the
//! driver table (`root`, `bucket`, ...) so the caller can prefix its own
//! path (`modules.content.media.s3.bucket`).
use merezha_platform::{Secret, ports::BlobKey};
use std::{path::PathBuf, time::Duration};

/// Default per-request timeout of the S3 driver (`media.s3.timeout`).
pub const DEFAULT_S3_TIMEOUT: Duration = Duration::from_secs(30);
/// Shortest and longest accepted `timeout`.
pub const S3_TIMEOUT_RANGE: (Duration, Duration) =
    (Duration::from_secs(1), Duration::from_secs(300));
/// Longest `key_prefix`.
pub const MAX_KEY_PREFIX_BYTES: usize = 256;
/// Longest `endpoint`.
const MAX_ENDPOINT_BYTES: usize = 2048;

/// Which driver to open.
#[derive(Clone, Debug)]
pub enum DriverConfig {
    Local(LocalConfig),
    S3(S3Config),
}

/// `media.local.*`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalConfig {
    /// Absolute. Created with mode 0700 if missing; refused when writable by
    /// group or others.
    pub root: PathBuf,
}
impl LocalConfig {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }
    /// Every problem as `(key, code)`.
    pub fn validate(&self) -> Vec<(&'static str, &'static str)> {
        if self.root.is_absolute() {
            Vec::new()
        } else {
            vec![("root", "invalid_path")]
        }
    }
}

/// `media.s3.*`. `Debug` redacts both credentials.
#[derive(Clone, Debug)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    /// `None` targets AWS (`https://s3.<region>.amazonaws.com`).
    pub endpoint: Option<String>,
    /// Path-style requests (`<endpoint>/<bucket>/<key>`); virtual-hosted
    /// style when `false`.
    pub path_style: bool,
    /// Must be `true` for a plain `http://` endpoint.
    pub allow_http: bool,
    pub access_key_id: Secret,
    pub secret_access_key: Secret,
    /// 1 s to 300 s. Bounds each request; an object read (`get`) is bounded
    /// per wait for its next bytes instead, so a proxied download of a
    /// large file may take longer while it keeps moving.
    pub timeout: Duration,
    /// Prepended to every key. Empty, or key segments; a missing trailing
    /// `/` is added, so `tenant-a` stores `tenant-a/<key>`.
    pub key_prefix: String,
}
impl S3Config {
    /// The required settings with every default from CT §9.
    pub fn new(
        bucket: impl Into<String>,
        region: impl Into<String>,
        access_key_id: Secret,
        secret_access_key: Secret,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            region: region.into(),
            endpoint: None,
            path_style: true,
            allow_http: false,
            access_key_id,
            secret_access_key,
            timeout: DEFAULT_S3_TIMEOUT,
            key_prefix: String::new(),
        }
    }
    /// Every problem as `(key, code)`, in a stable order.
    pub fn validate(&self) -> Vec<(&'static str, &'static str)> {
        let mut issues = Vec::new();
        if self.bucket.is_empty() {
            issues.push(("bucket", "missing_required"));
        } else if !valid_bucket(&self.bucket) {
            issues.push(("bucket", "invalid_name"));
        }
        if self.region.is_empty() {
            issues.push(("region", "missing_required"));
        } else if !valid_region(&self.region) {
            issues.push(("region", "invalid_name"));
        }
        if let Some(endpoint) = &self.endpoint {
            match endpoint_scheme(endpoint) {
                None => issues.push(("endpoint", "invalid_endpoint")),
                Some(Scheme::Http) if !self.allow_http => {
                    issues.push(("endpoint", "plaintext_endpoint_refused"));
                }
                Some(_) => {}
            }
        }
        if self.access_key_id.expose().is_empty() {
            issues.push(("access_key_id", "missing_required"));
        }
        if self.secret_access_key.expose().is_empty() {
            issues.push(("secret_access_key", "missing_required"));
        }
        let (min, max) = S3_TIMEOUT_RANGE;
        if self.timeout < min || self.timeout > max {
            issues.push(("timeout", "out_of_range"));
        }
        if normalized_prefix(&self.key_prefix).is_none() {
            issues.push(("key_prefix", "invalid_key_prefix"));
        }
        issues
    }
}

/// `""`, or the prefix with exactly one trailing `/`; `None` when invalid.
pub(crate) fn normalized_prefix(prefix: &str) -> Option<String> {
    if prefix.is_empty() {
        return Some(String::new());
    }
    let trimmed = prefix.strip_suffix('/').unwrap_or(prefix);
    (prefix.len() <= MAX_KEY_PREFIX_BYTES && BlobKey::new(trimmed).is_ok())
        .then(|| format!("{trimmed}/"))
}

/// S3 bucket naming: 3 to 63 bytes of lowercase letters, digits, `.` and
/// `-`, starting and ending with a letter or digit.
fn valid_bucket(bucket: &str) -> bool {
    let bytes = bucket.as_bytes();
    (3..=63).contains(&bytes.len())
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b".-".contains(b))
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
}

fn valid_region(region: &str) -> bool {
    (1..=64).contains(&region.len())
        && region
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Scheme {
    Http,
    Https,
}

/// `http(s)://host[:port][/path]`: printable ASCII, a host, no user info,
/// query or fragment.
fn endpoint_scheme(endpoint: &str) -> Option<Scheme> {
    if endpoint.len() > MAX_ENDPOINT_BYTES
        || !endpoint.bytes().all(|b| b.is_ascii_graphic())
        || endpoint.contains(['?', '#', '@', '\\'])
    {
        return None;
    }
    let (scheme, rest) = match endpoint.strip_prefix("https://") {
        Some(rest) => (Scheme::Https, rest),
        None => (Scheme::Http, endpoint.strip_prefix("http://")?),
    };
    let authority = rest.split('/').next().unwrap_or_default();
    let (host, port) = match authority.strip_prefix('[') {
        // An IPv6 literal: `[addr]` or `[addr]:port`.
        Some(v6) => {
            let (addr, after) = v6.split_once(']')?;
            let port = match after {
                "" => "",
                _ => after.strip_prefix(':').filter(|p| !p.is_empty())?,
            };
            (addr, port)
        }
        None => match authority.split_once(':') {
            Some((host, port)) => (host, port),
            None => (authority, ""),
        },
    };
    let port_ok = port.is_empty() && !authority.ends_with(':')
        || (1..=5).contains(&port.len()) && port.bytes().all(|b| b.is_ascii_digit());
    (!host.is_empty() && port_ok).then_some(scheme)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s3() -> S3Config {
        S3Config::new(
            "media",
            "us-east-1",
            Secret::new("AKID"),
            Secret::new("secret"),
        )
    }

    #[test]
    fn defaults_follow_the_design_table() {
        let config = s3();
        assert!(config.path_style);
        assert!(!config.allow_http);
        assert_eq!(config.timeout, Duration::from_secs(30));
        assert_eq!(config.endpoint, None);
        assert!(config.validate().is_empty());
    }

    #[test]
    fn debug_never_shows_credentials() {
        let mut config = s3();
        config.secret_access_key = Secret::new("hunter2-very-secret");
        config.access_key_id = Secret::new("AKIDVISIBLE");
        let debug = format!("{config:?}");
        assert!(!debug.contains("hunter2"), "{debug}");
        assert!(!debug.contains("AKIDVISIBLE"), "{debug}");
    }

    #[test]
    fn every_problem_is_reported_at_once() {
        let mut config = s3();
        config.bucket = "Bad_Bucket".into();
        config.region = String::new();
        config.endpoint = Some("http://127.0.0.1:9000".into());
        config.timeout = Duration::from_millis(500);
        config.key_prefix = "a//b".into();
        config.secret_access_key = Secret::new("");
        assert_eq!(
            config.validate(),
            vec![
                ("bucket", "invalid_name"),
                ("region", "missing_required"),
                ("endpoint", "plaintext_endpoint_refused"),
                ("secret_access_key", "missing_required"),
                ("timeout", "out_of_range"),
                ("key_prefix", "invalid_key_prefix"),
            ]
        );
    }

    #[test]
    fn endpoints_accept_hosts_and_refuse_query_fragment_and_userinfo() {
        for good in [
            "https://s3.example.com",
            "https://s3.example.com/",
            "http://127.0.0.1:9000",
            "https://abc.r2.cloudflarestorage.com",
            "http://[::1]:9000",
        ] {
            assert!(endpoint_scheme(good).is_some(), "{good}");
        }
        for bad in [
            "",
            "s3.example.com",
            "ftp://host",
            "https://",
            "https://host?x=1",
            "https://host#f",
            "https://user:pw@host",
            "https://host:",
            "https://host:99999x",
            "https://ho st",
        ] {
            assert!(endpoint_scheme(bad).is_none(), "{bad}");
        }
        let mut config = s3();
        config.endpoint = Some("http://127.0.0.1:9000".into());
        config.allow_http = true;
        assert!(config.validate().is_empty());
    }

    #[test]
    fn timeouts_are_bounded() {
        let mut config = s3();
        for (secs, ok) in [(1, true), (300, true), (0, false), (301, false)] {
            config.timeout = Duration::from_secs(secs);
            assert_eq!(config.validate().is_empty(), ok, "{secs}");
        }
    }

    #[test]
    fn key_prefixes_are_key_segments() {
        assert_eq!(normalized_prefix("").as_deref(), Some(""));
        assert_eq!(normalized_prefix("tenant-a").as_deref(), Some("tenant-a/"));
        assert_eq!(normalized_prefix("a/b/").as_deref(), Some("a/b/"));
        for bad in ["/", "a//", "../a", "a b", "a/./b", &"x".repeat(257)] {
            assert_eq!(normalized_prefix(bad), None, "{bad}");
        }
    }

    #[test]
    fn buckets_follow_s3_naming() {
        for good in ["abc", "media-1", "my.bucket", &"a".repeat(63)] {
            assert!(valid_bucket(good), "{good}");
        }
        for bad in ["ab", "-abc", "abc-", "ABC", "a_b", &"a".repeat(64)] {
            assert!(!valid_bucket(bad), "{bad}");
        }
    }

    #[test]
    fn local_roots_must_be_absolute() {
        assert!(LocalConfig::new("/srv/media").validate().is_empty());
        assert_eq!(
            LocalConfig::new("media").validate(),
            vec![("root", "invalid_path")]
        );
    }
}
