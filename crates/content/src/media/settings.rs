//! `[modules.content.media]` (section 9): the blob driver and its
//! `local.*` or `s3.*` table, serving, upload admission, the accepted and
//! inline types, the spool directory, caching and the cleanup schedule.
//! Every problem is reported at once with its dotted key.
use super::sniff::{TEXT_TYPES, active_type, valid_media_type};
use merezha_blob::{DriverConfig, LocalConfig, S3Config};
use merezha_platform::{
    ModuleSection, Secret,
    config::{ConfigIssue, Origin, parse_duration},
};
use std::{path::PathBuf, time::Duration};

/// How delivery serves asset bytes (`media.serve`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Serve {
    /// Stream from the blob store through the host (default).
    Proxy,
    /// Answer 302 to a presigned GET URL (S3 only).
    Redirect,
}

/// `[modules.content.media]`, validated.
#[derive(Clone, Debug)]
pub struct MediaSettings {
    /// `driver` with its `local.*` or `s3.*` table. `Debug` redacts the S3
    /// credentials.
    pub driver: DriverConfig,
    pub serve: Serve,
    /// Lifetime of a redirect's presigned URL, 60 s to 3600 s.
    pub signed_url_ttl: Duration,
    /// Largest upload, 1024 bytes to 5 GiB.
    pub max_upload_bytes: u64,
    /// Uploads in flight on this host, 1 to 64; beyond it `overloaded`.
    pub max_concurrent_uploads: u32,
    /// Bare MIME types an upload may declare.
    pub allowed_types: Vec<String>,
    /// Types served `inline`; a subset of `allowed_types` without HTML,
    /// SVG, XML or PDF.
    pub inline_types: Vec<String>,
    /// Where uploads are spooled, a private (0700) directory.
    pub spool_dir: PathBuf,
    /// `Cache-Control` of an asset delivered through a `public` type.
    pub asset_cache_control: String,
    /// How long an orphaned blob stays before `content.blob_cleanup`
    /// deletes it, 1 hour to 30 days.
    pub orphan_grace: Duration,
    /// The `content.blob_cleanup` schedule, five cron fields in UTC.
    pub cleanup_cron: String,
}

/// `media.allowed_types` when the key is absent.
pub const DEFAULT_ALLOWED_TYPES: [&str; 13] = [
    "image/png",
    "image/jpeg",
    "image/gif",
    "image/webp",
    "image/avif",
    "image/svg+xml",
    "application/pdf",
    "video/mp4",
    "video/webm",
    "audio/mpeg",
    "audio/ogg",
    "text/plain",
    "text/csv",
];
/// `media.inline_types` when the key is absent: the raster images. Plain
/// text is not inline by default, so an HTML file uploaded as text is an
/// attachment; listing `text/plain` serves such files inline as plain
/// text (with `nosniff` and the sandbox CSP).
pub const DEFAULT_INLINE_TYPES: [&str; 5] = [
    "image/png",
    "image/jpeg",
    "image/gif",
    "image/webp",
    "image/avif",
];
/// `media.max_upload_bytes` when the key is absent: 50 MiB.
pub const DEFAULT_MAX_UPLOAD_BYTES: u64 = 52_428_800;
/// `media.cleanup_cron` when the key is absent.
pub const DEFAULT_CLEANUP_CRON: &str = "47 3 * * *";
/// `media.asset_cache_control` when the key is absent.
pub const DEFAULT_ASSET_CACHE_CONTROL: &str = "public, max-age=3600";
/// Most entries of `allowed_types` or `inline_types`.
const MAX_TYPES: usize = 64;

const MEDIA_KEYS: [&str; 13] = [
    "driver",
    "local",
    "s3",
    "serve",
    "signed_url_ttl",
    "max_upload_bytes",
    "max_concurrent_uploads",
    "allowed_types",
    "inline_types",
    "spool_dir",
    "asset_cache_control",
    "orphan_grace",
    "cleanup_cron",
];
const LOCAL_KEYS: [&str; 1] = ["root"];
const S3_KEYS: [&str; 9] = [
    "bucket",
    "region",
    "endpoint",
    "path_style",
    "allow_http",
    "access_key_id",
    "secret_access_key",
    "timeout",
    "key_prefix",
];

/// Typed reads from one table of the section, collecting issues.
struct Reader<'a> {
    prefix: String,
    table: &'a toml::Table,
    issues: &'a mut Vec<ConfigIssue>,
}
impl Reader<'_> {
    fn key(&self, key: &str) -> String {
        format!("{}.{key}", self.prefix)
    }
    fn issue(&mut self, key: &str, code: &'static str) {
        let key = self.key(key);
        self.issues.push(ConfigIssue::new(key, code, Origin::File));
    }
    fn unknown(&mut self, known: &[&str]) {
        let unknown: Vec<String> = self
            .table
            .keys()
            .filter(|k| !known.contains(&k.as_str()))
            .cloned()
            .collect();
        for key in unknown {
            let key = self.key(&key);
            self.issues
                .push(ConfigIssue::new(key, "unknown_key", Origin::File));
        }
    }
    fn string(&mut self, key: &str) -> Option<String> {
        match self.table.get(key)? {
            toml::Value::String(value) => Some(value.clone()),
            _ => {
                self.issue(key, "expected_string");
                None
            }
        }
    }
    fn boolean(&mut self, key: &str, default: bool) -> bool {
        match self.table.get(key) {
            None => default,
            Some(toml::Value::Boolean(value)) => *value,
            Some(_) => {
                self.issue(key, "expected_boolean");
                default
            }
        }
    }
    fn integer(&mut self, key: &str, default: u64, (min, max): (u64, u64)) -> u64 {
        match self.table.get(key) {
            None => default,
            Some(toml::Value::Integer(n)) => match u64::try_from(*n) {
                Ok(n) if (min..=max).contains(&n) => n,
                _ => {
                    self.issue(key, "out_of_range");
                    default
                }
            },
            Some(_) => {
                self.issue(key, "expected_integer");
                default
            }
        }
    }
    fn duration(
        &mut self,
        key: &str,
        default: Duration,
        (min, max): (Duration, Duration),
    ) -> Duration {
        match self.table.get(key) {
            None => default,
            Some(value) => match value.as_str().and_then(parse_duration) {
                Some(d) if (min..=max).contains(&d) => d,
                Some(_) => {
                    self.issue(key, "out_of_range");
                    default
                }
                None => {
                    self.issue(key, "expected_duration");
                    default
                }
            },
        }
    }
    /// A list of bare lowercase MIME types, each once.
    fn types(&mut self, key: &str, default: &[&str]) -> Vec<String> {
        let Some(value) = self.table.get(key) else {
            return default.iter().map(|t| (*t).to_owned()).collect();
        };
        let Some(items) = value.as_array() else {
            self.issue(key, "expected_string_list");
            return Vec::new();
        };
        if items.len() > MAX_TYPES {
            self.issue(key, "too_many_items");
            return Vec::new();
        }
        let mut out: Vec<String> = Vec::new();
        for item in items {
            match item.as_str() {
                Some(t) if valid_media_type(t) && !out.iter().any(|o| o == t) => {
                    out.push(t.to_owned());
                }
                Some(t) if valid_media_type(t) => self.issue(key, "duplicate_name"),
                Some(_) => self.issue(key, "invalid_media_type"),
                None => self.issue(key, "expected_string_list"),
            }
        }
        out
    }
    fn table(&mut self, key: &str) -> Option<&toml::Table> {
        match self.table.get(key)? {
            toml::Value::Table(table) => Some(table),
            _ => {
                self.issue(key, "expected_table");
                None
            }
        }
    }
}

/// Header-safe printable ASCII, 1 to 256 bytes.
fn header_safe(value: &str) -> bool {
    (1..=256).contains(&value.len()) && value.bytes().all(|b| (0x20..0x7f).contains(&b))
}

/// An upload type the sniffer can confirm: a binary signature `infer`
/// knows, or one of the text types checked as UTF-8.
fn sniffable(media_type: &str) -> bool {
    TEXT_TYPES.contains(&media_type)
        || (infer::is_mime_supported(media_type) && !media_type.starts_with("text/"))
}

impl MediaSettings {
    /// Read `[modules.content.media]`: `None` when absent (media off).
    pub(crate) fn parse(
        section: &ModuleSection,
        issues: &mut Vec<ConfigIssue>,
    ) -> Option<MediaSettings> {
        let value = section.table.get("media")?;
        let prefix = section.key("media");
        let Some(table) = value.as_table() else {
            issues.push(ConfigIssue::new(prefix, "expected_table", Origin::File));
            return None;
        };
        let mut r = Reader {
            prefix: prefix.clone(),
            table,
            issues,
        };
        r.unknown(&MEDIA_KEYS);
        let driver_name = r.string("driver");
        let local = r.table("local").cloned();
        let s3 = r.table("s3").cloned();
        let driver = match driver_name.as_deref() {
            None | Some("local") => {
                if s3.is_some() {
                    r.issue("s3", "unused_driver_settings");
                }
                DriverConfig::Local(local_config(&prefix, local.as_ref(), r.issues))
            }
            Some("s3") => {
                if local.is_some() {
                    r.issue("local", "unused_driver_settings");
                }
                DriverConfig::S3(s3_config(section, &prefix, s3.as_ref(), r.issues))
            }
            Some(_) => {
                r.issue("driver", "invalid_choice");
                DriverConfig::Local(LocalConfig::new(PathBuf::new()))
            }
        };
        let serve = match r.string("serve").as_deref() {
            None | Some("proxy") => Serve::Proxy,
            Some("redirect") => {
                // The local driver signs no URLs (section 6).
                if !matches!(driver, DriverConfig::S3(_)) {
                    r.issue("serve", "media_config_invalid");
                }
                Serve::Redirect
            }
            Some(_) => {
                r.issue("serve", "invalid_choice");
                Serve::Proxy
            }
        };
        let signed_url_ttl = r.duration(
            "signed_url_ttl",
            Duration::from_secs(300),
            (Duration::from_secs(60), Duration::from_secs(3600)),
        );
        let max_upload_bytes = r.integer(
            "max_upload_bytes",
            DEFAULT_MAX_UPLOAD_BYTES,
            (1024, merezha_blob::MAX_BLOB_BYTES),
        );
        let max_concurrent_uploads = r.integer("max_concurrent_uploads", 4, (1, 64)) as u32;
        let allowed_types = r.types("allowed_types", &DEFAULT_ALLOWED_TYPES);
        for media_type in &allowed_types {
            if !sniffable(media_type) {
                r.issue("allowed_types", "media_type_unsupported");
                break;
            }
        }
        let inline_types = r.types("inline_types", &DEFAULT_INLINE_TYPES);
        if inline_types.iter().any(|t| active_type(t)) {
            r.issue("inline_types", "inline_type_refused");
        } else if inline_types.iter().any(|t| !allowed_types.contains(t)) {
            r.issue("inline_types", "inline_type_not_allowed");
        }
        let spool_dir = match r.string("spool_dir") {
            None => std::env::temp_dir().join("merezha-content"),
            Some(path) if PathBuf::from(&path).is_absolute() && path.len() <= 4096 => {
                PathBuf::from(path)
            }
            Some(_) => {
                r.issue("spool_dir", "invalid_path");
                PathBuf::new()
            }
        };
        let asset_cache_control = match r.string("asset_cache_control") {
            None => DEFAULT_ASSET_CACHE_CONTROL.to_owned(),
            Some(value) if header_safe(&value) => value,
            Some(_) => {
                r.issue("asset_cache_control", "invalid_header_value");
                String::new()
            }
        };
        let orphan_grace = r.duration(
            "orphan_grace",
            Duration::from_secs(86_400),
            (Duration::from_secs(3600), Duration::from_secs(30 * 86_400)),
        );
        let cleanup_cron = match r.string("cleanup_cron") {
            None => DEFAULT_CLEANUP_CRON.to_owned(),
            Some(cron) if crate::settings::valid_cron(&cron) => cron,
            Some(_) => {
                r.issue("cleanup_cron", "invalid_cron");
                String::new()
            }
        };
        Some(MediaSettings {
            driver,
            serve,
            signed_url_ttl,
            max_upload_bytes,
            max_concurrent_uploads,
            allowed_types,
            inline_types,
            spool_dir,
            asset_cache_control,
            orphan_grace,
            cleanup_cron,
        })
    }
    /// Whether delivery and the redirect attributes serve `media_type`
    /// inline.
    pub fn inline(&self, media_type: &str) -> bool {
        self.inline_types.iter().any(|t| t == media_type)
    }
}

/// `media.local.*`.
fn local_config(
    prefix: &str,
    table: Option<&toml::Table>,
    issues: &mut Vec<ConfigIssue>,
) -> LocalConfig {
    let key = format!("{prefix}.local");
    let Some(table) = table else {
        issues.push(ConfigIssue::new(
            format!("{key}.root"),
            "missing_required",
            Origin::File,
        ));
        return LocalConfig::new(PathBuf::new());
    };
    let mut r = Reader {
        prefix: key.clone(),
        table,
        issues,
    };
    r.unknown(&LOCAL_KEYS);
    let config = match r.string("root") {
        Some(root) => LocalConfig::new(root),
        None => {
            if !table.contains_key("root") {
                r.issue("root", "missing_required");
            }
            return LocalConfig::new(PathBuf::new());
        }
    };
    for (field, code) in config.validate() {
        r.issue(field, code);
    }
    config
}

/// `media.s3.*`, credentials through nested secret references.
fn s3_config(
    section: &ModuleSection,
    prefix: &str,
    table: Option<&toml::Table>,
    issues: &mut Vec<ConfigIssue>,
) -> S3Config {
    let key = format!("{prefix}.s3");
    let empty = toml::Table::new();
    let present = table.is_some();
    let table = table.unwrap_or(&empty);
    let mut r = Reader {
        prefix: key,
        table,
        issues,
    };
    r.unknown(&S3_KEYS);
    let secret = |r: &mut Reader<'_>, name: &str| -> Secret {
        match section.secret_at(&["media", "s3", name]) {
            Ok(Some(secret)) => secret,
            Ok(None) => Secret::new(""),
            Err(issue) => {
                r.issues.push(issue);
                // Already reported: keep validation from adding
                // `missing_required` for the same key.
                Secret::new("unresolved")
            }
        }
    };
    let access_key_id = secret(&mut r, "access_key_id");
    let secret_access_key = secret(&mut r, "secret_access_key");
    let mut config = S3Config::new(
        r.string("bucket").unwrap_or_default(),
        r.string("region").unwrap_or_default(),
        access_key_id,
        secret_access_key,
    );
    config.endpoint = r.string("endpoint");
    config.path_style = r.boolean("path_style", true);
    config.allow_http = r.boolean("allow_http", false);
    // Capped below the driver's own range so an object delete that gets
    // no answer ends within half the delete lease.
    config.timeout = r.duration(
        "timeout",
        merezha_blob::DEFAULT_S3_TIMEOUT,
        (merezha_blob::S3_TIMEOUT_RANGE.0, super::MAX_S3_TIMEOUT),
    );
    config.key_prefix = r.string("key_prefix").unwrap_or_default();
    if !present {
        r.issues.push(ConfigIssue::new(
            r.key("bucket"),
            "missing_required",
            Origin::File,
        ));
        return config;
    }
    for (field, code) in config.validate() {
        r.issue(field, code);
    }
    config
}
