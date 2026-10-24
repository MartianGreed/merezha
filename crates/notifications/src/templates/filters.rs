//! The block's template filters (NT §4): `url`, `datetime(format)` and
//! `truncate_chars(n)`. Failures carry a fixed detail so the render error
//! code is stable and no input is echoed.
use minijinja::{Environment, Error, ErrorKind, State, Value};

/// Detail of a `url` refusal.
pub const URL_REJECTED: &str = "url_rejected";
/// Detail of a `datetime` refusal.
pub const DATETIME_REJECTED: &str = "datetime_rejected";
/// Default `datetime` format.
pub const DEFAULT_DATETIME_FORMAT: &str = "%Y-%m-%d %H:%M %Z";

/// Whether `text` is a link templates may emit: `https:` with a host, or
/// `http:` to a loopback host when `allow_http` (`templates.allow_http_links`).
pub fn link_allowed(text: &str, allow_http: bool) -> bool {
    if text.chars().any(|c| c.is_control() || c.is_whitespace()) {
        return false;
    }
    let Ok(url) = url::Url::parse(text) else {
        return false;
    };
    let Some(host) = url.host() else {
        return false;
    };
    match url.scheme() {
        "https" => true,
        "http" => allow_http && crate::config::loopback_host(&host),
        _ => false,
    }
}

/// Add the block's filters to `env`.
pub fn register(env: &mut Environment<'static>, allow_http_links: bool) {
    env.add_filter("url", move |value: String| -> Result<String, Error> {
        if link_allowed(&value, allow_http_links) {
            Ok(value)
        } else {
            Err(Error::new(ErrorKind::InvalidOperation, URL_REJECTED))
        }
    });
    env.add_filter("datetime", datetime);
    env.add_filter("truncate_chars", truncate_chars);
}

/// Format an RFC 3339 string or Unix seconds in the recipient's timezone
/// (`recipient.timezone`, UTC when unknown) with a jiff `strftime` format.
fn datetime(state: &State, value: Value, format: Option<String>) -> Result<String, Error> {
    let rejected = || Error::new(ErrorKind::InvalidOperation, DATETIME_REJECTED);
    let timestamp = if let Some(text) = value.as_str() {
        text.parse::<jiff::Timestamp>().map_err(|_| rejected())?
    } else if let Some(seconds) = value.as_i64() {
        jiff::Timestamp::from_second(seconds).map_err(|_| rejected())?
    } else {
        return Err(rejected());
    };
    let zone = state
        .lookup("recipient")
        .and_then(|r| r.get_attr("timezone").ok())
        .and_then(|tz| {
            tz.as_str()
                .and_then(|name| jiff::tz::TimeZone::get(name).ok())
        })
        .unwrap_or(jiff::tz::TimeZone::UTC);
    let format = format.as_deref().unwrap_or(DEFAULT_DATETIME_FORMAT);
    if format.len() > 128 {
        return Err(rejected());
    }
    let zoned = timestamp.to_zoned(zone);
    jiff::fmt::strtime::format(format, &zoned).map_err(|_| rejected())
}

/// The first `n` characters, cut at a character boundary.
fn truncate_chars(value: String, n: usize) -> String {
    match value.char_indices().nth(n) {
        Some((at, _)) => value[..at].to_owned(),
        None => value,
    }
}
