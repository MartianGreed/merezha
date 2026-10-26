//! Endpoint URLs (NT §5.5, §5.6). Registration accepts a URL when its
//! scheme is `https` (or `http` with `webhooks.allow_http`), it has no
//! userinfo and no fragment, it is at most 2,048 characters long, its host
//! is not a blocked IP literal, and its host resolves to at least one
//! address the egress guard allows. That last check only gives early
//! feedback: the one that counts runs at send time, in the guarded
//! resolver, on the addresses actually dialled.
use crate::egress::{EgressPolicy, GuardedResolver, ResolveError};
use std::time::Duration;

/// Most characters of an endpoint URL.
pub const MAX_URL: usize = 2048;
/// The registration lookup gives up after this long (`unresolvable`).
pub const LOOKUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Why a URL was refused: the `details` code of `webhook_url_rejected`.
pub mod reason {
    pub const SCHEME: &str = "scheme";
    pub const USERINFO: &str = "userinfo";
    pub const FRAGMENT: &str = "fragment";
    pub const BLOCKED_ADDRESS: &str = "blocked_address";
    pub const UNRESOLVABLE: &str = "unresolvable";
    pub const TOO_LONG: &str = "too_long";
    pub const INVALID_URL: &str = "invalid_url";
}

/// The checks that need no lookup, at registration and again at send:
/// the parsed URL, or the reason it was refused.
pub fn parse(text: &str, allow_http: bool) -> Result<url::Url, &'static str> {
    if text.chars().count() > MAX_URL {
        return Err(reason::TOO_LONG);
    }
    let url = url::Url::parse(text).map_err(|_| reason::INVALID_URL)?;
    if url.as_str().chars().count() > MAX_URL {
        return Err(reason::TOO_LONG);
    }
    match url.scheme() {
        "https" => {}
        "http" if allow_http => {}
        _ => return Err(reason::SCHEME),
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(reason::USERINFO);
    }
    if url.fragment().is_some() {
        return Err(reason::FRAGMENT);
    }
    if url.host().is_none() {
        return Err(reason::INVALID_URL);
    }
    Ok(url)
}

/// The registration check: [`parse`], a blocked IP literal refused, and a
/// name that resolves to at least one allowed address within
/// [`LOOKUP_TIMEOUT`].
pub async fn check(
    text: &str,
    allow_http: bool,
    policy: &EgressPolicy,
    resolver: &GuardedResolver,
) -> Result<url::Url, &'static str> {
    let url = parse(text, allow_http)?;
    let host = url.host().ok_or(reason::INVALID_URL)?;
    if policy.check_host(&host).is_err() {
        return Err(reason::BLOCKED_ADDRESS);
    }
    if let url::Host::Domain(name) = host
        && name.parse::<std::net::IpAddr>().is_err()
    {
        match tokio::time::timeout(LOOKUP_TIMEOUT, resolver.resolve_host(name)).await {
            Ok(Ok(_)) => {}
            Ok(Err(ResolveError::Blocked(_))) => return Err(reason::BLOCKED_ADDRESS),
            Ok(Err(ResolveError::Lookup(_))) | Err(_) => return Err(reason::UNRESOLVABLE),
        }
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn static_checks_name_their_reason() {
        assert!(parse("https://hooks.example.com/in", false).is_ok());
        assert_eq!(
            parse("http://hooks.example.com/", false),
            Err(reason::SCHEME)
        );
        assert!(parse("http://hooks.example.com/", true).is_ok());
        assert_eq!(parse("ftp://hooks.example.com/", true), Err(reason::SCHEME));
        assert_eq!(
            parse("https://user:pw@hooks.example.com/", false),
            Err(reason::USERINFO)
        );
        assert_eq!(
            parse("https://hooks.example.com/#x", false),
            Err(reason::FRAGMENT)
        );
        assert_eq!(
            parse(&format!("https://h.example/{}", "a".repeat(2048)), false),
            Err(reason::TOO_LONG)
        );
        assert_eq!(parse("not a url", false), Err(reason::INVALID_URL));
    }
}
