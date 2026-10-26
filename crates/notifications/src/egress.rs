//! The egress guard (NT §5.6, invariant 10): requests to URLs a browser or a
//! tenant supplied (push endpoints here, webhook URLs with NT-N7) go only to
//! addresses that passed the guard, with redirects and proxies disabled.
//!
//! The guard sits in the client's DNS resolver ([`GuardedResolver`]):
//! reqwest dials only the addresses the resolver returned, so the checked
//! set and the dialled set are the same and no second lookup can rebind the
//! name between the check and the connect. IP literals skip the resolver,
//! so [`EgressPolicy::check_host`] checks them before a request is built.
use crate::config::Egress;
use merezha_domain::Failure;
use merezha_platform::{BoxFuture, net::Cidr};
use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

/// The connect timeout of the guarded client.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
/// Most addresses one lookup may hand to the client.
const MAX_ADDRESSES: usize = 16;

/// The error code of a refused destination.
pub const EGRESS_BLOCKED: &str = "egress_blocked";

/// IPv4 ranges never dialled (NT §5.6).
const BLOCKED_V4: [(Ipv4Addr, u8); 15] = [
    (Ipv4Addr::new(0, 0, 0, 0), 8),
    (Ipv4Addr::new(10, 0, 0, 0), 8),
    (Ipv4Addr::new(100, 64, 0, 0), 10),
    (Ipv4Addr::new(127, 0, 0, 0), 8),
    (Ipv4Addr::new(169, 254, 0, 0), 16),
    (Ipv4Addr::new(172, 16, 0, 0), 12),
    (Ipv4Addr::new(192, 0, 0, 0), 24),
    (Ipv4Addr::new(192, 0, 2, 0), 24),
    (Ipv4Addr::new(192, 88, 99, 0), 24),
    (Ipv4Addr::new(192, 168, 0, 0), 16),
    (Ipv4Addr::new(198, 18, 0, 0), 15),
    (Ipv4Addr::new(198, 51, 100, 0), 24),
    (Ipv4Addr::new(203, 0, 113, 0), 24),
    (Ipv4Addr::new(224, 0, 0, 0), 4),
    (Ipv4Addr::new(240, 0, 0, 0), 4),
];
/// IPv6 ranges never dialled (NT §5.6). `::/128` and `::1/128` are covered
/// by the IPv4-compatible range `::/96`, checked by its embedded address.
const BLOCKED_V6: [(u128, u8); 7] = [
    (0x0100_0000_0000_0000_0000_0000_0000_0000, 64),
    (0x2001_0000_0000_0000_0000_0000_0000_0000, 23),
    (0x2001_0db8_0000_0000_0000_0000_0000_0000, 32),
    (0xfc00_0000_0000_0000_0000_0000_0000_0000, 7),
    (0xfe80_0000_0000_0000_0000_0000_0000_0000, 10),
    (0xfec0_0000_0000_0000_0000_0000_0000_0000, 10),
    (0xff00_0000_0000_0000_0000_0000_0000_0000, 8),
];

fn in_v4(address: Ipv4Addr, (network, prefix): (Ipv4Addr, u8)) -> bool {
    let mask = u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0);
    u32::from(address) & mask == u32::from(network) & mask
}
fn in_v6(address: u128, (network, prefix): (u128, u8)) -> bool {
    let mask = u128::MAX.checked_shl(128 - u32::from(prefix)).unwrap_or(0);
    address & mask == network & mask
}

/// The IPv4 address an IPv6 address carries, for the ranges NT §5.6 checks
/// by their embedded address: IPv4-mapped `::ffff:0:0/96`, IPv4-compatible
/// `::/96`, NAT64 `64:ff9b::/96` and `64:ff9b:1::/48`, and 6to4 `2002::/16`.
fn embedded_v4(address: Ipv6Addr) -> Option<Ipv4Addr> {
    let bits = u128::from(address);
    let low = Ipv4Addr::from((bits & 0xffff_ffff) as u32);
    if in_v6(bits, (0xffff_0000_0000, 96))
        || in_v6(bits, (0, 96))
        || in_v6(bits, (0x0064_ff9b_0000_0000_0000_0000_0000_0000, 96))
        || in_v6(bits, (0x0064_ff9b_0001_0000_0000_0000_0000_0000, 48))
    {
        return Some(low);
    }
    if in_v6(bits, (0x2002_0000_0000_0000_0000_0000_0000_0000, 16)) {
        return Some(Ipv4Addr::from(((bits >> 80) & 0xffff_ffff) as u32));
    }
    None
}

/// Whether `address` lies in a range NT §5.6 blocks.
pub fn blocked(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(v4) => BLOCKED_V4.iter().any(|range| in_v4(v4, *range)),
        IpAddr::V6(v6) => match embedded_v4(v6) {
            Some(v4) => BLOCKED_V4.iter().any(|range| in_v4(v4, *range)),
            None => {
                let bits = u128::from(v6);
                BLOCKED_V6.iter().any(|range| in_v6(bits, *range))
            }
        },
    }
}

/// Which addresses the guarded client may dial.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EgressPolicy {
    /// `egress.allow_private_networks` (development and tests; refused with
    /// `http.public = true`): lifts the blocked ranges.
    pub allow_private_networks: bool,
    /// `egress.deny_cidrs`: always refused, private networks allowed or not.
    pub deny_cidrs: Vec<Cidr>,
    /// `MEREZHA_NOTIFY_NO_EGRESS=1` (CI): loopback only.
    pub loopback_only: bool,
}
impl EgressPolicy {
    pub fn new(egress: &Egress, no_egress: bool) -> Self {
        Self {
            allow_private_networks: egress.allow_private_networks,
            deny_cidrs: egress.deny_cidrs.clone(),
            loopback_only: no_egress,
        }
    }
    /// Whether the client may dial `address`.
    pub fn allows(&self, address: IpAddr) -> bool {
        let address = address.to_canonical();
        if self.loopback_only && !address.is_loopback() {
            return false;
        }
        if self.deny_cidrs.iter().any(|cidr| cidr.contains(address)) {
            return false;
        }
        self.allow_private_networks || !blocked(address)
    }
    /// The check of a URL host before the request is built: an IP literal
    /// must be allowed, since it never reaches the resolver. A name is
    /// checked by the resolver at connect time.
    pub fn check_host(&self, host: &url::Host<&str>) -> Result<(), Failure> {
        let literal = match host {
            url::Host::Ipv4(v4) => Some(IpAddr::V4(*v4)),
            url::Host::Ipv6(v6) => Some(IpAddr::V6(*v6)),
            url::Host::Domain(name) => name.parse::<IpAddr>().ok(),
        };
        match literal {
            Some(address) if !self.allows(address) => Err(Failure::permanent(EGRESS_BLOCKED)),
            _ => Ok(()),
        }
    }
}

/// Name resolution under the guard: the system resolver in production,
/// `testkit::TestResolver` in tests (feature `testkit`).
pub trait Lookup: Send + Sync {
    fn lookup(&self, host: &str) -> BoxFuture<'static, std::io::Result<Vec<IpAddr>>>;
}

/// `tokio::net::lookup_host`.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemLookup;
impl Lookup for SystemLookup {
    fn lookup(&self, host: &str) -> BoxFuture<'static, std::io::Result<Vec<IpAddr>>> {
        let host = host.to_owned();
        Box::pin(async move {
            let found = tokio::net::lookup_host((host.as_str(), 0)).await?;
            Ok(found.map(|a| a.ip()).take(MAX_ADDRESSES * 4).collect())
        })
    }
}

/// The resolver error of a name whose every address the guard refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EgressBlocked;
impl std::fmt::Display for EgressBlocked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(EGRESS_BLOCKED)
    }
}
impl std::error::Error for EgressBlocked {}

/// Whether `error` (a reqwest error, through its sources) is the guard's
/// refusal.
pub fn is_blocked(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    let mut depth = 0;
    while let Some(error) = current {
        if error.is::<EgressBlocked>() {
            return true;
        }
        depth += 1;
        if depth > 16 {
            return false;
        }
        current = error.source();
    }
    false
}

/// `reqwest::dns::Resolve` over a [`Lookup`]: blocked addresses are
/// removed, and a name with none left fails with [`EgressBlocked`].
#[derive(Clone)]
pub struct GuardedResolver {
    policy: Arc<EgressPolicy>,
    lookup: Arc<dyn Lookup>,
}
impl GuardedResolver {
    pub fn new(policy: Arc<EgressPolicy>, lookup: Arc<dyn Lookup>) -> Self {
        Self { policy, lookup }
    }
    /// The addresses of `host` the client may dial, at most 16.
    pub async fn resolve_host(&self, host: &str) -> Result<Vec<IpAddr>, ResolveError> {
        let found = self
            .lookup
            .lookup(host)
            .await
            .map_err(ResolveError::Lookup)?;
        let allowed: Vec<IpAddr> = found
            .into_iter()
            .filter(|address| self.policy.allows(*address))
            .take(MAX_ADDRESSES)
            .collect();
        if allowed.is_empty() {
            Err(ResolveError::Blocked(EgressBlocked))
        } else {
            Ok(allowed)
        }
    }
}
/// A failed guarded lookup.
#[derive(Debug)]
pub enum ResolveError {
    Lookup(std::io::Error),
    Blocked(EgressBlocked),
}
impl reqwest::dns::Resolve for GuardedResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let resolver = self.clone();
        let host = name.as_str().to_owned();
        Box::pin(async move {
            match resolver.resolve_host(&host).await {
                Ok(addresses) => {
                    let addresses: reqwest::dns::Addrs = Box::new(
                        addresses
                            .into_iter()
                            .map(|address| SocketAddr::new(address, 0)),
                    );
                    Ok(addresses)
                }
                Err(ResolveError::Lookup(error)) => Err(Box::new(error) as _),
                Err(ResolveError::Blocked(blocked)) => Err(Box::new(blocked) as _),
            }
        })
    }
}

/// The guarded client push and webhooks share: no redirects, no proxy, the
/// guarded resolver, a 5 s connect timeout, at most 2 idle connections per
/// host and no automatic retries.
pub fn client(
    policy: Arc<EgressPolicy>,
    lookup: Arc<dyn Lookup>,
) -> Result<reqwest::Client, Failure> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .no_proxy()
        .dns_resolver(Arc::new(GuardedResolver::new(policy, lookup)))
        .connect_timeout(CONNECT_TIMEOUT)
        .pool_max_idle_per_host(2)
        .retry(reqwest::retry::never())
        .user_agent(concat!("merezha/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(|_| Failure::permanent("http_client_config"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ip(text: &str) -> IpAddr {
        text.parse().unwrap()
    }

    #[test]
    fn the_design_ranges_are_blocked_with_embedded_ipv4() {
        for address in [
            "0.1.2.3",
            "10.0.0.5",
            "100.64.0.1",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "172.31.255.255",
            "192.0.0.8",
            "192.0.2.1",
            "192.88.99.1",
            "192.168.1.1",
            "198.18.0.1",
            "198.51.100.1",
            "203.0.113.1",
            "224.0.0.1",
            "240.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "100::1",
            "2001::1",
            "2001:db8::1",
            "fc00::1",
            "fd12:3456::1",
            "fe80::1",
            "fec0::1",
            "ff02::1",
            "::ffff:127.0.0.1",
            "::ffff:10.0.0.5",
            "::127.0.0.1",
            "64:ff9b::a00:5",
            "64:ff9b:1::a00:5",
            "2002:a00:5::1",
            "2002:7f00:1::",
        ] {
            assert!(blocked(ip(address)), "{address} must be blocked");
        }
        for address in [
            "8.8.8.8",
            "142.250.0.1",
            "172.32.0.1",
            "2606:4700::1111",
            "::ffff:8.8.8.8",
            "64:ff9b::808:808",
            "2002:808:808::1",
        ] {
            assert!(!blocked(ip(address)), "{address} must be allowed");
        }
    }

    #[test]
    fn the_policy_combines_private_networks_deny_lists_and_no_egress() {
        let strict = EgressPolicy::new(&Egress::default(), false);
        assert!(strict.allows(ip("8.8.8.8")) && !strict.allows(ip("127.0.0.1")));
        let dev = EgressPolicy {
            allow_private_networks: true,
            deny_cidrs: vec!["10.0.0.0/8".parse().unwrap()],
            loopback_only: false,
        };
        assert!(dev.allows(ip("127.0.0.1")) && dev.allows(ip("192.168.0.1")));
        assert!(!dev.allows(ip("10.0.0.5")) && !dev.allows(ip("::ffff:10.0.0.5")));
        let ci = EgressPolicy {
            loopback_only: true,
            ..dev.clone()
        };
        assert!(ci.allows(ip("127.0.0.1")) && !ci.allows(ip("8.8.8.8")));
        let literal = |text: &str| url::Url::parse(text).unwrap();
        for url in [
            "https://127.0.0.1/",
            "https://[::ffff:127.0.0.1]/",
            "https://0x7f000001/",
        ] {
            let parsed = literal(url);
            assert_eq!(
                strict.check_host(&parsed.host().unwrap()),
                Err(Failure::permanent(EGRESS_BLOCKED)),
                "{url}"
            );
        }
        assert_eq!(
            strict.check_host(&literal("https://fcm.googleapis.com/").host().unwrap()),
            Ok(())
        );
    }

    struct Fixed(Vec<IpAddr>);
    impl Lookup for Fixed {
        fn lookup(&self, _: &str) -> BoxFuture<'static, std::io::Result<Vec<IpAddr>>> {
            let found = self.0.clone();
            Box::pin(async move { Ok(found) })
        }
    }

    #[tokio::test]
    async fn the_resolver_hands_over_only_allowed_addresses() {
        let policy = Arc::new(EgressPolicy::new(&Egress::default(), false));
        let mixed = GuardedResolver::new(
            policy.clone(),
            Arc::new(Fixed(vec![ip("10.0.0.5"), ip("8.8.8.8"), ip("::1")])),
        );
        assert_eq!(
            mixed.resolve_host("push.example").await.unwrap(),
            vec![ip("8.8.8.8")]
        );
        let private = GuardedResolver::new(policy, Arc::new(Fixed(vec![ip("10.0.0.5")])));
        assert!(matches!(
            private.resolve_host("push.example").await,
            Err(ResolveError::Blocked(_))
        ));
    }

    /// A name resolving only to a blocked address never connects, and the
    /// client's error says the guard refused it.
    #[tokio::test]
    async fn a_blocked_name_fails_before_connecting_with_the_guard_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let policy = Arc::new(EgressPolicy::new(&Egress::default(), false));
        let client = client(policy, Arc::new(Fixed(vec![ip("127.0.0.1")]))).unwrap();
        let error = client
            .post(format!("http://push.example:{port}/x"))
            .send()
            .await
            .unwrap_err();
        assert!(is_blocked(&error), "{error:?}");
        let accepted = tokio::time::timeout(Duration::from_millis(200), listener.accept()).await;
        assert!(accepted.is_err(), "the client connected");
    }
}
