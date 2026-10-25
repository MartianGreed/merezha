//! `[modules.identity]` (section 8) for PR 1 and PR 2, validated with every
//! problem reported at once. Tables of PR 3 (`oauth`, `passkeys`,
//! `links.sign_in_redirect`, `links.oauth_error`) stay `unknown_key` until
//! it gives them behaviour.
use crate::{
    catalog::{Catalog, Wildcards},
    gcra::{self, Rate},
    rbac::{DeclaredRole, RESERVED_ROLE_KEYS, valid_role_key},
};
use merezha_platform::{
    ConfigInput, ModuleSection, ProjectConfig, Secret,
    config::{ConfigIssue, Origin, parse_duration},
};
use std::{collections::BTreeMap, net::SocketAddr, time::Duration};

/// What identity needs to know about the host it runs in, which its own
/// section cannot say: the listener and the application name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HostFacts {
    pub listen: SocketAddr,
    pub public: bool,
    /// `http.trusted_proxies` is not empty: requests arrive from elsewhere
    /// even on a loopback listener.
    pub behind_proxy: bool,
    /// `app.name`, a context word of the password policy.
    pub app_name: Option<String>,
}
impl HostFacts {
    pub fn from_config(config: &ProjectConfig) -> Self {
        Self {
            listen: config.http.listen,
            public: config.http.public,
            behind_proxy: !config.http.trusted_proxies.is_empty(),
            app_name: Some(config.app.name.clone()),
        }
    }
    /// The facts of `input` when its kernel configuration validates. When
    /// it does not, the kernel reports why and identity's cross checks are
    /// skipped (startup fails either way).
    pub fn load(input: &ConfigInput) -> Option<Self> {
        ProjectConfig::load(input)
            .ok()
            .map(|config| Self::from_config(&config))
    }
    /// A private loopback listener: the only place the development cookie
    /// and `dev_print_links` are allowed.
    pub fn private_loopback(&self) -> bool {
        !self.public && !self.behind_proxy && self.listen.ip().is_loopback()
    }
    /// Facts for an embedded or test host listening on loopback.
    pub fn loopback() -> Self {
        Self {
            listen: SocketAddr::from(([127, 0, 0, 1], 8080)),
            public: false,
            behind_proxy: false,
            app_name: None,
        }
    }
}

/// `registration` (owner answer ID8).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Registration {
    Open,
    InviteOnly,
    Closed,
}

/// `[modules.identity.password]`; its presence enables password sign-in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PasswordSettings {
    pub min_length: usize,
    pub max_length: usize,
    /// The operator's extra blocklist, one password per line.
    pub blocklist: Option<String>,
    pub memory_kib: u32,
    pub iterations: u32,
    pub parallelism: u32,
    /// 0 is the available parallelism.
    pub hash_concurrency: usize,
    pub hash_queue_timeout: Duration,
}
impl Default for PasswordSettings {
    fn default() -> Self {
        Self {
            min_length: crate::password::DEFAULT_MIN_LENGTH,
            max_length: crate::password::MAX_LENGTH_CEILING,
            blocklist: None,
            memory_kib: 65_536,
            iterations: 2,
            parallelism: 1,
            hash_concurrency: 0,
            hash_queue_timeout: Duration::from_secs(2),
        }
    }
}
impl PasswordSettings {
    /// The Argon2 memory floor (OWASP minimum, 19 MiB).
    pub const MIN_MEMORY_KIB: u32 = 19_456;
    pub const MIN_ITERATIONS: u32 = 2;
}

/// `[modules.identity.sessions]` (owner answer ID3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SessionSettings {
    pub idle_timeout: Duration,
    pub absolute_timeout: Duration,
    pub reauthentication_window: Duration,
    pub touch_interval: Duration,
    pub max_per_user: u32,
    pub bearer: bool,
}
impl Default for SessionSettings {
    fn default() -> Self {
        Self {
            idle_timeout: days(7),
            absolute_timeout: days(30),
            reauthentication_window: minutes(10),
            touch_interval: Duration::from_secs(60),
            max_per_user: 50,
            bearer: true,
        }
    }
}

/// `[modules.identity.tokens]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TokenSettings {
    pub email_verification_ttl: Duration,
    pub magic_link_ttl: Duration,
    pub password_reset_ttl: Duration,
    pub email_change_ttl: Duration,
    pub invitation_ttl: Duration,
}
impl Default for TokenSettings {
    fn default() -> Self {
        Self {
            email_verification_ttl: minutes(24 * 60),
            magic_link_ttl: minutes(15),
            password_reset_ttl: minutes(30),
            email_change_ttl: minutes(60),
            invitation_ttl: days(7),
        }
    }
}

/// `[modules.identity.organizations]` (owner answer ID4: no personal
/// organization). `user_can_create = false` gives an application without
/// organizations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrganizationSettings {
    pub user_can_create: bool,
    /// Organizations one user may create.
    pub max_per_user: u32,
    pub max_members: u32,
}
impl Default for OrganizationSettings {
    fn default() -> Self {
        Self {
            user_can_create: true,
            max_per_user: 50,
            max_members: 10_000,
        }
    }
}

/// `[modules.identity.rbac.builtin_grants]`: what the implicit `user` role,
/// `admin` and `member` add to identity's own grants.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BuiltinGrants {
    pub user: Vec<String>,
    pub admin: Vec<String>,
    pub member: Vec<String>,
}

/// `[modules.identity.rbac]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RbacSettings {
    pub cache_entries: usize,
    /// Custom roles per organization.
    pub max_custom_roles: u32,
    /// Resource grants per holder and scope kept in a snapshot.
    pub max_resource_assignments: u32,
    pub builtin_grants: BuiltinGrants,
    /// Declared application roles by key.
    pub application_roles: BTreeMap<String, DeclaredRole>,
}
impl Default for RbacSettings {
    fn default() -> Self {
        Self {
            cache_entries: 10_000,
            max_custom_roles: 100,
            max_resource_assignments: 500,
            builtin_grants: BuiltinGrants::default(),
            application_roles: BTreeMap::new(),
        }
    }
}

/// `[modules.identity.api_keys]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ApiKeySettings {
    /// Unrevoked keys per organization, and for the application.
    pub max_per_scope: u32,
    pub max_ttl: Duration,
    /// `last_used_at` is written at most once per interval.
    pub touch_interval: Duration,
}
impl Default for ApiKeySettings {
    fn default() -> Self {
        Self {
            max_per_scope: 100,
            max_ttl: days(365),
            touch_interval: minutes(5),
        }
    }
}

/// `[modules.identity.links]`: absolute URLs on `app_origin` the token is
/// appended to as a fragment (`#token=...`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Links {
    pub verify_email: String,
    pub magic_link: String,
    pub password_reset: String,
    pub email_change: String,
    pub invitation: String,
}

/// The validated section.
#[derive(Clone)]
pub struct Settings {
    /// `scheme://host[:port]`, lowercase.
    pub app_origin: String,
    pub api_base_url: String,
    pub allowed_origins: Vec<String>,
    pub secret_key: Secret,
    pub secret_key_previous: Option<Secret>,
    pub registration: Registration,
    pub require_verified_email: bool,
    pub unverified_account_ttl: Duration,
    pub dev_print_links: bool,
    pub notify_new_sign_in: bool,
    pub links: Links,
    pub password: Option<PasswordSettings>,
    pub sessions: SessionSettings,
    pub tokens: TokenSettings,
    /// `[modules.identity.magic_links]`; its presence enables magic links.
    /// The value is `sign_up`.
    pub magic_links: Option<bool>,
    pub rate_limits: BTreeMap<&'static str, Rate>,
    pub audit_retention: Duration,
    /// The cleanup job's five-field cron expression.
    pub cleanup_cron: String,
    pub cleanup_batch: u32,
    /// The loopback development cookie (`merezha_session`, no `Secure`).
    pub dev_cookie: bool,
    pub app_name: Option<String>,
    pub organizations: OrganizationSettings,
    pub rbac: RbacSettings,
    /// `service_accounts.max_per_scope`.
    pub service_accounts_max: u32,
    pub api_keys: ApiKeySettings,
}
impl std::fmt::Debug for Settings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Settings")
            .field("app_origin", &self.app_origin)
            .field("registration", &self.registration)
            .field("require_verified_email", &self.require_verified_email)
            .field("password", &self.password.is_some())
            .field("magic_links", &self.magic_links)
            .field("dev_cookie", &self.dev_cookie)
            .finish_non_exhaustive()
    }
}
impl Settings {
    pub fn rate(&self, policy: &gcra::Policy) -> Rate {
        self.rate_limits
            .get(policy.name)
            .copied()
            .unwrap_or(policy.default)
    }
    /// The registration binding cookie's name (section 2, pre-hijacking).
    pub fn binding_cookie_name(&self) -> &'static str {
        if self.dev_cookie {
            "merezha_verify"
        } else {
            "__Host-merezha_verify"
        }
    }
    /// The session cookie's name.
    pub fn cookie_name(&self) -> &'static str {
        if self.dev_cookie {
            "merezha_session"
        } else {
            "__Host-merezha_session"
        }
    }
}

const fn minutes(m: u64) -> Duration {
    Duration::from_secs(m * 60)
}
const fn days(d: u64) -> Duration {
    Duration::from_secs(d * 86_400)
}

const KEYS: [&str; 22] = [
    "app_origin",
    "api_base_url",
    "allowed_origins",
    "secret_key",
    "secret_key_previous",
    "registration",
    "require_verified_email",
    "unverified_account_ttl",
    "dev_print_links",
    "notify_new_sign_in",
    "links",
    "password",
    "sessions",
    "tokens",
    "magic_links",
    "rate_limits",
    "audit",
    "cleanup",
    "organizations",
    "rbac",
    "service_accounts",
    "api_keys",
];
/// Most declared application roles.
const MAX_DECLARED_ROLES: usize = 64;
/// Most grants in one list.
const MAX_GRANTS: usize = 256;
/// Most `allowed_origins`.
const MAX_ORIGINS: usize = 32;
/// Shortest `secret_key`.
pub const MIN_SECRET_BYTES: usize = 32;

/// Collects issues while reading the section.
struct Reader<'a> {
    section: &'a ModuleSection,
    issues: Vec<ConfigIssue>,
}
impl<'a> Reader<'a> {
    fn issue(&mut self, key: &str, code: &'static str) {
        let origin = self.section.origin(key);
        self.issues
            .push(ConfigIssue::new(self.section.key(key), code, origin));
    }
    /// The sub-table `name`, its unknown keys reported.
    fn table(&mut self, name: &str, known: &[&str]) -> Option<&'a toml::Table> {
        let value = self.section.table.get(name)?;
        let Some(table) = value.as_table() else {
            self.issue(name, "expected_table");
            return None;
        };
        for key in table.keys() {
            if !known.contains(&key.as_str()) {
                self.issue(&format!("{name}.{key}"), "unknown_key");
            }
        }
        Some(table)
    }
    fn string(&mut self, table: Option<&toml::Table>, path: &str, key: &str) -> Option<String> {
        let value = lookup(self.section, table, key)?;
        match value.as_str() {
            Some(text) => Some(text.to_owned()),
            None => {
                self.issue(&join(path, key), "expected_string");
                None
            }
        }
    }
    fn boolean(
        &mut self,
        table: Option<&toml::Table>,
        path: &str,
        key: &str,
        default: bool,
    ) -> bool {
        match lookup(self.section, table, key) {
            None => default,
            Some(toml::Value::Boolean(b)) => *b,
            Some(_) => {
                self.issue(&join(path, key), "expected_boolean");
                default
            }
        }
    }
    fn integer(
        &mut self,
        table: Option<&toml::Table>,
        path: &str,
        key: &str,
        default: u64,
        range: (u64, u64),
    ) -> u64 {
        match lookup(self.section, table, key) {
            None => default,
            Some(toml::Value::Integer(n)) => match u64::try_from(*n) {
                Ok(n) if (range.0..=range.1).contains(&n) => n,
                _ => {
                    self.issue(&join(path, key), "out_of_range");
                    default
                }
            },
            Some(_) => {
                self.issue(&join(path, key), "expected_integer");
                default
            }
        }
    }
    fn duration(
        &mut self,
        table: Option<&toml::Table>,
        path: &str,
        key: &str,
        default: Duration,
        range: (Duration, Duration),
    ) -> Duration {
        match lookup(self.section, table, key) {
            None => default,
            Some(toml::Value::String(text)) => match parse_duration(text) {
                Some(d) if (range.0..=range.1).contains(&d) => d,
                Some(_) => {
                    self.issue(&join(path, key), "out_of_range");
                    default
                }
                None => {
                    self.issue(&join(path, key), "expected_duration");
                    default
                }
            },
            Some(_) => {
                self.issue(&join(path, key), "expected_duration");
                default
            }
        }
    }
}
fn lookup<'a>(
    section: &'a ModuleSection,
    table: Option<&'a toml::Table>,
    key: &str,
) -> Option<&'a toml::Value> {
    match table {
        Some(table) => table.get(key),
        None => section.table.get(key),
    }
}
fn join(path: &str, key: &str) -> String {
    if path.is_empty() {
        key.to_owned()
    } else {
        format!("{path}.{key}")
    }
}

/// An origin `http(s)://host[:port]`, lowercase, without path, query,
/// fragment or userinfo; `None` otherwise.
pub fn parse_origin(text: &str) -> Option<String> {
    let (scheme, rest) = text.split_once("://")?;
    if scheme != "https" && scheme != "http" {
        return None;
    }
    if rest.is_empty() || rest.contains(['/', '?', '#', '@']) || rest != rest.to_ascii_lowercase() {
        return None;
    }
    let (host, port) = if let Some(v6) = rest.strip_prefix('[') {
        let (address, after) = v6.split_once(']')?;
        address.parse::<std::net::Ipv6Addr>().ok()?;
        (&rest[..address.len() + 2], after.strip_prefix(':'))
    } else {
        match rest.split_once(':') {
            Some((host, port)) => (host, Some(port)),
            None => (rest, None),
        }
    };
    if let Some(port) = port
        && !port.parse::<u16>().is_ok_and(|p| p > 0)
    {
        return None;
    }
    let host_ok = host.starts_with('[')
        || (!host.is_empty()
            && host.len() <= 253
            && host.split('.').all(|label| {
                !label.is_empty()
                    && label.len() <= 63
                    && label
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            }));
    host_ok.then(|| text.to_owned())
}
/// `http://localhost`, `http://127.0.0.1` or `http://[::1]`, any port.
pub fn loopback_origin(origin: &str) -> bool {
    let Some(rest) = origin.strip_prefix("http://") else {
        return false;
    };
    let host = if rest.starts_with('[') {
        rest.split_once(']')
            .map_or(rest, |(h, _)| h)
            .trim_start_matches('[')
    } else {
        rest.split(':').next().unwrap_or(rest)
    };
    matches!(host, "localhost" | "127.0.0.1" | "::1")
}

impl Settings {
    /// Validate `section` for a host described by `facts`. `facts` is
    /// `None` when the kernel configuration itself is invalid; the checks
    /// that need it are skipped then.
    ///
    /// `catalog` is the permission catalog `builtin_grants` and declared
    /// application roles are checked against.
    pub fn parse(
        section: &ModuleSection,
        facts: Option<&HostFacts>,
        catalog: &Catalog,
    ) -> Result<Self, Vec<ConfigIssue>> {
        let mut r = Reader {
            section,
            issues: section.unknown_keys(&KEYS),
        };
        let private = facts.is_none_or(HostFacts::private_loopback);
        // Origins.
        let origin = |r: &mut Reader<'_>, key: &str| -> Option<String> {
            let text = r.string(None, "", key)?;
            match parse_origin(&text) {
                None => {
                    r.issue(key, "invalid_origin");
                    None
                }
                Some(origin)
                    if origin.starts_with("http://") && !(loopback_origin(&origin) && private) =>
                {
                    r.issue(key, "insecure_origin");
                    None
                }
                Some(origin) => Some(origin),
            }
        };
        let app_origin = match section.table.get("app_origin") {
            None => {
                r.issue("app_origin", "missing_required");
                None
            }
            Some(_) => origin(&mut r, "app_origin"),
        };
        let api_base_url = match section.table.get("api_base_url") {
            None => app_origin.clone(),
            Some(_) => origin(&mut r, "api_base_url"),
        };
        let mut allowed_origins = Vec::new();
        match section.table.get("allowed_origins") {
            None => allowed_origins.extend(app_origin.clone()),
            Some(toml::Value::Array(items)) => {
                if items.is_empty() || items.len() > MAX_ORIGINS {
                    r.issue("allowed_origins", "out_of_range");
                }
                for item in items {
                    match item.as_str().and_then(parse_origin) {
                        Some(o)
                            if o.starts_with("https://") || (loopback_origin(&o) && private) =>
                        {
                            allowed_origins.push(o)
                        }
                        Some(_) => r.issue("allowed_origins", "insecure_origin"),
                        None => r.issue("allowed_origins", "invalid_origin"),
                    }
                }
            }
            Some(_) => r.issue("allowed_origins", "expected_string_list"),
        }
        // Secrets.
        let secret = |r: &mut Reader<'_>, key: &str, required: bool| -> Option<Secret> {
            match section.secret(key) {
                Ok(Some(secret)) if secret.expose().len() >= MIN_SECRET_BYTES => Some(secret),
                Ok(Some(_)) => {
                    r.issue(key, "secret_too_short");
                    None
                }
                Ok(None) => {
                    if required {
                        r.issue(key, "missing_required");
                    }
                    None
                }
                Err(issue) => {
                    r.issues.push(issue);
                    None
                }
            }
        };
        let secret_key = secret(&mut r, "secret_key", true);
        let secret_key_previous = secret(&mut r, "secret_key_previous", false);
        let registration = match r.string(None, "", "registration").as_deref() {
            None | Some("open") => Registration::Open,
            Some("invite_only") => Registration::InviteOnly,
            Some("closed") => Registration::Closed,
            Some(_) => {
                r.issue("registration", "invalid_choice");
                Registration::Open
            }
        };
        let require_verified_email = r.boolean(None, "", "require_verified_email", true);
        let unverified_account_ttl = r.duration(
            None,
            "",
            "unverified_account_ttl",
            days(7),
            (minutes(60), days(365)),
        );
        let dev_print_links = r.boolean(None, "", "dev_print_links", false);
        if dev_print_links && !private {
            r.issue("dev_print_links", "refused_when_public");
        }
        let notify_new_sign_in = r.boolean(None, "", "notify_new_sign_in", false);
        // Links.
        let links_table = r.table(
            "links",
            &[
                "verify_email",
                "magic_link",
                "password_reset",
                "email_change",
                "invitation",
            ],
        );
        let base = app_origin.clone().unwrap_or_default();
        let link = |r: &mut Reader<'_>, key: &str, default: &str| -> String {
            let text = r
                .string(links_table, "links", key)
                .unwrap_or_else(|| default.to_owned());
            let path = text.strip_prefix(&base).unwrap_or(&text);
            match crate::normalize::return_to(path) {
                Ok(path) if !path.contains('#') => format!("{base}{path}"),
                _ => {
                    r.issue(&format!("links.{key}"), "invalid_url");
                    String::new()
                }
            }
        };
        let links = Links {
            verify_email: link(&mut r, "verify_email", "/auth/verify-email"),
            magic_link: link(&mut r, "magic_link", "/auth/magic-link"),
            password_reset: link(&mut r, "password_reset", "/auth/reset-password"),
            email_change: link(&mut r, "email_change", "/auth/confirm-email"),
            invitation: link(&mut r, "invitation", "/auth/invitation"),
        };
        // Password.
        let password = r
            .table(
                "password",
                &[
                    "min_length",
                    "max_length",
                    "blocklist_file",
                    "argon2_memory_kib",
                    "argon2_iterations",
                    "argon2_parallelism",
                    "hash_concurrency",
                    "hash_queue_timeout",
                ],
            )
            .map(|table| {
                let t = Some(table);
                let d = PasswordSettings::default();
                let min_length =
                    r.integer(t, "password", "min_length", d.min_length as u64, (8, 256));
                let max_length =
                    r.integer(t, "password", "max_length", d.max_length as u64, (8, 256));
                if max_length < min_length {
                    r.issue("password.max_length", "out_of_range");
                }
                let blocklist = match table.get("blocklist_file") {
                    None => None,
                    Some(_) => {
                        let mut sub = section.clone();
                        sub.name = format!("{}.password", section.name);
                        sub.table = table.clone();
                        match sub.secret("blocklist_file") {
                            Ok(found) => found.map(|s| s.expose().to_owned()),
                            Err(issue) => {
                                r.issues.push(issue);
                                None
                            }
                        }
                    }
                };
                PasswordSettings {
                    min_length: min_length as usize,
                    max_length: max_length as usize,
                    blocklist,
                    memory_kib: r.integer(
                        t,
                        "password",
                        "argon2_memory_kib",
                        u64::from(d.memory_kib),
                        (u64::from(PasswordSettings::MIN_MEMORY_KIB), 4_194_304),
                    ) as u32,
                    iterations: r.integer(
                        t,
                        "password",
                        "argon2_iterations",
                        u64::from(d.iterations),
                        (u64::from(PasswordSettings::MIN_ITERATIONS), 64),
                    ) as u32,
                    parallelism: r.integer(t, "password", "argon2_parallelism", 1, (1, 64)) as u32,
                    hash_concurrency: r.integer(t, "password", "hash_concurrency", 0, (0, 1024))
                        as usize,
                    hash_queue_timeout: r.duration(
                        t,
                        "password",
                        "hash_queue_timeout",
                        d.hash_queue_timeout,
                        (Duration::from_millis(10), Duration::from_secs(30)),
                    ),
                }
            });
        // Sessions.
        let sessions = {
            let t = r.table(
                "sessions",
                &[
                    "idle_timeout",
                    "absolute_timeout",
                    "reauthentication_window",
                    "touch_interval",
                    "max_per_user",
                    "bearer",
                ],
            );
            let d = SessionSettings::default();
            let s = SessionSettings {
                idle_timeout: r.duration(
                    t,
                    "sessions",
                    "idle_timeout",
                    d.idle_timeout,
                    (minutes(1), days(365)),
                ),
                absolute_timeout: r.duration(
                    t,
                    "sessions",
                    "absolute_timeout",
                    d.absolute_timeout,
                    (minutes(1), days(365)),
                ),
                reauthentication_window: r.duration(
                    t,
                    "sessions",
                    "reauthentication_window",
                    d.reauthentication_window,
                    (minutes(1), days(1)),
                ),
                touch_interval: r.duration(
                    t,
                    "sessions",
                    "touch_interval",
                    d.touch_interval,
                    (Duration::from_secs(1), minutes(60)),
                ),
                max_per_user: r.integer(t, "sessions", "max_per_user", 50, (1, 1000)) as u32,
                bearer: r.boolean(t, "sessions", "bearer", true),
            };
            if s.idle_timeout > s.absolute_timeout {
                r.issue("sessions.idle_timeout", "idle_exceeds_absolute");
            }
            if s.touch_interval >= s.idle_timeout {
                r.issue("sessions.touch_interval", "out_of_range");
            }
            s
        };
        // Tokens.
        let tokens = {
            let t = r.table(
                "tokens",
                &[
                    "email_verification_ttl",
                    "magic_link_ttl",
                    "password_reset_ttl",
                    "email_change_ttl",
                    "invitation_ttl",
                ],
            );
            let d = TokenSettings::default();
            TokenSettings {
                email_verification_ttl: r.duration(
                    t,
                    "tokens",
                    "email_verification_ttl",
                    d.email_verification_ttl,
                    (minutes(5), days(7)),
                ),
                magic_link_ttl: r.duration(
                    t,
                    "tokens",
                    "magic_link_ttl",
                    d.magic_link_ttl,
                    (minutes(1), days(1)),
                ),
                password_reset_ttl: r.duration(
                    t,
                    "tokens",
                    "password_reset_ttl",
                    d.password_reset_ttl,
                    (minutes(5), days(1)),
                ),
                email_change_ttl: r.duration(
                    t,
                    "tokens",
                    "email_change_ttl",
                    d.email_change_ttl,
                    (minutes(5), days(7)),
                ),
                invitation_ttl: r.duration(
                    t,
                    "tokens",
                    "invitation_ttl",
                    d.invitation_ttl,
                    (minutes(60), days(30)),
                ),
            }
        };
        let magic_links = r
            .table("magic_links", &["sign_up"])
            .map(|table| r.boolean(Some(table), "magic_links", "sign_up", true));
        if password.is_none() && magic_links.is_none() {
            r.issue("password", "no_sign_in_method");
        }
        // Rate limits.
        let names: Vec<String> = gcra::POLICIES.iter().map(gcra::table_name).collect();
        let known: Vec<&str> = names.iter().map(String::as_str).collect();
        let mut rate_limits = BTreeMap::new();
        if let Some(table) = r.table("rate_limits", &known) {
            for policy in gcra::POLICIES {
                let name = gcra::table_name(&policy);
                let Some(value) = table.get(&name) else {
                    continue;
                };
                let path = format!("rate_limits.{name}");
                let Some(inner) = value.as_table() else {
                    r.issue(&path, "expected_table");
                    continue;
                };
                for key in inner.keys() {
                    if key != "limit" && key != "period" {
                        r.issue(&format!("{path}.{key}"), "unknown_key");
                    }
                }
                let limit = r.integer(
                    Some(inner),
                    &path,
                    "limit",
                    u64::from(policy.default.limit),
                    (1, 1_000_000),
                );
                let period = r.duration(
                    Some(inner),
                    &path,
                    "period",
                    policy.default.period,
                    (Duration::from_secs(1), days(30)),
                );
                rate_limits.insert(policy.name, Rate::new(limit as u32, period));
            }
        }
        let audit_retention = {
            let t = r.table("audit", &["retention"]);
            r.duration(t, "audit", "retention", days(400), (days(1), days(3650)))
        };
        let (cleanup_cron, cleanup_batch) = {
            let t = r.table("cleanup", &["interval", "batch_size"]);
            let interval = r.duration(t, "cleanup", "interval", minutes(10), (minutes(1), days(1)));
            let cron = cron_for(interval).unwrap_or_else(|| {
                r.issue("cleanup.interval", "out_of_range");
                String::new()
            });
            let batch = r.integer(t, "cleanup", "batch_size", 1000, (1, 10_000)) as u32;
            (cron, batch)
        };
        let organizations = {
            let t = r.table(
                "organizations",
                &["user_can_create", "max_per_user", "max_members"],
            );
            let d = OrganizationSettings::default();
            OrganizationSettings {
                user_can_create: r.boolean(t, "organizations", "user_can_create", true),
                max_per_user: r.integer(t, "organizations", "max_per_user", 50, (1, 10_000)) as u32,
                max_members: r.integer(
                    t,
                    "organizations",
                    "max_members",
                    u64::from(d.max_members),
                    (1, 1_000_000),
                ) as u32,
            }
        };
        let rbac = rbac(&mut r, catalog);
        let service_accounts_max = {
            let t = r.table("service_accounts", &["max_per_scope"]);
            r.integer(t, "service_accounts", "max_per_scope", 100, (1, 10_000)) as u32
        };
        let api_keys = {
            let t = r.table("api_keys", &["max_per_scope", "max_ttl", "touch_interval"]);
            let d = ApiKeySettings::default();
            ApiKeySettings {
                max_per_scope: r.integer(t, "api_keys", "max_per_scope", 100, (1, 10_000)) as u32,
                max_ttl: r.duration(
                    t,
                    "api_keys",
                    "max_ttl",
                    d.max_ttl,
                    (minutes(60), days(3650)),
                ),
                touch_interval: r.duration(
                    t,
                    "api_keys",
                    "touch_interval",
                    d.touch_interval,
                    (Duration::from_secs(1), days(1)),
                ),
            }
        };
        let dev_cookie = app_origin.as_deref().is_some_and(loopback_origin) && private;
        if !r.issues.is_empty() {
            return Err(r.issues);
        }
        let (Some(app_origin), Some(api_base_url), Some(secret_key)) =
            (app_origin, api_base_url, secret_key)
        else {
            return Err(vec![ConfigIssue::new(
                section.key("app_origin"),
                "missing_required",
                Origin::Default,
            )]);
        };
        Ok(Self {
            app_origin,
            api_base_url,
            allowed_origins,
            secret_key,
            secret_key_previous,
            registration,
            require_verified_email,
            unverified_account_ttl,
            dev_print_links,
            notify_new_sign_in,
            links,
            password,
            sessions,
            tokens,
            magic_links,
            rate_limits,
            audit_retention,
            cleanup_cron,
            cleanup_batch,
            dev_cookie,
            app_name: facts.and_then(|f| f.app_name.clone()),
            organizations,
            rbac,
            service_accounts_max,
            api_keys,
        })
    }
}

/// A string list of grants at `path`, each checked against `catalog`.
fn grants(
    r: &mut Reader<'_>,
    value: Option<&toml::Value>,
    path: &str,
    catalog: &Catalog,
    wildcards: Wildcards,
) -> Vec<String> {
    let Some(value) = value else {
        return Vec::new();
    };
    let Some(items) = value.as_array() else {
        r.issue(path, "expected_string_list");
        return Vec::new();
    };
    if items.len() > MAX_GRANTS {
        r.issue(path, "out_of_range");
    }
    let mut out = Vec::new();
    for item in items {
        match item.as_str() {
            Some(grant) if catalog.check_grant(grant, wildcards).is_ok() => {
                out.push(grant.to_owned())
            }
            Some(_) => r.issue(path, "unknown_permission"),
            None => r.issue(path, "expected_string_list"),
        }
    }
    out
}

/// `[modules.identity.rbac]`: limits, `builtin_grants` (operator
/// configuration, any trailing wildcard) and the declared application
/// roles (wildcards in the application's own namespaces only, keys outside
/// the built-in ones).
fn rbac(r: &mut Reader<'_>, catalog: &Catalog) -> RbacSettings {
    let t = r.table(
        "rbac",
        &[
            "cache_entries",
            "max_custom_roles",
            "max_resource_assignments",
            "builtin_grants",
            "application_roles",
        ],
    );
    let d = RbacSettings::default();
    let mut settings = RbacSettings {
        cache_entries: r.integer(t, "rbac", "cache_entries", 10_000, (2, 1_000_000)) as usize,
        max_custom_roles: r.integer(t, "rbac", "max_custom_roles", 100, (1, 1000)) as u32,
        max_resource_assignments: r.integer(
            t,
            "rbac",
            "max_resource_assignments",
            u64::from(d.max_resource_assignments),
            (1, 10_000),
        ) as u32,
        ..d
    };
    let Some(t) = t else {
        return settings;
    };
    match t.get("builtin_grants") {
        None => {}
        Some(toml::Value::Table(table)) => {
            for key in table.keys() {
                if !["user", "admin", "member"].contains(&key.as_str()) {
                    r.issue(&format!("rbac.builtin_grants.{key}"), "unknown_key");
                }
            }
            // `user` holds in every organization and needs no grant at
            // all to receive, so it follows the custom role rule: no
            // wildcard inside a block namespace.
            let mut list = |name: &str| {
                grants(
                    r,
                    table.get(name),
                    &format!("rbac.builtin_grants.{name}"),
                    catalog,
                    if name == "user" {
                        Wildcards::Application
                    } else {
                        Wildcards::Any
                    },
                )
            };
            settings.builtin_grants = BuiltinGrants {
                user: list("user"),
                admin: list("admin"),
                member: list("member"),
            };
        }
        Some(_) => r.issue("rbac.builtin_grants", "expected_table"),
    }
    match t.get("application_roles") {
        None => {}
        Some(toml::Value::Table(roles)) => {
            if roles.len() > MAX_DECLARED_ROLES {
                r.issue("rbac.application_roles", "out_of_range");
            }
            for (key, value) in roles {
                let path = format!("rbac.application_roles.{key}");
                if !valid_role_key(key) {
                    r.issue(&path, "invalid_role_key");
                    continue;
                }
                if RESERVED_ROLE_KEYS.contains(&key.as_str()) {
                    r.issue(&path, "reserved_role_key");
                    continue;
                }
                let Some(role) = value.as_table() else {
                    r.issue(&path, "expected_table");
                    continue;
                };
                for field in role.keys() {
                    if !["name", "description", "permissions"].contains(&field.as_str()) {
                        r.issue(&format!("{path}.{field}"), "unknown_key");
                    }
                }
                let name = match role.get("name").and_then(|v| v.as_str()) {
                    Some(name) if (1..=128).contains(&name.chars().count()) => name.to_owned(),
                    Some(_) => {
                        r.issue(&format!("{path}.name"), "out_of_range");
                        continue;
                    }
                    None => {
                        r.issue(&format!("{path}.name"), "missing_required");
                        continue;
                    }
                };
                let description = match role.get("description") {
                    None => None,
                    Some(toml::Value::String(text)) if text.chars().count() <= 1024 => {
                        Some(text.clone())
                    }
                    Some(_) => {
                        r.issue(&format!("{path}.description"), "out_of_range");
                        None
                    }
                };
                let permissions = grants(
                    r,
                    role.get("permissions"),
                    &format!("{path}.permissions"),
                    catalog,
                    Wildcards::Application,
                );
                settings.application_roles.insert(
                    key.clone(),
                    DeclaredRole {
                        name,
                        description,
                        grants: permissions,
                    },
                );
            }
        }
        Some(_) => r.issue("rbac.application_roles", "expected_table"),
    }
    settings
}

/// The cron expression of a cleanup interval: whole minutes dividing an
/// hour, whole hours dividing a day, or one day.
pub fn cron_for(interval: Duration) -> Option<String> {
    let secs = interval.as_secs();
    if interval.subsec_nanos() != 0 || !secs.is_multiple_of(60) {
        return None;
    }
    let minutes = secs / 60;
    match minutes {
        1 => Some("* * * * *".into()),
        m if m < 60 && 60_u64.is_multiple_of(m) => Some(format!("*/{m} * * * *")),
        60 => Some("0 * * * *".into()),
        m if m.is_multiple_of(60) && m < 1440 && 24_u64.is_multiple_of(m / 60) => {
            Some(format!("0 */{} * * *", m / 60))
        }
        1440 => Some("0 0 * * *".into()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn origins_are_exact() {
        for good in [
            "https://app.example.com",
            "https://app.example.com:8443",
            "http://localhost:5173",
            "http://127.0.0.1",
            "http://[::1]:3000",
        ] {
            assert_eq!(parse_origin(good).as_deref(), Some(good), "{good}");
        }
        for bad in [
            "app.example.com",
            "ftp://app.example.com",
            "https://app.example.com/",
            "https://app.example.com/path",
            "https://App.example.com",
            "https://user@app.example.com",
            "https://app.example.com:0",
            "https://app.example.com:99999",
            "https://",
            "https://a..b",
        ] {
            assert!(parse_origin(bad).is_none(), "{bad}");
        }
        assert!(loopback_origin("http://localhost:5173"));
        assert!(loopback_origin("http://[::1]:3000"));
        assert!(!loopback_origin("https://localhost"));
        assert!(!loopback_origin("http://localhost.evil.example"));
    }

    #[test]
    fn cleanup_intervals_map_to_cron() {
        assert_eq!(cron_for(minutes(10)).as_deref(), Some("*/10 * * * *"));
        assert_eq!(cron_for(minutes(1)).as_deref(), Some("* * * * *"));
        assert_eq!(cron_for(minutes(60)).as_deref(), Some("0 * * * *"));
        assert_eq!(cron_for(minutes(360)).as_deref(), Some("0 */6 * * *"));
        assert_eq!(cron_for(days(1)).as_deref(), Some("0 0 * * *"));
        assert_eq!(cron_for(minutes(7)), None);
        assert_eq!(cron_for(minutes(300)), None);
        assert_eq!(cron_for(Duration::from_secs(90)), None);
    }
}
