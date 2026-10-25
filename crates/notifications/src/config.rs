//! `[modules.notifications]` (NT §7.2): typed settings, parsed and validated
//! from the module's section with every problem reported at once.
//!
//! Checks that need the rest of the project (`http.public`, the
//! `[jobs.queues]` settings, `MEREZHA_NOTIFY_NO_EGRESS`) read
//! [`HostFacts`]; template checks live in [`crate::templates`].
use crate::topics::{self, Category, Topic};
use base64::Engine;
use merezha_platform::{
    ModuleSection,
    config::{
        ConfigIssue, JobDefaults, JobsConfig, Origin, ProjectConfig, flag_path, parse_duration,
    },
    net::Cidr,
    ports::Channel,
};
use merezha_postgres::Secret;
use std::{
    collections::{BTreeMap, BTreeSet},
    net::IpAddr,
    path::{Path, PathBuf},
    time::Duration,
};

/// The environment variable CI sets: providers and endpoints must be
/// loopback (NT §8).
pub const NO_EGRESS_ENV: &str = "MEREZHA_NOTIFY_NO_EGRESS";
/// Most declared topics, built-ins included.
pub const MAX_TOPICS: usize = 256;
/// Most entries in a list setting such as `templates.locales`.
const MAX_LIST: usize = 64;
const DAY: u64 = 86_400;

/// Facts about the project outside `[modules.notifications]` that the
/// block's startup checks need. The host's `Module::configure` only sees
/// the module's own section, so the embedding binary gathers these from the
/// same configuration input before it registers the module.
#[derive(Clone, Debug, Default)]
pub struct HostFacts {
    /// Directory of `merezha.toml`; `templates.dir` resolves against it.
    pub base_dir: PathBuf,
    /// `http.public`; `None` when the setting itself is malformed, which
    /// the kernel reports.
    pub http_public: Option<bool>,
    /// `app.name` (default `app.id`), the default of `templates.app_name`.
    pub app_name: Option<String>,
    /// `[jobs]`; `None` when that section itself does not validate, which
    /// the kernel reports.
    pub jobs: Option<JobsConfig>,
    /// Origins of the queue settings the block checks, by issue key such as
    /// `jobs.queues."notify.email".lease_ms`. Absent keys are defaults.
    pub jobs_origins: BTreeMap<String, Origin>,
    /// `MEREZHA_NOTIFY_NO_EGRESS=1`.
    pub no_egress: bool,
}

/// The queue settings the cross checks read.
const CHECKED_QUEUE_KEYS: [(&str, &str); 4] = [
    ("notify.email", "lease_ms"),
    ("notify.sms", "lease_ms"),
    ("notify.webhook", "lease_ms"),
    ("notify.webhook", "backoff_max_ms"),
];
/// Stands in for the database URL while `[jobs]` is validated alone.
const FACTS_URL_ENV: &str = "NOTIFY_FACTS_PLACEHOLDER_URL";

/// The raw layers of the configuration input, read with the kernel's
/// precedence: flag, then `MEREZHA_<SECTION>__<KEY>`, then the file.
struct RawLayers<'a> {
    file: toml::Table,
    input: &'a merezha_platform::ConfigInput,
}
impl RawLayers<'_> {
    /// The value of `section.key` and its origin. `section` lists path
    /// segments (`["jobs", "queues", "notify.email"]`); flags are split with
    /// the kernel's own [`flag_path`], so quoted segments match as they do
    /// there.
    fn get(&self, section: &[&str], key: &str) -> Option<(toml::Value, Origin)> {
        let path: Vec<&str> = section
            .iter()
            .copied()
            .chain(std::iter::once(key))
            .collect();
        if let Some((_, value)) = self.input.overrides.iter().rev().find(|(k, _)| {
            flag_path(k).is_some_and(|p| p.iter().map(String::as_str).eq(path.iter().copied()))
        }) {
            return Some((toml::Value::String(value.clone()), Origin::Flag));
        }
        let wanted: Vec<String> = section
            .iter()
            .chain(std::iter::once(&key))
            .map(|s| s.to_ascii_lowercase())
            .collect();
        let env = self.input.env.iter().rev().find(|(name, _)| {
            name.strip_prefix(merezha_platform::config::ENV_PREFIX)
                .is_some_and(|rest| {
                    rest.split("__")
                        .map(str::to_ascii_lowercase)
                        .eq(wanted.iter().cloned())
                })
        });
        if let Some((name, value)) = env {
            return Some((
                toml::Value::String(value.clone()),
                Origin::Env(name.clone()),
            ));
        }
        let mut table = &self.file;
        for segment in section {
            table = table.get(*segment)?.as_table()?;
        }
        table.get(key).map(|v| (v.clone(), Origin::File))
    }
}

impl HostFacts {
    /// Gather the facts from the same input the host loads, with the
    /// kernel's precedence, whether or not the rest of the configuration
    /// validates: `http.public`, `app.name` and `[jobs]` are read from the
    /// layers directly, so the block's checks run in the same pass as the
    /// kernel's and every problem is reported at once.
    pub fn load(input: &merezha_platform::ConfigInput) -> Self {
        use std::io::Read;
        let base_dir = input
            .path
            .canonicalize()
            .ok()
            .and_then(|p| p.parent().map(Path::to_path_buf))
            .unwrap_or_default();
        let no_egress = input
            .env
            .iter()
            .any(|(name, value)| name == NO_EGRESS_ENV && value == "1");
        let mut text = String::new();
        let readable = std::fs::File::open(&input.path)
            .and_then(|f| {
                f.take(merezha_platform::config::MAX_CONFIG_BYTES + 1)
                    .read_to_string(&mut text)
            })
            .is_ok()
            && text.len() as u64 <= merezha_platform::config::MAX_CONFIG_BYTES;
        let file: toml::Table = if readable {
            toml::from_str(&text).unwrap_or_default()
        } else {
            toml::Table::new()
        };
        let layers = RawLayers { file, input };
        let http_public = match layers.get(&["http"], "public") {
            None => Some(false),
            Some((toml::Value::Boolean(b), _)) => Some(b),
            Some((toml::Value::String(text), Origin::Env(_) | Origin::Flag)) => {
                match text.as_str() {
                    "true" => Some(true),
                    "false" => Some(false),
                    _ => None,
                }
            }
            Some(_) => None,
        };
        let app_name = [("name"), ("id")]
            .iter()
            .find_map(|key| match layers.get(&["app"], key) {
                Some((toml::Value::String(name), _)) if !name.is_empty() => Some(name),
                _ => None,
            });
        let jobs = Self::jobs_alone(&layers, &base_dir);
        let mut jobs_origins = BTreeMap::new();
        for (queue, key) in CHECKED_QUEUE_KEYS {
            let origin = layers
                .get(&["jobs", "queues", queue], key)
                .or_else(|| layers.get(&["jobs", "defaults"], key))
                .map(|(_, origin)| origin);
            if let Some(origin) = origin {
                jobs_origins.insert(format!("jobs.queues.\"{queue}\".{key}"), origin);
            }
        }
        Self {
            base_dir,
            http_public,
            app_name,
            jobs,
            jobs_origins,
            no_egress,
        }
    }

    /// `[jobs]` validated by the kernel on its own: the file's section and
    /// its environment and flag layers, with placeholder kernel sections.
    fn jobs_alone(layers: &RawLayers<'_>, base_dir: &Path) -> Option<JobsConfig> {
        let mut document = toml::Table::new();
        let table = |pairs: &[(&str, toml::Value)]| {
            toml::Value::Table(
                pairs
                    .iter()
                    .map(|(k, v)| ((*k).to_owned(), v.clone()))
                    .collect(),
            )
        };
        document.insert("app".into(), table(&[("id", "facts".into())]));
        let url = table(&[("env", FACTS_URL_ENV.into())]);
        document.insert("database".into(), table(&[("url", url)]));
        document.insert(
            "runtime".into(),
            table(&[("manifest", "runtime.toml".into())]),
        );
        if let Some(jobs) = layers.file.get("jobs") {
            document.insert("jobs".into(), jobs.clone());
        }
        let text = toml::to_string(&document).ok()?;
        let mut env: Vec<(String, String)> = layers
            .input
            .env
            .iter()
            .filter(|(name, _)| {
                name.strip_prefix(merezha_platform::config::ENV_PREFIX)
                    .and_then(|rest| rest.split("__").next())
                    .is_some_and(|section| section.eq_ignore_ascii_case("jobs"))
            })
            .cloned()
            .collect();
        env.push((
            FACTS_URL_ENV.into(),
            "postgres://facts@127.0.0.1:1/facts".into(),
        ));
        let overrides: Vec<(String, String)> = layers
            .input
            .overrides
            .iter()
            .filter(|(key, _)| {
                flag_path(key).is_some_and(|p| p.first().is_some_and(|s| s == "jobs"))
            })
            .cloned()
            .collect();
        ProjectConfig::parse(&text, base_dir.to_path_buf(), &env, &overrides)
            .ok()
            .map(|config| config.jobs)
    }

    /// The facts of a loaded project.
    pub fn from_config(config: &ProjectConfig, no_egress: bool) -> Self {
        Self {
            base_dir: config.base_dir.clone(),
            http_public: Some(config.http.public),
            app_name: Some(config.app.name.clone()),
            jobs: Some(config.jobs.clone()),
            jobs_origins: BTreeMap::new(),
            no_egress,
        }
    }
}

/// Key material decoded from `secret_key`: at least 32 bytes. `Debug`
/// never shows it.
#[derive(Clone, PartialEq, Eq)]
pub struct KeyMaterial(Vec<u8>);
impl KeyMaterial {
    pub fn expose(&self) -> &[u8] {
        &self.0
    }
    /// Key material from decoded bytes: at least 32, as `secret_key`
    /// requires; `None` otherwise. For tools and tests that sign tokens
    /// the block verifies.
    pub fn from_bytes(bytes: Vec<u8>) -> Option<Self> {
        (bytes.len() >= 32).then_some(Self(bytes))
    }
    #[cfg(test)]
    pub(crate) fn for_tests(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}
impl std::fmt::Debug for KeyMaterial {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("KeyMaterial(<redacted>)")
    }
}

/// `limits`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Limits {
    pub max_recipients: usize,
    pub max_targets: usize,
    pub max_marketing_targets: usize,
    /// `data` plus `secret_data`, serialized.
    pub max_data_bytes: usize,
}
/// `links`: pages of the application the block links to.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Links {
    pub unsubscribe_page: Option<String>,
    pub preferences_page: Option<String>,
}
/// `egress` (NT §5.6).
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Egress {
    pub allow_private_networks: bool,
    pub deny_cidrs: Vec<Cidr>,
}
/// `retention`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Retention {
    pub data_after_settled: Duration,
    pub deliveries: Duration,
    pub webhook_deliveries: Duration,
    /// Captured messages kept by the capture drivers.
    pub captures: u32,
}
/// `[modules.notifications.templates]` (NT §4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TemplateSettings {
    /// Resolved against the configuration file's directory.
    pub dir: PathBuf,
    /// Whether `dir` was set explicitly; only then must it exist.
    pub dir_explicit: bool,
    pub default_locale: String,
    /// Closed list, the default locale included.
    pub locales: Vec<String>,
    pub app_name: String,
    pub app_base_url: Option<String>,
    pub allow_http_links: bool,
    pub fuel: u64,
    pub preview: bool,
    pub preview_in_public: bool,
}
/// Delivery driver of the email and SMS channels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Driver {
    Brevo,
    Capture,
    Disabled,
}
impl Driver {
    pub fn as_str(self) -> &'static str {
        match self {
            Driver::Brevo => "brevo",
            Driver::Capture => "capture",
            Driver::Disabled => "disabled",
        }
    }
}
/// A sender or reply-to mailbox, `Name <local@domain>` or `local@domain`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Mailbox {
    pub name: Option<String>,
    pub address: String,
}
/// `[modules.notifications.email]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EmailSettings {
    pub driver: Driver,
    pub from: Mailbox,
    pub reply_to: Option<Mailbox>,
}
/// `[modules.notifications.sms]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SmsSettings {
    pub driver: Driver,
    pub sender: String,
    pub organisation_prefix: Option<String>,
    pub max_segments: u32,
}
/// `[modules.notifications.brevo]`. `Debug` redacts the secrets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BrevoSettings {
    pub api_key: Option<Secret>,
    pub base_url: String,
    pub timeout: Duration,
    pub connect_timeout: Duration,
    pub sandbox: bool,
    pub webhook_token: Option<Secret>,
    pub webhook_allowed_cidrs: Vec<Cidr>,
}
/// `[modules.notifications.push]`; present means push is on. The VAPID key
/// is checked for shape here and parsed by the push slice.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushSettings {
    pub vapid_private_key: Secret,
    pub vapid_subject: String,
    pub ttl: Duration,
    pub allowed_hosts: Vec<String>,
    pub max_subscriptions_per_user: u32,
}
/// `[modules.notifications.inbox]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboxSettings {
    pub retention: Duration,
    pub archived_retention: Duration,
}
/// `[modules.notifications.sse]`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SseSettings {
    pub heartbeat: Duration,
    pub reconcile_interval: Duration,
    pub replay_max: u32,
    pub max_streams_per_user: u32,
    pub max_streams: u32,
    pub max_lifetime: Duration,
    pub reauth_interval: Duration,
}
/// `[modules.notifications.webhooks]`; present means webhooks are on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WebhookSettings {
    pub max_endpoints_per_tenant: u32,
    pub max_payload_bytes: u32,
    pub timeout: Duration,
    pub connect_timeout: Duration,
    pub concurrency_per_endpoint: u32,
    pub auto_disable_after: Duration,
    pub allow_http: bool,
    pub alert_roles: Vec<String>,
    pub application_alert_roles: Vec<String>,
    /// Application event types and their descriptions.
    pub event_types: BTreeMap<String, String>,
}

/// The validated `[modules.notifications]`. `Debug` redacts secrets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Settings {
    pub secret_key: KeyMaterial,
    pub secret_key_previous: Option<KeyMaterial>,
    /// Without a trailing slash, such as `https://api.acme.com`.
    pub public_base_url: Option<String>,
    pub limits: Limits,
    pub links: Links,
    pub egress: Egress,
    pub retention: Retention,
    pub templates: TemplateSettings,
    pub email: EmailSettings,
    pub sms: SmsSettings,
    pub brevo: Option<BrevoSettings>,
    pub push: Option<PushSettings>,
    pub inbox: InboxSettings,
    pub sse: SseSettings,
    pub webhooks: Option<WebhookSettings>,
    /// Every topic, built-ins included, by name.
    pub topics: BTreeMap<String, Topic>,
}
impl Settings {
    /// Whether `channel` can deliver: its driver is not disabled and its
    /// section is present. The inbox is always available.
    pub fn channel_available(&self, channel: Channel) -> bool {
        match channel {
            Channel::Email => self.email.driver != Driver::Disabled,
            Channel::Sms => self.sms.driver != Driver::Disabled,
            Channel::Push => self.push.is_some(),
            Channel::Inbox => true,
        }
    }
    /// Whether a capture driver is active, which enables
    /// `GET /v1/notifications/dev/captures`.
    pub fn capture_active(&self) -> bool {
        self.email.driver == Driver::Capture || self.sms.driver == Driver::Capture
    }
}

/// Dotted key segment, quoted when it contains a dot, as TOML does.
pub(crate) fn segment(name: &str) -> String {
    if name.contains('.') || name.contains('/') {
        format!("\"{name}\"")
    } else {
        name.to_owned()
    }
}

/// Typed reads from one table, collecting issues. Keys are taken out as
/// they are read, so [`Reader::finish`] reports the leftovers.
pub(crate) struct Reader<'a> {
    prefix: String,
    table: toml::Table,
    origins: BTreeMap<String, Origin>,
    issues: &'a mut Vec<ConfigIssue>,
}
impl<'a> Reader<'a> {
    pub(crate) fn new(
        prefix: String,
        table: toml::Table,
        origins: BTreeMap<String, Origin>,
        issues: &'a mut Vec<ConfigIssue>,
    ) -> Self {
        Self {
            prefix,
            table,
            origins,
            issues,
        }
    }
    fn key(&self, key: &str) -> String {
        format!("{}.{key}", self.prefix)
    }
    fn origin(&self, key: &str) -> Origin {
        self.origins.get(key).cloned().unwrap_or(Origin::File)
    }
    pub(crate) fn issue(&mut self, key: &str, code: &'static str) {
        let origin = self.origin(key);
        self.issues
            .push(ConfigIssue::new(self.key(key), code, origin));
    }
    pub(crate) fn issue_default(&mut self, key: &str, code: &'static str) {
        self.issues
            .push(ConfigIssue::new(self.key(key), code, Origin::Default));
    }
    fn take(&mut self, key: &str) -> Option<toml::Value> {
        self.table.remove(key)
    }
    pub(crate) fn has(&self, key: &str) -> bool {
        self.table.contains_key(key)
    }
    pub(crate) fn string(&mut self, key: &str) -> Option<String> {
        match self.take(key)? {
            toml::Value::String(value) => Some(value),
            _ => {
                self.issue(key, "expected_string");
                None
            }
        }
    }
    pub(crate) fn boolean(&mut self, key: &str, default: bool) -> bool {
        match self.take(key) {
            None => default,
            Some(toml::Value::Boolean(value)) => value,
            Some(_) => {
                self.issue(key, "expected_boolean");
                default
            }
        }
    }
    pub(crate) fn integer(&mut self, key: &str, default: u64, min: u64, max: u64) -> u64 {
        match self.take(key) {
            None => default,
            Some(toml::Value::Integer(value)) => match u64::try_from(value) {
                Ok(value) if (min..=max).contains(&value) => value,
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
    pub(crate) fn optional_integer(&mut self, key: &str, min: u64, max: u64) -> Option<u64> {
        if !self.has(key) {
            return None;
        }
        let value = self.integer(key, u64::MAX, min, max);
        (value != u64::MAX).then_some(value)
    }
    pub(crate) fn duration(
        &mut self,
        key: &str,
        default: Duration,
        min: Duration,
        max: Duration,
    ) -> Duration {
        match self.take(key) {
            None => default,
            Some(toml::Value::String(text)) => match parse_duration(&text) {
                Some(value) if (min..=max).contains(&value) => value,
                Some(_) => {
                    self.issue(key, "out_of_range");
                    default
                }
                None => {
                    self.issue(key, "expected_duration");
                    default
                }
            },
            Some(_) => {
                self.issue(key, "expected_duration");
                default
            }
        }
    }
    pub(crate) fn list(&mut self, key: &str, max: usize) -> Option<Vec<String>> {
        let value = self.take(key)?;
        let items: Option<Vec<String>> = match value {
            toml::Value::Array(items) => items
                .into_iter()
                .map(|v| match v {
                    toml::Value::String(s) => Some(s),
                    _ => None,
                })
                .collect(),
            _ => None,
        };
        match items {
            Some(items) if items.len() <= max => Some(items),
            Some(_) => {
                self.issue(key, "too_many_items");
                None
            }
            None => {
                self.issue(key, "expected_string_list");
                None
            }
        }
    }
    pub(crate) fn choice<T: Copy>(&mut self, key: &str, default: T, choices: &[(&str, T)]) -> T {
        let Some(value) = self.string(key) else {
            return default;
        };
        match choices.iter().find(|(name, _)| *name == value) {
            Some((_, choice)) => *choice,
            None => {
                self.issue(key, "invalid_choice");
                default
            }
        }
    }
    /// A nested table, or `None` when absent (or not a table, with an issue).
    pub(crate) fn table(&mut self, key: &str) -> Option<toml::Table> {
        match self.take(key)? {
            toml::Value::Table(table) => Some(table),
            _ => {
                self.issue(key, "expected_table");
                None
            }
        }
    }
    /// A child reader for the nested table `key`.
    pub(crate) fn child<'b>(
        &self,
        key: &str,
        table: toml::Table,
        issues: &'b mut Vec<ConfigIssue>,
    ) -> Reader<'b> {
        Reader::new(self.key(key), table, BTreeMap::new(), issues)
    }
    pub(crate) fn finish(self) {
        for key in self.table.keys() {
            let origin = self.origins.get(key).cloned().unwrap_or(Origin::File);
            self.issues.push(ConfigIssue::new(
                format!("{}.{key}", self.prefix),
                "unknown_key",
                origin,
            ));
        }
    }
}

/// `^[a-z]{2,3}(-[A-Z]{2})?$`.
pub fn valid_locale(locale: &str) -> bool {
    let (language, region) = match locale.split_once('-') {
        Some((language, region)) => (language, Some(region)),
        None => (locale, None),
    };
    (2..=3).contains(&language.len())
        && language.bytes().all(|b| b.is_ascii_lowercase())
        && region.is_none_or(|r| r.len() == 2 && r.bytes().all(|b| b.is_ascii_uppercase()))
}

/// Whether `host` is a loopback name or address.
pub(crate) fn loopback_host(host: &url::Host<&str>) -> bool {
    match host {
        url::Host::Domain(domain) => *domain == "localhost",
        url::Host::Ipv4(ip) => ip.is_loopback(),
        url::Host::Ipv6(ip) => IpAddr::V6(*ip).to_canonical().is_loopback(),
    }
}

/// A base URL: `https`, or `http` to a loopback host when `allow_http`;
/// no userinfo, query or fragment. Returned without a trailing slash.
fn base_url(text: &str, allow_loopback_http: bool) -> Result<(String, bool), &'static str> {
    let url = url::Url::parse(text).map_err(|_| "invalid_url")?;
    let host = url.host().ok_or("invalid_url")?;
    let loopback = loopback_host(&host);
    let scheme_ok = url.scheme() == "https" || (url.scheme() == "http" && loopback);
    if !scheme_ok
        || (url.scheme() == "http" && !allow_loopback_http)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || text.len() > 2048
    {
        return Err("invalid_url");
    }
    Ok((url.as_str().trim_end_matches('/').to_owned(), loopback))
}

/// Parse `Name <local@domain>` or `local@domain`.
pub fn parse_mailbox(text: &str) -> Option<Mailbox> {
    let text = text.trim();
    if text.chars().any(char::is_control) || text.len() > 320 {
        return None;
    }
    let (name, address) = match text.strip_suffix('>') {
        Some(rest) => {
            let (name, address) = rest.rsplit_once('<')?;
            let name = name.trim().trim_matches('"').trim();
            ((!name.is_empty()).then(|| name.to_owned()), address.trim())
        }
        None => (None, text),
    };
    let (local, domain) = address.split_once('@')?;
    let valid = !local.is_empty()
        && !domain.is_empty()
        && !domain.contains('@')
        && domain.contains('.') | (domain == "localhost")
        && address.len() <= 254
        && !address
            .chars()
            .any(|c| c.is_whitespace() || "<>\",;".contains(c));
    if name.as_ref().is_some_and(|n| n.contains(['<', '>'])) {
        return None;
    }
    valid.then(|| Mailbox {
        name,
        address: address.to_owned(),
    })
}

/// Brevo SMS sender: at most 11 alphanumeric characters, or at most 15 digits.
pub fn valid_sms_sender(sender: &str) -> bool {
    let digits = sender.bytes().all(|b| b.is_ascii_digit());
    !sender.is_empty()
        && sender.bytes().all(|b| b.is_ascii_alphanumeric())
        && sender.len() <= if digits { 15 } else { 11 }
}

/// Decode `secret_key`: base64 (standard or URL-safe, padding optional) of
/// at least 32 bytes.
fn key_material(secret: &Secret) -> Option<KeyMaterial> {
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
    let text = secret.expose().trim();
    [STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD]
        .iter()
        .find_map(|engine| engine.decode(text).ok())
        .filter(|bytes| bytes.len() >= 32 && bytes.len() <= 1024)
        .map(KeyMaterial)
}

/// A VAPID private key in shape: a PKCS#8 PEM, or base64url of 32 bytes.
fn vapid_shape(secret: &Secret) -> bool {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let text = secret.expose().trim();
    if text.starts_with("-----BEGIN PRIVATE KEY-----") {
        return text.ends_with("-----END PRIVATE KEY-----");
    }
    URL_SAFE_NO_PAD
        .decode(text.trim_end_matches('='))
        .is_ok_and(|bytes| bytes.len() == 32)
}

/// A lowercase DNS host name.
fn valid_host(host: &str) -> bool {
    (1..=253).contains(&host.len())
        && host.split('.').all(|label| {
            (1..=63).contains(&label.len())
                && label
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
                && !label.starts_with('-')
                && !label.ends_with('-')
        })
}

/// A role key such as `admin`.
fn valid_role(role: &str) -> bool {
    merezha_domain::valid_name(role) && role.len() <= 64
}

/// R11 event type names: `<module>.<aggregate>_<past verb>`, at most 128
/// bytes, `^[a-z][a-z0-9_]*(\.[a-z0-9_]+){1,4}$`.
pub fn valid_event_type(name: &str) -> bool {
    topics::valid_dotted_name(name)
}

/// Resolve a secret in a nested table with the kernel's rules, through a
/// section view of that table.
fn nested_secret(
    section: &ModuleSection,
    path: &str,
    table: &toml::Table,
    key: &str,
) -> Result<Option<Secret>, ConfigIssue> {
    let mut view = section.clone();
    view.name = format!("{}.{path}", section.name);
    view.table = table.clone();
    view.overridden = BTreeMap::new();
    view.secret(key)
}

/// Parse and validate `[modules.notifications]`, reporting every problem.
/// Template checks run separately, once the settings are known.
pub fn parse(section: &ModuleSection, facts: &HostFacts) -> Result<Settings, Vec<ConfigIssue>> {
    let (settings, issues) = parse_all(section, facts);
    if issues.is_empty() {
        Ok(settings)
    } else {
        Err(issues)
    }
}

/// The settings with every issue found. When a setting is invalid its
/// default stands in, so the template checks still run and report their
/// own problems in the same pass.
pub fn parse_all(section: &ModuleSection, facts: &HostFacts) -> (Settings, Vec<ConfigIssue>) {
    let mut issues = Vec::new();
    let settings = parse_into(section, facts, &mut issues);
    (settings, issues)
}

fn parse_into(
    section: &ModuleSection,
    facts: &HostFacts,
    issues: &mut Vec<ConfigIssue>,
) -> Settings {
    let prefix = section.key("").trim_end_matches('.').to_owned();
    let mut top = section.table.clone();
    // Secrets resolve through the section with the kernel rules.
    let secret_key = match section.secret("secret_key") {
        Ok(Some(secret)) => match key_material(&secret) {
            Some(key) => Some(key),
            None => {
                issues.push(ConfigIssue::new(
                    section.key("secret_key"),
                    "invalid_secret_key",
                    section.origin("secret_key"),
                ));
                None
            }
        },
        Ok(None) => {
            issues.push(ConfigIssue::new(
                section.key("secret_key"),
                "missing_required",
                Origin::Default,
            ));
            None
        }
        Err(issue) => {
            issues.push(issue);
            None
        }
    };
    let secret_key_previous = match section.secret("secret_key_previous") {
        Ok(Some(secret)) => match key_material(&secret) {
            Some(key) => Some(key),
            None => {
                issues.push(ConfigIssue::new(
                    section.key("secret_key_previous"),
                    "invalid_secret_key",
                    section.origin("secret_key_previous"),
                ));
                None
            }
        },
        Ok(None) => None,
        Err(issue) => {
            issues.push(issue);
            None
        }
    };
    top.remove("secret_key");
    top.remove("secret_key_previous");
    let mut r = Reader::new(prefix, top, section.overridden.clone(), issues);

    let public_base_url = match r.string("public_base_url") {
        Some(text) if text.is_empty() => None,
        Some(text) => match base_url(&text, true) {
            Ok((url, _)) => Some(url),
            Err(code) => {
                r.issue("public_base_url", code);
                None
            }
        },
        None => None,
    };

    // limits
    let limits = {
        let table = r.table("limits").unwrap_or_default();
        let mut issues = Vec::new();
        let mut l = r.child("limits", table, &mut issues);
        let max_targets = l.integer("max_targets", 1000, 1, 100_000) as usize;
        let max_marketing_targets = l.integer("max_marketing_targets", 1000, 1, 1000) as usize;
        let limits = Limits {
            max_recipients: l.integer("max_recipients", 100, 1, 100) as usize,
            max_targets,
            max_marketing_targets,
            max_data_bytes: l.integer("max_data_bytes", 32_768, 1024, 49_152) as usize,
        };
        if max_marketing_targets > max_targets {
            l.issue("max_marketing_targets", "out_of_range");
        }
        l.finish();
        r.issues.extend(issues);
        limits
    };

    // links
    let links = {
        let table = r.table("links").unwrap_or_default();
        let mut issues = Vec::new();
        let mut l = r.child("links", table, &mut issues);
        let page = |l: &mut Reader<'_>, key: &str| match l.string(key) {
            Some(text) if text.is_empty() => None,
            Some(text) => match base_url_with_path(&text) {
                Some(url) => Some(url),
                None => {
                    l.issue(key, "invalid_url");
                    None
                }
            },
            None => None,
        };
        let links = Links {
            unsubscribe_page: page(&mut l, "unsubscribe_page"),
            preferences_page: page(&mut l, "preferences_page"),
        };
        l.finish();
        r.issues.extend(issues);
        links
    };

    // egress
    let egress = {
        let table = r.table("egress").unwrap_or_default();
        let mut issues = Vec::new();
        let mut l = r.child("egress", table, &mut issues);
        let allow_private_networks = l.boolean("allow_private_networks", false);
        let mut deny_cidrs = Vec::new();
        for text in l.list("deny_cidrs", MAX_LIST).unwrap_or_default() {
            match text.parse::<Cidr>() {
                Ok(cidr) => deny_cidrs.push(cidr),
                Err(_) => l.issue("deny_cidrs", "invalid_cidr"),
            }
        }
        l.finish();
        r.issues.extend(issues);
        Egress {
            allow_private_networks,
            deny_cidrs,
        }
    };

    // retention
    let retention = {
        let table = r.table("retention").unwrap_or_default();
        let mut issues = Vec::new();
        let mut l = r.child("retention", table, &mut issues);
        let span = (Duration::from_secs(3600), Duration::from_secs(3650 * DAY));
        let retention = Retention {
            data_after_settled: l.duration(
                "data_after_settled",
                Duration::from_secs(7 * DAY),
                span.0,
                span.1,
            ),
            deliveries: l.duration("deliveries", Duration::from_secs(90 * DAY), span.0, span.1),
            webhook_deliveries: l.duration(
                "webhook_deliveries",
                Duration::from_secs(30 * DAY),
                span.0,
                span.1,
            ),
            captures: l.integer("captures", 1000, 1, 100_000) as u32,
        };
        l.finish();
        r.issues.extend(issues);
        retention
    };

    // templates
    let templates = {
        let table = r.table("templates").unwrap_or_default();
        let mut issues = Vec::new();
        let mut l = r.child("templates", table, &mut issues);
        let dir_explicit = l.has("dir");
        let dir = match l.string("dir") {
            Some(dir) if !dir.is_empty() && dir.len() <= 4096 => facts.base_dir.join(dir),
            Some(_) => {
                l.issue("dir", "invalid_path");
                facts.base_dir.join("notifications/templates")
            }
            None => facts.base_dir.join("notifications/templates"),
        };
        let default_locale = l.string("default_locale").unwrap_or_else(|| "en".into());
        if !valid_locale(&default_locale) {
            l.issue("default_locale", "invalid_locale");
        }
        let locales = l.list("locales", 32).unwrap_or_else(|| vec!["en".into()]);
        let mut seen = BTreeSet::new();
        for locale in &locales {
            if !valid_locale(locale) {
                l.issue("locales", "invalid_locale");
            } else if !seen.insert(locale.clone()) {
                l.issue("locales", "duplicate_name");
            }
        }
        if valid_locale(&default_locale) && !locales.contains(&default_locale) {
            l.issue("locales", "default_locale_not_listed");
        }
        let app_name = l
            .string("app_name")
            .or_else(|| facts.app_name.clone())
            .unwrap_or_else(|| "Merezha".into());
        if app_name.is_empty() || app_name.len() > 128 || app_name.chars().any(char::is_control) {
            l.issue("app_name", "invalid_name");
        }
        let app_base_url = match l.string("app_base_url") {
            Some(text) if text.is_empty() => None,
            Some(text) => match base_url_with_path(&text) {
                Some(url) => Some(url),
                None => {
                    l.issue("app_base_url", "invalid_url");
                    None
                }
            },
            None => None,
        };
        let settings = TemplateSettings {
            dir,
            dir_explicit,
            default_locale,
            locales,
            app_name,
            app_base_url,
            allow_http_links: l.boolean("allow_http_links", false),
            fuel: l.integer("fuel", 50_000, 1_000, 10_000_000),
            preview: l.boolean("preview", true),
            preview_in_public: l.boolean("preview_in_public", false),
        };
        l.finish();
        r.issues.extend(issues);
        settings
    };

    // email
    let email = {
        let table = r.table("email").unwrap_or_default();
        let mut issues = Vec::new();
        let mut l = r.child("email", table, &mut issues);
        let driver = l.choice("driver", Driver::Capture, DRIVERS);
        let from_present = l.has("from");
        let from = match l.string("from") {
            Some(text) => parse_mailbox(&text).unwrap_or_else(|| {
                l.issue("from", "invalid_sender");
                placeholder_mailbox()
            }),
            None => {
                if driver == Driver::Brevo && !from_present {
                    l.issue_default("from", "missing_required");
                }
                placeholder_mailbox()
            }
        };
        let reply_to = match l.string("reply_to") {
            Some(text) if text.is_empty() => None,
            Some(text) => {
                let parsed = parse_mailbox(&text);
                if parsed.is_none() {
                    l.issue("reply_to", "invalid_sender");
                }
                parsed
            }
            None => None,
        };
        l.finish();
        r.issues.extend(issues);
        EmailSettings {
            driver,
            from,
            reply_to,
        }
    };

    // sms
    let sms = {
        let table = r.table("sms").unwrap_or_default();
        let mut issues = Vec::new();
        let mut l = r.child("sms", table, &mut issues);
        let driver = l.choice("driver", Driver::Disabled, DRIVERS);
        let sender = l.string("sender");
        match &sender {
            Some(sender) if !valid_sms_sender(sender) => l.issue("sender", "invalid_sender"),
            None if driver != Driver::Disabled => l.issue_default("sender", "missing_required"),
            _ => {}
        }
        let organisation_prefix = match l.string("organisation_prefix") {
            Some(text) if text.is_empty() => None,
            Some(text) if text.len() <= 32 && !text.chars().any(char::is_control) => Some(text),
            Some(_) => {
                l.issue("organisation_prefix", "out_of_range");
                None
            }
            None => None,
        };
        let sms = SmsSettings {
            driver,
            sender: sender.unwrap_or_default(),
            organisation_prefix,
            max_segments: l.integer("max_segments", 4, 1, 10) as u32,
        };
        l.finish();
        r.issues.extend(issues);
        sms
    };

    // brevo
    let brevo_needed = email.driver == Driver::Brevo || sms.driver == Driver::Brevo;
    let brevo = match r.table("brevo") {
        Some(table) => {
            let mut issues = Vec::new();
            let api_key = nested_secret(section, "brevo", &table, "api_key")
                .map_err(|issue| issues.push(issue))
                .ok()
                .flatten();
            let webhook_token = nested_secret(section, "brevo", &table, "webhook_token")
                .map_err(|issue| issues.push(issue))
                .ok()
                .flatten();
            let mut rest = table;
            rest.remove("api_key");
            rest.remove("webhook_token");
            let mut l = r.child("brevo", rest, &mut issues);
            if brevo_needed && api_key.is_none() && !l.has_issue_for("api_key") {
                l.issue_default("api_key", "missing_required");
            }
            match &webhook_token {
                Some(token) if token.expose().len() < 32 => {
                    l.issue("webhook_token", "secret_too_short");
                }
                None if email.driver == Driver::Brevo && !l.has_issue_for("webhook_token") => {
                    l.issue_default("webhook_token", "missing_required");
                }
                _ => {}
            }
            let base_url = match l.string("base_url") {
                Some(text) => match base_url(&text, true) {
                    Ok((url, _)) => url,
                    Err(code) => {
                        l.issue("base_url", code);
                        "https://api.brevo.com".into()
                    }
                },
                None => "https://api.brevo.com".into(),
            };
            let timeout = l.duration(
                "timeout",
                Duration::from_secs(10),
                Duration::from_secs(1),
                Duration::from_secs(60),
            );
            let connect_timeout = l.duration(
                "connect_timeout",
                Duration::from_secs(5),
                Duration::from_millis(100),
                Duration::from_secs(30),
            );
            let sandbox = l.boolean("sandbox", false);
            let mut webhook_allowed_cidrs = Vec::new();
            for text in l
                .list("webhook_allowed_cidrs", MAX_LIST)
                .unwrap_or_default()
            {
                match text.parse::<Cidr>() {
                    Ok(cidr) => webhook_allowed_cidrs.push(cidr),
                    Err(_) => l.issue("webhook_allowed_cidrs", "invalid_cidr"),
                }
            }
            l.finish();
            r.issues.extend(issues);
            Some(BrevoSettings {
                api_key,
                base_url,
                timeout,
                connect_timeout,
                sandbox,
                webhook_token,
                webhook_allowed_cidrs,
            })
        }
        None => {
            if brevo_needed {
                r.issue_default("brevo", "missing_required");
            }
            None
        }
    };

    // push
    let push = match r.table("push") {
        Some(table) => {
            let mut issues = Vec::new();
            let key = nested_secret(section, "push", &table, "vapid_private_key")
                .map_err(|issue| issues.push(issue))
                .ok()
                .flatten();
            let mut rest = table;
            rest.remove("vapid_private_key");
            let mut l = r.child("push", rest, &mut issues);
            let vapid_private_key = match key {
                Some(key) if vapid_shape(&key) => Some(key),
                Some(_) => {
                    l.issue("vapid_private_key", "invalid_vapid_key");
                    None
                }
                None => {
                    if !l.has_issue_for("vapid_private_key") {
                        l.issue_default("vapid_private_key", "missing_required");
                    }
                    None
                }
            };
            let vapid_subject = match l.string("vapid_subject") {
                Some(subject) if valid_vapid_subject(&subject) => Some(subject),
                Some(_) => {
                    l.issue("vapid_subject", "invalid_url");
                    None
                }
                None => {
                    l.issue_default("vapid_subject", "missing_required");
                    None
                }
            };
            let ttl = l.duration(
                "ttl",
                Duration::from_secs(DAY),
                Duration::ZERO,
                Duration::from_secs(28 * DAY),
            );
            let allowed_hosts = match l.list("allowed_hosts", MAX_LIST) {
                Some(hosts) => {
                    if hosts.iter().any(|h| !valid_host(h)) {
                        l.issue("allowed_hosts", "invalid_host");
                    }
                    hosts
                }
                None => DEFAULT_PUSH_HOSTS.iter().map(|h| (*h).to_owned()).collect(),
            };
            let max_subscriptions_per_user = l.integer("max_subscriptions_per_user", 10, 1, 100);
            l.finish();
            r.issues.extend(issues);
            match (vapid_private_key, vapid_subject) {
                (Some(vapid_private_key), Some(vapid_subject)) => Some(PushSettings {
                    vapid_private_key,
                    vapid_subject,
                    ttl,
                    allowed_hosts,
                    max_subscriptions_per_user: max_subscriptions_per_user as u32,
                }),
                _ => None,
            }
        }
        None => None,
    };

    // inbox
    let inbox = {
        let table = r.table("inbox").unwrap_or_default();
        let mut issues = Vec::new();
        let mut l = r.child("inbox", table, &mut issues);
        let span = (Duration::from_secs(DAY), Duration::from_secs(3650 * DAY));
        let inbox = InboxSettings {
            retention: l.duration("retention", Duration::from_secs(180 * DAY), span.0, span.1),
            archived_retention: l.duration(
                "archived_retention",
                Duration::from_secs(30 * DAY),
                span.0,
                span.1,
            ),
        };
        l.finish();
        r.issues.extend(issues);
        inbox
    };

    // sse
    let sse = {
        let table = r.table("sse").unwrap_or_default();
        let mut issues = Vec::new();
        let mut l = r.child("sse", table, &mut issues);
        let s = Duration::from_secs;
        let sse = SseSettings {
            heartbeat: l.duration("heartbeat", s(15), s(1), s(300)),
            reconcile_interval: l.duration("reconcile_interval", s(30), s(1), s(600)),
            replay_max: l.integer("replay_max", 500, 1, 10_000) as u32,
            max_streams_per_user: l.integer("max_streams_per_user", 8, 1, 64) as u32,
            max_streams: l.integer("max_streams", 10_000, 1, 100_000) as u32,
            max_lifetime: l.duration("max_lifetime", s(3600), s(60), s(DAY)),
            reauth_interval: l.duration("reauth_interval", s(300), s(10), s(3600)),
        };
        l.finish();
        r.issues.extend(issues);
        sse
    };

    // webhooks
    let webhooks = match r.table("webhooks") {
        Some(table) => {
            let mut issues = Vec::new();
            let mut l = r.child("webhooks", table, &mut issues);
            let s = Duration::from_secs;
            let max_endpoints_per_tenant =
                l.integer("max_endpoints_per_tenant", 20, 1, 1000) as u32;
            let max_payload_bytes = l.integer("max_payload_bytes", 262_144, 1024, 1_048_576) as u32;
            let timeout = l.duration("timeout", s(10), s(1), s(60));
            let connect_timeout =
                l.duration("connect_timeout", s(5), Duration::from_millis(100), s(30));
            let concurrency_per_endpoint = l.integer("concurrency_per_endpoint", 4, 1, 64) as u32;
            let auto_disable_after =
                l.duration("auto_disable_after", s(5 * DAY), s(3600), s(30 * DAY));
            let allow_http = l.boolean("allow_http", false);
            let roles = |l: &mut Reader<'_>, key: &str, default: &[&str]| {
                let roles = l
                    .list(key, MAX_LIST)
                    .unwrap_or_else(|| default.iter().map(|r| (*r).to_owned()).collect());
                if roles.iter().any(|role| !valid_role(role)) {
                    l.issue(key, "invalid_role");
                }
                roles
            };
            let alert_roles = roles(&mut l, "alert_roles", &["owner", "admin"]);
            let application_alert_roles = roles(&mut l, "application_alert_roles", &[]);
            let mut event_types = BTreeMap::new();
            if let Some(table) = l.table("event_types") {
                if table.len() > MAX_TOPICS {
                    l.issue("event_types", "too_many_items");
                }
                for (name, value) in table {
                    let key = format!("event_types.{}", segment(&name));
                    if !valid_event_type(&name) || name == "webhook.test" {
                        l.issue(&key, "invalid_event_type");
                        continue;
                    }
                    let toml::Value::Table(entry) = value else {
                        l.issue(&key, "expected_table");
                        continue;
                    };
                    let mut entry_issues = Vec::new();
                    let mut e = l.child(&key, entry, &mut entry_issues);
                    let description = e.string("description").unwrap_or_default();
                    if description.len() > 200 || description.chars().any(char::is_control) {
                        e.issue("description", "out_of_range");
                    }
                    e.finish();
                    l.issues.extend(entry_issues);
                    event_types.insert(name, description);
                }
            }
            l.finish();
            r.issues.extend(issues);
            Some(WebhookSettings {
                max_endpoints_per_tenant,
                max_payload_bytes,
                timeout,
                connect_timeout,
                concurrency_per_endpoint,
                auto_disable_after,
                allow_http,
                alert_roles,
                application_alert_roles,
                event_types,
            })
        }
        None => None,
    };

    // quiet hours are NT-N9, deferred from v1.
    if r.table("quiet_hours").is_some() {
        r.issue("quiet_hours", "unsupported_section");
    }

    // topics
    let declared = r.table("topics").unwrap_or_default();
    let topics = topics::parse(&r.key("topics"), declared, r.issues);

    r.finish();

    let settings = Settings {
        secret_key: secret_key.unwrap_or_else(|| KeyMaterial(Vec::new())),
        secret_key_previous,
        public_base_url,
        limits,
        links,
        egress,
        retention,
        templates,
        email,
        sms,
        brevo,
        push,
        inbox,
        sse,
        webhooks,
        topics,
    };
    cross_checks(section, &settings, facts, issues);
    settings
}

const DRIVERS: &[(&str, Driver)] = &[
    ("brevo", Driver::Brevo),
    ("capture", Driver::Capture),
    ("disabled", Driver::Disabled),
];
/// The push services browsers use today (NT §7.2).
pub const DEFAULT_PUSH_HOSTS: [&str; 5] = [
    "fcm.googleapis.com",
    "updates.push.services.mozilla.com",
    "push.services.mozilla.com",
    "web.push.apple.com",
    "notify.windows.com",
];

fn placeholder_mailbox() -> Mailbox {
    Mailbox {
        name: None,
        address: "no-reply@localhost".into(),
    }
}

/// An application page URL: `https` (or `http` to loopback), with a path,
/// no userinfo or fragment.
fn base_url_with_path(text: &str) -> Option<String> {
    let url = url::Url::parse(text).ok()?;
    let host = url.host()?;
    let ok = (url.scheme() == "https" || (url.scheme() == "http" && loopback_host(&host)))
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
        && text.len() <= 2048;
    ok.then(|| text.to_owned())
}

fn valid_vapid_subject(subject: &str) -> bool {
    if let Some(address) = subject.strip_prefix("mailto:") {
        return parse_mailbox(address).is_some_and(|m| m.name.is_none());
    }
    subject.starts_with("https://") && url::Url::parse(subject).is_ok() && subject.len() <= 512
}

impl Reader<'_> {
    /// Whether an issue for `key` was already recorded.
    fn has_issue_for(&self, key: &str) -> bool {
        let full = self.key(key);
        self.issues.iter().any(|i| i.key == full)
    }
}

/// The effective settings of `queue`: its own table or `[jobs.defaults]`.
fn queue_settings(jobs: &JobsConfig, queue: &str) -> (JobDefaults, bool) {
    match jobs.queues.get(queue) {
        Some(config) => (config.settings.clone(), true),
        None => (jobs.defaults.clone(), false),
    }
}

/// Checks across sections and against the host (NT §7.2).
fn cross_checks(
    section: &ModuleSection,
    settings: &Settings,
    facts: &HostFacts,
    issues: &mut Vec<ConfigIssue>,
) {
    // Issues name the module key with the layer that set it, or `default`.
    let at = |path: &[&str], code: &'static str| {
        let dotted: Vec<String> = path.iter().map(|p| segment(p)).collect();
        ConfigIssue::new(
            section.key(&dotted.join(".")),
            code,
            module_origin(section, path),
        )
    };

    // Topic channels need a driver and a section.
    for (name, topic) in &settings.topics {
        if !topic.declared {
            continue;
        }
        for channel in &topic.channels {
            if !settings.channel_available(*channel) {
                issues.push(at(&["topics", name, "channels"], "channel_unavailable"));
                break;
            }
        }
    }

    // `http.public` refuses development conveniences.
    if facts.http_public == Some(true) {
        if settings.email.driver == Driver::Capture {
            issues.push(at(&["email", "driver"], "refused_when_public"));
        }
        if settings.sms.driver == Driver::Capture {
            issues.push(at(&["sms", "driver"], "refused_when_public"));
        }
        if settings.egress.allow_private_networks {
            issues.push(at(
                &["egress", "allow_private_networks"],
                "refused_when_public",
            ));
        }
        if settings.webhooks.as_ref().is_some_and(|w| w.allow_http) {
            issues.push(at(&["webhooks", "allow_http"], "refused_when_public"));
        }
        if settings.templates.allow_http_links {
            issues.push(at(
                &["templates", "allow_http_links"],
                "refused_when_public",
            ));
        }
        if settings
            .brevo
            .as_ref()
            .is_some_and(|b| !b.base_url.starts_with("https://"))
        {
            issues.push(at(&["brevo", "base_url"], "refused_when_public"));
        }
        if settings.templates.preview && !settings.templates.preview_in_public {
            issues.push(at(&["templates", "preview"], "refused_when_public"));
        }
    }

    // Unsubscribe links (non-security email through Brevo) and the Brevo
    // SMS status URL need the public base URL.
    let unsubscribe_needed = settings.email.driver == Driver::Brevo
        && settings
            .topics
            .values()
            .any(|t| t.category != Category::Security && t.channels.contains(&Channel::Email));
    if settings.public_base_url.is_none()
        && (unsubscribe_needed || settings.sms.driver == Driver::Brevo)
    {
        issues.push(at(&["public_base_url"], "missing_required"));
    }

    // Queue leases cover three provider deadlines; the webhook ladder needs
    // a 24-hour backoff cap (R1).
    if let Some(jobs) = &facts.jobs {
        let queue_issue = |queue: &str, k: &str, code: &'static str| {
            let key = format!("jobs.queues.\"{queue}\".{k}");
            let origin = facts
                .jobs_origins
                .get(&key)
                .cloned()
                .unwrap_or(Origin::Default);
            ConfigIssue::new(key, code, origin)
        };
        if let Some(brevo) = &settings.brevo {
            for (queue, driver) in [
                ("notify.email", settings.email.driver),
                ("notify.sms", settings.sms.driver),
            ] {
                let (effective, _) = queue_settings(jobs, queue);
                if driver == Driver::Brevo && effective.lease < brevo.timeout * 3 {
                    issues.push(queue_issue(queue, "lease_ms", "lease_too_short"));
                }
            }
        }
        if let Some(webhooks) = &settings.webhooks {
            let (effective, _) = queue_settings(jobs, "notify.webhook");
            if effective.lease < webhooks.timeout * 3 {
                issues.push(queue_issue("notify.webhook", "lease_ms", "lease_too_short"));
            }
            if effective.backoff_max < Duration::from_secs(DAY) {
                issues.push(queue_issue(
                    "notify.webhook",
                    "backoff_max_ms",
                    "backoff_max_too_short",
                ));
            }
        }
    }

    // CI: no provider outside loopback.
    if facts.no_egress
        && let Some(brevo) = &settings.brevo
    {
        let loopback = url::Url::parse(&brevo.base_url)
            .ok()
            .and_then(|u| u.host().map(|h| loopback_host(&h)))
            .unwrap_or(false);
        if !loopback {
            issues.push(at(&["brevo", "base_url"], "egress_not_loopback"));
        }
    }
}

/// The layer that set `path` in the section: the override's origin for a
/// top-level key, `file` for a key present in the table, else `default`.
fn module_origin(section: &ModuleSection, path: &[&str]) -> Origin {
    if let [key] = path
        && let Some(origin) = section.overridden.get(*key)
    {
        return origin.clone();
    }
    let Some((last, parents)) = path.split_last() else {
        return Origin::Default;
    };
    let mut table = &section.table;
    for parent in parents {
        match table.get(*parent).and_then(toml::Value::as_table) {
            Some(child) => table = child,
            None => return Origin::Default,
        }
    }
    if table.contains_key(*last) {
        Origin::File
    } else {
        Origin::Default
    }
}
