//! `merezha.toml`, the typed project configuration.
//!
//! Layers, lowest to highest precedence: code defaults, the file,
//! `MEREZHA_<SECTION>__<KEY>` environment variables, then command-line
//! overrides. Every problem in every layer is collected and reported at once
//! as [`ConfigErrors`]; nothing is reported by value, so secrets never appear
//! in errors. Secret settings accept `{ env = "NAME" }` or `{ file = "PATH" }`
//! in the file, never a literal.
use crate::net::Cidr;
use merezha_postgres::{DatabaseConfig, Secret, TlsMode};
use std::{
    collections::BTreeMap,
    io::Read,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

/// Largest accepted configuration file.
pub const MAX_CONFIG_BYTES: u64 = 262_144;
/// Largest accepted secret file.
pub const MAX_SECRET_BYTES: u64 = 65_536;
/// Prefix of configuration environment variables. Only names that also
/// contain `__` are configuration; `MEREZHA_TEST_DATABASE_URL` is not.
pub const ENV_PREFIX: &str = "MEREZHA_";
const SECTIONS: [&str; 7] = [
    "app",
    "http",
    "database",
    "runtime",
    "application",
    "telemetry",
    "modules",
];

/// Where a setting came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Origin {
    Default,
    File,
    /// The environment variable name, never its value.
    Env(String),
    Flag,
}
impl std::fmt::Display for Origin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Origin::Default => f.write_str("default"),
            Origin::File => f.write_str("file"),
            Origin::Env(name) => write!(f, "env {name}"),
            Origin::Flag => f.write_str("flag"),
        }
    }
}

/// One configuration problem: the dotted setting, a stable code and the
/// layer that supplied it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigIssue {
    pub key: String,
    pub code: &'static str,
    pub origin: Origin,
}
impl ConfigIssue {
    pub fn new(key: impl Into<String>, code: &'static str, origin: Origin) -> Self {
        Self {
            key: key.into(),
            code,
            origin,
        }
    }
}
impl std::fmt::Display for ConfigIssue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {} ({})", self.key, self.code, self.origin)
    }
}
/// Every problem found while loading, in a stable order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConfigErrors(pub Vec<ConfigIssue>);
impl ConfigErrors {
    pub fn codes(&self) -> Vec<(&str, &str)> {
        self.0.iter().map(|i| (i.key.as_str(), i.code)).collect()
    }
}
impl std::fmt::Display for ConfigErrors {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, issue) in self.0.iter().enumerate() {
            if index > 0 {
                f.write_str("; ")?;
            }
            write!(f, "{issue}")?;
        }
        Ok(())
    }
}
impl std::error::Error for ConfigErrors {}

/// Inputs to [`ProjectConfig::load`]. Pass `std::env::vars()` in production;
/// tests pass explicit pairs.
#[derive(Clone, Debug, Default)]
pub struct ConfigInput {
    pub path: PathBuf,
    pub env: Vec<(String, String)>,
    /// Command-line overrides as `("section.key", value)`.
    pub overrides: Vec<(String, String)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppConfig {
    /// Lowercase label `[a-z0-9-]`, 1 to 63 bytes, starting with a letter.
    pub id: String,
    pub name: String,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CorsConfig {
    /// Exact origins such as `https://app.example.com`, or `["*"]`.
    pub allowed_origins: Vec<String>,
    pub allow_credentials: bool,
    pub max_age: Duration,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HttpConfig {
    pub listen: SocketAddr,
    pub public: bool,
    pub trusted_proxies: Vec<Cidr>,
    pub body_limit: usize,
    pub cors: CorsConfig,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeConfig {
    pub manifest: PathBuf,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationConfig {
    pub path: PathBuf,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
    Json,
    Text,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TelemetryConfig {
    pub log_level: LogLevel,
    pub format: LogFormat,
}

/// A module's opaque `[modules.<name>]` table, validated by that module.
#[derive(Clone)]
pub struct ModuleSection {
    pub name: String,
    pub table: toml::Table,
    /// Keys whose value came from an environment variable or flag, with
    /// that origin. A secret key from the environment holds the secret itself.
    pub overridden: BTreeMap<String, Origin>,
    resolver: Arc<SecretResolver>,
}
impl std::fmt::Debug for ModuleSection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModuleSection")
            .field("name", &self.name)
            .field("keys", &self.table.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}
impl ModuleSection {
    /// An empty section, for tests and modules enabled without settings.
    pub fn empty(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            table: toml::Table::new(),
            overridden: BTreeMap::new(),
            resolver: Arc::new(SecretResolver::default()),
        }
    }
    /// Dotted key for issues about `key` in this section.
    pub fn key(&self, key: &str) -> String {
        format!("modules.{}.{key}", self.name)
    }
    /// Resolve a secret setting with the same rules as kernel settings.
    pub fn secret(&self, key: &str) -> Result<Option<Secret>, ConfigIssue> {
        let Some(value) = self.table.get(key) else {
            return Ok(None);
        };
        let origin = self.origin(key);
        self.resolver
            .resolve(value, &origin)
            .map(Some)
            .map_err(|code| ConfigIssue::new(self.key(key), code, origin))
    }
    /// The layer that supplied `key`.
    pub fn origin(&self, key: &str) -> Origin {
        self.overridden.get(key).cloned().unwrap_or(Origin::File)
    }
    /// Issues for every key not in `known`.
    pub fn unknown_keys(&self, known: &[&str]) -> Vec<ConfigIssue> {
        self.table
            .keys()
            .filter(|k| !known.contains(&k.as_str()))
            .map(|k| ConfigIssue::new(self.key(k), "unknown_key", self.origin(k)))
            .collect()
    }
}

/// The validated project. `Debug` redacts secrets.
#[derive(Clone, Debug)]
pub struct ProjectConfig {
    /// Canonical directory of the configuration file; relative paths resolve here.
    pub base_dir: PathBuf,
    pub app: AppConfig,
    pub http: HttpConfig,
    pub database: DatabaseConfig,
    pub runtime: RuntimeConfig,
    pub application: Option<ApplicationConfig>,
    pub modules: BTreeMap<String, ModuleSection>,
    pub telemetry: TelemetryConfig,
}

#[derive(Clone)]
struct Entry {
    value: toml::Value,
    origin: Origin,
}
type Section = BTreeMap<String, Entry>;

#[derive(Default)]
struct SecretResolver {
    base_dir: PathBuf,
    env: BTreeMap<String, String>,
}
impl SecretResolver {
    fn resolve(&self, value: &toml::Value, origin: &Origin) -> Result<Secret, &'static str> {
        match (value, origin) {
            (toml::Value::String(value), Origin::Env(_)) if !value.is_empty() => {
                Ok(Secret::new(value.clone()))
            }
            (toml::Value::String(_), Origin::Flag) => Err("secret_on_command_line"),
            (toml::Value::String(_), _) => Err("secret_literal"),
            (toml::Value::Table(table), _) if table.len() == 1 => match table.iter().next() {
                Some((kind, toml::Value::String(name))) if kind == "env" => self
                    .env
                    .get(name)
                    .filter(|v| !v.is_empty())
                    .map(|v| Secret::new(v.clone()))
                    .ok_or("secret_unavailable"),
                Some((kind, toml::Value::String(path))) if kind == "file" => {
                    self.read(Path::new(path))
                }
                _ => Err("invalid_secret_reference"),
            },
            _ => Err("invalid_secret_reference"),
        }
    }
    fn read(&self, path: &Path) -> Result<Secret, &'static str> {
        let path = self.base_dir.join(path);
        let file = std::fs::File::open(path).map_err(|_| "secret_unavailable")?;
        if !file.metadata().map_err(|_| "secret_unavailable")?.is_file() {
            return Err("secret_unavailable");
        }
        let mut text = String::new();
        file.take(MAX_SECRET_BYTES + 1)
            .read_to_string(&mut text)
            .map_err(|_| "secret_unavailable")?;
        if text.len() as u64 > MAX_SECRET_BYTES {
            return Err("secret_too_large");
        }
        let value = text.strip_suffix('\n').unwrap_or(&text);
        let value = value.strip_suffix('\r').unwrap_or(value);
        if value.is_empty() {
            return Err("secret_unavailable");
        }
        Ok(Secret::new(value))
    }
}

/// Typed reads from one section, collecting issues and tracking leftovers.
struct Reader<'a> {
    prefix: &'a str,
    entries: Section,
    issues: &'a mut Vec<ConfigIssue>,
}
impl Reader<'_> {
    fn issue(&mut self, key: &str, code: &'static str, origin: Origin) {
        self.issues.push(ConfigIssue::new(
            format!("{}.{key}", self.prefix),
            code,
            origin,
        ));
    }
    fn take(&mut self, key: &str) -> Option<Entry> {
        self.entries.remove(key)
    }
    /// A string-typed value, or an issue for any other type.
    fn text(&mut self, key: &str) -> Option<(String, Origin)> {
        let entry = self.take(key)?;
        match entry.value {
            toml::Value::String(value) => Some((value, entry.origin)),
            _ => {
                self.issue(key, "expected_string", entry.origin);
                None
            }
        }
    }
    fn string(&mut self, key: &str) -> Option<(String, Origin)> {
        self.text(key)
    }
    fn boolean(&mut self, key: &str, default: bool) -> bool {
        let Some(entry) = self.take(key) else {
            return default;
        };
        match (&entry.value, &entry.origin) {
            (toml::Value::Boolean(value), _) => *value,
            (toml::Value::String(value), Origin::Env(_) | Origin::Flag) => match value.as_str() {
                "true" => true,
                "false" => false,
                _ => {
                    self.issue(key, "expected_boolean", entry.origin);
                    default
                }
            },
            _ => {
                self.issue(key, "expected_boolean", entry.origin);
                default
            }
        }
    }
    fn integer(&mut self, key: &str, default: u64, min: u64, max: u64) -> u64 {
        let Some(entry) = self.take(key) else {
            return default;
        };
        let value = match (&entry.value, &entry.origin) {
            (toml::Value::Integer(value), _) => u64::try_from(*value).ok(),
            (toml::Value::String(value), Origin::Env(_) | Origin::Flag) => value.parse().ok(),
            _ => None,
        };
        match value {
            Some(value) if (min..=max).contains(&value) => value,
            Some(_) => {
                self.issue(key, "out_of_range", entry.origin);
                default
            }
            None => {
                self.issue(key, "expected_integer", entry.origin);
                default
            }
        }
    }
    fn millis(
        &mut self,
        key: &str,
        default: Duration,
        (min, max): (Duration, Duration),
    ) -> Duration {
        Duration::from_millis(self.integer(
            key,
            default.as_millis() as u64,
            min.as_millis() as u64,
            max.as_millis() as u64,
        ))
    }
    fn list(&mut self, key: &str, max: usize) -> Vec<(String, Origin)> {
        let Some(entry) = self.take(key) else {
            return Vec::new();
        };
        let items: Option<Vec<String>> = match (&entry.value, &entry.origin) {
            (toml::Value::Array(items), _) => items
                .iter()
                .map(|v| v.as_str().map(str::to_owned))
                .collect(),
            (toml::Value::String(value), Origin::Env(_) | Origin::Flag) => Some(
                value
                    .split(',')
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .collect(),
            ),
            _ => None,
        };
        match items {
            Some(items) if items.len() <= max => items
                .into_iter()
                .map(|item| (item, entry.origin.clone()))
                .collect(),
            Some(_) => {
                self.issue(key, "too_many_items", entry.origin);
                Vec::new()
            }
            None => {
                self.issue(key, "expected_string_list", entry.origin);
                Vec::new()
            }
        }
    }
    fn choice<T: Copy>(&mut self, key: &str, default: T, choices: &[(&str, T)]) -> T {
        let Some((value, origin)) = self.text(key) else {
            return default;
        };
        match choices.iter().find(|(name, _)| *name == value) {
            Some((_, choice)) => *choice,
            None => {
                self.issue(key, "invalid_choice", origin);
                default
            }
        }
    }
    fn path(&mut self, key: &str, base: &Path) -> Option<PathBuf> {
        let (value, origin) = self.text(key)?;
        if value.is_empty() || value.len() > 4096 {
            self.issue(key, "invalid_path", origin);
            return None;
        }
        Some(base.join(value))
    }
    fn secret(&mut self, key: &str, resolver: &SecretResolver) -> Option<Secret> {
        let Some(entry) = self.take(key) else {
            self.issue(key, "missing_required", Origin::Default);
            return None;
        };
        match resolver.resolve(&entry.value, &entry.origin) {
            Ok(secret) => Some(secret),
            Err(code) => {
                self.issue(key, code, entry.origin);
                None
            }
        }
    }
    fn required<T>(&mut self, key: &str, value: Option<T>, present: bool) -> Option<T> {
        if value.is_none() && !present {
            self.issue(key, "missing_required", Origin::Default);
        }
        value
    }
    fn finish(self) {
        for (key, entry) in self.entries {
            self.issues.push(ConfigIssue::new(
                format!("{}.{key}", self.prefix),
                "unknown_key",
                entry.origin,
            ));
        }
    }
}

fn valid_app_id(id: &str) -> bool {
    (1..=63).contains(&id.len())
        && id.as_bytes()[0].is_ascii_lowercase()
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}
/// Module names are section names, route segments and migration keys.
pub fn valid_module_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && name.as_bytes()[0].is_ascii_lowercase()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}
fn valid_origin(origin: &str) -> bool {
    let Some(rest) = origin
        .strip_prefix("https://")
        .or_else(|| origin.strip_prefix("http://"))
    else {
        return false;
    };
    !rest.is_empty()
        && rest.len() <= 253
        && rest
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"-.:[]".contains(&b))
}

/// TOML literal for module overrides, falling back to a plain string.
fn module_value(raw: &str) -> toml::Value {
    toml::from_str::<toml::Table>(&format!("v = {raw}"))
        .ok()
        .and_then(|mut t| t.remove("v"))
        .filter(|v| !v.is_table())
        .unwrap_or_else(|| toml::Value::String(raw.to_owned()))
}

struct Layers {
    sections: BTreeMap<String, Section>,
    modules: BTreeMap<String, (toml::Table, BTreeMap<String, Origin>)>,
}
impl Layers {
    fn set_entry(&mut self, section: &str, key: String, value: toml::Value, origin: Origin) {
        self.sections
            .entry(section.to_owned())
            .or_default()
            .insert(key, Entry { value, origin });
    }
    /// Apply one override at `path` (lowercase segments).
    fn set(
        &mut self,
        path: &[String],
        value: String,
        origin: Origin,
        issues: &mut Vec<ConfigIssue>,
    ) {
        let dotted = path.join(".");
        match path {
            [modules, name, key]
                if modules == "modules" && valid_module_name(name) && !key.is_empty() =>
            {
                let (table, overridden) = self.modules.entry(name.clone()).or_default();
                table.insert(key.clone(), module_value(&value));
                overridden.insert(key.clone(), origin);
            }
            [http, cors, key] if http == "http" && cors == "cors" && !key.is_empty() => {
                self.set_entry("http.cors", key.clone(), toml::Value::String(value), origin);
            }
            [section, key]
                if SECTIONS.contains(&section.as_str())
                    && section != "modules"
                    && !key.is_empty() =>
            {
                self.set_entry(section, key.clone(), toml::Value::String(value), origin);
            }
            _ => issues.push(ConfigIssue::new(dotted, "unknown_key", origin)),
        }
    }
}

impl ProjectConfig {
    /// Load, layer and validate the configuration. Kernel sections only;
    /// [`crate::host::configure`] adds module validation.
    pub fn load(input: &ConfigInput) -> Result<Self, ConfigErrors> {
        into_result(load_parts(input))
    }

    /// Validate configuration text as if read from a file in `base_dir`.
    pub fn parse(
        text: &str,
        base_dir: PathBuf,
        env: &[(String, String)],
        overrides: &[(String, String)],
    ) -> Result<Self, ConfigErrors> {
        into_result(parse_parts(text, base_dir, env, overrides))
    }
}

/// A load attempt: the typed config when the kernel sections built, the
/// module sections whenever the file parsed, and every issue found.
pub(crate) struct Parts {
    pub config: Option<ProjectConfig>,
    pub modules: BTreeMap<String, ModuleSection>,
    pub issues: Vec<ConfigIssue>,
}
fn into_result(parts: Parts) -> Result<ProjectConfig, ConfigErrors> {
    match parts.config {
        Some(config) if parts.issues.is_empty() => Ok(config),
        _ => Err(ConfigErrors(parts.issues)),
    }
}
fn failed(code: &'static str) -> Parts {
    Parts {
        config: None,
        modules: BTreeMap::new(),
        issues: vec![ConfigIssue::new("config", code, Origin::File)],
    }
}

pub(crate) fn load_parts(input: &ConfigInput) -> Parts {
    let Ok(handle) = std::fs::File::open(&input.path) else {
        return failed("config_unreadable");
    };
    if !handle.metadata().is_ok_and(|m| m.is_file()) {
        return failed("config_unreadable");
    }
    let mut text = String::new();
    if handle
        .take(MAX_CONFIG_BYTES + 1)
        .read_to_string(&mut text)
        .is_err()
    {
        return failed("config_unreadable");
    }
    if text.len() as u64 > MAX_CONFIG_BYTES {
        return failed("config_too_large");
    }
    let Some(base_dir) = input
        .path
        .canonicalize()
        .ok()
        .and_then(|p| p.parent().map(Path::to_path_buf))
    else {
        return failed("config_unreadable");
    };
    parse_parts(&text, base_dir, &input.env, &input.overrides)
}

fn parse_parts(
    text: &str,
    base_dir: PathBuf,
    env: &[(String, String)],
    overrides: &[(String, String)],
) -> Parts {
    {
        let document: toml::Table = match toml::from_str(text) {
            Ok(document) => document,
            Err(error) => {
                // Report the position only: the message may quote a secret.
                let line = error
                    .span()
                    .map(|span| text[..span.start.min(text.len())].lines().count().max(1))
                    .unwrap_or(0);
                return Parts {
                    config: None,
                    modules: BTreeMap::new(),
                    issues: vec![ConfigIssue::new(
                        format!("line {line}"),
                        "invalid_toml",
                        Origin::File,
                    )],
                };
            }
        };
        let mut issues = Vec::new();
        let mut layers = Layers {
            sections: BTreeMap::new(),
            modules: BTreeMap::new(),
        };
        for (name, value) in document {
            let toml::Value::Table(table) = value else {
                let code = if SECTIONS.contains(&name.as_str()) {
                    "expected_table"
                } else {
                    "unknown_section"
                };
                issues.push(ConfigIssue::new(name, code, Origin::File));
                continue;
            };
            match name.as_str() {
                "modules" => {
                    for (module, section) in table {
                        match section {
                            toml::Value::Table(section) if valid_module_name(&module) => {
                                layers.modules.insert(module, (section, BTreeMap::new()));
                            }
                            toml::Value::Table(_) => issues.push(ConfigIssue::new(
                                format!("modules.{module}"),
                                "invalid_module_name",
                                Origin::File,
                            )),
                            _ => issues.push(ConfigIssue::new(
                                format!("modules.{module}"),
                                "expected_table",
                                Origin::File,
                            )),
                        }
                    }
                }
                section if SECTIONS.contains(&section) => {
                    for (key, value) in table {
                        let (target, key, value) = match (section, key.as_str(), value) {
                            ("http", "cors", toml::Value::Table(cors)) => {
                                for (key, value) in cors {
                                    layers.set_entry("http.cors", key, value, Origin::File);
                                }
                                continue;
                            }
                            ("http", "cors", _) => {
                                issues.push(ConfigIssue::new(
                                    "http.cors",
                                    "expected_table",
                                    Origin::File,
                                ));
                                continue;
                            }
                            (_, _, value) => (section, key, value),
                        };
                        layers.set_entry(target, key, value, Origin::File);
                    }
                }
                _ => issues.push(ConfigIssue::new(name, "unknown_section", Origin::File)),
            }
        }
        let mut env_map = BTreeMap::new();
        let mut env_overrides = Vec::new();
        for (name, value) in env {
            env_map.insert(name.clone(), value.clone());
            if let Some(rest) = name.strip_prefix(ENV_PREFIX)
                && rest.contains("__")
            {
                let path: Vec<String> = rest.split("__").map(str::to_ascii_lowercase).collect();
                env_overrides.push((path, value.clone(), Origin::Env(name.clone())));
            }
        }
        // Stable application order regardless of the platform's env order.
        env_overrides.sort_by(|a, b| a.0.cmp(&b.0));
        for (path, value, origin) in env_overrides {
            layers.set(&path, value, origin, &mut issues);
        }
        for (key, value) in overrides {
            let path: Vec<String> = key.split('.').map(str::to_owned).collect();
            layers.set(&path, value.clone(), Origin::Flag, &mut issues);
        }
        let resolver = Arc::new(SecretResolver {
            base_dir: base_dir.clone(),
            env: env_map,
        });
        let (config, modules) = build(&mut layers, &base_dir, &resolver, &mut issues);
        Parts {
            config,
            modules,
            issues,
        }
    }
}

fn reader<'a>(
    layers: &mut Layers,
    name: &'a str,
    issues: &'a mut Vec<ConfigIssue>,
) -> (Reader<'a>, bool) {
    let entries = layers.sections.remove(name);
    let present = entries.is_some();
    (
        Reader {
            prefix: name,
            entries: entries.unwrap_or_default(),
            issues,
        },
        present,
    )
}

fn build(
    layers: &mut Layers,
    base_dir: &Path,
    resolver: &Arc<SecretResolver>,
    issues: &mut Vec<ConfigIssue>,
) -> (Option<ProjectConfig>, BTreeMap<String, ModuleSection>) {
    // [app]
    let (mut r, _) = reader(layers, "app", issues);
    let id = r.string("id");
    let id = match id {
        Some((id, _)) if valid_app_id(&id) => Some(id),
        Some((_, origin)) => {
            r.issue("id", "invalid_app_id", origin);
            None
        }
        None => r.required("id", None::<String>, false),
    };
    let name = match r.string("name") {
        Some((name, _)) if (1..=128).contains(&name.len()) => Some(name),
        Some((_, origin)) => {
            r.issue("name", "invalid_name", origin);
            None
        }
        None => id.clone(),
    };
    r.finish();

    // [http] and [http.cors]
    let (mut r, _) = reader(layers, "http", issues);
    let listen = match r.string("listen") {
        Some((value, origin)) => match value.parse::<SocketAddr>() {
            Ok(address) => Some((address, origin)),
            Err(_) => {
                r.issue("listen", "invalid_address", origin);
                None
            }
        },
        None => Some((SocketAddr::from(([127, 0, 0, 1], 8080)), Origin::Default)),
    };
    let public = r.boolean("public", false);
    if let Some((address, origin)) = &listen
        && !address.ip().is_loopback()
        && !public
    {
        r.issue("listen", "public_listen_not_enabled", origin.clone());
    }
    let mut trusted_proxies = Vec::new();
    for (value, origin) in r.list("trusted_proxies", 64) {
        match value.parse::<Cidr>() {
            Ok(cidr) => trusted_proxies.push(cidr),
            Err(_) => r.issue("trusted_proxies", "invalid_cidr", origin),
        }
    }
    let body_limit = r.integer("body_limit", 1_048_576, 1024, 67_108_864) as usize;
    r.finish();
    let (mut r, _) = reader(layers, "http.cors", issues);
    let origins = r.list("allowed_origins", 64);
    let allow_credentials = r.boolean("allow_credentials", false);
    let max_age = Duration::from_secs(r.integer("max_age_seconds", 600, 0, 86_400));
    let wildcard = origins.iter().any(|(o, _)| o == "*");
    for (origin_value, origin) in &origins {
        if origin_value != "*" && !valid_origin(origin_value) {
            r.issue("allowed_origins", "invalid_origin", origin.clone());
        }
    }
    if wildcard && (origins.len() > 1 || allow_credentials) {
        let origin = origins[0].1.clone();
        r.issue("allowed_origins", "invalid_wildcard_origin", origin);
    }
    r.finish();
    let cors = CorsConfig {
        allowed_origins: origins.into_iter().map(|(o, _)| o).collect(),
        allow_credentials,
        max_age,
    };

    // [database]
    let (mut r, _) = reader(layers, "database", issues);
    let url = r.secret("url", resolver);
    let defaults = DatabaseConfig::new(Secret::new(String::new()));
    let pool_size = r.integer(
        "pool_size",
        defaults.pool_size as u64,
        merezha_postgres::limits::POOL_SIZE.0 as u64,
        merezha_postgres::limits::POOL_SIZE.1 as u64,
    ) as usize;
    let connect_timeout = r.millis(
        "connect_timeout_ms",
        defaults.connect_timeout,
        merezha_postgres::limits::CONNECT_TIMEOUT,
    );
    let acquire_timeout = r.millis(
        "acquire_timeout_ms",
        defaults.acquire_timeout,
        merezha_postgres::limits::ACQUIRE_TIMEOUT,
    );
    let statement_timeout = r.millis(
        "statement_timeout_ms",
        defaults.statement_timeout,
        merezha_postgres::limits::STATEMENT_TIMEOUT,
    );
    let migrate = r.boolean("migrate", false);
    let tls = r.choice(
        "tls",
        TlsMode::Disable,
        &[("disable", TlsMode::Disable), ("require", TlsMode::Require)],
    );
    let tls_ca_file = r.path("tls_ca_file", base_dir);
    let database = url.map(|url| DatabaseConfig {
        url,
        pool_size,
        connect_timeout,
        acquire_timeout,
        statement_timeout,
        migrate,
        tls,
        tls_ca_file,
    });
    if let Some(database) = &database {
        for (key, code) in database.validate() {
            // Range issues were already reported by the typed reads.
            if code != "out_of_range" {
                r.issue(key, code, Origin::File);
            }
        }
    }
    r.finish();

    // [runtime]
    let (mut r, _) = reader(layers, "runtime", issues);
    let manifest = r.path("manifest", base_dir);
    let manifest = r.required("manifest", manifest.clone(), manifest.is_some());
    r.finish();

    // [application], optional as a whole
    let (mut r, present) = reader(layers, "application", issues);
    let application = if present {
        let path = r.path("path", base_dir);
        let path = r.required("path", path.clone(), path.is_some());
        path.map(|path| ApplicationConfig { path })
    } else {
        None
    };
    r.finish();

    // [telemetry]
    let (mut r, _) = reader(layers, "telemetry", issues);
    let log_level = r.choice(
        "log_level",
        LogLevel::Info,
        &[
            ("error", LogLevel::Error),
            ("warn", LogLevel::Warn),
            ("info", LogLevel::Info),
            ("debug", LogLevel::Debug),
            ("trace", LogLevel::Trace),
        ],
    );
    let format = r.choice(
        "format",
        LogFormat::Json,
        &[("json", LogFormat::Json), ("text", LogFormat::Text)],
    );
    r.finish();

    // Sections created only by overrides for unknown names were already
    // reported; anything left is a nested table the schema does not know.
    for (name, section) in std::mem::take(&mut layers.sections) {
        for (key, entry) in section {
            issues.push(ConfigIssue::new(
                format!("{name}.{key}"),
                "unknown_key",
                entry.origin,
            ));
        }
    }
    let modules: BTreeMap<String, ModuleSection> = std::mem::take(&mut layers.modules)
        .into_iter()
        .map(|(name, (table, overridden))| {
            (
                name.clone(),
                ModuleSection {
                    name,
                    table,
                    overridden,
                    resolver: resolver.clone(),
                },
            )
        })
        .collect();
    let config = (|| {
        Some(ProjectConfig {
            base_dir: base_dir.to_path_buf(),
            app: AppConfig {
                id: id?,
                name: name?,
            },
            http: HttpConfig {
                listen: listen?.0,
                public,
                trusted_proxies,
                body_limit,
                cors,
            },
            database: database?,
            runtime: RuntimeConfig {
                manifest: manifest?,
            },
            application,
            modules: modules.clone(),
            telemetry: TelemetryConfig { log_level, format },
        })
    })();
    (config, modules)
}
