use merezha_domain::{Failure, Identity};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;

/// The `[limits]` keys, in document order.
const LIMIT_KEYS: [&str; 10] = [
    "timeout_ms",
    "max_events",
    "max_streams",
    "max_store_bytes",
    "max_idempotency",
    "max_payload_bytes",
    "max_state_bytes",
    "batch_size",
    "max_concurrent",
    "max_stream_events",
];
/// Total-capacity keys. PostgreSQL storage rejects them when present,
/// because total capacity is an operator disk concern there.
const CAPACITY_KEYS: [&str; 4] = [
    "max_events",
    "max_streams",
    "max_store_bytes",
    "max_idempotency",
];
/// Default and ceiling of `max_stream_events`.
const MAX_STREAM_EVENTS: usize = 4096;

/// The `[limits]` keys a parsed document wrote. Defaults, and values set
/// in code after parsing, are not present.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LimitKeys(BTreeSet<&'static str>);
impl LimitKeys {
    pub fn contains(&self, key: &str) -> bool {
        self.0.contains(key)
    }
    pub fn iter(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.0.iter().copied()
    }
}

/// Hard ceilings apply even when configuring a replaceable adapter.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(from = "RawLimits")]
pub struct Limits {
    pub timeout_ms: u64,
    pub max_events: usize,
    pub max_streams: usize,
    pub max_store_bytes: usize,
    pub max_idempotency: usize,
    pub max_payload_bytes: usize,
    pub max_state_bytes: usize,
    pub batch_size: usize,
    pub max_concurrent: usize,
    /// Per-stream history ceiling (version 2), on every backend: the whole
    /// history goes to the guest on each decision. Left out of the encoding
    /// at its default, so version 1 configurations encode byte for byte as
    /// before (the SQLite configuration fingerprint).
    #[serde(skip_serializing_if = "default_stream_events")]
    pub max_stream_events: usize,
    /// Which keys the parsed document wrote.
    #[serde(skip)]
    pub present: LimitKeys,
}
fn default_stream_events(value: &usize) -> bool {
    *value == MAX_STREAM_EVENTS
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            timeout_ms: 5000,
            max_events: 4096,
            max_streams: 256,
            max_store_bytes: 8_388_608,
            max_idempotency: 1024,
            max_payload_bytes: 16_384,
            max_state_bytes: 65_536,
            batch_size: 16,
            max_concurrent: 16,
            max_stream_events: MAX_STREAM_EVENTS,
            present: LimitKeys::default(),
        }
    }
}
/// The document form of [`Limits`]: every key optional, so presence is
/// known after parsing.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawLimits {
    timeout_ms: Option<u64>,
    max_events: Option<usize>,
    max_streams: Option<usize>,
    max_store_bytes: Option<usize>,
    max_idempotency: Option<usize>,
    max_payload_bytes: Option<usize>,
    max_state_bytes: Option<usize>,
    batch_size: Option<usize>,
    max_concurrent: Option<usize>,
    max_stream_events: Option<usize>,
}
impl From<RawLimits> for Limits {
    fn from(raw: RawLimits) -> Self {
        let defaults = Limits::default();
        let written = [
            raw.timeout_ms.is_some(),
            raw.max_events.is_some(),
            raw.max_streams.is_some(),
            raw.max_store_bytes.is_some(),
            raw.max_idempotency.is_some(),
            raw.max_payload_bytes.is_some(),
            raw.max_state_bytes.is_some(),
            raw.batch_size.is_some(),
            raw.max_concurrent.is_some(),
            raw.max_stream_events.is_some(),
        ];
        Self {
            timeout_ms: raw.timeout_ms.unwrap_or(defaults.timeout_ms),
            max_events: raw.max_events.unwrap_or(defaults.max_events),
            max_streams: raw.max_streams.unwrap_or(defaults.max_streams),
            max_store_bytes: raw.max_store_bytes.unwrap_or(defaults.max_store_bytes),
            max_idempotency: raw.max_idempotency.unwrap_or(defaults.max_idempotency),
            max_payload_bytes: raw.max_payload_bytes.unwrap_or(defaults.max_payload_bytes),
            max_state_bytes: raw.max_state_bytes.unwrap_or(defaults.max_state_bytes),
            batch_size: raw.batch_size.unwrap_or(defaults.batch_size),
            max_concurrent: raw.max_concurrent.unwrap_or(defaults.max_concurrent),
            max_stream_events: raw.max_stream_events.unwrap_or(defaults.max_stream_events),
            present: LimitKeys(
                LIMIT_KEYS
                    .iter()
                    .zip(written)
                    .filter_map(|(key, written)| written.then_some(*key))
                    .collect(),
            ),
        }
    }
}
impl Limits {
    pub fn validate(&self) -> Result<(), Failure> {
        if !(1..=30_000).contains(&self.timeout_ms) {
            return Err(Failure::permanent("invalid_limits"));
        }
        for (value, max) in [
            (self.max_events, 4096),
            (self.max_streams, 256),
            (self.max_store_bytes, 8_388_608),
            (self.max_idempotency, 1024),
            (self.max_payload_bytes, 65_536),
            (self.max_state_bytes, 262_144),
            (self.batch_size, 64),
            (self.max_concurrent, 64),
            (self.max_stream_events, MAX_STREAM_EVENTS),
        ] {
            if value == 0 || value > max {
                return Err(Failure::permanent("invalid_limits"));
            }
        }
        Ok(())
    }
    /// Total-capacity keys the document wrote: `max_events`, `max_streams`,
    /// `max_store_bytes` and `max_idempotency`.
    pub fn capacity_keys(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.present
            .iter()
            .filter(|key| CAPACITY_KEYS.contains(key))
    }
    /// Fails `invalid_limits` when the document wrote a total-capacity key,
    /// as backends without a total capacity require (PostgreSQL, AP-A2).
    pub fn reject_capacity_keys(&self) -> Result<(), Failure> {
        match self.capacity_keys().next() {
            Some(_) => Err(Failure::permanent("invalid_limits")),
            None => Ok(()),
        }
    }
}
/// Route access when no permission is named (version 2).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Access {
    Public,
    Authenticated,
}
/// Where a route's permission is checked (version 2).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RouteScope {
    Application,
    Tenant,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CommandRoute {
    pub tag: Identity,
    pub service: Identity,
    pub aggregate: Identity,
    /// Version 2, enforced from AP-C2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission: Option<Identity>,
    /// Version 2, enforced from AP-C2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<Access>,
    /// Version 2, enforced from AP-C2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<RouteScope>,
    /// Version 2, commands only: an identity resource kind, from AP-C2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<Identity>,
}
impl CommandRoute {
    fn access_keys(&self) -> bool {
        self.permission.is_some()
            || self.access.is_some()
            || self.scope.is_some()
            || self.resource.is_some()
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRoute {
    pub tag: Identity,
    pub service: Identity,
    pub projection: Identity,
    /// Version 2, enforced from AP-C2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission: Option<Identity>,
    /// Version 2, enforced from AP-C2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<Access>,
    /// Version 2, enforced from AP-C2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<RouteScope>,
}
impl QueryRoute {
    fn access_keys(&self) -> bool {
        self.permission.is_some() || self.access.is_some() || self.scope.is_some()
    }
}
/// How a projection's state is partitioned (version 2).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Partition {
    None,
    Tenant,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Projection {
    pub name: Identity,
    pub service: Identity,
    pub initial: Value,
    /// Application-owned compatibility version; changing it requires a new
    /// database or a separately reviewed migration, never an implicit reset.
    #[serde(default = "projection_revision")]
    pub revision: u32,
    /// Version 2, from AP-C2.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<Partition>,
}
fn projection_revision() -> u32 {
    1
}
/// An application permission for the route catalog (version 2, AP-C2).
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Permission {
    pub name: Identity,
    #[serde(default)]
    pub description: String,
}
/// Effect types a reactor may request (version 2, AP-C3).
#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum EffectKind {
    Notify,
    Webhook,
    Command,
}
/// Where a new reactor row starts (version 2, AP-C3).
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReactorStart {
    #[default]
    Head,
    Beginning,
}
/// What a permanent guest failure on one event does (version 2, AP-C3).
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnError {
    #[default]
    Halt,
    Skip,
}
/// A reactor declaration (version 2, AP-C3).
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Reactor {
    pub name: Identity,
    pub service: Identity,
    #[serde(default)]
    pub events: Vec<Identity>,
    #[serde(default)]
    pub effects: Vec<EffectKind>,
    #[serde(default)]
    pub commands: Vec<Identity>,
    #[serde(default)]
    pub permissions: Vec<Identity>,
    #[serde(default)]
    pub allow_addresses: bool,
    #[serde(default)]
    pub start: ReactorStart,
    #[serde(default)]
    pub on_error: OnError,
}
/// Storage is selected by the host only. Initialization never adopts an
/// existing file; normal open never creates one.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
pub enum StorageConfig {
    #[default]
    Memory,
    Sqlite {
        path: std::path::PathBuf,
        application: Identity,
        #[serde(default)]
        initialize: bool,
    },
    /// The `merezha_app` schema on the host's `[database]` pool (AP-A2).
    /// There is no URL here and no `initialize`: the first start on a
    /// migrated, empty schema records `application` in the manifest row,
    /// and every later start must name the same identity.
    Postgres { application: Identity },
}
/// The application configuration. Version 1 is the original format.
/// Version 2 adds `limits.max_stream_events`, which this build enforces,
/// and the access, tenancy and reactor keys (`[[permissions]]`, route
/// `permission`, `access`, `scope` and `resource`, projection `partition`
/// and `[[reactors]]`), which it parses and refuses with
/// `unsupported_in_this_build`. A version 1 document using a version 2 key
/// fails `invalid_config`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub version: u16,
    #[serde(default)]
    pub limits: Limits,
    #[serde(default)]
    pub commands: Vec<CommandRoute>,
    #[serde(default)]
    pub queries: Vec<QueryRoute>,
    #[serde(default)]
    pub projections: Vec<Projection>,
    /// Version 2, from AP-C2. `Some` whenever the document wrote the key,
    /// even as an empty list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permissions: Option<Vec<Permission>>,
    /// Version 2, from AP-C3. `Some` whenever the document wrote the key.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reactors: Option<Vec<Reactor>>,
    #[serde(default)]
    pub storage: StorageConfig,
}
impl Config {
    pub fn parse(input: &str) -> Result<Self, Failure> {
        if input.len() > 65_536 {
            return Err(Failure::permanent("config_limit"));
        }
        let config: Self =
            toml::from_str(input).map_err(|_| Failure::permanent("invalid_config"))?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), Failure> {
        let unsupported = self.unsupported_keys();
        let version_2 = unsupported || self.limits.present.contains("max_stream_events");
        if !matches!(self.version, 1 | 2) || (self.version == 1 && version_2) {
            return Err(Failure::permanent("invalid_config"));
        }
        self.limits.validate()?;
        if self.commands.len() > 64
            || self.queries.len() > 64
            || self.projections.len() > 32
            || self.permissions.as_ref().is_some_and(|p| p.len() > 256)
            || self.reactors.as_ref().is_some_and(|r| r.len() > 32)
            || self
                .permissions
                .iter()
                .flatten()
                .any(|p| p.description.len() > 256)
        {
            return Err(Failure::permanent("invalid_config"));
        }
        let commands: BTreeSet<_> = self.commands.iter().map(|r| &r.tag).collect();
        let queries: BTreeSet<_> = self.queries.iter().map(|r| &r.tag).collect();
        let projections: BTreeSet<_> = self.projections.iter().map(|r| &r.name).collect();
        if commands.len() != self.commands.len()
            || self
                .commands
                .iter()
                .any(|r| r.aggregate.as_str().contains('/'))
            || queries.len() != self.queries.len()
            || projections.len() != self.projections.len()
            || !commands.is_disjoint(&queries)
            || self
                .queries
                .iter()
                .any(|q| !projections.contains(&q.projection))
            || self.projections.iter().any(|p| {
                p.revision == 0
                    || serde_json::to_vec(&p.initial)
                        .map_or(true, |v| v.len() > self.limits.max_state_bytes)
            })
        {
            return Err(Failure::permanent("invalid_routes"));
        }
        if unsupported {
            return Err(Failure::permanent("unsupported_in_this_build"));
        }
        // Total capacity is an operator disk concern on PostgreSQL (§A.6).
        if matches!(self.storage, StorageConfig::Postgres { .. }) {
            self.limits.reject_capacity_keys()?;
        }
        Ok(())
    }
    /// Whether any key enforced only by later slices is set: permissions
    /// and route access (AP-C2), tenant scope and partitions (AP-C2) and
    /// reactors (AP-C3). Accepting them unenforced would silently open
    /// routes that declare a permission.
    fn unsupported_keys(&self) -> bool {
        self.permissions.is_some()
            || self.reactors.is_some()
            || self.commands.iter().any(CommandRoute::access_keys)
            || self.queries.iter().any(QueryRoute::access_keys)
            || self.projections.iter().any(|p| p.partition.is_some())
    }
}
