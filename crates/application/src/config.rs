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
/// Hard ceiling of `max_payload_bytes`. Durable stores read stored events
/// up to it, so lowering the configured limit never makes history
/// unreadable; the configured limit binds writes.
pub(crate) const MAX_PAYLOAD_BYTES: usize = 65_536;
/// Hard ceiling of `max_state_bytes`, read like [`MAX_PAYLOAD_BYTES`].
pub(crate) const MAX_STATE_BYTES: usize = 262_144;

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
            (self.max_payload_bytes, MAX_PAYLOAD_BYTES),
            (self.max_state_bytes, MAX_STATE_BYTES),
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
    /// Version 2: the permission the caller needs at the route's scope
    /// (exclusive with `access`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission: Option<Identity>,
    /// Version 2: `public` or `authenticated` instead of a permission.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<Access>,
    /// Version 2: where the permission is checked and whose streams the
    /// route writes; `application` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<RouteScope>,
    /// Version 2, commands only: an identity resource kind. The permission
    /// is then checked on the resource `{tenant, kind, id}` whose id is the
    /// command's local stream id (tenant scope only).
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
    /// What the route requires of its caller.
    pub fn rule(&self) -> Rule<'_> {
        rule(self.permission.as_ref(), self.access)
    }
    /// The route's scope, `application` when absent.
    pub fn route_scope(&self) -> RouteScope {
        self.scope.unwrap_or(RouteScope::Application)
    }
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryRoute {
    pub tag: Identity,
    pub service: Identity,
    pub projection: Identity,
    /// Version 2: as [`CommandRoute::permission`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub permission: Option<Identity>,
    /// Version 2: as [`CommandRoute::access`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<Access>,
    /// Version 2: a tenant-scoped query reads the caller's tenant partition
    /// of a `partition = "tenant"` projection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<RouteScope>,
    /// Version 2: an application-scoped query on an unpartitioned
    /// projection, in an application with tenant routes, that anonymous
    /// callers or every user may call **returns every tenant's data**. It
    /// is refused at startup (`cross_tenant_query`) unless this opts in.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub cross_tenant: bool,
}
impl QueryRoute {
    fn access_keys(&self) -> bool {
        self.permission.is_some()
            || self.access.is_some()
            || self.scope.is_some()
            || self.cross_tenant
    }
    /// What the route requires of its caller.
    pub fn rule(&self) -> Rule<'_> {
        rule(self.permission.as_ref(), self.access)
    }
    /// The route's scope, `application` when absent.
    pub fn route_scope(&self) -> RouteScope {
        self.scope.unwrap_or(RouteScope::Application)
    }
}
/// What a route requires of its caller (AP §C.4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Rule<'a> {
    /// Neither `permission` nor `access`: a version 1 route. A host with the
    /// identity block refuses such routes at startup
    /// (`route_access_undeclared`); without it the host's policy decides.
    Undeclared,
    /// `access = "public"`: anyone, anonymous callers included.
    Public,
    /// `access = "authenticated"`: any principal.
    Authenticated,
    /// `permission = "..."`: a principal holding it at the route's scope.
    Permission(&'a Identity),
}
fn rule(permission: Option<&Identity>, access: Option<Access>) -> Rule<'_> {
    match (permission, access) {
        (Some(permission), _) => Rule::Permission(permission),
        (None, Some(Access::Public)) => Rule::Public,
        (None, Some(Access::Authenticated)) => Rule::Authenticated,
        (None, None) => Rule::Undeclared,
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
    /// Version 2: `tenant` keeps one state per tenant (PostgreSQL only).
    /// Changing it needs a new `revision`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<Partition>,
}
impl Projection {
    /// Whether the state is kept per tenant (`partition = "tenant"`).
    pub fn partitioned(&self) -> bool {
        self.partition == Some(Partition::Tenant)
    }
}
fn projection_revision() -> u32 {
    1
}
/// An application permission for the route catalog (version 2, AP-C2). It
/// joins the identity block's permission catalog, so roles and keys can
/// grant it.
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
/// Most `events` tags one reactor filters on.
pub const MAX_REACTOR_EVENTS: usize = 64;
/// Longest reactor name, so `reaction:{reactor}:{event_id}:{index}` and
/// `rx:{reactor}:{position}:{index}` stay within their key limits.
pub const MAX_REACTOR_NAME: usize = 48;
/// A reactor name: 1 to [`MAX_REACTOR_NAME`] bytes of `[A-Za-z0-9._-]`.
pub fn valid_reactor_name(name: &str) -> bool {
    (1..=MAX_REACTOR_NAME).contains(&name.len())
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
}
/// A reactor declaration (version 2, AP §C.5). PostgreSQL only.
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
/// Version 2 adds `limits.max_stream_events`, the access and tenancy keys
/// (`[[permissions]]`, route `permission`, `access`, `scope` and
/// `resource`, projection `partition`, AP §C.4) and `[[reactors]]` (AP
/// §C.5, PostgreSQL only).
/// A version 1 document using a version 2 key fails `invalid_config`.
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
        let config = Self::decode(input)?;
        config.validate()?;
        Ok(config)
    }
    /// The document without [`Config::validate`], for a host that lists
    /// every access issue before refusing it ([`Config::host_issues`]).
    pub fn decode(input: &str) -> Result<Self, Failure> {
        if input.len() > 65_536 {
            return Err(Failure::permanent("config_limit"));
        }
        toml::from_str(input).map_err(|_| Failure::permanent("invalid_config"))
    }
    pub fn validate(&self) -> Result<(), Failure> {
        let version_2 = self.version_2_keys() || self.limits.present.contains("max_stream_events");
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
        if let Some(issue) = self.route_issues().into_iter().next() {
            return Err(Failure::permanent(issue.code));
        }
        // Total capacity is an operator disk concern on PostgreSQL (§A.6).
        if matches!(self.storage, StorageConfig::Postgres { .. }) {
            self.limits.reject_capacity_keys()?;
        }
        Ok(())
    }
    /// Whether any version 2 key other than `max_stream_events` is set.
    fn version_2_keys(&self) -> bool {
        self.permissions.is_some()
            || self.reactors.is_some()
            || self.commands.iter().any(CommandRoute::access_keys)
            || self.queries.iter().any(QueryRoute::access_keys)
            || self.projections.iter().any(|p| p.partition.is_some())
    }
    /// Every problem of the access, tenancy and permission declarations
    /// that the document shows on its own (AP §C.4), in document order:
    ///
    /// - `invalid_permission`: a `[[permissions]]` or route permission off
    ///   the grammar (two to four `[a-z][a-z0-9_]{0,31}` segments);
    ///   `block_prefix`: an application permission in a block's namespace
    ///   (`identity.`, `content.`, `notifications.`, `jobs.`,
    ///   `application.`, `system.`); `duplicate_permission`;
    /// - `route_access_conflict`: a route with both `permission` and
    ///   `access`;
    /// - `resource_requires_tenant`: `resource` without `scope = "tenant"`;
    /// - `tenant_query_unpartitioned`: a tenant-scoped query on a projection
    ///   without `partition = "tenant"`; `partitioned_query_unscoped`: an
    ///   application-scoped query on one;
    /// - `tenant_aggregate`: with any tenant scope, an aggregate containing
    ///   `:`, so a tenant stream `{tenant}:{aggregate}/{id}` never reads as
    ///   another route's;
    /// - `partitions_require_postgres`: a partitioned projection on memory
    ///   or SQLite storage;
    /// - `cross_tenant_query`: with any tenant-scoped route, an
    ///   application-scoped query on an unpartitioned projection open to
    ///   anonymous callers (`public`, or undeclared) or to any principal
    ///   (`authenticated`), unless it sets `cross_tenant = true`, since it
    ///   returns every tenant's data; `cross_tenant_scoped`: the opt-in on
    ///   a tenant-scoped query;
    /// - for `[[reactors]]` (AP §C.5), keyed `reactors.{name}`:
    ///   `invalid_reactor_name` (1 to 48 bytes of `[A-Za-z0-9._-]`, so the
    ///   derived keys and the `sys_reactor_{name}` subject stay bounded),
    ///   `duplicate_reactor`, `unknown_command` for a `commands` tag no
    ///   `[[commands]]` route declares, `invalid_permission` for a
    ///   `permissions` entry off the grammar, `too_many_events` past 64
    ///   `events` tags, and `reactors_require_postgres` on memory or SQLite
    ///   storage, which have no outbox.
    ///
    /// [`Config::validate`] fails with the first; a host lists them all
    /// with [`Config::host_issues`].
    pub fn route_issues(&self) -> Vec<RouteIssue> {
        let mut issues = Vec::new();
        let mut issue = |key: String, code: &'static str| issues.push(RouteIssue { key, code });
        let mut names = BTreeSet::new();
        for permission in self.permissions.iter().flatten() {
            let key = format!("permissions.{}", permission.name.as_str());
            if !valid_permission(permission.name.as_str()) {
                issue(key, "invalid_permission");
            } else if block_namespace(permission.name.as_str()) {
                issue(key, "block_prefix");
            } else if !names.insert(permission.name.as_str()) {
                issue(key, "duplicate_permission");
            }
        }
        let tenant_scoped = self
            .commands
            .iter()
            .map(CommandRoute::route_scope)
            .chain(self.queries.iter().map(QueryRoute::route_scope))
            .any(|scope| scope == RouteScope::Tenant);
        for route in &self.commands {
            let key = format!("commands.{}", route.tag.as_str());
            if route.permission.is_some() && route.access.is_some() {
                issue(key.clone(), "route_access_conflict");
            }
            if route
                .permission
                .as_ref()
                .is_some_and(|p| !valid_permission(p.as_str()))
            {
                issue(key.clone(), "invalid_permission");
            }
            if route.resource.is_some() && route.route_scope() != RouteScope::Tenant {
                issue(key.clone(), "resource_requires_tenant");
            }
            if tenant_scoped && route.aggregate.as_str().contains(':') {
                issue(key, "tenant_aggregate");
            }
        }
        for route in &self.queries {
            let key = format!("queries.{}", route.tag.as_str());
            if route.permission.is_some() && route.access.is_some() {
                issue(key.clone(), "route_access_conflict");
            }
            if route
                .permission
                .as_ref()
                .is_some_and(|p| !valid_permission(p.as_str()))
            {
                issue(key.clone(), "invalid_permission");
            }
            let partitioned = self
                .projections
                .iter()
                .find(|p| p.name == route.projection)
                .is_some_and(Projection::partitioned);
            match (route.route_scope(), partitioned) {
                (RouteScope::Tenant, false) => issue(key.clone(), "tenant_query_unpartitioned"),
                (RouteScope::Application, true) => issue(key.clone(), "partitioned_query_unscoped"),
                _ => {}
            }
            if route.cross_tenant && route.route_scope() == RouteScope::Tenant {
                issue(key.clone(), "cross_tenant_scoped");
            }
            // Fail closed: once tenants exist, an open application query on
            // an unpartitioned projection reads every tenant's events.
            if self.cross_tenant_exposed(route, partitioned)
                && matches!(
                    route.rule(),
                    Rule::Public | Rule::Authenticated | Rule::Undeclared
                )
            {
                issue(key, "cross_tenant_query");
            }
        }
        self.reactor_issues(&mut issue);
        if !matches!(self.storage, StorageConfig::Postgres { .. }) {
            for projection in self.projections.iter().filter(|p| p.partitioned()) {
                issue(
                    format!("projections.{}", projection.name.as_str()),
                    "partitions_require_postgres",
                );
            }
        }
        issues
    }
    /// [`Config::route_issues`] plus what only the host knows:
    /// `unknown_permission` for a route permission `catalog` does not hold
    /// (the union of `[[permissions]]` and the block permissions), and,
    /// when the identity block is enabled, `route_access_undeclared` for a
    /// route setting neither `permission` nor `access`. Without identity a
    /// route may stay undeclared: only a private loopback listener's
    /// allow-all policy serves it.
    ///
    /// `everyone` says whether every user holds a permission (identity's
    /// `builtin_grants.user`): an application query checking one is as
    /// open as `access = "authenticated"`, so `cross_tenant_query` applies
    /// to it too.
    pub fn host_issues(
        &self,
        catalog: &dyn Fn(&str) -> bool,
        everyone: &dyn Fn(&str) -> bool,
        identity: bool,
    ) -> Vec<RouteIssue> {
        let mut issues = self.route_issues();
        for route in &self.queries {
            let partitioned = self
                .projections
                .iter()
                .find(|p| p.name == route.projection)
                .is_some_and(Projection::partitioned);
            if let Rule::Permission(permission) = route.rule()
                && self.cross_tenant_exposed(route, partitioned)
                && everyone(permission.as_str())
            {
                issues.push(RouteIssue {
                    key: format!("queries.{}", route.tag.as_str()),
                    code: "cross_tenant_query",
                });
            }
        }
        let declared: BTreeSet<&str> = self
            .permissions
            .iter()
            .flatten()
            .map(|p| p.name.as_str())
            .collect();
        let routes = self
            .commands
            .iter()
            .map(|r| (format!("commands.{}", r.tag.as_str()), r.rule()))
            .chain(
                self.queries
                    .iter()
                    .map(|r| (format!("queries.{}", r.tag.as_str()), r.rule())),
            );
        for reactor in self.reactors.iter().flatten() {
            for permission in &reactor.permissions {
                if valid_permission(permission.as_str())
                    && !declared.contains(permission.as_str())
                    && !catalog(permission.as_str())
                {
                    issues.push(RouteIssue {
                        key: format!("reactors.{}", reactor.name.as_str()),
                        code: "unknown_permission",
                    });
                }
            }
        }
        for (key, rule) in routes {
            match rule {
                Rule::Undeclared if identity => issues.push(RouteIssue {
                    key,
                    code: "route_access_undeclared",
                }),
                Rule::Permission(permission)
                    if valid_permission(permission.as_str())
                        && !declared.contains(permission.as_str())
                        && !catalog(permission.as_str()) =>
                {
                    issues.push(RouteIssue {
                        key,
                        code: "unknown_permission",
                    })
                }
                _ => {}
            }
        }
        issues
    }
    /// The `[[reactors]]` part of [`Config::route_issues`].
    fn reactor_issues(&self, issue: &mut impl FnMut(String, &'static str)) {
        let commands: BTreeSet<&str> = self.commands.iter().map(|r| r.tag.as_str()).collect();
        let mut names = BTreeSet::new();
        let postgres = matches!(self.storage, StorageConfig::Postgres { .. });
        for reactor in self.reactors.iter().flatten() {
            let key = format!("reactors.{}", reactor.name.as_str());
            if !valid_reactor_name(reactor.name.as_str()) {
                issue(key.clone(), "invalid_reactor_name");
            } else if !names.insert(reactor.name.as_str()) {
                issue(key.clone(), "duplicate_reactor");
            }
            if reactor
                .commands
                .iter()
                .any(|tag| !commands.contains(tag.as_str()))
            {
                issue(key.clone(), "unknown_command");
            }
            if reactor
                .permissions
                .iter()
                .any(|p| !valid_permission(p.as_str()))
            {
                issue(key.clone(), "invalid_permission");
            }
            if reactor.events.len() > MAX_REACTOR_EVENTS {
                issue(key.clone(), "too_many_events");
            }
            if !postgres {
                issue(key, "reactors_require_postgres");
            }
        }
    }
    /// Whether `route` would read every tenant's data: the application has
    /// a tenant-scoped route, the query is application-scoped on an
    /// unpartitioned projection, and it does not opt in (`cross_tenant`).
    fn cross_tenant_exposed(&self, route: &QueryRoute, partitioned: bool) -> bool {
        let tenants = self
            .commands
            .iter()
            .map(CommandRoute::route_scope)
            .chain(self.queries.iter().map(QueryRoute::route_scope))
            .any(|scope| scope == RouteScope::Tenant);
        tenants
            && route.route_scope() == RouteScope::Application
            && !partitioned
            && !route.cross_tenant
    }
    /// `block_resource_kind` for each command whose `resource` names a
    /// kind a block declares (`block_kinds`, such as content's
    /// `content_type`), as `block_prefix` refuses block permission names:
    /// a route would otherwise check grants the block hands out for its
    /// own resources.
    pub fn block_kind_issues(&self, block_kinds: &[&str]) -> Vec<RouteIssue> {
        self.commands
            .iter()
            .filter(|r| {
                r.resource
                    .as_ref()
                    .is_some_and(|kind| block_kinds.contains(&kind.as_str()))
            })
            .map(|r| RouteIssue {
                key: format!("commands.{}", r.tag.as_str()),
                code: "block_resource_kind",
            })
            .collect()
    }
    /// The resource kinds routes name in `resource`, for the identity
    /// block's role assignments.
    pub fn resource_kinds(&self) -> BTreeSet<&str> {
        self.commands
            .iter()
            .filter_map(|r| r.resource.as_ref().map(Identity::as_str))
            .collect()
    }
}
/// One problem of a configuration's access declarations: `key` names the
/// entry (`commands.CreateTask`, `queries.ListTasks`,
/// `permissions.tasks.create`, `projections.tasks`) and `code` the reason.
/// Neither ever carries a value from outside the document.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RouteIssue {
    pub key: String,
    pub code: &'static str,
}
/// Block namespaces application permissions may not use (identity §5).
pub const BLOCK_PREFIXES: [&str; 6] = [
    "identity",
    "content",
    "notifications",
    "jobs",
    "application",
    "system",
];
/// The permission grammar of identity §5: two to four segments of
/// `[a-z][a-z0-9_]{0,31}`.
pub fn valid_permission(name: &str) -> bool {
    let segments: Vec<&str> = name.split('.').collect();
    (2..=4).contains(&segments.len())
        && segments.iter().all(|segment| {
            let bytes = segment.as_bytes();
            !bytes.is_empty()
                && bytes.len() <= 32
                && bytes[0].is_ascii_lowercase()
                && bytes
                    .iter()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
        })
}
fn block_namespace(name: &str) -> bool {
    BLOCK_PREFIXES.contains(&name.split('.').next().unwrap_or(""))
}
