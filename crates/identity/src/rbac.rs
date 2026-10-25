//! RBAC (section 5): the built-in, declared and implicit roles, the grant
//! snapshot a principal carries, the version-stamped snapshot cache and
//! the [`RbacAuthorizer`] the host installs.
//!
//! Evaluation itself is the platform's [`GrantSnapshot::evaluate`]: this
//! module only decides what goes into a snapshot. A check is synchronous and
//! does no I/O; the `Authenticator` loads every grant once per request.
use crate::{
    catalog::{Catalog, IDENTITY_PERMISSIONS, Wildcards},
    config::RbacSettings,
    core::Core,
};
use merezha_application::Authorizer as ApplicationAuthorizer;
use merezha_domain::{Failure, Identity, MessageKind, Metadata};
use merezha_platform::ports::{
    AccessDenied, Allowed, Authorizer, DenyReason, GrantSnapshot, Grants, KeyLimit, Permission,
    PermissionSet, Principal, ResourceScope, TenantId,
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{Arc, Mutex, OnceLock},
};
use uuid::Uuid;

/// Seeded role ids (migration 2).
pub const OWNER_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_7000_8000_0000_0000_0001);
pub const ADMIN_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_7000_8000_0000_0000_0002);
pub const MEMBER_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_7000_8000_0000_0000_0003);
pub const PLATFORM_ADMIN_ID: Uuid = Uuid::from_u128(0x0000_0000_0000_7000_8000_0000_0000_0004);

pub const OWNER: &str = "owner";
pub const ADMIN: &str = "admin";
pub const MEMBER: &str = "member";
pub const PLATFORM_ADMIN: &str = "platform_admin";
/// The implicit application role of every active user.
pub const USER: &str = "user";
/// Keys no declared or custom role may take.
pub const RESERVED_ROLE_KEYS: [&str; 5] = [OWNER, ADMIN, MEMBER, PLATFORM_ADMIN, USER];

/// The built-in tenant role of a seeded id.
pub fn builtin_tenant_role(id: Uuid) -> Option<&'static str> {
    match id {
        OWNER_ID => Some(OWNER),
        ADMIN_ID => Some(ADMIN),
        MEMBER_ID => Some(MEMBER),
        _ => None,
    }
}
/// The seeded id of a built-in tenant role.
pub fn builtin_tenant_id(key: &str) -> Option<Uuid> {
    match key {
        OWNER => Some(OWNER_ID),
        ADMIN => Some(ADMIN_ID),
        MEMBER => Some(MEMBER_ID),
        _ => None,
    }
}
/// `^[a-z][a-z0-9_]{0,62}$`.
pub fn valid_role_key(key: &str) -> bool {
    let bytes = key.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
}

/// A declared application role (`[modules.identity.rbac.application_roles]`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeclaredRole {
    pub name: String,
    pub description: Option<String>,
    /// As configured, wildcards unexpanded.
    pub grants: Vec<String>,
}

/// The role definitions of one configuration and catalog. Immutable; a new
/// configuration builds a new policy with the next generation.
#[derive(Debug)]
pub struct Policy {
    catalog: Catalog,
    generation: u64,
    owner: PermissionSet,
    admin: PermissionSet,
    member: PermissionSet,
    user: PermissionSet,
    declared: BTreeMap<String, (DeclaredRole, PermissionSet)>,
}

/// One application role assignment row.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ApplicationRow {
    pub role_key: String,
    pub resource: Option<(String, String)>,
}
/// One tenant role assignment row: a built-in key, or a custom role's key
/// with its stored permissions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenantRow {
    pub role_key: String,
    pub custom: Option<Vec<String>>,
    pub resource: Option<(String, String)>,
}

/// What a snapshot is built for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Holder {
    /// A user: the implicit `user` role, and break-glass for a
    /// `platform_admin` without a key limit.
    User,
    /// A service account: no implicit role, never break-glass.
    ServiceAccount,
}

impl Policy {
    pub fn new(catalog: Catalog, settings: &RbacSettings, generation: u64) -> Self {
        let identity = |names: &[&str]| -> Vec<String> {
            names.iter().map(|n| (*n).to_owned()).collect::<Vec<_>>()
        };
        let admin_base: Vec<String> = IDENTITY_PERMISSIONS
            .iter()
            .filter(|p| {
                !matches!(
                    **p,
                    "identity.organization.delete"
                        | "identity.organization.transfer"
                        // Application scope only: a tenant grant of these
                        // would never satisfy a check.
                        | "identity.users.read"
                        | "identity.users.manage"
                        | "identity.organization.enter"
                )
            })
            .map(|p| (*p).to_owned())
            .collect();
        let mut admin_grants: Vec<String> = catalog
            .entries()
            .filter(|(name, _)| {
                !name.starts_with("identity.") || admin_base.iter().any(|a| a == name)
            })
            .map(|(name, _)| name.to_owned())
            .collect();
        admin_grants.extend(settings.builtin_grants.admin.iter().cloned());
        let mut member_grants = identity(&[
            "identity.organization.read",
            "identity.members.read",
            "identity.roles.read",
            "identity.api_keys.create",
        ]);
        member_grants.extend(settings.builtin_grants.member.iter().cloned());
        let expand = |grants: &[String]| catalog.expand(grants.iter().map(String::as_str));
        let declared = settings
            .application_roles
            .iter()
            .map(|(key, role)| (key.clone(), (role.clone(), expand(&role.grants))))
            .collect();
        Self {
            owner: catalog.expand(["*"]),
            admin: expand(&admin_grants),
            member: expand(&member_grants),
            user: expand(&settings.builtin_grants.user),
            declared,
            generation,
            catalog,
        }
    }
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn declared(&self) -> impl Iterator<Item = (&str, &DeclaredRole, &PermissionSet)> {
        self.declared
            .iter()
            .map(|(k, (role, set))| (k.as_str(), role, set))
    }
    /// The permissions of a built-in tenant role.
    pub fn tenant_builtin(&self, key: &str) -> Option<&PermissionSet> {
        match key {
            OWNER => Some(&self.owner),
            ADMIN => Some(&self.admin),
            MEMBER => Some(&self.member),
            _ => None,
        }
    }
    /// The permissions of a custom role's stored grants.
    pub fn custom(&self, grants: &[String]) -> PermissionSet {
        self.catalog.expand(grants.iter().map(String::as_str))
    }
    /// The permissions of an application role: the implicit `user`, a
    /// declared role, or none for `platform_admin` (break-glass only) and
    /// unknown keys.
    pub fn application_role(&self, key: &str) -> PermissionSet {
        match key {
            USER => self.user.clone(),
            _ => self
                .declared
                .get(key)
                .map(|(_, set)| set.clone())
                .unwrap_or_default(),
        }
    }
    /// A tenant row's permissions.
    pub fn tenant_row(&self, row: &TenantRow) -> PermissionSet {
        match &row.custom {
            Some(grants) => self.custom(grants),
            None => self
                .tenant_builtin(&row.role_key)
                .cloned()
                .unwrap_or_default(),
        }
    }
    /// An API key's limit: its list expanded, pinned to `tenant`.
    pub fn key_limit(&self, grants: &[String], tenant: Option<TenantId>) -> KeyLimit {
        let list: Vec<&str> = grants
            .iter()
            .map(String::as_str)
            // A stored wildcard that is no longer allowed grants nothing.
            .filter(|g| self.catalog.check_grant(g, Wildcards::Application).is_ok())
            .collect();
        KeyLimit::new(self.catalog.expand(list), tenant)
    }

    /// Assemble a snapshot. `tenant` is `None` without an active tenant;
    /// `Some(rows)` with one, possibly empty (an organization entered
    /// through `identity.organization.enter`). Resource rows past
    /// `max_resource` per scope are ignored; the count of ignored rows is
    /// returned.
    pub fn snapshot(
        &self,
        holder: Holder,
        application: &[ApplicationRow],
        tenant: Option<&[TenantRow]>,
        key: Option<KeyLimit>,
        max_resource: usize,
    ) -> (GrantSnapshot, usize) {
        let mut truncated = 0;
        let mut snapshot = GrantSnapshot::empty();
        let mut roles: Vec<Identity> = Vec::new();
        let mut permissions: HashSet<Permission> = HashSet::new();
        let mut break_glass = false;
        if holder == Holder::User {
            roles.push(id(USER));
            permissions.extend(self.user.iter().cloned());
        }
        let mut resources: HashMap<(Identity, Identity), HashSet<Permission>> = HashMap::new();
        for row in application {
            match &row.resource {
                None => {
                    if row.role_key == PLATFORM_ADMIN {
                        break_glass = holder == Holder::User && key.is_none();
                    } else {
                        permissions.extend(self.application_role(&row.role_key).iter().cloned());
                    }
                    if let Ok(key) = Identity::new(row.role_key.clone())
                        && !roles.contains(&key)
                    {
                        roles.push(key);
                    }
                }
                Some((kind, rid)) => {
                    let (Ok(kind), Ok(rid)) =
                        (Identity::new(kind.clone()), Identity::new(rid.clone()))
                    else {
                        continue;
                    };
                    let entry = (kind, rid);
                    if !resources.contains_key(&entry) && resources.len() >= max_resource {
                        truncated += 1;
                        continue;
                    }
                    resources
                        .entry(entry)
                        .or_default()
                        .extend(self.application_role(&row.role_key).iter().cloned());
                }
            }
        }
        if break_glass {
            snapshot = snapshot.with_break_glass();
        }
        snapshot = snapshot.with_application(Grants::new(roles, permissions.into_iter().collect()));
        for ((kind, rid), set) in resources {
            snapshot = snapshot.with_application_resource(kind, rid, set.into_iter().collect());
        }
        if let Some(rows) = tenant {
            let mut roles: Vec<Identity> = Vec::new();
            let mut permissions: HashSet<Permission> = HashSet::new();
            let mut resources: HashMap<(Identity, Identity), HashSet<Permission>> = HashMap::new();
            for row in rows {
                let set = self.tenant_row(row);
                match &row.resource {
                    None => {
                        permissions.extend(set.iter().cloned());
                        if let Ok(key) = Identity::new(row.role_key.clone())
                            && !roles.contains(&key)
                        {
                            roles.push(key);
                        }
                    }
                    Some((kind, rid)) => {
                        let (Ok(kind), Ok(rid)) =
                            (Identity::new(kind.clone()), Identity::new(rid.clone()))
                        else {
                            continue;
                        };
                        let entry = (kind, rid);
                        if !resources.contains_key(&entry) && resources.len() >= max_resource {
                            truncated += 1;
                            continue;
                        }
                        resources
                            .entry(entry)
                            .or_default()
                            .extend(set.iter().cloned());
                    }
                }
            }
            snapshot = snapshot.with_tenant(Grants::new(roles, permissions.into_iter().collect()));
            for ((kind, rid), set) in resources {
                snapshot = snapshot.with_resource(kind, rid, set.into_iter().collect());
            }
        }
        if let Some(key) = key {
            snapshot = snapshot.with_key_limit(key);
        }
        (snapshot, truncated)
    }
}

fn id(text: &str) -> Identity {
    Identity::new(text).unwrap_or_else(|_| unreachable!("role keys are valid identities"))
}

/// What a cached snapshot was built from. A mismatch on any part rebuilds
/// it, so a grant change is visible on the next request on every instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Stamp {
    pub owner_version: i64,
    pub org_version: Option<i64>,
    pub generation: u64,
    /// The membership (or the tenant service account) the tenant grants
    /// came from; `None` without tenant grants.
    pub membership: Option<Uuid>,
}

/// The in-process snapshot cache, keyed by `(credential id, organization
/// id)`. Two generations of at most half the capacity each: a lookup that
/// hits the older one moves the entry forward, and the older one is dropped
/// when the newer fills, which approximates LRU in O(1).
#[derive(Debug)]
pub struct GrantCache {
    capacity: usize,
    inner: Mutex<Generations>,
}
type CacheKey = (Uuid, Option<Uuid>);
type CacheEntry = (Stamp, Arc<GrantSnapshot>);
#[derive(Debug, Default)]
struct Generations {
    current: HashMap<CacheKey, CacheEntry>,
    previous: HashMap<CacheKey, CacheEntry>,
}
impl GrantCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(2),
            inner: Mutex::new(Generations::default()),
        }
    }
    /// The snapshot cached under `key` when its stamp equals `stamp`.
    pub fn get(&self, key: CacheKey, stamp: &Stamp) -> Option<Arc<GrantSnapshot>> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some((cached, snapshot)) = inner.current.get(&key) {
            return (cached == stamp).then(|| snapshot.clone());
        }
        let (cached, snapshot) = inner.previous.remove(&key)?;
        if cached != *stamp {
            return None;
        }
        let half = self.capacity / 2;
        Self::insert_into(&mut inner, half, key, (cached, snapshot.clone()));
        Some(snapshot)
    }
    pub fn put(&self, key: CacheKey, stamp: Stamp, snapshot: Arc<GrantSnapshot>) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let half = self.capacity / 2;
        Self::insert_into(&mut inner, half, key, (stamp, snapshot));
    }
    fn insert_into(inner: &mut Generations, half: usize, key: CacheKey, entry: CacheEntry) {
        if inner.current.len() >= half && !inner.current.contains_key(&key) {
            inner.previous = std::mem::take(&mut inner.current);
        }
        inner.previous.remove(&key);
        inner.current.insert(key, entry);
    }
    pub fn len(&self) -> usize {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.current.len() + inner.previous.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A bounded set of `(principal subject or session, permission)` pairs
/// already audited by this instance, so break-glass and organization entry
/// are audited once per session and permission per instance.
#[derive(Debug, Default)]
pub struct Once {
    seen: Mutex<HashSet<(String, String)>>,
}
/// Pairs remembered before the set starts over.
const ONCE_CAPACITY: usize = 10_000;
impl Once {
    /// `true` the first time `(who, what)` is seen.
    pub fn first(&self, who: &str, what: &str) -> bool {
        let mut seen = self.seen.lock().unwrap_or_else(|e| e.into_inner());
        if seen.len() >= ONCE_CAPACITY {
            seen.clear();
        }
        seen.insert((who.to_owned(), what.to_owned()))
    }
}

/// The RBAC `Authorizer` (section 5), installed by the host when the
/// identity block is on. Checks are the platform evaluation of the
/// principal's snapshot; a break-glass allow is audited as
/// `authorization.break_glass` once per session and permission per instance.
///
/// Application commands and queries (`ApplicationAuthorizer::authorize`)
/// are refused `unauthorized`: their route permissions are AP-C2's, which
/// checks them with the principal before any guest call. Until then no
/// command or query runs while identity is on.
#[derive(Clone, Default)]
pub struct RbacAuthorizer {
    core: Arc<OnceLock<Arc<Core>>>,
}
impl std::fmt::Debug for RbacAuthorizer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RbacAuthorizer")
            .field("started", &self.core.get().is_some())
            .finish()
    }
}
impl RbacAuthorizer {
    pub(crate) fn new(core: Arc<OnceLock<Arc<Core>>>) -> Self {
        Self { core }
    }
}
impl ApplicationAuthorizer for RbacAuthorizer {
    fn authorize(
        &self,
        _: MessageKind,
        _: &str,
        _: Option<&str>,
        _: &Value,
        _: &Metadata,
    ) -> Result<(), Failure> {
        Err(Failure::permanent("unauthorized"))
    }
}
impl Authorizer for RbacAuthorizer {
    fn check(
        &self,
        principal: Option<&Principal>,
        permission: &Permission,
        scope: &ResourceScope,
    ) -> Result<Allowed, AccessDenied> {
        let Some(principal) = principal else {
            return Err(AccessDenied::new(DenyReason::Unauthenticated));
        };
        let allowed = principal.check(permission, scope)?;
        if allowed == Allowed::BreakGlass
            && let Some(core) = self.core.get()
        {
            core.break_glass(principal, permission);
        }
        Ok(allowed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BuiltinGrants, RbacSettings};

    fn catalog() -> Catalog {
        let mut catalog = Catalog::builtin();
        for name in ["tasks.read", "tasks.create", "tasks.delete"] {
            catalog.add_application(name, "").unwrap();
        }
        catalog
    }
    fn policy() -> Policy {
        let mut settings = RbacSettings {
            builtin_grants: BuiltinGrants {
                user: vec!["tasks.read".into()],
                admin: vec!["tasks.*".into()],
                member: vec!["tasks.create".into()],
            },
            ..RbacSettings::default()
        };
        settings.application_roles.insert(
            "support".into(),
            DeclaredRole {
                name: "Support".into(),
                description: None,
                grants: vec!["identity.users.read".into(), "tasks.*".into()],
            },
        );
        Policy::new(catalog(), &settings, 1)
    }
    fn p(name: &str) -> Identity {
        Identity::new(name).unwrap()
    }
    fn tenant(key: &str) -> TenantRow {
        TenantRow {
            role_key: key.into(),
            custom: None,
            resource: None,
        }
    }

    #[test]
    fn built_in_roles_hold_what_section_five_says() {
        let policy = policy();
        let owner = policy.tenant_builtin(OWNER).unwrap();
        assert_eq!(owner.len(), policy.catalog().len());
        let admin = policy.tenant_builtin(ADMIN).unwrap();
        assert!(admin.contains(&p("identity.members.invite")));
        assert!(admin.contains(&p("tasks.delete")));
        assert!(!admin.contains(&p("identity.organization.delete")));
        assert!(!admin.contains(&p("identity.organization.transfer")));
        let member = policy.tenant_builtin(MEMBER).unwrap();
        let mut names: Vec<&str> = member.iter().map(Identity::as_str).collect();
        names.sort();
        assert_eq!(
            names,
            [
                "identity.api_keys.create",
                "identity.members.read",
                "identity.organization.read",
                "identity.roles.read",
                "tasks.create"
            ]
        );
        assert!(policy.application_role(PLATFORM_ADMIN).is_empty());
        assert!(policy.application_role(USER).contains(&p("tasks.read")));
    }

    #[test]
    fn snapshots_follow_the_evaluation_steps() {
        let policy = policy();
        let org = p("org_a");
        let (snapshot, truncated) = policy.snapshot(
            Holder::User,
            &[ApplicationRow {
                role_key: "support".into(),
                resource: None,
            }],
            Some(&[tenant(MEMBER)]),
            None,
            10,
        );
        assert_eq!(truncated, 0);
        let principal = |snapshot: GrantSnapshot, tenant: Option<Identity>| Principal {
            subject: p("usr_1"),
            kind: merezha_platform::ports::PrincipalKind::User,
            user: Some(p("usr_1")),
            service_account: None,
            tenant,
            grants: Arc::new(snapshot),
            session: None,
        };
        let user = principal(snapshot, Some(org.clone()));
        // Step 4: an application grant allows at every scope.
        assert!(
            user.check(&p("tasks.delete"), &ResourceScope::Application)
                .is_ok()
        );
        assert!(
            user.check(&p("tasks.delete"), &ResourceScope::Tenant(p("org_b")))
                .is_ok()
        );
        // Step 5: no tenant grant satisfies an application check.
        assert_eq!(
            user.check(&p("identity.members.read"), &ResourceScope::Application)
                .unwrap_err()
                .reason,
            DenyReason::NoGrant
        );
        // Step 6 and 7.
        assert!(
            user.check(
                &p("identity.members.read"),
                &ResourceScope::Tenant(org.clone())
            )
            .is_ok()
        );
        assert_eq!(
            user.check(
                &p("identity.members.read"),
                &ResourceScope::Tenant(p("org_b"))
            )
            .unwrap_err()
            .reason,
            DenyReason::TenantMismatch
        );
        assert_eq!(user.grants.application().roles()[0].as_str(), USER);
    }

    #[test]
    fn platform_admins_break_glass_only_without_a_key() {
        let policy = policy();
        let admin = [ApplicationRow {
            role_key: PLATFORM_ADMIN.into(),
            resource: None,
        }];
        let (session, _) = policy.snapshot(Holder::User, &admin, None, None, 10);
        assert!(session.is_break_glass());
        let (key, _) = policy.snapshot(
            Holder::User,
            &admin,
            None,
            Some(policy.key_limit(&["tasks.read".into()], None)),
            10,
        );
        assert!(!key.is_break_glass());
        // The key gets the owner's `user` grants, never more.
        let principal = Principal {
            subject: p("key_1"),
            kind: merezha_platform::ports::PrincipalKind::ApiKey,
            user: Some(p("usr_1")),
            service_account: None,
            tenant: None,
            grants: Arc::new(key),
            session: None,
        };
        assert!(
            principal
                .check(&p("tasks.read"), &ResourceScope::Application)
                .is_ok()
        );
        assert!(
            principal
                .check(&p("identity.users.read"), &ResourceScope::Application)
                .is_err()
        );
        let (service, _) = policy.snapshot(Holder::ServiceAccount, &admin, None, None, 10);
        assert!(!service.is_break_glass());
        assert!(
            service
                .application()
                .roles()
                .iter()
                .all(|r| r.as_str() != USER)
        );
    }

    #[test]
    fn resource_grants_are_capped() {
        let policy = policy();
        let rows: Vec<TenantRow> = (0..5)
            .map(|i| TenantRow {
                role_key: MEMBER.into(),
                custom: None,
                resource: Some(("project".into(), format!("p{i}"))),
            })
            .collect();
        let (_, truncated) = policy.snapshot(Holder::User, &[], Some(&rows), None, 3);
        assert_eq!(truncated, 2);
    }

    #[test]
    fn custom_roles_and_keys_expand_only_what_the_catalog_has() {
        let policy = policy();
        let set = policy.custom(&["tasks.*".into(), "removed.permission".into()]);
        assert_eq!(set.len(), 3);
        let limit = policy.key_limit(&["identity.*".into(), "tasks.read".into()], None);
        let (snapshot, _) = policy.snapshot(Holder::User, &[], None, Some(limit), 10);
        let principal = Principal {
            subject: p("key_1"),
            kind: merezha_platform::ports::PrincipalKind::ApiKey,
            user: Some(p("usr_1")),
            service_account: None,
            tenant: None,
            grants: Arc::new(snapshot),
            session: None,
        };
        assert!(
            principal
                .check(&p("tasks.read"), &ResourceScope::Application)
                .is_ok()
        );
        assert_eq!(
            principal
                .check(&p("identity.users.read"), &ResourceScope::Application)
                .unwrap_err()
                .reason,
            DenyReason::KeyLimit
        );
    }

    #[test]
    fn the_cache_rebuilds_on_any_stamp_change_and_stays_bounded() {
        let cache = GrantCache::new(4);
        let stamp = Stamp {
            owner_version: 1,
            org_version: Some(2),
            generation: 1,
            membership: None,
        };
        let key = (Uuid::from_u128(1), None);
        cache.put(key, stamp, Arc::new(GrantSnapshot::empty()));
        assert!(cache.get(key, &stamp).is_some());
        for changed in [
            Stamp {
                owner_version: 2,
                ..stamp
            },
            Stamp {
                org_version: Some(3),
                ..stamp
            },
            Stamp {
                generation: 2,
                ..stamp
            },
            Stamp {
                membership: Some(Uuid::from_u128(9)),
                ..stamp
            },
        ] {
            assert!(cache.get(key, &changed).is_none());
        }
        for i in 0..100 {
            cache.put(
                (Uuid::from_u128(i), None),
                stamp,
                Arc::new(GrantSnapshot::empty()),
            );
        }
        assert!(cache.len() <= 4);
    }
}
