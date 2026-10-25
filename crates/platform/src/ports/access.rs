//! Who is calling and what they may do: [`Principal`] with its
//! [`GrantSnapshot`] and [`SessionFacts`], [`ResourceScope`], the
//! [`Authenticator`] and [`Authorizer`] ports and their fallbacks.
use crate::BoxFuture;
use merezha_application::Authorizer as ApplicationAuthorizer;
use merezha_domain::{Failure, Identity, MessageKind, Metadata};
use serde::{Serialize, ser::SerializeStruct};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::Arc,
};

/// A user id such as `usr_0192f5c4...`.
pub type UserId = Identity;
/// An organization id; organizations are the tenants.
pub type TenantId = Identity;
pub type ServiceAccountId = Identity;
/// A resource kind such as `content_type` or `task`.
pub type ResourceKind = Identity;
pub type ResourceId = Identity;
/// A role key such as `admin`.
pub type RoleKey = Identity;
/// A permission name such as `tasks.create`.
pub type Permission = Identity;

/// Prefix of every system principal's subject (`sys_<name>`, reactors
/// `sys_reactor_<name>`).
pub const SYSTEM_PREFIX: &str = "sys_";

/// What kind of party a principal is.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PrincipalKind {
    User,
    ApiKey,
    /// The host itself, for internal work such as jobs and reactors.
    System,
}

/// How a session's credential travels.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SessionTransport {
    Cookie,
    Bearer,
}

/// Facts about the session behind a user principal. Never serialised and
/// never recorded in logs or telemetry: `Debug` prints nothing of it, and
/// the type has no `Serialize` implementation.
#[derive(Clone, PartialEq, Eq)]
pub struct SessionFacts {
    /// The server-side session id, never its secret.
    pub id: Identity,
    /// How the user authenticated, such as `password` or `passkey`.
    pub method: Identity,
    pub authenticated_at: jiff::Timestamp,
    pub transport: SessionTransport,
}
impl std::fmt::Debug for SessionFacts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SessionFacts(<redacted>)")
    }
}

/// Concrete permissions. Wildcards are expanded against the catalog when a
/// snapshot is built, so a check is one hash lookup.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PermissionSet(HashSet<Permission>);
impl PermissionSet {
    pub fn contains(&self, permission: &Permission) -> bool {
        self.0.contains(permission)
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn iter(&self) -> impl Iterator<Item = &Permission> {
        self.0.iter()
    }
}
impl FromIterator<Permission> for PermissionSet {
    fn from_iter<I: IntoIterator<Item = Permission>>(iter: I) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// The roles held at one scope and the permissions they grant.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Grants {
    roles: Vec<RoleKey>,
    permissions: PermissionSet,
}
impl Grants {
    pub fn new(roles: Vec<RoleKey>, permissions: PermissionSet) -> Self {
        Self { roles, permissions }
    }
    pub fn roles(&self) -> &[RoleKey] {
        &self.roles
    }
    pub fn permissions(&self) -> &PermissionSet {
        &self.permissions
    }
}

/// The limit an API key places on its owner's grants.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeyLimit {
    permissions: PermissionSet,
    /// `None` for an application-scoped key.
    tenant: Option<TenantId>,
}
impl KeyLimit {
    pub fn new(permissions: PermissionSet, tenant: Option<TenantId>) -> Self {
        Self {
            permissions,
            tenant,
        }
    }
}

/// Everything a check needs, loaded once per request by the
/// [`Authenticator`] and immutable afterwards. The authenticator bounds its
/// size (identity caps resource grants per holder).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GrantSnapshot {
    break_glass: bool,
    application: Grants,
    application_resources: HashMap<(ResourceKind, ResourceId), PermissionSet>,
    tenant: Option<Grants>,
    resources: HashMap<(ResourceKind, ResourceId), PermissionSet>,
    key: Option<KeyLimit>,
}
impl GrantSnapshot {
    /// No grants at all.
    pub fn empty() -> Self {
        Self::default()
    }
    /// The `platform_admin` break-glass grant. Ignored for API keys.
    pub fn with_break_glass(mut self) -> Self {
        self.break_glass = true;
        self
    }
    /// Application-scope roles, the implicit `user` role included.
    pub fn with_application(mut self, grants: Grants) -> Self {
        self.application = grants;
        self
    }
    /// Grants on one resource that belongs to no tenant.
    pub fn with_application_resource(
        mut self,
        kind: ResourceKind,
        id: ResourceId,
        permissions: PermissionSet,
    ) -> Self {
        self.application_resources.insert((kind, id), permissions);
        self
    }
    /// Tenant-scope roles in the principal's active tenant.
    pub fn with_tenant(mut self, grants: Grants) -> Self {
        self.tenant = Some(grants);
        self
    }
    /// Grants on one resource in the principal's active tenant.
    pub fn with_resource(
        mut self,
        kind: ResourceKind,
        id: ResourceId,
        permissions: PermissionSet,
    ) -> Self {
        self.resources.insert((kind, id), permissions);
        self
    }
    /// Limit the snapshot to an API key's list and scope.
    pub fn with_key_limit(mut self, key: KeyLimit) -> Self {
        self.key = Some(key);
        self
    }
    pub fn application(&self) -> &Grants {
        &self.application
    }
    pub fn tenant(&self) -> Option<&Grants> {
        self.tenant.as_ref()
    }
    pub fn is_break_glass(&self) -> bool {
        self.break_glass && self.key.is_none()
    }

    /// Decide `permission` at `scope` for a principal whose active tenant is
    /// `active_tenant`. Synchronous and I/O-free. In order:
    ///
    /// 1. break-glass (never for an API key) allows;
    /// 2. a key pinned to a tenant refuses application scope, resources
    ///    without a tenant and other tenants (`KeyScope`);
    /// 3. a key whose list lacks the permission refuses (`KeyLimit`);
    /// 4. an application grant allows at every scope;
    /// 5. application scope otherwise refuses; a resource without a tenant
    ///    allows only through its application resource grant;
    /// 6. a scope naming another tenant than the active one refuses
    ///    (`TenantMismatch`);
    /// 7. tenant grants, or the resource's grant in the tenant, allow.
    pub fn evaluate(
        &self,
        active_tenant: Option<&TenantId>,
        permission: &Permission,
        scope: &ResourceScope,
    ) -> Result<Allowed, AccessDenied> {
        if self.is_break_glass() {
            return Ok(Allowed::BreakGlass);
        }
        let deny = |reason| Err(AccessDenied { reason });
        let scope_tenant = match scope {
            ResourceScope::Application => None,
            ResourceScope::Tenant(tenant) => Some(tenant),
            ResourceScope::Resource { tenant, .. } => tenant.as_ref(),
        };
        if let Some(key) = &self.key {
            if let Some(pinned) = &key.tenant
                && scope_tenant != Some(pinned)
            {
                return deny(DenyReason::KeyScope);
            }
            if !key.permissions.contains(permission) {
                return deny(DenyReason::KeyLimit);
            }
        }
        if self.application.permissions.contains(permission) {
            return Ok(Allowed::Grant);
        }
        let Some(tenant) = scope_tenant else {
            return match scope {
                ResourceScope::Resource { kind, id, .. }
                    if self
                        .application_resources
                        .get(&(kind.clone(), id.clone()))
                        .is_some_and(|p| p.contains(permission)) =>
                {
                    Ok(Allowed::Grant)
                }
                _ => deny(DenyReason::NoGrant),
            };
        };
        if active_tenant != Some(tenant) {
            return deny(DenyReason::TenantMismatch);
        }
        let in_tenant = self
            .tenant
            .as_ref()
            .is_some_and(|g| g.permissions.contains(permission));
        let on_resource = match scope {
            ResourceScope::Resource { kind, id, .. } => self
                .resources
                .get(&(kind.clone(), id.clone()))
                .is_some_and(|p| p.contains(permission)),
            _ => false,
        };
        if in_tenant || on_resource {
            Ok(Allowed::Grant)
        } else {
            deny(DenyReason::NoGrant)
        }
    }
}

/// An authenticated party, established by the host and never by a guest.
///
/// `Debug` never shows session facts. `Serialize` writes the projection
/// guests receive (subject, kind, tenant and the role keys of both scopes)
/// and never the session or the permission sets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    /// `usr_...`, `key_...` or `sys_<name>`.
    pub subject: Identity,
    pub kind: PrincipalKind,
    /// The user itself, or the owner of a user's API key.
    pub user: Option<UserId>,
    /// The owner of a service account's API key.
    pub service_account: Option<ServiceAccountId>,
    /// The active organization.
    pub tenant: Option<TenantId>,
    pub grants: Arc<GrantSnapshot>,
    pub session: Option<SessionFacts>,
}
impl Principal {
    /// The system principal `sys_<name>` with an explicit grant set, for
    /// jobs, reactors and the CLI. `name` is `[a-z][a-z0-9_]*`, at most 64
    /// bytes; anything else fails `invalid_system_principal`.
    pub fn system(name: &str, grants: GrantSnapshot) -> Result<Self, Failure> {
        if !crate::config::valid_module_name(name) {
            return Err(Failure::permanent("invalid_system_principal"));
        }
        Ok(Self {
            subject: Identity::new(format!("{SYSTEM_PREFIX}{name}"))?,
            kind: PrincipalKind::System,
            user: None,
            service_account: None,
            tenant: None,
            grants: Arc::new(grants),
            session: None,
        })
    }
    pub fn is_system(&self) -> bool {
        self.kind == PrincipalKind::System && self.subject.as_str().starts_with(SYSTEM_PREFIX)
    }
    /// The limited projection a version 2 guest receives (AP §C.2):
    /// subject, kind, active tenant and the role keys held at `scope`,
    /// the application roles for `Application` and tenant-less resources,
    /// the tenant roles otherwise. Never the session, the permission sets
    /// or the break-glass flag. Fails `invalid_principal` past 16 roles or
    /// a role key longer than 64 bytes.
    pub fn guest(&self, scope: &ResourceScope) -> Result<merezha_domain::Principal, Failure> {
        let roles: &[RoleKey] = match scope {
            ResourceScope::Application | ResourceScope::Resource { tenant: None, .. } => {
                self.grants.application.roles()
            }
            ResourceScope::Tenant(_)
            | ResourceScope::Resource {
                tenant: Some(_), ..
            } => self.grants.tenant.as_ref().map_or(&[], |g| g.roles()),
        };
        merezha_domain::Principal::new(
            self.subject.clone(),
            match self.kind {
                PrincipalKind::User => merezha_domain::PrincipalKind::User,
                PrincipalKind::ApiKey => merezha_domain::PrincipalKind::ApiKey,
                PrincipalKind::System => merezha_domain::PrincipalKind::System,
            },
            self.tenant.clone(),
            roles.to_vec(),
        )
    }
    /// [`GrantSnapshot::evaluate`] with this principal's active tenant.
    pub fn check(
        &self,
        permission: &Permission,
        scope: &ResourceScope,
    ) -> Result<Allowed, AccessDenied> {
        self.grants
            .evaluate(self.tenant.as_ref(), permission, scope)
    }
}
impl Serialize for Principal {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Roles<'a> {
            application: &'a [RoleKey],
            tenant: &'a [RoleKey],
        }
        let mut state = serializer.serialize_struct("Principal", 4)?;
        state.serialize_field("subject", &self.subject)?;
        state.serialize_field("kind", &self.kind)?;
        state.serialize_field("tenant", &self.tenant)?;
        state.serialize_field(
            "roles",
            &Roles {
                application: self.grants.application.roles(),
                tenant: self.grants.tenant.as_ref().map_or(&[], |g| g.roles()),
            },
        )?;
        state.end()
    }
}

/// The resource a permission is checked against (R5).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ResourceScope {
    /// The whole application, every tenant and resource included. Every
    /// check other designs call "global" is an application check.
    Application,
    /// One tenant and every resource in it.
    Tenant(TenantId),
    /// One resource. Without a tenant it is an application-level resource,
    /// such as an application-scoped content type.
    Resource {
        tenant: Option<TenantId>,
        kind: ResourceKind,
        id: ResourceId,
    },
}

/// A permitted check. The caller audits `BreakGlass`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Allowed {
    Grant,
    BreakGlass,
}
/// Why a check failed, for audit and debug logs only. HTTP shows
/// `unauthenticated` (401) without a principal and `unauthorized` (403)
/// otherwise.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DenyReason {
    Unauthenticated,
    NoGrant,
    TenantMismatch,
    KeyScope,
    KeyLimit,
    /// A break-glass allow whose audit row could not be written first: the
    /// use is refused, transient `audit_unavailable` (503), so no
    /// break-glass use is ever unaudited.
    AuditUnavailable,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AccessDenied {
    pub reason: DenyReason,
}
impl AccessDenied {
    pub fn new(reason: DenyReason) -> Self {
        Self { reason }
    }
    /// The failure shown to callers.
    pub fn failure(&self) -> Failure {
        match self.reason {
            DenyReason::Unauthenticated => Failure::permanent("unauthenticated"),
            DenyReason::AuditUnavailable => Failure::transient("audit_unavailable"),
            _ => Failure::permanent("unauthorized"),
        }
    }
}
impl From<AccessDenied> for Failure {
    fn from(denied: AccessDenied) -> Self {
        denied.failure()
    }
}

/// The request head an [`Authenticator`] reads.
pub type RequestHead = axum::http::request::Parts;

/// Resolves a request to a principal.
///
/// Contract: return `Ok(None)` when the request carries no credentials,
/// `Err` with permanent `unauthenticated` when credentials are present but
/// invalid, expired or revoked, and a transient failure when the backing store
/// is unavailable. Load every grant a check needs into the snapshot. Never log
/// or echo credentials. Implementations bound their own I/O; the edge applies
/// the request deadline on top.
pub trait Authenticator: Send + Sync {
    fn authenticate<'a>(
        &'a self,
        request: &'a RequestHead,
    ) -> BoxFuture<'a, Result<Option<Arc<Principal>>, Failure>>;
    /// Headers the edge adds to the error response when
    /// [`Authenticator::authenticate`] refused `request` with `failure`,
    /// such as a `Set-Cookie` that clears a dead session cookie so the
    /// browser's next request is anonymous instead of refused again. No
    /// header by default.
    fn rejection_headers(&self, request: &RequestHead, failure: &Failure) -> axum::http::HeaderMap {
        let _ = (request, failure);
        axum::http::HeaderMap::new()
    }
}

/// Permission policy over principals, extending the application's
/// command/query [`merezha_application::Authorizer`].
///
/// Contract: synchronous and I/O-free, decided from the principal's
/// snapshot. Deny with [`DenyReason::Unauthenticated`] when the principal is
/// absent. A tenant-scoped check must fail for a principal of another tenant,
/// and a tenant grant never satisfies an application check.
pub trait Authorizer: ApplicationAuthorizer {
    fn check(
        &self,
        principal: Option<&Principal>,
        permission: &Permission,
        scope: &ResourceScope,
    ) -> Result<Allowed, AccessDenied>;
    /// Whether this policy grants every check whatever the principal, as
    /// [`LoopbackAllowAll`] does. The host refuses such a policy beside an
    /// authenticator (`allow_all_with_authenticator`), so identity can never
    /// run with permission checks switched off.
    fn allows_all(&self) -> bool {
        false
    }
}

/// Fallback policy without the identity block. Constructible only for a
/// loopback listener: it is a trusted local demo policy, not security.
#[derive(Debug)]
pub struct LoopbackAllowAll(());
impl LoopbackAllowAll {
    /// Fails `allow_all_requires_loopback` for any non-loopback address.
    pub fn new(listen: SocketAddr) -> Result<Self, Failure> {
        if listen.ip().is_loopback() {
            Ok(Self(()))
        } else {
            Err(Failure::permanent("allow_all_requires_loopback"))
        }
    }
}
impl ApplicationAuthorizer for LoopbackAllowAll {
    fn authorize(
        &self,
        kind: MessageKind,
        tag: &str,
        stream: Option<&str>,
        payload: &Value,
        metadata: &Metadata,
    ) -> Result<(), Failure> {
        merezha_application::AllowAll.authorize(kind, tag, stream, payload, metadata)
    }
}
impl Authorizer for LoopbackAllowAll {
    fn check(
        &self,
        _: Option<&Principal>,
        _: &Permission,
        _: &ResourceScope,
    ) -> Result<Allowed, AccessDenied> {
        Ok(Allowed::Grant)
    }
    fn allows_all(&self) -> bool {
        true
    }
}
/// Fallback policy for a public listener without the identity block: every
/// check fails, `unauthenticated` without a principal and `unauthorized`
/// with one, as the [`Authorizer`] contract says.
#[derive(Debug, Default)]
pub struct DenyAll;
impl ApplicationAuthorizer for DenyAll {
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
impl Authorizer for DenyAll {
    fn check(
        &self,
        principal: Option<&Principal>,
        _: &Permission,
        _: &ResourceScope,
    ) -> Result<Allowed, AccessDenied> {
        Err(AccessDenied::new(match principal {
            None => DenyReason::Unauthenticated,
            Some(_) => DenyReason::NoGrant,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(value: &str) -> Identity {
        Identity::new(value).unwrap()
    }
    fn set(names: &[&str]) -> PermissionSet {
        names.iter().map(|n| id(n)).collect()
    }
    fn user(tenant: Option<&str>, grants: GrantSnapshot) -> Principal {
        Principal {
            subject: id("usr_1"),
            kind: PrincipalKind::User,
            user: Some(id("usr_1")),
            service_account: None,
            tenant: tenant.map(id),
            grants: Arc::new(grants),
            session: Some(SessionFacts {
                id: id("ses_secretish_0192"),
                method: id("password"),
                authenticated_at: jiff::Timestamp::UNIX_EPOCH,
                transport: SessionTransport::Cookie,
            }),
        }
    }
    fn resource(tenant: Option<&str>, kind: &str, rid: &str) -> ResourceScope {
        ResourceScope::Resource {
            tenant: tenant.map(id),
            kind: id(kind),
            id: id(rid),
        }
    }
    fn reason(result: Result<Allowed, AccessDenied>) -> Option<DenyReason> {
        result.err().map(|d| d.reason)
    }

    #[test]
    fn guests_see_subject_kind_tenant_and_scope_roles_only() {
        let grants = GrantSnapshot::empty()
            .with_break_glass()
            .with_application(Grants::new(
                vec![id("user"), id("admin")],
                set(&["tasks.create"]),
            ))
            .with_tenant(Grants::new(vec![id("member")], set(&["tasks.read"])));
        let principal = user(Some("org_1"), grants);
        let application = principal.guest(&ResourceScope::Application).unwrap();
        let encoded = serde_json::to_string(&application).unwrap();
        assert_eq!(
            encoded,
            r#"{"subject":"usr_1","kind":"user","tenant":"org_1","roles":["admin","user"]}"#
        );
        assert!(!encoded.contains("ses_") && !encoded.contains("tasks."));
        let tenant = principal
            .guest(&ResourceScope::Tenant(id("org_1")))
            .unwrap();
        assert_eq!(tenant.roles, vec![id("member")]);
        assert_eq!(
            principal
                .guest(&resource(Some("org_1"), "task", "t-1"))
                .unwrap()
                .roles,
            vec![id("member")]
        );
        let system = Principal::system("reactor_notifier", GrantSnapshot::empty()).unwrap();
        assert_eq!(
            serde_json::to_string(&system.guest(&ResourceScope::Tenant(id("org_1"))).unwrap())
                .unwrap(),
            r#"{"subject":"sys_reactor_notifier","kind":"system"}"#
        );
        let crowded = user(
            None,
            GrantSnapshot::empty().with_application(Grants::new(
                (0..17).map(|i| id(&format!("r{i}"))).collect(),
                set(&[]),
            )),
        );
        assert_eq!(
            crowded.guest(&ResourceScope::Application).unwrap_err().code,
            "invalid_principal"
        );
    }

    #[test]
    fn allow_all_exists_only_on_loopback() {
        let local = LoopbackAllowAll::new("127.0.0.1:1".parse().unwrap()).unwrap();
        assert_eq!(
            local.check(None, &id("tasks.create"), &ResourceScope::Application),
            Ok(Allowed::Grant)
        );
        assert!(LoopbackAllowAll::new("[::1]:1".parse().unwrap()).is_ok());
        assert_eq!(
            LoopbackAllowAll::new("0.0.0.0:1".parse().unwrap())
                .unwrap_err()
                .code,
            "allow_all_requires_loopback"
        );
        let denied = DenyAll
            .check(None, &id("tasks.create"), &ResourceScope::Application)
            .unwrap_err();
        assert_eq!(denied.failure().code, "unauthenticated");
        let someone = user(None, GrantSnapshot::empty().with_break_glass());
        let denied = DenyAll
            .check(
                Some(&someone),
                &id("tasks.create"),
                &ResourceScope::Application,
            )
            .unwrap_err();
        assert_eq!(denied.failure().code, "unauthorized");
        assert_eq!(
            AccessDenied::new(DenyReason::Unauthenticated)
                .failure()
                .code,
            "unauthenticated"
        );
    }

    #[test]
    fn application_level_resources_need_an_application_or_resource_grant() {
        let entry = resource(None, "content_type", "article");
        let publish = id("content.entries.publish");
        // A per-type grant on an application-scoped content type.
        let editor = user(
            None,
            GrantSnapshot::empty().with_application_resource(
                id("content_type"),
                id("article"),
                set(&["content.entries.publish"]),
            ),
        );
        assert_eq!(editor.check(&publish, &entry), Ok(Allowed::Grant));
        assert_eq!(
            reason(editor.check(&publish, &resource(None, "content_type", "page"))),
            Some(DenyReason::NoGrant),
            "a grant on one resource says nothing about another"
        );
        assert_eq!(
            reason(editor.check(&publish, &ResourceScope::Application)),
            Some(DenyReason::NoGrant)
        );
        // An application grant covers every scope.
        let admin = user(
            None,
            GrantSnapshot::empty().with_application(Grants::new(
                vec![id("support")],
                set(&["content.entries.publish"]),
            )),
        );
        assert_eq!(admin.check(&publish, &entry), Ok(Allowed::Grant));
        assert_eq!(
            admin.check(&publish, &ResourceScope::Tenant(id("org_9"))),
            Ok(Allowed::Grant)
        );
        // Tenant grants, even owner-like ones, never satisfy an
        // application-level resource or the application scope.
        let owner = user(
            Some("org_1"),
            GrantSnapshot::empty()
                .with_tenant(Grants::new(
                    vec![id("owner")],
                    set(&["content.entries.publish"]),
                ))
                .with_resource(
                    id("content_type"),
                    id("article"),
                    set(&["content.entries.publish"]),
                ),
        );
        assert_eq!(
            reason(owner.check(&publish, &entry)),
            Some(DenyReason::NoGrant)
        );
        assert_eq!(
            reason(owner.check(&publish, &ResourceScope::Application)),
            Some(DenyReason::NoGrant)
        );
        assert_eq!(
            owner.check(
                &publish,
                &resource(Some("org_1"), "content_type", "article")
            ),
            Ok(Allowed::Grant)
        );
    }

    #[test]
    fn tenant_scopes_follow_the_active_tenant() {
        let create = id("tasks.create");
        let member = user(
            Some("org_1"),
            GrantSnapshot::empty()
                .with_tenant(Grants::new(vec![id("member")], PermissionSet::default()))
                .with_resource(id("project"), id("p1"), set(&["tasks.create"])),
        );
        assert_eq!(
            member.check(&create, &resource(Some("org_1"), "project", "p1")),
            Ok(Allowed::Grant)
        );
        assert_eq!(
            reason(member.check(&create, &ResourceScope::Tenant(id("org_1")))),
            Some(DenyReason::NoGrant)
        );
        assert_eq!(
            reason(member.check(&create, &resource(Some("org_2"), "project", "p1"))),
            Some(DenyReason::TenantMismatch)
        );
    }

    #[test]
    fn keys_never_exceed_their_list_or_scope_and_never_break_glass() {
        let read = id("tasks.read");
        let owner_grants = GrantSnapshot::empty()
            .with_break_glass()
            .with_application(Grants::new(vec![], set(&["tasks.read", "tasks.delete"])));
        let admin = user(None, owner_grants.clone());
        assert_eq!(
            admin.check(&id("anything"), &ResourceScope::Application),
            Ok(Allowed::BreakGlass)
        );
        let pinned = user(
            Some("org_1"),
            owner_grants
                .clone()
                .with_key_limit(KeyLimit::new(set(&["tasks.read"]), Some(id("org_1")))),
        );
        assert!(!pinned.grants.is_break_glass());
        for scope in [
            ResourceScope::Application,
            resource(None, "task", "t1"),
            ResourceScope::Tenant(id("org_2")),
        ] {
            assert_eq!(
                reason(pinned.check(&read, &scope)),
                Some(DenyReason::KeyScope)
            );
        }
        assert_eq!(
            pinned.check(&read, &ResourceScope::Tenant(id("org_1"))),
            Ok(Allowed::Grant)
        );
        assert_eq!(
            reason(pinned.check(&id("tasks.delete"), &ResourceScope::Tenant(id("org_1")))),
            Some(DenyReason::KeyLimit)
        );
        let application_key = user(
            None,
            owner_grants.with_key_limit(KeyLimit::new(set(&["tasks.read"]), None)),
        );
        assert_eq!(
            application_key.check(&read, &ResourceScope::Application),
            Ok(Allowed::Grant)
        );
    }

    #[test]
    fn session_facts_are_never_serialised_or_printed() {
        let principal = user(
            Some("org_1"),
            GrantSnapshot::empty()
                .with_application(Grants::new(vec![id("user")], PermissionSet::default()))
                .with_tenant(Grants::new(vec![id("admin")], set(&["tasks.create"]))),
        );
        let json = serde_json::to_value(&principal).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "subject": "usr_1", "kind": "user", "tenant": "org_1",
                "roles": {"application": ["user"], "tenant": ["admin"]}
            })
        );
        let printed = format!("{principal:?}");
        assert!(!printed.contains("ses_secretish"), "{printed}");
        assert!(!json.to_string().contains("ses_secretish"));
        assert!(!json.to_string().contains("password"));
    }

    #[test]
    fn system_principals_use_sys_subjects() {
        let reactor = Principal::system(
            "reactor_tasks",
            GrantSnapshot::empty()
                .with_application(Grants::new(vec![], set(&["notifications.send"]))),
        )
        .unwrap();
        assert_eq!(reactor.subject.as_str(), "sys_reactor_tasks");
        assert!(reactor.is_system());
        assert_eq!(
            reactor.check(&id("notifications.send"), &ResourceScope::Application),
            Ok(Allowed::Grant)
        );
        assert_eq!(
            Principal::system("Bad Name", GrantSnapshot::empty())
                .unwrap_err()
                .code,
            "invalid_system_principal"
        );
    }
}
