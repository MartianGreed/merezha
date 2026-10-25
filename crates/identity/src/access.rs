//! Checks on identity's own routes (section 4): who is calling, the
//! permission check at `Tenant(org)` or `Application`, the reauthentication
//! requirement and the escalation rule of every grant path.
use crate::{
    core::{Actor, ApiError, Audit, Client, Core, plus},
    grants::principal_user,
    ids,
    rbac::OWNER,
};
use jiff::Timestamp;
use merezha_domain::Identity;
use merezha_platform::ports::{
    Allowed, DenyReason, PermissionSet, Principal, PrincipalKind, ResourceScope,
};
use std::sync::Arc;
use uuid::Uuid;

/// The scope a service account, an API key or an assignment lives in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    Application,
    Tenant(Uuid),
}
impl Scope {
    pub fn organization(&self) -> Option<Uuid> {
        match self {
            Scope::Application => None,
            Scope::Tenant(org) => Some(*org),
        }
    }
    pub fn resource_scope(&self) -> ResourceScope {
        match self {
            Scope::Application => ResourceScope::Application,
            Scope::Tenant(org) => ResourceScope::Tenant(ids::identity(ids::ORGANIZATION, *org)),
        }
    }
    /// One resource in this scope.
    pub fn resource(&self, kind: &Identity, id: &Identity) -> ResourceScope {
        ResourceScope::Resource {
            tenant: self
                .organization()
                .map(|org| ids::identity(ids::ORGANIZATION, org)),
            kind: kind.clone(),
            id: id.clone(),
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Scope::Application => "application",
            Scope::Tenant(_) => "tenant",
        }
    }
}

/// The caller of an identity route: the principal the edge authenticated
/// (re-scoped to the route's organization), the audit actor and the
/// client facts.
#[derive(Clone, Debug)]
pub struct Who {
    pub principal: Arc<Principal>,
    pub actor: Actor,
    /// The user behind the principal: the session's, or a user key's owner.
    pub user: Option<Uuid>,
    /// The session id and its authentication time, for session principals.
    pub session: Option<(Uuid, Timestamp)>,
    pub client: Client,
}
impl Who {
    /// `None` without a principal.
    pub fn new(principal: Option<Arc<Principal>>, client: Client) -> Option<Self> {
        let principal = principal?;
        let user = principal_user(&principal);
        let session = principal.session.as_ref().and_then(|facts| {
            ids::parse(ids::SESSION, facts.id.as_str())
                .ok()
                .map(|id| (id, facts.authenticated_at))
        });
        let actor = match principal.kind {
            PrincipalKind::User => user.map_or(Actor::Anonymous, Actor::User),
            PrincipalKind::ApiKey => ids::parse(ids::API_KEY, principal.subject.as_str())
                .map_or(Actor::Anonymous, Actor::ApiKey),
            PrincipalKind::System => Actor::System,
        };
        Some(Self {
            principal,
            actor,
            user,
            session,
            client,
        })
    }
    /// An audit row by this caller.
    pub fn audit(&self, action: &'static str) -> Audit {
        let mut audit = Audit::new(action, self.actor);
        if let (Actor::ApiKey(_), Some(user)) = (self.actor, self.user) {
            audit = audit.target(user);
        }
        audit
    }
    /// The created-by id stored on rows: the user, else none.
    pub fn creator(&self) -> Option<Uuid> {
        self.user
    }
}

fn permission(name: &str) -> Result<Identity, ApiError> {
    Identity::new(name).map_err(|_| ApiError::code("unknown_permission"))
}

impl Core {
    /// Check `name` at `scope` for `who`. A break-glass allow is used only
    /// once its audit row is written (`503 audit_unavailable` otherwise); a
    /// denial is audited once per principal and permission per instance.
    pub async fn require(
        self: &Arc<Self>,
        who: &Who,
        name: &str,
        scope: &ResourceScope,
    ) -> Result<Allowed, ApiError> {
        let permission = permission(name)?;
        match who.principal.check(&permission, scope) {
            Ok(allowed) => {
                if allowed == Allowed::BreakGlass {
                    self.audit_break_glass(&who.principal, &permission).await?;
                }
                Ok(allowed)
            }
            Err(denied) => {
                let marker = who
                    .session
                    .map_or(who.principal.subject.as_str().to_owned(), |(s, _)| {
                        s.simple().to_string()
                    });
                if self.audited.first(&marker, &format!("deny:{name}")) {
                    let mut audit = who
                        .audit("authorization.denied")
                        .denied()
                        .detail("permission", name)
                        .detail("reason", reason(denied.reason));
                    if let ResourceScope::Tenant(t)
                    | ResourceScope::Resource {
                        tenant: Some(t), ..
                    } = scope
                        && let Ok(org) = ids::parse(ids::ORGANIZATION, t.as_str())
                    {
                        audit = audit.organization(org);
                    }
                    self.audit_later(audit);
                }
                Err(ApiError::from(denied.failure()))
            }
        }
    }
    /// `who` acting in `org` for one of identity's tenant routes, after the
    /// check of `name` at `Tenant(org)`. A denial for a caller without any
    /// standing in `org` (no membership, no application grant) is
    /// `404 not_found`, so organization ids cannot be probed (section 4).
    pub async fn in_org(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        name: &str,
    ) -> Result<Who, ApiError> {
        let scoped = self.scoped(who, org).await?;
        match self
            .require(&scoped, name, &Scope::Tenant(org).resource_scope())
            .await
        {
            Ok(_) => Ok(scoped),
            Err(error) if !self.standing(&scoped, org) => {
                let _ = error;
                Err(ApiError::code("not_found"))
            }
            Err(error) => Err(error),
        }
    }
    /// `who` re-scoped to `org` without a check.
    pub async fn scoped(self: &Arc<Self>, who: &Who, org: Uuid) -> Result<Who, ApiError> {
        let principal = self.principal_in(&who.principal, org).await?;
        Ok(Who {
            principal,
            ..who.clone()
        })
    }
    /// Whether `who` is active in `org` (a member, the organization's
    /// tenant key or service account, or an entered organization).
    pub fn standing(&self, who: &Who, org: Uuid) -> bool {
        who.principal.tenant.as_ref() == Some(&ids::identity(ids::ORGANIZATION, org))
    }
    /// The application check of `name`.
    pub async fn in_application(self: &Arc<Self>, who: &Who, name: &str) -> Result<(), ApiError> {
        self.require(who, name, &ResourceScope::Application)
            .await
            .map(|_| ())
    }
    /// A session authenticated within the reauthentication window. Keys
    /// are never recent: such routes need a session.
    pub fn recent(&self, who: &Who) -> Result<(), ApiError> {
        let Some((_, authenticated_at)) = who.session else {
            return Err(ApiError::code("unauthorized"));
        };
        if plus(
            authenticated_at,
            self.settings.sessions.reauthentication_window,
        ) < self.now()
        {
            Err(ApiError::code("reauthentication_required"))
        } else {
            Ok(())
        }
    }
    /// The escalation rule (section 4): the caller holds every permission
    /// of `permissions` at `scope`. A break-glass session holds them all.
    pub fn holds_all(&self, who: &Who, permissions: &PermissionSet, scope: &ResourceScope) -> bool {
        permissions
            .iter()
            .all(|p| who.principal.check(p, scope).is_ok())
    }
    /// [`Core::holds_all`] or `permission_escalation`.
    pub fn no_escalation(
        &self,
        who: &Who,
        permissions: &PermissionSet,
        scope: &ResourceScope,
    ) -> Result<(), ApiError> {
        if self.holds_all(who, permissions, scope) {
            Ok(())
        } else {
            Err(ApiError::code("permission_escalation"))
        }
    }
    /// Only owners assign `owner`: the caller holds it in `org`, or breaks
    /// glass.
    pub fn owner_in(&self, who: &Who, org: Uuid) -> bool {
        if who.principal.grants.is_break_glass() {
            return true;
        }
        self.standing(who, org)
            && who
                .principal
                .grants
                .tenant()
                .is_some_and(|g| g.roles().iter().any(|r| r.as_str() == OWNER))
    }
    /// The catalog permissions `who` may exercise at `scope`, sorted.
    pub fn effective(&self, who: &Who, scope: &ResourceScope) -> Vec<String> {
        self.rbac
            .catalog()
            .entries()
            .filter(|(name, _)| {
                Identity::new(*name).is_ok_and(|p| who.principal.check(&p, scope).is_ok())
            })
            .map(|(name, _)| name.to_owned())
            .collect()
    }
}

fn reason(reason: DenyReason) -> &'static str {
    match reason {
        DenyReason::Unauthenticated => "unauthenticated",
        DenyReason::NoGrant => "no_grant",
        DenyReason::TenantMismatch => "tenant_mismatch",
        DenyReason::KeyScope => "key_scope",
        DenyReason::KeyLimit => "key_limit",
        DenyReason::AuditUnavailable => "audit_unavailable",
    }
}
