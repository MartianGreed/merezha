//! Route access (AP §C.4, R6): who is calling, as the host established it,
//! and the host's policy for the permission a route declares. The
//! application checks every route before any guest call, so a refused
//! caller costs no guest fuel and never reaches a stored receipt.
//!
//! The platform implements [`Caller`] over its principal and its
//! `Authorizer` (`merezha-platform` depends on this crate, not the other
//! way round). [`Anonymous`], [`AllowAll`] and a bare
//! [`Principal`] cover embedding hosts and tests.
use crate::{
    AllowAll,
    config::{RouteScope, Rule},
};
use merezha_domain::{Failure, Identity, Principal};

/// Where a permission is checked: the application's mirror of the
/// platform's `ResourceScope` (R5).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Scope {
    /// The whole application.
    Application,
    /// One tenant, the caller's active tenant.
    Tenant(Identity),
    /// One resource of the caller's tenant: `kind` from the route's
    /// `resource`, `id` the command's local stream id.
    Resource {
        tenant: Option<Identity>,
        kind: Identity,
        id: Identity,
    },
}

/// The caller of one command or query, as the host established it, with
/// the host's permission policy for it. Never built from a request body.
pub trait Caller {
    /// The projection a version 2 guest receives, with the role keys held
    /// at `scope`; `None` for an anonymous caller.
    fn principal(&self, scope: &Scope) -> Option<Principal>;
    /// The principal's active tenant, which tenant-scoped routes write and
    /// read under.
    fn tenant(&self) -> Option<Identity>;
    /// Whether a principal is present.
    fn authenticated(&self) -> bool;
    /// Decide `permission` at `scope`: `unauthenticated` without a
    /// principal, `unauthorized` when the grant is missing, or the
    /// policy's own transient failure (`audit_unavailable`).
    fn check(&self, permission: &Identity, scope: &Scope) -> Result<(), Failure>;
    /// Whether the policy is the explicit allow-all of a trusted local
    /// host without identity: `authenticated` routes then pass anonymous
    /// callers too.
    fn allows_all(&self) -> bool {
        false
    }
    /// A route that declares neither `permission` nor `access` (version
    /// 1). Embedding hosts keep their payload [`crate::Authorizer`] as the
    /// policy, as before version 2, so the default allows it; the platform
    /// allows it only under its loopback allow-all policy (identity refuses
    /// such routes at startup).
    fn undeclared(&self) -> Result<(), Failure> {
        Ok(())
    }
}

/// An anonymous caller without a host policy: public and undeclared routes
/// pass, every other route fails `unauthenticated`. What
/// [`crate::Application::command`] and [`crate::Application::query`] use.
#[derive(Clone, Copy, Debug, Default)]
pub struct Anonymous;
impl Caller for Anonymous {
    fn principal(&self, _: &Scope) -> Option<Principal> {
        None
    }
    fn tenant(&self) -> Option<Identity> {
        None
    }
    fn authenticated(&self) -> bool {
        false
    }
    fn check(&self, _: &Identity, _: &Scope) -> Result<(), Failure> {
        Err(Failure::permanent("unauthenticated"))
    }
}

/// The trusted local demo policy (`merezha serve`): an anonymous caller
/// that every declared permission admits. Never an authentication
/// mechanism; tenant-scoped routes still need a tenant (`tenant_required`).
impl Caller for AllowAll {
    fn principal(&self, _: &Scope) -> Option<Principal> {
        None
    }
    fn tenant(&self) -> Option<Identity> {
        None
    }
    fn authenticated(&self) -> bool {
        false
    }
    fn check(&self, _: &Identity, _: &Scope) -> Result<(), Failure> {
        Ok(())
    }
    fn allows_all(&self) -> bool {
        true
    }
}

/// A host-established principal whose grants the application cannot see:
/// it is the subject, kind, tenant and roles of every call, `public`,
/// `authenticated` and undeclared routes admit it, and every permission
/// check fails `unauthorized`. Hosts with a policy implement [`Caller`]
/// themselves.
impl Caller for Principal {
    fn principal(&self, _: &Scope) -> Option<Principal> {
        Some(self.clone())
    }
    fn tenant(&self) -> Option<Identity> {
        self.tenant.clone()
    }
    fn authenticated(&self) -> bool {
        true
    }
    fn check(&self, _: &Identity, _: &Scope) -> Result<(), Failure> {
        Err(Failure::permanent("unauthorized"))
    }
}
impl<C: Caller + ?Sized> Caller for &C {
    fn principal(&self, scope: &Scope) -> Option<Principal> {
        (**self).principal(scope)
    }
    fn tenant(&self) -> Option<Identity> {
        (**self).tenant()
    }
    fn authenticated(&self) -> bool {
        (**self).authenticated()
    }
    fn check(&self, permission: &Identity, scope: &Scope) -> Result<(), Failure> {
        (**self).check(permission, scope)
    }
    fn allows_all(&self) -> bool {
        (**self).allows_all()
    }
    fn undeclared(&self) -> Result<(), Failure> {
        (**self).undeclared()
    }
}
impl<C: Caller + ?Sized> Caller for std::sync::Arc<C> {
    fn principal(&self, scope: &Scope) -> Option<Principal> {
        (**self).principal(scope)
    }
    fn tenant(&self) -> Option<Identity> {
        (**self).tenant()
    }
    fn authenticated(&self) -> bool {
        (**self).authenticated()
    }
    fn check(&self, permission: &Identity, scope: &Scope) -> Result<(), Failure> {
        (**self).check(permission, scope)
    }
    fn allows_all(&self) -> bool {
        (**self).allows_all()
    }
    fn undeclared(&self) -> Result<(), Failure> {
        (**self).undeclared()
    }
}

/// What a route admitted: the tenant its streams and receipts belong to
/// (tenant scope) and the scope the permission was checked at, whose roles
/// the guest sees.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Admitted {
    pub tenant: Option<Identity>,
    pub scope: Scope,
}

/// Tenant ids as streams store them (`merezha_app.streams`): 1 to 40 of
/// `[A-Za-z0-9_-]`, so the tenant ends at the stream's first `:`.
fn valid_tenant(tenant: &Identity) -> bool {
    let bytes = tenant.as_str().as_bytes();
    (1..=40).contains(&bytes.len())
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-')
}

/// Enforce a route (AP §C.4 steps 2 and 3) before any guest call:
///
/// 1. `public` passes; `authenticated` and `permission` need a principal
///    (`unauthenticated`), unless the policy allows all; undeclared routes
///    ask [`Caller::undeclared`];
/// 2. a tenant route takes the principal's active tenant, never one the
///    request names (`tenant_required` without one);
/// 3. a `permission` is checked at `Application`, `Tenant(tenant)` or, for a
///    command with `resource`, `Resource { tenant, kind, id: stream }`.
pub(crate) fn admit(
    caller: &dyn Caller,
    rule: Rule<'_>,
    scope: RouteScope,
    resource: Option<(&Identity, &Identity)>,
) -> Result<Admitted, Failure> {
    match rule {
        Rule::Undeclared => caller.undeclared()?,
        Rule::Public => {}
        Rule::Authenticated | Rule::Permission(_) => {
            if !caller.authenticated() && !caller.allows_all() {
                return Err(Failure::permanent("unauthenticated"));
            }
        }
    }
    let tenant = match scope {
        RouteScope::Application => None,
        RouteScope::Tenant => {
            let tenant = caller
                .tenant()
                .ok_or_else(|| Failure::permanent("tenant_required"))?;
            if !valid_tenant(&tenant) {
                return Err(Failure::permanent("validation_failed"));
            }
            Some(tenant)
        }
    };
    let scope = match (&tenant, resource) {
        (None, _) => Scope::Application,
        (Some(tenant), None) => Scope::Tenant(tenant.clone()),
        (Some(tenant), Some((kind, id))) => Scope::Resource {
            tenant: Some(tenant.clone()),
            kind: kind.clone(),
            id: id.clone(),
        },
    };
    if let Rule::Permission(permission) = rule {
        caller.check(permission, &scope)?;
    }
    Ok(Admitted { tenant, scope })
}

#[cfg(test)]
mod tests {
    use super::*;
    use merezha_domain::PrincipalKind;

    fn id(value: &str) -> Identity {
        Identity::new(value).unwrap()
    }
    fn principal(tenant: Option<&str>) -> Principal {
        Principal::new(id("usr_1"), PrincipalKind::User, tenant.map(id), vec![]).unwrap()
    }
    fn code(result: Result<Admitted, Failure>) -> String {
        result.unwrap_err().code
    }

    #[test]
    fn rules_scopes_and_tenants_are_enforced_in_order() {
        let read = id("tasks.read");
        let permission = Rule::Permission(&read);
        let tenant = RouteScope::Tenant;
        let app = RouteScope::Application;
        // Anonymous: public and undeclared pass at application scope.
        for rule in [Rule::Public, Rule::Undeclared] {
            assert_eq!(
                admit(&Anonymous, rule, app, None).unwrap().scope,
                Scope::Application
            );
        }
        for rule in [Rule::Authenticated, permission] {
            assert_eq!(code(admit(&Anonymous, rule, app, None)), "unauthenticated");
            assert_eq!(
                code(admit(&Anonymous, rule, tenant, None)),
                "unauthenticated"
            );
        }
        assert_eq!(
            code(admit(&Anonymous, Rule::Public, tenant, None)),
            "tenant_required"
        );
        // The local allow-all admits permissions but has no tenant.
        assert!(admit(&AllowAll, permission, app, None).is_ok());
        assert!(admit(&AllowAll, Rule::Authenticated, app, None).is_ok());
        assert_eq!(
            code(admit(&AllowAll, permission, tenant, None)),
            "tenant_required"
        );
        // A bare principal: no grants are visible, so permissions fail.
        let member = principal(Some("org_1"));
        assert_eq!(code(admit(&member, permission, app, None)), "unauthorized");
        let admitted = admit(&member, Rule::Authenticated, tenant, None).unwrap();
        assert_eq!(admitted.tenant, Some(id("org_1")));
        assert_eq!(admitted.scope, Scope::Tenant(id("org_1")));
        assert_eq!(
            code(admit(&principal(None), Rule::Authenticated, tenant, None)),
            "tenant_required"
        );
        let resource = admit(
            &member,
            Rule::Authenticated,
            tenant,
            Some((&id("task"), &id("t-1"))),
        )
        .unwrap();
        assert_eq!(
            resource.scope,
            Scope::Resource {
                tenant: Some(id("org_1")),
                kind: id("task"),
                id: id("t-1")
            }
        );
        // A tenant id that cannot prefix a stream.
        for bad in ["org:1", "org/1", "org.1", &"o".repeat(41)] {
            assert_eq!(
                code(admit(
                    &principal(Some(bad)),
                    Rule::Authenticated,
                    tenant,
                    None
                )),
                "validation_failed",
                "{bad}"
            );
        }
    }
}
