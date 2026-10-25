//! Loading grant snapshots (section 5): the rows behind a principal, the
//! version-stamped cache in front of them, and the principals of sessions
//! and API keys in a given organization.
use crate::{
    core::{Actor, Audit, Client, Core, sql},
    ids,
    rbac::{ApplicationRow, Holder, Stamp, TenantRow},
};
use merezha_domain::{Failure, Identity};
use merezha_platform::ports::{GrantSnapshot, Principal, PrincipalKind};
use merezha_postgres::Session;
use std::sync::Arc;
use uuid::Uuid;

/// Who holds the grants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Owner {
    User(Uuid),
    /// A service account; `organization` is `Some` for a tenant one.
    ServiceAccount {
        id: Uuid,
        organization: Option<Uuid>,
    },
}
impl Owner {
    pub fn holder(&self) -> Holder {
        match self {
            Owner::User(_) => Holder::User,
            Owner::ServiceAccount { .. } => Holder::ServiceAccount,
        }
    }
}

/// The organization a snapshot is built for and where its tenant grants
/// come from: the owner's membership, or the tenant service account itself.
/// `holder` is `None` for an organization entered without one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OrgFacts {
    pub id: Uuid,
    pub version: i64,
    pub holder: Option<Uuid>,
}

/// An API key's limit: its stored list and pinned organization.
#[derive(Clone, Debug)]
pub struct KeyFacts {
    pub grants: Vec<String>,
    pub organization: Option<Uuid>,
}

impl Core {
    /// The snapshot of `credential` (a session or key id) held by `owner`
    /// at version `owner_version`, in `org` when given, limited by `key`.
    /// Served from the cache while every version matches.
    pub(crate) async fn snapshot(
        &self,
        session: &impl Session,
        credential: Uuid,
        owner: Owner,
        owner_version: i64,
        org: Option<OrgFacts>,
        key: Option<&KeyFacts>,
    ) -> Result<Arc<GrantSnapshot>, Failure> {
        let stamp = Stamp {
            owner_version,
            org_version: org.map(|o| o.version),
            generation: self.rbac.generation(),
            membership: org.and_then(|o| o.holder),
        };
        let cache_key = (credential, org.map(|o| o.id));
        if let Some(snapshot) = self.grants.get(cache_key, &stamp) {
            self.metrics.authz_cache("hit");
            return Ok(snapshot);
        }
        self.metrics.authz_cache("miss");
        let max_resource = self.settings.rbac.max_resource_assignments as usize;
        let limit =
            i64::try_from(self.rbac.declared().count() + 2 + max_resource + 1).unwrap_or(i64::MAX);
        let rows = match owner {
            Owner::User(user) => {
                self.db()
                    .query(
                        session,
                        &crate::sql::GRANTS_APPLICATION_USER,
                        &[&user, &limit],
                    )
                    .await
            }
            Owner::ServiceAccount { id, .. } => {
                self.db()
                    .query(
                        session,
                        &crate::sql::GRANTS_APPLICATION_SERVICE,
                        &[&id, &limit],
                    )
                    .await
            }
        }
        .map_err(sql)?;
        let application: Vec<ApplicationRow> = rows
            .iter()
            .map(|r| ApplicationRow {
                role_key: r.get(0),
                resource: resource(r.get(1), r.get(2)),
            })
            .collect();
        let tenant = match org {
            None => None,
            Some(OrgFacts { holder: None, .. }) => Some(Vec::new()),
            Some(OrgFacts {
                id,
                holder: Some(holder),
                ..
            }) => {
                let (membership, service): (Option<Uuid>, Option<Uuid>) = match owner {
                    Owner::User(_) => (Some(holder), None),
                    Owner::ServiceAccount { .. } => (None, Some(holder)),
                };
                let limit =
                    i64::from(self.settings.rbac.max_custom_roles) + 4 + max_resource as i64;
                let rows = self
                    .db()
                    .query(
                        session,
                        &crate::sql::GRANTS_TENANT,
                        &[&membership, &service, &id, &limit],
                    )
                    .await
                    .map_err(sql)?;
                Some(
                    rows.iter()
                        .filter_map(|r| {
                            let key: Option<String> = r.get(0);
                            let custom: bool = r.get(1);
                            let permissions: Option<Vec<String>> = r.get(2);
                            Some(TenantRow {
                                role_key: key?,
                                custom: if custom {
                                    Some(permissions.unwrap_or_default())
                                } else {
                                    None
                                },
                                resource: resource(r.get(3), r.get(4)),
                            })
                        })
                        .collect::<Vec<_>>(),
                )
            }
        };
        let key_limit = key.map(|k| {
            self.rbac.key_limit(
                &k.grants,
                k.organization
                    .map(|org| ids::identity(ids::ORGANIZATION, org)),
            )
        });
        let (snapshot, truncated) = self.rbac.snapshot(
            owner.holder(),
            &application,
            tenant.as_deref(),
            key_limit,
            max_resource,
        );
        if truncated > 0 {
            merezha_telemetry::event!(WARN, "identity.grants_truncated", count = truncated as u64);
        }
        let snapshot = Arc::new(snapshot);
        self.grants.put(cache_key, stamp, snapshot.clone());
        Ok(snapshot)
    }

    /// The principal of a user session (section 4): the session's user
    /// with the snapshot for `org`.
    pub(crate) fn user_principal(
        &self,
        user: Uuid,
        tenant: Option<Uuid>,
        grants: Arc<GrantSnapshot>,
        session: Option<merezha_platform::ports::SessionFacts>,
    ) -> Principal {
        let id = ids::identity(ids::USER, user);
        Principal {
            subject: id.clone(),
            kind: PrincipalKind::User,
            user: Some(id),
            service_account: None,
            tenant: tenant.map(|t| ids::identity(ids::ORGANIZATION, t)),
            grants,
            session,
        }
    }
    /// The principal of an API key.
    pub(crate) fn key_principal(
        &self,
        key: Uuid,
        owner: Owner,
        tenant: Option<Uuid>,
        grants: Arc<GrantSnapshot>,
    ) -> Principal {
        let (user, service_account) = match owner {
            Owner::User(user) => (Some(ids::identity(ids::USER, user)), None),
            Owner::ServiceAccount { id, .. } => {
                (None, Some(ids::identity(ids::SERVICE_ACCOUNT, id)))
            }
        };
        Principal {
            subject: ids::identity(ids::API_KEY, key),
            kind: PrincipalKind::ApiKey,
            user,
            service_account,
            tenant: tenant.map(|t| ids::identity(ids::ORGANIZATION, t)),
            grants,
            session: None,
        }
    }

    /// `principal` acting in organization `org`, for identity's tenant
    /// routes. A principal already active there is returned as is. A user
    /// session that is a member of `org` gets the snapshot for `org`; any
    /// other principal stays as it is, so only its application grants can
    /// satisfy a check in `org` (a tenant key of another organization fails
    /// `key_scope`).
    pub(crate) async fn principal_in(
        self: &Arc<Self>,
        principal: &Arc<Principal>,
        org: Uuid,
    ) -> Result<Arc<Principal>, Failure> {
        let target = ids::identity(ids::ORGANIZATION, org);
        if principal.tenant.as_ref() == Some(&target) || principal.is_system() {
            return Ok(principal.clone());
        }
        let session = self.db().acquire(self.deadline()).await?;
        match principal.kind {
            PrincipalKind::User => {
                let (Some(user), Some(facts)) = (principal.user.as_ref(), &principal.session)
                else {
                    return Ok(principal.clone());
                };
                let user = ids::parse(ids::USER, user.as_str())?;
                let credential = ids::parse(ids::SESSION, facts.id.as_str())?;
                let row = self
                    .db()
                    .query_opt(&session, &crate::sql::USER_IN_ORGANIZATION, &[&user, &org])
                    .await
                    .map_err(sql)?;
                let Some(row) = row else {
                    return Err(Failure::permanent("unauthenticated"));
                };
                let version: i64 = row.get(0);
                let (Some(org_id), Some(org_version), Some(membership)) = (
                    row.get::<_, Option<Uuid>>(2),
                    row.get::<_, Option<i64>>(3),
                    row.get::<_, Option<Uuid>>(4),
                ) else {
                    return Ok(principal.clone());
                };
                let grants = self
                    .snapshot(
                        &session,
                        credential,
                        Owner::User(user),
                        version,
                        Some(OrgFacts {
                            id: org_id,
                            version: org_version,
                            holder: Some(membership),
                        }),
                        None,
                    )
                    .await?;
                Ok(Arc::new(self.user_principal(
                    user,
                    Some(org),
                    grants,
                    principal.session.clone(),
                )))
            }
            // A tenant key acts in its own organization only and an
            // application key never in one: either keeps its principal, so
            // only what it already holds can satisfy a check in `org`.
            PrincipalKind::ApiKey => Ok(principal.clone()),
            PrincipalKind::System => Ok(principal.clone()),
        }
    }
}

impl Core {
    /// Sync the declared application roles into `roles` (section 5), under
    /// a transaction-scoped advisory lock so instances starting together
    /// agree. A declared role removed from configuration while still
    /// assigned stops startup (`declared_role_in_use`, logged with the role
    /// key and the count); one without assignments is deleted. Stored
    /// custom role grants that name nothing in the catalog are counted and
    /// logged: evaluation ignores them.
    pub(crate) async fn sync_declared_roles(&self) -> Result<(), Failure> {
        let declared: Vec<(String, String, Option<String>)> = self
            .rbac
            .declared()
            .map(|(key, role, _)| (key.to_owned(), role.name.clone(), role.description.clone()))
            .collect();
        let now = self.now();
        let ids_: Vec<Uuid> = declared
            .iter()
            .map(|_| self.new_id())
            .collect::<Result<_, _>>()?;
        let catalog: Vec<String> = self
            .rbac
            .catalog()
            .entries()
            .map(|(name, _)| name.to_owned())
            .collect();
        let db = self.db().clone();
        let unknown: i64 = self
            .db()
            .transaction(self.deadline(), move |tx| {
                Box::pin(async move {
                    db.execute(&*tx, &crate::sql::ROLES_SYNC_LOCK, &[])
                        .await
                        .map_err(sql)?;
                    for ((key, name, description), id) in declared.iter().zip(&ids_) {
                        let row = db
                            .query_opt(
                                &*tx,
                                &crate::sql::ROLE_UPSERT_DECLARED,
                                &[id, key, name, description, &now],
                            )
                            .await
                            .map_err(sql)?;
                        if row.is_none() {
                            return Err(Failure::permanent("declared_role_in_use"));
                        }
                    }
                    let keys: Vec<&str> = declared.iter().map(|(k, _, _)| k.as_str()).collect();
                    let stale = db
                        .query(&*tx, &crate::sql::ROLES_UNDECLARED, &[&keys])
                        .await
                        .map_err(sql)?;
                    for row in &stale {
                        let id: Uuid = row.get(0);
                        let key: String = row.get(1);
                        let count: i64 = row.get(2);
                        if count > 0 {
                            merezha_telemetry::event!(
                                ERROR,
                                "identity.declared_role_in_use",
                                role = key.as_str(),
                                count = count as u64
                            );
                            return Err(Failure::permanent("declared_role_in_use"));
                        }
                        db.execute(&*tx, &crate::sql::ROLE_DELETE_DECLARED, &[&id])
                            .await
                            .map_err(sql)?;
                    }
                    let unknown: i64 = db
                        .query_one(&*tx, &crate::sql::ROLES_UNKNOWN_GRANTS, &[&catalog])
                        .await
                        .map_err(sql)?
                        .get(0);
                    Ok(unknown)
                })
            })
            .await?;
        if unknown > 0 {
            merezha_telemetry::event!(WARN, "identity.unknown_permissions", count = unknown as u64);
        }
        Ok(())
    }

    /// Audit `organization.entered` once per session and organization per
    /// instance.
    pub(crate) fn entered(self: &Arc<Self>, user: Uuid, session: Uuid, org: Uuid) {
        if self.audited.first(
            &session.simple().to_string(),
            &format!("enter:{}", org.simple()),
        ) {
            self.audit_later(
                Audit::new("organization.entered", Actor::User(user)).organization(org),
            );
        }
    }
    /// [`Core::entered`] for an application key.
    pub(crate) fn entered_by_key(self: &Arc<Self>, user: Option<Uuid>, key: Uuid, org: Uuid) {
        if self.audited.first(
            &key.simple().to_string(),
            &format!("enter:{}", org.simple()),
        ) {
            let mut audit =
                Audit::new("organization.entered", Actor::ApiKey(key)).organization(org);
            if let Some(user) = user {
                audit = audit.target(user);
            }
            self.audit_later(audit);
        }
    }
    /// Audit `authorization.break_glass` once per session and permission
    /// per instance (section 5).
    pub(crate) fn break_glass(self: &Arc<Self>, principal: &Principal, permission: &Identity) {
        let who = principal
            .session
            .as_ref()
            .map_or(principal.subject.as_str(), |s| s.id.as_str())
            .to_owned();
        if !self.audited.first(&who, permission.as_str()) {
            return;
        }
        let Some(user) = principal_user(principal) else {
            return;
        };
        let mut audit = Audit::new("authorization.break_glass", Actor::User(user))
            .detail("permission", permission.as_str());
        if let Some(tenant) = principal
            .tenant
            .as_ref()
            .and_then(|t| ids::parse(ids::ORGANIZATION, t.as_str()).ok())
        {
            audit = audit.organization(tenant);
        }
        self.audit_later(audit);
    }
    /// Write `audit` in its own transaction on a background task, for the
    /// synchronous paths (the `Authorizer`). At most
    /// [`crate::core::AUDIT_SLOTS`] writes run at once; past that the row is
    /// dropped and counted as an audit write failure.
    pub(crate) fn audit_later(self: &Arc<Self>, audit: Audit) {
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            self.metrics.audit_failure();
            return;
        };
        let Ok(slot) = self.audit_slots.clone().try_acquire_owned() else {
            self.metrics.audit_failure();
            return;
        };
        let core = self.clone();
        handle.spawn(async move {
            core.audit_alone(audit, &Client::default()).await;
            drop(slot);
        });
    }
}

fn resource(kind: Option<String>, id: Option<String>) -> Option<(String, String)> {
    Some((kind?, id?))
}

/// The organization id of a `Merezha-Organization` header value, or
/// `not_found`.
pub fn header_organization(value: &str) -> Result<Uuid, Failure> {
    ids::parse(ids::ORGANIZATION, value.trim())
}

/// Whether the principal is `user` itself (a session) or one of its keys:
/// the `user` access of section 4.
pub fn principal_user(principal: &Principal) -> Option<Uuid> {
    let user = principal.user.as_ref()?;
    ids::parse(ids::USER, user.as_str()).ok()
}

/// Rendered ids as identities.
pub fn org_identity(org: Uuid) -> Identity {
    ids::identity(ids::ORGANIZATION, org)
}
