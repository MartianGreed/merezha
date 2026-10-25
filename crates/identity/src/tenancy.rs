//! The tenant use cases of section 4 (PR 2): organizations, the active
//! organization, members, invitations, custom roles and tenant role
//! assignments. Every write locks the organization row first, which
//! serializes owner changes, membership removal and key minting, and runs
//! in one transaction with its audit row, its integration events and the
//! version bumps that make the change visible on the next request on every
//! instance (section 5).
use crate::{
    access::{Scope, Who},
    core::{Actor, ApiError, Audit, Client, Core, Method, UserRow, event, sql, topic},
    flows::{Committed, Reply},
    ids,
    normalize::Email,
    rbac::{OWNER, builtin_tenant_id, builtin_tenant_role},
    secrets::RawToken,
};
use jiff::Timestamp;
use merezha_domain::{Failure, Identity};
use merezha_platform::{Tx, ports::PermissionSet};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::Row;
use uuid::Uuid;

/// Most roles in one request (`role_ids`).
pub const MAX_ROLE_IDS: usize = 16;

// ----- shapes -----

pub(crate) fn org_json(row: &Row) -> Value {
    json!({
        "id": ids::render(ids::ORGANIZATION, row.get(0)),
        "slug": row.get::<_, String>(1),
        "name": row.get::<_, String>(2),
        "created_at": row.get::<_, Timestamp>(4).to_string(),
        "updated_at": row.get::<_, Timestamp>(5).to_string(),
    })
}
pub(crate) fn membership_json(id: Uuid, org: Uuid, user: Uuid, created_at: Timestamp) -> Value {
    json!({
        "id": ids::render(ids::MEMBERSHIP, id),
        "organization_id": ids::render(ids::ORGANIZATION, org),
        "user_id": ids::render(ids::USER, user),
        "created_at": created_at.to_string(),
    })
}
fn member_json(row: &Row, roles: Vec<Value>) -> Value {
    json!({
        "id": ids::render(ids::MEMBERSHIP, row.get(0)),
        "organization_id": ids::render(ids::ORGANIZATION, row.get(1)),
        "user": {
            "id": ids::render(ids::USER, row.get(2)),
            "email": row.get::<_, String>(4),
            "display_name": row.get::<_, Option<String>>(5),
        },
        "roles": roles,
        "created_at": row.get::<_, Timestamp>(3).to_string(),
    })
}
pub(crate) fn role_ref_json(id: Uuid, key: &str) -> Value {
    json!({"id": ids::render(ids::ROLE, id), "key": key})
}
pub(crate) fn assignment_json(row: &Row, org: Uuid) -> Value {
    let role_id = match (
        row.get::<_, Option<String>>(3),
        row.get::<_, Option<Uuid>>(4),
    ) {
        (_, Some(custom)) => Some(custom),
        (Some(builtin), None) => builtin_tenant_id(&builtin),
        _ => None,
    };
    json!({
        "id": ids::render(ids::ASSIGNMENT, row.get(0)),
        "scope": "tenant",
        "organization_id": ids::render(ids::ORGANIZATION, org),
        "membership_id": row.get::<_, Option<Uuid>>(1).map(|m| ids::render(ids::MEMBERSHIP, m)),
        "service_account_id": row.get::<_, Option<Uuid>>(2).map(|s| ids::render(ids::SERVICE_ACCOUNT, s)),
        "user_id": row.get::<_, Option<Uuid>>(10).map(|u| ids::render(ids::USER, u)),
        "role_id": role_id.map(|r| ids::render(ids::ROLE, r)),
        "role_key": row.get::<_, Option<String>>(5),
        "resource": resource_json(row.get(6), row.get(7)),
        "created_at": row.get::<_, Timestamp>(9).to_string(),
    })
}
pub(crate) fn resource_json(kind: Option<String>, id: Option<String>) -> Value {
    match (kind, id) {
        (Some(kind), Some(id)) => json!({"kind": kind, "id": id}),
        _ => Value::Null,
    }
}
fn invitation_json(row: &Row) -> Value {
    let roles: Vec<Uuid> = row.get(7);
    json!({
        "id": ids::render(ids::INVITATION, row.get(0)),
        "organization_id": ids::render(ids::ORGANIZATION, row.get(1)),
        "email": row.get::<_, String>(2),
        "role_ids": roles.into_iter().map(|r| ids::render(ids::ROLE, r)).collect::<Vec<_>>(),
        "invited_by": row.get::<_, Option<Uuid>>(4).map(|u| ids::render(ids::USER, u)),
        "invited_by_key": row.get::<_, Option<Uuid>>(8).map(|k| ids::render(ids::API_KEY, k)),
        "created_at": row.get::<_, Timestamp>(5).to_string(),
        "expires_at": row.get::<_, Timestamp>(6).to_string(),
    })
}

/// `a***@example.com`: the first character of the local part only.
pub fn mask_email(email: &str) -> String {
    match email.rsplit_once('@') {
        Some((local, domain)) => {
            let first: String = local.chars().take(1).collect();
            format!("{first}***@{domain}")
        }
        None => "***".to_owned(),
    }
}

/// A role an organization may use, resolved under its lock.
#[derive(Clone, Debug)]
pub(crate) struct RoleRef {
    pub id: Uuid,
    pub key: String,
    /// The stored grants of a custom role; `None` for a built-in one.
    pub custom: Option<Vec<String>>,
}
impl RoleRef {
    fn from_row(row: &Row) -> Self {
        let org: Option<Uuid> = row.get(2);
        Self {
            id: row.get(0),
            key: row.get(3),
            custom: org.map(|_| row.get(6)),
        }
    }
    pub fn builtin(&self) -> Option<&'static str> {
        if self.custom.is_some() {
            None
        } else {
            builtin_tenant_role(self.id)
        }
    }
}

/// The input of a role assignment: who, which role, where.
#[derive(Clone, Debug)]
pub struct AssignmentInput {
    pub membership: Option<Uuid>,
    pub service_account: Option<Uuid>,
    pub user: Option<Uuid>,
    pub role: Uuid,
    pub resource: Option<(Identity, Identity)>,
}

impl Core {
    /// The permissions of a tenant role.
    pub(crate) fn role_permissions(&self, role: &RoleRef) -> PermissionSet {
        match &role.custom {
            Some(grants) => self.rbac.custom(grants),
            None => self
                .rbac
                .tenant_builtin(&role.key)
                .cloned()
                .unwrap_or_default(),
        }
    }
    /// Lock organization `org`; `not_found` when it does not exist.
    pub(crate) async fn lock_org(&self, tx: &mut Tx<'_>, org: Uuid) -> Result<Row, ApiError> {
        self.db()
            .query_opt(&*tx, &crate::sql::ORG_LOCK, &[&org])
            .await
            .map_err(sql)?
            .ok_or_else(|| ApiError::code("not_found"))
    }
    /// Resolve `role` for `org` under its lock: a built-in tenant role or
    /// one of the organization's own custom roles, else `not_found`, so a
    /// custom role of another organization can never be used (section 3).
    pub(crate) async fn tenant_role(
        &self,
        tx: &mut Tx<'_>,
        org: Uuid,
        role: Uuid,
    ) -> Result<RoleRef, ApiError> {
        let row = self
            .db()
            .query_opt(&*tx, &crate::sql::ROLE_IN_ORG, &[&role, &org])
            .await
            .map_err(sql)?
            .ok_or_else(|| ApiError::code("not_found"))?;
        Ok(RoleRef::from_row(&row))
    }
    /// The escalation rule for granting or removing `role` at `scope` in
    /// `org`: hold all of its permissions there, and be an owner for
    /// `owner`, re-read under the organization lock the caller holds, so a
    /// demotion racing the request is honoured.
    pub(crate) async fn may_grant(
        &self,
        tx: &mut Tx<'_>,
        who: &Who,
        org: Uuid,
        role: &RoleRef,
        scope: &merezha_platform::ports::ResourceScope,
    ) -> Result<(), ApiError> {
        if role.builtin() == Some(OWNER) && !self.owner_live(tx, who, org).await? {
            return Err(ApiError::code("permission_escalation"));
        }
        self.no_escalation(who, &self.role_permissions(role), scope)
    }
    /// Whether `who` is an owner of `org` right now (or a live platform
    /// admin breaking glass); the caller holds the organization lock.
    pub(crate) async fn owner_live(
        &self,
        tx: &mut Tx<'_>,
        who: &Who,
        org: Uuid,
    ) -> Result<bool, Failure> {
        if !self.owner_in(who, org) {
            return Ok(false);
        }
        let Some(user) = who.user else {
            return Ok(false);
        };
        let row = if who.principal.grants.is_break_glass() {
            self.db()
                .query_one(&*tx, &crate::sql::USER_IS_PLATFORM_ADMIN, &[&user])
                .await
        } else {
            self.db()
                .query_one(&*tx, &crate::sql::USER_IS_OWNER, &[&org, &user])
                .await
        };
        Ok(row.map_err(sql)?.get(0))
    }
    /// The live grants of member `user` in `org`, read in the caller's
    /// transaction without the cache; `None` without an active membership.
    pub(crate) async fn live_member(
        &self,
        tx: &mut Tx<'_>,
        org: Uuid,
        user: Uuid,
        client: &Client,
    ) -> Result<Option<Who>, Failure> {
        let row = self
            .db()
            .query_opt(&*tx, &crate::sql::USER_IN_ORGANIZATION, &[&user, &org])
            .await
            .map_err(sql)?;
        let Some(row) = row else {
            return Ok(None);
        };
        let status: String = row.get(1);
        let (Some(org_id), Some(version), Some(membership)) = (
            row.get::<_, Option<Uuid>>(2),
            row.get::<_, Option<i64>>(3),
            row.get::<_, Option<Uuid>>(4),
        ) else {
            return Ok(None);
        };
        if status != "active" {
            return Ok(None);
        }
        let grants = self
            .live_snapshot(
                &*tx,
                crate::grants::Owner::User(user),
                Some(crate::grants::OrgFacts {
                    id: org_id,
                    version,
                    holder: Some(membership),
                }),
                None,
            )
            .await?;
        let principal = Arc::new(self.user_principal(user, Some(org), grants, None));
        Ok(Who::new(Some(principal), client.clone()))
    }
    /// The inviter of an invitation as it stands now: the inviting key, or
    /// the inviting member; `None` when it can no longer act in `org`.
    async fn live_inviter(
        self: &Arc<Self>,
        tx: &mut Tx<'_>,
        org: Uuid,
        user: Option<Uuid>,
        key: Option<Uuid>,
        client: &Client,
    ) -> Result<Option<Who>, Failure> {
        match (key, user) {
            (Some(key), _) => Ok(self
                .live_key(&*tx, key, org)
                .await?
                .and_then(|p| Who::new(Some(Arc::new(p)), client.clone()))),
            (None, Some(user)) => self.live_member(tx, org, user, client).await,
            (None, None) => Ok(None),
        }
    }
    /// Revoke every key `user` minted or owns, anywhere, audited with
    /// `reason`: its account is being deleted or disabled. The service
    /// accounts stay.
    pub(crate) async fn revoke_everything_minted(
        &self,
        tx: &mut Tx<'_>,
        user: Uuid,
        reason: &'static str,
        actor: crate::core::Actor,
        client: &Client,
    ) -> Result<(), Failure> {
        let revoked = self
            .db()
            .query(
                &*tx,
                &crate::sql::KEYS_REVOKE_MINTED_ALL,
                &[&user, &self.now()],
            )
            .await
            .map_err(sql)?;
        for key in &revoked {
            let mut audit = Audit::new("api_key.revoked", actor)
                .target_of("api_key", key.get(0))
                .detail("reason", reason);
            if let Some(org) = key.get::<_, Option<Uuid>>(1) {
                audit = audit.organization(org);
            }
            self.audit(tx, audit, client).await?;
        }
        Ok(())
    }
    /// What a member keeps after losing authority in `org` (removed, or a
    /// role change; the caller holds the organization lock): the service
    /// account keys it minted are revoked unless it still holds
    /// `identity.service_accounts.manage` there, its own keys unless it
    /// still holds `identity.api_keys.create`, and the invitations it sent
    /// are deleted unless it still holds `identity.members.invite`. A
    /// removed member keeps none of them.
    pub(crate) async fn after_authority_change(
        &self,
        tx: &mut Tx<'_>,
        org: Uuid,
        user: Uuid,
        removed: bool,
        who: &Who,
    ) -> Result<(), Failure> {
        let live = if removed {
            None
        } else {
            self.live_member(tx, org, user, &who.client).await?
        };
        let scope = Scope::Tenant(org).resource_scope();
        let holds = |name: &str| {
            live.as_ref().is_some_and(|w| {
                Identity::new(name).is_ok_and(|p| w.principal.check(&p, &scope).is_ok())
            })
        };
        let reason = if removed {
            "membership_removed"
        } else {
            "minter_demoted"
        };
        let now = self.now();
        let mut revoked = Vec::new();
        if !holds("identity.service_accounts.manage") {
            revoked.extend(
                self.db()
                    .query(&*tx, &crate::sql::KEYS_REVOKE_MINTED, &[&user, &org, &now])
                    .await
                    .map_err(sql)?,
            );
        }
        if !holds("identity.api_keys.create") {
            revoked.extend(
                self.db()
                    .query(&*tx, &crate::sql::KEYS_REVOKE_MEMBER, &[&user, &org, &now])
                    .await
                    .map_err(sql)?,
            );
        }
        for key in &revoked {
            self.audit(
                tx,
                who.audit("api_key.revoked")
                    .organization(org)
                    .target_of("api_key", key.get(0))
                    .detail("reason", reason),
                &who.client,
            )
            .await?;
        }
        if !holds("identity.members.invite") {
            let deleted = self
                .db()
                .query(
                    &*tx,
                    &crate::sql::INVITATIONS_DELETE_BY_INVITER,
                    &[&org, &user],
                )
                .await
                .map_err(sql)?;
            for invitation in &deleted {
                self.audit(
                    tx,
                    who.audit("invitation.revoked")
                        .organization(org)
                        .target_of("invitation", invitation.get(0))
                        .detail(
                            "reason",
                            if removed {
                                "inviter_removed"
                            } else {
                                "inviter_demoted"
                            },
                        ),
                    &who.client,
                )
                .await?;
            }
        }
        Ok(())
    }
    /// The organization-scope roles of a membership.
    async fn member_roles(
        &self,
        tx: &mut Tx<'_>,
        org: Uuid,
        membership: Uuid,
    ) -> Result<Vec<(Uuid, RoleRef)>, Failure> {
        let rows = self
            .db()
            .query(
                &*tx,
                &crate::sql::ASSIGNMENTS_OF_HOLDER,
                &[&org, &Some(membership), &None::<Uuid>],
            )
            .await
            .map_err(sql)?;
        Ok(rows
            .iter()
            .filter_map(|r| {
                let assignment: Uuid = r.get(0);
                let builtin: Option<String> = r.get(3);
                let custom: Option<Uuid> = r.get(4);
                let key: Option<String> = r.get(5);
                let role = match (builtin, custom) {
                    (Some(builtin), None) => RoleRef {
                        id: builtin_tenant_id(&builtin)?,
                        key: builtin,
                        custom: None,
                    },
                    (None, Some(custom)) => RoleRef {
                        id: custom,
                        key: key?,
                        custom: Some(Vec::new()),
                    },
                    _ => return None,
                };
                Some((assignment, role))
            })
            .collect())
    }
    /// Refuse `sole_owner` when `user` is the last owner of `org`; the
    /// caller holds the organization lock.
    pub(crate) async fn keep_an_owner(
        &self,
        tx: &mut Tx<'_>,
        org: Uuid,
        user: Uuid,
    ) -> Result<(), ApiError> {
        let others: i64 = self
            .db()
            .query_one(&*tx, &crate::sql::OTHER_OWNERS, &[&org, &user])
            .await
            .map_err(sql)?
            .get(0);
        if others == 0 {
            Err(ApiError::conflict("sole_owner"))
        } else {
            Ok(())
        }
    }
    pub(crate) async fn bump_user(&self, tx: &mut Tx<'_>, user: Uuid) -> Result<(), Failure> {
        self.db()
            .execute(&*tx, &crate::sql::USER_BUMP, &[&user, &self.now()])
            .await
            .map_err(sql)?;
        Ok(())
    }
    async fn role_keys_of(
        &self,
        tx: &mut Tx<'_>,
        org: Uuid,
        membership: Uuid,
    ) -> Result<Vec<String>, Failure> {
        let mut keys: Vec<String> = self
            .member_roles(tx, org, membership)
            .await?
            .into_iter()
            .map(|(_, r)| r.key)
            .collect();
        keys.sort();
        keys.dedup();
        Ok(keys)
    }
    async fn publish_roles_changed(
        &self,
        tx: &mut Tx<'_>,
        org: Uuid,
        membership: Uuid,
        user: Uuid,
        client: &Client,
    ) -> Result<(), Failure> {
        let keys = self.role_keys_of(tx, org, membership).await?;
        let change = self.new_id()?;
        self.publish_in(
            tx,
            event::MEMBERSHIP_ROLES_CHANGED,
            format!(
                "membership:{}:roles:{}",
                membership.simple(),
                change.simple()
            ),
            json!({
                "membership_id": ids::render(ids::MEMBERSHIP, membership),
                "organization_id": ids::render(ids::ORGANIZATION, org),
                "user_id": ids::render(ids::USER, user),
                "role_keys": keys,
            }),
            client,
            Some(org),
        )
        .await
    }
    /// Insert membership `user` of `org` with `roles` and publish
    /// `identity.membership_created`; `None` when the user already is one.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn add_member(
        &self,
        tx: &mut Tx<'_>,
        org: Uuid,
        user: Uuid,
        roles: &[RoleRef],
        via: &'static str,
        created_by: Option<Uuid>,
        client: &Client,
    ) -> Result<Option<(Uuid, Timestamp)>, Failure> {
        let id = self.new_id()?;
        let now = self.now();
        let inserted = self
            .db()
            .query_opt(
                &*tx,
                &crate::sql::MEMBERSHIP_INSERT,
                &[&id, &org, &user, &now],
            )
            .await
            .map_err(sql)?;
        if inserted.is_none() {
            return Ok(None);
        }
        for role in roles {
            self.insert_assignment(tx, org, Some(id), None, role, None, created_by)
                .await?;
        }
        self.bump_user(tx, user).await?;
        let mut keys: Vec<String> = roles.iter().map(|r| r.key.clone()).collect();
        keys.sort();
        keys.dedup();
        self.publish_in(
            tx,
            event::MEMBERSHIP_CREATED,
            format!("membership:{}:created", id.simple()),
            json!({
                "membership_id": ids::render(ids::MEMBERSHIP, id),
                "organization_id": ids::render(ids::ORGANIZATION, org),
                "user_id": ids::render(ids::USER, user),
                "role_keys": keys,
                "via": via,
            }),
            client,
            Some(org),
        )
        .await?;
        Ok(Some((id, now)))
    }
    /// Insert one tenant assignment; `false` when it already exists.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn insert_assignment(
        &self,
        tx: &mut Tx<'_>,
        org: Uuid,
        membership: Option<Uuid>,
        service_account: Option<Uuid>,
        role: &RoleRef,
        resource: Option<(&str, &str)>,
        created_by: Option<Uuid>,
    ) -> Result<Option<Uuid>, Failure> {
        let id = self.new_id()?;
        let builtin = role.builtin();
        let custom = role.custom.as_ref().map(|_| role.id);
        let row = self
            .db()
            .query_opt(
                &*tx,
                &crate::sql::ASSIGNMENT_INSERT,
                &[
                    &id,
                    &org,
                    &membership,
                    &service_account,
                    &builtin,
                    &custom,
                    &resource.map(|r| r.0),
                    &resource.map(|r| r.1),
                    &created_by,
                    &self.now(),
                ],
            )
            .await
            .map_err(|e| {
                let failure = sql(e);
                // A custom role deleted meanwhile: the composite foreign
                // key refuses the row, so no assignment dangles.
                if failure.code == "constraint_violation" {
                    Failure::permanent("not_found")
                } else {
                    failure
                }
            })?;
        Ok(row.map(|r| r.get(0)))
    }

    // ----- organizations -----

    /// `POST /v1/identity/organizations`: the caller becomes its owner.
    pub async fn create_organization(
        self: &Arc<Self>,
        who: &Who,
        name: String,
        slug: String,
    ) -> Result<Reply, ApiError> {
        let Some((_, _)) = who.session else {
            return Err(ApiError::code("unauthorized"));
        };
        let user = who.user.ok_or_else(|| ApiError::code("unauthorized"))?;
        if !self.settings.organizations.user_can_create && !who.principal.grants.is_break_glass() {
            return Err(ApiError::code("unauthorized"));
        }
        let core = self.clone();
        let who = who.clone();
        let outcome: Committed<Value> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    // The user lock serializes the per-user count.
                    if core.user_by_id(tx, user, true).await?.is_none() {
                        return Ok(Err(ApiError::code("unauthenticated")));
                    }
                    let created: i64 = core
                        .db()
                        .query_one(&*tx, &crate::sql::ORG_COUNT_CREATED_BY, &[&user])
                        .await
                        .map_err(sql)?
                        .get(0);
                    if created >= i64::from(core.settings.organizations.max_per_user) {
                        return Ok(Err(ApiError::code("organization_limit")));
                    }
                    let id = core.new_id()?;
                    let now = core.now();
                    let inserted = core
                        .db()
                        .query_opt(
                            &*tx,
                            &crate::sql::ORG_INSERT,
                            &[&id, &slug, &name, &Some(user), &now],
                        )
                        .await
                        .map_err(sql)?;
                    if inserted.is_none() {
                        return Ok(Err(ApiError::conflict("slug_taken")));
                    }
                    let owner = RoleRef {
                        id: crate::rbac::OWNER_ID,
                        key: OWNER.to_owned(),
                        custom: None,
                    };
                    let (membership, joined) = core
                        .add_member(tx, id, user, &[owner], "creation", Some(user), &who.client)
                        .await?
                        .ok_or_else(|| Failure::transient("internal"))?;
                    core.audit(
                        tx,
                        who.audit("organization.created")
                            .organization(id)
                            .target_of("organization", id),
                        &who.client,
                    )
                    .await?;
                    core.audit(
                        tx,
                        who.audit("membership.created")
                            .organization(id)
                            .target(user)
                            .detail("role_keys", json!([OWNER])),
                        &who.client,
                    )
                    .await?;
                    core.publish_in(
                        tx,
                        event::ORGANIZATION_CREATED,
                        format!("organization:{}:created", id.simple()),
                        json!({
                            "organization_id": ids::render(ids::ORGANIZATION, id),
                            "slug": slug,
                            "created_by": ids::render(ids::USER, user),
                            "created_at": now.to_string(),
                        }),
                        &who.client,
                        Some(id),
                    )
                    .await?;
                    let row = core
                        .db()
                        .query_one(&*tx, &crate::sql::ORG_GET, &[&id])
                        .await
                        .map_err(sql)?;
                    Ok(Ok(json!({
                        "organization": org_json(&row),
                        "membership": membership_json(membership, id, user, joined),
                    })))
                })
            })
            .await?;
        Ok(Reply::json(201, outcome?))
    }

    /// `GET /v1/identity/organizations`: the caller's organizations.
    pub async fn list_organizations(
        self: &Arc<Self>,
        who: &Who,
        cursor: Option<Uuid>,
        limit: i64,
    ) -> Result<Reply, ApiError> {
        let user = who.user.ok_or_else(|| ApiError::code("unauthorized"))?;
        let client = self.db().acquire(self.deadline()).await?;
        let rows = self
            .db()
            .query(
                &client,
                &crate::sql::ORGS_OF_USER,
                &[&user, &cursor, &(limit + 1)],
            )
            .await
            .map_err(sql)?;
        let (items, next) = page(
            &rows,
            limit,
            |r| {
                json!({
                    "organization": org_json(r),
                    "membership": membership_json(r.get(6), r.get(0), user, r.get(7)),
                })
            },
            |r| ids::render(ids::ORGANIZATION, r.get(0)),
        );
        Ok(Reply::json(
            200,
            json!({"items": items, "next_cursor": next}),
        ))
    }

    /// `PUT /v1/identity/session/organization`: the session's active
    /// organization, one the caller belongs to or, with
    /// `identity.organization.enter`, any (audited `organization.entered`).
    pub async fn select_organization(
        self: &Arc<Self>,
        who: &Who,
        org: Option<Uuid>,
    ) -> Result<Reply, ApiError> {
        let (Some((session, _)), Some(user), Actor::User(_)) = (who.session, who.user, who.actor)
        else {
            return Err(ApiError::code("unauthorized"));
        };
        let enter = self.may_enter(&who.principal);
        let core = self.clone();
        let client = who.client.clone();
        let outcome: Committed<(crate::core::SessionRow, bool)> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let mut entered = false;
                    if let Some(org) = org {
                        let exists = core
                            .db()
                            .query_opt(&*tx, &crate::sql::ORG_GET, &[&org])
                            .await
                            .map_err(sql)?
                            .is_some();
                        let member = core
                            .db()
                            .query_opt(&*tx, &crate::sql::MEMBERSHIP_OF_USER, &[&org, &user])
                            .await
                            .map_err(sql)?
                            .is_some();
                        if !exists || !(member || enter) {
                            return Ok(Err(ApiError::code("not_found")));
                        }
                        entered = !member;
                    }
                    core.db()
                        .execute(
                            &*tx,
                            &crate::sql::SESSION_SET_ORGANIZATION,
                            &[&session, &org],
                        )
                        .await
                        .map_err(sql)?;
                    if entered && let Some(org) = org {
                        core.audit(
                            tx,
                            Audit::new("organization.entered", Actor::User(user)).organization(org),
                            &client,
                        )
                        .await?;
                    }
                    let row = core
                        .session(tx, session)
                        .await?
                        .ok_or_else(|| ApiError::code("unauthenticated"))?;
                    Ok(Ok((row, entered)))
                })
            })
            .await?;
        let (row, _) = outcome?;
        Ok(Reply::json(200, json!({"session": row.json(true)})))
    }

    /// `GET /organizations/{org}`.
    pub async fn get_organization(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
    ) -> Result<Reply, ApiError> {
        self.in_org(who, org, "identity.organization.read").await?;
        let client = self.db().acquire(self.deadline()).await?;
        let row = self
            .db()
            .query_opt(&client, &crate::sql::ORG_GET, &[&org])
            .await
            .map_err(sql)?
            .ok_or_else(|| ApiError::code("not_found"))?;
        Ok(Reply::json(200, json!({"organization": org_json(&row)})))
    }

    /// `PATCH /organizations/{org}`.
    pub async fn update_organization(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        name: Option<String>,
        slug: Option<String>,
    ) -> Result<Reply, ApiError> {
        let who = self
            .in_org(who, org, "identity.organization.update")
            .await?;
        let core = self.clone();
        let outcome: Committed<Value> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_org(tx, org).await?;
                    if let Some(slug) = &slug {
                        let taken = core
                            .db()
                            .query_opt(&*tx, &crate::sql::ORG_SLUG_TAKEN, &[slug, &org])
                            .await
                            .map_err(sql)?;
                        if taken.is_some() {
                            return Ok(Err(ApiError::conflict("slug_taken")));
                        }
                    }
                    let now = core.now();
                    core.db()
                        .execute(&*tx, &crate::sql::ORG_UPDATE, &[&org, &name, &slug, &now])
                        .await
                        .map_err(|e| {
                            let failure = sql(e);
                            if failure.code == "unique_violation" {
                                Failure::conflict("slug_taken")
                            } else {
                                failure
                            }
                        })?;
                    let row = core
                        .db()
                        .query_one(&*tx, &crate::sql::ORG_GET, &[&org])
                        .await
                        .map_err(sql)?;
                    core.audit(
                        tx,
                        who.audit("organization.updated").organization(org),
                        &who.client,
                    )
                    .await?;
                    core.publish_in(
                        tx,
                        event::ORGANIZATION_UPDATED,
                        format!(
                            "organization:{}:updated:{}",
                            org.simple(),
                            core.new_id()?.simple()
                        ),
                        json!({
                            "organization_id": ids::render(ids::ORGANIZATION, org),
                            "slug": row.get::<_, String>(1),
                            "updated_at": now.to_string(),
                        }),
                        &who.client,
                        Some(org),
                    )
                    .await?;
                    Ok(Ok(json!({"organization": org_json(&row)})))
                })
            })
            .await?;
        Ok(Reply::json(200, outcome?))
    }

    /// `DELETE /organizations/{org}` (recent): every membership, role,
    /// assignment, invitation, service account and key goes with it; the
    /// members' snapshots rebuild on their next request.
    pub async fn delete_organization(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        confirm_slug: String,
    ) -> Result<Reply, ApiError> {
        let who = self
            .in_org(who, org, "identity.organization.delete")
            .await?;
        self.recent(&who)?;
        let core = self.clone();
        let outcome: Committed<()> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let row = core.lock_org(tx, org).await?;
                    if row.get::<_, String>(1) != confirm_slug {
                        return Ok(Err(ApiError::invalid(vec![("/confirm_slug", "mismatch")])));
                    }
                    core.db()
                        .execute(&*tx, &crate::sql::USERS_BUMP_OF_ORG, &[&org])
                        .await
                        .map_err(sql)?;
                    core.db()
                        .execute(&*tx, &crate::sql::ORG_DELETE, &[&org])
                        .await
                        .map_err(sql)?;
                    core.audit(
                        tx,
                        who.audit("organization.deleted")
                            .organization(org)
                            .target_of("organization", org),
                        &who.client,
                    )
                    .await?;
                    core.publish_in(
                        tx,
                        event::ORGANIZATION_DELETED,
                        format!("organization:{}:deleted", org.simple()),
                        json!({
                            "organization_id": ids::render(ids::ORGANIZATION, org),
                            "deleted_at": core.now().to_string(),
                        }),
                        &who.client,
                        Some(org),
                    )
                    .await?;
                    Ok(Ok(()))
                })
            })
            .await?;
        outcome?;
        Ok(Reply::empty(crate::flows::CookieAction::Keep))
    }

    /// `POST /organizations/{org}/ownership-transfers` (recent): the target
    /// member becomes an owner and, unless `keep_owner`, the caller stops
    /// being one.
    pub async fn transfer_ownership(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        membership: Uuid,
        keep_owner: bool,
    ) -> Result<Reply, ApiError> {
        let who = self
            .in_org(who, org, "identity.organization.transfer")
            .await?;
        self.recent(&who)?;
        if !self.owner_in(&who, org) {
            return Err(ApiError::code("permission_escalation"));
        }
        let core = self.clone();
        let outcome: Committed<Value> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_org(tx, org).await?;
                    // Re-read under the lock: an owner demoted meanwhile
                    // transfers nothing.
                    if !core.owner_live(tx, &who, org).await? {
                        return Ok(Err(ApiError::code("permission_escalation")));
                    }
                    let Some(target) = core
                        .db()
                        .query_opt(&*tx, &crate::sql::MEMBERSHIP_GET, &[&membership, &org])
                        .await
                        .map_err(sql)?
                    else {
                        return Ok(Err(ApiError::code("not_found")));
                    };
                    let target_user: Uuid = target.get(2);
                    let owner = RoleRef {
                        id: crate::rbac::OWNER_ID,
                        key: OWNER.to_owned(),
                        custom: None,
                    };
                    core.insert_assignment(
                        tx,
                        org,
                        Some(membership),
                        None,
                        &owner,
                        None,
                        who.creator(),
                    )
                    .await?;
                    core.bump_user(tx, target_user).await?;
                    core.publish_roles_changed(tx, org, membership, target_user, &who.client)
                        .await?;
                    if !keep_owner
                        && let Some(user) = who.user
                        && user != target_user
                        && let Some(mine) = core
                            .db()
                            .query_opt(&*tx, &crate::sql::MEMBERSHIP_OF_USER, &[&org, &user])
                            .await
                            .map_err(sql)?
                    {
                        let mine: Uuid = mine.get(0);
                        for (assignment, role) in core.member_roles(tx, org, mine).await? {
                            if role.builtin() == Some(OWNER) {
                                core.db()
                                    .execute(
                                        &*tx,
                                        &crate::sql::ASSIGNMENT_DELETE,
                                        &[&assignment, &org],
                                    )
                                    .await
                                    .map_err(sql)?;
                            }
                        }
                        core.bump_user(tx, user).await?;
                        core.after_authority_change(tx, org, user, false, &who)
                            .await?;
                        core.publish_roles_changed(tx, org, mine, user, &who.client)
                            .await?;
                    }
                    core.audit(
                        tx,
                        who.audit("organization.ownership_transferred")
                            .organization(org)
                            .target(target_user)
                            .detail("keep_owner", keep_owner),
                        &who.client,
                    )
                    .await?;
                    Ok(Ok(json!({"membership": membership_json(
                        membership, org, target_user, target.get(3))})))
                })
            })
            .await?;
        Ok(Reply::json(200, outcome?))
    }

    /// `GET /organizations/{org}/me/permissions`: what the caller may do
    /// in `org`.
    pub async fn org_permissions(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
    ) -> Result<Reply, ApiError> {
        if who.user.is_none() {
            return Err(ApiError::code("unauthorized"));
        }
        let who = self.scoped(who, org).await?;
        if !self.standing(&who, org) {
            return Err(ApiError::code("not_found"));
        }
        let roles: Vec<String> = who
            .principal
            .grants
            .tenant()
            .map(|g| g.roles().iter().map(|r| r.as_str().to_owned()).collect())
            .unwrap_or_default();
        Ok(Reply::json(
            200,
            json!({
                "organization_id": ids::render(ids::ORGANIZATION, org),
                "roles": roles,
                "permissions": self.effective(&who, &Scope::Tenant(org).resource_scope()),
            }),
        ))
    }

    // ----- members -----

    /// `GET /organizations/{org}/members`.
    pub async fn list_members(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        cursor: Option<Uuid>,
        limit: i64,
    ) -> Result<Reply, ApiError> {
        self.in_org(who, org, "identity.members.read").await?;
        let client = self.db().acquire(self.deadline()).await?;
        let rows = self
            .db()
            .query(
                &client,
                &crate::sql::MEMBERS_LIST,
                &[&org, &cursor, &(limit + 1)],
            )
            .await
            .map_err(sql)?;
        let shown: Vec<Uuid> = rows.iter().take(limit as usize).map(|r| r.get(0)).collect();
        let roles = self
            .db()
            .query(&client, &crate::sql::ROLES_OF_MEMBERSHIPS, &[&org, &shown])
            .await
            .map_err(sql)?;
        let (items, next) = page(
            &rows,
            limit,
            |r| {
                let id: Uuid = r.get(0);
                let mine: Vec<Value> = roles
                    .iter()
                    .filter(|x| x.get::<_, Uuid>(0) == id)
                    .map(|x| {
                        role_ref_json(x.get(1), &x.get::<_, Option<String>>(2).unwrap_or_default())
                    })
                    .collect();
                member_json(r, mine)
            },
            |r| ids::render(ids::MEMBERSHIP, r.get(0)),
        );
        Ok(Reply::json(
            200,
            json!({"items": items, "next_cursor": next}),
        ))
    }

    /// One member with its roles.
    async fn member(
        &self,
        tx: &mut Tx<'_>,
        org: Uuid,
        membership: Uuid,
    ) -> Result<Value, ApiError> {
        let row = self
            .db()
            .query_opt(&*tx, &crate::sql::MEMBERSHIP_GET, &[&membership, &org])
            .await
            .map_err(sql)?
            .ok_or_else(|| ApiError::code("not_found"))?;
        let roles: Vec<Value> = self
            .member_roles(tx, org, membership)
            .await?
            .iter()
            .map(|(_, role)| role_ref_json(role.id, &role.key))
            .collect();
        Ok(member_json(&row, roles))
    }

    /// `PATCH /organizations/{org}/members/{membership}`: replace the
    /// member's organization-scope roles. Each role added or removed must
    /// be one the caller could grant; `owner` only by an owner, and never
    /// the last one away.
    pub async fn update_member_roles(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        membership: Uuid,
        role_ids: Vec<Uuid>,
    ) -> Result<Reply, ApiError> {
        let who = self
            .in_org(who, org, "identity.members.update_roles")
            .await?;
        let core = self.clone();
        let outcome: Committed<Value> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_org(tx, org).await?;
                    let Some(target) = core
                        .db()
                        .query_opt(&*tx, &crate::sql::MEMBERSHIP_GET, &[&membership, &org])
                        .await
                        .map_err(sql)?
                    else {
                        return Ok(Err(ApiError::code("not_found")));
                    };
                    let user: Uuid = target.get(2);
                    let mut wanted = Vec::new();
                    for id in &role_ids {
                        let role = core.tenant_role(tx, org, *id).await?;
                        if !wanted.iter().any(|r: &RoleRef| r.id == role.id) {
                            wanted.push(role);
                        }
                    }
                    let current = core.member_roles(tx, org, membership).await?;
                    let scope = Scope::Tenant(org).resource_scope();
                    let mut changed = false;
                    for (assignment, role) in &current {
                        if wanted.iter().any(|w| w.id == role.id) {
                            continue;
                        }
                        let role = core.tenant_role(tx, org, role.id).await?;
                        core.may_grant(tx, &who, org, &role, &scope).await?;
                        if role.builtin() == Some(OWNER) {
                            core.keep_an_owner(tx, org, user).await?;
                        }
                        core.db()
                            .execute(&*tx, &crate::sql::ASSIGNMENT_DELETE, &[assignment, &org])
                            .await
                            .map_err(sql)?;
                        changed = true;
                    }
                    for role in &wanted {
                        if current.iter().any(|(_, c)| c.id == role.id) {
                            continue;
                        }
                        core.may_grant(tx, &who, org, role, &scope).await?;
                        core.insert_assignment(
                            tx,
                            org,
                            Some(membership),
                            None,
                            role,
                            None,
                            who.creator(),
                        )
                        .await?;
                        changed = true;
                    }
                    if changed {
                        core.bump_user(tx, user).await?;
                        core.after_authority_change(tx, org, user, false, &who)
                            .await?;
                        let keys = core.role_keys_of(tx, org, membership).await?;
                        core.audit(
                            tx,
                            who.audit("membership.roles_changed")
                                .organization(org)
                                .target(user)
                                .detail("role_keys", json!(keys)),
                            &who.client,
                        )
                        .await?;
                        core.publish_roles_changed(tx, org, membership, user, &who.client)
                            .await?;
                    }
                    Ok(Ok(
                        json!({"member": core.member(tx, org, membership).await?}),
                    ))
                })
            })
            .await?;
        Ok(Reply::json(200, outcome?))
    }

    /// `DELETE /organizations/{org}/members/{membership}`: remove a member
    /// (`identity.members.remove`, or the member itself). In one
    /// transaction under the organization lock: the membership and its
    /// assignments go, the user's keys for the organization are revoked
    /// (never revived by rejoining), its sessions lose the organization and
    /// its version is bumped.
    pub async fn remove_member(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        membership: Uuid,
    ) -> Result<Reply, ApiError> {
        let scoped = self.scoped(who, org).await?;
        let core = self.clone();
        let outcome: Committed<()> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_org(tx, org).await?;
                    let target = core
                        .db()
                        .query_opt(&*tx, &crate::sql::MEMBERSHIP_GET, &[&membership, &org])
                        .await
                        .map_err(sql)?;
                    let own = target
                        .as_ref()
                        .is_some_and(|t| Some(t.get::<_, Uuid>(2)) == scoped.user)
                        && scoped.session.is_some();
                    if !own {
                        match core
                            .require(
                                &scoped,
                                "identity.members.remove",
                                &Scope::Tenant(org).resource_scope(),
                            )
                            .await
                        {
                            Ok(_) => {}
                            Err(_) if !core.standing(&scoped, org) => {
                                return Ok(Err(ApiError::code("not_found")));
                            }
                            Err(error) => return Ok(Err(error)),
                        }
                    }
                    let Some(target) = target else {
                        return Ok(Err(ApiError::code("not_found")));
                    };
                    let user: Uuid = target.get(2);
                    let roles = core.member_roles(tx, org, membership).await?;
                    if roles.iter().any(|(_, r)| r.builtin() == Some(OWNER)) {
                        if !own && !core.owner_live(tx, &scoped, org).await? {
                            return Ok(Err(ApiError::code("permission_escalation")));
                        }
                        core.keep_an_owner(tx, org, user).await?;
                    }
                    core.remove_membership(
                        tx,
                        org,
                        membership,
                        user,
                        if own { "left" } else { "removed" },
                        &scoped,
                    )
                    .await?;
                    Ok(Ok(()))
                })
            })
            .await?;
        outcome?;
        Ok(Reply::empty(crate::flows::CookieAction::Keep))
    }
    /// The removal itself; the caller holds the organization lock.
    async fn remove_membership(
        &self,
        tx: &mut Tx<'_>,
        org: Uuid,
        membership: Uuid,
        user: Uuid,
        reason: &'static str,
        who: &Who,
    ) -> Result<(), Failure> {
        self.db()
            .execute(&*tx, &crate::sql::MEMBERSHIP_DELETE, &[&membership, &org])
            .await
            .map_err(sql)?;
        // Its keys for the organization, the service account keys it
        // minted there and its pending invitations go with it.
        self.after_authority_change(tx, org, user, true, who)
            .await?;
        self.db()
            .execute(
                &*tx,
                &crate::sql::SESSIONS_LEAVE_ORGANIZATION,
                &[&user, &org],
            )
            .await
            .map_err(sql)?;
        self.bump_user(tx, user).await?;
        self.audit(
            tx,
            who.audit("membership.removed")
                .organization(org)
                .target(user)
                .detail("reason", reason),
            &who.client,
        )
        .await?;
        self.publish_in(
            tx,
            event::MEMBERSHIP_REMOVED,
            format!("membership:{}:removed", membership.simple()),
            json!({
                "membership_id": ids::render(ids::MEMBERSHIP, membership),
                "organization_id": ids::render(ids::ORGANIZATION, org),
                "user_id": ids::render(ids::USER, user),
                "reason": reason,
            }),
            &who.client,
            Some(org),
        )
        .await
    }

    // ----- invitations -----

    /// `POST /organizations/{org}/invitations`: invite `email` with 1 to 16
    /// roles the caller could grant, mailed through `identity.invitation`.
    /// A new invitation for the same address replaces the old one.
    pub async fn invite(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        email: Email,
        role_ids: Vec<Uuid>,
    ) -> Result<Reply, ApiError> {
        let who = self.in_org(who, org, "identity.members.invite").await?;
        self.charge(
            crate::gcra::INVITATION_ORGANIZATION,
            crate::core::Subject::Id(org),
            &who.client,
        )
        .await?;
        let core = self.clone();
        let outcome: Committed<Value> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let org_row = core.lock_org(tx, org).await?;
                    let scope = Scope::Tenant(org).resource_scope();
                    let mut roles: Vec<RoleRef> = Vec::new();
                    for id in &role_ids {
                        let role = core.tenant_role(tx, org, *id).await?;
                        core.may_grant(tx, &who, org, &role, &scope).await?;
                        if !roles.iter().any(|r| r.id == role.id) {
                            roles.push(role);
                        }
                    }
                    let existing = core.user_by_email(tx, email.normalized(), false).await?;
                    if let Some(user) = &existing
                        && core
                            .db()
                            .query_opt(&*tx, &crate::sql::MEMBERSHIP_OF_USER, &[&org, &user.id])
                            .await
                            .map_err(sql)?
                            .is_some()
                    {
                        return Ok(Err(ApiError::conflict("already_member")));
                    }
                    core.db()
                        .execute(
                            &*tx,
                            &crate::sql::INVITATION_DELETE_FOR_EMAIL,
                            &[&org, &email.normalized()],
                        )
                        .await
                        .map_err(sql)?;
                    let id = core.new_id()?;
                    let token = RawToken::generate(core.ctx.random())?;
                    let digest = token.digest();
                    let d: &[u8] = &digest;
                    let now = core.now();
                    let expires = crate::core::plus(now, core.settings.tokens.invitation_ttl);
                    core.db()
                        .execute(
                            &*tx,
                            &crate::sql::INVITATION_INSERT,
                            &[
                                &id,
                                &org,
                                &email.display(),
                                &email.normalized(),
                                &d,
                                &who.creator(),
                                &now,
                                &expires,
                                &match who.actor {
                                    Actor::ApiKey(key) => Some(key),
                                    _ => None,
                                },
                            ],
                        )
                        .await
                        .map_err(sql)?;
                    for role in &roles {
                        core.db()
                            .execute(
                                &*tx,
                                &crate::sql::INVITATION_ROLE_INSERT,
                                &[&id, &org, &role.builtin(), &role.custom.as_ref().map(|_| role.id)],
                            )
                            .await
                            .map_err(sql)?;
                    }
                    let mut data = Core::data(existing.as_ref(), Some(expires));
                    data.insert("organization_name".into(), Value::String(org_row.get(2)));
                    core.notify(
                        tx,
                        topic::INVITATION,
                        Core::address(email.display(), existing.as_ref())?,
                        data,
                        Some((Core::link(&core.settings.links.invitation, &token), "invitation")),
                        id,
                        Some(expires),
                        &who.client,
                    )
                    .await?;
                    core.metrics.token_issued("invitation");
                    let mut keys: Vec<&str> = roles.iter().map(|r| r.key.as_str()).collect();
                    keys.sort();
                    core.audit(
                        tx,
                        who.audit("invitation.created")
                            .organization(org)
                            .target_of("invitation", id)
                            .detail("role_keys", json!(keys)),
                        &who.client,
                    )
                    .await?;
                    Ok(Ok(json!({"invitation": {
                        "id": ids::render(ids::INVITATION, id),
                        "organization_id": ids::render(ids::ORGANIZATION, org),
                        "email": email.display(),
                        "role_ids": roles.iter().map(|r| ids::render(ids::ROLE, r.id)).collect::<Vec<_>>(),
                        "invited_by": who.creator().map(|u| ids::render(ids::USER, u)),
                        "invited_by_key": match who.actor {
                            Actor::ApiKey(key) => Some(ids::render(ids::API_KEY, key)),
                            _ => None,
                        },
                        "created_at": now.to_string(),
                        "expires_at": expires.to_string(),
                    }})))
                })
            })
            .await?;
        Ok(Reply::json(201, outcome?))
    }

    /// `GET /organizations/{org}/invitations`: pending invitations.
    pub async fn list_invitations(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
    ) -> Result<Reply, ApiError> {
        self.in_org(who, org, "identity.members.invite").await?;
        let client = self.db().acquire(self.deadline()).await?;
        let rows = self
            .db()
            .query(&client, &crate::sql::INVITATIONS_LIST, &[&org, &self.now()])
            .await
            .map_err(sql)?;
        let items: Vec<Value> = rows.iter().map(invitation_json).collect();
        Ok(Reply::json(200, json!({"items": items})))
    }

    /// `DELETE /organizations/{org}/invitations/{invitation}`.
    pub async fn revoke_invitation(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        invitation: Uuid,
    ) -> Result<Reply, ApiError> {
        let who = self.in_org(who, org, "identity.members.invite").await?;
        let core = self.clone();
        let outcome: Committed<()> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_org(tx, org).await?;
                    let deleted = core
                        .db()
                        .execute(&*tx, &crate::sql::INVITATION_DELETE, &[&invitation, &org])
                        .await
                        .map_err(sql)?;
                    if deleted == 0 {
                        return Ok(Err(ApiError::code("not_found")));
                    }
                    core.audit(
                        tx,
                        who.audit("invitation.revoked")
                            .organization(org)
                            .target_of("invitation", invitation),
                        &who.client,
                    )
                    .await?;
                    Ok(Ok(()))
                })
            })
            .await?;
        outcome?;
        Ok(Reply::empty(crate::flows::CookieAction::Keep))
    }

    /// `POST /v1/identity/invitations/inspect`: what an invitation offers,
    /// to its token holder only.
    pub async fn inspect_invitation(
        self: &Arc<Self>,
        client: &Client,
        token: &str,
    ) -> Result<Reply, ApiError> {
        self.peek(
            crate::gcra::TOKEN_IP,
            crate::core::Subject::Ip(client.ip),
            client,
        )
        .await?;
        let result = async {
            let token = RawToken::decode(token).ok_or_else(|| ApiError::code("invalid_token"))?;
            let digest = token.digest();
            let d: &[u8] = &digest;
            let session = self.db().acquire(self.deadline()).await?;
            let row = self
                .db()
                .query_opt(&session, &crate::sql::INVITATION_BY_TOKEN, &[&d])
                .await
                .map_err(sql)?
                .ok_or_else(|| ApiError::code("invalid_token"))?;
            let expires: Timestamp = row.get(6);
            if expires <= self.now() {
                return Err(ApiError::code("invalid_token"));
            }
            let normalized: String = row.get(3);
            let account = self.read_user_by_email(&normalized).await?;
            Ok(Reply::json(
                200,
                json!({
                    "organization": {"name": row.get::<_, String>(9), "slug": row.get::<_, String>(10)},
                    "inviter_display_name": row.get::<_, Option<String>>(11),
                    "email_masked": mask_email(&row.get::<_, String>(2)),
                    "expires_at": expires.to_string(),
                    "account_exists": account.is_some_and(|u| u.verified()),
                }),
            ))
        }
        .await;
        self.token_failed(result, client).await
    }

    /// `POST /v1/identity/invitations/accept` (section 4): a signed-in user
    /// with the invited address joins, its address verified; another
    /// address is `email_mismatch`. Anonymously, an address without an
    /// account, or with only an unverified one (claimed under the
    /// pre-hijacking rule first), gets a verified account and a session;
    /// a verified account must sign in first (`sign_in_required`). The
    /// token is consumed only on success.
    pub async fn accept_invitation(
        self: &Arc<Self>,
        client: &Client,
        caller: Option<&crate::flows::Caller>,
        token: &str,
        transport: Option<crate::core::Transport>,
    ) -> Result<Reply, ApiError> {
        let transport = self.transport(transport)?;
        self.peek(
            crate::gcra::TOKEN_IP,
            crate::core::Subject::Ip(client.ip),
            client,
        )
        .await?;
        let caller = caller.copied();
        let result = async {
            let token = RawToken::decode(token).ok_or_else(|| ApiError::code("invalid_token"))?;
            let core = self.clone();
            let client = client.clone();
            let outcome: Committed<(Value, Option<crate::core::NewSession>, Option<UserRow>)> = self
                .in_tx(move |tx| {
                    Box::pin(async move {
                        let digest = token.digest();
                        let d: &[u8] = &digest;
                        let Some(invitation) = core
                            .db()
                            .query_opt(&*tx, &crate::sql::INVITATION_BY_TOKEN, &[&d])
                            .await
                            .map_err(sql)?
                        else {
                            return Ok(Err(ApiError::code("invalid_token")));
                        };
                        let id: Uuid = invitation.get(0);
                        let org: Uuid = invitation.get(1);
                        let display: String = invitation.get(2);
                        let normalized: String = invitation.get(3);
                        if invitation.get::<_, Timestamp>(6) <= core.now() {
                            return Ok(Err(ApiError::code("invalid_token")));
                        }
                        core.lock_org(tx, org).await?;
                        let roles: Vec<RoleRef> = core
                            .db()
                            .query(&*tx, &crate::sql::INVITATION_ROLES, &[&id])
                            .await
                            .map_err(sql)?
                            .iter()
                            .filter_map(|r| {
                                let builtin: Option<String> = r.get(0);
                                let custom: Option<Uuid> = r.get(1);
                                match (builtin, custom) {
                                    (Some(key), None) => Some(RoleRef {
                                        id: builtin_tenant_id(&key)?,
                                        key,
                                        custom: None,
                                    }),
                                    (None, Some(custom)) => Some(RoleRef {
                                        id: custom,
                                        key: String::new(),
                                        custom: Some(Vec::new()),
                                    }),
                                    _ => None,
                                }
                            })
                            .collect();
                        let mut resolved = Vec::new();
                        for role in roles {
                            resolved.push(core.tenant_role(tx, org, role.id).await?);
                        }
                        // The inviter's authority as it stands now: an
                        // invitation never grants what its inviter could no
                        // longer grant (removed, demoted, key revoked).
                        let invited_by: Option<Uuid> = invitation.get(4);
                        let invited_by_key: Option<Uuid> = invitation.get(8);
                        let scope = Scope::Tenant(org).resource_scope();
                        let inviter = core
                            .live_inviter(tx, org, invited_by, invited_by_key, &client)
                            .await?;
                        let mut still = inviter.is_some();
                        if let Some(inviter) = &inviter {
                            for role in &resolved {
                                if core.may_grant(tx, inviter, org, role, &scope).await.is_err() {
                                    still = false;
                                    break;
                                }
                            }
                        }
                        if !still {
                            core.db()
                                .execute(&*tx, &crate::sql::INVITATION_DELETE, &[&id, &org])
                                .await
                                .map_err(sql)?;
                            core.audit(
                                tx,
                                Audit::new("invitation.revoked", Actor::System)
                                    .organization(org)
                                    .target_of("invitation", id)
                                    .detail("reason", "inviter_lacks_grant"),
                                &client,
                            )
                            .await?;
                            return Ok(Err(ApiError::conflict("invitation_stale")));
                        }
                        let mut user = match caller {
                            Some(caller) => {
                                let Some(user) = core.user_by_id(tx, caller.user, true).await? else {
                                    return Ok(Err(ApiError::code("unauthenticated")));
                                };
                                if user.email_normalized != normalized {
                                    return Ok(Err(ApiError::code("email_mismatch")));
                                }
                                user
                            }
                            None => match core.user_by_email(tx, &normalized, true).await? {
                                Some(user) if user.verified() => {
                                    return Ok(Err(ApiError::code("sign_in_required")));
                                }
                                Some(mut user) => {
                                    if !user.active() {
                                        return Ok(Err(ApiError::code("account_disabled")));
                                    }
                                    // The token proves inbox control: strip
                                    // whatever an attacker attached first.
                                    core.claim_unverified(tx, &mut user, &client).await?;
                                    user
                                }
                                None => {
                                    if core.settings.registration == crate::config::Registration::Closed {
                                        return Ok(Err(ApiError::code("registration_closed")));
                                    }
                                    let Ok(email) = crate::normalize::email(&display) else {
                                        return Ok(Err(ApiError::code("invalid_token")));
                                    };
                                    let Some(id) = core.insert_user(tx, &email, true, None, None).await? else {
                                        return Ok(Err(ApiError::code("invalid_token")));
                                    };
                                    let user = core
                                        .user_by_id(tx, id, true)
                                        .await?
                                        .ok_or_else(|| Failure::transient("internal"))?;
                                    core.audit(
                                        tx,
                                        Audit::new("user.registered", Actor::User(id)).detail("method", "invitation"),
                                        &client,
                                    )
                                    .await?;
                                    core.publish(
                                        tx,
                                        event::USER_REGISTERED,
                                        format!("user:{}:registered", id.simple()),
                                        json!({"user_id": ids::render(ids::USER, id), "method": "invitation",
                                               "registered_at": user.created_at.to_string()}),
                                        &client,
                                    )
                                    .await?;
                                    user
                                }
                            },
                        };
                        if !user.active() {
                            return Ok(Err(ApiError::code("account_disabled")));
                        }
                        if core
                            .db()
                            .query_opt(&*tx, &crate::sql::MEMBERSHIP_OF_USER, &[&org, &user.id])
                            .await
                            .map_err(sql)?
                            .is_some()
                        {
                            return Ok(Err(ApiError::conflict("already_member")));
                        }
                        let members: i64 = core
                            .db()
                            .query_one(&*tx, &crate::sql::MEMBERS_COUNT, &[&org])
                            .await
                            .map_err(sql)?
                            .get(0);
                        if members >= i64::from(core.settings.organizations.max_members) {
                            return Ok(Err(ApiError::code("organization_limit")));
                        }
                        let consumed = core
                            .db()
                            .query_opt(&*tx, &crate::sql::INVITATION_CONSUME, &[&id, &d])
                            .await
                            .map_err(sql)?;
                        if consumed.is_none() {
                            return Ok(Err(ApiError::code("invalid_token")));
                        }
                        core.mark_verified(tx, &mut user, &client).await?;
                        let Some((membership, joined)) = core
                            .add_member(tx, org, user.id, &resolved, "invitation", invited_by, &client)
                            .await?
                        else {
                            return Ok(Err(ApiError::conflict("already_member")));
                        };
                        let mut keys: Vec<&str> = resolved.iter().map(|r| r.key.as_str()).collect();
                        keys.sort();
                        core.audit(
                            tx,
                            Audit::new("invitation.accepted", Actor::User(user.id))
                                .organization(org)
                                .target_of("invitation", id),
                            &client,
                        )
                        .await?;
                        core.audit(
                            tx,
                            Audit::new("membership.created", Actor::User(user.id))
                                .organization(org)
                                .detail("role_keys", json!(keys)),
                            &client,
                        )
                        .await?;
                        let session = match caller {
                            Some(_) => None,
                            None => Some(
                                core.create_session(tx, user.id, Method::Invitation, transport, &client, None, true)
                                    .await?,
                            ),
                        };
                        Ok(Ok((membership_json(membership, org, user.id, joined), session, Some(user))))
                    })
                })
                .await?;
            let (membership, session, user) = outcome?;
            Ok(match session {
                Some(session) => {
                    let mut reply = self.session_reply(201, session, user.as_ref());
                    if let Some(body) = reply.body.as_mut() {
                        body["membership"] = membership;
                    }
                    reply
                }
                None => Reply::json(201, json!({"membership": membership})),
            })
        }
        .await;
        self.token_failed(result, client).await
    }

    // ----- custom roles -----

    /// The JSON of a role row, built-in permissions expanded (`owner` and
    /// `platform_admin` as `*`).
    pub(crate) fn role_json(&self, row: &Row) -> Value {
        let id: Uuid = row.get(0);
        let scope: String = row.get(1);
        let org: Option<Uuid> = row.get(2);
        let key: String = row.get(3);
        let permissions: Vec<String> = match (org, key.as_str()) {
            (Some(_), _) => row.get(6),
            (None, OWNER) | (None, crate::rbac::PLATFORM_ADMIN) => vec!["*".to_owned()],
            (None, _) if scope == "tenant" => sorted(self.rbac.tenant_builtin(&key)),
            (None, _) => self
                .rbac
                .declared()
                .find(|(k, _, _)| *k == key)
                .map(|(_, role, _)| role.grants.clone())
                .unwrap_or_default(),
        };
        json!({
            "id": ids::render(ids::ROLE, id),
            "key": key,
            "name": row.get::<_, String>(4),
            "description": row.get::<_, Option<String>>(5),
            "scope": scope,
            "organization_id": org.map(|o| ids::render(ids::ORGANIZATION, o)),
            "builtin": org.is_none(),
            "permissions": permissions,
            "created_at": row.get::<_, Timestamp>(7).to_string(),
            "updated_at": row.get::<_, Timestamp>(8).to_string(),
        })
    }

    /// `GET /organizations/{org}/roles`: built-in and custom.
    pub async fn list_roles(self: &Arc<Self>, who: &Who, org: Uuid) -> Result<Reply, ApiError> {
        self.in_org(who, org, "identity.roles.read").await?;
        let client = self.db().acquire(self.deadline()).await?;
        let rows = self
            .db()
            .query(&client, &crate::sql::ROLES_OF_ORG, &[&org])
            .await
            .map_err(sql)?;
        let items: Vec<Value> = rows.iter().map(|r| self.role_json(r)).collect();
        Ok(Reply::json(200, json!({"items": items})))
    }

    /// Check a custom role's grant list: 1 to 256 catalog permissions or
    /// application wildcards (`unknown_permission`), all held by the caller
    /// at `Tenant(org)` (`permission_escalation`).
    fn custom_grants(
        &self,
        who: &Who,
        org: Uuid,
        grants: &[String],
    ) -> Result<PermissionSet, ApiError> {
        if grants.is_empty() || grants.len() > 256 {
            return Err(ApiError::invalid(vec![("/permissions", "out_of_range")]));
        }
        for grant in grants {
            self.rbac
                .catalog()
                .check_grant(grant, crate::catalog::Wildcards::Application)
                .map_err(ApiError::code)?;
        }
        let set = self.rbac.custom(grants);
        self.no_escalation(who, &set, &Scope::Tenant(org).resource_scope())?;
        Ok(set)
    }

    /// `POST /organizations/{org}/roles`.
    pub async fn create_role(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        key: String,
        name: String,
        description: Option<String>,
        grants: Vec<String>,
    ) -> Result<Reply, ApiError> {
        let who = self.in_org(who, org, "identity.roles.manage").await?;
        if crate::rbac::RESERVED_ROLE_KEYS.contains(&key.as_str()) {
            return Err(ApiError::conflict("role_key_taken"));
        }
        self.custom_grants(&who, org, &grants)?;
        let core = self.clone();
        let outcome: Committed<Value> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_org(tx, org).await?;
                    let count: i64 = core
                        .db()
                        .query_one(&*tx, &crate::sql::ROLES_COUNT, &[&org])
                        .await
                        .map_err(sql)?
                        .get(0);
                    if count >= i64::from(core.settings.rbac.max_custom_roles) {
                        return Ok(Err(ApiError::code("role_limit")));
                    }
                    let id = core.new_id()?;
                    let inserted = core
                        .db()
                        .query_opt(
                            &*tx,
                            &crate::sql::ROLE_INSERT,
                            &[&id, &org, &key, &name, &description, &grants, &core.now()],
                        )
                        .await
                        .map_err(sql)?;
                    if inserted.is_none() {
                        return Ok(Err(ApiError::conflict("role_key_taken")));
                    }
                    core.audit(
                        tx,
                        who.audit("role.created")
                            .organization(org)
                            .target_of("role", id)
                            .detail("role_keys", json!([key]))
                            .detail("permissions", json!(grants)),
                        &who.client,
                    )
                    .await?;
                    let row = core
                        .db()
                        .query_one(&*tx, &crate::sql::ROLE_IN_ORG, &[&id, &org])
                        .await
                        .map_err(sql)?;
                    Ok(Ok(json!({"role": core.role_json(&row)})))
                })
            })
            .await?;
        Ok(Reply::json(201, outcome?))
    }

    /// `PATCH /organizations/{org}/roles/{role}`: a custom role only
    /// (`builtin_role`). The caller must hold every permission the role had
    /// and will have; the organization's version is bumped so its holders'
    /// snapshots rebuild on their next request.
    #[allow(clippy::too_many_arguments)]
    pub async fn update_role(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        role: Uuid,
        name: Option<String>,
        description: Option<Option<String>>,
        grants: Option<Vec<String>>,
    ) -> Result<Reply, ApiError> {
        let who = self.in_org(who, org, "identity.roles.manage").await?;
        let core = self.clone();
        let outcome: Committed<Value> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_org(tx, org).await?;
                    // The role first: one of another organization is
                    // `not_found` whatever the body asks.
                    let current = core.tenant_role(tx, org, role).await?;
                    if current.custom.is_none() {
                        return Ok(Err(ApiError::code("builtin_role")));
                    }
                    if let Some(grants) = &grants {
                        core.custom_grants(&who, org, grants)?;
                    }
                    core.no_escalation(
                        &who,
                        &core.role_permissions(&current),
                        &Scope::Tenant(org).resource_scope(),
                    )?;
                    let now = core.now();
                    core.db()
                        .execute(
                            &*tx,
                            &crate::sql::ROLE_UPDATE,
                            &[
                                &role,
                                &org,
                                &name,
                                &description.is_some(),
                                &description.clone().flatten(),
                                &grants,
                                &now,
                            ],
                        )
                        .await
                        .map_err(sql)?;
                    core.db()
                        .execute(&*tx, &crate::sql::ORG_BUMP, &[&org, &now])
                        .await
                        .map_err(sql)?;
                    let row = core
                        .db()
                        .query_one(&*tx, &crate::sql::ROLE_IN_ORG, &[&role, &org])
                        .await
                        .map_err(sql)?;
                    let stored: Vec<String> = row.get(6);
                    core.audit(
                        tx,
                        who.audit("role.updated")
                            .organization(org)
                            .target_of("role", role)
                            .detail("permissions", json!(stored)),
                        &who.client,
                    )
                    .await?;
                    Ok(Ok(json!({"role": core.role_json(&row)})))
                })
            })
            .await?;
        Ok(Reply::json(200, outcome?))
    }

    /// `DELETE /organizations/{org}/roles/{role}`: refused `role_in_use`
    /// while an assignment or a pending invitation uses it.
    pub async fn delete_role(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        role: Uuid,
    ) -> Result<Reply, ApiError> {
        let who = self.in_org(who, org, "identity.roles.manage").await?;
        let core = self.clone();
        let outcome: Committed<()> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_org(tx, org).await?;
                    let current = core.tenant_role(tx, org, role).await?;
                    if current.custom.is_none() {
                        return Ok(Err(ApiError::code("builtin_role")));
                    }
                    core.no_escalation(
                        &who,
                        &core.role_permissions(&current),
                        &Scope::Tenant(org).resource_scope(),
                    )?;
                    let uses: i64 = core
                        .db()
                        .query_one(&*tx, &crate::sql::ROLE_USES, &[&role])
                        .await
                        .map_err(sql)?
                        .get(0);
                    if uses > 0 {
                        return Ok(Err(ApiError::conflict("role_in_use")));
                    }
                    core.db()
                        .execute(&*tx, &crate::sql::ROLE_DELETE, &[&role, &org])
                        .await
                        .map_err(|e| {
                            let failure = sql(e);
                            if failure.code == "constraint_violation" {
                                Failure::conflict("role_in_use")
                            } else {
                                failure
                            }
                        })?;
                    core.db()
                        .execute(&*tx, &crate::sql::ORG_BUMP, &[&org, &core.now()])
                        .await
                        .map_err(sql)?;
                    core.audit(
                        tx,
                        who.audit("role.deleted")
                            .organization(org)
                            .target_of("role", role)
                            .detail("role_keys", json!([current.key])),
                        &who.client,
                    )
                    .await?;
                    Ok(Ok(()))
                })
            })
            .await?;
        outcome?;
        Ok(Reply::empty(crate::flows::CookieAction::Keep))
    }

    // ----- tenant role assignments -----

    /// Whether `kind` is a resource kind some route or block declares.
    pub(crate) fn known_kind(&self, kind: &str) -> Result<(), ApiError> {
        if self.resource_kinds.contains(kind) {
            Ok(())
        } else {
            Err(ApiError::code("unknown_resource_kind"))
        }
    }

    /// `GET /organizations/{org}/role-assignments`.
    #[allow(clippy::too_many_arguments)]
    pub async fn list_assignments(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        membership: Option<Uuid>,
        service_account: Option<Uuid>,
        resource_kind: Option<String>,
        resource_id: Option<String>,
        cursor: Option<Uuid>,
        limit: i64,
    ) -> Result<Reply, ApiError> {
        self.in_org(who, org, "identity.roles.read").await?;
        let client = self.db().acquire(self.deadline()).await?;
        let rows = self
            .db()
            .query(
                &client,
                &crate::sql::ASSIGNMENTS_LIST,
                &[
                    &org,
                    &membership,
                    &service_account,
                    &resource_kind,
                    &resource_id,
                    &cursor,
                    &(limit + 1),
                ],
            )
            .await
            .map_err(sql)?;
        let (items, next) = page(
            &rows,
            limit,
            |r| assignment_json(r, org),
            |r| ids::render(ids::ASSIGNMENT, r.get(0)),
        );
        Ok(Reply::json(
            200,
            json!({"items": items, "next_cursor": next}),
        ))
    }

    /// `POST /organizations/{org}/role-assignments`: assign a built-in or
    /// custom role of `org` to a member or a tenant service account of
    /// `org`, at the organization or one resource in it.
    pub async fn assign_tenant(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        input: AssignmentInput,
    ) -> Result<Reply, ApiError> {
        let who = self.in_org(who, org, "identity.roles.assign").await?;
        if let Some((kind, _)) = &input.resource {
            self.known_kind(kind.as_str())?;
        }
        let core = self.clone();
        let outcome: Committed<Value> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_org(tx, org).await?;
                    let role = core.tenant_role(tx, org, input.role).await?;
                    let user = match (input.membership, input.service_account) {
                        (Some(membership), None) => {
                            let Some(row) = core
                                .db()
                                .query_opt(&*tx, &crate::sql::MEMBERSHIP_GET, &[&membership, &org])
                                .await
                                .map_err(sql)?
                            else {
                                return Ok(Err(ApiError::code("not_found")));
                            };
                            Some(row.get::<_, Uuid>(2))
                        }
                        (None, Some(service)) => {
                            if core
                                .db()
                                .query_opt(&*tx, &crate::sql::SERVICE_LOCK, &[&service, &Some(org)])
                                .await
                                .map_err(sql)?
                                .is_none()
                            {
                                return Ok(Err(ApiError::code("not_found")));
                            }
                            None
                        }
                        _ => {
                            return Ok(Err(ApiError::invalid(vec![(
                                "/membership_id",
                                "required",
                            )])));
                        }
                    };
                    let scope = match &input.resource {
                        None => Scope::Tenant(org).resource_scope(),
                        Some((kind, id)) => {
                            if role.builtin() == Some(OWNER) {
                                return Ok(Err(ApiError::invalid(vec![("/resource", "invalid")])));
                            }
                            Scope::Tenant(org).resource(kind, id)
                        }
                    };
                    core.may_grant(tx, &who, org, &role, &scope).await?;
                    let held: i64 = core
                        .db()
                        .query_one(
                            &*tx,
                            &crate::sql::ASSIGNMENTS_COUNT_HOLDER,
                            &[&org, &input.membership, &input.service_account],
                        )
                        .await
                        .map_err(sql)?
                        .get(0);
                    let cap = i64::from(core.settings.rbac.max_custom_roles)
                        + 3
                        + i64::from(core.settings.rbac.max_resource_assignments);
                    if held >= cap {
                        return Ok(Err(ApiError::code("assignment_limit")));
                    }
                    let resource = input
                        .resource
                        .as_ref()
                        .map(|(k, i)| (k.as_str(), i.as_str()));
                    let Some(id) = core
                        .insert_assignment(
                            tx,
                            org,
                            input.membership,
                            input.service_account,
                            &role,
                            resource,
                            who.creator(),
                        )
                        .await?
                    else {
                        return Ok(Err(ApiError::conflict("conflict")));
                    };
                    match (user, input.membership) {
                        (Some(user), Some(membership)) => {
                            core.bump_user(tx, user).await?;
                            if resource.is_none() {
                                core.publish_roles_changed(tx, org, membership, user, &who.client)
                                    .await?;
                            }
                        }
                        _ => {
                            if let Some(service) = input.service_account {
                                core.db()
                                    .execute(&*tx, &crate::sql::SERVICE_BUMP, &[&service])
                                    .await
                                    .map_err(sql)?;
                            }
                        }
                    }
                    let mut audit = who
                        .audit("role_assignment.created")
                        .organization(org)
                        .target_of("role_assignment", id)
                        .detail(
                            "scope",
                            if resource.is_some() {
                                "resource"
                            } else {
                                "tenant"
                            },
                        )
                        .detail("role_keys", json!([role.key]));
                    if let Some((kind, _)) = resource {
                        audit = audit.detail("resource_kind", kind);
                    }
                    core.audit(tx, audit, &who.client).await?;
                    let row = core
                        .db()
                        .query_one(&*tx, &crate::sql::ASSIGNMENT_GET, &[&id, &org])
                        .await
                        .map_err(sql)?;
                    Ok(Ok(json!({"assignment": assignment_json(&row, org)})))
                })
            })
            .await?;
        Ok(Reply::json(201, outcome?))
    }

    /// `DELETE /organizations/{org}/role-assignments/{assignment}`.
    pub async fn unassign_tenant(
        self: &Arc<Self>,
        who: &Who,
        org: Uuid,
        assignment: Uuid,
    ) -> Result<Reply, ApiError> {
        let who = self.in_org(who, org, "identity.roles.assign").await?;
        let core = self.clone();
        let outcome: Committed<()> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_org(tx, org).await?;
                    let Some(row) = core
                        .db()
                        .query_opt(&*tx, &crate::sql::ASSIGNMENT_GET, &[&assignment, &org])
                        .await
                        .map_err(sql)?
                    else {
                        return Ok(Err(ApiError::code("not_found")));
                    };
                    let membership: Option<Uuid> = row.get(1);
                    let service: Option<Uuid> = row.get(2);
                    let builtin: Option<String> = row.get(3);
                    let custom: Option<Uuid> = row.get(4);
                    let role_id = custom.or_else(|| builtin.as_deref().and_then(builtin_tenant_id));
                    let Some(role_id) = role_id else {
                        return Ok(Err(ApiError::code("not_found")));
                    };
                    let role = core.tenant_role(tx, org, role_id).await?;
                    let kind: Option<String> = row.get(6);
                    let rid: Option<String> = row.get(7);
                    let scope = match (&kind, &rid) {
                        (Some(kind), Some(rid)) => {
                            match (Identity::new(kind.clone()), Identity::new(rid.clone())) {
                                (Ok(kind), Ok(rid)) => Scope::Tenant(org).resource(&kind, &rid),
                                _ => Scope::Tenant(org).resource_scope(),
                            }
                        }
                        _ => Scope::Tenant(org).resource_scope(),
                    };
                    core.may_grant(tx, &who, org, &role, &scope).await?;
                    let user: Option<Uuid> = row.get(10);
                    if role.builtin() == Some(OWNER)
                        && kind.is_none()
                        && let Some(user) = user
                    {
                        core.keep_an_owner(tx, org, user).await?;
                    }
                    core.db()
                        .execute(&*tx, &crate::sql::ASSIGNMENT_DELETE, &[&assignment, &org])
                        .await
                        .map_err(sql)?;
                    if let (Some(user), Some(membership)) = (user, membership) {
                        core.bump_user(tx, user).await?;
                        core.after_authority_change(tx, org, user, false, &who)
                            .await?;
                        if kind.is_none() {
                            core.publish_roles_changed(tx, org, membership, user, &who.client)
                                .await?;
                        }
                    }
                    if let Some(service) = service {
                        core.db()
                            .execute(&*tx, &crate::sql::SERVICE_BUMP, &[&service])
                            .await
                            .map_err(sql)?;
                    }
                    core.audit(
                        tx,
                        who.audit("role_assignment.deleted")
                            .organization(org)
                            .target_of("role_assignment", assignment)
                            .detail("scope", if kind.is_some() { "resource" } else { "tenant" })
                            .detail("role_keys", json!([role.key])),
                        &who.client,
                    )
                    .await?;
                    Ok(Ok(()))
                })
            })
            .await?;
        outcome?;
        Ok(Reply::empty(crate::flows::CookieAction::Keep))
    }

    /// Refuse `DELETE /me` with `sole_owner` while the user is the only
    /// owner of an organization; the organizations it owns are locked in
    /// id order first, so two owners leaving together keep one. Publishes
    /// `identity.membership_removed` (reason `user_deleted`) for each of
    /// its memberships.
    pub(crate) async fn before_user_deleted(
        &self,
        tx: &mut Tx<'_>,
        user: Uuid,
        client: &Client,
    ) -> Result<(), ApiError> {
        let owned: Vec<Uuid> = self
            .db()
            .query(&*tx, &crate::sql::ORGS_OWNED_LOCK, &[&user])
            .await
            .map_err(sql)?
            .iter()
            .map(|r| r.get(0))
            .collect();
        for org in owned {
            self.keep_an_owner(tx, org, user).await?;
        }
        let memberships = self
            .db()
            .query(&*tx, &crate::sql::MEMBERSHIPS_OF_USER, &[&user])
            .await
            .map_err(sql)?;
        for row in &memberships {
            let membership: Uuid = row.get(0);
            let org: Uuid = row.get(1);
            self.publish_in(
                tx,
                event::MEMBERSHIP_REMOVED,
                format!("membership:{}:removed", membership.simple()),
                json!({
                    "membership_id": ids::render(ids::MEMBERSHIP, membership),
                    "organization_id": ids::render(ids::ORGANIZATION, org),
                    "user_id": ids::render(ids::USER, user),
                    "reason": "user_deleted",
                }),
                client,
                Some(org),
            )
            .await?;
        }
        Ok(())
    }
}

/// Sorted names of a permission set.
pub(crate) fn sorted(set: Option<&PermissionSet>) -> Vec<String> {
    let mut names: Vec<String> = set
        .map(|s| s.iter().map(|p| p.as_str().to_owned()).collect())
        .unwrap_or_default();
    names.sort();
    names
}

/// The first `limit` rows as items, and the cursor of the last one when
/// more rows exist.
pub(crate) fn page(
    rows: &[Row],
    limit: i64,
    item: impl Fn(&Row) -> Value,
    cursor: impl Fn(&Row) -> String,
) -> (Vec<Value>, Option<String>) {
    let limit = usize::try_from(limit).unwrap_or(0);
    let items: Vec<Value> = rows.iter().take(limit).map(item).collect();
    let next = (rows.len() > limit)
        .then(|| rows.get(limit.saturating_sub(1)).map(cursor))
        .flatten();
    (items, next)
}
