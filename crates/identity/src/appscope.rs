//! The application-scope use cases of section 4 (PR 2), checked at
//! `Application`: users and their status, application roles and role
//! assignments, the caller's application permissions, and the
//! `platform_admin` grant of the CLI. An application without organizations
//! administers users and grants here.
use crate::{
    access::{Scope, Who},
    core::{Actor, ApiError, Audit, Client, Core, event, sql},
    flows::{Committed, CookieAction, Reply},
    grants::Owner,
    ids,
    rbac::{PLATFORM_ADMIN, PLATFORM_ADMIN_ID, USER},
    tenancy::{page, resource_json, sorted},
};
use jiff::Timestamp;
use merezha_domain::{Failure, Identity};
use merezha_platform::{Tx, ports::ResourceScope};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::Row;
use uuid::Uuid;

fn app_assignment_json(row: &Row) -> Value {
    json!({
        "id": ids::render(ids::ASSIGNMENT, row.get(0)),
        "scope": "application",
        "user_id": row.get::<_, Option<Uuid>>(1).map(|u| ids::render(ids::USER, u)),
        "service_account_id": row.get::<_, Option<Uuid>>(2).map(|s| ids::render(ids::SERVICE_ACCOUNT, s)),
        "role_id": ids::render(ids::ROLE, row.get(3)),
        "role_key": row.get::<_, String>(4),
        "resource": resource_json(row.get(5), row.get(6)),
        "created_at": row.get::<_, Timestamp>(8).to_string(),
    })
}

/// What the platform-admin CLI did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdminChange {
    Granted,
    Revoked,
    Unchanged,
}
impl AdminChange {
    /// `granted`, `revoked` or `unchanged`, the CLI's `action`.
    pub fn as_str(self) -> &'static str {
        match self {
            AdminChange::Granted => "granted",
            AdminChange::Revoked => "revoked",
            AdminChange::Unchanged => "unchanged",
        }
    }
}

impl Core {
    /// `GET /v1/identity/me/permissions`: the caller's application roles
    /// and the permissions it may exercise at `Application`.
    pub async fn application_permissions(self: &Arc<Self>, who: &Who) -> Result<Reply, ApiError> {
        if who.user.is_none() {
            return Err(ApiError::code("unauthorized"));
        }
        let roles: Vec<String> = who
            .principal
            .grants
            .application()
            .roles()
            .iter()
            .map(|r| r.as_str().to_owned())
            .collect();
        Ok(Reply::json(
            200,
            json!({"application": {
                "roles": roles,
                "permissions": self.effective(who, &ResourceScope::Application),
                "break_glass": who.principal.grants.is_break_glass(),
            }}),
        ))
    }

    /// `GET /v1/identity/permissions`: the catalog.
    pub fn permission_catalog(&self, who: &Who) -> Result<Reply, ApiError> {
        if who.user.is_none() {
            return Err(ApiError::code("unauthorized"));
        }
        let items: Vec<Value> = self
            .rbac
            .catalog()
            .entries()
            .map(|(name, entry)| {
                json!({"name": name, "description": entry.description, "source": entry.source})
            })
            .collect();
        Ok(Reply::json(200, json!({"items": items})))
    }

    /// `GET /application/users`.
    pub async fn list_users(
        self: &Arc<Self>,
        who: &Who,
        email: Option<String>,
        cursor: Option<Uuid>,
        limit: i64,
    ) -> Result<Reply, ApiError> {
        self.in_application(who, "identity.users.read")?;
        let client = self.db().acquire(self.deadline()).await?;
        let rows = self
            .db()
            .query(
                &client,
                &crate::sql::USERS_LIST,
                &[&email, &cursor, &(limit + 1)],
            )
            .await
            .map_err(sql)?;
        let (items, next) = page(
            &rows,
            limit,
            |r| {
                let user = crate::core::user_row(r);
                let mut json = user.json();
                json["status"] = Value::String(user.status.clone());
                json
            },
            |r| ids::render(ids::USER, r.get(0)),
        );
        Ok(Reply::json(
            200,
            json!({"items": items, "next_cursor": next}),
        ))
    }

    /// `PATCH /application/users/{user}` (recent): enable or disable a
    /// user. The caller must hold every application permission of the
    /// target (break-glass for a `platform_admin`). Disabling revokes every
    /// session and token; keys stop on the next request.
    pub async fn set_user_status(
        self: &Arc<Self>,
        who: &Who,
        target: Uuid,
        status: String,
    ) -> Result<Reply, ApiError> {
        self.in_application(who, "identity.users.manage")?;
        self.recent(who)?;
        let session = self.db().acquire(self.deadline()).await?;
        let version: Option<i64> = self
            .db()
            .query_opt(
                &session,
                &crate::sql::USER_IN_ORGANIZATION,
                &[&target, &None::<Uuid>],
            )
            .await
            .map_err(sql)?
            .map(|r| r.get(0));
        let Some(version) = version else {
            return Err(ApiError::code("not_found"));
        };
        let theirs = self
            .snapshot(&session, target, Owner::User(target), version, None, None)
            .await?;
        drop(session);
        if theirs.is_break_glass() && !who.principal.grants.is_break_glass() {
            return Err(ApiError::code("permission_escalation"));
        }
        self.no_escalation(
            who,
            theirs.application().permissions(),
            &ResourceScope::Application,
        )?;
        let core = self.clone();
        let who = who.clone();
        let outcome: Committed<Value> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let Some(_) = core.user_by_id(tx, target, true).await? else {
                        return Ok(Err(ApiError::code("not_found")));
                    };
                    let now = core.now();
                    let changed = core
                        .db()
                        .execute(
                            &*tx,
                            &crate::sql::USER_SET_STATUS,
                            &[&target, &status, &now],
                        )
                        .await
                        .map_err(sql)?;
                    if changed > 0 {
                        let disabled = status == "disabled";
                        if disabled {
                            let revoked = core.delete_sessions(tx, target, None).await?;
                            core.metrics.sessions_revoked("admin", revoked);
                            core.delete_tokens(tx, target).await?;
                        }
                        core.audit(
                            tx,
                            who.audit(if disabled {
                                "user.disabled"
                            } else {
                                "user.enabled"
                            })
                            .target(target),
                            &who.client,
                        )
                        .await?;
                        let (topic, at) = if disabled {
                            (event::USER_DISABLED, "disabled_at")
                        } else {
                            (event::USER_ENABLED, "enabled_at")
                        };
                        core.publish(
                            tx,
                            topic,
                            format!(
                                "user:{}:{status}:{}",
                                target.simple(),
                                core.new_id()?.simple()
                            ),
                            json!({"user_id": ids::render(ids::USER, target), at: now.to_string()}),
                            &who.client,
                        )
                        .await?;
                    }
                    let user = core
                        .user_by_id(tx, target, false)
                        .await?
                        .ok_or_else(|| Failure::transient("internal"))?;
                    let mut json = user.json();
                    json["status"] = Value::String(user.status.clone());
                    Ok(Ok(json!({"user": json})))
                })
            })
            .await?;
        Ok(Reply::json(200, outcome?))
    }

    /// `GET /application/roles`: `platform_admin`, the implicit `user` and
    /// the declared roles.
    pub async fn list_application_roles(self: &Arc<Self>, who: &Who) -> Result<Reply, ApiError> {
        self.in_application(who, "identity.roles.read")?;
        let client = self.db().acquire(self.deadline()).await?;
        let rows = self
            .db()
            .query(&client, &crate::sql::ROLES_OF_APPLICATION, &[])
            .await
            .map_err(sql)?;
        let mut items: Vec<Value> = rows.iter().map(|r| self.role_json(r)).collect();
        items.push(json!({
            "id": Value::Null,
            "key": USER,
            "name": "User",
            "description": "Held by every active user",
            "scope": "application",
            "organization_id": Value::Null,
            "builtin": true,
            "permissions": sorted(Some(&self.rbac.application_role(USER))),
        }));
        Ok(Reply::json(200, json!({"items": items})))
    }

    /// `GET /application/role-assignments`.
    #[allow(clippy::too_many_arguments)]
    pub async fn list_application_assignments(
        self: &Arc<Self>,
        who: &Who,
        user: Option<Uuid>,
        service_account: Option<Uuid>,
        role: Option<Uuid>,
        cursor: Option<Uuid>,
        limit: i64,
    ) -> Result<Reply, ApiError> {
        self.in_application(who, "identity.roles.read")?;
        let client = self.db().acquire(self.deadline()).await?;
        let rows = self
            .db()
            .query(
                &client,
                &crate::sql::APP_ASSIGNMENTS_LIST,
                &[&user, &service_account, &role, &cursor, &(limit + 1)],
            )
            .await
            .map_err(sql)?;
        let (items, next) = page(&rows, limit, app_assignment_json, |r| {
            ids::render(ids::ASSIGNMENT, r.get(0))
        });
        Ok(Reply::json(
            200,
            json!({"items": items, "next_cursor": next}),
        ))
    }

    async fn publish_application_roles(
        &self,
        tx: &mut Tx<'_>,
        user: Uuid,
        client: &Client,
    ) -> Result<(), Failure> {
        let keys: Vec<String> = self
            .db()
            .query(&*tx, &crate::sql::APP_ROLE_KEYS_OF_USER, &[&user])
            .await
            .map_err(sql)?
            .iter()
            .map(|r| r.get(0))
            .collect();
        self.publish(
            tx,
            event::USER_APPLICATION_ROLES_CHANGED,
            format!(
                "user:{}:application_roles:{}",
                user.simple(),
                self.new_id()?.simple()
            ),
            json!({"user_id": ids::render(ids::USER, user), "role_keys": keys}),
            client,
        )
        .await
    }

    /// The scope of an application assignment's resource, if any.
    fn application_target(resource: Option<&(Identity, Identity)>) -> ResourceScope {
        match resource {
            None => ResourceScope::Application,
            Some((kind, id)) => Scope::Application.resource(kind, id),
        }
    }

    /// `POST /application/role-assignments` (recent): a declared role to a
    /// user or an application service account, at the application or one
    /// application-level resource. `platform_admin` and `user` are never
    /// assigned over HTTP (`builtin_role`).
    pub async fn assign_application(
        self: &Arc<Self>,
        who: &Who,
        input: crate::tenancy::AssignmentInput,
    ) -> Result<Reply, ApiError> {
        self.in_application(who, "identity.roles.assign")?;
        self.recent(who)?;
        if input.role == PLATFORM_ADMIN_ID {
            return Err(ApiError::code("builtin_role"));
        }
        if let Some((kind, _)) = &input.resource {
            self.known_kind(kind.as_str())?;
        }
        let core = self.clone();
        let who = who.clone();
        let outcome: Committed<Value> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let Some(role) = core
                        .db()
                        .query_opt(&*tx, &crate::sql::ROLE_APPLICATION, &[&input.role])
                        .await
                        .map_err(sql)?
                    else {
                        return Ok(Err(ApiError::code("not_found")));
                    };
                    let key: String = role.get(3);
                    if key == PLATFORM_ADMIN {
                        return Ok(Err(ApiError::code("builtin_role")));
                    }
                    let scope = Core::application_target(input.resource.as_ref());
                    core.no_escalation(&who, &core.rbac.application_role(&key), &scope)?;
                    match (input.user, input.service_account) {
                        (Some(user), None) => {
                            if core.user_by_id(tx, user, true).await?.is_none() {
                                return Ok(Err(ApiError::code("not_found")));
                            }
                        }
                        (None, Some(service)) => {
                            if core
                                .db()
                                .query_opt(
                                    &*tx,
                                    &crate::sql::SERVICE_LOCK,
                                    &[&service, &None::<Uuid>],
                                )
                                .await
                                .map_err(sql)?
                                .is_none()
                            {
                                return Ok(Err(ApiError::code("not_found")));
                            }
                        }
                        _ => return Ok(Err(ApiError::invalid(vec![("/user_id", "required")]))),
                    }
                    let held: i64 = core
                        .db()
                        .query_one(
                            &*tx,
                            &crate::sql::APP_ASSIGNMENTS_COUNT_HOLDER,
                            &[&input.user, &input.service_account],
                        )
                        .await
                        .map_err(sql)?
                        .get(0);
                    let cap = core.rbac.declared().count() as i64
                        + i64::from(core.settings.rbac.max_resource_assignments);
                    if held >= cap {
                        return Ok(Err(ApiError::code("assignment_limit")));
                    }
                    let id = core.new_id()?;
                    let inserted = core
                        .db()
                        .query_opt(
                            &*tx,
                            &crate::sql::APP_ASSIGNMENT_INSERT,
                            &[
                                &id,
                                &input.user,
                                &input.service_account,
                                &input.role,
                                &input.resource.as_ref().map(|r| r.0.as_str()),
                                &input.resource.as_ref().map(|r| r.1.as_str()),
                                &who.creator(),
                                &core.now(),
                            ],
                        )
                        .await
                        .map_err(sql)?;
                    if inserted.is_none() {
                        return Ok(Err(ApiError::conflict("conflict")));
                    }
                    core.after_application_change(
                        tx,
                        input.user,
                        input.service_account,
                        &who.client,
                    )
                    .await?;
                    let mut audit = who
                        .audit("role_assignment.created")
                        .target_of("role_assignment", id)
                        .detail(
                            "scope",
                            if input.resource.is_some() {
                                "resource"
                            } else {
                                "application"
                            },
                        )
                        .detail("role_keys", json!([key]));
                    if let Some((kind, _)) = &input.resource {
                        audit = audit.detail("resource_kind", kind.as_str());
                    }
                    core.audit(tx, audit, &who.client).await?;
                    let row = core
                        .db()
                        .query_one(&*tx, &crate::sql::APP_ASSIGNMENT_GET, &[&id])
                        .await
                        .map_err(sql)?;
                    Ok(Ok(json!({"assignment": app_assignment_json(&row)})))
                })
            })
            .await?;
        Ok(Reply::json(201, outcome?))
    }

    async fn after_application_change(
        &self,
        tx: &mut Tx<'_>,
        user: Option<Uuid>,
        service: Option<Uuid>,
        client: &Client,
    ) -> Result<(), Failure> {
        if let Some(user) = user {
            self.bump_user(tx, user).await?;
            self.publish_application_roles(tx, user, client).await?;
        }
        if let Some(service) = service {
            self.db()
                .execute(&*tx, &crate::sql::SERVICE_BUMP, &[&service])
                .await
                .map_err(sql)?;
        }
        Ok(())
    }

    /// `DELETE /application/role-assignments/{id}` (recent).
    pub async fn unassign_application(
        self: &Arc<Self>,
        who: &Who,
        assignment: Uuid,
    ) -> Result<Reply, ApiError> {
        self.in_application(who, "identity.roles.assign")?;
        self.recent(who)?;
        let core = self.clone();
        let who = who.clone();
        let outcome: Committed<()> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let Some(row) = core
                        .db()
                        .query_opt(&*tx, &crate::sql::APP_ASSIGNMENT_GET, &[&assignment])
                        .await
                        .map_err(sql)?
                    else {
                        return Ok(Err(ApiError::code("not_found")));
                    };
                    let key: String = row.get(4);
                    if key == PLATFORM_ADMIN {
                        return Ok(Err(ApiError::code("builtin_role")));
                    }
                    let kind: Option<String> = row.get(5);
                    let rid: Option<String> = row.get(6);
                    let resource = match (kind, rid) {
                        (Some(k), Some(i)) => Identity::new(k).ok().zip(Identity::new(i).ok()),
                        _ => None,
                    };
                    let scope = Core::application_target(resource.as_ref());
                    core.no_escalation(&who, &core.rbac.application_role(&key), &scope)?;
                    core.db()
                        .execute(&*tx, &crate::sql::APP_ASSIGNMENT_DELETE, &[&assignment])
                        .await
                        .map_err(sql)?;
                    let user: Option<Uuid> = row.get(1);
                    let service: Option<Uuid> = row.get(2);
                    if let Some(user) = user {
                        core.user_by_id(tx, user, true).await?;
                    }
                    core.after_application_change(tx, user, service, &who.client)
                        .await?;
                    core.audit(
                        tx,
                        who.audit("role_assignment.deleted")
                            .target_of("role_assignment", assignment)
                            .detail(
                                "scope",
                                if resource.is_some() {
                                    "resource"
                                } else {
                                    "application"
                                },
                            )
                            .detail("role_keys", json!([key])),
                        &who.client,
                    )
                    .await?;
                    Ok(Ok(()))
                })
            })
            .await?;
        outcome?;
        Ok(Reply::empty(CookieAction::Keep))
    }

    /// `merezha identity platform-admin grant|revoke <email>`: the only way
    /// to grant `platform_admin` (section 5), audited as
    /// `platform_admin.granted` / `platform_admin.revoked` by the system
    /// principal `sys_cli`. Granting needs a verified, active account.
    pub async fn platform_admin(
        self: &Arc<Self>,
        email: &crate::normalize::Email,
        grant: bool,
    ) -> Result<(AdminChange, Uuid), ApiError> {
        let core = self.clone();
        let email = email.clone();
        let outcome: Committed<(AdminChange, Uuid)> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    let client = Client::default();
                    let Some(user) = core.user_by_email(tx, email.normalized(), true).await? else {
                        return Ok(Err(ApiError::code("not_found")));
                    };
                    let changed = if grant {
                        if !user.verified() || !user.active() {
                            return Ok(Err(ApiError::code("email_unverified")));
                        }
                        let id = core.new_id()?;
                        core.db()
                            .query_opt(
                                &*tx,
                                &crate::sql::PLATFORM_ADMIN_GRANT,
                                &[&id, &user.id, &core.now()],
                            )
                            .await
                            .map_err(sql)?
                            .is_some()
                    } else {
                        core.db()
                            .query_opt(&*tx, &crate::sql::PLATFORM_ADMIN_REVOKE, &[&user.id])
                            .await
                            .map_err(sql)?
                            .is_some()
                    };
                    if !changed {
                        return Ok(Ok((AdminChange::Unchanged, user.id)));
                    }
                    core.after_application_change(tx, Some(user.id), None, &client)
                        .await?;
                    core.audit(
                        tx,
                        Audit::new(
                            if grant {
                                "platform_admin.granted"
                            } else {
                                "platform_admin.revoked"
                            },
                            Actor::System,
                        )
                        .target(user.id)
                        .detail("reason", "cli"),
                        &client,
                    )
                    .await?;
                    Ok(Ok((
                        if grant {
                            AdminChange::Granted
                        } else {
                            AdminChange::Revoked
                        },
                        user.id,
                    )))
                })
            })
            .await?;
        outcome
    }
}
