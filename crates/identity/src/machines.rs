//! Service accounts, API keys and audit listings (section 4), at
//! `Tenant(org)` under `/organizations/{org}` and at `Application` under
//! `/application`, with the same bodies and errors.
use crate::{
    access::{Scope, Who},
    apikey,
    core::{ApiError, Core, plus, sql},
    flows::{Committed, CookieAction, Reply},
    ids,
    tenancy::page,
};
use jiff::Timestamp;
use merezha_domain::Failure;
use secrecy::ExposeSecret;
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_postgres::Row;
use uuid::Uuid;

fn service_json(row: &Row) -> Value {
    json!({
        "id": ids::render(ids::SERVICE_ACCOUNT, row.get(0)),
        "scope": row.get::<_, String>(1),
        "organization_id": row.get::<_, Option<Uuid>>(2).map(|o| ids::render(ids::ORGANIZATION, o)),
        "name": row.get::<_, String>(3),
        "description": row.get::<_, Option<String>>(4),
        "status": row.get::<_, String>(5),
        "created_at": row.get::<_, Timestamp>(7).to_string(),
        "updated_at": row.get::<_, Timestamp>(8).to_string(),
    })
}
fn key_json(row: &Row) -> Value {
    json!({
        "id": apikey::render(row.get(0)),
        "organization_id": row.get::<_, Option<Uuid>>(1).map(|o| ids::render(ids::ORGANIZATION, o)),
        "user_id": row.get::<_, Option<Uuid>>(2).map(|u| ids::render(ids::USER, u)),
        "service_account_id": row.get::<_, Option<Uuid>>(3).map(|s| ids::render(ids::SERVICE_ACCOUNT, s)),
        "name": row.get::<_, String>(4),
        "permissions": row.get::<_, Vec<String>>(5),
        "created_by": row.get::<_, Option<Uuid>>(6).map(|u| ids::render(ids::USER, u)),
        "created_at": row.get::<_, Timestamp>(7).to_string(),
        "expires_at": row.get::<_, Timestamp>(8).to_string(),
        "last_used_at": row.get::<_, Option<Timestamp>>(9).map(|t| t.to_string()),
        "revoked_at": row.get::<_, Option<Timestamp>>(10).map(|t| t.to_string()),
    })
}
/// An audit row as the audit routes show it: ids rendered by kind, no IP
/// or user agent (those stay for the account owner's own listing).
pub(crate) fn audit_json(row: &Row) -> Value {
    let render = |kind: Option<&str>, id: Option<Uuid>| -> Value {
        let Some(id) = id else {
            return Value::Null;
        };
        let prefix = match kind {
            Some("user") => ids::USER,
            Some("api_key") => ids::API_KEY,
            Some("organization") => ids::ORGANIZATION,
            Some("membership") => ids::MEMBERSHIP,
            Some("invitation") => ids::INVITATION,
            Some("role") => ids::ROLE,
            Some("role_assignment") => ids::ASSIGNMENT,
            Some("service_account") => ids::SERVICE_ACCOUNT,
            _ => return Value::String(id.simple().to_string()),
        };
        Value::String(ids::render(prefix, id))
    };
    let actor_kind: Option<String> = row.get(4);
    let target_kind: Option<String> = row.get(7);
    json!({
        "id": row.get::<_, Uuid>(0).hyphenated().to_string(),
        "occurred_at": row.get::<_, Timestamp>(1).to_string(),
        "action": row.get::<_, String>(2),
        "outcome": row.get::<_, String>(3),
        "actor_kind": actor_kind,
        "actor_id": render(actor_kind.as_deref(), row.get(5)),
        "organization_id": row.get::<_, Option<Uuid>>(6).map(|o| ids::render(ids::ORGANIZATION, o)),
        "target_kind": target_kind,
        "target_id": render(target_kind.as_deref(), row.get(8)),
        "details": row.get::<_, Value>(9),
    })
}

/// What `POST .../api-keys` asks for.
#[derive(Clone, Debug)]
pub struct NewKeyInput {
    pub name: String,
    pub permissions: Vec<String>,
    pub expires_at: Timestamp,
    pub service_account: Option<Uuid>,
}

impl Core {
    /// The caller checked for `name` at `scope`: the tenant rule of
    /// [`Core::in_org`] or the application check.
    async fn at(self: &Arc<Self>, who: &Who, scope: Scope, name: &str) -> Result<Who, ApiError> {
        match scope {
            Scope::Tenant(org) => self.in_org(who, org, name).await,
            Scope::Application => {
                self.in_application(who, name).await?;
                Ok(who.clone())
            }
        }
    }
    /// Lock the organization of a tenant scope; nothing for the application.
    async fn lock_scope(
        &self,
        tx: &mut merezha_platform::Tx<'_>,
        scope: Scope,
    ) -> Result<(), ApiError> {
        if let Scope::Tenant(org) = scope {
            self.lock_org(tx, org).await?;
        }
        Ok(())
    }
    fn scope_audit(audit: crate::core::Audit, scope: Scope) -> crate::core::Audit {
        match scope {
            Scope::Tenant(org) => audit.organization(org),
            Scope::Application => audit,
        }
        .detail("scope", scope.as_str())
    }

    // ----- service accounts -----

    pub async fn list_service_accounts(
        self: &Arc<Self>,
        who: &Who,
        scope: Scope,
        cursor: Option<Uuid>,
        limit: i64,
    ) -> Result<Reply, ApiError> {
        self.at(who, scope, "identity.service_accounts.read")
            .await?;
        let client = self.db().acquire(self.deadline()).await?;
        let rows = self
            .db()
            .query(
                &client,
                &crate::sql::SERVICES_LIST,
                &[&scope.organization(), &cursor, &(limit + 1)],
            )
            .await
            .map_err(sql)?;
        let (items, next) = page(&rows, limit, service_json, |r| {
            ids::render(ids::SERVICE_ACCOUNT, r.get(0))
        });
        Ok(Reply::json(
            200,
            json!({"items": items, "next_cursor": next}),
        ))
    }

    pub async fn create_service_account(
        self: &Arc<Self>,
        who: &Who,
        scope: Scope,
        name: String,
        description: Option<String>,
    ) -> Result<Reply, ApiError> {
        let who = self
            .at(who, scope, "identity.service_accounts.manage")
            .await?;
        self.recent(&who)?;
        let core = self.clone();
        let outcome: Committed<Value> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_scope(tx, scope).await?;
                    let count: i64 = core
                        .db()
                        .query_one(&*tx, &crate::sql::SERVICES_COUNT, &[&scope.organization()])
                        .await
                        .map_err(sql)?
                        .get(0);
                    if count >= i64::from(core.settings.service_accounts_max) {
                        return Ok(Err(ApiError::code("service_account_limit")));
                    }
                    let id = core.new_id()?;
                    core.db()
                        .execute(
                            &*tx,
                            &crate::sql::SERVICE_INSERT,
                            &[
                                &id,
                                &scope.as_str(),
                                &scope.organization(),
                                &name,
                                &description,
                                &who.creator(),
                                &core.now(),
                            ],
                        )
                        .await
                        .map_err(sql)?;
                    core.audit(
                        tx,
                        Core::scope_audit(who.audit("service_account.created"), scope)
                            .target_of("service_account", id),
                        &who.client,
                    )
                    .await?;
                    let row = core
                        .db()
                        .query_one(
                            &*tx,
                            &crate::sql::SERVICE_LOCK,
                            &[&id, &scope.organization()],
                        )
                        .await
                        .map_err(sql)?;
                    Ok(Ok(json!({"service_account": service_json(&row)})))
                })
            })
            .await?;
        Ok(Reply::json(201, outcome?))
    }

    /// `PATCH .../service-accounts/{id}`: disabling one stops its keys on
    /// the next request (its version is bumped).
    pub async fn update_service_account(
        self: &Arc<Self>,
        who: &Who,
        scope: Scope,
        id: Uuid,
        name: Option<String>,
        description: Option<Option<String>>,
        status: Option<String>,
    ) -> Result<Reply, ApiError> {
        let who = self
            .at(who, scope, "identity.service_accounts.manage")
            .await?;
        self.recent(&who)?;
        let core = self.clone();
        let outcome: Committed<Value> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_scope(tx, scope).await?;
                    if core
                        .db()
                        .query_opt(
                            &*tx,
                            &crate::sql::SERVICE_LOCK,
                            &[&id, &scope.organization()],
                        )
                        .await
                        .map_err(sql)?
                        .is_none()
                    {
                        return Ok(Err(ApiError::code("not_found")));
                    }
                    core.db()
                        .execute(
                            &*tx,
                            &crate::sql::SERVICE_UPDATE,
                            &[
                                &id,
                                &name,
                                &description.is_some(),
                                &description.clone().flatten(),
                                &status,
                                &core.now(),
                            ],
                        )
                        .await
                        .map_err(sql)?;
                    let mut audit = Core::scope_audit(who.audit("service_account.updated"), scope)
                        .target_of("service_account", id);
                    if let Some(status) = &status {
                        audit = audit.detail("status", status.as_str());
                    }
                    core.audit(tx, audit, &who.client).await?;
                    let row = core
                        .db()
                        .query_one(
                            &*tx,
                            &crate::sql::SERVICE_LOCK,
                            &[&id, &scope.organization()],
                        )
                        .await
                        .map_err(sql)?;
                    Ok(Ok(json!({"service_account": service_json(&row)})))
                })
            })
            .await?;
        Ok(Reply::json(200, outcome?))
    }

    /// `DELETE .../service-accounts/{id}`: its keys and assignments go
    /// with it.
    pub async fn delete_service_account(
        self: &Arc<Self>,
        who: &Who,
        scope: Scope,
        id: Uuid,
    ) -> Result<Reply, ApiError> {
        let who = self
            .at(who, scope, "identity.service_accounts.manage")
            .await?;
        self.recent(&who)?;
        let core = self.clone();
        let outcome: Committed<()> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_scope(tx, scope).await?;
                    if core
                        .db()
                        .query_opt(
                            &*tx,
                            &crate::sql::SERVICE_LOCK,
                            &[&id, &scope.organization()],
                        )
                        .await
                        .map_err(sql)?
                        .is_none()
                    {
                        return Ok(Err(ApiError::code("not_found")));
                    }
                    core.db()
                        .execute(&*tx, &crate::sql::SERVICE_DELETE, &[&id])
                        .await
                        .map_err(sql)?;
                    core.audit(
                        tx,
                        Core::scope_audit(who.audit("service_account.deleted"), scope)
                            .target_of("service_account", id),
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

    // ----- API keys -----

    /// `GET .../api-keys`: every key of the scope with
    /// `identity.api_keys.read`, else the caller's own.
    pub async fn list_keys(
        self: &Arc<Self>,
        who: &Who,
        scope: Scope,
        service_account: Option<Uuid>,
    ) -> Result<Reply, ApiError> {
        let who = match scope {
            Scope::Tenant(org) => self.scoped(who, org).await?,
            Scope::Application => who.clone(),
        };
        let read = merezha_domain::Identity::new("identity.api_keys.read")
            .map_err(|_| Failure::permanent("internal"))?;
        let all = match who.principal.check(&read, &scope.resource_scope()) {
            // Break-glass is audited before the listing is served.
            Ok(merezha_platform::ports::Allowed::BreakGlass) => {
                self.audit_break_glass(&who.principal, &read).await?;
                true
            }
            Ok(_) => true,
            Err(_) => false,
        };
        let only = if all {
            None
        } else {
            match (scope, who.user) {
                (Scope::Tenant(org), Some(user)) if self.standing(&who, org) => Some(user),
                (Scope::Application, Some(user)) => Some(user),
                (Scope::Tenant(org), _) if !self.standing(&who, org) => {
                    return Err(ApiError::code("not_found"));
                }
                _ => return Err(ApiError::code("unauthorized")),
            }
        };
        let client = self.db().acquire(self.deadline()).await?;
        let rows = self
            .db()
            .query(
                &client,
                &crate::sql::KEYS_LIST,
                &[&scope.organization(), &only, &service_account, &1000_i64],
            )
            .await
            .map_err(sql)?;
        let items: Vec<Value> = rows.iter().map(key_json).collect();
        Ok(Reply::json(200, json!({"items": items})))
    }

    /// `POST .../api-keys` (recent). An own key needs
    /// `identity.api_keys.create` and, in a tenant, a live membership
    /// re-checked under the organization lock (so minting cannot race a
    /// removal); a service account's key needs
    /// `identity.service_accounts.manage`. Every listed permission must be
    /// held by the minter at the key's scope, so a key never exceeds its
    /// owner. The key appears once in the answer.
    pub async fn create_key(
        self: &Arc<Self>,
        who: &Who,
        scope: Scope,
        input: NewKeyInput,
    ) -> Result<Reply, ApiError> {
        let permission = if input.service_account.is_some() {
            "identity.service_accounts.manage"
        } else {
            "identity.api_keys.create"
        };
        let who = self.at(who, scope, permission).await?;
        self.recent(&who)?;
        let user = who.user.ok_or_else(|| ApiError::code("unauthorized"))?;
        if input.permissions.is_empty() || input.permissions.len() > 256 {
            return Err(ApiError::invalid(vec![("/permissions", "out_of_range")]));
        }
        for grant in &input.permissions {
            self.rbac
                .catalog()
                .check_grant(grant, crate::catalog::Wildcards::Application)
                .map_err(ApiError::code)?;
        }
        let set = self.rbac.custom(&input.permissions);
        self.no_escalation(&who, &set, &scope.resource_scope())?;
        let now = self.now();
        if input.expires_at <= now || input.expires_at > plus(now, self.settings.api_keys.max_ttl) {
            return Err(ApiError::invalid(vec![("/expires_at", "out_of_range")]));
        }
        let minted = apikey::mint(self.ctx.random())?;
        let core = self.clone();
        let outcome: Committed<Value> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_scope(tx, scope).await?;
                    let (owner_user, owner_service) = match input.service_account {
                        Some(service) => {
                            if core
                                .db()
                                .query_opt(
                                    &*tx,
                                    &crate::sql::SERVICE_LOCK,
                                    &[&service, &scope.organization()],
                                )
                                .await
                                .map_err(sql)?
                                .is_none()
                            {
                                return Ok(Err(ApiError::code("not_found")));
                            }
                            (None, Some(service))
                        }
                        None => {
                            if let Scope::Tenant(org) = scope
                                && core
                                    .db()
                                    .query_opt(
                                        &*tx,
                                        &crate::sql::MEMBERSHIP_OF_USER,
                                        &[&org, &user],
                                    )
                                    .await
                                    .map_err(sql)?
                                    .is_none()
                            {
                                return Ok(Err(ApiError::code("unauthorized")));
                            }
                            (Some(user), None)
                        }
                    };
                    let count: i64 = core
                        .db()
                        .query_one(
                            &*tx,
                            &crate::sql::KEYS_COUNT,
                            &[&scope.organization(), &core.now()],
                        )
                        .await
                        .map_err(sql)?
                        .get(0);
                    if count >= i64::from(core.settings.api_keys.max_per_scope) {
                        return Ok(Err(ApiError::code("api_key_limit")));
                    }
                    let id = core.new_id()?;
                    let digest: &[u8] = &minted.digest;
                    core.db()
                        .execute(
                            &*tx,
                            &crate::sql::KEY_INSERT,
                            &[
                                &id,
                                &minted.public_id,
                                &digest,
                                &scope.organization(),
                                &owner_user,
                                &owner_service,
                                &input.name,
                                &input.permissions,
                                &Some(user),
                                &core.now(),
                                &input.expires_at,
                            ],
                        )
                        .await
                        .map_err(sql)?;
                    core.audit(
                        tx,
                        Core::scope_audit(who.audit("api_key.created"), scope)
                            .target_of("api_key", id)
                            .detail("permissions", json!(input.permissions)),
                        &who.client,
                    )
                    .await?;
                    let row = core
                        .db()
                        .query_one(&*tx, &crate::sql::KEY_GET, &[&id, &scope.organization()])
                        .await
                        .map_err(sql)?;
                    Ok(Ok(json!({
                        "api_key": key_json(&row),
                        "key": minted.value.expose_secret(),
                    })))
                })
            })
            .await?;
        Ok(Reply::json(201, outcome?))
    }

    /// `DELETE .../api-keys/{id}`: one's own key, or with
    /// `identity.api_keys.revoke`. The row stays 30 days so a later use is
    /// audited as a use after revocation.
    pub async fn revoke_key(
        self: &Arc<Self>,
        who: &Who,
        scope: Scope,
        key: Uuid,
    ) -> Result<Reply, ApiError> {
        let who = match scope {
            Scope::Tenant(org) => self.scoped(who, org).await?,
            Scope::Application => who.clone(),
        };
        let core = self.clone();
        let outcome: Committed<()> = self
            .in_tx(move |tx| {
                Box::pin(async move {
                    core.lock_scope(tx, scope).await?;
                    let Some(row) = core
                        .db()
                        .query_opt(&*tx, &crate::sql::KEY_GET, &[&key, &scope.organization()])
                        .await
                        .map_err(sql)?
                    else {
                        return Ok(Err(ApiError::code("not_found")));
                    };
                    let owner: Option<Uuid> = row.get(2);
                    let own = owner.is_some() && owner == who.user;
                    if !own {
                        match core
                            .require(&who, "identity.api_keys.revoke", &scope.resource_scope())
                            .await
                        {
                            Ok(_) => {}
                            Err(_) if matches!(scope, Scope::Tenant(org) if !core.standing(&who, org)) => {
                                return Ok(Err(ApiError::code("not_found")));
                            }
                            Err(error) => return Ok(Err(error)),
                        }
                    }
                    core.db()
                        .execute(&*tx, &crate::sql::KEY_REVOKE, &[&key, &core.now()])
                        .await
                        .map_err(sql)?;
                    core.audit(
                        tx,
                        Core::scope_audit(who.audit("api_key.revoked"), scope)
                            .target_of("api_key", key)
                            .detail("reason", "revoked"),
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

    // ----- audit -----

    /// `GET .../audit-events`: newest first; a tenant lists its own rows,
    /// the application every organization's (optionally one).
    #[allow(clippy::too_many_arguments)]
    pub async fn list_audit(
        self: &Arc<Self>,
        who: &Who,
        scope: Scope,
        organization: Option<Uuid>,
        action: Option<String>,
        actor: Option<Uuid>,
        cursor: Option<Uuid>,
        limit: i64,
    ) -> Result<Reply, ApiError> {
        self.at(who, scope, "identity.audit.read").await?;
        let org = scope.organization().or(organization);
        let client = self.db().acquire(self.deadline()).await?;
        let rows = self
            .db()
            .query(
                &client,
                &crate::sql::AUDIT_LIST_ORG,
                &[&org, &action, &actor, &cursor, &(limit + 1)],
            )
            .await
            .map_err(sql)?;
        let (items, next) = page(&rows, limit, audit_json, |r| {
            r.get::<_, Uuid>(0).hyphenated().to_string()
        });
        Ok(Reply::json(
            200,
            json!({"items": items, "next_cursor": next}),
        ))
    }
}
