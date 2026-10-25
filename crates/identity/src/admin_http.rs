//! The PR 2 routes under `/v1/identity` (section 4): organizations,
//! members, invitations, roles, role assignments, service accounts, API
//! keys and audit listings at `Tenant(org)`, and the same administration at
//! `Application` under `/application`. Same conventions as the PR 1 routes:
//! JSON bodies of at most 16 KiB, `Cache-Control: no-store`, errors that
//! never echo input, and prefixed ids (an id of another type, or of another
//! organization, is `404 not_found`).
use crate::{
    access::{Scope, Who},
    core::{ApiError, Core},
    flows::Reply,
    http::{Ctx, Fields, Shared, finish, json, respond},
    ids,
    machines::NewKeyInput,
    normalize,
    tenancy::{AssignmentInput, MAX_ROLE_IDS},
};
use axum::{
    Router,
    extract::{Path, Request, State},
    response::Response,
    routing::{delete, get, patch, post, put},
};
use merezha_domain::Identity;
use merezha_platform::edge::Authenticated;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

/// `limit` of listings: 1 to 100, 50 by default.
const DEFAULT_LIMIT: i64 = 50;
const MAX_LIMIT: i64 = 100;

pub(crate) fn routes() -> Router<Arc<Core>> {
    Router::new()
        .route("/organizations", post(create_org).get(list_orgs))
        .route("/session/organization", put(select_org))
        .route("/permissions", get(catalog))
        .route("/me/permissions", get(application_permissions))
        .route("/invitations/inspect", post(inspect))
        .route("/invitations/accept", post(accept))
        .route(
            "/organizations/{org}",
            get(get_org).patch(update_org).delete(delete_org),
        )
        .route("/organizations/{org}/ownership-transfers", post(transfer))
        .route("/organizations/{org}/me/permissions", get(org_permissions))
        .route("/organizations/{org}/members", get(list_members))
        .route(
            "/organizations/{org}/members/{membership}",
            patch(update_member).delete(remove_member),
        )
        .route(
            "/organizations/{org}/invitations",
            post(invite).get(list_invitations),
        )
        .route(
            "/organizations/{org}/invitations/{invitation}",
            delete(revoke_invitation),
        )
        .route(
            "/organizations/{org}/roles",
            get(list_roles).post(create_role),
        )
        .route(
            "/organizations/{org}/roles/{role}",
            patch(update_role).delete(delete_role),
        )
        .route(
            "/organizations/{org}/role-assignments",
            get(list_assignments).post(assign),
        )
        .route(
            "/organizations/{org}/role-assignments/{assignment}",
            delete(unassign),
        )
        .route(
            "/organizations/{org}/service-accounts",
            get(org_list_services).post(org_create_service),
        )
        .route(
            "/organizations/{org}/service-accounts/{service}",
            patch(org_update_service).delete(org_delete_service),
        )
        .route(
            "/organizations/{org}/api-keys",
            get(org_list_keys).post(org_create_key),
        )
        .route(
            "/organizations/{org}/api-keys/{key}",
            delete(org_revoke_key),
        )
        .route("/organizations/{org}/audit-events", get(org_audit))
        .route("/application/users", get(list_users))
        .route("/application/users/{user}", patch(update_user))
        .route("/application/roles", get(application_roles))
        .route(
            "/application/role-assignments",
            get(list_application_assignments).post(assign_application),
        )
        .route(
            "/application/role-assignments/{assignment}",
            delete(unassign_application),
        )
        .route(
            "/application/service-accounts",
            get(app_list_services).post(app_create_service),
        )
        .route(
            "/application/service-accounts/{service}",
            patch(app_update_service).delete(app_delete_service),
        )
        .route(
            "/application/api-keys",
            get(app_list_keys).post(app_create_key),
        )
        .route("/application/api-keys/{key}", delete(app_revoke_key))
        .route("/application/audit-events", get(app_audit))
}

// ----- request helpers -----

fn who(ctx: &Ctx) -> Result<Who, ApiError> {
    let principal = ctx
        .parts
        .extensions
        .get::<Authenticated>()
        .and_then(|a| a.0.clone());
    Who::new(principal, ctx.client.clone()).ok_or_else(|| ApiError::code("unauthenticated"))
}
/// A prefixed id in a path; anything else is `not_found`.
fn path_id(prefix: &str, text: &str) -> Result<Uuid, ApiError> {
    Ok(ids::parse(prefix, text)?)
}

/// Parsed `?key=value` pairs, every key in `allowed`.
struct Query {
    pairs: Vec<(String, String)>,
}
impl Query {
    fn parse(query: Option<&str>, allowed: &[&str]) -> Result<Self, ApiError> {
        let mut pairs = Vec::new();
        let mut details = Vec::new();
        for pair in query.unwrap_or("").split('&').filter(|p| !p.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            if !allowed.contains(&key) || pairs.iter().any(|(k, _): &(String, String)| k == key) {
                details.push(("/query", "unknown"));
                continue;
            }
            pairs.push((key.to_owned(), decode(value)));
        }
        if details.is_empty() {
            Ok(Self { pairs })
        } else {
            Err(ApiError::invalid(details))
        }
    }
    fn get(&self, key: &str) -> Option<&str> {
        self.pairs
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
    /// A prefixed id filter; a malformed one is `validation_failed`.
    fn id(&self, key: &'static str, prefix: &str) -> Result<Option<Uuid>, ApiError> {
        self.get(key)
            .map(|v| {
                ids::parse(prefix, v)
                    .map_err(|_| ApiError::invalid(vec![(path_of(key), "invalid")]))
            })
            .transpose()
    }
    /// The page: `cursor` (a prefixed id, or a UUID for audit rows) and
    /// `limit`.
    fn page(&self, prefix: Option<&str>) -> Result<(Option<Uuid>, i64), ApiError> {
        let cursor = match (self.get("cursor"), prefix) {
            (None, _) => None,
            (Some(v), Some(prefix)) => Some(
                ids::parse(prefix, v)
                    .map_err(|_| ApiError::invalid(vec![("/cursor", "invalid")]))?,
            ),
            (Some(v), None) => Some(
                Uuid::try_parse(v).map_err(|_| ApiError::invalid(vec![("/cursor", "invalid")]))?,
            ),
        };
        let limit = match self.get("limit") {
            None => DEFAULT_LIMIT,
            Some(v) => match v.parse::<i64>() {
                Ok(n) if (1..=MAX_LIMIT).contains(&n) => n,
                _ => return Err(ApiError::invalid(vec![("/limit", "invalid")])),
            },
        };
        Ok((cursor, limit))
    }
}
fn path_of(key: &'static str) -> &'static str {
    match key {
        "membership_id" => "/membership_id",
        "service_account_id" => "/service_account_id",
        "user_id" => "/user_id",
        "role_id" => "/role_id",
        "actor_id" => "/actor_id",
        "organization_id" => "/organization_id",
        _ => "/query",
    }
}
/// Percent-decoding of a query value (`%40` in an email filter); `+` is
/// a space.
fn decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = |j: usize| -> Option<u8> {
            let pair = bytes.get(j + 1..j + 3)?;
            u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()
        };
        match bytes[i] {
            b'%' => match hex(i) {
                Some(b) => {
                    out.push(b);
                    i += 3;
                }
                None => {
                    out.push(b'%');
                    i += 1;
                }
            },
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

impl Fields {
    /// A list of 0 to `max` strings.
    fn strings(&mut self, name: &str, max: usize, required: bool) -> Option<Vec<String>> {
        match self.take(name) {
            None | Some(Value::Null) => {
                if required {
                    self.problem(name, "required");
                }
                None
            }
            Some(Value::Array(items)) if items.len() <= max => {
                let mut out = Vec::new();
                for item in items {
                    match item {
                        Value::String(s) => out.push(s),
                        _ => {
                            self.problem(name, "invalid");
                            return None;
                        }
                    }
                }
                Some(out)
            }
            Some(Value::Array(_)) => {
                self.problem(name, "out_of_range");
                None
            }
            Some(_) => {
                self.problem(name, "invalid");
                None
            }
        }
    }
    /// A name or role name: NFC, 1 to 128 scalar values, no control
    /// characters.
    fn name(&mut self, field: &str, required: bool) -> Option<String> {
        let text = if required {
            self.string(field)?
        } else {
            self.nullable(field)??
        };
        match normalize::display_name(&text) {
            Ok(name) => Some(name),
            Err(code) => {
                self.problem(field, code);
                None
            }
        }
    }
    /// An optional description of at most 1024 scalar values; `null`
    /// clears it.
    fn description(&mut self) -> Option<Option<String>> {
        match self.nullable("description")? {
            None => Some(None),
            Some(text) if text.chars().count() <= 1024 && !text.chars().any(char::is_control) => {
                Some(Some(text))
            }
            Some(_) => {
                self.problem("description", "too_long");
                None
            }
        }
    }
    fn id(&mut self, name: &str, prefix: &str) -> Option<Uuid> {
        let text = self.nullable(name)??;
        match ids::parse(prefix, &text) {
            Ok(id) => Some(id),
            Err(_) => {
                self.problem(name, "invalid");
                None
            }
        }
    }
    /// `resource: {kind, id}`: a resource kind (`[a-z][a-z0-9_]{0,62}`) and
    /// an id of 1 to 128 identity characters.
    fn resource(&mut self) -> Option<(Identity, Identity)> {
        match self.take("resource") {
            None | Some(Value::Null) => None,
            Some(Value::Object(mut map)) => {
                let kind = map
                    .remove("kind")
                    .and_then(|v| v.as_str().map(str::to_owned));
                let id = map.remove("id").and_then(|v| v.as_str().map(str::to_owned));
                let kind = kind.filter(|k| crate::rbac::valid_role_key(k));
                match (kind, id, map.is_empty()) {
                    (Some(kind), Some(id), true) => {
                        match (Identity::new(kind), Identity::new(id)) {
                            (Ok(kind), Ok(id)) => Some((kind, id)),
                            _ => {
                                self.problem("resource", "invalid");
                                None
                            }
                        }
                    }
                    _ => {
                        self.problem("resource", "invalid");
                        None
                    }
                }
            }
            Some(_) => {
                self.problem("resource", "invalid");
                None
            }
        }
    }
    fn timestamp(&mut self, name: &str) -> Option<jiff::Timestamp> {
        let text = self.string(name)?;
        match text.parse::<jiff::Timestamp>() {
            Ok(at) => Some(at),
            Err(_) => {
                self.problem(name, "invalid");
                None
            }
        }
    }
    fn permissions(&mut self, required: bool) -> Option<Vec<String>> {
        let list = self.strings("permissions", 256, required)?;
        if list.is_empty() {
            self.problem("permissions", "out_of_range");
            return None;
        }
        Some(list)
    }
}

/// A route without a body: the caller.
async fn bodyless<F, Fut>(core: Arc<Core>, request: Request, work: F) -> Response
where
    F: FnOnce(Arc<Core>, Who, Ctx) -> Fut,
    Fut: std::future::Future<Output = Result<Reply, ApiError>>,
{
    let (parts, _) = request.into_parts();
    let ctx = Ctx::new(parts);
    let result = match who(&ctx) {
        Ok(who) => work(core.clone(), who, ctx).await,
        Err(error) => Err(error),
    };
    respond(&core, result)
}
/// A route with a JSON body: the caller and the fields.
async fn with_body<F, Fut>(core: Arc<Core>, request: Request, work: F) -> Response
where
    F: FnOnce(Arc<Core>, Who, Ctx, Fields) -> Fut,
    Fut: std::future::Future<Output = Result<Reply, ApiError>>,
{
    let result = async {
        let (ctx, fields) = json(request).await?;
        let who = who(&ctx)?;
        work(core.clone(), who, ctx, fields).await
    }
    .await;
    respond(&core, result)
}

// ----- organizations -----

async fn create_org(State(core): Shared, request: Request) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let name = f.name("name", true);
        let slug = match f.string("slug") {
            Some(text) => match normalize::slug(&text) {
                Ok(slug) => Some(Ok(slug)),
                Err("reserved") => Some(Err(ApiError::conflict("slug_reserved"))),
                Err(code) => {
                    f.problem("slug", code);
                    None
                }
            },
            None => None,
        };
        finish(&mut f)?;
        let slug = slug.ok_or_else(|| ApiError::invalid(vec![]))??;
        core.create_organization(&who, crate::http::required(name)?, slug)
            .await
    })
    .await
}
async fn list_orgs(State(core): Shared, request: Request) -> Response {
    bodyless(core, request, |core, who, ctx| async move {
        let query = Query::parse(ctx.parts.uri.query(), &["cursor", "limit"])?;
        let (cursor, limit) = query.page(Some(ids::ORGANIZATION))?;
        core.list_organizations(&who, cursor, limit).await
    })
    .await
}
async fn select_org(State(core): Shared, request: Request) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let org = match f.take("organization_id") {
            None => {
                f.problem("organization_id", "required");
                None
            }
            Some(Value::Null) => Some(None),
            Some(Value::String(text)) => match ids::parse(ids::ORGANIZATION, &text) {
                Ok(id) => Some(Some(id)),
                // An id of no organization names no organization.
                Err(_) => return Err(ApiError::code("not_found")),
            },
            Some(_) => {
                f.problem("organization_id", "invalid");
                None
            }
        };
        finish(&mut f)?;
        core.select_organization(&who, crate::http::required(org)?)
            .await
    })
    .await
}
async fn catalog(State(core): Shared, request: Request) -> Response {
    bodyless(core, request, |core, who, _| async move {
        core.permission_catalog(&who)
    })
    .await
}
async fn application_permissions(State(core): Shared, request: Request) -> Response {
    bodyless(core, request, |core, who, _| async move {
        core.application_permissions(&who).await
    })
    .await
}
async fn get_org(State(core): Shared, Path(org): Path<String>, request: Request) -> Response {
    bodyless(core, request, |core, who, _| async move {
        core.get_organization(&who, path_id(ids::ORGANIZATION, &org)?)
            .await
    })
    .await
}
async fn update_org(State(core): Shared, Path(org): Path<String>, request: Request) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let org = path_id(ids::ORGANIZATION, &org)?;
        let name = f.name("name", false);
        let mut reserved = false;
        let slug = match f.nullable("slug").flatten() {
            Some(text) => match normalize::slug(&text) {
                Ok(slug) => Some(slug),
                Err("reserved") => {
                    reserved = true;
                    None
                }
                Err(code) => {
                    f.problem("slug", code);
                    None
                }
            },
            None => None,
        };
        finish(&mut f)?;
        if reserved {
            return Err(ApiError::conflict("slug_reserved"));
        }
        core.update_organization(&who, org, name, slug).await
    })
    .await
}
async fn delete_org(State(core): Shared, Path(org): Path<String>, request: Request) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let org = path_id(ids::ORGANIZATION, &org)?;
        let confirm = f.string("confirm_slug");
        finish(&mut f)?;
        core.delete_organization(&who, org, crate::http::required(confirm)?)
            .await
    })
    .await
}
async fn transfer(State(core): Shared, Path(org): Path<String>, request: Request) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let org = path_id(ids::ORGANIZATION, &org)?;
        if !f.has("membership_id") {
            f.problem("membership_id", "required");
        }
        let membership = f.id("membership_id", ids::MEMBERSHIP);
        let keep = f.boolean("keep_owner").unwrap_or(false);
        finish(&mut f)?;
        core.transfer_ownership(&who, org, crate::http::required(membership)?, keep)
            .await
    })
    .await
}
async fn org_permissions(
    State(core): Shared,
    Path(org): Path<String>,
    request: Request,
) -> Response {
    bodyless(core, request, |core, who, _| async move {
        core.org_permissions(&who, path_id(ids::ORGANIZATION, &org)?)
            .await
    })
    .await
}

// ----- members -----

async fn list_members(State(core): Shared, Path(org): Path<String>, request: Request) -> Response {
    bodyless(core, request, |core, who, ctx| async move {
        let org = path_id(ids::ORGANIZATION, &org)?;
        let query = Query::parse(ctx.parts.uri.query(), &["cursor", "limit"])?;
        let (cursor, limit) = query.page(Some(ids::MEMBERSHIP))?;
        core.list_members(&who, org, cursor, limit).await
    })
    .await
}
async fn update_member(
    State(core): Shared,
    Path((org, membership)): Path<(String, String)>,
    request: Request,
) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let org = path_id(ids::ORGANIZATION, &org)?;
        let membership = path_id(ids::MEMBERSHIP, &membership)?;
        let roles = f.strings("role_ids", MAX_ROLE_IDS, true);
        finish(&mut f)?;
        let mut role_ids = Vec::new();
        for text in crate::http::required(roles)? {
            role_ids.push(path_id(ids::ROLE, &text)?);
        }
        core.update_member_roles(&who, org, membership, role_ids)
            .await
    })
    .await
}
async fn remove_member(
    State(core): Shared,
    Path((org, membership)): Path<(String, String)>,
    request: Request,
) -> Response {
    bodyless(core, request, |core, who, _| async move {
        core.remove_member(
            &who,
            path_id(ids::ORGANIZATION, &org)?,
            path_id(ids::MEMBERSHIP, &membership)?,
        )
        .await
    })
    .await
}

// ----- invitations -----

async fn invite(State(core): Shared, Path(org): Path<String>, request: Request) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let org = path_id(ids::ORGANIZATION, &org)?;
        let email = f.email("email");
        let roles = f.strings("role_ids", MAX_ROLE_IDS, true);
        if roles.as_ref().is_some_and(Vec::is_empty) {
            f.problem("role_ids", "out_of_range");
        }
        finish(&mut f)?;
        let mut role_ids = Vec::new();
        for text in crate::http::required(roles)? {
            role_ids.push(path_id(ids::ROLE, &text)?);
        }
        core.invite(&who, org, crate::http::required(email)?, role_ids)
            .await
    })
    .await
}
async fn list_invitations(
    State(core): Shared,
    Path(org): Path<String>,
    request: Request,
) -> Response {
    bodyless(core, request, |core, who, _| async move {
        core.list_invitations(&who, path_id(ids::ORGANIZATION, &org)?)
            .await
    })
    .await
}
async fn revoke_invitation(
    State(core): Shared,
    Path((org, invitation)): Path<(String, String)>,
    request: Request,
) -> Response {
    bodyless(core, request, |core, who, _| async move {
        core.revoke_invitation(
            &who,
            path_id(ids::ORGANIZATION, &org)?,
            path_id(ids::INVITATION, &invitation)?,
        )
        .await
    })
    .await
}
async fn inspect(State(core): Shared, request: Request) -> Response {
    let result = async {
        let (ctx, mut f) = json(request).await?;
        ctx.anonymous_origin(&core)?;
        let token = f.string("token");
        finish(&mut f)?;
        core.inspect_invitation(&ctx.client, &crate::http::required(token)?)
            .await
    }
    .await;
    respond(&core, result)
}
async fn accept(State(core): Shared, request: Request) -> Response {
    let result = async {
        let (ctx, mut f) = json(request).await?;
        ctx.anonymous_origin(&core)?;
        let token = f.string("token");
        let transport = f.transport();
        finish(&mut f)?;
        core.accept_invitation(
            &ctx.client,
            ctx.caller.as_ref(),
            &crate::http::required(token)?,
            transport,
        )
        .await
    }
    .await;
    respond(&core, result)
}

// ----- roles -----

async fn list_roles(State(core): Shared, Path(org): Path<String>, request: Request) -> Response {
    bodyless(core, request, |core, who, _| async move {
        core.list_roles(&who, path_id(ids::ORGANIZATION, &org)?)
            .await
    })
    .await
}
async fn create_role(State(core): Shared, Path(org): Path<String>, request: Request) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let org = path_id(ids::ORGANIZATION, &org)?;
        let key = match f.string("key") {
            Some(key) if crate::rbac::valid_role_key(&key) => Some(key),
            Some(_) => {
                f.problem("key", "invalid");
                None
            }
            None => None,
        };
        let name = f.name("name", true);
        let description = f.description();
        let permissions = f.permissions(true);
        finish(&mut f)?;
        core.create_role(
            &who,
            org,
            crate::http::required(key)?,
            crate::http::required(name)?,
            description.flatten(),
            crate::http::required(permissions)?,
        )
        .await
    })
    .await
}
async fn update_role(
    State(core): Shared,
    Path((org, role)): Path<(String, String)>,
    request: Request,
) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let org = path_id(ids::ORGANIZATION, &org)?;
        let role = path_id(ids::ROLE, &role)?;
        let name = f.name("name", false);
        let present = f.has("description");
        let description = f.description();
        let permissions = f.permissions(false);
        finish(&mut f)?;
        let description = if present { description } else { None };
        core.update_role(&who, org, role, name, description, permissions)
            .await
    })
    .await
}
async fn delete_role(
    State(core): Shared,
    Path((org, role)): Path<(String, String)>,
    request: Request,
) -> Response {
    bodyless(core, request, |core, who, _| async move {
        core.delete_role(
            &who,
            path_id(ids::ORGANIZATION, &org)?,
            path_id(ids::ROLE, &role)?,
        )
        .await
    })
    .await
}

// ----- tenant role assignments -----

async fn list_assignments(
    State(core): Shared,
    Path(org): Path<String>,
    request: Request,
) -> Response {
    bodyless(core, request, |core, who, ctx| async move {
        let org = path_id(ids::ORGANIZATION, &org)?;
        let query = Query::parse(
            ctx.parts.uri.query(),
            &[
                "membership_id",
                "service_account_id",
                "resource_kind",
                "resource_id",
                "cursor",
                "limit",
            ],
        )?;
        let (cursor, limit) = query.page(Some(ids::ASSIGNMENT))?;
        core.list_assignments(
            &who,
            org,
            query.id("membership_id", ids::MEMBERSHIP)?,
            query.id("service_account_id", ids::SERVICE_ACCOUNT)?,
            query.get("resource_kind").map(str::to_owned),
            query.get("resource_id").map(str::to_owned),
            cursor,
            limit,
        )
        .await
    })
    .await
}
async fn assign(State(core): Shared, Path(org): Path<String>, request: Request) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let org = path_id(ids::ORGANIZATION, &org)?;
        let membership = f.nullable("membership_id").flatten();
        let service = f.nullable("service_account_id").flatten();
        let role = f.string("role_id");
        let resource = f.resource();
        if membership.is_some() == service.is_some() {
            f.problem("membership_id", "required");
        }
        finish(&mut f)?;
        let input = AssignmentInput {
            membership: membership
                .map(|m| path_id(ids::MEMBERSHIP, &m))
                .transpose()?,
            service_account: service
                .map(|s| path_id(ids::SERVICE_ACCOUNT, &s))
                .transpose()?,
            user: None,
            role: path_id(ids::ROLE, &crate::http::required(role)?)?,
            resource,
        };
        core.assign_tenant(&who, org, input).await
    })
    .await
}
async fn unassign(
    State(core): Shared,
    Path((org, assignment)): Path<(String, String)>,
    request: Request,
) -> Response {
    bodyless(core, request, |core, who, _| async move {
        core.unassign_tenant(
            &who,
            path_id(ids::ORGANIZATION, &org)?,
            path_id(ids::ASSIGNMENT, &assignment)?,
        )
        .await
    })
    .await
}

// ----- service accounts and API keys, both scopes -----

async fn list_services(core: Arc<Core>, scope: Option<String>, request: Request) -> Response {
    bodyless(core, request, |core, who, ctx| async move {
        let scope = scope_of(scope)?;
        let query = Query::parse(ctx.parts.uri.query(), &["cursor", "limit"])?;
        let (cursor, limit) = query.page(Some(ids::SERVICE_ACCOUNT))?;
        core.list_service_accounts(&who, scope, cursor, limit).await
    })
    .await
}
async fn create_service(core: Arc<Core>, scope: Option<String>, request: Request) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let scope = scope_of(scope)?;
        let name = f.name("name", true);
        let description = f.description();
        finish(&mut f)?;
        core.create_service_account(
            &who,
            scope,
            crate::http::required(name)?,
            description.flatten(),
        )
        .await
    })
    .await
}
async fn update_service(
    core: Arc<Core>,
    scope: Option<String>,
    service: String,
    request: Request,
) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let scope = scope_of(scope)?;
        let service = path_id(ids::SERVICE_ACCOUNT, &service)?;
        let name = f.name("name", false);
        let present = f.has("description");
        let description = f.description();
        let status = match f.nullable("status").flatten() {
            Some(s) if s == "active" || s == "disabled" => Some(s),
            Some(_) => {
                f.problem("status", "invalid");
                None
            }
            None => None,
        };
        finish(&mut f)?;
        let description = if present { description } else { None };
        core.update_service_account(&who, scope, service, name, description, status)
            .await
    })
    .await
}
async fn delete_service(
    core: Arc<Core>,
    scope: Option<String>,
    service: String,
    request: Request,
) -> Response {
    bodyless(core, request, |core, who, _| async move {
        core.delete_service_account(
            &who,
            scope_of(scope)?,
            path_id(ids::SERVICE_ACCOUNT, &service)?,
        )
        .await
    })
    .await
}
async fn list_keys(core: Arc<Core>, scope: Option<String>, request: Request) -> Response {
    bodyless(core, request, |core, who, ctx| async move {
        let scope = scope_of(scope)?;
        let query = Query::parse(ctx.parts.uri.query(), &["service_account_id"])?;
        core.list_keys(
            &who,
            scope,
            query.id("service_account_id", ids::SERVICE_ACCOUNT)?,
        )
        .await
    })
    .await
}
async fn create_key(core: Arc<Core>, scope: Option<String>, request: Request) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let scope = scope_of(scope)?;
        let name = f.name("name", true);
        let permissions = f.permissions(true);
        let expires_at = f.timestamp("expires_at");
        let service = f.id("service_account_id", ids::SERVICE_ACCOUNT);
        finish(&mut f)?;
        core.create_key(
            &who,
            scope,
            NewKeyInput {
                name: crate::http::required(name)?,
                permissions: crate::http::required(permissions)?,
                expires_at: crate::http::required(expires_at)?,
                service_account: service,
            },
        )
        .await
    })
    .await
}
async fn revoke_key(
    core: Arc<Core>,
    scope: Option<String>,
    key: String,
    request: Request,
) -> Response {
    bodyless(core, request, |core, who, _| async move {
        core.revoke_key(&who, scope_of(scope)?, path_id(ids::API_KEY, &key)?)
            .await
    })
    .await
}
fn scope_of(org: Option<String>) -> Result<Scope, ApiError> {
    Ok(match org {
        None => Scope::Application,
        Some(org) => Scope::Tenant(path_id(ids::ORGANIZATION, &org)?),
    })
}

async fn org_list_services(State(core): Shared, Path(org): Path<String>, r: Request) -> Response {
    list_services(core, Some(org), r).await
}
async fn org_create_service(State(core): Shared, Path(org): Path<String>, r: Request) -> Response {
    create_service(core, Some(org), r).await
}
async fn org_update_service(
    State(core): Shared,
    Path((org, service)): Path<(String, String)>,
    r: Request,
) -> Response {
    update_service(core, Some(org), service, r).await
}
async fn org_delete_service(
    State(core): Shared,
    Path((org, service)): Path<(String, String)>,
    r: Request,
) -> Response {
    delete_service(core, Some(org), service, r).await
}
async fn org_list_keys(State(core): Shared, Path(org): Path<String>, r: Request) -> Response {
    list_keys(core, Some(org), r).await
}
async fn org_create_key(State(core): Shared, Path(org): Path<String>, r: Request) -> Response {
    create_key(core, Some(org), r).await
}
async fn org_revoke_key(
    State(core): Shared,
    Path((org, key)): Path<(String, String)>,
    r: Request,
) -> Response {
    revoke_key(core, Some(org), key, r).await
}
async fn app_list_services(State(core): Shared, r: Request) -> Response {
    list_services(core, None, r).await
}
async fn app_create_service(State(core): Shared, r: Request) -> Response {
    create_service(core, None, r).await
}
async fn app_update_service(
    State(core): Shared,
    Path(service): Path<String>,
    r: Request,
) -> Response {
    update_service(core, None, service, r).await
}
async fn app_delete_service(
    State(core): Shared,
    Path(service): Path<String>,
    r: Request,
) -> Response {
    delete_service(core, None, service, r).await
}
async fn app_list_keys(State(core): Shared, r: Request) -> Response {
    list_keys(core, None, r).await
}
async fn app_create_key(State(core): Shared, r: Request) -> Response {
    create_key(core, None, r).await
}
async fn app_revoke_key(State(core): Shared, Path(key): Path<String>, r: Request) -> Response {
    revoke_key(core, None, key, r).await
}

// ----- audit -----

async fn audit(core: Arc<Core>, org: Option<String>, request: Request) -> Response {
    bodyless(core, request, |core, who, ctx| async move {
        let scope = scope_of(org)?;
        let allowed: &[&str] = match scope {
            Scope::Tenant(_) => &["cursor", "limit", "action", "actor_id"],
            Scope::Application => &["cursor", "limit", "action", "actor_id", "organization_id"],
        };
        let query = Query::parse(ctx.parts.uri.query(), allowed)?;
        let (cursor, limit) = query.page(None)?;
        let actor = match query.get("actor_id") {
            None => None,
            Some(text) => Some(
                ids::parse(ids::USER, text)
                    .or_else(|_| ids::parse(ids::API_KEY, text))
                    .map_err(|_| ApiError::invalid(vec![("/actor_id", "invalid")]))?,
            ),
        };
        let action = query.get("action").map(str::to_owned);
        if action.as_ref().is_some_and(|a| a.len() > 64) {
            return Err(ApiError::invalid(vec![("/action", "invalid")]));
        }
        core.list_audit(
            &who,
            scope,
            query.id("organization_id", ids::ORGANIZATION)?,
            action,
            actor,
            cursor,
            limit,
        )
        .await
    })
    .await
}
async fn org_audit(State(core): Shared, Path(org): Path<String>, r: Request) -> Response {
    audit(core, Some(org), r).await
}
async fn app_audit(State(core): Shared, r: Request) -> Response {
    audit(core, None, r).await
}

// ----- application scope -----

async fn list_users(State(core): Shared, request: Request) -> Response {
    bodyless(core, request, |core, who, ctx| async move {
        let query = Query::parse(ctx.parts.uri.query(), &["cursor", "limit", "email"])?;
        let (cursor, limit) = query.page(Some(ids::USER))?;
        let email = match query.get("email") {
            None => None,
            Some(text) => Some(
                normalize::email(text)
                    .map_err(|_| ApiError::invalid(vec![("/email", "invalid")]))?
                    .normalized()
                    .to_owned(),
            ),
        };
        core.list_users(&who, email, cursor, limit).await
    })
    .await
}
async fn update_user(State(core): Shared, Path(user): Path<String>, request: Request) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let user = path_id(ids::USER, &user)?;
        let status = match f.string("status") {
            Some(s) if s == "active" || s == "disabled" => Some(s),
            Some(_) => {
                f.problem("status", "invalid");
                None
            }
            None => None,
        };
        finish(&mut f)?;
        core.set_user_status(&who, user, crate::http::required(status)?)
            .await
    })
    .await
}
async fn application_roles(State(core): Shared, request: Request) -> Response {
    bodyless(core, request, |core, who, _| async move {
        core.list_application_roles(&who).await
    })
    .await
}
async fn list_application_assignments(State(core): Shared, request: Request) -> Response {
    bodyless(core, request, |core, who, ctx| async move {
        let query = Query::parse(
            ctx.parts.uri.query(),
            &[
                "user_id",
                "service_account_id",
                "role_id",
                "cursor",
                "limit",
            ],
        )?;
        let (cursor, limit) = query.page(Some(ids::ASSIGNMENT))?;
        core.list_application_assignments(
            &who,
            query.id("user_id", ids::USER)?,
            query.id("service_account_id", ids::SERVICE_ACCOUNT)?,
            query.id("role_id", ids::ROLE)?,
            cursor,
            limit,
        )
        .await
    })
    .await
}
async fn assign_application(State(core): Shared, request: Request) -> Response {
    with_body(core, request, |core, who, _, mut f| async move {
        let user = f.nullable("user_id").flatten();
        let service = f.nullable("service_account_id").flatten();
        let role = f.string("role_id");
        let resource = f.resource();
        if user.is_some() == service.is_some() {
            f.problem("user_id", "required");
        }
        finish(&mut f)?;
        let input = AssignmentInput {
            membership: None,
            service_account: service
                .map(|s| path_id(ids::SERVICE_ACCOUNT, &s))
                .transpose()?,
            user: user.map(|u| path_id(ids::USER, &u)).transpose()?,
            role: path_id(ids::ROLE, &crate::http::required(role)?)?,
            resource,
        };
        core.assign_application(&who, input).await
    })
    .await
}
async fn unassign_application(
    State(core): Shared,
    Path(assignment): Path<String>,
    request: Request,
) -> Response {
    bodyless(core, request, |core, who, _| async move {
        core.unassign_application(&who, path_id(ids::ASSIGNMENT, &assignment)?)
            .await
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queries_are_strict_and_decoded() {
        let query =
            Query::parse(Some("email=ada%40example.com&limit=5"), &["email", "limit"]).unwrap();
        assert_eq!(query.get("email"), Some("ada@example.com"));
        assert_eq!(query.page(None).unwrap(), (None, 5));
        for bad in ["other=1", "limit=1&limit=2"] {
            assert!(Query::parse(Some(bad), &["limit"]).is_err(), "{bad}");
        }
        for bad in ["limit=0", "limit=101", "cursor=nope"] {
            let query = Query::parse(Some(bad), &["limit", "cursor"]).unwrap();
            assert!(query.page(Some(ids::USER)).is_err(), "{bad}");
        }
        assert_eq!(decode("a%2"), "a%2");
        assert_eq!(decode("a+b%zz"), "a b%zz");
    }
}
