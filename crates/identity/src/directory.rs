//! The `Directory` port (R7, section 8): verified contacts of active users
//! and memberships, which notifications reads to resolve `User`, `Tenant`
//! and `Application` recipients. Users that are disabled or unverified are
//! simply absent; service accounts never appear; pages are keyset-ordered
//! by user id and bounded; `is_member` counts memberships only.
use crate::{core::Core, core::sql, ids, rbac::USER};
use merezha_domain::Failure;
use merezha_platform::{
    BoxFuture,
    ports::{Contact, Directory, MAX_USERS_PER_CALL, Page, PageRequest, RoleKey, TenantId, UserId},
};
use std::sync::{Arc, OnceLock};
use uuid::Uuid;

/// Identity's [`Directory`]. It exists before the block starts, so the
/// host can take it at construction, and answers `unavailable` until the
/// block is running.
#[derive(Clone, Default)]
pub struct IdentityDirectory {
    core: Arc<OnceLock<Arc<Core>>>,
}
impl std::fmt::Debug for IdentityDirectory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IdentityDirectory")
            .field("started", &self.core.get().is_some())
            .finish()
    }
}
impl IdentityDirectory {
    pub(crate) fn new(core: Arc<OnceLock<Arc<Core>>>) -> Self {
        Self { core }
    }
    fn core(&self) -> Result<&Arc<Core>, Failure> {
        self.core
            .get()
            .ok_or_else(|| Failure::transient("unavailable"))
    }
}

fn contact(row: &tokio_postgres::Row) -> Contact {
    Contact {
        user: ids::identity(ids::USER, row.get(0)),
        email: row.get(1),
        display_name: row.get(2),
        locale: row.get(3),
    }
}
/// Ids that are no user id are absent, like unknown users.
fn user_ids(ids_: &[UserId]) -> Vec<Uuid> {
    ids_.iter()
        .filter_map(|id| ids::parse(ids::USER, id.as_str()).ok())
        .collect()
}
/// `None` (everyone) for no filter or one naming the implicit `user`.
fn role_filter(roles: Option<&[RoleKey]>) -> Option<Vec<String>> {
    let roles = roles?;
    if roles.is_empty() || roles.iter().any(|r| r.as_str() == USER) {
        return None;
    }
    Some(roles.iter().map(|r| r.as_str().to_owned()).collect())
}
fn after(page: &PageRequest) -> Result<Option<Uuid>, Failure> {
    page.after()
        .map(|id| {
            ids::parse(ids::USER, id.as_str()).map_err(|_| Failure::permanent("invalid_page"))
        })
        .transpose()
}
fn paged(rows: Vec<tokio_postgres::Row>, limit: u16) -> Page<Contact> {
    let limit = usize::from(limit);
    let more = rows.len() > limit;
    let items: Vec<Contact> = rows.iter().take(limit).map(contact).collect();
    let next = if more {
        items.last().map(|c| c.user.clone())
    } else {
        None
    };
    Page { items, next }
}

impl Directory for IdentityDirectory {
    fn users<'a>(&'a self, ids_: &'a [UserId]) -> BoxFuture<'a, Result<Vec<Contact>, Failure>> {
        Box::pin(async move {
            if ids_.len() > MAX_USERS_PER_CALL {
                return Err(Failure::permanent("too_many_ids"));
            }
            let core = self.core()?;
            let wanted = user_ids(ids_);
            if wanted.is_empty() {
                return Ok(Vec::new());
            }
            let session = core.db().acquire(core.deadline()).await?;
            let rows = core
                .db()
                .query(&session, &crate::sql::DIRECTORY_USERS, &[&wanted])
                .await
                .map_err(sql)?;
            Ok(rows.iter().map(contact).collect())
        })
    }
    fn tenant_members<'a>(
        &'a self,
        tenant: &'a TenantId,
        roles: Option<&'a [RoleKey]>,
        page: PageRequest,
    ) -> BoxFuture<'a, Result<Page<Contact>, Failure>> {
        Box::pin(async move {
            let core = self.core()?;
            let Ok(org) = ids::parse(ids::ORGANIZATION, tenant.as_str()) else {
                return Ok(Page::empty());
            };
            let filter = role_filter(roles);
            let after = after(&page)?;
            let limit = i64::from(page.limit()) + 1;
            let session = core.db().acquire(core.deadline()).await?;
            let rows = core
                .db()
                .query(
                    &session,
                    &crate::sql::DIRECTORY_TENANT_MEMBERS,
                    &[&org, &filter, &after, &limit],
                )
                .await
                .map_err(sql)?;
            Ok(paged(rows, page.limit()))
        })
    }
    fn application_members<'a>(
        &'a self,
        roles: Option<&'a [RoleKey]>,
        page: PageRequest,
    ) -> BoxFuture<'a, Result<Page<Contact>, Failure>> {
        Box::pin(async move {
            let core = self.core()?;
            let filter = role_filter(roles);
            let after = after(&page)?;
            let limit = i64::from(page.limit()) + 1;
            let session = core.db().acquire(core.deadline()).await?;
            let rows = core
                .db()
                .query(
                    &session,
                    &crate::sql::DIRECTORY_APPLICATION_MEMBERS,
                    &[&filter, &after, &limit],
                )
                .await
                .map_err(sql)?;
            Ok(paged(rows, page.limit()))
        })
    }
    fn is_member<'a>(
        &'a self,
        user: &'a UserId,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<bool, Failure>> {
        Box::pin(async move {
            let core = self.core()?;
            let (Ok(user), Ok(org)) = (
                ids::parse(ids::USER, user.as_str()),
                ids::parse(ids::ORGANIZATION, tenant.as_str()),
            ) else {
                return Ok(false);
            };
            let session = core.db().acquire(core.deadline()).await?;
            Ok(core
                .db()
                .query_one(&session, &crate::sql::DIRECTORY_IS_MEMBER, &[&user, &org])
                .await
                .map_err(sql)?
                .get(0))
        })
    }
}
