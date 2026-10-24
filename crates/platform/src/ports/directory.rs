//! The `Directory` port (R7, ID §8): verified contacts and memberships,
//! which identity implements and notifications reads.
use super::{RoleKey, TenantId, UserId};
use crate::BoxFuture;
use merezha_domain::Failure;

/// Most ids in one [`Directory::users`] call.
pub const MAX_USERS_PER_CALL: usize = 100;
/// Largest page of members.
pub const MAX_PAGE: u16 = 500;

/// A user's contact. Only active users with a verified email appear. `Debug`
/// shows neither the address nor the name; there are no phone numbers (ID10).
#[derive(Clone, PartialEq, Eq)]
pub struct Contact {
    pub user: UserId,
    pub email: String,
    pub display_name: Option<String>,
    pub locale: Option<String>,
}
impl std::fmt::Debug for Contact {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Contact")
            .field("user", &self.user)
            .field("locale", &self.locale)
            .finish_non_exhaustive()
    }
}

/// A keyset page request ordered by user id.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PageRequest {
    after: Option<UserId>,
    limit: u16,
}
impl PageRequest {
    /// The page after `after` (exclusive) of at most `limit` contacts,
    /// 1 to [`MAX_PAGE`]; fails permanent `invalid_page`.
    pub fn new(after: Option<UserId>, limit: u16) -> Result<Self, Failure> {
        if (1..=MAX_PAGE).contains(&limit) {
            Ok(Self { after, limit })
        } else {
            Err(Failure::permanent("invalid_page"))
        }
    }
    pub fn first(limit: u16) -> Result<Self, Failure> {
        Self::new(None, limit)
    }
    pub fn after(&self) -> Option<&UserId> {
        self.after.as_ref()
    }
    pub fn limit(&self) -> u16 {
        self.limit
    }
}
/// One page; `next` is the cursor for the following page, `None` at the end.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next: Option<UserId>,
}
impl<T> Page<T> {
    pub fn empty() -> Self {
        Self {
            items: Vec::new(),
            next: None,
        }
    }
}

/// Batch lookups of users and memberships.
///
/// Contract: return only active users with a verified email; absent ids are
/// simply missing. `users` takes at most [`MAX_USERS_PER_CALL`] ids
/// (`too_many_ids`); pages hold at most the requested limit. Role filters
/// match tenant roles or application roles, `None` meaning everyone.
/// Service accounts never appear. `is_member` counts memberships only, not
/// application grants. Every call uses its own pooled connection, so callers
/// resolve recipients and memberships before they open a transaction (R4);
/// no port waits on a second connection while a caller transaction is open.
pub trait Directory: Send + Sync {
    fn users<'a>(&'a self, ids: &'a [UserId]) -> BoxFuture<'a, Result<Vec<Contact>, Failure>>;
    fn tenant_members<'a>(
        &'a self,
        tenant: &'a TenantId,
        roles: Option<&'a [RoleKey]>,
        page: PageRequest,
    ) -> BoxFuture<'a, Result<Page<Contact>, Failure>>;
    fn application_members<'a>(
        &'a self,
        roles: Option<&'a [RoleKey]>,
        page: PageRequest,
    ) -> BoxFuture<'a, Result<Page<Contact>, Failure>>;
    fn is_member<'a>(
        &'a self,
        user: &'a UserId,
        tenant: &'a TenantId,
    ) -> BoxFuture<'a, Result<bool, Failure>>;
}

/// Fallback without identity: resolves nothing. Users are absent, pages are
/// empty and nobody is a member, so notifications skip user, tenant and
/// application recipients while address recipients still work.
#[derive(Debug, Default)]
pub struct EmptyDirectory;
impl Directory for EmptyDirectory {
    fn users<'a>(&'a self, ids: &'a [UserId]) -> BoxFuture<'a, Result<Vec<Contact>, Failure>> {
        let result = if ids.len() > MAX_USERS_PER_CALL {
            Err(Failure::permanent("too_many_ids"))
        } else {
            Ok(Vec::new())
        };
        Box::pin(async move { result })
    }
    fn tenant_members<'a>(
        &'a self,
        _: &'a TenantId,
        _: Option<&'a [RoleKey]>,
        _: PageRequest,
    ) -> BoxFuture<'a, Result<Page<Contact>, Failure>> {
        Box::pin(async { Ok(Page::empty()) })
    }
    fn application_members<'a>(
        &'a self,
        _: Option<&'a [RoleKey]>,
        _: PageRequest,
    ) -> BoxFuture<'a, Result<Page<Contact>, Failure>> {
        Box::pin(async { Ok(Page::empty()) })
    }
    fn is_member<'a>(
        &'a self,
        _: &'a UserId,
        _: &'a TenantId,
    ) -> BoxFuture<'a, Result<bool, Failure>> {
        Box::pin(async { Ok(false) })
    }
}
