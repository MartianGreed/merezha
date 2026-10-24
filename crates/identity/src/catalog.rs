//! The permission names identity knows in PR 1 (section 5): its own, and
//! those the host checks itself. ID PR 2 builds the full catalog from these,
//! the other blocks' lists (such as `merezha_content::CONTENT_PERMISSIONS`)
//! and `[[permissions]]` in `application.toml`, expands wildcards against it
//! and implements the `Authorizer`. Until then nothing grants them: the host
//! denies every permission check while identity is on.

/// Identity's own permissions, checked at `Tenant` under
/// `/organizations/` and at `Application` under `/application/` (PR 2).
pub const IDENTITY_PERMISSIONS: [&str; 20] = [
    "identity.organization.read",
    "identity.organization.update",
    "identity.organization.delete",
    "identity.organization.transfer",
    "identity.members.read",
    "identity.members.invite",
    "identity.members.update_roles",
    "identity.members.remove",
    "identity.roles.read",
    "identity.roles.manage",
    "identity.roles.assign",
    "identity.service_accounts.read",
    "identity.service_accounts.manage",
    "identity.api_keys.read",
    "identity.api_keys.create",
    "identity.api_keys.revoke",
    "identity.audit.read",
    "identity.users.read",
    "identity.users.manage",
    "identity.organization.enter",
];

/// Permissions the host checks at `Application` scope: the application
/// layer's administration, the jobs admin API and TE's
/// `/v1/admin/status` (`system.status.read`).
pub const HOST_PERMISSIONS: [&str; 8] = [
    "application.projections.catch_up",
    "application.projections.rebuild",
    "application.admin.read",
    "application.admin.write",
    "jobs.admin.read",
    "jobs.admin.write",
    "jobs.admin.payload",
    "system.status.read",
];

/// Section 5's grammar: two to four segments of `[a-z][a-z0-9_]{0,31}`.
pub fn valid_permission(name: &str) -> bool {
    let segments: Vec<&str> = name.split('.').collect();
    (2..=4).contains(&segments.len())
        && segments.iter().all(|s| {
            let bytes = s.as_bytes();
            !bytes.is_empty()
                && bytes.len() <= 32
                && bytes[0].is_ascii_lowercase()
                && bytes
                    .iter()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_catalog_follows_the_grammar_and_has_no_duplicates() {
        let all: Vec<&str> = IDENTITY_PERMISSIONS
            .iter()
            .chain(HOST_PERMISSIONS.iter())
            .copied()
            .collect();
        assert!(all.iter().all(|p| valid_permission(p)), "{all:?}");
        let unique: std::collections::BTreeSet<_> = all.iter().collect();
        assert_eq!(unique.len(), all.len());
        assert!(HOST_PERMISSIONS.contains(&"system.status.read"));
        for bad in [
            "tasks",
            "a.b.c.d.e",
            "Tasks.create",
            "tasks.",
            "tasks.1x",
            "tasks.*",
        ] {
            assert!(!valid_permission(bad), "{bad}");
        }
    }
}
