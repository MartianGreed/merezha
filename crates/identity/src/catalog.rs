//! The permission catalog (section 5): identity's own permissions, those
//! the host checks itself, the other blocks' lists (such as
//! `merezha_content::CONTENT_PERMISSIONS`) and the application's
//! `[[permissions]]`. Grants name catalog entries or trailing wildcards,
//! expanded against the catalog when a snapshot is built, so a check is one
//! hash lookup. Names outside the catalog grant nothing.
use merezha_domain::Identity;
use merezha_platform::ports::PermissionSet;
use std::collections::BTreeMap;

/// Identity's own permissions, checked at `Tenant` under
/// `/organizations/` and at `Application` under `/application/`.
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
/// What each identity permission allows, for `GET /v1/identity/permissions`.
const IDENTITY_DESCRIPTIONS: [&str; 20] = [
    "Read the organization",
    "Rename the organization or change its slug",
    "Delete the organization",
    "Transfer the organization's ownership",
    "List members",
    "Invite members",
    "Change members' roles",
    "Remove members",
    "Read roles and role assignments",
    "Create, edit and delete custom roles",
    "Assign and unassign roles",
    "List service accounts",
    "Create, edit and delete service accounts and their keys",
    "List API keys",
    "Create one's own API keys",
    "Revoke API keys",
    "Read the audit log",
    "List the application's users",
    "Enable and disable users",
    "Act in any organization without a membership",
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
const HOST_DESCRIPTIONS: [&str; 8] = [
    "Catch projections up",
    "Rebuild projections",
    "Read the application's administration API",
    "Delete projection revisions and abandon rebuilds",
    "Read jobs and the outbox",
    "Retry, cancel and replay jobs",
    "Read job payloads",
    "Read /v1/admin/status",
];

/// Block prefixes (AP §C.4). Application permissions must not use them,
/// and only operator configuration (`builtin_grants`) may grant a wildcard
/// inside them, so a later release adding a block permission never widens
/// a custom role, a declared role or an API key.
pub const BLOCK_PREFIXES: [&str; 6] = [
    "identity",
    "content",
    "notifications",
    "jobs",
    "application",
    "system",
];

/// Section 5's grammar: two to four segments of `[a-z][a-z0-9_]{0,31}`.
pub fn valid_permission(name: &str) -> bool {
    let segments: Vec<&str> = name.split('.').collect();
    (2..=4).contains(&segments.len()) && segments.iter().all(|s| valid_segment(s))
}
fn valid_segment(segment: &str) -> bool {
    let bytes = segment.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 32
        && bytes[0].is_ascii_lowercase()
        && bytes
            .iter()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
}
fn block_namespace(name: &str) -> bool {
    let first = name.split('.').next().unwrap_or("");
    BLOCK_PREFIXES.contains(&first)
}

/// Where wildcards may point in a grant list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wildcards {
    /// Operator configuration: any trailing wildcard (`tasks.*`,
    /// `content.*`).
    Any,
    /// Custom roles, declared application roles and API keys: trailing
    /// wildcards inside the application's own namespaces only.
    Application,
}

/// One catalog entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub description: String,
    /// `identity`, `host`, a block name or `application`.
    pub source: &'static str,
}

/// The union of every permission the host knows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Catalog {
    entries: BTreeMap<String, Entry>,
}
impl Default for Catalog {
    fn default() -> Self {
        Self::builtin()
    }
}
impl Catalog {
    /// Identity's and the host's permissions.
    pub fn builtin() -> Self {
        let mut entries = BTreeMap::new();
        for (name, description) in IDENTITY_PERMISSIONS.iter().zip(IDENTITY_DESCRIPTIONS) {
            entries.insert(
                (*name).to_owned(),
                Entry {
                    description: description.to_owned(),
                    source: "identity",
                },
            );
        }
        for (name, description) in HOST_PERMISSIONS.iter().zip(HOST_DESCRIPTIONS) {
            entries.insert(
                (*name).to_owned(),
                Entry {
                    description: description.to_owned(),
                    source: "host",
                },
            );
        }
        Self { entries }
    }
    /// Add a block's permissions (`source` is the block name). Each must
    /// follow the grammar and start with `source.`; fails with the first
    /// offending name.
    pub fn add_block(&mut self, source: &'static str, names: &[&str]) -> Result<(), String> {
        for name in names {
            if !valid_permission(name) || !name.starts_with(&format!("{source}.")) {
                return Err((*name).to_owned());
            }
            self.entries.entry((*name).to_owned()).or_insert(Entry {
                description: String::new(),
                source,
            });
        }
        Ok(())
    }
    /// Add an application permission (`[[permissions]]`). Fails
    /// `invalid_permission` off the grammar and `block_prefix` inside a
    /// block's namespace.
    pub fn add_application(&mut self, name: &str, description: &str) -> Result<(), &'static str> {
        if !valid_permission(name) {
            return Err("invalid_permission");
        }
        if block_namespace(name) {
            return Err("block_prefix");
        }
        self.entries.insert(
            name.to_owned(),
            Entry {
                description: description.to_owned(),
                source: "application",
            },
        );
        Ok(())
    }
    pub fn contains(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn entries(&self) -> impl Iterator<Item = (&str, &Entry)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }
    /// Check one grant: a catalog entry, or a trailing wildcard (`a.*`,
    /// `a.b.*`, `a.b.c.*`) matching at least one entry and allowed by
    /// `wildcards`. The bare `*` is never accepted here: only the built-in
    /// `owner` and `platform_admin` roles hold it. Fails
    /// `unknown_permission`.
    pub fn check_grant(&self, grant: &str, wildcards: Wildcards) -> Result<(), &'static str> {
        match grant.strip_suffix(".*") {
            Some(prefix) => {
                let segments: Vec<&str> = prefix.split('.').collect();
                let shaped =
                    (1..=3).contains(&segments.len()) && segments.iter().all(|s| valid_segment(s));
                let allowed = wildcards == Wildcards::Any || !block_namespace(prefix);
                let dotted = format!("{prefix}.");
                let matches = self.entries.keys().any(|k| k.starts_with(&dotted));
                if shaped && allowed && matches {
                    Ok(())
                } else {
                    Err("unknown_permission")
                }
            }
            None if self.contains(grant) => Ok(()),
            None => Err("unknown_permission"),
        }
    }
    /// The concrete permissions `grants` name: entries, `prefix.*` for
    /// every entry under `prefix.`, `*` for all. Names outside the catalog
    /// (a permission a later release removed) grant nothing.
    pub fn expand<'a>(&self, grants: impl IntoIterator<Item = &'a str>) -> PermissionSet {
        let mut out = Vec::new();
        for grant in grants {
            if grant == "*" {
                out.extend(self.entries.keys().cloned());
            } else if let Some(prefix) = grant.strip_suffix(".*") {
                let dotted = format!("{prefix}.");
                out.extend(
                    self.entries
                        .keys()
                        .filter(|k| k.starts_with(&dotted))
                        .cloned(),
                );
            } else if self.contains(grant) {
                out.push(grant.to_owned());
            }
        }
        out.into_iter()
            .filter_map(|name| Identity::new(name).ok())
            .collect()
    }
    /// Grants of `grants` that name nothing in the catalog.
    pub fn unknown<'a>(&self, grants: impl IntoIterator<Item = &'a str>) -> usize {
        grants
            .into_iter()
            .filter(|g| *g != "*" && self.check_grant(g, Wildcards::Any).is_err())
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(set: &PermissionSet) -> Vec<String> {
        let mut out: Vec<String> = set.iter().map(|p| p.as_str().to_owned()).collect();
        out.sort();
        out
    }

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
        assert_eq!(Catalog::builtin().len(), all.len());
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

    #[test]
    fn application_permissions_stay_out_of_block_namespaces() {
        let mut catalog = Catalog::builtin();
        assert_eq!(catalog.add_application("tasks.create", "Create"), Ok(()));
        for prefix in BLOCK_PREFIXES {
            assert_eq!(
                catalog.add_application(&format!("{prefix}.x"), ""),
                Err("block_prefix")
            );
        }
        assert_eq!(
            catalog.add_application("tasks", ""),
            Err("invalid_permission")
        );
        assert_eq!(
            catalog.add_block("content", &["content.entries.read"]),
            Ok(())
        );
        assert_eq!(
            catalog.add_block("content", &["identity.users.read"]),
            Err("identity.users.read".to_owned())
        );
    }

    #[test]
    fn wildcards_expand_against_the_catalog() {
        let mut catalog = Catalog::builtin();
        for name in [
            "tasks.read",
            "tasks.create",
            "tasks.admin.purge",
            "taskforce.x",
        ] {
            catalog.add_application(name, "").unwrap();
        }
        catalog
            .add_block(
                "content",
                &["content.entries.read", "content.entries.write"],
            )
            .unwrap();
        assert_eq!(
            names(&catalog.expand(["tasks.*"])),
            ["tasks.admin.purge", "tasks.create", "tasks.read"]
        );
        assert_eq!(
            names(&catalog.expand(["tasks.admin.*", "content.entries.read"])),
            ["content.entries.read", "tasks.admin.purge"]
        );
        assert_eq!(catalog.expand(["*"]).len(), catalog.len());
        // A removed or unknown name grants nothing.
        assert!(catalog.expand(["tasks.gone", "gone.*"]).is_empty());
        assert_eq!(catalog.unknown(["tasks.gone", "tasks.read", "*"]), 1);
    }

    #[test]
    fn grants_are_checked_against_the_catalog_and_their_wildcard_rule() {
        let mut catalog = Catalog::builtin();
        catalog.add_application("tasks.read", "").unwrap();
        catalog
            .add_block("content", &["content.entries.read"])
            .unwrap();
        let app = Wildcards::Application;
        assert_eq!(catalog.check_grant("tasks.read", app), Ok(()));
        assert_eq!(catalog.check_grant("tasks.*", app), Ok(()));
        assert_eq!(catalog.check_grant("identity.users.read", app), Ok(()));
        for bad in [
            "*",
            "content.*",
            "identity.*",
            "identity.members.*",
            "tasks.write",
            "nothing.*",
            "tasks.*.*",
            "a.b.c.d.*",
            "",
        ] {
            assert_eq!(
                catalog.check_grant(bad, app),
                Err("unknown_permission"),
                "{bad}"
            );
        }
        assert_eq!(catalog.check_grant("content.*", Wildcards::Any), Ok(()));
        assert_eq!(
            catalog.check_grant("*", Wildcards::Any),
            Err("unknown_permission")
        );
    }
}
