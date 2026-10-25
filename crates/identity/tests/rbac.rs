//! RBAC through the real host path (acceptance 2, 3, 6, 7 and 10): the
//! permission matrix of every built-in, declared and custom role at the
//! four scopes, rendered into `docs/design/identity-permissions.md`; the
//! escalation rule on every grant path; the owner invariant; the scope
//! constraints of migration 2; an application without organizations; and
//! the cross-organization custom role attack.
mod support;
use serde_json::json;
use std::collections::BTreeSet;
use support::*;

/// Declared roles and builtin grants of these tests.
fn rbac_section(extra: &str) -> String {
    section(
        extra,
        &[],
        "[modules.identity.rbac.application_roles.support]\nname = \"Support\"\n\
         permissions = [\"identity.users.read\", \"identity.organization.enter\", \"tasks.read\"]\n\
         [modules.identity.rbac.application_roles.assigner]\nname = \"Assigner\"\n\
         permissions = [\"identity.roles.assign\", \"identity.roles.read\", \"identity.users.manage\", \"identity.users.read\"]\n\
         [modules.identity.rbac.application_roles.editor]\nname = \"Editor\"\n\
         permissions = [\"tasks.*\", \"content.entries.read\"]\n",
    )
}

const APP_SAMPLE: [&str; 5] = [
    "tasks.read",
    "tasks.delete",
    "content.entries.read",
    "system.status.read",
    "jobs.admin.read",
];
fn permissions() -> Vec<&'static str> {
    merezha_identity::catalog::IDENTITY_PERMISSIONS
        .iter()
        .copied()
        .chain(APP_SAMPLE)
        .collect()
}
const SCOPES: [&str; 4] = [
    "application",
    "tenant",
    "tenant resource",
    "application resource",
];

/// The expected allows of each role, written out by hand from section 5.
fn expected(role: &str, scope: &str) -> BTreeSet<&'static str> {
    let application_only = [
        "identity.users.read",
        "identity.users.manage",
        "identity.organization.enter",
    ];
    let tenant = matches!(scope, "tenant" | "tenant resource");
    let all: BTreeSet<&str> = permissions().into_iter().collect();
    match role {
        "platform_admin" => all,
        "owner" if tenant => all,
        "admin" if tenant => all
            .into_iter()
            .filter(|p| {
                !matches!(
                    *p,
                    "identity.organization.delete" | "identity.organization.transfer"
                ) && !application_only.contains(p)
            })
            .collect(),
        "member" if tenant => [
            "identity.organization.read",
            "identity.members.read",
            "identity.roles.read",
            "identity.api_keys.create",
        ]
        .into_iter()
        .collect(),
        // Application roles hold at every scope.
        "support" => [
            "identity.users.read",
            "identity.organization.enter",
            "tasks.read",
        ]
        .into_iter()
        .collect(),
        "editor" => ["tasks.read", "tasks.delete", "content.entries.read"]
            .into_iter()
            .collect(),
        "custom" if tenant => ["tasks.read", "identity.members.read"]
            .into_iter()
            .collect(),
        _ => BTreeSet::new(),
    }
}

/// Acceptance 2: the permission matrix, asserted cell by cell through the
/// host's `Authorizer` and rendered into the docs.
// Multi-threaded: the host `Authorizer` writes a break-glass audit in
// place before allowing the use (`block_in_place`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_permission_matrix_holds_and_is_documented() {
    let Some(world) = World::new(&rbac_section("")).await else {
        return;
    };
    let i = world.instance().await;
    let mut owner = user(&world, &i, "owner@example.com").await;
    let org = organization(&i, &mut owner, "acme").await;
    let custom = owner
        .post(&i, &format!("/organizations/{org}/roles"), json!({"key": "custom", "name": "Custom", "permissions": ["tasks.read", "identity.members.read"]}))
        .await;
    let custom_role = id_at(&custom, "/role/id");
    // One user per role, each active in the organization.
    let mut holders: Vec<(&str, Browser)> = Vec::new();
    for (role, role_id) in [
        ("admin", ADMIN_ROLE),
        ("member", MEMBER_ROLE),
        ("custom", custom_role.as_str()),
    ] {
        let email = format!("{role}@example.com");
        let mut browser = user(&world, &i, &email).await;
        join(
            &world,
            &i,
            &mut owner,
            &org,
            &mut browser,
            &email,
            &[role_id],
        )
        .await;
        holders.push((role, browser));
    }
    let roles = owner.get(&i, "/application/roles").await;
    assert_eq!(roles.status, 403, "an owner holds no application grant");
    holders.insert(0, ("owner", owner));
    // Application roles, assigned with the platform admin's break-glass.
    let mut root = user(&world, &i, "root@example.com").await;
    platform_admin(&i, "root@example.com").await;
    let listed = root.get(&i, "/application/roles").await;
    let role_id = |key: &str| {
        listed.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["key"] == key)
            .unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    for role in ["support", "editor"] {
        let email = format!("{role}@example.com");
        let browser = user(&world, &i, &email).await;
        let account = root
            .get(&i, &format!("/application/users?email={email}"))
            .await;
        let user_id = account.body["items"][0]["id"].as_str().unwrap().to_owned();
        let assigned = root
            .post(
                &i,
                "/application/role-assignments",
                json!({"user_id": user_id, "role_id": role_id(role)}),
            )
            .await;
        assert_eq!(assigned.status, 201, "{assigned:?}");
        holders.push((role, browser));
    }
    holders.push(("platform_admin", root));
    let mut rows = Vec::new();
    for (role, browser) in &mut holders {
        if !matches!(*role, "support" | "editor" | "platform_admin") {
            browser.organization = Some(org.clone());
        }
        for scope in SCOPES {
            let target = match scope {
                "application" => "application".to_owned(),
                "tenant" => format!("tenant:{org}"),
                "tenant resource" => format!("resource:{org}:project:p1"),
                _ => "resource:-:content_type:article".to_owned(),
            };
            let mut allowed = BTreeSet::new();
            for permission in permissions() {
                if browser.allowed(&i, permission, &target).await {
                    allowed.insert(permission);
                }
            }
            assert_eq!(allowed, expected(role, scope), "{role} at {scope}");
            rows.push((role.to_string(), scope, allowed));
        }
    }
    // Render the matrix.
    let mut text = String::from(
        "# Identity permission matrix\n\nGenerated by `crates/identity/tests/rbac.rs::the_permission_matrix_holds_and_is_documented`\n\
         (`MEREZHA_WRITE_PERMISSION_MATRIX=1` rewrites it). Each cell lists the scopes where the role\n\
         allows the permission: `A` application, `T` its tenant, `R` a resource in that tenant,\n\
         `AR` an application-level resource. `custom` is a custom role with `tasks.read` and\n\
         `identity.members.read`; `support` and `editor` are declared application roles\n\
         (`support`: `identity.users.read`, `identity.organization.enter`, `tasks.read`;\n\
         `editor`: `tasks.*`, `content.entries.read`); the implicit `user` grants nothing here.\n\
         No tenant grant satisfies an application check.\n\n",
    );
    let order = [
        "platform_admin",
        "owner",
        "admin",
        "member",
        "custom",
        "support",
        "editor",
    ];
    text.push_str("| Permission |");
    for role in order {
        text.push_str(&format!(" `{role}` |"));
    }
    text.push_str("\n| --- |");
    for _ in order {
        text.push_str(" --- |");
    }
    text.push('\n');
    for permission in permissions() {
        text.push_str(&format!("| `{permission}` |"));
        for role in order {
            let cells: Vec<&str> = SCOPES
                .iter()
                .zip(["A", "T", "R", "AR"])
                .filter(|(scope, _)| {
                    rows.iter().any(|(r, s, allowed)| {
                        r == role && s == *scope && allowed.contains(permission)
                    })
                })
                .map(|(_, short)| short)
                .collect();
            let cell = if cells.is_empty() {
                "-".to_owned()
            } else {
                cells.join(" ")
            };
            text.push_str(&format!(" {cell} |"));
        }
        text.push('\n');
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../docs/design/identity-permissions.md");
    if std::env::var_os("MEREZHA_WRITE_PERMISSION_MATRIX").is_some() {
        std::fs::write(&path, &text).unwrap();
    }
    assert_eq!(
        std::fs::read_to_string(&path).unwrap_or_default(),
        text,
        "docs/design/identity-permissions.md is stale: rerun with MEREZHA_WRITE_PERMISSION_MATRIX=1"
    );
}

/// Acceptance 3: no grant path gives a permission the actor lacks, and an
/// organization always keeps an owner.
#[tokio::test]
async fn no_grant_path_escalates_and_an_owner_always_remains() {
    let Some(world) = World::new(&rbac_section("")).await else {
        return;
    };
    let i = world.instance().await;
    let mut owner = user(&world, &i, "owner@example.com").await;
    let mut admin = user(&world, &i, "admin@example.com").await;
    let mut member = user(&world, &i, "member@example.com").await;
    let org = organization(&i, &mut owner, "acme").await;
    let admin_m = join(
        &world,
        &i,
        &mut owner,
        &org,
        &mut admin,
        "admin@example.com",
        &[ADMIN_ROLE],
    )
    .await;
    let member_m = join(
        &world,
        &i,
        &mut owner,
        &org,
        &mut member,
        "member@example.com",
        &[MEMBER_ROLE],
    )
    .await;
    let owner_m = membership_of(&i, &mut owner, &org, "owner@example.com").await;
    let escalation = |answer: &Answer| {
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (403, "permission_escalation"),
            "{answer:?}"
        )
    };
    // Custom role creation and edition.
    escalation(&admin.post(&i, &format!("/organizations/{org}/roles"), json!({"key": "boss", "name": "Boss", "permissions": ["identity.organization.delete"]})).await);
    let fine = admin
        .post(
            &i,
            &format!("/organizations/{org}/roles"),
            json!({"key": "reader", "name": "Reader", "permissions": ["tasks.read"]}),
        )
        .await;
    assert_eq!(fine.status, 201);
    let reader = id_at(&fine, "/role/id");
    escalation(
        &admin
            .patch(
                &i,
                &format!("/organizations/{org}/roles/{reader}"),
                json!({"permissions": ["tasks.read", "identity.organization.transfer"]}),
            )
            .await,
    );
    // An owner's role with a permission the admin lacks: the admin can
    // neither edit, delete nor assign it.
    let strong = owner.post(&i, &format!("/organizations/{org}/roles"), json!({"key": "strong", "name": "Strong", "permissions": ["identity.organization.delete"]})).await;
    let strong = id_at(&strong, "/role/id");
    escalation(
        &admin
            .patch(
                &i,
                &format!("/organizations/{org}/roles/{strong}"),
                json!({"name": "Mine"}),
            )
            .await,
    );
    escalation(
        &admin
            .delete(&i, &format!("/organizations/{org}/roles/{strong}"))
            .await,
    );
    escalation(
        &admin
            .post(
                &i,
                &format!("/organizations/{org}/role-assignments"),
                json!({"membership_id": admin_m, "role_id": strong}),
            )
            .await,
    );
    escalation(&admin.post(&i, &format!("/organizations/{org}/role-assignments"), json!({"membership_id": member_m, "role_id": strong, "resource": {"kind": "project", "id": "p1"}})).await);
    // Only owners assign owner, by every path.
    escalation(
        &admin
            .post(
                &i,
                &format!("/organizations/{org}/role-assignments"),
                json!({"membership_id": admin_m, "role_id": OWNER_ROLE}),
            )
            .await,
    );
    escalation(
        &admin
            .patch(
                &i,
                &format!("/organizations/{org}/members/{member_m}"),
                json!({"role_ids": [OWNER_ROLE]}),
            )
            .await,
    );
    escalation(
        &admin
            .post(
                &i,
                &format!("/organizations/{org}/invitations"),
                json!({"email": "x@example.com", "role_ids": [OWNER_ROLE]}),
            )
            .await,
    );
    escalation(
        &admin
            .post(
                &i,
                &format!("/organizations/{org}/invitations"),
                json!({"email": "x@example.com", "role_ids": [strong]}),
            )
            .await,
    );
    // Nor demote one, nor remove one.
    escalation(
        &admin
            .patch(
                &i,
                &format!("/organizations/{org}/members/{owner_m}"),
                json!({"role_ids": [MEMBER_ROLE]}),
            )
            .await,
    );
    escalation(
        &admin
            .delete(&i, &format!("/organizations/{org}/members/{owner_m}"))
            .await,
    );
    let transfer = admin
        .post(
            &i,
            &format!("/organizations/{org}/ownership-transfers"),
            json!({"membership_id": admin_m}),
        )
        .await;
    assert_eq!(
        (transfer.status.as_u16(), transfer.code()),
        (403, "unauthorized")
    );
    // A member cannot grant itself anything, nor mint a key above itself.
    let assign = member
        .post(
            &i,
            &format!("/organizations/{org}/role-assignments"),
            json!({"membership_id": member_m, "role_id": ADMIN_ROLE}),
        )
        .await;
    assert_eq!(assign.code(), "unauthorized");
    escalation(&member.post(&i, &format!("/organizations/{org}/api-keys"), json!({"name": "k", "permissions": ["identity.members.invite"], "expires_at": "2027-06-01T00:00:00Z"})).await);
    let own = member.post(&i, &format!("/organizations/{org}/api-keys"), json!({"name": "k", "permissions": ["identity.members.read"], "expires_at": "2027-06-01T00:00:00Z"})).await;
    assert_eq!(own.status, 201, "{own:?}");
    // A service account's key never exceeds its minter.
    let service = admin
        .post(
            &i,
            &format!("/organizations/{org}/service-accounts"),
            json!({"name": "CI"}),
        )
        .await;
    let service = id_at(&service, "/service_account/id");
    escalation(&admin.post(&i, &format!("/organizations/{org}/api-keys"), json!({"name": "k", "permissions": ["identity.organization.delete"], "expires_at": "2027-06-01T00:00:00Z", "service_account_id": service})).await);
    // ... nor does the owner hand the service account owner through it.
    escalation(
        &admin
            .post(
                &i,
                &format!("/organizations/{org}/role-assignments"),
                json!({"service_account_id": service, "role_id": OWNER_ROLE}),
            )
            .await,
    );
    // Nothing changed: the admin is still no owner, the member still a member.
    admin.organization = Some(org.clone());
    assert!(
        !admin
            .allowed(&i, "identity.organization.delete", &format!("tenant:{org}"))
            .await
    );
    member.organization = Some(org.clone());
    assert!(
        !member
            .allowed(&i, "identity.members.invite", &format!("tenant:{org}"))
            .await
    );
    // Application paths: an assigner without `tasks.*` cannot assign the
    // editor role, never platform_admin, and cannot disable a stronger user.
    user(&world, &i, "root@example.com").await;
    platform_admin(&i, "root@example.com").await;
    let mut root2 = sign_in(&i, "root@example.com").await;
    let roles = root2.get(&i, "/application/roles").await;
    let id_of = |key: &str| {
        roles.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["key"] == key)
            .unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let users = root2.get(&i, "/application/users?limit=100").await;
    let user_id = |email: &str| {
        users.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|u| u["email"] == email)
            .unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let assigned = root2
        .post(
            &i,
            "/application/role-assignments",
            json!({"user_id": user_id("admin@example.com"), "role_id": id_of("assigner")}),
        )
        .await;
    assert_eq!(assigned.status, 201, "{assigned:?}");
    admin.organization = None;
    let mut assigner = sign_in(&i, "admin@example.com").await;
    escalation(
        &assigner
            .post(
                &i,
                "/application/role-assignments",
                json!({"user_id": user_id("member@example.com"), "role_id": id_of("editor")}),
            )
            .await,
    );
    let builtin = assigner
        .post(
            &i,
            "/application/role-assignments",
            json!({"user_id": user_id("member@example.com"), "role_id": PLATFORM_ADMIN_ROLE}),
        )
        .await;
    assert_eq!(
        (builtin.status.as_u16(), builtin.code()),
        (403, "builtin_role")
    );
    let builtin = root2
        .post(
            &i,
            "/application/role-assignments",
            json!({"user_id": user_id("member@example.com"), "role_id": PLATFORM_ADMIN_ROLE}),
        )
        .await;
    assert_eq!(
        builtin.code(),
        "builtin_role",
        "not even break-glass assigns it over HTTP"
    );
    escalation(
        &assigner
            .patch(
                &i,
                &format!("/application/users/{}", user_id("root@example.com")),
                json!({"status": "disabled"}),
            )
            .await,
    );
    root2
        .post(
            &i,
            "/application/role-assignments",
            json!({"user_id": user_id("member@example.com"), "role_id": id_of("support")}),
        )
        .await;
    escalation(
        &assigner
            .patch(
                &i,
                &format!("/application/users/{}", user_id("member@example.com")),
                json!({"status": "disabled"}),
            )
            .await,
    );
    // Holding every permission is not being an owner: a member whose
    // custom role lists the whole catalog still cannot hand out `owner`.
    let catalog = owner.get(&i, "/permissions").await;
    let everything: Vec<String> = catalog.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["name"].as_str().unwrap().to_owned())
        .collect();
    let all = owner
        .post(
            &i,
            &format!("/organizations/{org}/roles"),
            json!({"key": "all", "name": "All", "permissions": everything}),
        )
        .await;
    assert_eq!(all.status, 201, "{all:?}");
    let all = id_at(&all, "/role/id");
    owner
        .patch(
            &i,
            &format!("/organizations/{org}/members/{member_m}"),
            json!({"role_ids": [MEMBER_ROLE, all]}),
        )
        .await;
    member.organization = None;
    escalation(
        &member
            .post(
                &i,
                &format!("/organizations/{org}/role-assignments"),
                json!({"membership_id": member_m, "role_id": OWNER_ROLE}),
            )
            .await,
    );
    escalation(
        &member
            .patch(
                &i,
                &format!("/organizations/{org}/members/{admin_m}"),
                json!({"role_ids": [OWNER_ROLE]}),
            )
            .await,
    );
    escalation(
        &member
            .post(
                &i,
                &format!("/organizations/{org}/invitations"),
                json!({"email": "y@example.com", "role_ids": [OWNER_ROLE]}),
            )
            .await,
    );
    escalation(
        &member
            .delete(&i, &format!("/organizations/{org}/members/{owner_m}"))
            .await,
    );
    // The owner invariant.
    let sole = owner
        .patch(
            &i,
            &format!("/organizations/{org}/members/{owner_m}"),
            json!({"role_ids": [ADMIN_ROLE]}),
        )
        .await;
    assert_eq!((sole.status.as_u16(), sole.code()), (409, "sole_owner"));
    let assignments = owner
        .get(
            &i,
            &format!("/organizations/{org}/role-assignments?membership_id={owner_m}"),
        )
        .await;
    let owner_assignment = assignments.body["items"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let sole = owner
        .delete(
            &i,
            &format!("/organizations/{org}/role-assignments/{owner_assignment}"),
        )
        .await;
    assert_eq!(sole.code(), "sole_owner");
    let sole = owner
        .delete(&i, &format!("/organizations/{org}/members/{owner_m}"))
        .await;
    assert_eq!(sole.code(), "sole_owner");
    // With a second owner the first may step down.
    let promoted = owner
        .patch(
            &i,
            &format!("/organizations/{org}/members/{admin_m}"),
            json!({"role_ids": [OWNER_ROLE]}),
        )
        .await;
    assert_eq!(promoted.status, 200, "{promoted:?}");
    let stepped = owner
        .patch(
            &i,
            &format!("/organizations/{org}/members/{owner_m}"),
            json!({"role_ids": [MEMBER_ROLE]}),
        )
        .await;
    assert_eq!(stepped.status, 200, "{stepped:?}");
    let owners: i64 = world
        .scalar(
            "SELECT count(*) FROM merezha_identity.role_assignments WHERE builtin_role = 'owner'",
        )
        .await;
    assert_eq!(owners, 1);
}

/// Acceptance 10: X owns A and is admin in B. Assigning or inviting with
/// A's custom role in B is `404 not_found`, and editing it in A changes no
/// grant in B.
#[tokio::test]
async fn a_custom_role_of_one_organization_grants_nothing_in_another() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let i = world.instance().await;
    let mut x = user(&world, &i, "x@example.com").await;
    let mut y = user(&world, &i, "y@example.com").await;
    let a = organization(&i, &mut x, "alpha").await;
    let b = organization(&i, &mut y, "beta").await;
    let x_in_b = join(
        &world,
        &i,
        &mut y,
        &b,
        &mut x,
        "x@example.com",
        &[ADMIN_ROLE],
    )
    .await;
    // R in A with permissions X holds in B, so the escalation check passes.
    let role = x
        .post(
            &i,
            &format!("/organizations/{a}/roles"),
            json!({"key": "r", "name": "R", "permissions": ["tasks.read"]}),
        )
        .await;
    let r = id_at(&role, "/role/id");
    for (method, path, body) in [
        (
            "POST",
            format!("/organizations/{b}/role-assignments"),
            json!({"membership_id": x_in_b, "role_id": r}),
        ),
        (
            "POST",
            format!("/organizations/{b}/role-assignments"),
            json!({"membership_id": x_in_b, "role_id": r, "resource": {"kind": "project", "id": "p1"}}),
        ),
        (
            "POST",
            format!("/organizations/{b}/invitations"),
            json!({"email": "z@example.com", "role_ids": [r]}),
        ),
        (
            "PATCH",
            format!("/organizations/{b}/members/{x_in_b}"),
            json!({"role_ids": [ADMIN_ROLE, r]}),
        ),
        (
            "PATCH",
            format!("/organizations/{b}/roles/{r}"),
            json!({"permissions": ["identity.organization.delete"]}),
        ),
        ("DELETE", format!("/organizations/{b}/roles/{r}"), json!({})),
    ] {
        let answer = x.send(&i, method, &path, Some(body)).await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (404, "not_found"),
            "{method} {path}"
        );
    }
    // R is not listed in B.
    let listed = x.get(&i, &format!("/organizations/{b}/roles")).await;
    assert!(
        listed.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .all(|role| role["id"] != r.as_str())
    );
    // Editing R in A, where X is owner, to owner-like power changes no
    // grant in B.
    let edited = x.patch(&i, &format!("/organizations/{a}/roles/{r}"), json!({"permissions": ["tasks.read", "identity.organization.delete", "identity.organization.transfer"]})).await;
    assert_eq!(edited.status, 200, "{edited:?}");
    x.organization = Some(b.clone());
    assert!(
        !x.allowed(&i, "identity.organization.delete", &format!("tenant:{b}"))
            .await
    );
    assert!(
        !x.allowed(&i, "identity.organization.transfer", &format!("tenant:{b}"))
            .await
    );
    let delete = x
        .delete_with(
            &i,
            &format!("/organizations/{b}"),
            json!({"confirm_slug": "beta"}),
        )
        .await;
    assert_eq!(
        (delete.status.as_u16(), delete.code()),
        (403, "unauthorized")
    );
    // The database refuses the row outright.
    let refused = world
        .try_execute(&format!(
            "INSERT INTO merezha_identity.role_assignments (id, organization_id, membership_id, custom_role_id, created_at)
             VALUES (gen_random_uuid(), '{}', '{}', '{}', now())",
            hex(&b), hex(&x_in_b), hex(&r)
        ))
        .await;
    assert_eq!(refused, Err("23503".to_owned()));
}

/// Acceptance 6: the scope constraints of migration 2 reject every
/// forbidden row shape.
#[tokio::test]
async fn the_scope_constraints_reject_every_forbidden_row() {
    let Some(world) = World::new(&rbac_section("")).await else {
        return;
    };
    let i = world.instance().await;
    let mut x = user(&world, &i, "x@example.com").await;
    let mut y = user(&world, &i, "y@example.com").await;
    let a = organization(&i, &mut x, "alpha").await;
    let b = organization(&i, &mut y, "beta").await;
    let (a, b) = (hex(&a), hex(&b));
    let role = x
        .post(
            &i,
            &format!("/organizations/org_{a}/roles"),
            json!({"key": "r", "name": "R", "permissions": ["tasks.read"]}),
        )
        .await;
    let r = hex(&id_at(&role, "/role/id"));
    let x_in_a = hex(&membership_of(&i, &mut x, &format!("org_{a}"), "x@example.com").await);
    let y_in_b = hex(&membership_of(&i, &mut y, &format!("org_{b}"), "y@example.com").await);
    world.execute(&format!(
        "INSERT INTO merezha_identity.service_accounts (id, scope, organization_id, name, created_at, updated_at)
         VALUES ('00000000-0000-7000-8000-0000000000a1', 'tenant', '{a}', 'tenant sa', now(), now()),
                ('00000000-0000-7000-8000-0000000000a2', 'application', NULL, 'app sa', now(), now())"
    ))
    .await;
    world.execute(&format!(
        "INSERT INTO merezha_identity.invitations (id, organization_id, email, email_normalized, token_hash, created_at, expires_at)
         VALUES ('00000000-0000-7000-8000-0000000000b1', '{b}', 'z@example.com', 'z@example.com', decode(repeat('ab', 32), 'hex'), now(), now() + interval '1 day')"
    ))
    .await;
    let declared: String = {
        let client = world.db.acquire(soon()).await.unwrap();
        client
            .query_one("SELECT replace(id::text, '-', '') FROM merezha_identity.roles WHERE key = 'support'", &[])
            .await
            .unwrap()
            .get(0)
    };
    let user_x = {
        let client = world.db.acquire(soon()).await.unwrap();
        let id: uuid::Uuid = client
            .query_one(
                "SELECT id FROM merezha_identity.users WHERE email_normalized = 'x@example.com'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        id.simple().to_string()
    };
    let cases = [
        // A's custom role in a row of B.
        (format!("INSERT INTO merezha_identity.role_assignments (id, organization_id, membership_id, custom_role_id, created_at) VALUES (gen_random_uuid(), '{b}', '{y_in_b}', '{r}', now())"), "23503"),
        // A membership of A in a row of B.
        (format!("INSERT INTO merezha_identity.role_assignments (id, organization_id, membership_id, builtin_role, created_at) VALUES (gen_random_uuid(), '{b}', '{x_in_a}', 'member', now())"), "23503"),
        // A tenant service account of A in a row of B.
        (format!("INSERT INTO merezha_identity.role_assignments (id, organization_id, service_account_id, builtin_role, created_at) VALUES (gen_random_uuid(), '{b}', '00000000-0000-7000-8000-0000000000a1', 'member', now())"), "23503"),
        // An application service account in a tenant row.
        (format!("INSERT INTO merezha_identity.role_assignments (id, organization_id, service_account_id, builtin_role, created_at) VALUES (gen_random_uuid(), '{a}', '00000000-0000-7000-8000-0000000000a2', 'member', now())"), "23503"),
        // Both or neither holder, both or neither role.
        (format!("INSERT INTO merezha_identity.role_assignments (id, organization_id, membership_id, service_account_id, builtin_role, created_at) VALUES (gen_random_uuid(), '{a}', '{x_in_a}', '00000000-0000-7000-8000-0000000000a1', 'member', now())"), "23514"),
        (format!("INSERT INTO merezha_identity.role_assignments (id, organization_id, membership_id, builtin_role, custom_role_id, created_at) VALUES (gen_random_uuid(), '{a}', '{x_in_a}', 'member', '{r}', now())"), "23514"),
        (format!("INSERT INTO merezha_identity.role_assignments (id, organization_id, membership_id, created_at) VALUES (gen_random_uuid(), '{a}', '{x_in_a}', now())"), "23514"),
        // A resource kind without an id.
        (format!("INSERT INTO merezha_identity.role_assignments (id, organization_id, membership_id, builtin_role, resource_kind, created_at) VALUES (gen_random_uuid(), '{a}', '{x_in_a}', 'member', 'project', now())"), "23514"),
        // A tenant role at application scope.
        (format!("INSERT INTO merezha_identity.application_role_assignments (id, user_id, role_id, created_at) VALUES (gen_random_uuid(), '{user_x}', '00000000-0000-7000-8000-000000000001', now())"), "23503"),
        (format!("INSERT INTO merezha_identity.application_role_assignments (id, user_id, role_id, created_at) VALUES (gen_random_uuid(), '{user_x}', '{r}', now())"), "23503"),
        // A tenant service account at application scope.
        (format!("INSERT INTO merezha_identity.application_role_assignments (id, service_account_id, role_id, created_at) VALUES (gen_random_uuid(), '00000000-0000-7000-8000-0000000000a1', '{declared}', now())"), "23503"),
        // platform_admin to a service account, or on a resource.
        ("INSERT INTO merezha_identity.application_role_assignments (id, service_account_id, role_id, created_at) VALUES (gen_random_uuid(), '00000000-0000-7000-8000-0000000000a2', '00000000-0000-7000-8000-000000000004', now())".to_owned(), "23514"),
        (format!("INSERT INTO merezha_identity.application_role_assignments (id, user_id, role_id, resource_kind, resource_id, created_at) VALUES (gen_random_uuid(), '{user_x}', '00000000-0000-7000-8000-000000000004', 'content_type', 'article', now())"), "23514"),
        // A's custom role in an invitation of B.
        (format!("INSERT INTO merezha_identity.invitation_roles (invitation_id, organization_id, custom_role_id) VALUES ('00000000-0000-7000-8000-0000000000b1', '{b}', '{r}')"), "23503"),
        // A custom role at application scope, a shared role with permissions.
        (format!("INSERT INTO merezha_identity.roles (id, scope, organization_id, key, name, created_at, updated_at) VALUES (gen_random_uuid(), 'application', '{a}', 'k', 'K', now(), now())"), "23514"),
        ("INSERT INTO merezha_identity.roles (id, scope, key, name, permissions, created_at, updated_at) VALUES (gen_random_uuid(), 'application', 'k2', 'K', '{tasks.read}', now(), now())".to_owned(), "23514"),
        // A service account whose scope and organization disagree.
        (format!("INSERT INTO merezha_identity.service_accounts (id, scope, organization_id, name, created_at, updated_at) VALUES (gen_random_uuid(), 'application', '{a}', 's', now(), now())"), "23514"),
        // A key with two owners.
        (format!("INSERT INTO merezha_identity.api_keys (id, public_id, secret_hash, user_id, service_account_id, name, permissions, created_at, expires_at) VALUES (gen_random_uuid(), 'abcdefghijklmnop', decode(repeat('ab', 32), 'hex'), '{user_x}', '00000000-0000-7000-8000-0000000000a2', 'k', '{{tasks.read}}', now(), now())"), "23514"),
    ];
    for (sql, code) in &cases {
        assert_eq!(
            world.try_execute(sql).await,
            Err((*code).to_owned()),
            "{sql}"
        );
    }
    // A role still assigned cannot be deleted; its organization can.
    world.execute(&format!(
        "INSERT INTO merezha_identity.role_assignments (id, organization_id, membership_id, custom_role_id, created_at) VALUES (gen_random_uuid(), '{a}', '{x_in_a}', '{r}', now())"
    ))
    .await;
    assert_eq!(
        world
            .try_execute(&format!(
                "DELETE FROM merezha_identity.roles WHERE id = '{r}'"
            ))
            .await,
        Err("23503".to_owned())
    );
    assert_eq!(
        world
            .try_execute(&format!(
                "DELETE FROM merezha_identity.organizations WHERE id = '{a}'"
            ))
            .await,
        Ok(())
    );
}

/// Acceptance 7: with `user_can_create = false` the application works on
/// the implicit `user` role and declared roles alone.
#[tokio::test]
async fn an_application_without_organizations_works_on_application_roles() {
    let Some(world) = World::new(&section(
        "",
        &[],
        "[modules.identity.organizations]\nuser_can_create = false\n\
         [modules.identity.rbac.builtin_grants]\nuser = [\"tasks.read\", \"identity.api_keys.create\"]\n\
         [modules.identity.rbac.application_roles.editor]\nname = \"Editor\"\npermissions = [\"tasks.*\"]\n",
    ))
    .await
    else {
        return;
    };
    let i = world.instance().await;
    let mut ada = user(&world, &i, "ada@example.com").await;
    let refused = ada
        .post(
            &i,
            "/organizations",
            json!({"name": "Acme", "slug": "acme"}),
        )
        .await;
    assert_eq!(
        (refused.status.as_u16(), refused.code()),
        (403, "unauthorized")
    );
    // The implicit `user` role, at every scope.
    assert!(ada.allowed(&i, "tasks.read", "application").await);
    assert!(
        ada.allowed(&i, "tasks.read", "resource:-:content_type:article")
            .await
    );
    assert!(!ada.allowed(&i, "tasks.delete", "application").await);
    let mine = ada.get(&i, "/me/permissions").await;
    assert_eq!(mine.body["application"]["roles"], json!(["user"]));
    assert_eq!(
        mine.body["application"]["permissions"],
        json!(["identity.api_keys.create", "tasks.read"])
    );
    // A platform admin assigns the declared role, application-wide and on
    // one application-level resource.
    user(&world, &i, "root@example.com").await;
    platform_admin(&i, "root@example.com").await;
    let mut root = sign_in(&i, "root@example.com").await;
    let roles = root.get(&i, "/application/roles").await;
    let editor = roles.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["key"] == "editor")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let users = root
        .get(&i, "/application/users?email=ada%40example.com")
        .await;
    let ada_id = users.body["items"][0]["id"].as_str().unwrap().to_owned();
    let on_article = root
        .post(&i, "/application/role-assignments", json!({"user_id": ada_id, "role_id": editor, "resource": {"kind": "content_type", "id": "article"}}))
        .await;
    assert_eq!(on_article.status, 201, "{on_article:?}");
    assert!(
        ada.allowed(&i, "tasks.delete", "resource:-:content_type:article")
            .await
    );
    assert!(
        !ada.allowed(&i, "tasks.delete", "resource:-:content_type:page")
            .await
    );
    assert!(!ada.allowed(&i, "tasks.delete", "application").await);
    let everywhere = root
        .post(
            &i,
            "/application/role-assignments",
            json!({"user_id": ada_id, "role_id": editor}),
        )
        .await;
    assert_eq!(everywhere.status, 201);
    assert!(ada.allowed(&i, "tasks.delete", "application").await);
    // An application key and an application service account.
    let key = ada.post(&i, "/application/api-keys", json!({"name": "cli", "permissions": ["tasks.read"], "expires_at": "2027-06-01T00:00:00Z"})).await;
    assert_eq!(key.status, 201, "{key:?}");
    let mut cli = Browser::new("198.51.100.60");
    cli.bearer = Some(key.body["key"].as_str().unwrap().to_owned());
    assert!(cli.allowed(&i, "tasks.read", "application").await);
    assert!(
        !cli.allowed(&i, "tasks.delete", "application").await,
        "the key's list limits it"
    );
    let service = root
        .post(
            &i,
            "/application/service-accounts",
            json!({"name": "importer"}),
        )
        .await;
    let service = id_at(&service, "/service_account/id");
    let assigned = root
        .post(
            &i,
            "/application/role-assignments",
            json!({"service_account_id": service, "role_id": editor}),
        )
        .await;
    assert_eq!(assigned.status, 201, "{assigned:?}");
    let machine = root.post(&i, "/application/api-keys", json!({"name": "import", "permissions": ["tasks.create"], "expires_at": "2027-06-01T00:00:00Z", "service_account_id": service})).await;
    assert_eq!(machine.status, 201, "{machine:?}");
    let mut importer = Browser::new("198.51.100.61");
    importer.bearer = Some(machine.body["key"].as_str().unwrap().to_owned());
    assert!(importer.allowed(&i, "tasks.create", "application").await);
    assert!(!importer.allowed(&i, "tasks.read", "application").await);
    assert_eq!(
        importer.get(&i, "/me").await.status,
        401,
        "a service account is no user"
    );
    let listed = root
        .get(
            &i,
            &format!("/application/role-assignments?user_id={ada_id}"),
        )
        .await;
    assert_eq!(listed.body["items"].as_array().unwrap().len(), 2);
    let audit = root
        .get(
            &i,
            "/application/audit-events?action=role_assignment.created",
        )
        .await;
    assert_eq!(audit.body["items"].as_array().unwrap().len(), 3);
    let changed: i64 = world
        .scalar("SELECT count(*) FROM merezha_jobs.outbox WHERE topic = 'identity.user_application_roles_changed'")
        .await;
    assert_eq!(changed, 3, "two editor assignments and platform_admin");
}
