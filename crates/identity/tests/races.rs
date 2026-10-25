//! ID PR 2 concurrency and failure cases (section 10 test plan), and
//! acceptance 12: an anonymous invitation acceptance for an unverified
//! account's address claims it.
mod support;
use serde_json::json;
use support::*;

/// Acceptance 12.
#[tokio::test]
async fn an_invitation_claims_an_unverified_account() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let i = world.instance().await;
    // The attacker registers the victim's address and never verifies it.
    let mut attacker = Browser::new("203.0.113.66");
    let registered = attacker
        .post(
            &i,
            "/registrations",
            json!({"email": "victim@example.com", "password": "attacker chosen secret"}),
        )
        .await;
    assert_eq!(registered.status, 202);
    let mut owner = user(&world, &i, "owner@example.com").await;
    let org = organization(&i, &mut owner, "acme").await;
    owner
        .post(
            &i,
            &format!("/organizations/{org}/invitations"),
            json!({"email": "victim@example.com", "role_ids": [MEMBER_ROLE]}),
        )
        .await;
    let token = world
        .mail
        .token("identity.invitation", "victim@example.com");
    let mut victim = Browser::new("198.51.100.90");
    let inspected = victim
        .post(&i, "/invitations/inspect", json!({"token": token}))
        .await;
    assert_eq!(
        inspected.body["account_exists"], false,
        "an unverified account does not count"
    );
    let accepted = victim
        .post(
            &i,
            "/invitations/accept",
            json!({"token": token, "transport": "bearer"}),
        )
        .await;
    assert_eq!(accepted.status, 201, "{accepted:?}");
    assert!(
        accepted.body["session"]["token"]
            .as_str()
            .unwrap()
            .starts_with("mzs_")
    );
    assert_eq!(accepted.body["user"]["email_verified"], true);
    assert_eq!(accepted.body["user"]["credentials"]["password"], false);
    assert_eq!(accepted.body["session"]["method"], "invitation");
    // No attacker credential or session survives.
    assert_eq!(world.scalar("SELECT count(*) FROM merezha_identity.password_credentials p JOIN merezha_identity.users u ON u.id = p.user_id WHERE u.email_normalized = 'victim@example.com'").await, 0);
    assert_eq!(world.scalar("SELECT count(*) FROM merezha_identity.sessions s JOIN merezha_identity.users u ON u.id = s.user_id WHERE u.email_normalized = 'victim@example.com'").await, 1);
    assert_eq!(world.scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'user.unverified_account_claimed'").await, 1);
    let attempt = attacker
        .post(
            &i,
            "/sessions",
            json!({"email": "victim@example.com", "password": "attacker chosen secret"}),
        )
        .await;
    assert_eq!(attempt.code(), "invalid_credentials");
    // The victim is a member, with the invited role.
    let mine = victim
        .get(&i, &format!("/organizations/{org}/me/permissions"))
        .await;
    assert_eq!(mine.body["roles"], json!(["member"]));
    // An address without any account gets one, verified.
    owner
        .post(
            &i,
            &format!("/organizations/{org}/invitations"),
            json!({"email": "new@example.com", "role_ids": [MEMBER_ROLE]}),
        )
        .await;
    let token = world.mail.token("identity.invitation", "new@example.com");
    let created = Browser::new("198.51.100.91")
        .post(&i, "/invitations/accept", json!({"token": token}))
        .await;
    assert_eq!(created.status, 201, "{created:?}");
    assert!(created.set_cookie().is_some());
    assert_eq!(world.scalar("SELECT count(*) FROM merezha_jobs.outbox WHERE topic = 'identity.user_registered' AND payload->'payload'->>'method' = 'invitation'").await, 1);
}

#[tokio::test]
async fn two_owners_demoting_each_other_leave_one_owner() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let i = world.instance().await;
    for round in 0..5 {
        let mut a = user(&world, &i, &format!("a{round}@example.com")).await;
        let mut b = user(&world, &i, &format!("b{round}@example.com")).await;
        let org = organization(&i, &mut a, &format!("org{round}")).await;
        let b_m = join(
            &world,
            &i,
            &mut a,
            &org,
            &mut b,
            &format!("b{round}@example.com"),
            &[OWNER_ROLE],
        )
        .await;
        let a_m = membership_of(&i, &mut a, &org, &format!("a{round}@example.com")).await;
        let (to_b, to_a) = (
            format!("/organizations/{org}/members/{b_m}"),
            format!("/organizations/{org}/members/{a_m}"),
        );
        let (left, right) = tokio::join!(
            a.patch(&i, &to_b, json!({"role_ids": [MEMBER_ROLE]})),
            b.patch(&i, &to_a, json!({"role_ids": [MEMBER_ROLE]})),
        );
        let codes = [left.status.as_u16(), right.status.as_u16()];
        let owners = world
            .scalar(&format!("SELECT count(*) FROM merezha_identity.role_assignments WHERE organization_id = '{}' AND builtin_role = 'owner'", hex(&org)))
            .await;
        assert_eq!(owners, 1, "round {round}: {codes:?}");
        assert!(codes.contains(&200), "{codes:?}");
        assert!(
            codes.contains(&409) || codes.contains(&403),
            "{codes:?} {left:?} {right:?}"
        );
    }
}

#[tokio::test]
async fn parallel_acceptances_make_one_membership() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let i = world.instance().await;
    let mut owner = user(&world, &i, "owner@example.com").await;
    let bob = user(&world, &i, "bob@example.com").await;
    let org = organization(&i, &mut owner, "acme").await;
    owner
        .post(
            &i,
            &format!("/organizations/{org}/invitations"),
            json!({"email": "bob@example.com", "role_ids": [MEMBER_ROLE]}),
        )
        .await;
    let token = world.mail.token("identity.invitation", "bob@example.com");
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let mut browser = bob.clone();
        let i = i.clone();
        let token = token.clone();
        tasks.push(tokio::spawn(async move {
            browser
                .post(&i, "/invitations/accept", json!({"token": token}))
                .await
        }));
    }
    let mut created = 0;
    for task in tasks {
        let answer = task.await.unwrap();
        match answer.status.as_u16() {
            201 => created += 1,
            _ => assert!(
                matches!(answer.code(), "already_member" | "invalid_token"),
                "{answer:?}"
            ),
        }
    }
    assert_eq!(created, 1);
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.memberships")
            .await,
        2
    );
}

#[tokio::test]
async fn a_role_deleted_while_assigned_never_leaves_a_dangling_assignment() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let i = world.instance().await;
    let mut owner = user(&world, &i, "owner@example.com").await;
    let org = organization(&i, &mut owner, "acme").await;
    let owner_m = membership_of(&i, &mut owner, &org, "owner@example.com").await;
    for round in 0..10 {
        let role = owner
            .post(
                &i,
                &format!("/organizations/{org}/roles"),
                json!({"key": format!("r{round}"), "name": "R", "permissions": ["tasks.read"]}),
            )
            .await;
        let role = id_at(&role, "/role/id");
        let mut other = owner.clone();
        let (assignments, role_path) = (
            format!("/organizations/{org}/role-assignments"),
            format!("/organizations/{org}/roles/{role}"),
        );
        let (assign, delete) = tokio::join!(
            owner.post(
                &i,
                &assignments,
                json!({"membership_id": owner_m, "role_id": role})
            ),
            other.delete(&i, &role_path),
        );
        match (assign.status.as_u16(), delete.status.as_u16()) {
            (201, 409) => assert_eq!(delete.code(), "role_in_use"),
            (404, 204) => {}
            other => panic!("{other:?} {assign:?} {delete:?}"),
        }
    }
    let dangling = world
        .scalar("SELECT count(*) FROM merezha_identity.role_assignments a WHERE a.custom_role_id IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM merezha_identity.roles r WHERE r.id = a.custom_role_id)")
        .await;
    assert_eq!(dangling, 0);
}

#[tokio::test]
async fn keys_follow_role_and_membership_changes_made_meanwhile() {
    let Some(world) = World::new(&section(
        "",
        &[],
        "[modules.identity.rbac.builtin_grants]\nuser = [\"identity.api_keys.create\"]\n\
         [modules.identity.rbac.application_roles.editor]\nname = \"Editor\"\npermissions = [\"tasks.*\"]\n",
    ))
    .await
    else {
        return;
    };
    let i = world.instance().await;
    let mut ada = user(&world, &i, "ada@example.com").await;
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
    let assigned = root
        .post(
            &i,
            "/application/role-assignments",
            json!({"user_id": ada_id, "role_id": editor}),
        )
        .await;
    let assignment = id_at(&assigned, "/assignment/id");
    let minted = ada
        .post(&i, "/application/api-keys", json!({"name": "k", "permissions": ["tasks.delete"], "expires_at": "2027-06-01T00:00:00Z"}))
        .await;
    assert_eq!(minted.status, 201, "{minted:?}");
    let mut key = Browser::new("198.51.100.95");
    key.bearer = Some(minted.body["key"].as_str().unwrap().to_owned());
    assert!(key.allowed(&i, "tasks.delete", "application").await);
    // The application role revoked while the key is in use: once a
    // request sees the revocation, no later one is allowed.
    let in_use = {
        let mut key = key.clone();
        let i = i.clone();
        tokio::spawn(async move {
            let mut seen = Vec::new();
            for _ in 0..20 {
                seen.push(key.allowed(&i, "tasks.delete", "application").await);
            }
            seen
        })
    };
    root.delete(&i, &format!("/application/role-assignments/{assignment}"))
        .await;
    let seen = in_use.await.unwrap();
    assert!(!key.allowed(&i, "tasks.delete", "application").await);
    if let Some(first) = seen.iter().position(|allowed| !allowed) {
        assert!(seen[first..].iter().all(|allowed| !allowed), "{seen:?}");
    }
}

#[tokio::test]
async fn key_minting_racing_removal_leaves_no_key_without_membership() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let i = world.instance().await;
    let mut owner = user(&world, &i, "owner@example.com").await;
    let org = organization(&i, &mut owner, "acme").await;
    for round in 0..8 {
        let email = format!("bob{round}@example.com");
        let mut bob = user(&world, &i, &email).await;
        let bob_m = join(
            &world,
            &i,
            &mut owner,
            &org,
            &mut bob,
            &email,
            &[MEMBER_ROLE],
        )
        .await;
        let (keys, member) = (
            format!("/organizations/{org}/api-keys"),
            format!("/organizations/{org}/members/{bob_m}"),
        );
        let (minted, removed) = tokio::join!(
            bob.post(&i, &keys, json!({"name": "k", "permissions": ["identity.members.read"], "expires_at": "2027-06-01T00:00:00Z"})),
            owner.delete(&i, &member),
        );
        assert_eq!(removed.status, 204, "{removed:?}");
        assert!(
            matches!(minted.status.as_u16(), 201 | 401 | 403 | 404),
            "{minted:?}"
        );
        let live = world
            .scalar(&format!(
                "SELECT count(*) FROM merezha_identity.api_keys k JOIN merezha_identity.users u ON u.id = k.user_id
                  WHERE u.email_normalized = '{email}' AND k.revoked_at IS NULL"
            ))
            .await;
        assert_eq!(live, 0, "round {round}: {minted:?}");
        if minted.status == 201 {
            let mut key = Browser::new("198.51.100.96");
            key.bearer = Some(minted.body["key"].as_str().unwrap().to_owned());
            assert_eq!(key.get(&i, "/me").await.status, 401);
        }
    }
}

/// A refused role change rolls back: no version moves, no row changes.
#[tokio::test]
async fn a_failed_role_change_bumps_no_version() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let i = world.instance().await;
    let mut owner = user(&world, &i, "owner@example.com").await;
    let mut admin = user(&world, &i, "admin@example.com").await;
    let mut member = user(&world, &i, "member@example.com").await;
    let org = organization(&i, &mut owner, "acme").await;
    join(
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
    let versions = "SELECT sum(authz_version)::bigint FROM merezha_identity.users";
    let org_versions = "SELECT sum(authz_version)::bigint FROM merezha_identity.organizations";
    let before = (
        world.scalar(versions).await,
        world.scalar(org_versions).await,
    );
    let assignments = world
        .scalar("SELECT count(*) FROM merezha_identity.role_assignments")
        .await;
    // Removing member then adding owner: the second step is refused, so the
    // first is rolled back with it.
    let refused = admin
        .patch(
            &i,
            &format!("/organizations/{org}/members/{member_m}"),
            json!({"role_ids": [OWNER_ROLE]}),
        )
        .await;
    assert_eq!(refused.code(), "permission_escalation");
    let refused = owner
        .patch(
            &i,
            &format!("/organizations/{org}/roles/{OWNER_ROLE}"),
            json!({"name": "x"}),
        )
        .await;
    assert_eq!(refused.code(), "builtin_role");
    let refused = owner
        .post(
            &i,
            &format!("/organizations/{org}/roles"),
            json!({"key": "x", "name": "x", "permissions": ["gone.permission"]}),
        )
        .await;
    assert_eq!(refused.code(), "unknown_permission");
    assert_eq!(
        (
            world.scalar(versions).await,
            world.scalar(org_versions).await
        ),
        before
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.role_assignments")
            .await,
        assignments
    );
}

/// Stored grants a later catalog no longer has grant nothing, and startup
/// logs how many; a declared role removed while assigned stops startup.
#[tokio::test]
async fn catalog_shrink_and_removed_declared_roles() {
    let declared = "[modules.identity.rbac.application_roles.editor]\nname = \"Editor\"\npermissions = [\"tasks.*\"]\n";
    let Some(world) = World::new(&section("", &[], declared)).await else {
        return;
    };
    let i = world.instance().await;
    let mut owner = user(&world, &i, "owner@example.com").await;
    let org = organization(&i, &mut owner, "acme").await;
    let owner_m = membership_of(&i, &mut owner, &org, "owner@example.com").await;
    let role = owner
        .post(
            &i,
            &format!("/organizations/{org}/roles"),
            json!({"key": "old", "name": "Old", "permissions": ["tasks.read"]}),
        )
        .await;
    let role = id_at(&role, "/role/id");
    owner
        .post(&i, &format!("/organizations/{org}/role-assignments"), json!({"membership_id": owner_m, "role_id": role, "resource": {"kind": "project", "id": "p1"}}))
        .await;
    // A release removed `tasks.archive` (written behind the API's back).
    world
        .execute(&format!(
            "UPDATE merezha_identity.roles SET permissions = '{{tasks.read,tasks.archive,archive.*}}' WHERE id = '{}'",
            hex(&role)
        ))
        .await;
    world
        .execute("UPDATE merezha_identity.organizations SET authz_version = authz_version + 1")
        .await;
    let harness = merezha_telemetry::testing::Harness::new();
    let started = {
        let _guard = tracing::dispatcher::set_default(harness.dispatch());
        world.try_start(&section("", &[], declared)).await
    };
    assert!(started.is_ok(), "{started:?}");
    let warned: Vec<_> = harness
        .stderr_lines()
        .into_iter()
        .filter(|l| l["event"] == "identity.unknown_permissions")
        .collect();
    assert_eq!(warned.len(), 1, "{:?}", harness.stderr_lines());
    assert_eq!(warned[0]["count"], 2);
    owner.organization = Some(org.clone());
    assert!(
        owner
            .allowed(&i, "tasks.read", &format!("resource:{org}:project:p1"))
            .await
    );
    // Assign the declared role, then start without it.
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
        .get(&i, "/application/users?email=owner%40example.com")
        .await;
    let owner_id = users.body["items"][0]["id"].as_str().unwrap().to_owned();
    let assigned = root
        .post(
            &i,
            "/application/role-assignments",
            json!({"user_id": owner_id, "role_id": editor}),
        )
        .await;
    assert_eq!(assigned.status, 201);
    let harness = merezha_telemetry::testing::Harness::new();
    let refused = {
        let _guard = tracing::dispatcher::set_default(harness.dispatch());
        world.try_start(&base()).await
    };
    assert_eq!(refused.unwrap_err().code, "declared_role_in_use");
    let logged: Vec<_> = harness
        .stderr_lines()
        .into_iter()
        .filter(|l| l["event"] == "identity.declared_role_in_use")
        .collect();
    assert_eq!(
        (logged[0]["role"].as_str(), logged[0]["count"].as_u64()),
        (Some("editor"), Some(1))
    );
    // Without assignments it is simply dropped.
    let assignment = id_at(&assigned, "/assignment/id");
    root.delete(&i, &format!("/application/role-assignments/{assignment}"))
        .await;
    assert!(world.try_start(&base()).await.is_ok());
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.roles WHERE key = 'editor'")
            .await,
        0
    );
}
