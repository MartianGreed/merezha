//! Acceptance 4: grant changes made on one instance take effect on the
//! next request on another instance sharing the database, with no restart
//! and no cross-instance message, while unchanged grants are served from
//! each instance's cache.
mod support;
use serde_json::json;
use support::*;

#[tokio::test]
async fn grant_changes_are_seen_by_every_instance_on_the_next_request() {
    let Some(world) = World::new(&section(
        "",
        &[],
        "[modules.identity.rbac.application_roles.editor]\nname = \"Editor\"\npermissions = [\"tasks.*\"]\n",
    ))
    .await
    else {
        return;
    };
    let one = world.instance().await;
    let two = world.instance().await;
    let mut owner = user(&world, &one, "owner@example.com").await;
    let mut bob = user(&world, &one, "bob@example.com").await;
    let org = organization(&one, &mut owner, "acme").await;
    let bob_m = join(
        &world,
        &one,
        &mut owner,
        &org,
        &mut bob,
        "bob@example.com",
        &[MEMBER_ROLE],
    )
    .await;
    bob.organization = Some(org.clone());
    let tenant = format!("tenant:{org}");
    // Warm both caches.
    for instance in [&one, &two] {
        assert!(
            !bob.allowed(instance, "identity.members.invite", &tenant)
                .await
        );
        assert!(
            !bob.allowed(instance, "identity.members.invite", &tenant)
                .await
        );
    }
    // A tenant role change on instance one.
    owner
        .patch(
            &one,
            &format!("/organizations/{org}/members/{bob_m}"),
            json!({"role_ids": [ADMIN_ROLE]}),
        )
        .await;
    assert!(bob.allowed(&two, "identity.members.invite", &tenant).await);
    assert!(bob.allowed(&one, "identity.members.invite", &tenant).await);
    // A custom role definition change, held by Bob.
    let role = owner
        .post(
            &one,
            &format!("/organizations/{org}/roles"),
            json!({"key": "reader", "name": "Reader", "permissions": ["tasks.read"]}),
        )
        .await;
    let reader = id_at(&role, "/role/id");
    owner
        .patch(
            &one,
            &format!("/organizations/{org}/members/{bob_m}"),
            json!({"role_ids": [reader]}),
        )
        .await;
    assert!(bob.allowed(&two, "tasks.read", &tenant).await);
    assert!(!bob.allowed(&two, "tasks.delete", &tenant).await);
    owner
        .patch(
            &one,
            &format!("/organizations/{org}/roles/{reader}"),
            json!({"permissions": ["tasks.read", "tasks.delete"]}),
        )
        .await;
    assert!(bob.allowed(&two, "tasks.delete", &tenant).await);
    // An application role, granted and revoked by a platform admin on two.
    user(&world, &two, "root@example.com").await;
    platform_admin(&two, "root@example.com").await;
    let mut root = sign_in(&two, "root@example.com").await;
    let roles = root.get(&two, "/application/roles").await;
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
        .get(&two, "/application/users?email=bob%40example.com")
        .await;
    let bob_id = users.body["items"][0]["id"].as_str().unwrap().to_owned();
    assert!(!bob.allowed(&one, "tasks.create", "application").await);
    let assigned = root
        .post(
            &two,
            "/application/role-assignments",
            json!({"user_id": bob_id, "role_id": editor}),
        )
        .await;
    let assignment = id_at(&assigned, "/assignment/id");
    assert!(bob.allowed(&one, "tasks.create", "application").await);
    root.delete(&two, &format!("/application/role-assignments/{assignment}"))
        .await;
    assert!(!bob.allowed(&one, "tasks.create", "application").await);
    // Break-glass revoked through the CLI path on one: gone on two.
    assert!(
        root.allowed(&two, "identity.users.manage", "application")
            .await
    );
    let email = merezha_identity::normalize::email("root@example.com").unwrap();
    one.handle.platform_admin(&email, false).await.unwrap();
    assert!(
        !root
            .allowed(&two, "identity.users.manage", "application")
            .await
    );
    // Membership removal on two: Bob loses the organization on one.
    owner
        .delete(&two, &format!("/organizations/{org}/members/{bob_m}"))
        .await;
    let answer = bob.check(&one, "tasks.read", &tenant).await;
    assert_eq!(
        (answer.status.as_u16(), answer.code()),
        (404, "not_found"),
        "the header names an organization he left"
    );
    bob.organization = None;
    assert!(!bob.allowed(&one, "tasks.read", &tenant).await);
}

/// Unchanged grants are served from the cache: one snapshot build per
/// instance, then hits.
#[tokio::test]
async fn unchanged_grants_come_from_the_cache() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let telemetry = merezha_telemetry::testing::Harness::new();
    let meter = telemetry.telemetry().meter("identity");
    let i = world.instance_with(Some(&meter)).await;
    let mut owner = user(&world, &i, "owner@example.com").await;
    let org = organization(&i, &mut owner, "acme").await;
    owner.organization = Some(org.clone());
    for _ in 0..5 {
        assert!(
            owner
                .allowed(&i, "identity.members.read", &format!("tenant:{org}"))
                .await
        );
    }
    let lookups = telemetry.metric("merezha.identity.authz_cache");
    let hits = lookups.value(&[("identity.result", "hit")]).unwrap_or(0.0);
    let misses = lookups.value(&[("identity.result", "miss")]).unwrap_or(0.0);
    assert!(hits >= 4.0, "{hits} hits, {misses} misses");
}
