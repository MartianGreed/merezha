//! API keys in the `Authenticator` (acceptance 5 and 11): a key never
//! exceeds its owner, never breaks glass, dies with its owner's membership,
//! status or account, and is audited when used after revocation.
mod support;
use serde_json::json;
use std::time::Duration;
use support::*;

const EXPIRES: &str = "2027-06-01T00:00:00Z";

/// `value` with its last secret character changed: the right public id
/// with a wrong secret.
fn tampered(value: &str) -> String {
    let last = if value.ends_with('A') { 'B' } else { 'A' };
    format!("{}{last}", &value[..value.len() - 1])
}
fn keyed(value: &str) -> Browser {
    let mut browser = Browser::new("198.51.100.70");
    browser.bearer = Some(value.to_owned());
    browser
}
async fn mint(i: &Instance, owner: &mut Browser, path: &str, permissions: &[&str]) -> String {
    let answer = owner
        .post(
            i,
            path,
            json!({"name": "k", "permissions": permissions, "expires_at": EXPIRES}),
        )
        .await;
    assert_eq!(answer.status, 201, "{answer:?}");
    answer.body["key"].as_str().unwrap().to_owned()
}

/// Acceptance 5.
// Multi-threaded: the host `Authorizer` writes a break-glass audit in
// place before allowing the use (`block_in_place`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_never_exceeds_its_owner() {
    let Some(world) = World::new(&section(
        "",
        &[],
        "[modules.identity.rbac.builtin_grants]\nuser = [\"tasks.read\", \"identity.api_keys.create\"]\n",
    ))
    .await
    else {
        return;
    };
    let i = world.instance().await;
    let mut owner = user(&world, &i, "owner@example.com").await;
    let mut ada = user(&world, &i, "ada@example.com").await;
    let k = organization(&i, &mut owner, "kappa").await;
    let other = organization(&i, &mut owner, "other").await;
    let ada_m = join(
        &world,
        &i,
        &mut owner,
        &k,
        &mut ada,
        "ada@example.com",
        &[ADMIN_ROLE],
    )
    .await;
    let tenant = mint(
        &i,
        &mut ada,
        &format!("/organizations/{k}/api-keys"),
        &["tasks.read", "tasks.delete"],
    )
    .await;
    let mut key = keyed(&tenant);
    // Inside its organization, within its list and its owner's grants.
    assert!(
        key.allowed(&i, "tasks.delete", &format!("tenant:{k}"))
            .await
    );
    assert!(
        key.allowed(&i, "tasks.delete", &format!("resource:{k}:project:p1"))
            .await
    );
    // Denied outside it and at the application, even where its owner's
    // `user` grants would allow.
    for scope in [
        "application".to_owned(),
        "resource:-:content_type:article".to_owned(),
        format!("tenant:{other}"),
    ] {
        let answer = key.check(&i, "tasks.read", &scope).await;
        assert_eq!(answer.body["allowed"], false, "{scope}");
    }
    // Beyond its list, although its owner holds it.
    assert!(
        !key.allowed(&i, "identity.members.invite", &format!("tenant:{k}"))
            .await
    );
    // A header naming another organization is 404.
    key.organization = Some(other.clone());
    assert_eq!(key.get(&i, "/me").await.status, 404);
    key.organization = None;
    // An application key gets only its owner's application grants: a
    // permission its owner holds only in a tenant cannot even be listed.
    let refused = ada
        .post(&i, "/application/api-keys", json!({"name": "k", "permissions": ["tasks.read", "tasks.delete"], "expires_at": EXPIRES}))
        .await;
    assert_eq!(refused.code(), "permission_escalation");
    let application = mint(&i, &mut ada, "/application/api-keys", &["tasks.read"]).await;
    let mut app_key = keyed(&application);
    assert!(app_key.allowed(&i, "tasks.read", "application").await);
    assert!(
        app_key
            .allowed(&i, "tasks.read", &format!("tenant:{k}"))
            .await
    );
    assert!(
        !app_key
            .allowed(&i, "identity.members.read", &format!("tenant:{k}"))
            .await,
        "tenant grants are not the key's"
    );
    app_key.organization = Some(k.clone());
    assert_eq!(
        app_key.get(&i, "/me").await.status,
        404,
        "an application key never acts in a tenant"
    );
    // Revoking the owner's role shrinks the key on the next request.
    owner
        .patch(
            &i,
            &format!("/organizations/{k}/members/{ada_m}"),
            json!({"role_ids": [MEMBER_ROLE]}),
        )
        .await;
    assert!(
        !key.allowed(&i, "tasks.delete", &format!("tenant:{k}"))
            .await
    );
    // `user` grants hold in every organization, so the key keeps them.
    assert!(key.allowed(&i, "tasks.read", &format!("tenant:{k}")).await);
    // A platform admin's key gets no break-glass.
    user(&world, &i, "root@example.com").await;
    platform_admin(&i, "root@example.com").await;
    let mut root = sign_in(&i, "root@example.com").await;
    assert!(
        root.check(&i, "tasks.delete", "application").await.body["break_glass"]
            .as_bool()
            .unwrap()
    );
    let root_key = mint(
        &i,
        &mut root,
        "/application/api-keys",
        &["tasks.read", "tasks.delete"],
    )
    .await;
    let mut root_key = keyed(&root_key);
    let answer = root_key.check(&i, "tasks.delete", "application").await;
    assert_eq!(
        (
            answer.body["allowed"].as_bool(),
            answer.body["break_glass"].as_bool()
        ),
        (Some(false), Some(false))
    );
    assert!(
        root_key.allowed(&i, "tasks.read", "application").await,
        "its owner's `user` grant only"
    );
    // Keys die with their owner's status...
    let users = root
        .get(&i, "/application/users?email=ada%40example.com")
        .await;
    let ada_id = users.body["items"][0]["id"].as_str().unwrap().to_owned();
    let disabled = root
        .patch(
            &i,
            &format!("/application/users/{ada_id}"),
            json!({"status": "disabled"}),
        )
        .await;
    assert_eq!(disabled.status, 200, "{disabled:?}");
    assert_eq!(app_key.get(&i, "/me").await.status, 401);
    app_key.organization = None;
    assert_eq!(
        app_key.check(&i, "tasks.read", "application").await.status,
        401
    );
    root.patch(
        &i,
        &format!("/application/users/{ada_id}"),
        json!({"status": "active"}),
    )
    .await;
    assert!(
        app_key.allowed(&i, "tasks.read", "application").await,
        "back with its owner"
    );
    // ...and its account.
    let mut fresh = sign_in(&i, "ada@example.com").await;
    let leave = fresh
        .delete(&i, &format!("/organizations/{k}/members/{ada_m}"))
        .await;
    assert_eq!(leave.status, 204);
    let gone = fresh
        .delete_with(&i, "/me", json!({"confirm_email": "ada@example.com"}))
        .await;
    assert_eq!(gone.status, 204, "{gone:?}");
    assert_eq!(
        app_key.check(&i, "tasks.read", "application").await.status,
        401
    );
}

/// Acceptance 11: with `builtin_grants.user = ["tasks.read"]`, Bob's tenant
/// key in K answers 401 on both instances once he is removed, also after
/// rejoining, and also when his membership row is deleted in SQL.
#[tokio::test]
async fn a_removed_members_key_is_refused_everywhere() {
    let Some(world) = World::new(&section(
        "",
        &[],
        "[modules.identity.rbac.builtin_grants]\nuser = [\"tasks.read\"]\n",
    ))
    .await
    else {
        return;
    };
    let one = world.instance().await;
    let two = world.instance().await;
    let mut owner = user(&world, &one, "owner@example.com").await;
    let mut bob = user(&world, &one, "bob@example.com").await;
    let k = organization(&one, &mut owner, "kappa").await;
    let bob_m = join(
        &world,
        &one,
        &mut owner,
        &k,
        &mut bob,
        "bob@example.com",
        &[MEMBER_ROLE],
    )
    .await;
    let value = mint(
        &one,
        &mut bob,
        &format!("/organizations/{k}/api-keys"),
        &["tasks.read"],
    )
    .await;
    let mut key = keyed(&value);
    for instance in [&one, &two] {
        assert!(
            key.allowed(instance, "tasks.read", &format!("tenant:{k}"))
                .await
        );
    }
    let removed = owner
        .delete(&one, &format!("/organizations/{k}/members/{bob_m}"))
        .await;
    assert_eq!(removed.status, 204);
    for instance in [&one, &two] {
        let answer = key
            .check(instance, "tasks.read", &format!("tenant:{k}"))
            .await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (401, "unauthenticated")
        );
    }
    let revoked: i64 = world
        .scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'api_key.revoked' AND details->>'reason' = 'membership_removed'")
        .await;
    assert_eq!(revoked, 1);
    // Rejoining revives nothing.
    join(
        &world,
        &one,
        &mut owner,
        &k,
        &mut bob,
        "bob@example.com",
        &[MEMBER_ROLE],
    )
    .await;
    for instance in [&one, &two] {
        assert_eq!(
            key.check(instance, "tasks.read", &format!("tenant:{k}"))
                .await
                .status,
            401
        );
    }
    // A key minted after rejoining dies with a membership row deleted
    // behind identity's back, without any version bump.
    let value = mint(
        &one,
        &mut bob,
        &format!("/organizations/{k}/api-keys"),
        &["tasks.read"],
    )
    .await;
    let mut fresh = keyed(&value);
    for instance in [&one, &two] {
        assert!(
            fresh
                .allowed(instance, "tasks.read", &format!("tenant:{k}"))
                .await
        );
    }
    world
        .execute(
            "DELETE FROM merezha_identity.memberships m USING merezha_identity.users u
                   WHERE u.id = m.user_id AND u.email_normalized = 'bob@example.com'",
        )
        .await;
    for instance in [&one, &two] {
        assert_eq!(
            fresh
                .check(instance, "tasks.read", &format!("tenant:{k}"))
                .await
                .status,
            401
        );
    }
}

#[tokio::test]
async fn revoked_disabled_and_malformed_keys_are_refused() {
    let Some(world) = World::new(&section("", &[("api_key_invalid_ip", 3, "1m")], "")).await else {
        return;
    };
    let i = world.instance().await;
    let mut owner = user(&world, &i, "owner@example.com").await;
    let org = organization(&i, &mut owner, "acme").await;
    let created = owner
        .post(
            &i,
            &format!("/organizations/{org}/api-keys"),
            json!({"name": "k", "permissions": ["identity.members.read"], "expires_at": EXPIRES}),
        )
        .await;
    let key_id = id_at(&created, "/api_key/id");
    let mut key = keyed(created.body["key"].as_str().unwrap());
    assert_eq!(
        key.get(&i, &format!("/organizations/{org}/members"))
            .await
            .status,
        200
    );
    let touched: i64 = world
        .scalar("SELECT count(*) FROM merezha_identity.api_keys WHERE last_used_at IS NOT NULL")
        .await;
    assert_eq!(touched, 1);
    // Used after revocation: 401 and audited.
    owner
        .delete(&i, &format!("/organizations/{org}/api-keys/{key_id}"))
        .await;
    let after = key.get(&i, &format!("/organizations/{org}/members")).await;
    assert_eq!(
        (after.status.as_u16(), after.code()),
        (401, "unauthenticated")
    );
    let audited: i64 = world.scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'api_key.used_after_revocation'").await;
    assert_eq!(audited, 1);
    // A disabled service account's key, then enabled again.
    let service = owner
        .post(
            &i,
            &format!("/organizations/{org}/service-accounts"),
            json!({"name": "CI"}),
        )
        .await;
    let service = id_at(&service, "/service_account/id");
    owner
        .post(
            &i,
            &format!("/organizations/{org}/role-assignments"),
            json!({"service_account_id": service, "role_id": MEMBER_ROLE}),
        )
        .await;
    let machine = owner.post(&i, &format!("/organizations/{org}/api-keys"), json!({"name": "ci", "permissions": ["identity.members.read"], "expires_at": EXPIRES, "service_account_id": service})).await;
    let mut ci = keyed(machine.body["key"].as_str().unwrap());
    assert_eq!(
        ci.get(&i, &format!("/organizations/{org}/members"))
            .await
            .status,
        200
    );
    owner
        .patch(
            &i,
            &format!("/organizations/{org}/service-accounts/{service}"),
            json!({"status": "disabled"}),
        )
        .await;
    assert_eq!(
        ci.get(&i, &format!("/organizations/{org}/members"))
            .await
            .status,
        401
    );
    owner
        .patch(
            &i,
            &format!("/organizations/{org}/service-accounts/{service}"),
            json!({"status": "active"}),
        )
        .await;
    assert_eq!(
        ci.get(&i, &format!("/organizations/{org}/members"))
            .await
            .status,
        200
    );
    // An expired key.
    let short = owner.post(&i, &format!("/organizations/{org}/api-keys"), json!({"name": "short", "permissions": ["identity.members.read"], "expires_at": "2027-01-20T00:00:00Z"})).await;
    let mut short = keyed(short.body["key"].as_str().unwrap());
    world.clock.advance(Duration::from_secs(10 * 86_400));
    assert_eq!(
        short
            .get(&i, &format!("/organizations/{org}/members"))
            .await
            .status,
        401
    );
    // Malformed and unknown keys spend the address's failure budget.
    for bad in [
        "mzk_short",
        "mzk_aaaaaaaaaaaaaaaa_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
        &tampered(machine.body["key"].as_str().unwrap()),
    ] {
        let mut probe = keyed(bad);
        assert_eq!(probe.get(&i, "/me").await.status, 401, "{bad}");
    }
    let limited = keyed("mzk_aaaaaaaaaaaaaaaa_AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
        .get(&i, "/me")
        .await;
    assert_eq!(
        (limited.status.as_u16(), limited.code()),
        (429, "rate_limited")
    );
    assert!(limited.headers.contains_key("retry-after"));
    // Only the public id and the secret's SHA-256 digest are stored.
    let value = machine.body["key"].as_str().unwrap();
    let (public_id, secret) = (&value[4..20], &value[21..]);
    let stored: i64 = world
        .scalar(&format!(
            "SELECT count(*) FROM merezha_identity.api_keys
              WHERE public_id = '{public_id}'
                AND secret_hash = sha256(decode(translate('{secret}', '-_', '+/') || '=', 'base64'))"
        ))
        .await;
    assert_eq!(stored, 1);
}
