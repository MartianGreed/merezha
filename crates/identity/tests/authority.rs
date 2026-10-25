//! PR #47 review fixes, from the reviewer's PoCs: a removed or demoted
//! member keeps neither the keys it minted nor its pending invitations; an
//! invitation is re-checked against its inviter at acceptance; break-glass
//! is never used unaudited; `is_member` leaves disabled and unverified
//! users out; owner-only paths re-read ownership under the lock.
mod support;
use serde_json::{Value, json};
use std::time::Duration;
use support::*;

const EXPIRES: &str = "2027-06-01T00:00:00Z";

fn keyed(value: &str) -> Browser {
    let mut browser = Browser::new("198.51.100.70");
    browser.bearer = Some(value.to_owned());
    browser
}

/// PoC A, fixed: an invitation never outlives its inviter's authority.
#[tokio::test]
async fn an_invitation_dies_with_its_inviters_authority() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let i = world.instance().await;
    let mut owner = user(&world, &i, "owner@example.com").await;
    let mut x = user(&world, &i, "x@example.com").await;
    let k = organization(&i, &mut owner, "kappa").await;
    let x_m = join(
        &world,
        &i,
        &mut owner,
        &k,
        &mut x,
        "x@example.com",
        &[ADMIN_ROLE],
    )
    .await;
    let sent = x
        .post(
            &i,
            &format!("/organizations/{k}/invitations"),
            json!({"email": "x-alt@example.com", "role_ids": [ADMIN_ROLE]}),
        )
        .await;
    assert_eq!(sent.status, 201, "{sent:?}");
    assert!(
        sent.body["invitation"]["invited_by"]
            .as_str()
            .unwrap()
            .starts_with("usr_")
    );
    let listed = owner
        .get(&i, &format!("/organizations/{k}/invitations"))
        .await;
    assert_eq!(
        listed.body["items"][0]["invited_by"],
        sent.body["invitation"]["invited_by"]
    );
    let token = world.mail.token("identity.invitation", "x-alt@example.com");
    // (a) Removal deletes the inviter's pending invitations.
    assert_eq!(
        owner
            .delete(&i, &format!("/organizations/{k}/members/{x_m}"))
            .await
            .status,
        204
    );
    let mut alt = Browser::new("198.51.100.99");
    let answer = alt
        .post(
            &i,
            "/invitations/accept",
            json!({"token": token, "transport": "bearer"}),
        )
        .await;
    assert_eq!(
        (answer.status.as_u16(), answer.code()),
        (401, "invalid_token"),
        "{answer:?}"
    );
    assert_eq!(
        owner
            .get(&i, &format!("/organizations/{k}/invitations"))
            .await
            .body["items"],
        json!([])
    );
    // A demotion that drops `identity.members.invite` deletes them too.
    let mut y = user(&world, &i, "y@example.com").await;
    let y_m = join(
        &world,
        &i,
        &mut owner,
        &k,
        &mut y,
        "y@example.com",
        &[ADMIN_ROLE],
    )
    .await;
    y.post(
        &i,
        &format!("/organizations/{k}/invitations"),
        json!({"email": "y-alt@example.com", "role_ids": [MEMBER_ROLE]}),
    )
    .await;
    owner
        .patch(
            &i,
            &format!("/organizations/{k}/members/{y_m}"),
            json!({"role_ids": [MEMBER_ROLE]}),
        )
        .await;
    assert_eq!(
        owner
            .get(&i, &format!("/organizations/{k}/invitations"))
            .await
            .body["items"],
        json!([])
    );
    // (b) A demotion that keeps `identity.members.invite` but loses what
    // the invitation grants: refused at acceptance, `invitation_stale`.
    let role = owner
        .post(&i, &format!("/organizations/{k}/roles"), json!({"key": "inviter", "name": "Inviter", "permissions": ["identity.members.invite", "identity.members.read"]}))
        .await;
    let inviter_role = id_at(&role, "/role/id");
    owner
        .patch(
            &i,
            &format!("/organizations/{k}/members/{y_m}"),
            json!({"role_ids": [ADMIN_ROLE]}),
        )
        .await;
    let sent = y
        .post(
            &i,
            &format!("/organizations/{k}/invitations"),
            json!({"email": "z@example.com", "role_ids": [ADMIN_ROLE]}),
        )
        .await;
    assert_eq!(sent.status, 201, "{sent:?}");
    let token = world.mail.token("identity.invitation", "z@example.com");
    owner
        .patch(
            &i,
            &format!("/organizations/{k}/members/{y_m}"),
            json!({"role_ids": [inviter_role]}),
        )
        .await;
    assert_eq!(
        owner
            .get(&i, &format!("/organizations/{k}/invitations"))
            .await
            .body["items"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let answer = Browser::new("198.51.100.98")
        .post(&i, "/invitations/accept", json!({"token": token}))
        .await;
    assert_eq!(
        (answer.status.as_u16(), answer.code()),
        (409, "invitation_stale"),
        "{answer:?}"
    );
    assert_eq!(world.scalar("SELECT count(*) FROM merezha_identity.users WHERE email_normalized = 'z@example.com'").await, 0, "nothing committed but the invitation's removal");
    assert_eq!(
        owner
            .get(&i, &format!("/organizations/{k}/invitations"))
            .await
            .body["items"],
        json!([])
    );
    // An inviter still able to grant: accepted.
    let sent = y
        .post(
            &i,
            &format!("/organizations/{k}/invitations"),
            json!({"email": "w@example.com", "role_ids": [inviter_role]}),
        )
        .await;
    assert_eq!(sent.status, 201, "{sent:?}");
    let token = world.mail.token("identity.invitation", "w@example.com");
    let answer = Browser::new("198.51.100.97")
        .post(&i, "/invitations/accept", json!({"token": token}))
        .await;
    assert_eq!(answer.status, 201, "{answer:?}");
}

/// PoC B, fixed: a removed admin keeps no key it minted, service account
/// keys included, and cannot re-enter through one.
#[tokio::test]
async fn a_removed_admin_keeps_no_key_it_minted() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let i = world.instance().await;
    let mut owner = user(&world, &i, "owner@example.com").await;
    let mut x = user(&world, &i, "x@example.com").await;
    let k = organization(&i, &mut owner, "kappa").await;
    let x_m = join(
        &world,
        &i,
        &mut owner,
        &k,
        &mut x,
        "x@example.com",
        &[ADMIN_ROLE],
    )
    .await;
    let created = x
        .post(
            &i,
            &format!("/organizations/{k}/service-accounts"),
            json!({"name": "ci"}),
        )
        .await;
    let sa = id_at(&created, "/service_account/id");
    x.post(
        &i,
        &format!("/organizations/{k}/role-assignments"),
        json!({"service_account_id": sa, "role_id": ADMIN_ROLE}),
    )
    .await;
    let roles = x.get(&i, &format!("/organizations/{k}/roles")).await;
    let admin: Vec<Value> = roles.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["key"] == "admin")
        .unwrap()["permissions"]
        .as_array()
        .unwrap()
        .clone();
    let minted = x
        .post(&i, &format!("/organizations/{k}/api-keys"), json!({"name": "ci", "permissions": admin, "expires_at": EXPIRES, "service_account_id": sa}))
        .await;
    assert_eq!(minted.status, 201, "{minted:?}");
    let secret = minted.body["key"].as_str().unwrap().to_owned();
    // The listing says who minted it.
    let listed = owner.get(&i, &format!("/organizations/{k}/api-keys")).await;
    let x_id = listed.body["items"][0]["created_by"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(x_id.starts_with("usr_"), "{listed:?}");
    let mut key = keyed(&secret);
    assert_eq!(
        key.get(&i, &format!("/organizations/{k}/members"))
            .await
            .status,
        200
    );
    assert_eq!(
        owner
            .delete(&i, &format!("/organizations/{k}/members/{x_m}"))
            .await
            .status,
        204
    );
    let answer = key.get(&i, &format!("/organizations/{k}/members")).await;
    assert_eq!(
        (answer.status.as_u16(), answer.code()),
        (401, "unauthenticated")
    );
    let answer = key
        .post(
            &i,
            &format!("/organizations/{k}/invitations"),
            json!({"email": "x@example.com", "role_ids": [ADMIN_ROLE]}),
        )
        .await;
    assert_eq!(answer.status, 401);
    // The service account itself stays, for its other keys.
    let accounts = owner
        .get(&i, &format!("/organizations/{k}/service-accounts"))
        .await;
    assert_eq!(accounts.body["items"].as_array().unwrap().len(), 1);
    assert_eq!(world.scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'api_key.revoked' AND details->>'reason' = 'membership_removed'").await, 1);
    // Demotion: an admin who loses `identity.service_accounts.manage`
    // loses the service account keys it minted, but the owner's stay.
    let mut y = user(&world, &i, "y@example.com").await;
    let y_m = join(
        &world,
        &i,
        &mut owner,
        &k,
        &mut y,
        "y@example.com",
        &[ADMIN_ROLE],
    )
    .await;
    let theirs = y
        .post(&i, &format!("/organizations/{k}/api-keys"), json!({"name": "y", "permissions": ["identity.members.read"], "expires_at": EXPIRES, "service_account_id": sa}))
        .await;
    let ours = owner
        .post(&i, &format!("/organizations/{k}/api-keys"), json!({"name": "o", "permissions": ["identity.members.read"], "expires_at": EXPIRES, "service_account_id": sa}))
        .await;
    owner
        .patch(
            &i,
            &format!("/organizations/{k}/members/{y_m}"),
            json!({"role_ids": [MEMBER_ROLE]}),
        )
        .await;
    let mut theirs = keyed(theirs.body["key"].as_str().unwrap());
    let mut ours = keyed(ours.body["key"].as_str().unwrap());
    assert_eq!(
        theirs
            .get(&i, &format!("/organizations/{k}/members"))
            .await
            .status,
        401
    );
    assert_eq!(
        ours.get(&i, &format!("/organizations/{k}/members"))
            .await
            .status,
        200
    );
    // Its own key survives a demotion that keeps `identity.api_keys.create`.
    let own = y
        .post(
            &i,
            &format!("/organizations/{k}/api-keys"),
            json!({"name": "own", "permissions": ["identity.members.read"], "expires_at": EXPIRES}),
        )
        .await;
    assert_eq!(own.status, 201, "{own:?}");
    let role = owner
        .post(
            &i,
            &format!("/organizations/{k}/roles"),
            json!({"key": "reader", "name": "Reader", "permissions": ["identity.members.read"]}),
        )
        .await;
    let reader = id_at(&role, "/role/id");
    let mut own = keyed(own.body["key"].as_str().unwrap());
    assert_eq!(
        own.get(&i, &format!("/organizations/{k}/members"))
            .await
            .status,
        200
    );
    owner
        .patch(
            &i,
            &format!("/organizations/{k}/members/{y_m}"),
            json!({"role_ids": [reader]}),
        )
        .await;
    assert_eq!(
        own.get(&i, &format!("/organizations/{k}/members"))
            .await
            .status,
        401,
        "lost identity.api_keys.create"
    );
}

/// PoC C, fixed: with audit writes blocked, break-glass is refused
/// (`503 audit_unavailable`) rather than used unaudited, on identity's own
/// routes and through the host `Authorizer`; once audits can be written
/// it works and every use is audited.
#[tokio::test]
async fn break_glass_is_never_used_unaudited() {
    let Some(world) = World::with_pool(
        &base(),
        32,
        Duration::from_secs(20),
        Duration::from_secs(30),
    )
    .await
    else {
        return;
    };
    let i = world.instance().await;
    user(&world, &i, "root@example.com").await;
    platform_admin(&i, "root@example.com").await;
    let mut admin = sign_in(&i, "root@example.com").await;
    let lock = world.db.acquire(soon()).await.unwrap();
    lock.batch_execute("BEGIN; LOCK TABLE merezha_identity.audit_events IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    // Identity's route waits for the audit, then refuses.
    let users = admin.get(&i, "/application/users").await;
    assert_eq!(
        (users.status.as_u16(), users.code()),
        (503, "audit_unavailable"),
        "{users:?}"
    );
    // The host `Authorizer`, on this current-thread runtime, refuses at once.
    for n in 0..70 {
        let answer = admin
            .check(&i, &format!("filler.p{n}"), "application")
            .await;
        assert_eq!(
            (
                answer.body["allowed"].as_bool(),
                answer.body["code"].as_str()
            ),
            (Some(false), Some("audit_unavailable")),
            "{answer:?}"
        );
    }
    let answer = admin.check(&i, "system.status.read", "application").await;
    assert_eq!(answer.body["allowed"], false);
    lock.batch_execute("ROLLBACK").await.unwrap();
    drop(lock);
    // With audits back, every allowed use has its row first.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    let mut allowed = false;
    while std::time::Instant::now() < deadline {
        let answer = admin.check(&i, "system.status.read", "application").await;
        if answer.body["allowed"] == true {
            assert_eq!(answer.body["break_glass"], true);
            allowed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(allowed, "break-glass works again once audited");
    let audited = world
        .scalar("SELECT count(*) FROM merezha_identity.audit_events
                  WHERE action = 'authorization.break_glass' AND details->>'permission' = 'system.status.read'")
        .await;
    assert_eq!(audited, 1);
    assert_eq!(admin.get(&i, "/application/users").await.status, 200);
    assert_eq!(
        world.scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'authorization.break_glass' AND details->>'permission' = 'identity.users.read'").await,
        1
    );
}

/// The host `Authorizer` on a multi-threaded runtime writes the audit in
/// place: the first use is allowed and already audited.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn break_glass_is_audited_before_its_first_use() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let i = world.instance().await;
    user(&world, &i, "root@example.com").await;
    platform_admin(&i, "root@example.com").await;
    let mut admin = sign_in(&i, "root@example.com").await;
    let answer = admin.check(&i, "system.status.read", "application").await;
    assert_eq!(answer.body["break_glass"], true, "{answer:?}");
    assert_eq!(
        world.scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'authorization.break_glass' AND details->>'permission' = 'system.status.read'").await,
        1
    );
    admin.check(&i, "system.status.read", "application").await;
    assert_eq!(
        world.scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'authorization.break_glass'").await,
        1,
        "once per session and permission"
    );
}

/// PoC D, fixed: `is_member` is false for disabled and unverified users.
#[tokio::test]
async fn is_member_leaves_out_disabled_and_unverified_users() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let i = world.instance().await;
    let mut owner = user(&world, &i, "owner@example.com").await;
    let mut ada = user(&world, &i, "ada@example.com").await;
    let k = organization(&i, &mut owner, "kappa").await;
    join(
        &world,
        &i,
        &mut owner,
        &k,
        &mut ada,
        "ada@example.com",
        &[MEMBER_ROLE],
    )
    .await;
    let hexid: String = {
        let client = world.db.acquire(soon()).await.unwrap();
        client
            .query_one("SELECT replace(id::text, '-', '') FROM merezha_identity.users WHERE email_normalized = 'ada@example.com'", &[])
            .await
            .unwrap()
            .get(0)
    };
    let directory = i.handle.directory();
    let ada = merezha_domain::Identity::new(format!("usr_{hexid}")).unwrap();
    let tenant = merezha_domain::Identity::new(k.clone()).unwrap();
    assert!(directory.is_member(&ada, &tenant).await.unwrap());
    world.execute("UPDATE merezha_identity.users SET status = 'disabled' WHERE email_normalized = 'ada@example.com'").await;
    assert!(!directory.is_member(&ada, &tenant).await.unwrap());
    world.execute("UPDATE merezha_identity.users SET status = 'active', email_verified_at = NULL WHERE email_normalized = 'ada@example.com'").await;
    assert!(!directory.is_member(&ada, &tenant).await.unwrap());
}

/// Owner-only paths re-read ownership under the organization lock: an
/// owner demoted after it authenticated cannot transfer ownership or
/// remove an owner.
#[tokio::test]
async fn owner_only_paths_honour_a_demotion_racing_the_request() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let i = world.instance().await;
    let mut a = user(&world, &i, "a@example.com").await;
    let mut b = user(&world, &i, "b@example.com").await;
    let mut c = user(&world, &i, "c@example.com").await;
    let k = organization(&i, &mut a, "kappa").await;
    let b_m = join(
        &world,
        &i,
        &mut a,
        &k,
        &mut b,
        "b@example.com",
        &[OWNER_ROLE],
    )
    .await;
    let c_m = join(
        &world,
        &i,
        &mut a,
        &k,
        &mut c,
        "c@example.com",
        &[MEMBER_ROLE],
    )
    .await;
    let a_m = membership_of(&i, &mut a, &k, "a@example.com").await;
    // Hold the organization lock while B demotes A: A's transfer, already
    // past authentication with its owner snapshot, waits and then sees it.
    let lock = world.db.acquire(soon()).await.unwrap();
    lock.batch_execute(&format!(
        "BEGIN; SELECT 1 FROM merezha_identity.organizations WHERE id = '{}' FOR UPDATE",
        hex(&k)
    ))
    .await
    .unwrap();
    let transfer = {
        let mut a = a.clone();
        let i = i.clone();
        let path = format!("/organizations/{k}/ownership-transfers");
        tokio::spawn(async move {
            a.post(
                &i,
                &path,
                json!({"membership_id": c_m, "keep_owner": false}),
            )
            .await
        })
    };
    let removal = {
        let mut a = a.clone();
        let i = i.clone();
        let path = format!("/organizations/{k}/members/{b_m}");
        tokio::spawn(async move { a.delete(&i, &path).await })
    };
    tokio::time::sleep(Duration::from_millis(300)).await;
    // The demotion, as B would make it, committed before A's requests
    // take the lock.
    lock.batch_execute(&format!(
        "DELETE FROM merezha_identity.role_assignments WHERE membership_id = '{}' AND builtin_role = 'owner';
         UPDATE merezha_identity.users SET authz_version = authz_version + 1
          WHERE email_normalized = 'a@example.com';
         COMMIT",
        hex(&a_m)
    ))
    .await
    .unwrap();
    let transfer = transfer.await.unwrap();
    let removal = removal.await.unwrap();
    assert_eq!(transfer.code(), "permission_escalation", "{transfer:?}");
    assert_eq!(removal.code(), "permission_escalation", "{removal:?}");
    let owners = world
        .scalar(&format!("SELECT count(*) FROM merezha_identity.role_assignments WHERE organization_id = '{}' AND builtin_role = 'owner'", hex(&k)))
        .await;
    assert_eq!(owners, 1, "B stays the only owner; C was not promoted");
}
