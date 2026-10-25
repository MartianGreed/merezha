//! The cleanup job (section 8) and acceptance 13: a never-used unverified
//! account is deleted after its TTL only with `require_verified_email =
//! true`; with `false`, or once it signed in, it survives every pass.
mod support;

use serde_json::json;
use std::time::Duration;
use support::*;

const DAY: u64 = 86_400;

async fn users(world: &World) -> Vec<String> {
    let client = world.db.acquire(soon()).await.unwrap();
    client
        .query(
            "SELECT email FROM merezha_identity.users ORDER BY email",
            &[],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect()
}

#[tokio::test]
async fn unverified_accounts_expire_only_while_verification_is_required() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut b = Browser::new("198.51.100.120");
    b.post(
        &node,
        "/registrations",
        json!({"email": "never@example.com", "password": PASSWORD}),
    )
    .await;
    signed_up(&world, &node, "verified@example.com", PASSWORD).await;
    // Six days: still inside the seven-day TTL.
    world.clock.advance(Duration::from_secs(6 * DAY));
    let report = node.handle.cleanup_now().await.unwrap();
    assert_eq!(report.unverified_accounts, 0);
    world.clock.advance(Duration::from_secs(2 * DAY));
    let report = node.handle.cleanup_now().await.unwrap();
    assert_eq!(report.unverified_accounts, 1);
    assert_eq!(users(&world).await, vec!["verified@example.com".to_owned()]);
    let audited = world
        .scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'user.deleted' AND actor_kind = 'system'")
        .await;
    assert_eq!(audited, 1);
}

#[tokio::test]
async fn unverified_accounts_survive_without_required_verification_or_once_signed_in() {
    let Some(world) = World::new(&section("require_verified_email = false", &[], "")).await else {
        return;
    };
    let node = world.instance().await;
    let mut b = Browser::new("198.51.100.121");
    b.post(
        &node,
        "/registrations",
        json!({"email": "lazy@example.com", "password": PASSWORD}),
    )
    .await;
    for _ in 0..3 {
        world.clock.advance(Duration::from_secs(30 * DAY));
        assert_eq!(
            node.handle.cleanup_now().await.unwrap().unverified_accounts,
            0
        );
    }
    assert_eq!(users(&world).await, vec!["lazy@example.com".to_owned()]);
    // With verification required again, an account that signed in (when it
    // was not required) still survives.
    let strict = World {
        test: world.test,
        db: world.db.clone(),
        clock: world.clock.clone(),
        mail: world.mail.clone(),
        section: base(),
    };
    let lenient = node;
    let signed = b
        .post(
            &lenient,
            "/sessions",
            json!({"email": "lazy@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(signed.status, 201);
    let node = strict.instance().await;
    for _ in 0..3 {
        strict.clock.advance(Duration::from_secs(30 * DAY));
        assert_eq!(
            node.handle.cleanup_now().await.unwrap().unverified_accounts,
            0
        );
    }
    assert_eq!(users(&strict).await, vec!["lazy@example.com".to_owned()]);
}

#[tokio::test]
async fn cleanup_removes_expired_sessions_tokens_buckets_and_old_audit_rows() {
    let text = section(
        "",
        &[],
        "[modules.identity.audit]\nretention = \"30d\"\n[modules.identity.cleanup]\nbatch_size = 2\n",
    );
    let Some(world) = World::new(&text).await else {
        return;
    };
    let node = world.instance().await;
    for n in 0..3 {
        signed_up(&world, &node, &format!("u{n}@example.com"), PASSWORD).await;
    }
    let mut b = Browser::new("198.51.100.122");
    b.post(&node, "/magic-links", json!({"email": "u0@example.com"}))
        .await;
    let report = node.handle.cleanup_now().await.unwrap();
    assert_eq!(
        (report.sessions, report.tokens, report.active_sessions),
        (0, 0, 3)
    );
    world.clock.advance(Duration::from_secs(31 * DAY));
    let report = node.handle.cleanup_now().await.unwrap();
    assert_eq!(report.sessions, 3, "batches of 2 until done");
    assert_eq!(report.tokens, 1);
    assert!(report.buckets >= 1);
    assert!(report.audit_events >= 6);
    assert_eq!(report.active_sessions, 0);
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.sessions")
            .await,
        0
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.rate_limits")
            .await,
        0
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.audit_events")
            .await,
        0
    );
}

/// ID PR 2's conditions: an unverified account holding a membership or an
/// application role is never a cleanup candidate (so never an
/// organization's owner); expired invitations and keys revoked or expired
/// 30 days ago go.
#[tokio::test]
async fn attached_unverified_accounts_survive_and_dead_rows_go() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    for email in [
        "member@example.com",
        "role@example.com",
        "alone@example.com",
    ] {
        Browser::new("198.51.100.122")
            .post(
                &node,
                "/registrations",
                json!({"email": email, "password": PASSWORD}),
            )
            .await;
    }
    let mut owner = user(&world, &node, "owner@example.com").await;
    let org = organization(&node, &mut owner, "acme").await;
    world
        .execute(&format!(
            "INSERT INTO merezha_identity.memberships (id, organization_id, user_id, created_at)
             SELECT gen_random_uuid(), '{}', id, now() FROM merezha_identity.users
              WHERE email_normalized = 'member@example.com';
             INSERT INTO merezha_identity.roles (id, scope, key, name, created_at, updated_at)
             VALUES ('00000000-0000-7000-8000-0000000000c1', 'application', 'support', 'Support', now(), now());
             INSERT INTO merezha_identity.application_role_assignments (id, user_id, role_id, created_at)
             SELECT gen_random_uuid(), id, '00000000-0000-7000-8000-0000000000c1', now()
               FROM merezha_identity.users WHERE email_normalized = 'role@example.com'",
            hex(&org)
        ))
        .await;
    let invited = owner
        .post(
            &node,
            &format!("/organizations/{org}/invitations"),
            json!({"email": "late@example.com", "role_ids": [MEMBER_ROLE]}),
        )
        .await;
    assert_eq!(invited.status, 201);
    let key = owner
        .post(&node, &format!("/organizations/{org}/api-keys"), json!({"name": "k", "permissions": ["tasks.read"], "expires_at": "2027-06-01T00:00:00Z"}))
        .await;
    let key_id = id_at(&key, "/api_key/id");
    owner
        .delete(&node, &format!("/organizations/{org}/api-keys/{key_id}"))
        .await;
    world.clock.advance(Duration::from_secs(8 * DAY));
    let report = node.handle.cleanup_now().await.unwrap();
    assert_eq!(report.unverified_accounts, 1);
    assert_eq!(report.invitations, 1);
    assert_eq!(report.api_keys, 0, "a revoked key stays 30 days");
    assert_eq!(
        users(&world).await,
        vec![
            "member@example.com".to_owned(),
            "owner@example.com".to_owned(),
            "role@example.com".to_owned()
        ]
    );
    world.clock.advance(Duration::from_secs(23 * DAY));
    assert_eq!(node.handle.cleanup_now().await.unwrap().api_keys, 1);
}
