//! Acceptance 9: the account pre-hijacking rule. An attacker registers the
//! victim's address with a password and never verifies it; when the victim
//! proves control of the inbox through a magic link or a password reset,
//! the attacker's password and sessions are gone.
mod support;

use serde_json::json;
use support::*;

const ATTACKER: &str = "attacker chosen passphrase";

async fn pre_hijacked(world: &World, node: &Instance) {
    let mut attacker = Browser::new("203.0.113.66");
    let answer = attacker
        .post(
            node,
            "/registrations",
            json!({"email": "victim@example.com", "password": ATTACKER}),
        )
        .await;
    assert_eq!(answer.status, 202);
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.password_credentials")
            .await,
        1
    );
}

async fn attacker_cannot_sign_in(world: &World, node: &Instance) {
    let mut attacker = Browser::new("203.0.113.66");
    let answer = attacker
        .post(
            node,
            "/sessions",
            json!({"email": "victim@example.com", "password": ATTACKER}),
        )
        .await;
    assert_eq!(answer.code(), "invalid_credentials");
    let claimed = world
        .scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'user.unverified_account_claimed'")
        .await;
    assert_eq!(claimed, 1);
}

#[tokio::test]
async fn a_magic_link_strips_the_attackers_password() {
    // With verification not required the attacker even holds a session.
    let Some(world) = World::new(&section("require_verified_email = false", &[], "")).await else {
        return;
    };
    let node = world.instance().await;
    pre_hijacked(&world, &node).await;
    let mut attacker = Browser::new("203.0.113.66");
    let session = attacker
        .post(
            &node,
            "/sessions",
            json!({"email": "victim@example.com", "password": ATTACKER}),
        )
        .await;
    assert_eq!(session.status, 201);
    let mut victim = Browser::new("198.51.100.77");
    victim
        .post(
            &node,
            "/magic-links",
            json!({"email": "victim@example.com"}),
        )
        .await;
    let token = world
        .mail
        .token("identity.magic_link", "victim@example.com");
    let answer = victim
        .post(&node, "/magic-links/consume", json!({"token": token}))
        .await;
    assert_eq!(answer.status, 201, "{answer:?}");
    assert_eq!(answer.body["user"]["email_verified"], true);
    assert_eq!(answer.body["user"]["credentials"]["password"], false);
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.password_credentials")
            .await,
        0
    );
    assert_eq!(
        attacker.get(&node, "/session").await.code(),
        "unauthenticated"
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.sessions")
            .await,
        1
    );
    attacker_cannot_sign_in(&world, &node).await;
    assert_eq!(victim.get(&node, "/me").await.status, 200);
}

#[tokio::test]
async fn a_password_reset_strips_the_attackers_password() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    pre_hijacked(&world, &node).await;
    let mut victim = Browser::new("198.51.100.78");
    victim
        .post(
            &node,
            "/password-resets",
            json!({"email": "victim@example.com"}),
        )
        .await;
    let token = world
        .mail
        .token("identity.password_reset", "victim@example.com");
    let answer = victim
        .post(
            &node,
            "/password-resets/complete",
            json!({"token": token, "new_password": "a different own passphrase"}),
        )
        .await;
    assert_eq!(answer.status, 201, "{answer:?}");
    assert_eq!(answer.body["user"]["email_verified"], true);
    attacker_cannot_sign_in(&world, &node).await;
    let mine = Browser::new("198.51.100.78")
        .post(
            &node,
            "/sessions",
            json!({"email": "victim@example.com", "password": "a different own passphrase"}),
        )
        .await;
    assert_eq!(mine.status, 201);
}

/// A verification link sent at the attacker's registration does not claim
/// the account: the design reserves the rule for magic links, resets,
/// invitations and trusted providers. It does verify the address.
#[tokio::test]
async fn an_ordinary_verification_keeps_the_registered_password() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    signed_up(&world, &node, "kept@example.com", PASSWORD).await;
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.password_credentials")
            .await,
        1
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'user.unverified_account_claimed'")
            .await,
        0
    );
}
