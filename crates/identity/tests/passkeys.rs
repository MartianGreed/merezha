//! ID PR 3, passkeys (section 6): registration, email-first and
//! discoverable sign-in, reauthentication and management, against the
//! `webauthn-authenticator-rs` soft passkey; single-use ceremonies bound
//! to user and session; counter regressions; decoys for unknown
//! addresses; the last credential; the ownership claim strips passkeys.
#![cfg(feature = "passkeys")]
mod support;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use serde_json::{Value, json};
use std::time::Duration;
use support::*;
use webauthn_authenticator_rs::{
    AuthenticatorBackend, WebauthnAuthenticator, softpasskey::SoftPasskey,
};
use webauthn_rs::prelude::{CreationChallengeResponse, RequestChallengeResponse, Url};

const PASSKEYS: &str = "[modules.identity.passkeys]\n";

fn origin() -> Url {
    Url::parse(APP).unwrap()
}
fn authenticator() -> WebauthnAuthenticator<SoftPasskey> {
    // The soft token cannot verify a user; it asserts UV as asked.
    WebauthnAuthenticator::new(SoftPasskey::new(true))
}

async fn world(top: &str, limits: &[(&str, u32, &str)]) -> Option<(World, Instance)> {
    let world = World::new(&section(top, limits, PASSKEYS)).await?;
    let node = world.instance().await;
    Some((world, node))
}

/// Register a passkey for `browser`'s user; the passkey JSON.
async fn register(
    node: &Instance,
    browser: &mut Browser,
    key: &mut WebauthnAuthenticator<SoftPasskey>,
    label: Option<&str>,
) -> Value {
    let options = browser
        .send(node, "POST", "/passkeys/registrations/options", None)
        .await;
    assert_eq!(options.status, 200, "{options:?}");
    let challenge: CreationChallengeResponse =
        serde_json::from_value(json!({"publicKey": options.body["public_key"]})).unwrap();
    let credential = key.do_registration(origin(), challenge).unwrap();
    let mut body = json!({"ceremony_id": options.body["ceremony_id"], "credential": credential});
    if let Some(label) = label {
        body["label"] = json!(label);
    }
    let answer = browser
        .post(node, "/passkeys/registrations/verify", body)
        .await;
    assert_eq!(answer.status, 201, "{answer:?}");
    answer.body["passkey"].clone()
}

/// Options for `email` (or discoverable), then the authenticator's
/// assertion: `(ceremony_id, credential)`.
async fn assertion(
    node: &Instance,
    browser: &mut Browser,
    key: &mut WebauthnAuthenticator<SoftPasskey>,
    email: Option<&str>,
    discoverable: Option<(&[u8], uuid::Uuid)>,
) -> (Value, Value) {
    let body = match email {
        Some(email) => json!({ "email": email }),
        None => json!({}),
    };
    let options = browser
        .post(node, "/passkeys/authentications/options", body)
        .await;
    assert_eq!(options.status, 200, "{options:?}");
    let mut public_key = options.body["public_key"].clone();
    if let Some((credential_id, _)) = discoverable {
        // A browser finds the resident credential itself; the soft token
        // needs it named.
        assert_eq!(public_key["allowCredentials"], json!([]));
        public_key["allowCredentials"] =
            json!([{"type": "public-key", "id": B64.encode(credential_id)}]);
    }
    let challenge: RequestChallengeResponse =
        serde_json::from_value(json!({ "publicKey": public_key })).unwrap();
    let credential = key.do_authentication(origin(), challenge).unwrap();
    let mut credential = serde_json::to_value(credential).unwrap();
    if let Some((_, handle)) = discoverable {
        // A discoverable credential returns the user handle.
        credential["response"]["userHandle"] = json!(B64.encode(handle.as_bytes()));
    }
    (options.body["ceremony_id"].clone(), credential)
}

async fn sign_in(
    node: &Instance,
    browser: &mut Browser,
    ceremony: Value,
    credential: Value,
) -> Answer {
    browser
        .post(
            node,
            "/passkeys/authentications/verify",
            json!({"ceremony_id": ceremony, "credential": credential, "transport": "bearer"}),
        )
        .await
}

async fn handle(world: &World, email: &str) -> uuid::Uuid {
    let client = world.db.acquire(soon()).await.unwrap();
    client
        .query_one(
            "SELECT webauthn_handle FROM merezha_identity.users WHERE email_normalized = $1",
            &[&email],
        )
        .await
        .unwrap()
        .get(0)
}
async fn credential_id(world: &World) -> Vec<u8> {
    let client = world.db.acquire(soon()).await.unwrap();
    client
        .query_one("SELECT credential_id FROM merezha_identity.passkeys", &[])
        .await
        .unwrap()
        .get(0)
}
async fn counter(world: &World) -> i64 {
    world
        .scalar("SELECT (passkey->'cred'->>'counter')::bigint FROM merezha_identity.passkeys")
        .await
}

/// Acceptance 4: registration, then both sign-in flows, with the soft
/// passkey; the session method is `passkey`.
#[tokio::test]
async fn register_then_sign_in_email_first_and_discoverable() {
    let Some((world, node)) = world("", &[]).await else {
        return;
    };
    let mut ada = user(&world, &node, "ada@example.com").await;
    let mut key = authenticator();
    let passkey = register(&node, &mut ada, &mut key, Some("Laptop")).await;
    assert!(passkey["id"].as_str().unwrap().starts_with("pky_"));
    assert_eq!(passkey["label"], "Laptop");
    let listed = ada.get(&node, "/me/passkeys").await;
    assert_eq!(listed.body["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        ada.get(&node, "/me").await.body["user"]["credentials"]["passkeys"],
        1
    );
    let registered = world
        .scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'passkey.registered'")
        .await;
    assert_eq!(registered, 1);

    // Email-first.
    let mut browser = Browser::new("198.51.100.60");
    let (ceremony, credential) =
        assertion(&node, &mut browser, &mut key, Some("ada@example.com"), None).await;
    let answer = sign_in(&node, &mut browser, ceremony, credential).await;
    assert_eq!(answer.status, 201, "{answer:?}");
    assert_eq!(answer.body["session"]["method"], "passkey");
    assert_eq!(answer.body["user"]["email"], "ada@example.com");
    assert_eq!(counter(&world).await, 1);

    // Discoverable (conditional UI).
    let options = browser
        .post(&node, "/passkeys/authentications/options", json!({}))
        .await;
    assert_eq!(options.body["mediation"], "conditional");
    let id = credential_id(&world).await;
    let owner = handle(&world, "ada@example.com").await;
    let (ceremony, credential) =
        assertion(&node, &mut browser, &mut key, None, Some((&id, owner))).await;
    let answer = sign_in(&node, &mut browser, ceremony, credential).await;
    assert_eq!(answer.status, 201, "{answer:?}");
    assert_eq!(answer.body["session"]["method"], "passkey");
    assert_eq!(counter(&world).await, 2);
    // Another user's handle with Ada's credential is refused.
    let (ceremony, credential) = assertion(
        &node,
        &mut browser,
        &mut key,
        None,
        Some((&id, uuid::Uuid::from_u128(7))),
    )
    .await;
    let answer = sign_in(&node, &mut browser, ceremony, credential).await;
    assert_eq!(answer.code(), "invalid_credentials");
    // A disabled account.
    world
        .execute("UPDATE merezha_identity.users SET status = 'disabled'")
        .await;
    let mut fresh = Browser::new("198.51.100.69");
    let (ceremony, credential) =
        assertion(&node, &mut fresh, &mut key, Some("ada@example.com"), None).await;
    assert_eq!(
        sign_in(&node, &mut fresh, ceremony, credential)
            .await
            .code(),
        "account_disabled"
    );
}

/// Acceptance 4: ceremony state is single use, bound to its user and
/// session, and expires.
#[tokio::test]
async fn ceremonies_are_single_use_bound_and_expire() {
    let Some((world, node)) = world("", &[]).await else {
        return;
    };
    let mut ada = user(&world, &node, "ada@example.com").await;
    let mut key = authenticator();
    register(&node, &mut ada, &mut key, None).await;
    // Replay of an authentication ceremony.
    let mut browser = Browser::new("198.51.100.61");
    let (ceremony, credential) =
        assertion(&node, &mut browser, &mut key, Some("ada@example.com"), None).await;
    assert_eq!(
        sign_in(&node, &mut browser, ceremony.clone(), credential.clone())
            .await
            .status,
        201
    );
    let replay = sign_in(&node, &mut browser, ceremony, credential).await;
    assert_eq!(replay.code(), "invalid_ceremony");
    // A registration ceremony is the requesting session's only.
    let options = ada
        .send(&node, "POST", "/passkeys/registrations/options", None)
        .await;
    let challenge: CreationChallengeResponse =
        serde_json::from_value(json!({"publicKey": options.body["public_key"]})).unwrap();
    let mut second = authenticator();
    let credential = second.do_registration(origin(), challenge).unwrap();
    let body = json!({"ceremony_id": options.body["ceremony_id"], "credential": credential});
    let mut other_session = sign_in_helper(&node, "ada@example.com").await;
    let answer = other_session
        .post(&node, "/passkeys/registrations/verify", body.clone())
        .await;
    assert_eq!(answer.code(), "invalid_ceremony");
    let mut bob = user(&world, &node, "bob@example.com").await;
    let answer = bob
        .post(&node, "/passkeys/registrations/verify", body.clone())
        .await;
    assert_eq!(answer.code(), "invalid_ceremony");
    // An unknown or malformed ceremony id.
    let answer = ada
        .post(
            &node,
            "/passkeys/registrations/verify",
            json!({"ceremony_id": "cer_00000000000040008000000000000000", "credential": body["credential"]}),
        )
        .await;
    assert_eq!(answer.code(), "invalid_ceremony");
    let answer = ada
        .post(
            &node,
            "/passkeys/registrations/verify",
            json!({"ceremony_id": "nope", "credential": {}}),
        )
        .await;
    assert_eq!(answer.code(), "validation_failed");
    // Expired between options and verify.
    let (ceremony, credential) =
        assertion(&node, &mut browser, &mut key, Some("ada@example.com"), None).await;
    world.clock.advance(Duration::from_secs(6 * 60));
    assert_eq!(
        sign_in(&node, &mut browser, ceremony, credential)
            .await
            .code(),
        "invalid_ceremony"
    );
    // Registration needs a recent session.
    world.clock.advance(Duration::from_secs(5 * 60));
    let answer = ada
        .send(&node, "POST", "/passkeys/registrations/options", None)
        .await;
    assert_eq!(answer.code(), "reauthentication_required");
}

async fn sign_in_helper(node: &Instance, email: &str) -> Browser {
    support::sign_in(node, email).await
}

/// Acceptance 4: a counter regression on a credential with a nonzero
/// counter rejects the sign-in and is audited.
#[tokio::test]
async fn a_counter_regression_rejects_sign_in() {
    let Some((world, node)) = world("", &[]).await else {
        return;
    };
    let mut ada = user(&world, &node, "ada@example.com").await;
    let mut key = authenticator();
    register(&node, &mut ada, &mut key, None).await;
    let mut browser = Browser::new("198.51.100.62");
    let (ceremony, credential) =
        assertion(&node, &mut browser, &mut key, Some("ada@example.com"), None).await;
    assert_eq!(
        sign_in(&node, &mut browser, ceremony, credential)
            .await
            .status,
        201
    );
    // A clone of the authenticator ran ahead.
    world
        .execute("UPDATE merezha_identity.passkeys SET passkey = jsonb_set(passkey, '{cred,counter}', '100')")
        .await;
    let sessions = world
        .scalar("SELECT count(*) FROM merezha_identity.sessions")
        .await;
    let (ceremony, credential) =
        assertion(&node, &mut browser, &mut key, Some("ada@example.com"), None).await;
    let answer = sign_in(&node, &mut browser, ceremony, credential).await;
    assert_eq!(answer.code(), "invalid_credentials");
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.sessions")
            .await,
        sessions
    );
    assert_eq!(counter(&world).await, 100);
    let audited = world
        .scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'passkey.counter_regression'")
        .await;
    assert_eq!(audited, 1);
}

/// Section 4: options for an unknown address, or a user without passkeys,
/// return stable decoys in the shape of a real answer.
#[tokio::test]
async fn unknown_addresses_get_decoys_of_the_same_shape() {
    let Some((world, node)) = world("", &[]).await else {
        return;
    };
    let mut ada = user(&world, &node, "ada@example.com").await;
    let mut key = authenticator();
    register(&node, &mut ada, &mut key, None).await;
    user(&world, &node, "bob@example.com").await;
    let mut browser = Browser::new("198.51.100.63");
    let options = |email: &'static str| {
        let node = node.clone();
        let mut browser = browser.clone();
        async move {
            browser
                .post(
                    &node,
                    "/passkeys/authentications/options",
                    json!({ "email": email }),
                )
                .await
        }
    };
    let real = options("ada@example.com").await;
    let unknown = options("nobody@example.com").await;
    let unknown_again = options("nobody@example.com").await;
    let without = options("bob@example.com").await;
    let keys = |a: &Answer| {
        let mut top: Vec<String> = a.body.as_object().unwrap().keys().cloned().collect();
        let mut inner: Vec<String> = a.body["public_key"]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        let mut entry: Vec<String> = a.body["public_key"]["allowCredentials"][0]
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect();
        top.sort();
        inner.sort();
        entry.sort();
        (top, inner, entry)
    };
    assert_eq!(keys(&real), keys(&unknown));
    assert_eq!(keys(&real), keys(&without));
    assert_eq!(real.body["mediation"], Value::Null);
    assert_eq!(unknown.body["mediation"], Value::Null);
    assert_eq!(
        unknown.body["public_key"]["allowCredentials"],
        unknown_again.body["public_key"]["allowCredentials"],
        "decoys are stable"
    );
    assert_ne!(
        unknown.body["public_key"]["allowCredentials"],
        without.body["public_key"]["allowCredentials"]
    );
    // Verifying against a decoy ceremony fails as bad credentials.
    let (_, credential) =
        assertion(&node, &mut browser, &mut key, Some("ada@example.com"), None).await;
    let answer = sign_in(
        &node,
        &mut browser,
        unknown.body["ceremony_id"].clone(),
        credential,
    )
    .await;
    assert_eq!(answer.code(), "invalid_credentials");
}

/// Reauthentication with a passkey rotates the session (method
/// `passkey`) and makes it recent again; failures charge
/// `reauth.session`.
#[tokio::test]
async fn passkey_reauthentication_rotates_the_session() {
    let Some((world, node)) = world("", &[("reauth_session", 2, "15m")]).await else {
        return;
    };
    let mut ada = user(&world, &node, "ada@example.com").await;
    let mut key = authenticator();
    register(&node, &mut ada, &mut key, None).await;
    world.clock.advance(Duration::from_secs(11 * 60));
    let old = ada.bearer.clone().unwrap();
    let options = ada
        .send(
            &node,
            "POST",
            "/session/reauthenticate/passkey/options",
            None,
        )
        .await;
    assert_eq!(options.status, 200, "{options:?}");
    let challenge: RequestChallengeResponse =
        serde_json::from_value(json!({"publicKey": options.body["public_key"]})).unwrap();
    let credential = key.do_authentication(origin(), challenge).unwrap();
    let answer = ada
        .post(
            &node,
            "/session/reauthenticate/passkey/verify",
            json!({"ceremony_id": options.body["ceremony_id"], "credential": credential}),
        )
        .await;
    assert_eq!(answer.status, 200, "{answer:?}");
    assert_eq!(answer.body["session"]["method"], "passkey");
    assert_ne!(ada.bearer.as_deref(), Some(old.as_str()));
    let mut stale = ada.clone();
    stale.bearer = Some(old);
    assert_eq!(stale.get(&node, "/session").await.code(), "unauthenticated");
    // Recent again: registration options work.
    let again = ada
        .send(&node, "POST", "/passkeys/registrations/options", None)
        .await;
    assert_eq!(again.status, 200, "{again:?}");
    // A sign-in ceremony is no reauthentication ceremony.
    let mut browser = Browser::new("198.51.100.64");
    let (ceremony, credential) =
        assertion(&node, &mut browser, &mut key, Some("ada@example.com"), None).await;
    let answer = ada
        .post(
            &node,
            "/session/reauthenticate/passkey/verify",
            json!({"ceremony_id": ceremony, "credential": credential}),
        )
        .await;
    assert_eq!(answer.code(), "invalid_ceremony");
    // Wrong assertions charge the reauthentication budget.
    for _ in 0..2 {
        let options = ada
            .send(
                &node,
                "POST",
                "/session/reauthenticate/passkey/options",
                None,
            )
            .await;
        let challenge: RequestChallengeResponse =
            serde_json::from_value(json!({"publicKey": options.body["public_key"]})).unwrap();
        let mut credential =
            serde_json::to_value(key.do_authentication(origin(), challenge).unwrap()).unwrap();
        credential["response"]["signature"] = json!(B64.encode([7u8; 64]));
        let answer = ada
            .post(
                &node,
                "/session/reauthenticate/passkey/verify",
                json!({"ceremony_id": options.body["ceremony_id"], "credential": credential}),
            )
            .await;
        assert_eq!(answer.code(), "invalid_credentials");
    }
    let answer = ada
        .post(
            &node,
            "/session/reauthenticate/passkey/verify",
            json!({"ceremony_id": "cer_00000000000040008000000000000000", "credential": credential_stub()}),
        )
        .await;
    assert_eq!(answer.status, 429);
}

fn credential_stub() -> Value {
    json!({"id": "AA", "rawId": "AA", "type": "public-key",
           "response": {"authenticatorData": "AA", "clientDataJSON": "AA", "signature": "AA"}})
}

/// Acceptance 5 and management: rename, list, remove; the last
/// credential cannot go.
#[tokio::test]
async fn management_keeps_the_last_credential() {
    let text = section("", &[], PASSKEYS).replace("[modules.identity.magic_links]\n", "");
    let Some(world) = World::new(&text).await else {
        return;
    };
    let node = world.instance().await;
    let mut ada = user(&world, &node, "ada@example.com").await;
    let mut key = authenticator();
    let passkey = register(&node, &mut ada, &mut key, None).await;
    let id = passkey["id"].as_str().unwrap().to_owned();
    let answer = ada
        .patch(
            &node,
            &format!("/me/passkeys/{id}"),
            json!({"label": "  Phone  "}),
        )
        .await;
    assert_eq!(answer.status, 200, "{answer:?}");
    assert_eq!(answer.body["passkey"]["label"], "Phone");
    let answer = ada
        .patch(
            &node,
            &format!("/me/passkeys/{id}"),
            json!({"label": "x".repeat(65)}),
        )
        .await;
    assert_eq!(answer.code(), "validation_failed");
    let answer = ada
        .patch(
            &node,
            "/me/passkeys/pky_00000000000070008000000000000000",
            json!({"label": "y"}),
        )
        .await;
    assert_eq!(answer.status, 404);
    // Bob cannot touch Ada's passkey.
    let mut bob = user(&world, &node, "bob@example.com").await;
    assert_eq!(
        bob.delete(&node, &format!("/me/passkeys/{id}"))
            .await
            .status,
        404
    );
    // Without a password and without magic links, the passkey is the last
    // credential.
    world
        .execute("DELETE FROM merezha_identity.password_credentials")
        .await;
    let answer = ada.delete(&node, &format!("/me/passkeys/{id}")).await;
    assert_eq!(answer.status, 409);
    assert_eq!(answer.code(), "last_credential");
    // With a second passkey the first can go.
    let mut other = authenticator();
    register(&node, &mut ada, &mut other, None).await;
    assert_eq!(
        ada.delete(&node, &format!("/me/passkeys/{id}"))
            .await
            .status,
        204
    );
    let removed = world
        .scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action IN ('passkey.renamed', 'passkey.removed')")
        .await;
    assert_eq!(removed, 2);
    // Not recent: removal needs a reauthentication.
    world.clock.advance(Duration::from_secs(11 * 60));
    let last = ada.get(&node, "/me/passkeys").await.body["items"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        ada.delete(&node, &format!("/me/passkeys/{last}"))
            .await
            .code(),
        "reauthentication_required"
    );
}

/// The `passkey_limit` and the `passkey_exists` constraint.
#[tokio::test]
async fn limits_and_duplicates() {
    let text = section("", &[], "[modules.identity.passkeys]\nmax_per_user = 1\n");
    let Some(world) = World::new(&text).await else {
        return;
    };
    let node = world.instance().await;
    let mut ada = user(&world, &node, "ada@example.com").await;
    let mut key = authenticator();
    register(&node, &mut ada, &mut key, None).await;
    let answer = ada
        .send(&node, "POST", "/passkeys/registrations/options", None)
        .await;
    assert_eq!(answer.code(), "passkey_limit");
}

/// Section 2: the ownership claim strips the attacker's passkeys as well.
#[tokio::test]
async fn the_ownership_claim_strips_passkeys() {
    let Some((world, node)) = world("require_verified_email = false", &[]).await else {
        return;
    };
    let mut attacker = Browser::new("203.0.113.70");
    attacker
        .post(
            &node,
            "/registrations",
            json!({"email": "victim@example.com", "password": "attacker chosen passphrase"}),
        )
        .await;
    let answer = attacker
        .post(
            &node,
            "/sessions",
            json!({"email": "victim@example.com", "password": "attacker chosen passphrase", "transport": "bearer"}),
        )
        .await;
    assert_eq!(answer.status, 201);
    let mut key = authenticator();
    register(&node, &mut attacker, &mut key, None).await;
    let id = credential_id(&world).await;
    let owner = handle(&world, "victim@example.com").await;
    // The victim proves the inbox with a magic link.
    let mut victim = Browser::new("198.51.100.65");
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
    assert_eq!(answer.body["user"]["credentials"]["passkeys"], 0);
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.passkeys")
            .await,
        0
    );
    // The attacker's passkey no longer signs in.
    let mut browser = Browser::new("203.0.113.71");
    let (ceremony, credential) =
        assertion(&node, &mut browser, &mut key, None, Some((&id, owner))).await;
    let answer = sign_in(&node, &mut browser, ceremony, credential).await;
    assert_eq!(answer.code(), "invalid_credentials");
}

/// Concurrency: two parallel sign-ins with one credential. The assertion
/// with the higher counter always succeeds and the stored counter is the
/// maximum: no lost update.
#[tokio::test]
async fn parallel_sign_ins_keep_the_highest_counter() {
    let Some((world, node)) = world("", &[]).await else {
        return;
    };
    let mut ada = user(&world, &node, "ada@example.com").await;
    let mut key = authenticator();
    register(&node, &mut ada, &mut key, None).await;
    let mut browser = Browser::new("198.51.100.66");
    let (low_ceremony, low) =
        assertion(&node, &mut browser, &mut key, Some("ada@example.com"), None).await;
    let (high_ceremony, high) =
        assertion(&node, &mut browser, &mut key, Some("ada@example.com"), None).await;
    let (mut a, mut b) = (browser.clone(), browser.clone());
    let (low_answer, high_answer) = tokio::join!(
        sign_in(&node, &mut a, low_ceremony, low),
        sign_in(&node, &mut b, high_ceremony, high)
    );
    assert_eq!(high_answer.status, 201, "{high_answer:?}");
    assert!(
        low_answer.status == 201 || low_answer.code() == "invalid_credentials",
        "{low_answer:?}"
    );
    assert_eq!(counter(&world).await, 2);
}

/// `passkey.ip` charges the options routes on arrival; the verification
/// routes take 64 KiB bodies, the others 16 KiB.
#[tokio::test]
async fn options_are_rate_limited_and_verify_bodies_bounded() {
    let Some((world, node)) = world("", &[("passkey_ip", 2, "10m")]).await else {
        return;
    };
    let mut browser = Browser::new("198.51.100.67");
    for _ in 0..2 {
        let answer = browser
            .post(&node, "/passkeys/authentications/options", json!({}))
            .await;
        assert_eq!(answer.status, 200);
    }
    let answer = browser
        .post(&node, "/passkeys/authentications/options", json!({}))
        .await;
    assert_eq!(answer.status, 429);
    let padding = "p".repeat(40 * 1024);
    let mut stub = credential_stub();
    stub["extensions"] = json!({});
    stub["padding"] = json!(padding);
    let answer = browser
        .post(
            &node,
            "/passkeys/authentications/verify",
            json!({"ceremony_id": "cer_00000000000040008000000000000000", "credential": stub}),
        )
        .await;
    assert_eq!(
        answer.code(),
        "invalid_ceremony",
        "40 KiB is within the verify limit"
    );
    let answer = browser
        .post(
            &node,
            "/passkeys/authentications/verify",
            json!({"ceremony_id": "x", "credential": {"padding": "p".repeat(70 * 1024)}}),
        )
        .await;
    assert_eq!(answer.status, 413);
    let answer = browser
        .post(
            &node,
            "/passkeys/authentications/options",
            json!({"email": "p".repeat(20 * 1024)}),
        )
        .await;
    assert_eq!(answer.status, 413);
    let _ = world;
}

/// Without `[modules.identity.passkeys]` the routes are absent.
#[tokio::test]
async fn passkeys_off_without_their_table() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut browser = Browser::new("198.51.100.68");
    let answer = browser
        .post(&node, "/passkeys/authentications/options", json!({}))
        .await;
    assert_eq!(answer.status, 404);
}

// ----- security review of PR #55 -----

/// Hold the row lock of `email`'s user in an open transaction on its own
/// connection, as a concurrent claim or revocation does.
async fn hold_user(world: &World, email: &str) -> merezha_postgres::PooledClient {
    let client = world.db.acquire(soon()).await.unwrap();
    client.batch_execute("BEGIN").await.unwrap();
    client
        .execute(
            "SELECT 1 FROM merezha_identity.users WHERE email_normalized = $1 FOR UPDATE",
            &[&email],
        )
        .await
        .unwrap();
    client
}
/// Until a request waits on a row lock.
async fn until_waiting(world: &World) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let waiting = world
            .scalar("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()")
            .await;
        if waiting > 0 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the request never waited"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// A registration racing the ownership claim: the ceremony was consumed,
/// then the claim removed the session before the passkey was stored.
/// Nothing the attacker's session asked for may land.
#[tokio::test]
async fn a_registration_racing_the_claim_does_not_survive_it() {
    let Some((world, node)) = world("require_verified_email = false", &[]).await else {
        return;
    };
    let mut attacker = Browser::new("203.0.113.90");
    attacker
        .post(
            &node,
            "/registrations",
            json!({"email": "victim@example.com", "password": "attacker chosen passphrase"}),
        )
        .await;
    let answer = attacker
        .post(
            &node,
            "/sessions",
            json!({"email": "victim@example.com", "password": "attacker chosen passphrase", "transport": "bearer"}),
        )
        .await;
    assert_eq!(answer.status, 201);
    let options = attacker
        .send(&node, "POST", "/passkeys/registrations/options", None)
        .await;
    let challenge: CreationChallengeResponse =
        serde_json::from_value(json!({"publicKey": options.body["public_key"]})).unwrap();
    let credential = authenticator()
        .do_registration(origin(), challenge)
        .unwrap();
    let body = json!({"ceremony_id": options.body["ceremony_id"], "credential": credential});
    let lock = hold_user(&world, "victim@example.com").await;
    let request = {
        let node = node.clone();
        let mut attacker = attacker.clone();
        tokio::spawn(async move {
            attacker
                .post(&node, "/passkeys/registrations/verify", body)
                .await
        })
    };
    until_waiting(&world).await;
    // The claim's work, under the user lock: every session goes.
    lock.execute("DELETE FROM merezha_identity.sessions", &[])
        .await
        .unwrap();
    lock.batch_execute("COMMIT").await.unwrap();
    let answer = request.await.unwrap();
    assert_eq!(answer.code(), "unauthenticated", "{answer:?}");
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.passkeys")
            .await,
        0
    );
}

/// A passkey removed while its assertion is verified signs nobody in.
#[tokio::test]
async fn a_passkey_removed_mid_sign_in_creates_no_session() {
    let Some((world, node)) = world("", &[]).await else {
        return;
    };
    let mut ada = user(&world, &node, "ada@example.com").await;
    let mut key = authenticator();
    register(&node, &mut ada, &mut key, None).await;
    let mut browser = Browser::new("198.51.100.90");
    let (ceremony, credential) =
        assertion(&node, &mut browser, &mut key, Some("ada@example.com"), None).await;
    let sessions = world
        .scalar("SELECT count(*) FROM merezha_identity.sessions")
        .await;
    let lock = hold_user(&world, "ada@example.com").await;
    let request = {
        let node = node.clone();
        let mut browser = browser.clone();
        tokio::spawn(async move { sign_in(&node, &mut browser, ceremony, credential).await })
    };
    until_waiting(&world).await;
    lock.execute("DELETE FROM merezha_identity.passkeys", &[])
        .await
        .unwrap();
    lock.batch_execute("COMMIT").await.unwrap();
    let answer = request.await.unwrap();
    assert_eq!(answer.code(), "invalid_credentials", "{answer:?}");
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.sessions")
            .await,
        sessions
    );
}

/// A reauthentication ceremony belongs to the session that asked for it.
#[tokio::test]
async fn a_reauthentication_ceremony_is_bound_to_its_session() {
    let Some((world, node)) = world("", &[]).await else {
        return;
    };
    let mut ada = user(&world, &node, "ada@example.com").await;
    let mut key = authenticator();
    register(&node, &mut ada, &mut key, None).await;
    let options = ada
        .send(
            &node,
            "POST",
            "/session/reauthenticate/passkey/options",
            None,
        )
        .await;
    let challenge: RequestChallengeResponse =
        serde_json::from_value(json!({"publicKey": options.body["public_key"]})).unwrap();
    let credential = key.do_authentication(origin(), challenge).unwrap();
    let mut other = support::sign_in(&node, "ada@example.com").await;
    let answer = other
        .post(
            &node,
            "/session/reauthenticate/passkey/verify",
            json!({"ceremony_id": options.body["ceremony_id"], "credential": credential}),
        )
        .await;
    assert_eq!(answer.code(), "invalid_ceremony");
}

/// A registered passkey sends the `identity.sign_in_method_added` notice.
#[tokio::test]
async fn a_registered_passkey_sends_a_notice() {
    let Some((world, node)) = world("", &[]).await else {
        return;
    };
    let mut ada = user(&world, &node, "ada@example.com").await;
    let id = ada.get(&node, "/me").await.body["user"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    register(&node, &mut ada, &mut authenticator(), None).await;
    let notices = world
        .mail
        .to("identity.sign_in_method_added", &format!("user:{id}"));
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].data["method"], "passkey");
}

/// A native Android app's origin, for a signing certificate digest of
/// `byte` repeated: what Credential Manager puts in the client data.
fn android(byte: u8) -> String {
    format!("android:apk-key-hash:{}", B64.encode([byte; 32]))
}

async fn android_world(origins: &[String]) -> Option<(World, Instance)> {
    let tables = format!("{PASSKEYS}android_origins = {origins:?}\n");
    let world = World::new(&section("", &[], &tables)).await?;
    let node = world.instance().await;
    Some((world, node))
}

/// A native registration: the app's platform signs client data with the
/// app's `origin`, for `rp_id` when it differs from the options' RP.
async fn native_registration(
    node: &Instance,
    browser: &mut Browser,
    key: &mut SoftPasskey,
    origin: &str,
    rp_id: Option<&str>,
) -> Answer {
    let options = browser
        .send(node, "POST", "/passkeys/registrations/options", None)
        .await;
    assert_eq!(options.status, 200, "{options:?}");
    let mut public_key = options.body["public_key"].clone();
    if let Some(rp_id) = rp_id {
        public_key["rp"]["id"] = json!(rp_id);
    }
    let challenge: CreationChallengeResponse =
        serde_json::from_value(json!({ "publicKey": public_key })).unwrap();
    let credential = key
        .perform_register(Url::parse(origin).unwrap(), challenge.public_key, 60_000)
        .unwrap();
    browser
        .post(
            node,
            "/passkeys/registrations/verify",
            json!({"ceremony_id": options.body["ceremony_id"], "credential": credential}),
        )
        .await
}

/// A native email-first sign-in with the app's `origin`, for `rp_id`
/// when it differs from the options' RP.
async fn native_sign_in(
    node: &Instance,
    key: &mut SoftPasskey,
    email: &str,
    origin: &str,
    rp_id: Option<&str>,
) -> Answer {
    let mut browser = Browser::new("198.51.100.61");
    let options = browser
        .post(
            node,
            "/passkeys/authentications/options",
            json!({ "email": email }),
        )
        .await;
    assert_eq!(options.status, 200, "{options:?}");
    let mut public_key = options.body["public_key"].clone();
    if let Some(rp_id) = rp_id {
        public_key["rpId"] = json!(rp_id);
    }
    let challenge: RequestChallengeResponse =
        serde_json::from_value(json!({ "publicKey": public_key })).unwrap();
    let credential = key
        .perform_auth(Url::parse(origin).unwrap(), challenge.public_key, 60_000)
        .unwrap();
    let credential = serde_json::to_value(credential).unwrap();
    sign_in(
        node,
        &mut browser,
        options.body["ceremony_id"].clone(),
        credential,
    )
    .await
}

async fn sessions(world: &World) -> i64 {
    world
        .scalar("SELECT count(*) FROM merezha_identity.sessions")
        .await
}

/// A configured native origin registers and signs in through the HTTP
/// flow; web passkeys keep working beside it.
#[tokio::test]
async fn a_configured_android_origin_registers_and_signs_in() {
    let Some((world, node)) = android_world(&[android(1), android(2)]).await else {
        return;
    };
    let mut ada = user(&world, &node, "ada@example.com").await;
    let mut key = SoftPasskey::new(true);
    let answer = native_registration(&node, &mut ada, &mut key, &android(2), None).await;
    assert_eq!(answer.status, 201, "{answer:?}");
    let answer = native_sign_in(&node, &mut key, "ada@example.com", &android(2), None).await;
    assert_eq!(answer.status, 201, "{answer:?}");
    assert_eq!(answer.body["session"]["method"], "passkey");
    assert_eq!(answer.body["user"]["email"], "ada@example.com");
    assert_eq!(counter(&world).await, 1);

    let mut web = authenticator();
    register(&node, &mut ada, &mut web, None).await;
    let mut browser = Browser::new("198.51.100.62");
    let (ceremony, credential) =
        assertion(&node, &mut browser, &mut web, Some("ada@example.com"), None).await;
    let answer = sign_in(&node, &mut browser, ceremony, credential).await;
    assert_eq!(answer.status, 201, "{answer:?}");
}

/// A well-formed but unconfigured native origin, or the native origin
/// as an HTTP `Origin`, is refused.
#[tokio::test]
async fn an_unconfigured_android_origin_is_refused() {
    let Some((world, node)) = android_world(&[android(1)]).await else {
        return;
    };
    let mut ada = user(&world, &node, "ada@example.com").await;
    let mut key = SoftPasskey::new(true);
    let answer = native_registration(&node, &mut ada, &mut key, &android(2), None).await;
    assert_eq!(
        (answer.status.as_u16(), answer.code()),
        (422, "passkey_rejected"),
        "{answer:?}"
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.passkeys")
            .await,
        0
    );

    let answer = native_registration(&node, &mut ada, &mut key, &android(1), None).await;
    assert_eq!(answer.status, 201, "{answer:?}");
    let before = sessions(&world).await;
    let answer = native_sign_in(&node, &mut key, "ada@example.com", &android(2), None).await;
    assert_eq!(
        (answer.status.as_u16(), answer.code()),
        (401, "invalid_credentials"),
        "{answer:?}"
    );
    assert_eq!(sessions(&world).await, before);
    assert_eq!(counter(&world).await, 0);

    // Native origins are WebAuthn origins only, never HTTP ones.
    let mut app = Browser::new("198.51.100.63");
    app.origin = Some(android(1));
    let answer = app
        .post(
            &node,
            "/passkeys/authentications/options",
            json!({ "email": "ada@example.com" }),
        )
        .await;
    assert_eq!(
        (answer.status.as_u16(), answer.code()),
        (403, "origin_rejected"),
        "{answer:?}"
    );
}

/// A native credential minted for another RP id is refused at
/// registration and at sign-in.
#[tokio::test]
async fn a_native_authenticator_for_another_rp_is_refused() {
    let Some((world, node)) = android_world(&[android(1)]).await else {
        return;
    };
    let mut ada = user(&world, &node, "ada@example.com").await;
    let mut key = SoftPasskey::new(true);
    let answer = native_registration(
        &node,
        &mut ada,
        &mut key,
        &android(1),
        Some("other.example"),
    )
    .await;
    assert_eq!(
        (answer.status.as_u16(), answer.code()),
        (422, "passkey_rejected"),
        "{answer:?}"
    );

    let answer = native_registration(&node, &mut ada, &mut key, &android(1), None).await;
    assert_eq!(answer.status, 201, "{answer:?}");
    let before = sessions(&world).await;
    let answer = native_sign_in(
        &node,
        &mut key,
        "ada@example.com",
        &android(1),
        Some("other.example"),
    )
    .await;
    assert_eq!(
        (answer.status.as_u16(), answer.code()),
        (401, "invalid_credentials"),
        "{answer:?}"
    );
    assert_eq!(sessions(&world).await, before);
}
