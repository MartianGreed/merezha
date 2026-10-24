//! The findings of the PR #42 security review, as tests: the pre-hijacking
//! rule on verification links (the reviewer's two proofs of concept, now
//! refused), email changes bound to the requesting session, identical
//! request paths for known and unknown addresses, and the bindings the
//! review found untested (transport, purpose, session ownership, the
//! address re-check).
mod support;

use merezha_telemetry::testing::Harness;
use serde_json::json;
use std::{collections::BTreeMap, time::Instant};
use support::*;

const ATTACKER_PW: &str = "attacker chosen passphrase 1";

/// The reviewer's first proof of concept: the attacker registers the
/// victim's address; the victim clicks the verification mail in another
/// browser. That click is an ownership claim: the attacker's password is
/// gone and the victim is signed in with `password_unset`.
#[tokio::test]
async fn poc_a_verification_click_elsewhere_strips_the_attackers_password() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut attacker = Browser::new("203.0.113.66");
    let a = attacker
        .post(
            &node,
            "/registrations",
            json!({"email": "victim@example.com", "password": ATTACKER_PW}),
        )
        .await;
    assert_eq!(a.status, 202);
    assert!(a.binding_cookie().unwrap().contains("HttpOnly"));
    let token = world
        .mail
        .token("identity.verify_email", "victim@example.com");
    let mut victim = Browser::new("198.51.100.7");
    let v = victim
        .post(
            &node,
            "/email-verifications/confirm",
            json!({"token": token}),
        )
        .await;
    assert_eq!(v.status, 201, "{v:?}");
    assert_eq!(v.body["session"]["password_unset"], true);
    assert_eq!(v.body["user"]["credentials"]["password"], false);
    assert_eq!(v.body["user"]["email_verified"], true);
    let s = Browser::new("203.0.113.66")
        .post(
            &node,
            "/sessions",
            json!({"email": "victim@example.com", "password": ATTACKER_PW}),
        )
        .await;
    assert_eq!(
        s.code(),
        "invalid_credentials",
        "the attacker's password is gone"
    );
    let claimed = world
        .scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'user.unverified_account_claimed'")
        .await;
    assert_eq!(claimed, 1);
    // The victim sets a password from the fresh, recent session.
    let set = victim
        .send(
            &node,
            "PUT",
            "/me/password",
            Some(json!({"new_password": "the rightful owner's phrase"})),
        )
        .await;
    assert_eq!(set.status, 200, "{set:?}");
}

/// The reviewer's second proof of concept: with verification not
/// required, the attacker's live session dies with the claim.
#[tokio::test]
async fn poc_an_unverified_session_dies_with_the_claim() {
    let Some(world) = World::new(&section("require_verified_email = false", &[], "")).await else {
        return;
    };
    let node = world.instance().await;
    let mut attacker = Browser::new("203.0.113.67");
    attacker
        .post(
            &node,
            "/registrations",
            json!({"email": "victim@example.com", "password": ATTACKER_PW}),
        )
        .await;
    let s = attacker
        .post(
            &node,
            "/sessions",
            json!({"email": "victim@example.com", "password": ATTACKER_PW}),
        )
        .await;
    assert_eq!(s.status, 201);
    let token = world
        .mail
        .token("identity.verify_email", "victim@example.com");
    let v = Browser::new("198.51.100.8")
        .post(
            &node,
            "/email-verifications/confirm",
            json!({"token": token}),
        )
        .await;
    assert_eq!(v.status, 201);
    assert_eq!(attacker.get(&node, "/me").await.code(), "unauthenticated");
}

/// The registering browser keeps its password: its binding cookie proves
/// it asked for the link. A second registration from the same browser
/// keeps the binding, and a resend does too.
#[tokio::test]
async fn the_registering_browser_verifies_and_keeps_its_password() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut b = Browser::new("198.51.100.9");
    b.post(
        &node,
        "/registrations",
        json!({"email": "ada@example.com", "password": PASSWORD}),
    )
    .await;
    let binding = b.binding.clone().unwrap();
    b.post(
        &node,
        "/registrations",
        json!({"email": "ada@example.com", "password": PASSWORD}),
    )
    .await;
    b.post(
        &node,
        "/email-verifications",
        json!({"email": "ada@example.com"}),
    )
    .await;
    assert_eq!(
        b.binding.as_deref(),
        Some(binding.as_str()),
        "the binding stays"
    );
    let token = world.mail.token("identity.verify_email", "ada@example.com");
    let v = b
        .post(
            &node,
            "/email-verifications/confirm",
            json!({"token": token}),
        )
        .await;
    assert_eq!(v.status, 201, "{v:?}");
    assert_eq!(v.body["session"]["password_unset"], false);
    assert!(
        v.binding_cookie().unwrap().contains("Max-Age=0"),
        "the binding is cleared"
    );
    let s = Browser::new("198.51.100.10")
        .post(
            &node,
            "/sessions",
            json!({"email": "ada@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(s.status, 201);
    // A forged binding is a claim like none at all.
    let mut forged = Browser::new("203.0.113.70");
    forged
        .post(
            &node,
            "/registrations",
            json!({"email": "eve@example.com", "password": ATTACKER_PW}),
        )
        .await;
    let token = world.mail.token("identity.verify_email", "eve@example.com");
    let mut other = Browser::new("198.51.100.11");
    other.binding = Some("A".repeat(43));
    let v = other
        .post(
            &node,
            "/email-verifications/confirm",
            json!({"token": token}),
        )
        .await;
    assert_eq!(v.body["session"]["password_unset"], true);
}

/// The email-change proof of concept: account A asks to move to the
/// victim's address; the victim clicks the link. Nothing changes: only A's
/// requesting session confirms.
#[tokio::test]
async fn poc_an_email_change_link_clicked_by_its_recipient_changes_nothing() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut a = signed_up(&world, &node, "a@example.com", PASSWORD).await;
    let mut victim = signed_up(&world, &node, "elsewhere@example.com", PASSWORD).await;
    a.post(
        &node,
        "/me/email-changes",
        json!({"new_email": "victim@example.com"}),
    )
    .await;
    // The notice goes to A's current address, captured at request time.
    assert_eq!(
        world
            .mail
            .to("identity.email_change_notice", "a@example.com")
            .len(),
        1
    );
    let token = world
        .mail
        .token("identity.email_change_confirm", "victim@example.com");
    for clicker in [&mut victim, &mut Browser::new("198.51.100.12")] {
        let answer = clicker
            .post(&node, "/me/email-changes/confirm", json!({"token": token}))
            .await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (401, "sign_in_required")
        );
    }
    assert_eq!(
        a.get(&node, "/me").await.body["user"]["email"],
        "a@example.com"
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.users WHERE email_normalized = 'victim@example.com'")
            .await,
        0
    );
    // A bearer session of the requesting account confirms too.
    let mut phone = Browser::new("198.51.100.13");
    phone
        .post(
            &node,
            "/sessions",
            json!({"email": "a@example.com", "password": PASSWORD, "transport": "bearer"}),
        )
        .await;
    phone.origin = None;
    phone
        .send(
            &node,
            "POST",
            "/session/reauthenticate",
            Some(json!({"password": PASSWORD})),
        )
        .await;
    phone
        .post(
            &node,
            "/me/email-changes",
            json!({"new_email": "a2@example.com"}),
        )
        .await;
    let token = world
        .mail
        .token("identity.email_change_confirm", "a2@example.com");
    let ok = phone
        .post(&node, "/me/email-changes/confirm", json!({"token": token}))
        .await;
    assert_eq!(ok.status, 200, "{ok:?}");
    assert_eq!(ok.body["user"]["email"], "a2@example.com");
}

/// The statement names one request ran, from its CLIENT spans.
fn statements(harness: &Harness, since: usize) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for span in harness.spans().into_iter().skip(since) {
        for kv in &span.attributes {
            if kv.key.as_str() == "merezha.db.statement" {
                *counts.entry(kv.value.to_string()).or_insert(0) += 1;
            }
        }
    }
    counts
}

/// Mail-sending routes do the same work on the request path for known and
/// unknown addresses: the same registered statements, the same count. The
/// account lookup runs later, in the `identity.link_request` job.
#[tokio::test]
async fn link_requests_run_the_same_statements_for_every_address() {
    let harness = Harness::new();
    let meter = harness.telemetry().meter(merezha_postgres::METER_SCOPE);
    let Some(world) = World::with_meter(&base(), &meter).await else {
        return;
    };
    let node = world.instance_full(None, false).await;
    // A known, verified account and a known, unverified one, written in SQL
    // so no span of theirs precedes the measurement.
    world
        .execute(
            "INSERT INTO merezha_identity.users (id, email, email_normalized, email_verified_at, created_at, updated_at)
             VALUES (gen_random_uuid(), 'known@example.com', 'known@example.com', now(), now(), now()),
                    (gen_random_uuid(), 'pending@example.com', 'pending@example.com', NULL, now(), now())",
        )
        .await;
    for (path, known, unknown) in [
        (
            "/password-resets",
            "known@example.com",
            "nobody@example.com",
        ),
        ("/magic-links", "known@example.com", "nobody2@example.com"),
        (
            "/email-verifications",
            "pending@example.com",
            "nobody3@example.com",
        ),
    ] {
        let mut measured = Vec::new();
        for email in [known, unknown] {
            let since = harness.spans().len();
            let started = Instant::now();
            let answer = Browser::new("198.51.100.30")
                .post(&node, path, json!({"email": email}))
                .await;
            let _ = started.elapsed();
            assert_eq!(answer.status, 202, "{path}");
            measured.push((statements(&harness, since), answer.body.to_string()));
        }
        assert!(!measured[0].0.is_empty(), "{path}: spans recorded");
        assert!(
            !measured[0].0.keys().any(|k| k.contains("users")),
            "{path}: no lookup on the request path"
        );
        assert_eq!(measured[0], measured[1], "{path}");
    }
}

/// Transport binding: a cookie session presented as a bearer token and a
/// bearer session presented as a cookie are refused.
#[tokio::test]
async fn sessions_are_bound_to_their_transport() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let b = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    let mut x = Browser::new("198.51.100.40");
    x.bearer = b.cookie.clone();
    assert_eq!(x.get(&node, "/me").await.code(), "unauthenticated");
    let mut phone = Browser::new("198.51.100.41");
    phone
        .post(
            &node,
            "/sessions",
            json!({"email": "ada@example.com", "password": PASSWORD, "transport": "bearer"}),
        )
        .await;
    let mut y = Browser::new("198.51.100.42");
    y.cookie = phone.bearer.clone();
    assert_eq!(y.get(&node, "/me").await.code(), "unauthenticated");
    let mut ok = Browser::new("198.51.100.43");
    ok.bearer = phone.bearer.clone();
    assert_eq!(ok.get(&node, "/me").await.status, 200);
}

/// Purpose binding: a reset token is refused by every other consumer and
/// stays usable for its own purpose (the reviewer's test).
#[tokio::test]
async fn tokens_are_bound_to_their_purpose() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut b = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    b.post(
        &node,
        "/password-resets",
        json!({"email": "ada@example.com"}),
    )
    .await;
    let reset = world
        .mail
        .token("identity.password_reset", "ada@example.com");
    for path in [
        "/magic-links/consume",
        "/email-verifications/confirm",
        "/me/email-changes/confirm",
    ] {
        let a = Browser::new("198.51.100.20")
            .post(&node, path, json!({"token": reset}))
            .await;
        assert_eq!(a.code(), "invalid_token", "{path}: {a:?}");
    }
    b.post(&node, "/magic-links", json!({"email": "ada@example.com"}))
        .await;
    let magic = world.mail.token("identity.magic_link", "ada@example.com");
    let a = Browser::new("198.51.100.20")
        .post(
            &node,
            "/password-resets/complete",
            json!({"token": magic, "new_password": "another fine passphrase"}),
        )
        .await;
    assert_eq!(a.code(), "invalid_token");
    let a = Browser::new("198.51.100.20")
        .post(
            &node,
            "/magic-links/consume",
            json!({"token": b.cookie.clone().unwrap()[4..].to_owned()}),
        )
        .await;
    assert_eq!(a.code(), "invalid_token");
    let ok = Browser::new("198.51.100.20")
        .post(
            &node,
            "/password-resets/complete",
            json!({"token": reset, "new_password": "another fine passphrase"}),
        )
        .await;
    assert_eq!(ok.status, 201, "{ok:?}");
}

/// `DELETE /sessions/{id}` revokes only the caller's own sessions.
#[tokio::test]
async fn a_session_id_of_another_user_is_not_found() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut ada = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    let mut bob = signed_up(&world, &node, "bob@example.com", PASSWORD).await;
    let bobs = bob.get(&node, "/session").await.body["session"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let answer = ada
        .send(&node, "DELETE", &format!("/sessions/{bobs}"), None)
        .await;
    assert_eq!((answer.status.as_u16(), answer.code()), (404, "not_found"));
    assert_eq!(bob.get(&node, "/session").await.status, 200);
}

/// A token for an address the account no longer has is refused: the
/// consumer re-checks the address it was sent to.
#[tokio::test]
async fn a_token_for_an_old_address_is_refused() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut b = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    b.post(&node, "/magic-links", json!({"email": "ada@example.com"}))
        .await;
    b.post(
        &node,
        "/password-resets",
        json!({"email": "ada@example.com"}),
    )
    .await;
    let magic = world.mail.token("identity.magic_link", "ada@example.com");
    let reset = world
        .mail
        .token("identity.password_reset", "ada@example.com");
    world
        .execute("UPDATE merezha_identity.users SET email = 'ada@moved.example', email_normalized = 'ada@moved.example'")
        .await;
    let a = Browser::new("198.51.100.50")
        .post(&node, "/magic-links/consume", json!({"token": magic}))
        .await;
    assert_eq!(a.code(), "invalid_token");
    let a = Browser::new("198.51.100.50")
        .post(
            &node,
            "/password-resets/complete",
            json!({"token": reset, "new_password": "another fine passphrase"}),
        )
        .await;
    assert_eq!(a.code(), "invalid_token");
}

/// Twenty concurrent redemptions of one reset token: one wins, and only
/// its password works (the reviewer's test).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_reset_token_redeemed_twenty_times_sets_one_password() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let _ = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    Browser::new("198.51.100.21")
        .post(
            &node,
            "/password-resets",
            json!({"email": "ada@example.com"}),
        )
        .await;
    let reset = world
        .mail
        .token("identity.password_reset", "ada@example.com");
    let mut tasks = Vec::new();
    for i in 0..20 {
        let node = node.clone();
        let reset = reset.clone();
        tasks.push(tokio::spawn(async move {
            let pw = format!("racing passphrase number {i}");
            let a = Browser::new(&format!("198.51.100.{}", 30 + i))
                .post(
                    &node,
                    "/password-resets/complete",
                    json!({"token": reset, "new_password": pw}),
                )
                .await;
            (a.status.as_u16(), a.code().to_owned(), pw)
        }));
    }
    let mut winners = Vec::new();
    for t in tasks {
        let (status, code, pw) = t.await.unwrap();
        if status == 201 {
            winners.push(pw);
        } else {
            assert_eq!(code, "invalid_token");
        }
    }
    assert_eq!(winners.len(), 1, "{winners:?}");
    let ok = Browser::new("198.51.100.90")
        .post(
            &node,
            "/sessions",
            json!({"email": "ada@example.com", "password": winners[0]}),
        )
        .await;
    assert_eq!(ok.status, 201);
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.sessions")
            .await,
        2
    );
}

/// Origin edge cases of cookie writes (the reviewer's test): `null`, a
/// sibling, a look-alike, a missing Origin, a Referer instead of it.
#[tokio::test]
async fn origin_edge_cases_are_refused() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let b = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    for origin in [
        Some("null"),
        Some("https://evil.example"),
        Some("https://app.example.evil"),
        None,
    ] {
        let mut x = b.clone();
        x.origin = origin.map(str::to_owned);
        let a = x
            .post(&node, "/sessions/revoke-all", json!({"keep_current": true}))
            .await;
        assert_eq!(a.code(), "origin_rejected", "{origin:?}");
    }
    use tower::ServiceExt;
    let mut x = b.clone();
    x.origin = None;
    let mut request = x.request(
        "POST",
        "/sessions/revoke-all",
        Some(&json!({"keep_current": true})),
    );
    request
        .headers_mut()
        .insert("referer", "https://app.example/settings".parse().unwrap());
    request
        .headers_mut()
        .insert("sec-fetch-site", "same-site".parse().unwrap());
    let a = read(node.router.clone().oneshot(request).await.unwrap()).await;
    assert_eq!(a.code(), "origin_rejected");
    let other = signed_up(&world, &node, "bob@example.com", PASSWORD).await;
    let mut x = b.clone();
    x.csrf = other.csrf.clone();
    assert_eq!(
        x.post(&node, "/sessions/revoke-all", json!({}))
            .await
            .code(),
        "csrf_failed"
    );
}

/// `X-Forwarded-For` from an untrusted peer does not move the IP buckets
/// (the reviewer's test).
#[tokio::test]
async fn forwarded_for_from_an_untrusted_peer_is_ignored() {
    let Some(world) = World::new(&section("", &[("sign_in_ip", 3, "1h")], "")).await else {
        return;
    };
    let node = world.instance().await;
    use tower::ServiceExt;
    let mut codes = Vec::new();
    for i in 0..5 {
        let b = Browser::new("198.51.100.60");
        let mut request = b.request(
            "POST",
            "/sessions",
            Some(&json!({"email": "x@example.com", "password": "whatever passphrase"})),
        );
        request
            .headers_mut()
            .insert("x-forwarded-for", format!("192.0.2.{i}").parse().unwrap());
        let a = read(node.router.clone().oneshot(request).await.unwrap()).await;
        codes.push(a.code().to_owned());
    }
    assert_eq!(codes[3], "rate_limited", "{codes:?}");
}

/// The residual trade-off of the global admission gate (section 7): a
/// flood of failing sign-ins from many addresses can push other sign-ins
/// into `503 overloaded`; they are never answered with another account's
/// outcome. `sign_in.ip`, charged before the gate, bounds each address.
#[tokio::test]
async fn a_sign_in_flood_overloads_the_gate_but_never_crosses_accounts() {
    let text = section("", &[], "").replace(
        "hash_concurrency = 4",
        "hash_concurrency = 1\nhash_queue_timeout = \"200ms\"",
    );
    let Some(world) = World::new(&text).await else {
        return;
    };
    let node = world.instance().await;
    let _ = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    let mut tasks = Vec::new();
    for i in 0..40 {
        let node = node.clone();
        tasks.push(tokio::spawn(async move {
            Browser::new(&format!("10.1.{i}.1"))
                .post(&node, "/sessions", json!({"email": format!("spray{i}@example.com"), "password": "spray passphrase"}))
                .await
                .code()
                .to_owned()
        }));
    }
    let legit = Browser::new("198.51.100.99")
        .post(
            &node,
            "/sessions",
            json!({"email": "ada@example.com", "password": PASSWORD}),
        )
        .await;
    assert!(
        legit.status == 201 || legit.code() == "overloaded",
        "{legit:?}"
    );
    for t in tasks {
        let code = t.await.unwrap();
        assert!(
            matches!(code.as_str(), "overloaded" | "invalid_credentials"),
            "{code}"
        );
    }
}

/// The planted-secrets check over refusals, a revoke by id, a failed
/// reauthentication and deletion (the reviewer's test).
#[tokio::test]
async fn planted_secrets_stay_out_of_refusals_and_telemetry() {
    let harness = Harness::new();
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let meter = harness.telemetry().meter("identity");
    let node = world.instance_with(Some(&meter)).await;
    let mut planted: Vec<String> = Vec::new();
    let mut b = signed_up(&world, &node, "canary.two@spotted.example", PASSWORD).await;
    planted.push("canary.two@spotted.example".into());
    planted.push(b.cookie.clone().unwrap());
    let long = format!("canarylong{}", "x".repeat(2000));
    for body in [
        json!({"email": "canary.three@spotted.example", "password": long}),
        json!({"email": "canary.three@spotted.example", "password": "canaryshort"}),
        json!({"email": "canary.three@spotted.example", "password": "canary-pw-4471-ok", "bogus_canary_field": "canary-field-value"}),
        json!({"email": "not-an-email-canary", "password": "canary-pw-4471-ok"}),
    ] {
        let a = Browser::new("198.51.100.120")
            .post(&node, "/registrations", body)
            .await;
        let text = a.body.to_string().replace("bogus_canary_field", "");
        assert!(!text.contains("canary"), "{a:?}");
    }
    planted.extend([
        "canary.three@spotted.example".into(),
        "canarylong".into(),
        "canaryshort".into(),
        "canary-pw-4471-ok".into(),
        "canary-field-value".into(),
        "not-an-email-canary".into(),
    ]);
    let mut x = b.clone();
    x.origin = Some("https://evil-canary.example".into());
    x.post(&node, "/sessions/revoke-all", json!({})).await;
    planted.push("evil-canary.example".into());
    b.post(
        &node,
        "/session/reauthenticate",
        json!({"password": "canary-reauth-wrong-9"}),
    )
    .await;
    planted.push("canary-reauth-wrong-9".into());
    let mut other = Browser::new("198.51.100.121");
    other
        .post(
            &node,
            "/sessions",
            json!({"email": "canary.two@spotted.example", "password": PASSWORD}),
        )
        .await;
    let list = b.get(&node, "/sessions").await;
    for item in list.body["items"].as_array().unwrap() {
        let id = item["id"].as_str().unwrap().to_owned();
        if item["current"] == false {
            let a = b
                .send(&node, "DELETE", &format!("/sessions/{id}"), None)
                .await;
            assert_eq!(a.status, 204);
        }
        planted.push(id.trim_start_matches("ses_").to_owned());
        planted.push(id);
    }
    b.send(&node, "DELETE", "/sessions/ses_canarybadid", None)
        .await;
    planted.push("canarybadid".into());
    let a = b
        .send(
            &node,
            "DELETE",
            "/me",
            Some(json!({"confirm_email": "canary.two@spotted.example"})),
        )
        .await;
    assert_eq!(a.status, 204, "{a:?}");
    for canary in &planted {
        harness.assert_no_canary(canary);
    }
    let client = world.db.acquire(soon()).await.unwrap();
    for row in client
        .query(
            "SELECT row_to_json(t)::text FROM merezha_identity.audit_events t",
            &[],
        )
        .await
        .unwrap()
    {
        let text: &str = row.get(0);
        assert!(!text.contains("spotted.example"), "{text}");
    }
}
