//! Acceptance 2: every route of the PR 1 table answers as specified,
//! including every listed error code; plus acceptance 3 (registration does
//! not reveal accounts), 4 (sign-in costs one verification either way) and
//! 8 (tokens are single use, superseded and expiring).
mod support;

use merezha_platform::Module;

use axum::http::{StatusCode, header};
use serde_json::{Value, json};
use std::time::Duration;
use support::*;
use tower::ServiceExt;

fn detail(answer: &Answer) -> Vec<(String, String)> {
    answer.body["error"]["details"]
        .as_array()
        .map(|items| {
            items
                .iter()
                .map(|d| {
                    (
                        d["path"].as_str().unwrap().to_owned(),
                        d["code"].as_str().unwrap().to_owned(),
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

#[tokio::test]
async fn registration_answers_and_refuses_as_specified() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut b = Browser::new("198.51.100.20");
    let ok = b
        .post(
            &node,
            "/registrations",
            json!({"email": "Ada@Example.com", "password": PASSWORD, "display_name": "Ada"}),
        )
        .await;
    assert_eq!(ok.status, 202);
    assert_eq!(ok.body, json!({"status": "pending_verification"}));
    assert_eq!(ok.headers[header::CACHE_CONTROL], "no-store");
    // validation_failed lists every field and never echoes input.
    let bad = b
        .post(
            &node,
            "/registrations",
            json!({"email": "not-an-email", "display_name": 7, "extra": "x"}),
        )
        .await;
    assert_eq!(
        (bad.status, bad.code()),
        (StatusCode::UNPROCESSABLE_ENTITY, "validation_failed")
    );
    let mut found = detail(&bad);
    found.sort();
    assert_eq!(
        found,
        vec![
            ("/display_name".into(), "invalid".into()),
            ("/email".into(), "invalid".into()),
            ("/extra".into(), "unknown".into()),
            ("/password".into(), "required".into()),
        ]
    );
    assert!(!bad.body.to_string().contains("not-an-email"));
    for (password, code) in [
        ("short pass", "password_too_short"),
        (&*"x".repeat(257), "password_too_long"),
        ("password1234", "password_blocklisted"),
        ("lovelace-and-babbage", "password_contextual"),
    ] {
        let answer = b
            .post(
                &node,
                "/registrations",
                json!({"email": "lovelace@example.com", "password": password}),
            )
            .await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (422, code),
            "{password}"
        );
    }
    // 415 without JSON, 413 past 16 KiB, 400 for a non-object body.
    let mut request = b.request("POST", "/registrations", None);
    *request.body_mut() = axum::body::Body::from("email=x");
    request.headers_mut().insert(
        header::CONTENT_TYPE,
        "application/x-www-form-urlencoded".parse().unwrap(),
    );
    let answer = read(node.router.clone().oneshot(request).await.unwrap()).await;
    assert_eq!(
        (answer.status.as_u16(), answer.code()),
        (415, "unsupported_media_type")
    );
    let big = b
        .post(
            &node,
            "/registrations",
            json!({"email": "a@example.com", "password": "x".repeat(17_000)}),
        )
        .await;
    assert_eq!((big.status.as_u16(), big.code()), (413, "body_limit"));
    let array = b.post(&node, "/registrations", json!([1, 2])).await;
    assert_eq!(
        (array.status.as_u16(), array.code()),
        (400, "invalid_input")
    );
    // The Origin check of anonymous unsafe requests.
    let mut foreign = Browser::new("198.51.100.21");
    foreign.origin = Some("https://evil.example".into());
    let answer = foreign
        .post(
            &node,
            "/registrations",
            json!({"email": "x@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(
        (answer.status.as_u16(), answer.code()),
        (403, "origin_rejected")
    );
    foreign.origin = None;
    let answer = foreign
        .post(
            &node,
            "/registrations",
            json!({"email": "x@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(answer.code(), "origin_rejected");
    let mut request = foreign.request(
        "POST",
        "/registrations",
        Some(&json!({"email": "y@example.com", "password": PASSWORD})),
    );
    request
        .headers_mut()
        .insert("sec-fetch-site", "same-origin".parse().unwrap());
    let answer = read(node.router.clone().oneshot(request).await.unwrap()).await;
    assert_eq!(answer.status, 202, "same-origin without Origin passes");
}

#[tokio::test]
async fn registration_modes_and_its_rate_limit() {
    for mode in ["invite_only", "closed"] {
        let Some(world) =
            World::new(&section(&format!("registration = \"{mode}\""), &[], "")).await
        else {
            return;
        };
        let node = world.instance().await;
        let answer = Browser::new("198.51.100.22")
            .post(
                &node,
                "/registrations",
                json!({"email": "a@example.com", "password": PASSWORD}),
            )
            .await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (403, "registration_closed")
        );
    }
    let Some(world) = World::new(&section("", &[("registration_ip", 2, "1h")], "")).await else {
        return;
    };
    let node = world.instance().await;
    let mut b = Browser::new("198.51.100.23");
    for n in 0..2 {
        let answer = b
            .post(
                &node,
                "/registrations",
                json!({"email": format!("u{n}@example.com"), "password": PASSWORD}),
            )
            .await;
        assert_eq!(answer.status, 202);
    }
    let limited = b
        .post(
            &node,
            "/registrations",
            json!({"email": "u9@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(
        (limited.status.as_u16(), limited.code()),
        (429, "rate_limited")
    );
    let retry: u64 = limited.headers[header::RETRY_AFTER]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=1800).contains(&retry), "{retry}");
}

/// Acceptance 3: a new and an existing address get the same status and
/// body; the mail differs.
#[tokio::test]
async fn registration_never_reveals_an_existing_account() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut b = Browser::new("198.51.100.24");
    let new = b
        .post(
            &node,
            "/registrations",
            json!({"email": "grace@example.com", "password": PASSWORD}),
        )
        .await;
    let again = b
        .post(
            &node,
            "/registrations",
            json!({"email": "GRACE@example.com", "password": "another fine passphrase"}),
        )
        .await;
    assert_eq!((new.status, &new.body), (again.status, &again.body));
    assert_eq!(
        world
            .mail
            .to("identity.verify_email", "grace@example.com")
            .len(),
        1
    );
    assert_eq!(
        world
            .mail
            .to("identity.account_exists", "GRACE@example.com")
            .len(),
        1
    );
    assert!(
        world
            .mail
            .to("identity.account_exists", "GRACE@example.com")[0]
            .secret
            .is_empty()
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.users")
            .await,
        1
    );
    // Both paths hashed exactly once.
    assert_eq!(node.handle.hash_counts().unwrap().0, 2);
}

#[tokio::test]
async fn verification_resend_and_confirm() {
    let Some(world) = World::new(&section("", &[("email_address", 2, "1h")], "")).await else {
        return;
    };
    let node = world.instance().await;
    let mut b = Browser::new("198.51.100.25");
    b.post(
        &node,
        "/registrations",
        json!({"email": "kat@example.com", "password": PASSWORD}),
    )
    .await;
    let first = world.mail.token("identity.verify_email", "kat@example.com");
    let resend = b
        .post(
            &node,
            "/email-verifications",
            json!({"email": "kat@example.com"}),
        )
        .await;
    assert_eq!(resend.body, json!({"status": "sent_if_eligible"}));
    let unknown = b
        .post(
            &node,
            "/email-verifications",
            json!({"email": "nobody@example.com"}),
        )
        .await;
    assert_eq!(
        (unknown.status, &unknown.body),
        (resend.status, &resend.body)
    );
    let invalid = b
        .post(&node, "/email-verifications", json!({"email": "x"}))
        .await;
    assert_eq!(invalid.code(), "validation_failed");
    // The per-address cap: two mails to kat this hour, the third is refused.
    let limited = b
        .post(
            &node,
            "/email-verifications",
            json!({"email": "kat@example.com"}),
        )
        .await;
    assert_eq!(
        (limited.status.as_u16(), limited.code()),
        (429, "rate_limited")
    );
    // The resend superseded the first link (acceptance 8).
    let stale = b
        .post(
            &node,
            "/email-verifications/confirm",
            json!({"token": first}),
        )
        .await;
    assert_eq!(
        (stale.status.as_u16(), stale.code()),
        (401, "invalid_token")
    );
    let token = world.mail.token("identity.verify_email", "kat@example.com");
    let ok = b
        .post(
            &node,
            "/email-verifications/confirm",
            json!({"token": token, "transport": "bearer"}),
        )
        .await;
    assert_eq!(ok.status, 201, "{ok:?}");
    assert!(
        ok.body["session"]["token"]
            .as_str()
            .unwrap()
            .starts_with("mzs_")
    );
    assert_eq!(ok.body["session"]["method"], "email_token");
    assert_eq!(ok.body["user"]["email_verified"], true);
    assert!(ok.body["csrf_token"].is_string());
    assert!(ok.set_cookie().is_none(), "bearer sessions set no cookie");
    // Single use.
    let replay = b
        .post(
            &node,
            "/email-verifications/confirm",
            json!({"token": token}),
        )
        .await;
    assert_eq!(replay.code(), "invalid_token");
}

#[tokio::test]
async fn token_failures_are_rate_limited_per_ip() {
    let Some(world) = World::new(&section("", &[("token_ip", 3, "10m")], "")).await else {
        return;
    };
    let node = world.instance().await;
    let mut b = Browser::new("198.51.100.26");
    for path in [
        "/email-verifications/confirm",
        "/magic-links/consume",
        "/password-resets/complete",
    ] {
        let mut body = json!({"token": "A".repeat(43)});
        if path.ends_with("complete") {
            body["new_password"] = json!(PASSWORD);
        }
        let answer = b.post(&node, path, body).await;
        assert_eq!(answer.code(), "invalid_token", "{path}");
    }
    let limited = b
        .post(
            &node,
            "/me/email-changes/confirm",
            json!({"token": "B".repeat(43)}),
        )
        .await;
    assert_eq!(
        (limited.status.as_u16(), limited.code()),
        (429, "rate_limited")
    );
    // Another address is not affected.
    let other = Browser::new("198.51.100.27")
        .post(
            &node,
            "/magic-links/consume",
            json!({"token": "C".repeat(43)}),
        )
        .await;
    assert_eq!(other.code(), "invalid_token");
}

/// Acceptance 4: unknown email and wrong password answer identically and
/// each costs exactly one Argon2id verification.
#[tokio::test]
async fn sign_in_is_identical_for_unknown_accounts_and_wrong_passwords() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    signed_up(&world, &node, "linus@example.com", PASSWORD).await;
    let count = || node.handle.hash_counts().unwrap();
    let (_, before) = count();
    let mut b = Browser::new("198.51.100.30");
    let unknown = b
        .post(
            &node,
            "/sessions",
            json!({"email": "nobody@example.com", "password": PASSWORD}),
        )
        .await;
    let (_, after_unknown) = count();
    let wrong = b
        .post(
            &node,
            "/sessions",
            json!({"email": "linus@example.com", "password": "wrong but long enough"}),
        )
        .await;
    let (_, after_wrong) = count();
    assert_eq!(after_unknown - before, 1);
    assert_eq!(after_wrong - after_unknown, 1);
    assert_eq!(
        (unknown.status.as_u16(), unknown.code()),
        (401, "invalid_credentials")
    );
    assert_eq!((unknown.status, &unknown.body), (wrong.status, &wrong.body));
    let strip = |a: &Answer| {
        let mut h = a.headers.clone();
        h.remove("x-request-id");
        h.remove(header::CONTENT_LENGTH);
        h
    };
    assert_eq!(strip(&unknown), strip(&wrong));
    let ok = b
        .post(
            &node,
            "/sessions",
            json!({"email": "LINUS@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(ok.status, 201);
    assert_eq!(
        count().1 - after_wrong,
        1,
        "a successful sign-in verifies once too"
    );
    let cookie = ok.set_cookie().unwrap();
    assert!(
        cookie.starts_with("__Host-merezha_session=mzs_"),
        "{cookie}"
    );
    for attribute in [
        "Path=/",
        "HttpOnly",
        "SameSite=Lax",
        "Secure",
        "Max-Age=2592000",
    ] {
        assert!(cookie.contains(attribute), "{cookie} lacks {attribute}");
    }
    assert!(
        ok.body["session"]["token"].is_null(),
        "the cookie carries the token"
    );
    // A malformed address is a validation error, not a credential check.
    let invalid = b
        .post(
            &node,
            "/sessions",
            json!({"email": "linus", "password": PASSWORD}),
        )
        .await;
    assert_eq!(invalid.code(), "validation_failed");
}

#[tokio::test]
async fn sign_in_reveals_unverified_and_disabled_only_after_the_password() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut b = Browser::new("198.51.100.31");
    b.post(
        &node,
        "/registrations",
        json!({"email": "una@example.com", "password": PASSWORD}),
    )
    .await;
    let unverified = b
        .post(
            &node,
            "/sessions",
            json!({"email": "una@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(
        (unverified.status.as_u16(), unverified.code()),
        (403, "email_unverified")
    );
    let wrong = b
        .post(
            &node,
            "/sessions",
            json!({"email": "una@example.com", "password": "not her password"}),
        )
        .await;
    assert_eq!(wrong.code(), "invalid_credentials");
    signed_up(&world, &node, "dis@example.com", PASSWORD).await;
    world
        .execute("UPDATE merezha_identity.users SET status = 'disabled' WHERE email_normalized = 'dis@example.com'")
        .await;
    let disabled = b
        .post(
            &node,
            "/sessions",
            json!({"email": "dis@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(
        (disabled.status.as_u16(), disabled.code()),
        (403, "account_disabled")
    );
    let wrong = b
        .post(
            &node,
            "/sessions",
            json!({"email": "dis@example.com", "password": "not her password"}),
        )
        .await;
    assert_eq!(wrong.code(), "invalid_credentials");
    // With verification not required, the unverified account signs in.
    let Some(world) = World::new(&section("require_verified_email = false", &[], "")).await else {
        return;
    };
    let node = world.instance().await;
    b.post(
        &node,
        "/registrations",
        json!({"email": "una@example.com", "password": PASSWORD}),
    )
    .await;
    let ok = b
        .post(
            &node,
            "/sessions",
            json!({"email": "una@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(ok.status, 201);
    assert_eq!(ok.body["user"]["email_verified"], false);
}

#[tokio::test]
async fn sign_in_rate_limits() {
    let limits = [("sign_in_ip", 4, "5m"), ("sign_in_account_ip", 2, "15m")];
    let Some(world) = World::new(&section("", &limits, "")).await else {
        return;
    };
    let node = world.instance().await;
    signed_up(&world, &node, "rob@example.com", PASSWORD).await;
    let mut b = Browser::new("198.51.100.32");
    for _ in 0..2 {
        let answer = b
            .post(
                &node,
                "/sessions",
                json!({"email": "rob@example.com", "password": "wrong password!!"}),
            )
            .await;
        assert_eq!(answer.code(), "invalid_credentials");
    }
    // Two failures spent sign_in.account_ip: even the right password waits.
    let locked = b
        .post(
            &node,
            "/sessions",
            json!({"email": "rob@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(
        (locked.status.as_u16(), locked.code()),
        (429, "rate_limited")
    );
    // sign_in.ip is charged on arrival: the fourth request from this IP is
    // the last one admitted.
    let other = b
        .post(
            &node,
            "/sessions",
            json!({"email": "x@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(other.code(), "invalid_credentials");
    let arrival = b
        .post(
            &node,
            "/sessions",
            json!({"email": "y@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(arrival.code(), "rate_limited");
    // Another IP signs in.
    let ok = Browser::new("198.51.100.33")
        .post(
            &node,
            "/sessions",
            json!({"email": "rob@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(ok.status, 201);
    // A denial is audited once per bucket per period.
    let audited = world
        .scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'rate_limit.exceeded'")
        .await;
    assert_eq!(audited, 2);
}

#[tokio::test]
async fn a_saturated_hasher_answers_overloaded() {
    let tables = "";
    let text = section("", &[], tables).replace(
        "hash_concurrency = 4",
        "hash_concurrency = 1\nhash_queue_timeout = \"10ms\"",
    );
    let Some(world) = World::new(&text).await else {
        return;
    };
    let node = world.instance().await;
    let calls = (0..12).map(|n| {
        let node = node.clone();
        async move {
            Browser::new(&format!("198.51.100.{}", 100 + n))
                .post(
                    &node,
                    "/sessions",
                    json!({"email": format!("p{n}@example.com"), "password": PASSWORD}),
                )
                .await
        }
    });
    let answers = futures_join(calls).await;
    let overloaded: Vec<&Answer> = answers
        .iter()
        .filter(|a| a.code() == "overloaded")
        .collect();
    assert!(
        !overloaded.is_empty(),
        "{:?}",
        answers.iter().map(Answer::code).collect::<Vec<_>>()
    );
    for answer in overloaded {
        assert_eq!(answer.status, 503);
        assert!(answer.headers.contains_key(header::RETRY_AFTER));
    }
    assert!(
        answers
            .iter()
            .all(|a| matches!(a.code(), "overloaded" | "invalid_credentials"))
    );
}

async fn futures_join<F: std::future::Future<Output = Answer> + Send + 'static>(
    calls: impl Iterator<Item = F>,
) -> Vec<Answer> {
    let handles: Vec<_> = calls.map(tokio::spawn).collect();
    let mut out = Vec::new();
    for handle in handles {
        out.push(handle.await.unwrap());
    }
    out
}

#[tokio::test]
async fn session_routes() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut ada = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    let mut phone = Browser::new("2001:db8:abcd:12::7");
    phone.user_agent = "Phone/2".into();
    let bearer = phone
        .post(
            &node,
            "/sessions",
            json!({"email": "ada@example.com", "password": PASSWORD, "transport": "bearer"}),
        )
        .await;
    assert_eq!(bearer.status, 201);
    phone.origin = None;
    let session = ada.get(&node, "/session").await;
    assert_eq!(session.status, 200);
    assert_eq!(session.body["session"]["current"], true);
    assert_eq!(session.body["user"]["email"], "ada@example.com");
    assert_eq!(session.body["csrf_token"], json!(ada.csrf.clone().unwrap()));
    let list = ada.get(&node, "/sessions").await;
    let items = list.body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    let others: Vec<&Value> = items.iter().filter(|i| i["current"] == false).collect();
    assert_eq!(others[0]["ip"], "2001:db8:abcd::/48");
    assert_eq!(others[0]["user_agent"], "Phone/2");
    assert_eq!(
        items.iter().find(|i| i["current"] == true).unwrap()["ip"],
        "198.51.100.0/24"
    );
    // Unknown and foreign session ids are not found.
    let missing = ada
        .send(
            &node,
            "DELETE",
            "/sessions/ses_0192f5c4e1b07a3c9d2e4f6a8b0c1d2e",
            None,
        )
        .await;
    assert_eq!(
        (missing.status.as_u16(), missing.code()),
        (404, "not_found")
    );
    let wrong_type = ada
        .send(
            &node,
            "DELETE",
            "/sessions/usr_0192f5c4e1b07a3c9d2e4f6a8b0c1d2e",
            None,
        )
        .await;
    assert_eq!(wrong_type.code(), "not_found");
    let phone_id = others[0]["id"].as_str().unwrap().to_owned();
    let revoked = ada
        .send(&node, "DELETE", &format!("/sessions/{phone_id}"), None)
        .await;
    assert_eq!(revoked.status, 204);
    let gone = phone.get(&node, "/session").await;
    assert_eq!(
        (gone.status.as_u16(), gone.code()),
        (401, "unauthenticated")
    );
    // Log out everywhere, keeping the current one.
    let mut laptop = Browser::new("198.51.100.40");
    laptop
        .post(
            &node,
            "/sessions",
            json!({"email": "ada@example.com", "password": PASSWORD}),
        )
        .await;
    let all = ada
        .post(&node, "/sessions/revoke-all", json!({"keep_current": true}))
        .await;
    assert_eq!(all.body, json!({"revoked": 1}));
    assert_eq!(
        laptop.get(&node, "/session").await.code(),
        "unauthenticated"
    );
    assert_eq!(ada.get(&node, "/session").await.status, 200);
    // Sign out clears the cookie; the dead cookie is refused and cleared.
    let stale = ada.clone();
    let out = ada.send(&node, "DELETE", "/session", None).await;
    assert_eq!(out.status, 204);
    assert!(out.set_cookie().unwrap().contains("Max-Age=0"));
    let mut stale = stale;
    let refused = stale.get(&node, "/session").await;
    assert_eq!(
        (refused.status.as_u16(), refused.code()),
        (401, "unauthenticated")
    );
    assert!(
        refused.set_cookie().unwrap().contains("Max-Age=0"),
        "{refused:?}"
    );
    assert!(stale.cookie.is_none());
    // Anonymous calls to session routes.
    let mut anonymous = Browser::new("198.51.100.41");
    for (method, path) in [
        ("GET", "/session"),
        ("DELETE", "/session"),
        ("GET", "/sessions"),
        ("GET", "/me"),
    ] {
        let answer = anonymous.send(&node, method, path, None).await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (401, "unauthenticated"),
            "{method} {path}"
        );
    }
    let everything = anonymous
        .post(&node, "/sessions/revoke-all", json!({}))
        .await;
    assert_eq!(everything.code(), "unauthenticated");
    // Both credential kinds at once.
    let mut both = phone.clone();
    both.cookie = Some("mzs_x".into());
    let ambiguous = both.get(&node, "/session").await;
    assert_eq!(
        (ambiguous.status.as_u16(), ambiguous.code()),
        (400, "ambiguous_credentials")
    );
    // Malformed credentials never fall back to anonymous.
    for value in ["Bearer nope", "Basic abc", "Bearer mzk_aaaaaaaaaaaaaaaa_x"] {
        let mut request = anonymous.request(
            "POST",
            "/magic-links",
            Some(&json!({"email": "a@example.com"})),
        );
        request
            .headers_mut()
            .insert(header::AUTHORIZATION, value.parse().unwrap());
        let answer = read(node.router.clone().oneshot(request).await.unwrap()).await;
        assert_eq!(answer.code(), "unauthenticated", "{value}");
    }
}

#[tokio::test]
async fn reauthentication_rotates_and_is_rate_limited() {
    let Some(world) = World::new(&section("", &[("reauth_session", 2, "15m")], "")).await else {
        return;
    };
    let node = world.instance().await;
    let mut ada = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    let old = ada.clone();
    let before = ada.get(&node, "/session").await.body["session"]["id"].clone();
    let ok = ada
        .post(
            &node,
            "/session/reauthenticate",
            json!({"password": PASSWORD}),
        )
        .await;
    assert_eq!(ok.status, 200, "{ok:?}");
    assert_ne!(ok.body["session"]["id"], before, "rotated");
    assert!(ok.body["user"].is_null());
    assert!(ok.set_cookie().is_some());
    let mut old = old;
    old.csrf = None;
    assert_eq!(old.get(&node, "/session").await.code(), "unauthenticated");
    for _ in 0..2 {
        let wrong = ada
            .post(
                &node,
                "/session/reauthenticate",
                json!({"password": "not the password"}),
            )
            .await;
        assert_eq!(
            (wrong.status.as_u16(), wrong.code()),
            (401, "invalid_credentials")
        );
    }
    let limited = ada
        .post(
            &node,
            "/session/reauthenticate",
            json!({"password": PASSWORD}),
        )
        .await;
    assert_eq!(limited.code(), "rate_limited");
}

#[tokio::test]
async fn magic_link_routes() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut b = Browser::new("198.51.100.50");
    // An unknown address gets a sign-up link; the answer is the same.
    let unknown = b
        .post(
            &node,
            "/magic-links",
            json!({"email": "New@Example.com", "return_to": "/welcome?x=1"}),
        )
        .await;
    assert_eq!(unknown.body, json!({"status": "sent_if_eligible"}));
    let link = world.mail.to("identity.magic_link", "New@example.com");
    assert!(
        link[0].secret["link"]
            .as_str()
            .unwrap()
            .starts_with("https://app.example/auth/magic-link#token=")
    );
    let token = link[0].token();
    let ok = b
        .post(&node, "/magic-links/consume", json!({"token": token}))
        .await;
    assert_eq!(ok.status, 201, "{ok:?}");
    assert_eq!(ok.body["return_to"], "https://app.example/welcome?x=1");
    assert_eq!(ok.body["session"]["method"], "magic_link");
    assert_eq!(ok.body["user"]["email"], "New@Example.com");
    assert_eq!(ok.body["user"]["credentials"]["password"], false);
    let again = b
        .post(&node, "/magic-links/consume", json!({"token": token}))
        .await;
    assert_eq!(again.code(), "invalid_token");
    // A known account gets a sign-in link.
    b.post(&node, "/magic-links", json!({"email": "new@example.com"}))
        .await;
    let token = world.mail.token("identity.magic_link", "New@example.com");
    let ok = Browser::new("198.51.100.51")
        .post(&node, "/magic-links/consume", json!({"token": token}))
        .await;
    assert_eq!(ok.status, 201);
    assert!(ok.body["return_to"].is_null());
    for return_to in [
        "//evil.example",
        "https://evil.example",
        "/\\evil",
        "/%2F%2Fevil",
    ] {
        let answer = b
            .post(
                &node,
                "/magic-links",
                json!({"email": "new@example.com", "return_to": return_to}),
            )
            .await;
        assert_eq!(
            detail(&answer),
            vec![("/return_to".into(), "invalid".into())],
            "{return_to}"
        );
    }
    let invalid = b.post(&node, "/magic-links", json!({})).await;
    assert_eq!(invalid.code(), "validation_failed");
    // A disabled account gets nothing, and its live link is refused.
    b.post(&node, "/magic-links", json!({"email": "new@example.com"}))
        .await;
    let token = world.mail.token("identity.magic_link", "New@example.com");
    world
        .execute("UPDATE merezha_identity.users SET status = 'disabled'")
        .await;
    // Disabling also kills the account's sessions at the edge.
    assert_eq!(b.get(&node, "/session").await.code(), "unauthenticated");
    let disabled = Browser::new("198.51.100.53")
        .post(&node, "/magic-links/consume", json!({"token": token}))
        .await;
    assert_eq!(
        (disabled.status.as_u16(), disabled.code()),
        (403, "account_disabled")
    );
    let sent = world.mail.sent().len();
    b.post(&node, "/magic-links", json!({"email": "new@example.com"}))
        .await;
    assert_eq!(world.mail.sent().len(), sent);
}

#[tokio::test]
async fn magic_link_sign_up_follows_the_registration_mode() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut b = Browser::new("198.51.100.52");
    b.post(&node, "/magic-links", json!({"email": "late@example.com"}))
        .await;
    let token = world.mail.token("identity.magic_link", "late@example.com");
    // Registration closed after the link went out; the open instance stops.
    node.jobs
        .shutdown(std::time::Instant::now() + Duration::from_secs(5))
        .await;
    let text = section("registration = \"closed\"", &[], "");
    let closed = World {
        test: world.test,
        db: world.db.clone(),
        clock: world.clock.clone(),
        mail: world.mail.clone(),
        section: text,
        endpoints: Default::default(),
    };
    let node = closed.instance().await;
    let refused = b
        .post(&node, "/magic-links/consume", json!({"token": token}))
        .await;
    assert_eq!(
        (refused.status.as_u16(), refused.code()),
        (403, "registration_closed")
    );
    // With registration closed, unknown addresses get no link at all.
    let sent = closed.mail.sent().len();
    let answer = b
        .post(&node, "/magic-links", json!({"email": "other@example.com"}))
        .await;
    assert_eq!(answer.status, 202);
    assert_eq!(closed.mail.sent().len(), sent);
    // Without the magic_links table the routes do not exist.
    let Some(world) =
        World::new(&section("", &[], "").replace("[modules.identity.magic_links]\n", "")).await
    else {
        return;
    };
    let node = world.instance().await;
    let answer = b
        .post(&node, "/magic-links", json!({"email": "a@example.com"}))
        .await;
    assert_eq!(answer.status, 404);
}

#[tokio::test]
async fn password_reset_routes() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut ada = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    let mut b = Browser::new("198.51.100.60");
    let answer = b
        .post(
            &node,
            "/password-resets",
            json!({"email": "ada@example.com"}),
        )
        .await;
    let unknown = b
        .post(
            &node,
            "/password-resets",
            json!({"email": "nobody@example.com"}),
        )
        .await;
    assert_eq!(
        (answer.status, &answer.body),
        (unknown.status, &unknown.body)
    );
    assert_eq!(answer.body, json!({"status": "sent_if_eligible"}));
    assert_eq!(
        b.post(&node, "/password-resets", json!({"email": 3}))
            .await
            .code(),
        "validation_failed"
    );
    let token = world
        .mail
        .token("identity.password_reset", "ada@example.com");
    // Policy refusals keep the token usable.
    for (password, code) in [
        ("short", "password_too_short"),
        ("password1234", "password_blocklisted"),
    ] {
        let refused = b
            .post(
                &node,
                "/password-resets/complete",
                json!({"token": token, "new_password": password}),
            )
            .await;
        assert_eq!(refused.code(), code);
    }
    let ok = b
        .post(
            &node,
            "/password-resets/complete",
            json!({"token": token, "new_password": "a brand new passphrase"}),
        )
        .await;
    assert_eq!(ok.status, 201, "{ok:?}");
    // Every older session is gone; the old password no longer works.
    assert_eq!(ada.get(&node, "/session").await.code(), "unauthenticated");
    let old = b
        .post(
            &node,
            "/sessions",
            json!({"email": "ada@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(old.code(), "invalid_credentials");
    let new = b
        .post(
            &node,
            "/sessions",
            json!({"email": "ada@example.com", "password": "a brand new passphrase"}),
        )
        .await;
    assert_eq!(new.status, 201);
    assert_eq!(
        world
            .mail
            .to(
                "identity.password_changed",
                &format!("user:{}", ok.body["user"]["id"].as_str().unwrap())
            )
            .len(),
        1
    );
    let replay = b
        .post(
            &node,
            "/password-resets/complete",
            json!({"token": token, "new_password": "yet another passphrase"}),
        )
        .await;
    assert_eq!(replay.code(), "invalid_token");
}

#[tokio::test]
async fn me_routes() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut ada = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    let me = ada.get(&node, "/me").await;
    assert_eq!(me.status, 200);
    assert!(me.body["user"]["id"].as_str().unwrap().starts_with("usr_"));
    assert_eq!(
        me.body["user"]["credentials"],
        json!({"password": true, "passkeys": 0, "oauth": []})
    );
    let patched = ada
        .send(
            &node,
            "PATCH",
            "/me",
            Some(json!({"display_name": "Ada L.", "locale": "fr"})),
        )
        .await;
    assert_eq!(patched.status, 200);
    assert_eq!(
        (
            patched.body["user"]["display_name"].clone(),
            patched.body["user"]["locale"].clone()
        ),
        (json!("Ada L."), json!("fr"))
    );
    let cleared = ada
        .send(&node, "PATCH", "/me", Some(json!({"locale": null})))
        .await;
    assert_eq!(cleared.body["user"]["locale"], Value::Null);
    assert_eq!(cleared.body["user"]["display_name"], "Ada L.");
    let invalid = ada
        .send(
            &node,
            "PATCH",
            "/me",
            Some(json!({"display_name": "", "locale": "1"})),
        )
        .await;
    let mut found = detail(&invalid);
    found.sort();
    assert_eq!(
        found,
        vec![
            ("/display_name".into(), "invalid".into()),
            ("/locale".into(), "invalid".into())
        ]
    );
    // Password change needs a recent session.
    world.clock.advance(Duration::from_secs(11 * 60));
    let stale = ada
        .send(
            &node,
            "PUT",
            "/me/password",
            Some(json!({"new_password": "a brand new passphrase"})),
        )
        .await;
    assert_eq!(
        (stale.status.as_u16(), stale.code()),
        (401, "reauthentication_required")
    );
    ada.post(
        &node,
        "/session/reauthenticate",
        json!({"password": PASSWORD}),
    )
    .await;
    let short = ada
        .send(
            &node,
            "PUT",
            "/me/password",
            Some(json!({"new_password": "short"})),
        )
        .await;
    assert_eq!(short.code(), "password_too_short");
    let mut other = Browser::new("198.51.100.70");
    other
        .post(
            &node,
            "/sessions",
            json!({"email": "ada@example.com", "password": PASSWORD}),
        )
        .await;
    let before = ada.get(&node, "/session").await.body["session"]["id"].clone();
    // UUIDv7 audit ids order by millisecond; move the fake clock on.
    world.clock.advance(Duration::from_secs(1));
    let changed = ada
        .send(
            &node,
            "PUT",
            "/me/password",
            Some(json!({"new_password": "a brand new passphrase"})),
        )
        .await;
    assert_eq!(changed.status, 200, "{changed:?}");
    assert_ne!(changed.body["session"]["id"], before);
    assert_eq!(ada.get(&node, "/session").await.status, 200);
    assert_eq!(other.get(&node, "/session").await.code(), "unauthenticated");
    // Security events: the caller's own audit rows, newest first, paged.
    let page = ada.get(&node, "/me/security-events?limit=2").await;
    assert_eq!(page.status, 200);
    assert_eq!(page.body["items"].as_array().unwrap().len(), 2);
    assert_eq!(page.body["items"][0]["action"], "user.password_changed");
    let cursor = page.body["next_cursor"].as_str().unwrap();
    let next = ada
        .get(
            &node,
            &format!("/me/security-events?cursor={cursor}&limit=100"),
        )
        .await;
    let actions: Vec<&str> = next.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["action"].as_str().unwrap())
        .collect();
    assert!(
        actions.contains(&"user.registered") && actions.contains(&"user.email_verified"),
        "{actions:?}"
    );
    assert!(next.body["next_cursor"].is_null());
    assert_eq!(
        ada.get(&node, "/me/security-events?limit=0").await.code(),
        "validation_failed"
    );
}

#[tokio::test]
async fn email_change_routes() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut ada = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    let mut other = signed_up(&world, &node, "other@example.com", PASSWORD).await;
    let invalid = ada
        .post(&node, "/me/email-changes", json!({"new_email": "nope"}))
        .await;
    assert_eq!(invalid.code(), "validation_failed");
    let ok = ada
        .post(
            &node,
            "/me/email-changes",
            json!({"new_email": "Ada@New.example"}),
        )
        .await;
    assert_eq!(ok.body, json!({"status": "sent_if_eligible"}));
    assert_eq!(
        world
            .mail
            .to("identity.email_change_notice", "ada@example.com")
            .len(),
        1
    );
    let token = world
        .mail
        .token("identity.email_change_confirm", "Ada@new.example");
    // The confirming browser holds Ada's session: rotated; others revoked.
    let mut laptop = Browser::new("198.51.100.80");
    laptop
        .post(
            &node,
            "/sessions",
            json!({"email": "ada@example.com", "password": PASSWORD}),
        )
        .await;
    let confirmed = ada
        .post(&node, "/me/email-changes/confirm", json!({"token": token}))
        .await;
    assert_eq!(confirmed.status, 200, "{confirmed:?}");
    assert_eq!(confirmed.body["user"]["email"], "Ada@New.example");
    assert!(confirmed.body["session"]["id"].is_string());
    assert_eq!(ada.get(&node, "/session").await.status, 200);
    assert_eq!(
        laptop.get(&node, "/session").await.code(),
        "unauthenticated"
    );
    // A taken address is refused at confirmation.
    other
        .post(
            &node,
            "/me/email-changes",
            json!({"new_email": "ada@new.example"}),
        )
        .await;
    let token = world
        .mail
        .token("identity.email_change_confirm", "ada@new.example");
    // Only the session that asked confirms; anyone else changes nothing
    // and the token stays usable.
    let elsewhere = Browser::new("198.51.100.81")
        .post(&node, "/me/email-changes/confirm", json!({"token": token}))
        .await;
    assert_eq!(
        (elsewhere.status.as_u16(), elsewhere.code()),
        (401, "sign_in_required")
    );
    let wrong_session = ada
        .post(&node, "/me/email-changes/confirm", json!({"token": token}))
        .await;
    assert_eq!(wrong_session.code(), "sign_in_required");
    let taken = other
        .post(&node, "/me/email-changes/confirm", json!({"token": token}))
        .await;
    assert_eq!((taken.status.as_u16(), taken.code()), (409, "email_taken"));
    let used = other
        .post(&node, "/me/email-changes/confirm", json!({"token": token}))
        .await;
    assert_eq!(used.code(), "invalid_token");
    // A stale session must reauthenticate first.
    world.clock.advance(Duration::from_secs(11 * 60));
    let stale = ada
        .post(
            &node,
            "/me/email-changes",
            json!({"new_email": "x@example.com"}),
        )
        .await;
    assert_eq!(stale.code(), "reauthentication_required");
}

#[tokio::test]
async fn account_deletion() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut ada = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    let mismatch = ada
        .send(
            &node,
            "DELETE",
            "/me",
            Some(json!({"confirm_email": "eve@example.com"})),
        )
        .await;
    assert_eq!(
        detail(&mismatch),
        vec![("/confirm_email".into(), "mismatch".into())]
    );
    world.clock.advance(Duration::from_secs(11 * 60));
    let stale = ada
        .send(
            &node,
            "DELETE",
            "/me",
            Some(json!({"confirm_email": "ada@example.com"})),
        )
        .await;
    assert_eq!(stale.code(), "reauthentication_required");
    ada.post(
        &node,
        "/session/reauthenticate",
        json!({"password": PASSWORD}),
    )
    .await;
    let deleted = ada
        .send(
            &node,
            "DELETE",
            "/me",
            Some(json!({"confirm_email": "ADA@example.com"})),
        )
        .await;
    assert_eq!(deleted.status, 204);
    assert!(deleted.set_cookie().unwrap().contains("Max-Age=0"));
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.users")
            .await,
        0
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.sessions")
            .await,
        0
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.password_credentials")
            .await,
        0
    );
    // Audit rows stay, keyed by the user id only.
    assert!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'user.deleted'"
            )
            .await
            == 1
    );
    let event = world
        .scalar("SELECT count(*) FROM merezha_jobs.outbox WHERE topic = 'identity.user_deleted'")
        .await;
    assert_eq!(event, 1);
    let mut fresh = Browser::new("198.51.100.90");
    assert_eq!(
        fresh
            .post(
                &node,
                "/sessions",
                json!({"email": "ada@example.com", "password": PASSWORD})
            )
            .await
            .code(),
        "invalid_credentials"
    );
}

/// Acceptance 8: tokens expire.
#[tokio::test]
async fn tokens_expire() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let mut b = Browser::new("198.51.100.95");
    signed_up(&world, &node, "ada@example.com", PASSWORD).await;
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
    world.clock.advance(Duration::from_secs(15 * 60));
    let expired = b
        .post(&node, "/magic-links/consume", json!({"token": magic}))
        .await;
    assert_eq!(expired.code(), "invalid_token");
    // Expired rows are deleted on the attempt, not left for a second try.
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_identity.one_time_tokens WHERE purpose = 'magic_link'"
            )
            .await,
        0
    );
    let fresh = b
        .post(
            &node,
            "/password-resets/complete",
            json!({"token": reset, "new_password": "a brand new passphrase"}),
        )
        .await;
    assert_eq!(fresh.status, 201, "a 30-minute reset token is still alive");
}
