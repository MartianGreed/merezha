//! Secrets never leak: acceptance 10 (planted secrets in no log line, span
//! or metric), acceptance 11 (tokens only in `secret_data`, the outbox
//! record flagged and its payload null after relay), the rollback of a
//! failed notification request, and the loopback `identity.dev_link`.
mod support;

use merezha_telemetry::testing::Harness;
use serde_json::{Value, json};
use std::{sync::atomic::Ordering, time::Duration};
use support::*;

const CANARY_PASSWORD: &str = "planted-canary-password-7781";
const CANARY_NEW_PASSWORD: &str = "planted-canary-new-password-3349";
const CANARY_EMAIL: &str = "canary.address@spotted.example";
const CANARY_NEW_EMAIL: &str = "second.canary@spotted.example";
const CANARY_AGENT: &str = "PlantedCanaryAgent/9.4";

/// Every text of every identity row, for "is this secret stored" checks.
async fn stored(world: &World) -> String {
    let client = world.db.acquire(soon()).await.unwrap();
    let mut text = String::new();
    for table in [
        "users",
        "password_credentials",
        "sessions",
        "one_time_tokens",
        "rate_limits",
        "audit_events",
    ] {
        for row in client
            .query(
                &format!("SELECT row_to_json(t)::text FROM merezha_identity.{table} t"),
                &[],
            )
            .await
            .unwrap()
        {
            text.push_str(row.get::<_, &str>(0));
        }
    }
    text
}

/// Acceptance 10: run every flow with planted secrets and find none of
/// them in a log line, span or metric.
#[tokio::test]
async fn planted_secrets_never_reach_logs_spans_or_metrics() {
    let harness = Harness::new();
    let Some(world) = World::new(&section("", &[("email_ip", 40, "1h")], "")).await else {
        return;
    };
    let meter = harness.telemetry().meter("identity");
    let node = world.instance_with(Some(&meter)).await;
    let mut planted: Vec<String> = vec![
        CANARY_PASSWORD.into(),
        CANARY_NEW_PASSWORD.into(),
        CANARY_EMAIL.into(),
        CANARY_NEW_EMAIL.into(),
        "spotted.example".into(),
        CANARY_AGENT.into(),
        "203.0.113.199".into(),
        "2001:db8:5eed".into(),
    ];
    let mut b = Browser::new("203.0.113.199");
    b.user_agent = CANARY_AGENT.into();
    b.post(
        &node,
        "/registrations",
        json!({"email": CANARY_EMAIL, "password": CANARY_PASSWORD}),
    )
    .await;
    b.post(
        &node,
        "/registrations",
        json!({"email": CANARY_EMAIL, "password": CANARY_PASSWORD}),
    )
    .await;
    let verify = world.mail.token("identity.verify_email", CANARY_EMAIL);
    planted.push(verify.clone());
    let answer = b
        .post(
            &node,
            "/email-verifications/confirm",
            json!({"token": verify}),
        )
        .await;
    assert_eq!(answer.status, 201);
    planted.push(b.cookie.clone().unwrap());
    planted.push(b.csrf.clone().unwrap());
    let failed = Browser::new("2001:db8:5eed::1")
        .post(
            &node,
            "/sessions",
            json!({"email": CANARY_EMAIL, "password": "planted wrong password"}),
        )
        .await;
    assert_eq!(failed.code(), "invalid_credentials");
    planted.push("planted wrong password".into());
    let mut phone = Browser::new("2001:db8:5eed::2");
    phone.user_agent = CANARY_AGENT.into();
    phone
        .post(
            &node,
            "/sessions",
            json!({"email": CANARY_EMAIL, "password": CANARY_PASSWORD, "transport": "bearer"}),
        )
        .await;
    planted.push(phone.bearer.clone().unwrap());
    let list = b.get(&node, "/sessions").await;
    for item in list.body["items"].as_array().unwrap() {
        let id = item["id"].as_str().unwrap().to_owned();
        let hex = id.trim_start_matches("ses_").to_owned();
        planted.push(id);
        planted.push(hex);
    }
    let asked = b
        .post(
            &node,
            "/magic-links",
            json!({"email": CANARY_EMAIL, "return_to": "/planted-return"}),
        )
        .await;
    assert_eq!(asked.status, 202, "{asked:?}");
    let magic = world.mail.token("identity.magic_link", CANARY_EMAIL);
    planted.push(magic.clone());
    Browser::new("203.0.113.199")
        .post(&node, "/magic-links/consume", json!({"token": magic}))
        .await;
    let asked = b
        .post(&node, "/password-resets", json!({"email": CANARY_EMAIL}))
        .await;
    assert_eq!(asked.status, 202, "{asked:?}");
    let reset = world.mail.token("identity.password_reset", CANARY_EMAIL);
    planted.push(reset.clone());
    let mut fresh = Browser::new("203.0.113.199");
    fresh
        .post(
            &node,
            "/password-resets/complete",
            json!({"token": reset, "new_password": CANARY_NEW_PASSWORD}),
        )
        .await;
    planted.push(fresh.cookie.clone().unwrap());
    let asked = fresh
        .post(
            &node,
            "/me/email-changes",
            json!({"new_email": CANARY_NEW_EMAIL}),
        )
        .await;
    assert_eq!(asked.status, 202, "{asked:?}");
    let change = world
        .mail
        .token("identity.email_change_confirm", CANARY_NEW_EMAIL);
    planted.push(change.clone());
    fresh
        .post(&node, "/me/email-changes/confirm", json!({"token": change}))
        .await;
    // Refusals: a forged CSRF token, a dead cookie, a malformed bearer.
    let mut forged = fresh.clone();
    forged.csrf = Some("planted-forged-csrf-token".into());
    planted.push("planted-forged-csrf-token".into());
    assert_eq!(
        forged
            .post(&node, "/sessions/revoke-all", json!({}))
            .await
            .code(),
        "csrf_failed"
    );
    let mut dead = Browser::new("203.0.113.199");
    dead.cookie = Some(format!("mzs_{}", "P".repeat(43)));
    assert_eq!(dead.get(&node, "/me").await.code(), "unauthenticated");
    planted.push("P".repeat(43));
    let mut nonsense = Browser::new("203.0.113.199");
    nonsense.bearer = Some("planted-bearer-garbage".into());
    assert_eq!(nonsense.get(&node, "/me").await.code(), "unauthenticated");
    planted.push("planted-bearer-garbage".into());
    let report = node.handle.cleanup_now().await.unwrap();
    assert!(report.active_sessions >= 1);
    // The edge logged every request; identity's metrics recorded.
    assert!(harness.stderr().contains("http.request.completed"));
    let logins = harness.metric("merezha.identity.login.attempts");
    assert!(
        logins
            .value(&[("identity.outcome", "success")])
            .unwrap_or(0.0)
            >= 1.0,
        "{logins:?}"
    );
    assert!(
        harness
            .metric("merezha.identity.sessions.created")
            .points()
            .len()
            >= 2
    );
    assert!(
        !harness
            .metric("merezha.identity.password_hash.duration")
            .points()
            .is_empty()
    );
    for canary in &planted {
        harness.assert_no_canary(canary);
    }
    harness.assert_catalogued();
    // No raw token is stored: only digests and Argon2id strings.
    let rows = stored(&world).await;
    for token in [&verify, &magic, &reset, &change] {
        assert!(!rows.contains(token.as_str()));
    }
    assert!(!rows.contains(&phone.bearer.clone().unwrap()));
    assert!(!rows.contains(CANARY_PASSWORD) && !rows.contains(CANARY_NEW_PASSWORD));
}

/// Acceptance 11: tokens travel only in `secret_data`; identity's own
/// outbox rows (integration events, link requests) never carry one and are
/// never flagged; the notification record (here written by the test
/// notifier for a request with secret data) is flagged and its payload is
/// null after relay.
#[tokio::test]
async fn tokens_travel_only_in_secret_data_and_vanish_at_relay() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    // No workers: the outbox stays as identity wrote it until relayed here.
    let node = world.instance_full(None, false).await;
    let mut b = Browser::new("198.51.100.150");
    b.post(
        &node,
        "/registrations",
        json!({"email": "ada@example.com", "password": PASSWORD}),
    )
    .await;
    b.post(
        &node,
        "/registrations",
        json!({"email": "ada@example.com", "password": PASSWORD}),
    )
    .await;
    b.post(
        &node,
        "/password-resets",
        json!({"email": "ada@example.com"}),
    )
    .await;
    let sent = world.mail.sent();
    let verify = sent
        .iter()
        .find(|s| s.topic == "identity.verify_email")
        .unwrap();
    let exists = sent
        .iter()
        .find(|s| s.topic == "identity.account_exists")
        .unwrap();
    let token = verify.token();
    // The request identity made: the link only in `secret_data`.
    assert!(
        !Value::Object(verify.data.clone())
            .to_string()
            .contains(&token)
    );
    assert!(verify.data.contains_key("expires_at"));
    assert!(verify.dedupe.starts_with("identity.verify_email:"));
    assert!(exists.secret.is_empty());
    let client = world.db.acquire(soon()).await.unwrap();
    let rows = client
        .query(
            "SELECT contains_secret, payload::text, source, job_kind FROM merezha_jobs.outbox ORDER BY id",
            &[],
        )
        .await
        .unwrap();
    let identity: Vec<_> = rows
        .iter()
        .filter(|r| r.get::<_, String>(2) == "identity")
        .collect();
    assert!(
        identity
            .iter()
            .any(|r| r.get::<_, Option<String>>(3).as_deref() == Some("identity.link_request"))
    );
    assert!(identity.len() >= 2, "the event and the link request");
    for row in &identity {
        assert!(
            !row.get::<_, bool>(0),
            "identity's own rows carry no secret"
        );
        assert!(!row.get::<_, Option<String>>(1).unwrap().contains(&token));
    }
    let carrying: Vec<_> = rows
        .iter()
        .filter(|r| {
            r.get::<_, Option<String>>(1)
                .is_some_and(|p| p.contains(&token))
        })
        .collect();
    assert_eq!(carrying.len(), 1);
    assert!(
        carrying[0].get::<_, bool>(0),
        "the record with the link is flagged"
    );
    node.jobs.relay_now().await.unwrap();
    let after = client
        .query(
            "SELECT payload IS NULL, relayed_at IS NOT NULL OR quarantined_at IS NOT NULL
               FROM merezha_jobs.outbox WHERE contains_secret",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(after.len(), 1);
    assert!(
        after[0].get::<_, bool>(0),
        "the flagged payload is null after relay"
    );
    assert!(after[0].get::<_, bool>(1));
}

/// A notification request that fails rolls the token and the user back.
#[tokio::test]
async fn a_failed_notification_request_rolls_everything_back() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    world.mail.fail.store(true, Ordering::SeqCst);
    let answer = Browser::new("198.51.100.151")
        .post(
            &node,
            "/registrations",
            json!({"email": "ada@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(
        (answer.status.as_u16(), answer.code()),
        (503, "unavailable")
    );
    for table in [
        "users",
        "password_credentials",
        "one_time_tokens",
        "audit_events",
    ] {
        assert_eq!(
            world
                .scalar(&format!("SELECT count(*) FROM merezha_identity.{table}"))
                .await,
            0,
            "{table}"
        );
    }
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_jobs.outbox")
            .await,
        0
    );
    world.mail.fail.store(false, Ordering::SeqCst);
    let retried = Browser::new("198.51.100.152")
        .post(
            &node,
            "/registrations",
            json!({"email": "ada@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(retried.status, 202);
    assert_eq!(
        world
            .mail
            .to("identity.verify_email", "ada@example.com")
            .len(),
        1
    );
}

/// With `dev_print_links` on loopback each link goes to stderr as
/// `identity.dev_link`, and nowhere else.
#[tokio::test]
async fn printed_links_go_to_stderr_only() {
    let harness = Harness::new();
    let Some(world) = World::new(&section("dev_print_links = true", &[], "")).await else {
        return;
    };
    let meter = harness.telemetry().meter("identity");
    let node = world.instance_with(Some(&meter)).await;
    Browser::new("198.51.100.153")
        .post(&node, "/magic-links", json!({"email": "dev@example.com"}))
        .await;
    let token = world.mail.token("identity.magic_link", "dev@example.com");
    let line = harness
        .stderr_lines()
        .into_iter()
        .find(|l| l["event"] == "identity.dev_link")
        .expect("a dev_link line");
    assert_eq!(line["purpose"], "magic_link");
    assert!(line["link"].as_str().unwrap().ends_with(&token));
    assert!(
        !format!("{:?}", harness.logs()).contains(&token),
        "never exported"
    );
    assert!(!format!("{:?}", harness.spans()).contains(&token));
    tokio::time::sleep(Duration::from_millis(1)).await;
}
