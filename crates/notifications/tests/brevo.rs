//! Brevo email (NT-N3) against real PostgreSQL and `FakeBrevo`: the
//! request shape, the classifier through the worker (retries, rate limits,
//! account errors, exhaustion and redelivery), the crash and connection
//! drop ambiguity paths, the email status callback (bearer, CIDR,
//! correlation, deduplication, suppressions) and a telemetry scan.
mod harness;
use harness::*;
use merezha_notifications::{faults, testkit::FakeBrevo};
use merezha_platform::ports::{
    Address, ChannelSelection, Correlation, DedupeKey, EmailAddress, NotificationRequest, Recipient,
};
use serde_json::{Map, Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const API_KEY: &str = "xkeysib-test-0123456789abcdef";
const TOKEN: &str = "brevo-webhook-token-0123456789abcdef-xyz";
const CALLBACK: &str = "/v1/notifications/providers/brevo/email";
/// A transactional email topic of the project (template of a built-in).
const ASSIGNED: &str = "[modules.notifications.topics.\"tasks.assigned\"]\n\
    category = \"transactional\"\nchannels = [\"email\"]\ntemplate = \"identity.password_changed\"\n";

/// AP raises a retry to the handler's `retry_after` only up to the
/// queue's `backoff_max_ms` (B.6): the block's production cap of an hour.
const PROVIDER_DELAYS: &str = "max_attempts = 10\nbackoff_base_ms = 10\nbackoff_max_ms = 3600000\n";

/// The block on `fake`, with fast retries unless `queue` says otherwise.
fn section(fake: &FakeBrevo, queue: &str, brevo: &str, top: &str) -> String {
    let queue = if queue.is_empty() {
        "max_attempts = 10\nbackoff_base_ms = 10\nbackoff_max_ms = 300000\n"
    } else {
        queue
    };
    format!(
        "[jobs.queues.\"notify.email\"]\n{queue}\
         {SECTION}public_base_url = \"https://api.acme.test\"\n{top}\
         [modules.notifications.email]\ndriver = \"brevo\"\nfrom = \"Acme <no-reply@acme.test>\"\n\
         [modules.notifications.brevo]\napi_key = {{ env = \"BREVO_API_KEY\" }}\n\
         webhook_token = {{ env = \"BREVO_WEBHOOK_TOKEN\" }}\nbase_url = \"{}\"\ntimeout = \"2s\"\n\
         {brevo}{ASSIGNED}",
        fake.base_url()
    )
}
fn options(section: &str) -> Options {
    let mut options = Options::new(section, Arc::new(People::default()));
    options.env = vec![
        ("BREVO_API_KEY".into(), API_KEY.into()),
        ("BREVO_WEBHOOK_TOKEN".into(), TOKEN.into()),
    ];
    options
}

fn request(topic: &str, recipient: Recipient, key: &str) -> NotificationRequest {
    NotificationRequest {
        topic: id(topic),
        recipients: vec![recipient],
        channels: ChannelSelection::TopicDefault,
        template: None,
        data: Map::new(),
        secret_data: Map::new(),
        locale: None,
        dedupe_key: DedupeKey::new(key).unwrap(),
        tenant: None,
        members: Vec::new(),
        not_before: None,
        expires_at: None,
        collapse_key: None,
        correlation: Correlation {
            correlation_id: id("corr1"),
            causation_id: None,
        },
    }
}
fn email(address: &str, name: Option<&str>) -> Recipient {
    Recipient::Address(Address::Email {
        address: EmailAddress::new(address).unwrap(),
        name: name.map(str::to_owned),
        user: None,
        locale: None,
    })
}
async fn commit(instance: &Instance, request: NotificationRequest) {
    let notifier = instance.notifier("identity");
    instance
        .db
        .transaction(soon(), move |tx| {
            Box::pin(async move { Ok(notifier.request(tx, request).await) })
        })
        .await
        .unwrap()
        .unwrap();
}
/// `status:reason` of the delivery to `address`.
async fn delivery(world: &World, address: &str) -> String {
    world
        .text(&format!(
            "SELECT status || ':' || coalesce(status_reason, '') FROM merezha_notify.deliveries \
             WHERE address = '{address}'"
        ))
        .await
        .remove(0)
}
async fn attempts(world: &World) -> Vec<String> {
    world
        .text("SELECT outcome FROM merezha_notify.delivery_attempts ORDER BY attempt")
        .await
}
async fn wait_status(instance: &Instance, status: &str, count: i64) {
    instance
        .until(
            &format!("SELECT count(*) FROM merezha_notify.deliveries WHERE status = '{status}'"),
            count,
        )
        .await;
}
fn sends(fake: &FakeBrevo) -> Vec<merezha_notifications::testkit::Recorded> {
    fake.requests_to("/v3/smtp/email")
}
async fn callback(instance: &Instance, payload: &Value) -> (u16, Value) {
    let (status, body) = instance
        .raw(FakeBrevo::event_request(CALLBACK, TOKEN, payload))
        .await;
    (status.as_u16(), body)
}

/// NT-N3 acceptance: requests carry `api-key`, one recipient,
/// `idempotencyKey` and `X-Mailin-custom`; a transactional topic gets the
/// RFC 8058 pair and a security topic none; names are cut to 70
/// characters; sandbox mode adds `X-Sib-Sandbox: drop`.
#[tokio::test]
async fn requests_carry_the_key_one_recipient_and_the_correlation_headers() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    let instance =
        Instance::start(&world, options(&section(&fake, "", "sandbox = true\n", ""))).await;
    let long = "N".repeat(90);
    commit(
        &instance,
        request(
            "tasks.assigned",
            email("ada@example.com", Some(&long)),
            "a:1",
        ),
    )
    .await;
    commit(
        &instance,
        request(
            "identity.password_changed",
            email("bob@example.com", None),
            "identity.password_changed:a2",
        ),
    )
    .await;
    wait_status(&instance, "accepted", 2).await;
    let sent = sends(&fake);
    assert_eq!(sent.len(), 2);
    for request in &sent {
        assert_eq!(request.headers["api-key"], API_KEY);
        assert_eq!(request.headers["content-type"], "application/json");
        assert_eq!(request.headers["accept"], "application/json");
        assert!(
            !request.headers.contains_key("traceparent"),
            "Brevo is a third party"
        );
        let body = &request.body;
        assert_eq!(body["to"].as_array().unwrap().len(), 1);
        assert_eq!(
            body["sender"],
            json!({"email": "no-reply@acme.test", "name": "Acme"})
        );
        assert_eq!(body["headers"]["X-Sib-Sandbox"], "drop");
        let to = body["to"][0]["email"].as_str().unwrap();
        let id = world
            .text(&format!(
                "SELECT id::text FROM merezha_notify.deliveries WHERE address = '{to}'"
            ))
            .await
            .remove(0);
        let key =
            merezha_notifications::ids::idempotency_key(uuid::Uuid::parse_str(&id).unwrap(), 0);
        assert_eq!(body["headers"]["idempotencyKey"], key.to_string());
        assert_eq!(body["headers"]["X-Mailin-custom"], format!("mzd:{id}"));
        let security = to == "bob@example.com";
        assert_eq!(
            body["headers"].get("List-Unsubscribe").is_some(),
            !security,
            "{to}"
        );
        if security {
            assert!(body["headers"].get("List-Unsubscribe-Post").is_none());
            assert_eq!(body["tags"], json!(["identity.password_changed"]));
        } else {
            assert_eq!(
                body["headers"]["List-Unsubscribe-Post"],
                "List-Unsubscribe=One-Click"
            );
            assert!(
                body["headers"]["List-Unsubscribe"]
                    .as_str()
                    .unwrap()
                    .starts_with("<https://api.acme.test/v1/notifications/unsubscribe?token=v1.")
            );
            assert_eq!(body["to"][0]["name"], "N".repeat(70));
            assert!(body["htmlContent"].as_str().unwrap().contains("<p>"));
        }
    }
    // The 201's messageId is the delivery's provider message id.
    let ids = world
        .text(
            "SELECT provider || ' ' || provider_message_id FROM merezha_notify.deliveries \
             ORDER BY provider_message_id",
        )
        .await;
    let mut want: Vec<String> = fake
        .processed()
        .iter()
        .map(|p| format!("brevo {}", p.message_id))
        .collect();
    want.sort();
    assert_eq!(ids, want);
    instance.stop().await;
}

/// NT-N3 acceptance: three 503s then a 201 give `accepted` after four
/// attempts, with one message processed at the fake and one key.
#[tokio::test]
async fn three_unavailable_answers_then_created_give_accepted_after_four_attempts() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    fake.fail_next(3, 503);
    let instance = Instance::start(&world, options(&section(&fake, "", "", ""))).await;
    commit(
        &instance,
        request("tasks.assigned", email("carol@example.org", None), "b:1"),
    )
    .await;
    wait_status(&instance, "accepted", 1).await;
    assert_eq!(
        attempts(&world).await,
        ["transient", "transient", "transient", "accepted"]
    );
    let sent = sends(&fake);
    assert_eq!(sent.len(), 4);
    let keys: std::collections::BTreeSet<_> = sent
        .iter()
        .map(|r| r.body["headers"]["idempotencyKey"].to_string())
        .collect();
    assert_eq!(keys.len(), 1, "every attempt reuses the key");
    assert_eq!(fake.processed().len(), 1);
    assert_eq!(
        world
            .text("SELECT error_code FROM merezha_notify.delivery_attempts WHERE attempt = 1")
            .await,
        ["provider_unavailable"]
    );
    instance.stop().await;
}

/// NT-N3 acceptance: a 401 keeps the delivery retrying as
/// `provider_account`, at least five minutes later, and logs an error and
/// counts it.
#[tokio::test]
async fn an_unauthorized_answer_keeps_retrying_as_provider_account_and_logs_an_error() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    fake.fail_next(1, 401);
    let mut options = options(&section(&fake, PROVIDER_DELAYS, "", ""));
    options.meters = Some(harness.telemetry().meters());
    let instance = Instance::start(&world, options).await;
    commit(
        &instance,
        request("tasks.assigned", email("dan@example.org", None), "c:1"),
    )
    .await;
    wait_status(&instance, "retrying", 1).await;
    assert_eq!(
        delivery(&world, "dan@example.org").await,
        "retrying:provider_account"
    );
    instance
        .until(
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.deliver.email' \
             AND state = 'available' AND run_at > now() + interval '4 minutes'",
            1,
        )
        .await;
    assert_eq!(fake.processed().len(), 0);
    let logged = harness.recorded().into_iter().any(|r| {
        r.name.as_deref() == Some("notification.provider_account_error")
            && r.level == tracing::Level::ERROR
    });
    assert!(logged, "the account error is logged at error level");
    assert_eq!(
        harness
            .metric("merezha.notification.provider_account_errors")
            .value(&[("notify.provider", "brevo"), ("notify.channel", "email")]),
        Some(1.0)
    );
    harness.assert_catalogued();
    instance.stop().await;
}

/// NT-N3 test **provider outage**: 503 until exhaustion gives `failed` /
/// `retries_exhausted` and a dead letter; a redelivery after recovery
/// sends, under a new generation and key.
#[tokio::test]
async fn a_provider_outage_exhausts_into_a_dead_letter_and_redelivery_sends_after_recovery() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    fake.fail_next(3, 503);
    let instance = Instance::start(
        &world,
        options(&section(
            &fake,
            "max_attempts = 3\nbackoff_base_ms = 10\nbackoff_max_ms = 300000\n",
            "",
            "",
        )),
    )
    .await;
    commit(
        &instance,
        request("tasks.assigned", email("erin@example.org", None), "d:1"),
    )
    .await;
    wait_status(&instance, "failed", 1).await;
    assert_eq!(
        delivery(&world, "erin@example.org").await,
        "failed:retries_exhausted"
    );
    instance
        .until(
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.deliver.email' \
             AND state = 'dead'",
            1,
        )
        .await;
    assert_eq!((sends(&fake).len(), fake.processed().len()), (3, 0));
    // Brevo recovered: an operator redelivers.
    instance
        .tokens
        .add("admin", user("usr_admin", &["notifications.admin"], None));
    let id = world
        .text("SELECT id::text FROM merezha_notify.deliveries")
        .await
        .remove(0);
    let (status, body) = instance
        .call(
            "POST",
            &format!("/v1/notifications/deliveries/{id}/redeliver"),
            Some("admin"),
            None,
        )
        .await;
    assert_eq!(
        (status.as_u16(), body["generation"].clone()),
        (202, json!(1))
    );
    wait_status(&instance, "accepted", 1).await;
    assert_eq!(fake.processed().len(), 1);
    let key = merezha_notifications::ids::idempotency_key(uuid::Uuid::parse_str(&id).unwrap(), 1);
    assert_eq!(
        fake.processed()[0].body["headers"]["idempotencyKey"],
        key.to_string()
    );
    instance.stop().await;
}

/// The callback answers 401 to a wrong, missing or session-shaped bearer
/// and to a source outside the CIDRs, and stores nothing; the right bearer
/// passes the edge, which never runs the session authenticator on it.
#[tokio::test]
async fn the_callback_refuses_a_wrong_bearer_or_source_and_stores_nothing() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    let instance = Instance::start(
        &world,
        options(&section(
            &fake,
            "",
            "webhook_allowed_cidrs = [\"1.179.112.0/20\"]\n",
            "",
        )),
    )
    .await;
    // A principal token the harness authenticator knows: never a Brevo token.
    instance.tokens.add(
        "session-token",
        user("usr_admin", &["notifications.admin"], None),
    );
    let event = json!({"event": "delivered", "message-id": "<x@relay>", "ts_event": 1});
    let from = |ip: &str, token: Option<&str>| {
        let mut request = FakeBrevo::event_request(CALLBACK, token.unwrap_or(""), &event);
        if token.is_none() {
            request.headers_mut().remove("authorization");
        }
        let address: std::net::SocketAddr = format!("{ip}:443").parse().unwrap();
        request
            .extensions_mut()
            .insert(axum::extract::ConnectInfo(address));
        request
    };
    let brevo_ip = "1.179.113.7";
    for (request, why) in [
        (
            from(brevo_ip, Some("wrong-token-0123456789abcdef0123456789")),
            "wrong",
        ),
        (from(brevo_ip, Some("session-token")), "session"),
        (from(brevo_ip, None), "missing"),
        (from("203.0.113.9", Some(TOKEN)), "source"),
        // No client address at all: refused while CIDRs are configured.
        (
            FakeBrevo::event_request(CALLBACK, TOKEN, &event),
            "no address",
        ),
    ] {
        let (status, body) = instance.raw(request).await;
        assert_eq!(
            (status.as_u16(), body["error"]["code"].clone()),
            (401, json!("unauthenticated")),
            "{why}"
        );
    }
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.provider_events")
            .await,
        0
    );
    let rejected = harness
        .recorded()
        .into_iter()
        .filter(|r| r.name.as_deref() == Some("notification.provider_event_rejected"))
        .count();
    assert_eq!(rejected, 5);
    let (status, _) = instance.raw(from(brevo_ip, Some(TOKEN))).await;
    assert_eq!(status.as_u16(), 200);
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.provider_events")
            .await,
        1
    );
    harness.assert_no_canary(TOKEN);
    harness.assert_catalogued();
    instance.stop().await;
}

/// NT-N3 acceptance: `hard_bounce` gives `bounced` plus a suppression, the
/// next non-security email to that address is `skipped`, and security mail
/// still goes out. Also: a duplicate event is applied once, `delivered`
/// and a later `spam` complaint move the status, opens are dropped, and an
/// uncorrelated event gets 200 and is stored.
#[tokio::test]
async fn events_move_deliveries_write_suppressions_and_are_applied_once() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    let mut options = options(&section(&fake, "", "", ""));
    options.meters = Some(harness.telemetry().meters());
    let instance = Instance::start(&world, options).await;
    for (address, key) in [("frank@example.org", "e:1"), ("gina@example.org", "e:2")] {
        commit(
            &instance,
            request("tasks.assigned", email(address, None), key),
        )
        .await;
    }
    wait_status(&instance, "accepted", 2).await;
    let processed = fake.processed();
    let by_to = |to: &str| {
        processed
            .iter()
            .find(|p| p.body["to"][0]["email"] == to)
            .unwrap()
            .clone()
    };
    let (frank, gina) = (by_to("frank@example.org"), by_to("gina@example.org"));

    // A hard bounce, sent twice in one batch and again later.
    let bounce = fake.event("hard_bounce", &frank);
    assert_eq!(
        callback(&instance, &json!([bounce, bounce])).await,
        (200, json!({}))
    );
    assert_eq!(callback(&instance, &bounce).await.0, 200);
    assert_eq!(
        delivery(&world, "frank@example.org").await,
        "bounced:hard_bounce"
    );
    assert_eq!(
        world
            .text(
                "SELECT reason || ':' || coalesce(topic, '*') || ':' || address \
                 FROM merezha_notify.suppressions"
            )
            .await,
        ["hard_bounce:*:frank@example.org"]
    );
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.provider_events WHERE event = 'hard_bounce'"
            )
            .await,
        1,
        "a duplicate event is applied once"
    );
    let applied = |event: &str, correlated: &str| {
        harness
            .metric("merezha.notification.provider_events")
            .value(&[
                ("notify.provider", "brevo"),
                ("notify.event", event),
                ("notify.correlated", correlated),
            ])
    };
    assert_eq!(applied("hard_bounce", "true"), Some(1.0));

    // Delivered, then a complaint after reading; opens are dropped.
    let delivered = fake.event("delivered", &gina);
    let opened = fake.event("opened", &gina);
    assert_eq!(
        callback(&instance, &json!([delivered, opened])).await.0,
        200
    );
    assert_eq!(delivery(&world, "gina@example.org").await, "delivered:");
    assert_eq!(callback(&instance, &fake.event("spam", &gina)).await.0, 200);
    assert_eq!(
        delivery(&world, "gina@example.org").await,
        "complained:spam"
    );
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.suppressions \
                 WHERE reason = 'spam_complaint' AND address = 'gina@example.org'"
            )
            .await,
        1
    );
    let gina_id = world
        .text("SELECT id::text FROM merezha_notify.deliveries WHERE address = 'gina@example.org'")
        .await
        .remove(0);
    assert_eq!(
        world
            .text(&format!(
                "SELECT event FROM merezha_notify.provider_events \
                 WHERE delivery_id = '{gina_id}' ORDER BY id"
            ))
            .await,
        ["delivered", "spam"],
        "engagement is not stored"
    );

    // An event about a message the block never sent: 200, stored, NULL.
    let foreign = json!({"event": "delivered", "email": "zed@example.org",
        "message-id": "<foreign@relay>", "ts_event": 1_700_000_999});
    assert_eq!(callback(&instance, &foreign).await, (200, json!({})));
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.provider_events \
                 WHERE delivery_id IS NULL AND provider_message_id = '<foreign@relay>'"
            )
            .await,
        1
    );
    assert_eq!(applied("delivered", "false"), Some(1.0));
    // Correlation by message id alone, when the custom header is missing.
    let mut by_message = fake.event("delivered", &frank);
    by_message
        .as_object_mut()
        .unwrap()
        .remove("X-Mailin-custom");
    assert_eq!(callback(&instance, &by_message).await.0, 200);
    let frank_id = world
        .text("SELECT id::text FROM merezha_notify.deliveries WHERE address = 'frank@example.org'")
        .await
        .remove(0);
    assert_eq!(
        world
            .scalar(&format!(
                "SELECT count(*) FROM merezha_notify.provider_events \
                 WHERE event = 'delivered' AND delivery_id = '{frank_id}'"
            ))
            .await,
        1
    );
    assert_eq!(
        delivery(&world, "frank@example.org").await,
        "bounced:hard_bounce",
        "a bounced delivery stays bounced"
    );

    // The next non-security email to the bounced address is skipped;
    // security mail still goes out.
    commit(
        &instance,
        request("tasks.assigned", email("frank@example.org", None), "e:3"),
    )
    .await;
    commit(
        &instance,
        request(
            "identity.password_changed",
            email("frank@example.org", None),
            "identity.password_changed:e4",
        ),
    )
    .await;
    instance.settle().await;
    assert_eq!(
        world
            .text(
                "SELECT n.topic || ':' || d.status || ':' || coalesce(d.status_reason, '') \
                 FROM merezha_notify.deliveries d JOIN merezha_notify.notifications n \
                 ON n.id = d.notification_id WHERE d.address = 'frank@example.org' \
                 AND n.dedupe_key IN ('e:3', 'identity.password_changed:e4') ORDER BY 1"
            )
            .await,
        [
            "identity.password_changed:accepted:",
            "tasks.assigned:skipped:suppressed"
        ]
    );
    assert_eq!(fake.processed().len(), 3);
    instance.stop().await;
}

/// An email `unsubscribed` event writes an `unsubscribed` suppression for
/// the delivery's topic only.
#[tokio::test]
async fn an_unsubscribed_event_suppresses_the_topic_of_its_delivery() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    let instance = Instance::start(&world, options(&section(&fake, "", "", ""))).await;
    commit(
        &instance,
        request("tasks.assigned", email("hana@example.org", None), "u:1"),
    )
    .await;
    wait_status(&instance, "accepted", 1).await;
    let message = fake.processed().remove(0);
    assert_eq!(
        callback(&instance, &fake.event("unsubscribed", &message))
            .await
            .0,
        200
    );
    assert_eq!(
        world
            .text("SELECT reason || ':' || coalesce(topic, '*') FROM merezha_notify.suppressions")
            .await,
        ["unsubscribed:tasks.assigned"]
    );
    assert_eq!(delivery(&world, "hana@example.org").await, "accepted:");
    instance.stop().await;
}

/// NT-N3 acceptance: a scan of the captured logs and spans of a send with
/// a retry, a rate limit and a bounce finds no address, subject or body,
/// and no API key.
#[tokio::test]
async fn logs_and_spans_hold_no_address_subject_or_body() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    fake.fail_next(1, 503);
    fake.rate_limit(&[("retry-after", "0")]);
    let address = "kanary9q@example.org";
    let name = "Zorblaxname";
    // `app.name` appears in the subject and the body of the template.
    let app = "Quxsubjectbody";
    let mut options = options(&section(&fake, "", "", ""));
    options.section = options.section.replace(
        "[modules.notifications.email]",
        &format!(
            "[modules.notifications.templates]\napp_name = \"{app}\"\n[modules.notifications.email]"
        ),
    );
    options.meters = Some(harness.telemetry().meters());
    let instance = Instance::start(&world, options).await;
    commit(
        &instance,
        request("tasks.assigned", email(address, Some(name)), "t:1"),
    )
    .await;
    wait_status(&instance, "accepted", 1).await;
    let message = fake.processed().remove(0);
    assert!(message.body["subject"].as_str().unwrap().contains(app));
    assert!(message.body["textContent"].as_str().unwrap().contains(name));
    assert_eq!(
        callback(&instance, &fake.event("hard_bounce", &message))
            .await
            .0,
        200
    );
    assert_eq!(delivery(&world, address).await, "bounced:hard_bounce");
    let names: Vec<String> = harness
        .recorded()
        .into_iter()
        .filter_map(|r| r.name)
        .collect();
    for wanted in [
        "notification.retry_scheduled",
        "notification.provider_rate_limited",
        "notification.accepted",
        "notification.provider_event_applied",
    ] {
        assert!(
            names.iter().any(|n| n == wanted),
            "{wanted} not in {names:?}"
        );
    }
    let outbound = harness.spans_named("POST /v3/smtp/email");
    assert_eq!(outbound.len(), 3, "one client span per provider call");
    assert_eq!(
        merezha_telemetry::testing::span_attribute(&outbound[0], "merezha.provider").as_deref(),
        Some("brevo")
    );
    for canary in [address, "kanary9q", name, app, API_KEY, &message.message_id] {
        harness.assert_no_canary(canary);
    }
    harness.assert_catalogued();
    instance.stop().await;
}

/// The id of the delivery to `address`.
async fn delivery_id(world: &World, address: &str) -> String {
    world
        .text(&format!(
            "SELECT id::text FROM merezha_notify.deliveries WHERE address = '{address}'"
        ))
        .await
        .remove(0)
}
/// Make every waiting delivery job due now, until `sql` answers `want`:
/// the provider-asked delays are minutes long.
async fn hurry_until(world: &World, sql: &str, want: i64) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        world
            .execute(
                "UPDATE merezha_jobs.jobs SET run_at = now() \
                 WHERE state = 'available' AND run_at > now()",
            )
            .await;
        let got = world.scalar(sql).await;
        if got == want {
            return;
        }
        assert!(Instant::now() < deadline, "{sql} stayed {got}, not {want}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Review M1: an event no delivery of ours correlates has no effect, so a
/// forged `invalid_email` naming any address cannot block its mail, not
/// even security mail; it is still stored and answered 200.
#[tokio::test]
async fn an_uncorrelated_event_suppresses_nothing() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    let instance = Instance::start(&world, options(&section(&fake, "", "", ""))).await;
    let forged = [
        json!({"event": "invalid_email", "email": "victim@example.org", "ts_event": 1}),
        json!({"event": "spam", "email": "victim@example.org", "ts_event": 2,
            "X-Mailin-custom": format!("mzd:{}", uuid::Uuid::from_u128(42))}),
        json!({"event": "hard_bounce", "email": "victim@example.org", "ts_event": 3,
            "message-id": "<never-sent@relay>"}),
    ];
    assert_eq!(callback(&instance, &json!(forged)).await, (200, json!({})));
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.provider_events WHERE delivery_id IS NULL")
            .await,
        3
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.suppressions")
            .await,
        0
    );
    commit(
        &instance,
        request(
            "identity.password_changed",
            email("victim@example.org", None),
            "identity.password_changed:m1",
        ),
    )
    .await;
    commit(
        &instance,
        request("tasks.assigned", email("victim@example.org", None), "m:2"),
    )
    .await;
    wait_status(&instance, "accepted", 2).await;
    instance.stop().await;
}

/// Review M2: after an attempt Brevo may have processed, a resend must
/// start inside the margin of the idempotency window (25 of Brevo's 30
/// minutes); a `Retry-After: 3600` would fall later, so the delivery is
/// `unknown` and never resent, as an ambiguous SMS.
#[tokio::test]
async fn a_resend_past_the_idempotency_window_is_never_made() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    fake.close_after_accept();
    // The resend after the dropped answer waits at least a second.
    let queue = "max_attempts = 10\nbackoff_base_ms = 2000\nbackoff_max_ms = 3600000\n";
    let instance = Instance::start(&world, options(&section(&fake, queue, "", ""))).await;
    commit(
        &instance,
        request("tasks.assigned", email("lena@example.org", None), "w:1"),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while fake.processed().is_empty() {
        assert!(Instant::now() < deadline, "never sent");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    fake.rate_limit(&[("Retry-After", "3600")]);
    wait_status(&instance, "unknown", 1).await;
    assert_eq!(
        delivery(&world, "lena@example.org").await,
        "unknown:idempotency_window"
    );
    assert_eq!(attempts(&world).await, ["ambiguous", "rate_limited"]);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!((sends(&fake).len(), fake.processed().len()), (2, 1));
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.deliver.email' \
                 AND state NOT IN ('succeeded')"
            )
            .await,
        0,
        "nothing left to resend"
    );
    // A provider event can still settle it.
    let message = fake.processed().remove(0);
    assert_eq!(
        callback(&instance, &fake.event("delivered", &message))
            .await
            .0,
        200
    );
    assert_eq!(delivery(&world, "lena@example.org").await, "delivered:");
    instance.stop().await;
}

/// NT-N4 review: `X-Mailin-custom` names the delivery, which a redelivery
/// keeps. A late event about the first generation's message (`unknown`
/// past the idempotency window, then redelivered and accepted with another
/// message id) moves no status: a `blocked` for the first email must not
/// fail the second. An event about the current message still applies.
#[tokio::test]
async fn a_late_event_about_an_earlier_generation_moves_no_status() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    fake.close_after_accept();
    let queue = "max_attempts = 10\nbackoff_base_ms = 2000\nbackoff_max_ms = 3600000\n";
    let instance = Instance::start(&world, options(&section(&fake, queue, "", ""))).await;
    commit(
        &instance,
        request("tasks.assigned", email("gen@example.org", None), "e:1"),
    )
    .await;
    let deadline = Instant::now() + Duration::from_secs(10);
    while fake.processed().is_empty() {
        assert!(Instant::now() < deadline, "never sent");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    fake.rate_limit(&[("Retry-After", "3600")]);
    wait_status(&instance, "unknown", 1).await;
    let first = fake.processed().remove(0);
    instance
        .tokens
        .add("admin", user("usr_admin", &["notifications.admin"], None));
    let id = world
        .text("SELECT id::text FROM merezha_notify.deliveries")
        .await
        .remove(0);
    let (status, _) = instance
        .call(
            "POST",
            &format!("/v1/notifications/deliveries/{id}/redeliver"),
            Some("admin"),
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 202);
    wait_status(&instance, "accepted", 1).await;
    let second = fake.processed().remove(1);
    assert_ne!(first.message_id, second.message_id);
    assert_eq!(
        first.body["headers"]["X-Mailin-custom"],
        second.body["headers"]["X-Mailin-custom"]
    );
    assert_eq!(
        callback(&instance, &fake.event("blocked", &first)).await,
        (200, json!({}))
    );
    assert_eq!(delivery(&world, "gen@example.org").await, "accepted:");
    assert_eq!(
        world
            .scalar(&format!(
                "SELECT count(*) FROM merezha_notify.provider_events \
                 WHERE event = 'blocked' AND delivery_id = '{id}'"
            ))
            .await,
        1,
        "stored, correlated, no effect"
    );
    assert_eq!(
        callback(&instance, &fake.event("delivered", &second))
            .await
            .0,
        200
    );
    assert_eq!(delivery(&world, "gen@example.org").await, "delivered:");
    instance.stop().await;
}

/// Review M3: `provider_account` answers consume no attempt. A 70-minute
/// run of 401s (twelve, five minutes apart on the block's clock) on a
/// queue of two attempts keeps the security mail retrying, logs one error
/// for the run and counts every refusal; once the key is fixed it is sent.
#[tokio::test]
async fn a_long_provider_account_streak_consumes_no_attempts_and_delivers_once_fixed() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    fake.fail_next(12, 401);
    let clock = Arc::new(Shifted::default());
    let mut options = options(&section(
        &fake,
        "max_attempts = 2\nbackoff_base_ms = 10\nbackoff_max_ms = 3600000\n",
        "",
        "",
    ));
    options.meters = Some(harness.telemetry().meters());
    options.clock = Some(clock.clone());
    let instance = Instance::start(&world, options).await;
    commit(
        &instance,
        request(
            "identity.password_changed",
            email("mo@example.org", None),
            "identity.password_changed:s1",
        ),
    )
    .await;
    for done in 1..=12 {
        instance
            .until(
                "SELECT count(*) FROM merezha_notify.delivery_attempts WHERE outcome <> 'started'",
                done,
            )
            .await;
        assert_eq!(
            delivery(&world, "mo@example.org").await,
            "retrying:provider_account",
            "refusal {done}"
        );
        clock.advance(Duration::from_secs(6 * 60));
        hurry_until(
            &world,
            "SELECT count(*) FROM merezha_notify.delivery_attempts",
            done + 1,
        )
        .await;
    }
    wait_status(&instance, "accepted", 1).await;
    let outcomes = attempts(&world).await;
    assert_eq!(outcomes.len(), 13);
    assert!(
        outcomes[..12].iter().all(|o| o == "transient"),
        "{outcomes:?}"
    );
    assert_eq!(outcomes[12], "accepted");
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.deliver.email' \
                 AND state = 'dead'"
            )
            .await,
        0,
        "no attempt budget ran out"
    );
    let errors = harness
        .recorded()
        .into_iter()
        .filter(|r| r.name.as_deref() == Some("notification.provider_account_error"))
        .count();
    assert_eq!(errors, 1, "one error per run of refusals");
    assert_eq!(
        harness
            .metric("merezha.notification.provider_account_errors")
            .value(&[("notify.provider", "brevo")]),
        Some(12.0)
    );
    harness.assert_catalogued();
    instance.stop().await;
}

/// Review M3: a run of `provider_account` answers ends at the
/// notification's `expires_at` as `failed` / `provider_account_unresolved`.
#[tokio::test]
async fn a_provider_account_streak_fails_unresolved_at_the_deadline() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    fake.fail_next(5, 401);
    let clock = Arc::new(Shifted::default());
    let mut options = options(&section(&fake, PROVIDER_DELAYS, "", ""));
    options.clock = Some(clock.clone());
    let instance = Instance::start(&world, options).await;
    let mut expiring = request("tasks.assigned", email("nia@example.org", None), "x:1");
    expiring.expires_at = Some(jiff::Timestamp::now() + jiff::SignedDuration::from_mins(10));
    commit(&instance, expiring).await;
    wait_status(&instance, "retrying", 1).await;
    clock.advance(Duration::from_secs(6 * 60));
    hurry_until(
        &world,
        "SELECT count(*) FROM merezha_notify.deliveries WHERE status = 'failed'",
        1,
    )
    .await;
    assert_eq!(
        delivery(&world, "nia@example.org").await,
        "failed:provider_account_unresolved"
    );
    instance
        .until(
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.deliver.email' \
             AND state = 'dead'",
            1,
        )
        .await;
    assert_eq!(fake.processed().len(), 0);
    instance.stop().await;
}

/// Review L5: without Brevo's message id, the delivery and the address
/// keep two bounces of the same time apart: both apply.
#[tokio::test]
async fn bounces_without_a_message_id_are_told_apart() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    let instance = Instance::start(&world, options(&section(&fake, "", "", ""))).await;
    for (address, key) in [("oli@example.org", "d:1"), ("pia@example.org", "d:2")] {
        commit(
            &instance,
            request("tasks.assigned", email(address, None), key),
        )
        .await;
    }
    wait_status(&instance, "accepted", 2).await;
    let bounces: Vec<Value> = fake
        .processed()
        .iter()
        .map(|message| {
            let mut event = fake.event("hard_bounce", message);
            let object = event.as_object_mut().unwrap();
            object.remove("message-id");
            object.insert("ts_event".into(), json!(1_700_000_500));
            object.insert("ts_epoch".into(), json!(1_700_000_500_000_i64));
            event
        })
        .collect();
    assert_eq!(callback(&instance, &json!(bounces)).await.0, 200);
    assert_eq!(
        world
            .text(
                "SELECT address FROM merezha_notify.suppressions \
                 WHERE reason = 'hard_bounce' ORDER BY address"
            )
            .await,
        ["oli@example.org", "pia@example.org"]
    );
    for address in ["oli@example.org", "pia@example.org"] {
        assert_eq!(delivery(&world, address).await, "bounced:hard_bounce");
    }
    instance.stop().await;
}

/// Review M5: a callback of more than `MAX_EVENTS` events is refused with
/// 413 and stores nothing; `MAX_EVENTS` events are taken.
#[tokio::test]
async fn a_callback_over_the_event_limit_is_refused_whole() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    let instance = Instance::start(&world, options(&section(&fake, "", "", ""))).await;
    let batch = |n: usize| {
        Value::Array(
            (0..n)
                .map(|i| json!({"event": "deferred", "message-id": format!("<m{i}@relay>"), "ts_event": i}))
                .collect(),
        )
    };
    let max = merezha_notifications::brevo::events::MAX_EVENTS;
    let (status, body) = callback(&instance, &batch(max + 1)).await;
    assert_eq!(
        (status, body["error"]["code"].clone()),
        (413, json!("body_limit"))
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.provider_events")
            .await,
        0
    );
    assert_eq!(callback(&instance, &batch(max)).await.0, 200);
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.provider_events")
            .await,
        max as i64
    );
    instance.stop().await;
}

/// Crashes and rate-limit delays race lease and retry deadlines (issue
/// #37): CI runs them alone (`cargo test -- --test-threads=1 timing::`).
mod timing {
    use super::*;

    /// Leases (three Brevo timeouts) expire after 6 s and a crashed run is
    /// aborted after 3 s.
    const CRASHING: &str = "concurrency = 2\nlease_ms = 6000\ntimeout_ms = 3000\n\
        backoff_base_ms = 10\nbackoff_max_ms = 300000\n";

    async fn crashed(point: Option<&'static str>, close: bool) -> (World, FakeBrevo, Instance) {
        let world = World::new().await.expect("database");
        let fake = FakeBrevo::start(API_KEY).await.unwrap();
        if close {
            fake.close_after_accept();
        }
        let mut options = options(&section(&fake, CRASHING, "", ""));
        options.heartbeat = false;
        if let Some(point) = point {
            options.faults.crash_once(point);
        }
        let instance = Instance::start(&world, options).await;
        commit(
            &instance,
            request("tasks.assigned", email("ivy@example.org", None), "k:1"),
        )
        .await;
        wait_status(&instance, "accepted", 1).await;
        (world, fake, instance)
    }

    /// NT-N3 test **crash between send and record** (`after_provider_call`):
    /// the rerun gets `duplicate_parameter`, the delivery is `accepted`
    /// without a message id, and the fake shows two requests and one
    /// processed message.
    #[tokio::test]
    async fn a_crash_after_the_provider_call_resends_and_gets_duplicate() {
        if World::new().await.is_none() {
            return;
        }
        let (world, fake, instance) = crashed(Some(faults::AFTER_PROVIDER_CALL), false).await;
        assert_eq!(attempts(&world).await, ["ambiguous", "duplicate"]);
        assert_eq!((sends(&fake).len(), fake.processed().len()), (2, 1));
        assert_eq!(
            sends(&fake)[0].body["headers"]["idempotencyKey"],
            sends(&fake)[1].body["headers"]["idempotencyKey"]
        );
        assert_eq!(
            world
                .text("SELECT coalesce(provider_message_id, 'none') FROM merezha_notify.deliveries")
                .await,
            ["none"]
        );
        // Events still correlate through X-Mailin-custom.
        let message = fake.processed().remove(0);
        assert_eq!(
            callback(&instance, &fake.event("delivered", &message))
                .await
                .0,
            200
        );
        assert_eq!(delivery(&world, "ivy@example.org").await, "delivered:");
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(fake.processed().len(), 1);
        instance.stop().await;
    }

    /// `close_after_accept`: Brevo processed the message and dropped the
    /// connection; the attempt is ambiguous and the resend is a duplicate.
    #[tokio::test]
    async fn a_dropped_connection_after_acceptance_resends_and_gets_duplicate() {
        if World::new().await.is_none() {
            return;
        }
        let (world, fake, instance) = crashed(None, true).await;
        assert_eq!(attempts(&world).await, ["ambiguous", "duplicate"]);
        assert_eq!((sends(&fake).len(), fake.processed().len()), (2, 1));
        instance.stop().await;
    }

    /// `after_tx_b`: the outcome was recorded before the crash, so the
    /// rerun finds the delivery settled and sends nothing.
    #[tokio::test]
    async fn a_crash_after_recording_sends_nothing_more() {
        if World::new().await.is_none() {
            return;
        }
        let (world, fake, instance) = crashed(Some(faults::AFTER_TX_B), false).await;
        // Past the crashed run's deadline and the lease.
        tokio::time::sleep(Duration::from_secs(7)).await;
        assert_eq!(attempts(&world).await, ["accepted"]);
        assert_eq!((sends(&fake).len(), fake.processed().len()), (1, 1));
        instance.stop().await;
    }

    /// NT-N3 acceptance: `Retry-After: 7` delays the next attempt by at
    /// least 7 s.
    #[tokio::test]
    async fn retry_after_delays_the_next_attempt() {
        let Some(world) = World::new().await else {
            return;
        };
        let fake = FakeBrevo::start(API_KEY).await.unwrap();
        fake.rate_limit(&[("Retry-After", "7")]);
        let instance =
            Instance::start(&world, options(&section(&fake, PROVIDER_DELAYS, "", ""))).await;
        commit(
            &instance,
            request("tasks.assigned", email("jo@example.org", None), "r:1"),
        )
        .await;
        let started = Instant::now();
        instance
            .until(
                "SELECT count(*) FROM merezha_notify.deliveries WHERE status = 'accepted'",
                1,
            )
            .await;
        let sent = sends(&fake);
        assert_eq!(sent.len(), 2);
        let gap = sent[1].at.duration_since(sent[0].at);
        assert!(gap >= Duration::from_secs(7), "{gap:?}");
        assert!(started.elapsed() < Duration::from_secs(25));
        assert_eq!(attempts(&world).await, ["rate_limited", "accepted"]);
        instance.stop().await;
    }

    /// Review M2 on the rerun path: a crash after the provider call, and
    /// the block's clock 26 minutes later when the lease expires: the
    /// rerun does not resend, the delivery is `unknown`.
    #[tokio::test]
    async fn a_rerun_past_the_idempotency_window_does_not_resend() {
        let Some(world) = World::new().await else {
            return;
        };
        let fake = FakeBrevo::start(API_KEY).await.unwrap();
        let clock = Arc::new(Shifted::default());
        let mut options = options(&section(&fake, CRASHING, "", ""));
        options.heartbeat = false;
        options.clock = Some(clock.clone());
        options.faults.crash_once(faults::AFTER_PROVIDER_CALL);
        let instance = Instance::start(&world, options).await;
        commit(
            &instance,
            request("tasks.assigned", email("quin@example.org", None), "q:1"),
        )
        .await;
        let deadline = Instant::now() + Duration::from_secs(10);
        while fake.processed().is_empty() {
            assert!(Instant::now() < deadline, "never sent");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        clock.advance(Duration::from_secs(26 * 60));
        wait_status(&instance, "unknown", 1).await;
        assert_eq!(
            delivery(&world, "quin@example.org").await,
            "unknown:idempotency_window"
        );
        assert_eq!(attempts(&world).await, ["ambiguous"]);
        assert_eq!((sends(&fake).len(), fake.processed().len()), (1, 1));
        instance.stop().await;
    }

    /// Review L4: an event about a delivery its worker still owns is kept
    /// pending, not consumed, and Tx B applies it once the outcome is
    /// recorded.
    #[tokio::test]
    async fn an_event_during_the_send_is_applied_after_the_outcome() {
        let Some(world) = World::new().await else {
            return;
        };
        let fake = FakeBrevo::start(API_KEY).await.unwrap();
        let options = options(&section(&fake, "", "", ""));
        options
            .faults
            .pause_once(faults::AFTER_PROVIDER_CALL, Duration::from_secs(3));
        let instance = Instance::start(&world, options).await;
        commit(
            &instance,
            request("tasks.assigned", email("rae@example.org", None), "p:1"),
        )
        .await;
        let deadline = Instant::now() + Duration::from_secs(10);
        while fake.processed().is_empty() {
            assert!(Instant::now() < deadline, "never sent");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let message = fake.processed().remove(0);
        assert_eq!(
            callback(&instance, &fake.event("delivered", &message))
                .await
                .0,
            200
        );
        assert_eq!(delivery(&world, "rae@example.org").await, "sending:");
        let id = delivery_id(&world, "rae@example.org").await;
        assert_eq!(
            world
                .scalar(&format!(
                    "SELECT count(*) FROM merezha_notify.provider_events \
                     WHERE delivery_id = '{id}' AND pending"
                ))
                .await,
            1
        );
        wait_status(&instance, "delivered", 1).await;
        assert_eq!(
            world
                .scalar("SELECT count(*) FROM merezha_notify.provider_events WHERE pending")
                .await,
            0
        );
        assert_eq!(attempts(&world).await, ["accepted"]);
        instance.stop().await;
    }
}

/// Run the email delivery handler once, as the jobs runtime would on
/// `attempt` of 10, for a job of idempotency key `key` with `payload`.
async fn run_keyed(
    instance: &Instance,
    payload: Value,
    attempt: u32,
    key: &str,
) -> Result<(), String> {
    use merezha_platform::ports::{Channel, JobContext, JobHandler};
    let slot: merezha_notifications::notifier::Slot =
        Arc::new(std::sync::OnceLock::from(instance.core()));
    let (_cancel, cancelled) = tokio::sync::watch::channel(false);
    let ctx = JobContext {
        id: 0,
        kind: id("notify.deliver.email"),
        attempt,
        max_attempts: 10,
        idempotency_key: key.to_owned(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(20),
        cancel: cancelled,
        correlation_id: id("corr1"),
        causation_id: None,
        tenant: None,
    };
    merezha_notifications::handler::Deliver(slot, Channel::Email)
        .handle(ctx, payload)
        .await
        .map_err(|failure| failure.code.clone())
}

/// Issue #63: a `provider_account` answer hands the delivery to a new job
/// that carries the attempt it answered. The first job running again
/// (its worker died after Tx B, before the job was marked done) is
/// dropped by Tx A: no second chain sends. The continuation still sends.
#[tokio::test]
async fn a_job_run_again_after_handing_the_delivery_on_sends_nothing() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    fake.fail_next(1, 401);
    let instance = Instance::start(&world, options(&section(&fake, PROVIDER_DELAYS, "", ""))).await;
    commit(
        &instance,
        request("tasks.assigned", email("fen@example.org", None), "f:1"),
    )
    .await;
    // The continuation waits at least five minutes; its key names the
    // attempt it continues after (the first job's state also counts as
    // available while it is retried, so wait for the continuation itself).
    let continuation_job = "SELECT count(*) FROM merezha_jobs.jobs \
        WHERE kind = 'notify.deliver.email' AND state = 'available' \
        AND idempotency_key LIKE '%:g0:a1'";
    instance.until(continuation_job, 1).await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.deliveries \
             WHERE status = 'retrying' AND status_reason = 'provider_account'",
            1,
        )
        .await;
    let delivery_id = world
        .text("SELECT id::text FROM merezha_notify.deliveries")
        .await
        .remove(0);
    let (key, continuation) = {
        let row = world
            .text(
                "SELECT idempotency_key || ' ' || payload::text FROM merezha_jobs.jobs \
                 WHERE kind = 'notify.deliver.email' AND state = 'available' \
                 AND idempotency_key LIKE '%:g0:a1'",
            )
            .await
            .remove(0);
        let (key, payload) = row.split_once(' ').unwrap();
        (
            key.to_owned(),
            serde_json::from_str::<Value>(payload).unwrap(),
        )
    };
    // The payload carries no fence, so no older binary refuses it.
    assert!(
        continuation.get("after_attempt").is_none(),
        "{continuation}"
    );
    // The first job, run again with its own key, finds another chain.
    let first_key = key.trim_end_matches(":a1").to_owned();
    let first = json!({ "delivery_id": delivery_id, "generation": 0 });
    assert_eq!(run_keyed(&instance, first, 2, &first_key).await, Ok(()));
    assert_eq!(attempts(&world).await, ["transient"]);
    assert!(fake.processed().is_empty(), "no second chain sent");
    // A job naming another attempt than the last one is dropped too.
    let stray = key.replace(":a1", ":a7");
    assert_eq!(
        run_keyed(&instance, continuation.clone(), 1, &stray).await,
        Ok(())
    );
    assert_eq!(attempts(&world).await, ["transient"]);
    // The continuation, fenced by its key alone, is the live chain.
    assert_eq!(run_keyed(&instance, continuation, 1, &key).await, Ok(()));
    assert_eq!(attempts(&world).await, ["transient", "accepted"]);
    assert_eq!(fake.processed().len(), 1);
    assert_eq!(delivery(&world, "fen@example.org").await, "accepted:");
    instance.stop().await;
}

/// Issue #68: message ids are kept per attempt, so a late report about an
/// earlier generation's message moves no status even when the current
/// generation has none of its own (N1: here a duplicate accepted the
/// redelivery). Without a message id, a report on a redelivered delivery
/// may only move it to `delivered` (N2).
#[tokio::test]
async fn late_reports_never_settle_a_redelivery_without_a_message_id() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    let instance = Instance::start(&world, options(&section(&fake, "", "", ""))).await;
    commit(
        &instance,
        request("tasks.assigned", email("gen2@example.org", None), "g:2"),
    )
    .await;
    wait_status(&instance, "accepted", 1).await;
    let first = fake.processed().remove(0);
    assert_eq!(
        callback(&instance, &fake.event("blocked", &first)).await.0,
        200
    );
    assert_eq!(delivery(&world, "gen2@example.org").await, "failed:blocked");
    assert_eq!(
        world
            .text(
                "SELECT generation || ':' || provider_message_id \
                 FROM merezha_notify.delivery_attempts WHERE provider_message_id IS NOT NULL"
            )
            .await,
        [format!("0:{}", first.message_id)]
    );
    // The redelivery is processed, its connection dropped, and the resend
    // answered as a duplicate: accepted without a message id.
    fake.close_after_accept();
    instance
        .tokens
        .add("admin", user("usr_admin", &["notifications.admin"], None));
    let id = world
        .text("SELECT id::text FROM merezha_notify.deliveries")
        .await
        .remove(0);
    let (status, _) = instance
        .call(
            "POST",
            &format!("/v1/notifications/deliveries/{id}/redeliver"),
            Some("admin"),
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 202);
    wait_status(&instance, "accepted", 1).await;
    assert_eq!(
        attempts(&world).await,
        ["accepted", "ambiguous", "duplicate"]
    );
    assert_eq!(
        world
            .text("SELECT coalesce(provider_message_id, 'none') FROM merezha_notify.deliveries")
            .await,
        ["none"]
    );
    // N1: a late bounce about the first message is stored, moves nothing.
    assert_eq!(
        callback(&instance, &fake.event("hard_bounce", &first))
            .await
            .0,
        200
    );
    assert_eq!(delivery(&world, "gen2@example.org").await, "accepted:");
    // N2: without a message id, a failure report moves nothing...
    let bare = |event: &str| {
        let mut payload = fake.event(event, &first);
        payload.as_object_mut().unwrap().remove("message-id");
        payload.as_object_mut().unwrap().remove("messageId");
        payload
    };
    assert_eq!(callback(&instance, &bare("blocked")).await.0, 200);
    assert_eq!(delivery(&world, "gen2@example.org").await, "accepted:");
    // ... and a `delivered` still applies.
    assert_eq!(callback(&instance, &bare("delivered")).await.0, 200);
    assert_eq!(delivery(&world, "gen2@example.org").await, "delivered:");
    assert_eq!(
        world
            .scalar(&format!(
                "SELECT count(*) FROM merezha_notify.provider_events WHERE delivery_id = '{id}'"
            ))
            .await,
        4,
        "every report is stored"
    );
    instance.stop().await;
}
