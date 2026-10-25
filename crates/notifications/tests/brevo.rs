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
        "max_attempts = 10\nbackoff_base_ms = 10\nbackoff_max_ms = 50\n"
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
            "max_attempts = 3\nbackoff_base_ms = 10\nbackoff_max_ms = 50\n",
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
    assert_eq!(rejected, 4);
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

/// Crashes and rate-limit delays race lease and retry deadlines (issue
/// #37): CI runs them alone (`cargo test -- --test-threads=1 timing::`).
mod timing {
    use super::*;

    /// Leases (three Brevo timeouts) expire after 6 s and a crashed run is
    /// aborted after 3 s.
    const CRASHING: &str = "concurrency = 2\nlease_ms = 6000\ntimeout_ms = 3000\n\
        backoff_base_ms = 10\nbackoff_max_ms = 50\n";

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
}
