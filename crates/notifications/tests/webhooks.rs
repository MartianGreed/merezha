//! Outgoing webhooks (NT §5.5, slice NT-N7) against real PostgreSQL and
//! the test kit's `WebhookReceiver` and `TestResolver`: the endpoint API,
//! sealed secrets and rotation, the fan-out and its tenant isolation, the
//! signed worker and its schedule, 410 and auto-disable, audit rows, the
//! egress guard at registration and at send, and what never reaches
//! telemetry.
mod harness;
use harness::*;
use merezha_domain::Failure;
use merezha_notifications::{
    faults,
    maintenance::{self, Task},
    testkit::{TestResolver, WebhookReceiver},
    webhooks::{self, fanout},
};
use merezha_platform::ports::{
    EventId, GrantSnapshot, Grants, PermissionSet, Principal, PrincipalKind, WebhookEvent,
};
use serde_json::{Value, json};
use std::{
    net::IpAddr,
    sync::Arc,
    time::{Duration, Instant},
};

const READ: &str = "notifications.webhooks.read";
const MANAGE: &str = "notifications.webhooks.manage";

/// Webhooks on, plain HTTP to loopback allowed (development), the
/// 24-hour backoff cap, and `queue` added to the `notify.webhook` queue.
fn section(private: bool, queue: &str) -> String {
    format!(
        "[jobs.queues.\"notify.webhook\"]\nbackoff_max_ms = 86400000\n{queue}\
         {SECTION}egress = {{ allow_private_networks = {private} }}\n\
         [modules.notifications.webhooks]\nallow_http = true\n\
         event_types = {{ \"tasks.task_completed\" = {{ description = \"A task was completed\" }} }}\n"
    )
}

/// A user with `permissions` in `tenant` (tenant-scope grants).
fn member(subject: &str, tenant: &str, permissions: &[&str]) -> Principal {
    let set: PermissionSet = permissions.iter().map(|p| id(p)).collect();
    Principal {
        subject: id(subject),
        kind: PrincipalKind::User,
        user: Some(id(subject)),
        service_account: None,
        tenant: Some(id(tenant)),
        grants: Arc::new(GrantSnapshot::empty().with_tenant(Grants::new(Vec::new(), set))),
        session: None,
    }
}

fn people() -> Arc<People> {
    let people = Arc::new(People::default());
    people.user("usr_ada", "ada@example.com", Some("Ada"), None);
    people.user("usr_bob", "bob@example.com", Some("Bob"), None);
    people.join("org_a", "usr_ada", &["owner"]);
    people.join("org_a", "usr_bob", &["member"]);
    people
}

async fn start_with(world: &World, section: &str, edit: impl FnOnce(&mut Options)) -> Instance {
    let mut options = Options::new(section, people());
    edit(&mut options);
    let instance = Instance::start(world, options).await;
    instance
        .tokens
        .add("ada", member("usr_ada", "org_a", &[READ, MANAGE]));
    instance
        .tokens
        .add("bea", member("usr_bea", "org_b", &[READ, MANAGE]));
    instance
        .tokens
        .add("ops", user("usr_ops", &[READ, MANAGE], None));
    instance
}
async fn start(world: &World, section: &str) -> Instance {
    start_with(world, section, |_| {}).await
}

/// `POST …/webhooks`: the endpoint with its secret.
async fn create(instance: &Instance, token: &str, prefix: &str, body: Value) -> (u16, Value) {
    let (status, body) = instance
        .call(
            "POST",
            &format!("/v1/notifications{prefix}/webhooks"),
            Some(token),
            Some(body),
        )
        .await;
    (status.as_u16(), body)
}

/// Emit `event_type` from `source` for `tenant` in a committed transaction.
async fn emit(
    instance: &Instance,
    source: &str,
    tenant: Option<&str>,
    event_type: &str,
    key: &str,
) {
    let notifier = instance.notifier(source);
    let event = WebhookEvent {
        tenant: tenant.map(id),
        event_type: id(event_type),
        source_event_id: EventId::new(key).unwrap(),
        occurred_at: jiff::Timestamp::from_second(1_800_000_000).unwrap(),
        data: json!({ "key": key }),
    };
    instance
        .db
        .transaction(soon(), move |tx| {
            Box::pin(async move {
                notifier
                    .emit_webhook(tx, event)
                    .await
                    .map_err(Failure::from)
            })
        })
        .await
        .unwrap();
}

/// `status:last_error_code` of every delivery, by creation.
async fn deliveries(world: &World) -> Vec<String> {
    world
        .text(
            "SELECT status || ':' || COALESCE(last_error_code, '') \
             FROM merezha_notify.webhook_deliveries ORDER BY created_at, id",
        )
        .await
}
async fn attempts(world: &World) -> Vec<String> {
    world
        .text(
            "SELECT outcome || ':' || COALESCE(http_status::text, '') \
             FROM merezha_notify.webhook_attempts ORDER BY started_at, attempt",
        )
        .await
}
/// Run the waiting delivery jobs now.
async fn run_now(world: &World) {
    world
        .execute(
            "UPDATE merezha_jobs.jobs SET run_at = now() \
             WHERE kind = 'notify.webhook.deliver' AND state = 'available'",
        )
        .await;
}

/// The design acceptance: a delivered webhook verifies with the secret
/// shown at creation, over the frozen `{type, timestamp, data}` envelope,
/// with the Standard Webhooks headers.
#[tokio::test]
async fn a_webhook_reaches_its_endpoint_signed_over_the_frozen_envelope() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = start(&world, &section(true, "")).await;
    let (status, endpoint) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url("a"), "description": "CRM" }),
    )
    .await;
    assert_eq!(status, 201, "{endpoint}");
    let secret = endpoint["secret"].as_str().unwrap().to_owned();
    assert!(secret.starts_with("whsec_"));
    assert_eq!(endpoint["status"], "active");
    assert_eq!(endpoint["revision"], 1);
    receiver.trust(&secret);
    emit(
        &instance,
        "application",
        Some("org_a"),
        "tasks.task_completed",
        "e1",
    )
    .await;
    instance.settle().await;
    assert_eq!(deliveries(&world).await, ["succeeded:"]);
    let received = receiver.received();
    assert_eq!(received.len(), 1);
    let got = &received[0];
    assert_eq!(got.verified_by, [0]);
    assert_eq!(got.signatures().len(), 1);
    assert_eq!(got.user_agent.as_deref(), Some("Merezha-Webhooks/1"));
    assert_eq!(got.content_type.as_deref(), Some("application/json"));
    let delivery: String = world
        .text("SELECT replace(id::text, '-', '') FROM merezha_notify.webhook_deliveries")
        .await
        .remove(0);
    assert_eq!(
        got.webhook_id.as_deref(),
        Some(format!("msg_{delivery}").as_str())
    );
    assert_eq!(
        String::from_utf8(got.body.clone()).unwrap(),
        r#"{"type":"tasks.task_completed","timestamp":"2027-01-15T08:00:00Z","data":{"key":"e1"}}"#
    );
    let stored: Vec<String> = world
        .text("SELECT convert_from(body, 'UTF8') FROM merezha_notify.webhook_events")
        .await;
    assert_eq!(stored[0].as_bytes(), got.body.as_slice());
    let (_, endpoint) = instance
        .call(
            "GET",
            &format!(
                "/v1/notifications/tenants/org_a/webhooks/{}",
                endpoint["id"].as_str().unwrap()
            ),
            Some("ada"),
            None,
        )
        .await;
    assert!(endpoint["last_success_at"].is_string());
    instance.stop().await;
}

/// Tenant isolation of the fan-out: with endpoints for tenants A and B and
/// at application level, an event of A reaches only A's, and a tenantless
/// content event only the application-level ones whose filter matches.
#[tokio::test]
async fn fan_out_keeps_tenants_and_the_application_registry_apart() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = start(&world, &section(true, "")).await;
    for (token, prefix, name, filters) in [
        ("ada", "/tenants/org_a", "a", json!([])),
        ("bea", "/tenants/org_b", "b", json!([])),
        ("ops", "", "app-content", json!(["content.*"])),
        ("ops", "", "app-tasks", json!(["tasks.task_completed"])),
    ] {
        let (status, body) = create(
            &instance,
            token,
            prefix,
            json!({ "url": receiver.url(name), "event_types": filters }),
        )
        .await;
        assert_eq!(status, 201, "{name}: {body}");
    }
    emit(
        &instance,
        "application",
        Some("org_a"),
        "tasks.task_completed",
        "t1",
    )
    .await;
    instance.settle().await;
    let names = |r: &WebhookReceiver| {
        let mut names: Vec<String> = r.received().into_iter().map(|r| r.name).collect();
        names.sort();
        names
    };
    assert_eq!(names(&receiver), ["a"]);
    emit(
        &instance,
        "content",
        None,
        "content.entry_published",
        "entry-1",
    )
    .await;
    instance.settle().await;
    assert_eq!(names(&receiver), ["a", "app-content"]);
    // A tenantless task event reaches the application-level task endpoint
    // only, never a tenant's.
    emit(&instance, "application", None, "tasks.task_completed", "t2").await;
    instance.settle().await;
    assert_eq!(names(&receiver), ["a", "app-content", "app-tasks"]);
    // Another tenant's principal cannot see or manage A's registry.
    let (status, _) = instance
        .call(
            "GET",
            "/v1/notifications/tenants/org_a/webhooks",
            Some("bea"),
            None,
        )
        .await;
    assert_eq!(status, 403);
    let (status, _) = instance
        .call("GET", "/v1/notifications/webhooks", Some("ada"), None)
        .await;
    assert_eq!(status, 403);
    let (status, listed) = instance
        .call("GET", "/v1/notifications/webhooks", Some("ops"), None)
        .await;
    assert_eq!(status, 200);
    assert_eq!(listed.as_array().unwrap().len(), 2);
    instance.stop().await;
}

/// NT-N7 test: a fan-out job that runs twice still gives one delivery per
/// endpoint, and one job record each.
#[tokio::test]
async fn a_duplicated_fan_out_gives_one_delivery_per_endpoint() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let mut opts = Options::new(&section(true, ""), people());
    opts.workers = false;
    let instance = Instance::start(&world, opts).await;
    instance
        .tokens
        .add("ada", member("usr_ada", "org_a", &[READ, MANAGE]));
    for name in ["a", "b"] {
        let (status, _) = create(
            &instance,
            "ada",
            "/tenants/org_a",
            json!({ "url": receiver.url(name) }),
        )
        .await;
        assert_eq!(status, 201);
    }
    let core = instance.core();
    let event = fanout::Event {
        id: uuid::Uuid::from_u128(7),
        source: "application".into(),
        source_event_id: "dup".into(),
        tenant: Some("org_a".into()),
        event_type: "tasks.task_completed".into(),
        occurred_at: jiff::Timestamp::now(),
        data: json!({}),
    };
    for _ in 0..2 {
        let outcome = fanout::fan_out(&core, event.clone(), id("corr"), soon()).await;
        assert_eq!(outcome, merezha_notifications::handler::Outcome::Done);
    }
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.webhook_deliveries")
            .await,
        2
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_jobs.outbox WHERE dedupe_key LIKE 'webhook:%:g0'")
            .await,
        2
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.webhook_events")
            .await,
        1
    );
}

/// A rotation window carries two signatures, the new secret's first, and
/// both verify; `keep_previous_hours = 0` keeps none, and a window that
/// closed signs with the new secret alone.
#[tokio::test]
async fn a_rotation_window_carries_two_signatures() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let clock = Arc::new(Shifted::default());
    let instance = start_with(&world, &section(true, ""), |o| {
        o.clock = Some(clock.clone());
    })
    .await;
    let (_, endpoint) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url("a") }),
    )
    .await;
    let id = endpoint["id"].as_str().unwrap().to_owned();
    receiver.trust(endpoint["secret"].as_str().unwrap());
    let path = format!("/v1/notifications/tenants/org_a/webhooks/{id}");
    let (status, rotated) = instance
        .call("POST", &format!("{path}/rotate-secret"), Some("ada"), None)
        .await;
    assert_eq!(status, 200, "{rotated}");
    receiver.trust(rotated["secret"].as_str().unwrap());
    assert!(rotated["previous_expires_at"].is_string());
    let test = |name: &'static str| {
        let (instance, path) = (&instance, path.clone());
        async move {
            let (status, body) = instance
                .call("POST", &format!("{path}/test"), Some("ada"), None)
                .await;
            assert_eq!(status, 202, "{name}: {body}");
            instance.settle().await;
        }
    };
    test("window").await;
    let got = receiver.received().pop().unwrap();
    assert_eq!(got.signatures().len(), 2);
    assert_eq!(got.verified_by, [0, 1]);
    assert_eq!(got.json()["type"], "webhook.test");
    // The window closes after 24 h.
    clock.advance(Duration::from_secs(25 * 3600));
    test("closed").await;
    let got = receiver.received().pop().unwrap();
    assert_eq!(got.signatures().len(), 1);
    assert_eq!(got.verified_by, [1]);
    // No window at all.
    let (_, rotated) = instance
        .call(
            "POST",
            &format!("{path}/rotate-secret"),
            Some("ada"),
            Some(json!({ "keep_previous_hours": 0 })),
        )
        .await;
    assert!(rotated["previous_expires_at"].is_null());
    receiver.trust(rotated["secret"].as_str().unwrap());
    test("none").await;
    let got = receiver.received().pop().unwrap();
    assert_eq!(
        (got.signatures().len(), got.verified_by.clone()),
        (1, vec![2])
    );
    let (status, _) = instance
        .call(
            "POST",
            &format!("{path}/rotate-secret"),
            Some("ada"),
            Some(json!({ "keep_previous_hours": 73 })),
        )
        .await;
    assert_eq!(status, 422);
    instance.stop().await;
}

/// `webhook-id` stays stable across retries and a redelivery; a 503 sets
/// `failing_since` and a success clears it.
#[tokio::test]
async fn webhook_id_is_stable_across_retries_and_redelivery() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = start(&world, &section(true, "")).await;
    let (_, endpoint) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url("a") }),
    )
    .await;
    let id = endpoint["id"].as_str().unwrap().to_owned();
    receiver.fail_next(1, 503, "down for maintenance\u{7}");
    emit(
        &instance,
        "application",
        Some("org_a"),
        "tasks.task_completed",
        "r1",
    )
    .await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.webhook_deliveries WHERE status = 'retrying'",
            1,
        )
        .await;
    assert_eq!(
        world
            .text("SELECT response_snippet FROM merezha_notify.webhook_attempts")
            .await,
        ["down for maintenance"]
    );
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.webhook_endpoints WHERE failing_since IS NOT NULL"
            )
            .await,
        1
    );
    run_now(&world).await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.webhook_deliveries WHERE status = 'succeeded'",
            1,
        )
        .await;
    assert_eq!(attempts(&world).await, ["failed:503", "succeeded:204"]);
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.webhook_endpoints WHERE failing_since IS NULL"
            )
            .await,
        1
    );
    let delivery = world
        .text("SELECT id::text FROM merezha_notify.webhook_deliveries")
        .await
        .remove(0);
    let path = format!("/v1/notifications/tenants/org_a/webhooks/{id}/deliveries");
    let (status, redelivered) = instance
        .call(
            "POST",
            &format!("{path}/{delivery}/redeliver"),
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(status, 202, "{redelivered}");
    assert_eq!(redelivered["generation"], 1);
    instance.settle().await;
    let received = receiver.received();
    assert_eq!(received.len(), 3);
    assert!(
        received
            .iter()
            .all(|r| r.webhook_id == received[0].webhook_id)
    );
    // The listing shows the delivery and its attempts.
    let (status, listed) = instance.call("GET", &path, Some("ada"), None).await;
    assert_eq!(status, 200);
    assert_eq!(listed["items"][0]["attempts"].as_array().unwrap().len(), 3);
    assert_eq!(listed["items"][0]["status"], "succeeded");
    // An open delivery cannot be redelivered, an expired body answers 410.
    world
        .execute("UPDATE merezha_notify.webhook_events SET body = NULL")
        .await;
    let (status, body) = instance
        .call(
            "POST",
            &format!("{path}/{delivery}/redeliver"),
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(
        (status.as_u16(), body["error"]["code"].clone()),
        (410, json!("event_body_expired"))
    );
    instance.stop().await;
}

/// A 410 disables the endpoint at once (`gone`), fails the delivery and
/// cancels the endpoint's queued deliveries; disabling is audited.
#[tokio::test]
async fn a_410_disables_the_endpoint_at_once() {
    let Some(world) = World::new().await else {
        return;
    };
    let harness = merezha_telemetry::testing::Harness::new();
    let receiver = WebhookReceiver::start().await;
    let instance = start_with(&world, &section(true, ""), |o| {
        o.meters = Some(harness.telemetry().meters());
    })
    .await;
    let (_, endpoint) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url("a") }),
    )
    .await;
    let id = endpoint["id"].as_str().unwrap().to_owned();
    receiver.respond(410);
    emit(
        &instance,
        "application",
        Some("org_a"),
        "tasks.task_completed",
        "g1",
    )
    .await;
    instance.settle().await;
    assert_eq!(deliveries(&world).await, ["failed:gone"]);
    let (_, shown) = instance
        .call(
            "GET",
            &format!("/v1/notifications/tenants/org_a/webhooks/{id}"),
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(
        (shown["status"].clone(), shown["disabled_reason"].clone()),
        (json!("disabled"), json!("gone"))
    );
    // Nothing more is sent; an event now creates no delivery.
    emit(
        &instance,
        "application",
        Some("org_a"),
        "tasks.task_completed",
        "g2",
    )
    .await;
    instance.settle().await;
    assert_eq!(receiver.received().len(), 1);
    assert_eq!(
        world
            .text("SELECT action || ':' || COALESCE(reason, '') || ':' || actor FROM merezha_notify.webhook_audit ORDER BY id")
            .await,
        ["create::usr_ada", "disable:gone:sys_notifications"]
    );
    assert_eq!(
        harness
            .metric("merezha.webhook.endpoints_disabled")
            .value(&[("notify.reason", "gone")]),
        Some(1.0)
    );
    assert_eq!(
        harness
            .metric("merezha.webhook.attempts")
            .value(&[("notify.class", "410")]),
        Some(1.0)
    );
    // A queued delivery of a disabled endpoint is cancelled, not sent.
    let (status, _) = instance
        .call(
            "POST",
            &format!("/v1/notifications/tenants/org_a/webhooks/{id}/test"),
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(status, 409);
    instance.stop().await;
}

/// Registration refuses blocked IP literals in every spelling and names
/// that resolve only to blocked addresses, each with its reason.
#[tokio::test]
async fn registration_refuses_private_destinations() {
    let Some(world) = World::new().await else {
        return;
    };
    let resolver = TestResolver::new();
    resolver.map("internal.test", &["10.0.0.5".parse().unwrap()]);
    resolver.map("hooks.test", &["93.184.216.34".parse().unwrap()]);
    let instance = start_with(&world, &section(false, ""), |o| {
        o.push_lookup = Some(Arc::new(resolver.clone()));
    })
    .await;
    for (url, reason) in [
        ("https://127.0.0.1/hook", "blocked_address"),
        ("https://[::ffff:127.0.0.1]/hook", "blocked_address"),
        ("https://0x7f000001/hook", "blocked_address"),
        ("https://internal.test/hook", "blocked_address"),
        ("https://nowhere.test/hook", "unresolvable"),
        ("ftp://hooks.test/hook", "scheme"),
        ("https://user:pw@hooks.test/hook", "userinfo"),
        ("https://hooks.test/hook#x", "fragment"),
    ] {
        let (status, body) =
            create(&instance, "ada", "/tenants/org_a", json!({ "url": url })).await;
        assert_eq!(status, 422, "{url}: {body}");
        assert_eq!(body["error"]["code"], "webhook_url_rejected", "{url}");
        assert_eq!(
            body["error"]["details"],
            json!([{ "path": "/url", "code": reason }]),
            "{url}"
        );
    }
    let (status, body) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": "https://hooks.test/hook" }),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    let (status, body) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": "https://hooks.test/hook", "event_types": ["tasks.unknown"] }),
    )
    .await;
    assert_eq!(
        (status, body["error"]["code"].clone()),
        (422, json!("unknown_event_type"))
    );
    instance.stop().await;
}

/// A 3xx is a failure and its `Location`, a private address, is never
/// followed.
#[tokio::test]
async fn a_redirect_to_a_private_address_is_not_followed() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let elsewhere = WebhookReceiver::start().await;
    let instance = start(&world, &section(true, "")).await;
    create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url("a") }),
    )
    .await;
    receiver.redirect_next(307, &elsewhere.url("inside"));
    emit(
        &instance,
        "application",
        Some("org_a"),
        "tasks.task_completed",
        "x1",
    )
    .await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.webhook_deliveries WHERE status = 'retrying'",
            1,
        )
        .await;
    assert_eq!(attempts(&world).await, ["failed:307"]);
    assert_eq!(deliveries(&world).await, ["retrying:redirected"]);
    assert_eq!(receiver.received().len(), 1);
    assert!(elsewhere.received().is_empty(), "the redirect was followed");
    instance.stop().await;
}

/// When the resolver returns a private and an allowed address, only the
/// allowed one is dialled; a name that later resolves only to the denied
/// range is blocked at send, which counts as a failed attempt.
#[tokio::test]
async fn the_guard_dials_only_the_allowed_address() {
    let Some(world) = World::new().await else {
        return;
    };
    let harness = merezha_telemetry::testing::Harness::new();
    let resolver = TestResolver::new();
    let private: IpAddr = "10.0.0.5".parse().unwrap();
    let allowed: IpAddr = "127.0.0.1".parse().unwrap();
    resolver.map("hooks.test", &[private, allowed]);
    let receiver = WebhookReceiver::start().await;
    // Development: private networks allowed except 10.0.0.0/8, so the
    // loopback receiver stands in for the public address.
    let config = section(true, "").replace(
        "egress = { allow_private_networks = true }",
        "egress = { allow_private_networks = true, deny_cidrs = [\"10.0.0.0/8\"] }",
    );
    let instance = start_with(&world, &config, |o| {
        o.push_lookup = Some(Arc::new(resolver.clone()));
        o.meters = Some(harness.telemetry().meters());
    })
    .await;
    let (status, body) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url_at("hooks.test", "pinned") }),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    emit(
        &instance,
        "application",
        Some("org_a"),
        "tasks.task_completed",
        "p1",
    )
    .await;
    instance.settle().await;
    assert_eq!(deliveries(&world).await, ["succeeded:"]);
    assert_eq!(receiver.received().len(), 1);
    // A name that resolved to an allowed address at registration and
    // only to the denied range at send is caught at send, where it counts
    // as a failed attempt.
    resolver.map("rebind.test", &[allowed]);
    let (status, body) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url_at("rebind.test", "rebound") }),
    )
    .await;
    assert_eq!(status, 201, "{body}");
    resolver.map("rebind.test", &[private]);
    emit(
        &instance,
        "application",
        Some("org_a"),
        "tasks.task_completed",
        "p2",
    )
    .await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.webhook_attempts WHERE outcome = 'blocked'",
            1,
        )
        .await;
    instance.settle().await;
    let names: Vec<String> = receiver.received().into_iter().map(|r| r.name).collect();
    assert_eq!(names, ["pinned", "pinned"]);
    let mut states = deliveries(&world).await;
    states.sort();
    assert_eq!(
        states,
        ["retrying:egress_blocked", "succeeded:", "succeeded:"]
    );
    assert_eq!(
        harness
            .metric("merezha.notification.egress_blocked")
            .value(&[("notify.target", "webhook")]),
        Some(1.0)
    );
    instance.stop().await;
}

/// The endpoint API: read and PATCH with a revision (412 on a stale one),
/// the per-registry limit, enable, disable, delete, the catalog, and an
/// audit row for every change, never with the secret.
#[tokio::test]
async fn the_endpoint_api_is_revisioned_limited_and_audited() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = start(
        &world,
        &section(true, "").replace(
            "allow_http = true\n",
            "allow_http = true\nmax_endpoints_per_tenant = 2\n",
        ),
    )
    .await;
    let (_, endpoint) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url("a"), "event_types": ["content.*", "tasks.task_completed"] }),
    )
    .await;
    let id = endpoint["id"].as_str().unwrap().to_owned();
    let path = format!("/v1/notifications/tenants/org_a/webhooks/{id}");
    let (status, _) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url("b") }),
    )
    .await;
    assert_eq!(status, 201);
    let (status, body) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url("c") }),
    )
    .await;
    assert_eq!(
        (status, body["error"]["code"].clone()),
        (409, json!("webhook_endpoint_limit"))
    );
    let (status, updated) = instance
        .call(
            "PATCH",
            &path,
            Some("ada"),
            Some(json!({ "description": "CRM", "event_types": [], "revision": 1 })),
        )
        .await;
    assert_eq!(status, 200, "{updated}");
    assert_eq!(
        (updated["revision"].clone(), updated["description"].clone()),
        (json!(2), json!("CRM"))
    );
    assert_eq!(updated["event_types"], json!([]));
    assert!(updated.get("secret").is_none());
    let (status, stale) = instance
        .call(
            "PATCH",
            &path,
            Some("ada"),
            Some(json!({ "description": null, "revision": 1 })),
        )
        .await;
    assert_eq!(
        (status.as_u16(), stale["error"]["code"].clone()),
        (412, json!("revision_conflict"))
    );
    let (status, disabled) = instance
        .call("POST", &format!("{path}/disable"), Some("ada"), None)
        .await;
    assert_eq!(status, 200);
    assert_eq!(disabled["disabled_reason"], "manual");
    let (_, enabled) = instance
        .call("POST", &format!("{path}/enable"), Some("ada"), None)
        .await;
    assert_eq!(
        (
            enabled["status"].clone(),
            enabled["disabled_reason"].clone()
        ),
        (json!("active"), Value::Null)
    );
    let (_, _) = instance
        .call("POST", &format!("{path}/rotate-secret"), Some("ada"), None)
        .await;
    let (status, _) = instance.call("DELETE", &path, Some("ada"), None).await;
    assert_eq!(status, 204);
    let (status, _) = instance.call("GET", &path, Some("ada"), None).await;
    assert_eq!(status, 404);
    // Another tenant's id under this tenant's path is 404, a missing grant 403.
    let (status, _) = instance
        .call(
            "GET",
            "/v1/notifications/tenants/org_a/webhooks",
            Some("bea"),
            None,
        )
        .await;
    assert_eq!(status, 403);
    assert_eq!(
        world
            .text(&format!(
                "SELECT action || ':' || COALESCE(reason, '') || ':' || revision \
                 FROM merezha_notify.webhook_audit WHERE endpoint_id = '{id}' ORDER BY id"
            ))
            .await,
        [
            "create::1",
            "update::2",
            "disable:manual:3",
            "enable::4",
            "rotate::5",
            "delete::6"
        ]
    );
    let (status, types) = instance
        .call(
            "GET",
            "/v1/notifications/webhook-event-types",
            Some("bea"),
            None,
        )
        .await;
    assert_eq!(status, 200);
    let names: Vec<&str> = types
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["type"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "content.entry_deleted",
            "content.entry_published",
            "content.entry_unpublished",
            "tasks.task_completed",
            "webhook.test"
        ]
    );
    instance.stop().await;
}

/// Signing secrets are sealed at rest and never reach a log, a span, a
/// metric, an error or an answer other than create and rotate.
#[tokio::test]
async fn secrets_are_sealed_and_never_shown_again() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = start_with(&world, &section(true, ""), |o| {
        o.meters = Some(harness.telemetry().meters());
    })
    .await;
    let (_, endpoint) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url("url-canary-5c1e") }),
    )
    .await;
    let id = endpoint["id"].as_str().unwrap().to_owned();
    let secret = endpoint["secret"].as_str().unwrap().to_owned();
    let encoded = secret.trim_start_matches("whsec_").to_owned();
    receiver.trust(&secret);
    let path = format!("/v1/notifications/tenants/org_a/webhooks/{id}");
    let mut answers = Vec::new();
    for (method, route, body) in [
        ("GET", path.clone(), None),
        (
            "GET",
            "/v1/notifications/tenants/org_a/webhooks".to_owned(),
            None,
        ),
        (
            "PATCH",
            path.clone(),
            Some(json!({ "revision": 1, "url": "nope" })),
        ),
        ("PATCH", path.clone(), Some(json!({ "revision": 9 }))),
        ("POST", format!("{path}/disable"), None),
        ("POST", format!("{path}/enable"), None),
        ("POST", format!("{path}/test"), None),
        ("GET", format!("{path}/deliveries"), None),
    ] {
        answers.push(
            instance
                .call(method, &route, Some("ada"), body)
                .await
                .1
                .to_string(),
        );
    }
    instance.settle().await;
    assert_eq!(receiver.received().last().unwrap().verified_by, [0]);
    for answer in &answers {
        assert!(
            !answer.contains(&encoded) && !answer.contains("whsec_"),
            "{answer}"
        );
    }
    // At rest: AES-256-GCM ciphertext and tag, not the secret bytes.
    use base64::Engine;
    let raw = base64::engine::general_purpose::STANDARD
        .decode(&encoded)
        .unwrap();
    let client = world.db.acquire(soon()).await.unwrap();
    let row = client
        .query_one(
            "SELECT secret_ct, secret_nonce, key_version FROM merezha_notify.webhook_endpoints",
            &[],
        )
        .await
        .unwrap();
    let (ct, nonce, version): (Vec<u8>, Vec<u8>, i16) = (row.get(0), row.get(1), row.get(2));
    assert_eq!((ct.len(), nonce.len(), version), (48, 12, 0));
    assert!(!ct.windows(raw.len()).any(|w| w == raw.as_slice()));
    drop(client);
    for canary in [secret.as_str(), encoded.as_str(), "url-canary-5c1e"] {
        harness.assert_no_canary(canary);
    }
    harness.assert_catalogued();
    instance.stop().await;
}

/// `retention` nulls old bodies, deletes old settled events, drops closed
/// rotation windows, and reseals secrets only `secret_key_previous` opens.
#[tokio::test]
async fn retention_expires_bodies_and_reseals_secrets_under_the_current_key() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = start(&world, &section(true, "")).await;
    let (_, endpoint) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url("a") }),
    )
    .await;
    let secret = endpoint["secret"].as_str().unwrap().to_owned();
    emit(
        &instance,
        "application",
        Some("org_a"),
        "tasks.task_completed",
        "old",
    )
    .await;
    instance.settle().await;
    instance.stop().await;
    world
        .execute("UPDATE merezha_notify.webhook_events SET created_at = now() - interval '40 days'")
        .await;
    // Rotate `secret_key`: the old one becomes `secret_key_previous`.
    let rotated = section(true, "").replace(
        "secret_key = { env = \"NOTIFY_KEY\" }\n",
        "secret_key = { env = \"NEW_KEY\" }\nsecret_key_previous = { env = \"NOTIFY_KEY\" }\n",
    );
    let instance = start_with(&world, &rotated, |o| {
        o.env.push((
            "NEW_KEY".into(),
            "ZmVkY2JhOTg3NjU0MzIxMGZlZGNiYTk4NzY1NDMyMTA=".into(),
        ));
    })
    .await;
    let before: Vec<String> = world
        .text("SELECT encode(secret_ct, 'hex') FROM merezha_notify.webhook_endpoints")
        .await;
    let changed = maintenance::run(&instance.core(), Task::Retention, soon())
        .await
        .unwrap();
    assert!(changed >= 2, "{changed}");
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.webhook_events WHERE body IS NULL")
            .await,
        1
    );
    let after: Vec<String> = world
        .text("SELECT encode(secret_ct, 'hex') FROM merezha_notify.webhook_endpoints")
        .await;
    assert_ne!(before, after, "resealed");
    // Without the previous key the resealed secret still signs.
    instance.stop().await;
    let current_only = section(true, "").replace(
        "secret_key = { env = \"NOTIFY_KEY\" }\n",
        "secret_key = { env = \"NEW_KEY\" }\n",
    );
    let instance = start_with(&world, &current_only, |o| {
        o.env.push((
            "NEW_KEY".into(),
            "ZmVkY2JhOTg3NjU0MzIxMGZlZGNiYTk4NzY1NDMyMTA=".into(),
        ));
    })
    .await;
    receiver.trust(&secret);
    emit(
        &instance,
        "application",
        Some("org_a"),
        "tasks.task_completed",
        "new",
    )
    .await;
    instance.settle().await;
    assert_eq!(receiver.received().last().unwrap().verified_by, [0]);
    world
        .execute(
            "UPDATE merezha_notify.webhook_events SET created_at = now() - interval '100 days'",
        )
        .await;
    maintenance::run(&instance.core(), Task::Retention, soon())
        .await
        .unwrap();
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.webhook_events")
            .await,
        0
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.webhook_deliveries")
            .await,
        0
    );
    instance.stop().await;
}

/// Crashes and retry delays race lease deadlines (issue #37): CI runs them
/// alone, one at a time (`cargo test -- --test-threads=1 timing::`).
mod timing {
    use super::*;

    /// `notify.webhook` leases expire after 3 s (three times the 1 s
    /// request timeout) and a crashed run is aborted after 6 s.
    fn crashing() -> String {
        section(true, "lease_ms = 3000\ntimeout_ms = 6000\n").replace(
            "allow_http = true\n",
            "allow_http = true\ntimeout = \"1s\"\n",
        )
    }

    /// NT-N7 test: a crash between the request and Tx B
    /// (`after_provider_call`) resends with the same `webhook-id`.
    #[tokio::test]
    async fn a_crash_after_the_call_resends_with_the_same_webhook_id() {
        let Some(world) = World::new().await else {
            return;
        };
        let receiver = WebhookReceiver::start().await;
        let instance = start_with(&world, &crashing(), |o| {
            o.heartbeat = false;
            o.faults.crash_once(faults::AFTER_PROVIDER_CALL);
        })
        .await;
        create(
            &instance,
            "ada",
            "/tenants/org_a",
            json!({ "url": receiver.url("a") }),
        )
        .await;
        emit(
            &instance,
            "application",
            Some("org_a"),
            "tasks.task_completed",
            "c1",
        )
        .await;
        instance
            .until(
                "SELECT count(*) FROM merezha_notify.webhook_deliveries WHERE status = 'succeeded'",
                1,
            )
            .await;
        assert_eq!(attempts(&world).await, ["ambiguous:", "succeeded:204"]);
        let received = receiver.received();
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].webhook_id, received[1].webhook_id);
        assert_eq!(received[0].body, received[1].body);
        instance.stop().await;
    }

    /// NT-N7 tests: a receiver outage follows the schedule on an injected
    /// clock (each delay within ±10 % of its step, AP keeping the 24-hour
    /// step under `backoff_max_ms = 86400000`), attempt 10 fails the
    /// delivery, and five days of failures disable the endpoint and email
    /// the tenant's `alert_roles` (Ada, an owner; not Bob, a member).
    #[tokio::test]
    async fn an_outage_follows_the_schedule_then_disables_and_alerts() {
        let Some(world) = World::new().await else {
            return;
        };
        let receiver = WebhookReceiver::start().await;
        receiver.respond(503);
        let clock = Arc::new(Shifted::default());
        let instance = start_with(&world, &section(true, ""), |o| {
            o.clock = Some(clock.clone());
        })
        .await;
        let (_, endpoint) = create(
            &instance,
            "ada",
            "/tenants/org_a",
            json!({ "url": receiver.url("a") }),
        )
        .await;
        let id = endpoint["id"].as_str().unwrap().to_owned();
        emit(
            &instance,
            "application",
            Some("org_a"),
            "tasks.task_completed",
            "o1",
        )
        .await;
        for attempt in 1..=10u32 {
            instance
                .until(
                    "SELECT count(*) FROM merezha_notify.webhook_attempts WHERE outcome = 'failed'",
                    i64::from(attempt),
                )
                .await;
            if attempt == 10 {
                break;
            }
            let step = webhooks::SCHEDULE[attempt as usize - 1].as_secs_f64();
            instance
                .until(
                    "SELECT count(*) FROM merezha_jobs.jobs \
                     WHERE kind = 'notify.webhook.deliver' AND state = 'available'",
                    1,
                )
                .await;
            let wait = world
                .scalar(
                    "SELECT extract(epoch FROM run_at - now())::bigint FROM merezha_jobs.jobs \
                     WHERE kind = 'notify.webhook.deliver' AND state = 'available'",
                )
                .await as f64;
            assert!(
                wait >= step * 0.9 - 2.0 && wait <= (step * 1.1).min(86_400.0) + 1.0,
                "attempt {attempt}: waits {wait} s for a {step} s step"
            );
            clock.advance(Duration::from_secs_f64(step));
            run_now(&world).await;
        }
        instance
            .until(
                "SELECT count(*) FROM merezha_notify.webhook_deliveries WHERE status = 'failed'",
                1,
            )
            .await;
        assert_eq!(deliveries(&world).await, ["failed:http_server_error"]);
        assert_eq!(receiver.received().len(), 10);
        // Over three days in, the endpoint is still active.
        let core = instance.core();
        assert_eq!(
            maintenance::run(&core, Task::WebhookAutoDisable, soon())
                .await
                .unwrap(),
            0
        );
        clock.advance(Duration::from_secs(2 * 86_400));
        let started = Instant::now();
        assert_eq!(
            maintenance::run(&core, Task::WebhookAutoDisable, soon())
                .await
                .unwrap(),
            1
        );
        assert!(started.elapsed() < Duration::from_secs(10));
        let (_, shown) = instance
            .call(
                "GET",
                &format!("/v1/notifications/tenants/org_a/webhooks/{id}"),
                Some("ada"),
                None,
            )
            .await;
        assert_eq!(
            (shown["status"].clone(), shown["disabled_reason"].clone()),
            (json!("disabled"), json!("failing"))
        );
        instance.settle().await;
        assert_eq!(
            world
                .text(
                    "SELECT target FROM merezha_notify.captures c \
                     JOIN merezha_notify.deliveries d ON d.id = c.delivery_id \
                     JOIN merezha_notify.notifications n ON n.id = d.notification_id \
                     WHERE n.topic = 'notifications.webhook_disabled'"
                )
                .await,
            ["ada@example.com"]
        );
        assert_eq!(
            world
                .scalar(
                    "SELECT count(*) FROM merezha_notify.inbox_items \
                     WHERE topic = 'notifications.webhook_disabled' AND user_id = 'usr_ada'"
                )
                .await,
            1
        );
        // Re-enabling clears `failing_since`.
        let (_, enabled) = instance
            .call(
                "POST",
                &format!("/v1/notifications/tenants/org_a/webhooks/{id}/enable"),
                Some("ada"),
                None,
            )
            .await;
        assert!(enabled["failing_since"].is_null());
        instance.stop().await;
    }
}
