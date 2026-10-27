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
        ("http://[::ffff:0:7f00:1]/hook", "blocked_address"),
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
            "SELECT secret_ct, secret_nonce, key_id FROM merezha_notify.webhook_endpoints",
            &[],
        )
        .await
        .unwrap();
    let (ct, nonce, key_id): (Vec<u8>, Vec<u8>, Vec<u8>) = (row.get(0), row.get(1), row.get(2));
    assert_eq!((ct.len(), nonce.len(), key_id.len()), (48, 12, 8));
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

/// A disabled endpoint gets no new delivery from the fan-out.
#[tokio::test]
async fn a_disabled_endpoint_gets_no_new_delivery() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = start(&world, &section(true, "")).await;
    let mut ids = Vec::new();
    for name in ["on", "off"] {
        let (_, endpoint) = create(
            &instance,
            "ada",
            "/tenants/org_a",
            json!({ "url": receiver.url(name) }),
        )
        .await;
        ids.push(endpoint["id"].as_str().unwrap().to_owned());
    }
    let (status, _) = instance
        .call(
            "POST",
            &format!(
                "/v1/notifications/tenants/org_a/webhooks/{}/disable",
                ids[1]
            ),
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(status, 200);
    emit(
        &instance,
        "application",
        Some("org_a"),
        "tasks.task_completed",
        "d1",
    )
    .await;
    instance.settle().await;
    assert_eq!(deliveries(&world).await, ["succeeded:"]);
    let names: Vec<String> = receiver.received().into_iter().map(|r| r.name).collect();
    assert_eq!(names, ["on"]);
    instance.stop().await;
}

/// Every `{id}` route answers 404 when the caller is authorized on one
/// registry and names an endpoint (or a delivery) of another: tenant B
/// naming tenant A's, the application registry naming a tenant's, and a
/// tenant naming an application-level one. The endpoint is left intact.
#[tokio::test]
async fn an_endpoint_of_another_registry_is_not_found_on_every_route() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = start(&world, &section(true, "")).await;
    let (_, a) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url("a") }),
    )
    .await;
    let (_, app) = create(&instance, "ops", "", json!({ "url": receiver.url("app") })).await;
    let (a, app) = (
        a["id"].as_str().unwrap().to_owned(),
        app["id"].as_str().unwrap().to_owned(),
    );
    let delivery = |endpoint: String, token: &'static str, prefix: &'static str| {
        let instance = &instance;
        async move {
            let (status, body) = instance
                .call(
                    "POST",
                    &format!("/v1/notifications{prefix}/webhooks/{endpoint}/test"),
                    Some(token),
                    None,
                )
                .await;
            assert_eq!(status, 202, "{body}");
            body["delivery_id"].as_str().unwrap().to_owned()
        }
    };
    let a_delivery = delivery(a.clone(), "ada", "/tenants/org_a").await;
    let app_delivery = delivery(app.clone(), "ops", "").await;
    instance.settle().await;
    assert_eq!(deliveries(&world).await, ["succeeded:", "succeeded:"]);
    for (token, prefix, id, delivery) in [
        ("bea", "/tenants/org_b", &a, &a_delivery),
        ("ops", "/tenants/org_b", &a, &a_delivery),
        ("ops", "", &a, &a_delivery),
        ("ada", "/tenants/org_a", &app, &app_delivery),
    ] {
        let base = format!("/v1/notifications{prefix}/webhooks/{id}");
        for (method, path, body) in [
            ("GET", base.clone(), None),
            (
                "PATCH",
                base.clone(),
                Some(json!({ "description": "x", "revision": 1 })),
            ),
            ("POST", format!("{base}/rotate-secret"), None),
            ("POST", format!("{base}/enable"), None),
            ("POST", format!("{base}/disable"), None),
            ("POST", format!("{base}/test"), None),
            ("GET", format!("{base}/deliveries"), None),
            (
                "POST",
                format!("{base}/deliveries/{delivery}/redeliver"),
                None,
            ),
            ("DELETE", base.clone(), None),
        ] {
            let (status, answer) = instance.call(method, &path, Some(token), body).await;
            assert_eq!(status, 404, "{token} {method} {path}: {answer}");
            assert!(!answer.to_string().contains("/hooks/"), "{answer}");
        }
    }
    for (token, prefix, id) in [("ada", "/tenants/org_a", &a), ("ops", "", &app)] {
        let (status, shown) = instance
            .call(
                "GET",
                &format!("/v1/notifications{prefix}/webhooks/{id}"),
                Some(token),
                None,
            )
            .await;
        assert_eq!(status, 200);
        assert_eq!(
            (shown["status"].clone(), shown["revision"].clone()),
            (json!("active"), json!(1))
        );
    }
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.webhook_deliveries WHERE generation > 0")
            .await,
        0
    );
    instance.stop().await;
}

/// An open delivery whose job is gone (untouched for an hour) can be
/// redelivered; a fresh open one cannot.
#[tokio::test]
async fn a_stranded_open_delivery_can_be_redelivered() {
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
    receiver.fail_next(1, 503, "");
    emit(
        &instance,
        "application",
        Some("org_a"),
        "tasks.task_completed",
        "s1",
    )
    .await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.webhook_deliveries WHERE status = 'retrying'",
            1,
        )
        .await;
    let delivery = world
        .text("SELECT id::text FROM merezha_notify.webhook_deliveries")
        .await
        .remove(0);
    let path =
        format!("/v1/notifications/tenants/org_a/webhooks/{id}/deliveries/{delivery}/redeliver");
    let (status, body) = instance.call("POST", &path, Some("ada"), None).await;
    assert_eq!(
        (status.as_u16(), body["error"]["code"].clone()),
        (409, json!("not_redeliverable"))
    );
    // The job died and the delivery has not moved for two hours.
    world
        .execute(
            "UPDATE merezha_jobs.jobs SET state = 'dead', finished_at = now() \
             WHERE kind = 'notify.webhook.deliver' AND state = 'available'; \
             UPDATE merezha_notify.webhook_deliveries SET updated_at = now() - interval '2 hours', \
             next_attempt_at = now() - interval '2 hours'",
        )
        .await;
    let (status, body) = instance.call("POST", &path, Some("ada"), None).await;
    assert_eq!(status, 202, "{body}");
    instance.settle().await;
    assert_eq!(deliveries(&world).await, ["succeeded:"]);
    instance.stop().await;
}

/// When a job's last attempt fails with a retryable error, the delivery is
/// settled `failed` with that code instead of staying open behind a dead
/// job (here Tx B fails on every run).
#[tokio::test]
async fn a_job_whose_attempts_run_out_never_leaves_its_delivery_open() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = start(&world, &section(true, "max_attempts = 1\n")).await;
    create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url("a") }),
    )
    .await;
    world
        .execute(
            "CREATE FUNCTION merezha_notify.refuse() RETURNS trigger LANGUAGE plpgsql AS \
             $$ BEGIN RAISE EXCEPTION 'refused'; END $$; \
             CREATE TRIGGER refuse BEFORE UPDATE ON merezha_notify.webhook_endpoints \
             FOR EACH ROW EXECUTE FUNCTION merezha_notify.refuse()",
        )
        .await;
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
            "SELECT count(*) FROM merezha_notify.webhook_deliveries WHERE status = 'failed'",
            1,
        )
        .await;
    assert_eq!(attempts(&world).await, ["ambiguous:"]);
    assert_eq!(receiver.received().len(), 1);
    instance
        .until(
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.webhook.deliver' \
             AND state = 'dead'",
            1,
        )
        .await;
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.webhook_deliveries \
                 WHERE status IN ('queued', 'sending', 'retrying')"
            )
            .await,
        0
    );
    instance.stop().await;
}

/// `POST …/test` is limited per endpoint and per registry: 429
/// `rate_limited` with `Retry-After`.
#[tokio::test]
async fn test_events_are_rate_limited_per_endpoint_and_registry() {
    use tower::ServiceExt;
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
    let mut ids = Vec::new();
    for n in 0..7 {
        let (_, endpoint) = create(
            &instance,
            "ada",
            "/tenants/org_a",
            json!({ "url": receiver.url(&format!("e{n}")) }),
        )
        .await;
        ids.push(endpoint["id"].as_str().unwrap().to_owned());
    }
    let test = |id: &str| format!("/v1/notifications/tenants/org_a/webhooks/{id}/test");
    let per_endpoint = webhooks::routes::TESTS_PER_ENDPOINT;
    for _ in 0..per_endpoint {
        assert_eq!(
            instance
                .call("POST", &test(&ids[0]), Some("ada"), None)
                .await
                .0,
            202
        );
    }
    let request = axum::http::Request::post(test(&ids[0]))
        .header("authorization", "Bearer ada")
        .body(axum::body::Body::empty())
        .unwrap();
    let response = instance.router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 429);
    assert_eq!(response.headers()["retry-after"], "60");
    // Another endpoint still has room, until the registry's budget is spent.
    let per_registry = webhooks::routes::TESTS_PER_REGISTRY;
    let mut sent = per_endpoint;
    'outer: for id in &ids[1..] {
        for _ in 0..per_endpoint {
            if sent == per_registry {
                break 'outer;
            }
            assert_eq!(
                instance.call("POST", &test(id), Some("ada"), None).await.0,
                202
            );
            sent += 1;
        }
    }
    let (status, body) = instance
        .call("POST", &test(&ids[6]), Some("ada"), None)
        .await;
    assert_eq!(
        (status.as_u16(), body["error"]["code"].clone()),
        (429, json!("rate_limited"))
    );
}

/// `retention` reseals at most its bounds per run and picks up where the
/// last run stopped, since resealed rows no longer match; the gauge counts
/// what remains, and `secret_key_previous` can go once it is 0.
#[tokio::test]
async fn resealing_resumes_across_runs_and_counts_what_remains() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = start(&world, &section(true, "")).await;
    let mut secrets = Vec::new();
    for n in 0..7 {
        let (_, endpoint) = create(
            &instance,
            "ada",
            "/tenants/org_a",
            json!({ "url": receiver.url(&format!("r{n}")) }),
        )
        .await;
        secrets.push(endpoint["secret"].as_str().unwrap().to_owned());
    }
    instance.stop().await;
    let rotated = section(true, "").replace(
        "secret_key = { env = \"NOTIFY_KEY\" }\n",
        "secret_key = { env = \"NEW_KEY\" }\nsecret_key_previous = { env = \"NOTIFY_KEY\" }\n",
    );
    let instance = start_with(&world, &rotated, |o| {
        o.env.push(("NEW_KEY".into(), NEW_KEY.into()));
        o.meters = Some(harness.telemetry().meters());
    })
    .await;
    let bounds = webhooks::tasks::ResealBounds {
        batch: 2,
        batches: 2,
    };
    let core = instance.core();
    let first = webhooks::tasks::reseal(&core, soon(), bounds)
        .await
        .unwrap();
    assert_eq!((first.resealed, first.remaining), (4, 3));
    assert_eq!(
        harness
            .metric("merezha.webhook.secrets.previous_key")
            .value(&[]),
        Some(3.0)
    );
    let second = webhooks::tasks::reseal(&core, soon(), bounds)
        .await
        .unwrap();
    assert_eq!((second.resealed, second.remaining), (3, 0));
    instance.stop().await;
    // Without the previous key every endpoint still signs.
    let current_only = section(true, "").replace(
        "secret_key = { env = \"NOTIFY_KEY\" }\n",
        "secret_key = { env = \"NEW_KEY\" }\n",
    );
    let instance = start_with(&world, &current_only, |o| {
        o.env.push(("NEW_KEY".into(), NEW_KEY.into()));
    })
    .await;
    for secret in &secrets {
        receiver.trust(secret);
    }
    emit(
        &instance,
        "application",
        Some("org_a"),
        "tasks.task_completed",
        "all",
    )
    .await;
    instance.settle().await;
    let received = receiver.received();
    assert_eq!(received.len(), 7);
    let mut verified: Vec<usize> = received
        .iter()
        .flat_map(|r| r.verified_by.clone())
        .collect();
    verified.sort();
    assert_eq!(verified, (0..7).collect::<Vec<_>>());
    instance.stop().await;
}

/// A second `secret_key`, for rotation tests.
const NEW_KEY: &str = "ZmVkY2JhOTg3NjU0MzIxMGZlZGNiYTk4NzY1NDMyMTA=";

/// `section(true, "")` with `secret_key` rotated: `NEW_KEY` current, the
/// harness key previous (or gone with `keep_previous = false`).
fn rotated_section(keep_previous: bool) -> String {
    let previous = if keep_previous {
        "secret_key_previous = { env = \"NOTIFY_KEY\" }\n"
    } else {
        ""
    };
    section(true, "").replace(
        "secret_key = { env = \"NOTIFY_KEY\" }\n",
        &format!("secret_key = {{ env = \"NEW_KEY\" }}\n{previous}"),
    )
}

/// The review's reproduction: once a rotation window closed, `retention`
/// drops the previous secret with its key id and succeeds.
#[tokio::test]
async fn retention_drops_a_closed_rotation_window() {
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
    let (status, _) = instance
        .call(
            "POST",
            &format!("/v1/notifications/tenants/org_a/webhooks/{id}/rotate-secret"),
            Some("ada"),
            Some(json!({ "keep_previous_hours": 1 })),
        )
        .await;
    assert_eq!(status, 200);
    world
        .execute(
            "UPDATE merezha_notify.webhook_endpoints \
             SET previous_expires_at = now() - interval '1 minute'",
        )
        .await;
    maintenance::run(&instance.core(), Task::Retention, soon())
        .await
        .unwrap();
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.webhook_endpoints WHERE previous_secret_ct IS NULL \
                 AND previous_secret_nonce IS NULL AND previous_key_id IS NULL \
                 AND previous_expires_at IS NULL"
            )
            .await,
        1
    );
    instance.stop().await;
}

/// A failing retention step neither stops the later ones nor the reseal
/// and its gauges; it is logged and is the task's outcome.
#[tokio::test]
async fn a_failing_retention_step_still_lets_the_reseal_and_gauges_run() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = start(&world, &section(true, "")).await;
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
        "old",
    )
    .await;
    instance.settle().await;
    instance.stop().await;
    world
        .execute(
            "UPDATE merezha_notify.webhook_events SET created_at = now() - interval '40 days'; \
             CREATE FUNCTION merezha_notify.refuse() RETURNS trigger LANGUAGE plpgsql AS \
             $$ BEGIN RAISE EXCEPTION 'refused'; END $$; \
             CREATE TRIGGER refuse BEFORE UPDATE ON merezha_notify.webhook_events \
             FOR EACH ROW EXECUTE FUNCTION merezha_notify.refuse()",
        )
        .await;
    let instance = start_with(&world, &rotated_section(true), |o| {
        o.env.push(("NEW_KEY".into(), NEW_KEY.into()));
        o.meters = Some(harness.telemetry().meters());
    })
    .await;
    let before = world
        .text("SELECT encode(key_id, 'hex') FROM merezha_notify.webhook_endpoints")
        .await;
    let outcome = maintenance::run(&instance.core(), Task::Retention, soon()).await;
    assert!(outcome.is_err(), "{outcome:?}");
    let after = world
        .text("SELECT encode(key_id, 'hex') FROM merezha_notify.webhook_endpoints")
        .await;
    assert_ne!(before, after, "the reseal ran");
    let gauge = |name: &str| harness.metric(name).value(&[]);
    assert_eq!(gauge("merezha.webhook.secrets.previous_key"), Some(0.0));
    assert_eq!(gauge("merezha.webhook.secrets.unreadable"), Some(0.0));
    let failed: Vec<_> = harness
        .recorded()
        .into_iter()
        .filter(|r| r.name.as_deref() == Some("notification.maintenance_step_failed"))
        .collect();
    assert_eq!(failed.len(), 1);
    harness.assert_catalogued();
    instance.stop().await;
}

/// Secrets neither key opens are marked and skipped: with small bounds and
/// the unreadable rows first in id order, every run still progresses, the
/// previous-key gauge reaches 0 and the unreadable ones are counted apart.
#[tokio::test]
async fn unreadable_secrets_are_set_aside_and_counted_apart() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = start(&world, &section(true, "")).await;
    for n in 0..5 {
        create(
            &instance,
            "ada",
            "/tenants/org_a",
            json!({ "url": receiver.url(&format!("u{n}")) }),
        )
        .await;
    }
    instance.stop().await;
    // The three oldest endpoints hold a secret no key opens.
    let originals = world
        .text(
            "SELECT id::text || ' ' || encode(secret_ct, 'hex') FROM merezha_notify.webhook_endpoints \
             ORDER BY id LIMIT 3",
        )
        .await;
    world
        .execute(
            "UPDATE merezha_notify.webhook_endpoints SET secret_ct = decode(repeat('ab', 48), 'hex') \
             WHERE id IN (SELECT id FROM merezha_notify.webhook_endpoints ORDER BY id LIMIT 3)",
        )
        .await;
    let instance = start_with(&world, &rotated_section(true), |o| {
        o.env.push(("NEW_KEY".into(), NEW_KEY.into()));
        o.meters = Some(harness.telemetry().meters());
    })
    .await;
    let core = instance.core();
    let bounds = webhooks::tasks::ResealBounds {
        batch: 2,
        batches: 1,
    };
    let mut runs = Vec::new();
    for _ in 0..3 {
        let run = webhooks::tasks::reseal(&core, soon(), bounds)
            .await
            .unwrap();
        runs.push((run.resealed, run.remaining, run.unreadable));
    }
    assert_eq!(runs, [(0, 3, 2), (1, 1, 3), (1, 0, 3)]);
    let gauge = |name: &str| harness.metric(name).value(&[]);
    assert_eq!(gauge("merezha.webhook.secrets.previous_key"), Some(0.0));
    assert_eq!(gauge("merezha.webhook.secrets.unreadable"), Some(3.0));
    // A row repaired in place (its ciphertext changed) is tried again.
    let (repaired, ciphertext) = originals[0].split_once(' ').unwrap();
    world
        .execute(&format!(
            "UPDATE merezha_notify.webhook_endpoints SET secret_ct = decode('{ciphertext}', 'hex') \
             WHERE id = '{repaired}'"
        ))
        .await;
    let again = webhooks::tasks::reseal(&core, soon(), bounds)
        .await
        .unwrap();
    assert_eq!(
        (again.resealed, again.remaining, again.unreadable),
        (1, 0, 2)
    );
    // Rotating an unreadable endpoint's secret clears its mark.
    let id = world
        .text(
            "SELECT id::text FROM merezha_notify.webhook_endpoints \
             WHERE unreadable_under IS NOT NULL ORDER BY id LIMIT 1",
        )
        .await
        .remove(0);
    let (status, _) = instance
        .call(
            "POST",
            &format!("/v1/notifications/tenants/org_a/webhooks/{id}/rotate-secret"),
            Some("ada"),
            Some(json!({ "keep_previous_hours": 0 })),
        )
        .await;
    assert_eq!(status, 200);
    let last = webhooks::tasks::reseal(&core, soon(), bounds)
        .await
        .unwrap();
    assert_eq!((last.remaining, last.unreadable), (0, 1));
    instance.stop().await;
}

/// A hand-off replayed after a lost completion dedupes: one follow-up job.
#[tokio::test]
async fn a_replayed_hand_off_starts_no_second_chain() {
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
    let (_, endpoint) = create(
        &instance,
        "ada",
        "/tenants/org_a",
        json!({ "url": receiver.url("a") }),
    )
    .await;
    let endpoint = uuid::Uuid::parse_str(endpoint["id"].as_str().unwrap()).unwrap();
    let core = instance.core();
    let event = fanout::Event {
        id: uuid::Uuid::from_u128(11),
        source: "application".into(),
        source_event_id: "replay".into(),
        tenant: Some("org_a".into()),
        event_type: "tasks.task_completed".into(),
        occurred_at: jiff::Timestamp::now(),
        data: json!({}),
    };
    fanout::fan_out(&core, event, id("corr"), soon()).await;
    let delivery = webhooks::delivery_id(uuid::Uuid::from_u128(11), endpoint);
    let sender = core.webhooks.clone().unwrap();
    let mut held = Vec::new();
    while let Some(permit) = sender.occupy(endpoint) {
        held.push(permit);
    }
    let slot = Arc::new(std::sync::OnceLock::new());
    let _ = slot.set(core.clone());
    let handler = webhooks::worker::Handler(slot);
    let job = webhooks::DeliverJob {
        webhook_delivery_id: delivery,
        endpoint_id: endpoint,
        generation: 0,
        busy: 0,
        busy_since: None,
        busy_step: None,
    };
    for _ in 0..2 {
        let (_cancel, cancelled) = tokio::sync::watch::channel(false);
        let ctx = merezha_platform::ports::JobContext {
            id: 1,
            kind: id("notify.webhook.deliver"),
            attempt: 1,
            max_attempts: 10,
            idempotency_key: "k".into(),
            deadline: tokio::time::Instant::now() + Duration::from_secs(30),
            cancel: cancelled,
            correlation_id: id("corr"),
            causation_id: None,
            tenant: Some(id("org_a")),
        };
        use merezha_platform::ports::JobHandler;
        handler.handle(ctx, job.to_value()).await.unwrap();
    }
    assert_eq!(
        world
            .scalar(&format!(
                "SELECT count(*) FROM merezha_jobs.outbox WHERE dedupe_key = 'webhook:{delivery}:g0:s0:b1'"
            ))
            .await,
        1
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_jobs.outbox WHERE dedupe_key LIKE 'webhook:%:b%'")
            .await,
        1
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.webhook_attempts")
            .await,
        0
    );
    drop(held);
}

/// A fresh instance whose endpoints are still on the previous key reports
/// their count from the start, never a 0 nobody measured, and the status
/// route answers from the database.
#[tokio::test]
async fn a_fresh_instance_reports_the_secrets_still_on_the_previous_key() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = start(&world, &section(true, "")).await;
    for n in 0..2 {
        create(
            &instance,
            "ada",
            "/tenants/org_a",
            json!({ "url": receiver.url(&format!("p{n}")) }),
        )
        .await;
    }
    instance.stop().await;
    let instance = start_with(&world, &rotated_section(true), |o| {
        o.env.push(("NEW_KEY".into(), NEW_KEY.into()));
        o.meters = Some(harness.telemetry().meters());
    })
    .await;
    instance
        .tokens
        .add("root", user("usr_root", &["notifications.admin"], None));
    let gauge = |name: &str| harness.metric(name).value(&[]);
    assert_eq!(gauge("merezha.webhook.secrets.previous_key"), Some(2.0));
    assert_eq!(gauge("merezha.webhook.secrets.unreadable"), Some(0.0));
    let status_of = |token: &'static str| {
        let instance = &instance;
        async move {
            instance
                .call(
                    "GET",
                    "/v1/notifications/webhook-secrets/status",
                    Some(token),
                    None,
                )
                .await
        }
    };
    let (status, body) = status_of("root").await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        (
            body["previous_key"].clone(),
            body["unreadable"].clone(),
            body["previous_key_configured"].clone(),
            body["previous_key_removable"].clone()
        ),
        (json!(2), json!(0), json!(true), json!(false))
    );
    assert_eq!(body["current_key_id"].as_str().unwrap().len(), 16);
    assert_eq!(status_of("ada").await.0, 403);
    maintenance::run(&instance.core(), Task::Retention, soon())
        .await
        .unwrap();
    let (_, body) = status_of("root").await;
    assert_eq!(
        (
            body["previous_key"].clone(),
            body["previous_key_removable"].clone()
        ),
        (json!(0), json!(true))
    );
    instance.stop().await;
}

/// Before any count the gauges report nothing rather than 0.
#[test]
fn the_secret_gauges_report_nothing_before_a_count() {
    let harness = merezha_telemetry::testing::Harness::new();
    let meter = harness
        .telemetry()
        .meters()
        .meter(merezha_notifications::METER_SCOPE);
    let metrics = merezha_notifications::metrics::Metrics::new(&meter, Vec::new());
    let read = || {
        harness
            .metric("merezha.webhook.secrets.previous_key")
            .value(&[])
    };
    assert_eq!(read(), None);
    metrics.webhook_secrets(4, 1);
    assert_eq!(read(), Some(4.0));
}

/// A busy endpoint's `endpoint_busy` attempts do not count toward
/// auto-disable, even while a real failure set `failing_since`.
#[tokio::test]
async fn busy_attempts_do_not_count_toward_auto_disable() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    receiver.fail_next(1, 503, "");
    let clock = Arc::new(Shifted::default());
    let instance = start_with(&world, &section(true, ""), |o| {
        o.clock = Some(clock.clone());
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
        "b1",
    )
    .await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.webhook_deliveries WHERE status = 'retrying'",
            1,
        )
        .await;
    world
        .execute(
            "INSERT INTO merezha_notify.webhook_attempts (delivery_id, attempt, generation, \
             started_at, finished_at, outcome, error_code) \
             SELECT d.id, 100 + n, 0, now(), now() + interval '1 second', 'failed', 'endpoint_busy' \
             FROM merezha_notify.webhook_deliveries d, generate_series(1, 10) AS n",
        )
        .await;
    clock.advance(Duration::from_secs(6 * 86_400));
    let disabled = maintenance::run(&instance.core(), Task::WebhookAutoDisable, soon())
        .await
        .unwrap();
    assert_eq!(disabled, 0);
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

    /// The review's proof of concept: one slot per endpoint, eight queue
    /// workers, two job attempts and a receiver answering in 900 ms. A
    /// busy job hands its delivery to a later job instead of waiting or
    /// spending an attempt, so all eight deliveries succeed, one request
    /// at a time, and no job dies.
    #[tokio::test]
    async fn a_busy_endpoint_reschedules_without_spending_attempts() {
        let Some(world) = World::new().await else {
            return;
        };
        let receiver = WebhookReceiver::start().await;
        receiver.delay_all(Duration::from_millis(900));
        let config = section(true, "concurrency = 8\nmax_attempts = 2\n").replace(
            "allow_http = true\n",
            "allow_http = true\nconcurrency_per_endpoint = 1\n",
        );
        let instance = start(&world, &config).await;
        create(
            &instance,
            "ada",
            "/tenants/org_a",
            json!({ "url": receiver.url("slow") }),
        )
        .await;
        for n in 0..8 {
            emit(
                &instance,
                "application",
                Some("org_a"),
                "tasks.task_completed",
                &format!("b{n}"),
            )
            .await;
        }
        instance
            .until(
                "SELECT count(*) FROM merezha_notify.webhook_deliveries WHERE status = 'succeeded'",
                8,
            )
            .await;
        assert_eq!(
            world
                .scalar("SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.webhook.deliver' AND state = 'dead'")
                .await,
            0
        );
        assert_eq!(
            world
                .scalar("SELECT count(*) FROM merezha_notify.webhook_attempts")
                .await,
            8
        );
        let mut arrivals: Vec<Instant> = receiver.received().iter().map(|r| r.at).collect();
        arrivals.sort();
        for pair in arrivals.windows(2) {
            assert!(
                pair[1].duration_since(pair[0]) >= Duration::from_millis(850),
                "two requests overlapped on a one-slot endpoint"
            );
        }
        instance.stop().await;
    }

    /// A chain of hand-offs is bounded: past `BUSY_WINDOW` on the block's
    /// clock (injected), the busy endpoint costs the delivery a
    /// `failed` / `endpoint_busy` attempt on the schedule, which moves to
    /// its next step; `failing_since` is left alone.
    #[tokio::test]
    async fn a_busy_chain_past_its_window_costs_an_attempt_on_the_schedule() {
        let Some(world) = World::new().await else {
            return;
        };
        let receiver = WebhookReceiver::start().await;
        let clock = Arc::new(Shifted::default());
        let config = section(true, "").replace(
            "allow_http = true\n",
            "allow_http = true\nconcurrency_per_endpoint = 1\n",
        );
        let instance = start_with(&world, &config, |o| {
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
        let endpoint = uuid::Uuid::parse_str(endpoint["id"].as_str().unwrap()).unwrap();
        let sender = instance.core().webhooks.clone().unwrap();
        let held = sender.occupy(endpoint).unwrap();
        emit(
            &instance,
            "application",
            Some("org_a"),
            "tasks.task_completed",
            "w1",
        )
        .await;
        // The first job hands off.
        instance
            .until(
                "SELECT count(*) FROM merezha_jobs.outbox WHERE dedupe_key LIKE 'webhook:%:b1'",
                1,
            )
            .await;
        assert_eq!(
            world
                .scalar("SELECT count(*) FROM merezha_notify.webhook_attempts")
                .await,
            0
        );
        clock.advance(webhooks::worker::BUSY_WINDOW + Duration::from_secs(1));
        instance
            .until(
                "SELECT count(*) FROM merezha_notify.webhook_attempts WHERE error_code = 'endpoint_busy'",
                1,
            )
            .await;
        assert_eq!(deliveries(&world).await, ["retrying:endpoint_busy"]);
        assert_eq!(
            world
                .scalar("SELECT count(*) FROM merezha_notify.webhook_endpoints WHERE failing_since IS NULL")
                .await,
            1
        );
        // The ladder retry runs the same payload: with an attempt recorded
        // since, its budget starts again, so it hands off rather than
        // spending another attempt at once.
        run_now(&world).await;
        instance
            .until(
                "SELECT count(*) FROM merezha_jobs.outbox WHERE dedupe_key LIKE 'webhook:%:s1:b1'",
                1,
            )
            .await;
        // Due on the database's clock (the block's runs 5 minutes ahead).
        assert_eq!(
            world
                .scalar(
                    "SELECT count(*) FROM merezha_jobs.outbox WHERE dedupe_key LIKE 'webhook:%:s1:b1' \
                     AND run_at < now() + interval '1 minute'"
                )
                .await,
            1
        );
        assert_eq!(
            world
                .scalar("SELECT count(*) FROM merezha_notify.webhook_attempts")
                .await,
            1
        );
        drop(held);
        run_now(&world).await;
        instance
            .until(
                "SELECT count(*) FROM merezha_notify.webhook_deliveries WHERE status = 'succeeded'",
                1,
            )
            .await;
        assert_eq!(attempts(&world).await, ["failed:", "succeeded:204"]);
        instance.stop().await;
    }

    /// The Tx B fence: a run that loses its lease while its request is in
    /// flight is taken over; the takeover's outcome stands, and the late
    /// run records nothing (not its 503, not `failing_since`).
    #[tokio::test]
    async fn a_late_run_records_nothing_after_a_takeover() {
        let Some(world) = World::new().await else {
            return;
        };
        let receiver = WebhookReceiver::start().await;
        receiver.fail_next(1, 503, "");
        let instance = start_with(&world, &crashing(), |o| {
            o.heartbeat = false;
            o.faults
                .pause_once(faults::AFTER_PROVIDER_CALL, Duration::from_millis(4500));
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
            "f1",
        )
        .await;
        instance
            .until(
                "SELECT count(*) FROM merezha_notify.webhook_deliveries WHERE status = 'succeeded'",
                1,
            )
            .await;
        // Past the paused run's resumption and its Tx B.
        tokio::time::sleep(Duration::from_millis(3000)).await;
        assert_eq!(attempts(&world).await, ["ambiguous:", "succeeded:204"]);
        assert_eq!(deliveries(&world).await, ["succeeded:"]);
        assert_eq!(
            world
                .scalar("SELECT count(*) FROM merezha_notify.webhook_endpoints WHERE failing_since IS NULL")
                .await,
            1
        );
        instance.stop().await;
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
