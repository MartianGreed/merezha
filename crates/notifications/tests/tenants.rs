//! Deleted organizations (issue #63) against real PostgreSQL: the block
//! consumes `identity.organization_deleted`, refuses every write of the
//! tenant from the mark on, and purges its webhook endpoints and events,
//! push subscriptions, inbox items and notifications with their
//! deliveries, leaving other tenants alone.
mod harness;
use harness::*;
use merezha_domain::Failure;
use merezha_notifications::{
    push,
    testkit::{FakePushService, WebhookReceiver},
};
use merezha_platform::ports::{
    ChannelSelection, Correlation, DedupeKey, GrantSnapshot, Grants, MessageContext, NewEvent,
    NotificationRequest, OnDuplicate, PermissionSet, Principal, PrincipalKind, Recipient, Source,
    SourceOutbox,
};
use serde_json::{Map, json};
use std::{sync::Arc, time::Duration};

const MANAGE: &str = "notifications.webhooks.manage";

/// Webhooks and push on, plain HTTP and loopback allowed (development).
fn section() -> String {
    format!(
        "[jobs.queues.\"notify.webhook\"]\nbackoff_max_ms = 86400000\n\
         {SECTION}egress = {{ allow_private_networks = true }}\n\
         [modules.notifications.webhooks]\nallow_http = true\n\
         [modules.notifications.push]\nvapid_private_key = {{ env = \"VAPID_KEY\" }}\n\
         vapid_subject = \"mailto:ops@example.com\"\nallowed_hosts = [\"127.0.0.1\"]\n"
    )
}

/// A user acting in `tenant`, with `permissions` there.
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

async fn start(world: &World, edit: impl FnOnce(&mut Options)) -> Instance {
    let people = Arc::new(People::default());
    people.user("usr_ada", "ada@example.com", Some("Ada"), None);
    people.user("usr_bea", "bea@example.com", Some("Bea"), None);
    people.join("org_a", "usr_ada", &["owner"]);
    people.join("org_b", "usr_bea", &["owner"]);
    let mut options = Options::new(&section(), people);
    let keys = push::generate().unwrap();
    options.env.push(("VAPID_KEY".into(), keys.private_key));
    edit(&mut options);
    let instance = Instance::start(world, options).await;
    instance
        .tokens
        .add("ada", member("usr_ada", "org_a", &[MANAGE]));
    instance
        .tokens
        .add("bea", member("usr_bea", "org_b", &[MANAGE]));
    instance
}

/// `identity.password_changed` (email and inbox) for `user` in `tenant`,
/// as identity requests it.
async fn notify(instance: &Instance, user: &str, tenant: &str, key: &str) {
    notify_at(instance, user, tenant, key, None).await;
}
/// [`notify`] held back until `not_before`.
async fn notify_at(
    instance: &Instance,
    user: &str,
    tenant: &str,
    key: &str,
    not_before: Option<jiff::Timestamp>,
) {
    let request = NotificationRequest {
        topic: id("identity.password_changed"),
        recipients: vec![Recipient::User { id: id(user) }],
        channels: ChannelSelection::TopicDefault,
        template: None,
        data: json!({"display_name": "Ada"}).as_object().cloned().unwrap(),
        secret_data: Map::new(),
        locale: None,
        dedupe_key: DedupeKey::new(key).unwrap(),
        tenant: Some(id(tenant)),
        members: Vec::new(),
        not_before,
        expires_at: None,
        collapse_key: None,
        correlation: Correlation {
            correlation_id: id("corr1"),
            causation_id: None,
        },
    };
    let notifier = instance.notifier("identity");
    instance
        .db
        .transaction(soon(), move |tx| {
            Box::pin(async move { notifier.request(tx, request).await.map_err(Failure::from) })
        })
        .await
        .unwrap();
}

/// Identity's `identity.organization_deleted` for `organization`, as it
/// publishes it (an `Event` record from source `identity`).
async fn delete_organization(instance: &Instance, world: &World, organization: &str, key: &str) {
    let outbox = SourceOutbox::new(
        Source::new("identity").unwrap(),
        merezha_platform::JobsRuntime::ports(&instance.jobs, &world.db).outbox,
    );
    let record = outbox.event(
        id("identity.organization_deleted"),
        NewEvent {
            dedupe_key: DedupeKey::new(key).unwrap(),
            version: 1,
            occurred_at: jiff::Timestamp::now(),
            payload: json!({ "organization_id": organization, "deleted_at": "2026-10-27T00:00:00Z" }),
            context: MessageContext {
                correlation_id: id("corr-identity"),
                causation_id: None,
                tenant: Some(id(organization)),
            },
        },
    );
    world
        .db
        .transaction(soon(), move |tx| {
            Box::pin(async move {
                outbox.append(tx, &[record], OnDuplicate::Fail).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

/// Rows of `table` whose `column` is `tenant`.
async fn rows(world: &World, table: &str, tenant: &str) -> i64 {
    world
        .scalar(&format!(
            "SELECT count(*) FROM merezha_notify.{table} WHERE tenant_id = '{tenant}'"
        ))
        .await
}

/// Issue #63: once identity deletes organization A, the block purges A's
/// webhook endpoints and events, push subscriptions, inbox items and
/// notifications with their deliveries and provider events, and touches
/// nothing of B. From the mark on, nothing of A is written again: its
/// content events reach no endpoint, its notifications expand to nothing,
/// and its registry and push subscriptions refuse new rows. A redelivered
/// deletion purges nothing twice.
#[tokio::test]
async fn a_deleted_organization_is_purged_alone_and_refused_from_then_on() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let fake = FakePushService::start().await;
    let instance = start(&world, |o| o.meters = Some(harness.telemetry().meters())).await;
    for (token, tenant) in [("ada", "org_a"), ("bea", "org_b")] {
        let (status, body) = instance
            .call(
                "POST",
                &format!("/v1/notifications/tenants/{tenant}/webhooks"),
                Some(token),
                Some(json!({ "url": receiver.url(tenant) })),
            )
            .await;
        assert_eq!(status, 201, "{body}");
        let (status, body) = instance
            .call(
                "POST",
                "/v1/notifications/push/subscriptions",
                Some(token),
                Some(fake.subscription(tenant)),
            )
            .await;
        assert_eq!(status, 201, "{body}");
        content_event(
            &instance,
            &world,
            "content.entry_published",
            &format!("{tenant}:entry:1"),
            Some(tenant),
        )
        .await;
    }
    notify(&instance, "usr_ada", "org_a", "pw:a").await;
    notify(&instance, "usr_bea", "org_b", "pw:b").await;
    instance.settle().await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.webhook_deliveries WHERE status = 'succeeded'",
            2,
        )
        .await;
    world
        .execute(
            "INSERT INTO merezha_notify.provider_events (provider, channel, event, event_key, \
               delivery_id, received_at) \
             SELECT 'brevo', 'email', 'delivered', 'ev-' || d.id, d.id, now() \
             FROM merezha_notify.deliveries d WHERE d.channel = 'email'",
        )
        .await;
    for (table, count) in [
        ("notifications", 1),
        ("inbox_items", 1),
        ("push_subscriptions", 1),
        ("webhook_endpoints", 1),
        ("webhook_events", 1),
    ] {
        assert_eq!(rows(&world, table, "org_a").await, count, "{table}");
    }

    delete_organization(&instance, &world, "org_a", "organization:a:deleted").await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.tenants_deleted \
             WHERE tenant_id = 'org_a' AND purged_at IS NOT NULL",
            1,
        )
        .await;
    for table in [
        "notifications",
        "inbox_items",
        "push_subscriptions",
        "webhook_endpoints",
        "webhook_events",
    ] {
        assert_eq!(rows(&world, table, "org_a").await, 0, "{table} of A");
        assert_eq!(rows(&world, table, "org_b").await, 1, "{table} of B");
    }
    for orphan in [
        "SELECT count(*) FROM merezha_notify.deliveries d WHERE NOT EXISTS \
         (SELECT 1 FROM merezha_notify.notifications n WHERE n.id = d.notification_id)",
        "SELECT count(*) FROM merezha_notify.provider_events e WHERE NOT EXISTS \
         (SELECT 1 FROM merezha_notify.deliveries d WHERE d.id = e.delivery_id)",
        "SELECT count(*) FROM merezha_notify.webhook_deliveries d WHERE NOT EXISTS \
         (SELECT 1 FROM merezha_notify.webhook_endpoints e WHERE e.id = d.endpoint_id)",
    ] {
        assert_eq!(world.scalar(orphan).await, 0, "{orphan}");
    }
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.deliveries")
            .await,
        2,
        "B's email and inbox deliveries"
    );
    assert_eq!(
        world
            .text(
                "SELECT action || ':' || actor FROM merezha_notify.webhook_audit \
                 WHERE tenant_id = 'org_a' ORDER BY id"
            )
            .await,
        ["create:usr_ada", "delete:sys_notifications"]
    );
    assert!(
        world
            .scalar(
                "SELECT purged_through FROM merezha_notify.inbox_counters \
                 WHERE user_id = 'usr_ada'"
            )
            .await
            > 0,
        "a resume from before the purge resets"
    );

    // From the mark on, nothing of A is written.
    content_event(
        &instance,
        &world,
        "content.entry_published",
        "org_a:entry:2",
        Some("org_a"),
    )
    .await;
    notify(&instance, "usr_ada", "org_a", "pw:a2").await;
    instance.settle().await;
    assert_eq!(rows(&world, "webhook_events", "org_a").await, 0);
    assert_eq!(rows(&world, "notifications", "org_a").await, 0);
    let (status, body) = instance
        .call(
            "POST",
            "/v1/notifications/tenants/org_a/webhooks",
            Some("ada"),
            Some(json!({ "url": receiver.url("again") })),
        )
        .await;
    assert_eq!(
        (status.as_u16(), body["error"]["code"].clone()),
        (404, json!("not_found"))
    );
    let (status, _) = instance
        .call(
            "POST",
            "/v1/notifications/push/subscriptions",
            Some("ada"),
            Some(fake.subscription("again")),
        )
        .await;
    assert_eq!(status.as_u16(), 404);
    assert_eq!(rows(&world, "push_subscriptions", "org_a").await, 0);

    // A redelivered deletion marks and purges nothing twice.
    delete_organization(&instance, &world, "org_a", "organization:a:deleted-again").await;
    instance
        .until(
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.organization_deleted' \
             AND state = 'succeeded'",
            2,
        )
        .await;
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.organization_purge'"
            )
            .await,
        1
    );
    let recorded = harness.recorded();
    let count = |name: &str| {
        recorded
            .iter()
            .filter(|r| r.name.as_deref() == Some(name))
            .count()
    };
    assert_eq!(count("notification.organization_deleted"), 1);
    assert_eq!(count("notification.organization_purged"), 1);
    assert!(
        count("notification.rejected") >= 1,
        "the request of A was refused"
    );
    harness.assert_catalogued();
    instance.stop().await;
}

/// The mark waits for a writer of the tenant in flight (one holding the
/// tenant's shared lock), so that writer commits first and its rows are
/// purged after it; the mark then commits.
#[tokio::test]
async fn the_mark_waits_for_a_writer_in_flight() {
    let Some(world) = World::new().await else {
        return;
    };
    let instance = start(&world, |o| o.workers = false).await;
    let receiver = WebhookReceiver::start().await;
    let (status, endpoint) = instance
        .call(
            "POST",
            "/v1/notifications/tenants/org_a/webhooks",
            Some("ada"),
            Some(json!({ "url": receiver.url("a") })),
        )
        .await;
    assert_eq!(status, 201, "{endpoint}");
    let endpoint = endpoint["id"].as_str().unwrap().to_owned();
    let writer = world.db.acquire(soon()).await.unwrap();
    writer
        .batch_execute(&format!(
            "BEGIN; SELECT pg_advisory_xact_lock_shared({}, hashtext('org_a'));",
            merezha_notifications::sql::TENANT_LOCK_CLASS
        ))
        .await
        .unwrap();
    let core = instance.core();
    let mark = tokio::spawn(async move {
        merezha_notifications::tenants::mark_deleted(&core, "org_a", id("corr1"), soon()).await
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(!mark.is_finished(), "the mark waits on the writer");
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.tenants_deleted")
            .await,
        0
    );
    writer.batch_execute("COMMIT").await.unwrap();
    assert_eq!(mark.await.unwrap(), Ok(true));
    let core = instance.core();
    assert_eq!(
        merezha_notifications::tenants::mark_deleted(&core, "org_a", id("corr1"), soon()).await,
        Ok(false),
        "marked once"
    );
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_jobs.outbox WHERE job_kind = 'notify.organization_purge'"
            )
            .await,
        1,
        "one purge record"
    );
    // Marked, not purged yet (no worker runs): the endpoint is still there,
    // but its registry takes no test event and no new endpoint.
    let (status, _) = instance
        .call(
            "POST",
            &format!("/v1/notifications/tenants/org_a/webhooks/{endpoint}/test"),
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 404);
    let (status, _) = instance
        .call(
            "POST",
            "/v1/notifications/tenants/org_a/webhooks",
            Some("ada"),
            Some(json!({ "url": receiver.url("b") })),
        )
        .await;
    assert_eq!(status.as_u16(), 404);
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.webhook_events")
            .await,
        0
    );
    instance.stop().await;
}

/// Record the deletion of `tenant` as the mark does, without its purge job:
/// what the workers see between the mark and the purge reaching the rows.
async fn mark_only(world: &World, tenant: &str) {
    world
        .execute(&format!(
            "INSERT INTO merezha_notify.tenants_deleted (tenant_id, deleted_at, source) \
             VALUES ('{tenant}', now(), 'event')"
        ))
        .await;
}

/// Security review of #82 (Medium): a deleted tenant's queued deliveries
/// stop at the mark, not when the purge reaches them. A delivery job that
/// runs after the mark cancels its delivery `tenant_deleted` before any
/// attempt, and nothing is captured. From the mark on, the tenant's inbox
/// and stream answer 404, while a session without the tenant still reads
/// its own inbox.
#[tokio::test]
async fn a_queued_delivery_of_a_deleted_tenant_is_cancelled_at_its_next_run() {
    let Some(world) = World::new().await else {
        return;
    };
    // Delivery job records stay in the outbox until relayed by hand.
    let instance = start(&world, |o| o.relay = false).await;
    instance.tokens.add("ada_alone", user("usr_ada", &[], None));
    notify(&instance, "usr_ada", "org_a", "pw:queued").await;
    instance.relay_now().await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.deliveries \
             WHERE channel = 'email' AND status = 'queued'",
            1,
        )
        .await;
    let (status, _) = instance
        .call("GET", "/v1/notifications/inbox", Some("ada"), None)
        .await;
    assert_eq!(status, 200);
    mark_only(&world, "org_a").await;
    instance.relay_now().await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.deliveries \
             WHERE channel = 'email' AND status = 'cancelled' AND status_reason = 'tenant_deleted'",
            1,
        )
        .await;
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.delivery_attempts")
            .await,
        0
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.captures")
            .await,
        0,
        "nothing sent"
    );
    for path in [
        "/v1/notifications/inbox",
        "/v1/notifications/inbox/unread-count",
        "/v1/notifications/inbox/stream",
    ] {
        let (status, _) = instance.call("GET", path, Some("ada"), None).await;
        assert_eq!(status.as_u16(), 404, "{path}");
    }
    let (status, _) = instance
        .call(
            "POST",
            "/v1/notifications/inbox/read-all",
            Some("ada"),
            Some(json!({})),
        )
        .await;
    assert_eq!(status.as_u16(), 404);
    let (status, body) = instance
        .call("GET", "/v1/notifications/inbox", Some("ada_alone"), None)
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        body["items"],
        json!([]),
        "the tenant's items are out of scope"
    );
    instance.stop().await;
}

/// The review's proof of concept: a notification of the tenant held back
/// by `not_before`, the tenant deleted while it waits. Nothing is sent,
/// captured or served.
#[tokio::test]
async fn a_notification_waiting_at_the_mark_sends_nothing() {
    let Some(world) = World::new().await else {
        return;
    };
    let instance = start(&world, |_| {}).await;
    let later = jiff::Timestamp::now() + jiff::SignedDuration::from_secs(3);
    notify_at(&instance, "usr_ada", "org_a", "pw:later", Some(later)).await;
    mark_only(&world, "org_a").await;
    tokio::time::sleep(Duration::from_secs(4)).await;
    instance.settle().await;
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.captures")
            .await,
        0
    );
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.deliveries \
                 WHERE status IN ('accepted', 'delivered')"
            )
            .await,
        0
    );
    let (status, _) = instance
        .call("GET", "/v1/notifications/inbox", Some("ada"), None)
        .await;
    assert_eq!(status.as_u16(), 404);
    instance.stop().await;
}

/// Security review of #82: a deleted tenant's webhook delivery still being
/// retried is cancelled `tenant_deleted` at its next attempt, and the
/// endpoint gets no further request.
#[tokio::test]
async fn a_retried_webhook_of_a_deleted_tenant_is_cancelled() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    receiver.fail_next(1, 500, "down");
    let instance = start(&world, |_| {}).await;
    let (status, _) = instance
        .call(
            "POST",
            "/v1/notifications/tenants/org_a/webhooks",
            Some("ada"),
            Some(json!({ "url": receiver.url("a") })),
        )
        .await;
    assert_eq!(status, 201);
    content_event(
        &instance,
        &world,
        "content.entry_published",
        "org_a:entry:retry",
        Some("org_a"),
    )
    .await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.webhook_deliveries WHERE status = 'retrying'",
            1,
        )
        .await;
    assert_eq!(receiver.received().len(), 1);
    mark_only(&world, "org_a").await;
    world
        .execute(
            "UPDATE merezha_jobs.jobs SET run_at = now() \
             WHERE kind = 'notify.webhook.deliver' AND state = 'available'",
        )
        .await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.webhook_deliveries \
             WHERE status = 'cancelled' AND last_error_code = 'tenant_deleted'",
            1,
        )
        .await;
    assert_eq!(receiver.received().len(), 1, "no request after the mark");
    instance.stop().await;
}

/// Security review of #82 (write fence): from the mark on, the tenant's
/// endpoints take no update, rotation, enable or redelivery, and its
/// notifications no redelivery (404); a purge of a tenant never marked
/// touches nothing.
#[tokio::test]
async fn a_marked_tenant_takes_no_change_or_redelivery() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = start(&world, |o| o.workers = false).await;
    instance
        .tokens
        .add("admin", user("usr_admin", &["notifications.admin"], None));
    let (status, endpoint) = instance
        .call(
            "POST",
            "/v1/notifications/tenants/org_a/webhooks",
            Some("ada"),
            Some(json!({ "url": receiver.url("a") })),
        )
        .await;
    assert_eq!(status, 201);
    let endpoint = endpoint["id"].as_str().unwrap().to_owned();
    let core = instance.core();
    // Never marked: the purge refuses and deletes nothing.
    assert_eq!(
        merezha_notifications::tenants::purge_step(
            &core.database,
            "org_a",
            jiff::Timestamp::now(),
            soon()
        )
        .await,
        Ok(merezha_notifications::tenants::PurgeStep::NotDeleted)
    );
    assert_eq!(rows(&world, "webhook_endpoints", "org_a").await, 1);
    // A failed delivery of the tenant and a failed webhook delivery.
    world
        .execute(&format!(
            "INSERT INTO merezha_notify.notifications (id, source, dedupe_key, topic, template, \
               tenant_id, category, priority, data, channels, status, target_count, requested_at, \
               expanded_at, settled_at) VALUES ('00000000-0000-0000-0000-0000000000a1', 'api', \
               'k:a', 'identity.password_changed', 'identity.password_changed', 'org_a', \
               'security', 'normal', '{{}}', '{{email}}', 'expanded', 1, now(), now(), now());
             INSERT INTO merezha_notify.deliveries (id, notification_id, channel, target_key, \
               address, locale, status, status_reason, run_at, created_at, updated_at, \
               settled_at) VALUES ('00000000-0000-0000-0000-0000000000b1', \
               '00000000-0000-0000-0000-0000000000a1', 'email', 'email:ada@example.com', \
               'ada@example.com', 'en', 'failed', 'blocked', now(), now(), now(), now());
             INSERT INTO merezha_notify.webhook_events (id, source, source_event_id, tenant_id, \
               event_type, occurred_at, body, created_at) VALUES \
               ('00000000-0000-0000-0000-0000000000c1', 'content', 'e1', 'org_a', \
                'content.entry_published', now(), '\\x7b7d', now());
             INSERT INTO merezha_notify.webhook_deliveries (id, event_id, endpoint_id, status, \
               created_at, updated_at, settled_at) VALUES \
               ('00000000-0000-0000-0000-0000000000d1', '00000000-0000-0000-0000-0000000000c1', \
                '{endpoint}', 'failed', now(), now(), now());"
        ))
        .await;
    mark_only(&world, "org_a").await;
    let base = format!("/v1/notifications/tenants/org_a/webhooks/{endpoint}");
    for (method, path, body) in [
        (
            "PATCH",
            base.clone(),
            Some(json!({ "description": "x", "revision": 1 })),
        ),
        ("POST", format!("{base}/rotate-secret"), None),
        ("POST", format!("{base}/disable"), None),
        ("POST", format!("{base}/enable"), None),
        (
            "POST",
            format!("{base}/deliveries/00000000-0000-0000-0000-0000000000d1/redeliver"),
            None,
        ),
    ] {
        let (status, _) = instance.call(method, &path, Some("ada"), body).await;
        assert_eq!(status.as_u16(), 404, "{method} {path}");
    }
    let (status, _) = instance
        .call(
            "POST",
            "/v1/notifications/deliveries/00000000-0000-0000-0000-0000000000b1/redeliver",
            Some("admin"),
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 404);
    assert_eq!(
        world
            .text(
                "SELECT status || ':' || generation FROM merezha_notify.deliveries \
                 UNION ALL SELECT status || ':' || generation \
                 FROM merezha_notify.webhook_deliveries"
            )
            .await,
        ["failed:0", "failed:0"]
    );
    instance.stop().await;
}
