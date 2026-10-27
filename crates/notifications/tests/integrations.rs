//! NT-N8 against real PostgreSQL: the per-target rate cap
//! (`max_per_target_per_hour`), content's events reaching webhooks through
//! the AP subscription (`notify.module_event`) without content calling the
//! `Notifier`, identity's token mail keyed by its token, and the
//! `retention` task's delivery, provider event and capture part. Each test
//! creates its own database through `MEREZHA_TEST_DATABASE_URL` and skips
//! with a message when it is unset.
mod harness;
use harness::*;
use merezha_domain::{Failure, Identity};
use merezha_notifications::{
    maintenance::{self, Task},
    testkit::WebhookReceiver,
    webhooks::fanout::{self, MODULE_EVENT_KIND, SUBSCRIBED_EVENT_TYPES},
};
use merezha_platform::ports::{
    Address, Channel, ChannelSelection, Correlation, DedupeKey, EmailAddress, GrantSnapshot,
    Grants, IntegrationEvent, NotificationRequest, PermissionSet, Principal, PrincipalKind,
    Recipient,
};
use serde_json::{Map, Value, json};
use std::sync::Arc;

const MANAGE: &str = "notifications.webhooks.manage";

fn people() -> Arc<People> {
    let people = Arc::new(People::default());
    people.user("usr_ada", "ada@example.com", Some("Ada"), None);
    people.user("usr_bob", "bob@example.com", Some("Bob"), None);
    people.join("org_a", "usr_ada", &["owner"]);
    people
}

/// `tasks.assigned` for `user` (email and inbox) under `key`.
fn assigned(user: &str, key: &str) -> NotificationRequest {
    request("tasks.assigned", user, key)
}
/// `identity.password_changed` (security, email and inbox), which needs
/// `display_name`.
fn password_changed(user: &str, key: &str) -> NotificationRequest {
    let mut request = request("identity.password_changed", user, key);
    request.data = json!({"display_name": "Ada"}).as_object().cloned().unwrap();
    request
}
fn request(topic: &str, user: &str, key: &str) -> NotificationRequest {
    NotificationRequest {
        topic: id(topic),
        recipients: vec![Recipient::User { id: id(user) }],
        channels: ChannelSelection::Only(vec![Channel::Email, Channel::Inbox]),
        template: None,
        data: json!({"title": "Write docs"}).as_object().cloned().unwrap(),
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

/// Commit `requests` from the HTTP send source (no tenant, so no
/// membership check) in one transaction.
async fn commit(instance: &Instance, requests: Vec<NotificationRequest>) {
    let notifier = instance.notifier("api");
    instance
        .db
        .transaction(soon(), move |tx| {
            Box::pin(async move {
                for request in requests {
                    notifier.request(tx, request).await.map_err(Failure::from)?;
                }
                Ok(())
            })
        })
        .await
        .unwrap();
}

/// `channel:status:reason` of each delivery of the notification keyed
/// `key`, by channel.
async fn of(world: &World, key: &str) -> Vec<String> {
    world
        .text(&format!(
            "SELECT d.channel || ':' || d.status || ':' || coalesce(d.status_reason, '') \
             FROM merezha_notify.deliveries d \
             JOIN merezha_notify.notifications n ON n.id = d.notification_id \
             WHERE n.dedupe_key = '{key}' ORDER BY d.channel"
        ))
        .await
}

/// Two transactional project topics capped at two per target and hour
/// (NT §6), and an uncapped one; security topics refuse a cap.
const CAPPED: &str = "[modules.notifications.topics.\"tasks.assigned\"]\n\
    category = \"transactional\"\nchannels = [\"inbox\", \"email\"]\n\
    max_per_target_per_hour = 2\n\
    [modules.notifications.topics.\"tasks.reminded\"]\n\
    category = \"transactional\"\nchannels = [\"inbox\", \"email\"]\n\
    max_per_target_per_hour = 2\n\
    [modules.notifications.topics.\"tasks.digest\"]\n\
    category = \"transactional\"\nchannels = [\"inbox\", \"email\"]\n";

/// The capped section with the topics' templates.
fn capped(prefix: &str) -> Options {
    let mut options = Options::new(&format!("{prefix}{SECTION}{CAPPED}"), people());
    for topic in ["tasks.assigned", "tasks.reminded", "tasks.digest"] {
        let base = format!("notifications/templates/{topic}");
        options.files.extend([
            (
                format!("{base}/template.toml"),
                "description = \"Tasks\"\n[preview]\ntitle = \"T\"\n".to_owned(),
            ),
            (
                format!("{base}/en/subject.txt"),
                "Task: {{ title }}".to_owned(),
            ),
            (format!("{base}/en/body.txt"), "{{ title }}".to_owned()),
            (
                format!("{base}/en/inbox.toml"),
                "title = \"{{ title }}\"\nbody = \"Task\"\n".to_owned(),
            ),
        ]);
    }
    options
}

/// NT-N8 `max_per_target_per_hour`: past two deliveries of the topic to one
/// target within the hour, the next is `skipped` / `target_rate_limited`
/// on every channel, and settled at once; another target, another topic,
/// and the same target an hour later are not held back.
#[tokio::test]
async fn deliveries_past_the_per_target_cap_are_skipped() {
    let Some(world) = World::new().await else {
        return;
    };
    let instance = Instance::start(&world, capped("")).await;
    for key in ["k1", "k2", "k3"] {
        commit(&instance, vec![assigned("usr_ada", key)]).await;
        instance.settle().await;
    }
    assert_eq!(
        of(&world, "k1").await,
        ["email:accepted:", "inbox:delivered:"]
    );
    assert_eq!(
        of(&world, "k2").await,
        ["email:accepted:", "inbox:delivered:"]
    );
    assert_eq!(
        of(&world, "k3").await,
        [
            "email:skipped:target_rate_limited",
            "inbox:skipped:target_rate_limited"
        ]
    );
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.notifications \
                 WHERE dedupe_key = 'k3' AND settled_at IS NOT NULL"
            )
            .await,
        1,
        "nothing left to send"
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.inbox_items WHERE user_id = 'usr_ada'")
            .await,
        2,
        "no inbox item past the cap"
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.captures WHERE target = 'ada@example.com'")
            .await,
        2,
        "no mail past the cap"
    );
    // Another target, an uncapped topic and another capped topic on the
    // same target are not held back: the cap counts per topic.
    commit(&instance, vec![assigned("usr_bob", "b1")]).await;
    commit(&instance, vec![request("tasks.digest", "usr_ada", "s1")]).await;
    for key in ["r1", "r2"] {
        commit(&instance, vec![request("tasks.reminded", "usr_ada", key)]).await;
        instance.settle().await;
    }
    instance.settle().await;
    assert_eq!(
        of(&world, "b1").await,
        ["email:accepted:", "inbox:delivered:"]
    );
    for key in ["s1", "r1", "r2"] {
        assert_eq!(
            of(&world, key).await,
            ["email:accepted:", "inbox:delivered:"],
            "{key}"
        );
    }
    // An hour later the target is free again.
    world
        .execute(
            "UPDATE merezha_notify.deliveries SET created_at = created_at - interval '61 minutes'",
        )
        .await;
    commit(&instance, vec![assigned("usr_ada", "k4")]).await;
    instance.settle().await;
    assert_eq!(
        of(&world, "k4").await,
        ["email:accepted:", "inbox:delivered:"]
    );
    instance.stop().await;
}

/// The cap holds when the expansions of one target run at the same time:
/// each counts under the target's lock, so exactly two of eight requests
/// committed together are sent.
#[tokio::test]
async fn concurrent_expansions_respect_the_per_target_cap() {
    let Some(world) = World::new().await else {
        return;
    };
    let instance = Instance::start(
        &world,
        capped("[jobs.queues.\"notify.expand\"]\nconcurrency = 8\n"),
    )
    .await;
    let requests: Vec<NotificationRequest> = (0..8)
        .map(|n| assigned("usr_ada", &format!("c{n}")))
        .collect();
    commit(&instance, requests).await;
    instance.settle().await;
    for (channel, sent) in [("email", "accepted"), ("inbox", "delivered")] {
        assert_eq!(
            world
                .scalar(&format!(
                    "SELECT count(*) FROM merezha_notify.deliveries \
                     WHERE channel = '{channel}' AND status = '{sent}'"
                ))
                .await,
            2,
            "{channel}"
        );
        assert_eq!(
            world
                .scalar(&format!(
                    "SELECT count(*) FROM merezha_notify.deliveries WHERE channel = '{channel}' \
                     AND status = 'skipped' AND status_reason = 'target_rate_limited'"
                ))
                .await,
            6,
            "{channel}"
        );
    }
    instance.stop().await;
}

/// Webhooks on, plain HTTP to loopback allowed (development).
fn webhooks_section() -> String {
    format!(
        "[jobs.queues.\"notify.webhook\"]\nbackoff_max_ms = 86400000\n\
         {SECTION}egress = {{ allow_private_networks = true }}\n\
         [modules.notifications.webhooks]\nallow_http = true\n"
    )
}

/// A user holding the webhook permissions in `tenant`, or at application
/// scope without one.
fn manager(subject: &str, tenant: Option<&str>) -> Principal {
    let set: PermissionSet = [id(MANAGE)].into_iter().collect();
    let grants = Grants::new(Vec::new(), set);
    Principal {
        subject: id(subject),
        kind: PrincipalKind::User,
        user: Some(id(subject)),
        service_account: None,
        tenant: tenant.map(id),
        grants: Arc::new(match tenant {
            Some(_) => GrantSnapshot::empty().with_tenant(grants),
            None => GrantSnapshot::empty().with_application(grants),
        }),
        session: None,
    }
}

/// Register an endpoint for `name` on `prefix` (`/tenants/<id>` or the
/// application registry); its signing secret.
async fn endpoint(
    instance: &Instance,
    token: &str,
    prefix: &str,
    receiver: &WebhookReceiver,
    name: &str,
) -> String {
    let (status, body) = instance
        .call(
            "POST",
            &format!("/v1/notifications{prefix}/webhooks"),
            Some(token),
            Some(json!({ "url": receiver.url(name), "event_types": ["content.*"] })),
        )
        .await;
    assert_eq!(status.as_u16(), 201, "{body}");
    body["secret"].as_str().unwrap().to_owned()
}

/// NT-N8 acceptance: publishing an entry delivers `content.entry_published`
/// without content calling the `Notifier`. Content's `Event` records reach
/// the block through its `notify.module_event` subscription, which maps the
/// envelope onto a webhook event of source `content`: a tenant's entry
/// reaches that tenant's endpoints, an application entry the
/// application-level ones, signed over `{type, timestamp, data}`; no
/// `notify.webhook_event` job exists, and a second run of the subscription
/// job gives no second delivery.
#[tokio::test]
async fn content_events_reach_webhooks_through_the_subscription() {
    let Some(world) = World::new().await else {
        return;
    };
    let receiver = WebhookReceiver::start().await;
    let instance = Instance::start(&world, Options::new(&webhooks_section(), people())).await;
    instance
        .tokens
        .add("ada", manager("usr_ada", Some("org_a")));
    instance.tokens.add("ops", manager("usr_ops", None));
    let tenant_secret = endpoint(&instance, "ada", "/tenants/org_a", &receiver, "org_a").await;
    let app_secret = endpoint(&instance, "ops", "", &receiver, "app").await;
    receiver.trust(&tenant_secret);
    receiver.trust(&app_secret);
    assert_eq!(
        world
            .text(
                "SELECT topic FROM merezha_jobs.subscriptions \
                 WHERE job_kind = 'notify.module_event' AND deactivated_at IS NULL ORDER BY topic"
            )
            .await,
        {
            let mut types = SUBSCRIBED_EVENT_TYPES.map(str::to_owned).to_vec();
            types.sort();
            types
        }
    );
    content_event(
        &instance,
        &world,
        "content.entry_published",
        "e1",
        Some("org_a"),
    )
    .await;
    content_event(&instance, &world, "content.entry_published", "e2", None).await;
    instance.settle().await;
    let mut received = receiver.received();
    received.sort_by(|a, b| a.name.cmp(&b.name));
    let seen: Vec<(String, Value, Value)> = received
        .iter()
        .map(|r| {
            let body = r.json();
            (
                r.name.clone(),
                body["type"].clone(),
                body["data"]["id"].clone(),
            )
        })
        .collect();
    assert_eq!(
        seen,
        [
            (
                "app".to_owned(),
                json!("content.entry_published"),
                json!("e2")
            ),
            (
                "org_a".to_owned(),
                json!("content.entry_published"),
                json!("e1")
            ),
        ]
    );
    // Each verifies with its own endpoint's secret (trusted in that order).
    let verified: Vec<Vec<usize>> = received.iter().map(|r| r.verified_by.clone()).collect();
    assert_eq!(verified, [vec![1], vec![0]], "signed");
    assert_eq!(
        world
            .text(
                "SELECT source || ' ' || source_event_id || ' ' || coalesce(tenant_id, '-') \
                 FROM merezha_notify.webhook_events ORDER BY source_event_id"
            )
            .await,
        ["content content:e1 org_a", "content content:e2 -"]
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.webhook_event'")
            .await,
        0,
        "content called no emit_webhook"
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.module_event'")
            .await,
        2
    );
    // A second run of a subscription job (a lost lease) finds the event and
    // writes nothing more.
    world
        .execute(
            "UPDATE merezha_jobs.jobs SET state = 'available', run_at = now(), \
             finished_at = NULL WHERE kind = 'notify.module_event'",
        )
        .await;
    instance.settle().await;
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.webhook_deliveries")
            .await,
        2
    );
    assert_eq!(receiver.received().len(), 2);
    instance.stop().await;
}

/// Without webhooks the block subscribes to nothing, so content's events
/// stay unrouted.
#[tokio::test]
async fn without_webhooks_no_module_event_subscription_exists() {
    let Some(world) = World::new().await else {
        return;
    };
    let instance = Instance::start(&world, Options::new(SECTION, people())).await;
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_jobs.subscriptions \
                 WHERE job_kind = 'notify.module_event'"
            )
            .await,
        0
    );
    instance.stop().await;
}

fn envelope(event_type: &str, source: &str, event_id: &str) -> IntegrationEvent {
    IntegrationEvent {
        event_id: event_id.to_owned(),
        event_type: event_type.to_owned(),
        version: 1,
        occurred_at: jiff::Timestamp::from_second(1_800_000_000).unwrap(),
        source: source.to_owned(),
        tenant: Some(Identity::new("org_a").unwrap()),
        correlation_id: id("corr"),
        causation_id: None,
        payload: json!({"id": "x"}),
    }
}

/// The mapping of NT §5.5: source the owning module, `source_event_id` the
/// envelope's id, the envelope's tenant and payload; types outside the
/// subscribed set and envelopes whose type is not their source's are
/// permanent failures.
#[test]
fn module_events_map_onto_webhook_events() {
    let event = fanout::module_event(envelope(
        "content.entry_unpublished",
        "content",
        "content:e9",
    ))
    .unwrap();
    assert_eq!(event.source, "content");
    assert_eq!(event.source_event_id, "content:e9");
    assert_eq!(event.tenant.as_deref(), Some("org_a"));
    assert_eq!(event.event_type, "content.entry_unpublished");
    assert_eq!(event.data, json!({"id": "x"}));
    assert_eq!(
        event.id,
        merezha_notifications::ids::webhook_event_id("content", "content:e9")
    );
    for (event_type, source, event_id, code) in [
        (
            "webhook.test",
            "content",
            "content:e1",
            "unknown_event_type",
        ),
        (
            "content.entry_saved",
            "content",
            "content:e1",
            "unknown_event_type",
        ),
        (
            "content.entry_published",
            "identity",
            "identity:e1",
            "invalid_job_payload",
        ),
        (
            "content.entry_published",
            "contentx",
            "contentx:e1",
            "invalid_job_payload",
        ),
        (
            "content.entry_published",
            "content",
            "",
            "invalid_job_payload",
        ),
    ] {
        let failure = fanout::module_event(envelope(event_type, source, event_id)).unwrap_err();
        assert_eq!(failure.code, code, "{event_type} from {source}");
    }
    let long = "x".repeat(201);
    assert_eq!(
        fanout::module_event(envelope("content.entry_deleted", "content", &long))
            .unwrap_err()
            .code,
        "invalid_job_payload"
    );
    assert_eq!(MODULE_EVENT_KIND, "notify.module_event");
}

/// NT-N8 acceptance (notifications side of identity's token mail): a
/// password reset requested by identity with its link in `secret_data`
/// gives one captured email carrying the link, and requesting it again for
/// the same token (`identity.password_reset:{token id}`, as a second run of
/// identity's job would) is a duplicate that sends no second email; the
/// link stays out of the notification row.
#[tokio::test]
async fn a_password_reset_for_one_token_sends_one_email() {
    let Some(world) = World::new().await else {
        return;
    };
    let instance = Instance::start(&world, Options::new(SECTION, people())).await;
    let reset = |key: &str| {
        let mut secret = Map::new();
        secret.insert(
            "link".into(),
            json!("https://app.example.com/auth/reset-password#token=mzt_secret_reset"),
        );
        NotificationRequest {
            topic: id("identity.password_reset"),
            recipients: vec![Recipient::Address(Address::Email {
                address: EmailAddress::new("ada@example.com").unwrap(),
                name: Some("Ada".into()),
                user: Some(id("usr_ada")),
                locale: None,
            })],
            channels: ChannelSelection::TopicDefault,
            secret_data: secret,
            dedupe_key: DedupeKey::new(key).unwrap(),
            expires_at: Some(jiff::Timestamp::now() + jiff::SignedDuration::from_mins(30)),
            ..password_changed("usr_ada", key)
        }
    };
    let key = "identity.password_reset:0193e5a4c3b27d4c9f0e5a1b2c3d4e5f";
    let notifier = instance.notifier("identity");
    for duplicate in [false, true] {
        let notifier = notifier.clone();
        let request = reset(key);
        let handle = instance
            .db
            .transaction(soon(), move |tx| {
                Box::pin(async move { notifier.request(tx, request).await.map_err(Failure::from) })
            })
            .await
            .unwrap();
        assert_eq!(handle.duplicate, duplicate);
        instance.settle().await;
    }
    let captures = world
        .text("SELECT rendered::text FROM merezha_notify.captures")
        .await;
    assert_eq!(captures.len(), 1, "one email");
    assert!(
        captures[0].contains("#token=mzt_secret_reset"),
        "the link from secret_data"
    );
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.notifications \
                 WHERE data::text LIKE '%mzt_secret_reset%' OR NOT has_secret"
            )
            .await,
        0
    );
    instance.stop().await;
}

/// NT-N8 `retention`: notifications settled before `retention.deliveries`
/// go with their deliveries and attempts, provider events received before
/// then go, recent rows stay, and captures are trimmed to the newest
/// `retention.captures`.
#[tokio::test]
async fn retention_deletes_settled_notifications_provider_events_and_old_captures() {
    let Some(world) = World::new().await else {
        return;
    };
    let section = SECTION.replace(
        "secret_key",
        "retention = { deliveries = \"90d\", captures = 3 }\nsecret_key",
    );
    let instance = Instance::start(&world, Options::new(&section, people())).await;
    commit(
        &instance,
        vec![
            password_changed("usr_ada", "old"),
            password_changed("usr_ada", "new"),
        ],
    )
    .await;
    instance.settle().await;
    let core = instance.core();
    maintenance::run(&core, Task::SettleNotifications, soon())
        .await
        .unwrap();
    world
        .execute(
            "UPDATE merezha_notify.notifications SET settled_at = now() - interval '91 days' \
             WHERE dedupe_key = 'old';
             INSERT INTO merezha_notify.provider_events (provider, channel, event, event_key, \
               received_at) VALUES \
               ('brevo', 'email', 'delivered', 'old-event', now() - interval '91 days'), \
               ('brevo', 'email', 'delivered', 'new-event', now());
             INSERT INTO merezha_notify.captures (delivery_id, channel, target, rendered, \
               captured_at) SELECT gen_random_uuid(), 'email', 'x@example.com', '{}'::jsonb, now() \
               FROM generate_series(1, 4);",
        )
        .await;
    assert!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.delivery_attempts")
            .await
            >= 2
    );
    let removed = maintenance::run(&core, Task::Retention, soon())
        .await
        .unwrap();
    assert!(removed >= 3, "{removed}");
    assert_eq!(
        world
            .text("SELECT dedupe_key FROM merezha_notify.notifications")
            .await,
        ["new"]
    );
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.deliveries d WHERE NOT EXISTS \
                 (SELECT 1 FROM merezha_notify.notifications n WHERE n.id = d.notification_id)"
            )
            .await,
        0,
        "deliveries went with their notification"
    );
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.delivery_attempts a WHERE NOT EXISTS \
                 (SELECT 1 FROM merezha_notify.deliveries d WHERE d.id = a.delivery_id)"
            )
            .await,
        0,
        "attempts went with their delivery"
    );
    assert_eq!(
        of(&world, "new").await,
        ["email:accepted:", "inbox:delivered:"]
    );
    assert_eq!(
        world
            .text("SELECT event_key FROM merezha_notify.provider_events")
            .await,
        ["new-event"]
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.captures")
            .await,
        3
    );
    instance.stop().await;
}

/// NT-N8 review: only deliveries that were not skipped count toward the
/// cap. An email skipped by the user's preference leaves room for two
/// more emails, while the inbox, delivered each time, reaches its cap.
#[tokio::test]
async fn skipped_deliveries_do_not_count_toward_the_cap() {
    let Some(world) = World::new().await else {
        return;
    };
    let instance = Instance::start(&world, capped("")).await;
    world
        .execute(
            "INSERT INTO merezha_notify.preferences (user_id, topic, channel, enabled, updated_at) \
             VALUES ('usr_ada', 'tasks.assigned', 'email', false, now())",
        )
        .await;
    commit(&instance, vec![assigned("usr_ada", "p1")]).await;
    instance.settle().await;
    assert_eq!(
        of(&world, "p1").await,
        ["email:skipped:preference", "inbox:delivered:"]
    );
    world
        .execute("DELETE FROM merezha_notify.preferences")
        .await;
    for key in ["p2", "p3"] {
        commit(&instance, vec![assigned("usr_ada", key)]).await;
        instance.settle().await;
    }
    assert_eq!(
        of(&world, "p2").await,
        ["email:accepted:", "inbox:delivered:"]
    );
    assert_eq!(
        of(&world, "p3").await,
        ["email:accepted:", "inbox:skipped:target_rate_limited"]
    );
    instance.stop().await;
}

/// NT-N8 review: an expansion with many targets takes a bounded number of
/// cap locks (at most 64 buckets per channel, whatever the target count),
/// so capped expansions cannot exhaust PostgreSQL's shared lock table,
/// and it takes them in ascending key order.
#[tokio::test]
async fn cap_locks_are_bounded_per_channel_and_taken_in_order() {
    use merezha_notifications::sql::{TARGET_LOCK_BUCKETS, TARGET_LOCK_CLASS, TARGET_LOCKS};
    let Some(world) = World::new().await else {
        return;
    };
    let (channels, keys): (Vec<String>, Vec<String>) = (0..1000)
        .flat_map(|n| {
            [
                ("email".to_owned(), format!("email:user{n}@example.com")),
                ("inbox".to_owned(), format!("inbox:usr_{n}")),
            ]
        })
        .unzip();
    let db = world.db.clone();
    let (locked, held): (Vec<i32>, i64) = world
        .db
        .transaction(soon(), move |tx| {
            Box::pin(async move {
                let locked: Vec<i32> = db
                    .query(
                        tx,
                        &TARGET_LOCKS,
                        &[
                            &TARGET_LOCK_CLASS,
                            &"tasks.assigned",
                            &channels,
                            &keys,
                            &TARGET_LOCK_BUCKETS,
                        ],
                    )
                    .await
                    .map_err(|e| merezha_postgres::classify(&e))?
                    .iter()
                    .map(|row| row.get(0))
                    .collect();
                let held: i64 = (*tx)
                    .query_one(
                        "SELECT count(*) FROM pg_locks \
                         WHERE locktype = 'advisory' AND pid = pg_backend_pid()",
                        &[],
                    )
                    .await
                    .map_err(|e| merezha_postgres::classify(&e))?
                    .get(0);
                Ok((locked, held))
            })
        })
        .await
        .unwrap();
    assert!(
        locked.len() <= 2 * TARGET_LOCK_BUCKETS as usize && locked.len() > 64,
        "{} locks for 2,000 targets",
        locked.len()
    );
    assert_eq!(held, locked.len() as i64, "one advisory lock per bucket");
    let mut sorted = locked.clone();
    sorted.sort_unstable();
    assert_eq!(locked, sorted, "taken in ascending key order");
}

/// NT-N8 review (R11): content's event types arrive only through the
/// subscription, so `emit_webhook` refuses them from any source, a
/// reaction included; application types and `webhook.test` are accepted.
#[tokio::test]
async fn emit_webhook_refuses_another_modules_event_types() {
    use merezha_platform::ports::{EventId, NotifyError, WebhookEvent};
    let Some(world) = World::new().await else {
        return;
    };
    let section = format!(
        "{}event_types = {{ \"tasks.task_completed\" = {{ description = \"Done\" }} }}\n",
        webhooks_section()
    );
    let instance = Instance::start(&world, Options::new(&section, people())).await;
    for (source, event_type, accepted) in [
        ("application", "content.entry_published", false),
        ("content", "content.entry_deleted", false),
        ("application", "tasks.task_completed", true),
        ("application", "webhook.test", true),
    ] {
        let notifier = instance.notifier(source);
        let event = WebhookEvent {
            tenant: Some(id("org_a")),
            event_type: id(event_type),
            source_event_id: EventId::new(format!("{source}-{event_type}")).unwrap(),
            occurred_at: jiff::Timestamp::from_second(1_800_000_000).unwrap(),
            data: json!({}),
        };
        let result = instance
            .db
            .transaction(soon(), move |tx| {
                Box::pin(async move { Ok(notifier.emit_webhook(tx, event).await) })
            })
            .await
            .unwrap();
        match result {
            Ok(_) => assert!(accepted, "{source} {event_type}"),
            Err(error) => {
                assert!(!accepted, "{source} {event_type}: {error:?}");
                assert!(matches!(error, NotifyError::UnknownEventType), "{error:?}");
            }
        }
    }
    instance.stop().await;
}

/// NT-N8 review: `retention` keeps up with a backlog. With small batch
/// bounds (100 per batch, 5 batches per run), 5,000 settled notifications
/// past the cutoff all go: each run deletes its share in short
/// transactions and chains a `notify.maintenance.retention` job while work
/// remains, and the chain stops once the backlog is gone.
#[tokio::test]
async fn retention_drains_a_backlog_through_continuation_jobs() {
    let Some(world) = World::new().await else {
        return;
    };
    let mut options = Options::new(SECTION, people());
    options.retention = Some(maintenance::RetentionBounds {
        notifications: 100,
        rows: 100,
        max_batches: 5,
    });
    let instance = Instance::start(&world, options).await;
    world
        .execute(
            "INSERT INTO merezha_notify.notifications (id, source, dedupe_key, topic, template, \
               category, priority, channels, status, target_count, requested_at, expanded_at, \
               settled_at) \
             SELECT gen_random_uuid(), 'api', 'bulk:' || g, 'identity.password_changed', \
               'identity.password_changed', 'security', 'normal', '{email}', 'expanded', 1, \
               now() - interval '100 days', now() - interval '100 days', \
               now() - interval '100 days' - g * interval '1 second' \
             FROM generate_series(1, 5000) AS g;
             INSERT INTO merezha_notify.deliveries (id, notification_id, channel, target_key, \
               locale, status, run_at, created_at, updated_at, settled_at) \
             SELECT gen_random_uuid(), id, 'email', 'email:ada@example.com', 'en', 'accepted', \
               settled_at, settled_at, settled_at, settled_at FROM merezha_notify.notifications;
             INSERT INTO merezha_notify.provider_events (provider, channel, event, event_key, \
               received_at) \
             SELECT 'brevo', 'email', 'delivered', 'bulk-' || g, now() - interval '100 days' \
             FROM generate_series(1, 1200) AS g;",
        )
        .await;
    let core = instance.core();
    let first = maintenance::run(&core, Task::Retention, soon())
        .await
        .unwrap();
    assert_eq!(first, 500 + 500, "one run's share of each backlog");
    instance
        .until("SELECT count(*) FROM merezha_notify.notifications", 0)
        .await;
    instance
        .until("SELECT count(*) FROM merezha_notify.provider_events", 0)
        .await;
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.deliveries")
            .await,
        0
    );
    // Ten runs drain 5,000 notifications: the first and nine continuations,
    // the last of which finds no more work and chains nothing.
    instance
        .until(
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.maintenance.retention' \
             AND idempotency_key LIKE '%retention:more:%' AND state = 'succeeded'",
            10,
        )
        .await;
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.maintenance.retention' \
                 AND idempotency_key LIKE '%retention:more:%'"
            )
            .await,
        10
    );
    instance.stop().await;
}

/// NT-N8 review (the reviewer's proof of concept): a redelivery reopens a
/// settled notification (it locks the delivery, then sets `settled_at =
/// NULL`) while `retention` picks that notification. Retention waits on
/// the delivery, as redeliver takes it first, and then keeps the reopened
/// notification instead of deleting it; neither deadlocks.
#[tokio::test]
async fn retention_keeps_a_notification_a_redelivery_reopened() {
    let Some(world) = World::new().await else {
        return;
    };
    let instance = Instance::start(&world, Options::new(SECTION, people())).await;
    commit(&instance, vec![password_changed("usr_ada", "reopen")]).await;
    instance.settle().await;
    world
        .execute("UPDATE merezha_notify.notifications SET settled_at = now() - interval '91 days'")
        .await;
    // The redelivery's transaction, held open.
    let redeliver = world.db.acquire(soon()).await.unwrap();
    redeliver
        .batch_execute(
            "BEGIN; \
             SELECT 1 FROM merezha_notify.deliveries WHERE channel = 'email' FOR UPDATE; \
             UPDATE merezha_notify.notifications SET settled_at = NULL;",
        )
        .await
        .unwrap();
    let core = instance.core();
    let retention = tokio::spawn(async move {
        maintenance::run(
            &core,
            Task::Retention,
            std::time::Instant::now() + std::time::Duration::from_secs(20),
        )
        .await
    });
    // Retention is now waiting on the delivery the redelivery holds.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    assert!(!retention.is_finished(), "retention waits on the delivery");
    redeliver.batch_execute("COMMIT").await.unwrap();
    retention.await.unwrap().unwrap();
    assert_eq!(
        world
            .text("SELECT dedupe_key FROM merezha_notify.notifications")
            .await,
        ["reopen"],
        "the reopened notification stays"
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.deliveries")
            .await,
        2
    );
    instance.stop().await;
}

/// Issue #63: with a `notify.maintenance` deadline inside the 3 s margin,
/// a `retention` run still deletes one batch per step and chains on, so
/// the backlog drains; once nothing is left, a run near its deadline
/// chains nothing (before, every such run deleted nothing and queued
/// itself again forever).
#[tokio::test]
async fn a_retention_run_near_its_deadline_still_makes_progress() {
    let Some(world) = World::new().await else {
        return;
    };
    let mut options = Options::new(SECTION, people());
    options.retention = Some(maintenance::RetentionBounds {
        notifications: 100,
        rows: 100,
        max_batches: 5,
    });
    let instance = Instance::start(&world, options).await;
    world
        .execute(
            "INSERT INTO merezha_notify.notifications (id, source, dedupe_key, topic, template, \
               category, priority, channels, status, target_count, requested_at, expanded_at, \
               settled_at) \
             SELECT gen_random_uuid(), 'api', 'near:' || g, 'identity.password_changed', \
               'identity.password_changed', 'security', 'normal', '{email}', 'expanded', 0, \
               now() - interval '100 days', now() - interval '100 days', \
               now() - interval '100 days' - g * interval '1 second' \
             FROM generate_series(1, 300) AS g;",
        )
        .await;
    let chained =
        "SELECT count(*) FROM merezha_jobs.outbox WHERE dedupe_key LIKE 'retention:more:%'";
    let core = instance.core();
    let near = || std::time::Instant::now() + std::time::Duration::from_secs(1);
    let removed = maintenance::run(&core, Task::Retention, near())
        .await
        .unwrap();
    assert_eq!(removed, 100, "one batch despite the deadline");
    assert_eq!(
        world.scalar(chained).await,
        1,
        "work left: one continuation"
    );
    instance
        .until("SELECT count(*) FROM merezha_notify.notifications", 0)
        .await;
    instance
        .until(
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.maintenance.retention' \
             AND state IN ('available', 'running')",
            0,
        )
        .await;
    let before = world.scalar(chained).await;
    let removed = maintenance::run(&core, Task::Retention, near())
        .await
        .unwrap();
    assert_eq!(removed, 0);
    assert_eq!(
        world.scalar(chained).await,
        before,
        "no work, no continuation"
    );
    instance.stop().await;
}

/// Issue #63, NT §2.5 `reclaim_stuck`: an open delivery whose `run_at` and
/// `updated_at` are both more than a day old lost its job. It is settled
/// `job_lost`: `failed`, or `unknown` when an attempt was still `started`
/// (whose pending provider events then apply). Webhook deliveries open
/// and due as long are settled `failed` / `job_lost`. Recent open rows
/// stay. Their notifications then settle, and `retention` deletes them.
#[tokio::test]
async fn deliveries_whose_job_was_lost_are_reclaimed_and_retention_deletes_them() {
    let Some(world) = World::new().await else {
        return;
    };
    let mut options = Options::new(SECTION, people());
    // Delivery job records stay in the outbox: their jobs are "lost".
    options.relay = false;
    let instance = Instance::start(&world, options).await;
    commit(
        &instance,
        vec![
            password_changed("usr_ada", "lost"),
            password_changed("usr_ada", "sent"),
            password_changed("usr_ada", "fresh"),
        ],
    )
    .await;
    instance.relay_now().await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.deliveries WHERE channel = 'email' \
             AND status = 'queued'",
            3,
        )
        .await;
    let email = |key: &str| {
        format!(
            "(SELECT d.id FROM merezha_notify.deliveries d JOIN merezha_notify.notifications n \
              ON n.id = d.notification_id WHERE n.dedupe_key = '{key}' AND d.channel = 'email')"
        )
    };
    let (lost, sent) = (email("lost"), email("sent"));
    world
        .execute(&format!(
            "UPDATE merezha_notify.deliveries SET run_at = now() - interval '25 hours', \
               updated_at = now() - interval '25 hours' WHERE id IN ({lost}, {sent});
             UPDATE merezha_notify.deliveries SET status = 'sending', attempt_count = 1 \
               WHERE id = {sent};
             INSERT INTO merezha_notify.delivery_attempts (delivery_id, attempt, generation, \
               started_at, outcome) SELECT {sent}, 1, 0, now() - interval '25 hours', 'started';
             INSERT INTO merezha_notify.provider_events (provider, channel, event, event_key, \
               delivery_id, received_at, pending) \
               SELECT 'brevo', 'email', 'delivered', 'sent-delivered', {sent}, now(), true;
             INSERT INTO merezha_notify.webhook_endpoints (id, url, secret_ct, secret_nonce, \
               key_id, status, created_by, created_at, updated_at) VALUES \
               ('00000000-0000-0000-0000-0000000000e1', 'https://hooks.example.com/a', \
                '\\x00', '\\x000000000000000000000000', '\\x0000000000000000', 'active', \
                'usr_ops', now(), now());
             INSERT INTO merezha_notify.webhook_events (id, source, source_event_id, event_type, \
               occurred_at, body, created_at) VALUES \
               ('00000000-0000-0000-0000-0000000000f1', 'application', 'e1', \
                'tasks.task_completed', now(), '\\x7b7d', now() - interval '3 days'), \
               ('00000000-0000-0000-0000-0000000000f2', 'application', 'e2', \
                'tasks.task_completed', now(), '\\x7b7d', now() - interval '3 days');
             INSERT INTO merezha_notify.webhook_deliveries (id, event_id, endpoint_id, status, \
               attempt_count, next_attempt_at, created_at, updated_at) VALUES \
               ('00000000-0000-0000-0000-0000000000d1', '00000000-0000-0000-0000-0000000000f1', \
                '00000000-0000-0000-0000-0000000000e1', 'sending', 1, NULL, \
                now() - interval '3 days', now() - interval '25 hours'), \
               ('00000000-0000-0000-0000-0000000000d2', '00000000-0000-0000-0000-0000000000f2', \
                '00000000-0000-0000-0000-0000000000e1', 'retrying', 9, \
                now() + interval '1 hour', now() - interval '3 days', now() - interval '25 hours');
             INSERT INTO merezha_notify.webhook_attempts (delivery_id, attempt, generation, \
               started_at, outcome) VALUES \
               ('00000000-0000-0000-0000-0000000000d1', 1, 0, now() - interval '25 hours', \
                'started');"
        ))
        .await;
    let core = instance.core();
    let reclaimed = maintenance::run(&core, Task::ReclaimStuck, soon())
        .await
        .unwrap();
    assert_eq!(reclaimed, 3);
    assert_eq!(of(&world, "lost").await[0], "email:failed:job_lost");
    // `unknown` / `job_lost`, then the pending `delivered` applied.
    assert_eq!(of(&world, "sent").await[0], "email:delivered:");
    assert_eq!(of(&world, "fresh").await[0], "email:queued:");
    assert_eq!(
        world
            .text(&format!(
                "SELECT outcome || ':' || error_code FROM merezha_notify.delivery_attempts \
                 WHERE delivery_id = {sent}"
            ))
            .await,
        ["ambiguous:lost_outcome"]
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.provider_events WHERE pending")
            .await,
        0
    );
    assert_eq!(
        world
            .text(
                "SELECT status || ':' || COALESCE(last_error_code, '') \
                 FROM merezha_notify.webhook_deliveries ORDER BY id"
            )
            .await,
        ["failed:job_lost", "retrying:"]
    );
    assert_eq!(
        world
            .text("SELECT outcome || ':' || error_code FROM merezha_notify.webhook_attempts")
            .await,
        ["ambiguous:lost_outcome"]
    );
    // A second run finds nothing more.
    assert_eq!(
        maintenance::run(&core, Task::ReclaimStuck, soon())
            .await
            .unwrap(),
        0
    );
    maintenance::run(&core, Task::SettleNotifications, soon())
        .await
        .unwrap();
    assert_eq!(
        world
            .text("SELECT dedupe_key FROM merezha_notify.notifications WHERE settled_at IS NULL")
            .await,
        ["fresh"]
    );
    world
        .execute(
            "UPDATE merezha_notify.notifications SET settled_at = now() - interval '100 days' \
             WHERE settled_at IS NOT NULL",
        )
        .await;
    maintenance::run(&core, Task::Retention, soon())
        .await
        .unwrap();
    assert_eq!(
        world
            .text("SELECT dedupe_key FROM merezha_notify.notifications")
            .await,
        ["fresh"]
    );
    instance.stop().await;
}

/// Issue #63: a `retention` run that changed nothing chains no
/// continuation, even with work left (here no step may run a batch), so
/// it cannot keep the maintenance queue busy without progress.
#[tokio::test]
async fn a_retention_run_without_progress_chains_nothing() {
    let Some(world) = World::new().await else {
        return;
    };
    let mut options = Options::new(SECTION, people());
    options.retention = Some(maintenance::RetentionBounds {
        notifications: 100,
        rows: 100,
        max_batches: 0,
    });
    let instance = Instance::start(&world, options).await;
    world
        .execute(
            "INSERT INTO merezha_notify.provider_events (provider, channel, event, event_key, \
               received_at) VALUES ('brevo', 'email', 'delivered', 'stale', now() - interval '100 days')",
        )
        .await;
    let removed = maintenance::run(&instance.core(), Task::Retention, soon())
        .await
        .unwrap();
    assert_eq!(removed, 0);
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_jobs.outbox WHERE dedupe_key LIKE 'retention:more:%'"
            )
            .await,
        0
    );
    instance.stop().await;
}
