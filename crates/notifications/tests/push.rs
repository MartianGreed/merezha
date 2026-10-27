//! Web Push (NT §5.3, slice NT-N6) against real PostgreSQL and the test
//! kit's `FakePushService`: subscriptions, the push worker, VAPID, the
//! strict host allowlist at subscribe and at send, the egress guard, the
//! payload limit, `push_expire`, and what never reaches telemetry.
mod harness;
use harness::*;
use merezha_notifications::{
    faults,
    maintenance::{self, Task},
    push,
    testkit::{FakePushService, TestResolver},
};
use merezha_platform::ports::{
    ChannelSelection, Correlation, DedupeKey, NotificationRequest, Recipient,
};
use serde_json::{Map, Value, json};
use std::{
    net::IpAddr,
    sync::Arc,
    time::{Duration, Instant},
};

/// The project topic with a push template.
const TOPIC: &str = "[modules.notifications.topics.\"tasks.assigned\"]\n\
    category = \"transactional\"\nchannels = [\"push\"]\n";
const TEMPLATE: &str = "description = \"A task was assigned\"\n\
    [preview]\ntitle = \"Write the report\"\nbody = \"Due Friday\"\n";
const PUSH_TOML: &str = "title = \"{{ title }}\"\nbody = \"{{ body }}\"\n\
    url = \"https://app.example.com/tasks/1\"\n";

/// A section with push on, `hosts` allowed, and `extra` appended.
fn section(hosts: &str, private: bool, extra: &str) -> String {
    format!(
        "{SECTION}egress = {{ allow_private_networks = {private} }}\n\
         [modules.notifications.push]\nvapid_private_key = {{ env = \"VAPID_KEY\" }}\n\
         vapid_subject = \"mailto:ops@example.com\"\nallowed_hosts = [{hosts}]\n{extra}{TOPIC}"
    )
}

/// Options for `section`, with the topic's template files, a fresh VAPID
/// key and Ada in the Directory.
fn options(section: &str, keys: &push::GeneratedKeys) -> Options {
    let people = Arc::new(People::default());
    people.user("usr_ada", "ada@example.com", Some("Ada"), None);
    people.user("usr_bob", "bob@example.com", Some("Bob"), None);
    let mut options = Options::new(section, people);
    options
        .env
        .push(("VAPID_KEY".into(), keys.private_key.clone()));
    options.files = vec![
        (
            "notifications/templates/tasks.assigned/template.toml".into(),
            TEMPLATE.into(),
        ),
        (
            "notifications/templates/tasks.assigned/en/push.toml".into(),
            PUSH_TOML.into(),
        ),
    ];
    options
}

async fn start(world: &World, section: &str) -> (Instance, push::GeneratedKeys) {
    let keys = push::generate().unwrap();
    let instance = Instance::start(world, options(section, &keys)).await;
    (instance, keys)
}

fn request(user: &str, key: &str, data: Value) -> NotificationRequest {
    let Value::Object(data) = data else {
        panic!("data is an object")
    };
    NotificationRequest {
        topic: id("tasks.assigned"),
        recipients: vec![Recipient::User { id: id(user) }],
        channels: ChannelSelection::TopicDefault,
        template: None,
        data,
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

async fn commit(instance: &Instance, request: NotificationRequest) {
    let notifier = instance.notifier("notifications");
    instance
        .db
        .transaction(soon(), move |tx| {
            Box::pin(async move { Ok(notifier.request(tx, request).await) })
        })
        .await
        .unwrap()
        .unwrap();
}

fn assigned() -> Value {
    json!({"title": "Write the report", "body": "Due Friday"})
}

/// Subscribe `token`'s user to the fake at `name`.
async fn subscribe(instance: &Instance, token: &str, body: Value) -> (u16, Value) {
    let (status, body) = instance
        .call(
            "POST",
            "/v1/notifications/push/subscriptions",
            Some(token),
            Some(body),
        )
        .await;
    (status.as_u16(), body)
}

/// `channel:status:reason` of every delivery, ordered.
async fn deliveries(world: &World) -> Vec<String> {
    world
        .text(
            "SELECT channel || ':' || status || ':' || coalesce(status_reason, '') \
             FROM merezha_notify.deliveries ORDER BY channel, target_key",
        )
        .await
}
async fn attempts(world: &World) -> Vec<String> {
    world
        .text("SELECT outcome FROM merezha_notify.delivery_attempts ORDER BY attempt")
        .await
}
async fn wait_settled(instance: &Instance, count: i64) {
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.deliveries \
             WHERE status NOT IN ('queued', 'sending', 'retrying')",
            count,
        )
        .await;
}

/// NT-N6 acceptance: the fake decrypts the payload, and the JWT `aud` is
/// the endpoint origin with `exp` at most 24 h out; the request carries
/// `TTL`, `Urgency` and `Topic`, and the delivery is accepted with the
/// `Location` as its message id.
#[tokio::test]
async fn a_push_reaches_the_fake_decrypted_with_a_vapid_jwt_for_its_origin() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakePushService::start().await;
    let (instance, keys) = start(&world, &section("\"127.0.0.1\"", true, "")).await;
    fake.expect_vapid_key(&keys.public_key);
    instance.tokens.add("ada", user("usr_ada", &[], None));
    // The public key, unauthenticated.
    let (status, body) = instance
        .call("GET", "/v1/notifications/push/vapid-public-key", None, None)
        .await;
    assert_eq!(status.as_u16(), 200);
    assert_eq!(
        body,
        json!({"public_key": keys.public_key, "key_id": keys.key_id})
    );
    let (status, body) = subscribe(&instance, "ada", fake.subscription("ada-laptop")).await;
    assert_eq!(status, 201, "{body}");
    assert_eq!(body["key_id"], json!(keys.key_id));
    let subscription = body["id"].as_str().unwrap().to_owned();
    let mut sent = request("usr_ada", "p:1", assigned());
    sent.collapse_key = Some("task-1".into());
    commit(&instance, sent).await;
    wait_settled(&instance, 1).await;
    assert_eq!(deliveries(&world).await, ["push:accepted:"]);
    let received = fake.received();
    assert_eq!(received.len(), 1);
    let push = &received[0];
    assert_eq!(push.problem, None, "{push:?}");
    assert_eq!(push.audience.as_deref(), Some(fake.origin()));
    let expires_in = push.expires_in.unwrap();
    assert!(
        (11 * 3600..=24 * 3600).contains(&expires_in),
        "{expires_in}"
    );
    assert_eq!(push.subject.as_deref(), Some("mailto:ops@example.com"));
    assert_eq!(push.content_encoding.as_deref(), Some("aes128gcm"));
    assert_eq!(push.ttl.as_deref(), Some("86400"));
    assert_eq!(push.urgency.as_deref(), Some("normal"));
    assert_eq!(
        push.topic.as_deref(),
        Some(push::topic_header("task-1").as_str())
    );
    let delivery = world
        .text("SELECT id::text FROM merezha_notify.deliveries")
        .await
        .remove(0);
    let notification = world
        .text("SELECT id::text FROM merezha_notify.notifications")
        .await
        .remove(0);
    assert_eq!(
        push.payload,
        json!({
            "id": delivery, "notification_id": notification, "topic": "tasks.assigned",
            "title": "Write the report", "body": "Due Friday",
            "url": "https://app.example.com/tasks/1", "tag": "task-1",
        })
    );
    assert_eq!(
        world
            .text(
                "SELECT provider || ' ' || provider_message_id || ' ' || \
                 push_subscription_id::text FROM merezha_notify.deliveries"
            )
            .await,
        [format!(
            "webpush {}/messages/0 {subscription}",
            fake.origin()
        )]
    );
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.push_subscriptions \
                 WHERE last_success_at IS NOT NULL AND consecutive_failures = 0"
            )
            .await,
        1
    );
    // The list shows the origin, never the endpoint or the keys.
    let (status, body) = instance
        .call(
            "GET",
            "/v1/notifications/push/subscriptions",
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 200);
    assert_eq!(body[0]["endpoint_origin"], json!(fake.origin()));
    assert_eq!(body[0]["id"], json!(subscription));
    assert!(!body.to_string().contains("ada-laptop"), "{body}");
    instance.stop().await;
}

/// NT-N6 acceptance: 410 (and 404) deletes the subscription and fails the
/// delivery with `subscription_gone`; a redelivery then finds no
/// subscription.
#[tokio::test]
async fn gone_answers_delete_the_subscription_and_fail_the_delivery() {
    for status in [410, 404] {
        let Some(world) = World::new().await else {
            return;
        };
        let fake = FakePushService::start().await;
        let (instance, _) = start(&world, &section("\"127.0.0.1\"", true, "")).await;
        instance.tokens.add("ada", user("usr_ada", &[], None));
        instance
            .tokens
            .add("admin", user("usr_admin", &["notifications.admin"], None));
        assert_eq!(
            subscribe(&instance, "ada", fake.subscription("a")).await.0,
            201
        );
        fake.respond(status);
        commit(&instance, request("usr_ada", "g:1", assigned())).await;
        wait_settled(&instance, 1).await;
        assert_eq!(
            deliveries(&world).await,
            ["push:failed:subscription_gone"],
            "{status}"
        );
        assert_eq!(attempts(&world).await, ["permanent"], "{status}");
        assert_eq!(
            world
                .scalar("SELECT count(*) FROM merezha_notify.push_subscriptions")
                .await,
            0,
            "{status}"
        );
        assert_eq!(fake.received().len(), 1);
        let delivery = world
            .text("SELECT id::text FROM merezha_notify.deliveries")
            .await
            .remove(0);
        let (code, _) = instance
            .call(
                "POST",
                &format!("/v1/notifications/deliveries/{delivery}/redeliver"),
                Some("admin"),
                None,
            )
            .await;
        assert_eq!(code.as_u16(), 202);
        wait_settled(&instance, 1).await;
        assert_eq!(deliveries(&world).await, ["push:skipped:no_subscription"]);
        assert_eq!(
            fake.received().len(),
            1,
            "nothing sent to a gone subscription"
        );
        instance.stop().await;
    }
}

/// NT-N6 acceptance: a host outside the allowlist is refused at subscribe;
/// the allowlist applies again at send, so a subscription whose host was
/// removed from it is never contacted.
#[tokio::test]
async fn hosts_outside_the_allowlist_are_refused_at_subscribe_and_at_send() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakePushService::start().await;
    let (instance, _) = start(
        &world,
        &section("\"fcm.googleapis.com\", \"127.0.0.1\"", false, ""),
    )
    .await;
    instance.tokens.add("ada", user("usr_ada", &[], None));
    let mut body = fake.subscription("a");
    for endpoint in [
        "https://evil.example/fcm.googleapis.com/x",
        "https://fcm.googleapis.com.evil.example/x",
        "http://fcm.googleapis.com/x",
        "https://fcm.googleapis.com:8443/x",
        // Without allow_private_networks, the loopback fake is refused too.
        fake.endpoint("a").as_str(),
    ] {
        body["endpoint"] = json!(endpoint);
        let (status, reply) = subscribe(&instance, "ada", body.clone()).await;
        assert_eq!(
            (status, reply["error"]["code"].clone()),
            (422, json!("push_host_not_allowed")),
            "{endpoint}"
        );
        assert_eq!(
            reply["error"]["details"],
            json!([{"path": "/endpoint", "code": "host_not_allowed"}])
        );
    }
    body["endpoint"] = json!("https://user:pw@fcm.googleapis.com/x");
    let (status, reply) = subscribe(&instance, "ada", body.clone()).await;
    assert_eq!(
        (status, reply["error"]["code"].clone()),
        (422, json!("subscription_invalid"))
    );
    body["endpoint"] = json!("https://fcm.googleapis.com/fcm/send/abc");
    assert_eq!(subscribe(&instance, "ada", body.clone()).await.0, 201);
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.push_subscriptions")
            .await,
        1
    );
    // A row whose host is not on the list (an entry was removed since).
    world
        .execute(
            "UPDATE merezha_notify.push_subscriptions SET \
             endpoint = 'https://push.removed.example/s/1'",
        )
        .await;
    commit(&instance, request("usr_ada", "h:1", assigned())).await;
    wait_settled(&instance, 1).await;
    assert_eq!(
        deliveries(&world).await,
        ["push:failed:push_host_not_allowed"]
    );
    assert!(fake.received().is_empty());
    instance.stop().await;
}

/// NT-N6 acceptance: a payload over 3,000 bytes is cut at a character
/// boundary at send, and still decrypts as JSON.
#[tokio::test]
async fn a_payload_over_the_limit_is_truncated_at_send() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakePushService::start().await;
    let (instance, _) = start(&world, &section("\"127.0.0.1\"", true, "")).await;
    instance.tokens.add("ada", user("usr_ada", &[], None));
    assert_eq!(
        subscribe(&instance, "ada", fake.subscription("a")).await.0,
        201
    );
    let long = "ü".repeat(5000);
    commit(
        &instance,
        request("usr_ada", "t:1", json!({"title": "Long", "body": long})),
    )
    .await;
    wait_settled(&instance, 1).await;
    assert_eq!(deliveries(&world).await, ["push:accepted:"]);
    let received = fake.received();
    let push = &received[0];
    assert_eq!(push.problem, None);
    assert!(push.plaintext_len <= 3000, "{}", push.plaintext_len);
    let body = push.payload["body"].as_str().unwrap();
    assert!(!body.is_empty() && body.len() < long.len());
    assert!(long.starts_with(body));
    instance.stop().await;
}

/// NT-N6 acceptance: subscriptions made with an old VAPID key are not
/// targeted; `push_expire` deletes them once unused for the rotation grace,
/// the expired ones and the failing ones with no recent success.
#[tokio::test]
async fn subscriptions_of_an_old_vapid_key_are_not_targeted_and_expire() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakePushService::start().await;
    let keys = push::generate().unwrap();
    let mut opts = options(&section("\"127.0.0.1\"", true, ""), &keys);
    opts.meters = Some(harness.telemetry().meters());
    let instance = Instance::start(&world, opts).await;
    instance.tokens.add("ada", user("usr_ada", &[], None));
    assert_eq!(
        subscribe(&instance, "ada", fake.subscription("new"))
            .await
            .0,
        201
    );
    // A subscription of the previous key, one expired, one failing.
    for (name, extra) in [
        (
            "old",
            "vapid_key_id = '00000000000000aa', last_used_at = now() - interval '8 days'",
        ),
        ("expired", "expires_at = now() - interval '1 minute'"),
        (
            "failing",
            "consecutive_failures = 12, created_at = now() - interval '40 days'",
        ),
    ] {
        world
            .execute(&format!(
                "INSERT INTO merezha_notify.push_subscriptions (id, user_id, endpoint, \
                 endpoint_origin, p256dh, auth, vapid_key_id, created_at, last_used_at) \
                 SELECT gen_random_uuid(), user_id, endpoint || '-{name}', endpoint_origin, \
                 p256dh, auth, vapid_key_id, now(), now() \
                 FROM merezha_notify.push_subscriptions WHERE endpoint NOT LIKE '%-%'; \
                 UPDATE merezha_notify.push_subscriptions SET {extra} \
                 WHERE endpoint LIKE '%-{name}'"
            ))
            .await;
    }
    commit(&instance, request("usr_ada", "k:1", assigned())).await;
    wait_settled(&instance, 2).await;
    // The current, unexpired subscriptions only: `new` and `failing`.
    assert_eq!(
        deliveries(&world).await,
        ["push:accepted:", "push:accepted:"]
    );
    let mut names: Vec<String> = fake.received().into_iter().map(|r| r.name).collect();
    names.sort();
    assert_eq!(names, ["new", "new-failing"]);
    // The successful push reset the failing one; make it fail again.
    world
        .execute(
            "UPDATE merezha_notify.push_subscriptions SET consecutive_failures = 12, \
             last_success_at = now() - interval '31 days' WHERE endpoint LIKE '%-failing'",
        )
        .await;
    let removed = maintenance::run(&instance.core(), Task::PushExpire, soon())
        .await
        .unwrap();
    assert_eq!(removed, 3);
    assert_eq!(
        world
            .text("SELECT endpoint FROM merezha_notify.push_subscriptions")
            .await,
        [fake.endpoint("new")]
    );
    let points = harness.metric("merezha.notification.push_subscriptions_removed");
    for reason in ["key_changed", "expired", "failures"] {
        assert_eq!(
            points.value(&[("notify.reason", reason)]),
            Some(1.0),
            "{reason}"
        );
    }
    instance.stop().await;
}

/// A VAPID key rotation rolled out instance by instance: the instance on
/// the old key and the one on the new key each keep the subscriptions the
/// other made, since `push_expire` deletes another key's subscription only
/// once it has gone unused for the 7-day grace; after the rollout the old
/// key's subscriptions go.
#[tokio::test]
async fn a_rolling_key_rotation_keeps_the_subscriptions_of_both_keys() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakePushService::start().await;
    let section = section("\"127.0.0.1\"", true, "");
    let (old_keys, new_keys) = (push::generate().unwrap(), push::generate().unwrap());
    let mut on_old = options(&section, &old_keys);
    on_old.workers = false;
    let old = Instance::start(&world, on_old).await;
    let new = Instance::start(&world, options(&section, &new_keys)).await;
    for instance in [&old, &new] {
        instance.tokens.add("ada", user("usr_ada", &[], None));
    }
    assert_eq!(subscribe(&old, "ada", fake.subscription("k1")).await.0, 201);
    assert_eq!(subscribe(&new, "ada", fake.subscription("k2")).await.0, 201);
    // Made long ago, used recently: the grace counts from the last use.
    world
        .execute(
            "UPDATE merezha_notify.push_subscriptions SET created_at = now() - interval '60 days'",
        )
        .await;
    let endpoints = || async {
        world
            .text("SELECT endpoint FROM merezha_notify.push_subscriptions ORDER BY endpoint")
            .await
    };
    for instance in [&old, &new] {
        let removed = maintenance::run(&instance.core(), Task::PushExpire, soon())
            .await
            .unwrap();
        assert_eq!(removed, 0);
    }
    assert_eq!(
        endpoints().await,
        [fake.endpoint("k1"), fake.endpoint("k2")]
    );
    // The new key's instance pushes to its own subscriptions only.
    commit(&new, request("usr_ada", "rot:1", assigned())).await;
    wait_settled(&new, 1).await;
    assert_eq!(
        fake.received()
            .into_iter()
            .map(|r| r.name)
            .collect::<Vec<_>>(),
        ["k2"]
    );
    // The rollout is over: the old key's subscription is no longer used,
    // and goes once the grace has passed.
    world
        .execute(
            "UPDATE merezha_notify.push_subscriptions SET last_used_at = now() - interval '6 days' \
             WHERE endpoint LIKE '%k1'",
        )
        .await;
    let core = new.core();
    let expire = || maintenance::run(&core, Task::PushExpire, soon());
    assert_eq!(expire().await.unwrap(), 0);
    world
        .execute(
            "UPDATE merezha_notify.push_subscriptions SET last_used_at = now() - interval '8 days' \
             WHERE endpoint LIKE '%k1'",
        )
        .await;
    assert_eq!(expire().await.unwrap(), 1);
    assert_eq!(endpoints().await, [fake.endpoint("k2")]);
    new.stop().await;
}

/// The worker checks the subscription again at send (Tx A): one that left
/// the notification's tenant, or now belongs to another user, is skipped
/// `no_subscription` and never contacted.
#[tokio::test]
async fn send_skips_a_subscription_that_left_the_tenant_or_the_user() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakePushService::start().await;
    let (instance, _) = start(&world, &section("\"127.0.0.1\"", true, "")).await;
    instance
        .tokens
        .add("ada@a", user("usr_ada", &[], Some("org_a")));
    instance
        .tokens
        .add("admin", user("usr_admin", &["notifications.admin"], None));
    assert_eq!(
        subscribe(&instance, "ada@a", fake.subscription("in-a"))
            .await
            .0,
        201
    );
    // Two tenant notifications whose first attempt fails, so both can be
    // redelivered.
    fake.fail_next(2, 400);
    let mut ids = Vec::new();
    for key in ["mv:1", "mv:2"] {
        let mut sent = request("usr_ada", key, assigned());
        sent.tenant = Some(id("org_a"));
        sent.members = vec![id("usr_ada")];
        commit(&instance, sent).await;
        wait_settled(&instance, ids.len() as i64 + 1).await;
        let found = world
            .text(&format!(
                "SELECT d.id::text FROM merezha_notify.deliveries d \
                 JOIN merezha_notify.notifications n ON n.id = d.notification_id \
                 WHERE n.dedupe_key LIKE '%{key}'"
            ))
            .await;
        ids.push(found[0].clone());
    }
    assert_eq!(
        deliveries(&world).await,
        ["push:failed:push_rejected", "push:failed:push_rejected"]
    );
    let redeliver = |delivery: String| {
        let instance = &instance;
        async move {
            let (code, _) = instance
                .call(
                    "POST",
                    &format!("/v1/notifications/deliveries/{delivery}/redeliver"),
                    Some("admin"),
                    None,
                )
                .await;
            assert_eq!(code.as_u16(), 202);
        }
    };
    let status = |delivery: String| {
        let world = &world;
        async move {
            world
                .text(&format!(
                    "SELECT status || ':' || coalesce(status_reason, '') \
                     FROM merezha_notify.deliveries WHERE id = '{delivery}'"
                ))
                .await
                .remove(0)
        }
    };
    // Same id, same user, another tenant now.
    world
        .execute("UPDATE merezha_notify.push_subscriptions SET tenant_id = 'org_b'")
        .await;
    redeliver(ids[0].clone()).await;
    wait_settled(&instance, 2).await;
    assert_eq!(status(ids[0].clone()).await, "skipped:no_subscription");
    // Same id, the tenant back, another user now.
    world
        .execute(
            "UPDATE merezha_notify.push_subscriptions SET tenant_id = 'org_a', user_id = 'usr_bob'",
        )
        .await;
    redeliver(ids[1].clone()).await;
    wait_settled(&instance, 2).await;
    assert_eq!(status(ids[1].clone()).await, "skipped:no_subscription");
    assert_eq!(
        fake.received().len(),
        2,
        "only the two refused first attempts"
    );
    instance.stop().await;
}

/// An IP-literal endpoint never reaches the resolver, so the egress guard
/// checks it before the request: an allowed host whose address
/// `deny_cidrs` refuses fails `egress_blocked` and is never dialled.
#[tokio::test]
async fn an_ip_literal_endpoint_is_checked_by_the_egress_guard_at_send() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakePushService::start().await;
    let (instance, _) = start(
        &world,
        &section("\"127.0.0.1\"", true, "").replace(
            "egress = { allow_private_networks = true }",
            "egress = { allow_private_networks = true, deny_cidrs = [\"127.0.0.0/8\"] }",
        ),
    )
    .await;
    instance.tokens.add("ada", user("usr_ada", &[], None));
    assert_eq!(
        subscribe(&instance, "ada", fake.subscription("literal"))
            .await
            .0,
        201
    );
    commit(&instance, request("usr_ada", "ip:1", assigned())).await;
    wait_settled(&instance, 1).await;
    assert_eq!(deliveries(&world).await, ["push:failed:egress_blocked"]);
    assert!(fake.received().is_empty());
    instance.stop().await;
}

/// The subscription a `POST` just made is never the one the per-user limit
/// evicts, even when another subscription was used later than this
/// instance's clock says (another instance, clock skew).
#[tokio::test]
async fn a_new_subscription_is_never_the_one_the_limit_evicts() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakePushService::start().await;
    let (instance, _) = start(
        &world,
        &section("\"127.0.0.1\"", true, "max_subscriptions_per_user = 1\n"),
    )
    .await;
    instance.tokens.add("ada", user("usr_ada", &[], None));
    assert_eq!(
        subscribe(&instance, "ada", fake.subscription("first"))
            .await
            .0,
        201
    );
    world
        .execute(
            "UPDATE merezha_notify.push_subscriptions SET last_used_at = now() + interval '1 hour'",
        )
        .await;
    let (status, body) = subscribe(&instance, "ada", fake.subscription("second")).await;
    assert_eq!(status, 201, "{body}");
    assert_eq!(
        world
            .text("SELECT id::text FROM merezha_notify.push_subscriptions")
            .await,
        [body["id"].as_str().unwrap()]
    );
    instance.stop().await;
}

/// The render refuses a push whose title and url would not fit the
/// payload with the worst collapse key (64 control characters, six bytes
/// each in JSON), so the request is refused `render_failed` rather than
/// sent without its tag.
#[tokio::test]
async fn a_push_that_would_not_fit_with_the_worst_tag_is_refused_at_render() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakePushService::start().await;
    let keys = push::generate().unwrap();
    let mut opts = options(&section("\"127.0.0.1\"", true, ""), &keys);
    opts.files = vec![
        (
            "notifications/templates/tasks.assigned/template.toml".into(),
            "description = \"A task was assigned\"\n[preview]\ntitle = \"T\"\nbody = \"B\"\n\
             url = \"https://app.example.com/p\"\n"
                .into(),
        ),
        (
            "notifications/templates/tasks.assigned/en/push.toml".into(),
            "title = \"{{ title }}\"\nbody = \"{{ body }}\"\nurl = \"{{ url }}\"\n".into(),
        ),
    ];
    let instance = Instance::start(&world, opts).await;
    instance.tokens.add("ada", user("usr_ada", &[], None));
    assert_eq!(
        subscribe(&instance, "ada", fake.subscription("a")).await.0,
        201
    );
    // With a 64-byte tag of letters this fits; with control characters it
    // would not, even with an empty body.
    let data = json!({
        "title": "€".repeat(200),
        "body": "Due Friday",
        "url": format!("https://app.example.com/{}", "a".repeat(2000)),
    });
    let mut sent = request("usr_ada", "big:1", data);
    sent.collapse_key = Some("\u{1}".repeat(64));
    let notifier = instance.notifier("notifications");
    let refused = instance
        .db
        .transaction(soon(), move |tx| {
            Box::pin(async move { Ok(notifier.request(tx, sent).await) })
        })
        .await
        .unwrap();
    assert!(
        matches!(
            refused,
            Err(merezha_platform::ports::NotifyError::RenderFailed { .. })
        ),
        "{refused:?}"
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.notifications")
            .await,
        0
    );
    assert!(fake.received().is_empty());
    instance.stop().await;
}

/// Subscriptions belong to one user and follow the inbox's tenant scope;
/// the per-user limit evicts the least recently used; an endpoint that
/// changes hands gets a new id; invalid keys are refused with details.
#[tokio::test]
async fn subscriptions_are_per_user_tenant_scoped_and_limited() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakePushService::start().await;
    let (instance, _) = start(
        &world,
        &section("\"127.0.0.1\"", true, "max_subscriptions_per_user = 2\n"),
    )
    .await;
    instance.tokens.add("ada", user("usr_ada", &[], None));
    instance
        .tokens
        .add("ada@a", user("usr_ada", &[], Some("org_a")));
    instance
        .tokens
        .add("ada@b", user("usr_ada", &[], Some("org_b")));
    instance.tokens.add("bob", user("usr_bob", &[], None));
    let (_, plain) = subscribe(&instance, "ada", fake.subscription("plain")).await;
    let (_, in_a) = subscribe(&instance, "ada@a", fake.subscription("in-a")).await;
    let ids = |body: &Value| -> Vec<String> {
        body.as_array()
            .unwrap()
            .iter()
            .map(|s| s["id"].as_str().unwrap().to_owned())
            .collect()
    };
    let list = |token: &'static str| {
        let instance = &instance;
        async move {
            instance
                .call(
                    "GET",
                    "/v1/notifications/push/subscriptions",
                    Some(token),
                    None,
                )
                .await
                .1
        }
    };
    // Tenant B sees only the tenantless subscription; A sees both.
    assert_eq!(ids(&list("ada@b").await), [plain["id"].as_str().unwrap()]);
    assert_eq!(ids(&list("ada@a").await).len(), 2);
    assert!(ids(&list("bob").await).is_empty());
    // Another tenant's or another user's subscription is 404.
    let path = format!(
        "/v1/notifications/push/subscriptions/{}",
        in_a["id"].as_str().unwrap()
    );
    for token in ["ada@b", "bob"] {
        let (status, _) = instance.call("DELETE", &path, Some(token), None).await;
        assert_eq!(status.as_u16(), 404, "{token}");
    }
    // A notification of tenant A reaches the subscription made in A only.
    let mut sent = request("usr_ada", "s:1", assigned());
    sent.tenant = Some(id("org_a"));
    sent.members = vec![id("usr_ada")];
    commit(&instance, sent).await;
    wait_settled(&instance, 1).await;
    // Expansion itself skips the tenantless subscription: one delivery,
    // not a second one the worker would then skip.
    assert_eq!(deliveries(&world).await, ["push:accepted:"]);
    assert_eq!(
        fake.received()
            .into_iter()
            .map(|r| r.name)
            .collect::<Vec<_>>(),
        ["in-a"]
    );
    // A third subscription evicts the least recently used one (`plain`;
    // `in-a` was used by the push).
    assert_eq!(
        subscribe(&instance, "ada", fake.subscription("third"))
            .await
            .0,
        201
    );
    assert_eq!(
        world
            .text("SELECT endpoint FROM merezha_notify.push_subscriptions ORDER BY endpoint")
            .await,
        [fake.endpoint("in-a"), fake.endpoint("third")]
    );
    // Bob's browser profile takes over `third`: it moves with a new id.
    let third_before = world
        .text("SELECT id::text FROM merezha_notify.push_subscriptions WHERE endpoint LIKE '%third'")
        .await;
    let (status, moved) = subscribe(&instance, "bob", fake.subscription("third")).await;
    assert_eq!(status, 201);
    assert_ne!(moved["id"].as_str().unwrap(), third_before[0]);
    assert_eq!(ids(&list("bob").await), [moved["id"].as_str().unwrap()]);
    assert_eq!(
        ids(&list("ada").await).len(),
        0,
        "ada sees no tenantless one"
    );
    // The owner deletes its own subscription.
    let (status, _) = instance.call("DELETE", &path, Some("ada@a"), None).await;
    assert_eq!(status.as_u16(), 204);
    // Invalid keys, with the field in details and no echo of the input.
    let mut bad = fake.subscription("bad");
    bad["keys"]["p256dh"] = json!("AAAA");
    let (status, reply) = subscribe(&instance, "ada", bad.clone()).await;
    assert_eq!(
        (status, reply["error"]["details"].clone()),
        (
            422,
            json!([{"path": "/keys/p256dh", "code": "invalid_key"}])
        )
    );
    let mut bad = fake.subscription("bad");
    bad["keys"]["auth"] = json!("c2hvcnQ");
    let (status, reply) = subscribe(&instance, "ada", bad).await;
    assert_eq!(
        (status, reply["error"]["details"].clone()),
        (422, json!([{"path": "/keys/auth", "code": "invalid_key"}]))
    );
    // Not a user: 401 without a principal.
    let (status, _) = instance
        .call(
            "POST",
            "/v1/notifications/push/subscriptions",
            None,
            Some(fake.subscription("x")),
        )
        .await;
    assert_eq!(status.as_u16(), 401);
    instance.stop().await;
}

/// Without a push section the push routes answer 404 `push_disabled`.
#[tokio::test]
async fn push_routes_answer_push_disabled_without_the_section() {
    let Some(world) = World::new().await else {
        return;
    };
    let mut options = Options::new(SECTION, Arc::new(People::default()));
    options.workers = false;
    let instance = Instance::start(&world, options).await;
    instance.tokens.add("ada", user("usr_ada", &[], None));
    for (method, path, token) in [
        ("GET", "/v1/notifications/push/vapid-public-key", None),
        ("GET", "/v1/notifications/push/subscriptions", Some("ada")),
    ] {
        let (status, body) = instance.call(method, path, token, None).await;
        assert_eq!(
            (status.as_u16(), body["error"]["code"].clone()),
            (404, json!("push_disabled")),
            "{path}"
        );
    }
}

/// The egress guard: a host resolving only to private addresses is never
/// dialled (permanent `egress_blocked`), and when the resolver returns an
/// allowed and a denied address, only the allowed one is dialled.
#[tokio::test]
async fn the_egress_guard_blocks_private_destinations_and_dials_only_allowed_addresses() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let resolver = TestResolver::new();
    let private: IpAddr = "10.0.0.5".parse().unwrap();
    let loopback: IpAddr = "127.0.0.1".parse().unwrap();
    resolver.map("push.test", &[private]);
    let keys = push::generate().unwrap();
    let mut opts = options(&section("\"push.test\"", false, ""), &keys);
    opts.push_lookup = Some(Arc::new(resolver.clone()));
    opts.meters = Some(harness.telemetry().meters());
    let instance = Instance::start(&world, opts).await;
    instance.tokens.add("ada", user("usr_ada", &[], None));
    let fake = FakePushService::start().await;
    let mut body = fake.subscription("a");
    body["endpoint"] = json!("https://push.test/push/a");
    assert_eq!(subscribe(&instance, "ada", body).await.0, 201);
    commit(&instance, request("usr_ada", "e:1", assigned())).await;
    wait_settled(&instance, 1).await;
    assert_eq!(deliveries(&world).await, ["push:failed:egress_blocked"]);
    assert!(resolver.lookups() >= 1);
    assert_eq!(
        harness
            .metric("merezha.notification.egress_blocked")
            .value(&[("notify.target", "push")]),
        Some(1.0)
    );
    instance.stop().await;

    // Development: private networks allowed, 10.0.0.0/8 denied; the name
    // resolves to both, and only the loopback fake is dialled.
    let Some(world) = World::new().await else {
        return;
    };
    resolver.map("push.test", &[private, loopback]);
    let mut opts = options(
        &section("\"push.test\"", true, "").replace(
            "egress = { allow_private_networks = true }",
            "egress = { allow_private_networks = true, deny_cidrs = [\"10.0.0.0/8\"] }",
        ),
        &keys,
    );
    opts.push_lookup = Some(Arc::new(resolver.clone()));
    let instance = Instance::start(&world, opts).await;
    instance.tokens.add("ada", user("usr_ada", &[], None));
    let port = fake.origin().rsplit(':').next().unwrap().to_owned();
    let mut body = fake.subscription("pinned");
    body["endpoint"] = json!(format!("http://push.test:{port}/push/pinned"));
    assert_eq!(subscribe(&instance, "ada", body).await.0, 201);
    commit(&instance, request("usr_ada", "e:2", assigned())).await;
    wait_settled(&instance, 1).await;
    assert_eq!(deliveries(&world).await, ["push:accepted:"]);
    let received = fake.received();
    assert_eq!(received.len(), 1);
    // The JWT audience is the endpoint's origin, the name and port.
    assert_eq!(
        received[0].audience.as_deref(),
        Some(format!("http://push.test:{port}").as_str())
    );
    instance.stop().await;
}

/// Endpoints, `p256dh`, `auth`, the payload and the VAPID key never reach a
/// span, a log or a metric.
#[tokio::test]
async fn push_never_logs_endpoints_keys_or_payloads() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakePushService::start().await;
    let keys = push::generate().unwrap();
    let mut opts = options(&section("\"127.0.0.1\"", true, ""), &keys);
    opts.meters = Some(harness.telemetry().meters());
    let instance = Instance::start(&world, opts).await;
    instance.tokens.add("ada", user("usr_ada", &[], None));
    let subscription = fake.subscription("endpoint-canary-41c9");
    assert_eq!(
        subscribe(&instance, "ada", subscription.clone()).await.0,
        201
    );
    commit(
        &instance,
        request(
            "usr_ada",
            "c:1",
            json!({"title": "Title", "body": "body-canary-9d2e"}),
        ),
    )
    .await;
    // One accepted push, then a gone one for the error paths.
    wait_settled(&instance, 1).await;
    fake.respond(410);
    commit(
        &instance,
        request(
            "usr_ada",
            "c:2",
            json!({"title": "T", "body": "body-canary-9d2e"}),
        ),
    )
    .await;
    wait_settled(&instance, 2).await;
    assert_eq!(fake.received().len(), 2);
    for canary in [
        "endpoint-canary-41c9",
        "body-canary-9d2e",
        subscription["keys"]["p256dh"].as_str().unwrap(),
        subscription["keys"]["auth"].as_str().unwrap(),
        keys.private_key.as_str(),
    ] {
        harness.assert_no_canary(canary);
    }
    harness.assert_catalogued();
    let names: Vec<String> = harness
        .recorded()
        .into_iter()
        .filter_map(|r| r.name)
        .collect();
    for name in [
        "notification.accepted",
        "notification.push_subscription_removed",
    ] {
        assert!(names.iter().any(|n| n == name), "{name} not in {names:?}");
    }
    instance.stop().await;
}

/// Crashes and retry delays race lease deadlines (issue #37): CI runs them
/// alone, one at a time (`cargo test -- --test-threads=1 timing::`).
mod timing {
    use super::*;

    /// `notify.push` leases expire after 1 s and a crashed run is aborted
    /// after 3 s.
    fn crashing() -> String {
        "[jobs.queues.\"notify.push\"]\nconcurrency = 2\nlease_ms = 1000\ntimeout_ms = 3000\n\
         backoff_base_ms = 10\nbackoff_max_ms = 60000\n"
            .to_owned()
    }

    /// NT-N6 test: a delivery job that runs twice (a crash after Tx B, the
    /// lease expires, the job runs again) gives one push.
    #[tokio::test]
    async fn duplicate_jobs_give_one_push() {
        let Some(world) = World::new().await else {
            return;
        };
        let fake = FakePushService::start().await;
        let keys = push::generate().unwrap();
        let mut opts = options(
            &format!("{}{}", crashing(), section("\"127.0.0.1\"", true, "")),
            &keys,
        );
        opts.heartbeat = false;
        opts.faults.crash_once(faults::AFTER_TX_B);
        let instance = Instance::start(&world, opts).await;
        instance.tokens.add("ada", user("usr_ada", &[], None));
        assert_eq!(
            subscribe(&instance, "ada", fake.subscription("a")).await.0,
            201
        );
        commit(&instance, request("usr_ada", "d:1", assigned())).await;
        wait_settled(&instance, 1).await;
        // Past the crashed run's deadline and a rerun of the job.
        instance
            .until(
                "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.deliver.push' \
                 AND state = 'succeeded'",
                1,
            )
            .await;
        assert!(
            world
                .scalar("SELECT max(attempts)::bigint FROM merezha_jobs.jobs WHERE kind = 'notify.deliver.push'")
                .await
                >= 2,
            "the job ran again"
        );
        assert_eq!(deliveries(&world).await, ["push:accepted:"]);
        assert_eq!(attempts(&world).await, ["accepted"]);
        assert_eq!(fake.received().len(), 1);
        instance.stop().await;
    }

    /// A crash after the push service answered leaves a `started` attempt:
    /// push resends (NT §5.1), with the same delivery id in the payload for
    /// the client to deduplicate.
    #[tokio::test]
    async fn a_crash_after_the_push_call_resends_with_the_same_id() {
        let Some(world) = World::new().await else {
            return;
        };
        let fake = FakePushService::start().await;
        let keys = push::generate().unwrap();
        let mut opts = options(
            &format!("{}{}", crashing(), section("\"127.0.0.1\"", true, "")),
            &keys,
        );
        opts.heartbeat = false;
        opts.faults.crash_once(faults::AFTER_PROVIDER_CALL);
        let instance = Instance::start(&world, opts).await;
        instance.tokens.add("ada", user("usr_ada", &[], None));
        assert_eq!(
            subscribe(&instance, "ada", fake.subscription("a")).await.0,
            201
        );
        commit(&instance, request("usr_ada", "a:1", assigned())).await;
        wait_settled(&instance, 1).await;
        assert_eq!(deliveries(&world).await, ["push:accepted:"]);
        assert_eq!(attempts(&world).await, ["ambiguous", "accepted"]);
        let received = fake.received();
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].payload["id"], received[1].payload["id"]);
        instance.stop().await;
    }

    /// NT-N6 test: a 429 with `Retry-After: 2` delays the next attempt by
    /// at least 2 s.
    #[tokio::test]
    async fn a_429_retry_after_is_honoured() {
        let Some(world) = World::new().await else {
            return;
        };
        let fake = FakePushService::start().await;
        let (instance, _) = start(
            &world,
            &format!("{}{}", crashing(), section("\"127.0.0.1\"", true, "")),
        )
        .await;
        instance.tokens.add("ada", user("usr_ada", &[], None));
        assert_eq!(
            subscribe(&instance, "ada", fake.subscription("a")).await.0,
            201
        );
        fake.rate_limit_next(2);
        let started = Instant::now();
        commit(&instance, request("usr_ada", "r:1", assigned())).await;
        wait_settled(&instance, 1).await;
        assert_eq!(deliveries(&world).await, ["push:accepted:"]);
        assert_eq!(attempts(&world).await, ["rate_limited", "accepted"]);
        assert_eq!(
            world
                .scalar(
                    "SELECT retry_after_ms::bigint FROM merezha_notify.delivery_attempts \
                     WHERE attempt = 1"
                )
                .await,
            2000
        );
        let received = fake.received();
        assert_eq!(received.len(), 2);
        let gap = received[1].at.duration_since(received[0].at);
        assert!(gap >= Duration::from_secs(2), "{gap:?}");
        assert!(started.elapsed() < Duration::from_secs(20));
        instance.stop().await;
    }

    /// 401 and 403 are transient `push_auth`, at most 3 attempts, logged
    /// at error level.
    #[tokio::test]
    async fn push_auth_is_retried_three_times_then_fails() {
        let harness = merezha_telemetry::testing::Harness::new();
        let Some(world) = World::new().await else {
            return;
        };
        let fake = FakePushService::start().await;
        let keys = push::generate().unwrap();
        let mut opts = options(
            &format!("{}{}", crashing(), section("\"127.0.0.1\"", true, "")),
            &keys,
        );
        opts.meters = Some(harness.telemetry().meters());
        let instance = Instance::start(&world, opts).await;
        instance.tokens.add("ada", user("usr_ada", &[], None));
        assert_eq!(
            subscribe(&instance, "ada", fake.subscription("a")).await.0,
            201
        );
        fake.respond(403);
        commit(&instance, request("usr_ada", "u:1", assigned())).await;
        wait_settled(&instance, 1).await;
        assert_eq!(deliveries(&world).await, ["push:failed:push_auth"]);
        assert_eq!(
            attempts(&world).await,
            ["transient", "transient", "permanent"]
        );
        assert_eq!(fake.received().len(), 3);
        assert_eq!(
            harness
                .metric("merezha.notification.provider_account_errors")
                .value(&[("notify.provider", "webpush"), ("notify.channel", "push")]),
            Some(3.0)
        );
        assert_eq!(
            world
                .scalar(
                    "SELECT consecutive_failures::bigint FROM merezha_notify.push_subscriptions"
                )
                .await,
            3
        );
        instance.stop().await;
    }
}

/// Run the push delivery handler of `instance` once, on `attempt` of 6.
async fn run_push(instance: &Instance, delivery: &str, attempt: u32) -> Result<(), String> {
    use merezha_platform::ports::{Channel, JobContext, JobHandler};
    let slot: merezha_notifications::notifier::Slot =
        Arc::new(std::sync::OnceLock::from(instance.core()));
    let (_cancel, cancelled) = tokio::sync::watch::channel(false);
    let ctx = JobContext {
        id: 0,
        kind: id("notify.deliver.push"),
        attempt,
        max_attempts: 6,
        idempotency_key: String::new(),
        deadline: tokio::time::Instant::now() + Duration::from_secs(20),
        cancel: cancelled,
        correlation_id: id("corr1"),
        causation_id: None,
        tenant: None,
    };
    merezha_notifications::handler::Deliver(slot, Channel::Push)
        .handle(ctx, json!({ "delivery_id": delivery, "generation": 0 }))
        .await
        .map_err(|failure| failure.code.clone())
}

/// Issue #66: during a rolling VAPID rotation, an instance on the new key
/// that leases a push for a subscription of the old key starts no attempt
/// and retries `vapid_key_mismatch`, so an instance holding the old key
/// sends it; on the job's last attempt the push is skipped
/// `no_subscription`, as before.
#[tokio::test]
async fn a_push_leased_by_an_instance_on_another_key_is_retried_for_its_own() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakePushService::start().await;
    let section = section("\"127.0.0.1\"", true, "");
    let (old_keys, new_keys) = (push::generate().unwrap(), push::generate().unwrap());
    fake.expect_vapid_key(&old_keys.public_key);
    // Delivery jobs are run by hand: their records are never relayed.
    let mut on_old = options(&section, &old_keys);
    on_old.relay = false;
    let old = Instance::start(&world, on_old).await;
    let mut on_new = options(&section, &new_keys);
    on_new.workers = false;
    let new = Instance::start(&world, on_new).await;
    old.tokens.add("ada", user("usr_ada", &[], None));
    assert_eq!(subscribe(&old, "ada", fake.subscription("k1")).await.0, 201);
    let world = &world;
    let queued = |key: &'static str| async move {
        world
            .text(&format!(
                "SELECT d.id::text FROM merezha_notify.deliveries d \
                 JOIN merezha_notify.notifications n ON n.id = d.notification_id \
                 WHERE n.dedupe_key = '{key}' AND d.status = 'queued'"
            ))
            .await
    };
    for key in ["rot:1", "rot:2"] {
        commit(&old, request("usr_ada", key, assigned())).await;
    }
    old.relay_now().await;
    old.until(
        "SELECT count(*) FROM merezha_notify.deliveries WHERE status = 'queued'",
        2,
    )
    .await;
    let first = queued("rot:1").await.remove(0);
    // The new key's instance retries without an attempt...
    assert_eq!(
        run_push(&new, &first, 1).await,
        Err("vapid_key_mismatch".to_owned())
    );
    assert_eq!(queued("rot:1").await, std::slice::from_ref(&first));
    assert!(attempts(world).await.is_empty());
    // The live chain shows on the row, so `reclaim_stuck` does not take it
    // for a lost one.
    assert_eq!(
        world
            .scalar(&format!(
                "SELECT count(*) FROM merezha_notify.deliveries \
                 WHERE id = '{first}' AND updated_at > created_at"
            ))
            .await,
        1
    );
    assert!(fake.received().is_empty());
    // ... and the old key's instance sends it.
    assert_eq!(run_push(&old, &first, 2).await, Ok(()));
    let received = fake.received();
    assert_eq!(received.len(), 1);
    assert_eq!(received[0].problem, None, "{:?}", received[0]);
    // On the last attempt the mismatch skips the push.
    let second = queued("rot:2").await.remove(0);
    assert_eq!(run_push(&new, &second, 6).await, Ok(()));
    assert_eq!(
        deliveries(world).await,
        ["push:accepted:", "push:skipped:no_subscription"]
    );
    assert_eq!(fake.received().len(), 1);
    old.stop().await;
}
