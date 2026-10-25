//! The NT-N2 routes behind the platform edge against real PostgreSQL:
//! `send`, `status`, `redeliver`, the inbox, preferences, unsubscribe and
//! suppressions, and the inbox expiry task.
mod harness;
use axum::{body::Body, http::Request};
use harness::*;
use merezha_notifications::{keys::Unsubscribe, maintenance::Task};
use merezha_platform::ports::PrincipalKind;
use serde_json::{Value, json};
use std::sync::Arc;

fn people() -> Arc<People> {
    let people = Arc::new(People::default());
    people.user("usr_ada", "ada@example.com", Some("Ada"), None);
    people.user("usr_bob", "bob@example.com", Some("Bob"), None);
    people.join("org_1", "usr_ada", &["member"]);
    people.join("org_2", "usr_ada", &["member"]);
    people
}

/// A `password_changed` send (security: email and inbox) to `recipients`.
fn send_body(recipients: Value, key: &str) -> Value {
    json!({
        "topic": "identity.password_changed",
        "recipients": recipients,
        "channels": ["email", "inbox"],
        "dedupe_key": key,
    })
}

async fn started(section: &str) -> Option<(World, Instance)> {
    let world = World::new().await?;
    let instance = Instance::start(&world, Options::new(section, people())).await;
    let tokens = &instance.tokens;
    tokens.add(
        "sender",
        principal(
            "key_sender",
            PrincipalKind::ApiKey,
            &["notifications.send", "notifications.status.read"],
            None,
        ),
    );
    tokens.add(
        "other",
        principal(
            "key_other",
            PrincipalKind::ApiKey,
            &["notifications.send"],
            None,
        ),
    );
    tokens.add(
        "admin",
        user(
            "usr_admin",
            &["notifications.admin", "notifications.suppressions.manage"],
            None,
        ),
    );
    tokens.add("ada", user("usr_ada", &[], None));
    tokens.add("ada_org", user("usr_ada", &[], Some("org_1")));
    tokens.add("bob", user("usr_bob", &[], None));
    Some((world, instance))
}

#[tokio::test]
async fn send_namespaces_keys_by_caller_and_checks_permissions_and_membership() {
    let Some((world, instance)) = started(SECTION).await else {
        return;
    };
    let body = send_body(json!([{"user": {"id": "usr_ada"}}]), "order-7");
    let (status, _) = instance
        .call("POST", "/v1/notifications/send", None, Some(body.clone()))
        .await;
    assert_eq!(status.as_u16(), 401);
    let (status, reply) = instance
        .call(
            "POST",
            "/v1/notifications/send",
            Some("ada"),
            Some(body.clone()),
        )
        .await;
    assert_eq!(
        (status.as_u16(), reply["error"]["code"].clone()),
        (403, json!("unauthorized"))
    );
    let (status, first) = instance
        .call(
            "POST",
            "/v1/notifications/send",
            Some("sender"),
            Some(body.clone()),
        )
        .await;
    assert_eq!(status.as_u16(), 202, "{first}");
    assert_eq!(first["duplicate"], false);
    let (_, again) = instance
        .call(
            "POST",
            "/v1/notifications/send",
            Some("sender"),
            Some(body.clone()),
        )
        .await;
    assert_eq!(again["notification_id"], first["notification_id"]);
    assert_eq!(again["duplicate"], true);
    // Another caller with the same key gets its own notification (R4a).
    let (status, other) = instance
        .call("POST", "/v1/notifications/send", Some("other"), Some(body))
        .await;
    assert_eq!(status.as_u16(), 202);
    assert_ne!(other["notification_id"], first["notification_id"]);
    instance.settle().await;
    assert_eq!(
        world
            .text("SELECT source || ':' || dedupe_key FROM merezha_notify.notifications ORDER BY 1")
            .await,
        ["api:api:key_other:order-7", "api:api:key_sender:order-7"]
    );
    // A tenant send checks the user recipients' membership first.
    let mut tenant = send_body(
        json!([{"user": "usr_ada"}, {"user": {"id": "usr_bob"}}]),
        "t1",
    );
    tenant["tenant"] = json!("org_1");
    let (status, reply) = instance
        .call(
            "POST",
            "/v1/notifications/send",
            Some("sender"),
            Some(tenant.clone()),
        )
        .await;
    assert_eq!(
        (status.as_u16(), reply["error"]["code"].clone()),
        (422, json!("recipient_not_in_tenant"))
    );
    tenant["recipients"] = json!([{"user": "usr_ada"}]);
    let (status, _) = instance
        .call(
            "POST",
            "/v1/notifications/send",
            Some("sender"),
            Some(tenant),
        )
        .await;
    assert_eq!(status.as_u16(), 202);
    // Keys that do not fit once namespaced, and other caller errors.
    for (body, code) in [
        (
            send_body(json!([{"user": "usr_ada"}]), &"k".repeat(150)),
            "invalid_dedupe_key",
        ),
        (
            send_body(json!([{"user": "usr_ada"}]), "has space"),
            "invalid_dedupe_key",
        ),
        (
            json!({"topic": "tasks.nope", "recipients": [{"user": "usr_ada"}], "dedupe_key": "x"}),
            "unknown_topic",
        ),
        (
            json!({"topic": "identity.password_changed", "recipients": [{"user": "usr_ada"}],
                   "channels": ["sms"], "dedupe_key": "x"}),
            "channel_not_allowed",
        ),
        (
            json!({"topic": "identity.password_changed", "recipients": [{"address": {"email": "nope"}}],
                   "dedupe_key": "x"}),
            "invalid_recipient",
        ),
        (
            json!({"topic": "identity.password_changed", "recipients": [{"user": "usr_ada"}],
                   "data": {"secret": 1}, "dedupe_key": "x"}),
            "reserved_data_key",
        ),
    ] {
        let (status, reply) = instance
            .call("POST", "/v1/notifications/send", Some("sender"), Some(body))
            .await;
        assert_eq!(
            (status.as_u16(), reply["error"]["code"].as_str()),
            (422, Some(code)),
            "{reply}"
        );
    }
    // The Directory failing is the caller's transient error.
    let failing = Instance::start(
        &world,
        Options::new(SECTION, {
            let people = people();
            people
                .failing
                .store(true, std::sync::atomic::Ordering::SeqCst);
            people
        }),
    )
    .await;
    failing.tokens.add(
        "sender",
        principal(
            "key_sender",
            PrincipalKind::ApiKey,
            &["notifications.send"],
            None,
        ),
    );
    let mut tenant = send_body(json!([{"user": "usr_ada"}]), "t2");
    tenant["tenant"] = json!("org_1");
    let (status, reply) = failing
        .call(
            "POST",
            "/v1/notifications/send",
            Some("sender"),
            Some(tenant),
        )
        .await;
    assert_eq!(
        (status.as_u16(), reply["error"]["code"].clone()),
        (503, json!("unavailable"))
    );
    failing.stop().await;
    instance.stop().await;
}

#[tokio::test]
async fn status_masks_targets_and_redeliver_sends_a_new_generation() {
    let Some((world, instance)) = started(SECTION).await else {
        return;
    };
    let body = send_body(
        json!([{"user": "usr_ada"}, {"address": {"email": "carol@example.org", "name": "Carol"}}]),
        "s1",
    );
    let (_, sent) = instance
        .call("POST", "/v1/notifications/send", Some("sender"), Some(body))
        .await;
    instance.settle().await;
    let id = sent["notification_id"].as_str().unwrap().to_owned();
    let (status, _) = instance
        .call(
            "GET",
            &format!("/v1/notifications/status/{id}"),
            Some("other"),
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 403);
    // `status.read` covers the caller's own sends only; admin reads all.
    instance.tokens.add(
        "reader",
        principal(
            "key_reader",
            PrincipalKind::ApiKey,
            &["notifications.status.read"],
            None,
        ),
    );
    let path = format!("/v1/notifications/status/{id}");
    let (status, _) = instance.call("GET", &path, Some("reader"), None).await;
    assert_eq!(status.as_u16(), 404, "another caller's notification");
    let (status, _) = instance.call("GET", &path, Some("admin"), None).await;
    assert_eq!(status.as_u16(), 200, "notifications.admin reads every one");
    let (status, report) = instance
        .call(
            "GET",
            &format!("/v1/notifications/status/{id}"),
            Some("sender"),
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 200, "{report}");
    assert_eq!(report["status"], "expanded");
    let targets: Vec<(String, String, String)> = report["deliveries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            (
                d["channel"].as_str().unwrap().to_owned(),
                d["status"].as_str().unwrap().to_owned(),
                d["target"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        targets,
        [
            ("email".into(), "accepted".into(), "a***@example.com".into()),
            ("email".into(), "accepted".into(), "c***@example.org".into()),
            ("inbox".into(), "delivered".into(), "inbox".into()),
        ]
    );
    assert!(!report.to_string().contains("ada@example.com"));
    let (status, _) = instance
        .call(
            "GET",
            "/v1/notifications/status/00000000-0000-0000-0000-000000000000",
            Some("sender"),
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 404);
    // Redelivery: only from failed, unknown or expired.
    let delivery = report["deliveries"][0]["id"].as_str().unwrap().to_owned();
    let path = format!("/v1/notifications/deliveries/{delivery}/redeliver");
    let (status, reply) = instance.call("POST", &path, Some("admin"), None).await;
    assert_eq!(
        (status.as_u16(), reply["error"]["code"].clone()),
        (409, json!("not_redeliverable"))
    );
    world
        .execute(&format!(
            "UPDATE merezha_notify.deliveries SET status = 'failed', status_reason = 'x' \
             WHERE id = '{delivery}'"
        ))
        .await;
    let (status, _) = instance.call("POST", &path, Some("sender"), None).await;
    assert_eq!(status.as_u16(), 403);
    let (status, reply) = instance.call("POST", &path, Some("admin"), None).await;
    assert_eq!(
        (status.as_u16(), reply.clone()),
        (202, json!({"generation": 1}))
    );
    instance.settle().await;
    assert_eq!(
        world
            .text(&format!(
                "SELECT status || ':' || generation::text FROM merezha_notify.deliveries \
                 WHERE id = '{delivery}'"
            ))
            .await,
        ["accepted:1"]
    );
    // The new generation is a new idempotency key: a second capture.
    assert_eq!(
        world
            .scalar(&format!(
                "SELECT count(*) FROM merezha_notify.captures WHERE delivery_id = '{delivery}'"
            ))
            .await,
        2
    );
    instance.stop().await;
}

/// NT-N2 tests: the inbox for its user only; another user's item is 404.
#[tokio::test]
async fn the_inbox_lists_and_changes_only_the_callers_items_in_counter_order() {
    let Some((world, instance)) = started(SECTION).await else {
        return;
    };
    // Bob has an inbox of his own, so his counter exists.
    let (status, _) = instance
        .call(
            "POST",
            "/v1/notifications/send",
            Some("sender"),
            Some(json!({
                "topic": "identity.password_changed",
                "recipients": [{"user": "usr_bob"}],
                "channels": ["inbox"],
                "dedupe_key": "bob-1",
            })),
        )
        .await;
    assert_eq!(status.as_u16(), 202);
    for n in 1..=3 {
        let (status, _) = instance
            .call(
                "POST",
                "/v1/notifications/send",
                Some("sender"),
                Some(json!({
                    "topic": "identity.password_changed",
                    "recipients": [{"user": "usr_ada"}],
                    "channels": ["inbox"],
                    "dedupe_key": format!("i{n}"),
                })),
            )
            .await;
        assert_eq!(status.as_u16(), 202);
    }
    instance.settle().await;
    let (status, page) = instance
        .call("GET", "/v1/notifications/inbox?limit=2", Some("ada"), None)
        .await;
    assert_eq!(status.as_u16(), 200, "{page}");
    let items = page["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    assert_eq!(items[0]["seq"], 3, "newest first");
    assert_eq!(items[0]["title"], "Your password was changed");
    let cursor = page["next_cursor"].as_str().unwrap().to_owned();
    let (_, rest) = instance
        .call(
            "GET",
            &format!("/v1/notifications/inbox?limit=2&cursor={cursor}"),
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(rest["items"].as_array().unwrap().len(), 1);
    assert_eq!(rest["next_cursor"], Value::Null);
    let (status, _) = instance
        .call(
            "GET",
            "/v1/notifications/inbox?cursor=abc",
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 400);
    let (_, unread) = instance
        .call(
            "GET",
            "/v1/notifications/inbox/unread-count",
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(unread, json!({"unread": 3}));
    let first = items[0]["id"].as_str().unwrap().to_owned();
    // Another user's item answers 404 on every change, as a missing one.
    for action in ["read", "unread", "archive"] {
        let (status, _) = instance
            .call(
                "POST",
                &format!("/v1/notifications/inbox/{first}/{action}"),
                Some("bob"),
                None,
            )
            .await;
        assert_eq!(status.as_u16(), 404, "{action}");
    }
    let (status, _) = instance
        .call("GET", "/v1/notifications/inbox", None, None)
        .await;
    assert_eq!(status.as_u16(), 401);
    let (status, read) = instance
        .call(
            "POST",
            &format!("/v1/notifications/inbox/{first}/read"),
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 200);
    assert_eq!(read["change_seq"], 4, "the next counter value");
    assert!(read["read_at"].is_string());
    // Reading again changes nothing and takes no number.
    let (_, again) = instance
        .call(
            "POST",
            &format!("/v1/notifications/inbox/{first}/read"),
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(again["change_seq"], 4);
    let (_, unread) = instance
        .call(
            "GET",
            "/v1/notifications/inbox?state=unread",
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(unread["items"].as_array().unwrap().len(), 2);
    let (_, all) = instance
        .call(
            "POST",
            "/v1/notifications/inbox/read-all",
            Some("ada"),
            Some(json!({})),
        )
        .await;
    assert_eq!(all, json!({"updated": 2}));
    let (_, archived) = instance
        .call(
            "POST",
            &format!("/v1/notifications/inbox/{first}/archive"),
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(archived["change_seq"], 7);
    assert_eq!(
        world
            .text(
                "SELECT string_agg(change_seq::text, ',' ORDER BY change_seq) \
                 FROM merezha_notify.inbox_items WHERE user_id = 'usr_ada'"
            )
            .await,
        ["5,6,7"]
    );
    assert_eq!(
        world
            .scalar("SELECT last_seq FROM merezha_notify.inbox_counters WHERE user_id = 'usr_ada'")
            .await,
        7
    );
    let (_, page) = instance
        .call("GET", "/v1/notifications/inbox", Some("ada"), None)
        .await;
    assert_eq!(
        page["items"].as_array().unwrap().len(),
        2,
        "archived items are hidden"
    );
    let (_, bob) = instance
        .call("GET", "/v1/notifications/inbox", Some("bob"), None)
        .await;
    let bob = bob["items"].as_array().unwrap();
    assert_eq!(bob.len(), 1);
    assert!(bob[0]["read_at"].is_null(), "Bob's own item is untouched");
    instance.stop().await;
}

/// Tenant isolation (NT §5.4): a principal sees the items of its active
/// tenant and the tenantless ones; without an active tenant only the
/// tenantless ones; another tenant is refused or not found.
#[tokio::test]
async fn inbox_items_are_isolated_by_the_active_tenant_and_expire_with_purged_through() {
    let Some((world, instance)) = started(SECTION).await else {
        return;
    };
    instance
        .tokens
        .add("ada_org2", user("usr_ada", &[], Some("org_2")));
    let mut ids = std::collections::BTreeMap::new();
    for (key, tenant) in [
        ("t-org1", Some("org_1")),
        ("t-org2", Some("org_2")),
        ("t-none", None),
    ] {
        let mut body = json!({
            "topic": "identity.password_changed",
            "recipients": [{"user": "usr_ada"}],
            "channels": ["inbox"],
            "dedupe_key": key,
        });
        if let Some(tenant) = tenant {
            body["tenant"] = json!(tenant);
        }
        let (status, reply) = instance
            .call("POST", "/v1/notifications/send", Some("sender"), Some(body))
            .await;
        assert_eq!(status.as_u16(), 202, "{reply}");
    }
    instance.settle().await;
    for row in world
        .text(
            "SELECT coalesce(tenant_id, 'none') || '=' || id::text FROM merezha_notify.inbox_items",
        )
        .await
    {
        let (tenant, id) = row.split_once('=').unwrap();
        ids.insert(tenant.to_owned(), id.to_owned());
    }
    let tenants_of = |page: &Value| -> Vec<String> {
        let mut found: Vec<String> = page["items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["tenant_id"].as_str().unwrap_or("none").to_owned())
            .collect();
        found.sort();
        found
    };
    let instance = &instance;
    let list = |token: &'static str, query: &'static str| async move {
        instance
            .call(
                "GET",
                &format!("/v1/notifications/inbox{query}"),
                Some(token),
                None,
            )
            .await
    };
    let (_, page) = list("ada", "").await;
    assert_eq!(
        tenants_of(&page),
        ["none"],
        "no active tenant: tenantless only"
    );
    let (_, page) = list("ada_org", "").await;
    assert_eq!(tenants_of(&page), ["none", "org_1"]);
    let (_, page) = list("ada_org", "?tenant=org_1").await;
    assert_eq!(tenants_of(&page), ["none", "org_1"]);
    for (token, query) in [
        ("ada_org", "?tenant=org_2"),
        ("ada", "?tenant=org_1"),
        ("ada_org", "/unread-count?tenant=org_2"),
    ] {
        let (status, reply) = list(token, query).await;
        assert_eq!(
            (status.as_u16(), reply["error"]["code"].clone()),
            (403, json!("unauthorized")),
            "{token} {query}"
        );
    }
    let (_, unread) = list("ada", "/unread-count").await;
    assert_eq!(unread, json!({"unread": 1}));
    let (_, unread) = list("ada_org", "/unread-count").await;
    assert_eq!(unread, json!({"unread": 2}));
    // Another tenant's item is not found on every change.
    for action in ["read", "unread", "archive"] {
        let (status, _) = instance
            .call(
                "POST",
                &format!("/v1/notifications/inbox/{}/{action}", ids["org_2"]),
                Some("ada_org"),
                None,
            )
            .await;
        assert_eq!(status.as_u16(), 404, "{action}");
    }
    let (status, _) = instance
        .call(
            "POST",
            &format!("/v1/notifications/inbox/{}/read", ids["org_1"]),
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(
        status.as_u16(),
        404,
        "a tenant item without its tenant active"
    );
    let (status, _) = instance
        .call(
            "POST",
            "/v1/notifications/inbox/read-all",
            Some("ada_org"),
            Some(json!({"tenant": "org_2"})),
        )
        .await;
    assert_eq!(status.as_u16(), 403);
    let (_, all) = instance
        .call(
            "POST",
            "/v1/notifications/inbox/read-all",
            Some("ada_org"),
            Some(json!({})),
        )
        .await;
    assert_eq!(all, json!({"updated": 2}));
    let (_, unread) = list("ada_org2", "?state=unread").await;
    assert_eq!(tenants_of(&unread), ["org_2"], "org_2's item is untouched");
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.inbox_items WHERE read_at IS NULL")
            .await,
        1
    );
    // Expiry deletes in user order and raises purged_through.
    let expiring = world
        .scalar("SELECT change_seq FROM merezha_notify.inbox_items WHERE tenant_id = 'org_2'")
        .await;
    world
        .execute(
            "UPDATE merezha_notify.inbox_items SET expires_at = now() - interval '1 second' \
             WHERE tenant_id = 'org_2'",
        )
        .await;
    let core = instance.core();
    let deleted = merezha_notifications::maintenance::run(&core, Task::InboxExpire, soon())
        .await
        .unwrap();
    assert_eq!(deleted, 1);
    let last = world
        .scalar("SELECT last_seq FROM merezha_notify.inbox_counters")
        .await;
    assert_eq!(
        world
            .scalar("SELECT purged_through FROM merezha_notify.inbox_counters")
            .await,
        expiring
    );
    assert_eq!(last, 5, "three items, then two read");
    instance.stop().await;
}

fn token_for(instance: &Instance, claims: &Unsubscribe) -> String {
    instance.core().keys.unsubscribe_token(claims)
}

/// NT-N2 test: a forged unsubscribe MAC gets 400.
#[tokio::test]
async fn unsubscribe_tokens_write_preferences_or_suppressions_and_forgeries_fail() {
    let section = format!(
        "{SECTION}[modules.notifications.topics.\"tasks.assigned\"]\n\
         category = \"transactional\"\nchannels = [\"email\"]\ntemplate = \"identity.password_changed\"\n"
    );
    let Some((world, instance)) = started(&section).await else {
        return;
    };
    let user_token = token_for(
        &instance,
        &Unsubscribe {
            u: Some("usr_ada".into()),
            a: None,
            t: "tasks.assigned".into(),
            c: "email".into(),
            iat: 1,
        },
    );
    let address_token = token_for(
        &instance,
        &Unsubscribe {
            u: None,
            a: Some("carol@example.org".into()),
            t: "tasks.assigned".into(),
            c: "email".into(),
            iat: 1,
        },
    );
    // Tokens name a configured topic users can turn off.
    for topic in ["identity.password_changed", "tasks.nope"] {
        let token = token_for(
            &instance,
            &Unsubscribe {
                u: Some("usr_ada".into()),
                a: None,
                t: topic.into(),
                c: "email".into(),
                iat: 1,
            },
        );
        let (status, _) = instance
            .call(
                "POST",
                "/v1/notifications/unsubscribe",
                None,
                Some(json!({"token": token})),
            )
            .await;
        assert_eq!(status.as_u16(), 400, "{topic}");
    }
    let (signed, mac) = user_token.rsplit_once('.').unwrap();
    let forged = format!("{signed}.{}", mac.chars().rev().collect::<String>());
    let (status, reply) = instance
        .call(
            "POST",
            "/v1/notifications/unsubscribe",
            None,
            Some(json!({"token": forged})),
        )
        .await;
    assert_eq!(
        (status.as_u16(), reply["error"]["code"].clone()),
        (400, json!("token_invalid"))
    );
    let (status, _) = instance
        .call(
            "GET",
            &format!("/v1/notifications/unsubscribe?token={forged}"),
            None,
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 400);
    // GET changes nothing: mail scanners prefetch links.
    let (status, page) = instance
        .call(
            "GET",
            &format!("/v1/notifications/unsubscribe?token={user_token}"),
            None,
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 200);
    assert!(page.as_str().unwrap().contains("<form method=\"post\""));
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.preferences")
            .await,
        0
    );
    // RFC 8058 one-click: form body, token in the query.
    let request = Request::post(format!("/v1/notifications/unsubscribe?token={user_token}"))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from("List-Unsubscribe=One-Click"))
        .unwrap();
    let (status, reply) = instance.raw(request).await;
    assert_eq!((status.as_u16(), reply), (200, json!({})));
    let (status, _) = instance
        .call(
            "POST",
            "/v1/notifications/unsubscribe",
            None,
            Some(json!({"token": address_token})),
        )
        .await;
    assert_eq!(status.as_u16(), 200);
    assert_eq!(
        world
            .text(
                "SELECT user_id || ':' || topic || ':' || channel || ':' || enabled::text \
                 FROM merezha_notify.preferences"
            )
            .await,
        ["usr_ada:tasks.assigned:email:false"]
    );
    assert_eq!(
        world
            .text(
                "SELECT address || ':' || topic || ':' || reason FROM merezha_notify.suppressions"
            )
            .await,
        ["carol@example.org:tasks.assigned:unsubscribed"]
    );
    // The next transactional mail to both is skipped.
    let (status, _) = instance
        .call(
            "POST",
            "/v1/notifications/send",
            Some("sender"),
            Some(json!({
                "topic": "tasks.assigned",
                "recipients": [{"user": "usr_ada"}, {"address": {"email": "Carol@example.org"}}],
                "dedupe_key": "after-unsubscribe",
            })),
        )
        .await;
    assert_eq!(status.as_u16(), 202);
    instance.settle().await;
    assert_eq!(
        world
            .text("SELECT status || ':' || status_reason FROM merezha_notify.deliveries ORDER BY 1")
            .await,
        ["skipped:preference", "skipped:suppressed"]
    );
    instance.stop().await;
}

#[tokio::test]
async fn preferences_are_read_and_written_for_configurable_topics_only() {
    let section = format!(
        "{SECTION}[modules.notifications.topics.\"tasks.assigned\"]\n\
         category = \"transactional\"\nchannels = [\"inbox\", \"email\"]\n\
         default_channels = [\"inbox\"]\ntemplate = \"identity.password_changed\"\n\
         description = \"Someone assigned you a task\"\n"
    );
    let Some((_world, instance)) = started(&section).await else {
        return;
    };
    let (status, prefs) = instance
        .call("GET", "/v1/notifications/preferences", Some("ada"), None)
        .await;
    assert_eq!(status.as_u16(), 200, "{prefs}");
    // Security topics are hidden; the transactional default applies.
    let topics: Vec<&str> = prefs["topics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["topic"].as_str().unwrap())
        .collect();
    assert!(topics.contains(&"tasks.assigned"));
    assert!(!topics.iter().any(|t| t.starts_with("identity.")));
    let assigned = prefs["topics"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["topic"] == "tasks.assigned")
        .unwrap()
        .clone();
    assert_eq!(
        assigned["channels"],
        json!([
            {"channel": "inbox", "enabled": true, "configurable": true},
            {"channel": "email", "enabled": false, "configurable": true},
        ])
    );
    let (status, reply) = instance
        .call(
            "PUT",
            "/v1/notifications/preferences",
            Some("ada"),
            Some(json!({"preferences": [
                {"topic": "identity.password_changed", "channel": "email", "enabled": false}
            ]})),
        )
        .await;
    assert_eq!(
        (status.as_u16(), reply["error"]["code"].clone()),
        (422, json!("topic_not_configurable"))
    );
    assert_eq!(reply["error"]["details"][0]["path"], "/preferences/0/topic");
    let (status, reply) = instance
        .call(
            "PUT",
            "/v1/notifications/preferences",
            Some("ada"),
            Some(json!({"settings": {"timezone": "Mars/Olympus"}})),
        )
        .await;
    assert_eq!(
        (status.as_u16(), reply["error"]["code"].clone()),
        (422, json!("invalid_timezone"))
    );
    let (status, updated) = instance
        .call(
            "PUT",
            "/v1/notifications/preferences",
            Some("ada"),
            Some(json!({
                "preferences": [
                    {"topic": "tasks.assigned", "channel": "email", "enabled": true},
                    {"topic": "*", "channel": "inbox", "enabled": false}
                ],
                "settings": {"timezone": "Europe/Paris"}
            })),
        )
        .await;
    assert_eq!(status.as_u16(), 200, "{updated}");
    assert_eq!(updated["settings"], json!({"timezone": "Europe/Paris"}));
    let assigned = updated["topics"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["topic"] == "tasks.assigned")
        .unwrap()
        .clone();
    assert_eq!(assigned["channels"][0]["enabled"], false, "the '*' row");
    assert_eq!(assigned["channels"][1]["enabled"], true, "the topic row");
    // An API key acts for no user.
    let (status, _) = instance
        .call("GET", "/v1/notifications/preferences", Some("sender"), None)
        .await;
    assert_eq!(status.as_u16(), 403);
    instance.stop().await;
}

#[tokio::test]
async fn suppressions_are_managed_with_the_permission() {
    let Some((world, instance)) = started(SECTION).await else {
        return;
    };
    let body = json!({"channel": "email", "address": "Dave@Example.com", "reason": "manual"});
    let (status, _) = instance
        .call(
            "POST",
            "/v1/notifications/suppressions",
            Some("ada"),
            Some(body.clone()),
        )
        .await;
    assert_eq!(status.as_u16(), 403);
    let (status, created) = instance
        .call(
            "POST",
            "/v1/notifications/suppressions",
            Some("admin"),
            Some(body.clone()),
        )
        .await;
    assert_eq!(status.as_u16(), 201, "{created}");
    assert_eq!(created["address"], "dave@example.com");
    let (_, again) = instance
        .call(
            "POST",
            "/v1/notifications/suppressions",
            Some("admin"),
            Some(body),
        )
        .await;
    assert_eq!(again["id"], created["id"], "one suppression per key");
    let (status, reply) = instance
        .call(
            "POST",
            "/v1/notifications/suppressions",
            Some("admin"),
            Some(json!({"channel": "sms", "address": "0612", "reason": "manual"})),
        )
        .await;
    assert_eq!(status.as_u16(), 422, "{reply}");
    let (_, list) = instance
        .call(
            "GET",
            "/v1/notifications/suppressions?channel=email&address=dave@example.com",
            Some("admin"),
            None,
        )
        .await;
    assert_eq!(list["items"].as_array().unwrap().len(), 1);
    let id = created["id"].as_str().unwrap();
    let (status, _) = instance
        .call(
            "DELETE",
            &format!("/v1/notifications/suppressions/{id}"),
            Some("admin"),
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 204);
    let (status, _) = instance
        .call(
            "DELETE",
            &format!("/v1/notifications/suppressions/{id}"),
            Some("admin"),
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 404);
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.suppressions")
            .await,
        0
    );
    instance.stop().await;
}

#[tokio::test]
async fn send_bounds_not_before_and_expires_at() {
    let Some((world, instance)) = started(SECTION).await else {
        return;
    };
    let now = jiff::Timestamp::now();
    let at = |days: i64| (now + jiff::SignedDuration::from_hours(days * 24)).to_string();
    for (not_before, expires_at, path, code) in [
        (at(31), None, "/not_before", "too_far"),
        (at(2), Some(at(2)), "/expires_at", "not_after_not_before"),
        (at(2), Some(at(1)), "/expires_at", "not_after_not_before"),
    ] {
        let mut body = send_body(json!([{"user": "usr_ada"}]), "b1");
        body["not_before"] = json!(not_before);
        if let Some(expires_at) = expires_at {
            body["expires_at"] = json!(expires_at);
        }
        let (status, reply) = instance
            .call("POST", "/v1/notifications/send", Some("sender"), Some(body))
            .await;
        assert_eq!(status.as_u16(), 422, "{reply}");
        assert_eq!(reply["error"]["code"], "validation_failed");
        assert_eq!(reply["error"]["details"][0]["path"], path);
        assert_eq!(reply["error"]["details"][0]["code"], code);
    }
    let mut body = send_body(json!([{"user": "usr_ada"}]), "b2");
    body["not_before"] = json!(at(29));
    body["expires_at"] = json!(at(30));
    let (status, _) = instance
        .call("POST", "/v1/notifications/send", Some("sender"), Some(body))
        .await;
    assert_eq!(status.as_u16(), 202);
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_jobs.outbox WHERE run_at > now()")
            .await,
        1
    );
    instance.stop().await;
}
