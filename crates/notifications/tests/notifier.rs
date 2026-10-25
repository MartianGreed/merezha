//! NT-N2 against real PostgreSQL: the `Notifier` port, expansion,
//! preferences, suppressions, the delivery worker over the capture driver,
//! `send`, `status`, `redeliver` and the maintenance tasks. Each test
//! creates its own database through `MEREZHA_TEST_DATABASE_URL` and skips
//! with a message when it is unset.
mod harness;
use harness::*;
use merezha_notifications::{faults, maintenance::Task};
use merezha_platform::ports::{
    Address, Channel, ChannelSelection, Correlation, DedupeKey, EmailAddress, EmptyDirectory,
    NotificationRequest, NotifyError, Recipient,
};
use serde_json::{Map, Value, json};
use std::{sync::Arc, time::Duration};

fn data(value: Value) -> Map<String, Value> {
    value.as_object().cloned().unwrap_or_default()
}

/// A request for `topic` to `recipients` under `key`.
fn request(topic: &str, recipients: Vec<Recipient>, key: &str) -> NotificationRequest {
    NotificationRequest {
        topic: id(topic),
        recipients,
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
fn to_user(user: &str) -> Recipient {
    Recipient::User { id: id(user) }
}
fn to_email(address: &str, user: Option<&str>) -> Recipient {
    Recipient::Address(Address::Email {
        address: EmailAddress::new(address).unwrap(),
        name: None,
        user: user.map(id),
        locale: None,
    })
}

/// `password_changed` (security, email and inbox) needs `display_name`.
fn password_changed(recipients: Vec<Recipient>, key: &str) -> NotificationRequest {
    let mut request = request("identity.password_changed", recipients, key);
    request.channels = ChannelSelection::Only(vec![Channel::Email, Channel::Inbox]);
    request.data = data(json!({"display_name": "Ada"}));
    request
}

/// Commit one request from `source` in its own transaction.
async fn commit(
    instance: &Instance,
    source: &str,
    request: NotificationRequest,
) -> Result<merezha_platform::ports::NotificationHandle, NotifyError> {
    let notifier = instance.notifier(source);
    instance
        .db
        .transaction(soon(), move |tx| {
            Box::pin(async move { Ok(notifier.request(tx, request).await) })
        })
        .await
        .unwrap()
}

/// `(channel, status, status_reason)` of every delivery, ordered.
async fn deliveries(world: &World) -> Vec<String> {
    world
        .text(
            "SELECT channel || ':' || status || ':' || coalesce(status_reason, '') \
             FROM merezha_notify.deliveries ORDER BY channel, target_key",
        )
        .await
}

/// A section with a transactional and a marketing topic of the project.
const TOPICS: &str = "[modules.notifications.topics.\"tasks.assigned\"]\n\
    category = \"transactional\"\nchannels = [\"inbox\", \"email\"]\ndefault_channels = [\"inbox\", \"email\"]\n\
    [modules.notifications.topics.\"tasks.digest\"]\n\
    category = \"marketing\"\nchannels = [\"inbox\", \"email\"]\ndefault_channels = [\"inbox\"]\n";

fn project_templates(dir: &std::path::Path) {
    for (topic, title) in [("tasks.assigned", "Assigned"), ("tasks.digest", "Digest")] {
        let base = dir.join("notifications/templates").join(topic);
        std::fs::create_dir_all(base.join("en")).unwrap();
        std::fs::write(
            base.join("template.toml"),
            format!("description = \"{title}\"\n[preview]\ntitle = \"T\"\n"),
        )
        .unwrap();
        std::fs::write(
            base.join("en/subject.txt"),
            format!("{title}: {{{{ title }}}}"),
        )
        .unwrap();
        std::fs::write(
            base.join("en/body.txt"),
            "{{ title }} for {{ recipient.name }}",
        )
        .unwrap();
        std::fs::write(
            base.join("en/inbox.toml"),
            "title = \"{{ title }}\"\nbody = \"For {{ recipient.name }}\"\n",
        )
        .unwrap();
    }
}

/// A section whose templates live in a directory the test owns.
fn with_templates(extra: &str) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().unwrap();
    project_templates(dir.path());
    let section = format!(
        "{SECTION}{extra}[modules.notifications.templates]\ndir = \"{}\"\n{TOPICS}",
        dir.path().join("notifications/templates").display()
    );
    (dir, section)
}

#[tokio::test]
async fn the_same_dedupe_key_twice_gives_one_notification_with_the_same_id() {
    let Some(world) = World::new().await else {
        return;
    };
    let people = Arc::new(People::default());
    people.user("usr_ada", "ada@example.com", Some("Ada"), None);
    let instance = Instance::start(&world, Options::new(SECTION, people)).await;
    let first = commit(
        &instance,
        "identity",
        password_changed(vec![to_user("usr_ada")], "identity.password_changed:a1"),
    )
    .await
    .unwrap();
    let second = commit(
        &instance,
        "identity",
        password_changed(vec![to_user("usr_ada")], "identity.password_changed:a1"),
    )
    .await
    .unwrap();
    assert_eq!(first.notification_id, second.notification_id);
    assert!(!first.duplicate);
    assert!(second.duplicate);
    instance.settle().await;
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.notifications")
            .await,
        1
    );
    assert_eq!(
        world
            .text("SELECT id::text FROM merezha_notify.notifications")
            .await,
        vec![first.notification_id.to_string()]
    );
    // Security mail ignores preferences; the inbox item was written with it.
    assert_eq!(
        deliveries(&world).await,
        ["email:accepted:", "inbox:delivered:"]
    );
    instance.stop().await;
}

/// Tests that race a lease deadline (issue #37): CI runs them alone, one
/// at a time (`cargo test -- --test-threads=1 timing::`).
mod timing {
    use super::*;

    /// NT-N2 test: duplicate delivery. The first expansion run commits and then
    /// hangs 5 s, past its 1 s lease (no heartbeat); the reaper re-offers the job and
    /// the second run finds the notification and writes nothing.
    #[tokio::test]
    async fn duplicate_delivery_an_expansion_run_twice_through_lease_expiry_writes_once() {
        let Some(world) = World::new().await else {
            return;
        };
        let people = Arc::new(People::default());
        people.user("usr_ada", "ada@example.com", Some("Ada"), None);
        let mut options = Options::new(
            &format!(
                "[jobs.queues.\"notify.expand\"]\nconcurrency = 2\nlease_ms = 1000\n\
                 backoff_base_ms = 10\n{SECTION}"
            ),
            people,
        );
        options.heartbeat = false;
        options
            .faults
            .pause_once(faults::AFTER_EXPANSION_COMMIT, Duration::from_secs(5));
        let instance = Instance::start(&world, options).await;
        commit(
            &instance,
            "identity",
            password_changed(vec![to_user("usr_ada")], "identity.password_changed:dup"),
        )
        .await
        .unwrap();
        let counts = "SELECT (SELECT count(*) FROM merezha_notify.notifications) * 1000000 \
            + (SELECT count(*) FROM merezha_notify.deliveries) * 10000 \
            + (SELECT count(*) FROM merezha_notify.inbox_items) * 100 \
            + (SELECT count(*) FROM merezha_jobs.outbox WHERE source = 'notifications')";
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        while world
            .scalar("SELECT count(*) FROM merezha_notify.notifications")
            .await
            == 0
        {
            assert!(std::time::Instant::now() < deadline, "never expanded");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let after_first = world.scalar(counts).await;
        // Wait for the second run: two attempts, the first one's lease expired.
        let attempts = "SELECT count(*) FROM merezha_jobs.attempts a \
            JOIN merezha_jobs.jobs j ON j.id = a.job_id WHERE j.kind = 'notify.expand'";
        while world.scalar(attempts).await < 2
            || world
                .scalar(
                    "SELECT count(*) FROM merezha_jobs.jobs \
                     WHERE kind = 'notify.expand' AND state <> 'succeeded'",
                )
                .await
                > 0
        {
            assert!(std::time::Instant::now() < deadline, "never ran twice");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(
            world
                .scalar(
                    "SELECT count(*) FROM merezha_jobs.attempts WHERE outcome = 'lease_expired'"
                )
                .await,
            1
        );
        // Let the paused first run finish too, then compare.
        tokio::time::sleep(Duration::from_secs(5)).await;
        instance.settle().await;
        assert_eq!(world.scalar(counts).await, after_first);
        // The second run took no inbox number either.
        assert_eq!(
            world
                .scalar(
                    "SELECT last_seq FROM merezha_notify.inbox_counters WHERE user_id = 'usr_ada'"
                )
                .await,
            1
        );
        assert_eq!(
            after_first, 1_020_102,
            "1 notification, 2 deliveries, 1 inbox item, the request and the email job records"
        );
        instance.stop().await;
    }
}

#[tokio::test]
async fn a_caller_transaction_rolled_back_after_request_leaves_nothing() {
    let Some(world) = World::new().await else {
        return;
    };
    let people = Arc::new(People::default());
    people.user("usr_ada", "ada@example.com", Some("Ada"), None);
    let instance = Instance::start(&world, Options::new(SECTION, people)).await;
    let notifier = instance.notifier("identity");
    let request = password_changed(vec![to_user("usr_ada")], "identity.password_changed:rb");
    let result: Result<(), merezha_domain::Failure> = instance
        .db
        .transaction(soon(), move |tx| {
            Box::pin(async move {
                notifier
                    .request(tx, request)
                    .await
                    .map_err(merezha_domain::Failure::from)?;
                Err(merezha_domain::Failure::permanent(
                    "caller_changed_its_mind",
                ))
            })
        })
        .await;
    assert_eq!(result.unwrap_err().code, "caller_changed_its_mind");
    // Give the relay several intervals to find anything.
    tokio::time::sleep(Duration::from_millis(500)).await;
    instance.settle().await;
    for table in [
        "merezha_jobs.outbox",
        "merezha_jobs.jobs",
        "merezha_notify.notifications",
        "merezha_notify.deliveries",
        "merezha_notify.inbox_items",
    ] {
        let rows = world
            .scalar(&format!(
                "SELECT count(*) FROM {table} t WHERE t::text NOT LIKE '%notify.maintenance%'"
            ))
            .await;
        assert_eq!(rows, 0, "{table}");
    }
    instance.stop().await;
}

#[tokio::test]
async fn going_over_max_targets_rejects_the_notification() {
    let Some(world) = World::new().await else {
        return;
    };
    let people = Arc::new(People::default());
    for n in 1..=3 {
        people.user(
            &format!("usr_{n}"),
            &format!("u{n}@example.com"),
            None,
            None,
        );
        people.join("org_1", &format!("usr_{n}"), &["admin"]);
    }
    let section = format!("{SECTION}limits = {{ max_targets = 2, max_marketing_targets = 2 }}\n");
    let instance = Instance::start(&world, Options::new(&section, people)).await;
    let mut alert = request(
        "notifications.webhook_disabled",
        vec![Recipient::Tenant {
            id: id("org_1"),
            roles: None,
        }],
        "whdisable:e1:1",
    );
    alert.tenant = Some(id("org_1"));
    alert.data = data(json!({"endpoint_id": "e1", "endpoint_url": "https://x.example/h"}));
    alert.channels = ChannelSelection::Only(vec![Channel::Email]);
    commit(&instance, "notifications", alert).await.unwrap();
    instance.settle().await;
    assert_eq!(
        world
            .text(
                "SELECT status || ':' || status_reason || ':' || target_count::text \
                 FROM merezha_notify.notifications"
            )
            .await,
        ["rejected:too_many_targets:3"]
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.deliveries")
            .await,
        0
    );
    instance.stop().await;
}

#[tokio::test]
async fn marketing_needs_an_opt_in_and_security_ignores_a_disabled_preference() {
    let Some(world) = World::new().await else {
        return;
    };
    let people = Arc::new(People::default());
    people.user("usr_ada", "ada@example.com", Some("Ada"), None);
    people.user("usr_bob", "bob@example.com", Some("Bob"), None);
    let (_templates, section) = with_templates("");
    let instance = Instance::start(&world, Options::new(&section, people)).await;
    world
        .execute(
            "INSERT INTO merezha_notify.preferences VALUES \
             ('usr_bob', 'tasks.digest', 'inbox', true, now()), \
             ('usr_ada', 'identity.password_changed', 'inbox', false, now()), \
             ('usr_ada', 'identity.password_changed', 'email', false, now()), \
             ('usr_ada', '*', 'inbox', false, now())",
        )
        .await;
    let mut digest = request(
        "tasks.digest",
        vec![to_user("usr_ada"), to_user("usr_bob")],
        "digest:1",
    );
    digest.data = data(json!({"title": "Weekly"}));
    commit(&instance, "notifications", digest).await.unwrap();
    commit(
        &instance,
        "identity",
        password_changed(vec![to_user("usr_ada")], "identity.password_changed:p1"),
    )
    .await
    .unwrap();
    instance.settle().await;
    let rows = world
        .text(
            "SELECT n.topic || ':' || d.user_id || ':' || d.channel || ':' || d.status || ':' \
             || coalesce(d.status_reason, '') FROM merezha_notify.deliveries d \
             JOIN merezha_notify.notifications n ON n.id = d.notification_id \
             ORDER BY 1",
        )
        .await;
    assert_eq!(
        rows,
        [
            "identity.password_changed:usr_ada:email:accepted:",
            "identity.password_changed:usr_ada:inbox:delivered:",
            "tasks.digest:usr_ada:inbox:skipped:not_opted_in",
            "tasks.digest:usr_bob:inbox:delivered:",
        ]
    );
    instance.stop().await;
}

#[tokio::test]
async fn a_user_and_an_explicit_address_with_the_same_email_give_one_delivery() {
    let Some(world) = World::new().await else {
        return;
    };
    let people = Arc::new(People::default());
    people.user("usr_ada", "Ada@Example.com", Some("Ada"), None);
    let instance = Instance::start(&world, Options::new(SECTION, people)).await;
    let mut both = password_changed(
        vec![to_user("usr_ada"), to_email("ada@example.com", None)],
        "identity.password_changed:same",
    );
    both.channels = ChannelSelection::Only(vec![Channel::Email]);
    commit(&instance, "identity", both).await.unwrap();
    instance.settle().await;
    assert_eq!(
        world
            .text(
                "SELECT channel || ':' || target_key || ':' || coalesce(user_id, '-') \
                 FROM merezha_notify.deliveries"
            )
            .await,
        ["email:email:ada@example.com:usr_ada"]
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.captures")
            .await,
        1
    );
    instance.stop().await;
}

#[tokio::test]
async fn data_is_purged_after_settlement() {
    let Some(world) = World::new().await else {
        return;
    };
    let people = Arc::new(People::default());
    people.user("usr_ada", "ada@example.com", Some("Ada"), None);
    let instance = Instance::start(&world, Options::new(SECTION, people)).await;
    commit(
        &instance,
        "identity",
        password_changed(vec![to_user("usr_ada")], "identity.password_changed:purge"),
    )
    .await
    .unwrap();
    instance.settle().await;
    let core = instance.core();
    world
        .execute("UPDATE merezha_notify.notifications SET settled_at = NULL")
        .await;
    let settled = merezha_notifications::maintenance::run(&core, Task::SettleNotifications, soon())
        .await
        .unwrap();
    assert_eq!(
        settled, 1,
        "every delivery settled, so the notification did"
    );
    // Not yet: `retention.data_after_settled` (7 days) has not passed.
    assert_eq!(
        merezha_notifications::maintenance::run(&core, Task::PurgeData, soon())
            .await
            .unwrap(),
        0
    );
    world
        .execute("UPDATE merezha_notify.notifications SET settled_at = now() - interval '8 days'")
        .await;
    assert_eq!(
        merezha_notifications::maintenance::run(&core, Task::PurgeData, soon())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.notifications \
                 WHERE data IS NULL AND data_purged_at IS NOT NULL"
            )
            .await,
        1
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.deliveries WHERE name IS NOT NULL")
            .await,
        0
    );
    // A failed delivery whose data is gone cannot be redelivered.
    world
        .execute(
            "UPDATE merezha_notify.deliveries SET status = 'failed', status_reason = 'x' \
             WHERE channel = 'email'",
        )
        .await;
    let delivery = world
        .text("SELECT id::text FROM merezha_notify.deliveries WHERE channel = 'email'")
        .await
        .remove(0);
    instance
        .tokens
        .add("admin", user("usr_admin", &["notifications.admin"], None));
    let (status, body) = instance
        .call(
            "POST",
            &format!("/v1/notifications/deliveries/{delivery}/redeliver"),
            Some("admin"),
            None,
        )
        .await;
    assert_eq!(
        (status.as_u16(), body["error"]["code"].clone()),
        (410, json!("data_purged"))
    );
    instance.stop().await;
}

/// NT-N2 acceptance: `request` checks `members` without a Directory call
/// and completes on a pool of one connection the caller's transaction
/// holds (R4).
#[tokio::test]
async fn a_non_member_fails_and_request_completes_on_a_pool_of_one() {
    let Some(world) = World::new().await else {
        return;
    };
    let people = Arc::new(People::default());
    people.user("usr_ada", "ada@example.com", Some("Ada"), None);
    people.join("org_1", "usr_ada", &["member"]);
    let one = world.pool(1);
    let mut options = Options::new(SECTION, people.clone());
    options.workers = false;
    options.database = Some(one.clone());
    let instance = Instance::start(&world, options).await;
    let calls = people.calls.load(std::sync::atomic::Ordering::SeqCst);
    let mut outsider = password_changed(vec![to_user("usr_ada")], "api:usr_x:k1");
    outsider.tenant = Some(id("org_1"));
    let mut member = outsider.clone();
    member.dedupe_key = DedupeKey::new("api:usr_x:k2").unwrap();
    member.members = vec![id("usr_ada")];
    let notifier = instance.notifier("api");
    let (refused, accepted) = one
        .transaction(soon(), move |tx| {
            Box::pin(async move {
                let refused = notifier.request(tx, outsider).await;
                let accepted = notifier.request(tx, member).await;
                Ok((refused, accepted))
            })
        })
        .await
        .unwrap();
    assert_eq!(refused, Err(NotifyError::RecipientNotInTenant));
    assert!(accepted.is_ok());
    assert_eq!(
        people.calls.load(std::sync::atomic::Ordering::SeqCst),
        calls,
        "request makes no Directory call"
    );
    // Identity is exempt: invitations address non-members.
    let mut invitation = request(
        "identity.invitation",
        vec![to_email("new@example.com", None)],
        "identity.invitation:i1",
    );
    invitation.tenant = Some(id("org_1"));
    invitation.data = data(json!({"organization_name": "Org", "inviter_name": "Ada"}));
    invitation.secret_data = data(json!({"link": "https://app.example/i#t"}));
    let identity = instance.notifier("identity");
    let invited = one
        .transaction(soon(), move |tx| {
            Box::pin(async move { Ok(identity.request(tx, invitation).await) })
        })
        .await
        .unwrap();
    assert!(invited.is_ok(), "{invited:?}");
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_jobs.outbox WHERE source = 'notifications'")
            .await,
        2
    );
}

#[tokio::test]
async fn without_a_directory_users_are_skipped_and_addresses_still_work() {
    let Some(world) = World::new().await else {
        return;
    };
    let instance = Instance::start(&world, Options::new(SECTION, Arc::new(EmptyDirectory))).await;
    let mut alert = request(
        "notifications.webhook_disabled",
        vec![
            to_user("usr_ada"),
            Recipient::Application {
                roles: vec![id("support")],
            },
            to_email("ops@example.com", None),
        ],
        "whdisable:e2:1",
    );
    alert.data = data(json!({"endpoint_id": "e2", "endpoint_url": "https://x.example/h"}));
    commit(&instance, "notifications", alert).await.unwrap();
    instance.settle().await;
    assert_eq!(
        world
            .text(
                "SELECT channel || ':' || target_key || ':' || status || ':' \
                 || coalesce(status_reason, '') FROM merezha_notify.deliveries ORDER BY 1"
            )
            .await,
        [
            "email:application:support:skipped:no_directory",
            "email:email:ops@example.com:accepted:",
            "email:user:usr_ada:skipped:no_directory",
            "inbox:application:support:skipped:no_directory",
            "inbox:user:usr_ada:skipped:no_directory",
        ]
    );
    instance.stop().await;
}

#[tokio::test]
async fn unknown_users_suppressions_and_a_disabled_email_driver_skip_with_reasons() {
    let Some(world) = World::new().await else {
        return;
    };
    let people = Arc::new(People::default());
    people.user("usr_ada", "ada@example.com", Some("Ada"), None);
    let instance = Instance::start(&world, Options::new(SECTION, people.clone())).await;
    world
        .execute(
            "INSERT INTO merezha_notify.suppressions VALUES \
             (gen_random_uuid(), 'email', 'bounced@example.com', NULL, 'hard_bounce', NULL, now()), \
             (gen_random_uuid(), 'email', 'invalid@example.com', NULL, 'invalid', NULL, now())",
        )
        .await;
    let mut both = password_changed(
        vec![
            to_user("usr_gone"),
            to_email("bounced@example.com", None),
            to_email("invalid@example.com", None),
        ],
        "identity.password_changed:s1",
    );
    both.channels = ChannelSelection::Only(vec![Channel::Email]);
    commit(&instance, "identity", both).await.unwrap();
    instance.settle().await;
    // A hard bounce does not stop security mail; `invalid` does.
    assert_eq!(
        deliveries(&world).await,
        [
            "email:accepted:",
            "email:skipped:suppressed",
            "email:skipped:unknown_user"
        ]
    );
    instance.stop().await;

    // With the email driver disabled, built-in security mail is skipped
    // with a reason instead of dropped silently (NT-N1 review).
    let Some(world) = World::new().await else {
        return;
    };
    let section = format!("{SECTION}[modules.notifications.email]\ndriver = \"disabled\"\n");
    let instance = Instance::start(&world, Options::new(&section, people)).await;
    commit(
        &instance,
        "identity",
        password_changed(vec![to_user("usr_ada")], "identity.password_changed:d1"),
    )
    .await
    .unwrap();
    instance.settle().await;
    assert_eq!(
        deliveries(&world).await,
        ["email:skipped:channel_disabled", "inbox:delivered:"]
    );
    instance.stop().await;
}

#[tokio::test]
async fn validation_refuses_caller_errors_before_any_write() {
    let Some(world) = World::new().await else {
        return;
    };
    let instance = Instance::start(&world, Options::new(SECTION, Arc::new(EmptyDirectory))).await;
    let notifier = instance.notifier("identity");
    let base = password_changed(vec![to_user("usr_ada")], "identity.password_changed:v");
    let cases: Vec<(NotificationRequest, NotifyError)> = vec![
        (
            NotificationRequest {
                topic: id("tasks.nope"),
                ..base.clone()
            },
            NotifyError::UnknownTopic,
        ),
        (
            NotificationRequest {
                template: Some(id("tasks.nope")),
                ..base.clone()
            },
            NotifyError::UnknownTemplate,
        ),
        (
            NotificationRequest {
                channels: ChannelSelection::Only(vec![Channel::Sms]),
                ..base.clone()
            },
            NotifyError::ChannelNotAllowed,
        ),
        (
            NotificationRequest {
                recipients: vec![to_user("usr_ada"); 101],
                ..base.clone()
            },
            NotifyError::TooManyRecipients,
        ),
        (
            NotificationRequest {
                recipients: Vec::new(),
                ..base.clone()
            },
            NotifyError::InvalidRecipient,
        ),
        (
            NotificationRequest {
                data: data(json!({"display_name": "Ada", "links": 1})),
                ..base.clone()
            },
            NotifyError::ReservedDataKey,
        ),
        (
            NotificationRequest {
                data: data(json!({"display_name": "x".repeat(40_000)})),
                ..base.clone()
            },
            NotifyError::DataTooLarge,
        ),
        // Strict undefined: the magic link without its `secret.link`.
        (
            NotificationRequest {
                topic: id("identity.magic_link"),
                channels: ChannelSelection::Only(vec![Channel::Email]),
                ..base.clone()
            },
            NotifyError::RenderFailed {
                channel: Channel::Email,
            },
        ),
    ];
    for (request, expected) in cases {
        assert_eq!(notifier.validate(&request), Err(expected));
        assert_eq!(commit(&instance, "identity", request).await, Err(expected));
    }
    assert!(notifier.validate(&base).is_ok());
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_jobs.outbox")
            .await,
        0
    );
    instance.stop().await;
}

/// NT-N2 acceptance: after a send with `secret_data`, no table, log line
/// or span holds the secret (the development-only captures aside), the
/// outbox and job payloads are null, and `redeliver` answers 409
/// `secret_purged`.
#[tokio::test]
async fn secret_data_never_outlives_the_send() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let canary = "s3cr3t-canary-7f1d";
    let people = Arc::new(People::default());
    let instance = Instance::start(&world, Options::new(SECTION, people)).await;
    let mut magic = request(
        "identity.magic_link",
        vec![to_email("ada@example.com", Some("usr_ada"))],
        "identity.magic_link:t1",
    );
    magic.channels = ChannelSelection::Only(vec![Channel::Email]);
    magic.data = data(json!({"display_name": "Ada"}));
    magic.secret_data = data(json!({"link": format!("https://app.example/m#token={canary}")}));
    commit(&instance, "identity", magic).await.unwrap();
    instance.settle().await;
    assert_eq!(deliveries(&world).await, ["email:accepted:"]);
    // Only the development capture holds the rendered link.
    assert_eq!(
        world
            .scalar(&format!(
                "SELECT count(*) FROM merezha_notify.captures WHERE rendered::text LIKE '%{canary}%'"
            ))
            .await,
        1
    );
    let tables = world
        .text(
            "SELECT table_schema || '.' || table_name FROM information_schema.tables \
             WHERE table_schema IN ('merezha_notify', 'merezha_jobs') \
             AND table_name <> 'captures' ORDER BY 1",
        )
        .await;
    assert!(tables.len() > 10, "{tables:?}");
    for table in tables {
        let found = world
            .scalar(&format!(
                "SELECT count(*) FROM {table} t WHERE t::text LIKE '%{canary}%'"
            ))
            .await;
        assert_eq!(found, 0, "{table} holds the secret");
    }
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_jobs.outbox \
                 WHERE contains_secret AND payload IS NOT NULL"
            )
            .await,
        0
    );
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_jobs.jobs \
                 WHERE contains_secret AND (payload IS NOT NULL OR state <> 'succeeded')"
            )
            .await,
        0
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_jobs.jobs WHERE contains_secret")
            .await,
        2,
        "the expansion and the delivery job were flagged"
    );
    // The worker's events reached the harness, so the scan covered them.
    let names: Vec<String> = harness
        .recorded()
        .into_iter()
        .filter_map(|r| r.name)
        .collect();
    for name in ["notification.requested", "notification.accepted"] {
        assert!(names.iter().any(|n| n == name), "{name} not in {names:?}");
    }
    harness.assert_no_canary(canary);
    harness.assert_catalogued();
    // A failed delivery of a secret-bearing notification is not redelivered.
    world
        .execute("UPDATE merezha_notify.deliveries SET status = 'failed', status_reason = 'x'")
        .await;
    let delivery = world
        .text("SELECT id::text FROM merezha_notify.deliveries")
        .await
        .remove(0);
    instance
        .tokens
        .add("admin", user("usr_admin", &["notifications.admin"], None));
    let (status, body) = instance
        .call(
            "POST",
            &format!("/v1/notifications/deliveries/{delivery}/redeliver"),
            Some("admin"),
            None,
        )
        .await;
    assert_eq!(
        (status.as_u16(), body["error"]["code"].clone()),
        (409, json!("secret_purged"))
    );
    instance.stop().await;
}

#[tokio::test]
async fn expired_requests_and_the_inbox_render_follow_the_request() {
    let Some(world) = World::new().await else {
        return;
    };
    let people = Arc::new(People::default());
    people.user("usr_ada", "ada@example.com", Some("Ada"), Some("fr-CA"));
    let section =
        format!("{SECTION}[modules.notifications.templates]\nlocales = [\"en\", \"fr\"]\n");
    let instance = Instance::start(&world, Options::new(&section, people)).await;
    let mut late = password_changed(vec![to_user("usr_ada")], "identity.password_changed:late");
    late.expires_at = Some(jiff::Timestamp::now() - jiff::SignedDuration::from_secs(60));
    commit(&instance, "identity", late).await.unwrap();
    instance.settle().await;
    assert_eq!(
        deliveries(&world).await,
        ["email:expired:expired", "inbox:expired:expired"]
    );
    let mut fresh = password_changed(vec![to_user("usr_ada")], "identity.password_changed:fr");
    fresh.channels = ChannelSelection::Only(vec![Channel::Inbox]);
    commit(&instance, "identity", fresh).await.unwrap();
    instance.settle().await;
    // The contact's `fr-CA` falls back to `fr`; the inbox never sees secrets.
    assert_eq!(
        world
            .text("SELECT locale FROM merezha_notify.deliveries WHERE status = 'delivered'")
            .await,
        ["fr"]
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.inbox_items WHERE seq = change_seq")
            .await,
        1
    );
    instance.stop().await;
}

/// Metrics carry declared topics only: a caller's undeclared topic is
/// labelled `unknown`, so the label set stays bounded (R9).
#[tokio::test]
async fn metrics_label_undeclared_topics_unknown() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let mut options = Options::new(SECTION, Arc::new(EmptyDirectory));
    options.workers = false;
    options.meters = Some(harness.telemetry().meters());
    let instance = Instance::start(&world, options).await;
    let refused = commit(
        &instance,
        "identity",
        request("tasks.never_declared_7f", vec![to_user("usr_ada")], "m:1"),
    )
    .await;
    assert_eq!(refused, Err(NotifyError::UnknownTopic));
    commit(
        &instance,
        "identity",
        password_changed(vec![to_user("usr_ada")], "identity.password_changed:m2"),
    )
    .await
    .unwrap();
    let points = harness.metric("merezha.notification.requests");
    assert_eq!(
        points.value(&[("notify.topic", "unknown"), ("notify.outcome", "invalid")]),
        Some(1.0)
    );
    assert_eq!(
        points.value(&[
            ("notify.topic", "identity.password_changed"),
            ("notify.outcome", "accepted")
        ]),
        Some(1.0)
    );
}
