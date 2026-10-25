//! The delivery worker of NT §5.1 against real PostgreSQL: the preference
//! re-check before a send, the ambiguity paths after a crash (SMS is never
//! resent, NT1; email resends with the same idempotency key) and a
//! retrying delivery keeping its notification open.
mod harness;
use harness::*;
use merezha_notifications::{
    drivers::{EmailDriver, EmailMessage, Provider, SendOutcome},
    faults,
    maintenance::Task,
};
use merezha_platform::{
    BoxFuture,
    ports::{
        Address, ChannelSelection, Correlation, DedupeKey, EmailAddress, NotificationRequest,
        PhoneNumber, Recipient,
    },
};
use serde_json::Map;
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

/// A transactional email topic of the project (template of a built-in).
const ASSIGNED: &str = "[modules.notifications.topics.\"tasks.assigned\"]\n\
    category = \"transactional\"\nchannels = [\"email\"]\ntemplate = \"identity.password_changed\"\n";

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
fn email(address: &str) -> Recipient {
    Recipient::Address(Address::Email {
        address: EmailAddress::new(address).unwrap(),
        name: None,
        user: None,
        locale: None,
    })
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

/// `status:reason` of the only delivery.
async fn delivery(world: &World) -> String {
    world
        .text("SELECT status || ':' || coalesce(status_reason, '') FROM merezha_notify.deliveries")
        .await
        .remove(0)
}
async fn attempts(world: &World) -> Vec<String> {
    world
        .text("SELECT outcome FROM merezha_notify.delivery_attempts ORDER BY attempt")
        .await
}

#[tokio::test]
async fn an_opt_out_between_expansion_and_send_skips_the_delivery() {
    let Some(world) = World::new().await else {
        return;
    };
    let people = Arc::new(People::default());
    people.user("usr_ada", "ada@example.com", Some("Ada"), None);
    let mut options = Options::new(&format!("{SECTION}{ASSIGNED}"), people);
    options.relay = false;
    let instance = Instance::start(&world, options).await;
    commit(
        &instance,
        request(
            "tasks.assigned",
            Recipient::User { id: id("usr_ada") },
            "t:1",
        ),
    )
    .await;
    // Expansion only: its delivery job record is not relayed yet.
    instance.relay_now().await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.deliveries WHERE status = 'queued'",
            1,
        )
        .await;
    world
        .execute(
            "INSERT INTO merezha_notify.preferences VALUES \
             ('usr_ada', 'tasks.assigned', 'email', false, now())",
        )
        .await;
    instance.relay_now().await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.deliveries WHERE status = 'skipped'",
            1,
        )
        .await;
    assert_eq!(delivery(&world).await, "skipped:preference");
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.captures")
            .await,
        0
    );
    assert!(attempts(&world).await.is_empty(), "no attempt was started");
    instance.stop().await;
}

/// Transient until `ok` turns on, then accepted.
#[derive(Default)]
struct Flaky {
    ok: AtomicBool,
    calls: AtomicUsize,
}
impl EmailDriver for Flaky {
    fn provider(&self) -> Provider {
        Provider::Brevo
    }
    fn send<'a>(&'a self, _: &'a EmailMessage, _: Instant) -> BoxFuture<'a, SendOutcome> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let outcome = if self.ok.load(Ordering::SeqCst) {
            SendOutcome::Accepted {
                provider: Provider::Brevo,
                message_id: Some("m1".into()),
            }
        } else {
            SendOutcome::Transient {
                code: "upstream_unavailable".into(),
                retry_after: Some(Duration::from_millis(200)),
            }
        };
        Box::pin(async move { outcome })
    }
}

#[tokio::test]
async fn a_retrying_delivery_keeps_its_notification_unsettled() {
    let Some(world) = World::new().await else {
        return;
    };
    let driver = Arc::new(Flaky::default());
    let mut options = Options::new(
        &format!(
            "[jobs.queues.\"notify.email\"]\nmax_attempts = 50\nbackoff_base_ms = 10\n\
             backoff_max_ms = 200\n{SECTION}{ASSIGNED}"
        ),
        Arc::new(People::default()),
    );
    options.email_driver = Some(driver.clone());
    let instance = Instance::start(&world, options).await;
    commit(
        &instance,
        request("tasks.assigned", email("carol@example.org"), "t:2"),
    )
    .await;
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.deliveries WHERE status = 'retrying'",
            1,
        )
        .await;
    assert_eq!(delivery(&world).await, "retrying:upstream_unavailable");
    let core = instance.core();
    merezha_notifications::maintenance::run(&core, Task::SettleNotifications, soon())
        .await
        .unwrap();
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.notifications WHERE settled_at IS NULL")
            .await,
        1,
        "a retrying delivery is not settled"
    );
    driver.ok.store(true, Ordering::SeqCst);
    instance
        .until(
            "SELECT count(*) FROM merezha_notify.deliveries WHERE status = 'accepted'",
            1,
        )
        .await;
    let settled = merezha_notifications::maintenance::run(&core, Task::SettleNotifications, soon())
        .await
        .unwrap();
    assert_eq!(settled, 1);
    let outcomes = attempts(&world).await;
    assert_eq!(outcomes.last().map(String::as_str), Some("accepted"));
    assert!(
        outcomes[..outcomes.len() - 1]
            .iter()
            .all(|o| o == "transient")
    );
    assert_eq!(driver.calls.load(Ordering::SeqCst), outcomes.len());
    instance.stop().await;
}

/// Crashes race a lease deadline (issue #37): CI runs them alone, one at a
/// time (`cargo test -- --test-threads=1 timing::`).
mod timing {
    use super::*;

    /// A section where `queue`'s leases expire after 1 s and a crashed run
    /// is aborted after 3 s.
    fn crashing(queue: &str, extra: &str) -> String {
        format!(
            "[jobs.queues.\"{queue}\"]\nconcurrency = 2\nlease_ms = 1000\ntimeout_ms = 3000\n\
             backoff_base_ms = 10\n{SECTION}{extra}"
        )
    }

    /// NT1: a crash after the SMS provider call leaves a `started`
    /// attempt; the rerun sets `unknown` and never sends again.
    #[tokio::test]
    async fn a_crash_after_the_sms_provider_call_gives_unknown_and_one_send() {
        let Some(world) = World::new().await else {
            return;
        };
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().join("notifications/templates/tasks.code");
        std::fs::create_dir_all(base.join("en")).unwrap();
        std::fs::write(base.join("template.toml"), "description = \"A code\"\n").unwrap();
        std::fs::write(base.join("en/sms.txt"), "Your code is ready").unwrap();
        let extra = format!(
            "[modules.notifications.sms]\ndriver = \"capture\"\nsender = \"Acme\"\n\
             [modules.notifications.templates]\ndir = \"{}\"\n\
             [modules.notifications.topics.\"tasks.code\"]\ncategory = \"transactional\"\n\
             channels = [\"sms\"]\n",
            dir.path().join("notifications/templates").display()
        );
        let mut options =
            Options::new(&crashing("notify.sms", &extra), Arc::new(People::default()));
        options.heartbeat = false;
        options.faults.crash_once(faults::AFTER_PROVIDER_CALL);
        let instance = Instance::start(&world, options).await;
        let phone = Recipient::Address(Address::Phone {
            e164: PhoneNumber::new("+33612345678").unwrap(),
            user: None,
            locale: None,
        });
        commit(&instance, request("tasks.code", phone, "c:1")).await;
        instance
            .until(
                "SELECT count(*) FROM merezha_notify.deliveries WHERE status = 'unknown'",
                1,
            )
            .await;
        assert_eq!(delivery(&world).await, "unknown:ambiguous");
        assert_eq!(attempts(&world).await, ["ambiguous"]);
        // Past the crashed run's deadline: still one send, never resent.
        tokio::time::sleep(Duration::from_secs(4)).await;
        assert_eq!(
            world
                .scalar("SELECT count(*) FROM merezha_notify.captures WHERE channel = 'sms'")
                .await,
            1
        );
        assert_eq!(delivery(&world).await, "unknown:ambiguous");
        instance.stop().await;
    }

    /// Email resends with the same idempotency key after either crash:
    /// after the provider call the capture answers duplicate, after the
    /// attempt commit (nothing sent) it accepts; one message either way.
    #[tokio::test]
    async fn email_reuses_its_idempotency_key_after_a_crash() {
        for (point, second) in [
            (faults::AFTER_PROVIDER_CALL, "duplicate"),
            (faults::AFTER_ATTEMPT_COMMITTED, "accepted"),
        ] {
            let Some(world) = World::new().await else {
                return;
            };
            let mut options = Options::new(
                &crashing("notify.email", ASSIGNED),
                Arc::new(People::default()),
            );
            options.heartbeat = false;
            options.faults.crash_once(point);
            let instance = Instance::start(&world, options).await;
            commit(
                &instance,
                request("tasks.assigned", email("carol@example.org"), "e:1"),
            )
            .await;
            instance
                .until(
                    "SELECT count(*) FROM merezha_notify.deliveries WHERE status = 'accepted'",
                    1,
                )
                .await;
            assert_eq!(attempts(&world).await, ["ambiguous", second], "{point}");
            let delivery_id = world
                .text("SELECT id::text FROM merezha_notify.deliveries")
                .await
                .remove(0);
            let key = merezha_notifications::ids::idempotency_key(
                uuid::Uuid::parse_str(&delivery_id).unwrap(),
                0,
            );
            assert_eq!(
                world
                    .text("SELECT rendered->>'idempotency_key' FROM merezha_notify.captures")
                    .await,
                [key.to_string()],
                "{point}"
            );
            tokio::time::sleep(Duration::from_secs(4)).await;
            assert_eq!(
                world
                    .scalar("SELECT count(*) FROM merezha_notify.captures")
                    .await,
                1,
                "{point}"
            );
            instance.stop().await;
        }
    }
}
