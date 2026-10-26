//! Brevo SMS (NT-N4) against real PostgreSQL and `FakeBrevo`: the request
//! shape (number without `+`, encoding, type, organisation prefix, signed
//! status URL), the segment cap, outage and retry through the shared
//! classifier, the no-resend ambiguity rule (NT1), the signed status
//! callback (MAC, CIDR, credential-free edge) with its suppressions, and a
//! telemetry scan for numbers and content.
mod harness;
use harness::*;
use merezha_notifications::{faults, testkit::FakeBrevo};
use merezha_platform::ports::{
    Address, ChannelSelection, Correlation, DedupeKey, NotificationRequest, PhoneNumber, Recipient,
};
use serde_json::{Map, Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const API_KEY: &str = "xkeysib-test-0123456789abcdef";
const SMS_PATH: &str = "/v3/transactionalSMS/send";
/// One SMS template, `{{ text }}`, for a transactional and a marketing
/// topic.
const TOPICS: &str = "[modules.notifications.topics.\"tasks.code\"]\n\
    category = \"transactional\"\nchannels = [\"sms\"]\n\
    [modules.notifications.topics.\"tasks.promo\"]\n\
    category = \"marketing\"\nchannels = [\"sms\"]\ntemplate = \"tasks.code\"\n";

/// The project's template directory.
fn templates() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().join("tasks.code");
    std::fs::create_dir_all(base.join("en")).unwrap();
    std::fs::write(
        base.join("template.toml"),
        "description = \"A code\"\n[preview]\ntext = \"Code 1234\"\n",
    )
    .unwrap();
    std::fs::write(base.join("en/sms.txt"), "{{ text }}").unwrap();
    dir
}

/// The block on `fake` with Brevo SMS, fast retries unless `queue` says
/// otherwise, and `sms` / `brevo` extra keys.
fn section(
    fake: &FakeBrevo,
    dir: &tempfile::TempDir,
    queue: &str,
    sms: &str,
    brevo: &str,
) -> String {
    let queue = if queue.is_empty() {
        "max_attempts = 6\nbackoff_base_ms = 10\nbackoff_max_ms = 300000\n"
    } else {
        queue
    };
    format!(
        "[jobs.queues.\"notify.sms\"]\n{queue}\
         {SECTION}public_base_url = \"https://api.acme.test\"\n\
         [modules.notifications.sms]\ndriver = \"brevo\"\nsender = \"Acme\"\n{sms}\
         [modules.notifications.templates]\ndir = \"{}\"\n\
         [modules.notifications.brevo]\napi_key = {{ env = \"BREVO_API_KEY\" }}\n\
         base_url = \"{}\"\ntimeout = \"2s\"\n{brevo}{TOPICS}",
        dir.path().display(),
        fake.base_url()
    )
}
fn options(section: &str) -> Options {
    let mut options = Options::new(section, Arc::new(People::default()));
    options.env = vec![("BREVO_API_KEY".into(), API_KEY.into())];
    options
}

fn phone(number: &str) -> Recipient {
    Recipient::Address(Address::Phone {
        e164: PhoneNumber::new(number).unwrap(),
        user: None,
        locale: None,
    })
}
fn request(topic: &str, number: &str, text: &str, key: &str) -> NotificationRequest {
    let mut data = Map::new();
    data.insert("text".into(), Value::String(text.into()));
    NotificationRequest {
        topic: id(topic),
        recipients: vec![phone(number)],
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
    let notifier = instance.notifier("api");
    instance
        .db
        .transaction(soon(), move |tx| {
            Box::pin(async move { Ok(notifier.request(tx, request).await) })
        })
        .await
        .unwrap()
        .unwrap();
}
/// `status:reason` of the latest delivery to `number`.
async fn delivery(world: &World, number: &str) -> String {
    world
        .text(&format!(
            "SELECT status || ':' || coalesce(status_reason, '') FROM merezha_notify.deliveries \
             WHERE address = '{number}' ORDER BY created_at DESC LIMIT 1"
        ))
        .await
        .remove(0)
}
async fn delivery_id(world: &World, number: &str) -> uuid::Uuid {
    let id = world
        .text(&format!(
            "SELECT id::text FROM merezha_notify.deliveries WHERE address = '{number}' \
             ORDER BY created_at LIMIT 1"
        ))
        .await
        .remove(0);
    uuid::Uuid::parse_str(&id).unwrap()
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
    fake.requests_to(SMS_PATH)
}
/// The processed message to `number` (written without `+`).
fn sent_to(fake: &FakeBrevo, number: &str) -> merezha_notifications::testkit::Processed {
    fake.processed()
        .into_iter()
        .find(|p| p.body["recipient"] == number.trim_start_matches('+'))
        .unwrap_or_else(|| panic!("nothing sent to {number}"))
}
async fn report(
    instance: &Instance,
    message: &merezha_notifications::testkit::Processed,
    payload: &Value,
) -> (u16, Value) {
    let (status, body) = instance
        .raw(FakeBrevo::sms_event_request(message, payload))
        .await;
    (status.as_u16(), body)
}

/// NT-N4 acceptance: numbers are sent without `+`; non-GSM content sets
/// `unicodeEnabled`; marketing sends `type: "marketing"`. Also: the
/// sender, the organisation prefix, the tags, the signed `webUrl`, the
/// numeric message id stored as text, and the segments counted by
/// encoding.
#[tokio::test]
async fn requests_carry_the_number_without_plus_the_encoding_the_type_and_a_signed_status_url() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    let dir = templates();
    let mut options = options(&section(
        &fake,
        &dir,
        "",
        "organisation_prefix = \"ACME\"\n",
        "",
    ));
    options.meters = Some(harness.telemetry().meters());
    let instance = Instance::start(&world, options).await;
    commit(
        &instance,
        request("tasks.code", "+33611111111", "Code 1234", "s:1"),
    )
    .await;
    commit(
        &instance,
        request("tasks.code", "+33622222222", "Код 1234", "s:2"),
    )
    .await;
    commit(
        &instance,
        request("tasks.promo", "+33633333333", "Sale today", "s:3"),
    )
    .await;
    wait_status(&instance, "accepted", 3).await;
    let sent = sends(&fake);
    assert_eq!(sent.len(), 3);
    for request in &sent {
        assert_eq!(request.headers["api-key"], API_KEY);
        assert_eq!(request.headers["content-type"], "application/json");
        assert!(!request.headers.contains_key("traceparent"));
        assert_eq!(request.body["sender"], "Acme");
        assert_eq!(request.body["organisationPrefix"], "ACME");
    }
    let gsm = sent_to(&fake, "+33611111111");
    let unicode = sent_to(&fake, "+33622222222");
    let promo = sent_to(&fake, "+33633333333");
    assert_eq!(gsm.body["recipient"], "33611111111");
    assert_eq!(
        (gsm.body["unicodeEnabled"].clone(), gsm.body["type"].clone()),
        (json!(false), json!("transactional"))
    );
    assert_eq!(gsm.body["content"], "Code 1234");
    assert_eq!(unicode.body["unicodeEnabled"], true);
    assert_eq!(promo.body["type"], "marketing");
    let id = delivery_id(&world, "+33611111111").await;
    assert_eq!(gsm.body["tag"], json!(["tasks.code", format!("mzd:{id}")]));
    let mac = instance.core().keys.sms_status_mac(id);
    assert_eq!(
        gsm.body["webUrl"],
        format!("https://api.acme.test/v1/notifications/providers/brevo/sms/{id}/{mac}")
    );
    // The 201's numeric messageId is stored as text.
    assert_eq!(
        world
            .text(&format!(
                "SELECT provider || ' ' || provider_message_id FROM merezha_notify.deliveries \
                 WHERE id = '{id}'"
            ))
            .await,
        [format!("brevo {}", gsm.message_id)]
    );
    let segments = |encoding: &str| {
        harness
            .metric("merezha.notification.sms_segments")
            .value(&[("notify.encoding", encoding)])
    };
    assert_eq!((segments("gsm7"), segments("ucs2")), (Some(2.0), Some(1.0)));
    harness.assert_catalogued();
    instance.stop().await;
}

/// NT-N4 acceptance: content over `max_segments` fails the render. A
/// request whose own data overflows is refused before the caller commits
/// (`render_failed`, NT §3.1); content that only overflows at send time
/// (here the recipient's `fr` template) fails the delivery with
/// `render_failed` and sends nothing; content at the cap is sent.
#[tokio::test]
async fn content_over_max_segments_fails_the_render() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    let dir = templates();
    let fr = dir.path().join("tasks.code/fr");
    std::fs::create_dir_all(&fr).unwrap();
    std::fs::write(fr.join("sms.txt"), "{{ text }} {{ text }}").unwrap();
    let section = section(&fake, &dir, "", "max_segments = 2\n", "").replace(
        "[modules.notifications.templates]\n",
        "[modules.notifications.templates]\nlocales = [\"en\", \"fr\"]\n",
    );
    let instance = Instance::start(&world, options(&section)).await;
    // Two concatenated GSM-7 parts hold 306 septets, UCS-2 parts 67 units.
    for (number, text, key) in [
        ("+33611111111", "a".repeat(306), "m:1"),
        ("+33622222222", "ж".repeat(134), "m:2"),
    ] {
        commit(&instance, request("tasks.code", number, &text, key)).await;
    }
    for (text, key) in [("a".repeat(307), "m:3"), ("ж".repeat(135), "m:4")] {
        let notifier = instance.notifier("api");
        let refused = instance
            .db
            .transaction(soon(), move |tx| {
                Box::pin(async move {
                    Ok(notifier
                        .request(tx, request("tasks.code", "+33633333333", &text, key))
                        .await)
                })
            })
            .await
            .unwrap();
        assert_eq!(
            refused.map_err(|e| format!("{e:?}")),
            Err("RenderFailed { channel: Sms }".to_owned()),
            "{key}"
        );
    }
    // 200 septets in `en`, 401 in the recipient's `fr`.
    let mut french = request("tasks.code", "+33644444444", &"b".repeat(200), "m:5");
    french.recipients = vec![Recipient::Address(Address::Phone {
        e164: PhoneNumber::new("+33644444444").unwrap(),
        user: None,
        locale: Some(merezha_platform::ports::Locale::new("fr").unwrap()),
    })];
    commit(&instance, french).await;
    instance.settle().await;
    for number in ["+33611111111", "+33622222222"] {
        assert_eq!(delivery(&world, number).await, "accepted:", "{number}");
    }
    assert_eq!(
        delivery(&world, "+33644444444").await,
        "failed:render_failed"
    );
    assert_eq!(sends(&fake).len(), 2);
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.deliveries WHERE address = '+33633333333'")
            .await,
        0
    );
    instance.stop().await;
}

/// NT-N4 "outage and retry as in N3": three 503s then a 201 give
/// `accepted` after four attempts, with one message processed.
#[tokio::test]
async fn three_unavailable_answers_then_created_give_accepted_after_four_attempts() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    fake.fail_next(3, 503);
    let dir = templates();
    let instance = Instance::start(&world, options(&section(&fake, &dir, "", "", ""))).await;
    commit(
        &instance,
        request("tasks.code", "+33611111111", "Code", "o:1"),
    )
    .await;
    wait_status(&instance, "accepted", 1).await;
    assert_eq!(
        attempts(&world).await,
        ["transient", "transient", "transient", "accepted"]
    );
    assert_eq!((sends(&fake).len(), fake.processed().len()), (4, 1));
    instance.stop().await;
}

/// NT-N4 "outage as in N3": 503 until exhaustion gives `failed` /
/// `retries_exhausted` and a dead letter; a redelivery after recovery
/// sends once.
#[tokio::test]
async fn a_provider_outage_exhausts_into_a_dead_letter_and_redelivery_sends_after_recovery() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    fake.fail_next(3, 503);
    let dir = templates();
    let instance = Instance::start(
        &world,
        options(&section(
            &fake,
            &dir,
            "max_attempts = 3\nbackoff_base_ms = 10\nbackoff_max_ms = 300000\n",
            "",
            "",
        )),
    )
    .await;
    commit(
        &instance,
        request("tasks.code", "+33611111111", "Code", "x:1"),
    )
    .await;
    wait_status(&instance, "failed", 1).await;
    assert_eq!(
        delivery(&world, "+33611111111").await,
        "failed:retries_exhausted"
    );
    instance
        .until(
            "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.deliver.sms' \
             AND state = 'dead'",
            1,
        )
        .await;
    assert_eq!((sends(&fake).len(), fake.processed().len()), (3, 0));
    instance
        .tokens
        .add("admin", user("usr_admin", &["notifications.admin"], None));
    let id = delivery_id(&world, "+33611111111").await;
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
    instance.stop().await;
}

/// Review: a `duplicate_parameter` answer to an SMS, sent without an
/// idempotency key, does not say whether Brevo sent it. The delivery is
/// `unknown` / `ambiguous` and never resent (NT1), not a `failed` an
/// operator would redeliver.
#[tokio::test]
async fn a_duplicate_answer_is_ambiguous_and_never_resent() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    fake.fail_next_with(400, "duplicate_parameter");
    let dir = templates();
    let instance = Instance::start(&world, options(&section(&fake, &dir, "", "", ""))).await;
    commit(
        &instance,
        request("tasks.code", "+33611111111", "Code", "d:1"),
    )
    .await;
    wait_status(&instance, "unknown", 1).await;
    instance.settle().await;
    assert_eq!(delivery(&world, "+33611111111").await, "unknown:ambiguous");
    assert_eq!(attempts(&world).await, ["ambiguous"]);
    assert_eq!(sends(&fake).len(), 1);
    instance.stop().await;
}

/// Review: the signed URL names the delivery, which a redelivery keeps. A
/// late report about the first generation's message (`unknown` after a
/// dropped connection, then redelivered and accepted with another message
/// id) moves no status: a `rejected` for the first SMS must not fail the
/// second, which would invite a third send (NT1). A report about the
/// current message still applies.
#[tokio::test]
async fn a_late_report_about_an_earlier_generation_moves_no_status() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    fake.close_after_accept();
    let dir = templates();
    let instance = Instance::start(&world, options(&section(&fake, &dir, "", "", ""))).await;
    commit(
        &instance,
        request("tasks.code", "+33611111111", "Code", "g:1"),
    )
    .await;
    wait_status(&instance, "unknown", 1).await;
    assert_eq!(delivery(&world, "+33611111111").await, "unknown:ambiguous");
    let first = fake.processed().remove(0);
    instance
        .tokens
        .add("admin", user("usr_admin", &["notifications.admin"], None));
    let id = delivery_id(&world, "+33611111111").await;
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
    assert_eq!(first.body["webUrl"], second.body["webUrl"]);
    assert_eq!(
        report(&instance, &first, &fake.sms_event("rejected", &first)).await,
        (200, json!({}))
    );
    assert_eq!(delivery(&world, "+33611111111").await, "accepted:");
    assert_eq!(
        world
            .text(&format!(
                "SELECT event FROM merezha_notify.provider_events WHERE delivery_id = '{id}'"
            ))
            .await,
        ["rejected"],
        "stored, correlated, no effect"
    );
    assert_eq!(
        report(&instance, &second, &fake.sms_event("delivered", &second))
            .await
            .0,
        200
    );
    assert_eq!(delivery(&world, "+33611111111").await, "delivered:");
    assert_eq!(fake.processed().len(), 2);
    instance.stop().await;
}

/// A callback body holds at most 100 reports in at most 64 KiB; past
/// either, 413 `body_limit` and nothing is stored.
#[tokio::test]
async fn an_oversized_callback_gets_413_and_stores_nothing() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    let dir = templates();
    let instance = Instance::start(&world, options(&section(&fake, &dir, "", "", ""))).await;
    commit(
        &instance,
        request("tasks.code", "+33611111111", "Code", "z:1"),
    )
    .await;
    wait_status(&instance, "accepted", 1).await;
    let message = fake.processed().remove(0);
    let many: Vec<Value> = (0..101)
        .map(|_| fake.sms_event("soft_bounce", &message))
        .collect();
    assert!(json!(many).to_string().len() < 64 * 1024);
    let mut padded = fake.sms_event("delivered", &message);
    padded["description"] = json!("x".repeat(64 * 1024));
    for (payload, why) in [(json!(many), "101 reports"), (padded, "65 KiB")] {
        let (status, body) = report(&instance, &message, &payload).await;
        assert_eq!(
            (status, body["error"]["code"].clone()),
            (413, json!("body_limit")),
            "{why}"
        );
    }
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.provider_events")
            .await,
        0
    );
    assert_eq!(delivery(&world, "+33611111111").await, "accepted:");
    // At the limit: 100 reports are taken.
    let hundred: Vec<Value> = (0..100)
        .map(|_| fake.sms_event("soft_bounce", &message))
        .collect();
    assert_eq!(report(&instance, &message, &json!(hundred)).await.0, 200);
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.provider_events")
            .await,
        100
    );
    instance.stop().await;
}

/// NT-N4 acceptance: a bad MAC gets 401 (so does a MAC for another
/// delivery, a non-canonical id and a source outside the CIDRs) and stores
/// nothing; the right URL passes the edge even with a bearer the session
/// authenticator would refuse, since the route is credential-free.
#[tokio::test]
async fn a_bad_mac_gets_401_and_stores_nothing() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    let dir = templates();
    let instance = Instance::start(
        &world,
        options(&section(
            &fake,
            &dir,
            "",
            "",
            "webhook_allowed_cidrs = [\"1.179.112.0/20\"]\n",
        )),
    )
    .await;
    commit(
        &instance,
        request("tasks.code", "+33611111111", "Code", "b:1"),
    )
    .await;
    wait_status(&instance, "accepted", 1).await;
    let message = fake.processed().remove(0);
    let id = delivery_id(&world, "+33611111111").await;
    let mac = instance.core().keys.sms_status_mac(id);
    let other = instance
        .core()
        .keys
        .sms_status_mac(uuid::Uuid::from_u128(1));
    let mut flipped = mac.clone().into_bytes();
    flipped[0] = if flipped[0] == b'A' { b'B' } else { b'A' };
    let flipped = String::from_utf8(flipped).unwrap();
    let event = fake.sms_event("delivered", &message);
    let to = |path: String, ip: Option<&str>| {
        let mut request = axum::http::Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .header("authorization", "Bearer not-a-session")
            .body(axum::body::Body::from(event.to_string()))
            .unwrap();
        if let Some(ip) = ip {
            let address: std::net::SocketAddr = format!("{ip}:443").parse().unwrap();
            request
                .extensions_mut()
                .insert(axum::extract::ConnectInfo(address));
        }
        request
    };
    let base = "/v1/notifications/providers/brevo/sms";
    let brevo_ip = Some("1.179.113.7");
    let upper = id.hyphenated().to_string().to_uppercase();
    for (request, why) in [
        (to(format!("{base}/{id}/{flipped}"), brevo_ip), "flipped"),
        (
            to(format!("{base}/{id}/{other}"), brevo_ip),
            "other delivery",
        ),
        (
            to(
                format!("{base}/{}/{mac}", uuid::Uuid::from_u128(1)),
                brevo_ip,
            ),
            "other id",
        ),
        (
            to(format!("{base}/{upper}/{mac}"), brevo_ip),
            "non-canonical id",
        ),
        (to(format!("{base}/{id}/{}", &mac[..21]), brevo_ip), "short"),
        (
            to(format!("{base}/{id}/{mac}"), Some("203.0.113.9")),
            "source",
        ),
        (to(format!("{base}/{id}/{mac}"), None), "no address"),
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
    assert_eq!(delivery(&world, "+33611111111").await, "accepted:");
    let rejected: Vec<_> = harness
        .recorded()
        .into_iter()
        .filter(|r| r.name.as_deref() == Some("notification.provider_event_rejected"))
        .collect();
    assert_eq!(rejected.len(), 7);
    let (status, body) = instance
        .raw(to(format!("{base}/{id}/{mac}"), brevo_ip))
        .await;
    assert_eq!((status.as_u16(), body), (200, json!({})));
    assert_eq!(delivery(&world, "+33611111111").await, "delivered:");
    harness.assert_no_canary(&mac);
    harness.assert_catalogued();
    instance.stop().await;
}

/// Reports: `delivered` is applied once however often it comes; a hard
/// bounce gives `bounced` plus a suppression; `rejected` fails the
/// delivery; a STOP (`unsubscribed`) and `blacklisted` suppress the
/// number, so its next SMS is `skipped`; replies are dropped unstored; a
/// report names only the delivery of its signed URL, whatever message id
/// it carries.
#[tokio::test]
async fn reports_move_deliveries_and_opt_outs_suppress_the_number() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    let dir = templates();
    let mut options = options(&section(&fake, &dir, "", "", ""));
    options.meters = Some(harness.telemetry().meters());
    let instance = Instance::start(&world, options).await;
    let numbers = [
        "+33611111111",
        "+33622222222",
        "+33633333333",
        "+33644444444",
        "+33655555555",
    ];
    for (i, number) in numbers.iter().enumerate() {
        commit(
            &instance,
            request("tasks.code", number, "Code", &format!("r:{i}")),
        )
        .await;
    }
    wait_status(&instance, "accepted", 5).await;
    let [ada, bob, cid, dee, eve] = numbers.map(|n| sent_to(&fake, n));

    let delivered = fake.sms_event("delivered", &ada);
    assert_eq!(
        report(&instance, &ada, &json!([delivered, delivered])).await,
        (200, json!({}))
    );
    assert_eq!(report(&instance, &ada, &delivered).await.0, 200);
    assert_eq!(delivery(&world, numbers[0]).await, "delivered:");
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_notify.provider_events WHERE event = 'delivered'")
            .await,
        1
    );
    // A reply carries the recipient's text: dropped, not stored.
    let mut reply = fake.sms_event("replied", &ada);
    reply["reply"] = json!("call me at home");
    assert_eq!(report(&instance, &ada, &reply).await.0, 200);

    assert_eq!(
        report(&instance, &bob, &fake.sms_event("hard_bounce", &bob))
            .await
            .0,
        200
    );
    assert_eq!(delivery(&world, numbers[1]).await, "bounced:hard_bounce");
    assert_eq!(
        report(&instance, &cid, &fake.sms_event("unsubscribed", &cid))
            .await
            .0,
        200
    );
    assert_eq!(delivery(&world, numbers[2]).await, "accepted:");
    assert_eq!(
        report(&instance, &dee, &fake.sms_event("rejected", &dee))
            .await
            .0,
        200
    );
    assert_eq!(
        delivery(&world, numbers[3]).await,
        "failed:provider_rejected"
    );
    // Eve's URL with Ada's message id still reports on Eve only.
    let mut crossed = fake.sms_event("blacklisted", &eve);
    crossed["messageId"] = json!(ada.message_id.parse::<u64>().unwrap());
    assert_eq!(report(&instance, &eve, &crossed).await.0, 200);
    let eve_id = delivery_id(&world, numbers[4]).await;
    assert_eq!(
        world
            .text(
                "SELECT event || ':' || delivery_id::text FROM merezha_notify.provider_events \
                 WHERE event = 'blacklisted'"
            )
            .await,
        [format!("blacklisted:{eve_id}")]
    );
    assert_eq!(
        world
            .text(
                "SELECT channel || ':' || reason || ':' || coalesce(topic, '*') || ':' || address \
                 FROM merezha_notify.suppressions ORDER BY address"
            )
            .await,
        [
            "sms:hard_bounce:*:+33622222222",
            "sms:unsubscribed:*:+33633333333",
            "sms:unsubscribed:*:+33655555555",
        ]
    );
    let ada_id = delivery_id(&world, numbers[0]).await;
    assert_eq!(
        world
            .text(&format!(
                "SELECT event FROM merezha_notify.provider_events WHERE delivery_id = '{ada_id}'"
            ))
            .await,
        ["delivered"],
        "replies are not stored"
    );
    assert_eq!(
        harness
            .metric("merezha.notification.provider_events")
            .value(&[("notify.channel", "sms"), ("notify.event", "delivered")]),
        Some(1.0)
    );

    // A signed URL of a delivery that no longer exists, with Ada's message
    // id: stored uncorrelated, Ada untouched.
    let gone = uuid::Uuid::from_u128(77);
    let mac = instance.core().keys.sms_status_mac(gone);
    let mut stray = fake.sms_event("rejected", &ada);
    stray["ts_event"] = json!(1_800_000_000);
    let (status, _) = instance
        .raw(
            axum::http::Request::builder()
                .method("POST")
                .uri(format!(
                    "/v1/notifications/providers/brevo/sms/{gone}/{mac}"
                ))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(stray.to_string()))
                .unwrap(),
        )
        .await;
    assert_eq!(status.as_u16(), 200);
    assert_eq!(delivery(&world, numbers[0]).await, "delivered:");
    assert_eq!(
        world
            .scalar(
                "SELECT count(*) FROM merezha_notify.provider_events \
                 WHERE event = 'rejected' AND delivery_id IS NULL"
            )
            .await,
        1
    );

    // The next SMS to a suppressed number is skipped; Ada still gets hers.
    for (i, number) in [numbers[0], numbers[1], numbers[2], numbers[4]]
        .iter()
        .enumerate()
    {
        commit(
            &instance,
            request("tasks.code", number, "Again", &format!("n:{i}")),
        )
        .await;
    }
    instance.settle().await;
    assert_eq!(delivery(&world, numbers[0]).await, "accepted:");
    for number in [numbers[1], numbers[2], numbers[4]] {
        assert_eq!(
            delivery(&world, number).await,
            "skipped:suppressed",
            "{number}"
        );
    }
    assert_eq!(fake.processed().len(), 6);
    instance.stop().await;
}

/// Without the Brevo SMS driver the callback does not exist (404), even
/// with a well-formed URL.
#[tokio::test]
async fn the_callback_is_404_without_the_brevo_sms_driver() {
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    let dir = templates();
    let section =
        section(&fake, &dir, "", "", "").replace("driver = \"brevo\"", "driver = \"capture\"");
    let instance = Instance::start(&world, options(&section)).await;
    let id = uuid::Uuid::from_u128(5);
    let mac = instance.core().keys.sms_status_mac(id);
    let (status, _) = instance
        .call(
            "POST",
            &format!("/v1/notifications/providers/brevo/sms/{id}/{mac}"),
            None,
            Some(json!({"msg_status": "delivered"})),
        )
        .await;
    assert_eq!(status.as_u16(), 404);
    instance.stop().await;
}

/// No log line, span or metric holds the number, the content or the
/// status URL MAC; each provider call has one outbound span.
#[tokio::test]
async fn logs_and_spans_hold_no_number_or_content() {
    let harness = merezha_telemetry::testing::Harness::new();
    let Some(world) = World::new().await else {
        return;
    };
    let fake = FakeBrevo::start(API_KEY).await.unwrap();
    fake.fail_next(1, 503);
    fake.rate_limit(&[("retry-after", "0")]);
    let dir = templates();
    let mut options = options(&section(&fake, &dir, "", "", ""));
    options.meters = Some(harness.telemetry().meters());
    let instance = Instance::start(&world, options).await;
    let (number, content) = ("+33698765432", "Kanarytext 4711");
    commit(&instance, request("tasks.code", number, content, "l:1")).await;
    wait_status(&instance, "accepted", 1).await;
    let message = sent_to(&fake, number);
    assert_eq!(
        report(&instance, &message, &fake.sms_event("delivered", &message))
            .await
            .0,
        200
    );
    let outbound = harness.spans_named("POST /v3/transactionalSMS/send");
    assert_eq!(outbound.len(), 3, "one client span per provider call");
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
    let mac = message.body["webUrl"]
        .as_str()
        .unwrap()
        .rsplit('/')
        .next()
        .unwrap()
        .to_owned();
    for canary in [
        number,
        "98765432",
        content,
        "Kanarytext",
        API_KEY,
        &message.message_id,
        &mac,
    ] {
        harness.assert_no_canary(canary);
    }
    harness.assert_catalogued();
    instance.stop().await;
}

/// Crashes and provider-asked delays race lease and retry deadlines (issue
/// #37): CI runs them alone (`cargo test -- --test-threads=1 timing::`).
mod timing {
    use super::*;

    /// Leases (three Brevo timeouts) expire after 6 s and a crashed run is
    /// aborted after 3 s.
    const CRASHING: &str = "concurrency = 2\nlease_ms = 6000\ntimeout_ms = 3000\n\
        backoff_base_ms = 10\nbackoff_max_ms = 300000\n";

    async fn ambiguous(
        point: Option<&'static str>,
        close: bool,
    ) -> (World, FakeBrevo, Instance, tempfile::TempDir) {
        let world = World::new().await.expect("database");
        let fake = FakeBrevo::start(API_KEY).await.unwrap();
        if close {
            fake.close_after_accept();
        }
        let dir = templates();
        let mut options = options(&section(&fake, &dir, CRASHING, "", ""));
        options.heartbeat = false;
        if let Some(point) = point {
            options.faults.crash_once(point);
        }
        let instance = Instance::start(&world, options).await;
        commit(
            &instance,
            request("tasks.code", "+33611111111", "Code", "k:1"),
        )
        .await;
        wait_status(&instance, "unknown", 1).await;
        (world, fake, instance, dir)
    }

    /// NT-N4 test: a crash after the provider call gives `unknown` and
    /// exactly one request at the fake (NT1); NT-N4 acceptance: a later
    /// `delivered` report moves `unknown` to `delivered`.
    #[tokio::test]
    async fn a_crash_after_the_provider_call_gives_unknown_and_exactly_one_request() {
        if World::new().await.is_none() {
            return;
        }
        let (world, fake, instance, _dir) =
            ambiguous(Some(faults::AFTER_PROVIDER_CALL), false).await;
        assert_eq!(delivery(&world, "+33611111111").await, "unknown:ambiguous");
        assert_eq!(attempts(&world).await, ["ambiguous"]);
        // Past the crashed run's deadline and the lease: never resent.
        tokio::time::sleep(Duration::from_secs(7)).await;
        assert_eq!((sends(&fake).len(), fake.processed().len()), (1, 1));
        let message = fake.processed().remove(0);
        assert_eq!(
            report(&instance, &message, &fake.sms_event("delivered", &message)).await,
            (200, json!({}))
        );
        assert_eq!(delivery(&world, "+33611111111").await, "delivered:");
        instance.stop().await;
    }

    /// Brevo processed the message and dropped the connection: the attempt
    /// is ambiguous, the delivery `unknown`, and nothing is resent.
    #[tokio::test]
    async fn a_dropped_connection_after_acceptance_gives_unknown_and_one_request() {
        if World::new().await.is_none() {
            return;
        }
        let (world, fake, instance, _dir) = ambiguous(None, true).await;
        assert_eq!(delivery(&world, "+33611111111").await, "unknown:ambiguous");
        assert_eq!(attempts(&world).await, ["ambiguous"]);
        tokio::time::sleep(Duration::from_secs(2)).await;
        assert_eq!((sends(&fake).len(), fake.processed().len()), (1, 1));
        assert_eq!(
            world
                .scalar(
                    "SELECT count(*) FROM merezha_jobs.jobs WHERE kind = 'notify.deliver.sms' \
                     AND state NOT IN ('succeeded')"
                )
                .await,
            0,
            "nothing left to resend"
        );
        instance.stop().await;
    }

    /// "Outage and retry as in N3": `Retry-After: 7` delays the next SMS
    /// attempt by at least 7 s.
    #[tokio::test]
    async fn retry_after_delays_the_next_attempt() {
        let Some(world) = World::new().await else {
            return;
        };
        let fake = FakeBrevo::start(API_KEY).await.unwrap();
        fake.rate_limit(&[("Retry-After", "7")]);
        let dir = templates();
        let queue = "max_attempts = 6\nbackoff_base_ms = 10\nbackoff_max_ms = 3600000\n";
        let instance = Instance::start(&world, options(&section(&fake, &dir, queue, "", ""))).await;
        commit(
            &instance,
            request("tasks.code", "+33611111111", "Code", "w:1"),
        )
        .await;
        let started = Instant::now();
        wait_status(&instance, "accepted", 1).await;
        let sent = sends(&fake);
        assert_eq!(sent.len(), 2);
        let gap = sent[1].at.duration_since(sent[0].at);
        assert!(gap >= Duration::from_secs(7), "{gap:?}");
        assert!(started.elapsed() < Duration::from_secs(25));
        assert_eq!(attempts(&world).await, ["rate_limited", "accepted"]);
        instance.stop().await;
    }

    /// Review: a late report about an earlier generation that arrives
    /// while the redelivered SMS is being sent waits `pending` and, once
    /// the worker records the new message id, moves nothing.
    #[tokio::test]
    async fn a_pending_report_about_an_earlier_generation_moves_no_status() {
        let Some(world) = World::new().await else {
            return;
        };
        let fake = FakeBrevo::start(API_KEY).await.unwrap();
        fake.close_after_accept();
        let dir = templates();
        let options = options(&section(&fake, &dir, "", "", ""));
        let armed = options.faults.clone();
        let instance = Instance::start(&world, options).await;
        commit(
            &instance,
            request("tasks.code", "+33611111111", "Code", "q:1"),
        )
        .await;
        wait_status(&instance, "unknown", 1).await;
        let first = fake.processed().remove(0);
        armed.pause_once(faults::AFTER_PROVIDER_CALL, Duration::from_secs(3));
        instance
            .tokens
            .add("admin", user("usr_admin", &["notifications.admin"], None));
        let id = delivery_id(&world, "+33611111111").await;
        let (status, _) = instance
            .call(
                "POST",
                &format!("/v1/notifications/deliveries/{id}/redeliver"),
                Some("admin"),
                None,
            )
            .await;
        assert_eq!(status.as_u16(), 202);
        let deadline = Instant::now() + Duration::from_secs(10);
        while fake.processed().len() < 2 {
            assert!(Instant::now() < deadline, "never resent");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            report(&instance, &first, &fake.sms_event("rejected", &first))
                .await
                .0,
            200
        );
        assert_eq!(delivery(&world, "+33611111111").await, "sending:");
        assert_eq!(
            world
                .scalar("SELECT count(*) FROM merezha_notify.provider_events WHERE pending")
                .await,
            1
        );
        wait_status(&instance, "accepted", 1).await;
        instance
            .until(
                "SELECT count(*) FROM merezha_notify.provider_events WHERE NOT pending",
                1,
            )
            .await;
        assert_eq!(delivery(&world, "+33611111111").await, "accepted:");
        assert_eq!(fake.processed().len(), 2);
        instance.stop().await;
    }

    /// A report about a delivery its worker still owns waits `pending`,
    /// and the worker applies it by the SMS event set once the outcome is
    /// recorded: `rejected` (unknown to email) fails it.
    #[tokio::test]
    async fn a_report_during_the_send_is_applied_after_the_outcome() {
        let Some(world) = World::new().await else {
            return;
        };
        let fake = FakeBrevo::start(API_KEY).await.unwrap();
        let dir = templates();
        let options = options(&section(&fake, &dir, "", "", ""));
        options
            .faults
            .pause_once(faults::AFTER_PROVIDER_CALL, Duration::from_secs(3));
        let instance = Instance::start(&world, options).await;
        commit(
            &instance,
            request("tasks.code", "+33611111111", "Code", "p:1"),
        )
        .await;
        let deadline = Instant::now() + Duration::from_secs(10);
        while fake.processed().is_empty() {
            assert!(Instant::now() < deadline, "never sent");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let message = fake.processed().remove(0);
        assert_eq!(
            report(&instance, &message, &fake.sms_event("rejected", &message))
                .await
                .0,
            200
        );
        assert_eq!(delivery(&world, "+33611111111").await, "sending:");
        assert_eq!(
            world
                .scalar("SELECT count(*) FROM merezha_notify.provider_events WHERE pending")
                .await,
            1
        );
        wait_status(&instance, "failed", 1).await;
        assert_eq!(
            delivery(&world, "+33611111111").await,
            "failed:provider_rejected"
        );
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
