//! NT-N5: the inbox stream behind the platform edge against real
//! PostgreSQL: authentication and limits, `Last-Event-ID` replay and
//! `reset`, tenant scope, concurrent writers, slow consumers, the drain,
//! and (in `timing`) cross-instance fan-out, re-authentication and the
//! `LISTEN` session's recovery.
mod harness;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use harness::*;
use merezha_notifications::{
    maintenance::Task,
    stream::CHANNEL_CAPACITY,
    testkit::{EventReader, SseEvent},
};
use merezha_platform::{Module, ports::PrincipalKind};
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tower::ServiceExt;

const STREAM: &str = "/v1/notifications/inbox/stream";
const WAIT: Duration = Duration::from_secs(10);

fn people() -> Arc<People> {
    let people = Arc::new(People::default());
    people.user("usr_ada", "ada@example.com", Some("Ada"), None);
    people.user("usr_bob", "bob@example.com", Some("Bob"), None);
    people.join("org_1", "usr_ada", &["member"]);
    people.join("org_2", "usr_ada", &["member"]);
    people
}

/// The minimal section with stream settings `sse` (TOML lines).
fn section(sse: &str) -> String {
    format!("{SECTION}[modules.notifications.sse]\nheartbeat = \"1s\"\n{sse}")
}

fn tokens(instance: &Instance) {
    let tokens = &instance.tokens;
    tokens.add(
        "sender",
        principal(
            "key_sender",
            PrincipalKind::ApiKey,
            &["notifications.send"],
            None,
        ),
    );
    tokens.add("ada", user("usr_ada", &[], None));
    tokens.add("ada_org", user("usr_ada", &[], Some("org_1")));
    tokens.add("bob", user("usr_bob", &[], None));
}

async fn started(sse: &str) -> Option<(World, Instance)> {
    let world = World::new().await?;
    let instance = Instance::start(&world, Options::new(&section(sse), people())).await;
    tokens(&instance);
    Some((world, instance))
}

/// One inbox item for `user` (and `tenant`) through `send`.
async fn send(instance: &Instance, user: &str, key: &str, tenant: Option<&str>) {
    let mut body = json!({
        "topic": "identity.password_changed",
        "recipients": [{"user": user}],
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

/// Open a stream with `token` and `Last-Event-ID`.
async fn open(
    instance: &Instance,
    token: Option<&str>,
    last: Option<&str>,
) -> axum::http::Response<Body> {
    open_at(instance, STREAM, token, last).await
}
async fn open_at(
    instance: &Instance,
    path: &str,
    token: Option<&str>,
    last: Option<&str>,
) -> axum::http::Response<Body> {
    let mut request = Request::builder().method("GET").uri(path);
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    if let Some(last) = last {
        request = request.header("last-event-id", last);
    }
    instance
        .router
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

/// An open stream past its `retry:` block.
async fn reader(instance: &Instance, token: &str, last: Option<&str>) -> EventReader {
    let response = open(instance, Some(token), last).await;
    assert_eq!(response.status(), StatusCode::OK);
    let mut reader = EventReader::new(response.into_body());
    let first = reader.next(WAIT).await.unwrap().unwrap();
    assert_eq!(first.retry, Some(5000), "{first:?}");
    reader
}

async fn error(response: axum::http::Response<Body>) -> (u16, Value) {
    let status = response.status().as_u16();
    // A stream that opened instead never ends: fail, not hang.
    let bytes = tokio::time::timeout(
        Duration::from_secs(5),
        axum::body::to_bytes(response.into_body(), 1 << 16),
    )
    .await
    .unwrap_or_else(|_| panic!("expected an error reply, got a {status} stream"))
    .unwrap();
    (
        status,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// The next non-heartbeat block.
async fn next(reader: &mut EventReader) -> SseEvent {
    reader
        .next_event(WAIT)
        .await
        .expect("an event in time")
        .expect("the stream is open")
}

/// `item` events until `count` items arrived, `unread` events skipped:
/// `(id, item)` pairs in arrival order.
async fn items(reader: &mut EventReader, count: usize) -> Vec<(i64, Value)> {
    let mut found = Vec::new();
    while found.len() < count {
        let event = next(reader).await;
        match event.event.as_deref() {
            Some("item") => {
                let id: i64 = event.id.as_deref().unwrap().parse().unwrap();
                let item = event.json();
                assert_eq!(item["change_seq"], id, "the id is the change_seq");
                found.push((id, item));
            }
            Some("unread") => assert!(event.id.is_none(), "unread moves no resume point"),
            other => panic!("unexpected {other:?}: {event:?}"),
        }
    }
    found
}

/// No item, `hello` or `reset` for `quiet`: heartbeats and the `unread`
/// that closes a batch only.
async fn quiet(reader: &mut EventReader, quiet: Duration) {
    let deadline = tokio::time::Instant::now() + quiet;
    loop {
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        match reader.next_event(left).await {
            Err(()) => return,
            Ok(Some(event)) if event.event.as_deref() == Some("unread") => {}
            Ok(event) => panic!("expected no item, got {event:?}"),
        }
    }
}

/// `user`'s items as `id -> change_seq`, by `seq`.
async fn change_seqs(world: &World, user: &str) -> Vec<(String, i64)> {
    world
        .text(&format!(
            "SELECT id::text || '=' || change_seq FROM merezha_notify.inbox_items \
             WHERE user_id = '{user}' ORDER BY seq"
        ))
        .await
        .into_iter()
        .map(|row| {
            let (id, seq) = row.split_once('=').unwrap();
            (id.to_owned(), seq.parse().unwrap())
        })
        .collect()
}

/// NT-N5 acceptance: a user's 9th stream gets 429; only a user principal
/// may stream, without credentials in the query string; the stream opens
/// with `retry`, `hello` and heartbeats under the §5.4 headers.
#[tokio::test]
async fn a_users_ninth_stream_gets_429_and_only_users_stream() {
    let Some((_world, instance)) = started("").await else {
        return;
    };
    let (status, body) = error(open(&instance, None, None).await).await;
    assert_eq!(
        (status, body["error"]["code"].clone()),
        (401, json!("unauthenticated"))
    );
    let (status, body) = error(open(&instance, Some("sender"), None).await).await;
    assert_eq!(
        (status, body["error"]["code"].clone()),
        (403, json!("unauthorized"))
    );
    let (status, _) = error(
        open_at(
            &instance,
            &format!("{STREAM}?access_token=ada"),
            Some("ada"),
            None,
        )
        .await,
    )
    .await;
    assert_eq!(status, 400, "no credentials in the query string");

    let response = open(&instance, Some("ada"), None).await;
    assert_eq!(response.status(), StatusCode::OK);
    let headers = response.headers();
    assert_eq!(headers["content-type"], "text/event-stream");
    assert_eq!(headers["cache-control"], "no-store");
    assert_eq!(headers["x-accel-buffering"], "no");
    let mut first = EventReader::new(response.into_body());
    assert_eq!(next(&mut first).await.retry, Some(5000));
    let hello = next(&mut first).await;
    assert_eq!(hello.event.as_deref(), Some("hello"));
    assert_eq!(hello.id.as_deref(), Some("0"), "no counter row yet");
    assert_eq!(hello.json(), json!({"unread": 0}));
    // Idle streams carry `: hb` comments every `sse.heartbeat`.
    let beat = first.next(Duration::from_secs(3)).await.unwrap().unwrap();
    assert!(beat.is_comment() && beat.comments == ["hb"], "{beat:?}");

    let mut open_streams = vec![first];
    for _ in 1..8 {
        open_streams.push(reader(&instance, "ada", None).await);
    }
    let streams = instance.module.streams().unwrap();
    assert_eq!(streams.open_for("usr_ada"), 8);
    let (status, body) = error(open(&instance, Some("ada"), None).await).await;
    assert_eq!(
        (status, body["error"]["code"].clone()),
        (429, json!("stream_limit"))
    );
    // Bob is not affected, and no stream of Ada's was evicted.
    let _bob = reader(&instance, "bob", None).await;
    assert_eq!(streams.open_for("usr_ada"), 8);
    // A closed stream frees its place.
    drop(open_streams.pop());
    let deadline = Instant::now() + WAIT;
    while streams.open_for("usr_ada") > 7 {
        assert!(
            Instant::now() < deadline,
            "the closed stream was never released"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let _again = reader(&instance, "ada", None).await;
    instance.stop().await;
}

/// The instance bound: past `sse.max_streams`, 503 `overloaded` with
/// `Retry-After`.
#[tokio::test]
async fn a_full_instance_answers_503_overloaded() {
    let Some((_world, instance)) = started("max_streams = 2\n").await else {
        return;
    };
    let _a = reader(&instance, "ada", None).await;
    let _b = reader(&instance, "bob", None).await;
    let response = open(&instance, Some("ada"), None).await;
    assert!(response.headers().contains_key("retry-after"));
    let (status, body) = error(response).await;
    assert_eq!(
        (status, body["error"]["code"].clone()),
        (503, json!("overloaded"))
    );
    instance.stop().await;
}

/// NT-N5 acceptance: `Last-Event-ID` replays exactly the missed items,
/// once and in order, each in its current state; a change after the
/// replay arrives live; anything but a decimal id counts as absent.
#[tokio::test]
async fn last_event_id_replays_exactly_the_missed_items_once_and_in_order() {
    let Some((world, instance)) = started("").await else {
        return;
    };
    for n in 1..=4 {
        send(&instance, "usr_ada", &format!("r{n}"), None).await;
    }
    instance.settle().await;
    let seqs = change_seqs(&world, "usr_ada").await;
    assert_eq!(
        seqs.iter().map(|(_, s)| *s).collect::<Vec<_>>(),
        [1, 2, 3, 4]
    );
    let mut live = reader(&instance, "ada", Some("1")).await;
    let replayed = items(&mut live, 3).await;
    assert_eq!(
        replayed.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        [2, 3, 4]
    );
    assert_eq!(replayed[0].1["id"], json!(seqs[1].0));
    let unread = next(&mut live).await;
    assert_eq!(unread.event.as_deref(), Some("unread"));
    assert_eq!(unread.json(), json!({"unread": 4}));
    quiet(&mut live, Duration::from_millis(1500)).await;

    // Reading the second item is a change: it comes again, read, as 5.
    let (status, _) = instance
        .call(
            "POST",
            &format!("/v1/notifications/inbox/{}/read", seqs[1].0),
            Some("ada"),
            None,
        )
        .await;
    assert_eq!(status.as_u16(), 200);
    let changed = items(&mut live, 1).await;
    assert_eq!(changed[0].0, 5);
    assert_eq!(changed[0].1["id"], json!(seqs[1].0));
    assert!(changed[0].1["read_at"].is_string());

    // Resuming from 3 gets 4 then 5; from 0, every row in its current
    // state, once: 1, 3, 4 and 5 (the second item moved to 5).
    let mut resumed = reader(&instance, "ada", Some("3")).await;
    let got = items(&mut resumed, 2).await;
    assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), [4, 5]);
    quiet(&mut resumed, Duration::from_millis(1200)).await;
    let mut all = reader(&instance, "ada", Some("0")).await;
    let got = items(&mut all, 4).await;
    assert_eq!(
        got.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        [1, 3, 4, 5]
    );
    // A non-decimal id is absent: hello at last_seq with the unread count.
    let mut fresh = reader(&instance, "ada", Some("abc")).await;
    let hello = next(&mut fresh).await;
    assert_eq!(hello.event.as_deref(), Some("hello"));
    assert_eq!(hello.id.as_deref(), Some("5"));
    assert_eq!(hello.json(), json!({"unread": 3}));
    instance.stop().await;
}

/// NT-N5 acceptance: an id at or below the user's `purged_through` gives
/// `reset`, as does one above `last_seq` or one with more than
/// `sse.replay_max` rows changed since; the stream then goes on live.
#[tokio::test]
async fn reset_at_or_below_purged_through_above_last_seq_or_past_replay_max() {
    let Some((world, instance)) = started("replay_max = 2\n").await else {
        return;
    };
    for n in 1..=3 {
        send(&instance, "usr_ada", &format!("p{n}"), None).await;
    }
    instance.settle().await;
    let expect_reset = |event: SseEvent| {
        assert_eq!(event.event.as_deref(), Some("reset"), "{event:?}");
        assert_eq!(event.json(), json!({}));
        event.id.unwrap()
    };
    // Three rows changed since 0: more than replay_max.
    let mut stream = reader(&instance, "ada", Some("0")).await;
    assert_eq!(expect_reset(next(&mut stream).await), "3");
    // Above last_seq.
    let mut stream = reader(&instance, "ada", Some("4")).await;
    assert_eq!(expect_reset(next(&mut stream).await), "3");
    // Two since 1: replayed.
    let mut stream = reader(&instance, "ada", Some("1")).await;
    let got = items(&mut stream, 2).await;
    assert_eq!(got.iter().map(|(id, _)| *id).collect::<Vec<_>>(), [2, 3]);

    // The first item expires: purged_through becomes 1.
    let seqs = change_seqs(&world, "usr_ada").await;
    world
        .execute(&format!(
            "UPDATE merezha_notify.inbox_items SET expires_at = now() - interval '1 second' \
             WHERE id = '{}'",
            seqs[0].0
        ))
        .await;
    let core = instance.core();
    let deleted = merezha_notifications::maintenance::run(&core, Task::InboxExpire, soon())
        .await
        .unwrap();
    assert_eq!(deleted, 1);
    assert_eq!(
        world
            .scalar("SELECT purged_through FROM merezha_notify.inbox_counters")
            .await,
        1
    );
    let mut stream = reader(&instance, "ada", Some("1")).await;
    assert_eq!(expect_reset(next(&mut stream).await), "3");
    let mut resumed = reader(&instance, "ada", Some("2")).await;
    let got = items(&mut resumed, 1).await;
    assert_eq!(got[0].0, 3);
    // After a reset the stream is live from last_seq.
    send(&instance, "usr_ada", "p4", None).await;
    let got = items(&mut stream, 1).await;
    assert_eq!(got[0].0, 4);
    instance.stop().await;
}

/// Tenant isolation as N2's REST: items without a tenant and those of the
/// active tenant only, live and replayed.
#[tokio::test]
async fn streams_carry_only_the_active_tenants_items_and_tenantless_ones() {
    let Some((_world, instance)) = started("").await else {
        return;
    };
    send(&instance, "usr_ada", "t1", Some("org_1")).await;
    send(&instance, "usr_ada", "t2", Some("org_2")).await;
    send(&instance, "usr_ada", "t3", None).await;
    instance.settle().await;
    let mut plain = reader(&instance, "ada", Some("0")).await;
    let mut org = reader(&instance, "ada_org", Some("0")).await;
    let got: Vec<_> = items(&mut plain, 1).await;
    assert_eq!(got[0].1["tenant_id"], Value::Null);
    let got: Vec<_> = items(&mut org, 2).await;
    assert_eq!(
        got.iter()
            .map(|(_, i)| i["tenant_id"].clone())
            .collect::<Vec<_>>(),
        [json!("org_1"), Value::Null]
    );
    // Live: org_2 reaches neither; a tenantless item reaches both.
    send(&instance, "usr_ada", "t4", Some("org_2")).await;
    send(&instance, "usr_ada", "t5", None).await;
    let plain_got = items(&mut plain, 1).await;
    let org_got = items(&mut org, 1).await;
    assert_eq!(plain_got[0].1["tenant_id"], Value::Null);
    assert_eq!(org_got[0].1["tenant_id"], Value::Null);
    assert_eq!(plain_got[0].0, org_got[0].0);
    instance.stop().await;
}

/// Review of PR #61: `sse.replay_max` counts only the rows in the stream's
/// scope. Other tenants' changes never turn a resume into `reset`.
#[tokio::test]
async fn replay_max_counts_only_the_rows_in_the_streams_tenant_scope() {
    let Some((_world, instance)) = started("replay_max = 2\n").await else {
        return;
    };
    for n in 1..=3 {
        send(&instance, "usr_ada", &format!("o2-{n}"), Some("org_2")).await;
    }
    send(&instance, "usr_ada", "o1", Some("org_1")).await;
    instance.settle().await;
    // Four rows changed since 0, one of them in org_1's scope: replayed.
    let mut org = reader(&instance, "ada_org", Some("0")).await;
    let got = items(&mut org, 1).await;
    assert_eq!(got[0].1["tenant_id"], json!("org_1"));
    quiet(&mut org, Duration::from_millis(1200)).await;
    instance.stop().await;
}

/// NT-N5 test, concurrent writers: A takes the user's next `change_seq`
/// and pauses before commit while B writes for the same user. B waits; a
/// live stream and a resume from before A both get A's and B's changes,
/// once, in order.
#[tokio::test]
async fn concurrent_writers_reach_live_and_resumed_streams_once_in_order() {
    let Some((world, instance)) = started("").await else {
        return;
    };
    send(&instance, "usr_ada", "c1", None).await;
    send(&instance, "usr_ada", "c2", None).await;
    instance.settle().await;
    let seqs = change_seqs(&world, "usr_ada").await;
    let mut live = reader(&instance, "ada", None).await;
    let hello = next(&mut live).await;
    assert_eq!(hello.id.as_deref(), Some("2"));

    // Writer A: the statements of the read route, held open.
    let a = world.db.acquire(soon()).await.unwrap();
    a.batch_execute(&format!(
        "BEGIN; \
         UPDATE merezha_notify.inbox_counters SET last_seq = last_seq + 1 \
           WHERE user_id = 'usr_ada'; \
         UPDATE merezha_notify.inbox_items SET read_at = now(), change_seq = \
           (SELECT last_seq FROM merezha_notify.inbox_counters WHERE user_id = 'usr_ada') \
           WHERE id = '{}'; \
         SELECT pg_notify('merezha_notify_inbox', 'usr_ada');",
        seqs[0].0
    ))
    .await
    .unwrap();
    // Writer B: the read route on the other item, through the block.
    let router = instance.router.clone();
    let path = format!("/v1/notifications/inbox/{}/read", seqs[1].0);
    let b = tokio::spawn(async move {
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("authorization", "Bearer ada")
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 16)
            .await
            .unwrap();
        (status, serde_json::from_slice::<Value>(&bytes).unwrap())
    });
    quiet(&mut live, Duration::from_millis(800)).await;
    assert!(!b.is_finished(), "B waits for A's counter lock");
    a.batch_execute("COMMIT").await.unwrap();
    let (status, item) = b.await.unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(item["change_seq"], 4, "B numbers after A");

    let got = items(&mut live, 2).await;
    assert_eq!(
        got.iter()
            .map(|(id, i)| (*id, i["id"].as_str().unwrap().to_owned()))
            .collect::<Vec<_>>(),
        [(3, seqs[0].0.clone()), (4, seqs[1].0.clone())]
    );
    quiet(&mut live, Duration::from_millis(1200)).await;
    let mut resumed = reader(&instance, "ada", Some("2")).await;
    let again = items(&mut resumed, 2).await;
    assert_eq!(again.iter().map(|(id, _)| *id).collect::<Vec<_>>(), [3, 4]);
    quiet(&mut resumed, Duration::from_millis(1200)).await;
    instance.stop().await;
}

/// NT-N5 test: a consumer that stops reading is disconnected after one
/// heartbeat interval with at most the channel's events buffered, and
/// writers and other streams of the same user never wait on it.
#[tokio::test]
async fn a_slow_consumer_is_disconnected_without_blocking_writers() {
    let Some((world, instance)) = started("").await else {
        return;
    };
    let count = CHANNEL_CAPACITY + 6;
    for n in 0..count {
        send(&instance, "usr_ada", &format!("s{n}"), None).await;
    }
    instance.settle().await;
    let streams = instance.module.streams().unwrap();
    let mut fast = reader(&instance, "ada", None).await;
    assert_eq!(next(&mut fast).await.event.as_deref(), Some("hello"));
    // The slow client asks for a replay of every item and never reads.
    let slow = open(&instance, Some("ada"), Some("0")).await;
    assert_eq!(slow.status(), StatusCode::OK);
    assert_eq!(streams.open_for("usr_ada"), 2);
    // Writers for the same user go on at their own pace meanwhile.
    let seqs = change_seqs(&world, "usr_ada").await;
    for (id, _) in seqs.iter().take(5) {
        let started = Instant::now();
        let (status, _) = instance
            .call(
                "POST",
                &format!("/v1/notifications/inbox/{id}/read"),
                Some("ada"),
                None,
            )
            .await;
        assert_eq!(status.as_u16(), 200);
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "a writer waited"
        );
    }
    let got = items(&mut fast, 5).await;
    assert_eq!(got.len(), 5);
    // Within a few heartbeats the slow stream is gone server-side.
    let deadline = Instant::now() + WAIT;
    while streams.open_for("usr_ada") > 1 {
        assert!(
            Instant::now() < deadline,
            "the slow stream was never dropped"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    // What it holds is bounded by the channel, and then it ends.
    let mut slow = EventReader::new(slow.into_body());
    let mut held = 0;
    while slow.next(WAIT).await.unwrap().is_some() {
        held += 1;
    }
    assert!(held <= CHANNEL_CAPACITY, "{held} events were buffered");
    assert!(held < count, "the replay was cut short");
    instance.stop().await;
}

/// Graceful drain: `Module::draining` ends every stream at once and later
/// streams are refused with 503 `overloaded`.
#[tokio::test]
async fn draining_ends_every_stream_and_refuses_new_ones() {
    let Some((_world, instance)) = started("").await else {
        return;
    };
    let mut a = reader(&instance, "ada", None).await;
    let mut b = reader(&instance, "bob", None).await;
    instance.module.draining();
    assert!(a.ends_within(Duration::from_secs(2)).await);
    assert!(b.ends_within(Duration::from_secs(2)).await);
    let (status, body) = error(open(&instance, Some("ada"), None).await).await;
    assert_eq!(
        (status, body["error"]["code"].clone()),
        (503, json!("overloaded"))
    );
    instance
        .module
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await;
    assert_eq!(instance.module.streams().unwrap().open(), 0);
    instance.stop().await;
}

/// Latency and deadline races (issue #37): CI runs them alone, one at a
/// time (`cargo test -- --test-threads=1 timing::`).
mod timing {
    use super::*;

    const LISTENING: &str = "SELECT count(*) FROM pg_stat_activity \
                             WHERE datname = current_database() \
                             AND query = 'LISTEN merezha_notify_inbox;'";
    const TERMINATE: &str = "SELECT count(*) FROM (SELECT pg_terminate_backend(pid) \
                             FROM pg_stat_activity WHERE datname = current_database() \
                             AND query = 'LISTEN merezha_notify_inbox;') t";

    /// Change Ada's first item as the read route does, without
    /// `pg_notify`: only a wake or the reconcile interval delivers it.
    async fn silent_change(world: &World) -> i64 {
        let seqs = change_seqs(world, "usr_ada").await;
        world
            .execute(&format!(
                "UPDATE merezha_notify.inbox_counters SET last_seq = last_seq + 1 \
                   WHERE user_id = 'usr_ada'; \
                 UPDATE merezha_notify.inbox_items SET read_at = now(), change_seq = \
                   (SELECT last_seq FROM merezha_notify.inbox_counters WHERE user_id = 'usr_ada') \
                   WHERE id = '{}';",
                seqs[0].0
            ))
            .await;
        world
            .scalar("SELECT last_seq FROM merezha_notify.inbox_counters WHERE user_id = 'usr_ada'")
            .await
    }

    /// An instance that reconciles every 10 minutes with one item for Ada,
    /// a stream of Ada's past its `hello`, and its `LISTEN` session up.
    async fn listening() -> Option<(World, Instance, EventReader)> {
        let (world, instance) = started("reconcile_interval = \"600s\"\n").await?;
        send(&instance, "usr_ada", "w1", None).await;
        instance.settle().await;
        let mut stream = reader(&instance, "ada", None).await;
        assert_eq!(next(&mut stream).await.event.as_deref(), Some("hello"));
        instance.until(LISTENING, 1).await;
        Some((world, instance, stream))
    }

    /// Review of PR #61: a `LISTEN` session that connects and drops again
    /// backs off (1 s, then 2 s and more) instead of reconnecting at once.
    #[tokio::test]
    async fn a_listen_session_that_keeps_dropping_backs_off() {
        let harness = merezha_telemetry::testing::Harness::new();
        let Some((world, instance)) = started("").await else {
            return;
        };
        instance.until(LISTENING, 1).await;
        let until = Instant::now() + Duration::from_secs(3);
        let mut kills = 0;
        while Instant::now() < until {
            kills += world.scalar(TERMINATE).await;
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let restored = harness
            .recorded()
            .into_iter()
            .filter(|r| r.name.as_deref() == Some("notification.sse_listen_restored"))
            .count();
        // Delays of 1 s, 1 to 2 s and 2 to 4 s allow 2 or 3 sessions in 3 s.
        assert!(kills >= 1, "the session was never terminated");
        assert!(
            restored <= 3,
            "{restored} reconnects for {kills} kills in 3 s: no backoff"
        );
        instance.stop().await;
    }

    /// Review of PR #61: once the `LISTEN` session is back, every local
    /// stream is woken and picks up what changed while it was down.
    #[tokio::test]
    async fn a_restored_listen_session_wakes_streams_that_missed_a_change() {
        let Some((world, instance, mut stream)) = listening().await else {
            return;
        };
        let seq = silent_change(&world).await;
        quiet(&mut stream, Duration::from_millis(700)).await;
        assert_eq!(world.scalar(TERMINATE).await, 1);
        let started = Instant::now();
        let got = items(&mut stream, 1).await;
        assert_eq!(got[0].0, seq);
        // A 1 s backoff, then a wake spread over 1 s.
        let took = started.elapsed();
        assert!(took <= Duration::from_secs(4), "arrived after {took:?}");
        instance.stop().await;
    }

    /// Review of PR #61: a burst of notifications that may have overflowed
    /// the session's queue wakes every local stream, not only the users it
    /// names.
    #[tokio::test]
    async fn a_burst_that_may_overflow_the_listen_queue_wakes_every_stream() {
        let Some((world, instance, mut stream)) = listening().await else {
            return;
        };
        let seq = silent_change(&world).await;
        quiet(&mut stream, Duration::from_millis(700)).await;
        // 2,000 notifications for other users, delivered at one commit.
        world
            .execute(
                "SELECT pg_notify('merezha_notify_inbox', 'usr_ghost_' || g) \
                 FROM generate_series(1, 2000) g",
            )
            .await;
        let started = Instant::now();
        let got = items(&mut stream, 1).await;
        assert_eq!(got[0].0, seq);
        let took = started.elapsed();
        assert!(took <= Duration::from_secs(3), "arrived after {took:?}");
        instance.stop().await;
    }

    /// Review of PR #61: re-authentication ends a stream whose credentials
    /// now give another active tenant, no active tenant (the membership
    /// was removed) or another user; unchanged credentials keep theirs.
    #[tokio::test]
    async fn re_authentication_ends_a_stream_whose_user_or_tenant_changed() {
        let Some((_world, instance)) = started("reauth_interval = \"10s\"\n").await else {
            return;
        };
        let tokens = &instance.tokens;
        tokens.add("ada_switch", user("usr_ada", &[], Some("org_1")));
        tokens.add("ada_removed", user("usr_ada", &[], Some("org_1")));
        tokens.add("ada_other", user("usr_ada", &[], None));
        tokens.add("ada_kept", user("usr_ada", &[], Some("org_1")));
        let mut switched = reader(&instance, "ada_switch", None).await;
        let mut removed = reader(&instance, "ada_removed", None).await;
        let mut other = reader(&instance, "ada_other", None).await;
        let mut kept = reader(&instance, "ada_kept", None).await;
        // The same credentials now authenticate differently.
        tokens.add("ada_switch", user("usr_ada", &[], Some("org_2")));
        tokens.add("ada_removed", user("usr_ada", &[], None));
        tokens.add("ada_other", user("usr_bob", &[], None));
        let started = Instant::now();
        assert!(
            switched.ends_within(Duration::from_secs(12)).await,
            "tenant"
        );
        assert!(
            removed.ends_within(Duration::from_secs(2)).await,
            "membership"
        );
        assert!(other.ends_within(Duration::from_secs(2)).await, "user");
        let took = started.elapsed();
        assert!(took <= Duration::from_secs(12), "ended after {took:?}");
        assert!(!kept.ends_within(Duration::from_millis(500)).await);
        assert_eq!(instance.module.streams().unwrap().open_for("usr_ada"), 1);
        instance.stop().await;
    }

    /// NT-N5 acceptance: with two host instances on one database, an item
    /// created through B reaches a stream on A within 1 s. A runs no jobs
    /// and reconciles only every 10 minutes, so the item can only arrive
    /// through B's `pg_notify` and A's `LISTEN`.
    #[tokio::test]
    async fn an_item_created_through_b_reaches_a_stream_on_a_within_a_second() {
        let Some(world) = World::new().await else {
            return;
        };
        let mut options = Options::new(&section("reconcile_interval = \"600s\"\n"), people());
        options.workers = false;
        let a = Instance::start(&world, options).await;
        tokens(&a);
        let b = Instance::start(&world, Options::new(&section(""), people())).await;
        tokens(&b);
        let mut stream = reader(&a, "ada", None).await;
        assert_eq!(next(&mut stream).await.event.as_deref(), Some("hello"));
        // Both LISTEN sessions are up once they show in pg_stat_activity.
        a.until(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() \
             AND query = 'LISTEN merezha_notify_inbox;'",
            2,
        )
        .await;
        for n in 0..3 {
            let sent = Instant::now();
            send(&b, "usr_ada", &format!("x{n}"), None).await;
            let got = items(&mut stream, 1).await;
            let arrived = jiff::Timestamp::now();
            let created: jiff::Timestamp =
                got[0].1["created_at"].as_str().unwrap().parse().unwrap();
            let latency = arrived.duration_since(created).unsigned_abs();
            assert!(
                latency < Duration::from_secs(1),
                "created through B, on A after {latency:?} ({:?} after the send)",
                sent.elapsed()
            );
        }
        a.stop().await;
        b.stop().await;
    }

    /// NT-N5 acceptance: a revoked session ends the stream within
    /// `reauth_interval`, and an unrevoked one survives the check.
    #[tokio::test]
    async fn a_revoked_session_ends_the_stream_within_reauth_interval() {
        let Some((_world, instance)) = started("reauth_interval = \"10s\"\n").await else {
            return;
        };
        instance.tokens.add("ada_2", user("usr_ada", &[], None));
        let mut revoked = reader(&instance, "ada", None).await;
        let mut kept = reader(&instance, "ada_2", None).await;
        instance.tokens.revoke("ada");
        let started = Instant::now();
        assert!(revoked.ends_within(Duration::from_secs(12)).await);
        let took = started.elapsed();
        assert!(took <= Duration::from_secs(11), "ended after {took:?}");
        // The other session passed the same check and still streams.
        assert!(!kept.ends_within(Duration::from_millis(500)).await);
        assert_eq!(instance.module.streams().unwrap().open_for("usr_ada"), 1);
        instance.stop().await;
    }

    /// NT-N5 acceptance: after `pg_terminate_backend` on the LISTEN
    /// connection, items still arrive within `reconcile_interval`, and
    /// `notification.sse_listen_lost` and `notification.sse_listen_restored`
    /// are logged.
    #[tokio::test]
    async fn items_arrive_after_the_listen_connection_is_terminated() {
        let harness = merezha_telemetry::testing::Harness::new();
        let Some((world, instance)) = started("reconcile_interval = \"3s\"\n").await else {
            return;
        };
        let mut stream = reader(&instance, "ada", None).await;
        assert_eq!(next(&mut stream).await.event.as_deref(), Some("hello"));
        let listening = "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() \
                         AND query = 'LISTEN merezha_notify_inbox;'";
        instance.until(listening, 1).await;
        world
            .execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                 WHERE datname = current_database() AND query = 'LISTEN merezha_notify_inbox;'",
            )
            .await;
        let sent = Instant::now();
        send(&instance, "usr_ada", "after-kill", None).await;
        let got = items(&mut stream, 1).await;
        assert_eq!(got.len(), 1);
        let took = sent.elapsed();
        assert!(took <= Duration::from_secs(4), "arrived after {took:?}");
        // The session comes back after its 1 s backoff, at most.
        instance.until(listening, 1).await;
        let names: Vec<String> = harness
            .recorded()
            .into_iter()
            .filter_map(|r| r.name)
            .collect();
        assert!(
            names.iter().any(|n| n == "notification.sse_listen_lost"),
            "{names:?}"
        );
        assert!(
            names
                .iter()
                .any(|n| n == "notification.sse_listen_restored")
        );
        harness.assert_catalogued();
        instance.stop().await;
    }
}
