//! Port fallbacks and source-bound handles inside real PostgreSQL
//! transactions.
use merezha_domain::{Failure, Identity};
use merezha_platform::{
    AppContext, BoxFuture, Database, Tx,
    ports::{
        Address, Appended, ChannelSelection, Correlation, DedupeKey, EmailAddress,
        LoopbackAllowAll, MessageContext, NotificationRequest, OnDuplicate, Outbox, OutboxRecord,
        OutboxTarget, Recipient, WebhookEvent,
    },
};
use merezha_postgres::TestDatabase;
use serde_json::{Map, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

fn id(value: &str) -> Identity {
    Identity::new(value).unwrap()
}
fn soon() -> Instant {
    Instant::now() + Duration::from_secs(10)
}
fn context(db: Database) -> merezha_platform::context::AppContextBuilder {
    let policy = Arc::new(LoopbackAllowAll::new("127.0.0.1:1".parse().unwrap()).unwrap());
    AppContext::builder("tasks", db, policy)
}
fn record(key: &str) -> OutboxRecord {
    OutboxRecord {
        dedupe_key: DedupeKey::new(key).unwrap(),
        target: OutboxTarget::Event {
            topic: id("tasks.task_created"),
        },
        payload: json!({"task_id": "t1"}),
        contains_secret: false,
        run_at: None,
        priority: 0,
        context: MessageContext {
            correlation_id: id("c1"),
            causation_id: None,
            tenant: None,
        },
    }
}

/// Counts calls and writes one row per record in the caller's transaction.
#[derive(Default)]
struct RecordingOutbox {
    calls: AtomicUsize,
}
impl Outbox for RecordingOutbox {
    fn append<'a>(
        &'a self,
        tx: &'a mut Tx<'_>,
        source: &'a str,
        records: &'a [OutboxRecord],
        _: OnDuplicate,
    ) -> BoxFuture<'a, Result<Appended, Failure>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            for record in records {
                tx.execute(
                    "INSERT INTO appended VALUES ($1, $2)",
                    &[&source, &record.dedupe_key.as_str()],
                )
                .await
                .map_err(|e| merezha_postgres::classify(&e))?;
            }
            Ok(Appended {
                inserted: records.len(),
                duplicates: 0,
            })
        })
    }
}

async fn rows(db: &Database, table: &str) -> i64 {
    let client = db.acquire(soon()).await.unwrap();
    client
        .query_one(&format!("SELECT count(*) FROM {table}"), &[])
        .await
        .unwrap()
        .get(0)
}

#[tokio::test]
async fn the_unavailable_outbox_rolls_the_callers_transaction_back() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    let client = db.acquire(soon()).await.unwrap();
    client
        .batch_execute("CREATE TABLE tasks (id text)")
        .await
        .unwrap();
    drop(client);
    let ctx = context(db.clone()).build().unwrap();
    let outbox = ctx.outbox("tasks").unwrap();
    let error = db
        .transaction(soon(), |tx| {
            Box::pin(async move {
                tx.execute("INSERT INTO tasks VALUES ('t1')", &[])
                    .await
                    .map_err(|e| merezha_postgres::classify(&e))?;
                outbox
                    .append(tx, &[record("tasks:t1:created")], OnDuplicate::Ignore)
                    .await
            })
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, "outbox_unavailable");
    assert_eq!(rows(&db, "tasks").await, 0, "the caller's row rolled back");
    assert_eq!(
        ctx.outbox("Not A Source").unwrap_err().code,
        "invalid_source"
    );
}

#[tokio::test]
async fn source_bound_outboxes_validate_before_the_implementation_runs() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    let client = db.acquire(soon()).await.unwrap();
    client
        .batch_execute("CREATE TABLE appended (source text, dedupe_key text)")
        .await
        .unwrap();
    drop(client);
    let recording = Arc::new(RecordingOutbox::default());
    let ctx = context(db.clone())
        .outbox(recording.clone())
        .build()
        .unwrap();
    let outbox = ctx.outbox("identity").unwrap();
    let appended = db
        .transaction(soon(), |tx| {
            Box::pin(async move {
                outbox
                    .append(
                        tx,
                        &[record("identity:a"), record("identity:b")],
                        OnDuplicate::Fail,
                    )
                    .await
            })
        })
        .await
        .unwrap();
    assert_eq!(appended.inserted, 2);
    let client = db.acquire(soon()).await.unwrap();
    let sources: Vec<String> = client
        .query("SELECT DISTINCT source FROM appended", &[])
        .await
        .unwrap()
        .iter()
        .map(|r| r.get(0))
        .collect();
    assert_eq!(sources, vec!["identity".to_owned()], "the source is bound");
    drop(client);
    let outbox = ctx.outbox("identity").unwrap();
    let mut too_big = record("identity:c");
    too_big.payload = json!("x".repeat(70_000));
    let error = db
        .transaction(soon(), |tx| {
            Box::pin(async move { outbox.append(tx, &[too_big], OnDuplicate::Fail).await })
        })
        .await
        .unwrap_err();
    assert_eq!(error.code, "outbox_payload_limit");
    assert_eq!(
        recording.calls.load(Ordering::SeqCst),
        1,
        "refused before the port"
    );
    assert_eq!(rows(&db, "appended").await, 2);
}

#[tokio::test]
async fn capture_notifications_accept_inside_a_transaction_without_writing() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let db = Database::connect(&test.config()).unwrap();
    let ctx = context(db.clone()).build().unwrap();
    assert!(!ctx.notifications_deliver());
    let notifier = ctx.notifier("identity").unwrap();
    let request = NotificationRequest {
        topic: id("identity.password_reset"),
        recipients: vec![Recipient::Address(Address::Email {
            address: EmailAddress::new("ada@example.com").unwrap(),
            name: None,
            user: None,
            locale: None,
        })],
        channels: ChannelSelection::TopicDefault,
        template: None,
        data: Map::new(),
        secret_data: json!({"link": "https://x/r3s3t"})
            .as_object()
            .unwrap()
            .clone(),
        locale: None,
        dedupe_key: DedupeKey::new("identity.password_reset:tok_1").unwrap(),
        tenant: None,
        members: Vec::new(),
        not_before: None,
        expires_at: None,
        collapse_key: None,
        correlation: Correlation {
            correlation_id: id("c1"),
            causation_id: None,
        },
    };
    let (handle, webhook) = db
        .transaction(soon(), |tx| {
            Box::pin(async move {
                let handle = notifier.request(tx, request).await.map_err(Failure::from)?;
                let webhook = notifier
                    .emit_webhook(
                        tx,
                        WebhookEvent {
                            tenant: None,
                            event_type: id("tasks.task_completed"),
                            source_event_id: DedupeKey::new("tasks:t1:completed").unwrap(),
                            occurred_at: jiff::Timestamp::UNIX_EPOCH,
                            data: json!({}),
                        },
                    )
                    .await
                    .map_err(Failure::from)?;
                Ok((handle, webhook))
            })
        })
        .await
        .unwrap();
    assert!(!handle.duplicate && !webhook.duplicate);
    assert_ne!(handle.notification_id, webhook.webhook_event_id);
    assert!(
        ctx.directory()
            .users(&[id("usr_1")])
            .await
            .unwrap()
            .is_empty()
    );
}
