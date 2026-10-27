//! Issue #56, deleted organizations, against a real write in flight: a
//! save of the organization paused right after its deleted-organization
//! gate (the `test-hooks` pause, so it holds the scope's shared lock as
//! every organization write does) makes the deletion's mark wait, commits
//! first and is purged; a write after the mark is refused. Its own test
//! binary: the pause is process-wide.
mod http;

use axum::http::StatusCode;
use http::*;
use merezha_postgres::TestDatabase;
use serde_json::json;
use std::time::Duration;

const MODEL: &str = include_str!("fixtures/organizations.toml");

/// Append identity's `identity.organization_deleted` for `org`.
async fn organization_deleted(fx: &Fixture, org: &str) {
    use merezha_domain::Identity;
    use merezha_platform::ports::{
        DedupeKey, MessageContext, NewEvent, OnDuplicate, Source, SourceOutbox,
    };
    let outbox = SourceOutbox::new(
        Source::new("identity").unwrap(),
        std::sync::Arc::new(merezha_jobs::PgOutbox),
    );
    let record = outbox.event(
        Identity::new(merezha_content::ORGANIZATION_DELETED_TOPIC).unwrap(),
        NewEvent {
            dedupe_key: DedupeKey::new(format!("organization:{org}:deleted")).unwrap(),
            version: 1,
            occurred_at: jiff::Timestamp::now(),
            payload: json!({"organization_id": org}),
            context: MessageContext {
                correlation_id: Identity::new("delete-request").unwrap(),
                causation_id: None,
                tenant: Some(Identity::new(org).unwrap()),
            },
        },
    );
    fx.db
        .transaction(soon(), move |tx| {
            Box::pin(async move {
                outbox.append(tx, &[record], OnDuplicate::Fail).await?;
                Ok(())
            })
        })
        .await
        .unwrap();
}

/// Wait up to 20 seconds for `sql`'s single text value to be `expected`.
async fn eventually(fx: &Fixture, sql: &str, expected: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        let found = strings(&fx.db, sql).await;
        if found == [expected] {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{sql}: {found:?}, not {expected}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_mark_waits_for_a_write_in_flight_and_refuses_later_ones() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let dir = tempfile::tempdir().unwrap();
    let media = format!(
        "media = {{ driver = \"local\", local = {{ root = {:?} }}, spool_dir = {:?} }}",
        dir.path().join("media"),
        dir.path().join("spool")
    );
    let fx = start_organizations(&test, &media, MODEL).await;
    let scope = format!("org:{ORG_B}");
    let state = format!(
        "SELECT coalesce((SELECT state FROM merezha_content.scopes_deleted \
          WHERE scope = '{scope}'), 'none')"
    );
    // A save of B, held right after its gate passed.
    let pause = merezha_content::testing::pause_next_write(&scope);
    let client = fx.client.clone();
    let save = tokio::spawn(async move {
        client
            .post("/v1/content/manage/entries/notice")
            .by(Some("key_b"))
            .json(&json!({"fields": {"title": "In flight"}}))
            .send()
            .await
    });
    tokio::time::timeout_at(soon().into(), pause.reached())
        .await
        .expect("the save holds its gate");
    organization_deleted(&fx, ORG_B).await;
    // The mark waits on the save's shared lock: one advisory lock request
    // is not granted, and nothing is marked.
    eventually(
        &fx,
        "SELECT count(*)::text FROM pg_locks WHERE locktype = 'advisory' AND NOT granted",
        "1",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(strings(&fx.db, &state).await, ["none"]);
    pause.resume();
    let saved = save.await.unwrap();
    assert_eq!(saved.status, StatusCode::CREATED, "{}", saved.text);
    // Committed first, then marked and purged with the rest.
    eventually(&fx, &state, "purged").await;
    assert_eq!(
        scalar(
            &fx.db,
            &format!("SELECT count(*) FROM merezha_content.entries WHERE scope = '{scope}'")
        )
        .await,
        0
    );
    let late = fx
        .post("/v1/content/manage/entries/notice")
        .by(Some("key_b"))
        .json(&json!({"fields": {"title": "Late"}}))
        .send()
        .await;
    assert_eq!(
        (late.status.as_u16(), late.code()),
        (404, "organization_not_found"),
        "{}",
        late.text
    );
}
