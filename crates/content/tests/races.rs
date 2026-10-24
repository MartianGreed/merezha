//! CT-C4 races between saves, publishes and deletes of referenced
//! entries, made deterministic with advisory-lock gates inside triggers
//! and `pg_blocking_pids`: a reference resolved by a save locks its target
//! until the save commits, and `require_published` sees a target's
//! unpublish in flight.
mod http;

use axum::http::{Method, StatusCode};
use http::*;
use merezha_postgres::TestDatabase;
use serde_json::json;

/// A session holding advisory lock `key`, and its backend pid.
struct Gate {
    client: merezha_postgres::PooledClient,
    pid: i32,
    key: i64,
}
async fn hold(fx: &Fixture, key: i64) -> Gate {
    let client = fx.db.acquire(soon()).await.unwrap();
    client
        .execute("SELECT pg_advisory_lock($1)", &[&key])
        .await
        .unwrap();
    let pid = client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    Gate { client, pid, key }
}
impl Gate {
    async fn open(self) {
        self.client
            .execute("SELECT pg_advisory_unlock($1)", &[&self.key])
            .await
            .unwrap();
    }
}

/// A trigger on `table` that waits for advisory lock `key` when `when`
/// holds for the row.
async fn gate_trigger(fx: &Fixture, name: &str, table: &str, event: &str, when: &str, key: i64) {
    execute(
        &fx.db,
        &format!(
            "CREATE FUNCTION public.{name}() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
               IF {when} THEN PERFORM pg_advisory_xact_lock({key}); END IF;
               RETURN NEW;
             END $$;
             CREATE TRIGGER {name} BEFORE {event} ON merezha_content.{table}
               FOR EACH ROW EXECUTE FUNCTION public.{name}();"
        ),
    )
    .await;
}

async fn author(fx: &Fixture) -> String {
    id_of(
        &fx.create("author", json!({"fields": {"name": "Ada"}}))
            .await,
    )
}

async fn incoming_refs(fx: &Fixture, id: &str) -> i64 {
    scalar(
        &fx.db,
        &format!(
            "SELECT count(*) FROM merezha_content.refs \
              WHERE target_kind = 'entry' AND target_id = '{id}'"
        ),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_waits_for_a_save_that_resolved_a_reference_to_it() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    for (query, expected) in [
        ("?if_unreferenced=true", "entry_referenced"),
        ("?purge=true", ""),
    ] {
        let target = author(&fx).await;
        let title = format!("R {query}");
        let referrer = id_of(
            &fx.create("article", json!({"fields": {"title": title}}))
                .await,
        );
        let name = format!("hold_refs_{}", if expected.is_empty() { "p" } else { "u" });
        // The save stops after resolving, as it writes the ref.
        gate_trigger(
            &fx,
            &name,
            "refs",
            "INSERT",
            &format!("NEW.target_id = '{target}'"),
            51,
        )
        .await;
        let gate = hold(&fx, 51).await;
        let client = fx.client.clone();
        let path = variant("article", &referrer, "en");
        let id = target.clone();
        let save = tokio::spawn(async move {
            client
                .put(&path)
                .if_match(1)
                .json(&json!({"fields": {"title": title, "author": {"id": id}}}))
                .send()
                .await
        });
        let saver = wait_blocked_by(&fx.db, gate.pid).await;
        // The delete waits for the save's lock on its target.
        let client = fx.client.clone();
        let uri = format!("/v1/content/manage/entries/author/{target}{query}");
        let delete = tokio::spawn(async move {
            client
                .call(Method::DELETE, &uri)
                .by(Some("admin"))
                .send()
                .await
        });
        wait_blocked_by(&fx.db, saver).await;
        gate.open().await;
        let saved = save.await.unwrap();
        assert_eq!(saved.status, StatusCode::OK, "{:?}", saved.body);
        let deleted = delete.await.unwrap();
        if expected.is_empty() {
            // The purge then removes the new incoming ref with the entry.
            assert_eq!(deleted.status, StatusCode::NO_CONTENT, "{:?}", deleted.body);
            assert_eq!(incoming_refs(&fx, &target).await, 0);
        } else {
            // The guarded delete counts the save's ref.
            assert_eq!(
                (deleted.status, deleted.code()),
                (StatusCode::CONFLICT, expected)
            );
            assert_eq!(incoming_refs(&fx, &target).await, 1);
        }
        execute(
            &fx.db,
            &format!("DROP TRIGGER {name} ON merezha_content.refs"),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_save_waits_for_a_delete_of_its_target_and_finds_it_gone() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let target = author(&fx).await;
    let referrer = id_of(
        &fx.create("article", json!({"fields": {"title": "R"}}))
            .await,
    );
    // The delete stops as it trashes the entry, holding its lock.
    gate_trigger(
        &fx,
        "hold_delete",
        "entries",
        "UPDATE",
        "OLD.deleted_at IS NULL AND NEW.deleted_at IS NOT NULL",
        52,
    )
    .await;
    let gate = hold(&fx, 52).await;
    let client = fx.client.clone();
    let uri = format!("/v1/content/manage/entries/author/{target}");
    let delete = tokio::spawn(async move {
        client
            .call(Method::DELETE, &uri)
            .by(Some("admin"))
            .send()
            .await
    });
    let deleter = wait_blocked_by(&fx.db, gate.pid).await;
    let client = fx.client.clone();
    let path = variant("article", &referrer, "en");
    let id = target.clone();
    let save = tokio::spawn(async move {
        client
            .put(&path)
            .if_match(1)
            .json(&json!({"fields": {"title": "R", "author": {"id": id}}}))
            .send()
            .await
    });
    wait_blocked_by(&fx.db, deleter).await;
    gate.open().await;
    assert_eq!(delete.await.unwrap().status, StatusCode::NO_CONTENT);
    let saved = save.await.unwrap();
    assert_eq!(
        (saved.code(), saved.details()),
        (
            "validation_failed",
            vec![(
                "/fields/author".to_owned(),
                "reference_not_found".to_owned()
            )]
        )
    );
    assert_eq!(incoming_refs(&fx, &target).await, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn require_published_sees_an_unpublish_of_its_target_in_flight() {
    let Some(test) = TestDatabase::create() else {
        return;
    };
    let fx = start(&test, "", CHAIN_MODEL).await;
    let target = author(&fx).await;
    fx.publish("author", &target, "en", 1).await;
    let referrer = id_of(
        &fx.create(
            "article",
            json!({"fields": {"title": "R", "author": {"id": target}}}),
        )
        .await,
    );
    // The unpublish stops as it clears the pointer, holding the entry.
    gate_trigger(
        &fx,
        "hold_unpublish",
        "variants",
        "UPDATE",
        "OLD.published_revision IS NOT NULL AND NEW.published_revision IS NULL",
        53,
    )
    .await;
    let gate = hold(&fx, 53).await;
    let client = fx.client.clone();
    let id = target.clone();
    let unpublish = tokio::spawn(async move { client.unpublish("author", &id, "en", 1).await });
    let unpublisher = wait_blocked_by(&fx.db, gate.pid).await;
    let client = fx.client.clone();
    let id = referrer.clone();
    let publish = tokio::spawn(async move { client.publish("article", &id, "en", 1).await });
    wait_blocked_by(&fx.db, unpublisher).await;
    gate.open().await;
    assert_eq!(unpublish.await.unwrap().status, StatusCode::OK);
    let published = publish.await.unwrap();
    assert_eq!(
        published.details(),
        [(
            "/fields/author".to_owned(),
            "reference_unpublished".to_owned()
        )]
    );
    assert!(rows(&fx.db, &referrer).await.is_empty());
    // The other order: a publish holding its target makes the unpublish
    // wait until it commits.
    fx.publish("author", &target, "en", 1).await;
    gate_trigger(
        &fx,
        "hold_publish",
        "published",
        "INSERT",
        &format!("NEW.entry_id = '{referrer}'"),
        54,
    )
    .await;
    let gate = hold(&fx, 54).await;
    let client = fx.client.clone();
    let id = referrer.clone();
    let publish = tokio::spawn(async move { client.publish("article", &id, "en", 1).await });
    let publisher = wait_blocked_by(&fx.db, gate.pid).await;
    let client = fx.client.clone();
    let id = target.clone();
    let unpublish = tokio::spawn(async move { client.unpublish("author", &id, "en", 1).await });
    wait_blocked_by(&fx.db, publisher).await;
    gate.open().await;
    assert_eq!(publish.await.unwrap().status, StatusCode::OK);
    assert_eq!(unpublish.await.unwrap().status, StatusCode::OK);
}
