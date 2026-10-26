//! Sessions at the edge: idle and absolute expiry and touch writes
//! (acceptance 5), revocation across instances (6), the Origin and CSRF
//! checks of cookie requests (7), and authentication failures that must be
//! 503, never 401.
mod support;

use axum::http::header;
use serde_json::json;
use std::time::Duration;
use support::*;
use tower::ServiceExt;

const SHORT: &str = "[modules.identity.sessions]\nidle_timeout = \"1h\"\nabsolute_timeout = \"3h\"\ntouch_interval = \"60s\"\n";

async fn touches(world: &World) -> i64 {
    world
        .scalar("SELECT count(*) FROM public.session_touches")
        .await
}

/// Acceptance 5.
#[tokio::test]
async fn sessions_expire_at_their_idle_and_absolute_limits() {
    let Some(world) = World::new(&section("", &[], SHORT)).await else {
        return;
    };
    let node = world.instance().await;
    world
        .execute(
            "CREATE TABLE public.session_touches (at timestamptz DEFAULT now());
             CREATE FUNCTION public.count_touch() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN INSERT INTO public.session_touches DEFAULT VALUES; RETURN NEW; END $$;
             CREATE TRIGGER count_touch AFTER UPDATE ON merezha_identity.sessions
               FOR EACH ROW EXECUTE FUNCTION public.count_touch();",
        )
        .await;
    let mut ada = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    // Within the touch interval no request writes.
    for _ in 0..5 {
        assert_eq!(ada.get(&node, "/session").await.status, 200);
    }
    assert_eq!(touches(&world).await, 0);
    // Past it, one request writes once, the next ones do not.
    world.clock.advance(Duration::from_secs(61));
    for _ in 0..5 {
        assert_eq!(ada.get(&node, "/session").await.status, 200);
    }
    assert_eq!(touches(&world).await, 1);
    // Activity keeps sliding the idle expiry ...
    for _ in 0..2 {
        world.clock.advance(Duration::from_secs(59 * 60));
        let answer = ada.get(&node, "/session").await;
        assert_eq!(answer.status, 200, "{answer:?}");
    }
    assert_eq!(touches(&world).await, 3);
    // ... but never past the absolute expiry, three hours after creation.
    world.clock.advance(Duration::from_secs(55 * 60));
    let last = ada.get(&node, "/session").await;
    assert_eq!(last.status, 200);
    assert_eq!(
        last.body["session"]["idle_expires_at"], last.body["session"]["absolute_expires_at"],
        "the idle expiry is capped by the absolute one"
    );
    // 2h54m after creation; the absolute limit is at 3h.
    world.clock.advance(Duration::from_secs(6 * 60));
    assert_eq!(ada.get(&node, "/session").await.code(), "unauthenticated");
    // A session left alone dies at its idle limit.
    let mut bob = signed_up(&world, &node, "bob@example.com", PASSWORD).await;
    world.clock.advance(Duration::from_secs(59 * 60));
    assert_eq!(bob.get(&node, "/session").await.status, 200);
    world.clock.advance(Duration::from_secs(61 * 60));
    assert_eq!(bob.get(&node, "/session").await.code(), "unauthenticated");
}

/// Acceptance 6: revocation and log out everywhere take effect on the next
/// request, through another instance sharing the database too.
#[tokio::test]
async fn revocation_is_seen_by_every_instance_on_the_next_request() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let a = world.instance().await;
    let b = world.instance().await;
    let mut ada = signed_up(&world, &a, "ada@example.com", PASSWORD).await;
    assert_eq!(
        ada.get(&b, "/session").await.status,
        200,
        "instance b accepts a's session"
    );
    let mut phone = Browser::new("198.51.100.12");
    phone
        .post(
            &b,
            "/sessions",
            json!({"email": "ada@example.com", "password": PASSWORD, "transport": "bearer"}),
        )
        .await;
    assert_eq!(phone.get(&a, "/me").await.status, 200);
    // Sign out on b; a refuses the cookie at once.
    let mut copy = ada.clone();
    assert_eq!(ada.send(&b, "DELETE", "/session", None).await.status, 204);
    assert_eq!(copy.get(&a, "/session").await.code(), "unauthenticated");
    // Log out everywhere on a; b refuses the bearer token at once.
    let mut ada = Browser::new("198.51.100.13");
    ada.post(
        &a,
        "/sessions",
        json!({"email": "ada@example.com", "password": PASSWORD}),
    )
    .await;
    let all = ada.post(&a, "/sessions/revoke-all", json!({})).await;
    assert_eq!(all.body, json!({"revoked": 2}));
    assert!(all.set_cookie().unwrap().contains("Max-Age=0"));
    assert_eq!(phone.get(&b, "/me").await.code(), "unauthenticated");
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.sessions")
            .await,
        0
    );
}

/// Acceptance 7: unsafe cookie requests need the Origin and the CSRF
/// token, on identity's routes and every other block's; bearer requests
/// need neither.
#[tokio::test]
async fn cookie_writes_need_origin_and_csrf_bearer_writes_do_not() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let ada = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    for path in ["/v1/probe", "/v1/identity/sessions/revoke-all"] {
        let mut b = ada.clone();
        b.origin = None;
        let answer = b.post(&node, path, json!({"keep_current": true})).await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (403, "origin_rejected"),
            "{path}"
        );
        b.origin = Some("https://evil.example".into());
        assert_eq!(
            b.post(&node, path, json!({})).await.code(),
            "origin_rejected"
        );
        b.origin = Some(APP.into());
        b.csrf = None;
        let answer = b.post(&node, path, json!({"keep_current": true})).await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (403, "csrf_failed"),
            "{path}"
        );
        b.csrf = Some("bm90IHRoZSB0b2tlbg".into());
        assert_eq!(
            b.post(&node, path, json!({"keep_current": true}))
                .await
                .code(),
            "csrf_failed"
        );
        let mut good = ada.clone();
        let answer = good.post(&node, path, json!({"keep_current": true})).await;
        assert_eq!(answer.status, 200, "{path}: {answer:?}");
    }
    // Safe methods need no token; the probe sees the principal.
    let mut reader = ada.clone();
    reader.csrf = None;
    reader.origin = None;
    let probe = reader.get(&node, "/v1/probe").await;
    assert!(probe.body["subject"].as_str().unwrap().starts_with("usr_"));
    // A rotated session invalidates the old CSRF token.
    let mut rotating = ada.clone();
    let old_csrf = rotating.csrf.clone();
    rotating
        .post(
            &node,
            "/session/reauthenticate",
            json!({"password": PASSWORD}),
        )
        .await;
    let mut stale = rotating.clone();
    stale.csrf = old_csrf;
    assert_eq!(
        stale.post(&node, "/v1/probe", json!({})).await.code(),
        "csrf_failed"
    );
    // Bearer writes skip both checks.
    let mut phone = Browser::new("198.51.100.14");
    phone
        .post(
            &node,
            "/sessions",
            json!({"email": "ada@example.com", "password": PASSWORD, "transport": "bearer"}),
        )
        .await;
    phone.origin = None;
    phone.csrf = None;
    let answer = phone
        .post(&node, "/sessions/revoke-all", json!({"keep_current": true}))
        .await;
    assert_eq!(answer.status, 200, "{answer:?}");
    assert_eq!(phone.post(&node, "/v1/probe", json!({})).await.status, 200);
    // A cookie token presented as a bearer token is refused.
    let mut swapped = Browser::new("198.51.100.15");
    swapped.bearer = rotating.cookie.clone();
    assert_eq!(
        swapped.get(&node, "/session").await.code(),
        "unauthenticated"
    );
}

/// CSRF tokens derived from `secret_key_previous` stay valid during a
/// rotation.
#[tokio::test]
async fn csrf_tokens_survive_a_secret_rotation() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let before = world.instance().await;
    let ada = signed_up(&world, &before, "ada@example.com", PASSWORD).await;
    let rotated = section("", &[], "").replace(
        "secret_key = { env = \"IDENTITY_SECRET\" }",
        "secret_key = { env = \"IDENTITY_SECRET_NEXT\" }\nsecret_key_previous = { env = \"IDENTITY_SECRET\" }",
    );
    let after = World {
        test: world.test,
        db: world.db.clone(),
        clock: world.clock.clone(),
        mail: world.mail.clone(),
        section: rotated,
        endpoints: Default::default(),
    };
    let node = after.instance().await;
    let mut b = ada.clone();
    assert_eq!(b.post(&node, "/v1/probe", json!({})).await.status, 200);
    let fresh = b.get(&node, "/session").await;
    assert_ne!(
        fresh.body["csrf_token"].as_str(),
        ada.csrf.as_deref(),
        "new tokens use the new key"
    );
}

/// Pool exhaustion and statement timeouts during authentication answer
/// 503, never 401 or anonymous.
#[tokio::test]
async fn authentication_failures_of_the_store_are_unavailable() {
    let Some(world) = World::with_pool(
        &base(),
        3,
        Duration::from_millis(200),
        Duration::from_millis(500),
    )
    .await
    else {
        return;
    };
    let node = world.instance_full(None, false).await;
    let ada = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    // Every pooled connection held elsewhere.
    let held: Vec<_> = futures(&world, 3).await;
    let mut b = ada.clone();
    let answer = b.get(&node, "/session").await;
    assert_eq!(
        (answer.status.as_u16(), answer.code()),
        (503, "unavailable"),
        "{answer:?}"
    );
    assert!(answer.headers.contains_key(header::RETRY_AFTER));
    drop(held);
    // A lock that outlasts the statement timeout.
    let mut locker = world.db.acquire(soon()).await.unwrap();
    let tx = locker.transaction().await.unwrap();
    tx.batch_execute("LOCK TABLE merezha_identity.sessions IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    let answer = b.get(&node, "/session").await;
    assert_eq!(
        (answer.status.as_u16(), answer.code()),
        (503, "unavailable")
    );
    tx.rollback().await.unwrap();
    drop(locker);
    assert_eq!(b.get(&node, "/session").await.status, 200);
    // Without credentials the store is not consulted at all.
    let request = Browser::new("198.51.100.16").request("GET", "/v1/probe", None);
    let response = node.router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), 200);
}

async fn futures(world: &World, n: usize) -> Vec<merezha_postgres::PooledClient> {
    let mut held = Vec::new();
    for _ in 0..n {
        held.push(world.db.acquire(soon()).await.unwrap());
    }
    held
}
