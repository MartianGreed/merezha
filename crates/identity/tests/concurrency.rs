//! The concurrency cases of the PR 1 test plan.
mod support;

use serde_json::json;
use support::*;

/// 50 parallel consumptions of one magic link produce exactly one session.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_magic_link_consumed_fifty_times_signs_in_once() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    let before = world
        .scalar("SELECT count(*) FROM merezha_identity.sessions")
        .await;
    Browser::new("198.51.100.160")
        .post(&node, "/magic-links", json!({"email": "ada@example.com"}))
        .await;
    let token = world.mail.token("identity.magic_link", "ada@example.com");
    let calls: Vec<_> = (0..50)
        .map(|n| {
            let node = node.clone();
            let token = token.clone();
            tokio::spawn(async move {
                Browser::new(&format!("198.51.100.{}", 161 + n % 50))
                    .post(&node, "/magic-links/consume", json!({"token": token}))
                    .await
                    .status
                    .as_u16()
            })
        })
        .collect();
    let mut created = 0;
    for call in calls {
        match call.await.unwrap() {
            201 => created += 1,
            401 => {}
            other => panic!("{other}"),
        }
    }
    assert_eq!(created, 1);
    let after = world
        .scalar("SELECT count(*) FROM merezha_identity.sessions")
        .await;
    assert_eq!(after - before, 1);
}

/// 20 parallel registrations of one address produce one user.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn twenty_parallel_registrations_create_one_user() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let calls: Vec<_> = (0..20)
        .map(|n| {
            let node = node.clone();
            tokio::spawn(async move {
                Browser::new(&format!("198.51.100.{}", 20 + n))
                    .post(
                        &node,
                        "/registrations",
                        json!({"email": "same@example.com", "password": PASSWORD}),
                    )
                    .await
                    .status
                    .as_u16()
            })
        })
        .collect();
    for call in calls {
        assert_eq!(call.await.unwrap(), 202);
    }
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.users")
            .await,
        1
    );
    assert_eq!(
        world
            .mail
            .to("identity.verify_email", "same@example.com")
            .len(),
        1
    );
    assert_eq!(
        world
            .mail
            .to("identity.account_exists", "same@example.com")
            .len(),
        19
    );
}

/// Log out everywhere racing with requests leaves no live session.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn log_out_everywhere_racing_with_requests_leaves_no_session() {
    let Some(world) = World::new(&section(
        "",
        &[],
        "[modules.identity.sessions]\ntouch_interval = \"1s\"\n",
    ))
    .await
    else {
        return;
    };
    let node = world.instance().await;
    let mut ada = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    let mut browsers = Vec::new();
    for n in 0..8 {
        let mut b = Browser::new(&format!("198.51.100.{}", 60 + n));
        b.post(
            &node,
            "/sessions",
            json!({"email": "ada@example.com", "password": PASSWORD}),
        )
        .await;
        browsers.push(b);
    }
    world.clock.advance(std::time::Duration::from_secs(2));
    let readers: Vec<_> = browsers
        .into_iter()
        .map(|mut b| {
            let node = node.clone();
            tokio::spawn(async move {
                for _ in 0..10 {
                    b.get(&node, "/me").await;
                }
            })
        })
        .collect();
    let revoke = ada.post(&node, "/sessions/revoke-all", json!({})).await;
    assert_eq!(revoke.status, 200);
    for reader in readers {
        reader.await.unwrap();
    }
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.sessions")
            .await,
        0
    );
}

/// A password change racing with a reset ends with one password and only
/// the winner's session.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_password_change_racing_a_reset_leaves_one_password_and_one_session() {
    let Some(world) = World::new(&base()).await else {
        return;
    };
    let node = world.instance().await;
    let ada = signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    Browser::new("198.51.100.90")
        .post(
            &node,
            "/password-resets",
            json!({"email": "ada@example.com"}),
        )
        .await;
    let token = world
        .mail
        .token("identity.password_reset", "ada@example.com");
    let changer = {
        let node = node.clone();
        let mut ada = ada.clone();
        tokio::spawn(async move {
            ada.send(
                &node,
                "PUT",
                "/me/password",
                Some(json!({"new_password": "the changed passphrase"})),
            )
            .await
            .status
            .as_u16()
        })
    };
    let resetter = {
        let node = node.clone();
        tokio::spawn(async move {
            Browser::new("198.51.100.91")
                .post(
                    &node,
                    "/password-resets/complete",
                    json!({"token": token, "new_password": "the reset passphrase"}),
                )
                .await
                .status
                .as_u16()
        })
    };
    let (changed, reset) = (changer.await.unwrap(), resetter.await.unwrap());
    // Whichever commits first wins: a change deletes the user's reset
    // token, a reset revokes the changer's session.
    let winner = match (changed, reset) {
        (200, 401) => "the changed passphrase",
        (401, 201) => "the reset passphrase",
        other => panic!("{other:?}"),
    };
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.password_credentials")
            .await,
        1
    );
    assert_eq!(
        world
            .scalar("SELECT count(*) FROM merezha_identity.sessions")
            .await,
        1
    );
    for password in ["the changed passphrase", "the reset passphrase"] {
        let answer = Browser::new("198.51.100.92")
            .post(
                &node,
                "/sessions",
                json!({"email": "ada@example.com", "password": password}),
            )
            .await;
        assert_eq!(answer.status == 201, password == winner, "{password}");
    }
}
