//! Rate limits in PostgreSQL (section 7): acceptance 14, exact admission
//! of arrival-charged buckets under concurrency, the bound on
//! failure-charged ones, and state that survives a restart.
mod support;

use serde_json::json;
use support::*;

/// Acceptance 14: requests naming one address from five IPs exhaust only
/// their own `email.address_ip` buckets; the victim's IP still gets its
/// magic link.
#[tokio::test]
async fn naming_an_address_from_other_ips_spends_their_budgets_not_the_victims() {
    // The defaults: email.address_ip 5/h, email.address 30/h, email.ip 20/h.
    let mut text = section("", &[], "");
    for table in ["email_address_ip", "email_address", "email_ip"] {
        let start = text
            .find(&format!("[modules.identity.rate_limits.{table}]"))
            .unwrap();
        let end =
            start + text[start..].find("period = \"1h\"\n").unwrap() + "period = \"1h\"\n".len();
        text.replace_range(start..end, "");
    }
    let Some(world) = World::new(&text).await else {
        return;
    };
    let node = world.instance().await;
    signed_up(&world, &node, "victim@example.com", PASSWORD).await;
    world.mail.clear();
    for n in 0..5 {
        let mut attacker = Browser::new(&format!("203.0.113.{}", 10 + n));
        for _ in 0..5 {
            let answer = attacker
                .post(
                    &node,
                    "/magic-links",
                    json!({"email": "victim@example.com"}),
                )
                .await;
            assert_eq!(answer.status, 202);
        }
        let refused = attacker
            .post(
                &node,
                "/magic-links",
                json!({"email": "victim@example.com"}),
            )
            .await;
        assert_eq!(
            refused.code(),
            "rate_limited",
            "the attacker's own bucket is full"
        );
    }
    let mut victim = Browser::new("198.51.100.200");
    let mine = victim
        .post(
            &node,
            "/magic-links",
            json!({"email": "victim@example.com"}),
        )
        .await;
    assert_eq!(mine.status, 202, "{mine:?}");
    let sent = world.mail.to("identity.magic_link", "victim@example.com");
    assert_eq!(sent.len(), 26);
    let token = sent.last().unwrap().token();
    let signed_in = victim
        .post(&node, "/magic-links/consume", json!({"token": token}))
        .await;
    assert_eq!(signed_in.status, 201);
}

/// 100 parallel requests against an arrival-charged bucket of 5 admit
/// exactly 5.
#[tokio::test]
async fn an_arrival_charged_bucket_admits_exactly_its_limit() {
    let Some(world) = World::new(&section("", &[("email_ip", 5, "1h")], "")).await else {
        return;
    };
    // The request path only: the queued link requests need no worker.
    let node = world.instance_full(None, false).await;
    let calls: Vec<_> = (0..100)
        .map(|n| {
            let node = node.clone();
            tokio::spawn(async move {
                let answer = Browser::new("203.0.113.50")
                    .post(
                        &node,
                        "/password-resets",
                        json!({"email": format!("u{n}@example.com")}),
                    )
                    .await;
                (answer.status.as_u16(), answer.code().to_owned())
            })
        })
        .collect();
    let mut admitted = 0;
    for call in calls {
        match call.await.unwrap() {
            (202, _) => admitted += 1,
            (429, _) => {}
            other => panic!("{other:?}"),
        }
    }
    assert_eq!(admitted, 5);
}

/// A failure-charged bucket of 5 admits at most 5 plus `hash_concurrency`
/// attempts that were already in flight.
#[tokio::test]
async fn a_failure_charged_bucket_admits_at_most_its_limit_plus_hash_concurrency() {
    let Some(world) = World::new(&section("", &[("sign_in_account_ip", 5, "15m")], "")).await
    else {
        return;
    };
    let node = world.instance().await;
    signed_up(&world, &node, "ada@example.com", PASSWORD).await;
    let calls: Vec<_> = (0..100)
        .map(|_| {
            let node = node.clone();
            tokio::spawn(async move {
                Browser::new("203.0.113.51")
                    .post(
                        &node,
                        "/sessions",
                        json!({"email": "ada@example.com", "password": "a wrong password"}),
                    )
                    .await
                    .code()
                    .to_owned()
            })
        })
        .collect();
    let mut attempted = 0;
    for call in calls {
        match call.await.unwrap().as_str() {
            "invalid_credentials" => attempted += 1,
            "rate_limited" => {}
            other => panic!("{other}"),
        }
    }
    assert!((5..=5 + 4).contains(&attempted), "{attempted}");
}

/// A restart keeps the state: a new instance on the same database refuses
/// what the old one exhausted.
#[tokio::test]
async fn rate_limit_state_survives_a_restart() {
    let Some(world) = World::new(&section("", &[("registration_ip", 1, "1h")], "")).await else {
        return;
    };
    let before = world.instance().await;
    let mut b = Browser::new("203.0.113.52");
    assert_eq!(
        b.post(
            &before,
            "/registrations",
            json!({"email": "a@example.com", "password": PASSWORD})
        )
        .await
        .status,
        202
    );
    drop(before);
    let after = world.instance().await;
    let refused = b
        .post(
            &after,
            "/registrations",
            json!({"email": "b@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(refused.code(), "rate_limited");
    // Only keyed digests are stored: no address or IP in the table.
    let client = world.db.acquire(soon()).await.unwrap();
    let rows = client
        .query(
            "SELECT encode(bucket, 'escape') FROM merezha_identity.rate_limits",
            &[],
        )
        .await
        .unwrap();
    for row in rows {
        let text: String = row.get(0);
        assert!(!text.contains("203.0.113") && !text.contains("example.com"));
    }
}
