//! Acceptance 8: the `Directory` returns only active users with a verified
//! address, in bounded keyset pages, never service accounts, and
//! `is_member` is false for a holder of application grants only.
mod support;
use merezha_domain::Identity;
use merezha_platform::ports::PageRequest;
use serde_json::json;
use support::*;

fn id(text: &str) -> Identity {
    Identity::new(text).unwrap()
}

#[tokio::test]
async fn the_directory_returns_only_active_verified_users_in_bounded_pages() {
    let Some(world) = World::new(&section(
        "",
        &[],
        "[modules.identity.rbac.application_roles.support]\nname = \"Support\"\npermissions = [\"tasks.read\"]\n",
    ))
    .await
    else {
        return;
    };
    let i = world.instance().await;
    let directory = i.handle.directory();
    let mut owner = user(&world, &i, "owner@example.com").await;
    let org = organization(&i, &mut owner, "acme").await;
    let mut members = Vec::new();
    for n in 0..5 {
        let email = format!("m{n}@example.com");
        let mut browser = user(&world, &i, &email).await;
        join(
            &world,
            &i,
            &mut owner,
            &org,
            &mut browser,
            &email,
            &[if n == 0 { ADMIN_ROLE } else { MEMBER_ROLE }],
        )
        .await;
        members.push(browser);
    }
    // An unverified account and a disabled member.
    let mut stranger = Browser::new("198.51.100.80");
    stranger
        .post(
            &i,
            "/registrations",
            json!({"email": "unverified@example.com", "password": PASSWORD}),
        )
        .await;
    user(&world, &i, "support@example.com").await;
    user(&world, &i, "root@example.com").await;
    platform_admin(&i, "root@example.com").await;
    let mut root = sign_in(&i, "root@example.com").await;
    let users = root.get(&i, "/application/users?limit=100").await;
    let user_id = |email: &str| {
        users.body["items"]
            .as_array()
            .unwrap()
            .iter()
            .find(|u| u["email"] == email)
            .unwrap()["id"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let disabled = user_id("m4@example.com");
    root.patch(
        &i,
        &format!("/application/users/{disabled}"),
        json!({"status": "disabled"}),
    )
    .await;
    // A service account in the organization, and an application-only holder.
    owner
        .post(
            &i,
            &format!("/organizations/{org}/service-accounts"),
            json!({"name": "CI"}),
        )
        .await;
    let roles = root.get(&i, "/application/roles").await;
    let support = roles.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["key"] == "support")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    root.post(
        &i,
        "/application/role-assignments",
        json!({"user_id": user_id("support@example.com"), "role_id": support}),
    )
    .await;
    // users(): only active, verified, at most 100 ids.
    let unverified: uuid::Uuid = {
        let client = world.db.acquire(soon()).await.unwrap();
        client
            .query_one("SELECT id FROM merezha_identity.users WHERE email_normalized = 'unverified@example.com'", &[])
            .await
            .unwrap()
            .get(0)
    };
    let asked = vec![
        id(&user_id("m1@example.com")),
        id(&disabled),
        id(&format!("usr_{}", unverified.simple())),
        id("usr_00000000000070008000000000000099"),
        id("svc_00000000000070008000000000000099"),
    ];
    let contacts = directory.users(&asked).await.unwrap();
    assert_eq!(contacts.len(), 1);
    assert_eq!(contacts[0].email, "m1@example.com");
    let too_many: Vec<Identity> = (0..101)
        .map(|_| id("usr_00000000000070008000000000000099"))
        .collect();
    assert_eq!(
        directory.users(&too_many).await.unwrap_err().code,
        "too_many_ids"
    );
    // tenant_members(): every active verified member, by keyset pages.
    let tenant = id(&org);
    let mut seen = Vec::new();
    let mut page = PageRequest::first(2).unwrap();
    loop {
        let got = directory
            .tenant_members(&tenant, None, page.clone())
            .await
            .unwrap();
        assert!(got.items.len() <= 2);
        seen.extend(got.items.iter().map(|c| c.email.clone()));
        match got.next {
            Some(next) => page = PageRequest::new(Some(next), 2).unwrap(),
            None => break,
        }
    }
    seen.sort();
    assert_eq!(
        seen,
        [
            "m0@example.com",
            "m1@example.com",
            "m2@example.com",
            "m3@example.com",
            "owner@example.com"
        ]
    );
    // Role filters: organization-scope tenant roles; `user` means everyone.
    let admins = directory
        .tenant_members(
            &tenant,
            Some(&[id("admin"), id("owner")]),
            PageRequest::first(500).unwrap(),
        )
        .await
        .unwrap();
    let mut admins: Vec<String> = admins.items.into_iter().map(|c| c.email).collect();
    admins.sort();
    assert_eq!(admins, ["m0@example.com", "owner@example.com"]);
    let everyone = directory
        .tenant_members(
            &tenant,
            Some(&[id("user")]),
            PageRequest::first(500).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(everyone.items.len(), 5);
    // application_members(): every active verified user, or by role.
    let all = directory
        .application_members(None, PageRequest::first(500).unwrap())
        .await
        .unwrap();
    assert_eq!(all.items.len(), 7, "owner, m0 to m3, root and support");
    let supporters = directory
        .application_members(Some(&[id("support")]), PageRequest::first(500).unwrap())
        .await
        .unwrap();
    assert_eq!(
        supporters
            .items
            .iter()
            .map(|c| c.email.as_str())
            .collect::<Vec<_>>(),
        ["support@example.com"]
    );
    let admins = directory
        .application_members(
            Some(&[id("platform_admin")]),
            PageRequest::first(500).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        admins
            .items
            .iter()
            .map(|c| c.email.as_str())
            .collect::<Vec<_>>(),
        ["root@example.com"]
    );
    // is_member counts memberships only.
    assert!(
        directory
            .is_member(&id(&user_id("m1@example.com")), &tenant)
            .await
            .unwrap()
    );
    assert!(
        !directory
            .is_member(&id(&user_id("support@example.com")), &tenant)
            .await
            .unwrap()
    );
    assert!(
        !directory
            .is_member(&id(&user_id("root@example.com")), &tenant)
            .await
            .unwrap(),
        "break-glass is no membership"
    );
    assert!(
        !directory
            .is_member(&id("usr_00000000000070008000000000000099"), &tenant)
            .await
            .unwrap()
    );
    // Contacts never print addresses.
    assert!(!format!("{:?}", contacts[0]).contains("m1@example.com"));
}
