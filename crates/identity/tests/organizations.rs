//! ID PR 2 route suite (acceptance 1 and 9): organizations, the active
//! organization, members, invitations, roles, role assignments, service
//! accounts, API keys and audit listings, with two organizations and
//! members in each; ids of another organization answer 404, and integration
//! events commit with their change under R11 names.
mod support;
use serde_json::{Value, json};
use support::*;

async fn world() -> Option<(World, Instance)> {
    let world = World::new(&base()).await?;
    let instance = world.instance().await;
    Some((world, instance))
}

async fn events(world: &World, kind: &str) -> Vec<Value> {
    let client = world.db.acquire(soon()).await.unwrap();
    client
        .query(
            "SELECT payload FROM merezha_jobs.outbox WHERE source = 'identity' AND topic = $1
              ORDER BY id",
            &[&kind],
        )
        .await
        .unwrap()
        .iter()
        .map(|r| {
            let envelope = r.get::<_, Option<Value>>(0).unwrap_or(Value::Null);
            assert_eq!(envelope["type"], kind);
            assert_eq!(envelope["version"], 1);
            envelope
        })
        .collect()
}

#[tokio::test]
async fn organizations_answer_as_specified() {
    let Some((world, i)) = world().await else {
        return;
    };
    let mut ada = user(&world, &i, "ada@example.com").await;
    // Creation: the caller owns it.
    let created = ada
        .post(
            &i,
            "/organizations",
            json!({"name": "Acme", "slug": "acme"}),
        )
        .await;
    assert_eq!(created.status, 201, "{created:?}");
    let org = id_at(&created, "/organization/id");
    assert!(org.starts_with("org_"));
    assert_eq!(created.body["organization"]["slug"], "acme");
    assert!(id_at(&created, "/membership/id").starts_with("mem_"));
    // Slug rules.
    for (body, status, code) in [
        (json!({"name": "Again", "slug": "acme"}), 409, "slug_taken"),
        (
            json!({"name": "Admin", "slug": "admin"}),
            409,
            "slug_reserved",
        ),
        (
            json!({"name": "Bad", "slug": "-bad"}),
            422,
            "validation_failed",
        ),
        (
            json!({"name": "", "slug": "fine"}),
            422,
            "validation_failed",
        ),
        (json!({"slug": "fine"}), 422, "validation_failed"),
    ] {
        let answer = ada.post(&i, "/organizations", body.clone()).await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (status, code),
            "{body}"
        );
    }
    // Listing.
    let listed = ada.get(&i, "/organizations").await;
    assert_eq!(listed.status, 200);
    assert_eq!(listed.body["items"].as_array().unwrap().len(), 1);
    assert_eq!(listed.body["items"][0]["organization"]["id"], org.as_str());
    // Read, update, permissions.
    let read = ada.get(&i, &format!("/organizations/{org}")).await;
    assert_eq!(
        (
            read.status.as_u16(),
            read.body["organization"]["name"].as_str()
        ),
        (200, Some("Acme"))
    );
    let updated = ada
        .patch(
            &i,
            &format!("/organizations/{org}"),
            json!({"name": "Acme Inc", "slug": "acme-inc"}),
        )
        .await;
    assert_eq!(updated.status, 200, "{updated:?}");
    assert_eq!(updated.body["organization"]["slug"], "acme-inc");
    let mine = ada
        .get(&i, &format!("/organizations/{org}/me/permissions"))
        .await;
    assert_eq!(mine.status, 200);
    assert_eq!(mine.body["roles"], json!(["owner"]));
    assert!(
        mine.body["permissions"]
            .as_array()
            .unwrap()
            .contains(&json!("identity.organization.delete"))
    );
    let application = ada.get(&i, "/me/permissions").await;
    assert_eq!(application.body["application"]["roles"], json!(["user"]));
    assert_eq!(application.body["application"]["break_glass"], false);
    let catalog = ada.get(&i, "/permissions").await;
    let names: Vec<&str> = catalog.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    for name in [
        "identity.members.invite",
        "system.status.read",
        "content.entries.read",
        "tasks.read",
    ] {
        assert!(names.contains(&name), "{name}");
    }
    // The active organization.
    let selected = ada
        .put(&i, "/session/organization", json!({"organization_id": org}))
        .await;
    assert_eq!(selected.status, 200, "{selected:?}");
    assert_eq!(
        selected.body["session"]["active_organization_id"],
        org.as_str()
    );
    let probe = ada
        .check(&i, "identity.members.read", &format!("tenant:{org}"))
        .await;
    assert_eq!(probe.body["tenant"], org.as_str());
    let cleared = ada
        .put(
            &i,
            "/session/organization",
            json!({"organization_id": null}),
        )
        .await;
    assert_eq!(
        cleared.body["session"]["active_organization_id"],
        Value::Null
    );
    for bad in [
        json!({"organization_id": "org_00000000000070008000000000000099"}),
        json!({"organization_id": "usr_00000000000070008000000000000099"}),
    ] {
        let answer = ada.put(&i, "/session/organization", bad).await;
        assert_eq!((answer.status.as_u16(), answer.code()), (404, "not_found"));
    }
    // Deletion needs the slug and a recent session.
    let refused = ada
        .delete_with(
            &i,
            &format!("/organizations/{org}"),
            json!({"confirm_slug": "acme"}),
        )
        .await;
    assert_eq!(
        (refused.status.as_u16(), refused.code()),
        (422, "validation_failed")
    );
    world.clock.advance(std::time::Duration::from_secs(11 * 60));
    let stale = ada
        .delete_with(
            &i,
            &format!("/organizations/{org}"),
            json!({"confirm_slug": "acme-inc"}),
        )
        .await;
    assert_eq!(
        (stale.status.as_u16(), stale.code()),
        (401, "reauthentication_required")
    );
    let mut fresh = sign_in(&i, "ada@example.com").await;
    let deleted = fresh
        .delete_with(
            &i,
            &format!("/organizations/{org}"),
            json!({"confirm_slug": "acme-inc"}),
        )
        .await;
    assert_eq!(deleted.status, 204, "{deleted:?}");
    assert_eq!(
        fresh.get(&i, &format!("/organizations/{org}")).await.status,
        404
    );
    // Events, R11 names, in the same transactions.
    assert_eq!(
        events(&world, "identity.organization_created").await.len(),
        1
    );
    assert_eq!(
        events(&world, "identity.organization_updated").await.len(),
        1
    );
    assert_eq!(
        events(&world, "identity.organization_deleted").await.len(),
        1
    );
    let membership = events(&world, "identity.membership_created").await;
    assert_eq!(membership[0]["payload"]["via"], "creation");
    assert_eq!(membership[0]["payload"]["role_keys"], json!(["owner"]));
    // The envelope's tenant is the organization.
    assert_eq!(membership[0]["tenant"], org.as_str());
    let created = events(&world, "identity.organization_created").await;
    assert_eq!(created[0]["payload"]["organization_id"], org.as_str());
    assert_eq!(created[0]["payload"]["slug"], "acme");
}

#[tokio::test]
async fn members_invitations_and_roles_answer_as_specified() {
    let Some((world, i)) = world().await else {
        return;
    };
    let mut owner = user(&world, &i, "owner@example.com").await;
    let mut bob = user(&world, &i, "bob@example.com").await;
    let org = organization(&i, &mut owner, "acme").await;
    // Invitations: inspect, list, accept.
    let invited = owner
        .post(
            &i,
            &format!("/organizations/{org}/invitations"),
            json!({"email": "bob@example.com", "role_ids": [MEMBER_ROLE]}),
        )
        .await;
    assert_eq!(invited.status, 201, "{invited:?}");
    assert_eq!(invited.body["invitation"]["role_ids"], json!([MEMBER_ROLE]));
    let token = world.mail.token("identity.invitation", "bob@example.com");
    let mail = world.mail.to("identity.invitation", "bob@example.com");
    assert_eq!(mail[0].data["organization_name"], "acme");
    assert!(
        mail[0].data.get("link").is_none(),
        "the link travels in secret_data only"
    );
    let mut anonymous = Browser::new("198.51.100.99");
    let inspected = anonymous
        .post(&i, "/invitations/inspect", json!({"token": token}))
        .await;
    assert_eq!(inspected.status, 200, "{inspected:?}");
    assert_eq!(inspected.body["email_masked"], "b***@example.com");
    assert_eq!(inspected.body["account_exists"], true);
    assert_eq!(inspected.body["organization"]["slug"], "acme");
    let listed = owner
        .get(&i, &format!("/organizations/{org}/invitations"))
        .await;
    assert_eq!(listed.body["items"].as_array().unwrap().len(), 1);
    // A verified account must sign in first; the token survives.
    let anon = anonymous
        .post(&i, "/invitations/accept", json!({"token": token}))
        .await;
    assert_eq!(
        (anon.status.as_u16(), anon.code()),
        (401, "sign_in_required")
    );
    // Another signed-in user gets email_mismatch.
    let mut carol = user(&world, &i, "carol@example.com").await;
    let other = carol
        .post(&i, "/invitations/accept", json!({"token": token}))
        .await;
    assert_eq!(
        (other.status.as_u16(), other.code()),
        (403, "email_mismatch")
    );
    let accepted = bob
        .post(&i, "/invitations/accept", json!({"token": token}))
        .await;
    assert_eq!(accepted.status, 201, "{accepted:?}");
    let membership = id_at(&accepted, "/membership/id");
    let again = bob
        .post(&i, "/invitations/accept", json!({"token": token}))
        .await;
    assert_eq!(
        (again.status.as_u16(), again.code()),
        (401, "invalid_token")
    );
    let already = owner
        .post(
            &i,
            &format!("/organizations/{org}/invitations"),
            json!({"email": "bob@example.com", "role_ids": [MEMBER_ROLE]}),
        )
        .await;
    assert_eq!(
        (already.status.as_u16(), already.code()),
        (409, "already_member")
    );
    // Revocation.
    let pending = owner
        .post(
            &i,
            &format!("/organizations/{org}/invitations"),
            json!({"email": "dan@example.com", "role_ids": [MEMBER_ROLE]}),
        )
        .await;
    let invitation = id_at(&pending, "/invitation/id");
    assert_eq!(
        owner
            .delete(
                &i,
                &format!("/organizations/{org}/invitations/{invitation}")
            )
            .await
            .status,
        204
    );
    assert_eq!(
        owner
            .delete(
                &i,
                &format!("/organizations/{org}/invitations/{invitation}")
            )
            .await
            .status,
        404
    );
    let revoked_token = world.mail.token("identity.invitation", "dan@example.com");
    let dead = anonymous
        .post(&i, "/invitations/accept", json!({"token": revoked_token}))
        .await;
    assert_eq!(dead.code(), "invalid_token");
    // Members.
    let members = owner
        .get(&i, &format!("/organizations/{org}/members"))
        .await;
    assert_eq!(members.status, 200);
    let items = members.body["items"].as_array().unwrap();
    assert_eq!(items.len(), 2);
    let bob_row = items
        .iter()
        .find(|m| m["id"] == membership.as_str())
        .unwrap_or_else(|| panic!("{membership} in {items:?}"));
    assert_eq!(
        bob_row["roles"],
        json!([{"id": MEMBER_ROLE, "key": "member"}])
    );
    assert_eq!(bob_row["user"]["email"], "bob@example.com");
    let paged = owner
        .get(&i, &format!("/organizations/{org}/members?limit=1"))
        .await;
    assert_eq!(paged.body["items"].as_array().unwrap().len(), 1);
    let cursor = paged.body["next_cursor"].as_str().unwrap().to_owned();
    let rest = owner
        .get(
            &i,
            &format!("/organizations/{org}/members?limit=1&cursor={cursor}"),
        )
        .await;
    assert_eq!(rest.body["items"].as_array().unwrap().len(), 1);
    assert_eq!(rest.body["next_cursor"], Value::Null);
    // A member reads, cannot invite.
    let bob_reads = bob.get(&i, &format!("/organizations/{org}/members")).await;
    assert_eq!(bob_reads.status, 200);
    let bob_invites = bob
        .post(
            &i,
            &format!("/organizations/{org}/invitations"),
            json!({"email": "x@example.com", "role_ids": [MEMBER_ROLE]}),
        )
        .await;
    assert_eq!(
        (bob_invites.status.as_u16(), bob_invites.code()),
        (403, "unauthorized")
    );
    // Custom roles.
    let role = owner
        .post(&i, &format!("/organizations/{org}/roles"), json!({"key": "editor", "name": "Editor", "permissions": ["tasks.*", "identity.members.read"]}))
        .await;
    assert_eq!(role.status, 201, "{role:?}");
    let role_id = id_at(&role, "/role/id");
    for (body, status, code) in [
        (
            json!({"key": "editor", "name": "Again", "permissions": ["tasks.read"]}),
            409,
            "role_key_taken",
        ),
        (
            json!({"key": "owner", "name": "Owner", "permissions": ["tasks.read"]}),
            409,
            "role_key_taken",
        ),
        (
            json!({"key": "x", "name": "X", "permissions": ["tasks.nope"]}),
            422,
            "unknown_permission",
        ),
        (
            json!({"key": "y", "name": "Y", "permissions": ["identity.*"]}),
            422,
            "unknown_permission",
        ),
        (
            json!({"key": "z", "name": "Z", "permissions": []}),
            422,
            "validation_failed",
        ),
        (
            json!({"key": "Bad", "name": "Z", "permissions": ["tasks.read"]}),
            422,
            "validation_failed",
        ),
    ] {
        let answer = owner
            .post(&i, &format!("/organizations/{org}/roles"), body.clone())
            .await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (status, code),
            "{body}"
        );
    }
    let roles = owner.get(&i, &format!("/organizations/{org}/roles")).await;
    let keys: Vec<&str> = roles.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["key"].as_str().unwrap())
        .collect();
    assert_eq!(keys, ["admin", "member", "owner", "editor"]);
    let patched = owner
        .patch(
            &i,
            &format!("/organizations/{org}/roles/{role_id}"),
            json!({"name": "Writer", "permissions": ["tasks.read"]}),
        )
        .await;
    assert_eq!(patched.status, 200, "{patched:?}");
    assert_eq!(patched.body["role"]["permissions"], json!(["tasks.read"]));
    let builtin = owner
        .patch(
            &i,
            &format!("/organizations/{org}/roles/{ADMIN_ROLE}"),
            json!({"name": "Boss"}),
        )
        .await;
    assert_eq!(
        (builtin.status.as_u16(), builtin.code()),
        (403, "builtin_role")
    );
    assert_eq!(
        owner
            .delete(&i, &format!("/organizations/{org}/roles/{MEMBER_ROLE}"))
            .await
            .code(),
        "builtin_role"
    );
    // Role assignments: member update, assignment, deletion while in use.
    let updated = owner
        .patch(
            &i,
            &format!("/organizations/{org}/members/{membership}"),
            json!({"role_ids": [MEMBER_ROLE, role_id]}),
        )
        .await;
    assert_eq!(updated.status, 200, "{updated:?}");
    assert_eq!(updated.body["member"]["roles"].as_array().unwrap().len(), 2);
    let in_use = owner
        .delete(&i, &format!("/organizations/{org}/roles/{role_id}"))
        .await;
    assert_eq!(
        (in_use.status.as_u16(), in_use.code()),
        (409, "role_in_use")
    );
    let assigned = owner
        .post(&i, &format!("/organizations/{org}/role-assignments"), json!({"membership_id": membership, "role_id": ADMIN_ROLE, "resource": {"kind": "project", "id": "p1"}}))
        .await;
    assert_eq!(assigned.status, 201, "{assigned:?}");
    let assignment = id_at(&assigned, "/assignment/id");
    assert_eq!(
        assigned.body["assignment"]["resource"],
        json!({"kind": "project", "id": "p1"})
    );
    let twice = owner
        .post(&i, &format!("/organizations/{org}/role-assignments"), json!({"membership_id": membership, "role_id": ADMIN_ROLE, "resource": {"kind": "project", "id": "p1"}}))
        .await;
    assert_eq!((twice.status.as_u16(), twice.code()), (409, "conflict"));
    let unknown_kind = owner
        .post(&i, &format!("/organizations/{org}/role-assignments"), json!({"membership_id": membership, "role_id": ADMIN_ROLE, "resource": {"kind": "planet", "id": "p1"}}))
        .await;
    assert_eq!(
        (unknown_kind.status.as_u16(), unknown_kind.code()),
        (422, "unknown_resource_kind")
    );
    let listed = owner
        .get(
            &i,
            &format!("/organizations/{org}/role-assignments?membership_id={membership}"),
        )
        .await;
    assert_eq!(listed.body["items"].as_array().unwrap().len(), 3);
    let by_resource = owner
        .get(
            &i,
            &format!("/organizations/{org}/role-assignments?resource_kind=project&resource_id=p1"),
        )
        .await;
    assert_eq!(by_resource.body["items"].as_array().unwrap().len(), 1);
    assert_eq!(
        owner
            .delete(
                &i,
                &format!("/organizations/{org}/role-assignments/{assignment}")
            )
            .await
            .status,
        204
    );
    assert_eq!(
        owner
            .delete(
                &i,
                &format!("/organizations/{org}/role-assignments/{assignment}")
            )
            .await
            .status,
        404
    );
    // Removing the custom role from the member frees it.
    owner
        .patch(
            &i,
            &format!("/organizations/{org}/members/{membership}"),
            json!({"role_ids": [MEMBER_ROLE]}),
        )
        .await;
    assert_eq!(
        owner
            .delete(&i, &format!("/organizations/{org}/roles/{role_id}"))
            .await
            .status,
        204
    );
    // Ownership transfer and leaving.
    let transferred = owner
        .post(
            &i,
            &format!("/organizations/{org}/ownership-transfers"),
            json!({"membership_id": membership, "keep_owner": false}),
        )
        .await;
    assert_eq!(transferred.status, 200, "{transferred:?}");
    let bob_now = bob
        .get(&i, &format!("/organizations/{org}/me/permissions"))
        .await;
    assert!(
        bob_now.body["roles"]
            .as_array()
            .unwrap()
            .contains(&json!("owner"))
    );
    let owner_now = owner
        .get(&i, &format!("/organizations/{org}/me/permissions"))
        .await;
    assert!(
        !owner_now.body["roles"]
            .as_array()
            .unwrap()
            .contains(&json!("owner"))
    );
    // Bob is the sole owner: he cannot leave or delete his account.
    let leave = bob
        .delete(&i, &format!("/organizations/{org}/members/{membership}"))
        .await;
    assert_eq!((leave.status.as_u16(), leave.code()), (409, "sole_owner"));
    let gone = bob
        .delete_with(&i, "/me", json!({"confirm_email": "bob@example.com"}))
        .await;
    assert_eq!((gone.status.as_u16(), gone.code()), (409, "sole_owner"));
    // The former owner kept no role: a member without grants reads nothing
    // (403, it still belongs to the organization), and leaves on its own.
    let refused = owner
        .get(&i, &format!("/organizations/{org}/members"))
        .await;
    assert_eq!(
        (refused.status.as_u16(), refused.code()),
        (403, "unauthorized")
    );
    let mine = bob.get(&i, &format!("/organizations/{org}/members")).await;
    let own_membership = mine.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["user"]["email"] == "owner@example.com")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(
        owner
            .delete(
                &i,
                &format!("/organizations/{org}/members/{own_membership}")
            )
            .await
            .status,
        204
    );
    assert_eq!(
        owner.get(&i, &format!("/organizations/{org}")).await.status,
        404
    );
    let removed = events(&world, "identity.membership_removed").await;
    assert_eq!(removed.last().unwrap()["payload"]["reason"], "left");
    assert!(
        !events(&world, "identity.membership_roles_changed")
            .await
            .is_empty()
    );
    // Audit, newest first, scoped to the organization.
    let audit = bob
        .get(&i, &format!("/organizations/{org}/audit-events?limit=100"))
        .await;
    assert_eq!(audit.status, 200, "{audit:?}");
    let actions: Vec<&str> = audit.body["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["action"].as_str().unwrap())
        .collect();
    for action in [
        "organization.created",
        "invitation.created",
        "invitation.accepted",
        "role.created",
        "role_assignment.created",
        "organization.ownership_transferred",
        "membership.removed",
    ] {
        assert!(actions.contains(&action), "{action} in {actions:?}");
    }
    let filtered = bob
        .get(
            &i,
            &format!("/organizations/{org}/audit-events?action=role.created"),
        )
        .await;
    assert_eq!(filtered.body["items"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn service_accounts_and_keys_answer_as_specified() {
    let Some((world, i)) = world().await else {
        return;
    };
    let mut owner = user(&world, &i, "owner@example.com").await;
    let org = organization(&i, &mut owner, "acme").await;
    let created = owner
        .post(
            &i,
            &format!("/organizations/{org}/service-accounts"),
            json!({"name": "CI", "description": "builds"}),
        )
        .await;
    assert_eq!(created.status, 201, "{created:?}");
    let service = id_at(&created, "/service_account/id");
    assert_eq!(created.body["service_account"]["scope"], "tenant");
    let listed = owner
        .get(&i, &format!("/organizations/{org}/service-accounts"))
        .await;
    assert_eq!(listed.body["items"].as_array().unwrap().len(), 1);
    let renamed = owner
        .patch(
            &i,
            &format!("/organizations/{org}/service-accounts/{service}"),
            json!({"name": "CI bot"}),
        )
        .await;
    assert_eq!(renamed.body["service_account"]["name"], "CI bot");
    // Keys: own and service account.
    let expires = "2027-06-01T00:00:00Z";
    let own = owner
        .post(
            &i,
            &format!("/organizations/{org}/api-keys"),
            json!({"name": "laptop", "permissions": ["tasks.read"], "expires_at": expires}),
        )
        .await;
    assert_eq!(own.status, 201, "{own:?}");
    let value = own.body["key"].as_str().unwrap();
    assert!(value.starts_with("mzk_") && value.len() == 64);
    assert!(own.body["api_key"].get("key").is_none());
    let machine = owner
        .post(&i, &format!("/organizations/{org}/api-keys"), json!({"name": "deploy", "permissions": ["tasks.read"], "expires_at": expires, "service_account_id": service}))
        .await;
    assert_eq!(machine.status, 201, "{machine:?}");
    assert_eq!(
        machine.body["api_key"]["service_account_id"],
        service.as_str()
    );
    for (body, code) in [
        (
            json!({"name": "x", "permissions": ["tasks.read"], "expires_at": "2020-01-01T00:00:00Z"}),
            "validation_failed",
        ),
        (
            json!({"name": "x", "permissions": ["tasks.read"], "expires_at": "2099-01-01T00:00:00Z"}),
            "validation_failed",
        ),
        (
            json!({"name": "x", "permissions": ["tasks.nope"], "expires_at": expires}),
            "unknown_permission",
        ),
        (
            json!({"name": "x", "permissions": [], "expires_at": expires}),
            "validation_failed",
        ),
        (
            json!({"name": "x", "permissions": ["tasks.read"]}),
            "validation_failed",
        ),
    ] {
        let answer = owner
            .post(&i, &format!("/organizations/{org}/api-keys"), body.clone())
            .await;
        assert_eq!(answer.code(), code, "{body}");
    }
    let keys = owner
        .get(&i, &format!("/organizations/{org}/api-keys"))
        .await;
    assert_eq!(keys.body["items"].as_array().unwrap().len(), 2);
    let of_service = owner
        .get(
            &i,
            &format!("/organizations/{org}/api-keys?service_account_id={service}"),
        )
        .await;
    assert_eq!(of_service.body["items"].as_array().unwrap().len(), 1);
    // The key works, at its organization.
    let mut robot = Browser::new("198.51.100.50");
    robot.bearer = Some(value.to_owned());
    assert!(
        robot
            .allowed(&i, "tasks.read", &format!("tenant:{org}"))
            .await
    );
    assert_eq!(
        robot.get(&i, "/me").await.body["user"]["email"],
        "owner@example.com"
    );
    let key_id = id_at(&own, "/api_key/id");
    assert_eq!(
        owner
            .delete(&i, &format!("/organizations/{org}/api-keys/{key_id}"))
            .await
            .status,
        204
    );
    let after = robot.get(&i, "/me").await;
    assert_eq!(
        (after.status.as_u16(), after.code()),
        (401, "unauthenticated")
    );
    // The service account goes with its keys.
    assert_eq!(
        owner
            .delete(
                &i,
                &format!("/organizations/{org}/service-accounts/{service}")
            )
            .await
            .status,
        204
    );
    assert_eq!(
        owner
            .delete(
                &i,
                &format!("/organizations/{org}/service-accounts/{service}")
            )
            .await
            .status,
        404
    );
    let mut deploy = Browser::new("198.51.100.51");
    deploy.bearer = Some(machine.body["key"].as_str().unwrap().to_owned());
    assert_eq!(
        deploy
            .check(&i, "tasks.read", &format!("tenant:{org}"))
            .await
            .status,
        401
    );
    // Recent sessions only.
    world.clock.advance(std::time::Duration::from_secs(11 * 60));
    let stale = owner
        .post(
            &i,
            &format!("/organizations/{org}/service-accounts"),
            json!({"name": "late"}),
        )
        .await;
    assert_eq!(stale.code(), "reauthentication_required");
}

/// Acceptance 9: an id of another organization is 404, never 403, on
/// every tenant route, also for an owner of that id's organization calling
/// through its own.
#[tokio::test]
async fn ids_from_another_organization_are_not_found() {
    let Some((world, i)) = world().await else {
        return;
    };
    let mut alice = user(&world, &i, "alice@example.com").await;
    let mut bob = user(&world, &i, "bob@example.com").await;
    let a = organization(&i, &mut alice, "alpha").await;
    let b = organization(&i, &mut bob, "beta").await;
    // Bob's ids in beta.
    let beta_members = bob.get(&i, &format!("/organizations/{b}/members")).await;
    let bob_membership = beta_members.body["items"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let role = bob
        .post(
            &i,
            &format!("/organizations/{b}/roles"),
            json!({"key": "editor", "name": "Editor", "permissions": ["tasks.read"]}),
        )
        .await;
    let beta_role = id_at(&role, "/role/id");
    let service = bob
        .post(
            &i,
            &format!("/organizations/{b}/service-accounts"),
            json!({"name": "CI"}),
        )
        .await;
    let beta_service = id_at(&service, "/service_account/id");
    let invited = bob
        .post(
            &i,
            &format!("/organizations/{b}/invitations"),
            json!({"email": "x@example.com", "role_ids": [MEMBER_ROLE]}),
        )
        .await;
    let beta_invitation = id_at(&invited, "/invitation/id");
    let key = bob
        .post(&i, &format!("/organizations/{b}/api-keys"), json!({"name": "k", "permissions": ["tasks.read"], "expires_at": "2027-06-01T00:00:00Z"}))
        .await;
    let beta_key = id_at(&key, "/api_key/id");
    let assignment = bob
        .get(&i, &format!("/organizations/{b}/role-assignments"))
        .await
        .body["items"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    // Alice, owner of alpha, names them through alpha: 404 everywhere.
    let requests: Vec<(&str, String, Option<Value>)> = vec![
        (
            "PATCH",
            format!("/organizations/{a}/members/{bob_membership}"),
            Some(json!({"role_ids": [MEMBER_ROLE]})),
        ),
        (
            "DELETE",
            format!("/organizations/{a}/members/{bob_membership}"),
            None,
        ),
        (
            "PATCH",
            format!("/organizations/{a}/roles/{beta_role}"),
            Some(json!({"name": "Mine"})),
        ),
        (
            "DELETE",
            format!("/organizations/{a}/roles/{beta_role}"),
            None,
        ),
        (
            "POST",
            format!("/organizations/{a}/role-assignments"),
            Some(json!({"membership_id": bob_membership, "role_id": MEMBER_ROLE})),
        ),
        (
            "DELETE",
            format!("/organizations/{a}/role-assignments/{assignment}"),
            None,
        ),
        (
            "PATCH",
            format!("/organizations/{a}/service-accounts/{beta_service}"),
            Some(json!({"name": "Mine"})),
        ),
        (
            "DELETE",
            format!("/organizations/{a}/service-accounts/{beta_service}"),
            None,
        ),
        (
            "POST",
            format!("/organizations/{a}/api-keys"),
            Some(
                json!({"name": "k", "permissions": ["tasks.read"], "expires_at": "2027-06-01T00:00:00Z", "service_account_id": beta_service}),
            ),
        ),
        (
            "DELETE",
            format!("/organizations/{a}/api-keys/{beta_key}"),
            None,
        ),
        (
            "DELETE",
            format!("/organizations/{a}/invitations/{beta_invitation}"),
            None,
        ),
        (
            "POST",
            format!("/organizations/{a}/ownership-transfers"),
            Some(json!({"membership_id": bob_membership})),
        ),
    ];
    for (method, path, body) in &requests {
        let answer = alice.send(&i, method, path, body.clone()).await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (404, "not_found"),
            "{method} {path}"
        );
    }
    // Alice on beta's own routes, without a membership: 404, not 403.
    let probes: Vec<(&str, String, Option<Value>)> = vec![
        ("GET", format!("/organizations/{b}"), None),
        ("GET", format!("/organizations/{b}/members"), None),
        ("GET", format!("/organizations/{b}/roles"), None),
        ("GET", format!("/organizations/{b}/audit-events"), None),
        ("GET", format!("/organizations/{b}/api-keys"), None),
        ("GET", format!("/organizations/{b}/me/permissions"), None),
        (
            "POST",
            format!("/organizations/{b}/invitations"),
            Some(json!({"email": "y@example.com", "role_ids": [MEMBER_ROLE]})),
        ),
        (
            "DELETE",
            format!("/organizations/{b}/members/{bob_membership}"),
            None,
        ),
        (
            "GET",
            "/organizations/org_00000000000070008000000000000099".to_owned(),
            None,
        ),
        (
            "GET",
            "/organizations/usr_00000000000070008000000000000099".to_owned(),
            None,
        ),
    ];
    for (method, path, body) in &probes {
        let answer = alice.send(&i, method, path, body.clone()).await;
        assert_eq!(
            (answer.status.as_u16(), answer.code()),
            (404, "not_found"),
            "{method} {path}"
        );
    }
    // Nothing of beta changed.
    let after = bob.get(&i, &format!("/organizations/{b}/members")).await;
    assert_eq!(after.body["items"].as_array().unwrap().len(), 1);
    assert_eq!(after.body["items"][0]["roles"][0]["key"], "owner");
    // The header naming another organization: 404 at the edge.
    alice.organization = Some(b.clone());
    let header = alice.get(&i, "/me").await;
    assert_eq!((header.status.as_u16(), header.code()), (404, "not_found"));
}
