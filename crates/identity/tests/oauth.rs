//! ID PR 3, OAuth and OIDC (section 6): sign-in, sign-up and linking
//! against an in-process OIDC provider and a GitHub API double, through
//! the real `openidconnect` and `oauth2` code paths; every linking rule;
//! state, binding, nonce, issuer, signature and expiry attacks; unlinking;
//! provider outages.
mod support;

use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
use serde_json::{Value, json};
use std::time::{Duration, UNIX_EPOCH};
use support::{idp::*, *};

/// The providers: `acme` (OIDC, trusted email), `loose` (OIDC, untrusted
/// email, the default) and `github`, all served by `idp`.
fn providers(base: &str, acme: &str) -> String {
    format!(
        "[modules.identity.oauth]\nhttp_timeout = \"2s\"\n\
         [modules.identity.oauth.providers.acme]\nkind = \"oidc\"\nissuer = \"{base}\"\n\
         client_id = \"{CLIENT_ID}\"\nclient_secret = {{ env = \"OAUTH_SECRET\" }}\ntrust_email = true\n{acme}\n\
         [modules.identity.oauth.providers.loose]\nkind = \"oidc\"\nissuer = \"{base}\"\n\
         client_id = \"{CLIENT_ID}\"\nclient_secret = {{ env = \"OAUTH_SECRET\" }}\n\
         [modules.identity.oauth.providers.github]\n\
         client_id = \"{CLIENT_ID}\"\nclient_secret = {{ env = \"OAUTH_SECRET\" }}\n"
    )
}

/// How a forged case builds its ID token from the claims.
type MakeToken = Box<dyn Fn(&serde_json::Map<String, Value>) -> Token>;

struct Env {
    world: World,
    node: Instance,
    idp: Idp,
}
async fn env_with(top: &str, acme: &str, limits: &[(&str, u32, &str)]) -> Option<Env> {
    let idp = Idp::start().await;
    let world = World::new(&section(top, limits, &providers(&idp.base, acme))).await?;
    world
        .endpoints
        .lock()
        .unwrap()
        .push(("github".into(), idp.github_endpoints()));
    let node = world.instance().await;
    Some(Env { world, node, idp })
}
async fn env() -> Option<Env> {
    env_with("", "", &[]).await
}

impl Env {
    fn now(&self) -> i64 {
        merezha_platform::Clock::now(self.world.clock.as_ref())
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }
    async fn start(&self, browser: &mut Browser, provider: &str, intent: &str) -> Authorization {
        let answer = browser
            .post(
                &self.node,
                &format!("/oauth/{provider}/start"),
                json!({ "intent": intent }),
            )
            .await;
        assert_eq!(answer.status, 200, "{answer:?}");
        assert!(
            answer
                .oauth_cookie()
                .unwrap()
                .contains("HttpOnly; SameSite=Lax")
        );
        Authorization::parse(answer.body["authorization_url"].as_str().unwrap())
    }
    async fn callback(&self, browser: &mut Browser, provider: &str, query: &str) -> Answer {
        let answer = browser
            .get(&self.node, &format!("/oauth/{provider}/callback?{query}"))
            .await;
        assert_eq!(answer.status, 303, "{answer:?}");
        // A new cookie session: fetch its CSRF token, as a front end does.
        if answer
            .set_cookie()
            .is_some_and(|c| !c.contains("Max-Age=0"))
        {
            let session = browser.get(&self.node, "/session").await;
            assert_eq!(session.status, 200, "{session:?}");
        }
        answer
    }
    /// Start, grant with these claims, come back: the callback's answer.
    async fn oidc(
        &self,
        browser: &mut Browser,
        provider: &str,
        intent: &str,
        claims: Value,
    ) -> Answer {
        let auth = self.start(browser, provider, intent).await;
        let claims = self.idp.claims(&auth, self.now(), claims);
        let code = self.idp.grant(&auth, claims, Token::Signed);
        self.callback(
            browser,
            provider,
            &format!("code={code}&state={}", auth.state),
        )
        .await
    }
    async fn github(&self, browser: &mut Browser, intent: &str) -> Answer {
        let auth = self.start(browser, "github", intent).await;
        let code = self.idp.grant(&auth, json!({}), Token::Signed);
        self.callback(
            browser,
            "github",
            &format!("code={code}&state={}", auth.state),
        )
        .await
    }
    async fn sessions(&self) -> i64 {
        self.world
            .scalar("SELECT count(*) FROM merezha_identity.sessions")
            .await
    }
    async fn identities(&self) -> i64 {
        self.world
            .scalar("SELECT count(*) FROM merezha_identity.oauth_identities")
            .await
    }
}

const SIGNED_IN: &str = "https://app.example/";
fn error_page(code: &str) -> String {
    format!("https://app.example/auth/error#error={code}")
}

/// The signed-in user of `browser` (cookie session).
async fn me(node: &Instance, browser: &mut Browser) -> Value {
    let answer = browser.get(node, "/session").await;
    assert_eq!(answer.status, 200, "{answer:?}");
    assert_eq!(answer.body["session"]["method"], "oauth");
    answer.body["user"].clone()
}

/// Acceptance 1: OIDC sign-up, then sign-in by subject, with PKCE S256,
/// state, nonce and the binding cookie; the provider's tokens are never
/// stored.
#[tokio::test]
async fn oidc_sign_up_and_sign_in() {
    let Some(env) = env().await else { return };
    let mut browser = Browser::new("198.51.100.30");
    let auth = env.start(&mut browser, "acme", "sign_in").await;
    assert_eq!(auth.method, "S256");
    assert_eq!(auth.client_id, CLIENT_ID);
    assert_eq!(
        auth.redirect_uri,
        "https://app.example/v1/identity/oauth/acme/callback"
    );
    assert!(
        auth.url
            .starts_with(&format!("{}/authorize?", env.idp.base))
    );
    assert!(auth.nonce.is_some() && auth.scope.split(' ').any(|s| s == "openid"));
    let claims = env.idp.claims(
        &auth,
        env.now(),
        json!({"sub": "acme-1", "email": "Ada@Example.com", "email_verified": true, "name": "Ada L"}),
    );
    let code = env.idp.grant(&auth, claims, Token::Signed);
    let answer = env
        .callback(
            &mut browser,
            "acme",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    assert_eq!(answer.headers["referrer-policy"], "no-referrer");
    assert!(answer.oauth_cookie().unwrap().contains("Max-Age=0"));
    let user = me(&env.node, &mut browser).await;
    assert_eq!(user["email"], "Ada@Example.com");
    assert_eq!(user["email_verified"], true);
    assert_eq!(user["display_name"], "Ada L");
    assert_eq!(
        user["credentials"],
        json!({"password": false, "passkeys": 0, "oauth": ["acme"]})
    );
    let registered = env
        .world
        .scalar("SELECT count(*) FROM merezha_jobs.outbox WHERE topic = 'identity.user_registered' AND payload::text LIKE '%\"method\":\"oauth\"%'")
        .await;
    assert_eq!(registered, 1);
    // No provider token is kept anywhere.
    let leaked = env
        .world
        .scalar("SELECT count(*) FROM merezha_identity.oauth_identities WHERE row_to_json(oauth_identities)::text LIKE '%provider-%'")
        .await;
    assert_eq!(leaked, 0);

    // Sign in again from another browser: same user, by subject even
    // when the provider now asserts another address.
    let mut other = Browser::new("198.51.100.31");
    let answer = env
        .oidc(
            &mut other,
            "acme",
            "sign_in",
            json!({"sub": "acme-1", "email": "ada@new.example", "email_verified": true}),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    assert_eq!(me(&env.node, &mut other).await["id"], user["id"]);
    assert_eq!(env.identities().await, 1);
    let stored = env
        .world
        .scalar("SELECT count(*) FROM merezha_identity.oauth_identities WHERE email_normalized = 'ada@new.example' AND last_used_at IS NOT NULL")
        .await;
    assert_eq!(stored, 1, "rule 1 refreshes the stored assertion");
    // The provider list.
    let listed = other.get(&env.node, "/oauth/providers").await;
    assert_eq!(
        listed.body["items"],
        json!([
            {"id": "acme", "kind": "oidc", "display_name": "acme"},
            {"id": "github", "kind": "github", "display_name": "GitHub"},
            {"id": "loose", "kind": "oidc", "display_name": "loose"},
        ])
    );
    // `return_to` is kept with the flow and resolved against app_origin.
    let answer = other
        .post(
            &env.node,
            "/oauth/acme/start",
            json!({"intent": "sign_in", "return_to": "/projects/7"}),
        )
        .await;
    let auth = Authorization::parse(answer.body["authorization_url"].as_str().unwrap());
    let claims = env.idp.claims(&auth, env.now(), json!({"sub": "acme-1"}));
    let code = env.idp.grant(&auth, claims, Token::Signed);
    let answer = env
        .callback(
            &mut other,
            "acme",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), "https://app.example/projects/7");
    for bad in ["//evil.example", "https://evil.example", "/\\evil"] {
        let answer = other
            .post(
                &env.node,
                "/oauth/acme/start",
                json!({"intent": "sign_in", "return_to": bad}),
            )
            .await;
        assert_eq!(answer.code(), "validation_failed", "{bad}");
    }
    let answer = other
        .post(&env.node, "/oauth/nope/start", json!({"intent": "sign_in"}))
        .await;
    assert_eq!(answer.status, 404);
}

/// Acceptance 1: GitHub sign-up through `oauth2` and the two API calls,
/// subject the numeric id; the verified primary address only.
#[tokio::test]
async fn github_sign_up_sign_in_and_email_selection() {
    let Some(env) = env().await else { return };
    env.idp.with(|s| {
        s.github_emails = vec![
            ("old@octo.example".into(), false, true),
            ("cat@octo.example".into(), true, true),
        ]
    });
    let mut browser = Browser::new("198.51.100.32");
    let auth = env.start(&mut browser, "github", "sign_in").await;
    assert_eq!(auth.nonce, None, "GitHub is not OIDC");
    assert_eq!(auth.scope, "read:user user:email");
    let code = env.idp.grant(&auth, json!({}), Token::Signed);
    let answer = env
        .callback(
            &mut browser,
            "github",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    let user = me(&env.node, &mut browser).await;
    assert_eq!(user["email"], "cat@octo.example");
    assert_eq!(user["email_verified"], true);
    let subject = env
        .world
        .scalar("SELECT count(*) FROM merezha_identity.oauth_identities WHERE provider = 'github' AND subject = '4242'")
        .await;
    assert_eq!(subject, 1);
    // An unverified primary is no address: a new GitHub account without
    // one cannot sign up.
    env.idp.with(|s| {
        s.github_id = 5151;
        s.github_emails = vec![
            ("new@octo.example".into(), true, false),
            ("alt@octo.example".into(), false, true),
        ];
    });
    let mut other = Browser::new("198.51.100.33");
    assert_eq!(
        env.github(&mut other, "sign_in").await.location(),
        error_page("email_unverified")
    );
    // No addresses at all, likewise.
    env.idp.with(|s| s.github_emails.clear());
    assert_eq!(
        env.github(&mut other, "sign_in").await.location(),
        error_page("email_unverified")
    );
    // The linked account still signs in by id whatever its addresses say.
    env.idp.with(|s| s.github_id = 4242);
    assert_eq!(
        env.github(&mut other, "sign_in").await.location(),
        SIGNED_IN
    );
    assert_eq!(me(&env.node, &mut other).await["id"], user["id"]);
}

/// Acceptance 1 and rule 2: a signed-in user links an identity from
/// settings, then signs in with it; one identity per provider per user;
/// another user's identity cannot be taken (rule 1, link intent).
#[tokio::test]
async fn link_from_settings_then_sign_in() {
    let Some(env) = env().await else { return };
    let mut ada = signed_up(&env.world, &env.node, "ada@example.com", PASSWORD).await;
    // An anonymous link start is refused.
    let mut anonymous = Browser::new("198.51.100.34");
    let answer = anonymous
        .post(&env.node, "/oauth/acme/start", json!({"intent": "link"}))
        .await;
    assert_eq!(answer.code(), "unauthenticated");
    // Another address at the provider, even untrusted, links by subject.
    let answer = env
        .oidc(
            &mut ada,
            "loose",
            "link",
            json!({"sub": "loose-ada", "email": "ada@elsewhere.example"}),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    assert!(answer.set_cookie().is_none(), "a link creates no session");
    let listed = ada.get(&env.node, "/me/oauth-identities").await;
    assert_eq!(listed.body["items"][0]["provider"], "loose");
    assert_eq!(listed.body["items"][0]["email"], "ada@elsewhere.example");
    // One identity per provider per user.
    let answer = env
        .oidc(&mut ada, "loose", "link", json!({"sub": "loose-ada-2"}))
        .await;
    assert_eq!(answer.location(), error_page("identity_already_linked"));
    // Linking it again is a no-op.
    let answer = env
        .oidc(&mut ada, "loose", "link", json!({"sub": "loose-ada"}))
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    // Bob cannot link Ada's identity.
    let mut bob = signed_up(&env.world, &env.node, "bob@example.com", PASSWORD).await;
    let answer = env
        .oidc(&mut bob, "loose", "link", json!({"sub": "loose-ada"}))
        .await;
    assert_eq!(answer.location(), error_page("identity_already_linked"));
    // Ada signs in with it from a fresh browser.
    let mut fresh = Browser::new("198.51.100.35");
    let answer = env
        .oidc(&mut fresh, "loose", "sign_in", json!({"sub": "loose-ada"}))
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    let user = me(&env.node, &mut fresh).await;
    assert_eq!(user["email"], "ada@example.com");
    assert_eq!(user["credentials"]["oauth"], json!(["loose"]));
    let audits = env
        .world
        .scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'oauth.linked' AND details->>'provider' = 'loose'")
        .await;
    assert_eq!(audits, 1);
}

/// Rule 3 under owner answer ID1: a trusted verified address of an
/// existing verified account answers `account_exists` and links nothing.
#[tokio::test]
async fn a_verified_address_is_never_linked_automatically() {
    let Some(env) = env().await else { return };
    signed_up(&env.world, &env.node, "ada@example.com", PASSWORD).await;
    let sessions = env.sessions().await;
    let mut browser = Browser::new("198.51.100.36");
    let answer = env
        .oidc(
            &mut browser,
            "acme",
            "sign_in",
            json!({"sub": "acme-ada", "email": "ada@example.com", "email_verified": true}),
        )
        .await;
    assert_eq!(answer.location(), error_page("account_exists"));
    assert!(answer.set_cookie().is_none());
    assert_eq!(env.identities().await, 0);
    assert_eq!(env.sessions().await, sessions);
    // Rule 4: an untrusted address colliding with an account, likewise.
    let answer = env
        .oidc(
            &mut browser,
            "loose",
            "sign_in",
            json!({"sub": "loose-x", "email": "ada@example.com", "email_verified": true}),
        )
        .await;
    assert_eq!(answer.location(), error_page("account_exists"));
    assert_eq!(env.identities().await, 0);
    let failed = env
        .world
        .scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'oauth.sign_in_failed' AND details->>'reason' = 'account_exists'")
        .await;
    assert_eq!(failed, 2);
}

/// Rule 3 with `link_by_verified_email = true`: link and sign in.
#[tokio::test]
async fn link_by_verified_email_links_and_signs_in() {
    let idp = Idp::start().await;
    let text = section(
        "",
        &[],
        &providers(&idp.base, "").replace(
            "http_timeout = \"2s\"",
            "http_timeout = \"2s\"\nlink_by_verified_email = true",
        ),
    );
    let Some(world) = World::new(&text).await else {
        return;
    };
    let node = world.instance().await;
    let env = Env { world, node, idp };
    signed_up(&env.world, &env.node, "ada@example.com", PASSWORD).await;
    let mut browser = Browser::new("198.51.100.37");
    let answer = env
        .oidc(
            &mut browser,
            "acme",
            "sign_in",
            json!({"sub": "acme-ada", "email": "ada@example.com", "email_verified": true}),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    let user = me(&env.node, &mut browser).await;
    assert_eq!(user["email"], "ada@example.com");
    assert_eq!(
        user["credentials"]["password"], true,
        "a verified account keeps its password"
    );
    // An untrusted provider still cannot link by address.
    let mut other = Browser::new("198.51.100.38");
    let answer = env
        .oidc(
            &mut other,
            "loose",
            "sign_in",
            json!({"sub": "loose-ada", "email": "ada@example.com", "email_verified": true}),
        )
        .await;
    assert_eq!(answer.location(), error_page("account_exists"));
}

/// Rule 3, the pre-hijacking claim (section 2): an attacker registered the
/// victim's address, signed in (verification not required here), linked
/// an identity of their own; the victim's trusted sign-in strips every
/// attacker credential and session, verifies the address and signs the
/// victim in. A provider assertion is never "the registering browser".
#[tokio::test]
async fn an_oauth_sign_in_claims_an_unverified_account() {
    let Some(env) = env_with("require_verified_email = false", "", &[]).await else {
        return;
    };
    let mut attacker = Browser::new("203.0.113.66");
    let answer = attacker
        .post(
            &env.node,
            "/registrations",
            json!({"email": "victim@example.com", "password": "attacker chosen passphrase"}),
        )
        .await;
    assert_eq!(answer.status, 202);
    // The attacker keeps the registration binding cookie and a session.
    let answer = attacker
        .post(
            &env.node,
            "/sessions",
            json!({"email": "victim@example.com", "password": "attacker chosen passphrase"}),
        )
        .await;
    assert_eq!(answer.status, 201);
    let answer = env
        .oidc(
            &mut attacker,
            "loose",
            "link",
            json!({"sub": "attacker-sub", "email": "evil@attacker.example"}),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    assert_eq!(env.identities().await, 1);

    let mut victim = Browser::new("198.51.100.39");
    // Even in the attacker's own browser (binding cookie and all) the
    // provider proof claims the account; here from the victim's.
    let answer = env
        .oidc(
            &mut victim,
            "acme",
            "sign_in",
            json!({"sub": "victim-sub", "email": "victim@example.com", "email_verified": true}),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    let user = me(&env.node, &mut victim).await;
    assert_eq!(user["email_verified"], true);
    assert_eq!(
        user["credentials"],
        json!({"password": false, "passkeys": 0, "oauth": ["acme"]})
    );
    // The attacker's session, password and identity are gone.
    let answer = attacker.get(&env.node, "/session").await;
    assert_eq!(answer.code(), "unauthenticated");
    let answer = attacker
        .post(
            &env.node,
            "/sessions",
            json!({"email": "victim@example.com", "password": "attacker chosen passphrase"}),
        )
        .await;
    assert_eq!(answer.code(), "invalid_credentials");
    let attackers = env
        .world
        .scalar(
            "SELECT count(*) FROM merezha_identity.oauth_identities WHERE subject = 'attacker-sub'",
        )
        .await;
    assert_eq!(attackers, 0);
    let claimed = env
        .world
        .scalar("SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'user.unverified_account_claimed'")
        .await;
    assert_eq!(claimed, 1);
}

/// Rule 3 with verification required: the attacker's unverified account
/// holds only a password; the victim's sign-in claims it all the same, and
/// from the attacker's registering browser too.
#[tokio::test]
async fn the_claim_ignores_the_registration_binding() {
    let Some(env) = env().await else { return };
    let mut attacker = Browser::new("203.0.113.67");
    attacker
        .post(
            &env.node,
            "/registrations",
            json!({"email": "victim@example.com", "password": "attacker chosen passphrase"}),
        )
        .await;
    assert!(attacker.binding.is_some());
    // The victim's provider sign-in happens in the registering browser.
    let answer = env
        .oidc(
            &mut attacker,
            "acme",
            "sign_in",
            json!({"sub": "victim-sub", "email": "victim@example.com", "email_verified": true}),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    let user = me(&env.node, &mut attacker).await;
    assert_eq!(user["credentials"]["password"], false, "the claim ran");
}

/// Rule 4: an untrusted address creates an unverified account; with
/// verification required it waits for its link, which keeps the identity
/// when clicked in the browser that signed up; then the identity signs in.
#[tokio::test]
async fn an_untrusted_sign_up_waits_for_verification() {
    let Some(env) = env().await else { return };
    let mut browser = Browser::new("198.51.100.40");
    let answer = env
        .oidc(
            &mut browser,
            "loose",
            "sign_in",
            json!({"sub": "loose-new", "email": "new@example.com", "email_verified": true}),
        )
        .await;
    assert_eq!(answer.location(), error_page("email_unverified"));
    assert!(answer.set_cookie().is_none());
    assert!(browser.binding.is_some(), "the registration binding is set");
    let token = env
        .world
        .mail
        .token("identity.verify_email", "new@example.com");
    // Signing in again before verifying: still `email_unverified`.
    let mut other = Browser::new("198.51.100.41");
    let again = env
        .oidc(&mut other, "loose", "sign_in", json!({"sub": "loose-new"}))
        .await;
    assert_eq!(again.location(), error_page("email_unverified"));
    let answer = browser
        .post(
            &env.node,
            "/email-verifications/confirm",
            json!({"token": token}),
        )
        .await;
    assert_eq!(answer.status, 201, "{answer:?}");
    assert_eq!(answer.body["session"]["password_unset"], true);
    assert_eq!(
        answer.body["user"]["credentials"]["oauth"],
        json!(["loose"]),
        "no claim"
    );
    let answer = env
        .oidc(&mut other, "loose", "sign_in", json!({"sub": "loose-new"}))
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
}

/// Rule 4 refusals: no address, registration closed, sign-up off; a
/// disabled account.
#[tokio::test]
async fn sign_up_refusals_and_disabled_accounts() {
    let Some(env) = env_with("", "allow_sign_up = false", &[]).await else {
        return;
    };
    let mut browser = Browser::new("198.51.100.42");
    let answer = env
        .oidc(&mut browser, "loose", "sign_in", json!({"sub": "no-email"}))
        .await;
    assert_eq!(answer.location(), error_page("email_unverified"));
    let answer = env
        .oidc(
            &mut browser,
            "acme",
            "sign_in",
            json!({"sub": "acme-closed", "email": "closed@example.com", "email_verified": true}),
        )
        .await;
    assert_eq!(answer.location(), error_page("registration_closed"));
    let users = env
        .world
        .scalar("SELECT count(*) FROM merezha_identity.users")
        .await;
    assert_eq!(users, 0);
    // A linked, then disabled, account.
    let answer = env
        .oidc(
            &mut browser,
            "loose",
            "sign_in",
            json!({"sub": "loose-dis", "email": "dis@example.com"}),
        )
        .await;
    assert_eq!(answer.location(), error_page("email_unverified"));
    env.world
        .execute("UPDATE merezha_identity.users SET status = 'disabled'")
        .await;
    let answer = env
        .oidc(
            &mut browser,
            "loose",
            "sign_in",
            json!({"sub": "loose-dis"}),
        )
        .await;
    assert_eq!(answer.location(), error_page("account_disabled"));
    assert_eq!(env.sessions().await, 0);
}

/// Registration modes other than `open` refuse OAuth sign-up.
#[tokio::test]
async fn invite_only_refuses_oauth_sign_up() {
    let Some(env) = env_with("registration = \"invite_only\"", "", &[]).await else {
        return;
    };
    let mut browser = Browser::new("198.51.100.43");
    let answer = env
        .oidc(
            &mut browser,
            "acme",
            "sign_in",
            json!({"sub": "acme-i", "email": "i@example.com", "email_verified": true}),
        )
        .await;
    assert_eq!(answer.location(), error_page("registration_closed"));
}

/// Acceptance 3: every forged or misdirected callback fails with its code
/// and creates no session.
#[tokio::test]
async fn forged_callbacks_fail_and_create_no_session() {
    let Some(env) = env().await else { return };
    let now = env.now();
    let claims = |auth: &Authorization, extra: Value| {
        let mut claims = env
            .idp
            .claims(
                auth,
                now,
                json!({"sub": "acme-x", "email": "x@example.com", "email_verified": true}),
            )
            .as_object()
            .unwrap()
            .clone();
        for (key, value) in extra.as_object().unwrap() {
            claims.insert(key.clone(), value.clone());
        }
        claims.retain(|_, v| !v.is_null());
        claims
    };
    let stranger = TestKey::rsa("rsa-unknown");
    // (name, claims change, token kind, expected code)
    let cases: Vec<(&str, Value, MakeToken, &str)> = vec![
        (
            "wrong nonce",
            json!({"nonce": "not-the-flows"}),
            Box::new(|_| Token::Signed),
            "oauth_failed",
        ),
        (
            "no nonce",
            json!({"nonce": null}),
            Box::new(|_| Token::Signed),
            "oauth_failed",
        ),
        (
            "wrong issuer in the token",
            json!({"iss": "http://127.0.0.1:1"}),
            Box::new(|_| Token::Signed),
            "oauth_failed",
        ),
        (
            "wrong audience",
            json!({"aud": "someone-else"}),
            Box::new(|_| Token::Signed),
            "oauth_failed",
        ),
        (
            "extra untrusted audience",
            json!({"aud": [CLIENT_ID, "other"], "azp": CLIENT_ID}),
            Box::new(|_| Token::Signed),
            "oauth_failed",
        ),
        (
            "azp of another client",
            json!({"azp": "other"}),
            Box::new(|_| Token::Signed),
            "oauth_failed",
        ),
        (
            "expired",
            json!({"exp": now - 1, "iat": now - 600}),
            Box::new(|_| Token::Signed),
            "oauth_failed",
        ),
        (
            "issued in the future",
            json!({"iat": now + 600, "exp": now + 900}),
            Box::new(|_| Token::Signed),
            "oauth_failed",
        ),
        (
            "unknown key",
            json!({}),
            Box::new(move |_| Token::With(stranger.clone())),
            "oauth_failed",
        ),
        (
            "alg none",
            json!({}),
            Box::new(|c| Token::Raw(unsigned(&Value::Object(c.clone())))),
            "oauth_failed",
        ),
        (
            "alg HS256 with the client secret",
            json!({}),
            Box::new(|c| Token::Raw(hs256(&Value::Object(c.clone())))),
            "oauth_failed",
        ),
    ];
    for (name, extra, token, expected) in cases {
        let mut browser = Browser::new("198.51.100.44");
        let auth = env.start(&mut browser, "acme", "sign_in").await;
        let claims = claims(&auth, extra);
        let code = env
            .idp
            .grant(&auth, Value::Object(claims.clone()), token(&claims));
        let answer = env
            .callback(
                &mut browser,
                "acme",
                &format!("code={code}&state={}", auth.state),
            )
            .await;
        assert_eq!(answer.location(), error_page(expected), "{name}");
        assert!(answer.set_cookie().is_none(), "{name}");
    }
    assert_eq!(env.sessions().await, 0);
    assert_eq!(
        env.world
            .scalar("SELECT count(*) FROM merezha_identity.users")
            .await,
        0
    );

    // The flow-level attacks.
    let mut browser = Browser::new("198.51.100.45");
    let auth = env.start(&mut browser, "acme", "sign_in").await;
    let good = env.idp.claims(
        &auth,
        now,
        json!({"sub": "acme-y", "email": "y@example.com", "email_verified": true}),
    );
    // A wrong `iss` parameter (RFC 9207 mix-up).
    let code = env.idp.grant(&auth, good.clone(), Token::Signed);
    let answer = env
        .callback(
            &mut browser,
            "acme",
            &format!(
                "code={code}&state={}&iss=https%3A%2F%2Fevil.example",
                auth.state
            ),
        )
        .await;
    assert_eq!(answer.location(), error_page("oauth_failed"));
    // That flow is spent: replaying its state finds nothing.
    let answer = env
        .callback(
            &mut browser,
            "acme",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), error_page("invalid_state"));
    // No binding cookie (callback injection into another browser).
    let auth = env.start(&mut browser, "acme", "sign_in").await;
    let code = env.idp.grant(
        &auth,
        env.idp.claims(&auth, now, json!({"sub": "acme-y"})),
        Token::Signed,
    );
    let mut victim = Browser::new("198.51.100.46");
    let answer = env
        .callback(
            &mut victim,
            "acme",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), error_page("invalid_state"));
    // ... and the flow is gone for its own browser too.
    let answer = env
        .callback(
            &mut browser,
            "acme",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), error_page("invalid_state"));
    // A state of one provider at another's callback (mix-up).
    let auth = env.start(&mut browser, "loose", "sign_in").await;
    let code = env.idp.grant(
        &auth,
        env.idp.claims(&auth, now, json!({"sub": "acme-y"})),
        Token::Signed,
    );
    let answer = env
        .callback(
            &mut browser,
            "acme",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), error_page("invalid_state"));
    // Garbage, missing and repeated parameters; an expired flow.
    for query in ["code=x", "code=x&state=short", "state=a&state=b", ""] {
        let answer = env.callback(&mut browser, "acme", query).await;
        assert_eq!(answer.location(), error_page("invalid_state"), "{query}");
    }
    let auth = env.start(&mut browser, "acme", "sign_in").await;
    env.world.clock.advance(Duration::from_secs(11 * 60));
    let code = env.idp.grant(
        &auth,
        env.idp.claims(&auth, env.now(), json!({"sub": "acme-y"})),
        Token::Signed,
    );
    let answer = env
        .callback(
            &mut browser,
            "acme",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), error_page("invalid_state"));
    // The provider denied, or answered an error.
    let auth = env.start(&mut browser, "acme", "sign_in").await;
    let answer = env
        .callback(
            &mut browser,
            "acme",
            &format!("error=access_denied&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), error_page("oauth_denied"));
    let auth = env.start(&mut browser, "acme", "sign_in").await;
    let answer = env
        .callback(
            &mut browser,
            "acme",
            &format!("code=never-granted&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), error_page("oauth_failed"));
    assert_eq!(env.sessions().await, 0);
}

/// The provider rotated its keys: an unknown `kid` refetches the key set
/// once, and the sign-in succeeds; a second unknown key within the
/// throttle window does not refetch.
#[tokio::test]
async fn key_rotation_refetches_the_key_set_once() {
    let Some(env) = env().await else { return };
    let mut browser = Browser::new("198.51.100.47");
    let first = env
        .oidc(
            &mut browser,
            "acme",
            "sign_in",
            json!({"sub": "rot", "email": "rot@example.com", "email_verified": true}),
        )
        .await;
    assert_eq!(first.location(), SIGNED_IN);
    let fetched = env.idp.with(|s| s.jwks_hits);
    env.idp.with(|s| {
        s.keys = vec![TestKey::ed25519("ed-2")];
        s.signer = 0;
    });
    let answer = env
        .oidc(&mut browser, "acme", "sign_in", json!({"sub": "rot"}))
        .await;
    assert_eq!(answer.location(), SIGNED_IN, "EdDSA from the rotated set");
    assert_eq!(env.idp.with(|s| s.jwks_hits), fetched + 1);
    // Another rotation right away: the refetch is throttled, so the sign-in
    // fails rather than hammer the provider.
    env.idp.with(|s| s.keys = vec![TestKey::rsa("rsa-3")]);
    let answer = env
        .oidc(&mut browser, "acme", "sign_in", json!({"sub": "rot"}))
        .await;
    assert_eq!(answer.location(), error_page("oauth_failed"));
    assert_eq!(env.idp.with(|s| s.jwks_hits), fetched + 1);
}

/// Acceptance 5: an identity is unlinked only from a recent session, and
/// with magic links on the verified address is a credential, so the
/// identity can go.
#[tokio::test]
async fn unlinking_needs_a_recent_session() {
    let Some(env) = env().await else { return };
    let mut browser = Browser::new("198.51.100.48");
    env.oidc(
        &mut browser,
        "acme",
        "sign_in",
        json!({"sub": "only", "email": "only@example.com", "email_verified": true}),
    )
    .await;
    let me = browser.get(&env.node, "/session").await;
    browser.csrf = me.body["csrf_token"].as_str().map(str::to_owned);
    let listed = browser.get(&env.node, "/me/oauth-identities").await;
    let id = listed.body["items"][0]["id"].as_str().unwrap().to_owned();
    let answer = browser
        .delete(
            &env.node,
            "/me/oauth-identities/oid_00000000000070008000000000000000",
        )
        .await;
    assert_eq!(answer.status, 404);
    let answer = browser
        .delete(
            &env.node,
            "/me/oauth-identities/usr_00000000000070008000000000000000",
        )
        .await;
    assert_eq!(answer.status, 404, "an id of another type");
    env.world.clock.advance(Duration::from_secs(11 * 60));
    let answer = browser
        .delete(&env.node, &format!("/me/oauth-identities/{id}"))
        .await;
    assert_eq!(answer.code(), "reauthentication_required");
    // A fresh sign-in is recent; magic links keep the account reachable.
    env.oidc(&mut browser, "acme", "sign_in", json!({"sub": "only"}))
        .await;
    let me = browser.get(&env.node, "/session").await;
    browser.csrf = me.body["csrf_token"].as_str().map(str::to_owned);
    let answer = browser
        .delete(&env.node, &format!("/me/oauth-identities/{id}"))
        .await;
    assert_eq!(answer.status, 204, "{answer:?}");
}

/// Acceptance 5 without magic links: the identity is the last credential
/// until a password exists.
#[tokio::test]
async fn the_last_identity_stays_without_another_credential() {
    let idp = Idp::start().await;
    let text =
        section("", &[], &providers(&idp.base, "")).replace("[modules.identity.magic_links]\n", "");
    let Some(world) = World::new(&text).await else {
        return;
    };
    let node = world.instance().await;
    let env = Env { world, node, idp };
    let mut browser = Browser::new("198.51.100.49");
    env.oidc(
        &mut browser,
        "acme",
        "sign_in",
        json!({"sub": "only", "email": "only@example.com", "email_verified": true}),
    )
    .await;
    let me = browser.get(&env.node, "/session").await;
    browser.csrf = me.body["csrf_token"].as_str().map(str::to_owned);
    let id = browser.get(&env.node, "/me/oauth-identities").await.body["items"][0]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let answer = browser
        .delete(&env.node, &format!("/me/oauth-identities/{id}"))
        .await;
    assert_eq!(answer.status, 409);
    assert_eq!(answer.code(), "last_credential");
    // With a password set (the fresh session is recent), it can go.
    let answer = browser
        .put(&env.node, "/me/password", json!({"new_password": PASSWORD}))
        .await;
    assert_eq!(answer.status, 200, "{answer:?}");
    let answer = browser
        .delete(&env.node, &format!("/me/oauth-identities/{id}"))
        .await;
    assert_eq!(answer.status, 204, "{answer:?}");
    assert_eq!(env.identities().await, 0);
    let unlinked = env
        .world
        .scalar(
            "SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'oauth.unlinked'",
        )
        .await;
    assert_eq!(unlinked, 1);
}

/// Acceptance 6: an unreachable or broken provider answers
/// `provider_unavailable` (start) and readiness stays green; redirects are
/// never followed; documents over 1 MiB are refused.
#[tokio::test]
async fn provider_outages_answer_provider_unavailable() {
    let Some(env) = env().await else { return };
    let mut browser = Browser::new("198.51.100.50");
    for mode in [Discovery::Broken, Discovery::Huge, Discovery::Redirect] {
        env.idp.with(|s| s.discovery = mode);
        let hits = env.idp.with(|s| s.discovery_hits);
        let answer = browser
            .post(&env.node, "/oauth/acme/start", json!({"intent": "sign_in"}))
            .await;
        assert_eq!(answer.status, 503, "{mode:?}: {answer:?}");
        assert_eq!(answer.code(), "provider_unavailable", "{mode:?}");
        assert!(env.node.module.ready().await.is_ok(), "{mode:?}");
        assert_eq!(env.idp.with(|s| s.discovery_hits), hits + 1, "{mode:?}");
        // The failure is shared for 30 s: the next start does not wait on
        // the provider again.
        let again = browser
            .post(&env.node, "/oauth/acme/start", json!({"intent": "sign_in"}))
            .await;
        assert_eq!(again.code(), "provider_unavailable", "{mode:?}");
        assert_eq!(env.idp.with(|s| s.discovery_hits), hits + 1, "{mode:?}");
        env.world.clock.advance(Duration::from_secs(31));
    }
    assert_eq!(
        env.idp.with(|s| s.redirect_target_hits),
        0,
        "redirects are not followed"
    );
    // Recovered: once the 30 s are over, the next start works.
    env.idp.with(|s| s.discovery = Discovery::Normal);
    env.start(&mut browser, "acme", "sign_in").await;
    // GitHub refuses the code at the callback.
    let auth = env.start(&mut browser, "github", "sign_in").await;
    let code = env.idp.grant(&auth, json!({}), Token::Signed);
    env.idp.with(|s| s.codes.clear());
    let answer = env
        .callback(
            &mut browser,
            "github",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), error_page("oauth_failed"));
    assert!(env.node.module.ready().await.is_ok());
}

/// A provider that cannot be reached at all.
#[tokio::test]
async fn an_unreachable_issuer_is_provider_unavailable() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);
    let Some(world) = World::new(&section("", &[], &providers(&base, ""))).await else {
        return;
    };
    let node = world.instance().await;
    let mut browser = Browser::new("198.51.100.51");
    let answer = browser
        .post(&node, "/oauth/acme/start", json!({"intent": "sign_in"}))
        .await;
    assert_eq!(answer.code(), "provider_unavailable");
    assert!(node.module.ready().await.is_ok());
}

/// The start route's `oauth.ip` limit, on arrival.
#[tokio::test]
async fn starts_are_rate_limited_per_ip() {
    let Some(env) = env_with("", "", &[("oauth_ip", 2, "10m")]).await else {
        return;
    };
    let mut browser = Browser::new("198.51.100.52");
    env.start(&mut browser, "acme", "sign_in").await;
    env.start(&mut browser, "github", "sign_in").await;
    let answer = browser
        .post(&env.node, "/oauth/acme/start", json!({"intent": "sign_in"}))
        .await;
    assert_eq!(answer.status, 429);
    assert!(answer.headers.contains_key("retry-after"));
    let mut elsewhere = Browser::new("198.51.100.53");
    env.start(&mut elsewhere, "acme", "sign_in").await;
}

/// User deleted mid-flow: its link flow is cascade-deleted and the
/// callback answers `invalid_state`.
#[tokio::test]
async fn a_user_deleted_mid_flow_ends_the_flow() {
    let Some(env) = env().await else { return };
    let mut ada = signed_up(&env.world, &env.node, "ada@example.com", PASSWORD).await;
    let auth = env.start(&mut ada, "acme", "link").await;
    let code = env.idp.grant(
        &auth,
        env.idp.claims(&auth, env.now(), json!({"sub": "gone"})),
        Token::Signed,
    );
    env.world
        .execute("DELETE FROM merezha_identity.users")
        .await;
    let mut browser = ada.clone();
    browser.cookie = None;
    browser.csrf = None;
    let answer = env
        .callback(
            &mut browser,
            "acme",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), error_page("invalid_state"));
    assert_eq!(
        env.idp.with(|s| s.token_hits),
        0,
        "the code was never exchanged"
    );
}

/// Concurrency: the same callback delivered twice in parallel yields one
/// session; one identity linked by two users in parallel yields one link
/// and one `identity_already_linked`.
#[tokio::test]
async fn parallel_callbacks_and_links() {
    let Some(env) = env().await else { return };
    let mut browser = Browser::new("198.51.100.54");
    let auth = env.start(&mut browser, "acme", "sign_in").await;
    let code = env.idp.grant(
        &auth,
        env.idp.claims(
            &auth,
            env.now(),
            json!({"sub": "twice", "email": "twice@example.com", "email_verified": true}),
        ),
        Token::Signed,
    );
    let query = format!("code={code}&state={}", auth.state);
    let (mut a, mut b) = (browser.clone(), browser.clone());
    let (one, two) = tokio::join!(
        env.callback(&mut a, "acme", &query),
        env.callback(&mut b, "acme", &query)
    );
    let mut locations = [one.location(), two.location()];
    locations.sort();
    let mut expected = [error_page("invalid_state"), SIGNED_IN.to_owned()];
    expected.sort();
    assert_eq!(locations, expected);
    assert_eq!(env.sessions().await, 1);

    let mut ada = signed_up(&env.world, &env.node, "ada@example.com", PASSWORD).await;
    let mut bob = signed_up(&env.world, &env.node, "bob@example.com", PASSWORD).await;
    let ada_auth = env.start(&mut ada, "loose", "link").await;
    let bob_auth = env.start(&mut bob, "loose", "link").await;
    let now = env.now();
    let ada_code = env.idp.grant(
        &ada_auth,
        env.idp.claims(&ada_auth, now, json!({"sub": "contested"})),
        Token::Signed,
    );
    let bob_code = env.idp.grant(
        &bob_auth,
        env.idp.claims(&bob_auth, now, json!({"sub": "contested"})),
        Token::Signed,
    );
    let ada_query = format!("code={ada_code}&state={}", ada_auth.state);
    let bob_query = format!("code={bob_code}&state={}", bob_auth.state);
    let (one, two) = tokio::join!(
        env.callback(&mut ada, "loose", &ada_query),
        env.callback(&mut bob, "loose", &bob_query)
    );
    let mut locations = [one.location(), two.location()];
    locations.sort();
    let mut expected = [error_page("identity_already_linked"), SIGNED_IN.to_owned()];
    expected.sort();
    assert_eq!(locations, expected);
    let contested = env
        .world
        .scalar(
            "SELECT count(*) FROM merezha_identity.oauth_identities WHERE subject = 'contested'",
        )
        .await;
    assert_eq!(contested, 1);
}

/// Google: discovery at its issuer (overridden to the test provider), the
/// `hd` check, and an address used only when asserted verified.
#[tokio::test]
async fn google_checks_its_hosted_domain_and_verified_address() {
    let idp = Idp::start().await;
    let google = format!(
        "[modules.identity.oauth.providers.google]\nclient_id = \"{CLIENT_ID}\"\n\
         client_secret = {{ env = \"OAUTH_SECRET\" }}\nallowed_hosted_domains = [\"example.com\"]\n"
    );
    let Some(world) = World::new(&section("", &[], &google)).await else {
        return;
    };
    world.endpoints.lock().unwrap().push((
        "google".into(),
        merezha_identity::oauth::ProviderEndpoints {
            issuer: Some(idp.base.clone()),
            ..Default::default()
        },
    ));
    let node = world.instance().await;
    let env = Env { world, node, idp };
    let mut browser = Browser::new("198.51.100.55");
    let answer = env
        .oidc(&mut browser, "google", "sign_in", json!({"sub": "g1", "email": "g@other.example", "email_verified": true, "hd": "other.example"}))
        .await;
    assert_eq!(answer.location(), error_page("oauth_failed"));
    let answer = env
        .oidc(
            &mut browser,
            "google",
            "sign_in",
            json!({"sub": "g1", "email": "g@other.example", "email_verified": true}),
        )
        .await;
    assert_eq!(
        answer.location(),
        error_page("oauth_failed"),
        "no hd at all"
    );
    let answer = env
        .oidc(&mut browser, "google", "sign_in", json!({"sub": "g1", "email": "g@example.com", "email_verified": false, "hd": "example.com"}))
        .await;
    assert_eq!(
        answer.location(),
        error_page("email_unverified"),
        "an unverified Google address is no address"
    );
    assert_eq!(
        env.world
            .scalar("SELECT count(*) FROM merezha_identity.users")
            .await,
        0,
        "and creates no account"
    );
    let answer = env
        .oidc(&mut browser, "google", "sign_in", json!({"sub": "g1", "email": "g@example.com", "email_verified": true, "hd": "example.com"}))
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
}

/// No provider token, code, state or address reaches a log line, span or
/// metric: the planted secrets canary for the OAuth routes (TE-T6); the
/// login metric carries the provider name.
#[tokio::test]
async fn oauth_secrets_stay_out_of_telemetry() {
    let harness = merezha_telemetry::testing::Harness::new();
    let idp = Idp::start().await;
    let Some(world) = World::new(&section("", &[], &providers(&idp.base, ""))).await else {
        return;
    };
    let meter = harness.telemetry().meter("identity");
    let node = world.instance_with(Some(&meter)).await;
    let env = Env { world, node, idp };
    let mut browser = Browser::new("198.51.100.56");
    let auth = env.start(&mut browser, "acme", "sign_in").await;
    let claims = env.idp.claims(
        &auth,
        env.now(),
        json!({"sub": "canary-sub", "email": "canary@spotted.example", "email_verified": true}),
    );
    let code = env.idp.grant(&auth, claims, Token::Signed);
    let answer = env
        .callback(
            &mut browser,
            "acme",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    let failed = env
        .callback(
            &mut browser,
            "acme",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(failed.location(), error_page("invalid_state"));
    let logins = harness.metric("merezha.identity.login.attempts");
    assert_eq!(
        logins.value(&[
            ("identity.method", "oauth"),
            ("identity.outcome", "success"),
            ("identity.provider", "acme")
        ]),
        Some(1.0)
    );
    for canary in [
        code.as_str(),
        auth.state.as_str(),
        auth.nonce.as_deref().unwrap(),
        browser.cookie.as_deref().unwrap(),
        "provider-access-token",
        "provider-refresh-token",
        CLIENT_SECRET,
        "canary@spotted.example",
        "canary-sub",
    ] {
        harness.assert_no_canary(canary);
    }
    harness.assert_catalogued();
}

// ----- security review of PR #55 -----

/// Linking adds a way into the account: it needs a recent session, and a
/// successful link sends the `identity.sign_in_method_added` notice.
#[tokio::test]
async fn linking_needs_a_recent_session_and_sends_a_notice() {
    let Some(env) = env().await else { return };
    let mut ada = signed_up(&env.world, &env.node, "ada@example.com", PASSWORD).await;
    let user = ada.get(&env.node, "/me").await.body["user"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    env.world.clock.advance(Duration::from_secs(11 * 60));
    let answer = ada
        .post(&env.node, "/oauth/acme/start", json!({"intent": "link"}))
        .await;
    assert_eq!(answer.code(), "reauthentication_required");
    assert_eq!(
        env.world
            .scalar("SELECT count(*) FROM merezha_identity.oauth_flows")
            .await,
        0
    );
    let answer = ada
        .post(
            &env.node,
            "/session/reauthenticate",
            json!({"password": PASSWORD}),
        )
        .await;
    assert_eq!(answer.status, 200, "{answer:?}");
    let answer = env
        .oidc(&mut ada, "loose", "link", json!({"sub": "loose-ada"}))
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    let notices = env
        .world
        .mail
        .to("identity.sign_in_method_added", &format!("user:{user}"));
    assert_eq!(notices.len(), 1);
    assert_eq!(notices[0].data["method"], "oauth");
    assert_eq!(notices[0].data["provider"], "loose");
    // A sign-up creates an account, not a method on one: no notice.
    let mut other = Browser::new("198.51.100.70");
    env.oidc(
        &mut other,
        "acme",
        "sign_in",
        json!({"sub": "new", "email": "new@example.com", "email_verified": true}),
    )
    .await;
    assert_eq!(
        env.world
            .mail
            .sent()
            .iter()
            .filter(|m| m.topic == "identity.sign_in_method_added")
            .count(),
        1
    );
}

/// Callbacks with unknown or forged states are counted, not audited, and
/// charged to their own bucket; a callback that named a live flow is
/// audited and charged to `oauth_callback.ip`.
#[tokio::test]
async fn failed_callbacks_are_throttled_and_unknown_states_not_audited() {
    // One failure of a live flow fills the budget; junk does not count.
    let Some(env) = env_with("", "", &[("oauth_callback_ip", 1, "10m")]).await else {
        return;
    };
    let mut attacker = Browser::new("203.0.113.80");
    for n in 0..2 {
        let state = B64.encode([n as u8; 32]);
        let answer = env
            .callback(&mut attacker, "acme", &format!("code=x&state={state}"))
            .await;
        assert_eq!(answer.location(), error_page("invalid_state"));
        assert!(
            answer.oauth_cookie().is_none(),
            "a forged callback clears nothing"
        );
    }
    let audited = || {
        env.world.scalar(
            "SELECT count(*) FROM merezha_identity.audit_events WHERE action = 'oauth.sign_in_failed'",
        )
    };
    assert_eq!(audited().await, 0);
    // A live flow finished without its binding cookie is audited.
    let mut victim = Browser::new("198.51.100.71");
    let auth = env.start(&mut victim, "acme", "sign_in").await;
    let answer = env
        .callback(
            &mut attacker,
            "acme",
            &format!("code=x&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), error_page("invalid_state"));
    assert_eq!(audited().await, 1);
    // The budget is spent: even a good callback from that IP is refused
    // before any work.
    let mut same_ip = Browser::new("203.0.113.80");
    let auth = env.start(&mut same_ip, "acme", "sign_in").await;
    let code = env.idp.grant(
        &auth,
        env.idp.claims(&auth, env.now(), json!({"sub": "t"})),
        Token::Signed,
    );
    let answer = env
        .callback(
            &mut same_ip,
            "acme",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), error_page("rate_limited"));
    assert_eq!(env.idp.with(|s| s.token_hits), 0);
    // Another IP is unaffected.
    let mut elsewhere = Browser::new("198.51.100.72");
    let answer = env
        .oidc(
            &mut elsewhere,
            "acme",
            "sign_in",
            json!({"sub": "t", "email": "t@example.com", "email_verified": true}),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
}

/// Two tabs of one browser share the binding cookie; the first callback
/// keeps it while the second flow is pending, the last one clears it.
#[tokio::test]
async fn two_tabs_both_complete() {
    let Some(env) = env().await else { return };
    let mut browser = Browser::new("198.51.100.73");
    let first = env.start(&mut browser, "acme", "sign_in").await;
    let binding = browser.oauth.clone().unwrap();
    let second = env.start(&mut browser, "acme", "sign_in").await;
    assert_eq!(
        browser.oauth.as_deref(),
        Some(binding.as_str()),
        "the binding is kept"
    );
    let now = env.now();
    let claims = json!({"sub": "tabs", "email": "tabs@example.com", "email_verified": true});
    let code = env.idp.grant(
        &first,
        env.idp.claims(&first, now, claims.clone()),
        Token::Signed,
    );
    let mut tab = browser.clone();
    let answer = env
        .callback(
            &mut tab,
            "acme",
            &format!("code={code}&state={}", first.state),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    assert!(answer.oauth_cookie().is_none(), "a flow is still pending");
    let code = env
        .idp
        .grant(&second, env.idp.claims(&second, now, claims), Token::Signed);
    let mut tab = browser.clone();
    let answer = env
        .callback(
            &mut tab,
            "acme",
            &format!("code={code}&state={}", second.state),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    assert!(answer.oauth_cookie().unwrap().contains("Max-Age=0"));
}

/// Rule 4 keeps a well-formed registration binding the browser holds.
#[tokio::test]
async fn an_untrusted_sign_up_keeps_the_presented_registration_binding() {
    let Some(env) = env().await else { return };
    let mut browser = Browser::new("198.51.100.74");
    let held = B64.encode([42u8; 32]);
    browser.binding = Some(held.clone());
    let answer = env
        .oidc(
            &mut browser,
            "loose",
            "sign_in",
            json!({"sub": "keep", "email": "keep@example.com"}),
        )
        .await;
    assert_eq!(answer.location(), error_page("email_unverified"));
    let set = answer.binding_cookie().unwrap();
    assert!(set.starts_with(&format!("{BINDING}={held};")), "{set}");
    let token = env
        .world
        .mail
        .token("identity.verify_email", "keep@example.com");
    let answer = browser
        .post(
            &env.node,
            "/email-verifications/confirm",
            json!({"token": token}),
        )
        .await;
    assert_eq!(
        answer.body["user"]["credentials"]["oauth"],
        json!(["loose"]),
        "no claim"
    );
}

/// Identities carry their issuer: one linked under another issuer never
/// signs in, so changing a provider's issuer never re-maps subjects.
#[tokio::test]
async fn an_identity_of_another_issuer_never_signs_in() {
    let Some(env) = env().await else { return };
    let mut browser = Browser::new("198.51.100.75");
    let claims = json!({"sub": "iss-1", "email": "iss@example.com", "email_verified": true});
    env.oidc(&mut browser, "acme", "sign_in", claims.clone())
        .await;
    let stored = env
        .world
        .scalar(&format!(
            "SELECT count(*) FROM merezha_identity.oauth_identities WHERE issuer = '{}'",
            env.idp.base
        ))
        .await;
    assert_eq!(stored, 1);
    env.world
        .execute(
            "UPDATE merezha_identity.oauth_identities SET issuer = 'https://old-issuer.example'",
        )
        .await;
    let mut other = Browser::new("198.51.100.76");
    let answer = env.oidc(&mut other, "acme", "sign_in", claims).await;
    assert_eq!(answer.location(), error_page("oauth_failed"));
    assert!(answer.set_cookie().is_none());
}

/// A provider that advertises RFC 9207 must send `iss`.
#[tokio::test]
async fn iss_is_required_when_advertised() {
    let Some(env) = env().await else { return };
    env.idp.with(|s| s.iss_supported = true);
    let mut browser = Browser::new("198.51.100.77");
    let auth = env.start(&mut browser, "acme", "sign_in").await;
    let claims = env.idp.claims(
        &auth,
        env.now(),
        json!({"sub": "r", "email": "r@example.com", "email_verified": true}),
    );
    let code = env.idp.grant(&auth, claims.clone(), Token::Signed);
    let answer = env
        .callback(
            &mut browser,
            "acme",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), error_page("oauth_failed"));
    let auth = env.start(&mut browser, "acme", "sign_in").await;
    let claims = env.idp.claims(
        &auth,
        env.now(),
        json!({"sub": "r", "email": "r@example.com", "email_verified": true}),
    );
    let code = env.idp.grant(&auth, claims, Token::Signed);
    let iss =
        oauth2::url::form_urlencoded::byte_serialize(env.idp.base.as_bytes()).collect::<String>();
    let answer = env
        .callback(
            &mut browser,
            "acme",
            &format!("code={code}&state={}&iss={iss}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
}

/// An identity of a provider removed from configuration is no credential:
/// with it the only other method, the last identity cannot go.
#[tokio::test]
async fn identities_of_removed_providers_are_no_credential() {
    let idp = Idp::start().await;
    let tables = providers(&idp.base, "");
    let text = section("", &[], &tables).replace("[modules.identity.magic_links]\n", "");
    let Some(world) = World::new(&text).await else {
        return;
    };
    let node = world.instance().await;
    let env = Env { world, node, idp };
    let mut browser = Browser::new("198.51.100.78");
    env.oidc(
        &mut browser,
        "acme",
        "sign_in",
        json!({"sub": "two", "email": "two@example.com", "email_verified": true}),
    )
    .await;
    let answer = env
        .oidc(&mut browser, "loose", "link", json!({"sub": "two-loose"}))
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    // The same database without `loose` in the configuration.
    let without = text.replace(
        &format!(
            "[modules.identity.oauth.providers.loose]\nkind = \"oidc\"\nissuer = \"{}\"\n\
             client_id = \"{CLIENT_ID}\"\nclient_secret = {{ env = \"OAUTH_SECRET\" }}\n",
            env.idp.base
        ),
        "",
    );
    assert!(!without.contains("providers.loose"));
    let Env {
        world,
        node: _,
        idp,
    } = env;
    let later = World {
        test: world.test,
        db: world.db.clone(),
        clock: world.clock.clone(),
        mail: world.mail.clone(),
        section: without,
        endpoints: Default::default(),
    };
    let node = later.instance().await;
    let env = Env {
        world: later,
        node,
        idp,
    };
    let mut fresh = Browser::new("198.51.100.79");
    env.oidc(&mut fresh, "acme", "sign_in", json!({"sub": "two"}))
        .await;
    let items = fresh.get(&env.node, "/me/oauth-identities").await.body["items"].clone();
    let acme = items
        .as_array()
        .unwrap()
        .iter()
        .find(|i| i["provider"] == "acme")
        .unwrap()["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let answer = fresh
        .delete(&env.node, &format!("/me/oauth-identities/{acme}"))
        .await;
    assert_eq!(answer.code(), "last_credential");
}

/// Rule 2: a link whose starting session expired meanwhile is refused.
#[tokio::test]
async fn a_link_needs_its_session_alive_at_the_callback() {
    let Some(env) = env().await else { return };
    let mut ada = signed_up(&env.world, &env.node, "ada@example.com", PASSWORD).await;
    let auth = env.start(&mut ada, "acme", "link").await;
    let code = env.idp.grant(
        &auth,
        env.idp.claims(&auth, env.now(), json!({"sub": "late"})),
        Token::Signed,
    );
    env.world
        .execute("UPDATE merezha_identity.sessions SET idle_expires_at = created_at, absolute_expires_at = created_at")
        .await;
    // The browser's own cookie is dead too; it comes back without it.
    let mut browser = ada.clone();
    browser.cookie = None;
    browser.csrf = None;
    let answer = env
        .callback(
            &mut browser,
            "acme",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), error_page("invalid_state"));
    assert_eq!(env.identities().await, 0);
}

/// Rule 1 on an unverified account: a trusted provider asserting its exact
/// address verifies it, without the claim (its password stays); while
/// verification is not required an untrusted one signs in unverified.
#[tokio::test]
async fn rule_one_verifies_through_a_trusted_assertion() {
    let Some(env) = env_with("require_verified_email = false", "", &[]).await else {
        return;
    };
    let mut ada = Browser::new("198.51.100.80");
    ada.post(
        &env.node,
        "/registrations",
        json!({"email": "ada@example.com", "password": PASSWORD}),
    )
    .await;
    let answer = ada
        .post(
            &env.node,
            "/sessions",
            json!({"email": "ada@example.com", "password": PASSWORD}),
        )
        .await;
    assert_eq!(answer.status, 201);
    // Linked while the provider asserts another address: still unverified.
    let answer = env
        .oidc(
            &mut ada,
            "acme",
            "link",
            json!({"sub": "r1", "email": "other@example.com", "email_verified": true}),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    assert_eq!(
        ada.get(&env.node, "/me").await.body["user"]["email_verified"],
        false
    );
    // An untrusted identity signs in unverified here.
    let answer = env
        .oidc(&mut ada, "loose", "link", json!({"sub": "r1-loose"}))
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    let mut phone = Browser::new("198.51.100.81");
    let answer = env
        .oidc(&mut phone, "loose", "sign_in", json!({"sub": "r1-loose"}))
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    assert_eq!(
        phone.get(&env.node, "/me").await.body["user"]["email_verified"],
        false
    );
    // Now the trusted provider asserts the account's own address.
    let mut laptop = Browser::new("198.51.100.82");
    let answer = env
        .oidc(
            &mut laptop,
            "acme",
            "sign_in",
            json!({"sub": "r1", "email": "ada@example.com", "email_verified": true}),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
    let user = laptop.get(&env.node, "/me").await.body["user"].clone();
    assert_eq!(user["email_verified"], true);
    assert_eq!(user["credentials"]["password"], true, "no claim");
    assert_eq!(user["credentials"]["oauth"], json!(["acme", "loose"]));
}

/// A flood of junk callbacks exhausts only its own bucket: past it junk is
/// answered `rate_limited`, while the same IP's legitimate callback still
/// signs in.
#[tokio::test]
async fn junk_callbacks_never_lock_out_legitimate_ones() {
    let Some(env) = env_with(
        "",
        "",
        &[
            ("oauth_callback_ip", 5, "10m"),
            ("oauth_callback_unknown_ip", 20, "10m"),
        ],
    )
    .await
    else {
        return;
    };
    let mut attacker = Browser::new("203.0.113.85");
    let mut limited = 0;
    for n in 0..60u32 {
        let state = B64.encode([&n.to_be_bytes()[..], &[7u8; 28]].concat());
        let answer = env
            .callback(&mut attacker, "acme", &format!("code=x&state={state}"))
            .await;
        if answer.location() == error_page("rate_limited") {
            limited += 1;
        } else {
            assert_eq!(answer.location(), error_page("invalid_state"));
        }
    }
    assert_eq!(limited, 40, "junk past its own budget is refused");
    let mut legit = Browser::new("203.0.113.85");
    let answer = env
        .oidc(
            &mut legit,
            "acme",
            "sign_in",
            json!({"sub": "after-flood", "email": "flood@example.com", "email_verified": true}),
        )
        .await;
    assert_eq!(answer.location(), SIGNED_IN);
}

/// The link callback re-checks the reauthentication window: a link started
/// with a recent session (9 min) and completed past it (18 min) links
/// nothing.
#[tokio::test]
async fn a_link_completed_past_the_reauthentication_window_is_refused() {
    let Some(env) = env().await else { return };
    let mut ada = signed_up(&env.world, &env.node, "ada@example.com", PASSWORD).await;
    env.world.clock.advance(Duration::from_secs(9 * 60));
    let auth = env.start(&mut ada, "acme", "link").await;
    env.world.clock.advance(Duration::from_secs(9 * 60));
    let claims = env
        .idp
        .claims(&auth, env.now(), json!({"sub": "slow-link"}));
    let code = env.idp.grant(&auth, claims, Token::Signed);
    let answer = env
        .callback(
            &mut ada,
            "acme",
            &format!("code={code}&state={}", auth.state),
        )
        .await;
    assert_eq!(answer.location(), error_page("reauthentication_required"));
    assert_eq!(env.identities().await, 0);
}
