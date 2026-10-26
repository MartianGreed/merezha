//! The `[modules.notifications]` configuration matrix (NT §7.2): defaults,
//! every startup refusal, all problems reported at once, and secrets kept
//! out of issues and `Debug`.
mod support;
use merezha_notifications::{
    config::{Driver, HostFacts, parse_mailbox, valid_sms_sender},
    topics::Category,
};
use merezha_platform::ports::Channel;
use std::time::Duration;
use support::*;

const N: &str = "modules.notifications";

#[test]
fn a_minimal_section_takes_the_documented_defaults() {
    let checked = Project::new(MINIMAL).configure().unwrap();
    let s = &checked.settings;
    assert_eq!(s.secret_key.expose(), b"0123456789abcdef0123456789abcdef");
    assert_eq!(s.email.driver, Driver::Capture);
    assert_eq!(s.sms.driver, Driver::Disabled);
    assert!(s.push.is_none() && s.webhooks.is_none() && s.brevo.is_none());
    assert_eq!(s.limits.max_recipients, 100);
    assert_eq!(s.limits.max_targets, 1000);
    assert_eq!(s.limits.max_data_bytes, 32_768);
    assert_eq!(
        s.retention.data_after_settled,
        Duration::from_secs(7 * 86_400)
    );
    assert_eq!(s.retention.captures, 1000);
    assert_eq!(s.templates.locales, vec!["en".to_owned()]);
    assert_eq!(s.templates.app_name, "Tasks", "defaults to app.name");
    assert_eq!(s.templates.fuel, 50_000);
    assert!(s.templates.preview && !s.templates.preview_in_public);
    assert_eq!(s.inbox.retention, Duration::from_secs(180 * 86_400));
    assert_eq!(s.sse.max_streams_per_user, 8);
    assert_eq!(s.sse.heartbeat, Duration::from_secs(15));
    assert!(s.capture_active());
    assert!(s.channel_available(Channel::Inbox) && !s.channel_available(Channel::Push));
    // Every identity topic of R2 and the block's alert are predeclared.
    assert_eq!(s.topics.len(), 11);
    let reset = &s.topics["identity.password_reset"];
    assert_eq!(reset.category, Category::Security);
    assert!(!reset.user_configurable && reset.builtin && !reset.declared);
    assert_eq!(checked.registry.templates().count(), 11);
}

#[test]
fn all_config_errors_are_reported_at_once() {
    let project = Project::new(
        r#"
[http]
listen = "not an address"

[modules.notifications]
secret_key = { env = "UNSET_KEY" }
public_base_url = "ftp://api.example.com"
color = "blue"
limits = { max_recipients = 500 }

[modules.notifications.templates]
locales = ["en", "english"]

[modules.notifications.email]
driver = "brevo"
from = "not a mailbox"

[modules.notifications.sms]
driver = "capture"
sender = "This sender is far too long"

[modules.notifications.push]
vapid_subject = "mailto:ops@example.com"

[modules.notifications.quiet_hours]
max_jitter = "5m"

[modules.notifications.topics."tasks.assigned"]
category = "loud"
channels = ["inbox", "fax"]

[modules.notifications.topics."tasks.digest"]
category = "marketing"
channels = ["email"]
default_channels = ["inbox"]

[modules.notifications.topics."identity.password_reset"]
category = "marketing"
user_configurable = true
"#,
    );
    let issues = project.issues();
    for (key, code) in [
        ("http.listen".to_owned(), "invalid_address"),
        (format!("{N}.secret_key"), "secret_unavailable"),
        (format!("{N}.public_base_url"), "invalid_url"),
        (format!("{N}.color"), "unknown_key"),
        (format!("{N}.limits.max_recipients"), "out_of_range"),
        (format!("{N}.templates.locales"), "invalid_locale"),
        (format!("{N}.email.from"), "invalid_sender"),
        (format!("{N}.brevo"), "missing_required"),
        (format!("{N}.sms.sender"), "invalid_sender"),
        (format!("{N}.push.vapid_private_key"), "missing_required"),
        (format!("{N}.quiet_hours"), "unsupported_section"),
        (
            format!("{N}.topics.\"tasks.assigned\".category"),
            "invalid_choice",
        ),
        (
            format!("{N}.topics.\"tasks.assigned\".channels"),
            "invalid_channel",
        ),
        (
            format!("{N}.topics.\"tasks.digest\".default_channels"),
            "default_channel_not_allowed",
        ),
        (
            format!("{N}.topics.\"identity.password_reset\".category"),
            "category_fixed",
        ),
        (
            format!("{N}.topics.\"identity.password_reset\".user_configurable"),
            "security_not_configurable",
        ),
        // Template checks run in the same pass.
        (
            format!("{N}.topics.\"tasks.digest\".template"),
            "template_missing",
        ),
    ] {
        assert!(has(&issues, &key, code), "{key} {code} not in {issues:?}");
    }
}

#[test]
fn http_public_refuses_development_conveniences() {
    let public = |extra: &str| {
        Project::new(&format!(
            "[http]\nlisten = \"0.0.0.0:8080\"\npublic = true\n{MINIMAL}{extra}"
        ))
    };
    let issues = public(
        "egress = { allow_private_networks = true }\n\
         [modules.notifications.templates]\nallow_http_links = true\n\
         [modules.notifications.webhooks]\nallow_http = true\n\
         [modules.notifications.sms]\ndriver = \"capture\"\nsender = \"Tasks\"\n",
    )
    .issues();
    for key in [
        "email.driver",
        "sms.driver",
        "egress.allow_private_networks",
        "templates.allow_http_links",
        "webhooks.allow_http",
        "templates.preview",
    ] {
        assert!(
            has(&issues, &format!("{N}.{key}"), "refused_when_public"),
            "{key} not in {issues:?}"
        );
    }
    // The same capture driver is fine on a private listener.
    assert!(Project::new(MINIMAL).configure().is_ok());
    // A public deployment with Brevo and previews explicitly allowed starts.
    let production = public(
        "public_base_url = \"https://api.example.com\"\n\
         [modules.notifications.templates]\npreview_in_public = true\n\
         [modules.notifications.email]\ndriver = \"brevo\"\nfrom = \"Tasks <no-reply@example.com>\"\n\
         [modules.notifications.brevo]\napi_key = { env = \"BREVO_KEY\" }\nwebhook_token = { env = \"BREVO_TOKEN\" }\n",
    )
    .env("BREVO_KEY", "xkeysib-test")
    .env("BREVO_TOKEN", &"t".repeat(32));
    let checked = production.configure().unwrap();
    assert_eq!(checked.settings.email.driver, Driver::Brevo);
    assert!(!checked.settings.capture_active());
    // A non-https Brevo URL is refused there.
    let issues = public(
        "public_base_url = \"https://api.example.com\"\n\
         [modules.notifications.templates]\npreview = false\n\
         [modules.notifications.email]\ndriver = \"disabled\"\n\
         [modules.notifications.brevo]\nbase_url = \"http://127.0.0.1:9\"\n",
    )
    .issues();
    assert!(
        has(
            &issues,
            &format!("{N}.brevo.base_url"),
            "refused_when_public"
        ),
        "{issues:?}"
    );
}

#[test]
fn brevo_drivers_need_their_secrets_and_the_public_base_url() {
    let brevo = |extra: &str| {
        Project::new(&format!(
            "{MINIMAL}{extra}\n[modules.notifications.email]\ndriver = \"brevo\"\nfrom = \"no-reply@example.com\"\n\
             [modules.notifications.sms]\ndriver = \"brevo\"\nsender = \"Tasks\"\n"
        ))
    };
    let issues = brevo("\n[modules.notifications.brevo]\nsandbox = true\n").issues();
    for (key, code) in [
        ("brevo.api_key", "missing_required"),
        ("brevo.webhook_token", "missing_required"),
        ("public_base_url", "missing_required"),
    ] {
        assert!(
            has(&issues, &format!("{N}.{key}"), code),
            "{key} {code} not in {issues:?}"
        );
    }
    let issues = brevo(
        "public_base_url = \"https://api.example.com/\"\n\
         [modules.notifications.brevo]\napi_key = \"literal\"\nwebhook_token = { env = \"SHORT\" }\n",
    )
    .env("SHORT", "short")
    .issues();
    assert!(
        has(&issues, &format!("{N}.brevo.api_key"), "secret_literal"),
        "{issues:?}"
    );
    assert!(
        has(
            &issues,
            &format!("{N}.brevo.webhook_token"),
            "secret_too_short"
        ),
        "{issues:?}"
    );
    let checked = brevo(
        "public_base_url = \"https://api.example.com/\"\n\
         [modules.notifications.brevo]\napi_key = { env = \"K\" }\nwebhook_token = { file = \"token\" }\n",
    )
    .env("K", "xkeysib-secret-value");
    std::fs::write(
        checked.path().join("token"),
        format!("{}\n", "w".repeat(40)),
    )
    .unwrap();
    let checked = checked.configure().unwrap();
    let s = &checked.settings;
    assert_eq!(
        s.public_base_url.as_deref(),
        Some("https://api.example.com")
    );
    let brevo = s.brevo.as_ref().unwrap();
    assert_eq!(brevo.base_url, "https://api.brevo.com");
    assert_eq!(brevo.timeout, Duration::from_secs(10));
    assert_eq!(
        brevo.webhook_token.as_ref().unwrap().expose(),
        "w".repeat(40)
    );
    // Secrets never reach Debug output.
    let printed = format!("{s:?}");
    for secret in ["xkeysib-secret-value", &"w".repeat(40), "MDEyMzQ1"] {
        assert!(!printed.contains(secret), "Debug leaked {secret}");
    }
}

#[test]
fn queue_leases_and_the_webhook_backoff_are_checked_against_jobs() {
    let base = "public_base_url = \"https://api.example.com\"\n\
        [modules.notifications.email]\ndriver = \"brevo\"\nfrom = \"no-reply@example.com\"\n\
        [modules.notifications.brevo]\napi_key = { env = \"K\" }\nwebhook_token = { env = \"T\" }\n\
        [modules.notifications.webhooks]\n";
    let project = |jobs: &str| {
        Project::new(&format!("{jobs}\n{MINIMAL}{base}"))
            .env("K", "key")
            .env("T", &"t".repeat(32))
    };
    let issues = project("[jobs.queues.\"notify.email\"]\nlease_ms = 20000\n").issues();
    assert!(
        has(
            &issues,
            "jobs.queues.\"notify.email\".lease_ms",
            "lease_too_short"
        ),
        "{issues:?}"
    );
    // Without a notify.webhook queue the 1 h default cap is too short (R1).
    assert!(
        has(
            &issues,
            "jobs.queues.\"notify.webhook\".backoff_max_ms",
            "backoff_max_too_short"
        ),
        "{issues:?}"
    );
    let fine = project(
        "[jobs.queues]\n\"notify.email\" = { lease_ms = 30000 }\n\
         \"notify.webhook\" = { backoff_max_ms = 86400000 }\n",
    );
    let checked = fine.configure().unwrap();
    assert!(checked.settings.webhooks.is_some());
    assert_eq!(
        checked.settings.provider_retry_cap,
        Duration::from_secs(3600),
        "the default 1 h email backoff cap"
    );
    // NT-N3: an account problem waits at least five minutes, which the
    // email queue's backoff cap must allow.
    let issues = project(
        "[jobs.queues]\n\"notify.email\" = { lease_ms = 30000, backoff_max_ms = 60000 }\n\
         \"notify.webhook\" = { backoff_max_ms = 86400000 }\n",
    )
    .issues();
    assert!(
        has(
            &issues,
            "jobs.queues.\"notify.email\".backoff_max_ms",
            "backoff_max_too_short"
        ),
        "{issues:?}"
    );
}

#[test]
fn channels_need_a_driver_and_a_section() {
    let issues = Project::new(&format!(
        "{MINIMAL}[modules.notifications.topics.\"tasks.assigned\"]\ncategory = \"transactional\"\nchannels = [\"inbox\", \"push\"]\n\
         [modules.notifications.topics.\"tasks.texted\"]\ncategory = \"transactional\"\nchannels = [\"sms\"]\n"
    ))
    .issues();
    for topic in ["tasks.assigned", "tasks.texted"] {
        assert!(
            has(
                &issues,
                &format!("{N}.topics.\"{topic}\".channels"),
                "channel_unavailable"
            ),
            "{topic} not in {issues:?}"
        );
    }
}

#[test]
fn no_egress_requires_loopback_providers() {
    let project = |url: &str| {
        Project::new(&format!(
            "{MINIMAL}[modules.notifications.email]\ndriver = \"disabled\"\n\
             [modules.notifications.brevo]\nbase_url = \"{url}\"\n"
        ))
        .env("MEREZHA_NOTIFY_NO_EGRESS", "1")
    };
    let issues = project("https://api.brevo.com").issues();
    assert!(
        has(
            &issues,
            &format!("{N}.brevo.base_url"),
            "egress_not_loopback"
        ),
        "{issues:?}"
    );
    assert!(project("http://127.0.0.1:9").configure().is_ok());
}

#[test]
fn secret_keys_come_from_the_environment_and_must_be_long_enough() {
    let project = Project::new("[modules.notifications]\n")
        .env("MEREZHA_MODULES__NOTIFICATIONS__SECRET_KEY", KEY);
    assert!(project.configure().is_ok());
    let missing = Project::new("[modules.notifications]\n").issues();
    assert!(has(
        &missing,
        &format!("{N}.secret_key"),
        "missing_required"
    ));
    let short = Project::new(MINIMAL).env("NOTIFY_KEY", "c2hvcnQ=");
    let issues = short.issues();
    assert!(
        has(&issues, &format!("{N}.secret_key"), "invalid_secret_key"),
        "{issues:?}"
    );
    // The value never appears in an issue.
    let errors = short.configure().unwrap_err();
    assert!(!errors.to_string().contains("c2hvcnQ"));
}

#[test]
fn builtin_topics_accept_channel_template_and_priority_overrides() {
    let checked = Project::new(&format!(
        "{MINIMAL}[modules.notifications.topics.\"identity.new_sign_in\"]\n\
         category = \"security\"\nchannels = [\"email\"]\npriority = \"high\"\n"
    ))
    .configure()
    .unwrap();
    let topic = &checked.settings.topics["identity.new_sign_in"];
    assert_eq!(topic.channels, vec![Channel::Email]);
    assert_eq!(topic.default_channels, vec![Channel::Email]);
    assert_eq!(topic.priority.as_str(), "high");
    assert!(topic.builtin && topic.declared);
}

#[test]
fn mailboxes_and_sms_senders_are_parsed_strictly() {
    let named = parse_mailbox("Acme <no-reply@acme.com>").unwrap();
    assert_eq!(named.name.as_deref(), Some("Acme"));
    assert_eq!(named.address, "no-reply@acme.com");
    assert!(parse_mailbox("no-reply@acme.com").unwrap().name.is_none());
    for bad in [
        "",
        "acme",
        "Acme <>",
        "a@b@c.com",
        "Acme <a@b.com",
        "a b@c.com",
        "x\r\n@y.com",
    ] {
        assert!(parse_mailbox(bad).is_none(), "{bad:?}");
    }
    assert!(valid_sms_sender("Acme") && valid_sms_sender("123456789012345"));
    for bad in ["", "Acme Inc", "TwelveLetter", "1234567890123456", "Acmé"] {
        assert!(!valid_sms_sender(bad), "{bad}");
    }
}

#[test]
fn host_facts_follow_the_kernel_configuration() {
    let project = Project::new(&format!(
        "[http]\nlisten = \"0.0.0.0:80\"\npublic = true\n{MINIMAL}"
    ));
    let facts = HostFacts::load(&project.input());
    assert_eq!(facts.http_public, Some(true));
    assert_eq!(facts.app_name.as_deref(), Some("Tasks"));
    assert_eq!(facts.base_dir, project.path().canonicalize().unwrap());
    assert!(!facts.no_egress);
    // A broken kernel section does not hide the facts the block reads.
    let broken = Project::raw("[app]\n[modules.notifications]\n");
    let facts = HostFacts::load(&broken.input());
    assert_eq!(facts.http_public, Some(false));
    assert!(facts.jobs.is_some());
    assert_eq!(facts.base_dir, broken.path().canonicalize().unwrap());
}

/// `(key, code, origin)` of every issue.
fn with_origins(project: &Project) -> Vec<(String, &'static str, String)> {
    match project.configure() {
        Ok(_) => Vec::new(),
        Err(errors) => errors
            .0
            .iter()
            .map(|i| (i.key.clone(), i.code, i.origin.to_string()))
            .collect(),
    }
}

#[test]
fn host_checks_run_even_when_kernel_sections_are_invalid() {
    let project = Project::raw(&format!(
        "{}\n[http]\nlisten = \"0.0.0.0:8080\"\npublic = true\n{MINIMAL}\n[modules.notifications.webhooks]\n",
        support::KERNEL.replace("manifest = \"runtime.toml\"", "manifest = 3")
    ));
    let issues = project.issues();
    assert!(
        has(&issues, "runtime.manifest", "expected_string"),
        "{issues:?}"
    );
    assert!(
        has(&issues, &format!("{N}.email.driver"), "refused_when_public"),
        "{issues:?}"
    );
    assert!(
        has(
            &issues,
            "jobs.queues.\"notify.webhook\".backoff_max_ms",
            "backoff_max_too_short"
        ),
        "{issues:?}"
    );
}

#[test]
fn host_facts_follow_the_kernel_layers() {
    let project = Project::raw(
        "[app]\nid = \"tasks\"\n[http]\npublic = true\n[runtime]\nmanifest = 3\n\
         [jobs.queues.\"notify.email\"]\nlease_ms = 20000\n",
    );
    let facts = HostFacts::load(&project.input());
    assert_eq!(facts.http_public, Some(true), "the file layer");
    assert_eq!(
        facts.app_name.as_deref(),
        Some("tasks"),
        "app.name defaults to app.id"
    );
    let jobs = facts.jobs.as_ref().expect("[jobs] validates alone");
    assert_eq!(
        jobs.queues["notify.email"].settings.lease,
        Duration::from_millis(20_000)
    );
    let mut input = project.input();
    input
        .env
        .push(("MEREZHA_HTTP__PUBLIC".into(), "false".into()));
    input
        .env
        .push(("MEREZHA_JOBS__DEFAULTS__LEASE_MS".into(), "45000".into()));
    let facts = HostFacts::load(&input);
    assert_eq!(
        facts.http_public,
        Some(false),
        "the environment wins over the file"
    );
    assert_eq!(
        facts.jobs.as_ref().unwrap().defaults.lease,
        Duration::from_millis(45_000)
    );
    assert_eq!(
        facts.jobs_origins["jobs.queues.\"notify.webhook\".lease_ms"].to_string(),
        "env MEREZHA_JOBS__DEFAULTS__LEASE_MS"
    );
    assert_eq!(
        facts.jobs_origins["jobs.queues.\"notify.email\".lease_ms"].to_string(),
        "file"
    );
    input.overrides.push(("http.public".into(), "true".into()));
    assert_eq!(
        HostFacts::load(&input).http_public,
        Some(true),
        "a flag wins"
    );
    // A malformed value leaves the decision to the kernel's own issue.
    let broken = Project::raw("[http]\npublic = \"yes\"\n");
    assert_eq!(HostFacts::load(&broken.input()).http_public, None);
}

#[test]
fn cross_check_issues_carry_the_layer_that_set_the_key() {
    let public = |extra: &str, jobs: &str| {
        Project::new(&format!(
            "[http]\nlisten = \"0.0.0.0:8080\"\npublic = true\n{jobs}\n{MINIMAL}{extra}"
        ))
    };
    let issues = with_origins(&public(
        "[modules.notifications.sms]\ndriver = \"capture\"\nsender = \"Tasks\"\n\
         [modules.notifications.templates]\nfuel = 60000\n\
         [modules.notifications.webhooks]\n",
        "[jobs.queues.\"notify.webhook\"]\nlease_ms = 1000\n",
    ));
    let origin = |key: &str, code: &str| {
        issues
            .iter()
            .find(|(k, c, _)| k == key && *c == code)
            .map(|(_, _, o)| o.clone())
            .unwrap_or_else(|| panic!("{key} {code} not in {issues:?}"))
    };
    assert_eq!(
        origin(&format!("{N}.email.driver"), "refused_when_public"),
        "default"
    );
    assert_eq!(
        origin(&format!("{N}.sms.driver"), "refused_when_public"),
        "file"
    );
    assert_eq!(
        origin(&format!("{N}.templates.preview"), "refused_when_public"),
        "default"
    );
    // `[templates]` exists but `preview` is not in it: still the default.
    assert_eq!(
        origin("jobs.queues.\"notify.webhook\".lease_ms", "lease_too_short"),
        "file"
    );
    assert_eq!(
        origin(
            "jobs.queues.\"notify.webhook\".backoff_max_ms",
            "backoff_max_too_short"
        ),
        "default"
    );
    let issues = with_origins(
        &Project::new(&format!(
            "{MINIMAL}[modules.notifications.sms]\ndriver = \"brevo\"\nsender = \"Tasks\"\n\
             [modules.notifications.brevo]\napi_key = {{ env = \"K\" }}\n"
        ))
        .env("K", "key")
        .env("MEREZHA_MODULES__NOTIFICATIONS__PUBLIC_BASE_URL", ""),
    );
    assert!(
        issues
            .iter()
            .any(|(k, c, o)| k == &format!("{N}.public_base_url")
                && *c == "missing_required"
                && o == "env MEREZHA_MODULES__NOTIFICATIONS__PUBLIC_BASE_URL"),
        "{issues:?}"
    );
}

#[test]
fn host_facts_split_flags_and_queue_variables_as_the_kernel_does() {
    let project = Project::new(&format!("{MINIMAL}[modules.notifications.webhooks]\n"));
    for flag in ["\"http\".public", "http.\"public\""] {
        let mut input = project.input();
        input.overrides.push((flag.into(), "true".into()));
        assert_eq!(HostFacts::load(&input).http_public, Some(true), "{flag}");
    }
    let mut input = project.input();
    input
        .overrides
        .push(("\"jobs\".defaults.lease_ms".into(), "45000".into()));
    let facts = HostFacts::load(&input);
    assert_eq!(
        facts.jobs.as_ref().unwrap().defaults.lease,
        Duration::from_millis(45_000)
    );
    assert_eq!(
        facts.jobs_origins["jobs.queues.\"notify.webhook\".lease_ms"].to_string(),
        "flag"
    );
    // The kernel reads a queue-level variable; the issue names that layer.
    let project = project.env("MEREZHA_JOBS__QUEUES__NOTIFY.WEBHOOK__LEASE_MS", "1000");
    let facts = HostFacts::load(&project.input());
    assert_eq!(
        facts.jobs.as_ref().unwrap().queues["notify.webhook"]
            .settings
            .lease,
        Duration::from_millis(1000)
    );
    let issues = with_origins(&project);
    assert!(
        issues
            .iter()
            .any(|(k, c, o)| k == "jobs.queues.\"notify.webhook\".lease_ms"
                && *c == "lease_too_short"
                && o == "env MEREZHA_JOBS__QUEUES__NOTIFY.WEBHOOK__LEASE_MS"),
        "{issues:?}"
    );
}
